use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use external_storage::{BlobObject, ExternalStorage, WalkBlobStorage, WalkExternalStorage};
use futures::{
    future::{FusedFuture, FutureExt, TryFutureExt},
    io::AsyncReadExt,
    stream::{Fuse, FusedStream, StreamExt, TryStreamExt},
};
use prometheus::core::{Atomic, AtomicU64};
use tikv_util::{stream::RetryExt, time::Instant};
use tokio_stream::Stream;
use tracing::{instrument::Instrumented, span::Entered, Span};
use tracing_active_tree::frame;
use txn_types::TimeStamp;

use super::{
    errors::{Error, Result},
    statistic::LoadMetaStatistic,
};
use crate::utils;

const METADATA_PREFIX: &'static str = "v1/backupmeta";

#[derive(Debug)]
pub struct MetaFile {
    pub name: Arc<str>,
    pub logs: Vec<LogFile>,
    pub min_ts: u64,
    pub max_ts: u64,
}

#[derive(Debug)]

pub struct LogFile {
    pub id: LogFileId,
    pub real_size: u64,
    pub region_id: u64,
    pub cf: &'static str,
    pub min_ts: u64,
    pub max_ts: u64,
    pub min_start_ts: u64,
    pub min_key: Arc<[u8]>,
    pub max_key: Arc<[u8]>,
    pub is_meta: bool,
}

#[derive(Clone)]
pub struct LogFileId {
    pub name: Arc<str>,
    pub offset: u64,
    pub length: u64,
}

impl std::fmt::Debug for LogFileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Id")
            .field(&self.name)
            .field(&format_args!("@{}+{}", self.offset, self.length))
            .finish()
    }
}

pub struct LoadFromExt<'a> {
    pub max_concurrent_fetch: usize,
    pub on_update_stat: Option<Box<dyn FnMut(LoadMetaStatistic) + 'a>>,
    pub loading_content_span: Option<Span>,
}

impl<'a> LoadFromExt<'a> {
    fn enter_load_span(&self) -> Option<Entered> {
        if let Some(span) = &self.loading_content_span {
            Some(span.enter())
        } else {
            None
        }
    }
}

impl<'a> Default for LoadFromExt<'a> {
    fn default() -> Self {
        Self {
            max_concurrent_fetch: 16,
            on_update_stat: None,
            loading_content_span: None,
        }
    }
}

pub struct StreamyMetaStorage<'a> {
    prefetch: VecDeque<
        Prefetch<Pin<Box<dyn Future<Output = Result<(MetaFile, LoadMetaStatistic)>> + 'a>>>,
    >,
    ext_storage: &'a dyn ExternalStorage,
    ext: LoadFromExt<'a>,
    stat: LoadMetaStatistic,

    files: Fuse<Pin<Box<dyn Stream<Item = std::io::Result<BlobObject>> + 'a>>>,
}

#[pin_project::pin_project(project = ProjPrefetch)]
enum Prefetch<F: Future> {
    Polling(#[pin] F),
    Ready(<F as Future>::Output),
}

impl<F: Future> Prefetch<F> {
    fn must_fetch(self) -> <F as Future>::Output {
        match self {
            Prefetch::Ready(v) => v,
            _ => panic!("must_cached call but the future not ready"),
        }
    }

    fn new(f: F) -> Self {
        Self::Polling(f)
    }
}

impl<F: Future> Future for Prefetch<F> {
    type Output = ();

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.as_mut().project() {
            ProjPrefetch::Polling(fut) => {
                let resolved = ready!(fut.poll(cx));
                unsafe {
                    // SAFETY: we won't poll it anymore.
                    *self.get_unchecked_mut() = Prefetch::Ready(resolved);
                }
                ().into()
            }
            ProjPrefetch::Ready(_) => std::task::Poll::Pending,
        }
    }
}

impl<F: Future> FusedFuture for Prefetch<F> {
    fn is_terminated(&self) -> bool {
        match self {
            Prefetch::Polling(_) => false,
            Prefetch::Ready(_) => true,
        }
    }
}

impl<'a> Stream for StreamyMetaStorage<'a> {
    type Item = Result<MetaFile>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.prefetch.is_empty() {
            return self.poll_fetch_or_finish(cx);
        }

        let first_result = self.poll_first_prefetch(cx);
        match first_result {
            Poll::Ready(item) => Some(item).into(),
            Poll::Pending => self.poll_fetch_or_finish(cx),
        }
    }
}

impl<'a> StreamyMetaStorage<'a> {
    fn poll_fetch_or_finish(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<MetaFile>>> {
        loop {
            // No more space for prefetching.
            if self.prefetch.len() >= self.ext.max_concurrent_fetch {
                return Poll::Pending;
            }
            if self.files.is_terminated() {
                self.ext.loading_content_span.take();
                if self.prefetch.is_empty() {
                    return None.into();
                } else {
                    return Poll::Pending;
                }
            }
            match {
                let _enter = self.ext.enter_load_span();
                self.files.next().poll_unpin(cx)
            } {
                Poll::Ready(Some(load)) => {
                    let mut fut =
                        Prefetch::new(MetaFile::load_from(self.ext_storage, load?).boxed_local());
                    // start the execution of this future.
                    let poll = fut.poll_unpin(cx);
                    if poll.is_ready() {
                        // We need to check this in next run.
                        cx.waker().wake_by_ref();
                    }
                    self.stat.prefetch_task_emitted += 1;
                    self.prefetch.push_back(fut);
                }
                Poll::Ready(None) | Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_first_prefetch(&mut self, cx: &mut Context<'_>) -> Poll<Result<MetaFile>> {
        for fut in &mut self.prefetch {
            if !fut.is_terminated() {
                let _ = fut.poll_unpin(cx);
            }
        }
        if self.prefetch[0].is_terminated() {
            let file = self.prefetch.pop_front().unwrap().must_fetch();
            match file {
                Ok((file, stat)) => {
                    self.stat += stat;
                    self.stat.meta_files_in += 1;
                    self.stat.prefetch_task_finished += 1;
                    self.flush_stat();
                    Ok(file).into()
                }
                Err(err) => Err(err.attach_current_frame()).into(),
            }
        } else {
            Poll::Pending
        }
    }

    fn flush_stat(&mut self) {
        let stat = std::mem::take(&mut self.stat);
        if let Some(u) = &mut self.ext.on_update_stat {
            u(stat)
        }
    }

    pub fn load_from_ext(s: &'a dyn WalkExternalStorage, ext: LoadFromExt<'a>) -> Self {
        let files = s.walk(&METADATA_PREFIX).fuse();
        Self {
            prefetch: VecDeque::new(),
            files,
            ext_storage: s,
            ext,
            stat: LoadMetaStatistic::default(),
        }
    }

    pub async fn count_objects(s: &'a dyn WalkExternalStorage) -> std::io::Result<u64> {
        let mut n = 0;
        let mut items = s.walk(&METADATA_PREFIX);
        while let Some(_) = items.try_next().await? {
            n += 1
        }
        Ok(n)
    }
}

impl MetaFile {
    #[tracing::instrument(skip_all, fields(blob=%blob))]
    async fn load_from(
        s: &dyn ExternalStorage,
        blob: BlobObject,
    ) -> Result<(Self, LoadMetaStatistic)> {
        use protobuf::Message;

        let mut stat = LoadMetaStatistic::default();
        let begin = Instant::now();

        let error_cnt = Arc::new(AtomicU64::new(0));
        let error_cnt2 = Arc::clone(&error_cnt);
        let blob_name = blob.key.clone();
        let ext = RetryExt::default().with_fail_hook(move |err| {
            eprintln!(
                "[{}] warn: encountered error err={} downloading={}",
                TimeStamp::physical_now(),
                err,
                blob_name
            );
            error_cnt.inc_by(1);
        });

        let loading_file = tikv_util::stream::retry_all_ext(
            || async {
                let mut content = vec![];
                let n = s.read(&blob.key).read_to_end(&mut content).await?;
                std::io::Result::Ok((n, content))
            },
            ext,
        );
        let (n, content) = frame!(loading_file)
            .await
            .map_err(|err| Error::from(err).message(format_args!("reading {}", blob.key)))?;
        stat.physical_bytes_loaded += n as u64;
        stat.error_during_downloading += error_cnt2.get();

        let mut meta_file = kvproto::brpb::Metadata::new();
        meta_file.merge_from_bytes(&content)?;
        let mut log_files = vec![];
        let min_ts = meta_file.min_ts;
        let max_ts = meta_file.max_ts;

        for mut group in meta_file.take_file_groups().into_iter() {
            stat.physical_data_files_in += 1;
            let name = Arc::from(group.path.clone().into_boxed_str());
            for mut log_file in group.take_data_files_info().into_iter() {
                stat.logical_data_files_in += 1;
                log_files.push(LogFile {
                    id: LogFileId {
                        name: Arc::clone(&name),
                        offset: log_file.range_offset,
                        length: log_file.range_length,
                    },
                    real_size: log_file.length,
                    region_id: log_file.region_id as _,
                    cf: utils::cf_name(&log_file.cf),
                    max_ts: log_file.max_ts,
                    min_ts: log_file.min_ts,
                    max_key: Arc::from(log_file.take_end_key().into_boxed_slice()),
                    min_key: Arc::from(log_file.take_start_key().clone().into_boxed_slice()),
                    is_meta: log_file.is_meta,
                    min_start_ts: log_file.min_begin_ts_in_default_cf,
                })
            }
        }
        drop(meta_file);

        let result = Self {
            name: Arc::from(blob.key.to_owned().into_boxed_str()),
            logs: log_files,
            min_ts,
            max_ts,
        };
        stat.load_file_duration += begin.saturating_elapsed();
        Ok((result, stat))
    }
}
