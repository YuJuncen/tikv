use std::{fmt::Display, sync::Arc};

use external_storage::{ExternalStorage, UnpinReader};
use futures::{future::TryFutureExt, io::Cursor};
use kvproto::brpb::{DataFileGroup, DataFileInfo};
use tikv_util::{
    debug, info,
    time::{Instant, Limiter},
    warn,
};
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::instrument;
use txn_types::TimeStamp;

use crate::{
    checkpoint_manager::{ErrorContext, FlushDoneContext, FlushObserver},
    errors::{ContextualResultExt, Error, ReportableResult, Result},
    router::{DataFile, FormatType, MetadataInfo, TempFileKey},
    tempfiles::TempFilePool,
    utils::{
        actor::{Actor, Address},
        CompressionWriter, FilesReader, StopWatch,
    },
};

pub struct FlushManager {
    /// support external storage. eg local/s3.
    pub(crate) storage: Arc<dyn ExternalStorage>,
    /// flushing_files contains files pending flush.
    flushing_files: Vec<FlushingFile>,
    /// flushing_meta_files contains meta files pending flush.
    flushing_meta_files: Vec<FlushingFile>,
    /// This counts how many times this task has failed to flush.
    flush_fail_count: usize,
    /// The size limit of the merged file for this task.
    merged_file_size_limit: u64,
    /// The pool for holding the temporary files.
    temp_file_pool: Arc<TempFilePool>,

    store_id: u64,
    /// The "epoch" of executing flushing.
    /// Each incoming `FlushFiles` task will increase this by one.
    ///
    /// This is useful because some observers may only need to be executed once
    /// if there are batched flushing. (Say, updating GC safepoint or global
    /// checkpoint.)
    ///
    /// The hooks of observer will hint the current SN and when it began. So we
    /// can check whether the observer is the "leader" of this flush.
    flush_sn: u64,
    /// The pending observers.
    /// They will be removed once flush succeeded.
    pending_observers: Vec<PendingObserver>,
}

struct PendingObserver {
    obs: Box<dyn FlushObserver>,
    start_sn: u64,
}

#[derive(Debug)]
pub struct FlushingFile {
    pub key: TempFileKey,
    pub fd: DataFile,
    pub info: DataFileInfo,
}

pub enum Task {
    FlushFiles {
        files: Vec<FlushingFile>,
        observer: Box<dyn FlushObserver>,
    },
    Retry,
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FlushFiles { files, observer } => {
                f.debug_struct("FlushFiles").field("files", files).finish()
            }
            Self::Retry => write!(f, "Retry"),
        }
    }
}

#[async_trait::async_trait]
impl Actor for FlushManager {
    type Message = Task;

    async fn on_msg(&mut self, msg: Task, this: &Address<Self::Message>) {
        match msg {
            Task::FlushFiles {
                files,
                mut observer,
            } => {
                self.flush_sn += 1;
                info!(#"FlushManager", "Start flushing."; "epoch" => self.flush_sn);
                observer.before_flush().await;

                self.pending_observers.push(PendingObserver {
                    obs: observer,
                    start_sn: self.flush_sn,
                });
                for f in files {
                    if f.key.is_meta {
                        self.flushing_meta_files.push(f);
                    } else {
                        self.flushing_files.push(f);
                    }
                }
                self.flush(this).await;
            }
            Task::Retry => {
                info!(#"FlushManager", "Retry flushing."; "epoch" => self.flush_sn);
                self.flush(this).await;
            }
        }
    }
}

impl FlushManager {
    pub fn new(
        store_id: u64,
        merged_file_size_limit: u64,
        storage: Arc<dyn ExternalStorage>,
        temp_file_pool: Arc<TempFilePool>,
    ) -> Self {
        Self {
            storage,
            flushing_files: Default::default(),
            flushing_meta_files: Default::default(),
            flush_fail_count: 0,
            flush_sn: 0,
            merged_file_size_limit,
            temp_file_pool,
            store_id,
            pending_observers: vec![],
        }
    }

    pub async fn flush_all(&self, metadata: &mut MetadataInfo) -> Result<()> {
        self.flush_by_chunks(metadata, &self.flushing_files, false)
            .await?;
        self.flush_by_chunks(metadata, &self.flushing_meta_files, true)
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn flush_meta(&self, metadata_info: MetadataInfo) -> Result<()> {
        if !metadata_info.file_groups.is_empty() {
            let meta_path = metadata_info.path_to_meta();
            let meta_buff = metadata_info.marshal_to()?;
            let buflen = meta_buff.len();

            self.storage
                .write(
                    &meta_path,
                    UnpinReader(Box::new(Cursor::new(meta_buff))),
                    buflen as _,
                )
                .await
                .context(format_args!("flush meta {:?}", meta_path))?;
        }
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn generate_metadata(&mut self) -> Result<MetadataInfo> {
        let w = &mut self.flushing_files;
        let wm = &mut self.flushing_meta_files;
        // Let's flush all files first...
        futures::future::join_all(
            w.iter_mut()
                .chain(wm.iter_mut())
                .map(|file| file.fd.inner.done()),
        )
        .await
        .into_iter()
        .map(|r| r.map_err(Error::from))
        .fold(Ok(()), Result::and)?;

        let mut metadata = MetadataInfo::with_capacity(w.len() + wm.len());
        metadata.set_store_id(self.store_id);
        // delay push files until log files are flushed
        Ok(metadata)
    }

    async fn flush(&mut self, this: &Address<Task>) {
        match self.do_flush().await {
            Ok(_) => {
                let end_sn = self.flush_sn;
                futures::future::try_join_all(self.pending_observers.drain(..).map(
                    |mut po| async move {
                        po.obs
                            .after_flush_done(FlushDoneContext {
                                start_sn: po.start_sn,
                                end_sn,
                            })
                            .await
                    },
                ))
                .map_ok(|_| ())
                .await
                .report_if_err("during executing callback of flushing");
            }
            Err(err) => {
                let mut retry_after = None;
                let size = self.pending_observers.len();
                info!(#"FlushManager", "Encountered flush error."; "err" => %err,
                        "current_epoch" => self.flush_sn, "pending_observer" => size);
                let mut err = Some(err);
                for po in self.pending_observers.iter_mut() {
                    let last_retry = retry_after;
                    let ctx = ErrorContext {
                        start_sn: po.start_sn,
                        current_sn: self.flush_sn,
                        error: &mut err,

                        retry_after: &mut retry_after,
                    };
                    po.obs.on_err(ctx);
                    if err.is_none() {
                        info!(#"FlushManager", "An observer has taken the error. Skip following observers.");
                        break;
                    }
                    retry_after = retry_after.max(last_retry);
                }
                if let Some(after) = retry_after {
                    info!(#"FlushManager", "Scheduling retry."; 
                        "after" => ?after, "current_epoch" => self.flush_sn);
                    let addr = Address::clone(this);
                    tokio::spawn(async move {
                        tokio::time::sleep(after).await;
                        addr.send(Task::Retry);
                    });
                }
            }
        }
    }

    /// execute the flush: copy local files to external storage.
    /// if success, return the last resolved ts of this flush.
    /// The caller can try to advance the resolved ts and provide it to the
    /// function, and we would use `max(resolved_ts_provided,
    /// resolved_ts_from_file)`.
    #[instrument(skip_all)]
    async fn do_flush(&mut self) -> Result<()> {
        // do nothing if not flushing status.
        let result = async {
            let begin = Instant::now_coarse();
            let mut sw = StopWatch::by_now();

            // generate meta data and prepare to flush to storage
            let mut metadata_info = self.generate_metadata().await?;

            fail::fail_point!("after_moving_to_flushing_files");
            crate::metrics::FLUSH_DURATION
                .with_label_values(&["generate_metadata"])
                .observe(sw.lap().as_secs_f64());

            // flush log file to storage.
            self.flush_all(&mut metadata_info).await?;

            // compress length
            let file_size_vec = metadata_info
                .file_groups
                .iter()
                .map(|d| (d.length, d.data_files_info.len()))
                .collect::<Vec<_>>();
            // flush meta file to storage.
            self.flush_meta(metadata_info).await?;
            crate::metrics::FLUSH_DURATION
                .with_label_values(&["save_files"])
                .observe(sw.lap().as_secs_f64());

            // clear flushing files
            self.clear_flushing_files().await;
            crate::metrics::FLUSH_DURATION
                .with_label_values(&["clear_temp_files"])
                .observe(sw.lap().as_secs_f64());
            file_size_vec
                .iter()
                .for_each(|(size, _)| crate::metrics::FLUSH_FILE_SIZE.observe(*size as _));
            info!(#"FlushManager", "log backup flush done";
                "merged_files" => %file_size_vec.len(),    // the number of the merged files
                "files" => %file_size_vec.iter().map(|(_, v)| v).sum::<usize>(),
                "total_size" => %file_size_vec.iter().map(|(v, _)| v).sum::<u64>(), // the size of the merged files after compressed
                "take" => ?begin.saturating_elapsed(),
            );
            Result::Ok(())
        }
        .await;

        if result.is_err() {
            self.flush_fail_count += 1;
        } else {
            self.flush_fail_count = 0;
        }
        result
    }

    async fn flush_by_chunks(
        &self,
        metadata: &mut MetadataInfo,
        files: &[FlushingFile],
        is_meta: bool,
    ) -> Result<()> {
        let mut batch_size = 0;
        // file[batch_begin_index, i) is a batch
        let mut batch_begin_index = 0;
        // TODO: upload the merged file concurrently,
        // then collect merged_file_infos and push them into `metadata`.
        for i in 0..files.len() {
            if batch_size >= self.merged_file_size_limit {
                self.flush_files(&files[batch_begin_index..i], metadata, is_meta)
                    .await?;

                batch_begin_index = i;
                batch_size = 0;
            }

            batch_size += files[i].info.length;
        }
        if batch_begin_index < files.len() {
            self.flush_files(&files[batch_begin_index..], metadata, is_meta)
                .await?;
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn clear_flushing_files(&mut self) {
        for file in self
            .flushing_files
            .drain(..)
            .chain(self.flushing_meta_files.drain(..))
        {
            let data_file = file.fd;
            if !self.temp_file_pool.remove(data_file.inner.path()) {
                warn!(#"FlushManager", "Trying to remove file not exists."; "file" => %data_file.inner.path().display());
            }
        }
    }

    #[instrument(skip_all)]
    async fn flush_files(
        &self,
        files: &[FlushingFile],
        metadata: &mut MetadataInfo,
        is_meta: bool,
    ) -> Result<()> {
        let mut data_files_open = Vec::new();
        let mut data_file_infos = Vec::new();
        let mut merged_file_info = DataFileGroup::new();
        let mut stat_length = 0;
        let mut max_ts: Option<u64> = None;
        let mut min_ts: Option<u64> = None;
        let mut min_resolved_ts: Option<u64> = None;
        for f in files {
            let mut file_info_clone = f.info.to_owned();
            // Update offset of file_info(DataFileInfo)
            //  and push it into merged_file_info(DataFileGroup).
            file_info_clone.set_range_offset(stat_length);
            data_files_open.push({
                let file = self
                    .temp_file_pool
                    .open_raw_for_read(f.fd.inner.path())
                    .context(format_args!(
                        "failed to open read file {:?}",
                        f.fd.inner.path()
                    ))?;
                let compress_length = file.len().await?;
                stat_length += compress_length;
                file_info_clone.set_range_length(compress_length);
                file
            });
            data_file_infos.push(file_info_clone);

            let rts = f.info.resolved_ts;
            min_resolved_ts = min_resolved_ts.map_or(Some(rts), |r| Some(r.min(rts)));
            min_ts = min_ts.map_or(Some(f.info.min_ts), |ts| Some(ts.min(f.info.min_ts)));
            max_ts = max_ts.map_or(Some(f.info.max_ts), |ts| Some(ts.max(f.info.max_ts)));
        }
        let min_ts = min_ts.unwrap_or_default();
        let max_ts = max_ts.unwrap_or_default();
        merged_file_info.set_path(file_name(metadata.store_id, min_ts, max_ts, is_meta));
        merged_file_info.set_data_files_info(data_file_infos.into());
        merged_file_info.set_length(stat_length);
        merged_file_info.set_max_ts(max_ts);
        merged_file_info.set_min_ts(min_ts);
        merged_file_info.set_min_resolved_ts(min_resolved_ts.unwrap_or_default());

        // to do: limiter to storage
        let limiter = Limiter::new(std::f64::INFINITY);

        let files_reader = FilesReader::new(data_files_open);

        let reader = UnpinReader(Box::new(limiter.limit(files_reader.compat())));
        let filepath = &merged_file_info.path;

        let ret = self.storage.write(filepath, reader, stat_length).await;

        match ret {
            Ok(_) => {
                debug!(
                    #"FlushManager","backup stream flush success";
                    "storage_file" => ?filepath,
                    "est_len" => ?stat_length,
                );
            }
            Err(e) => {
                warn!(#"FlushManager","backup stream flush failed";
                    "est_len" => ?stat_length,
                    "err" => ?e,
                );
                return Err(Error::Io(e));
            }
        }

        // push merged file into metadata
        metadata.push(merged_file_info);
        Ok(())
    }
}

fn file_name(store_id: u64, min_ts: u64, max_ts: u64, is_meta: bool) -> String {
    if is_meta {
        path_to_schema_file(store_id, min_ts, max_ts)
    } else {
        path_to_log_file(store_id, min_ts, max_ts)
    }
}

/// path_to_log_file specifies the path of record log for v2.
/// ```text
/// V1: v1/${date}/${hour}/${store_id}/t00000071/434098800931373064-f0251bd5-1441-499a-8f53-adc0d1057a73.log
/// V2: v1/${date}/${hour}/${store_id}/434098800931373064-f0251bd5-1441-499a-8f53-adc0d1057a73.log
/// ```
/// For v2, we merged the small files (partition by table_id) into one file.
fn path_to_log_file(store_id: u64, min_ts: u64, max_ts: u64) -> String {
    format!(
        "v1/{}/{}/{}/{}-{}.log",
        // We may delete a range of files, so using the max_ts for preventing remove some
        // records wrong.
        format_date_time(max_ts, FormatType::Date),
        format_date_time(max_ts, FormatType::Hour),
        store_id,
        min_ts,
        uuid::Uuid::new_v4()
    )
}

/// path_to_schema_file specifies the path of schema log for v2.
/// ```text
/// V1: v1/${date}/${hour}/${store_id}/schema-meta/434055683656384515-cc3cb7a3-e03b-4434-ab6c-907656fddf67.log
/// V2: v1/${date}/${hour}/${store_id}/schema-meta/434055683656384515-cc3cb7a3-e03b-4434-ab6c-907656fddf67.log
/// ```
/// For v2, we merged the small files (partition by table_id) into one file.
fn path_to_schema_file(store_id: u64, min_ts: u64, max_ts: u64) -> String {
    format!(
        "v1/{}/{}/{}/schema-meta/{}-{}.log",
        format_date_time(max_ts, FormatType::Date),
        format_date_time(max_ts, FormatType::Hour),
        store_id,
        min_ts,
        uuid::Uuid::new_v4(),
    )
}

fn format_date_time(ts: u64, t: FormatType) -> impl Display {
    use chrono::prelude::*;
    let millis = TimeStamp::physical(ts.into());
    let dt = Utc.timestamp_millis(millis as _);
    match t {
        FormatType::Date => dt.format("%Y%m%d"),
        FormatType::Hour => dt.format("%H"),
    }
}

#[test]
mod test {}
