// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
pub mod hooks;

#[cfg(test)]
mod test;

use std::{borrow::Cow, path::Path, sync::Arc};

use engine_rocks::RocksEngine;
pub use engine_traits::SstCompressionType;
use engine_traits::SstExt;
use external_storage::{BackendConfig, IterableExternalStorage};
use futures::stream::{self, StreamExt};
use hooks::{
    AfterFinishCtx, BeforeStartCtx, CId, ExecHooks, SubcompactionFinishCtx, SubcompactionStartCtx,
};
use kvproto::brpb::StorageBackend;
use tikv_util::config::ReadableSize;
use tokio::runtime::Handle;
use tracing::{trace_span, Instrument};
use tracing_active_tree::{frame, root};

use super::{
    compaction::{
        collector::{CollectSubcompaction, CollectSubcompactionConfig},
        exec::{SubcompactExt, SubcompactionExec},
    },
    storage::{LoadFromExt, StreamyMetaStorage},
};
use crate::{
    compaction::{exec::SubcompactionExecArg, SubcompactionResult},
    errors::{Result, TraceResultExt},
    util,
};

/// The config for an execution of a compaction.
///
/// This structure itself fully defines what work the compaction need to do.
/// That is, keeping this structure unchanged, the compaction should always
/// generate the same artifices.
pub struct ExecutionConfig {
    /// Filter out files doesn't contain any record with TS great or equal than
    /// this.
    pub from_ts: u64,
    /// Filter out files doesn't contain any record with TS less than this.
    pub until_ts: u64,
    /// The compress algorithm we are going to use for output.
    pub compression: SstCompressionType,
    /// The compress level we are going to use.
    ///
    /// If `None`, we will use the default level of the selected algorithm.
    pub compression_level: Option<i32>,
}

impl ExecutionConfig {
    /// Create a suitable (but not forced) prefix for the artifices of the
    /// compaction.
    ///
    /// You may specify a `name`, which will be included in the path, then the
    /// compaction will be easier to be found.
    pub fn recommended_prefix(&self, name: &str) -> String {
        let mut hasher = crc64fast::Digest::new();
        hasher.write(name.as_bytes());
        hasher.write(&self.from_ts.to_le_bytes());
        hasher.write(&self.until_ts.to_le_bytes());
        hasher.write(&util::compression_type_to_u8(self.compression).to_le_bytes());
        hasher.write(&self.compression_level.unwrap_or(0).to_le_bytes());

        format!("{}_{}", name, util::aligned_u64(hasher.sum64()))
    }
}

/// An execution of compaction.
pub struct Execution<DB: SstExt = RocksEngine> {
    /// The configuration.
    pub cfg: ExecutionConfig,

    /// Max subcompactions can be executed concurrently.
    pub max_concurrent_subcompaction: u64,
    /// The external storage for input and output.
    pub external_storage: StorageBackend,
    /// The RocksDB instance for generating SST.
    pub db: Option<DB>,
    /// The prefix of the artifices.
    pub out_prefix: String,
}

impl Execution {
    fn gen_name(&self) -> String {
        let compaction_name = Path::new(&self.out_prefix)
            .file_name()
            .map(|v| v.to_string_lossy())
            .unwrap_or(Cow::Borrowed("unknown"));
        let pid = tikv_util::sys::thread::thread_id();
        let hostname = tikv_util::sys::hostname();
        format!(
            "{}#{}@{}",
            compaction_name,
            pid,
            hostname.as_deref().unwrap_or("unknown")
        )
    }

    pub fn run(self, mut hooks: impl ExecHooks) -> Result<()> {
        let storage = external_storage::create_iterable_storage(
            &self.external_storage,
            BackendConfig::default(),
        )?;
        let storage: Arc<dyn IterableExternalStorage> = Arc::from(storage);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let all_works = async move {
            let mut ext = LoadFromExt::default();
            let next_compaction = trace_span!("next_compaction");
            ext.max_concurrent_fetch = 128;
            ext.loading_content_span = Some(trace_span!(
                parent: next_compaction.clone(),
                "load_meta_file_names"
            ));

            let cx = BeforeStartCtx {
                storage: storage.as_ref(),
                async_rt: &tokio::runtime::Handle::current(),
                this: &self,
            };
            hooks.before_execution_started(cx).await?;
            let meta = StreamyMetaStorage::load_from_ext(storage.as_ref(), ext);
            let stream = meta.flat_map(|file| match file {
                Ok(file) => stream::iter(file.into_logs()).map(Ok).left_stream(),
                Err(err) => stream::once(futures::future::err(err)).right_stream(),
            });
            let mut compact_stream = CollectSubcompaction::new(
                stream,
                CollectSubcompactionConfig {
                    compact_from_ts: self.cfg.from_ts,
                    compact_to_ts: self.cfg.until_ts,
                    subcompaction_size_threshold: ReadableSize::mb(128).0,
                },
            );
            let mut pending = Vec::new();
            let mut id = 0;

            while let Some(c) = compact_stream
                .next()
                .instrument(next_compaction.clone())
                .await
            {
                let cstat = compact_stream.take_statistic();
                let lstat = compact_stream.get_mut().get_mut().take_statistic();

                let c = c?;
                let cid = CId(id);
                let cx = SubcompactionStartCtx {
                    subc: &c,
                    load_stat_diff: &lstat,
                    collect_compaction_stat_diff: &cstat,
                };
                hooks.before_a_subcompaction_start(cid, cx);

                id += 1;

                let compact_args = SubcompactionExecArg {
                    out_prefix: Some(Path::new(&self.out_prefix).to_owned()),
                    db: self.db.clone(),
                    storage: Arc::clone(&storage) as _,
                };
                let compact_worker = SubcompactionExec::from(compact_args);
                let compact_work = async move {
                    let mut ext = SubcompactExt::default();
                    ext.max_load_concurrency = 32;
                    ext.compression = self.cfg.compression;
                    ext.compression_level = self.cfg.compression_level;
                    let res = compact_worker.run(c, ext).await.trace_err()?;
                    res.verify_checksum()
                        .annotate(format_args!("the compaction is {:?}", res.origin))?;
                    Result::Ok((res, cid))
                };
                let join_handle = tokio::spawn(root!(compact_work));
                pending.push(join_handle);

                if pending.len() >= self.max_concurrent_subcompaction as _ {
                    let join = util::select_vec(&mut pending);
                    let (cres, cid) = frame!("wait_for_compaction"; join).await.unwrap()?;
                    self.on_compaction_finish(cid, &cres, storage.as_ref(), &mut hooks)
                        .await?;
                }
            }
            drop(next_compaction);

            for join in pending {
                let (cres, cid) = frame!("final_wait"; join).await.unwrap()?;
                self.on_compaction_finish(cid, &cres, storage.as_ref(), &mut hooks)
                    .await?;
            }
            let cx = AfterFinishCtx {
                async_rt: &Handle::current(),
                external_storage: storage.as_ref(),
            };
            hooks.after_execution_finished(cx).await?;

            Result::Ok(())
        };
        runtime.block_on(frame!(all_works))
    }

    async fn on_compaction_finish(
        &self,
        cid: CId,
        result: &SubcompactionResult,
        external_storage: &dyn IterableExternalStorage,
        hooks: &mut impl ExecHooks,
    ) -> Result<()> {
        let cx = SubcompactionFinishCtx {
            this: &self,
            external_storage,
            result,
        };
        hooks.after_a_subcompaction_end(cid, cx).await?;
        Result::Ok(())
    }
}