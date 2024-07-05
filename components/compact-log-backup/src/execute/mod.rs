pub mod hooks;

use std::{
    borrow::Cow,
    collections::VecDeque,
    path::Path,
    sync::{Arc, Mutex},
};

use engine_rocks::RocksEngine;
pub use engine_traits::SstCompressionType;
use engine_traits::SstExt;
use external_storage::{BackendConfig, FullFeaturedStorage};
use futures::stream::{self, StreamExt};
use hooks::{AfterFinishCtx, BeforeStartCtx, CId, CompactionFinishCtx, ExecHooks};
use kvproto::brpb::StorageBackend;
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
    compaction::{exec::SingleCompactionArg, SubcompactionResult},
    errors::{Result, TraceResultExt},
    util,
};

pub struct ExecutionConfig {
    pub from_ts: u64,
    pub until_ts: u64,
    pub compression: SstCompressionType,
    pub compression_level: Option<i32>,
}

impl ExecutionConfig {
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

pub struct Execution<DB: SstExt = RocksEngine> {
    pub cfg: ExecutionConfig,

    pub max_concurrent_compaction: u64,
    pub external_storage: StorageBackend,
    pub db: Option<DB>,
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
        let storage = external_storage::create_full_featured_storage(
            &self.external_storage,
            BackendConfig::default(),
        )?;
        let storage: Arc<dyn FullFeaturedStorage> = Arc::from(storage);
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

            let total = StreamyMetaStorage::count_objects(storage.as_ref()).await?;
            let cx = BeforeStartCtx {
                est_meta_size: total,
                async_rt: &tokio::runtime::Handle::current(),
                this: &self,
            };
            hooks.before_execution_started(cx);
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
                },
            );
            let mut pending = VecDeque::new();
            let mut id = 0;

            while let Some(c) = compact_stream
                .next()
                .instrument(next_compaction.clone())
                .await
            {
                hooks.update_collect_compaction_stat(&compact_stream.take_statistic());
                hooks.update_load_meta_stat(&compact_stream.get_mut().get_mut().take_statistic());

                let c = c?;
                let cid = CId(id);
                hooks.before_a_compaction_start(cid, &c);

                id += 1;

                let compact_args = SingleCompactionArg {
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
                    Result::Ok(res)
                };
                let join_handle = tokio::spawn(root!(compact_work));
                pending.push_back((join_handle, cid));

                if pending.len() >= self.max_concurrent_compaction as _ {
                    let (join, cid) = pending.pop_front().unwrap();
                    let cres = frame!("wait_for_compaction"; join).await.unwrap()?;
                    self.on_compaction_finish(cid, &cres, storage.as_ref(), &mut hooks)
                        .await?;
                }
            }
            drop(next_compaction);

            hooks.update_collect_compaction_stat(&compact_stream.take_statistic());

            for (join, cid) in pending {
                let cres = frame!("final_wait"; join).await.unwrap()?;
                self.on_compaction_finish(cid, &cres, storage.as_ref(), &mut hooks)
                    .await?;
            }
            let mut cx = AfterFinishCtx {
                async_rt: Handle::current(),
                comments: String::new(),
                external_storage: storage.as_ref(),
            };
            hooks.after_execution_finished(&mut cx).await?;

            Result::Ok(())
        };
        runtime.block_on(frame!(all_works))
    }

    async fn on_compaction_finish(
        &self,
        cid: CId,
        result: &SubcompactionResult,
        external_storage: &dyn FullFeaturedStorage,
        hooks: &mut impl ExecHooks,
    ) -> Result<()> {
        let mut cx = CompactionFinishCtx {
            this: &self,
            external_storage,
            result,
        };
        hooks.after_a_compaction_end(cid, &mut cx).await?;
        Result::Ok(())
    }
}
