use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use engine_rocks::RocksEngine;
use futures::future::FutureExt;
use kvproto::brpb::StorageBackend;
use tokio::sync::mpsc::Sender;

use super::{checkpoint::Checkpoint, hooks::SaveMeta, Execution, ExecutionConfig};
use crate::{
    compaction::SubcompactionResult,
    errors::OtherErrExt,
    execute::hooks::ExecHooks,
    test_util::{gen_step, CompactInMem, KvGen, LogFileBuilder, TmpStorage},
};

#[derive(Clone)]
struct CompactionSpy(Sender<SubcompactionResult>);

impl ExecHooks for CompactionSpy {
    async fn after_a_subcompaction_end(
        &mut self,
        _cid: super::hooks::CId,
        res: super::hooks::SubcompactionFinishCtx<'_>,
    ) -> crate::Result<()> {
        self.0
            .send(res.result.clone())
            .map(|res| res.adapt_err())
            .await
    }
}

fn gen_builder(cm: &mut HashMap<usize, CompactInMem>, batch: i64, num: i64) -> Vec<LogFileBuilder> {
    let mut result = vec![];
    for (n, i) in (num * batch..num * (batch + 1)).enumerate() {
        let it = cm
            .entry(n)
            .or_default()
            .tap_on(KvGen::new(gen_step(1, i, num), move |_| {
                vec![42u8; (n + 1) * 12]
            }))
            .take(200);
        let mut b = LogFileBuilder::new(|v| v.region_id = n as u64);
        for kv in it {
            b.add_encoded(&kv.key, &kv.value)
        }
        result.push(b);
    }
    result
}

pub fn create_compaction(st: StorageBackend) -> Execution {
    Execution::<RocksEngine> {
        cfg: ExecutionConfig {
            from_ts: 0,
            until_ts: u64::MAX,
            compression: engine_traits::SstCompressionType::Lz4,
            compression_level: None,
        },
        max_concurrent_subcompaction: 3,
        external_storage: st,
        db: None,
        out_prefix: "test-output".to_owned(),
    }
}

#[tokio::test]
async fn test_exec_simple() {
    let st = TmpStorage::create();
    let mut cm = HashMap::new();

    for i in 0..3 {
        st.build_flush(
            &format!("{}.log", i),
            &format!("v1/backupmeta/{}.meta", i),
            gen_builder(&mut cm, i, 10),
        )
        .await;
    }

    let exec = create_compaction(st.backend());

    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let bg_exec = tokio::task::spawn_blocking(move || {
        exec.run((SaveMeta::default(), CompactionSpy(tx))).unwrap()
    });
    while let Some(item) = rx.recv().await {
        let rid = item.meta.get_meta().get_region_id() as usize;
        st.verify_result(item, cm.remove(&rid).unwrap());
    }
    bg_exec.await.unwrap();

    let mut migs = st.load_migrations().await.unwrap();
    assert_eq!(migs.len(), 1);
    let (id, mig) = migs.pop().unwrap();
    assert_eq!(id, 1);
    assert_eq!(mig.edit_meta.len(), 3);
    assert_eq!(mig.compactions.len(), 1);
    let subc = st
        .load_subcompactions(mig.compactions[0].get_artifacts())
        .await
        .unwrap();
    assert_eq!(subc.len(), 10);
}

#[tokio::test]
async fn test_checkpointing() {
    let st = TmpStorage::create();
    let mut cm = HashMap::new();
    test_util::init_log_for_test();

    for i in 0..3 {
        st.build_flush(
            &format!("{}.log", i),
            &format!("v1/backupmeta/{}.meta", i),
            gen_builder(&mut cm, i, 15),
        )
        .await;
    }

    #[derive(Clone)]
    struct AbortEvery3TimesAndRecordFinishCount(Arc<AtomicU64>);

    const ERR_MSG: &str = "nameless you. back to where you from";

    impl ExecHooks for AbortEvery3TimesAndRecordFinishCount {
        async fn after_a_subcompaction_end(
            &mut self,
            cid: crate::execute::hooks::CId,
            _res: crate::execute::hooks::SubcompactionFinishCtx<'_>,
        ) -> crate::Result<()> {
            if cid.0 == 4 {
                Err(crate::ErrorKind::Other(ERR_MSG.to_owned()).into())
            } else {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
    }

    let be = st.backend();
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let cnt = Arc::new(AtomicU64::default());
    let cloneable_hooks = (
        AbortEvery3TimesAndRecordFinishCount(cnt.clone()),
        CompactionSpy(tx),
    );
    let hooks = move || {
        (
            (SaveMeta::default(), Checkpoint::default()),
            cloneable_hooks.clone(),
        )
    };
    let bg_exec = tokio::task::spawn_blocking(move || {
        while let Err(err) = create_compaction(be.clone()).run(hooks()) {
            if !err.kind.to_string().contains(&ERR_MSG) {
                return Err(err);
            }
        }
        Ok(())
    });

    while let Some(item) = rx.recv().await {
        let rid = item.meta.get_meta().get_region_id() as usize;
        st.verify_result(item, cm.remove(&rid).unwrap());
    }
    bg_exec.await.unwrap().unwrap();

    let mut migs = st.load_migrations().await.unwrap();
    assert_eq!(migs.len(), 1);
    let (id, mig) = migs.pop().unwrap();
    assert_eq!(id, 1);
    assert_eq!(mig.edit_meta.len(), 3);
    assert_eq!(mig.compactions.len(), 1);
    let subc = st
        .load_subcompactions(mig.compactions[0].get_artifacts())
        .await
        .unwrap();
    assert_eq!(subc.len(), 15);
    assert_eq!(cnt.load(Ordering::SeqCst), 15);
}
