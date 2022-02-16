// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg(test)]

use std::{
    collections::{HashMap, HashSet},
    iter::FromIterator,
    path::Path,
    sync::Arc,
};

use br_stream::{
    metadata::{store::SlashEtcStore, MetadataClient, StreamTask},
    observer::BackupStreamObserver,
    Endpoint, Task,
};
use grpcio::ChannelBuilder;
use kvproto::{brpb::Local, tikvpb::*};
use kvproto::{brpb::StorageBackend, kvrpcpb::*};
use tempdir::TempDir;
use test_raftstore::{new_server_cluster, Cluster, ServerCluster};
use tikv::config::BackupStreamConfig;
use tikv_util::{
    codec::{
        number::NumberEncoder,
        stream_event::{EventIterator, Iterator},
    },
    worker::LazyWorker,
    HandyRwLock,
};
use txn_types::{Key, TimeStamp, WriteRef};
use walkdir::WalkDir;

fn mutation(k: Vec<u8>, v: Vec<u8>) -> Mutation {
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k;
    mutation.value = v;
    mutation
}

fn make_table_key(table_id: i64, key: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut table_key = b"t".to_vec();
    // make it comparable to uint.
    table_key
        .encode_u64(table_id as u64 ^ 0x8000_0000_0000_0000)
        .unwrap();
    Write::write_all(&mut table_key, key).unwrap();
    table_key
}

fn make_record_key(table_id: i64, handle: u64) -> Vec<u8> {
    let mut record = b"_r".to_vec();
    record.encode_u64(handle ^ 0x8000_0000_0000_0000).unwrap();
    make_table_key(table_id, &record)
}

fn make_encoded_record_key(table_id: i64, handle: u64, ts: u64) -> Vec<u8> {
    let key = Key::from_raw(&make_record_key(table_id, handle));
    key.append_ts(TimeStamp::new(ts)).into_encoded()
}

pub struct Suite {
    endpoints: HashMap<u64, LazyWorker<Task>>,
    meta_store: SlashEtcStore,
    cluster: Cluster<ServerCluster>,
    tikv_cli: HashMap<u64, TikvClient>,
    obs: HashMap<u64, BackupStreamObserver>,
    env: Arc<grpcio::Environment>,

    temp_files: TempDir,
    flushed_files: TempDir,
}

impl Suite {
    pub fn simple_task(&self, name: &str) -> StreamTask {
        let mut task = StreamTask::default();
        task.info.set_name(name.to_owned());
        task.info.set_start_ts(0);
        task.info.set_end_ts(1000);
        let mut storage = StorageBackend::new();
        let mut local = Local::new();
        local.path = self.flushed_files.path().display().to_string();
        storage.set_local(local);
        task.info.set_storage(storage);
        task.info.set_table_filter(vec!["*.*".to_owned()].into());
        task
    }

    fn start_br_stream_on(&mut self, id: u64) -> LazyWorker<Task> {
        let cluster = &mut self.cluster;
        let worker = LazyWorker::new(format!("br-{}", id));
        let mut s = cluster.sim.wl();

        let ob = BackupStreamObserver::new(worker.scheduler());
        let ob2 = ob.clone();
        s.coprocessor_hooks
            .entry(id)
            .or_default()
            .push(Box::new(move |host| {
                ob.register_to(host);
            }));
        self.obs.insert(id, ob2);
        worker
    }

    fn start_endpoint(&mut self, id: u64) {
        let cluster = &mut self.cluster;
        let worker = self.endpoints.get_mut(&id).unwrap();
        let sim = cluster.sim.wl();
        let raft_router = sim.get_server_router(id);
        let regions = sim.region_info_accessors.get(&id).unwrap().clone();
        let mut cfg = BackupStreamConfig::default();
        cfg.enable_streaming = true;
        cfg.streaming_path = format!("/{}/{}", self.temp_files.path().display(), id);
        let ob = self.obs.get(&id).unwrap().clone();
        let endpoint = Endpoint::with_client(
            id,
            cfg,
            worker.scheduler(),
            ob,
            regions,
            raft_router,
            MetadataClient::new(self.meta_store.clone(), id),
        );
        worker.start(endpoint);
    }

    pub fn new(n: usize) -> Self {
        let cluster = new_server_cluster(42, n);
        let mut suite = Self {
            endpoints: Default::default(),
            meta_store: Default::default(),
            obs: Default::default(),
            tikv_cli: Default::default(),
            env: Arc::new(grpcio::Environment::new(1)),
            cluster,

            temp_files: TempDir::new("temp").unwrap(),
            flushed_files: TempDir::new("flush").unwrap(),
        };
        for id in 1..=(n as u64) {
            let worker = suite.start_br_stream_on(id);
            suite.endpoints.insert(id, worker);
        }
        suite.cluster.run();
        for id in 1..=(n as u64) {
            suite.start_endpoint(id);
        }
        suite
    }

    fn get_meta_cli(&self) -> MetadataClient<SlashEtcStore> {
        MetadataClient::new(self.meta_store.clone(), 0)
    }

    fn write_records(&mut self, from: usize, n: usize, for_table: i64) {
        for ts in (from..(from + n)).map(|x| x * 2) {
            let ts = ts as u64;
            let key = make_record_key(for_table, ts);
            let muts = vec![mutation(key.clone(), b"hello, world".to_vec())];
            let region = self.cluster.get_region_id(&key);
            self.must_kv_prewrite(region, muts, key.clone(), TimeStamp::new(ts));
            self.must_kv_commit(
                region,
                vec![key.clone()],
                TimeStamp::new(ts),
                TimeStamp::new(ts + 1),
            );
        }
    }

    fn check_for_write_records(&self, n: u64, for_table: i64, path: &Path) {
        let mut remain_keys: HashSet<Vec<u8>> = HashSet::from_iter(
            (0..n)
                .map(|x| x * 2)
                .map(|n| make_encoded_record_key(for_table, n, n + 1)),
        );
        for entry in WalkDir::new(path) {
            let entry = entry.unwrap();
            println!("checking: {:?}", entry);
            if entry.file_type().is_file()
                && entry
                    .file_name()
                    .to_str()
                    .map_or(false, |s| s.ends_with(".log"))
            {
                let content = std::fs::read(entry.path()).unwrap();
                let mut iter = EventIterator::new(content);
                loop {
                    iter.next().unwrap();
                    if !iter.valid() {
                        break;
                    }
                    assert!(
                        remain_keys.remove(iter.key()),
                        "key {:?} not found, maybe recorded twice or lost",
                        iter.key(),
                    );

                    let value = iter.value();
                    let wf = WriteRef::parse(value).unwrap();
                    assert_eq!(wf.short_value, Some(b"hello, world" as &[u8]));
                }
            }
        }

        if !remain_keys.is_empty() {
            panic!(
                "not all keys are recorded: it remains {:?} (total = {})",
                remain_keys.iter().take(3).collect::<Vec<_>>(),
                remain_keys.len()
            );
        }
    }
}

// Copy & Paste from cdc::tests::TestSuite, maybe make it a mixin?
impl Suite {
    pub fn must_kv_prewrite(
        &mut self,
        region_id: u64,
        muts: Vec<Mutation>,
        pk: Vec<u8>,
        ts: TimeStamp,
    ) {
        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(self.get_context(region_id));
        prewrite_req.set_mutations(muts.into());
        prewrite_req.primary_lock = pk;
        prewrite_req.start_version = ts.into_inner();
        prewrite_req.lock_ttl = prewrite_req.start_version + 1;
        let prewrite_resp = self
            .get_tikv_client(region_id)
            .kv_prewrite(&prewrite_req)
            .unwrap();
        assert!(
            !prewrite_resp.has_region_error(),
            "{:?}",
            prewrite_resp.get_region_error()
        );
        assert!(
            prewrite_resp.errors.is_empty(),
            "{:?}",
            prewrite_resp.get_errors()
        );
    }

    pub fn must_kv_commit(
        &mut self,
        region_id: u64,
        keys: Vec<Vec<u8>>,
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
    ) {
        let mut commit_req = CommitRequest::default();
        commit_req.set_context(self.get_context(region_id));
        commit_req.start_version = start_ts.into_inner();
        commit_req.set_keys(keys.into_iter().collect());
        commit_req.commit_version = commit_ts.into_inner();
        let commit_resp = self
            .get_tikv_client(region_id)
            .kv_commit(&commit_req)
            .unwrap();
        assert!(
            !commit_resp.has_region_error(),
            "{:?}",
            commit_resp.get_region_error()
        );
        assert!(!commit_resp.has_error(), "{:?}", commit_resp.get_error());
    }

    pub fn get_context(&mut self, region_id: u64) -> Context {
        let epoch = self.cluster.get_region_epoch(region_id);
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        let mut context = Context::default();
        context.set_region_id(region_id);
        context.set_peer(leader);
        context.set_region_epoch(epoch);
        context
    }

    pub fn get_tikv_client(&mut self, region_id: u64) -> &TikvClient {
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        let store_id = leader.get_store_id();
        let addr = self.cluster.sim.rl().get_addr(store_id);
        let env = self.env.clone();
        self.tikv_cli
            .entry(leader.get_store_id())
            .or_insert_with(|| {
                let channel = ChannelBuilder::new(env).connect(&addr);
                TikvClient::new(channel)
            })
    }
}

mod test {
    use std::time::Duration;

    use br_stream::Task;

    use crate::make_table_key;

    #[test]
    fn basic() {
        let mut suite = super::Suite::new(4);
        let meta_cli = suite.get_meta_cli();
        let rt = tokio::runtime::Runtime::new().unwrap();

        // write data before the task starting, for testing incremental scanning.
        suite.write_records(0, 128, 1);
        rt.block_on(meta_cli.insert_task_with_range(
            &suite.simple_task("test"),
            &[(&make_table_key(1, b""), &make_table_key(2, b""))],
        ))
        .unwrap();
        suite.write_records(128, 128, 1);

        suite.check_for_write_records(255, 1, suite.temp_files.path());

        for worker in suite.endpoints.values() {
            worker
                .scheduler()
                .schedule(Task::Flush("test".to_owned()))
                .unwrap();
        }
        std::thread::sleep(Duration::from_secs(4));
        suite.check_for_write_records(255, 1, suite.flushed_files.path());

        suite.cluster.shutdown();
    }
}
