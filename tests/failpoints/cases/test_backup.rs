// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{thread, time::Duration};

use futures::{executor::block_on, StreamExt};
use kvproto::{brpb::Error_oneof_detail, kvrpcpb::*};
use tempfile::Builder;
use test_backup::*;
use txn_types::TimeStamp;

#[test]
fn backup_blocked_by_memory_lock() {
    let suite = TestSuite::new(1, 144 * 1024 * 1024, ApiVersion::V1);

    fail::cfg("raftkv_async_write_finish", "pause").unwrap();
    let tikv_cli = suite.tikv_cli.clone();
    let (k, v) = (b"my_key", b"my_value");
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.to_vec();
    mutation.value = v.to_vec();
    let mut prewrite_req = PrewriteRequest::default();
    prewrite_req.set_context(suite.context.clone());
    prewrite_req.mut_mutations().push(mutation);
    prewrite_req.set_primary_lock(k.to_vec());
    prewrite_req.set_start_version(20);
    prewrite_req.set_lock_ttl(2000);
    prewrite_req.set_use_async_commit(true);
    let th = thread::spawn(move || tikv_cli.kv_prewrite(&prewrite_req).unwrap());

    thread::sleep(Duration::from_millis(200));

    // Trigger backup request.
    let tmp = Builder::new().tempdir().unwrap();
    let backup_ts = TimeStamp::from(21);
    let storage_path = make_unique_dir(tmp.path());
    let rx = suite.backup(
        b"a".to_vec(), // start
        b"z".to_vec(), // end
        0.into(),      // begin_ts
        backup_ts,
        &storage_path,
    );
    let resp = block_on(rx.collect::<Vec<_>>());
    match &resp[0].get_error().detail {
        Some(Error_oneof_detail::KvError(key_error)) => {
            assert!(key_error.has_locked());
        }
        _ => panic!("unexpected response"),
    }

    fail::remove("raftkv_async_write_finish");
    th.join().unwrap();

    suite.stop();
}

mod disk_snap {
    use std::time::Duration;

    use test_backup::disk_snap::{assert_success, Suite};

    #[test]
    fn test_merge() {
        let mut suite = Suite::new(1);
        suite.split(b"k");
        let mut source = suite.cluster.get_region(b"a");
        let target = suite.cluster.get_region(b"z");
        assert_ne!(source.id, target.id);
        fail::cfg("on_schedule_merge", "pause").unwrap();
        let resp = suite.cluster.try_merge(source.id, target.id);
        assert_success(&resp);
        let mut call = suite.prepare_backup(1);
        call.prepare(60);
        fail::remove("on_schedule_merge");
        // Manually "apply" the prepare merge on region epoch.
        source.mut_region_epoch().set_conf_ver(2);
        source.mut_region_epoch().set_version(3);
        call.wait_apply([source, target].iter().cloned());
        let source = suite.cluster.get_region(b"a");
        let target = suite.cluster.get_region(b"z");
        assert_ne!(source.id, target.id);
        suite.nodes[&1].rejector.reset();
        test_util::eventually(Duration::from_secs(1), Duration::from_secs(10), || {
            let source = suite.cluster.get_region(b"a");
            let target = suite.cluster.get_region(b"z");
            source.id == target.id
        })
    }
}
