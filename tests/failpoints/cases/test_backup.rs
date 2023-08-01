// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{thread, time::Duration};

use engine_rocks::raw::CompactOptions;
use engine_traits::CF_WRITE;
use external_storage_export::make_local_backend;
use futures::{executor::block_on, StreamExt};
use kvproto::{
    brpb::{BackupMode, Error_oneof_detail},
    kvrpcpb::*,
};
use tempfile::{Builder, TempDir};
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

#[test]
fn test_file_based_backup_compaction() {
    let mut suite = TestSuite::new(3, 144 * 1024 * 1024, ApiVersion::V1);
    suite.must_kv_put(1_000, 4);
    let ts = suite.alloc_ts();
    let tmp = TempDir::new().unwrap();
    fail::cfg("file_backup_save_sst_metadata", "pause").unwrap();
    let resp = suite.backup_with(|req| {
        req.set_end_version(ts.into_inner());
        req.set_mode(BackupMode::File);
        req.set_storage_backend(make_local_backend(tmp.path()));
    });
    for eng in suite.cluster.engines.values() {
        let cf = eng.kv.as_inner().cf_handle(CF_WRITE).unwrap();
        eng.kv.as_inner().compact_range_cf(cf, None, None);
    }
    fail::remove("file_backup_save_sst_metadata");

    let resp = block_on(resp.collect::<Vec<_>>());
    println!("{:?}", resp);
    assert!(!resp[0].has_error());
    suite.stop();
}
