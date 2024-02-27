// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use core::num;
use std::{collections::HashMap, fs, path::Path, process::Output, sync::Arc};

use engine_rocks::{
    raw::{DBEntryType, Env, TablePropertiesCollector, TablePropertiesCollectorFactory},
    util::new_engine_opt,
    RocksCfOptions, RocksDbOptions, RocksEngine, RocksSstReader, RocksSstWriterBuilder,
};
pub use engine_rocks::{RocksEngine as TestEngine, RocksSstWriter};
use engine_traits::{IterOptions, Iterator, KvEngine, RefIterable, SstWriter, SstWriterBuilder};
use futures::{future::FutureExt, sink::SinkExt};
use kvproto::{import_sstpb::*, kvrpcpb::Context, metapb::Region};
use uuid::Uuid;

pub const PROP_TEST_MARKER_CF_NAME: &[u8] = b"tikv.test_marker_cf_name";

pub fn new_test_engine(path: &str, cfs: &[&str]) -> RocksEngine {
    new_test_engine_with_options(path, cfs, |_, _| {})
}

pub fn new_test_engine_with_env(path: &str, cfs: &[&str], env: Arc<Env>) -> RocksEngine {
    new_test_engine_with_options_and_env(path, cfs, |_, _| {}, Some(env))
}

pub fn new_test_engine_with_options_and_env<F>(
    path: &str,
    cfs: &[&str],
    mut apply: F,
    env: Option<Arc<Env>>,
) -> RocksEngine
where
    F: FnMut(&str, &mut RocksCfOptions),
{
    let cf_opts = cfs
        .iter()
        .map(|cf| {
            let mut opt = RocksCfOptions::default();
            if let Some(ref env) = env {
                opt.set_env(env.clone());
            }
            apply(cf, &mut opt);
            opt.add_table_properties_collector_factory(
                "tikv.test_properties",
                TestPropertiesCollectorFactory::new(*cf),
            );
            (*cf, opt)
        })
        .collect();

    let db_opts = env.map_or_else(RocksDbOptions::default, |e| {
        let mut opts = RocksDbOptions::default();
        opts.set_env(e);
        opts
    });
    new_engine_opt(path, db_opts, cf_opts).expect("rocks test engine")
}

pub fn new_test_engine_with_options<F>(path: &str, cfs: &[&str], apply: F) -> RocksEngine
where
    F: FnMut(&str, &mut RocksCfOptions),
{
    new_test_engine_with_options_and_env(path, cfs, apply, None)
}

pub fn new_sst_reader(path: &str, e: Option<Arc<Env>>) -> RocksSstReader {
    RocksSstReader::open_with_env(path, e).expect("test sst reader")
}

pub fn new_sst_writer(path: &str) -> RocksSstWriter {
    RocksSstWriterBuilder::new()
        .build(path)
        .expect("test writer builder")
}

pub fn calc_data_crc32(data: &[u8]) -> u32 {
    let mut digest = crc32fast::Hasher::new();
    digest.update(data);
    digest.finalize()
}

pub fn check_db_range<E>(db: &E, range: (u8, u8))
where
    E: KvEngine,
{
    for i in range.0..range.1 {
        let k = keys::data_key(&[i]);
        assert_eq!(db.get_value(&k).unwrap().unwrap(), &[i]);
    }
}

pub fn gen_sst_file_by_db<P: AsRef<Path>>(
    path: P,
    range: (u8, u8),
    db: Option<&RocksEngine>,
) -> (SstMeta, Vec<u8>) {
    let mut builder = RocksSstWriterBuilder::new();
    if let Some(db) = db {
        builder = builder.set_db(db);
    }
    let mut w = builder.build(path.as_ref().to_str().unwrap()).unwrap();
    for i in range.0..range.1 {
        let k = keys::data_key(&[i]);
        w.put(&k, &[i]).unwrap();
    }
    w.finish().unwrap();

    read_sst_file(path, (&[range.0], &[range.1]))
}

pub fn gen_sst_file<P: AsRef<Path>>(path: P, range: (u8, u8)) -> (SstMeta, Vec<u8>) {
    gen_sst_file_by_db(path, range, None)
}

pub fn gen_sst_file_with_kvs<P: AsRef<Path>>(
    path: P,
    kvs: &[(&[u8], &[u8])],
) -> (SstMeta, Vec<u8>) {
    let builder = RocksSstWriterBuilder::new();
    let mut w = builder.build(path.as_ref().to_str().unwrap()).unwrap();
    for (k, v) in kvs {
        let dk = keys::data_key(k);
        w.put(&dk, v).unwrap();
    }
    w.finish().unwrap();

    let start_key = kvs[0].0;
    let end_key = keys::next_key(kvs.last().cloned().unwrap().0);
    read_sst_file(path, (start_key, &end_key))
}

pub fn read_sst_file<P: AsRef<Path>>(path: P, range: (&[u8], &[u8])) -> (SstMeta, Vec<u8>) {
    let data = fs::read(path).unwrap();
    let crc32 = calc_data_crc32(&data);

    let mut meta = SstMeta::default();
    meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    meta.mut_range().set_start(range.0.to_vec());
    meta.mut_range().set_end(range.1.to_vec());
    meta.set_crc32(crc32);
    meta.set_length(data.len() as u64);
    meta.set_cf_name("default".to_owned());

    (meta, data)
}

pub async fn call_write(
    cli: &ImportSstClient,
    region: &Region,
    iter: impl std::iter::Iterator<Item = (Vec<u8>, Vec<u8>)>,
) -> SstMeta {
    let mut ctx = Context::new();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(region.get_peers()[0].clone());
    let mut wb = WriteBatch::new();
    let mut first_key = None;
    let mut last_key = None;
    let mut len = 0;
    let mut num_of_entries = 0;
    for (k, v) in iter {
        num_of_entries += 1;
        len += k.len() + v.len();
        if first_key.is_none() {
            first_key = Some(k.clone());
        }
        last_key = Some(k.clone());
        let mut pair = Pair::new();
        pair.set_key(k);
        pair.set_value(v);
        wb.mut_pairs().push(pair);
    }

    let mut meta = SstMeta::new();
    meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    meta.set_region_id(region.get_id());
    meta.set_region_epoch(region.get_region_epoch().clone());
    meta.set_total_kvs(num_of_entries);
    meta.set_total_bytes(len as _);
    meta.set_cf_name("default".to_owned());
    meta.set_range({
        let mut r = Range::new();
        r.set_start(first_key.unwrap_or_default());
        r.set_end(last_key.unwrap_or_default());
        r
    });
    let (mut send, recv) = cli.write().unwrap();
    let mut req = WriteRequest::new();
    req.set_context(ctx);
    req.set_meta(meta);
    send.send((req.clone(), Default::default())).await.unwrap();
    req.clear_meta();
    req.set_batch(wb);
    send.send((req, Default::default())).await.unwrap();
    send.close().await.unwrap();
    let resp = recv.await.unwrap();
    assert_eq!(resp.get_metas().len(), 1);
    assert!(!resp.has_error(), "{:?}", resp.get_error());
    resp.get_metas()[0].clone()
}

pub fn call_ingest(cli: &ImportSstClient, region: &Region, meta: SstMeta) {
    let mut ctx = Context::new();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(region.get_peers()[0].clone());
    let mut req = IngestRequest::new();
    req.set_context(ctx);
    req.set_sst(meta);
    let resp = cli.ingest(&req).unwrap();
    assert!(!resp.has_error(), "{:?}", resp.get_error());
}

#[derive(Default)]
struct TestPropertiesCollectorFactory {
    cf: String,
}

impl TestPropertiesCollectorFactory {
    pub fn new(cf: impl Into<String>) -> Self {
        Self { cf: cf.into() }
    }
}

impl TablePropertiesCollectorFactory<TestPropertiesCollector> for TestPropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> TestPropertiesCollector {
        TestPropertiesCollector::new(self.cf.clone())
    }
}

struct TestPropertiesCollector {
    cf: String,
}

impl TestPropertiesCollector {
    pub fn new(cf: String) -> Self {
        Self { cf }
    }
}

impl TablePropertiesCollector for TestPropertiesCollector {
    fn add(&mut self, _: &[u8], _: &[u8], _: DBEntryType, _: u64, _: u64) {}

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        std::iter::once((
            PROP_TEST_MARKER_CF_NAME.to_owned(),
            self.cf.as_bytes().to_owned(),
        ))
        .collect()
    }
}
