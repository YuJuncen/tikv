// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{Cursor, Write},
    sync::Arc,
};

use external_storage::ExternalStorage;
use kvproto::brpb;
use tikv_util::codec::stream_event::EventEncoder;

pub struct Border {
    start: Vec<u8>,
    id: u64,
}

pub struct Terra {
    borders: Vec<Border>,
}

pub struct Narrator {
    terra: Terra,
    land: Arc<dyn ExternalStorage>,
}

pub struct Leaf {
    kvs: Vec<(Vec<u8>, Vec<u8>)>,
    start_ts: u64,
    commit_ts: u64,
}

pub struct Page {
    k: Vec<u8>,
    v: Vec<u8>,
    cf: &'static str,
}

pub struct Promise {
    pages: Vec<Page>,
}

impl Narrator {
    pub fn weave_tales(&self, of: impl Iterator<Item = Leaf>) -> Promise {
        todo!()
    }
}

struct LogFileBuilder {
    pub name: String,
    pub region_id: u64,
    pub cf: &'static str,
    pub ty: brpb::FileType,
    pub is_meta: bool,

    content: zstd::Encoder<'static, Cursor<Vec<u8>>>,
    min_ts: u64,
    max_ts: u64,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    number_of_entries: u64,
    crc64xor: u64,
    compression: brpb::CompressionType,
    file_real_size: u64,
}

impl LogFileBuilder {
    pub fn new(name: &str, region_id: u64) -> Self {
        Self {
            name: name.to_owned(),
            region_id,
            cf: "default",
            ty: brpb::FileType::Put,
            is_meta: false,

            content: zstd::Encoder::new(Cursor::new(vec![]), 3).unwrap(),
            min_ts: 0,
            max_ts: 0,
            min_key: vec![],
            max_key: vec![],
            number_of_entries: 0,
            crc64xor: 0,
            compression: brpb::CompressionType::Zstd,
            file_real_size: 0,
        }
    }

    pub fn add_encoded(&mut self, key: &[u8], value: &[u8]) {
        let ts = txn_types::Key::decode_ts_from(key)
            .expect("key without ts")
            .into_inner();
        for part in EventEncoder::encode_event(key, value) {
            self.file_real_size += part.as_ref().len() as u64;
            self.content.write_all(part.as_ref()).unwrap();
        }
        // Update metadata.
        self.number_of_entries += 1;
        self.min_ts = self.min_ts.min(ts);
        self.max_ts = self.max_ts.max(ts);
        if self.min_key.is_empty() || key < self.min_key.as_slice() {
            self.min_key = key.to_owned();
        }
        if self.max_key.is_empty() || key > self.max_key.as_slice() {
            self.max_key = key.to_owned();
        }
        let mut d = crc64fast::Digest::new();
        d.write(key);
        d.write(value);
        self.crc64xor ^= d.sum64();
    }

    pub fn build(&mut self) {}
}
