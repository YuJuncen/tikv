// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
use std::{cmp::Ordering, collections::BinaryHeap};

use engine_rocks::RocksSstIterator;
use engine_traits::Iterator;
use keys::validate_data_key;
use txn_types::{Key, TimeStamp, WriteRef, WriteType};

use crate::{Error, Result};

#[derive(Eq, PartialEq)]
struct Entry {
    key: Key,
    from: usize,
}

impl Entry {
    fn new(key: Key, from: usize) -> Self {
        Entry { key, from }
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.key.cmp(&other.key) {
            std::cmp::Ordering::Equal => self.from.cmp(&other.from),
            std::cmp::Ordering::Greater => std::cmp::Ordering::Less,
            std::cmp::Ordering::Less => std::cmp::Ordering::Greater,
        }
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct MergeIterator<'a> {
    resolved_ts: TimeStamp,
    sst_iters: Vec<RocksSstIterator<'a>>,

    // maintain the next key from each SstReader
    entry_cache: BinaryHeap<Entry>,
}

impl<'a> MergeIterator<'a> {
    pub fn new(resolved_ts: u64, sst_iters: Vec<RocksSstIterator<'a>>) -> Self {
        let entry_cache = BinaryHeap::with_capacity(6);
        MergeIterator {
            resolved_ts: TimeStamp::new(resolved_ts),
            sst_iters,

            entry_cache,
        }
    }
}

impl<'a> Iterator for MergeIterator<'a> {
    fn seek(&mut self, key: &[u8]) -> engine_traits::Result<bool> {
        self.entry_cache.clear();
        'next_iter: for (i, iter) in self.sst_iters.iter_mut().enumerate() {
            if !iter.seek(key)? {
                // empty sst, so skip obtaining entry from it
                continue;
            }
            while !validate_data_key(iter.key())
                || Key::decode_ts_from(iter.key())? > self.resolved_ts
            {
                if !iter.next()? {
                    continue 'next_iter;
                }
            }
            self.entry_cache
                .push(Entry::new(Key::from_encoded_slice(iter.key()), i));
        }
        Ok(!self.entry_cache.is_empty())
    }

    fn seek_for_prev(&mut self, _key: &[u8]) -> engine_traits::Result<bool> {
        unimplemented!()
    }

    fn seek_to_first(&mut self) -> engine_traits::Result<bool> {
        self.entry_cache.clear();
        'next_iter: for (i, iter) in &mut self.sst_iters.iter_mut().enumerate() {
            if !iter.seek_to_first()? {
                // empty sst, so skip obtaining entry from it
                continue;
            }
            while !validate_data_key(iter.key())
                || Key::decode_ts_from(iter.key())? > self.resolved_ts
            {
                if !iter.next()? {
                    continue 'next_iter;
                }
            }
            self.entry_cache
                .push(Entry::new(Key::from_encoded_slice(iter.key()), i));
        }
        Ok(!self.entry_cache.is_empty())
    }

    fn seek_to_last(&mut self) -> engine_traits::Result<bool> {
        unimplemented!()
    }

    fn prev(&mut self) -> engine_traits::Result<bool> {
        unimplemented!()
    }

    fn next(&mut self) -> engine_traits::Result<bool> {
        if let Some(Entry { from, .. }) = self.entry_cache.pop() {
            let iter = &mut self.sst_iters[from];
            while iter.next()? {
                if !validate_data_key(iter.key()) {
                    continue;
                }
                if Key::decode_ts_from(iter.key())? <= self.resolved_ts {
                    self.entry_cache
                        .push(Entry::new(Key::from_encoded_slice(iter.key()), from));
                    break;
                }
            }
        }
        Ok(!self.entry_cache.is_empty())
    }

    fn key(&self) -> &[u8] {
        self.entry_cache.peek().unwrap().key.as_encoded()
    }

    fn value(&self) -> &[u8] {
        self.sst_iters[self.entry_cache.peek().unwrap().from].value()
    }

    fn valid(&self) -> engine_traits::Result<bool> {
        Ok(!self.entry_cache.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use engine_rocks::RocksSstReader;
    use engine_traits::{
        IterOptions, Iterator, KvEngine, RefIterable, SstExt, SstReader, SstWriter,
        SstWriterBuilder,
    };
    use tempfile::TempDir;
    use tikv::storage::TestEngineBuilder;
    use txn_types::{Key, TimeStamp};

    use super::{BinaryIterator, Entry, MergeIterator};

    fn prepare_ssts<E: KvEngine>(name: &str, db: E, path: &Path) {
        let mut writer = <E as SstExt>::SstWriterBuilder::new()
            //.set_in_memory(true)
            .set_cf(engine_traits::CF_WRITE)
            .set_db(&db)
            .set_compression_type(None)
            .set_compression_level(0)
            .build(path.join(name).to_str().unwrap())
            .unwrap();

        for k in 1..10 {
            for ts in 1..10 {
                let raw = format!("test{k}");
                let mut key = Key::from_encoded(raw.into_bytes());
                key.append_ts_inplace(TimeStamp::new(10 - ts));
                let dkey = keys::data_key(key.as_encoded());
                let val = format!("for test {}, {}, {}.", name, k, 10 - ts);
                writer.put(&dkey, val.as_bytes()).unwrap();
            }
        }

        writer.finish().unwrap();
    }

    fn read_ssts(names: Vec<String>, path: &Path) {
        let mut readers = Vec::new();
        let mut iters = Vec::new();
        for name in names {
            let dst_file_name = path.join(name);
            let sst_reader =
                RocksSstReader::open_with_env(dst_file_name.to_str().unwrap(), None).unwrap();
            sst_reader.verify_checksum().unwrap();
            readers.push(sst_reader);
        }

        for reader in &readers {
            let iter = reader.iter(IterOptions::default()).unwrap();
            iters.push(iter);
        }

        let mut iter = BinaryIterator::new(5, iters);

        iter.seek_to_first().unwrap();

        let mut last_key = Key::from_encoded(Vec::new());
        while iter.valid().unwrap() {
            iter.print_debug();
            let key = Key::from_encoded(iter.key().to_vec());
            // let value = String::from_utf8(iter.value().to_vec()).unwrap();
            // println!("key: {}. value: {}.", key, value);
            assert!(last_key <= key);
            last_key = key;
            iter.next().unwrap();
        }
    }

    #[test]
    fn test_123() {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs([
                engine_traits::CF_DEFAULT,
                engine_traits::CF_LOCK,
                engine_traits::CF_WRITE,
            ])
            .build()
            .unwrap();
        let db = rocks.get_rocksdb();
        let mut names = Vec::new();
        for i in 0..5 {
            let name = format!("foo{i}");
            prepare_ssts(&name, db.clone(), temp.path());
            names.push(name);
        }

        read_ssts(names, temp.path());
    }

    use std::collections::BinaryHeap;
    #[test]
    fn test1() {
        let mut cache = BinaryHeap::new();

        cache.push(Entry::new(
            Key::from_encoded("7A7465737430FFFFFFFFFFFFFFFB".to_owned().into_bytes()),
            0,
        ));
        cache.push(Entry::new(
            Key::from_encoded("7A7465737430FFFFFFFFFFFFFFFB".to_owned().into_bytes()),
            1,
        ));
        cache.push(Entry::new(
            Key::from_encoded("7A7465737430FFFFFFFFFFFFFFFB".to_owned().into_bytes()),
            2,
        ));
        cache.push(Entry::new(
            Key::from_encoded("7A7465737430FFFFFFFFFFFFFFFB".to_owned().into_bytes()),
            3,
        ));
        cache.push(Entry::new(
            Key::from_encoded("7A7465737430FFFFFFFFFFFFFFFB".to_owned().into_bytes()),
            4,
        ));

        let entry = cache.pop().unwrap();
        println!("key: {}, from: {}.", entry.key, entry.from);
        assert!(entry.from == 4);

        cache.push(Entry::new(
            Key::from_encoded("7A7465737431FFFFFFFFFFFFFFFB".to_owned().into_bytes()),
            4,
        ));

        let entry = cache.pop().unwrap();
        println!("key: {}, from: {}.", entry.key, entry.from);
        assert!(entry.from == 3);
    }
}

struct BinaryIterator<'a> {
    resolved_ts: TimeStamp,
    sst_iters: Vec<RocksSstIterator<'a>>,

    // maintain the next key from each SstReader
    entry_cache: Vec<usize>,
}

impl<'a> BinaryIterator<'a> {
    pub fn new(resolved_ts: u64, sst_iters: Vec<RocksSstIterator<'a>>) -> Self {
        let entry_cache = Vec::with_capacity(6);
        BinaryIterator {
            resolved_ts: TimeStamp::new(resolved_ts),
            sst_iters,

            entry_cache,
        }
    }

    fn iter_key(&self, pos: usize) -> &[u8] {
        self.sst_iters[self.entry_cache[pos]].key()
    }

    fn sift_up(&mut self, mut pos: usize) {
        while pos > 0 {
            let parent = (pos - 1) / 2;
            if self.iter_key(pos) < self.iter_key(parent) {
                self.entry_cache.swap(pos, parent);
                pos = parent;
            } else {
                return;
            }
        }
    }

    fn sift_down(&mut self, mut pos: usize) {
        // left child
        let mut child = pos * 2 + 1;
        while child < self.entry_cache.len() {
            // maybe right child has smaller key
            if child + 1 < self.entry_cache.len() && self.iter_key(child) > self.iter_key(child + 1)
            {
                child += 1;
            }

            if self.iter_key(pos) <= self.iter_key(child) {
                return;
            }

            self.entry_cache.swap(pos, child);
            pos = child;
            child = pos * 2 + 1;
        }
    }

    fn push(&mut self, from: usize) {
        let pos = self.entry_cache.len();
        self.entry_cache.push(from);
        self.sift_up(pos);
    }

    fn pop(&mut self) -> Option<usize> {
        if self.entry_cache.is_empty() {
            return None;
        }
        let from = self.entry_cache.swap_remove(0);
        self.sift_down(0);

        Some(from)
    }

    fn peek(&self) -> usize {
        self.entry_cache[0]
    }

    fn heap(&mut self) {
        if self.entry_cache.len() <= 1 {
            return;
        }

        let end = self.entry_cache.len() - 1;
        let mut parent = (end - 1) / 2;
        loop {
            self.sift_down(parent);

            if parent == 0 {
                return;
            }

            parent -= 1;
        }
    }

    #[cfg(test)]
    pub fn print_debug(&self) {
        println!("debug:");
        for i in &self.entry_cache {
            let iter = &self.sst_iters[*i];
            let key = Key::from_encoded(iter.key().to_vec());
            let value = String::from_utf8(iter.value().to_vec()).unwrap();
            println!("\tkey: {}. value: {}.", key, value);
        }
    }
}

impl<'a> Iterator for BinaryIterator<'a> {
    fn seek(&mut self, key: &[u8]) -> engine_traits::Result<bool> {
        self.entry_cache.clear();
        'next_iter: for (i, iter) in self.sst_iters.iter_mut().enumerate() {
            if !iter.seek(key)? {
                // empty sst, so skip obtaining entry from it
                continue;
            }
            while !validate_data_key(iter.key())
                || Key::decode_ts_from(iter.key())? > self.resolved_ts
            {
                if !iter.next()? {
                    continue 'next_iter;
                }
            }
            self.entry_cache.push(i);
        }
        self.heap();
        Ok(!self.entry_cache.is_empty())
    }

    fn seek_for_prev(&mut self, _key: &[u8]) -> engine_traits::Result<bool> {
        unimplemented!()
    }

    fn seek_to_first(&mut self) -> engine_traits::Result<bool> {
        self.entry_cache.clear();
        'next_iter: for (i, iter) in self.sst_iters.iter_mut().enumerate() {
            if !iter.seek_to_first()? {
                // empty sst, so skip obtaining entry from it
                continue;
            }
            while !validate_data_key(iter.key())
                || Key::decode_ts_from(iter.key())? > self.resolved_ts
            {
                if !iter.next()? {
                    continue 'next_iter;
                }
            }
            self.entry_cache.push(i);
        }
        self.heap();
        Ok(!self.entry_cache.is_empty())
    }

    fn seek_to_last(&mut self) -> engine_traits::Result<bool> {
        unimplemented!()
    }

    fn prev(&mut self) -> engine_traits::Result<bool> {
        unimplemented!()
    }

    fn next(&mut self) -> engine_traits::Result<bool> {
        if let Some(from) = self.pop() {
            let iter = &mut self.sst_iters[from];
            while iter.next()? {
                if validate_data_key(iter.key())
                    && Key::decode_ts_from(iter.key())? <= self.resolved_ts
                {
                    self.push(from);
                    break;
                }
            }
        }
        Ok(!self.entry_cache.is_empty())
    }

    fn key(&self) -> &[u8] {
        self.sst_iters[self.peek()].key()
    }

    fn value(&self) -> &[u8] {
        self.sst_iters[self.peek()].value()
    }

    fn valid(&self) -> engine_traits::Result<bool> {
        Ok(!self.entry_cache.is_empty())
    }
}
// struct SstWriteDispather {
// default_writer: SstWriter,
// write_writer: SstWriter,
// }
//
// impl SstWriteDispatcher {
// pub fn new(default_dst_file_name: &str, write_dst_file_name: &str) -> Self {
// SstWriteDispather {
// default_writer,
// write_writer,
// }
// }
// }

pub struct SstEntryScanner<I: Iterator> {
    default_iter: I,
    write_iter: I,
}

impl<I: Iterator> SstEntryScanner<I> {
    pub fn new(default_iter: I, write_iter: I) -> Self {
        SstEntryScanner {
            default_iter,
            write_iter,
        }
    }

    pub fn seek_to_first(&mut self) -> Result<bool> {
        Ok(self.write_iter.seek_to_first()? && self.default_iter.seek_to_first()?)
    }

    pub fn seek(&mut self, key: &[u8]) -> Result<bool> {
        Ok(self.write_iter.seek(key)? && self.default_iter.seek(key)?)
    }

    pub fn next_write(&mut self) -> Result<bool> {
        self.write_iter.next().map_err(Error::from)
    }

    pub fn next_default(&mut self) -> Result<bool> {
        self.default_iter.next().map_err(Error::from)
    }

    // NOTICE: equivalent to WriteRef::parse, but only parse one field.
    fn start_ts(&self) -> Result<Option<TimeStamp>> {
        let val = self.write_iter.value();
        let write_ref = WriteRef::parse(val).map_err(|e| {
            Error::BadFormat(format!(
                "write {}: {}",
                log_wrappers::Value::key(keys::origin_key(self.write_iter.key())),
                e
            ))
        })?;

        if write_ref.write_type == WriteType::Put && write_ref.short_value.is_none() {
            return Ok(Some(write_ref.start_ts));
        }

        Ok(None)
    }

    pub fn write_valid(&self) -> Result<bool> {
        self.write_iter.valid().map_err(Error::from)
    }

    // equal to Iterator::valid(), and advance the default iter.
    pub fn read(&mut self) -> Result<bool> {
        let (wkey, commit_ts) = Key::split_on_ts_for(self.write_iter.key())?;
        let start_ts = match self.start_ts()? {
            Some(ts) => ts,
            None => return Ok(false),
        };
        // seek to the same user key from default
        loop {
            if !self.default_iter.valid()? {
                return Err(Error::Engine(box_err!(
                    "default not found(end). key: {}, ts: {}",
                    log_wrappers::Value::key(wkey),
                    commit_ts
                )));
            }
            let (dkey, dstart_ts) = Key::split_on_ts_for(self.default_iter.key())?;
            match (dkey.cmp(wkey), dstart_ts.cmp(&start_ts)) {
                (Ordering::Equal, Ordering::Equal) => return Ok(true),
                (Ordering::Greater, _) | (Ordering::Equal, Ordering::Less) => {
                    return Err(Error::Engine(box_err!(
                        "default not found(not match). key: {}, cts: {}, sts: {}, next dkey: {}, next ts: {}",
                        log_wrappers::Value::key(wkey),
                        commit_ts,
                        start_ts,
                        log_wrappers::Value::key(dkey),
                        dstart_ts
                    )));
                }
                (Ordering::Less, _) | (Ordering::Equal, Ordering::Greater) => {
                    self.default_iter.next()?;
                    continue;
                }
            }
        }
    }

    #[inline]
    pub fn write_key(&self) -> &[u8] {
        self.write_iter.key()
    }

    #[inline]
    pub fn write_value(&self) -> &[u8] {
        self.write_iter.value()
    }

    #[inline]
    pub fn default_key(&self) -> &[u8] {
        self.default_iter.key()
    }

    #[inline]
    pub fn default_value(&self) -> &[u8] {
        self.default_iter.value()
    }
}
