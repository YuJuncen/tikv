// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
use std::ops::Bound;

use engine_traits::Iterator;
use kvproto::import_sstpb::{DownloadRequestType, Range, RewriteRule, SstMeta};
use tikv_util::codec::bytes::encode_bytes;

use crate::{Error, Result};

fn key_to_bound(key: &[u8]) -> Bound<&[u8]> {
    if key.is_empty() {
        Bound::Unbounded
    } else {
        Bound::Included(key)
    }
}

fn key_to_exclusive_bound(key: &[u8]) -> Bound<&[u8]> {
    if key.is_empty() {
        Bound::Unbounded
    } else {
        Bound::Excluded(key)
    }
}

// The backup file saves the key with old prefix,
// and the range has new prefix in the SstMeta.
// do_pre_rewrite undo the range to be compared with these keys.
pub async fn do_pre_rewrite(
    meta: &SstMeta,
    rewrite_rule: &RewriteRule,
    req_type: DownloadRequestType,
) -> Result<(Bound<Vec<u8>>, Bound<Vec<u8>>)> {
    // undo key rewrite so we could compare with the keys inside SST
    let old_prefix = rewrite_rule.get_old_key_prefix();
    let new_prefix = rewrite_rule.get_new_key_prefix();
    let req_type = req_type;

    debug!("rewrite file";
        "old_prefix" => log_wrappers::Value::key(old_prefix),
        "new_prefix" => log_wrappers::Value::key(new_prefix),
        "req_type" => ?req_type,
    );

    let range_start = meta.get_range().get_start();
    let range_end = meta.get_range().get_end();

    println!("start key  : {}", log_wrappers::Value::key(range_start));
    println!("end key    : {}", log_wrappers::Value::key(range_end));
    println!("old prefix : {}", log_wrappers::Value::key(old_prefix));
    println!("new prefix : {}", log_wrappers::Value::key(new_prefix));

    let range_start_bound = key_to_bound(range_start);
    let range_end_bound = if meta.get_end_key_exclusive() {
        key_to_exclusive_bound(range_end)
    } else {
        key_to_bound(range_end)
    };

    let mut range_start =
        keys::rewrite::rewrite_prefix_of_start_bound(new_prefix, old_prefix, range_start_bound)
            .map_err(|_| Error::WrongKeyPrefix {
                what: "SST start range",
                key: range_start.to_vec(),
                prefix: new_prefix.to_vec(),
            })?;
    let mut range_end =
        keys::rewrite::rewrite_prefix_of_end_bound(new_prefix, old_prefix, range_end_bound)
            .map_err(|_| Error::WrongKeyPrefix {
                what: "SST end range",
                key: range_end.to_vec(),
                prefix: new_prefix.to_vec(),
            })?;

    if req_type == DownloadRequestType::Keyspace {
        range_start = keys::rewrite::encode_bound(range_start);
        range_end = keys::rewrite::encode_bound(range_end);
    }
    Ok((range_start, range_end))
}

fn is_before_start_bound<K: AsRef<[u8]>>(value: &[u8], bound: &Bound<K>) -> bool {
    match bound {
        Bound::Unbounded => false,
        Bound::Included(b) => *value < *b.as_ref(),
        Bound::Excluded(b) => *value <= *b.as_ref(),
    }
}

pub fn is_after_end_bound<K: AsRef<[u8]>>(value: &[u8], bound: &Bound<K>) -> bool {
    match bound {
        Bound::Unbounded => false,
        Bound::Included(b) => *value > *b.as_ref(),
        Bound::Excluded(b) => *value >= *b.as_ref(),
    }
}

pub fn try_skip_rewriting<Iter: Iterator>(
    meta: &SstMeta,
    mut iter: Iter,
    range_start: &Bound<Vec<u8>>,
    range_end: &Bound<Vec<u8>>,
    rewrite_rule: &RewriteRule,
    req_type: DownloadRequestType,
) -> Result<Option<Range>> {
    // read the first and last keys from the SST, determine if we could
    // simply move the entire SST instead of iterating and generate a new one.
    if rewrite_rule.old_key_prefix != rewrite_rule.new_key_prefix || rewrite_rule.new_timestamp != 0
    {
        // must iterate if we perform key rewrite
        return Ok(None);
    }
    if !iter.seek_to_first()? {
        let mut range = meta.get_range().clone();
        if req_type == DownloadRequestType::Keyspace {
            *range.mut_start() = encode_bytes(&range.take_start());
            *range.mut_end() = encode_bytes(&range.take_end());
        }
        // the SST is empty, so no need to iterate at all (should be impossible?)
        return Ok(Some(range));
    }

    let start_key = keys::origin_key(iter.key());
    if is_before_start_bound(start_key, range_start) {
        // SST's start is before the range to consume, so needs to iterate to skip over
        return Ok(None);
    }
    let start_key = start_key.to_vec();

    // seek to end and fetch the last (inclusive) key of the SST.
    iter.seek_to_last()?;
    let last_key = keys::origin_key(iter.key());
    if is_after_end_bound(last_key, range_end) {
        // SST's end is after the range to consume
        return Ok(None);
    }

    // range contained the entire SST, no need to iterate, just moving the file is
    // ok
    let mut range = Range::default();
    range.set_start(start_key);
    range.set_end(last_key.to_vec());
    Ok(Some(range))
}

pub struct EntryRewrite {
    data_key: Vec<u8>,
    user_key: Vec<u8>,

    data_key_prefix_len: usize,
    user_key_prefix_len: usize,
    old_key_prefix_len: usize,

    req_type: DownloadRequestType,
}

impl EntryRewrite {
    pub fn new(
        new_prefix: &[u8],
        old_key_prefix_len: usize,
        req_type: DownloadRequestType,
    ) -> Self {
        let data_key = keys::DATA_PREFIX_KEY.to_vec();
        let data_key_prefix_len = keys::DATA_PREFIX_KEY.len();
        let user_key = new_prefix.to_vec();
        let user_key_prefix_len = new_prefix.len();
        EntryRewrite {
            data_key,
            user_key,

            data_key_prefix_len,
            user_key_prefix_len,
            old_key_prefix_len,

            req_type,
        }
    }

    #[inline]
    pub fn rewrite_key(&mut self, old_key: &[u8], ts: Option<Vec<u8>>) -> &[u8] {
        self.data_key.truncate(self.data_key_prefix_len);
        self.user_key.truncate(self.user_key_prefix_len);
        self.user_key
            .extend_from_slice(&old_key[self.old_key_prefix_len..]);
        if self.req_type == DownloadRequestType::Keyspace {
            self.data_key.extend(encode_bytes(&self.user_key));
            self.data_key.extend(ts.unwrap());
        } else {
            self.data_key.extend_from_slice(&self.user_key);
        }
        self.data_key()
    }

    #[inline]
    pub fn data_key(&self) -> &[u8] {
        &self.data_key
    }
}
