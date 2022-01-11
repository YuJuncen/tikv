// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    collections::{hash_map::RandomState, HashMap},
    time::Duration,
};

use crate::errors::{Error, Result};
use tikv_util::{box_err, time::Instant};
use tokio::sync::{Mutex, RwLock};
use txn_types::{Key, TimeStamp};

/// wrap a user key with encoded data key.
pub fn wrap_key(v: Vec<u8>) -> Vec<u8> {
    // TODO: encode in place.
    let key = Key::from_raw(v.as_slice()).into_encoded();
    key
}

/// decode ts from a key, and transform the error to the crate error.
pub fn get_ts(key: &Key) -> Result<TimeStamp> {
    key.decode_ts().map_err(|err| {
        Error::Other(box_err!(
            "failed to get ts from key {}: {}",
            log_wrappers::Value::key(key.as_encoded().as_slice()),
            err
        ))
    })
}

/// StopWatch is a utility for record time cost in multi-stage tasks.
/// NOTE: Maybe it should be generic over somewhat Clock type?
pub struct StopWatch(Instant);

impl StopWatch {
    /// Create a new stopwatch via current time.
    pub fn new() -> Self {
        Self(Instant::now_coarse())
    }

    /// Get time elapsed since last lap (or creation if the first time).
    pub fn lap(&mut self) -> Duration {
        let elapsed = self.0.saturating_elapsed();
        self.0 = Instant::now_coarse();
        elapsed
    }
}

/// Slot is a shareable slot in the slot map.
pub type Slot<T> = Mutex<T>;

/// SlotMap is a trivial concurrent map which sharding over each key.
/// NOTE: Maybe we can use dashmap for replacing the RwLock.
pub type SlotMap<K, V, S = RandomState> = RwLock<HashMap<K, Slot<V>, S>>;
