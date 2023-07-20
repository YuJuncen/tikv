// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! This module provides the ability of observing the progress of "flushing"
//! ssts.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use engine_traits::{MiscExt, CF_DEFAULT, CF_WRITE};
use raftstore::store::RegionReadProgressRegistry;
use txn_types::TimeStamp;

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct FlushProgress {
    flushed_index: u64,
    resolved_ts: TimeStamp,
}

#[derive(Clone)]
pub struct Advancer<E>(Arc<Mutex<AdvancerCore<E>>>);

impl<E> Advancer<E> {
    pub fn new(core: AdvancerCore<E>) -> Self {
        Self(Arc::new(Mutex::new(core)))
    }

    pub fn with<T>(&self, f: impl FnOnce(&mut AdvancerCore<E>) -> T) -> T {
        let mut core = self.0.lock().unwrap();
        f(&mut core)
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum ResolvedTsNotReadyReason {
    NoFlushRecorded,
    NoResolvedTsRecorded,
    RegionMerged {
        merge_index: u64,
        flush_index: u64,
        last_applied_index: u64,
    },
    FlushTsLessThanBackupTs {
        flushed_ts: TimeStamp,
        required: TimeStamp,
    },
}

pub struct AdvancerCore<E> {
    read_progress: RegionReadProgressRegistry,
    last_flush: HashMap<u64, FlushProgress>,
    engine: E,
}

impl<E> AdvancerCore<E> {
    pub fn new(read_progress: RegionReadProgressRegistry, engine: E) -> Self {
        Self {
            read_progress,
            engine,
            last_flush: Default::default(),
        }
    }

    pub fn check_resolved_ts(
        &self,
        region: u64,
        target_rts: TimeStamp,
    ) -> std::result::Result<(), ResolvedTsNotReadyReason> {
        use ResolvedTsNotReadyReason::*;
        let local_prog = self.get_progress(region).ok_or(NoFlushRecorded)?;
        let prog = self
            .read_progress
            .get(&region)
            .ok_or(NoResolvedTsRecorded)?;
        let core = prog.get_core();
        let merged = local_prog.flushed_index < core.last_merge_index()
            && core.last_merge_index() < core.applied_index();
        if merged {
            return Err(RegionMerged {
                merge_index: core.last_merge_index(),
                flush_index: local_prog.flushed_index,
                last_applied_index: core.applied_index(),
            });
        }
        let flushed = local_prog.resolved_ts >= target_rts;
        if !flushed {
            return Err(FlushTsLessThanBackupTs {
                flushed_ts: local_prog.resolved_ts,
                required: target_rts,
            });
        }
        Ok(())
    }

    pub fn get_progress(&self, region: u64) -> Option<FlushProgress> {
        self.last_flush.get(&region).copied()
    }

    pub fn hint_advance_resolved_ts(&self, region: u64) {
        self.read_progress
            .get(&region)
            .map(|r| r.notify_advance_resolved_ts());
    }
}

impl<E: MiscExt> AdvancerCore<E> {
    pub fn advance_progress(&mut self) -> crate::errors::Result<()> {
        let current = self.read_progress.with(|r| {
            r.iter()
                .map(|(id, prog)| {
                    let core = prog.get_core();
                    let fprog = FlushProgress {
                        resolved_ts: prog.resolved_ts().into(),
                        flushed_index: core.applied_index(),
                    };
                    (*id, fprog)
                })
                .collect::<HashMap<_, _>>()
        });
        self.engine.flush_cfs(&[CF_DEFAULT, CF_WRITE], true)?;
        self.last_flush = current;
        Ok(())
    }
}
