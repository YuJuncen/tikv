// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::Arc;
use std::fmt;
use std::fmt::Display;

use raftstore::router::RaftStoreRouter;
use engine_rocks::raw::DB;
use tikv::config::BackupConfig;
use tikv::storage::kv::Engine;
use tikv_util::worker::{Runnable, Scheduler};
use crate::metadata::MetadataClient;
use crate::metadata::store::{MetaStore, EtcdStore};

pub struct Endpoint<S: MetaStore, T: Send + Display> {
    config: BackupConfig,
    meta_client: MetadataClient<S>,
    scheduler: Scheduler<T>,
}

impl<S, T> Endpoint<S, T>
where
    S: MetaStore,
    T: Send + Display,
 {
    pub fn new(
        meta_client: MetadataClient<S>,
        config: BackupConfig,
        scheduler: Scheduler<T>,
    ) -> Endpoint<S, T> {
        Endpoint {
            config,
            meta_client,
            scheduler,
        }
    }
}
pub enum Task {
    WatchRanges
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("BackupStreamTask");
        match self {
            Task::WatchRanges => de.field("name", &"wartch_ranges").finish(),
        }
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
 
impl<S, T> Runnable for Endpoint<S, T>
where 
    S: MetaStore,
    T: Send + Display,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        unimplemented!()
    }
}