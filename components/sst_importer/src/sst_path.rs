// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    any::Any,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use kvproto::import_sstpb::SstMeta;
use tikv_util::Either;

pub use crate::import_file::ImportDir;
use crate::SstImporter;

pub trait SstPath: Any {
    fn sst_path(&self, meta: &SstMeta) -> Option<PathBuf>;
}

impl<E: engine_traits::KvEngine> SstPath for SstImporter<E> {
    fn sst_path(&self, meta: &SstMeta) -> Option<PathBuf> {
        self.dir.join_for_read(meta).map(|x| x.save).ok()
    }
}

impl<E: engine_traits::KvEngine> SstPath for ImportDir<E> {
    fn sst_path(&self, meta: &SstMeta) -> Option<PathBuf> {
        self.join_for_read(meta).map(|x| x.save).ok()
    }
}

impl<T: SstPath> SstPath for Arc<T> {
    fn sst_path(&self, meta: &SstMeta) -> Option<PathBuf> {
        T::sst_path(&self, meta)
    }
}

impl<T: SstPath> SstPath for Mutex<T> {
    fn sst_path(&self, meta: &SstMeta) -> Option<PathBuf> {
        self.lock().unwrap().sst_path(meta)
    }
}

impl<T: SstPath, L: SstPath> SstPath for Either<T, L> {
    fn sst_path(&self, meta: &SstMeta) -> Option<PathBuf> {
        match self {
            Either::Left(l) => l.sst_path(meta),
            Either::Right(r) => r.sst_path(meta),
        }
    }
}

pub struct PanicSstPath;

impl SstPath for PanicSstPath {
    fn sst_path(&self, _meta: &SstMeta) -> Option<PathBuf> {
        panic!("call sst_path in PanicSstPath")
    }
}
