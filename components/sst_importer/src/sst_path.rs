// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    any::Any,
    path::PathBuf,
    sync::{Arc, LazyLock},
};

use encryption::DataKeyManager;
use kvproto::import_sstpb::SstMeta;
use tikv_util::Either;

pub use crate::import_file::ImportDir;
use crate::SstImporter;

pub trait SstPath: Any {
    fn sst_path(&self, meta: &SstMeta) -> Option<PathBuf>;
    fn encryption(&self) -> Option<&DataKeyManager>;
}

impl<E: engine_traits::KvEngine> SstPath for SstImporter<E> {
    fn sst_path(&self, meta: &SstMeta) -> Option<PathBuf> {
        self.dir.join_for_read(meta).map(|x| x.save).ok()
    }
    fn encryption(&self) -> Option<&DataKeyManager> {
        self.key_manager.as_deref()
    }
}

impl<T: SstPath> SstPath for Arc<T> {
    fn sst_path(&self, meta: &SstMeta) -> Option<PathBuf> {
        T::sst_path(self, meta)
    }

    fn encryption(&self) -> Option<&DataKeyManager> {
        T::encryption(self)
    }
}

impl<T: SstPath, L: SstPath> SstPath for Either<T, L> {
    fn sst_path(&self, meta: &SstMeta) -> Option<PathBuf> {
        match self {
            Either::Left(l) => l.sst_path(meta),
            Either::Right(r) => r.sst_path(meta),
        }
    }
    fn encryption(&self) -> Option<&DataKeyManager> {
        match self {
            Either::Left(l) => l.encryption(),
            Either::Right(r) => r.encryption(),
        }
    }
}

impl<T: SstPath, F: FnOnce() -> T + 'static> SstPath for LazyLock<T, F> {
    fn sst_path(&self, meta: &SstMeta) -> Option<PathBuf> {
        T::sst_path(self, meta)
    }

    fn encryption(&self) -> Option<&DataKeyManager> {
        T::encryption(self)
    }
}

pub struct PanicSstPath;

impl SstPath for PanicSstPath {
    fn sst_path(&self, _meta: &SstMeta) -> Option<PathBuf> {
        panic!("call sst_path in PanicSstPath")
    }
    fn encryption(&self) -> Option<&DataKeyManager> {
        panic!("call encryption in PanicSstPath")
    }
}
