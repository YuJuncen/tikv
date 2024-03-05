// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    any::Any,
    cell::LazyCell,
    ffi::OsStr,
    io::Write,
    panic::UnwindSafe,
    path::PathBuf,
    process::Command,
    sync::{Arc, LazyLock, Mutex},
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
        dbg!(self.key_manager.is_some());
        self.key_manager.as_deref()
    }
}

fn system(cmd: &str, args: impl IntoIterator<Item = impl AsRef<OsStr>> + UnwindSafe) {
    let exec = || {
        let child = Command::new(cmd).args(args).spawn().unwrap();
        println!("====={child:?}=====");
        let exit = child.wait_with_output().unwrap();
        std::io::copy(
            &mut std::io::Cursor::new(exit.stdout),
            &mut std::io::stdout(),
        )
        .unwrap();
        std::io::copy(
            &mut std::io::Cursor::new(exit.stderr),
            &mut std::io::stdout(),
        )
        .unwrap();
        std::io::stdout().flush().unwrap();
    };
    println!("====={:?}=====", std::panic::catch_unwind(exec));
}

impl<T: SstPath> SstPath for Arc<T> {
    fn sst_path(&self, meta: &SstMeta) -> Option<PathBuf> {
        T::sst_path(&self, meta)
    }

    fn encryption(&self) -> Option<&DataKeyManager> {
        T::encryption(&self)
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
        T::sst_path(&*self, meta)
    }

    fn encryption(&self) -> Option<&DataKeyManager> {
        T::encryption(&*self)
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
