// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;

use crate::{CfName, MvccProperties};

#[derive(Clone, PartialEq, Eq)]
pub struct SstMetaData {
    pub name: String,
    pub level: u64,
    pub size: u64,
    pub column_family: CfName,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub abs_path: PathBuf,
    pub mvcc_properties: Option<MvccProperties>,
}

impl std::fmt::Debug for SstMetaData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SstMetaData")
            .field("name", &self.name)
            .field("level", &self.level)
            .field("size", &self.size)
            .field("column_family", &self.column_family)
            .field(
                "smallest_key",
                &format_args!("{}", log_wrappers::Value::key(&self.smallest_key)),
            )
            .field(
                "largest_key",
                &format_args!("{}", log_wrappers::Value::key(&self.largest_key)),
            )
            .field("abs_path", &self.abs_path)
            .field("mvcc_properties", &self.mvcc_properties)
            .finish()
    }
}

pub trait LsmVersion: Send + 'static {
    fn get_files_in_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<SstMetaData>, String>;
}

pub trait VersionedLsmExt {
    type Version: LsmVersion;

    fn lock_current_version(&self, on_cf: CfName) -> Result<Self::Version, String>;
}
