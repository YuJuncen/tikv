// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;

use crate::CfName;

#[derive(Clone, PartialEq, Eq)]
pub struct SstMetaData {
    pub name: String,
    pub level: u64,
    pub size: u64,
    pub column_family: CfName,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub abs_path: PathBuf,
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
            .finish()
    }
}

/// test whether two ranges overlapping.
/// end key is exclusive.
/// empty end key means infinity.
fn is_overlapping(range: (&[u8], &[u8]), range2: (&[u8], &[u8])) -> bool {
    let (x1, y1) = range;
    let (x2, y2) = range2;
    match (x1, y1, x2, y2) {
        // 1:       |__________________|
        // 2:   |______________________|
        (_, b"", _, b"") => true,
        // 1:   (x1)|__________________|
        // 2:   |_________________|(y2)
        (x1, b"", _, y2) => x1 < y2,
        // 1:   |________________|(y1)
        // 2:    (x2)|_________________|
        (_, y1, x2, b"") => x2 < y1,
        // 1:  (x1)|________|(y1)
        // 2:    (x2)|__________|(y2)
        (x1, y1, x2, y2) => x2 < y1 && x1 < y2,
    }
}

pub trait LsmVersion {
    fn get_all_files(&self) -> Result<Vec<SstMetaData>, String>;

    fn get_files_in_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<SstMetaData>, String> {
        self.get_all_files().map(|mut v| {
            // NOTE: what if the file only contains the key ""?
            v.retain(|m| is_overlapping((start, end), (&m.smallest_key, &m.largest_key)));
            v
        })
    }
}

pub trait VersionedLsmExt {
    type Version: LsmVersion;

    fn lock_current_version(&self, on_cf: CfName) -> Result<Self::Version, String>;
}
