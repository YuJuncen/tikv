// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use collections::HashMap;
use engine_traits::{CfName, LsmVersion, SstMetaData, VersionedLsmExt};
use rocksdb::{DBIterator, ReadOptions, DB};

use crate::{RocksEngine, RocksMvccProperties};

pub struct RocksVersion {
    iter: rocksdb::DBIterator<Arc<DB>>,
    eng: RocksEngine,
    cf: CfName,
    db_path: PathBuf,
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

impl LsmVersion for RocksVersion {
    fn get_files_in_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<SstMetaData>, String> {
        let properties = self.eng.get_range_properties_cf(self.cf, start, end)?;
        let properties_map = properties
            .iter()
            .map(|(k, v)| {
                (
                    Path::new(k)
                        .file_name()
                        .expect("get_range_properties_cf: sst is not a file")
                        .to_string_lossy(),
                    v,
                )
            })
            .collect::<HashMap<_, _>>();
        let properties_ref = &properties_map;
        let metas = self.iter.get_column_family_meta_data()?;
        let metas = metas
            .get_levels()
            .into_iter()
            .enumerate()
            .flat_map(|(level, level_md)| {
                level_md
                    .get_files()
                    .into_iter()
                    .filter(|file_md| {
                        is_overlapping(
                            (start, end),
                            (file_md.get_smallestkey(), file_md.get_largestkey()),
                        )
                    })
                    .map(move |file_md| {
                        let name = file_md.get_name().trim_start_matches('/').to_owned();
                        let abs_path = self.db_path.join(&name);
                        SstMetaData {
                            name,
                            level: level as u64,
                            size: file_md.get_size() as u64,
                            column_family: self.cf,
                            smallest_key: file_md.get_smallestkey().to_owned(),
                            largest_key: file_md.get_largestkey().to_owned(),
                            abs_path,
                            mvcc_properties: properties_ref
                                .get(file_md.get_name().as_str().trim_start_matches('/'))
                                .and_then(|mvcc| {
                                    RocksMvccProperties::decode(mvcc.user_collected_properties())
                                        .ok()
                                }),
                        }
                    })
            })
            .collect();
        Ok(metas)
    }
}

impl VersionedLsmExt for RocksEngine {
    type Version = RocksVersion;

    fn lock_current_version(&self, on_cf: CfName) -> Result<Self::Version, String> {
        let mut opts = ReadOptions::new();
        opts.set_fill_cache(false);
        let db = self.as_inner();
        let cf_handle = db
            .cf_handle(on_cf)
            .ok_or_else(|| format!("InvalidArgument: no cf named {on_cf}"))?;
        let iter = DBIterator::new_cf(self.get_sync_db(), cf_handle, opts);
        let ver = RocksVersion {
            iter,
            eng: self.clone(),
            cf: on_cf,
            db_path: PathBuf::from(db.path()),
        };
        Ok(ver)
    }
}
