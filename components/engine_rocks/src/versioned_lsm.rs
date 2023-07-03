// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{path::PathBuf, sync::Arc};

use engine_traits::{CfName, LsmVersion, SstMetaData, VersionedLsmExt};
use rocksdb::{CFHandle, DBIterator, ReadOptions, SstFileMetaData, DB};

use crate::RocksEngine;

pub struct RocksVersion {
    handler: rocksdb::DBIterator<Arc<DB>>,
    cf: CfName,
    db_path: PathBuf,
}

impl LsmVersion for RocksVersion {
    fn get_all_files(&self) -> Result<Vec<engine_traits::SstMetaData>, String> {
        let metas = self.handler.get_column_family_meta_data()?;
        let metas = metas
            .get_levels()
            .into_iter()
            .enumerate()
            .flat_map(|(level, level_md)| {
                level_md.get_files().into_iter().map(move |file_md| {
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
            handler: iter,
            cf: on_cf,
            db_path: PathBuf::from(db.path()),
        };
        Ok(ver)
    }
}
