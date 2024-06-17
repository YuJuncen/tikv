use std::time::Duration;

use derive_more::{Add, AddAssign};

#[derive(Default, Debug, Add, AddAssign)]
pub struct LoadMetaStatistic {
    pub meta_files_in: u64,
    pub physical_bytes_loaded: u64,
    pub physical_data_files_in: u64,
    pub logical_data_files_in: u64,
    pub load_file_duration: Duration,
}

#[derive(Default, Debug, Add, AddAssign)]
pub struct LoadStatistic {
    pub files_in: u64,
    pub keys_in: u64,
    pub physical_bytes_in: u64,
    pub logical_key_bytes_in: u64,
    pub logical_value_bytes_in: u64,
    pub error_during_downloading: u64,
}

impl LoadStatistic {
    pub fn merge_with(&mut self, other: &Self) {
        *self += other;
    }
}

#[derive(Default, Debug, Add, AddAssign)]
pub struct CompactStatistic {
    pub keys_out: u64,
    pub physical_bytes_out: u64,
    pub logical_key_bytes_out: u64,
    pub logical_value_bytes_out: u64,

    pub write_sst_duration: Duration,
    pub load_duration: Duration,
    pub sort_duration: Duration,
    pub save_duration: Duration,

    pub empty_generation: u64,
}

impl CompactStatistic {
    pub fn merge_with(&mut self, other: &Self) {
        *self += other;
    }
}
