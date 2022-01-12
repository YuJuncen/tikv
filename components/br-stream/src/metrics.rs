// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref HANDLE_EVENT_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_stream_event_handle_duration_sec",
        "The duration of handling an cmd batch.",
        &["stage"],
        exponential_buckets(0.001, 2.0, 16).unwrap()
    )
    .unwrap();
    pub static ref HANDLE_KV_HISTOGRAM: Histogram = register_histogram!(
        "tikv_stream_handle_kv_batch",
        "The total kv pair change handle by the stream backup",
        exponential_buckets(1.0, 2.0, 16).unwrap()
    )
    .unwrap();
    pub static ref INCREMENTAL_SCAN_SIZE: Histogram = register_histogram!(
        "tikv_stream_incremental_scan_bytes",
        "The size of scanning.",
        exponential_buckets(64.0, 2.0, 16).unwrap()
    )
    .unwrap();
    pub static ref SKIP_KV_COUNTER: Counter = register_counter!(
        "tikv_stream_skip_kv_count",
        "The total kv size skipped by the streaming",
    )
    .unwrap();
}
