// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use engine_traits::{KvEngine, CF_DEFAULT, CF_WRITE};

use futures::executor::block_on;
use raftstore::{
    coprocessor::{ObserveHandle, RegionInfoProvider},
    router::RaftStoreRouter,
    store::{fsm::ChangeObserver, Callback, SignificantMsg},
};
use tikv::storage::{
    kv::StatisticsSummary,
    mvcc::{DeltaScanner, ScannerBuilder},
    txn::{EntryBatch, TxnEntry, TxnEntryScanner},
    Snapshot, Statistics,
};
use tikv_util::{box_err, info, warn};
use txn_types::{Key, TimeStamp};

use crate::{
    annotate,
    errors::{Error, Result},
    utils::RegionPager,
};
use crate::{
    metrics,
    router::{ApplyEvent, Router},
};

use kvproto::{kvrpcpb::ExtraOp, metapb::Region};

/// EventLoader transforms data from the snapshot into ApplyEvent.
pub struct EventLoader<S: Snapshot> {
    scanner: DeltaScanner<S>,
    region_id: u64,
}

impl<S: Snapshot> EventLoader<S> {
    pub fn load_from(
        snapshot: S,
        from_ts: TimeStamp,
        to_ts: TimeStamp,
        region: &Region,
    ) -> Result<Self> {
        let region_id = region.get_id();
        let scanner = ScannerBuilder::new(snapshot, to_ts)
            .range(
                Some(Key::from_encoded_slice(&region.start_key)),
                Some(Key::from_encoded_slice(&region.end_key)),
            )
            .hint_min_ts(Some(from_ts))
            .fill_cache(false)
            .build_delta_scanner(from_ts, ExtraOp::Noop)
            .map_err(|err| {
                annotate!(
                    err,
                    "failed to create entry scanner from_ts = {}, to_ts = {}, region = {}",
                    from_ts,
                    to_ts,
                    region_id
                )
            })?;

        Ok(Self { scanner, region_id })
    }

    /// scan a batch of events from the snapshot.
    /// note: maybe make something like [`EntryBatch`] for reducing allocation.
    fn scan_batch(
        &mut self,
        batch_size: usize,
        result: &mut Vec<ApplyEvent>,
    ) -> Result<Statistics> {
        let mut b = EntryBatch::with_capacity(batch_size);
        self.scanner.scan_entries(&mut b)?;
        for entry in b.drain() {
            match entry {
                TxnEntry::Prewrite {
                    default: (key, value),
                    ..
                } => {
                    if !key.is_empty() {
                        result.push(ApplyEvent::from_prewrite(key, value, self.region_id));
                    }
                }
                TxnEntry::Commit { default, write, .. } => {
                    let write =
                        ApplyEvent::from_committed(CF_WRITE, write.0, write.1, self.region_id)?;
                    result.push(write);
                    if !default.0.is_empty() {
                        let default = ApplyEvent::from_committed(
                            CF_DEFAULT,
                            default.0,
                            default.1,
                            self.region_id,
                        )?;
                        result.push(default);
                    }
                }
            }
        }
        Ok(self.scanner.take_statistics())
    }
}

/// The context for loading incremental data between range.
/// Like [`cdc::Initializer`], but supports initialize over range.
/// Note: maybe we can merge those two structures?
#[derive(Clone)]
pub struct InitialDataLoader<E, R, RT> {
    router: RT,
    regions: R,
    // Note: maybe we can make it an abstract thing like `EventSink` with
    //       method `async (KvEvent) -> Result<()>`?
    sink: Router,
    store_id: u64,

    _engine: PhantomData<E>,
}

impl<E, R, RT> InitialDataLoader<E, R, RT>
where
    E: KvEngine,
    R: RegionInfoProvider + Clone + 'static,
    RT: RaftStoreRouter<E>,
{
    pub fn new(router: RT, regions: R, sink: Router, store_id: u64) -> Self {
        Self {
            router,
            regions,
            sink,
            store_id,
            _engine: PhantomData,
        }
    }

    /// Start observe over some region.
    /// This will register the region to the raftstore as observing,
    /// and return the current snapshot of that region.
    pub fn observe_over(&self, region: &Region, cmd: ChangeObserver) -> Result<impl Snapshot> {
        // There are 2 ways for getting the initial snapshot of a region:
        //   1. the BR method: use the interface in the RaftKv interface, read the key-values directly.
        //   2. the CDC method: use the raftstore message `SignificantMsg::CaptureChange` to
        //      register the region to CDC observer and get a snapshot at the same time.
        // Registering the observer to the raftstore is necessary because we should only listen events from leader.
        // In CDC, the change observer is per-delegate(i.e. per-region), we can create the command per-region here too.

        let (callback, fut) = tikv_util::future::paired_future_callback();
        self.router
            .significant_send(
                region.id,
                SignificantMsg::CaptureChange {
                    cmd,
                    region_epoch: region.get_region_epoch().clone(),
                    callback: Callback::Read(Box::new(|snapshot| callback(snapshot.snapshot))),
                },
            )
            .map_err(|err| {
                annotate!(
                    err,
                    "failed to register the observer to region {}",
                    region.get_id()
                )
            })?;
        let snap = block_on(fut)
            .expect("BUG: channel of paired_future_callback canceled.")
            .ok_or_else(|| {
                Error::Other(box_err!(
                    "failed to get initial snapshot: the channel is dropped (region_id = {})",
                    region.get_id()
                ))
            })?;
        // Note: maybe warp the snapshot via `RegionSnapshot`?
        Ok(snap)
    }

    /// Initialize the region: register it to the raftstore and the observer.
    pub fn initialize_region(
        &self,
        region: &Region,
        start_ts: TimeStamp,
        cmd: ChangeObserver,
    ) -> Result<Statistics> {
        let snap = self.observe_over(region, cmd)?;
        let mut event_loader = EventLoader::load_from(snap, start_ts, TimeStamp::max(), region)?;
        let mut stats = StatisticsSummary::default();
        loop {
            let mut events = Vec::with_capacity(2048);
            let stat = event_loader.scan_batch(1024, &mut events)?;
            if events.is_empty() {
                break;
            }
            stats.add_statistics(&stat);
            let sink = self.sink.clone();
            // Note: maybe we'd better don't spawn it to another thread for preventing OOM?
            tokio::spawn(async move {
                for event in events {
                    metrics::INCREMENTAL_SCAN_SIZE.observe(event.size() as f64);
                    if let Err(err) = sink.on_event(event).await {
                        warn!("failed to send event to sink"; "err" => %err);
                    }
                }
            });
        }
        Ok(stats.stat)
    }

    pub fn initialize_range(
        &self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        start_ts: TimeStamp,
        mut on_register_range: impl FnMut(u64, ObserveHandle),
    ) -> Result<Statistics> {
        let mut pager = RegionPager::scan_from(self.regions.clone(), start_key, end_key);
        let mut total_stat = StatisticsSummary::default();
        loop {
            let regions = pager.next_page(8)?;
            info!("scanning for entries in region."; "regions" => ?regions);
            if regions.len() == 0 {
                break;
            }
            for r in regions {
                let handle = ObserveHandle::new();
                let ob = ChangeObserver::from_cdc(r.region.get_id(), handle.clone());
                let stat = self.initialize_region(&r.region, start_ts, ob)?;
                on_register_range(r.region.get_id(), handle);
                total_stat.add_statistics(&stat);
            }
        }
        Ok(total_stat.stat)
    }
}
