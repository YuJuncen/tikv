// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use crossbeam::channel::{Receiver as SyncReceiver, Sender as SyncSender};
use crossbeam_channel::SendError;
use engine_traits::KvEngine;
use error_code::{backup_stream as errs, ErrorCodeExt};
use futures::FutureExt;
use kvproto::metapb::Region;
use raft::StateRole;
use raftstore::{
    coprocessor::{ObserveHandle, RegionInfoProvider},
    router::RaftStoreRouter,
    store::fsm::ChangeObserver,
};
use resolved_ts::LeadershipResolver;
use tikv::storage::Statistics;
use tikv_util::{box_err, debug, info, time::Instant, warn, worker::Scheduler};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use txn_types::TimeStamp;
use yatp::task::callback::Handle as YatpHandle;

use crate::{
    annotate,
    endpoint::ObserveOp,
    errors::{Error, Result},
    event_loader::InitialDataLoader,
    future,
    metadata::{store::MetaStore, CheckpointProvider, MetadataClient},
    metrics,
    router::{Router, TaskSelector},
    subscription_track::{ResolveResult, SubscriptionTracer},
    try_send,
    utils::{self, CallbackWaitGroup, Work},
    Task,
};

type ScanPool = yatp::ThreadPool<yatp::task::callback::TaskCell>;

const INITIAL_SCAN_FAILURE_MAX_RETRY_TIME: usize = 10;
const RETRY_AWAIT_INTERVAL: Duration = Duration::from_secs(2);
const TRY_START_OBSERVE_MAX_RETRY_TIME: u8 = 12;

/// a request for doing initial scanning.
struct ScanCmd {
    region: Region,
    handle: ObserveHandle,
    last_checkpoint: TimeStamp,
    _work: Work,
}

/// The response of requesting resolve the new checkpoint of regions.
pub struct ResolvedRegions {
    items: Vec<ResolveResult>,
    checkpoint: TimeStamp,
}

impl ResolvedRegions {
    /// Compose the calculated global checkpoint and region checkpoints.
    /// Note: Maybe we can compute the global checkpoint internal and getting
    /// the interface clear. However we must take the `min_ts` or we cannot
    /// provide valid global checkpoint if there isn't any region checkpoint.
    pub fn new(checkpoint: TimeStamp, checkpoints: Vec<ResolveResult>) -> Self {
        Self {
            items: checkpoints,
            checkpoint,
        }
    }

    /// take the region checkpoints from the structure.
    #[deprecated = "please use `take_resolve_result` instead."]
    pub fn take_region_checkpoints(&mut self) -> Vec<(Region, TimeStamp)> {
        std::mem::take(&mut self.items)
            .into_iter()
            .map(|x| (x.region, x.checkpoint))
            .collect()
    }

    /// take the resolve result from this struct.
    pub fn take_resolve_result(&mut self) -> Vec<ResolveResult> {
        std::mem::take(&mut self.items)
    }

    /// get the global checkpoint.
    pub fn global_checkpoint(&self) -> TimeStamp {
        self.checkpoint
    }
}

/// returns whether the error should be retried.
/// for some errors, like `epoch not match` or `not leader`,
/// implies that the region is drifting, and no more need to be observed by us.
fn should_retry(err: &Error) -> bool {
    match err.without_context() {
        Error::RaftRequest(pbe) => {
            !(pbe.has_epoch_not_match()
                || pbe.has_not_leader()
                || pbe.get_message().contains("stale observe id")
                || pbe.has_region_not_found())
        }
        Error::RaftStore(raftstore::Error::RegionNotFound(_))
        | Error::RaftStore(raftstore::Error::NotLeader(..))
        | Error::ObserveCanceled(..)
        | Error::RaftStore(raftstore::Error::EpochNotMatch(..)) => false,
        _ => true,
    }
}

/// the abstraction over a "DB" which provides the initial scanning.
trait InitialScan: Clone {
    fn do_initial_scan(
        &self,
        region: &Region,
        start_ts: TimeStamp,
        handle: ObserveHandle,
    ) -> Result<Statistics>;

    fn handle_fatal_error(&self, region: &Region, err: Error);
}

impl<E, R, RT> InitialScan for InitialDataLoader<E, R, RT>
where
    E: KvEngine,
    R: RegionInfoProvider + Clone + 'static,
    RT: RaftStoreRouter<E>,
{
    fn do_initial_scan(
        &self,
        region: &Region,
        start_ts: TimeStamp,
        handle: ObserveHandle,
    ) -> Result<Statistics> {
        let region_id = region.get_id();
        let h = handle.clone();
        // Note: we have external retry at `ScanCmd::exec_by_with_retry`, should we keep
        // retrying here?
        let snap = self.observe_over_with_retry(region, move || {
            ChangeObserver::from_pitr(region_id, handle.clone())
        })?;
        #[cfg(feature = "failpoints")]
        fail::fail_point!("scan_after_get_snapshot");
        let stat = self.do_initial_scan(region, h, start_ts, snap)?;
        Ok(stat)
    }

    fn handle_fatal_error(&self, region: &Region, err: Error) {
        try_send!(
            self.scheduler,
            Task::FatalError(
                TaskSelector::ByRange(
                    region.get_start_key().to_owned(),
                    region.get_end_key().to_owned()
                ),
                Box::new(err),
            )
        );
    }
}

impl ScanCmd {
    /// execute the initial scanning via the specificated [`InitialDataLoader`].
    fn exec_by(&self, initial_scan: impl InitialScan) -> Result<()> {
        let Self {
            region,
            handle,
            last_checkpoint,
            ..
        } = self;
        let begin = Instant::now_coarse();
        let stat = initial_scan.do_initial_scan(region, *last_checkpoint, handle.clone())?;
        info!("initial scanning finished!"; "takes" => ?begin.saturating_elapsed(), "from_ts" => %last_checkpoint, utils::slog_region(region));
        utils::record_cf_stat("lock", &stat.lock);
        utils::record_cf_stat("write", &stat.write);
        utils::record_cf_stat("default", &stat.data);
        Ok(())
    }

    /// execute the command, when meeting error, retrying.
    fn exec_by_with_retry(self, init: impl InitialScan, cancel: &AtomicBool) {
        let mut retry_time = INITIAL_SCAN_FAILURE_MAX_RETRY_TIME;
        loop {
            if cancel.load(Ordering::SeqCst) {
                return;
            }
            match self.exec_by(init.clone()) {
                Err(err) if should_retry(&err) && retry_time > 0 => {
                    // NOTE: blocking this thread may stick the process.
                    // Maybe spawn a task to tokio and reschedule the task then?
                    std::thread::sleep(Duration::from_millis(500));
                    warn!("meet retryable error"; "err" => %err, "retry_time" => retry_time);
                    retry_time -= 1;
                    continue;
                }
                Err(err) if retry_time == 0 => {
                    init.handle_fatal_error(&self.region, err.context("retry time exceeds"));
                    break;
                }
                // Errors which `should_retry` returns false means they can be ignored.
                Err(_) | Ok(_) => break,
            }
        }
    }
}

fn scan_executor_loop(
    init: impl InitialScan,
    cmds: SyncReceiver<ScanCmd>,
    canceled: Arc<AtomicBool>,
) {
    while let Ok(cmd) = cmds.recv() {
        fail::fail_point!("execute_scan_command");
        debug!("handling initial scan request"; "region_id" => %cmd.region.get_id());
        metrics::PENDING_INITIAL_SCAN_LEN
            .with_label_values(&["queuing"])
            .dec();
        if canceled.load(Ordering::Acquire) {
            return;
        }

        metrics::PENDING_INITIAL_SCAN_LEN
            .with_label_values(&["executing"])
            .inc();
        cmd.exec_by_with_retry(init.clone(), &canceled);
        metrics::PENDING_INITIAL_SCAN_LEN
            .with_label_values(&["executing"])
            .dec();
    }
}

/// spawn the executors in the scan pool.
/// we make workers thread instead of spawn scan task directly into the pool
/// because the [`InitialDataLoader`] isn't `Sync` hence we must use it very
/// carefully or rustc (along with tokio) would complain that we made a `!Send`
/// future. so we have moved the data loader to the synchronous context so its
/// reference won't be shared between threads any more.
fn spawn_executors(init: impl InitialScan + Send + 'static, number: usize) -> ScanPoolHandle {
    let (tx, rx) = crossbeam::channel::bounded(MESSAGE_BUFFER_SIZE);
    let pool = create_scan_pool(number);
    let stopped = Arc::new(AtomicBool::new(false));
    for _ in 0..number {
        let init = init.clone();
        let rx = rx.clone();
        let stopped = stopped.clone();
        pool.spawn(move |_: &mut YatpHandle<'_>| {
            tikv_alloc::add_thread_memory_accessor();
            let _io_guard = file_system::WithIoType::new(file_system::IoType::Replication);
            scan_executor_loop(init, rx, stopped);
            tikv_alloc::remove_thread_memory_accessor();
        })
    }
    ScanPoolHandle {
        tx,
        _pool: pool,
        stopped,
    }
}

struct ScanPoolHandle {
    tx: SyncSender<ScanCmd>,
    stopped: Arc<AtomicBool>,

    // in fact, we won't use the pool any more.
    // but we should hold the reference to the pool so it won't try to join the threads running.
    _pool: ScanPool,
}

impl Drop for ScanPoolHandle {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
    }
}

impl ScanPoolHandle {
    fn request(&self, cmd: ScanCmd) -> std::result::Result<(), SendError<ScanCmd>> {
        if self.stopped.load(Ordering::Acquire) {
            warn!("scan pool is stopped, ignore the scan command"; "region" => %cmd.region.get_id());
            return Ok(());
        }
        metrics::PENDING_INITIAL_SCAN_LEN
            .with_label_values(&["queuing"])
            .inc();
        self.tx.send(cmd)
    }
}

/// The default channel size.
const MESSAGE_BUFFER_SIZE: usize = 32768;

#[derive(Clone)]
pub struct Handle(Sender<ObserveOp>);

impl Handle {
    /// send an operation request to the manager.
    /// the returned future would be resolved after send is success.
    /// the opeartion would be executed asynchronously.
    pub async fn request(&self, op: ObserveOp) {
        if let Err(err) = self.0.send(op).await {
            annotate!(err, "BUG: region operator channel closed.")
                .report("when executing region op");
        }
    }
}

/// The operator for region subscription.
/// It make a queue for operations over the `SubscriptionTracer`, generally,
/// we should only modify the `SubscriptionTracer` itself (i.e. insert records,
/// remove records) at here. So the order subscription / desubscription won't be
/// broken.
pub struct RegionSubscriptionManager<S, R> {
    // Note: these fields appear everywhere, maybe make them a `context` type?
    regions: R,
    meta_cli: MetadataClient<S>,
    range_router: Router,
    scheduler: Scheduler<Task>,
    subs: SubscriptionTracer,
    scan_pool_handle: Arc<ScanPoolHandle>,
    scans: Arc<CallbackWaitGroup>,

    stat: Statistic,
}

/// Some information recorded for testing or something.
/// This should be in `#[cfg(test)]`, however that would make this unusable in
/// integration tests.
#[derive(Default)]
pub struct Statistic {
    event_retry_start: usize,
    event_retry_request: usize,
    event_retry_skipped: usize,
}

impl Statistic {
    fn skip_retry(&mut self, tag: &'static str) {
        metrics::SKIP_RETRY.with_label_values(&[tag]).inc();
        self.event_retry_skipped += 1;
    }
}

/// Create a yatp pool for doing initial scanning.
fn create_scan_pool(num_threads: usize) -> ScanPool {
    yatp::Builder::new("log-backup-scan")
        .max_thread_count(num_threads)
        .build_callback_pool()
}

impl<S, R> RegionSubscriptionManager<S, R>
where
    S: MetaStore + 'static,
    R: RegionInfoProvider + Clone + 'static,
{
    /// create a [`RegionSubscriptionManager`].
    ///
    /// # returns
    ///
    /// a two-tuple, the first is the handle to the manager, the second is the
    /// operator loop future.
    pub fn start<E, RT>(
        initial_loader: InitialDataLoader<E, R, RT>,
        meta_cli: MetadataClient<S>,
        scan_pool_size: usize,
        leader_checker: LeadershipResolver,
    ) -> (Handle, future![()])
    where
        E: KvEngine,
        RT: RaftStoreRouter<E> + 'static,
    {
        let (tx, rx) = channel(MESSAGE_BUFFER_SIZE);
        let handle = Handle(tx);
        let scan_pool_handle = spawn_executors(initial_loader.clone(), scan_pool_size);
        let op = Self {
            regions: initial_loader.regions.clone(),
            meta_cli,
            range_router: initial_loader.sink.clone(),
            scheduler: initial_loader.scheduler.clone(),
            subs: initial_loader.tracing,
            scan_pool_handle: Arc::new(scan_pool_handle),
            scans: CallbackWaitGroup::new(),
            stat: Default::default(),
        };
        let fut = op.region_operator_loop(rx, leader_checker);
        (handle, fut)
    }

    /// wait initial scanning get finished.
    fn wait(&self, timeout: Duration) -> future![bool] {
        tokio::time::timeout(timeout, self.scans.wait()).map(|result| result.is_err())
    }

    /// the handler loop.
    async fn region_operator_loop(
        mut self,
        mut message_box: Receiver<ObserveOp>,
        mut leader_checker: LeadershipResolver,
    ) {
        while let Some(op) = message_box.recv().await {
            info!("backup stream: on_modify_observe"; "op" => ?op);
            match op {
                ObserveOp::Start { region } => {
                    fail::fail_point!("delay_on_start_observe");
                    self.start_observe(region).await;
                    metrics::INITIAL_SCAN_REASON
                        .with_label_values(&["leader-changed"])
                        .inc();
                }
                ObserveOp::Stop { ref region } => {
                    self.subs.deregister_region_if(region, |_, _| true);
                }
                ObserveOp::Destroy { ref region } => {
                    self.subs.deregister_region_if(region, |old, new| {
                        raftstore::store::util::compare_region_epoch(
                            old.meta.get_region_epoch(),
                            new,
                            true,
                            true,
                            false,
                        )
                        .map_err(|err| warn!("check epoch and stop failed."; utils::slog_region(region), "err" => %err))
                        .is_ok()
                    });
                }
                ObserveOp::RefreshResolver { ref region } => self.refresh_resolver(region).await,
                ObserveOp::NotifyFailToStartObserve {
                    region,
                    handle,
                    err,
                    has_failed_for,
                } => {
                    info!("retry observe region"; "region" => %region.get_id(), "err" => %err);
                    // No need for retrying observe canceled.
                    let err_code = ErrorCodeExt::error_code(&*err);
                    if err_code == errs::OBSERVE_CANCELED || err_code == errs::NO_SUCH_TASK {
                        return;
                    }
                    let (start, end) = (
                        region.get_start_key().to_owned(),
                        region.get_end_key().to_owned(),
                    );
                    match self.retry_observe(region, handle, has_failed_for).await {
                        Ok(()) => {}
                        Err(e) => {
                            let msg = Task::FatalError(
                                TaskSelector::ByRange(start, end),
                                Box::new(Error::Contextual {
                                    context: format!("retry meet error, origin error is {}", err),
                                    inner_error: Box::new(e),
                                }),
                            );
                            try_send!(self.scheduler, msg);
                        }
                    }
                }
                ObserveOp::ResolveRegions { callback, min_ts } => {
                    let now = Instant::now();
                    let timedout = self.wait(Duration::from_secs(5)).await;
                    if timedout {
                        warn!("waiting for initial scanning done timed out, forcing progress!"; 
                            "take" => ?now.saturating_elapsed(), "timedout" => %timedout);
                    }
                    let regions = leader_checker
                        .resolve(self.subs.current_regions(), min_ts)
                        .await;
                    let cps = self.subs.resolve_with(min_ts, regions);
                    let min_region = cps.iter().min_by_key(|rs| rs.checkpoint);
                    // If there isn't any region observed, the `min_ts` can be used as resolved ts
                    // safely.
                    let rts = min_region.map(|rs| rs.checkpoint).unwrap_or(min_ts);
                    info!("getting checkpoint"; "defined_by_region" => ?min_region);
                    callback(ResolvedRegions::new(rts, cps));
                }
            }
        }
    }

    async fn refresh_resolver(&self, region: &Region) {
        let need_refresh_all = !self.subs.try_update_region(region);

        if need_refresh_all {
            let canceled = self.subs.deregister_region_if(region, |_, _| true);
            let handle = ObserveHandle::new();
            if canceled {
                if let Some(for_task) = self.find_task_by_region(region) {
                    metrics::INITIAL_SCAN_REASON
                        .with_label_values(&["region-changed"])
                        .inc();
                    let r = async {
                        self.subs.add_pending_region(region);
                        self.observe_over_with_initial_data_from_checkpoint(
                            region,
                            self.get_last_checkpoint_of(&for_task, region).await?,
                            handle.clone(),
                        );
                        Result::Ok(())
                    }
                    .await;
                    if let Err(e) = r {
                        try_send!(
                            self.scheduler,
                            Task::ModifyObserve(ObserveOp::NotifyFailToStartObserve {
                                region: region.clone(),
                                handle,
                                err: Box::new(e),
                                has_failed_for: 0,
                            })
                        );
                    }
                } else {
                    warn!(
                        "BUG: the region {:?} is register to no task but being observed",
                        utils::debug_region(region)
                    );
                }
            }
        }
    }

    async fn try_start_observe(&self, region: &Region, handle: ObserveHandle) -> Result<()> {
        match self.find_task_by_region(region) {
            None => {
                warn!(
                    "the region {:?} is register to no task but being observed (start_key = {}; end_key = {}; task_stat = {:?}): maybe stale, aborting",
                    region,
                    utils::redact(&region.get_start_key()),
                    utils::redact(&region.get_end_key()),
                    self.range_router
                );
            }

            Some(for_task) => {
                // the extra failpoint is used to pause the thread.
                // once it triggered "pause" it cannot trigger early return then.
                fail::fail_point!("try_start_observe0");
                fail::fail_point!("try_start_observe", |_| {
                    Err(Error::Other(box_err!("Nature is boring")))
                });
                let tso = self.get_last_checkpoint_of(&for_task, region).await?;
                self.observe_over_with_initial_data_from_checkpoint(region, tso, handle.clone());
            }
        }
        Ok(())
    }

    async fn start_observe(&mut self, region: Region) {
        self.retry_start_observe(region, 0).await
    }

    async fn retry_start_observe(&mut self, region: Region, has_failed_for: u8) {
        let handle = ObserveHandle::new();
        let schd = self.scheduler.clone();
        self.subs.add_pending_region(&region);
        if let Err(err) = self.try_start_observe(&region, handle.clone()).await {
            warn!("failed to start observe, would retry"; "err" => %err, utils::slog_region(&region));
            self.stat.event_retry_request += 1;
            tokio::spawn(async move {
                #[cfg(not(feature = "failpoints"))]
                let delay = RETRY_AWAIT_INTERVAL;
                #[cfg(feature = "failpoints")]
                let delay = (|| {
                    fail::fail_point!("subscribe_mgr_retry_start_observe_delay", |v| {
                        let dur = v
                            .expect("should provide delay time (in ms)")
                            .parse::<u64>()
                            .expect("should be number (in ms)");
                        Duration::from_millis(dur)
                    });
                    RETRY_AWAIT_INTERVAL
                })();
                tokio::time::sleep(delay).await;
                try_send!(
                    schd,
                    Task::ModifyObserve(ObserveOp::NotifyFailToStartObserve {
                        region,
                        handle,
                        err: Box::new(err),
                        has_failed_for: has_failed_for + 1
                    })
                )
            });
        }
    }

    async fn retry_observe(
        &mut self,
        region: Region,
        handle: ObserveHandle,
        failure_count: u8,
    ) -> Result<()> {
        if failure_count > TRY_START_OBSERVE_MAX_RETRY_TIME {
            return Err(Error::Other(
                format!(
                    "retry time exceeds for region {:?}",
                    utils::debug_region(&region)
                )
                .into(),
            ));
        }

        let (tx, rx) = crossbeam::channel::bounded(1);
        self.regions
            .find_region_by_id(
                region.get_id(),
                Box::new(move |item| {
                    tx.send(item)
                        .expect("BUG: failed to send to newly created channel.");
                }),
            )
            .map_err(|err| {
                annotate!(
                    err,
                    "failed to send request to region info accessor, server maybe too too too busy. (region id = {})",
                    region.get_id()
                )
            })?;
        let new_region_info = rx
            .recv()
            .map_err(|err| annotate!(err, "BUG?: unexpected channel message dropped."))?;
        if new_region_info.is_none() {
            self.stat.skip_retry("region-absent");
            return Ok(());
        }
        let new_region_info = new_region_info.unwrap();
        if new_region_info.role != StateRole::Leader {
            self.stat.skip_retry("not-leader");
            return Ok(());
        }
        // Note: we may fail before we insert the region info to the subscription map.
        // At that time, the command isn't steal and we should retry it.
        let mut exists = false;
        let removed = self.subs.deregister_region_if(&region, |old, _| {
            exists = true;
            let should_remove = old.handle().id == handle.id;
            if !should_remove {
                warn!("stale retry command"; utils::slog_region(&region), "handle" => ?handle, "old_handle" => ?old.handle());
            }
            should_remove
        });
        if !removed && exists {
            self.stat.skip_retry("stale-command");
            return Ok(());
        }
        metrics::INITIAL_SCAN_REASON
            .with_label_values(&["retry"])
            .inc();
        self.stat.event_retry_start += 1;
        self.retry_start_observe(region, failure_count).await;
        Ok(())
    }

    async fn get_last_checkpoint_of(&self, task: &str, region: &Region) -> Result<TimeStamp> {
        fail::fail_point!("get_last_checkpoint_of", |hint| Err(Error::Other(
            box_err!(
                "get_last_checkpoint_of({}, {:?}) failed because {:?}",
                task,
                region,
                hint
            )
        )));
        let meta_cli = self.meta_cli.clone();
        let cp = meta_cli.get_region_checkpoint(task, region).await?;
        debug!("got region checkpoint"; "region_id" => %region.get_id(), "checkpoint" => ?cp);
        if matches!(cp.provider, CheckpointProvider::Global) {
            metrics::STORE_CHECKPOINT_TS
                .with_label_values(&[task])
                .set(cp.ts.into_inner() as _);
        }
        Ok(cp.ts)
    }

    fn spawn_scan(&self, cmd: ScanCmd) {
        // we should not spawn initial scanning tasks to the tokio blocking pool
        // because it is also used for converting sync File I/O to async. (for now!)
        // In that condition, if we blocking for some resources(for example, the
        // `MemoryQuota`) at the block threads, we may meet some ghosty
        // deadlock.
        let s = self.scan_pool_handle.request(cmd);
        if let Err(err) = s {
            let region_id = err.0.region.get_id();
            annotate!(err, "BUG: scan_pool closed")
                .report(format!("during initial scanning for region {}", region_id));
        }
    }

    fn observe_over_with_initial_data_from_checkpoint(
        &self,
        region: &Region,
        last_checkpoint: TimeStamp,
        handle: ObserveHandle,
    ) {
        self.subs
            .register_region(region, handle.clone(), Some(last_checkpoint));
        self.spawn_scan(ScanCmd {
            region: region.clone(),
            handle,
            last_checkpoint,
            _work: self.scans.clone().work(),
        })
    }

    fn find_task_by_region(&self, r: &Region) -> Option<String> {
        self.range_router
            .find_task_by_range(&r.start_key, &r.end_key)
    }
}

#[cfg(test)]
mod test {
    use kvproto::metapb::Region;
    use tikv::storage::Statistics;

    use super::InitialScan;

    #[derive(Clone, Copy)]
    struct NoopInitialScan;

    impl InitialScan for NoopInitialScan {
        fn do_initial_scan(
            &self,
            _region: &Region,
            _start_ts: txn_types::TimeStamp,
            _handle: raftstore::coprocessor::ObserveHandle,
        ) -> crate::errors::Result<tikv::storage::Statistics> {
            Ok(Statistics::default())
        }

        fn handle_fatal_error(&self, region: &Region, err: crate::errors::Error) {
            panic!("fatal {:?} {}", region, err)
        }
    }

    #[test]
    #[cfg(feature = "failpoints")]
    fn test_message_delay_and_exit() {
        use std::time::Duration;

        use super::ScanCmd;
        use crate::{subscription_manager::spawn_executors, utils::CallbackWaitGroup};

        fn should_finish_in(f: impl FnOnce() + Send + 'static, d: std::time::Duration) {
            let (tx, rx) = futures::channel::oneshot::channel();
            std::thread::spawn(move || {
                f();
                tx.send(()).unwrap();
            });
            let pool = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let _e = pool.handle().enter();
            pool.block_on(tokio::time::timeout(d, rx)).unwrap().unwrap();
        }

        let pool = spawn_executors(NoopInitialScan, 1);
        let wg = CallbackWaitGroup::new();
        fail::cfg("execute_scan_command", "sleep(100)").unwrap();
        for _ in 0..100 {
            let wg = wg.clone();
            pool.request(ScanCmd {
                region: Default::default(),
                handle: Default::default(),
                last_checkpoint: Default::default(),
                // Note: Maybe make here a Box<dyn FnOnce()> or some other trait?
                _work: wg.work(),
            })
            .unwrap()
        }

        should_finish_in(move || drop(pool), Duration::from_secs(5));
    }
}
