// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{convert::AsRef, fmt, marker::PhantomData, path::PathBuf, sync::Arc, time::Duration};

use concurrency_manager::ConcurrencyManager;
use engine_traits::KvEngine;
use error_code::ErrorCodeExt;
use futures::{Future, FutureExt};
use kvproto::{
    brpb::{StreamBackupError, StreamBackupTaskInfo},
    metapb::Region,
};
use online_config::ConfigChange;
use pd_client::PdClient;
use raftstore::{
    coprocessor::{CmdBatch, ObserveHandle, RegionInfoProvider},
    router::RaftStoreRouter,
};
use tikv::config::BackupStreamConfig;
use tikv_util::{
    config::ReadableDuration,
    debug, defer, error, info,
    time::Instant,
    warn,
    worker::{Runnable, Scheduler},
    HandyRwLock,
};
use tokio::{
    io::Result as TokioResult,
    runtime::{Handle, Runtime},
};
use tokio_stream::StreamExt;
use txn_types::TimeStamp;

use super::metrics::HANDLE_EVENT_DURATION_HISTOGRAM;
use crate::{
    async_task_manager::RegionSubscriptionManager,
    errors::{Error, Result},
    event_loader::{InitialDataLoader, PendingMemoryQuota},
    metadata::{
        store::{EtcdStore, MetaStore},
        MetadataClient, MetadataEvent, StreamTask,
    },
    metrics::{self, TaskStatus},
    observer::BackupStreamObserver,
    router::{ApplyEvents, Router, FLUSH_STORAGE_INTERVAL},
    subscription_track::SubscriptionTracer,
    try_send,
    utils::{self, StopWatch},
};

const SLOW_EVENT_THRESHOLD: f64 = 120.0;

pub struct Endpoint<S: MetaStore + 'static, R, E, RT, PDC> {
    meta_client: Option<MetadataClient<S>>,
    range_router: Router,
    scheduler: Scheduler<Task>,
    observer: BackupStreamObserver,
    pool: Runtime,
    store_id: u64,
    regions: R,
    engine: PhantomData<E>,
    router: RT,
    pd_client: Arc<PDC>,
    subs: SubscriptionTracer,
    concurrency_manager: ConcurrencyManager,
    initial_scan_memory_quota: PendingMemoryQuota,
    region_operator: RegionSubscriptionManager<S, R, PDC>,
}

impl<S, R, E, RT, PDC> Endpoint<S, R, E, RT, PDC>
where
    R: RegionInfoProvider + 'static + Clone,
    E: KvEngine,
    RT: RaftStoreRouter<E> + 'static,
    PDC: PdClient + 'static,
    S: MetaStore + 'static,
{
    pub fn with_client(
        store_id: u64,
        cli: MetadataClient<S>,
        config: BackupStreamConfig,
        scheduler: Scheduler<Task>,
        observer: BackupStreamObserver,
        accessor: R,
        router: RT,
        pd_client: Arc<PDC>,
        cm: ConcurrencyManager,
    ) -> Self {
        // Always use 2 threads for I/O tasks.
        let pool = create_tokio_runtime(config.io_threads, "br-stream")
            .expect("failed to create tokio runtime for backup stream worker.");

        // TODO consider TLS?
        let meta_client = Some(cli);
        let range_router = Router::new(
            PathBuf::from(config.temp_path.clone()),
            scheduler.clone(),
            config.temp_file_size_limit_per_task.0,
            config.max_flush_interval.0,
        );

        if let Some(meta_client) = meta_client.as_ref() {
            // spawn a worker to watch task changes from etcd periodically.
            let meta_client_clone = meta_client.clone();
            let scheduler_clone = scheduler.clone();
            // TODO build a error handle mechanism #error 2
            pool.spawn(async {
                if let Err(err) =
                    Self::start_and_watch_tasks(meta_client_clone, scheduler_clone).await
                {
                    err.report("failed to start watch tasks");
                }
            });
            pool.spawn(Self::starts_flush_ticks(range_router.clone()));
        }
        let initial_scan_memory_quota =
            PendingMemoryQuota::new(config.initial_scan_pending_memory_quota.0 as _);
        let subs = SubscriptionTracer::default();
        let (region_operator, op_loop) = RegionSubscriptionManager::start(
            InitialDataLoader::new(
                router.clone(),
                accessor.clone(),
                range_router.clone(),
                subs.clone(),
                scheduler.clone(),
                initial_scan_memory_quota.clone(),
                pool.handle().clone(),
            ),
            observer.clone(),
            meta_client.clone(),
            pd_client.clone(),
            config.num_threads,
        );
        pool.spawn(op_loop);
        info!("the endpoint of backup stream started"; "path" => %config.temp_path);
        Endpoint {
            meta_client,
            range_router,
            scheduler,
            observer,
            pool,
            store_id,
            regions: accessor,
            engine: PhantomData,
            router,
            pd_client,
            subs,
            concurrency_manager: cm,
            initial_scan_memory_quota,
            region_operator,
        }
    }
}

impl<R, E, RT, PDC> Endpoint<EtcdStore, R, E, RT, PDC>
where
    R: RegionInfoProvider + 'static + Clone,
    E: KvEngine,
    RT: RaftStoreRouter<E> + 'static,
    PDC: PdClient + 'static,
{
    pub fn new<S: AsRef<str>>(
        store_id: u64,
        endpoints: &dyn AsRef<[S]>,
        config: BackupStreamConfig,
        scheduler: Scheduler<Task>,
        observer: BackupStreamObserver,
        accessor: R,
        router: RT,
        pd_client: Arc<PDC>,
        concurrency_manager: ConcurrencyManager,
    ) -> Endpoint<EtcdStore, R, E, RT, PDC> {
        crate::metrics::STREAM_ENABLED.inc();
        let pool = create_tokio_runtime(config.io_threads, "backup-stream")
            .expect("failed to create tokio runtime for backup stream worker.");

        // TODO consider TLS?
        let meta_client = match pool.block_on(etcd_client::Client::connect(&endpoints, None)) {
            Ok(c) => {
                let meta_store = EtcdStore::from(c);
                Some(MetadataClient::new(meta_store, store_id))
            }
            Err(e) => {
                error!("failed to create etcd client for backup stream worker"; "error" => ?e);
                None
            }
        };

        let range_router = Router::new(
            PathBuf::from(config.temp_path.clone()),
            scheduler.clone(),
            config.temp_file_size_limit_per_task.0,
            config.max_flush_interval.0,
        );

        if let Some(meta_client) = meta_client.as_ref() {
            // spawn a worker to watch task changes from etcd periodically.
            let meta_client_clone = meta_client.clone();
            let scheduler_clone = scheduler.clone();
            // TODO build a error handle mechanism #error 2
            pool.spawn(async {
                if let Err(err) =
                    Self::start_and_watch_tasks(meta_client_clone, scheduler_clone).await
                {
                    err.report("failed to start watch tasks");
                }
            });

            pool.spawn(Self::starts_flush_ticks(range_router.clone()));
        }

        let initial_scan_memory_quota =
            PendingMemoryQuota::new(config.initial_scan_pending_memory_quota.0 as _);
        info!("the endpoint of stream backup started"; "path" => %config.temp_path);
        let subs = SubscriptionTracer::default();
        let (region_operator, op_loop) = RegionSubscriptionManager::start(
            InitialDataLoader::new(
                router.clone(),
                accessor.clone(),
                range_router.clone(),
                subs.clone(),
                scheduler.clone(),
                initial_scan_memory_quota.clone(),
                pool.handle().clone(),
            ),
            observer.clone(),
            meta_client.clone(),
            pd_client.clone(),
            config.num_threads,
        );
        pool.spawn(op_loop);
        Endpoint {
            meta_client,
            range_router,
            scheduler,
            observer,
            pool,
            store_id,
            regions: accessor,
            engine: PhantomData,
            router,
            pd_client,
            subs,
            concurrency_manager,
            initial_scan_memory_quota,
            region_operator,
        }
    }
}

impl<S, R, E, RT, PDC> Endpoint<S, R, E, RT, PDC>
where
    S: MetaStore + 'static,
    R: RegionInfoProvider + Clone + 'static,
    E: KvEngine,
    RT: RaftStoreRouter<E> + 'static,
    PDC: PdClient + 'static,
{
    fn get_meta_client(&self) -> MetadataClient<S> {
        self.meta_client.as_ref().unwrap().clone()
    }

    fn on_fatal_error(&self, task: String, err: Box<Error>) {
        // Let's pause the task locally first.
        let start_ts = self.unload_task(&task).map(|task| task.start_ts);
        err.report_fatal();
        metrics::update_task_status(TaskStatus::Error, &task);

        let meta_cli = self.get_meta_client();
        let store_id = self.store_id;
        let sched = self.scheduler.clone();
        let register_safepoint = self
            .register_service_safepoint(task.clone(), TimeStamp::new(start_ts.unwrap_or_default()));
        self.pool.block_on(async move {
            let err_fut = async {
                register_safepoint.await?;
                meta_cli.pause(&task).await?;
                let mut last_error = StreamBackupError::new();
                last_error.set_error_code(err.error_code().code.to_owned());
                last_error.set_error_message(err.to_string());
                last_error.set_store_id(store_id);
                last_error.set_happen_at(TimeStamp::physical_now());
                meta_cli.report_last_error(&task, last_error).await?;
                Result::Ok(())
            };
            if let Err(err_report) = err_fut.await {
                err_report.report(format_args!("failed to upload error {}", err_report));
                // Let's retry reporting after 5s.
                tokio::task::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    try_send!(sched, Task::FatalError(task, err));
                });
            }
        })
    }

    async fn starts_flush_ticks(router: Router) {
        loop {
            // check every 15s.
            // TODO: maybe use global timer handle in the `tikv_utils::timer` (instead of enabling timing in the current runtime)?
            tokio::time::sleep(Duration::from_secs(FLUSH_STORAGE_INTERVAL / 20)).await;
            debug!("backup stream trigger flush tick");
            router.tick().await;
        }
    }

    // TODO find a proper way to exit watch tasks
    async fn start_and_watch_tasks(
        meta_client: MetadataClient<S>,
        scheduler: Scheduler<Task>,
    ) -> Result<()> {
        let tasks = meta_client.get_tasks().await?;
        for task in tasks.inner {
            info!("backup stream watch task"; "task" => ?task);
            if task.is_paused {
                continue;
            }
            // move task to schedule
            scheduler.schedule(Task::WatchTask(TaskOp::AddTask(task)))?;
        }

        let revision = tasks.revision;
        let meta_client_clone = meta_client.clone();
        let scheduler_clone = scheduler.clone();

        Handle::current().spawn(async move {
            if let Err(err) =
                Self::starts_watch_task(meta_client_clone, scheduler_clone, revision).await
            {
                err.report("failed to start watch tasks");
            }
        });

        Handle::current().spawn(async move {
            if let Err(err) = Self::starts_watch_pause(meta_client, scheduler, revision).await {
                err.report("failed to start watch pause");
            }
        });

        Ok(())
    }

    async fn starts_watch_task(
        meta_client: MetadataClient<S>,
        scheduler: Scheduler<Task>,
        revision: i64,
    ) -> Result<()> {
        let mut watcher = meta_client.events_from(revision).await?;
        loop {
            if let Some(event) = watcher.stream.next().await {
                info!("backup stream watch event from etcd"; "event" => ?event);
                match event {
                    MetadataEvent::AddTask { task } => {
                        scheduler.schedule(Task::WatchTask(TaskOp::AddTask(task)))?;
                    }
                    MetadataEvent::RemoveTask { task } => {
                        scheduler.schedule(Task::WatchTask(TaskOp::RemoveTask(task)))?;
                    }
                    MetadataEvent::Error { err } => err.report("metadata client watch meet error"),
                    _ => panic!("BUG: invalid event {:?}", event),
                }
            }
        }
    }

    async fn starts_watch_pause(
        meta_client: MetadataClient<S>,
        scheduler: Scheduler<Task>,
        revision: i64,
    ) -> Result<()> {
        let mut watcher = meta_client.events_from_pause(revision).await?;
        loop {
            if let Some(event) = watcher.stream.next().await {
                info!("backup stream watch event from etcd"; "event" => ?event);
                match event {
                    MetadataEvent::PauseTask { task } => {
                        scheduler.schedule(Task::WatchTask(TaskOp::PauseTask(task)))?;
                    }
                    MetadataEvent::ResumeTask { task } => {
                        let task = meta_client.get_task(&task).await?;
                        scheduler.schedule(Task::WatchTask(TaskOp::ResumeTask(task)))?;
                    }
                    MetadataEvent::Error { err } => err.report("metadata client watch meet error"),
                    _ => panic!("BUG: invalid event {:?}", event),
                }
            }
        }
    }

    fn backup_batch(&self, batch: CmdBatch) {
        let mut sw = StopWatch::new();
        let region_id = batch.region_id;
        let mut resolver = match self.subs.get_subscription_of(region_id) {
            Some(rts) => rts,
            None => {
                warn!("BUG: the region isn't registered (no resolver found) but sent to backup_batch."; "region_id" => %region_id);
                return;
            }
        };
        // Stale data is accpetable, while stale locks may block the checkpoint advancing.
        // Let L be the instant some key locked, U be the instant it unlocked,
        // +---------*-------L-----------U--*-------------+
        //           ^   ^----(1)----^      ^ We get the snapshot for initial scanning at here.
        //           +- If we issue refresh resolver at here, and the cmd batch (1) is the last cmd batch of the first observing.
        //              ...the background initial scanning may keep running, and the lock would be sent to the scanning.
        //              ...note that (1) is the last cmd batch of first observing, so the unlock event would never be sent to us.
        //              ...then the lock would get an eternal life in the resolver :|
        //                 (Before we refreshing the resolver for this region again)
        if batch.pitr_id != resolver.value().handle.id {
            debug!("stale command"; "region_id" => %region_id, "now" => ?resolver.value().handle.id, "remote" => ?batch.pitr_id);
            return;
        }
        let sched = self.scheduler.clone();

        let kvs = ApplyEvents::from_cmd_batch(batch, resolver.value_mut().resolver());
        drop(resolver);
        if kvs.is_empty() {
            return;
        }

        HANDLE_EVENT_DURATION_HISTOGRAM
            .with_label_values(&["to_stream_event"])
            .observe(sw.lap().as_secs_f64());
        let router = self.range_router.clone();
        self.pool.spawn(async move {
            HANDLE_EVENT_DURATION_HISTOGRAM
                .with_label_values(&["get_router_lock"])
                .observe(sw.lap().as_secs_f64());
            let kv_count = kvs.len();
            let total_size = kvs.size();
            metrics::HEAP_MEMORY
                .add(total_size as _);
            utils::handle_on_event_result(&sched, router.on_events(kvs).await);
            metrics::HEAP_MEMORY
                .sub(total_size as _);
            let time_cost = sw.lap().as_secs_f64();
            if time_cost > SLOW_EVENT_THRESHOLD {
                warn!("write to temp file too slow."; "time_cost" => ?time_cost, "region_id" => %region_id, "len" => %kv_count);
            }
            HANDLE_EVENT_DURATION_HISTOGRAM
                .with_label_values(&["save_to_temp_file"])
                .observe(time_cost)
        });
    }

    /// Make an initial data loader using the resource of the endpoint.
    pub fn make_initial_loader(&self) -> InitialDataLoader<E, R, RT> {
        InitialDataLoader::new(
            self.router.clone(),
            self.regions.clone(),
            self.range_router.clone(),
            self.subs.clone(),
            self.scheduler.clone(),
            self.initial_scan_memory_quota.clone(),
            self.pool.handle().clone(),
        )
    }

    pub fn handle_watch_task(&self, op: TaskOp) {
        match op {
            TaskOp::AddTask(task) => {
                self.on_register(task);
            }
            TaskOp::RemoveTask(task_name) => {
                self.on_unregister(&task_name);
            }
            TaskOp::PauseTask(task_name) => {
                self.on_pause(&task_name);
            }
            TaskOp::ResumeTask(task) => {
                self.load_task(task);
            }
        }
    }

    async fn observe_and_scan_region(
        &self,
        init: InitialDataLoader<E, R, RT>,
        task: &StreamTask,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Result<()> {
        let start = Instant::now_coarse();
        let success = self
            .observer
            .ranges
            .wl()
            .add((start_key.clone(), end_key.clone()));
        if !success {
            warn!("backup stream task ranges overlapped, which hasn't been supported for now";
                "task" => ?task,
                "start_key" => utils::redact(&start_key),
                "end_key" => utils::redact(&end_key),
            );
        }
        // Assuming the `region info provider` would read region info form `StoreMeta` directly and this would be fast.
        // If this gets slow, maybe make it async again. (Will that bring race conditions? say `Start` handled after `ResfreshResolver` of some region.)
        let range_init_result = init.initialize_range(start_key.clone(), end_key.clone());
        match range_init_result {
            Ok(()) => {
                info!("backup stream success to initialize"; 
                        "start_key" => utils::redact(&start_key),
                        "end_key" => utils::redact(&end_key),
                        "take" => ?start.saturating_elapsed(),)
            }
            Err(e) => {
                e.report("backup stream initialize failed");
            }
        }
        Ok(())
    }

    // register task ranges
    pub fn on_register(&self, task: StreamTask) {
        let name = task.info.name.clone();
        let start_ts = task.info.start_ts;
        self.load_task(task);

        metrics::STORE_CHECKPOINT_TS
            .with_label_values(&[name.as_str()])
            .set(start_ts as _);
    }

    /// Load the task into memory: this would make the endpint start to observe.
    fn load_task(&self, task: StreamTask) {
        if let Some(cli) = self.meta_client.as_ref() {
            let cli = cli.clone();
            let init = self.make_initial_loader();
            let range_router = self.range_router.clone();

            info!(
                "register backup stream task";
                "task" => ?task,
            );
            let task_name = task.info.get_name().to_owned();
            // clean the safepoint created at pause(if there is)
            self.pool.spawn(
                self.pd_client
                    .update_service_safe_point(
                        format!("{}-pause-guard", task.info.name),
                        TimeStamp::zero(),
                        Duration::new(0, 0),
                    )
                    .map(|r| {
                        r.map_err(|err| Error::from(err).report("removing safe point for pausing"))
                    }),
            );
            self.pool.block_on(async move {
                let task_name = task.info.get_name();
                match cli.ranges_of_task(task_name).await {
                    Ok(ranges) => {
                        info!(
                            "register backup stream ranges";
                            "task" => ?task,
                            "ranges-count" => ranges.inner.len(),
                        );
                        let ranges = ranges
                            .inner
                            .into_iter()
                            .map(|(start_key, end_key)| {
                                (utils::wrap_key(start_key), utils::wrap_key(end_key))
                            })
                            .collect::<Vec<_>>();
                        if let Err(err) = range_router
                            .register_task(task.clone(), ranges.clone())
                            .await
                        {
                            err.report(format!(
                                "failed to register backup stream task {}",
                                task.info.name
                            ));
                            return;
                        }

                        for (start_key, end_key) in ranges {
                            let init = init.clone();

                            self.observe_and_scan_region(init, &task, start_key, end_key)
                                .await
                                .unwrap();
                        }
                        info!(
                            "finish register backup stream ranges";
                            "task" => ?task,
                        );
                    }
                    Err(e) => {
                        e.report(format!(
                            "failed to register backup stream task {} to router: ranges not found",
                            task.info.get_name()
                        ));
                    }
                }
            });
            metrics::update_task_status(TaskStatus::Running, &task_name);
        };
    }

    fn register_service_safepoint(
        &self,
        task_name: String,
        // hint for make optimized safepoint when we cannot get that from `SubscriptionTracker`
        start_ts: TimeStamp,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        self.pd_client
            .update_service_safe_point(
                format!("{}-pause-guard", task_name),
                self.subs.safepoint().max(start_ts),
                ReadableDuration::hours(24).0,
            )
            .map(|r| r.map_err(|err| err.into()))
    }

    pub fn on_pause(&self, task: &str) {
        let t = self.unload_task(task);

        if let Some(task) = t {
            self.pool.spawn(
                self.register_service_safepoint(task.name, TimeStamp::new(task.start_ts))
                    .map(|r| {
                        r.map_err(|err| err.report("during setting service safepoint for task"))
                    }),
            );
        }

        metrics::update_task_status(TaskStatus::Paused, task);
    }

    pub fn on_unregister(&self, task: &str) -> Option<StreamBackupTaskInfo> {
        let info = self.unload_task(task);

        // reset the checkpoint ts of the task so it won't mislead the metrics.
        metrics::STORE_CHECKPOINT_TS
            .with_label_values(&[task])
            .set(0);
        info
    }

    /// unload a task from memory: this would stop observe the changes required by the task temporarily.
    fn unload_task(&self, task: &str) -> Option<StreamBackupTaskInfo> {
        let router = self.range_router.clone();

        // for now, we support one concurrent task only.
        // so simply clear all info would be fine.
        self.observer.ranges.wl().clear();
        self.subs.clear();
        self.pool.block_on(router.unregister_task(task))
    }

    /// try advance the resolved ts by the pd tso.
    async fn try_resolve(
        cm: &ConcurrencyManager,
        pd_client: Arc<PDC>,
        resolvers: SubscriptionTracer,
    ) -> TimeStamp {
        let pd_tso = pd_client
            .get_tso()
            .await
            .map_err(|err| Error::from(err).report("failed to get tso from pd"))
            .unwrap_or_default();
        let min_ts = cm.global_min_lock_ts().unwrap_or(TimeStamp::max());
        let tso = Ord::min(pd_tso, min_ts);
        let ts = resolvers.resolve_with(tso);
        resolvers.warn_if_gap_too_huge(ts);
        ts
    }

    async fn flush_for_task(
        task: String,
        store_id: u64,
        router: Router,
        pd_cli: Arc<PDC>,
        resolvers: SubscriptionTracer,
        meta_cli: MetadataClient<S>,
        concurrency_manager: ConcurrencyManager,
    ) {
        let start = Instant::now_coarse();
        // NOTE: Maybe push down the resolve step to the router?
        //       Or if there are too many duplicated `Flush` command, we may do some useless works.
        let new_rts = Self::try_resolve(&concurrency_manager, pd_cli.clone(), resolvers).await;
        metrics::FLUSH_DURATION
            .with_label_values(&["resolve_by_now"])
            .observe(start.saturating_elapsed_secs());
        if let Some(rts) = router.do_flush(&task, store_id, new_rts).await {
            info!("flushing and refreshing checkpoint ts.";
                "checkpoint_ts" => %rts,
                "task" => %task,
            );
            if rts == 0 {
                // We cannot advance the resolved ts for now.
                return;
            }
            concurrency_manager.update_max_ts(TimeStamp::new(rts));
            if let Err(err) = pd_cli
                .update_service_safe_point(
                    format!("backup-stream-{}-{}", task, store_id),
                    TimeStamp::new(rts),
                    // Add a service safe point for 10mins (2x the default flush interval).
                    // It would probably be safe.
                    Duration::from_secs(600),
                )
                .await
            {
                Error::from(err).report("failed to update service safe point!");
                // don't give up?
            }
            if let Err(err) = meta_cli.set_local_task_checkpoint(&task, rts).await {
                err.report(format!("on flushing task {}", task));
                // we can advance the progress at next time.
                // return early so we won't be mislead by the metrics.
                return;
            }
            metrics::STORE_CHECKPOINT_TS
                // Currently, we only support one task at the same time,
                // so use the task as label would be ok.
                .with_label_values(&[task.as_str()])
                .set(rts as _)
        }
    }

    pub fn on_force_flush(&self, task: String, store_id: u64) {
        let router = self.range_router.clone();
        let cli = self
            .meta_client
            .as_ref()
            .expect("on_flush: executed from an endpoint without cli")
            .clone();
        let pd_cli = self.pd_client.clone();
        let resolvers = self.subs.clone();
        let cm = self.concurrency_manager.clone();
        self.pool.spawn(async move {
            let info = router.get_task_info(&task).await;
            // This should only happen in testing, it would be to unwrap...
            let _ = info.unwrap().set_flushing_status_cas(false, true);
            Self::flush_for_task(task, store_id, router, pd_cli, resolvers, cli, cm).await;
        });
    }

    pub fn on_flush(&self, task: String, store_id: u64) {
        let router = self.range_router.clone();
        let cli = self
            .meta_client
            .as_ref()
            .expect("on_flush: executed from an endpoint without cli")
            .clone();
        let pd_cli = self.pd_client.clone();
        let resolvers = self.subs.clone();
        let cm = self.concurrency_manager.clone();
        self.pool.spawn(Self::flush_for_task(
            task, store_id, router, pd_cli, resolvers, cli, cm,
        ));
    }

    /// Modify observe over some region.
    /// This would register the region to the RaftStore.
    pub fn on_modify_observe(&self, op: ObserveOp) {
        info!("backup stream: on_modify_observe"; "op" => ?op);
        self.pool.block_on(self.region_operator.request(op));
    }

    pub fn run_task(&self, task: Task) {
        debug!("run backup stream task"; "task" => ?task);
        let now = Instant::now_coarse();
        let label = task.label();
        defer! {
            metrics::INTERNAL_ACTOR_MESSAGE_HANDLE_DURATION.with_label_values(&[label])
                .observe(now.saturating_elapsed_secs())
        }
        match task {
            Task::WatchTask(op) => self.handle_watch_task(op),
            Task::BatchEvent(events) => self.do_backup(events),
            Task::Flush(task) => self.on_flush(task, self.store_id),
            Task::ModifyObserve(op) => self.on_modify_observe(op),
            Task::ForceFlush(task) => self.on_force_flush(task, self.store_id),
            Task::FatalError(task, err) => self.on_fatal_error(task, err),
            Task::ChangeConfig(_) => {
                warn!("change config online isn't supported for now.")
            }
            Task::Sync(cb, mut cond) => {
                if cond(&self.range_router) {
                    cb()
                } else {
                    let sched = self.scheduler.clone();
                    self.pool.spawn(async move {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        sched.schedule(Task::Sync(cb, cond)).unwrap();
                    });
                }
            }
        }
    }

    pub fn do_backup(&self, events: Vec<CmdBatch>) {
        for batch in events {
            self.backup_batch(batch)
        }
    }
}

/// Create a standard tokio runtime
/// (which allows io and time reactor, involve thread memory accessor),
fn create_tokio_runtime(thread_count: usize, thread_name: &str) -> TokioResult<Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name(thread_name)
        // Maybe make it more configurable?
        // currently, blocking threads would be used for tokio local I/O.
        // (`File` API in `tokio::io` would use this pool.)
        .max_blocking_threads(thread_count * 8)
        .worker_threads(thread_count)
        .enable_io()
        .enable_time()
        .on_thread_start(|| {
            tikv_alloc::add_thread_memory_accessor();
        })
        .on_thread_stop(|| {
            tikv_alloc::remove_thread_memory_accessor();
        })
        .build()
}

pub enum Task {
    WatchTask(TaskOp),
    BatchEvent(Vec<CmdBatch>),
    ChangeConfig(ConfigChange),
    /// Flush the task with name.
    Flush(String),
    /// Change the observe status of some region.
    ModifyObserve(ObserveOp),
    /// Convert status of some task into `flushing` and do flush then.
    ForceFlush(String),
    /// FatalError pauses the task and set the error.
    FatalError(String, Box<Error>),
    /// Run the callback when see this message. Only for test usage.
    /// NOTE: Those messages for testing are not guared by `#[cfg(test)]` for now, because
    ///       the integration test would not enable test config when compiling (why?)
    Sync(
        // Run the closure if ...
        Box<dyn FnOnce() + Send>,
        // This returns `true`.
        Box<dyn FnMut(&Router) -> bool + Send>,
    ),
}

#[derive(Debug)]
pub enum TaskOp {
    AddTask(StreamTask),
    RemoveTask(String),
    PauseTask(String),
    ResumeTask(StreamTask),
}

#[derive(Debug)]
pub enum ObserveOp {
    Start {
        region: Region,
    },
    Stop {
        region: Region,
    },
    CheckEpochAndStop {
        region: Region,
    },
    RefreshResolver {
        region: Region,
    },
    NotifyFailToStartObserve {
        region: Region,
        handle: ObserveHandle,
        err: Box<Error>,
    },
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WatchTask(arg0) => f.debug_tuple("WatchTask").field(arg0).finish(),
            Self::BatchEvent(arg0) => f
                .debug_tuple("BatchEvent")
                .field(&format!("[{} events...]", arg0.len()))
                .finish(),
            Self::ChangeConfig(arg0) => f.debug_tuple("ChangeConfig").field(arg0).finish(),
            Self::Flush(arg0) => f.debug_tuple("Flush").field(arg0).finish(),
            Self::ModifyObserve(op) => f.debug_tuple("ModifyObserve").field(op).finish(),
            Self::ForceFlush(arg0) => f.debug_tuple("ForceFlush").field(arg0).finish(),
            Self::FatalError(task, err) => {
                f.debug_tuple("FatalError").field(task).field(err).finish()
            }
            Self::Sync(..) => f.debug_tuple("Sync").finish(),
        }
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Task {
    fn label(&self) -> &'static str {
        match self {
            Task::WatchTask(w) => match w {
                TaskOp::AddTask(_) => "watch_task.add",
                TaskOp::RemoveTask(_) => "watch_task.remove",
                TaskOp::PauseTask(_) => "watch_task.pause",
                TaskOp::ResumeTask(_) => "watch_task.resume",
            },
            Task::BatchEvent(_) => "batch_event",
            Task::ChangeConfig(_) => "change_config",
            Task::Flush(_) => "flush",
            Task::ModifyObserve(o) => match o {
                ObserveOp::Start { .. } => "modify_observe.start",
                ObserveOp::Stop { .. } => "modify_observe.stop",
                ObserveOp::CheckEpochAndStop { .. } => "modify_observe.check_epoch_and_stop",
                ObserveOp::RefreshResolver { .. } => "modify_observe.refresh_resolver",
                ObserveOp::NotifyFailToStartObserve { .. } => "modify_observe.retry",
            },
            Task::ForceFlush(_) => "force_flush",
            Task::FatalError(..) => "fatal_error",
            Task::Sync(..) => "sync",
        }
    }
}

impl<S, R, E, RT, PDC> Runnable for Endpoint<S, R, E, RT, PDC>
where
    S: MetaStore + 'static,
    R: RegionInfoProvider + Clone + 'static,
    E: KvEngine,
    RT: RaftStoreRouter<E> + 'static,
    PDC: PdClient + 'static,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        self.run_task(task)
    }
}
