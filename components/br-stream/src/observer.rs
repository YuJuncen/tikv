use std::sync::{Arc, RwLock};

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use crate::errors::Error;
use crate::utils::SegmentTree;
use dashmap::DashMap;
use engine_traits::KvEngine;
use kvproto::metapb::Region;
use raft::StateRole;
use raftstore::coprocessor::*;
use tikv_util::worker::Scheduler;
use tikv_util::{debug, warn};
use tikv_util::{info, HandyRwLock};

use crate::endpoint::Task;

/// An Observer for Backup Stream.
///
/// It observes raftstore internal events, such as:
///   1. Apply command events.
#[derive(Clone)]
pub struct BackupStreamObserver {
    scheduler: Scheduler<Task>,
    // Note: maybe wrap those fields to methods?
    pub subs: SubscriptionTracer,
    pub ranges: Arc<RwLock<SegmentTree<Vec<u8>>>>,
}

impl BackupStreamObserver {
    /// Create a new `BackupStreamObserver`.
    ///
    /// Events are strong ordered, so `scheduler` must be implemented as
    /// a FIFO queue.
    pub fn new(scheduler: Scheduler<Task>) -> BackupStreamObserver {
        BackupStreamObserver {
            scheduler,
            subs: Default::default(),
            ranges: Default::default(),
        }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<impl KvEngine>) {
        let registry = &mut coprocessor_host.registry;

        // use 0 as the priority of the cmd observer. should have a higher priority than
        // the `resolved-ts`'s cmd observer
        registry.register_cmd_observer(0, BoxCmdObserver::new(self.clone()));
        registry.register_role_observer(100, BoxRoleObserver::new(self.clone()));
        registry.register_region_change_observer(100, BoxRegionChangeObserver::new(self.clone()));
    }

    /// The internal way to register a region.
    /// It delegate the initial scanning and modify of the subs to the endpoint.
    fn register_region(&self, region: &Region) {
        if let Err(err) = self.scheduler.schedule(Task::ObserverRegion {
            region: region.clone(),
        }) {
            Error::from(err).report(format!(
                "failed to schedule role change for region {}",
                region.get_id()
            ))
        }
    }

    /// Test whether a region should be observed by the observer.
    fn should_register_region(&self, region: &Region) -> bool {
        self.ranges
            .rl()
            .is_overlapping((region.get_start_key(), region.get_end_key()))
    }
}

impl Coprocessor for BackupStreamObserver {}

/// A utility to tracing the regions being subscripted.
#[derive(Clone, Default)]
pub struct SubscriptionTracer(Arc<DashMap<u64, ObserveHandle>>);

impl SubscriptionTracer {
    pub fn register_region(&self, region_id: u64, handle: ObserveHandle) {
        info!("start listen stream from store"; "observer" => ?handle, "region_id" => %region_id);
        if let Some(o) = self.0.insert(region_id, handle) {
            warn!("register region which is already registered"; "region_id" => %region_id);
            o.stop_observing();
        }
    }

    pub fn deregister_region(&self, region_id: u64) {
        match self.0.remove(&region_id) {
            Some(o) => {
                o.1.stop_observing();
                info!("stop listen stream from store"; "observer" => ?o.1, "region_id"=> %region_id);
            }
            None => {
                warn!("trying to deregister region not registered"; "region_id" => %region_id);
            }
        }
    }

    pub fn should_observe(&self, region_id: u64) -> bool {
        if let Some(o) = self.0.get(&region_id) {
            if o.is_observing() {
                return true;
            }
            // drop the reference to the handle or we may meet deadlock.
            // (It says "May deadlock if called when holding any sort of reference into the map.")
            drop(o);
            self.0.remove_if(&region_id, |_, o| !o.is_observing());
        }
        false
    }
}

impl<E: KvEngine> CmdObserver<E> for BackupStreamObserver {
    // `BackupStreamObserver::on_flush_applied_cmd_batch` should only invoke if `cmd_batches` is not empty
    // and only leader will trigger this.
    fn on_flush_applied_cmd_batch(
        &self,
        max_level: ObserveLevel,
        cmd_batches: &mut Vec<CmdBatch>,
        _engine: &E,
    ) {
        assert!(!cmd_batches.is_empty());
        debug!(
            "observe backup stream kv";
            "cmd_batches len" => cmd_batches.len(),
            "level" => ?max_level,
        );

        if max_level != ObserveLevel::All {
            return;
        }

        // TODO may be we should filter cmd batch here, to reduce the cost of clone.
        let cmd_batches: Vec<_> = cmd_batches
            .iter()
            .filter(|cb| {
                !cb.is_empty()
                    && cb.level == ObserveLevel::All
                    // Once the observe has been canceled by outside things, we should be able to stop.
                    && self.subs.should_observe(cb.region_id)
            })
            .cloned()
            .collect();
        if cmd_batches.is_empty() {
            return;
        }
        if let Err(e) = self.scheduler.schedule(Task::BatchEvent(cmd_batches)) {
            warn!("backup stream schedule task failed"; "error" => ?e);
        }
    }

    fn on_applied_current_term(&self, _: StateRole, _: &Region) {}
}

impl RoleObserver for BackupStreamObserver {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, r: StateRole) {
        let region = ctx.region();
        if r == StateRole::Leader && self.should_register_region(&region) {
            self.register_region(region);
        } else {
            self.subs.deregister_region(region.get_id());
        }
    }
}

impl RegionChangeObserver for BackupStreamObserver {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        role: StateRole,
    ) {
        match event {
            RegionChangeEvent::Create => {
                if role == StateRole::Leader && self.should_register_region(ctx.region()) {
                    self.register_region(ctx.region());
                }
            }
            RegionChangeEvent::Destroy => self.subs.deregister_region(ctx.region().get_id()),
            _ => {}
        }
    }
}

#[cfg(test)]

mod tests {
    use std::time::Duration;

    use engine_panic::PanicEngine;

    use kvproto::metapb::Region;
    use raft::StateRole;
    use raftstore::coprocessor::{
        Cmd, CmdBatch, CmdObserveInfo, CmdObserver, ObserveHandle, ObserveLevel, ObserverContext,
        RegionChangeEvent, RegionChangeObserver, RoleObserver,
    };

    use tikv_util::worker::dummy_scheduler;
    use tikv_util::HandyRwLock;

    use crate::endpoint::Task;

    use super::BackupStreamObserver;

    fn fake_region(id: u64, start: &[u8], end: &[u8]) -> Region {
        let mut r = Region::new();
        r.set_id(id);
        r.set_start_key(start.to_vec().into());
        r.set_end_key(end.to_vec().into());
        r
    }

    #[test]
    fn test_observer_cancel() {
        let (sched, mut rx) = dummy_scheduler();

        // Prepare: assuming a task wants the range of [0001, 0010].
        let o = BackupStreamObserver::new(sched);
        assert!(o.ranges.wl().add((b"0001".to_vec(), b"0010".to_vec())));

        // Test regions can be registered.
        let r = fake_region(42, b"0008", b"0009");
        o.register_region(&r);
        let task = rx.recv_timeout(Duration::from_secs(0)).unwrap().unwrap();
        let handle = ObserveHandle::new();
        if let Task::ObserverRegion { region } = task {
            o.subs.register_region(region.get_id(), handle.clone())
        } else {
            panic!("unexpected message received: it is {}", task);
        }
        assert!(o.subs.should_observe(42));
        handle.stop_observing();
        assert!(!o.subs.should_observe(42));
    }

    #[test]
    fn test_observer_basic() {
        let mock_engine = PanicEngine;
        let (sched, mut rx) = dummy_scheduler();

        // Prepare: assuming a task wants the range of [0001, 0010].
        let o = BackupStreamObserver::new(sched);
        assert!(o.ranges.wl().add((b"0001".to_vec(), b"0010".to_vec())));

        // Test regions can be registered.
        let r = fake_region(42, b"0008", b"0009");
        o.register_region(&r);
        let task = rx.recv_timeout(Duration::from_secs(0)).unwrap().unwrap();
        let handle = ObserveHandle::new();
        if let Task::ObserverRegion { region } = task {
            o.subs.register_region(region.get_id(), handle.clone())
        } else {
            panic!("unexpected message received: it is {}", task);
        }

        // Test events with key in the range can be observed.
        let observe_info = CmdObserveInfo::from_handle(handle.clone(), ObserveHandle::new());
        let mut cb = CmdBatch::new(&observe_info, 42);
        cb.push(&observe_info, 42, Cmd::default());
        let mut cmd_batches = vec![cb];
        o.on_flush_applied_cmd_batch(ObserveLevel::All, &mut cmd_batches, &mock_engine);
        let task = rx.recv_timeout(Duration::from_secs(0)).unwrap().unwrap();
        if let Task::BatchEvent(batches) = task {
            assert!(batches.len() == 1);
            assert!(batches[0].region_id == 42);
            assert!(batches[0].cdc_id == handle.id);
        } else {
            panic!("unexpected message received: it is {}", task);
        }

        // Test event from other region should not be send.
        let observe_info = CmdObserveInfo::from_handle(ObserveHandle::new(), ObserveHandle::new());
        let mut cb = CmdBatch::new(&observe_info, 43);
        cb.push(&observe_info, 43, Cmd::default());
        cb.level = ObserveLevel::None;
        let mut cmd_batches = vec![cb];
        o.on_flush_applied_cmd_batch(ObserveLevel::None, &mut cmd_batches, &mock_engine);
        let task = rx.recv_timeout(Duration::from_millis(20));
        assert!(task.is_err(), "it is {:?}", task);

        // Test region out of range won't be added to observe list.
        let r = fake_region(43, b"0010", b"0042");
        let mut ctx = ObserverContext::new(&r);
        o.on_role_change(&mut ctx, StateRole::Leader);
        let task = rx.recv_timeout(Duration::from_millis(20));
        assert!(task.is_err(), "it is {:?}", task);
        assert!(!o.subs.should_observe(43));

        // Test region out of range won't be added to observe list.
        let mut ctx = ObserverContext::new(&r);
        o.on_region_changed(&mut ctx, RegionChangeEvent::Create, StateRole::Leader);
        let task = rx.recv_timeout(Duration::from_millis(20));
        assert!(task.is_err(), "it is {:?}", task);
        assert!(!o.subs.should_observe(43));

        // Test give up subscripting when become follower.
        let r = fake_region(42, b"0008", b"0009");
        let mut ctx = ObserverContext::new(&r);
        o.on_role_change(&mut ctx, StateRole::Follower);
        let task = rx.recv_timeout(Duration::from_millis(20));
        assert!(task.is_err(), "it is {:?}", task);
        assert!(!o.subs.should_observe(42));
    }
}
