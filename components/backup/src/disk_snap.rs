//! This module contains things about disk snapshot.

use std::{
    sync::{atomic::AtomicU64, Arc},
    task::Poll,
    time::Duration,
};

use futures::future;
use futures_util::{
    future::{BoxFuture, FutureExt},
    sink::SinkExt,
    stream::StreamExt,
};
use grpcio::{RpcStatus, WriteFlags};
use kvproto::{
    brpb::{
        PrepareSnapshotBackupEventType as PEvnT, PrepareSnapshotBackupRequest as PReq,
        PrepareSnapshotBackupRequestType as PReqT, PrepareSnapshotBackupResponse as PResp,
    },
    errorpb,
    metapb::Region,
};
use raftstore::store::{
    snapshot_backup::{
        RejectIngestAndAdmin, SnapshotBrHandle, SnapshotBrWaitApplyRequest, UnimplementedHandle,
    },
    SnapshotBrWaitApplySyncer,
};
use tikv_util::warn;
use tokio::sync::oneshot;
use tokio_stream::Stream;

type Result<T> = std::result::Result<T, Error>;

enum Error {
    NotInitialized,
    LeaseExpired,
    WaitApplyAborted,
    RaftStore(raftstore::Error),
}

enum HandleErr {
    AbortStream(RpcStatus),
    SendErrResp(errorpb::Error),
}

pub struct ResultSink(grpcio::DuplexSink<PResp>);

impl From<grpcio::DuplexSink<PResp>> for ResultSink {
    fn from(value: grpcio::DuplexSink<PResp>) -> Self {
        Self(value)
    }
}

impl ResultSink {
    async fn send(
        mut self,
        result: Result<PResp>,
        error_extra_info: impl FnOnce(&mut PResp),
    ) -> grpcio::Result<Self> {
        match result {
            // Note: should we batch here?
            Ok(item) => self.0.send((item, WriteFlags::default())).await?,
            Err(err) => match err.how_to_handle() {
                HandleErr::AbortStream(status) => {
                    self.0.fail(status.clone()).await?;
                    return Err(grpcio::Error::RpcFinished(Some(status)));
                }
                HandleErr::SendErrResp(err) => {
                    let mut resp = PResp::new();
                    error_extra_info(&mut resp);
                    resp.set_error(err);
                    self.0.send((resp, WriteFlags::default())).await?;
                }
            },
        }
        Ok(self)
    }
}

impl Error {
    fn how_to_handle(self) -> HandleErr {
        match self {
            Error::NotInitialized => HandleErr::AbortStream(RpcStatus::with_message(
                grpcio::RpcStatusCode::UNAVAILABLE,
                "coprocessor not initialized".to_owned(),
            )),
            Error::RaftStore(r) => HandleErr::SendErrResp(errorpb::Error::from(r)),
            Error::WaitApplyAborted => {
                HandleErr::SendErrResp({
                    let mut err = errorpb::Error::new();
                    err.set_message("wait apply has been aborted, perhaps epoch not match or leadership changed".to_owned());
                    err
                })
            }
            Error::LeaseExpired => HandleErr::AbortStream(RpcStatus::with_message(
                grpcio::RpcStatusCode::FAILED_PRECONDITION,
                "the lease has expired, you may not send `wait_apply` because it is no meaning"
                    .to_string(),
            )),
        }
    }
}

pub struct Env<SR: SnapshotBrHandle> {
    pub(crate) handle: Arc<SR>,
    rejector: Arc<RejectIngestAndAdmin>,
    active_stream: Arc<AtomicU64>,
}

impl<SR: SnapshotBrHandle> Clone for Env<SR> {
    fn clone(&self) -> Self {
        Self {
            handle: Arc::clone(&self.handle),
            rejector: Arc::clone(&self.rejector),
            active_stream: Arc::clone(&self.active_stream),
        }
    }
}

impl Env<UnimplementedHandle> {
    pub fn unimplemented_for_v2() -> Self {
        Self {
            handle: Arc::new(UnimplementedHandle {
                reason: "RaftStoreV2 doesn't support snapshot backup.".to_owned(),
            }),
            rejector: Default::default(),
            active_stream: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl<SR: SnapshotBrHandle> Env<SR> {
    pub fn with_rejector(handle: SR, rejector: Arc<RejectIngestAndAdmin>) -> Self {
        Self {
            handle: Arc::new(handle),
            rejector,
            active_stream: Arc::new(AtomicU64::new(0)),
        }
    }

    fn check_initialized(&self) -> Result<()> {
        if !self.rejector.connected() {
            return Err(Error::NotInitialized);
        }
        Ok(())
    }

    fn check_rejected(&self) -> Result<()> {
        self.check_initialized()?;
        if self.rejector.allowed() {
            return Err(Error::LeaseExpired);
        }
        Ok(())
    }

    fn update_lease(&self, lease_dur: Duration) -> Result<PResp> {
        self.check_initialized()?;
        let mut event = PResp::new();
        event.set_ty(PEvnT::UpdateLeaseResult);
        event.set_last_lease_is_valid(self.rejector.heartbeat(lease_dur));
        Ok(event)
    }

    fn reset(&self) -> PResp {
        let rejected = !self.rejector.allowed();
        let mut event = PResp::new();
        event.set_ty(PEvnT::UpdateLeaseResult);
        event.set_last_lease_is_valid(rejected);
        event
    }
}

pub struct StreamHandleLoop<SR: SnapshotBrHandle + 'static> {
    pending_regions: Vec<BoxFuture<'static, (Region, Result<()>)>>,
    env: Env<SR>,
}

enum StreamHandleEvent {
    Req(PReq),
    WaitApplyDone(Region, Result<()>),
    ConnectionGone(Option<grpcio::Error>),
}

impl<SR: SnapshotBrHandle + 'static> StreamHandleLoop<SR> {
    pub fn new(env: Env<SR>) -> Self {
        Self {
            env,
            pending_regions: vec![],
        }
    }

    fn async_wait_apply(&mut self, region: &Region) -> BoxFuture<'static, (Region, Result<()>)> {
        if let Err(err) = self.env.check_rejected() {
            return Box::pin(future::ready((region.clone(), Err(err))));
        }

        let (tx, rx) = oneshot::channel();
        let syncer = SnapshotBrWaitApplySyncer::new(region.id, tx);
        let handle = Arc::clone(&self.env.handle);
        let region = region.clone();
        let epoch = region.get_region_epoch().clone();
        let id = region.get_id();
        let send_res = handle
            .send_wait_apply(id, SnapshotBrWaitApplyRequest::strict(syncer, epoch))
            .map_err(Error::RaftStore);
        Box::pin(
            async move {
                send_res?;
                rx.await.map_err(|_| Error::WaitApplyAborted).map(|_| ())
            }
            .map(move |res| (region, res)),
        )
    }

    async fn next_event(
        &mut self,
        input: &mut (impl Stream<Item = grpcio::Result<PReq>> + Unpin),
    ) -> StreamHandleEvent {
        let wait_applies = future::poll_fn(|cx| {
            let selected =
                self.pending_regions
                    .iter_mut()
                    .enumerate()
                    .find_map(|(i, fut)| match fut.poll_unpin(cx) {
                        Poll::Ready(r) => Some((i, r)),
                        Poll::Pending => None,
                    });
            match selected {
                Some((i, region)) => {
                    self.pending_regions.swap_remove(i);
                    region.into()
                }
                None => Poll::Pending,
            }
        });

        tokio::select! {
            wres = wait_applies => {
                StreamHandleEvent::WaitApplyDone(wres.0, wres.1)
            }
            req = input.next() => {
                match req {
                    Some(Ok(req)) => StreamHandleEvent::Req(req),
                    Some(Err(err)) => StreamHandleEvent::ConnectionGone(Some(err)),
                    None => StreamHandleEvent::ConnectionGone(None)
                }
            }
        }
    }

    pub async fn run(
        mut self,
        mut input: impl Stream<Item = grpcio::Result<PReq>> + Unpin,
        mut sink: ResultSink,
    ) -> grpcio::Result<()> {
        loop {
            match self.next_event(&mut input).await {
                StreamHandleEvent::Req(req) => match req.get_ty() {
                    PReqT::UpdateLease => {
                        let lease_dur = Duration::from_secs(req.get_lease_in_seconds());
                        sink = sink
                            .send(self.env.update_lease(lease_dur), |resp| {
                                resp.set_ty(PEvnT::UpdateLeaseResult);
                            })
                            .await?;
                    }
                    PReqT::WaitApply => {
                        let regions = req.get_regions();
                        for region in regions {
                            let res = self.async_wait_apply(region);
                            self.pending_regions.push(res);
                        }
                    }
                    PReqT::Finish => {
                        sink.send(Ok(self.env.reset()), |_| {})
                            .await?
                            .0
                            .close()
                            .await?;
                        return Ok(());
                    }
                },
                StreamHandleEvent::WaitApplyDone(region, res) => {
                    let resp = res.map(|_| {
                        let mut resp = PResp::new();
                        resp.set_region(region.clone());
                        resp.set_ty(PEvnT::WaitApplyDone);
                        resp
                    });
                    sink = sink
                        .send(resp, |resp| {
                            resp.set_ty(PEvnT::WaitApplyDone);
                            resp.set_region(region);
                        })
                        .await?;
                }
                StreamHandleEvent::ConnectionGone(err) => {
                    warn!("the client has gone, aborting loop"; "err" => ?err);
                    return match err {
                        None => Ok(()),
                        Some(err) => Err(err),
                    };
                }
            }
        }
    }
}
