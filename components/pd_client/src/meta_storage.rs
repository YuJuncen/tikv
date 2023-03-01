// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! `meta_storage` is the API set for storing generic KV pairs.
//! It is a trimmed version of the KV service of etcd, along with some metrics.

use futures::Stream;
use kvproto::meta_storagepb as pb;
use tikv_util::codec;

use crate::PdFuture;

#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct Get {
    inner: pb::GetRequest,
}

impl From<Get> for pb::GetRequest {
    fn from(value: Get) -> Self {
        value.inner
    }
}

impl Get {
    pub fn of(key: impl Into<Vec<u8>>) -> Self {
        let mut inner = pb::GetRequest::default();
        inner.set_key(key.into());
        Self { inner }
    }

    pub fn prefixed(mut self) -> Self {
        let next = codec::next_prefix_of(self.inner.key.clone());
        self.inner.set_range_end(next);
        self
    }

    pub fn range_to(mut self, to: impl Into<Vec<u8>>) -> Self {
        self.inner.set_range_end(to.into());
        self
    }

    pub fn rev(mut self, rev: i64) -> Self {
        self.inner.set_revision(rev);
        self
    }

    pub fn limit(mut self, limit: i64) -> Self {
        self.inner.set_limit(limit);
        self
    }
}

#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct Put {
    inner: pb::PutRequest,
}

impl Put {
    pub fn of(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        let mut inner = pb::PutRequest::default();
        inner.set_key(key.into());
        inner.set_value(value.into());
        Self { inner }
    }

    pub fn fetch_prev_kv(mut self) -> Self {
        self.inner.prev_kv = true;
        self
    }
}

impl From<Put> for pb::PutRequest {
    fn from(value: Put) -> Self {
        value.inner
    }
}

#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct Watch {
    inner: pb::WatchRequest,
}

impl Watch {
    pub fn of(key: impl Into<Vec<u8>>) -> Self {
        let mut inner = pb::WatchRequest::default();
        inner.set_key(key.into());

        Self { inner }
    }

    pub fn prefixed(mut self) -> Self {
        let next = codec::next_prefix_of(self.inner.key.clone());
        self.inner.set_range_end(next);
        self
    }

    pub fn range_to(mut self, to: impl Into<Vec<u8>>) -> Self {
        self.inner.set_range_end(to.into());
        self
    }

    pub fn from_rev(mut self, rev: i64) -> Self {
        self.inner.set_start_revision(rev);
        self
    }
}

impl From<Watch> for pb::WatchRequest {
    fn from(value: Watch) -> Self {
        value.inner
    }
}

pub trait MetaStorageClient: Send + Sync + 'static {
    // Note: some of our clients needs to maintain some state, which means we may
    // need to move part of the structure.
    // Though we can write some unsafe code and prove the move won't make wrong
    // things, for keeping things simple, we added the `Unpin` constraint here.
    // Given before the stream generator get stable, there shouldn't be too many
    // stream implementation that must be pinned...
    // Also note this `Unpin` cannot be added in the client side because higher kind
    // types aren't stable...
    type WatchStream<T>: Stream<Item = grpcio::Result<T>> + Unpin + Send;

    fn get(&self, req: Get) -> PdFuture<pb::GetResponse>;
    fn put(&self, req: Put) -> PdFuture<pb::PutResponse>;
    fn watch(&self, req: Watch) -> PdFuture<Self::WatchStream<pb::WatchResponse>>;
}
