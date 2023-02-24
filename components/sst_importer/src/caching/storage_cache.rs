// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io,
    pin::Pin,
    sync::{Arc, RwLock},
    task::ready,
};

use async_trait::async_trait;
use external_storage_export::{ExternalData, ExternalStorage, RestoreConfig};
use futures::{AsyncRead, Future, StreamExt};
use kvproto::brpb::StorageBackend;
use rand::Rng;
use tikv_util::{time::Limiter, HandyRwLock};

use super::cache_map::{MakeCache, ShareOwned};

impl ShareOwned for SharedStoragePool {
    type Shared = SharedStoragePool;

    fn share_owned(&self) -> Self::Shared {
        self.clone()
    }
}

impl MakeCache for StorageBackend {
    type Cached = SharedStoragePool;
    type Error = io::Error;

    fn make_cache(&self) -> io::Result<Self::Cached> {
        Ok(SharedStoragePool(Arc::new(StoragePool::create(self, 16)?)))
    }
}

pub struct StoragePool(
    RwLock<Box<[Arc<dyn ExternalStorage>]>>,
    StorageBackend,
    usize,
);

struct ErrorReportStream<S> {
    c: ExternalData<'static>,
    holder: S,

    report_idx: usize,
    report_to: Arc<StoragePool>,
}

impl<S: ExternalStorage> AsyncRead for ErrorReportStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<io::Result<usize>> {
        // Safety: trivial projection.
        let (inner, report_idx, report_to) = unsafe {
            let this = self.get_unchecked_mut();
            (
                Pin::new_unchecked(&mut this.c),
                this.report_idx,
                &this.report_to,
            )
        };

        let next = ready!(inner.poll_read(cx, buf));
        match next {
            Ok(item) => Ok(item).into(),
            Err(err) => {
                warn!("read: reloading the external storage because meet error."; "err" => %err);
                report_to.reload(report_idx)?;
                Err(err).into()
            }
        }
    }
}

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct SharedStoragePool(Arc<StoragePool>);

impl std::ops::Deref for SharedStoragePool {
    type Target = StoragePool;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

#[async_trait]
impl ExternalStorage for SharedStoragePool {
    fn name(&self) -> &'static str {
        self.with(|x| Ok(x.name()))
            .expect("unreachable: failed for a infailible function.")
    }

    fn url(&self) -> io::Result<url::Url> {
        self.with(|x| x.url())
    }

    async fn write(
        &self,
        name: &str,
        reader: external_storage_export::UnpinReader,
        content_length: u64,
    ) -> io::Result<()> {
        self.with_async(move |x| async move { x.write(name, reader, content_length).await })
            .await?;
        Ok(())
    }

    fn read(&self, name: &str) -> external_storage_export::ExternalData<'_> {
        let idx = rand::thread_rng().gen_range(0..self.2);
        let r = Arc::clone(&self.0.0.rl()[idx]);
        let readed = r.read(name);

        Box::new(ErrorReportStream {
            holder: Arc::clone(&r),
            c: unsafe { std::mem::transmute(readed) },

            report_idx: idx,
            report_to: Arc::clone(&self.0),
        })
    }

    fn read_part(
        &self,
        name: &str,
        off: u64,
        len: u64,
    ) -> external_storage_export::ExternalData<'_> {
        let idx = rand::thread_rng().gen_range(0..self.2);
        let r = Arc::clone(&self.0.0.rl()[idx]);
        let readed = r.read_part(name, off, len);
        Box::new(ErrorReportStream {
            holder: Arc::clone(&r),
            // Safety: this transmute make us borrow self for 'static (i.e. the full lifetime
            // of the ErrorReportStream), which is safe because `self` is a Arc pointer held by the
            // structure -- its life time would long enough, it is pretty like self-referental
            // someway.
            c: unsafe { std::mem::transmute(readed) },

            report_idx: idx,
            report_to: Arc::clone(&self.0),
        })
    }

    async fn restore(
        &self,
        storage_name: &str,
        restore_name: std::path::PathBuf,
        expected_length: u64,
        speed_limiter: &Limiter,
        restore_config: RestoreConfig,
    ) -> io::Result<()> {
        self.with_async(move |x| async move {
            x.restore(
                storage_name,
                restore_name,
                expected_length,
                speed_limiter,
                restore_config,
            )
            .await
        })
        .await
    }
}

impl StoragePool {
    fn create(backend: &StorageBackend, size: usize) -> io::Result<Self> {
        let mut r = Vec::with_capacity(size);
        for _ in 0..size {
            let s = external_storage_export::create_storage(backend, Default::default())?;
            r.push(Arc::from(s));
        }
        Ok(Self(
            RwLock::new(r.into_boxed_slice()),
            backend.clone(),
            size,
        ))
    }

    fn get(&self) -> Arc<dyn ExternalStorage> {
        let idx = rand::thread_rng().gen_range(0..self.2);
        Arc::clone(&self.0.rl()[idx])
    }

    fn with<T: 'static>(
        &self,
        f: impl FnOnce(&dyn ExternalStorage) -> io::Result<T>,
    ) -> io::Result<T> {
        let idx = rand::thread_rng().gen_range(0..self.2);
        let r = f(self.0.rl()[idx].as_ref());
        if r.is_err() {
            self.reload(idx);
        }
        r
    }

    async fn with_async<T, F>(&self, f: impl FnOnce(Arc<dyn ExternalStorage>) -> F) -> io::Result<T>
    where
        T: 'static,
        F: Future<Output = io::Result<T>>,
    {
        let idx = rand::thread_rng().gen_range(0..self.2);
        let ext_storage = Arc::clone(&self.0.rl()[idx]);

        let r = f(ext_storage).await;
        if r.is_err() {
            self.reload(idx)?;
        }
        r
    }

    fn reload(&self, idx: usize) -> io::Result<()> {
        self.0.wl()[idx] = Arc::from(external_storage_export::create_storage(
            &self.1,
            Default::default(),
        )?);
        Ok(())
    }
}

impl std::fmt::Debug for StoragePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let url = self
            .get()
            .url()
            .map(|u| u.to_string())
            .unwrap_or_else(|_| "<unknown>".to_owned());
        f.debug_tuple("StoragePool")
            .field(&format_args!("{}", url))
            .finish()
    }
}
