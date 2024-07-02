use std::{
    collections::HashMap,
    io::{Read, Seek},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_compression::futures::write::ZstdDecoder;
use external_storage::ExternalStorage;
use futures::io::{AllowStdIo, AsyncWriteExt, Cursor};
use prometheus::core::{Atomic, AtomicU64};
use tikv_util::{
    codec::stream_event::{self, Iterator},
    stream::{retry_all_ext, JustRetry, RetryExt},
};
use tokio::sync::OnceCell;
use txn_types::Key;

use super::{statistic::LoadStatistic, storage::LogFileId, util::Cooperate};
use crate::errors::Result;

#[derive(Clone)]
pub struct Source {
    inner: Arc<dyn ExternalStorage>,
    cache_manager: Option<Arc<CacheManager>>,
}

struct CacheManager {
    base: PathBuf,
    files: Mutex<HashMap<Arc<str>, Arc<OnceCell<PathBuf>>>>,
}

impl Source {
    pub fn new(inner: Arc<dyn ExternalStorage>) -> Self {
        Self {
            inner,
            cache_manager: None,
        }
    }

    pub fn with_cache(
        inner: Arc<dyn ExternalStorage>,
        cache_prefix: impl AsRef<Path>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            inner,
            cache_manager: Some(Arc::new(CacheManager::new(cache_prefix)?)),
        })
    }
}

impl CacheManager {
    fn new(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let base = path.as_ref().join(format!(
            "compact-log-cache-{}",
            tikv_util::sys::thread::process_id()
        ));
        std::fs::create_dir_all(&base)?;
        Ok(Self {
            base,
            files: Default::default(),
        })
    }

    async fn load_file(
        &self,
        id: LogFileId,
        storage: &Arc<dyn ExternalStorage>,
    ) -> std::io::Result<(Vec<u8>, u64, u64)> {
        // (error_during_downloading, physical_bytes_in)
        let stat = Arc::new((AtomicU64::new(0), AtomicU64::new(0)));
        let stat_ref = Arc::clone(&stat);
        let ext = RetryExt::default().with_fail_hook(move |err: &JustRetry<std::io::Error>| {
            eprintln!("retry the error: {:?}", err.0);
            stat_ref.0.inc_by(1)
        });
        let fetch = || {
            let storage = storage.clone();
            let id = id.clone();
            let stat = Arc::clone(&stat);
            async move {
                let path = self.base.join(id.name.as_ref());
                let local = std::fs::File::create(&path)?;
                let mut decompress = ZstdDecoder::new(AllowStdIo::new(local));
                let source = storage.read(&id.name);
                let n = futures::io::copy(source, &mut decompress).await?;
                stat.1.inc_by(n);
                decompress.flush().await?;
                std::result::Result::<_, std::io::Error>::Ok(path)
            }
        };

        let path_cell = {
            let mut files = self.files.lock().unwrap();
            if !files.contains_key(&id.name) {
                files.insert(Arc::clone(&id.name), Arc::default());
            }
            Arc::clone(&files[&id.name])
        };

        let path = path_cell
            .get_or_try_init(|| retry_all_ext(fetch, ext))
            .await?;
        let mut f = std::fs::File::options()
            .read(true)
            .write(false)
            .open(path)?;
        // NOTE: initializing this is somehow costy. Maybe don't initialize this?
        // (unsafe)
        let mut v = vec![0u8; id.length as usize];
        f.seek(futures_io::SeekFrom::Start(id.offset))?;
        f.read_exact(&mut v[..])?;
        Ok((v, stat.0.get(), stat.1.get()))
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Record {
    pub fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }

    pub fn ts(&self) -> Result<u64> {
        let ts = Key::decode_ts_from(&self.key)?.into_inner();
        Ok(ts)
    }
}

impl Source {
    #[tracing::instrument(skip_all)]
    pub async fn load_remote(
        &self,
        id: LogFileId,
        stat: &mut Option<&mut LoadStatistic>,
    ) -> Result<Vec<u8>> {
        let error_during_downloading = Arc::new(AtomicU64::new(0));
        let counter = error_during_downloading.clone();
        let ext = RetryExt::default().with_fail_hook(move |err: &JustRetry<std::io::Error>| {
            eprintln!("retry the error2: {:?}", err.0);
            counter.inc_by(1)
        });
        let fetch = || {
            let storage = self.inner.clone();
            let id = id.clone();
            async move {
                let mut content = vec![];
                let mut decompress = ZstdDecoder::new(Cursor::new(&mut content));
                let source = storage.read_part(&id.name, id.offset, id.length);
                let n = futures::io::copy(source, &mut decompress).await?;
                decompress.flush().await?;
                std::result::Result::<_, std::io::Error>::Ok((content, n))
            }
        };
        let (content, size) = retry_all_ext(fetch, ext).await?;
        stat.as_mut().map(|stat| {
            stat.physical_bytes_in += size;
            stat.error_during_downloading += error_during_downloading.get();
        });
        Ok(content)
    }

    #[tracing::instrument(skip_all, fields(id=?id))]
    pub async fn load(
        &self,
        id: LogFileId,
        mut stat: Option<&mut LoadStatistic>,
        mut on_key_value: impl FnMut(&[u8], &[u8]),
    ) -> Result<()> {
        let content = if let Some(cache_mgr) = &self.cache_manager {
            let (content, errors, loaded_bytes) = cache_mgr.load_file(id, &self.inner).await?;
            stat.as_mut().map(|s| {
                s.error_during_downloading += errors;
                s.physical_bytes_in += loaded_bytes;
            });
            content
        } else {
            self.load_remote(id, &mut stat).await?
        };

        let mut co = Cooperate::new(4096);
        let mut iter = stream_event::EventIterator::new(&content);
        loop {
            if !iter.valid() {
                break;
            }

            iter.next()?;
            co.step().await;
            on_key_value(iter.key(), iter.value());
            stat.as_mut().map(|stat| {
                stat.keys_in += 1;
                stat.logical_key_bytes_in += iter.key().len() as u64;
                stat.logical_value_bytes_in += iter.value().len() as u64;
            });
        }
        stat.as_mut().map(|stat| stat.files_in += 1);
        Ok(())
    }
}