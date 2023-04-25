// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
//! This mod provides the ability of managing the temporary files generated by
//! log backup.

use std::{
    collections::HashMap,
    fs::File as SyncOsFile,
    io::{Read, Seek},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex as BlockMutex, MutexGuard,
    },
    task::{ready, Context, Poll},
};

use futures::{io::Cursor, Future, FutureExt, TryFutureExt};
use futures_io::SeekFrom;
use kvproto::brpb::CompressionType;
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    mpsc::Receiver,
    stream::block_on_external_io,
};
use tokio::{
    fs::File as OsFile,
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter},
    pin,
    sync::Mutex,
};

use crate::{
    errors::{Error, Result},
    utils::{CompressionWriter, ZstdCompressionWriter}, annotate,
};

pub struct Config {
    /// When the in memory bytes reaches this, start to flush files into disk at
    /// background.
    pub soft_max: usize,
    /// When the in memory bytes reaches this, abort the log backup task.
    pub hard_max: usize,
    /// The base directory for swapping out files.
    pub swap_files: PathBuf,
    /// The compression type used for compression.
    pub artificate_compression: CompressionType,
}

pub struct TempFilePool {
    cfg: Config,
    current: AtomicUsize,
    files: BlockMutex<FileSet>,
}

#[derive(Default)]
struct File {
    content: Arc<BlockMutex<Content>>,
    writer_count: Arc<AtomicU8>,
}

enum PersistentFile {
    Compressed(ZstdCompressionWriter),
    Plain(OsFile),
}

#[derive(Default)]
struct Content {
    in_mem: Vec<u8>,
    external_file: Option<PersistentFile>,

    /// self.mem[0..written] has been written to out file.
    written: usize,
}

pub struct ForWrite {
    content: Arc<BlockMutex<Content>>,

    shared: Arc<TempFilePool>,
    rel_path: PathBuf,

    ref_counter: Arc<AtomicU8>,
    done: bool,
}

pub struct ForRead {
    content: Arc<BlockMutex<Content>>,

    myfile: Option<OsFile>,
    read: usize,
}

#[derive(Default)]
struct FileSet {
    items: HashMap<PathBuf, File>,
}

impl TempFilePool {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            current: AtomicUsize::new(0usize),
            files: BlockMutex::default(),
        }
    }

    pub fn open(self: &Arc<Self>, p: &Path) -> ForWrite {
        let mut fs = self.files.lock().unwrap();
        let f = fs.items.entry(p.to_owned()).or_default();
        let fr = ForWrite {
            content: Arc::clone(&f.content),
            shared: Arc::clone(self),
            ref_counter: Arc::clone(&f.writer_count),
            rel_path: p.to_owned(),
            done: false,
        };
        fr
    }

    /// Open a file reference for reading.
    pub fn open_for_read(&self, p: &Path) -> std::io::Result<ForRead> {
        use std::io::{Error, ErrorKind};

        let mut fs = self.files.lock().unwrap();
        let f = fs.items.get(p);
        if f.is_none() {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("file {} not found", p.display()),
            ));
        }
        let f = f.unwrap();
        if f.writer_count.load(Ordering::SeqCst) > 0 {
            // NOTE: the current implementation doesn't allow us to write when there are
            // readers, because once the writter swapped out the file, the
            // reader may not notice that.
            return Err(Error::new(
                ErrorKind::Other,
                "open_for_read isn't allowed when there are concurrent writing.",
            ));
        }
        let st = f.content.lock().unwrap();
        let myfile = if st.external_file.is_some() {
            Some(self.open_relative(p)?)
        } else {
            None
        };
        Ok(ForRead {
            content: Arc::clone(&f.content),
            myfile,
            read: 0,
        })
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    /// Create a file for writting.
    /// This function is synchronous so we can call it easier in the polling
    /// context. (Anyway, it is really hard to call an async function in the
    /// polling context.)
    fn create_relative(&self, p: &Path) -> std::io::Result<PersistentFile> {
        use std::io::{Error, ErrorKind};
        let file = OsFile::from_std(SyncOsFile::create(self.cfg.swap_files.join(p))?);
        let pfile = match self.cfg.artificate_compression {
            CompressionType::Unknown => PersistentFile::Plain(file),
            CompressionType::Zstd => {
                PersistentFile::Compressed(ZstdCompressionWriter::new(BufWriter::new(file)))
            }
            _ => Err(Error::new(
                ErrorKind::Unsupported,
                format!(
                    "the compression {:?} isn't supported.",
                    self.cfg.artificate_compression
                ),
            ))?,
        };
        Ok(pfile)
    }

    /// Open a file by a relative path.
    /// This will open a raw OS file for reading. The file content may be
    /// compressed if the configuration requires.
    fn open_relative(&self, p: &Path) -> std::io::Result<OsFile> {
        let file = SyncOsFile::open(self.cfg.swap_files.join(p))?;
        Ok(OsFile::from_std(file))
    }
}

impl ForWrite {
    pub fn path(&self) -> &Path {
        &self.rel_path
    }

    pub async fn done(&mut self) -> Result<()> {
        self.done = true;
        self.ref_counter.fetch_sub(1, Ordering::SeqCst);
        let st_lock = self.content.clone();
        tokio::task::spawn_blocking(move || {
            let mut st = st_lock.lock().unwrap();
            if let Some(PersistentFile::Compressed(c)) = &mut st.external_file {
                tokio::runtime::Handle::current().block_on(Pin::new(c).done())?;
            }
            Result::Ok(())
        }).map_err(|err| annotate!(err, "joining the background `done` job")).await??;
        Ok(())
    }
}

impl Content {
    fn poll_swap_out_unpin(
        &mut self,
        cx: &mut Context<'_>,
        shared: &TempFilePool,
        to_path: &Path,
    ) -> Poll<std::io::Result<()>> {
        loop {
            let to_write = &self.in_mem[self.written..];
            if to_write.is_empty() {
                self.in_mem = vec![];
                shared.current.fetch_sub(self.written, Ordering::SeqCst);
                self.written = 0;
                return Ok(()).into();
            }
            if self.external_file.is_none() {
                self.external_file = Some(shared.create_relative(to_path)?);
            }
            let ext_file = Pin::new(self.external_file.as_mut().unwrap());
            let n = ready!(ext_file.poll_write(cx, to_write))?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "during swapping out file",
                ))
                .into();
            }
            self.written += n;
        }
    }
}

impl AsyncWrite for ForWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        use std::io::{Error as IoErr, ErrorKind};
        let mem_use = self.shared.current.load(Ordering::SeqCst);
        let mut stat = self.content.lock().unwrap();
        let should_swap_out = mem_use > self.shared.cfg.soft_max && stat.in_mem.len() > 4096;
        if should_swap_out || stat.written > 0 {
            ready!(stat.poll_swap_out_unpin(cx, &self.shared, &self.rel_path))?;
        }
        if mem_use > self.shared.cfg.hard_max {
            return Err(IoErr::new(
                ErrorKind::OutOfMemory,
                format!(
                    "the memory usage {} exceeds the quota {}",
                    mem_use, self.shared.cfg.hard_max
                ),
            ))
            .into();
        }

        self.shared.current.fetch_add(buf.len(), Ordering::SeqCst);
        stat.in_mem.extend(buf.iter());
        Ok(buf.len()).into()
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        let mut stat = self.content.lock().unwrap();
        if let Some(f) = &mut stat.external_file {
            ready!(Pin::new(f).poll_flush(cx))?;
        }
        Ok(()).into()
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        let mut stat = self.content.lock().unwrap();
        if let Some(f) = &mut stat.external_file {
            ready!(Pin::new(f).poll_shutdown(cx))?;
        }
        Ok(()).into()
    }
}

impl Drop for ForWrite {
    fn drop(&mut self) {
        if !self.done {
            self.ref_counter.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl ForRead {
    pub async fn len(&self) -> Result<u64> {
        let len_in_file = if let Some(mf) = &self.myfile {
            mf.metadata().await?.len()
        } else {
            0
        };
        let mut st = self.content.lock().unwrap();
        let len_in_mem = st.in_mem.len() - st.written;
        Ok(len_in_file + len_in_mem as u64)
    }
}

impl AsyncRead for ForRead {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.read == 0 && this.myfile.is_some() {
            let old = buf.remaining();
            let ext_file = Pin::new(this.myfile.as_mut().unwrap());
            ready!(ext_file.poll_read(cx, buf))?;
            if buf.remaining() != old {
                return Ok(()).into();
            }
        }
        let st = this.content.lock().unwrap();
        let rem = buf.remaining();
        let fill_len = st.in_mem.len().min(rem);
        let to_fill = &st.in_mem[this.read..fill_len];
        buf.put_slice(to_fill);
        this.read += fill_len;
        Ok(()).into()
    }
}

impl AsyncWrite for PersistentFile {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        match self.get_mut() {
            PersistentFile::Compressed(c) => Pin::new(c).poll_write(cx, buf),
            PersistentFile::Plain(f) => Pin::new(f).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        match self.get_mut() {
            PersistentFile::Compressed(c) => Pin::new(c).poll_flush(cx),
            PersistentFile::Plain(f) => Pin::new(f).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        match self.get_mut() {
            PersistentFile::Compressed(c) => Pin::new(c).poll_shutdown(cx),
            PersistentFile::Plain(f) => Pin::new(f).poll_shutdown(cx),
        }
    }
}

fn block_on_current_rt<T>(f: impl Future<Output = T>) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(rt) => tokio::task::block_in_place(|| rt.block_on(f)),
        Err(_) => block_on_external_io(f),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use kvproto::brpb::CompressionType;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{Config, TempFilePool};

    fn rt_for_test() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    fn simple_pool(soft_max: usize) -> Arc<TempFilePool> {
        Arc::new(TempFilePool::new(Config {
            soft_max,
            hard_max: 99999999999,
            swap_files: std::env::temp_dir().join(format!("{}", std::process::id())),
            artificate_compression: CompressionType::Zstd,
        }))
    }

    #[test]
    fn test_read() {
        let pool = simple_pool(255);
        let mut f = pool.open("hello.txt".as_ref());
        let rt = rt_for_test();
        rt.block_on(f.write(b"Hello, world.")).unwrap();
        let mut cur = pool.open_for_read("hello.txt".as_ref()).unwrap();
        rt.block_on(rt.spawn(async move {
            let mut buf = [0u8; 6];
            assert_eq!(cur.read(&mut buf[..]).await.unwrap(), 6);
            assert_eq!(&buf, b"Hello,");
            assert_eq!(cur.read(&mut buf[..]).await.unwrap(), 6);
            assert_eq!(&buf, b" world");
        }))
        .unwrap();
    }

    #[test]
    fn test_swapout() {
        let pool = simple_pool(30);
        let mut f = pool.open("world.txt".as_ref());
        let rt = rt_for_test();
        rt.block_on(f.write(b"Once the word count...")).unwrap();
        rt.block_on(f.write(b"Reachs 30. The content of files shall be swaped out to the disk."))
            .unwrap();
        let mut cur = pool.open_for_read("world.txt".as_ref()).unwrap();
        let mut buf = vec![];
        rt.block_on(cur.read_to_end(&mut buf)).unwrap();
        assert_eq!(b"Once the word count...Reachs 30. The content of files shall be swaped out to the disk.", buf.as_slice());
        let mut local_file = std::fs::File::open(&pool.cfg.swap_files.join("world.txt"));
    }
}
