// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
//! This mod provides the ability of managing the temporary files generated by
//! log backup.

use std::{
    collections::HashMap,
    fs::File as SyncOsFile,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex as BlockMutex,
    },
    task::{ready, Context, Poll},
};

use futures::TryFutureExt;
use kvproto::brpb::CompressionType;
use tikv_util::warn;
use tokio::{
    fs::File as OsFile,
    io::{AsyncRead, AsyncWrite},
};

use crate::{
    annotate,
    errors::Result,
    utils::{CompressionWriter, ZstdCompressionWriter},
};

#[derive(Debug)]
pub struct Config {
    /// The max memory usage of the in memory file content.
    pub cache_size: usize,
    /// The base directory for swapping out files.
    pub swap_files: PathBuf,
    /// The compression type applied for files.
    pub content_compression: CompressionType,
    /// Prevent files with size less than this being swapped out.
    /// We perfer to swap larger files for reducing IOps.
    pub minimal_swap_out_file_size: usize,
    /// The buffer size for writting swap files.
    /// Even some of files has been swapped out, when new content appended,
    /// those content would be kept in memory before they reach a threshold.
    /// This would help us to reduce the I/O system calls.
    pub write_buffer_size: usize,
}

pub struct TempFilePool {
    cfg: Config,
    current: AtomicUsize,
    files: BlockMutex<FileSet>,

    #[cfg(test)]
    override_swapout: Option<
        Box<dyn Fn(&Path) -> Pin<Box<dyn AsyncWrite + Send + 'static>> + Send + Sync + 'static>,
    >,
}

impl std::fmt::Debug for TempFilePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TempFilePool")
            .field("cfg", &self.cfg)
            .field("current", &self.current)
            .finish()
    }
}

struct File {
    content: Arc<BlockMutex<FileCore>>,
    writer_count: Arc<AtomicU8>,
    reader_count: Arc<AtomicU8>,
}

enum PersistentFile {
    Plain(OsFile),
    #[cfg(test)]
    Dynamic(Pin<Box<dyn AsyncWrite + Send + 'static>>),
}

impl std::fmt::Debug for PersistentFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(_) => f.debug_tuple("Plain").finish(),
            #[cfg(test)]
            Self::Dynamic(_) => f.debug_tuple("Dynamic").finish(),
        }
    }
}

#[derive(Debug)]
struct FileCore {
    in_mem: Vec<u8>,
    external_file: Option<PersistentFile>,

    /// self.mem[0..written] has been written to out file.
    written: usize,
    the_pool: Arc<TempFilePool>,
    rel_path: PathBuf,
}

pub enum ForWrite {
    ZstdCompressed(ZstdCompressionWriter<ForWriteCore>),
    Plain(ForWriteCore),
}

#[derive(Debug)]
pub struct ForWriteCore {
    core: Arc<BlockMutex<FileCore>>,

    rel_path: PathBuf,
    ref_counter: Arc<AtomicU8>,
    done: bool,
}

#[derive(Debug)]
pub struct ForRead {
    content: Arc<BlockMutex<FileCore>>,

    myfile: Option<OsFile>,
    read: usize,
    ref_counter: Arc<AtomicU8>,
}

#[derive(Default)]
struct FileSet {
    items: HashMap<PathBuf, File>,
}

impl TempFilePool {
    pub fn new(cfg: Config) -> Result<Self> {
        if let Ok(true) = std::fs::metadata(&cfg.swap_files).map(|x| x.is_dir()) {
            warn!("find content in the swap file directory node. truncating them."; "dir" => %cfg.swap_files.display());
            std::fs::remove_dir_all(&cfg.swap_files)?;
        }
        std::fs::create_dir_all(&cfg.swap_files)?;

        let this = Self {
            cfg,
            current: AtomicUsize::new(0usize),
            files: BlockMutex::default(),

            #[cfg(test)]
            override_swapout: None,
        };
        Ok(this)
    }

    pub fn open_for_write(self: &Arc<Self>, p: &Path) -> std::io::Result<ForWrite> {
        use std::io::{Error, ErrorKind};
        let mut fs = self.files.lock().unwrap();
        let f = fs.items.entry(p.to_owned()).or_insert_with(|| File {
            content: Arc::new(BlockMutex::new(FileCore::new(
                Arc::clone(self),
                p.to_owned(),
            ))),
            writer_count: Arc::default(),
            reader_count: Arc::default(),
        });
        if f.reader_count.load(Ordering::SeqCst) > 0 {
            return Err(Error::new(
                ErrorKind::Other,
                "open_for_write isn't allowed when there are concurrent reading.",
            ));
        }
        let fr = ForWriteCore {
            core: Arc::clone(&f.content),
            ref_counter: Arc::clone(&f.writer_count),
            rel_path: p.to_owned(),
            done: false,
        };
        f.writer_count.fetch_add(1, Ordering::SeqCst);
        match self.cfg.content_compression {
            CompressionType::Unknown => Ok(ForWrite::Plain(fr)),
            CompressionType::Zstd => Ok(ForWrite::ZstdCompressed(ZstdCompressionWriter::new(fr))),
            unknown_compression => Err(Error::new(
                ErrorKind::Unsupported,
                format!(
                    "the compression method {:?} isn't supported for now.",
                    unknown_compression
                ),
            )),
        }
    }

    /// Open a file reference for reading.
    /// Please notice that once a compression applied, this would yield the
    /// compressed content (won't decompress them.) -- that is what "raw"
    /// implies.
    /// "But why there isn't a `open_for_read` which decompresses the content?"
    /// "Because in our use case, we only need the raw content -- we just send
    /// it to external storage."
    pub fn open_raw_for_read(&self, p: &Path) -> std::io::Result<ForRead> {
        use std::io::{Error, ErrorKind};

        let fs = self.files.lock().unwrap();
        let f = fs.items.get(p);
        if f.is_none() {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("file {} not found", p.display()),
            ));
        }
        let f = f.unwrap();
        let refc = f.writer_count.load(Ordering::SeqCst);
        if refc > 0 {
            // NOTE: the current implementation doesn't allow us to write when there are
            // readers, because once the writter swapped out the file, the reader may not
            // notice that. Perhaps in the future, we can implement something
            // like cursors to allow the reader be able to access consistent
            // File snapshot even there are writers appending contents
            // to the file. But that isn't needed for now.
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "open_for_read isn't allowed when there are concurrent writing (there are still {} reads for file {}.).",
                    refc,
                    p.display()
                ),
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
            ref_counter: Arc::clone(&f.reader_count),
            read: 0,
        })
    }

    /// Remove a file from the pool.
    /// If there are still some reference to the file, the deletion may be
    /// delaied until all reference to the file drop.
    pub fn remove(&self, p: &Path) -> bool {
        let mut files = self.files.lock().unwrap();
        files.items.remove(p).is_some()
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    /// Create a file for writting.
    /// This function is synchronous so we can call it easier in the polling
    /// context. (Anyway, it is really hard to call an async function in the
    /// polling context.)
    fn create_relative(&self, p: &Path) -> std::io::Result<PersistentFile> {
        let abs_path = self.cfg.swap_files.join(p);
        #[cfg(test)]
        let pfile = match &self.override_swapout {
            Some(f) => PersistentFile::Dynamic(f(&abs_path)),
            None => {
                let file = OsFile::from_std(SyncOsFile::create(&abs_path)?);
                PersistentFile::Plain(file)
            }
        };
        #[cfg(not(test))]
        let pfile = {
            let file = OsFile::from_std(SyncOsFile::create(abs_path)?);
            PersistentFile::Plain(file)
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

    fn delete_relative(&self, p: &Path) -> std::io::Result<()> {
        std::fs::remove_file(self.cfg.swap_files.join(p))?;
        Ok(())
    }
}

impl ForWrite {
    pub fn path(&self) -> &Path {
        match self {
            ForWrite::ZstdCompressed(z) => z.get_ref().path(),
            ForWrite::Plain(r) => r.path(),
        }
    }
}

#[async_trait::async_trait]
impl CompressionWriter for ForWrite {
    async fn done(&mut self) -> Result<()> {
        match self {
            ForWrite::ZstdCompressed(z) => {
                z.done().await?;
                z.get_mut().done().await
            }
            ForWrite::Plain(c) => c.done().await,
        }
    }
}

impl ForWriteCore {
    pub fn path(&self) -> &Path {
        &self.rel_path
    }

    pub async fn done(&mut self) -> Result<()> {
        // Given we have blocked new writes after we have `done`, it is safe to skip
        // flushing here.
        if self.done {
            return Ok(());
        }
        self.done = true;
        self.ref_counter.fetch_sub(1, Ordering::SeqCst);
        let core_lock = self.core.clone();
        // FIXME: For now, it cannot be awaited directly because `content` should be
        // guarded by a sync mutex. Given the `sync_all` is an async function,
        // it is almost impossible to implement some `poll` like things based on
        // it. We also cannot use an async mutex to guard the `content` : that will
        // make implementing `AsyncRead` and `AsyncWrite` become very very hard.
        tokio::task::spawn_blocking(move || {
            let mut st = core_lock.lock().unwrap();
            if let Some(PersistentFile::Plain(c)) = &mut st.external_file {
                tokio::runtime::Handle::current().block_on(async { c.sync_all().await })?;
            }
            Result::Ok(())
        })
        .map_err(|err| annotate!(err, "joining the background `done` job"))
        .await??;
        Ok(())
    }
}

impl FileCore {
    fn poll_swap_out_unpin(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        loop {
            let to_write = &self.in_mem[self.written..];
            let buf_size = self.the_pool.cfg.write_buffer_size;
            if to_write.is_empty() {
                modify_and_update_cap_diff(&mut self.in_mem, &self.the_pool.current, |v| {
                    v.clear();
                    v.shrink_to(buf_size);
                });
                self.written = 0;
                return Ok(()).into();
            }
            if self.external_file.is_none() {
                self.external_file = Some(self.the_pool.create_relative(&self.rel_path)?);
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

    fn append_to_buffer(&mut self, bs: &[u8]) {
        modify_and_update_cap_diff(&mut self.in_mem, &self.the_pool.current, |v| {
            v.extend_from_slice(bs);
        })
    }

    fn should_swap_out(&self) -> bool {
        let mem_use = self.the_pool.current.load(Ordering::SeqCst);
        let in_mem_exceeds_quota = self.in_mem.len() > self.the_pool.cfg.minimal_swap_out_file_size
            && mem_use > self.the_pool.cfg.cache_size;
        let already_exist =
            self.in_mem.len() > self.the_pool.cfg.write_buffer_size && self.external_file.is_some();
        let swapping = self.written > 0;
        in_mem_exceeds_quota || already_exist || swapping
    }

    fn new(pool: Arc<TempFilePool>, rel_path: PathBuf) -> Self {
        let cap = pool.cfg.write_buffer_size;
        let v = Vec::with_capacity(cap);
        pool.current.fetch_add(v.capacity(), Ordering::SeqCst);
        Self {
            in_mem: v,
            external_file: None,
            written: 0,
            the_pool: pool,
            rel_path,
        }
    }
}

impl AsyncWrite for ForWriteCore {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        use std::io::{Error as IoErr, ErrorKind};
        if self.done {
            return Err(IoErr::new(
                ErrorKind::BrokenPipe,
                "the write part has been closed",
            ))
            .into();
        }

        let mut stat = self.core.lock().unwrap();

        if stat.should_swap_out() {
            ready!(stat.poll_swap_out_unpin(cx))?;
        }

        stat.append_to_buffer(buf);
        Ok(buf.len()).into()
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        let mut stat = self.core.lock().unwrap();
        if let Some(f) = &mut stat.external_file {
            ready!(Pin::new(f).poll_flush(cx))?;
        }
        Ok(()).into()
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        let mut stat = self.core.lock().unwrap();
        if let Some(f) = &mut stat.external_file {
            ready!(Pin::new(f).poll_shutdown(cx))?;
        }
        Ok(()).into()
    }
}

impl Drop for FileCore {
    fn drop(&mut self) {
        self.the_pool
            .current
            .fetch_sub(self.in_mem.capacity(), Ordering::SeqCst);
        if self.external_file.is_some() {
            if let Err(err) = self.the_pool.delete_relative(&self.rel_path) {
                warn!("failed to remove the file."; "file" => %self.rel_path.display(), "err" => %err);
            }
        }
    }
}

impl Drop for ForWriteCore {
    fn drop(&mut self) {
        if !self.done {
            self.ref_counter.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl Drop for ForRead {
    fn drop(&mut self) {
        self.ref_counter.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ForRead {
    pub async fn len(&self) -> Result<u64> {
        let len_in_file = if let Some(mf) = &self.myfile {
            mf.metadata().await?.len()
        } else {
            0
        };
        let st = self.content.lock().unwrap();
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
        let fill_len = Ord::min(st.in_mem.len() - this.read, rem);
        let to_fill = &st.in_mem[this.read..this.read + fill_len];
        buf.put_slice(to_fill);
        this.read += fill_len;
        Ok(()).into()
    }
}

impl AsyncWrite for ForWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        match self.get_mut() {
            ForWrite::ZstdCompressed(c) => Pin::new(c).poll_write(cx, buf),
            ForWrite::Plain(p) => Pin::new(p).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        match self.get_mut() {
            ForWrite::ZstdCompressed(c) => Pin::new(c).poll_flush(cx),
            ForWrite::Plain(p) => Pin::new(p).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        match self.get_mut() {
            ForWrite::ZstdCompressed(c) => Pin::new(c).poll_shutdown(cx),
            ForWrite::Plain(p) => Pin::new(p).poll_shutdown(cx),
        }
    }
}

// NOTE: the implementation is exactly isomorphic to the implementation above.
// Perhaps we can implement AsyncWrite for Either<T, U> where T, U : AsyncWrite.
impl AsyncWrite for PersistentFile {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        match self.get_mut() {
            PersistentFile::Plain(f) => Pin::new(f).poll_write(cx, buf),
            #[cfg(test)]
            PersistentFile::Dynamic(d) => d.as_mut().poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        match self.get_mut() {
            PersistentFile::Plain(f) => Pin::new(f).poll_flush(cx),
            #[cfg(test)]
            PersistentFile::Dynamic(d) => d.as_mut().poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        match self.get_mut() {
            PersistentFile::Plain(f) => Pin::new(f).poll_shutdown(cx),
            #[cfg(test)]
            PersistentFile::Dynamic(d) => d.as_mut().poll_shutdown(cx),
        }
    }
}

#[inline(always)]
fn modify_and_update_cap_diff(v: &mut Vec<u8>, record: &AtomicUsize, f: impl FnOnce(&mut Vec<u8>)) {
    let cap_old = v.capacity();
    f(v);
    let cap_new = v.capacity();
    use std::cmp::Ordering::*;
    match cap_old.cmp(&cap_new) {
        Less => {
            record.fetch_add(cap_new - cap_old, Ordering::SeqCst);
        }
        Equal => {}
        Greater => {
            record.fetch_sub(cap_old - cap_new, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        io::Read,
        mem::ManuallyDrop,
        path::Path,
        pin::Pin,
        sync::{atomic::Ordering, Arc},
    };

    use async_compression::tokio::bufread::ZstdDecoder;
    use kvproto::brpb::CompressionType;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
    use walkdir::WalkDir;

    use super::{Config, TempFilePool};
    use crate::{tempfiles::ForWrite, utils::CompressionWriter};

    fn rt_for_test() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    fn simple_pool_with_modify(m: impl FnOnce(&mut Config)) -> Arc<TempFilePool> {
        let mut cfg = Config {
            cache_size: 99999999999,
            swap_files: std::env::temp_dir().join(format!(
                "backup_stream::tempfiles::test::{}",
                std::process::id()
            )),
            content_compression: CompressionType::Unknown,
            minimal_swap_out_file_size: 8192,
            write_buffer_size: 4096,
        };
        m(&mut cfg);
        Arc::new(TempFilePool::new(cfg).unwrap())
    }

    fn simple_pool_with_soft_max(soft_max: usize) -> Arc<TempFilePool> {
        simple_pool_with_modify(|cfg| {
            cfg.cache_size = soft_max;
            cfg.minimal_swap_out_file_size = 8192.min(soft_max)
        })
    }

    #[test]
    fn test_read() {
        let pool = simple_pool_with_soft_max(255);
        let mut f = pool.open_for_write("hello.txt".as_ref()).unwrap();
        let rt = rt_for_test();
        rt.block_on(f.write(b"Hello, world.")).unwrap();
        drop(f);
        let mut cur = pool.open_raw_for_read("hello.txt".as_ref()).unwrap();
        rt.block_on(rt.spawn(async move {
            let mut buf = [0u8; 6];
            assert_eq!(cur.read(&mut buf[..]).await.unwrap(), 6);
            assert_eq!(&buf, b"Hello,");
            let mut buf = [0u8; 6];
            assert_eq!(
                cur.read(&mut buf[..]).await.unwrap(),
                6,
                "{}",
                buf.escape_ascii()
            );
            assert_eq!(&buf, b" world");
        }))
        .unwrap();
    }

    #[test]
    fn test_swapout() {
        let pool = simple_pool_with_soft_max(30);
        let mut f = pool.open_for_write("world.txt".as_ref()).unwrap();
        let rt = rt_for_test();
        rt.block_on(f.write(b"Once the word count...")).unwrap();
        rt.block_on(f.write(b"Reachs 30. The content of files shall be swaped out to the disk."))
            .unwrap();
        rt.block_on(f.write(b"Isn't it? This swap will be finished in this call."))
            .unwrap();
        rt.block_on(f.done()).unwrap();
        let mut cur = pool.open_raw_for_read("world.txt".as_ref()).unwrap();
        let mut buf = vec![];
        rt.block_on(cur.read_to_end(&mut buf)).unwrap();
        let excepted = b"Once the word count...Reachs 30. The content of files shall be swaped out to the disk.Isn't it? This swap will be finished in this call.";
        assert_eq!(
            excepted,
            buf.as_slice(),
            "\n{}\n ## \n{}",
            excepted.escape_ascii(),
            buf.escape_ascii()
        );

        // The newly written bytes would be kept in memory.
        let excepted = b"Once the word count...Reachs 30. The content of files shall be swaped out to the disk.";
        let mut local_file = pool
            .open_relative("world.txt".as_ref())
            .unwrap()
            .try_into_std()
            .unwrap();
        buf.clear();
        local_file.read_to_end(&mut buf).unwrap();
        assert_eq!(
            excepted,
            buf.as_slice(),
            "\n{}\n ## \n{}",
            excepted.escape_ascii(),
            buf.escape_ascii()
        );
    }

    #[test]
    fn test_compression() {
        let pool = simple_pool_with_modify(|cfg| {
            cfg.content_compression = CompressionType::Zstd;
            cfg.cache_size = 15;
            cfg.minimal_swap_out_file_size = 15;
        });
        let file_name = "compression.bin";
        let rt = rt_for_test();
        let mut f = pool.open_for_write(file_name.as_ref()).unwrap();
        let content_to_write : [&[u8]; 4] = [
            b"Today, we are going to test the compression.",
            b"Well, once swaped out, the current implementation will keep new content in the buffer.",
            b"...until it reachs a constant. (That may be configuriable while you are reading this.)",
            b"Meow!",
        ];
        for content in content_to_write {
            assert_eq!(rt.block_on(f.write(content)).unwrap(), content.len());
            match &mut f {
                // Flush the compressed writer so we can test swapping out.
                ForWrite::ZstdCompressed(z) => rt.block_on(z.flush()).unwrap(),
                ForWrite::Plain(_) => unreachable!(),
            }
        }
        rt.block_on(f.done()).unwrap();

        let r = pool.open_raw_for_read(file_name.as_ref()).unwrap();
        let mut buf = vec![];
        let mut dr = ZstdDecoder::new(BufReader::new(r));
        rt.block_on(dr.read_to_end(&mut buf)).unwrap();
        let required = content_to_write.join(&b""[..]);
        assert_eq!(required, buf);
    }

    #[test]
    fn test_write_many_times() {
        let mut pool = simple_pool_with_modify(|cfg| {
            cfg.cache_size = 15;
            cfg.minimal_swap_out_file_size = 15;
        });
        Arc::get_mut(&mut pool).unwrap().override_swapout = Some(Box::new(|p| {
            println!("creating {}", p.display());
            Box::pin(ThrottleWrite(tokio::fs::File::from_std(
                std::fs::File::create(p).unwrap(),
            )))
        }));
        struct ThrottleWrite<R>(R);
        impl<R: AsyncWrite + Unpin> AsyncWrite for ThrottleWrite<R> {
            fn poll_write(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buf: &[u8],
            ) -> std::task::Poll<Result<usize, std::io::Error>> {
                let take = 2.min(buf.len());
                Pin::new(&mut self.0).poll_write(cx, &buf[..take])
            }

            fn poll_flush(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), std::io::Error>> {
                Pin::new(&mut self.0).poll_flush(cx)
            }

            fn poll_shutdown(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), std::io::Error>> {
                Pin::new(&mut self.0).poll_shutdown(cx)
            }
        }
        let file_name = "evil-os.txt";
        let rt = rt_for_test();
        let content_to_write: [&[u8]; 4] = [
            b"In this case, we are going to test over a evil OS.",
            b"In that OS, every `write` system call only writes 2 bytes.",
            b"That is a sort of hell... A nightmare of computer scientists.",
            b"Thankfully we are just testing. May such OS never exist.",
        ];

        let mut f = pool.open_for_write(file_name.as_ref()).unwrap();
        for content in content_to_write {
            assert_eq!(rt.block_on(f.write(content)).unwrap(), content.len());
        }
        rt.block_on(f.done()).unwrap();
        let mut dr = pool.open_raw_for_read(file_name.as_ref()).unwrap();
        let mut buf = vec![];
        rt.block_on(dr.read_to_end(&mut buf)).unwrap();
        let required = content_to_write.join(&b""[..]);
        assert_eq!(required, buf);
    }

    #[test]
    fn test_read_many_times() {
        let pool = simple_pool_with_modify(|cfg| {
            cfg.cache_size = 15;
            cfg.minimal_swap_out_file_size = 15;
        });
        let file_name = "read many times.txt";
        let rt = rt_for_test();
        let mut f = pool.open_for_write(file_name.as_ref()).unwrap();
        let content_to_write: [&[u8]; 4] = [
            b"In this case, we are going to make sure that a file can be read many times after",
            b"Before this file deleted, we should be able to read it many times.",
            b"(Which is essential for retrying.)",
            b"But when to delete them? You shall delete them after uploading them manually.",
        ];

        for content in content_to_write {
            assert_eq!(rt.block_on(f.write(content)).unwrap(), content.len());
        }
        rt.block_on(f.done()).unwrap();

        let mut buf = vec![];
        for _ in 0..3 {
            let mut r = pool.open_raw_for_read(file_name.as_ref()).unwrap();
            rt.block_on(r.read_to_end(&mut buf)).unwrap();
            assert_eq!(content_to_write.join(&b""[..]), buf.as_slice());
            buf.clear();
        }
    }

    fn assert_dir_empty(p: &Path) {
        for file in WalkDir::new(p) {
            let file = file.unwrap();
            if file.depth() > 0 {
                panic!("file leaked: {}", file.path().display());
            }
        }
    }

    #[test]
    fn test_not_leaked() {
        // Open a distinct dir for this case.
        let tmp = tempdir().unwrap();
        let pool = simple_pool_with_modify(|cfg| {
            cfg.cache_size = 15;
            cfg.minimal_swap_out_file_size = 15;
            cfg.swap_files = tmp.path().to_owned();
        });
        let rt = rt_for_test();
        let content_to_write: [&[u8]; 4] = [
            b"This case tests whether the resource(Say, files, memory.) leaked.",
            b"That is it, but I wanna write 4 sentences to keep every case aliged.",
            b"What to write? Perhaps some poems or lyrics.",
            b"But will that bring some copyright conflicts? Emmm, 4 sentences already, bye.",
        ];
        let file_names = ["object-a.txt", "object-b.txt"];

        let mut buf = vec![];
        for file_name in file_names {
            let mut f = pool.open_for_write(file_name.as_ref()).unwrap();
            for content in content_to_write {
                assert_eq!(rt.block_on(f.write(content)).unwrap(), content.len());
            }
            rt.block_on(f.done()).unwrap();
            let mut r = pool.open_raw_for_read(file_name.as_ref()).unwrap();
            rt.block_on(r.read_to_end(&mut buf)).unwrap();
            assert_eq!(content_to_write.join(&b""[..]), buf.as_slice());
            buf.clear();
        }
        for file_name in file_names {
            assert!(pool.remove(file_name.as_ref()));
        }
        assert_eq!(pool.current.load(Ordering::SeqCst), 0);
        assert_dir_empty(tmp.path());
    }

    #[test]
    fn test_panic_not_leaked() {
        let tmp = tempdir().unwrap();
        let pool = simple_pool_with_modify(|cfg| {
            cfg.cache_size = 15;
            cfg.minimal_swap_out_file_size = 15;
            cfg.swap_files = tmp.path().to_owned();
        });
        let rt = rt_for_test();
        let content_to_write: [&[u8]; 4] = [
            b"This case is pretty like the previous case, the different is in this case...",
            b"We are going to simulating TiKV panic. That will be implemented by leak the pool itself.",
            b"Emm, is there information need to be added? Nope. Well let me write you a random string.",
            b"A cat in my dream, leaps across the fence around the yard.",
        ];
        let mut f = pool.open_for_write("delete-me.txt".as_ref()).unwrap();
        for content in content_to_write {
            assert_eq!(rt.block_on(f.write(content)).unwrap(), content.len());
        }
        drop(f);
        // TiKV panicked!
        let _ = ManuallyDrop::new(pool);

        let pool = simple_pool_with_modify(|cfg| {
            cfg.swap_files = tmp.path().to_owned();
        });
        assert_dir_empty(tmp.path());
        let mut f = pool.open_for_write("delete-me.txt".as_ref()).unwrap();
        for content in content_to_write {
            assert_eq!(rt.block_on(f.write(content)).unwrap(), content.len());
        }
        drop(f);
        // Happy path.
        drop(pool);
        assert_dir_empty(tmp.path());
    }
}
