// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    collections::{BTreeMap, HashMap},
    io,
    path::{Path, PathBuf},
    result,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, RwLock as SyncRwLock,
    },
};

use crate::{
    endpoint::Task,
    errors::Error,
    metadata::{store::EtcdStore, StreamTask},
    utils::SlotMap,
};

use super::errors::Result;
use engine_traits::{CF_DEFAULT, CF_WRITE};

use external_storage::{BackendConfig, UnpinReader};
use external_storage_export::{create_storage, ExternalStorage};

use futures::io::{AllowStdIo, Cursor};
use kvproto::{
    brpb::{DataFileInfo, FileType, Metadata},
    raft_cmdpb::CmdType,
};
use openssl::hash::{Hasher, MessageDigest};
use protobuf::{Message, RepeatedField};
use raftstore::coprocessor::CmdBatch;
use slog_global::debug;
use tidb_query_datatype::codec::table::decode_table_id;
use tikv_util::{box_err, info, time::Limiter, warn, worker::Scheduler};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::{fs::remove_file, fs::File};
use txn_types::{Key, TimeStamp};

#[derive(Debug)]
pub struct ApplyEvent {
    key: Vec<u8>,
    value: Vec<u8>,
    cf: String,
    region_id: u64,
    region_resolved_ts: u64,
    cmd_type: CmdType,
}

impl ApplyEvent {
    /// Convert a [CmdBatch] to a vector of events. Ignoring admin / error commands.
    /// Assuming the resolved ts of the region is `resolved_ts`.
    /// Note: the resolved ts cannot be advanced if there is no command,
    ///       maybe we also need to update resolved_ts when flushing?
    pub fn from_cmd_batch(cmd: CmdBatch, resolved_ts: u64) -> Vec<Self> {
        let region_id = cmd.region_id;
        let mut result = vec![];
        for mut req in cmd
            .cmds
            .into_iter()
            .filter(|cmd| {
                // We will add some log then, this is just a template.
                #[allow(clippy::if_same_then_else)]
                #[allow(clippy::needless_bool)]
                if cmd.response.get_header().has_error() {
                    // Add some log for skipping the error.
                    false
                } else if cmd.request.has_admin_request() {
                    // Add some log for skipping the admin request.
                    false
                } else {
                    true
                }
            })
            .flat_map(|mut cmd| cmd.request.take_requests().into_iter())
        {
            let (key, value, cf) = match req.get_cmd_type() {
                CmdType::Put => {
                    let mut put = req.take_put();
                    (put.take_key(), put.take_value(), put.cf)
                }
                CmdType::Delete => {
                    let mut del = req.take_delete();
                    (del.take_key(), Vec::new(), del.cf)
                }
                _ => {
                    debug!(
                        "backup stream skip other command";
                        "command" => ?req,
                    );
                    continue;
                }
            };
            result.push(Self {
                key,
                value,
                cf,
                region_id,
                region_resolved_ts: resolved_ts,
                cmd_type: req.get_cmd_type(),
            })
        }
        result
    }

    /// Check whether the key associate to the event is a meta key.
    pub fn is_meta(&self) -> bool {
        // Can we make things not looking so hacky?
        self.key.starts_with(b"m")
    }

    /// Check whether the event should be recorded.
    /// (We would ignore LOCK cf)
    pub fn should_record(&self) -> bool {
        let cf_can_handle = self.cf == CF_DEFAULT || self.cf == CF_WRITE;
        // should we handle prewrite here?
        let cmd_can_handle = self.cmd_type == CmdType::Delete || self.cmd_type == CmdType::Put;
        cf_can_handle && cmd_can_handle
    }
}

/// The shared version of router.
#[derive(Debug, Clone)]
pub struct Router(Arc<RouterInner>);

impl Router {
    /// Create a new router with the temporary folder.
    pub fn new(prefix: PathBuf, scheduler: Scheduler<Task>, temp_file_size_limit: u64) -> Self {
        Self(Arc::new(RouterInner::new(
            prefix,
            scheduler,
            temp_file_size_limit,
        )))
    }
}

impl std::ops::Deref for Router {
    type Target = RouterInner;

    fn deref(&self) -> &Self::Target {
        Arc::deref(&self.0)
    }
}

/// An Router for Backup Stream.
///
/// It works as a table-filter.
///   1. route the kv event to different task
///   2. filter the kv event not belong to the task
// TODO maybe we should introduce table key from tidb_query_datatype module.
pub struct RouterInner {
    // TODO find a proper way to record the ranges of table_filter.
    // TODO replace all map like things with lock free map, to get rid of the Mutex.
    /// The index for search tasks by range.
    /// It uses the `start_key` of range as the key.
    /// Given there isn't overlapping, we can simply use binary search to find
    /// which range a point belongs to.
    ranges: SyncRwLock<BTreeMap<KeyRange, TaskRange>>,
    /// The temporary files associated to some task.
    tasks: Mutex<HashMap<String, Arc<StreamTaskInfo>>>,
    /// The temporary directory for all tasks.
    prefix: PathBuf,

    /// The handle to Endpoint, we should send `Flush` to endpoint if there are too many temporary files.
    scheduler: Scheduler<Task>,
    /// The size limit of temporary file per task.
    temp_file_size_limit: u64,
}

impl std::fmt::Debug for RouterInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterInner")
            .field("ranges", &self.ranges)
            .field("tasks", &self.tasks)
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl RouterInner {
    pub fn new(prefix: PathBuf, scheduler: Scheduler<Task>, temp_file_size_limit: u64) -> Self {
        RouterInner {
            ranges: SyncRwLock::new(BTreeMap::default()),
            tasks: Mutex::new(HashMap::default()),
            prefix,
            scheduler,
            temp_file_size_limit,
        }
    }

    /// Register some ranges associated to some task.
    /// Because the observer interface yields encoded data key, the key should be ENCODED DATA KEY too.    
    /// (i.e. encoded by `Key::from_raw(key).into_encoded()`, [`utils::wrap_key`] could be a shortcut.).    
    /// We keep ranges in memory to filter kv events not in these ranges.  
    fn register_ranges(&self, task_name: &str, ranges: Vec<(Vec<u8>, Vec<u8>)>) {
        // TODO reigister ranges to filter kv event
        // register ranges has two main purpose.
        // 1. filter kv event that no need to backup
        // 2. route kv event to the corresponding file.

        let mut w = self.ranges.write().unwrap();
        for range in ranges {
            let key_range = KeyRange(range.0);
            let task_range = TaskRange {
                end: range.1,
                task_name: task_name.to_string(),
            };
            debug!(
                "backup stream register observe range";
                "task_name" => task_name,
                "start_key" => &log_wrappers::Value::key(&key_range.0),
                "end_key" => &log_wrappers::Value::key(&task_range.end),
            );
            w.insert(key_range, task_range);
        }
    }

    // register task info ans range info to router
    pub async fn register_task(
        &self,
        mut task: StreamTask,
        ranges: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let task_name = task.info.take_name();

        // register ragnes
        self.register_ranges(&task_name, ranges);

        // register task info
        let prefix_path = self.prefix.join(&task_name);
        let stream_task = StreamTaskInfo::new(prefix_path, task).await?;

        let _ = self
            .tasks
            .lock()
            .await
            .insert(task_name, Arc::new(stream_task));
        Ok(())
    }

    /// get the task name by a key.
    pub fn get_task_by_key(&self, key: &[u8]) -> Option<String> {
        // TODO avoid key.to_vec()
        let r = self.ranges.read().unwrap();
        let k = &KeyRange(key.to_vec());
        r.range(..k)
            .next_back()
            .filter(|r| key <= &r.1.end[..] && key >= &r.0.0[..])
            .map_or_else(
                || {
                    r.range(k..)
                        .next()
                        .filter(|r| key <= &r.1.end[..] && key >= &r.0.0[..])
                        .map(|r| r.1.task_name.clone())
                },
                |r| Some(r.1.task_name.clone()),
            )
    }

    pub async fn on_event(&self, kv: ApplyEvent) -> Result<()> {
        if let Some(task) = self.get_task_by_key(&kv.key) {
            debug!(
                "backup stream kv";
                "cmdtype" => ?kv.cmd_type,
                "cf" => ?kv.cf,
                "key" => &log_wrappers::Value::key(&kv.key),
            );

            let task_info = match self.tasks.lock().await.get(&task) {
                Some(t) => t.clone(),
                None => {
                    info!("backup stream no task"; "task" => ?task);
                    return Err(Error::NoSuchTask { task_name: task });
                }
            };
            task_info.on_event(kv).await?;

            // When this event make the size of temporary files exceeds the size limit, make a flush.
            // Note that we only flush if the size is less than the limit before the event,
            // or we may send multiplied flush requests.
            debug!(
                "backup stream statics size";
                "task" => ?task,
                "next_size" => task_info.total_size(),
                "size_limit" => self.temp_file_size_limit,
            );
            let cur_size = task_info.total_size();
            if cur_size > self.temp_file_size_limit && !task_info.is_flushing() {
                info!("try flushing task"; "task" => %task, "size" => %cur_size);
                if task_info.set_flushing_status_cas(false, true).is_ok() {
                    // delay the schedule when failure? (Why the scheduler doesn't support blocking send...)
                    if self.scheduler.schedule(Task::Flush(task)).is_err() {
                        // oops... we failed, let's leave the chance to next challenger.
                        task_info.set_flushing_status(false);
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn do_flush(&self, task_name: &str, store_id: u64) {
        debug!("backup stream do flush"; "task" => task_name);
        if let Some(task_info) = self.tasks.lock().await.get(task_name) {
            if let Err(e) = task_info.do_flush(store_id).await {
                warn!("backup steam do flush fail"; "err" => ?e);
            }
            // set false to flushing whether success or fail
            task_info.set_flushing_status(false);
        }
    }
}

/// The handle of a temporary file.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
struct TempFileKey {
    is_meta: bool,
    table_id: i64,
    region_id: u64,
    cf: String,
    cmd_type: CmdType,
}

impl TempFileKey {
    /// Create the key for an event. The key can be used to find which temporary file the event should be stored.
    fn of(kv: &ApplyEvent) -> Self {
        let table_id = if kv.is_meta() {
            // Force table id of meta key be zero.
            0
        } else {
            // When we cannot extract the table key, use 0 for the table key(perhaps we insert meta key here.).
            // Can we emit the copy here(or at least, take a slice of key instead of decoding the whole key)?
            Key::from_encoded_slice(&kv.key)
                .into_raw()
                .ok()
                .and_then(|decoded_key| decode_table_id(&decoded_key).ok())
                .unwrap_or(0)
        };
        Self {
            is_meta: kv.is_meta(),
            table_id,
            region_id: kv.region_id,
            cf: kv.cf.clone(),
            cmd_type: kv.cmd_type,
        }
    }

    fn get_file_type(&self) -> FileType {
        let file_type = match self.cmd_type {
            CmdType::Put => FileType::Put,
            CmdType::Delete => FileType::Delete,
            _ => {
                warn!("error cmdtype"; "cmdtype" => ?self.cmd_type);
                panic!("error CmdType");
            }
        };
        file_type
    }

    /// The full name of the file owns the key.
    fn temp_file_name(&self) -> String {
        if self.is_meta {
            format!(
                "meta_{:08}_{}_{:?}_{}.temp.log",
                self.region_id,
                self.cf,
                self.cmd_type,
                TimeStamp::physical_now(),
            )
        } else {
            format!(
                "{:08}_{:08}_{}_{:?}_{}.temp.log",
                self.table_id,
                self.region_id,
                self.cf,
                self.cmd_type,
                TimeStamp::physical_now(),
            )
        }
    }

    fn path_to_log_file(&self, min_ts: u64) -> String {
        format!(
            // "/v1/t{:012}/{:012}-{}.log",
            "v1_t{:012}_{:012}-{}.log",
            self.table_id,
            min_ts,
            uuid::Uuid::new_v4()
        )
    }

    fn path_to_schema_file(min_ts: u64) -> String {
        // format!("/v1/m/{:012}-{}.log", min_ts, uuid::Uuid::new_v4())
        format!("v1_m{:012}-{}.log", min_ts, uuid::Uuid::new_v4())
    }

    fn file_name(&self, min_ts: TimeStamp) -> String {
        if self.is_meta {
            Self::path_to_schema_file(min_ts.into_inner())
        } else {
            self.path_to_log_file(min_ts.into_inner())
        }
    }
}

pub struct StreamTaskInfo {
    task: StreamTask,
    /// support external storage. eg local/s3.
    storage: Box<dyn ExternalStorage>,
    /// The parent directory of temporary files.
    temp_dir: PathBuf,
    /// The temporary file index. Both meta (m prefixed keys) and data (t prefixed keys).
    files: SlotMap<TempFileKey, DataFile>,
    /// flushing_files contains files pending flush.
    flushing_files: SlotMap<TempFileKey, DataFile>,
    /// The min resolved TS of all regions involved.
    min_resolved_ts: TimeStamp,
    /// Total size of all temporary files in byte.
    total_size: AtomicUsize,
    /// Whether those files are already requested to be flushed.
    ///
    /// This should only be set to `true` by `compare_and_set(current=false, value=ture)`.
    /// The thread who setting it to `true` takes the responsibility of sending the request to the
    /// scheduler for flushing the files then.
    ///
    /// If the request failed, that thread can set it to `false` back then.
    flushing: AtomicBool,
}

impl std::fmt::Debug for StreamTaskInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamTaskInfo")
            .field("task", &self.task.info.name)
            .field("temp_dir", &self.temp_dir)
            .field("min_resolved_ts", &self.min_resolved_ts)
            .field("total_size", &self.total_size)
            .field("flushing", &self.flushing)
            .finish()
    }
}

impl StreamTaskInfo {
    /// Create a new temporary file set at the `temp_dir`.
    pub async fn new(temp_dir: PathBuf, task: StreamTask) -> Result<Self> {
        tokio::fs::create_dir_all(&temp_dir).await?;
        let storage = create_storage(task.info.get_storage(), BackendConfig::default())?;
        Ok(Self {
            task,
            storage,
            temp_dir,
            min_resolved_ts: TimeStamp::max(),
            files: SlotMap::default(),
            flushing_files: SlotMap::default(),
            total_size: AtomicUsize::new(0),
            flushing: AtomicBool::new(false),
        })
    }

    // TODO: make a file-level lock for getting rid of the &mut.
    /// Append a event to the files. This wouldn't trigger `fsync` syscall.
    /// i.e. No guarantee of persistence.
    pub async fn on_event(&self, kv: ApplyEvent) -> Result<()> {
        let key = TempFileKey::of(&kv);

        if let Some(f) = self.files.read().await.get(&key) {
            self.total_size
                .fetch_add(f.lock().await.on_event(kv).await?, Ordering::SeqCst);
            return Ok(());
        }

        // slow path: try to insert the element.
        let mut w = self.files.write().await;
        // double check before insert. there may be someone already insert that
        // when we are waiting for the write lock.
        if !w.contains_key(&key) {
            let path = self.temp_dir.join(key.temp_file_name());
            let val = Mutex::new(DataFile::new(path).await?);
            w.insert(key.clone(), val);
        }

        let f = w.get(&key).unwrap();
        self.total_size
            .fetch_add(f.lock().await.on_event(kv).await?, Ordering::SeqCst);

        Ok(())
    }

    pub fn total_size(&self) -> u64 {
        self.total_size.load(Ordering::SeqCst) as _
    }

    /// Flush all files and generate corresponding metadata.
    pub async fn generate_metadata(&self, store_id: u64) -> Result<MetadataInfo> {
        let w = self.flushing_files.read().await;
        // Let's flush all files first...
        futures::future::join_all(
            w.iter()
                .map(|(_, f)| async move { f.lock().await.inner.sync_all().await }),
        )
        .await
        .into_iter()
        .map(|r| r.map_err(Error::from))
        .fold(Ok(()), Result::and)?;

        let mut metadata = MetadataInfo::with_capacity(w.len());
        metadata.set_store_id(store_id);
        for (file_key, data_file) in w.iter() {
            let mut data_file = data_file.lock().await;
            let file_meta = data_file.generate_metadata(file_key)?;
            metadata.push(file_meta)
        }
        Ok(metadata)
    }

    pub fn set_flushing_status_cas(&self, expect: bool, new: bool) -> result::Result<bool, bool> {
        self.flushing
            .compare_exchange(expect, new, Ordering::SeqCst, Ordering::SeqCst)
    }

    pub fn set_flushing_status(&self, set_flushing: bool) {
        self.flushing.store(set_flushing, Ordering::SeqCst);
    }

    pub fn is_flushing(&self) -> bool {
        self.flushing.load(Ordering::SeqCst)
    }

    /// move need-flushing files to flushing_files.
    pub async fn move_to_flushing_files(&self) -> &Self {
        let mut w = self.files.write().await;
        for (k, v) in w.drain() {
            self.flushing_files.write().await.insert(k, v);
        }
        self
    }

    pub async fn flush_log(&self) -> Result<()> {
        // if failed to write storage, we should retry write flushing_files.
        for (_, v) in self.flushing_files.write().await.drain() {
            let data_file = v.lock().await;
            // to do: limiter to storage
            let limiter = Limiter::builder(std::f64::INFINITY).build();
            let reader = std::fs::File::open(data_file.local_path.clone()).unwrap();
            let reader = UnpinReader(Box::new(limiter.limit(AllowStdIo::new(reader))));
            let filepath = &data_file.storage_path;

            let ret = self.storage.write(filepath, reader, 1024).await;
            match ret {
                Ok(_) => {
                    debug!(
                        "backup stream flush success";
                        "tmp file" => ?data_file.local_path,
                        "storage file" => ?filepath,
                    );
                    self.total_size
                        .fetch_sub(data_file.file_size, Ordering::SeqCst);
                    let _ = data_file.remove_temp_file().await;
                }
                Err(e) => warn!("backup stream flush failed";
                    "file" => ?data_file.local_path,
                    "err" => ?e,
                ),
            }
        }

        Ok(())
    }

    pub async fn flush_meta(&self, metadata_info: MetadataInfo) -> Result<()> {
        let meta_path = metadata_info.path_to_meta();
        let meta_buff = metadata_info.marshar_to()?;
        let buflen = meta_buff.len();

        self.storage
            .write(
                &meta_path,
                UnpinReader(Box::new(Cursor::new(meta_buff))),
                buflen as _,
            )
            .await?;
        Ok(())
    }

    pub async fn do_flush(&self, store_id: u64) -> Result<()> {
        // do nothing if not flushing status.
        if !self.is_flushing() {
            return Ok(());
        }

        // generage meta data and prepare to flush to storage
        let metadata_info = self
            .move_to_flushing_files()
            .await
            .generate_metadata(store_id)
            .await?;

        // flush log file to storage.
        self.flush_log().await?;

        // flush meta file to storage.
        self.flush_meta(metadata_info).await?;
        Ok(())
    }
}

/// A opened log file with some metadata.
struct DataFile {
    min_ts: TimeStamp,
    max_ts: TimeStamp,
    resolved_ts: TimeStamp,
    sha256: Hasher,
    inner: File,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    number_of_entries: usize,
    file_size: usize,
    local_path: PathBuf,
    storage_path: String,
}

#[derive(Debug)]
pub struct MetadataInfo {
    pub files: Vec<DataFileInfo>,
    pub min_resolved_ts: u64,
    pub store_id: u64,
}

impl MetadataInfo {
    fn with_capacity(cap: usize) -> Self {
        Self {
            files: Vec::with_capacity(cap),
            min_resolved_ts: u64::MAX,
            store_id: 0,
        }
    }

    fn set_store_id(&mut self, store_id: u64) {
        self.store_id = store_id;
    }

    fn push(&mut self, file: DataFileInfo) {
        let rts = file.resolved_ts;
        self.min_resolved_ts = self.min_resolved_ts.min(rts);
        self.files.push(file);
    }

    fn marshar_to(self) -> Result<Vec<u8>> {
        let mut metadata = Metadata::new();
        metadata.set_file(RepeatedField::<DataFileInfo>::from_vec(self.files));
        metadata.set_store_id(self.store_id as _);
        metadata.set_resloved_ts(self.min_resolved_ts as _);

        metadata
            .write_to_bytes()
            .map_err(|err| Error::Other(box_err!("openssl hasher failed to init: {}", err)))
    }

    fn path_to_meta(&self) -> String {
        format!(
            // "/v1/backupmeta/{:012}-{}.meta",
            "v1_backupmeta_{:012}-{}.meta",
            self.min_resolved_ts,
            uuid::Uuid::new_v4()
        )
    }
}

impl DataFile {
    /// create and open a logfile at the path.
    /// Note: if a file with same name exists, would truncate it.
    async fn new(local_path: impl AsRef<Path>) -> Result<Self> {
        let sha256 = Hasher::new(MessageDigest::sha256())
            .map_err(|err| Error::Other(box_err!("openssl hasher failed to init: {}", err)))?;
        Ok(Self {
            min_ts: TimeStamp::max(),
            max_ts: TimeStamp::zero(),
            resolved_ts: TimeStamp::zero(),
            inner: File::create(local_path.as_ref()).await?,
            sha256,
            number_of_entries: 0,
            file_size: 0,
            start_key: vec![],
            end_key: vec![],
            local_path: local_path.as_ref().to_owned(),
            storage_path: String::default(),
        })
    }

    async fn remove_temp_file(&self) -> io::Result<()> {
        remove_file(&self.local_path).await
    }

    /// Add a new KV pair to the file, returning its size.
    async fn on_event(&mut self, mut kv: ApplyEvent) -> Result<usize> {
        let encoded = super::endpoint::Endpoint::<EtcdStore>::encode_event(&kv.key, &kv.value);
        let mut size = 0;
        for slice in encoded {
            let slice = slice.as_ref();
            self.inner.write_all(slice).await?;
            self.sha256.update(slice).map_err(|err| {
                Error::Other(box_err!("openssl hasher failed to update: {}", err))
            })?;
            size += slice.len();
        }
        let key = Key::from_encoded(std::mem::take(&mut kv.key));
        let ts = key.decode_ts().expect("key without ts");
        self.min_ts = self.min_ts.min(ts);
        self.max_ts = self.max_ts.max(ts);
        self.resolved_ts = self.resolved_ts.max(kv.region_resolved_ts.into());
        self.number_of_entries += 1;
        self.file_size += size;
        self.update_key_bound(key.into_encoded());
        Ok(size)
    }

    /// Update the `start_key` and `end_key` of `self` as if a new key added.
    fn update_key_bound(&mut self, key: Vec<u8>) {
        // if there is nothing in file, fill the start_key and end_key by current key.
        if self.start_key.is_empty() && self.end_key.is_empty() {
            self.start_key = key.clone();
            self.end_key = key;
            return;
        }

        // expand the start_key and end_key if key out-of-range joined.
        if self.start_key > key {
            self.start_key = key;
        } else if self.end_key < key {
            self.end_key = key;
        }
    }

    /// generage path for log file before flushing to Storage
    fn generage_storage_path(&mut self, path: String) {
        self.storage_path = path;
    }

    /// generate the metadata in protocol buffer of the file.
    fn generate_metadata(&mut self, file_key: &TempFileKey) -> Result<DataFileInfo> {
        self.generage_storage_path(file_key.file_name(self.min_ts));

        let mut meta = DataFileInfo::new();
        meta.set_sha_256(
            self.sha256
                .finish()
                .map(|bytes| bytes.to_vec())
                .map_err(|err| {
                    Error::Other(box_err!("openssl hasher failed to finish: {}", err))
                })?,
        );
        meta.set_path(self.storage_path.clone());
        meta.set_number_of_entries(self.number_of_entries as _);
        meta.set_max_ts(self.max_ts.into_inner() as _);
        meta.set_min_ts(self.min_ts.into_inner() as _);
        meta.set_resolved_ts(self.resolved_ts.into_inner() as _);
        meta.set_start_key(std::mem::take(&mut self.start_key));
        meta.set_end_key(std::mem::take(&mut self.end_key));

        meta.set_cf(file_key.cf.clone());
        meta.set_region_id(file_key.region_id as i64);
        meta.set_type(file_key.get_file_type());

        Ok(meta)
    }
}

impl std::fmt::Debug for DataFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFile")
            .field("min_ts", &self.min_ts)
            .field("max_ts", &self.max_ts)
            .field("resolved_ts", &self.resolved_ts)
            .field("local_path", &self.local_path.display())
            .finish()
    }
}

#[derive(Clone, Ord, PartialOrd, PartialEq, Eq, Debug)]
struct KeyRange(Vec<u8>);

#[derive(Clone, Debug)]
struct TaskRange {
    end: Vec<u8>,
    task_name: String,
}

#[cfg(test)]
mod tests {
    use crate::utils;

    use kvproto::brpb::{Local, StorageBackend, StreamBackupTaskInfo};

    use std::time::Duration;
    use tikv_util::{
        codec::number::NumberEncoder,
        worker::{dummy_scheduler, ReceiverWrapper},
    };

    use super::*;

    #[derive(Debug)]
    struct KvEventsBuilder {
        region_id: u64,
        region_resolved_ts: u64,
        events: Vec<ApplyEvent>,
    }

    fn make_table_key(table_id: i64, key: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut table_key = b"t".to_vec();
        // make it comparable to uint.
        table_key
            .encode_u64(table_id as u64 ^ 0x8000_0000_0000_0000)
            .unwrap();
        Write::write_all(&mut table_key, key).unwrap();
        table_key
    }

    impl KvEventsBuilder {
        fn new(region_id: u64, region_resolved_ts: u64) -> Self {
            Self {
                region_id,
                region_resolved_ts,
                events: vec![],
            }
        }

        fn wrap_key(&self, key: Vec<u8>) -> Vec<u8> {
            let key = Key::from_encoded(utils::wrap_key(key));
            key.append_ts(TimeStamp::compose(
                TimeStamp::physical_now(),
                self.events.len() as _,
            ))
            .into_encoded()
        }

        fn put_event(&self, cf: &'static str, key: Vec<u8>, value: Vec<u8>) -> ApplyEvent {
            ApplyEvent {
                key: self.wrap_key(key),
                value,
                cf: cf.to_owned(),
                region_id: self.region_id,
                region_resolved_ts: self.region_resolved_ts,
                cmd_type: CmdType::Put,
            }
        }

        fn delete_event(&self, cf: &'static str, key: Vec<u8>) -> ApplyEvent {
            ApplyEvent {
                key: self.wrap_key(key),
                value: vec![],
                cf: cf.to_owned(),
                region_id: self.region_id,
                region_resolved_ts: self.region_resolved_ts,
                cmd_type: CmdType::Delete,
            }
        }

        fn put_table(&mut self, cf: &'static str, table: i64, key: &[u8], value: &[u8]) {
            let table_key = make_table_key(table, key);
            self.events
                .push(self.put_event(cf, table_key, value.to_vec()));
        }

        fn delete_table(&mut self, cf: &'static str, table: i64, key: &[u8]) {
            let table_key = make_table_key(table, key);
            self.events.push(self.delete_event(cf, table_key));
        }

        fn flush_events(&mut self) -> Vec<ApplyEvent> {
            std::mem::take(&mut self.events)
        }
    }

    #[test]
    fn test_register() {
        let (tx, _) = dummy_scheduler();
        let router = RouterInner::new(PathBuf::new(), tx, 1024);
        // -----t1.start-----t1.end-----t2.start-----t2.end------
        // --|------------|----------|------------|-----------|--
        // case1        case2      case3        case4       case5
        // None        Found(t1)    None        Found(t2)   None
        router.register_ranges("t1", vec![(vec![1, 2, 3], vec![2, 3, 4])]);

        router.register_ranges("t2", vec![(vec![2, 3, 6], vec![3, 4])]);

        assert_eq!(router.get_task_by_key(&[1, 1, 1]), None);
        assert_eq!(router.get_task_by_key(&[1, 2, 4]), Some("t1".to_string()),);
        assert_eq!(router.get_task_by_key(&[2, 3, 5]), None);
        assert_eq!(router.get_task_by_key(&[2, 4]), Some("t2".to_string()),);
        assert_eq!(router.get_task_by_key(&[4, 4]), None,)
    }

    fn collect_recv(mut rx: ReceiverWrapper<Task>) -> Vec<Task> {
        let mut result = vec![];
        while let Ok(Some(task)) = rx.recv_timeout(Duration::from_secs(0)) {
            result.push(task);
        }
        result
    }

    fn create_local_storage_backend(path: String) -> StorageBackend {
        let mut local = Local::default();
        local.set_path(path);

        let mut sb = StorageBackend::default();
        sb.set_local(local);
        sb
    }

    #[tokio::test]
    async fn test_basic_file() -> Result<()> {
        let tmp = std::env::temp_dir().join(format!("{}", uuid::Uuid::new_v4()));
        println!("tmp_path={:?}", tmp);
        tokio::fs::create_dir_all(&tmp).await?;
        let (tx, rx) = dummy_scheduler();
        let router = RouterInner::new(tmp.clone(), tx, 32);
        let mut stream_task = StreamBackupTaskInfo::default();
        stream_task.set_name("dummy".to_string());
        let storage_path = std::env::temp_dir().join(format!("{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&storage_path).await?;
        println!("storage={:?}", storage_path);
        stream_task.set_storage(create_local_storage_backend(
            storage_path.to_str().unwrap().to_string(),
        ));

        router
            .register_task(
                StreamTask { info: stream_task },
                vec![(
                    utils::wrap_key(make_table_key(1, b"")),
                    utils::wrap_key(make_table_key(2, b"")),
                )],
            )
            .await?;
        let now = TimeStamp::physical_now();
        let mut region1 = KvEventsBuilder::new(1, now);
        let start_ts = TimeStamp::physical_now();
        region1.put_table(CF_DEFAULT, 1, b"hello", b"world");
        region1.put_table(CF_WRITE, 1, b"hello", b"this isn't a write record :3");
        region1.put_table(CF_WRITE, 1, b"bonjour", b"this isn't a write record :3");
        region1.put_table(CF_WRITE, 1, b"nihao", b"this isn't a write record :3");
        region1.put_table(CF_WRITE, 2, b"hello", b"this isn't a write record :3");
        region1.put_table(CF_WRITE, 1, b"hello", b"still isn't a write record :3");
        region1.delete_table(CF_DEFAULT, 1, b"hello");
        println!("{:?}", region1);
        let events = region1.flush_events();
        for event in events {
            router.on_event(event).await?;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let end_ts = TimeStamp::physical_now();
        let files = router.tasks.lock().await.get("dummy").unwrap().clone();
        println!("{:?}", files);
        let meta = files
            .move_files_to_flushing_fils()
            .await
            .generate_metadata(1)
            .await?;
        assert_eq!(meta.files.len(), 3, "test file len = {}", meta.files.len());
        assert!(
            meta.files.iter().all(|item| {
                TimeStamp::new(item.min_ts as _).physical() >= start_ts
                    && TimeStamp::new(item.max_ts as _).physical() <= end_ts
                    && item.min_ts <= item.max_ts
            }),
            "meta = {:#?}; start ts = {}, end ts = {}",
            meta.files,
            start_ts,
            end_ts
        );
        println!("{:#?}", meta);
        files.flush_log().await?;
        files.flush_meta(meta).await?;

        drop(router);
        let cmds = collect_recv(rx);
        assert_eq!(cmds.len(), 1, "test cmds len = {}", cmds.len());
        match &cmds[0] {
            Task::Flush(task) => assert_eq!(task, "dummy", "task = {}", task),
            _ => panic!("the cmd isn't flush!"),
        }

        let mut meta_count = 0;
        let mut log_count = 0;
        let mut a = tokio::fs::read_dir(storage_path).await?;
        while let Some(entry) = a.next_entry().await? {
            assert!(
                entry.path().is_file(),
                "log file {:?} is not a file",
                entry.path()
            );
            let filename = entry.file_name();
            if filename.to_str().unwrap().find("v1_backupmeta").is_some() {
                meta_count += 1;
            } else {
                log_count += 1;
                let f = entry.metadata().await?;
                assert!(
                    f.len() > 10,
                    "the log file {:?} is too small (size = {}B)",
                    filename,
                    f.len()
                );
            }
        }
        assert_eq!(meta_count, 1);
        assert_eq!(log_count, 3);
        Ok(())
    }
}
