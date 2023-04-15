// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::{error::Error as StdError, io, time::Duration};

use async_trait::async_trait;
use aws_credential_types::{provider::ProvideCredentials, Credentials};
use aws_sdk_s3::{
    operation::get_object::GetObjectError,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use aws_smithy_client::{bounds::SmithyConnector, SdkError};
use bytes::Bytes;
use cloud::{
    blob::{BlobConfig, BlobStorage, BucketConf, PutResource, StringNonEmpty},
    metrics::CLOUD_REQUEST_HISTOGRAM_VEC,
    none_to_empty,
};
use fail::fail_point;
use futures::{executor::block_on, FutureExt, Stream, TryStreamExt};
use futures_util::io::{AsyncRead, AsyncReadExt};
pub use kvproto::brpb::{Bucket as InputBucket, CloudDynamic, S3 as InputConfig};
use thiserror::Error;
use tikv_util::{
    debug,
    stream::{error_stream, RetryError},
    time::Instant,
};
use tokio::time::{sleep, timeout};

use crate::util::{self, retry_and_count};

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(900);
pub const STORAGE_VENDOR_NAME_AWS: &str = "aws";

#[derive(Clone)]
pub struct AccessKeyPair {
    pub access_key: StringNonEmpty,
    pub secret_access_key: StringNonEmpty,
}

impl std::fmt::Debug for AccessKeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessKeyPair")
            .field("access_key", &self.access_key)
            .field("secret_access_key", &"?")
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    bucket: BucketConf,
    sse: Option<StringNonEmpty>,
    acl: Option<StringNonEmpty>,
    access_key_pair: Option<AccessKeyPair>,
    force_path_style: bool,
    sse_kms_key_id: Option<StringNonEmpty>,
    storage_class: Option<StringNonEmpty>,
    multi_part_size: usize,
    object_lock_enabled: bool,
}

impl Config {
    #[cfg(test)]
    pub fn default(bucket: BucketConf) -> Self {
        Self {
            bucket,
            sse: None,
            acl: None,
            access_key_pair: None,
            force_path_style: false,
            sse_kms_key_id: None,
            storage_class: None,
            multi_part_size: MINIMUM_PART_SIZE,
            object_lock_enabled: false,
        }
    }

    pub fn from_cloud_dynamic(cloud_dynamic: &CloudDynamic) -> io::Result<Config> {
        let bucket = BucketConf::from_cloud_dynamic(cloud_dynamic)?;
        let attrs = &cloud_dynamic.attrs;
        let def = &String::new();
        let force_path_style_str = attrs.get("force_path_style").unwrap_or(def).clone();
        let force_path_style = force_path_style_str == "true" || force_path_style_str == "True";
        let access_key_opt = attrs.get("access_key");
        let access_key_pair = if let Some(access_key) = access_key_opt {
            let secret_access_key = attrs.get("secret_access_key").unwrap_or(def).clone();
            Some(AccessKeyPair {
                access_key: StringNonEmpty::required_field(access_key.clone(), "access_key")?,
                secret_access_key: StringNonEmpty::required_field(
                    secret_access_key,
                    "secret_access_key",
                )?,
            })
        } else {
            None
        };
        let storage_class = bucket.storage_class.clone();
        Ok(Config {
            bucket,
            storage_class,
            sse: StringNonEmpty::opt(attrs.get("sse").unwrap_or(def).clone()),
            acl: StringNonEmpty::opt(attrs.get("acl").unwrap_or(def).clone()),
            access_key_pair,
            force_path_style,
            sse_kms_key_id: StringNonEmpty::opt(attrs.get("sse_kms_key_id").unwrap_or(def).clone()),
            multi_part_size: MINIMUM_PART_SIZE,
            object_lock_enabled: false,
        })
    }

    pub fn from_input(input: InputConfig) -> io::Result<Config> {
        let storage_class = StringNonEmpty::opt(input.storage_class);
        let endpoint = StringNonEmpty::opt(input.endpoint);
        let config_bucket = BucketConf {
            endpoint,
            bucket: StringNonEmpty::required_field(input.bucket, "bucket")?,
            prefix: StringNonEmpty::opt(input.prefix),
            storage_class: storage_class.clone(),
            region: StringNonEmpty::opt(input.region),
        };
        let access_key_pair = match StringNonEmpty::opt(input.access_key) {
            None => None,
            Some(ak) => Some(AccessKeyPair {
                access_key: ak,
                secret_access_key: StringNonEmpty::required_field(
                    input.secret_access_key,
                    "secret_access_key",
                )?,
            }),
        };
        Ok(Config {
            storage_class,
            bucket: config_bucket,
            sse: StringNonEmpty::opt(input.sse),
            acl: StringNonEmpty::opt(input.acl),
            access_key_pair,
            force_path_style: input.force_path_style,
            sse_kms_key_id: StringNonEmpty::opt(input.sse_kms_key_id),
            multi_part_size: MINIMUM_PART_SIZE,
            object_lock_enabled: input.object_lock_enabled,
        })
    }
}

impl BlobConfig for Config {
    fn name(&self) -> &'static str {
        STORAGE_NAME
    }

    fn url(&self) -> io::Result<url::Url> {
        self.bucket.url("s3").map_err(|s| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("error creating bucket url: {}", s),
            )
        })
    }
}

pub struct S3CompletedPart {
    pub e_tag: Option<String>,
    pub part_number: i32,
}

#[derive(Clone)]
pub struct S3Storage {
    config: Config,
    client: Client,
}

impl S3Storage {
    pub fn from_input(input: InputConfig) -> io::Result<Self> {
        Self::new(Config::from_input(input)?)
    }

    pub fn from_cloud_dynamic(cloud_dynamic: &CloudDynamic) -> io::Result<Self> {
        Self::new(Config::from_cloud_dynamic(cloud_dynamic)?)
    }

    /// Create a new S3 storage for the given config.
    pub fn new(config: Config) -> io::Result<Self> {
        let conn = util::new_http_conn();
        Self::new_with_connector(config, conn)
    }

    fn new_with_connector<Conn>(config: Config, connector: Conn) -> io::Result<Self>
    where
        Conn: SmithyConnector + 'static,
    {
        // static credentials are used with minio
        if let Some(access_key_pair) = &config.access_key_pair {
            let creds = Credentials::from_keys(
                (*access_key_pair.access_key).to_owned(),
                (*access_key_pair.secret_access_key).to_owned(),
                None,
            );
            Self::new_with_creds_connector(config, connector, creds)
        } else {
            let creds = util::new_credentials_provider();
            Self::new_with_creds_connector(config, connector, creds)
        }
    }

    pub fn new_with_creds_connector<Creds, Conn>(
        config: Config,
        connector: Conn,
        credentials_provider: Creds,
    ) -> io::Result<Self>
    where
        Conn: SmithyConnector + 'static,
        Creds: ProvideCredentials + 'static,
    {
        block_on(Self::new_with_creds_connector_async(
            config,
            connector,
            credentials_provider,
        ))
    }

    async fn new_with_creds_connector_async<Creds, Conn>(
        config: Config,
        connector: Conn,
        credentials_provider: Creds,
    ) -> io::Result<Self>
    where
        Conn: SmithyConnector + 'static,
        Creds: ProvideCredentials + 'static,
    {
        let bucket_region = none_to_empty(config.bucket.region.clone());
        let bucket_endpoint = none_to_empty(config.bucket.endpoint.clone());

        let mut loader = aws_config::from_env().credentials_provider(credentials_provider);
        loader = util::configure_region(loader, &bucket_region, !bucket_endpoint.is_empty())?;
        loader = util::configure_endpoint(loader, &bucket_endpoint);
        loader = loader.http_connector(connector);

        let sdk_config = loader.load().await;

        let mut builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        builder.set_force_path_style(Some(config.force_path_style));
        let s3_config = builder.build();

        let client = Client::from_conf(s3_config);

        Ok(S3Storage { config, client })
    }

    pub fn set_multi_part_size(&mut self, mut size: usize) {
        if size < MINIMUM_PART_SIZE {
            // default multi_part_size is 5MB, S3 cannot allow a smaller size.
            size = MINIMUM_PART_SIZE
        }
        self.config.multi_part_size = size;
    }

    fn maybe_prefix_key(&self, key: &str) -> String {
        if let Some(prefix) = &self.config.bucket.prefix {
            return format!("{}/{}", *prefix, key);
        }
        key.to_owned()
    }

    fn get_range(&self, name: &str, range: Option<String>) -> cloud::blob::BlobStream<'_> {
        let key = self.maybe_prefix_key(name);
        let bucket = self.config.bucket.bucket.clone();
        debug!("read file from s3 storage"; "key" => %key);

        let async_read = self
            .client
            .get_object()
            .key(key.clone())
            .bucket((*bucket).clone())
            .set_range(range)
            .send()
            .map(move |fut| {
                let stream: Box<dyn Stream<Item = io::Result<Bytes>> + Unpin + Send> = match fut {
                    Ok(out) => Box::new(
                        out.body
                            .map_err(|err| io::Error::new(io::ErrorKind::Other, err)),
                    ),
                    Err(SdkError::ServiceError(service_err)) => match service_err.err() {
                        GetObjectError::NoSuchKey(_) => create_error_stream(
                            io::ErrorKind::NotFound,
                            format!("no key {} at bucket {}", key, *bucket),
                        ),
                        _ => create_error_stream(
                            io::ErrorKind::Other,
                            format!("failed to get object {:?}", service_err),
                        ),
                    },
                    Err(e) => create_error_stream(
                        io::ErrorKind::Other,
                        format!("failed to get object {}", e),
                    ),
                };
                stream
            })
            .flatten_stream()
            .into_async_read();

        Box::new(Box::pin(async_read))
    }
}

fn create_error_stream(
    kind: io::ErrorKind,
    msg: String,
) -> Box<dyn Stream<Item = io::Result<Bytes>> + Unpin + Send + Sync> {
    Box::new(error_stream(io::Error::new(kind, msg)))
}

/// A helper for uploading a large files to S3 storage.
///
/// Note: this uploader does not support uploading files larger than 19.5 GiB.
struct S3Uploader<'client> {
    client: &'client Client,

    bucket: String,
    key: String,
    acl: Option<StringNonEmpty>,
    server_side_encryption: Option<StringNonEmpty>,
    sse_kms_key_id: Option<StringNonEmpty>,
    storage_class: Option<StringNonEmpty>,
    multi_part_size: usize,
    object_lock_enabled: bool,

    upload_id: String,
    parts: Vec<S3CompletedPart>,
}

/// The errors a uploader can meet.
/// This was made for make the result of [S3Uploader::run] get [Send].
#[derive(Debug, Error)]
pub enum UploadError {
    #[error("io error {0}")]
    Io(#[from] io::Error),
    #[error("aws-sdk error: {msg}")]
    // Maybe make it a trait if needed?
    Sdk { msg: String, retryable: bool },
}

impl RetryError for UploadError {
    fn is_retryable(&self) -> bool {
        match self {
            UploadError::Io(_) => false,
            UploadError::Sdk { msg: _, retryable } => *retryable,
        }
    }
}

impl<T: 'static + StdError> From<SdkError<T>> for UploadError {
    fn from(err: SdkError<T>) -> Self {
        let msg = format!("{}", err);
        Self::Sdk {
            msg,
            retryable: util::is_retryable(&err),
        }
    }
}

/// try_read_exact tries to read exact length data as the buffer size.  
/// like [`std::io::Read::read_exact`], but won't return `UnexpectedEof` when
/// cannot read anything more from the `Read`. once returning a size less than
/// the buffer length, implies a EOF was meet, or nothing read.
async fn try_read_exact<R: AsyncRead + ?Sized + Unpin>(
    r: &mut R,
    buf: &mut [u8],
) -> io::Result<usize> {
    let mut size_read = 0;
    loop {
        let r = r.read(&mut buf[size_read..]).await?;
        if r == 0 {
            return Ok(size_read);
        }
        size_read += r;
        if size_read >= buf.len() {
            return Ok(size_read);
        }
    }
}

fn get_content_md5(object_lock_enabled: bool, content: &[u8]) -> Option<String> {
    object_lock_enabled.then(|| {
        let digest = md5::compute(content);
        base64::encode(digest.0)
    })
}

/// Specifies the minimum size to use multi-part upload.
/// AWS S3 requires each part to be at least 5 MiB.
const MINIMUM_PART_SIZE: usize = 5 * 1024 * 1024;

impl<'client> S3Uploader<'client> {
    /// Creates a new uploader with a given target location and upload
    /// configuration.
    fn new(client: &'client Client, config: &Config, key: String) -> Self {
        Self {
            client,
            key,
            bucket: config.bucket.bucket.to_string(),
            acl: config.acl.as_ref().cloned(),
            server_side_encryption: config.sse.as_ref().cloned(),
            sse_kms_key_id: config.sse_kms_key_id.as_ref().cloned(),
            storage_class: config.storage_class.as_ref().cloned(),
            multi_part_size: config.multi_part_size,
            object_lock_enabled: config.object_lock_enabled,
            upload_id: "".to_owned(),
            parts: Vec::new(),
        }
    }

    /// Executes the upload process.
    async fn run(
        mut self,
        reader: &mut (dyn AsyncRead + Unpin + Send),
        est_len: u64,
    ) -> Result<(), UploadError> {
        if est_len <= self.multi_part_size as u64 {
            // For short files, execute one put_object to upload the entire thing.
            let mut data = Vec::with_capacity(est_len as usize);
            reader.read_to_end(&mut data).await?;
            retry_and_count(|| self.upload(&data), "upload_small_file").await?;
            Ok(())
        } else {
            // Otherwise, use multipart upload to improve robustness.
            self.upload_id = retry_and_count(|| self.begin(), "begin_upload").await?;
            let upload_res = async {
                let mut buf = vec![0; self.multi_part_size];
                let mut part_number = 1;
                loop {
                    let data_size = try_read_exact(reader, &mut buf).await?;
                    if data_size == 0 {
                        break;
                    }
                    let part = retry_and_count(
                        || self.upload_part(part_number, &buf[..data_size]),
                        "upload_part",
                    )
                    .await?;
                    self.parts.push(part);
                    part_number += 1;
                }
                Ok(())
            }
            .await;

            if upload_res.is_ok() {
                retry_and_count(|| self.complete(), "complete_upload").await?;
            } else {
                let _ = retry_and_count(|| self.abort(), "abort_upload").await;
            }
            upload_res
        }
    }

    /// Starts a multipart upload process.
    async fn begin(&self) -> Result<String, UploadError> {
        let request = async {
            self.client
                .create_multipart_upload()
                .bucket(self.bucket.clone())
                .key(&self.key)
                .set_acl(self.acl.as_ref().map(|s| s.as_str().into()))
                .set_server_side_encryption(
                    self.server_side_encryption
                        .as_ref()
                        .map(|s| s.as_str().into()),
                )
                .set_ssekms_key_id(self.sse_kms_key_id.as_ref().map(|s| s.to_string()))
                .set_storage_class(self.storage_class.as_ref().map(|s| s.as_str().into()))
                .send()
                .await?
                .upload_id()
                .ok_or_else(|| UploadError::Sdk {
                    msg: "missing upload-id from create_multipart_upload()".to_owned(),
                    retryable: false,
                })
                .map(|s| s.into())
        };
        timeout(Self::get_timeout(), request)
            .await
            .map_err(|_| UploadError::Sdk {
                msg: "timeout after 15mins for begin in s3 storage".to_owned(),
                retryable: false,
            })?
    }

    /// Completes a multipart upload process, asking S3 to join all parts into a
    /// single file.
    async fn complete(&self) -> Result<(), UploadError> {
        let request = async {
            let aws_parts: Vec<_> = self
                .parts
                .iter()
                .map(|p| {
                    CompletedPart::builder()
                        .part_number(p.part_number)
                        .set_e_tag(p.e_tag.clone())
                        .build()
                })
                .collect();

            self.client
                .complete_multipart_upload()
                .bucket(self.bucket.clone())
                .key(&self.key)
                .upload_id(&self.upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(aws_parts))
                        .build(),
                )
                .send()
                .await?;
            Ok(())
        };
        timeout(Self::get_timeout(), request)
            .await
            .map_err(|_| UploadError::Sdk {
                msg: "timeout after 15mins for upload in s3 storage".to_owned(),
                retryable: false,
            })?
    }

    /// Aborts the multipart upload process, deletes all uploaded parts.
    async fn abort(&self) -> Result<(), UploadError> {
        let request = async {
            self.client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(&self.upload_id)
                .send()
                .await?;
            Ok(())
        };
        timeout(Self::get_timeout(), request)
            .await
            .map_err(|_| UploadError::Sdk {
                msg: "timeout after 15mins for upload in s3 storage".to_owned(),
                retryable: false,
            })?
    }

    /// Uploads a part of the file.
    ///
    /// The `part_number` must be between 1 to 10000.
    async fn upload_part(
        &self,
        part_number: i64,
        data: &[u8],
    ) -> Result<S3CompletedPart, UploadError> {
        let request = async {
            let result = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(&self.upload_id)
                .part_number(part_number as i32)
                .content_length(data.len() as i64)
                .set_content_md5(get_content_md5(self.object_lock_enabled, data))
                .body(data.to_vec().into())
                .send()
                .await?;
            Ok(S3CompletedPart {
                e_tag: result.e_tag().map(|t| t.into()),
                part_number: part_number as i32,
            })
        };
        timeout(Self::get_timeout(), async {
            let start = Instant::now();
            let result = request.await;
            CLOUD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["s3", "upload_part"])
                .observe(start.saturating_elapsed().as_secs_f64());
            result
        })
        .await
        .map_err(|_| UploadError::Sdk {
            msg: "timeout after 15mins for upload part in s3 storage".to_owned(),
            retryable: false,
        })?
    }

    /// Uploads a file atomically.
    ///
    /// This should be used only when the data is known to be short, and thus
    /// relatively cheap to retry the entire upload.
    async fn upload(&self, data: &[u8]) -> Result<(), UploadError> {
        let request = async {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&self.key)
                .set_acl(self.acl.as_ref().map(|s| s.as_str().into()))
                .set_ssekms_key_id(self.sse_kms_key_id.as_ref().map(|s| s.to_string()))
                .set_storage_class(self.storage_class.as_ref().map(|s| s.as_str().into()))
                .content_length(data.len() as i64)
                .body(data.to_vec().into())
                .set_server_side_encryption(
                    self.server_side_encryption
                        .as_ref()
                        .map(|s| s.as_str().into()),
                )
                .set_content_md5(get_content_md5(self.object_lock_enabled, data))
                .send()
                .await
                .map(|_| ())
                .map_err(|err| err.into())
        };
        timeout(Self::get_timeout(), async {
            #[cfg(feature = "failpoints")]
            let delay_duration = (|| {
                fail_point!("s3_sleep_injected", |t| {
                    let t = t.unwrap().parse::<u64>().unwrap();
                    Duration::from_millis(t)
                });
                Duration::from_millis(0)
            })();
            #[cfg(not(feature = "failpoints"))]
            let delay_duration = Duration::from_millis(0);

            if delay_duration > Duration::from_millis(0) {
                sleep(delay_duration).await;
            }

            fail_point!("s3_put_obj_err", |_| {
                Err(UploadError::Sdk {
                    msg: "failed to put object".to_owned(),
                    retryable: false,
                })
            });

            let start = Instant::now();

            let result = request.await;

            CLOUD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["s3", "put_object"])
                .observe(start.saturating_elapsed().as_secs_f64());
            result
        })
        .await
        .map_err(|_| UploadError::Sdk {
            msg: "timeout after 15mins for upload in s3 storage".to_owned(),
            retryable: false,
        })?
    }

    fn get_timeout() -> Duration {
        fail_point!("s3_timeout_injected", |t| -> Duration {
            let t = t.unwrap().parse::<u64>();
            Duration::from_millis(t.unwrap())
        });
        CONNECTION_TIMEOUT
    }
}

pub const STORAGE_NAME: &str = "s3";

#[async_trait]
impl BlobStorage for S3Storage {
    fn config(&self) -> Box<dyn BlobConfig> {
        Box::new(self.config.clone()) as Box<dyn BlobConfig>
    }

    async fn put(
        &self,
        name: &str,
        mut reader: PutResource,
        content_length: u64,
    ) -> io::Result<()> {
        let key = self.maybe_prefix_key(name);
        debug!("save file to s3 storage"; "key" => %key);

        let uploader = S3Uploader::new(&self.client, &self.config, key);
        let result = uploader.run(&mut reader, content_length).await;
        result.map_err(|e| {
            let error_code = if let UploadError::Io(ref io_error) = e {
                io_error.kind()
            } else {
                io::ErrorKind::Other
            };
            // Even we can check whether there is an `io::Error` internal and extract it
            // directly, We still need to keep the message 'failed to put object' here for
            // adapting the string-matching based retry logic in BR :(
            io::Error::new(error_code, format!("failed to put object {}", e))
        })
    }

    fn get(&self, name: &str) -> cloud::blob::BlobStream<'_> {
        self.get_range(name, None)
    }

    fn get_part(&self, name: &str, off: u64, len: u64) -> cloud::blob::BlobStream<'_> {
        // inclusive, bytes=0-499 -> [0, 499]
        self.get_range(name, Some(format!("bytes={}-{}", off, off + len - 1)))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use aws_sdk_s3::{config::Credentials, primitives::SdkBody};
    use aws_smithy_client::test_connection::TestConnection;
    use http::Uri;

    use super::*;

    #[test]
    fn test_s3_get_content_md5() {
        // base64 encode md5sum "helloworld"
        let code = "helloworld".to_string();
        let expect = "/F4DjTilcDIIVEHn/nAQsA==".to_string();
        let actual = get_content_md5(true, code.as_bytes()).unwrap();
        assert_eq!(actual, expect);

        let actual = get_content_md5(false, b"xxx");
        assert!(actual.is_none())
    }

    #[test]
    fn test_s3_config() {
        let bucket_name = StringNonEmpty::required("mybucket".to_string()).unwrap();
        let mut bucket = BucketConf::default(bucket_name);
        bucket.region = StringNonEmpty::opt("ap-southeast-2".to_string());
        bucket.prefix = StringNonEmpty::opt("myprefix".to_string());
        let mut config = Config::default(bucket);
        config.access_key_pair = Some(AccessKeyPair {
            access_key: StringNonEmpty::required("abc".to_string()).unwrap(),
            secret_access_key: StringNonEmpty::required("xyz".to_string()).unwrap(),
        });
        let mut s = S3Storage::new(config.clone()).unwrap();
        // set a less than 5M value not work
        s.set_multi_part_size(1024);
        assert_eq!(s.config.multi_part_size, 5 * 1024 * 1024);
        // set 8M
        s.set_multi_part_size(8 * 1024 * 1024);
        assert_eq!(s.config.multi_part_size, 8 * 1024 * 1024);
        // set 6M
        s.set_multi_part_size(6 * 1024 * 1024);
        assert_eq!(s.config.multi_part_size, 6 * 1024 * 1024);
        // set a less than 5M value will fallback to 5M
        s.set_multi_part_size(1024);
        assert_eq!(s.config.multi_part_size, 5 * 1024 * 1024);

        config.bucket.region = StringNonEmpty::opt("foo".to_string());
        assert!(S3Storage::new(config).is_err());
    }

    #[tokio::test]
    async fn test_s3_storage_multi_part() {
        let magic_contents = "567890";

        let bucket_name = StringNonEmpty::required("mybucket".to_string()).unwrap();
        let mut bucket = BucketConf::default(bucket_name);
        bucket.region = Some(StringNonEmpty::required("cn-north-1".to_string()).unwrap());

        let mut config = Config::default(bucket);
        let multi_part_size = 2;
        // set multi_part_size to use upload_part function
        config.multi_part_size = multi_part_size;
        config.force_path_style = true;

        // split magic_contents into 3 parts, so we mock 5 requests here(1 begin + 3
        // part + 1 complete)
        let conn = TestConnection::new(vec![
            (
                http::Request::builder()
                    .uri(Uri::from_static(
                        "https://s3.cn-north-1.amazonaws.com.cn/mybucket/mykey?uploads&x-id=CreateMultipartUpload"
                    ))
                    .body(SdkBody::from(""))
                    .unwrap(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                            <InitiateMultipartUploadResult>
                                <Bucket>mybucket</Bucket>
                                <Key>mykey</Key>
                                <UploadId>1</UploadId>
                            </InitiateMultipartUploadResult>"#
                    )).unwrap()
            ),
            (
                http::Request::builder()
                    .uri(Uri::from_static(
                        "https://s3.cn-north-1.amazonaws.com.cn/mybucket/mykey?x-id=UploadPart&partNumber=1&uploadId=1"
                    ))
                    .body(SdkBody::from("56"))
                    .unwrap(),
                http::Response::builder().status(200).body(SdkBody::from("")).unwrap()
            ),
            (
                http::Request::builder()
                    .uri(Uri::from_static(
                        "https://s3.cn-north-1.amazonaws.com.cn/mybucket/mykey?x-id=UploadPart&partNumber=2&uploadId=1"
                    ))
                    .body(SdkBody::from("78"))
                    .unwrap(),
                http::Response::builder().status(200).body(SdkBody::from("")).unwrap()
            ),
            (
                http::Request::builder()
                    .uri(Uri::from_static(
                        "https://s3.cn-north-1.amazonaws.com.cn/mybucket/mykey?x-id=UploadPart&partNumber=3&uploadId=1"
                    ))
                    .body(SdkBody::from("90"))
                    .unwrap(),
                http::Response::builder().status(200).body(SdkBody::from("")).unwrap()
            ),
            (
                http::Request::builder()
                    .uri(Uri::from_static(
                        "https://s3.cn-north-1.amazonaws.com.cn/mybucket/mykey?x-id=CompleteMultipartUpload&uploadId=1"
                    ))
                    .body(SdkBody::from(
                        r#"<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><PartNumber>1</PartNumber></Part><Part><PartNumber>2</PartNumber></Part><Part><PartNumber>3</PartNumber></Part></CompleteMultipartUpload>"#
                    ))
                    .unwrap(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
                            <CompleteMultipartUploadResult>
                                <Location>https://s3.cn-north-1.amazonaws.com.cn/mybucket/mykey</Location>
                                <Bucket>mybucket</Bucket>
                                <Key>mykey</Key>
                                <Etag></ETag>
                            </CompleteMultipartUploadResult>
                            "#
                    )).unwrap()
            )
        ]);

        let creds = Credentials::from_keys("abc".to_string(), "xyz".to_string(), None);

        let s = S3Storage::new_with_creds_connector(config.clone(), conn.clone(), creds).unwrap();
        s.put(
            "mykey",
            PutResource(Box::new(magic_contents.as_bytes())),
            magic_contents.len() as u64,
        )
        .await
        .unwrap();

        conn.assert_requests_match(&[]);

        assert_eq!(
            CLOUD_REQUEST_HISTOGRAM_VEC
                .get_metric_with_label_values(&["s3", "upload_part"])
                .unwrap()
                .get_sample_count(),
            // length of magic_contents
            (magic_contents.len() / multi_part_size) as u64,
        );
    }

    #[cfg(feature = "failpoints")]
    #[tokio::test]
    async fn test_s3_storage() {
        let magic_contents = "5678";
        let bucket_name = StringNonEmpty::required("mybucket".to_string()).unwrap();
        let mut bucket = BucketConf::default(bucket_name);
        bucket.region = StringNonEmpty::opt("ap-southeast-2".to_string());
        bucket.prefix = StringNonEmpty::opt("myprefix".to_string());
        let mut config = Config::default(bucket);
        config.force_path_style = true;

        let conn = TestConnection::new(vec![
            (
                http::Request::builder()
                    .method("PUT")
                    .uri(Uri::from_static(
                        "https://s3.ap-southeast-2.amazonaws.com/mybucket/myprefix/mykey?x-id=PutObject",
                    ))
                    .body(SdkBody::from("5678"))
                    .unwrap(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(""))
                    .unwrap(),
            ),
            (
                http::Request::builder()
                    .method("GET")
                    .uri(Uri::from_static(
                        "https://s3.ap-southeast-2.amazonaws.com/mybucket/myprefix/mykey?x-id=GetObject",
                    ))
                    .body(SdkBody::from(""))
                    .unwrap(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from("5678"))
                    .unwrap(),
            ),
            (
                http::Request::builder()
                    .method("PUT")
                    .uri(Uri::from_static(
                        "https://s3.ap-southeast-2.amazonaws.com/mybucket/myprefix/mykey?x-id=PutObject",
                    ))
                    .body(SdkBody::from("5678"))
                    .unwrap(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(""))
                    .unwrap(),
            ),
        ]);

        let creds = Credentials::from_keys("abc".to_string(), "xyz".to_string(), None);

        let s = S3Storage::new_with_creds_connector(config.clone(), conn.clone(), creds).unwrap();
        s.put(
            "mykey",
            PutResource(Box::new(magic_contents.as_bytes())),
            magic_contents.len() as u64,
        )
        .await
        .unwrap();

        let mut reader = s.get("mykey");
        let mut buf = Vec::new();
        let ret = reader.read_to_end(&mut buf).await;
        assert!(ret.unwrap() == 4);
        assert!(!buf.is_empty());

        // inject put error
        let s3_put_obj_err_fp = "s3_put_obj_err";
        fail::cfg(s3_put_obj_err_fp, "return").unwrap();
        s.put(
            "mykey",
            PutResource(Box::new(magic_contents.as_bytes())),
            magic_contents.len() as u64,
        )
        .await
        .unwrap_err();

        fail::remove(s3_put_obj_err_fp);

        // test timeout
        let s3_timeout_injected_fp = "s3_timeout_injected";
        let s3_sleep_injected_fp = "s3_sleep_injected";

        // inject 100ms timeout
        fail::cfg(s3_timeout_injected_fp, "return(100)").unwrap();
        // inject 200ms delay
        fail::cfg(s3_sleep_injected_fp, "return(200)").unwrap();
        // timeout occur due to delay 200ms
        s.put(
            "mykey",
            PutResource(Box::new(magic_contents.as_bytes())),
            magic_contents.len() as u64,
        )
        .await
        .unwrap_err();
        fail::remove(s3_sleep_injected_fp);

        // inject 50ms delay
        fail::cfg(s3_sleep_injected_fp, "return(50)").unwrap();
        s.put(
            "mykey",
            PutResource(Box::new(magic_contents.as_bytes())),
            magic_contents.len() as u64,
        )
        .await
        .unwrap();
        fail::remove(s3_sleep_injected_fp);
        fail::remove(s3_timeout_injected_fp);

        conn.assert_requests_match(&[]);
    }

    #[tokio::test]
    async fn test_s3_storage_with_virtual_host() {
        let magic_contents = "abcd";
        let bucket_name = StringNonEmpty::required("bucket2".to_string()).unwrap();
        let mut bucket = BucketConf::default(bucket_name);
        bucket.region = StringNonEmpty::opt("ap-southeast-1".to_string());
        bucket.prefix = StringNonEmpty::opt("prefix2".to_string());
        let mut config = Config::default(bucket);
        config.force_path_style = false;

        let conn = TestConnection::new(vec![(
            http::Request::builder()
                .method("PUT")
                .uri(Uri::from_static(
                    "https://bucket2.s3.ap-southeast-1.amazonaws.com/prefix2/key2?x-id=PutObject",
                ))
                .body(SdkBody::from("abcd"))
                .unwrap(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(""))
                .unwrap(),
        )]);

        let creds = Credentials::from_keys("abc".to_string(), "xyz".to_string(), None);

        let s = S3Storage::new_with_creds_connector(config.clone(), conn.clone(), creds).unwrap();
        s.put(
            "key2",
            PutResource(Box::new(magic_contents.as_bytes())),
            magic_contents.len() as u64,
        )
        .await
        .unwrap();

        conn.assert_requests_match(&[]);
    }

    #[tokio::test]
    #[cfg(FALSE)]
    // FIXME: enable this (or move this to an integration test) if we've got a
    // reliable way to test s3 (rusoto_mock requires custom logic to verify the
    // body stream which itself can have bug)
    async fn test_real_s3_storage() {
        use tikv_util::time::Limiter;

        let bucket = BucketConf {
            endpoint: Some(StringNonEmpty::required("http://127.0.0.1:9000".to_owned()).unwrap()),
            bucket: StringNonEmpty::required("bucket".to_owned()).unwrap(),
            prefix: Some(StringNonEmpty::required("prefix".to_owned()).unwrap()),
            region: None,
            storage_class: None,
        };
        let s3 = Config {
            access_key_pair: Some(AccessKeyPair {
                access_key: StringNonEmpty::required("93QZ01QRBYQQXC37XHZV".to_owned()).unwrap(),
                secret_access_key: StringNonEmpty::required(
                    "N2VcI4Emg0Nm7fDzGBMJvguHHUxLGpjfwt2y4+vJ".to_owned(),
                )
                .unwrap(),
            }),
            force_path_style: true,
            ..Config::default(bucket)
        };

        let limiter = Limiter::new(f64::INFINITY);

        let storage = S3Storage::new(s3).unwrap();
        const LEN: usize = 1024 * 1024 * 4;
        static CONTENT: [u8; LEN] = [50_u8; LEN];
        storage
            .put(
                "huge_file",
                PutResource(Box::new(limiter.limit(&CONTENT[..]))),
                LEN as u64,
            )
            .await
            .unwrap();

        let mut reader = storage.get("huge_file");
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), LEN);
        assert_eq!(buf.iter().position(|b| *b != 50_u8), None);
    }

    #[test]
    fn test_url_of_backend() {
        let bucket_name = StringNonEmpty::required("bucket".to_owned()).unwrap();
        let mut bucket = BucketConf::default(bucket_name);
        bucket.prefix = Some(StringNonEmpty::static_str("/backup 01/prefix/"));
        let s3 = Config::default(bucket.clone());
        assert_eq!(
            s3.url().unwrap().to_string(),
            "s3://bucket/backup%2001/prefix/"
        );
        bucket.endpoint = Some(StringNonEmpty::static_str("http://endpoint.com"));
        let s3 = Config::default(bucket);
        assert_eq!(
            s3.url().unwrap().to_string(),
            "http://endpoint.com/bucket/backup%2001/prefix/"
        );
    }

    #[test]
    fn test_config_round_trip() {
        let mut input = InputConfig::default();
        input.set_bucket("bucket".to_owned());
        input.set_prefix("backup 02/prefix/".to_owned());
        input.set_region("us-west-2".to_owned());
        let c1 = Config::from_input(input.clone()).unwrap();
        let c2 = Config::from_cloud_dynamic(&cloud_dynamic_from_input(input)).unwrap();
        assert_eq!(c1.bucket.bucket, c2.bucket.bucket);
        assert_eq!(c1.bucket.prefix, c2.bucket.prefix);
        assert_eq!(c1.bucket.region, c2.bucket.region);
        assert_eq!(
            c1.bucket.region,
            StringNonEmpty::opt("us-west-2".to_owned())
        );
    }

    fn cloud_dynamic_from_input(mut s3: InputConfig) -> CloudDynamic {
        let mut bucket = InputBucket::default();
        if !s3.endpoint.is_empty() {
            bucket.endpoint = s3.take_endpoint();
        }
        if !s3.region.is_empty() {
            bucket.region = s3.take_region();
        }
        if !s3.prefix.is_empty() {
            bucket.prefix = s3.take_prefix();
        }
        if !s3.storage_class.is_empty() {
            bucket.storage_class = s3.take_storage_class();
        }
        if !s3.bucket.is_empty() {
            bucket.bucket = s3.take_bucket();
        }
        let mut attrs = std::collections::HashMap::new();
        if !s3.sse.is_empty() {
            attrs.insert("sse".to_owned(), s3.take_sse());
        }
        if !s3.acl.is_empty() {
            attrs.insert("acl".to_owned(), s3.take_acl());
        }
        if !s3.access_key.is_empty() {
            attrs.insert("access_key".to_owned(), s3.take_access_key());
        }
        if !s3.secret_access_key.is_empty() {
            attrs.insert("secret_access_key".to_owned(), s3.take_secret_access_key());
        }
        if !s3.sse_kms_key_id.is_empty() {
            attrs.insert("sse_kms_key_id".to_owned(), s3.take_sse_kms_key_id());
        }
        if s3.force_path_style {
            attrs.insert("force_path_style".to_owned(), "true".to_owned());
        }
        let mut cd = CloudDynamic::default();
        cd.set_provider_name("aws".to_owned());
        cd.set_attrs(attrs);
        cd.set_bucket(bucket);
        cd
    }

    #[tokio::test]
    async fn test_try_read_exact() {
        use std::io::{self, Cursor, Read};

        use futures::io::AllowStdIo;

        use self::try_read_exact;

        /// ThrottleRead throttles a `Read` -- make it emits 2 chars for each
        /// `read` call.
        struct ThrottleRead<R>(R);
        impl<R: Read> Read for ThrottleRead<R> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                let idx = buf.len().min(2);
                self.0.read(&mut buf[..idx])
            }
        }

        let mut data = AllowStdIo::new(ThrottleRead(Cursor::new(b"muthologia.")));
        let mut buf = vec![0u8; 6];
        assert_matches!(try_read_exact(&mut data, &mut buf).await, Ok(6));
        assert_eq!(buf, b"muthol");
        assert_matches!(try_read_exact(&mut data, &mut buf).await, Ok(5));
        assert_eq!(&buf[..5], b"ogia.");
        assert_matches!(try_read_exact(&mut data, &mut buf).await, Ok(0));
    }
}
