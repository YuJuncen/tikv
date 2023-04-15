// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use std::{error::Error as StdError, io};

use aws_config::{
    default_provider::credentials::DefaultCredentialsChain,
    environment::EnvironmentVariableRegionProvider,
    meta::region::{self, ProvideRegion, RegionProviderChain},
    profile::ProfileFileRegionProvider,
    web_identity_token::WebIdentityTokenCredentialsProvider,
    ConfigLoader,
};
use aws_credential_types::provider::{error::CredentialsError, ProvideCredentials};
use aws_smithy_client::{bounds::SmithyConnector, hyper_ext, SdkError};
use aws_types::region::Region;
use cloud::metrics;
use futures::{executor::block_on, Future, TryFutureExt};
use tikv_util::{
    stream::{retry_ext, RetryError, RetryExt},
    warn,
};

#[allow(dead_code)] // This will be used soon, please remove the allow.
pub const READ_BUF_SIZE: usize = 1024 * 1024 * 2;
pub const AWS_WEB_IDENTITY_TOKEN_FILE: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";

const DEFAULT_REGION: &str = "us-east-1";

struct CredentialsErrorWrapper(CredentialsError);

impl From<CredentialsErrorWrapper> for CredentialsError {
    fn from(c: CredentialsErrorWrapper) -> CredentialsError {
        c.0
    }
}

impl std::fmt::Display for CredentialsErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)?;
        Ok(())
    }
}

impl RetryError for CredentialsErrorWrapper {
    fn is_retryable(&self) -> bool {
        true
    }
}

pub fn new_http_conn() -> impl SmithyConnector + 'static {
    hyper_ext::Builder::default()
        .hyper_builder({
            let mut builder = hyper::Client::builder();
            builder.http1_read_buf_exact_size(READ_BUF_SIZE);
            builder
        })
        .build(aws_smithy_client::conns::https())
}

pub fn new_credentials_provider() -> DefaultCredentialsProvider {
    block_on(DefaultCredentialsProvider::new())
}

pub fn is_retryable<T>(error: &SdkError<T>) -> bool {
    match error {
        SdkError::TimeoutError(_) => true,
        SdkError::DispatchFailure(_) => true,
        SdkError::ResponseError(resp_err) => {
            let code = resp_err.raw().http().status();
            code.is_server_error() || code == http::StatusCode::REQUEST_TIMEOUT
        }
        _ => false,
    }
}

pub fn configure_endpoint(loader: ConfigLoader, endpoint: &str) -> ConfigLoader {
    if !endpoint.is_empty() {
        loader.endpoint_url(endpoint)
    } else {
        loader
    }
}

pub fn configure_region(
    loader: ConfigLoader,
    region: &str,
    custom: bool,
) -> io::Result<ConfigLoader> {
    if !region.is_empty() {
        validate_region(region, custom)?;
        Ok(loader.region(Region::new(region.to_owned())))
    } else {
        Ok(loader.region(DefaultRegionProvider::new()))
    }
}

fn validate_region(region: &str, custom: bool) -> io::Result<()> {
    if custom {
        return Ok(());
    }
    let v: &str = &region.to_lowercase();

    match v {
        "ap-east-1" | "apeast1" | "ap-northeast-1" | "apnortheast1" | "ap-northeast-2"
        | "apnortheast2" | "ap-northeast-3" | "apnortheast3" | "ap-south-1" | "apsouth1"
        | "ap-southeast-1" | "apsoutheast1" | "ap-southeast-2" | "apsoutheast2"
        | "ca-central-1" | "cacentral1" | "eu-central-1" | "eucentral1" | "eu-west-1"
        | "euwest1" | "eu-west-2" | "euwest2" | "eu-west-3" | "euwest3" | "eu-north-1"
        | "eunorth1" | "eu-south-1" | "eusouth1" | "me-south-1" | "mesouth1" | "us-east-1"
        | "useast1" | "sa-east-1" | "saeast1" | "us-east-2" | "useast2" | "us-west-1"
        | "uswest1" | "us-west-2" | "uswest2" | "us-gov-east-1" | "usgoveast1"
        | "us-gov-west-1" | "usgovwest1" | "cn-north-1" | "cnnorth1" | "cn-northwest-1"
        | "cnnorthwest1" | "af-south-1" | "afsouth1" => Ok(()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid aws region format {}", region),
        )),
    }
}

pub async fn retry_and_count<G, T, F, E>(action: G, name: &'static str) -> Result<T, E>
where
    G: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
    E: RetryError + std::fmt::Display,
{
    let id = uuid::Uuid::new_v4();
    retry_ext(
        action,
        RetryExt::default().with_fail_hook(move |err: &E| {
            warn!("aws request meet error."; "err" => %err, "retry?" => %err.is_retryable(), "context" => %name, "uuid" => %id);
            metrics::CLOUD_ERROR_VEC.with_label_values(&["aws", name]).inc();
        }),
    ).await
}

#[derive(Debug)]
struct DefaultRegionProvider(RegionProviderChain);

impl DefaultRegionProvider {
    fn new() -> Self {
        let env_provider = EnvironmentVariableRegionProvider::new();
        let profile_provider = ProfileFileRegionProvider::builder().build();

        // same as default region resolving in rusoto
        let chain = RegionProviderChain::first_try(env_provider)
            .or_else(profile_provider)
            .or_else(Region::new(DEFAULT_REGION));

        Self(chain)
    }
}

impl ProvideRegion for DefaultRegionProvider {
    fn region(&self) -> region::future::ProvideRegion<'_> {
        ProvideRegion::region(&self.0)
    }
}

#[derive(Debug)]
pub struct DefaultCredentialsProvider {
    web_identity_provider: WebIdentityTokenCredentialsProvider,
    default_provider: DefaultCredentialsChain,
}

impl DefaultCredentialsProvider {
    async fn new() -> Self {
        let default_provider = DefaultCredentialsChain::builder().build().await;
        let web_identity_provider = WebIdentityTokenCredentialsProvider::builder().build();
        Self {
            web_identity_provider,
            default_provider,
        }
    }
}

impl ProvideCredentials for DefaultCredentialsProvider {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(async move {
            // use web identity provider first for the kubernetes environment.
            let cred = if std::env::var(AWS_WEB_IDENTITY_TOKEN_FILE).is_ok() {
                // we need invoke assume_role in web identity provider
                // this API may failed sometimes.
                // according to AWS experience, it's better to retry it with 10 times
                // exponential backoff for every error, because we cannot
                // distinguish the error type.
                retry_and_count(
                    || {
                        #[cfg(test)]
                        fail::fail_point!("cred_err", |_| {
                            let cause: Box<dyn StdError + Send + Sync + 'static> =
                                String::from("injected error").into();
                            Box::pin(futures::future::err(CredentialsErrorWrapper(
                                CredentialsError::provider_error(cause),
                            )))
                                as std::pin::Pin<Box<dyn futures::Future<Output = _> + Send>>
                        });

                        let res = self
                            .web_identity_provider
                            .provide_credentials()
                            .map_err(|err| CredentialsErrorWrapper(err));

                        #[cfg(test)]
                        return Box::pin(res);
                        #[cfg(not(test))]
                        res
                    },
                    "get_cred_over_the_cloud",
                )
                .await
                .map_err(|err| err.0)
            } else {
                // Add exponential backoff for every error, because we cannot
                // distinguish the error type.
                retry_and_count(
                    || {
                        self.default_provider
                            .provide_credentials()
                            .map_err(|e| CredentialsErrorWrapper(e))
                    },
                    "get_cred_on_premise",
                )
                .await
                .map_err(|e| e.0)
            };

            cred.map_err(|e| {
                let msg = e
                    .source()
                    .map(|src_err| src_err.to_string())
                    .unwrap_or_else(|| e.to_string());
                let cause: Box<dyn StdError + Send + Sync + 'static> =
                    format_args!("Couldn't find AWS credentials in sources ({}).", msg)
                        .to_string()
                        .into();
                CredentialsError::provider_error(cause)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[cfg(feature = "failpoints")]
    #[tokio::test]
    async fn test_default_provider() {
        let default_provider = DefaultCredentialsProvider::new().await;
        std::env::set_var(AWS_WEB_IDENTITY_TOKEN_FILE, "tmp");
        // mock k8s env with web_identitiy_provider
        fail::cfg("cred_err", "return").unwrap();
        fail::cfg("retry_count", "return(1)").unwrap();
        let res = default_provider.provide_credentials().await;
        assert_eq!(res.is_err(), true);

        let err = res.unwrap_err();

        match err {
            CredentialsError::ProviderError(_) => {
                assert_eq!(
                    err.source().unwrap().to_string(),
                    "Couldn't find AWS credentials in sources (injected error)."
                )
            }
            err => panic!("unexpected error type: {}", err),
        }

        fail::remove("cred_err");
        fail::remove("retry_count");
        std::env::remove_var(AWS_WEB_IDENTITY_TOKEN_FILE);
    }
}
