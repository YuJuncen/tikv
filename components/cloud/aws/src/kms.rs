// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Deref;

use async_trait::async_trait;
use aws_credential_types::provider::ProvideCredentials;
use aws_http::auth::CredentialsStageError;
use aws_sdk_kms::{
    operation::{decrypt::DecryptError, generate_data_key::GenerateDataKeyError},
    primitives::Blob,
    types::DataKeySpec,
    Client,
};
use aws_sig_auth::middleware::SigningStageError;
use aws_smithy_client::bounds::SmithyConnector;
use aws_smithy_http::result::SdkError;
use cloud::{
    error::{Error, KmsError, OtherError, Result},
    kms::{Config, CryptographyType, DataKeyPair, EncryptedKey, KeyId, KmsProvider, PlainKey},
};
use futures::executor::block_on;

use crate::util::{self, is_retryable};

const AWS_KMS_DATA_KEY_SPEC: DataKeySpec = DataKeySpec::Aes256;

pub const ENCRYPTION_VENDOR_NAME_AWS_KMS: &str = "AWS";

pub struct AwsKms {
    client: Client,
    current_key_id: KeyId,
    region: String,
    endpoint: String,
}

impl std::fmt::Debug for AwsKms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kms_client = KmsClientDebug {
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
        };
        f.debug_struct("AwsKms")
            .field("client", &kms_client)
            .field("current_key_id", &self.current_key_id)
            .finish()
    }
}

impl AwsKms {
    fn new_with_creds_connector<Creds, Conn>(
        config: Config,
        connector: Conn,
        credentials_provider: Creds,
    ) -> Result<AwsKms>
    where
        Conn: SmithyConnector + 'static,
        Creds: ProvideCredentials + 'static,
    {
        let mut loader = aws_config::from_env().credentials_provider(credentials_provider);
        loader = util::configure_region(
            loader,
            &config.location.region,
            !config.location.endpoint.is_empty(),
        )?;
        loader = util::configure_endpoint(loader, &config.location.endpoint);
        loader = loader.http_connector(connector);

        let sdk_config = block_on(loader.load());
        let client = Client::new(&sdk_config);

        Ok(AwsKms {
            client,
            current_key_id: config.key_id,
            region: config.location.region,
            endpoint: config.location.endpoint,
        })
    }

    pub fn new(config: Config) -> Result<AwsKms> {
        let conn = util::new_http_conn();
        let creds = util::new_credentials_provider();

        Self::new_with_creds_connector(config, conn, creds)
    }
}

#[async_trait]
impl KmsProvider for AwsKms {
    fn name(&self) -> &str {
        ENCRYPTION_VENDOR_NAME_AWS_KMS
    }

    // On decrypt failure, the rule is to return WrongMasterKey error in case it is
    // possible that a wrong master key has been used, or other error otherwise.
    async fn decrypt_data_key(&self, data_key: &EncryptedKey) -> Result<Vec<u8>> {
        self.client
            .decrypt()
            .ciphertext_blob(Blob::new(data_key.clone().into_inner()))
            .key_id(self.current_key_id.deref().clone())
            .send()
            .await
            .map_err(classify_decrypt_error)
            .map(|response| response.plaintext().unwrap().as_ref().to_vec())
    }

    async fn generate_data_key(&self) -> Result<DataKeyPair> {
        self.client
            .generate_data_key()
            .key_id(self.current_key_id.deref().clone())
            .key_spec(AWS_KMS_DATA_KEY_SPEC)
            .send()
            .await
            .map_err(classify_generate_data_key_error)
            .and_then(|response| {
                let ciphertext_key = response.ciphertext_blob().unwrap().as_ref().to_vec();
                let plaintext_key = response.plaintext().unwrap().as_ref().to_vec();
                Ok(DataKeyPair {
                    encrypted: EncryptedKey::new(ciphertext_key)?,
                    plaintext: PlainKey::new(plaintext_key, CryptographyType::AesGcm256)?,
                })
            })
    }
}

// aws-sdk errors Display implementation just gives the cause message and
// discards the type. This is really bad when the cause message is empty!
// Use Debug instead: this will show both
pub struct FixSdkErrorDisplay<E: std::fmt::Debug + std::error::Error + Send + Sync + 'static>(
    SdkError<E>,
);
impl<E: std::error::Error + Send + Sync + 'static> std::fmt::Debug for FixSdkErrorDisplay<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}
impl<E: std::error::Error + Send + Sync + 'static> std::fmt::Display for FixSdkErrorDisplay<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}
impl<E: std::error::Error + Send + Sync + 'static> std::error::Error for FixSdkErrorDisplay<E> {}

fn classify_generate_data_key_error(err: SdkError<GenerateDataKeyError>) -> Error {
    if let SdkError::ServiceError(service_err) = &err {
        match &service_err.err() {
            GenerateDataKeyError::NotFoundException(_) => Error::ApiNotFound(err.into()),
            GenerateDataKeyError::InvalidKeyUsageException(_) => {
                // Error::KmsError(KmsError::Other(err.into()))
             Error::KmsError(KmsError::Other(OtherError::new(is_retryable(&err), FixSdkErrorDisplay(err).into())))
            }
            GenerateDataKeyError::DependencyTimeoutException(_) => Error::ApiTimeout(err.into()),
            GenerateDataKeyError::KmsInternalException(_) => Error::ApiInternal(err.into()),
            _ => Error::KmsError(KmsError::Other(OtherError::new(is_retryable(&err), FixSdkErrorDisplay(err).into()))),
        }
    } else {
        classify_error(err)
    }
}

fn classify_decrypt_error(err: SdkError<DecryptError>) -> Error {
    if let SdkError::ServiceError(service_err) = &err {
        match &service_err.err() {
            DecryptError::IncorrectKeyException(_) | DecryptError::NotFoundException(_) => {
                Error::KmsError(KmsError::WrongMasterKey(err.into()))
            }
            DecryptError::DependencyTimeoutException(_) => Error::ApiTimeout(err.into()),
            DecryptError::KmsInternalException(_) => Error::ApiInternal(err.into()),
            _ => Error::KmsError(KmsError::Other(OtherError::new(is_retryable(&err), FixSdkErrorDisplay(err).into()))),
        }
    } else {
        classify_error(err)
    }
}

fn classify_error<E: std::error::Error + Send + Sync + 'static>(err: SdkError<E>) -> Error {
    let src = std::error::Error::source(&err).unwrap();
    match &err {
        SdkError::DispatchFailure(_) => Error::ApiTimeout(err.into()),
        SdkError::ConstructionFailure(_)
            if src.is::<SigningStageError>() || src.is::<CredentialsStageError>() =>
        {
            Error::ApiAuthentication(err.into())
        }
        _ => Error::KmsError(KmsError::Other(OtherError::new(is_retryable(&err), FixSdkErrorDisplay(err).into()))),
    }
}

struct KmsClientDebug {
    region: String,
    endpoint: String,
}

impl std::fmt::Debug for KmsClientDebug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KmsClient")
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_kms::config::Credentials;
    use aws_smithy_client::test_connection::TestConnection;
    use aws_smithy_http::body::SdkBody;
    use cloud::kms::Location;
    use http::Uri;

    use super::*;

    #[tokio::test]
    async fn test_aws_kms() {
        let magic_contents = b"5678" as &[u8];
        let key_contents = vec![1u8; 32];
        let config = Config {
            key_id: KeyId::new("test_key_id".to_string()).unwrap(),
            vendor: String::new(),
            location: Location {
                region: "cn-north-1".to_string(),
                endpoint: String::new(),
            },
            azure: None,
            gcp: None,
        };

        let resp = format!(
            "{{\"KeyId\": \"test_key_id\", \"Plaintext\": \"{}\", \"CiphertextBlob\": \"{}\" }}",
            base64::encode(key_contents.clone()),
            base64::encode(magic_contents)
        );

        let conn = TestConnection::new(vec![(
            http::Request::builder()
                .method("POST")
                .uri(Uri::from_static("https://kms.cn-north-1.amazonaws.com.cn/"))
                .body(SdkBody::from(
                    "{\"KeyId\":\"test_key_id\",\"KeySpec\":\"AES_256\"}",
                ))
                .unwrap(),
            http::Response::builder().status(200).body(resp).unwrap(),
        )]);

        let creds = Credentials::from_keys("abc", "xyz", None);

        let aws_kms =
            AwsKms::new_with_creds_connector(config.clone(), conn.clone(), creds.clone()).unwrap();

        let data_key = aws_kms.generate_data_key().await.unwrap();

        assert_eq!(
            data_key.encrypted,
            EncryptedKey::new(magic_contents.to_vec()).unwrap()
        );
        assert_eq!(*data_key.plaintext, key_contents);

        conn.assert_requests_match(&[]);

        let req = format!(
            "{{\"KeyId\":\"test_key_id\",\"CiphertextBlob\":\"{}\"}}",
            base64::encode(data_key.encrypted.clone().into_inner())
        );

        let resp = format!(
            "{{\"KeyId\": \"test_key_id\", \"Plaintext\": \"{}\", \"EncryptionAlgorithm\": \"SYMMETRIC_DEFAULT\" }}",
            base64::encode(key_contents.clone()),
        );

        let conn = TestConnection::new(vec![(
            http::Request::builder()
                .uri(Uri::from_static("https://kms.cn-north-1.amazonaws.com.cn/"))
                .body(SdkBody::from(req))
                .unwrap(),
            http::Response::builder().status(200).body(resp).unwrap(),
        )]);

        let aws_kms = AwsKms::new_with_creds_connector(config, conn.clone(), creds).unwrap();

        let plaintext = aws_kms.decrypt_data_key(&data_key.encrypted).await.unwrap();
        assert_eq!(plaintext, key_contents);

        conn.assert_requests_match(&[]);
    }

    #[tokio::test]
    async fn test_kms_wrong_key_id() {
        let config = Config {
            key_id: KeyId::new("test_key_id".to_string()).unwrap(),
            vendor: String::new(),
            location: Location {
                region: "cn-north-1".to_string(),
                endpoint: String::new(),
            },
            azure: None,
            gcp: None,
        };

        let enc_key = EncryptedKey::new(b"invalid".to_vec()).unwrap();

        let req = format!(
            "{{\"KeyId\":\"test_key_id\",\"CiphertextBlob\":\"{}\"}}",
            base64::encode(enc_key.clone().into_inner())
        );

        // IncorrectKeyException
        //
        // HTTP Status Code: 400
        // Json, see:
        // https://docs.aws.amazon.com/kms/latest/APIReference/API_Decrypt.html#API_Decrypt_Errors
        let conn = TestConnection::new(vec![(
            http::Request::builder()
                .uri(Uri::from_static("https://kms.cn-north-1.amazonaws.com.cn/"))
                .body(SdkBody::from(req))
                .unwrap(),
            http::Response::builder()
                .status(400)
                .body(
                    r#"{
                        "__type": "IncorrectKeyException",
                        "Message": "mock"
                    }"#,
                )
                .unwrap(),
        )]);

        let creds = Credentials::from_keys("abc", "xyz", None);

        let aws_kms = AwsKms::new_with_creds_connector(config, conn.clone(), creds).unwrap();
        let fut = aws_kms.decrypt_data_key(&enc_key);

        match fut.await {
            Err(Error::KmsError(KmsError::WrongMasterKey(_))) => (),
            other => panic!("{:?}", other),
        }

        conn.assert_requests_match(&[]);
    }

    #[tokio::test]
    #[cfg(FALSE)]
    // FIXME: enable this (or move this to an integration test)
    async fn test_aws_kms_localstack() {
        let config = Config {
            key_id: KeyId::new("cbf4ef24-982d-4fd3-a75b-b95aaec84860".to_string()).unwrap(),
            vendor: String::new(),
            location: Location {
                region: "us-east-1".to_string(),
                endpoint: "http://localhost:4566".to_string(),
            },
        };

        let creds =
            Credentials::from_keys("testUser".to_string(), "testAccessKey".to_string(), None);
        let aws_kms =
            AwsKms::new_with_creds_connector(config, util::new_http_conn(), creds).unwrap();

        let data_key = aws_kms.generate_data_key().await.unwrap();
        let plaintext = aws_kms.decrypt_data_key(&data_key.encrypted).await.unwrap();

        assert_eq!(plaintext, data_key.plaintext.into_inner());
    }
}
