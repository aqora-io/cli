use futures::TryStreamExt as _;
use indicatif::ProgressBar;
use reqwest::{
    header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE},
    Body, Response, StatusCode,
};
use serde::Deserialize;
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;
use url::Url;

use crate::{checksum::Checksum, error};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
#[cfg_attr(test, derive(Eq, PartialEq))]
struct UploadErrorResponse {
    code: UploadErrorCodeValue,
    message: String,
    request_id: String,
}

#[derive(Deserialize, Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
struct UploadErrorCodeValue {
    #[serde(rename = "$value")]
    value: UploadErrorCode,
}

#[derive(Deserialize, Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum UploadErrorCode {
    BadDigest,
    InvalidArgument,
    InvalidDigest,
    InvalidSignature,
    SignatureDoesNotMatch,
    #[serde(untagged)]
    #[allow(unused)]
    Unknown(String),
}

impl UploadErrorCode {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::BadDigest
                | Self::InvalidArgument
                | Self::InvalidDigest
                | Self::InvalidSignature
                | Self::SignatureDoesNotMatch
        )
    }
}

#[derive(thiserror::Error, Debug)]
pub enum UploadError {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    Xml(#[from] quick_xml::de::DeError),

    #[error("ETag header not found in response")]
    ETagNotFound,
    #[error("ETag header is not valid UTF-8: {0}")]
    ETagNotUnicode(#[from] reqwest::header::ToStrError),

    #[error("Upload failed {status} {code:?}: {message} (request_id={request_id})")]
    Response {
        status: StatusCode,
        code: UploadErrorCode,
        request_id: String,
        message: String,
    },
}

impl From<UploadError> for error::Error {
    fn from(value: UploadError) -> Self {
        error::system_with_internal(
            &value.to_string(),
            "Please check your network connection",
            value,
        )
    }
}

pub struct UploadResponse {
    pub e_tag: String,
}

impl TryFrom<Response> for UploadResponse {
    type Error = UploadError;

    fn try_from(value: Response) -> Result<Self, Self::Error> {
        let e_tag = value
            .headers()
            .get("ETag")
            .ok_or(UploadError::ETagNotFound)?
            .to_str()?
            .to_string();
        Ok(Self { e_tag })
    }
}

pub async fn upload(
    client: &reqwest::Client,
    body: impl AsyncRead + Send + 'static,
    upload_url: &Url,
    content_length: usize,
    content_type: Option<&str>,
    content_cksum: Checksum,
    pb: &ProgressBar,
) -> Result<UploadResponse, UploadError> {
    // prepare request
    let mut request = client
        .put(upload_url.to_string())
        .header(AUTHORIZATION, "")
        .header(CONTENT_LENGTH, content_length)
        .header(&content_cksum, &content_cksum);
    if let Some(content_type) = content_type {
        request = request.header(CONTENT_TYPE, content_type);
    }
    let pb = pb.clone();
    request = request.body(Body::wrap_stream(ReaderStream::new(body).inspect_ok(
        move |data| {
            pb.inc(data.len() as u64);
        },
    )));

    // send request
    let response = request.send().await?;
    if response.status().is_success() {
        return response.try_into();
    }

    // verify error
    let status = response.status();
    let error = response.text().await?;
    let error: UploadErrorResponse = quick_xml::de::from_str(&error)?;
    Err(UploadError::Response {
        status,
        code: error.code.value,
        request_id: error.request_id,
        message: error.message,
    })
}

#[cfg(test)]
mod tests {
    use super::{UploadErrorCode, UploadErrorCodeValue, UploadErrorResponse};

    #[test]
    fn test_de_upload_error_response() {
        assert_eq!(
            UploadErrorResponse {
                code: UploadErrorCodeValue {
                    value: UploadErrorCode::Unknown("NoSuchKey".to_string())
                },
                message: "The resource you requested does not exist".to_string(),
                request_id: "4442587FB7D0A2F9".to_string(),
            },
            quick_xml::de::from_str(
                r#"
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The resource you requested does not exist</Message>
  <Resource>/mybucket/myfoto.jpg</Resource>
  <RequestId>4442587FB7D0A2F9</RequestId>
</Error>
        "#,
            )
            .unwrap(),
        );

        assert_eq!(
            UploadErrorResponse {
                code: UploadErrorCodeValue {
                    value: UploadErrorCode::BadDigest
                },
                message: "foobar".to_string(),
                request_id: "4 8 15 16 23 42".to_string(),
            },
            quick_xml::de::from_str(
                r#"
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>BadDigest</Code>
  <Message>foobar</Message>
  <Resource>buzz</Resource>
  <RequestId>4 8 15 16 23 42</RequestId>
</Error>
        "#,
            )
            .unwrap(),
        );
    }
}
