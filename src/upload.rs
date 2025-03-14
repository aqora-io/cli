use std::path::Path;

use futures::StreamExt as _;
use graphql_client::GraphQLQuery;
use indicatif::ProgressBar;
use reqwest::{
    header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE},
    Body, Response,
};
use serde::Deserialize;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncSeek, AsyncSeekExt},
};
use tokio_util::io::ReaderStream;
use url::Url;

use crate::{
    checksum::Checksum,
    error::{self, Result},
    graphql_client::GraphQLClient,
    id::Id,
    io_util::{AsyncTryClone, FilePart},
    progress_bar::{self, TempProgressStyle},
};

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/create_multipart_upload.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
struct CreateMultipartUpload;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/complete_multipart_upload.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
struct CompleteMultipartUpload;

const MB: usize = 1024 * 1024;
const CHUNK_SIZE: usize = 10 * MB;
const MAX_RETRY_UPLOAD: usize = 3;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
#[cfg_attr(test, derive(Eq, PartialEq))]
struct UploadError {
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
enum UploadErrorCode {
    BadDigest,
    InvalidArgument,
    InvalidDigest,
    InvalidSignature,
    SignatureDoesNotMatch,
    #[serde(untagged)]
    Unknown(String),
}

async fn do_upload(
    client: &reqwest::Client,
    mut body: impl AsyncRead + AsyncSeek + AsyncTryClone + Send + Unpin + 'static,
    upload_url: &Url,
    content_length: usize,
    content_type: Option<&str>,
    pb: &ProgressBar,
) -> Result<Response> {
    let checksum = Checksum::read_default_from(&mut body).await?;

    for retry in 0..MAX_RETRY_UPLOAD {
        if retry > 0 {
            pb.suspend(|| {
                tracing::error!("Retrying upload...");
            });
        }

        // prepare request body
        body.rewind().await?;
        let body = ReaderStream::new(body.try_clone().await?);
        let body = Body::wrap_stream(body.inspect({
            let pb = pb.clone();
            move |chunk| {
                if let Ok(chunk) = chunk.as_ref() {
                    pb.inc(chunk.len() as u64);
                }
            }
        }));

        // prepare request
        let mut request = client
            .put(upload_url.to_string())
            .header(AUTHORIZATION, "")
            .header(CONTENT_LENGTH, content_length)
            .header(checksum.header_name(), checksum.header_value());
        if let Some(content_type) = content_type {
            request = request.header(CONTENT_TYPE, content_type);
        }
        request = request.body(body);

        // send request
        let response = request.send().await?;
        if response.status().is_success() {
            return Ok(response);
        }

        // verify error
        let status = response.status();
        let error = response.text().await?;
        let error: UploadError = quick_xml::de::from_str(&error)
            .map_err(|_| error::system(&format!("Upload failed: {status}"), ""))?;
        match &error.code.value {
            UploadErrorCode::BadDigest
            | UploadErrorCode::InvalidArgument
            | UploadErrorCode::InvalidDigest
            | UploadErrorCode::InvalidSignature
            | UploadErrorCode::SignatureDoesNotMatch => {
                // retry immediately when checksum verification fails
            }
            UploadErrorCode::Unknown(code) => {
                // abort prematurely for any other failure
                return Err(error::system(
                    &format!(
                        "Upload failed: {status:?} {} request_id={} {}",
                        code, error.request_id, error.message
                    ),
                    "Please report this issue on our forums",
                ));
            }
        }
    }

    // abort upload if checksum verification fails too much
    Err(error::system(
        "Upload failed",
        "Please verify reliability of your network and/or disk",
    ))
}

async fn simple_upload(
    client: &reqwest::Client,
    file: File,
    upload_url: &Url,
    content_length: usize,
    content_type: Option<&str>,
    pb: &ProgressBar,
) -> Result<()> {
    let _guard = TempProgressStyle::new(pb);
    pb.reset();
    pb.set_style(progress_bar::pretty_bytes());
    pb.disable_steady_tick();
    pb.set_position(0);
    pb.set_length(content_length as u64);
    let _ = do_upload(client, file, upload_url, content_length, content_type, pb).await?;
    Ok(())
}

async fn upload_part(
    client: &reqwest::Client,
    path: impl AsRef<Path>,
    chunk_number: u64,
    content_length: usize,
    content_type: Option<&str>,
    upload_url: Url,
    pb: &ProgressBar,
) -> Result<String> {
    let file = tokio::fs::File::open(path).await?;
    let chunk = FilePart::slice(file, chunk_number * CHUNK_SIZE as u64, CHUNK_SIZE).await?;
    let response = do_upload(client, chunk, &upload_url, content_length, content_type, pb).await?;
    Ok(response
        .headers()
        .get("ETag")
        .ok_or_else(|| error::system("ETag header not found in response", ""))?
        .to_str()
        .map_err(|_| error::system("ETag header is not valid UTF-8", ""))?
        .to_string())
}

async fn multipart_upload(
    client: &GraphQLClient,
    path: impl AsRef<Path>,
    id: &Id,
    content_length: usize,
    content_type: Option<&str>,
    pb: &ProgressBar,
) -> Result<()> {
    let mut chunks = [CHUNK_SIZE].repeat(content_length / CHUNK_SIZE);
    if content_length % CHUNK_SIZE != 0 {
        chunks.push(content_length % CHUNK_SIZE);
    }
    let create_multipart_upload = client
        .send::<CreateMultipartUpload>(create_multipart_upload::Variables {
            id: id.to_node_id(),
            chunks: chunks.iter().map(|&x| x as i64).collect(),
        })
        .await?
        .create_project_version_file_multipart_upload;

    let _guard = TempProgressStyle::new(pb);
    pb.reset();
    pb.set_style(progress_bar::pretty_bytes());
    pb.disable_steady_tick();
    pb.set_position(0);
    pb.set_length(content_length as u64);

    let e_tags = futures::future::try_join_all(
        create_multipart_upload
            .urls
            .into_iter()
            .enumerate()
            .map(|(i, url)| {
                chunks
                    .get(i)
                    .ok_or_else(|| error::system("Chunk index out of bounds", ""))
                    .map(|content_length| (i, url, *content_length))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|(i, url, content_length)| {
                upload_part(
                    client.inner(),
                    path.as_ref(),
                    i as u64,
                    content_length,
                    content_type,
                    url,
                    pb,
                )
            }),
    )
    .await?;

    let _ = client
        .send::<CompleteMultipartUpload>(complete_multipart_upload::Variables {
            id: id.to_node_id(),
            upload_id: create_multipart_upload.upload_id,
            e_tags,
        })
        .await?;

    Ok(())
}

#[tracing::instrument(ret, err, skip(client, pb))]
pub async fn upload_project_version_file(
    client: &GraphQLClient,
    path: impl AsRef<Path> + std::fmt::Debug,
    id: &Id,
    content_type: Option<&str>,
    upload_url: Option<&Url>,
    pb: &ProgressBar,
) -> Result<()> {
    let file = tokio::fs::File::open(path.as_ref()).await?;
    let content_len = file.metadata().await?.len() as usize;
    if content_len < CHUNK_SIZE && upload_url.is_some() {
        simple_upload(
            client.inner(),
            file,
            upload_url.unwrap(),
            content_len,
            content_type,
            pb,
        )
        .await
    } else {
        multipart_upload(client, path, id, content_len, content_type, pb).await
    }
}

#[cfg(test)]
mod tests {
    use super::{UploadError, UploadErrorCode, UploadErrorCodeValue};

    #[test]
    fn test_de_upload_error_response() {
        assert_eq!(
            UploadError {
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
            UploadError {
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
