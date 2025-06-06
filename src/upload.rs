use std::{convert::Infallible, io::SeekFrom, path::Path, pin::Pin};

use graphql_client::GraphQLQuery;
use indicatif::ProgressBar;
use reqwest::header::{HeaderName, HeaderValue, InvalidHeaderValue};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use url::Url;

use crate::{
    checksum::Checksum,
    error::{self, Result},
    graphql_client::GraphQLClient,
    id::Id,
    progress_bar::{self, TempProgressStyle},
    s3,
};

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/create_multipart_upload.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
struct CreateMultipartUpload;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/complete_multipart_upload.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
struct CompleteMultipartUpload;

const MB: usize = 1024 * 1024;
const CHUNK_SIZE: usize = 10 * MB;
const MAX_RETRY_UPLOAD: usize = 3;

impl TryFrom<&Checksum> for HeaderName {
    type Error = Infallible;

    fn try_from(value: &Checksum) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            Checksum::Crc32(_) => HeaderName::from_static("x-amz-checksum-crc32"),
        })
    }
}

impl TryFrom<&Checksum> for HeaderValue {
    type Error = InvalidHeaderValue;

    fn try_from(value: &Checksum) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.to_be_base64())
    }
}

/// `FileRef` identifies a file by its path, an offset within the file, a length, and an ID.
///
/// It can either be:
///   - a complete file, with `id=0`, `offset=0`, `length=N` where `N` is the full length of the file,
///   - a chunk of `length=M` of a file, with `id` incrementally increasing along with `offset`
///     where `M` is determined by the chunking algorithm (nb: fixed at the time of writing).
///
/// Users of this API cannot assume a single `FileRef` is complete or not.
#[derive(Clone, Debug, PartialEq)]
struct FileRef<'a> {
    path: &'a Path,
    id: u64,
    offset: u64,
    length: usize,
}

impl<'a> FileRef<'a> {
    /// Logical chunking algorithm with fixed chunk size.
    /// Returns at least one chunk, even when the file is empty.
    async fn chunks(path: &'a Path, chunk_size: usize) -> std::io::Result<(Vec<Self>, usize)> {
        let total_length = tokio::fs::File::open(path)
            .await?
            .seek(SeekFrom::End(0))
            .await? as usize;

        let chunk_count = std::cmp::max(1, total_length.div_ceil(chunk_size));

        let mut chunks = Vec::with_capacity(chunk_count);
        let mut remaining = total_length;
        for index in 0..chunk_count {
            let chunk_length = std::cmp::min(chunk_size, remaining);
            remaining -= chunk_length;

            chunks.push(FileRef {
                path,
                id: index as u64,
                offset: (index * chunk_size) as u64,
                length: chunk_length,
            });
        }

        Ok((chunks, total_length))
    }

    /// Open the file in read-only mode, ready to be read at `offset` with exactly `length` bytes.
    /// Returns a file descriptor that you can drop, and read monotonously.
    async fn open(&self) -> std::io::Result<Pin<Box<dyn AsyncRead + Send + 'static>>> {
        let mut file = tokio::fs::File::open(self.path).await?;
        file.seek(SeekFrom::Start(self.offset)).await?;
        let part = file.take(self.length as u64);
        Ok(Box::pin(part))
    }
}

async fn retry_upload(
    client: &reqwest::Client,
    part: FileRef<'_>,
    upload_url: &Url,
    content_type: Option<&str>,
    pb: &ProgressBar,
) -> Result<s3::UploadResponse> {
    let cksum = Checksum::read_default_from(part.open().await?).await?;

    for retry in 0..MAX_RETRY_UPLOAD {
        if retry > 0 {
            pb.suspend(|| {
                tracing::warn!(
                    "Retrying upload... ({} of {} max)",
                    retry + 1,
                    MAX_RETRY_UPLOAD
                );
            });
        }

        // prepare request body
        let result = s3::upload(
            client,
            part.open().await?,
            upload_url,
            part.length,
            content_type,
            cksum.clone(),
            pb,
        )
        .await;

        match result {
            Ok(response) => return Ok(response),
            Err(error) if error.is_retryable() => {
                tracing::debug!(error = ?error, "Cannot upload part {}", part.id);
            }
            Err(error) => return Err(error.into()),
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
    file: FileRef<'_>,
    upload_url: &Url,
    content_type: Option<&str>,
    pb: &ProgressBar,
) -> Result<()> {
    let _ = retry_upload(client, file, upload_url, content_type, pb).await?;
    Ok(())
}

async fn upload_part(
    client: &reqwest::Client,
    part: FileRef<'_>,
    upload_url: Url,
    content_type: Option<&str>,
    pb: &ProgressBar,
) -> Result<String> {
    Ok(retry_upload(client, part, &upload_url, content_type, pb)
        .await?
        .e_tag)
}

async fn multipart_upload(
    client: &GraphQLClient,
    parts: Vec<FileRef<'_>>,
    id: &Id,
    content_type: Option<&str>,
    pb: &ProgressBar,
) -> Result<()> {
    let create_multipart_upload = client
        .send::<CreateMultipartUpload>(create_multipart_upload::Variables {
            id: id.to_node_id(),
            chunks: parts.iter().map(|x| x.id as i64).collect(),
        })
        .await?
        .create_project_version_file_multipart_upload;

    if create_multipart_upload.urls.len() != parts.len() {
        return Err(error::system(
            "Multipart upload preparation is invalid",
            "Please report this bug to our support",
        ));
    }

    let e_tags = futures::future::try_join_all(
        std::iter::zip(parts, create_multipart_upload.urls)
            .map(|(part, url)| upload_part(client.inner(), part.clone(), url, content_type, pb)),
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
    upload_url: &Url,
    pb: &ProgressBar,
) -> Result<()> {
    let (parts, total_length) = FileRef::chunks(path.as_ref(), CHUNK_SIZE).await?;

    let _guard = TempProgressStyle::new(pb);
    pb.reset();
    pb.set_style(progress_bar::pretty_bytes());
    pb.disable_steady_tick();
    pb.set_position(0);
    pb.set_length(total_length as u64);

    if parts.len() == 1 {
        let mut parts = parts;
        let file = parts.pop().expect("Vec is unexpectly empty");
        simple_upload(client.inner(), file, upload_url, content_type, pb).await
    } else {
        multipart_upload(client, parts, id, content_type, pb).await
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, path::Path};

    use rand::{thread_rng, RngCore};
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_chunks() {
        let mut file = NamedTempFile::new().unwrap();
        let mut buf = vec![0u8; 512 * 1024];
        for _ in 0..50 {
            thread_rng().fill_bytes(&mut buf[..]);
            file.write_all(&buf[..]).unwrap();
        }

        let path = file.into_temp_path();
        let (chunks, total_size) = super::FileRef::chunks(path.as_ref(), 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(total_size, 50 * 512 * 1024);
        assert_eq!(chunks.len(), 25);
        assert_eq!(chunks[0].id, 0);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[0].length, 1024 * 1024);
        assert_eq!(chunks[0].path, path.as_ref() as &Path);
    }
}
