use std::path::Path;
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc,
};
use std::time::Duration;

use aqora_client::multipart::BufferOptions;
use aqora_client::{
    error::BoxError,
    middleware::{Middleware, MiddlewareError, Next},
    multipart::{Multipart, DEFAULT_PART_SIZE},
    retry::{BackoffBuilder, ExponentialBackoffBuilder, RetryMiddleware},
    Client, GraphQLQuery,
};
use bytes::Bytes;
use futures::stream::StreamExt;
use indicatif::ProgressBar;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

use crate::{
    error::Result,
    graphql_client::GraphQLClient,
    id::Id,
    progress_bar::{self, TempProgressStyle},
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
    query_path = "src/graphql/part_upload.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
struct PartUpload;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/complete_multipart_upload.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
struct CompleteMultipartUpload;

struct ProjectVersionFileMultipart {
    id: String,
}

impl ProjectVersionFileMultipart {
    fn new(id: &Id) -> Self {
        Self {
            id: id.to_node_id(),
        }
    }
}

#[async_trait::async_trait]
impl Multipart for ProjectVersionFileMultipart {
    type File = String;
    type Output = ();
    async fn create(&self, client: &Client) -> Result<Self::File, BoxError> {
        Ok(client
            .send::<CreateMultipartUpload>(create_multipart_upload::Variables {
                id: self.id.clone(),
            })
            .await?
            .create_project_version_file_multipart_upload
            .upload_id)
    }
    async fn create_part(
        &self,
        client: &Client,
        file: &Self::File,
        index: usize,
        size: usize,
    ) -> Result<Url, BoxError> {
        Ok(client
            .send::<PartUpload>(part_upload::Variables {
                id: self.id.clone(),
                upload_id: file.clone(),
                chunk: index as i64,
                chunk_len: size as i64,
            })
            .await?
            .upload_project_version_file_part)
    }
    async fn complete(
        &self,
        client: &Client,
        file: &Self::File,
        etags: Vec<String>,
    ) -> Result<Self::Output, BoxError> {
        let _ = client
            .send::<CompleteMultipartUpload>(complete_multipart_upload::Variables {
                id: self.id.clone(),
                upload_id: file.clone(),
                e_tags: etags,
            })
            .await?;
        Ok(())
    }
}

const DEFAULT_CHUNK_SIZE: usize = 64 * 1024; // 64 KiB

struct ByteChunks {
    bytes: Bytes,
    chunk_size: usize,
}

impl ByteChunks {
    fn new(bytes: &[u8], chunk_size: usize) -> Self {
        Self {
            bytes: Bytes::copy_from_slice(bytes),
            chunk_size,
        }
    }
}

impl Iterator for ByteChunks {
    type Item = Bytes;
    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes.is_empty() {
            return None;
        }
        let index = std::cmp::min(self.chunk_size, self.bytes.len());
        let next = self.bytes.split_to(index);
        Some(next)
    }
}

pub struct ChunkProgressMiddleware {
    chunk_size: usize,
    _temp_style: TempProgressStyle<'static>,
    pb: ProgressBar,
}

impl ChunkProgressMiddleware {
    pub fn new(total_size: usize, chunk_size: usize, pb: ProgressBar) -> Self {
        let _temp_style = TempProgressStyle::owned(pb.clone());
        pb.reset();
        pb.set_style(progress_bar::pretty_bytes());
        pb.disable_steady_tick();
        pb.set_position(0);
        pb.set_length(total_size as u64);
        Self {
            chunk_size,
            _temp_style,
            pb,
        }
    }
}

#[async_trait::async_trait]
impl Middleware for ChunkProgressMiddleware {
    async fn handle(
        &self,
        mut req: reqwest::Request,
        next: Next<'_>,
    ) -> Result<reqwest::Response, MiddlewareError> {
        let processed = Arc::new(AtomicUsize::new(0));
        if let Some(bytes) = req.body().and_then(|body| body.as_bytes()) {
            let pb = self.pb.clone();
            let processed = processed.clone();
            let chunks = ByteChunks::new(bytes, self.chunk_size);
            req.body_mut().replace(reqwest::Body::wrap_stream(
                futures::stream::iter(chunks)
                    .inspect(move |chunk| {
                        let len = chunk.len();
                        pb.inc(len as u64);
                        processed.fetch_add(len, AtomicOrdering::Relaxed);
                    })
                    .map(Result::<_, std::convert::Infallible>::Ok),
            ));
        }
        match next.handle(req).await?.error_for_status() {
            Ok(res) => Ok(res),
            Err(err) => {
                self.pb.dec(processed.load(AtomicOrdering::Relaxed) as u64);
                Err(MiddlewareError::Request(err))
            }
        }
    }
}

struct InspectedBackoff<T> {
    pb: ProgressBar,
    backoff: T,
}

impl<T> Iterator for InspectedBackoff<T>
where
    T: Iterator<Item = Duration>,
{
    type Item = Duration;
    fn next(&mut self) -> Option<Duration> {
        if let Some(next) = self.backoff.next() {
            self.pb.suspend(|| {
                tracing::warn!("An error occurred, retrying in {:?}", next);
            });
            Some(next)
        } else {
            None
        }
    }
}

struct InspectedBackoffBuilder<T> {
    pb: ProgressBar,
    builder: T,
}

impl<T> BackoffBuilder for InspectedBackoffBuilder<T>
where
    T: BackoffBuilder,
{
    type Iter = InspectedBackoff<T::Iter>;
    fn build(&self) -> Self::Iter {
        InspectedBackoff {
            pb: self.pb.clone(),
            backoff: self.builder.build(),
        }
    }
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
    let mut file = File::open(path).await?;
    let len = file.metadata().await?.len() as usize;
    let mut client = client.clone();
    client
        .s3_with(RetryMiddleware::new(InspectedBackoffBuilder {
            pb: pb.clone(),
            builder: ExponentialBackoffBuilder::default(),
        }))
        .s3_with(ChunkProgressMiddleware::new(
            len,
            DEFAULT_CHUNK_SIZE,
            pb.clone(),
        ));
    if len > DEFAULT_PART_SIZE {
        let mut multipart = client
            .multipart(ProjectVersionFileMultipart::new(id))
            .with_buffer_options(
                BufferOptions::default()
                    .for_total_size(len)
                    .for_concurrency(3),
            )
            .expect("Buffer options with positive concurrency should always be valid");
        tokio::io::copy(&mut file, &mut multipart).await?;
        multipart.shutdown().await?;
    } else {
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        client.s3_put(upload_url.clone(), bytes).await?;
    }
    Ok(())
}
