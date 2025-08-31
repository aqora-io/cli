use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use pin_project_lite::pin_project;
use thiserror::Error;
use tokio::io::AsyncWrite;
use url::Url;

use crate::async_util::{MaybeLocalBoxFuture, MaybeLocalFutureExt, MaybeSend, MaybeSync};
use crate::error::{BoxError, DynError, Error};
use crate::s3::S3PutResponse;
use crate::Client;

#[cfg_attr(feature = "threaded", async_trait)]
#[cfg_attr(not(feature = "threaded"), async_trait(?Send))]
pub trait Multipart {
    type File;
    type Output;
    async fn create(&self, client: &Client) -> Result<Self::File, BoxError>;
    async fn create_part(
        &self,
        client: &Client,
        file: &Self::File,
        index: usize,
        size: usize,
    ) -> Result<Url, BoxError>;
    async fn complete(
        &self,
        client: &Client,
        file: &Self::File,
        etags: Vec<String>,
    ) -> Result<Self::Output, BoxError>;
}

#[cfg_attr(feature = "threaded", async_trait)]
#[cfg_attr(not(feature = "threaded"), async_trait(?Send))]
impl<T> Multipart for &T
where
    T: ?Sized + Multipart + MaybeSend + MaybeSync,
    T::File: MaybeSend + MaybeSync,
{
    type File = T::File;
    type Output = T::Output;
    async fn create(&self, client: &Client) -> Result<Self::File, BoxError> {
        T::create(self, client).await
    }
    async fn create_part(
        &self,
        client: &Client,
        file: &Self::File,
        index: usize,
        size: usize,
    ) -> Result<Url, BoxError> {
        T::create_part(self, client, file, index, size).await
    }
    async fn complete(
        &self,
        client: &Client,
        file: &Self::File,
        etags: Vec<String>,
    ) -> Result<Self::Output, BoxError> {
        T::complete(self, client, file, etags).await
    }
}

fn owned_create<'m, M>(
    multipart: Arc<M>,
    client: &Client,
) -> MaybeLocalBoxFuture<'m, Result<M::File, BoxError>>
where
    M: Multipart + MaybeSend + MaybeSync + 'm,
{
    let client = client.clone();
    async move { M::create(multipart.as_ref(), &client).await }.boxed_maybe_local()
}
fn owned_create_part<'m, M>(
    multipart: Arc<M>,
    client: &Client,
    file: Arc<M::File>,
    index: usize,
    size: usize,
) -> MaybeLocalBoxFuture<'m, Result<Url, BoxError>>
where
    M: Multipart + MaybeSend + MaybeSync + 'm,
    M::File: MaybeSend + MaybeSync,
{
    let client = client.clone();
    async move { M::create_part(multipart.as_ref(), &client, file.as_ref(), index, size).await }
        .boxed_maybe_local()
}
fn owned_complete<'m, M>(
    multipart: Arc<M>,
    client: &Client,
    file: Arc<M::File>,
    etags: Vec<String>,
) -> MaybeLocalBoxFuture<'m, Result<M::Output, BoxError>>
where
    M: Multipart + MaybeSend + MaybeSync + 'm,
    M::File: MaybeSend + MaybeSync,
{
    let client = client.clone();
    async move { M::complete(multipart.as_ref(), &client, file.as_ref(), etags).await }
        .boxed_maybe_local()
}

const MB: usize = 1024 * 1024;
pub const DEFAULT_PART_SIZE: usize = 10 * MB;
const MAX_PART_NUM: usize = 10_000;

#[derive(Error, Debug)]
#[error("Buffer size too small: {0} bytes, minimum is {1} bytes")]
pub struct BufferSizeTooSmall(usize, usize);

#[derive(Clone, Copy, Debug)]
pub struct BufferOptions {
    max_buffer_size: Option<usize>,
    part_size: usize,
}

impl BufferOptions {
    pub fn with_max_buffer_size(self, max_buffer_size: Option<usize>) -> Self {
        Self {
            max_buffer_size,
            ..self
        }
    }

    pub fn with_part_size(self, part_size: usize) -> Self {
        Self { part_size, ..self }
    }

    pub fn for_total_size(mut self, total_size: usize) -> Self {
        let min_part_size = total_size.div_ceil(MAX_PART_NUM);
        let part_size = if min_part_size > DEFAULT_PART_SIZE {
            min_part_size
        } else {
            DEFAULT_PART_SIZE
        };
        if let Some(max_buffer_size) = self.max_buffer_size {
            if part_size > max_buffer_size {
                self = self.with_max_buffer_size(Some(part_size))
            }
        }
        self.with_part_size(part_size)
    }

    pub fn for_concurrency(self, max_concurrency: usize) -> Self {
        self.with_max_buffer_size(Some(self.part_size * max_concurrency))
    }

    fn validate(&self) -> Result<(), BufferSizeTooSmall> {
        if let Some(buffer_size) = self.max_buffer_size {
            if buffer_size < self.part_size {
                return Err(BufferSizeTooSmall(buffer_size, self.part_size));
            }
        }
        Ok(())
    }
}

impl Default for BufferOptions {
    fn default() -> Self {
        Self {
            max_buffer_size: None,
            part_size: DEFAULT_PART_SIZE,
        }
    }
}

#[derive(Error, Debug, Clone)]
pub enum UploadError {
    #[error(transparent)]
    Multipart(Arc<DynError>),
    #[error(transparent)]
    Client(Arc<Error>),
}

#[cfg(feature = "threaded")]
impl From<UploadError> for io::Error {
    fn from(value: UploadError) -> Self {
        io::Error::other(value)
    }
}

#[cfg(not(feature = "threaded"))]
impl From<UploadError> for io::Error {
    fn from(value: UploadError) -> Self {
        io::Error::other(value.to_string())
    }
}

enum PartState {
    Creating(Bytes, MaybeLocalBoxFuture<'static, Result<Url, BoxError>>),
    Uploading(
        Bytes,
        MaybeLocalBoxFuture<'static, Result<S3PutResponse, Error>>,
    ),
    Error(UploadError),
    Finished(S3PutResponse),
}

impl fmt::Debug for PartState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartState::Creating(bytes, _) => f.debug_tuple("Creating").field(&bytes.len()).finish(),
            PartState::Uploading(bytes, _) => {
                f.debug_tuple("Uploading").field(&bytes.len()).finish()
            }
            PartState::Error(err) => f.debug_tuple("Error").field(err).finish(),
            PartState::Finished(res) => f.debug_tuple("Finished").field(&res.etag).finish(),
        }
    }
}

#[derive(Debug)]
struct UploadPart {
    client: Client,
    state: PartState,
}

impl UploadPart {
    pub fn new<M>(
        multipart: Arc<M>,
        client: &Client,
        file: Arc<M::File>,
        index: usize,
        body: Bytes,
    ) -> Self
    where
        M: Multipart + MaybeSend + MaybeSync + 'static,
        M::File: MaybeSend + MaybeSync,
    {
        let size = body.len();
        let state = PartState::Creating(
            body,
            owned_create_part(multipart, client, file, index, size),
        );
        Self {
            client: client.clone(),
            state,
        }
    }
}

impl UploadPart {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<String, UploadError>> {
        use PartState::*;
        let state = &mut self.state;
        loop {
            match state {
                Creating(bytes, fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(url) => {
                        let client = self.client.clone();
                        let body = bytes.clone();
                        *state = Uploading(
                            body.clone(),
                            async move { client.s3_put(url, body).await }.boxed_maybe_local(),
                        );
                    }
                    Err(err) => {
                        let err = UploadError::Multipart(err.into());
                        *state = Error(err.clone());
                        return Poll::Ready(Err(err));
                    }
                },
                Uploading(_, fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(res) => {
                        *state = Finished(res);
                    }
                    Err(err) => {
                        // We are using Arc because its safe in non-threaded as well but we could
                        // also in the future use async_util to imply a non-threaded context
                        #[allow(clippy::arc_with_non_send_sync)]
                        let err = UploadError::Client(Arc::new(err));
                        *state = Error(err.clone());
                        return Poll::Ready(Err(err));
                    }
                },
                Finished(res) => return Poll::Ready(Ok(res.etag.clone())),
                Error(err) => return Poll::Ready(Err(err.clone())),
            }
        }
    }

    fn buffer_size(&self) -> usize {
        use PartState::*;
        match &self.state {
            Creating(bytes, _) => bytes.len(),
            Uploading(bytes, _) => bytes.len(),
            _ => 0,
        }
    }
}

enum UploadState<M>
where
    M: Multipart,
{
    Creating(MaybeLocalBoxFuture<'static, Result<M::File, BoxError>>),
    Uploading {
        file: Arc<M::File>,
        parts: Vec<UploadPart>,
    },
    Completing(MaybeLocalBoxFuture<'static, Result<M::Output, BoxError>>),
    Error(UploadError),
    Finished(M::Output),
}

impl<M> fmt::Debug for UploadState<M>
where
    M: Multipart,
    M::File: fmt::Debug,
    M::Output: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UploadState::Creating(_) => write!(f, "Creating"),
            UploadState::Uploading { file, parts } => f
                .debug_struct("Uploading")
                .field("file", file)
                .field("parts", parts)
                .finish(),
            UploadState::Completing(_) => write!(f, "Completing"),
            UploadState::Error(err) => f.debug_tuple("Error").field(err).finish(),
            UploadState::Finished(output) => f.debug_tuple("Finished").field(output).finish(),
        }
    }
}

pin_project! {
pub struct MultipartUpload<M>
where
    M: Multipart,
{
    client: Client,
    multipart: Arc<M>,
    buffer: BytesMut,
    buffer_options: BufferOptions,
    state: UploadState<M>,
}
}

impl<M> fmt::Debug for MultipartUpload<M>
where
    M: Multipart + fmt::Debug,
    M::File: fmt::Debug,
    M::Output: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultipartUpload")
            .field("client", &self.client)
            .field("multipart", &self.multipart)
            .field("buffer_options", &self.buffer_options)
            .field("state", &self.state)
            .finish()
    }
}

impl<M> MultipartUpload<M>
where
    M: Multipart + MaybeSend + MaybeSync + 'static,
    M::File: MaybeSend + MaybeSync,
{
    pub fn new(client: Client, multipart: M) -> MultipartUpload<M> {
        let multipart = Arc::new(multipart);
        let state = UploadState::Creating(owned_create(multipart.clone(), &client));
        MultipartUpload {
            client,
            multipart,
            buffer: Default::default(),
            buffer_options: Default::default(),
            state,
        }
    }

    pub fn with_buffer_options(
        self,
        buffer_options: BufferOptions,
    ) -> Result<Self, BufferSizeTooSmall> {
        buffer_options.validate()?;
        Ok(Self {
            buffer_options,
            ..self
        })
    }
}

impl<M> MultipartUpload<M>
where
    M: Multipart + MaybeSend + MaybeSync + 'static,
    M::File: MaybeSend + MaybeSync,
{
    fn poll(&mut self, cx: &mut Context<'_>, complete: bool) -> Poll<Result<(), UploadError>> {
        use UploadState::*;
        let state = &mut self.state;
        loop {
            match state {
                Creating(fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(file) => {
                        *state = Uploading {
                            file: Arc::new(file),
                            parts: vec![],
                        }
                    }
                    Err(err) => {
                        let err = UploadError::Multipart(err.into());
                        *state = Error(err.clone());
                        return Poll::Ready(Err(err));
                    }
                },
                Uploading { file, parts } => {
                    let mut chunks = vec![];
                    while self.buffer.len() >= self.buffer_options.part_size {
                        chunks.push(self.buffer.split_to(self.buffer_options.part_size));
                    }
                    if complete && !self.buffer.is_empty() {
                        chunks.push(self.buffer.split_off(0));
                    }
                    for chunk in chunks {
                        parts.push(UploadPart::new(
                            self.multipart.clone(),
                            &self.client,
                            file.clone(),
                            parts.len(),
                            chunk.into(),
                        ));
                    }
                    let mut pending = false;
                    let mut etags = vec![];
                    for part in parts.iter_mut() {
                        match part.poll(cx) {
                            Poll::Pending => {
                                pending = true;
                            }
                            Poll::Ready(Ok(etag)) => {
                                etags.push(etag);
                            }
                            Poll::Ready(Err(err)) => {
                                *state = Error(err.clone());
                                return Poll::Ready(Err(err));
                            }
                        }
                    }
                    if pending {
                        return Poll::Pending;
                    } else if complete {
                        *state = Completing(owned_complete(
                            self.multipart.clone(),
                            &self.client,
                            file.clone(),
                            etags,
                        ));
                    } else {
                        return Poll::Ready(Ok(()));
                    }
                }
                Completing(fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(file) => {
                        *state = Finished(file);
                        return Poll::Ready(Ok(()));
                    }
                    Err(err) => {
                        let err = UploadError::Multipart(err.into());
                        *state = Error(err.clone());
                        return Poll::Ready(Err(err));
                    }
                },
                Finished(_) => {
                    return Poll::Ready(Ok(()));
                }
                Error(err) => {
                    return Poll::Ready(Err(err.clone()));
                }
            }
        }
    }

    fn buffer_size(&self) -> usize {
        match &self.state {
            UploadState::Uploading { parts, .. } => {
                parts.iter().map(|part| part.buffer_size()).sum::<usize>() + self.buffer.len()
            }
            _ => self.buffer.len(),
        }
    }

    fn remaining_capacity(&self) -> Option<usize> {
        self.buffer_options
            .max_buffer_size
            .map(|max_size| max_size.saturating_sub(self.buffer_size()))
    }

    fn write_buffer(&mut self, buf: &[u8]) -> usize {
        let write_len = if let Some(capacity) = self.remaining_capacity() {
            std::cmp::min(capacity, buf.len())
        } else {
            buf.len()
        };
        self.buffer.extend_from_slice(&buf[..write_len]);
        write_len
    }
}

impl<M> AsyncWrite for MultipartUpload<M>
where
    M: Multipart + MaybeSend + MaybeSync + 'static,
    M::File: MaybeSend + MaybeSync,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut total_written = self.write_buffer(buf);
        buf = &buf[total_written..];
        loop {
            match self.poll(cx, false) {
                Poll::Ready(Ok(_)) => {
                    let written = self.write_buffer(buf);
                    total_written += written;
                    if written == 0 {
                        return Poll::Ready(Ok(total_written));
                    } else {
                        buf = &buf[written..];
                    }
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err.into())),
                Poll::Pending => {
                    let written = self.write_buffer(buf);
                    total_written += written;
                    if written == 0 {
                        if total_written == 0 {
                            return Poll::Pending;
                        } else {
                            return Poll::Ready(Ok(total_written));
                        }
                    } else {
                        buf = &buf[written..];
                    }
                }
            }
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), io::Error>> {
        self.poll(cx, false).map_err(|err| err.into())
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), io::Error>> {
        self.poll(cx, true).map_err(|err| err.into())
    }
}

impl Client {
    pub fn multipart<M>(&self, multipart: M) -> MultipartUpload<M>
    where
        M: Multipart + MaybeSend + MaybeSync + 'static,
        M::File: MaybeSend + MaybeSync,
    {
        MultipartUpload::new(self.clone(), multipart)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

    async fn s3_client() -> aws_sdk_s3::Client {
        let sdk_config = aws_config::ConfigLoader::default()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .endpoint_url(std::env::var("AWS_ENDPOINT_URL").expect("AWS_ENDPOINT_URL must be set"))
            .region(aws_config::environment::EnvironmentVariableRegionProvider::default())
            .credentials_provider(
                aws_config::environment::EnvironmentVariableCredentialsProvider::default(),
            )
            .load()
            .await;
        let config = aws_sdk_s3::config::Config::from(&sdk_config)
            .to_builder()
            .force_path_style(true)
            .build();
        aws_sdk_s3::Client::from_conf(config)
    }

    #[derive(Clone)]
    struct AwsMultipart {
        client: Arc<aws_sdk_s3::Client>,
        bucket: String,
        key: String,
    }

    impl fmt::Debug for AwsMultipart {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("AwsMultipart")
                .field("bucket", &self.bucket)
                .field("key", &self.key)
                .finish()
        }
    }

    impl AwsMultipart {
        async fn new(bucket: impl Into<String>, key: impl Into<String>) -> Self {
            Self {
                client: Arc::new(s3_client().await),
                bucket: bucket.into(),
                key: key.into(),
            }
        }
    }

    #[cfg_attr(feature = "threaded", async_trait)]
    #[cfg_attr(not(feature = "threaded"), async_trait(?Send))]
    impl Multipart for AwsMultipart {
        type File = String;
        type Output = ();
        async fn create(&self, _: &Client) -> Result<Self::File, BoxError> {
            Ok(self
                .client
                .create_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .send()
                .await?
                .upload_id
                .ok_or("No upload ID found")?)
        }
        async fn create_part(
            &self,
            _: &Client,
            upload_id: &String,
            num: usize,
            size: usize,
        ) -> Result<Url, BoxError> {
            Ok(self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(&self.key)
                .part_number(num as i32 + 1)
                .content_length(size as i64)
                .upload_id(upload_id)
                .presigned(
                    aws_sdk_s3::presigning::PresigningConfig::builder()
                        .expires_in(std::time::Duration::from_secs(30))
                        .build()
                        .unwrap(),
                )
                .await?
                .uri()
                .parse()?)
        }
        async fn complete(
            &self,
            _: &Client,
            upload_id: &String,
            etags: Vec<String>,
        ) -> Result<(), BoxError> {
            self.client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(upload_id)
                .multipart_upload(
                    aws_sdk_s3::types::CompletedMultipartUpload::builder()
                        .set_parts(Some(
                            etags
                                .into_iter()
                                .enumerate()
                                .map(|(index, etag)| {
                                    aws_sdk_s3::types::CompletedPart::builder()
                                        .e_tag(etag)
                                        .part_number(index as i32 + 1)
                                        .build()
                                })
                                .collect(),
                        ))
                        .build(),
                )
                .send()
                .await?;
            Ok(())
        }
    }

    async fn random(size: usize) -> impl AsyncRead + Sync + Send + Unpin {
        tokio::fs::File::open("/dev/urandom")
            .await
            .unwrap()
            .take(size as u64)
    }

    #[ignore]
    #[tokio::test]
    async fn test_s3_simple() {
        let client = Client::new("http://localhost:9090".parse().unwrap());
        let multipart = AwsMultipart::new("test", "multipart-simple").await;
        let mut multipart_upload = MultipartUpload::new(client, multipart);
        let mut input = random(100 * MB).await;
        tokio::io::copy(&mut input, &mut multipart_upload)
            .await
            .unwrap();
        multipart_upload.shutdown().await.unwrap();
    }

    #[ignore]
    #[cfg(feature = "crc32fast")]
    #[tokio::test]
    async fn test_s3_checksum() {
        use crate::checksum::{crc32fast::Crc32, S3ChecksumLayer};
        let mut client = Client::new("http://localhost:9090".parse().unwrap());
        client.s3_layer(S3ChecksumLayer::new(Crc32::new()));
        let multipart = AwsMultipart::new("test", "multipart-checksum").await;
        let mut multipart_upload = MultipartUpload::new(client, multipart);
        let mut input = random(100 * MB).await;
        tokio::io::copy(&mut input, &mut multipart_upload)
            .await
            .unwrap();
        multipart_upload.shutdown().await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn test_s3_buffered() {
        let client = Client::new("http://localhost:9090".parse().unwrap());
        let multipart = AwsMultipart::new("test", "multipart-buffered").await;
        let size = 100 * MB;
        let mut multipart_upload = MultipartUpload::new(client, multipart)
            .with_buffer_options(
                BufferOptions::default()
                    .for_total_size(size)
                    .for_concurrency(3),
            )
            .unwrap();
        let mut input = random(size).await;
        tokio::io::copy(&mut input, &mut multipart_upload)
            .await
            .unwrap();
        multipart_upload.shutdown().await.unwrap();
    }
}
