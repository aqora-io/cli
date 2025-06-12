use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use bytes::{Bytes, BytesMut};
use pin_project_lite::pin_project;
use thiserror::Error;
use tokio::io::AsyncWrite;
use url::Url;

use crate::async_util::{MaybeLocalBoxFuture, MaybeLocalFutureExt};
use crate::backoff::{
    sleep_next, Backoff, BackoffFactory, DefaultBackoffFactory, ExponentialBackoffFactory,
    SleepFuture, SystemClock,
};
use crate::checksum::{Checksum, ChecksumAlgorithm, DefaultChecksum};
use crate::error::{ArcError, BoxError};
use crate::{s3, Client};

pub trait Multipart {
    type File;
    type Output;
    fn create(&self, client: &Client)
        -> MaybeLocalBoxFuture<'static, Result<Self::File, BoxError>>;
    fn create_part(
        &self,
        client: &Client,
        file: &Self::File,
        index: usize,
        size: usize,
    ) -> MaybeLocalBoxFuture<'static, Result<Url, BoxError>>;
    fn retry_part(
        &self,
        client: &Client,
        file: &Self::File,
        index: usize,
        size: usize,
    ) -> MaybeLocalBoxFuture<'static, Result<Url, BoxError>> {
        self.create_part(client, file, index, size)
    }
    fn complete(
        &self,
        client: &Client,
        file: &Self::File,
        etags: Vec<String>,
    ) -> MaybeLocalBoxFuture<'static, Result<Self::Output, BoxError>>;
}

impl<T> Multipart for &T
where
    T: ?Sized + Multipart,
{
    type File = T::File;
    type Output = T::Output;
    fn create(
        &self,
        client: &Client,
    ) -> MaybeLocalBoxFuture<'static, Result<Self::File, BoxError>> {
        T::create(self, client)
    }
    fn create_part(
        &self,
        client: &Client,
        file: &Self::File,
        index: usize,
        size: usize,
    ) -> MaybeLocalBoxFuture<'static, Result<Url, BoxError>> {
        T::create_part(self, client, file, index, size)
    }
    fn retry_part(
        &self,
        client: &Client,
        file: &Self::File,
        index: usize,
        size: usize,
    ) -> MaybeLocalBoxFuture<'static, Result<Url, BoxError>> {
        T::retry_part(self, client, file, index, size)
    }
    fn complete(
        &self,
        client: &Client,
        file: &Self::File,
        etags: Vec<String>,
    ) -> MaybeLocalBoxFuture<'static, Result<Self::Output, BoxError>> {
        T::complete(self, client, file, etags)
    }
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
    Multipart(ArcError),
    #[error(transparent)]
    Transport(Arc<reqwest::Error>),
    #[error(transparent)]
    Sleep(Arc<io::Error>),
    #[error("Max retries exceeded")]
    MaxRetriesExceeded,
    #[error("Max duration exceeded")]
    MaxDurationExceeded,
    #[error("Bad ETag")]
    BadETag,
    #[error("Bytes consumed")]
    BytesConsumed,
}

#[cfg(feature = "threaded")]
impl From<UploadError> for io::Error {
    fn from(value: UploadError) -> Self {
        io::Error::new(io::ErrorKind::Other, value)
    }
}

#[cfg(not(feature = "threaded"))]
impl From<UploadError> for io::Error {
    fn from(value: UploadError) -> Self {
        io::Error::new(io::ErrorKind::Other, value.to_string())
    }
}

enum PartProgress {
    Creating(MaybeLocalBoxFuture<'static, Result<Url, BoxError>>),
    Uploading(MaybeLocalBoxFuture<'static, reqwest::Result<reqwest::Response>>),
    Error(UploadError),
    Finished(String),
}

impl fmt::Debug for PartProgress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartProgress::Creating(_) => write!(f, "Creating"),
            PartProgress::Uploading(_) => write!(f, "Uploading"),
            PartProgress::Error(err) => write!(f, "Error({})", err),
            PartProgress::Finished(etag) => write!(f, "Finished({})", etag),
        }
    }
}

struct PartState {
    client: reqwest::Client,
    bytes: Option<Bytes>,
    checksum: Vec<u8>,
    checksum_algo: ChecksumAlgorithm,
    progress: PartProgress,
}

impl fmt::Debug for PartState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartState")
            .field("bytes", &self.bytes.as_ref().map(|bytes| bytes.len()))
            .field("checksum", &self.checksum)
            .field("checksum_algo", &self.checksum_algo)
            .field("progress", &self.progress)
            .finish()
    }
}

impl PartState {
    pub fn new<M>(
        multipart: M,
        client: &Client,
        file: &M::File,
        index: usize,
        bytes: Bytes,
        checksum: impl Checksum,
    ) -> Self
    where
        M: Multipart,
    {
        let checksum_algo = checksum.algo();
        let checksum = checksum.digest(&bytes);
        let progress =
            PartProgress::Creating(multipart.create_part(client, file, index, bytes.len()));
        Self {
            client: client.inner().clone(),
            bytes: Some(bytes),
            checksum,
            checksum_algo,
            progress,
        }
    }

    pub fn retry<M>(&mut self, multipart: M, client: &Client, file: &M::File, index: usize)
    where
        M: Multipart,
    {
        self.progress = if let Some(bytes) = self.bytes.as_ref() {
            PartProgress::Creating(multipart.retry_part(client, file, index, bytes.len()))
        } else {
            PartProgress::Error(UploadError::BytesConsumed)
        }
    }
}

impl PartState {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<String, UploadError>> {
        use PartProgress::*;
        let progress = &mut self.progress;
        loop {
            match progress {
                Creating(fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(url) => {
                        if let Some(bytes) = self.bytes.as_ref() {
                            let client = self.client.clone();
                            let bytes = bytes.clone();
                            let checksum_algo = self.checksum_algo;
                            let checksum = self.checksum.clone();
                            *progress = Uploading(
                                async move {
                                    s3::upload_precalculated(
                                        &client,
                                        url,
                                        bytes,
                                        checksum_algo,
                                        &checksum,
                                        Default::default(),
                                    )
                                    .await
                                }
                                .boxed_maybe_local(),
                            );
                        } else {
                            let err = UploadError::BytesConsumed;
                            *progress = Error(err.clone());
                            return Poll::Ready(Err(err));
                        }
                    }
                    Err(err) => {
                        let err = UploadError::Multipart(err.into());
                        *progress = Error(err.clone());
                        return Poll::Ready(Err(err));
                    }
                },
                Uploading(fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(res) => {
                        match res
                            .headers()
                            .get(reqwest::header::ETAG)
                            .and_then(|etag| etag.to_str().ok())
                            .ok_or(UploadError::BadETag)
                        {
                            Ok(etag) => {
                                self.bytes = None;
                                *progress = Finished(etag.to_string());
                            }
                            Err(err) => {
                                *progress = Error(err.clone());
                                return Poll::Ready(Err(err));
                            }
                        }
                    }
                    Err(err) => {
                        let err = UploadError::Transport(Arc::new(err));
                        *progress = Error(err.clone());
                        return Poll::Ready(Err(err));
                    }
                },
                Finished(etag) => return Poll::Ready(Ok(etag.clone())),
                Error(err) => return Poll::Ready(Err(err.clone())),
            }
        }
    }
}

const DEFAULT_MAX_RETRIES: usize = 5;

#[derive(Debug)]
pub struct RetryOptions<B> {
    max_retries: Option<usize>,
    backoff: Arc<B>,
}

impl<B> Default for RetryOptions<B>
where
    B: Default,
{
    fn default() -> Self {
        Self {
            max_retries: Some(DEFAULT_MAX_RETRIES),
            backoff: Default::default(),
        }
    }
}

impl<B> Clone for RetryOptions<B> {
    fn clone(&self) -> Self {
        Self {
            max_retries: self.max_retries,
            backoff: self.backoff.clone(),
        }
    }
}

impl<B> RetryOptions<B> {
    pub fn with_max_retries(self, max_retries: Option<usize>) -> Self {
        Self {
            max_retries,
            ..self
        }
    }
    pub fn with_backoff<T>(self, backoff: T) -> RetryOptions<T> {
        RetryOptions {
            backoff: Arc::new(backoff),
            max_retries: self.max_retries,
        }
    }
}

#[derive(Debug)]
enum SleepState {
    Idle,
    Sleeping(Pin<Box<SleepFuture>>),
    Error(UploadError),
}

#[derive(Debug)]
struct RetryPartState<B> {
    retries: usize,
    backoff: B,
    state: SleepState,
    part: PartState,
}

impl<B> RetryPartState<B> {
    pub fn new<M>(
        backoff: B,
        multipart: M,
        client: &Client,
        file: &M::File,
        index: usize,
        bytes: Bytes,
        checksum: impl Checksum,
    ) -> Self
    where
        M: Multipart,
    {
        Self {
            retries: 0,
            backoff,
            state: SleepState::Idle,
            part: PartState::new(multipart, client, file, index, bytes, checksum),
        }
    }

    pub fn with_backoff<T>(self, backoff: T) -> RetryPartState<T> {
        RetryPartState {
            retries: self.retries,
            backoff,
            state: self.state,
            part: self.part,
        }
    }
}

impl<B> RetryPartState<B>
where
    B: Backoff,
{
    pub fn retry<M>(&mut self, multipart: M, client: &Client, file: &M::File, index: usize)
    where
        M: Multipart,
    {
        match sleep_next(&mut self.backoff) {
            Some(fut) => {
                self.state = SleepState::Sleeping(Box::pin(fut));
                self.part.retry(multipart, client, file, index);
            }
            None => {
                self.state = SleepState::Error(UploadError::MaxDurationExceeded);
            }
        }
        self.retries += 1;
    }

    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<String, (UploadError, usize)>> {
        use SleepState::*;
        let state = &mut self.state;
        match state {
            Idle => {}
            Sleeping(fut) => match ready!(fut.as_mut().poll(cx)) {
                Ok(_) => {
                    *state = Idle;
                }
                Err(err) => {
                    let err = UploadError::Sleep(Arc::new(err));
                    *state = Error(err.clone());
                    return Poll::Ready(Err((err, self.retries)));
                }
            },
            Error(err) => return Poll::Ready(Err((err.clone(), self.retries))),
        }
        Poll::Ready(ready!(self.part.poll(cx)).map_err(|err| (err, self.retries)))
    }
}

enum UploadProgress<M, B>
where
    B: BackoffFactory,
    M: Multipart,
{
    Creating(MaybeLocalBoxFuture<'static, Result<M::File, BoxError>>),
    Uploading {
        file: M::File,
        parts: Vec<RetryPartState<B::Backoff>>,
    },
    Completing(MaybeLocalBoxFuture<'static, Result<M::Output, BoxError>>),
    Error(UploadError),
    Finished(M::Output),
}

impl<M, B> fmt::Debug for UploadProgress<M, B>
where
    M: Multipart,
    M::File: fmt::Debug,
    B: BackoffFactory,
    B::Backoff: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UploadProgress::Creating(_) => write!(f, "Creating"),
            UploadProgress::Uploading { file, parts } => f
                .debug_struct("Uploading")
                .field("file", file)
                .field("parts", parts)
                .finish(),
            UploadProgress::Completing(_) => write!(f, "Completing"),
            UploadProgress::Error(err) => write!(f, "Error({})", err),
            UploadProgress::Finished(_) => write!(f, "Finished"),
        }
    }
}

pin_project! {
pub struct MultipartUpload<M, C, B>
where
    M: Multipart,
    C: Checksum,
    B: BackoffFactory,
{
    client: Client,
    multipart: Arc<M>,
    checksum: Arc<C>,
    buffer: BytesMut,
    buffer_options: BufferOptions,
    retry_options: RetryOptions<B>,
    progress: UploadProgress<M, B>,
}
}

impl<M, C, B> fmt::Debug for MultipartUpload<M, C, B>
where
    M: Multipart + fmt::Debug,
    C: Checksum + fmt::Debug,
    M::File: fmt::Debug,
    B: BackoffFactory + fmt::Debug,
    B::Backoff: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultipartUpload")
            .field("client", &self.client)
            .field("multipart", &self.multipart)
            .field("checksum", &self.checksum)
            .field("buffer_options", &self.buffer_options)
            .field("retry_options", &self.retry_options)
            .field("progress", &self.progress)
            .finish()
    }
}

impl<M> MultipartUpload<M, DefaultChecksum, DefaultBackoffFactory>
where
    M: Multipart,
{
    pub fn new(
        client: Client,
        multipart: M,
    ) -> MultipartUpload<M, crc32fast::Hasher, ExponentialBackoffFactory<SystemClock>> {
        let progress = UploadProgress::Creating(multipart.create(&client));
        MultipartUpload {
            client,
            multipart: Arc::new(multipart),
            checksum: Arc::new(Default::default()),
            buffer: Default::default(),
            buffer_options: Default::default(),
            retry_options: Default::default(),
            progress,
        }
    }
}

impl<M, C, B> MultipartUpload<M, C, B>
where
    M: Multipart,
    C: Checksum,
    B: BackoffFactory,
{
    pub fn with_checksum<T>(self, checksum: T) -> MultipartUpload<M, T, B>
    where
        T: Checksum,
    {
        MultipartUpload {
            client: self.client,
            multipart: self.multipart,
            checksum: Arc::new(checksum),
            buffer: self.buffer,
            buffer_options: self.buffer_options,
            retry_options: self.retry_options,
            progress: self.progress,
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

    pub fn with_retry_options<T>(self, retry_options: RetryOptions<T>) -> MultipartUpload<M, C, T>
    where
        T: BackoffFactory,
    {
        use UploadProgress::*;
        let progress = match self.progress {
            Creating(fut) => Creating(fut),
            Uploading { file, parts } => Uploading {
                file,
                parts: parts
                    .into_iter()
                    .map(|part| part.with_backoff(retry_options.backoff.create()))
                    .collect(),
            },
            Completing(fut) => Completing(fut),
            Error(err) => Error(err),
            Finished(file) => Finished(file),
        };
        MultipartUpload {
            client: self.client,
            multipart: self.multipart,
            checksum: self.checksum,
            buffer: self.buffer,
            buffer_options: self.buffer_options,
            retry_options,
            progress,
        }
    }
}

impl<M, C, B> MultipartUpload<M, C, B>
where
    M: Multipart,
    C: Checksum,
    B: BackoffFactory,
{
    fn poll(&mut self, cx: &mut Context<'_>, complete: bool) -> Poll<Result<(), UploadError>> {
        use UploadProgress::*;
        let progress = &mut self.progress;
        loop {
            match progress {
                Creating(fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(file) => {
                        *progress = Uploading {
                            file,
                            parts: vec![],
                        }
                    }
                    Err(err) => {
                        let err = UploadError::Multipart(err.into());
                        *progress = Error(err.clone());
                        return Poll::Ready(Err(err));
                    }
                },
                Uploading { file, parts } => {
                    while self.buffer.len() >= self.buffer_options.part_size {
                        let next = self.buffer.split_to(self.buffer_options.part_size);
                        parts.push(RetryPartState::new(
                            self.retry_options.backoff.create(),
                            self.multipart.as_ref(),
                            &self.client,
                            file,
                            parts.len(),
                            next.into(),
                            self.checksum.as_ref(),
                        ));
                    }
                    if complete && !self.buffer.is_empty() {
                        let next = self.buffer.split_off(0);
                        parts.push(RetryPartState::new(
                            self.retry_options.backoff.create(),
                            self.multipart.as_ref(),
                            &self.client,
                            file,
                            parts.len(),
                            next.into(),
                            self.checksum.as_ref(),
                        ));
                    }
                    let mut pending = false;
                    let mut etags = vec![];
                    for (index, part) in parts.iter_mut().enumerate() {
                        loop {
                            match part.poll(cx) {
                                Poll::Pending => {
                                    pending = true;
                                    break;
                                }
                                Poll::Ready(Ok(etag)) => {
                                    etags.push(etag);
                                    break;
                                }
                                Poll::Ready(Err((err, retries))) => {
                                    if self
                                        .retry_options
                                        .max_retries
                                        .is_none_or(|max_retries| retries < max_retries)
                                    {
                                        part.retry(
                                            self.multipart.as_ref(),
                                            &self.client,
                                            file,
                                            index + 1,
                                        )
                                    } else {
                                        *progress = Error(err.clone());
                                        return Poll::Ready(Err(err));
                                    }
                                }
                            }
                        }
                    }
                    if pending {
                        return Poll::Pending;
                    } else if complete {
                        *progress = Completing(self.multipart.complete(&self.client, file, etags));
                    } else {
                        return Poll::Ready(Ok(()));
                    }
                }
                Completing(fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(file) => {
                        *progress = Finished(file);
                        return Poll::Ready(Ok(()));
                    }
                    Err(err) => {
                        let err = UploadError::Multipart(err.into());
                        *progress = Error(err.clone());
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
        match &self.progress {
            UploadProgress::Uploading { parts, .. } => {
                parts
                    .iter()
                    .flat_map(|part| part.part.bytes.as_ref())
                    .map(|bytes| bytes.len())
                    .sum::<usize>()
                    + self.buffer.len()
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

impl<M, C, B> AsyncWrite for MultipartUpload<M, C, B>
where
    M: Multipart,
    C: Checksum,
    B: BackoffFactory,
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

#[cfg(test)]
mod test {
    use super::*;
    use futures::prelude::*;
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

    impl Multipart for AwsMultipart {
        type File = String;
        type Output = ();
        fn create(&self, _: &Client) -> MaybeLocalBoxFuture<'static, Result<Self::File, BoxError>> {
            let request = self
                .client
                .create_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key);
            async move {
                Ok(request
                    .send()
                    .await?
                    .upload_id
                    .ok_or("No upload ID found")?)
            }
            .boxed()
        }
        fn create_part(
            &self,
            _: &Client,
            upload_id: &String,
            num: usize,
            size: usize,
        ) -> MaybeLocalBoxFuture<'static, Result<Url, BoxError>> {
            let request = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(&self.key)
                .part_number(num as i32 + 1)
                .content_length(size as i64)
                .upload_id(upload_id);
            let presign_config = aws_sdk_s3::presigning::PresigningConfig::builder()
                .expires_in(std::time::Duration::from_secs(30))
                .build()
                .unwrap();
            async move { Ok(request.presigned(presign_config).await?.uri().parse()?) }.boxed()
        }
        fn complete(
            &self,
            _: &Client,
            upload_id: &String,
            etags: Vec<String>,
        ) -> MaybeLocalBoxFuture<'static, Result<(), BoxError>> {
            let request = self
                .client
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
                );
            async move {
                request.send().await?;
                Ok(())
            }
            .boxed()
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
