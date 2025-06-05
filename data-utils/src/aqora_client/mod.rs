mod graphql;

use std::pin::Pin;
use std::task::{ready, Context, Poll};

use aqora_client::{Client, CredentialsProvider};
use bytes::{Bytes, BytesMut};
use futures::future::LocalBoxFuture;
use futures::prelude::*;
use parquet::arrow::async_writer::AsyncFileWriter;
use pin_project::pin_project;
use thiserror::Error;
use url::Url;

use graphql::WrappedClient;

use crate::write::AsyncPartitionWriter;

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

const MAX_PART_NUM: usize = 10_000;
const DEFAULT_PART_SIZE: usize = 10 * 1024 * 1024; // 10 MB

#[derive(Error, Debug)]
pub enum BufferSizeError {
    #[error("Part size too small: {0} bytes, minimum is {1} bytes")]
    PartSizeTooSmall(usize, usize),
    #[error("Buffer size too small: {0} bytes, minimum is {1} bytes")]
    BufferSizeTooSmall(usize, usize),
}

#[derive(Clone, Copy)]
pub struct BufferOptions {
    max_partition_size: Option<usize>,
    max_buffer_size: Option<usize>,
    part_size: usize,
}

impl BufferOptions {
    pub fn max_partition_size(self, max_partition_size: usize) -> Self {
        Self {
            max_partition_size: Some(max_partition_size),
            ..self
        }
    }

    pub fn max_buffer_size(self, max_buffer_size: usize) -> Self {
        Self {
            max_buffer_size: Some(max_buffer_size),
            ..self
        }
    }

    pub fn part_size(self, part_size: usize) -> Self {
        Self { part_size, ..self }
    }
}

impl Default for BufferOptions {
    fn default() -> Self {
        Self {
            max_partition_size: None,
            max_buffer_size: None,
            part_size: DEFAULT_PART_SIZE,
        }
    }
}

impl BufferOptions {
    fn validate(&self) -> Result<(), BufferSizeError> {
        if let Some(partition_size) = self.max_partition_size {
            let minimum_part_size = partition_size.div_ceil(MAX_PART_NUM);
            if self.part_size < minimum_part_size {
                return Err(BufferSizeError::PartSizeTooSmall(
                    self.part_size,
                    minimum_part_size,
                ));
            }
        }
        if let Some(buffer_size) = self.max_buffer_size {
            if buffer_size < self.part_size {
                return Err(BufferSizeError::BufferSizeTooSmall(
                    buffer_size,
                    self.part_size,
                ));
            }
        }
        Ok(())
    }
}

pub struct DatasetVersionWriter<C> {
    client: Client<C>,
    dataset_version_id: String,
    buffer_options: BufferOptions,
    partition: usize,
}

impl<C> DatasetVersionWriter<C> {
    pub fn new(
        client: Client<C>,
        dataset_version_id: String,
        buffer_options: BufferOptions,
    ) -> Result<Self, BufferSizeError> {
        buffer_options.validate()?;
        Ok(Self {
            client,
            dataset_version_id,
            partition: 0,
            buffer_options,
        })
    }
}

#[async_trait::async_trait(?Send)]
impl<C> AsyncPartitionWriter for DatasetVersionWriter<C>
where
    C: CredentialsProvider + Clone + Send + Sync + 'static,
{
    type Writer = DatasetVersionFilePartWriter<C>;
    async fn next_partition(&mut self) -> std::io::Result<Self::Writer> {
        self.partition += 1;
        Ok(DatasetVersionFilePartWriter::new(
            self.client.clone(),
            self.dataset_version_id.clone(),
            self.partition,
            self.buffer_options.part_size,
            self.buffer_options.max_buffer_size,
        ))
    }

    fn max_partition_size(&self) -> Option<usize> {
        self.buffer_options.max_partition_size
    }
}

enum PartState {
    Creating(Bytes, LocalBoxFuture<'static, aqora_client::Result<Url>>),
    Uploading(LocalBoxFuture<'static, reqwest::Result<reqwest::Response>>),
    Error(String),
    Finished(String),
}

impl PartState {
    fn poll<C>(
        &mut self,
        cx: &mut Context<'_>,
        client: &WrappedClient<C>,
    ) -> Poll<Result<&str, BoxedError>> {
        loop {
            match self {
                Self::Creating(bytes, fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(url) => {
                        *self = Self::Uploading(
                            client
                                .inner()
                                .put(url)
                                .body(bytes.clone())
                                .send()
                                .boxed_local(),
                        );
                    }
                    Err(err) => {
                        *self = Self::Error(err.to_string());
                        return Poll::Ready(Err(err.into()));
                    }
                },
                Self::Uploading(fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(res) => match res
                        .error_for_status()
                        .map_err(|err| Box::new(err) as BoxedError)
                        .and_then(|res| {
                            Ok(res
                                .headers()
                                .get(reqwest::header::ETAG)
                                .ok_or("No etag header")?
                                .to_str()?
                                .to_string())
                        }) {
                        Ok(etag) => {
                            *self = Self::Finished(etag);
                        }
                        Err(err) => {
                            *self = Self::Error(err.to_string());
                            return Poll::Ready(Err(err));
                        }
                    },
                    Err(err) => {
                        *self = Self::Error(err.to_string());
                        return Poll::Ready(Err(err.into()));
                    }
                },
                Self::Finished(etag) => return Poll::Ready(Ok(etag)),
                Self::Error(err) => return Poll::Ready(Err(err.as_str().into())),
            }
        }
    }
}

enum UploadState {
    Creating(LocalBoxFuture<'static, aqora_client::Result<String>>),
    Uploading {
        dataset_version_file_id: String,
        parts: Vec<PartState>,
    },
    Completing(LocalBoxFuture<'static, aqora_client::Result<String>>),
    Error(String),
    Finished(String),
}

impl UploadState {
    fn poll<C>(
        &mut self,
        cx: &mut Context<'_>,
        client: &WrappedClient<C>,
        buffer: &mut BytesMut,
        part_size: usize,
        complete: bool,
    ) -> Poll<Result<(), BoxedError>>
    where
        C: CredentialsProvider + Clone + Send + Sync + 'static,
    {
        loop {
            match self {
                Self::Creating(fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(dataset_version_file_id) => {
                        *self = Self::Uploading {
                            dataset_version_file_id,
                            parts: vec![],
                        }
                    }
                    Err(err) => {
                        *self = Self::Error(err.to_string());
                        return Poll::Ready(Err(err.into()));
                    }
                },
                Self::Uploading {
                    dataset_version_file_id,
                    parts,
                } => {
                    while buffer.len() >= part_size {
                        let next = buffer.split_to(part_size);
                        parts.push(PartState::Creating(
                            next.into(),
                            client.upload_dataset_version_file_part(
                                dataset_version_file_id.clone(),
                                parts.len() + 1,
                                part_size,
                            ),
                        ));
                    }
                    if complete && !buffer.is_empty() {
                        let next = buffer.split_off(0);
                        parts.push(PartState::Creating(
                            next.into(),
                            client.upload_dataset_version_file_part(
                                dataset_version_file_id.clone(),
                                parts.len() + 1,
                                part_size,
                            ),
                        ));
                    }
                    let mut pending = false;
                    let mut etags = vec![];
                    for part in parts {
                        match part.poll(cx, client) {
                            Poll::Pending => {
                                pending = true;
                            }
                            Poll::Ready(Ok(etag)) => etags.push(etag),
                            Poll::Ready(Err(err)) => {
                                *self = Self::Error(err.to_string());
                                return Poll::Ready(Err(err));
                            }
                        }
                    }
                    if pending {
                        return Poll::Pending;
                    } else if complete {
                        *self = Self::Completing(client.complete_dataset_version_file(
                            dataset_version_file_id.clone(),
                            etags.into_iter().map(|s| s.to_string()).collect(),
                        ));
                    } else {
                        return Poll::Ready(Ok(()));
                    }
                }
                Self::Completing(fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(dataset_version_file_id) => {
                        *self = Self::Finished(dataset_version_file_id);
                        return Poll::Ready(Ok(()));
                    }
                    Err(err) => {
                        *self = Self::Error(err.to_string());
                        return Poll::Ready(Err(err.into()));
                    }
                },
                Self::Finished(_) => {
                    return Poll::Ready(Ok(()));
                }
                Self::Error(err) => {
                    return Poll::Ready(Err(err.as_str().into()));
                }
            }
        }
    }
}

pub struct DatasetVersionFilePartWriter<C> {
    client: WrappedClient<C>,
    buffer: BytesMut,
    part_size: usize,
    max_buffer_size: Option<usize>,
    upload_state: UploadState,
}

impl<C> DatasetVersionFilePartWriter<C>
where
    C: CredentialsProvider + Clone + Send + Sync + 'static,
{
    pub fn new(
        client: Client<C>,
        dataset_version_id: String,
        partition: usize,
        part_size: usize,
        max_buffer_size: Option<usize>,
    ) -> Self {
        let client = WrappedClient::new(client);
        let upload_state = UploadState::Creating(
            client.create_dataset_version_file(dataset_version_id, partition),
        );
        let max_buffer_size =
            max_buffer_size.map(|buffer_size| std::cmp::max(buffer_size, part_size));
        let buffer = if let Some(buffer_size) = max_buffer_size {
            BytesMut::with_capacity(buffer_size)
        } else {
            BytesMut::new()
        };
        Self {
            client,
            part_size,
            max_buffer_size,
            buffer,
            upload_state,
        }
    }

    pub fn dataset_version_file_id(&self) -> Option<&str> {
        match &self.upload_state {
            UploadState::Uploading {
                dataset_version_file_id,
                ..
            }
            | UploadState::Finished(dataset_version_file_id) => Some(dataset_version_file_id),
            _ => None,
        }
    }

    fn has_capacity(&self) -> bool {
        if let Some(buffer_size) = self.max_buffer_size {
            self.buffer.len() < buffer_size
        } else {
            true
        }
    }
}

#[pin_project]
struct DatasetVersionFilePartWriterWriteFut<'a, C> {
    bytes: Bytes,
    writer: &'a mut DatasetVersionFilePartWriter<C>,
}

impl<C> Future for DatasetVersionFilePartWriterWriteFut<'_, C>
where
    C: CredentialsProvider + Clone + Send + Sync + 'static,
{
    type Output = parquet::errors::Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let mut written = this.bytes.is_empty();
        loop {
            if !written && this.writer.has_capacity() {
                this.writer.buffer.extend_from_slice(this.bytes);
                written = true
            };
            match this.writer.upload_state.poll(
                cx,
                &this.writer.client,
                &mut this.writer.buffer,
                this.writer.part_size,
                false,
            ) {
                Poll::Pending => {
                    if written {
                        return Poll::Ready(Ok(()));
                    } else if !this.writer.has_capacity() {
                        return Poll::Pending;
                    }
                }
                Poll::Ready(res) => {
                    return Poll::Ready(res.map_err(parquet::errors::ParquetError::External))
                }
            }
        }
    }
}

#[pin_project]
struct DatasetVersionFilePartWriterCompleteFut<'a, C> {
    writer: &'a mut DatasetVersionFilePartWriter<C>,
}

impl<C> Future for DatasetVersionFilePartWriterCompleteFut<'_, C>
where
    C: CredentialsProvider + Clone + Send + Sync + 'static,
{
    type Output = parquet::errors::Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        Poll::Ready(
            ready!(this.writer.upload_state.poll(
                cx,
                &this.writer.client,
                &mut this.writer.buffer,
                this.writer.part_size,
                true,
            ))
            .map_err(parquet::errors::ParquetError::External),
        )
    }
}

impl<C> AsyncFileWriter for DatasetVersionFilePartWriter<C>
where
    C: CredentialsProvider + Clone + Send + Sync + 'static,
{
    fn write(&mut self, bs: Bytes) -> LocalBoxFuture<'_, parquet::errors::Result<()>> {
        DatasetVersionFilePartWriterWriteFut {
            bytes: bs,
            writer: self,
        }
        .boxed_local()
    }
    fn complete(&mut self) -> LocalBoxFuture<'_, parquet::errors::Result<()>> {
        DatasetVersionFilePartWriterCompleteFut { writer: self }.boxed_local()
    }
}
