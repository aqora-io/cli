use std::pin::Pin;
use std::task::{ready, Context, Poll};

use arrow::record_batch::RecordBatch;
use futures::prelude::*;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use serde_arrow::{schema::SerdeArrowSchema, ArrayBuilder};

use crate::async_util::parquet_async::*;
use crate::error::{Error, Result};
use crate::infer;
use crate::schema::Schema;

pub trait RecordBatchStream {
    fn schema(&self) -> Schema;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>>;
}

impl<T> RecordBatchStream for parquet::arrow::async_reader::ParquetRecordBatchStream<T>
where
    T: parquet::arrow::async_reader::AsyncFileReader + Unpin + MaybeSend + 'static,
{
    fn schema(&self) -> Schema {
        self.schema().clone().into()
    }
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        match ready!(Stream::poll_next(self, cx)) {
            Some(res) => Poll::Ready(Some(res.map_err(|e| e.into()))),
            None => Poll::Ready(None),
        }
    }
}

#[pin_project]
pub struct WrappedRecordBatchStream<S> {
    #[pin]
    inner: S,
}

impl<S> WrappedRecordBatchStream<S>
where
    S: RecordBatchStream,
{
    pub fn schema(&self) -> Schema {
        self.inner.schema()
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S> Stream for WrappedRecordBatchStream<S>
where
    S: RecordBatchStream,
{
    type Item = Result<RecordBatch>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

#[pin_project]
pub struct DeserializedRecordBatchStream<S, T> {
    records: Vec<T>,
    #[pin]
    inner: S,
}

impl<S, T> DeserializedRecordBatchStream<S, T> {
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            records: Vec::new(),
        }
    }
}

impl<S, T> Stream for DeserializedRecordBatchStream<S, T>
where
    S: RecordBatchStream,
    T: serde::de::DeserializeOwned,
{
    type Item = Result<T>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let this = self.as_mut().project();
            if let Some(record) = this.records.pop() {
                return Poll::Ready(Some(Ok(record)));
            }
            match ready!(this.inner.poll_next(cx)) {
                Some(Ok(record)) => match serde_arrow::from_record_batch::<Vec<T>>(&record) {
                    Ok(mut records) => {
                        records.reverse();
                        *this.records = records;
                    }
                    Err(err) => return Poll::Ready(Some(Err(err.into()))),
                },
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => return Poll::Ready(None),
            }
        }
    }
}

pub trait RecordBatchStreamExt: RecordBatchStream {
    fn wrap(self) -> WrappedRecordBatchStream<Self>
    where
        Self: Sized,
    {
        WrappedRecordBatchStream { inner: self }
    }

    fn deserialize<T>(self) -> DeserializedRecordBatchStream<Self, T>
    where
        Self: Sized,
    {
        DeserializedRecordBatchStream::new(self)
    }
}

impl<T> RecordBatchStreamExt for T where T: RecordBatchStream {}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
#[cfg_attr(
    feature = "wasm",
    derive(ts_rs::TS),
    ts(rename = "ReadOptions", export)
)]
pub struct Options {
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional))]
    pub batch_size: Option<usize>,
}

#[pin_project]
pub struct ValueRecordBatchStream<S> {
    #[pin]
    stream: S,
    schema: Schema,
    builder: ArrayBuilder,
    options: Options,
    current_batch_size: usize,
    reader_done: bool,
}

pub type ValueRecordBatchBoxStream<'a, T> = ValueRecordBatchStream<BoxStream<'a, T>>;

impl<S> ValueRecordBatchStream<S> {
    fn new(stream: S, schema: Schema, builder: ArrayBuilder, options: Options) -> Self {
        Self {
            stream,
            schema,
            builder,
            options,
            current_batch_size: 0,
            reader_done: false,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    #[cfg(feature = "wasm")]
    pub(crate) fn map_inner<T, F>(self, f: F) -> ValueRecordBatchStream<futures::stream::Map<S, F>>
    where
        F: FnMut(S::Item) -> T,
        S: Stream + Sized,
    {
        ValueRecordBatchStream {
            stream: self.stream.map(f),
            schema: self.schema,
            builder: self.builder,
            options: self.options,
            current_batch_size: self.current_batch_size,
            reader_done: self.reader_done,
        }
    }
}

impl<S, T, E> Stream for ValueRecordBatchStream<S>
where
    S: Stream<Item = Result<T, E>>,
    Error: From<E>,
    T: Serialize,
{
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let this = self.as_mut().project();
            if *this.reader_done
                || this
                    .options
                    .batch_size
                    .is_some_and(|size| *this.current_batch_size >= size)
            {
                return Poll::Ready(match this.builder.to_record_batch() {
                    Ok(record_batch) => {
                        *this.current_batch_size = 0;
                        if record_batch.num_rows() > 0 {
                            Some(Ok(record_batch))
                        } else {
                            None
                        }
                    }
                    Err(err) => Some(Err(err.into())),
                });
            }
            match ready!(this.stream.poll_next(cx)) {
                Some(Ok(item)) => match this.builder.push(item) {
                    Ok(()) => {
                        *this.current_batch_size += 1;
                    }
                    Err(err) => return Poll::Ready(Some(Err(err.into()))),
                },
                Some(Err(err)) => return Poll::Ready(Some(Err(err.into()))),
                None => {
                    *this.reader_done = true;
                }
            }
        }
    }
}

impl<S, T, E> RecordBatchStream for ValueRecordBatchStream<S>
where
    S: Stream<Item = Result<T, E>>,
    Error: From<E>,
    T: Serialize,
{
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            let this = self.as_mut().project();
            if *this.reader_done
                || this
                    .options
                    .batch_size
                    .is_some_and(|size| *this.current_batch_size >= size)
            {
                return Poll::Ready(match this.builder.to_record_batch() {
                    Ok(record_batch) => {
                        *this.current_batch_size = 0;
                        if record_batch.num_rows() > 0 {
                            Some(Ok(record_batch))
                        } else {
                            None
                        }
                    }
                    Err(err) => Some(Err(err.into())),
                });
            }
            match ready!(this.stream.poll_next(cx)) {
                Some(Ok(item)) => match this.builder.push(item) {
                    Ok(()) => {
                        *this.current_batch_size += 1;
                    }
                    Err(err) => return Poll::Ready(Some(Err(err.into()))),
                },
                Some(Err(err)) => return Poll::Ready(Some(Err(err.into()))),
                None => {
                    *this.reader_done = true;
                }
            }
        }
    }
}

pub fn from_value_stream<S, T, E>(
    stream: S,
    schema: Schema,
    options: Options,
) -> Result<ValueRecordBatchStream<S>>
where
    S: Stream<Item = Result<T, E>>,
    Error: From<E>,
    T: Serialize,
{
    let builder = ArrayBuilder::new(SerdeArrowSchema::try_from(&schema)?)?;
    Ok(ValueRecordBatchStream::new(
        stream, schema, builder, options,
    ))
}

pub async fn from_inferred_value_stream<'s, S, T, E>(
    mut stream: S,
    infer_options: infer::Options,
    sample_size: Option<usize>,
    read_options: Options,
) -> Result<ValueRecordBatchStream<BoxStream<'s, Result<T, E>>>>
where
    S: Stream<Item = Result<T, E>> + Unpin + MaybeSend + 's,
    Error: From<E>,
    T: Serialize + MaybeSend + 's,
    E: 's,
{
    let samples = infer::take_samples(&mut stream, sample_size).await?;
    let schema = infer::from_samples(&samples, infer_options)?;
    from_value_stream(
        boxed_stream(
            futures::stream::iter(samples.into_iter().map(Result::<T, E>::Ok)).chain(stream),
        ),
        schema.clone(),
        read_options,
    )
}

pub trait ValueStream:
    TryStream<Ok: Serialize + MaybeSend, Error: Into<Error>> + Sized + Unpin + MaybeSend
{
    fn take_samples(
        &mut self,
        sample_size: Option<usize>,
    ) -> BoxFuture<'_, Result<Vec<Self::Ok>, Self::Error>> {
        boxed_fut(async move { infer::take_samples(self, sample_size).await })
    }

    fn infer_schema(
        &mut self,
        options: infer::Options,
        sample_size: Option<usize>,
    ) -> BoxFuture<'_, Result<Schema>> {
        boxed_fut(async move {
            let samples = self
                .take_samples(sample_size)
                .await
                .map_err(|err| err.into())?;
            Ok(infer::from_samples(&samples, options)?)
        })
    }

    fn into_record_batch_stream<'s>(
        self,
        schema: Schema,
        options: Options,
    ) -> Result<ValueRecordBatchStream<BoxStream<'s, Result<Self::Ok>>>>
    where
        Self: 's,
    {
        from_value_stream(
            boxed_stream(self.map_err(Self::Error::into).into_stream()),
            schema,
            options,
        )
    }

    fn into_inferred_record_batch_stream<'s>(
        self,
        infer_options: infer::Options,
        sample_size: Option<usize>,
        read_options: Options,
    ) -> BoxFuture<'s, Result<ValueRecordBatchBoxStream<'s, Result<Self::Ok>>>>
    where
        Self: 's,
    {
        boxed_fut(from_inferred_value_stream(
            self.map_err(Self::Error::into).into_stream(),
            infer_options,
            sample_size,
            read_options,
        ))
    }
}

impl<T> ValueStream for T where
    T: TryStream<Ok: Serialize + MaybeSend, Error: Into<Error>> + Sized + Unpin + MaybeSend
{
}
