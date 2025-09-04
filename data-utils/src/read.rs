use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use arrow::record_batch::RecordBatch;
use bytes::{Bytes, BytesMut};
use futures::prelude::*;
use parquet::arrow::async_reader::AsyncFileReader;
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

type MergeMapping<Idx> = Vec<(usize, Range<Idx>)>;

/// Merges overlapping ranges and returns:
/// 1. The merged, non-overlapping ranges
/// 2. A mapping of how the original ranges fit into the merged ranges,
///    expressed as (index into merged, relative subrange)
fn merge_ranges<Idx>(mut ranges: Vec<Range<Idx>>) -> (Vec<Range<Idx>>, Vec<MergeMapping<Idx>>)
where
    Idx: Ord + Clone + std::ops::Sub<Output = Idx>,
{
    let originals = ranges.clone();

    ranges.sort_by(|a, b| a.start.cmp(&b.start));

    let mut merged: Vec<Range<Idx>> = Vec::new();

    for range in ranges {
        if let Some(last) = merged.last_mut() {
            if range.start <= last.end {
                // Extend last merged range
                if range.end > last.end {
                    last.end = range.end.clone();
                }
            } else {
                merged.push(range.clone());
            }
        } else {
            merged.push(range.clone());
        }
    }

    // Build mapping: for each original range, project it into merged ranges
    let mut mapping: Vec<Vec<(usize, Range<Idx>)>> = Vec::new();

    for orig in originals {
        let mut subranges = Vec::new();
        for (i, merged_range) in merged.iter().enumerate() {
            if orig.end <= merged_range.start {
                break; // no more overlap possible
            }
            if orig.start >= merged_range.end {
                continue; // not yet overlapping
            }
            // Intersection in absolute terms
            let start_abs = orig.start.clone().max(merged_range.start.clone());
            let end_abs = orig.end.clone().min(merged_range.end.clone());

            if start_abs < end_abs {
                // Convert to relative range inside merged_range
                let start_rel = start_abs.clone() - merged_range.start.clone();
                let end_rel = end_abs.clone() - merged_range.start.clone();
                subranges.push((i, start_rel..end_rel));
            }
        }
        mapping.push(subranges);
    }

    (merged, mapping)
}

pub struct MergeRangesAsyncFileReader<T> {
    inner: T,
}

impl<T> AsyncFileReader for MergeRangesAsyncFileReader<T>
where
    T: AsyncFileReader,
{
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.inner.get_bytes(range)
    }
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a parquet::arrow::arrow_reader::ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<parquet::file::metadata::ParquetMetaData>>> {
        self.inner.get_metadata(options)
    }
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        boxed_fut(async move {
            let (merged, mapping) = merge_ranges(ranges);
            let merged_bytes = self.inner.get_byte_ranges(merged).await?;

            // Rebuild original slices
            let mut result = Vec::new();
            for submaps in mapping {
                let bytes = if submaps.len() == 1 {
                    let (i, rel) = submaps.into_iter().next().unwrap();
                    merged_bytes[i].slice(rel.start as usize..rel.end as usize)
                } else {
                    let mut bytes = BytesMut::with_capacity(
                        submaps
                            .iter()
                            .map(|(_, r)| (r.end - r.start) as usize)
                            .sum(),
                    );
                    bytes.extend(submaps.into_iter().map(|(i, rel)| {
                        merged_bytes[i].slice(rel.start as usize..rel.end as usize)
                    }));
                    bytes.freeze()
                };
                result.push(bytes);
            }

            Ok(result)
        })
    }
}

pub struct InspectAsyncFileReader<T, F> {
    inner: T,
    func: F,
}

impl<T, F> AsyncFileReader for InspectAsyncFileReader<T, F>
where
    T: AsyncFileReader,
    F: FnMut(Range<u64>) + MaybeSend,
{
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        (self.func)(range.clone());
        self.inner.get_bytes(range)
    }
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a parquet::arrow::arrow_reader::ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<parquet::file::metadata::ParquetMetaData>>> {
        self.inner.get_metadata(options)
    }
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        for range in ranges.iter() {
            (self.func)(range.clone());
        }
        self.inner.get_byte_ranges(ranges)
    }
}

pub trait AsyncFileReaderExt: AsyncFileReader {
    fn inspect<F>(self, func: F) -> InspectAsyncFileReader<Self, F>
    where
        Self: Sized,
        F: FnMut(Range<u64>),
    {
        InspectAsyncFileReader { inner: self, func }
    }

    fn merge_ranges(self) -> MergeRangesAsyncFileReader<Self>
    where
        Self: Sized,
    {
        MergeRangesAsyncFileReader { inner: self }
    }
}

impl<T> AsyncFileReaderExt for T where T: AsyncFileReader {}
