use std::pin::Pin;
use std::task::{ready, Context, Poll};

use arrow::record_batch::RecordBatch;
use futures::prelude::*;
use pin_project_lite::pin_project;
use serde::{Deserialize, Serialize};
use serde_arrow::{schema::SerdeArrowSchema, ArrayBuilder};

use crate::error::{Error, Result};
use crate::schema::Schema;

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
#[cfg_attr(
    feature = "wasm",
    derive(ts_rs::TS),
    ts(rename = "ReadOptions", export, export_to = "bindings.ts")
)]
pub struct Options {
    #[serde(default)]
    #[cfg_attr(feature = "wasm", ts(optional))]
    pub batch_size: Option<usize>,
}

pin_project! {
pub struct RecordBatchStream<S> {
    #[pin]
    stream: S,
    schema: Schema,
    builder: ArrayBuilder,
    options: Options,
    current_batch_size: usize,
    reader_done: bool,
}
}

impl<S> RecordBatchStream<S> {
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
    pub(crate) fn map_inner<T, F>(self, f: F) -> RecordBatchStream<futures::stream::Map<S, F>>
    where
        F: FnMut(S::Item) -> T,
        S: Stream + Sized,
    {
        RecordBatchStream {
            stream: self.stream.map(f),
            schema: self.schema,
            builder: self.builder,
            options: self.options,
            current_batch_size: self.current_batch_size,
            reader_done: self.reader_done,
        }
    }
}

impl<S, T, E> Stream for RecordBatchStream<S>
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

pub fn from_stream<S, T, E>(
    stream: S,
    schema: Schema,
    options: Options,
) -> Result<RecordBatchStream<S>>
where
    S: Stream<Item = Result<T, E>>,
    Error: From<E>,
    T: Serialize,
{
    Ok(RecordBatchStream::new(
        stream,
        schema.clone(),
        ArrayBuilder::new(SerdeArrowSchema::try_from(schema)?)?,
        options,
    ))
}
