use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::stream::Stream;
use pin_project::pin_project;
use tokio::io::{self, AsyncRead, ReadBuf};

use std::pin::Pin;
use std::task::{ready, Context, Poll};

use crate::async_util::parquet_async::*;
use crate::value::Value;

pub(crate) type ProcessItemStream<'a, T = Value, E = io::Error> =
    BoxStream<'a, Result<ProcessItem<T>, E>>;

#[cfg(feature = "wasm")]
const INITIAL_CHUNK_SIZE: usize = 65_536;
#[cfg(not(feature = "wasm"))]
const INITIAL_CHUNK_SIZE: usize = 4096;

pub enum ByteProcessResult<T, E> {
    Ok((usize, usize, T)),
    Done(usize),
    NotReady(usize),
    Err(E),
}

pub trait ByteProcessor {
    type Item;
    type Error;
    fn process(&mut self, bytes: Bytes, is_eof: bool)
        -> ByteProcessResult<Self::Item, Self::Error>;
}

#[pin_project(project = ProcessReadStreamProject)]
pub struct ProcessReadStream<R, P> {
    #[pin]
    reader: R,
    processor: P,
    should_read: bool,
    chunk_size: usize,
    buffer: BytesMut,
    reader_done: bool,
    pos: u64,
}

impl<R, P> ProcessReadStream<R, P> {
    pub fn new(reader: R, processor: P) -> Self {
        Self {
            reader,
            processor,
            should_read: true,
            chunk_size: INITIAL_CHUNK_SIZE,
            buffer: BytesMut::with_capacity(INITIAL_CHUNK_SIZE),
            reader_done: false,
            pos: 0,
        }
    }

    pub fn into_inner(self) -> R {
        self.reader
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessItem<T> {
    pub start: u64,
    pub end: u64,
    pub item: T,
}

impl<T> ProcessItem<T> {
    pub fn map<F, U>(self, mut f: F) -> ProcessItem<U>
    where
        F: FnMut(T) -> U,
    {
        ProcessItem {
            start: self.start,
            end: self.end,
            item: f(self.item),
        }
    }
}

impl<T> ProcessItem<Option<T>> {
    pub fn transpose(self) -> Option<ProcessItem<T>> {
        Some(ProcessItem {
            start: self.start,
            end: self.end,
            item: self.item?,
        })
    }
}

impl<T, E> ProcessItem<Result<T, E>> {
    pub fn transpose(self) -> Result<ProcessItem<T>, E> {
        Ok(ProcessItem {
            start: self.start,
            end: self.end,
            item: self.item?,
        })
    }
}

impl<T> serde::Serialize for ProcessItem<T>
where
    T: serde::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.item.serialize(serializer)
    }
}

impl<R, P> ProcessReadStream<R, P>
where
    P: Default,
{
    pub fn new_default(reader: R) -> Self {
        Self::new(reader, P::default())
    }
}

impl<R, P> Stream for ProcessReadStream<R, P>
where
    R: AsyncRead,
    P: ByteProcessor,
    P::Error: Into<io::Error>,
{
    type Item = io::Result<ProcessItem<P::Item>>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.reader_done && self.buffer.is_empty() {
                return Poll::Ready(None);
            }
            let this = self.as_mut().project();
            if *this.should_read && !*this.reader_done {
                if this.buffer.capacity() < *this.chunk_size {
                    this.buffer
                        .reserve(*this.chunk_size - this.buffer.capacity());
                }
                let n = {
                    let dst = this.buffer.chunk_mut();
                    let dst = unsafe { dst.as_uninit_slice_mut() };
                    let mut buf = ReadBuf::uninit(dst);
                    let ptr = buf.filled().as_ptr();
                    ready!(this.reader.poll_read(cx, &mut buf)?);
                    assert_eq!(ptr, buf.filled().as_ptr());
                    buf.filled().len()
                };
                if n == 0 {
                    *this.reader_done = true;
                } else {
                    unsafe {
                        this.buffer.advance_mut(n);
                    }
                }
            }
            let bytes = std::mem::take(this.buffer).freeze();
            let proccess_result = this.processor.process(bytes.clone(), *this.reader_done);
            *this.buffer = bytes.into();
            match proccess_result {
                ByteProcessResult::Ok((start_byte_offset, end_byte_offset, result)) => {
                    let result = ProcessItem {
                        start: *this.pos + start_byte_offset as u64,
                        end: *this.pos + end_byte_offset as u64,
                        item: result,
                    };
                    if end_byte_offset > *this.chunk_size {
                        *this.chunk_size = end_byte_offset;
                    }
                    *this.should_read = false;
                    *this.pos += end_byte_offset as u64;
                    this.buffer.advance(end_byte_offset);
                    return Poll::Ready(Some(Ok(result)));
                }
                ByteProcessResult::Done(byte_offset) => {
                    if *this.reader_done {
                        this.buffer.clear();
                        return Poll::Ready(None);
                    }
                    *this.should_read = true;
                    *this.pos += byte_offset as u64;
                    this.buffer.advance(byte_offset);
                }
                ByteProcessResult::NotReady(byte_offset) => {
                    if *this.reader_done {
                        this.buffer.clear();
                        return Poll::Ready(Some(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "Unexpected EOF reached",
                        ))));
                    }
                    *this.should_read = true;
                    *this.pos += byte_offset as u64;
                    this.buffer.advance(byte_offset);
                }
                ByteProcessResult::Err(err) => {
                    *this.reader_done = true;
                    this.buffer.clear();
                    return Poll::Ready(Some(Err(err.into())));
                }
            }
        }
    }
}
