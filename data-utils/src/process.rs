use bytes::{Buf, BufMut, BytesMut};
use futures::stream::Stream;
use pin_project_lite::pin_project;
use tokio::io::{self, AsyncRead, ReadBuf};

use std::pin::Pin;
use std::task::{ready, Context, Poll};

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
    fn process(&mut self, bytes: &[u8], is_eof: bool)
        -> ByteProcessResult<Self::Item, Self::Error>;
}

pin_project! {
#[project = ProcessReadStreamProject]
pub struct ProcessReadStream<R, P> {
    #[pin]
    reader: R,
    processor: P,
    should_read: bool,
    chunk_size: usize,
    buffer: BytesMut,
    reader_done: bool,
    pos: usize,
    last_item_pos: Option<(usize, usize)>,
}
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
            last_item_pos: None,
        }
    }

    pub fn last_item_pos(&self) -> Option<(usize, usize)> {
        self.last_item_pos
    }

    pub fn into_inner(self) -> R {
        self.reader
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
    type Item = io::Result<P::Item>;
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
            match this
                .processor
                .process(this.buffer.as_ref(), *this.reader_done)
            {
                ByteProcessResult::Ok((start_byte_offset, end_byte_offset, result)) => {
                    if end_byte_offset > *this.chunk_size {
                        *this.chunk_size = end_byte_offset;
                    }
                    *this.should_read = false;
                    *this.last_item_pos =
                        Some((*this.pos + start_byte_offset, *this.pos + end_byte_offset));
                    *this.pos += end_byte_offset;
                    this.buffer.advance(end_byte_offset);
                    return Poll::Ready(Some(Ok(result)));
                }
                ByteProcessResult::Done(byte_offset) => {
                    if *this.reader_done {
                        this.buffer.clear();
                        return Poll::Ready(None);
                    }
                    *this.should_read = true;
                    *this.pos += byte_offset;
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
                    *this.pos += byte_offset;
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
