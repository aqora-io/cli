use std::{
    future::Future,
    io::{Result, SeekFrom},
    ops::DerefMut,
    pin::Pin,
    task::ready,
};

use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt, ReadBuf};

/// Generalizes the capability to clone a file descriptor for other kinds of data.
/// Attention: you must not process clones concurrently.
pub trait AsyncTryClone: Sized {
    fn try_clone(&self) -> impl Future<Output = Result<Self>>;
}

impl AsyncTryClone for tokio::fs::File {
    #[inline]
    fn try_clone(&self) -> impl Future<Output = Result<Self>> {
        self.try_clone()
    }
}

/// Creates a bounded view of a file.
pub struct FilePart<F> {
    file: F,
    offset: u64,
    length: usize,
    current: u64,
}

impl<F: AsyncSeek + Unpin> FilePart<F> {
    pub async fn slice(mut file: F, offset: u64, length: usize) -> Result<Self> {
        file.seek(SeekFrom::Start(offset)).await?;
        Ok(Self {
            file,
            offset,
            length,
            current: 0,
        })
    }
}

impl<F> FilePart<F> {
    #[inline]
    fn remaining(&self) -> usize {
        self.length - self.current as usize
    }

    #[inline]
    fn end_offset(&self) -> u64 {
        self.offset + self.length as u64
    }

    #[inline]
    unsafe fn advance(&mut self, n: usize) {
        self.current += n as u64;
    }

    #[inline]
    fn eof(&self) -> bool {
        self.current == self.end_offset()
    }

    #[inline]
    fn check_bounds(&self, position: u64) -> bool {
        position >= self.offset && position <= self.end_offset()
    }

    fn resolve_position_from_start(&self, position: u64) -> Result<SeekFrom> {
        if self.check_bounds(position) {
            Ok(SeekFrom::Start(position))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "position out of bounds",
            ))
        }
    }

    fn map_position(&self, position: SeekFrom) -> Result<SeekFrom> {
        match position {
            position @ SeekFrom::Current(_) => Ok(position),
            SeekFrom::Start(start) => self.resolve_position_from_start(self.offset + start),
            SeekFrom::End(end) if end > 0 => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "position out of bounds",
            )),
            SeekFrom::End(end) => self.resolve_position_from_start(
                self.end_offset().checked_add_signed(end).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "integer overflow while mapping seek position from end",
                    )
                })?,
            ),
        }
    }
}

impl<F: AsyncRead + Unpin> AsyncRead for FilePart<F> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<Result<()>> {
        use std::task::Poll::Ready;

        if self.eof() {
            return Ready(Ok(()));
        }

        let this = self.deref_mut();

        // (a) buf_part is guaranteed to not exceed both `buf`` or `remaining()`
        let mut buf_part = buf.take(this.remaining());
        ready!(Pin::new(&mut this.file).poll_read(cx, &mut buf_part))?;
        let n = buf_part.filled().len();

        // Safe because of (a)
        unsafe {
            buf.assume_init(n);
        }
        buf.advance(n);
        // Safe because of (a)
        unsafe {
            this.advance(n);
        }
        Ready(Ok(()))
    }
}

impl<F: AsyncSeek + Unpin> AsyncSeek for FilePart<F> {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> Result<()> {
        let new_position = self.map_position(position)?;
        Pin::new(&mut self.deref_mut().file).start_seek(new_position)
    }

    fn poll_complete(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<u64>> {
        use std::task::Poll::Ready;

        let this = self.deref_mut();
        let absolute_current = ready!(Pin::new(&mut this.file).poll_complete(cx))?;
        let current = absolute_current - this.offset;
        this.current = current;
        Ready(Ok(current))
    }
}

impl<F: AsyncTryClone> AsyncTryClone for FilePart<F> {
    async fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            file: self.file.try_clone().await?,
            offset: self.offset,
            length: self.length,
            current: self.current,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::min, io::SeekFrom};

    use futures::StreamExt;
    use rand::{thread_rng, Rng};
    use tempfile::tempfile;
    use tokio::{
        fs::File,
        io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    };
    use tokio_util::io::ReaderStream;

    use super::AsyncTryClone as _;

    #[tokio::test]
    async fn test_file_part() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        const KB: usize = 1024;
        const MB: usize = 1024 * KB;
        const FILE_SIZE: usize = 10 * MB;

        {
            // create file with random
            let mut file = File::from_std(tempfile()?);
            let mut written = 0usize;
            let mut rand_buf = [0u8; 512 * KB];
            while written < FILE_SIZE {
                thread_rng().fill(&mut rand_buf[..]);
                let to_write = min(rand_buf.len(), FILE_SIZE - written);
                file.write_all(&rand_buf[..to_write]).await?;
                written += to_write;
            }
            assert_eq!(written, FILE_SIZE);

            // seek file from start
            file.rewind().await?;

            // slice file
            let chunk_offset = 3 * MB as u64;
            let chunk_length = 2 * MB;
            let chunk_end_offset = chunk_offset + chunk_length as u64;
            let mut chunk =
                super::FilePart::slice(file.try_clone().await?, chunk_offset, chunk_length).await?;

            // read all slice
            let mut chunk_data = Vec::with_capacity(2 * MB);
            let chunk_read = chunk.read_to_end(&mut chunk_data).await?;
            assert_eq!(chunk_read, chunk_length);
            assert_eq!(chunk.stream_position().await?, chunk_length as u64);
            assert_eq!(file.stream_position().await?, chunk_end_offset);

            // seek slice from start
            assert_eq!(chunk.rewind().await?, 0);
            assert_eq!(file.stream_position().await?, chunk_offset);
            assert_eq!(chunk.stream_position().await?, 0);

            // seek slice from end
            assert_eq!(chunk.seek(SeekFrom::End(0)).await?, chunk_length as u64);
            assert_eq!(file.stream_position().await?, chunk_end_offset);
            assert_eq!(chunk.stream_position().await?, chunk_length as u64);

            // ReaderStream
            chunk.rewind().await?;
            let mut stream = ReaderStream::new(chunk.try_clone().await?);
            let mut stream_data = Vec::with_capacity(chunk_length);
            while let Some(buf) = stream.next().await.transpose()? {
                stream_data.extend(buf);
            }
            assert_eq!(stream_data, chunk_data);
            assert_eq!(chunk.stream_position().await?, chunk_length as u64);
        }

        Ok(())
    }
}
