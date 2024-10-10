use gzp::ZWriter;
use std::io::{Result, Write};
use tokio::io::{AsyncWrite, AsyncWriteExt};

struct UnasyncWriter<W>(W);

impl<W: AsyncWrite + Unpin> Write for UnasyncWriter<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        crate::run::tokio_runtime().block_on(self.0.write(buf))
    }

    fn flush(&mut self) -> Result<()> {
        crate::run::tokio_runtime().block_on(self.0.flush())
    }
}

pub struct Encoder(gzp::par::compress::ParCompress<gzp::deflate::Gzip>);

impl Encoder {
    pub fn new<W: AsyncWrite + Send + Sync + 'static>(inner: W) -> Self {
        Self(gzp::par::compress::ParCompress::builder().from_writer(UnasyncWriter(Box::pin(inner))))
    }
}

impl AsyncWrite for Encoder {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize>> {
        std::task::Poll::Ready(self.0.write(buf))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        std::task::Poll::Ready(self.0.flush())
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        std::task::Poll::Ready(
            self.0
                .finish()
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error)),
        )
    }
}
