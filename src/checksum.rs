use std::pin::pin;

use tokio::io::AsyncRead;
use tokio_stream::StreamExt as _;
use tokio_util::io::ReaderStream;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Checksum {
    Crc32(u32),
}

impl Checksum {
    pub async fn read_default_from<R: AsyncRead>(reader: R) -> std::io::Result<Checksum> {
        Self::read_from::<R, crc32fast::Hasher>(reader).await
    }

    pub async fn read_from<R: AsyncRead, H: Hash>(reader: R) -> std::io::Result<Checksum> {
        let mut reader = pin!(ReaderStream::new(reader));
        let mut hash = H::default();
        while let Some(buf) = reader.try_next().await? {
            hash.update(buf.as_ref());
        }
        Ok(hash.finalize())
    }

    /// Returns the contents of this checksum as a base64 formatted string,
    /// with bytes in big endian order.
    pub fn to_be_base64(&self) -> String {
        use base64::prelude::*;
        match self {
            Self::Crc32(crc32) => BASE64_STANDARD.encode(crc32.to_be_bytes()),
        }
    }
}

/// Hash is a reduced trait compared to std::hasher::Hasher in order to
/// support hash results of different sizes.
pub trait Hash: Default {
    fn update(&mut self, data: &[u8]);
    fn finalize(self) -> Checksum;
}

impl Hash for crc32fast::Hasher {
    fn update(&mut self, data: &[u8]) {
        std::hash::Hasher::write(self, data)
    }

    fn finalize(self) -> Checksum {
        Checksum::Crc32(self.finalize())
    }
}
