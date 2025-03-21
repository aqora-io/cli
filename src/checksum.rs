use std::hash::Hasher;

use futures::TryStreamExt as _;
use reqwest::header::{HeaderName, HeaderValue};
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;

pub type DefaultHash = crc32fast::Hasher;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Checksum {
    Crc32(u32),
}

impl Checksum {
    pub async fn read_default_from<R: AsyncRead>(reader: R) -> std::io::Result<Checksum> {
        Self::read_from::<R, DefaultHash>(reader).await
    }

    pub async fn read_from<R: AsyncRead, H: Hasher>(reader: R) -> std::io::Result<Checksum>
    where
        CollectHash<H>: Default + FinishHash,
    {
        Ok(ReaderStream::new(reader)
            .try_collect::<CollectHash<H>>()
            .await?
            .finish())
    }

    pub fn header_name(self) -> HeaderName {
        match self {
            Self::Crc32(_) => HeaderName::from_static("x-amz-checksum-crc32"),
        }
    }

    pub fn header_value(self) -> HeaderValue {
        use base64::prelude::*;
        match self {
            Self::Crc32(crc32) => BASE64_STANDARD
                .encode(crc32.to_be_bytes())
                .try_into()
                .expect("base64 encoding is not ascii"),
            // _ => HeaderValue::from_static(src)
        }
    }
}

pub trait FinishHash {
    fn finish(self) -> Checksum;
}

pub struct CollectHash<H: Hasher>(H);

impl Default for CollectHash<crc32fast::Hasher> {
    #[inline]
    fn default() -> Self {
        Self(crc32fast::Hasher::new())
    }
}

impl FinishHash for CollectHash<crc32fast::Hasher> {
    #[inline]
    fn finish(self) -> Checksum {
        Checksum::Crc32(self.0.finalize())
    }
}

impl<H: Hasher> Extend<tokio_util::bytes::Bytes> for CollectHash<H> {
    fn extend<T: IntoIterator<Item = tokio_util::bytes::Bytes>>(&mut self, iter: T) {
        for bytes in iter {
            self.0.write(bytes.as_ref());
        }
    }
}
