use std::sync::Arc;
use std::task::{Context, Poll};

use base64::prelude::*;
use reqwest::header::{HeaderName, HeaderValue};
use tower::{Layer, Service};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChecksumDigest {
    Crc32(u32),
}

pub trait Checksum {
    fn digest(&self, bytes: &[u8]) -> ChecksumDigest;
}

impl<T> Checksum for &T
where
    T: ?Sized + Checksum,
{
    #[inline]
    fn digest(&self, bytes: &[u8]) -> ChecksumDigest {
        T::digest(self, bytes)
    }
}

impl<T> Checksum for Box<T>
where
    T: ?Sized + Checksum,
{
    #[inline]
    fn digest(&self, bytes: &[u8]) -> ChecksumDigest {
        T::digest(self, bytes)
    }
}

#[cfg(feature = "crc32fast")]
pub mod crc32fast {
    use super::*;
    pub use ::crc32fast::Hasher as Crc32;

    impl Checksum for Crc32 {
        #[inline]
        fn digest(&self, bytes: &[u8]) -> ChecksumDigest {
            let mut hasher = self.clone();
            hasher.update(bytes);
            ChecksumDigest::Crc32(hasher.finalize())
        }
    }
}

impl ChecksumDigest {
    fn s3_header_name(&self) -> HeaderName {
        match self {
            Self::Crc32(_) => HeaderName::from_static("x-amz-checksum-crc32"),
        }
    }

    fn s3_header_value(&self) -> HeaderValue {
        match self {
            Self::Crc32(val) => BASE64_STANDARD
                .encode(val.to_be_bytes())
                .try_into()
                .expect("Base64 should always be a valid header"),
        }
    }
}

pub struct S3ChecksumService<T, S> {
    checksum: Arc<T>,
    inner: S,
}

impl<T, S> Clone for S3ChecksumService<T, S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            checksum: self.checksum.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<T, S> S3ChecksumService<T, S> {
    fn new_arc(checksum: Arc<T>, service: S) -> Self {
        Self {
            checksum,
            inner: service,
        }
    }

    pub fn new(checksum: T, service: S) -> Self {
        Self::new_arc(Arc::new(checksum), service)
    }
}

impl<T, S> Service<crate::http::Request> for S3ChecksumService<T, S>
where
    T: Checksum,
    S: Service<crate::http::Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;
    fn poll_ready(&mut self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, mut req: crate::http::Request) -> Self::Future {
        if let Some(body) = req.body().as_bytes() {
            let digest = self.checksum.digest(body);
            req.headers_mut()
                .insert(digest.s3_header_name(), digest.s3_header_value());
        }
        self.inner.call(req)
    }
}

pub struct S3ChecksumLayer<T> {
    checksum: Arc<T>,
}

impl<T> Clone for S3ChecksumLayer<T> {
    fn clone(&self) -> Self {
        Self {
            checksum: self.checksum.clone(),
        }
    }
}

impl<T> S3ChecksumLayer<T> {
    pub fn new(checksum: T) -> Self {
        Self {
            checksum: Arc::new(checksum),
        }
    }
}

impl<T, S> Layer<S> for S3ChecksumLayer<T> {
    type Service = S3ChecksumService<T, S>;
    fn layer(&self, inner: S) -> Self::Service {
        S3ChecksumService::new_arc(self.checksum.clone(), inner)
    }
}
