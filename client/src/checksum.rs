use async_trait::async_trait;
use base64::prelude::*;
use reqwest::{
    header::{HeaderName, HeaderValue},
    Request, Response,
};

use crate::async_util::{MaybeSend, MaybeSync};
use crate::middleware::{Middleware, MiddlewareError, Next};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChecksumDigest {
    Crc32(u32),
}

pub trait Checksum {
    type Hasher: ChecksumHasher;
    fn create(&self) -> Self::Hasher;
    fn digest(&self, bytes: &[u8]) -> ChecksumDigest {
        let mut hasher = self.create();
        hasher.update(bytes);
        hasher.finalize()
    }
}

impl<T> Checksum for &T
where
    T: ?Sized + Checksum,
{
    type Hasher = T::Hasher;

    #[inline]
    fn create(&self) -> Self::Hasher {
        T::create(self)
    }

    #[inline]
    fn digest(&self, bytes: &[u8]) -> ChecksumDigest {
        T::digest(self, bytes)
    }
}

pub trait ChecksumHasher {
    fn update(&mut self, bytes: &[u8]);
    fn finalize(self) -> ChecksumDigest;
}

#[cfg(feature = "crc32fast")]
pub mod crc32fast {
    use super::*;
    pub use ::crc32fast::Hasher;

    impl ChecksumHasher for Hasher {
        #[inline]
        fn update(&mut self, bytes: &[u8]) {
            self.update(bytes);
        }
        #[inline]
        fn finalize(self) -> ChecksumDigest {
            ChecksumDigest::Crc32(self.finalize())
        }
    }

    #[derive(Default)]
    pub struct Crc32;

    impl Crc32 {
        #[inline]
        pub fn new() -> Self {
            Self
        }
    }

    impl Checksum for Crc32 {
        type Hasher = Hasher;

        #[inline]
        fn create(&self) -> Self::Hasher {
            Hasher::new()
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

#[derive(Clone, Debug)]
pub struct S3ChecksumMiddleware<T>(T);

impl<T> S3ChecksumMiddleware<T> {
    pub fn new(checksum: T) -> Self {
        Self(checksum)
    }
}

#[cfg_attr(feature = "threaded", async_trait)]
#[cfg_attr(not(feature = "threaded"), async_trait(?Send))]
impl<T> Middleware for S3ChecksumMiddleware<T>
where
    T: Checksum + MaybeSend + MaybeSync,
{
    async fn handle(&self, mut req: Request, next: Next<'_>) -> Result<Response, MiddlewareError> {
        if let Some(body) = req.body().and_then(|body| body.as_bytes()) {
            let digest = self.0.digest(body);
            req.headers_mut()
                .insert(digest.s3_header_name(), digest.s3_header_value());
        }
        next.handle(req).await
    }
}
