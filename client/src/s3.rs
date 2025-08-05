use std::ops::RangeBounds;

use tower::{Layer, Service, ServiceExt};
use url::Url;

use crate::async_util::{MaybeSend, MaybeSync};
use crate::error::{MiddlewareError, Result, S3Error};
use crate::http::{check_status, Body, HttpBoxService, HttpClient, Request, Response};
use crate::Client;

pub struct S3PutResponse {
    pub etag: String,
    pub head: http::response::Parts,
    pub body: Body,
}

impl TryFrom<Response> for S3PutResponse {
    type Error = S3Error;
    fn try_from(response: Response) -> Result<Self, Self::Error> {
        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|etag| etag.to_str().ok())
            .ok_or(S3Error::InvalidETag)?
            .to_string();
        let (head, body) = response.into_parts();
        Ok(Self { etag, head, body })
    }
}

pub struct S3GetResponse {
    pub content_length: Option<usize>,
    pub content_type: Option<String>,
    pub content_disposition: Option<String>,
    pub head: http::response::Parts,
    pub body: Body,
}

impl TryFrom<Response> for S3GetResponse {
    type Error = S3Error;
    fn try_from(response: Response) -> Result<Self, Self::Error> {
        let content_length = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .map(|content_len| {
                content_len
                    .to_str()
                    .map_err(|_| S3Error::InvalidContentLength)
                    .and_then(|content_len| {
                        content_len
                            .parse::<usize>()
                            .map_err(|_| S3Error::InvalidContentLength)
                    })
            })
            .transpose()?;
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .map(|ct| ct.to_str())
            .transpose()
            .map_err(|_| S3Error::InvalidContentType)?
            .map(|ct| ct.to_string());
        let content_disposition = response
            .headers()
            .get(reqwest::header::CONTENT_DISPOSITION)
            .map(|cd| cd.to_str())
            .transpose()
            .map_err(|_| S3Error::InvalidContentDisposition)?
            .map(|cd| cd.to_string());
        let (head, body) = response.into_parts();
        Ok(Self {
            content_length,
            content_type,
            content_disposition,
            head,
            body,
        })
    }
}

impl Client {
    pub fn s3_layer<L, E>(&mut self, layer: L) -> &mut Self
    where
        L: Layer<HttpBoxService> + MaybeSend + MaybeSync + 'static,
        L::Service: Service<Request, Response = Response, Error = E> + Clone + MaybeSend + 'static,
        <L::Service as Service<Request>>::Future: MaybeSend + 'static,
        MiddlewareError: From<E>,
        E: 'static,
    {
        self.s3_layer.stack(layer);
        self
    }

    #[inline]
    fn s3_service(&self) -> HttpBoxService {
        self.s3_layer.layer(HttpClient::new(self.inner().clone()))
    }

    pub async fn s3_put(&self, url: Url, body: impl Into<Body>) -> Result<S3PutResponse> {
        let mut request = http::Request::builder()
            .method(http::Method::PUT)
            .uri(url.to_string());
        let body = body.into();
        if let Some(content_length) = body.content_length() {
            request.headers_mut().map(|headers| {
                headers.insert(reqwest::header::CONTENT_LENGTH, content_length.into())
            });
        }
        let request = request.body(body)?;
        let res = self.s3_service().oneshot(request).await?;
        check_status(&res.status())?;
        Ok(res.try_into()?)
    }

    #[inline]
    pub async fn s3_get(&self, url: Url) -> Result<S3GetResponse> {
        self.s3_get_range(url, ..).await
    }

    pub async fn s3_get_range(&self, url: Url, range: impl Into<S3Range>) -> Result<S3GetResponse> {
        self.validate_url_host(&url)?;
        let mut request = http::Request::builder()
            .method(http::Method::GET)
            .uri(url.to_string());
        if let Some(range) = range.into().into_header()? {
            request = request.header(http::header::RANGE, range);
        }
        let request = request.body(Body::default())?;
        let res = self.s3_service().oneshot(request).await?;
        check_status(&res.status())?;
        Ok(res.try_into()?)
    }
}

pub struct S3Range {
    pub lo: Option<usize>,
    pub hi: Option<usize>,
}

impl<T: RangeBounds<usize>> From<T> for S3Range {
    fn from(value: T) -> Self {
        use std::ops::Bound::*;
        let lo = match value.start_bound() {
            Unbounded => None,
            Included(lo) => Some(*lo),
            Excluded(lo) => Some(lo + 1),
        };
        let hi = match value.end_bound() {
            Unbounded => None,
            Included(hi) => Some(*hi),
            Excluded(hi) => Some(hi - 1),
        };
        Self { lo, hi }
    }
}

impl S3Range {
    pub const FULL: Self = Self { lo: None, hi: None };

    pub fn into_header(self) -> Result<Option<http::HeaderValue>> {
        match (self.lo, self.hi) {
            (Some(lo), Some(hi)) if lo > hi => Err(crate::error::Error::BadS3Range),
            (Some(lo), Some(hi)) => Ok(Some(http::HeaderValue::try_from(format!(
                "bytes={lo}-{hi}"
            ))?)),
            (Some(lo), None) => Ok(Some(http::HeaderValue::try_from(format!("bytes={lo}-"))?)),
            (None, Some(hi)) => Ok(Some(http::HeaderValue::try_from(format!("bytes=-{hi}"))?)),
            (None, None) => Ok(None),
        }
    }
}
