use bytes::Bytes;
use tower::{Layer, Service, ServiceExt};
use url::Url;

use crate::async_util::{MaybeSend, MaybeSync};
use crate::error::{MiddlewareError, Result, S3Error};
use crate::http::{HttpBoxService, HttpClient, Request, Response};
use crate::Client;

pub struct S3PutResponse {
    pub etag: String,
    pub original: reqwest::Response,
}

impl TryFrom<reqwest::Response> for S3PutResponse {
    type Error = S3Error;
    fn try_from(response: reqwest::Response) -> Result<Self, Self::Error> {
        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|etag| etag.to_str().ok())
            .ok_or(S3Error::InvalidETag)?;
        Ok(Self {
            etag: etag.to_string(),
            original: response,
        })
    }
}

pub struct S3GetResponse {
    pub content_length: Option<usize>,
    pub content_type: Option<String>,
    pub content_disposition: Option<String>,
    pub original: reqwest::Response,
}

impl S3GetResponse {
    #[cfg(feature = "response-stream")]
    pub fn into_async_read(
        self,
    ) -> tokio_util::io::StreamReader<
        impl futures::stream::Stream<Item = std::io::Result<Bytes>>,
        Bytes,
    > {
        use futures::stream::TryStreamExt;
        tokio_util::io::StreamReader::new(
            self.original
                .bytes_stream()
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err)),
        )
    }
}

impl TryFrom<reqwest::Response> for S3GetResponse {
    type Error = S3Error;
    fn try_from(response: reqwest::Response) -> Result<Self, Self::Error> {
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
        Ok(Self {
            content_length,
            content_type,
            content_disposition,
            original: response,
        })
    }
}

pub enum S3Payload {
    Bytes(Bytes),
    #[cfg(feature = "request-stream")]
    Streaming {
        content_length: usize,
        stream: futures::stream::BoxStream<'static, Result<Bytes, crate::error::BoxError>>,
    },
}

impl S3Payload {
    pub fn bytes(bytes: impl Into<Bytes>) -> Self {
        Self::Bytes(bytes.into())
    }
    #[cfg(feature = "request-stream")]
    pub fn streaming<S>(content_length: usize, stream: S) -> Self
    where
        S: futures::stream::TryStream + Send + 'static,
        S::Error: Into<crate::error::BoxError>,
        Bytes: From<S::Ok>,
    {
        use futures::{StreamExt, TryStreamExt};
        Self::Streaming {
            content_length,
            stream: stream.map_ok(Bytes::from).map_err(|err| err.into()).boxed(),
        }
    }
    pub fn content_length(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
            #[cfg(feature = "request-stream")]
            Self::Streaming { content_length, .. } => *content_length,
        }
    }
}

impl<T> From<T> for S3Payload
where
    T: Into<Bytes>,
{
    fn from(bytes: T) -> Self {
        Self::bytes(bytes)
    }
}
impl From<S3Payload> for reqwest::Body {
    fn from(payload: S3Payload) -> Self {
        match payload {
            S3Payload::Bytes(bytes) => bytes.into(),
            #[cfg(feature = "request-stream")]
            S3Payload::Streaming { stream, .. } => reqwest::Body::wrap_stream(stream),
        }
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

    pub async fn s3_put(&self, url: Url, body: impl Into<S3Payload>) -> Result<S3PutResponse> {
        let mut request = reqwest::Request::new(reqwest::Method::PUT, url);
        let body = body.into();
        request.headers_mut().insert(
            reqwest::header::CONTENT_LENGTH,
            body.content_length().into(),
        );
        request.body_mut().replace(body.into());
        let res = self.s3_service().oneshot(request.try_into()?).await?;
        Ok(reqwest::Response::from(res)
            .error_for_status()?
            .try_into()?)
    }

    pub async fn s3_get(&self, url: Url) -> Result<S3GetResponse> {
        let request = reqwest::Request::new(reqwest::Method::GET, url);
        let res = self.s3_service().oneshot(request.try_into()?).await?;
        Ok(reqwest::Response::from(res)
            .error_for_status()?
            .try_into()?)
    }
}
