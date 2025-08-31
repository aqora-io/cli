use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use bytes::Bytes;
use futures::future::TryFutureExt;
use futures::stream::{Stream, TryStreamExt};
use http_body::Body as HttpBody;
use tower::{Layer, Service};

use crate::async_util::{
    MaybeLocalBoxFuture, MaybeLocalBoxStream, MaybeLocalFutureExt, MaybeLocalStreamExt, MaybeSend,
};
use crate::error::{BoxError, MiddlewareError};
use crate::tower_util::{ArcLayer, BoxService};

pub use http_body::SizeHint;

pub enum Body {
    Bytes(Bytes),
    Stream {
        size_hint: http_body::SizeHint,
        stream: MaybeLocalBoxStream<'static, Result<Bytes, BoxError>>,
    },
}

impl fmt::Debug for Body {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Body::Bytes(bytes) => f.debug_tuple("Bytes").field(&bytes.len()).finish(),
            Body::Stream { size_hint, .. } => f.debug_tuple("Stream").field(&size_hint).finish(),
        }
    }
}

impl Default for Body {
    fn default() -> Self {
        Body::Bytes(Bytes::new())
    }
}

impl Body {
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            Body::Bytes(bytes) => Some(bytes),
            Body::Stream { .. } => None,
        }
    }

    pub fn content_length(&self) -> Option<usize> {
        match self {
            Body::Bytes(bytes) => Some(bytes.len()),
            Body::Stream { size_hint, .. } => size_hint.exact().map(|size| size as usize),
        }
    }

    pub async fn bytes(self) -> crate::error::Result<Bytes> {
        Ok(http_body_util::BodyExt::collect(self)
            .await
            .map_err(crate::error::Error::Middleware)?
            .to_bytes())
    }

    pub async fn json<T: serde::de::DeserializeOwned>(self) -> crate::error::Result<T> {
        let bytes = self.bytes().await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub fn wrap<B>(body: B) -> Body
    where
        B: HttpBody + MaybeSend + 'static,
        B::Data: Into<Bytes>,
        B::Error: Into<BoxError>,
    {
        Body::Stream {
            size_hint: body.size_hint(),
            stream: http_body_util::BodyDataStream::new(body)
                .map_ok(|bytes| bytes.into())
                .map_err(|err| err.into())
                .boxed_maybe_local(),
        }
    }

    pub fn into_async_read(
        self,
    ) -> tokio_util::io::StreamReader<MaybeLocalBoxStream<'static, io::Result<Bytes>>, Bytes> {
        tokio_util::io::StreamReader::new(match self {
            Body::Bytes(bytes) => futures::stream::iter(vec![Ok(bytes)]).boxed_maybe_local(),
            Body::Stream { stream, .. } => stream
                .map_err(|err| {
                    #[cfg(feature = "threaded")]
                    {
                        io::Error::other(err)
                    }
                    #[cfg(not(feature = "threaded"))]
                    {
                        io::Error::other(err.to_string())
                    }
                })
                .boxed_maybe_local(),
        })
    }

    pub fn try_clone(&self) -> Option<Self> {
        match self {
            Self::Bytes(bytes) => Some(Self::Bytes(bytes.clone())),
            Self::Stream { .. } => None,
        }
    }
}

impl From<String> for Body {
    fn from(s: String) -> Self {
        Body::Bytes(Bytes::from(s))
    }
}

impl From<Vec<u8>> for Body {
    fn from(v: Vec<u8>) -> Self {
        Body::Bytes(Bytes::from(v))
    }
}

impl From<Bytes> for Body {
    fn from(bytes: Bytes) -> Self {
        Body::Bytes(bytes)
    }
}

impl HttpBody for Body {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        match *self {
            Body::Bytes(ref mut bytes) => {
                let out = bytes.split_off(0);
                if out.is_empty() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Ok(http_body::Frame::data(out))))
                }
            }
            Body::Stream { ref mut stream, .. } => Poll::Ready(
                ready!(Pin::new(stream).poll_next(cx)).map(|opt| opt.map(http_body::Frame::data)),
            ),
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        match *self {
            Body::Bytes(ref bytes) => http_body::SizeHint::with_exact(bytes.len() as u64),
            Body::Stream { ref size_hint, .. } => size_hint.clone(),
        }
    }

    fn is_end_stream(&self) -> bool {
        match *self {
            Body::Bytes(ref bytes) => bytes.is_empty(),
            Body::Stream { .. } => false,
        }
    }
}

impl TryFrom<reqwest::Body> for Body {
    type Error = MiddlewareError;
    fn try_from(body: reqwest::Body) -> Result<Self, MiddlewareError> {
        if let Some(bytes) = body.as_bytes() {
            Ok(Self::Bytes(Bytes::copy_from_slice(bytes)))
        } else {
            #[cfg(target_arch = "wasm32")]
            {
                Err(MiddlewareError::StreamsNotSupported)
            }
            #[cfg(not(target_arch = "wasm32"))]
            {
                Ok(Body::Stream {
                    size_hint: body.size_hint(),
                    stream: http_body_util::BodyDataStream::new(body)
                        .map_err(|err| err.into())
                        .boxed_maybe_local(),
                })
            }
        }
    }
}

impl TryFrom<Body> for reqwest::Body {
    type Error = MiddlewareError;
    fn try_from(body: Body) -> Result<Self, MiddlewareError> {
        match body {
            Body::Bytes(bytes) => Ok(reqwest::Body::from(bytes)),
            #[cfg(any(target_arch = "wasm32", not(feature = "threaded"),))]
            Body::Stream { .. } => Err(MiddlewareError::StreamsNotSupported),
            #[cfg(all(not(target_arch = "wasm32"), feature = "threaded"))]
            Body::Stream { stream, .. } => Ok(reqwest::Body::wrap_stream(stream)),
        }
    }
}

fn to_reqwest(request: http::Request<Body>) -> Result<reqwest::Request, MiddlewareError> {
    let (parts, body) = request.into_parts();
    let new_request = http::Request::from_parts(parts, reqwest::Body::from(Bytes::new()));
    let mut request = reqwest::Request::try_from(new_request).map_err(MiddlewareError::Request)?;
    request.body_mut().replace(body.try_into()?);
    Ok(request)
}

fn from_reqwest(response: reqwest::Response) -> Result<http::Response<Body>, MiddlewareError> {
    let mut builder = http::Response::builder()
        .status(response.status())
        .extension(response.url().clone());
    #[cfg(not(target_arch = "wasm32"))]
    {
        builder = builder.version(response.version());
    }
    if let Some(headers) = builder.headers_mut() {
        *headers = response.headers().clone()
    }
    let size_hint = response
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok()?.parse::<u64>().ok())
        .map(http_body::SizeHint::with_exact)
        .unwrap_or_default();
    builder
        .body(Body::Stream {
            size_hint,
            stream: response
                .bytes_stream()
                .map_err(|err| err.into())
                .boxed_maybe_local(),
        })
        .map_err(|err| MiddlewareError::Middleware(err.into()))
}

pub type Request = http::Request<Body>;
pub type Response = http::Response<Body>;

pub fn check_status(status: &http::StatusCode) -> Result<(), crate::error::Error> {
    if status.is_client_error() || status.is_server_error() {
        Err(crate::error::Error::BadStatus(*status))
    } else {
        Ok(())
    }
}

#[derive(Clone)]
pub struct HttpClient {
    client: reqwest::Client,
}

impl HttpClient {
    pub(crate) fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl Service<Request> for HttpClient {
    type Response = Response;
    type Error = MiddlewareError;
    type Future = MaybeLocalBoxFuture<'static, Result<Response, MiddlewareError>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), MiddlewareError>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: Request) -> Self::Future {
        let client = self.client.clone();
        async move {
            from_reqwest(
                client
                    .execute(to_reqwest(req)?)
                    .await
                    .map_err(MiddlewareError::Request)?,
            )
        }
        .boxed_maybe_local()
    }
}

pub type HttpBoxService = BoxService<Request, Response, MiddlewareError>;
pub type HttpArcLayer<Client = HttpBoxService> =
    ArcLayer<Client, Request, Response, MiddlewareError>;

#[derive(Clone, Debug)]
pub struct NormalizeHttpService<S> {
    inner: S,
}

impl<S> NormalizeHttpService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, ResBody> Service<Request> for NormalizeHttpService<S>
where
    S: Service<Request, Response = http::Response<ResBody>>,
    ResBody: HttpBody + MaybeSend + 'static,
    ResBody::Data: Into<bytes::Bytes>,
    ResBody::Error: Into<BoxError>,
{
    type Response = Response;
    type Error = S::Error;
    type Future = futures::future::MapOk<S::Future, fn(http::Response<ResBody>) -> Response>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, req: Request) -> Self::Future {
        self.inner.call(req).map_ok(|res| {
            let (parts, body) = res.into_parts();
            http::Response::from_parts(parts, Body::wrap(body))
        })
    }
}

pub struct NormalizeHttpLayer<L> {
    layer: Arc<L>,
}

impl<L> NormalizeHttpLayer<L> {
    pub fn new(layer: L) -> Self {
        Self {
            layer: Arc::new(layer),
        }
    }
}

impl<L> Clone for NormalizeHttpLayer<L> {
    fn clone(&self) -> Self {
        Self {
            layer: self.layer.clone(),
        }
    }
}

impl<L, S> Layer<S> for NormalizeHttpLayer<L>
where
    L: Layer<S>,
{
    type Service = NormalizeHttpService<L::Service>;
    fn layer(&self, inner: S) -> Self::Service {
        NormalizeHttpService::new(self.layer.layer(inner))
    }
}
