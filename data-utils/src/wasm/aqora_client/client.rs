use std::task::{Context, Poll};

use aqora_client::{
    error::MiddlewareError,
    http::{HttpArcLayer, Request, Response},
    Client,
};
use futures::{future::LocalBoxFuture, FutureExt, TryFutureExt};
use serde::{Deserialize, Serialize};
use tower::{Layer, Service};
use ts_rs::TS;
use wasm_bindgen::prelude::*;

use crate::wasm::{
    cast::JsCastExt,
    error::WasmError,
    serde::{from_value, preserve, DeserializeTagged},
};

use super::multipart::{DatasetVersionFileUploadOptions, JsDatasetVersionFileUploader};

#[wasm_bindgen(js_name = Client)]
#[derive(Clone)]
pub struct JsClient {
    inner: Client,
}

impl JsClient {
    pub fn into_inner(self) -> Client {
        self.inner
    }
}

#[wasm_bindgen(js_class = Client)]
impl JsClient {
    #[wasm_bindgen(constructor)]
    pub fn new(url: String) -> Result<Self, JsError> {
        Ok(JsClient {
            inner: Client::new(url.parse()?),
        })
    }

    #[wasm_bindgen(js_name = "graphqlLayer")]
    pub fn graphql_layer(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ClientLayer")] layer: JsValue,
    ) -> Result<Self, WasmError> {
        self.inner
            .graphql_layer(from_value::<JsClientLayer>(layer)?.into_arc_layer());
        Ok(self.clone())
    }

    #[wasm_bindgen(js_name = "s3Layer")]
    pub fn s3_layer(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ClientLayer")] layer: JsValue,
    ) -> Result<Self, WasmError> {
        self.inner
            .s3_layer(from_value::<JsClientLayer>(layer)?.into_arc_layer());
        Ok(self.clone())
    }

    #[wasm_bindgen(js_name = "datasetVersionFileUploader")]
    pub fn dataset_version_file_uploader(
        &self,
        #[wasm_bindgen(unchecked_param_type = "DatasetVersionFileUploadOptions")] options: JsValue,
    ) -> Result<JsDatasetVersionFileUploader, WasmError> {
        let options = from_value::<DatasetVersionFileUploadOptions>(options)?;
        Ok(JsDatasetVersionFileUploader::new(self.clone(), options))
    }
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LayerKind {
    Custom,
    #[cfg(feature = "aqora-client-retry")]
    Retry,
    #[cfg(feature = "aqora-client-credentials")]
    Credentials,
    #[cfg(feature = "aqora-client-checksum")]
    S3Checksum,
}

#[derive(TS, Serialize, Debug, Clone, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(export, rename = "ClientLayer")]
pub enum JsClientLayer {
    Custom(JsCustomLayer),
    #[cfg(feature = "aqora-client-retry")]
    Retry(super::retry::RetryOptions),
    #[cfg(feature = "aqora-client-credentials")]
    Credentials(super::credentials::JsCredentialsProvider),
    #[cfg(feature = "aqora-client-checksum")]
    S3Checksum(super::checksum::S3ChecksumOptions),
}

impl JsClientLayer {
    fn into_arc_layer(self) -> HttpArcLayer {
        match self {
            Self::Custom(layer) => layer.into_arc_layer(),
            #[cfg(feature = "aqora-client-retry")]
            Self::Retry(layer) => layer.into_arc_layer(),
            #[cfg(feature = "aqora-client-credentials")]
            Self::Credentials(layer) => layer.into_arc_layer(),
            #[cfg(feature = "aqora-client-checksum")]
            Self::S3Checksum(layer) => layer.into_arc_layer(),
        }
    }
}

impl<'de> DeserializeTagged<'de> for JsClientLayer {
    const TAG: &'static str = "kind";
    type Tag = LayerKind;

    fn deserialize_tagged<D>(tag: Self::Tag, deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        match tag {
            LayerKind::Custom => Ok(JsClientLayer::Custom(JsCustomLayer::deserialize(
                deserializer,
            )?)),
            #[cfg(feature = "aqora-client-retry")]
            LayerKind::Retry => Ok(JsClientLayer::Retry(
                super::retry::RetryOptions::deserialize(deserializer)?,
            )),
            #[cfg(feature = "aqora-client-credentials")]
            LayerKind::Credentials => Ok(JsClientLayer::Credentials(
                super::credentials::JsCredentialsProvider::deserialize(deserializer)?,
            )),
            #[cfg(feature = "aqora-client-checksum")]
            LayerKind::S3Checksum => Ok(JsClientLayer::S3Checksum(
                super::checksum::S3ChecksumOptions::deserialize(deserializer)?,
            )),
        }
    }
}

impl<'de> Deserialize<'de> for JsClientLayer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <Self as DeserializeTagged>::deserialize(deserializer)
    }
}

mod js_http {
    use crate::wasm::serde::DEFAULT_SERIALIZER;
    use serde_wasm_bindgen::{Deserializer, Error};
    use wasm_bindgen::JsValue;

    macro_rules! serde_impl {
        ($mod:ident, $struct:ident) => {
            pub mod $mod {
                use super::*;
                pub fn to_value(value: &::http::$struct) -> Result<JsValue, Error> {
                    ::http_serde::$mod::serialize(value, &DEFAULT_SERIALIZER)
                }
                pub fn from_value(value: JsValue) -> Result<::http::$struct, Error> {
                    ::http_serde::$mod::deserialize(Deserializer::from(value))
                }
            }
        };
    }
    serde_impl!(method, Method);
    serde_impl!(status_code, StatusCode);
    serde_impl!(version, Version);
    serde_impl!(header_map, HeaderMap);
    serde_impl!(uri, Uri);
}

macro_rules! http_class_impl {
    ($struct:ident(js_name = $js_class:ident) { $($mod:ident($param_type:literal, $getter:ident = $get_method:ident, $setter:ident = $set_method:ident))* }) => {
        #[wasm_bindgen(js_class = $js_class)]
        impl $struct {
        $(
            #[wasm_bindgen(getter = $getter, unchecked_return_type = $param_type)]
            pub fn $getter(&self) -> Result<::wasm_bindgen::JsValue, ::serde_wasm_bindgen::Error> {
                js_http::$mod::to_value(&self.inner.$get_method())
            }
            #[wasm_bindgen(setter = $getter)]
            pub fn $setter(&mut self, #[wasm_bindgen(unchecked_param_type = $param_type)] value: JsValue) -> Result<(), ::serde_wasm_bindgen::Error> {
                *self.inner.$set_method() = js_http::$mod::from_value(value)?;
                Ok(())
            }
        )*
        }
    };
}

#[wasm_bindgen(js_name = ClientRequest)]
pub struct JsRequest {
    inner: Request,
}

impl JsRequest {
    pub fn new(inner: Request) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> Request {
        self.inner
    }
}

http_class_impl! {
JsRequest(js_name = ClientRequest) {
    method("string", method = method, set_method = method_mut)
    uri("string", uri = uri, set_uri = uri_mut)
    version("string", version = version, set_version = version_mut)
    header_map("Record<string, string>", headers = headers, set_headers = headers_mut)
}
}

#[wasm_bindgen(js_class = ClientRequest)]
impl JsRequest {
    fn as_bytes(&self) -> Result<&[u8], JsError> {
        self.inner
            .body()
            .as_bytes()
            .map(|bytes| bytes.as_ref())
            .ok_or_else(|| JsError::new("Could not access request body"))
    }

    #[wasm_bindgen(getter = "contentLength")]
    pub fn content_length(&self) -> Result<usize, JsError> {
        Ok(self.as_bytes()?.len())
    }

    #[wasm_bindgen(getter)]
    pub fn body(&self) -> Result<js_sys::Uint8Array, JsError> {
        let bytes = self.as_bytes()?;
        let array = js_sys::Uint8Array::new_with_length(bytes.len() as u32);
        array.copy_from(bytes);
        Ok(array)
    }

    #[wasm_bindgen(setter)]
    pub fn set_body(&mut self, body: js_sys::Uint8Array) {
        *self.inner.body_mut() = body.to_vec().into();
    }
}

#[wasm_bindgen(js_name = ClientResponse)]
pub struct JsResponse {
    inner: Response,
}

impl JsResponse {
    pub fn new(inner: Response) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> Response {
        self.inner
    }
}

http_class_impl! {
JsResponse(js_name = ClientResponse) {
    status_code("number", status = status, set_status = status_mut)
    version("string", version = version, set_version = version_mut)
    header_map("Record<string, string>", headers = headers, set_headers = headers_mut)
}
}

enum ReadyState {
    Pending(LocalBoxFuture<'static, Result<(), MiddlewareError>>),
    Ready,
}

impl ReadyState {
    fn start(ready: Option<js_sys::Function>) -> Self {
        match ready {
            Some(func) => Self::Pending(
                async move {
                    Ok(func
                        .call0(&JsValue::NULL)
                        .map_err(WasmError::from)?
                        .promise_void()
                        .await?)
                }
                .boxed_local(),
            ),
            None => Self::Ready,
        }
    }
}

pub struct JsService<S> {
    /// (req: ClientRequest, next: (req: ClientRequest) => Promise<ClientResponse) => Promise<ClientResponse>
    func: js_sys::Function,
    /// () => Promise<void>
    ready: Option<js_sys::Function>,
    ready_state: ReadyState,
    inner: S,
    inner_ready: bool,
}

impl<S> JsService<S> {
    pub fn new(inner: S, func: js_sys::Function, ready: Option<js_sys::Function>) -> Self {
        Self {
            func,
            ready: ready.clone(),
            ready_state: ReadyState::start(ready),
            inner,
            inner_ready: false,
        }
    }
}

impl<S> Clone for JsService<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self::new(self.inner.clone(), self.func.clone(), self.ready.clone())
    }
}

impl<S, E> Service<Request> for JsService<S>
where
    S: Service<Request, Response = Response, Error = E> + Clone + 'static,
    S::Future: 'static,
    E: 'static,
    MiddlewareError: From<E>,
{
    type Response = Response;
    type Error = MiddlewareError;
    type Future = LocalBoxFuture<'static, Result<Response, MiddlewareError>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let ready_pending = match &mut self.ready_state {
            ReadyState::Pending(fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => {
                    self.ready_state = ReadyState::Ready;
                    false
                }
                Poll::Pending => true,
            },
            ReadyState::Ready => false,
        };
        let inner_pending = if self.inner_ready {
            false
        } else {
            match self.inner.poll_ready(cx) {
                Poll::Ready(Err(err)) => return Poll::Ready(Err(MiddlewareError::from(err))),
                Poll::Ready(Ok(())) => {
                    self.inner_ready = true;
                    false
                }
                Poll::Pending => true,
            }
        };
        if ready_pending || inner_pending {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn call(&mut self, req: Request) -> Self::Future {
        let mut inner = self.inner.clone();
        let next = Closure::new::<Box<dyn FnMut(JsRequest) -> js_sys::Promise>>(Box::new(
            move |req: JsRequest| -> js_sys::Promise {
                wasm_bindgen_futures::future_to_promise(
                    inner
                        .call(req.into_inner())
                        .map_ok(|res| JsValue::from(JsResponse::new(res)))
                        .map_err(|err| {
                            JsValue::from(JsError::new(&MiddlewareError::from(err).to_string()))
                        }),
                )
            },
        ));
        let js_req = JsRequest::new(req);
        let fut = self
            .func
            .call2(&JsValue::NULL, &js_req.into(), &next.into_js_value())
            .map(|res| res.promise());
        async move { Ok::<_, WasmError>(fut?.await?.convert_into::<JsResponse>()?.into_inner()) }
            .map_err(|err| MiddlewareError::Middleware(err.into()))
            .boxed_local()
    }
}

#[derive(TS, Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[ts(rename = "CustomClientLayer", export)]
pub struct JsCustomLayer {
    #[serde(with = "preserve")]
    #[ts(
        type = "(req: ClientRequest, next: (req: ClientRequest) => Promise<ClientResponse>) => Promise<ClientResponse>"
    )]
    pub func: js_sys::Function,
    #[serde(default, with = "preserve::option")]
    #[ts(optional, type = "() => Promise<void>")]
    pub ready: Option<js_sys::Function>,
}

impl JsCustomLayer {
    pub fn into_arc_layer(self) -> HttpArcLayer {
        HttpArcLayer::new(self)
    }
}

impl<S> Layer<S> for JsCustomLayer {
    type Service = JsService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        JsService::new(inner, self.func.clone(), self.ready.clone())
    }
}
