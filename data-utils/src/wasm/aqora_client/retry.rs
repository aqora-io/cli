use std::time::Duration;

use aqora_client::{
    error::MiddlewareError,
    http::{Body, HttpArcLayer},
    retry::{
        BackoffBuilderExt, BackoffRetryLayer, BoxedBackoffBuilder, ExponentialBackoffBuilder,
        RetryClassifier, RetryStatusCodeRange,
    },
};
use serde::{Deserialize, Serialize};
use ts_rs::TS;
use wasm_bindgen::prelude::*;

use super::client::JsResponse;
use crate::wasm::{
    cast::JsCastExt,
    serde::{preserve, DeserializeTagged},
};

#[derive(TS, Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[ts(export)]
pub struct RetryOptions {
    #[ts(optional)]
    backoff: Option<JsBackoff>,
    #[ts(optional)]
    classifer: Option<JsRetryClassifier>,
}

type BoxedRetryClassifier<B, E> = Box<dyn RetryClassifier<http::Response<B>, E>>;
type BoxedBackoffRetryLayer<B, E> =
    BackoffRetryLayer<BoxedRetryClassifier<B, E>, BoxedBackoffBuilder>;

impl<B, E> From<RetryOptions> for BoxedBackoffRetryLayer<B, E>
where
    E: ToString,
{
    fn from(value: RetryOptions) -> Self {
        let backoff = match value.backoff {
            Some(backoff) => backoff.into(),
            None => ExponentialBackoffBuilder::default().boxed(),
        };
        let retry_classifer: BoxedRetryClassifier<B, E> = match value.classifer {
            Some(classifier) => Box::new(classifier),
            None => Box::new(RetryStatusCodeRange::for_client_and_server_errors()),
        };
        BackoffRetryLayer::new(retry_classifer, backoff)
    }
}

impl RetryOptions {
    pub fn into_arc_layer(self) -> HttpArcLayer {
        HttpArcLayer::new(BoxedBackoffRetryLayer::<Body, MiddlewareError>::from(self))
    }
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BackoffKind {
    Exponential,
}

#[derive(TS, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(export, rename = "Backoff")]
pub enum JsBackoff {
    Exponential(ExponentialBackoffOptions),
}

impl From<JsBackoff> for BoxedBackoffBuilder {
    fn from(value: JsBackoff) -> Self {
        match value {
            JsBackoff::Exponential(options) => ExponentialBackoffBuilder::from(options).boxed(),
        }
    }
}

impl JsBackoff {}

impl<'de> DeserializeTagged<'de> for JsBackoff {
    const TAG: &'static str = "kind";
    type Tag = BackoffKind;

    fn deserialize_tagged<D>(tag: Self::Tag, deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        match tag {
            BackoffKind::Exponential => Ok(JsBackoff::Exponential(
                ExponentialBackoffOptions::deserialize(deserializer)?,
            )),
        }
    }
}

#[derive(TS, Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[ts(export)]
pub struct ExponentialBackoffOptions {
    start_delay_ms: usize,
    factor: f64,
    #[serde(default)]
    #[ts(optional)]
    max_delay_ms: Option<usize>,
    #[serde(default)]
    #[ts(optional)]
    max_retries: Option<usize>,
}

impl From<ExponentialBackoffOptions> for ExponentialBackoffBuilder {
    fn from(value: ExponentialBackoffOptions) -> ExponentialBackoffBuilder {
        ExponentialBackoffBuilder {
            start_delay: Duration::from_millis(value.start_delay_ms as u64),
            factor: value.factor,
            max_delay: value
                .max_delay_ms
                .map(|delay| Duration::from_millis(delay as u64)),
            max_retries: value.max_retries,
        }
    }
}

#[derive(TS, Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[ts(export, rename = "RetryClassifier")]
pub struct JsRetryClassifier {
    #[serde(with = "preserve")]
    #[ts(type = "(res: ClientResponse) => boolean")]
    on_response: js_sys::Function,
    #[serde(with = "preserve::option")]
    #[ts(optional, type = "(error: Error) => boolean")]
    on_error: Option<js_sys::Function>,
}

impl<B, E> RetryClassifier<http::Response<B>, E> for JsRetryClassifier
where
    E: ToString,
{
    fn should_retry(&self, res: &Result<http::Response<B>, E>) -> bool {
        match res {
            Ok(res) => {
                let mut builder = http::Response::builder()
                    .status(res.status())
                    .version(res.version());
                if let Some(headers) = builder.headers_mut() {
                    *headers = res.headers().clone();
                }
                let res = builder.body(Default::default()).unwrap_throw();
                self.on_response
                    .call1(&JsValue::NULL, &JsResponse::new(res).into())
                    .unwrap_throw()
                    .cast_into::<js_sys::Boolean>()
                    .unwrap_throw()
                    .into()
            }
            Err(err) => {
                if let Some(on_error) = &self.on_error {
                    on_error
                        .call1(&JsValue::NULL, &JsError::new(&err.to_string()).into())
                        .unwrap_throw()
                        .cast_into::<js_sys::Boolean>()
                        .unwrap_throw()
                        .into()
                } else {
                    true
                }
            }
        }
    }
}
