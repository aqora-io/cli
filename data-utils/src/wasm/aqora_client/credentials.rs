use aqora_client::{
    credentials::{CredentialsLayer, CredentialsProvider, Tokens},
    error::BoxError,
    http::HttpArcLayer,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use ts_rs::TS;
use wasm_bindgen::prelude::*;

use crate::wasm::{
    cast::JsCastExt,
    error::WasmError,
    serde::{from_value, preserve, to_value},
};

#[derive(TS, Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[ts(rename = "Tokens", export)]
struct JsTokens {
    #[ts(optional)]
    pub access_token: Option<String>,
    #[ts(optional)]
    pub refresh_token: Option<String>,
}

impl From<Tokens> for JsTokens {
    fn from(value: Tokens) -> Self {
        JsTokens {
            access_token: value.access_token,
            refresh_token: value.refresh_token,
        }
    }
}

#[derive(TS, Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[ts(rename = "CredentialsProvider", export)]
pub struct JsCredentialsProvider {
    #[serde(with = "preserve")]
    #[ts(type = "() => Promise<string | undefined | null>")]
    pub bearer_token: js_sys::Function,
    #[serde(default, with = "preserve::option")]
    #[ts(optional, type = "() => Promise<void>")]
    pub revoke_access_token: Option<js_sys::Function>,
    #[serde(default, with = "preserve::option")]
    #[ts(optional, type = "() => Promise<void>")]
    pub revoke_refresh_token: Option<js_sys::Function>,
    #[serde(default, with = "preserve::option")]
    #[ts(optional, type = "(tokens: Tokens) => Promise<void>")]
    pub refresh: Option<js_sys::Function>,
}

impl JsCredentialsProvider {
    pub fn into_arc_layer(self) -> HttpArcLayer {
        HttpArcLayer::new(CredentialsLayer::new(self))
    }
}

#[async_trait(?Send)]
impl CredentialsProvider for JsCredentialsProvider {
    async fn bearer_token(&self) -> Result<Option<String>, BoxError> {
        let res = self
            .bearer_token
            .call0(&JsValue::NULL)
            .map_err(WasmError::from)?
            .promise()
            .await?;
        let token = from_value::<Option<String>>(res)?;
        Ok(token)
    }
    async fn revoke_access_token(&self) -> Result<(), BoxError> {
        if let Some(revoke_fn) = self.revoke_access_token.as_ref() {
            revoke_fn
                .call0(&JsValue::NULL)
                .map_err(WasmError::from)?
                .promise_void()
                .await?;
        }
        Ok(())
    }
    async fn revoke_refresh_token(&self) -> Result<(), BoxError> {
        if let Some(revoke_fn) = self.revoke_refresh_token.as_ref() {
            revoke_fn
                .call0(&JsValue::NULL)
                .map_err(WasmError::from)?
                .promise_void()
                .await?;
        }
        Ok(())
    }
    async fn refresh(&self, tokens: Tokens) -> Result<(), BoxError> {
        if let Some(refresh_fn) = self.refresh.as_ref() {
            let js_tokens = to_value(&JsTokens::from(tokens))?;
            refresh_fn
                .call1(&JsValue::NULL, &js_tokens)
                .map_err(WasmError::from)?
                .promise_void()
                .await?;
        }
        Ok(())
    }
}
