use futures::{future::LocalBoxFuture, FutureExt, TryFutureExt};
use js_sys::{JsString, Object};
use wasm_bindgen::{convert::TryFromJsValue, prelude::*};
use wasm_bindgen_futures::JsFuture;

use super::error::{UnexpectedTypeError, WasmError};

pub trait JsCastExt {
    fn type_name(&self) -> JsString;
    fn cast_into<T>(self) -> Result<T, UnexpectedTypeError>
    where
        T: JsCast;
    fn convert_into<T>(self) -> Result<T, UnexpectedTypeError>
    where
        T: TryFromJsValue<Error = JsValue>;
    fn promise(self) -> LocalBoxFuture<'static, Result<JsValue, WasmError>>;
    fn promise_void(self) -> LocalBoxFuture<'static, Result<(), WasmError>>
    where
        Self: Sized,
    {
        self.promise().map_ok(|_| ()).boxed_local()
    }
    fn promise_cast<T>(self) -> LocalBoxFuture<'static, Result<T, WasmError>>
    where
        Self: Sized,
        T: JsCast + 'static,
    {
        self.promise()
            .and_then(|value| futures::future::ready(value.cast_into().map_err(WasmError::from)))
            .boxed_local()
    }
    fn promise_convert<T>(self) -> LocalBoxFuture<'static, Result<T, WasmError>>
    where
        Self: Sized,
        T: TryFromJsValue<Error = JsValue> + 'static,
    {
        self.promise()
            .and_then(|value| futures::future::ready(value.convert_into().map_err(WasmError::from)))
            .boxed_local()
    }
}

impl<T> JsCastExt for T
where
    T: JsCast,
{
    fn type_name(&self) -> JsString {
        if self.as_ref().is_object() {
            self.unchecked_ref::<Object>().constructor().name()
        } else {
            self.as_ref().js_typeof().unchecked_into::<JsString>()
        }
    }
    fn cast_into<U>(self) -> Result<U, UnexpectedTypeError>
    where
        U: JsCast,
    {
        self.dyn_into()
            .map_err(|value| UnexpectedTypeError::new::<U>(value.into()))
    }
    fn convert_into<U>(self) -> Result<U, UnexpectedTypeError>
    where
        U: TryFromJsValue<Error = JsValue>,
    {
        U::try_from_js_value(self.into()).map_err(|value| UnexpectedTypeError::new::<U>(value))
    }
    fn promise(self) -> LocalBoxFuture<'static, Result<JsValue, WasmError>> {
        match self.cast_into::<js_sys::Promise>() {
            Ok(promise) => {
                let fut = JsFuture::from(promise);
                async move { Ok(fut.await?) }.boxed_local()
            }
            Err(err) => futures::future::err(err.into()).boxed_local(),
        }
    }
}
