use std::time::Duration;

#[cfg(feature = "tokio-time")]
mod tokio_impl {
    use super::*;

    pub use tokio::time::Sleep;

    pub fn sleep(duration: Duration) -> Sleep {
        tokio::time::sleep(duration)
    }
}

#[cfg(feature = "wasm-time")]
mod wasm_impl {
    use super::*;
    use crate::wasm::global;

    use futures::future::{FutureExt, Map};
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{
        js_sys::{Function, Promise},
        wasm_bindgen::{JsValue, UnwrapThrowExt},
    };

    fn throw(result: Result<JsValue, JsValue>) {
        result.unwrap_throw();
    }

    pub type Sleep = Map<JsFuture, fn(Result<JsValue, JsValue>) -> ()>;

    pub fn sleep(duration: Duration) -> Sleep {
        let millis = duration.as_millis() as i32;
        let mut cb = |resolve: Function, reject: Function| {
            let _ = match global().set_timeout(&resolve, millis) {
                Ok(i32) => resolve.call1(&JsValue::NULL, &i32.into()),
                Err(err) => reject.call1(&JsValue::NULL, &err),
            };
        };
        JsFuture::from(Promise::new(&mut cb)).map(throw)
    }
}

#[cfg(not(any(feature = "tokio-time", feature = "wasm-time")))]
compile_error!("Either feature \"tokio-time\" or \"wasm-time\" must be enabled for this crate.");
#[cfg(feature = "tokio-time")]
pub use tokio_impl::*;
#[cfg(all(not(feature = "tokio-time"), feature = "wasm-time"))]
pub use wasm_impl::*;
