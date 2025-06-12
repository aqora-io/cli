use std::future::Future;
use std::io;
use std::time::Duration;

#[cfg(feature = "tokio-time")]
mod tokio_sleep {
    use super::*;
    use futures::FutureExt;

    pub fn sleep(duration: Duration) -> impl Future<Output = io::Result<()>> {
        tokio::time::sleep(duration).map(Ok)
    }
}

#[cfg(feature = "wasm-time")]
mod wasm_sleep {
    use super::*;
    use crate::wasm::global;

    use futures::FutureExt;
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{
        js_sys::{Function, Promise},
        wasm_bindgen::JsValue,
    };

    fn js_to_io_result(result: Result<JsValue, JsValue>) -> io::Result<()> {
        result.map(|_| ()).map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "JavaScript error: {}",
                    err.as_string()
                        .unwrap_or_else(|| "Unknown error".to_string())
                ),
            )
        })
    }

    pub fn sleep(duration: Duration) -> impl Future<Output = io::Result<()>> {
        let millis = duration.as_millis() as i32;
        let mut cb = |resolve: Function, reject: Function| {
            let _ = match global().set_timeout(&resolve, millis) {
                Ok(i32) => resolve.call1(&JsValue::NULL, &i32.into()),
                Err(err) => reject.call1(&JsValue::NULL, &err),
            };
        };
        JsFuture::from(Promise::new(&mut cb)).map(js_to_io_result)
    }
}

#[cfg(not(any(feature = "tokio-time", feature = "wasm-time")))]
compile_error!("Either feature \"tokio-time\" or \"wasm-time\" must be enabled for this crate.");
#[cfg(all(not(feature = "wasm-time"), feature = "tokio-time"))]
pub use tokio_sleep::sleep;
#[cfg(feature = "wasm-time")]
pub use wasm_sleep::sleep;
