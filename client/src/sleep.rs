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

    use std::future::Future;
    use std::pin::Pin;
    use std::task::{ready, Context, Poll};

    use wasm_bindgen_futures::JsFuture;
    use web_sys::{
        js_sys::{Function, Promise},
        wasm_bindgen::{JsValue, UnwrapThrowExt},
    };

    pub struct Sleep {
        future: JsFuture,
        timeout: Option<i32>,
    }

    impl Future for Sleep {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let result = ready!(Pin::new(&mut self.future).poll(cx));
            self.timeout = None;
            result.unwrap_throw();
            Poll::Ready(())
        }
    }

    impl Drop for Sleep {
        fn drop(&mut self) {
            if let Some(timeout) = self.timeout.take() {
                global().clear_timeout(timeout);
            }
        }
    }

    pub fn sleep(duration: Duration) -> Sleep {
        let millis = duration.as_millis().min(i32::MAX as u128) as i32;
        let mut timeout = None;
        let mut cb =
            |resolve: Function, reject: Function| match global().set_timeout(&resolve, millis) {
                Ok(handle) => timeout = Some(handle),
                Err(err) => {
                    let _ = reject.call1(&JsValue::NULL, &err);
                }
            };
        let future = JsFuture::from(Promise::new(&mut cb));
        Sleep { future, timeout }
    }
}

#[cfg(not(any(feature = "tokio-time", feature = "wasm-time")))]
compile_error!("Either feature \"tokio-time\" or \"wasm-time\" must be enabled for this crate.");
#[cfg(feature = "tokio-time")]
pub use tokio_impl::*;
#[cfg(all(not(feature = "tokio-time"), feature = "wasm-time"))]
pub use wasm_impl::*;
