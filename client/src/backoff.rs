use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::time::Duration;

use backoff::default::{
    INITIAL_INTERVAL_MILLIS, MAX_ELAPSED_TIME_MILLIS, MAX_INTERVAL_MILLIS, MULTIPLIER,
    RANDOMIZATION_FACTOR,
};
use backoff::exponential::{ExponentialBackoff, ExponentialBackoffBuilder};
use backoff::Clock;

pub use backoff::backoff::Backoff;
pub use backoff::SystemClock;

pub type DefaultBackoffFactory = ExponentialBackoffFactory<SystemClock>;

pub(crate) trait Sleeper {
    type Future: Future<Output = io::Result<()>>;
    fn sleep(&self, duration: Duration) -> Self::Future;
}

#[cfg(feature = "tokio-time")]
mod tokio_sleep {
    use super::*;
    use futures::FutureExt;

    #[derive(Default)]
    pub struct TokioSleeper;

    impl Sleeper for TokioSleeper {
        type Future = futures::future::Map<tokio::time::Sleep, fn(()) -> io::Result<()>>;
        #[inline]
        fn sleep(&self, duration: Duration) -> Self::Future {
            tokio::time::sleep(duration).map(Ok)
        }
    }
}

#[cfg(feature = "wasm-time")]
mod wasm_sleep {
    use super::*;
    use futures::FutureExt;
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{
        js_sys::{global, Error, Function, Promise},
        wasm_bindgen::{JsCast, JsValue},
        Window, WorkerGlobalScope,
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

    #[derive(Default)]
    pub struct WasmSleeper;

    impl Sleeper for WasmSleeper {
        type Future =
            futures::future::Map<JsFuture, fn(Result<JsValue, JsValue>) -> io::Result<()>>;
        #[inline]
        fn sleep(&self, duration: Duration) -> Self::Future {
            let millis = duration.as_millis() as i32;
            let mut cb = |resolve: Function, reject: Function| {
                let scope = global();
                let res = if let Some(scope) = scope.dyn_ref::<Window>() {
                    scope.set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, millis)
                } else if let Some(scope) = scope.dyn_ref::<WorkerGlobalScope>() {
                    scope.set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, millis)
                } else {
                    Err(Error::new("Global scope is neither Window nor WorkerGlobalScope").into())
                };
                let _ = match res {
                    Ok(i32) => resolve.call1(&JsValue::NULL, &i32.into()),
                    Err(err) => reject.call1(&JsValue::NULL, &err),
                };
            };
            JsFuture::from(Promise::new(&mut cb)).map(js_to_io_result)
        }
    }
}

#[cfg(not(any(feature = "tokio-time", feature = "wasm-time")))]
compile_error!("Either feature \"tokio-time\" or \"wasm-time\" must be enabled for this crate.");
#[cfg(feature = "tokio-time")]
type SysSleeper = tokio_sleep::TokioSleeper;
#[cfg(all(feature = "wasm-time", not(feature = "tokio-time")))]
type SysSleeper = wasm_sleep::WasmSleeper;

pub(crate) type SleepFuture = <SysSleeper as Sleeper>::Future;

pub(crate) fn sleep_next<B>(backoff: &mut B) -> Option<SleepFuture>
where
    B: Backoff,
{
    backoff
        .next_backoff()
        .map(|duration| SysSleeper::default().sleep(duration))
}

pub trait BackoffFactory {
    type Backoff: Backoff;
    fn create(&self) -> Self::Backoff;
}

impl<T> BackoffFactory for &T
where
    T: ?Sized + BackoffFactory,
{
    type Backoff = T::Backoff;
    fn create(&self) -> Self::Backoff {
        T::create(self)
    }
}

#[derive(Debug, Clone)]
pub struct ExponentialBackoffFactory<C> {
    initial_interval: Duration,
    randomization_factor: f64,
    multiplier: f64,
    max_interval: Duration,
    max_elapsed_time: Option<Duration>,
    clock: PhantomData<C>,
}

impl<C> Default for ExponentialBackoffFactory<C> {
    fn default() -> Self {
        Self {
            initial_interval: Duration::from_millis(INITIAL_INTERVAL_MILLIS),
            randomization_factor: RANDOMIZATION_FACTOR,
            multiplier: MULTIPLIER,
            max_interval: Duration::from_millis(MAX_INTERVAL_MILLIS),
            max_elapsed_time: Some(Duration::from_millis(MAX_ELAPSED_TIME_MILLIS)),
            clock: PhantomData,
        }
    }
}

impl<C> ExponentialBackoffFactory<C> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn system() -> ExponentialBackoffFactory<SystemClock> {
        ExponentialBackoffFactory::<SystemClock>::new()
    }

    pub fn with_initial_interval(self, initial_interval: Duration) -> Self {
        Self {
            initial_interval,
            ..self
        }
    }
    pub fn with_randomization_factor(self, randomization_factor: f64) -> Self {
        Self {
            randomization_factor,
            ..self
        }
    }
    pub fn with_multiplier(self, multiplier: f64) -> Self {
        Self { multiplier, ..self }
    }
    pub fn with_max_interval(self, max_interval: Duration) -> Self {
        Self {
            max_interval,
            ..self
        }
    }
    pub fn with_max_elapsed_time(self, max_elapsed_time: Option<Duration>) -> Self {
        Self {
            max_elapsed_time,
            ..self
        }
    }
}

impl<C> BackoffFactory for ExponentialBackoffFactory<C>
where
    C: Clock + Default,
{
    type Backoff = ExponentialBackoff<C>;
    fn create(&self) -> Self::Backoff {
        ExponentialBackoffBuilder::<C>::new()
            .with_initial_interval(self.initial_interval)
            .with_randomization_factor(self.randomization_factor)
            .with_multiplier(self.multiplier)
            .with_max_interval(self.max_interval)
            .with_max_elapsed_time(self.max_elapsed_time)
            .build()
    }
}
