use std::time::Duration;

#[cfg(feature = "tokio-time")]
mod std_instant {
    use super::*;

    pub struct StdInstant(std::time::Instant);

    impl StdInstant {
        pub fn now() -> Self {
            Self(std::time::Instant::now())
        }

        pub fn elapsed(&self) -> Duration {
            self.0.elapsed()
        }
    }
}

#[cfg(feature = "wasm-time")]
mod wasm_instant {
    use super::*;

    use crate::wasm::global;

    pub struct WasmInstant(Duration);

    impl WasmInstant {
        pub fn now() -> Self {
            let millis = if let Some(performance) = global().performance() {
                performance.now()
            } else {
                web_sys::js_sys::Date::now()
            };
            Self(Duration::from_secs_f64(millis * 1000.))
        }

        pub fn elapsed(&self) -> Duration {
            let now = Self::now();
            self.0.saturating_sub(now.0)
        }
    }
}

#[cfg(not(any(feature = "tokio-time", feature = "wasm-time")))]
compile_error!("Either feature \"tokio-time\" or \"wasm-time\" must be enabled for this crate.");
#[cfg(feature = "tokio-time")]
pub type Instant = std_instant::StdInstant;
#[cfg(all(feature = "wasm-time", not(feature = "tokio-time")))]
pub type Instant = wasm_instant::WasmInstant;
