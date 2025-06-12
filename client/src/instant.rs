use std::time::Duration;

#[cfg(not(feature = "wasm-instant"))]
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

#[cfg(feature = "wasm-instant")]
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

#[cfg(not(feature = "wasm-instant"))]
pub type Instant = std_instant::StdInstant;
#[cfg(feature = "wasm-instant")]
pub type Instant = wasm_instant::WasmInstant;
