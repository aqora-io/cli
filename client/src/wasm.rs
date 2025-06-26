use web_sys::{
    js_sys::Function,
    wasm_bindgen::{JsCast, JsValue},
    Window, WorkerGlobalScope,
};

pub enum GlobalScope {
    Window(Window),
    Worker(WorkerGlobalScope),
}

pub fn global() -> GlobalScope {
    match web_sys::js_sys::global().dyn_into::<Window>() {
        Ok(window) => GlobalScope::Window(window),
        Err(scope) => match scope.dyn_into::<WorkerGlobalScope>() {
            Ok(worker) => GlobalScope::Worker(worker),
            Err(_) => panic!("Global scope is neither Window nor WorkerGlobalScope"),
        },
    }
}

impl GlobalScope {
    #[cfg(feature = "wasm-time")]
    pub fn set_timeout(&self, handler: &Function, millis: i32) -> Result<i32, JsValue> {
        match self {
            Self::Window(window) => {
                window.set_timeout_with_callback_and_timeout_and_arguments_0(handler, millis)
            }
            Self::Worker(worker) => {
                worker.set_timeout_with_callback_and_timeout_and_arguments_0(handler, millis)
            }
        }
    }
}
