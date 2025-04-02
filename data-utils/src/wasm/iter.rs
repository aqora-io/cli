use futures::prelude::*;
use wasm_bindgen::prelude::*;

use crate::error::Result;

#[wasm_bindgen]
pub struct IteratorNext {
    done: bool,
    value: JsValue,
}

#[wasm_bindgen]
impl IteratorNext {
    #[wasm_bindgen(getter)]
    pub fn done(&self) -> bool {
        self.done
    }

    #[wasm_bindgen(getter)]
    pub fn value(&self) -> JsValue {
        self.value.clone()
    }
}

impl<E> TryFrom<Option<Result<JsValue, E>>> for IteratorNext {
    type Error = E;
    fn try_from(value: Option<Result<JsValue, E>>) -> Result<Self, Self::Error> {
        if let Some(value) = value {
            Ok(IteratorNext {
                done: false,
                value: value?,
            })
        } else {
            Ok(IteratorNext {
                done: true,
                value: JsValue::UNDEFINED,
            })
        }
    }
}

#[wasm_bindgen(js_name = "ValueIter")]
pub struct JsValueIter(Box<dyn Iterator<Item = Result<JsValue>>>);

#[wasm_bindgen(js_class = "ValueIter")]
impl JsValueIter {
    #[wasm_bindgen(js_name = "next")]
    pub fn js_next(&mut self) -> Result<IteratorNext> {
        IteratorNext::try_from(self.0.next())
    }
}

pub fn iterable<I>(iter: I) -> Result<js_sys::Object>
where
    I: Iterator<Item = Result<JsValue>> + 'static,
{
    let closure = Closure::once_into_js(move || JsValueIter(Box::new(iter)));
    let object = js_sys::Object::new();
    js_sys::Reflect::set(&object, &js_sys::Symbol::iterator(), &closure)?;
    Ok(object)
}

#[wasm_bindgen(js_name = "ValueStream")]
pub struct JsValueStream(Box<dyn Stream<Item = Result<JsValue>> + Unpin>);

#[wasm_bindgen(js_class = "ValueStream")]
impl JsValueStream {
    #[wasm_bindgen(js_name = "next")]
    pub async fn js_next(&mut self) -> Result<IteratorNext> {
        IteratorNext::try_from(self.0.next().await)
    }
}

pub fn async_iterable<S>(stream: S) -> Result<js_sys::Object>
where
    S: Stream<Item = Result<JsValue>> + Unpin + 'static,
{
    let closure = Closure::once_into_js(move || JsValueStream(Box::new(stream)));
    let object = js_sys::Object::new();
    js_sys::Reflect::set(&object, &js_sys::Symbol::async_iterator(), &closure)?;
    Ok(object)
}
