use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct JsDateParseOptions {
    pub date_fmt: Option<String>,
    pub timestamp_fmt: Option<String>,
}
