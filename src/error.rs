use axum::{
    body::Body,
    response::{IntoResponse, Response},
};

human_errors::error_shim!(Error);

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<url::ParseError> for Error {
    fn from(e: url::ParseError) -> Self {
        user(&format!("Invalid URL provided: {e}"), "")
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        system(&format!("I/O error: {e}"), "")
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        system(
            &format!("Error sending request to aqora: {e}"),
            "Check your internet connection",
        )
    }
}

impl From<toml::de::Error> for Error {
    fn from(e: toml::de::Error) -> Self {
        user(&format!("Error parsing toml: {e}"), "")
    }
}

impl From<toml::ser::Error> for Error {
    fn from(e: toml::ser::Error) -> Self {
        system(&format!("Error serializing toml: {e}"), "")
    }
}

impl From<toml_edit::TomlError> for Error {
    fn from(e: toml_edit::TomlError) -> Self {
        user(
            &format!("Error parsing toml: {e}"),
            "Please make sure the file is valid toml",
        )
    }
}

impl From<reqwest::header::InvalidHeaderValue> for Error {
    fn from(e: reqwest::header::InvalidHeaderValue) -> Self {
        system(&format!("Invalid header value: {e}"), "")
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        system(&format!("Error parsing JSON: {e}"), "")
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(e: tokio::sync::oneshot::error::RecvError) -> Self {
        system(&format!("Error receiving oneshot: {e}"), "")
    }
}

impl From<dialoguer::Error> for Error {
    fn from(e: dialoguer::Error) -> Self {
        system(&format!("Error with dialog: {e}"), "")
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response<Body> {
        let body = Body::new(format!("{}", self));
        Response::builder()
            .status(if self.is_user() { 400 } else { 500 })
            .header("Content-Type", "text/plain")
            .body(body)
            .unwrap()
    }
}

// macro_rules! bail_system {
//     ($message:expr, $advice:expr) => {
//         return Err(system($message, $advice).into());
//     };
// }
// pub(crate) use bail_system;

// macro_rules! bail_user {
//     ($message:expr, $advice:expr) => {
//         return Err(user($message, $advice).into());
//     };
// }
// pub(crate) use bail_user;
