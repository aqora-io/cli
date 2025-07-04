use std::fmt;

use serde::{Deserialize, Serialize};
use tower::Layer;
use tower_http::trace::{
    DefaultMakeSpan, DefaultOnBodyChunk, DefaultOnEos, DefaultOnFailure, HttpMakeClassifier,
    OnRequest, OnResponse, Trace,
};
use tracing::Level;

use crate::http::{Body, NormalizeHttpService, Request};

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct GraphQLQuery {
    query: String,
    operation_name: Option<String>,
    variables: Option<serde_json::Value>,
    extensions: Option<serde_json::Value>,
    #[serde(flatten)]
    rest: Option<serde_json::Value>,
}

fn safe_display_json(json: &serde_json::Value) -> String {
    if let Ok(str) = serde_json::to_string_pretty(json) {
        str
    } else {
        format!("{json:?}")
    }
}

impl fmt::Display for GraphQLQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.operation_name {
            writeln!(f, "[name] {name}")?;
        }
        write!(f, "[query] {}", self.query)?;
        if let Some(variables) = &self.variables {
            write!(f, "\n[variables] {}", safe_display_json(variables))?;
        }
        if let Some(extensions) = &self.extensions {
            write!(f, "\n[extensions] {}", safe_display_json(extensions))?;
        }
        if let Some(rest) = &self.rest {
            if rest.as_object().is_none_or(|object| !object.is_empty()) {
                write!(f, "\n[rest] {}", safe_display_json(rest))?;
            }
        }
        Ok(())
    }
}

enum DisplayBody<'a> {
    Bytes(&'a [u8]),
    String(&'a str),
    Json(serde_json::Value),
    GraphQL(GraphQLQuery),
    Stream,
}

impl fmt::Display for DisplayBody<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes(bytes) => write!(f, "Bytes({})", bytes.len()),
            Self::String(string) => write!(f, "String({})", string),
            Self::Json(json) => write!(f, "Json({})", safe_display_json(json)),
            Self::GraphQL(graphql) => write!(f, "GraphQL({})", graphql),
            Self::Stream => write!(f, "Stream"),
        }
    }
}

impl<'a> From<&'a Body> for DisplayBody<'a> {
    fn from(body: &'a Body) -> Self {
        if let Some(bytes) = body.as_bytes() {
            if let Ok(string) = std::str::from_utf8(bytes) {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(string) {
                    if let Ok(graphql) = serde_json::from_value(json.clone()) {
                        DisplayBody::GraphQL(graphql)
                    } else {
                        DisplayBody::Json(json)
                    }
                } else {
                    DisplayBody::String(string)
                }
            } else {
                DisplayBody::Bytes(bytes)
            }
        } else {
            DisplayBody::Stream
        }
    }
}

macro_rules! trace_dynamic {
    ($lvl:expr, $($tt:tt)*) => {
        match $lvl {
            tracing::Level::TRACE => tracing::event!(tracing::Level::TRACE, $($tt)*),
            tracing::Level::DEBUG => tracing::event!(tracing::Level::DEBUG, $($tt)*),
            tracing::Level::INFO => tracing::event!(tracing::Level::INFO, $($tt)*),
            tracing::Level::WARN => tracing::event!(tracing::Level::WARN, $($tt)*),
            tracing::Level::ERROR => tracing::event!(tracing::Level::ERROR, $($tt)*),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TraceRequest {
    level: Level,
    debug_body: bool,
}

impl TraceRequest {
    #[inline]
    pub fn new() -> Self {
        Self {
            level: Level::DEBUG,
            debug_body: false,
        }
    }

    #[inline]
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }

    #[inline]
    pub fn debug_body(mut self, debug: bool) -> Self {
        self.debug_body = debug;
        self
    }
}

impl Default for TraceRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl OnRequest<Body> for TraceRequest {
    fn on_request(&mut self, request: &Request, _: &tracing::Span) {
        trace_dynamic!(
            self.level,
            "request started: method={} uri={} {}",
            request.method(),
            request.uri(),
            if self.debug_body {
                format!("body={}", DisplayBody::from(request.body()))
            } else {
                format!(
                    "size={}",
                    request
                        .body()
                        .content_length()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "unknown".to_string())
                )
            }
        )
    }
}

#[derive(Clone, Debug)]
pub struct TraceResponse {
    level: Level,
}

impl TraceResponse {
    #[inline]
    pub fn new() -> Self {
        Self {
            level: Level::DEBUG,
        }
    }

    #[inline]
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }
}

impl Default for TraceResponse {
    fn default() -> Self {
        Self::new()
    }
}

impl<B> OnResponse<B> for TraceResponse {
    fn on_response(
        self,
        response: &http::Response<B>,
        latency: std::time::Duration,
        _: &tracing::Span,
    ) {
        trace_dynamic!(
            self.level,
            status = ?response.status(),
            latency = ?latency,
            "response {} in {:?}",
            response.status(),
            latency
        );
    }
}

#[derive(Clone, Debug)]
pub struct TraceLayer {
    level: Level,
    debug_body: bool,
}

impl TraceLayer {
    #[inline]
    pub fn new() -> Self {
        Self {
            level: Level::DEBUG,
            debug_body: false,
        }
    }

    #[inline]
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }

    #[inline]
    pub fn debug_body(mut self, debug: bool) -> Self {
        self.debug_body = debug;
        self
    }
}

impl Default for TraceLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for TraceLayer {
    type Service = NormalizeHttpService<
        Trace<
            S,
            HttpMakeClassifier,
            DefaultMakeSpan,
            TraceRequest,
            TraceResponse,
            DefaultOnBodyChunk,
            DefaultOnEos,
            DefaultOnFailure,
        >,
    >;
    fn layer(&self, inner: S) -> Self::Service {
        NormalizeHttpService::new(
            Trace::new_for_http(inner)
                .make_span_with(DefaultMakeSpan::new().level(self.level))
                .on_request(
                    TraceRequest::new()
                        .level(self.level)
                        .debug_body(self.debug_body),
                )
                .on_response(TraceResponse::new().level(self.level))
                .on_body_chunk(DefaultOnBodyChunk::new())
                .on_eos(DefaultOnEos::new().level(self.level))
                .on_failure(DefaultOnFailure::new().level(self.level)),
        )
    }
}
