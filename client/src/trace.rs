use std::borrow::Cow;
use std::fmt;

use async_trait::async_trait;
use reqwest::{header::HeaderMap, Body, Request, Response};
use tracing::Level;

use crate::instant::Instant;
use crate::middleware::{Middleware, MiddlewareError, Next};

enum BodyDebug<'a> {
    Bytes(&'a [u8]),
    String(&'a str),
    Stream,
}

impl fmt::Debug for BodyDebug<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes(bytes) => write!(f, "Bytes({})", bytes.len()),
            Self::String(string) => write!(f, "String({})", string),
            Self::Stream => write!(f, "Stream"),
        }
    }
}

impl<'a> From<&'a Body> for BodyDebug<'a> {
    fn from(body: &'a Body) -> Self {
        if let Some(bytes) = body.as_bytes() {
            if let Ok(string) = std::str::from_utf8(bytes) {
                BodyDebug::String(string)
            } else {
                BodyDebug::Bytes(bytes)
            }
        } else {
            BodyDebug::Stream
        }
    }
}

fn strip_sensitive(headers: &HeaderMap) -> Cow<HeaderMap> {
    let mut headers = Cow::Borrowed(headers);
    for header in [
        "authorization",
        "x-amz-security-token",
        "x-access-token",
        "x-refresh-token",
        "cookie",
    ] {
        if headers.contains_key(header) {
            headers.to_mut().remove(header);
        }
    }
    headers
}

pub struct DebugMiddleware;

#[cfg_attr(feature = "threaded", async_trait)]
#[cfg_attr(not(feature = "threaded"), async_trait(?Send))]
impl Middleware for DebugMiddleware {
    async fn handle(&self, req: Request, next: Next<'_>) -> Result<Response, MiddlewareError> {
        let debug_body = req.body().map(BodyDebug::from);
        let span = tracing::span!(
            Level::DEBUG,
            "request",
            method = %req.method(),
            url = %req.url(),
            headers = ?strip_sensitive(req.headers()),
            body = ?debug_body
        );
        let _enter = span.enter();
        tracing::event!(
            Level::DEBUG,
            "started {} {} {:?}",
            req.method(),
            req.url(),
            debug_body
        );
        let instant = Instant::now();
        match next.handle(req).await {
            Ok(res) => {
                let elapsed = instant.elapsed();
                tracing::event!(
                    Level::DEBUG,
                    status = ?res.status(),
                    headers = ?strip_sensitive(res.headers()),
                    content_len = ?res.content_length(),
                    "finished in {:?}: {} {:?} bytes",
                    elapsed,
                    res.status(),
                    res.content_length()
                );
                Ok(res)
            }
            Err(err) => {
                tracing::event!(
                    Level::WARN,
                    err = ?err,
                    "An error occured while processing request: {err}"
                );
                Err(err)
            }
        }
    }
}

#[cfg(feature = "ws")]
mod ws {
    use super::*;
    use crate::middleware::{WsMiddleware, WsMiddlewareError, WsNext};
    use crate::ws::{Websocket, WsRequest, WsResponse};

    #[cfg_attr(feature = "threaded", async_trait)]
    #[cfg_attr(not(feature = "threaded"), async_trait(?Send))]
    impl WsMiddleware for DebugMiddleware {
        async fn handle(
            &self,
            req: WsRequest,
            next: WsNext<'_>,
        ) -> Result<(Websocket, WsResponse), WsMiddlewareError> {
            let span = tracing::span!(
                Level::DEBUG,
                "request",
                method = %req.method(),
                url = %req.uri(),
                headers = ?strip_sensitive(req.headers()),
            );
            let _enter = span.enter();
            tracing::event!(Level::DEBUG, "started {} {}", req.method(), req.uri(),);
            let instant = Instant::now();
            match next.handle(req).await {
                Ok((websocket, res)) => {
                    let elapsed = instant.elapsed();
                    tracing::event!(
                        Level::DEBUG,
                        status = ?res.status(),
                        headers = ?strip_sensitive(res.headers()),
                        "finished in {:?}: {}",
                        elapsed,
                        res.status(),
                    );
                    Ok((websocket, res))
                }
                Err(err) => {
                    tracing::event!(
                        Level::WARN,
                        err = ?err,
                        "An error occured while processing request: {err}"
                    );
                    Err(err)
                }
            }
        }
    }
}
