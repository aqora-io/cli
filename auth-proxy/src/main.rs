use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use aqora_auth_proxy::{sig_header_name, sign, SigningKey};
use axum::{
    extract::{Request, State},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use clap::Parser;
use http::{HeaderName, HeaderValue, StatusCode};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Parser, Debug)]
#[command(name = "auth-proxy", version, about = "Signing reverse proxy")]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    host: IpAddr,
    #[arg(long, default_value_t = 7777)]
    port: u16,
    /// Header to inject before signing, e.g. -H "Authorization: Bearer ..."
    #[arg(short = 'H', long = "header", value_parser = parse_header)]
    header: Vec<(HeaderName, HeaderValue)>,
    /// PEM file containing a PKCS#8 Ed25519 private key
    key: PathBuf,
    /// Destination origin to forward to, e.g. https://aqora.io
    to: url::Url,
}

fn parse_header(s: &str) -> Result<(HeaderName, HeaderValue), String> {
    let (n, v) = s.split_once(':').ok_or("expected NAME:VALUE")?;
    let name = HeaderName::try_from(n.trim()).map_err(|e| e.to_string())?;
    let value = HeaderValue::try_from(v.trim()).map_err(|e| e.to_string())?;
    Ok((name, value))
}

#[derive(Clone)]
struct AppState {
    key: Arc<SigningKey>,
    to: Arc<url::Url>,
    extra_headers: Arc<Vec<(HeaderName, HeaderValue)>>,
    upstream: reqwest::Client,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    let pem_str = std::fs::read_to_string(&args.key)
        .with_context(|| format!("reading key file {}", args.key.display()))?;
    let key = SigningKey::from_pkcs8_pem(&pem_str).context("parsing PEM key")?;

    if args.to.cannot_be_a_base() || args.to.host_str().is_none() {
        anyhow::bail!("`to` must be an absolute URL with a host, e.g. https://aqora.io");
    }

    let state = AppState {
        key: Arc::new(key),
        to: Arc::new(args.to),
        extra_headers: Arc::new(args.header),
        upstream: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
    };

    let app = Router::new().fallback(any(handler)).with_state(state);

    let addr = SocketAddr::from((args.host, args.port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("auth-proxy listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

enum ProxyError {
    SigAlreadyPresent,
    Sign(aqora_auth_proxy::Error),
    InvalidUpstreamUri(String),
    Upstream(reqwest::Error),
}

impl From<aqora_auth_proxy::Error> for ProxyError {
    fn from(e: aqora_auth_proxy::Error) -> Self {
        match e {
            aqora_auth_proxy::Error::SignatureAlreadyPresent => Self::SigAlreadyPresent,
            other => Self::Sign(other),
        }
    }
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        match self {
            Self::SigAlreadyPresent => (
                StatusCode::BAD_REQUEST,
                "X-Auth-Proxy-Sig already present on incoming request\n",
            )
                .into_response(),
            Self::Sign(e) => {
                tracing::error!(error = %e, "signing failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("signing error: {e}\n"),
                )
                    .into_response()
            }
            Self::InvalidUpstreamUri(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid upstream URI: {msg}\n"),
            )
                .into_response(),
            Self::Upstream(e) => {
                tracing::warn!(error = %e, "upstream request failed");
                (StatusCode::BAD_GATEWAY, format!("upstream error: {e}\n")).into_response()
            }
        }
    }
}

async fn handler(State(state): State<AppState>, req: Request) -> Result<Response, ProxyError> {
    if req.headers().contains_key(sig_header_name()) {
        return Err(ProxyError::SigAlreadyPresent);
    }

    let (mut parts, body) = req.into_parts();

    let mut target = (*state.to).clone();
    let pq = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let (path, query) = match pq.find('?') {
        Some(idx) => (&pq[..idx], Some(&pq[idx + 1..])),
        None => (pq, None),
    };
    target.set_path(path);
    target.set_query(query);

    parts.uri = http::Uri::try_from(target.as_str())
        .map_err(|e| ProxyError::InvalidUpstreamUri(e.to_string()))?;

    let host_header = match target.port() {
        Some(p) => format!("{}:{}", target.host_str().unwrap_or(""), p),
        None => target.host_str().unwrap_or("").to_string(),
    };
    if let Ok(hv) = HeaderValue::try_from(host_header) {
        parts.headers.insert(http::header::HOST, hv);
    }

    for (name, value) in state.extra_headers.iter() {
        parts.headers.insert(name.clone(), value.clone());
    }

    let mutated = Request::from_parts(parts, body);
    let signed = sign(mutated, &state.key).await?;

    let (parts, body_bytes) = signed.into_parts();
    let req_for_reqwest = http::Request::from_parts(parts, reqwest::Body::from(body_bytes));
    let reqwest_req = reqwest::Request::try_from(req_for_reqwest)
        .map_err(|e| ProxyError::InvalidUpstreamUri(e.to_string()))?;

    let response = state
        .upstream
        .execute(reqwest_req)
        .await
        .map_err(ProxyError::Upstream)?;

    let status = response.status();
    let version = response.version();
    let headers = response.headers().clone();
    let body = axum::body::Body::from_stream(response.bytes_stream());

    let mut resp = Response::new(body);
    *resp.status_mut() = status;
    *resp.version_mut() = version;
    *resp.headers_mut() = headers;

    Ok(resp)
}
