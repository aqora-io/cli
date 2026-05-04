use base64::prelude::*;
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request};
use http_body_util::BodyExt;
use ring::signature::KeyPair;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub const SIG_HEADER_NAME_STR: &str = "x-auth-proxy-sig";

pub fn sig_header_name() -> HeaderName {
    HeaderName::from_static(SIG_HEADER_NAME_STR)
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("X-Auth-Proxy-Sig header already present")]
    SignatureAlreadyPresent,
    #[error("missing X-Auth-Proxy-Sig header")]
    SignatureMissing,
    #[error("invalid signature header: {0}")]
    SignatureMalformed(&'static str),
    #[error("signature verification failed")]
    SignatureInvalid,
    #[error("body collect error: {0}")]
    Body(#[source] BoxError),
    #[error("invalid PEM: {0}")]
    Pem(String),
    #[error("invalid PKCS#8 Ed25519 key")]
    InvalidKey,
}

pub struct SigningKey(ring::signature::Ed25519KeyPair);

impl SigningKey {
    pub fn from_pkcs8_pem(pem_str: &str) -> Result<Self, Error> {
        let parsed = pem::parse(pem_str).map_err(|e| Error::Pem(e.to_string()))?;
        if parsed.tag() != "PRIVATE KEY" {
            return Err(Error::Pem(format!(
                "expected `PRIVATE KEY` (PKCS#8) tag, got `{}`",
                parsed.tag()
            )));
        }
        Self::from_pkcs8_der(parsed.contents())
    }

    pub fn from_pkcs8_der(der: &[u8]) -> Result<Self, Error> {
        ring::signature::Ed25519KeyPair::from_pkcs8_maybe_unchecked(der)
            .map(SigningKey)
            .map_err(|_| Error::InvalidKey)
    }

    pub fn verifying_key(&self) -> VerifyingKey {
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(self.0.public_key().as_ref());
        VerifyingKey(bytes)
    }
}

#[derive(Clone)]
pub struct VerifyingKey([u8; 32]);

impl VerifyingKey {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

fn is_excluded(name: &HeaderName) -> bool {
    let s = name.as_str();
    s == SIG_HEADER_NAME_STR || s == "host" || s == "content-length" || HOP_BY_HOP.contains(&s)
}

/// Build a deterministic byte representation of the request that can be signed
/// or verified. Header names are already lowercase per the `http` crate's
/// `HeaderName` invariant; we sort by `(name, value)` so multi-value headers
/// inserted in any order produce identical output.
///
/// Excluded from the signed set: `x-auth-proxy-sig`, `host`, `content-length`,
/// and standard hop-by-hop headers.
//
// NOTE: We roll our own simple format rather than RFC 9421 (HTTP Message
// Signatures); revisit if interop with other implementations becomes a
// requirement.
pub fn canonicalize(method: &Method, target: &str, headers: &HeaderMap, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + body.len());
    out.extend_from_slice(method.as_str().as_bytes());
    out.push(b' ');
    out.extend_from_slice(target.as_bytes());
    out.push(b'\n');

    let mut pairs: Vec<(&[u8], &[u8])> = headers
        .iter()
        .filter(|(name, _)| !is_excluded(name))
        .map(|(name, value)| (name.as_str().as_bytes(), value.as_bytes()))
        .collect();
    pairs.sort();
    for (name, value) in pairs {
        out.extend_from_slice(name);
        out.extend_from_slice(b": ");
        out.extend_from_slice(value);
        out.push(b'\n');
    }

    out.push(b'\n');
    out.extend_from_slice(body);
    out
}

/// Sign a request, attaching the `X-Auth-Proxy-Sig` header. The body is
/// buffered into memory so it can be both signed and forwarded; the returned
/// request carries the body as `Bytes`.
///
/// Errors with `Error::SignatureAlreadyPresent` if the header is already set.
pub async fn sign<B>(req: Request<B>, key: &SigningKey) -> Result<Request<Bytes>, Error>
where
    B: http_body::Body,
    B::Error: Into<BoxError>,
{
    let sig_name = sig_header_name();
    if req.headers().contains_key(&sig_name) {
        return Err(Error::SignatureAlreadyPresent);
    }
    let (mut parts, body) = req.into_parts();
    let body_bytes = body
        .collect()
        .await
        .map_err(|e| Error::Body(e.into()))?
        .to_bytes();

    let target = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let canonical = canonicalize(&parts.method, target, &parts.headers, &body_bytes);

    let signature = key.0.sign(&canonical);
    let encoded = BASE64_URL_SAFE_NO_PAD.encode(signature.as_ref());
    let value =
        HeaderValue::try_from(encoded).expect("base64-url-safe is always a valid header value");
    parts.headers.insert(sig_name, value);

    Ok(Request::from_parts(parts, body_bytes))
}

/// Verify a signed request. Returns the request with the body buffered as
/// `Bytes` and the signature header preserved.
pub async fn verify<B>(req: Request<B>, key: &VerifyingKey) -> Result<Request<Bytes>, Error>
where
    B: http_body::Body,
    B::Error: Into<BoxError>,
{
    let sig_name = sig_header_name();
    let sig_value = req
        .headers()
        .get(&sig_name)
        .ok_or(Error::SignatureMissing)?
        .clone();
    let sig_str = sig_value
        .to_str()
        .map_err(|_| Error::SignatureMalformed("not ASCII"))?;
    let sig_bytes = BASE64_URL_SAFE_NO_PAD
        .decode(sig_str)
        .map_err(|_| Error::SignatureMalformed("not base64url-no-pad"))?;

    let (mut parts, body) = req.into_parts();
    let body_bytes = body
        .collect()
        .await
        .map_err(|e| Error::Body(e.into()))?
        .to_bytes();

    parts.headers.remove(&sig_name);

    let target = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let canonical = canonicalize(&parts.method, target, &parts.headers, &body_bytes);

    let public_key =
        ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, key.0.as_ref());
    public_key
        .verify(&canonical, &sig_bytes)
        .map_err(|_| Error::SignatureInvalid)?;

    parts.headers.insert(sig_name, sig_value);

    Ok(Request::from_parts(parts, body_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Full;

    fn fresh_key() -> SigningKey {
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = ring::signature::Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        SigningKey::from_pkcs8_der(pkcs8.as_ref()).unwrap()
    }

    fn req(method: Method, uri: &str, body: &'static [u8]) -> Request<Full<Bytes>> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .header("x-extra", "value")
            .body(Full::new(Bytes::from_static(body)))
            .unwrap()
    }

    fn rebody(req: Request<Bytes>) -> Request<Full<Bytes>> {
        let (parts, body) = req.into_parts();
        Request::from_parts(parts, Full::new(body))
    }

    #[tokio::test]
    async fn round_trip() {
        let key = fresh_key();
        let verifying = key.verifying_key();

        let signed = sign(req(Method::POST, "/foo?x=1", b"hello"), &key)
            .await
            .unwrap();
        assert!(signed.headers().contains_key(SIG_HEADER_NAME_STR));

        verify(rebody(signed), &verifying).await.unwrap();
    }

    #[tokio::test]
    async fn rejects_double_sign() {
        let key = fresh_key();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/x")
            .header(SIG_HEADER_NAME_STR, "already-here")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let err = sign(req, &key).await.unwrap_err();
        assert!(matches!(err, Error::SignatureAlreadyPresent));
    }

    #[tokio::test]
    async fn rejects_missing_sig() {
        let key = fresh_key();
        let err = verify(req(Method::GET, "/x", b""), &key.verifying_key())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::SignatureMissing));
    }

    #[tokio::test]
    async fn rejects_tampered_header() {
        let key = fresh_key();
        let signed = sign(req(Method::POST, "/foo", b"hello"), &key)
            .await
            .unwrap();
        let mut tampered = rebody(signed);
        tampered
            .headers_mut()
            .insert("x-extra", HeaderValue::from_static("tampered"));
        let err = verify(tampered, &key.verifying_key()).await.unwrap_err();
        assert!(matches!(err, Error::SignatureInvalid));
    }

    #[tokio::test]
    async fn rejects_tampered_body() {
        let key = fresh_key();
        let signed = sign(req(Method::POST, "/foo", b"hello"), &key)
            .await
            .unwrap();
        let (parts, _body) = signed.into_parts();
        let tampered = Request::from_parts(parts, Full::new(Bytes::from_static(b"goodbye")));
        let err = verify(tampered, &key.verifying_key()).await.unwrap_err();
        assert!(matches!(err, Error::SignatureInvalid));
    }

    #[test]
    fn canonical_is_order_independent() {
        let mut a = HeaderMap::new();
        a.insert("x-a", HeaderValue::from_static("1"));
        a.insert("x-b", HeaderValue::from_static("2"));
        a.append("x-a", HeaderValue::from_static("3"));

        let mut b = HeaderMap::new();
        b.insert("x-b", HeaderValue::from_static("2"));
        b.insert("x-a", HeaderValue::from_static("1"));
        b.append("x-a", HeaderValue::from_static("3"));

        let target = "/path?q=v";
        let body = b"body";
        assert_eq!(
            canonicalize(&Method::POST, target, &a, body),
            canonicalize(&Method::POST, target, &b, body),
        );
    }

    #[test]
    fn canonical_excludes_unsigned_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("example.com"));
        headers.insert("content-length", HeaderValue::from_static("4"));
        headers.insert("connection", HeaderValue::from_static("close"));
        headers.insert(SIG_HEADER_NAME_STR, HeaderValue::from_static("xxx"));
        headers.insert("x-keep", HeaderValue::from_static("yes"));

        let canonical = canonicalize(&Method::GET, "/", &headers, b"");
        let s = std::str::from_utf8(&canonical).unwrap();
        assert!(s.contains("x-keep: yes"));
        assert!(!s.contains("host:"));
        assert!(!s.contains("content-length:"));
        assert!(!s.contains("connection:"));
        assert!(!s.contains(SIG_HEADER_NAME_STR));
    }
}
