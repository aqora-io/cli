//! AWS Signature Version 4 for the aqora store gateway, which verifies
//! header-signed requests with `s3s`. Only what the store needs: no query
//! parameters, single-chunk payloads, and the canonical URI taken verbatim
//! from a path already encoded with [`uri_encode`].
#![cfg_attr(not(feature = "extension-module"), allow(dead_code))]

use chrono::{DateTime, Utc};
use ring::{digest, hmac};
use url::Url;

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const SERVICE: &str = "s3";

pub struct SigningInput<'a> {
    pub method: &'a str,
    pub url: &'a Url,
    /// Headers to sign beyond `host`, `x-amz-content-sha256` and `x-amz-date`.
    pub extra_headers: &'a [(&'a str, &'a str)],
    /// Lowercase hex SHA-256 of the payload.
    pub payload_hash: &'a str,
    pub access_key_id: &'a str,
    pub secret_access_key: &'a str,
    pub region: &'a str,
    pub now: DateTime<Utc>,
}

pub struct Signature {
    pub amz_date: String,
    pub authorization: String,
}

/// Percent-encode a path the way SigV4 canonicalises it: unreserved bytes and
/// `/` pass through, everything else becomes uppercase `%XX`.
pub fn uri_encode(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for byte in path.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

pub fn sha256_hex(data: &[u8]) -> String {
    hex(digest::digest(&digest::SHA256, data).as_ref())
}

/// The `Host` header value reqwest sends for `url`: the port only when explicit.
pub fn host_header(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    }
}

pub fn sign(input: SigningInput<'_>) -> Signature {
    let amz_date = input.now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = &amz_date[..8];
    let host = host_header(input.url);

    let mut headers: Vec<(String, &str)> = input
        .extra_headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim()))
        .collect();
    headers.push(("host".into(), &host));
    headers.push(("x-amz-content-sha256".into(), input.payload_hash));
    headers.push(("x-amz-date".into(), &amz_date));
    headers.sort();

    let signed_headers = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers = headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        input.method,
        input.url.path(),
        input.url.query().unwrap_or_default(),
        canonical_headers,
        signed_headers,
        input.payload_hash
    );

    let scope = format!("{date}/{}/{SERVICE}/aws4_request", input.region);
    let string_to_sign = format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    let mut key = hmac_sha256(
        format!("AWS4{}", input.secret_access_key).as_bytes(),
        date.as_bytes(),
    );
    for part in [input.region, SERVICE, "aws4_request"] {
        key = hmac_sha256(&key, part.as_bytes());
    }
    let signature = hex(&hmac_sha256(&key, string_to_sign.as_bytes()));

    Signature {
        authorization: format!(
            "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            input.access_key_id
        ),
        amz_date,
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
        .as_ref()
        .to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn encodes_everything_but_unreserved_bytes_and_slashes() {
        assert_eq!(
            uri_encode("/alice/path/to/item.json"),
            "/alice/path/to/item.json"
        );
        assert_eq!(uri_encode("/a b/é~x"), "/a%20b/%C3%A9~x");
    }

    /// The GET example from "Signature Calculations for the Authorization
    /// Header: Transferring Payload in a Single Chunk" in the S3 API reference.
    #[test]
    fn matches_the_aws_documented_example() {
        let url = Url::parse("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let signature = sign(SigningInput {
            method: "GET",
            url: &url,
            extra_headers: &[("Range", "bytes=0-9")],
            payload_hash: &sha256_hex(b""),
            access_key_id: "AKIAIOSFODNN7EXAMPLE",
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region: "us-east-1",
            now: Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap(),
        });
        assert_eq!(signature.amz_date, "20130524T000000Z");
        assert_eq!(
            signature.authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn host_header_keeps_only_explicit_ports() {
        assert_eq!(
            host_header(&Url::parse("http://100.123.32.73:3001/x").unwrap()),
            "100.123.32.73:3001"
        );
        assert_eq!(
            host_header(&Url::parse("https://s3.aqora.io/x").unwrap()),
            "s3.aqora.io"
        );
    }
}
