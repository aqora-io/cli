use base64::prelude::*;
use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, CONTENT_LENGTH};

use crate::checksum::{Checksum, ChecksumAlgorithm};

impl From<ChecksumAlgorithm> for HeaderName {
    fn from(algo: ChecksumAlgorithm) -> HeaderName {
        use ChecksumAlgorithm::*;
        match algo {
            Crc32 => HeaderName::from_static("x-amz-checksum-crc32"),
        }
    }
}

pub async fn upload(
    client: &reqwest::Client,
    url: impl reqwest::IntoUrl,
    body: impl Into<Bytes>,
    checksum: impl Checksum,
    headers: HeaderMap,
) -> reqwest::Result<reqwest::Response> {
    let body = body.into();
    let checksum_algo = checksum.algo();
    let checksum = checksum.digest(&body);
    upload_precalculated(client, url, body, checksum_algo, &checksum, headers).await
}

pub(crate) async fn upload_precalculated(
    client: &reqwest::Client,
    url: impl reqwest::IntoUrl,
    body: Bytes,
    checksum_algo: ChecksumAlgorithm,
    checksum: &[u8],
    headers: HeaderMap,
) -> reqwest::Result<reqwest::Response> {
    let checksum = BASE64_STANDARD.encode(checksum);
    client
        .put(url)
        .header(CONTENT_LENGTH, body.len())
        .header(checksum_algo, checksum)
        .headers(headers)
        .body(body)
        .send()
        .await?
        .error_for_status()
}
