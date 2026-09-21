//! Short-lived S3 credentials for the caller's aqora storage, minted through
//! `createStoreCredentials` and shared by the CLI commands and the Python
//! bindings; [`engine`] adds the cache and signed object requests behind
//! `aqora.Store` and `aqora.KV`.

mod engine;

use chrono::{SecondsFormat, Utc};
use graphql_client::GraphQLQuery;
use serde::Serialize;
use url::Url;

pub use engine::*;

use crate::{
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
};

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/create_store_credentials.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct CreateStoreCredentials;

/// SigV4 credentials for the caller's bucket on the aqora object store.
#[derive(Debug, Clone, Serialize)]
pub struct StoreCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub expires_at: DateTime,
    /// Origin without a trailing slash; objects live at `{endpoint}/{bucket}/{key}`.
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
}

impl From<create_store_credentials::CreateStoreCredentialsCreateStoreCredentials>
    for StoreCredentials
{
    fn from(creds: create_store_credentials::CreateStoreCredentialsCreateStoreCredentials) -> Self {
        Self {
            access_key_id: creds.access_key_id,
            secret_access_key: creds.secret_access_key,
            expires_at: creds.expires_at,
            endpoint: creds.endpoint,
            bucket: creds.bucket,
            region: creds.region,
        }
    }
}

impl StoreCredentials {
    /// `false` for a plain-http endpoint (local development), `true` otherwise.
    pub fn use_ssl(&self) -> bool {
        !self.endpoint.starts_with("http://")
    }

    /// `host[:port]` of the endpoint, the form DuckDB's `ENDPOINT` takes.
    pub fn host(&self) -> Result<String> {
        let invalid = |detail: &str| {
            error::system(
                &format!("Invalid store endpoint {}: {detail}", self.endpoint),
                "Please contact support if the problem persists.",
            )
        };
        let url = Url::parse(&self.endpoint).map_err(|err| invalid(&err.to_string()))?;
        let host = url.host_str().ok_or_else(|| invalid("no host"))?;
        Ok(match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_owned(),
        })
    }

    /// RFC 3339 with a `+00:00` offset rather than `Z`, which Python 3.10's
    /// `datetime.fromisoformat` cannot parse.
    pub fn expires_at_rfc3339(&self) -> String {
        self.expires_at
            .with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Secs, false)
    }

    /// A `CREATE OR REPLACE SECRET` statement registering the bucket with DuckDB.
    pub fn duckdb_sql(&self, name: &str) -> Result<String> {
        let quote = |value: &str| value.replace('\'', "''");
        Ok(format!(
            "CREATE OR REPLACE SECRET {name} (\n    \
                TYPE s3,\n    \
                KEY_ID '{}',\n    \
                SECRET '{}',\n    \
                ENDPOINT '{}',\n    \
                REGION '{}',\n    \
                URL_STYLE 'path',\n    \
                USE_SSL {},\n    \
                SCOPE 's3://{}/'\n\
            );",
            quote(&self.access_key_id),
            quote(&self.secret_access_key),
            quote(&self.host()?),
            quote(&self.region),
            self.use_ssl(),
            quote(&self.bucket),
        ))
    }
}

pub async fn create_store_credentials(
    client: &GraphQLClient,
    duration_secs: Option<i64>,
) -> Result<StoreCredentials> {
    let data = client
        .send::<CreateStoreCredentials>(create_store_credentials::Variables {
            input: Some(create_store_credentials::CreateStoreCredentialsInput { duration_secs }),
        })
        .await?;
    Ok(data.create_store_credentials.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn creds(endpoint: &str) -> StoreCredentials {
        StoreCredentials {
            access_key_id: "AQS1abc".into(),
            secret_access_key: "secret".into(),
            expires_at: Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap(),
            endpoint: endpoint.into(),
            bucket: "alice".into(),
            region: "aqora".into(),
        }
    }

    #[test]
    fn host_keeps_an_explicit_port_only() {
        assert_eq!(
            creds("http://100.123.32.73:3001").host().unwrap(),
            "100.123.32.73:3001"
        );
        assert_eq!(creds("https://s3.aqora.io").host().unwrap(), "s3.aqora.io");
    }

    #[test]
    fn only_plain_http_disables_ssl() {
        assert!(!creds("http://100.123.32.73:3001").use_ssl());
        assert!(creds("https://s3.aqora.io").use_ssl());
    }

    #[test]
    fn expiry_uses_a_numeric_offset() {
        assert_eq!(
            creds("https://s3.aqora.io").expires_at_rfc3339(),
            "2026-01-02T03:04:05+00:00"
        );
    }

    #[test]
    fn duckdb_sql_quotes_values() {
        let creds = StoreCredentials {
            secret_access_key: "se cret'".into(),
            ..creds("http://100.123.32.73:3001")
        };
        assert_eq!(
            creds.duckdb_sql("mine").unwrap(),
            "CREATE OR REPLACE SECRET mine (\n    TYPE s3,\n    KEY_ID 'AQS1abc',\n    \
             SECRET 'se cret''',\n    ENDPOINT '100.123.32.73:3001',\n    REGION 'aqora',\n    \
             URL_STYLE 'path',\n    USE_SSL false,\n    SCOPE 's3://alice/'\n);"
        );
    }
}
