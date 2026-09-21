use clap::{Args, ValueEnum};
use serde::Serialize;

use crate::{
    commands::GlobalArgs,
    error::Result,
    store::{create_store_credentials, StoreCredentials},
};

#[derive(Debug, Serialize, ValueEnum, Clone, Copy)]
pub enum CredentialsFormat {
    /// A JSON object with snake_case keys
    Json,
    /// `export` lines for `eval`, read by object_store, the AWS SDKs and the AWS CLI
    Env,
    /// The JSON an AWS `credential_process` must print
    AwsProcess,
    /// A `CREATE SECRET` statement for DuckDB
    Duckdb,
}

#[derive(Args, Debug, Serialize)]
#[command(
    author,
    version,
    about = "Mint short-lived S3 credentials for your aqora storage and print them on stdout"
)]
pub struct Credentials {
    #[arg(
        long,
        value_parser = clap::value_parser!(i64).range(60..),
        help = "Lifetime in seconds, at least 60. The server clamps the maximum and picks its default when omitted."
    )]
    duration: Option<i64>,
    #[arg(value_enum, long, default_value_t = CredentialsFormat::Json)]
    format: CredentialsFormat,
}

pub async fn credentials(args: Credentials, global: GlobalArgs) -> Result<()> {
    let client = global.graphql_client().await?;
    let creds = create_store_credentials(&client, args.duration).await?;
    println!("{}", render_credentials(args.format, &creds)?);
    Ok(())
}

pub fn render_credentials(format: CredentialsFormat, creds: &StoreCredentials) -> Result<String> {
    Ok(match format {
        CredentialsFormat::Json => serde_json::to_string_pretty(creds)?,
        CredentialsFormat::Env => render_env(creds),
        CredentialsFormat::AwsProcess => render_aws_process(creds)?,
        CredentialsFormat::Duckdb => render_duckdb(creds)?,
    })
}

/// `AWS_ENDPOINT`, `AWS_BUCKET`, `AWS_ALLOW_HTTP` and
/// `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` are object_store's names; the AWS SDKs
/// and CLI read `AWS_ENDPOINT_URL`, and botocore only honours
/// `AWS_DEFAULT_REGION` for the region. A session token left over from an
/// earlier AWS login would be sent alongside these keys, so it is unset.
fn render_env(creds: &StoreCredentials) -> String {
    let mut vars = vec![
        ("AWS_ACCESS_KEY_ID", creds.access_key_id.as_str()),
        ("AWS_SECRET_ACCESS_KEY", creds.secret_access_key.as_str()),
        ("AWS_REGION", creds.region.as_str()),
        ("AWS_DEFAULT_REGION", creds.region.as_str()),
        ("AWS_ENDPOINT", creds.endpoint.as_str()),
        ("AWS_ENDPOINT_URL", creds.endpoint.as_str()),
        ("AWS_BUCKET", creds.bucket.as_str()),
        ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false"),
    ];
    if !creds.use_ssl() {
        vars.push(("AWS_ALLOW_HTTP", "true"));
    }
    std::iter::once("unset AWS_SESSION_TOKEN".to_owned())
        .chain(
            vars.into_iter()
                .map(|(name, value)| format!("export {name}={}", shell_words::quote(value))),
        )
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_aws_process(creds: &StoreCredentials) -> Result<String> {
    Ok(serde_json::to_string(&serde_json::json!({
        "Version": 1,
        "AccessKeyId": creds.access_key_id,
        "SecretAccessKey": creds.secret_access_key,
        "Expiration": creds.expires_at_rfc3339(),
    }))?)
}

fn render_duckdb(creds: &StoreCredentials) -> Result<String> {
    creds.duckdb_sql("aqora")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn creds(endpoint: &str) -> StoreCredentials {
        StoreCredentials {
            access_key_id: "AQS1abc".into(),
            secret_access_key: "se cret'".into(),
            expires_at: Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap(),
            endpoint: endpoint.into(),
            bucket: "alice".into(),
            region: "aqora".into(),
        }
    }

    #[test]
    fn env_exports_every_variable_and_quotes_values() {
        let rendered = render_env(&creds("http://100.123.32.73:3001"));
        assert_eq!(
            rendered,
            "unset AWS_SESSION_TOKEN\n\
             export AWS_ACCESS_KEY_ID=AQS1abc\n\
             export AWS_SECRET_ACCESS_KEY='se cret'\\'''\n\
             export AWS_REGION=aqora\n\
             export AWS_DEFAULT_REGION=aqora\n\
             export AWS_ENDPOINT=http://100.123.32.73:3001\n\
             export AWS_ENDPOINT_URL=http://100.123.32.73:3001\n\
             export AWS_BUCKET=alice\n\
             export AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false\n\
             export AWS_ALLOW_HTTP=true"
        );
        assert!(!render_env(&creds("https://s3.aqora.io")).contains("AWS_ALLOW_HTTP"));
    }

    #[test]
    fn aws_process_follows_the_credential_process_contract() {
        let rendered = render_aws_process(&creds("https://s3.aqora.io")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(parsed["Version"], 1);
        assert_eq!(parsed["AccessKeyId"], "AQS1abc");
        assert_eq!(parsed["SecretAccessKey"], "se cret'");
        assert_eq!(parsed["Expiration"], "2026-01-02T03:04:05+00:00");
    }

    #[test]
    fn duckdb_secret_uses_the_host_and_escapes_quotes() {
        let rendered = render_duckdb(&creds("http://100.123.32.73:3001")).unwrap();
        assert!(rendered.starts_with("CREATE OR REPLACE SECRET aqora ("));
        assert!(rendered.contains("SECRET 'se cret''',"));
        assert!(rendered.contains("ENDPOINT '100.123.32.73:3001',"));
        assert!(rendered.contains("USE_SSL false,"));
        assert!(rendered.contains("SCOPE 's3://alice/'"));
        assert!(render_duckdb(&creds("https://s3.aqora.io"))
            .unwrap()
            .contains("USE_SSL true,"));
    }

    #[test]
    fn json_uses_snake_case_keys() {
        let rendered =
            render_credentials(CredentialsFormat::Json, &creds("https://s3.aqora.io")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(parsed["access_key_id"], "AQS1abc");
        assert_eq!(parsed["expires_at"], "2026-01-02T03:04:05Z");
        assert_eq!(parsed["bucket"], "alice");
    }
}
