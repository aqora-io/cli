use std::{
    io::Write,
    path::{Path, PathBuf},
};

use clap::Args;
use serde::Serialize;

use crate::{
    commands::GlobalArgs,
    error::{self, Result},
    store::create_store_credentials,
};

#[derive(Args, Debug, Serialize)]
#[command(
    author,
    version,
    about = "Add a profile to your AWS config that fetches aqora storage credentials on demand"
)]
pub struct ConfigureAws {
    #[arg(long, default_value = "aqora", help = "Name of the profile to write")]
    profile: String,
    #[arg(
        long,
        env = "AWS_CONFIG_FILE",
        help = "AWS config file to edit (default: ~/.aws/config)"
    )]
    config_file: Option<PathBuf>,
    #[arg(
        long,
        help = "aqora executable for credential_process (default: `aqora` on PATH, else this executable)"
    )]
    aqora_bin: Option<PathBuf>,
}

pub async fn configure_aws(args: ConfigureAws, global: GlobalArgs) -> Result<()> {
    let aqora_bin = resolve_aqora_bin(args.aqora_bin)?;
    let path = match args.config_file {
        Some(path) => path,
        None => default_config_file()?,
    };
    // Write through a symlinked config rather than replacing the link.
    let path = dunce::canonicalize(&path).unwrap_or(path);
    let client = global.graphql_client().await?;
    // Minted only to learn the endpoint and region and to fail early when not
    // logged in; the secret itself is never written anywhere.
    let creds = create_store_credentials(&client, None).await?;
    let command = credential_process_command(
        &aqora_bin,
        &global.url,
        global.config_home.as_deref(),
        global.allow_insecure_host,
    );
    let header = profile_header(&args.profile);
    let body = profile_body(&creds.region, &creds.endpoint, &command);
    let existing = match tokio::fs::read_to_string(&path).await {
        Ok(existing) => existing,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(err) => {
            return Err(error::system(
                &format!("Could not read {}: {err}", path.display()),
                "Check the file's permissions and try again.",
            ))
        }
    };
    write_atomically(&path, &upsert_profile(&existing, &header, &body))?;
    println!("Wrote profile '{}' to {}", args.profile, path.display());
    println!(
        "Try it: aws --profile {} s3 ls s3://{}/",
        args.profile, creds.bucket
    );
    Ok(())
}

/// `--aqora-bin`, else `aqora` on PATH, else the running executable. PATH comes
/// first because under the Python wheel the running executable is the
/// interpreter rather than the `aqora` entry point. Symlinks are kept as they
/// are: package managers point them at versioned targets that move on upgrade.
fn resolve_aqora_bin(explicit: Option<PathBuf>) -> Result<PathBuf> {
    let bin = match explicit.or_else(|| which::which("aqora").ok()) {
        Some(bin) => bin,
        None => std::env::current_exe().map_err(|err| {
            error::system(
                &format!("Could not locate the aqora executable: {err}"),
                "Pass --aqora-bin with the path to aqora.",
            )
        })?,
    };
    let invalid = |detail: String| {
        error::user(
            &format!("Could not resolve {}: {detail}", bin.display()),
            "Pass --aqora-bin with the path to an existing aqora executable.",
        )
    };
    if !bin.is_file() {
        return Err(invalid("not a file".to_owned()));
    }
    std::path::absolute(&bin).map_err(|err| invalid(err.to_string()))
}

/// The command botocore runs for the profile. `--url` is always explicit
/// because the AWS tooling's environment will not carry `AQORA_URL`.
fn credential_process_command(
    aqora_bin: &Path,
    url: &str,
    config_home: Option<&Path>,
    allow_insecure_host: bool,
) -> String {
    let mut words = vec![
        aqora_bin.to_string_lossy().into_owned(),
        "--url".to_owned(),
        url.to_owned(),
    ];
    if let Some(config_home) = config_home {
        words.push("--config-home".to_owned());
        words.push(config_home.to_string_lossy().into_owned());
    }
    if allow_insecure_host {
        words.push("--allow-insecure-host".to_owned());
    }
    words.extend(
        ["store", "credentials", "--format", "aws-process"]
            .into_iter()
            .map(str::to_owned),
    );
    join_command(words, cfg!(windows))
}

/// botocore splits `credential_process` with `shlex` on POSIX but with the
/// Windows C runtime rules on Windows, where double quotes are the only
/// quoting and single quotes are literal.
fn join_command(words: Vec<String>, windows: bool) -> String {
    if !windows {
        return shell_words::join(words);
    }
    words
        .into_iter()
        .map(|word| {
            if word.is_empty() || word.contains([' ', '\t', '"']) {
                format!("\"{}\"", word.replace('"', "\\\""))
            } else {
                word
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn profile_header(profile: &str) -> String {
    if profile == "default" {
        "[default]".to_owned()
    } else {
        format!("[profile {profile}]")
    }
}

fn profile_body(region: &str, endpoint: &str, command: &str) -> String {
    format!(
        "region = {region}\n\
         endpoint_url = {endpoint}\n\
         credential_process = {command}\n\
         s3 =\n    \
             addressing_style = path"
    )
}

/// Replace the section under `header` with `body`, or append it, leaving every
/// other line untouched.
pub fn upsert_profile(existing: &str, header: &str, body: &str) -> String {
    let lines: Vec<&str> = existing.lines().collect();
    let mut out: Vec<&str> = Vec::with_capacity(lines.len() + 8);
    match lines.iter().position(|line| line.trim() == header) {
        Some(start) => {
            let end = lines[start + 1..]
                .iter()
                .position(|line| line.trim_start().starts_with('['))
                .map_or(lines.len(), |offset| start + 1 + offset);
            out.extend(&lines[..start]);
            out.push(header);
            out.extend(body.lines());
            if end < lines.len() {
                out.push("");
                out.extend(&lines[end..]);
            }
        }
        None => {
            out.extend(&lines);
            if out.last().is_some_and(|last| !last.trim().is_empty()) {
                out.push("");
            }
            out.push(header);
            out.extend(body.lines());
        }
    }
    let mut result = out.join("\n");
    result.push('\n');
    result
}

fn default_config_file() -> Result<PathBuf> {
    dirs::home_dir()
        .map(|home| home.join(".aws").join("config"))
        .ok_or_else(|| {
            error::system(
                "Could not determine your home directory",
                "Pass --config-file with the path to your AWS config.",
            )
        })
}

fn write_atomically(path: &Path, contents: &str) -> Result<()> {
    let io_error = |err: std::io::Error| {
        error::system(
            &format!("Could not write {}: {err}", path.display()),
            "Check the file's permissions and try again.",
        )
    };
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).map_err(io_error)?;
    let mut file = tempfile::NamedTempFile::new_in(parent).map_err(io_error)?;
    file.write_all(contents.as_bytes()).map_err(io_error)?;
    file.persist(path).map_err(|err| io_error(err.error))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &str = "region = aqora\nendpoint_url = http://example:3001";

    #[test]
    fn replaces_an_existing_section_and_keeps_the_rest() {
        let existing = "[default]\nregion = us-east-1\n\n[profile aqora]\nregion = old\n# stale comment\n\n[profile other]\nregion = eu-west-1\n";
        let updated = upsert_profile(existing, "[profile aqora]", BODY);
        assert_eq!(
            updated,
            "[default]\nregion = us-east-1\n\n[profile aqora]\nregion = aqora\nendpoint_url = http://example:3001\n\n[profile other]\nregion = eu-west-1\n"
        );
    }

    #[test]
    fn replaces_a_trailing_section() {
        let updated = upsert_profile("[profile aqora]\nregion = old\n", "[profile aqora]", BODY);
        assert_eq!(
            updated,
            "[profile aqora]\nregion = aqora\nendpoint_url = http://example:3001\n"
        );
    }

    #[test]
    fn appends_when_missing_and_handles_empty_input() {
        assert_eq!(
            upsert_profile("[default]\nregion = us-east-1\n", "[profile aqora]", BODY),
            "[default]\nregion = us-east-1\n\n[profile aqora]\nregion = aqora\nendpoint_url = http://example:3001\n"
        );
        assert_eq!(
            upsert_profile("", "[profile aqora]", BODY),
            "[profile aqora]\nregion = aqora\nendpoint_url = http://example:3001\n"
        );
    }

    #[test]
    fn default_profile_uses_the_bare_header() {
        assert_eq!(profile_header("default"), "[default]");
        assert_eq!(profile_header("aqora"), "[profile aqora]");
    }

    #[test]
    fn credential_process_quotes_paths_and_passes_the_url() {
        assert_eq!(
            credential_process_command(
                Path::new("/opt/my tools/aqora"),
                "https://aqora.io",
                Some(Path::new("/home/me/.config/aqora")),
                true,
            ),
            "'/opt/my tools/aqora' --url https://aqora.io --config-home /home/me/.config/aqora --allow-insecure-host store credentials --format aws-process"
        );
    }

    #[test]
    fn windows_command_uses_double_quotes_only_where_needed() {
        let words = |items: &[&str]| items.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            join_command(
                words(&[
                    r"C:\Program Files\aqora\aqora.exe",
                    "--url",
                    "https://aqora.io"
                ]),
                true
            ),
            r#""C:\Program Files\aqora\aqora.exe" --url https://aqora.io"#
        );
        assert_eq!(
            join_command(words(&[r"C:\Users\me\.local\bin\aqora.exe", "store"]), true),
            r"C:\Users\me\.local\bin\aqora.exe store"
        );
    }
}
