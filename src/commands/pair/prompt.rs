use std::io::Write;
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use url::Url;

use crate::error::Result;

use super::target::PairEditor;

/// POSIX single-quoting, so a quote or a space in a URL or a path cannot change
/// how the agent's shell parses the command it is handed.
fn quote(value: impl std::fmt::Display) -> String {
    format!("'{}'", value.to_string().replace('\'', r"'\''"))
}

/// `session` is the one session to target, when there is exactly one; the
/// scripts resolve it themselves otherwise.
pub fn build_prompt(
    editor: &PairEditor,
    token_path: &Path,
    editor_page: &Url,
    session: Option<&str>,
) -> String {
    let session_flag = session
        .map(|id| format!(" --session {}", quote(id)))
        .unwrap_or_default();
    let execute_cmd = format!(
        "execute-code.sh --url {}{session_flag}",
        quote(&editor.base_url)
    );
    // The id is current now, but marimo renames a session when the browser
    // reconnects, so the agent needs a way out.
    let session_hint = if session.is_some() {
        " If the script reports the session id is stale, drop --session and try again."
    } else {
        ""
    };
    format!(
        "Use the /marimo-pair skill to pair-program on a running marimo notebook.

Connect to the notebook at: {base_url}

Use `{execute_cmd}` from the marimo-pair skill to execute code in the notebook.

An auth token is stored at {token_path}. Pass it via `{execute_cmd} \
--token \"$(cat {quoted_token_path})\"`.

The notebook must be open in a browser for a session to exist. If the server reports no \
active sessions, ask the user to open {editor_page} and then try again.{session_hint}

Once you are connected, send a fun toast (mo.status.toast(...)) to the user inside marimo \
letting them know you're ready to pair.",
        base_url = editor.base_url,
        token_path = token_path.display(),
        quoted_token_path = quote(token_path.display()),
        editor_page = editor_page,
    )
}

/// Keep the token out of the prompt text (and so out of shell history and the
/// agent's transcript) by handing the agent a path instead, the way
/// `marimo pair prompt --with-token` does.
///
/// The token lives as long as the returned directory: dropping it takes the
/// token with it once the agent has exited. A caller that prints the prompt
/// rather than launching an agent must leak the directory with `into_path`,
/// since the agent it is printed for outlives this process.
pub fn write_token(token: &str) -> Result<(TempDir, PathBuf)> {
    let dir = tempfile::Builder::new().prefix("aqora-pair-").tempdir()?;
    make_private_dir(dir.path())?;
    let path = dir.path().join("token.txt");
    let mut file = open_private(&path)?;
    file.write_all(token.as_bytes())?;
    file.sync_all()?;
    Ok((dir, path))
}

/// The umask decides the temp dir's mode, so narrow it explicitly rather than
/// relying on the token file's own 0600 alone.
#[cfg(unix)]
fn make_private_dir(dir: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    Ok(std::fs::set_permissions(
        dir,
        std::fs::Permissions::from_mode(0o700),
    )?)
}

#[cfg(not(unix))]
fn make_private_dir(_dir: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn open_private(path: &Path) -> Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    Ok(std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?)
}

#[cfg(not(unix))]
fn open_private(path: &Path) -> Result<std::fs::File> {
    // The containing directory is already private to the user on Windows.
    Ok(std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn editor(base_url: &str) -> PairEditor {
        PairEditor {
            base_url: Url::parse(base_url).unwrap(),
            token: "unused".into(),
            phase: "READY".into(),
            editor_page_id: "workspace-id".into(),
        }
    }

    fn editor_page() -> Url {
        Url::parse("https://aqora.io/workspaces/workspace-id/edit").unwrap()
    }

    #[test]
    fn commands_carry_a_quoted_url_and_token_path() {
        let prompt = build_prompt(
            &editor("http://host/runner/it's/"),
            Path::new("/tmp/it's dir/token.txt"),
            &editor_page(),
            None,
        );

        assert!(
            prompt.contains(r#"--url 'http://host/runner/it'\''s/'"#),
            "{prompt}"
        );
        assert!(
            prompt.contains(r#"cat '/tmp/it'\''s dir/token.txt'"#),
            "{prompt}"
        );
        assert!(!prompt.contains("--session"), "{prompt}");
    }

    #[test]
    fn commands_carry_the_session_when_it_is_known() {
        let prompt = build_prompt(
            &editor("http://host/runner/abc/"),
            Path::new("/tmp/token.txt"),
            &editor_page(),
            Some("s_1"),
        );

        // Both the bare command and the one with the token target the session.
        assert_eq!(prompt.matches("--session 's_1'").count(), 2, "{prompt}");
        assert!(prompt.contains("stale"), "{prompt}");
    }

    #[test]
    fn the_token_file_is_removed_when_its_handle_drops() {
        let (dir, path) = write_token("s3cret").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "s3cret");

        drop(dir);

        assert!(!path.exists(), "{} outlived its handle", path.display());
    }

    #[test]
    fn a_leaked_token_file_outlives_its_handle() {
        let (dir, path) = write_token("s3cret").unwrap();

        let _ = dir.into_path();

        assert!(path.exists(), "{} was removed", path.display());
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
