use std::time::Duration;

use indicatif::ProgressBar;
use url::Url;

use crate::error::{self, Result};

use super::target::PairEditor;

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const TIMEOUT: Duration = Duration::from_secs(60);
/// Bounds one probe. A runner that accepts a connection and then goes quiet
/// would otherwise hang the poll loop straight through its deadline.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// The runner's `/api/sessions`, which is what the marimo-pair scripts use to
/// find a session. A session only exists while the notebook is open in a
/// browser, and the prompt is useless without one.
pub struct Sessions {
    client: reqwest::Client,
    url: Url,
    token: String,
}

impl Sessions {
    pub fn new(editor: &PairEditor, allow_insecure_host: bool) -> Result<Self> {
        // The runner URL may arrive without a trailing slash, which would make
        // `join` replace its last path segment instead of appending.
        let mut base = editor.base_url.clone();
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(allow_insecure_host)
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        Ok(Self {
            client,
            url: base.join("api/sessions")?,
            token: editor.token.clone(),
        })
    }

    /// The ids of the sessions live right now. A runner that is still starting
    /// refuses connections and answers errors, so anything that is not a clear
    /// answer counts as "none yet" — the caller decides how long to keep asking.
    pub async fn live(&self) -> Vec<String> {
        match self.query().await {
            Ok(live) => live,
            Err(err) => {
                tracing::debug!("Could not read sessions from {}: {err}", self.url);
                Vec::new()
            }
        }
    }

    async fn query(&self) -> Result<Vec<String>> {
        let response = self
            .client
            .get(self.url.clone())
            .bearer_auth(&self.token)
            .send()
            .await?
            .error_for_status()?;
        session_ids(&response.bytes().await?)
    }

    /// Poll until the notebook connects, or give up.
    pub async fn wait(&self, pb: &ProgressBar, editor_page: &Url) -> Result<Vec<String>> {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        loop {
            let live = self.live().await;
            if !live.is_empty() {
                return Ok(live);
            }
            // Give up rather than sleeping through the deadline first.
            if tokio::time::Instant::now() + POLL_INTERVAL >= deadline {
                break;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        pb.finish_and_clear();
        Err(error::user(
            "The notebook never connected",
            &format!(
                "Pairing needs the notebook open in a browser. Open {editor_page} and \
                 try again."
            ),
        ))
    }
}

/// `/api/sessions` answers an object keyed by session id, so an empty object
/// means no notebook is open.
fn session_ids(body: &[u8]) -> Result<Vec<String>> {
    let sessions: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(body)?;
    Ok(sessions.into_iter().map(|(id, _)| id).collect())
}

/// The session the prompt targets: the one asked for, so long as it is live,
/// else the one live session. Several live and none asked for means the
/// notebook is open in more than one tab with no telling which the user means,
/// so the agent picks.
pub fn choose<'a>(asked: Option<&'a str>, live: &'a [String]) -> Result<Option<&'a str>> {
    match (asked, live) {
        (Some(id), _) if live.iter().any(|s| s == id) => Ok(Some(id)),
        (Some(id), _) => Err(error::user(
            &format!("Session {id} is not open on the runner"),
            &format!(
                "The live sessions are: {}. Pass one of those, or drop --session.",
                live.join(", ")
            ),
        )),
        (None, [only]) => Ok(Some(only)),
        (None, _) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A one-shot HTTP server standing in for the runner. Returns the URL it is
    /// serving and the request it received.
    async fn serve_once(body: &'static str) -> (Url, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "http://{}/runner/abc/",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let read = stream.read(&mut buf).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..read]);
            }
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.flush().await.unwrap();
            String::from_utf8_lossy(&request).into_owned()
        });
        (url, handle)
    }

    fn editor(base_url: Url) -> PairEditor {
        PairEditor {
            base_url,
            token: "s3cret".into(),
            phase: "Running".into(),
            editor_page_id: "id".into(),
        }
    }

    #[tokio::test]
    async fn asks_the_runner_for_its_sessions_with_the_token() {
        let (url, served) = serve_once(r#"{"s_1": {"path": "overview.py"}}"#).await;
        let sessions = Sessions::new(&editor(url), false).unwrap();

        assert_eq!(sessions.live().await, ["s_1"]);

        let request = served.await.unwrap().to_lowercase();
        assert!(
            request.contains("get /runner/abc/api/sessions "),
            "{request}"
        );
        assert!(
            request.contains("authorization: bearer s3cret"),
            "{request}"
        );
    }

    /// A runner that accepts the connection and then goes quiet would otherwise
    /// hang the poll loop past its deadline, forever.
    #[tokio::test]
    async fn gives_up_on_a_runner_that_never_answers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "http://{}/runner/abc/",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        // Hold the connection open rather than dropping it, which would answer
        // the request with a reset.
        let _silent = tokio::spawn(async move {
            let _connection = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let sessions = Sessions::new(&editor(url), false).unwrap();

        let live = tokio::time::timeout(Duration::from_secs(15), sessions.live())
            .await
            .expect("live never gave up");

        assert!(live.is_empty());
    }

    #[tokio::test]
    async fn nothing_is_live_when_the_runner_reports_no_sessions() {
        let (url, _served) = serve_once("{}").await;
        let sessions = Sessions::new(&editor(url), false).unwrap();

        assert!(sessions.live().await.is_empty());
    }

    #[test]
    fn no_session_ids_when_the_map_is_empty() {
        assert!(session_ids(b"{}").unwrap().is_empty());
    }

    #[test]
    fn session_ids_are_the_maps_keys() {
        let body = br#"{"s_1234": {"path": "/notebooks/overview.py"}}"#;
        assert_eq!(session_ids(body).unwrap(), ["s_1234"]);
    }

    #[test]
    fn errors_on_a_body_that_is_not_json() {
        assert!(session_ids(b"<html>not marimo</html>").is_err());
    }

    #[test]
    fn the_session_is_known_when_exactly_one_is_live() {
        assert_eq!(choose(None, &["s_1".to_string()]).unwrap(), Some("s_1"));
    }

    #[test]
    fn no_session_is_known_with_none_or_several_live() {
        assert_eq!(choose(None, &[]).unwrap(), None);
        let two = ["s_1".to_string(), "s_2".to_string()];
        assert_eq!(choose(None, &two).unwrap(), None);
    }

    #[test]
    fn an_asked_for_session_is_used_when_it_is_live() {
        let two = ["s_1".to_string(), "s_2".to_string()];
        assert_eq!(choose(Some("s_2"), &two).unwrap(), Some("s_2"));
    }

    #[test]
    fn an_asked_for_session_that_is_not_live_is_an_error() {
        let err = choose(Some("s_9"), &["s_1".to_string()]).unwrap_err();
        assert!(err.is_user());
        assert!(err.to_string().contains("s_9"), "{err}");
        assert!(err.to_string().contains("s_1"), "{err}");
    }
}
