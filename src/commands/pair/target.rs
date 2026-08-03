use std::str::FromStr;

use graphql_client::GraphQLQuery;
use url::Url;

use crate::{
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
};

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/workspace_pair_editor.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct WorkspacePairEditor;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/workspace_version_pair_editor.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct WorkspaceVersionPairEditor;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/dataset_pair_editor.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct DatasetPairEditor;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/dataset_version_pair_editor.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct DatasetVersionPairEditor;

/// A workspace to pair on, written `owner/slug` with an optional `@version`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairTarget {
    pub owner: String,
    pub slug: String,
    pub version: Option<semver::Version>,
}

const TARGET_ADVICE: &str = "Expected a workspace like: {owner}/{workspace}[@{version}]";

impl FromStr for PairTarget {
    type Err = crate::error::Error;

    fn from_str(input: &str) -> Result<Self> {
        let input = input.strip_prefix('@').unwrap_or(input);
        let (owner, rest) = input
            .split_once('/')
            .ok_or_else(|| error::user("Malformed workspace", TARGET_ADVICE))?;

        // Only an `@` *after* the slash introduces a version, so a leading
        // `@owner` stays part of the owner.
        let (slug, version) = match rest.split_once('@') {
            Some((slug, version)) => {
                let version = semver::Version::parse(version).map_err(|err| {
                    error::user(
                        &format!("Malformed version '{version}': {err}"),
                        "Versions are semver, like: 1.2.3",
                    )
                })?;
                (slug, Some(version))
            }
            None => (rest, None),
        };

        if owner.is_empty() || slug.is_empty() {
            return Err(error::user("Malformed workspace", TARGET_ADVICE));
        }

        Ok(PairTarget {
            owner: owner.to_string(),
            slug: slug.to_string(),
            version,
        })
    }
}

impl std::fmt::Display for PairTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.owner, self.slug)?;
        if let Some(version) = &self.version {
            write!(f, "@{version}")?;
        }
        Ok(())
    }
}

/// Everything the emitted prompt needs about a workspace's editor runner.
#[derive(Debug)]
pub struct PairEditor {
    /// The runner's base URL, with the `access_token` query stripped off.
    pub base_url: Url,
    pub token: String,
    pub phase: String,
    /// Node id of whatever owns the editor, for the `/workspaces/{id}/edit` page.
    pub editor_page_id: String,
}

/// The platform hands back `{host}/runner/{id}/?file=…&access_token=…`; the
/// marimo-pair scripts want the bare base URL plus the token as a header.
fn split_url_and_token(mut url: Url) -> Result<(Url, String)> {
    let token = url
        .query_pairs()
        .find(|(key, _)| key == "access_token")
        .map(|(_, value)| value.into_owned())
        .ok_or_else(|| {
            error::system(
                "Runner URL carried no access_token",
                "The platform returned an unexpected runner URL. Please report this.",
            )
        })?;
    url.set_query(None);
    Ok((url, token))
}

fn no_editor(target: &PairTarget, published: bool) -> crate::error::Error {
    if published {
        error::user(
            &format!("{target} is published and has no editor"),
            "Published versions are read-only. Pair on the workspace's draft version \
             by dropping the @version, or create a new draft.",
        )
    } else {
        error::user(
            &format!("No editor available for {target}"),
            "The version has no editor runner. Open the workspace on aqora.io to start \
             one, then try again.",
        )
    }
}

fn no_draft(target: &PairTarget) -> crate::error::Error {
    error::user(
        &format!("{target} has no draft version"),
        "Pairing edits a workspace's draft version. Create a draft version on aqora.io, \
         then try again.",
    )
}

fn cannot_edit(target: &PairTarget) -> crate::error::Error {
    error::user(
        &format!("You cannot edit {target}"),
        "Pairing needs edit access to the version. Check that you are logged in as a user \
         who can edit this workspace.",
    )
}

pub async fn resolve_editor(
    client: &GraphQLClient,
    target: &PairTarget,
    notebook: Option<String>,
) -> Result<PairEditor> {
    match &target.version {
        Some(version) => resolve_pinned(client, target, version, notebook).await,
        None => resolve_draft(client, target, notebook).await,
    }
}

async fn resolve_pinned(
    client: &GraphQLClient,
    target: &PairTarget,
    version: &semver::Version,
    notebook: Option<String>,
) -> Result<PairEditor> {
    let workspace = client
        .send::<WorkspaceVersionPairEditor>(workspace_version_pair_editor::Variables {
            owner: target.owner.clone(),
            slug: target.slug.clone(),
            version: version.to_string(),
            notebook,
        })
        .await?
        .workspace_by_slug
        .ok_or_else(|| workspace_not_found(target))?;

    let version = workspace.version.ok_or_else(|| {
        error::user(
            &format!("{target} does not exist"),
            &format!(
                "Check the version with 'aqora pair {}/{}' or on aqora.io",
                target.owner, target.slug
            ),
        )
    })?;

    // Published first: a read-only version is a clearer answer than telling
    // someone they cannot edit it.
    if version.published_at.is_some() {
        return Err(no_editor(target, true));
    }
    if !version.viewer_can_edit {
        return Err(cannot_edit(target));
    }

    let editor = version.editor.ok_or_else(|| no_editor(target, false))?;
    let (base_url, token) = split_url_and_token(editor.url)?;

    Ok(PairEditor {
        base_url,
        token,
        phase: format!("{:?}", editor.phase),
        editor_page_id: version.id,
    })
}

async fn resolve_draft(
    client: &GraphQLClient,
    target: &PairTarget,
    notebook: Option<String>,
) -> Result<PairEditor> {
    let workspace = client
        .send::<WorkspacePairEditor>(workspace_pair_editor::Variables {
            owner: target.owner.clone(),
            slug: target.slug.clone(),
            notebook,
        })
        .await?
        .workspace_by_slug
        .ok_or_else(|| workspace_not_found(target))?;

    // Newest draft first, so this is the version the workspace is edited
    // through. A workspace that owns a runner directly is not one of these.
    let draft = workspace
        .versions
        .nodes
        .into_iter()
        .next()
        .ok_or_else(|| no_draft(target))?;

    if !draft.viewer_can_edit {
        return Err(cannot_edit(target));
    }

    let editor = draft.editor.ok_or_else(|| no_editor(target, false))?;
    let (base_url, token) = split_url_and_token(editor.url)?;

    Ok(PairEditor {
        base_url,
        token,
        phase: format!("{:?}", editor.phase),
        editor_page_id: draft.id,
    })
}

fn workspace_not_found(target: &PairTarget) -> crate::error::Error {
    error::user(
        &format!("Workspace {}/{} not found", target.owner, target.slug),
        "Please double check the workspace on aqora.io",
    )
}

fn dataset_not_found(target: &PairTarget) -> crate::error::Error {
    error::user(
        &format!("Dataset {}/{} not found", target.owner, target.slug),
        "Please double check the dataset on aqora.io",
    )
}

/// A dataset is edited through the workspace its version owns.
pub async fn resolve_dataset_editor(
    client: &GraphQLClient,
    target: &PairTarget,
    notebook: Option<String>,
) -> Result<PairEditor> {
    match &target.version {
        Some(version) => resolve_dataset_pinned(client, target, version, notebook).await,
        None => resolve_dataset_draft(client, target, notebook).await,
    }
}

async fn resolve_dataset_pinned(
    client: &GraphQLClient,
    target: &PairTarget,
    version: &semver::Version,
    notebook: Option<String>,
) -> Result<PairEditor> {
    // Datasets are pinned by their three numbers, with nowhere to put the rest
    // of a semver — better to say so than to quietly resolve a different one.
    if !version.pre.is_empty() || !version.build.is_empty() {
        return Err(error::user(
            &format!("Cannot pair on {target}"),
            "Datasets are pinned by major.minor.patch, so a prerelease or build suffix \
             cannot be resolved. Drop the suffix, or drop the @version to use the newest \
             draft.",
        ));
    }

    let dataset = client
        .send::<DatasetVersionPairEditor>(dataset_version_pair_editor::Variables {
            owner: target.owner.clone(),
            local_slug: target.slug.clone(),
            major: version.major as i64,
            minor: version.minor as i64,
            patch: version.patch as i64,
            notebook,
        })
        .await?
        .dataset_by_slug
        .ok_or_else(|| dataset_not_found(target))?;

    let version = dataset.version.ok_or_else(|| {
        error::user(
            &format!("{target} does not exist"),
            &format!(
                "Check the version with 'aqora pair --dataset {}/{}' or on aqora.io",
                target.owner, target.slug
            ),
        )
    })?;

    if version.published_at.is_some() {
        return Err(no_editor(target, true));
    }

    let workspace = version.workspace.ok_or_else(|| no_editor(target, false))?;
    if !workspace.viewer_can_edit {
        return Err(cannot_edit(target));
    }

    let editor = workspace.editor.ok_or_else(|| no_editor(target, false))?;
    let (base_url, token) = split_url_and_token(editor.url)?;

    Ok(PairEditor {
        base_url,
        token,
        phase: format!("{:?}", editor.phase),
        editor_page_id: workspace.id,
    })
}

async fn resolve_dataset_draft(
    client: &GraphQLClient,
    target: &PairTarget,
    notebook: Option<String>,
) -> Result<PairEditor> {
    let dataset = client
        .send::<DatasetPairEditor>(dataset_pair_editor::Variables {
            owner: target.owner.clone(),
            local_slug: target.slug.clone(),
            notebook,
        })
        .await?
        .dataset_by_slug
        .ok_or_else(|| dataset_not_found(target))?;

    let draft = dataset
        .versions
        .nodes
        .into_iter()
        .next()
        .ok_or_else(|| no_draft(target))?;

    let workspace = draft.workspace.ok_or_else(|| no_editor(target, false))?;
    if !workspace.viewer_can_edit {
        return Err(cannot_edit(target));
    }

    let editor = workspace.editor.ok_or_else(|| no_editor(target, false))?;
    let (base_url, token) = split_url_and_token(editor.url)?;

    Ok(PairEditor {
        base_url,
        token,
        phase: format!("{:?}", editor.phase),
        editor_page_id: workspace.id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use aqora_client::ClientOptions;

    use crate::graphql_client::unauthenticated_client;

    fn parse(input: &str) -> PairTarget {
        input.parse().unwrap()
    }

    /// Whether a request has been read in full, so the canned answer is not
    /// written before the query has arrived.
    fn is_complete(request: &[u8]) -> bool {
        let text = String::from_utf8_lossy(request);
        let Some((headers, body)) = text.split_once("\r\n\r\n") else {
            return false;
        };
        let length = headers
            .lines()
            .find_map(|line| {
                line.to_lowercase()
                    .strip_prefix("content-length:")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        body.len() >= length
    }

    /// A one-shot GraphQL server answering with a canned response. Hands back
    /// the request it was sent, so a caller can check what was asked for.
    async fn serve_graphql(
        response: &'static str,
    ) -> (GraphQLClient, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while !is_complete(&request) {
                let read = stream.read(&mut buf).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..read]);
            }
            let body = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{response}",
                response.len()
            );
            stream.write_all(body.as_bytes()).await.unwrap();
            stream.flush().await.unwrap();
            String::from_utf8_lossy(&request).into_owned()
        });
        let client = unauthenticated_client(url, ClientOptions::default()).unwrap();
        (client, handle)
    }

    /// A workspace that owns an editor directly *and* has a draft version, so
    /// the two candidate runners can be told apart.
    const WORKSPACE_WITH_DRAFT: &str = r#"{"data":{"workspaceBySlug":{
        "id": "workspace-id",
        "editor": {"id":"r-workspace","phase":"READY",
                   "url":"http://localhost:8080/runner/workspace/?access_token=workspace-token"},
        "versions":{"nodes":[{"id":"version-id","version":"0.1.0","viewerCanEdit":true,
            "editor":{"id":"r-draft","phase":"READY",
                      "url":"http://localhost:8080/runner/draft/?access_token=draft-token"}}]}
    }}}"#;

    #[tokio::test]
    async fn a_draft_target_uses_the_draft_versions_editor() {
        let (client, _server) = serve_graphql(WORKSPACE_WITH_DRAFT).await;

        let editor = resolve_editor(&client, &parse("alice/ws"), None)
            .await
            .unwrap();

        assert_eq!(
            editor.base_url.as_str(),
            "http://localhost:8080/runner/draft/"
        );
        assert_eq!(editor.token, "draft-token");
        assert_eq!(editor.editor_page_id, "version-id");
    }

    #[tokio::test]
    async fn a_draft_target_errors_when_the_workspace_has_no_draft_version() {
        let (client, _server) = serve_graphql(
            r#"{"data":{"workspaceBySlug":{"id":"workspace-id","versions":{"nodes":[]}}}}"#,
        )
        .await;

        let err = resolve_editor(&client, &parse("alice/ws"), None)
            .await
            .unwrap_err();

        assert!(err.is_user());
        assert!(err.to_string().contains("has no draft version"), "{err}");
        assert!(err.to_string().contains("Create a draft version"), "{err}");
    }

    #[tokio::test]
    async fn a_draft_target_errors_when_the_viewer_cannot_edit_the_draft() {
        let (client, _server) = serve_graphql(
            r#"{"data":{"workspaceBySlug":{"id":"workspace-id","versions":{"nodes":[
                {"id":"version-id","version":"0.1.0","viewerCanEdit":false,
                 "editor":{"id":"r-draft","phase":"READY",
                           "url":"http://localhost:8080/runner/draft/?access_token=draft-token"}}
            ]}}}}"#,
        )
        .await;

        let err = resolve_editor(&client, &parse("alice/ws"), None)
            .await
            .unwrap_err();

        assert!(err.is_user());
        assert!(err.to_string().contains("edit"), "{err}");
    }

    /// A dataset version is edited through the workspace it owns.
    const DATASET_WITH_DRAFT: &str = r#"{"data":{"datasetBySlug":{
        "id": "dataset-id",
        "versions":{"nodes":[{"id":"dataset-version-id","version":"0.1.0","workspace":{
            "id": "workspace-id",
            "viewerCanEdit": true,
            "editor":{"id":"r-dataset","phase":"READY",
                      "url":"http://localhost:8080/runner/dataset/?access_token=dataset-token"}}}]}
    }}}"#;

    #[tokio::test]
    async fn a_dataset_target_uses_the_draft_versions_workspace_editor() {
        let (client, _server) = serve_graphql(DATASET_WITH_DRAFT).await;

        let editor = resolve_dataset_editor(&client, &parse("alice/ds"), None)
            .await
            .unwrap();

        assert_eq!(
            editor.base_url.as_str(),
            "http://localhost:8080/runner/dataset/"
        );
        assert_eq!(editor.token, "dataset-token");
        // The edit page resolves a workspace, not a dataset version.
        assert_eq!(editor.editor_page_id, "workspace-id");
    }

    #[tokio::test]
    async fn a_dataset_target_errors_when_the_dataset_has_no_draft_version() {
        let (client, _server) = serve_graphql(
            r#"{"data":{"datasetBySlug":{"id":"dataset-id","versions":{"nodes":[]}}}}"#,
        )
        .await;

        let err = resolve_dataset_editor(&client, &parse("alice/ds"), None)
            .await
            .unwrap_err();

        assert!(err.is_user());
        assert!(err.to_string().contains("has no draft version"), "{err}");
    }

    #[tokio::test]
    async fn a_dataset_target_errors_when_the_dataset_does_not_exist() {
        let (client, _server) = serve_graphql(r#"{"data":{"datasetBySlug":null}}"#).await;

        let err = resolve_dataset_editor(&client, &parse("alice/ds"), None)
            .await
            .unwrap_err();

        assert!(err.is_user());
        assert!(
            err.to_string().contains("Dataset alice/ds not found"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_dataset_target_errors_when_the_viewer_cannot_edit_the_workspace() {
        let (client, _server) = serve_graphql(
            r#"{"data":{"datasetBySlug":{"id":"dataset-id","versions":{"nodes":[
                {"id":"dataset-version-id","version":"0.1.0","workspace":{
                    "id":"workspace-id","viewerCanEdit":false,
                    "editor":{"id":"r-dataset","phase":"READY",
                              "url":"http://localhost:8080/runner/dataset/?access_token=t"}}}
            ]}}}}"#,
        )
        .await;

        let err = resolve_dataset_editor(&client, &parse("alice/ds"), None)
            .await
            .unwrap_err();

        assert!(err.is_user());
        assert!(err.to_string().contains("edit"), "{err}");
    }

    #[tokio::test]
    async fn a_pinned_dataset_target_rejects_a_prerelease_version() {
        let (client, _server) = serve_graphql(DATASET_WITH_DRAFT).await;

        let err = resolve_dataset_editor(&client, &parse("alice/ds@1.2.3-beta.1"), None)
            .await
            .unwrap_err();

        assert!(err.is_user());
        assert!(err.to_string().contains("prerelease"), "{err}");
    }

    #[tokio::test]
    async fn a_pinned_dataset_target_asks_for_that_version() {
        let (client, served) = serve_graphql(
            r#"{"data":{"datasetBySlug":{"id":"dataset-id","version":{
                "id":"dataset-version-id","version":"1.2.3","publishedAt":null,"workspace":{
                    "id":"workspace-id","viewerCanEdit":true,
                    "editor":{"id":"r-dataset","phase":"READY",
                              "url":"http://localhost:8080/runner/dataset/?access_token=t"}}}
            }}}"#,
        )
        .await;

        let editor = resolve_dataset_editor(&client, &parse("alice/ds@1.2.3"), None)
            .await
            .unwrap();

        assert_eq!(editor.editor_page_id, "workspace-id");
        let request = served.await.unwrap();
        assert!(request.contains(r#""major":1"#), "{request}");
        assert!(request.contains(r#""minor":2"#), "{request}");
        assert!(request.contains(r#""patch":3"#), "{request}");
    }

    #[tokio::test]
    async fn a_pinned_target_errors_when_the_viewer_cannot_edit_the_version() {
        let (client, _server) = serve_graphql(
            r#"{"data":{"workspaceBySlug":{"id":"workspace-id","version":
                {"id":"version-id","version":"1.2.3","publishedAt":null,"viewerCanEdit":false,
                 "editor":{"id":"r-draft","phase":"READY",
                           "url":"http://localhost:8080/runner/draft/?access_token=draft-token"}}
            }}}"#,
        )
        .await;

        let err = resolve_editor(&client, &parse("alice/ws@1.2.3"), None)
            .await
            .unwrap_err();

        assert!(err.is_user());
        assert!(err.to_string().contains("edit"), "{err}");
    }

    #[test]
    fn parses_owner_and_slug() {
        let target = parse("alice/my-workspace");
        assert_eq!(target.owner, "alice");
        assert_eq!(target.slug, "my-workspace");
        assert_eq!(target.version, None);
    }

    #[test]
    fn strips_leading_at_from_owner() {
        assert_eq!(parse("@alice/my-workspace"), parse("alice/my-workspace"));
    }

    #[test]
    fn parses_version() {
        let target = parse("@alice/my-workspace@1.2.3");
        assert_eq!(target.owner, "alice");
        assert_eq!(target.slug, "my-workspace");
        assert_eq!(target.version, Some(semver::Version::new(1, 2, 3)));
    }

    #[test]
    fn round_trips_through_display() {
        for input in ["alice/my-workspace", "alice/my-workspace@1.2.3"] {
            assert_eq!(parse(input).to_string(), input);
        }
    }

    #[test]
    fn rejects_malformed_targets() {
        for input in ["my-workspace", "alice/", "/my-workspace", "alice/ws@nope"] {
            assert!(input.parse::<PairTarget>().is_err(), "accepted {input:?}");
        }
    }

    #[test]
    fn splits_token_off_the_runner_url() {
        let url = Url::parse("http://localhost:8080/runner/abc/?file=readme.py&access_token=tok")
            .unwrap();
        let (base, token) = split_url_and_token(url).unwrap();
        assert_eq!(base.as_str(), "http://localhost:8080/runner/abc/");
        assert_eq!(token, "tok");
    }

    #[test]
    fn rejects_a_runner_url_without_a_token() {
        let url = Url::parse("http://localhost:8080/runner/abc/?file=readme.py").unwrap();
        assert!(split_url_and_token(url).is_err());
    }
}
