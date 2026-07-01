use crate::error::{self, Result};
use crate::graphql_client::custom_scalars::Semver;
use graphql_client::GraphQLQuery;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use url::Url;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_workspace_notebook_download_url.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct GetWorkspaceNotebookDownloadUrl;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_workspace_version_notebook_download_url.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct GetWorkspaceVersionNotebookDownloadUrl;

/// A workspace version's notebook metadata, normalized across the "latest
/// version" and "specific version" queries so the download logic is shared.
struct NotebookVersion {
    lib_notebook: Option<String>,
    files: Vec<(String, Option<Url>)>,
}

/// Fetches the notebook metadata for the requested version, or the latest
/// published version when `version` is `None`.
async fn fetch_notebook_version(
    client: &aqora_client::Client,
    owner: &str,
    slug: &str,
    version: Option<String>,
) -> Result<NotebookVersion> {
    let workspace_not_found = || {
        error::user(
            &format!("Workspace '{owner}/{slug}' not found"),
            "Please make sure the owner and slug are correct",
        )
    };

    if let Some(version) = version {
        use get_workspace_version_notebook_download_url::*;
        let selected = client
            .send::<GetWorkspaceVersionNotebookDownloadUrl>(Variables {
                owner: owner.to_owned(),
                slug: slug.to_owned(),
                version: version.clone(),
            })
            .await?
            .workspace_by_slug
            .ok_or_else(workspace_not_found)?
            .version
            .ok_or_else(|| {
                error::user(
                    &format!("Workspace '{owner}/{slug}' has no version '{version}'"),
                    "Please make sure the version exists and is published",
                )
            })?;
        Ok(NotebookVersion {
            lib_notebook: selected.lib_notebook,
            files: selected
                .entries
                .unwrap_or_default()
                .into_iter()
                .filter_map(|entry| match entry {
                    GetWorkspaceVersionNotebookDownloadUrlWorkspaceBySlugVersionEntries::WorkspaceFile(file) => {
                        Some((file.name, file.download_url))
                    }
                    _ => None,
                })
                .collect(),
        })
    } else {
        use get_workspace_notebook_download_url::*;
        let latest = client
            .send::<GetWorkspaceNotebookDownloadUrl>(Variables {
                owner: owner.to_owned(),
                slug: slug.to_owned(),
            })
            .await?
            .workspace_by_slug
            .ok_or_else(workspace_not_found)?
            .latest_version
            .ok_or_else(|| {
                error::user(
                    &format!("Workspace '{owner}/{slug}' has no published version"),
                    "Please make sure the workspace has a published version",
                )
            })?;
        Ok(NotebookVersion {
            lib_notebook: latest.lib_notebook,
            files: latest
                .entries
                .unwrap_or_default()
                .into_iter()
                .filter_map(|entry| match entry {
                    GetWorkspaceNotebookDownloadUrlWorkspaceBySlugLatestVersionEntries::WorkspaceFile(file) => {
                        Some((file.name, file.download_url))
                    }
                    _ => None,
                })
                .collect(),
        })
    }
}

pub async fn download_workspace_notebook(
    client: aqora_client::Client,
    owner: String,
    slug: String,
    dest_dir: impl AsRef<Path>,
    notebook: Option<String>,
    version: Option<String>,
    force: bool,
) -> Result<String> {
    let selected = fetch_notebook_version(&client, &owner, &slug, version).await?;

    let notebook_name = notebook.or(selected.lib_notebook).ok_or_else(|| {
        error::user(
            &format!(
                "No notebook specified and workspace '{owner}/{slug}' has no default notebook"
            ),
            "Please provide a filename or set a default notebook on the workspace",
        )
    })?;

    let dest = dest_dir.as_ref().join(&notebook_name);

    if !force && dest.exists() {
        return Ok(notebook_name);
    }

    let download_url = selected
        .files
        .into_iter()
        .find_map(|(name, download_url)| (name == notebook_name).then_some(download_url).flatten())
        .ok_or_else(|| {
            error::user(
                &format!("File '{notebook_name}' not found in workspace '{owner}/{slug}'"),
                "Please make sure the workspace contains this file",
            )
        })?;

    let dest_dir = dest_dir.as_ref();
    tokio::fs::create_dir_all(dest_dir).await.map_err(|e| {
        error::user(
            &format!(
                "Failed to create destination directory '{}': {e}",
                dest_dir.display()
            ),
            "Please check your write permissions for the destination path",
        )
    })?;

    let temp = tempfile::NamedTempFile::new_in(dest_dir).map_err(|e| {
        error::user(
            &format!(
                "Failed to create temporary file in '{}': {e}",
                dest_dir.display()
            ),
            "Please check your write permissions for the destination path",
        )
    })?;
    let temp_path = temp.path().to_owned();

    let response = client.s3_get(download_url).await?;
    let file = tokio::fs::File::from_std(temp.into_file());

    let mut reader = response.body.into_async_read();
    let mut writer = tokio::io::BufWriter::new(file);
    match async {
        tokio::io::copy_buf(&mut reader, &mut writer).await?;
        writer.flush().await
    }
    .await
    {
        Ok(()) => {
            tokio::fs::rename(&temp_path, &dest).await?;
            Ok(notebook_name)
        }
        Err(e) => {
            let _ = tokio::fs::remove_file(&temp_path).await;
            Err(e.into())
        }
    }
}
