use crate::error::{self, Result};
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

pub async fn download_workspace_notebook(
    client: aqora_client::Client,
    owner: String,
    slug: String,
    dest_dir: impl AsRef<Path>,
    notebook: Option<String>,
    force: bool,
) -> Result<String> {
    let workspace = client
        .send::<GetWorkspaceNotebookDownloadUrl>(get_workspace_notebook_download_url::Variables {
            owner: owner.clone(),
            slug: slug.clone(),
        })
        .await?
        .workspace_by_slug
        .ok_or_else(|| {
            error::user(
                &format!("Workspace '{owner}/{slug}' not found"),
                "Please make sure the owner and slug are correct",
            )
        })?;

    let notebook_name = notebook.or(workspace.default_notebook).ok_or_else(|| {
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

    let download_url = workspace
        .entries
        .unwrap_or_default()
        .into_iter()
        .find_map(|entry| match entry {
            get_workspace_notebook_download_url::GetWorkspaceNotebookDownloadUrlWorkspaceBySlugEntries::WorkspaceFile(
                file,
            ) if file.name == notebook_name => file.download_url,
            _ => None,
        })
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
