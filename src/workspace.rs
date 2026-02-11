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
    dest: impl AsRef<Path>,
) -> Result<()> {
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

    let download_url = workspace
        .entries
        .into_iter()
        .find_map(|entry| match entry {
            get_workspace_notebook_download_url::GetWorkspaceNotebookDownloadUrlWorkspaceBySlugEntries::WorkspaceFile(
                file,
            ) if file.name == "readme.py" => file.download_url,
            _ => None,
        })
        .ok_or_else(|| {
            error::user(
                &format!("File 'readme.py' not found in workspace '{owner}/{slug}'"),
                "Please make sure the workspace contains a readme.py entry",
            )
        })?;

    let dest = dest.as_ref();
    if let Some(parent) = dest.parent().filter(|p| !p.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            error::user(
                &format!(
                    "Failed to create destination directory '{}': {e}",
                    parent.display()
                ),
                "Please check your write permissions for the destination path",
            )
        })?;
    }

    let response = client.s3_get(download_url).await?;
    let file = tokio::fs::File::create(dest).await.map_err(|e| {
        error::user(
            &format!(
                "Failed to create destination file '{}': {e}",
                dest.display()
            ),
            "Please check your write permissions for the destination path",
        )
    })?;

    let mut reader = response.body.into_async_read();
    let mut writer = tokio::io::BufWriter::new(file);
    tokio::io::copy_buf(&mut reader, &mut writer).await?;
    writer.flush().await?;

    Ok(())
}
