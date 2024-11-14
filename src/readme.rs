use aqora_config::ReadMe;
use mime::Mime;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReadmeError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("README not found")]
    NotFound,
    #[error("README content type not supported. Only markdown and plaintext supported")]
    ContentTypeNotSupported,
}

pub async fn get_readme_path(
    project_dir: impl AsRef<Path>,
    readme: Option<&ReadMe>,
) -> Result<Option<PathBuf>, ReadmeError> {
    let path = match readme {
        Some(ReadMe::Table {
            ref file,
            text: _,
            content_type,
        }) => {
            let path: Option<&Path> = file.as_deref().map(str::as_ref);
            if let Some(content_type) = content_type {
                let mime: Mime = content_type
                    .parse()
                    .map_err(|_| ReadmeError::ContentTypeNotSupported)?;
                if !(mime.type_() == mime::TEXT
                    && (mime.subtype() == mime::PLAIN || mime.subtype() == "markdown"))
                {
                    return Err(ReadmeError::ContentTypeNotSupported);
                }
            }
            path.map(|p| p.to_path_buf())
        }
        Some(ReadMe::RelativePath(ref path)) => Some(PathBuf::from(path)),
        None => None,
    };

    if let Some(path) = path {
        return Ok(Some(path));
    }

    let mut dir = tokio::fs::read_dir(&project_dir).await?;
    while let Some(entry) = dir.next_entry().await? {
        match entry.file_name().to_string_lossy().to_lowercase().as_str() {
            "readme.md" | "readme.txt" => {}
            _ => continue,
        }
        let metadata = entry.metadata().await?;
        if metadata.is_file() {
            return Ok(Some(entry.path()));
        }
    }

    Ok(None)
}

pub async fn read_readme(
    project_dir: impl AsRef<Path>,
    readme: Option<&ReadMe>,
) -> Result<Option<String>, ReadmeError> {
    let path = match get_readme_path(project_dir, readme).await? {
        Some(path) => path,
        None => return Ok(None),
    };

    match path
        .extension()
        .map(|ext| ext.to_string_lossy().to_lowercase())
        .as_deref()
    {
        Some("md") | Some("txt") | None => {}
        _ => return Err(ReadmeError::ContentTypeNotSupported),
    }

    if !tokio::fs::try_exists(&path).await? {
        return Err(ReadmeError::NotFound);
    }

    Ok(Some(tokio::fs::read_to_string(path).await?))
}
