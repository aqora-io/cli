use aqora_config::ReadMe;
use mime::Mime;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReadMeError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Readme not found")]
    NotFound,
    #[error("Readme content type not supported. Only markdown and plaintext supported")]
    ContentTypeNotSupported,
}

pub async fn read_readme(
    project_dir: impl AsRef<Path>,
    readme: Option<&ReadMe>,
) -> Result<Option<String>, ReadMeError> {
    let path = match readme {
        Some(ReadMe::Table {
            ref file,
            text,
            content_type,
        }) => {
            let path: Option<&Path> = file.as_deref().map(str::as_ref);
            if let Some(content_type) = content_type {
                let mime: Mime = content_type
                    .parse()
                    .map_err(|_| ReadMeError::ContentTypeNotSupported)?;
                if !(mime.type_() == mime::TEXT
                    && (mime.subtype() == mime::PLAIN || mime.subtype() == "markdown"))
                {
                    return Err(ReadMeError::ContentTypeNotSupported);
                }
            }
            if let Some(text) = text {
                return Ok(Some(text.to_owned()));
            }
            path
        }
        Some(ReadMe::RelativePath(ref path)) => Some(path.as_ref()),
        None => None,
    };
    let path = if let Some(path) = path {
        project_dir.as_ref().join(path)
    } else {
        let mut dir = tokio::fs::read_dir(&project_dir).await?;
        let mut path = None;
        while let Some(entry) = dir.next_entry().await? {
            match entry.file_name().to_string_lossy().to_lowercase().as_str() {
                "readme.md" | "readme.txt" => {}
                _ => continue,
            }
            let metadata = entry.metadata().await?;
            if !metadata.is_file() {
                continue;
            }
            path = Some(entry.path());
            break;
        }
        if let Some(path) = path {
            path
        } else {
            return Ok(None);
        }
    };
    match path
        .extension()
        .map(|ext| ext.to_string_lossy().to_lowercase())
        .as_deref()
    {
        Some("md") | Some("txt") | None => {}
        _ => {
            return Err(ReadMeError::ContentTypeNotSupported);
        }
    }
    if !tokio::fs::try_exists(&path).await? {
        return Err(ReadMeError::NotFound);
    }
    Ok(Some(tokio::fs::read_to_string(path).await?))
}
