use aqora_config::ReadMe;
use mime::Mime;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs;

#[derive(Debug, Error)]
pub enum ReadMeError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Readme not found")]
    NotFound,
    #[error("Readme content type not supported. Only markdown and plaintext supported")]
    ContentTypeNotSupported,
}

fn is_supported_mime(content_type: &str) -> Result<(), ReadMeError> {
    let mime: Mime = content_type
        .parse()
        .map_err(|_| ReadMeError::ContentTypeNotSupported)?;
    if mime.type_() == mime::TEXT && (mime.subtype() == mime::PLAIN || mime.subtype() == "markdown")
    {
        Ok(())
    } else {
        Err(ReadMeError::ContentTypeNotSupported)
    }
}

fn is_supported_extension(extension: Option<&str>) -> bool {
    match extension {
        Some(ext) => {
            let ext = ext.to_lowercase();
            ext == "md" || ext == "txt"
        }
        None => true,
    }
}

async fn find_readme_path(
    project_dir: &Path,
    readme: Option<&ReadMe>,
) -> Result<Option<PathBuf>, ReadMeError> {
    if let Some(readme) = readme {
        match readme {
            ReadMe::Table {
                file,
                text: _,
                content_type,
            } => {
                if let Some(content_type) = content_type {
                    is_supported_mime(content_type)?;
                }
                if let Some(file) = file {
                    let path = project_dir.join(file);
                    if is_supported_extension(path.extension().and_then(|s| s.to_str())) {
                        return Ok(Some(path));
                    } else {
                        return Err(ReadMeError::ContentTypeNotSupported);
                    }
                }
            }
            ReadMe::RelativePath(path) => {
                let path = project_dir.join(path);
                if is_supported_extension(path.extension().and_then(|s| s.to_str())) {
                    return Ok(Some(path));
                } else {
                    return Err(ReadMeError::ContentTypeNotSupported);
                }
            }
        }
    }

    let mut dir = fs::read_dir(project_dir).await?;
    while let Some(entry) = dir.next_entry().await? {
        let file_name = entry.file_name().to_string_lossy().to_lowercase();
        let readme_files = ["readme.md", "readme.txt", "readme"];
        if readme_files.contains(&file_name.as_str()) && entry.metadata().await?.is_file() {
            return Ok(Some(entry.path()));
        }
    }

    Ok(None)
}

pub async fn read_readme(
    project_dir: impl AsRef<Path>,
    readme: Option<&ReadMe>,
) -> Result<Option<String>, ReadMeError> {
    let project_dir = project_dir.as_ref();

    if let Some(ReadMe::Table {
        text: Some(text), ..
    }) = readme
    {
        return Ok(Some(text.clone()));
    }

    let path = find_readme_path(project_dir, readme).await?;

    if let Some(path) = path {
        if !fs::try_exists(&path).await? {
            return Err(ReadMeError::NotFound);
        }
        let content = fs::read_to_string(path).await?;
        Ok(Some(content))
    } else {
        Ok(None)
    }
}

pub async fn write_readme(project_dir: impl AsRef<Path>, content: &str) -> Result<(), ReadMeError> {
    let project_dir = project_dir.as_ref();
    let existing_readme_path = find_readme_path(project_dir, None).await?;
    if let Some(existing_path) = existing_readme_path {
        fs::write(existing_path, content).await?;
    } else {
        return Err(ReadMeError::NotFound);
    }
    Ok(())
}
