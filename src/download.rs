use crate::{
    compress::decompress,
    error::{self, Result},
    graphql_client::GraphQLClient,
    progress_bar::{self, TempProgressStyle},
};
use indicatif::ProgressBar;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use url::Url;

struct DownloadInspector<'a> {
    _temp: TempProgressStyle<'a>,
    pb: &'a ProgressBar,
    should_inc: bool,
}

impl<'a> DownloadInspector<'a> {
    fn new(pb: &'a ProgressBar, content_length: Option<usize>) -> Self {
        let _temp = TempProgressStyle::new(pb);
        let should_inc = if let Some(content_length) = content_length {
            pb.reset();
            pb.set_style(progress_bar::pretty_bytes());
            pb.disable_steady_tick();
            pb.set_position(0);
            pb.set_length(content_length as u64);
            true
        } else {
            false
        };
        Self {
            _temp,
            pb,
            should_inc,
        }
    }

    fn inspect(&self, bytes: &[u8]) {
        if self.should_inc {
            self.pb.inc(bytes.len() as u64)
        }
    }
}

pub async fn download_archive(
    client: &GraphQLClient,
    url: Url,
    dir: impl AsRef<Path>,
    pb: &ProgressBar,
) -> Result<()> {
    tokio::fs::create_dir_all(&dir).await.map_err(|e| {
        error::user(
            &format!(
                "Failed to create directory {}: {}",
                dir.as_ref().display(),
                e
            ),
            "Please make sure you have permission to create directories in this directory",
        )
    })?;

    let response = client.s3_get(url).await?;

    let filename = response
        .content_disposition
        .as_ref()
        .map(|s| content_disposition::parse_content_disposition(s))
        .and_then(|cd| cd.filename_full())
        .ok_or_else(|| error::system("No filename found for download", ""))?;
    let tar_dir = tempfile::TempDir::new().map_err(|e| {
        error::user(
            &format!("Failed to create temporary file: {e}"),
            "Please make sure you have permission to create files in this directory",
        )
    })?;
    let tar_path = tar_dir.path().join(filename);

    let inspector = DownloadInspector::new(pb, response.content_length);
    let mut tar_file = tokio::io::BufWriter::new(tokio_util::io::InspectWriter::new(
        tokio::fs::File::create(&tar_path).await?,
        move |bytes| {
            inspector.inspect(bytes);
        },
    ));
    tokio::io::copy_buf(&mut response.body.into_async_read(), &mut tar_file).await?;
    tar_file.flush().await?;
    drop(tar_file);

    decompress(tar_path, &dir, pb).await.map_err(|e| {
        error::user(
            &format!("Failed to decompress data: {e}"),
            "Please make sure you have permission to create files in this directory",
        )
    })?;
    Ok(())
}
