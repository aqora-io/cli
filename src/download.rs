use crate::{
    compress::decompress,
    error::{self, Result},
    graphql_client::GraphQLClient,
    progress_bar::{self, TempProgressStyle},
};
use futures::{prelude::*, TryStreamExt};
use indicatif::ProgressBar;
use std::path::Path;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;
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

#[derive(Debug, Clone)]
pub struct MultipartOptions {
    pub chunk_size: usize,
    pub concurrency: usize,
}

impl MultipartOptions {
    pub fn new(chunk_size: usize, concurrency: usize) -> Self {
        Self {
            chunk_size,
            concurrency,
        }
    }
}

struct ChunkIter {
    current: u64,
    end: u64,
    step: u64,
}

impl ChunkIter {
    fn new(end: u64, step: u64) -> Self {
        Self {
            current: 0,
            end,
            step,
        }
    }
}

impl Iterator for ChunkIter {
    type Item = std::ops::Range<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.end {
            return None;
        }
        let start = self.current;
        let next_end = (start + self.step).min(self.end);
        self.current = next_end;
        Some(start..next_end)
    }
}

pub async fn multipart_download(
    client: &GraphQLClient,
    size: u64,
    url: Url,
    options: &MultipartOptions,
    path: impl AsRef<Path>,
    pb: &ProgressBar,
) -> Result<()> {
    let output_path = path.as_ref();
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(output_path)
        .await?;
    file.set_len(size).await?;

    let inspector = DownloadInspector::new(pb, Some(size as _));

    stream::iter(ChunkIter::new(size, options.chunk_size as _))
        .map(|range| {
            let client = &client;
            let inspector = &inspector;
            let url = url.to_owned();
            let file_path = output_path.to_owned();

            async move {
                let body = client
                    .s3_get_range(url, range.start as usize..range.end as usize)
                    .await?
                    .body;

                let data = ReaderStream::new(body.into_async_read())
                    .map_err(|e| crate::error::system("S3 body stream", &e.to_string()))
                    .try_fold(
                        Vec::with_capacity((range.end - range.start) as usize),
                        |mut acc, chunk| {
                            inspector.inspect(&chunk);
                            acc.extend_from_slice(&chunk);
                            async move { Ok::<_, crate::error::Error>(acc) }
                        },
                    )
                    .await?;

                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&file_path)
                    .await?;
                file.seek(std::io::SeekFrom::Start(range.start)).await?;
                file.write_all(&data).await?;
                file.flush().await?;

                Ok::<_, crate::error::Error>(())
            }
        })
        .buffer_unordered(options.concurrency)
        .try_collect::<()>()
        .await?;

    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(output_path)
        .await?;
    file.sync_all().await?;

    Ok(())
}
