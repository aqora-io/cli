use crate::{
    compress::decompress,
    error::{self, Result},
    graphql_client::GraphQLClient,
    progress_bar::{self, TempProgressStyle},
};
use aqora_client::retry::{
    BackoffBuilder, ExponentialBackoffBuilder, RetryClassifier, RetryStatusCodeRange,
};
use clap::Args;
use futures::{prelude::*, TryStreamExt};
use indicatif::ProgressBar;
use serde::Serialize;
use std::{
    ops::Range,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio_util::io::InspectWriter;
use url::Url;

const DEFAULT_CHUNK_SIZE: usize = 16 * 1024 * 1024; // 16 Mib
const DEFAULT_CONCURRENCY: usize = 10;

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

/// Stream an S3 GET response body into `file`, driving byte progress on `pb`.
async fn write_s3_response(
    response: aqora_client::s3::S3GetResponse,
    file: tokio::fs::File,
    pb: &ProgressBar,
) -> Result<()> {
    let inspector = DownloadInspector::new(pb, response.content_length);
    let mut writer = BufWriter::new(InspectWriter::new(file, move |bytes| {
        inspector.inspect(bytes);
    }));
    tokio::io::copy_buf(&mut response.body.into_async_read(), &mut writer).await?;
    writer.flush().await?;
    Ok(())
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

    let tar_file = tokio::fs::File::create(&tar_path).await?;
    write_s3_response(response, tar_file, pb).await?;

    decompress(tar_path, &dir, pb).await.map_err(|e| {
        error::user(
            &format!("Failed to decompress data: {e}"),
            "Please make sure you have permission to create files in this directory",
        )
    })?;
    Ok(())
}

/// Stream a presigned S3 GET straight to `output`, showing byte progress on `pb`.
///
/// Suitable for small single-file payloads: it issues one plain GET (no chunking
/// or ranged retries) and atomically renames a temp file into place on success.
pub async fn download_stream_to_file(
    client: &GraphQLClient,
    url: Url,
    output: &Path,
    pb: &ProgressBar,
) -> Result<()> {
    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    tokio::fs::create_dir_all(&parent).await?;

    let response = client.s3_get(url).await?;

    // Write through the temp file's existing handle (via `into_parts`) instead
    // of reopening `temp.path()`: reopening a `NamedTempFile` fails on Windows
    // and races the still-open handle.
    let (temp_file, temp_path) = tempfile::NamedTempFile::new_in(&parent)?.into_parts();
    write_s3_response(response, tokio::fs::File::from_std(temp_file), pb).await?;

    temp_path.persist(output).map_err(|err| {
        error::user(
            &format!("Failed to save download to {}: {}", output.display(), err),
            "Make sure you have permission to write to this location.",
        )
    })?;

    Ok(())
}

fn parse_duration(arg: &str) -> Result<std::time::Duration, std::num::ParseIntError> {
    let seconds = arg.parse()?;
    Ok(std::time::Duration::from_secs(seconds))
}

#[derive(Debug, Clone, Default, Serialize, Args)]
pub struct ExponentialBackoffOptions {
    #[arg(long, value_parser = parse_duration, default_value = "1")]
    pub start_delay: Duration,
    #[arg(long, default_value_t = 2.)]
    pub factor: f64,
    #[arg(long, value_parser = parse_duration, default_value = "60")]
    pub max_delay: Duration,
    #[arg(long, default_value_t = 5)]
    pub max_retries: usize,
}

impl From<ExponentialBackoffOptions> for ExponentialBackoffBuilder {
    fn from(value: ExponentialBackoffOptions) -> Self {
        ExponentialBackoffBuilder {
            start_delay: value.start_delay,
            factor: value.factor,
            max_delay: Some(value.max_delay),
            max_retries: Some(value.max_retries),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Args)]
pub struct MultipartOptions {
    #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
    pub chunk_size: usize,
    #[arg(long, default_value_t = DEFAULT_CONCURRENCY)]
    pub chunk_concurrency: usize,
    #[command(flatten)]
    backoff: ExponentialBackoffOptions,
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
    type Item = Range<u64>;

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

#[derive(Clone)]
struct RangeDownloader<R> {
    client: GraphQLClient,
    retry_classifier: R,
    backoff_builder: ExponentialBackoffBuilder,
}

impl<R> RangeDownloader<R>
where
    R: RetryClassifier<aqora_client::http::Response, crate::error::Error> + Send + Sync + 'static,
{
    async fn retry_range(
        &self,
        url: &Url,
        range: Range<u64>,
        inspector: &DownloadInspector<'_>,
        path: &Path,
    ) -> Result<()> {
        for delay in self.backoff_builder.build() {
            match self
                .client
                .s3_get_range(url.clone(), range.start as usize..range.end as usize)
                .await
            {
                Ok(response) => {
                    let mut file = tokio::fs::OpenOptions::new().write(true).open(path).await?;
                    file.seek(std::io::SeekFrom::Start(range.start)).await?;
                    let mut writer = BufWriter::new(InspectWriter::new(file, |bytes: &[u8]| {
                        inspector.inspect(bytes);
                    }));
                    let mut reader = BufReader::new(response.body.into_async_read());

                    tokio::io::copy_buf(&mut reader, &mut writer).await?;
                    writer.flush().await?;
                    return Ok(());
                }
                Err(err) => {
                    if !self.retry_classifier.should_retry(&Err(err.into())) {
                        return Err(crate::error::system("S3 range", "non-retryable error"));
                    }
                    tokio::time::sleep(delay).await;
                }
            }
        }

        Err(crate::error::system("S3 range", "exhausted retries"))
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
    drop(file);

    let inspector = DownloadInspector::new(pb, Some(size as _));

    let downloader = RangeDownloader {
        client: client.clone(),
        retry_classifier: RetryStatusCodeRange::for_client_and_server_errors(),
        backoff_builder: options.backoff.clone().into(),
    };

    stream::iter(ChunkIter::new(size, options.chunk_size as _))
        .map(|range| {
            let downloader = downloader.clone();
            let url = url.clone();
            let range = range.clone();
            let inspector = &inspector;
            let path = output_path.to_owned();

            async move { downloader.retry_range(&url, range, inspector, &path).await }
        })
        .buffer_unordered(options.chunk_concurrency)
        .try_collect::<()>()
        .await?;

    Ok(())
}
