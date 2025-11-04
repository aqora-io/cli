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
use std::{ops::Range, path::Path, time::Duration};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio_util::io::InspectWriter;
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

    fn rollback(&self, bytes: u64) {
        if self.should_inc {
            self.pb.dec(bytes)
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

const DEFAULT_CHUNK_SIZE: usize = 64 * 1024; // 64 KiB

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
    #[arg(long, default_value_t = 10)]
    pub chunck_concurrency: usize,
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
        mut file: tokio::fs::File,
    ) -> Result<()> {
        for delay in self.backoff_builder.build() {
            match self
                .client
                .s3_get_range(url.clone(), range.start as usize..range.end as usize)
                .await
            {
                Ok(response) => {
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
                    inspector.rollback(range.end - range.start);
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
            let inspector = &inspector;
            let file = &file;

            async move {
                downloader
                    .retry_range(&url, range.clone(), inspector, file.try_clone().await?)
                    .await
            }
        })
        .buffer_unordered(options.chunck_concurrency)
        .try_collect::<()>()
        .await?;

    file.sync_all().await?;
    Ok(())
}
