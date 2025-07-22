use std::{fmt::Display, io::SeekFrom, str::FromStr};

use aqora_client::{
    checksum::{crc32fast::Crc32, S3ChecksumLayer},
    retry::{BackoffRetryLayer, ExponentialBackoffBuilder, RetryStatusCodeRange},
};
use aqora_data_utils::{
    aqora_client::DatasetVersionFileUploader, write::RecordBatchStreamParquetExt as _,
};
use clap::Args;
use futures::TryStreamExt as _;
use graphql_client::GraphQLQuery;
use indicatif::ProgressBar;
use parquet::{
    basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::Serialize;
use thiserror::Error;
use tokio::io::AsyncSeekExt as _;

use crate::{
    error::{self, Result},
    progress_bar,
};

use super::GlobalArgs;

/// Upload a file to Aqora.io
#[derive(Args, Debug, Serialize)]
pub struct Upload {
    /// Dataset you want to upload to, must respect "{owner}/{dataset}" form.
    slug: String,
    /// Path to file you will upload to Aqora.
    file: String,
    /// Dataset version you will be uploading. Omit this flag to draft a new version.
    #[arg(short, long)]
    version: Option<Semver>,
    /// How many records to read from the file to infer its schema
    #[arg(long, default_value_t = 100)]
    sample_size: usize,
    /// How many records should the reader gather in a batch
    #[arg(long, default_value_t = 100)]
    read_batch_size: usize,
    /// Max number of partitions to upload concurrently
    #[arg(long, default_value_t = 2)]
    concurrent_uploads: usize,
    /// Max size in MB for a single Parquet partition
    #[arg(long, default_value_t = 100)]
    max_partition_size: usize,
    /// Max size in MB for a single uploaded chunk, a single partition is uploaded in many chunks: each chunk may be reuploaded upon integrity violation
    #[arg(long, default_value_t = 10)]
    max_chunk_size: usize,
    /// How columns should be compressed, available schemes are: "none", "snappy", "lzo", "lz4",
    /// "lz4_raw", "zstd", "zstd,{level}", "brotli", "brotli,{level}", "gzip", "gzip,{level}"
    #[arg(long, default_value_t = UploadCompression::ZSTD)]
    compression: UploadCompression,
}

#[derive(Debug, Clone)]
pub struct UploadCompression(Compression);

impl UploadCompression {
    // SAFETY: ZstdLevel wraps a i32
    const DEFAULT_ZSTD_LEVEL: ZstdLevel = unsafe { std::mem::transmute(3i32) };
    const ZSTD: Self = Self(Compression::ZSTD(Self::DEFAULT_ZSTD_LEVEL));
}

impl From<UploadCompression> for Compression {
    fn from(value: UploadCompression) -> Self {
        value.0
    }
}

impl Display for UploadCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Compression::UNCOMPRESSED => f.write_str("none"),
            Compression::SNAPPY => f.write_str("snappy"),
            Compression::GZIP(level) => {
                if level == GzipLevel::default() {
                    f.write_str("gzip")
                } else {
                    f.write_fmt(format_args!("gzip,{}", level.compression_level()))
                }
            }
            Compression::LZO => f.write_str("lzo"),
            Compression::BROTLI(level) => {
                if level == BrotliLevel::default() {
                    f.write_str("brotli")
                } else {
                    f.write_fmt(format_args!("brotli,{}", level.compression_level()))
                }
            }
            Compression::LZ4 => f.write_str("lz4"),
            Compression::ZSTD(level) => {
                if level == Self::DEFAULT_ZSTD_LEVEL {
                    f.write_str("zstd")
                } else {
                    f.write_fmt(format_args!("zstd,{}", level.compression_level()))
                }
            }
            Compression::LZ4_RAW => f.write_str("lz4_raw"),
        }
    }
}

impl Serialize for UploadCompression {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[derive(Debug, Error)]
#[error("Malformed compression scheme")]
pub struct MalformedUploadCompressionError;

impl FromStr for UploadCompression {
    type Err = MalformedUploadCompressionError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s
            .split_once(",")
            .map_or((s, Option::<&str>::None), |(x, y)| (x, Some(y)))
        {
            ("brotli", level) => Ok(Self(Compression::BROTLI(if let Some(level) = level {
                BrotliLevel::try_new(level.parse().map_err(|_| MalformedUploadCompressionError)?)
                    .map_err(|_| MalformedUploadCompressionError)?
            } else {
                BrotliLevel::default()
            }))),

            ("gzip", level) => Ok(Self(Compression::GZIP(if let Some(level) = level {
                GzipLevel::try_new(level.parse().map_err(|_| MalformedUploadCompressionError)?)
                    .map_err(|_| MalformedUploadCompressionError)?
            } else {
                GzipLevel::default()
            }))),

            ("lz4", _) => Ok(Self(Compression::LZ4)),
            ("lz4_raw", _) => Ok(Self(Compression::LZ4_RAW)),

            ("lzo", _) => Ok(Self(Compression::LZO)),

            ("none", _) => Ok(Self(Compression::UNCOMPRESSED)),

            ("snappy", _) => Ok(Self(Compression::SNAPPY)),

            ("zstd", level) => Ok(Self(Compression::ZSTD(if let Some(level) = level {
                ZstdLevel::try_new(level.parse().map_err(|_| MalformedUploadCompressionError)?)
                    .map_err(|_| MalformedUploadCompressionError)?
            } else {
                Self::DEFAULT_ZSTD_LEVEL
            }))),

            _ => Err(MalformedUploadCompressionError),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Semver {
    major: i32,
    minor: i32,
    patch: i32,
}

impl Display for Semver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{major}.{minor}.{patch}",
            major = self.major,
            minor = self.minor,
            patch = self.patch
        ))
    }
}

#[derive(Debug, Error)]
#[error("Malformed semver version, expected a string matching: \"<major>.<minor>.<patch>\"")]
pub struct MalformedSemverError;

impl FromStr for Semver {
    type Err = MalformedSemverError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let mut it = s.split('.');
        let major = it
            .next()
            .ok_or(MalformedSemverError)?
            .parse()
            .map_err(|_| MalformedSemverError)?;
        let minor = it
            .next()
            .ok_or(MalformedSemverError)?
            .parse()
            .map_err(|_| MalformedSemverError)?;
        let patch = it
            .next()
            .ok_or(MalformedSemverError)?
            .parse()
            .map_err(|_| MalformedSemverError)?;
        if it.next().is_some() {
            return Err(MalformedSemverError);
        }
        Ok(Self {
            major,
            minor,
            patch,
        })
    }
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/dataset_upload_info.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct DatasetUploadInfo;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/dataset_version_create.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct DatasetVersionCreate;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/dataset_version_info.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct DatasetVersionInfo;

pub async fn upload(args: Upload, global: GlobalArgs) -> Result<()> {
    let client = global.graphql_client().await?;

    // Find dataset the user wants to upload
    let Some((owner, local_slug)) = args.slug.split_once('/') else {
        return Err(error::user(
            "Malformed slug",
            "Expected a slug like: {owner}/{dataset}",
        ));
    };
    let dataset_info = client
        .send::<DatasetUploadInfo>(dataset_upload_info::Variables {
            owner: owner.to_string(),
            local_slug: local_slug.to_string(),
        })
        .await?;
    let Some(dataset_info) = dataset_info.dataset_by_slug else {
        return prompt_dataset_creation(global).await;
    };
    if !dataset_info.viewer_can_create_version {
        return Err(error::user(
            "You cannot upload a version for this dataset",
            "Did you type the right slug?",
        ));
    }

    // Find or create a dataset version
    let dataset_version_id = if let Some(semver) = args.version {
        let result = client
            .send::<DatasetVersionInfo>(dataset_version_info::Variables {
                dataset_id: dataset_info.id,
                major: semver.major as _,
                minor: semver.minor as _,
                patch: semver.patch as _,
            })
            .await?;

        if let dataset_version_info::DatasetVersionInfoNode::Dataset(node) = result.node {
            node.version
                .ok_or_else(|| {
                    error::user(
                        &format!("No such dataset version: {owner}/{local_slug}@{semver}"),
                        "You may also omit the version argument to draft a new version",
                    )
                })?
                .id
        } else {
            return Err(error::system(
                "Cannot find dataset by id",
                "This should not happen, kindly report this bug to Aqora developers please!",
            ));
        }
    } else {
        client
            .send::<DatasetVersionCreate>(dataset_version_create::Variables {
                dataset_id: dataset_info.id,
            })
            .await?
            .create_dataset_version
            .node
            .id
    };

    // Open file and infer its schema
    let (mut file_schema, file_format) = {
        let _pb = global.spinner().with_message(format!(
            "Inferring dataset schema at {file}",
            file = args.file
        ));
        let mut file = aqora_data_utils::fs::open(&args.file)
            .await
            .map_err(|error| {
                error::user_with_internal(
                    &format!("Cannot read file at {file}", file = args.file),
                    "Please make sure you have read permissions",
                    error,
                )
            })?;
        let file_schema = file
            .infer_schema(Default::default(), Some(args.sample_size))
            .await
            .map_err(|error| {
                error::user_with_internal(
                    &format!(
                        "Cannot infer schema for the file at {file}",
                        file = args.file
                    ),
                    "Please check the file is formatted correctly",
                    error,
                )
            })?;
        (file_schema, file.format().clone())
    };

    // Let the user review the inferred schema
    let will_review_schema = global
        .confirm()
        .with_prompt(
            "Schema successfully inferred from file, would you like to review it before uploading?",
        )
        .default(false)
        .interact()?;
    if will_review_schema {
        let json_schema = serde_json::to_string_pretty(&file_schema)?;
        let new_json_schema = dialoguer::Editor::new()
            .extension(".json")
            .edit(&json_schema)?;
        if new_json_schema.as_deref().is_some_and(str::is_empty) {
            tracing::info!("Aborting upload...");
            return Ok(());
        }
        if let Some(new_json_schema) = new_json_schema {
            file_schema = serde_json::from_str(&new_json_schema)?;
        }
    }

    // Upload the file
    let total_rows = {
        // Open the file
        let mut file = tokio::fs::File::open(&args.file).await?;
        let file_size = file.seek(SeekFrom::End(0)).await?;
        file.rewind().await?;

        // Create progress bar
        let pb = ProgressBar::new(file_size).with_style(progress_bar::pretty_bytes());

        // Stream batches of records from file
        let reader = aqora_data_utils::format::FormatReader::new(file, file_format);
        let values = reader.into_value_stream().await?.inspect_ok(|item| {
            pb.inc((item.end - item.start) as u64);
        });
        let stream = aqora_data_utils::read::from_value_stream(
            values,
            file_schema,
            aqora_data_utils::read::Options {
                batch_size: Some(args.read_batch_size),
            },
        )
        .map_err(|error| {
            error::user_with_internal("Cannot read the file you provided", "TODO", error)
        })?;

        // Create upload client
        const MB: usize = 1024 * 1024;
        let client = client
            .clone()
            .s3_layer(BackoffRetryLayer::new(
                RetryStatusCodeRange::for_client_and_server_errors(),
                ExponentialBackoffBuilder::default(),
            ))
            .s3_layer(S3ChecksumLayer::new(Crc32::new()))
            .to_owned();
        let writer = DatasetVersionFileUploader::new(client, dataset_version_id)
            .with_concurrency(Some(args.concurrent_uploads))
            .with_max_partition_size(Some(args.max_partition_size * MB));

        // Upload batches to server
        pb.suspend(|| tracing::info!("Starting to upload to server..."));
        stream
            .write_to_parquet(
                writer,
                aqora_data_utils::write::Options::default().with_properties(
                    WriterProperties::builder()
                        .set_compression(args.compression.into())
                        .build(),
                ),
                aqora_data_utils::write::BufferOptions {
                    batch_buffer_size: Some(2),
                    max_memory_size: Some(args.max_chunk_size * MB),
                },
            )
            .try_fold(0u64, async |acc, (_part, meta)| {
                Ok(acc + 0u64.saturating_add_signed(meta.num_rows))
            })
            .await
            .map_err(|error| {
                error::system_with_internal("Cannot upload the file you provided", "TODO", error)
            })?
    };

    tracing::info!(
        "Successfully uploaded {total_rows} rows to {slug:?} from {file}",
        slug = args.slug,
        file = args.file
    );

    Ok(())
}

async fn prompt_dataset_creation(global: GlobalArgs) -> Result<()> {
    let open_browser = global
        .confirm()
        .with_prompt("This dataset cannot be found, would like to open a browser to create it?")
        .default(false)
        .interact()?;
    if open_browser {
        open::that("https://aqora.io/datasets/new")?;
        Ok(())
    } else {
        Err(error::user(
            "Dataset not found",
            "Please double check the slug on Aqora.io",
        ))
    }
}
