use std::{fmt::Display, path::PathBuf, str::FromStr};

use aqora_client::{
    checksum::{crc32fast::Crc32, S3ChecksumLayer},
    retry::{BackoffRetryLayer, ExponentialBackoffBuilder, RetryStatusCodeRange},
};
use aqora_data_utils::{aqora_client::DatasetVersionFileUploader, read, write};
use clap::Args;
use futures::TryStreamExt as _;
use graphql_client::GraphQLQuery;
use indicatif::ProgressBar;
use serde::Serialize;
use thiserror::Error;

use crate::commands::GlobalArgs;
use crate::error::{self, Result};

use super::{
    common::{get_dataset_by_slug, DatasetCommonArgs},
    convert::{BufferOptions, WriteOptions},
    infer::{open, FormatOptions, InferOptions, OpenOptions, SchemaOutput},
    version::{
        common::{get_dataset_version, get_dataset_versions},
        new::{create_dataset_version, CreateDatasetVersionInput},
    },
};

/// Upload a file to Aqora.io
#[derive(Args, Debug, Serialize)]
#[group(skip)]
pub struct Upload {
    #[command(flatten)]
    common: DatasetCommonArgs,
    /// Path to file you will upload to Aqora.
    src: PathBuf,
    /// Target dataset version.
    /// Omit to draft a new version (0.0.0).
    /// Non-existing versions are created, existing ones are overwritten.
    #[arg(short, long)]
    version: Option<Semver>,

    #[command(flatten)]
    format: Box<FormatOptions>,
    #[command(flatten)]
    infer: Box<InferOptions>,
    #[command(flatten)]
    write: Box<WriteOptions>,
    #[command(flatten)]
    buffer: Box<BufferOptions>,
    #[command(flatten)]
    writer: Box<WriterOptions>,
    #[arg(long)]
    schema: Option<String>,
    #[arg(long, default_value_t = 1024)]
    record_batch_size: usize,
    #[arg(long, value_enum, default_value_t = SchemaOutput::Table)]
    schema_output: SchemaOutput,
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

#[derive(Args, Debug, Serialize)]
pub struct WriterOptions {
    #[arg(long, default_value_t = 8)]
    concurrent_uploads: usize,
    #[arg(long, default_value_t = 1_000_000_000)]
    max_part_size: usize,
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/finish_dataset_version_upload.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct FinishDatasetVersionUpload;

pub async fn prompt_draft_versions(
    pb: &ProgressBar,
    global: &GlobalArgs,
    owner: impl AsRef<str>,
    local_slug: impl AsRef<str>,
) -> Result<String> {
    let client = global.graphql_client().await?;

    let versions = get_dataset_versions(
        &client,
        get_dataset_versions::Variables {
            owner: owner.as_ref().into(),
            local_slug: local_slug.as_ref().into(),
            limit: None,
            filters: Some(get_dataset_versions::DatasetVersionQueryFilters {
                order: get_dataset_versions::DatasetVersionConnectionOrder::UPDATED_AT,
                sort_direction:
                    get_dataset_versions::DatasetVersionConnectionSortDirection::ASCENDING,
                published: Some(false),
            }),
        },
    )
    .await?
    .ok_or_else(|| {
        error::user(
            "You cannot upload a version for this dataset",
            "Did you type the right slug?",
        )
    })?;

    let items = versions
        .versions
        .nodes
        .iter()
        .map(|version| version.version.to_string());

    pb.suspend(|| {
        global
            .fuzzy_select()
            .items(items)
            .with_prompt("Select a version to upload to:")
            .interact_opt()?
            .map(|index| versions.versions.nodes[index].id.clone())
            .ok_or_else(|| error::user("Dataset version not found", "Please specify a version"))
    })
}

pub async fn upload(args: Upload, global: GlobalArgs) -> Result<()> {
    let pb = global.spinner();
    let client = global.graphql_client().await?;

    let (owner, local_slug) = args.common.slug_pair()?;
    let dataset = get_dataset_by_slug(&global, owner, local_slug).await?;

    if !dataset.viewer_can_create_version {
        return Err(error::user(
            "You cannot upload a version for this dataset",
            "Did you type the right slug?",
        ));
    }
    let dataset_id = dataset.id;

    // Find or create a dataset version
    let dataset_version_id = match args.version {
        None => prompt_draft_versions(&pb, &global, owner, local_slug).await?,
        Some(semver) => {
            let dataset_version = get_dataset_version(
                &client,
                dataset_id.clone(),
                semver.major as _,
                semver.minor as _,
                semver.patch as _,
            )
            .await?;
            match dataset_version {
                Some(version) => version.id,
                None => {
                    create_dataset_version(
                        &client,
                        dataset_id,
                        Some(CreateDatasetVersionInput {
                            dataset_version_id: None,
                            version: Some(semver.to_string()),
                        }),
                    )
                    .await?
                }
            }
        }
    };

    let write_options = args.write.parse()?;

    let (schema, stream) = open(
        &args.src,
        OpenOptions {
            format: *args.format,
            infer: *args.infer,
            read: read::Options {
                batch_size: Some(args.record_batch_size),
            },
            progress: Some(pb.clone()),
            schema: None,
        },
        &global,
        args.schema_output,
    )
    .await?;

    pb.println("\n");
    pb.set_message("Uploading...");
    pb.set_style(crate::progress_bar::pretty_bytes());

    let mut writer_client = client.clone();
    writer_client.s3_layer(S3ChecksumLayer::new(Crc32::new()));
    writer_client.s3_layer(BackoffRetryLayer::new(
        RetryStatusCodeRange::for_client_and_server_errors(),
        ExponentialBackoffBuilder::default(),
    ));
    let writer = DatasetVersionFileUploader::new(writer_client, &dataset_version_id)
        .with_concurrency(Some(args.writer.concurrent_uploads))
        .with_max_partition_size(Some(args.writer.max_part_size));

    let written_records =
        write::ParquetStream::new(stream, writer, schema, write_options, args.buffer.parse())
            .try_fold(0usize, async |acc, (_part, meta)| {
                Ok(acc + 0usize.saturating_add_signed(meta.num_rows as _))
            })
            .await
            .map_err(|err| {
                error::user(
                    &format!("An error occurred while writing to the output file: {err}"),
                    "Please check the file format and try again",
                )
            })?;

    let _ = client
        .send::<FinishDatasetVersionUpload>(finish_dataset_version_upload::Variables {
            dataset_version_id,
        })
        .await?;

    pb.set_style(indicatif::ProgressStyle::default_spinner());
    pb.finish_with_message(format!("{written_records} records written",));
    Ok(())
}
