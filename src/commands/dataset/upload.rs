use std::{fmt::Display, path::PathBuf, str::FromStr};

use aqora_client::{
    checksum::{crc32fast::Crc32, S3ChecksumLayer},
    retry::{BackoffRetryLayer, ExponentialBackoffBuilder, RetryStatusCodeRange},
};
use aqora_data_utils::{aqora_client::DatasetVersionFileUploader, infer, read, write, Schema};
use clap::Args;
use futures::{StreamExt as _, TryStreamExt as _};
use graphql_client::GraphQLQuery;
use serde::Serialize;
use thiserror::Error;

use crate::commands::GlobalArgs;
use crate::error::{self, Result};

use super::{
    common::{get_dataset_by_slug, DatasetCommonArgs},
    convert::WriteOptions,
    infer::{render_sample_debug, render_schema, FormatOptions, InferOptions, SchemaOutput},
    utils::from_json_str_or_file,
    version::{
        common::get_dataset_version,
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
    writer: Box<WriterOptions>,
    #[arg(long)]
    schema: Option<String>,
    #[arg(long, default_value_t = 1024)]
    record_batch_size: usize,
    #[arg(long)]
    batch_buffer_size: Option<usize>,
    #[arg(long)]
    writer_max_memory_size: Option<usize>,
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
    #[arg(long, default_value_t = 2)]
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

pub async fn upload(args: Upload, global: GlobalArgs) -> Result<()> {
    let pb = global.spinner();
    let client = global.graphql_client().await?;

    let dataset = get_dataset_by_slug(&global, args.common.slug).await?;

    if !dataset.viewer_can_create_version {
        return Err(error::user(
            "You cannot upload a version for this dataset",
            "Did you type the right slug?",
        ));
    }

    let dataset_id = dataset.id;

    // Find or create a dataset version
    let dataset_version_id = match args.version {
        None => create_dataset_version(&client, dataset_id, None).await?,
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

    let mut writer_client = client.clone();
    writer_client.s3_layer(S3ChecksumLayer::new(Crc32::new()));
    writer_client.s3_layer(BackoffRetryLayer::new(
        RetryStatusCodeRange::for_client_and_server_errors(),
        ExponentialBackoffBuilder::default(),
    ));
    let writer = DatasetVersionFileUploader::new(writer_client, &dataset_version_id)
        .with_concurrency(Some(args.writer.concurrent_uploads))
        .with_max_partition_size(Some(args.writer.max_part_size));
    let mut reader = args.format.open(&args.src).await?;
    let file_len = reader
        .reader()
        .metadata()
        .await
        .map_err(|err| {
            error::user(
                &format!("Could not read metadata from input file: {err}"),
                "Please check the file and try again",
            )
        })?
        .len();
    let write_options = args.write.parse()?;
    let read_options = read::Options {
        batch_size: Some(args.record_batch_size),
    };
    let (schema, stream) = if let Some(schema) = args.schema.as_ref() {
        let schema: Schema = from_json_str_or_file(schema)?;
        let stream = reader
            .stream_values()
            .await
            .map_err(|err| {
                error::user(
                    &format!("Could not read from input file: {err}"),
                    "Please check the file format and try again",
                )
            })?
            .boxed();
        (schema, stream)
    } else {
        pb.set_message(format!("Inferring schema of {}", args.src.display()));
        let infer_options = args.infer.parse()?;
        let mut stream = reader.stream_values().await?;
        let sample_size = args.infer.max_samples();
        let mut samples = if let Some(sample_size) = sample_size {
            Vec::with_capacity(sample_size)
        } else {
            Vec::new()
        };
        while let Some(value) = stream.next().await.transpose().map_err(|err| {
            error::user(
                &format!("Failed to read record: {err}"),
                "Check the data or file and try again",
            )
        })? {
            samples.push(value);
            if sample_size.is_some_and(|s| samples.len() >= s) {
                break;
            }
        }
        let schema = if let Ok(schema) = infer::from_samples(&samples, infer_options.clone()) {
            schema
        } else {
            pb.println(render_sample_debug(
                args.schema_output,
                &global,
                infer::debug_samples(&samples, infer_options),
                &samples,
            )?);
            return Err(error::user(
                "Could not infer the schema from the file given",
                "Please make sure the data is conform or set overwrites with --overwrites",
            ));
        };
        let stream = futures::stream::iter(samples.into_iter().map(std::io::Result::Ok))
            .chain(stream)
            .boxed();
        (schema, stream)
    };

    pb.println(format!(
        "Using schema:\n\n{}\n\n",
        render_schema(args.schema_output, &global, &schema)
            .unwrap_or("Failed to render schema".to_string())
    ));

    pb.set_style(crate::progress_bar::pretty_bytes());
    pb.set_message("Writing...");
    pb.set_length(file_len);
    pb.set_position(0);

    let stream = stream.inspect_ok(|item| pb.set_position(item.end));

    let stream = read::from_value_stream(stream, schema.clone(), read_options).map_err(|err| {
        error::user(
            &format!("Error reading from input file: {err}"),
            "Please check the file and try again",
        )
    })?;

    let written_records = write::ParquetStream::new(
        stream,
        writer,
        schema,
        write_options,
        write::BufferOptions {
            batch_buffer_size: args.batch_buffer_size,
            max_memory_size: args.writer_max_memory_size,
        },
    )
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
