use clap::Args;
use futures::{stream, StreamExt, TryStreamExt};
use graphql_client::GraphQLQuery;
use indicatif::{HumanBytes, MultiProgress, ProgressBar};
use serde::Serialize;
use std::path::PathBuf;
use url::Url;

use crate::{
    commands::{
        dataset::{
            common::{get_dataset_by_slug, DatasetCommonArgs},
            download::get_dataset_version_files::GetDatasetVersionFilesNodeOnDatasetVersionFilesNodes,
            version::common::get_dataset_version,
        },
        GlobalArgs,
    },
    download::{multipart_download, MultipartOptions},
    error::{self, Result},
};

#[derive(Args, Debug, Serialize)]
pub struct Download {
    #[command(flatten)]
    common: DatasetCommonArgs,
    #[arg(short, long)]
    version: semver::Version,
    #[arg(short, long)]
    destination: PathBuf,
    #[command(flatten)]
    options: MultipartOptions,
    #[clap(long, default_value_t = 10)]
    concurrency: usize,
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_dataset_version_files.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct GetDatasetVersionFiles;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_dataset_version_file_by_partition.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct GetDatasetVersionFileByPartition;

pub async fn download(args: Download, global: GlobalArgs) -> Result<()> {
    let m = MultiProgress::new();

    let client = global.graphql_client().await?;

    let (owner, local_slug) = args.common.slug_pair()?;
    let multipart_options = args.options;

    let dataset = get_dataset_by_slug(&global, owner, local_slug).await?;
    if !dataset.viewer_can_read_dataset_version_file {
        return Err(error::user(
            "Permission denied",
            "Cannot read dataset files",
        ));
    }

    let dataset_version = get_dataset_version(
        &client,
        dataset.id,
        args.version.major as _,
        args.version.minor as _,
        args.version.patch as _,
    )
    .await?
    .ok_or_else(|| error::user("Not found", "Dataset version not found"))?;

    let response = client
        .send::<GetDatasetVersionFiles>(get_dataset_version_files::Variables {
            dataset_version_id: dataset_version.id,
        })
        .await?;

    let dataset_version_files = match response.node {
        get_dataset_version_files::GetDatasetVersionFilesNode::DatasetVersion(v) => v,
        _ => {
            return Err(error::system(
                "Invalid node type",
                "Unexpected GraphQL response",
            ))
        }
    };

    let nodes = dataset_version_files.files.nodes;
    let dataset_name = dataset_version_files.dataset.name;

    let dataset_dir = args.destination.join(&dataset_name);
    tokio::fs::create_dir_all(&dataset_dir).await?;

    let total_size = dataset_version.size as u64;
    let total_files = nodes.len();

    let overall_progress = m.add(global.spinner().with_message(format!(
        "Downloading '{}' ({} files, {})",
        dataset_name,
        total_files,
        HumanBytes(total_size)
    )));

    stream::iter(nodes)
        .map(|node| {
            let client = client.to_owned();
            let m = m.to_owned();
            let multipart_options = multipart_options.to_owned();
            let dataset_dir = dataset_dir.to_owned();
            let dataset_name = dataset_name.to_owned();

            async move {
                download_partition_file(
                    &m,
                    &client,
                    &multipart_options,
                    &dataset_dir,
                    &dataset_name,
                    node,
                )
                .await
            }
        })
        .buffer_unordered(args.concurrency)
        .try_collect::<()>()
        .await?;

    overall_progress.finish_with_message("Done");

    Ok(())
}

async fn download_partition_file(
    m: &MultiProgress,
    client: &aqora_client::Client,
    multipart_options: &MultipartOptions,
    output_dir: &std::path::Path,
    dataset_name: &str,
    file_node: GetDatasetVersionFilesNodeOnDatasetVersionFilesNodes,
) -> Result<()> {
    let (metadata, url) = match client.s3_head(file_node.url.clone()).await {
        Ok(metadata) => (metadata, file_node.url.clone()),
        // retry if presigned url expired due to long dataset download time
        Err(e) => {
            tracing::warn!(error = %e, "Retrying: failed to fetch object header");
            let response = client
                .send::<GetDatasetVersionFileByPartition>(
                    get_dataset_version_file_by_partition::Variables {
                        dataset_version_id: file_node.dataset_version.id,
                        partition_num: file_node.partition_num,
                    },
                )
                .await?;

            let dataset_version_file = match response.node {
            get_dataset_version_file_by_partition::GetDatasetVersionFileByPartitionNode::DatasetVersion(v) => v,
            _ => {
                return Err(error::system(
                    "Invalid node type",
                    "Unexpected GraphQL response",
                ));
            }
        };
            let file_by_partition_num = match dataset_version_file.file_by_partition_num {
                Some(file_by_partition_num) => file_by_partition_num,
                None => {
                    return Err(error::system(
                        "Invalid partition number",
                        "The partition does not exist",
                    ))
                }
            };

            let file_url = file_by_partition_num.url;
            (client.s3_head(file_url.clone()).await?, file_url)
        }
    };

    let filename = format!("{}-{}.parquet", dataset_name, file_node.partition_num);
    let output_path = output_dir.join(&filename);

    if let Ok(existing) = tokio::fs::metadata(&output_path).await {
        if existing.len() == metadata.size {
            return Ok(());
        }
    }

    tokio::fs::create_dir_all(output_path.parent().unwrap()).await?;

    let temp = tempfile::NamedTempFile::new_in(output_dir)?;
    let temp_path = temp.path().to_owned();

    let pb = m.add(ProgressBar::new_spinner());
    pb.set_message(filename);

    multipart_download(
        &client,
        metadata.size,
        url,
        multipart_options,
        &temp_path,
        &pb,
    )
    .await?;

    pb.finish_and_clear();
    tokio::fs::rename(&temp_path, &output_path).await?;

    Ok(())
}
