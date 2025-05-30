use std::path::PathBuf;
use std::str::FromStr as _;

use aqora_data_utils::infer;
use clap::Args;
use futures::{StreamExt as _, TryStreamExt as _};
use graphql_client::GraphQLQuery;
use indicatif::ProgressBar;
use serde::Serialize;
use tokio::fs::create_dir_all;

use crate::{
    commands::{
        data::{render_sample_debug, FormatOptions, InferOptions, SchemaOutput},
        GlobalArgs,
    },
    dataset::{DatasetConfig, DatasetRootConfig, DatasetVersion},
    error::{self, Result},
    graphql_client::custom_scalars::*,
};

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/dataset_info.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
struct DatasetInfo;

#[derive(Args, Debug, Serialize)]
pub struct Dataset {
    /// Name of the dataset
    name: String,
    /// Path to dataset file
    file: PathBuf,
    /// Dataset version
    #[arg(short, long)]
    version: Option<DatasetVersion>,

    #[command(flatten)]
    format: Box<FormatOptions>,
    #[command(flatten)]
    infer: Box<InferOptions>,
}

pub async fn dataset(args: Dataset, global: GlobalArgs) -> Result<()> {
    let pb = global
        .spinner()
        .with_message(format!("Creating dataset for '{}'", args.name));

    let graphql = global.graphql_client().await?;
    let dataset_info = graphql
        .send::<DatasetInfo>(dataset_info::Variables {
            slug: args.name.clone(),
        })
        .await?;

    if dataset_info.dataset_by_slug.is_none() {
        return Err(error::user(
            &format!("Dataset {:?} does not exist", args.name),
            "Please verify the name is spelled correctly",
        ));
    }

    if !global.project.is_dir() {
        if global.project.exists() {
            return Err(error::user(
                &format!(
                    "Destination {} must be a directory",
                    global.project.display()
                ),
                "",
            ));
        }

        create_dir_all(&global.project).await?;
    }

    let config_path = global.project.join("aqora.toml");
    let mut config = if config_path.exists() {
        let config_data = tokio::fs::read_to_string(&config_path).await?;
        toml::from_str::<DatasetRootConfig>(&config_data)?
    } else {
        let create_config = pb.suspend(|| {
            global
                .confirm()
                .with_prompt(format!("Do you wish to create {:?}", config_path.display()))
                .default(false)
                .interact()
        })?;
        if !create_config {
            return Err(error::user(
                "Dataset creation was aborted",
                &format!(
                    "You may otherwise write by yourself the file at {:?}",
                    config_path.display()
                ),
            ));
        }
        DatasetRootConfig::default()
    };

    let infer_options = args.infer.parse()?;
    let mut reader = args.format.open(&args.file).await?;
    let mut stream = reader
        .stream_values()
        .await
        .map_err(|e| error::user("Could not read values from file", &format!("Error: {}", e)))?
        .boxed_local();
    if let Some(max_samples) = args.infer.max_samples() {
        stream = stream.take(max_samples).boxed_local();
    }
    let values = stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| error::user("Could not read values from file", &format!("Error: {}", e)))?;
    let Ok(schema) = infer::from_samples(&values, infer_options.clone()) else {
        pb.println(render_sample_debug(
            SchemaOutput::Table,
            &global,
            infer::debug_samples(&values, infer_options),
            &values,
        )?);
        return Err(error::user(
            "Could not infer the schema from the file given",
            "Please make sure the data is conform or set overwrites with --overwrites",
        ));
    };

    if let Some(dataset_config) = config.aqora.dataset.get_mut(&args.name) {
        let version = if let Some(version) = args.version {
            version
        } else if schema == dataset_config.schema {
            let next_version = dataset_config.version.next_patch();
            let should_increment = pb.suspend(|| {
                global
                    .confirm()
                    .default(true)
                    .with_prompt(format!(
                        "Do you want to increment version to {next_version} ?"
                    ))
                    .interact()
            })?;
            if should_increment {
                next_version
            } else {
                input_version(&global, &pb)?
            }
        } else {
            input_version(&global, &pb)?
        };

        dataset_config.version = version;
    } else {
        let version = args
            .version
            .map(Ok)
            .unwrap_or_else(|| input_version(&global, &pb))?;

        config.aqora.dataset.insert(
            args.name,
            DatasetConfig {
                path: args.file,
                version,
                authors: vec![],
                schema,
                format: *args.format,
            },
        );
    }

    tokio::fs::write(&config_path, toml::to_string_pretty(&config)?.into_bytes()).await?;

    pb.finish_with_message(format!("Created dataset in {:?}", config_path.display()));
    Ok(())
}

fn input_version(global: &GlobalArgs, pb: &ProgressBar) -> Result<DatasetVersion> {
    pb.suspend(|| loop {
        let version = global
            .input()
            .with_prompt("Please enter a version")
            .with_default(DatasetVersion::ONE.to_string())
            .interact_text()
            .map_err(error::Error::from)?;
        let version = match DatasetVersion::from_str(&version) {
            Ok(version) => version,
            Err(error) => {
                if global.no_prompt {
                    return Err(error::user(
                        "Invalid version",
                        "Please enter a valid version",
                    ));
                } else {
                    pb.println(error.to_string());
                    continue;
                }
            }
        };
        return Result::Ok(version);
    })
}
