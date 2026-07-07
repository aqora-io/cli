use clap::Args;
use graphql_client::GraphQLQuery;
use serde::Serialize;
use std::path::PathBuf;
use url::Url;

use crate::{
    commands::GlobalArgs,
    download::download_stream_to_file,
    error::{self, Result},
    id::{Id, NodeType},
};

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/provider_job_model.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct ProviderJobModel;

#[derive(Args, Debug, Serialize)]
pub struct Model {
    /// The provider job whose model payload to download, as a UUID or global node id
    id: String,
    /// Where to write the payload (defaults to <uuid>.json in the current directory)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

pub async fn model(args: Model, global: GlobalArgs) -> Result<()> {
    let id = Id::parse_lenient(&args.id, NodeType::ProviderJob).map_err(|err| {
        error::user(
            &format!("Invalid provider job id '{}': {err}", args.id),
            "Pass a provider job UUID or its global node id.",
        )
    })?;
    let client = global.graphql_client().await?;
    let response = client
        .send::<ProviderJobModel>(provider_job_model::Variables {
            id: id.to_node_id(),
        })
        .await?;
    let model = match response.node {
        provider_job_model::ProviderJobModelNode::ProviderJob(job) => job.provider_model,
        _ => {
            return Err(error::user(
                "Not a provider job",
                "The given id does not refer to a provider job.",
            ))
        }
    };

    let model_uuid = Id::parse_node_id(&model.id)
        .map(|id| id.id.to_string())
        .unwrap_or_else(|_| model.id.clone());
    let output = args
        .output
        .unwrap_or_else(|| PathBuf::from(format!("{model_uuid}.json")));

    let pb = global
        .spinner()
        .with_message(format!("Downloading model for job {}", id.id));
    download_stream_to_file(&client, model.download_url, &output, &pb).await?;
    pb.finish_with_message(format!("Downloaded provider model to {}", output.display()));

    Ok(())
}
