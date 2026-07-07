use clap::Args;
use serde::Serialize;
use std::path::PathBuf;

use crate::{
    commands::{
        model::common::{get_provider_model, resolve_provider_model_id},
        GlobalArgs,
    },
    download::download_stream_to_file,
    error::Result,
};

#[derive(Args, Debug, Serialize)]
pub struct Download {
    /// The provider model to download, given as a UUID or a global node id
    id: String,
    /// Where to write the payload (defaults to <uuid>.json in the current directory)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

pub async fn download(args: Download, global: GlobalArgs) -> Result<()> {
    let id = resolve_provider_model_id(&args.id)?;
    let client = global.graphql_client().await?;
    let model = get_provider_model(&client, &id).await?;

    let output = args
        .output
        .unwrap_or_else(|| PathBuf::from(format!("{}.json", id.id)));

    let pb = global
        .spinner()
        .with_message(format!("Downloading provider model {}", id.id));
    download_stream_to_file(&client, model.download_url, &output, &pb).await?;
    pb.finish_with_message(format!("Downloaded provider model to {}", output.display()));

    Ok(())
}
