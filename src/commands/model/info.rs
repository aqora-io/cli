use clap::Args;
use serde::Serialize;

use crate::{
    commands::{
        model::common::{get_provider_model, resolve_provider_model_id},
        GlobalArgs,
    },
    error::Result,
};

#[derive(Args, Debug, Serialize)]
pub struct Info {
    /// The provider model to inspect, given as a UUID or a global node id
    id: String,
}

pub async fn info(args: Info, global: GlobalArgs) -> Result<()> {
    let id = resolve_provider_model_id(&args.id)?;
    let client = global.graphql_client().await?;
    let model = get_provider_model(&client, &id).await?;

    let mut table = global.table();
    table.set_header(vec!["ID", "Created At"]);
    table.add_row(vec![model.id, model.created_at.to_rfc3339()]);
    println!("{table}");

    Ok(())
}
