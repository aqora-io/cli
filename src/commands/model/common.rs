use graphql_client::GraphQLQuery;
use url::Url;

use crate::{
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
    id::{Id, NodeType},
};

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/provider_model_info.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct ProviderModelInfo;

pub use provider_model_info::ProviderModelInfoNodeOnProviderModel as ProviderModel;

/// Resolve a user-supplied id (a raw UUID or an already-global node id) into the
/// global node id of a provider model.
pub fn resolve_provider_model_id(input: &str) -> Result<Id> {
    Id::parse_lenient(input, NodeType::ProviderModel).map_err(|err| {
        error::user(
            &format!("Invalid provider model id '{input}': {err}"),
            "Pass a provider model UUID or its global node id.",
        )
    })
}

/// Look up a provider model by id, erroring if the id does not point at one.
pub async fn get_provider_model(client: &GraphQLClient, id: &Id) -> Result<ProviderModel> {
    let response = client
        .send::<ProviderModelInfo>(provider_model_info::Variables {
            id: id.to_node_id(),
        })
        .await?;
    match response.node {
        provider_model_info::ProviderModelInfoNode::ProviderModel(model) => Ok(model),
        _ => Err(error::user(
            "Not a provider model",
            "The given id does not refer to a provider model.",
        )),
    }
}
