#![allow(clippy::all, warnings)]
pub struct CreateDataset;
pub mod create_dataset {
    #![allow(dead_code)]
    use std::result::Result;
    pub const OPERATION_NAME: &str = "CreateDataset";
    pub const QUERY : & str = "mutation CreateDataset($owner: UsernameOrID, $input: CreateDatasetInput!) {\n  createDataset(asEntity: $owner, input: $input) {\n    id\n    localSlug\n    owner {\n      __typename\n      username\n    }\n    viewerCanCreateVersion: viewerCan(action: CREATE_DATASET_VERSION)\n  }\n}\n" ;
    use super::*;
    use serde::{Deserialize, Serialize};
    #[allow(dead_code)]
    type Boolean = bool;
    #[allow(dead_code)]
    type Float = f64;
    #[allow(dead_code)]
    type Int = i64;
    #[allow(dead_code)]
    type ID = String;
    type UsernameOrID = super::UsernameOrID;
    #[derive(Serialize)]
    pub struct CreateDatasetInput {
        #[serde(rename = "localSlug")]
        pub local_slug: String,
        pub name: String,
        pub private: Boolean,
        pub tags: Vec<ID>,
    }
    #[derive(Serialize)]
    pub struct Variables {
        pub owner: Option<UsernameOrID>,
        pub input: CreateDatasetInput,
    }
    impl Variables {}
    #[derive(Deserialize)]
    pub struct ResponseData {
        #[serde(rename = "createDataset")]
        pub create_dataset: CreateDatasetCreateDataset,
    }
    #[derive(Deserialize)]
    pub struct CreateDatasetCreateDataset {
        pub id: ID,
        #[serde(rename = "localSlug")]
        pub local_slug: String,
        pub owner: CreateDatasetCreateDatasetOwner,
        #[serde(rename = "viewerCanCreateVersion")]
        pub viewer_can_create_version: Boolean,
    }
    #[derive(Deserialize)]
    pub struct CreateDatasetCreateDatasetOwner {
        pub username: String,
        #[serde(flatten)]
        pub on: CreateDatasetCreateDatasetOwnerOn,
    }
    #[derive(Deserialize)]
    #[serde(tag = "__typename")]
    pub enum CreateDatasetCreateDatasetOwnerOn {
        Organization,
        User,
    }
}
impl graphql_client::GraphQLQuery for CreateDataset {
    type Variables = create_dataset::Variables;
    type ResponseData = create_dataset::ResponseData;
    fn build_query(variables: Self::Variables) -> ::graphql_client::QueryBody<Self::Variables> {
        graphql_client::QueryBody {
            variables,
            query: create_dataset::QUERY,
            operation_name: create_dataset::OPERATION_NAME,
        }
    }
}
