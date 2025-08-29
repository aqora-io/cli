#![allow(clippy::all, warnings)]
pub struct GetDatasetBySlug;
pub mod get_dataset_by_slug {
    #![allow(dead_code)]
    use std::result::Result;
    pub const OPERATION_NAME: &str = "GetDatasetBySlug";
    pub const QUERY : & str = "query GetDatasetBySlug($owner: String!, $localSlug: String!) {\n  datasetBySlug(owner: $owner, localSlug: $localSlug) {\n    id\n    localSlug\n    owner {\n      __typename\n      username\n    }\n    viewerCanCreateVersion: viewerCan(action: CREATE_DATASET_VERSION)\n  }\n}\n" ;
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
    #[derive(Serialize)]
    pub struct Variables {
        pub owner: String,
        #[serde(rename = "localSlug")]
        pub local_slug: String,
    }
    impl Variables {}
    #[derive(Deserialize)]
    pub struct ResponseData {
        #[serde(rename = "datasetBySlug")]
        pub dataset_by_slug: Option<GetDatasetBySlugDatasetBySlug>,
    }
    #[derive(Deserialize)]
    pub struct GetDatasetBySlugDatasetBySlug {
        pub id: ID,
        #[serde(rename = "localSlug")]
        pub local_slug: String,
        pub owner: GetDatasetBySlugDatasetBySlugOwner,
        #[serde(rename = "viewerCanCreateVersion")]
        pub viewer_can_create_version: Boolean,
    }
    #[derive(Deserialize)]
    pub struct GetDatasetBySlugDatasetBySlugOwner {
        pub username: String,
        #[serde(flatten)]
        pub on: GetDatasetBySlugDatasetBySlugOwnerOn,
    }
    #[derive(Deserialize)]
    #[serde(tag = "__typename")]
    pub enum GetDatasetBySlugDatasetBySlugOwnerOn {
        Organization,
        User,
    }
}
impl graphql_client::GraphQLQuery for GetDatasetBySlug {
    type Variables = get_dataset_by_slug::Variables;
    type ResponseData = get_dataset_by_slug::ResponseData;
    fn build_query(variables: Self::Variables) -> ::graphql_client::QueryBody<Self::Variables> {
        graphql_client::QueryBody {
            variables,
            query: get_dataset_by_slug::QUERY,
            operation_name: get_dataset_by_slug::OPERATION_NAME,
        }
    }
}
