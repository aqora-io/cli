use aqora_client::{Client, CredentialsProvider, GraphQLQuery};
use futures::future::{FutureExt, LocalBoxFuture};
use std::ops::Deref;
use url::Url;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/aqora_client/mod.graphql",
    schema_path = "../schema.graphql"
)]
pub struct CreateDatasetVersionFileMutation;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/aqora_client/mod.graphql",
    schema_path = "../schema.graphql"
)]
pub struct UploadDatasetVersionFilePartMutation;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/aqora_client/mod.graphql",
    schema_path = "../schema.graphql"
)]
pub struct CompleteDatasetVersionFileMutation;

#[derive(Clone)]
pub struct WrappedClient<C>(Client<C>);

impl<C> Deref for WrappedClient<C> {
    type Target = Client<C>;
    fn deref(&self) -> &Client<C> {
        &self.0
    }
}

impl<C> WrappedClient<C>
where
    C: CredentialsProvider + Clone + Send + Sync + 'static,
{
    pub fn new(client: Client<C>) -> Self {
        Self(client)
    }

    fn client(&self) -> Client<C> {
        self.0.clone()
    }

    pub fn create_dataset_version_file(
        &self,
        dataset_version_id: String,
        partition: usize,
    ) -> LocalBoxFuture<'static, aqora_client::Result<String>> {
        let client = self.client();
        async move {
            Ok(client
                .send::<CreateDatasetVersionFileMutation>(
                    create_dataset_version_file_mutation::Variables {
                        dataset_version_id,
                        file_name: format!("{partition}.parquet"),
                    },
                )
                .await?
                .create_dataset_version_file)
        }
        .boxed_local()
    }

    pub fn upload_dataset_version_file_part(
        &self,
        dataset_version_file_id: String,
        part: usize,
        part_size: usize,
    ) -> LocalBoxFuture<'static, aqora_client::Result<Url>> {
        let client = self.client();
        async move {
            Ok(client
                .send::<UploadDatasetVersionFilePartMutation>(
                    upload_dataset_version_file_part_mutation::Variables {
                        dataset_version_file_id,
                        part: part as i64,
                        part_size: part_size as i64,
                    },
                )
                .await?
                .upload_dataset_version_file_part)
        }
        .boxed_local()
    }

    pub fn complete_dataset_version_file(
        &self,
        dataset_version_file_id: String,
        e_tags: Vec<String>,
    ) -> LocalBoxFuture<'static, aqora_client::Result<String>> {
        let client = self.client();
        async move {
            Ok(client
                .send::<CompleteDatasetVersionFileMutation>(
                    complete_dataset_version_file_mutation::Variables {
                        dataset_version_file_id,
                        e_tags,
                    },
                )
                .await?
                .complete_dataset_version_file
                .id)
        }
        .boxed_local()
    }
}
