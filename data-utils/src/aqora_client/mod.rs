use std::io;

use aqora_client::{
    error::BoxError,
    multipart::{BufferOptions, Multipart, MultipartUpload},
    Client, GraphQLQuery,
};
use async_trait::async_trait;
use url::Url;

use crate::write::AsyncPartitionWriter;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/aqora_client/graphql/CreateDatasetVersionFileMutation.graphql",
    schema_path = "../schema.graphql"
)]
struct CreateDatasetVersionFileMutation;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/aqora_client/graphql/UploadDatasetVersionFilePartMutation.graphql",
    schema_path = "../schema.graphql"
)]
struct UploadDatasetVersionFilePartMutation;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/aqora_client/graphql/CompleteDatasetVersionFileMutation.graphql",
    schema_path = "../schema.graphql"
)]
struct CompleteDatasetVersionFileMutation;

pub struct DatasetVersionFileUploader {
    client: Client,
    dataset_version_id: String,
    concurrency: Option<usize>,
    max_partition_size: Option<usize>,
    partition_num: usize,
}

impl DatasetVersionFileUploader {
    pub fn new(client: Client, dataset_version_id: impl Into<String>) -> Self {
        Self {
            client,
            dataset_version_id: dataset_version_id.into(),
            concurrency: None,
            max_partition_size: None,
            partition_num: 0,
        }
    }

    pub fn with_concurrency(mut self, concurrency: Option<usize>) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn with_max_partition_size(mut self, max_partition_size: Option<usize>) -> Self {
        self.max_partition_size = max_partition_size;
        self
    }
}

#[cfg_attr(feature = "parquet-no-send", async_trait(?Send))]
#[cfg_attr(not(feature = "parquet-no-send"), async_trait)]
impl AsyncPartitionWriter for DatasetVersionFileUploader {
    type Writer = MultipartUpload<DatasetVersionFileMultipart>;
    async fn next_partition(&mut self) -> io::Result<Self::Writer> {
        let mut buffer_options = BufferOptions::default();
        if let Some(max_partition_size) = self.max_partition_size {
            const WIGGLE_ROOM: usize = 100 * 1024 * 1024; // 100 MB
            buffer_options = buffer_options.for_total_size(max_partition_size + WIGGLE_ROOM);
        }
        if let Some(concurrency) = self.concurrency {
            buffer_options = buffer_options.for_concurrency(concurrency);
        }
        let writer = self
            .client
            .multipart(DatasetVersionFileMultipart {
                dataset_version_id: self.dataset_version_id.clone(),
                partition_num: self.partition_num,
            })
            .with_buffer_options(buffer_options)
            .expect("total size and concurrency should produce valid buffer options");
        self.partition_num += 1;
        Ok(writer)
    }
    fn max_partition_size(&self) -> Option<usize> {
        self.max_partition_size
    }
}

#[derive(Debug, Clone)]
pub struct DatasetVersionFileMultipart {
    dataset_version_id: String,
    partition_num: usize,
}

#[cfg_attr(feature = "aqora-client-threaded", async_trait)]
#[cfg_attr(not(feature = "aqora-client-threaded"), async_trait(?Send))]
impl Multipart for DatasetVersionFileMultipart {
    type File = String;
    type Output = String;
    async fn create(&self, client: &Client) -> Result<Self::File, BoxError> {
        Ok(client
            .send::<CreateDatasetVersionFileMutation>(
                create_dataset_version_file_mutation::Variables {
                    dataset_version_id: self.dataset_version_id.clone(),
                    partition_num: self.partition_num as i64,
                },
            )
            .await?
            .create_dataset_version_file)
    }
    async fn create_part(
        &self,
        client: &Client,
        file: &Self::File,
        index: usize,
        size: usize,
    ) -> Result<Url, BoxError> {
        Ok(client
            .send::<UploadDatasetVersionFilePartMutation>(
                upload_dataset_version_file_part_mutation::Variables {
                    dataset_version_file_id: file.clone(),
                    part: index as i64,
                    part_size: size as i64,
                },
            )
            .await?
            .upload_dataset_version_file_part)
    }
    async fn complete(
        &self,
        client: &Client,
        file: &Self::File,
        etags: Vec<String>,
    ) -> Result<Self::Output, BoxError> {
        Ok(client
            .send::<CompleteDatasetVersionFileMutation>(
                complete_dataset_version_file_mutation::Variables {
                    dataset_version_file_id: file.clone(),
                    e_tags: etags,
                },
            )
            .await?
            .complete_dataset_version_file
            .id)
    }
}
