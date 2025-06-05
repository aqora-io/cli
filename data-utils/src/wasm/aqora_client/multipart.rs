use serde::{Deserialize, Serialize};
use ts_rs::TS;
use wasm_bindgen::prelude::*;

use super::client::JsClient;
use crate::{aqora_client::DatasetVersionFileUploader, write::AsyncPartitionWriter};

#[derive(TS, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[ts(export)]
pub struct DatasetVersionFileUploadOptions {
    dataset_version_id: String,
    #[ts(optional)]
    concurrency: Option<usize>,
    #[ts(optional)]
    max_partition_size: Option<usize>,
}

#[wasm_bindgen(js_name = DatasetVersionFileUploader)]
pub struct JsDatasetVersionFileUploader {
    inner: DatasetVersionFileUploader,
}

impl JsDatasetVersionFileUploader {
    pub fn new(client: JsClient, options: DatasetVersionFileUploadOptions) -> Self {
        let inner =
            DatasetVersionFileUploader::new(client.into_inner(), options.dataset_version_id)
                .with_concurrency(options.concurrency)
                .with_max_partition_size(options.max_partition_size);

        Self { inner }
    }
}

#[async_trait::async_trait(?Send)]
impl AsyncPartitionWriter for JsDatasetVersionFileUploader {
    type Writer = <DatasetVersionFileUploader as AsyncPartitionWriter>::Writer;
    async fn next_partition(&mut self) -> std::io::Result<Self::Writer> {
        self.inner.next_partition().await
    }
    fn max_partition_size(&self) -> Option<usize> {
        self.inner.max_partition_size()
    }
}
