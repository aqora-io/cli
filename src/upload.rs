use std::path::Path;

use aqora_client::{
    checksum::DefaultChecksum,
    error::BoxError,
    multipart::{BufferOptions, Multipart, MultipartUpload, DEFAULT_PART_SIZE},
    s3, Client, GraphQLQuery,
};
use futures::future::{BoxFuture, FutureExt};
use indicatif::ProgressBar;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

use crate::{
    error::Result,
    graphql_client::GraphQLClient,
    id::Id,
    progress_bar::{self, TempProgressStyle},
};

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/create_multipart_upload.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
struct CreateMultipartUpload;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/part_upload.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
struct PartUpload;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/complete_multipart_upload.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
struct CompleteMultipartUpload;

struct ProjectVersionFileMultipart {
    id: String,
    pb: ProgressBar,
}

impl ProjectVersionFileMultipart {
    fn new(id: &Id, pb: &ProgressBar) -> Self {
        Self {
            id: id.to_node_id(),
            pb: pb.clone(),
        }
    }
}

impl Multipart for ProjectVersionFileMultipart {
    type File = String;
    type Output = ();
    fn create(&self, client: &Client) -> BoxFuture<'static, Result<Self::File, BoxError>> {
        let client = client.clone();
        let variables = create_multipart_upload::Variables {
            id: self.id.clone(),
        };
        async move {
            Ok(client
                .send::<CreateMultipartUpload>(variables)
                .await?
                .create_project_version_file_multipart_upload
                .upload_id)
        }
        .boxed()
    }
    fn create_part(
        &self,
        client: &Client,
        file: &Self::File,
        index: usize,
        size: usize,
    ) -> BoxFuture<'static, Result<Url, BoxError>> {
        self.pb.inc(size as u64);
        self.retry_part(client, file, index, size)
    }
    fn retry_part(
        &self,
        client: &Client,
        file: &Self::File,
        index: usize,
        size: usize,
    ) -> BoxFuture<'static, Result<Url, BoxError>> {
        let client = client.clone();
        let variables = part_upload::Variables {
            id: self.id.clone(),
            upload_id: file.clone(),
            chunk: index as i64,
            chunk_len: size as i64,
        };
        async move {
            Ok(client
                .send::<PartUpload>(variables)
                .await?
                .upload_project_version_file_part)
        }
        .boxed()
    }
    fn complete(
        &self,
        client: &Client,
        file: &Self::File,
        etags: Vec<String>,
    ) -> BoxFuture<'static, Result<Self::Output, BoxError>> {
        let client = client.clone();
        let variables = complete_multipart_upload::Variables {
            id: self.id.clone(),
            upload_id: file.clone(),
            e_tags: etags,
        };
        async move {
            let _ = client.send::<CompleteMultipartUpload>(variables).await?;
            Ok(())
        }
        .boxed()
    }
}

#[tracing::instrument(ret, err, skip(client, pb))]
pub async fn upload_project_version_file(
    client: &GraphQLClient,
    path: impl AsRef<Path> + std::fmt::Debug,
    id: &Id,
    content_type: Option<&str>,
    upload_url: &Url,
    pb: &ProgressBar,
) -> Result<()> {
    let mut file = File::open(path).await?;
    let len = file.metadata().await?.len() as usize;
    if len > DEFAULT_PART_SIZE {
        let mut upload =
            MultipartUpload::new(client.clone(), ProjectVersionFileMultipart::new(id, pb))
                .with_buffer_options(
                    BufferOptions::default()
                        .for_total_size(len)
                        .for_concurrency(3),
                )
                .expect("Buffer can never be too small with positive concurrency");
        upload = async move {
            let _guard = TempProgressStyle::new(pb);
            pb.reset();
            pb.set_style(progress_bar::pretty_bytes());
            pb.disable_steady_tick();
            pb.set_position(0);
            pb.set_length(len as u64);
            tokio::io::copy(&mut file, &mut upload)
                .await
                .map(|_| upload)
        }
        .await?;
        pb.set_message("Finishing upload");
        upload.shutdown().await?;
    } else {
        let mut bytes = Vec::with_capacity(len);
        file.read_to_end(&mut bytes).await?;
        s3::upload(
            client.inner(),
            upload_url.clone(),
            bytes,
            DefaultChecksum::default(),
            Default::default(),
        )
        .await?;
    }
    Ok(())
}
