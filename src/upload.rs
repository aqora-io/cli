use std::path::Path;

use futures::prelude::*;
use graphql_client::GraphQLQuery;
use indicatif::ProgressBar;
use reqwest::{
    header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE},
    Body, Response,
};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio_util::io::ReaderStream;
use url::Url;

use crate::{
    error::{self, Result},
    graphql_client::GraphQLClient,
    id::Id,
    progress_bar::{self, TempProgressStyle},
};

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/create_multipart_upload.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
struct CreateMultipartUpload;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/complete_multipart_upload.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
struct CompleteMultipartUpload;

// const CHUNK_SIZE: u64 = 1024 * 1024 * 100;
const CHUNK_SIZE: u64 = 1024 * 1024 * 10;

async fn do_upload(
    client: &reqwest::Client,
    body: impl AsyncRead + Send + 'static,
    upload_url: &Url,
    content_length: u64,
    content_type: Option<&str>,
    pb: &ProgressBar,
) -> Result<Response> {
    let mut request = client
        .put(upload_url.to_string())
        .header(AUTHORIZATION, "")
        .header(CONTENT_LENGTH, content_length);
    if let Some(content_type) = content_type {
        request = request.header(CONTENT_TYPE, content_type);
    }
    let pb = pb.clone();
    let body = Body::wrap_stream(ReaderStream::new(body).inspect(move |chunk| {
        if let Ok(chunk) = chunk.as_ref() {
            pb.inc(chunk.len() as u64);
        }
    }));
    let response = request.body(body).send().await?;
    if !response.status().is_success() {
        Err(error::system(
            &format!(
                "Could not upload data: [{}] {}",
                response.status(),
                response.text().await.unwrap_or("".to_string())
            ),
            "",
        ))
    } else {
        Ok(response)
    }
}

async fn simple_upload(
    client: &reqwest::Client,
    file: File,
    upload_url: &Url,
    content_length: u64,
    content_type: Option<&str>,
    pb: &ProgressBar,
) -> Result<()> {
    let _guard = TempProgressStyle::new(pb);
    pb.reset();
    pb.set_style(progress_bar::pretty_bytes());
    pb.disable_steady_tick();
    pb.set_position(0);
    pb.set_length(content_length);
    let _ = do_upload(client, file, upload_url, content_length, content_type, pb).await?;
    Ok(())
}

async fn upload_part(
    client: &reqwest::Client,
    path: impl AsRef<Path>,
    chunk_number: u64,
    content_length: u64,
    content_type: Option<&str>,
    upload_url: Url,
    pb: &ProgressBar,
) -> Result<String> {
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(SeekFrom::Start(chunk_number * CHUNK_SIZE))
        .await?;
    let chunk = file.take(CHUNK_SIZE);
    let response = do_upload(client, chunk, &upload_url, content_length, content_type, pb).await?;
    Ok(response
        .headers()
        .get("ETag")
        .ok_or_else(|| error::system("ETag header not found in response", ""))?
        .to_str()
        .map_err(|_| error::system("ETag header is not valid UTF-8", ""))?
        .to_string())
}

async fn multipart_upload(
    client: &GraphQLClient,
    path: impl AsRef<Path>,
    id: &Id,
    content_length: u64,
    content_type: Option<&str>,
    pb: &ProgressBar,
) -> Result<()> {
    let mut chunks = [CHUNK_SIZE].repeat((content_length / CHUNK_SIZE) as usize);
    if content_length % CHUNK_SIZE != 0 {
        chunks.push(content_length % CHUNK_SIZE);
    }
    let create_multipart_upload = client
        .send::<CreateMultipartUpload>(create_multipart_upload::Variables {
            id: id.to_node_id(),
            chunks: chunks.iter().map(|&x| x as i64).collect(),
        })
        .await?
        .create_project_version_file_multipart_upload;

    let _guard = TempProgressStyle::new(pb);
    pb.reset();
    pb.set_style(progress_bar::pretty_bytes());
    pb.disable_steady_tick();
    pb.set_position(0);
    pb.set_length(content_length);

    let e_tags = futures::future::try_join_all(
        create_multipart_upload
            .urls
            .into_iter()
            .enumerate()
            .map(|(i, url)| {
                chunks
                    .get(i)
                    .ok_or_else(|| error::system("Chunk index out of bounds", ""))
                    .map(|content_length| (i, url, *content_length))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|(i, url, content_length)| {
                upload_part(
                    client.inner(),
                    path.as_ref(),
                    i as u64,
                    content_length,
                    content_type,
                    url,
                    pb,
                )
            }),
    )
    .await?;

    let _ = client
        .send::<CompleteMultipartUpload>(complete_multipart_upload::Variables {
            id: id.to_node_id(),
            upload_id: create_multipart_upload.upload_id,
            e_tags,
        })
        .await?;

    Ok(())
}

#[tracing::instrument(ret, err, skip(client, pb))]
pub async fn upload_project_version_file(
    client: &GraphQLClient,
    path: impl AsRef<Path> + std::fmt::Debug,
    id: &Id,
    content_type: Option<&str>,
    upload_url: Option<&Url>,
    pb: &ProgressBar,
) -> Result<()> {
    let file = tokio::fs::File::open(path.as_ref()).await?;
    let content_len = file.metadata().await?.len();
    if content_len < CHUNK_SIZE && upload_url.is_some() {
        simple_upload(
            client.inner(),
            file,
            upload_url.unwrap(),
            content_len,
            content_type,
            pb,
        )
        .await
    } else {
        multipart_upload(client, path, id, content_len, content_type, pb).await
    }
}
