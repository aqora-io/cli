use crate::{
    compress::compress,
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
    id::Id,
    pyproject::{PackageName, PyProject},
    python::PyEnv,
};
use clap::Args;
use futures::prelude::*;
use graphql_client::GraphQLQuery;
use indicatif::{MultiProgress, ProgressBar};
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE};
use std::path::{Path, PathBuf};
use tempfile::tempdir;
use url::Url;

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Upload {
    #[arg(short, long, default_value = "https://app.aqora.io")]
    pub url: String,
    #[arg(short, long, default_value = ".")]
    pub project_dir: PathBuf,
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/competition_id_by_slug.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct CompetitionIdBySlug;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/update_use_case.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct UpdateUseCaseMutation;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/validate_use_case.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct ValidateUseCaseMutation;

async fn upload_file(
    client: &reqwest::Client,
    file: impl AsRef<Path>,
    upload_url: &Url,
    content_type: &str,
) -> Result<()> {
    let file = tokio::fs::File::open(file).await?;
    let content_len = file.metadata().await?.len();
    let response = client
        .put(upload_url.to_string())
        .header(CONTENT_LENGTH, content_len)
        .header(CONTENT_TYPE, content_type)
        .body(file)
        .send()
        .await?;
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
        Ok(())
    }
}

pub async fn upload_use_case(args: Upload, competition_id: Id, project: PyProject) -> Result<()> {
    let tempdir = tempdir().map_err(|err| {
        error::user(
            &format!("could not create temporary directory: {}", err),
            "Please make sure you have permission to create temporary directories",
        )
    })?;
    let version = project.version()?;
    let package_name = format!("use-case-{}-{}", competition_id.to_package_id(), version);
    let data_path = project.aqora().data.map(|path| args.project_dir.join(path));

    let data_path = if let Some(data_path) = data_path.as_ref() {
        if !data_path.exists() {
            return Err(error::user(
                &format!("{} does not exist", data_path.display()),
                "Please make sure the data directory exists",
            ));
        }
        data_path
    } else {
        return Err(error::user(
            "No data directory given",
            "Please add a data directory to 'tools.aqora' in your pyproject.toml",
        ));
    };

    let m = MultiProgress::new();

    let mut use_case_pb = ProgressBar::new_spinner().with_message("Updating version");
    use_case_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    use_case_pb = m.add(use_case_pb);

    let client = GraphQLClient::new(args.url.parse()?).await?;
    let use_case = client
        .send::<UpdateUseCaseMutation>(update_use_case_mutation::Variables {
            competition_id: competition_id.to_node_id(),
            version: version.to_string(),
        })
        .await?
        .update_use_case
        .node;

    use_case_pb.finish_with_message("Version updated");

    let s3_client = reqwest::Client::new();

    let data_fut = {
        let upload_url = if let Some(url) = use_case.data_set.upload_url.as_ref() {
            url
        } else {
            return Err(error::system(
                "No upload URL found",
                "This is a bug, please report it",
            ));
        };
        let data_tar_file = tempdir.path().join(format!("{package_name}.data.tar.gz"));
        let mut data_pb = ProgressBar::new_spinner().with_message("Compressing data");
        data_pb.enable_steady_tick(std::time::Duration::from_millis(100));
        data_pb = m.add(data_pb);

        let data_pb_cloned = data_pb.clone();
        let client = s3_client.clone();
        async move {
            compress(data_path, "data", &data_tar_file)?;
            data_pb_cloned.set_message("Uploading data");
            upload_file(&client, data_tar_file, upload_url, "application/gzip").await
        }
        .map(move |res| {
            if res.is_ok() {
                data_pb.finish_with_message("Data uploaded");
            } else {
                data_pb.finish_with_message("An error occurred while processing data");
            }
            res
        })
        .boxed()
    };

    let package_fut = {
        let upload_url = if let Some(url) = use_case.package.upload_url.as_ref() {
            url
        } else {
            return Err(error::system(
                "No upload URL found",
                "This is a bug, please report it",
            ));
        };
        let package_tar_file = tempdir.path().join(format!("{package_name}.tar.gz"));
        let mut package_pb = ProgressBar::new_spinner().with_message("Building package");
        package_pb.enable_steady_tick(std::time::Duration::from_millis(100));
        package_pb = m.add(package_pb);

        let package_pb_cloned = package_pb.clone();
        let client = s3_client.clone();
        async move {
            package_pb_cloned.set_message("Initializing Python environment");
            let env = PyEnv::init(&args.project_dir).await?;
            package_pb_cloned.set_message("Building package");
            env.build_package(&args.project_dir, tempdir.path(), Some(&package_pb_cloned))
                .await?;
            package_pb_cloned.set_message("Uploading package");
            upload_file(&client, package_tar_file, upload_url, "application/gzip").await
        }
        .map(move |res| {
            if res.is_ok() {
                package_pb.finish_with_message("Package uploaded");
            } else {
                package_pb.finish_with_message("An error occurred while processing package");
            }
            res
        })
        .boxed()
    };

    futures::future::try_join(data_fut, package_fut).await?;

    let mut validate_pb = ProgressBar::new_spinner().with_message("Validating use case");
    validate_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    validate_pb = m.add(validate_pb);

    let _ = client
        .send::<ValidateUseCaseMutation>(validate_use_case_mutation::Variables { id: use_case.id })
        .await?;

    validate_pb.finish_with_message("Done!");

    Ok(())
}

pub async fn upload(args: Upload) -> Result<()> {
    let project = PyProject::for_project(&args.project_dir)?;
    match project.name()? {
        PackageName::UseCase { competition } => upload_use_case(args, competition, project).await,
        PackageName::Submission { .. } => Err(error::system("Submissions not supported yet", "")),
    }
}
