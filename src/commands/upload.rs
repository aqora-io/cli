use crate::{
    compress::compress,
    error::{self, Error, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
    id::{Id, NodeType},
    python::build_package,
};
use clap::Args;
use futures::prelude::*;
use graphql_client::GraphQLQuery;
use indicatif::{MultiProgress, ProgressBar};
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use tempfile::tempdir;
use url::Url;

#[derive(Serialize, Deserialize, Debug)]
pub struct AqoraConfig {
    pub data: Option<PathBuf>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Tools {
    pub aqora: Option<AqoraConfig>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PyProject {
    pub build_system: Option<pyproject_toml::BuildSystem>,
    pub project: Option<pyproject_toml::Project>,
    pub tool: Option<Tools>,
}

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Upload {
    #[arg(short, long, default_value = "https://app.aqora.io")]
    pub url: String,
    #[arg(short, long, default_value = ".")]
    pub project_dir: PathBuf,
}

pub enum PackageName {
    UseCase { competition: Id },
    Submission { competition: Id, user: Id },
}

impl FromStr for PackageName {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(package_id) = s.strip_prefix("use-case-") {
            Id::parse_package_id(package_id, NodeType::Competition)
                .map_err(|err| {
                    error::user(
                        &format!("Invalid package id: {}", err),
                        "Make sure the package id is valid",
                    )
                })
                .map(|competition| PackageName::UseCase { competition })
        } else if let Some(package_id) = s.strip_prefix("submission-") {
            if let Some((competition, user)) = package_id.split_once('-') {
                let competition = Id::parse_package_id(competition, NodeType::Competition)
                    .map_err(|err| {
                        error::user(
                            &format!("Invalid package id: {}", err),
                            "Make sure the package id is valid",
                        )
                    })?;
                let user = Id::parse_package_id(user, NodeType::User).map_err(|err| {
                    error::user(
                        &format!("Invalid package id: {}", err),
                        "Make sure the package id is valid",
                    )
                })?;
                Ok(PackageName::Submission { competition, user })
            } else {
                Err(error::user(
                    "Invalid package name: Missing user id",
                    "Make sure the package id is valid",
                ))
            }
        } else {
            Err(error::user(
                "Invalid package name",
                "Make sure the package name starts with either 'use-case' or 'submission'",
            ))
        }
    }
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

pub async fn upload_use_case(args: Upload, competition_id: Id, pyproject: PyProject) -> Result<()> {
    let tempdir = tempdir().map_err(|err| {
        error::user(
            &format!("could not create temporary directory: {}", err),
            "Please make sure you have permission to create temporary directories",
        )
    })?;
    let aqora_config = pyproject.tool.and_then(|tool| tool.aqora);
    let version = pyproject
        .project
        .as_ref()
        .and_then(|project| project.version.to_owned())
        .ok_or_else(|| {
            error::user(
                "No version given",
                "Make sure the version is set in the project section \
                        of your pyproject.toml",
            )
        })?;
    let package_name = format!("use-case-{}-{}", competition_id.to_package_id(), version);

    let data_path = aqora_config
        .and_then(|config| config.data)
        .map(|path| args.project_dir.join(path));

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
            build_package(&args.project_dir, tempdir.path()).await?;
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
    let pyproject_path = args.project_dir.join("pyproject.toml");
    let pyproject: PyProject =
        toml::from_str(&std::fs::read_to_string(&pyproject_path).map_err(|err| {
            error::user(
                &format!("could not read {}: {}", pyproject_path.display(), err),
                "Please run this command in the root of your project or set the --project-dir flag",
            )
        })?)
        .map_err(|err| {
            error::user(
                &format!("could not read {}: {}", pyproject_path.display(), err),
                "Please make sure your pyproject.toml is valid",
            )
        })?;
    let name = pyproject
        .project
        .as_ref()
        .map(|project| project.name.to_owned())
        .ok_or_else(|| {
            error::user(
                "No name given",
                "Make sure the name is set in the project section \
                        of your pyproject.toml and it matches the competition",
            )
        })?;
    match name.parse()? {
        PackageName::UseCase { competition } => upload_use_case(args, competition, pyproject).await,
        PackageName::Submission { .. } => {
            todo!()
        }
    }
}
