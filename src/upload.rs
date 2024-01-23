use crate::{
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
};
use clap::Args;
use flate2::GzBuilder;
use graphql_client::GraphQLQuery;
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use std::{fs::File, path::PathBuf};
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

pub async fn upload(args: Upload) -> Result<()> {
    let tempdir = tempdir().map_err(|err| {
        error::user(
            &format!("could not create temporary directory: {}", err),
            "Please make sure you have permission to create temporary directories",
        )
    })?;

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
    let aqora_config = pyproject.tool.and_then(|tool| tool.aqora);
    let slug = pyproject
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

    let package_name = format!("{}-{}", slug, version);

    let data_path = aqora_config
        .and_then(|config| config.data)
        .map(|path| args.project_dir.join(path));

    let data_tar_path = if let Some(path) = data_path {
        let destination = tempdir.path().join(format!("{package_name}.data.tar.gz"));
        let mut file = File::create(destination.clone()).map_err(|err| {
            error::user(
                &format!("Could not create {}: {}", destination.display(), err),
                "Please make sure you have permission to create files in this directory",
            )
        })?;
        let mut gz = GzBuilder::new().write(&mut file, Default::default());
        let mut tar = tar::Builder::new(&mut gz);
        tar.append_dir_all("data", path.clone()).map_err(|err| {
            error::user(
                &format!(
                    "Could not add data contents to tar from {}: {}",
                    path.display(),
                    err
                ),
                "Please make sure the data directory exists and you have permission to read it",
            )
        })?;
        tar.finish()
            .map_err(|err| error::system(&format!("Could not finish tar: {}", err), ""))?;
        drop(tar);
        gz.finish()
            .map_err(|err| error::system(&format!("Could not finish gz: {}", err), ""))?;
        Some(destination)
    } else {
        None
    };

    let client = GraphQLClient::new(args.url.parse()?).await?;
    let competition_id = client
        .send::<CompetitionIdBySlug>(competition_id_by_slug::Variables { slug })
        .await?
        .competition_by_slug
        .ok_or_else(|| {
            error::user(
                "Competition not found",
                "Please check the competition name and try again",
            )
        })?
        .id;
    let use_case = client
        .send::<UpdateUseCaseMutation>(update_use_case_mutation::Variables {
            competition_id,
            version: version.to_string(),
        })
        .await?
        .update_use_case
        .node;

    let s3_client = reqwest::Client::new();

    if let Some(path) = data_tar_path {
        let file = tokio::fs::File::open(path).await?;
        let content_len = file.metadata().await?.len();
        let url = use_case.data_set.upload_url.as_ref().ok_or_else(|| {
            error::system("No data set upload URL", "This is a bug, please report it")
        })?;
        let response = s3_client
            .put(url.to_string())
            .header(CONTENT_LENGTH, content_len)
            .header(CONTENT_TYPE, "application/gzip")
            .body(file)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(error::system(
                &format!(
                    "Could not upload data: [{}] {}",
                    response.status(),
                    response.text().await.unwrap_or("".to_string())
                ),
                "",
            ));
        }
    }

    Ok(())
}
