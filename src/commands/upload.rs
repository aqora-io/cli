use crate::{
    commands::GlobalArgs,
    compress::compress,
    dirs::{
        init_venv, project_last_run_dir, project_last_run_result, pyproject_path, read_pyproject,
    },
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
    id::Id,
    python::{build_package, LastRunResult},
    readme::read_readme,
    revert_file::RevertFile,
};
use aqora_config::{PyProject, Version};
use clap::Args;
use futures::prelude::*;
use graphql_client::GraphQLQuery;
use indicatif::{MultiProgress, ProgressBar};
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE};
use std::path::Path;
use tempfile::tempdir;
use url::Url;

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Upload {
    pub competition: Option<String>,
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/competition_id_by_slug.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct CompetitionIdBySlug;

pub async fn get_competition_id_by_slug(
    client: &GraphQLClient,
    slug: impl Into<String>,
) -> Result<Id> {
    let slug = slug.into();
    let competition = client
        .send::<CompetitionIdBySlug>(competition_id_by_slug::Variables { slug: slug.clone() })
        .await?
        .competition_by_slug
        .ok_or_else(|| {
            error::user(
                &format!("Competition '{}' not found", slug),
                "Please make sure the competition is correct",
            )
        })?;
    Id::parse_node_id(competition.id).map_err(|err| {
        error::system(
            &format!("Could not parse competition ID: {}", err),
            "This is a bug, please report it",
        )
    })
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/submission_upload_info.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct SubmissionUploadInfo;

pub struct SubmissionUploadInfoResponse {
    competition_id: Id,
    use_case_version: Version,
    entity_id: Id,
}

pub async fn get_submission_upload_info(
    client: &GraphQLClient,
    slug: impl Into<String>,
    username: Option<impl Into<String>>,
) -> Result<SubmissionUploadInfoResponse> {
    let slug = slug.into();
    let username = username.map(|u| u.into());
    let response = client
        .send::<SubmissionUploadInfo>(submission_upload_info::Variables {
            slug: slug.clone(),
            username: username.clone().unwrap_or_default(),
            use_username: username.is_some(),
        })
        .await?;
    let competition = response.competition_by_slug.ok_or_else(|| {
        error::user(
            &format!("Competition '{}' not found", slug),
            "Please make sure the competition is correct",
        )
    })?;
    let competition_id = Id::parse_node_id(competition.id).map_err(|err| {
        error::system(
            &format!("Could not parse competition ID: {}", err),
            "This is a bug, please report it",
        )
    })?;
    let use_case_version = competition
        .use_case
        .latest
        .ok_or_else(|| {
            error::system(
                "No use case version found",
                "Please contact the competition organizer",
            )
        })?
        .version
        .parse()
        .map_err(|err| {
            error::system(
                &format!("Invalid use case version found: {err}"),
                "Please contact the competition organizer",
            )
        })?;
    let entity_id = if let Some(username) = username {
        response
            .entity_by_username
            .ok_or_else(|| {
                error::user(
                    &format!("User '{}' not found", username),
                    "Please make sure the username is correct",
                )
            })?
            .id
    } else {
        response.viewer.id
    };
    let entity_id = Id::parse_node_id(entity_id).map_err(|err| {
        error::system(
            &format!("Could not parse entity ID: {}", err),
            "This is a bug, please report it",
        )
    })?;
    Ok(SubmissionUploadInfoResponse {
        competition_id,
        use_case_version,
        entity_id,
    })
}

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

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/update_submission.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct UpdateSubmissionMutation;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/validate_submission.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct ValidateSubmissionMutation;

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

pub async fn upload_use_case(args: Upload, global: GlobalArgs, project: PyProject) -> Result<()> {
    let m = MultiProgress::new();

    let mut use_case_pb = ProgressBar::new_spinner().with_message("Updating version");
    use_case_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    use_case_pb = m.add(use_case_pb);

    let client = GraphQLClient::new(global.url.parse()?).await?;

    let tempdir = tempdir().map_err(|err| {
        error::user(
            &format!("could not create temporary directory: {}", err),
            "Please make sure you have permission to create temporary directories",
        )
    })?;
    let pyproject_toml = std::fs::read_to_string(pyproject_path(&global.project))?;
    let config = project
        .aqora()
        .and_then(|aqora| aqora.as_use_case())
        .ok_or_else(|| {
            error::user(
                "Project is not a use case",
                "Please make sure you are in the correct directory",
            )
        })?;

    let slug = args
        .competition
        .as_ref()
        .or(config.competition.as_ref())
        .ok_or_else(|| {
            error::user(
                "No competition provided",
                "Please specify a competition in either the pyproject.toml or the command line",
            )
        })?;
    let competition_id = get_competition_id_by_slug(&client, slug).await?;

    let version = project.version().ok_or_else(|| {
        error::user(
            "Could not get project version",
            "Please make sure the project is valid",
        )
    })?;
    let package_name = format!("use-case-{}", competition_id.to_package_id());
    let data_path = global.project.join(&config.data);
    if !data_path.exists() {
        return Err(error::user(
            &format!("{} does not exist", data_path.display()),
            "Please make sure the data directory exists",
        ));
    }
    let template_path = config
        .template
        .as_ref()
        .map(|template| global.project.join(template));

    let readme = read_readme(
        &global.project,
        project.project.as_ref().and_then(|p| p.readme.as_ref()),
    )
    .await
    .map_err(|err| {
        error::user(
            &format!("Could not read readme: {}", err),
            "Please make sure the readme is valid",
        )
    })?;
    let project_version = client
        .send::<UpdateUseCaseMutation>(update_use_case_mutation::Variables {
            competition_id: competition_id.to_node_id(),
            pyproject_toml,
            readme,
        })
        .await?
        .create_use_case_version
        .node;

    use_case_pb.finish_with_message("Version updated");

    let s3_client = reqwest::Client::new();

    let data_fut = {
        let upload_url = if let Some(url) = project_version
            .files
            .iter()
            .find(|f| {
                matches!(
                    f.kind,
                    update_use_case_mutation::ProjectVersionFileKind::DATA
                )
            })
            .and_then(|f| f.upload_url.as_ref())
        {
            url
        } else {
            return Err(error::system(
                "No upload URL found",
                "This is a bug, please report it",
            ));
        };
        let data_tar_file = tempdir
            .path()
            .join(format!("{package_name}-{version}.data.tar.gz"));
        let mut data_pb = ProgressBar::new_spinner().with_message("Compressing data");
        data_pb.enable_steady_tick(std::time::Duration::from_millis(100));
        data_pb = m.add(data_pb);

        let data_pb_cloned = data_pb.clone();
        let client = s3_client.clone();
        async move {
            compress(data_path, &data_tar_file).await.map_err(|err| {
                error::system(
                    &format!("Could not compress data: {}", err),
                    "Please make sure the data directory is valid",
                )
            })?;
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

    let template_fut = {
        if let Some(template_path) = template_path {
            let upload_url = if let Some(url) = project_version
                .files
                .iter()
                .find(|f| {
                    matches!(
                        f.kind,
                        update_use_case_mutation::ProjectVersionFileKind::TEMPLATE
                    )
                })
                .and_then(|f| f.upload_url.as_ref())
            {
                url
            } else {
                return Err(error::system(
                    "No upload URL found",
                    "This is a bug, please report it",
                ));
            };
            let template_tar_file = tempdir
                .path()
                .join(format!("{package_name}-{version}.template.tar.gz"));
            let mut template_pb = ProgressBar::new_spinner().with_message("Compressing template");
            template_pb.enable_steady_tick(std::time::Duration::from_millis(100));
            template_pb = m.add(template_pb);

            let template_pb_cloned = template_pb.clone();
            let client = s3_client.clone();
            async move {
                compress(template_path, &template_tar_file)
                    .await
                    .map_err(|err| {
                        error::system(
                            &format!("Could not compress template: {}", err),
                            "Please make sure the template directory is valid",
                        )
                    })?;
                template_pb_cloned.set_message("Uploading template");
                upload_file(&client, template_tar_file, upload_url, "application/gzip").await
            }
            .map(move |res| {
                if res.is_ok() {
                    template_pb.finish_with_message("Template uploaded");
                } else {
                    template_pb.finish_with_message("An error occurred while processing template");
                }
                res
            })
            .boxed()
        } else {
            futures::future::ready(Ok(())).boxed()
        }
    };

    let package_fut = {
        let upload_url = if let Some(url) = project_version
            .files
            .iter()
            .find(|f| {
                matches!(
                    f.kind,
                    update_use_case_mutation::ProjectVersionFileKind::PACKAGE
                )
            })
            .and_then(|f| f.upload_url.as_ref())
        {
            url
        } else {
            return Err(error::system(
                "No upload URL found",
                "This is a bug, please report it",
            ));
        };
        let package_tar_file = tempdir
            .path()
            .join(format!("{package_name}-{version}.tar.gz"));
        let mut package_pb = ProgressBar::new_spinner().with_message("Building package");
        package_pb.enable_steady_tick(std::time::Duration::from_millis(100));
        package_pb = m.add(package_pb);

        let package_pb_cloned = package_pb.clone();
        let client = s3_client.clone();
        async move {
            let env = init_venv(
                &global.project,
                global.uv.as_ref(),
                &package_pb_cloned,
                global.color,
            )
            .await?;

            let project_file = RevertFile::save(pyproject_path(&global.project))?;
            let mut new_project = project.clone();
            new_project.set_name(package_name);
            std::fs::write(&project_file, new_project.toml()?)?;
            build_package(&env, &global.project, tempdir.path(), &package_pb_cloned).await?;
            project_file.revert()?;

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

    futures::future::try_join_all([data_fut, template_fut, package_fut]).await?;

    let mut validate_pb = ProgressBar::new_spinner().with_message("Validating use case");
    validate_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    validate_pb = m.add(validate_pb);

    let _ = client
        .send::<ValidateUseCaseMutation>(validate_use_case_mutation::Variables {
            project_version_id: project_version.id,
        })
        .await?;

    validate_pb.finish_with_message("Done!");

    Ok(())
}

pub async fn upload_submission(args: Upload, global: GlobalArgs, project: PyProject) -> Result<()> {
    let m = MultiProgress::new();

    let mut use_case_pb = ProgressBar::new_spinner().with_message("Updating version");
    use_case_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    use_case_pb = m.add(use_case_pb);

    let client = GraphQLClient::new(global.url.parse()?).await?;

    let tempdir = tempdir().map_err(|err| {
        error::user(
            &format!("could not create temporary directory: {}", err),
            "Please make sure you have permission to create temporary directories",
        )
    })?;
    let pyproject_toml = std::fs::read_to_string(pyproject_path(&global.project))?;
    let config = project
        .aqora()
        .and_then(|aqora| aqora.as_submission())
        .ok_or_else(|| {
            error::user(
                "Project is not a submission",
                "Please make sure you are in the correct directory",
            )
        })?;

    let readme = read_readme(
        &global.project,
        project.project.as_ref().and_then(|p| p.readme.as_ref()),
    )
    .await
    .map_err(|err| {
        error::user(
            &format!("Could not read readme: {}", err),
            "Please make sure the readme is valid",
        )
    })?;

    let version = project.version().ok_or_else(|| {
        error::user(
            "Could not get project version",
            "Please make sure the project is valid",
        )
    })?;

    let evaluation_path = project_last_run_dir(&global.project);
    if !evaluation_path.exists() {
        return Err(error::user(
            "No last run result found",
            "Please make sure you have run `aqora test`",
        ));
    }
    let last_run_result: LastRunResult =
        std::fs::File::open(project_last_run_result(&global.project))
            .map_err(rmp_serde::decode::Error::InvalidDataRead)
            .and_then(rmp_serde::from_read)
            .map_err(|err| {
                error::user(
                    &format!("Could not read last run result: {}", err),
                    "Please make sure your last call to `aqora test` was successful",
                )
            })?;

    if last_run_result.submission_version.as_ref() != Some(&version) {
        return Err(error::user(
            "Submission version does not match last run result",
            "Please re-run `aqora test`",
        ));
    }

    let slug = args
        .competition
        .as_ref()
        .or(config.competition.as_ref())
        .ok_or_else(|| {
            error::user(
                "No competition provided",
                "Please specify a competition in either the pyproject.toml or the command line",
            )
        })?;

    let SubmissionUploadInfoResponse {
        entity_id,
        competition_id,
        use_case_version,
    } = get_submission_upload_info(&client, slug, config.entity.as_ref()).await?;
    let package_name = format!(
        "submission-{}-{}",
        competition_id.to_package_id(),
        entity_id.to_package_id()
    );

    if last_run_result.use_case_version.as_ref() != Some(&use_case_version) {
        return Err(error::user(
            "Use case version does not match last run result",
            "Please install the latest version with `aqora install --upgrade` and re-run `aqora test`",
        ));
    }

    let project_version = client
        .send::<UpdateSubmissionMutation>(update_submission_mutation::Variables {
            competition_id: competition_id.to_node_id(),
            pyproject_toml,
            readme,
        })
        .await?
        .create_submission_version
        .node;

    use_case_pb.finish_with_message("Version updated");

    let s3_client = reqwest::Client::new();

    let evaluation_fut = {
        let upload_url = if let Some(url) = project_version
            .files
            .iter()
            .find(|f| {
                matches!(
                    f.kind,
                    update_submission_mutation::ProjectVersionFileKind::SUBMISSION_EVALUATION
                )
            })
            .and_then(|f| f.upload_url.as_ref())
        {
            url
        } else {
            return Err(error::system(
                "No upload URL found",
                "This is a bug, please report it",
            ));
        };
        let evaluation_tar_file = tempdir
            .path()
            .join(format!("{package_name}-{version}.evaluation.tar.gz"));
        let mut evaluation_pb = ProgressBar::new_spinner().with_message("Compressing evaluation");
        evaluation_pb.enable_steady_tick(std::time::Duration::from_millis(100));
        evaluation_pb = m.add(evaluation_pb);

        let evaluation_pb_cloned = evaluation_pb.clone();
        let client = s3_client.clone();
        async move {
            compress(evaluation_path, &evaluation_tar_file)
                .await
                .map_err(|err| {
                    error::system(
                        &format!("Could not compress evaluation: {}", err),
                        "Please make sure the evaluation directory is valid",
                    )
                })?;
            evaluation_pb_cloned.set_message("Uploading evaluation");
            upload_file(&client, evaluation_tar_file, upload_url, "application/gzip").await
        }
        .map(move |res| {
            if res.is_ok() {
                evaluation_pb.finish_with_message("Evaluation uploaded");
            } else {
                evaluation_pb.finish_with_message("An error occurred while processing evaluation");
            }
            res
        })
        .boxed()
    };

    let package_fut = {
        let upload_url = if let Some(url) = project_version
            .files
            .iter()
            .find(|f| {
                matches!(
                    f.kind,
                    update_submission_mutation::ProjectVersionFileKind::PACKAGE
                )
            })
            .and_then(|f| f.upload_url.as_ref())
        {
            url
        } else {
            return Err(error::system(
                "No upload URL found",
                "This is a bug, please report it",
            ));
        };
        let package_tar_file = tempdir
            .path()
            .join(format!("{package_name}-{version}.tar.gz"));
        let mut package_pb = ProgressBar::new_spinner().with_message("Building package");
        package_pb.enable_steady_tick(std::time::Duration::from_millis(100));
        package_pb = m.add(package_pb);

        let package_pb_cloned = package_pb.clone();
        let client = s3_client.clone();
        async move {
            let env = init_venv(
                &global.project,
                global.uv.as_ref(),
                &package_pb_cloned,
                global.color,
            )
            .await?;

            let project_file = RevertFile::save(pyproject_path(&global.project))?;
            let mut new_project = project.clone();
            new_project.set_name(package_name);
            std::fs::write(&project_file, new_project.toml()?)?;
            build_package(&env, &global.project, tempdir.path(), &package_pb_cloned).await?;
            project_file.revert()?;

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

    futures::future::try_join_all([evaluation_fut, package_fut]).await?;

    let mut validate_pb = ProgressBar::new_spinner().with_message("Validating submission");
    validate_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    validate_pb = m.add(validate_pb);

    let _ = client
        .send::<ValidateSubmissionMutation>(validate_submission_mutation::Variables {
            project_version_id: project_version.id,
        })
        .await?;

    validate_pb.finish_with_message("Done!");

    Ok(())
}

pub async fn upload(args: Upload, global: GlobalArgs) -> Result<()> {
    let project = read_pyproject(&global.project).await?;
    let aqora = project.aqora().ok_or_else(|| {
        error::user(
            "No [tool.aqora] section found in pyproject.toml",
            "Please make sure you are in the correct directory",
        )
    })?;
    if aqora.is_use_case() {
        upload_use_case(args, global, project).await
    } else if aqora.is_submission() {
        upload_submission(args, global, project).await
    } else {
        Err(error::user(
            "Other project types not supported yet",
            "Try one of the supported project types: use_case, submission",
        ))
    }
}
