use crate::{
    commands::{login::check_login, GlobalArgs},
    compress::{compress, DEFAULT_ARCH_EXTENSION, DEFAULT_ARCH_MIME_TYPE},
    dirs::{
        project_last_run_dir, project_last_run_result, project_use_case_toml_path, pyproject_path,
        read_pyproject,
    },
    error::{self, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
    id::Id,
    ipynb::convert_project_notebooks,
    python::{build_package, LastRunResult},
    readme::read_readme,
    revert_file::RevertFile,
    upload::upload_project_version_file,
};
use aqora_config::{PyProject, Version};
use clap::Args;
use futures::prelude::*;
use graphql_client::GraphQLQuery;
use indicatif::{MultiProgress, ProgressBar};
use serde::Serialize;
use std::path::Path;
use tempfile::tempdir;
use tracing::Instrument as _;
use url::Url;

use super::test::run_submission_tests;

const DEFAULT_RULES: &str = r#"=========================
Rules, Terms & Conditions
=========================

By default all competitions must comply to the rules defined in [aqora's Terms of Use](https://aqora.io/terms)."#;

#[derive(Args, Debug, Serialize)]
#[command(author, version, about)]
pub struct Upload {
    pub competition: Option<String>,
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/competition_by_slug.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
struct CompetitionBySlug;

struct CompetitionInfo {
    id: Id,
    version: Option<Version>,
}

async fn get_competition_by_slug(
    client: &GraphQLClient,
    slug: impl Into<String>,
) -> Result<CompetitionInfo> {
    let slug = slug.into();
    let competition = client
        .send::<CompetitionBySlug>(competition_by_slug::Variables { slug: slug.clone() })
        .await?
        .competition_by_slug
        .ok_or_else(|| {
            error::user(
                &format!("Competition '{}' not found", slug),
                "Please make sure the competition is correct",
            )
        })?;
    let id = Id::parse_node_id(competition.id).map_err(|err| {
        error::system(
            &format!("Could not parse competition ID: {}", err),
            "This is a bug, please report it",
        )
    })?;
    let version = competition
        .use_case
        .latest
        .map(|latest| {
            latest.version.parse().map_err(|err| {
                error::system(
                    &format!("Invalid use case version found: {err}"),
                    "Please contact the competition organizer",
                )
            })
        })
        .transpose()?;
    Ok(CompetitionInfo { id, version })
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/submission_upload_info.graphql",
    schema_path = "schema.graphql",
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
    query_path = "src/graphql/latest_submission_version.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct LatestSubmissionVersion;

#[derive(Debug)]
pub struct LatestSubmissionVersionResponse {
    is_member: bool,
    previously_agreed: bool,
    latest_agreed: bool,
    rule_text: String,
    version: Option<Version>,
}

pub async fn get_latest_submission_version(
    client: &GraphQLClient,
    slug: String,
    entity_id: Id,
) -> Result<LatestSubmissionVersionResponse> {
    let competition = client
        .send::<LatestSubmissionVersion>(latest_submission_version::Variables {
            slug: slug.clone(),
            entity_id: entity_id.to_node_id(),
        })
        .await?
        .competition_by_slug
        .ok_or_else(|| {
            error::user(
                &format!("Competition '{}' not found", slug),
                "Please make sure the competition is correct",
            )
        })?;

    let version = competition
        .submissions
        .nodes
        .first()
        .and_then(|submission| {
            submission.latest.as_ref().map(|latest| {
                latest.version.parse().map_err(|err| {
                    error::system(
                        &format!("Invalid submission version found: {err}"),
                        "Please contact the competition organizer",
                    )
                })
            })
        })
        .transpose()?;
    let latest_agreed = competition.latest_rule.entity_agreement.is_some();
    let previously_agreed =
        competition.entity_rule_agreements.nodes.len() > if latest_agreed { 1 } else { 0 };
    Ok(LatestSubmissionVersionResponse {
        is_member: competition.membership.is_some(),
        rule_text: competition.latest_rule.text,
        previously_agreed,
        latest_agreed,
        version,
    })
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/join_competition.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct JoinCompetition;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/accept_competition_rules.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct AcceptCompetitionRules;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/update_use_case.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug,PartialEq"
)]
pub struct UpdateUseCaseMutation;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/validate_use_case.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct ValidateUseCaseMutation;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/update_submission.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug,PartialEq"
)]
pub struct UpdateSubmissionMutation;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/validate_submission.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub struct ValidateSubmissionMutation;

pub trait ProjectVersionFile {
    type Kind: PartialEq + std::fmt::Debug;
    fn id(&self) -> &str;
    fn kind(&self) -> &Self::Kind;
    fn upload_url(&self) -> Option<&Url>;
}

impl ProjectVersionFile
    for update_use_case_mutation::UpdateUseCaseMutationCreateUseCaseVersionNodeFiles
{
    type Kind = update_use_case_mutation::ProjectVersionFileKind;
    fn id(&self) -> &str {
        &self.id
    }
    fn kind(&self) -> &Self::Kind {
        &self.kind
    }
    fn upload_url(&self) -> Option<&Url> {
        self.upload_url.as_ref()
    }
}

impl ProjectVersionFile
    for update_submission_mutation::UpdateSubmissionMutationCreateSubmissionVersionNodeFiles
{
    type Kind = update_submission_mutation::ProjectVersionFileKind;
    fn id(&self) -> &str {
        &self.id
    }
    fn kind(&self) -> &Self::Kind {
        &self.kind
    }
    fn upload_url(&self) -> Option<&Url> {
        self.upload_url.as_ref()
    }
}

fn find_project_version_file<F: ProjectVersionFile>(
    files: &[F],
    kind: F::Kind,
) -> Result<(Id, &Url)> {
    let file = files.iter().find(|f| f.kind() == &kind).ok_or_else(|| {
        error::system(
            &format!("{kind:?} file not found in project version"),
            "This is a bug, please report it",
        )
    })?;
    let id = Id::parse_node_id(file.id()).map_err(|err| {
        error::system(
            &format!("Could not parse project version file ID: {}", err),
            "This is a bug, please report it",
        )
    })?;
    let url = file
        .upload_url()
        .ok_or_else(|| error::system("No upload URL found", "This is a bug, please report it"))?;
    Ok((id, url))
}

fn increment_version(version: &Version) -> Version {
    let mut release = version.release().to_vec();
    if let Some(patch) = release.last_mut() {
        *patch += 1
    } else {
        panic!("Invalid project version: no release");
    }
    version.clone().with_release(release)
}

async fn update_pyproject_version(path: impl AsRef<Path>, version: &Version) -> Result<()> {
    let mut document = tokio::fs::read_to_string(path.as_ref())
        .await
        .map_err(|err| {
            error::user(
                &format!("Could not read pyproject.toml: {}", err),
                "Please make sure the file exists",
            )
        })?
        .parse::<toml_edit::DocumentMut>()
        .map_err(|err| {
            error::user(
                &format!("Could not parse pyproject.toml: {}", err),
                "Please make sure the file is valid",
            )
        })?;
    document["project"]["version"] = toml_edit::value(version.to_string());
    tokio::fs::write(path, document.to_string())
        .await
        .map_err(|err| {
            error::user(
                &format!("Could not write pyproject.toml: {}", err),
                "Please make sure you have permission to write to the file",
            )
        })?;
    Ok(())
}

async fn update_project_version(
    project: &mut PyProject,
    project_path: impl AsRef<Path>,
    last_version: Option<&Version>,
    pb: &ProgressBar,
    global_args: &GlobalArgs,
) -> Result<Version> {
    let mut version = project.version().unwrap();

    if let Some(last_version) = last_version {
        if last_version >= &version {
            let new_version = increment_version(last_version);
            let confirmation = pb.suspend(|| {
                global_args
                    .confirm()
                    .with_prompt(format!(
                        r#"Project version must be greater than {last_version}.
Do you want to update the version to {new_version} now?"#
                    ))
                    .default(true)
                    .interact()
            })?;
            if confirmation {
                update_pyproject_version(&pyproject_path(project_path.as_ref()), &new_version)
                    .await?;
                version = new_version;
                *project = read_pyproject(project_path.as_ref()).await?;
            } else {
                return Err(error::user(
                    &format!("Project version must be greater than {last_version}"),
                    "Please update the project version in pyproject.toml",
                ));
            }
        }
    }
    Ok(version)
}

#[tracing::instrument(skip(args, global, project), err)]
pub async fn upload_use_case(
    args: Upload,
    global: GlobalArgs,
    mut project: PyProject,
) -> Result<()> {
    let m = MultiProgress::new();
    check_login(global.clone(), &m).await?;

    project.validate_version().map_err(|err| {
        error::user(
            &format!("Invalid project version: {err}"),
            "Please make sure the project is valid",
        )
    })?;

    let venv_pb = m.add(
        global
            .spinner()
            .with_message("Initializing virtual environment..."),
    );

    let env = global.init_venv(&venv_pb).await?;

    venv_pb.finish_with_message("Virtual environment initialized");

    let tempdir = tempdir().map_err(|err| {
        error::user(
            &format!("could not create temporary directory: {}", err),
            "Please make sure you have permission to create temporary directories",
        )
    })?;
    let config = project
        .aqora()
        .and_then(|aqora| aqora.as_use_case())
        .ok_or_else(|| {
            error::user(
                "Project is not a use case",
                "Please make sure you are in the correct directory",
            )
        })?
        .clone();

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
    if let Err(err) = config.validate() {
        return Err(error::user(
            &format!("Invalid use case: {err}"),
            "Please make sure the use case is valid",
        ));
    }

    let use_case_pb = m.add(global.spinner().with_message("Updating version"));

    let client = global.graphql_client().await?;
    let competition = get_competition_by_slug(&client, slug).await?;

    let version = update_project_version(
        &mut project,
        &global.project,
        competition.version.as_ref(),
        &use_case_pb,
        &global,
    )
    .await?;

    let pyproject_toml = std::fs::read_to_string(pyproject_path(&global.project))?;

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
            competition_id: competition.id.to_node_id(),
            pyproject_toml,
            readme,
            compression: update_use_case_mutation::ProjectVersionCompressor::ZSTANDARD,
        })
        .await?
        .create_use_case_version
        .node;
    let package_name = project_version.project.name.replace('-', "_");

    use_case_pb.finish_with_message("Version updated");

    let futs = stream::FuturesUnordered::new();

    futs.push({
        let (id, upload_url) = find_project_version_file(
            &project_version.files,
            update_use_case_mutation::ProjectVersionFileKind::DATA,
        )?;
        let data_tar_file = tempdir.path().join(format!(
            "{package_name}-{version}.data.{DEFAULT_ARCH_EXTENSION}"
        ));
        let data_pb = m.add(global.spinner().with_message("Compressing data"));

        let data_pb_cloned = data_pb.clone();
        let client = client.clone();
        async move {
            data_pb_cloned.set_message("Compressing data");
            compress(data_path, &data_tar_file, &data_pb_cloned, true)
                .await
                .map_err(|err| {
                    error::system(
                        &format!("Could not compress data: {}", err),
                        "Please make sure the data directory is valid",
                    )
                })?;
            data_pb_cloned.set_message("Uploading data");
            upload_project_version_file(
                &client,
                data_tar_file,
                &id,
                Some(DEFAULT_ARCH_MIME_TYPE),
                upload_url,
                &data_pb_cloned,
            )
            .await
        }
        .inspect(move |res| {
            if res.is_ok() {
                data_pb.finish_with_message("Data uploaded");
            } else {
                data_pb.finish_with_message("An error occurred while processing data");
            }
        })
        .instrument(tracing::debug_span!("data"))
        .boxed_local()
    });

    if let Some(template_path) = template_path {
        futs.push({
            let (id, upload_url) = find_project_version_file(
                &project_version.files,
                update_use_case_mutation::ProjectVersionFileKind::TEMPLATE,
            )?;
            let template_tar_file = tempdir.path().join(format!(
                "{package_name}-{version}.template.{DEFAULT_ARCH_EXTENSION}"
            ));
            let template_pb = m.add(global.spinner().with_message("Compressing template"));

            let template_pb_cloned = template_pb.clone();
            let client = client.clone();
            async move {
                template_pb_cloned.set_message("Compressing template");
                compress(
                    template_path,
                    &template_tar_file,
                    &template_pb_cloned,
                    false,
                )
                .await
                .map_err(|err| {
                    error::system(
                        &format!("Could not compress template: {}", err),
                        "Please make sure the template directory is valid",
                    )
                })?;

                template_pb_cloned.set_message("Uploading template");
                upload_project_version_file(
                    &client,
                    template_tar_file,
                    &id,
                    Some(DEFAULT_ARCH_MIME_TYPE),
                    upload_url,
                    &template_pb_cloned,
                )
                .await
            }
            .inspect(move |res| {
                if res.is_ok() {
                    template_pb.finish_with_message("Template uploaded");
                } else {
                    template_pb.finish_with_message("An error occurred while processing template");
                }
            })
            .instrument(tracing::debug_span!("template"))
            .boxed_local()
        });
    }

    futs.push({
        let (id, upload_url) = find_project_version_file(
            &project_version.files,
            update_use_case_mutation::ProjectVersionFileKind::PACKAGE,
        )?;
        let package_build_path = tempdir.path().join("dist");
        let package_tar_file = package_build_path.join(format!("{package_name}-{version}.tar.gz"));
        let package_pb = m.add(global.spinner().with_message("Building package"));

        let package_pb_cloned = package_pb.clone();
        let client = client.clone();
        let project_path = global.project.clone();
        async move {
            package_pb_cloned.set_message("Building package");
            let project_file = RevertFile::save(pyproject_path(&project_path))?;
            let mut new_project = project.clone();
            new_project.set_name(package_name);
            convert_project_notebooks(&env, new_project.aqora_mut().unwrap()).await?;
            std::fs::write(&project_file, new_project.toml()?)?;
            build_package(&env, &project_path, &package_build_path, &package_pb_cloned).await?;
            project_file.revert()?;

            package_pb_cloned.set_message("Uploading package");
            upload_project_version_file(
                &client,
                package_tar_file,
                &id,
                Some(DEFAULT_ARCH_MIME_TYPE),
                upload_url,
                &package_pb_cloned,
            )
            .await
        }
        .inspect(move |res| {
            if res.is_ok() {
                package_pb.finish_with_message("Package uploaded");
            } else {
                package_pb.finish_with_message("An error occurred while processing package");
            }
        })
        .instrument(tracing::debug_span!("package"))
        .boxed_local()
    });

    futs.try_collect::<()>()
        .instrument(tracing::debug_span!("try_join_all"))
        .await?;

    let validate_pb = m.add(global.spinner().with_message("Validating use case"));

    let _ = client
        .send::<ValidateUseCaseMutation>(validate_use_case_mutation::Variables {
            project_version_id: project_version.id,
        })
        .await?;

    validate_pb.finish_with_message("Done!");

    Ok(())
}

pub async fn upload_submission(
    args: Upload,
    global: GlobalArgs,
    mut project: PyProject,
) -> Result<()> {
    let m = MultiProgress::new();
    check_login(global.clone(), &m).await?;

    let use_case_toml_path = project_use_case_toml_path(&global.project);
    if !use_case_toml_path.exists() {
        return Err(error::user(
            "Project not setup",
            "Run `aqora install` first.",
        ));
    }

    project.validate_version().map_err(|err| {
        error::user(
            &format!("Invalid project version: {err}"),
            "Please make sure the project is valid",
        )
    })?;

    let venv_pb = m.add(
        global
            .spinner()
            .with_message("Initializing virtual environment..."),
    );

    let env = global.init_venv(&venv_pb).await?;

    venv_pb.finish_with_message("Virtual environment initialized");

    let use_case_pb = m.add(global.spinner().with_message("Updating version"));

    let tempdir = tempdir().map_err(|err| {
        error::user(
            &format!("could not create temporary directory: {}", err),
            "Please make sure you have permission to create temporary directories",
        )
    })?;
    let config = project
        .aqora()
        .and_then(|aqora| aqora.as_submission())
        .ok_or_else(|| {
            error::user(
                "Project is not a submission",
                "Please make sure you are in the correct directory",
            )
        })?
        .clone();

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

    let use_case_toml = PyProject::from_toml(
        tokio::fs::read_to_string(project_use_case_toml_path(&global.project)).await?,
    )
    .map_err(|e| {
        error::system(
            &format!("Failed to read use case: {e}"),
            "Try running `aqora install` again",
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

    let client = global.graphql_client().await?;

    let SubmissionUploadInfoResponse {
        entity_id,
        competition_id,
        use_case_version,
    } = get_submission_upload_info(&client, slug, config.entity.as_ref()).await?;

    let LatestSubmissionVersionResponse {
        version: submission_version,
        previously_agreed,
        latest_agreed,
        rule_text,
        is_member,
    } = get_latest_submission_version(&client, slug.clone(), entity_id).await?;

    if !latest_agreed {
        let message = if previously_agreed {
            "The competition rules have been updated since you last agreed to them."
        } else {
            "You must agree to the competition rules before submitting."
        };
        let mut rules = DEFAULT_RULES.to_string();
        if !rule_text.trim().is_empty() {
            rules.push_str(&format!("\n\n{rule_text}"));
        }

        let accepts = m.suspend(|| {
            let will_review = global
                .confirm()
                .with_prompt(format!("{message} Would you like to review them now?"))
                .default(true)
                .interact()
                .ok()
                .unwrap_or_default();
            if !will_review {
                return false;
            }
            if dialoguer::Editor::new().edit(&rules).is_err() {
                return false;
            }
            global
                .confirm()
                .with_prompt("Would you like to accept?")
                .no_prompt_value(true)
                .interact()
                .ok()
                .unwrap_or_default()
        });
        if !accepts {
            let mut url = global.aqora_url().unwrap();
            url.set_path(&format!("competitions/{slug}/rules"));
            return Err(error::user(
                message,
                &format!("Please agree to the competition rules at {url}",),
            ));
        }
        let pb = m.add(global.spinner().with_message("Accepting rules..."));
        if !is_member {
            client
                .send::<JoinCompetition>(join_competition::Variables {
                    competition_id: competition_id.to_node_id(),
                    entity_id: entity_id.to_node_id(),
                })
                .await?;
        }
        client
            .send::<AcceptCompetitionRules>(accept_competition_rules::Variables {
                competition_id: competition_id.to_node_id(),
                entity_id: entity_id.to_node_id(),
            })
            .await?;
        pb.finish_with_message("Rules accepted");
    }

    if use_case_toml.version().as_ref() != Some(&use_case_version) {
        return Err(error::user(
            "Use case is not updated to the latest version",
            "Please install the latest version with `aqora install`",
        ));
    }

    let evaluation_path = project_last_run_dir(&global.project);
    if !evaluation_path.exists() {
        let confirmation = m.suspend(|| {
            global
                .confirm()
                .with_prompt(
                    r#"No last run result found.
Would you like to run the tests now?"#,
                )
                .default(true)
                .interact()
        })?;
        if confirmation {
            run_submission_tests(&m, &global, &project, Default::default()).await?;
        } else {
            return Err(error::user(
                "No last run result found",
                "Please make sure you have run `aqora test`",
            ));
        }
    }

    let last_run_result: Result<LastRunResult> =
        std::fs::File::open(project_last_run_result(&global.project))
            .map_err(rmp_serde::decode::Error::InvalidDataRead)
            .and_then(rmp_serde::from_read)
            .map_err(|err| {
                error::user(
                    &format!("Could not read last run result: {}", err),
                    "Please make sure your last call to `aqora test` was successful",
                )
            });

    if let Ok(last_run_result) = last_run_result.as_ref() {
        if last_run_result.use_case_version.as_ref() != Some(&use_case_version) {
            let confirmation = m.suspend(|| {
                global
                    .confirm()
                    .with_prompt(
                        r#"It seems the use case version has changed since the last test run.
It is required to run the tests again.
Do you want to run the tests now?"#,
                    )
                    .default(true)
                    .interact()
            })?;
            if confirmation {
                run_submission_tests(&m, &global, &project, Default::default()).await?;
            } else {
                return Err(error::user(
                    "Use case version does not match last run result",
                    "Please re-run `aqora test`",
                ));
            }
        } else {
            let time = last_run_result.time;
            let mut should_run_tests = false;
            for entry in ignore::WalkBuilder::new(&global.project)
                .hidden(false)
                .require_git(false)
                .build()
                .skip(1)
                .flatten()
            {
                if let Some(modified) = entry.metadata().ok().and_then(|meta| meta.modified().ok())
                {
                    if chrono::DateTime::<chrono::Utc>::from(modified) > time {
                        should_run_tests = true;
                        break;
                    }
                }
            }
            if should_run_tests {
                let confirmation = m.suspend(|| {
                    global
                        .confirm()
                        .with_prompt(
                            r#"It seems you have made some changes since since the last test run.
Those changes may not be reflected in the submission unless you re-run the tests.
Do you want to re-run the tests now?"#,
                        )
                        .default(true)
                        .interact()
                })?;
                if confirmation {
                    run_submission_tests(&m, &global, &project, Default::default()).await?;
                }
            }
        }
    } else {
        let confirmation = m.suspend(|| {
            global
                .confirm()
                .with_prompt(
                    r#"It seems the last test run result is corrupted or missing.
It is required to run the tests again.
Do you want to run the tests now?"#,
                )
                .default(true)
                .interact()
        })?;
        if confirmation {
            run_submission_tests(&m, &global, &project, Default::default()).await?;
        } else {
            return Err(error::user(
                "Last test run result is corrupted or missing",
                "Please re-run `aqora test`",
            ));
        }
    }

    let version = update_project_version(
        &mut project,
        &global.project,
        submission_version.as_ref(),
        &use_case_pb,
        &global,
    )
    .await?;

    let pyproject_toml = std::fs::read_to_string(pyproject_path(&global.project))?;

    let project_version = client
        .send::<UpdateSubmissionMutation>(update_submission_mutation::Variables {
            competition_id: competition_id.to_node_id(),
            pyproject_toml,
            readme,
            entity_id: entity_id.to_node_id(),
            compression: update_submission_mutation::ProjectVersionCompressor::ZSTANDARD,
        })
        .await?
        .create_submission_version
        .node;

    use_case_pb.finish_with_message("Version updated");

    let package_name = project_version.project.name.replace('-', "_");

    let evaluation_fut = {
        let (id, upload_url) = find_project_version_file(
            &project_version.files,
            update_submission_mutation::ProjectVersionFileKind::SUBMISSION_EVALUATION,
        )?;
        let evaluation_tar_file = tempdir.path().join(format!(
            "{package_name}-{version}.evaluation.{DEFAULT_ARCH_EXTENSION}"
        ));
        let evaluation_pb = m.add(global.spinner().with_message("Compressing evaluation"));

        let evaluation_pb_cloned = evaluation_pb.clone();
        let client = client.clone();
        async move {
            compress(
                evaluation_path,
                &evaluation_tar_file,
                &evaluation_pb_cloned,
                true,
            )
            .await
            .map_err(|err| {
                error::system(
                    &format!("Could not compress evaluation: {}", err),
                    "Please make sure the evaluation directory is valid",
                )
            })?;
            evaluation_pb_cloned.set_message("Uploading evaluation");
            upload_project_version_file(
                &client,
                evaluation_tar_file,
                &id,
                Some(DEFAULT_ARCH_MIME_TYPE),
                upload_url,
                &evaluation_pb_cloned,
            )
            .await
        }
        .instrument(tracing::debug_span!("evaluation"))
        .inspect(move |res| {
            if res.is_ok() {
                evaluation_pb.finish_with_message("Evaluation uploaded");
            } else {
                evaluation_pb.finish_with_message("An error occurred while processing evaluation");
            }
        })
        .boxed_local()
    };

    let package_fut = {
        let (id, upload_url) = find_project_version_file(
            &project_version.files,
            update_submission_mutation::ProjectVersionFileKind::PACKAGE,
        )?;
        let package_build_path = tempdir.path().join("dist");
        let package_tar_file = package_build_path.join(format!(
            "{}-{version}.tar.gz",
            package_name.replace('-', "_")
        ));
        let package_pb = m.add(global.spinner().with_message("Building package"));

        let package_pb_cloned = package_pb.clone();
        let client = client.clone();
        let project_path = global.project.clone();
        async move {
            let project_file = RevertFile::save(pyproject_path(&project_path))?;
            let mut new_project = project.clone();
            new_project.set_name(package_name);
            convert_project_notebooks(&env, new_project.aqora_mut().unwrap()).await?;
            std::fs::write(&project_file, new_project.toml()?)?;
            build_package(&env, &project_path, package_build_path, &package_pb_cloned).await?;
            project_file.revert()?;

            package_pb_cloned.set_message("Uploading package");
            upload_project_version_file(
                &client,
                package_tar_file,
                &id,
                Some(DEFAULT_ARCH_MIME_TYPE),
                upload_url,
                &package_pb_cloned,
            )
            .await
        }
        .instrument(tracing::debug_span!("package"))
        .inspect(move |res| {
            if res.is_ok() {
                package_pb.finish_with_message("Package uploaded");
            } else {
                package_pb.finish_with_message("An error occurred while processing package");
            }
        })
        .boxed_local()
    };

    futures::future::try_join_all([evaluation_fut, package_fut]).await?;

    let validate_pb = m.add(global.spinner().with_message("Validating submission"));

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
