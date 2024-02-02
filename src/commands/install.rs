use crate::{
    credentials::get_access_token,
    error::{self, Result},
    pyproject::PyProject,
    python::{pypi_url, PipOptions, PyEnv},
};
use clap::Args;
use indicatif::{MultiProgress, ProgressBar};
use std::path::PathBuf;

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Install {
    #[arg(short, long, default_value = "https://app.aqora.io")]
    pub url: String,
    #[arg(short, long, default_value = ".")]
    pub project_dir: PathBuf,
    #[arg(long)]
    pub upgrade: bool,
}

pub async fn install_submission(args: Install, project: PyProject) -> Result<()> {
    let m = MultiProgress::new();

    let mut pb = ProgressBar::new_spinner().with_message("Setting up virtual environment");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb = m.add(pb);

    let env = PyEnv::init(&args.project_dir).await?;

    let config = project.aqora()?.as_submission()?;
    let competition_id = config.competition;
    let use_case_package = format!("use-case-{}", competition_id.to_package_id());
    let extra_index_urls = {
        let url = args.url.clone().parse()?;
        vec![pypi_url(&url, get_access_token(url.clone()).await?)?]
    };

    let mut use_case_pb = ProgressBar::new_spinner().with_message("Installing use case...");
    use_case_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    use_case_pb = m.insert_before(&pb, use_case_pb);

    env.pip_install(
        [use_case_package],
        &PipOptions {
            upgrade: args.upgrade,
            extra_index_urls,
            ..Default::default()
        },
        Some(&use_case_pb),
    )
    .await?;

    use_case_pb.finish_with_message("Use case installed");

    let mut local_pb = ProgressBar::new_spinner().with_message("Installing local project...");
    local_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    local_pb = m.insert_before(&pb, local_pb);

    env.pip_install(
        [args.project_dir.to_string_lossy().to_string()],
        &PipOptions {
            upgrade: args.upgrade,
            ..Default::default()
        },
        Some(&local_pb),
    )
    .await?;

    local_pb.finish_with_message("Local project installed");

    pb.finish_with_message("Virtual environment setup");

    Ok(())
}

pub async fn install(args: Install) -> Result<()> {
    let project = PyProject::for_project(&args.project_dir)?;
    let aqora = project.aqora()?;
    if aqora.is_submission() {
        install_submission(args, project).await
    } else {
        Err(error::user(
            "Use cases not supported",
            "Run install on a submission instead",
        ))
    }
}
