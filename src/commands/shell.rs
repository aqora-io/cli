use crate::{pyproject::PyProject, python::PyEnv};
use clap::Args;
use std::path::PathBuf;

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Shell {
    #[arg(short, long, default_value = ".")]
    pub project_dir: PathBuf,
}

pub async fn shell(args: Shell) -> crate::error::Result<()> {
    let _ = PyProject::for_project(&args.project_dir)?;
    let env = PyEnv::init(&args.project_dir).await?;
    let tempfile = tempfile::NamedTempFile::new()?;
    std::fs::write(
        &tempfile,
        format!("source {}", env.activate_path().to_string_lossy()),
    )?;
    tokio::process::Command::new("bash")
        .arg("--rcfile")
        .arg(tempfile.path())
        .spawn()?
        .wait()
        .await?;
    Ok(())
}
