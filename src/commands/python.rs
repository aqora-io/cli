use crate::{pyproject::PyProject, python::PyEnv};
use clap::Args;
use std::path::PathBuf;

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Python {
    #[arg(short, long, default_value = ".")]
    pub project_dir: PathBuf,
}

pub async fn python(args: Python) -> crate::error::Result<()> {
    let _ = PyProject::for_project(&args.project_dir)?;
    let env = PyEnv::init(&args.project_dir).await?;
    env.python_cmd().spawn()?.wait().await?;
    Ok(())
}
