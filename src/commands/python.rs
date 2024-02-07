use crate::dirs::{init_venv, read_pyproject};
use clap::Args;
use std::path::PathBuf;

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Python {
    #[arg(short, long, default_value = ".")]
    pub project_dir: PathBuf,
}

pub async fn python(args: Python) -> crate::error::Result<()> {
    let _ = read_pyproject(&args.project_dir).await?;
    let env = init_venv(&args.project_dir).await?;
    env.python_cmd().spawn()?.wait().await?;
    Ok(())
}
