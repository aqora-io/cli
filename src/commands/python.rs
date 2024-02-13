use crate::dirs::{init_venv, read_pyproject};
use clap::Args;
use std::{ffi::OsString, path::PathBuf};

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Python {
    #[arg(short, long, default_value = ".")]
    pub project_dir: PathBuf,
    #[arg(last = true)]
    pub slop: Vec<OsString>,
}

pub async fn python(args: Python) -> crate::error::Result<()> {
    let _ = read_pyproject(&args.project_dir).await?;
    let env = init_venv(&args.project_dir).await?;
    let mut cmd = env.python_cmd();
    for arg in args.slop {
        cmd.arg(arg);
    }
    cmd.spawn()?.wait().await?;
    Ok(())
}
