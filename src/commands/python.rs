use crate::dirs::{init_venv, read_pyproject};
use clap::Args;
use indicatif::ProgressBar;
use std::{ffi::OsString, path::PathBuf, time::Duration};

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Python {
    #[arg(short, long, default_value = ".")]
    pub project: PathBuf,
    #[arg(long)]
    pub uv: Option<PathBuf>,
    #[arg(last = true)]
    pub slop: Vec<OsString>,
}

pub async fn python(args: Python) -> crate::error::Result<()> {
    let _ = read_pyproject(&args.project).await?;
    let progress = ProgressBar::new_spinner();
    progress.set_message("Initializing virtual environment");
    progress.enable_steady_tick(Duration::from_millis(100));
    let env = init_venv(&args.project, args.uv.as_ref(), &progress).await?;
    progress.finish_and_clear();
    let mut cmd = env.python_cmd();
    for arg in args.slop {
        cmd.arg(arg);
    }
    cmd.spawn()?.wait().await?;
    Ok(())
}
