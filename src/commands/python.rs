use crate::{
    commands::GlobalArgs,
    dirs::{init_venv, read_pyproject},
};
use clap::Args;
use indicatif::ProgressBar;
use serde::Serialize;
use std::{ffi::OsString, time::Duration};

#[derive(Args, Debug, Serialize)]
#[command(author, version, about)]
pub struct Python {
    #[arg(last = true)]
    pub slop: Vec<OsString>,
}

pub async fn python(args: Python, global: GlobalArgs) -> crate::error::Result<()> {
    let _ = read_pyproject(&global.project).await?;
    let progress = ProgressBar::new_spinner();
    progress.set_message("Initializing virtual environment");
    progress.enable_steady_tick(Duration::from_millis(100));
    let env = init_venv(&global.project, global.uv.as_ref(), &progress, global.color).await?;
    progress.finish_and_clear();
    let mut cmd = env.python_cmd();
    for arg in args.slop {
        cmd.arg(arg);
    }
    cmd.spawn()?.wait().await?;
    Ok(())
}
