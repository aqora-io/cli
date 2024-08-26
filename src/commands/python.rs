use crate::{commands::GlobalArgs, dirs::read_pyproject};
use clap::Args;
use indicatif::ProgressBar;
use serde::Serialize;
use std::{ffi::OsString, time::Duration};

#[derive(Args, Debug, Serialize)]
#[command(author, version, about)]
pub struct Python {
    #[arg(short = 'm', help = "run library module as a script")]
    pub module: Option<OsString>,
    #[arg(last = true)]
    pub python_args: Vec<OsString>,
}

pub async fn python(args: Python, global: GlobalArgs) -> crate::error::Result<()> {
    let _ = read_pyproject(&global.project).await?;
    let progress = ProgressBar::new_spinner();
    progress.set_message("Initializing virtual environment");
    progress.enable_steady_tick(Duration::from_millis(100));
    let env = global.init_venv(&progress).await?;
    progress.finish_and_clear();
    let mut cmd = env.python_cmd();
    cmd.current_dir(&global.project);
    if let Some(run_mod) = args.module {
        cmd.arg("-m").arg(run_mod);
    }
    for arg in args.python_args {
        cmd.arg(arg);
    }
    cmd.spawn()?.wait().await?;
    Ok(())
}
