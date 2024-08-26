use crate::{commands::GlobalArgs, dirs::read_pyproject};
use clap::Args;
use indicatif::ProgressBar;
use serde::Serialize;
use std::{ffi::OsString, time::Duration};

#[derive(Args, Debug, Serialize)]
#[command(author, version, about)]
pub struct Shell {
    #[arg(last = true)]
    pub bash_args: Vec<OsString>,
}

pub async fn shell(args: Shell, global: GlobalArgs) -> crate::error::Result<()> {
    let _ = read_pyproject(&global.project).await?;
    let progress = ProgressBar::new_spinner();
    progress.set_message("Initializing virtual environment");
    progress.enable_steady_tick(Duration::from_millis(100));
    let env = global.init_venv(&progress).await?;
    progress.finish_and_clear();
    let tempfile = tempfile::NamedTempFile::new()?;
    std::fs::write(
        &tempfile,
        format!("source {}", env.activate_path().to_string_lossy()),
    )?;

    tokio::process::Command::new("bash")
        .current_dir(&global.project)
        .arg("--rcfile")
        .arg(tempfile.path())
        .args(args.bash_args)
        .spawn()?
        .wait()
        .await?;

    Ok(())
}
