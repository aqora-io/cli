use crate::{
    commands::GlobalArgs,
    dirs::{init_venv, read_pyproject},
};
use clap::Args;
use indicatif::ProgressBar;
use std::time::Duration;

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Shell;

pub async fn shell(_: Shell, global: GlobalArgs) -> crate::error::Result<()> {
    let _ = read_pyproject(&global.project).await?;
    let progress = ProgressBar::new_spinner();
    progress.set_message("Initializing virtual environment");
    progress.enable_steady_tick(Duration::from_millis(100));
    let env = init_venv(&global.project, global.uv.as_ref(), &progress, global.color).await?;
    progress.finish_and_clear();
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
