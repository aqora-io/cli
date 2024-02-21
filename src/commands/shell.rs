use crate::dirs::{init_venv, read_pyproject};
use clap::Args;
use indicatif::ProgressBar;
use std::{path::PathBuf, time::Duration};

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Shell {
    #[arg(short, long, default_value = ".")]
    pub project: PathBuf,
    #[arg(long)]
    pub uv: Option<PathBuf>,
}

pub async fn shell(args: Shell) -> crate::error::Result<()> {
    let _ = read_pyproject(&args.project).await?;
    let progress = ProgressBar::new_spinner();
    progress.set_message("Initializing virtual environment");
    progress.enable_steady_tick(Duration::from_millis(100));
    let env = init_venv(&args.project, args.uv.as_ref(), &progress).await?;
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
