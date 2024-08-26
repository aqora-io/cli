use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

use aqora_runner::python::PyEnv;
use clap::Args;
use indicatif::ProgressBar;
use serde::Serialize;
use serde_json::json;
use tokio::process::Command;

use crate::{
    dirs::{project_vscode_dir, vscode_settings_path},
    error::{self, Result},
    process::run_command,
};

use super::GlobalArgs;

use crate::commands::python::{python, Python};

async fn is_vscode_available(pb: &ProgressBar) -> Result<()> {
    run_command(
        Command::new("code").arg("--version"),
        pb,
        Some("Checking for VS Code"),
    )
    .await
    .map_err(|_| {
        error::user(
            "VS Code not found 😞",
            "You can install it from https://code.visualstudio.com/ or \
            run `aqora lab -j` to open the lab without VS Code.",
        )
    })
}

async fn install_extensions(pb: &ProgressBar) -> Result<()> {
    let extensions = vec!["ms-python.python", "ms-toolsai.jupyter"];

    pb.set_message("Checking installed VS Code extensions");
    let installed_extensions =
        if let Ok(output) = Command::new("code").arg("--list-extensions").output().await {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .map(|s| s.to_owned())
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

    for extension in extensions {
        if installed_extensions.contains(&extension.to_string()) {
            continue;
        }
        if run_command(
            Command::new("code").args(["--install-extension", extension, "--force"]),
            pb,
            Some(&format!("Installing VS Code extension {extension}")),
        )
        .await
        .is_err()
        {
            pb.println(format!(
                "Warning: could not install extension {extension}: please install manually"
            ));
        }
    }

    pb.set_message("VS Code extensions installed");

    Ok(())
}

async fn open_vscode(path: PathBuf, pb: &ProgressBar) -> Result<(), std::io::Error> {
    run_command(Command::new("code").arg(path), pb, Some("Opening VS Code")).await
}

fn create_vscode_settings(project_dir: &Path, env: &PyEnv) -> Result<()> {
    let vscode_dir = project_vscode_dir(project_dir);

    if vscode_dir.exists() {
        Ok(())
    } else {
        fs::create_dir_all(&vscode_dir)?;
        let interpreter_path = env.activate_path().to_string_lossy().to_string();
        let settings = json!({
            "python.defaultInterpreterPath": interpreter_path
        });

        fs::write(vscode_settings_path(project_dir), settings.to_string())?;
        Ok(())
    }
}

async fn handle_vscode_integration(
    global_args: GlobalArgs,
    env: &PyEnv,
    pb: &ProgressBar,
) -> Result<()> {
    is_vscode_available(pb).await?;

    install_extensions(pb).await?;
    create_vscode_settings(&global_args.project, env)?;

    open_vscode(global_args.project, pb).await?;

    Ok(())
}

#[derive(Args, Debug, Serialize)]
pub struct Lab {
    pub jupyter_args: Vec<OsString>,
    #[arg(short = 'j', long)]
    pub jupyter_notebook: bool,
}

pub async fn lab(args: Lab, global_args: GlobalArgs) -> Result<()> {
    let pb = ProgressBar::new_spinner().with_message("Setting up virtual environment");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let env = global_args.init_venv(&pb).await?;

    if !args.jupyter_notebook {
        handle_vscode_integration(global_args, &env, &pb).await
    } else {
        let args = Python {
            module: Some("jupyterlab".into()),
            python_args: args.jupyter_args,
        };
        python(args, global_args).await
    }
}
