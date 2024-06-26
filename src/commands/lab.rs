use std::{ffi::OsString, path::PathBuf, process::Command};

use clap::Args;
use serde::Serialize;

use crate::{
    dirs::read_pyproject,
    error::{self, Result},
};

use super::GlobalArgs;

use crate::commands::python::{python, Python};

const VS_CODE_NOT_FOUND_MSG: &str = "VS Code not found. You can install it from https://code.visualstudio.com/ or run `aqora lab -j` to open the lab without VS Code.";

fn is_vscode_available() -> Result<(), String> {
    match Command::new("code").arg("--version").output() {
        Ok(output) => {
            if output.status.success() {
                Ok(())
            } else {
                Err(VS_CODE_NOT_FOUND_MSG.to_string())
            }
        }
        Err(_) => Err(VS_CODE_NOT_FOUND_MSG.to_string()),
    }
}

fn install_extensions() -> Result<()> {
    let extensions = vec!["ms-python.python", "ms-toolsai.jupyter"];

    for extension in extensions {
        Command::new("code")
            .args(["--install-extension", extension, "--force"])
            .spawn()?
            .wait()?;
    }

    Ok(())
}

fn open_vscode(path: PathBuf, module: String, name: String) -> Result<(), std::io::Error> {
    let notebook_path = format!("{}/{}/{}.ipynb", path.display(), module, name);
    run_vscode_with_args(&[
        path.display().to_string(),
        "--goto".to_string(),
        notebook_path,
    ])
}

fn open_vscode_pyproject(path: PathBuf) -> Result<(), std::io::Error> {
    let toml_path = path.join("pyproject.toml");
    run_vscode_with_args(&[toml_path.display().to_string()])
}

fn run_vscode_with_args(args: &[String]) -> Result<(), std::io::Error> {
    Command::new("code").args(args).spawn()?.wait()?;
    Ok(())
}

async fn handle_vscode_integration(global_args: GlobalArgs) -> Result<()> {
    is_vscode_available().map_err(|err_msg| error::user("vscode not found 😞", &err_msg))?;

    install_extensions()?;

    let project = read_pyproject(&global_args.project).await?;
    let aqora = project.aqora().ok_or_else(|| {
        error::user(
            "No [tool.aqora] section found in pyproject.toml",
            "Please make sure you are in the correct directory",
        )
    })?;

    if let Some(submission) = aqora.as_submission() {
        if let Some((_key, function_def)) = submission.refs.iter().next() {
            let path = function_def.path.clone();
            open_vscode(
                global_args.project,
                path.module().to_string(),
                path.name().to_string(),
            )?;
        } else {
            open_vscode_pyproject(global_args.project)?;
        }
    }

    Ok(())
}

#[derive(Args, Debug, Serialize)]
pub struct Lab {
    pub jupyter_args: Vec<OsString>,
    #[arg(short = 'j', long)]
    pub jupyter_notebook: bool,
}

pub async fn lab(args: Lab, global_args: GlobalArgs) -> Result<()> {
    if !args.jupyter_notebook {
        handle_vscode_integration(global_args).await
    } else {
        let args = Python {
            module: Some("jupyterlab".into()),
            python_args: args.jupyter_args,
        };
        python(args, global_args).await
    }
}
