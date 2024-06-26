use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::Command,
};

use clap::Args;
use serde::Serialize;

use crate::error::Result;

use super::GlobalArgs;

use crate::commands::python::{python, Python};

fn activate_virtual_env(virtual_env_path: &Path) -> Result<(), std::io::Error> {
    let activate_script = if cfg!(windows) {
        virtual_env_path.join("Scripts").join("activate.bat")
    } else {
        virtual_env_path.join("bin").join("activate")
    };

    let cmd = if cfg!(windows) {
        format!("\"{}\"", activate_script.to_str().unwrap())
    } else {
        format!("source {}", activate_script.to_str().unwrap())
    };

    let output = Command::new("sh").arg("-c").arg(&cmd).output()?;

    println!("{}", String::from_utf8_lossy(&output.stdout));

    Ok(())
}

fn install_extensions() -> Result<(), std::io::Error> {
    let python_extension = "ms-python.python";
    Command::new("code")
        .args(["--install-extension", python_extension, "--force"])
        .spawn()?
        .wait()?;

    let jupyter_extension = "ms-toolsai.jupyter";
    Command::new("code")
        .args(["--install-extension", jupyter_extension, "--force"])
        .spawn()?
        .wait()?;

    Ok(())
}

fn open_vscode(path: PathBuf) -> Result<(), std::io::Error> {
    let path_with_submission = format!("{}", path.display());
    Command::new("code")
        .args(&[
            path_with_submission.clone(),
            "--goto".to_string(),
            format!("{}/submission/notebook.ipynb:1:1", path_with_submission),
        ])
        .spawn()?
        .wait()?;
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
        install_extensions()?;
        activate_virtual_env(&global_args.project.join(".venv"))?;
        open_vscode(global_args.project)?;
        Ok(())
    } else {
        let args = Python {
            module: Some("jupyterlab".into()),
            python_args: args.jupyter_args,
        };
        python(args, global_args).await
    }
}
