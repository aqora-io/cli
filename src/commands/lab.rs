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
use url::Url;

use crate::{
    dirs::{project_vscode_dir, vscode_settings_path},
    error::{self, Result},
    process::run_command,
    vscode::UserVSCodeSettings,
};

use crate::commands::python::{python, Python};

use super::GlobalArgs;

const VSCODE_EXT: [&str; 3] = [
    "ms-python.python",
    "ms-toolsai.jupyter",
    "aqora-quantum.aqora",
];

async fn is_vscode_available(pb: &ProgressBar) -> Result<()> {
    run_command(Command::new("code").arg("--version"), pb, Some("Checking for VS Code"))
        .await
        .map_err(|_| error::user(
            "VS Code not found 😞",
            "You can install it from https://code.visualstudio.com/ or run `aqora lab -j` to open the lab without VS Code.",
        ))
}

async fn install_extensions(pb: &ProgressBar) -> Result<()> {
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

    for extension in VSCODE_EXT {
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
        fs::write(
            vscode_settings_path(project_dir),
            json!({"python.defaultInterpreterPath": env.activate_path().to_string_lossy()})
                .to_string(),
        )?;
        Ok(())
    }
}

async fn handle_vscode_integration(
    global_args: GlobalArgs,
    env: &PyEnv,
    pb: &ProgressBar,
) -> Result<()> {
    is_vscode_available(pb).await?;

    if UserVSCodeSettings::load(global_args.config_home().await?)
        .await?
        .can_install_extensions
        .unwrap_or(false)
    {
        install_extensions(pb).await?;
    }

    create_vscode_settings(&global_args.project, env)?;
    open_vscode(global_args.project, pb).await?;
    Ok(())
}

async fn ask_for_install_vscode_extensions(
    allow_vscode_extensions: Option<bool>,
    pb: &ProgressBar,
    global_args: &GlobalArgs,
) -> Result<()> {
    let mut vscode_settings = UserVSCodeSettings::load(global_args.config_home().await?).await?;

    if let Some(allow) = allow_vscode_extensions {
        vscode_settings.can_install_extensions(allow).save().await?;
        return Ok(());
    }

    fn format_extensions() -> String {
        VSCODE_EXT
            .iter()
            .map(|ext| {
                format!(
                    "🔹 {ext}: {}",
                    Url::parse_with_params(
                        "https://marketplace.visualstudio.com/items",
                        &[("itemName", *ext)]
                    )
                    .unwrap()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    let prompt_message = format!(
        "\n✨ Ready to set up your VS Code environment! ✨\n\n\
        Some essential extensions are recommended:\n\n{}\n\n\
        Install these VS Code extensions? ",
        format_extensions()
    );

    let can_install = tokio::task::spawn_blocking({
        let pb = pb.clone();
        let args = global_args.clone();
        move || {
            pb.suspend(|| {
                args.confirm()
                    .with_prompt(prompt_message)
                    .default(true)
                    .interact()
                    .map_err(|_| error::system("Failed to read input", "Please try again"))
            })
        }
    })
    .await
    .map_err(|_| error::user("The extension installation prompt was interrupted.", ""))??;

    vscode_settings
        .can_install_extensions(can_install)
        .save()
        .await?;
    Ok(())
}

#[derive(Args, Debug, Serialize)]
pub struct Lab {
    #[arg(help = "Additional arguments passed to Jupyter (e.g., --port, --no-browser)")]
    pub jupyter_args: Vec<OsString>,
    #[arg(
        short = 'j',
        long,
        help = "Launch Jupyter in notebook mode instead of Lab mode"
    )]
    pub jupyter_notebook: bool,
    #[arg(
        long,
        help = "Allow or prevent the installation of VS Code extensions in the environment"
    )]
    pub allow_vscode_extensions: Option<bool>,
}

pub async fn lab(args: Lab, global_args: GlobalArgs) -> Result<()> {
    let pb = global_args
        .spinner()
        .with_message("Setting up virtual environment");

    let env = global_args.init_venv(&pb).await?;

    if !args.jupyter_notebook {
        if UserVSCodeSettings::load(global_args.config_home().await?)
            .await?
            .can_install_extensions
            .is_none()
        {
            ask_for_install_vscode_extensions(args.allow_vscode_extensions, &pb, &global_args)
                .await?;
        }
        handle_vscode_integration(global_args, &env, &pb).await
    } else {
        python(
            Python {
                module: Some("jupyterlab".into()),
                python_args: args.jupyter_args,
            },
            global_args,
        )
        .await
    }
}
