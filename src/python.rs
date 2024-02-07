use crate::{
    error::{self, Result},
    process::run_command,
};
use aqora_runner::python::{PipOptions, PyEnv};
use indicatif::ProgressBar;
use std::path::Path;

pub async fn build_package(
    env: &PyEnv,
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    pb: &ProgressBar,
) -> Result<()> {
    pb.set_message("Checking for build module");
    if !env
        .is_module_installed("build")
        .await
        .map_err(|e| error::system(&format!("Failed to check for build module: {e}"), ""))?
    {
        pip_install(env, ["build"], &PipOptions::default(), pb).await?;
    }
    pb.set_message("Building package");
    let mut cmd = env.build_package(input, output);
    run_command(&mut cmd, pb, Some("Building package"))
        .await
        .map_err(|e| error::system(&format!("Failed to build package: {e}"), ""))
}

pub async fn pip_install(
    env: &PyEnv,
    modules: impl IntoIterator<Item = impl ToString>,
    options: &PipOptions,
    pb: &ProgressBar,
) -> Result<()> {
    let modules = modules
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    pb.set_message(format!("pip install {}", modules.join(" ")));
    let mut cmd = env.pip_install(modules, options);
    run_command(&mut cmd, pb, Some("pip install"))
        .await
        .map_err(|e| {
            error::system(
                &format!("Failed to install build module: {e}"),
                "Please make sure you have permissions to install packages",
            )
        })
}
