use crate::{
    error::{self, Result},
    process::run_command,
};
use aqora_config::{PackageName, Version};
use aqora_runner::{
    pipeline::EvaluateAllInfo,
    python::{PipOptions, PipPackage, PyEnv},
};
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Serialize, Deserialize, Debug)]
pub struct LastRunResult {
    #[serde(flatten)]
    pub info: EvaluateAllInfo,
    pub time: chrono::DateTime<chrono::Utc>,
    pub use_case_version: Option<Version>,
}

#[tracing::instrument(skip(env, pb))]
pub async fn build_package(
    env: &PyEnv,
    input: impl AsRef<Path> + std::fmt::Debug,
    output: impl AsRef<Path> + std::fmt::Debug,
    pb: &ProgressBar,
) -> Result<()> {
    pb.set_message("Building package");
    let mut cmd = env.build_package(input, output);
    run_command(&mut cmd, pb, Some("Building package"))
        .await
        .map_err(|e| error::system(&format!("Failed to build package: {e}"), ""))
}

pub async fn pip_install(
    env: &PyEnv,
    modules: impl IntoIterator<Item = PipPackage>,
    options: &PipOptions,
    pb: &ProgressBar,
) -> Result<()> {
    let modules = modules.into_iter().collect::<Vec<_>>();
    let debug_modules = modules
        .iter()
        .map(|module| module.name())
        .collect::<Vec<_>>()
        .join(" ");
    pb.set_message(format!("pip install {debug_modules}",));
    let mut cmd = env.pip_install(modules, options);
    run_command(&mut cmd, pb, Some("pip install"))
        .await
        .map_err(|e| {
            error::system(
                &format!("Failed to pip install {debug_modules}: {e}"),
                "Please make sure you have permissions to install packages",
            )
        })
}

pub async fn pip_uninstall(
    env: &PyEnv,
    modules: impl IntoIterator<Item = PackageName>,
    options: &PipOptions,
    pb: &ProgressBar,
) -> Result<()> {
    let modules = modules.into_iter().collect::<Vec<_>>();
    let debug_modules = modules
        .iter()
        .map(|module| module.to_string())
        .collect::<Vec<_>>()
        .join(" ");
    pb.set_message(format!("pip uninstall {debug_modules}",));
    let mut cmd = env.pip_uninstall(modules, options);
    run_command(&mut cmd, pb, Some("pip uninstall"))
        .await
        .map_err(|e| {
            error::system(
                &format!("Failed to pip uninstall {debug_modules}: {e}"),
                "Please make sure you have permissions to uninstall packages",
            )
        })
}
