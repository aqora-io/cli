use crate::{
    error::{self, Result},
    process::run_command,
};
use aqora_config::Version;
use aqora_runner::{
    pipeline::EvaluateAllInfo,
    python::{PipOptions, PipPackage, PyEnv},
};
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LastRunResult {
    #[serde(flatten)]
    pub info: EvaluateAllInfo,
    pub use_case_version: Option<Version>,
    pub submission_version: Option<Version>,
}

pub async fn build_package(
    env: &PyEnv,
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
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
    pb.set_message(format!(
        "pip install {}",
        modules
            .iter()
            .map(|module| module.to_string())
            .collect::<Vec<_>>()
            .join(" ")
    ));
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
