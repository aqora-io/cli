use crate::error::{self, Result};
use pyo3::prelude::*;
use std::path::{Path, PathBuf};

lazy_static::lazy_static! {
    static ref PYTHON_PATH: PathBuf = {
        Python::with_gil(|py| {
            let sys = py.import("sys").unwrap();
            let executable: String = sys
                .getattr(pyo3::intern!(sys.py(), "executable")).unwrap()
                .extract().unwrap();
            PathBuf::from(executable)
        })
    };
}

fn is_module_installed(module: &str) -> bool {
    Python::with_gil(|py| py.import(module).is_ok())
}

async fn ensure_build_installed() -> Result<()> {
    if is_module_installed("build") {
        return Ok(());
    }
    if !is_module_installed("pip") {
        return Err(error::user(
            "pip is not installed",
            "Please install pip and try again",
        ));
    }
    let confirmation = dialoguer::Confirm::new()
        .with_prompt("The Python 'build' module is not installed, install it now?")
        .default(true)
        .interact()?;
    if !confirmation {
        return Err(error::user(
            "The Python 'build' module is not installed",
            "Please install it and try again",
        ));
    }
    let output = tokio::process::Command::new(PYTHON_PATH.as_os_str())
        .arg("-m")
        .arg("pip")
        .arg("install")
        .arg("--upgrade")
        .arg("build")
        .output()
        .await?;
    if output.status.success() {
        Ok(())
    } else {
        Err(error::system(
            &format!(
                "Could not install 'build' module: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
            "",
        ))
    }
}

pub async fn build_package(input: impl AsRef<Path>, output: impl AsRef<Path>) -> Result<()> {
    ensure_build_installed().await?;
    let output = tokio::process::Command::new(PYTHON_PATH.as_os_str())
        .arg("-m")
        .arg("build")
        .arg("--sdist")
        .arg("--outdir")
        .arg(output.as_ref().as_os_str())
        .arg(input.as_ref().as_os_str())
        .output()
        .await?;
    if output.status.success() {
        Ok(())
    } else {
        Err(error::user(
            &format!(
                "Could not build package: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
            "",
        ))
    }
}
