use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use clap::Parser;

use crate::commands::Cli;
use crate::dirs::project_bin_dir;
use crate::manifest::{manifest_version, parse_aqora_version};

static TOKIO: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

pub(crate) fn tokio_runtime() -> &'static tokio::runtime::Runtime {
    TOKIO.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    })
}

fn find_venv_aqora(name: impl AsRef<Path>, cli: &Cli) -> Option<PathBuf> {
    if cli.global.ignore_venv_aqora {
        return None;
    }
    let name = name.as_ref().file_name()?;
    let path = project_bin_dir(&cli.global.project).join(name);
    if path.exists() {
        if let Some(venv_version) = Command::new(&path)
            .args(["--ignore-venv-aqora", "--version"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|o| parse_aqora_version(&o))
        {
            if &venv_version > manifest_version() {
                Some(path)
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    }
}

pub fn run<I, T>(args: I) -> u8
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = if cfg!(debug_assertions) {
        let args = args.into_iter().map(|s| s.into()).collect::<Vec<_>>();
        Cli::parse_from(args.clone())
    } else {
        let mut args = args.into_iter().map(|s| s.into()).collect::<Vec<_>>();
        let cli = Cli::parse_from(args.clone());
        let name = args.remove(0);
        if let Some(venv_aqora) = find_venv_aqora(name, &cli) {
            args.push("--ignore-venv-aqora".into());
            let status = std::process::Command::new(venv_aqora)
                .args(args)
                .status()
                .unwrap();
            return status.code().unwrap_or(1).try_into().unwrap_or(1);
        }
        cli
    };

    let tokio = tokio_runtime();
    pyo3_async_runtimes::tokio::init_with_runtime(tokio).unwrap();
    let success = tokio.block_on(async { cli.run().await });
    if success {
        0
    } else {
        1
    }
}
