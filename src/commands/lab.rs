use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

use aqora_config::{FunctionDef, RefMap};
use aqora_runner::python::PyEnv;
use clap::Args;
use indicatif::ProgressBar;
use pyo3::{types::PyAnyMethods, PyResult};
use serde::Serialize;
use serde_json::json;
use tokio::process::Command;
use url::Url;

use crate::{
    dirs::{project_vscode_dir, read_pyproject, vscode_settings_path},
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

async fn open_vscode(
    path: PathBuf,
    files: &[PathBuf],
    pb: &ProgressBar,
) -> Result<(), std::io::Error> {
    run_command(
        Command::new("code").arg(path).args(files),
        pb,
        Some("Opening VS Code"),
    )
    .await
}

/// Directories Python searches for submodules of `package`, or none if it
/// cannot be imported (e.g. the project is not installed yet).
fn package_search_locations(package: &str) -> Vec<PathBuf> {
    pyo3::Python::attach(|py| -> PyResult<Vec<PathBuf>> {
        let spec = py
            .import(pyo3::intern!(py, "importlib.util"))?
            .getattr(pyo3::intern!(py, "find_spec"))?
            .call1((package,))?;
        if spec.is_none() {
            return Ok(Vec::new());
        }
        let locations = spec.getattr(pyo3::intern!(py, "submodule_search_locations"))?;
        if locations.is_none() {
            return Ok(Vec::new());
        }
        locations
            .try_iter()?
            .map(|path| path.and_then(|p| p.extract::<PathBuf>()))
            .collect()
    })
    .unwrap_or_default()
}

/// The file a `[tool.aqora.refs]` entry points to: the notebook for a
/// notebook ref, otherwise the Python module defining the function.
///
/// The package is looked up in `search_locations` (as returned by
/// `package_search_locations`) and then relative to `project_dir`.
fn find_ref_file(
    def: &FunctionDef,
    project_dir: &Path,
    search_locations: impl Fn(&str) -> Vec<PathBuf>,
) -> Option<PathBuf> {
    let path = def.path.to_string();
    let mut parts = path.split('.').collect::<Vec<_>>();
    if !def.notebook {
        // The last part is the function; the file is its module.
        parts.pop();
    }
    let (stem, package) = parts.split_last()?;
    let candidates = if def.notebook {
        vec![PathBuf::from(format!("{stem}.ipynb"))]
    } else {
        vec![
            PathBuf::from(format!("{stem}.py")),
            Path::new(stem).join("__init__.py"),
        ]
    };
    let mut dirs = if package.is_empty() {
        Vec::new()
    } else {
        search_locations(&package.join("."))
    };
    dirs.push(
        package
            .iter()
            .fold(project_dir.to_path_buf(), |dir, part| dir.join(part)),
    );
    dirs.iter()
        .flat_map(|dir| candidates.iter().map(move |file| dir.join(file)))
        .find(|file| file.is_file())
}

/// Files referenced in `[tool.aqora.refs]`, ordered by ref name. Refs that
/// cannot be found are reported and skipped.
async fn ref_files(project_dir: &Path, pb: &ProgressBar) -> Vec<PathBuf> {
    let refs: RefMap = match read_pyproject(project_dir).await {
        Ok(project) => project
            .aqora()
            .and_then(|aqora| aqora.as_submission())
            .map(|submission| submission.refs.clone())
            .unwrap_or_default(),
        Err(_) => return Vec::new(),
    };
    let mut refs = refs.into_iter().collect::<Vec<_>>();
    refs.sort_by(|(a, _), (b, _)| a.cmp(b));
    let mut files = Vec::new();
    for (name, def) in refs {
        match find_ref_file(&def, project_dir, package_search_locations) {
            Some(file) => {
                if !files.contains(&file) {
                    files.push(file)
                }
            }
            None => pb.println(format!(
                "Warning: could not find the file for ref '{name}' ({})",
                def.path
            )),
        }
    }
    files
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
    open_refs: bool,
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
    let files = if open_refs {
        ref_files(&global_args.project, pb).await
    } else {
        Vec::new()
    };
    open_vscode(global_args.project, &files, pb).await?;
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
    #[arg(
        long,
        help = "Do not open the files referenced in [tool.aqora.refs] of pyproject.toml"
    )]
    pub no_refs: bool,
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
        handle_vscode_integration(global_args, &env, !args.no_refs, &pb).await
    } else {
        let mut jupyter_args = args.jupyter_args;
        if !args.no_refs {
            // Jupyter serves the current directory, so it can only open
            // files below it.
            let cwd = dunce::canonicalize(std::env::current_dir()?)?;
            for file in ref_files(&global_args.project, &pb).await {
                let file = dunce::canonicalize(&file).unwrap_or(file);
                if let Ok(relative) = file.strip_prefix(&cwd) {
                    jupyter_args.push(relative.into());
                }
            }
        }
        python(
            Python {
                module: Some("jupyterlab".into()),
                python_args: jupyter_args,
            },
            global_args,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(path: &str, notebook: bool) -> FunctionDef {
        FunctionDef {
            path: path.parse().unwrap(),
            notebook,
        }
    }

    fn no_locations(_: &str) -> Vec<PathBuf> {
        Vec::new()
    }

    #[test]
    fn finds_a_notebook_ref_in_the_project() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("submission")).unwrap();
        let notebook = dir.path().join("submission").join("solution.ipynb");
        fs::write(&notebook, "{}").unwrap();
        assert_eq!(
            find_ref_file(&def("submission.solution", true), dir.path(), no_locations),
            Some(notebook)
        );
    }

    #[test]
    fn finds_the_module_of_a_function_ref() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("submission")).unwrap();
        let module = dir.path().join("submission").join("solution.py");
        fs::write(&module, "def solution(): pass").unwrap();
        assert_eq!(
            find_ref_file(
                &def("submission.solution.solution", false),
                dir.path(),
                no_locations
            ),
            Some(module)
        );
    }

    #[test]
    fn finds_a_package_function_ref() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("submission")).unwrap();
        let init = dir.path().join("submission").join("__init__.py");
        fs::write(&init, "def solution(): pass").unwrap();
        assert_eq!(
            find_ref_file(&def("submission.solution", false), dir.path(), no_locations),
            Some(init)
        );
    }

    #[test]
    fn prefers_the_installed_package_location() {
        let project = tempfile::tempdir().unwrap();
        let installed = tempfile::tempdir().unwrap();
        let notebook = installed.path().join("solution.ipynb");
        fs::write(&notebook, "{}").unwrap();
        let installed_dir = installed.path().to_path_buf();
        let found = find_ref_file(
            &def("submission.solution", true),
            project.path(),
            move |package| {
                assert_eq!(package, "submission");
                vec![installed_dir.clone()]
            },
        );
        assert_eq!(found, Some(notebook));
    }

    #[test]
    fn missing_refs_are_none() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            find_ref_file(&def("submission.solution", true), dir.path(), no_locations),
            None
        );
        assert_eq!(
            find_ref_file(&def("solution", false), dir.path(), no_locations),
            None
        );
    }
}
