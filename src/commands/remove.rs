use crate::{
    commands::GlobalArgs,
    dirs::{pyproject_path, read_pyproject},
    error::{self, Result},
    python::{pip_install, pip_uninstall},
    revert_file::RevertFile,
};
use aqora_config::{PackageName, Requirement};
use aqora_runner::python::PipPackage;
use clap::Args;
use serde::Serialize;
use tokio::fs;
use toml_edit::DocumentMut;

#[derive(Args, Debug, Serialize)]
#[command(author, version, about)]
pub struct Remove {
    pub deps: Vec<String>,
}

pub fn remove_formatted(dependencies: &mut toml_edit::Array, index: usize) {
    let item = dependencies.remove(index);
    if let Some(prefix) = item.decor().prefix() {
        if let Some(next) = dependencies.get_mut(index) {
            next.decor_mut().set_prefix(prefix.clone());
        }
    }
    if let Some(suffix) = item.decor().suffix() {
        if let Some(previous) = dependencies.get_mut(index - 1) {
            previous.decor_mut().set_suffix(suffix.clone());
        }
    }
}

pub fn remove_matching_dependencies(
    dependencies: &mut toml_edit::Array,
    name: &PackageName,
) -> Result<Vec<Requirement>> {
    let mut removed = Vec::new();
    for (index, req) in dependencies
        .iter()
        .enumerate()
        .map(|(index, d)| {
            let req = d
                .as_str()
                .ok_or_else(|| {
                    error::user("Invalid pyproject.toml", "Dependencies must be strings")
                })
                .and_then(|s| {
                    s.parse::<Requirement>().map_err(|err| {
                        error::system(&format!("Could not parse dependencies: {err}"), "")
                    })
                })?;
            Ok((index, req))
        })
        .collect::<Result<Vec<_>>>()?
    {
        if &req.name == name {
            remove_formatted(dependencies, index - removed.len());
            removed.push(req);
        }
    }
    Ok(removed)
}

pub async fn remove(args: Remove, global: GlobalArgs) -> Result<()> {
    let mut deps = Vec::new();
    for dep in args.deps.iter() {
        let req = PackageName::new(dep.to_string())
            .map_err(|e| error::user(&format!("Invalid package name '{dep}': {e}"), ""))?;
        deps.push(req);
    }
    let _ = read_pyproject(&global.project).await?;
    let progress = global
        .spinner()
        .with_message("Initializing virtual environment");
    let env = global.init_venv(&progress).await?;
    let project_file = RevertFile::save(pyproject_path(&global.project))?;
    let mut toml = fs::read_to_string(&project_file)
        .await?
        .parse::<DocumentMut>()?;
    let dependencies = toml
        .get_mut("project")
        .map(|project| {
            project.as_table_mut().ok_or_else(|| {
                error::user(
                    "Invalid pyproject.toml",
                    "The 'project' section must be a table",
                )
            })
        })
        .transpose()?
        .and_then(|project| project.get_mut("dependencies"))
        .map(|dependencies| {
            dependencies.as_array_mut().ok_or_else(|| {
                error::user(
                    "Invalid pyproject.toml",
                    "The 'dependencies' section must be an array",
                )
            })
        })
        .transpose()?;
    if let Some(dependencies) = dependencies {
        let mut removed_deps = Vec::new();
        for dep in deps.iter() {
            removed_deps.extend(remove_matching_dependencies(dependencies, dep)?);
        }
        if !removed_deps.is_empty() {
            fs::write(&project_file, toml.to_string())
                .await
                .map_err(|e| {
                    error::user(
                        &format!("Failed to write pyproject.toml: {e}"),
                        &format!(
                            "Make sure you have permissions to write to {}",
                            project_file.as_ref().display()
                        ),
                    )
                })?;
        }
    }
    let pip_options = global.pip_options();
    pip_uninstall(&env, deps.clone(), &pip_options, &progress).await?;
    pip_install(
        &env,
        [PipPackage::editable(&global.project)],
        &pip_options,
        &progress,
    )
    .await?;
    project_file
        .commit()
        .map_err(|err| error::system(&format!("Failed to save pyproject.toml: {err}"), ""))?;
    let removed_deps = deps
        .iter()
        .map(|dep| dep.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    progress.finish_with_message(format!("Removed dependencies: {removed_deps}"));
    Ok(())
}
