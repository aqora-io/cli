use crate::{
    colors::ColorChoiceExt,
    commands::GlobalArgs,
    dirs::{init_venv, pyproject_path, read_pyproject},
    error::{self, Result},
    python::pip_install,
    revert_file::RevertFile,
};
use aqora_config::{PackageName, Requirement};
use aqora_runner::python::{PipOptions, PipPackage};
use clap::Args;
use indicatif::ProgressBar;
use std::time::Duration;
use tokio::fs;
use toml_edit::DocumentMut;

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Remove {
    pub deps: Vec<String>,
}

pub fn remove_matching_dependencies(
    dependencies: &mut toml_edit::Array,
    name: &PackageName,
) -> Result<Vec<Requirement>> {
    let mut removed = Vec::new();
    loop {
        let matched = dependencies
            .iter()
            .enumerate()
            .find_map(|(index, d)| {
                match d
                    .as_str()
                    .ok_or_else(|| {
                        error::user("Invalid pyproject.toml", "Dependencies must be strings")
                    })
                    .and_then(|s| {
                        s.parse::<Requirement>().map_err(|err| {
                            error::system(&format!("Could not parse dependencies: {err}"), "")
                        })
                    }) {
                    Ok(req) => {
                        if &req.name == name {
                            Some(Ok((index, req)))
                        } else {
                            None
                        }
                    }
                    Err(err) => Some(Err(err)),
                }
            })
            .transpose()?;
        if let Some((index, req)) = matched {
            dependencies.remove(index);
            removed.push(req);
        } else {
            break;
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
    let progress = ProgressBar::new_spinner();
    progress.set_message("Initializing virtual environment");
    progress.enable_steady_tick(Duration::from_millis(100));
    let env = init_venv(&global.project, global.uv.as_ref(), &progress, global.color).await?;
    let project_file = RevertFile::save(pyproject_path(&global.project))?;
    let mut toml = fs::read_to_string(&project_file)
        .await?
        .parse::<DocumentMut>()?;
    let project = if let Some(project) = toml.get_mut("project") {
        project
    } else {
        progress.finish_with_message("No dependencies to remove");
        return Ok(());
    };
    let dependencies = if let Some(dependencies) = project
        .as_table_mut()
        .ok_or_else(|| {
            error::user(
                "Invalid pyproject.toml",
                "The 'project' section must be a table",
            )
        })?
        .get_mut("dependencies")
    {
        dependencies
    } else {
        progress.finish_with_message("No dependencies to remove");
        return Ok(());
    };
    let dependencies = dependencies.as_array_mut().ok_or_else(|| {
        error::user(
            "Invalid pyproject.toml",
            "The 'dependencies' section must be an array",
        )
    })?;
    let mut removed_deps = Vec::new();
    for dep in deps.iter() {
        removed_deps.extend(remove_matching_dependencies(dependencies, dep)?);
    }
    if removed_deps.is_empty() {
        progress.finish_with_message("No dependencies to remove");
        return Ok(());
    }
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
    pip_install(
        &env,
        [PipPackage::editable(&global.project)],
        &PipOptions {
            color: global.color.pip(),
            ..Default::default()
        },
        &progress,
    )
    .await?;
    project_file
        .commit()
        .map_err(|err| error::system(&format!("Failed to save pyproject.toml: {err}"), ""))?;
    let removed_deps = removed_deps
        .iter()
        .map(|dep| format!("'{dep}'"))
        .collect::<Vec<_>>()
        .join(", ");
    progress.finish_with_message(format!("Removed dependencies: {removed_deps}"));
    Ok(())
}
