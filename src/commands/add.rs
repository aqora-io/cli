use crate::{
    colors::ColorChoiceExt,
    commands::GlobalArgs,
    dirs::{init_venv, pyproject_path, read_pyproject},
    python::pip_install,
    revert_file::RevertFile,
};
use aqora_config::Requirement;
use aqora_runner::python::{PipOptions, PipPackage};
use clap::Args;
use indicatif::ProgressBar;
use std::time::Duration;
use tokio::fs;
use toml_edit::DocumentMut;

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Add {
    pub deps: Vec<String>,
}

pub fn insert_formatted(array: &mut toml_edit::Array, item: impl Into<toml_edit::Value>) {
    let mut new_item = item.into();
    let trailing = array.trailing().as_str().unwrap_or_default().to_string();
    if let Some(last_item) = array.iter_mut().last() {
        let indent = if let Some(end) = last_item
            .decor()
            .prefix()
            .and_then(|s| s.as_str())
            .and_then(|s| s.rsplit_terminator('\n').next())
        {
            end
        } else {
            ""
        };
        let end = if let Some(suffix) = last_item.decor().suffix().and_then(|s| s.as_str()) {
            format!("{suffix}{trailing}")
        } else {
            trailing
        };
        let (prefix, trailing) = if let Some(newline_index) = end.rfind('\n') {
            let (prefix, suffix) = end.split_at(newline_index + 1);
            (format!("{prefix}{indent}"), format!("\n{suffix}"))
        } else {
            (indent.to_string(), end)
        };
        last_item.decor_mut().set_suffix("");
        new_item
            .decor_mut()
            .set_prefix(if prefix.is_empty() { " " } else { &prefix });
        array.push_formatted(new_item);
        array.set_trailing(trailing);
    } else {
        array.push(new_item);
    }
}

pub async fn add(args: Add, global: GlobalArgs) -> crate::error::Result<()> {
    let mut deps = Vec::new();
    for dep in args.deps.iter() {
        let req = Requirement::parse(dep, &global.project)
            .map_err(|e| crate::error::user(&format!("Invalid requirement '{dep}': {e}"), ""))?;
        deps.push(req);
    }
    let pyproject = read_pyproject(&global.project).await?;
    if let Some(old_deps) = pyproject
        .project
        .as_ref()
        .and_then(|p| p.dependencies.as_ref())
    {
        for old_dep in old_deps.iter() {
            if deps.iter().any(|dep| dep.name == old_dep.name) {
                return Err(crate::error::user(
                    "Dependency already exists",
                    &format!("The dependency '{old_dep}' is already in the project's dependencies. Use `aqora remove` to remove it first."),
                ));
            }
        }
    }
    let progress = ProgressBar::new_spinner();
    progress.set_message("Initializing virtual environment");
    progress.enable_steady_tick(Duration::from_millis(100));
    let env = init_venv(&global.project, global.uv.as_ref(), &progress, global.color).await?;
    let project_file = RevertFile::save(pyproject_path(&global.project))?;
    let mut toml = fs::read_to_string(&project_file)
        .await?
        .parse::<DocumentMut>()?;
    let dependencies = toml
        .entry("project")
        .or_insert(toml_edit::table())
        .as_table_mut()
        .ok_or_else(|| {
            crate::error::user(
                "Invalid pyproject.toml",
                "The 'project' section must be a table",
            )
        })?
        .entry("dependencies")
        .or_insert(toml_edit::array())
        .as_array_mut()
        .ok_or_else(|| {
            crate::error::user(
                "Invalid pyproject.toml",
                "The 'dependencies' section must be an array",
            )
        })?;
    for dep in deps.iter() {
        insert_formatted(dependencies, dep.to_string());
    }
    fs::write(&project_file, toml.to_string())
        .await
        .map_err(|e| {
            crate::error::user(
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
    project_file.commit().map_err(|err| {
        crate::error::system(&format!("Failed to save pyproject.toml: {err}"), "")
    })?;
    let added_deps = deps
        .iter()
        .map(|dep| format!("'{dep}'"))
        .collect::<Vec<_>>()
        .join(", ");
    progress.finish_with_message(format!("Added dependencies: {added_deps}"));
    Ok(())
}
