use crate::{
    colors::ColorChoiceExt,
    commands::{remove::remove_matching_dependencies, GlobalArgs},
    dirs::{init_venv, pyproject_path, read_pyproject},
    error::{self, Result},
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
    #[arg(long, short)]
    pub upgrade: bool,
    pub deps: Vec<String>,
}

fn insert_formatted(array: &mut toml_edit::Array, item: impl Into<toml_edit::Value>) {
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

fn merge_requirements(old: &Requirement, new: &Requirement) -> Option<Requirement> {
    if old.name != new.name {
        return None;
    }
    let mut merged = old.clone();
    if let Some(new_version) = new.version_or_url.as_ref() {
        if old.version_or_url.is_some() && old.version_or_url.as_ref() != Some(new_version) {
            return None;
        }
        merged.version_or_url = Some(new_version.clone());
    }
    merged.extras.extend(new.extras.iter().cloned());
    merged.extras.dedup();
    if let Some(new_marker) = new.marker.as_ref() {
        if old.marker.is_some() && old.marker.as_ref() != Some(new_marker) {
            return None;
        } else {
            merged.marker = Some(new_marker.clone());
        }
    }
    Some(merged)
}

fn requirement_needs_update(old: &Requirement, new: &Requirement) -> bool {
    if old.name != new.name {
        return true;
    }
    if let Some(new_version) = new.version_or_url.as_ref() {
        if old.version_or_url.as_ref() != Some(new_version) {
            return true;
        }
    }
    for new_extra in new.extras.iter() {
        if !old.extras.contains(new_extra) {
            return true;
        }
    }
    if let Some(new_marker) = new.marker.as_ref() {
        if old.marker.as_ref() != Some(new_marker) {
            return true;
        }
    }
    false
}

pub async fn add(args: Add, global: GlobalArgs) -> Result<()> {
    let mut deps = Vec::new();
    for dep in args.deps.iter() {
        let req = Requirement::parse(dep, &global.project)
            .map_err(|e| error::user(&format!("Invalid requirement '{dep}': {e}"), ""))?;
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
    let dependencies = toml
        .entry("project")
        .or_insert(toml_edit::table())
        .as_table_mut()
        .ok_or_else(|| {
            error::user(
                "Invalid pyproject.toml",
                "The 'project' section must be a table",
            )
        })?
        .entry("dependencies")
        .or_insert(toml_edit::array())
        .as_array_mut()
        .ok_or_else(|| {
            error::user(
                "Invalid pyproject.toml",
                "The 'dependencies' section must be an array",
            )
        })?;
    let mut added_deps = Vec::new();
    for dep in deps.iter() {
        let mut merge_list = remove_matching_dependencies(dependencies, &dep.name)?;
        let to_insert = if merge_list.is_empty() {
            added_deps.push(dep.clone());
            dep.clone()
        } else if merge_list.len() == 1 && !requirement_needs_update(&merge_list[0], dep) {
            merge_list[0].clone()
        } else {
            merge_list.push(dep.clone());
            let merged = merge_list
                .iter()
                .skip(1)
                .try_fold(merge_list[0].clone(), |merged, next| {
                    merge_requirements(&merged, next)
                })
                .ok_or_else(|| {
                    let conflicting = merge_list
                        .iter()
                        .map(|req| format!("'{req}'"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    error::user(
                        &format!("Could not merge dependencies: {conflicting}"),
                        "The dependencies have incompatible versions or markers. Try using `aqora remove` to remove the conflicting dependencies.",
                    )
                })?;
            added_deps.push(merged.clone());
            merged
        };
        insert_formatted(dependencies, to_insert.to_string());
    }
    if added_deps.is_empty() {
        progress.finish_with_message("No dependencies to update");
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
            upgrade: args.upgrade,
            color: global.color.pip(),
            ..Default::default()
        },
        &progress,
    )
    .await?;
    project_file
        .commit()
        .map_err(|err| error::system(&format!("Failed to save pyproject.toml: {err}"), ""))?;
    added_deps.dedup();
    let added_deps = added_deps
        .iter()
        .map(|dep| format!("'{dep}'"))
        .collect::<Vec<_>>()
        .join(", ");
    progress.finish_with_message(format!("Updated dependencies: {added_deps}"));
    Ok(())
}
