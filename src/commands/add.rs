use crate::{
    commands::{remove::remove_matching_dependencies, GlobalArgs},
    dirs::{pyproject_path, read_pyproject},
    error::{self, Result},
    python::pip_install,
    revert_file::RevertFile,
};
use aqora_config::{
    pep440_rs::{Operator, VersionPattern, VersionSpecifier},
    pep508_rs::VersionOrUrl,
    PackageName, Requirement, Version,
};
use aqora_runner::python::{PipOptions, PipPackage};
use clap::Args;
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Duration};
use tokio::fs;
use toml_edit::DocumentMut;

#[derive(Args, Debug, Serialize)]
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
        let indent = last_item
            .decor()
            .prefix()
            .and_then(|s| s.as_str())
            .and_then(|s| s.rsplit_terminator('\n').next())
            .unwrap_or_default();
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
        merged.version_or_url = Some(new_version.clone());
    }
    merged.extras.extend(new.extras.iter().cloned());
    merged.extras.dedup();
    if old.marker != new.marker {
        return None;
    }
    merged.marker = new.marker.clone();
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
    old.marker == new.marker
}

enum UpdateAction {
    Insert,
    Merge,
    Skip,
}

fn update_dependencies_action(
    dependencies: &toml_edit::Array,
    new: &Requirement,
) -> Result<UpdateAction> {
    let mut matched = false;
    for old in dependencies
        .iter()
        .map(|d| {
            d.as_str()
                .ok_or_else(|| {
                    error::user("Invalid pyproject.toml", "Dependencies must be strings")
                })
                .and_then(|s| {
                    s.parse::<Requirement>().map_err(|err| {
                        error::system(&format!("Could not parse dependencies: {err}"), "")
                    })
                })
        })
        .collect::<Result<Vec<_>>>()?
    {
        if old.name == new.name {
            if matched || requirement_needs_update(&old, new) {
                return Ok(UpdateAction::Merge);
            }
            matched = true
        }
    }
    if matched {
        Ok(UpdateAction::Skip)
    } else {
        Ok(UpdateAction::Insert)
    }
}

#[derive(Debug, Deserialize)]
struct PypiProject {
    releases: HashMap<Version, serde_json::Value>,
}

async fn get_latest_version(
    client: &reqwest::Client,
    package: &PackageName,
) -> reqwest::Result<Option<Version>> {
    Ok(client
        .get(format!("https://pypi.org/pypi/{package}/json"))
        .send()
        .await?
        .json::<PypiProject>()
        .await?
        .releases
        .into_keys()
        .reduce(|latest, next| {
            if next.any_prerelease() {
                latest
            } else if next > latest {
                next
            } else {
                latest
            }
        }))
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
    let env = global.init_venv(&progress).await?;
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
    progress.set_message("Adding dependencies");
    let mut added_deps = Vec::new();
    let pypi_client = reqwest::Client::new();
    for dep in deps.iter() {
        let to_insert = match update_dependencies_action(dependencies, dep)? {
            UpdateAction::Skip => continue,
            UpdateAction::Insert => {
                let mut dep = dep.clone();
                let name = &dep.name;
                if dep.version_or_url.is_none() {
                    match get_latest_version(&pypi_client, name).await {
                        Ok(Some(version)) => {
                            if let Ok(version) = VersionSpecifier::from_pattern(
                                Operator::TildeEqual,
                                VersionPattern::verbatim(version),
                            ) {
                                dep.version_or_url =
                                    Some(VersionOrUrl::VersionSpecifier(version.into()))
                            } else {
                                progress.println(format!(
                                    "Warning: Could not find compatible version for '{name}'"
                                ));
                            }
                        }
                        Ok(None) => {
                            progress.println(format!(
                                "Warning: Could not find latest version for '{name}'"
                            ));
                        }
                        Err(err) => {
                            progress.println(format!(
                                "Warning: Could not get latest version for '{name}': {err}"
                            ));
                        }
                    }
                }
                dep
            }
            UpdateAction::Merge => {
                let mut merge_list = remove_matching_dependencies(dependencies, &dep.name)?;
                merge_list.push(dep.clone());
                merge_list
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
                })?
            }
        };
        insert_formatted(dependencies, to_insert.to_string());
        added_deps.push(to_insert);
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
            ..global.pip_options()
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
