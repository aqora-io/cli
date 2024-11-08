use git2::{Error, IndexAddOption, Oid, Repository, RepositoryInitOptions, Signature};
use indicatif::ProgressBar;
use std::{env, path::Path};

pub fn init_repository(
    pb: &ProgressBar,
    dest: impl AsRef<Path>,
    description: Option<String>,
) -> Result<(), Error> {
    pb.set_message("Initializing local Git repository...");
    let mut opts = RepositoryInitOptions::new();
    opts.description(
        description
            .unwrap_or("Aqora competition".to_string())
            .as_str(),
    )
    .no_reinit(true)
    .initial_head("main");
    match Repository::init_opts(dest, &opts) {
        Ok(_) => {
            pb.set_message("Repository initialized successfully.");
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub fn add_and_commit(
    path: impl AsRef<Path>,
    commit_message: impl AsRef<str>,
) -> Result<(), Error> {
    let repository = Repository::open(path)?;
    add_all_files(&repository)?;
    commit_changes(&repository, commit_message)?;
    Ok(())
}

fn add_all_files(repository: &Repository) -> Result<(), Error> {
    let mut index = repository.index()?;
    index.add_all(["*"].iter(), IndexAddOption::DEFAULT, None)?;
    index.write()
}

fn commit_changes(repository: &Repository, message: impl AsRef<str>) -> Result<Oid, Error> {
    let name = env::var("GIT_AUTHOR_NAME").unwrap_or("Default User".to_string());
    let email = env::var("GIT_AUTHOR_EMAIL").unwrap_or("default.email@example.com".to_string());
    let signature = Signature::now(&name, &email)?;

    let mut index = repository.index()?;
    let tree_oid = index.write_tree()?;
    let tree = repository.find_tree(tree_oid)?;

    let parent_commit = match repository.head() {
        Ok(head) => Some(repository.find_commit(head.target().unwrap())?),
        Err(_) => None,
    };

    repository.commit(
        Some("HEAD"),
        &signature,
        &signature,
        message.as_ref(),
        &tree,
        parent_commit
            .as_ref()
            .into_iter()
            .collect::<Vec<_>>()
            .as_slice(),
    )
}
