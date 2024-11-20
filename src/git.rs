use git2::{Error, Repository, RepositoryInitOptions};
use indicatif::ProgressBar;
use std::path::Path;

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
