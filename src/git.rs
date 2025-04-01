use git2::{ErrorCode, Repository, RepositoryInitOptions};
use indicatif::ProgressBar;
use std::path::Path;

use crate::error::{format_permission_error, Result};

pub fn init_repository(
    pb: &ProgressBar,
    dest: impl AsRef<Path>,
    description: Option<String>,
) -> Result<()> {
    pb.set_message("Initializing local Git repository...");
    let mut opts = RepositoryInitOptions::new();
    opts.description(
        description
            .unwrap_or("Aqora competition".to_string())
            .as_str(),
    )
    .no_reinit(true);
    match Repository::init_opts(&dest, &opts) {
        Ok(_) => {
            pb.set_message("Repository initialized successfully.");
            Ok(())
        }
        Err(error) => match error.code() {
            ErrorCode::Exists => {
                pb.set_message("Repository already initialized.");
                Ok(())
            }
            _ => Err(format_permission_error(
                "init a local Git repository",
                dest.as_ref(),
                &error,
            )),
        },
    }
}
