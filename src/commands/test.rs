use crate::{error::Result, python::get_python_executable};
use clap::Args;
use std::path::PathBuf;

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Test {
    #[arg(short, long, default_value = "https://app.aqora.io")]
    pub url: String,
    #[arg(short, long, default_value = ".")]
    pub project_dir: PathBuf,
}

pub async fn test(_: Test) -> Result<()> {
    dbg!(get_python_executable()?);
    Ok(())
}
