use crate::{error::Result, python::build_package};
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

pub async fn test(args: Test) -> Result<()> {
    // TODO change this to a tempdir
    let outdir = args.project_dir.join("dist");
    build_package(&args.project_dir, &outdir).await?;
    Ok(())
}
