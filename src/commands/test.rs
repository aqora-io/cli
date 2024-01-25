use crate::{compress::compress, error::Result, python::build_package};
use clap::Args;
use futures::prelude::*;
use indicatif::{MultiProgress, ProgressBar};
use std::{path::PathBuf, time::Duration};

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
    let data = args.project_dir.join("data");
    let data_out = outdir.join("data.tar.gz");

    let m = MultiProgress::new();

    let mut compress_pb = ProgressBar::new_spinner().with_message("Compressing data");
    compress_pb.enable_steady_tick(Duration::from_millis(100));
    compress_pb = m.add(compress_pb);
    let compress_fut = async move {
        let res = compress(&data, "data", &data_out);
        if res.is_ok() {
            compress_pb.finish_with_message("Data compressed");
        } else {
            compress_pb.finish_with_message("An error occurred while compressing the data");
        }
        res
    };

    let mut build_pb = ProgressBar::new_spinner().with_message("Building package");
    build_pb.enable_steady_tick(Duration::from_millis(100));
    build_pb = m.add(build_pb);
    let build_fut = build_package(&args.project_dir, &outdir).map(|res| {
        if res.is_ok() {
            build_pb.finish_with_message("Package built");
        } else {
            build_pb.finish_with_message("An error occurred while building the package");
        }
        res
    });

    futures::future::try_join(build_fut, compress_fut).await?;

    Ok(())
}
