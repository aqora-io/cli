use futures::{future::Either, prelude::*};
use indicatif::ProgressBar;
use std::process::Stdio;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};
use tokio_stream::wrappers::LinesStream;

pub async fn run_command(
    cmd: &mut Command,
    pb: &ProgressBar,
    prefix: Option<&str>,
) -> tokio::io::Result<()> {
    let prefix = prefix.map(|p| format!("{}: ", p)).unwrap_or_default();
    let mut child = cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?;
    let output_lines =
        LinesStream::new(BufReader::new(child.stdout.take().unwrap()).lines()).map(Either::Left);
    let err_lines =
        LinesStream::new(BufReader::new(child.stderr.take().unwrap()).lines()).map(Either::Right);
    let mut merged = futures::stream::select(output_lines, err_lines);
    while let Some(line) = merged.next().await {
        match line {
            Either::Left(line) => {
                pb.set_message(format!("{}{}", prefix, line?));
            }
            Either::Right(line) => {
                let line = line?;
                if line.trim().is_empty() {
                    pb.println("");
                } else {
                    pb.println(line);
                }
            }
        }
    }
    let result = child.wait().await?;
    if result.success() {
        Ok(())
    } else {
        Err(tokio::io::Error::other(format!(
            "Command failed with status: {}",
            result.code().unwrap()
        )))
    }
}
