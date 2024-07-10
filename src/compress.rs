use async_compression::tokio::write::GzipEncoder;
use futures::StreamExt;
use indicatif::ProgressBar;
use std::path::Path;
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter},
};
use tokio_tar::{Archive as TarArchive, Builder as TarBuilder};

use crate::progress_bar::{self, TempProgressStyle};

#[derive(Error, Debug)]
pub enum CompressError {
    #[error(transparent)]
    Io(#[from] tokio::io::Error),
    #[error(transparent)]
    Ignore(#[from] ignore::Error),
    #[error(transparent)]
    StripPrefix(#[from] std::path::StripPrefixError),
}

pub async fn compress(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    pb: &ProgressBar,
) -> Result<(), CompressError> {
    let mut builder = TarBuilder::new(GzipEncoder::new(BufWriter::new(
        File::create(output).await?,
    )));
    let entries = ignore::WalkBuilder::new(&input)
        .hidden(false)
        .build()
        .skip(1)
        .collect::<Result<Vec<_>, _>>()?;
    let _guard = TempProgressStyle::new(pb);
    pb.reset();
    pb.set_style(progress_bar::pretty());
    pb.set_position(0);
    pb.set_length(entries.len() as u64);
    for entry in entries {
        let metadata = entry.metadata()?;
        let path = entry.path();
        let name = path.strip_prefix(&input)?;
        if metadata.is_dir() {
            builder.append_dir(name, path).await?;
        } else {
            builder.append_path_with_name(path, name).await?;
        }
        pb.inc(1);
    }
    builder.finish().await?;
    Ok(builder.into_inner().await?.shutdown().await?)
}

fn async_tempfile_error(error: async_tempfile::Error) -> std::io::Error {
    match error {
        async_tempfile::Error::Io(error) => error,
        async_tempfile::Error::InvalidFile => std::io::Error::new(
            std::io::ErrorKind::Other,
            "async_tempfile::Error::InvalidFile",
        ),
        async_tempfile::Error::InvalidDirectory => std::io::Error::new(
            std::io::ErrorKind::Other,
            "async_tempfile::Error::InvalidDirectory",
        ),
    }
}

pub async fn decompress(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    pb: &ProgressBar,
) -> tokio::io::Result<()> {
    let output = output.as_ref().to_owned();
    tokio::fs::create_dir_all(&output).await?;

    let inflated = async_tempfile::TempFile::new_in(output.as_path())
        .await
        .map_err(async_tempfile_error)?;

    // (1) inflate tar.gz into tar
    {
        // find out input size
        let mut input = File::open(input).await?;
        let input_len = input.seek(std::io::SeekFrom::End(0)).await?;
        input.seek(std::io::SeekFrom::Start(0)).await?;
        pb.reset();
        pb.set_style(crate::progress_bar::pretty_bytes());
        pb.set_position(0);
        pb.set_length(input_len);
        pb.set_message("Inflating archive");

        // actually inflate
        let mut inflated = inflated.open_rw().await.map_err(async_tempfile_error)?;
        let mut inflater = async_compression::tokio::write::GzipDecoder::new(&mut inflated);
        let mut buf = Box::new([0u8; 1024 * 1024]);
        loop {
            let read = input.read(&mut buf[..]).await?;
            inflater.write_all(&buf[0..read]).await?;
            pb.inc(read as u64);
            if read == 0 {
                break;
            }
        }
        inflater.shutdown().await?;
    }

    // (2) count number of entries in tar
    let entry_count = {
        let mut inflated = inflated.open_ro().await.map_err(async_tempfile_error)?;
        let mut tar = TarArchive::new(&mut inflated);
        let mut entries = tar.entries()?;
        let mut count = 0;
        while let Some(entry) = entries.next().await {
            entry?;
            count += 1;
        }
        count
    };

    pb.reset();
    pb.set_style(crate::progress_bar::pretty());
    pb.set_position(0);
    pb.set_length(entry_count as u64);
    pb.set_message("Extracting files");

    // (3) extract files from tar
    {
        let mut inflated = inflated.open_ro().await.map_err(async_tempfile_error)?;
        let mut tar = TarArchive::new(&mut inflated);
        let mut entries = tar.entries()?;
        while let Some(entry) = entries.next().await {
            let mut entry =
                entry.map_err(|e| tokio::io::Error::new(tokio::io::ErrorKind::InvalidData, e))?;
            entry.unpack_in(&output).await?;
            pb.inc(1);
        }
    }

    Ok(())
}
