use async_compression::tokio::{bufread::GzipDecoder, write::GzipEncoder};
use std::path::Path;
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufReader, BufWriter},
};
use tokio_tar::{Archive as TarArchive, Builder as TarBuilder};

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
) -> Result<(), CompressError> {
    let mut builder = TarBuilder::new(GzipEncoder::new(BufWriter::new(
        File::create(output).await?,
    )));
    for entry in ignore::WalkBuilder::new(&input)
        .hidden(false)
        .build()
        .skip(1)
    {
        let entry = entry?;
        let metadata = entry.metadata()?;
        let path = entry.path();
        let name = path.strip_prefix(&input)?;
        if metadata.is_dir() {
            builder.append_dir(name, path).await?;
        } else {
            builder.append_path_with_name(path, name).await?;
        }
    }
    builder.finish().await?;
    Ok(builder.into_inner().await?.shutdown().await?)
}

pub async fn decompress(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
) -> tokio::io::Result<()> {
    tokio::fs::create_dir_all(&output).await?;
    TarArchive::new(GzipDecoder::new(BufReader::new(File::open(input).await?)))
        .unpack(output)
        .await
}
