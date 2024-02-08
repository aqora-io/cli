use async_compression::tokio::{bufread::GzipDecoder, write::GzipEncoder};
use std::path::Path;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufReader, BufWriter},
};
use tokio_tar::{Archive as TarArchive, Builder as TarBuilder};

pub async fn compress(input: impl AsRef<Path>, output: impl AsRef<Path>) -> tokio::io::Result<()> {
    let mut builder = TarBuilder::new(GzipEncoder::new(BufWriter::new(
        File::create(output).await?,
    )));
    builder.append_dir_all("", input).await?;
    builder.finish().await?;
    builder.into_inner().await?.shutdown().await
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
