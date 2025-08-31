use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

const PARQUET_MAGIC: &[u8] = b"PAR1";

pub async fn is_parquet<R>(reader: &mut R) -> io::Result<bool>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    let mut magic = [0u8; 4];
    reader.seek(io::SeekFrom::Start(0)).await?;
    reader.read_exact(&mut magic).await?;
    if magic != PARQUET_MAGIC {
        return Ok(false);
    }
    reader.seek(io::SeekFrom::End(-4)).await?;
    reader.read_exact(&mut magic).await?;
    Ok(magic == PARQUET_MAGIC)
}
