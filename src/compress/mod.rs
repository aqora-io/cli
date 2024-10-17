use ignore::DirEntry;
use indicatif::{ProgressBar, ProgressIterator as _};
use std::{
    fs::File,
    io::{Read, Seek as _, SeekFrom, Write},
    path::Path,
};
use thiserror::Error;

use crate::progress_bar::TempProgressStyle;

pub const DEFAULT_ARCH_EXTENSION: &str = "tar.zst";
pub const DEFAULT_ARCH_MIME_TYPE: &str = "application/zstd";

#[cfg(test)]
struct StopWatch(std::time::Instant);

#[cfg(test)]
impl StopWatch {
    fn new() -> Self {
        tracing::info!("started");
        Self(std::time::Instant::now())
    }
}

#[cfg(test)]
impl Drop for StopWatch {
    fn drop(&mut self) {
        let elapsed = self.0.elapsed().as_secs_f32();
        tracing::Span::current().record("elapsed", format!("{elapsed:?}s"));
    }
}

struct IndicatifReader<R: Read> {
    reader: R,
    progress_bar: ProgressBar,
}

impl<R: Read> IndicatifReader<R> {
    fn new(reader: R, progress_bar: ProgressBar, length: u64) -> Self {
        progress_bar.set_position(0);
        progress_bar.set_length(length);
        progress_bar.set_style(crate::progress_bar::pretty_bytes());
        Self {
            reader,
            progress_bar,
        }
    }
}

impl IndicatifReader<File> {
    fn for_file(mut file: File, progress_bar: ProgressBar) -> std::io::Result<Self> {
        let length = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;
        Ok(Self::new(file, progress_bar, length))
    }
}

impl<R: Read> Read for IndicatifReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reader
            .read(buf)
            .inspect(|read| self.progress_bar.inc(*read as u64))
    }
}

#[derive(Error, Debug)]
pub enum CompressError {
    #[error(transparent)]
    Io(#[from] tokio::io::Error),
    #[error(transparent)]
    Ignore(#[from] ignore::Error),
    #[error(transparent)]
    StripPrefix(#[from] std::path::StripPrefixError),
    #[error(transparent)]
    Tokio(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Tempfile(#[from] async_tempfile::Error),
    #[error("unsupported compression {0:?}")]
    UnsupportedCompression(String),
}

#[inline]
fn get_extension(path: &Path) -> Option<&str> {
    path.extension()?.to_str()
}

fn expect_extension(path: impl AsRef<Path>, expected: &'_ str) -> Result<(), CompressError> {
    let path = path.as_ref();
    let extension = get_extension(path)
        .ok_or_else(|| CompressError::UnsupportedCompression(format!("{:?}", path.extension())))?;
    if extension != expected {
        return Err(CompressError::UnsupportedCompression(format!(
            "{extension:?}"
        )));
    }
    Ok(())
}

#[tracing::instrument(skip(pb), fields(elapsed), ret, err)]
pub fn sync_compress(
    input: impl AsRef<Path> + std::fmt::Debug,
    output: impl AsRef<Path> + std::fmt::Debug,
    pb: &ProgressBar,
) -> Result<(), CompressError> {
    let _pb = TempProgressStyle::new(pb);
    #[cfg(test)]
    let _sw = StopWatch::new();

    let input = input.as_ref();
    let output = output.as_ref();

    expect_extension(output, "zst")?;
    expect_extension(output.with_extension(""), "tar")?;

    let input_paths = ignore::WalkBuilder::new(input)
        .hidden(false)
        .build()
        .skip(1)
        .map(|result| result.map(DirEntry::into_path))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|path| path.metadata().is_ok_and(|meta| meta.is_file()))
        .collect::<Vec<_>>();

    let n_workers = std::thread::available_parallelism()?.get() as u32;
    tracing::debug!("n_workers = {}", n_workers);

    let mut zst =
        zstd::stream::write::Encoder::new(File::create(output)?, zstd::DEFAULT_COMPRESSION_LEVEL)?;
    zst.multithread(n_workers)?;
    let mut tar = tar::Builder::new(zst);

    pb.reset();
    pb.set_style(crate::progress_bar::pretty());
    pb.set_length(input_paths.len() as u64);
    for input_path in input_paths.into_iter().progress_with(pb.clone()) {
        let arch_path = input_path
            .strip_prefix(input)
            .expect("not a prefix")
            .to_path_buf();
        tar.append_file(arch_path, &mut File::open(input_path)?)?;
    }

    tar.into_inner()?.finish()?.flush()?;

    Ok(())
}

#[tracing::instrument(skip(pb), fields(elapsed), ret, err)]
pub fn sync_decompress(
    input: impl AsRef<Path> + std::fmt::Debug,
    output: impl AsRef<Path> + std::fmt::Debug,
    pb: &ProgressBar,
) -> Result<(), CompressError> {
    let _pb = TempProgressStyle::new(pb);
    #[cfg(test)]
    let _sw = StopWatch::new();

    let input = input.as_ref();
    let output = output.as_ref();

    let input_extension = get_extension(input)
        .ok_or_else(|| CompressError::UnsupportedCompression(format!("{:?}", input)))?;
    expect_extension(input.with_extension(""), "tar")?;

    let input_file: Box<dyn std::io::Read> = match input_extension {
        "gz" => Box::new(flate2::read::MultiGzDecoder::new(
            IndicatifReader::for_file(File::open(input)?, pb.clone())?,
        )),
        "zst" => Box::new(zstd::stream::read::Decoder::new(
            IndicatifReader::for_file(File::open(input)?, pb.clone())?,
        )?),
        extension => return Err(CompressError::UnsupportedCompression(extension.to_string())),
    };
    let mut tar = tar::Archive::new(input_file);

    for tar_entry in tar.entries()? {
        let mut tar_entry = tar_entry?;
        if !tar_entry.unpack_in(output)? {
            tracing::warn!("{:?} was not unpacked", tar_entry.path());
        }
        pb.inc(1);
    }

    Ok(())
}

pub async fn compress(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    pb: &ProgressBar,
) -> Result<(), CompressError> {
    let input = input.as_ref().to_path_buf();
    let output = output.as_ref().to_path_buf();
    let pb = pb.clone();
    tokio::task::spawn_blocking(move || sync_compress(input, output, &pb)).await?
}

pub async fn decompress(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    pb: &ProgressBar,
) -> Result<(), CompressError> {
    let input = input.as_ref().to_path_buf();
    let output = output.as_ref().to_path_buf();
    let pb = pb.clone();
    tokio::task::spawn_blocking(move || sync_decompress(input, output, &pb)).await?
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        fs::{create_dir_all, File},
        io::Write,
        path::{Path, PathBuf},
    };

    use base64::prelude::*;
    use indicatif::{ProgressBar, ProgressDrawTarget};
    use rand::{rngs::ThreadRng, thread_rng, RngCore};
    use rayon::iter::{IntoParallelIterator, ParallelIterator as _};
    use tempfile::{NamedTempFile, TempDir, TempPath};
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt as _, Layer};

    struct Crc32(crc32fast::Hasher);

    impl Write for Crc32 {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.update(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Crc32 {
        fn new() -> Self {
            Self(crc32fast::Hasher::new())
        }
        fn finalize(self) -> u32 {
            self.0.finalize()
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_two_concurrent_compress() {
        let in_path = std::env::current_dir()
            .expect("cannot get cwd")
            .join("src")
            .join("compress")
            .join("test_file.txt");
        let out_a = NamedTempFile::new()
            .expect("cannot create temp file")
            .into_temp_path();
        let out_b = NamedTempFile::new()
            .expect("cannot create temp file")
            .into_temp_path();
        let pb = ProgressBar::hidden();

        let (a, b) = tokio::join!(
            super::compress(&in_path, out_a, &pb),
            super::compress(&in_path, out_b, &pb),
        );
        a.expect("cannot compress");
        b.expect("cannot compress");
    }

    #[test]
    fn test_identity() {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_filter(
                tracing_subscriber::filter::LevelFilter::from_level(tracing::Level::DEBUG),
            ))
            .init();

        let pb = ProgressBar::with_draw_target(Some(80), ProgressDrawTarget::stdout_with_hz(1));

        let src_dir = generate_data_dir();
        let src_entries = scan_data_dir(&src_dir).unwrap();

        let mut arch_name = vec![0u8; 8];
        thread_rng().fill_bytes(&mut arch_name[..]);
        let arch_name = BASE64_STANDARD.encode(arch_name);

        let arch_path =
            TempPath::from_path(std::env::temp_dir().join(format!("{arch_name}.tar.zst")));
        super::sync_compress(&src_dir, &arch_path, &pb).unwrap();
        assert!(arch_path.with_extension("").metadata().is_err());

        let dst_dir = TempDir::new().unwrap();
        super::sync_decompress(&arch_path, &dst_dir, &pb).unwrap();
        assert!(arch_path.with_extension("").metadata().is_err());
        let dst_entries = scan_data_dir(dst_dir.path()).unwrap();

        let dst_keys = dst_entries.keys().collect::<HashSet<_>>();
        let src_keys = src_entries.keys().collect::<HashSet<_>>();
        let diff_keys = src_keys.symmetric_difference(&dst_keys).collect::<Vec<_>>();
        assert!(diff_keys.is_empty(), "{diff_keys:?}");
        for key in dst_entries.keys() {
            let actual = dst_entries.get(key).unwrap();
            let expected = src_entries.get(key).unwrap();
            assert_eq!(actual, expected, "key {key:?}");
        }
    }

    #[tracing::instrument]
    fn generate_data_dir() -> TempDir {
        let _sw = super::StopWatch::new();

        let src_dir = TempDir::new().unwrap();
        for i in 0..20 {
            let name = std::iter::repeat(unsafe { char::from_u32_unchecked('a' as u32 + i) })
                .take(20)
                .collect::<String>();
            let entry_path = (0..5).fold(PathBuf::new(), |acc, _| acc.join(&name));
            let out_path = src_dir.path().join(&entry_path);
            create_dir_all(out_path.parent().unwrap()).unwrap();
            let mut out_file = File::create(&out_path).unwrap();
            let mut buf = vec![0u8; 1_024_000];
            for _ in 0..10 {
                ThreadRng::default().fill_bytes(&mut buf[..]);
                out_file.write_all(&buf[..]).unwrap();
            }
        }
        src_dir
    }

    #[tracing::instrument]
    fn scan_data_dir(
        data_dir: impl AsRef<Path> + std::fmt::Debug,
    ) -> Result<HashMap<PathBuf, u32>, super::CompressError> {
        let _sw = super::StopWatch::new();

        let data_dir = data_dir.as_ref();
        ignore::WalkBuilder::new(data_dir)
            .standard_filters(false)
            .build()
            .map(|entry| -> Result<_, super::CompressError> {
                let entry = entry?;
                let meta = entry.metadata()?;
                Ok((entry.into_path(), meta))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_par_iter()
            .filter_map(|(path, meta)| if meta.is_file() { Some(path) } else { None })
            .map(|path| -> Result<_, super::CompressError> {
                let entry_path = path.strip_prefix(data_dir)?.to_path_buf();
                let mut hasher = Crc32::new();
                let mut file = File::open(path)?;
                std::io::copy(&mut file, &mut hasher)?;
                let entry_hash = hasher.finalize();
                Ok((entry_path, entry_hash))
            })
            .collect::<Result<HashMap<_, _>, _>>()
    }
}
