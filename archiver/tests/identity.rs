use aqora_archiver::{Archiver, Unarchiver};
use base64::prelude::*;
use rand::{rngs::ThreadRng, thread_rng, RngCore};
use rayon::iter::{IntoParallelIterator, ParallelIterator as _};
use std::{
    collections::{HashMap, HashSet},
    fs::{create_dir_all, File},
    io::Write,
    path::{Path, PathBuf},
};
use tempfile::{TempDir, TempPath};
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

#[cfg(feature = "indicatif")]
fn create_archiver(input: &Path, output: &Path) -> Archiver {
    let pb = indicatif::ProgressBar::with_draw_target(
        Some(80),
        indicatif::ProgressDrawTarget::stdout_with_hz(1),
    );
    Archiver::new_with_progress_bar(input.to_path_buf(), output.to_path_buf(), pb.clone())
}

#[cfg(not(feature = "indicatif"))]
fn create_archiver(input: &Path, output: &Path) -> Archiver {
    Archiver::new(input.to_path_buf(), output.to_path_buf())
}

#[cfg(feature = "indicatif")]
fn create_unarchiver(input: &Path, output: &Path) -> Unarchiver {
    let pb = indicatif::ProgressBar::with_draw_target(
        Some(80),
        indicatif::ProgressDrawTarget::stdout_with_hz(1),
    );
    Unarchiver::new_with_progress_bar(input.to_path_buf(), output.to_path_buf(), pb.clone())
}

#[cfg(not(feature = "indicatif"))]
fn create_unarchiver(input: &Path, output: &Path) -> Unarchiver {
    Unarchiver::new(input.to_path_buf(), output.to_path_buf())
}

#[test]
fn test_identity() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            tracing_subscriber::filter::LevelFilter::from_level(tracing::Level::DEBUG),
        ))
        .init();

    let src_dir = generate_data_dir();
    let src_entries = scan_data_dir(&src_dir).unwrap();

    let mut arch_name = vec![0u8; 8];
    thread_rng().fill_bytes(&mut arch_name[..]);
    let arch_name = BASE64_STANDARD.encode(arch_name);

    let arch_path = TempPath::from_path(std::env::temp_dir().join(format!("{arch_name}.tar.zst")));
    create_archiver(src_dir.path(), &arch_path)
        .synchronously()
        .expect("Cannot run archiver synchronously");
    assert!(arch_path.with_extension("").metadata().is_err());

    let dst_dir = TempDir::new().unwrap();
    create_unarchiver(&arch_path, dst_dir.path())
        .synchronously()
        .expect("Cannot run unarchiver synchronously");
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
) -> Result<HashMap<PathBuf, u32>, aqora_archiver::Error> {
    let data_dir = data_dir.as_ref();
    ignore::WalkBuilder::new(data_dir)
        .standard_filters(false)
        .build()
        .map(|entry| -> Result<_, aqora_archiver::Error> {
            let entry = entry?;
            let meta = entry.metadata()?;
            Ok((entry.into_path(), meta))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_par_iter()
        .filter_map(|(path, meta)| if meta.is_file() { Some(path) } else { None })
        .map(|path| -> Result<_, aqora_archiver::Error> {
            let entry_path = path.strip_prefix(data_dir)?.to_path_buf();
            let mut hasher = Crc32::new();
            let mut file = File::open(path)?;
            std::io::copy(&mut file, &mut hasher)?;
            let entry_hash = hasher.finalize();
            Ok((entry_path, entry_hash))
        })
        .collect::<Result<HashMap<_, _>, _>>()
}
