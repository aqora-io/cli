mod common;

use crate::common::Crc32Writer;
use aqora_archiver::{ArchiveKind, Archiver, Compression, Unarchiver};
use rand::{thread_rng, RngCore};
use rayon::iter::{IntoParallelIterator, ParallelIterator as _};
use std::{
    collections::{HashMap, HashSet},
    fs::{create_dir_all, File},
    io::Write,
    path::{Path, PathBuf},
    sync::OnceLock,
};
use tempfile::{NamedTempFile, TempDir};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt as _, Layer};

fn create_archiver(input: &Path, output: &Path) -> Archiver {
    let mut archiver = Archiver::new(input.to_path_buf(), output.to_path_buf());
    if cfg!(feature = "indicatif") {
        archiver = archiver.with_progress_bar(indicatif::ProgressBar::with_draw_target(
            Some(80),
            indicatif::ProgressDrawTarget::stdout_with_hz(1),
        ));
    }
    archiver
}

fn create_unarchiver(input: &Path, output: &Path) -> Unarchiver {
    let mut unarchiver = Unarchiver::new(input.to_path_buf(), output.to_path_buf());
    if cfg!(feature = "indicatif") {
        unarchiver = unarchiver.with_progress_bar(indicatif::ProgressBar::with_draw_target(
            Some(80),
            indicatif::ProgressDrawTarget::stdout_with_hz(1),
        ));
    }
    unarchiver
}

fn run_test_identity(src_dir: &Path, arch_kind: ArchiveKind) {
    let src_entries = scan_data_dir(src_dir).unwrap();

    let arch_path = NamedTempFile::new().unwrap();

    create_archiver(src_dir, arch_path.path())
        .with_target_kind(arch_kind)
        .synchronously()
        .expect("Cannot run archiver synchronously");
    // assert!(arch_path.path().with_extension("").metadata().is_err());

    let dst_dir = TempDir::new().unwrap();
    create_unarchiver(arch_path.path(), dst_dir.path())
        .with_source_kind(arch_kind)
        .synchronously()
        .expect("Cannot run unarchiver synchronously");
    // assert!(arch_path.path().with_extension("").metadata().is_err());

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

static TRACING_SETUP: OnceLock<()> = OnceLock::new();
static DATA_DIR: OnceLock<TempDir> = OnceLock::new();

fn tracing_setup() {
    TRACING_SETUP.get_or_init(|| {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_filter(
                tracing_subscriber::filter::LevelFilter::from_level(tracing::Level::DEBUG),
            ))
            .init()
    });
}

fn data_dir() -> &'static Path {
    let data_dir = DATA_DIR.get_or_init(|| generate_data_dir(20, 10, 5));
    data_dir.path()
}

#[test]
fn test_identity_tar_zst() {
    tracing_setup();
    run_test_identity(data_dir(), ArchiveKind::Tar(Some(Compression::Zstandard)));
}

#[test]
fn test_identity_tar_gz() {
    tracing_setup();
    run_test_identity(data_dir(), ArchiveKind::Tar(Some(Compression::Gzip)));
}

#[test]
fn test_identity_tar() {
    tracing_setup();
    run_test_identity(data_dir(), ArchiveKind::Tar(None));
}

#[test]
fn test_identity_zip() {
    tracing_setup();
    run_test_identity(data_dir(), ArchiveKind::Zip);
}

#[tracing::instrument]
fn generate_data_dir(
    num_entries: u32,
    entry_size_megabytes: u8,
    entry_hierarchy_depth: u8,
) -> TempDir {
    let src_dir = TempDir::new().unwrap();
    for i in 0..num_entries {
        let name = std::iter::repeat(unsafe { char::from_u32_unchecked('a' as u32 + i) })
            .take(20)
            .collect::<String>();
        let entry_path = (0..entry_hierarchy_depth).fold(PathBuf::new(), |acc, _| acc.join(&name));
        let out_path = src_dir.path().join(&entry_path);
        create_dir_all(out_path.parent().unwrap()).unwrap();
        let mut out_file = File::create(&out_path).unwrap();
        let mut buf = vec![0u8; 1_024_000];
        for _ in 0..entry_size_megabytes {
            thread_rng().fill_bytes(&mut buf[..]);
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
            let mut hasher = crate::Crc32Writer::new();
            let mut file = File::open(path)?;
            std::io::copy(&mut file, &mut hasher)?;
            let entry_hash = hasher.finalize();
            Ok((entry_path, entry_hash))
        })
        .collect::<Result<HashMap<_, _>, _>>()
}
