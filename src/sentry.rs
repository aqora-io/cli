use std::borrow::Cow;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::prelude::*;

use crate::manifest::manifest_version;

#[must_use]
#[allow(dead_code)]
pub struct Guard(
    Option<sentry::ClientInitGuard>,
    Option<tracing_appender::non_blocking::WorkerGuard>,
    TracingGCGuard,
);

pub fn setup() -> Guard {
    Guard(sentry_setup(), tracing_setup(), tracing_gc())
}

fn do_not_track() -> bool {
    if let Some(value) = std::env::var_os("DO_NOT_TRACK") {
        !matches!(
            value.to_string_lossy().to_lowercase().trim(),
            "0" | "false" | "no" | ""
        )
    } else {
        false
    }
}

fn sentry_setup() -> Option<sentry::ClientInitGuard> {
    if cfg!(debug_assertions) || do_not_track() {
        return None;
    }

    let opts = sentry::ClientOptions {
        release: Some(Cow::Owned(manifest_version().to_string())),
        dsn: if let Some(sentry_dsn) = option_env!("SENTRY_DSN") {
            if let Ok(sentry_dsn) = sentry_dsn.parse() {
                Some(sentry_dsn)
            } else {
                tracing::error!("Bad SENTRY_DSN: {sentry_dsn:?}");
                return None;
            }
        } else {
            return None;
        },
        ..Default::default()
    };

    Some(sentry::init(opts))
}

const LOG_FILENAME: &str = "aqora.log";

fn log_dir() -> Option<std::path::PathBuf> {
    dirs::state_dir()
        .or(dirs::cache_dir())
        .map(|dir| dir.join("aqora"))
}

fn tracing_setup() -> Option<tracing_appender::non_blocking::WorkerGuard> {
    let mut layers = Vec::new();
    let mut opt_guard = None;

    let formatter = tracing_subscriber::fmt::format::debug_fn(|writer, field, value| {
        if field.name() == "message" {
            write!(writer, "{value:?}")
        } else {
            Ok(())
        }
    });

    // console logger
    layers.push(
        tracing_subscriber::fmt::layer()
            .compact()
            .without_time()
            .with_target(false)
            .fmt_fields(formatter)
            .with_filter(
                tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(LevelFilter::INFO.into())
                    .with_env_var("AQORA_LOG")
                    .from_env_lossy(),
            )
            .boxed(),
    );

    // file logger
    if let Some(log_dir) = log_dir() {
        let appender = tracing_appender::rolling::daily(log_dir, LOG_FILENAME);
        let (appender, guard) = tracing_appender::non_blocking(appender);
        opt_guard.replace(guard);
        layers.push(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(appender)
                .with_filter(tracing::level_filters::LevelFilter::TRACE)
                .boxed(),
        );
    }

    // sentry logger
    layers.push(
        sentry::integrations::tracing::layer()
            .event_filter(|meta| {
                use sentry::integrations::tracing::EventFilter::*;
                if meta.level() > &tracing::Level::WARN || meta.fields().field("is_user").is_some()
                {
                    Ignore
                } else if meta.level() > &tracing::Level::ERROR {
                    Event
                } else {
                    Exception
                }
            })
            .boxed(),
    );

    tracing_subscriber::registry().with(layers).init();

    opt_guard
}

enum GCRuntimeWrapper {
    None,
    Owned(tokio::runtime::Runtime),
    Borrowed(tokio::runtime::Handle),
}

struct TracingGCGuard {
    runtime: GCRuntimeWrapper,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for TracingGCGuard {
    #[tracing::instrument(skip(self))]
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            match &self.runtime {
                GCRuntimeWrapper::None => {}
                GCRuntimeWrapper::Owned(rt) => {
                    let _ = rt.block_on(task);
                }
                GCRuntimeWrapper::Borrowed(h) => {
                    let _ = h.block_on(task);
                }
            }
        }
    }
}

fn tracing_gc() -> TracingGCGuard {
    use std::{
        io::{Error, ErrorKind, Result, SeekFrom},
        path::{Path, PathBuf},
    };
    use tokio::{fs::File, io::AsyncSeekExt};

    const GC_THRESHOLD: usize = 10_000_000;
    const GC_THREADS: usize = 1;

    /// Scan for archived logfiles on disk.
    #[tracing::instrument(err, skip(dir))]
    async fn read_dir(dir: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
        let mut entries = tokio::fs::read_dir(dir).await?;
        let mut children = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let entry_name = entry.file_name();
            let entry_name = entry_name.to_string_lossy();
            if entry_name == LOG_FILENAME || !entry_name.starts_with(LOG_FILENAME) {
                continue;
            }
            children.push(entry.path());
        }
        Ok(children)
    }

    /// Fetch logging file sizes.
    #[tracing::instrument(err, skip(paths))]
    async fn size_of(paths: &[impl AsRef<Path>]) -> Result<Vec<usize>> {
        let mut sizes = Vec::new();
        for path in paths {
            let mut file = File::open(path).await?;
            let pos = file.seek(SeekFrom::End(0)).await?;
            sizes.push(pos as usize);
        }
        Ok(sizes)
    }

    /// Return first index where the accumulation exceeds gc threshold.
    /// For example:
    /// - with threshold=100 and sizes=[200, 10, 10], it will return 0,
    /// - with threshold=100 and sizes=[70, 30, 50], it will return 1,
    /// - with threshold=100 and sizes=[60, 30, 40], it will return 2,
    /// - with threshold=100 and sizes=[10, 20, 30], it will return 3,
    fn find_garbage(sizes: &[usize], threshold: usize) -> usize {
        let mut acc = 0;
        for (index, size) in sizes.iter().enumerate() {
            acc += size;
            if acc >= threshold {
                return index;
            }
        }
        sizes.len()
    }

    #[tracing::instrument(err, skip(paths))]
    async fn erase_all(paths: &[impl AsRef<Path>]) -> Result<()> {
        let mut last_error = Ok(());
        for child in paths {
            let child = child.as_ref();
            last_error = tokio::fs::remove_file(child).await;
            tracing::debug!("GCed {child:?}: {last_error:?}");
        }
        last_error
    }

    #[tracing::instrument(err)]
    async fn attempt_gc() -> Result<()> {
        let gc_dir = log_dir().ok_or(Error::from(ErrorKind::NotFound))?;

        // (a) account for every log files that would need to be collected,
        let mut files = read_dir(gc_dir).await?;
        if files.is_empty() {
            return Ok(());
        }

        // (b) sort files by descending order (newest files first),
        files.sort_by(|lhs, rhs| lhs.file_name().cmp(&rhs.file_name()).reverse());

        // (c) get each files' size,
        let sizes = size_of(&files[..]).await?;

        // (d) find index where total size exceeds our threshold,
        let gc_index = find_garbage(&sizes[..], GC_THRESHOLD);

        // (e) erase everything else after
        let to_collect = files.iter().skip(gc_index).collect::<Vec<_>>();
        erase_all(&to_collect[..]).await
    }

    #[tracing::instrument]
    async fn run_gc() {
        let start = std::time::Instant::now();
        let succeed = attempt_gc().await.is_ok();
        let elapsed = start.elapsed().as_micros();
        tracing::debug!("succeed={succeed:?} in {elapsed}us");
    }

    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        TracingGCGuard {
            task: Some(handle.spawn(run_gc())),
            runtime: GCRuntimeWrapper::Borrowed(handle),
        }
    } else {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(GC_THREADS)
            .build();
        if let Ok(rt) = rt {
            TracingGCGuard {
                task: Some(rt.spawn(run_gc())),
                runtime: GCRuntimeWrapper::Owned(rt),
            }
        } else {
            tracing::debug!("cannot create tokio runtime");
            TracingGCGuard {
                task: None,
                runtime: GCRuntimeWrapper::None,
            }
        }
    }
}
