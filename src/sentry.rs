use std::borrow::Cow;
use tracing_subscriber::prelude::*;

use crate::manifest::manifest_version;

#[must_use]
pub struct Guard(
    Option<tracing_appender::non_blocking::WorkerGuard>,
    Option<sentry::ClientInitGuard>,
);

pub fn setup() -> Guard {
    Guard(tracing_setup(), sentry_setup())
}

#[inline]
fn is_whitespace_ascii(b: u8) -> bool {
    b <= 0x20u8
}

fn do_not_track() -> bool {
    if let Some(value) = std::env::var_os("DO_NOT_TRACK") {
        value
            .as_encoded_bytes()
            .iter()
            .any(|b| !is_whitespace_ascii(*b))
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

fn tracing_setup() -> Option<tracing_appender::non_blocking::WorkerGuard> {
    let mut layers = Vec::new();
    let mut opt_guard = None;

    // console logger
    layers.push(
        tracing_subscriber::fmt::layer()
            .compact()
            .without_time()
            .with_target(false)
            .with_filter(tracing::level_filters::LevelFilter::INFO)
            .boxed(),
    );

    // file logger
    if let Some(log_dir) = dirs::state_dir()
        .or(dirs::cache_dir())
        .map(|dir| dir.join("aqora"))
    {
        let appender = tracing_appender::rolling::daily(&log_dir, "aqora.log");
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

    tracing_subscriber::registry().with(layers).init();

    opt_guard
}
