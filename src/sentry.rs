use std::borrow::Cow;

use chrono::Utc;
use colored::Colorize as _;

const RELEASE: &str = concat!(env!("CARGO_CRATE_NAME"), "_", env!("CARGO_PKG_VERSION"));

pub fn setup() -> Option<sentry::ClientInitGuard> {
    fern_setup();

    log::trace!("Hello, World!");

    if cfg!(debug_assertions) || do_not_track() {
        return None;
    }

    let opts = sentry::ClientOptions {
        release: Some(Cow::Borrowed(RELEASE)),
        dsn: if let Some(sentry_dsn) = option_env!("SENTRY_DSN") {
            if let Ok(sentry_dsn) = sentry_dsn.parse() {
                Some(sentry_dsn)
            } else {
                log::error!("Bad SENTRY_DSN: {sentry_dsn:?}");
                return None;
            }
        } else {
            return None;
        },
        ..Default::default()
    };

    Some(sentry::init(opts))
}

fn do_not_track() -> bool {
    if let Some(value) = std::env::var_os("DO_NOT_TRACK") {
        return value.as_encoded_bytes().iter().any(|b| *b > 0x20u8);
    }

    false
}

fn fern_setup() {
    let env_level = std::env::var("AQORA_LOG")
        .ok()
        .and_then(|env| env.parse().ok())
        .unwrap_or(log::LevelFilter::Info);

    let mut log = fern::Dispatch::new();

    // console logger
    log = log.chain(
        fern::Dispatch::new()
            .level(env_level)
            .format(|out, message, record| {
                let level = match record.level() {
                    log::Level::Error => "E".red(),
                    log::Level::Warn => "W".yellow(),
                    log::Level::Info => "I".green(),
                    log::Level::Debug => "D".white(),
                    log::Level::Trace => "T".blue(),
                };

                out.finish(format_args!("[{level}] {message}"))
            })
            .chain(std::io::stdout()),
    );

    // file logger
    if let Some(base_dir) = dirs::state_dir().or(dirs::cache_dir()) {
        let log_dir = base_dir.join("aqora");
        if std::fs::create_dir_all(&log_dir).is_ok() {
            let log_path = log_dir.join("aqora.log");
            if let Ok(file) = fern::log_file(log_path) {
                log = log.chain(
                    fern::Dispatch::new()
                        .level(std::cmp::max(log::LevelFilter::Debug, env_level))
                        .format(|out, message, record| {
                            out.finish(format_args!(
                                "[{} {} {}] {}",
                                Utc::now().to_rfc3339(),
                                record.level(),
                                record.target(),
                                message
                            ))
                        })
                        .chain(file),
                );
            }
        }
    }

    log.apply()
        .expect("A logger has already been already initialized");
}
