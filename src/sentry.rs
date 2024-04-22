use std::borrow::Cow;

const RELEASE: &str = concat!(env!("CARGO_CRATE_NAME"), "_", env!("CARGO_PKG_VERSION"));

pub fn setup() -> Option<sentry::ClientInitGuard> {
    if cfg!(debug_assertions) || do_not_track() {
        return None;
    }

    let opts = sentry::ClientOptions {
        release: Some(Cow::Borrowed(RELEASE)),
        dsn: if let Some(sentry_dsn) = option_env!("SENTRY_DSN") {
            if let Ok(sentry_dsn) = sentry_dsn.parse() {
                Some(sentry_dsn)
            } else {
                eprintln!("Bad SENTRY_DSN: {sentry_dsn:?}");
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

    return false;
}
