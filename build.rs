use sentry_types::Dsn;
use std::{env, str::FromStr};
use toml_edit::DocumentMut;

fn main() {
    println!("cargo:rerun-if-env-changed=CARGO_PKG_VERSION");
    let version = std::env::var("CARGO_PKG_VERSION").unwrap();
    let mut document = std::fs::read_to_string("pyproject.toml")
        .unwrap()
        .parse::<DocumentMut>()
        .unwrap();
    *document
        .get_mut("project")
        .unwrap()
        .get_mut("version")
        .unwrap() = toml_edit::value(version);
    std::fs::write("pyproject.toml", document.to_string()).unwrap();

    println!("cargo:rerun-if-env-changed=SENTRY_DSN");
    match env::var("SENTRY_DSN") {
        Ok(sentry_dsn) => {
            if let Err(dsn_error) = Dsn::from_str(&sentry_dsn) {
                panic!("Malformed Sentry DSN {sentry_dsn:?}: {dsn_error:?}");
            }
        }
        Err(env::VarError::NotPresent) => {}
        Err(error) => panic!("Unexpected Sentry DSN: {error:?}"),
    }
}
