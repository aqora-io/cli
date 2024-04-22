use toml_edit::DocumentMut;

fn main() {
    println!("cargo:rerun-if-env-changed=SENTRY_DSN");
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
}
