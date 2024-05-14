use aqora_cli::{Cli, CliExit};
use clap::Parser;

fn main() -> CliExit {
    let _sentry = aqora_cli::sentry::setup();
    pyo3::prepare_freethreaded_python();
    let cli = Cli::parse();
    let mut builder = pyo3_asyncio::tokio::re_exports::runtime::Builder::new_multi_thread();
    builder.enable_all();
    pyo3_asyncio::tokio::init(builder);
    pyo3::Python::with_gil(|py| cli.run(py))
}
