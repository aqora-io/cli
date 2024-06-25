use aqora_cli::Cli;
use clap::Parser;
use pyo3::Python;

fn main() {
    let _sentry = aqora_cli::sentry::setup();

    pyo3::prepare_freethreaded_python();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    pyo3_asyncio::tokio::init(builder);

    let cli = Cli::parse();

    Python::with_gil(|py| {
        let _ = pyo3_asyncio::tokio::run(py, async move {
            cli.run().await;
            Ok(())
        });
    });
}
