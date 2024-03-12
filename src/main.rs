use aqora::Cli;
use clap::Parser;

#[pyo3_asyncio::tokio::main]
async fn main() -> pyo3::PyResult<()> {
    let cli = Cli::parse();
    pyo3::prepare_freethreaded_python();
    if let Err(e) = cli.run().await {
        eprintln!("{}", e);
        std::process::exit(1)
    }
    Ok(())
}
