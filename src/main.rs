mod cache;
mod commands;
mod compress;
mod credentials;
mod dirs;
mod error;
mod graphql_client;
mod id;
mod process;
mod python;
mod readme;
mod revert_file;

#[pyo3_asyncio::tokio::main]
async fn main() -> pyo3::PyResult<()> {
    if let Err(e) = commands::Cli::run().await {
        eprintln!("{}", e);
        std::process::exit(1)
    }
    Ok(())
}
