use std::ffi::OsString;
use std::sync::OnceLock;

use clap::Parser;

use crate::commands::Cli;

static TOKIO: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

pub fn run<I, T>(args: I) -> u8
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = Cli::parse_from(args);
    let tokio = TOKIO.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    });
    pyo3_asyncio::tokio::init_with_runtime(tokio).unwrap();
    let success = tokio.block_on(async { cli.run().await });
    if success {
        0
    } else {
        1
    }
}
