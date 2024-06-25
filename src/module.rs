use std::sync::OnceLock;

use crate::commands::Cli;
use aqora_runner::pipeline::{LayerEvaluation, PipelineConfig};
use clap::Parser;
use pyo3::prelude::*;

#[pyfunction]
pub fn main(py: Python<'_>) -> PyResult<()> {
    let _sentry = crate::sentry::setup();

    static TOKIO: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    let tokio = TOKIO.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    });
    pyo3_asyncio::tokio::init_with_runtime(tokio).unwrap();

    let argv = py
        .import("sys")
        .and_then(|sys| sys.getattr("argv"))
        .and_then(|argv| argv.extract::<Vec<String>>())
        .unwrap_or_else(|_| std::env::args().skip(1).collect());

    py.allow_threads(|| {
        tokio.block_on(async {
            let cli = Cli::parse_from(argv);
            cli.run().await;
        });
    });

    Ok(())
}

#[pymodule]
pub fn aqora_cli(_: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(main, m)?)?;
    m.add_class::<PipelineConfig>()?;
    m.add_class::<LayerEvaluation>()?;
    Ok(())
}
