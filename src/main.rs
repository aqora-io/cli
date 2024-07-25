use std::process::ExitCode;

fn main() -> ExitCode {
    let _sentry = aqora_cli::sentry::setup();
    pyo3::prepare_freethreaded_python();
    aqora_cli::run(std::env::args_os()).into()
}
