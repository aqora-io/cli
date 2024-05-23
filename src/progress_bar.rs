use indicatif::{ProgressState, ProgressStyle};
use std::fmt::Write;

pub fn pretty() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner} [{elapsed_precise}] {msg} [{wide_bar}] {pos:>7}/{len:7} ({eta})",
    )
    .unwrap()
    .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
        write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
    })
    .progress_chars("#>-")
}

pub fn pretty_bytes() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner} [{elapsed_precise}] {msg} [{wide_bar}] {bytes}/{total_bytes} ({eta})",
    )
    .unwrap()
    .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
        write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
    })
    .progress_chars("#>-")
}
