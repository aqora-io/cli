use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::borrow::Cow;
use std::fmt::Write;

pub struct TempProgressStyle<'a> {
    pb: Cow<'a, ProgressBar>,
    style: ProgressStyle,
}

impl<'a> TempProgressStyle<'a> {
    pub fn new(pb: &'a ProgressBar) -> Self {
        Self {
            style: pb.style(),
            pb: Cow::Borrowed(pb),
        }
    }
    pub fn owned(pb: ProgressBar) -> TempProgressStyle<'static> {
        TempProgressStyle {
            style: pb.style(),
            pb: Cow::Owned(pb),
        }
    }
}

impl Drop for TempProgressStyle<'_> {
    fn drop(&mut self) {
        self.pb.reset();
        self.pb.set_style(self.style.clone());
    }
}

pub fn default_spinner(tick: bool) -> ProgressBar {
    let progress = ProgressBar::new_spinner();
    if tick {
        progress.enable_steady_tick(std::time::Duration::from_millis(100));
    } else {
        let style = ProgressStyle::default_spinner().tick_chars("+ ");
        progress.set_style(style);
    }
    progress
}

pub fn pretty() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner} [{elapsed_precise}] {msg} [{wide_bar}] {pos:>7}/{len:7} ({eta})",
    )
    .unwrap()
    .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
        write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
    })
    .progress_chars("=>-")
}

pub fn pretty_bytes() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner} [{elapsed_precise}] {msg} [{wide_bar}] {bytes}/{total_bytes} ({eta})",
    )
    .unwrap()
    .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
        write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
    })
    .progress_chars("=>-")
}
