use aqora_runner::python::ColorChoice;
use dialoguer::theme::{ColorfulTheme, SimpleTheme};

pub fn supports_color() -> bool {
    supports_color::on_cached(supports_color::Stream::Stdout)
        .zip(supports_color::on_cached(supports_color::Stream::Stderr))
        .map(|(stdout, stderr)| stdout.has_basic && stderr.has_basic)
        .unwrap_or(false)
}

pub trait ColorChoiceExt {
    fn should_use_color(self) -> bool;
    fn forced(self) -> Self;
    fn set_override(self);
    fn dialoguer(self) -> Box<dyn dialoguer::theme::Theme>;
}

impl ColorChoiceExt for ColorChoice {
    fn should_use_color(self) -> bool {
        match self {
            ColorChoice::Auto => supports_color(),
            ColorChoice::Always => true,
            ColorChoice::Never => false,
        }
    }

    fn forced(self) -> Self {
        if self.should_use_color() {
            ColorChoice::Always
        } else {
            ColorChoice::Never
        }
    }

    fn dialoguer(self) -> Box<dyn dialoguer::theme::Theme> {
        if self.should_use_color() {
            Box::<ColorfulTheme>::default()
        } else {
            Box::new(SimpleTheme)
        }
    }

    fn set_override(self) {
        match self {
            ColorChoice::Auto => {}
            ColorChoice::Always => owo_colors::set_override(true),
            ColorChoice::Never => owo_colors::set_override(false),
        }
    }
}
