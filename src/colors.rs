use aqora_runner::python::ColorChoice as PipColorChoice;
use clap::ColorChoice;
use dialoguer::theme::{ColorfulTheme, SimpleTheme};

pub fn supports_color() -> bool {
    supports_color::on_cached(supports_color::Stream::Stdout)
        .zip(supports_color::on_cached(supports_color::Stream::Stderr))
        .map(|(stdout, stderr)| stdout.has_basic && stderr.has_basic)
        .unwrap_or(false)
}

pub trait ColorChoiceExt {
    fn pip(self) -> PipColorChoice;
    fn set_override(self);
    fn dialoguer(self) -> Box<dyn dialoguer::theme::Theme>;
}

impl ColorChoiceExt for ColorChoice {
    fn pip(self) -> PipColorChoice {
        match self {
            ColorChoice::Auto => {
                if supports_color() {
                    PipColorChoice::Always
                } else {
                    PipColorChoice::Never
                }
            }
            ColorChoice::Always => PipColorChoice::Always,
            ColorChoice::Never => PipColorChoice::Never,
        }
    }

    fn dialoguer(self) -> Box<dyn dialoguer::theme::Theme> {
        match self {
            ColorChoice::Auto => {
                if supports_color() {
                    Box::<ColorfulTheme>::default()
                } else {
                    Box::new(SimpleTheme)
                }
            }
            ColorChoice::Always => Box::<ColorfulTheme>::default(),
            ColorChoice::Never => Box::new(SimpleTheme),
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
