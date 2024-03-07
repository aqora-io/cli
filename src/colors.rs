use aqora_runner::python::ColorChoice as PipColorChoice;
use clap::ColorChoice;

pub trait ColorChoiceExt {
    fn pip(self) -> PipColorChoice;
    fn set_override(self);
}

impl ColorChoiceExt for ColorChoice {
    fn pip(self) -> PipColorChoice {
        match self {
            ColorChoice::Auto => {
                if supports_color::on_cached(supports_color::Stream::Stdout)
                    .zip(supports_color::on_cached(supports_color::Stream::Stderr))
                    .map(|(stdout, stderr)| stdout.has_basic && stderr.has_basic)
                    .unwrap_or(false)
                {
                    PipColorChoice::Always
                } else {
                    PipColorChoice::Never
                }
            }
            ColorChoice::Always => PipColorChoice::Always,
            ColorChoice::Never => PipColorChoice::Never,
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
