use std::boxed::Box;

use dialoguer::{
    theme::{SimpleTheme, Theme},
    Confirm as BaseConfirm, FuzzySelect as BaseFuzzySelect, Input,
};

pub struct Confirm {
    theme: Box<dyn Theme>,
    no_prompt: bool,
    no_prompt_value: Option<bool>,
    prompt: String,
    report: bool,
    default: Option<bool>,
    show_default: bool,
    initial_text: Option<String>,
    wait_for_newline: bool,
}

impl Default for Confirm {
    fn default() -> Self {
        Confirm::new()
    }
}

impl Confirm {
    pub fn new() -> Self {
        Self {
            theme: Box::new(SimpleTheme),
            no_prompt: false,
            no_prompt_value: None,
            prompt: "".into(),
            report: true,
            default: None,
            initial_text: None,
            show_default: true,
            wait_for_newline: false,
        }
    }

    pub fn with_theme(self, theme: Box<dyn Theme>) -> Self {
        Self { theme, ..self }
    }

    pub fn no_prompt(self, no_prompt: bool) -> Self {
        Self { no_prompt, ..self }
    }

    pub fn no_prompt_value(self, no_prompt_value: bool) -> Self {
        Self {
            no_prompt_value: Some(no_prompt_value),
            ..self
        }
    }

    pub fn with_prompt<S: Into<String>>(self, prompt: S) -> Self {
        Self {
            prompt: prompt.into(),
            ..self
        }
    }

    pub fn default(self, val: bool) -> Self {
        Self {
            default: Some(val),
            ..self
        }
    }

    pub fn with_initial_text<S: Into<String>>(self, text: S) -> Self {
        Self {
            initial_text: Some(text.into()),
            ..self
        }
    }

    pub fn interact(self) -> dialoguer::Result<bool> {
        if self.no_prompt {
            if let Some(default) = self.no_prompt_value.or(self.default) {
                return Ok(default);
            } else {
                return Err(dialoguer::Error::IO(std::io::Error::other(
                    "No auto confirm value set on dialog",
                )));
            }
        }
        let mut confirm = BaseConfirm::with_theme(self.theme.as_ref())
            .report(self.report)
            .with_prompt(self.prompt)
            .show_default(self.show_default)
            .wait_for_newline(self.wait_for_newline);
        if let Some(default) = self.default {
            confirm = confirm.default(default);
        }
        confirm.interact()
    }

    pub fn interact_text(self) -> dialoguer::Result<String> {
        let mut input = Input::with_theme(self.theme.as_ref())
            .report(self.report)
            .allow_empty(false)
            .with_prompt(self.prompt)
            .show_default(self.show_default);

        if let Some(initial_text) = self.initial_text {
            input = input.with_initial_text(initial_text);
        }

        input.interact_text()
    }
}

pub struct FuzzySelect {
    theme: Box<dyn Theme>,
    no_prompt: bool,
    default: Option<usize>,
    items: Vec<String>,
    prompt: String,
    report: bool,
    clear: bool,
    highlight_matches: bool,
    enable_vim_mode: bool,
    max_length: Option<usize>,
    initial_text: String,
}

impl Default for FuzzySelect {
    fn default() -> Self {
        FuzzySelect::new()
    }
}

impl FuzzySelect {
    pub fn new() -> Self {
        Self {
            theme: Box::new(SimpleTheme),
            no_prompt: false,
            default: None,
            items: vec![],
            prompt: "".into(),
            report: true,
            clear: true,
            highlight_matches: true,
            enable_vim_mode: false,
            max_length: None,
            initial_text: "".into(),
        }
    }

    pub fn with_theme(self, theme: Box<dyn Theme>) -> Self {
        Self { theme, ..self }
    }

    pub fn no_prompt(self, no_prompt: bool) -> Self {
        Self { no_prompt, ..self }
    }

    pub fn with_prompt<S: Into<String>>(self, prompt: S) -> Self {
        Self {
            prompt: prompt.into(),
            ..self
        }
    }

    pub fn items(self, items: impl IntoIterator<Item = impl ToString>) -> Self {
        Self {
            items: items.into_iter().map(|item| item.to_string()).collect(),
            ..self
        }
    }

    pub fn with_initial_text<S: Into<String>>(self, text: S) -> Self {
        Self {
            initial_text: text.into(),
            ..self
        }
    }

    pub fn interact_opt(self) -> dialoguer::Result<Option<usize>> {
        if self.no_prompt {
            return Ok(None);
        }

        let mut select = BaseFuzzySelect::with_theme(self.theme.as_ref())
            .report(self.report)
            .clear(self.clear)
            .with_prompt(self.prompt)
            .items(&self.items)
            .clear(self.clear)
            .highlight_matches(self.highlight_matches)
            .vim_mode(self.enable_vim_mode)
            .with_initial_text(self.initial_text);
        if let Some(max_length) = self.max_length {
            select = select.max_length(max_length)
        }
        if let Some(default) = self.default {
            select = select.default(default);
        }
        select.interact_opt()
    }
}
