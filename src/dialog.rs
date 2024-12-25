use dialoguer::{theme::Theme, Confirm};

#[derive(Clone)]
pub struct AutoConfirmDialog<'a> {
    confirm: Confirm<'a>,
    auto_confirm: bool,
}

impl<'a> AutoConfirmDialog<'a> {
    pub fn new() -> Self {
        Self {
            confirm: Confirm::new(),
            auto_confirm: false,
        }
    }

    pub fn with_theme(theme: &'a dyn Theme) -> Self {
        Self {
            confirm: Confirm::with_theme(theme),
            auto_confirm: false,
        }
    }

    pub fn auto_confirm(mut self, yes: bool) -> Self {
        self.auto_confirm = yes;
        self
    }

    pub fn with_prompt<S: Into<String>>(mut self, prompt: S) -> Self {
        self.confirm = self.confirm.with_prompt(prompt);
        self
    }

    pub fn default(mut self, val: bool) -> Self {
        self.confirm = self.confirm.default(val);
        self
    }
    pub fn interact(self) -> dialoguer::Result<bool> {
        if self.auto_confirm {
            return Ok(true);
        }

        self.confirm.interact()
    }
}
