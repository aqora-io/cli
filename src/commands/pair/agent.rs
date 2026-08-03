use std::path::PathBuf;

use crate::error::{self, Result};

const SKILL_NAME: &str = "marimo-pair";
const SKILL_FILE: &str = "SKILL.md";

pub const INSTALL_HINT: &str = "npx skills add marimo-team/marimo-pair";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agent {
    Claude,
    Codex,
    Opencode,
}

/// What an agent is missing, if anything. Both halves are needed to pair: the
/// CLI to launch, and the skill the prompt tells it to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Availability {
    pub binary: bool,
    pub skill: bool,
}

impl Availability {
    fn is_ready(&self) -> bool {
        self.binary && self.skill
    }
}

impl Agent {
    /// The order auto-detection tries.
    pub const ALL: [Agent; 3] = [Agent::Claude, Agent::Codex, Agent::Opencode];

    pub fn display_name(&self) -> &'static str {
        match self {
            Agent::Claude => "Claude Code",
            Agent::Codex => "Codex",
            Agent::Opencode => "opencode",
        }
    }

    pub fn binary(&self) -> &'static str {
        match self {
            Agent::Claude => "claude",
            Agent::Codex => "codex",
            Agent::Opencode => "opencode",
        }
    }

    /// The prompt goes in as a single argument. It carries no token — only the
    /// path to one — so it is safe in `ps` and in shell history.
    pub fn command(&self, prompt: &str) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(self.binary());
        match self {
            Agent::Claude | Agent::Codex => command.arg(prompt),
            Agent::Opencode => command.args(["--prompt", prompt]),
        };
        command
    }

    pub fn availability(&self) -> Availability {
        Availability {
            binary: which::which(self.binary()).is_ok(),
            skill: self.has_skill(),
        }
    }

    /// Directories that may hold `<dir>/marimo-pair/SKILL.md`, mirroring the
    /// layouts `marimo pair prompt` looks in.
    fn skill_dirs(&self) -> Vec<PathBuf> {
        let home = dirs::home_dir();
        let cwd = std::env::current_dir().ok();
        let roots = |sub: &[&str]| -> Vec<PathBuf> {
            let mut dirs = Vec::new();
            for root in [home.as_ref(), cwd.as_ref()].into_iter().flatten() {
                dirs.push(sub.iter().fold(root.clone(), |path, part| path.join(part)));
            }
            dirs
        };
        match self {
            Agent::Claude => [
                roots(&[".claude", "skills"]),
                roots(&[".claude", "plugins"]),
                roots(&[".claude", "plugins", "marketplaces"]),
            ]
            .concat(),
            Agent::Codex => roots(&[".codex", "skills"]),
            Agent::Opencode => [
                roots(&[".opencode", "skills"]),
                roots(&[".config", "opencode", "skills"]),
                roots(&[".claude", "skills"]),
                roots(&[".agents", "skills"]),
            ]
            .concat(),
        }
    }

    /// `Path::exists` follows symlinks, which matters — skills are commonly
    /// installed once and symlinked into each agent's directory.
    pub fn has_skill(&self) -> bool {
        self.skill_dirs()
            .into_iter()
            .any(|dir| dir.join(SKILL_NAME).join(SKILL_FILE).exists())
    }
}

fn skill_missing(agent: Agent) -> error::Error {
    error::user(
        &format!(
            "The {SKILL_NAME} skill for {} could not be found",
            agent.display_name()
        ),
        &format!("Install it with:\n\n  {INSTALL_HINT}"),
    )
}

/// Decide which agent to launch. Selection is local, so it runs before anything
/// is resolved or opened and a misconfigured machine fails without side effects.
pub fn select(
    explicit: Option<Agent>,
    availability: impl Fn(Agent) -> Availability,
) -> Result<Agent> {
    if let Some(agent) = explicit {
        let available = availability(agent);
        if !available.binary {
            return Err(error::user(
                &format!("{} is not installed", agent.display_name()),
                &format!(
                    "Install it and make sure '{}' is on your PATH, or pair with another agent.",
                    agent.binary()
                ),
            ));
        }
        if !available.skill {
            return Err(skill_missing(agent));
        }
        return Ok(agent);
    }

    Agent::ALL
        .into_iter()
        .find(|agent| availability(*agent).is_ready())
        .ok_or_else(|| {
            error::user(
                "No agent is ready to pair",
                &format!(
                    "Pairing needs claude, codex or opencode on your PATH with the \
                     {SKILL_NAME} skill installed.\n\nInstall the skill with:\n\n  \
                     {INSTALL_HINT}\n\nOr print the prompt for another agent with --prompt-only.",
                ),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const READY: Availability = Availability {
        binary: true,
        skill: true,
    };
    const NO_BINARY: Availability = Availability {
        binary: false,
        skill: true,
    };
    const NO_SKILL: Availability = Availability {
        binary: true,
        skill: false,
    };
    const MISSING: Availability = Availability {
        binary: false,
        skill: false,
    };

    /// Availability by agent, in `Agent::ALL` order.
    fn given(all: [Availability; 3]) -> impl Fn(Agent) -> Availability {
        move |agent| all[Agent::ALL.iter().position(|a| *a == agent).unwrap()]
    }

    #[test]
    fn auto_picks_the_first_fully_available_agent() {
        let selected = select(None, given([READY, READY, READY])).unwrap();
        assert_eq!(selected, Agent::Claude);
    }

    #[test]
    fn auto_skips_an_agent_that_is_missing_its_skill() {
        let selected = select(None, given([NO_SKILL, READY, READY])).unwrap();
        assert_eq!(selected, Agent::Codex);
    }

    #[test]
    fn auto_skips_an_agent_that_is_not_installed() {
        let selected = select(None, given([NO_BINARY, NO_BINARY, READY])).unwrap();
        assert_eq!(selected, Agent::Opencode);
    }

    #[test]
    fn auto_errors_when_no_agent_is_ready() {
        let err = select(None, given([NO_SKILL, NO_BINARY, MISSING])).unwrap_err();
        assert!(err.is_user());
        assert!(err.to_string().contains(INSTALL_HINT), "{err}");
    }

    /// The prompt is one argv element, never split and never a shell string.
    fn argv(agent: Agent) -> Vec<String> {
        let command = agent.command("pair with me");
        std::iter::once(command.as_std().get_program())
            .chain(command.as_std().get_args())
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn claude_and_codex_take_the_prompt_as_their_first_argument() {
        assert_eq!(argv(Agent::Claude), ["claude", "pair with me"]);
        assert_eq!(argv(Agent::Codex), ["codex", "pair with me"]);
    }

    #[test]
    fn opencode_takes_the_prompt_behind_its_prompt_flag() {
        assert_eq!(
            argv(Agent::Opencode),
            ["opencode", "--prompt", "pair with me"]
        );
    }

    #[test]
    fn an_explicit_agent_is_used_when_it_is_ready() {
        let selected = select(Some(Agent::Opencode), given([READY, READY, READY])).unwrap();
        assert_eq!(selected, Agent::Opencode);
    }

    #[test]
    fn an_explicit_agent_errors_when_its_binary_is_missing() {
        let err = select(Some(Agent::Codex), given([READY, NO_BINARY, READY])).unwrap_err();
        assert!(err.is_user());
        assert!(err.to_string().contains("codex"), "{err}");
    }

    #[test]
    fn an_explicit_agent_errors_when_its_skill_is_missing() {
        let err = select(Some(Agent::Claude), given([NO_SKILL, READY, READY])).unwrap_err();
        assert!(err.is_user());
        assert!(err.to_string().contains(INSTALL_HINT), "{err}");
    }
}
