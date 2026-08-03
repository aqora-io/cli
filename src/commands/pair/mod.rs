mod agent;
mod prompt;
mod session;
mod target;

use clap::Args;
use serde::Serialize;

use crate::commands::GlobalArgs;
use crate::error::Result;

use agent::{select, Agent};
use prompt::{build_prompt, write_token};
use session::Sessions;
use target::{resolve_dataset_editor, resolve_editor, PairTarget};

#[derive(Args, Debug, Serialize)]
#[command(about = "Pair an agent CLI with a workspace's marimo notebook")]
pub struct Pair {
    /// The workspace, or dataset with --dataset, to pair on, as
    /// "{owner}/{slug}" with an optional "@{version}". Defaults to the newest
    /// draft version.
    target: String,
    /// Pair on a dataset's notebook instead of a workspace's
    #[arg(long)]
    dataset: bool,
    /// The notebook to open, defaulting to the workspace's overview notebook
    #[arg(long)]
    notebook: Option<String>,
    /// Pair with Claude Code instead of the first agent found
    #[arg(long, group = "agent")]
    claude: bool,
    /// Pair with Codex instead of the first agent found
    #[arg(long, group = "agent")]
    codex: bool,
    /// Pair with opencode instead of the first agent found
    #[arg(long, group = "agent")]
    opencode: bool,
    /// Do not open the notebook in a browser
    #[arg(long)]
    no_open: bool,
    /// Print the prompt instead of launching an agent
    #[arg(long, conflicts_with = "agent")]
    prompt_only: bool,
}

impl Pair {
    fn agent(&self) -> Option<Agent> {
        match (self.claude, self.codex, self.opencode) {
            (true, _, _) => Some(Agent::Claude),
            (_, true, _) => Some(Agent::Codex),
            (_, _, true) => Some(Agent::Opencode),
            _ => None,
        }
    }
}

pub async fn pair(args: Pair, global: GlobalArgs) -> Result<()> {
    let target: PairTarget = args.target.parse()?;

    // Selection only reads the local machine, so an agent that cannot pair
    // fails here, before anything is resolved or opened.
    let agent = if args.prompt_only {
        None
    } else {
        Some(select(args.agent(), |agent| agent.availability())?)
    };

    let pb = global
        .spinner()
        .with_message(format!("Resolving the editor for {target}"));
    let client = global.graphql_client().await?;
    let editor = if args.dataset {
        resolve_dataset_editor(&client, &target, args.notebook).await?
    } else {
        resolve_editor(&client, &target, args.notebook).await?
    };
    pb.set_message(format!("Editor for {target} is {}", editor.phase));

    let (token_dir, token_path) = write_token(&editor.token)?;
    let editor_page = global
        .aqora_url()?
        .join(&format!("workspaces/{}/edit", editor.editor_page_id))?;

    // A session only exists while the notebook is open in a browser, so open it
    // — but not if the user already has it open.
    let sessions = Sessions::new(&editor, global.allow_insecure_host)?;
    if !sessions.is_ready().await {
        if args.no_open {
            pb.println(format!("Please open {editor_page} to start the notebook"));
        } else {
            pb.set_message(format!("Opening {editor_page}"));
            if open::that(editor_page.as_str()).is_err() {
                pb.println(format!(
                    "Could not open a browser. Please open {editor_page}"
                ));
            }
        }
        pb.set_message("Waiting for the notebook to connect");
        sessions.wait(&pb, &editor_page).await?;
    }

    let prompt = build_prompt(&editor, &token_path, &editor_page);
    match agent {
        Some(agent) => {
            pb.finish_with_message(format!("Launching {}", agent.display_name()));
            agent.command(&prompt).spawn()?.wait().await?;
            // The agent is done with the token now.
            drop(token_dir);
        }
        None => {
            // The agent this prompt is for starts after we exit, so the token
            // has to outlive us.
            let _ = token_dir.into_path();
            pb.finish_and_clear();
            println!("{prompt}");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{Cli, Commands};
    use clap::Parser;

    fn parse(args: &[&str]) -> std::result::Result<Pair, clap::Error> {
        // `Cli`'s version string reports the embedded interpreter's version.
        pyo3::Python::initialize();
        let argv = [&["aqora", "pair"], args].concat();
        match Cli::try_parse_from(argv)?.commands {
            Commands::Pair(pair) => Ok(pair),
            other => panic!("parsed as {other:?}"),
        }
    }

    #[test]
    fn parses_a_bare_target() {
        let args = parse(&["alice/ws"]).unwrap();
        assert_eq!(args.target, "alice/ws");
        assert_eq!(args.agent(), None);
        assert!(!args.no_open);
        assert!(!args.prompt_only);
    }

    #[test]
    fn an_agent_flag_selects_that_agent() {
        assert_eq!(
            parse(&["alice/ws", "--codex"]).unwrap().agent(),
            Some(Agent::Codex)
        );
    }

    #[test]
    fn parses_the_dataset_flag() {
        assert!(parse(&["alice/ds", "--dataset"]).unwrap().dataset);
    }

    #[test]
    fn rejects_two_agent_flags() {
        assert!(parse(&["alice/ws", "--claude", "--codex"]).is_err());
    }

    #[test]
    fn rejects_an_agent_flag_with_prompt_only() {
        assert!(parse(&["alice/ws", "--claude", "--prompt-only"]).is_err());
    }

    #[test]
    fn prompt_only_and_no_open_are_independent() {
        let args = parse(&["alice/ws", "--prompt-only", "--no-open"]).unwrap();
        assert!(args.prompt_only);
        assert!(args.no_open);
    }
}
