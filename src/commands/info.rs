use crate::{
    commands::{version::python_version, GlobalArgs},
    dirs::{config_dir, locate_uv},
    error::Result,
    graphql_client::GraphQLClient,
    manifest::manifest_version,
};
use clap::Args;
use graphql_client::GraphQLQuery;
use serde::Serialize;
use std::env::args;
use which::which;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/viewer_info.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct ViewerInfo;

pub async fn get_viewer_info(global: &GlobalArgs) -> Result<viewer_info::ViewerInfoViewer> {
    Ok(GraphQLClient::new(global.url.parse()?)
        .await?
        .send::<ViewerInfo>(viewer_info::Variables {})
        .await?
        .viewer)
}

#[derive(Args, Debug, Serialize)]
#[command(author, version, about)]
pub struct Info;

pub async fn info(_: Info, global: GlobalArgs) -> Result<()> {
    let command = {
        let command = args().next().unwrap_or_else(|| "aqora".to_string());
        which(&command)
            .map(|c| c.display().to_string())
            .unwrap_or(command)
    };
    let uv_path = locate_uv(global.uv.as_ref()).await;
    let uv_version = {
        if let Some(uv_path) = uv_path.as_ref() {
            let mut cmd = std::process::Command::new(uv_path);
            cmd.arg("--version");
            cmd.output()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        } else {
            Ok("[not found]".to_string())
        }
    };
    let viewer = get_viewer_info(&global).await;
    tracing::info!("Command {}", command);
    tracing::info!("Version {}", manifest_version());
    tracing::info!("Python {}", python_version());
    tracing::info!(
        "UV Path {}",
        uv_path
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "[not found]".to_string())
    );
    tracing::info!(
        "UV Version {}",
        uv_version.unwrap_or_else(|err| format!("[error: {err}]"))
    );
    tracing::info!(
        "Config {}",
        config_dir()
            .await
            .map(|p| p.display().to_string())
            .unwrap_or_else(|err| format!("[error: {err}]"))
    );
    tracing::info!("URL {}", global.url);
    tracing::info!(
        "Viewer {}",
        viewer
            .map(|v| format!("{} {}", v.username, v.id))
            .unwrap_or_else(|err| format!("[error: {err}]"))
    );
    tracing::info!(
        "Project {}",
        global
            .project
            .canonicalize()
            .unwrap_or(global.project)
            .display()
    );
    Ok(())
}
