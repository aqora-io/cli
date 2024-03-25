use crate::{
    commands::{version::version, GlobalArgs},
    dirs::{config_dir, locate_uv},
    error::Result,
    graphql_client::GraphQLClient,
};
use clap::Args;
use graphql_client::GraphQLQuery;
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

#[derive(Args, Debug)]
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
    println!("Command {}", command);
    println!("Version {}", version());
    println!(
        "UV Path {}",
        uv_path
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "[not found]".to_string())
    );
    println!(
        "UV Version {}",
        uv_version.unwrap_or_else(|err| format!("[error: {err}]"))
    );
    println!(
        "Config {}",
        config_dir()
            .await
            .map(|p| p.display().to_string())
            .unwrap_or_else(|err| format!("[error: {err}]"))
    );
    println!("URL {}", global.url);
    println!(
        "Viewer {}",
        viewer
            .map(|v| format!("{} {}", v.username, v.id))
            .unwrap_or_else(|err| format!("[error: {err}]"))
    );
    println!(
        "Project {}",
        global
            .project
            .canonicalize()
            .unwrap_or(global.project)
            .display()
    );
    Ok(())
}
