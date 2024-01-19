use crate::error::Result;
use crate::graphql_client::graphql_client;
use clap::Args;

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Test {
    #[arg(short, long, default_value = "https://app.aqora.io")]
    pub url: String,
}

pub async fn test(args: Test) -> Result<()> {
    let client = graphql_client(args.url.parse()?).await?;
    Ok(())
}
