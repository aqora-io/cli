use crate::error::Result;
use crate::login::get_access_token;
use clap::Args;
use url::Url;

#[derive(Args, Debug)]
#[command(author, version, about)]
pub struct Test {
    #[arg(short, long, default_value = "https://app.aqora.io")]
    pub url: String,
}

pub async fn test(args: Test) -> Result<()> {
    println!("{:?}", get_access_token(&Url::parse(&args.url)?).await?);
    Ok(())
}
