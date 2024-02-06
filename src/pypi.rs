pub fn pypi_url(url: &Url, access_token: Option<impl AsRef<str>>) -> Result<Url> {
    let mut url = url.join("/pypi")?;
    if let Some(access_token) = access_token {
        url.set_username(access_token.as_ref())
            .map_err(|_| error::system("Could not set pypi access token", ""))?;
    }
    Ok(url)
}
