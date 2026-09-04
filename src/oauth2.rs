use crate::error::{self, Result};
use aqora_client::{credentials::CredentialsProvider, error::BoxError};
use async_trait::async_trait;
use base64::prelude::*;
use chrono::{DateTime, Duration, Utc};
use futures::{prelude::*, stream::BoxStream};
use graphql_client::GraphQLQuery;
use ring::signature::KeyPair;
use tokio::sync::RwLock;
use url::Url;

const EXPIRATION_PADDING_SEC: i64 = 60;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/oauth2_redirect_subscription.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub(crate) struct Oauth2RedirectSubscription;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/oauth2_token.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub(crate) struct Oauth2TokenMutation;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/oauth2_refresh.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
pub(crate) struct Oauth2RefreshMutation;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/oauth2_workspace_client.graphql",
    schema_path = "schema.graphql",
    response_derives = "Debug"
)]
// Used by the workspace app authorization flow, exposed through the python module
#[allow(dead_code)]
pub(crate) struct Oauth2WorkspaceClientQuery;

pub(crate) fn sub_redirect_uri(pubkey: &str) -> Result<Url> {
    Ok(Url::parse(&format!(
        "https://aqora.io/oauth2/sub/{pubkey}"
    ))?)
}

pub(crate) fn authorize_url(
    base: &Url,
    client_id: &str,
    redirect_uri: &Url,
    state: &str,
    scope: Option<&str>,
) -> Result<Url> {
    let mut url = base.clone();
    {
        let mut pairs = url.query_pairs_mut();
        pairs
            .append_pair("client_id", client_id)
            .append_pair("state", state)
            .append_pair("redirect_uri", redirect_uri.as_ref());
        if let Some(scope) = scope {
            pairs.append_pair("scope", scope);
        }
        pairs.finish();
    }
    Ok(url)
}

pub(crate) struct AuthorizationRequest {
    pub client_id: String,
    pub authorize_url: Url,
    pub redirect_uri: Url,
    pub signature: String,
}

pub(crate) fn new_authorization_request(
    authorize_base: &Url,
    client_id: &str,
    scope: Option<&str>,
) -> Result<AuthorizationRequest> {
    let rng = ring::rand::SystemRandom::new();
    let keypair = ring::signature::Ed25519KeyPair::from_seed_unchecked(
        &ring::rand::generate::<[u8; 32]>(&rng).unwrap().expose(),
    )
    .unwrap();
    let state_bytes = ring::rand::generate::<[u8; 16]>(&rng).unwrap().expose();

    let state = BASE64_URL_SAFE_NO_PAD.encode(state_bytes);
    let public_key = BASE64_URL_SAFE_NO_PAD.encode(keypair.public_key().as_ref());

    let redirect_uri = sub_redirect_uri(&public_key)?;
    let authorize_url = authorize_url(authorize_base, client_id, &redirect_uri, &state, scope)?;

    let signature_bytes = keypair.sign(authorize_url.as_str().as_bytes());
    let signature = BASE64_URL_SAFE_NO_PAD.encode(signature_bytes.as_ref());

    Ok(AuthorizationRequest {
        client_id: client_id.to_string(),
        authorize_url,
        redirect_uri,
        signature,
    })
}

pub(crate) async fn subscribe_code(
    client: &aqora_client::Client,
    req: &AuthorizationRequest,
) -> Result<BoxStream<'static, Result<String>>> {
    Ok(client
        .subscribe::<Oauth2RedirectSubscription>(oauth2_redirect_subscription::Variables {
            auth_url: req.authorize_url.clone(),
            signature: req.signature.clone(),
        })
        .await?
        .map(|item| Ok(item?.oauth2_redirect.code))
        .boxed())
}

#[derive(Debug, Clone)]
pub(crate) struct IssuedTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: DateTime<Utc>,
}

impl IssuedTokens {
    fn new(access_token: String, refresh_token: String, expires_in: i64) -> Self {
        Self {
            access_token,
            refresh_token,
            expires_at: Utc::now() + Duration::try_seconds(expires_in).unwrap(),
        }
    }

    fn is_expired(&self) -> bool {
        (self.expires_at - Duration::try_seconds(EXPIRATION_PADDING_SEC).unwrap()) <= Utc::now()
    }
}

fn check_issued(
    unauthorized: bool,
    client_error: bool,
    issued: Option<IssuedTokens>,
) -> Result<IssuedTokens> {
    if let Some(issued) = issued {
        Ok(issued)
    } else if unauthorized || client_error {
        Err(error::user(
            "Server denied this authentication request",
            "Please try again later",
        ))
    } else {
        Err(error::system(
            "GraphQL response missing issued",
            "This is a bug, please report it",
        ))
    }
}

pub(crate) async fn exchange_authorization_code(
    client: &aqora_client::Client,
    client_id: &str,
    redirect_uri: &Url,
    code: &str,
) -> Result<IssuedTokens> {
    let token = client
        .send::<Oauth2TokenMutation>(oauth2_token_mutation::Variables {
            client_id: client_id.to_string(),
            code: code.to_string(),
            redirect_uri: redirect_uri.clone(),
        })
        .await?
        .oauth2_token;
    check_issued(
        token.unauthorized,
        token.client_error,
        token.issued.map(|issued| {
            IssuedTokens::new(issued.access_token, issued.refresh_token, issued.expires_in)
        }),
    )
}

pub(crate) async fn exchange_code(
    client: &aqora_client::Client,
    req: &AuthorizationRequest,
    code: &str,
) -> Result<IssuedTokens> {
    exchange_authorization_code(client, &req.client_id, &req.redirect_uri, code).await
}

pub(crate) async fn refresh_tokens(
    client: &aqora_client::Client,
    client_id: &str,
    refresh_token: &str,
) -> Result<IssuedTokens> {
    let refresh = client
        .send::<Oauth2RefreshMutation>(oauth2_refresh_mutation::Variables {
            client_id: client_id.to_string(),
            client_secret: None,
            refresh_token: refresh_token.to_string(),
        })
        .await?
        .oauth2_refresh;
    check_issued(
        refresh.unauthorized,
        refresh.client_error,
        refresh.issued.map(|issued| {
            IssuedTokens::new(issued.access_token, issued.refresh_token, issued.expires_in)
        }),
    )
}

/// Viewer tokens obtained through a workspace runner: refreshes through
/// `base` (which sends no Authorization header, so the runner sidecar
/// authenticates the client) 60 s before expiry.
#[allow(dead_code)]
pub(crate) struct ViewerCredentials {
    base: aqora_client::Client,
    client_id: String,
    tokens: RwLock<IssuedTokens>,
}

#[allow(dead_code)]
impl ViewerCredentials {
    pub(crate) fn new(base: aqora_client::Client, client_id: String, tokens: IssuedTokens) -> Self {
        Self {
            base,
            client_id,
            tokens: RwLock::new(tokens),
        }
    }
}

#[async_trait]
impl CredentialsProvider for ViewerCredentials {
    async fn bearer_token(&self) -> Result<Option<String>, BoxError> {
        {
            let tokens = self.tokens.read().await;
            if !tokens.is_expired() {
                return Ok(Some(tokens.access_token.clone()));
            }
        }
        let mut tokens = self.tokens.write().await;
        if tokens.is_expired() {
            let refresh_token = tokens.refresh_token.clone();
            *tokens = refresh_tokens(&self.base, &self.client_id, &refresh_token).await?;
        }
        Ok(Some(tokens.access_token.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aqora_client::http::{Body, HttpBoxService, Request, Response};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    fn authorize_base() -> Url {
        "https://aqora.io/oauth2/authorize".parse().unwrap()
    }

    #[test]
    fn authorize_url_appends_query_pairs() {
        let redirect_uri = sub_redirect_uri("abc").unwrap();
        let url = authorize_url(&authorize_base(), "client", &redirect_uri, "state", None).unwrap();
        assert_eq!(url.path(), "/oauth2/authorize");
        assert_eq!(
            url.query_pairs().collect::<Vec<_>>(),
            vec![
                ("client_id".into(), "client".into()),
                ("state".into(), "state".into()),
                (
                    "redirect_uri".into(),
                    "https://aqora.io/oauth2/sub/abc".into()
                ),
            ]
        );
    }

    #[test]
    fn authorize_url_appends_scope_when_given() {
        let redirect_uri = sub_redirect_uri("abc").unwrap();
        let url = authorize_url(
            &authorize_base(),
            "client",
            &redirect_uri,
            "state",
            Some("viewer"),
        )
        .unwrap();
        assert_eq!(
            url.query_pairs().collect::<Vec<_>>(),
            vec![
                ("client_id".into(), "client".into()),
                ("state".into(), "state".into()),
                (
                    "redirect_uri".into(),
                    "https://aqora.io/oauth2/sub/abc".into()
                ),
                ("scope".into(), "viewer".into()),
            ]
        );
    }

    #[test]
    fn sub_redirect_uri_uses_the_pubkey() {
        assert_eq!(
            sub_redirect_uri("abc").unwrap().as_str(),
            "https://aqora.io/oauth2/sub/abc"
        );
    }

    #[test]
    fn new_authorization_request_signs_the_authorize_url() {
        let req =
            new_authorization_request(&authorize_base(), "workspace-1", Some("viewer")).unwrap();

        assert_eq!(req.client_id, "workspace-1");
        assert!(req
            .authorize_url
            .query_pairs()
            .any(|(key, value)| key == "scope" && value == "viewer"));
        assert!(req
            .authorize_url
            .query_pairs()
            .any(|(key, value)| key == "redirect_uri" && value == req.redirect_uri.as_str()));

        let public_key = req
            .redirect_uri
            .path()
            .strip_prefix("/oauth2/sub/")
            .expect("redirect uri embeds the pubkey");
        let public_key = BASE64_URL_SAFE_NO_PAD.decode(public_key).unwrap();
        let signature = BASE64_URL_SAFE_NO_PAD.decode(&req.signature).unwrap();
        ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, public_key)
            .verify(req.authorize_url.as_str().as_bytes(), &signature)
            .expect("signature verifies");
    }

    fn stub_client(body: &'static str, calls: Arc<AtomicUsize>) -> aqora_client::Client {
        let mut client = aqora_client::Client::new("https://aqora.io/graphql".parse().unwrap());
        client.graphql_layer(tower::layer::layer_fn(move |_: HttpBoxService| {
            let calls = calls.clone();
            tower::service_fn(move |_: Request| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    Ok::<_, std::convert::Infallible>(Response::new(Body::from(body.to_string())))
                }
            })
        }));
        client
    }

    const REFRESHED: &str = r#"{"data":{"oauth2Refresh":{"clientError":false,"unauthorized":false,"issued":{"expiresIn":3600,"accessToken":"refreshed-access","refreshToken":"refreshed-refresh"}}}}"#;

    #[tokio::test]
    async fn viewer_credentials_returns_the_current_token() {
        let calls = Arc::new(AtomicUsize::new(0));
        let credentials = ViewerCredentials::new(
            stub_client(REFRESHED, calls.clone()),
            "workspace-1".to_string(),
            IssuedTokens {
                access_token: "access".to_string(),
                refresh_token: "refresh".to_string(),
                expires_at: Utc::now() + Duration::try_hours(1).unwrap(),
            },
        );

        assert_eq!(
            credentials.bearer_token().await.unwrap(),
            Some("access".to_string())
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn viewer_credentials_refreshes_expired_tokens() {
        let calls = Arc::new(AtomicUsize::new(0));
        let credentials = ViewerCredentials::new(
            stub_client(REFRESHED, calls.clone()),
            "workspace-1".to_string(),
            IssuedTokens {
                access_token: "access".to_string(),
                refresh_token: "refresh".to_string(),
                expires_at: Utc::now() - Duration::try_hours(1).unwrap(),
            },
        );

        assert_eq!(
            credentials.bearer_token().await.unwrap(),
            Some("refreshed-access".to_string())
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            credentials.tokens.read().await.refresh_token,
            "refreshed-refresh"
        );

        assert_eq!(
            credentials.bearer_token().await.unwrap(),
            Some("refreshed-access".to_string())
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
