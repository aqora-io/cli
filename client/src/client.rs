use std::fmt;
use std::io;

use bytes::Bytes;
use graphql_client::GraphQLQuery;
use tower::{Layer, Service, ServiceExt};
use url::Url;

use crate::async_util::{MaybeSend, MaybeSync};
use crate::error::{Error, MiddlewareError, Result};
use crate::http::{
    check_status, Body, HttpArcLayer, HttpBoxService, HttpClient, Request, Response,
};
use crate::utils::is_url_secure;

pub(crate) fn get_data<Q: GraphQLQuery>(
    response: graphql_client::Response<Q::ResponseData>,
) -> Result<Q::ResponseData> {
    if let Some(data) = response.data {
        Ok(data)
    } else if let Some(errors) = response.errors {
        Err(Error::Response(errors))
    } else {
        Err(Error::NoData)
    }
}

#[derive(Clone, Default)]
pub struct ClientOptions {
    pub allow_insecure_host: bool,
}

#[derive(Clone)]
pub struct Client {
    inner: reqwest::Client,
    graphql_url: Url,
    options: ClientOptions,
    graphql_layer: HttpArcLayer<HttpClient>,
    #[cfg(feature = "s3")]
    pub(crate) s3_layer: HttpArcLayer<HttpClient>,
    #[cfg(feature = "ws")]
    pub(crate) ws_layer: HttpArcLayer<crate::ws::WsClient>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("client", &self.inner)
            .field("graphql_url", &self.graphql_url)
            .finish()
    }
}

impl Client {
    #[inline]
    pub fn new(graphql_url: Url) -> Self {
        Client::new_with_options(graphql_url, Default::default())
    }

    #[inline]
    pub fn new_with_options(graphql_url: Url, options: ClientOptions) -> Self {
        Client {
            inner: reqwest::Client::new(),
            graphql_url,
            options,
            graphql_layer: HttpArcLayer::default(),
            #[cfg(feature = "s3")]
            s3_layer: HttpArcLayer::default(),
            #[cfg(feature = "ws")]
            ws_layer: HttpArcLayer::default(),
        }
    }

    pub(crate) fn validate_host(&self, url: &Url) -> Result<()> {
        if !(self.options.allow_insecure_host || is_url_secure(url).map_err(Error::BadOrigin)?) {
            return Err(Error::BadOrigin(io::Error::other(format!(
                "Url {url} is insecure"
            ))));
        }
        Ok(())
    }

    #[inline]
    pub fn inner(&self) -> &reqwest::Client {
        &self.inner
    }

    #[inline]
    pub fn url(&self) -> &Url {
        &self.graphql_url
    }

    #[inline]
    pub fn url_mut(&mut self) -> &mut Url {
        &mut self.graphql_url
    }

    #[inline]
    pub fn options(&self) -> &ClientOptions {
        &self.options
    }

    #[inline]
    pub fn options_mut(&mut self) -> &mut ClientOptions {
        &mut self.options
    }

    pub fn graphql_layer<L, E>(&mut self, layer: L) -> &mut Self
    where
        L: Layer<HttpBoxService> + MaybeSend + MaybeSync + 'static,
        L::Service: Service<Request, Response = Response, Error = E> + Clone + MaybeSend + 'static,
        <L::Service as Service<Request>>::Future: MaybeSend + 'static,
        MiddlewareError: From<E>,
        E: 'static,
    {
        self.graphql_layer.stack(layer);
        self
    }

    #[inline]
    fn graphql_service(&self) -> HttpBoxService {
        self.graphql_layer
            .layer(HttpClient::new(self.inner.clone()))
    }

    async fn graphql_send(&self, body: impl Into<Body>) -> Result<Body> {
        self.validate_host(self.url())?;
        let res = self
            .graphql_service()
            .oneshot(
                http::Request::builder()
                    .method(http::Method::POST)
                    .uri(self.url().to_string())
                    .body(body.into())?,
            )
            .await?;
        check_status(&res.status())?;
        Ok(res.into_body())
    }

    pub async fn send<Q: GraphQLQuery>(&self, variables: Q::Variables) -> Result<Q::ResponseData> {
        let body = serde_json::to_string(&Q::build_query(variables))?;
        get_data::<Q>(self.graphql_send(body).await?.json().await?)
    }

    pub async fn send_raw(&self, body: impl Into<Body>) -> Result<Bytes> {
        self.graphql_send(body).await?.bytes().await
    }
}
