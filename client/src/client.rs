use std::fmt;

use graphql_client::GraphQLQuery;
use tower::{Layer, Service, ServiceExt};
use url::Url;

use crate::async_util::{MaybeSend, MaybeSync};
use crate::error::{Error, MiddlewareError, Result};
use crate::http::{check_status, HttpArcLayer, HttpBoxService, HttpClient, Request, Response};

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

fn graphql_request<Q: GraphQLQuery>(url: Url, variables: Q::Variables) -> Result<Request> {
    Ok(http::Request::builder()
        .method(http::Method::POST)
        .uri(url.to_string())
        .body(serde_json::to_string(&Q::build_query(variables))?.into())?)
}

#[derive(Clone)]
pub struct Client {
    inner: reqwest::Client,
    graphql_url: Url,
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
        Client {
            inner: reqwest::Client::new(),
            graphql_url,
            graphql_layer: HttpArcLayer::default(),
            #[cfg(feature = "s3")]
            s3_layer: HttpArcLayer::default(),
            #[cfg(feature = "ws")]
            ws_layer: HttpArcLayer::default(),
        }
    }

    #[inline]
    pub fn inner(&self) -> &reqwest::Client {
        &self.inner
    }

    #[inline]
    pub fn url(&self) -> &Url {
        &self.graphql_url
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

    pub async fn send<Q: GraphQLQuery>(&self, variables: Q::Variables) -> Result<Q::ResponseData> {
        let res = self
            .graphql_service()
            .oneshot(graphql_request::<Q>(self.graphql_url.clone(), variables)?)
            .await?;
        check_status(&res.status())?;
        get_data::<Q>(res.into_body().json().await?)
    }
}
