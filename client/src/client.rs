use std::fmt;
use std::sync::Arc;

use graphql_client::GraphQLQuery;
use url::Url;

use crate::error::{Error, Result};
use crate::middleware::{Middleware, Next};

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

fn graphql_request<Q: GraphQLQuery>(url: Url, variables: Q::Variables) -> Result<reqwest::Request> {
    let mut request = reqwest::Request::new(reqwest::Method::POST, url);
    request
        .body_mut()
        .replace(serde_json::to_string(&Q::build_query(variables))?.into());
    Ok(request)
}

#[derive(Clone)]
pub struct Client {
    inner: reqwest::Client,
    graphql_url: Url,
    graphql_middleware: Vec<Arc<dyn Middleware>>,
    #[cfg(feature = "s3")]
    pub(crate) s3_middleware: Vec<Arc<dyn Middleware>>,
    #[cfg(feature = "ws")]
    pub(crate) ws_middleware: Vec<Arc<dyn crate::middleware::WsMiddleware>>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dbg = f.debug_struct("Client");
        dbg.field("client", &self.inner)
            .field("graphql_url", &self.graphql_url)
            .field("graphql_middleware", &self.graphql_middleware.len());
        #[cfg(feature = "s3")]
        dbg.field("s3_middleware", &self.s3_middleware.len());
        #[cfg(feature = "ws")]
        dbg.field("ws_middleware", &self.ws_middleware.len());
        dbg.finish()
    }
}

impl Client {
    #[inline]
    pub fn new(graphql_url: Url) -> Self {
        Client {
            inner: reqwest::Client::new(),
            graphql_url,
            graphql_middleware: Vec::new(),
            #[cfg(feature = "s3")]
            s3_middleware: Vec::new(),
            #[cfg(feature = "ws")]
            ws_middleware: Vec::new(),
        }
    }

    #[inline]
    pub fn with(&mut self, middleware: impl Middleware + 'static) -> &mut Self {
        self.graphql_middleware.push(Arc::new(middleware));
        self
    }

    #[inline]
    pub fn with_arc(&mut self, middleware: Arc<dyn Middleware>) -> &mut Self {
        self.graphql_middleware.push(middleware);
        self
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
    pub fn middleware(&self) -> &[Arc<dyn Middleware>] {
        &self.graphql_middleware
    }

    #[inline]
    pub fn middleware_mut(&mut self) -> &mut [Arc<dyn Middleware>] {
        &mut self.graphql_middleware
    }

    #[inline]
    fn graphql_next(&self) -> Next {
        Next::new(&self.inner, &self.graphql_middleware)
    }

    pub async fn send<Q: GraphQLQuery>(&self, variables: Q::Variables) -> Result<Q::ResponseData> {
        let request = graphql_request::<Q>(self.graphql_url.clone(), variables)?;
        let res = self
            .graphql_next()
            .handle(request)
            .await?
            .error_for_status()?
            .json()
            .await?;
        get_data::<Q>(res)
    }
}
