use std::fmt;
use std::net::{IpAddr, Ipv4Addr};

use bytes::Bytes;
use graphql_client::GraphQLQuery;
use tower::{Layer, Service, ServiceExt};
use url::{Host, Url};

use crate::async_util::{MaybeSend, MaybeSync};
use crate::error::{Error, MiddlewareError, Result};
use crate::http::{
    check_status, Body, HttpArcLayer, HttpBoxService, HttpClient, Request, Response,
};

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

    pub(crate) fn validate_url_host(&self, url: &Url) -> Result<()> {
        if allow_request_url(url.scheme(), url.host(), self.graphql_url.host()) {
            Ok(())
        } else {
            Err(Error::BadOrigin)
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

    pub async fn send_raw(&self, body: impl Into<Body>) -> Result<Bytes> {
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
        res.into_body().bytes().await
    }
}

pub fn allow_request_url<T: AsRef<str>>(
    scheme: &str,
    host: Option<Host<T>>,
    expected_host: Option<Host<&str>>,
) -> bool {
    let Some((host, expected)) = host.zip(expected_host) else {
        return false;
    };
    match (host, expected) {
        (Host::Domain(actual), Host::Domain(expected)) => {
            if scheme != "https" {
                return actual.as_ref() == "localhost" && expected == "localhost";
            }
            actual
                .as_ref()
                .rsplit('.')
                .zip(expected.rsplit('.'))
                .all(|(a, b)| a == b)
        }
        (actual, expected) => {
            let actual = match actual {
                Host::Ipv4(ip) => IpAddr::V4(ip),
                Host::Ipv6(ip) => IpAddr::V6(ip),
                Host::Domain(domain) if domain.as_ref() == "localhost" => {
                    IpAddr::V4(Ipv4Addr::LOCALHOST)
                }
                _ => return false,
            };
            let expected = match expected {
                Host::Ipv4(ip) => IpAddr::V4(ip),
                Host::Ipv6(ip) => IpAddr::V6(ip),
                Host::Domain("localhost") => IpAddr::V4(Ipv4Addr::LOCALHOST),
                _ => return false,
            };
            if expected.is_loopback() {
                return actual.is_loopback();
            }
            scheme == "https" && actual == expected
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    macro_rules! assert_not {
        ($expr:expr) => {
            assert!(!$expr);
        };
    }

    #[test]
    fn test_allow_request_url() {
        // allowed
        assert!(allow_request_url(
            "https",
            Some(Host::Domain("aqora.io")),
            Some(Host::Domain("aqora.io")),
        ));
        assert!(allow_request_url(
            "https",
            Some(Host::Domain("cdn.aqora.io")),
            Some(Host::Domain("aqora.io")),
        ));
        assert!(allow_request_url(
            "https",
            Some(Host::Domain("cdn.api.aqora.io")),
            Some(Host::Domain("api.aqora.io")),
        ));
        assert!(allow_request_url(
            "http",
            Some(Host::Domain("localhost")),
            Some(Host::Domain("localhost")),
        ));
        assert!(allow_request_url::<String>(
            "http",
            Some(Host::Ipv4(Ipv4Addr::LOCALHOST)),
            Some(Host::Ipv4(Ipv4Addr::LOCALHOST)),
        ));
        assert!(allow_request_url::<String>(
            "http",
            Some(Host::Ipv6(Ipv6Addr::LOCALHOST)),
            Some(Host::Ipv4(Ipv4Addr::LOCALHOST)),
        ));
        assert!(allow_request_url::<String>(
            "http",
            Some(Host::Ipv6(Ipv6Addr::LOCALHOST)),
            Some(Host::Ipv6(Ipv6Addr::LOCALHOST)),
        ));
        assert!(allow_request_url::<String>(
            "http",
            Some(Host::Ipv4(Ipv4Addr::LOCALHOST)),
            Some(Host::Ipv6(Ipv6Addr::LOCALHOST)),
        ));
        assert!(allow_request_url::<String>(
            "https",
            Some(Host::Ipv4(Ipv4Addr::new(190, 54, 23, 233))),
            Some(Host::Ipv4(Ipv4Addr::new(190, 54, 23, 233))),
        ));

        // disallowed
        assert_not!(allow_request_url(
            // do not send api creds to cdn
            "https",
            Some(Host::Domain("cdn.aqora.io")),
            Some(Host::Domain("api.aqora.io")),
        ));
        assert_not!(allow_request_url::<String>(
            // no host found for target
            "https",
            None,
            Some(Host::Domain("aqora.io")),
        ));
        assert_not!(allow_request_url(
            // no host found for target
            "https",
            Some(Host::Domain("aqora.io")),
            None,
        ));
        assert_not!(allow_request_url(
            // wrong host
            "https",
            Some(Host::Domain("example.net")),
            Some(Host::Domain("aqora.io")),
        ));
        assert_not!(allow_request_url(
            // wrong host
            "https",
            Some(Host::Domain("my.sub.example.net")),
            Some(Host::Domain("aqora.io")),
        ));
        assert_not!(allow_request_url::<String>(
            // wrong host
            "https",
            Some(Host::Ipv4(Ipv4Addr::new(190, 54, 23, 233))),
            Some(Host::Domain("aqora.io")),
        ));
        assert_not!(allow_request_url(
            // wrong host
            "https",
            Some(Host::Domain("aqora.io")),
            Some(Host::Ipv4(Ipv4Addr::new(190, 54, 23, 233))),
        ));
        assert_not!(allow_request_url(
            // wrong host
            "https",
            Some(Host::Domain("localhost")),
            Some(Host::Ipv4(Ipv4Addr::new(190, 54, 23, 233))),
        ));
        assert_not!(allow_request_url(
            // no tls
            "http",
            Some(Host::Domain("aqora.io")),
            Some(Host::Domain("aqora.io")),
        ));
        assert_not!(allow_request_url::<String>(
            // no tls ipv4
            "http",
            Some(Host::Ipv4(Ipv4Addr::new(190, 54, 23, 233))),
            Some(Host::Ipv4(Ipv4Addr::new(190, 54, 23, 233))),
        ));
    }
}
