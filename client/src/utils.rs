use std::collections::BTreeSet;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use url::{Host, Url};

fn url_addrs(url: &Url) -> io::Result<BTreeSet<SocketAddr>> {
    let Some(host) = url.host() else {
        return Err(io::Error::other("No host found"));
    };
    let Some(port) = url.port_or_known_default() else {
        return Err(io::Error::other("No port found"));
    };
    Ok(match host {
        Host::Ipv4(ip) => vec![SocketAddr::from((ip, port))].into_iter(),
        Host::Ipv6(ip) => vec![SocketAddr::from((ip, port))].into_iter(),
        #[cfg(any(unix, windows, target_os = "wasi"))]
        Host::Domain(domain) => std::net::ToSocketAddrs::to_socket_addrs(&(domain, port))?,
        #[cfg(not(any(unix, windows, target_os = "wasi")))]
        Host::Domain("localhost") => {
            // assume localhost is 127.0.0.1 which may not alwasy be true
            // but is probably okay in the browser
            vec![SocketAddr::from((Ipv4Addr::LOCALHOST, port))].into_iter()
        }
        #[cfg(not(any(unix, windows, target_os = "wasi")))]
        Host::Domain(_) => vec![].into_iter(),
    }
    .map(|addr| {
        if addr.ip().is_loopback() || addr.ip().is_unspecified() {
            SocketAddr::from((Ipv4Addr::LOCALHOST, port))
        } else {
            SocketAddr::from((addr.ip().to_canonical(), port))
        }
    })
    .collect())
}

pub fn host_matches(left: &Url, right: &Url) -> io::Result<bool> {
    if let (Some(Host::Domain(left_host)), Some(Host::Domain(right_host))) =
        (left.host(), right.host())
    {
        return Ok(left_host
            .rsplit('.')
            .zip(right_host.rsplit('.'))
            .all(|(a, b)| a == b));
    }
    let left_addrs = url_addrs(left)?;
    let right_addrs = url_addrs(right)?;
    if left_addrs.is_empty() || right_addrs.is_empty() {
        return Ok(false);
    }
    if left_addrs.len() > right_addrs.len() {
        Ok(left_addrs.is_superset(&right_addrs))
    } else {
        Ok(left_addrs.is_subset(&right_addrs))
    }
}

pub fn is_url_secure(url: &Url) -> io::Result<bool> {
    if url.scheme() == "https" {
        return Ok(true);
    }
    let addrs = url_addrs(url)?;
    Ok(!addrs.is_empty() && addrs.iter().all(|ip| ip.ip().is_loopback()))
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_not {
        ($expr:expr) => {
            assert!(!$expr);
        };
    }

    #[test]
    fn test_host_matches_domains() {
        let left = Url::parse("https://cdn.aqora.io").expect("valid url");
        let right = Url::parse("https://aqora.io").expect("valid url");
        assert!(host_matches(&left, &right).expect("matches domain suffix"));
        assert!(host_matches(&right, &left).expect("matches domain suffix"));

        let mismatch_left = Url::parse("https://example.com").expect("valid url");
        let mismatch_right = Url::parse("https://aqora.io").expect("valid url");
        assert_not!(host_matches(&mismatch_left, &mismatch_right).expect("domain mismatch"));
    }

    #[test]
    fn test_host_matches_ips() {
        let left = Url::parse("http://127.0.0.1:8080").expect("valid url");
        let right = Url::parse("http://127.0.0.1:8080").expect("valid url");
        assert!(host_matches(&left, &right).expect("identical loopback ip"));

        let mismatch_right = Url::parse("http://192.168.10.10:8080").expect("valid url");
        assert_not!(host_matches(&left, &mismatch_right).expect("different host ip"));
    }

    #[test]
    fn test_host_matches_loopback_variants() {
        let ip_local = Url::parse("http://127.0.0.1:8080").expect("valid url");
        let domain_local = Url::parse("http://localhost:8080").expect("valid url");
        let unspecified = Url::parse("http://0.0.0.0:8080").expect("valid url");
        let private = Url::parse("http://192.168.0.1:8080").expect("valid url");

        assert!(host_matches(&ip_local, &domain_local).expect("loopback ip matches localhost"));
        assert!(host_matches(&domain_local, &ip_local).expect("localhost resolves to loopback"));
        assert!(host_matches(&unspecified, &ip_local).expect("unspecified treated as loopback"));
        assert!(host_matches(&unspecified, &domain_local).expect("unspecified matches localhost"));
        assert_not!(host_matches(&ip_local, &private).expect("loopback does not match private"));
        assert_not!(
            host_matches(&domain_local, &private).expect("localhost does not match private")
        );
    }

    #[test]
    fn test_host_matches_ipv6_variants() {
        let ipv6_loopback = Url::parse("http://[::1]:8080").expect("valid url");
        let ipv4_loopback = Url::parse("http://127.0.0.1:8080").expect("valid url");
        let ipv6_unspecified = Url::parse("http://[::]:8080").expect("valid url");
        let ipv6_mapped_loopback = Url::parse("http://[::ffff:7f00:1]:8080").expect("valid url");
        let ipv6_mapped_public = Url::parse("http://[::ffff:c000:201]:8080").expect("valid url");
        let ipv4_public = Url::parse("http://192.0.2.1:8080").expect("valid url");
        let ipv6_public = Url::parse("http://[2001:db8::1]:8080").expect("valid url");

        assert!(host_matches(&ipv6_loopback, &ipv4_loopback)
            .expect("loopback matches across address families"));
        assert!(host_matches(&ipv4_loopback, &ipv6_loopback)
            .expect("loopback matches across address families symmetrically"));
        assert!(host_matches(&ipv6_unspecified, &ipv4_loopback)
            .expect("unspecified IPv6 treated as loopback"));
        assert!(host_matches(&ipv6_mapped_loopback, &ipv4_loopback)
            .expect("IPv4-mapped loopback canonicalizes"));
        assert!(host_matches(&ipv6_mapped_public, &ipv4_public)
            .expect("IPv4-mapped address matches canonical IPv4"));
        assert_not!(host_matches(&ipv6_public, &ipv4_loopback)
            .expect("non-loopback IPv6 should not match loopback"));
        assert_not!(host_matches(&ipv6_public, &ipv4_public)
            .expect("different public addresses do not match"));
    }

    #[test]
    fn test_is_url_secure_http_unspecified() {
        let unspecified = Url::parse("http://0.0.0.0:8080").expect("valid url");
        assert!(is_url_secure(&unspecified).expect("unspecified http treated as loopback"));
    }

    #[test]
    fn test_is_url_secure_https() {
        let secure = Url::parse("https://example.com").expect("valid url");
        assert!(is_url_secure(&secure).expect("https urls are secure"));
    }

    #[test]
    fn test_is_url_secure_http_loopback() {
        let loopback = Url::parse("http://127.0.0.1:8080").expect("valid url");
        assert!(is_url_secure(&loopback).expect("loopback http allowed"));
    }

    #[test]
    fn test_is_url_secure_http_public() {
        let public = Url::parse("http://93.184.216.34").expect("valid url");
        assert_not!(is_url_secure(&public).expect("public http disallowed"));
        let public_domain = Url::parse("http://example.com").expect("valid url");
        assert_not!(is_url_secure(&public_domain).expect("public http disallowed"));
    }
}
