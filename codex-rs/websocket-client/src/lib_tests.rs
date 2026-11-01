use std::time::Duration;

use codex_http_client::HttpClientFactory;
use codex_http_client::OutboundProxyPolicy;
use pretty_assertions::assert_eq;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::server::Request;
use tokio_tungstenite::tungstenite::handshake::server::Response;
use tokio_tungstenite::tungstenite::http::HeaderMap;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::Uri;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::header::COOKIE;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use super::WebSocketConnector;
use super::WebSocketTlsMode;
use super::is_loopback_destination;

#[tokio::test]
async fn configured_headers_override_websocket_handshakes_only_for_the_matching_origin() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut received = Vec::new();
        for _ in 0..3 {
            let (stream, _) = listener.accept().await.unwrap();
            let connection = accept_hdr_async(stream, |request: &Request, response: Response| {
                received.push(request.headers().clone());
                Ok(response)
            })
            .await
            .unwrap();
            drop(connection);
        }
        received
    });
    for base_url in [
        format!("http://{address}/backend-api"),
        format!("https://{address}/backend-api"),
        "http://other.example/backend-api".to_string(),
    ] {
        let factory = HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault)
            .with_configured_headers(
                &base_url,
                HeaderMap::from_iter([
                    (
                        AUTHORIZATION,
                        HeaderValue::from_static("Bearer configured-test-token"),
                    ),
                    (COOKIE, HeaderValue::from_static("configured-cookie=1")),
                    (
                        "x-backend-token".parse().unwrap(),
                        HeaderValue::from_static("configured-header"),
                    ),
                ]),
            );
        let connector =
            WebSocketConnector::new_with_tls_mode(&factory, WebSocketTlsMode::TungsteniteDefault)
                .unwrap();
        let mut request = format!("ws://{address}/other-path")
            .into_client_request()
            .unwrap();
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer request-test-token"),
        );
        request
            .headers_mut()
            .insert(COOKIE, HeaderValue::from_static("request-cookie=1"));
        request.headers_mut().append(
            "x-backend-token",
            HeaderValue::from_static("request-header-one"),
        );
        request.headers_mut().append(
            "x-backend-token",
            HeaderValue::from_static("request-header-two"),
        );
        let (connection, _) = tokio::time::timeout(
            Duration::from_secs(/*secs*/ 5),
            connector.connect_loopback_direct(request, WebSocketConfig::default()),
        )
        .await
        .expect("handshake should not time out")
        .expect("handshake should succeed");
        drop(connection);
    }
    let requests = tokio::time::timeout(Duration::from_secs(/*secs*/ 5), server)
        .await
        .expect("server should not time out")
        .expect("server should finish");
    for (index, headers) in requests.iter().enumerate() {
        let matches = index == 0;
        assert_eq!(
            headers[AUTHORIZATION],
            if matches {
                "Bearer configured-test-token"
            } else {
                "Bearer request-test-token"
            },
        );
        assert_eq!(
            headers[COOKIE],
            if matches {
                "configured-cookie=1"
            } else {
                "request-cookie=1"
            },
        );
        assert_eq!(
            headers
                .get_all("x-backend-token")
                .iter()
                .map(|value| value.to_str().unwrap())
                .collect::<Vec<_>>(),
            if matches {
                vec!["configured-header"]
            } else {
                vec!["request-header-one", "request-header-two"]
            },
        );
    }
}

#[test]
fn recognizes_loopback_websocket_destinations() {
    for destination in [
        "ws://localhost:8080",
        "ws://LOCALHOST:8080",
        "ws://127.0.0.1:8080",
        "ws://[::1]:8080",
    ] {
        let uri = destination.parse::<Uri>().unwrap();
        assert!(
            is_loopback_destination(&uri),
            "expected loopback destination: {destination}; parsed host: {:?}",
            uri.host()
        );
    }
}

#[test]
fn rejects_non_loopback_websocket_destinations() {
    for destination in [
        "ws://relay.example:8080",
        "ws://192.0.2.1:8080",
        "ws://[2001:db8::1]:8080",
        "/missing-host",
    ] {
        assert!(!is_loopback_destination(
            &destination.parse::<Uri>().unwrap()
        ));
    }
}

#[path = "cookie_tests.rs"]
mod cookie_tests;
#[path = "network_policy_tests.rs"]
mod network_policy_tests;
