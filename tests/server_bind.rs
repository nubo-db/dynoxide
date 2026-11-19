//! `server::bind`: an application that embeds the HTTP server binds its
//! listener with the same checks `server::start` makes, then serves it with
//! `serve_with_shutdown`.

#![cfg(feature = "http-server")]

use dynoxide::Database;

/// A listener on the wildcard address takes connections to the loopback one
/// too, so `bind` refuses the port, as `start` does.
#[tokio::test]
async fn bind_refuses_a_port_a_wildcard_listener_holds() {
    let holder = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
    let port = holder.local_addr().unwrap().port();
    let err = dynoxide::server::bind("127.0.0.1", port)
        .await
        .expect_err("bind took a port another listener holds");
    assert!(
        err.starts_with(&format!("port {port} is already in use")),
        "{err}"
    );
}

#[tokio::test]
async fn a_bound_listener_serves_until_shutdown() {
    let listener = dynoxide::server::bind("127.0.0.1", 0).await.unwrap();
    let addr = listener.local_addr().unwrap();
    assert_ne!(addr.port(), 0, "bind should report the port it got");

    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(dynoxide::server::serve_with_shutdown(
        listener,
        Database::memory().unwrap(),
        async {
            let _ = stopped.await;
        },
    ));

    let response = reqwest::Client::new()
        .post(format!("http://{addr}"))
        .header("X-Amz-Target", "DynamoDB_20120810.ListTables")
        .header("Content-Type", "application/x-amz-json-1.0")
        .header("X-Amz-Date", "20260101T000000Z")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=fakekey/20260101/us-east-1/dynamodb/aws4_request, SignedHeaders=host;x-amz-date;x-amz-target, Signature=fakesig",
        )
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
