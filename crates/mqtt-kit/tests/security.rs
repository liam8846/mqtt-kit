use std::time::Duration;

use mqtt_kit::{LastWill, MqttClient, MqttError, ProtocolVersion, QoS, TlsOptions};

#[test]
fn builder_debug_redacts_nested_secrets_and_unvalidated_urls() {
    let tls = TlsOptions::new().client_auth_pem(
        b"audit-certificate-marker".to_vec(),
        b"audit-private-key-marker".to_vec(),
    );
    let builder = MqttClient::builder("audit-client")
        .broker("mqtt://url-user:url-password@localhost?token=audit-token")
        .credentials("audit-user", "audit-password")
        .last_will(LastWill::new(
            "status",
            "audit-will-payload",
            QoS::AtMostOnce,
            false,
        ))
        .tls(tls);
    for output in [format!("{builder:?}"), format!("{builder:#?}")] {
        assert!(output.contains("audit-client"));
        assert!(output.contains("[REDACTED]"));
        for secret in [
            "url-user",
            "url-password",
            "audit-token",
            "audit-user",
            "audit-password",
            "audit-certificate-marker",
            "audit-private-key-marker",
            "audit-will-payload",
        ] {
            assert!(!output.contains(secret));
            assert!(!output.contains(&format!("{:?}", secret.as_bytes())));
        }
    }
}

#[tokio::test]
async fn invalid_keepalives_return_config_errors_without_panicking() {
    for protocol in [ProtocolVersion::V311, ProtocolVersion::V5] {
        for duration in [
            Duration::ZERO,
            Duration::from_millis(500),
            Duration::from_secs(65536),
            Duration::MAX,
        ] {
            let result = MqttClient::builder("audit")
                .broker("mqtt://localhost")
                .protocol(protocol)
                .keep_alive(duration)
                .connect()
                .await;
            assert!(matches!(result, Err(MqttError::InvalidConfig(_))));
        }
    }
    for duration in [Duration::from_secs(1), Duration::from_secs(4)] {
        let result = MqttClient::builder("audit")
            .broker("mqtt://localhost")
            .protocol(ProtocolVersion::V5)
            .keep_alive(duration)
            .connect()
            .await;
        assert!(matches!(result, Err(MqttError::InvalidConfig(_))));
    }
}

#[tokio::test]
async fn valid_keepalive_boundaries_are_accepted() {
    for (protocol, minimum) in [(ProtocolVersion::V311, 1), (ProtocolVersion::V5, 5)] {
        for seconds in [minimum, 65535] {
            let client = MqttClient::builder("audit")
                .broker("mqtt://localhost:1")
                .protocol(protocol)
                .keep_alive(Duration::from_secs(seconds))
                .connect()
                .await
                .unwrap_or_else(|_| panic!("valid keepalive {seconds}"));
            drop(client);
        }
    }
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn tls_options_with_plaintext_broker_fail_before_any_connection() {
    for protocol in [ProtocolVersion::V311, ProtocolVersion::V5] {
        for broker in ["mqtt://localhost", "ws://localhost"] {
            if broker.starts_with("ws:") && !cfg!(feature = "websocket") {
                continue;
            }
            let result = MqttClient::builder("audit")
                .broker(broker)
                .protocol(protocol)
                .credentials("audit-user", "audit-password")
                .tls(TlsOptions::new().ca_pem(b"invalid CA".to_vec()))
                .connect()
                .await;
            assert!(matches!(result, Err(MqttError::InvalidConfig(_))));
        }
    }
}

#[cfg(not(feature = "tls"))]
#[tokio::test]
async fn tls_options_without_feature_are_rejected() {
    for broker in [
        "mqtt://localhost",
        "mqtts://localhost",
        "ws://localhost",
        "wss://localhost",
    ] {
        let result = MqttClient::builder("audit")
            .broker(broker)
            .tls(TlsOptions::new())
            .connect()
            .await;
        assert!(matches!(result, Err(MqttError::UnsupportedFeature(_))));
    }
}

#[cfg(feature = "tls-ring")]
#[tokio::test]
async fn custom_tls_config_with_plaintext_broker_is_rejected() {
    let config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(rustls::RootCertStore::empty())
    .with_no_client_auth();
    let config = std::sync::Arc::new(config);
    for protocol in [ProtocolVersion::V311, ProtocolVersion::V5] {
        for broker in ["mqtt://localhost", "ws://localhost"] {
            if broker.starts_with("ws:") && !cfg!(feature = "websocket") {
                continue;
            }
            let result = MqttClient::builder("audit")
                .broker(broker)
                .protocol(protocol)
                .tls_client_config(std::sync::Arc::clone(&config))
                .connect()
                .await;
            assert!(matches!(result, Err(MqttError::InvalidConfig(_))));
        }
    }
}

#[cfg(any(feature = "tls-ring", feature = "tls-aws-lc"))]
#[tokio::test]
async fn malformed_tls_material_returns_config_errors() {
    let certificate = b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n";
    for options in [
        TlsOptions::new().ca_pem(Vec::new()),
        TlsOptions::new().ca_pem(b"not a PEM certificate".to_vec()),
        TlsOptions::new().ca_pem(certificate.to_vec()),
        TlsOptions::new().client_auth_pem(Vec::new(), Vec::new()),
        TlsOptions::new().client_auth_pem(certificate.to_vec(), b"not a PEM key".to_vec()),
    ] {
        let result = MqttClient::builder("audit")
            .broker("mqtts://localhost")
            .tls(options)
            .connect()
            .await;
        assert!(matches!(result, Err(MqttError::InvalidConfig(_))));
    }
}

#[tokio::test]
async fn zero_resource_budgets_are_config_errors() {
    let base = MqttClient::builder("budgets").broker("mqtt://localhost");
    for builder in [
        base.clone().subscription_byte_capacity(0),
        base.clone().buffer_byte_capacity(0),
        base.max_subscriptions(0),
    ] {
        assert!(matches!(
            builder.connect().await,
            Err(MqttError::InvalidConfig(_))
        ));
    }
}
