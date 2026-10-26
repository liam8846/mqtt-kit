use mqtt_kit::{MqttClient, ProtocolVersion, TlsOptions};
use std::time::Duration;
use tokio::io::AsyncReadExt;

#[test]
fn debug_exposes_credentials_and_private_key() {
    let tls = TlsOptions::new().client_auth_pem(b"fake-cert".to_vec(), b"audit-private-key-marker".to_vec());
    let rendered = format!("{tls:?}");
    assert!(rendered.contains("97, 117, 100, 105, 116"));
    let builder = MqttClient::builder("audit").credentials("audit-user", "audit-password-marker").tls(tls);
    assert!(format!("{builder:?}").contains("audit-password-marker"));
}

#[tokio::test]
async fn tls_options_are_ignored_and_password_sent_as_plaintext() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = MqttClient::builder("audit")
        .broker(format!("mqtt://{address}"))
        .credentials("audit-user", "audit-password-marker")
        .tls(TlsOptions::new().ca_pem(b"invalid CA".to_vec()))
        .connect().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert_eq!(stream.read_u8().await.unwrap(), 0x10);
        let mut remaining = 0usize;
        let mut multiplier = 1usize;
        loop {
            let byte = stream.read_u8().await.unwrap();
            remaining += usize::from(byte & 127) * multiplier;
            if byte & 128 == 0 { break; }
            multiplier *= 128;
        }
        let mut packet = vec![0; remaining];
        stream.read_exact(&mut packet).await.unwrap();
        assert!(packet.windows(b"audit-password-marker".len()).any(|w| w == b"audit-password-marker"));
    }).await.unwrap();
    drop(client);
}

#[tokio::test]
#[should_panic(expected = "Keep alives should be >= 5 secs")]
async fn v5_small_keepalive_panics_instead_of_returning_error() {
    let _ = MqttClient::builder("audit").broker("mqtt://127.0.0.1:1")
        .protocol(ProtocolVersion::V5).keep_alive(Duration::from_secs(1)).connect().await;
}

#[tokio::test]
#[should_panic(expected = "Keep alives should be specified in seconds")]
async fn v3_subsecond_keepalive_panics_instead_of_returning_error() {
    let _ = MqttClient::builder("audit").broker("mqtt://127.0.0.1:1")
        .keep_alive(Duration::from_millis(500)).connect().await;
}
