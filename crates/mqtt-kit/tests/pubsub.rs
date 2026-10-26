mod common;

use std::time::Duration;

use bytes::Bytes;
use mqtt_kit::{MqttClient, ProtocolVersion, PublishOptions, QoS};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_wildcard_pubsub_roundtrip() {
    let address = common::start_broker(ProtocolVersion::V311).await;
    let client = MqttClient::builder(format!("v3-{}", address.port()))
        .broker(common::broker_url(address))
        .connect()
        .await
        .expect("create client");
    common::wait_connected(&client).await;

    let mut subscription = client
        .subscribe("integration/+/temperature", QoS::AtLeastOnce)
        .await
        .expect("subscribe");
    let payload = Bytes::from_static(b"24.5");
    client
        .publish(
            "integration/kitchen/temperature",
            payload.clone(),
            QoS::AtLeastOnce,
        )
        .await
        .expect("publish");

    let message = tokio::time::timeout(Duration::from_secs(5), subscription.recv())
        .await
        .expect("message timeout")
        .expect("message exists");
    assert_eq!(message.topic, "integration/kitchen/temperature");
    assert_eq!(message.payload, payload);
    assert_eq!(message.qos, QoS::AtLeastOnce);
    assert!(message.v5.is_none());
    client.disconnect().await.expect("disconnect");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v5_properties_pubsub_roundtrip() {
    let address = common::start_broker(ProtocolVersion::V5).await;
    let client = MqttClient::builder(format!("v5-{}", address.port()))
        .broker(common::broker_url(address))
        .protocol(ProtocolVersion::V5)
        .connect()
        .await
        .expect("create client");
    common::wait_connected(&client).await;

    let mut subscription = client
        .subscribe("integration/v5", QoS::AtLeastOnce)
        .await
        .expect("subscribe");
    client
        .publish_with(
            PublishOptions::new("integration/v5", "hello-v5", QoS::AtLeastOnce)
                .content_type("text/plain")
                .user_property("trace-id", "abc-123"),
        )
        .await
        .expect("publish");

    let message = tokio::time::timeout(Duration::from_secs(5), subscription.recv())
        .await
        .expect("message timeout")
        .expect("message exists");
    let properties = message.v5.expect("v5 properties");
    assert_eq!(properties.content_type.as_deref(), Some("text/plain"));
    assert_eq!(
        properties.user_properties,
        [("trace-id".to_owned(), "abc-123".to_owned())]
    );
    client.disconnect().await.expect("disconnect");
}
