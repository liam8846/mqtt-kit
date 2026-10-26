use mqtt_kit::{MqttClient, ProtocolVersion, PublishOptions, QoS};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = MqttClient::builder("mqtt-kit-v5-example")
        .broker("mqtt://localhost:1883")
        .protocol(ProtocolVersion::V5)
        .connect()
        .await?;
    let mut subscription = client.subscribe("mqtt-kit/v5", QoS::AtLeastOnce).await?;

    client
        .publish_with(
            PublishOptions::new("mqtt-kit/v5", "payload", QoS::AtLeastOnce)
                .content_type("text/plain")
                .user_property("source", "example"),
        )
        .await?;

    if let Some(message) = subscription.recv().await {
        println!("v5 properties: {:?}", message.v5);
    }
    client.disconnect().await?;
    Ok(())
}
