use std::time::Duration;

use mqtt_kit::{MqttClient, QoS};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = MqttClient::builder("mqtt-kit-basic-example")
        .broker("mqtt://localhost:1883")
        .keep_alive(Duration::from_secs(30))
        .connect()
        .await?;

    let mut subscription = client
        .subscribe("mqtt-kit/example/+", QoS::AtLeastOnce)
        .await?;
    client
        .publish("mqtt-kit/example/hello", "你好，MQTT", QoS::AtLeastOnce)
        .await?;

    if let Some(message) = subscription.recv().await {
        println!("{}: {:?}", message.topic, message.payload);
    }
    client.disconnect().await?;
    Ok(())
}
