use mqtt_kit::{MqttClient, QoS, TlsOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = MqttClient::builder("mqtt-kit-tls-example")
        .broker("mqtts://broker.example.com:8883")
        .tls(TlsOptions::new())
        .connect()
        .await?;

    client
        .publish("mqtt-kit/example/tls", "secure", QoS::AtLeastOnce)
        .await?;
    client.disconnect().await?;
    Ok(())
}
