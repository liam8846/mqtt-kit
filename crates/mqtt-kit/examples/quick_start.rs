use mqtt_kit::{ConnectionState, MqttClient, QoS};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = MqttClient::builder("mqtt-kit-demo")
        .broker("mqtt://127.0.0.1:1883")
        .connect()
        .await?;

    // connect() 啟動背景連線；透過 state() 等待 broker 完成握手。
    let mut state = client.state();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match *state.borrow_and_update() {
                ConnectionState::Connected => return Ok::<(), mqtt_kit::MqttError>(()),
                ConnectionState::Closed => return Err(mqtt_kit::MqttError::ClientClosed),
                _ => {}
            }
            state
                .changed()
                .await
                .map_err(|_| mqtt_kit::MqttError::ClientClosed)?;
        }
    })
    .await??;

    let mut messages = client.subscribe("demo/hello", QoS::AtLeastOnce).await?;
    client
        .publish("demo/hello", "Hello, MQTT!", QoS::AtLeastOnce)
        .await?;

    if let Some(message) = tokio::time::timeout(Duration::from_secs(5), messages.recv()).await? {
        println!("{} → {:?}", message.topic, message.payload);
    }

    client.disconnect().await?;
    Ok(())
}
