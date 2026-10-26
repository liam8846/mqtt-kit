mod common;

use mqtt_kit::{MqttClient, MqttError, ProtocolVersion, PublishOptions, QoS, SubscribeOptions};
use std::{io, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

const DEADLINE: Duration = Duration::from_secs(5);

async fn roundtrip(protocol: ProtocolVersion) {
    let address = common::start_broker(protocol).await;
    let client = MqttClient::builder(format!("confirmed-{}", address.port()))
        .broker(common::broker_url(address))
        .protocol(protocol)
        .connect()
        .await
        .unwrap();
    let mut subscription = client
        .subscribe_confirmed(
            SubscribeOptions::new("confirmed/data", QoS::ExactlyOnce).unwrap(),
            DEADLINE,
        )
        .await
        .unwrap();
    for qos in [QoS::AtLeastOnce, QoS::ExactlyOnce] {
        client
            .publish_confirmed(
                PublishOptions::new("confirmed/data", "confirmed", qos),
                DEADLINE,
            )
            .await
            .unwrap();
        let message = tokio::time::timeout(DEADLINE, subscription.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.payload, "confirmed");
    }
    client
        .unsubscribe_confirmed("confirmed/data", DEADLINE)
        .await
        .unwrap();
    assert!(subscription.recv().await.is_none());
    client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mqtt_v3_confirms_subscribe_qos_one_qos_two_and_unsubscribe() {
    roundtrip(ProtocolVersion::V311).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mqtt_v5_confirms_subscribe_qos_one_qos_two_and_unsubscribe() {
    roundtrip(ProtocolVersion::V5).await;
}

#[derive(Clone, Copy)]
enum Fault {
    RejectSubscribe,
    RejectPublish,
    RejectUnsubscribe,
    MixedPublish,
    Silence,
    Disconnect,
}

async fn read_packet(stream: &mut TcpStream) -> io::Result<(u8, Vec<u8>)> {
    let header = stream.read_u8().await?;
    let mut remaining = 0usize;
    let mut multiplier = 1usize;
    for index in 0..4 {
        let byte = stream.read_u8().await?;
        remaining += usize::from(byte & 127) * multiplier;
        if byte & 128 == 0 {
            if remaining > 1024 * 1024 {
                return Err(io::Error::other("oversized test packet"));
            }
            let mut body = vec![0; remaining];
            stream.read_exact(&mut body).await?;
            return Ok((header, body));
        }
        multiplier *= 128;
        if index == 3 {
            return Err(io::Error::other("invalid remaining length"));
        }
    }
    unreachable!()
}

async fn fault_broker(
    protocol: ProtocolVersion,
    fault: Fault,
) -> (String, flume::Receiver<u8>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (observed, requests) = flume::bounded(16);
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let (header, _) = read_packet(&mut stream).await.unwrap();
        assert_eq!(header >> 4, 1);
        let connack: &[u8] = if protocol == ProtocolVersion::V311 {
            &[0x20, 2, 0, 0]
        } else {
            &[0x20, 3, 0, 0, 0]
        };
        stream.write_all(connack).await.unwrap();
        while let Ok((header, body)) = read_packet(&mut stream).await {
            let kind = header >> 4;
            let _ = observed.try_send(kind);
            if kind == 14 {
                break;
            }
            if matches!(fault, Fault::Disconnect) {
                break;
            }
            if matches!(fault, Fault::Silence) {
                continue;
            }
            let id = match kind {
                8 | 10 => [body[0], body[1]],
                3 => {
                    let topic_len = usize::from(u16::from_be_bytes([body[0], body[1]]));
                    [body[topic_len + 2], body[topic_len + 3]]
                }
                _ => continue,
            };
            let response = match (kind, protocol, fault) {
                (8, ProtocolVersion::V311, Fault::RejectSubscribe) => {
                    vec![0x90, 3, id[0], id[1], 0x80]
                }
                (8, ProtocolVersion::V5, Fault::RejectSubscribe) => {
                    vec![0x90, 4, id[0], id[1], 0, 0x87]
                }
                (3, ProtocolVersion::V5, Fault::RejectPublish) => vec![
                    if (header >> 1) & 3 == 2 { 0x50 } else { 0x40 },
                    4,
                    id[0],
                    id[1],
                    0x87,
                    0,
                ],
                (3, ProtocolVersion::V5, Fault::MixedPublish) => {
                    let denied = body.get(2) == Some(&b'd');
                    vec![0x40, 4, id[0], id[1], if denied { 0x87 } else { 0 }, 0]
                }
                (10, ProtocolVersion::V5, Fault::RejectUnsubscribe) => {
                    vec![0xb0, 4, id[0], id[1], 0, 0x87]
                }
                (10, ProtocolVersion::V311, _) => vec![0xb0, 2, id[0], id[1]],
                (10, ProtocolVersion::V5, _) => vec![0xb0, 4, id[0], id[1], 0, 0],
                _ => continue,
            };
            if stream.write_all(&response).await.is_err() {
                break;
            }
        }
    });
    (format!("mqtt://{address}"), requests, task)
}

#[tokio::test]
async fn negative_subacks_are_errors_and_schedule_unsubscribe_for_both_protocols() {
    for protocol in [ProtocolVersion::V311, ProtocolVersion::V5] {
        let (broker, requests, task) = fault_broker(protocol, Fault::RejectSubscribe).await;
        let client = MqttClient::builder("reject-sub")
            .broker(broker)
            .protocol(protocol)
            .request_channel_capacity(1)
            .connect()
            .await
            .unwrap();
        let result = client
            .subscribe_confirmed(
                SubscribeOptions::new("denied/#", QoS::AtLeastOnce).unwrap(),
                DEADLINE,
            )
            .await;
        assert!(matches!(
            result,
            Err(MqttError::BrokerRejected {
                operation: "SUBSCRIBE",
                ..
            })
        ));
        tokio::time::timeout(DEADLINE, async {
            while requests.recv_async().await.unwrap() != 10 {}
        })
        .await
        .expect("rejected subscription must be cleaned remotely");
        client.disconnect().await.unwrap();
        task.await.unwrap();
    }
}

#[tokio::test]
async fn negative_puback_and_pubrec_do_not_report_delivery_success() {
    for qos in [QoS::AtLeastOnce, QoS::ExactlyOnce] {
        let (broker, _, task) = fault_broker(ProtocolVersion::V5, Fault::RejectPublish).await;
        let client = MqttClient::builder("reject-pub")
            .broker(broker)
            .protocol(ProtocolVersion::V5)
            .connect()
            .await
            .unwrap();
        let result = client
            .publish_confirmed(PublishOptions::new("denied", "payload", qos), DEADLINE)
            .await;
        assert!(matches!(
            result,
            Err(MqttError::BrokerRejected {
                operation: "PUBLISH",
                ..
            })
        ));
        client.disconnect().await.unwrap();
        task.await.unwrap();
    }
}

#[tokio::test]
async fn missing_suback_times_out_but_cleanup_can_bypass_a_full_ack_budget() {
    let (broker, requests, task) = fault_broker(ProtocolVersion::V311, Fault::Silence).await;
    let client = MqttClient::builder("timeout")
        .broker(broker)
        .request_channel_capacity(1)
        .connect()
        .await
        .unwrap();
    common::wait_connected(&client).await;
    let result = client
        .subscribe_confirmed(
            SubscribeOptions::new("silent/#", QoS::AtLeastOnce).unwrap(),
            Duration::from_millis(100),
        )
        .await;
    assert!(matches!(result, Err(MqttError::Timeout)));
    tokio::time::timeout(DEADLINE, async {
        while requests.recv_async().await.unwrap() != 10 {}
    })
    .await
    .expect("cleanup must not wait for missing SUBACK");
    client.disconnect().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn connection_loss_before_ack_is_not_success() {
    let (broker, _, task) = fault_broker(ProtocolVersion::V311, Fault::Disconnect).await;
    let client = MqttClient::builder("connection-loss")
        .broker(broker)
        .connect()
        .await
        .unwrap();
    let result = client
        .publish_confirmed(
            PublishOptions::new("lost", "payload", QoS::AtLeastOnce),
            DEADLINE,
        )
        .await;
    assert!(matches!(result, Err(MqttError::Connection { .. })));
    client.disconnect().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn qos_two_waits_for_pubcomp_after_pubrec_and_pubrel() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (ready, stage) = tokio::sync::oneshot::channel();
    let (finish, permitted) = tokio::sync::oneshot::channel();
    let broker = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_packet(&mut stream).await.unwrap();
        stream.write_all(&[0x20, 2, 0, 0]).await.unwrap();
        let (header, body) = read_packet(&mut stream).await.unwrap();
        assert_eq!((header >> 1) & 3, 2);
        let offset = usize::from(u16::from_be_bytes([body[0], body[1]])) + 2;
        let id = [body[offset], body[offset + 1]];
        stream.write_all(&[0x50, 2, id[0], id[1]]).await.unwrap();
        let (header, _) = read_packet(&mut stream).await.unwrap();
        assert_eq!(header >> 4, 6);
        ready.send(()).unwrap();
        permitted.await.unwrap();
        stream.write_all(&[0x70, 2, id[0], id[1]]).await.unwrap();
        assert_eq!(read_packet(&mut stream).await.unwrap().0 >> 4, 14);
    });
    let client = MqttClient::builder("qos-two-stages")
        .broker(format!("mqtt://{address}"))
        .connect()
        .await
        .unwrap();
    let sender = client.clone();
    let mut publish = tokio::spawn(async move {
        sender
            .publish_confirmed(
                PublishOptions::new("qos/two", "payload", QoS::ExactlyOnce),
                DEADLINE,
            )
            .await
    });
    tokio::time::timeout(DEADLINE, stage)
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut publish)
            .await
            .is_err(),
        "PUBREC/PUBREL must not complete a QoS2 receipt"
    );
    finish.send(()).unwrap();
    publish.await.unwrap().unwrap();
    client.disconnect().await.unwrap();
    broker.await.unwrap();
}

#[test]
fn dropping_runtime_marks_a_detached_background_task_closed() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = runtime.block_on(async {
        MqttClient::builder("runtime-abort")
            .broker("mqtt://127.0.0.1:1")
            .connect()
            .await
            .unwrap()
    });
    let state = client.state();
    drop(runtime);
    assert_eq!(*state.borrow(), mqtt_kit::ConnectionState::Closed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_confirmations_stay_correlated_when_mixed_with_unconfirmed_requests() {
    let (broker, _, task) = fault_broker(ProtocolVersion::V5, Fault::MixedPublish).await;
    let client = MqttClient::builder("mixed-outcomes")
        .broker(broker)
        .protocol(ProtocolVersion::V5)
        .request_channel_capacity(2)
        .connect()
        .await
        .unwrap();
    client
        .publish("denied/unconfirmed", "payload", QoS::AtLeastOnce)
        .await
        .unwrap();
    let (denied, accepted) = tokio::join!(
        client.publish_confirmed(
            PublishOptions::new("denied/confirmed", "payload", QoS::AtLeastOnce),
            DEADLINE
        ),
        client.publish_confirmed(
            PublishOptions::new("accepted/confirmed", "payload", QoS::AtLeastOnce),
            DEADLINE
        ),
    );
    assert!(matches!(denied, Err(MqttError::BrokerRejected { .. })));
    accepted.unwrap();
    client.disconnect().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn negative_unsuback_is_not_reported_as_success() {
    let (broker, _, task) = fault_broker(ProtocolVersion::V5, Fault::RejectUnsubscribe).await;
    let client = MqttClient::builder("reject-unsub")
        .broker(broker)
        .protocol(ProtocolVersion::V5)
        .connect()
        .await
        .unwrap();
    let result = client
        .unsubscribe_confirmed("denied/filter", DEADLINE)
        .await;
    assert!(matches!(
        result,
        Err(MqttError::BrokerRejected {
            operation: "UNSUBSCRIBE",
            ..
        })
    ));
    client.disconnect().await.unwrap();
    task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removing_latest_duplicate_restores_the_surviving_remote_qos() {
    for protocol in [ProtocolVersion::V311, ProtocolVersion::V5] {
        let address = common::start_broker(protocol).await;
        let client = MqttClient::builder(format!("restore-options-{}", address.port()))
            .broker(common::broker_url(address))
            .protocol(protocol)
            .request_channel_capacity(1)
            .connect()
            .await
            .unwrap();
        let mut first = client
            .subscribe_confirmed(
                SubscribeOptions::new("restore/options", QoS::AtMostOnce).unwrap(),
                DEADLINE,
            )
            .await
            .unwrap();
        let latest = client
            .subscribe_confirmed(
                SubscribeOptions::new("restore/options", QoS::ExactlyOnce).unwrap(),
                DEADLINE,
            )
            .await
            .unwrap();
        latest.unsubscribe().await.unwrap();
        client
            .publish_confirmed(
                PublishOptions::new("restore/options", "restored", QoS::AtLeastOnce),
                DEADLINE,
            )
            .await
            .unwrap();
        let message = tokio::time::timeout(DEADLINE, first.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.qos, QoS::AtMostOnce);
        client.disconnect().await.unwrap();
    }
}
