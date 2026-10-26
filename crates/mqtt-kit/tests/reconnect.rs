mod common;

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use mqtt_kit::{
    ConnectionEvent, ConnectionState, MqttClient, MqttError, ProtocolVersion, QoS, ReconnectPolicy,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, copy, copy_bidirectional},
    net::TcpListener,
    sync::watch,
    task::JoinHandle,
};

async fn start_proxy(
    target: std::net::SocketAddr,
) -> (std::net::SocketAddr, watch::Sender<u64>, JoinHandle<()>) {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind proxy");
    let address = listener.local_addr().expect("proxy address");
    let (generation, _) = watch::channel(0_u64);
    let generation_for_task = generation.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut inbound, _) = listener.accept().await.expect("accept proxy client");
            let mut changed = generation_for_task.subscribe();
            let accepted_generation = *changed.borrow();
            tokio::spawn(async move {
                let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await else {
                    return;
                };
                tokio::select! {
                    _ = copy_bidirectional(&mut inbound, &mut outbound) => {}
                    result = changed.changed() => {
                        if result.is_ok() && *changed.borrow() == accepted_generation {
                            let _ = changed.changed().await;
                        }
                    }
                }
            });
        }
    });
    (address, generation, task)
}

/// 啟動會記錄 client 至 broker 位元組的 TCP proxy。
async fn start_recording_proxy(
    target: std::net::SocketAddr,
) -> (std::net::SocketAddr, Arc<Mutex<Vec<u8>>>, JoinHandle<()>) {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind recording proxy");
    let address = listener.local_addr().expect("recording proxy address");
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let recorded_for_task = Arc::clone(&recorded);
    let task = tokio::spawn(async move {
        loop {
            let (inbound, _) = listener
                .accept()
                .await
                .expect("accept recording proxy client");
            let recorded = Arc::clone(&recorded_for_task);
            tokio::spawn(async move {
                let Ok(outbound) = tokio::net::TcpStream::connect(target).await else {
                    return;
                };
                let (mut client_reader, mut client_writer) = inbound.into_split();
                let (mut broker_reader, mut broker_writer) = outbound.into_split();
                let client_to_broker = async {
                    let transfer = async {
                        let mut buffer = [0_u8; 4096];
                        loop {
                            let count = client_reader.read(&mut buffer).await?;
                            if count == 0 {
                                return std::io::Result::Ok(());
                            }
                            recorded
                                .lock()
                                .expect("recording mutex")
                                .extend_from_slice(&buffer[..count]);
                            broker_writer.write_all(&buffer[..count]).await?;
                        }
                    }
                    .await;
                    // 將上行 EOF 傳給 broker，避免等待下行結束時互相阻塞。
                    let _ = broker_writer.shutdown().await;
                    transfer
                };
                let broker_to_client = async {
                    let transfer = copy(&mut broker_reader, &mut client_writer).await;
                    let _ = client_writer.shutdown().await;
                    transfer
                };
                // 下行關閉／寫入失敗不能取消上行：DISCONNECT 可能還在等待記錄。
                let _ = tokio::join!(client_to_broker, broker_to_client);
            });
        }
    });
    (address, recorded, task)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnects_and_restores_existing_subscription() {
    let broker = common::start_broker(ProtocolVersion::V311).await;
    let (proxy, generation, proxy_task) = start_proxy(broker).await;
    let client = MqttClient::builder(format!("reconnect-{}", proxy.port()))
        .broker(common::broker_url(proxy))
        .reconnect(
            ReconnectPolicy::exponential(Duration::from_millis(20), Duration::from_millis(100))
                .jitter(0.0)
                .max_retries(Some(20)),
        )
        .connect()
        .await
        .expect("create client");
    let mut events = client.events();
    common::wait_connected(&client).await;
    let mut subscription = client
        .subscribe("reconnect/+/data", QoS::AtLeastOnce)
        .await
        .expect("subscribe");

    client
        .publish("reconnect/before/data", "before", QoS::AtLeastOnce)
        .await
        .expect("publish before reconnect");
    tokio::time::timeout(Duration::from_secs(5), subscription.recv())
        .await
        .expect("before timeout")
        .expect("before message");

    generation.send_modify(|value| *value += 1);
    let (saw_reconnecting, saw_connected) = tokio::time::timeout(Duration::from_secs(8), async {
        let mut reconnecting = false;
        loop {
            match events.recv().await.expect("connection event") {
                ConnectionEvent::Reconnecting { .. } => reconnecting = true,
                ConnectionEvent::Connected { .. } if reconnecting => return (true, true),
                ConnectionEvent::ConnectionFailed { error } => panic!("reconnect failed: {error}"),
                _ => {}
            }
        }
    })
    .await
    .expect("reconnect timeout");
    assert!(saw_reconnecting && saw_connected);

    client
        .publish("reconnect/after/data", "after", QoS::AtLeastOnce)
        .await
        .expect("publish after reconnect");
    let message = tokio::time::timeout(Duration::from_secs(5), subscription.recv())
        .await
        .expect("after timeout")
        .expect("after message");
    assert_eq!(message.payload, "after");

    client.disconnect().await.expect("disconnect");
    proxy_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_exhaustion_closes_client_without_panic() {
    let address = common::unused_address();
    let client = MqttClient::builder(format!("closed-{}", address.port()))
        .broker(common::broker_url(address))
        .reconnect(
            ReconnectPolicy::exponential(Duration::from_millis(10), Duration::from_millis(20))
                .jitter(0.0)
                .max_retries(Some(2)),
        )
        .connect()
        .await
        .expect("create client");
    let mut events = client.events();
    // Windows 對未監聽埠的 TCP 連線可能需等待每次 SYN 重傳逾時（約 2 秒），
    // 多次重試會超過 Linux 收到立即 RST 時所需的 5 秒，因此保留充足的 CI 時限。
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if matches!(
                events.recv().await.expect("connection event"),
                ConnectionEvent::ConnectionFailed { .. }
            ) {
                return;
            }
        }
    })
    .await
    .expect("failure event timeout");

    let result = client
        .publish("closed/client", "data", QoS::AtMostOnce)
        .await;
    assert!(matches!(result, Err(MqttError::ClientClosed)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_all_handles_stops_background_task() {
    let address = common::start_broker(ProtocolVersion::V311).await;
    let client = MqttClient::builder(format!("drop-handles-{}", address.port()))
        .broker(common::broker_url(address))
        .connect()
        .await
        .expect("create client");
    common::wait_connected(&client).await;
    let clone = client.clone();
    let mut state = client.state();

    drop(client);
    assert_eq!(*state.borrow(), ConnectionState::Connected);
    drop(clone);

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if *state.borrow() == ConnectionState::Closed {
                return;
            }
            if state.changed().await.is_err() {
                return;
            }
        }
    })
    .await
    .expect("background task stops after the final handle is dropped");
}

/// 驗證最後一個 handle 被 Drop 時會送出 MQTT 3.1.1 DISCONNECT。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_all_handles_sends_disconnect_packet() {
    let broker = common::start_broker(ProtocolVersion::V311).await;
    let (proxy, recorded, proxy_task) = start_recording_proxy(broker).await;
    let client = MqttClient::builder(format!("drop-disconnect-{}", proxy.port()))
        .broker(common::broker_url(proxy))
        .connect()
        .await
        .expect("create client");
    common::wait_connected(&client).await;
    let _subscription = client
        .subscribe("drop/disconnect", QoS::AtLeastOnce)
        .await
        .expect("subscribe");
    let mut state = client.state();

    drop(client);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if *state.borrow() == ConnectionState::Closed {
                return;
            }
            if state.changed().await.is_err() {
                return;
            }
        }
    })
    .await
    .expect("background task stops after graceful disconnect");

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if recorded
                .lock()
                .expect("recording mutex")
                .ends_with(&[0xE0, 0x00])
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("proxy records disconnect packet");
    let recorded = recorded.lock().expect("recording mutex");
    assert!(
        recorded.ends_with(&[0xE0, 0x00]),
        "client 最後送出的封包不是 MQTT 3.1.1 DISCONNECT：{recorded:02x?}"
    );
    proxy_task.abort();
}

/// 驗證 request channel 滿載後，自動重訂閱仍會退避並補齊全部 filter。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resubscribe_retries_when_request_channel_is_full() {
    let broker = common::start_broker(ProtocolVersion::V311).await;
    let (proxy, generation, proxy_task) = start_proxy(broker).await;
    let client = MqttClient::builder(format!("resubscribe-queue-{}", proxy.port()))
        .broker(common::broker_url(proxy))
        .request_channel_capacity(1)
        .reconnect(
            ReconnectPolicy::exponential(Duration::from_millis(20), Duration::from_millis(100))
                .jitter(0.0)
                .max_retries(Some(20)),
        )
        .connect()
        .await
        .expect("create client");
    let mut events = client.events();
    common::wait_connected(&client).await;

    let mut subscriptions = Vec::new();
    for index in 0..5 {
        let topic = format!("resubscribe/retry/{index}");
        let subscription = client
            .subscribe(topic.as_str(), QoS::AtLeastOnce)
            .await
            .expect("subscribe");
        subscriptions.push((topic, subscription));
    }

    generation.send_modify(|value| *value += 1);
    // 第一輪恢復訂閱會在通知 Connected 前排入；同時確認重連與滿載失敗事件，
    // 不因等待其中一個事件而丟掉另一個已到達的事件。
    tokio::time::timeout(Duration::from_secs(8), async {
        let mut reconnecting = false;
        let mut connected = false;
        let mut resubscribe_failed = false;
        loop {
            match events.recv().await.expect("connection event") {
                ConnectionEvent::Reconnecting { .. } => reconnecting = true,
                ConnectionEvent::Connected { .. } if reconnecting => connected = true,
                ConnectionEvent::ResubscribeFailed { attempt: 1, .. } if reconnecting => {
                    resubscribe_failed = true;
                }
                ConnectionEvent::ConnectionFailed { error } => {
                    panic!("reconnect failed: {error}");
                }
                _ => {}
            }
            if connected && resubscribe_failed {
                return;
            }
        }
    })
    .await
    .expect("missing reconnect or initial resubscribe failure event");

    tokio::time::timeout(Duration::from_secs(20), async {
        for (topic, subscription) in &mut subscriptions {
            loop {
                client
                    .publish(topic.as_str(), "after-resubscribe", QoS::AtLeastOnce)
                    .await
                    .expect("publish while waiting for resubscribe");
                match tokio::time::timeout(Duration::from_millis(300), subscription.recv()).await {
                    Ok(Some(message)) => {
                        assert_eq!(message.topic, *topic);
                        assert_eq!(message.payload, "after-resubscribe");
                        break;
                    }
                    Ok(None) => panic!("subscription closed while waiting for resubscribe"),
                    Err(_) => {}
                }
            }
        }
    })
    .await
    .expect("all subscriptions recover within retry deadline");

    client.disconnect().await.expect("disconnect");
    proxy_task.abort();
}

/// 下行 EOF 只能半關閉 proxy，仍必須轉送並記錄之後抵達的上行位元組。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recording_proxy_preserves_upstream_after_broker_half_close() {
    let target = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = target.local_addr().unwrap();
    let target_task = tokio::spawn(async move {
        let (mut stream, _) = target.accept().await.unwrap();
        stream.shutdown().await.unwrap();
        let mut received = Vec::new();
        stream.read_to_end(&mut received).await.unwrap();
        received
    });
    let (proxy, recorded, proxy_task) = start_recording_proxy(address).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = tokio::net::TcpStream::connect(proxy).await.unwrap();
        let mut downlink = Vec::new();
        stream.read_to_end(&mut downlink).await.unwrap();
        assert!(downlink.is_empty());
        stream.write_all(&[0xE0, 0x00]).await.unwrap();
        stream.shutdown().await.unwrap();
        assert_eq!(target_task.await.unwrap(), [0xE0, 0x00]);
        assert_eq!(*recorded.lock().expect("recording mutex"), [0xE0, 0x00]);
    })
    .await
    .expect("proxy must preserve independent TCP half-closes");
    proxy_task.abort();
}
