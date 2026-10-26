use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    thread,
    time::Duration,
};

use mqtt_kit::{ConnectionState, MqttClient, ProtocolVersion};
use rumqttd::{Broker, Config, ConnectionSettings, RouterConfig, ServerSettings};

pub fn unused_address() -> SocketAddr {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind random port");
    listener.local_addr().expect("read random port")
}

pub async fn start_broker(protocol: ProtocolVersion) -> SocketAddr {
    let address = unused_address();
    let settings = ServerSettings {
        name: format!("test-{}", address.port()),
        listen: address,
        tls: None,
        next_connection_delay_ms: 0,
        connections: ConnectionSettings {
            connection_timeout_ms: 5_000,
            max_payload_size: 1024 * 1024,
            max_inflight_count: 100,
            auth: None,
            external_auth: None,
            dynamic_filters: true,
        },
    };
    let servers = HashMap::from([(address.port().to_string(), settings)]);
    let mut config = Config {
        id: usize::from(address.port()),
        router: RouterConfig {
            max_connections: 100,
            max_outgoing_packet_count: 200,
            max_segment_size: 1024 * 1024,
            max_segment_count: 10,
            ..RouterConfig::default()
        },
        ..Config::default()
    };
    match protocol {
        ProtocolVersion::V311 => config.v4 = Some(servers),
        ProtocolVersion::V5 => config.v5 = Some(servers),
    }

    thread::Builder::new()
        .name(format!("rumqttd-test-{}", address.port()))
        .spawn(move || {
            let mut broker = Broker::new(config);
            broker.start().expect("rumqttd starts");
        })
        .expect("spawn rumqttd thread");

    for _ in 0..100 {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            return address;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("rumqttd did not start on {address}");
}

pub async fn wait_connected(client: &MqttClient) {
    let mut state = client.state();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if *state.borrow() == ConnectionState::Connected {
                return;
            }
            state.changed().await.expect("state sender remains alive");
        }
    })
    .await
    .expect("client connects");
}

pub fn broker_url(address: SocketAddr) -> String {
    let host = match address.ip() {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    };
    format!("mqtt://{host}:{}", address.port())
}
