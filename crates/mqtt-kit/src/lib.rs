//! 以 rumqttc 為協定引擎的產品級非同步 MQTT client。
//!
//! 公開介面統一支援 MQTT 3.1.1 與 MQTT 5，並內建斷線重連、自動重訂閱、
//! bounded 訊息路由，以及跨平台 rustls TLS。

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod backend;
mod builder;
mod client;
mod event_loop;
mod subscription;

pub use builder::MqttClientBuilder;
pub use client::MqttClient;
pub use mqtt_kit_core::*;
pub use subscription::Subscription;
