//! `mqtt-kit` 的協定無關共通型別。
//!
//! 此 crate 刻意不依賴 `rumqttc`，可供應用程式的領域層安全使用。

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod error;
mod message;
mod options;
mod qos;
mod reconnect;
mod topic;

pub use error::MqttError;
pub use message::{Message, V5PublishProps};
pub use options::{
    LastWill, PublishOptions, RetainHandling, SubscribeOptions, TlsOptions, V5WillProps,
};
pub use qos::{ProtocolVersion, QoS};
pub use reconnect::{ConnectionEvent, ConnectionState, ReconnectPolicy};
pub use topic::TopicFilter;
