/// MQTT 訊息的服務品質等級。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QoS {
    /// 最多傳送一次，不等待確認。
    #[default]
    AtMostOnce,
    /// 至少傳送一次，可能收到重複訊息。
    AtLeastOnce,
    /// 恰好傳送一次，具有最高協定成本。
    ExactlyOnce,
}

/// Client 使用的 MQTT 協定版本。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProtocolVersion {
    /// MQTT 3.1.1。
    #[default]
    V311,
    /// MQTT 5.0。
    V5,
}
