use bytes::Bytes;

use crate::QoS;

/// 收到的 MQTT 5 PUBLISH 屬性。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct V5PublishProps {
    /// Payload format indicator；`0` 為未指定 bytes，`1` 為 UTF-8。
    pub payload_format_indicator: Option<u8>,
    /// 訊息剩餘有效秒數。
    pub message_expiry_interval: Option<u32>,
    /// Topic alias。
    pub topic_alias: Option<u16>,
    /// Request/response 模式的回應 topic。
    pub response_topic: Option<String>,
    /// Request/response 模式的關聯資料。
    pub correlation_data: Option<Bytes>,
    /// 使用者自訂鍵值；保留原始順序及重複鍵。
    pub user_properties: Vec<(String, String)>,
    /// Broker 附加的 subscription identifier。
    pub subscription_identifiers: Vec<usize>,
    /// Payload 的 MIME content type。
    pub content_type: Option<String>,
}

impl V5PublishProps {
    /// 建立空的 MQTT 5 PUBLISH 屬性。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 加入一筆 user property。
    #[must_use]
    pub fn user_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.user_properties.push((key.into(), value.into()));
        self
    }

    /// 設定 content type。
    #[must_use]
    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.content_type = Some(value.into());
        self
    }

    /// 設定訊息有效秒數。
    #[must_use]
    pub fn message_expiry_interval(mut self, seconds: u32) -> Self {
        self.message_expiry_interval = Some(seconds);
        self
    }

    /// 設定回應 topic。
    #[must_use]
    pub fn response_topic(mut self, topic: impl Into<String>) -> Self {
        self.response_topic = Some(topic.into());
        self
    }

    /// 設定 correlation data，且不複製既有 `Bytes`。
    #[must_use]
    pub fn correlation_data(mut self, data: impl Into<Bytes>) -> Self {
        self.correlation_data = Some(data.into());
        self
    }
}

/// 從 broker 收到並完成協定版本正規化的訊息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// MQTT topic name。
    pub topic: String,
    /// 訊息內容；使用參照計數的 `Bytes` 避免 payload 複製。
    pub payload: Bytes,
    /// 訊息服務品質。
    pub qos: QoS,
    /// 是否為 retained message。
    pub retain: bool,
    /// MQTT 5 PUBLISH 屬性；V3.1.1 訊息永遠為 `None`。
    pub v5: Option<V5PublishProps>,
}
