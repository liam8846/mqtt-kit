use bytes::Bytes;

use crate::{QoS, TopicFilter, V5PublishProps};

/// MQTT 5 遺囑訊息屬性。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct V5WillProps {
    /// 連線中斷後延遲發佈遺囑的秒數。
    pub delay_interval: Option<u32>,
    /// Payload format indicator。
    pub payload_format_indicator: Option<u8>,
    /// 遺囑訊息有效秒數。
    pub message_expiry_interval: Option<u32>,
    /// Payload 的 MIME content type。
    pub content_type: Option<String>,
    /// Request/response 回應 topic。
    pub response_topic: Option<String>,
    /// Request/response 關聯資料。
    pub correlation_data: Option<Bytes>,
    /// 使用者自訂屬性。
    pub user_properties: Vec<(String, String)>,
}

/// Client 非預期離線時由 broker 發佈的遺囑訊息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastWill {
    /// 遺囑 topic。
    pub topic: String,
    /// 遺囑 payload。
    pub payload: Bytes,
    /// 遺囑服務品質。
    pub qos: QoS,
    /// 是否由 broker 保留。
    pub retain: bool,
    /// MQTT 5 遺囑屬性。
    pub v5: Option<V5WillProps>,
}

impl LastWill {
    /// 建立遺囑訊息。
    #[must_use]
    pub fn new(
        topic: impl Into<String>,
        payload: impl Into<Bytes>,
        qos: QoS,
        retain: bool,
    ) -> Self {
        Self {
            topic: topic.into(),
            payload: payload.into(),
            qos,
            retain,
            v5: None,
        }
    }

    /// 設定 MQTT 5 遺囑屬性。
    #[must_use]
    pub fn v5_properties(mut self, properties: V5WillProps) -> Self {
        self.v5 = Some(properties);
        self
    }
}

/// 純資料形式的 TLS 與 mTLS 設定。
#[derive(Default, Clone, PartialEq, Eq)]
pub struct TlsOptions {
    /// PEM 編碼的自訂 CA；未設定時使用內建 Mozilla 根憑證。
    pub ca_pem: Option<Vec<u8>>,
    /// PEM 編碼的 client certificate 與 private key。
    pub client_auth: Option<(Vec<u8>, Vec<u8>)>,
}

impl std::fmt::Debug for TlsOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TlsOptions")
            .field("ca_pem", &self.ca_pem.as_ref().map(|_| "[REDACTED]"))
            .field(
                "client_auth",
                &self.client_auth.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

impl TlsOptions {
    /// 建立使用內建 Mozilla 根憑證的 TLS 設定。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 設定 PEM 編碼的自訂 CA bundle。
    #[must_use]
    pub fn ca_pem(mut self, ca: impl Into<Vec<u8>>) -> Self {
        self.ca_pem = Some(ca.into());
        self
    }

    /// 設定 PEM 編碼的 mTLS client certificate chain 與 private key。
    #[must_use]
    pub fn client_auth_pem(
        mut self,
        certificate: impl Into<Vec<u8>>,
        private_key: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_auth = Some((certificate.into(), private_key.into()));
        self
    }
}

/// 單次 publish 的完整選項。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishOptions {
    /// 發佈 topic。
    pub topic: String,
    /// Payload；傳入 `Bytes` 時不複製。
    pub payload: Bytes,
    /// 服務品質。
    pub qos: QoS,
    /// 是否要求 broker 保留。
    pub retain: bool,
    /// MQTT 5 PUBLISH 屬性。
    pub v5: Option<V5PublishProps>,
}

impl PublishOptions {
    /// 建立 publish 選項。
    #[must_use]
    pub fn new(topic: impl Into<String>, payload: impl Into<Bytes>, qos: QoS) -> Self {
        Self {
            topic: topic.into(),
            payload: payload.into(),
            qos,
            retain: false,
            v5: None,
        }
    }

    /// 設定 retained 旗標。
    #[must_use]
    pub fn retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }

    /// 設定全部 MQTT 5 PUBLISH 屬性。
    #[must_use]
    pub fn v5_properties(mut self, properties: V5PublishProps) -> Self {
        self.v5 = Some(properties);
        self
    }

    /// 加入一筆 MQTT 5 user property。
    #[must_use]
    pub fn user_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.v5
            .get_or_insert_with(V5PublishProps::default)
            .user_properties
            .push((key.into(), value.into()));
        self
    }

    /// 設定 MQTT 5 content type。
    #[must_use]
    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.v5
            .get_or_insert_with(V5PublishProps::default)
            .content_type = Some(value.into());
        self
    }

    /// 設定 MQTT 5 訊息有效秒數。
    #[must_use]
    pub fn message_expiry_interval(mut self, seconds: u32) -> Self {
        self.v5
            .get_or_insert_with(V5PublishProps::default)
            .message_expiry_interval = Some(seconds);
        self
    }
}

/// MQTT 5 retained message 的初次轉送規則。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RetainHandling {
    /// 每次 SUBSCRIBE 都轉送。
    #[default]
    OnEverySubscribe,
    /// 只有新訂閱時轉送。
    OnNewSubscribe,
    /// 永不因 SUBSCRIBE 轉送 retained message。
    Never,
}

/// 單次 subscribe 的完整選項。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeOptions {
    /// 已驗證的 topic filter。
    pub filter: TopicFilter,
    /// 要求的服務品質上限。
    pub qos: QoS,
    /// MQTT 5 no-local 選項。
    pub no_local: bool,
    /// MQTT 5 retain-as-published 選項。
    pub retain_as_published: bool,
    /// MQTT 5 retained message 轉送規則。
    pub retain_handling: RetainHandling,
    /// MQTT 5 subscription identifier。
    pub subscription_identifier: Option<usize>,
    /// MQTT 5 SUBSCRIBE user properties。
    pub user_properties: Vec<(String, String)>,
}

impl SubscribeOptions {
    /// 解析 filter 並建立 subscribe 選項。
    pub fn new(filter: impl Into<String>, qos: QoS) -> Result<Self, crate::MqttError> {
        Ok(Self {
            filter: TopicFilter::new(filter)?,
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::default(),
            subscription_identifier: None,
            user_properties: Vec::new(),
        })
    }

    /// 設定 MQTT 5 no-local。
    #[must_use]
    pub fn no_local(mut self, enabled: bool) -> Self {
        self.no_local = enabled;
        self
    }

    /// 設定 MQTT 5 retain-as-published。
    #[must_use]
    pub fn retain_as_published(mut self, enabled: bool) -> Self {
        self.retain_as_published = enabled;
        self
    }

    /// 設定 MQTT 5 retained message 轉送規則。
    #[must_use]
    pub fn retain_handling(mut self, handling: RetainHandling) -> Self {
        self.retain_handling = handling;
        self
    }

    /// 設定 MQTT 5 subscription identifier。
    #[must_use]
    pub fn subscription_identifier(mut self, identifier: usize) -> Self {
        self.subscription_identifier = Some(identifier);
        self
    }

    /// 加入一筆 MQTT 5 SUBSCRIBE user property。
    #[must_use]
    pub fn user_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.user_properties.push((key.into(), value.into()));
        self
    }

    /// 是否使用任何 MQTT 5 專屬訂閱能力。
    #[must_use]
    pub fn uses_v5_features(&self) -> bool {
        self.no_local
            || self.retain_as_published
            || self.retain_handling != RetainHandling::OnEverySubscribe
            || self.subscription_identifier.is_some()
            || !self.user_properties.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::TlsOptions;

    #[test]
    fn tls_debug_redacts_pem_bytes() {
        let options = TlsOptions::new()
            .ca_pem(b"audit-ca-marker".to_vec())
            .client_auth_pem(b"audit-cert-marker".to_vec(), b"audit-key-marker".to_vec());
        for output in [format!("{options:?}"), format!("{options:#?}")] {
            assert!(output.contains("[REDACTED]"));
            for secret in ["audit-ca-marker", "audit-cert-marker", "audit-key-marker"] {
                assert!(!output.contains(secret));
                assert!(!output.contains(&format!("{:?}", secret.as_bytes())));
            }
        }
        assert!(format!("{:?}", TlsOptions::new()).contains("None"));
    }
}
