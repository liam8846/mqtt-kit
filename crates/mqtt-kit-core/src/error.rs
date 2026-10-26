use std::error::Error;

/// mqtt-kit 所有可預期失敗的統一錯誤型別。
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MqttError {
    /// 組態值無效。
    #[error("無效的 MQTT 組態：{0}")]
    InvalidConfig(String),

    /// Topic name 或 topic filter 不符合 MQTT 規範。
    #[error("無效的 MQTT topic：{0}")]
    InvalidTopic(String),

    /// 與 broker 建立或維持連線時失敗。
    #[error("MQTT 連線失敗：{message}")]
    Connection {
        /// 適合寫入日誌的失敗摘要。
        message: String,
        /// 底層錯誤鏈（若有）。
        #[source]
        source: Option<Box<dyn Error + Send + Sync + 'static>>,
    },

    /// Broker 拒絕帳密或授權。
    #[error("MQTT 認證失敗：{reason}")]
    AuthenticationFailed {
        /// Broker 回傳的原因碼或說明。
        reason: String,
    },

    /// MQTT 封包或狀態違反協定。
    #[error("MQTT 協定錯誤：{0}")]
    Protocol(String),

    /// 操作在期限內未完成。
    #[error("MQTT 操作逾時")]
    Timeout,

    /// Client 已關閉，不能再接受操作。
    #[error("MQTT client 已關閉")]
    ClientClosed,

    /// 目前編譯功能或協定版本不支援要求的能力。
    #[error("不支援的 MQTT 功能：{0}")]
    UnsupportedFeature(String),

    /// 背景任務非預期終止。
    #[error("MQTT 背景任務失敗：{0}")]
    BackgroundTask(String),
}

impl MqttError {
    /// 建立保留底層錯誤鏈的連線錯誤。
    pub fn connection<E>(message: impl Into<String>, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Connection {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// 建立只有文字摘要的連線錯誤。
    pub fn connection_message(message: impl Into<String>) -> Self {
        Self::Connection {
            message: message.into(),
            source: None,
        }
    }
}
