use std::time::Duration;

use mqtt_kit_core::{LastWill, MqttError, ProtocolVersion, ReconnectPolicy, TlsOptions};
use url::{Host, Url};

use crate::{
    MqttClient,
    backend::{BackendConfig, create_backend},
    client::ClientInner,
};

/// 用於驗證組態並建立 [`MqttClient`] 的 builder。
#[derive(Clone)]
pub struct MqttClientBuilder {
    client_id: String,
    broker: Option<String>,
    protocol: ProtocolVersion,
    credentials: Option<(String, String)>,
    tls: Option<TlsOptions>,
    #[cfg(feature = "tls")]
    tls_client_config: Option<std::sync::Arc<rustls::ClientConfig>>,
    keep_alive: Duration,
    clean_start: bool,
    last_will: Option<LastWill>,
    reconnect: ReconnectPolicy,
    request_channel_capacity: usize,
    subscription_capacity: usize,
    subscription_byte_capacity: usize,
    buffer_byte_capacity: usize,
    max_subscriptions: usize,
}

impl std::fmt::Debug for MqttClientBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = formatter.debug_struct("MqttClientBuilder");
        debug
            .field("client_id", &self.client_id)
            .field("broker", &self.broker.as_ref().map(|_| "[REDACTED]"))
            .field("protocol", &self.protocol)
            .field(
                "credentials",
                &self.credentials.as_ref().map(|_| "[REDACTED]"),
            )
            .field("tls", &self.tls);
        #[cfg(feature = "tls")]
        debug.field(
            "tls_client_config",
            &self.tls_client_config.as_ref().map(|_| "[REDACTED]"),
        );
        debug
            .field("keep_alive", &self.keep_alive)
            .field("clean_start", &self.clean_start)
            .field("last_will", &self.last_will.as_ref().map(|_| "[REDACTED]"))
            .field("reconnect", &self.reconnect)
            .field("request_channel_capacity", &self.request_channel_capacity)
            .field("subscription_capacity", &self.subscription_capacity)
            .field(
                "subscription_byte_capacity",
                &self.subscription_byte_capacity,
            )
            .field("buffer_byte_capacity", &self.buffer_byte_capacity)
            .field("max_subscriptions", &self.max_subscriptions)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedBroker {
    scheme: String,
    host: String,
    port: u16,
}

impl MqttClientBuilder {
    pub(crate) fn new(client_id: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            broker: None,
            protocol: ProtocolVersion::V311,
            credentials: None,
            tls: None,
            #[cfg(feature = "tls")]
            tls_client_config: None,
            keep_alive: Duration::from_secs(60),
            clean_start: true,
            last_will: None,
            reconnect: ReconnectPolicy::default(),
            request_channel_capacity: 256,
            subscription_capacity: 128,
            subscription_byte_capacity: 4 * 1024 * 1024,
            buffer_byte_capacity: 16 * 1024 * 1024,
            max_subscriptions: 1024,
        }
    }

    /// 設定 broker URL；支援 `mqtt`、`mqtts`、`ws` 與 `wss`。
    #[must_use]
    pub fn broker(mut self, url: impl Into<String>) -> Self {
        self.broker = Some(url.into());
        self
    }

    /// 設定 MQTT 協定版本；預設為 V3.1.1。
    #[must_use]
    pub fn protocol(mut self, protocol: ProtocolVersion) -> Self {
        self.protocol = protocol;
        self
    }

    /// 設定 username/password 認證。
    #[must_use]
    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.credentials = Some((username.into(), password.into()));
        self
    }

    /// 設定 TLS；需要 `tls` feature 與 `mqtts`/`wss` URL，否則連線會回傳錯誤。
    #[must_use]
    pub fn tls(mut self, options: TlsOptions) -> Self {
        self.tls = Some(options);
        self
    }

    /// 直接設定完整的 rustls client config。
    ///
    /// 這是自訂 crypto provider、FIPS 模式、憑證驗證器及其他進階 TLS 行為的 escape hatch。
    /// 此設定與 [`Self::tls`] 互斥；兩者同時設定時，[`Self::connect`] 會回傳
    /// [`MqttError::InvalidConfig`]。
    ///
    /// # 相容性
    ///
    /// 此 API 讓 rustls 型別進入公開介面，因此 mqtt-kit 日後升級 rustls major version
    /// 會是 breaking change。
    ///
    /// # 安全注意事項
    ///
    /// 傳入自訂 [`rustls::ClientConfig`] 會完全接管 TLS 握手行為。若自訂驗證器關閉
    /// 伺服器憑證驗證，連線將面臨中間人攻擊（MITM）風險；呼叫端須自行確保憑證驗證
    /// 已正確配置。
    #[cfg(feature = "tls")]
    #[must_use]
    pub fn tls_client_config(mut self, config: std::sync::Arc<rustls::ClientConfig>) -> Self {
        self.tls_client_config = Some(config);
        self
    }

    /// 設定 keep-alive 間隔：V3.1.1 至少 1 秒，V5 至少 5 秒，最大 65535 秒。
    #[must_use]
    pub fn keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// 設定是否建立乾淨 session（MQTT 5 對應 clean start）。
    #[must_use]
    pub fn clean_start(mut self, clean_start: bool) -> Self {
        self.clean_start = clean_start;
        self
    }

    /// 設定非預期斷線時的 Last Will。
    #[must_use]
    pub fn last_will(mut self, last_will: LastWill) -> Self {
        self.last_will = Some(last_will);
        self
    }

    /// 設定斷線重連策略。
    #[must_use]
    pub fn reconnect(mut self, reconnect: ReconnectPolicy) -> Self {
        self.reconnect = reconnect;
        self
    }

    /// 設定送往協定事件迴圈的 bounded request queue 容量。
    #[must_use]
    pub fn request_channel_capacity(mut self, capacity: usize) -> Self {
        self.request_channel_capacity = capacity;
        self
    }

    /// 設定每個 [`crate::Subscription`] 的本地訊息容量；滿載時丟棄最舊訊息。
    #[must_use]
    pub fn subscription_capacity(mut self, capacity: usize) -> Self {
        self.subscription_capacity = capacity;
        self
    }

    /// 設定每個訂閱的緩衝內容位元組預算；預設 4 MiB。
    #[must_use]
    pub fn subscription_byte_capacity(mut self, bytes: usize) -> Self {
        self.subscription_byte_capacity = bytes;
        self
    }

    /// 設定所有本地佇列合計的內容位元組預算；預設 16 MiB。
    /// 共用 payload 在每個佇列分別計數；不代表整個程序的實際記憶體上限。
    #[must_use]
    pub fn buffer_byte_capacity(mut self, bytes: usize) -> Self {
        self.buffer_byte_capacity = bytes;
        self
    }

    /// 設定本地串流與待清理 filter 的數量上限；預設 1024。
    #[must_use]
    pub fn max_subscriptions(mut self, count: usize) -> Self {
        self.max_subscriptions = count;
        self
    }

    /// 驗證組態、建立 backend，並啟動背景事件迴圈。
    pub async fn connect(self) -> Result<MqttClient, MqttError> {
        self.validate()?;
        let broker_url = self
            .broker
            .as_deref()
            .ok_or_else(|| MqttError::InvalidConfig("必須設定 broker URL".into()))?;
        let broker = parse_broker(broker_url)?;
        validate_scheme_feature(&broker.scheme)?;
        self.validate_transport(&broker.scheme)?;

        let tls_v3 = self.tls.clone();
        let scheme_v3 = broker.scheme.clone();
        let tls_v5 = self.tls.clone();
        let scheme_v5 = broker.scheme.clone();
        #[cfg(feature = "tls")]
        let tls_client_config_v3 = self.tls_client_config.clone();
        #[cfg(feature = "tls")]
        let tls_client_config_v5 = self.tls_client_config.clone();
        let (backend, event_loop) = create_backend(
            BackendConfig {
                protocol: self.protocol,
                client_id: &self.client_id,
                host: &broker.host,
                port: broker.port,
                keep_alive: self.keep_alive,
                clean_start: self.clean_start,
                credentials: self.credentials.as_ref(),
                last_will: self.last_will.as_ref(),
                channel_capacity: self.request_channel_capacity,
            },
            move |options| {
                set_transport_v3(
                    options,
                    &scheme_v3,
                    tls_v3.as_ref(),
                    #[cfg(feature = "tls")]
                    tls_client_config_v3.as_ref(),
                )
            },
            move |options| {
                set_transport_v5(
                    options,
                    &scheme_v5,
                    tls_v5.as_ref(),
                    #[cfg(feature = "tls")]
                    tls_client_config_v5.as_ref(),
                )
            },
        )?;

        Ok(ClientInner::start(
            backend,
            event_loop,
            self.protocol,
            self.reconnect,
            self.subscription_capacity,
            crate::subscription::SubscriptionRegistry::new(
                self.max_subscriptions,
                self.subscription_byte_capacity,
                self.buffer_byte_capacity,
            ),
        ))
    }

    fn validate_transport(&self, scheme: &str) -> Result<(), MqttError> {
        let has_tls = self.tls.is_some();
        #[cfg(feature = "tls")]
        let has_tls = has_tls || self.tls_client_config.is_some();
        if has_tls && matches!(scheme, "mqtt" | "ws") {
            return Err(MqttError::InvalidConfig(
                "TLS 設定不可搭配明文 mqtt:// 或 ws://；請使用 mqtts:// 或 wss://".into(),
            ));
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), MqttError> {
        #[cfg(not(feature = "tls"))]
        if self.tls.is_some() {
            return Err(MqttError::UnsupportedFeature(
                "tls() 需要啟用 `tls` feature".into(),
            ));
        }
        #[cfg(feature = "tls")]
        if self.tls.is_some() && self.tls_client_config.is_some() {
            return Err(MqttError::InvalidConfig(
                "tls() 與 tls_client_config() 不可同時設定".into(),
            ));
        }
        if self.client_id.is_empty() {
            return Err(MqttError::InvalidConfig("client_id 不可為空".into()));
        }
        if self.client_id.as_bytes().contains(&0) {
            return Err(MqttError::InvalidConfig("client_id 不可包含 NUL".into()));
        }
        let minimum = match self.protocol {
            ProtocolVersion::V311 => Duration::from_secs(1),
            ProtocolVersion::V5 => Duration::from_secs(5),
        };
        if self.keep_alive < minimum || self.keep_alive > Duration::from_secs(u16::MAX.into()) {
            return Err(MqttError::InvalidConfig(format!(
                "keep_alive 必須介於 {} 與 65535 秒之間",
                minimum.as_secs()
            )));
        }
        if self.request_channel_capacity == 0 {
            return Err(MqttError::InvalidConfig(
                "request_channel_capacity 必須大於零".into(),
            ));
        }
        if self.subscription_capacity == 0 {
            return Err(MqttError::InvalidConfig(
                "subscription_capacity 必須大於零".into(),
            ));
        }
        if self.subscription_byte_capacity == 0
            || self.buffer_byte_capacity == 0
            || self.max_subscriptions == 0
        {
            return Err(MqttError::InvalidConfig(
                "位元組預算與 max_subscriptions 必須大於零".into(),
            ));
        }
        self.reconnect.validate()?;
        if let Some(will) = &self.last_will {
            validate_publish_topic(&will.topic)?;
        }
        Ok(())
    }
}

fn parse_broker(input: &str) -> Result<ParsedBroker, MqttError> {
    let url = Url::parse(input)
        .map_err(|error| MqttError::InvalidConfig(format!("broker URL 無法解析：{error}")))?;
    let scheme = url.scheme().to_ascii_lowercase();
    let default_port = match scheme.as_str() {
        "mqtt" => 1883,
        "mqtts" => 8883,
        "ws" => 80,
        "wss" => 443,
        _ => {
            return Err(MqttError::InvalidConfig(format!(
                "不支援的 broker URL scheme：{scheme}"
            )));
        }
    };
    let host = match url.host() {
        Some(Host::Ipv6(address)) => address.to_string(),
        Some(host) => host.to_string(),
        None => return Err(MqttError::InvalidConfig("broker URL 缺少 host".into())),
    };
    if host.is_empty() {
        return Err(MqttError::InvalidConfig("broker URL 缺少 host".into()));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(MqttError::InvalidConfig(
            "請使用 credentials() 設定帳密，不要放在 broker URL".into(),
        ));
    }
    let port = match url.port() {
        Some(port) => port,
        None => default_port,
    };
    Ok(ParsedBroker { scheme, host, port })
}

pub(crate) fn validate_publish_topic(topic: &str) -> Result<(), MqttError> {
    if topic.len() > usize::from(u16::MAX)
        || topic.is_empty()
        || topic.as_bytes().contains(&0)
        || topic.contains('+')
        || topic.contains('#')
    {
        return Err(MqttError::InvalidTopic(format!(
            "PUBLISH topic name 無效：{topic:?}"
        )));
    }
    Ok(())
}

fn validate_scheme_feature(scheme: &str) -> Result<(), MqttError> {
    match scheme {
        "mqtt" => Ok(()),
        "mqtts" => {
            if cfg!(feature = "tls") {
                Ok(())
            } else {
                Err(MqttError::UnsupportedFeature(
                    "mqtts:// 需要啟用 `tls` feature".into(),
                ))
            }
        }
        "ws" => {
            if cfg!(feature = "websocket") {
                Ok(())
            } else {
                Err(MqttError::UnsupportedFeature(
                    "ws:// 需要啟用 `websocket` feature".into(),
                ))
            }
        }
        "wss" => {
            if cfg!(all(feature = "tls", feature = "websocket")) {
                Ok(())
            } else {
                Err(MqttError::UnsupportedFeature(
                    "wss:// 需要同時啟用 `tls` 與 `websocket` feature".into(),
                ))
            }
        }
        _ => Err(MqttError::InvalidConfig("不支援的 broker scheme".into())),
    }
}

fn set_transport_v3(
    _options: &mut rumqttc::MqttOptions,
    scheme: &str,
    _tls: Option<&TlsOptions>,
    #[cfg(feature = "tls")] _tls_client_config: Option<&std::sync::Arc<rustls::ClientConfig>>,
) -> Result<(), MqttError> {
    match scheme {
        "mqtt" => Ok(()),
        #[cfg(feature = "tls")]
        "mqtts" => {
            _options.set_transport(rumqttc::Transport::tls_with_config(tls_configuration(
                _tls,
                _tls_client_config,
            )?));
            Ok(())
        }
        #[cfg(feature = "websocket")]
        "ws" => {
            _options.set_transport(rumqttc::Transport::ws());
            Ok(())
        }
        #[cfg(all(feature = "tls", feature = "websocket"))]
        "wss" => {
            _options.set_transport(rumqttc::Transport::wss_with_config(tls_configuration(
                _tls,
                _tls_client_config,
            )?));
            Ok(())
        }
        _ => Err(MqttError::UnsupportedFeature(format!(
            "目前編譯功能不支援 {scheme}://"
        ))),
    }
}

fn set_transport_v5(
    _options: &mut rumqttc::v5::MqttOptions,
    scheme: &str,
    _tls: Option<&TlsOptions>,
    #[cfg(feature = "tls")] _tls_client_config: Option<&std::sync::Arc<rustls::ClientConfig>>,
) -> Result<(), MqttError> {
    match scheme {
        "mqtt" => Ok(()),
        #[cfg(feature = "tls")]
        "mqtts" => {
            _options.set_transport(rumqttc::Transport::tls_with_config(tls_configuration(
                _tls,
                _tls_client_config,
            )?));
            Ok(())
        }
        #[cfg(feature = "websocket")]
        "ws" => {
            _options.set_transport(rumqttc::Transport::ws());
            Ok(())
        }
        #[cfg(all(feature = "tls", feature = "websocket"))]
        "wss" => {
            _options.set_transport(rumqttc::Transport::wss_with_config(tls_configuration(
                _tls,
                _tls_client_config,
            )?));
            Ok(())
        }
        _ => Err(MqttError::UnsupportedFeature(format!(
            "目前編譯功能不支援 {scheme}://"
        ))),
    }
}

#[cfg(feature = "tls")]
fn tls_configuration(
    options: Option<&TlsOptions>,
    client_config: Option<&std::sync::Arc<rustls::ClientConfig>>,
) -> Result<rumqttc::TlsConfiguration, MqttError> {
    let client_config = match client_config {
        Some(config) => std::sync::Arc::clone(config),
        None => std::sync::Arc::new(tls_config(options)?),
    };
    Ok(rumqttc::TlsConfiguration::Rustls(client_config))
}

#[cfg(feature = "tls")]
fn crypto_provider() -> Result<std::sync::Arc<rustls::crypto::CryptoProvider>, MqttError> {
    if let Some(provider) = rustls::crypto::CryptoProvider::get_default() {
        return Ok(std::sync::Arc::clone(provider));
    }

    #[cfg(feature = "tls-aws-lc")]
    {
        Ok(std::sync::Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
    }

    #[cfg(all(not(feature = "tls-aws-lc"), feature = "tls-ring"))]
    {
        Ok(std::sync::Arc::new(rustls::crypto::ring::default_provider()))
    }

    #[cfg(not(any(feature = "tls-aws-lc", feature = "tls-ring")))]
    {
        Err(MqttError::InvalidConfig(
            "TLS crypto provider 未設定；請啟用 tls-ring 或 tls-aws-lc feature，或於執行檔以 CryptoProvider::install_default() 安裝全域 provider，或改用 tls_client_config() 傳入完整設定".into(),
        ))
    }
}

#[cfg(feature = "tls")]
fn tls_config(options: Option<&TlsOptions>) -> Result<rustls::ClientConfig, MqttError> {
    use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

    let mut roots = rustls::RootCertStore::empty();
    if let Some(ca_pem) = options.and_then(|value| value.ca_pem.as_ref()) {
        let certificates = CertificateDer::pem_slice_iter(ca_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| MqttError::InvalidConfig(format!("自訂 CA PEM 無法解析：{error}")))?;
        if certificates.is_empty() {
            return Err(MqttError::InvalidConfig("自訂 CA PEM 不含憑證".into()));
        }
        for certificate in certificates {
            roots
                .add(certificate)
                .map_err(|error| MqttError::InvalidConfig(format!("自訂 CA 憑證無效：{error}")))?;
        }
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let builder = rustls::ClientConfig::builder_with_provider(crypto_provider()?)
        .with_safe_default_protocol_versions()
        .map_err(|error| {
            MqttError::InvalidConfig(format!("無法設定安全的 TLS protocol versions：{error}"))
        })?
        .with_root_certificates(roots);
    match options.and_then(|value| value.client_auth.as_ref()) {
        Some((certificate_pem, key_pem)) => {
            let certificates = CertificateDer::pem_slice_iter(certificate_pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    MqttError::InvalidConfig(format!("mTLS client certificate 無法解析：{error}"))
                })?;
            if certificates.is_empty() {
                return Err(MqttError::InvalidConfig(
                    "mTLS client certificate PEM 不含憑證".into(),
                ));
            }
            let key = PrivateKeyDer::from_pem_slice(key_pem).map_err(|error| {
                MqttError::InvalidConfig(format!("mTLS private key 無法解析：{error}"))
            })?;
            builder
                .with_client_auth_cert(certificates, key)
                .map_err(|error| {
                    MqttError::InvalidConfig(format!("mTLS certificate/key 不相符：{error}"))
                })
        }
        None => Ok(builder.with_no_client_auth()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_default_and_explicit_ports() {
        for (url, port) in [
            ("mqtt://localhost", 1883),
            ("mqtts://example.com", 8883),
            ("ws://example.com", 80),
            ("wss://example.com", 443),
            ("mqtt://localhost:2883", 2883),
        ] {
            assert_eq!(parse_broker(url).expect("valid URL").port, port);
        }
    }

    #[test]
    fn parses_ipv6_without_brackets() {
        let broker = parse_broker("mqtt://[::1]:1883").expect("valid IPv6 URL");
        assert_eq!(broker.host, "::1");
        assert_eq!(broker.port, 1883);
    }

    #[test]
    fn rejects_bad_urls() {
        for url in [
            "localhost:1883",
            "http://example.com",
            "mqtt:///missing",
            "mqtt://u:p@host",
        ] {
            assert!(parse_broker(url).is_err(), "{url}");
        }
    }

    #[tokio::test]
    async fn validates_builder_values() {
        let result = MqttClient::builder("")
            .broker("mqtt://localhost")
            .connect()
            .await;
        assert!(matches!(result, Err(MqttError::InvalidConfig(_))));

        let result = MqttClient::builder("id")
            .broker("mqtt://localhost")
            .subscription_capacity(0)
            .connect()
            .await;
        assert!(matches!(result, Err(MqttError::InvalidConfig(_))));
    }

    #[cfg(not(feature = "tls"))]
    #[tokio::test]
    async fn mqtts_without_tls_is_an_error() {
        let result = MqttClient::builder("id")
            .broker("mqtts://localhost")
            .connect()
            .await;
        assert!(matches!(result, Err(MqttError::UnsupportedFeature(_))));
    }

    #[cfg(all(
        feature = "tls",
        not(any(feature = "tls-ring", feature = "tls-aws-lc"))
    ))]
    #[tokio::test]
    async fn bare_tls_without_global_provider_is_an_error() {
        let result = MqttClient::builder("id")
            .broker("mqtts://localhost")
            .connect()
            .await;

        match result {
            Err(MqttError::InvalidConfig(message)) => {
                assert!(message.contains("tls-ring 或 tls-aws-lc"));
                assert!(message.contains("CryptoProvider::install_default()"));
                assert!(message.contains("tls_client_config()"));
            }
            _ => panic!("bare tls 應回傳 InvalidConfig"),
        }
    }

    #[cfg(any(feature = "tls-ring", feature = "tls-aws-lc"))]
    fn provider_client_config() -> std::sync::Arc<rustls::ClientConfig> {
        let provider = crypto_provider().expect("TLS provider");
        let config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("TLS provider 應支援 rustls 的安全預設版本")
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        std::sync::Arc::new(config)
    }

    #[cfg(any(feature = "tls-ring", feature = "tls-aws-lc"))]
    #[tokio::test]
    async fn tls_options_and_client_config_are_mutually_exclusive() {
        let result = MqttClient::builder("id")
            .broker("mqtts://localhost")
            .tls(TlsOptions::new())
            .tls_client_config(provider_client_config())
            .connect()
            .await;

        assert!(matches!(result, Err(MqttError::InvalidConfig(_))));
    }

    #[cfg(any(feature = "tls-ring", feature = "tls-aws-lc"))]
    #[tokio::test]
    async fn accepts_complete_tls_client_config() {
        let result = MqttClient::builder("id")
            .broker("mqtts://localhost")
            .tls_client_config(provider_client_config())
            .connect()
            .await;

        assert!(result.is_ok());
    }
}
