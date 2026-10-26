use mqtt_kit_core::{
    LastWill, Message, MqttError, ProtocolVersion, PublishOptions, QoS, RetainHandling,
    SubscribeOptions, V5PublishProps, V5WillProps,
};

#[derive(Clone)]
pub(crate) enum BackendClient {
    V3(rumqttc::AsyncClient),
    V5(rumqttc::v5::AsyncClient),
}

pub(crate) enum BackendEventLoop {
    V3(Box<rumqttc::EventLoop>),
    V5(Box<rumqttc::v5::EventLoop>),
}

pub(crate) enum BackendEvent {
    Connected { session_present: bool },
    Publish(Box<Message>),
    DisconnectSent,
    Other,
}

pub(crate) enum BackendError {
    Authentication(String),
    Protocol(String),
    Connection(String),
}

impl BackendClient {
    pub(crate) async fn publish(&self, options: PublishOptions) -> Result<(), MqttError> {
        match self {
            Self::V3(client) => {
                if options.v5.is_some() {
                    return Err(MqttError::UnsupportedFeature(
                        "MQTT 3.1.1 不支援 PUBLISH properties".into(),
                    ));
                }
                client
                    .publish_bytes(
                        options.topic,
                        qos_v3(options.qos),
                        options.retain,
                        options.payload,
                    )
                    .await
                    .map_err(|error| MqttError::connection("送出 MQTT 3.1.1 PUBLISH 失敗", error))
            }
            Self::V5(client) => {
                let PublishOptions {
                    topic,
                    payload,
                    qos,
                    retain,
                    v5,
                } = options;
                match v5 {
                    Some(properties) => {
                        client
                            .publish_with_properties(
                                topic,
                                qos_v5(qos),
                                retain,
                                payload,
                                publish_properties(properties),
                            )
                            .await
                    }
                    None => client.publish(topic, qos_v5(qos), retain, payload).await,
                }
                .map_err(|error| MqttError::connection("送出 MQTT 5 PUBLISH 失敗", error))
            }
        }
    }

    pub(crate) async fn subscribe(&self, options: &SubscribeOptions) -> Result<(), MqttError> {
        match self {
            Self::V3(client) => {
                if options.uses_v5_features() {
                    return Err(MqttError::UnsupportedFeature(
                        "MQTT 3.1.1 不支援訂閱 properties 或 v5 訂閱旗標".into(),
                    ));
                }
                client
                    .subscribe(options.filter.as_str(), qos_v3(options.qos))
                    .await
                    .map_err(|error| MqttError::connection("送出 MQTT 3.1.1 SUBSCRIBE 失敗", error))
            }
            Self::V5(client) => {
                use rumqttc::v5::mqttbytes::v5::{Filter, SubscribeProperties};

                let filter = Filter {
                    path: options.filter.as_str().to_owned(),
                    qos: qos_v5(options.qos),
                    nolocal: options.no_local,
                    preserve_retain: options.retain_as_published,
                    retain_forward_rule: retain_rule(options.retain_handling),
                };
                let properties = SubscribeProperties {
                    id: options.subscription_identifier,
                    user_properties: options.user_properties.clone(),
                };
                client
                    .subscribe_many_with_properties([filter], properties)
                    .await
                    .map_err(|error| MqttError::connection("送出 MQTT 5 SUBSCRIBE 失敗", error))
            }
        }
    }

    /// 以非阻塞方式排入 SUBSCRIBE。
    ///
    /// 供事件迴圈自身呼叫，避免 request channel 滿載時等待自己排空佇列。
    pub(crate) fn try_subscribe(&self, options: &SubscribeOptions) -> Result<(), MqttError> {
        match self {
            Self::V3(client) => {
                if options.uses_v5_features() {
                    return Err(MqttError::UnsupportedFeature(
                        "MQTT 3.1.1 不支援訂閱 properties 或 v5 訂閱旗標".into(),
                    ));
                }
                client
                    .try_subscribe(options.filter.as_str(), qos_v3(options.qos))
                    .map_err(|error| MqttError::connection("排入 MQTT 3.1.1 SUBSCRIBE 失敗", error))
            }
            Self::V5(client) => {
                use rumqttc::v5::mqttbytes::v5::{Filter, SubscribeProperties};

                let filter = Filter {
                    path: options.filter.as_str().to_owned(),
                    qos: qos_v5(options.qos),
                    nolocal: options.no_local,
                    preserve_retain: options.retain_as_published,
                    retain_forward_rule: retain_rule(options.retain_handling),
                };
                let properties = SubscribeProperties {
                    id: options.subscription_identifier,
                    user_properties: options.user_properties.clone(),
                };
                client
                    .try_subscribe_many_with_properties([filter], properties)
                    .map_err(|error| MqttError::connection("排入 MQTT 5 SUBSCRIBE 失敗", error))
            }
        }
    }

    pub(crate) async fn unsubscribe(&self, filter: &str) -> Result<(), MqttError> {
        match self {
            Self::V3(client) => client
                .unsubscribe(filter)
                .await
                .map_err(|error| MqttError::connection("送出 MQTT 3.1.1 UNSUBSCRIBE 失敗", error)),
            Self::V5(client) => client
                .unsubscribe(filter)
                .await
                .map_err(|error| MqttError::connection("送出 MQTT 5 UNSUBSCRIBE 失敗", error)),
        }
    }

    pub(crate) fn try_unsubscribe(&self, filter: &str) -> Result<(), MqttError> {
        match self {
            Self::V3(client) => client
                .try_unsubscribe(filter)
                .map_err(|error| MqttError::connection("排入 MQTT 3.1.1 UNSUBSCRIBE 失敗", error)),
            Self::V5(client) => client
                .try_unsubscribe(filter)
                .map_err(|error| MqttError::connection("排入 MQTT 5 UNSUBSCRIBE 失敗", error)),
        }
    }

    pub(crate) async fn disconnect(&self) -> Result<(), MqttError> {
        match self {
            Self::V3(client) => client
                .disconnect()
                .await
                .map_err(|error| MqttError::connection("送出 MQTT 3.1.1 DISCONNECT 失敗", error)),
            Self::V5(client) => client
                .disconnect()
                .await
                .map_err(|error| MqttError::connection("送出 MQTT 5 DISCONNECT 失敗", error)),
        }
    }

    /// 以非阻塞方式排入 DISCONNECT。
    ///
    /// 供事件迴圈自身呼叫；不可改用 async 版本，否則 request channel 已滿時
    /// 事件迴圈會等待自己才有辦法排空的 channel 而死鎖。
    pub(crate) fn try_disconnect(&self) -> Result<(), MqttError> {
        match self {
            Self::V3(client) => client
                .try_disconnect()
                .map_err(|error| MqttError::connection("排入 MQTT 3.1.1 DISCONNECT 失敗", error)),
            Self::V5(client) => client
                .try_disconnect()
                .map_err(|error| MqttError::connection("排入 MQTT 5 DISCONNECT 失敗", error)),
        }
    }
}

impl BackendEventLoop {
    pub(crate) async fn poll(&mut self) -> Result<BackendEvent, BackendError> {
        match self {
            Self::V3(event_loop) => match event_loop.poll().await {
                Ok(event) => convert_v3_event(event),
                Err(error) => Err(convert_v3_error(error)),
            },
            Self::V5(event_loop) => match event_loop.poll().await {
                Ok(event) => convert_v5_event(event),
                Err(error) => Err(convert_v5_error(error)),
            },
        }
    }
}

pub(crate) struct BackendConfig<'a> {
    pub(crate) protocol: ProtocolVersion,
    pub(crate) client_id: &'a str,
    pub(crate) host: &'a str,
    pub(crate) port: u16,
    pub(crate) keep_alive: std::time::Duration,
    pub(crate) clean_start: bool,
    pub(crate) credentials: Option<&'a (String, String)>,
    pub(crate) last_will: Option<&'a LastWill>,
    pub(crate) channel_capacity: usize,
}

pub(crate) fn create_backend(
    config: BackendConfig<'_>,
    set_v3_transport: impl FnOnce(&mut rumqttc::MqttOptions) -> Result<(), MqttError>,
    set_v5_transport: impl FnOnce(&mut rumqttc::v5::MqttOptions) -> Result<(), MqttError>,
) -> Result<(BackendClient, BackendEventLoop), MqttError> {
    match config.protocol {
        ProtocolVersion::V311 => {
            if config.last_will.is_some_and(|will| will.v5.is_some()) {
                return Err(MqttError::UnsupportedFeature(
                    "MQTT 3.1.1 不支援遺囑 properties".into(),
                ));
            }
            let mut options = rumqttc::MqttOptions::new(config.client_id, config.host, config.port);
            options
                .set_keep_alive(config.keep_alive)
                .set_clean_session(config.clean_start);
            if let Some((username, password)) = config.credentials {
                options.set_credentials(username, password);
            }
            if let Some(will) = config.last_will {
                options.set_last_will(rumqttc::LastWill {
                    topic: will.topic.clone(),
                    message: will.payload.clone(),
                    qos: qos_v3(will.qos),
                    retain: will.retain,
                });
            }
            set_v3_transport(&mut options)?;
            let (client, event_loop) = rumqttc::AsyncClient::new(options, config.channel_capacity);
            Ok((
                BackendClient::V3(client),
                BackendEventLoop::V3(Box::new(event_loop)),
            ))
        }
        ProtocolVersion::V5 => {
            let mut options =
                rumqttc::v5::MqttOptions::new(config.client_id, config.host, config.port);
            options
                .set_keep_alive(config.keep_alive)
                .set_clean_start(config.clean_start);
            if let Some((username, password)) = config.credentials {
                options.set_credentials(username, password);
            }
            if let Some(will) = config.last_will {
                options.set_last_will(rumqttc::v5::mqttbytes::v5::LastWill {
                    topic: bytes::Bytes::copy_from_slice(will.topic.as_bytes()),
                    message: will.payload.clone(),
                    qos: qos_v5(will.qos),
                    retain: will.retain,
                    properties: will.v5.clone().map(will_properties),
                });
            }
            set_v5_transport(&mut options)?;
            let (client, event_loop) =
                rumqttc::v5::AsyncClient::new(options, config.channel_capacity);
            Ok((
                BackendClient::V5(client),
                BackendEventLoop::V5(Box::new(event_loop)),
            ))
        }
    }
}

fn qos_v3(qos: QoS) -> rumqttc::QoS {
    match qos {
        QoS::AtMostOnce => rumqttc::QoS::AtMostOnce,
        QoS::AtLeastOnce => rumqttc::QoS::AtLeastOnce,
        QoS::ExactlyOnce => rumqttc::QoS::ExactlyOnce,
    }
}

fn qos_v5(qos: QoS) -> rumqttc::v5::mqttbytes::QoS {
    match qos {
        QoS::AtMostOnce => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
        QoS::AtLeastOnce => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
        QoS::ExactlyOnce => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
    }
}

fn from_qos_v3(qos: rumqttc::QoS) -> QoS {
    match qos {
        rumqttc::QoS::AtMostOnce => QoS::AtMostOnce,
        rumqttc::QoS::AtLeastOnce => QoS::AtLeastOnce,
        rumqttc::QoS::ExactlyOnce => QoS::ExactlyOnce,
    }
}

fn from_qos_v5(qos: rumqttc::v5::mqttbytes::QoS) -> QoS {
    match qos {
        rumqttc::v5::mqttbytes::QoS::AtMostOnce => QoS::AtMostOnce,
        rumqttc::v5::mqttbytes::QoS::AtLeastOnce => QoS::AtLeastOnce,
        rumqttc::v5::mqttbytes::QoS::ExactlyOnce => QoS::ExactlyOnce,
    }
}

fn retain_rule(value: RetainHandling) -> rumqttc::v5::mqttbytes::v5::RetainForwardRule {
    use rumqttc::v5::mqttbytes::v5::RetainForwardRule;
    match value {
        RetainHandling::OnEverySubscribe => RetainForwardRule::OnEverySubscribe,
        RetainHandling::OnNewSubscribe => RetainForwardRule::OnNewSubscribe,
        RetainHandling::Never => RetainForwardRule::Never,
    }
}

fn publish_properties(value: V5PublishProps) -> rumqttc::v5::mqttbytes::v5::PublishProperties {
    rumqttc::v5::mqttbytes::v5::PublishProperties {
        payload_format_indicator: value.payload_format_indicator,
        message_expiry_interval: value.message_expiry_interval,
        topic_alias: value.topic_alias,
        response_topic: value.response_topic,
        correlation_data: value.correlation_data,
        user_properties: value.user_properties,
        subscription_identifiers: value.subscription_identifiers,
        content_type: value.content_type,
    }
}

fn from_publish_properties(value: rumqttc::v5::mqttbytes::v5::PublishProperties) -> V5PublishProps {
    V5PublishProps {
        payload_format_indicator: value.payload_format_indicator,
        message_expiry_interval: value.message_expiry_interval,
        topic_alias: value.topic_alias,
        response_topic: value.response_topic,
        correlation_data: value.correlation_data,
        user_properties: value.user_properties,
        subscription_identifiers: value.subscription_identifiers,
        content_type: value.content_type,
    }
}

fn will_properties(value: V5WillProps) -> rumqttc::v5::mqttbytes::v5::LastWillProperties {
    rumqttc::v5::mqttbytes::v5::LastWillProperties {
        delay_interval: value.delay_interval,
        payload_format_indicator: value.payload_format_indicator,
        message_expiry_interval: value.message_expiry_interval,
        content_type: value.content_type,
        response_topic: value.response_topic,
        correlation_data: value.correlation_data,
        user_properties: value.user_properties,
    }
}

fn convert_v3_event(event: rumqttc::Event) -> Result<BackendEvent, BackendError> {
    match event {
        rumqttc::Event::Outgoing(rumqttc::Outgoing::Disconnect) => Ok(BackendEvent::DisconnectSent),
        rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(connack)) => {
            Ok(BackendEvent::Connected {
                session_present: connack.session_present,
            })
        }
        rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish)) => {
            Ok(BackendEvent::Publish(Box::new(Message {
                topic: publish.topic,
                payload: publish.payload,
                qos: from_qos_v3(publish.qos),
                retain: publish.retain,
                v5: None,
            })))
        }
        _ => Ok(BackendEvent::Other),
    }
}

fn convert_v5_event(event: rumqttc::v5::Event) -> Result<BackendEvent, BackendError> {
    use rumqttc::v5::mqttbytes::v5::Packet;
    match event {
        rumqttc::v5::Event::Outgoing(rumqttc::Outgoing::Disconnect) => {
            Ok(BackendEvent::DisconnectSent)
        }
        rumqttc::v5::Event::Incoming(Packet::ConnAck(connack)) => Ok(BackendEvent::Connected {
            session_present: connack.session_present,
        }),
        rumqttc::v5::Event::Incoming(Packet::Publish(publish)) => {
            let topic = std::str::from_utf8(&publish.topic)
                .map_err(|error| {
                    BackendError::Protocol(format!("PUBLISH topic 不是 UTF-8：{error}"))
                })?
                .to_owned();
            Ok(BackendEvent::Publish(Box::new(Message {
                topic,
                payload: publish.payload,
                qos: from_qos_v5(publish.qos),
                retain: publish.retain,
                v5: publish.properties.map(from_publish_properties),
            })))
        }
        _ => Ok(BackendEvent::Other),
    }
}

fn convert_v3_error(error: rumqttc::ConnectionError) -> BackendError {
    match error {
        rumqttc::ConnectionError::ConnectionRefused(code) => match code {
            rumqttc::ConnectReturnCode::BadUserNamePassword
            | rumqttc::ConnectReturnCode::NotAuthorized => {
                BackendError::Authentication(format!("{code:?}"))
            }
            rumqttc::ConnectReturnCode::ServiceUnavailable => {
                BackendError::Connection(format!("broker 暫時拒絕連線：{code:?}"))
            }
            _ => BackendError::Protocol(format!("broker 拒絕連線：{code:?}")),
        },
        other => BackendError::Connection(other.to_string()),
    }
}

fn convert_v5_error(error: rumqttc::v5::ConnectionError) -> BackendError {
    match error {
        rumqttc::v5::ConnectionError::ConnectionRefused(code) => match code {
            rumqttc::v5::mqttbytes::v5::ConnectReturnCode::BadUserNamePassword
            | rumqttc::v5::mqttbytes::v5::ConnectReturnCode::NotAuthorized
            | rumqttc::v5::mqttbytes::v5::ConnectReturnCode::BadAuthenticationMethod
            | rumqttc::v5::mqttbytes::v5::ConnectReturnCode::Banned => {
                BackendError::Authentication(format!("{code:?}"))
            }
            rumqttc::v5::mqttbytes::v5::ConnectReturnCode::ServerUnavailable
            | rumqttc::v5::mqttbytes::v5::ConnectReturnCode::ServerBusy
            | rumqttc::v5::mqttbytes::v5::ConnectReturnCode::QuotaExceeded
            | rumqttc::v5::mqttbytes::v5::ConnectReturnCode::ConnectionRateExceeded => {
                BackendError::Connection(format!("broker 暫時拒絕連線：{code:?}"))
            }
            _ => BackendError::Protocol(format!("broker 拒絕連線：{code:?}")),
        },
        other => BackendError::Connection(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v3_server_unavailable_is_retryable() {
        let error = rumqttc::ConnectionError::ConnectionRefused(
            rumqttc::ConnectReturnCode::ServiceUnavailable,
        );
        assert!(matches!(
            convert_v3_error(error),
            BackendError::Connection(_)
        ));
    }

    #[test]
    fn v5_temporary_refusals_are_retryable() {
        use rumqttc::v5::mqttbytes::v5::ConnectReturnCode;

        for code in [
            ConnectReturnCode::ServerUnavailable,
            ConnectReturnCode::ServerBusy,
            ConnectReturnCode::QuotaExceeded,
            ConnectReturnCode::ConnectionRateExceeded,
        ] {
            let error = rumqttc::v5::ConnectionError::ConnectionRefused(code);
            assert!(matches!(
                convert_v5_error(error),
                BackendError::Connection(_)
            ));
        }
    }

    #[test]
    fn v5_banned_is_an_authentication_failure() {
        let error = rumqttc::v5::ConnectionError::ConnectionRefused(
            rumqttc::v5::mqttbytes::v5::ConnectReturnCode::Banned,
        );
        assert!(matches!(
            convert_v5_error(error),
            BackendError::Authentication(_)
        ));
    }
}
