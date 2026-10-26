use crate::operations::{Completion, ConfirmationStage, Kind, Operations, Pending};
use mqtt_kit_core::{
    LastWill, Message, MqttError, ProtocolVersion, PublishOptions, QoS, RetainHandling,
    SubscribeOptions, V5PublishProps, V5WillProps,
};
use std::sync::Arc;

#[derive(Clone)]
enum Sender {
    V3(flume::Sender<rumqttc::Request>),
    V5(flume::Sender<rumqttc::v5::Request>),
}

enum WireRequest {
    V3(rumqttc::Request),
    V5(rumqttc::v5::Request),
}

impl WireRequest {
    fn kind(&self) -> Kind {
        match self {
            Self::V3(rumqttc::Request::Publish(_)) | Self::V5(rumqttc::v5::Request::Publish(_)) => {
                Kind::Publish
            }
            Self::V3(rumqttc::Request::Subscribe(_))
            | Self::V5(rumqttc::v5::Request::Subscribe(_)) => Kind::Subscribe,
            Self::V3(rumqttc::Request::Unsubscribe(_))
            | Self::V5(rumqttc::v5::Request::Unsubscribe(_)) => Kind::Unsubscribe,
            _ => unreachable!("only tracked requests use WireRequest"),
        }
    }
}

#[derive(Clone)]
pub(crate) struct BackendClient {
    sender: Sender,
    pub(crate) operations: Arc<Operations>,
}

pub(crate) enum BackendEventLoop {
    V3(
        Box<rumqttc::EventLoop>,
        flume::Receiver<rumqttc::Request>,
        bool,
    ),
    V5(
        Box<rumqttc::v5::EventLoop>,
        flume::Receiver<rumqttc::v5::Request>,
        bool,
    ),
}

pub(crate) enum BackendEvent {
    Connected {
        session_present: bool,
    },
    Publish(Box<Message>),
    DisconnectSent,
    Outgoing {
        kind: Kind,
        id: u16,
    },
    Acknowledged {
        kind: Kind,
        id: u16,
        ack: ConfirmationStage,
        result: Result<(), MqttError>,
    },
    Other,
}

pub(crate) enum BackendError {
    Authentication(String),
    Protocol(String),
    Connection(String),
}

impl BackendClient {
    fn new(sender: Sender, capacity: usize) -> Self {
        Self {
            sender,
            operations: Arc::new(Operations::new(capacity)),
        }
    }
    pub(crate) async fn publish(&self, options: PublishOptions) -> Result<(), MqttError> {
        self.publish_tracked(options, None).await
    }
    pub(crate) async fn publish_tracked(
        &self,
        options: PublishOptions,
        completion: Option<Completion>,
    ) -> Result<(), MqttError> {
        let expected = if options.qos == QoS::ExactlyOnce {
            ConfirmationStage::PubComp
        } else {
            ConfirmationStage::PubAck
        };
        let request = match &self.sender {
            Sender::V3(_) => {
                if options.v5.is_some() {
                    return Err(MqttError::UnsupportedFeature(
                        "MQTT 3.1.1 不支援 PUBLISH properties".into(),
                    ));
                }
                let mut publish = rumqttc::mqttbytes::v4::Publish::from_bytes(
                    options.topic,
                    qos_v3(options.qos),
                    options.payload,
                );
                publish.retain = options.retain;
                WireRequest::V3(rumqttc::Request::Publish(publish))
            }
            Sender::V5(_) => {
                let mut publish = rumqttc::v5::mqttbytes::v5::Publish::new(
                    options.topic,
                    qos_v5(options.qos),
                    options.payload,
                    options.v5.map(publish_properties),
                );
                publish.retain = options.retain;
                WireRequest::V5(rumqttc::v5::Request::Publish(publish))
            }
        };
        self.send(
            request,
            Pending {
                retransmission: false,
                expected,
                completion,
            },
        )
        .await
    }
    fn subscription_request(&self, options: &SubscribeOptions) -> Result<WireRequest, MqttError> {
        Ok(match &self.sender {
            Sender::V3(_) => {
                if options.uses_v5_features() {
                    return Err(MqttError::UnsupportedFeature(
                        "MQTT 3.1.1 不支援訂閱 properties 或 v5 訂閱旗標".into(),
                    ));
                }
                WireRequest::V3(rumqttc::Request::Subscribe(
                    rumqttc::mqttbytes::v4::Subscribe::new(
                        options.filter.as_str(),
                        qos_v3(options.qos),
                    ),
                ))
            }
            Sender::V5(_) => {
                use rumqttc::v5::mqttbytes::v5::{Filter, Subscribe, SubscribeProperties};
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
                WireRequest::V5(rumqttc::v5::Request::Subscribe(Subscribe::new_many(
                    [filter],
                    Some(properties),
                )))
            }
        })
    }
    pub(crate) fn try_subscribe_tracked(
        &self,
        options: &SubscribeOptions,
        completion: &mut Option<Completion>,
    ) -> Result<(), MqttError> {
        let mut request = Some(self.subscription_request(options)?);
        let mut pending = Some(Pending {
            retransmission: false,
            expected: ConfirmationStage::SubAck,
            completion: completion.take(),
        });
        let result = self.attempt(&mut request, &mut pending);
        if let Some(pending) = pending {
            *completion = pending.completion;
        }
        if result? {
            Ok(())
        } else {
            Err(MqttError::ResourceLimit("request/ACK queue 已滿".into()))
        }
    }
    pub(crate) fn try_subscribe(&self, options: &SubscribeOptions) -> Result<(), MqttError> {
        self.try_send(
            self.subscription_request(options)?,
            Pending {
                retransmission: false,
                expected: ConfirmationStage::SubAck,
                completion: None,
            },
        )
    }
    fn unsubscription_request(&self, filter: &str) -> WireRequest {
        match &self.sender {
            Sender::V3(_) => WireRequest::V3(rumqttc::Request::Unsubscribe(
                rumqttc::mqttbytes::v4::Unsubscribe::new(filter),
            )),
            Sender::V5(_) => WireRequest::V5(rumqttc::v5::Request::Unsubscribe(
                rumqttc::v5::mqttbytes::v5::Unsubscribe::new(filter, None),
            )),
        }
    }
    pub(crate) fn try_unsubscribe_tracked(
        &self,
        filter: &str,
        completion: Option<Completion>,
    ) -> Result<(), MqttError> {
        self.try_send(
            self.unsubscription_request(filter),
            Pending {
                retransmission: false,
                expected: ConfirmationStage::UnsubAck,
                completion,
            },
        )
    }
    pub(crate) fn try_unsubscribe(&self, filter: &str) -> Result<(), MqttError> {
        self.try_send(
            self.unsubscription_request(filter),
            Pending {
                retransmission: false,
                expected: ConfirmationStage::UnsubAck,
                completion: None,
            },
        )
    }
    pub(crate) fn try_disconnect(&self) -> Result<(), MqttError> {
        // 控制要求不受 ACK 追蹤容量限制，避免關閉等待自己的 ACK。
        match &self.sender {
            Sender::V3(sender) => sender
                .try_send(rumqttc::Request::Disconnect(
                    rumqttc::mqttbytes::v4::Disconnect,
                ))
                .map_err(|_| MqttError::ClientClosed),
            Sender::V5(sender) => sender
                .try_send(rumqttc::v5::Request::Disconnect)
                .map_err(|_| MqttError::ClientClosed),
        }
    }
    // try_send + 追蹤紀錄在同一把鎖下完成；取消 future 不會留下未送出的佔位紀錄。
    fn attempt(
        &self,
        request: &mut Option<WireRequest>,
        pending: &mut Option<Pending>,
    ) -> Result<bool, MqttError> {
        let mut state = self.operations.lock();
        if state.closed {
            return Err(MqttError::ClientClosed);
        }
        let kind = request
            .as_ref()
            .expect("request retained until sent")
            .kind();
        if !state.has_capacity_for(kind) {
            return Ok(false);
        }
        let result = match (
            &self.sender,
            request.take().expect("request retained until sent"),
        ) {
            (Sender::V3(sender), WireRequest::V3(request)) => {
                sender.try_send(request).map_err(|error| match error {
                    flume::TrySendError::Full(request) => (WireRequest::V3(request), false),
                    flume::TrySendError::Disconnected(request) => (WireRequest::V3(request), true),
                })
            }
            (Sender::V5(sender), WireRequest::V5(request)) => {
                sender.try_send(request).map_err(|error| match error {
                    flume::TrySendError::Full(request) => (WireRequest::V5(request), false),
                    flume::TrySendError::Disconnected(request) => (WireRequest::V5(request), true),
                })
            }
            _ => unreachable!("request protocol must match sender"),
        };
        match result {
            Ok(()) => {
                state.enqueue(kind, pending.take().expect("pending retained until sent"));
                Ok(true)
            }
            Err((returned, closed)) => {
                *request = Some(returned);
                if closed {
                    Err(MqttError::ClientClosed)
                } else {
                    Ok(false)
                }
            }
        }
    }
    fn try_send(&self, request: WireRequest, pending: Pending) -> Result<(), MqttError> {
        if self.attempt(&mut Some(request), &mut Some(pending))? {
            Ok(())
        } else {
            Err(MqttError::ResourceLimit("request/ACK queue 已滿".into()))
        }
    }
    async fn send(&self, request: WireRequest, pending: Pending) -> Result<(), MqttError> {
        let mut request = Some(request);
        let mut pending = Some(pending);
        loop {
            let notified = self.operations.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.attempt(&mut request, &mut pending)? {
                return Ok(());
            }
            notified.await;
        }
    }
    fn observe(&self, event: BackendEvent) -> Result<BackendEvent, BackendError> {
        match event {
            BackendEvent::Outgoing { kind, id } => {
                if !self.operations.outgoing(kind, id) {
                    return Err(BackendError::Protocol(
                        "尚未確認的 packet ID 被再次用於新要求".into(),
                    ));
                }
                Ok(BackendEvent::Other)
            }
            BackendEvent::Acknowledged {
                kind,
                id,
                ack,
                result,
            } => {
                if let Err(error) = &result {
                    tracing::warn!(operation = kind.name(), %error, "Broker 拒絕要求");
                }
                self.operations.acknowledge(kind, id, ack, result);
                Ok(BackendEvent::Other)
            }
            BackendEvent::Connected { session_present } => {
                self.operations.connected(session_present);
                Ok(BackendEvent::Connected { session_present })
            }
            other => Ok(other),
        }
    }
}

impl BackendEventLoop {
    pub(crate) async fn poll(
        &mut self,
        backend: &BackendClient,
    ) -> Result<BackendEvent, BackendError> {
        // 只在已連線且 pending 為空時移入一筆要求；維持 rumqttc 的 inflight 控制，
        // 不將有界 channel 一次搬進無界 pending，也不複製 Bytes payload。
        loop {
            let result = match self {
                Self::V3(event_loop, requests, connected) => {
                    let result = if *connected && event_loop.pending.is_empty() {
                        tokio::select! {
                            result = event_loop.poll() => result,
                            request = requests.recv_async() => {
                                event_loop.pending.push_back(request.map_err(|_| BackendError::Connection("要求通道已關閉".into()))?);
                                continue;
                            }
                        }
                    } else {
                        event_loop.poll().await
                    };
                    if result.is_err() {
                        *connected = false;
                        backend
                            .operations
                            .disconnected(event_loop.pending.iter().filter_map(pending_v3));
                    }
                    let result = result.map_err(convert_v3_error).and_then(convert_v3_event);
                    if matches!(result, Ok(BackendEvent::Connected { .. })) {
                        *connected = true;
                    }
                    result
                }
                Self::V5(event_loop, requests, connected) => {
                    let result = if *connected && event_loop.pending.is_empty() {
                        tokio::select! {
                            result = event_loop.poll() => result,
                            request = requests.recv_async() => {
                                event_loop.pending.push_back(request.map_err(|_| BackendError::Connection("要求通道已關閉".into()))?);
                                continue;
                            }
                        }
                    } else {
                        event_loop.poll().await
                    };
                    if result.is_err() {
                        *connected = false;
                        backend
                            .operations
                            .disconnected(event_loop.pending.iter().filter_map(pending_v5));
                    }
                    let result = result.map_err(convert_v5_error).and_then(convert_v5_event);
                    if matches!(result, Ok(BackendEvent::Connected { .. })) {
                        *connected = true;
                    }
                    result
                }
            };
            let rejected_qos2 = matches!(
                &result,
                Ok(BackendEvent::Acknowledged {
                    kind: Kind::Publish,
                    ack: ConfirmationStage::PubComp,
                    result: Err(_),
                    ..
                })
            );
            let observed = result.and_then(|event| backend.observe(event));
            if rejected_qos2 && observed.is_ok() {
                // rumqttc 0.25.1 在否定 PUBREC/PUBCOMP 後未釋放 inflight 計數。
                // 先交付 BrokerRejected，再重新建立連線，避免長期耗盡容量。
                match self {
                    Self::V3(event_loop, _, connected) => {
                        event_loop.clean();
                        *connected = false;
                        backend
                            .operations
                            .disconnected(event_loop.pending.iter().filter_map(pending_v3));
                    }
                    Self::V5(event_loop, _, connected) => {
                        event_loop.clean();
                        *connected = false;
                        backend
                            .operations
                            .disconnected(event_loop.pending.iter().filter_map(pending_v5));
                    }
                }
                return Err(BackendError::Connection(
                    "QoS2 要求被拒絕，重新建立協定狀態".into(),
                ));
            }
            return observed;
        }
    }
}

fn pending_v3(request: &rumqttc::Request) -> Option<(Kind, u16)> {
    match request {
        rumqttc::Request::Publish(p) => Some((Kind::Publish, p.pkid)),
        rumqttc::Request::Subscribe(_) => Some((Kind::Subscribe, 0)),
        rumqttc::Request::Unsubscribe(_) => Some((Kind::Unsubscribe, 0)),
        _ => None,
    }
}
fn pending_v5(request: &rumqttc::v5::Request) -> Option<(Kind, u16)> {
    match request {
        rumqttc::v5::Request::Publish(p) => Some((Kind::Publish, p.pkid)),
        rumqttc::v5::Request::Subscribe(_) => Some((Kind::Subscribe, 0)),
        rumqttc::v5::Request::Unsubscribe(_) => Some((Kind::Unsubscribe, 0)),
        _ => None,
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
            let event_loop = rumqttc::EventLoop::new(options, config.channel_capacity);
            let (sender, receiver) = flume::bounded(config.channel_capacity);
            Ok((
                BackendClient::new(Sender::V3(sender), config.channel_capacity),
                BackendEventLoop::V3(Box::new(event_loop), receiver, false),
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
            let event_loop = rumqttc::v5::EventLoop::new(options, config.channel_capacity);
            let (sender, receiver) = flume::bounded(config.channel_capacity);
            Ok((
                BackendClient::new(Sender::V5(sender), config.channel_capacity),
                BackendEventLoop::V5(Box::new(event_loop), receiver, false),
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

fn outgoing(event: rumqttc::Outgoing) -> BackendEvent {
    match event {
        rumqttc::Outgoing::Publish(id) => BackendEvent::Outgoing {
            kind: Kind::Publish,
            id,
        },
        rumqttc::Outgoing::Subscribe(id) => BackendEvent::Outgoing {
            kind: Kind::Subscribe,
            id,
        },
        rumqttc::Outgoing::Unsubscribe(id) => BackendEvent::Outgoing {
            kind: Kind::Unsubscribe,
            id,
        },
        rumqttc::Outgoing::Disconnect => BackendEvent::DisconnectSent,
        _ => BackendEvent::Other,
    }
}

fn acknowledged(
    kind: Kind,
    id: u16,
    ack: ConfirmationStage,
    accepted: bool,
    reason: impl std::fmt::Debug,
) -> BackendEvent {
    BackendEvent::Acknowledged {
        kind,
        id,
        ack,
        result: if accepted {
            Ok(())
        } else {
            Err(MqttError::BrokerRejected {
                operation: kind.name(),
                reason: format!("{reason:?}"),
            })
        },
    }
}

fn convert_v3_event(event: rumqttc::Event) -> Result<BackendEvent, BackendError> {
    use rumqttc::mqttbytes::v4::{Packet, SubscribeReasonCode};
    Ok(match event {
        rumqttc::Event::Outgoing(event) => outgoing(event),
        rumqttc::Event::Incoming(Packet::ConnAck(connack)) => BackendEvent::Connected {
            session_present: connack.session_present,
        },
        rumqttc::Event::Incoming(Packet::Publish(publish)) => {
            BackendEvent::Publish(Box::new(Message {
                topic: publish.topic,
                payload: publish.payload,
                qos: from_qos_v3(publish.qos),
                retain: publish.retain,
                v5: None,
            }))
        }
        rumqttc::Event::Incoming(Packet::PubAck(ack)) => acknowledged(
            Kind::Publish,
            ack.pkid,
            ConfirmationStage::PubAck,
            true,
            "Success",
        ),
        rumqttc::Event::Incoming(Packet::PubComp(ack)) => acknowledged(
            Kind::Publish,
            ack.pkid,
            ConfirmationStage::PubComp,
            true,
            "Success",
        ),
        rumqttc::Event::Incoming(Packet::SubAck(ack)) => acknowledged(
            Kind::Subscribe,
            ack.pkid,
            ConfirmationStage::SubAck,
            matches!(
                ack.return_codes.as_slice(),
                [SubscribeReasonCode::Success(_)]
            ),
            ack.return_codes,
        ),
        rumqttc::Event::Incoming(Packet::UnsubAck(ack)) => acknowledged(
            Kind::Unsubscribe,
            ack.pkid,
            ConfirmationStage::UnsubAck,
            true,
            "Success",
        ),
        _ => BackendEvent::Other,
    })
}

fn convert_v5_event(event: rumqttc::v5::Event) -> Result<BackendEvent, BackendError> {
    use rumqttc::v5::mqttbytes::v5::{
        Packet, PubAckReason, PubCompReason, PubRecReason, SubscribeReasonCode, UnsubAckReason,
    };
    Ok(match event {
        rumqttc::v5::Event::Outgoing(event) => outgoing(event),
        rumqttc::v5::Event::Incoming(Packet::ConnAck(connack)) => BackendEvent::Connected {
            session_present: connack.session_present,
        },
        rumqttc::v5::Event::Incoming(Packet::Publish(publish)) => {
            let topic = std::str::from_utf8(&publish.topic)
                .map_err(|error| {
                    BackendError::Protocol(format!("PUBLISH topic 不是 UTF-8：{error}"))
                })?
                .to_owned();
            BackendEvent::Publish(Box::new(Message {
                topic,
                payload: publish.payload,
                qos: from_qos_v5(publish.qos),
                retain: publish.retain,
                v5: publish.properties.map(from_publish_properties),
            }))
        }
        rumqttc::v5::Event::Incoming(Packet::PubAck(ack)) => acknowledged(
            Kind::Publish,
            ack.pkid,
            ConfirmationStage::PubAck,
            matches!(
                ack.reason,
                PubAckReason::Success | PubAckReason::NoMatchingSubscribers
            ),
            ack.reason,
        ),
        rumqttc::v5::Event::Incoming(Packet::PubComp(ack)) => acknowledged(
            Kind::Publish,
            ack.pkid,
            ConfirmationStage::PubComp,
            ack.reason == PubCompReason::Success,
            ack.reason,
        ),
        rumqttc::v5::Event::Incoming(Packet::PubRec(ack))
            if !matches!(
                ack.reason,
                PubRecReason::Success | PubRecReason::NoMatchingSubscribers
            ) =>
        {
            acknowledged(
                Kind::Publish,
                ack.pkid,
                ConfirmationStage::PubComp,
                false,
                ack.reason,
            )
        }
        rumqttc::v5::Event::Incoming(Packet::SubAck(ack)) => acknowledged(
            Kind::Subscribe,
            ack.pkid,
            ConfirmationStage::SubAck,
            matches!(
                ack.return_codes.as_slice(),
                [SubscribeReasonCode::Success(_)]
            ),
            ack.return_codes,
        ),
        rumqttc::v5::Event::Incoming(Packet::UnsubAck(ack)) => acknowledged(
            Kind::Unsubscribe,
            ack.pkid,
            ConfirmationStage::UnsubAck,
            matches!(
                ack.reasons.as_slice(),
                [UnsubAckReason::Success | UnsubAckReason::NoSubscriptionExisted]
            ),
            ack.reasons,
        ),
        _ => BackendEvent::Other,
    })
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
impl BackendClient {
    pub(crate) fn test_channel(capacity: usize) -> (Self, flume::Receiver<rumqttc::Request>) {
        let (sender, receiver) = flume::bounded(capacity);
        (Self::new(Sender::V3(sender), capacity), receiver)
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
