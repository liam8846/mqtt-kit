use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use bytes::Bytes;
use mqtt_kit_core::{
    ConnectionEvent, ConnectionState, MqttError, ProtocolVersion, PublishOptions, QoS,
    ReconnectPolicy, SubscribeOptions, TopicFilter,
};
use tokio::{
    sync::{Mutex, broadcast, watch},
    task::JoinHandle,
};

use crate::{
    Subscription,
    backend::{BackendClient, BackendEventLoop},
    builder::{MqttClientBuilder, validate_publish_topic},
    event_loop::run_event_loop,
    subscription::SubscriptionRegistry,
};

pub(crate) struct ClientInner {
    pub(crate) backend: BackendClient,
    pub(crate) protocol: ProtocolVersion,
    pub(crate) registry: SubscriptionRegistry,
    pub(crate) events: broadcast::Sender<ConnectionEvent>,
    pub(crate) state: watch::Sender<ConnectionState>,
    pub(crate) closed: AtomicBool,
    pub(crate) task: Mutex<Option<JoinHandle<()>>>,
    pub(crate) next_subscription_id: AtomicU64,
    pub(crate) subscription_capacity: usize,
}

struct HandleGuard {
    shutdown: watch::Sender<bool>,
}

/// 可 Clone、Send、Sync 的 MQTT client handle。
///
/// 每個 clone 共用同一個背景事件迴圈、連線狀態與訂閱註冊表。
/// 最後一個 handle 被 Drop 時，背景任務會嘗試送出 DISCONNECT 後關閉；但若
/// Tokio runtime 隨即結束，背景任務可能在封包送達前被取消。需要確定性的
/// 優雅斷線時，請呼叫 [`MqttClient::disconnect`] 並等待其完成。
#[derive(Clone)]
pub struct MqttClient {
    pub(crate) inner: Arc<ClientInner>,
    guard: Arc<HandleGuard>,
}

impl MqttClient {
    /// 建立 client builder。
    #[must_use]
    pub fn builder(client_id: impl Into<String>) -> MqttClientBuilder {
        MqttClientBuilder::new(client_id)
    }

    /// 發佈訊息；傳入 `Bytes` 時 payload 不會被複製。
    pub async fn publish(
        &self,
        topic: impl Into<String>,
        payload: impl Into<Bytes>,
        qos: QoS,
    ) -> Result<(), MqttError> {
        self.publish_with(PublishOptions::new(topic, payload, qos))
            .await
    }

    /// 使用完整選項發佈訊息。
    pub async fn publish_with(&self, options: PublishOptions) -> Result<(), MqttError> {
        self.ensure_open()?;
        validate_publish_topic(&options.topic)?;
        self.inner.backend.publish(options).await
    }

    /// 建立 topic filter 訂閱並回傳訊息串流。
    pub async fn subscribe(
        &self,
        filter: impl Into<String>,
        qos: QoS,
    ) -> Result<Subscription, MqttError> {
        self.subscribe_with(SubscribeOptions::new(filter, qos)?)
            .await
    }

    /// 使用完整選項建立訂閱並回傳訊息串流。
    pub async fn subscribe_with(
        &self,
        options: SubscribeOptions,
    ) -> Result<Subscription, MqttError> {
        self.ensure_open()?;
        if self.inner.protocol == ProtocolVersion::V311 && options.uses_v5_features() {
            return Err(MqttError::UnsupportedFeature(
                "MQTT 3.1.1 不支援訂閱 properties 或 v5 訂閱旗標".into(),
            ));
        }

        let id = self
            .inner
            .next_subscription_id
            .fetch_add(1, Ordering::Relaxed);
        let filter = options.filter.clone();
        let queue =
            self.inner
                .registry
                .insert(id, options.clone(), self.inner.subscription_capacity);
        if let Err(error) = self.inner.backend.subscribe(&options).await {
            self.inner.registry.remove(&filter, id);
            return Err(error);
        }
        Ok(Subscription::new(
            id,
            filter,
            queue,
            Arc::downgrade(&self.inner),
        ))
    }

    /// 取消相同 filter 的所有本地訂閱並向 broker 送出 UNSUBSCRIBE。
    pub async fn unsubscribe(&self, filter: impl Into<String>) -> Result<(), MqttError> {
        self.ensure_open()?;
        let filter = TopicFilter::new(filter)?;
        self.inner.registry.remove_filter(&filter);
        self.inner.backend.unsubscribe(filter.as_str()).await
    }

    /// 訂閱連線生命週期事件。
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<ConnectionEvent> {
        self.inner.events.subscribe()
    }

    /// 取得可持續監看最新連線狀態的 receiver。
    #[must_use]
    pub fn state(&self) -> watch::Receiver<ConnectionState> {
        self.inner.state.subscribe()
    }

    /// 送出 DISCONNECT 並等待背景任務停止。
    ///
    /// 這是在已連線時唯一能保證等待 DISCONNECT 送達的關閉方式。
    ///
    /// 多個 task 併發呼叫時，每個呼叫者都會等到狀態成為
    /// [`ConnectionState::Closed`] 才返回。
    pub async fn disconnect(&self) -> Result<(), MqttError> {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            let mut state = self.inner.state.subscribe();
            loop {
                if *state.borrow() == ConnectionState::Closed {
                    return Ok(());
                }
                if state.changed().await.is_err() {
                    return Ok(());
                }
            }
        }
        let disconnect_result = self.inner.backend.disconnect().await;
        self.guard.shutdown.send_replace(true);
        let task = self.inner.task.lock().await.take();
        if let Some(task) = task {
            task.await
                .map_err(|error| MqttError::BackgroundTask(error.to_string()))?;
        }
        disconnect_result
    }

    fn ensure_open(&self) -> Result<(), MqttError> {
        if self.inner.is_closed() {
            Err(MqttError::ClientClosed)
        } else {
            Ok(())
        }
    }
}

impl ClientInner {
    pub(crate) fn start(
        backend: BackendClient,
        event_loop: BackendEventLoop,
        protocol: ProtocolVersion,
        reconnect: ReconnectPolicy,
        subscription_capacity: usize,
    ) -> MqttClient {
        let (events, _) = broadcast::channel(64);
        let (state, _) = watch::channel(ConnectionState::Connecting);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let inner = Arc::new(Self {
            backend,
            protocol,
            registry: SubscriptionRegistry::default(),
            events,
            state,
            closed: AtomicBool::new(false),
            task: Mutex::new(None),
            next_subscription_id: AtomicU64::new(1),
            subscription_capacity,
        });
        let task_inner = Arc::clone(&inner);
        let task = tokio::spawn(async move {
            run_event_loop(task_inner, event_loop, reconnect, shutdown_receiver).await;
        });
        match inner.task.try_lock() {
            Ok(mut slot) => *slot = Some(task),
            Err(_) => {
                task.abort();
                inner.closed.store(true, Ordering::Release);
                inner.state.send_replace(ConnectionState::Closed);
            }
        }
        MqttClient {
            inner,
            guard: Arc::new(HandleGuard { shutdown }),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.state.send_replace(ConnectionState::Closed);
        self.registry.close_all();
    }
}
