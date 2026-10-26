use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use mqtt_kit_core::{
    ConnectionEvent, ConnectionState, MqttError, ProtocolVersion, PublishOptions, QoS,
    ReconnectPolicy, SubscribeOptions, TopicFilter,
};
use tokio::sync::{broadcast, oneshot, watch};

use crate::operations::Completion;

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
    pub(crate) shutdown_timed_out: AtomicBool,
    background_error: std::sync::Mutex<Option<String>>,
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
        self.subscribe_inner(options, None).await
    }

    async fn subscribe_inner(
        &self,
        options: SubscribeOptions,
        mut completion: Option<Completion>,
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
                .insert(id, options.clone(), self.inner.subscription_capacity)?;
        // 在 await 前交付清理責任；future 被取消時也會執行 Subscription::drop。
        let subscription = Subscription::new(id, filter, queue, Arc::downgrade(&self.inner));
        loop {
            let notified = self.inner.backend.operations.changed.notified();
            let registry_changed = self.inner.registry.changed.notified();
            tokio::pin!(notified, registry_changed);
            notified.as_mut().enable();
            registry_changed.as_mut().enable();
            self.ensure_open()?;
            let result =
                self.inner
                    .registry
                    .enqueue_subscription(subscription.filter(), id, |current| {
                        self.inner
                            .backend
                            .try_subscribe_tracked(current, &mut completion)
                    });
            match result {
                Ok(()) => break,
                Err(MqttError::ResourceLimit(_)) => {
                    tokio::select! { _ = notified => {}, _ = registry_changed => {} }
                }
                Err(error) => return Err(error),
            }
        }
        self.ensure_open()?;
        Ok(subscription)
    }

    /// 等待 QoS1 PUBACK 或 QoS2 PUBCOMP；逾時涵蓋排隊與確認。
    ///
    /// QoS0 沒有 broker ACK，因此回傳 UnsupportedFeature。逾時或取消只停止等待，
    /// 不會撤回已排入的發佈；斷線時回傳 Connection，訊息可能已送達。
    pub async fn publish_confirmed(
        &self,
        options: PublishOptions,
        timeout: Duration,
    ) -> Result<(), MqttError> {
        self.ensure_open()?;
        validate_publish_topic(&options.topic)?;
        if options.qos == QoS::AtMostOnce {
            return Err(MqttError::UnsupportedFeature(
                "QoS0 沒有 broker ACK，請使用 publish()".into(),
            ));
        }
        tokio::time::timeout(timeout, async {
            let (completion, receipt) = oneshot::channel();
            self.inner
                .backend
                .publish_tracked(options, Some(completion))
                .await?;
            receipt.await.map_err(|_| MqttError::ClientClosed)?
        })
        .await
        .map_err(|_| MqttError::Timeout)?
    }

    /// 建立訂閱並等待成功 SUBACK；拒絕、逾時或取消會回收本地訂閱。
    pub async fn subscribe_confirmed(
        &self,
        options: SubscribeOptions,
        timeout: Duration,
    ) -> Result<Subscription, MqttError> {
        tokio::time::timeout(timeout, async {
            let (completion, receipt) = oneshot::channel();
            let subscription = self.subscribe_inner(options, Some(completion)).await?;
            receipt.await.map_err(|_| MqttError::ClientClosed)??;
            self.ensure_open()?;
            Ok(subscription)
        })
        .await
        .map_err(|_| MqttError::Timeout)?
    }

    /// 排入 UNSUBSCRIBE 後取消相同 filter 的所有本地訂閱。
    /// 等待佇列期間取消 future 不會移除本地訂閱。
    pub async fn unsubscribe(&self, filter: impl Into<String>) -> Result<(), MqttError> {
        self.unsubscribe_inner(TopicFilter::new(filter)?, false)
            .await?;
        Ok(())
    }

    /// 取消相同 filter 的所有本地訂閱並等待 UNSUBACK。
    /// 成功排入後，取消或逾時不會還原已關閉的本地串流。
    pub async fn unsubscribe_confirmed(
        &self,
        filter: impl Into<String>,
        timeout: Duration,
    ) -> Result<(), MqttError> {
        let filter = TopicFilter::new(filter)?;
        tokio::time::timeout(timeout, async {
            let receipt = self
                .unsubscribe_inner(filter, true)
                .await?
                .expect("confirmed operation has a receipt");
            receipt.await.map_err(|_| MqttError::ClientClosed)?
        })
        .await
        .map_err(|_| MqttError::Timeout)?
    }

    async fn unsubscribe_inner(
        &self,
        filter: TopicFilter,
        confirmed: bool,
    ) -> Result<Option<oneshot::Receiver<Result<(), MqttError>>>, MqttError> {
        loop {
            let notified = self.inner.backend.operations.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.ensure_open()?;
            let (completion, receipt) = oneshot::channel();
            let result = self.inner.registry.remove_filter_after(&filter, || {
                self.inner
                    .backend
                    .try_unsubscribe_tracked(filter.as_str(), confirmed.then_some(completion))
            });
            match result {
                Ok(()) => return Ok(confirmed.then_some(receipt)),
                Err(MqttError::ResourceLimit(_)) => notified.await,
                Err(error) => return Err(error),
            }
        }
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

    /// 請求關閉並等待背景任務停止。
    ///
    /// 關閉訊號在第一次 await 前送出；取消此 future 不會取消背景關閉。
    /// 已連線時最多花兩秒嘗試送達 DISCONNECT，逾時回傳 [`MqttError::Timeout`]。
    /// 多個呼叫者會等待同一個終止狀態並取得相同結果。
    pub async fn disconnect(&self) -> Result<(), MqttError> {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.backend.operations.close();
        self.guard.shutdown.send_replace(true);
        let mut state = self.inner.state.subscribe();
        loop {
            if *state.borrow_and_update() == ConnectionState::Closed {
                if let Some(error) = self
                    .inner
                    .background_error
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                {
                    return Err(MqttError::BackgroundTask(error.clone()));
                }
                return if self.inner.shutdown_timed_out.load(Ordering::Acquire) {
                    Err(MqttError::Timeout)
                } else {
                    Ok(())
                };
            }
            if state.changed().await.is_err() {
                return Err(MqttError::BackgroundTask("連線狀態通道已關閉".into()));
            }
        }
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
        registry: SubscriptionRegistry,
    ) -> MqttClient {
        let (events, _) = broadcast::channel(64);
        let (state, _) = watch::channel(ConnectionState::Connecting);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let inner = Arc::new(Self {
            backend,
            protocol,
            registry,
            events,
            state,
            closed: AtomicBool::new(false),
            shutdown_timed_out: AtomicBool::new(false),
            background_error: std::sync::Mutex::new(None),
            next_subscription_id: AtomicU64::new(1),
            subscription_capacity,
        });
        let task_inner = Arc::clone(&inner);
        let completion = CompletionGuard(Arc::clone(&inner));
        tokio::spawn(async move {
            use futures_util::FutureExt;
            let _completion = completion;
            let result = std::panic::AssertUnwindSafe(run_event_loop(
                Arc::clone(&task_inner),
                event_loop,
                reconnect,
                shutdown_receiver,
            ))
            .catch_unwind()
            .await;
            if let Err(panic) = result {
                let reason = panic
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| {
                        panic
                            .downcast_ref::<&str>()
                            .map(|value| (*value).to_owned())
                    })
                    .unwrap_or_else(|| "背景事件迴圈 panic".into());
                *task_inner
                    .background_error
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(reason);
                task_inner.close();
            }
        });
        MqttClient {
            inner,
            guard: Arc::new(HandleGuard { shutdown }),
        }
    }

    pub(crate) async fn flush_cleanup(&self, filter: &TopicFilter) {
        loop {
            let changed = self.backend.operations.changed.notified();
            let registry_changed = self.registry.changed.notified();
            tokio::pin!(changed, registry_changed);
            changed.as_mut().enable();
            registry_changed.as_mut().enable();
            if self.is_closed() {
                return;
            }
            self.registry.flush_cleanup(&self.backend);
            if !self.registry.cleanup_pending(filter) {
                return;
            }
            tokio::select! { _ = changed => {}, _ = registry_changed => {} }
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.backend.operations.close();
        self.registry.close_all();
        self.state.send_replace(ConnectionState::Closed);
    }
}

// 即使 task 在第一次 poll 前被取消，也發布 Closed 並喚醒所有等待者。
struct CompletionGuard(Arc<ClientInner>);

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        if *self.0.state.borrow() != ConnectionState::Closed {
            *self
                .0
                .background_error
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some("背景事件迴圈被取消".into());
            self.0.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::{ConfirmationStage, Kind};
    use futures_util::poll;

    fn controlled_client(
        capacity: usize,
    ) -> (
        MqttClient,
        flume::Receiver<rumqttc::Request>,
        watch::Receiver<bool>,
    ) {
        let (backend, requests) = BackendClient::test_channel(capacity);
        let (events, _) = broadcast::channel(64);
        let (state, _) = watch::channel(ConnectionState::Connected);
        let (shutdown, observed) = watch::channel(false);
        let inner = Arc::new(ClientInner {
            backend,
            protocol: ProtocolVersion::V311,
            registry: SubscriptionRegistry::new(8, 1024, 4096),
            events,
            state,
            closed: AtomicBool::new(false),
            shutdown_timed_out: AtomicBool::new(false),
            background_error: std::sync::Mutex::new(None),
            next_subscription_id: AtomicU64::new(1),
            subscription_capacity: 8,
        });
        (
            MqttClient {
                inner,
                guard: Arc::new(HandleGuard { shutdown }),
            },
            requests,
            observed,
        )
    }
    #[tokio::test]
    async fn cancelling_blocked_subscribe_removes_registration_and_retries_cleanup() {
        let (client, requests, _shutdown) = controlled_client(1);
        client
            .publish("busy", "payload", QoS::AtMostOnce)
            .await
            .unwrap();
        let filter = TopicFilter::new("cancelled/#").unwrap();
        let mut pending = Box::pin(client.subscribe(filter.as_str(), QoS::AtLeastOnce));
        assert!(poll!(pending.as_mut()).is_pending());
        assert!(client.inner.registry.contains(&filter));
        drop(pending);
        assert!(!client.inner.registry.contains(&filter));
        client.inner.registry.flush_cleanup(&client.inner.backend);
        assert!(client.inner.registry.cleanup_pending(&filter));
        assert!(matches!(
            requests.try_recv().unwrap(),
            rumqttc::Request::Publish(_)
        ));
        client.inner.backend.operations.outgoing(Kind::Publish, 0);
        client.inner.registry.flush_cleanup(&client.inner.backend);
        assert!(matches!(
            requests.try_recv().unwrap(),
            rumqttc::Request::Unsubscribe(_)
        ));
        assert!(!client.inner.registry.cleanup_pending(&filter));
    }
    #[tokio::test]
    async fn cancelling_confirmed_subscribe_after_enqueue_still_schedules_cleanup() {
        let (client, requests, _shutdown) = controlled_client(1);
        let filter = TopicFilter::new("confirmed/#").unwrap();
        let mut pending = Box::pin(client.subscribe_confirmed(
            SubscribeOptions::new(filter.as_str(), QoS::AtLeastOnce).unwrap(),
            Duration::from_secs(10),
        ));
        assert!(poll!(pending.as_mut()).is_pending());
        assert!(matches!(
            requests.try_recv().unwrap(),
            rumqttc::Request::Subscribe(_)
        ));
        drop(pending);
        assert!(!client.inner.registry.contains(&filter));
        client.inner.backend.operations.outgoing(Kind::Subscribe, 7);
        client.inner.backend.operations.acknowledge(
            Kind::Subscribe,
            7,
            ConfirmationStage::SubAck,
            Ok(()),
        );
        client.inner.registry.flush_cleanup(&client.inner.backend);
        assert!(matches!(
            requests.try_recv().unwrap(),
            rumqttc::Request::Unsubscribe(_)
        ));
    }
    #[tokio::test]
    async fn cancelled_duplicate_subscribe_restores_surviving_options() {
        let (client, _requests, _shutdown) = controlled_client(1);
        let _first = client.subscribe("same/#", QoS::AtMostOnce).await.unwrap();
        let mut pending = Box::pin(client.subscribe("same/#", QoS::ExactlyOnce));
        assert!(poll!(pending.as_mut()).is_pending());
        drop(pending);
        let options = client.inner.registry.snapshot_options();
        assert_eq!(options.len(), 1);
        assert_eq!(options[0].qos, QoS::AtMostOnce);
    }
    #[tokio::test]
    async fn cancelling_blocked_unsubscribe_preserves_existing_stream() {
        let (client, _requests, _shutdown) = controlled_client(1);
        let _subscription = client
            .subscribe("still/active", QoS::AtLeastOnce)
            .await
            .unwrap();
        let filter = TopicFilter::new("still/active").unwrap();
        let mut pending = Box::pin(client.unsubscribe(filter.as_str()));
        assert!(poll!(pending.as_mut()).is_pending());
        drop(pending);
        assert!(client.inner.registry.contains(&filter));
    }
    #[tokio::test]
    async fn pending_subscribe_cannot_revive_a_filter_removed_by_explicit_unsubscribe() {
        let (client, requests, _shutdown) = controlled_client(1);
        client
            .publish("busy", "payload", QoS::AtLeastOnce)
            .await
            .unwrap();
        let mut pending = Box::pin(client.subscribe("removed/#", QoS::AtLeastOnce));
        assert!(poll!(pending.as_mut()).is_pending());
        assert!(matches!(
            requests.try_recv().unwrap(),
            rumqttc::Request::Publish(_)
        ));
        tokio::time::timeout(Duration::from_secs(1), client.unsubscribe("removed/#"))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), pending)
                .await
                .expect("cancelled subscription must wake"),
            Err(MqttError::SubscriptionClosed)
        ));
        assert!(matches!(
            requests.try_recv().unwrap(),
            rumqttc::Request::Unsubscribe(_)
        ));
        assert!(requests.is_empty());
    }

    #[tokio::test]
    async fn cancelling_disconnect_keeps_shutdown_requested_for_every_caller() {
        let (client, _requests, observed) = controlled_client(1);
        client
            .publish("busy", "payload", QoS::AtMostOnce)
            .await
            .unwrap();
        let mut pending = Box::pin(client.disconnect());
        assert!(poll!(pending.as_mut()).is_pending());
        assert!(*observed.borrow());
        drop(pending);
        client.inner.close();
        let (first, second) = tokio::join!(client.disconnect(), client.disconnect());
        first.unwrap();
        second.unwrap();
        assert!(matches!(
            client.publish("closed", "payload", QoS::AtMostOnce).await,
            Err(MqttError::ClientClosed)
        ));
    }
    #[tokio::test]
    async fn dropped_background_future_marks_closed_and_releases_waiters() {
        let (client, _requests, _shutdown) = controlled_client(1);
        let guard = CompletionGuard(Arc::clone(&client.inner));
        drop(guard);
        assert_eq!(*client.state().borrow(), ConnectionState::Closed);
        assert!(matches!(
            client.disconnect().await,
            Err(MqttError::BackgroundTask(_))
        ));
    }
    #[tokio::test]
    async fn qos_zero_confirmation_is_rejected_without_enqueueing() {
        let (client, requests, _shutdown) = controlled_client(1);
        let result = client
            .publish_confirmed(
                PublishOptions::new("qos/zero", "payload", QoS::AtMostOnce),
                Duration::from_secs(1),
            )
            .await;
        assert!(matches!(result, Err(MqttError::UnsupportedFeature(_))));
        assert!(requests.is_empty());
    }
}
