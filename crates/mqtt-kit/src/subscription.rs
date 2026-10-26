use crate::{backend::BackendClient, client::ClientInner, route_index::RouteIndex};
use futures_core::Stream;
use futures_util::task::AtomicWaker;
use mqtt_kit_core::{Message, MqttError, SubscribeOptions, TopicFilter};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};
use tokio::sync::Notify;

struct Budget {
    used: Mutex<usize>,
    limit: usize,
}
impl Budget {
    fn reserve(&self, bytes: usize) -> bool {
        let mut used = lock_recover(&self.used);
        if bytes > self.limit.saturating_sub(*used) {
            return false;
        }
        *used += bytes;
        true
    }
    fn release(&self, bytes: usize) {
        let mut used = lock_recover(&self.used);
        *used = used.saturating_sub(bytes);
    }
}

pub(crate) struct MessageQueue {
    state: Mutex<QueueState>,
    waker: AtomicWaker,
    capacity: usize,
    byte_capacity: usize,
    budget: Arc<Budget>,
    dropped: AtomicU64,
}
struct QueueState {
    messages: VecDeque<(Message, usize)>,
    bytes: usize,
    closed: bool,
}

fn message_bytes(message: &Message) -> usize {
    let mut bytes = std::mem::size_of::<Message>()
        .saturating_add(message.topic.len())
        .saturating_add(message.payload.len());
    if let Some(props) = &message.v5 {
        for value in [&props.response_topic, &props.content_type]
            .into_iter()
            .flatten()
        {
            bytes = bytes.saturating_add(value.len());
        }
        if let Some(data) = &props.correlation_data {
            bytes = bytes.saturating_add(data.len());
        }
        bytes = bytes.saturating_add(
            props
                .user_properties
                .len()
                .saturating_mul(std::mem::size_of::<(String, String)>()),
        );
        for (key, value) in &props.user_properties {
            bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
        }
        bytes = bytes.saturating_add(
            props
                .subscription_identifiers
                .len()
                .saturating_mul(std::mem::size_of::<usize>()),
        );
    }
    bytes
}
impl MessageQueue {
    fn new(capacity: usize, byte_capacity: usize, budget: Arc<Budget>) -> Self {
        Self {
            state: Mutex::new(QueueState {
                messages: VecDeque::new(),
                bytes: 0,
                closed: false,
            }),
            waker: AtomicWaker::new(),
            capacity,
            byte_capacity,
            budget,
            dropped: AtomicU64::new(0),
        }
    }
    fn record_drop(&self) {
        if self.dropped.fetch_add(1, Ordering::Relaxed) == 0 {
            tracing::warn!(
                capacity = self.capacity,
                byte_capacity = self.byte_capacity,
                "訂閱超出緩衝預算；後續丟棄數請查詢 dropped_messages()"
            );
        }
    }

    fn evict(&self, state: &mut QueueState) -> bool {
        if let Some((_, bytes)) = state.messages.pop_front() {
            state.bytes -= bytes;
            self.budget.release(bytes);
            self.record_drop();
            true
        } else {
            false
        }
    }
    pub(crate) fn push(&self, message: Message, _filter: &str) {
        let bytes = message_bytes(&message);
        let mut state = lock_recover(&self.state);
        if state.closed {
            return;
        }
        if bytes > self.byte_capacity || bytes > self.budget.limit {
            self.record_drop();
            return;
        }
        while state.messages.len() >= self.capacity
            || bytes > self.byte_capacity.saturating_sub(state.bytes)
        {
            if !self.evict(&mut state) {
                break;
            }
        }
        while !self.budget.reserve(bytes) {
            if !self.evict(&mut state) {
                self.record_drop();
                return;
            }
        }
        state.bytes += bytes;
        state.messages.push_back((message, bytes));
        drop(state);
        self.waker.wake();
    }
    pub(crate) fn close(&self) {
        lock_recover(&self.state).closed = true;
        self.waker.wake();
    }
    fn poll_next(&self, context: &mut Context<'_>) -> Poll<Option<Message>> {
        self.waker.register(context.waker());
        let mut state = lock_recover(&self.state);
        if let Some((message, bytes)) = state.messages.pop_front() {
            state.bytes -= bytes;
            self.budget.release(bytes);
            Poll::Ready(Some(message))
        } else if state.closed {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}
impl Drop for MessageQueue {
    fn drop(&mut self) {
        self.budget.release(lock_recover(&self.state).bytes);
    }
}
struct Subscriber {
    options: SubscribeOptions,
    queue: Arc<MessageQueue>,
}
struct RegistryEntry {
    subscribers: BTreeMap<u64, Subscriber>,
}
#[derive(Default)]
struct RegistryState {
    entries: HashMap<TopicFilter, RegistryEntry>,
    index: RouteIndex,
    cleanup: HashSet<TopicFilter>,
    refresh: HashSet<TopicFilter>,
    streams: usize,
}

pub(crate) struct SubscriptionRegistry {
    entries: Mutex<RegistryState>,
    budget: Arc<Budget>,
    byte_capacity: usize,
    max_subscriptions: usize,
    pub(crate) changed: Notify,
}
impl SubscriptionRegistry {
    pub(crate) fn new(max_subscriptions: usize, byte_capacity: usize, total_bytes: usize) -> Self {
        Self {
            entries: Mutex::new(RegistryState::default()),
            budget: Arc::new(Budget {
                used: Mutex::new(0),
                limit: total_bytes,
            }),
            byte_capacity,
            max_subscriptions,
            changed: Notify::new(),
        }
    }
    pub(crate) fn insert(
        &self,
        id: u64,
        options: SubscribeOptions,
        capacity: usize,
    ) -> Result<Arc<MessageQueue>, MqttError> {
        let queue = Arc::new(MessageQueue::new(
            capacity,
            self.byte_capacity,
            Arc::clone(&self.budget),
        ));
        let mut state = lock_recover(&self.entries);
        let extra = usize::from(!state.cleanup.contains(&options.filter));
        if state.streams + state.cleanup.len() + extra > self.max_subscriptions {
            return Err(MqttError::ResourceLimit(
                "訂閱與待清理 filter 數已達上限".into(),
            ));
        }
        state.cleanup.remove(&options.filter);
        if !state.entries.contains_key(&options.filter) {
            state.index.insert(&options.filter);
        }
        state
            .entries
            .entry(options.filter.clone())
            .or_insert_with(|| RegistryEntry {
                subscribers: BTreeMap::new(),
            })
            .subscribers
            .insert(
                id,
                Subscriber {
                    options,
                    queue: Arc::clone(&queue),
                },
            );
        state.streams += 1;
        drop(state);
        self.changed.notify_waiters();
        Ok(queue)
    }
    pub(crate) fn remove(&self, filter: &TopicFilter, id: u64) -> bool {
        let mut state = lock_recover(&self.entries);
        let Some(entry) = state.entries.get_mut(filter) else {
            return false;
        };
        let was_latest = entry
            .subscribers
            .last_key_value()
            .is_some_and(|(latest, _)| *latest == id);
        let removed = entry.subscribers.remove(&id);
        let last = entry.subscribers.is_empty();
        if let Some(subscriber) = removed {
            subscriber.queue.close();
            state.streams -= 1;
        }
        if last {
            state.entries.remove(filter);
            state.index.remove(filter);
            state.cleanup.insert(filter.clone());
            state.refresh.remove(filter);
        } else if was_latest {
            state.refresh.insert(filter.clone());
        }
        drop(state);
        self.changed.notify_one();
        last
    }
    pub(crate) fn enqueue_subscription(
        &self,
        filter: &TopicFilter,
        id: u64,
        enqueue: impl FnOnce(&SubscribeOptions) -> Result<(), MqttError>,
    ) -> Result<(), MqttError> {
        let mut state = lock_recover(&self.entries);
        let entry = state
            .entries
            .get(filter)
            .ok_or(MqttError::SubscriptionClosed)?;
        if !entry.subscribers.contains_key(&id) {
            return Err(MqttError::SubscriptionClosed);
        }
        let (_, latest) = entry
            .subscribers
            .last_key_value()
            .ok_or(MqttError::SubscriptionClosed)?;
        enqueue(&latest.options)?;
        state.refresh.remove(filter);
        Ok(())
    }

    pub(crate) fn remove_filter_after<T>(
        &self,
        filter: &TopicFilter,
        enqueue: impl FnOnce() -> Result<T, MqttError>,
    ) -> Result<T, MqttError> {
        let mut state = lock_recover(&self.entries);
        // 同一把註冊表鎖下 enqueue + 移除，後來的新訂閱不會被舊 UNSUBSCRIBE 越過。
        let result = enqueue()?;
        if let Some(entry) = state.entries.remove(filter) {
            state.streams -= entry.subscribers.len();
            state.index.remove(filter);
            for subscriber in entry.subscribers.into_values() {
                subscriber.queue.close();
            }
        }
        state.cleanup.remove(filter);
        state.refresh.remove(filter);
        drop(state);
        self.changed.notify_waiters();
        Ok(result)
    }
    pub(crate) fn flush_cleanup(&self, backend: &BackendClient) {
        let mut state = lock_recover(&self.entries);
        let filters: Vec<_> = state.cleanup.iter().cloned().collect();
        let mut progressed = false;
        for filter in filters {
            if backend.try_unsubscribe(filter.as_str()).is_err() {
                break;
            }
            state.cleanup.remove(&filter);
            progressed = true;
        }
        let refresh: Vec<_> = state.refresh.iter().cloned().collect();
        for filter in refresh {
            if let Some(entry) = state.entries.get(&filter) {
                if let Some((_, latest)) = entry.subscribers.last_key_value() {
                    if backend.try_subscribe(&latest.options).is_err() {
                        break;
                    }
                }
            }
            state.refresh.remove(&filter);
            progressed = true;
        }
        drop(state);
        if progressed {
            self.changed.notify_waiters();
        }
    }
    pub(crate) fn has_cleanup(&self) -> bool {
        let state = lock_recover(&self.entries);
        !state.cleanup.is_empty() || !state.refresh.is_empty()
    }
    pub(crate) fn cleanup_pending(&self, filter: &TopicFilter) -> bool {
        lock_recover(&self.entries).cleanup.contains(filter)
    }
    pub(crate) fn resubscribe(
        &self,
        filter: &TopicFilter,
        enqueue: impl FnOnce(&SubscribeOptions) -> Result<(), MqttError>,
    ) -> Option<Result<(), MqttError>> {
        let state = lock_recover(&self.entries);
        let (_, subscriber) = state.entries.get(filter)?.subscribers.last_key_value()?;
        Some(enqueue(&subscriber.options))
    }

    pub(crate) fn snapshot_options(&self) -> Vec<SubscribeOptions> {
        lock_recover(&self.entries)
            .entries
            .values()
            .filter_map(|entry| {
                entry
                    .subscribers
                    .last_key_value()
                    .map(|(_, subscriber)| subscriber.options.clone())
            })
            .collect()
    }
    #[cfg(test)]
    pub(crate) fn contains(&self, filter: &TopicFilter) -> bool {
        lock_recover(&self.entries).entries.contains_key(filter)
    }
    pub(crate) fn dispatch(&self, message: Message) {
        let destinations = {
            let state = lock_recover(&self.entries);
            let mut destinations = Vec::new();
            for filter in state.index.matches(&message.topic) {
                if let Some(entry) = state.entries.get(&filter) {
                    for subscriber in entry.subscribers.values() {
                        destinations.push((filter.clone(), Arc::clone(&subscriber.queue)));
                    }
                }
            }
            destinations
        };
        for (filter, queue) in destinations {
            queue.push(message.clone(), filter.as_str());
        }
    }
    pub(crate) fn close_all(&self) {
        let mut state = lock_recover(&self.entries);
        for entry in state.entries.values() {
            for subscriber in entry.subscribers.values() {
                subscriber.queue.close();
            }
        }
        *state = RegistryState::default();
        drop(state);
        self.changed.notify_waiters();
    }
}

/// 一筆本地訂閱的非同步訊息串流。
///
/// Drop 時會從本地路由表移除；若同一 filter 已無其他使用者，會向 broker 排入
/// UNSUBSCRIBE。若需要確認排入結果，請呼叫 [`Subscription::unsubscribe`]。
pub struct Subscription {
    id: u64,
    filter: TopicFilter,
    queue: Arc<MessageQueue>,
    client: Weak<ClientInner>,
    active: bool,
}

impl Subscription {
    pub(crate) fn new(
        id: u64,
        filter: TopicFilter,
        queue: Arc<MessageQueue>,
        client: Weak<ClientInner>,
    ) -> Self {
        Self {
            id,
            filter,
            queue,
            client,
            active: true,
        }
    }

    /// 取得此串流使用的 topic filter。
    #[must_use]
    pub fn filter(&self) -> &TopicFilter {
        &self.filter
    }

    /// 此訂閱因筆數或位元組預算丟棄的累計訊息數。
    #[must_use]
    pub fn dropped_messages(&self) -> u64 {
        self.queue.dropped.load(Ordering::Relaxed)
    }

    /// 此訂閱目前計入預算的緩衝位元組數。
    #[must_use]
    pub fn buffered_bytes(&self) -> usize {
        lock_recover(&self.queue.state).bytes
    }

    /// 接收下一筆訊息；訂閱關閉且緩衝已清空時回傳 `None`。
    pub async fn recv(&mut self) -> Option<Message> {
        futures_util::future::poll_fn(|context| Pin::new(&mut *self).poll_next(context)).await
    }

    /// 主動取消此本地訂閱；若為相同 filter 的最後一筆訂閱，也向 broker 取消。
    pub async fn unsubscribe(mut self) -> Result<(), MqttError> {
        if !self.active {
            return Ok(());
        }
        self.active = false;
        let Some(client) = self.client.upgrade() else {
            self.queue.close();
            return Ok(());
        };
        client.registry.remove(&self.filter, self.id);
        client.flush_cleanup(&self.filter).await;
        Ok(())
    }
}

impl Stream for Subscription {
    type Item = Message;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.queue.poll_next(context)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let Some(client) = self.client.upgrade() else {
            self.queue.close();
            return;
        };
        client.registry.remove(&self.filter, self.id);
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mqtt_kit_core::QoS;
    fn message(payload: &'static str) -> Message {
        Message {
            topic: "data".into(),
            payload: Bytes::from_static(payload.as_bytes()),
            qos: QoS::AtMostOnce,
            retain: false,
            v5: None,
        }
    }
    fn pop(queue: &MessageQueue) -> Option<Message> {
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        match queue.poll_next(&mut context) {
            Poll::Ready(message) => message,
            Poll::Pending => None,
        }
    }
    #[test]
    fn byte_budget_evicts_oldest_and_is_released_on_receive_and_drop() {
        let size = message_bytes(&message("a"));
        let budget = Arc::new(Budget {
            used: Mutex::new(0),
            limit: size * 4,
        });
        let queue = MessageQueue::new(10, size * 2, Arc::clone(&budget));
        for value in ["a", "b", "c"] {
            queue.push(message(value), "data");
        }
        assert_eq!(queue.dropped.load(Ordering::Relaxed), 1);
        assert_eq!(pop(&queue).unwrap().payload, "b");
        assert_eq!(*lock_recover(&budget.used), size);
        drop(queue);
        assert_eq!(*lock_recover(&budget.used), 0);
    }
    #[test]
    fn global_budget_limits_fanout_and_closed_queues_keep_their_reservation_until_drained() {
        let size = message_bytes(&message("a"));
        let registry = SubscriptionRegistry::new(8, size * 4, size);
        let options = SubscribeOptions::new("data", QoS::AtMostOnce).unwrap();
        let first = registry.insert(1, options.clone(), 8).unwrap();
        let second = registry.insert(2, options, 8).unwrap();
        registry.dispatch(message("a"));
        assert_eq!(*lock_recover(&registry.budget.used), size);
        assert_eq!(second.dropped.load(Ordering::Relaxed), 1);
        registry.close_all();
        assert_eq!(*lock_recover(&registry.budget.used), size);
        pop(&first).unwrap();
        assert_eq!(*lock_recover(&registry.budget.used), 0);
    }
    #[test]
    fn oversized_payloads_and_properties_are_counted_without_disturbing_existing_messages() {
        let size = message_bytes(&message("a"));
        let budget = Arc::new(Budget {
            used: Mutex::new(0),
            limit: size * 2,
        });
        let queue = MessageQueue::new(8, size, Arc::clone(&budget));
        queue.push(message("a"), "data");
        let mut oversized = message("a");
        oversized.v5 =
            Some(mqtt_kit_core::V5PublishProps::new().user_property("big", "x".repeat(size)));
        queue.push(oversized, "data");
        assert_eq!(queue.dropped.load(Ordering::Relaxed), 1);
        assert_eq!(pop(&queue).unwrap().payload, "a");
    }
    #[test]
    fn empty_user_properties_still_consume_metadata_budget() {
        let plain = message("a");
        let size = message_bytes(&plain);
        let mut heavy = plain.clone();
        let mut properties = mqtt_kit_core::V5PublishProps::new();
        properties.user_properties = vec![(String::new(), String::new()); 128];
        heavy.v5 = Some(properties);
        assert!(message_bytes(&heavy) >= size + 128 * std::mem::size_of::<(String, String)>());
        let queue = MessageQueue::new(
            8,
            size * 2,
            Arc::new(Budget {
                used: Mutex::new(0),
                limit: size * 2,
            }),
        );
        queue.push(heavy, "data");
        assert_eq!(queue.dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn cancelled_filters_hold_bounded_cleanup_slots_and_same_filter_can_reuse_its_slot() {
        let registry = SubscriptionRegistry::new(1, 1024, 4096);
        let options = SubscribeOptions::new("old/#", QoS::AtMostOnce).unwrap();
        let _queue = registry.insert(1, options.clone(), 1).unwrap();
        assert!(registry.remove(&options.filter, 1));
        assert!(matches!(
            registry.insert(
                2,
                SubscribeOptions::new("new/#", QoS::AtMostOnce).unwrap(),
                1
            ),
            Err(MqttError::ResourceLimit(_))
        ));
        registry.insert(3, options.clone(), 1).unwrap();
        assert!(!registry.cleanup_pending(&options.filter));
        assert!(registry.contains(&options.filter));
    }
}
