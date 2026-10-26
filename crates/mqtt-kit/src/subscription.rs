use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, Weak},
    task::{Context, Poll},
};

use futures_core::Stream;
use futures_util::task::AtomicWaker;
use mqtt_kit_core::{Message, MqttError, SubscribeOptions, TopicFilter};

use crate::client::ClientInner;

pub(crate) struct MessageQueue {
    state: Mutex<QueueState>,
    waker: AtomicWaker,
    capacity: usize,
}

struct QueueState {
    messages: VecDeque<Message>,
    closed: bool,
}

impl MessageQueue {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(QueueState {
                messages: VecDeque::new(),
                closed: false,
            }),
            waker: AtomicWaker::new(),
            capacity,
        }
    }

    pub(crate) fn push(&self, message: Message, filter: &str) {
        let mut state = lock_recover(&self.state);
        if state.closed {
            return;
        }
        if state.messages.len() == self.capacity {
            let _dropped = state.messages.pop_front();
            tracing::warn!(
                topic_filter = filter,
                capacity = self.capacity,
                "訂閱佇列已滿，丟棄最舊訊息"
            );
        }
        state.messages.push_back(message);
        drop(state);
        self.waker.wake();
    }

    pub(crate) fn close(&self) {
        lock_recover(&self.state).closed = true;
        self.waker.wake();
    }

    fn poll_next(&self, context: &mut Context<'_>) -> Poll<Option<Message>> {
        {
            let mut state = lock_recover(&self.state);
            if let Some(message) = state.messages.pop_front() {
                return Poll::Ready(Some(message));
            }
            if state.closed {
                return Poll::Ready(None);
            }
        }

        self.waker.register(context.waker());
        let mut state = lock_recover(&self.state);
        if let Some(message) = state.messages.pop_front() {
            Poll::Ready(Some(message))
        } else if state.closed {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

struct RegistryEntry {
    options: SubscribeOptions,
    subscribers: HashMap<u64, Arc<MessageQueue>>,
}

#[derive(Default)]
pub(crate) struct SubscriptionRegistry {
    entries: Mutex<HashMap<TopicFilter, RegistryEntry>>,
}

impl SubscriptionRegistry {
    pub(crate) fn insert(
        &self,
        id: u64,
        options: SubscribeOptions,
        capacity: usize,
    ) -> Arc<MessageQueue> {
        let queue = Arc::new(MessageQueue::new(capacity));
        let mut entries = lock_recover(&self.entries);
        let entry = entries
            .entry(options.filter.clone())
            .or_insert_with(|| RegistryEntry {
                options: options.clone(),
                subscribers: HashMap::new(),
            });
        entry.options = options;
        entry.subscribers.insert(id, Arc::clone(&queue));
        queue
    }

    pub(crate) fn remove(&self, filter: &TopicFilter, id: u64) -> bool {
        let mut entries = lock_recover(&self.entries);
        let Some(entry) = entries.get_mut(filter) else {
            return false;
        };
        if let Some(queue) = entry.subscribers.remove(&id) {
            queue.close();
        }
        if entry.subscribers.is_empty() {
            entries.remove(filter);
            true
        } else {
            false
        }
    }

    pub(crate) fn remove_filter(&self, filter: &TopicFilter) -> bool {
        let mut entries = lock_recover(&self.entries);
        let Some(entry) = entries.remove(filter) else {
            return false;
        };
        for queue in entry.subscribers.into_values() {
            queue.close();
        }
        true
    }

    pub(crate) fn snapshot_options(&self) -> Vec<SubscribeOptions> {
        lock_recover(&self.entries)
            .values()
            .map(|entry| entry.options.clone())
            .collect()
    }

    /// 查詢註冊表中是否仍有該 filter 的訂閱者。
    pub(crate) fn contains(&self, filter: &TopicFilter) -> bool {
        lock_recover(&self.entries).contains_key(filter)
    }

    pub(crate) fn dispatch(&self, message: Message) {
        let destinations: Vec<(String, Arc<MessageQueue>)> = {
            let entries = lock_recover(&self.entries);
            entries
                .iter()
                .filter(|(filter, _)| filter.matches(&message.topic))
                .flat_map(|(filter, entry)| {
                    entry
                        .subscribers
                        .values()
                        .map(|queue| (filter.as_str().to_owned(), Arc::clone(queue)))
                })
                .collect()
        };

        for (filter, queue) in destinations {
            queue.push(message.clone(), &filter);
        }
    }

    pub(crate) fn close_all(&self) {
        let mut entries = lock_recover(&self.entries);
        for entry in entries.values() {
            for queue in entry.subscribers.values() {
                queue.close();
            }
        }
        entries.clear();
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
        let last = client.registry.remove(&self.filter, self.id);
        if last && !client.is_closed() {
            client.backend.unsubscribe(self.filter.as_str()).await?;
        }
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
        let last = client.registry.remove(&self.filter, self.id);
        if last && !client.is_closed() {
            if let Err(error) = client.backend.try_unsubscribe(self.filter.as_str()) {
                tracing::warn!(%error, topic_filter = %self.filter, "Drop 時排入 UNSUBSCRIBE 失敗");
            }
        }
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
