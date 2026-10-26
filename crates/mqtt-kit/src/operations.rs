//! 將排入的要求、Outgoing packet ID 與 broker ACK 關聯；所有資料都有容量上限。
use mqtt_kit_core::MqttError;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Mutex, MutexGuard},
};
use tokio::sync::{Notify, oneshot};

pub(crate) type Completion = oneshot::Sender<Result<(), MqttError>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    Publish,
    Subscribe,
    Unsubscribe,
}

impl Kind {
    fn index(self) -> usize {
        match self {
            Self::Publish => 0,
            Self::Subscribe => 1,
            Self::Unsubscribe => 2,
        }
    }
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Publish => "PUBLISH",
            Self::Subscribe => "SUBSCRIBE",
            Self::Unsubscribe => "UNSUBSCRIBE",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConfirmationStage {
    PubAck,
    PubComp,
    SubAck,
    UnsubAck,
}

pub(crate) struct Pending {
    pub(crate) expected: ConfirmationStage,
    pub(crate) retransmission: bool,
    pub(crate) completion: Option<Completion>,
}

pub(crate) struct State {
    queued: [VecDeque<Pending>; 3],
    inflight: HashMap<(Kind, u16), Pending>,
    discard_on_new_session: [usize; 3],
    pub(crate) closed: bool,
    capacity: usize,
}

impl State {
    #[cfg(test)]
    pub(crate) fn has_capacity(&self) -> bool {
        self.has_capacity_for(Kind::Publish)
    }
    pub(crate) fn has_capacity_for(&self, kind: Kind) -> bool {
        // 預留有界的控制要求容量，讓逾時 SUBSCRIBE 的清理不必等待其 SUBACK。
        let limit = if kind == Kind::Unsubscribe {
            self.capacity.saturating_mul(2)
        } else {
            self.capacity
        };
        self.queued.iter().map(VecDeque::len).sum::<usize>() + self.inflight.len() < limit
    }
    pub(crate) fn enqueue(&mut self, kind: Kind, pending: Pending) {
        self.queued[kind.index()].push_back(pending);
    }
}

pub(crate) struct Operations {
    state: Mutex<State>,
    pub(crate) changed: Notify,
}

impl Operations {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(State {
                queued: Default::default(),
                inflight: HashMap::new(),
                discard_on_new_session: [0; 3],
                closed: false,
                capacity,
            }),
            changed: Notify::new(),
        }
    }
    pub(crate) fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
    pub(crate) fn outgoing(&self, kind: Kind, packet_id: u16) -> bool {
        let mut state = self.lock();
        // QoS1/2 重傳保留原 packet ID，不得錯配到下一筆新的要求。
        if let Some(existing) = state.inflight.get_mut(&(kind, packet_id)) {
            if kind == Kind::Publish && existing.retransmission {
                existing.retransmission = false;
                return true;
            }
            return false;
        }
        if let Some(pending) = state.queued[kind.index()].pop_front() {
            if kind == Kind::Publish && packet_id == 0 {
                // QoS0 只有送出事件，公開 confirmed API 不允許此模式。
                if let Some(completion) = pending.completion {
                    let _ = completion.send(Err(MqttError::UnsupportedFeature(
                        "QoS0 沒有 broker ACK".into(),
                    )));
                }
            } else {
                state.inflight.insert((kind, packet_id), pending);
            }
        }
        drop(state);
        self.changed.notify_waiters();
        true
    }
    pub(crate) fn acknowledge(
        &self,
        kind: Kind,
        packet_id: u16,
        ack: ConfirmationStage,
        result: Result<(), MqttError>,
    ) {
        let mut state = self.lock();
        if state
            .inflight
            .get(&(kind, packet_id))
            .is_some_and(|pending| pending.expected == ack)
        {
            if let Some(pending) = state.inflight.remove(&(kind, packet_id)) {
                if let Some(completion) = pending.completion {
                    let _ = completion.send(result);
                }
            }
        }
        drop(state);
        self.changed.notify_waiters();
    }
    // rumqttc 在錯誤時將 channel 內的要求移入 pending；新 session 會丟掉它們。
    // 在同一把送出鎖下記錄 pending 的未指派要求，避免重連後錯配確認。
    pub(crate) fn disconnected(&self, pending_requests: impl Iterator<Item = (Kind, u16)>) {
        let mut state = self.lock();
        let mut counts = [0; 3];
        for pending in state.inflight.values_mut() {
            pending.retransmission = false;
        }
        for (kind, id) in pending_requests {
            if let Some(pending) = state.inflight.get_mut(&(kind, id)) {
                pending.retransmission = kind == Kind::Publish;
            } else {
                counts[kind.index()] += 1;
            }
        }
        state.discard_on_new_session = counts;
        for queue in &mut state.queued {
            for pending in queue {
                if let Some(completion) = pending.completion.take() {
                    let _ = completion.send(Err(MqttError::connection_message(
                        "確認前連線中斷；要求可能已送達，請勿盲目重試",
                    )));
                }
            }
        }
        for pending in state.inflight.values_mut() {
            if let Some(completion) = pending.completion.take() {
                let _ = completion.send(Err(MqttError::connection_message(
                    "確認前連線中斷；要求可能已送達，請勿盲目重試",
                )));
            }
        }
        // rumqttc 只保留已送出的 Publish/PubRel 供 session 恢復，不重送 SUB/UNSUB。
        state.inflight.retain(|(kind, _), _| *kind == Kind::Publish);
        drop(state);
        self.changed.notify_waiters();
    }
    pub(crate) fn connected(&self, session_present: bool) {
        let mut state = self.lock();
        if !session_present {
            for index in 0..3 {
                for _ in 0..state.discard_on_new_session[index] {
                    state.queued[index].pop_front();
                }
            }
            state.inflight.clear();
        }
        state.discard_on_new_session = [0; 3];
        drop(state);
        self.changed.notify_waiters();
    }
    pub(crate) fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        for queue in &mut state.queued {
            for pending in queue.drain(..) {
                if let Some(completion) = pending.completion {
                    let _ = completion.send(Err(MqttError::ClientClosed));
                }
            }
        }
        for (_, pending) in state.inflight.drain() {
            if let Some(completion) = pending.completion {
                let _ = completion.send(Err(MqttError::ClientClosed));
            }
        }
        drop(state);
        self.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(
        operations: &Operations,
        kind: Kind,
        ack: ConfirmationStage,
    ) -> oneshot::Receiver<Result<(), MqttError>> {
        let (completion, receipt) = oneshot::channel();
        operations.lock().enqueue(
            kind,
            Pending {
                retransmission: false,
                expected: ack,
                completion: Some(completion),
            },
        );
        receipt
    }
    async fn complete(receipt: oneshot::Receiver<Result<(), MqttError>>) -> Result<(), MqttError> {
        tokio::time::timeout(std::time::Duration::from_secs(1), receipt)
            .await
            .expect("receipt must finish")
            .expect("receipt sender must resolve")
    }
    #[tokio::test]
    async fn wrong_ids_or_ack_phases_cannot_complete_another_operation() {
        let operations = Operations::new(2);
        let mut receipt = request(&operations, Kind::Publish, ConfirmationStage::PubComp);
        assert!(operations.outgoing(Kind::Publish, 7));
        operations.acknowledge(Kind::Publish, 8, ConfirmationStage::PubComp, Ok(()));
        operations.acknowledge(Kind::Publish, 7, ConfirmationStage::PubAck, Ok(()));
        assert!(matches!(
            receipt.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        operations.acknowledge(Kind::Publish, 7, ConfirmationStage::PubComp, Ok(()));
        complete(receipt).await.unwrap();
        assert!(operations.lock().has_capacity());
    }
    #[tokio::test]
    async fn retransmission_does_not_consume_the_next_receipt() {
        let operations = Operations::new(2);
        let first = request(&operations, Kind::Publish, ConfirmationStage::PubAck);
        operations.outgoing(Kind::Publish, 1);
        operations.disconnected(std::iter::once((Kind::Publish, 1)));
        assert!(matches!(
            complete(first).await,
            Err(MqttError::Connection { .. })
        ));
        operations.connected(true);
        let mut second = request(&operations, Kind::Publish, ConfirmationStage::PubAck);
        operations.outgoing(Kind::Publish, 1);
        operations.acknowledge(Kind::Publish, 1, ConfirmationStage::PubAck, Ok(()));
        assert!(matches!(
            second.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        operations.outgoing(Kind::Publish, 2);
        operations.acknowledge(Kind::Publish, 2, ConfirmationStage::PubAck, Ok(()));
        complete(second).await.unwrap();
    }
    #[tokio::test]
    async fn fresh_session_discards_only_requests_removed_by_the_transport() {
        let operations = Operations::new(3);
        let first = request(&operations, Kind::Subscribe, ConfirmationStage::SubAck);
        operations.disconnected(std::iter::once((Kind::Subscribe, 0)));
        assert!(matches!(
            complete(first).await,
            Err(MqttError::Connection { .. })
        ));
        let second = request(&operations, Kind::Subscribe, ConfirmationStage::SubAck);
        operations.connected(false);
        operations.outgoing(Kind::Subscribe, 5);
        operations.acknowledge(Kind::Subscribe, 5, ConfirmationStage::SubAck, Ok(()));
        complete(second).await.unwrap();
    }
    #[tokio::test]
    async fn rejection_and_close_release_all_waiters() {
        let operations = Operations::new(2);
        let rejected = request(&operations, Kind::Subscribe, ConfirmationStage::SubAck);
        operations.outgoing(Kind::Subscribe, 9);
        operations.acknowledge(
            Kind::Subscribe,
            9,
            ConfirmationStage::SubAck,
            Err(MqttError::BrokerRejected {
                operation: "SUBSCRIBE",
                reason: "denied".into(),
            }),
        );
        assert!(matches!(
            complete(rejected).await,
            Err(MqttError::BrokerRejected { .. })
        ));
        let pending = request(&operations, Kind::Unsubscribe, ConfirmationStage::UnsubAck);
        operations.close();
        assert!(matches!(
            complete(pending).await,
            Err(MqttError::ClientClosed)
        ));
        assert!(operations.lock().closed);
    }
    #[test]
    fn duplicate_subscription_packet_ids_fail_instead_of_misconfirming() {
        let operations = Operations::new(2);
        let _first = request(&operations, Kind::Subscribe, ConfirmationStage::SubAck);
        operations.outgoing(Kind::Subscribe, 1);
        let _second = request(&operations, Kind::Subscribe, ConfirmationStage::SubAck);
        assert!(!operations.outgoing(Kind::Subscribe, 1));
    }
}
