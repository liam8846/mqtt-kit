use std::{sync::Arc, time::Duration};

use mqtt_kit_core::{
    ConnectionEvent, ConnectionState, MqttError, ReconnectPolicy, SubscribeOptions,
};
use rand::Rng;
use tokio::sync::watch;

use crate::{
    backend::{BackendError, BackendEvent, BackendEventLoop},
    client::ClientInner,
};

/// 重訂閱重試的初始退避時間。
const RESUBSCRIBE_INITIAL_DELAY: Duration = Duration::from_millis(100);
/// 重訂閱重試的退避上限。
const RESUBSCRIBE_MAX_DELAY: Duration = Duration::from_secs(5);

/// 重連後尚未成功排入的訂閱重試隊列。
///
/// 退避參數刻意採用私有常數而不開放設定：實務上唯一的失敗成因是 rumqttc 的
/// request channel 短暫塞滿，毫秒級即恢復；沿用 [`ReconnectPolicy`] 的 60 秒
/// 上限只會讓訂閱空窗過久。
struct ResubscribeQueue {
    /// 待重試的訂閱選項。
    pending: Vec<SubscribeOptions>,
    /// 已重試輪數，自 1 起算。
    attempt: u32,
    /// 下一輪重試的絕對時點；`None` 代表目前沒有待重試項目。
    deadline: Option<tokio::time::Instant>,
}

impl Default for ResubscribeQueue {
    /// 建立尚無待重試項目的空隊列。
    fn default() -> Self {
        Self {
            pending: Vec::new(),
            attempt: 0,
            deadline: None,
        }
    }
}

impl ResubscribeQueue {
    /// 清除待重試項目並重設退避輪數。
    fn clear(&mut self) {
        self.pending.clear();
        self.attempt = 0;
        self.deadline = None;
    }

    /// 暫停重試並重設退避輪數，但保留尚未送達 broker 的待辦項目。
    fn suspend(&mut self) {
        self.attempt = 0;
        self.deadline = None;
    }

    /// 保存失敗項目，並依目前輪數設定下一輪的絕對重試時點。
    fn arm(&mut self, failed: Vec<SubscribeOptions>) {
        self.attempt = self.attempt.saturating_add(1);
        let multiplier = 1_u32
            .checked_shl(self.attempt.saturating_sub(1))
            .unwrap_or(u32::MAX);
        let delay = RESUBSCRIBE_INITIAL_DELAY
            .saturating_mul(multiplier)
            .min(RESUBSCRIBE_MAX_DELAY);
        self.pending = failed;
        self.deadline = Some(tokio::time::Instant::now() + delay);
    }

    /// 判斷目前是否沒有待重試項目。
    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

pub(crate) async fn run_event_loop(
    inner: Arc<ClientInner>,
    mut event_loop: BackendEventLoop,
    reconnect: ReconnectPolicy,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut attempt = 0_u32;
    let mut connected = false;
    let mut ever_connected = false;
    let mut resubscribe = ResubscribeQueue::default();

    loop {
        if *shutdown.borrow() {
            shutdown_gracefully(&inner, &mut event_loop, connected).await;
            break;
        }

        inner.registry.flush_cleanup(&inner.backend);
        let polled = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() {
                    shutdown_gracefully(&inner, &mut event_loop, connected).await;
                    break;
                }
                if *shutdown.borrow() {
                    shutdown_gracefully(&inner, &mut event_loop, connected).await;
                    break;
                }
                continue;
            }
            () = async {
                match resubscribe.deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                let candidates = std::mem::take(&mut resubscribe.pending);
                let next_attempt = resubscribe.attempt.saturating_add(1);
                let still_failed = try_resubscribe_round(&inner, candidates, next_attempt);
                if still_failed.is_empty() {
                    resubscribe.clear();
                } else {
                    resubscribe.arm(still_failed);
                    debug_assert!(!resubscribe.is_empty());
                }
                continue;
            }
            _ = inner.registry.changed.notified() => continue,
            _ = inner.backend.operations.changed.notified(), if inner.registry.has_cleanup() => continue,
            result = event_loop.poll(&inner.backend) => result,
        };

        match polled {
            Ok(BackendEvent::Connected { session_present }) => {
                connected = true;
                attempt = 0;

                let restoring = ever_connected;
                ever_connected = true;
                if !session_present && !restoring {
                    // 初次連線的要求仍在我們的有界 channel，無須重複訂閱。
                    resubscribe.clear();
                } else if !session_present {
                    resubscribe.clear();
                    let failed =
                        try_resubscribe_round(&inner, inner.registry.snapshot_options(), 1);
                    if !failed.is_empty() {
                        resubscribe.arm(failed);
                        debug_assert!(!resubscribe.is_empty());
                    }
                } else if resubscribe.is_empty() {
                    resubscribe.clear();
                } else {
                    let pending = std::mem::take(&mut resubscribe.pending);
                    let still_failed = try_resubscribe_round(&inner, pending, 1);
                    if still_failed.is_empty() {
                        resubscribe.clear();
                    } else {
                        resubscribe.arm(still_failed);
                        debug_assert!(!resubscribe.is_empty());
                    }
                }
                // 先排入恢復訂閱的要求，再喚醒觀察者，避免它們立即 publish 搶先入列。
                inner.state.send_replace(ConnectionState::Connected);
                tracing::info!(session_present, "MQTT 已連線");
                let _send_result = inner
                    .events
                    .send(ConnectionEvent::Connected { session_present });
            }
            Ok(BackendEvent::Publish(message)) => inner.registry.dispatch(*message),
            Ok(BackendEvent::DisconnectSent) => break,
            Ok(
                BackendEvent::Other
                | BackendEvent::Outgoing { .. }
                | BackendEvent::Acknowledged { .. },
            ) => {}
            Err(BackendError::Authentication(reason)) => {
                tracing::error!(%reason, "MQTT 認證失敗，停止重試");
                let _send_result = inner.events.send(ConnectionEvent::ConnectionFailed {
                    error: format!("認證失敗：{reason}"),
                });
                break;
            }
            Err(BackendError::Protocol(reason)) => {
                tracing::error!(%reason, "MQTT 協定錯誤，停止重試");
                let _send_result = inner.events.send(ConnectionEvent::ConnectionFailed {
                    error: format!("協定錯誤：{reason}"),
                });
                break;
            }
            Err(BackendError::Connection(reason)) => {
                connected = false;
                resubscribe.suspend();
                tracing::warn!(%reason, "MQTT 連線中斷");
                let _send_result = inner.events.send(ConnectionEvent::Disconnected {
                    reason: reason.clone(),
                });
                attempt = attempt.saturating_add(1);
                let random_unit = rand::rng().random_range(0.0..=1.0);
                let Some(next_delay) = reconnect.delay_for(attempt, random_unit) else {
                    tracing::error!(attempt, %reason, "MQTT 重試次數已用盡");
                    let _send_result = inner.events.send(ConnectionEvent::ConnectionFailed {
                        error: format!("重試次數用盡：{reason}"),
                    });
                    break;
                };
                inner.state.send_replace(ConnectionState::Reconnecting);
                let _send_result = inner.events.send(ConnectionEvent::Reconnecting {
                    attempt,
                    next_delay,
                });
                tracing::info!(attempt, ?next_delay, "等待 MQTT 重連");
                tokio::select! {
                    _ = tokio::time::sleep(next_delay) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() {
                            shutdown_gracefully(
                                &inner,
                                &mut event_loop,
                                connected,
                            )
                            .await;
                            break;
                        }
                        if *shutdown.borrow() {
                            shutdown_gracefully(
                                &inner,
                                &mut event_loop,
                                connected,
                            )
                            .await;
                            break;
                        }
                    }
                }
            }
        }
    }

    inner.close();
    tracing::info!("MQTT 背景事件迴圈已停止");
}

/// 嘗試非阻塞排入一輪自動重訂閱，並回傳仍需重試的項目。
fn try_resubscribe_round(
    inner: &ClientInner,
    candidates: Vec<SubscribeOptions>,
    attempt: u32,
) -> Vec<SubscribeOptions> {
    let mut failed = Vec::new();
    for options in candidates {
        let Some(result) = inner.registry.resubscribe(&options.filter, |current| {
            inner.backend.try_subscribe(current)
        }) else {
            continue;
        };
        match result {
            Ok(()) => {
                tracing::info!(
                    attempt,
                    topic_filter = %options.filter,
                    "自動重訂閱已排入"
                );
            }
            Err(error @ MqttError::UnsupportedFeature(_)) => {
                tracing::error!(
                    %error,
                    attempt,
                    topic_filter = %options.filter,
                    "自動重訂閱遇到永久性錯誤，停止重試"
                );
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    attempt,
                    topic_filter = %options.filter,
                    "自動重訂閱排入失敗，將於退避後重試"
                );
                let _send_result = inner.events.send(ConnectionEvent::ResubscribeFailed {
                    topic_filter: options.filter.as_str().to_owned(),
                    attempt,
                    error: error.to_string(),
                });
                failed.push(options);
            }
        }
    }
    failed
}

/// 事件迴圈自己排入 DISCONNECT；滿載時繼續 poll，整段關閉最多兩秒。
async fn shutdown_gracefully(
    inner: &ClientInner,
    event_loop: &mut BackendEventLoop,
    connected: bool,
) {
    if !connected {
        return;
    }
    let flush = async {
        let mut queued = false;
        loop {
            if !queued {
                queued = inner.backend.try_disconnect().is_ok();
            }
            match event_loop.poll(&inner.backend).await {
                Ok(BackendEvent::DisconnectSent) | Err(_) => break,
                Ok(BackendEvent::Publish(message)) => inner.registry.dispatch(*message),
                Ok(
                    BackendEvent::Connected { .. }
                    | BackendEvent::Other
                    | BackendEvent::Outgoing { .. }
                    | BackendEvent::Acknowledged { .. },
                ) => {}
            }
        }
    };
    if tokio::time::timeout(Duration::from_secs(2), flush)
        .await
        .is_err()
    {
        inner
            .shutdown_timed_out
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use mqtt_kit_core::{QoS, SubscribeOptions};

    use super::{RESUBSCRIBE_MAX_DELAY, ResubscribeQueue};
    use std::time::Duration;
    use tokio::time::Instant;

    /// 建立供重訂閱隊列測試使用的訂閱選項。
    fn test_options() -> SubscribeOptions {
        SubscribeOptions::new("queue/test", QoS::AtLeastOnce).expect("valid test filter")
    }

    /// 將一筆項目排入下一輪，並驗證實際 deadline 對應預期延遲。
    fn arm_and_assert_delay(
        queue: &mut ResubscribeQueue,
        options: &SubscribeOptions,
        expected: Duration,
    ) {
        let earliest = Instant::now() + expected;
        queue.arm(vec![options.clone()]);
        let latest = Instant::now() + expected;
        let deadline = queue.deadline.expect("armed queue has deadline");
        assert!(deadline >= earliest);
        assert!(deadline <= latest);
    }

    /// 驗證清除會移除全部待辦並重設重試狀態。
    #[test]
    fn clear_removes_pending_items() {
        let options = test_options();
        let mut queue = ResubscribeQueue::default();
        arm_and_assert_delay(&mut queue, &options, Duration::from_millis(100));

        queue.clear();

        assert!(queue.pending.is_empty());
        assert_eq!(queue.attempt, 0);
        assert!(queue.deadline.is_none());
    }

    /// 驗證暫停只重設退避狀態，不會遺失待辦項目。
    #[test]
    fn suspend_preserves_pending_items() {
        let options = test_options();
        let mut queue = ResubscribeQueue::default();
        arm_and_assert_delay(&mut queue, &options, Duration::from_millis(100));

        queue.suspend();

        assert_eq!(queue.pending.len(), 1);
        assert_eq!(queue.pending[0].filter, options.filter);
        assert_eq!(queue.attempt, 0);
        assert!(queue.deadline.is_none());
    }

    /// 驗證退避會由 100ms 倍增，並在 5 秒封頂。
    #[test]
    fn arm_uses_capped_exponential_backoff() {
        let options = test_options();
        let mut queue = ResubscribeQueue::default();
        let expected = [100, 200, 400, 800, 1_600, 3_200]
            .map(Duration::from_millis)
            .into_iter()
            .chain([RESUBSCRIBE_MAX_DELAY; 2]);

        for delay in expected {
            arm_and_assert_delay(&mut queue, &options, delay);
        }
        assert_eq!(queue.attempt, 8);
    }

    /// 驗證大量重試不會溢位，且延遲會穩定維持在上限。
    #[test]
    fn arm_handles_large_attempts_without_overflow() {
        let options = test_options();
        let mut queue = ResubscribeQueue::default();

        for delay in [100, 200, 400, 800, 1_600, 3_200].map(Duration::from_millis) {
            arm_and_assert_delay(&mut queue, &options, delay);
        }
        for _ in 6..40 {
            arm_and_assert_delay(&mut queue, &options, RESUBSCRIBE_MAX_DELAY);
        }

        assert_eq!(queue.attempt, 40);
    }
}
