use std::time::Duration;

use crate::MqttError;

/// 斷線後的指數退避策略。
#[derive(Debug, Clone, PartialEq)]
pub struct ReconnectPolicy {
    /// 第一次重試前的等待時間。
    pub initial_delay: Duration,
    /// 單次等待的上限。
    pub max_delay: Duration,
    /// 每次重試的指數倍率，必須至少為 `1.0`。
    pub multiplier: f64,
    /// 隨機抖動比例，範圍為 `0.0..=1.0`。
    pub jitter: f64,
    /// 最大重試次數；`None` 表示無限重試。
    pub max_retries: Option<u32>,
}

impl ReconnectPolicy {
    /// 建立預設倍率 `2.0`、抖動 `0.2` 的指數退避策略。
    #[must_use]
    pub fn exponential(initial: Duration, max: Duration) -> Self {
        Self {
            initial_delay: initial,
            max_delay: max,
            multiplier: 2.0,
            jitter: 0.2,
            max_retries: None,
        }
    }

    /// 設定指數倍率。
    #[must_use]
    pub fn multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    /// 設定隨機抖動比例。
    #[must_use]
    pub fn jitter(mut self, jitter: f64) -> Self {
        self.jitter = jitter;
        self
    }

    /// 設定最大重試次數；`None` 表示永不因次數用盡而停止。
    #[must_use]
    pub fn max_retries(mut self, max_retries: Option<u32>) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// 驗證策略是否可安全計算。
    pub fn validate(&self) -> Result<(), MqttError> {
        if self.initial_delay.is_zero() {
            return Err(MqttError::InvalidConfig("initial_delay 必須大於零".into()));
        }
        if self.max_delay < self.initial_delay {
            return Err(MqttError::InvalidConfig(
                "max_delay 不可小於 initial_delay".into(),
            ));
        }
        if !self.multiplier.is_finite() || self.multiplier < 1.0 {
            return Err(MqttError::InvalidConfig(
                "multiplier 必須是至少 1.0 的有限數值".into(),
            ));
        }
        if !self.jitter.is_finite() || !(0.0..=1.0).contains(&self.jitter) {
            return Err(MqttError::InvalidConfig(
                "jitter 必須介於 0.0 與 1.0".into(),
            ));
        }
        Ok(())
    }

    /// 計算指定重試次數的延遲。
    ///
    /// `attempt` 從 `1` 起算；`random_unit` 應位於 `0.0..=1.0`，方便測試注入固定亂數。
    /// 超過 `max_retries` 時回傳 `None`。
    #[must_use]
    pub fn delay_for(&self, attempt: u32, random_unit: f64) -> Option<Duration> {
        if attempt == 0 || self.max_retries.is_some_and(|max| attempt > max) {
            return None;
        }

        let exponent = i32::try_from(attempt.saturating_sub(1)).unwrap_or(i32::MAX);
        let base = (self.initial_delay.as_secs_f64() * self.multiplier.powi(exponent))
            .min(self.max_delay.as_secs_f64());
        let random_unit = random_unit.clamp(0.0, 1.0);
        let factor = 1.0 + self.jitter * (2.0 * random_unit - 1.0);
        Some(Duration::from_secs_f64(
            (base * factor).clamp(0.0, self.max_delay.as_secs_f64()),
        ))
    }

    /// 以可注入亂數函式產生前 `count` 次延遲；遇到重試上限便提早結束。
    #[must_use]
    pub fn delays_with<F>(&self, count: u32, mut random: F) -> Vec<Duration>
    where
        F: FnMut() -> f64,
    {
        (1..=count)
            .map_while(|attempt| self.delay_for(attempt, random()))
            .collect()
    }
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self::exponential(Duration::from_millis(500), Duration::from_secs(60))
    }
}

/// MQTT client 的即時連線狀態。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// 正在建立初始連線。
    Connecting,
    /// 已與 broker 連線。
    Connected,
    /// 斷線且正在等待或嘗試重連。
    Reconnecting,
    /// 已主動關閉或因不可恢復錯誤終止。
    Closed,
}

/// MQTT 連線生命週期事件。
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionEvent {
    /// 已連線，並指出 broker 是否延續既有 session。
    Connected {
        /// CONNACK 的 session present 旗標。
        session_present: bool,
    },
    /// 即將進行第 `attempt` 次重連。
    Reconnecting {
        /// 從一開始算的重試次數。
        attempt: u32,
        /// 下一次 poll 前的退避時間。
        next_delay: Duration,
    },
    /// 連線剛中斷；通常接著會送出 `Reconnecting`。
    Disconnected {
        /// 底層錯誤摘要。
        reason: String,
    },
    /// 自動重訂閱失敗，將於退避後重試。
    ResubscribeFailed {
        /// 失敗的 topic filter。
        topic_filter: String,
        /// 已重試輪數，自 1 起算；首次連線後立即失敗時為 1。
        attempt: u32,
        /// 失敗原因摘要。
        error: String,
    },
    /// 不可重試，或重試次數已用盡。
    ConnectionFailed {
        /// 最終錯誤摘要。
        error: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_sequence_is_capped() {
        let policy =
            ReconnectPolicy::exponential(Duration::from_millis(100), Duration::from_millis(450))
                .jitter(0.0);
        assert_eq!(
            policy.delays_with(5, || 0.5),
            [100, 200, 400, 450, 450].map(Duration::from_millis)
        );
    }

    #[test]
    fn fixed_random_controls_jitter() {
        let policy = ReconnectPolicy::exponential(Duration::from_secs(10), Duration::from_secs(30))
            .jitter(0.25);
        assert_eq!(policy.delay_for(1, 0.0), Some(Duration::from_millis(7_500)));
        assert_eq!(
            policy.delay_for(1, 1.0),
            Some(Duration::from_millis(12_500))
        );
    }

    #[test]
    fn max_retries_ends_sequence() {
        let policy = ReconnectPolicy::default().jitter(0.0).max_retries(Some(2));
        assert_eq!(policy.delays_with(5, || 0.5).len(), 2);
        assert_eq!(policy.delay_for(3, 0.5), None);
    }

    #[test]
    fn validation_rejects_bad_values() {
        assert!(
            ReconnectPolicy::default()
                .multiplier(0.5)
                .validate()
                .is_err()
        );
        assert!(ReconnectPolicy::default().jitter(1.1).validate().is_err());
        assert!(
            ReconnectPolicy::exponential(Duration::ZERO, Duration::from_secs(1))
                .validate()
                .is_err()
        );
    }
}
