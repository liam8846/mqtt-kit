use std::{fmt, str::FromStr};

use crate::MqttError;

/// 已驗證、可安全用於訂閱與本地路由的 MQTT topic filter。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicFilter(String);

impl TopicFilter {
    /// 解析並驗證 topic filter。
    ///
    /// `+` 必須獨占一個層級；`#` 必須獨占最後一個層級。
    pub fn new(filter: impl Into<String>) -> Result<Self, MqttError> {
        let filter = filter.into();
        validate_filter(&filter)?;
        Ok(Self(filter))
    }

    /// 取得原始 topic filter 字串。
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 判斷 MQTT topic name 是否符合此 filter。
    ///
    /// 依 MQTT 規範，`$` 開頭的系統 topic 不會被首層 `+` 或 `#` 比中。
    #[must_use]
    pub fn matches(&self, topic: &str) -> bool {
        if !valid_topic_name(topic) {
            return false;
        }

        let mut filters = self.0.split('/');
        let mut topics = topic.split('/');

        if topic.starts_with('$') && matches!(filters.clone().next(), Some("+" | "#")) {
            return false;
        }

        loop {
            match (filters.next(), topics.next()) {
                (Some("#"), _) => return true,
                (Some("+"), Some(_)) => {}
                (Some(filter), Some(level)) if filter == level => {}
                (None, None) => return true,
                _ => return false,
            }
        }
    }
}

impl fmt::Display for TopicFilter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl AsRef<str> for TopicFilter {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for TopicFilter {
    type Err = MqttError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

fn validate_filter(filter: &str) -> Result<(), MqttError> {
    if filter.is_empty() {
        return Err(MqttError::InvalidTopic("topic filter 不可為空".into()));
    }
    if filter.as_bytes().contains(&0) {
        return Err(MqttError::InvalidTopic("topic filter 不可包含 NUL".into()));
    }

    let levels: Vec<_> = filter.split('/').collect();
    for (index, level) in levels.iter().enumerate() {
        if level.contains('#') && (*level != "#" || index + 1 != levels.len()) {
            return Err(MqttError::InvalidTopic(
                "`#` 必須獨占最後一個 topic 層級".into(),
            ));
        }
        if level.contains('+') && *level != "+" {
            return Err(MqttError::InvalidTopic(
                "`+` 必須獨占一個 topic 層級".into(),
            ));
        }
    }
    Ok(())
}

fn valid_topic_name(topic: &str) -> bool {
    !topic.is_empty()
        && !topic.as_bytes().contains(&0)
        && !topic.contains('+')
        && !topic.contains('#')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_filters() {
        for filter in ["#", "+", "sensor/+/temp", "a/#", "/+", "a//b", "$SYS/#"] {
            assert!(TopicFilter::new(filter).is_ok(), "{filter}");
        }
    }

    #[test]
    fn rejects_invalid_filters() {
        for filter in ["", "a/#/b", "a#", "a/+b", "a/##", "a\0b"] {
            assert!(TopicFilter::new(filter).is_err(), "{filter:?}");
        }
    }

    #[test]
    fn matches_wildcards_and_empty_levels() {
        let filter = TopicFilter::new("sensor/+/temp").expect("valid filter");
        assert!(filter.matches("sensor/kitchen/temp"));
        assert!(filter.matches("sensor//temp"));
        assert!(!filter.matches("sensor/kitchen/humidity"));
        assert!(!filter.matches("sensor/a/b/temp"));

        let all = TopicFilter::new("sport/#").expect("valid filter");
        assert!(all.matches("sport"));
        assert!(all.matches("sport/"));
        assert!(all.matches("sport/tennis/player1"));
    }

    #[test]
    fn protects_system_topics() {
        assert!(!TopicFilter::new("#").expect("valid").matches("$SYS/status"));
        assert!(
            !TopicFilter::new("+/status")
                .expect("valid")
                .matches("$SYS/status")
        );
        assert!(
            TopicFilter::new("$SYS/#")
                .expect("valid")
                .matches("$SYS/status")
        );
    }

    #[test]
    fn rejects_invalid_topic_names_during_match() {
        let filter = TopicFilter::new("#").expect("valid");
        assert!(!filter.matches(""));
        assert!(!filter.matches("a/+"));
        assert!(!filter.matches("a/#"));
        assert!(!filter.matches("a\0b"));
    }
}
