# mqtt-kit-core

mqtt-kit 的協定無關共通型別，包含訊息、QoS、topic filter、發佈／訂閱／TLS 選項、錯誤與重連策略。本 crate 不依賴 rumqttc 或 Tokio；需要 Rust 1.86+。

## 目前安裝方式

目前尚未宣告 crates.io 發布，可先透過 Git 使用：

```toml
[dependencies]
mqtt-kit-core = { git = "https://github.com/liam8846/mqtt-kit" }
```

一般 MQTT 應用可直接使用 mqtt-kit；它會重新匯出本 crate 的公開型別。

## Topic filter

```rust
use mqtt_kit_core::TopicFilter;

let filter = TopicFilter::new("sensor/+/temperature")?;
assert!(filter.matches("sensor/kitchen/temperature"));
assert!(!filter.matches("sensor/kitchen/humidity"));
# Ok::<(), mqtt_kit_core::MqttError>(())
```

TopicFilter 驗證 wildcard 位置與 NUL，並遵守 $SYS 系統 topic 的首層 wildcard 隔離規則。payload 使用 bytes::Bytes，clone 會共用 payload 儲存空間。

TlsOptions 的 Debug 會遮蔽憑證與私鑰；其他訊息／選項資料可能含應用 payload，不應任意寫入日誌。

- [專案首頁](https://github.com/liam8846/mqtt-kit#readme)
- [完整使用指南](https://github.com/liam8846/mqtt-kit/blob/main/docs/guide.md)
- [安全政策](https://github.com/liam8846/mqtt-kit/blob/main/SECURITY.md)

授權：MIT OR Apache-2.0。套件內附 LICENSE-MIT 與 LICENSE-APACHE。
