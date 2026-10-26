# 使用指南

`mqtt-kit` 是以成熟的 [`rumqttc`](https://crates.io/crates/rumqttc) 為底層引擎、面向產品程式碼的
Rust 非同步 MQTT library。公開 API 統一 MQTT 3.1.1 與 MQTT 5，並內建斷線重連、自動重訂閱、
明確錯誤、bounded 訊息路由及跨平台 rustls TLS。所有 payload 從公開 API 到接收路由皆使用
`bytes::Bytes`，clone 只增加參照計數。

所有命令與 Cargo path 以下均以倉庫根目錄為基準。

## Workspace

```text
crates/
├── mqtt-kit-core/         # 不依賴 rumqttc 的共通型別
│   └── src/
│       ├── error.rs       # MqttError
│       ├── message.rs     # Message、MQTT 5 PUBLISH properties
│       ├── options.rs     # publish/subscribe/TLS/Last Will 選項
│       ├── qos.rs         # QoS、ProtocolVersion
│       ├── reconnect.rs   # 退避策略、連線狀態與事件
│       └── topic.rs       # TopicFilter 驗證及 wildcard 比對
└── mqtt-kit/              # rumqttc backend 與產品 API
    └── src/
        ├── backend/       # MQTT 3.1.1 / 5 enum 分派與型別轉換
        ├── builder.rs     # URL、feature、TLS 與組態驗證
        ├── client.rs      # Clone handle 與公開操作
        ├── event_loop.rs  # 背景 poll、重連、自動重訂閱
        └── subscription.rs# 註冊表、wildcard 路由與 bounded Stream
```

`mqtt-kit` 會 re-export `mqtt-kit-core` 的全部公開型別，應用程式通常只需依賴主 crate。

## 本地安裝（尚未發布套件）

```toml
[dependencies]
mqtt-kit = { path = "crates/mqtt-kit" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
bytes = "1"
futures-util = "0.3" # 使用 StreamExt 時才需要
```

### Cargo features

預設 features 全部關閉；帳密認證及 MQTT 3.1.1/5 版本選擇都是 runtime 設定。

| feature | 功能 |
|---|---|
| `tls` | 啟用 rustls TLS 機制，但不選擇 crypto provider；適合由最終 binary 安裝全域 provider 或傳入完整 `ClientConfig` |
| `tls-ring` | 啟用 `tls` 與 ring provider；跨平台建置負擔低，適合一般用途 |
| `tls-aws-lc` | 啟用 `tls` 與 AWS-LC provider；適合需要 AWS-LC 或進一步配置 FIPS 的應用 |
| `websocket` | 支援 `ws://`；`wss://` 需同時啟用 `tls` |

例如：

```toml
mqtt-kit = { path = "crates/mqtt-kit", features = ["tls-ring", "websocket"] }
```

一般跨平台應用建議選 `tls-ring`。若需要 FIPS，應選 AWS-LC 路線，並由最終 binary
額外啟用 rustls 的 `fips` feature、建立 FIPS provider，再透過全域 provider 或
`tls_client_config()` 套用；`tls-aws-lc` 本身不等同已完成 FIPS 設定。Windows 編譯
`tls-aws-lc` 需要 CMake 與 NASM。Cargo feature 會做聯集，因此 `tls-ring` 與 `tls-aws-lc`
被不同依賴同時啟用是合法情境。

TLS 設定不可搭配 `mqtt://` 或 `ws://`，否則回傳 `InvalidConfig`；未啟用 TLS feature 卻呼叫 `tls()` 會回傳 `UnsupportedFeature`。keep-alive 的允許範圍為 V3.1.1 的 1–65535 秒與 V5 的 5–65535 秒，超出範圍會回傳 `InvalidConfig`。

若未啟用對應 feature 卻使用 `mqtts://`、`ws://` 或 `wss://`，`connect()` 會回傳
`MqttError::UnsupportedFeature`，不會 panic 或默默降級。

## 快速上手

```rust,no_run
use std::time::Duration;
use mqtt_kit::{LastWill, MqttClient, ProtocolVersion, QoS, ReconnectPolicy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = MqttClient::builder("device-01")
        .broker("mqtt://localhost:1883")
        .protocol(ProtocolVersion::V5)
        .credentials("user", "pass")
        .keep_alive(Duration::from_secs(30))
        .clean_start(false)
        .last_will(LastWill::new(
            "status/device-01",
            "offline",
            QoS::AtLeastOnce,
            true,
        ))
        .reconnect(ReconnectPolicy::exponential(
            Duration::from_millis(500),
            Duration::from_secs(60),
        ))
        .connect()
        .await?;

    let mut subscription = client
        .subscribe("sensor/+/temperature", QoS::AtLeastOnce)
        .await?;

    client
        .publish("sensor/kitchen/temperature", "24.5", QoS::AtLeastOnce)
        .await?;

    if let Some(message) = subscription.recv().await {
        assert_eq!(message.topic, "sensor/kitchen/temperature");
    }

    client.disconnect().await?;
    Ok(())
}
```

`MqttClient` 是輕量的 `Arc` handle，可安全 clone，並實作 `Send + Sync` 語意。`connect()` 驗證設定並
啟動背景任務；可透過 `state()` 的 `tokio::watch::Receiver` 等待即時狀態，或透過 `events()` 的
`tokio::broadcast::Receiver` 觀察生命週期事件。最後一個 `MqttClient` handle 被 Drop 時會自動停止
背景任務並關閉 broker 連線；不需要額外保留管理用 handle。

## 發佈與 MQTT 5 properties

一般發佈使用 `publish()`；retained message 或 MQTT 5 properties 使用 `publish_with()`：

```rust,no_run
# use mqtt_kit::{MqttClient, PublishOptions, QoS};
# async fn publish(client: &MqttClient) -> Result<(), mqtt_kit::MqttError> {
client.publish_with(
    PublishOptions::new("device/telemetry", "payload", QoS::AtLeastOnce)
        .retain(true)
        .content_type("application/json")
        .message_expiry_interval(60)
        .user_property("trace-id", "abc-123"),
).await?;
# Ok(()) }
```

在 V3.1.1 client 使用任何 v5 PUBLISH/Last Will/訂閱屬性會明確回傳
`MqttError::UnsupportedFeature`，不會忽略屬性。

## 訂閱、路由與背壓

`Subscription` 實作 `futures_core::Stream<Item = Message>`，也提供不需要 extension trait 的
`recv().await`。每個 subscription 有獨立 bounded queue，預設為 128 筆／4 MiB，所有佇列合計 16 MiB；可用 `subscription_capacity()`、`subscription_byte_capacity()`、`buffer_byte_capacity()` 調整。事件迴圈絕不等待消費者；自身 queue 滿時會丟棄最舊訊息，單筆過大或全域預算不足則丟棄新訊息；首次丟棄送出 `tracing::warn!`，累計數可由 `dropped_messages()` 觀察。

本地路由使用已驗證的 `TopicFilter`，完整支援 `+`、`#`、空 topic 層級，以及 MQTT 規範對
`$SYS` 類系統 topic 的隔離規則。同一 filter 可有多個本地 `Subscription`；只有最後一個被 Drop
或主動取消時才安排 UNSUBSCRIBE；滿載時保留有界清理項目，稍後重試。若需要得知取消操作是否成功，使用
`subscription.unsubscribe().await`；`client.unsubscribe(filter).await` 則關閉相同 filter 的所有
本地串流。

同一個 filter 重複呼叫 `subscribe` 時，註冊表保存的 QoS 與 MQTT 5 訂閱選項以仍存活的最新本地訂閱為準；
若之後需要自動重訂閱，會使用這組最新 options。既有的本地 `Subscription` 串流仍會各自收到符合
filter 的訊息。

## 重連與自動重訂閱

底層 `poll()` 斷線後，背景任務依 `ReconnectPolicy` 做含 jitter 的 capped exponential backoff。
連線生命週期如下：

1. 斷線時送出 `Disconnected`。
2. 每次退避前更新狀態為 `Reconnecting`，並送出含 attempt 與 delay 的 `Reconnecting`。
3. rumqttc 在下次 poll 自動重建傳輸；收到成功 CONNACK 後 attempt 歸零。
4. CONNACK 的 `session_present` 為 `false` 時，以非阻塞方式重送註冊表中的全部訂閱；request
   channel 暫時滿載而排入失敗的訂閱，會從 100ms 開始指數退避、封頂 5 秒且不限次數重試。
   每輪每個仍失敗的 filter 都會送出 `ResubscribeFailed`，包含 filter、attempt 與錯誤摘要；
   期間若使用者已取消該 filter，會直接從重試隊列移除。再次斷線時隊列與輪數會重設，待下次
   CONNACK 重新取得註冊表快照。
5. `session_present` 為 `true` 時代表 broker 已保存既有訂閱，因此清除重試隊列並跳過重送，
   避免 MQTT 5 retained message 再次觸發或 subscription identifier 被重設。應用程式不必
   手動重訂閱。
6. 認證拒絕等不可恢復錯誤立即送出 `ConnectionFailed` 並關閉；一般錯誤超過 `max_retries`
   也會關閉。此後 publish/subscribe 等操作回傳 `MqttError::ClientClosed`。

`ReconnectPolicy::delay_for(attempt, random_unit)` 及 `delays_with()` 是純計算 API，可在應用測試中
注入固定亂數。`attempt` 從 1 起算；`max_retries: None` 表示無限重試。

`disconnect().await` 在第一次等待前送出關閉訊號，事件迴圈自己排入 DISCONNECT，整段最多兩秒；逾時回傳 Timeout。取消等待者不會取消關閉，多位等待者共享相同結果。最後一個 handle 被 Drop 也會請求關閉；runtime 被摧毀時不保證封包送達，但狀態會標記為 Closed，串流與待確認要求會被喚醒結束。未連線時直接關閉。

確認 API 與取消、逾時的語意見 [README](../README.md#等待-broker-確認)。`subscribe_confirmed()` 等待成功 SUBACK；`publish_confirmed()` 等待 PUBACK（QoS1）或 PUBCOMP（QoS2）；`unsubscribe_confirmed()` 等待 UNSUBACK。QoS0 不提供 broker 確認。確認前斷線會回報 Connection，已送出的操作不會被撤回。

本地路由採 arena topic trie，保留 wildcard 與 $SYS 行為，不必對每筆訊息掃描無關 filter。內容位元組預算包含訊息本體與字串／payload／properties 長度，各佇列分別計數，不是程序 RSS 預算；closed 串流仍可讀取緩衝，因此在讀完或 Drop 前仍佔用預算。

## MQTT 3.1.1 與 MQTT 5 差異

| 能力 | V3.1.1 | V5 |
|---|---:|---:|
| QoS、retain、Last Will、帳密 | 支援 | 支援 |
| PUBLISH user properties / content type / expiry | 不支援，回錯 | 支援並保留於 `Message.v5` |
| response topic / correlation data | 不支援，回錯 | 支援 |
| no-local / retain-as-published / retain handling | 不支援，回錯 | 支援 |
| Will properties | 不支援，回錯 | 支援 |

版本由 `.protocol(ProtocolVersion::V311)` 或 `.protocol(ProtocolVersion::V5)` 在 runtime 選擇，
不使用 Cargo feature，以便同一 binary 建立不同版本的 client。

## TLS 與 mTLS

使用 `TlsOptions` 時，未指定自訂 CA 會使用 `webpki-roots` 內建的 Mozilla 根憑證，不讀取 OS
憑證庫，因此 Linux、Windows、macOS 與 WSL 行為一致。最簡單的做法是啟用 `tls-ring`：

```rust,no_run
# use mqtt_kit::{MqttClient, TlsOptions};
# async fn tls(ca_pem: Vec<u8>, cert_pem: Vec<u8>, key_pem: Vec<u8>) -> Result<(), mqtt_kit::MqttError> {
let client = MqttClient::builder("secure-device")
    .broker("mqtts://broker.example.com:8883")
    .tls(
        TlsOptions::new()
            .ca_pem(ca_pem)
            .client_auth_pem(cert_pem, key_pem),
    )
    .connect()
    .await?;
# client.disconnect().await?;
# Ok(()) }
```

自訂 CA、client certificate 與 private key 均採 PEM。無法解析、空憑證鏈或不相符的 cert/key
會在 `connect()` 回 `InvalidConfig`。

Library 不會呼叫 `CryptoProvider::install_default()`；選擇程序依序為：

1. 使用最終 binary 已安裝、可由 `CryptoProvider::get_default()` 取得的全域 provider。
2. 若未安裝全域 provider 且有 `tls-aws-lc`，使用 AWS-LC。
3. 否則若有 `tls-ring`，使用 ring。
4. 若只有 bare `tls`，`connect()` 回傳 `InvalidConfig`，指引呼叫端選 feature、由 binary
   安裝全域 provider，或改用 `tls_client_config()`。

所以兩個 provider feature 同時開啟時會偏好 AWS-LC，與 rustls 上游預設一致；binary 安裝的
全域 provider 永遠優先。安裝 process-wide 預設值是最終 binary 的職權，不應由 library 代替決定。

若要使用任意 provider、FIPS 或客製憑證驗證器，可直接傳入完整設定。以下由 binary 明確以 ring
建立設定；這條路徑會略過上述 provider 選擇：

```rust,no_run
use std::sync::Arc;

use mqtt_kit::MqttClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();

    let client = MqttClient::builder("secure-device")
        .broker("mqtts://broker.example.com:8883")
        .tls_client_config(Arc::new(config))
        .connect()
        .await?;

    client.disconnect().await?;
    Ok(())
}
```

此例的應用程式需直接依賴 `rustls`（含 `std`、`ring`）與 `webpki-roots`，mqtt-kit 則只需啟用
`tls`。因 `rustls::ClientConfig` 已進入公開 API，mqtt-kit 日後升級 rustls major version 會是
breaking change。`tls_client_config()` 與 `tls(TlsOptions)` 互斥；同時設定會回 `InvalidConfig`。

## 錯誤處理指南

所有可預期失敗均使用 `MqttError`：

- `InvalidConfig` / `InvalidTopic`：呼叫端設定錯誤，修正後再建立 client。
- `UnsupportedFeature`：編譯 feature 或 runtime 協定版本不符。
- `Connection`：操作無法排入底層事件迴圈；保留 `#[source]` 錯誤鏈供診斷。
- `AuthenticationFailed`：帳密或 broker 授權拒絕；不應盲目重試。
- `Protocol`：broker reason code 或封包狀態不相容。
- `Timeout`：上層設定期限時可用於統一表達逾時。
- `ClientClosed`：背景重試已用盡、不可恢復錯誤，或 client 已 disconnect。

背景連線錯誤透過 `ConnectionEvent` 回報；應用程式可集中記錄及告警。Library 本身以 `tracing`
記錄連線、斷線、重試、重訂閱與訊息丟棄，不直接輸出到 stdout。

## 跨平台

本 workspace 不使用平台特定 API 或 `cfg(unix)` / `cfg(windows)` 分支。網路與 async runtime
由 Tokio 提供；TLS 使用 rustls 與一致的 `webpki-roots`，不依賴 OpenSSL 或 OS certificate
store。`tls-ring` 不需要 AWS-LC 的 CMake/NASM 建置鏈；選用 `tls-aws-lc` 時，Windows 需備妥
CMake 與 NASM。支援目標為 Linux、Windows、macOS，亦支援 WSL。

## Examples 與驗證

- `cargo run -p mqtt-kit --example basic_pubsub`
- `cargo run -p mqtt-kit --example v5_properties`
- `cargo run -p mqtt-kit --features tls-ring --example tls`

整合測試在程序內啟動 `rumqttd`，所有 listener 先向 OS 取得隨機埠，不需要 Docker。重連測試
另使用 Tokio TCP proxy 主動切斷既有連線，以驗證重連後原 subscription 仍可接收訊息。

```bash
cargo build --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
cargo build --workspace
```

## CI/CD

GitHub Actions 的 `ci.yml` 會在 `main`、`master` 的 push 及所有 pull request 執行：格式檢查、
Clippy、rustdoc、Rust 1.86 MSRV check，以及 stable Rust 在 Linux、Windows、macOS 的完整測試、
無 feature build 與 all-features examples build。同一分支有新執行時會取消舊的 CI run。

發版時將 workspace 版本更新為目標版本後推送 `vX.Y.Z` tag。`release.yml` 會先確認 tag 與
`workspace.package.version` 完全一致，再於三個平台執行完整測試；全部通過後才自動建立含產生式
release notes 的 GitHub Release。版本不一致或任一平台測試失敗都不會發版。


## 依賴安全

持續安全檢查使用 `python3 scripts/security_audit.py`，會核對最新 RustSec 公告。精確例外與到期日見 `scripts/security-exceptions.json`；說明見 [安全政策](../SECURITY.md)。Debug 會遮蔽 Builder 中的帳密、URL、遺囑內容、TLS 設定及 TlsOptions 的憑證與私鑰。應用仍應避免將 Message 或 PublishOptions 中的敏感 payload 寫入日誌。

供應鏈另執行 `cargo deny --locked check --hide-inclusion-graph`，必須與精確 RustSec gate 一起通過；詳見 [安全政策](../SECURITY.md#供應鏈政策)。
