# mqtt-kit

Async MQTT 3.1.1／5 工具庫，基於 Tokio 與 rumqttc，提供自動重連、訂閱恢復、有界訊息串流及 rustls TLS／mTLS。

本 crate 是主要應用入口；共通型別由 mqtt-kit-core 提供並在此重新匯出。需要 Rust 1.86+。

## 目前安裝方式

目前尚未宣告 crates.io 發布，可先透過 Git 使用：

```toml
[dependencies]
mqtt-kit = { git = "https://github.com/liam8846/mqtt-kit", features = ["tls-aws-lc"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

正式應用建議固定已審查的 Git revision，並保留 Cargo.lock。

## 最小範例

```rust,no_run
use mqtt_kit::{MqttClient, QoS};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = MqttClient::builder("demo")
        .broker("mqtt://127.0.0.1:1883")
        .connect()
        .await?;
    let mut messages = client.subscribe("demo/hello", QoS::AtLeastOnce).await?;
    client.publish("demo/hello", "Hello, MQTT!", QoS::AtLeastOnce).await?;
    if let Some(message) = messages.recv().await {
        println!("{}: {:?}", message.topic, message.payload);
    }
    client.disconnect().await?;
    Ok(())
}
```

此範例需要本機 broker，接收會等待訊息；完整的逾時及連線狀態處理見倉庫中的 quick_start 範例。

## 確認與資源控制

既有 publish／subscribe 保留排入佇列即返回的語意。新增 publish_confirmed、subscribe_confirmed 與 unsubscribe_confirmed，可附上整段逾時限制並等待對應 broker ACK。QoS2 會等待 PUBCOMP，QoS0 不提供 broker 確認；broker 拒絕、逾時與確認前斷線會回傳錯誤。逾時或取消不會撤回已排入的發佈。

訂閱取消會回收註冊並安排有界遠端清理。關閉訊號不依賴呼叫者持續等待，背景最多兩秒嘗試送達 DISCONNECT；逾時回傳 Timeout。

預設每個訂閱限制 128 筆／4 MiB，所有佇列合計 16 MiB，最多 1024 個串流與待清理 filter。可透過 builder 調整；超出預算可能丟棄訊息或回傳 ResourceLimit。可由 Subscription::dropped_messages 與 buffered_bytes 觀察緩衝。這是邏輯內容預算，並非程序 RSS 上限。

## Features

| Feature | 功能 |
| --- | --- |
| 預設 | 明文 TCP；協定版本在 runtime 選擇 |
| tls | rustls TLS 機制；由應用提供 crypto provider 或 ClientConfig |
| tls-ring | TLS + ring provider，相容選項 |
| tls-aws-lc | TLS + AWS-LC provider，主要推薦方案 |
| websocket | ws://，搭配 TLS feature 可使用 wss:// |

目前鎖定 aws-lc-rs 1.18.2／aws-lc-sys 0.46.0。一般非 FIPS 建置需要 C/C++ 編譯器；Windows x86／x86-64 建議安裝 NASM，CI 也使用 NASM。標準非 FIPS 建置不必強制安裝 CMake；FIPS 或特殊建置另有工具需求，詳見 [AWS-LC 建置文件](https://aws.github.io/aws-lc-rs/requirements/index.html)。

跨網路傳送帳密或敏感資料時請使用 mqtts://／wss://。TLS 設定搭配明文 scheme 會回傳錯誤。訊息佇列滿載時會丟棄最舊訊息；publish／subscribe 成功代表要求已排入底層佇列，並非 broker 確認。

- [專案首頁與快速開始](https://github.com/liam8846/mqtt-kit#readme)
- [完整使用指南](https://github.com/liam8846/mqtt-kit/blob/main/docs/guide.md)
- [安全政策與依賴例外](https://github.com/liam8846/mqtt-kit/blob/main/SECURITY.md)

授權：MIT OR Apache-2.0。套件內附 LICENSE-MIT 與 LICENSE-APACHE。
