# Changelog

## Unreleased

- TLS 文件、範例與跨平台主要測試推薦使用 AWS-LC；保留 ring、bare TLS 與應用程式自訂 provider。
- 補齊 AWS-LC 單獨啟用時的自訂 ClientConfig 測試，保留雙 provider feature 聯集驗證，更新原生建置工具說明。

## 0.3.0 — 2026-10-27

- 修正訂閱要求在 await 時被取消後遺留本地註冊的問題，恢復仍存活的最新同 filter 設定。
- 遠端取消訂閱改由有界清理佇列重試，與新訂閱／重訂閱以同一註冊表鎖協調順序。
- 關閉訊號在第一次 await 前送出，取消等待者不會取消關閉；共享終止結果，兩秒後回報逾時，背景 task 被取消也會發布 Closed。
- 新增具逾時的 broker 確認 API：publish_confirmed、subscribe_confirmed、unsubscribe_confirmed。原有 API 保留入列語意。
- ACK 依操作、packet ID 與 QoS 階段關聯，處理拒絕與 session 恢復，未釋放 ID 被重用時拒絕誤配。
- 路由改為不遞迴的 arena topic trie，維持 wildcard、空層級與 $SYS 規則。
- **預設資源行為調整**：每佇列 4 MiB、全域佇列內容 16 MiB、最多 1024 個串流與待清理 filter。原有 128 筆容量仍保留；應用可調整 builder 預算並觀察丟棄計數。
- 新增 cargo-deny 0.20.2 的授權、來源、版本與漏洞稽核；與精確 RustSec checksum／到期 gate 同時執行。
- 補上取消、滿載、錯誤 ACK、否定 ACK、分段 QoS2、斷線、runtime 銷毀與記憶體預算的測試。

## 0.2.0 — 2026-10-26

首個公開基準，提供 MQTT 3.1.1／5、重連、訂閱恢復、有界串流與 rustls TLS／mTLS。

Tag：[v0.2.0](https://github.com/liam8846/mqtt-kit/tree/v0.2.0)。
