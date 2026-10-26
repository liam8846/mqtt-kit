# 安全修復結果

日期：2026-10-26。對應 [修復計畫](REMEDIATION_PLAN.md) 與[原始審計](2026-10-26.md)。

## 修復內容

| 原始問題 | 處理結果 |
| --- | --- |
| 實際 TLS 路徑的已知漏洞 | rustls 升至 0.23.46、rustls-webpki 升至 0.103.16、webpki-roots 升至 1.0.10；Tokio 升至 1.53.3。同步提高直接依賴的最低修補版本 |
| Debug 洩漏秘密 | 手寫 Builder 與 TlsOptions 的 Debug；遮蔽帳密、原始 URL、PEM、遺囑內容與自訂 ClientConfig，涵蓋一般及 pretty Debug |
| 明文 scheme 忽略 TLS 設定 | mqtt://、ws:// 與 TLS options／ClientConfig 同時出現時回傳 InvalidConfig；未編譯 TLS 時傳入 TLS options 回傳 UnsupportedFeature |
| keep-alive 設定造成 panic | 在呼叫 backend 前驗證：V3.1.1 至少 1 秒，V5 至少 5 秒，兩者均不得超過 65535 秒；錯誤回傳 InvalidConfig |
| 停止維護的 PEM API | 專案直接使用的 rustls-pemfile 改為 rustls-pki-types::pem::PemObject；保留 CA、憑證與私鑰解析錯誤回報 |
| CI 供應鏈與依賴防護 | 所有外部 Actions 固定完整 SHA、checkout 不保留 credentials、Cargo 驗證使用 --locked、加入每週及 Release 安全檢查 |
| 文件與開源入口 | 新版繁體中文 README、SVG 橫幅、快速開始 example、完整指南、貢獻指南、安全政策、MIT／Apache-2.0 授權檔與秘密檔案忽略規則 |

最後線上檢查另外發現測試依賴 chacha20 0.10.1 已被撤回，已更新至 0.10.2；安全 gate 也會明確拒絕沒有公告 ID 的 yanked 警告。

另外調整一處既有 Result::map_or identity，讓目前 stable Clippy 的 -D warnings 能通過，行為不變。

## 依賴稽核剩餘項目

原始 cargo audit 從 **11 筆漏洞版本命中 + 2 筆資訊警告** 降為 **5 筆漏洞版本命中 + 1 筆停止維護通知**。

剩下的項目全部來自 rumqttc 0.25.1 的舊間接依賴：

- rustls-webpki 0.102.8：5 筆公告命中。對該發布版本的 source 檢查只找到 webpki::Error 的型別轉換；實際 TLS 驗證走修補後的 0.103.16，未發現呼叫舊版的憑證／CRL 驗證函式。
- rustls-pemfile 2.2.0：停止維護資訊通知，仍由 rumqttc 帶入。

沒有宣稱原始 cargo audit 已零命中。新增 gate 精確檢查公告、種類、套件版本、checksum、rumqttc 上游版本及期限，只接受已審查的這 6 筆；未知命中、相同公告出現在其他套件版本、上游版本變動或例外過期均失敗。到期日為 **2027-01-24**，需要在期限前重新評估上游與移除例外。

原始掃描 JSON 與例外設定可在本目錄的 `2026-10-26-rustsec-after.json` 及 `../../scripts/security-exceptions.json` 檢查。

## 驗證結果

- Rust stable：fmt、Clippy（all features／all targets，-D warnings）通過。
- 完整 features：36 項 Rust 測試通過；預設 features：33 項通過。
- 額外 feature 邊界：bare tls、tls-ring、tls-aws-lc 的安全測試及 tls,websocket 編譯通過。
- Python 安全 gate：7 項回歸測試通過；稽核結果為 6 筆精確審查例外、0 筆未接受命中。
- Rust 1.86.0：workspace all-features check 通過，保留既有 MSRV。
- API 文件：RUSTDOCFLAGS=-D warnings 的 all-features 文件建置通過。
- README 的 3 段 Rust 範例均已編譯檢查；快速開始保留為 examples/quick_start.rs，其他驗證用暫存 example 已移除。
- 所有正式 examples 的 all-features build 通過；工作流程 YAML、Action SHA 格式、文件本地連結與 SVG XML 經檢查。

## 邊界

本次在 Linux／WSL 完成本地驗證；Windows／macOS 仍需將來建立遠端倉庫後實際跑 CI。沒有建立 GitHub 倉庫、發版、推送或新增提交。尚未設定公開／私密漏洞回報管道，安全政策明確標示這個狀態。

原始審計與當時的漏洞探針保留作為歷史證據；那些探針描述的是修復前行為，不是現行測試。現行安全回歸測試為 crates/mqtt-kit/tests/security.rs 與 core options.rs 的測試。
