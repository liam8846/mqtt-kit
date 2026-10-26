# 安全政策

## 範圍與回報

本專案目前為 0.2.0 開發版本，尚未承諾長期支援分支。安全修正優先套用至目前開發版本。

請使用 GitHub 的 [Report a vulnerability](https://github.com/liam8846/mqtt-kit/security/advisories/new) 私密回報安全問題。請勿將可利用細節、真實帳密或私鑰公開張貼於一般 issue。回報由倉庫維護者處理，目前不承諾固定回覆時限。

回報資料可先整理：受影響版本、features、協定版本、最小重現步驟、影響及假資料測試案例。

## 使用安全

- 跨網路傳送敏感資料時使用 mqtts:// 或 wss://；TLS 設定搭配明文 scheme 會被拒絕。
- 預設驗證伺服器憑證與名稱；使用自訂 ClientConfig 時，應用負責維持驗證。
- Builder 與 TlsOptions 的 Debug 會遮蔽秘密；Message、PublishOptions 等資料型別可能含應用 payload，不應任意記錄。
- 限制應用允許的 broker 位址與 topic；本工具庫不替代 broker ACL 或應用授權。
- 持續更新最終應用的 Cargo.lock。Library 的鎖定檔不會強制下游應用採用相同解析結果。

## 依賴稽核與暫時例外

執行 `python3 scripts/security_audit.py`，或依 [貢獻指南](CONTRIBUTING.md) 安裝工具。CI 於 push、PR、每週排程及 release 驗證時執行；新公告或過期例外會使檢查失敗。

2026-10-26 修補後的實際 TLS 驗證路徑為 rustls 0.23.46、rustls-webpki 0.103.16 與 webpki-roots 1.0.10；Tokio 為 1.53.3。本專案直接的 PEM 解析已改用 rustls-pki-types。

上游 rumqttc 0.25.1 仍帶入：

| 套件 | 例外原因 | 到期日 |
| --- | --- | --- |
| rustls-webpki 0.102.8 | 5 筆公告版本命中；此版本在 rumqttc 僅作為 TLS 錯誤型別，未參與目前 TLS 憑證／CRL 驗證 | 2027-01-24 |
| rustls-pemfile 2.2.0 | 停止維護通知；仍為 rumqttc 的間接依賴，未列為已知可利用漏洞 | 2027-01-24 |

這些仍會出現在原始 cargo audit 結果中；本專案不宣稱原始掃描為零命中。`scripts/security-exceptions.json` 與檢查程式精確限制公告、種類、套件版本、checksum、上游 rumqttc 版本與到期日；相同公告若出現在其他版本，仍會失敗。

應在上游更新、依賴圖改變或例外到期前重新審查並移除可消除的例外；不應無條件延長日期。

## 審計紀錄

- [原始審計](docs/security-audit/2026-10-26.md)：保留修復前的歷史狀態。
- [修復計畫](docs/security-audit/REMEDIATION_PLAN.md)。
- [修復結果](docs/security-audit/REMEDIATION_RESULT.md)。
