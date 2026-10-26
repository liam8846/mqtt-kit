# 架構補強計畫

以 v0.2.0 為基準，保留既有排入佇列 API；所有變更經新分支與 PR 提交。

1. 訂閱物件在第一個 await 前取得回收責任；取消、送出失敗與 broker 拒絕都能移除本地註冊並安排遠端清理。同 filter 的設定依仍存活的最新訂閱決定。
2. 關閉請求以不需 await 的訊號交由事件迴圈執行；所有呼叫者等待相同終止狀態，取消其中一個不會中止關閉。滿載與未連線也有有界退出。
3. 新增具逾時的確認 API：SUBACK、UNSUBACK、QoS1 PUBACK、QoS2 PUBCOMP；QoS0 不宣稱 broker 確認。封包 ID、否定確認、取消、重連及關閉需要精確關聯與有界追蹤。
4. 用 topic 索引縮小訊息路由的比對範圍，保留 +/#/空層級與 $SYS 隔離。加入訂閱數、每佇列及全域佇列內容位元組限制。
5. 加入不依賴外部 broker 的故障注入與封包確認測試，驗證取消、阻塞、拒絕、錯誤 ID、斷線與關閉競態；保留真實 broker 整合測試。
6. 同步文件、完成 fmt/Clippy/default/all-features/MSRV/package/security 驗證，推送分支、建立 PR 並檢查跨平台 CI。不修改 v0.2.0 tag、不自動合併。

## 實作結果

- 訂閱在 await 前取得 RAII 回收責任，註冊表與送出在同一把同步鎖下協調；等待要求若被明確取消，會喚醒並回報 SubscriptionClosed。仍存活的最新設定也會安排同步回 broker。
- 清理與設定恢復集合均受訂閱數限制；要求及未確認操作有界，取消訂閱另保留一份有界控制容量。所有同步鎖範圍都不包含 await。
- 關閉由 watch 訊號交給事件迴圈，兩秒內嘗試排入及送出 DISCONNECT；取消任一等待者不會取消背景關閉。CompletionGuard 保證背景 future 被取消時仍發布 Closed。
- ACK 以操作種類、packet ID 與確認階段對應；未確認與已確認 API 共用追蹤以避免混配。重傳不消耗新要求的 receipt，fresh session 只丟棄 transport 已移除的要求；無法區分的 ID 重用採失敗關閉。
- rumqttc 的 pending 只從自有有界 channel 一次移入一筆，保留 Bytes 共用與 inflight 控制，不將整個 channel 搬入無界 VecDeque。
- MQTT 5 否定 PUBREC/PUBCOMP 後，上游 0.25.1 未釋放 inflight 計數；先回報 BrokerRejected，再清理並重新連線，其他未確認操作回報可能已送達的 Connection 錯誤。
- 路由使用 arena trie，查詢與刪除都不遞迴；位元組預算包含訊息本體、內容與 property entry header，避免大量空字串屬性繞過預算。預算不是程序 RSS 上限。
- cargo-deny 0.20.2 加入現有 CI 與 Release 安全 gate，檢查全部 features 與支援平台；公告例外與精確 checksum／日期政策同步驗證。多版本依賴列警告，未知來源、wildcard、撤回版本及未允許授權會失敗。
- 打包採唯一暫存 registry，避免 Cargo 重用同版本舊 sibling 來源；實際編譯封裝後檢查授權、metadata、README 與版本需求，成功產物保存於 target/package。

## 本地驗證（Linux／WSL）

- 完整 features：69 項 Rust 測試通過；預設 features：66 項通過。
- bare tls 的單元與 security 邊界：37 項通過；all-targets Clippy -D warnings、fmt、Rust 1.86 check 通過。
- Python gate：9 項測試通過；RustSec 精確 gate 為 6 筆既有審查例外、0 筆未接受命中。
- cargo-deny：advisories、bans、licenses、sources 均通過；上游多版本依賴保留警告，既有公告例外到期日仍為 2027-01-24。
- API 文件建置、4 段 README Rust 範例編譯及兩個 crate 的隔離打包驗證通過。

跨平台結果以此分支 PR 的 GitHub CI 為準。本變更不自動合併、不重寫 main，也不修改已存在的 v0.2.0 tag。
