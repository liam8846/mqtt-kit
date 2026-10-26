# 首個公開版本基準

日期：2026-10-26。倉庫：https://github.com/liam8846/mqtt-kit 。可見度：public。版本：0.2.0。

## 已完成的本地準備

- 全新 Git 紀錄，不包含已刪除的舊歷史。
- mqtt-kit 對 mqtt-kit-core 的版本需求已補上；兩個 crate 的 repository、keywords、categories、docs.rs feature metadata 已補齊。
- 兩個套件都包含完整 MIT／Apache-2.0 授權檔，README 可獨立閱讀，沒有依賴 workspace 相對文件連結。
- cargo package --locked --allow-dirty --workspace --all-features 已實際打包並編譯驗證兩個 crate；封裝內容檢查通過。
- 36 項完整功能 Rust 測試與 7 項安全 gate 測試通過；依賴稽核無未接受命中。6 筆上游精確例外仍依 SECURITY.md 揭露。
- 預備公開的程式、文件、workflow、稽核資料及授權檔已完成敏感資料樣式掃描；target、.env、.aws、.codex、.agents 與 Python cache 不會納入提交。
- CI 新增套件打包驗證與 CI passed 匯總檢查，任何必要 job 失敗或略過均不會視為通過。

## 提交與分支規則

目前程式碼、安全修復、套件設定與文件合併為唯一的初始提交，main 從此基準開始，不包含舊倉庫歷史。

分支保護規格見 .github/branch-protection.json：

- 後續變更透過 PR 合併，CI passed 必須成功，且分支必須包含最新 main。
- 必須解決討論，維持線性提交歷史。
- 禁止 force push 與刪除 main，管理員同樣受保護。
- 個人維護階段不要求另一位審查者核准。

## 發布範圍

此基準用於公開 GitHub 原始碼。crates.io 發布與 GitHub Release 分別依 docs/publishing.md 進行，不會因推送 main 自動觸發。

本機驗證結果如上；各平台 CI 與遠端保護的即時狀態以 GitHub 為準。安全維護及私密回報入口見 SECURITY.md。
