# 發布指南

公開倉庫為 [liam8846/mqtt-kit](https://github.com/liam8846/mqtt-kit)。GitHub 原始碼公開、GitHub Release 與 crates.io 發布是三個獨立動作；建立倉庫不會自動發布至 crates.io。

## 1. 合併前驗證

main 使用分支保護：變更透過 PR、通過 CI、分支需包含最新 main、討論需解決，禁止 force push 與刪除。個人維護階段不強制第二位審查者核准。

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-features --all-targets -- -D warnings
cargo test --locked --workspace --all-features
cargo +1.86.0 check --locked --workspace --all-features
python3 -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/security_audit.py
cargo deny --locked check --hide-inclusion-graph
```

CI 另外驗證 Windows／macOS、feature 邊界、文件與套件打包。請以要發布的 commit 的實際 CI 結果為準。

## 2. 套件打包

使用目前 stable Cargo 的 workspace 打包功能，讓兩個尚未發布的 sibling crate 透過 Cargo 暫存 registry 一起驗證：

```sh
python3 scripts/package_workspace.py
```

尚未提交的本地變更可加 --allow-dirty；正式發布應從已審查、已提交且工作目錄乾淨的狀態進行。

驗證使用獨立暫存目錄，避免 Cargo 將同版本舊 sibling 套件快取誤用於新來源。打包會實際編譯封裝後的程式碼，成功後將已驗證的 .crate 放到 target/package。檢查程式另外確認兩個 .crate 都包含完整 MIT／Apache 授權檔、正確 metadata、可獨立閱讀的 README 與版本化的 sibling 依賴。不要加 --offline：目前本機 Cargo 1.99.1 的 workspace 暫存 registry 在離線模式曾出現 checksum 內部錯誤；線上打包已驗證可通過。

## 3. crates.io 發布（需另行授權）

先確認套件名稱可用、登入帳號及擁有權，再發布 mqtt-kit-core，等待 registry 可查到該版本後，才發布 mqtt-kit：

```sh
cargo publish --locked -p mqtt-kit-core
cargo publish --locked -p mqtt-kit --all-features
```

不要同時發布兩者，也不要將 cargo package 成功解讀成套件已上架。版本調整時，需要同步更新 workspace.package.version 與 mqtt-kit 對 mqtt-kit-core 的版本需求。

## 4. GitHub Release（需另行授權）

確認 crates.io 發布狀態與文件安裝說明一致後，再決定是否推送符合 workspace 版本的 vX.Y.Z tag。既有 release workflow 會先做版本、跨平台測試與安全檢查，再建立 GitHub Release；它不會執行 cargo publish。

## 安全維護

精確依賴例外有到期日，詳見 [安全政策](../SECURITY.md)。請在新發版前重新下載最新 RustSec 資料庫。公開漏洞請先透過 GitHub 私密回報，而非一般 issue。
