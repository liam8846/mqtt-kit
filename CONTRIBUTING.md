# 參與貢獻

感謝協助改善 mqtt-kit。一般錯誤回報請附上最小重現程式、Rust 版本、作業系統、啟用的 features、協定版本與預期／實際結果。分享日誌前請移除帳密、私鑰、token 與敏感訊息內容。

一般問題可透過 [GitHub Issues](https://github.com/liam8846/mqtt-kit/issues) 回報，實作與文件變更可提交 pull request。安全問題請先閱讀 [SECURITY.md](SECURITY.md)。

## 本地驗證

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-features --all-targets -- -D warnings
cargo test --locked --workspace --features tls-aws-lc,websocket
cargo test --locked --workspace --all-features
cargo test --locked --workspace
cargo check --locked -p mqtt-kit --features tls
cargo test --locked -p mqtt-kit --features tls-ring --lib --test security
cargo check --locked -p mqtt-kit --features tls,websocket
cargo doc --locked --workspace --all-features --no-deps
cargo +1.86.0 check --locked --workspace --all-features
```

測試會綁定 loopback 臨時埠。主要 TLS 測試使用 AWS-LC，另驗證 ring 相容性與兩種 provider 同時啟用的情境。
目前鎖定版本的一般非 FIPS 建置需要 C/C++ 編譯器，Windows x86／x86-64 建議安裝 NASM，
標準建置不必強制安裝 CMake；詳見[跨平台建置說明](docs/guide.md#跨平台)。請保留 Cargo.lock，讓本地與 CI 使用相同依賴版本。

## 套件打包

使用目前 stable Cargo，執行 `python3 scripts/package_workspace.py`，它會在隔離目錄實際打包、編譯並檢查封裝，再將 .crate 複製至 target/package。檢查會比對每個封裝內的雙授權檔、metadata 與 sibling dependency 版本。crates.io 發布流程見 [發布指南](docs/publishing.md)。

## 安全檢查

需要 Python 3.11+、cargo-audit 與 cargo-deny：

```sh
cargo install cargo-audit --version 0.22.2 --locked
cargo install cargo-deny --version 0.20.2 --locked
python3 -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/security_audit.py
cargo deny --locked check --hide-inclusion-graph
```

命令會下載最新 RustSec 資料庫，並對所有 features 與支援平台檢查來源、授權及版本；多版本列警告，其他不符合政策的項目會失敗。`cargo-deny 0.20.2` 僅支援依 ID 忽略，因此必須搭配精確 RustSec gate，兩者的例外清單會被同步檢查。新增／變更例外需要說明可達性、精確版本、checksum 與到期日；不得只按公告 ID 全面忽略。

## 變更原則

- 維持預設、TLS 與 WebSocket 的 feature 邊界；不要在 library 內安裝程序全域 crypto provider。
- 對錯誤輸入回傳明確錯誤；修正行為時加入能重現問題的回歸測試。
- 不將帳密、私鑰或敏感 payload 寫入日誌或測試 fixture。
- 更新公開 API 時同步修改文件與 examples，並保留最低 Rust 版本相容性。
- 變更說明請交代問題、修正後行為與驗證結果。

除非另行明確約定，提交的貢獻依專案相同的 MIT OR Apache-2.0 雙授權提供。
