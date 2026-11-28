<h1 align="center">WataPachiBOT</h1>
<p align="center"><img width="140" src="assets/icon/watapachi-icon-512.png" alt="WataPachiBOT"></p>
<p align="center">YouTube の音楽を再生する、Discord 用の音楽 Bot です。<br><a href="https://github.com/NEONS-DESIGN/SatouSioBOT">SatouSioBOT</a> の後継として Rust で作り直し、再生の速さと安定性を重視しています。</p>

<p align="center">
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/Rust-v1.100.1-CE422B" alt="Rust"></a>
  <a href="https://github.com/twilight-rs/twilight"><img src="https://img.shields.io/badge/Twilight-v0.17-6f42c1" alt="Twilight"></a>
  <a href="https://github.com/serenity-rs/songbird"><img src="https://img.shields.io/badge/Songbird-v0.6-3498db" alt="Songbird"></a>
  <a href="https://github.com/yt-dlp/yt-dlp"><img src="https://img.shields.io/badge/yt--dlp-latest-FF0000" alt="yt-dlp"></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-v3.14-ffde57" alt="Python"></a>
  <a href="https://deno.com/"><img src="https://img.shields.io/badge/Deno-v2.9-70ffaf" alt="Deno"></a>
</p>

> [!NOTE]
> 身内での使用をメインに考えた Bot のため、サポートなどはいたしません。

---

## 🎵 概要
[SatouSioBOT](https://github.com/NEONS-DESIGN/SatouSioBOT) (Python 製) を Rust で作り直した、Discord 用の音楽 Bot です。
Bot 本体は [Twilight](https://github.com/twilight-rs/twilight) (Discord との通信) と [Songbird](https://github.com/serenity-rs/songbird) (音声の送信) で動き、曲の解析は [yt-dlp](https://github.com/yt-dlp/yt-dlp) を載せた Python 製のワーカーが行います。ワーカーは Bot 本体が子プロセスとして起動・管理します。

* 対応するのは **YouTube のみ** です。
* 操作は **テキストコマンド** (接頭辞は既定で `?`) で行います。スラッシュコマンドはありません。
* 動作環境は **Windows 11** です。

## ✨ 主な機能

* **YouTube の URL・曲名検索に対応**: 動画・プレイリストの URL と、曲名での検索から再生できます。(ライブ配信には対応していません)
* **速い再生開始**: 音声を小分けにして取得し、曲の始めとシークの直後は小さな単位で取得して、最初の音を早く出します。キューの先の曲 (既定 2 曲) は前もって準備しておきます。
* **解析を別プロセスで並列に実行**: yt-dlp のワーカーを複数 (既定 2 台) 常駐させ、解析中も Bot の応答が止まりません。YouTube の再生用の署名の解読には、ワーカーに常駐させた Deno を使います。
* **再生方式の自動切り替え**: 再生に問題が起きたときは、別の方式で最初から再生し直します。
* **検索・プレイリストの結果を保存 (SQLite)**: プレイリストの曲順 (既定 4 時間)、曲名の検索結果 (既定 30 日)、曲の情報 (既定 183 日)、再生履歴 (既定 90 日) をデータベースに保存します。
* **再起動の後に続きから再生**: 再生中の曲と位置を定期的に保存し、Bot を起動し直すと続きから再生します。一時停止中だった場合は、一時停止のまま復元します。
* **音声の接続の監視**: 接続の不調を検知すると自動で入り直します。不調が続く場合は一時停止し、`?rejoin` で再接続できることを知らせます。
* **yt-dlp の毎日の自動更新**: 毎日決まった時刻 (既定 4:00) に、[uv](https://github.com/astral-sh/uv) で新しい環境を作って yt-dlp を入れ、検査に通ってからワーカーを切り替えます。失敗した場合は元の環境のまま動き続けます。
* **サーバーごとの設定**: 再生音量、キューの最大曲数、プレイリストから追加する曲数、自動退出までの秒数、表示言語 (日本語・English)、Bot 管理者をサーバー単位で保存します。
* **見やすい UI**: Embed による再生カード、ページ送りボタン付きのキュー一覧・再生履歴。Bot をメンションすると `?help` の案内を表示します。Bot のステータスには `?help` を表示します。
* **日付ごとのログ保存**: ログを日付ごとに `logs` フォルダへ保存します。(既定 30 日分を保持)

---

## 💬 コマンド一覧
接頭辞 (既定 `?`) に続けて、通常のメッセージとして送信します。コマンド名の大文字と小文字は区別しません。`/` で並べたコマンドは、どちらの名前でも同じ働きをします。

### 再生・基本操作
| コマンド | 説明 |
|---|---|
| `?help` | コマンドの一覧と使い方を表示します。 |
| `?p` / `?play [URL・曲名]` | 曲を再生します。再生中の場合はキューに追加します。YouTube の動画・プレイリストの URL と、曲名の検索に対応しています。 |
| `?pause` / `?ps` | 再生中の曲を一時停止します。 |
| `?resume` / `?rs` | 一時停止中の曲を再開します。 |
| `?replay` / `?rp` | 現在再生中の曲を最初から再生し直します。 |
| `?skip` / `?s` | 再生中の曲をスキップします。 |

### キュー・音量・履歴
| コマンド | 説明 |
|---|---|
| `?qlist` / `?ql` | キューに入っている曲を一覧表示します。(ページ送り付き) |
| `?history` | 再生履歴を表示します。(ページ送り付き) |
| `?pnow` / `?pn [番号]` | キューの指定した番号 (`?qlist` の番号) の曲を今すぐ再生します。再生中だった曲は次の曲に回り、最初から再生し直します。 |
| `?clear [開始] [終了]` | キューの曲を削除します。引数なしで全件、`?clear 5` で先頭から 5 件、`?clear 3 8` で 3〜8 番目を削除します。 |
| `?loop` | キューのループ再生を切り替えます。 |
| `?sh` / `?shuffle` | キューの中身をシャッフルします。 |
| `?vol` / `?volume [1-200]` | 再生音量 (%) を変更します。再生中の曲にもすぐ反映され、設定はサーバーごとに保存されます。 |

### Bot 管理・設定
| コマンド | 説明 | 必要な権限 |
|---|---|---|
| `?move` | Bot を自分のいるボイスチャンネルへ移動させます。 | - |
| `?rejoin` / `?rj` | Bot をボイスチャンネルから退出させて入り直させ、続きから再生します。 | - |
| `?leave` | Bot をボイスチャンネルから退出させ、キューを削除します。 | - |
| `?setting admin add/remove [メンバー]` | Bot 管理者の付与・剥奪を行います。 | サーバー管理者 または Bot 管理者 |
| `?setting limit queue [値]` | キューの最大曲数を設定します。(1〜10000) | サーバー管理者 または Bot 管理者 |
| `?setting limit playlist [値]` | プレイリストの URL から追加する最大曲数を設定します。(1〜5000) | サーバー管理者 または Bot 管理者 |
| `?setting autoleave [0-600]` | 聴者がいなくなってから自動で退出するまでの秒数を設定します。(0 で自動退出しない) | サーバー管理者 または Bot 管理者 |
| `?setting lang [ja/en]` | Bot の表示言語を切り替えます。(ja: 日本語、en: English) | サーバー管理者 または Bot 管理者 |

> [!TIP]
> * 「サーバー管理者」は、サーバーの所有者か「管理者」権限を持つメンバーです。タイムアウト中のメンバーは、サーバー管理者でなければ `?setting` を使えません。
> * `?setting limit` と `?setting autoleave` の上限は `config.toml` の `[limits]` で変更できます。
> * `?setting admin` のメンバーは、メンションかユーザー ID で指定します。

---

## 🛠 準備

### 1. 外部ソフトウェアのインストール
以下をインストールし、uv と Deno は環境変数 `PATH` を通してください。

| ソフトウェア | 用途 |
|---|---|
| [**Rust**](https://www.rust-lang.org/tools/install) (rustup) | Bot 本体のビルド。版は `rust-toolchain.toml` で 1.100.1 に固定しています。 |
| [**Visual Studio Build Tools**](https://visualstudio.microsoft.com/visual-cpp-build-tools/) | C/C++ のビルドツール (MSVC) と CMake。依存ライブラリのビルドに使います。 |
| [**uv**](https://github.com/astral-sh/uv) | ワーカーの環境 (Python の venv) の作成と、yt-dlp のインストール・更新 |
| [**Python 3.14**](https://www.python.org/downloads/) | ワーカーの実行。(`uv python install 3.14` で入れることもできます) |
| [**Deno**](https://deno.com/) | YouTube の再生用の署名の解読 (yt-dlp が使用) |
| [**Firefox**](https://www.mozilla.org/firefox/) | Cookie の参照元。**YouTube にログインした状態**にしておいてください。 |

> [!IMPORTANT]
> * ビルドは MSVC で行ってください。C のコードが gcc でビルドされるとリンクに失敗します。(`___chkstk_ms` が未解決になる) 環境変数 `CC` を設定している場合は、ビルドする端末で解除してください。
> * `cmake` が `PATH` に無い場合は、Visual Studio Build Tools に同梱されている `Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin` を `PATH` に追加してください。

### 2. Discord Bot の作成
1. [Discord Developer Portal](https://discord.com/developers/applications) でアプリケーションを作成し、「Bot」ページでトークンを発行します。
2. 同じページの **Privileged Gateway Intents** で **MESSAGE CONTENT INTENT** を有効にします。(テキストコマンドの読み取りに必要です)
3. 「OAuth2」ページでスコープに `bot` を選び、以下の権限を付けた招待 URL でサーバーに招待します。

| 権限 | 用途 |
|---|---|
| チャンネルを見る | コマンドの受け取り |
| メッセージを送信 | 応答の送信 |
| 埋め込みリンク | Embed (再生カード・キュー一覧など) の表示 |
| 接続 | ボイスチャンネルへの接続 |
| 発言 | 音声の再生 |

### 3. 取得とビルド
```powershell
git clone https://github.com/NEONS-DESIGN/WataPachiBOT.git
cd WataPachiBOT
cargo build --release
New-Item -ItemType Directory -Force bin
Copy-Item target\release\watapachi-bot.exe bin\
```
起動スクリプト (`tools\start.ps1`) などは `bin\watapachi-bot.exe` を実行します。ビルドの出力で上書きされないよう、`target` の外に置いています。

### 4. 設定ファイルの作成
`.env.example` を `.env` に、`config.example.toml` を `config.toml` にコピーし、`.env` にトークンを書きます。

```powershell
Copy-Item .env.example .env
Copy-Item config.example.toml config.toml
```

### 5. ワーカーの環境の作成
Bot を止めている状態で、以下を実行します。uv で `worker\envs` に Python の環境を作り、yt-dlp を入れて検査します。

```powershell
bin\watapachi-bot.exe update-worker
```

> [!NOTE]
> 2 回目以降は、Bot が毎日決まった時刻 (`[update]` の `time`) に自動で更新します。

---

## ⚙️ 設定ファイル

### `.env`
| キー | 内容 |
|---|---|
| `DISCORD_TOKEN` | Discord Bot のトークン |

### `config.toml`
`config.example.toml` に全項目と既定値、説明があります。主な節は以下のとおりです。

| 節 | 主な内容 |
|---|---|
| `[bot]` | コマンドの接頭辞 (`prefix`)、ログの水準と保持日数、ステータスに出す名前 |
| `[paths]` | データベース・ログ・yt-dlp のキャッシュ・ワーカーの環境の場所、uv の実行ファイル |
| `[defaults]` | サーバーごとの設定の既定値 (音量 25%、キュー 5000 曲、プレイリスト 200 曲、自動退出 10 秒、言語 `ja`) |
| `[limits]` | `?setting` で設定できる上限 (キュー 10000 曲、プレイリスト 5000 曲、自動退出 600 秒) など |
| `[worker]` | ワーカーの台数、解析の上限時間、Cookie を読むブラウザ (`cookies_browser`、空欄で Cookie を使わない) など |
| `[update]` | yt-dlp の自動更新の時刻 (`time`、空欄で更新しない)、入れるパッケージ、Python の版、検査に使う動画 |
| `[playback]` | 音声の取得の大きさ・上限時間、先読みする曲数、「準備中」の表示を切り替えるまでの秒数など |
| `[health]` | 音声の接続の監視と、自動で入り直す回数の上限 |
| `[voice]` | ボイスチャンネルへの接続の上限時間 |
| `[cache]` | プレイリスト・検索結果・曲の情報・再生履歴の保存期間 |
| `[db]` | データベースの書き込みと読み取りの設定 |
| `[shutdown]` | 終了処理の上限時間 |
| `[restore]` | 再起動の後に複数のサーバーの再生を復元するときの間隔 |
| `[ui]` | サムネイルや動画のリンクの URL、ページ送りのボタンを受け付ける期間 |
| `[gateway]` | Discord への接続の直後の待ち時間など |

> [!NOTE]
> * 知らないキーがあると起動時にエラーになります。(綴りの誤りを見逃さないため)
> * 時間の設定 (`*_secs`) は 604800 (7 日) 以下にしてください。`shutdown.timeout_secs` は 600 以下です。
> * ログの水準は、環境変数 `RUST_LOG` があればそちらが優先されます。

---

## 🚀 実行方法
相対パスはカレントディレクトリが基準になるため、**リポジトリの直下で** 実行してください。

### コマンドから起動する
```powershell
bin\watapachi-bot.exe run
```
`Ctrl + C` を押すか、ウィンドウを閉じると、再生位置の保存などの終了処理を行ってから終了します。同じ PC で二重には起動しません。

### 実行ファイルのサブコマンド
| サブコマンド | 内容 |
|---|---|
| `run` | コンソールで実行します。(省略時も同じ) |
| `update-worker` | Bot を止めている間に、yt-dlp のワーカーの環境を作ります。 |
| `stop` | 動いている Bot に終了処理をさせ、止まるまで待ちます。(別のセッションからも送れます) |
| `service install` | Windows サービスとして登録します。(管理者権限が必要) |
| `service uninstall` | Windows サービスを解除します。(管理者権限が必要) |

### 起動スクリプト (異常終了時に自動で起動し直す)
```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File tools\start.ps1
```
* Bot が異常終了した場合は、待ってから起動し直します。待ち時間は続けて失敗するたびに 10 秒 → 60 秒 → 300 秒と延びます。
* `Ctrl + C` などで正常に終了した場合は、起動し直しません。
* 同じ Bot が既に起動している場合は、何もせずに終わります。

### 🔁 サインイン時に自動で起動する (任意)
管理者の端末で以下を実行すると、今のユーザーがサインインしたときに `tools\start.ps1` を実行するタスクをタスクスケジューラに登録します。

```powershell
# 登録 (既にあれば置き換える)
pwsh -NoProfile -ExecutionPolicy Bypass -File tools\autostart.ps1
# 解除 (起動中の Bot は止めない)
pwsh -NoProfile -ExecutionPolicy Bypass -File tools\autostart.ps1 -Unregister
```
* ウィンドウはサインインしたユーザーのデスクトップに表示されます。Firefox の Cookie を読めるよう、ユーザーのセッションで起動します。
* PowerShell 7 (`pwsh`) が無い場合は、Windows PowerShell 5.1 で登録します。PowerShell を入れ替えた後は、もう一度実行して登録し直してください。

### Windows サービスとして動かす (任意)
管理者の端末で、リポジトリの直下から以下を実行します。カレントディレクトリが作業ディレクトリとして登録されます。

```powershell
bin\watapachi-bot.exe service install
sc start WataPachiBOT
```
* 登録時に、今のユーザーの Windows のパスワードを尋ねます。(保存はしません)
* 登録の前に `config.toml` を検証し、誤りがあれば登録しません。
* 次回の Windows の起動からは自動で起動します。

### OS の再起動の前に終了処理をさせる (任意)
`tools\stop-before-reboot.ps1` は、タスクスケジューラで定期的に OS を再起動する場合に使うスクリプトです。`shutdown /r /t 60 /f` の後に実行すると、待ってから `bin\watapachi-bot.exe stop` を送り、Bot に終了処理をさせます。再起動が取り消された場合は、タスク `WataPachiBOT` で Bot を起動し直します。結果は `logs\stop-before-reboot.log` に記録します。

### 🌐 Bot の通信を優先する (任意)
`tools\net_priority.bat` をダブルクリックすると、メニューから切り替えられます。(管理者権限の確認が表示されます)

| 引数 | 内容 |
|---|---|
| `on` | Bot 本体とワーカーの通信に優先の印 (DSCP) を付け、Windows Update などの配信の最適化が使う帯域を制限する |
| `off` | 有効にする前の状態に戻す |
| `status` | 現在の設定を表示する |
| `check` | Bot の通信に実際に印が付いているかを、パケットで確かめる |

* 優先の印はルーターが対応している場合にだけ効きます。配信の最適化の帯域制限は PC の中で完結するため、ルーターに関係なく効きます。
* 実行にはワーカーの環境の Python を使います。(無ければ `py` ランチャー) 先に `update-worker` で環境を作っておくと確実です。
* SatouSioBOT の同名のツールの設定が残っている場合は、`on` と `off` の最初に引き継ぎます。

---

## 🩺 トラブルシューティング
| 症状 | 対処 |
|---|---|
| 起動時に Discord との接続がコード `4014` で切れる | Developer Portal で MESSAGE CONTENT INTENT を有効にしてください。 |
| ビルド時に `___chkstk_ms` が未解決でリンクに失敗する | C のコードが gcc でビルドされています。環境変数 `CC` を解除し、MSVC でビルドしてください。 |
| ビルド時に `cmake` が見つからない | Visual Studio Build Tools 同梱の `Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin` を `PATH` に追加してください。 |
| `start.ps1` で「実行ファイルがありません」と出る | `cargo build --release` の後、`target\release\watapachi-bot.exe` を `bin\` にコピーしてください。 |
| `update-worker` が失敗する | Bot が起動中だと実行できません。Bot を止めてから実行してください。uv に `PATH` が通っているか (`uv --version`) も確認してください。 |
| 再生に失敗する・読み込みエラーが続く | YouTube 側の仕様変更の可能性があります。Bot を止めて `bin\watapachi-bot.exe update-worker` を実行し、yt-dlp を更新してください。 |
| YouTube の取得が遅い・失敗する | Firefox で YouTube にログインしているか、Deno に `PATH` が通っているか (`deno --version`) を確認してください。 |
| 起動時に設定ファイルのエラーが出る | `config.toml` の綴りと値の範囲を、`config.example.toml` と見比べて確認してください。 |
| エラーの詳細を確認したい | `logs` フォルダのログファイル (`watapachi-bot.YYYY-MM-DD.log`) を確認してください。`config.toml` の `log_level` を `"debug"` にすると、詳細も出力します。 |

---

## ⚠️ 注意事項・セキュリティに関する重要なお知らせ
本ボットを利用する際は、以下の点に十分ご注意ください。
* **YouTube の制限**: YouTube 側の仕様変更により、再生に失敗する場合があります。yt-dlp は毎日自動で更新しますが、すぐに直したい場合は手動で `update-worker` を実行してください。
* **認証情報の取り扱いについて**: Bot トークン (`.env`) や Cookie は、あなたの本人確認を行うための非常に重要な情報です。これらが第三者に渡ると、アカウントの不正利用や個人情報の流出につながる恐れがあります。スクリーンショットやログ、ソースコードの共有時にこれらが含まれないよう、厳重に管理してください。
* **Bot を動かす PC のアカウント**: Bot は Firefox に保存された YouTube のログイン情報を使用して動画を取得します。Bot 経由での利用も、ログイン中の Google アカウントでの視聴として扱われます。
* **免責事項**: 本ソフトウェアの使用によって生じた、いかなる損害 (データの損失、アカウントの停止、金銭的被害など) についても、開発者は一切の責任を負いません。全て利用者の自己責任において使用するものとします。
* **非公式ツールであることの理解**: 本ツールは公式のサービス提供者が提供するものではありません。仕様変更により突然利用できなくなったり、予期せぬ挙動が発生したりする可能性があることをあらかじめご了承ください。
* **サポートについて**: 身内での使用をメインに考えた Bot のため、サポートなどはいたしません。

---

## 📄 ライセンス
このプロジェクトは [**MIT ライセンス**](LICENSE) に基づいて公開されています。  
Copyright © 2026 NEONS-DESIGN

`vendor/songbird` は [Songbird](https://github.com/serenity-rs/songbird) に変更を加えたもので、[ISC ライセンス](vendor/songbird/LICENSE.md) に従います。
