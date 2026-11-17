# KubotanUtil2

別のプラグインのコマンドでプレイヤーを投獄・BAN し、その状態を記録する Paper 用プラグイン(Paper 1.21.11 以降・Java 21 必須)。プレイヤー同士の評価(good / bad)は 2.0 で廃止した。
リポジトリ: https://github.com/gorogoro-space/KubotanUtil2 (LGPL-3.0)

## 作業の進め方(必ず守ること)

- **設計が確定するまで実装しない。** 機能追加や仕様変更は、まず設計案(何を・なぜ・どう変えるか、影響範囲)を提示し、承認を得てからコードを書く。
- 判断が必要な点は、選択肢を示して質問する。勝手に決めない。
- やり取りは日本語で行う。新しく書くコードのコメントは日本語でよい。既存の英語のコメントは、頼まれない限り書き換えない。
- 変更は必要最小限にする。頼まれていないリファクタリングや機能追加はしない。
- 作業後は、変更・追加・削除したファイルの一覧と変更内容を報告する。
- 仕様を変えたら README.md と CLAUDE.md も合わせて更新する。
- `git push` の前には必ず確認を取る。コミットは意味のある単位で分ける。
- リポジトリへの初回の `git push` の前には、「GitHub で Watch 設定(Custom → Issues と Pull requests)はしましたか?」と日本語で確認する。自動 Watch は廃止されており、設定しないと issue や PR の通知が届かない。
- コミットメッセージや PR に `Co-Authored-By: Claude` などの署名を付けない。`.claude/` は Git に入れない(`.git/info/exclude` で除外済み)。
- 実装中に設計の抜けや穴に気づいたら、黙って対処せず報告して相談する。
- プルリクエストをチェックするときは、次の観点で確認して報告する。
  - 変更概要(何を・なぜ変えているか)
  - 脆弱性(権限チェックの漏れ、入力値の検証、パスの扱い、コマンドに埋め込む文字列の検証など)
  - 安全面(データの破損・消失、再読み込み時の後始末、他プラグインの妨げなど)
  - 性能(TPS など)の低下(高頻度イベントでの重い処理、メインスレッドでの同期 I/O、定期タスクの追加など。「最重要の設計方針」に沿っているか)

## 環境

- Paper 1.21.11(`io.papermc.paper:paper-api:1.21.11-R0.1-SNAPSHOT`、`compileOnly`)/ `plugin.yml` の `api-version: '1.21.11'`(1.21.11 未満のサーバーでは読み込まれない)。勝手に変えない
- **Java 21 必須。** `build.gradle.kts` の toolchain で JDK 21 を使い、Java 21 の class ファイルを出力する。勝手に変えない
- ビルド: Gradle(Kotlin DSL、wrapper 9.7.1)。`gradlew.bat clean build`(Windows)。**JDK 21 で実行する**(JDK の場所は `CLAUDE.local.md`)。「ビルドして」と言われたら常にクリーンビルドする。成果物は `build/libs/KubotanUtil2-<version>.jar`
  - IntelliJ の「アーティファクトのビルド」はクラスファイルが入らないことがある。必ず Gradle でビルドする
- バージョンは `gradle.properties` の `version` だけで管理する。`plugin.yml` の `version: '${version}'` は `processResources` でビルド時に置き換わる
- パッケージ: `space.gorogoro.kubotanutil2`(**すべて小文字**。大文字が混ざると plugin.yml の main と一致せず起動しない)
- DB は SQLite(`plugins/KubotanUtil2/status.db`)。JDBC ドライバ(`org.sqlite.JDBC`)は Paper に同梱のものを使う
- 動作確認はサーバーを再起動して行う。PlugManX での読み込みは権限やコマンドの登録が不完全になることがある。jar を差し替えるときは先に古い jar を消す(同じ名前のプラグインが 2 つあると、どちらが読まれるかわからない)
- サーバーと開発 PC の環境情報は `CLAUDE.local.md`(Git に入れない)を参照する

## 最重要の設計方針

### TPS に影響させない
- 高頻度イベント(PlayerInteractEvent、PlayerMoveEvent など)は、安い判定を先に行い、対象外なら即座に抜ける
- BlockPhysicsEvent、VehicleMoveEvent など発生頻度が極端に高いイベントは使わない
- メインスレッドでファイルや DB の同期 I/O をしない(起動時・リロード時に一度だけ行う小さなファイルの読み込みは除く)
  - ログイン時とコマンドの DB の処理は、DB 専用のスレッド(`KubotanUtil2-DB`、1 本)で順番に行う(1.3 で変更。以前はメインスレッドで同期に読み書きしていた)。接続(`con`)は 1 つなので、DB 専用のスレッド以外から使わない(起動時の作成だけはメインスレッド)
  - DB 専用のスレッドからは、メッセージを `BukkitUtil.sendMessage`、コマンドを `BukkitUtil.dispatchCommand` で送る(メインスレッドに戻して実行する)。オンラインのプレイヤーの検索(`getPlayerExact`)はメインスレッドで先に行う
  - `Bukkit.getOfflinePlayers()` は使わない(全プレイヤーのデータをディスクから読み、サーバーが数秒止まる。1.3 で廃止)
- 定期タスクは最小限にし、追加するときは頻度と理由を設計案に書く

### データの保存
- プラグインフォルダ以外には何も書き込まない

### 他プラグインとの関係
- 既存の機能を妨げないこと。イベントをキャンセルする範囲は必要最小限にする

## 機能仕様

(現在の実装の動作。仕様を変えたらここを更新する)

- **起動時(`onEnable`)**: `config.yml` が無ければ jar 内のものを保存する。`status.db` に接続し(自動コミット無効)、`users`(UUID・名前・投獄・BAN の状態)のテーブルを無ければ作る。そのあと DB 専用のスレッドを作る
- **停止時(`onDisable`)**: DB 専用のスレッドに残っている処理を最大 5 秒待ち(終われば待たない)、接続を閉じる。5 秒で終わらなければ警告を出して打ち切る。停止中に実行しようとしたコマンドは実行せず、警告ログ(`Skipped the command because the plugin is disabled: ...`)を出す
- **プレイヤーの検索(`BukkitUtil.getPlayer`)**: オンラインのプレイヤー(`getPlayerExact`)→ `users` テーブル(名前の大文字・小文字を区別しない。同じ名前が複数あれば新しく登録された方)の順で探し、`getOfflinePlayer(UUID)` を返す。どちらにも無ければ「見つかりません」。一度もログインしていないプレイヤーは見つからない。名前の検索には、大文字・小文字を区別しない索引(`playername_nocase_index`、1.3 で追加)を使う
- 自分自身が対象かどうかは UUID で比べる。投獄・釈放のコマンドに入れる名前は `getName()` で、null のときはコマンドに打たれた名前を使う
- **ログイン時**: `users` に無ければ登録し、名前が変わっていれば更新する。メッセージは出さない(2.0 で「ようこそ」と評価値の表示を廃止)
- **コマンド**(プレイヤーからだけ。コンソールからは何もしない)
  - `space.gorogoro.kubotanutil2.default.*` の権限がないと、どのコマンドも何もしない
  - `/isjailed <名前>` — 投獄中か・BAN されているかを表示する(`users` の記録を名前で探す。大文字・小文字を区別しない)
  - `/kjail <名前>`(`space.gorogoro.kubotanutil2.kjail`) — config の `jail_command` と `mute_command` をコンソールから実行し、投獄中として記録する
  - `/kunjail <名前>`(`space.gorogoro.kubotanutil2.kunjail`) — `unjail_command`・`unmute_command`・`tp_command`(対象を実行した人の所へ)をコンソールから実行し、投獄中の記録を外す
  - `/kban <名前> <理由>`(`space.gorogoro.kubotanutil2.kban`) / `/kunban <名前>`(`space.gorogoro.kubotanutil2.kunban`) — `ban_command` / `unban_command` をコンソールから実行し、BAN の状態を記録する(`users` を名前で探す。大文字・小文字を区別しない)
  - `/bad`・`/good`・`/status`・`/update`・`/upvalue` は 2.0 で廃止した(評価制度の廃止)
- 投獄・BAN などの実際の処理は、config のコマンド(`%s` に名前などが入る)を別のプラグインに実行させる。このプラグイン自身は投獄・BAN をしない
- 権限は `plugin.yml` に登録していない
- 例外は `BukkitUtil.logStackMessage` でスタックトレースを警告ログに出す

## 過去にハマった点(他プロジェクトでの経験)

- `config.getString(path, "")` のように既定値を渡すと、jar 内 config.yml の既定値が参照されない。既定値なしで取得して null を判定すること
- plugin.yml で `default: true` にした権限でも、登録されないと Bukkit は「OP のみ」として扱う。全員向けの機能を権限で縛らない

## ファイル構成(src/main/java/space/gorogoro/kubotanutil2/)

- `KubotanUtil2.java` — メインクラス。起動・停止、DB の作成と DB 専用のスレッド、ログイン時の処理、コマンドの振り分け(引数の数の確認)
- `MainProcess.java` — 各コマンドの処理(投獄・BAN・表示)
- `BukkitUtil.java` — ログ出力、メインスレッドでのメッセージ送信・コマンド実行、プレイヤーの検索
- `src/main/resources/plugin.yml` — プラグイン定義
- `src/main/resources/config.yml` — 投獄や BAN で実行するコマンドの設定
- `build.gradle.kts` / `settings.gradle.kts` / `gradle.properties` — Gradle の設定(バージョンは `gradle.properties`)
