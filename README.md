# KubotanUtil2
[![Paper 1.21.11](https://img.shields.io/badge/Paper-1.21.11-brightgreen.svg)](https://fill-ui.papermc.io/projects/paper/version/1.21.11)
[![GitHub release](https://img.shields.io/github/release/gorogoro-space/KubotanUtil2.svg)](https://github.com/gorogoro-space/KubotanUtil2/releases)
[![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)](https://github.com/gorogoro-space/KubotanUtil2/issues)
[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://github.com/gorogoro-space/KubotanUtil2/blob/main/LICENSE)

Plugin for Gorogoro servers.

別のプラグインのコマンドを使って、プレイヤーを投獄・BAN し、その状態を記録するプラグインです。

2.0 で、プレイヤー同士の評価(good / bad)の機能を廃止しました。

# Requirements
- Paper 1.21.11 以降(1.21.11 未満のサーバーでは読み込まれません)
- Java 21
- 投獄・ミュート・BAN を行うプラグイン(実行するコマンドは config.yml で設定します)

# Installation method
Please place the .jar file in the Paper plugins folder.

.jar ファイルを Paper の plugins フォルダに置き、サーバーを再起動してください。

# Usage
```
/isjailed <player>
```

- `/isjailed` — 投獄中か・BAN されているかを表示します

管理用のコマンド
- `/kjail <player>` / `/kunjail <player>` — 投獄 / 釈放します(ミュート・解除を含む)
- `/kban <player> <reason>` / `/kunban <player>` — BAN / BAN 解除します

# Permission
```
space.gorogoro.kubotanutil2.default.*
space.gorogoro.kubotanutil2.kjail
space.gorogoro.kubotanutil2.kunjail
space.gorogoro.kubotanutil2.kban
space.gorogoro.kubotanutil2.kunban
```

すべてのコマンドに `space.gorogoro.kubotanutil2.default.*` が必要です。権限は plugin.yml に登録していないため、権限プラグインで与えてください。

# Config
`plugins/KubotanUtil2/config.yml`

| 項目 | 内容 |
|---|---|
| `jail_command` / `unjail_command` など | 投獄・釈放・ミュート・BAN で実行するコマンド(`%s` に名前などが入ります) |

1.x の `config.yml` にある `exex_interval_hour`・`bad_value`・`good_value`・`is_debug` は使われなくなりました(残っていても動作に影響はありません)。

プレイヤーの名前・UUID と、投獄・BAN の状態は `plugins/KubotanUtil2/status.db`(SQLite)に保存されます。

# Disclaimer
Do not assume any responsibility by use. Please use it at your own risk.

## ビルド

JDK 21 が必要です。Gradle はラッパー(`gradlew`)が自動でダウンロードするので、別途インストールする必要はありません。

```
gradlew.bat clean build
```

(macOS / Linux では `./gradlew clean build`)

## IntelliJ IDEA でのビルド手順

本プロジェクトはビルドツールに Gradle を使用しています。
IntelliJ IDEA 上で正しくプラグイン（JARファイル）を生成するには、以下の手順を実行してください。
「ビルド → アーティファクトのビルド」はクラスファイルが入らないことがあるので使わないでください。

### 🛠️ ビルド手順

1. IntelliJ IDEA の画面右端にある **「Gradle」タブ** をクリックして開きます。
2. プロジェクト名（KubotanUtil2）を展開し、 **`Tasks`** ツリーを開きます。
3. リスト内にある **`clean`** をダブルクリックして実行します（古いビルドキャッシュを削除します）。
4. 続けてリスト内にある **`build`** をダブルクリックして実行します。

### 📦 生成されたファイルの場所
ビルドが成功すると、プロジェクトのルート直下に `build/libs` フォルダが作成（または更新）され、その中に中身の詰まった正しい JAR ファイルが生成されます。

* **生成先:** `build/libs/KubotanUtil2-<version>.jar`(バージョンは `gradle.properties` の `version`)

この JAR ファイルを Minecraft サーバーの `plugins` フォルダに配置してください。

## 開発(IntelliJ IDEA で Claude Code を使う)

このリポジトリには、AI コーディングツール [Claude Code](https://docs.claude.com/ja/docs/claude-code/overview) 向けの作業ルールを書いた `CLAUDE.md` があります。IntelliJ IDEA で使う手順は次のとおりです。

1. Claude の有料プラン(Pro / Max)か、Anthropic API のアカウントを用意します。
2. Windows では、先に [Git for Windows](https://git-scm.com/downloads/win) をインストールします。
3. Claude Code 本体をインストールします。Windows では PowerShell で次を実行します。
   ```
   irm https://claude.ai/install.ps1 | iex
   ```
4. IntelliJ IDEA の「設定 → プラグイン → Marketplace」で「Claude Code」を検索してインストールし、IDE を再起動します。検索結果には Anthropic 以外が作った似た名前のプラグインも表示されます。プラグイン名の下に書かれた提供元が「Anthropic」になっているものを選んでください。
5. プロジェクトを開いて、ターミナルで `claude` を実行します(`Ctrl+Esc` でも起動できます)。初回はブラウザでログインします。

起動すると `CLAUDE.md` が自動で読み込まれ、このプロジェクトの設計方針に沿って作業します。

## ライセンス

[LICENSE](LICENSE) を参照してください。

## kubotan へのメモ

リポジトリを新規作成したら、**必ず Watch を設定すること**(忘れない!)。
GitHub の自動 Watch 機能は 2025 年 5 月に廃止されたため、設定しないと他の人が立てた issue や PR の通知が届かない。

1. リポジトリのページ右上の **「Watch」** を押す
2. **「Custom」** を選び、**Issues** と **Pull requests** にチェックを入れる
