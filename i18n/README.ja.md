> 本プロジェクトは変化が速く、この文書はすぐに古くなる可能性があります。プロジェクトルートの英語版 README が通常はより最新です。

# pinetree

pinetree は Rust 製の軽量・単一バイナリな **Misskey 互換ソーシャルネットワークサーバー**です。既存の Misskey クライアントは、意図的に選定されコントラクトテスト済みの Misskey API サブセット（およびストリーミング経由）で接続でき、Misskey、Sharkey、その他の fediverse ソフトウェアと ActivityPub で連合します。

意図的に**小さく運用できる**設計です。Postgres も Redis もメッセージブローカーも不要です。1つのバイナリ、1つの組み込みデータベースファイル（`redb`）、ローカルファイルストレージ、それだけです。

pinetree にはフロントエンドは付属しません。任意の Misskey 互換 Web クライアントを向けてください — 例えば [Keystone](https://github.com/estyseesghosts/keystone)（pinetree をバックエンド対象としています）や [KissMe](https://github.com/estyseesghosts/kissme）などです。

> **状態（2026-10-20）：** シングルノードサーバーは、クライアント互換性、連合、ストリーミング、引用投稿、分岐リプライ、投票、投稿編集、下書き/予約投稿、ブロック/ミュート、非公開リスト、ピン留め、通知のグループ化/フィルタリング、プロフィールのインポート/エクスポート、およびモデレーション＋登録ツールの第一弾まで完了しています。マルチノードクラスタリング（FoundationDB）は計画中であり、未構築です。詳細は以下を参照 — まだ配線されていないものも含みます。

---

## Table of contents

- [今日できること](#what-it-does-today)
- [できないこと](#what-it-does-not-do)
- [既知の不足点と粗い部分](#known-gaps-and-rough-edges)
- [現状](#where-things-stand)
- [自分のサーバーを動かす](#run-your-own-server)
  - [前提条件](#prerequisites)
  - [ビルド](#build-it)
  - [起動](#start-it)
  - [最初のアカウント作成](#create-your-first-account)
  - [ログインと投稿](#log-in-and-post)
- [日常的な使い方](#use-it-day-to-day)
- [連携対象](#what-it-talks-to)
- [プロジェクト構成](#project-layout)
- [自分でテストする](#testing-it-yourself)
- [コントリビュートと関連文書](#contributing--reading-more)
- [ライセンス](#license)

---

## 今日できること

この節のすべてはシングルノードサーバー上で動作し、実行済みテストでカバーされています（[自分でテストする](#testing-it-yourself) を参照）。約 95 の JSON API エンドポイントが `POST /api/{endpoint}` に応答します。リストにないものは `404 UNKNOWN_API_ENDPOINT` を返し、リストにあるエンドポイントの未知フィールドは `400 UNSUPPORTED_FIELD` を返します。正確なコントラクトは `docs/endpoint-manifest.md` です。

**アカウント、認証、登録：**

- 権限ごとのトークンによるサインアップ/サインイン、Misskey 形式の `429` + `retryAfter` によるレート制限。
- MiAuth Web フロー（`miauth/gen-token`、`GET|POST /miauth/{session}[/check]`）およびレガシー `auth/session/generate|show|userkey` + `auth/accept`。
- 登録ポリシー：オープンなサインアップ、招待制（`invite/create|list|delete|limit`、`admin/invite/create|list`）、または管理者管理（`admin/accounts/create`、`admin/meta` / `admin/update-meta` 経由の `disableRegistration`）。
- ロール：admin と moderator（`set-role` CLI）、ユーザーに付与される `isAdmin` / `isModerator`、モデレーションログ（`admin/show-moderation-logs`）。
- 凍結（`admin/suspend-user|unsuspend-user`）：サインインは `USER_SUSPENDED` で拒否、開いているストリームは切断、フォローは境界付きスイープで解消。
- フルプロフィールストア：`i/update` は上流の 39 プロフィールパラメータをすべて受け付け（name、description、location、birthday、avatar/banner、followedMessage、languages、ミュートワード/インスタンス、通知受信設定など）、さらに pinetree 独自の `rejectQuotes` と `defaultReaction` トグル付き。
- 自己削除（`i/delete-account`）と管理者削除（`admin/delete-account`）、管理者ユーザー表示（`admin/show-user(s)`、`admin/update-user-note`）。

**ノートとタイムライン：**

- 作成（本文、コンテンツワーニング、`public` / `home` / `followers` / `specified` 公開範囲、リプライ、リノート、ファイル添付、`localOnly`）、表示、削除、renote 解除。
- **投票**（`notes/polls/vote` による投票、期限、連合時は `Question`）（+ `pollVoted` イベント）。
- Sharkey の `notes/edit` 形式での**投稿編集**（reply は不変、公開範囲は強制的に戻る、投票の追加/置換は票をリセット、バージョン履歴）、および `edited` 通知と連合 `Update(Note)`。
- **引用投稿**（FEP-044f 正規形＋レガシー互換＋リクエスト/承認ワークフロー。引用側の `rejectQuotes` 強制、リモートはプレーンノートへの切り詰めフォールバック）および**分岐リプライスレッド**（親が遅れて到着しても壊れない。リゾルバが連結するまで保留中の親は fail-closed）。
- カーソルページネーション付きホーム/ローカル/ハイブリッド/グローバルタイムライン、`users/notes`、`notes/replies|renotes|children|conversation`、`notes/search|search-by-tag`（ローカルノートへの部分文字列スキャン）、`notes/featured`、`users/featured-notes`。
- 読み取り時強制は everywhere：ブロック/ミュート/凍結済み作者、インスタンスブロック、リノートミュートはすべてのタイムラインとノート読み取りでフィルタされます。

**リアクション、フォロー、ソーシャルグラフ：**

- Misskey 正規化付きリアクション作成/削除（素の `❤` フォールバック、`likeOnly`、レガシーマッピング）。ユーザーごとの `defaultReaction` により、素のハートタップはサーバー側で解決。`notes/reactions` でリアクション一覧全体を取得。
- フォロー/アンフォロー、鍵アカウントへのフォローリクエスト（`following/requests/list|accept|reject|cancel`）。
- ブロック、ミュート、リノートミュート（`blocking/*`、`mute/*`、`renote-mute/*`、`users/relation`）。フォロー/リアクト/投票/作成時とすべての読み取り時に強制。
- 非公開フォロワー/フォローリスト（self / follower / public ゲーティング、`FORBIDDEN` + 件数マスキング）および非公開リアクション（`users/reactions`）。
- ピン留め（`i/pin|unpin`、上限 5、連合対応）および文書化された「互換・簡易版」注目ノートランキング（上流とは同一ではありません）。

**通知、下書き、インポート/エクスポート：**

- `i/notifications`（カーソル、`markAsRead`、include/exclude タイプフィルター、作成時受信設定ゲート、ミュート/自己ドロップ）、`i/notifications-grouped`（リアクション/リノートのグループ化、pack 層のみ）、`notifications/mark-all-as-read`。
- 下書き＋予約投稿（`notes/drafts/create|list|update|delete|count`。予約 = 下書きへの `scheduledAt`、耐久ジョブにより公開、`scheduledNotePosted/Failed` 通知付き）。
- プロフィールのインポート/エクスポート：8 エクスポート（`following`、`blocking`、`mute`、`notes`、`antennas`、`clips`、`favorites`、`user-lists`）＋上流エラーコード付き 5 インポートおよび `exportCompleted` 通知。（pinetree にモデルのないエンティティ — user lists、clips — は有効な空ファイルをエクスポートし、0 行インポートします。antennas と favorites は実際の行をエクスポート/インポートします。これは文書化されており、暗黙ではありません。）

**ストリーミング：**

- 認証付き WebSocket `/streaming`（`?i=` または Bearer）：`main`、`homeTimeline`、`localTimeline`、`hybridTimeline`、`globalTimeline`、`antenna`（所有者スコープの `params.antennaId`、一致時に `note` フレーム）。
- ノートキャプチャ（`subNote`/`unsubNote` + エイリアス、公開範囲ゲート付き `reacted`/`unreacted`/`deleted`）、`readNotification`、`pollVoted`、`edited` ノート更新、未知チャネルには `UNSUPPORTED_CHANNEL` で応答。
- 凍結ユーザーのソケットはサーバー側で切断されます。

**メディア：**

- マルチパート/JSON アップロード（`drive/files/create`）、一覧/表示/更新/削除、`GET /files/{id}` で公開配信、ローカルファイルシステムの blob ストア。
- リモート添付は検証・キャッシュされます（scheme、SSRF、サイズ上限、MIME スニッフィング）。フォルダなし：`folderId` は null でなければなりません。

**連合（ActivityPub）：**

- WebFinger（JRD + XRD）、host-meta、NodeInfo 2.0/2.1、RSA 鍵付き安定 actor 文書、outbox/followers/following/featured コレクション、ノート文書（削除済みは tombstone `410`）。
- 共有＋個人 inbox。ダイジェスト＋レガシー HTTP Signatures（`rsa-sha256`）を inbound で検証・outbound で付与。64 KiB ボディ上限、`Date` ±5 分 skew、actor/object 権威チェック、原子的 activity-ID 重複排除（状態変更と同じトランザクション内）。
- アクティビティ：Follow / Accept / Reject / Undo、Create(Note)、Update（編集と投票）、Delete、`_misskey_reaction` 付き Like、Announce、`Question`（投票）、pinetree↔pinetree パスでの FEP-044f 引用認可。Inbound `EmojiReact`（Akkoma/Pleroma 系）は Like パス経由で受け入れます。
- 指数バックオフ付き耐久 outbound 配信、宛先ごとの制限、共有 inbox 重複排除。 terminally に失敗したジョブは、無限リトライではなく理由をログして隔離します。
- インスタンスブロック（`blockedHosts`）：サブドメイン対応、inbound（状態書き込み前に破棄）と outbound 配信時に強制。
- 連合入力は信頼できません：サイズ/タイムアウト/リダイレクト制限、SSRF 保護（デフォルトでプライベート IP をブロック）、HTML/MFM サニタイズ。

**耐久性：**

- すべてのローカル状態変更は outbox/job レコードと 1 つのストレージトランザクションで一緒にコミットします。ワーカーが通知、ストリーミング fan-out、配信を行います。ジョブは冪等、重複排除、リトライ可能、クラッシュセーフです（クラッシュリカバリテスト）。

---

## できないこと

意図的な非目標（proposal §1）と延期中の作業 — これらを求めると、壊れた半実装ではなく `404 UNKNOWN_API_ENDPOINT`（API）または `UNSUPPORTED_CHANNEL`（ストリーミング）を返します：

- **マルチノードモードなし。** `--mode=cluster` は起動を拒否します（`cluster mode is not implemented (Phase 4)`）。1 プロセス、1 組み込み `redb` ファイル、FoundationDB アダプタなし、アカウントディレクトリなし、ノード間ルーティングなし。これは Phase 4 であり、未着手です。
- **バンドル Web クライアントなし。**Keystone、KissMe、または任意の Misskey 互換クライアントを使用してください。
- **Mastodon クライアント REST API なし。** Mastodon 系サーバー*との* ActivityPub 連合はスコープ内です。Mastodon API 自体は非目標です（Sharkey は搭載しています。pinetree は proposal が変わらない限り搭載しません）。
- **カスタム絵文字はインスタンスキュレーション。** 公開 `emojis`/`emoji` 一覧、`admin/emoji/add|update|delete|list`（moderator）、大文字小文字を区別しない一意性の shortcode レジストリ、ユーザーごとのリモート絵文字オプトイン（`showRemoteEmojis`、メタデータ取り込み＋遅延取得）。外部 `:shortcode:` リアクションは逐語的に保存。Akkoma/Pleroma 系ホストへの outbound リアクションは、ライブ往復テストに通る `EmojiReact` 送信形状ができるまでプレーン `Like`（❤ favorite）に劣化します。
- **ユーザーリストは出荷済み（Batch O WP16）。** `users/lists/*`（12 エンドポイント）、`notes/user-list-timeline`（所有者のみ pull 読み取り）、`userList` ストリーミングチャネル（`userAdded`/`userRemoved`/ノート）、リスト通知受信設定、CSV エクスポート/インポート、およびアンテナ `src=list` はメンバーシップインデックス経由で解決します。制限を強制＋告知（`userListLimit 10`、`userEachUserListsLimit 50`）。リモートメンバーはシステム actor Follow をキューします。`create-from-public` は 404 のままです。
- **clips、channels、pages、drive folders、chat、Reversi、ハッシュタグ検索なし。** 注目タイムラインなし、`drive` ストリーミングチャネルなし、`signin-flow` なし、2FA/passkeys/webhooks なし。アンテナ、favorites、カスタム絵文字、ユーザーリスト、メンションタイムライン、通知ユーティリティ、discovery、versions、translate スタブは出荷済み（Batch N + O + P）。
- **OAuth なし。** MiAuth + API トークンのみ（対象クライアントが必要とする場合にのみ OAuth が来ます）。
- **スレッドミュートなし、RFC 9421 や Linked-Data 署名なし、** `_misskey_content` なし（決定により送信しません）、文書化されたエラーを超える削除済み/期限切れ境界での投票なし。アカウント移行（`i/move` + inbound `Move`）は Batch P で出荷 — 2 つの pinetree 間でライブ検証済み。
- **セルフサービス、フォロワー削除、リモート lookup、drive 追加、url-preview は出荷済み（Batch O WP9/WP10/WP12/WP13）。** `i/change-password`、`i/regenerate-token`（ネイティブローテーション。app  grants は存続）、`i/apps`、`i/authorized-apps`、実 `i/revoke-token{tokenId}`、`i/signin-history`（デプロイ以降の行、上限 100/user）、`following/invalidate`（フォロイー作成 Reject をリモートフォロワーへ）、`users/search-by-username-and-host`、`ap/get`（`ap/show` の資格情報付き安全サブセット）、`drive/files/upload-from-url`、`drive/files/find-by-hash`、ファーストパーティ `url-preview`。
- **メール identity + discovery バッチは出荷済み（Batch P）。** `i/update-email`、`request-reset-password`、`reset-password`、`verify-email`（コード完成、休眠：SMTP 未設定 — 送信は黙って成功せず明示的に失敗します）、`i/move` + inbound `Move`（エイリアスハンドシェイク、フォロワー移行、リプレイ/偽造カバー）、`i/registry/*`（8 エンドポイント）、`get-online-users-count`（ストリーミングプレゼンス定義）、`pinned-users`（`[]`）、`fetch-external-resources`、`fetch-rss`（文書化サブセット）、`server-info`、`stats`（実カウント）、`notes/mentions`、`notifications/test-notification`、`notifications/flush`（既読＋イベント）、`users/recommendation`、`notes/recommendations`（簡易スコアリング）、`notes/versions`（Sharkey 形式）、`notes/translate`（休眠：バックエンドなし — 明示的に失敗）。
- **通報は配線済み**（`users/report-abuse` + `Flag` 転送付き `admin/abuse-user-reports` 系）。管理者はリストをポーリングします（メール/webhook 通知なし）。

---

## 既知の不足点と粗い部分

動作はするが、文書化された制限でつまずく可能性があるもの：

- **ループバックテストのみ。** 相互運用はローカルラボ（下記参照）の実サーバー相手に検証されており、公開インターネット相手には未検証です。SSRF 保護はデフォルトでプライベート IP をブロックします。マシン上のサーバー間ローカル連合テストには `PINETREE_ALLOW_PRIVATE_IPS=1` が必要です。
- **シングルノードストレージ。** 単一ライター（`redb`）、読み取りレプリカなし、マルチノードへのライブ移行パスなし（エクスポート/インポートはアカウント、フォロー、ブロック、ミュート、ノートをカバー — 透過的アップグレードではありません）。
- **検索はキャッシュ済みノートへの境界付き部分文字列スキャン**であり、インデックスではありません。`notes/reactions` は `sinceId` のみのカーソルで新しい順にページングします（Misskey の昇順との文書化された差分）。
- **注目ノートは簡易ランキングを使用**（リアクション＋リノートでスコア化された最近のセンシティブでない公開ノート） — 形状は互換ですが、上流とは同一ではありません。
- **Actor RSA 鍵は初回使用時に遅延生成** — デバッグビルドでは遅いため、新規アカウントの最初のリモートフェッチがピア側でタイムアウトすることがあります。
- **ワードミュートはサーバー強制**（クライアント側のみで強制する上流からの、所有者承認済み差分）：`hardMutedWords` はすべての読み取りでノートを落とします。`mutedWords` はビューア固有 `isSoftMuted` フラグを pack します（一致し、かつノート自体に CW がない場合のみ true）。`docs/word-mutes.md` を参照。
- **非公開フォローリストにはユーザーごとの公開範囲ノブはまだありません** — プロフィールはフィールドを保持し、リストは文書化ルール（self/follower/public）でゲートします。
- **宛先ごとの配信調整なし、reactions-by-note インデックス以外のタイムラインインデックスなし、最小限の可観測性**（stderr 診断のみ）。これは Phase 5 の作業であり、計測から始めます。
- **ベンチマークはデバッグビルド・シングルノードの数値**（約 870 notes-create/s、約 430 timeline/s、約 1200 users-show/s） — 目安であり、保証ではありません。

---

## 現状

| Phase | 内容 | 状態 |
|---|---|---|
| 0 | コントラクト凍結（API マニフェスト、identity ルール、ストレージテスト） | 完了 |
| 1 | シングルノードサーバー（アカウント、ノート、フォロー、リアクション、drive、ジョブ） | 完了 |
| 2 | 連合（WebFinger、actor、署名、inbox、配信、重複排除） | 完了 |
| 3 | クライアント互換性とストリーミング（ライブ WebSocket、充実した API、実クライアントコントラクトテスト） | 完了 |
| — | 引用投稿（FEP-044f）と分岐リプライをその上に | 完了 |
| — | Misskey パリティバッチ：投票、編集、下書き/予約、ブロック/ミュート、非公開リスト、ピン留め、通知グループ化/フィルタリング、インポート/エクスポート | 完了 |
| — | モデレーションバッチ（Batch M、Phase 4 前）：ロール、登録/招待、凍結、インスタンスブロック、管理者表示。下面の API 層への通報配管 | ほぼ完了 — WP1–WP6 着陸、WP7+ 保留中 |
| — | 日常用バッチ（Batch N）：favorites + ノート状態、カスタム絵文字（+ リモートオプトイン）、`followRequestAccepted`、アンテナ、通報ルート、管理者残件、ワードミュート（サーバー側）、MiAuth 堅牢化、Sharkey 予約 shim | 完了（今セッション） — `url-preview`、セルフサービス、フォロワー削除、ユーザーリストは保留中 |
| 4 | FoundationDB によるマルチノード | 計画中 — 未着手 |
| 5 | スケールと堅牢化 | 計画中 |

シングルノードサーバーは**今日動かせます**。Phase 4 が着陸するまでマルチノードモードは起動を拒否します（意図的です）。

---

## 自分のサーバーを動かす

### 前提条件

1. **Rust**（最近の stable）。なければ [rustup](https://rustup.rs/) から入手してください。
2. サーバー自体の実行にはそれだけです。

> フルテストスイートや相互運用ラボを後で動かしたい場合は、`bun`、Docker（compose プラグイン付き）、Node 24、Node 22 も必要です。しかし *pinetree を動かす*だけなら Rust のみで足ります。

### ビルド

```bash
cargo build --release
```

これにより `target/release/pinetree` に単一バイナリができます。

> **WSL の場合：** チェックアウトはネイティブ Linux ファイルシステム上に置いてください（例：`/home/you/pinetree`、`/mnt/c/...` ではありません）。9P ファイルシステムはビルドもデータベースも数倍遅くなります。ビルド成果物を高速ドライブに置きたい場合は、`.cargo/config.toml`（git-ignored）でカスタム target dir を設定することもできます。

### 起動

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  --listen=127.0.0.1:3000
```

- `--public-url` は世界（および他サーバー）から見える URL です。`http://` または `https://` で始まりホストを含む必要があります。Actor URL、ノート URL、WebFinger はすべてここから構築されます。
- `--data-dir` は組み込みデータベース（`pinetree.redb`）とアップロードファイルの置き場です。バックアップできるディレクトリを指定してください。
- `--listen` は HTTP サーバーがバインドするアドレスです。ローカルのみに留めるには `127.0.0.1:3000`、ネットワークに公開するには `0.0.0.0:3000` を使用してください。

試しにローカルで動かすだけなら、これでも動きます：

```bash
target/release/pinetree \
  --public-url=http://127.0.0.1:3000 \
  --data-dir=./data \
  --listen=127.0.0.1:3000
```

> デフォルトでは、プライベート/内部 IP への連合リクエストはブロックされます（SSRF 保護）。マシン上のサーバー間でローカルのみの連合テストを行う場合は、`PINETREE_ALLOW_PRIVATE_IPS=1` でオプトインできます。本番ではブロックを有効のままにしてください。

### 最初のアカウント作成

コマンドラインからアカウントを作成できます：

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  create-account --username=alice --password=a-good-password
```

（はい、`create-account` サブコマンドはサーバーフラグの後に置きます — サーバーを起動せずアカウント作成後に終了します。登録ポリシー — オープン、招待制、管理者のみ — は後から `admin/update-meta` と招待エンドポイントで管理します。）

自分を admin（または moderator）にするには：

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  set-role --username=alice --admin
```

（`--moderator` や、剥奪用の `=false` も同様に動作します。）

### ログインと投稿

pinetree はいくつかの認証方法をサポートしています：

**API 経由（signin）：**

```bash
curl -s -X POST http://127.0.0.1:3000/api/signin \
  -H 'content-type: application/json' \
  -d '{"username":"alice","password":"a-good-password"}'
```

これは `{"i": "your-api-token"}` を返します。そのトークンを任意の API リクエストの `i` として、または `Authorization: Bearer <token>` として使用してください。

**Web 経由（MiAuth）：** `POST /api/miauth/gen-token` でトークンを生成した後、ブラウザで `http://127.0.0.1:3000/miauth/<session>` を開いてください。

**Misskey クライアント経由：** お気に入りの Misskey 互換クライアントをサーバー URL に向けて、上記の username/password でログインしてください。

その後ノートを投稿します：

```bash
curl -s -X POST http://127.0.0.1:3000/api/notes/create \
  -H 'content-type: application/json' \
  -H 'Authorization: Bearer your-api-token' \
  -d '{"text":"hello from pinetree","visibility":"public"}'
```

---

## 日常的な使い方

起動したら、pinetree とは任意の Misskey インスタンスと同じようにやり取りします：

- **投稿：** `text`、任意の `visibility`、`cw`（コンテンツワーニング）、`replyId`、`renoteId`、`fileIds`、`localOnly`、`poll` 付き `POST /api/notes/create`。`notes/edit` で編集、`notes/polls/vote` で投票。
- **タイムライン：** `POST /api/notes/timeline`、`notes/local-timeline`、`notes/hybrid-timeline`、`notes/global-timeline`。スレッド表示は `notes/replies|renotes|children|conversation` 経由。`notes/search`、`notes/search-by-tag`。
- **リアクション：** `noteId` + `reaction`（例：`"👍"`）付き `POST /api/notes/reactions/create`。`reaction` を省略すると `defaultReaction` を使用します。
- **フォロー：** `userId` 付き `POST /api/following/create`。鍵アカウントは `following/requests/*` 経由。ブロック/ミュートは `blocking/*`、`mute/*`、`renote-mute/*` 経由。
- **下書き：** `notes/drafts/create|list|update|delete|count`。後で公開するには `scheduledAt` を追加。
- **可搬性：** フォロー、ブロック、ミュート、ノートなどの `i/export-*` / `i/import-*`。
- **ストリーミング：** `ws://<host>/streaming?i=<token>`（または Bearer ヘッダー）に WebSocket 接続。チャネルには `main`、`homeTimeline`、`localTimeline`、`hybridTimeline`、`globalTimeline` があります。
- **Drive：** アップロードは `POST /api/drive/files/create`（multipart）。ファイルは `GET /files/<id>` で配信されます。

API 全面は `docs/endpoint-manifest.md`（凍結コントラクト＋日付付き superseding notes）と `docs/phase-3-profile.md`（互換性プロファイル）に文書化されています。そのリストにないものは `404 UNKNOWN_API_ENDPOINT` を返します。

---

## 連携対象

pinetree は ActivityPub で連合し、ローカル（ループバック/docker）ラボの実ソフトウェア相手にテストされています — これまでのところ公開インターネット相手にはありません：

- **Misskey**（2026.10.1-alpha.0）と **Sharkey**（2025.4.7）：ライブ 4 サーバー相互運用ラボ（2 pinetree + 実 Misskey + 実 Sharkey）でのフォロー、投稿、リアクト、リプライ、引用、配信フロー。
- **Akkoma / Pleroma 系、Mastodon、Catodon、リファレンス実装：** より広いマルチファミリーベンチハーネス（`scripts/fediverse-bench/`）で、フォロー、機能相互運用（ノート、投票、リアクション、引用）、ストレステストをカバー。これらの実行からの既知の現行制限：Akkoma 系ホストへの outbound 絵文字リアクションは favorites に劣化します。外部カスタム絵文字は描画されません。

コアアクティビティタイプ（Follow、Accept、Reject、Undo、Create、Update、Delete、Like、Announce、Question）、WebFinger、NodeInfo、HTTP Signatures、FEP-044f 引用認可（pinetree 間では完全ワークフロー、Misskey/Sharkey とはレガシー互換）を話します。

---

## プロジェクト構成

```
.
├── sns-server-proposal.md    the product & architecture plan (source of truth)
├── AGENTS.md                 rules for anyone (human or AI) working in this repo
├── docs/                     frozen contracts, phase plans, exit criteria, wiki
│   ├── endpoint-manifest.md  the Misskey API contract (+ superseding notes)
│   ├── phase-3-profile.md    the compatibility profile (target clients, scope)
│   ├── misskey-parity-plan.md  the parity batch (§§0–13 done; Batch M status)
│   ├── implementer-guide-batch-m.md  moderation/daily-use batch (WP1–WP6 done)
│   ├── testing.md            how to run every test suite
│   └── wiki/                 living overview docs (About, Current State, Roadmap)
├── src/                      the Rust crate
│   ├── domain/               pure types & rules (no HTTP, no DB)
│   ├── application/          use cases (commands, queries, ports)
│   ├── api/misskey/          HTTP API adapter (endpoints, packers, errors)
│   ├── activitypub/          ActivityPub protocol adapter
│   ├── federation/           inbox, outbox, delivery, retry
│   ├── persistence/redb/     the embedded database adapter
│   ├── streaming/            WebSocket server & channel hub
│   ├── jobs/                 durable job queue & worker
│   ├── media/                file blob storage
│   └── cluster/              Phase 4 multi-node (stub for now)
├── tests/                    conformance (100 cases incl. polls, edits,
│                             blocks, imports), federation, streaming,
│                             crash-recovery, real-client smoke tests
├── scripts/                  smoke-all.sh and friends (one-command test gates)
├── scripts/fediverse-bench/  multi-family federation bench harness
├── state/logs/               dated work logs with real command output
└── misskey, sharkey          symlinks to reference checkouts (git-ignored;
                               only needed for tests, not for running a server)
```

コードは**モジュラーモノリス**です：1 クレート、明確な依存方向（`domain` ← `application` ← adapters）。Misskey API と ActivityPub アダプタは*同じ*アプリケーション操作を呼びます — ビジネスロジックの重複はありません。

---

## 自分でテストする

完全な検証ゲートは：

```bash
bash scripts/smoke-all.sh
```

これは（順に）実行します：

1. `cargo test --offline` — Rust テストスイート全体。現在 **553 green**：424 ユニットテスト、redb 上 100 ストレージ適合ケース、29 統合テスト（ライブ WebSocket 上のストリーミング、プロセス内ノード間連合、クラッシュリカバリ、実 `misskey-js` クライアントスモークテスト）。
2. `scripts/smoke-phase1.sh` — 実 `misskey-js` クライアントライブラリで駆動するコアフロー。
3. `scripts/smoke-phase3.sh` — `misskey-js` 経由の互換性プロファイルエンドポイントとストリーミング。
4. `scripts/keystone-sweep.sh` — Keystone フロントエンドの正確な API 呼び出し（41 チェック、すべて通過）。
5. `scripts/smoke-phase2.sh` — ライブ 4 サーバー連合相互運用（実 Misskey + 実 Sharkey + 2 pinetree インスタンス）。Sharkey 側に Node 22 が必要。ない場合はクリーンスキップします。

個別にも実行できます：

```bash
cargo test --offline                          # Rust tests only
PINETREE_BIN=target/release/pinetree bash scripts/smoke-phase1.sh
```

完全ガイドは `docs/testing.md` を参照。相互運用リグとマルチファミリーベンチハーネスの手動実行方法を含みます。

---

## コントリビュートと関連文書

- `sns-server-proposal.md` — 製品とアーキテクチャの proposal。この README との不一致ではこちらが勝ちます。
- `AGENTS.md` — コントリビューター（人間・AI）の行動規範。
- `docs/` — 凍結コントラクトと living wiki。
- `state/logs/` — 実出力を含む日付付き作業ログ。最新エントリを読めば追いつけます。

手伝いたい場合は、最も有用なのは：試すこと、テストスイートを動かすこと、動かないものがあれば issue を立てることです。Batch M 残件（WP7+：favorites、emoji、セルフサービス、フォロワー削除、URL preview、drive 追加）と、その後の Phase 4（マルチノード）が開かれたフロンティアです。

---

## ライセンス

未宣言 — `LICENSE` ファイルはまだありません。仮定する前に問い合わせてください。
