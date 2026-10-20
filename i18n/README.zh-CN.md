> **注意：** 本项目迭代很快，本文档可能迅速过时。项目根目录下的英文 README 通常更新更及时。

# pinetree

pinetree 是一个轻量的单二进制 **Misskey 兼容社交网络服务器**，用 Rust 编写。现有的 Misskey 客户端可以通过一组精心挑选、经过契约测试的 Misskey API 子集（外加 streaming）与之通信，并通过 ActivityPub 与 Misskey、Sharkey 及其他 fediverse 软件联邦互联。

它的设计目标是**运行起来足够小**：不需要 Postgres，不需要 Redis，不需要消息队列。一个二进制文件、一个内嵌数据库文件（`redb`）、本地文件存储，仅此而已。

pinetree 不自带前端。请将任意 Misskey 兼容的 Web 客户端指向它——例如 [Keystone](https://github.com/estyseesghosts/keystone)（以 pinetree 作为后端目标）或 [KissMe](https://github.com/estyseesghosts/kissme)。

> **状态（2026-10-20）：** 单节点服务器已完成客户端兼容、联邦、streaming、引用帖、分支回复、投票、帖子编辑、草稿/定时发布、拉黑/静音、私密列表、置顶、通知分组/过滤、个人资料导入/导出，以及第一批审核与注册工具。多节点集群（FoundationDB）仍在规划中，尚未开始构建。详情见下文——包括尚未接通的部分。

---

## 目录

- [今日功能](#what-it-does-today)
- [不做的事](#what-it-does-not-do)
- [已知缺口与粗糙之处](#known-gaps-and-rough-edges)
- [项目进展](#where-things-stand)
- [运行自己的服务器](#run-your-own-server)
  - [前置条件](#prerequisites)
  - [构建](#build-it)
  - [启动](#start-it)
  - [创建第一个账户](#create-your-first-account)
  - [登录并发帖](#log-in-and-post)
- [日常使用](#use-it-day-to-day)
- [与哪些软件互联](#what-it-talks-to)
- [项目结构](#project-layout)
- [自行测试](#testing-it-yourself)
- [贡献与延伸阅读](#contributing--reading-more)
- [许可证](#license)

---

## 今日功能

本节中的所有内容都运行在单节点服务器上，并有实际执行的测试覆盖（见[自行测试](#testing-it-yourself)）。约有 95 个 JSON API 端点在 `POST /api/{endpoint}` 上应答；不在列表中的端点返回 `404 UNKNOWN_API_ENDPOINT`，已列端点上的未知字段返回 `400 UNSUPPORTED_FIELD`。确切契约见 `docs/endpoint-manifest.md`。

**账户、认证与注册：**

- 注册/登录，带按 token 的权限控制，以及 Misskey 风格的限流 `429` + `retryAfter`。
- MiAuth Web 流程（`miauth/gen-token`、`GET|POST /miauth/{session}[/check]`）与传统 `auth/session/generate|show|userkey` + `auth/accept`。
- 注册策略：开放注册、仅邀请（`invite/create|list|delete|limit`、`admin/invite/create|list`），或管理员管理（`admin/accounts/create`，经由 `admin/meta` / `admin/update-meta` 的 `disableRegistration`）。
- 角色：管理员与版主（`set-role` CLI），用户上打包 `isAdmin` / `isModerator`，审核日志（`admin/show-moderation-logs`）。
- 停用（`admin/suspend-user|unsuspend-user`）：登录时以 `USER_SUSPENDED` 拒绝，关闭已开启的 stream，以有界扫描拆除关注关系。
- 完整个人资料存储：`i/update` 接受上游全部 39 个资料参数（名称、简介、所在地、生日、头像/横幅、followedMessage、语言、屏蔽词/屏蔽实例、通知接收配置等），另加 pinetree 自有的 `rejectQuotes` 与 `defaultReaction` 开关。
- 自行注销（`i/delete-account`）与管理员删除（`admin/delete-account`），管理员用户视图（`admin/show-user(s)`、`admin/update-user-note`）。

**帖子与时间线：**

- 创建（正文、内容警告、`public` / `home` / `followers` / `specified` 可见性、回复、转帖、文件附件、`localOnly`）、查看、删除、取消转帖。
- **投票**，带投票（`notes/polls/vote`）、过期处理，并以 `Question` 联邦互联（+ `pollVoted` 事件）。
- Sharkey 形态的**帖子编辑**（`notes/edit`：回复不可变、可见性强制改回、添加/替换投票会重置票数、版本历史），外加 `edited` 通知与联邦 `Update(Note)`。
- **引用帖**（FEP-044f 规范形态 + 旧版兼容 + 请求/审批工作流；引用方强制执行 `rejectQuotes`，远端回退为剥离成普通帖）与**分支回复串**，父帖晚到时仍保持完整（在解析器关联之前，待定父帖默认失败关闭）。
- 本地/本地/混合/全局时间线，带游标分页，`users/notes`、`notes/replies|renotes|children|conversation`、`notes/search|search-by-tag`（对本地帖子的子串扫描）、`notes/featured`、`users/featured-notes`。
- 处处执行读取时过滤：拉黑/静音/被停用的作者、实例屏蔽、转帖屏蔽，在每条时间线与帖子读取时都会过滤。

**表情回应、关注与社交图谱：**

- 表情回应创建/删除，带 Misskey 规范化（裸 `❤` 回退、`likeOnly`、旧版映射）；按用户 `defaultReaction`，使裸心形点击在服务端解析；经由 `notes/reactions` 获取完整回应列表。
- 关注/取关，对锁定账户的关注请求（`following/requests/list|accept|reject|cancel`）。
- 拉黑、静音、转帖屏蔽（`blocking/*`、`mute/*`、`renote-mute/*`、`users/relation`），在关注/回应/投票/创建时及每次读取时强制执行。
- 私密关注者/关注列表（按本人/关注者/公开规则 gating，`FORBIDDEN` + 数量掩码）与私密回应（`users/reactions`）。
- 置顶（`i/pin|unpin`，上限 5 条，已联邦）与有文档说明的“兼容、简化版”精选帖排序（与上游不完全相同）。

**通知、草稿、导入/导出：**

- `i/notifications`（游标、`markAsRead`、包含/排除类型过滤、按创建时间的接收配置 gating、静音/自身丢弃）、`i/notifications-grouped`（回应/转帖分组，纯打包层）、`notifications/mark-all-as-read`。
- 草稿 + 定时帖（`notes/drafts/create|list|update|delete|count`；定时即草稿上的 `scheduledAt`，由持久化任务发布，附 `scheduledNotePosted/Failed` 通知）。
- 个人资料导入/导出：8 种导出（`following`、`blocking`、`mute`、`notes`、`antennas`、`clips`、`favorites`、`user-lists`）+ 5 种导入，带上游错误码与 `exportCompleted` 通知。（pinetree 没有数据模型的实体——用户列表、剪辑——导出空合法文件、导入零行；天线与收藏夹导出/导入真实数据行。这是有文档说明的行为，不是静默丢弃。）

**Streaming：**

- 认证 WebSocket `/streaming`（`?i=` 或 Bearer）：`main`、`homeTimeline`、`localTimeline`、`hybridTimeline`、`globalTimeline`、`antenna`（仅属主 `params.antennaId`，命中时推送 `note` 帧）。
- 帖子订阅（`subNote`/`unsubNote` + 别名，带可见性门禁的 `reacted`/`unreacted`/`deleted`）、`readNotification`、`pollVoted`、`edited` 帖子更新，未知频道回复 `UNSUPPORTED_CHANNEL`。
- 被停用用户的 socket 会在服务端关闭。

**媒体：**

- Multipart/JSON 上传（`drive/files/create`）、列出/查看/更新/删除，`GET /files/{id}` 公开 serving，本地文件系统 blob 存储。
- 远端附件会校验并缓存（scheme、SSRF、大小限制、MIME 嗅探）。没有文件夹：`folderId` 必须为 null。

**联邦（ActivityPub）：**

- WebFinger（JRD + XRD）、host-meta、NodeInfo 2.0/2.1、带 RSA 密钥的稳定 actor 文档、outbox/followers/following/featured 集合、帖子文档（已删除返回 tombstone `410`）。
- 共享 + 个人 inbox。入站验证、出站签发 Digest + 传统 HTTP Signatures（`rsa-sha256`）；64 KiB 正文上限、`Date` ±5 分钟 skew、actor/object 权限检查、activity-ID 原子去重（与状态变更在同一事务中）。
- Activity：Follow / Accept / Reject / Undo、Create(Note)、Update（编辑与投票）、Delete、带 `_misskey_reaction` 的 Like、Announce、`Question`（投票）、pinetree↔pinetree 路径上的 FEP-044f 引用授权。入站 `EmojiReact`（Akkoma/Pleroma 系）经由 Like 路径接受。
- 带指数退避的持久化出站投递，按目标限流、共享 inbox 去重；彻底失败的任务带日志原因隔离，不会无限重试。
- 实例屏蔽（`blockedHosts`）：子域名感知，入站（状态写入前丢弃）与出站投递时均强制执行。
- 联邦输入视为不可信：大小/超时/重定向限制、SSRF 防护（默认屏蔽私有 IP）、HTML/MFM 清洗。

**持久性：**

- 每次本地状态变更都与 outbox/任务记录在同一个存储事务中提交；worker 负责通知、streaming 扇出与投递。任务幂等、去重、可重试、崩溃安全（有崩溃恢复测试）。

---

## 不做的事

刻意不做的目标（proposal §1）与延期工作——请求这些会返回 `404 UNKNOWN_API_ENDPOINT`（API）或 `UNSUPPORTED_CHANNEL`（streaming），而不是给出残缺的半成品实现：

- **没有多节点模式。** `--mode=cluster` 会拒绝启动（`cluster mode is not implemented (Phase 4)`）。单进程、单个内嵌 `redb` 文件，没有 FoundationDB 适配器，没有账户目录，没有跨节点路由。那是 Phase 4，尚未开始。
- **没有内置 Web 客户端。** 请使用 Keystone、KissMe 或任意 Misskey 兼容客户端。
- **没有 Mastodon 客户端 REST API。** 与 Mastodon 系服务器的 ActivityPub 联邦仍在范围内；Mastodon API 本身是非目标（Sharkey 自带一个；除非 proposal 变更，pinetree 永远不会有）。
- **自定义表情由实例 curated。** 公开 `emojis`/`emoji` 列表、`admin/emoji/add|update|delete|list`（版主）、短码注册表（大小写不敏感唯一）、按用户 opt-in 的远端表情（`showRemoteEmojis`，元数据摄取 + 懒加载）。外来 `:shortcode:` 回应按原文存储。发往 Akkoma/Pleroma 系主机的出站回应降级为普通 `Like`（❤ 收藏），直到 `EmojiReact` 发送形态通过真实往返测试。
- **用户列表已交付（Batch O WP16）。** `users/lists/*`（12 个端点）、`notes/user-list-timeline`（仅属主拉取读取）、`userList` streaming 频道（`userAdded`/`userRemoved`/帖子）、列表通知接收配置、CSV 导出/导入，天线 `src=list` 现经由成员索引解析。限制强制执行并公示（`userListLimit 10`、`userEachUserListsLimit 50`）。远端成员排队由系统 actor 发起 Follow。`create-from-public` 仍为 404。
- **没有剪辑、频道、页面、网盘文件夹、聊天、Reversi，没有按 hashtag 搜索。** 没有精选时间线，没有 `drive` streaming 频道，没有 `signin-flow`，没有 2FA/passkey/webhook。天线、收藏夹、自定义表情、用户列表、提及时间线、通知工具、发现、版本与翻译桩已交付（Batch N + O + P）。
- **没有 OAuth。** 仅 MiAuth + API token（仅当目标客户端要求时才落地 OAuth）。
- **没有 thread muting，没有 RFC 9421 或 Linked-Data 签名，** 没有 `_misskey_content`（按决策从不发送），删除/过期边界之外的投票仅返回文档化错误。账户迁移（`i/move` + 入站 `Move`）在 Batch P 交付——已在两个 pinetree 之间真实验证。
- **自助服务、移除关注者、远端查找、网盘扩展与 url-preview 已交付（Batch O WP9/WP10/WP12/WP13）。** `i/change-password`、`i/regenerate-token`（原生轮换；应用授权保留）、`i/apps`、`i/authorized-apps`、真正的 `i/revoke-token{tokenId}`、`i/signin-history`（向前部署的行，每用户上限 100 行）、`following/invalidate`（被关注者向远端关注者发出 Reject）、`users/search-by-username-and-host`、`ap/get`（`ap/show` 的授信安全子集）、`drive/files/upload-from-url`、`drive/files/find-by-hash`、第一方 `url-preview`。
- **邮箱身份 + 发现批次已交付（Batch P）。** `i/update-email`、`request-reset-password`、`reset-password`、`verify-email`（代码完整、休眠中：未配置 SMTP——发送会大声失败，绝不静默）、`i/move` + 入站 `Move`（别名握手、关注者迁移，已覆盖重放/伪造）、`i/registry/*`（8 个端点）、`get-online-users-count`（streaming 在线定义）、`pinned-users`（`[]`）、`fetch-external-resources`、`fetch-rss`（文档化子集）、`server-info`、`stats`（真实计数）、`notes/mentions`、`notifications/test-notification`、`notifications/flush`（标记已读 + 事件）、`users/recommendation`、`notes/recommendations`（简化打分）、`notes/versions`（Sharkey 形态）、`notes/translate`（休眠中：无后端——大声失败）。
- **举报已接通**（`users/report-abuse` + 带 `Flag` 转发的 `admin/abuse-user-reports` 系列）；管理员轮询列表（无邮件/webhook 通知）。

---

## 已知缺口与粗糙之处

能用、但有文档化限制，可能会绊到你：

- **仅回环测试。** 互联只在本地实验室中针对真实服务器验证（见下文），从未在公开互联网上验证。SSRF 防护默认屏蔽私有 IP；本机服务器之间的本地联邦测试需要 `PINETREE_ALLOW_PRIVATE_IPS=1`。
- **单节点存储。** 单写入者（`redb`），无只读副本，尚无到多节点的在线迁移路径（导出/导入覆盖账户、关注、拉黑、静音与帖子——不是透明升级）。
- **搜索是针对缓存帖子的有界子串扫描**，不是索引。`notes/reactions` 仅在 `sinceId` 游标下按最新优先分页（与 Misskey 升序的文档化差异）。
- **精选帖使用简化排序**（近期非敏感公开帖，按回应 + 转帖打分）——形态兼容，与上游不完全相同。
- **Actor RSA 密钥在首次使用时懒生成**——debug 构建下很慢，因此新账户首次被远端抓取时可能让对方超时。
- **关键词静音由服务端强制执行**（经属主认可、与上游的差异，上游仅在客户端执行）：`hardMutedWords` 在每次读取时丢弃帖子；`mutedWords` 打包面向查看者的 `isSoftMuted` 标记（仅在命中且帖子本身无 CW 时为 true）。见 `docs/word-mutes.md`。
- **私密关注列表尚无按用户可见性开关**——资料中存有字段，列表按文档化规则 gating（本人/关注者/公开）。
- **没有按目标投递调优，除 reactions-by-note 索引外没有时间线索引，可观测性极简**（仅 stderr 诊断）。那是 Phase 5 的工作，且只会从实测开始。
- **基准是 debug 构建、单节点数字**（约 870 帖子创建/秒、约 430 时间线/秒、约 1200 users-show/秒）——仅供粗略参考，不作承诺。

---

## 项目进展

| 阶段 | 内容 | 状态 |
|---|---|---|
| 0 | 冻结契约（API 清单、身份规则、存储测试） | 已完成 |
| 1 | 单节点服务器（账户、帖子、关注、回应、网盘、任务） | 已完成 |
| 2 | 联邦（WebFinger、actor、签名、inbox、投递、去重） | 已完成 |
| 3 | 客户端兼容与 streaming（实时 WebSocket、更丰富的 API、真实客户端契约测试） | 已完成 |
| — | 引用帖（FEP-044f）与分支回复，在此之上 | 已完成 |
| — | Misskey 对齐批次：投票、编辑、草稿/定时、拉黑/静音、私密列表、置顶、通知分组/过滤、导入/导出 | 已完成 |
| — | 审核批次（Batch M，Phase 4 之前）：角色、注册/邀请、停用、实例屏蔽、管理员视图；API 层之下的举报管道 | 大部分完成——WP1–WP6 已落地，WP7+ 待定 |
| — | 日常使用批次（Batch N）：收藏 + 帖子状态、自定义表情（含远端 opt-in）、`followRequestAccepted`、天线、举报路由、管理员剩余项、关键词静音（服务端）、MiAuth 加固、Sharkey 定时兼容层 | 已完成（本 session）——`url-preview`、自助服务、移除关注者、用户列表仍待定 |
| 4 | 基于 FoundationDB 的多节点 | 规划中——未开始 |
| 5 | 规模与加固 | 规划中 |

单节点服务器**今天就可以跑**。多节点模式在 Phase 4 落地前会拒绝启动（这是有意的）。

---

## 运行自己的服务器

### 前置条件

1. **Rust**（较新的 stable）。如果还没有，请从 [rustup](https://rustup.rs/) 获取。
2. 仅运行服务器本身的话，有上面这一项就够了。

> 如果之后还想跑完整测试套件或互联实验室，还需要 `bun`、Docker（含 compose 插件）、Node 24 与 Node 22。但要*运行 pinetree*，只需要 Rust。

### 构建

```bash
cargo build --release
```

这会在 `target/release/pinetree` 产出单个二进制文件。

> **如果你在用 WSL：** 请把 checkout 放在原生 Linux 文件系统上（例如 `/home/you/pinetree`，而不是 `/mnt/c/...`）。9P 文件系统构建与数据库都会慢数倍。如果想把构建产物放在快盘上，也可以在 `.cargo/config.toml`（git-ignored）中设置自定义 target 目录。

### 启动

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  --listen=127.0.0.1:3000
```

- `--public-url` 是世界（及其他服务器）看到的 URL。必须以 `http://` 或 `https://` 开头并包含 host。Actor URL、帖子 URL 与 WebFinger 都会基于它构建。
- `--data-dir` 是内嵌数据库（`pinetree.redb`）与上传文件所在目录。请指向一个你可以备份的目录。
- `--listen` 是 HTTP 服务器绑定的地址。用 `127.0.0.1:3000` 仅本地监听，用 `0.0.0.0:3000` 暴露到你的网络。

如果只是在本地试用，这样也可以：

```bash
target/release/pinetree \
  --public-url=http://127.0.0.1:3000 \
  --data-dir=./data \
  --listen=127.0.0.1:3000
```

> 默认情况下，发往私有/内网 IP 的联邦请求会被拦截（SSRF 防护）。要在本机服务器之间做本地联邦测试，可用 `PINETREE_ALLOW_PRIVATE_IPS=1` opt-in。生产环境请保持拦截开启。

### 创建第一个账户

可以从命令行创建账户：

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  create-account --username=alice --password=a-good-password
```

（是的，`create-account` 子命令放在服务器 flag 之后——它创建完账户即退出，不会启动服务器。注册策略——开放、仅邀请或仅管理员——之后经由 `admin/update-meta` 与邀请端点管理。）

将自己设为管理员（或版主）：

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  set-role --username=alice --admin
```

（`--moderator` 与 `=false` 撤销同理。）

### 登录并发帖

pinetree 支持几种认证方式：

**经由 API（signin）：**

```bash
curl -s -X POST http://127.0.0.1:3000/api/signin \
  -H 'content-type: application/json' \
  -d '{"username":"alice","password":"a-good-password"}'
```

这会返回 `{"i": "your-api-token"}`。在任意 API 请求中把该 token 作为 `i` 使用，或作为 `Authorization: Bearer <token>` 使用。

**经由 Web（MiAuth）：** 用 `POST /api/miauth/gen-token` 生成 token 后，在浏览器中打开 `http://127.0.0.1:3000/miauth/<session>`。

**经由 Misskey 客户端：** 将你喜欢的 Misskey 兼容客户端指向你的服务器 URL，用上面的用户名/密码登录。

然后发一条帖子：

```bash
curl -s -X POST http://127.0.0.1:3000/api/notes/create \
  -H 'content-type: application/json' \
  -H 'Authorization: Bearer your-api-token' \
  -d '{"text":"hello from pinetree","visibility":"public"}'
```

---

## 日常使用

跑起来之后，你与 pinetree 的交互方式与任意 Misskey 实例相同：

- **发帖：** `POST /api/notes/create`，带 `text`，可选 `visibility`、`cw`（内容警告）、`replyId`、`renoteId`、`fileIds`、`localOnly`、`poll`。用 `notes/edit` 编辑，用 `notes/polls/vote` 投票。
- **时间线：** `POST /api/notes/timeline`、`notes/local-timeline`、`notes/hybrid-timeline`、`notes/global-timeline`；串视图经由 `notes/replies|renotes|children|conversation`；`notes/search`、`notes/search-by-tag`。
- **回应：** `POST /api/notes/reactions/create`，带 `noteId` + `reaction`（例如 `"👍"`）；省略 `reaction` 即使用你的 `defaultReaction`。
- **关注：** `POST /api/following/create`，带 `userId`；锁定账户走 `following/requests/*`。经由 `blocking/*`、`mute/*`、`renote-mute/*` 拉黑/静音。
- **草稿：** `notes/drafts/create|list|update|delete|count`；加 `scheduledAt` 稍后发布。
- **可携带：** `i/export-*` / `i/import-*`，用于关注、拉黑、静音、帖子等。
- **Streaming：** 连接 WebSocket 到 `ws://<host>/streaming?i=<token>`（或带 Bearer 头）。频道包括 `main`、`homeTimeline`、`localTimeline`、`hybridTimeline` 与 `globalTimeline`。
- **网盘：** `POST /api/drive/files/create`（multipart）上传；文件在 `GET /files/<id>` 下 serving。

完整 API 面见 `docs/endpoint-manifest.md`（冻结契约 + 按日期追加的 superseding 说明）与 `docs/phase-3-profile.md`（兼容性画像）。不在该列表中的一律返回 `404 UNKNOWN_API_ENDPOINT`。

---

## 与哪些软件互联

pinetree 通过 ActivityPub 联邦，已在本地（回环/docker）实验室中针对真实软件测试——目前为止从未在公开互联网上测试：

- **Misskey**（2026.10.1-alpha.0）与 **Sharkey**（2025.4.7）：在实时四服务器互联实验室（两个 pinetree + 真实 Misskey + 真实 Sharkey）中覆盖关注、发帖、回应、回复、引用与投递流程。
- **Akkoma / Pleroma 系、Mastodon、Catodon 与一个参考实现：** 更广的多家族 bench harness（`scripts/fediverse-bench/`），覆盖关注、功能互操作（帖子、投票、回应、引用）与压力运行。那些运行中已知的现有限制：发往 Akkoma 系主机的出站表情回应降级为收藏；外来自定义表情永不渲染。

它讲核心 activity 类型（Follow、Accept、Reject、Undo、Create、Update、Delete、Like、Announce、Question）、WebFinger、NodeInfo、HTTP Signatures，以及 FEP-044f 引用授权（pinetree 之间完整工作流；与 Misskey/Sharkey 旧版兼容）。

---

## 项目结构

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

代码是**模块化单体**：一个 crate，依赖方向清晰（`domain` ← `application` ← adapters）。Misskey API 与 ActivityPub 适配器调用*同一套*应用层操作——业务逻辑不重复。

---

## 自行测试

完整验证门禁是：

```bash
bash scripts/smoke-all.sh
```

它按顺序运行：

1. `cargo test --offline`——完整 Rust 测试套件。目前 **553 通过**：424 个单元测试、redb 上的 100 个存储一致性用例，以及 29 个集成测试（实时 WebSocket 上的 streaming、进程内节点间联邦、崩溃恢复、真实 `misskey-js` 客户端冒烟测试）。
2. `scripts/smoke-phase1.sh`——由真实 `misskey-js` 客户端库驱动的核心流程。
3. `scripts/smoke-phase3.sh`——经由 `misskey-js` 的兼容性画像端点与 streaming。
4. `scripts/keystone-sweep.sh`——Keystone 前端的精确 API 调用（41 项检查，全部通过）。
5. `scripts/smoke-phase2.sh`——实时四服务器联邦互联（真实 Misskey + 真实 Sharkey + 两个 pinetree 实例）。Sharkey 一侧需要 Node 22；缺失时会干净跳过。

也可以逐项运行：

```bash
cargo test --offline                          # Rust tests only
PINETREE_BIN=target/release/pinetree bash scripts/smoke-phase1.sh
```

完整指南见 `docs/testing.md`，包括如何手工运行互联 rig 与多家族 bench harness。

---

## 贡献与延伸阅读

- `sns-server-proposal.md`——产品与架构 proposal。与本 README 有分歧时以它为准。
- `AGENTS.md`——贡献者（人类或 AI）的行动规则。
- `docs/`——冻结契约与 living wiki。
- `state/logs/`——带真实输出的按日期工作日志。要快速跟进请读最新一篇。

如果想帮忙，最有用的是：试用、跑测试套件、在不好用时提 issue。Batch M 剩余项（WP7+：收藏、表情、自助服务、移除关注者、URL preview、网盘扩展）与随后的 Phase 4（多节点）是开放的前沿。

---

## 许可证

尚未声明——目前没有 `LICENSE` 文件。做任何假设前请先询问。
