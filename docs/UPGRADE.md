# 0.2 增量升级交付说明

当前 0.3.1 的管理台、会话表迁移和 Compose 网络变更见 [管理台与滚动记忆说明](ADMIN_AND_MEMORY.md)。本页保留 0.2 的升级记录。

后续 0.2.1 模型调整已合入，当前分档、单轮深度指令和日记总结行为以 [模型策略说明](MODEL_POLICY.md) 为准。

0.2.2 新增人设、群内可信身份和 Bot 互聊限额；其文件清单、新表和配置迁移见 [人设与互动保护说明](PERSONA_AND_BOT_GUARD.md)。本页原协议、Session 和 Drive 设计继续适用。

这次在现有 `hakimi-surf` 上新增协议、富媒体、物理隔离 Session 和日记模块，继续使用原来的持久化队列、去重、限流、费用预留、输出检查和 B 站只读巡逻。没有接入 DSH 私有插件接口。

## 新增和修改文件

| 文件 | 变化 |
| --- | --- |
| `src/hakimi/onebot_transport.py` | 新增 HTTP／正向 WS／反向 WS 的统一 `call` 接口，echo 关联、身份核对、心跳、重连与背压 |
| `src/hakimi/rich_message_parser.py` | 新增 text、at、image、face、json、forward、node、reply 和 CQ 兼容解析 |
| `src/hakimi/media.py` | 新增 QQ CDN 受限下载、公共 IP 校验、解码重编码和 Gemini 图片内容块 |
| `src/hakimi/session_manager.py` | 新增每会话一个 SQLite 文件、加密 20 轮窗口、TTL、清空和旧私聊迁移 |
| `src/hakimi/roaming_logger.py` | 新增日记事件、脱敏活动统计、定时生成、同步状态与失败退避 |
| `src/hakimi/drive_client.py` | 新增专用 Service Account 鉴权和单目录 Drive 写入器 |
| `app.py`、`adapters.py`、`service.py` | 修改生命周期、传输注入、富媒体路由、发送延迟、Session 接入和 `/new-session` |
| `gemini.py` | 保留 Decisions／Interactions 调用及预算逻辑，新增图文输入和图片 Token 预留；单独标识注入拦截 |
| `store.py`、`security.py`、`models.py`、`config.py` | 修改队列会话键、群策略、物理会话标识、附件字段、配置校验和可见截断提示 |
| `bilibili.py` | 新增受限 b23 短链解析，只读重定向头，不访问重定向目标 |
| `requirements.txt`、`pyproject.toml`、`uv.lock` | 新增／更新可复现依赖；最低 `google-genai>=3.2.0`，锁定已测 3.2.0 |
| `.env.example`、`scripts/init_env.py` | 新增 WS Token／地址、OWNER_QQ_ID、群策略、Session、图片、Drive 和日记配置 |
| `Dockerfile`、`deploy/nginx.conf`、`compose.roaming.yaml` | 修改 WS 帧限制及心跳，新增 TLS 代理路径、只读 Service Account 挂载方案 |
| `scripts/smoke_container.py`、`scripts/package_delivery.py` | 新增无外网、零模型费用容器测试和显式文件白名单打包脚本 |
| `tests/test_*` | 保留原测试，新增富媒体、WS、物理隔离、日记、Drive 和多模态 SDK 契约测试 |

`admin.py` 的本机预算维护、官方 QQ Webhook 验签、订阅和报告接口继续可用。运行 `python3 scripts/package_delivery.py` 可在 `dist/` 生成完整项目包和逐文件代码汇总，包含架构图与通用部署示例，不包含 `.env`、真实凭证、本地部署覆盖文件、数据目录或虚拟环境。

## SQLite 设计与迁移

原 `data/hakimi.sqlite3` 继续保存队列、预算和订阅；新会话历史使用独立数据库：

```text
data/
  hakimi.sqlite3
  service.lock
  sessions/
    group/<HMAC(namespace, platform, bot, group, group_id)>.sqlite3
    private/<HMAC(namespace, platform, bot, private, user_id)>.sqlite3
```

逻辑会话名为 `group:{group_id}`、`private:{user_id}`，物理路径使用 HMAC，避免在文件名暴露 QQ 身份。平台、机器人账号、namespace 同时参与隔离。同群成员共用本群历史；主人在群里的消息属于本群，其私聊数据库从不参与群回复。应用不执行跨文件 `ATTACH` 或历史搜索。

每个 Session 文件只有以下两张表：

| 表 | 字段及职责 |
| --- | --- |
| `session_meta` | `singleton` 固定为 1；`identity` 加密绑定完整会话标识；`enabled` 记忆开关；`migrated` 迁移完成标志 |
| `turns` | `id` 本文件内顺序；`payload` 加密的 user／assistant 文本；`expires` 过期时间；按 `id` 保留最近 15–20 轮，默认 20 |

文件权限 `0600`，目录 `0700`。每条密文绑定本会话键，复制密文到另一会话也无法通过上下文校验。所有数据库仍由同一服务进程持有加密能力；物理文件隔离不等于进程被攻陷后的独立权限域。

主库新增表：

| 表 | 结构与作用 |
| --- | --- |
| `group_policies` | `scope PRIMARY KEY, mode`；HMAC 群标识和 `at_only`／`smart_reply`／`muted` |
| `roaming_events` | `id PRIMARY KEY, day, payload`；仅保存已检查的公开巡逻摘要，正文加密 |
| `roaming_receipts` | `id PRIMARY KEY, day`；日记事件去重，防止重复计数 |
| `roaming_counts` | `(day, kind) PRIMARY KEY, n`；注入次数和获准群聊的活动类别计数 |
| `roaming_summaries` | `day PRIMARY KEY, source_digest, payload, status, created`；每日模型总结尝试和加密结果，0.2.1 新增 |
| `roaming_sync` | `day PRIMARY KEY, digest, file_id, slot, next_attempt, failures`；每日同步摘要、时间槽与重试进度 |

日记表仅在启用日记时创建。群配置写入主库，重启恢复；`.env` 中明确指定的策略覆盖已存策略。恢复默认时显式设置 `at_only`。所有身份白名单继续先于群策略生效。

旧版本本身已经使用 SQLite，而不是纯内存。本次不会删除／重建旧主库：首次访问某个私聊时，按旧精确 scope 迁移该私聊仍有效的历史和显式记忆开关，标记后不重复迁移。旧群历史原来按成员隔离，不自动并入新的共享群上下文；旧表不再供业务读取，内容按原 TTL 清理。迁移源暂留至 TTL，使两份数据库之间的恢复不依赖分布式事务。明确清空私聊时，两处该私聊内容都会删除。

停服后备份整个 data 卷和独立保存的加密密钥，再更新代码和配置。已有 `.env` 不运行初始化脚本覆盖，只补充新增配置。保持 namespace、原加密密钥、用户和群白名单不变。首次使用新版后若需回滚，应同时恢复停服备份，避免新旧会话语义混用。

## 协议与富媒体行为

反向 WS 采用标准 OneBot v11 universal 连接：`/ws/onebot` 要求 Bearer Token、`X-Self-ID` 和 `X-Client-Role: Universal`，拒绝 query token、未认证连接和 Event/API 分离连接。握手后再用 `get_login_info` 验证账号。NapCat 负责反向重连；正向模式由本服务指数退避加抖动重连。

默认每 20 秒 `get_status` 检查在线状态；正向连接及 Docker 中的 Uvicorn 同时启用 WebSocket ping/pong。API 回执按随机 echo 匹配，事件消费与回执读取分离，不会因为查询转发内容堵住 reader。最多 8 个并发 RPC、32 条内存等待事件、64 KB 帧。队列已满时断开连接；WS 不承诺离线消息重放，可能丢失尚未持久化的事件。HTTP 入站仍可使用，跨 HTTP／WS 重复消息由旧持久化去重机制消除。

出站每次串行等待 0.8–2.0 秒随机延迟，然后复核权限与回复时效。长回复只发一条，超过上限添加截断提示，默认 1800 字符。遇到发送超时、断线或进程崩溃，不自动重发可能已经成功的消息，沿用原 outbox 的 `uncertain` 状态。

卡片中的 BV 和 B 站链接进入原只读摘要流程；卡片标题／描述作为不可信文字参加 Decisions 检查。未知 URL 不会被任意访问。b23 只允许 `https://b23.tv/<短码>`，只从单次响应的 Location 提取正规 B 站视频 URL 中的 BV。

顶层 `at` 才能唤醒机器人，`at all`、转发内部的 `at`、文字里的伪造 CQ 和卡片标题都不算。只有用户直接输入的 text 可以调用固定命令，卡片／转发中的 `/new-session` 不执行。群内记忆开关及清空仅允许配置中的主人操作；固定命令不需要模型授权。

图片仅接受 HTTPS 的 `*.qpic.cn`、`multimedia.nt.qq.com`、`multimedia.nt.qq.com.cn`，最多两张、每张 4 MiB、解码前限制 1200 万像素。连接使用经公共 IP 校验的 DNS 结果，不再次解析目标；拒绝内网地址、重定向、代理环境变量和非 JPEG／PNG／WebP。经 Pillow 转为最大 1024 像素 JPEG，移除 EXIF 等元数据，只在内存持有图片内容。QQ CDN 签名 URL 不交给模型或保存到 Session，短期加密消息队列可能暂存原附件引用。

被拒绝、过期或不支持的图片不会降级为未经检查的模型输入。B 站依然只摘要公开标题／简介，不下载视频或绕过网站限制。

## Gemini 规范与费用

SDK 使用 `from google import genai`，只调用异步接口：

```python
decision_client = genai.Client(http_options={"api_version": "v1beta"})
result = await decision_client.aio.decisions.create(
    model="gemini-decision-1",
    input=[
        {"type": "text", "text": untrusted_json},
        {"type": "image", "data": image_base64, "mime_type": "image/jpeg", "resolution": "low"},
    ],
    questions=questions,
)
# result.answers[name].noul / .confidence；由代码按阈值决定是否继续。

chat_client = genai.Client(http_options={"api_version": "v1"})
result = await chat_client.aio.interactions.create(
    model="gemini-4-hydrogen",
    system_instruction=system_instruction,
    input=the_same_screened_input,
    tools=[],
    store=False,
    background=False,
    stream=False,
    automatic_continuation=False,
    generation_config={
        "max_output_tokens": 1536,
        "thinking_level": "low",
        "thinking_summaries": "none",
        "tool_choice": "none",
    },
)
```

这里展示接口结构，实际实现还显式设置 API Key、固定服务地址、超时、禁用自动重试和失败熔断。每轮重新传 system_instruction 和 tools。图片和文字一起先过 Decisions，后进入 Interactions，生成文本仍有独立输出检查。所有生成统一使用 `gemini-4-hydrogen`；普通群聊 low，Owner 私聊、视频摘要和日记总结 medium，获准深度任务 high。

保留原总／用户／巡逻每日预算事务。每张低分辨率图片每次调用额外保守预留 8192 输入 Token，成功后按 usage 结算；没有 usage 或超时则保留预留。实际费用超过预留会触发原持久化预算熔断，不假设图片是免费输入。日记明细排版使用确定性模板；每日收尾新增一次 Hydrogen 总结，其输入／输出筛查和生成均记入巡逻预算，尝试状态持久化，详见模型策略说明。

## Google Drive 的权限边界

以下是默认 `GDRIVE_AUTH_MODE=service_account` 模式。个人网盘还可显式选用新增的 `oauth` 模式，配置与授权步骤见 [README](../README.md#个人网盘-oauth不需要-workspace)。两种模式不自动互相回退，小本本保持独立控制。

1. 创建专门用于日记的 Service Account，启用其项目的 Drive API，下载 JSON，文件作为服务器只读 secret 挂载。
2. 选择 Shared Drive 中一个专用子文件夹，仅向该 Service Account 分享此文件夹的必要编辑权限；不要给它整个 Shared Drive 成员权限，不启用域级委派，不复用接触其他资料的账号。
3. 配置 `GDRIVE_ROAMING_FOLDER_ID` 及所用认证方式的凭据文件。日记在本地常驻。旧版 `HAKIMI_ROAMING_ENABLED` 作为聊天原文和原有敏感导出的授权继续有效。

此模式只请求 `https://www.googleapis.com/auth/drive.file`，固定 Google OAuth 端点，拒绝其他凭证类型和 token_uri。不会使用 ADC、用户 OAuth、账号模拟或主网盘连接器。`drive.file` 不是“目录级 OAuth scope”；实际目录权限来自专用账号的文件夹 ACL，再由代码限制访问范围。[Google scopes 文档](https://developers.google.com/workspace/drive/api/guides/api-specific-auth)

每次同步只读取配置文件夹的元数据，查询始终包含 `'<folder>' in parents` 和当天文件名。只可更新该查询返回、父目录完全相符、名称／MIME／应用标记相符的文件；更新前再次核对，服务返回 ETag 时使用 If-Match。创建时 parents 固定为该文件夹。重名、被移动、非本应用文件、shortcut 或权限错误均停止该次同步，不遍历其他文件夹，不更改分享权限。

**Service Account 没有个人 Drive 存储配额，不能拥有新文件；普通 My Drive 共享文件夹不满足每日新建的要求。** SA 模式因此要求指定文件夹有 `driveId`，否则保持待同步并退避，不自动扩大 scope 或切换主人 OAuth。[Google 官方存储配额说明](https://developers.google.com/workspace/drive/api/guides/handle-errors#storageQuotaExceeded)

日记包含公开巡逻摘要、脱敏活动描述和注入次数。为优先保证群聊隐私，“趣事”采用图片交流／视频分享／日常交流等类别和次数，不做可能残留身份线索的原话摘录或自由改写。私聊永远不进入这个入口，图片和提示词注入原文也不记录。注入计数不包括普通违规或低置信度失败。

默认按 Asia/Shanghai 每两小时同步，另在 23:55 做一次收尾；重启使用持久化时间槽避免重复上传，午夜补同步前一天收尾后新增的记录。内容相同则不上传。失败从 60 秒退避至两小时；本地约保留 8 天，超过保留期的积压不再自动补发。每天最多 200 条公开摘要，生成文件另受总长度限制。云端日记不自动删除。

## 验证

0.2.0 协议与存储升级验证：**138 项测试全部通过**；Ruff 检查和格式检查通过；WS 测试组针对关闭竞态连续复验 8 次通过；Docker 镜像构建、基础／可选日记 Compose 配置检查和最终容器冒烟测试通过。

验证命令：

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
docker build -t hakimi-surf:upgrade-test .
uv run python scripts/smoke_container.py hakimi-surf:upgrade-test
```

测试覆盖实际 SDK 3.2.0 序列化、API 版本和多模态结构；WS 双模式、echo、鉴权、重连和心跳；B 站 JSON 卡片、短链目标限制、嵌套 @ 和指令隔离；图片 DNS／像素限制；群／私聊物理文件、重启和旧数据迁移；日记生成、去重、脱敏、定时、失败恢复；Drive 父目录约束和 Service Account scope。

容器冒烟测试使用网络隔离的临时容器，验证实际 Uvicorn 反向 WS 握手、固定命令回复、跨 HTTP／WS 消息去重、伪造 Webhook 拒绝、非 root 身份和费用账本为零；脚本自动删除临时容器与测试凭证。真实 QQ／NapCat、B 站网络和 Google Drive 账号联调仍需部署方提供配置，未执行真实云端写入或付费 API 调用。

## 参考来源

- [OneBot v11 正向 WS](https://github.com/botuniverse/onebot-11/blob/master/communication/ws.md)、[反向 WS](https://github.com/botuniverse/onebot-11/blob/master/communication/ws-reverse.md)、[消息段](https://github.com/botuniverse/onebot-11/blob/master/message/segment.md)
- [dsh-qq-bridge](https://github.com/TomoyoNatsume/dsh-qq-bridge)：核对的快照为 `6e13f95415bf58c9534b4616ed074087dc5e5631`，借鉴白名单、路由和会话设计；实现遵循标准 OneBot，没有复制 DSH 私有接口。
- [Gemini Decisions](https://aistudio.google.com/docs/decisions-api)、[Interactions](https://aistudio.google.com/docs/interactions-overview)、[图片输入](https://aistudio.google.com/docs/image-understanding)
- [Drive 文件上传](https://developers.google.com/workspace/drive/api/guides/manage-uploads)、[文件查询](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/list)、[最小权限 scopes](https://developers.google.com/workspace/drive/api/guides/api-specific-auth)

## 实际建表语句

Session 文件：

```sql
CREATE TABLE IF NOT EXISTS session_meta (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1), identity BLOB NOT NULL,
  enabled INTEGER NOT NULL CHECK(enabled IN (0,1)), migrated INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS turns (
  id INTEGER PRIMARY KEY, payload BLOB NOT NULL, expires REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS turns_expiry ON turns(expires);
```

主库新增群策略：

```sql
CREATE TABLE IF NOT EXISTS group_policies (
  scope TEXT PRIMARY KEY,
  mode TEXT NOT NULL CHECK(mode IN ('at_only','smart_reply','muted'))
);
```

主库可选日记表：

```sql
CREATE TABLE IF NOT EXISTS roaming_events (
  id TEXT PRIMARY KEY, day TEXT NOT NULL, payload BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS roaming_events_day ON roaming_events(day);
CREATE TABLE IF NOT EXISTS roaming_receipts (id TEXT PRIMARY KEY, day TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS roaming_counts (
  day TEXT NOT NULL, kind TEXT NOT NULL, n INTEGER NOT NULL, PRIMARY KEY(day,kind)
);
CREATE TABLE IF NOT EXISTS roaming_sync (
  day TEXT PRIMARY KEY, digest TEXT, file_id TEXT, slot TEXT,
  next_attempt REAL NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0
);
```
