# 0.3.1 内嵌管理台、滚动记忆与只读快照

在原 FastAPI 服务中增加 `/admin`，没有 Node.js、npm、CDN、外部字体或前端构建步骤。完整页面位于 `src/hakimi/admin_page.html`，内联 CSS 与原生 Fetch；管理接口均位于 `/admin/api/`。

## 文件清单

| 文件 | 作用 |
| --- | --- |
| `admin_auth.py` | 非空 ADMIN_TOKEN、恒时 Bearer 校验、登录失败限速、同源与本机 Host 检查 |
| `admin_routes.py` | 概览、沙盒、群策略、Bot 过滤、会话、订阅、日记、快照管理 |
| `admin_page.html` | 淡紫单页，桌面／移动布局，所有动态内容按文本显示 |
| `runtime_state.py` | 加密运行配置、会话索引、固定事件码日志和 UTC 日统计 |
| `session_manager.py` | 旧会话平滑迁移、滚动摘要与热窗口、版本检查和彻底重置 |
| `compression_policy.py` | 群聊／私聊阈值、短消息暂缓、容量上限与环境／后台统一参数校验 |
| `memory_compressor.py` | 回复发送后的异步压缩，输入筛查、凭证检查、并发与失败计费限制 |
| `notebook_snapshot.py` | 默认关闭；指定 Drive 单文件只读／本地文件快照，内存缓存 |
| `drive_client.py` | 抽出共用 Service Account 鉴权；原单目录写入器范围不变 |
| `gemini.py`、`generation_policy.py` | 可观测 Decisions 结果、记忆数据边界、压缩 low 档 |
| `app.py`、`service.py`、`store.py` | 生命周期、管理鉴权、后台任务、预算分区、清空与待发回复联动 |
| `bilibili.py`、`roaming_logger.py` | 直播间公开状态、412 等固定告警、手动当日日记同步及脱敏错误 |
| `docker-compose.yml`、`compose.yaml`、`Dockerfile` | 容器服务名 hakimi-bot，宿主端口只绑定 127.0.0.1 |
| `.env.example`、`compose.roaming.yaml`、`deploy/nginx.conf` | 配置说明、Service Account 只读挂载、公共代理明确拒绝后台路径 |

## 管理鉴权与浏览器边界

`ADMIN_TOKEN` 不存在、为空或全为空白时，`/admin` 及其全部管理接口均返回 **403**。配置口令后，登录页本身可以打开；所有管理 API 都必须提供 `Authorization: Bearer <ADMIN_TOKEN>`。本实现采用请求头认证，不下发登录 Cookie。页面中的 token 仅存活于 JS 闭包，退出或刷新后失效，不写 localStorage、sessionStorage、URL、HTML、服务器日志或数据库。建议使用至少 32 字符的随机口令。

API 先认证，再读取／验证请求体。后台只接受 `127.0.0.1`、`localhost` 或 `::1` Host；拒绝跨源 Origin、跨站／同站跨源 Fetch 和查询参数，防止 URL 携带口令与 DNS rebinding。不会信任代理提供的管理身份。登录失败有短期限速，模型／手动同步等耗费操作每分钟最多 5 次，并有并发锁。

所有管理响应 `Cache-Control: no-store`；页面启用精确内联脚本和样式 SHA-256 CSP、禁止嵌入 iframe、禁止外部资源。动态内容只用 textContent、textarea.value 等方式显示，日记 Markdown 不转成可执行 HTML。管理口令等已知凭证也加入原输入／输出保密检查。

## 页面功能与语义

- **概览**：运行时长、OneBot WS 在线／重连状态（HTTP 模式单独标记）、UTC 今日 API 尝试次数、费用与预留、全局和各业务预算、分钟入队限流、生产 Decisions 的注入与噪音忽略计数。沙盒计数另列来源，不混入生产拦截次数；入口直接忽略的已知 Bot 不产生 Decisions 计数。
- **立即巡逻**：立即拉取订阅更新并入原有队列；首次订阅建立基线，保留原去重、B 站最小间隔和风控熔断。后台工作线程正常运行后生成摘要与 Owner 报告。
- **立即汇总上传**：只处理今天，可以绕过时间槽和上传重试等待；相同 Markdown 不重复上传。同一天的模型总结仍最多尝试一次，后续点击只更新模板明细，避免重复计费。按钮并不授予其他 Drive 目录权限。
- **沙盒**：模拟 OWNER／成员／已知 Bot，以及群 @／普通群消息／私聊。可选择现有群策略，显示三个概率、置信度、网关结论、人设、thinking_level 和回复。调用真实 Gemini，使用独立管理预算；不调用 Sender、Service.accept、真实历史、私人快照、消息入队或 Session 写入。有效 BV／完整视频链接会走公开元数据筛查与 medium 摘要，Owner 私聊 `/deep` 可试 high；会修改状态的斜杠指令只提示跳过。已知 Bot、静默或未满足 @ 策略时显示“未调用”，不会伪造概率。OWNER 私聊人设仍要求先配置真实 OWNER_QQ_ID；未配置时保持 PUBLIC。
- **群策略**：仅能修改已在白名单中的群，不扩大群／用户白名单。后台保存值优先于环境初始值，并在重启后恢复。
- **Bot 过滤**：增删持久化列表并显示群冷却；不允许把 Owner 或本机器人加入过滤。移出后按普通成员策略处理，因此页面会要求确认移除。
- **滚动压缩参数**：位于“群聊与记忆”，分群聊／私聊调整触发条数、字符数、保留数，以及群聊短消息暂缓条件；可切换自动压缩，保存即生效，SQLite 持久化优先于环境。支持恢复环境配置。
- **Session**：列出已记录会话与热消息数；单独打开当前摘要，支持筛查后编辑、手动压缩、彻底重置。列表和详情不自动展示原始聊天内容。重置保留该会话的记忆开关，与 `/new-session` 一致。
- **订阅**：增删 UP UID 和直播间 ID。新增 UP 订阅投递给配置的 OneBot Owner；原官方 QQ 订阅继续保留。直播间只轮询公开开播状态（未开播／直播中／轮播），不拉流、不发弹幕、不生成直播内容摘要。
- **日记与日志**：预览最近约 8 天 Markdown、最后成功同步时间、固定错误码与下次重试。最近 50 条事件每 15 秒刷新，只含时间／来源／固定代码，完全不保存聊天、注入原文、URL、身份或原始异常。

直播状态使用固定的匿名公开端点 `api.live.bilibili.com/room/v1/Room/get_info`，同样受原限流和风控暂停约束；它和既有网页元数据端点都不提供本项目可依赖的稳定性保证。访问失败时记录告警，不尝试登录、绕过风控或抓取直播流。

## SQLite 变更

现有主库增加以下表，敏感配置和会话标识使用原 Vault 加密，索引使用原 HMAC：

| 表 | 字段／用途 |
| --- | --- |
| `admin_config` | `key PRIMARY KEY, payload BLOB`；后台覆盖的群策略、Bot 列表和 `compression` 参数对象 |
| `session_registry` | `id PRIMARY KEY, identity BLOB`；仅加密会话身份，供后台索引，不含聊天正文 |
| `audit_events` | `id, at, code, source`；最多 500 条固定事件码，接口返回最近 50 条 |
| `daily_metrics` | `(day, code, source) PRIMARY KEY, n`；按 UTC 日累计，约 32 天清理 |
| `live_watches` | `room_id PRIMARY KEY, target BLOB, last_status, next_poll`；直播间状态配置，投递身份加密 |
| `roaming_sync` 新列 | `last_error TEXT, last_success REAL`；只存受限错误码和时间 |

每个群／私聊仍使用独立 SQLite 文件，路径、密钥和隔离方式保持不变。每个 Session 文件新增：

```sql
CREATE TABLE recent_messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  payload BLOB NOT NULL,
  expires REAL NOT NULL
);
CREATE TABLE rolling_state (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  summary BLOB,
  expires REAL NOT NULL DEFAULT 0,
  epoch INTEGER NOT NULL DEFAULT 0,
  version INTEGER NOT NULL DEFAULT 0,
  attempt TEXT,
  retry_after REAL NOT NULL DEFAULT 0
);
```

`summary` 是逻辑上的 `rolling_summary`，最多 800 字符、加密保存；`recent_messages` 每行是一条 user 或 assistant 消息，同样加密。群内“谁”使用稳定匿名成员标签；真实 Owner 可标记为 BOISER。旧 `turns` 每轮一问一答首次访问时转换成两条热消息，事务内标记并清空旧表，不重复迁移。`session_meta` 和旧 `turns` 表保留以便识别版本，旧主库私聊迁移继续生效。启动时仅从现有会话文件的加密身份建立后台索引。

## 压缩与重置的一致性

一次用户发言和一次助手回答算 **2 条消息**，不是 2 轮。字符数是热窗口 user／assistant 正文的 Unicode 字符数之和，不计角色标签、旧摘要或小本本。默认值如下：

| 会话 | 自动触发条件 | 压缩后保留 |
| --- | --- | --- |
| `group:*` | 字符数 **≥6000**，或条数 **≥60**；按条数触发时，若字符数 **<1200 且条数 <90** 则暂缓 | 最近 **25 条** |
| `private:*` | 条数 **≥40** 或字符数 **≥6000** | 最近 **16 条** |

群聊 60 条／1199 字暂缓，60 条／1200 字会压缩；90 条短消息也会压缩；6000 字可提前触发，不受短消息条件限制。无旧消息可移出（总数不超过保留数）时不调用模型。私聊配置也用于其他已获准私聊，仅改变窗口，不赋予 Owner 人设或私人快照权限。

在回复发送成功后安排异步压缩，保留窗口之外的历史连同旧摘要先经过 Decisions，再由 Hydrogen Interactions `thinking_level="low"` 合并。同会话加锁，全局最多一个压缩模型任务，后台队列有上限。管理员“立即压缩”绕过自动开关、触发线和短消息暂缓，但仍保留配置的热窗口，并遵守安全、预算与失败冷却约束。

可在 `.env` 中设置（启动／重启时读取）：

```dotenv
HAKIMI_COMPRESSION_ENABLED=true
HAKIMI_GROUP_COMPRESSION_MESSAGES=60
HAKIMI_GROUP_COMPRESSION_CHARS=6000
HAKIMI_GROUP_COMPRESSION_KEEP=25
HAKIMI_GROUP_COMPRESSION_MIN_CHARS=1200
HAKIMI_GROUP_COMPRESSION_MAX_MESSAGES=90
HAKIMI_PRIVATE_COMPRESSION_MESSAGES=40
HAKIMI_PRIVATE_COMPRESSION_CHARS=6000
HAKIMI_PRIVATE_COMPRESSION_KEEP=16
```

后台“群聊与记忆 → 滚动压缩参数”可直接热更新，无需重启。`GET /admin/api/compression` 返回当前值、来源和本次启动的环境值；`PUT` 保存完整参数对象；`DELETE` 删除后台覆盖，恢复本次启动的环境值。三者沿用 Bearer 鉴权。保存只更新一个加密的 `admin_config` 记录（key=`compression`），不改物理会话表、不调用模型。保存值在重启后继续优先；如果修改了 `.env`，需先重启，再点击“恢复 .env 配置”。参数整体校验并原子切换，保留数须小于触发条数；群聊最大暂缓条数须不小于条数触发线，暂缓最小字符数须不大于字符触发线。条数上限 500、字符触发上限 64000。

更改参数不立即批量扫描或压缩会话，后续回复使用新值；可按会话手动压缩。正在执行的旧参数压缩如果与当前配置不符，将丢弃结果，不删除任何热消息；关闭自动压缩同样阻止该自动任务提交。已发生的模型调用仍按原预算结算。

压缩输入只来自当前会话，不包含小本本或其他会话。群聊记录客观话题、匿名成员、群梗和未完问题；Owner 私聊可额外保留明确表达的关注事项、情绪与约定。系统指令明确排除角色覆盖、忽略规则、赋权和敏感凭证，输入与生成输出经过安全门禁；摘要编辑同样要筛查。主对话将摘要放入 `<memory_summary_data>`，其内容被转义，且系统声明其只是不可信背景，不能覆盖规则。模型不控制存储操作。

每次压缩在付费前保存输入指纹与重试冷却。同一批输入发生失败、取消或崩溃后不自动重放；新数据最早在 10 分钟后允许新尝试。压缩成功只删除当次快照中的旧消息，期间新来的消息保留。压缩失败保留旧摘要和热窗口。为避免无预算时无限增长，硬上限随参数调整：群聊为最大暂缓条数的 2 倍（默认 180 条），私聊为触发条数的 2 倍（默认 80 条），且不小于旧 `HAKIMI_SESSION_TURNS × 2`。因此不会在达到新阈值前被旧 40 条上限截断；超过硬上限时才按旧到新裁剪。极长旧消息作为截断片段参与压缩，并明确标识省略。请求仍服从上下文字节上限。

`epoch` 用于清空／人工编辑，`version` 用于摘要版本检查。清空或编辑时立即递增；运行中的旧压缩结果不能提交。生成中清空会丢弃旧上下文产生的回复；已排队但未发送的旧回复在发送前再次检查，包括随机延迟之后；旧版无 epoch 的待发回复通过原 jobs.scope 定位并取消，无需解密其他会话。压缩期间追加消息不会使新消息丢失。已经发出的网络请求无法撤回。

热消息维持原 TTL（默认 24 小时），滚动摘要默认保留 7 天，可用 `HAKIMI_SUMMARY_TTL_SECONDS` 调整到 1 小时至 30 天。`/new-session`、撤回与历史折叠在清空或更新层的同时立即删除加密 `interaction_chains`；非沙盒且开启记忆的主对话仅在投递确认后提交 `interaction_chains`（`store=True` + `previous_interaction_id` 差量输入），而沙盒、禁用记忆会话、搜索与所有后台任务保持 `store=False`。

## 《小本本》：单文件只读 + 本地兜底

```dotenv
ENABLE_NOTEBOOK_SNAPSHOT=false
GDRIVE_NOTEBOOK_SNAPSHOT_FILE_ID=
NOTEBOOK_SNAPSHOT_PATH=./data/owner_notebook_snapshot.md
GDRIVE_SERVICE_ACCOUNT_FILE=/run/secrets/roaming-service-account.json
```

开启后严格只有认证 OneBot 的 `user_id == OWNER_QQ_ID` 且为对应私聊时，才把已筛查快照放入模型输入。群聊、非 Owner 私聊、官方 QQ OpenID、沙盒、摘要压缩和日记均不读取它。后台持有 ADMIN_TOKEN 的管理员可以查看快照，这是显式管理权限。

配置文件 ID 时，每 6 小时只读该文件并缓存于内存，也可通过按钮刷新。复用同一专用 Service Account，但只读令牌请求 `drive.readonly`；文件 ACL 仅向该账号授予该**单个脱敏文件**的 Viewer。禁止给它主记忆库或其他私人目录权限，禁止域级委派。只调用这个固定 ID 的元数据和下载／纯文本导出接口，不调用 files.list、目录遍历、快捷方式解析或写入接口。支持 Markdown、纯文本和 Google Docs 的纯文本导出。[Google 官方文件下载与导出说明](https://developers.google.com/workspace/drive/api/guides/manage-downloads)

`drive.readonly` 是只读权限范围，**不是单文件范围**；单文件边界由 Service Account 的文件 ACL 与代码的固定 ID 共同落实。日记仍使用原 `drive.file` 令牌与独立单目录写入器。两种业务不互用内容。远端刷新失败时保留上次通过检查的内存快照并显示错误，不静默切换到其他文件，也不扩大权限。

仅当文件 ID 为空时才读取本地兜底路径。后台粘贴更新仅在启用且为本地模式时可用，先筛查，再以 `0600` 临时文件原子替换；路径不能由请求修改，拒绝符号链接和非普通文件。读取最多前 4000 字符，下载字节也有上限；更新请求超过 4000 字符则拒绝。云端缓存不会自动落盘，重启后重新读取；本地模式的文件本身由管理员持久保存。

## 部署与 SSH 隧道验收

1. 停服并备份现有数据卷与独立保存的加密密钥。保留原 `.env`，补充新配置；不要用初始化脚本覆盖已有密钥。无第三方运行依赖新增，继续使用已锁定的 `google-genai>=3.2.0`。
2. 设置一个随机 `ADMIN_TOKEN`。例如在服务器本机执行 `python3 -c 'import secrets; print(secrets.token_urlsafe(32))'` 后填写到受保护的 `.env`。填好 `OWNER_QQ_ID`，需要快照时再显式开启相关开关并只读挂载 Service Account JSON。
3. 本次标准 Compose 服务名为 `hakimi-bot`，使用 bridge 网络，**唯一端口映射为 `127.0.0.1:12141:12141`**；容器内监听 `0.0.0.0:12141`。`compose.yaml` 与 `docker-compose.yml` 内容相同，`docker compose` 默认使用前者，也可显式 `-f docker-compose.yml`。
4. 从 0.2.x 的 host 网络版本迁移时，**在覆盖旧 Compose 文件之前**用旧配置运行 `docker compose down` 停掉旧服务（不加 `-v`；若已覆盖，先停止旧 `hakimi` 服务容器），再 `docker compose -f docker-compose.yml up -d --build`。保持项目目录／project name 不变以复用原 `hakimi-data` 卷。查看 `docker compose -f docker-compose.yml logs --tail 30 hakimi-bot`。
5. 从 0.3.0 升级时默认端口由 8080 改为 12141；同步修改 NapCat 反向 WS 地址、Nginx upstream 和 SSH 隧道。推荐 NapCat 反向 WS：宿主机 NapCat 连接 `ws://127.0.0.1:12141/ws/onebot`。正向 WS／HTTP 模式中，容器的 `127.0.0.1` 已不是宿主机；可配置受保护的 `onebot` Docker host-gateway 地址（例如 `http://onebot:3000`），同时让 NapCat 仅在可达的受保护接口监听，或放在同一个专用 Docker 网络。不要因此把 NapCat API 暴露公网。
6. 在**自己的电脑**执行：

```bash
ssh -N -L 12141:127.0.0.1:12141 user@your-server
```

保持 SSH 连接，浏览器打开 **http://127.0.0.1:12141/admin**，输入 ADMIN_TOKEN。本地 12141 被占用时用 `-L 18080:127.0.0.1:12141`，访问 `http://127.0.0.1:18080/admin`。后台拒绝公共域名，Nginx 模板也明确拒绝 `/admin`，不能通过公开机器人域名访问它。

先检查概览；在沙盒选择“已知 Bot”确认显示未调用，再选择普通群友群 @ 进行一次真实付费测试。新建测试私聊后查看摘要／热窗口，进行压缩和重置；确认群与私聊互不影响。未配置 ADMIN_TOKEN 的部署应对管理路径全部返回 403，缺失或错误 Bearer 应返回 401。真实 QQ／B 站／Drive 联调需要你的凭证与网络，本次未调用真实服务。

新增费用配置（单位 USD／UTC 日，均包含在原全局上限内）：

```dotenv
HAKIMI_COMPRESSION_ENABLED=true
HAKIMI_SUMMARY_TTL_SECONDS=604800
HAKIMI_MEMORY_DAILY_USD=0.20
HAKIMI_ADMIN_DAILY_USD=0.20
```

压缩输入／输出筛查和生成记入 `memory`；沙盒、摘要编辑筛查、快照安全筛查记入 `admin`；日记手动总结仍记入 `patrol`。达到额度后停止相应模型操作，不降级绕过安全检查。Google Drive 下载与固定模板操作本身不调用 Gemini。

## 本次验证结果

- 333 项自动测试全部通过；覆盖后台鉴权、来源检查、配置恢复、沙盒无副作用、真实 SDK 概率／模型／思考档位、压缩并发／取消／重置竞态、旧会话迁移、单文件 Drive 读取和只读 scope。
- Ruff 检查、格式检查、锁文件校验通过；本次没有新增运行依赖。
- 使用临时数据与模拟网关进行 Chromium 实际点击验收：登录、无口令持久化、群策略、Bot 列表、摘要编辑、沙盒、订阅、快照编辑、HTML 注入不执行、退出，以及桌面／移动布局。没有外部资源请求。
- `hakimi-surf:compression-test` 镜像构建和禁用外网、非 root、只读根文件系统的容器测试通过：反向 WS、Webhook 验签／去重、Bot 忽略／授权、后台 HTML 与 Bearer 鉴权、费用账本为零。基础与可选 Drive Compose 均确认只绑定 127.0.0.1:12141。
- 未使用真实 Gemini、QQ 或 Drive 凭证做线上联调；浏览器截图使用模拟数据。

0.3.1 新增验证覆盖精确触发边界、短消息暂缓不调用模型、25 条奇数窗口、热配置校验／持久化／恢复环境、在途压缩参数变更与提交序列化。Chromium 实际点击确认参数保存、页面刷新恢复和恢复环境值；12141 端口的容器反向 WS／Webhook／后台鉴权验收通过。
