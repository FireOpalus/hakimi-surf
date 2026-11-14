# Owner 专属主动私信（0.3.2 增量升级）

本模块沿用 FastAPI、OneBot、SQLite 和现有 Gemini 网关，无新增 Python 依赖、前端构建或外部 CDN。管理入口仍是 `http://127.0.0.1:12141/admin`，进入「主动私信」。

## 行为与费用

| 类型 | 触发 | 模型与预算 | 离线处理 |
| --- | --- | --- | --- |
| 运维告警 | 日预算／累计预算 85%、100%；B 站实际巡逻请求连续 3 次 412；Gemini 同一接口连续 5 次错误；NapCat 断线至少 5 分钟后恢复 | 本地模板，0 Token，预算熔断和夜间仍能投递 | 最多保留 12 小时 |
| 每日汇报 | 默认 22:30，也可后台手动触发 | Hydrogen、Interactions v1、`store=False`、`medium`，输入和输出经过 Decisions v1beta | 保留到该计划的有效期；定时汇报最迟当天结束，手动创建最多 12 小时 |
| 陪伴冒泡 | 每天本地随机抽取 1／2 个时间点；最近 3 小时私聊活跃则跳过 | Hydrogen、`low`；到点才调用，不轮询模型 | 最多 2 小时，且不跨免打扰开始或当地午夜 |

随机计划在每日零点后的第一轮调度中保存，首次启动也会补建当天计划。调度每 10 秒检查本地表；错过超过 5 分钟的陪伴时间点直接跳过，不在重启或免打扰结束后补发。正常断线期间，已生成且尚未过期的消息可在重连后按序投递。HTTP OneBot 模式没有 WS 在线信号，提交失败按结果不确定处理；本部署使用反向 WS。

日报和手动按钮共享当天唯一的生成记录。失败、取消或服务崩溃后不自动重复付费；再次点击只返回已有状态。日报仅取公开 B 站标题／简介摘要，以及明确获准记录的群聊活动类别和次数；不会猜测群内趣事、读取私聊原文或《小本本》。无素材时如实报告暂无更新。汇报不会触发一次新的 Drive 总结生成；Drive 的原有日记同步继续独立运行。

付费主动生成另有 `HAKIMI_PROACTIVE_DAILY_USD=0.10` 子预算，仍同时受全局、用户和累计上限约束。发送前检查硬熔断，每个真实模型请求仍执行事务预留。模板告警、测试消息与它们的记忆写入不触发模型或压缩调用。

费用是本地按配置价格估算，包含未结算预留；日报金额不包含生成它自身的调用。新账本按 `BOT_TIMEZONE` 汇总时间戳，升级前缺少精确时间的账目暂按 UTC 日近似归属。既有每日额度仍按 UTC 重置，后台明确显示这一点。累计计数以升级时仍保留的账目为起点，之后不随 32 天账目清理而归零；无法恢复升级前已清理的历史花销。**累计上限不是 Google 账户余额查询。**

## 安全与发送确认

- 发送底层同时校验平台、私聊类型、当前 Bot 身份及 `target_user_id == int(OWNER_QQ_ID)`。普通白名单用户、旧管理员、群、官方 QQ OpenID 均不能作为主动接收人。管理 API 不接受目标 ID。
- 生成时使用 `OWNER_PERSONA_PROMPT`；固定模板称呼 BOISER。没有提供任意工具执行能力。正文、目标和确认回执在 SQLite 中加密，日志只记录固定状态码。
- 新队列的 `pending → sending → delivered → sent` 分别表示等待、已准备提交、已收到 OneBot `message_id` 确认、记忆持久化完成。已提交但未得到确认的消息标为 `uncertain`，不会在重启或重连后自动重复发送。
- 收到确认后，只向 Owner 独立私聊库追加一条 `assistant`。Owner 对话处理与发送、记忆写入共用异步锁。确认与记忆写入之间崩溃时，只恢复记忆，不重发消息；回执表防止重复写入。
- 期间执行的清空操作使旧 epoch 失效，恢复不会复活旧记忆。显式关闭该会话记忆时继续尊重关闭设置，后台发送记录标为 `memory_reset_or_disabled`；默认记忆开启时正常保存主动消息。
- 同类运维告警默认冷却 6 小时；日预算／累计预算相同阈值每天只入队一次。一次同时跨越两个阈值只报更高阈值。关闭总开关会取消待发消息，开启后不恢复已取消消息。

## 环境变量与后台

```dotenv
BOT_TIMEZONE=Asia/Shanghai
PROACTIVE_DM_ENABLED=true
DAILY_DIGEST_ENABLED=true
DAILY_DIGEST_TIME=22:30
COMPANION_PING_ENABLED=true
PROACTIVE_CHAT_MAX_PER_DAY=1
QUIET_HOURS=23:30-08:30
HAKIMI_PROACTIVE_DAILY_USD=0.10
HAKIMI_TOTAL_USD=40.00
HAKIMI_PROACTIVE_ALERT_COOLDOWN_SECONDS=21600
HAKIMI_PROACTIVE_WS_ALERT_AFTER_SECONDS=300
```

后台可修改三个开关、日报时间、免打扰时段和每日上限，保存在加密的 `admin_config['proactive']`，优先于 `.env`。修改 `.env` 需重启；「恢复 .env 配置」移除覆盖。时区需使用有效 IANA 名称，修改后重启。`HAKIMI_TOTAL_USD=0` 可关闭累计上限，仍保留日额度。

页面展示今日已确认发送的陪伴次数、下次计划时间、离线／不确定队列状态、告警冷却和预算。页面操作仍经过原有 `ADMIN_TOKEN`、Origin／Host 校验和操作频控。关闭主动私信总开关时，两种手动发送也被禁用。

## SQLite 增量结构

| 表／改动 | 用途 |
| --- | --- |
| `proactive_days(scope, day)` | 当日计划已建立，防止重启重新抽签 |
| `proactive_schedule(id, scope, day, kind, due_at, expires_at, status, reason)` | 持久化本地时间计划和付费尝试状态 |
| `pending_owner_dms(id, scope, day, kind, payload, created, expires_at, status, session_epoch, sent_at, receipt, reason)` | 加密待发正文、确认状态、有效期和记忆恢复 |
| `alert_cooldown(alert_key, queued_at, cooldown_until, dm_id, sent_at)` | 同类告警去重，包含预算阈值和日期 |
| `owner_dm_state(scope, last_private_at, ever_online, online, disconnected_at)` | 私聊活跃窗口和长断线恢复检测 |
| `operational_health(provider, failures, changed)` | 实际 B 站／Gemini 请求的连续错误次数 |
| `budget_totals(singleton, amount)`、`ledger_times(id, created)` | 持久累计微美元和新请求时间戳 |
| 每个隔离 Session 的 `outgoing_receipts(id, expires)` | 已发送主动消息的幂等记忆写入 |

`scope` 绑定 namespace、当前 Bot 和唯一 Owner，切换 Owner 后旧待发消息会取消，不会改投给新 Owner。队列状态保留 8 天，计划保留约 9 天；已投递待修复的正文保留至记忆修复完成。迁移仅增加表，不重建或合并原有 Session。

主要新增文件：`src/hakimi/proactive_dm.py`、`src/hakimi/proactive_policy.py`、`tests/test_proactive_dm.py`。修改发送器、Service、WS 状态回调、账本、Session、Gemini 场景、B 站错误计数、管理路由和 `admin_page.html`。现有 Docker Compose 安全设置和 OAuth 只读挂载继续使用。

## 部署与验收

本服务器已使用 NapCat 与 Drive OAuth 叠加配置；更新时保留三个 Compose 文件：

```bash
cd /root/Document/hakimi-surf
docker compose -f docker-compose.yml -f compose.napcat.yml -f compose.roaming-oauth.yaml build hakimi-bot
docker compose -f docker-compose.yml -f compose.napcat.yml -f compose.roaming-oauth.yaml up -d --no-deps hakimi-bot
```

在自己的电脑保持 SSH 隧道运行：

```bash
ssh -N -L 12141:127.0.0.1:12141 用户名@服务器地址
```

打开 `http://127.0.0.1:12141/admin`，使用服务器 `.env` 的 `ADMIN_TOKEN` 登录，进入「主动私信」。检查时区、WS 状态、计划时间，点击「发送测试私信（0 Token）」后在 Owner QQ 查看固定文本。点击「立即生成并发送今日汇报」会消耗当天那一次汇报机会；反复点击不会重复生成。

全量单元测试使用假 OneBot 与模拟 SDK HTTP，不连接真实 QQ／Gemini／Drive。可运行 `.venv/bin/pytest -q` 和 `.venv/bin/ruff check src tests scripts`。生产只需一个后端进程，SQLite 的进程锁仍会拒绝第二个实例。

## 本服务器验收记录

2026-11-14：426 项测试全部通过，Ruff 检查和格式检查通过。Chromium 实测了设置保存、0 Token 测试按钮、模拟日报按钮和手机宽度布局；没有页面脚本错误或外部资源请求。

0.3.2 已通过原有三文件 Docker Compose 更新，后端与 NapCat 均 healthy，反向 WS 在线，12141 继续只绑定 127.0.0.1。真实通路已向唯一 Owner 发送一条固定测试私信：收到 OneBot 确认、Owner 私聊 assistant 记录及幂等回执均已落盘，队列正文已清理，发送前后 API 调用数增量为 0。

日报生成经过真实 SDK 序列化和模拟响应测试；部署验收没有额外执行收费的日报生成，保留默认 22:30 计划。Drive OAuth 同步保持启用，最近同步无错误；《小本本》仍关闭。升级前数据库、环境文件与旧镜像已有服务器本地备份。
