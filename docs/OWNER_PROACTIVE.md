# Owner 通知和世界引擎

在管理后台的「世界与主动引擎」设置活跃时段、主动免打扰时段及每日自主活动上限。B 站小号巡逻和主动聊天共用这一套调度。Owner 最近三小时在私聊互动过时，世界引擎跳过普通陪伴问候。高价值巡逻发现可进入世界事件队列，由引擎决定是否分享。

## 每日汇报

引擎按 `BOT_TIMEZONE` 的自然日，在活跃时段、免打扰和全局休息时段共同允许的最后 15 分钟内安排一份日报。例如活跃时段在 22:30 结束时，日报从 22:15 起可执行。跨午夜的活跃时段按当地自然日分账，完全没有可用窗口的日子跳过。日报单独保留每日一次机会，仍受模型预算、引擎开关以及 Owner 投递权限约束。

「QQ → 主动私信」可查看投递状态、离线队列和告警，也可以测试通路或提前发送今日汇报。手动和自动共用同一份 SQLite 生成记录。重启或重复点击保留已生成结果。日报仅取公开巡逻素材和脱敏群聊统计，断线期间保存在本地，定时汇报到窗口结束即过期。

## 运维和迁移

预算、平台风控和断线恢复告警使用本地模板，独立于生命引擎的规划周期及免打扰。接收人锁定为 Owner，投递确认后才写入私聊记忆。预算子限额在「Token 与预算账本」中维护。

升级时，原 QQ 投递许可、免打扰和已关停的日报迁入世界引擎。旧表中未开始的时刻表及待发随机问候退休，已提交、已送达和结果不确定的投递保留其记录。旧 B 站时间点和随机偏移停止参与排程。小号凭证、订阅、账号冷却、互动及原片费用上限沿用已有配置。

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
QUIET_HOURS=23:30-08:30
HAKIMI_PROACTIVE_DAILY_USD=0.10
HAKIMI_TOTAL_USD=40.00
HAKIMI_PROACTIVE_ALERT_COOLDOWN_SECONDS=21600
HAKIMI_PROACTIVE_WS_ALERT_AFTER_SECONDS=300
```

世界引擎的后台配置覆盖启动默认值，保存在加密的 `admin_config['life_engine']`。QQ 页提供投递状态、离线队列、告警和手动操作，继续使用现有后台鉴权和频控。投递许可关闭时同样禁止手动发送。

## SQLite 增量结构

| 表／改动 | 用途 |
| --- | --- |
| `proactive_days(scope, day)` | 旧随机日程的迁移记录 |
| `proactive_schedule(id, scope, day, kind, due_at, expires_at, status, reason)` | 每日汇报的幂等凭据与付费尝试状态 |
| `pending_owner_dms(id, scope, day, kind, payload, created, expires_at, status, session_epoch, sent_at, receipt, reason)` | 加密待发正文、确认状态、有效期和记忆恢复 |
| `alert_cooldown(alert_key, queued_at, cooldown_until, dm_id, sent_at)` | 同类告警去重，包含预算阈值和日期 |
| `owner_dm_state(scope, last_private_at, ever_online, online, disconnected_at)` | 私聊活跃窗口和长断线恢复检测 |
| `operational_health(provider, failures, changed)` | 实际 B 站／Gemini 请求的连续错误次数 |
| `budget_totals(singleton, amount)`、`ledger_times(id, created)` | 持久累计微美元和新请求时间戳 |
| 每个隔离 Session 的 `outgoing_receipts(id, expires)` | 已发送主动消息的幂等记忆写入 |

`scope` 绑定 namespace、当前 Bot 和唯一 Owner，切换 Owner 后旧待发消息会取消，不会改投给新 Owner。队列状态保留 8 天，计划保留约 9 天；已投递待修复的正文保留至记忆修复完成。迁移仅增加表，不重建或合并原有 Session。

主要新增文件：`src/hakimi/proactive_dm.py`、`src/hakimi/proactive_policy.py`、`tests/test_proactive_dm.py`。修改发送器、Service、WS 状态回调、账本、Session、Gemini 场景、B 站错误计数、管理路由和 `admin_page.html`。现有 Docker Compose 安全设置和 OAuth 只读挂载继续使用。

## 部署与验收

首尔后端使用 Drive OAuth 叠加配置。

```bash
cd /root/Document/hakimi-surf
docker compose -f docker-compose.yml -f compose.roaming-oauth.yaml build hakimi-bot
docker compose -f docker-compose.yml -f compose.roaming-oauth.yaml up -d --no-deps hakimi-bot
```

在自己的电脑保持 SSH 隧道运行：

```bash
ssh -N -L 12141:127.0.0.1:12141 用户名@服务器地址
```

打开 `http://127.0.0.1:12141/admin`，使用服务器 `.env` 的 `ADMIN_TOKEN` 登录，进入「主动私信」。检查时区、WS 状态、计划时间，点击「发送测试私信（0 Token）」后在 Owner QQ 查看固定文本。点击「立即生成并发送今日汇报」会消耗当天那一次汇报机会；反复点击不会重复生成。

全量单元测试使用假 OneBot 与模拟 SDK HTTP，不连接真实 QQ／Gemini／Drive。可运行 `.venv/bin/pytest -q` 和 `.venv/bin/ruff check src tests scripts`。生产只需一个后端进程，SQLite 的进程锁仍会拒绝第二个实例。

## 0.3.2 历史验收记录

2026-11-14：426 项测试全部通过，Ruff 检查和格式检查通过。Chromium 实测了设置保存、0 Token 测试按钮、模拟日报按钮和手机宽度布局；没有页面脚本错误或外部资源请求。

0.3.2 已通过原有三文件 Docker Compose 更新，后端与 NapCat 均 healthy，反向 WS 在线，12141 继续只绑定 127.0.0.1。真实通路已向唯一 Owner 发送一条固定测试私信：收到 OneBot 确认、Owner 私聊 assistant 记录及幂等回执均已落盘，队列正文已清理，发送前后 API 调用数增量为 0。

日报生成经过真实 SDK 序列化和模拟响应测试；部署验收没有额外执行收费的日报生成，保留默认 22:30 计划。Drive OAuth 同步保持启用，最近同步无错误；《小本本》仍关闭。升级前数据库、环境文件与旧镜像已有服务器本地备份。
