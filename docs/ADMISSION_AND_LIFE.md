# 0.4.0 准入名单、小黑屋与受控个签

> 准入与身份说明已更新至 0.6.1：群成员准入不再依赖私聊名单或旧 `HAKIMI_ALLOW_GROUP_MEMBERS`。聊天管理权限仅限真实 OneBot `OWNER_QQ_ID`；人设变化见 [0.6.1 说明](PERSONA_AND_ADMISSION.md)。文末部署记录属于 0.4.0 历史验收。

本次在 0.3.2 上增量升级。原有 Decisions `gemini-decision-1`／`v1beta`、Interactions `gemini-4-hydrogen`／`v1`、SQLite 账本、物理 Session、Owner 私信和 Drive 日记继续使用。未新增 Node/npm/CDN 或第三方 Python 依赖。

## 管理台与准入

使用原来的 SSH 隧道和 `ADMIN_TOKEN`：

```bash
ssh -N -L 12141:127.0.0.1:12143 用户名@服务器
```

浏览器打开 `http://127.0.0.1:12141/admin`。新增「准入与防互刷名单」「小黑屋与每日个签」「B站小号巡逻配置」三个页签。所有管理 API 继承原鉴权、同源检查、请求体和速率限制；空口令仍全部返回 403。

准入面板可增删群白名单、普通好友白名单和已知 Bot 黑名单。输入必须是无前导零的纯数字 QQ／群号，重复添加返回明确错误。新群立即出现在群聊策略列表中；移除群立刻撤销接收权限，不删除已有隔离历史。空群白名单拒收所有群，未提供隐式全群放行。群获准后，所有非黑名单成员均可按 `at_only`／`smart_reply`／`muted` 策略互动，不要求成员列入私聊 `allowed_users`。`HAKIMI_ALLOW_GROUP_MEMBERS` 为兼容旧配置保留，已不参与群成员准入。

私聊模式默认 `owner_only`，可在后台改为 `owner_and_allowlist`。真实 `OWNER_QQ_ID` 不依赖普通好友名单，无法在页面误删。白名单好友只获得公开人设与自己 `private:{user_id}` 的物理 SQLite 会话，不能读取 Owner 快照、私聊或专属命令。昵称、正文或模型输出不参与身份认定。

旧 `HAKIMI_ONEBOT_OWNER_IDS` 和 `HAKIMI_QQ_OWNER_IDS` 不再授予聊天管理或 Owner 权限。官方 QQ 的 OpenID 只能作为公开身份；普通好友私聊需单独列入 `HAKIMI_QQ_ALLOWED_USERS`。管理后台仍由原 `ADMIN_TOKEN` 保护，不能通过 QQ 昵称或群角色登录。

群／私聊准入、外部 Bot 和小黑屋均在模型与素材下载前判断。黑名单 Bot 无条件忽略；旧 `/bot-interact` 不再放行。Owner 与机器人自身不能加入 Bot 黑名单或小黑屋。陌生加好友请求忽略，不调用自动批准接口。

三份准入名单和私聊模式作为加密 `admin_config['admission']` 持久化；启动优先加载 SQLite，首次读取 `.env`，兼容旧的 `bot_ids` 后台记录。页面展示来源和 `.env` 初始配置，可明确重置回环境值。添加、移除、模式切换立即生效；更改 `.env` 文件本身仍需重启进程。

## 小黑屋与明确审批

对非 Owner 群友，只有其**当前消息**被 Decisions 判定为注入或违规才记一次 strike，不把旧历史、网页素材或模型输出的拦截归咎于当前发送者。同一消息去重；24 小时内达到 3 次生成编号提案，48 小时后过期，同一用户 24 小时冷却。通知通过现有 Owner 主动私信通路发送固定模板，含 QQ、清理后的昵称和原因类别，不含原攻击文本，不额外调用模型。

没有自动屏蔽。Owner 私聊可用：

```text
/mute-approve <提案编号>
/mute-reject <提案编号>
/mute <QQ号>
/unmute <QQ号>
```

后台也支持通过、驳回、手动屏蔽和解除。普通批准／屏蔽只更新应用层拒收，不动 QQ 好友关系。若明确需要删除好友，使用单独的「批准并移除好友」确认，或：

```text
/mute <QQ号> --delete-friend
/mute-approve <提案编号> --delete-friend
```

程序先硬校验非 Owner、非机器人自身，再检查好友列表，只有目标确实是好友才调用 `delete_friend`。删除失败不撤销已批准的应用层屏蔽；请求结果不明时记录 uncertain，不自动重放。解除小黑屋不会自动重新添加好友。

## 每日个签

`ENABLE_DAILY_LONGNICK=false` 默认关闭，`LONGNICK_COOLDOWN_HOURS=20`。后台配置持久化覆盖环境值；开启后每日汇报在**同一次 Hydrogen Interactions 调用**中附带 `daily_longnick` 字段，不另开生成调用。程序独立检查开关、20 小时冷却、50 字上限、纯文本和敏感信息，再调用 NapCat `set_self_longnick`，参数为 `longNick`。自动尝试也计入冷却，避免失败刷接口。

Owner 私聊 `/set-sign <内容>` 或管理台输入框可明确手动修改。手动操作有 60 秒重复提交保护。模型没有账号管理工具；其他群友、群聊中的 Owner、伪造昵称都不能通过对话触发。NapCat 不支持、返回风控错误或连接中断时记录固定告警状态，不影响日报发送；不确定结果不会自动重试。

未实现空间说说、修改头像和修改昵称。共享提示词要求对能力与结果保持诚实；三个提示词正文使用 0.6.1 提供的原文，不另追加能力清单。账号操作仅在协议确认成功后宣称完成。

## 新增表和模块

主库以 `CREATE TABLE IF NOT EXISTS` 增量迁移，原有每会话 SQLite 文件无需转换。原始身份、凭证、签名和摘要继续使用现有加密密钥绑定用途加密，检索身份使用 HMAC。

| 表／配置 | 内容 |
| --- | --- |
| `admin_config['admission']` | 群／好友／Bot 名单、私聊模式 |
| `muted_users` | 身份密文、来源、屏蔽时间、可选好友删除结果 |
| `user_strikes` | 去重事件键、用户键、固定原因类别、时间 |
| `pending_mute_proposals` | 编号、目标密文、原因、过期时间、审批状态 |
| `admin_config['longnick']`、`longnick_state` | 自动开关／冷却、最后尝试与成功、签名密文、状态 |
| `bili_credentials`、`bili_account_state` | 小号 Cookie 密文、登录检查、账号密文与冷却 |
| `bili_patrol_runs`、`bili_watched_history`、`bili_actions` | 每日轮次、BV 去重、文本心得及互动配额／结果 |

新增 `admission_policy.py`、`moderation.py`、`longnick.py`、`community_admin.py`、`bili_watcher.py`、`bili_account_admin.py`。修改现有 `runtime_state.py`、`security.py`、`service.py`、`app.py`、`onebot_transport.py`、`proactive_dm.py`、`gemini.py`、`generation_policy.py`、`persona.py`、`admin_routes.py` 和内嵌 `admin_page.html`。完整 B 站配置、固定接口与风险限制见 [小号文本巡逻](BILI_ACCOUNT.md)。

## 升级与验收

备份 `.env`、原加密密钥与 SQLite 数据，保留 namespace、Owner 和 Drive 配置。不要重新运行 `init_env.py` 覆盖现有配置。当前使用 NapCat + 个人 Drive OAuth 的服务器更新后端命令：

```bash
docker compose -f docker-compose.yml -f compose.napcat.yml -f compose.roaming-oauth.yaml build hakimi-bot
docker compose -f docker-compose.yml -f compose.napcat.yml -f compose.roaming-oauth.yaml up -d --no-deps hakimi-bot
```

SQLite 配置与数据卷保留，NapCat 不重建，12141 仍只绑定回环地址；《小本本》保持关闭。新 B 站凭证未配置时保持巡逻关闭，需在后台填入独立小号 Cookie 后先进行 nav 测试。部署验证只检查状态与权限，不自动删除好友、修改签名或给真实视频点赞。

验证包括 539 项自动测试、Ruff 和 Chromium 浏览器交互：名单即时联动、公开好友隔离、审批与保护对象、单次日报调用、签名失败隔离、Cookie 密文与不回显、WBI 向量、预算门禁、12 小时退避、每日次数及重启恢复。浏览器同时检查无外部资源请求和窄屏溢出。上述外部动作使用模拟协议，不代表真实 B 站 Cookie 或所有 NapCat 版本已经完成账号联调。

2026-11-14 生产部署检查：0.4.0 镜像内源码与交付源码摘要一致，`/healthz` 正常，NapCat 反向 WS 在线；新接口未鉴权均返回 401。后端继续以 UID 10001、只读根文件系统运行，12141 仅绑定 127.0.0.1，Drive OAuth 文件只读挂载，已有日记同步成功状态保留。群白名单当前为空，私聊为 Owner-only，小本本／自动个签／小号巡逻关闭，尚未填写 B 站 Cookie。

升级前数据库与 `.env` 备份位于 `/root/.local/share/hakimi-backups/20261114-143255-pre-0.4.0`（仅服务器 root 可读），旧镜像保留为 `hakimi-surf-hakimi-bot:pre-controls-0.3.2`。备份不进入交付包。隔离容器另行验证了实际 WS 握手／固定回复、Webhook 验签／去重、新接口及零模型账本；没有对真实 QQ 好友或资料执行修改。
