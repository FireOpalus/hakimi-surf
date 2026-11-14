# 人设、身份与 Bot 过滤（0.6.1）

本页说明当前规则，并保留旧 Bot 邀请机制的迁移背景。0.6.1 的三段提示词及群／私聊准入变化见 [本次升级说明](PERSONA_AND_ADMISSION.md)；0.6.0 的网关阈值、审计与预算机制继续有效，见 [过滤与账本](FILTERING_AND_BUDGETS.md)。

## 提示词顺序

`src/hakimi/persona.py` 保存用户提供的 `SYSTEM_INSTRUCTION`、`PUBLIC_PERSONA_PROMPT` 和 `OWNER_PERSONA_PROMPT` 原文。不会另加括号动作、舞台旁白或 AI 客服套话，也不通过删除输出中的全部括号来改写正常代码或说明。

一般 Interactions 调用按以下顺序组装 `system_instruction`：

1. 所有场景共享的 `SYSTEM_INSTRUCTION`。
2. 由服务器身份校验选出的公开或 Owner 差异段。
3. 对应摘要、巡逻、主动汇报等场景的业务指令。
4. 服务器生成的可信角色与场景标记。

滚动记忆压缩保留原特例：共享 `SYSTEM_INSTRUCTION` 加客观压缩指令，不装载 Owner 人设或动态身份块。三个提示词正文不因场景变化而重写；动态时间和业务数据不拼到共享前缀前面。

## 人设与身份边界

| 来源 | 可信角色 | 身份差异段 | 使用的历史 |
| --- | --- | --- | --- |
| OneBot Owner 本人私聊 | OWNER | OWNER_PERSONA_PROMPT | 仅该私聊文件 |
| OneBot Owner 在群内 | OWNER | PUBLIC_PERSONA_PROMPT | 仅当前群文件 |
| 其他获准群成员／白名单私聊好友 | MEMBER | PUBLIC_PERSONA_PROMPT | 当前群／本人私聊文件 |
| 获准的官方 QQ Bot OpenID 用户 | MEMBER | PUBLIC_PERSONA_PROMPT | 该平台当前会话文件 |
| 定时公开巡逻、每日云端日记 | MEMBER | PUBLIC_PERSONA_PROMPT | 无聊天历史 |

Owner 身份来自已通过 HMAC／WS Token、机器人 Self-ID 与事件格式检查的 OneBot `user_id`，精确匹配唯一 `OWNER_QQ_ID`。不信任昵称、群名片、正文中的角色声明或模型判断。只有本人私聊才装载专属人设及已启用的只读快照；在群中仍使用公开人设，不读取 Owner 私聊数据库。

旧 `HAKIMI_ONEBOT_OWNER_IDS`／`HAKIMI_QQ_OWNER_IDS` 为配置兼容保留，不授予聊天管理权限、`/deep` 或 Owner 身份。官方 QQ OpenID 不能证明数字 QQ 身份；普通好友私聊须列入 `HAKIMI_QQ_ALLOWED_USERS`。`OWNER_QQ_ID` 为空时不授予 Owner 权限。管理后台独立验证 `ADMIN_TOKEN`。

来自消息、历史、网页及卡片的 JSON 仍放入 `<untrusted_user_input>`；尖括号与 `&` 转义，正文里的字面 `[role=...]` 改写成不可信角色声明。所有人设选择、会话访问和管理权限均由 Python 决定，模型不能切换身份。Interactions 每轮重传 `system_instruction`，使用 `tools=[]`、`store=False`，不通过服务器端 interaction ID 串联历史。

## 群成员与私聊准入

群白名单与私聊白名单彻底独立：群号在 `allowed_groups` 内时，所有非黑名单成员均可按本群策略互动；不要求成员列入 `allowed_users`，旧 `HAKIMI_ALLOW_GROUP_MEMBERS` 也不再限制群员。

- `at_only`：响应顶层真实 @、直接昵称呼唤、核验过的 Bot 引用回复以及 B 站链接／卡片等明确请求。
- `smart_reply`：除明确请求外，可根据同群近期上下文判断自然插话。
- `muted`：本地直接忽略。

OneBot 私聊仍默认仅 Owner；后台切换 `owner_and_allowlist` 后才允许指定普通好友。群内获准不意味着该成员可以单独私聊、读取其他会话或执行管理命令。群消息同样先过滤机器人自身、外部 Bot 和小黑屋成员，命中时不下载素材、不读取会话、不调用模型。

明确请求跳过 `should_reply`，仍做消息安全检查。普通成员自由插话的默认阈值为 `0.42`，群预设为安静 `0.65`／自然 `0.42`／活跃 `0.28`；真实 Owner 在群内的自由插话阈值取当前群阈值和 `0.28` 中较小者。偏爱只影响语气和活跃度，不增加权限，也不解除静默、黑名单或预算限制。

## 当前 Bot 过滤

在原 `.env` 中配置真实数字 ID，或通过 `/admin` 热更新外部 Bot 名单：

```dotenv
OWNER_QQ_ID=你的真实数字QQ号
BOT_QQ_IDS=["其他机器人数字QQ号"]
```

`BOT_QQ_IDS` 兼容名称为 `IGNORED_BOT_QQ_IDS` 或 `HAKIMI_BOT_QQ_IDS`，只使用一个名称。Owner 与机器人自身不能加入此列表；机器人自身消息由自回环规则直接忽略。

0.4.0 起，名单中的 Bot 在群聊和私聊中均被前置忽略，真实 @ 或 `/bot-interact` 也不能临时放行。若 Owner 希望解除该限制，需要在后台明确移除对应 Bot，之后仍受群／私聊准入、互动策略、限流和预算控制。程序不会识别尚未列入名单的其他机器人账号。

## 历史机制：0.2.2 Bot 邀请（已废弃）

0.2.2 曾支持 Owner 在群内通过 `/bot-interact` 与真实 @ 创建一次性邀请：单条消息、2 分钟有效、至少 5 分钟冷却。邀请消耗与任务入队在同一事务中处理，授权和冷却保存在 `bot_interactions`、`bot_turns` 中。

这些表的 scope、目标、话题和授权人使用 HMAC 绑定 namespace／平台／机器人／群／事件，不保存原始话题或私聊资料。原表及历史恢复逻辑可以保留，但不构成当前名单的例外，旧邀请不会让黑名单 Bot 重新获准。不要按旧文档配置用户白名单或发送邀请来绕过当前过滤。

## 升级与验证

本次不增加模型或工具权限：安全与输出筛查继续 `gemini-decision-1`／`v1beta`，生成继续 `gemini-4-hydrogen`／Interactions `v1`。人设文本仍计入费用预留；本地准入与固定命令不调用模型。

升级保留原 `.env`、加密密钥、namespace、SQLite 数据和已保存的名单。不要重新运行 `init_env.py` 覆盖配置。回归检查应覆盖真实 SDK 的共享提示词前缀、身份差异段、伪造身份、群／私聊隔离、普通群成员放行、黑名单零消耗、私聊拒收以及旧管理员无法执行管理命令。当前验证记录见 [0.6.1 升级说明](PERSONA_AND_ADMISSION.md)。
