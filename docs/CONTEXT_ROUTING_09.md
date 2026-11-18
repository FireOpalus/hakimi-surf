# v0.9.0：稳定聊天前缀、单次入站路由与 Carbon 专家档

本次增量升级不更改境内 NapCat／境外 FastAPI 部署拓扑。12141 继续只绑定回环，公网 Nginx 仅放行受鉴权的反向 WS 路径；Owner 身份、Fernet 隔离文件、epoch 清空、BotGuard 两轮熔断和北京时间账本保持原契约。

## 文件与入口

| 文件 | 改动 |
| --- | --- |
| `persona.py`、`prefix_runtime.py` | 原三段正文哈希不变；新增公开／Owner 静态扩展、10／8 组示例及进程内静态前缀池 |
| `bot_server.py`、`context_budget.py` | 稳定序号历史序列化、B 在前 C 在后、转义伪造角色与标签、本地长度估算 |
| `session_manager.py`、`memory_compressor.py` | 追加历史、单事务整批折半、加密持久化压缩交接与后台恢复 |
| `gateway.py`、`gemini.py`、`routing.py` | 一次入站 Decisions 同时安全检查、接话、召回、查资料与推理分档；严格保留来源／输出门禁 |
| `thinking_policy.py`、`generation_policy.py`、`config.py` | 两个聊天场景 auto；Carbon 单路并发、预留不足回退与冷池上下文 |
| `usage_accounting.py`、`model_accounting.py`、`store.py` | 分模型保守扣账／折后估算、有效缓存覆盖率及异常事件 |
| `admin_routes.py`、`admin_page.html` | 聊天 auto 选项、真实模型预览、分模型账本、有效覆盖率和新压缩策略说明 |

## A / B / C

- **A**：`system_instruction`。共享指令、人设、自我身份锚点、公开 AT／MEME 协议与白名单、静态扩展按固定顺序拼接。切换群成员不会改 A。管理员更新表情时刷新；极大白名单会占用补充说明段预算，保留所有标签、固定协议和全部示例，不修改原三段正文。
- **B**：`input` 首部 `<conversation_history_log>`，每条为 `seq|role|QQ:数字|转义昵称|转义正文`。序号来自 SQLite 自增主键，不随窗口位置变化。新增记录只在旧历史行尾追加；闭合标签自然后移，不把闭合标签当成上一轮可复用的历史字节。
- **C**：末尾 `<transient_turn_context>`。当前时间与时区、协议核验身份、当前批次、允许点名名单、摘要、Owner 独享背景和沙箱状态均置于此。图片最多两张，位于整个输入数组最末端。不会写回历史，只保存用户纯文本／附图占位与助手可见回复。

本地估算目标：A 4500–5200 Token，C 不超过 1800 Token（短消息不为凑下限填充文本）。SDK 尚无可离线使用的 Gemini 4 精确 tokenizer，因此本地 CJK／ASCII 估算用于长度控制，不能声称与服务端 Token 完全相等。收费、缓存与告警使用服务端 `usage` 原始计数，预算预留仍按更保守的 UTF-8 字节和媒体上界计算。

B 默认跨过 **估算 28000 Token 或 96000 UTF-8 字节**才裁最旧一半记录。单条当前输入仍受 16000 字节上限约束。隐私 TTL 到期、手动压缩、清空、关闭记忆会改变前缀，均优先执行并标记窗口改变，不能为了缓存保留过期隐私数据。旧 40/60 条规则只供显式兼容模式或手动压缩保留条数使用。

当前消息过长、扣除判定规则后放不进单次守门的本地 1200 Token 文本预算，或放不进 C 上限时会拒绝本轮，不会偷偷截掉可能含攻击的尾部再放行。该估算含问题说明与文本；低分辨率图片另计媒体用量，不保证图文请求总数恰好小于 1200。请拆成较短问题，不对超限批次多轮重试。

## 守门与模型分档

入站一次 `v1beta decisions.create(gemini-decision-1)` 提问五个 `noul` 和一个 `choice`：安全／注入、是否接话、是否召回、是否查询 B 站，以及 `reasoning_tier`。最多八条脱敏短背景与当前批次分块；安全题明确只追责当前批次与附图。来源和生成结果仍单独过严格 Decisions 门禁，不能把“入站一次”理解为全流程只有一次 API 请求。

硬阈值仍由后台热控；群里直接找 Bot 且注入概率落入 `[0.55, 硬阈值)` 时，强制 L1 沙箱，清空 B 及 C 中摘要／私密背景，禁止引出 B 站分支和 Bot 互动授权。手动 xhigh 与 `/deep` 均不能覆盖安全沙箱。

| 档位 | 判定（从高到低） | 主模型／思考 | 输出＋思考上限 |
| --- | --- | --- | ---: |
| L4 | p4 ≥ .42 | Carbon medium | 12288 |
| L3 | p3+p4 ≥ .48 | Hydrogen high | 8192 |
| L2 | p2+p3+p4 ≥ .45 | Hydrogen medium | 4096 |
| L1 | 其他 | Hydrogen low | 1536 |

有图、召回概率 ≥ .50 或当前正文 >280 字时最低 L2；沙箱优先固定 L1。自动模式不使用 Hydrogen xhigh 或 Carbon low。仅 `fast_chat`、`owner_private` 默认 auto；未收到路由结果的直调回退 medium，其他七个场景仍 medium。手动 low/medium/high/xhigh 优先于自动路由，沿用场景原输出上限；Owner 明确 `/deep` 保持原授权和 8192 上限。

Carbon 群聊只接受直接找 Bot 或真实 Owner 触发。关闭、并发占满或本地预算预留不足时立即回退 Hydrogen high。Carbon 全局单路，不排队等待专家槽位；API 已发出后的超时、失败、取消或不完整结果**不换模型重做**。600 秒内沿用同一历史起点，冷池只取最近 12 条完整历史（估算 ≤3000 Token）；重置、TTL／折半截断会使锚点失效。管理沙盒使用独立的运行态命名空间。

两种主模型均使用 v1 Interactions、空工具与无后台续写；v1.1.4 起，非沙箱且开启本地记忆的主对话（`FAST_CHAT` / `OWNER_PRIVATE`）启用 `store=True` + 加密本地托管的 `previous_interaction_id` 差量续链，而 `search()`、沙盒与所有后台摘要／压缩任务继续保持 `store=False`、无 `previous_interaction_id`。

## 迁移与账本

所有扩展幂等；没有重写 Fernet 密文格式或原财务表：

- 每个隔离 Session 增加 `rolling_state.trim_version`，新增 `history_folds(id, payload, epoch, expires, attempted)`。先在同一事务加密保存旧半窗，再删除旧记录并增加版本。压缩提交校验 epoch/version，清空删除未完成交接。历史折半保留期遵循摘要 TTL，最多积压 16 批；无法确认的已付费尝试不自动重播。
- 主库新增 `model_usage_aggregates(period, model_lane, ...)`、`cache_aggregates(period, model_lane, ...)`，使用迁移标记防重复归集。原 `ledger_usage` 七列结构不变；旧 `interaction` 统计归为 Hydrogen，缺失模型记录标为 unknown，已清理的旧明细不会编造。
- Carbon 每百万输入／缓存／输出默认 $2 / $0.10 / $10，2026-12-01 UTC 起自动使用 $4 / $0.20 / $20；对应促销／常规六个环境配置可调整。[官方费率](https://aistudio.google.com/docs/pricing#gemini-4-carbon)
- `cache_observed_hit_ratio` 保留。`effective_cache_hit_ratio` 对非首轮、非窗口改变轮且输入 ≥6144 的聊天计算 `cached / max(1,input-2050)`，累计时按合资格分母加权。分模型展示，缺少缓存字段为未知，不记作零。
- 同会话同模型曾命中 ≥8000，连续两轮跌到 <5200 且没有清空／裁剪／静态规则刷新时，写脱敏 `cache_prefix_busted`。事件不包含原聊天、QQ 号或提示词。

[官方缓存规范](https://aistudio.google.com/docs/caching)说明 Gemini 4 的缓存起点与末尾 2050 Token 重算规则。B 超过一万 Token 后，90% 是线上稳态观察目标，服务无法保证第三方实际缓存命中；不能用本地字节估算代替 API 缓存证据。

## 配置和验收

`.env.example` 已列新配置。已有 SQLite 手动思考设置不会被迁移覆盖；需要路由分档时在 `/admin` 把前两个场景改为 auto 并保存。Carbon 开关及价格在环境中设置，重启后生效；每日／月预算及聊天档位仍支持管理台热更新。

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

测试用实际 SDK + MockTransport 验证请求，外网 socket 被测试夹具阻断；本地回环仅用于 WS 测试。新增覆盖：三段人设哈希、八轮字节前缀、历史折半事务回滚与压缩恢复、静默入史、角色转义、软注入、上尾分档、媒体托底、Carbon 槽位／预算／不确定失败、冷池锚点、费率与缓存告警。升级前另在当前生产 0.8.1 主库副本连续初始化两次，对五张原财务表逐表比较内容哈希。

部署沿用 `docker-compose.yml` + `compose.roaming-oauth.yaml`，仅替换后端镜像，不动境内登录态、Nginx 或网盘授权。回滚须停后端后同时恢复预升级镜像与备份数据库；不能将新版本产生的账本直接丢弃后恢复旧日预算继续使用。
