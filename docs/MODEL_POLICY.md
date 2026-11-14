# 0.8.1 模型与按场景思考强度

安全与路由继续使用 `gemini-decision-1`、`v1beta`、Decisions API；所有生成继续使用 `gemini-4-hydrogen`、GA `v1`、Interactions API，`store=False`。管理台调档只改变后续生成请求的 `generation_config.thinking_level`，不替换模型、不赋予权限，也不会为了保存配置额外调用模型。

Hydrogen 的四档为 `low`、`medium`、`high`、`xhigh`，不支持 `minimal`。0.8.1 按最新要求将以下九个场景的默认值全部改为 `medium`，后台最高允许 `xhigh`。此前版本的群快聊、压缩、陪伴与候选挑选 `low` 默认属于历史行为，不再代表新版本默认值。[官方档位说明](https://aistudio.google.com/docs/thinking#thinking-levels)

| 后台场景 | 配置键 | 默认值 |
| --- | --- | --- |
| 群聊／普通好友 | `fast_chat` | `medium` |
| Owner 私聊 | `owner_private` | `medium` |
| B站视频摘要 | `bilibili_summary` | `medium` |
| 巡逻日记 | `daily_diary` | `medium` |
| 滚动记忆压缩 | `memory_compression` | `medium` |
| Owner 每日汇报 | `owner_digest` | `medium` |
| 日常陪伴 | `owner_companion` | `medium` |
| B站候选挑选 | `bili_selection` | `medium` |
| B站学习心得 | `bili_learning` | `medium` |

## 管理台操作与热更新

通过 SSH 隧道进入 `/admin`，选择「思考强度」。九个下拉框可以分别调整；顶部「统一设置全部场景」先把选定档位填入下拉框，再点击「保存并立即生效」。改动保存到 SQLite，重启后优先使用后台保存值；无需重建容器。

面板同时显示配置来源与启动时 `.env` 初始值。「恢复 .env 初始值」删除 SQLite 覆盖并立即恢复本次启动时读到的环境配置；它不会重新读取磁盘上尚未重启加载的新 `.env`。刷新配置会覆盖未保存选择，因此存在编辑内容时会先提醒。

以下接口沿用全部管理接口的 `ADMIN_TOKEN` 鉴权，不回显任何凭证：

- `GET /admin/api/thinking`：返回 `config`、`source`、`environment`、`levels`。
- `PUT /admin/api/thinking`：请求体直接为九个配置键及其档位，不包一层 `config`。成功即持久化并热生效。
- `DELETE /admin/api/thinking`：恢复环境初始值。

`source` 为 `env` 或 `sqlite`，`levels` 为四个合法档位。配置读写不执行 Decisions 或 Interactions。沙盒预览继续显示**该次调用实际使用的** `thinking_level`；运行沙盒本身仍计入管理预算。

## 环境配置与身份约束

未设置的场景统一使用 `medium`。以下写法保留已有 Owner／摘要／日记变量名，其他场景使用新增变量：

```dotenv
HAKIMI_CHAT_MODEL=gemini-4-hydrogen
HAKIMI_FAST_CHAT_THINKING_LEVEL=medium
HAKIMI_OWNER_THINKING_LEVEL=medium
HAKIMI_SUMMARY_THINKING_LEVEL=medium
HAKIMI_DIARY_THINKING_LEVEL=medium
HAKIMI_MEMORY_COMPRESSION_THINKING_LEVEL=medium
HAKIMI_OWNER_DIGEST_THINKING_LEVEL=medium
HAKIMI_OWNER_COMPANION_THINKING_LEVEL=medium
HAKIMI_BILI_SELECTION_THINKING_LEVEL=medium
HAKIMI_BILI_LEARNING_THINKING_LEVEL=medium
```

场景选择来自服务器校验过的身份和业务路径。群中的 Owner 仍采用群聊场景；正文、图片、卡片、转发中的 `thinking_level=xhigh`、伪造 Owner 标签或被引用的 `/deep` 不会改变设置。只有真实 OneBot `OWNER_QQ_ID` 私聊可以使用直接 `/deep <问题或视频链接>`，本轮至少使用 `high`，若该场景已设 `xhigh` 则保持 `xhigh`，不持久修改下一轮档位；旧 Owner 列表与官方 QQ OpenID 不授予该权限。

每轮重新传入可信 `system_instruction`、`tools=[]` 和 `generation_config`，保持无自动续写、无工具调用、无思考摘要输出。调高场景档位不会触发读取其他会话或解开 Owner 私密信息边界。

## Token 上限与成本

`max_output_tokens` 同时包含思考和最终回答。思考强度不等于可见回复长度：公开闲聊依然遵循短句约束。高档位通常需要更多时间与 Token，但不能绕过本地预算预留、日／月上限、场景子预算或累计熔断；不保证一定提高某次回答质量或命中缓存。[Token 上限说明](https://aistudio.google.com/docs/thinking#token-limits)

后台调档不会自动增加单次 Token cap，也不会因为选了 `xhigh` 而偷偷降回 `high`。普通路径沿用原场景上限：

| 场景 | 默认单次思考＋输出 Token cap |
| --- | ---: |
| 群聊／普通好友、滚动压缩 | 1536，使用 `HAKIMI_CHAT_OUTPUT_TOKENS` |
| Owner 私聊、视频摘要、巡逻日记 | 4096，分别使用原场景输出上限变量 |
| Owner 每日汇报、B站学习心得 | 4096 |
| 日常陪伴 | 512 |
| B站候选挑选 | 1536 |
| 已授权的显式 `/deep` | 8192，使用 `HAKIMI_HIGH_OUTPUT_TOKENS` |

没有新增 `XHIGH_OUTPUT_TOKENS` 变量。在短输出预算下选择高思考档，可能更早耗尽 Token 而返回 incomplete；可以在后台改档位，不能靠模型自行续费重试。

如返回 incomplete，不发布残缺输出，也不自动续写或重做。Interactions usage 的输入、输出、缓存与思考 Token 按已有账本规则采集；缓存折后估算与保守扣账分别展示。日／月预算按 `BOT_TIMEZONE`，旧 UTC 迁移规则见 [0.8 管道说明](CORE_PIPELINE_08.md#预算时区与旧账本迁移)。

## 每日日记的模型调用

0.6.0 起，账本同时记录缓存命中与思考 Token，并分开展示缓存折扣后的实际估算和原有保守熔断金额。Hydrogen Standard 缓存输入单价默认 $0.075／百万 Token；日／自然月限额可在后台热保存。参见 [过滤与账本说明](FILTERING_AND_BUDGETS.md)。

白天每两小时仍同步确定性的脱敏记录；在配置时区（默认 Asia/Shanghai）23:55 收尾时，新增一次 Hydrogen 总结，补在日记末尾。重启会为保留期内尚未尝试的过去日期补做一次；空白日不调用模型。

输入只来自 `roaming_events` 和 `roaming_counts`：已筛查的公开 B 站摘要片段、获准群的活动类别计数、注入拦截次数。最多选最近 20 条公开摘要、每条 500 字符，再服从原上下文字节上限，明确提供省略条数。没有私聊、Session、群号、用户身份、聊天原话或图片。输入经过 Decisions，生成输出也通过原有输出门禁和凭证／联系方式检查。

所有相关调用记入 `patrol` 预算，继续受全局和后台主体额度约束。新增主库表：

```sql
CREATE TABLE IF NOT EXISTS roaming_summaries (
  day TEXT PRIMARY KEY,
  source_digest TEXT NOT NULL,
  payload BLOB,
  status TEXT NOT NULL CHECK(status IN ('attempted','done','failed')),
  created REAL NOT NULL
);
```

在首次付费调用前写入 attempted。成功后把日记总结加密保存为 done；失败标记 failed，崩溃／取消保留 attempted。Drive 重试和服务重启都复用状态，不重复生成同一天的总结。失败时保留模板明细并注明总结未完成。每日最多尝试一次，因此后到记录只更新确定性明细，不重新生成已完成的总结；日记附带快照时间。缓存和原日记数据一起按约 8 天保留期清理。

这套规则让 Drive 上传重试与模型计费分离。不会为弥补一次失败而自动增加费用，也不扩大原来单目录 Service Account 的权限。

## 文件变化与验证

- `generation_policy.py`：可信场景和档位／Token 上限映射。
- `config.py`、`.env.example`：统一模型、场景参数和统一计价。
- `gemini.py`：每轮通过 `generation_config.thinking_level` 分档，所有生成使用 Hydrogen。
- `service.py`：按已验证身份选择场景，新增 Owner 私聊单轮 `/deep`。
- `roaming_logger.py`、`app.py`：每日安全总结、持久化尝试和结果缓存。
- `security.py`：复用同一组服务凭证检查，覆盖日记输出。
- 测试：核对 SDK 实际请求的模型／版本／档位／价格，验证身份不可伪造、引用不可升档、日记预算失败和取消恢复不重复生成。

依赖继续使用 `google-genai>=3.2.0,<4`，已测锁定版本为 3.2.0。离线测试不调用付费 Gemini、不写真实 Drive；容器测试使用隔离网络和临时凭证。

早期 0.2.1 验证记录（非本次 0.8.1 测试总数）：**166 项测试通过**，Ruff 检查和格式检查通过，锁文件校验通过；`hakimi-surf:policy-test` 镜像构建及禁用外网、非 root、只读根文件系统的容器冒烟测试通过。容器完成反向 WS 命令回复、Webhook 验签和去重，模型费用为零。未使用真实 Gemini／QQ／Drive 凭证进行线上联调。

0.3.0 曾新增压缩 low 场景与独立 memory／admin 预算；0.8.1 压缩默认已改为 medium，仍包含在原全局预算内。管理台与快照详见 [对应说明](ADMIN_AND_MEMORY.md)。
