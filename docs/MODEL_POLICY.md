# 0.10.0 模型与思考强度

0.10 聊天开启流式与恒定五维 JSON；取消保留预留，仅超时 / incomplete 最多重新预留并降一档重试一次，详见 [流式出站说明](STREAMING_010.md)。后台摘要不改变协议。

聊天路由、缓存、Carbon 价格和迁移说明见 [0.9 实现说明](CONTEXT_ROUTING_09.md)。`fast_chat` 和 `owner_private` 默认 auto；其他七个生成场景默认 medium。后台仍可把聊天手动设为 low / medium / high / xhigh，配置持久化热生效。现有 SQLite 手动值保持不变；需要自动路由时将前两项改为 auto。

所有管理读写继续受 ADMIN_TOKEN 鉴权；GET/PUT/DELETE `/admin/api/thinking` 返回配置、来源、环境初值、手动 levels 与聊天 chat_levels。修改不调用模型。沙盒会真实计费并显示实际使用的模型与档位，不写生产 Session。Owner 私聊的 `/deep` 原授权不变；自动路由不能授予权限或绕过沙箱。

后台单次任务不注入长聊天宪章，仍用 Hydrogen 与原场景输出上限。下列日记与安全说明继续适用。

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
- `gemini.py`：每轮通过 `generation_config.thinking_level` 分档，后台生成使用 Hydrogen，聊天沿用 v0.9 的 Hydrogen / Carbon 路由。
- `service.py`：按已验证身份选择场景，新增 Owner 私聊单轮 `/deep`。
- `roaming_logger.py`、`app.py`：每日安全总结、持久化尝试和结果缓存。
- `security.py`：复用同一组服务凭证检查，覆盖日记输出。
- 测试：核对 SDK 实际请求的模型／版本／档位／价格，验证身份不可伪造、引用不可升档、日记预算失败和取消恢复不重复生成。

依赖继续使用 `google-genai>=3.2.0,<4`，已测锁定版本为 3.2.0。离线测试不调用付费 Gemini、不写真实 Drive；容器测试使用隔离网络和临时凭证。

早期 0.2.1 验证记录（非本次 0.8.1 测试总数）：**166 项测试通过**，Ruff 检查和格式检查通过，锁文件校验通过；`hakimi-surf:policy-test` 镜像构建及禁用外网、非 root、只读根文件系统的容器冒烟测试通过。容器完成反向 WS 命令回复、Webhook 验签和去重，模型费用为零。未使用真实 Gemini／QQ／Drive 凭证进行线上联调。

0.3.0 曾新增压缩 low 场景与独立 memory／admin 预算；0.8.1 压缩默认已改为 medium，仍包含在原全局预算内。管理台与快照详见 [对应说明](ADMIN_AND_MEMORY.md)。


v1.1 联网补充：普通对话仍不传工具。后台显式开启的独立公开检索请求使用 GA v1 Hydrogen、`store=False`、medium 和唯一 `google_search` 工具；原消息及搜索来源均经过 Decisions，不传任何历史／私密快照。见 [联网与预算边界](CONTROLS_SEARCH_110.md)。

v1.1.4 续链补充：仅非沙箱、启用记忆的 `fast_chat` / `owner_private` 会话开启 `store=True` 与经本地加密 `interaction_chains` 托管的 `previous_interaction_id`（发生撤回、清空、折叠、过期或送达失败立即失效重链）；非聊天后台任务、记忆压缩、日记、巡逻与沙盒仍严格保持 `store=False`。
