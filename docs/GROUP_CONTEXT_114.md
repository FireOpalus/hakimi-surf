# v1.1.4：群聊上下文与差量续链

本版整理群聊历史中的重复身份前缀，在相邻消息间隔达到 30 分钟时增加按 `BOT_TIMEZONE` 格式化的时间断点。当前批次含多图时仍补齐邻近群聊文本，扩展批处理、转发解包与上下文容量；身份鉴权、预算预留和群／私聊隔离沿用原机制。

## 主对话续链

启用本地记忆的非沙盒主对话默认使用 `store=True`，并按模型通道携带 `previous_interaction_id`，只传尚未覆盖的历史条目。每轮继续传入静态系统指令；预算按完整上下文保守预留，按返回用量结算。搜索、沙盒、日记、巡逻、压缩及其他后台任务保持 `store=False`。

每个隔离会话 SQLite 自动新增 `interaction_chains(lane TEXT PRIMARY KEY, payload BLOB NOT NULL, expires REAL NOT NULL)`。续链标识与校验信息经现有 Fernet 加密保存；仅在回复确认送达后提交。撤回、清空、历史裁剪、过期、前缀变化或送达失败会使旧链失效。旧会话和账本无需导出重建。

新增环境变量：

- `HAKIMI_INTERACTION_STORE_ENABLED=true`：允许上述主对话云端保存与续链；设为 `false` 可继续全量本地上下文方式。
- `HAKIMI_INTERACTION_CHAIN_TTL_SECONDS=21600`：本地续链有效期，另受会话记忆有效期限制。该值不是云端数据删除期限。

上下文默认上限随本版扩容，已有 `.env` 中显式填写的值继续优先生效。`/admin` 保存的防抖、思考强度、预算、准入与联网开关不覆盖。

## 验证与部署

新增 `tests/test_group_context_chain_v114.py` 覆盖历史格式、多图上下文、扩容、预算预留、续链与失效路径。本版全量离线测试 **1319 项通过**；Ruff、前端 JavaScript 语法、依赖锁检查及容器断网冒烟均通过。生产数据库副本连续初始化两次后，5 张账本表和 3 个隔离会话的既有数据保持一致，新增续链表与数据库完整性检查通过。验证未调用真实 Gemini API。

部署前备份 `.env`、主 SQLite 及各会话 SQLite，保留旧镜像。沿用 `docker-compose.yml` 与 `compose.roaming-oauth.yaml`，仅替换首尔后端；上海 NapCat 保持运行并自动恢复反向 WSS。后端仅绑定 `127.0.0.1:12141`，管理台仍通过 SSH 隧道访问。
