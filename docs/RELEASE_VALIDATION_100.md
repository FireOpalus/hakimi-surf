# 1.0.0 发布验收

日期：2026-11-15，基线为生产 0.10.0。

- `uv sync --frozen`：通过，google-genai 保持 3.2.0，无新增重型依赖。
- `uv run ruff check .`、`uv run ruff format --check .`、`git diff --check`：通过。
- `uv run pytest -q`：**1224 passed**，267.11 秒。测试夹具拒绝外网连接；模型和 Drive 均使用 MockTransport，不产生真实 API 费用。
- 内联原生 JavaScript 语法检查与 CSP 自动散列契约：通过。
- 生产数据库副本连续初始化两次：5 张财务表逐表哈希不变；2 个隔离会话库的 session_meta、recent_messages、rolling_state、outgoing_receipts、received_messages 均保留原内容。
- 旧 0.8.1 表形状夹具双重迁移通过；WAL、secure_delete、0600、加密正文、跨群账号隔离、不可变冷史及 reset/归档竞争有契约覆盖。
- 真实 SDK + MockTransport 验证 v1 Flex、store=False、tools=[]、固定 medium、半价模型账本；入站 Decisions 仍 v1beta。半价通道容量失败不触发聊天熔断。
- Drive Mock 验证父目录与 appProperties 双重限制、缺失／改变 ETag、超限下载、反向编辑与删除、人工锁和私聊禁止导出。
- `scripts/smoke_container.py hakimi-surf:1.0.0`：通过。使用 `--network none`、只读根、UID 10001、假凭证，验证 WS、0 Token 指令、Bot 过滤、管理台双轨检索／擦除／解锁／CAS、表情包和有效缓存字段；本地账本费用为零。
- 发布镜像中全部 **68** 个后端 Python/HTML 文件哈希与工作区一致。

交付只包含显式白名单源码、测试、示例配置、文档及依赖锁；不含 `.env`、用户数据卷或 OAuth／Service Account 凭证。增量 diff 以随包保留的 0.10.0 源码为基准，不把更早未提交的升级重复算入此次变更。

实际模型 Flex 调用及新群档案的 Google Drive 推拉没有付费实测；服务器未启用任何新增群镜像，需管理员显式逐群打开。现有日记同步与 Owner 小本本设置保持原状。

生产验收：首尔后端已运行 1.0.0，健康检查正常；上海来源反向 WS 在线且最近心跳有效。管理 API 无 Token 返回 401，公网 QQ 子域名的 `/`、`/admin`、`/docs` 返回 404，错误 WS Token 返回 403且不挤掉合法连接。原思考／预算／准入／压缩配置逐项与升级前快照一致，小本本关闭，新增群档案镜像无默认放行群。只读根、10001:10001、768MiB 和回环 12141 保持；验收时内存约 98MiB。旧首尔 NapCat 保持 exited、restart=no。

已保留 3 份加密数据库备份及旧 `.env`，回滚镜像为 `hakimi-surf:pre-1.0.0`。真实凭证与备份位于服务器受限目录，不随交付包导出。
