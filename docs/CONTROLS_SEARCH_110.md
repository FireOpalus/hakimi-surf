# v1.1：防抖热配置、受控联网与账目归属

## 管理台使用

沿用原 SSH 隧道，打开 `http://127.0.0.1:12141/admin`，登录后选择 **防抖与联网**。

- 防抖可分别设置普通群聊、私聊／明确 @／引用 Bot、两类未说完的短句，以及最长等待。单位秒，范围 0–30；实际等待受最长等待截断。保存会重新计算尚未开始的开放批次；不会打断在途生成、放宽六条批次上限或取消会话串行锁。普通群聊等待或最长等待为 0 时全场景关闭合并等待，这是原配置的兼容语义。
- 联网默认关闭。开启后可私聊发 `/搜索 今天的科技新闻`，或在获准群中明确叫到机器人再发 `/search ...`。只有这两个斜杠指令直接触发检索；其他直接提问由门卫 `need_external_lookup >= 0.65` 决定，群聊须明确提及机器人。B 站 BV／卡片继续使用原 B 站管道。
- 可设置每日最多检索请求和独立美元预算，零表示禁用相应额度；仍受个人、全局日／月／总预算约束。失败尝试保守计入每日请求上限，防止重试烧额度。
- 两张卡的设置均加密保存到 SQLite `admin_config`，立即生效，重启优先读取。**恢复 .env 初始值**删除该项后台覆盖；不会改动服务器 `.env`。
- 沙盒预览、定时巡逻和记忆归档不会自动联网。关闭搜索会阻止新的检索步骤；正在取得的结果若尚未通过开关与来源校验，就不会进入主对话。已经过审并用于聊天的事实不再抹除；主对话及消息送达仍受撤回、会话重置和准入校验约束。已提交给供应商的请求可能已计费。

## 新增环境项

```dotenv
HAKIMI_WEB_SEARCH_ENABLED=false
HAKIMI_WEB_SEARCH_DAILY_LIMIT=10
HAKIMI_WEB_SEARCH_DAILY_USD=0.50
```

防抖继续读取已有五项：

```dotenv
HAKIMI_GROUP_DEBOUNCE_SECONDS=2.0
HAKIMI_MENTION_DEBOUNCE_SECONDS=0.6
HAKIMI_ORDINARY_INCOMPLETE_SECONDS=3.0
HAKIMI_MENTION_INCOMPLETE_SECONDS=2.2
HAKIMI_GROUP_DEBOUNCE_MAX_SECONDS=5.0
```

## API 与隔离

普通聊天仍使用 A/B/C 前缀及无工具五维流式协议。需要联网时使用一个独立、非流式的 `gemini-4-hydrogen` Interactions 请求：显式 GA `v1`、`store=False`、`thinking_level=medium`、只允许 `google_search`。只传当前触发原消息的检索问题（最长 500 字）；不传会话历史、画像、编年史、小本本、图片或展开的转发。密钥与凭证先由 `check_memory_text()` 拦截，随后静默剥除已配置数字账号与普通 URL 再校验长度，普通版本号（如 `Python 3.14`）保留用于查询。

门卫仍为 `v1beta` / `gemini-decision-1`。原消息先筛查；搜索回复再按不可信来源筛查。只接受 Google Search 和文本输出步骤，不执行任何任意函数；后台提取 200 字以内事实，去除原始 URL，不解析搜索建议 HTML，也不输出 Grounding 重定向长链，有合格来源时仅以纯文本附注媒体标题或主域名（`参考来源：…`）。完成 `google_search_call` 且正文非空即可成为候选资料，不强制要求引用角标；资料仍须通过来源门卫。没有结果、超限、跳过查询或检索不可用时只向主脑传状态，不假装搜索成功。

检索事实进入当轮 C 区的 `web_search_findings`（`web_search_status="grounded"`，并将主对话路由档位托底至至少 `medium`），未开启、跳过查询或失败时写入 `web_search_status`（`disabled` / `skipped_query` / `unavailable`），不再由外层硬编码拒答短路。主对话结合人设和本会话历史生成五维回复，送达后正常入史；后续追问可以引用已说过的内容。画像仍只从用户自述提取，助手转述不作为用户履历。群聊公开互动可沿既有规则进入群到 Owner 单向桥与归档；Owner 私密背景不出现在检索请求或群聊中。

`store=False` 禁止本应用依赖供应商会话续链，并不代表搜索服务没有自己的保留期；Google 对 Grounding 另有数据处理规则。

## 费用与 `unknown`

截图中的历史 `unknown` 为旧版未保留模型／usage 明细的 Gemini 调用，不能推断为 Google Drive。管理台改显示 **历史未归属（Gemini）**，Token 显示“未记录”，旧金额不变。Google Drive 文件同步没有写入本账本。

新增 `ledger_attribution(id, model_lane)`，在费用预留的同一事务中写入已知模型。正常结算、异常、取消及重启后遗留预留均保留此归属；缺失 Token 仍按未知处理，不捏造用量。旧数据不按业务 lane 猜测模型、不改写历史金额。

联网请求的 Hydrogen 行同时包含 Token 和搜索工具费。解析 `usage.grounding_tool_count` 中 `google_search.count`，按 **$0.014／查询**保守估算；不假设共享项目免费额度仍有剩余。每次预留十次查询费及 16,000 个工具输入 Token 的空间：这不是供应商查询次数的硬上限。若实际花销超过预留，照实记账并触发已有全局熔断；用量未知、超时、取消不自动重试，保留全额预留。

`web_search_attempts(id, day, status, query_count, tool_amount, created)` 只记录尝试和费用观测，不存查询／网页正文，按 Bot 时区计日，保留 32 天。后台展示今日尝试数、已观测查询数及工具费用，方便与云端账单核对。缓存输入与思考 Token 沿用现有独立统计。

## 新接口与文件

- `GET / PUT / DELETE /admin/api/debounce`
- `GET / PUT / DELETE /admin/api/web-search`

全部沿用 `ADMIN_TOKEN`、同源校验、禁止 URL 查询参数和 `no-store` 响应；公网 Nginx 仍不代理管理台。

新增 `debounce_policy.py`、`web_search.py`、`tests/test_controls_search_v110.py`；修改接收防抖、运行时配置、Gemini 网关、消息业务管道、账本、内嵌 HTML、环境示例及容器冒烟脚本。无需新依赖、Node 构建或外部 CDN；保留 `google-genai >= 3.2.0`，锁定环境仍使用 3.2.0。

验证依据：Google 的 [Interactions 联网接口](https://aistudio.google.com/docs/google-search)、[计费说明](https://aistudio.google.com/docs/pricing)与[搜索结果使用规则](https://aistudio.google.com/docs/terms#grounding-with-google-search)，以及本项目实际安装 SDK 的类型与 MockTransport 契约测试。源码测试禁止访问真实外部 API，不消耗 Gemini 额度。


## v1.1.1 范围调整

五项防抖参数在后台及环境变量中统一支持 0–30 秒；默认值及已保存值均保持原样。超过 5 秒的等待需要同时调高“同批次最长等待”，实际时间受该上限截断。配置校验、热更新、批次合并和流式管道相关的 92 项测试通过，容器断网冒烟验收通过。
