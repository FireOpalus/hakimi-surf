# v1.0.0 双轨记忆与治理

在 0.10.0 基线上增量升级，保持真实身份鉴权、单会话串行生成、五维流式输出、ACK 入史、外部 Bot 两轮上限、只读 UID 10001 容器与回环 12141。原三段人设正文不变；新增静态双轨边界放在 A，召回数据仅在 C。

## 数据与迁移

每个 `data/sessions/{group,private}/<HMAC>.sqlite3` 升级 WAL、FULL synchronous、secure_delete，文件 0600。既有表和密文行不改写。新增：

| 表 | 键与用途 |
| --- | --- |
| member_profiles | `(member_id,slot)`；alias/preferences/boundaries/state，Fernet 正文、过期时间、人工锁、删除墓碑 |
| chronicle_events | 稳定主题 HMAC ID；Fernet 事件正文及向量，热度、时间、人工锁、来源分片 |
| cold_history_slices | 不变源片段、epoch、序列范围、来源类型；禁止 UPDATE，只有显式重置允许清除 |
| memory_slice_jobs | 分片状态、付费前持久化 attempt 与 `flex-…` 本地批次 ID（不是 Google 远程 Batch ID） |
| memory_covered / memory_message_times | 入队去重水位、独立消息时钟；不改 B 载荷 |
| memory_control | 双轨就绪标记、凌晨扫描日、群镜像开关及加密的同步基线 |

读取旧滚动摘要作为兜底，首次成功提交双轨结果后停用旧摘要写入；原列保留。无可靠事实时提炼可返回空数组，不凭旧摘要猜测画像。旧版已尝试的折半任务标为终态，不自动重计费。

重置先在主库提交公开互动墓碑、取消待发任务，再在单个会话事务中清空热史、接收日志、回执与双轨／冷史，递增 epoch/version。同时关闭该群镜像，避免重新导入旧云端档案。云端已有文件不会自动删除；重新绑定前需人工处理该旧文件。

## 调度、价格和准确性边界

达到 28,000 本地估算 Token 或 96KB 时按约半数 Token 折半，原片段与删除热史同事务。在线仅写库、唤醒后台。默认每天 03:10（BOT_TIMEZONE）复制超过 24 小时且未入队的记录；快照和归档不裁切 B。正常 TTL 清理也先将到期热史入冷史，避免 24 小时过期早于凌晨扫描。

后台串行执行 `check_memory_text` → Decisions `purpose=source` → Interactions `service_tier=flex, thinking_level=medium, store=False, tools=[]` → 输出筛查 → epoch/version/人工锁核验。Flex 有独立执行槽，超时不打开聊天熔断；不降级标准价、不自动重试不明确付费结果。`/admin` 立即压缩才允许 `force=True` 同步等待，默认最长 900 秒。`memory` 日预算沿用 $0.20，包含门卫费用。

默认 Hydrogen Flex 价格为每百万输入 $0.75、缓存输入 $0.0375、输出（含思考）$3.75，来自 [Google Flex 文档](https://aistudio.google.com/docs/flex-inference) 和 [官方 Hydrogen 价格](https://aistudio.google.com/docs/pricing#gemini-4-hydrogen)，核对日期 2026-11-15。程序按配置标准价的一半估算，保留安全系数与完整预留的熔断口径，另列观测缓存折后估算；不是云端账单余额。Flex 为尽力可用的预览通道，可能需要数分钟。

画像必须包含数字账号本人发言的 seq 和匹配原话；他人贴标签、助手猜测不能写成画像。识别到玩梗标志的自述也不进事实槽。短期状态 1–14 天自动到期。事件按主题合并，人工修订受保护；删除正文后保留加密匹配向量及来源序列以抑制同一事件换个措辞复活。自然语言归因并不保证完美，仍需管理台人工纠偏。

本地检索使用词／汉字二元组与模型整理的短主题标签，构造 **192 维 feature-hashing 浮点向量**，不是神经语义 embedding，不调用额外模型。关键词与向量排名融合，适度加权近期／高热条目。索引含 Python 对象开销按 24 MiB 限额逐出，单会话每轨最多 512 条；不落明文索引、不跨库查询。纯同义词且没有共同标签时召回能力有限。

网关放行后读取当前账号画像；仅 `need_memory_recall` 才检索编年史，沙箱不读取。所有背景做身份标签转义，插在 C 末尾，超预算先裁撤可选记忆；C 默认 1,800 本地估算 Token，硬守 2,050 尾窗。A/B 不随检索变化。

## Drive 镜像

复用已配置 `drive.file` 单目录，显式 SA（Shared Drive）或个人 OAuth 文件夹标记。只处理显式开启同步且仍在白名单的群。初始开关来自 `HAKIMI_ROAMING_GROUP_IDS`，后台逐群覆盖持久化；私聊永不导出，小本本独立且仍默认关闭。

目录内文件命名 `group-memory-<scope前24位>.md`，查询必须同时包含 parent、文件名、专用 appProperties；下载与更新均再次验证身份、父目录及 ETag。缺 ETag／冲突直接停止，不无条件覆盖。文件上限 256KB；定时每小时检查人工修改，每日凌晨归档完成后尝试增量推送，未变不上传。

Markdown 条目使用稳定 `<!-- entry:... -->` 锚点。可以改条目正文，或删除整个条目块；保留文件头、其余锚点与结构。未知条目、重复锚点、损坏结构均拒绝。新增条目请通过正常对话提炼，不接受云端任意造账号／事件。人工更改先做凭证检查和 admin 预算 source 筛查，再按基线三方核对与 CAS 更新／删条、置人工锁。删除整个文件为冲突，不会抹掉全部本地数据或自动重建。修改与本地新修订撞车时，后台显示 `memory_mirror_local_conflict`，先人工核对再处理。

筛查摘要在付费前持久化；未确认成功的同一云端修改不重筛收费。请修改云端内容后再拉取。已通过筛查的导出重试只做 Drive I/O，不重复跑提炼或筛查模型。网络超时创建结果不明确时不会认领陌生文件，状态显示 `memory_mirror_unbound`，需人工核对。

## 后台与验收

`/admin` → 群聊与记忆 → 查看与管理：旧摘要兜底、双轨检索、编辑、擦除、解锁、立即提炼、重置；群会话另有镜像开关和推拉按钮。搜索通过 POST 正文，保持管理端禁止 URL 查询参数的原鉴权规则。所有动态内容仍用 textContent，CSP 由内联内容自动计算。

账本保留原缓存命中率，显示扣除 2050 动态窗后的有效覆盖率及前缀击穿告警；`memory_flex` 与 Hydrogen／Carbon／Decisions 分列。双轨后台调用不计入在线前缀稳定命中样本。90% 是稳态观察线而非保证值。

迁移和测试不读取用户原始私聊到输出，不调用真实模型或 Drive。版本包通过 `scripts/package_delivery.py` 显式白名单生成，排除 `.env`、OAuth/SA 凭证与数据卷。部署前备份加密 DB 与 `.env`，只重建境外 hakimi-bot；境内 NapCat、主域名 DSH 和 Nginx 不动。
