# B 站小号文本巡逻

0.5.0 在以下默认文本路径上增加了独立、默认关闭的 [受限原片分析](VIDEO_WATCH.md)。开启后，无可用字幕或明确要求视听细节时可以下载低清原片；仍不发送虚假播放心跳。字幕上限与最新完整性判断以该升级说明为准。

模块 `bili_watcher.py` 独立于原有匿名 UP 巡逻，默认关闭。只使用独立小号的凭证；Cookie 不进入模型、日志或管理接口响应。账号内容只通过固定 B 站接口读取，不向第三方发送 Cookie。

## 配置与后台

```dotenv
BILI_AUTONOMOUS_ENABLED=false
BILI_PATROL_TIMES=["13:00","20:30"]
BILI_SESSDATA=
BILI_BILI_JCT=
BILI_BUVID3=
BILI_PROXY=
```

后台「B站小号巡逻配置」可保存凭证、测试 nav 登录状态、启停巡逻、设置每天 1～2 个时间点，及分别启用点赞／收藏。凭证输入只填对应 Cookie 的值，不填完整请求头；留空保留旧值，明确清空才删除。GET 只返回是否已配置和遮盖后的账号编号，不回显 Cookie。SQLite 使用现有 Fernet 和上下文绑定加密。

收藏须指定一个当前小号拥有的收藏夹 ID。程序先请求该账号的收藏夹列表验证归属；不自动选择不明目录。默认不点赞、不收藏；启用后两种操作合计每天最多 1 次，可改为 2 次。重复点赞、已经提交但结果不确定的操作不会自动重试。

后台保存后热生效，重启优先使用 SQLite。凭证轮换会使正在处理的旧轮次失效；轮换不会清除 12 小时风控冷却。手动执行和定时执行共享每日轮数上限，失败或崩溃也消耗本轮尝试，避免不断重复付费。时间使用 `BOT_TIMEZONE`，启动时不补跑错过超过 5 分钟的时间点。

## 每轮做什么

1. 检查本地预算未硬熔断、开关开启、凭证存在且不在冷却期。
2. nav 验证登录、取得当天 WBI key；只计算签名，不访问 key 图片 URL。
3. 取至多 10 个首页推荐与 5 个关注动态视频，不翻页；只使用 BV、标题、UP 名、可用分区，不逐条额外抓取标签。已尝试视频按 BV 去重。
4. Decisions 检查候选后，Hydrogen `low` 选择至多 2 个候选。输出严格校验为候选集合内的 BV 列表；模型不能选择 URL 或执行任意操作。当前安装的 Hydrogen 不提供 `minimal`，因此使用 `low`。
5. 对选中视频读取简介、最多一份 CC／AI 字幕的有限节选、最多 3 条热评。字幕缺失就明确标注，不补造；不下载视频流或付费片段。字幕仅接受固定 HTTPS CDN 和 `/bfs/` 路径，禁止重定向，且不发送 Cookie。
6. 对相同已裁剪素材再次筛查，然后 Hydrogen `low` 生成文本学习心得，存入公开巡逻日记和 Owner 的巡逻报告来源。私人 Session 和《小本本》不参与。
7. 若模型建议互动且管理开关授权，完成本地每日限额、CSRF、收藏夹归属等检查后，最多执行指定点赞／收藏。没有评论、弹幕、B 站私信、关注、投币等写入口。

每次 HTTP 请求串行执行，相邻请求保持 2～6 秒随机间隔。读取正文有字节上限，字幕和评论有字符上限。遇到 `-101`、`-352`、`-412`、`v_voucher`，或 HTTP 401／403／412／429，立即停止本轮并持久化 12 小时冷却，通过现有 Owner 通路发本地模板告警；不求解验证码，不轮换身份或代理重试。

**本模式只阅读文本，因此不生成播放心跳、不上报虚构播放时长。** `heartbeat_supported=false` 明确向后台说明此边界。摘要也不得声称观看过全片。原有匿名 UP 巡逻仍只摘要公开视频标题／简介。

## 存储与接口

- `bili_credentials`：单一加密凭证记录。
- `bili_account_state`：加密账号身份、nav 检查时间、固定错误类别和冷却截止。
- `bili_patrol_runs`：每日时间槽与至多两次尝试，重启不重复模型调用。
- `bili_watched_history`：BV 去重、尝试／完成状态与加密文本心得。长期保留 BV 去重；原始字幕和评论不落盘。
- `bili_actions`：每日点赞／收藏名额、确认／不确定状态，BV 和动作联合去重。

管理端点全部位于受原有鉴权与同源保护的 `/admin/api/bili-account`：GET／PUT／DELETE 管理策略，POST `/credentials` 更新凭证，POST `/test` 检查登录，POST `/run` 手动执行一轮。不存在任意 URL 请求或任意 B 站动作 API。

## 核对来源与验证范围

固定端点和参数参考项目维护者公开代码／文档：[NextPili 视频接口](https://github.com/rownix101/NextPili/blob/main/docs/api/endpoints/video.md)、[WBI 算法与测试向量](https://github.com/Chilfish/bilibili-API/blob/master/docs/misc/sign/wbi.md)、[关注动态](https://github.com/Chilfish/bilibili-API/blob/master/docs/dynamic/all.md)、[播放器字幕](https://github.com/Chilfish/bilibili-API/blob/master/docs/video/player.md)、[评论分页](https://github.com/Chilfish/bilibili-API/blob/master/docs/comment/list.md)、[点赞与收藏参数](https://github.com/gotvapp/bilibili_api/blob/main/bilibili_api/data/api/video.json)。采用本项目内的有限接口实现，不引入包含大量写操作的第三方 SDK。

单元测试使用 HTTP Mock，覆盖 WBI 固定向量、凭证密文与脱敏、0 模型预算门禁、输入筛查、轮次去重、字幕域名与 Cookie 隔离、风险退避、热更新并发、CSRF 和每日互动上限。没有使用真实 B 站 Cookie 做账号联调；真实账号测试需先由 Owner 在后台填入独立小号凭证，保持写操作关闭后点击 nav 测试。
