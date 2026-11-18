# 0.5.0 字幕优先与受限原片分析

在现有 0.4.0 上增量实现。群／私聊的 B 站 BV、链接、JSON 卡片（同条消息含多个视频时自动取首个合法 BV，不报错拦截），以及独立小号首页巡逻选中的视频，共用 `VideoWatch`。旧 UP 订阅的批量更新仍维持低成本标题／简介摘要，不自动对整批视频下载原片。

## 配置与使用

```dotenv
ENABLE_VIDEO_FILE_WATCH=false
MAX_VIDEO_WATCH_SECONDS=900
DAILY_VIDEO_WATCH_LIMIT=3
```

通过原 SSH 隧道进入 `http://127.0.0.1:12141/admin`，在「B站小号」下的「双轨看片」卡片修改并保存；立即生效，重启优先恢复 SQLite，支持恢复 `.env`。原片开关独立于小号自主巡逻开关：可以关闭自主逛首页，同时按需分析 QQ 链接。原片时长可设 30～900 秒（15 分钟），每日次数可以选择 1～20 次、0 次（暂停）或「不限次数」。在 .env 中使用 `DAILY_VIDEO_WATCH_LIMIT=unlimited` 选择不限。次数按 `BOT_TIMEZONE` 计算，默认上海时区，所有用户／群／巡逻合用一份额度。

填写小号 `SESSDATA` 时，字幕读取和下载会使用该凭证；`BILI_PROXY` 同时用于小号 HTTP 与 yt-dlp。凭证可在既有面板热更新，无需写到聊天。没配置 Cookie 也可尝试公开匿名资源，但不保证平台可用。仅匿名 nav 的正常「未登录但提供公开 WBI key」响应不算凭证过期；带凭证的 -101 和任何验证码／-352／-412 始终停止。

例如发送 `看看这个视频 BV...`：优先读字幕。发送 `请分析这个视频的画面细节 BV...`、`听听这个视频的声音 BV...` 或 `/watch-video BV...`：在已放行身份和预算允许时尝试原片。明确视听要求只从顶层用户文字判断，卡片标题、合并转发或字幕里的要求不能强制下载。普通好友仍是公开人设，Owner 专属人设只用于真实 Owner 私聊。

## 巡逻选片与分享

世界引擎安排的 B 站巡逻与后台手动巡逻都在开始前检查巡逻子预算 `patrol`。QQ 主动发言和 Owner 日报的生成使用 `proactive`。直接在聊天中请求解析视频使用聊天账本。账户熔断始终生效。「世界与主动引擎」中的「B 站自主巡逻不限次数」控制自主出门巡逻的轮数。「B站小号」的每日原片次数控制每轮选中或聊天点播的视频文件分析。切回有限档时，当天已有记录仍计入额度。

同一次选片可以参考获准会话里的兴趣。因某个私聊话题挑中的视频，接下来的 24 小时优先进入该会话的推荐素材。Owner 可以通过已有的里程碑私信收到推荐，普通好友在下次私聊时可收到推荐。公开巡逻记录和按 BV 号点播照常可用，优先期过后视频也可作为普通推荐。服务器保存会话的带密钥摘要，选片模型只需引用本轮的话题编号。

## 两条路径

**A：文字资料。** 获取简介、CC／AI 字幕和至多三条热评。字幕最多 1000 行／12000 字，并检测时间戳及首尾覆盖；缺失、裁剪、时间戳不完整都不会标成完整字幕。现有模型上下文上限可能继续裁剪，裁剪后的同一份材料先筛查再生成。材料边界通过内部 `material_scope`（`full_subtitle` / `partial_subtitle` / `metadata_only`）传入模型以自然口吻总结，不为截断文本盲目追加原片费用，也不在用户回复外层拼接免责横幅或重复贴回原链接（后台 UP 更新推送与沙盒预览保留新视频链接，不含公文抬头）。评论只代表评论者观点。

**B：原片。** 无可用字幕或明确视听要求时，先检查独立开关、本地账本、每日次数和单任务锁。已知超时长的视频立即退回文字；未知时长必须由 yt-dlp 元数据证实后才下载。预检后仅选择最小体积的 360p／480p MP4，保留可用音轨；原本无音轨的视频也允许单视频流。不能满足格式、权限、时长或体积限制时降级。

下载在隔离子进程执行：固定 BV URL、无 shell、无插件、无任意命令／路径、无视频合集展开、无付费／DRM／试看绕过、无自动重试。只允许 GET／HEAD、固定 B 站及视频 CDN HTTPS 域名，并拒绝解析到私有地址（显式部署代理除外）。请求之间保持 2～6 秒间隔；Cookie 用域限定、权限 0600 的临时 Netscape 文件，不能传给视频 CDN，也不会出现在参数、模型输入或日志。

单个最终文件最多 80 MiB，包含临时音视频轨的任务目录最多 192 MiB，下载最多 180 秒；父进程监控大小、超时和权限变更，必要时终止整个进程组。ffprobe 再核实实际时长、MP4 格式、清晰度和流数量。ffmpeg 仅合并、不转码，线程限制为 1，不接受网络协议输入。

## Gemini 调用与费用

继续使用 `google-genai >= 3.2.0`，不调用旧 `generateContent`：

1. 独立 Files `v1beta` 异步上传，最多等待 120 秒处理，识别 ACTIVE／FAILED 状态，不无限轮询。
2. `gemini-decision-1`／`v1beta` 对文字和视频一起筛查，视频内口述／字幕／画面指令都是不可信数据。
3. `gemini-4-hydrogen`／Interactions `v1`，`store=False`、`tools=[]`，输入使用 text + video URI／mime_type。普通视频摘要 medium，原有合法 `/deep` 请求 high。默认低分辨率、1fps 采样，不能保证读清微小文字或捕捉瞬时动作；模型区分正片画面与评论玩梗，未见细节绝不脑补，仅在被专门追问缺失画面或原话时顺口说明，平时不主动背诵采样率免责套话。
4. 生成文本仍经过输出 Decisions。全程复用账本，保守预留视频及音频输入 token，随后按 `interaction.usage.total_input_tokens`、输出和思考 token 结算。原有用户、每日、巡逻和总预算继续有效，可能先于每日次数拒绝任务；不会放宽预算以强行看片。

每次开始原片尝试先在 SQLite 占一个名额，失败、取消、进程崩溃也不退回，防止重复重试烧额度。可用字幕的文本路径不消耗原片次数。协议通过的单个视频任务可延长到最多 15 分钟；普通消息的过期窗口不变。

## 清理、降级与风控

本地下载上下文和远端 Files 上下文均使用 `try...finally`。成功、异常、视频安全拒绝、超时和取消都会清理对应本地目录并请求删除对应远端文件。远端文件名在上传前先生成并密文登记，因此上传中断也有记录可追踪；删除失败保留待清理记录，启动和定时维护按有限批次重试，不执行 `files.list`，不删除其他应用文件。积压达到三项时暂停新原片上传。

yt-dlp 遇到凭证过期、-352／-412、HTTP 403／412／429 或验证码立即停止，调用既有 12 小时冷却和 Owner 0 Token 告警。聊天请求在下载、上传失败或风控降级后，使用本次已过审的文字材料回落主模型生成自然口语摘要（不再发起新的 B 站网络请求，也不直接倾倒原始标题／简介字段）；小号后台巡逻遇到风控保留简短标题记录后立即结束本轮，不继续下一视频或点赞／收藏。安全筛查拒绝、权限撤销和预算熔断不会被当作下载失败来绕过。

## 文件与部署

新增 `video_watch.py`（路径选择／持久次数）、`video_policy.py`、`video_downloader.py` 与 `video_download_worker.py`、`video_files.py`。扩展 `bili_watcher.py`、`gemini.py`、Service／任务过期机制、配置、后台路由和内嵌 HTML。新增表 `video_watch_attempts`、`gemini_video_files`；策略保存在加密 `admin_config['video_watch']`。

Dockerfile 安装 ffmpeg／ffprobe，依赖锁定当前稳定版 `yt-dlp==2026.10.26`。Compose 增加 `/tmp/bili_video_cache` 专用 256 MiB tmpfs（UID 10001、0700、noexec），重启不会保留缓存，后端内存上限调整为 768 MiB。保持回环 12141、非 root、只读根目录及原有 Drive 挂载。

当前 NapCat + Drive OAuth 部署使用：

```bash
docker compose -f docker-compose.yml -f compose.napcat.yml -f compose.roaming-oauth.yaml build hakimi-bot
docker compose -f docker-compose.yml -f compose.napcat.yml -f compose.roaming-oauth.yaml up -d --no-deps hakimi-bot
```

验证使用 SDK MockTransport、假下载进程与无网络容器中的本地合成视频，覆盖真实 yt-dlp NativeHttpFD／FFmpegMerger／ffprobe、无声片、Cookie 域隔离、取消与删除重试、热撤销、SQLite 配额与上海跨日。未使用真实 B 站账号下载或调用付费视频模型；开启后实际可用性仍受 B 站资源格式、地区和账号权限影响。

本次最终 **708 项测试全部通过**，Ruff 和 Chromium 后台交互验证通过。2026-11-14 生产部署已核验 0.5.0 镜像源码摘要、WS 在线、未鉴权接口 401、原片默认关闭／480 秒／3 次、缓存为空且为 256 MiB tmpfs、后端内存限制 768 MiB。既有名单、小号权限和凭证状态、主动私信、个签配置均逐项保持；Drive OAuth 只读挂载和已有同步状态正常，小本本继续关闭，NapCat 未重建。

升级前备份：`/root/.local/share/hakimi-backups/20261114-150542-pre-0.5.0`，仅 root 可读。回退镜像 `hakimi-surf-hakimi-bot:pre-video-0.4.0` 由已校验的 0.4.0 源码交付包重建。备份和真实凭证均不进入源码交付包。

参考：[Google 视频理解与 Files 示例](https://aistudio.google.com/docs/video-understanding)、[Files API](https://aistudio.google.com/docs/files)、[Interactions](https://aistudio.google.com/docs/interactions-overview)、[yt-dlp 官方版本](https://github.com/yt-dlp/yt-dlp/releases/tag/2026.10.26)。
