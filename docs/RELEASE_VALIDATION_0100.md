# v0.10.0 发布验收记录

验收日期：2026-11-15，Asia/Shanghai。

- `uv run ruff check .`：通过。
- `uv run ruff format --check .`：全部 Python 文件格式检查通过。
- `uv run pytest -q`：**1198 passed**，273.53 秒。最后的静态协议措辞与入站预取时机调整另通过 47 项前缀／SDK测试、89 项消息管道／热撤权回归。没有删减原有安全契约。
- 新增流式 / 五维出站契约：32 项，通过。使用真实 google-genai 3.2.0 序列化、HTTP/SSE Mock 与本地协议桩；socket 夹具阻断外网，测试没有真实 Gemini 花费。
- Docker `hakimi-surf:0.10.0` 在 `--network none` 下完成非 root、反向 WS 登录／收发、准入隔离、引用回复、Webhook 鉴权去重、L0 过滤、表情增删预览、后台账本控制与 ffmpeg／yt-dlp 检查，模拟账本费用为零。
- 容器内 66 个 Python／HTML 文件与当前工作区 SHA256 一致；Pillow 长图所需文泉驿中文字库存在。
- 当前 v0.9 生产主库副本连续初始化两次，五张财务表内容哈希不变；两份物理隔离 Session 副本重复初始化，历史密文不变。新增表全部幂等创建，没有重建旧数据表。
- 内联 ES6 管理 JS 通过 `node --check`；没有增加 Node/npm 部署依赖。

生产结果：

- 首尔后端版本 0.10.0，容器 healthy，UID/GID 10001，根文件系统只读、内存 768 MiB，绑定 `127.0.0.1:12141`。
- 上海反向 WSS 自动重连成功，来源与最近心跳验证通过；没有更改上海 NapCat 或重新扫码。
- 旧首尔 NapCat 继续 exited / restart=no。
- 管理 API 未鉴权返回 401；QQ 子域名公网 `/`、`/admin`、`/docs` 均为 404；未授权 WS 握手 403，合法连接不受影响。
- 升级前后思考配置逐项相同：两个聊天场景 auto，七个后台场景 medium，Carbon 开关启用。时区 Asia/Shanghai；小本本继续关闭。Google Drive OAuth 配置及挂载保留。
- `.env` 补入新防抖参数默认值，保留原凭证与既有配置；没有把 `.env`、数据或备份放进交付包。

升级前镜像标记为 `hakimi-surf:pre-0.10.0`，环境文件与三份数据库已保存在服务器专用备份目录。回退应用时使用这个镜像标签，并保留原 Compose OAuth 挂载；恢复数据库前应先停止后端并保全升级后的数据。

本次没有主动发送真实 QQ 测试消息、调用真实付费模型或额外触发 Drive 同步。线上实际语气、多模态识别效果与费用，仍以正常使用时的模型结果和 usage 为准。实施边界、图片同内容不同 URL 的去重限制以及 ACK 恢复语义见 [完整说明](STREAMING_010.md)。
