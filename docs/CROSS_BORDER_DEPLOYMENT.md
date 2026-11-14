# 上海 NapCat / 首尔主脑的分离部署

首尔继续使用 `12141`；需求片段中的 `8080` 是旧示例，不再另开端口。`docker-compose.yml`（以及同内容的 `compose.yaml`）只运行 FastAPI，上海使用独立 `docker-compose.napcat.yml`。两端环境均设置 `TZ=Asia/Shanghai`，后端额外固定 `BOT_TIMEZONE=Asia/Shanghai`。定时器与账本日／月边界都由应用 `BOT_TIMEZONE` 计算；0.8 在首次打开数据库时迁移旧 UTC 聚合并保留保守占用，详见 [账本时区迁移](CORE_PIPELINE_08.md#预算时区与旧账本迁移)。仅设置容器 TZ 不会替代这一迁移。

## 新增文件与变量

- 首尔：`docker-compose.yml`、`.env.example` 中的跨境段、`deploy/hakimi-ws.conf`。
- 上海：`docker-compose.napcat.yml`、`.env.napcat.example`、`deploy/napcat-onebot11.json.example`。
- `ONEBOT_ACCESS_TOKEN`：至少 32 字符，用 `python -c 'import secrets; print(secrets.token_urlsafe(32))'` 在服务器本地生成。兼容现有 `HAKIMI_ONEBOT_WS_TOKEN`，新变量存在时优先；不要把密钥贴进聊天或 Git。
- `ONEBOT_WS_PATH`：默认 `/ws/onebot` 兼容本地旧部署。跨境部署改成 `/ws/onebot/` 加至少 24 位随机后缀；建议用 `secrets.token_hex(24)`。Nginx 精确 location、FastAPI 变量与上海 wss URL 必须完全一致。
- `ONEBOT_TRUSTED_PROXY_IPS`：仅允许列出的直接代理提供 `X-Real-IP`，默认回环地址。如果宿主机 Nginx 经 Docker 端口映射访问后端，请检查项目网络的网关 IP 后加入这一具体地址，不要信任所有私网地址。
- `HAKIMI_WS_MAX_FRAME_BYTES=16777216`；Ping 25 秒、超时 30 秒。Uvicorn 同样明确设置 16 MiB 及有限接收队列，未启用 WebSocket 压缩。

`.env.napcat` 只放上海的 WebUI 口令、连接 URL、UID/GID 等协议端配置。不要复制首尔 `.env`，Gemini Key、加密密钥、Google Drive OAuth 文件留在首尔。

## NapCat 实际字段

反向 WS 配置写在 `network.websocketClients`，鉴权字段是 **`token`**，不是 `accessToken`。`enableLocalFile2Url: true` 是 JSON 顶层配置，不能写在单个 WS 客户端内。官方配置说明见 [NapCat 配置文档源码](https://github.com/NapNeko/NapCatDocs/blob/main/src/config/basic.md)。

本次检查了当前部署 NapCat 的真实实现：`get_image`、`get_file` 共享文件读取实现，接受 `file`（兼容 `file_id`）；启用顶层 `enableLocalFile2Url` 后，下载到本地的文件会增加 `base64` 返回字段。后端仍检查实际响应，不能把只有 `file` 本地路径或 `url` 的返回值当作已经拿到字节；任何情况下都不尝试读取上海容器路径，也不使用不存在的 `get_file_base64` 动作。

上海镜像使用用户指定的 `mlikiowa/napcat-docker:latest`。挂载 `/app/.config/QQ`、`/app/napcat/config` 和 `/app/napcat/plugins`；仅发布 `127.0.0.1:6099:6099`。官方镜像说明中，`MODE=reverse_ws`、`ONEBOT_URL` 与 `WEBUI_TOKEN` 负责缺失配置的初始化，已有文件不会靠环境变量自动改写，因此应先准备好 `onebot11_<机器人QQ号>.json`，或登录 WebUI 后核对客户端配置。[官方 Docker 说明](https://github.com/NapNeko/NapCat-Docker/blob/main/README.md)

仓库另提供官方 Node 镜像覆盖示例 `docker-compose.napcat.official-node.yml.example`。选择该方案时先复制为 `docker-compose.napcat.official-node.yml`，准备 `data/napcat-shanghai/state/config/` 中的 OneBot 与 WebUI 配置并校验 UID:GID `10001:10001` 权限，再叠加原上海 Compose 启动。覆盖文件不会替你生成连接凭证；真实 `.env.napcat`、运行覆盖文件和数据目录均由 Git 忽略。

```bash
cp docker-compose.napcat.official-node.yml.example docker-compose.napcat.official-node.yml
docker compose --env-file .env.napcat -f docker-compose.napcat.yml \
  -f docker-compose.napcat.official-node.yml up -d
```

## Nginx 与现有网站

提供的 `deploy/hakimi-ws.conf` 用独立域名 `qq.example.com`，保留原有 `example.com` 的 DSH 站点。先把新子域名解析到首尔，再为 `qq.example.com` 签发独立证书；仅覆盖主域名的证书不适用。

配置必须放在 Nginx `http {}` 加载范围，包含 `map` 和完整 `server`。精确匹配隐藏 WS 路径，`proxy_pass http://127.0.0.1:12141` 不带 URI，保留同名后端路由；其他所有路径统一 404，包括 `/admin`、`/docs`、`/openapi.json` 和 `/`。鉴权头、OneBot 身份头与升级头正常透传；不记录访问日志，避免保存隐藏路径。`client_max_body_size 20m` 并不代替 WS 帧限制，实际帧上限由 Uvicorn 和 Python 解码器共同执行。

在获得实际域名证书、替换随机路径后执行 `nginx -t` 再 reload。可选启用精确的上海出口 IP allow/deny；出口变化需先更新白名单。源码模板保留路径占位符，实际随机后缀只存在于受保护的运行配置，不写进源码、文档或交付包。

80 端口仅为本子域名保留 `/.well-known/acme-challenge/` 静态验证目录，用于证书自动续期；其余路径仍返回 404，不代理 FastAPI。证书续期的 deploy hook 必须先通过 `nginx -t` 再 reload，不应直接替换现有主域名站点。

## 本次部署进度（2026-11-14）

- `qq.example.com` 独立 Let's Encrypt 证书已签发，有效期至 **2027-02-12**；本子域名的 443 非 WS 路径返回 404。
- 主域名 DSH 的原配置校验值保持不变；新子域名使用独立 vhost。80 的专用 ACME 目录和续期校验／reload hook 已配置。
- 首尔后端已升级到 0.8.1，健康检查、后台鉴权、公网空／错误 Token 403、非 WS 路径 404 均通过；1,119 项测试及零模型调用的 Docker 收发 smoke 通过。上海 QQ 已登录，反向 WS 来源 IP 与心跳正常，Owner 已确认收到 `/哈基米 状态` 的零 Token 回复。用户指定的 `mlikiowa/napcat-docker:latest` 此次从 Docker Hub 拉取返回 `pull access denied`。
- 用户已明确选择并启用官方 `ghcr.io/napneko/nodenapcat` 固定 digest 备选，完整镜像校验后已启动，上海仅发布回环 `127.0.0.1:6099`；原 Docker Hub Compose 保留。上海 `/opt/hakimi-napcat/` 使用本地 `docker-compose.napcat.official-node.yml`，源码与交付包提供不含凭证的 `.example` 模板；NodeNapCat 的 entrypoint、UID 和数据卷布局不同，不能只替换 `image` 字段。不会恢复已下线的旧首尔 NapCat。

- `/admin` 思考强度面板已上线，九个场景均已保存为 `medium`，支持 `low / medium / high / xhigh` 热更新；小本本继续关闭。

## 已选择的官方 Node 镜像启动方式

先获取 override 中固定 digest 的官方镜像，再准备 `data/napcat-shanghai/qq/` 与 `data/napcat-shanghai/state/config/`，两者归 UID:GID `10001:10001`。OneBot JSON 和 WebUI 配置放在后者；此版本直接运行 Node loader，不能依赖 Docker Hub 版的 `MODE` / `WEBUI_TOKEN` 环境初始化。

```bash
cd /opt/hakimi-napcat
docker compose --env-file .env.napcat \
  -f docker-compose.napcat.yml -f docker-compose.napcat.official-node.yml up -d
```

官方来源：[NapCatLinuxNodeLoader](https://github.com/NapNeko/NapCatLinuxNodeLoader/blob/main/README.md)。`pull_policy: never` 要求镜像预先拉取／导入；固定摘要可避免重建时静默切换版本。协议端的真实配置仍只含 QQ／OneBot／WebUI 所需信息，不含 Gemini、Drive 或 SQLite 加密密钥。

## 连接管理与主动消息

握手在 `accept()` 前校验 Bearer Token（常量时间比较）、配置路径、真实 `X-Self-ID`、Universal 模式及禁止 query token。失败日志只含固定原因和校验过的来源 IP；来源头只信任配置中的直接代理。空或不足 32 字符的 WS 密钥在启动阶段拒绝运行。

新认证连接会立即替换活动 peer 指针，异步关闭旧的半开连接；每个 peer 独立保存 RPC echo、Future、发送锁和消费任务，旧连接清理不会清空新连接。登录后还需 `get_login_info` 核对机器人账号，并用 `get_status` 心跳核对 online/good。后台新增 `onebot_detail.online`、`last_heartbeat_at`、`source_ip`、`connected_at`。

`send_onebot_action_with_retry()` 最多等待连接 60 秒、至多三次尝试，仅当尚未向 Socket 写入任何内容时可以重试。一旦开始写入而缺少确认，保持“不确定”结果，绝不在新连接重复发送可能已送达的私信。超出等待的主动消息由既有 SQLite 待发队列继续保存，沿用原来的期限、免打扰、Owner 独占接收人和去重规则。

## 首尔停机冷却 → 上海上线清单

1. 停止旧首尔 NapCat，并设 `restart=no`；保留旧登录数据备份，禁止两端同时登录。不要执行带旧 `compose.napcat.yml` 的全量 `up`，以免重启旧节点。
2. 保留首尔数据库、加密密钥和 Drive 凭证；升级后端时仅使用首尔 Compose（若已启用 Drive，保留相应 OAuth 文件挂载 overlay）。确认仅监听回环 12141。
3. 配置新子域名 DNS、TLS 证书和独立 Nginx vhost；验证其他路径 404、无 Token 握手失败，不改变现有 DSH vhost。
4. 上海复制独立 Compose、`.env.napcat` 和填好 token/URL 的 OneBot JSON；确认文件权限与指定 UID/GID 一致。旧节点已停止后再启动上海，等 QQ 登录流程允许时扫码；不承诺某个固定冷却时长可消除平台异地风控，也不自动反复登录或绕过验证。
5. 仅通过 SSH 隧道进入上海 WebUI：`ssh -L 6099:127.0.0.1:6099 <上海SSH主机>`，本地打开 `http://127.0.0.1:6099/webui/`。如 QQ 要求设备验证，在手机端完成。
6. 首尔后台仍走隧道：`ssh -L 12141:127.0.0.1:12141 <首尔SSH主机>`，查看 `/admin` 中连接在线、来源及最近心跳。使用 Owner 明确测试指令验证收发；再测试引用原图和一个不超过 3,500,000 字节（约 3.5 MB）的截图。
7. 模拟短时断开上海连接，确认未发送的 Owner 主动消息仍在 SQLite 等待，恢复后仅投递未过期项目。不要通过重放已提交、状态不确定的消息验证“重试”。

本清单不需要上海开放 3000、3001 或 6099 公网端口；WSS 由上海主动连出，WebUI 只经现有 SSH 管理通道访问。

## 回退注意

升级前对所有 SQLite 文件执行在线备份，并保存原 `.env`、Nginx 站点配置与旧后端镜像。不要删除数据卷。0.8 的会话／索引表为增量新增，但日／月账本已从 UTC 重新分桶到上海时区；若回退旧代码，必须停后端后连同升级前数据库一起恢复，不能只切镜像标签继续写新账本。先另存当前数据库，避免丢失升级后的消息或费用记录；这些增量需单独核对后迁回。回退后端也不应自动重启旧首尔 NapCat，协议端只能保留一处登录。
