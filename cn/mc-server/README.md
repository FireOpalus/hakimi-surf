# 境内 Minecraft 服务端与按需开关机守护 (`cn/mc-server`)

部署于境内宿主机 `/opt/hakimi-mc-server/`，配合 `cn/mc-agent` 实现按需唤醒、无人自动休眠与定时世界备份。

## 组件说明

- `mc-powerd.py`：零外部依赖受控 HTTP 守护进程，仅监听私网桥接 IP（拒绝 `0.0.0.0`），提供 `/status`、`/start`、`/stop`（Bearer 鉴权）、可用内存闸门与无人闲置自动休眠。
- `compose.yaml`：Paper 服务端容器编排模板（默认通过 `MC_SERVER_PORT` 暴露游戏端口，加入 `hakimi-node-agent_hakimi_cn_net` 私网供 `mc-agent` 内网直连）。
- `mc-ctl.sh` / `mc-cmd.sh` / `backup.sh` / `mc-chunky.sh` / `mc-friend.sh`：启停检查、RCON 命令、在线安全刷盘备份、区块预生成与好友白名单管理脚本。

## 配置与部署

1. 复制 `.env.powerd.example` 至 `/opt/hakimi-mc-server/.env.powerd`（`chmod 600`），填入强随机 `MC_POWERD_TOKEN`（与 `/opt/hakimi-node-agent/.env.mc` 保持一致）。
2. 安装 systemd 服务：`cp hakimi-mc-powerd.service.example /etc/systemd/system/hakimi-mc-powerd.service && systemctl daemon-reload && systemctl enable --now hakimi-mc-powerd.service`。
3. 运行 `mc-friend.sh` 时，可通过环境变量 `MC_SERVER_ADDR`（如 `example.com:25565`）与 `MC_PROTECTED_ACCOUNTS`（逗号分隔受保护玩家名）注入本机参数，勿将真实地址或密码写入仓库。
