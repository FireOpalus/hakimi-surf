# 境内节点独立组件合集 (`cn/`)

本目录集中维护所有部署并独立运行在境内节点（`tencent-cloud-CN`）的组件、编排与运维脚本，与主仓境外核心引擎（`src/hakimi/`、`admin/`）保持零运行时代码耦合。

## 目录结构

| 目录 | 境内部署路径 | 职责与技术栈 | 独立测试 / 验证 |
| :--- | :--- | :--- | :--- |
| [`cn/mc-agent/`](mc-agent/) | `/opt/hakimi-node-agent` | Minecraft 第 0 层独立躯干侧车（Node 22 + Mineflayer，0-Token 生存、筹料合成、寻路脱困、WS 桥接） | `cd cn/mc-agent && npm test` |
| [`cn/node-agent/`](node-agent/) | `/opt/hakimi-node-agent` | 境内节点代理（Python 3.12，B 站采集委派 + MC 本地 WS 桥接转发），含 `node_agent/` 与 `compose.cn.yaml` | `pytest tests/test_v20_node_agent.py tests/test_v20_p4_node_mc_bridge.py` |
| [`cn/napcat/`](napcat/) | `/opt/hakimi-napcat` | NapCat OneBot v11 境内编排模板与配置示例 | — |
| [`cn/mc-server/`](mc-server/) | `/opt/hakimi-mc-server` | Paper MC 服务端编排、`mc-powerd.py` 受控按需开关机守护与备份/白名单运维脚本 | `ruff check cn/mc-server/mc-powerd.py` |
| [`cn/song-finder/`](song-finder/) | `/opt/hakimi-song-finder` | 找歌插件境内独立部署编排与 `.env` 模板（源码保留在 `plugins/song_finder/` 共用 `hakimi_plugin_sdk`） | `pytest tests/test_plugin_song_finder.py` |
| [`cn/legacy/napcat_voice_relay/`](legacy/napcat_voice_relay/) | 已撤线归档 | 历史境内 OneBot 语音转写中继（现已切为 QQ 语音原生直传 Gemini） | `pytest tests/test_napcat_voice_relay.py` |

> **安全边界**：真实 `.env*`、凭证、数据库与运行数据仅保存在境内节点（权限 `0600`），严禁提交入库。
