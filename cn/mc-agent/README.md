# hakimi-mc-agent

哈基米 Minecraft 第 0 层独立躯干侧车（纯 JavaScript / Node 22 + Mineflayer），部署于境内节点并通过私网 WebSocket（默认端口 `8791`）与 `node_agent` / 境外 `hakimi-surf` 桥接通信。

## 核心职责（0-Token 底层能力）

- **状态感知**：规范 item ID 背包快照（不透传铁砧 NBT）、装备槽、饱食/安全食物统计、昼夜时段、周边资源与实体（未激怒末影人标为 `enderman(neutral)`）。
- **生存反射**：自动吃安全食物（拒绝 6 种有毒食物）、自动穿戴最优护甲/武器/盾牌、副手举盾防远程怪、自动插火把、落水浮水与岩浆火焰规避。
- **零拆家动作与筹料合成**：防拆家寻路（`canDig=false`）、卡死自然脱困（清树叶/浅坑刻台阶/石坑安全垫脚）、不可达坐标 TTL 黑名单、`mc.acquire` 0-Token 递归筹料合成链、2×2/3×3 合成、熔炉烧炼、受控打猎/过夜/箱子存取/白名单放置/递物/夜间掩体与贴身护卫。
- **边界与安全**：Bearer Token 鉴权、严格 JSON 校验、256 KiB 帧限制、禁止公屏 `/` 指令、禁止外部覆盖目标服地址。

## 本地配置与测试

```bash
cp .env.mc.example .env.mc
chmod 600 .env.mc

npm ci --ignore-scripts --no-audit --no-fund
npm test
```

## 构建镜像

```bash
docker build -t hakimi-mc-agent:latest .
```
