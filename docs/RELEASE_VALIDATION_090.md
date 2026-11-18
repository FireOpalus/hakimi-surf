# v0.9.0 发布验收记录

验收日期：2026-11-14，时区 Asia/Shanghai。

- `ruff check .`：通过。
- `ruff format --check .`：136 个 Python 文件通过。
- 全量 pytest：**1166 passed**，三份互不重复的文件分片分别为 376、369、421 项。模型请求全部使用 Mock，测试夹具阻断外网 socket；没有真实模型测试费用。
- Docker 镜像 `hakimi-surf:0.9.0` 在 `--network none` 下完成独立验收：非 root、反向 WS 登录／收发、机器人拦截、群／私聊解耦、引用回复、Webhook 鉴权去重、L0 抢话过滤、表情增删、管理鉴权、预算及思考热控、ffprobe／yt-dlp、临时缓存清理，总账为零。
- 镜像内 60 个 Python／HTML 源文件与发布工作区 SHA256 一致。
- 当前 v0.8.1 生产主库副本连续初始化两次：`ledger`、`ledger_usage`、`usage_aggregates`、`budget_totals`、`ledger_times` 五表内容哈希完全一致。两份物理隔离 Session 副本同样重复初始化，历史密文逐表不变。
- 三段既有人设正文（含边界换行）的 SHA256 断言全部通过；八轮 Owner／成员交替及中途摘要修改的 A+B 字节前缀测试通过。
- 额外验收：100 个长中文表情标签全部保留，静态前缀在本地估算区间内。内联管理 JS 通过语法检查；没有增加 Node/npm 部署依赖。

生产验收：

- 境外后端已替换为 v0.9.0，容器 healthy；端口仍为 `127.0.0.1:12141`。
- 境内反向 WS 自动恢复，来源与最近心跳验证通过；没有重新扫码或更改境内登录态。
- 旧境外 NapCat 保持 exited / restart=no。
- 管理 API 未鉴权返回 401；QQ 子域名公网 `/`、`/admin`、`/docs` 均返回 404；未授权 WS 握手返回 403，合法连接不受影响。
- 两个聊天场景已热保存为 auto；其他七个后台场景保持 medium；Carbon 开关启用。小本本仍关闭，预算时区仍为 Asia/Shanghai。
- Google Drive OAuth 配置及挂载保留；此次未额外触发同步或付费日报生成。

升级前已单独保存旧镜像、环境文件与三份数据库；秘密和数据库不包含在交付包内。完整实现与长度估算边界见 [v0.9 说明](CONTEXT_ROUTING_09.md)。本次未主动执行付费真实模型验收，实际缓存覆盖率及 Carbon 账号可用性应以正常使用产生的服务端 usage／状态为准；90% 不是已被证明的线上结果。
