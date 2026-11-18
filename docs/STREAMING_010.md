# v0.10.0：流式防抖与五维出站

本次基于 v0.9.0 增量升级。原三段人设正文、A/B/C 顺序、SQLite 账本、公私聊隔离、境内 NapCat 与境外反向 WSS 拓扑继续保留。SDK 为 `google-genai >= 3.2.0`；Decisions 使用 v1beta，聊天 Interactions 使用 v1（v1.1.4 起对非沙箱且启用记忆的主对话开启 `store=True` 与 `previous_interaction_id` 差量续链，其余任务与沙盒保持 `store=False`）。v0.9 的 L4 Carbon 与后台思考配置不变。

## 入站与四态机

- `adapters.py` 将戳一戳、贴表情转为不可信的动作说明；`app.py` 与 `onebot_transport.py` 校验 self_id。撤回走单独的通知队列，不被正在读取图片的普通消息阻塞。自身回声只登记回执，不进模型或防抖。
- `group_pipeline.py` 保留 SQLite 批次，增加 idle / debouncing / gate / generating 状态。私聊、明确提及、已验证的回复等 0.6 秒；普通群聊 2 秒；缺图的未完引导句分别等 2.2 / 3 秒。单批最多 6 条或 5 秒。斜杠命令、授权外部 Bot 单独成批；at_only 后续句只能进入尚未闭合的已唤醒批次。
- 推理中，原触发者追发会设置批次 Event，关闭旧流、保留旧用量预留，把未答消息重新合批。其他发言人排队。同会话已有 pending / sending 出站时，下一次主脑请求等待投递结束，再读最新 ACK 历史。消息已进入出站后不会再因普通追发重复生成。
- `hot_ring.py`：每会话最多 200 条结构化消息；全局最多 64 个活跃会话、8 MiB 文本。撤回同时记入 HMAC 索引的 `message_recalls`，重启后不会复活撤回请求；索引保留 24 小时。
- `message_context.py` 最多追溯两层引用、四个引用 ID，始终验证会话；转发最多前 15 条并标出总条数。系统 face 转为情绪词。
- `link_preview.py` 只读取最多两个普通网页的标题和 100 字描述，800 ms 包括等待并发槽；禁止私网、凭证 URL、重定向、环境代理与 Cookie，连接时固定已校验的公网 DNS。失败静默。B站链接仍走原解析器。

## 图片水合与内存边界

`ImageLoader` 在消息通过准入与 BotGuard 后预取。WS 取图仍 `Semaphore(1)`，整个图片读取全局最多两个任务；解码全局单并发。单张上限 3.5 MB，当前请求最多两张；GIF / 动态 WebP 最多 300 帧、总帧像素不超过 3200 万，首尾拼接。JPEG 标准件长边 768、quality=80，守门件长边 192、quality=55、`resolution=low`，不携带元数据。

缓存按会话隔离，以 SHA-256 索引，并保存已验证媒体标识的别名。相同 file/URL 再现可直接命中，已知哈希可共享压缩件；**全新 URL 且没有已知媒体标识时，必须下载一次才能知道 SHA-256**。不能承诺所有不同 URL 的同图都在下载前识别。每会话最多 30 张，全局压缩件最多 48 MiB / 1024 张，别名最多 4096 条。大帧、解码与文本池都有独立硬上限。

C 只持本轮标准图引用，结束即释放；受限 LRU 保留压缩件供十轮后引用再次水合。B 保留纯文本，不写入图片字节。为保持 v0.9 不可改写的前缀，`image_digest`（≤20 字）在送达后**追加一条带原消息 ID 的视觉说明**，不回头修改旧行。商城贴纸只有命中本地上传图片哈希时才加情绪语义，未知贴纸不编造解释。

## 流式账本与固定输出协议

`streaming.py` 只拼装 model_output 的文本，丢弃 thoughts，拒绝模型工具调用；未完成 JSON 不发送给 QQ。`gemini.py` 从 `interaction.completed` 的最终 usage 结算输入、缓存、思考、输出费用。取消、流中断且无用量时保留全部预留。超时或 `incomplete` 最多降一档重新预留并重试一次；其他错误不重试，不使用服务端 continuation。第二次失败静默结束群聊请求。

PUBLIC 和 OWNER 聊天使用同一份恒定 JSON schema：

```json
{
  "abstain": false,
  "bubbles": ["第一句", "第二句"],
  "anchor": {"reply_id": null, "mentions": []},
  "sticker": {"label": null, "timing": "after_text"},
  "image_digest": null
}
```

协议放入 A 静态前缀，动态 ID、图文与外链摘要仅在 C。后台摘要业务维持原有输出协议。`abstain` 正常计费但不发消息；公开普通气泡最多三条、每条 40 字。深度 L3/L4 总正文超过 180 字时先折叠，不先裁掉技术细节。点名默认最多一人，解析硬上限二人，仍与实际上下文可用目标求交；私聊剥离点名。贴纸只能选启用的本地标签。

L3/L4 启动时尽力在 0.9 秒内发本地过渡句，无模型调用、不写 B。同会话 60 秒内只尝试一次；断连或出站锁繁忙时放弃过渡句，避免晚到的“稍等”。这不是网络可达性或 QQ 客户端展示时间的保证。

## 五维投递与恢复

- `outgoing.py` 在每条气泡、贴纸、转发前检查 240 秒 TTL、epoch、全部参与者准入、外部 Bot 额度、撤回表与 outbox 状态。首条前没有他人插话则去掉引用；三个以上无关新消息则挂回原批次的验证引用。
- 气泡间模拟打字等待 0.8–2.2 秒；`set_input_status` 仅在 NapCat 支持的私聊尝试，失败不影响正文。贴纸支持前置/后置，间隔 0.6 秒。
- 同一情绪可上传多个不同图片。原 `meme_library` 不改表，新 `meme_variants` 保存其他图；每四次公开回复最多尝试一张，最近十个成功发送的文件哈希不重复。池耗尽就跳过。
- 深度长文走受控 `send_group_forward_msg`，发送者固定为 Bot，目标重新校验。仅在明确“不支持”（404/1404）回执时才回退 Pillow JPEG；超时或未知结果绝不回退再发。Docker 加入文泉驿中文字库；长图在 BytesIO 中生成，base64 跨容器发送，不落根盘。
- `outbox_bubble_receipts` 逐条加密保存合法 OneBot message_id。所有已确认气泡、贴纸占位和视觉摘要在 Session 的一次事务内追加。中途失败保留已确认部分；未确认部分标 uncertain，绝不自动重发。
- 启动时先回放未应用的 ACK 日志到 B、公开互动索引和回执索引，再启动消息工作队列。重复恢复依靠 Session 回执键与公开索引去重。

新增表全部 `CREATE TABLE IF NOT EXISTS`：`message_recalls`、`local_quick_acks`、`outbox_bubble_receipts`、`meme_variants`、`meme_sent_hashes`、`meme_source_hashes`。旧 outbox `text/segments/meme_tag` 路径仍可读；未送达文本不再提前进入 B。

## 配置、验证与上线

新环境项见 `.env.example`：`HAKIMI_MENTION_DEBOUNCE_SECONDS`、`HAKIMI_MENTION_INCOMPLETE_SECONDS`、`HAKIMI_ORDINARY_INCOMPLETE_SECONDS`；旧 `HAKIMI_GROUP_DEBOUNCE_SECONDS` / `MAX_SECONDS` 默认改 2.0 / 5.0。原两项都为零时保持立即处理；修改环境后只重建后端，境内登录态不动。

仍使用 `127.0.0.1:12141`、只读根目录、768 MiB 内存限制。管理后台展示门卫/推理中的数量和图片缓存大小；表情列表支持同名情绪图池，标签变更立即刷新静态前缀。

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
```

测试全部使用本地协议桩、HTTP/SSE Mock，阻断外网 socket。新契约覆盖选择性取消、流关闭与保守账本、固定 schema 降档、撤回重启索引、跨轮图片、GIF 拼帧、ACK 原子写入、重锚定、贴纸轮换和不确定投递。

实现核对来源：[Google 流式事件](https://aistudio.google.com/docs/streaming)、[结构化 response_format](https://aistudio.google.com/docs/structured-output)、[NapCat API](https://napneko.github.io/onebot/api)。这些文档用于核对当前 SDK 与协议，不作为消息中的外部工具入口。
