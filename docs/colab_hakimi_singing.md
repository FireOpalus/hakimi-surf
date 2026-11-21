# 使用 Google Colab + RVC 为哈基米制作专属翻唱歌单

本文介绍如何利用 Google Colab 免费/付费 GPU（如 T4 / L4）训练哈基米的 **RVC v2** 音色模型，制作带伴奏的翻唱成品，并上传到管理后台「**猫猫歌单**」，让猫猫在私聊和群聊中随时点唱发语音条。

---

## 一、准备干声数据集（5～10 分钟）

1. **来源选择**
   - 直接使用当初给 Fish Audio 做音色克隆的**原始纯净干声**（最推荐，音色最还原）；
   - 或者用现有语音生成一批长短自然、情绪丰富的清晰语音拼起来作为补充素材。
2. **音频要求**
   - **纯干声，不能有背景音乐（BGM）、混响或环境底噪**（如有伴奏，先用 UVR5 / Demucs 提取干声并去混响）。
   - 格式推荐单声道 `WAV`（40kHz 或 48kHz），总时长 **5～10 分钟**即可满足高质量训练，过多重复或含静音片段反而降低清晰度。

---

## 二、在 Google Colab 使用 Applio 训练 RVC v2（RMVPE）

1. **打开 Applio 官方 Colab Notebook**（在 GPU 运行时启动，如 T4），并在首格挂载 Google Drive（`/content/drive/MyDrive/`），确保训练产物不会因 Colab 断连丢失：
   - 建议将模型保存在 `MyDrive/hakimi_rvc/` 目录。
2. **数据预处理（Preprocess）**
   - 采样率选择 **`40k`** 或 **`48k`**；
   - 将干声切分为 3～10 秒小段并清洗静音。
3. **提取特征（Extract）**
   - 音高提取器选择 **`RMVPE`**（对歌唱真假音和高音转折最稳）；
   - 特征编码器保持默认 `contentvec` 即可。
4. **训练（Train）**
   - 版本选 **`v2`**；
   - 5～10 分钟干声训练 **`150～300` Epoch**（`save_every_epoch = 25`），同时生成特征索引（`.index`）；
   - 建议在 TensorBoard 或试听检查点时挑选咬字最自然、无电音破音的那一版 `.pth` 与对应 `.index` 长期保存。

---

## 三、在 Colab 制作带伴奏翻唱成品

你可以继续使用 **Applio**，或专攻翻唱流水线的 **Ultimate RVC / AICoverGen**：

1. **分离原曲人声与伴奏**
   - 使用内置 UVR5 / BS-RoFormer 将原曲拆分为 `vocals.wav`（主唱干声，若含和声可用去和声模型再剥一遍）和 `instrumental.wav`（纯伴奏）。
2. **RVC 推理换声**
   - 载入上一步训好的哈基米 `.pth` 与 `.index`；
   - **音高偏移（Pitch Shift）**：
     - 女声唱女声歌：**`0`**；
     - 女声唱男声歌：**`+12`**（升高一个八度，避免音域过低闷声）；个别偏高或偏低的歌可微调 `+5` / `-2` 试听效果；
   - 音高算法选 **`RMVPE`**，检索比例（Index Rate）建议 `0.45～0.65`（保留猫猫音色又不丢原曲咬字）。
3. **混音导出**
   - 将换声后的干声与 `instrumental.wav` 混音，可加轻微大厅混响（Reverb `0.1～0.2`）让声音自然融进伴奏；
   - 导出为 `MP3`、`WAV` 或 `FLAC`（整首歌直接导出即可，后续切片在后台自动完成，但也可以在 Colab 先挑好副歌起点秒数）。

---

## 四、上传到哈基米「猫猫歌单」

QQ 语音条天然适合 **≤ 58 秒** 的副歌/高潮片段，因此无需提前手动剪切整首歌：

### 方式 1：管理后台可视化上传（推荐）
1. 打开哈基米管理后台（`/admin`），进入左侧「**关系与内容 → 猫猫歌单**」。
2. 选择或拖入 Colab 导出的翻唱音频（支持 `mp3 / wav / ogg / opus / m4a / flac`，单文件 `≤ 12 MiB`）。
3. 填写：
   - **歌名**（必填，例如 `小星星`，模型将按此歌名精准点唱）；
   - **原唱 / 风格标签**（可选，方便你和猫猫区分曲风，例如 `温柔 晚安`）；
   - **起始秒数 / 截取时长（5～58 秒）**（例如从副歌第 `62.5` 秒开始，截取 `45` 秒）。
4. 点击「**上传并切片转码**」，后端会自动去元数据并转码为 128 kbps MP3 入库（跨平台投递至支持 OGG Opus 的插件时还会实时转码），立即可直接在页面试听，并在私聊与群聊生效。

### 方式 2：从 Colab / 本机通过 `curl` 批量上传
如果你在 Colab 挂了多首成品，可以先经由本地 SSH 隧道把 KR `127.0.0.1:12143` 管理端口映射出来，再使用管理口令（`ADMIN_TOKEN`）配合 `X-Song-*` 请求头批量上传（后端出于安全规范禁止 `/admin/api/` 带 URL 查询参数；中文字符须先做 `encodeURIComponent` / `urllib.parse.quote` 编码）：

```bash
# 假设已建立本地 SSH 隧道：ssh -N -L 12143:127.0.0.1:12143 tencent-cloud-KR
TITLE_ENC=$(python3 -c 'import urllib.parse; print(urllib.parse.quote("小星星"))')
ARTIST_ENC=$(python3 -c 'import urllib.parse; print(urllib.parse.quote("传统儿歌"))')
TAGS_ENC=$(python3 -c 'import urllib.parse; print(urllib.parse.quote("温柔 晚安"))')
curl -fS -X POST \
  -H "Authorization: Bearer <YOUR_ADMIN_TOKEN>" \
  -H "Content-Type: audio/mpeg" \
  -H "X-Song-Title: ${TITLE_ENC}" \
  -H "X-Song-Artist: ${ARTIST_ENC}" \
  -H "X-Song-Tags: ${TAGS_ENC}" \
  -H "X-Song-Filename: cover.mp3" \
  -H "X-Song-Start: 0" \
  -H "X-Song-Duration: 50" \
  --data-binary "@/path/to/cover.mp3" \
  "http://127.0.0.1:12143/admin/api/songs/upload"
```

入库成功后，直接在私聊或开启了语音回复的群聊里对哈基米说「**唱一首《小星星》**」即可触发翻唱语音条。
