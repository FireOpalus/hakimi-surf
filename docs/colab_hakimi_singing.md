# 使用 Google Colab CLI 自动化训练哈基米 RVC 模型与翻唱入库

本文档说明如何在 KR 纯 CPU 服务器上通过官方 `google-colab-cli` 远程拉起 Google Colab GPU（L4 / T4）完成 **BreezeBlue 纯净干声构建 → RVC v2 音色模型训练 → 人声分离与翻唱混音 → 自动切副歌入库「猫猫歌单」**。

---

## 一、环境与目录约定

在 KR 服务器仓库根目录（`/root/Document/hakimi-surf`）下：

- `.colab-venv/`：独立虚拟环境，安装官方 `google-colab-cli`（不污染引擎 `.venv`）。
- `secrets/colab-adc.json`（`0600`）：`google-colab-cli --auth adc` 使用的 Google OAuth ADC 凭证（已在 `.gitignore` 中排除）。
- `data/rvc_datasets/hakimi_breezeblue/`：由 BreezeBlue（`voc_psm85weeq82a`）合成的 68 条多风格纯净干声 WAV 与打包归档 `dataset.tar.gz`。
- `data/rvc_models/hakimi/`：从 Colab 拉回并经 SHA-256 校验的 `hakimi.pth` 与 `hakimi.index`。
- `data/rvc_sources/`：待翻唱原曲音频素材（如 `yu_ai.m4a`）。
- `data/rvc_covers/<歌名>/`：翻唱成品（整首 `cover_full.mp3`、副歌版 `cover_chorus.mp3` 与 `cover_meta.json`）。

---

## 二、一次性 Google 授权（无头服务器免 Keyring 方案）

由于 KR 服务器无桌面 Keyring，全部命令统一使用 `--auth adc` + `secrets/colab-adc.json`：

```bash
# 1. 在 KR 安装独立 Colab CLI 环境（首次）
python3 -m venv .colab-venv
.colab-venv/bin/pip install --upgrade pip google-colab-cli

# 2. 生成一次性 OAuth 授权链接
.venv/bin/python scripts/colab_rvc.py auth --start

# 3. 浏览器打开链接登录 Colab Pro/Ultra 账号并复制授权码，回填交换凭据
.venv/bin/python scripts/colab_rvc.py auth --code "<PASTE_CODE>"

# 4. 校验登录账号与算力余额
.venv/bin/python scripts/colab_rvc.py whoami
```

---

## 三、端到端与分步命令

### 1. 生成 BreezeBlue 纯净干声训练集（68 条，约 6~8 分钟）

```bash
.venv/bin/python scripts/colab_rvc.py build-dataset
```

支持断点续传（已校验通过的 `utt_XXX.wav` 会自动跳过）。

### 2. 下载原曲音频（或自行上传至 `data/rvc_sources/`）

```bash
.venv/bin/python scripts/colab_rvc.py fetch-song \
  --url "https://www.youtube.com/watch?v=MirexJxv9PE" \
  --out data/rvc_sources/yu_ai.m4a
```

### 3. 一条命令完成训练 + 翻唱 + 自动入库「猫猫歌单」

```bash
.venv/bin/python scripts/colab_rvc.py all \
  --source data/rvc_sources/yu_ai.m4a \
  --title "雨爱" \
  --artist "杨丞琳" \
  --tags "温柔 抒情" \
  --chorus-anchor "窗外的雨滴" \
  --fallback-start 60 \
  --duration 50 \
  --gpu L4
```

- 优先申请 `L4` GPU（不可用时自动降级为 `T4`），所有文件大于 6 MiB 时自动分片传输并做 SHA-256 校验；
- Colab 上采用隔离的双 `uv` 虚拟环境：`/content/rvc-venv`（Applio RVC v2，`40k` / `RMVPE` / `contentvec`）与 `/content/sep-venv`（`audio-separator[gpu]` + `faster-whisper` 副歌歌词锚点定位）；
- 训练结束后立即将 `hakimi.pth` 与 `hakimi.index` 回传到 KR，并在同一台暖机 GPU 上完成 BS-RoFormer 伴奏分离、去混响、RVC 推理、响度对齐混音与淡入淡出裁剪；
- 任务完成或异常退出都会在 `finally` 中自动执行 `colab stop` 释放算力，避免空烧 Compute Units。

### 4. 模型训好后，单独加新歌 / 重新切片上传

```bash
# 直接复用已有 hakimi.pth + hakimi.index 在 Colab 翻唱新歌并自动入库
.venv/bin/python scripts/colab_rvc.py cover \
  --source data/rvc_sources/new_song.m4a \
  --title "新歌名" \
  --artist "原唱" \
  --pitch 0 \
  --start auto \
  --duration 50

# 若只想对已拉回的 cover_full.mp3 重新挑选起始秒数切片入库（不占用 Colab GPU）
.venv/bin/python scripts/colab_rvc.py upload-song \
  --audio data/rvc_covers/雨爱/cover_full.mp3 \
  --title "雨爱" \
  --artist "杨丞琳" \
  --tags "温柔 抒情" \
  --start 63 \
  --duration 50
```

---

## 四、管理后台手动上传补充

除自动化入库外，也可随时打开哈基米管理后台（`/admin`）左侧「**关系与内容 → 猫猫歌单**」，拖入任意 `cover_full.mp3` 或成品音频，可视化设置 `start_sec`（0～600s）与 `duration_sec`（5～58s）并在线试听。入库成功后，在私聊或群聊里直接对哈基米说「**唱一首《雨爱》**」即可触发点唱语音条。
