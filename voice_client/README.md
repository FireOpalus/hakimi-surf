# Hakimi Mac 实时语音客户端 (`voice_client`)

面向 macOS（Python 3.12+ / 3.15 + `ffmpeg` + `afplay`/`ffplay`）的轻量纯 Python 实时语音客户端，支持直连 KR 通用语音网关（`VOICE_GATEWAY_URL`，推荐，云端集中管理 STT/TTS Key 与流式音频转码）或使用 `--direct` 直连 `/ws/companion` 搭配本地 STT/TTS，零 C 扩展依赖。

## 1. 环境准备（macOS）

```bash
# 确保已安装 ffmpeg（用于 AVFoundation 麦克风采集与设备枚举；附带的 ffplay 支持流式边下边播）
brew install ffmpeg

# 创建虚拟环境并安装仅有的两个外部依赖 (httpx / websockets)
/opt/homebrew/bin/python3 -m venv ~/.local/share/hakimi-voice/venv
~/.local/share/hakimi-voice/venv/bin/pip install -r voice_client/requirements.txt
```

## 2. 配置文件 (`~/.config/hakimi-voice/.env`)

建议目录权限 `0700`，文件权限 `0600`：

### 2.1 通用语音网关模式（推荐，Mac 本地无需保存 STT/TTS API Key）

```ini
# 通用语音网关连接信息（设置 VOICE_GATEWAY_URL 后自动启用网关模式）
VOICE_GATEWAY_URL=wss://example.com/ws/voice/secret-path
VOICE_GATEWAY_TOKEN=your-32-char-or-longer-voice-gateway-token
# 可选覆盖网关输出格式: 留空跟随网关默认 / mp3 / wav / pcm_16k / pcm_48k_stereo
VOICE_GATEWAY_OUTPUT_FORMAT=mp3

COMPANION_ADAPTER_ID=mac-voice
COMPANION_CLIENT_ID=boiser-m4
COMPANION_MODE=owner_private
TTS_STREAMING=true
VOICE_MODE=vad
AUDIO_INPUT_DEVICE=0
```

### 2.2 直连 `/ws/companion` 模式（未设置 `VOICE_GATEWAY_URL` 或传 `--direct` 时启用）

```ini
# WebSocket 连接信息（与服务端 /ws/companion 配置一致）
COMPANION_WS_URL=wss://example.com/ws/companion-secret
COMPANION_WS_TOKEN=your-companion-ws-token
COMPANION_ADAPTER_ID=mac-voice
COMPANION_CLIENT_ID=boiser-m4
COMPANION_MODE=owner_private

# 语音方案 A：硅基流动 SiliconFlow
STT_PROVIDER=siliconflow
TTS_PROVIDER=siliconflow
SILICONFLOW_API_KEY=your-siliconflow-api-key
SILICONFLOW_BASE_URL=https://api.siliconflow.cn/v1
SILICONFLOW_STT_MODEL=FunAudioLLM/SenseVoiceSmall
SILICONFLOW_TTS_MODEL=FunAudioLLM/CosyVoice2-0.5B
SILICONFLOW_TTS_VOICE=FunAudioLLM/CosyVoice2-0.5B:diana
TTS_SPEED=1.05
TTS_GAIN=0.0
TTS_FORMAT=mp3

# 音频与 VAD / PTT 参数
VOICE_MODE=vad
AUDIO_INPUT_DEVICE=0
VAD_THRESHOLD=0.018
VAD_SILENCE_MS=480
VAD_MIN_SPEECH_MS=320
VAD_MAX_SPEECH_MS=20000
VAD_BARGE_IN_MULTIPLIER=2.2
BARGE_IN_ENABLED=true
```

- `TTS_BUFFER_MODE=turn`（默认）：流式收到文本后先在终端实时显示，等整轮说完后**一次性整段合成播报**（消除逗号逐句切片导致的机械停顿）；设为 `chunk` 或传 `--tts-buffer-mode chunk` 可切回逐句流式播报。
- 可选 Provider：
  - **Fish Audio STT + TTS（`STT_PROVIDER=fish`, `TTS_PROVIDER=fish`）**：低延迟约 200ms 识别（按净音频秒数计费，向上取整到整秒，挂机不收费），可设 `FISH_ASR_LANGUAGE=zh` 与 `TTS_STREAMING=true`（通过 `ffplay` 管道边下边播，不切片也能 ~300ms 开口）；
  - **BreezeBlue TTS（`TTS_PROVIDER=breezeblue`）**：配置 `BREEZEBLUE_API_KEY` 与 `BREEZEBLUE_VOICE_ID`（Saved Voice ID），可选 `BREEZEBLUE_MODEL_ID`（默认 `breeze-tts-2`）、`BREEZEBLUE_LANGUAGE_CODE`（默认 `zh`）、`BREEZEBLUE_INSTRUCTIONS`、`BREEZEBLUE_GUIDANCE_SCALE`（`1.0~10.0`）与 `BREEZEBLUE_SPEAKER_BOOST`。
  - **Fish Audio TTS 细节**：配置 `FISH_API_KEY`，可选填 `FISH_REFERENCE_ID`（音色 UUID，留空使用默认）、`FISH_MODEL`（如 `s2.1-pro`、`s2.1-pro-free`、`s2-pro`，留空走 Fish Audio 最新默认）、`FISH_LATENCY`（`balanced` / `normal`）；支持 `TTS_SPEED` 与 `TTS_GAIN` 语速音量控制。
  - **OpenAI/Groq 兼容 STT（`STT_PROVIDER=openai`）**：搭配 `OPENAI_STT_API_KEY` / `OPENAI_STT_BASE_URL` / `OPENAI_STT_MODEL`。
  - **本地 Edge TTS 兜底（`TTS_PROVIDER=edge`）**：依赖本地 `edge-tts` 命令。

## 3. 自检与工具命令

```bash
# 1) 列出 macOS 麦克风设备编号（确认 AUDIO_INPUT_DEVICE）
python3 -m voice_client --list-devices

# 2) 验证网关或 /ws/companion 握手与心跳 RTT（不触发 LLM 消耗）
python3 -m voice_client --check-ws
# 强制跳过 VOICE_GATEWAY_URL 直连 /ws/companion
python3 -m voice_client --direct --check-ws

# 3) 测试 TTS 合成与流式播放（网关模式下无需本地 API Key）
python3 -m voice_client --test-tts "哈基米收到啦，测试语音播报喵~"

# 4) 测试 4 秒麦克风录音 + STT 转写（网关模式下无需本地 API Key）
python3 -m voice_client --test-stt

# 5) 上传自定义参考音色，打印可直接写入 SILICONFLOW_TTS_VOICE 的 speech:... URI
python3 -m voice_client --upload-voice ./ref.wav --voice-name my-voice --ref-text "参考音频对应的准确台词"
```

## 4. 启动实时对话与快捷键

```bash
python3 -m voice_client --mode vad
# 或强制直连模式 / 按键说话模式 / 指定游戏标题 / 指定麦克风设备
python3 -m voice_client --direct --mode ptt --game "Minecraft" --device 0
```

交互模式快捷键：

| 按键 | 功能说明 |
| :--- | :--- |
| `[Space]` | PTT 模式下切换开始录音 / 停止并发送识别 |
| `[m]` | 实时切换 `VAD`（自动语音检测）↔ `PTT`（按键说话） |
| `[x]` | 立即打断当前播报与服务端本轮生成（`turn.cancel`） |
| `[t]` | 暂停单键模式，输入一行文字直接发送 |
| `[q]` | 干净结束会话并退出 |

- **VAD 打断（Barge-in）**：当 `BARGE_IN_ENABLED=true` 且播放器正在播报时，检测阈值自动提高至 `VAD_THRESHOLD * VAD_BARGE_IN_MULTIPLIER` 以减少回声误触发；检测到真实插话后立即终止 `afplay` 并向服务端发送 `turn.cancel`。
