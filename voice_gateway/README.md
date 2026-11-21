# Hakimi Voice Gateway (`voice_gateway`)

Standalone KR universal audio gateway (`hakimi-voice-gateway`) providing centralized STT/TTS transcoding and multiplexed JSON/binary WebSocket bridging to `hakimi-bot`'s `/ws/companion`.

## Architecture

- **Thin clients** (Mac `voice_client`, Windows/mobile/game adapters) connect over TLS WebSocket (`/ws/voice/<random-48-hex>`) using a single `VOICE_GATEWAY_TOKEN` (`>= 32` chars).
- **Audio codec normalization** (`audio_codec.py`):
  - Input (`audio.turn`, `audio.stream.*`, `stt.transcribe`, `POST /v1/stt`): `wav` (RIFF/WAVE passthrough), `pcm_s16le` / `pcm_16k` (pure-Python WAV wrapping), `pcm_48k_stereo` (ffmpeg downsample), and `opus` / `ogg` / `mp3` / `webm` (ffmpeg decode to 16kHz mono WAV).
  - Output (`mp3`, `wav`, `pcm_16k`, `pcm_48k_stereo`): zero-copy passthrough when formats match, or live `ffmpeg` pipe transcoding with subprocess termination on barge-in/cancel.
- **Standalone RPCs** (`ping` pre-session, `stt.transcribe`, `tts.synthesize`, `POST /v1/stt`, `POST /v1/tts`) never open an upstream `/ws/companion` session and incur zero LLM usage.
- **Interactive companion sessions** (`session.start` -> `audio.turn` / `audio.stream.*` / `text.turn` / `turn.cancel` -> `session.stop`) forward recognized speech (plus optional `speaker_id`, `speaker_name`, `frames`) to `/ws/companion` and serialize `tts.start` -> raw binary frames -> `tts.end` before emitting `turn.end(status="spoken")`.

## Configuration & Deployment (KR)

1. Copy `.env.voice-gateway.example` to `./secrets/voice-gateway.env` (`chmod 600`) and populate strong random values for `VOICE_GATEWAY_WS_PATH` and `VOICE_GATEWAY_TOKEN` plus provider keys (`SILICONFLOW_API_KEY` / `FISH_API_KEY` / `OPENAI_STT_API_KEY`) and `COMPANION_WS_URL` / `COMPANION_WS_TOKEN`.
2. Enable the matching `location = /ws/voice/<48-hex>` block in Nginx (`deploy/hakimi-ws.conf`), keeping `access_log off;` and `proxy_pass http://127.0.0.1:12145;` without a URI suffix.
3. Start alongside `hakimi-bot`:

```bash
docker compose -f compose.yaml -f compose.voice-gateway.yaml up -d --build hakimi-voice-gateway
```

## Internal HTTP Endpoints (Loopback / Docker Network Only)

- `GET /healthz`: unauthenticated liveness check (`status`, `stt_provider`, `tts_provider`).
- `POST /v1/stt`: Bearer-authenticated `multipart/form-data` (`file`, optional `format`, `sample_rate`, `channels`) -> `{"text", "stt_ms", "provider"}`.
- `POST /v1/tts`: Bearer-authenticated JSON (`{"text", "output_format", "stream"}`) -> streamed or buffered audio bytes with `X-Audio-Format`, `X-Sample-Rate`, `X-Channels`.

## WebSocket Protocol Summary

- Handshake headers: `Authorization: Bearer <VOICE_GATEWAY_TOKEN>`, optional `X-Voice-Adapter` (`^[a-z][a-z0-9_-]{1,31}$`), optional `X-Voice-Client-Id` (`^[A-Za-z0-9_.-]{1,64}$`). Query parameters, path mismatches, and invalid headers close with `1008`.
- Client -> Gateway JSON: `ping`, `stt.transcribe`, `tts.synthesize`, `session.start`, `audio.turn`, `audio.stream.start`, `audio.stream.end`, `text.turn`, `turn.cancel`, `session.stop`.
- Client -> Gateway binary: raw audio chunks between `audio.stream.start` and `audio.stream.end` (capped by `VOICE_GATEWAY_MAX_AUDIO_BYTES`).
- Gateway -> Client JSON: `pong`, `stt.result`, `session.ready`, `stt.final`, `turn.start`, `text.chunk`, `tts.start`, `tts.end`, `turn.end`, `session.ended`, `error` (all details secret-redacted).
- Gateway -> Client binary: raw synthesized audio frames between `tts.start` and `tts.end`.
