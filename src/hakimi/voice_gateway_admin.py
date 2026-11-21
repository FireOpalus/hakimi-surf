"""Encrypted persistence and hot-swap control plane for hakimi-voice-gateway API settings."""

from __future__ import annotations

import asyncio
import base64
import time
from typing import Any, Literal

import httpx
from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field

from .security import Rejected, configured_secrets

_CONFIG_KEY = "voice_gateway_override"
_VAULT_CTX = "voice-gateway-override:v1"


class VoiceGatewayConfigUpdate(BaseModel):
    """Strict payload accepted by PUT /admin/api/life/voice-companion/gateway."""

    model_config = ConfigDict(extra="forbid")

    stt_provider: Literal["siliconflow", "fish", "openai"]
    tts_provider: Literal["siliconflow", "fish", "breezeblue", "edge"]
    tts_speed: float = Field(default=1.05, ge=0.25, le=4.0, allow_inf_nan=False)
    tts_gain: float = Field(default=0.0, ge=-10.0, le=10.0, allow_inf_nan=False)
    tts_format: Literal["mp3", "wav"] = "mp3"
    tts_buffer_mode: Literal["turn", "chunk"] = "turn"
    tts_streaming: bool = True

    siliconflow_base_url: str = Field(default="https://api.siliconflow.cn/v1", max_length=256)
    siliconflow_stt_model: str = Field(default="FunAudioLLM/SenseVoiceSmall", max_length=128)
    siliconflow_tts_model: str = Field(default="FunAudioLLM/CosyVoice2-0.5B", max_length=128)
    siliconflow_tts_voice: str = Field(default="FunAudioLLM/CosyVoice2-0.5B:diana", max_length=128)
    siliconflow_api_key: str = Field(default="", max_length=512)
    clear_siliconflow_api_key: bool = False

    fish_base_url: str = Field(default="https://api.fish.audio", max_length=256)
    fish_reference_id: str = Field(default="", max_length=128)
    fish_model: str = Field(default="", max_length=128)
    fish_latency: Literal["balanced", "normal"] = "balanced"
    fish_asr_language: str = Field(default="zh", max_length=32)
    fish_asr_model: str = Field(default="", max_length=128)
    fish_api_key: str = Field(default="", max_length=512)
    clear_fish_api_key: bool = False

    breezeblue_base_url: str = Field(default="https://api.breeze.blue", max_length=256)
    breezeblue_voice_id: str = Field(default="", max_length=128)
    breezeblue_model_id: str = Field(default="breeze-tts-2", max_length=128)
    breezeblue_language_code: str = Field(default="zh", max_length=32)
    breezeblue_instructions: str = Field(default="", max_length=500)
    breezeblue_guidance_scale: float | None = Field(
        default=None, ge=1.0, le=10.0, allow_inf_nan=False
    )
    breezeblue_speaker_boost: bool | None = None
    breezeblue_api_key: str = Field(default="", max_length=512)
    clear_breezeblue_api_key: bool = False

    openai_stt_base_url: str = Field(default="https://api.groq.com/openai/v1", max_length=256)
    openai_stt_model: str = Field(default="whisper-large-v3-turbo", max_length=128)
    openai_stt_api_key: str = Field(default="", max_length=512)
    clear_openai_stt_api_key: bool = False

    edge_tts_voice: str = Field(default="zh-CN-XiaoyiNeural", max_length=128)


class VoiceGatewayAdminManager:
    """Manages sealed gateway API overrides in SQLite and syncs them with hakimi-voice-gateway."""

    def __init__(self, settings: Any, store: Any, *, http: httpx.AsyncClient | None = None) -> None:
        self.s = settings
        self.store = store
        self.http = http
        self.initialized = False
        self._init_lock = asyncio.Lock()
        self._op_lock = asyncio.Lock()
        self._override: dict[str, Any] | None = None

    def configured(self) -> bool:
        return bool(
            str(self.s.voice_gateway_url or "").strip()
            and str(self.s.voice_gateway_token.get_secret_value() or "").strip()
        )

    def _base_url(self) -> str:
        return str(self.s.voice_gateway_url or "").strip().rstrip("/")

    def _headers(self) -> dict[str, str]:
        tok = self.s.voice_gateway_token.get_secret_value().strip()
        return {"Authorization": f"Bearer {tok}"}

    def _scrub(self, text: Any, *extra_secrets: str) -> str:
        out = " ".join(str(text or "").split())
        secrets = list(configured_secrets(self.s))
        secrets.extend(s for s in extra_secrets if isinstance(s, str) and s.strip())
        if isinstance(self._override, dict):
            for k in (
                "siliconflow_api_key",
                "fish_api_key",
                "breezeblue_api_key",
                "openai_stt_api_key",
            ):
                val = str(self._override.get(k) or "").strip()
                if val:
                    secrets.append(val)
        for sec in sorted({s for s in secrets if s}, key=len, reverse=True):
            out = out.replace(sec, "[REDACTED]")
        return out[:220]

    def _saved_keys(self) -> dict[str, bool]:
        ov = self._override or {}
        return {
            "siliconflow": bool(str(ov.get("siliconflow_api_key") or "").strip()),
            "fish": bool(str(ov.get("fish_api_key") or "").strip()),
            "breezeblue": bool(str(ov.get("breezeblue_api_key") or "").strip()),
            "openai_stt": bool(str(ov.get("openai_stt_api_key") or "").strip()),
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        timeout_sec: float = 5.0,
    ) -> httpx.Response:
        url = f"{self._base_url()}{path}"
        timeout = httpx.Timeout(timeout_sec)
        if self.http is not None:
            return await self.http.request(
                method,
                url,
                headers=self._headers(),
                json=json_body,
                timeout=timeout,
            )
        async with httpx.AsyncClient(
            trust_env=False, follow_redirects=False, timeout=timeout
        ) as client:
            return await client.request(
                method,
                url,
                headers=self._headers(),
                json=json_body,
            )

    async def initialize(self) -> None:
        async with self._init_lock:
            if self.initialized:
                return
            async with self.store.lock:
                await self.store.db.execute(
                    "CREATE TABLE IF NOT EXISTS admin_config (key TEXT PRIMARY KEY, payload BLOB)"
                )
                row = await self.store.one(
                    "SELECT payload FROM admin_config WHERE key=?", (_CONFIG_KEY,)
                )
            if row and row[0]:
                try:
                    data = self.store.vault.open(_VAULT_CTX, row[0])
                    if isinstance(data, dict):
                        self._override = data
                except Exception:
                    self._override = None
            self.initialized = True

    async def sync_to_gateway(self) -> bool:
        await self.initialize()
        async with self._op_lock:
            if not self.configured() or not isinstance(self._override, dict):
                return False
            try:
                resp = await self._request("POST", "/v1/admin/config", json_body=self._override)
                return resp.status_code == 200
            except (httpx.HTTPError, TimeoutError, ValueError):
                return False

    async def status(self) -> dict[str, Any]:
        await self.initialize()
        if not self.configured():
            return {
                "configured": False,
                "reachable": False,
                "source": "unconfigured",
                "saved_keys": self._saved_keys(),
                "gateway": None,
            }
        source = "sqlite" if self._override is not None else "env"
        try:
            resp = await self._request("GET", "/v1/admin/config")
            if resp.status_code != 200:
                return {
                    "configured": True,
                    "reachable": False,
                    "error": f"gateway_http_{resp.status_code}",
                    "source": source,
                    "saved_keys": self._saved_keys(),
                    "gateway": None,
                }
            gw = resp.json()
            if isinstance(self._override, dict) and not gw.get("overridden"):
                sync_resp = await self._request(
                    "POST", "/v1/admin/config", json_body=self._override
                )
                if sync_resp.status_code == 200:
                    gw = sync_resp.json()
            return {
                "configured": True,
                "reachable": True,
                "source": source,
                "saved_keys": self._saved_keys(),
                "gateway": gw,
            }
        except (httpx.HTTPError, TimeoutError, ValueError) as exc:
            return {
                "configured": True,
                "reachable": False,
                "error": self._scrub(type(exc).__name__),
                "source": source,
                "saved_keys": self._saved_keys(),
                "gateway": None,
            }

    async def update(self, payload: VoiceGatewayConfigUpdate) -> dict[str, Any]:
        await self.initialize()
        if not self.configured():
            raise Rejected("voice_gateway_not_configured")

        merged = payload.model_dump()
        key_pairs = (
            ("siliconflow_api_key", "clear_siliconflow_api_key"),
            ("fish_api_key", "clear_fish_api_key"),
            ("breezeblue_api_key", "clear_breezeblue_api_key"),
            ("openai_stt_api_key", "clear_openai_stt_api_key"),
        )
        submitted_secrets: list[str] = []
        prev = self._override if isinstance(self._override, dict) else {}
        for key_field, clear_field in key_pairs:
            raw_val = str(merged.get(key_field) or "").strip()
            if raw_val:
                submitted_secrets.append(raw_val)
            if merged.get(clear_field) is True:
                merged[key_field] = ""
                merged[clear_field] = True
            elif raw_val:
                merged[key_field] = raw_val
                merged[clear_field] = False
            elif str(prev.get(key_field) or "").strip() and not bool(prev.get(clear_field)):
                merged[key_field] = str(prev[key_field]).strip()
                merged[clear_field] = False
            else:
                merged[key_field] = ""
                merged[clear_field] = False

        async with self._op_lock:
            try:
                resp = await self._request("POST", "/v1/admin/config", json_body=merged)
            except (httpx.HTTPError, TimeoutError, ValueError):
                raise HTTPException(503, "voice_gateway_unreachable") from None

            if resp.status_code == 400:
                detail = ""
                try:
                    detail = str((resp.json() or {}).get("detail") or "")
                except ValueError:
                    detail = resp.text
                safe = self._scrub(detail, *submitted_secrets) or "voice_gateway_invalid_config"
                raise HTTPException(400, safe)
            if resp.status_code >= 400:
                raise HTTPException(502, "voice_gateway_error")

            try:
                gw = resp.json()
            except ValueError:
                raise HTTPException(502, "voice_gateway_error") from None

            sealed = self.store.vault.seal(_VAULT_CTX, merged)
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT OR REPLACE INTO admin_config VALUES (?, ?)", (_CONFIG_KEY, sealed)
                )
            self._override = merged
            return {
                "configured": True,
                "reachable": True,
                "source": "sqlite",
                "saved_keys": self._saved_keys(),
                "gateway": gw,
            }

    async def reset(self) -> dict[str, Any]:
        await self.initialize()
        async with self._op_lock:
            async with self.store.transaction():
                await self.store.db.execute("DELETE FROM admin_config WHERE key=?", (_CONFIG_KEY,))
            self._override = None
            if self.configured():
                try:
                    await self._request("DELETE", "/v1/admin/config")
                except (httpx.HTTPError, TimeoutError, ValueError):
                    pass
        return await self.status()

    async def _needs_gateway_resync(self) -> bool:
        if not isinstance(self._override, dict):
            return False
        try:
            cfg_resp = await self._request("GET", "/v1/admin/config")
            if cfg_resp.status_code == 200:
                gw = cfg_resp.json()
                if isinstance(gw, dict):
                    return not bool(gw.get("overridden"))
        except (httpx.HTTPError, TimeoutError, ValueError):
            return False
        return False

    async def list_breezeblue_voices(self) -> dict[str, Any]:
        await self.initialize()
        if not self.configured():
            raise Rejected("voice_gateway_not_configured")
        try:
            resp = await self._request("GET", "/v1/admin/voices", timeout_sec=10.0)
        except (httpx.HTTPError, TimeoutError, ValueError):
            raise HTTPException(503, "voice_gateway_unreachable") from None

        has_override_key = isinstance(self._override, dict) and bool(
            str(self._override.get("breezeblue_api_key") or "").strip()
        )
        should_resync = False
        if has_override_key and resp.status_code == 400:
            should_resync = True
        elif has_override_key and resp.status_code == 502:
            should_resync = await self._needs_gateway_resync()

        if should_resync and await self.sync_to_gateway():
            try:
                resp = await self._request("GET", "/v1/admin/voices", timeout_sec=10.0)
            except (httpx.HTTPError, TimeoutError, ValueError):
                raise HTTPException(503, "voice_gateway_unreachable") from None

        if resp.status_code == 400:
            detail = ""
            try:
                detail = str((resp.json() or {}).get("detail") or "")
            except ValueError:
                detail = resp.text
            raise HTTPException(400, self._scrub(detail) or "breezeblue_key_missing")
        if resp.status_code >= 400:
            raise HTTPException(502, "voice_gateway_voices_unavailable")
        try:
            data = resp.json()
        except ValueError:
            raise HTTPException(502, "voice_gateway_voices_unavailable") from None
        voices = data.get("voices") if isinstance(data, dict) else []
        return {"voices": voices if isinstance(voices, list) else []}

    async def test_tts(self, voice_reply: Any) -> dict[str, Any]:
        await self.initialize()
        if not self.configured():
            raise Rejected("voice_gateway_not_configured")
        t0 = time.perf_counter()
        try:
            synth = await voice_reply.synthesize("语音网关切换测试成功喵~", output_format="mp3")
        except Rejected:
            raise HTTPException(502, "voice_gateway_tts_test_failed") from None
        latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)
        fallback_mime = "audio/wav" if getattr(synth, "fmt", "") == "wav" else "audio/mpeg"
        mime_type = str(getattr(synth, "mime_type", None) or "").strip() or fallback_mime
        return {
            "ok": True,
            "bytes": len(synth.audio_bytes),
            "duration_ms": synth.duration_ms,
            "latency_ms": latency_ms,
            "fmt": synth.fmt,
            "mime_type": mime_type,
            "audio_b64": base64.b64encode(synth.audio_bytes).decode("ascii"),
        }
