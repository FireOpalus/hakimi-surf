"""Bounded bidirectional chunked Blob transfer helpers for PluginBus."""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from typing import TYPE_CHECKING, Any

from hakimi_plugin_sdk.protocol import (
    BLOB_CHUNK_RAW_BYTES,
    BLOB_FIELD,
    BLOB_HARD_MAX_BYTES,
    BLOB_INLINE_MAX_BYTES,
    BLOB_RPC_GET,
    BLOB_RPC_PUT,
    BLOB_RPC_RELEASE,
    BLOB_TRANSFER_TTL_SECONDS,
    ToolCallResult,
    compute_chunk_raw_bytes,
    compute_inline_max_bytes,
    decode_blob_chunk,
    encode_blob_chunk,
    sanitize_blob_meta_str,
    sha256_hex,
    validate_blob_descriptor,
)

from ..security import Rejected
from .protocol import _CODE_RE
from .proxy import _PluginSession

__all__ = [
    "BLOB_CHUNK_RAW_BYTES",
    "BLOB_FIELD",
    "BLOB_HARD_MAX_BYTES",
    "BLOB_INLINE_MAX_BYTES",
    "BLOB_RPC_GET",
    "BLOB_RPC_PUT",
    "BLOB_RPC_RELEASE",
    "BLOB_TRANSFER_TTL_SECONDS",
    "PluginBusBlobMixin",
    "ToolCallResult",
]


class PluginBusBlobMixin:
    """Mixin implementing bounded chunked Blob upload, download, verification, and release."""

    if TYPE_CHECKING:
        _blob_transfer_sem: asyncio.Semaphore

        def _max_frame_bytes(self) -> int: ...

        async def _rpc(
            self,
            session: _PluginSession,
            method: str,
            params: dict[str, Any],
            *,
            timeout: float | None = None,  # noqa: ASYNC109
            delivery_id: str | None = None,
        ) -> dict[str, Any]: ...

    async def _best_effort_release_blob(
        self,
        session: _PluginSession,
        *,
        transfer_id: str | None = None,
        blob_id: str | None = None,
    ) -> None:
        if session.closed or not session.ready:
            return
        params: dict[str, Any] = {}
        if transfer_id:
            params["transfer_id"] = transfer_id
        if blob_id:
            params["blob_id"] = blob_id
        if not params:
            return
        try:
            await self._rpc(session, BLOB_RPC_RELEASE, params, timeout=1.5)
        except Exception:
            pass

    async def _upload_blob_chunks(
        self,
        session: _PluginSession,
        raw: bytes,
        *,
        digest: str,
        mime_type: str,
        filename: str,
        deadline: float,
    ) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        max_frame = self._max_frame_bytes()
        chunk_size = compute_chunk_raw_bytes(max_frame)
        total_chunks = max(1, (len(raw) + chunk_size - 1) // chunk_size)
        transfer_id = uuid.uuid4().hex
        completed = False
        async with self._blob_transfer_sem:
            try:
                for seq in range(total_chunks):
                    rem = deadline - loop.time()
                    if rem <= 0:
                        raise Rejected("plugin_rpc_timeout")
                    start = seq * chunk_size
                    end = min(len(raw), start + chunk_size)
                    put_resp = await self._rpc(
                        session,
                        BLOB_RPC_PUT,
                        {
                            "transfer_id": transfer_id,
                            "seq": seq,
                            "total_chunks": total_chunks,
                            "total_bytes": len(raw),
                            "sha256": digest,
                            "data_b64": encode_blob_chunk(raw[start:end]),
                            "mime_type": mime_type,
                            "filename": filename,
                        },
                        timeout=min(rem, 15.0),
                    )
                    if not put_resp.get("ok", False):
                        raw_err = str(put_resp.get("error") or "")
                        code = raw_err if _CODE_RE.fullmatch(raw_err) else "plugin_blob_failed"
                        raise Rejected(code)
                completed = True
            finally:
                if not completed:
                    await self._best_effort_release_blob(session, transfer_id=transfer_id)
        return {
            "encoding": "chunked",
            "transfer_id": transfer_id,
            "byte_size": len(raw),
            "sha256": digest,
            "total_chunks": total_chunks,
            "mime_type": mime_type,
            "filename": filename,
        }

    async def _download_blob_chunks(
        self,
        session: _PluginSession,
        desc: dict[str, Any],
        *,
        deadline: float,
    ) -> bytes:
        loop = asyncio.get_running_loop()
        blob_id = desc["blob_id"]
        total_chunks = int(desc["total_chunks"])
        expected_bytes = int(desc["byte_size"])
        expected_digest = str(desc["sha256"])
        chunk_cap = compute_chunk_raw_bytes(self._max_frame_bytes())
        buf = bytearray()
        hasher = hashlib.sha256()
        completed = False
        async with self._blob_transfer_sem:
            try:
                for seq in range(total_chunks):
                    rem = deadline - loop.time()
                    if rem <= 0:
                        raise Rejected("plugin_rpc_timeout")
                    get_resp = await self._rpc(
                        session,
                        BLOB_RPC_GET,
                        {"blob_id": blob_id, "seq": seq},
                        timeout=min(rem, 15.0),
                    )
                    if not get_resp.get("ok", False):
                        raw_err = str(get_resp.get("error") or "")
                        code = raw_err if _CODE_RE.fullmatch(raw_err) else "plugin_blob_failed"
                        raise Rejected(code)
                    chunk_res = get_resp.get("result")
                    if not isinstance(chunk_res, dict) or chunk_res.get("seq") != seq:
                        raise Rejected("plugin_blob_corrupt")
                    try:
                        chunk_bytes = decode_blob_chunk(
                            chunk_res.get("data_b64"), max_raw_bytes=chunk_cap
                        )
                    except ValueError as exc:
                        raise Rejected(
                            "plugin_blob_too_large"
                            if str(exc) == "blob_too_large"
                            else "plugin_blob_corrupt"
                        ) from None
                    if len(buf) + len(chunk_bytes) > expected_bytes:
                        raise Rejected("plugin_blob_too_large")
                    buf.extend(chunk_bytes)
                    hasher.update(chunk_bytes)
                if len(buf) != expected_bytes or hasher.hexdigest() != expected_digest:
                    raise Rejected("plugin_blob_corrupt")
                completed = True
                return bytes(buf)
            finally:
                if not completed:
                    await self._best_effort_release_blob(session, blob_id=blob_id)

    async def _prepare_request_blob(
        self,
        session: _PluginSession,
        blob: bytes | bytearray,
        *,
        rpc_params: dict[str, Any],
        blob_mime_type: str,
        blob_filename: str,
        req_cap: int,
        deadline: float,
    ) -> tuple[dict[str, Any], str | None]:
        if not isinstance(blob, (bytes, bytearray)):
            raise Rejected("plugin_invalid_params")
        raw_req = bytes(blob)
        if len(raw_req) > req_cap:
            raise Rejected("plugin_blob_too_large")
        req_digest = sha256_hex(raw_req)
        clean_mime = sanitize_blob_meta_str(
            blob_mime_type, max_len=96, default="application/octet-stream"
        )
        clean_fname = sanitize_blob_meta_str(
            blob_filename, max_len=160, default="", is_filename=True
        )
        max_frame = self._max_frame_bytes()
        inline_cap = compute_inline_max_bytes(max_frame)
        if len(raw_req) <= inline_cap:
            candidate_desc = {
                "encoding": "inline_b64",
                "data_b64": encode_blob_chunk(raw_req),
                "byte_size": len(raw_req),
                "sha256": req_digest,
                "mime_type": clean_mime,
                "filename": clean_fname,
            }
            test_params = {**rpc_params, BLOB_FIELD: candidate_desc}
            try:
                encoded = json.dumps(
                    {
                        "type": "rpc.request",
                        "id": "0" * 32,
                        "method": "action.run",
                        "params": test_params,
                    },
                    ensure_ascii=False,
                    allow_nan=False,
                )
            except (TypeError, ValueError):
                raise Rejected("plugin_invalid_params") from None
            if len(encoded.encode("utf-8")) <= max_frame:
                return candidate_desc, None
        chunk_desc = await self._upload_blob_chunks(
            session,
            raw_req,
            digest=req_digest,
            mime_type=clean_mime,
            filename=clean_fname,
            deadline=deadline,
        )
        return chunk_desc, str(chunk_desc["transfer_id"])

    async def _extract_response_blob(
        self,
        session: _PluginSession,
        res_dict: dict[str, Any],
        *,
        resp_cap: int,
        deadline: float,
    ) -> tuple[bytes | None, str, str]:
        if BLOB_FIELD not in res_dict:
            return None, "", ""
        raw_blob_desc = res_dict.pop(BLOB_FIELD)
        raw_blob_id = (
            raw_blob_desc.get("blob_id")
            if isinstance(raw_blob_desc, dict) and isinstance(raw_blob_desc.get("blob_id"), str)
            else None
        )
        if resp_cap <= 0:
            if raw_blob_id:
                await self._best_effort_release_blob(session, blob_id=raw_blob_id)
            raise Rejected("plugin_blob_too_large")
        try:
            desc = validate_blob_descriptor(raw_blob_desc, max_bytes=resp_cap, id_field="blob_id")
        except ValueError as exc:
            if raw_blob_id:
                await self._best_effort_release_blob(session, blob_id=raw_blob_id)
            raise Rejected(
                "plugin_blob_too_large" if str(exc) == "blob_too_large" else "plugin_blob_invalid"
            ) from None

        resp_mime = desc["mime_type"]
        resp_filename = desc["filename"]
        if desc["encoding"] == "inline_b64":
            try:
                resp_blob = decode_blob_chunk(desc["data_b64"], max_raw_bytes=resp_cap)
            except ValueError as exc:
                raise Rejected(
                    "plugin_blob_too_large"
                    if str(exc) == "blob_too_large"
                    else "plugin_blob_corrupt"
                ) from None
            if len(resp_blob) != desc["byte_size"] or sha256_hex(resp_blob) != desc["sha256"]:
                raise Rejected("plugin_blob_corrupt")
        else:
            resp_blob = await self._download_blob_chunks(session, desc, deadline=deadline)
        return resp_blob, resp_mime, resp_filename
