from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import aiohttp

from pipeline_core.errors import ErrorCode, PipelineError


async def download_image(
    url: str,
    output_dir: str | Path,
    *,
    filename: str,
    timeout_seconds: int,
    checksum_sha256: str | None = None,
) -> Path:
    target_dir = Path(output_dir)
    await asyncio.to_thread(target_dir.mkdir, parents=True, exist_ok=True)
    target = target_dir / filename
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                data = await resp.read()
    except Exception as exc:
        raise PipelineError(ErrorCode.FILE_TRANSFER_FAILED, str(exc), retryable=True) from exc
    if checksum_sha256 and hashlib.sha256(data).hexdigest() != checksum_sha256:
        raise PipelineError(ErrorCode.FILE_TRANSFER_FAILED, "Downloaded image checksum mismatch")
    await asyncio.to_thread(target.write_bytes, data)
    return target
