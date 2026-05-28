from __future__ import annotations

import asyncio
import base64
import hashlib
from pathlib import Path
from typing import Protocol

import aiohttp

from pipeline_core.errors import ErrorCode, PipelineError
from windows_worker.config import ImageGenSettings

PLACEHOLDER_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII="
)


class GeneratedImage:
    def __init__(self, path: Path):
        self.path = path
        self.bytes = path.read_bytes()
        self.sha256 = hashlib.sha256(self.bytes).hexdigest()
        self.size_bytes = len(self.bytes)


class ImageGenClient(Protocol):
    async def generate(self, summary: str, output_path: Path) -> GeneratedImage:
        ...


class OpenAIImageGenClient:
    def __init__(self, settings: ImageGenSettings):
        self.settings = settings

    async def generate(self, summary: str, output_path: Path) -> GeneratedImage:
        if not self.settings.api_key:
            raise PipelineError(ErrorCode.CONFIG_INVALID, "Image API key is empty", retryable=False)
        prompt = self.settings.prompt_template.format(summary=summary)
        payload = {
            "model": self.settings.model,
            "prompt": prompt,
            "size": self.settings.size,
            "quality": self.settings.quality,
            "n": 1,
        }
        headers = {"Authorization": f"Bearer {self.settings.api_key}"}
        timeout = aiohttp.ClientTimeout(total=self.settings.timeout_seconds)
        proxy = self.settings.proxy.as_aiohttp_proxy()
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{self.settings.base_url.rstrip('/')}/images/generations",
                    json=payload,
                    headers=headers,
                    proxy=proxy,
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                image = data["data"][0]
                if "b64_json" in image:
                    content = base64.b64decode(image["b64_json"])
                elif "url" in image:
                    async with session.get(image["url"], proxy=proxy) as resp:
                        resp.raise_for_status()
                        content = await resp.read()
                else:
                    raise PipelineError(
                        ErrorCode.IMAGE_GEN_FAILED,
                        "Image API returned no image data",
                    )
        except PipelineError:
            raise
        except Exception as exc:
            raise PipelineError(ErrorCode.IMAGE_GEN_FAILED, str(exc), retryable=True) from exc
        await asyncio.to_thread(output_path.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(output_path.write_bytes, content)
        return GeneratedImage(output_path)


class PlaceholderImageGenClient:
    async def generate(self, summary: str, output_path: Path) -> GeneratedImage:
        await asyncio.to_thread(output_path.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(output_path.write_bytes, PLACEHOLDER_PNG)
        return GeneratedImage(output_path)


def build_image_client(settings: ImageGenSettings) -> ImageGenClient:
    if not settings.enabled or settings.provider.lower() in {"mock", "placeholder", "fake"}:
        return PlaceholderImageGenClient()
    return OpenAIImageGenClient(settings)
