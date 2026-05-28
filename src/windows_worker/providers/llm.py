from __future__ import annotations

from typing import Protocol

import aiohttp

from pipeline_core.errors import ErrorCode, PipelineError
from windows_worker.config import LLMSettings


class LLMClient(Protocol):
    is_local: bool

    async def summarize(self, merged_input: str) -> str:
        ...


class OpenAICompatibleLLMClient:
    is_local = False

    def __init__(self, settings: LLMSettings):
        self.settings = settings

    async def summarize(self, merged_input: str) -> str:
        if not self.settings.api_key:
            raise PipelineError(ErrorCode.CONFIG_INVALID, "LLM API key is empty", retryable=False)
        payload = {
            "model": self.settings.model,
            "messages": [
                {"role": "system", "content": self.settings.system_prompt},
                {"role": "user", "content": merged_input},
            ],
            "temperature": self.settings.temperature,
            "max_tokens": self.settings.max_output_tokens,
        }
        headers = {"Authorization": f"Bearer {self.settings.api_key}"}
        timeout = aiohttp.ClientTimeout(total=self.settings.timeout_seconds)
        proxy = self.settings.proxy.as_aiohttp_proxy()
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{self.settings.base_url.rstrip('/')}/chat/completions",
                    json=payload,
                    headers=headers,
                    proxy=proxy,
                ) as resp:
                    if resp.status == 429:
                        raise PipelineError(ErrorCode.LLM_RATE_LIMIT, "LLM rate limited")
                    if resp.status >= 500:
                        raise PipelineError(
                            ErrorCode.LLM_TIMEOUT,
                            f"LLM server error {resp.status}",
                        )
                    resp.raise_for_status()
                    data = await resp.json()
        except TimeoutError as exc:
            raise PipelineError(ErrorCode.LLM_TIMEOUT, "LLM request timed out") from exc
        return str(data["choices"][0]["message"]["content"])


class LocalOpenAICompatibleLLMClient(OpenAICompatibleLLMClient):
    is_local = True


class AnthropicLLMClient:
    is_local = False

    def __init__(self, settings: LLMSettings):
        self.settings = settings

    async def summarize(self, merged_input: str) -> str:
        if not self.settings.api_key:
            raise PipelineError(
                ErrorCode.CONFIG_INVALID,
                "Anthropic API key is empty",
                retryable=False,
            )
        payload = {
            "model": self.settings.model,
            "max_tokens": self.settings.max_output_tokens,
            "temperature": self.settings.temperature,
            "system": self.settings.system_prompt,
            "messages": [{"role": "user", "content": merged_input}],
        }
        headers = {
            "x-api-key": self.settings.api_key,
            "anthropic-version": "2023-06-01",
        }
        timeout = aiohttp.ClientTimeout(total=self.settings.timeout_seconds)
        proxy = self.settings.proxy.as_aiohttp_proxy()
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{self.settings.base_url.rstrip('/')}/messages",
                    json=payload,
                    headers=headers,
                    proxy=proxy,
                ) as resp:
                    if resp.status == 429:
                        raise PipelineError(ErrorCode.LLM_RATE_LIMIT, "Anthropic rate limited")
                    if resp.status >= 500:
                        raise PipelineError(
                            ErrorCode.LLM_TIMEOUT,
                            f"Anthropic server error {resp.status}",
                        )
                    resp.raise_for_status()
                    data = await resp.json()
        except TimeoutError as exc:
            raise PipelineError(ErrorCode.LLM_TIMEOUT, "Anthropic request timed out") from exc

        content = data.get("content", [])
        if isinstance(content, list):
            return "\n".join(
                str(item.get("text", "")) for item in content if isinstance(item, dict)
            )
        return str(data)


class MockLLMClient:
    is_local = True

    async def summarize(self, merged_input: str) -> str:
        preview = merged_input.splitlines()[1:6]
        return "群聊摘要：\n" + "\n".join(preview)


def build_llm_client(settings: LLMSettings) -> LLMClient:
    provider = settings.provider.lower()
    if provider in {"mock", "fake"}:
        return MockLLMClient()
    if provider in {"ollama", "lm_studio", "local_openai"}:
        return LocalOpenAICompatibleLLMClient(settings)
    if provider in {"anthropic", "claude"}:
        return AnthropicLLMClient(settings)
    return OpenAICompatibleLLMClient(settings)
