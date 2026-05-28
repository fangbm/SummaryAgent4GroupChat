from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    backoff_factor: float = 2.0


async def retry_async(
    func: Callable[[], Awaitable[T]],
    *,
    policy: RetryPolicy,
    retryable: Callable[[BaseException], bool] | None = None,
) -> T:
    last_exc: BaseException | None = None
    for attempt in range(policy.attempts):
        try:
            return await func()
        except BaseException as exc:
            last_exc = exc
            if attempt == policy.attempts - 1:
                break
            if retryable is not None and not retryable(exc):
                break
            delay = min(
                policy.base_delay_seconds * (policy.backoff_factor**attempt),
                policy.max_delay_seconds,
            )
            await asyncio.sleep(delay)
    assert last_exc is not None
    raise last_exc

