from pipeline_core.retry import RetryPolicy, retry_async


async def test_retry_async_retries_until_success() -> None:
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise RuntimeError("try again")
        return "ok"

    result = await retry_async(
        flaky,
        policy=RetryPolicy(attempts=2, base_delay_seconds=0),
    )
    assert result == "ok"
    assert attempts == 2
