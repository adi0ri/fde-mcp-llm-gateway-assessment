import asyncio
import sqlite3

import pytest

from gateway_lab.limiter import RateLimited, TokenLimiter


async def test_window_boundary_settlement_tenant_isolation_and_restart(tmp_path):
    now = [1000.0]
    path = tmp_path / "quota.sqlite3"
    limiter = TokenLimiter(path, limit=100, clock=lambda: now[0])
    await limiter.initialize()
    first = await limiter.reserve("tenant-a", 80)
    await limiter.reserve("tenant-b", 100)
    with pytest.raises(RateLimited) as caught:
        await limiter.reserve("tenant-a", 30)
    assert caught.value.retry_after == 60
    await limiter.settle(first, 40)
    await limiter.reserve("tenant-a", 60)
    restored = TokenLimiter(path, limit=100, clock=lambda: now[0])
    await restored.initialize()
    now[0] = 1059.999
    with pytest.raises(RateLimited) as caught:
        await restored.reserve("tenant-a", 1)
    assert caught.value.retry_after == 1
    now[0] = 1060.0
    await restored.reserve("tenant-a", 100)
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM reservations").fetchone()[0] == 1


async def test_concurrent_independent_instances_cannot_oversubscribe(tmp_path):
    path = tmp_path / "quota.sqlite3"
    instances = [TokenLimiter(path, limit=100, clock=lambda: 1000) for _ in range(20)]
    await instances[0].initialize()

    async def attempt(limiter):
        try:
            return await limiter.reserve("same-api-key", 10)
        except RateLimited:
            return None

    results = await asyncio.gather(*(attempt(limiter) for limiter in instances))
    assert sum(result is not None for result in results) == 10
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT SUM(tokens) FROM reservations").fetchone()[0] == 100


async def test_retry_after_accounts_for_required_capacity(tmp_path):
    now = [1000.0]
    limiter = TokenLimiter(tmp_path / "quota.sqlite3", limit=100, clock=lambda: now[0])
    await limiter.initialize()
    await limiter.reserve("a", 20)
    now[0] += 10
    await limiter.reserve("a", 80)
    with pytest.raises(RateLimited) as caught:
        await limiter.reserve("a", 90)
    assert caught.value.retry_after == 60  # Expiring only the first 20 is insufficient.


async def test_backward_clock_does_not_reopen_quota(tmp_path):
    now = [1000.0]
    limiter = TokenLimiter(tmp_path / "quota.sqlite3", limit=10, clock=lambda: now[0])
    await limiter.initialize()
    await limiter.reserve("a", 10)
    now[0] = 900
    with pytest.raises(RateLimited):
        await limiter.reserve("a", 1)


@pytest.mark.parametrize("tokens", [0, -1, True, 1.5])
async def test_bad_token_counts_rejected(tmp_path, tokens):
    limiter = TokenLimiter(tmp_path / "quota.sqlite3")
    await limiter.initialize()
    with pytest.raises(ValueError):
        await limiter.reserve("a", tokens)
