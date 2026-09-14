"""Transactional, rolling 60-second token accounting in an on-disk SQLite DB."""

import math
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite


class RateLimited(Exception):
    def __init__(self, retry_after: int):
        self.retry_after = retry_after
        super().__init__("Token budget exhausted")


class TokenLimiter:
    def __init__(self, database: Path, limit: int = 50_000, window: float = 60, clock=time.time):
        if str(database) == ":memory:" or limit <= 0 or window <= 0:
            raise ValueError("Use an on-disk database and positive limits")
        self.database, self.limit, self.window, self.clock = database, limit, window, clock

    async def initialize(self):
        self.database.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.database) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS reservations (
                    id TEXT PRIMARY KEY, tenant TEXT NOT NULL,
                    at REAL NOT NULL, tokens INTEGER NOT NULL CHECK(tokens >= 0)
                );
                CREATE INDEX IF NOT EXISTS quota_tenant_time ON reservations(tenant, at);
                CREATE INDEX IF NOT EXISTS quota_time ON reservations(at);
                CREATE TABLE IF NOT EXISTS quota_clock (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1), last REAL NOT NULL
                );
                INSERT OR IGNORE INTO quota_clock VALUES(1, 0);
            """)
            await db.commit()

    @asynccontextmanager
    async def _transaction(self):
        async with aiosqlite.connect(self.database, timeout=5) as db:
            await db.execute("PRAGMA busy_timeout=5000")
            await db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                await db.commit()
            except BaseException:
                await db.rollback()
                raise

    async def reserve(self, tenant: str, tokens: int) -> str:
        if type(tokens) is not int or tokens <= 0:
            raise ValueError("Tokens must be a positive integer")
        if tokens > self.limit:
            raise RateLimited(math.ceil(self.window))
        async with self._transaction() as db:
            row = await (
                await db.execute("SELECT last FROM quota_clock WHERE singleton=1")
            ).fetchone()
            # Persisted clock floor avoids reopening budgets if the wall clock moves backwards.
            now = max(self.clock(), row[0])
            await db.execute("UPDATE quota_clock SET last=? WHERE singleton=1", (now,))
            await db.execute("DELETE FROM reservations WHERE at <= ?", (now - self.window,))
            rows = await (
                await db.execute(
                    "SELECT at, tokens FROM reservations WHERE tenant=? ORDER BY at", (tenant,)
                )
            ).fetchall()
            total = sum(row[1] for row in rows)
            if total + tokens > self.limit:
                need = total + tokens - self.limit
                for at, used in rows:
                    need -= used
                    if need <= 0:
                        raise RateLimited(max(1, math.ceil(at + self.window - now)))
            reservation = uuid.uuid4().hex
            await db.execute(
                "INSERT INTO reservations VALUES(?,?,?,?)", (reservation, tenant, now, tokens)
            )
            return reservation

    async def settle(self, reservation: str, actual_tokens: int):
        if type(actual_tokens) is not int or actual_tokens < 0:
            raise ValueError("Usage must be a nonnegative integer")
        async with self._transaction() as db:
            # Keep the admission timestamp. A completed request consumes a sliding-window event.
            # Provider adapters must enforce the reservation's input + max_output contract.
            await db.execute(
                "UPDATE reservations SET tokens=? WHERE id=?", (actual_tokens, reservation)
            )
