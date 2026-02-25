import os
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional, Any

import asyncpg

logger = logging.getLogger("twomoon.database")

_pool: Optional[asyncpg.Pool] = None
_pool_lock = asyncio.Lock()

CRDB_SERIALIZATION_RETRY = "40001"
MAX_RETRIES = 3
RETRY_BASE_DELAY = 0.1


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is not None and not _pool._closed:
        return _pool

    async with _pool_lock:
        if _pool is not None and not _pool._closed:
            return _pool

        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL environment variable is not set")

        _pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=2,
            max_size=8,
            max_inactive_connection_lifetime=300.0,
            command_timeout=30.0,
            statement_cache_size=0,
            server_settings={
                "application_name": "twomoon_core",
            },
        )
        logger.info("CockroachDB connection pool initialized (min=2, max=8)")
        return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None and not _pool._closed:
        await _pool.close()
        _pool = None
        logger.info("CockroachDB connection pool closed")


@asynccontextmanager
async def acquire():
    pool = await get_pool()
    async with pool.acquire() as conn:
        yield conn


async def execute(query: str, *args: Any, timeout: Optional[float] = None) -> str:
    pool = await get_pool()
    return await pool.execute(query, *args, timeout=timeout)


async def fetch(query: str, *args: Any, timeout: Optional[float] = None) -> list[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch(query, *args, timeout=timeout)


async def fetchrow(query: str, *args: Any, timeout: Optional[float] = None) -> Optional[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetchrow(query, *args, timeout=timeout)


async def fetchval(query: str, *args: Any, column: int = 0, timeout: Optional[float] = None) -> Any:
    pool = await get_pool()
    return await pool.fetchval(query, *args, column=column, timeout=timeout)


async def executemany(query: str, args: list[tuple], timeout: Optional[float] = None) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.executemany(query, args, timeout=timeout)


async def execute_with_retry(query: str, *args: Any, max_retries: int = MAX_RETRIES) -> str:
    pool = await get_pool()
    for attempt in range(max_retries):
        try:
            return await pool.execute(query, *args)
        except asyncpg.SerializationError:
            if attempt == max_retries - 1:
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            logger.warning(
                "CockroachDB serialization conflict (attempt %d/%d), retrying in %.2fs",
                attempt + 1,
                max_retries,
                delay,
            )
            await asyncio.sleep(delay)
    raise RuntimeError("Unreachable: retry loop exited without return or raise")


@asynccontextmanager
async def transaction():
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            yield conn


async def apply_schema(schema_path: str) -> None:
    pool = await get_pool()
    with open(schema_path, "r") as f:
        sql = f.read()
    async with pool.acquire() as conn:
        await conn.execute(sql)
    logger.info("Schema applied from %s", schema_path)
