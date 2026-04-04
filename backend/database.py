import os
import uuid
import datetime
import asyncpg
from typing import AsyncGenerator


def serialise(row) -> dict:
    """Convert asyncpg Record to JSON-safe dict.
    Converts UUID, date, datetime, Decimal to str/float for FastAPI."""
    import decimal
    out = {}
    for k, v in dict(row).items():
        if v is None:
            out[k] = None
        elif isinstance(v, uuid.UUID):
            out[k] = str(v)
        elif isinstance(v, (datetime.datetime, datetime.date)):
            out[k] = v.isoformat()
        elif isinstance(v, decimal.Decimal):
            out[k] = float(v)
        else:
            out[k] = v
    return out

DATABASE_URL = os.getenv("DATABASE_URL")


class Database:
    pool: asyncpg.Pool = None

    async def connect(self):
        if self.pool is not None:
            return
        # Parse URL and add SSL
        self.pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=5,
            command_timeout=30,
            ssl="require",
            server_settings={"application_name": "zy-invest-api"},
        )

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def _ensure_connected(self):
        if self.pool is None:
            await self.connect()

    async def fetch(self, query: str, *args):
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args):
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def execute(self, query: str, *args):
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def executemany(self, query: str, args_list):
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            return await conn.executemany(query, args_list)


engine = Database()


async def get_db() -> AsyncGenerator[Database, None]:
    yield engine
