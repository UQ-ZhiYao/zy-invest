"""
Database connection pool — async PostgreSQL via asyncpg + Supabase
"""
import os
import asyncpg
from typing import AsyncGenerator

# Loaded from environment variables (set in Render dashboard)
DATABASE_URL = os.getenv("DATABASE_URL")  # Supabase connection string
# Format: postgresql://postgres:[password]@[host]:5432/postgres


class Database:
    pool: asyncpg.Pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=10,
            command_timeout=60,
            ssl="require",  # Supabase requires SSL
        )

    async def disconnect(self):
        if self.pool:
            await self.pool.close()

    async def fetch(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def execute(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def executemany(self, query: str, args_list):
        async with self.pool.acquire() as conn:
            return await conn.executemany(query, args_list)


engine = Database()


async def get_db() -> AsyncGenerator[Database, None]:
    """FastAPI dependency — inject DB into route handlers"""
    yield engine
