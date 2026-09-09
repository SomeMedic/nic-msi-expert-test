"""Transport readiness checks; a healthy socket is not an inference warmup."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Protocol

import boto3
from botocore.config import Config
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from expert_clients.settings import Settings


@dataclass(frozen=True)
class DependencyHealth:
    name: str
    ready: bool


class StorageTransport(Protocol):
    def head_bucket(self, *, Bucket: str) -> Any: ...
    def close(self) -> None: ...


class Dependencies:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pool: AsyncConnectionPool | None = None
        self.redis: Redis | None = None
        self.storage: StorageTransport | None = None
        self.models_ready = False

    async def open(self) -> None:
        if self.settings.database_dsn:
            self.pool = AsyncConnectionPool(
                conninfo=self.settings.database_dsn.get_secret_value(), min_size=0, max_size=6,
                timeout=3, open=False, kwargs={"autocommit": True},
            )
            await self.pool.open()
        if self.settings.redis_url:
            self.redis = Redis.from_url(self.settings.redis_url.get_secret_value(), socket_connect_timeout=3, socket_timeout=3, decode_responses=True)
        if self.settings.service_name in {"backend", "ingestion-worker"}:
            self.storage = boto3.client(
                "s3", endpoint_url=str(self.settings.s3_endpoint),
                aws_access_key_id=self.settings.require_secret("s3_access_key").get_secret_value(),
                aws_secret_access_key=self.settings.require_secret("s3_secret_key").get_secret_value(),
                region_name="us-east-1",
                config=Config(connect_timeout=3, read_timeout=3, retries={"max_attempts": 0}, s3={"addressing_style": "path"}),
            )

    async def close(self) -> None:
        try:
            if self.pool is not None:
                await self.pool.close()
        finally:
            try:
                if self.redis is not None:
                    await self.redis.aclose()
            finally:
                if self.storage is not None:
                    self.storage.close()

    async def check(self) -> list[DependencyHealth]:
        async def probe(name: str, operation):
            try:
                async with asyncio.timeout(4):
                    await operation()
                return DependencyHealth(name, True)
            except Exception:
                return DependencyHealth(name, False)

        async def postgres():
            pool = self.pool
            if pool is None:
                raise RuntimeError("Database transport is not initialized")
            async with pool.connection() as connection:
                await connection.execute("SELECT 1")

        async def storage():
            storage_client = self.storage
            if storage_client is None:
                raise RuntimeError("Storage transport is not initialized")
            # Individual bucket permissions suffice; ListAllMyBuckets is not required.
            for bucket in (self.settings.s3_bucket_originals, self.settings.s3_bucket_artifacts):
                await asyncio.to_thread(storage_client.head_bucket, Bucket=bucket)

        checks = []
        if self.pool is not None:
            checks.append(probe("database", postgres))
        if self.redis is not None:
            checks.append(probe("event_transport", self.redis.ping))
        if self.storage is not None:
            checks.append(probe("object_storage", storage))
        results = list(await asyncio.gather(*checks))
        if self.settings.service_name == "retrieval-ml":
            results.append(DependencyHealth("local_models", self.models_ready))
        return results
