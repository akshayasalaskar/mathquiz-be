"""Async Redis connection helper."""

import os
from typing import Optional

import redis.asyncio as redis

_redis: Optional[redis.Redis] = None


def get_redis_url() -> str:
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return url


async def connect() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.from_url(get_redis_url(), decode_responses=True)
    return _redis


async def disconnect() -> None:
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None


def redis_conn() -> redis.Redis:
    if _redis is None:
        raise RuntimeError("Redis not connected. Call connect() during app lifespan.")
    return _redis
