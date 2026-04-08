"""MongoDB async client and leaderboard helpers."""

import os
from typing import Any, Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection

_client: Optional[AsyncIOMotorClient] = None
_db: Optional[AsyncIOMotorDatabase] = None


def get_mongodb_url() -> str:
    return os.getenv("MONGODB_URL", "mongodb://localhost:27017")


def get_db_name() -> str:
    return os.getenv("MONGODB_DB", "mathquiz")


async def connect() -> None:
    global _client, _db
    if _client is None:
        _client = AsyncIOMotorClient(get_mongodb_url())
        _db = _client[get_db_name()]


async def disconnect() -> None:
    global _client, _db
    if _client is not None:
        _client.close()
        _client = None
        _db = None


def db() -> AsyncIOMotorDatabase:
    if _db is None:
        raise RuntimeError("MongoDB not connected.")
    return _db


def leaderboard_collection() -> AsyncIOMotorCollection:
    return db()["leaderboard"]


async def increment_score(username: str, delta: int = 10) -> None:
    await leaderboard_collection().update_one(
        {"username": username},
        {"$inc": {"score": delta}},
        upsert=True,
    )


async def top_leaderboard(limit: int = 10) -> list[dict[str, Any]]:
    cursor = (
        leaderboard_collection()
        .find({}, {"_id": 0, "username": 1, "score": 1})
        .sort("score", -1)
        .limit(limit)
    )
    return await cursor.to_list(length=limit)
