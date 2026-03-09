"""
core/storage/cache.py
---------------------
Redis caching layer for the DataFlow API.

WHY CACHING MATTERS IN FINTECH:
- Database queries on time-series data can be slow
- The same AAPL/1d endpoint might be hit 1000 times/second
- Cache hot data in Redis (in-memory, microsecond latency)
- Only hit PostgreSQL for cold/uncached data

THIS PATTERN: Cache-aside (Lazy Loading)
1. Check Redis first
2. If cache miss → query DB → store in Redis
3. Next request → served from Redis

TTL (Time to Live) = data expires automatically
"""

import json
import hashlib
from typing import Optional, Any
from datetime import datetime
import redis

from core.settings import settings
from core.logger import get_logger

logger = get_logger(__name__)


class CacheClient:
    """
    Wrapper around Redis with JSON serialization and error handling.
    All operations are fail-safe — cache errors never crash the API.
    """

    def __init__(self):
        self._client: Optional[redis.Redis] = None

    @property
    def client(self) -> redis.Redis:
        """Lazy connection — only connects when first used."""
        if self._client is None:
            self._client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=0,
                decode_responses=True,  # Return str instead of bytes
                socket_connect_timeout=5,
                socket_timeout=5,
            )
        return self._client

    def get(self, key: str) -> Optional[Any]:
        """Retrieves and deserializes a cached value. Returns None on miss."""
        try:
            raw = self.client.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except redis.RedisError as e:
            logger.warning(f"Redis GET failed, bypassing cache", extra={"key": key, "error": str(e)})
            return None

    def set(self, key: str, value: Any, ttl_seconds: int = 3600) -> bool:
        """Serializes and stores a value with expiry."""
        try:
            serialized = json.dumps(value, default=str)  # default=str handles datetime
            self.client.setex(key, ttl_seconds, serialized)
            return True
        except redis.RedisError as e:
            logger.warning(f"Redis SET failed, skipping cache", extra={"key": key, "error": str(e)})
            return False

    def delete(self, key: str) -> bool:
        """Invalidates a cache entry."""
        try:
            self.client.delete(key)
            return True
        except redis.RedisError:
            return False

    def delete_pattern(self, pattern: str) -> int:
        """
        Deletes all keys matching a pattern.
        e.g. delete_pattern("ohlcv:AAPL:*") clears all AAPL data
        """
        try:
            keys = self.client.keys(pattern)
            if keys:
                return self.client.delete(*keys)
            return 0
        except redis.RedisError as e:
            logger.warning(f"Redis pattern delete failed", extra={"pattern": pattern, "error": str(e)})
            return 0

    def health_check(self) -> bool:
        """Returns True if Redis is reachable."""
        try:
            return self.client.ping()
        except redis.RedisError:
            return False


def make_cache_key(*parts: str) -> str:
    """
    Creates a consistent, collision-resistant cache key.

    For simple keys: make_cache_key("ohlcv", "AAPL", "1d")
    → "ohlcv:AAPL:1d"

    For complex params (query strings etc), hash them:
    make_cache_key("ohlcv", "AAPL", hash_params({"start": "2024-01-01"}))
    """
    return ":".join(str(p) for p in parts)


def hash_params(params: dict) -> str:
    """Creates a short hash of a params dict for cache keys."""
    serialized = json.dumps(params, sort_keys=True)
    return hashlib.md5(serialized.encode()).hexdigest()[:8]


# Singleton cache client
cache = CacheClient()
