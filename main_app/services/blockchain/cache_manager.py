"""
Tiered caching system for blockchain data.

Implements three cache tiers:
- L1: In-memory dict with LRU eviction (hot data)
- L2: functools.lru_cache (function-level caching)
- L3: S3 JSON storage (persistent cache)

Each tier has configurable TTL for different data types.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from functools import lru_cache, wraps
from typing import Any, Callable, Dict, Generic, Optional, TypeVar
from collections import OrderedDict
import threading
import hashlib

logger = logging.getLogger(__name__)

T = TypeVar('T')


class CacheTier(Enum):
    """Cache tier identifiers."""
    L1_MEMORY = "l1_memory"      # In-memory dict
    L2_LRU = "l2_lru"            # functools.lru_cache
    L3_S3 = "l3_s3"              # S3 storage


@dataclass
class TTLConfig:
    """TTL configuration for different data types (in seconds)."""
    ETH_PRICE: int = 300           # 5 minutes
    BLOCK_DATA: int = 3600         # 1 hour (immutable after confirmation)
    DECODED_TX: int = 86400        # 24 hours
    ABI: int = 604800              # 1 week
    TRANSACTION: int = 3600        # 1 hour
    RECEIPT: int = 3600            # 1 hour
    DEFAULT: int = 600             # 10 minutes fallback


@dataclass
class CacheEntry(Generic[T]):
    """
    A cached value with metadata.

    Tracks when the value was cached and its TTL for expiration.
    """
    key: str
    value: T
    cached_at: datetime
    ttl_seconds: int
    hits: int = 0
    tier: CacheTier = CacheTier.L1_MEMORY

    @property
    def is_expired(self) -> bool:
        """Check if this cache entry has expired."""
        if self.ttl_seconds <= 0:
            return False  # Never expires
        age = (datetime.now(timezone.utc) - self.cached_at).total_seconds()
        return age > self.ttl_seconds

    @property
    def age_seconds(self) -> float:
        """Get age of cache entry in seconds."""
        return (datetime.now(timezone.utc) - self.cached_at).total_seconds()

    def touch(self) -> None:
        """Increment hit counter."""
        self.hits += 1


class L1Cache:
    """
    Level 1 in-memory cache with LRU eviction.

    Fast access for frequently used data with automatic size management.
    """

    def __init__(self, max_size: int = 1000, default_ttl: int = 600):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.RLock()

        # Stats
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.

        Returns None if key not found or expired.
        """
        with self._lock:
            entry = self._cache.get(key)

            if entry is None:
                self._misses += 1
                return None

            if entry.is_expired:
                self._misses += 1
                del self._cache[key]
                return None

            # Move to end (most recently used)
            self._cache.move_to_end(key)
            entry.touch()
            self._hits += 1

            return entry.value

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> None:
        """
        Store value in cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Optional TTL in seconds (uses default if not specified)
        """
        with self._lock:
            # Evict oldest if at capacity
            while len(self._cache) >= self.max_size:
                self._cache.popitem(last=False)

            self._cache[key] = CacheEntry(
                key=key,
                value=value,
                cached_at=datetime.now(timezone.utc),
                ttl_seconds=ttl if ttl is not None else self.default_ttl,
                tier=CacheTier.L1_MEMORY,
            )

    def delete(self, key: str) -> bool:
        """Remove key from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> int:
        """Clear all entries. Returns count of entries cleared."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count

    def clear_expired(self) -> int:
        """Remove expired entries. Returns count of entries removed."""
        with self._lock:
            expired_keys = [
                key for key, entry in self._cache.items()
                if entry.is_expired
            ]
            for key in expired_keys:
                del self._cache[key]
            return len(expired_keys)

    def get_stats(self) -> Dict:
        """Get cache statistics."""
        with self._lock:
            total_requests = self._hits + self._misses
            hit_rate = (self._hits / total_requests * 100) if total_requests > 0 else 0

            return {
                "tier": "L1_MEMORY",
                "size": len(self._cache),
                "max_size": self.max_size,
                "utilization_pct": round(len(self._cache) / self.max_size * 100, 2),
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate_pct": round(hit_rate, 2),
            }


class CacheManager:
    """
    Multi-tier cache manager for blockchain data.

    Provides a unified interface to L1 (memory), L2 (lru_cache), and L3 (S3) caching.

    Usage:
        cache = CacheManager()

        # Simple get/set
        cache.set("eth_price_123", price, ttl=TTLConfig.ETH_PRICE)
        price = cache.get("eth_price_123")

        # With tier specification
        cache.set("decoded_tx_0x123", decoded, tier=CacheTier.L3_S3)

        # Decorator for function caching
        @cache.cached(ttl=TTLConfig.BLOCK_DATA)
        def get_block(block_number):
            return fetch_block(block_number)
    """

    def __init__(
        self,
        l1_max_size: int = 1000,
        l2_max_size: int = 10000,
        s3_client: Optional[Any] = None,
        s3_bucket: Optional[str] = None,
        s3_prefix: str = "cache/",
        ttl_config: Optional[TTLConfig] = None,
    ):
        """
        Initialize cache manager.

        Args:
            l1_max_size: Maximum entries in L1 memory cache
            l2_max_size: Maximum entries in L2 LRU cache
            s3_client: Optional boto3 S3 client for L3
            s3_bucket: S3 bucket name for L3 cache
            s3_prefix: S3 key prefix for cache objects
            ttl_config: TTL configuration for different data types
        """
        self.ttl = ttl_config or TTLConfig()

        # L1: In-memory with TTL
        self._l1 = L1Cache(max_size=l1_max_size, default_ttl=self.ttl.DEFAULT)

        # L2: Separate caches for different data types (using class methods)
        self._l2_max_size = l2_max_size
        self._l2_caches: Dict[str, Callable] = {}

        # L3: S3 configuration
        self._s3_client = s3_client
        self._s3_bucket = s3_bucket
        self._s3_prefix = s3_prefix

        # Global stats
        self._total_gets = 0
        self._total_sets = 0

    def get(
        self,
        key: str,
        tier: CacheTier = CacheTier.L1_MEMORY,
    ) -> Optional[Any]:
        """
        Get value from cache.

        For L1, checks L1 first.
        For L3, checks L1, then L3 (and populates L1 on hit).

        Args:
            key: Cache key
            tier: Which tier to check (L1_MEMORY or L3_S3)

        Returns:
            Cached value or None if not found
        """
        self._total_gets += 1

        # Always check L1 first
        value = self._l1.get(key)
        if value is not None:
            return value

        # If L3 requested, check S3
        if tier == CacheTier.L3_S3 and self._s3_client:
            value = self._get_from_s3(key)
            if value is not None:
                # Populate L1 for faster access next time
                self._l1.set(key, value)
                return value

        return None

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        tier: CacheTier = CacheTier.L1_MEMORY,
    ) -> None:
        """
        Store value in cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: TTL in seconds (uses default based on key type if not specified)
            tier: Which tier to store in
        """
        self._total_sets += 1

        # Infer TTL from key prefix if not specified
        if ttl is None:
            ttl = self._infer_ttl(key)

        # Always store in L1
        self._l1.set(key, value, ttl=ttl)

        # Also store in L3 if requested
        if tier == CacheTier.L3_S3 and self._s3_client:
            self._set_to_s3(key, value, ttl)

    def delete(self, key: str, tier: CacheTier = CacheTier.L1_MEMORY) -> None:
        """Delete key from cache tier(s)."""
        self._l1.delete(key)

        if tier == CacheTier.L3_S3 and self._s3_client:
            self._delete_from_s3(key)

    def clear(self, tier: Optional[CacheTier] = None) -> Dict[str, int]:
        """
        Clear cache entries.

        Args:
            tier: Specific tier to clear (None = all tiers)

        Returns:
            Dict with counts of cleared entries per tier
        """
        cleared = {}

        if tier is None or tier == CacheTier.L1_MEMORY:
            cleared["l1"] = self._l1.clear()

        if tier is None or tier == CacheTier.L2_LRU:
            # Clear all L2 caches
            for name, cache_func in self._l2_caches.items():
                if hasattr(cache_func, "cache_clear"):
                    cache_func.cache_clear()
            cleared["l2"] = len(self._l2_caches)

        return cleared

    def clear_expired(self) -> int:
        """Remove expired entries from L1."""
        return self._l1.clear_expired()

    def _infer_ttl(self, key: str) -> int:
        """Infer TTL from key prefix."""
        key_lower = key.lower()

        if key_lower.startswith("eth_price"):
            return self.ttl.ETH_PRICE
        elif key_lower.startswith("block_"):
            return self.ttl.BLOCK_DATA
        elif key_lower.startswith("decoded_"):
            return self.ttl.DECODED_TX
        elif key_lower.startswith("abi_"):
            return self.ttl.ABI
        elif key_lower.startswith("tx_") or key_lower.startswith("transaction_"):
            return self.ttl.TRANSACTION
        elif key_lower.startswith("receipt_"):
            return self.ttl.RECEIPT
        else:
            return self.ttl.DEFAULT

    def _get_from_s3(self, key: str) -> Optional[Any]:
        """Retrieve from S3 cache."""
        if not self._s3_client or not self._s3_bucket:
            return None

        s3_key = f"{self._s3_prefix}{self._hash_key(key)}.json"

        try:
            response = self._s3_client.get_object(
                Bucket=self._s3_bucket,
                Key=s3_key,
            )
            data = json.loads(response["Body"].read().decode("utf-8"))

            # Check expiration
            cached_at = datetime.fromisoformat(data.get("cached_at", ""))
            ttl = data.get("ttl", 0)
            age = (datetime.now(timezone.utc) - cached_at).total_seconds()

            if ttl > 0 and age > ttl:
                # Expired, delete from S3
                self._delete_from_s3(key)
                return None

            return data.get("value")

        except self._s3_client.exceptions.NoSuchKey:
            return None
        except Exception as e:
            logger.warning(f"Error reading from S3 cache: {e}")
            return None

    def _set_to_s3(self, key: str, value: Any, ttl: int) -> None:
        """Store in S3 cache."""
        if not self._s3_client or not self._s3_bucket:
            return

        s3_key = f"{self._s3_prefix}{self._hash_key(key)}.json"

        try:
            data = {
                "key": key,
                "value": value,
                "cached_at": datetime.now(timezone.utc).isoformat(),
                "ttl": ttl,
            }
            self._s3_client.put_object(
                Bucket=self._s3_bucket,
                Key=s3_key,
                Body=json.dumps(data, default=str),
                ContentType="application/json",
            )
        except Exception as e:
            logger.warning(f"Error writing to S3 cache: {e}")

    def _delete_from_s3(self, key: str) -> None:
        """Delete from S3 cache."""
        if not self._s3_client or not self._s3_bucket:
            return

        s3_key = f"{self._s3_prefix}{self._hash_key(key)}.json"

        try:
            self._s3_client.delete_object(
                Bucket=self._s3_bucket,
                Key=s3_key,
            )
        except Exception as e:
            logger.warning(f"Error deleting from S3 cache: {e}")

    def _hash_key(self, key: str) -> str:
        """Hash key for S3 storage (avoids special characters)."""
        return hashlib.sha256(key.encode()).hexdigest()[:32]

    def cached(
        self,
        ttl: Optional[int] = None,
        tier: CacheTier = CacheTier.L1_MEMORY,
        key_prefix: str = "",
    ) -> Callable:
        """
        Decorator for caching function results.

        Args:
            ttl: TTL in seconds
            tier: Which cache tier to use
            key_prefix: Optional prefix for cache keys

        Usage:
            @cache.cached(ttl=300, key_prefix="eth_price_")
            def get_eth_price(block_number):
                return fetch_price(block_number)
        """
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Generate cache key from function name and arguments
                key_parts = [key_prefix, func.__name__]
                key_parts.extend(str(arg) for arg in args)
                key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
                cache_key = ":".join(key_parts)

                # Try to get from cache
                cached_value = self.get(cache_key, tier=tier)
                if cached_value is not None:
                    return cached_value

                # Call function and cache result
                result = func(*args, **kwargs)
                self.set(cache_key, result, ttl=ttl, tier=tier)

                return result
            return wrapper
        return decorator

    def get_stats(self) -> Dict:
        """Get comprehensive cache statistics."""
        return {
            "l1": self._l1.get_stats(),
            "l2": {
                "registered_caches": len(self._l2_caches),
                "max_size": self._l2_max_size,
            },
            "l3": {
                "enabled": self._s3_client is not None,
                "bucket": self._s3_bucket,
                "prefix": self._s3_prefix,
            },
            "total_gets": self._total_gets,
            "total_sets": self._total_sets,
            "ttl_config": {
                "ETH_PRICE": self.ttl.ETH_PRICE,
                "BLOCK_DATA": self.ttl.BLOCK_DATA,
                "DECODED_TX": self.ttl.DECODED_TX,
                "ABI": self.ttl.ABI,
                "TRANSACTION": self.ttl.TRANSACTION,
                "RECEIPT": self.ttl.RECEIPT,
                "DEFAULT": self.ttl.DEFAULT,
            },
        }


# Convenience functions for L2 caching with lru_cache
def create_l2_cache(maxsize: int = 1000) -> Callable:
    """
    Create a reusable LRU cache decorator.

    Usage:
        cached_get_block = create_l2_cache(maxsize=1000)

        @cached_get_block
        def get_block(block_number):
            return w3.eth.get_block(block_number)
    """
    return lru_cache(maxsize=maxsize)


# Pre-configured L2 caches for common operations
@lru_cache(maxsize=10000)
def cached_get_transaction(w3, tx_hash: str):
    """Cached transaction lookup."""
    return w3.eth.get_transaction(tx_hash)


@lru_cache(maxsize=10000)
def cached_get_transaction_receipt(w3, tx_hash: str):
    """Cached receipt lookup."""
    return w3.eth.get_transaction_receipt(tx_hash)


@lru_cache(maxsize=5000)
def cached_get_block(w3, block_identifier):
    """Cached block lookup."""
    return w3.eth.get_block(block_identifier)
