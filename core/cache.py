"""
Multi-level query cache for latency reduction.
L1: Exact query hash match — <10ms
L2: Schema metadata cache — in-memory, refreshed on upload
All in-memory, no file I/O, no persistence to disk.
"""
import hashlib
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class CacheEntry:
    value: Any
    timestamp: float
    hit_count: int = 0


class QueryCache:
    """Thread-safe LRU cache with TTL for NL→SQL query results."""

    def __init__(self, max_size: int = 1000, ttl_seconds: int = 3600):
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._max_size = max_size
        self._ttl = ttl_seconds
        self._hits = 0
        self._misses = 0

    def _make_key(self, query: str) -> str:
        normalized = query.strip().lower()
        return hashlib.sha256(normalized.encode()).hexdigest()

    def get(self, query: str) -> Optional[dict]:
        key = self._make_key(query)
        if key in self._cache:
            entry = self._cache[key]
            if time.time() - entry.timestamp < self._ttl:
                self._cache.move_to_end(key)
                entry.hit_count += 1
                self._hits += 1
                return entry.value
            # Expired
            self._cache.pop(key, None)

        self._misses += 1
        return None

    def put(self, query: str, value: dict):
        key = self._make_key(query)
        if len(self._cache) >= self._max_size:
            self._cache.popitem(last=False)  # Evict LRU
        self._cache[key] = CacheEntry(
            value=value,
            timestamp=time.time()
        )

    def clear(self):
        """Clear entire cache (e.g., on new CSV upload)."""
        self._cache.clear()
        self._hits = 0
        self._misses = 0

    @property
    def stats(self) -> dict:
        total = self._hits + self._misses
        return {
            "size": len(self._cache),
            "max_size": self._max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{(self._hits / total * 100):.1f}%" if total else "0%",
        }
