"""Tile client — async-friendly tile CRUD with retry, caching, and batch operations."""
import time
import json
from dataclasses import dataclass, field
from typing import Optional, Any
from collections import defaultdict

@dataclass
class ClientConfig:
    base_url: str = ""
    api_key: str = ""
    timeout: float = 10.0
    max_retries: int = 3
    retry_delay: float = 1.0
    cache_ttl: float = 60.0
    batch_size: int = 50

@dataclass
class ClientResponse:
    status: int = 200
    data: Any = None
    error: str = ""
    latency_ms: float = 0.0
    cached: bool = False
    retries: int = 0

class TileClient:
    def __init__(self, config: ClientConfig = None):
        self.config = config or ClientConfig()
        self._cache: dict[str, tuple[float, Any]] = {}  # key -> (expires, data)
        self._stats = {"requests": 0, "cache_hits": 0, "errors": 0, "retries": 0}

    def _cache_get(self, key: str) -> Optional[Any]:
        if key in self._cache:
            expires, data = self._cache[key]
            if time.time() < expires:
                self._stats["cache_hits"] += 1
                return data
            del self._cache[key]
        return None

    def _cache_set(self, key: str, data: Any):
        self._cache[key] = (time.time() + self.config.cache_ttl, data)

    def _cache_invalidate(self, key: str):
        self._cache.pop(key, None)

    def create(self, tile: dict) -> ClientResponse:
        start = time.time()
        self._stats["requests"] += 1
        tile.setdefault("created_at", time.time())
        tile.setdefault("version", 1)
        tile.setdefault("confidence", 0.5)
        if "id" in tile:
            self._cache_invalidate(tile["id"])
        resp = ClientResponse(status=201, data=tile, latency_ms=(time.time() - start) * 1000)
        return resp

    def get(self, tile_id: str) -> ClientResponse:
        start = time.time()
        self._stats["requests"] += 1
        cached = self._cache_get(tile_id)
        if cached:
            return ClientResponse(status=200, data=cached, cached=True,
                                latency_ms=(time.time() - start) * 1000)
        # Simulate fetch with retry
        for attempt in range(self.config.max_retries):
            resp = ClientResponse(status=200, data={"id": tile_id, "content": "", "confidence": 0.5},
                                latency_ms=(time.time() - start) * 1000, retries=attempt)
            if resp.status == 200:
                self._cache_set(tile_id, resp.data)
                return resp
            self._stats["retries"] += 1
            time.sleep(self.config.retry_delay * (attempt + 1))
        self._stats["errors"] += 1
        return ClientResponse(status=503, error="Max retries exceeded",
                            latency_ms=(time.time() - start) * 1000)

    def update(self, tile_id: str, updates: dict) -> ClientResponse:
        start = time.time()
        self._stats["requests"] += 1
        self._cache_invalidate(tile_id)
        updates.setdefault("updated_at", time.time())
        resp = ClientResponse(status=200, data={"id": tile_id, **updates},
                            latency_ms=(time.time() - start) * 1000)
        return resp

    def delete(self, tile_id: str) -> ClientResponse:
        start = time.time()
        self._stats["requests"] += 1
        self._cache_invalidate(tile_id)
        return ClientResponse(status=204, data=None, latency_ms=(time.time() - start) * 1000)

    def search(self, query: str, domain: str = "", limit: int = 20) -> ClientResponse:
        start = time.time()
        self._stats["requests"] += 1
        results = {"query": query, "domain": domain, "limit": limit, "results": []}
        return ClientResponse(status=200, data=results, latency_ms=(time.time() - start) * 1000)

    def batch_create(self, tiles: list[dict]) -> list[ClientResponse]:
        results = []
        for i in range(0, len(tiles), self.config.batch_size):
            batch = tiles[i:i + self.config.batch_size]
            for tile in batch:
                results.append(self.create(tile))
        return results

    def batch_get(self, tile_ids: list[str]) -> list[ClientResponse]:
        return [self.get(tid) for tid in tile_ids]

    def clear_cache(self):
        self._cache.clear()

    def cache_stats(self) -> dict:
        valid = sum(1 for exp, _ in self._cache.values() if time.time() < exp)
        return {"entries": len(self._cache), "valid": valid, "ttl": self.config.cache_ttl}

    @property
    def stats(self) -> dict:
        return {**self._stats, "cache": self.cache_stats(),
                "config": {"timeout": self.config.timeout, "retries": self.config.max_retries}}
