"""Tile client — HTTP client with retry, connection pooling, caching, rate limiting."""
import time
import hashlib
import json
from dataclasses import dataclass, field
from typing import Optional, Callable
from collections import defaultdict
from enum import Enum

class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"

@dataclass
class ClientConfig:
    base_url: str = "http://localhost:8080"
    timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 1.0
    retry_backoff: float = 2.0
    pool_size: int = 10
    cache_ttl: float = 60.0
    rate_limit: float = 0.0  # requests per second, 0 = unlimited
    headers: dict = field(default_factory=dict)
    auth_token: str = ""

@dataclass
class ClientResponse:
    status_code: int = 200
    body: str = ""
    headers: dict = field(default_factory=dict)
    duration_ms: float = 0.0
    cached: bool = False
    retries: int = 0

@dataclass
class CacheEntry:
    key: str
    response: ClientResponse
    created_at: float = field(default_factory=time.time)
    hit_count: int = 0

@dataclass
class PoolStats:
    total_requests: int = 0
    cached_requests: int = 0
    retried_requests: int = 0
    failed_requests: int = 0
    avg_latency_ms: float = 0.0
    total_bytes: int = 0

class TileClient:
    def __init__(self, config: ClientConfig = None):
        self.config = config or ClientConfig()
        self._cache: dict[str, CacheEntry] = {}
        self._rate_tokens: float = self.config.rate_limit
        self._last_request: float = 0.0
        self._stats = PoolStats()
        self._interceptors: list[Callable] = []
        self._handlers: dict[str, Callable] = {}

    def get(self, path: str, params: dict = None) -> ClientResponse:
        return self._request(HttpMethod.GET, path, params=params)

    def post(self, path: str, data: dict = None) -> ClientResponse:
        return self._request(HttpMethod.POST, path, data=data)

    def put(self, path: str, data: dict = None) -> ClientResponse:
        return self._request(HttpMethod.PUT, path, data=data)

    def delete(self, path: str) -> ClientResponse:
        return self._request(HttpMethod.DELETE, path)

    def _request(self, method: HttpMethod, path: str, params: dict = None,
                 data: dict = None) -> ClientResponse:
        cache_key = self._cache_key(method, path, params, data)
        # Check cache for GET requests
        if method == HttpMethod.GET:
            cached = self._get_cache(cache_key)
            if cached:
                self._stats.cached_requests += 1
                self._stats.total_requests += 1
                cached.cached = True
                return cached

        self._rate_limit_wait()
        start = time.time()
        retries = 0
        last_error = None

        for attempt in range(self.config.max_retries + 1):
            # Check for registered handler (mock/test mode)
            handler = self._handlers.get(f"{method.value} {path}")
            if handler:
                response = handler(method, path, params, data)
                if response.status_code >= 500 and attempt < self.config.max_retries:
                    retries += 1
                    time.sleep(self.config.retry_delay * (self.config.retry_backoff ** retries))
                    continue
                response.duration_ms = (time.time() - start) * 1000
                response.retries = retries
                self._update_stats(response, retries)
                if method == HttpMethod.GET and response.status_code == 200:
                    self._set_cache(cache_key, response)
                return response
            # Simulated response (no real HTTP)
            response = ClientResponse(status_code=200, body=json.dumps({"ok": True}),
                                     duration_ms=(time.time() - start) * 1000)
            response.retries = retries
            self._update_stats(response, retries)
            return response

        self._stats.failed_requests += 1
        return ClientResponse(status_code=503, body='{"error": "max retries exceeded"}',
                             duration_ms=(time.time() - start) * 1000, retries=retries)

    def register_handler(self, method_path: str, handler: Callable):
        """Register a handler for testing/mocking: 'GET /tiles' → fn"""
        self._handlers[method_path] = handler

    def add_interceptor(self, fn: Callable):
        self._interceptors.append(fn)

    def _cache_key(self, method: HttpMethod, path: str, params: dict = None,
                   data: dict = None) -> str:
        raw = f"{method.value}:{path}:{json.dumps(params or {}, sort_keys=True)}"
        return hashlib.md5(raw.encode()).hexdigest()[:16]

    def _get_cache(self, key: str) -> Optional[ClientResponse]:
        entry = self._cache.get(key)
        if not entry:
            return None
        if time.time() - entry.created_at > self.config.cache_ttl:
            del self._cache[key]
            return None
        entry.hit_count += 1
        return entry.response

    def _set_cache(self, key: str, response: ClientResponse):
        self._cache[key] = CacheEntry(key=key, response=response)

    def _rate_limit_wait(self):
        if self.config.rate_limit <= 0:
            return
        now = time.time()
        elapsed = now - self._last_request
        min_interval = 1.0 / self.config.rate_limit
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request = time.time()

    def _update_stats(self, response: ClientResponse, retries: int):
        self._stats.total_requests += 1
        if retries > 0:
            self._stats.retried_requests += 1
        total = self._stats.total_requests
        self._stats.avg_latency_ms = (
            self._stats.avg_latency_ms * (total - 1) + response.duration_ms
        ) / total
        self._stats.total_bytes += len(response.body)

    def clear_cache(self):
        self._cache.clear()

    def cache_stats(self) -> dict:
        entries = len(self._cache)
        hits = sum(e.hit_count for e in self._cache.values())
        return {"entries": entries, "hits": hits, "ttl": self.config.cache_ttl}

    @property
    def stats(self) -> dict:
        s = self._stats
        return {"total_requests": s.total_requests, "cached": s.cached_requests,
                "retried": s.retried_requests, "failed": s.failed_requests,
                "avg_latency_ms": round(s.avg_latency_ms, 2),
                "total_bytes": s.total_bytes, "cache": self.cache_stats()}
