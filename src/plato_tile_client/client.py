"""HTTP client for PLATO tile server."""

import json, urllib.request, urllib.error
from typing import Optional

class TileClient:
    def __init__(self, base_url: str = "http://localhost:8847", timeout: int = 10):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._request_count = 0

    def _get(self, path: str, params: dict = None) -> dict | list:
        url = self.base_url + path
        if params:
            url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                self._request_count += 1
                return json.loads(resp.read().decode())
        except (urllib.error.URLError, json.JSONDecodeError, KeyError):
            return {"error": f"failed to fetch {path}"}

    def get_tiles(self, limit: int = 100, domain: str = None, min_confidence: float = 0.0) -> list[dict]:
        params = {"limit": limit}
        if domain: params["domain"] = domain
        if min_confidence > 0: params["min_confidence"] = min_confidence
        result = self._get("/tiles", params)
        return result if isinstance(result, list) else result.get("tiles", [])

    def get_tile(self, tile_id: str) -> dict | None:
        result = self._get(f"/tiles/{tile_id}")
        return None if isinstance(result, dict) and "error" in result else result

    def search(self, query: str, top_n: int = 10) -> list[dict]:
        result = self._get("/search", {"q": query, "n": top_n})
        return result if isinstance(result, list) else result.get("results", [])

    def score_and_rank(self, tiles: list[dict], query: str = "") -> list[dict]:
        """Client-side scoring when server search unavailable."""
        if not tiles or not query:
            return tiles
        q_words = set(query.lower().split())
        scored = []
        for t in tiles:
            c_words = set(t.get("content", "").lower().split())
            overlap = len(q_words & c_words) / max(len(q_words | c_words), 1)
            if overlap < 0.01:
                continue
            score = overlap * 0.5 + t.get("confidence", 0.5) * 0.3
            priority = t.get("priority", "P2")
            if priority == "P0": score += 10.0
            elif priority == "P1": score += 1.0
            t["_score"] = score
            scored.append(t)
        scored.sort(key=lambda x: -x.get("_score", 0))
        return scored[:10]

    def dedup_cache(self, tiles: list[dict]) -> list[dict]:
        seen = set()
        deduped = []
        for t in tiles:
            key = t.get("content", "")[:100]
            if key not in seen:
                seen.add(key)
                deduped.append(t)
        return deduped

    def status(self) -> dict:
        return self._get("/status")

    @property
    def request_count(self) -> int:
        return self._request_count
