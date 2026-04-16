"""Persistent cache for website-enrichment search results.

Prevents re-querying the same firm repeatedly across runs. Stores:
- Query that was run
- Result URL (or null for no hit)
- Search engine used
- Timestamp of last attempt

A firm is considered cached if:
- It has a recorded hit (always skip — we already found it), OR
- It has a recorded miss younger than ttl_days (skip to avoid re-burning budget).

Use --force in calling scripts to bypass and re-query everything.
"""
import json
import os
from datetime import datetime, timezone

DEFAULT_PATH = "/tmp/enrich_cache.json"
DEFAULT_TTL_DAYS = 30


class EnrichCache:
    def __init__(self, path=DEFAULT_PATH, ttl_days=DEFAULT_TTL_DAYS):
        self.path = path
        self.ttl_days = ttl_days
        self.data = self._load()

    def _load(self):
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path) as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}

    def save(self):
        try:
            with open(self.path, "w") as f:
                json.dump(self.data, f, indent=2)
        except OSError as e:
            print(f"[cache] Warning: couldn't save cache to {self.path}: {e}")

    def should_skip(self, firm_id, force=False):
        """Return True if we should skip searching this firm."""
        if force:
            return False
        entry = self.data.get(str(firm_id))
        if not entry:
            return False
        # Found hits are always skipped (we already have a URL)
        if entry.get("result"):
            return True
        # Misses are skipped if fresh
        try:
            last = datetime.fromisoformat(entry["last_tried"])
            age = (datetime.now(timezone.utc) - last).days
            return age < self.ttl_days
        except (KeyError, ValueError):
            return False

    def record(self, firm_id, query, result, engine):
        self.data[str(firm_id)] = {
            "query": query,
            "result": result,
            "engine": engine,
            "last_tried": datetime.now(timezone.utc).isoformat(),
        }

    def stats(self):
        total = len(self.data)
        hits = sum(1 for v in self.data.values() if v.get("result"))
        return {"total": total, "hits": hits, "misses": total - hits}
