"""
fetcher.py
==========
Base Blockfrost API client.

Features:
  - Authenticated requests via project_id header
  - Automatic pagination (fetches ALL pages for list endpoints)
  - Exponential backoff on 429 / 5xx responses
  - JSON response caching to ./cache/ to protect against rate limits
    and enable offline re-runs (per project proposal risk mitigation)
"""

import json
import logging
import os
import time
from pathlib import Path

import requests

from config import (
    BASE_URL,
    BLOCKFROST_API_KEY,
    CACHE_DIR,
    MAX_RETRIES,
    PAGE_SIZE,
    REQUEST_DELAY_SECONDS,
    RETRY_BACKOFF_BASE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


class BlockfrostClient:
    """Thin wrapper around the Blockfrost REST API."""

    def __init__(self):
        if BLOCKFROST_API_KEY == "YOUR_PROJECT_ID_HERE":
            raise ValueError(
                "Set your Blockfrost project_id in config.py "
                "or via the BLOCKFROST_API_KEY environment variable."
            )
        self.session = requests.Session()
        self.session.headers.update({"project_id": BLOCKFROST_API_KEY})
        Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _cache_path(self, cache_key: str) -> Path:
        """Return the file path for a given cache key."""
        safe_key = cache_key.replace("/", "__").replace("?", "_").replace("&", "_")
        return Path(CACHE_DIR) / f"{safe_key}.json"

    def _load_cache(self, cache_key: str):
        path = self._cache_path(cache_key)
        if path.exists():
            with open(path) as f:
                log.debug("Cache hit: %s", cache_key)
                return json.load(f)
        return None

    def _save_cache(self, cache_key: str, data):
        path = self._cache_path(cache_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        log.debug("Cached: %s", cache_key)

    def _get(self, endpoint: str, params: dict = None) -> dict | list:
        """
        Single authenticated GET with exponential backoff on rate-limit/server errors.
        Uses cache if available.
        """
        params = params or {}
        cache_key = endpoint + ("_" + "_".join(f"{k}{v}" for k, v in sorted(params.items())) if params else "")

        cached = self._load_cache(cache_key)
        if cached is not None:
            return cached

        url = f"{BASE_URL}{endpoint}"
        for attempt in range(MAX_RETRIES):
            time.sleep(REQUEST_DELAY_SECONDS)
            try:
                resp = self.session.get(url, params=params, timeout=30)

                if resp.status_code == 200:
                    data = resp.json()
                    self._save_cache(cache_key, data)
                    return data

                elif resp.status_code == 404:
                    log.warning("404 Not found: %s", endpoint)
                    return None

                elif resp.status_code == 429:
                    wait = RETRY_BACKOFF_BASE ** attempt
                    log.warning("Rate limited. Waiting %ss (attempt %d/%d)", wait, attempt + 1, MAX_RETRIES)
                    time.sleep(wait)

                elif resp.status_code >= 500:
                    wait = RETRY_BACKOFF_BASE ** attempt
                    log.warning("Server error %d. Waiting %ss", resp.status_code, wait)
                    time.sleep(wait)

                else:
                    log.error("Unexpected status %d for %s", resp.status_code, endpoint)
                    resp.raise_for_status()

            except requests.exceptions.RequestException as e:
                wait = RETRY_BACKOFF_BASE ** attempt
                log.warning("Request error: %s. Retrying in %ss", e, wait)
                time.sleep(wait)

        log.error("All %d retries exhausted for %s", MAX_RETRIES, endpoint)
        return None

    def get_paginated(self, endpoint: str, extra_params: dict = None) -> list:
        """
        Fetches ALL pages from a paginated Blockfrost list endpoint.
        Merges results into a single list.
        """
        extra_params = extra_params or {}
        all_results = []
        page = 1

        while True:
            params = {"count": PAGE_SIZE, "page": page, **extra_params}
            cache_key = f"{endpoint}_page{page}" + (
                "_" + "_".join(f"{k}{v}" for k, v in sorted(extra_params.items()))
                if extra_params else ""
            )

            # Check cache first
            cached = self._load_cache(cache_key)
            if cached is not None:
                batch = cached
            else:
                url = f"{BASE_URL}{endpoint}"
                time.sleep(REQUEST_DELAY_SECONDS)
                for attempt in range(MAX_RETRIES):
                    try:
                        resp = self.session.get(url, params=params, timeout=30)
                        if resp.status_code == 200:
                            batch = resp.json()
                            self._save_cache(cache_key, batch)
                            break
                        elif resp.status_code == 429:
                            wait = RETRY_BACKOFF_BASE ** attempt
                            log.warning("Rate limited on page %d. Waiting %ss", page, wait)
                            time.sleep(wait)
                        elif resp.status_code == 404:
                            return all_results
                        else:
                            resp.raise_for_status()
                    except requests.exceptions.RequestException as e:
                        time.sleep(RETRY_BACKOFF_BASE ** attempt)
                        log.warning("Error on page %d: %s", page, e)
                        batch = []
                        break
                else:
                    log.error("Retries exhausted on page %d of %s", page, endpoint)
                    break

            if not batch:
                break  # No more pages

            all_results.extend(batch)
            log.info("  %s — fetched page %d (%d items so far)", endpoint, page, len(all_results))

            if len(batch) < PAGE_SIZE:
                break  # Last page

            page += 1

        return all_results

    # ── Public convenience methods ─────────────────────────────────────────────

    def get(self, endpoint: str, params: dict = None):
        """Single item GET."""
        return self._get(endpoint, params)
