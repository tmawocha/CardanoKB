"""
fetch_pools.py
==============
Endpoint groups 2, 3, 4: Stake Pools

Fetches the top MAX_POOLS stake pools (by delegated stake) along with:
  - Pool metadata and parameters        /pools/{id}
  - Delegator list                      /pools/{id}/delegators
  - Per-epoch performance history       /pools/{id}/history

Output saved to: cache/pools/
"""

import json
import logging
from pathlib import Path

from config import CACHE_DIR, MAX_POOLS
from fetcher import BlockfrostClient

log = logging.getLogger(__name__)

POOLS_CACHE = Path(CACHE_DIR) / "pools"


def _save(filename: str, data):
    path = POOLS_CACHE / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ── Group 2: Pool list + details ───────────────────────────────────────────────

def fetch_pool_list(client: BlockfrostClient) -> list[str]:
    """
    Returns pool IDs for the top MAX_POOLS pools ordered by delegated stake
    (Blockfrost /pools?order=desc returns highest-stake pools first).
    """
    log.info("── Fetching top %d pool IDs ──", MAX_POOLS)

    # Blockfrost /pools returns IDs only, ordered by stake desc by default
    all_ids = client.get_paginated("/pools", extra_params={"order": "desc"})
    pool_ids = [p["pool_id"] if isinstance(p, dict) else p for p in all_ids][:MAX_POOLS]

    _save("pool_ids.json", pool_ids)
    log.info("Saved %d pool IDs", len(pool_ids))
    return pool_ids


def fetch_pool_details(client: BlockfrostClient, pool_ids: list[str]) -> list[dict]:
    """
    Fetches detailed info for each pool: parameters, metadata, live stake etc.
    """
    log.info("── Fetching pool details for %d pools ──", len(pool_ids))
    details = []

    for i, pool_id in enumerate(pool_ids, 1):
        log.info("  [%d/%d] Pool: %s", i, len(pool_ids), pool_id[:20] + "…")
        data = client.get(f"/pools/{pool_id}")
        if data:
            details.append(data)

    _save("pool_details.json", details)
    log.info("Saved details for %d pools", len(details))
    return details


def fetch_pool_metadata(client: BlockfrostClient, pool_ids: list[str]) -> dict:
    """
    Fetches off-chain metadata (name, ticker, description, homepage) per pool.
    Returns dict keyed by pool_id.
    """
    log.info("── Fetching pool metadata ──")
    metadata = {}

    for i, pool_id in enumerate(pool_ids, 1):
        log.info("  [%d/%d] Metadata: %s", i, len(pool_ids), pool_id[:20] + "…")
        data = client.get(f"/pools/{pool_id}/metadata")
        if data:
            metadata[pool_id] = data

    _save("pool_metadata.json", metadata)
    log.info("Saved metadata for %d pools", len(metadata))
    return metadata


# ── Group 3: Pool delegators ───────────────────────────────────────────────────

def fetch_pool_delegators(client: BlockfrostClient, pool_ids: list[str]) -> dict:
    """
    Fetches the current delegator list for each pool.
    Returns dict keyed by pool_id → list of delegator objects.
    """
    log.info("── Fetching pool delegators ──")
    all_delegators = {}

    for i, pool_id in enumerate(pool_ids, 1):
        log.info("  [%d/%d] Delegators: %s", i, len(pool_ids), pool_id[:20] + "…")
        delegators = client.get_paginated(f"/pools/{pool_id}/delegators")
        all_delegators[pool_id] = delegators

    _save("pool_delegators.json", all_delegators)
    log.info("Saved delegators for %d pools", len(all_delegators))
    return all_delegators


# ── Group 4: Pool history ──────────────────────────────────────────────────────

def fetch_pool_history(client: BlockfrostClient, pool_ids: list[str]) -> dict:
    """
    Fetches per-epoch history for each pool (active stake, blocks, rewards, etc.).
    Returns dict keyed by pool_id → list of epoch history objects.
    """
    log.info("── Fetching pool history ──")
    all_history = {}

    for i, pool_id in enumerate(pool_ids, 1):
        log.info("  [%d/%d] History: %s", i, len(pool_ids), pool_id[:20] + "…")
        history = client.get_paginated(f"/pools/{pool_id}/history", extra_params={"order": "desc"})
        all_history[pool_id] = history

    _save("pool_history.json", all_history)
    log.info("Saved history for %d pools", len(all_history))
    return all_history


# ── Orchestrator ───────────────────────────────────────────────────────────────

def run_pool_fetchers(client: BlockfrostClient) -> dict:
    """Run all pool-related fetchers and return combined results."""
    pool_ids  = fetch_pool_list(client)
    details   = fetch_pool_details(client, pool_ids)
    metadata  = fetch_pool_metadata(client, pool_ids)
    delegators = fetch_pool_delegators(client, pool_ids)
    history   = fetch_pool_history(client, pool_ids)

    return {
        "pool_ids":   pool_ids,
        "details":    details,
        "metadata":   metadata,
        "delegators": delegators,
        "history":    history,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    client = BlockfrostClient()
    results = run_pool_fetchers(client)

