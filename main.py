"""
main.py
=======
Cardano Governance Knowledge Graph — Data Ingestion Pipeline
============================================================

Orchestrates all 9 Blockfrost endpoint groups in the correct order.
All responses are cached to ./cache/ as JSON (risk mitigation per proposal).

Usage:
    export BLOCKFROST_API_KEY="mainnetXXXXXXXXXX"
    python main.py

    # Or to run individual modules:
    python fetch_epochs.py
    python fetch_pools.py
    python fetch_governance.py
"""

import json
import logging
import time
from pathlib import Path

from config import CACHE_DIR
from fetcher import BlockfrostClient
from fetch_epochs import fetch_epochs
from fetch_pools import run_pool_fetchers
from fetch_governance import run_governance_fetchers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SUMMARY_PATH = Path(CACHE_DIR) / "ingestion_summary.json"


def print_banner():
    print("""
╔══════════════════════════════════════════════════════════╗
║   Cardano Governance KG — Blockfrost Ingestion Pipeline  ║
║   KEN4256 Individual Project                             ║
╚══════════════════════════════════════════════════════════╝
""")


def main():
    print_banner()
    start_time = time.time()

    client = BlockfrostClient()

    # ── Phase 1: Epochs (defines temporal scope for all other queries) ─────────
    log.info("═══ PHASE 1/3: Epochs ═══")
    epochs = fetch_epochs(client)
    epoch_range = (epochs[-1]["epoch"], epochs[0]["epoch"])
    log.info("Epoch scope: %d → %d\n", *epoch_range)

    # ── Phase 2: Stake Pools ───────────────────────────────────────────────────
    log.info("═══ PHASE 2/3: Stake Pools ═══")
    pool_data = run_pool_fetchers(client)
    log.info("Pool ingestion complete.\n")

    # ── Phase 3: Governance (DReps, Proposals, Votes) ─────────────────────────
    log.info("═══ PHASE 3/3: Governance ═══")
    gov_data = run_governance_fetchers(client)
    log.info("Governance ingestion complete.\n")

    # ── Summary ────────────────────────────────────────────────────────────────
    elapsed = time.time() - start_time
    summary = {
        "status":          "complete",
        "elapsed_seconds": round(elapsed, 1),
        "epoch_range":     {"from": epoch_range[0], "to": epoch_range[1]},
        "counts": {
            "epochs":           len(epochs),
            "pools":            len(pool_data["pool_ids"]),
            "pool_delegators":  sum(len(v) for v in pool_data["delegators"].values()),
            "dreps":            len(gov_data["drep_list"]),
            "drep_delegators":  sum(len(v) for v in gov_data["drep_delegators"].values()),
            "drep_votes":       sum(len(v) for v in gov_data["drep_votes"].values()),
            "proposals":        len(gov_data["proposals"]),
            "proposal_votes":   sum(len(v) for v in gov_data["proposal_votes"].values()),
        }
    }

    Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)
    with open(SUMMARY_PATH, "w") as f:
        json.dump(summary, f, indent=2)

    print("\n" + "═" * 60)
    print("  INGESTION COMPLETE")
    print("═" * 60)
    print(f"  Elapsed:         {elapsed:.1f}s")
    print(f"  Epoch range:     {epoch_range[0]} → {epoch_range[1]}")
    print(f"  Pools:           {summary['counts']['pools']}")
    print(f"  Pool delegators: {summary['counts']['pool_delegators']}")
    print(f"  DReps:           {summary['counts']['dreps']}")
    print(f"  DRep delegators: {summary['counts']['drep_delegators']}")
    print(f"  DRep votes:      {summary['counts']['drep_votes']}")
    print(f"  Proposals:       {summary['counts']['proposals']}")
    print(f"  Proposal votes:  {summary['counts']['proposal_votes']}")
    print(f"\n  Cache written to: {CACHE_DIR}/")
    print(f"  Summary:          {SUMMARY_PATH}")
    print("═" * 60)


if __name__ == "__main__":
    main()
