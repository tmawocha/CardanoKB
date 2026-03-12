"""
fetch_governance.py
===================
Endpoint groups 5, 6, 7, 8, 9: Governance

Fetches the full governance actor graph:

  Group 5 — DReps
    /governance/dreps                    list of all active DReps
    /governance/dreps/{drep_id}          DRep detail (stake, metadata, status)

  Group 6 — DRep Delegators
    /governance/dreps/{drep_id}/delegators wallets delegating voting power to each DRep

  Group 7 — DRep Votes
    /governance/dreps/{drep_id}/votes    how each DRep voted on each proposal

  Group 8 — Proposals
    /governance/proposals               all governance proposals
    /governance/proposals/{tx}/{idx}     proposal detail (type, deposit, expiry, metadata)

  Group 9 — Proposal Votes
    /governance/proposals/{tx}/{idx}/votes  all votes cast on each proposal

Output saved to: cache/dreps/, cache/proposals/, cache/votes/
"""

import json
import logging
from pathlib import Path

from config import CACHE_DIR
from fetcher import BlockfrostClient

log = logging.getLogger(__name__)

DREP_CACHE     = Path(CACHE_DIR) / "dreps"
PROPOSAL_CACHE = Path(CACHE_DIR) / "proposals"
VOTE_CACHE     = Path(CACHE_DIR) / "votes"


def _save(directory: Path, filename: str, data):
    directory.mkdir(parents=True, exist_ok=True)
    with open(directory / filename, "w") as f:
        json.dump(data, f, indent=2)


# ── Group 5: DReps ─────────────────────────────────────────────────────────────

def fetch_drep_list(client: BlockfrostClient) -> list[dict]:
    """Fetches all registered DReps (paginated)."""
    log.info("── Fetching DRep list ──")
    dreps = client.get_paginated("/governance/dreps")
    _save(DREP_CACHE, "drep_list.json", dreps)
    log.info("Found %d DReps", len(dreps))
    return dreps


def fetch_drep_details(client: BlockfrostClient, drep_list: list[dict]) -> list[dict]:
    """
    Fetches full detail for each DRep: live power, metadata hash, registration tx, status.
    """
    log.info("── Fetching DRep details ──")
    details = []

    for i, drep in enumerate(drep_list, 1):
        drep_id = drep.get("drep_id") or drep.get("id")
        log.info("  [%d/%d] DRep: %s", i, len(drep_list), drep_id[:24] + "…" if drep_id else "?")
        data = client.get(f"/governance/dreps/{drep_id}")
        if data:
            details.append(data)

    _save(DREP_CACHE, "drep_details.json", details)
    log.info("Saved details for %d DReps", len(details))
    return details


# ── Group 6: DRep delegators ───────────────────────────────────────────────────

def fetch_drep_delegators(client: BlockfrostClient, drep_list: list[dict]) -> dict:
    """
    For each DRep, fetches the list of stake addresses that have delegated
    their voting power to that DRep.
    """
    log.info("── Fetching DRep delegators ──")
    all_delegators = {}

    for i, drep in enumerate(drep_list, 1):
        drep_id = drep.get("drep_id") or drep.get("id")
        log.info("  [%d/%d] DRep delegators: %s", i, len(drep_list), drep_id[:24] + "…" if drep_id else "?")
        delegators = client.get_paginated(f"/governance/dreps/{drep_id}/delegators")
        all_delegators[drep_id] = delegators

    _save(DREP_CACHE, "drep_delegators.json", all_delegators)
    total = sum(len(v) for v in all_delegators.values())
    log.info("Saved %d total delegator relationships across %d DReps", total, len(all_delegators))
    return all_delegators


# ── Group 7: DRep votes ────────────────────────────────────────────────────────

def fetch_drep_votes(client: BlockfrostClient, drep_list: list[dict]) -> dict:
    """
    For each DRep, fetches their full voting history (yes/no/abstain per proposal).
    This is the core data for voting coalition detection and power analysis.
    """
    log.info("── Fetching DRep votes ──")
    all_votes = {}

    for i, drep in enumerate(drep_list, 1):
        drep_id = drep.get("drep_id") or drep.get("id")
        log.info("  [%d/%d] DRep votes: %s", i, len(drep_list), drep_id[:24] + "…" if drep_id else "?")
        votes = client.get_paginated(f"/governance/dreps/{drep_id}/votes")
        all_votes[drep_id] = votes

    _save(VOTE_CACHE, "drep_votes.json", all_votes)
    total = sum(len(v) for v in all_votes.values())
    log.info("Saved %d total DRep vote records", total)
    return all_votes


# ── Group 8: Proposals ─────────────────────────────────────────────────────────

def fetch_proposal_list(client: BlockfrostClient) -> list[dict]:
    """Fetches all governance proposals (paginated, newest first)."""
    log.info("── Fetching governance proposals ──")
    proposals = client.get_paginated("/governance/proposals", extra_params={"order": "desc"})
    _save(PROPOSAL_CACHE, "proposal_list.json", proposals)
    log.info("Found %d proposals", len(proposals))
    return proposals


def fetch_proposal_details(client: BlockfrostClient, proposals: list[dict]) -> list[dict]:
    """
    Fetches full detail for each proposal: type (ParameterChange, TreasuryWithdrawal,
    HardForkInitiation, etc.), deposit, expiry epoch, metadata anchor.
    """
    log.info("── Fetching proposal details ──")
    details = []

    for i, proposal in enumerate(proposals, 1):
        tx_hash   = proposal.get("tx_hash")
        cert_index = proposal.get("cert_index", 0)
        log.info("  [%d/%d] Proposal: %s…", i, len(proposals), tx_hash[:16] if tx_hash else "?")
        data = client.get(f"/governance/proposals/{tx_hash}/{cert_index}")
        if data:
            details.append(data)

    _save(PROPOSAL_CACHE, "proposal_details.json", details)
    log.info("Saved details for %d proposals", len(details))
    return details


# ── Group 9: Proposal votes ────────────────────────────────────────────────────

def fetch_proposal_votes(client: BlockfrostClient, proposals: list[dict]) -> dict:
    """
    For each proposal, fetches all votes cast on it (by DReps, SPOs, and the
    constitutional committee). This gives the full vote tally per proposal.
    """
    log.info("── Fetching proposal votes ──")
    all_votes = {}

    for i, proposal in enumerate(proposals, 1):
        tx_hash    = proposal.get("tx_hash")
        cert_index = proposal.get("cert_index", 0)
        key        = f"{tx_hash}_{cert_index}"
        log.info("  [%d/%d] Votes on proposal: %s…", i, len(proposals), tx_hash[:16] if tx_hash else "?")
        votes = client.get_paginated(f"/governance/proposals/{tx_hash}/{cert_index}/votes")
        all_votes[key] = votes

    _save(VOTE_CACHE, "proposal_votes.json", all_votes)
    total = sum(len(v) for v in all_votes.values())
    log.info("Saved %d total proposal vote records across %d proposals", total, len(all_votes))
    return all_votes


# ── Orchestrator ───────────────────────────────────────────────────────────────

def run_governance_fetchers(client: BlockfrostClient) -> dict:
    """Run all governance-related fetchers and return combined results."""
    drep_list        = fetch_drep_list(client)
    drep_details     = fetch_drep_details(client, drep_list)
    drep_delegators  = fetch_drep_delegators(client, drep_list)
    drep_votes       = fetch_drep_votes(client, drep_list)
    proposals        = fetch_proposal_list(client)
    proposal_details = fetch_proposal_details(client, proposals)
    proposal_votes   = fetch_proposal_votes(client, proposals)

    return {
        "drep_list":        drep_list,
        "drep_details":     drep_details,
        "drep_delegators":  drep_delegators,
        "drep_votes":       drep_votes,
        "proposals":        proposals,
        "proposal_details": proposal_details,
        "proposal_votes":   proposal_votes,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    client = BlockfrostClient()
    results = run_governance_fetchers(client)
    
