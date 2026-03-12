"""
transform_to_rdf.py
===================
Reads the Blockfrost JSON cache produced by fetch_*.py and transforms
every entity into RDF triples using the Cardano Governance Ontology.

Output: governance_kg.ttl  (single merged Turtle file)

Run from the cardanokb/ directory:
    python transform_to_rdf.py

Cache layout expected:
    cache/
        epochs_summary.json
        pools/pool_details.json
        pools/pool_delegators.json
        pools/pool_metadata.json      (optional — ticker only)
        dreps/drep_details.json
        dreps/drep_delegators.json
        votes/drep_votes.json          (optional)
        proposals/proposal_details.json
        votes/proposal_votes.json
        ingestion_summary.json
"""

import json
import os
import re
from datetime import datetime, timezone
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.collection import Collection
from rdflib.namespace import RDF, RDFS, OWL, XSD, SKOS, DCTERMS

# =============================================================================
#  1. Paths
# =============================================================================

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR  = os.path.join(BASE_DIR, "cache")
OUTPUT_TTL = os.path.join(BASE_DIR, "governance_kg.ttl")

def cache(path):
    return os.path.join(CACHE_DIR, path)

def load_json(path, default=None):
    full = cache(path)
    if not os.path.exists(full):
        print(f"  [skip] {path} not found")
        return default if default is not None else []
    with open(full) as f:
        return json.load(f)

# =============================================================================
#  2. Namespaces
# =============================================================================

CGOV   = Namespace("https://w3id.org/cgov#")
SCHEMA = Namespace("https://schema.org/")
PROV   = Namespace("http://www.w3.org/ns/prov#")

g = Graph()
g.bind("cgov",   CGOV)
g.bind("schema", SCHEMA)
g.bind("prov",   PROV)
g.bind("xsd",    XSD)
g.bind("skos",   SKOS)
g.bind("dc",     DCTERMS)

# =============================================================================
#  3. Helpers
# =============================================================================

RETRIEVED_AT = Literal(
    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    datatype=XSD.dateTime
)

def safe_uri_fragment(s):
    """Strips characters that are invalid in URI fragments."""
    return re.sub(r"[^A-Za-z0-9._\-]", "_", str(s))

def uri(local):
    return CGOV[safe_uri_fragment(local)]

def pool_uri(pool_id):    return CGOV[f"pool_{safe_uri_fragment(pool_id)}"]
def stake_uri(address):   return CGOV[f"stake_{safe_uri_fragment(address)}"]
def drep_uri(drep_id):    return CGOV[f"drep_{safe_uri_fragment(drep_id)}"]
def epoch_uri(number):    return CGOV[f"epoch_{number}"]
def proposal_uri(tx, ci): return CGOV[f"proposal_{safe_uri_fragment(tx)}_{ci}"]
def vote_uri(tx, ci):     return CGOV[f"vote_{safe_uri_fragment(tx)}_{ci}"]

def add_provenance(subject, endpoint):
    g.add((subject, CGOV.retrievedFromEndpoint,
           Literal(endpoint, datatype=XSD.anyURI)))
    g.add((subject, CGOV.retrievedAt, RETRIEVED_AT))

def lit_int(val):
    if val is None: return None
    return Literal(int(val), datatype=XSD.integer)

def lit_str(val):
    if val is None: return None
    return Literal(str(val))

def lit_bool(val):
    return Literal(bool(val), datatype=XSD.boolean)

def lit_decimal(val):
    if val is None: return None
    return Literal(float(val), datatype=XSD.decimal)

def lit_dt(unix_ts):
    if unix_ts is None: return None
    dt = datetime.fromtimestamp(int(unix_ts), tz=timezone.utc)
    return Literal(dt.strftime("%Y-%m-%dT%H:%M:%SZ"), datatype=XSD.dateTime)

# Governance type mapping: Blockfrost snake_case → ontology concept URI
GOV_TYPE_MAP = {
    "treasury_withdrawals":  CGOV.TreasuryWithdrawal,
    "parameter_change":      CGOV.ParameterChange,
    "hard_fork_initiation":  CGOV.HardForkInitiation,
    "new_constitution":      CGOV.NewConstitution,
    "new_committee":         CGOV.NewCommittee,
    "info_action":           CGOV.InfoAction,
    "no_confidence":         CGOV.NoConfidence,
}

VOTE_MAP = {
    "yes":     CGOV.Yes,
    "no":      CGOV.No,
    "abstain": CGOV.Abstain,
}

# CIP-1694 predefined DRep IDs → named individuals already in ontology
PREDEFINED_DREPS = {
    "drep_always_abstain":       CGOV.AlwaysAbstain,
    "drep_always_no_confidence": CGOV.AlwaysNoConfidence,
}

# =============================================================================
#  4. Epochs
# =============================================================================

def transform_epochs():
    # Support both cache/epochs_summary.json and cache/epochs/epochs_summary.json
    epochs = load_json("epochs_summary.json")
    if not epochs:
        epochs = load_json("epochs/epochs_summary.json")
    print(f"  Epochs: {len(epochs)}")
    for ep in epochs:
        n   = ep["epoch"]
        uri_ = epoch_uri(n)
        g.add((uri_, RDF.type,               CGOV.Epoch))
        g.add((uri_, RDFS.label,             Literal(f"Epoch {n}")))
        g.add((uri_, CGOV.epochNumber,       lit_int(n)))
        if ep.get("start_time"):
            g.add((uri_, CGOV.epochStartTime, lit_int(ep["start_time"])))
        if ep.get("end_time"):
            g.add((uri_, CGOV.epochEndTime,   lit_int(ep["end_time"])))
        if ep.get("active_stake"):
            g.add((uri_, CGOV.totalActiveStakeLovelace, lit_int(ep["active_stake"])))
        add_provenance(uri_, "https://blockfrost.io/api/v0/epochs/{epoch}")

# =============================================================================
#  5. Stake Pools
# =============================================================================

def transform_pools():
    details  = load_json("pools/pool_details.json")
    metadata = {m["pool_id"]: m
                for m in load_json("pools/pool_metadata.json", default=[])
                if isinstance(m, dict) and "pool_id" in m}

    print(f"  Pools: {len(details)}")
    for pool in details:
        pid   = pool["pool_id"]
        uri_  = pool_uri(pid)

        g.add((uri_, RDF.type,    CGOV.StakePool))
        g.add((uri_, RDFS.label,  Literal(pid)))
        g.add((uri_, CGOV.poolId, lit_str(pid)))

        # Ticker from metadata if available
        meta = metadata.get(pid, {})
        if meta and meta.get("ticker"):
            g.add((uri_, CGOV.poolTicker, lit_str(meta["ticker"])))

        if pool.get("live_stake") is not None:
            g.add((uri_, CGOV.liveStakeLovelace,     lit_int(pool["live_stake"])))
        if pool.get("declared_pledge") is not None:
            g.add((uri_, CGOV.declaredPledgeLovelace, lit_int(pool["declared_pledge"])))
        if pool.get("margin_cost") is not None:
            g.add((uri_, CGOV.marginFee,             lit_decimal(pool["margin_cost"])))
        if pool.get("fixed_cost") is not None:
            g.add((uri_, CGOV.fixedCostLovelace,     lit_int(pool["fixed_cost"])))

        add_provenance(uri_, "https://blockfrost.io/api/v0/pools/{pool_id}")

# =============================================================================
#  6. Pool Delegators → StakeAddress + delegatesStakeTo
# =============================================================================

def transform_pool_delegators():
    delegators = load_json("pools/pool_delegators.json", default={})
    total = 0
    for pid, stakers in delegators.items():
        p_uri = pool_uri(pid)
        for s in stakers:
            addr  = s["address"]
            s_uri = stake_uri(addr)
            # Create StakeAddress if not already present
            g.add((s_uri, RDF.type,         CGOV.StakeAddress))
            g.add((s_uri, CGOV.stakeAddress, lit_str(addr)))
            if s.get("live_stake") is not None:
                g.add((s_uri, CGOV.delegatedStakeLovelace, lit_int(s["live_stake"])))
            # Delegation relation
            g.add((s_uri,  CGOV.delegatesStakeTo,      p_uri))
            g.add((p_uri,  CGOV.hasStakeDelegatedFrom, s_uri))
            add_provenance(s_uri, "https://blockfrost.io/api/v0/pools/{pool_id}/delegators")
            total += 1
    print(f"  Pool delegators (StakeAddresses): {total}")

# =============================================================================
#  7. DReps
# =============================================================================

def transform_dreps():
    details = load_json("dreps/drep_details.json")
    print(f"  DReps: {len(details)}")
    for drep in details:
        did  = drep["drep_id"]

        # Map predefined DReps to their named individuals
        if did in PREDEFINED_DREPS:
            uri_ = PREDEFINED_DREPS[did]
        else:
            uri_ = drep_uri(did)
            g.add((uri_, RDF.type,   CGOV.DRep))
            g.add((uri_, RDFS.label, Literal(did)))

        g.add((uri_, CGOV.drepId, lit_str(did)))
        if drep.get("hex"):
            g.add((uri_, CGOV.drepHex, lit_str(drep["hex"])))
        if drep.get("amount") is not None:
            g.add((uri_, CGOV.votingPowerLovelace, lit_int(drep["amount"])))
        if drep.get("active") is not None:
            g.add((uri_, CGOV.isActive,  lit_bool(drep["active"])))
        if drep.get("retired") is not None:
            g.add((uri_, CGOV.isRetired, lit_bool(drep["retired"])))
        if drep.get("expired") is not None:
            g.add((uri_, CGOV.isExpired, lit_bool(drep["expired"])))
        if drep.get("has_script") is not None:
            g.add((uri_, CGOV.hasScript, lit_bool(drep["has_script"])))
        if drep.get("active_epoch") is not None:
            g.add((uri_, CGOV.activeEpoch, lit_int(drep["active_epoch"])))
        if drep.get("last_active_epoch") is not None:
            g.add((uri_, CGOV.lastActiveEpoch, lit_int(drep["last_active_epoch"])))

        add_provenance(uri_, "https://blockfrost.io/api/v0/governance/dreps/{drep_id}")

# =============================================================================
#  8. DRep Delegators → StakeAddress + delegatesVotingPowerTo
# =============================================================================

def transform_drep_delegators():
    delegators = load_json("dreps/drep_delegators.json", default={})
    total = 0
    for did, stakers in delegators.items():
        if did in PREDEFINED_DREPS:
            d_uri = PREDEFINED_DREPS[did]
        else:
            d_uri = drep_uri(did)

        for s in stakers:
            addr  = s["address"]
            s_uri = stake_uri(addr)
            g.add((s_uri, RDF.type,         CGOV.StakeAddress))
            g.add((s_uri, CGOV.stakeAddress, lit_str(addr)))
            # amount here is the voting power delegated from this address
            if s.get("amount") is not None:
                # Only set if not already set (pool delegator may have set live_stake)
                existing = list(g.objects(s_uri, CGOV.delegatedStakeLovelace))
                if not existing:
                    g.add((s_uri, CGOV.delegatedStakeLovelace, lit_int(s["amount"])))
            # Voting power delegation relation
            g.add((s_uri,  CGOV.delegatesVotingPowerTo,       d_uri))
            g.add((d_uri,  CGOV.hasVotingPowerDelegatedFrom,  s_uri))
            add_provenance(s_uri,
                "https://blockfrost.io/api/v0/governance/dreps/{drep_id}/delegators")
            total += 1
    print(f"  DRep delegators (voting power relations): {total}")

# =============================================================================
#  9. Proposals
# =============================================================================

def transform_proposals():
    details = load_json("proposals/proposal_details.json")
    print(f"  Proposals: {len(details)}")
    for prop in details:
        tx  = prop["tx_hash"]
        ci  = prop["cert_index"]
        uri_ = proposal_uri(tx, ci)

        g.add((uri_, RDF.type,    CGOV.Proposal))
        g.add((uri_, RDFS.label,  Literal(f"Proposal {prop['id']}")))
        g.add((uri_, CGOV.proposalTxHash,    lit_str(tx)))
        g.add((uri_, CGOV.proposalCertIndex, lit_int(ci)))

        # Governance type → SKOS concept
        gtype = prop.get("governance_type", "").lower()
        concept = GOV_TYPE_MAP.get(gtype)
        if concept:
            g.add((uri_, CGOV.hasProposalType, concept))

        if prop.get("deposit") is not None:
            g.add((uri_, CGOV.depositLovelace, lit_int(prop["deposit"])))

        # expiration epoch link
        if prop.get("expiration") is not None:
            g.add((uri_, CGOV.expiresAtEpoch, epoch_uri(prop["expiration"])))

        # Metadata anchor (not always present in Blockfrost response)
        anchor = prop.get("metadata", {}) or {}
        if anchor.get("url"):
            g.add((uri_, CGOV.metadataAnchorUrl,
                   Literal(anchor["url"], datatype=XSD.anyURI)))
        if anchor.get("hash"):
            g.add((uri_, CGOV.metadataAnchorHash, lit_str(anchor["hash"])))

        add_provenance(uri_,
            "https://blockfrost.io/api/v0/governance/proposals/{tx_hash}/{cert_index}")

# =============================================================================
#  10. Votes
# =============================================================================

def transform_votes():
    votes_by_proposal = load_json("votes/proposal_votes.json", default={})
    total = 0
    for prop_key, vote_list in votes_by_proposal.items():
        # prop_key format: "txhash_certindex"
        parts = prop_key.rsplit("_", 1)
        p_uri = proposal_uri(parts[0], parts[1])

        for v in vote_list:
            vtx  = v["tx_hash"]
            vci  = v["cert_index"]
            uri_ = vote_uri(vtx, vci)

            g.add((uri_, RDF.type,         CGOV.Vote))
            g.add((uri_, RDFS.label,       Literal(f"Vote {vtx[:12]}…")))
            g.add((uri_, CGOV.voteTxHash,  lit_str(vtx)))
            g.add((uri_, CGOV.voterRole,   lit_str(v.get("voter_role", ""))))

            # Vote option → SKOS concept
            opt = VOTE_MAP.get(v.get("vote", "").lower())
            if opt:
                g.add((uri_, CGOV.hasVoteOption, opt))

            # Link Vote → Proposal
            g.add((uri_, CGOV.voteOnProposal, p_uri))

            # Link voter (DRep or StakePool) → Vote
            voter_id   = v.get("voter", "")
            voter_role = v.get("voter_role", "")
            if voter_role == "drep":
                if voter_id in PREDEFINED_DREPS:
                    voter_uri = PREDEFINED_DREPS[voter_id]
                else:
                    voter_uri = drep_uri(voter_id)
            elif voter_role == "spo":
                voter_uri = pool_uri(voter_id)
            else:
                # constitutional_committee or unknown — create a generic URI
                voter_uri = CGOV[f"voter_{safe_uri_fragment(voter_id)}"]
                g.add((voter_uri, RDF.type,   OWL.Thing))

            g.add((voter_uri, CGOV.castVote, uri_))

            add_provenance(uri_,
                "https://blockfrost.io/api/v0/governance/proposals/{tx_hash}/{cert_index}/votes")
            total += 1

    print(f"  Votes: {total}")

# =============================================================================
#  11. DRep votes (optional separate cache file)
# =============================================================================

def transform_drep_votes():
    """
    drep_votes.json format (keyed by drep_id):
    { "drep1...": [ {tx_hash, cert_index, voter_role, voter, vote}, ... ] }
    These may overlap with proposal_votes — we use a set to deduplicate.
    """
    drep_votes = load_json("votes/drep_votes.json", default={})
    if not drep_votes:
        return
    seen = set(g.subjects(RDF.type, CGOV.Vote))
    added = 0
    for did, vote_list in drep_votes.items():
        if did in PREDEFINED_DREPS:
            d_uri = PREDEFINED_DREPS[did]
        else:
            d_uri = drep_uri(did)

        for v in vote_list:
            vtx  = v.get("tx_hash", "")
            vci  = v.get("cert_index", 0)
            uri_ = vote_uri(vtx, vci)
            if uri_ in seen:
                continue  # already added from proposal_votes
            seen.add(uri_)
            g.add((uri_, RDF.type,        CGOV.Vote))
            g.add((uri_, CGOV.voteTxHash, lit_str(vtx)))
            g.add((uri_, CGOV.voterRole,  lit_str("drep")))
            opt = VOTE_MAP.get(v.get("vote", "").lower())
            if opt:
                g.add((uri_, CGOV.hasVoteOption, opt))
            g.add((d_uri, CGOV.castVote, uri_))
            add_provenance(uri_,
                "https://blockfrost.io/api/v0/governance/dreps/{drep_id}/votes")
            added += 1
    if added:
        print(f"  DRep votes (additional, non-duplicate): {added}")

# =============================================================================
#  12. Ontology header triple
# =============================================================================

def add_ontology_header():
    ONT = URIRef("https://w3id.org/cgov/kg")
    g.add((ONT, RDF.type,          OWL.Ontology))
    g.add((ONT, DCTERMS.title,     Literal("Cardano Governance Knowledge Graph")))
    g.add((ONT, DCTERMS.creator,   Literal("Tinaye Mawocha")))
    g.add((ONT, DCTERMS.date,      Literal(
        datetime.now(timezone.utc).strftime("%Y-%m-%d"))))
    g.add((ONT, OWL.imports,       URIRef("https://w3id.org/cgov")))

# =============================================================================
#  Main
# =============================================================================

if __name__ == "__main__":
    print("=== Cardano Governance RDF Transformer ===")
    print(f"Cache:  {CACHE_DIR}")
    print(f"Output: {OUTPUT_TTL}\n")

    add_ontology_header()

    print("Transforming epochs …")
    transform_epochs()

    print("Transforming stake pools …")
    transform_pools()

    print("Transforming pool delegators …")
    transform_pool_delegators()

    print("Transforming DReps …")
    transform_dreps()

    print("Transforming DRep delegators …")
    transform_drep_delegators()

    print("Transforming proposals …")
    transform_proposals()

    print("Transforming votes …")
    transform_votes()

    print("Transforming DRep votes (if present) …")
    transform_drep_votes()

    print(f"\nSerialising …")
    g.serialize(destination=OUTPUT_TTL, format="turtle")

    print(f"  Total triples : {len(g):,}")
    print(f"  Epochs        : {sum(1 for _ in g.subjects(RDF.type, CGOV.Epoch))}")
    print(f"  StakePools    : {sum(1 for _ in g.subjects(RDF.type, CGOV.StakePool))}")
    print(f"  DReps         : {sum(1 for _ in g.subjects(RDF.type, CGOV.DRep))}")
    print(f"  StakeAddresses: {sum(1 for _ in g.subjects(RDF.type, CGOV.StakeAddress))}")
    print(f"  Proposals     : {sum(1 for _ in g.subjects(RDF.type, CGOV.Proposal))}")
    print(f"  Votes         : {sum(1 for _ in g.subjects(RDF.type, CGOV.Vote))}")
    print(f"\nOutput written to: {OUTPUT_TTL}")