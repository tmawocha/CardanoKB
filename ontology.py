"""
build_ontology.py
=================
Programmatically constructs the Cardano Governance Ontology using rdflib
and serialises it to ontology.ttl.

To modify the ontology, edit the relevant section below and rerun:
    python build_ontology.py

Sections:
  1. Namespaces & graph setup
  2. Ontology metadata
  3. Classes
  4. Proposal type taxonomy (SKOS)
  5. Vote option taxonomy (SKOS)
  6. Object properties
  7. Datatype properties
  8. Predefined DRep individuals (CIP-1694 built-ins)
  9. Serialise to ontology.ttl
"""

from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.collection import Collection
from rdflib.namespace import RDF, RDFS, OWL, XSD, SKOS, FOAF, DCTERMS
import os


CGOV   = Namespace("https://w3id.org/cgov#")
SCHEMA = Namespace("https://schema.org/")
PROV   = Namespace("http://www.w3.org/ns/prov#")

g = Graph()
g.bind("cgov",   CGOV)
g.bind("schema", SCHEMA)
g.bind("prov",   PROV)
g.bind("owl",    OWL)
g.bind("skos",   SKOS)
g.bind("dc",     DCTERMS)
g.bind("foaf",   FOAF)


def named_union_class(uri, label, comment, *class_uris):
    """
    Creates a named (URI-bearing) owl:unionOf class.
    Named union classes are visible and labelled in graph visualisations,
    unlike anonymous blank nodes which appear as unlabelled floating circles.
    e.g. named_union_class(CGOV.DRepOrSPO, 'DRep or SPO', '...', CGOV.DRep, CGOV.StakePool)
    """
    list_node = BNode()
    g.add((uri, RDF.type,     OWL.Class))
    g.add((uri, RDFS.label,   Literal(label)))
    g.add((uri, RDFS.comment, Literal(comment)))
    g.add((uri, OWL.unionOf,  list_node))
    Collection(g, list_node, list(class_uris))
    return uri


def add_class(uri, label, comment, subclass_of=None, equivalent_class=None,
              exact_match=None, see_also=None):
    g.add((uri, RDF.type,     OWL.Class))
    g.add((uri, RDFS.label,   Literal(label)))
    g.add((uri, RDFS.comment, Literal(comment)))
    if subclass_of:
        for sc in (subclass_of if isinstance(subclass_of, list) else [subclass_of]):
            g.add((uri, RDFS.subClassOf, sc))
    if equivalent_class:
        g.add((uri, OWL.equivalentClass, equivalent_class))
    if exact_match:
        g.add((uri, SKOS.exactMatch, exact_match))
    if see_also:
        for ref in (see_also if isinstance(see_also, list) else [see_also]):
            g.add((uri, RDFS.seeAlso, ref))


def add_object_property(uri, label, comment, domain=None, range_=None,
                        inverse_of=None, subproperty_of=None):
    g.add((uri, RDF.type,     OWL.ObjectProperty))
    g.add((uri, RDFS.label,   Literal(label)))
    g.add((uri, RDFS.comment, Literal(comment)))
    if domain:
        g.add((uri, RDFS.domain, domain))
    if range_:
        g.add((uri, RDFS.range, range_))
    if inverse_of:
        g.add((uri, OWL.inverseOf, inverse_of))
    if subproperty_of:
        g.add((uri, RDFS.subPropertyOf, subproperty_of))


def add_datatype_property(uri, label, comment, domain=None, range_=None,
                          subproperty_of=None):
    g.add((uri, RDF.type,     OWL.DatatypeProperty))
    g.add((uri, RDFS.label,   Literal(label)))
    g.add((uri, RDFS.comment, Literal(comment)))
    if domain:
        g.add((uri, RDFS.domain, domain))
    if range_:
        g.add((uri, RDFS.range, range_))
    if subproperty_of:
        g.add((uri, RDFS.subPropertyOf, subproperty_of))


def add_skos_concept(uri, pref_label, definition, scheme_uri):
    g.add((uri, RDF.type,        SKOS.Concept))
    g.add((uri, SKOS.prefLabel,  Literal(pref_label)))
    g.add((uri, SKOS.definition, Literal(definition)))
    g.add((uri, SKOS.inScheme,   scheme_uri))


ONTOLOGY_URI = URIRef("https://w3id.org/cgov")

g.add((ONTOLOGY_URI, RDF.type,            OWL.Ontology))
g.add((ONTOLOGY_URI, DCTERMS.title,       Literal("Cardano Governance Ontology")))
g.add((ONTOLOGY_URI, DCTERMS.description, Literal(
    "An ontology for representing the Cardano on-chain governance ecosystem, "
    "including DReps, stake pools, governance proposals, and voting events "
    "as defined in CIP-1694."
)))
g.add((ONTOLOGY_URI, DCTERMS.creator,   Literal("Tinaye Mawocha")))
g.add((ONTOLOGY_URI, DCTERMS.date,      Literal("2026", datatype=XSD.gYear)))
g.add((ONTOLOGY_URI, DCTERMS.source,    URIRef("https://cips.cardano.org/cip/CIP-1694")))
g.add((ONTOLOGY_URI, OWL.versionInfo,   Literal("1.0")))


add_class(
    uri         = CGOV.DRep,
    label       = "Delegated Representative",
    comment     = (
        "An on-chain actor registered under CIP-1694 to receive delegated "
        "voting power from ADA holders and cast votes on governance proposals "
        "on their behalf."
    ),
    subclass_of = SCHEMA.Person,
    exact_match = URIRef("https://cips.cardano.org/cip/CIP-1694#drep"),
)

add_class(
    uri         = CGOV.StakePool,
    label       = "Stake Pool",
    comment     = (
        "A Cardano block-producing node operated by a stake pool operator (SPO). "
        "SPOs hold institutional voting power in the CIP-1694 governance framework."
    ),
    subclass_of = SCHEMA.Organization,
)

add_class(
    uri         = CGOV.StakeAddress,
    label       = "Stake Address",
    comment     = (
        "A Bech32-encoded Cardano stake address representing an ADA holder. "
        "A stake address simultaneously delegates stake (to a pool) and "
        "voting power (to a DRep)."
    ),
    subclass_of = SCHEMA.Thing,
)

add_class(
    uri              = CGOV.Proposal,
    label            = "Governance Proposal",
    comment          = (
        "An on-chain governance action submitted for ratification by DReps, SPOs, "
        "and the Constitutional Committee. Types include ParameterChange, "
        "TreasuryWithdrawal, HardForkInitiation, NewConstitution, NewCommittee, "
        "InfoAction, and NoConfidence."
    ),
    subclass_of      = SCHEMA.Action,
    equivalent_class = PROV.Activity,
    # Connects Proposal class -> ProposalTypeScheme in the visualisation
    see_also         = CGOV.ProposalTypeScheme,
)

add_class(
    uri         = CGOV.Vote,
    label       = "Vote",
    comment     = (
        "A single on-chain vote cast by a DRep, SPO, or Constitutional Committee "
        "member on a specific governance proposal. "
        "The vote option is one of: Yes, No, or Abstain."
    ),
    subclass_of = SCHEMA.Action,
    # Connects Vote class -> VoteOptionScheme in the visualisation
    see_also    = CGOV.VoteOptionScheme,
)

add_class(
    uri         = CGOV.Epoch,
    label       = "Epoch",
    comment     = (
        "A discrete time period on the Cardano blockchain, approximately 5 days "
        "in length. Epochs define the temporal scope of stake snapshots, reward "
        "distributions, and governance state."
    ),
    subclass_of = SCHEMA.Event,
)

PROPOSAL_SCHEME = CGOV.ProposalTypeScheme
g.add((PROPOSAL_SCHEME, RDF.type,      SKOS.ConceptScheme))
g.add((PROPOSAL_SCHEME, DCTERMS.title, Literal("Cardano CIP-1694 Governance Action Types")))

PROPOSAL_TYPES = [
    (CGOV.ParameterChange,    "Parameter Change",
     "A proposal to change one or more protocol parameters."),
    (CGOV.TreasuryWithdrawal, "Treasury Withdrawal",
     "A proposal to withdraw funds from the Cardano treasury."),
    (CGOV.HardForkInitiation, "Hard Fork Initiation",
     "A proposal to initiate a hard fork of the Cardano protocol."),
    (CGOV.NewConstitution,    "New Constitution",
     "A proposal to adopt a new or amended Cardano constitution."),
    (CGOV.NewCommittee,       "New Committee",
     "A proposal to update the membership or threshold of the Constitutional Committee."),
    (CGOV.InfoAction,         "Info Action",
     "A non-binding informational governance action with no on-chain effect."),
    (CGOV.NoConfidence,       "No Confidence",
     "A proposal expressing no confidence in the current Constitutional Committee."),
]

for uri, label, definition in PROPOSAL_TYPES:
    add_skos_concept(uri, label, definition, PROPOSAL_SCHEME)



VOTE_SCHEME = CGOV.VoteOptionScheme
g.add((VOTE_SCHEME, RDF.type,      SKOS.ConceptScheme))
g.add((VOTE_SCHEME, DCTERMS.title, Literal("CIP-1694 Vote Options")))

VOTE_OPTIONS = [
    (CGOV.Yes,     "Yes",     "An affirmative vote in favour of the proposal."),
    (CGOV.No,      "No",      "A vote against the proposal."),
    (CGOV.Abstain, "Abstain", "A vote that counts toward quorum but expresses no directional preference."),
]

for uri, label, definition in VOTE_OPTIONS:
    add_skos_concept(uri, label, definition, VOTE_SCHEME)


# Delegation
add_object_property(
    uri        = CGOV.delegatesVotingPowerTo,
    label      = "delegates voting power to",
    comment    = "Relates a stake address to the DRep to which it has delegated its voting power under CIP-1694.",
    domain     = CGOV.StakeAddress,
    range_     = CGOV.DRep,
    inverse_of = CGOV.hasVotingPowerDelegatedFrom,
)
add_object_property(
    uri     = CGOV.hasVotingPowerDelegatedFrom,
    label   = "has voting power delegated from",
    comment = "Inverse of delegatesVotingPowerTo. Relates a DRep to the stake addresses that delegate to it.",
    domain  = CGOV.DRep,
    range_  = CGOV.StakeAddress,
)
add_object_property(
    uri        = CGOV.delegatesStakeTo,
    label      = "delegates stake to",
    comment    = "Relates a stake address to the stake pool to which it has delegated its ADA for block production.",
    domain     = CGOV.StakeAddress,
    range_     = CGOV.StakePool,
    inverse_of = CGOV.hasStakeDelegatedFrom,
)
add_object_property(
    uri     = CGOV.hasStakeDelegatedFrom,
    label   = "has stake delegated from",
    comment = "Inverse of delegatesStakeTo. Relates a stake pool to its delegators.",
    domain  = CGOV.StakePool,
    range_  = CGOV.StakeAddress,
)

# Voting — domain is DRep OR StakePool (both can cast votes)
add_object_property(
    uri     = CGOV.castVote,
    label   = "cast vote",
    comment = "Relates a voter (DRep or SPO) to a Vote instance they submitted on-chain.",
    domain  = named_union_class(CGOV.DRepOrSPO, "DRep or SPO", "A voter that is either a Delegated Representative or a Stake Pool Operator.", CGOV.DRep, CGOV.StakePool),
    range_  = CGOV.Vote,
)
add_object_property(
    uri     = CGOV.voteOnProposal,
    label   = "vote on proposal",
    comment = "Relates a Vote to the Proposal it was cast on.",
    domain  = CGOV.Vote,
    range_  = CGOV.Proposal,
)
add_object_property(
    uri     = CGOV.hasVoteOption,
    label   = "has vote option",
    comment = "Relates a Vote to its option: cgov:Yes, cgov:No, or cgov:Abstain.",
    domain  = CGOV.Vote,
    range_  = SKOS.Concept,
)

# Proposal
add_object_property(
    uri     = CGOV.hasProposalType,
    label   = "has proposal type",
    comment = "Relates a Proposal to its governance action type from the ProposalTypeScheme.",
    domain  = CGOV.Proposal,
    range_  = SKOS.Concept,
)
add_object_property(
    uri     = CGOV.expiresAtEpoch,
    label   = "expires at epoch",
    comment = "Relates a Proposal to the Epoch at which it expires if not ratified.",
    domain  = CGOV.Proposal,
    range_  = CGOV.Epoch,
)

# submittedInEpoch — domain is Proposal OR Vote
add_object_property(
    uri     = CGOV.submittedInEpoch,
    label   = "submitted in epoch",
    comment = "Relates a Proposal or Vote to the Epoch in which it was submitted to the chain.",
    domain  = named_union_class(CGOV.ProposalOrVote, "Proposal or Vote", "An on-chain governance action that is either a Proposal submission or a Vote cast.", CGOV.Proposal, CGOV.Vote),
    range_  = CGOV.Epoch,
)
add_object_property(
    uri     = CGOV.activeInEpoch,
    label   = "active in epoch",
    comment = "Relates a StakePool to an Epoch in which it produced blocks and had active delegated stake.",
    domain  = CGOV.StakePool,
    range_  = CGOV.Epoch,
)



ALL_ENTITIES = named_union_class(
    CGOV.GovernanceEntity,
    "Governance Entity",
    "Any first-class entity in the Cardano governance knowledge graph: DRep, StakePool, StakeAddress, Proposal, Vote, or Epoch.",
    CGOV.DRep, CGOV.StakePool, CGOV.StakeAddress, CGOV.Proposal, CGOV.Vote, CGOV.Epoch
)

METADATA_ANCHOR_DOMAIN = named_union_class(CGOV.ProposalOrDRep, "Proposal or DRep", "An entity that carries an off-chain metadata anchor: either a Governance Proposal or a Delegated Representative.", CGOV.Proposal, CGOV.DRep)

# Provenance — attached to every ingested entity
add_datatype_property(
    uri            = CGOV.retrievedFromEndpoint,
    label          = "retrieved from endpoint",
    comment        = "The Blockfrost REST API endpoint path from which this entity was retrieved. Supports provenance and reproducibility.",
    domain         = ALL_ENTITIES,
    range_         = XSD.anyURI,
    subproperty_of = PROV.wasGeneratedBy,
)
add_datatype_property(
    uri            = CGOV.retrievedAt,
    label          = "retrieved at",
    comment        = "ISO 8601 timestamp at which this data was fetched from the Blockfrost API.",
    domain         = ALL_ENTITIES,
    range_         = XSD.dateTime,
    subproperty_of = PROV.generatedAtTime,
)

# DRep
add_datatype_property(CGOV.drepId,          "DRep ID",           "Bech32-encoded DRep identifier (drep1-).",                                    CGOV.DRep, XSD.string)
add_datatype_property(CGOV.drepHex,         "DRep hex",          "Raw hexadecimal bytes of the DRep credential.",                               CGOV.DRep, XSD.string)
add_datatype_property(CGOV.isActive,        "is active",         "True if the DRep is currently active (not retired or expired).",              CGOV.DRep, XSD.boolean)
add_datatype_property(CGOV.isRetired,       "is retired",        "True if the DRep has explicitly deregistered.",                               CGOV.DRep, XSD.boolean)
add_datatype_property(CGOV.isExpired,       "is expired",        "True if the DRep has been inactive for more than drep_activity epochs.",       CGOV.DRep, XSD.boolean)
add_datatype_property(CGOV.hasScript,       "has script",        "True if the DRep credential is a native or Plutus script rather than a key.", CGOV.DRep, XSD.boolean)
add_datatype_property(CGOV.activeEpoch,     "active epoch",      "Epoch number in which this DRep was first registered.",                       CGOV.DRep, XSD.integer)
add_datatype_property(CGOV.lastActiveEpoch, "last active epoch", "Epoch of the DRep's most recent on-chain action.",                            CGOV.DRep, XSD.integer)

# Voting power / stake (always in Lovelaces — 1 ADA = 1,000,000 Lovelace)
add_datatype_property(CGOV.votingPowerLovelace,    "voting power (Lovelace)",    "Total voting power of a DRep in Lovelaces.",                      CGOV.DRep,         XSD.integer)
add_datatype_property(CGOV.delegatedStakeLovelace, "delegated stake (Lovelace)", "ADA (Lovelaces) delegated by a stake address to a DRep or pool.", CGOV.StakeAddress, XSD.integer)

# Stake pool
add_datatype_property(CGOV.poolId,                "pool ID",                    "Bech32-encoded stake pool identifier (pool1-).",              CGOV.StakePool, XSD.string)
add_datatype_property(CGOV.poolTicker,            "pool ticker",                "Short ticker symbol identifying the stake pool (e.g. IOHK).", CGOV.StakePool, XSD.string)
add_datatype_property(CGOV.liveStakeLovelace,     "live stake (Lovelace)",      "Current live delegated stake to the pool in Lovelaces.",      CGOV.StakePool, XSD.integer)
add_datatype_property(CGOV.declaredPledgeLovelace,"declared pledge (Lovelace)", "ADA the operator pledged to keep delegated to their pool.",   CGOV.StakePool, XSD.integer)
add_datatype_property(CGOV.marginFee,             "margin fee",                 "Variable fee as a decimal fraction (e.g. 0.02 = 2%).",        CGOV.StakePool, XSD.decimal)
add_datatype_property(CGOV.fixedCostLovelace,     "fixed cost (Lovelace)",      "Pool's minimum fixed fee per epoch in Lovelaces.",            CGOV.StakePool, XSD.integer)

# Proposal
add_datatype_property(CGOV.proposalTxHash,    "proposal transaction hash",  "Transaction hash in which this governance proposal was submitted.",    CGOV.Proposal, XSD.string)
add_datatype_property(CGOV.proposalCertIndex, "proposal certificate index", "Index of the governance certificate within the proposal transaction.", CGOV.Proposal, XSD.integer)
add_datatype_property(CGOV.depositLovelace,   "deposit (Lovelace)",         "ADA deposit locked when this proposal was submitted.",                CGOV.Proposal, XSD.integer)

# metadataAnchorUrl / Hash — domain is Proposal OR DRep (both carry metadata anchors)
add_datatype_property(CGOV.metadataAnchorUrl,  "metadata anchor URL",  "URL to off-chain metadata associated with this proposal or DRep.",    METADATA_ANCHOR_DOMAIN, XSD.anyURI)
add_datatype_property(CGOV.metadataAnchorHash, "metadata anchor hash", "Hash of the off-chain metadata document for integrity verification.", METADATA_ANCHOR_DOMAIN, XSD.string)

# Vote
add_datatype_property(CGOV.voteTxHash, "vote transaction hash", "Transaction hash of the transaction in which this vote was cast.",             CGOV.Vote, XSD.string)
add_datatype_property(CGOV.voterRole,  "voter role",            "Governance role of the voter: 'drep', 'spo', or 'constitutional_committee'.", CGOV.Vote, XSD.string)

# Epoch
add_datatype_property(CGOV.epochNumber,              "epoch number",                   "Sequential integer identifier of this epoch.",              CGOV.Epoch, XSD.integer)
add_datatype_property(CGOV.epochStartTime,           "epoch start time",               "UNIX timestamp of the start of this epoch.",                CGOV.Epoch, XSD.integer)
add_datatype_property(CGOV.epochEndTime,             "epoch end time",                 "UNIX timestamp of the end of this epoch.",                  CGOV.Epoch, XSD.integer)
add_datatype_property(CGOV.totalActiveStakeLovelace, "total active stake (Lovelace)",  "Total active stake across all pools during this epoch.",    CGOV.Epoch, XSD.integer)

# StakeAddress
add_datatype_property(CGOV.stakeAddress, "stake address", "Bech32-encoded stake address (stake1-).", CGOV.StakeAddress, XSD.string)



PREDEFINED_DREPS = [
    (
        CGOV.AlwaysAbstain,
        "drep_always_abstain",
        "Always Abstain",
        (
            "A predefined CIP-1694 DRep. Stake delegated here counts toward quorum "
            "but always registers as Abstain on every proposal. Used by holders who "
            "wish to participate in quorum without expressing a directional preference."
        ),
    ),
    (
        CGOV.AlwaysNoConfidence,
        "drep_always_no_confidence",
        "Always No Confidence",
        (
            "A predefined CIP-1694 DRep. Stake delegated here always votes No Confidence "
            "on every proposal. Used by holders who wish to signal permanent opposition "
            "to the current governance structure."
        ),
    ),
]

for uri, drep_id, label, comment in PREDEFINED_DREPS:
    g.add((uri, RDF.type,     CGOV.DRep))
    g.add((uri, CGOV.drepId,  Literal(drep_id)))
    g.add((uri, RDFS.label,   Literal(label)))
    g.add((uri, RDFS.comment, Literal(comment)))
    g.add((uri, SKOS.note,    Literal("Not a registered actor — a protocol-level constant defined in CIP-1694.")))


# =============================================================================
#  9. Serialise to ontology.ttl
# =============================================================================

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "ontology.ttl")

g.serialize(destination=OUTPUT_PATH, format="turtle")

print(f"Ontology written to {OUTPUT_PATH}")
print(f"  Total triples:       {len(g)}")
print(f"  Classes:             {len(list(g.subjects(RDF.type, OWL.Class)))}")
print(f"  Object properties:   {len(list(g.subjects(RDF.type, OWL.ObjectProperty)))}")
print(f"  Datatype properties: {len(list(g.subjects(RDF.type, OWL.DatatypeProperty)))}")
print(f"  SKOS concepts:       {len(list(g.subjects(RDF.type, SKOS.Concept)))}")
print(f"  Individuals:         {len(list(g.subjects(RDF.type, CGOV.DRep)))}")