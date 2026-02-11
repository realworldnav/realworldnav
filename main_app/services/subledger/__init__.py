"""
Crypto Subledger — Pre-accounting layer of factual chain-derived balance movements.

Three strict layers. Data flows down only.

    Layer 1: RAW INGESTION (chain facts, stored verbatim)
        raw_blocks, raw_transactions, raw_logs, raw_traces

    Layer 2: NORMALIZED MOVEMENTS (the subledger)
        movements — one row per balance delta per wallet

    Layer 3: DERIVED STATE (computed, never edited)
        balance_snapshots, reconciliation_proofs
"""

from main_app.services.subledger.models import (
    RawBlock,
    RawTransaction,
    RawLog,
    RawTrace,
    Movement,
    BalanceSnapshot,
    ReconciliationResult,
    Checkpoint,
    MovementKind,
    SourceType,
    FinalityStatus,
    TraceCompleteness,
    TraceProvider,
)
from main_app.services.subledger.asset_registry import AssetRegistry, resolve_asset_id
from main_app.services.subledger.token_filter import TokenFilter, TokenFilterResult

__all__ = [
    "RawBlock",
    "RawTransaction",
    "RawLog",
    "RawTrace",
    "Movement",
    "BalanceSnapshot",
    "ReconciliationResult",
    "Checkpoint",
    "MovementKind",
    "SourceType",
    "FinalityStatus",
    "TraceCompleteness",
    "TraceProvider",
    "AssetRegistry",
    "resolve_asset_id",
    "TokenFilter",
    "TokenFilterResult",
]
