"""
Data models for the crypto subledger.

All tables from the three-layer architecture:
  Layer 1 (Raw): RawBlock, RawTransaction, RawLog, RawTrace
  Layer 2 (Normalized): Movement
  Layer 3 (Derived): BalanceSnapshot, ReconciliationResult, Checkpoint
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List, Dict, Any

import pandas as pd


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class FinalityStatus(str, Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    FINALIZED = "finalized"


class TraceProvider(str, Enum):
    NONE = "none"
    ETHERSCAN_INTERNAL = "etherscan_internal"
    DEBUG_TRACE = "debug_trace"


class TraceCompleteness(str, Enum):
    """Determines ETH sourcing policy per transaction.

    full_call_tree   -> ALL native ETH from traces, tx.value ignored
    internals_only   -> Top-level ETH from tx.value, internal from traces
    none             -> ETH from tx.value only; internal movements missing
    """
    FULL_CALL_TREE = "full_call_tree"
    INTERNALS_ONLY = "internals_only"
    NONE = "none"


class MovementKind(str, Enum):
    TRANSFER = "TRANSFER"
    FEE = "FEE"
    MINT = "MINT"
    BURN = "BURN"
    WRAP = "WRAP"
    UNWRAP = "UNWRAP"
    SELFDESTRUCT = "SELFDESTRUCT"
    CONTRACT_CREATION = "CONTRACT_CREATION"


class SourceType(str, Enum):
    TX_VALUE = "TX_VALUE"
    LOG = "LOG"
    TRACE = "TRACE"
    GAS_FEE = "GAS_FEE"


# ---------------------------------------------------------------------------
# Layer 1: Raw ingestion
# ---------------------------------------------------------------------------

@dataclass
class RawBlock:
    """One row per block we've ingested data from — the reorg backbone."""
    block_number: int
    block_hash: str
    parent_hash: str
    block_timestamp: datetime
    finality_status: str = FinalityStatus.PENDING.value
    tx_count: int = 0
    ingested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["block_timestamp"] = self.block_timestamp.isoformat()
        d["ingested_at"] = self.ingested_at.isoformat()
        return d


@dataclass
class RawTransaction:
    """One row per transaction receipt."""
    tx_hash: str
    block_number: int
    block_hash: str
    block_timestamp: datetime
    from_address: str
    to_address: Optional[str]  # null for contract creation
    value_wei: str  # string for precision
    gas_used: int
    gas_price_wei: str
    effective_gas_price_wei: str
    tx_status: int  # 1=success, 0=revert
    tx_type: int = 0  # 0=legacy, 1=access-list, 2=EIP-1559
    nonce: int = 0
    input_data: str = ""  # first 10 chars (method selector)
    contract_created: Optional[str] = None
    log_count: int = 0
    trace_count: Optional[int] = None
    max_fee_per_gas_wei: Optional[str] = None
    max_priority_fee_wei: Optional[str] = None
    trace_provider: str = TraceProvider.NONE.value
    trace_completeness: str = TraceCompleteness.NONE.value
    finality_status: str = FinalityStatus.PENDING.value
    ingested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["block_timestamp"] = self.block_timestamp.isoformat()
        d["ingested_at"] = self.ingested_at.isoformat()
        return d


@dataclass
class RawLog:
    """One row per event log."""
    log_id: str  # {tx_hash}:{log_index}
    tx_hash: str
    log_index: int
    block_number: int
    block_timestamp: datetime
    contract_address: str
    topic0: str
    topic1: Optional[str] = None
    topic2: Optional[str] = None
    topic3: Optional[str] = None
    data: str = ""
    decoded_event_name: Optional[str] = None
    removed: bool = False
    finality_status: str = FinalityStatus.PENDING.value
    ingested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["block_timestamp"] = self.block_timestamp.isoformat()
        d["ingested_at"] = self.ingested_at.isoformat()
        return d


@dataclass
class RawTrace:
    """One row per internal call/message. Used exclusively for native ETH movements."""
    trace_id: str  # {tx_hash}:trace:{trace_address}
    tx_hash: str
    trace_address: str  # "" for top-level, "0", "0_1", "0_1_0"
    block_number: int
    block_timestamp: datetime
    call_type: str  # call/delegatecall/staticcall/create/create2/selfdestruct
    from_address: str
    to_address: str
    value_wei: str  # ETH in wei, string
    gas_used: int = 0
    input_data: str = ""  # method selector first 10 chars
    output_data: str = ""
    error: Optional[str] = None
    subtraces: int = 0
    trace_provider: str = TraceProvider.ETHERSCAN_INTERNAL.value
    finality_status: str = FinalityStatus.PENDING.value
    ingested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["block_timestamp"] = self.block_timestamp.isoformat()
        d["ingested_at"] = self.ingested_at.isoformat()
        return d


# ---------------------------------------------------------------------------
# Layer 2: Normalized movements (the subledger)
# ---------------------------------------------------------------------------

@dataclass
class Movement:
    """One row per balance delta per wallet.

    Stores a signed delta in smallest units (wei / base units).
    Balance derivation is just SUM(amount_delta_raw).
    """
    movement_id: str  # deterministic: {tx_hash}:{wallet_address}:{source_type}:{source_index}
    tx_hash: str
    block_number: int
    block_timestamp: datetime
    wallet_address: str
    asset_id: str  # canonical: {chain_id}:{asset_type}:{contract}:{token_id}
    amount_delta_raw: str  # SIGNED amount in smallest units, string for precision
    movement_kind: str  # MovementKind value
    source_type: str  # SourceType value
    source_id: str  # FK to raw_transactions/raw_logs/raw_traces PK
    counterparty: Optional[str] = None
    finality_status: str = FinalityStatus.PENDING.value
    fund_id: str = ""
    ingested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["block_timestamp"] = self.block_timestamp.isoformat()
        d["ingested_at"] = self.ingested_at.isoformat()
        return d

    @property
    def direction(self) -> str:
        """Derived: IN if positive delta, OUT if negative."""
        try:
            return "IN" if int(self.amount_delta_raw) > 0 else "OUT"
        except (ValueError, TypeError):
            return "UNKNOWN"


# ---------------------------------------------------------------------------
# Layer 3: Derived state
# ---------------------------------------------------------------------------

@dataclass
class BalanceSnapshot:
    """Derived from movements. Computed, never manually edited."""
    snapshot_id: str  # {wallet_address}:{asset_id}:{block_number}
    wallet_address: str
    asset_id: str
    block_number: int
    block_timestamp: datetime
    balance_raw: str  # SUM(amount_delta_raw) in smallest units
    movement_count: int = 0
    last_movement_id: str = ""
    fund_id: str = ""
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["block_timestamp"] = self.block_timestamp.isoformat()
        d["computed_at"] = self.computed_at.isoformat()
        return d


@dataclass
class ReconciliationResult:
    """Proof mode: on-chain vs derived balance comparison."""
    recon_id: str
    wallet_address: str
    asset_id: str
    block_number: int
    derived_balance_raw: str
    onchain_balance_raw: str
    variance_raw: str
    is_match: bool
    diagnosis: Optional[str] = None
    trace_completeness: str = TraceCompleteness.NONE.value
    proof_run_id: str = ""
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["checked_at"] = self.checked_at.isoformat()
        return d


@dataclass
class Checkpoint:
    """Known-good point for replay."""
    checkpoint_id: str
    block_number: int
    block_hash: str
    wallet_count: int = 0
    movement_count: int = 0
    balance_hash: str = ""  # deterministic hash of all balances at this block
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["created_at"] = self.created_at.isoformat()
        return d


# ---------------------------------------------------------------------------
# DataFrame conversion helpers
# ---------------------------------------------------------------------------

def _records_to_df(records: List, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Convert a list of dataclass instances to a DataFrame."""
    if not records:
        if columns:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame()
    dicts = [r.to_dict() for r in records]
    df = pd.DataFrame(dicts)
    if columns:
        for col in columns:
            if col not in df.columns:
                df[col] = ""
        df = df[columns]
    return df


# Column lists for parquet schemas — match the dataclass fields exactly

RAW_BLOCK_COLUMNS = [
    "block_number", "block_hash", "parent_hash", "block_timestamp",
    "finality_status", "tx_count", "ingested_at",
]

RAW_TRANSACTION_COLUMNS = [
    "tx_hash", "block_number", "block_hash", "block_timestamp",
    "from_address", "to_address", "value_wei", "gas_used", "gas_price_wei",
    "effective_gas_price_wei", "tx_status", "tx_type", "nonce", "input_data",
    "contract_created", "log_count", "trace_count",
    "max_fee_per_gas_wei", "max_priority_fee_wei",
    "trace_provider", "trace_completeness", "finality_status", "ingested_at",
]

RAW_LOG_COLUMNS = [
    "log_id", "tx_hash", "log_index", "block_number", "block_timestamp",
    "contract_address", "topic0", "topic1", "topic2", "topic3", "data",
    "decoded_event_name", "removed", "finality_status", "ingested_at",
]

RAW_TRACE_COLUMNS = [
    "trace_id", "tx_hash", "trace_address", "block_number", "block_timestamp",
    "call_type", "from_address", "to_address", "value_wei", "gas_used",
    "input_data", "output_data", "error", "subtraces",
    "trace_provider", "finality_status", "ingested_at",
]

MOVEMENT_COLUMNS = [
    "movement_id", "tx_hash", "block_number", "block_timestamp",
    "wallet_address", "asset_id", "amount_delta_raw", "movement_kind",
    "source_type", "source_id", "counterparty", "finality_status",
    "fund_id", "ingested_at",
]

BALANCE_SNAPSHOT_COLUMNS = [
    "snapshot_id", "wallet_address", "asset_id", "block_number",
    "block_timestamp", "balance_raw", "movement_count", "last_movement_id",
    "fund_id", "computed_at",
]

RECONCILIATION_COLUMNS = [
    "recon_id", "wallet_address", "asset_id", "block_number",
    "derived_balance_raw", "onchain_balance_raw", "variance_raw",
    "is_match", "diagnosis", "trace_completeness", "proof_run_id", "checked_at",
]
