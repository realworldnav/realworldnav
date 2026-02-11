"""
Replay orchestration: full and range replay for the subledger.

Replay is the mechanism that ensures correctness:
  - After a normalizer change, replay recalculates all movements
  - After a reorg, replay re-ingests affected blocks
  - Full replay rebuilds everything from scratch

All replays are idempotent: block-range-scoped delete-then-write.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional, Set

from main_app.services.subledger.models import Checkpoint
from main_app.services.subledger.ingestion import SubledgerIngester
from main_app.services.subledger.normalizer import MovementNormalizer
from main_app.services.subledger.balance_engine import BalanceEngine
from main_app.services.subledger.storage import SubledgerStorage

logger = logging.getLogger(__name__)


class SubledgerReplay:
    """Orchestrates full and partial replay of the subledger pipeline."""

    def __init__(
        self,
        storage: SubledgerStorage,
        ingester: SubledgerIngester,
        our_wallets: Set[str],
        fund_id: str = "",
        chain_id: int = 1,
    ):
        self.storage = storage
        self.ingester = ingester
        self.our_wallets = {w.lower() for w in our_wallets}
        self.fund_id = fund_id
        self.chain_id = chain_id

    def _normalize(self, raw_txs, raw_logs, raw_traces):
        """Create a normalizer and run it."""
        normalizer = MovementNormalizer(
            our_wallets=self.our_wallets,
            chain_id=self.chain_id,
            fund_id=self.fund_id,
        )
        return normalizer.normalize_all(raw_txs, raw_logs, raw_traces)

    # ------------------------------------------------------------------
    # Block range ingestion (idempotent)
    # ------------------------------------------------------------------

    def ingest_block_range(
        self,
        start_block: int,
        end_block: int,
        wallet_addresses: Optional[List[str]] = None,
    ) -> int:
        """Idempotent ingestion for a block range.

        1. Fetch raw data from chain
        2. Delete existing data for this range
        3. Write raw data
        4. Normalize raw -> movements
        5. Write movements

        Returns number of movements created.
        """
        wallets = wallet_addresses or list(self.our_wallets)
        logger.info(
            f"Ingesting block range {start_block}-{end_block} "
            f"for {len(wallets)} wallets"
        )

        # 1. Fetch raw data
        raw_txs, raw_logs, raw_traces, block_numbers = self.ingester.ingest_wallets(
            wallets, start_block, end_block
        )

        if not raw_txs:
            logger.info("No transactions found in range")
            return 0

        # 2. Delete existing data for this block range
        self.storage.delete_block_range(start_block, end_block)

        # 3. Write raw data (skip block headers — only needed for reorg detection)
        self.storage.save_raw_transactions(raw_txs, start_block, end_block)
        self.storage.save_raw_logs(raw_logs, start_block, end_block)
        self.storage.save_raw_traces(raw_traces, start_block, end_block)

        # 5. Normalize
        movements = self._normalize(raw_txs, raw_logs, raw_traces)

        # 6. Write movements
        self.storage.save_movements(movements, start_block, end_block)

        logger.info(
            f"Block range {start_block}-{end_block}: "
            f"{len(raw_txs)} txs, {len(raw_logs)} logs, {len(raw_traces)} traces "
            f"-> {len(movements)} movements"
        )
        return len(movements)

    # ------------------------------------------------------------------
    # Full replay
    # ------------------------------------------------------------------

    def full_replay(
        self,
        start_block: int = 0,
        end_block: int = 99999999,
        chunk_size: int = 100000,
    ) -> int:
        """Full replay: delete all data, re-ingest from start.

        Processes in chunks to avoid memory issues.

        Returns total number of movements created.
        """
        logger.info(
            f"Starting full replay from block {start_block} to {end_block}"
        )

        # Delete all existing data first
        self.storage.delete_block_range(start_block, end_block)

        total_movements = 0
        current = start_block

        while current <= end_block:
            chunk_end = min(current + chunk_size - 1, end_block)
            count = self.ingest_block_range(current, chunk_end)
            total_movements += count

            if count == 0 and chunk_end >= end_block:
                break
            current = chunk_end + 1

        logger.info(f"Full replay complete: {total_movements} total movements")
        return total_movements

    # ------------------------------------------------------------------
    # Single wallet replay
    # ------------------------------------------------------------------

    def replay_wallet(
        self,
        wallet_address: str,
        start_block: int = 0,
        end_block: int = 99999999,
    ) -> int:
        """Replay ingestion for a single wallet.

        Note: This fetches data for one wallet but movements depend on the
        full raw dataset. Best used for initial sync of a new wallet.
        """
        logger.info(f"Replaying wallet {wallet_address[:10]}...")
        return self.ingest_block_range(
            start_block, end_block,
            wallet_addresses=[wallet_address],
        )

    # ------------------------------------------------------------------
    # Re-normalize from existing raw data (no re-fetch)
    # ------------------------------------------------------------------

    def renormalize(
        self,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
    ) -> int:
        """Re-run normalization on existing raw data without re-fetching.

        Useful after normalizer code changes — just re-derive movements
        from already-stored raw data.

        Returns number of movements created.
        """
        logger.info(
            f"Re-normalizing from stored raw data "
            f"(blocks {start_block or 'start'} to {end_block or 'end'})"
        )

        # Load existing raw data
        raw_txs_df = self.storage.load_raw_transactions(start_block, end_block)
        raw_logs_df = self.storage.load_raw_logs(start_block, end_block)
        raw_traces_df = self.storage.load_raw_traces(start_block, end_block)

        if raw_txs_df.empty:
            logger.info("No raw transactions found for re-normalization")
            return 0

        # Convert DataFrames back to dataclass instances
        from main_app.services.subledger.models import RawTransaction, RawLog, RawTrace

        raw_txs = []
        for _, row in raw_txs_df.iterrows():
            raw_txs.append(RawTransaction(
                tx_hash=row["tx_hash"],
                block_number=int(row["block_number"]),
                block_hash=str(row.get("block_hash", "")),
                block_timestamp=_parse_stored_timestamp(row["block_timestamp"]),
                from_address=str(row["from_address"]),
                to_address=str(row["to_address"]) if row.get("to_address") else None,
                value_wei=str(row["value_wei"]),
                gas_used=int(row["gas_used"]),
                gas_price_wei=str(row["gas_price_wei"]),
                effective_gas_price_wei=str(row["effective_gas_price_wei"]),
                tx_status=int(row["tx_status"]),
                tx_type=int(row.get("tx_type", 0)),
                nonce=int(row.get("nonce", 0)),
                trace_provider=str(row.get("trace_provider", "none")),
                trace_completeness=str(row.get("trace_completeness", "none")),
                finality_status=str(row.get("finality_status", "confirmed")),
            ))

        raw_logs = []
        for _, row in raw_logs_df.iterrows():
            raw_logs.append(RawLog(
                log_id=str(row["log_id"]),
                tx_hash=str(row["tx_hash"]),
                log_index=int(row["log_index"]),
                block_number=int(row["block_number"]),
                block_timestamp=_parse_stored_timestamp(row["block_timestamp"]),
                contract_address=str(row["contract_address"]),
                topic0=str(row["topic0"]),
                topic1=str(row.get("topic1", "")) or None,
                topic2=str(row.get("topic2", "")) or None,
                topic3=str(row.get("topic3", "")) or None,
                data=str(row.get("data", "")),
                decoded_event_name=str(row.get("decoded_event_name", "")) or None,
                finality_status=str(row.get("finality_status", "confirmed")),
            ))

        raw_traces = []
        for _, row in raw_traces_df.iterrows():
            raw_traces.append(RawTrace(
                trace_id=str(row["trace_id"]),
                tx_hash=str(row["tx_hash"]),
                trace_address=str(row["trace_address"]),
                block_number=int(row["block_number"]),
                block_timestamp=_parse_stored_timestamp(row["block_timestamp"]),
                call_type=str(row["call_type"]),
                from_address=str(row["from_address"]),
                to_address=str(row["to_address"]),
                value_wei=str(row["value_wei"]),
                error=str(row.get("error", "")) or None,
                trace_provider=str(row.get("trace_provider", "etherscan_internal")),
                finality_status=str(row.get("finality_status", "confirmed")),
            ))

        # Normalize
        movements = self._normalize(raw_txs, raw_logs, raw_traces)

        # Delete old movements in range and save new
        if start_block is not None and end_block is not None:
            # Only delete movements in the re-normalized range
            actual_start = int(raw_txs_df["block_number"].min())
            actual_end = int(raw_txs_df["block_number"].max())
            # We need to selectively delete movements only
            prefix = f"{self.storage._base}/movements/"
            keys = self.storage._list_keys(prefix)
            for key in keys:
                df = self.storage._load_df(key)
                if df.empty or "block_number" not in df.columns:
                    continue
                before = len(df)
                df = df[
                    (df["block_number"] < actual_start) | (df["block_number"] > actual_end)
                ]
                if len(df) == 0:
                    self.storage._delete(key)
                elif len(df) < before:
                    self.storage._save_df(df, key)

        # Save new movements
        if movements:
            actual_start = min(m.block_number for m in movements)
            actual_end = max(m.block_number for m in movements)
            self.storage.save_movements(movements, actual_start, actual_end)

        logger.info(f"Re-normalization complete: {len(movements)} movements")
        return len(movements)

    # ------------------------------------------------------------------
    # Checkpoint creation
    # ------------------------------------------------------------------

    def create_checkpoint(self, block_number: int, block_hash: str = "") -> Checkpoint:
        """Create a checkpoint at a specific block.

        Stores balance hash for future validation.
        """
        from main_app.services.subledger.reconciler import SubledgerReconciler

        balance_engine = BalanceEngine(self.storage, self.fund_id)
        reconciler = SubledgerReconciler(
            self.storage, balance_engine, fund_id=self.fund_id
        )

        balance_hash = reconciler.generate_balance_hash(block_number)
        balances = balance_engine.get_all_balances(up_to_block=block_number)
        movements = self.storage.load_movements(end_block=block_number)

        wallet_count = len(balances["wallet_address"].unique()) if not balances.empty else 0
        movement_count = len(movements)

        checkpoint = Checkpoint(
            checkpoint_id=f"ckpt_{block_number}",
            block_number=block_number,
            block_hash=block_hash,
            wallet_count=wallet_count,
            movement_count=movement_count,
            balance_hash=balance_hash,
        )

        self.storage.save_checkpoint(checkpoint)
        logger.info(
            f"Checkpoint created at block {block_number}: "
            f"{wallet_count} wallets, {movement_count} movements, "
            f"hash={balance_hash[:16]}..."
        )
        return checkpoint


def _parse_stored_timestamp(val) -> datetime:
    """Parse a timestamp that may be stored as ISO string or datetime."""
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val
    try:
        dt = datetime.fromisoformat(str(val))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)
