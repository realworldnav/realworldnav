"""
Reorg manager: Block-level finality tracking and rollback.

Reorg detection operates on raw_blocks, not transactions:
  - Scan non-finalized blocks for hash mismatches
  - On divergence: delete everything >= divergence block, re-ingest

MVP strategy:
  - Ingest with 3-block delay (wait for confirmations)
  - Mark everything 'confirmed' on ingest
  - Nightly reorg check for last 256 blocks
  - Manual rollback trigger (no automated rollback)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

from main_app.services.subledger.models import (
    FinalityStatus,
    RawBlock,
)
from main_app.services.subledger.storage import SubledgerStorage

logger = logging.getLogger(__name__)

# Number of confirmations before we consider a block safe
DEFAULT_CONFIRMATION_THRESHOLD = 12
# Conservative finality threshold (post-Merge Ethereum)
DEFAULT_FINALITY_THRESHOLD = 64
# Number of recent blocks to scan for reorgs
DEFAULT_SCAN_DEPTH = 256


class ReorgManager:
    """Manages block finality tracking and reorg detection."""

    def __init__(
        self,
        storage: SubledgerStorage,
        web3=None,
        confirmation_threshold: int = DEFAULT_CONFIRMATION_THRESHOLD,
        finality_threshold: int = DEFAULT_FINALITY_THRESHOLD,
        scan_depth: int = DEFAULT_SCAN_DEPTH,
    ):
        self.storage = storage
        self.w3 = web3
        self.confirmation_threshold = confirmation_threshold
        self.finality_threshold = finality_threshold
        self.scan_depth = scan_depth

    def _get_web3(self):
        """Lazy-initialize Web3 connection."""
        if self.w3 is not None:
            return self.w3
        try:
            from web3 import Web3
            infura_key = os.getenv("INFURA_API_KEY", "")
            if infura_key:
                self.w3 = Web3(Web3.HTTPProvider(f"https://mainnet.infura.io/v3/{infura_key}"))
        except ImportError:
            logger.error("web3 package not installed")
        return self.w3

    # ------------------------------------------------------------------
    # Finality status updates
    # ------------------------------------------------------------------

    def update_finality_status(self) -> Dict[str, int]:
        """Update finality_status on all non-finalized blocks.

        Returns dict with counts: {pending -> confirmed, confirmed -> finalized}.
        """
        w3 = self._get_web3()
        if w3 is None:
            logger.warning("No Web3 connection; cannot update finality")
            return {}

        current_block = w3.eth.block_number
        blocks_df = self.storage.load_raw_blocks()

        if blocks_df.empty:
            return {}

        promoted = {"to_confirmed": 0, "to_finalized": 0}

        # Filter non-finalized blocks
        non_final = blocks_df[blocks_df["finality_status"] != FinalityStatus.FINALIZED.value]
        if non_final.empty:
            return promoted

        for _, row in non_final.iterrows():
            block_num = int(row["block_number"])
            depth = current_block - block_num
            current_status = row["finality_status"]

            if depth >= self.finality_threshold and current_status != FinalityStatus.FINALIZED.value:
                # Promote to finalized
                blocks_df.loc[
                    blocks_df["block_number"] == block_num, "finality_status"
                ] = FinalityStatus.FINALIZED.value
                promoted["to_finalized"] += 1
            elif depth >= self.confirmation_threshold and current_status == FinalityStatus.PENDING.value:
                # Promote to confirmed
                blocks_df.loc[
                    blocks_df["block_number"] == block_num, "finality_status"
                ] = FinalityStatus.CONFIRMED.value
                promoted["to_confirmed"] += 1

        if promoted["to_confirmed"] > 0 or promoted["to_finalized"] > 0:
            # Save updated blocks (find the range and re-save)
            start = int(blocks_df["block_number"].min())
            end = int(blocks_df["block_number"].max())
            key = f"{self.storage._base}/raw/blocks/blocks_{start}_{end}.parquet"
            self.storage._save_df(blocks_df, key)
            logger.info(
                f"Finality update: {promoted['to_confirmed']} confirmed, "
                f"{promoted['to_finalized']} finalized"
            )

        return promoted

    # ------------------------------------------------------------------
    # Reorg detection
    # ------------------------------------------------------------------

    def find_reorg_point(self) -> Optional[int]:
        """Scan non-finalized blocks for the first hash divergence.

        Returns the block number where reorg occurred, or None if no reorg.
        """
        w3 = self._get_web3()
        if w3 is None:
            logger.warning("No Web3 connection; cannot check for reorgs")
            return None

        blocks_df = self.storage.load_raw_blocks()
        if blocks_df.empty:
            return None

        # Only check non-finalized blocks
        non_final = blocks_df[blocks_df["finality_status"] != FinalityStatus.FINALIZED.value]
        if non_final.empty:
            logger.debug("All blocks are finalized; no reorg check needed")
            return None

        non_final = non_final.sort_values("block_number")

        for _, row in non_final.iterrows():
            block_num = int(row["block_number"])
            stored_hash = str(row["block_hash"]).lower()

            try:
                canonical = w3.eth.get_block(block_num)
                canonical_hash = canonical["hash"].hex().lower()
            except Exception as e:
                logger.warning(f"Failed to fetch block {block_num} from chain: {e}")
                continue

            if stored_hash != canonical_hash:
                logger.warning(
                    f"REORG DETECTED at block {block_num}: "
                    f"stored={stored_hash[:16]}... canonical={canonical_hash[:16]}..."
                )
                return block_num

        logger.debug(f"No reorg detected across {len(non_final)} non-finalized blocks")
        return None

    def check_chain_continuity(self) -> List[int]:
        """Verify parent_hash chain continuity.

        Returns list of block numbers where continuity is broken.
        """
        blocks_df = self.storage.load_raw_blocks()
        if blocks_df.empty or len(blocks_df) < 2:
            return []

        blocks_df = blocks_df.sort_values("block_number").reset_index(drop=True)
        breaks = []

        for i in range(1, len(blocks_df)):
            prev = blocks_df.iloc[i - 1]
            curr = blocks_df.iloc[i]

            # Only check consecutive blocks
            if int(curr["block_number"]) != int(prev["block_number"]) + 1:
                continue

            if str(curr["parent_hash"]).lower() != str(prev["block_hash"]).lower():
                breaks.append(int(curr["block_number"]))
                logger.warning(
                    f"Chain continuity break at block {curr['block_number']}: "
                    f"parent_hash doesn't match previous block_hash"
                )

        return breaks

    # ------------------------------------------------------------------
    # Rollback
    # ------------------------------------------------------------------

    def rollback_from_block(self, block_number: int) -> int:
        """Delete all data at and above a block number.

        Returns number of rows deleted.
        """
        logger.warning(f"Rolling back all data from block {block_number}")
        # Use a very large end_block to delete everything above
        deleted = self.storage.delete_block_range(block_number, 999999999)
        logger.info(f"Rollback complete: {deleted} rows deleted from block {block_number}")
        return deleted

    # ------------------------------------------------------------------
    # Nightly reorg check (MVP)
    # ------------------------------------------------------------------

    def nightly_check(self) -> Dict:
        """Run nightly reorg check and finality updates.

        Returns summary dict.
        """
        logger.info("Starting nightly reorg check")
        summary = {
            "reorg_detected": False,
            "reorg_block": None,
            "finality_updates": {},
            "continuity_breaks": [],
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

        # 1. Update finality status
        summary["finality_updates"] = self.update_finality_status()

        # 2. Check for reorgs
        reorg_block = self.find_reorg_point()
        if reorg_block is not None:
            summary["reorg_detected"] = True
            summary["reorg_block"] = reorg_block
            logger.warning(
                f"Reorg detected at block {reorg_block}. "
                f"Manual intervention required — run rollback_from_block({reorg_block}) "
                f"then re-ingest."
            )

        # 3. Check chain continuity
        summary["continuity_breaks"] = self.check_chain_continuity()

        logger.info(f"Nightly check complete: {summary}")
        return summary
