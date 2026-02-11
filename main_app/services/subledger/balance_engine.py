"""
Balance engine: Movement -> Balance snapshot derivation.

Balance at any block is just SUM(amount_delta_raw) of all movements
up to and including that block, grouped by wallet x asset.

For MVP, computed on-the-fly (no cached snapshots).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd

from main_app.services.subledger.models import (
    Movement,
    BalanceSnapshot,
    MOVEMENT_COLUMNS,
)
from main_app.services.subledger.storage import SubledgerStorage

logger = logging.getLogger(__name__)


class BalanceEngine:
    """Compute wallet x asset balances from movements."""

    def __init__(self, storage: SubledgerStorage, fund_id: str = ""):
        self.storage = storage
        self.fund_id = fund_id

    def get_balance(
        self,
        wallet_address: str,
        asset_id: str,
        up_to_block: Optional[int] = None,
    ) -> int:
        """Compute balance as SUM(amount_delta_raw) for a specific wallet x asset.

        Args:
            wallet_address: Wallet to query (lowercase).
            asset_id: Canonical asset id.
            up_to_block: Include movements at or below this block. None = all.

        Returns:
            Balance in smallest units (wei / base units) as int.
        """
        df = self.storage.load_movements(
            end_block=up_to_block,
            wallet_address=wallet_address.lower(),
            asset_id=asset_id,
        )
        if df.empty:
            return 0

        # Ensure amount_delta_raw is string, convert to int, sum
        total = sum(int(x) for x in df["amount_delta_raw"])
        return total

    def get_all_balances(
        self,
        up_to_block: Optional[int] = None,
        wallet_address: Optional[str] = None,
    ) -> pd.DataFrame:
        """Compute all wallet x asset balances as of a block.

        Returns DataFrame with columns:
            wallet_address, asset_id, balance_raw, movement_count, last_block
        """
        df = self.storage.load_movements(
            end_block=up_to_block,
            wallet_address=wallet_address.lower() if wallet_address else None,
        )
        if df.empty:
            return pd.DataFrame(columns=[
                "wallet_address", "asset_id", "balance_raw", "movement_count", "last_block"
            ])

        # Convert amount_delta_raw to int
        df["delta_int"] = df["amount_delta_raw"].apply(lambda x: int(x))

        grouped = df.groupby(["wallet_address", "asset_id"]).agg(
            balance_raw=("delta_int", "sum"),
            movement_count=("movement_id", "count"),
            last_block=("block_number", "max"),
        ).reset_index()

        grouped["balance_raw"] = grouped["balance_raw"].apply(str)
        return grouped

    def get_active_assets(
        self,
        wallet_address: str,
        up_to_block: Optional[int] = None,
    ) -> List[str]:
        """Return list of asset_ids that have non-zero balance for a wallet."""
        balances = self.get_all_balances(
            up_to_block=up_to_block,
            wallet_address=wallet_address,
        )
        if balances.empty:
            return []
        # Filter non-zero
        balances = balances[balances["balance_raw"].apply(lambda x: int(x) != 0)]
        return balances["asset_id"].tolist()

    def compute_snapshots(
        self,
        block_number: int,
        block_timestamp: Optional[datetime] = None,
    ) -> List[BalanceSnapshot]:
        """Compute balance snapshots for ALL wallet x asset pairs at a block.

        Returns list of BalanceSnapshot objects.
        """
        if block_timestamp is None:
            block_timestamp = datetime.now(timezone.utc)

        balances = self.get_all_balances(up_to_block=block_number)
        if balances.empty:
            return []

        snapshots = []
        for _, row in balances.iterrows():
            wallet = row["wallet_address"]
            asset = row["asset_id"]
            snap_id = f"{wallet}:{asset}:{block_number}"

            # Find last movement_id for this wallet x asset
            df = self.storage.load_movements(
                end_block=block_number,
                wallet_address=wallet,
                asset_id=asset,
            )
            last_mv_id = ""
            if not df.empty:
                last_mv_id = df.iloc[-1]["movement_id"]

            snapshots.append(BalanceSnapshot(
                snapshot_id=snap_id,
                wallet_address=wallet,
                asset_id=asset,
                block_number=block_number,
                block_timestamp=block_timestamp,
                balance_raw=str(row["balance_raw"]),
                movement_count=int(row["movement_count"]),
                last_movement_id=last_mv_id,
                fund_id=self.fund_id,
            ))

        logger.info(
            f"Computed {len(snapshots)} balance snapshots at block {block_number}"
        )
        return snapshots

    def get_movement_history(
        self,
        wallet_address: str,
        asset_id: Optional[str] = None,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
    ) -> pd.DataFrame:
        """Get movement history with running balance for a wallet.

        Returns DataFrame with all movement columns plus running_balance_raw.
        """
        df = self.storage.load_movements(
            start_block=start_block,
            end_block=end_block,
            wallet_address=wallet_address.lower(),
            asset_id=asset_id,
        )
        if df.empty:
            return df

        df = df.sort_values(["block_number", "movement_id"]).reset_index(drop=True)
        df["delta_int"] = df["amount_delta_raw"].apply(lambda x: int(x))

        # Running balance per asset
        df["running_balance_raw"] = ""
        for asset in df["asset_id"].unique():
            mask = df["asset_id"] == asset
            cumsum = df.loc[mask, "delta_int"].cumsum()
            df.loc[mask, "running_balance_raw"] = cumsum.apply(str)

        df = df.drop(columns=["delta_int"])
        return df
