"""
Reconciler: On-chain vs derived balance comparison + proof mode.

Core principle: The subledger claims "balances correct at arbitrary block heights."
Reconciliation proves this claim.

Proof mode:
  1. Derive balance from movements: SUM(amount_delta_raw)
  2. Query on-chain balance at that block
  3. Compare (integer arithmetic, no floating point)
  4. Diagnose mismatches
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from main_app.services.subledger.models import (
    ReconciliationResult,
    TraceCompleteness,
)
from main_app.services.subledger.asset_registry import (
    is_native,
    extract_contract,
    ZERO_ADDRESS,
)
from main_app.services.subledger.balance_engine import BalanceEngine
from main_app.services.subledger.storage import SubledgerStorage

logger = logging.getLogger(__name__)

# Minimal ERC-20 ABI for balanceOf
ERC20_BALANCE_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    }
]


class SubledgerReconciler:
    """Compare derived balances against on-chain state."""

    def __init__(
        self,
        storage: SubledgerStorage,
        balance_engine: BalanceEngine,
        web3=None,
        fund_id: str = "",
    ):
        self.storage = storage
        self.balance_engine = balance_engine
        self.w3 = web3
        self.fund_id = fund_id

    def _get_web3(self):
        """Lazy-initialize Web3 connection."""
        if self.w3 is not None:
            return self.w3
        try:
            from web3 import Web3
            infura_key = os.getenv("INFURA_API_KEY", "")
            if infura_key:
                self.w3 = Web3(Web3.HTTPProvider(f"https://mainnet.infura.io/v3/{infura_key}"))
            else:
                logger.warning("No INFURA_API_KEY; on-chain queries will fail")
                return None
        except ImportError:
            logger.error("web3 package not installed")
            return None
        return self.w3

    def get_onchain_balance(
        self,
        wallet_address: str,
        asset_id: str,
        block_number: int,
    ) -> Optional[int]:
        """Query on-chain balance at a specific block.

        Returns balance in smallest units (wei / base units), or None on error.
        """
        w3 = self._get_web3()
        if w3 is None:
            return None

        try:
            wallet = w3.to_checksum_address(wallet_address)
            if is_native(asset_id):
                balance = w3.eth.get_balance(wallet, block_identifier=block_number)
                return balance
            else:
                contract_addr = extract_contract(asset_id)
                contract = w3.eth.contract(
                    address=w3.to_checksum_address(contract_addr),
                    abi=ERC20_BALANCE_ABI,
                )
                balance = contract.functions.balanceOf(wallet).call(
                    block_identifier=block_number
                )
                return balance
        except Exception as e:
            logger.error(
                f"On-chain balance query failed: {wallet_address} {asset_id} "
                f"block {block_number}: {e}"
            )
            return None

    def reconcile_single(
        self,
        wallet_address: str,
        asset_id: str,
        block_number: int,
        proof_run_id: str = "",
    ) -> ReconciliationResult:
        """Compare derived vs on-chain balance for one wallet x asset x block.

        Returns ReconciliationResult with variance and diagnosis.
        """
        wallet = wallet_address.lower()

        # 1. Derived balance from movements
        derived = self.balance_engine.get_balance(wallet, asset_id, up_to_block=block_number)

        # 2. On-chain balance
        onchain = self.get_onchain_balance(wallet, asset_id, block_number)
        if onchain is None:
            return ReconciliationResult(
                recon_id=f"{wallet}:{asset_id}:{block_number}",
                wallet_address=wallet,
                asset_id=asset_id,
                block_number=block_number,
                derived_balance_raw=str(derived),
                onchain_balance_raw="ERROR",
                variance_raw="ERROR",
                is_match=False,
                diagnosis="on-chain query failed",
                proof_run_id=proof_run_id,
            )

        # 3. Compare
        variance = derived - onchain
        is_match = variance == 0

        # 4. Diagnose mismatch
        diagnosis = None
        if not is_match:
            diagnosis = self._diagnose_mismatch(
                wallet, asset_id, block_number, derived, onchain, variance
            )

        return ReconciliationResult(
            recon_id=f"{wallet}:{asset_id}:{block_number}",
            wallet_address=wallet,
            asset_id=asset_id,
            block_number=block_number,
            derived_balance_raw=str(derived),
            onchain_balance_raw=str(onchain),
            variance_raw=str(variance),
            is_match=is_match,
            diagnosis=diagnosis,
            proof_run_id=proof_run_id,
        )

    def _diagnose_mismatch(
        self,
        wallet: str,
        asset_id: str,
        block_number: int,
        derived: int,
        onchain: int,
        variance: int,
    ) -> str:
        """Run diagnostic checks to explain a balance mismatch."""
        checks = []

        # Check 1: Missing traces
        if is_native(asset_id):
            txs = self.storage.load_raw_transactions(end_block=block_number)
            if not txs.empty:
                no_trace = txs[txs["trace_completeness"] == TraceCompleteness.NONE.value]
                if len(no_trace) > 0:
                    checks.append(
                        f"MISSING_TRACES: {len(no_trace)} txs have no trace data"
                    )

        # Check 2: Variance direction
        if variance > 0:
            checks.append(
                "OVER_COUNTED: derived > on-chain — possible double-counted movement"
            )
        else:
            checks.append(
                "UNDER_COUNTED: derived < on-chain — possible missing movement"
            )

        # Check 3: Pre-ingestion history
        movements = self.storage.load_movements(
            wallet_address=wallet,
            asset_id=asset_id,
            end_block=block_number,
        )
        if not movements.empty:
            first_block = int(movements["block_number"].min())
            checks.append(f"FIRST_MOVEMENT_BLOCK: {first_block}")
            if first_block > 0:
                checks.append(
                    "PRE_INGESTION: wallet may have had balance before first ingested block"
                )

        return "; ".join(checks) if checks else "UNKNOWN"

    # ------------------------------------------------------------------
    # Proof mode: batch reconciliation
    # ------------------------------------------------------------------

    def run_proof(
        self,
        wallets: List[str],
        asset_ids: List[str],
        block_numbers: List[int],
        proof_run_id: Optional[str] = None,
    ) -> List[ReconciliationResult]:
        """Run proof mode reconciliation across wallets x assets x blocks.

        Args:
            wallets: List of wallet addresses.
            asset_ids: List of asset_ids to check.
            block_numbers: List of block heights to verify.
            proof_run_id: Unique identifier for this proof run.

        Returns:
            List of ReconciliationResult objects.
        """
        if proof_run_id is None:
            proof_run_id = f"proof_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

        results = []
        total = len(wallets) * len(asset_ids) * len(block_numbers)
        count = 0

        for wallet in wallets:
            for asset_id in asset_ids:
                for block_number in block_numbers:
                    count += 1
                    logger.info(
                        f"Proof {count}/{total}: {wallet[:10]}... {asset_id} @ block {block_number}"
                    )
                    result = self.reconcile_single(
                        wallet, asset_id, block_number, proof_run_id
                    )
                    results.append(result)

        # Summary
        matches = sum(1 for r in results if r.is_match)
        mismatches = len(results) - matches
        errors = sum(1 for r in results if r.onchain_balance_raw == "ERROR")

        logger.info(
            f"Proof run {proof_run_id} complete: "
            f"{matches} matches, {mismatches} mismatches, {errors} errors "
            f"out of {len(results)} checks"
        )

        # Save results
        if results:
            for block_number in block_numbers:
                block_results = [r for r in results if r.block_number == block_number]
                if block_results:
                    self.storage.save_reconciliation(
                        block_results, proof_run_id, block_number
                    )

        return results

    def run_proof_latest(
        self,
        wallets: List[str],
        asset_ids: List[str],
        historical_checkpoints: int = 5,
        checkpoint_interval: int = 50000,
    ) -> List[ReconciliationResult]:
        """Run proof mode at latest block + historical checkpoints.

        Args:
            wallets: Wallet addresses to verify.
            asset_ids: Asset ids to verify.
            historical_checkpoints: Number of historical blocks to also verify.
            checkpoint_interval: Block interval between checkpoints.
        """
        w3 = self._get_web3()
        if w3 is None:
            logger.error("Cannot run proof: no Web3 connection")
            return []

        latest_block = w3.eth.block_number
        block_numbers = [latest_block]

        # Add historical checkpoints going backwards
        for i in range(1, historical_checkpoints + 1):
            historical = latest_block - (i * checkpoint_interval)
            if historical > 0:
                block_numbers.append(historical)

        block_numbers.sort()
        logger.info(
            f"Running proof at blocks: {block_numbers}"
        )

        return self.run_proof(wallets, asset_ids, block_numbers)

    def generate_balance_hash(self, up_to_block: int) -> str:
        """Generate a deterministic hash of all balances at a block.

        Used for checkpoint validation — if the hash matches, all balances match.
        """
        balances = self.balance_engine.get_all_balances(up_to_block=up_to_block)
        if balances.empty:
            return hashlib.sha256(b"empty").hexdigest()

        # Sort deterministically and hash
        balances = balances.sort_values(["wallet_address", "asset_id"]).reset_index(drop=True)
        content = balances.to_csv(index=False).encode("utf-8")
        return hashlib.sha256(content).hexdigest()
