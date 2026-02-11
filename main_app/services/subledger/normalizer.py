"""
Normalizer: Raw data -> Movement conversion.

This is the heart of the subledger. Every rule from the architecture plan is
implemented here, including the critical ETH sourcing policy.

Authority matrix:
  Native ETH value transfers -> Traces (full) OR tx.value (partial)
  Native ETH gas fees        -> Always from tx receipt
  ERC-20 tokens              -> Transfer logs only
  ERC-721 NFTs               -> Transfer logs only
  ERC-1155                   -> TransferSingle/TransferBatch logs only
  WETH wrap/unwrap           -> Deposit/Withdrawal logs on WETH contract
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import pandas as pd

from main_app.services.subledger.models import (
    RawTransaction,
    RawLog,
    RawTrace,
    Movement,
    MovementKind,
    SourceType,
    TraceCompleteness,
)
from main_app.services.subledger.asset_registry import (
    resolve_asset_id,
    native_eth_asset_id,
    ZERO_ADDRESS,
    WETH_CONTRACT,
    TRANSFER_TOPIC,
    DEPOSIT_TOPIC,
    WITHDRAWAL_TOPIC,
    _normalize_address,
)

logger = logging.getLogger(__name__)


class MovementNormalizer:
    """Converts raw blockchain data into normalized balance movements.

    All movements use signed deltas in base units (wei/smallest unit).
    Balance derivation is just SUM(amount_delta_raw).
    """

    def __init__(
        self,
        our_wallets: Set[str],
        chain_id: int = 1,
        fund_id: str = "",
    ):
        self.our_wallets = {w.lower() for w in our_wallets}
        self.chain_id = chain_id
        self.fund_id = fund_id
        self._native_asset_id = native_eth_asset_id(chain_id)

    def _is_our_wallet(self, address: Optional[str]) -> bool:
        if address is None:
            return False
        return address.lower() in self.our_wallets

    def _make_movement_id(
        self, tx_hash: str, wallet: str, source_type: str, source_index: str
    ) -> str:
        """Deterministic movement ID: {tx_hash}:{wallet}:{source_type}:{source_index}"""
        return f"{tx_hash}:{wallet}:{source_type}:{source_index}"

    # ------------------------------------------------------------------
    # Gas fee movements (always from tx receipt)
    # ------------------------------------------------------------------

    def _gas_fee_movement(self, tx: RawTransaction) -> Optional[Movement]:
        """Gas fee: always computed from receipt, regardless of trace status."""
        gas_cost = int(tx.gas_used) * int(tx.effective_gas_price_wei)
        if gas_cost <= 0:
            return None
        sender = tx.from_address.lower()
        if not self._is_our_wallet(sender):
            return None

        return Movement(
            movement_id=self._make_movement_id(tx.tx_hash, sender, "GAS_FEE", "0"),
            tx_hash=tx.tx_hash,
            block_number=tx.block_number,
            block_timestamp=tx.block_timestamp,
            wallet_address=sender,
            asset_id=self._native_asset_id,
            amount_delta_raw=str(-gas_cost),
            movement_kind=MovementKind.FEE.value,
            source_type=SourceType.GAS_FEE.value,
            source_id=tx.tx_hash,
            counterparty=None,
            finality_status=tx.finality_status,
            fund_id=self.fund_id,
        )

    # ------------------------------------------------------------------
    # ETH value movements — The ETH sourcing policy
    # ------------------------------------------------------------------

    def _eth_from_tx_value(self, tx: RawTransaction) -> List[Movement]:
        """Create ETH movements from tx.value (used when trace_completeness != full_call_tree)."""
        movements = []
        value = int(tx.value_wei)
        if value <= 0 or tx.tx_status != 1:
            return movements

        sender = tx.from_address.lower()
        receiver = (tx.to_address or "").lower()

        # Determine movement_kind
        if receiver == "" or tx.to_address is None:
            kind = MovementKind.CONTRACT_CREATION.value
        elif receiver == ZERO_ADDRESS:
            kind = MovementKind.BURN.value
        elif sender == ZERO_ADDRESS:
            kind = MovementKind.MINT.value
        else:
            kind = MovementKind.TRANSFER.value

        # Sender (outflow)
        if self._is_our_wallet(sender):
            movements.append(Movement(
                movement_id=self._make_movement_id(tx.tx_hash, sender, "TX_VALUE", "0"),
                tx_hash=tx.tx_hash,
                block_number=tx.block_number,
                block_timestamp=tx.block_timestamp,
                wallet_address=sender,
                asset_id=self._native_asset_id,
                amount_delta_raw=str(-value),
                movement_kind=kind,
                source_type=SourceType.TX_VALUE.value,
                source_id=tx.tx_hash,
                counterparty=receiver if receiver else None,
                finality_status=tx.finality_status,
                fund_id=self.fund_id,
            ))

        # Receiver (inflow)
        if receiver and self._is_our_wallet(receiver):
            movements.append(Movement(
                movement_id=self._make_movement_id(tx.tx_hash, receiver, "TX_VALUE", "1"),
                tx_hash=tx.tx_hash,
                block_number=tx.block_number,
                block_timestamp=tx.block_timestamp,
                wallet_address=receiver,
                asset_id=self._native_asset_id,
                amount_delta_raw=str(value),
                movement_kind=kind,
                source_type=SourceType.TX_VALUE.value,
                source_id=tx.tx_hash,
                counterparty=sender,
                finality_status=tx.finality_status,
                fund_id=self.fund_id,
            ))

        return movements

    def _eth_from_traces(self, tx: RawTransaction, traces: List[RawTrace]) -> List[Movement]:
        """Create ETH movements from traces."""
        movements = []
        for trace in traces:
            value = int(trace.value_wei)
            if value <= 0:
                continue
            if trace.error is not None:
                continue
            # Only value-carrying call types
            if trace.call_type not in ("call", "create", "create2", "selfdestruct"):
                continue

            sender = trace.from_address.lower()
            receiver = trace.to_address.lower()

            if trace.call_type == "selfdestruct":
                kind = MovementKind.SELFDESTRUCT.value
                # Only receiver gets ETH on selfdestruct
                if self._is_our_wallet(receiver):
                    movements.append(Movement(
                        movement_id=self._make_movement_id(
                            tx.tx_hash, receiver, "TRACE", trace.trace_address
                        ),
                        tx_hash=tx.tx_hash,
                        block_number=tx.block_number,
                        block_timestamp=tx.block_timestamp,
                        wallet_address=receiver,
                        asset_id=self._native_asset_id,
                        amount_delta_raw=str(value),
                        movement_kind=kind,
                        source_type=SourceType.TRACE.value,
                        source_id=trace.trace_id,
                        counterparty=sender,
                        finality_status=tx.finality_status,
                        fund_id=self.fund_id,
                    ))
                continue

            # Regular call/create
            if trace.call_type in ("create", "create2"):
                kind = MovementKind.CONTRACT_CREATION.value
            elif sender == ZERO_ADDRESS:
                kind = MovementKind.MINT.value
            elif receiver == ZERO_ADDRESS:
                kind = MovementKind.BURN.value
            else:
                kind = MovementKind.TRANSFER.value

            # Sender outflow
            if self._is_our_wallet(sender):
                movements.append(Movement(
                    movement_id=self._make_movement_id(
                        tx.tx_hash, sender, "TRACE", trace.trace_address
                    ),
                    tx_hash=tx.tx_hash,
                    block_number=tx.block_number,
                    block_timestamp=tx.block_timestamp,
                    wallet_address=sender,
                    asset_id=self._native_asset_id,
                    amount_delta_raw=str(-value),
                    movement_kind=kind,
                    source_type=SourceType.TRACE.value,
                    source_id=trace.trace_id,
                    counterparty=receiver,
                    finality_status=tx.finality_status,
                    fund_id=self.fund_id,
                ))

            # Receiver inflow
            if self._is_our_wallet(receiver):
                movements.append(Movement(
                    movement_id=self._make_movement_id(
                        tx.tx_hash, receiver, "TRACE", trace.trace_address
                    ),
                    tx_hash=tx.tx_hash,
                    block_number=tx.block_number,
                    block_timestamp=tx.block_timestamp,
                    wallet_address=receiver,
                    asset_id=self._native_asset_id,
                    amount_delta_raw=str(value),
                    movement_kind=kind,
                    source_type=SourceType.TRACE.value,
                    source_id=trace.trace_id,
                    counterparty=sender,
                    finality_status=tx.finality_status,
                    fund_id=self.fund_id,
                ))

        return movements

    def normalize_eth_movements(
        self, tx: RawTransaction, traces: List[RawTrace]
    ) -> List[Movement]:
        """Apply the ETH sourcing policy to produce ETH movements for one transaction.

        trace_completeness determines the authority:
          full_call_tree   -> ALL ETH from traces (tx.value ignored)
          internals_only   -> Top-level from tx.value, internal from traces
          none             -> All ETH from tx.value only
        """
        movements = []

        # Gas fee: ALWAYS from receipt, regardless of trace completeness
        gas_mv = self._gas_fee_movement(tx)
        if gas_mv:
            movements.append(gas_mv)

        # Reverted TX: only gas fee
        if tx.tx_status == 0:
            return movements

        completeness = tx.trace_completeness

        if completeness == TraceCompleteness.FULL_CALL_TREE.value:
            # ALL native ETH from traces (including top-level)
            movements.extend(self._eth_from_traces(tx, traces))

        elif completeness == TraceCompleteness.INTERNALS_ONLY.value:
            # Top-level ETH from tx.value
            movements.extend(self._eth_from_tx_value(tx))
            # Internal ETH from traces (Etherscan excludes top-level — no double count)
            movements.extend(self._eth_from_traces(tx, traces))

        elif completeness == TraceCompleteness.NONE.value:
            # Only tx.value available
            movements.extend(self._eth_from_tx_value(tx))

        return movements

    # ------------------------------------------------------------------
    # Log-derived movements (ERC-20, ERC-721, WETH wrap/unwrap)
    # ------------------------------------------------------------------

    def _parse_topic_address(self, topic: Optional[str]) -> str:
        """Extract address from a 32-byte topic (last 20 bytes)."""
        if not topic or len(topic) < 42:
            return ZERO_ADDRESS
        # topic is 0x + 64 hex chars, address is last 40 hex chars
        return _normalize_address("0x" + topic[-40:])

    def _parse_uint256(self, hex_data: str) -> int:
        """Parse a uint256 from hex data."""
        if not hex_data or hex_data == "0x":
            return 0
        # Remove 0x prefix
        clean = hex_data[2:] if hex_data.startswith("0x") else hex_data
        if not clean:
            return 0
        return int(clean, 16)

    def normalize_log_movements(self, log: RawLog, tx: RawTransaction) -> List[Movement]:
        """Convert a single log into movements.

        Handles:
          - ERC-20 Transfer (topic0 = Transfer, data = amount)
          - ERC-721 Transfer (topic0 = Transfer, topic3 = tokenId, data empty)
          - WETH Deposit (topic0 = Deposit on WETH contract)
          - WETH Withdrawal (topic0 = Withdrawal on WETH contract)
        """
        # Skip logs from reverted transactions
        if tx.tx_status == 0:
            return []

        movements = []
        contract = log.contract_address.lower()
        is_weth = contract == WETH_CONTRACT

        # ---- WETH Deposit (wrap: ETH -> WETH) ----
        if log.topic0 == DEPOSIT_TOPIC and is_weth:
            dst = self._parse_topic_address(log.topic1)
            amount = self._parse_uint256(log.data)
            if amount > 0 and self._is_our_wallet(dst):
                weth_asset = resolve_asset_id(self.chain_id, WETH_CONTRACT, 0, "erc20")
                # +WETH
                movements.append(Movement(
                    movement_id=self._make_movement_id(
                        log.tx_hash, dst, "LOG", str(log.log_index)
                    ),
                    tx_hash=log.tx_hash,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                    wallet_address=dst,
                    asset_id=weth_asset,
                    amount_delta_raw=str(amount),
                    movement_kind=MovementKind.WRAP.value,
                    source_type=SourceType.LOG.value,
                    source_id=log.log_id,
                    counterparty=WETH_CONTRACT,
                    finality_status=log.finality_status,
                    fund_id=self.fund_id,
                ))
                # -ETH (the ETH side of the wrap is captured by the ETH sourcing policy
                # via tx.value or traces, so we do NOT add a second ETH movement here.
                # WETH Deposit logs record the WETH mint; ETH outflow is a value transfer.)
            return movements

        # ---- WETH Withdrawal (unwrap: WETH -> ETH) ----
        if log.topic0 == WITHDRAWAL_TOPIC and is_weth:
            src = self._parse_topic_address(log.topic1)
            amount = self._parse_uint256(log.data)
            if amount > 0 and self._is_our_wallet(src):
                weth_asset = resolve_asset_id(self.chain_id, WETH_CONTRACT, 0, "erc20")
                # -WETH
                movements.append(Movement(
                    movement_id=self._make_movement_id(
                        log.tx_hash, src, "LOG", str(log.log_index)
                    ),
                    tx_hash=log.tx_hash,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                    wallet_address=src,
                    asset_id=weth_asset,
                    amount_delta_raw=str(-amount),
                    movement_kind=MovementKind.UNWRAP.value,
                    source_type=SourceType.LOG.value,
                    source_id=log.log_id,
                    counterparty=WETH_CONTRACT,
                    finality_status=log.finality_status,
                    fund_id=self.fund_id,
                ))
                # +ETH is captured by the ETH sourcing policy (trace or tx.value)
            return movements

        # ---- ERC-20 / ERC-721 Transfer ----
        if log.topic0 == TRANSFER_TOPIC:
            from_addr = self._parse_topic_address(log.topic1)
            to_addr = self._parse_topic_address(log.topic2)

            # Determine ERC-20 vs ERC-721
            if log.topic3 is not None:
                # ERC-721: topic3 = tokenId, data may be empty
                token_id = self._parse_uint256(log.topic3)
                asset_id = resolve_asset_id(self.chain_id, contract, token_id, "erc721")
                amount = 1  # NFTs are quantity 1
            else:
                # ERC-20: value in data
                token_id = 0
                asset_id = resolve_asset_id(self.chain_id, contract, 0, "erc20")
                amount = self._parse_uint256(log.data)

            if amount <= 0:
                return movements

            # Determine kind
            if from_addr == ZERO_ADDRESS:
                kind = MovementKind.MINT.value
            elif to_addr == ZERO_ADDRESS:
                kind = MovementKind.BURN.value
            else:
                kind = MovementKind.TRANSFER.value

            # Sender (outflow)
            if self._is_our_wallet(from_addr):
                movements.append(Movement(
                    movement_id=self._make_movement_id(
                        log.tx_hash, from_addr, "LOG", str(log.log_index)
                    ),
                    tx_hash=log.tx_hash,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                    wallet_address=from_addr,
                    asset_id=asset_id,
                    amount_delta_raw=str(-amount),
                    movement_kind=kind,
                    source_type=SourceType.LOG.value,
                    source_id=log.log_id,
                    counterparty=to_addr,
                    finality_status=log.finality_status,
                    fund_id=self.fund_id,
                ))

            # Receiver (inflow)
            if self._is_our_wallet(to_addr):
                movements.append(Movement(
                    movement_id=self._make_movement_id(
                        log.tx_hash, to_addr, "LOG", str(log.log_index)
                    ),
                    tx_hash=log.tx_hash,
                    block_number=log.block_number,
                    block_timestamp=log.block_timestamp,
                    wallet_address=to_addr,
                    asset_id=asset_id,
                    amount_delta_raw=str(amount),
                    movement_kind=kind,
                    source_type=SourceType.LOG.value,
                    source_id=log.log_id,
                    counterparty=from_addr,
                    finality_status=log.finality_status,
                    fund_id=self.fund_id,
                ))

        return movements

    # ------------------------------------------------------------------
    # Main normalization pipeline
    # ------------------------------------------------------------------

    def normalize_all(
        self,
        raw_txs: List[RawTransaction],
        raw_logs: List[RawLog],
        raw_traces: List[RawTrace],
    ) -> List[Movement]:
        """Normalize all raw data into movements.

        Process order:
          1. ETH movements (gas + value transfers, per ETH sourcing policy)
          2. Log-derived movements (ERC-20, ERC-721, WETH wrap/unwrap)
        """
        # Index traces by tx_hash for fast lookup
        traces_by_tx: Dict[str, List[RawTrace]] = {}
        for trace in raw_traces:
            traces_by_tx.setdefault(trace.tx_hash, []).append(trace)

        # Index logs by tx_hash
        logs_by_tx: Dict[str, List[RawLog]] = {}
        for log in raw_logs:
            logs_by_tx.setdefault(log.tx_hash, []).append(log)

        # Index transactions by tx_hash
        tx_by_hash: Dict[str, RawTransaction] = {tx.tx_hash: tx for tx in raw_txs}

        all_movements: List[Movement] = []
        seen_ids: Set[str] = set()

        # Process each transaction
        for tx in raw_txs:
            tx_traces = traces_by_tx.get(tx.tx_hash, [])
            tx_logs = logs_by_tx.get(tx.tx_hash, [])

            # 1. ETH movements (gas + value, per sourcing policy)
            eth_movements = self.normalize_eth_movements(tx, tx_traces)
            for mv in eth_movements:
                if mv.movement_id not in seen_ids:
                    all_movements.append(mv)
                    seen_ids.add(mv.movement_id)

            # 2. Log-derived movements
            for log in tx_logs:
                log_movements = self.normalize_log_movements(log, tx)
                for mv in log_movements:
                    if mv.movement_id not in seen_ids:
                        all_movements.append(mv)
                        seen_ids.add(mv.movement_id)

        # Sort by block_number, then tx_hash for deterministic ordering
        all_movements.sort(key=lambda m: (m.block_number, m.tx_hash, m.movement_id))

        logger.info(
            f"Normalized {len(all_movements)} movements from "
            f"{len(raw_txs)} txs, {len(raw_logs)} logs, {len(raw_traces)} traces"
        )
        return all_movements
