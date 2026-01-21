# -*- coding: utf-8 -*-
"""
Decoder-FIFO Integrator
=======================
Connects the transaction decoder output to FIFO cost basis tracking.

The integration point is `deemed_cash_usd` - the clearing account that
represents cash flows in the decoded journal entries:

- CR deemed_cash_usd = Cash OUT = Asset ACQUISITION → FIFO.add_acquisition()
- DR deemed_cash_usd = Cash IN = Asset DISPOSAL → FIFO.process_disposal()

This module:
1. Receives decoded transactions from DecoderRegistry
2. Identifies deemed_cash entries to determine cash flow direction
3. Updates FIFO tracker with acquisitions/disposals
4. Enriches journal entries with cost basis and gain/loss

Usage:
    from main_app.services.decoder_fifo_integrator import DecoderFIFOIntegrator
    from main_app.services.fifo_tracker import CostBasisTracker, CostBasisMethod

    # Initialize
    tracker = CostBasisTracker(method=CostBasisMethod.FIFO, fund_id="drip_capital")
    integrator = DecoderFIFOIntegrator(tracker)

    # Process decoded transaction
    result = integrator.process_decoded_transaction(decoded_tx)

    # Get enriched journal entries with gain/loss
    enriched_entries = result['journal_entries']
"""

from __future__ import annotations

import logging
from decimal import Decimal
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

from .fifo_tracker import (
    CostBasisTracker,
    SwapProcessor,
    CostBasisMethod,
    TaxLot,
    DisposalEvent,
)

logger = logging.getLogger(__name__)


# Assets that are ETH-equivalent (use ETH price for USD conversion)
ETH_EQUIVALENT_ASSETS = {
    'ETH', 'WETH', 'aWETH', 'stETH', 'wstETH', 'cbETH', 'rETH',
    'BLUR_POOL',  # Blur's ETH pool token
}

# Known clearing accounts
DEEMED_CASH_ACCOUNTS = {
    'deemed_cash_usd',
    'deemed_cash_eth',
    'deemed_cash_weth',
    'deemed_cash_usdc',
}


@dataclass
class ProcessedTransaction:
    """Result of processing a decoded transaction through FIFO"""
    tx_hash: str
    timestamp: datetime
    acquisitions: List[TaxLot]
    disposals: List[DisposalEvent]
    journal_entries: List[Dict]
    total_gain_loss_usd: Decimal
    errors: List[str]


class DecoderFIFOIntegrator:
    """
    Integrates decoded transaction journal entries with FIFO cost basis tracking.

    Identifies cash flows via deemed_cash accounts and updates the FIFO tracker:
    - CR deemed_cash = Cash out = Acquisition
    - DR deemed_cash = Cash in = Disposal
    """

    def __init__(self,
                 tracker: CostBasisTracker,
                 fund_id: str = "",
                 default_eth_price: Decimal = Decimal("3000")):
        """
        Initialize the integrator.

        Args:
            tracker: CostBasisTracker instance for cost basis management
            fund_id: Default fund identifier
            default_eth_price: Fallback ETH price if not available
        """
        self.tracker = tracker
        self.fund_id = fund_id or tracker.fund_id
        self.default_eth_price = default_eth_price
        self.swap_processor = SwapProcessor(tracker)

    def process_decoded_transaction(self,
                                    decoded_tx: Any,
                                    eth_price_usd: Optional[Decimal] = None) -> ProcessedTransaction:
        """
        Process a decoded transaction and update FIFO tracker.

        Args:
            decoded_tx: DecodedTransaction from registry
            eth_price_usd: ETH/USD price (uses decoded_tx.eth_price if not provided)

        Returns:
            ProcessedTransaction with acquisitions, disposals, and enriched entries
        """
        tx_hash = decoded_tx.tx_hash
        timestamp = decoded_tx.timestamp

        # Get ETH price
        if eth_price_usd is None:
            eth_price_usd = decoded_tx.eth_price or self.default_eth_price
        eth_price_usd = Decimal(str(eth_price_usd))

        acquisitions = []
        disposals = []
        errors = []
        enriched_entries = []

        # Process each journal entry
        for je in decoded_tx.journal_entries:
            try:
                result = self._process_journal_entry(
                    je,
                    tx_hash=tx_hash,
                    timestamp=timestamp,
                    eth_price_usd=eth_price_usd
                )

                if result.get('acquisition'):
                    acquisitions.append(result['acquisition'])
                if result.get('disposal'):
                    disposals.append(result['disposal'])

                # Create enriched entry
                enriched = self._enrich_journal_entry(je, result)
                enriched_entries.append(enriched)

            except Exception as e:
                logger.error(f"Error processing journal entry in {tx_hash}: {e}")
                errors.append(str(e))
                enriched_entries.append(self._entry_to_dict(je))  # Pass through unchanged

        # Calculate total gain/loss
        total_gain_loss = sum(d.gain_loss_usd for d in disposals)

        return ProcessedTransaction(
            tx_hash=tx_hash,
            timestamp=timestamp,
            acquisitions=acquisitions,
            disposals=disposals,
            journal_entries=enriched_entries,
            total_gain_loss_usd=total_gain_loss,
            errors=errors
        )

    def _process_journal_entry(self,
                               je: Any,
                               tx_hash: str,
                               timestamp: datetime,
                               eth_price_usd: Decimal) -> Dict:
        """
        Process a single journal entry and update FIFO tracker if it involves deemed_cash.

        Returns dict with 'acquisition' and/or 'disposal' if FIFO was updated.
        """
        result = {}

        # Get wallet and fund info
        wallet_id = je.wallet_address or ""
        fund_id = je.fund_id or self.fund_id

        # Check each sub-entry for deemed_cash flows
        for entry in je.entries:
            account = entry.get('account', '').lower()
            entry_type = entry.get('type', '')
            amount = Decimal(str(entry.get('amount', 0)))
            asset = entry.get('asset', 'ETH')

            if amount <= 0:
                continue

            # Check if this is a deemed_cash entry
            if not self._is_deemed_cash_account(account):
                continue

            # Calculate USD value
            if asset in ETH_EQUIVALENT_ASSETS:
                value_usd = amount * eth_price_usd
            else:
                # For stablecoins, 1:1 USD
                value_usd = amount

            # Determine the asset being acquired/disposed
            # Look at the other entries to find the asset account
            counterpart_asset = self._find_counterpart_asset(je.entries, entry)
            if counterpart_asset:
                asset = counterpart_asset

            if entry_type == 'CREDIT':
                # CR deemed_cash = Cash OUT = ACQUISITION
                lot = self.tracker.add_acquisition(
                    asset=asset,
                    amount=amount,
                    cost_usd=value_usd,
                    date=timestamp,
                    tx_hash=tx_hash,
                    wallet_id=wallet_id,
                    fund_id=fund_id
                )
                result['acquisition'] = lot
                logger.debug(f"FIFO acquisition: {amount} {asset} @ ${value_usd}")

            elif entry_type == 'DEBIT':
                # DR deemed_cash = Cash IN = DISPOSAL
                disposal = self.tracker.process_disposal(
                    asset=asset,
                    amount=amount,
                    proceeds_usd=value_usd,
                    date=timestamp,
                    tx_hash=tx_hash,
                    wallet_id=wallet_id,
                    fund_id=fund_id
                )
                result['disposal'] = disposal
                logger.debug(f"FIFO disposal: {amount} {asset}, gain/loss: ${disposal.gain_loss_usd}")

        return result

    def _is_deemed_cash_account(self, account: str) -> bool:
        """Check if account is a deemed cash clearing account"""
        account_lower = account.lower()
        return any(dc in account_lower for dc in ['deemed_cash', 'deemed cash'])

    def _find_counterpart_asset(self, entries: List[Dict], cash_entry: Dict) -> Optional[str]:
        """
        Find the asset being traded based on other entries in the journal entry.

        If deemed_cash is credited, find the debited asset account (acquisition).
        If deemed_cash is debited, find the credited asset account (disposal).
        """
        cash_type = cash_entry.get('type', '')

        for entry in entries:
            if entry == cash_entry:
                continue

            account = entry.get('account', '').lower()
            entry_type = entry.get('type', '')
            asset = entry.get('asset', '')

            # Skip other deemed_cash entries
            if self._is_deemed_cash_account(account):
                continue

            # Skip gain/loss accounts
            if 'gain' in account or 'loss' in account:
                continue

            # For acquisition (CR deemed_cash), find the DR asset
            if cash_type == 'CREDIT' and entry_type == 'DEBIT':
                return asset or self._extract_asset_from_account(account)

            # For disposal (DR deemed_cash), find the CR asset
            if cash_type == 'DEBIT' and entry_type == 'CREDIT':
                return asset or self._extract_asset_from_account(account)

        return None

    def _extract_asset_from_account(self, account: str) -> Optional[str]:
        """Extract asset name from account name like 'digital_assets_weth'"""
        prefixes = ['digital_assets_', 'investments_', 'crypto_assets_']
        account_lower = account.lower()

        for prefix in prefixes:
            if account_lower.startswith(prefix):
                return account_lower[len(prefix):].upper()

        return None

    def _enrich_journal_entry(self, je: Any, fifo_result: Dict) -> Dict:
        """
        Enrich journal entry with FIFO cost basis and gain/loss information.
        """
        entry_dict = self._entry_to_dict(je)

        # Add FIFO tracking info
        if fifo_result.get('acquisition'):
            lot = fifo_result['acquisition']
            entry_dict['fifo_lot_id'] = lot.lot_id
            entry_dict['fifo_cost_basis_usd'] = float(lot.cost_basis_usd)
            entry_dict['fifo_cost_per_unit'] = float(lot.cost_per_unit)
            entry_dict['fifo_action'] = 'ACQUISITION'

        if fifo_result.get('disposal'):
            disposal = fifo_result['disposal']
            entry_dict['fifo_disposal_id'] = disposal.disposal_id
            entry_dict['fifo_cost_basis_usd'] = float(disposal.cost_basis_usd)
            entry_dict['fifo_proceeds_usd'] = float(disposal.proceeds_usd)
            entry_dict['fifo_gain_loss_usd'] = float(disposal.gain_loss_usd)
            entry_dict['fifo_holding_days'] = disposal.holding_days
            entry_dict['fifo_is_long_term'] = disposal.is_long_term
            entry_dict['fifo_tax_treatment'] = disposal.tax_treatment
            entry_dict['fifo_action'] = 'DISPOSAL'
            entry_dict['fifo_lots_used'] = disposal.lots_used

        return entry_dict

    def _entry_to_dict(self, je: Any) -> Dict:
        """Convert JournalEntry to dict"""
        if hasattr(je, 'to_dict'):
            return je.to_dict()

        # Manual conversion for dataclass
        return {
            'entry_id': getattr(je, 'entry_id', ''),
            'date': getattr(je, 'date', datetime.now()).isoformat(),
            'description': getattr(je, 'description', ''),
            'tx_hash': getattr(je, 'tx_hash', ''),
            'category': getattr(je, 'category', None),
            'platform': getattr(je, 'platform', None),
            'wallet_address': getattr(je, 'wallet_address', ''),
            'fund_id': getattr(je, 'fund_id', ''),
            'entries': getattr(je, 'entries', []),
        }

    def process_swap(self,
                    from_asset: str,
                    from_amount: Decimal,
                    to_asset: str,
                    to_amount: Decimal,
                    eth_price_usd: Decimal,
                    timestamp: datetime,
                    tx_hash: str,
                    wallet_id: str,
                    fund_id: Optional[str] = None) -> Dict:
        """
        Process an asset swap (e.g., aWETH -> WETH for Aave withdraw).

        Convenience method that wraps SwapProcessor.
        """
        return self.swap_processor.process_swap(
            from_asset=from_asset,
            from_amount=from_amount,
            to_asset=to_asset,
            to_amount=to_amount,
            eth_price_usd=eth_price_usd,
            date=timestamp,
            tx_hash=tx_hash,
            wallet_id=wallet_id,
            fund_id=fund_id or self.fund_id
        )

    def get_position_summary(self) -> Dict:
        """Get current FIFO position summary"""
        df = self.tracker.get_all_positions()
        if df.empty:
            return {'positions': [], 'total_cost_basis_usd': 0}

        return {
            'positions': df.to_dict('records'),
            'total_cost_basis_usd': float(df['cost_basis_usd'].sum()) if 'cost_basis_usd' in df.columns else 0
        }


def create_integrator(fund_id: str = "drip_capital",
                     method: CostBasisMethod = CostBasisMethod.FIFO) -> DecoderFIFOIntegrator:
    """
    Factory function to create a DecoderFIFOIntegrator with a new tracker.

    Args:
        fund_id: Fund identifier
        method: Cost basis method (FIFO, LIFO, HIFO)

    Returns:
        Configured DecoderFIFOIntegrator
    """
    tracker = CostBasisTracker(method=method, fund_id=fund_id)
    return DecoderFIFOIntegrator(tracker=tracker, fund_id=fund_id)
