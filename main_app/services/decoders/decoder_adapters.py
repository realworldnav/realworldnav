"""
Decoder Adapters - Wrap notebook decoders with BaseDecoder interface

The notebook decoders (Gondi, Blur, Arcade, NFTfi, Zharta) have different
constructor signatures and output formats than the BaseDecoder interface
expected by the registry. These adapters bridge that gap.

Each adapter:
1. Accepts (w3, fund_wallets) like BaseDecoder
2. Internally creates the notebook decoder with proper contracts/metadata
3. Implements can_decode() and decode() methods
4. Converts notebook output to DecodedTransaction
"""

import logging
from typing import Dict, List, Optional, Any, Set
from decimal import Decimal
from datetime import datetime, timezone
from web3 import Web3

from .base import (
    BaseDecoder,
    BaseDecoderAdapter,
    DecodedTransaction,
    DecodedEvent,
    JournalEntry,
    Platform,
    TransactionCategory,
    PostingStatus,
    wei_to_eth,
    calculate_gas_fee,
)
from .abis import load_abi

logger = logging.getLogger(__name__)


# Contract addresses for routing
BLUR_BLEND_PROXY = "0x29469395eAf6f95920E59F858042f0e28D98a20B".lower()
BLUR_POOL = "0x0000000000A39bb272e79075ade125fd351887Ac".lower()

GONDI_CONTRACTS = {
    "0xf41b389e0c1950dc0b16c9498eae77131cc08a56": "v1",
    "0x478f6f994c6fb3cf3e444a489b3ad9edb8ccae16": "v2",
    "0xf65b99ce6dc5f6c556172bcc0ff27d3665a7d9a8": "v3",
    "0x59e0b87e3dcfb5d34c06c71c3fbf7f6b7d77a4ff": "multi_source",
}

ARCADE_CONTRACTS = {
    "0x81b2f8fc75bab64a6b144aa6d2faa127b4fa7fd9": "LoanCore",
    "0x89bc08ba00f135d608bc335f6b33d7a9abcc98af": "OriginationController_v1",
    "0xb7b1bc9b44eb0d3e61b52550c85c29d7a43db96c": "OriginationController_v2",
    "0xb7bfcca7d7ff0f371867b770856fac184b185878": "OriginationController_v3",
    "0x74241e1a9c021643289476426b9b70229ab40d53": "RepaymentController_legacy",
    "0xb39dab85fa05c381767ff992ccde4c94619993d4": "RepaymentController",
}

NFTFI_CONTRACTS = {
    "0xf896527c49b44aab3cf22ae356fa3af8e331f280": "v2",
    "0x8252df1d8b29057d1afe3062bf5a64d503152bc8": "v2.1",
    "0xd0a40eb7fcd530a13866b9e893e4a9e0d15d03eb": "v2.3",
    "0xd0c6e59b50c32530c627107f50acc71958c4341f": "v2.3_collection",
    "0x9f10d706d789e4c76a1a6434cd1a9841c875c0a6": "v3_asset",
    "0xb6adec2acc851d30d5fb64f3137234bcdcbbad0d": "v3_collection",
    # NFTfi refinancing/aggregator contracts
    "0x4bc5fa56f2931e7a37417fa55dda71e4b7c2f2a3": "refinancing",
    "0x1e0447b19bb6ecfdae1e4ae1694b0c3659614e4e": "DirectLoanFixedCollectionOffer",
}

ZHARTA_CONTRACTS = {
    "0x1cf3dab407aa14389f9c79b80b16e48cbc7246ee": "Loans_WETH_Pool",
    "0x5f19431bc8a3eb21222771c6c867a63a119deda7": "Loans_USDC_V2",
    "0x8d0f9c9fa4c1b265cd5032fe6ba4fefc9d94badb": "P2PLendingNfts",
    "0x5be916cff5f07870e9aef205960e07d9e287ef27": "LoansCore_WETH",
    "0x04fc02deeee6f4fa51e11cc762e2e47ab8873ecc": "Liquidations",
    "0x7ca34cf45a119bebef4d106318402964a331dfed": "CollateralVault",
}


class BlurDecoderAdapter(BaseDecoderAdapter):
    """Adapter for Blur notebook decoder (v1.0.0)"""

    PLATFORM = Platform.BLUR
    CONTRACT_ADDRESSES = [BLUR_BLEND_PROXY, BLUR_POOL]

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        super().__init__(w3, fund_wallets)

    def _initialize_decoder(self) -> bool:
        """Initialize Blur decoder with contracts."""
        try:
            from .blur_decoder import BlurEventDecoder, BlurJournalEntryGenerator

            # Load Blur Blend contract (proxy with implementation ABI)
            blend_abi = load_abi(BLUR_BLEND_PROXY, "blur")
            if not blend_abi:
                # Try loading implementation ABI
                impl_address = "0xB258CA5559b11cD702F363796522b04D7722Ea56"
                blend_abi = load_abi(impl_address, "blur_impl")

            pool_abi = load_abi(BLUR_POOL, "blur_pool")

            if not blend_abi or not pool_abi:
                self._initialization_error = "Failed to load Blur ABIs"
                return False

            blend_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(BLUR_BLEND_PROXY),
                abi=blend_abi
            )
            pool_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(BLUR_POOL),
                abi=pool_abi
            )

            self._notebook_decoder = BlurEventDecoder(
                w3=self.w3,
                blend_contract=blend_contract,
                pool_contract=pool_contract,
                wallet_metadata=self.wallet_metadata,
                debug=False
            )
            self._journal_generator = BlurJournalEntryGenerator(
                wallet_metadata=self.wallet_metadata
            )
            logger.info("Blur adapter initialized successfully")
            return True

        except ImportError as e:
            self._initialization_error = f"Import error: {e}"
            return False
        except Exception as e:
            self._initialization_error = str(e)
            return False

    def can_decode(self, tx: Dict, receipt: Dict) -> bool:
        """Check if transaction involves Blur contracts"""
        to_addr = (tx.get('to') or '').lower()
        if to_addr in [BLUR_BLEND_PROXY, BLUR_POOL]:
            return True

        # Check logs for Blur contract events
        for log in receipt.get('logs', []):
            log_addr = log.get('address', '').lower()
            if log_addr in [BLUR_BLEND_PROXY, BLUR_POOL]:
                return True

        return False

    def decode(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal) -> DecodedTransaction:
        """Decode Blur transaction using notebook decoder"""
        # Use lazy initialization via base class
        if not self._ensure_initialized():
            return self._create_basic_result(tx, receipt, block, eth_price,
                                            f"Blur decoder not initialized: {self._initialization_error}")

        tx_hash = self._normalize_tx_hash(tx)
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        try:
            # Decode using notebook decoder
            events = self._notebook_decoder.decode_transaction(tx_hash)
            logger.info(f"Blur decoded {len(events) if events else 0} events for tx {tx_hash[:16]}...")

            if events:
                for e in events:
                    logger.info(f"  Event: {e.event_type}, lien_id={getattr(e, 'lien_id', 'N/A')}")
            else:
                logger.warning(f"  No Blur events decoded for {tx_hash[:16]}...")

            # Convert to DecodedTransaction
            category = self._determine_category(events)
            journal_entries = []

            if events and self._journal_generator:
                # Generate journal entries using individual methods
                import pandas as pd
                events_df = pd.DataFrame([e.to_dict() for e in events])
                logger.info(f"  Blur events_df has {len(events_df)} rows, columns: {list(events_df.columns)[:5]}...")
                all_entries = []

                # Originations
                try:
                    df_orig = self._journal_generator.generate_loan_origination_entries(events_df)
                    if df_orig is not None and not df_orig.empty:
                        all_entries.append(df_orig)
                        logger.info(f"    Generated {len(df_orig)} origination entries")
                except Exception as e:
                    logger.warning(f"Origination entries error: {e}")

                # Repayments (returns tuple: entries, accruals)
                try:
                    df_repay, df_accruals = self._journal_generator.generate_loan_repayment_entries(events_df)
                    if df_repay is not None and not df_repay.empty:
                        all_entries.append(df_repay)
                        logger.info(f"    Generated {len(df_repay)} repayment entries")
                    if df_accruals is not None and not df_accruals.empty:
                        all_entries.append(df_accruals)
                        logger.info(f"    Generated {len(df_accruals)} accrual entries")
                except Exception as e:
                    logger.warning(f"Repayment entries error: {e}")

                # Refinances (returns tuple)
                try:
                    df_refi, df_refi_acc = self._journal_generator.generate_refinance_entries(events_df)
                    if df_refi is not None and not df_refi.empty:
                        all_entries.append(df_refi)
                        logger.info(f"    Generated {len(df_refi)} refinance entries")
                    if df_refi_acc is not None and not df_refi_acc.empty:
                        all_entries.append(df_refi_acc)
                        logger.info(f"    Generated {len(df_refi_acc)} refinance accrual entries")
                except Exception as e:
                    logger.warning(f"Refinance entries error: {e}")

                # Seizes (returns tuple)
                try:
                    df_seize, df_seize_acc = self._journal_generator.generate_seize_entries(events_df)
                    if df_seize is not None and not df_seize.empty:
                        all_entries.append(df_seize)
                        logger.info(f"    Generated {len(df_seize)} seize entries")
                    if df_seize_acc is not None and not df_seize_acc.empty:
                        all_entries.append(df_seize_acc)
                        logger.info(f"    Generated {len(df_seize_acc)} seize accrual entries")
                except Exception as e:
                    logger.warning(f"Seize entries error: {e}")

                if all_entries:
                    combined_df = pd.concat(all_entries, ignore_index=True)
                    journal_entries = self._convert_journal_entries(combined_df)

            return DecodedTransaction(
                status="success",
                tx_hash=tx_hash,
                platform=Platform.BLUR,
                category=category,
                block=tx.get('blockNumber', 0),
                timestamp=timestamp,
                eth_price=eth_price,
                gas_used=receipt.get('gasUsed', 0),
                gas_fee=gas_fee,
                from_address=tx.get('from', ''),
                to_address=tx.get('to', '') or '',
                value=wei_to_eth(tx.get('value', 0)),
                function_name=self._get_function_name(events),
                journal_entries=journal_entries,
                events=[DecodedEvent(
                    name=e.event_type,
                    args=e.to_dict(),
                    log_index=e.log_index,
                    contract_address=BLUR_BLEND_PROXY
                ) for e in events] if events else [],
                wallet_roles={},
                positions={},
            )

        except Exception as e:
            logger.error(f"Error decoding Blur tx {tx_hash}: {e}")
            return self._create_basic_result(tx, receipt, block, eth_price, str(e))

    def _determine_category(self, events) -> TransactionCategory:
        """Determine transaction category from Blur events"""
        if not events:
            return TransactionCategory.CONTRACT_CALL

        event_types = {e.event_type for e in events}

        if 'LoanOfferTaken' in event_types:
            return TransactionCategory.LOAN_ORIGINATION
        elif 'Repay' in event_types:
            return TransactionCategory.LOAN_REPAYMENT
        elif 'Refinance' in event_types:
            return TransactionCategory.LOAN_REFINANCE
        elif 'StartAuction' in event_types:
            return TransactionCategory.LOAN_AUCTION
        elif 'Seize' in event_types:
            return TransactionCategory.COLLATERAL_SEIZURE

        return TransactionCategory.CONTRACT_CALL

    def _get_function_name(self, events) -> str:
        """Get function name from events"""
        if not events:
            return "unknown"
        return events[0].event_type if events else "unknown"

    def _convert_journal_entries(self, entries_df) -> List[JournalEntry]:
        """Convert DataFrame to JournalEntry objects"""
        import pandas as pd

        journal_entries = []
        if entries_df is None or entries_df.empty:
            return journal_entries

        # Group by transaction (loan_id + event) to create proper journal entries
        group_cols = []
        if 'loan_id' in entries_df.columns:
            group_cols.append('loan_id')
        if 'lien_id' in entries_df.columns and 'loan_id' not in entries_df.columns:
            group_cols.append('lien_id')
        if 'event' in entries_df.columns:
            group_cols.append('event')
        if 'hash' in entries_df.columns:
            group_cols.append('hash')
        elif 'transactionHash' in entries_df.columns:
            group_cols.append('transactionHash')

        if not group_cols:
            group_cols = [entries_df.index]

        try:
            grouped = entries_df.groupby(group_cols, dropna=False)
        except Exception:
            # Fallback: treat entire df as one group
            grouped = [(None, entries_df)]

        for group_key, group in grouped:
            if group.empty:
                continue

            first_row = group.iloc[0]
            tx_hash = first_row.get('hash', first_row.get('transactionHash', 'unknown'))
            event_type = first_row.get('event', 'unknown')
            loan_id = first_row.get('loan_id', first_row.get('lien_id', 'unknown'))

            # Determine category
            if event_type in ['LoanOfferTaken', 'borrow']:
                category = TransactionCategory.LOAN_ORIGINATION
            elif event_type in ['Repay', 'LoanRepaid']:
                category = TransactionCategory.LOAN_REPAYMENT
            elif event_type in ['Refinance', 'BorrowerRefinance']:
                category = TransactionCategory.LOAN_REFINANCE
            elif event_type == 'Seize':
                category = TransactionCategory.COLLATERAL_SEIZURE
            else:
                category = TransactionCategory.CONTRACT_CALL

            # Create journal entry
            entry = JournalEntry(
                entry_id=f"{event_type}_{loan_id}_{tx_hash[:8] if tx_hash else 'unknown'}",
                date=pd.to_datetime(first_row.get('date', datetime.now(timezone.utc))),
                description=f"Blur {event_type} - Lien #{loan_id}",
                tx_hash=str(tx_hash) if tx_hash else 'unknown',
                category=category,
                platform=Platform.BLUR,
                wallet_address=str(first_row.get('wallet_id', first_row.get('lender', ''))),
                wallet_role="lender" if first_row.get('is_lender_fund', False) else "borrower",
                fund_id=str(first_row.get('fund_id', '')),
                eth_usd_price=Decimal(str(first_row.get('eth_usd_price') or 0)),
            )

            # Add debit/credit entries
            for _, row in group.iterrows():
                account = row.get('account_name', 'unknown')
                debit_val = row.get('debit', row.get('debit_crypto', 0))
                credit_val = row.get('credit', row.get('credit_crypto', 0))
                debit = Decimal(str(debit_val if debit_val is not None and not pd.isna(debit_val) else 0))
                credit = Decimal(str(credit_val if credit_val is not None and not pd.isna(credit_val) else 0))
                currency = row.get('cryptocurrency', row.get('currency', 'BLUR_POOL'))

                if debit > 0:
                    entry.add_debit(account, debit, currency)
                if credit > 0:
                    entry.add_credit(account, credit, currency)

            if entry.entries:  # Only add if there are actual debit/credit entries
                journal_entries.append(entry)

        return journal_entries


class GondiDecoderAdapter(BaseDecoderAdapter):
    """Adapter for Gondi notebook decoder (v1.7.1)"""

    PLATFORM = Platform.GONDI
    CONTRACT_ADDRESSES = list(GONDI_CONTRACTS.keys())

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        super().__init__(w3, fund_wallets)

    def _initialize_decoder(self) -> bool:
        """Initialize Gondi decoder with contracts."""
        try:
            from .gondi_decoder import GondiEventDecoder, GondiJournalEntryGenerator

            # Load Gondi contracts
            contracts = {}
            failed_abis = []
            for addr, version in GONDI_CONTRACTS.items():
                abi = load_abi(addr, f"gondi_{version}")
                if abi:
                    contracts[addr] = self.w3.eth.contract(
                        address=Web3.to_checksum_address(addr),
                        abi=abi
                    )
                    logger.info(f"  Loaded Gondi {version} ABI for {addr[:16]}... ({len(abi)} entries)")
                    # DEBUG: List event names in this ABI
                    event_names = [item.get('name') for item in abi if item.get('type') == 'event']
                    print(f"[DEBUG] Gondi {version} events: {event_names}")
                else:
                    failed_abis.append((addr, version))
                    logger.warning(f"  FAILED to load Gondi {version} ABI for {addr[:16]}...")

            if not contracts:
                self._initialization_error = f"Could not load any Gondi ABIs. Failed: {failed_abis}"
                logger.error(self._initialization_error)
                return False

            logger.info(f"Gondi adapter loaded {len(contracts)}/{len(GONDI_CONTRACTS)} contracts")
            if failed_abis:
                logger.warning(f"  Failed ABIs: {failed_abis}")

            logger.info(f"Gondi adapter wallet_metadata has {len(self.wallet_metadata)} wallets")
            # Log first few wallets for debugging
            for i, addr in enumerate(list(self.wallet_metadata.keys())[:3]):
                logger.info(f"  Fund wallet [{i+1}]: {addr}")

            self._notebook_decoder = GondiEventDecoder(
                w3=self.w3,
                contracts=contracts,
                wallet_metadata=self.wallet_metadata
            )
            logger.info(f"GondiEventDecoder fund_wallet_list has {len(self._notebook_decoder.fund_wallet_list)} wallets")
            logger.info(f"GondiEventDecoder contracts keys: {list(self._notebook_decoder.contracts.keys())}")

            self._journal_generator = GondiJournalEntryGenerator(
                wallet_metadata=self.wallet_metadata
            )
            logger.info(f"Gondi adapter initialized successfully with {len(contracts)} contracts")
            return True

        except ImportError as e:
            self._initialization_error = f"Import error: {e}"
            return False
        except Exception as e:
            self._initialization_error = str(e)
            return False

    def can_decode(self, tx: Dict, receipt: Dict) -> bool:
        """Check if transaction involves Gondi contracts"""
        to_addr = (tx.get('to') or '').lower()
        if to_addr in GONDI_CONTRACTS:
            return True

        for log in receipt.get('logs', []):
            log_addr = log.get('address', '').lower()
            if log_addr in GONDI_CONTRACTS:
                return True

        return False

    def decode(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal) -> DecodedTransaction:
        """Decode Gondi transaction using notebook decoder"""
        if not self._ensure_initialized():
            tx_hash = self._normalize_tx_hash(tx)
            logger.warning(f"Gondi decoder not initialized for tx {tx_hash[:16]}")
            return self._create_basic_result(tx, receipt, block, eth_price,
                                            f"Gondi decoder not initialized: {self._initialization_error}")

        tx_hash = self._normalize_tx_hash(tx)
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        try:
            events = self._notebook_decoder.decode_transaction(tx_hash)
            logger.info(f"Gondi decoded {len(events) if events else 0} events for tx {tx_hash[:16]}...")

            if events:
                for e in events:
                    logger.info(f"  Event: {e.event_type}, fund_tranches={len(e.fund_tranches)}, old_fund_tranches={len(e.old_fund_tranches)}, is_fund_borrower={e.is_fund_borrower}")
                    # Log loan participants for debugging
                    if e.loan:
                        logger.debug(f"    Loan borrower: {e.loan.borrower[:16]}...")
                        for t in e.loan.tranches[:3]:  # First 3 tranches
                            logger.debug(f"    Tranche lender: {t.lender[:16]}...")
            else:
                # Diagnose why no events - check if contracts loaded
                logger.warning(f"  No events decoded for {tx_hash[:16]}...")
                logger.warning(f"  Decoder has {len(self._notebook_decoder.contracts)} contracts")
                logger.warning(f"  Decoder has {len(self._notebook_decoder.fund_wallet_list)} fund wallets")

            category = self._determine_category(events)
            journal_entries = []

            if events and self._journal_generator:
                # Pass events list directly - generator expects DecodedGondiEvent objects
                result = self._journal_generator.process_events(events)
                # Combine all journal entry DataFrames from result
                import pandas as pd
                entry_keys = ['new_loans', 'repayments', 'refinances', 'foreclosures',
                              'liquidations', 'interest_accruals', 'accrual_reversals',
                              'sent_to_liquidator']
                all_entries = []
                for key in entry_keys:
                    if key in result and isinstance(result[key], pd.DataFrame) and not result[key].empty:
                        all_entries.append(result[key])
                if all_entries:
                    combined_df = pd.concat(all_entries, ignore_index=True)
                    journal_entries = self._convert_journal_entries(combined_df)

            return DecodedTransaction(
                status="success",
                tx_hash=tx_hash,
                platform=Platform.GONDI,
                category=category,
                block=tx.get('blockNumber', 0),
                timestamp=timestamp,
                eth_price=eth_price,
                gas_used=receipt.get('gasUsed', 0),
                gas_fee=gas_fee,
                from_address=tx.get('from', ''),
                to_address=tx.get('to', '') or '',
                value=wei_to_eth(tx.get('value', 0)),
                function_name=self._get_function_name(events),
                journal_entries=journal_entries,
                events=[DecodedEvent(
                    name=e.event_type,
                    args=e.to_dict(),
                    log_index=e.log_index,
                    contract_address=e.contract_address
                ) for e in events] if events else [],
                wallet_roles={},
                positions={},
            )

        except Exception as e:
            logger.error(f"Error decoding Gondi tx {tx_hash}: {e}")
            return self._create_basic_result(tx, receipt, block, eth_price, str(e))

    def _determine_category(self, events) -> TransactionCategory:
        if not events:
            return TransactionCategory.CONTRACT_CALL

        event_types = {e.event_type for e in events}

        if 'LoanEmitted' in event_types:
            return TransactionCategory.LOAN_ORIGINATION
        elif 'LoanRepaid' in event_types:
            return TransactionCategory.LOAN_REPAYMENT
        elif 'LoanRefinanced' in event_types or 'LoanRefinancedFromNewOffers' in event_types:
            return TransactionCategory.LOAN_REFINANCE
        elif 'LoanForeclosed' in event_types or 'LoanLiquidated' in event_types:
            return TransactionCategory.COLLATERAL_SEIZURE

        return TransactionCategory.CONTRACT_CALL

    def _get_function_name(self, events) -> str:
        if not events:
            return "unknown"
        return events[0].event_type if events else "unknown"

    def _convert_journal_entries(self, entries_df) -> List[JournalEntry]:
        """Convert DataFrame rows to JournalEntry objects, grouping by transaction"""
        from collections import defaultdict
        import uuid

        if entries_df is None or entries_df.empty:
            return []

        # Group entries by (tx_hash, event) to create one JournalEntry per event
        grouped = defaultdict(list)
        for _, row in entries_df.iterrows():
            tx_hash = row.get('hash', '')
            event = row.get('event', 'unknown')
            key = (tx_hash, event, row.get('loan_id', ''))
            grouped[key].append(row)

        journal_entries = []
        for (tx_hash, event, loan_id), rows in grouped.items():
            first_row = rows[0]
            currency = first_row.get('cryptocurrency', 'ETH')

            # Determine category and posting status
            category = (TransactionCategory.LOAN_ORIGINATION if 'Emitted' in event else
                        TransactionCategory.LOAN_REPAYMENT if 'Repaid' in event else
                        TransactionCategory.LOAN_REFINANCE if 'Refinanced' in event else
                        TransactionCategory.COLLATERAL_SEIZURE if 'Liquidated' in event else
                        TransactionCategory.CONTRACT_CALL)

            # Auto-post for known loan events, review queue for others
            # Use substring matching since events can have suffixes like _payoff, _origination
            auto_post_patterns = ['LoanEmitted', 'LoanRepaid', 'LoanRefinanced', 'LoanRefinancedFromNewOffers']
            posting_status = PostingStatus.AUTO_POST if any(p in event for p in auto_post_patterns) else PostingStatus.REVIEW_QUEUE

            entry = JournalEntry(
                entry_id=f"gondi_{event}_{loan_id}_{uuid.uuid4().hex[:8]}",
                date=first_row.get('date', datetime.now(timezone.utc)),
                description=f"Gondi {event} - Loan #{loan_id}" if loan_id else f"Gondi {event}",
                tx_hash=tx_hash,
                category=category,
                platform=Platform.GONDI,
                wallet_address=first_row.get('wallet_id', first_row.get('lender', '')),
                wallet_role=first_row.get('fund_role', 'lender'),
                eth_usd_price=Decimal(str(first_row.get('eth_price', 0))),
                posting_status=posting_status
            )

            # Add debit/credit entries from each row
            for row in rows:
                account = row.get('account_name', 'unknown')
                debit = Decimal(str(row.get('debit', 0)))
                credit = Decimal(str(row.get('credit', 0)))

                if debit > 0:
                    entry.add_debit(account, debit, currency)
                if credit > 0:
                    entry.add_credit(account, credit, currency)

            journal_entries.append(entry)

        return journal_entries


class ArcadeDecoderAdapter(BaseDecoderAdapter):
    """Adapter for Arcade notebook decoder (v2.0.0)"""

    PLATFORM = Platform.ARCADE
    CONTRACT_ADDRESSES = list(ARCADE_CONTRACTS.keys())

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        super().__init__(w3, fund_wallets)

    def _initialize_decoder(self) -> bool:
        """Initialize Arcade decoder with contracts."""
        try:
            from .arcade_decoder import ArcadeEventDecoder, ArcadeJournalEntryGenerator

            # Load LoanCore contract
            loan_core_addr = "0x81b2f8fc75bab64a6b144aa6d2faa127b4fa7fd9"
            loan_core_abi = load_abi(loan_core_addr, "arcade_loancore")

            if not loan_core_abi:
                self._initialization_error = "Could not load Arcade LoanCore ABI"
                return False

            loan_core_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(loan_core_addr),
                abi=loan_core_abi
            )

            self._notebook_decoder = ArcadeEventDecoder(
                w3=self.w3,
                wallet_metadata=self.wallet_metadata,
                loancore_address=loan_core_addr
            )
            self._journal_generator = ArcadeJournalEntryGenerator(
                wallet_metadata=self.wallet_metadata
            )
            logger.info("Arcade adapter initialized successfully")
            return True

        except ImportError as e:
            self._initialization_error = f"Import error: {e}"
            return False
        except Exception as e:
            self._initialization_error = str(e)
            return False

    def can_decode(self, tx: Dict, receipt: Dict) -> bool:
        to_addr = (tx.get('to') or '').lower()
        if to_addr in ARCADE_CONTRACTS:
            return True

        for log in receipt.get('logs', []):
            log_addr = log.get('address', '').lower()
            if log_addr in ARCADE_CONTRACTS:
                return True

        return False

    def decode(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal) -> DecodedTransaction:
        if not self._ensure_initialized():
            return self._create_basic_result(tx, receipt, block, eth_price,
                                            f"Arcade decoder not initialized: {self._initialization_error}")

        tx_hash = self._normalize_tx_hash(tx)
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        try:
            # Decode events from the transaction
            raw_events = self._notebook_decoder.decode_transaction(tx_hash)

            # Convert raw event dicts to DecodedEvent objects
            decoded_events = []
            for evt in raw_events:
                decoded_events.append(DecodedEvent(
                    name=evt.get('event', 'Unknown'),
                    args=evt,  # Pass full event dict as args
                    log_index=evt.get('logIndex', 0),
                    contract_address=self._notebook_decoder.loancore_address
                ))

            # Determine category from events
            category = self._determine_category(raw_events)

            # Generate journal entries using the journal generator
            journal_entries = self._generate_journal_entries(raw_events, eth_price, timestamp, tx_hash)

            # Determine wallet roles
            wallet_roles = self._extract_wallet_roles(raw_events)

            return DecodedTransaction(
                status="success",
                tx_hash=tx_hash,
                platform=Platform.ARCADE,
                category=category,
                block=tx.get('blockNumber', 0),
                timestamp=timestamp,
                eth_price=eth_price,
                gas_used=receipt.get('gasUsed', 0),
                gas_fee=gas_fee,
                from_address=tx.get('from', ''),
                to_address=tx.get('to', '') or '',
                value=wei_to_eth(tx.get('value', 0)),
                function_name=self._get_function_name(raw_events),
                journal_entries=journal_entries,
                events=decoded_events,
                wallet_roles=wallet_roles,
                positions={},
            )

        except Exception as e:
            logger.error(f"Error decoding Arcade tx {tx_hash}: {e}")
            import traceback
            traceback.print_exc()
            return self._create_basic_result(tx, receipt, block, eth_price, str(e))

    def _generate_journal_entries(self, events: List[Dict], eth_price: Decimal,
                                   timestamp: datetime, tx_hash: str) -> List[JournalEntry]:
        """Generate journal entries from decoded events using the journal generator."""
        import pandas as pd

        if not events:
            return []

        journal_entries = []

        try:
            # Convert events to DataFrame for the generator
            df = pd.DataFrame(events)

            # Add ETH price column required by generator
            df['eth_price'] = float(eth_price)

            # Process each event type
            for evt in events:
                event_type = evt.get('event', '')

                if event_type == 'LoanStarted':
                    # Generate loan origination entries
                    entries_df = self._journal_generator.generate_loan_started_entries(df[df['event'] == 'LoanStarted'])
                    journal_entries.extend(self._convert_df_to_journal_entries(entries_df, eth_price, timestamp, tx_hash))

                elif event_type == 'LoanRepaid':
                    # Generate loan repayment entries
                    entries_df = self._journal_generator.generate_loan_repaid_entries(df[df['event'] == 'LoanRepaid'])
                    journal_entries.extend(self._convert_df_to_journal_entries(entries_df, eth_price, timestamp, tx_hash))

                elif event_type == 'LoanClaimed':
                    # Generate collateral seizure entries
                    entries_df = self._journal_generator.generate_loan_claimed_entries(df[df['event'] == 'LoanClaimed'])
                    journal_entries.extend(self._convert_df_to_journal_entries(entries_df, eth_price, timestamp, tx_hash))

        except Exception as e:
            logger.warning(f"Could not generate Arcade journal entries: {e}")
            import traceback
            traceback.print_exc()

        return journal_entries

    def _convert_df_to_journal_entries(self, df, eth_price: Decimal,
                                        timestamp: datetime, tx_hash: str) -> List[JournalEntry]:
        """Convert journal entry DataFrame to JournalEntry objects.

        The ArcadeJournalEntryGenerator returns DataFrames with columns:
        - date, platform, account_name, debit, credit, cryptocurrency
        - fund_id, wallet_id, lender, borrower, loan_id, etc.

        We group entries by (loan_id, event) to create balanced journal entries.
        """
        if df is None or df.empty:
            return []

        import uuid
        entries = []

        # Determine grouping key - use loan_id and event if available, otherwise treat as single entry
        if 'loan_id' in df.columns and 'event' in df.columns:
            group_cols = ['loan_id', 'event']
        elif 'loan_id' in df.columns:
            group_cols = ['loan_id']
        else:
            # No grouping - treat entire df as one entry
            group_cols = None

        if group_cols:
            grouped = df.groupby(group_cols)
        else:
            # Create a single group
            grouped = [(('all',), df)]

        for group_key, group in grouped:
            first_row = group.iloc[0]

            # Get event type for description and category
            event_type = first_row.get('event', 'Unknown')
            loan_id = first_row.get('loan_id', '')

            # Determine category from event type
            if 'Started' in str(event_type) or 'Emitted' in str(event_type):
                category = TransactionCategory.LOAN_ORIGINATION
            elif 'Repaid' in str(event_type):
                category = TransactionCategory.LOAN_REPAYMENT
            elif 'Claimed' in str(event_type) or 'Foreclos' in str(event_type):
                category = TransactionCategory.COLLATERAL_SEIZURE
            else:
                category = TransactionCategory.CONTRACT_CALL

            entry = JournalEntry(
                entry_id=f"arcade_{event_type}_{loan_id}_{uuid.uuid4().hex[:8]}",
                date=first_row.get('date', timestamp),
                description=f"Arcade {event_type} - Loan #{loan_id}" if loan_id else f"Arcade {event_type}",
                tx_hash=tx_hash,
                category=category,
                platform=Platform.ARCADE,
                wallet_address=first_row.get('wallet_id', first_row.get('lender', '')),
                wallet_role=first_row.get('transaction_type', 'lender'),
                eth_usd_price=eth_price,
                posting_status=PostingStatus.AUTO_POST if event_type in ['LoanStarted', 'LoanRepaid', 'LoanClaimed'] else PostingStatus.REVIEW_QUEUE
            )

            for _, row in group.iterrows():
                # Handle both column naming conventions
                debit = float(row.get('debit', row.get('debit_crypto', 0)) or 0)
                credit = float(row.get('credit', row.get('credit_crypto', 0)) or 0)
                account = row.get('account_name', '')
                currency = row.get('cryptocurrency', 'WETH')

                if debit > 0:
                    entry.add_debit(account, Decimal(str(debit)), currency)
                if credit > 0:
                    entry.add_credit(account, Decimal(str(credit)), currency)

            entries.append(entry)

        return entries

    def _extract_wallet_roles(self, events: List[Dict]) -> Dict[str, str]:
        """Extract wallet roles from decoded events."""
        roles = {}
        for evt in events:
            lender = evt.get('lender', '')
            borrower = evt.get('borrower', '')
            if lender:
                roles[lender.lower()] = 'lender'
            if borrower:
                roles[borrower.lower()] = 'borrower'
        return roles

    def _determine_category(self, events: List[Dict]) -> TransactionCategory:
        if not events:
            return TransactionCategory.CONTRACT_CALL

        event_types = {evt.get('event', '') for evt in events}

        if 'LoanStarted' in event_types:
            return TransactionCategory.LOAN_ORIGINATION
        elif 'LoanRepaid' in event_types:
            return TransactionCategory.LOAN_REPAYMENT
        elif 'LoanRolledOver' in event_types:
            return TransactionCategory.LOAN_REFINANCE
        elif 'LoanClaimed' in event_types:
            return TransactionCategory.COLLATERAL_SEIZURE

        return TransactionCategory.CONTRACT_CALL

    def _get_function_name(self, events: List[Dict]) -> str:
        if not events:
            return "unknown"
        return events[0].get('event', 'unknown') if events else "unknown"


class NFTfiDecoderAdapter(BaseDecoderAdapter):
    """Adapter for NFTfi notebook decoder (v2.0.0)"""

    PLATFORM = Platform.NFTFI
    CONTRACT_ADDRESSES = list(NFTFI_CONTRACTS.keys())

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        super().__init__(w3, fund_wallets)

    def _initialize_decoder(self) -> bool:
        """Initialize NFTfi decoder."""
        try:
            from .nftfi_decoder import NFTfiEventDecoder, NFTfiJournalEntryGenerator

            self._notebook_decoder = NFTfiEventDecoder(
                w3=self.w3,
                wallet_metadata=self.wallet_metadata
            )
            self._journal_generator = NFTfiJournalEntryGenerator(
                w3=self.w3,
                wallet_metadata=self.wallet_metadata
            )
            logger.info("NFTfi adapter initialized successfully")
            return True

        except ImportError as e:
            self._initialization_error = f"Import error: {e}"
            return False
        except Exception as e:
            self._initialization_error = str(e)
            return False

    def can_decode(self, tx: Dict, receipt: Dict) -> bool:
        to_addr = (tx.get('to') or '').lower()
        if to_addr in NFTFI_CONTRACTS:
            return True

        for log in receipt.get('logs', []):
            log_addr = log.get('address', '').lower()
            if log_addr in NFTFI_CONTRACTS:
                return True

        return False

    def decode(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal) -> DecodedTransaction:
        if not self._ensure_initialized():
            return self._create_basic_result(tx, receipt, block, eth_price,
                                            f"NFTfi decoder not initialized: {self._initialization_error}")

        tx_hash = self._normalize_tx_hash(tx)
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        try:
            events = self._notebook_decoder.decode_transaction(tx_hash)
            category = self._determine_category(events)
            journal_entries = []

            # Convert events to DecodedEvent objects
            decoded_events = []
            if events:
                for e in events:
                    try:
                        event_dict = e.to_dict() if hasattr(e, 'to_dict') else vars(e)
                        decoded_events.append(DecodedEvent(
                            name=e.event_type if hasattr(e, 'event_type') else str(type(e).__name__),
                            args=event_dict,
                            log_index=getattr(e, 'log_index', 0),
                            contract_address=getattr(e, 'contract_address', '')
                        ))
                    except Exception as ev_err:
                        logger.debug(f"Error converting NFTfi event: {ev_err}")

            # Generate journal entries if we have events and a journal generator
            if events and self._journal_generator:
                try:
                    import pandas as pd
                    # Convert events to DataFrame for journal generator methods
                    events_df = pd.DataFrame([e.to_dict() if hasattr(e, 'to_dict') else vars(e) for e in events])
                    all_entries = []

                    # Generate loan started entries (origination)
                    try:
                        df_started = self._journal_generator.generate_loan_started_entries(events_df)
                        if df_started is not None and not df_started.empty:
                            all_entries.append(df_started)
                    except Exception as e:
                        logger.debug(f"LoanStarted entries error: {e}")

                    # Generate loan repaid entries
                    try:
                        df_repaid = self._journal_generator.generate_loan_repaid_entries(events_df)
                        if df_repaid is not None and not df_repaid.empty:
                            all_entries.append(df_repaid)
                    except Exception as e:
                        logger.debug(f"LoanRepaid entries error: {e}")

                    # Generate loan liquidated entries
                    try:
                        df_liquidated = self._journal_generator.generate_loan_liquidated_entries(events_df)
                        if df_liquidated is not None and not df_liquidated.empty:
                            all_entries.append(df_liquidated)
                    except Exception as e:
                        logger.debug(f"LoanLiquidated entries error: {e}")

                    if all_entries:
                        combined_df = pd.concat(all_entries, ignore_index=True)
                        journal_entries = self._convert_journal_entries(combined_df, events)
                except Exception as je_err:
                    logger.debug(f"Journal entry generation error: {je_err}")

            return DecodedTransaction(
                status="success",
                tx_hash=tx_hash,
                platform=Platform.NFTFI,
                category=category,
                block=tx.get('blockNumber', 0),
                timestamp=timestamp,
                eth_price=eth_price,
                gas_used=receipt.get('gasUsed', 0),
                gas_fee=gas_fee,
                from_address=tx.get('from', ''),
                to_address=tx.get('to', '') or '',
                value=wei_to_eth(tx.get('value', 0)),
                function_name=self._get_function_name(events),
                journal_entries=journal_entries,
                events=decoded_events,
                wallet_roles={},
                positions={},
            )

        except Exception as e:
            logger.error(f"Error decoding NFTfi tx {tx_hash}: {e}")
            return self._create_basic_result(tx, receipt, block, eth_price, str(e))

    def _determine_category(self, events) -> TransactionCategory:
        if not events:
            return TransactionCategory.CONTRACT_CALL

        event_types = {e.event_type for e in events}

        if 'LoanStarted' in event_types:
            return TransactionCategory.LOAN_ORIGINATION
        elif 'LoanRepaid' in event_types:
            return TransactionCategory.LOAN_REPAYMENT
        elif 'LoanLiquidated' in event_types:
            return TransactionCategory.COLLATERAL_SEIZURE

        return TransactionCategory.CONTRACT_CALL

    def _get_function_name(self, events) -> str:
        if not events:
            return "unknown"
        return events[0].event_type if events else "unknown"

    def _convert_journal_entries(self, entries_df, events) -> List[JournalEntry]:
        """Convert NFTfi DataFrame rows to JournalEntry objects"""
        from collections import defaultdict
        import uuid

        if entries_df is None or entries_df.empty:
            return []

        # Group entries by (tx_hash, event_type) to create one JournalEntry per event
        grouped = defaultdict(list)
        for _, row in entries_df.iterrows():
            tx_hash = row.get('hash', row.get('transaction_hash', ''))
            event = row.get('event', row.get('event_type', 'unknown'))
            key = (tx_hash, event, row.get('loan_id', ''))
            grouped[key].append(row)

        journal_entries = []
        for (tx_hash, event, loan_id), rows in grouped.items():
            first_row = rows[0]
            currency = first_row.get('cryptocurrency', first_row.get('currency', 'WETH'))

            # Map event type to category
            if 'Started' in event:
                cat = TransactionCategory.LOAN_ORIGINATION
            elif 'Repaid' in event:
                cat = TransactionCategory.LOAN_REPAYMENT
            elif 'Liquidated' in event:
                cat = TransactionCategory.COLLATERAL_SEIZURE
            else:
                cat = TransactionCategory.CONTRACT_CALL

            # Auto-post for known loan events, review queue for others
            auto_post_events = {'LoanStarted', 'LoanRepaid', 'LoanLiquidated'}
            posting_status = PostingStatus.AUTO_POST if any(e in event for e in auto_post_events) else PostingStatus.REVIEW_QUEUE

            entry = JournalEntry(
                entry_id=f"nftfi_{event}_{loan_id}_{uuid.uuid4().hex[:8]}",
                date=first_row.get('date', datetime.now(timezone.utc)),
                description=f"NFTfi {event} - Loan #{loan_id}" if loan_id else f"NFTfi {event}",
                tx_hash=tx_hash,
                category=cat,
                platform=Platform.NFTFI,
                wallet_address=first_row.get('wallet_id', first_row.get('lender', '')),
                wallet_role=first_row.get('fund_role', 'lender'),
                eth_usd_price=Decimal(str(first_row.get('eth_price', 0))),
                posting_status=posting_status
            )

            # Add debit/credit entries from each row
            for row in rows:
                account = row.get('account_name', 'unknown')
                debit = Decimal(str(row.get('debit', 0)))
                credit = Decimal(str(row.get('credit', 0)))

                if debit > 0:
                    entry.add_debit(account, debit, currency)
                if credit > 0:
                    entry.add_credit(account, credit, currency)

            journal_entries.append(entry)

        return journal_entries


class ZhartaDecoderAdapter(BaseDecoderAdapter):
    """Adapter for Zharta notebook decoder (v3.0.0)"""

    PLATFORM = Platform.ZHARTA
    CONTRACT_ADDRESSES = list(ZHARTA_CONTRACTS.keys())

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        super().__init__(w3, fund_wallets)
        # fund_wallets is already a list from base class

    def _initialize_decoder(self) -> bool:
        """Initialize Zharta decoder."""
        try:
            from .zharta_decoder import ZhartaDecoder, ZhartaJournalGenerator

            self._notebook_decoder = ZhartaDecoder(
                w3=self.w3,
                fund_wallets=self.fund_wallets  # List, not Set
            )
            self._journal_generator = ZhartaJournalGenerator(
                fund_wallets=self.fund_wallets  # List, not Set
            )
            logger.info("Zharta adapter initialized successfully")
            return True

        except ImportError as e:
            self._initialization_error = f"Import error: {e}"
            return False
        except Exception as e:
            self._initialization_error = str(e)
            return False

    def can_decode(self, tx: Dict, receipt: Dict) -> bool:
        to_addr = (tx.get('to') or '').lower()
        if to_addr in ZHARTA_CONTRACTS:
            return True

        for log in receipt.get('logs', []):
            log_addr = log.get('address', '').lower()
            if log_addr in ZHARTA_CONTRACTS:
                return True

        return False

    def decode(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal) -> DecodedTransaction:
        if not self._ensure_initialized():
            return self._create_basic_result(tx, receipt, block, eth_price,
                                            f"Zharta decoder not initialized: {self._initialization_error}")

        tx_hash = self._normalize_tx_hash(tx)
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        try:
            events = self._notebook_decoder.decode_transaction(tx_hash)
            logger.info(f"Zharta decoded {len(events) if events else 0} events for tx {tx_hash[:16]}...")

            if events:
                for e in events:
                    logger.info(f"  Event: {e.event_type}, loan_id={e.loan_id}, is_fund_borrower={e.is_fund_borrower}, is_fund_lender={e.is_fund_lender}")
            else:
                logger.warning(f"  No Zharta events decoded for {tx_hash[:16]}...")

            category = self._determine_category(events)

            # Convert ZhartaEvent objects to DecodedEvent objects
            decoded_events = []
            if events:
                for e in events:
                    decoded_events.append(DecodedEvent(
                        name=e.event_type,
                        args=e.to_dict(),
                        log_index=e.log_index,
                        contract_address=e.contract_name  # Use contract_name as identifier
                    ))

            # Generate journal entries using ZhartaJournalGenerator
            journal_entries = []
            if events and self._journal_generator:
                import pandas as pd
                events_df = pd.DataFrame([e.to_dict() for e in events])
                if not events_df.empty:
                    entries_df = self._journal_generator.generate_entries(events_df)
                    journal_entries = self._convert_journal_entries(entries_df, events, eth_price, timestamp)
                    logger.info(f"  Generated {len(journal_entries)} journal entries for Zharta tx")

            # Extract wallet roles
            wallet_roles = {}
            if events:
                for e in events:
                    if e.borrower and e.is_fund_borrower:
                        wallet_roles[e.borrower.lower()] = 'borrower'
                    if e.lender and e.is_fund_lender:
                        wallet_roles[e.lender.lower()] = 'lender'

            return DecodedTransaction(
                status="success",
                tx_hash=tx_hash,
                platform=Platform.ZHARTA,
                category=category,
                block=tx.get('blockNumber', 0),
                timestamp=timestamp,
                eth_price=eth_price,
                gas_used=receipt.get('gasUsed', 0),
                gas_fee=gas_fee,
                from_address=tx.get('from', ''),
                to_address=tx.get('to', '') or '',
                value=wei_to_eth(tx.get('value', 0)),
                function_name=self._get_function_name(events),
                journal_entries=journal_entries,
                events=decoded_events,
                wallet_roles=wallet_roles,
                positions={},
            )

        except Exception as e:
            logger.error(f"Error decoding Zharta tx {tx_hash}: {e}")
            import traceback
            traceback.print_exc()
            return self._create_basic_result(tx, receipt, block, eth_price, str(e))

    def _determine_category(self, events) -> TransactionCategory:
        if not events:
            return TransactionCategory.CONTRACT_CALL

        event_types = {e.event_type for e in events}

        if 'LoanCreated' in event_types:
            return TransactionCategory.LOAN_ORIGINATION
        elif 'LoanPaid' in event_types or 'LoanPayment' in event_types or 'LoanReplaced' in event_types:
            return TransactionCategory.LOAN_REPAYMENT
        elif 'LoanDefaulted' in event_types or 'LiquidationRemoved' in event_types:
            return TransactionCategory.COLLATERAL_SEIZURE

        return TransactionCategory.CONTRACT_CALL

    def _get_function_name(self, events) -> str:
        if not events:
            return "unknown"
        return events[0].event_type if events else "unknown"

    def _convert_journal_entries(self, entries_df, events, eth_price: Decimal, timestamp: datetime) -> List[JournalEntry]:
        """Convert Zharta journal entry DataFrame to JournalEntry objects"""
        if entries_df is None or entries_df.empty:
            return []

        from collections import defaultdict
        import uuid

        # Group by (tx_hash, event_type, loan_id)
        grouped = defaultdict(list)
        for _, row in entries_df.iterrows():
            tx_hash = row.get('tx_hash', '')
            event = row.get('event_type', 'unknown')
            loan_id = row.get('loan_id', '')
            key = (tx_hash, event, loan_id)
            grouped[key].append(row)

        journal_entries = []
        for (tx_hash, event, loan_id), rows in grouped.items():
            first_row = rows[0]
            currency = first_row.get('token_symbol', 'WETH')

            # Determine category
            if 'Created' in str(event):
                cat = TransactionCategory.LOAN_ORIGINATION
            elif 'Paid' in str(event) or 'Payment' in str(event) or 'Replaced' in str(event):
                cat = TransactionCategory.LOAN_REPAYMENT
            elif 'Defaulted' in str(event) or 'Liquidation' in str(event):
                cat = TransactionCategory.COLLATERAL_SEIZURE
            else:
                cat = TransactionCategory.CONTRACT_CALL

            # Auto-post for known loan events
            auto_post_events = {'LoanCreated', 'LoanPaid', 'LoanPayment', 'LoanReplaced'}
            posting_status = PostingStatus.AUTO_POST if event in auto_post_events else PostingStatus.REVIEW_QUEUE

            # Get date from row or use timestamp
            entry_date = first_row.get('date', first_row.get('timestamp', timestamp))

            entry = JournalEntry(
                entry_id=f"zharta_{event}_{loan_id}_{uuid.uuid4().hex[:8]}",
                date=entry_date,
                description=f"Zharta {event} - Loan #{loan_id}" if loan_id else f"Zharta {event}",
                tx_hash=str(tx_hash) if tx_hash else 'unknown',
                category=cat,
                platform=Platform.ZHARTA,
                wallet_address=str(first_row.get('borrower', first_row.get('lender', ''))),
                wallet_role="lender" if first_row.get('is_fund_lender') else "borrower",
                eth_usd_price=eth_price,
                posting_status=posting_status
            )

            # Add debit/credit entries
            for row in rows:
                account = row.get('account_name', 'unknown')
                debit = Decimal(str(row.get('debit_crypto', 0)))
                credit = Decimal(str(row.get('credit_crypto', 0)))

                if debit > 0:
                    entry.add_debit(account, debit, str(currency))
                if credit > 0:
                    entry.add_credit(account, credit, str(currency))

            if entry.entries:
                journal_entries.append(entry)

        return journal_entries
