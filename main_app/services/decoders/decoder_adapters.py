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
    "0xb7b1bc9b44eb0d3e61b52550c85c29d7a43db96c": "OriginationController",
    "0x74241e1a9c021643289476426b9b70229ab40d53": "RepaymentController",
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
}


def _build_wallet_metadata(fund_wallets: List[str]) -> Dict[str, Dict]:
    """Convert fund_wallets list to wallet_metadata dict expected by notebook decoders"""
    return {
        addr.lower(): {
            'fund_id': 'fund',
            'wallet_name': f'Wallet {addr[:8]}',
            'category': 'fund',
            'wallet_type': 'hot',
        }
        for addr in fund_wallets
    }


class BlurDecoderAdapter(BaseDecoder):
    """Adapter for Blur notebook decoder (v1.0.0)"""

    PLATFORM = Platform.BLUR
    CONTRACT_ADDRESSES = [BLUR_BLEND_PROXY, BLUR_POOL]

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        self.w3 = w3
        self.fund_wallets = [w.lower() for w in fund_wallets]
        self.wallet_metadata = _build_wallet_metadata(fund_wallets)
        self.positions = {}
        self.contracts_cache = {}
        self._notebook_decoder = None
        self._journal_generator = None
        self._initialized = False

    def _load_abis(self):
        """Load Blur contract ABIs and initialize notebook decoder"""
        if self._initialized:
            return

        try:
            from .blur_decoder import BlurEventDecoder, BlurJournalEntryGenerator

            # Load Blur Blend contract (proxy with implementation ABI)
            blend_abi = load_abi(BLUR_BLEND_PROXY, "blur")
            if not blend_abi:
                # Try loading implementation ABI
                impl_address = "0xB258CA5559b11cD702F363796522b04D7722Ea56"
                blend_abi = load_abi(impl_address, "blur_impl")

            pool_abi = load_abi(BLUR_POOL, "blur_pool")

            if blend_abi and pool_abi:
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
                self._initialized = True
                logger.info("Blur adapter initialized with contracts")
            else:
                logger.warning("Could not load Blur ABIs")

        except Exception as e:
            logger.error(f"Failed to initialize Blur adapter: {e}")

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
        self._load_abis()

        tx_hash = tx.get('hash', b'').hex() if isinstance(tx.get('hash'), bytes) else str(tx.get('hash', ''))
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        # If decoder not initialized, return basic result
        if not self._notebook_decoder:
            return self._create_basic_result(tx, receipt, block, eth_price, "Blur decoder not initialized")

        try:
            # Decode using notebook decoder
            events = self._notebook_decoder.decode_transaction(tx_hash)

            # Convert to DecodedTransaction
            category = self._determine_category(events)
            journal_entries = []

            if events and self._journal_generator:
                # Generate journal entries using individual methods
                import pandas as pd
                events_df = pd.DataFrame([e.to_dict() for e in events])
                all_entries = []

                # Originations
                try:
                    df_orig = self._journal_generator.generate_loan_origination_entries(events_df)
                    if not df_orig.empty:
                        all_entries.append(df_orig)
                except Exception as e:
                    logger.debug(f"Origination entries error: {e}")

                # Repayments (returns tuple: entries, accruals)
                try:
                    df_repay, df_accruals = self._journal_generator.generate_loan_repayment_entries(events_df)
                    if not df_repay.empty:
                        all_entries.append(df_repay)
                    if not df_accruals.empty:
                        all_entries.append(df_accruals)
                except Exception as e:
                    logger.debug(f"Repayment entries error: {e}")

                # Refinances (returns tuple)
                try:
                    df_refi, df_refi_acc = self._journal_generator.generate_refinance_entries(events_df)
                    if not df_refi.empty:
                        all_entries.append(df_refi)
                    if not df_refi_acc.empty:
                        all_entries.append(df_refi_acc)
                except Exception as e:
                    logger.debug(f"Refinance entries error: {e}")

                # Seizes (returns tuple)
                try:
                    df_seize, df_seize_acc = self._journal_generator.generate_seize_entries(events_df)
                    if not df_seize.empty:
                        all_entries.append(df_seize)
                    if not df_seize_acc.empty:
                        all_entries.append(df_seize_acc)
                except Exception as e:
                    logger.debug(f"Seize entries error: {e}")

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
        entries = []
        if entries_df is None or entries_df.empty:
            return entries

        for _, row in entries_df.iterrows():
            entries.append(JournalEntry(
                account=row.get('account_name', ''),
                debit=Decimal(str(row.get('debit', 0))),
                credit=Decimal(str(row.get('credit', 0))),
                currency=row.get('currency', 'ETH'),
                description=row.get('description', ''),
            ))
        return entries

    def _create_basic_result(self, tx: Dict, receipt: Dict, block: Dict,
                            eth_price: Decimal, error: str = None) -> DecodedTransaction:
        """Create basic result when decoding fails"""
        tx_hash = tx.get('hash', b'').hex() if isinstance(tx.get('hash'), bytes) else str(tx.get('hash', ''))
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        return DecodedTransaction(
            status="error" if error else "success",
            tx_hash=tx_hash,
            platform=Platform.BLUR,
            category=TransactionCategory.CONTRACT_CALL,
            block=tx.get('blockNumber', 0),
            timestamp=timestamp,
            eth_price=eth_price,
            gas_used=receipt.get('gasUsed', 0),
            gas_fee=gas_fee,
            from_address=tx.get('from', ''),
            to_address=tx.get('to', '') or '',
            value=wei_to_eth(tx.get('value', 0)),
            function_name="unknown",
            error=error,
        )


class GondiDecoderAdapter(BaseDecoder):
    """Adapter for Gondi notebook decoder (v1.7.1)"""

    PLATFORM = Platform.GONDI
    CONTRACT_ADDRESSES = list(GONDI_CONTRACTS.keys())

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        self.w3 = w3
        self.fund_wallets = [w.lower() for w in fund_wallets]
        self.wallet_metadata = _build_wallet_metadata(fund_wallets)
        self.positions = {}
        self.contracts_cache = {}
        self._notebook_decoder = None
        self._journal_generator = None
        self._initialized = False

    def _load_abis(self):
        """Load Gondi contract ABIs and initialize notebook decoder"""
        if self._initialized:
            return

        try:
            from .gondi_decoder import GondiEventDecoder, GondiJournalEntryGenerator

            # Load Gondi contracts
            contracts = {}
            for addr, version in GONDI_CONTRACTS.items():
                abi = load_abi(addr, f"gondi_{version}")
                if abi:
                    contracts[addr] = self.w3.eth.contract(
                        address=Web3.to_checksum_address(addr),
                        abi=abi
                    )

            if contracts:
                logger.info(f"Gondi adapter loaded {len(contracts)} contracts:")
                for addr, c in contracts.items():
                    logger.info(f"  {addr}: {c.address}")
                logger.info(f"Gondi adapter wallet_metadata has {len(self.wallet_metadata)} wallets")
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
                self._initialized = True
                logger.info(f"Gondi adapter initialized with {len(contracts)} contracts")
            else:
                logger.warning("Could not load any Gondi ABIs")

        except Exception as e:
            logger.error(f"Failed to initialize Gondi adapter: {e}")

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
        self._load_abis()

        tx_hash = tx.get('hash', b'').hex() if isinstance(tx.get('hash'), bytes) else str(tx.get('hash', ''))
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        if not self._notebook_decoder:
            logger.warning(f"Gondi decoder not initialized for tx {tx_hash[:16]}")
            return self._create_basic_result(tx, receipt, block, eth_price, "Gondi decoder not initialized")

        try:
            events = self._notebook_decoder.decode_transaction(tx_hash)
            logger.info(f"Gondi decoded {len(events) if events else 0} events for tx {tx_hash[:16]}")
            if events:
                for e in events:
                    logger.info(f"  Event: {e.event_type}, fund_tranches={len(e.fund_tranches)}, old_fund_tranches={len(e.old_fund_tranches)}")
            category = self._determine_category(events)
            journal_entries = []

            if events and self._journal_generator:
                # Pass events list directly - generator expects DecodedGondiEvent objects
                result = self._journal_generator.process_events(events)
                # Combine all journal entry DataFrames from result
                import pandas as pd
                entry_keys = ['new_loans', 'repayments', 'refinances', 'foreclosures',
                              'liquidations', 'interest_accruals', 'accrual_reversals']
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

            entry = JournalEntry(
                entry_id=f"gondi_{event}_{loan_id}_{uuid.uuid4().hex[:8]}",
                date=first_row.get('date', datetime.now(timezone.utc)),
                description=f"Gondi {event} - Loan #{loan_id}" if loan_id else f"Gondi {event}",
                tx_hash=tx_hash,
                category=TransactionCategory.LOAN_ORIGINATION if 'Emitted' in event else
                         TransactionCategory.LOAN_REPAYMENT if 'Repaid' in event else
                         TransactionCategory.LOAN_REFINANCE if 'Refinanced' in event else
                         TransactionCategory.COLLATERAL_SEIZURE if 'Liquidated' in event else
                         TransactionCategory.CONTRACT_CALL,
                platform=Platform.GONDI,
                wallet_address=first_row.get('wallet_id', first_row.get('lender', '')),
                wallet_role=first_row.get('fund_role', 'lender'),
                eth_usd_price=Decimal(str(first_row.get('eth_price', 0))),
                posting_status=PostingStatus.REVIEW_QUEUE
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

    def _create_basic_result(self, tx: Dict, receipt: Dict, block: Dict,
                            eth_price: Decimal, error: str = None) -> DecodedTransaction:
        tx_hash = tx.get('hash', b'').hex() if isinstance(tx.get('hash'), bytes) else str(tx.get('hash', ''))
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        return DecodedTransaction(
            status="error" if error else "success",
            tx_hash=tx_hash,
            platform=Platform.GONDI,
            category=TransactionCategory.CONTRACT_CALL,
            block=tx.get('blockNumber', 0),
            timestamp=timestamp,
            eth_price=eth_price,
            gas_used=receipt.get('gasUsed', 0),
            gas_fee=gas_fee,
            from_address=tx.get('from', ''),
            to_address=tx.get('to', '') or '',
            value=wei_to_eth(tx.get('value', 0)),
            function_name="unknown",
            error=error,
        )


class ArcadeDecoderAdapter(BaseDecoder):
    """Adapter for Arcade notebook decoder (v2.0.0)"""

    PLATFORM = Platform.ARCADE
    CONTRACT_ADDRESSES = list(ARCADE_CONTRACTS.keys())

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        self.w3 = w3
        self.fund_wallets = [w.lower() for w in fund_wallets]
        self.wallet_metadata = _build_wallet_metadata(fund_wallets)
        self.positions = {}
        self.contracts_cache = {}
        self._notebook_decoder = None
        self._journal_generator = None
        self._initialized = False

    def _load_abis(self):
        if self._initialized:
            return

        try:
            from .arcade_decoder import ArcadeEventDecoder, ArcadeJournalEntryGenerator

            # Load LoanCore contract
            loan_core_addr = "0x81b2f8fc75bab64a6b144aa6d2faa127b4fa7fd9"
            loan_core_abi = load_abi(loan_core_addr, "arcade_loancore")

            if loan_core_abi:
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
                self._initialized = True
                logger.info("Arcade adapter initialized")
            else:
                logger.warning("Could not load Arcade LoanCore ABI")

        except Exception as e:
            logger.error(f"Failed to initialize Arcade adapter: {e}")

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
        self._load_abis()

        tx_hash = tx.get('hash', b'').hex() if isinstance(tx.get('hash'), bytes) else str(tx.get('hash', ''))
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        if not self._notebook_decoder:
            return self._create_basic_result(tx, receipt, block, eth_price, "Arcade decoder not initialized")

        try:
            events = self._notebook_decoder.decode_transaction(tx_hash)
            category = self._determine_category(events)

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
                function_name=self._get_function_name(events),
                journal_entries=[],
                events=[],
                wallet_roles={},
                positions={},
            )

        except Exception as e:
            logger.error(f"Error decoding Arcade tx {tx_hash}: {e}")
            return self._create_basic_result(tx, receipt, block, eth_price, str(e))

    def _determine_category(self, events) -> TransactionCategory:
        if not events:
            return TransactionCategory.CONTRACT_CALL

        event_types = {e.event_type for e in events}

        if 'LoanStarted' in event_types:
            return TransactionCategory.LOAN_ORIGINATION
        elif 'LoanRepaid' in event_types:
            return TransactionCategory.LOAN_REPAYMENT
        elif 'LoanRolledOver' in event_types:
            return TransactionCategory.LOAN_REFINANCE
        elif 'LoanClaimed' in event_types:
            return TransactionCategory.COLLATERAL_SEIZURE

        return TransactionCategory.CONTRACT_CALL

    def _get_function_name(self, events) -> str:
        if not events:
            return "unknown"
        return events[0].event_type if events else "unknown"

    def _create_basic_result(self, tx: Dict, receipt: Dict, block: Dict,
                            eth_price: Decimal, error: str = None) -> DecodedTransaction:
        tx_hash = tx.get('hash', b'').hex() if isinstance(tx.get('hash'), bytes) else str(tx.get('hash', ''))
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        return DecodedTransaction(
            status="error" if error else "success",
            tx_hash=tx_hash,
            platform=Platform.ARCADE,
            category=TransactionCategory.CONTRACT_CALL,
            block=tx.get('blockNumber', 0),
            timestamp=timestamp,
            eth_price=eth_price,
            gas_used=receipt.get('gasUsed', 0),
            gas_fee=gas_fee,
            from_address=tx.get('from', ''),
            to_address=tx.get('to', '') or '',
            value=wei_to_eth(tx.get('value', 0)),
            function_name="unknown",
            error=error,
        )


class NFTfiDecoderAdapter(BaseDecoder):
    """Adapter for NFTfi notebook decoder (v2.0.0)"""

    PLATFORM = Platform.NFTFI
    CONTRACT_ADDRESSES = list(NFTFI_CONTRACTS.keys())

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        self.w3 = w3
        self.fund_wallets = [w.lower() for w in fund_wallets]
        self.wallet_metadata = _build_wallet_metadata(fund_wallets)
        self.positions = {}
        self.contracts_cache = {}
        self._notebook_decoder = None
        self._journal_generator = None
        self._initialized = False

    def _load_abis(self):
        if self._initialized:
            return

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
            self._initialized = True
            logger.info("NFTfi adapter initialized")

        except Exception as e:
            logger.error(f"Failed to initialize NFTfi adapter: {e}")

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
        self._load_abis()

        tx_hash = tx.get('hash', b'').hex() if isinstance(tx.get('hash'), bytes) else str(tx.get('hash', ''))
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        if not self._notebook_decoder:
            return self._create_basic_result(tx, receipt, block, eth_price, "NFTfi decoder not initialized")

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
                posting_status=PostingStatus.REVIEW_QUEUE
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

    def _create_basic_result(self, tx: Dict, receipt: Dict, block: Dict,
                            eth_price: Decimal, error: str = None) -> DecodedTransaction:
        tx_hash = tx.get('hash', b'').hex() if isinstance(tx.get('hash'), bytes) else str(tx.get('hash', ''))
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        return DecodedTransaction(
            status="error" if error else "success",
            tx_hash=tx_hash,
            platform=Platform.NFTFI,
            category=TransactionCategory.CONTRACT_CALL,
            block=tx.get('blockNumber', 0),
            timestamp=timestamp,
            eth_price=eth_price,
            gas_used=receipt.get('gasUsed', 0),
            gas_fee=gas_fee,
            from_address=tx.get('from', ''),
            to_address=tx.get('to', '') or '',
            value=wei_to_eth(tx.get('value', 0)),
            function_name="unknown",
            error=error,
        )


class ZhartaDecoderAdapter(BaseDecoder):
    """Adapter for Zharta notebook decoder (v3.0.0)"""

    PLATFORM = Platform.ZHARTA
    CONTRACT_ADDRESSES = list(ZHARTA_CONTRACTS.keys())

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        self.w3 = w3
        self.fund_wallets = [w.lower() for w in fund_wallets]
        self.fund_wallets_set = set(self.fund_wallets)
        self.positions = {}
        self.contracts_cache = {}
        self._notebook_decoder = None
        self._journal_generator = None
        self._initialized = False

    def _load_abis(self):
        if self._initialized:
            return

        try:
            from .zharta_decoder import ZhartaDecoder, ZhartaJournalGenerator

            self._notebook_decoder = ZhartaDecoder(
                w3=self.w3,
                fund_wallets=self.fund_wallets_set
            )
            self._journal_generator = ZhartaJournalGenerator(
                fund_wallets=self.fund_wallets_set
            )
            self._initialized = True
            logger.info("Zharta adapter initialized")

        except Exception as e:
            logger.error(f"Failed to initialize Zharta adapter: {e}")

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
        self._load_abis()

        tx_hash = tx.get('hash', b'').hex() if isinstance(tx.get('hash'), bytes) else str(tx.get('hash', ''))
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        if not self._notebook_decoder:
            return self._create_basic_result(tx, receipt, block, eth_price, "Zharta decoder not initialized")

        try:
            events = self._notebook_decoder.decode_transaction(tx_hash)
            category = self._determine_category(events)

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
                journal_entries=[],
                events=[],
                wallet_roles={},
                positions={},
            )

        except Exception as e:
            logger.error(f"Error decoding Zharta tx {tx_hash}: {e}")
            return self._create_basic_result(tx, receipt, block, eth_price, str(e))

    def _determine_category(self, events) -> TransactionCategory:
        if not events:
            return TransactionCategory.CONTRACT_CALL

        event_types = {e.event_type for e in events}

        if 'LoanCreated' in event_types:
            return TransactionCategory.LOAN_ORIGINATION
        elif 'LoanPaid' in event_types:
            return TransactionCategory.LOAN_REPAYMENT
        elif 'LoanDefaulted' in event_types:
            return TransactionCategory.COLLATERAL_SEIZURE

        return TransactionCategory.CONTRACT_CALL

    def _get_function_name(self, events) -> str:
        if not events:
            return "unknown"
        return events[0].event_type if events else "unknown"

    def _create_basic_result(self, tx: Dict, receipt: Dict, block: Dict,
                            eth_price: Decimal, error: str = None) -> DecodedTransaction:
        tx_hash = tx.get('hash', b'').hex() if isinstance(tx.get('hash'), bytes) else str(tx.get('hash', ''))
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        gas_fee = calculate_gas_fee(receipt, tx)

        return DecodedTransaction(
            status="error" if error else "success",
            tx_hash=tx_hash,
            platform=Platform.ZHARTA,
            category=TransactionCategory.CONTRACT_CALL,
            block=tx.get('blockNumber', 0),
            timestamp=timestamp,
            eth_price=eth_price,
            gas_used=receipt.get('gasUsed', 0),
            gas_fee=gas_fee,
            from_address=tx.get('from', ''),
            to_address=tx.get('to', '') or '',
            value=wei_to_eth(tx.get('value', 0)),
            function_name="unknown",
            error=error,
        )
