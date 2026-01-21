"""
Blur Blend (NFT Lending) Decoder - Consolidated One-Shot Solution
==================================================================

Decodes Blur Blend NFT lending events and generates GAAP-compliant journal entries.

Key Characteristics:
- Callable loans with NO fixed term (open-ended)
- Interest calculated retrospectively at repayment using continuous compounding
- Interest accruals generated AFTER repayment with backdated daily entries

Events Handled:
- LoanOfferTaken: New loan origination
- Repay: Loan repayment by borrower
- Refinance: Loan refinanced to new lender
- StartAuction: Lender calls the loan (begins Dutch auction)
- Seize: Lender seizes NFT collateral after auction expires
- BuyLocked: Third party purchases locked NFT

Author: Real World NAV
Version: 1.0.0
"""




from __future__ import annotations

import math
import json
import pandas as pd
import numpy as np
from pathlib import Path
from decimal import Decimal, getcontext
from datetime import datetime, timedelta, time as dt_time, timezone
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from web3 import Web3

# Set decimal precision for financial calculations
getcontext().prec = 28

# Module exports
__all__ = [
    # Core Classes
    'BlurEventDecoder',
    'BlurJournalGenerator',
    'DecodedBlurEvent',
    'LienData',
    'BlurEventType',
    'BlurAccounts',
    'WalletRole',
    # Constants
    'BLUR_BLEND_PROXY',
    'BLUR_BLEND_IMPL',
    'BLUR_POOL',
    'PLATFORM',
    # Utility Functions
    'standardize_dataframe',
    'convert_to_human_readable',
    'print_event_details',
]

# ============================================================================
# CONSTANTS
# ============================================================================

# Contract Addresses
BLUR_BLEND_PROXY = "0x29469395eAf6f95920E59F858042f0e28D98a20B"
BLUR_BLEND_IMPL = "0xB258CA5559b11cD702F363796522b04D7722Ea56"
BLUR_POOL = "0x0000000000A39bb272e79075ade125fd351887Ac"

# Conversion Constants
WAD = Decimal(10**18)  # 18 decimals (ETH/WETH/Blur Pool)
BASIS_POINTS = Decimal(10000)  # Rate denominator
SECONDS_PER_YEAR = Decimal(365 * 24 * 3600)  # 31,536,000 seconds

# Platform Identifier
PLATFORM = "Blur"

# Payable Currency (Blur Pool is ETH-equivalent)
BLUR_POOL_CURRENCY = BLUR_POOL  # 1:1 peg with ETH

NFT_MARKETPLACE_SELECTORS = {
    "0xda815cb5": "takeBidSingle",       # Sell single NFT
    "0x7034d120": "takeBid",             # Sell multiple NFTs
    "0x336d8206": "takeAskSinglePool",   # Buy NFT using Pool
}

# Event signature for ERC721 Transfer
ERC721_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Blur Pool addresses (for payment detection)
BLUR_POOL_ADDRESS = "0x0000000000A39bb272e79075ade125fd351887Ac".lower()

# ============================================================================
# CHART OF ACCOUNTS - EXACT NAMES
# ============================================================================

class BlurAccounts:
    """Exact account names from Chart of Accounts for Blur Pool"""

    # Assets
    DIGITAL_ASSETS_BLUR_POOL = "digital_assets_blur_pool"
    LOAN_RECEIVABLE = "loan_receivable_cryptocurrency_blur_pool"
    LOAN_RECEIVABLE_PROVISION = "loan_receivable_cryptocurrency_blur_pool_provision_for_bad_debt"
    INTEREST_RECEIVABLE = "interest_receivable_cryptocurrency_blur_pool"
    INVESTMENTS_NFTS_SEIZED = "investments_nfts_seized_collateral"

    # Liabilities (for borrower perspective)
    NOTE_PAYABLE = "note_payable_cryptocurrency_cryptocurrency_blur_pool"
    INTEREST_PAYABLE = "interest_payable_cryptocurrency_blur_pool"

    # Income
    INTEREST_INCOME = "interest_income_cryptocurrency_blur_pool"

    # Expenses
    INTEREST_EXPENSE = "interest_expense_cryptocurrency_blur_pool"
    BAD_DEBT_EXPENSE = "bad_debt_expense_cryptocurrency_blur_pool"
    GAS_FEE_EXPENSE = "gas_fee_expense"

    # Investments
    INVESTMENTS_NFTS = "investments_nfts"

    # Clearing
    DEEMED_CASH_USD = "deemed_cash_usd"


# ============================================================================
# ENUMS
# ============================================================================

class BlurEventType(Enum):
    """Blur Blend event types"""
    LOAN_OFFER_TAKEN = "LoanOfferTaken"
    REPAY = "Repay"
    REFINANCE = "Refinance"
    START_AUCTION = "StartAuction"
    SEIZE = "Seize"
    BUY_LOCKED = "BuyLocked"


class WalletRole(Enum):
    """Wallet roles in Blur transactions"""
    LENDER = "LENDER"
    BORROWER = "BORROWER"
    NEW_LENDER = "NEW_LENDER"
    OLD_LENDER = "OLD_LENDER"
    LENDER_RECEIVING = "LENDER_RECEIVING"
    BORROWER_REPAYING = "BORROWER_REPAYING"
    LENDER_CALLING_LOAN = "LENDER_CALLING_LOAN"
    LENDER_SEIZING = "LENDER_SEIZING"
    BORROWER_REFINANCING = "BORROWER_REFINANCING"
    LIQUIDATOR = "LIQUIDATOR"
    UNKNOWN = "UNKNOWN"


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class LienData:
    """
    Lien struct data from Blur Blend contract.

    Matches Solidity struct:
    struct Lien {
        address lender;
        address borrower;
        ERC721 collection;
        uint256 tokenId;
        uint256 amount;        // principal in wei
        uint256 startTime;     // timestamp (seconds)
        uint256 rate;          // annual rate in basis points
        uint256 auctionStartBlock;
        uint256 auctionDuration;
    }
    """
    lender: str
    borrower: str
    collection: str
    token_id: int
    amount_wei: int  # principal in wei
    start_time: int  # timestamp in seconds
    rate_bips: int   # annual rate in basis points
    auction_start_block: int
    auction_duration: int

    @classmethod
    def from_tuple(cls, data: tuple) -> 'LienData':
        """Create from function calldata tuple"""
        return cls(
            lender=str(data[0]).lower(),
            borrower=str(data[1]).lower(),
            collection=str(data[2]).lower(),
            token_id=int(data[3]),
            amount_wei=int(data[4]),
            start_time=int(data[5]),
            rate_bips=int(data[6]),
            auction_start_block=int(data[7]),
            auction_duration=int(data[8]),
        )

    @property
    def principal(self) -> Decimal:
        """Principal in ETH/Blur Pool units"""
        return Decimal(self.amount_wei) / WAD

    @property
    def rate_decimal(self) -> Decimal:
        """Rate as decimal (e.g., 0.10 for 10%)"""
        return Decimal(self.rate_bips) / BASIS_POINTS

    @property
    def start_datetime(self) -> datetime:
        """Start time as datetime"""
        return datetime.fromtimestamp(self.start_time, tz=timezone.utc)

    def compute_debt_at(self, timestamp: int) -> Decimal:
        """
        Compute current debt using continuous compounding.
        Matches Solidity: Helpers.computeCurrentDebt()

        Formula: debt = principal x e^(rate x time_in_years)
        """
        if timestamp <= self.start_time:
            return self.principal

        loan_time_seconds = Decimal(timestamp - self.start_time)
        years = loan_time_seconds / SECONDS_PER_YEAR

        # Continuous compounding: e^(r * t)
        exponent = float(self.rate_decimal * years)
        compound_factor = Decimal(str(math.exp(exponent)))

        debt_wei = Decimal(self.amount_wei) * compound_factor
        return debt_wei / WAD

    def compute_interest_at(self, timestamp: int) -> Decimal:
        """Compute accrued interest at given timestamp"""
        debt = self.compute_debt_at(timestamp)
        return debt - self.principal

    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'lender': self.lender,
            'borrower': self.borrower,
            'collection': self.collection,
            'token_id': self.token_id,
            'amount_wei': self.amount_wei,
            'principal': float(self.principal),
            'start_time': self.start_time,
            'start_datetime': self.start_datetime.isoformat(),
            'rate_bips': self.rate_bips,
            'rate_percent': float(self.rate_decimal * 100),
            'auction_start_block': self.auction_start_block,
            'auction_duration': self.auction_duration,
        }


@dataclass
class DecodedBlurEvent:
    """Decoded Blur Blend event with all fields"""
    event_type: str
    tx_hash: str
    block_number: int
    log_index: int
    transaction_timestamp: int  # Unix timestamp
    transaction_datetime: datetime

    # Lien data (from function calldata)
    lien_id: Optional[int] = None
    lien_data: Optional[LienData] = None

    # Event-specific fields
    offer_hash: Optional[str] = None

    # Refinance fields
    new_lender: Optional[str] = None
    new_amount_wei: Optional[int] = None
    new_rate_bips: Optional[int] = None
    new_auction_duration: Optional[int] = None

    # BuyLocked fields
    buyer: Optional[str] = None
    seller: Optional[str] = None
    purchase_price_wei: Optional[int] = None  # NFT purchase price from offer

    # BorrowerRefinance fields
    function_name: Optional[str] = None  # Function that triggered this event
    borrower_outflow_wei: Optional[int] = None  # Cash paid by borrower in refinance
    old_lender: Optional[str] = None  # For borrowerRefinance: the lender being paid off

    # Computed fields (populated at repayment/refinance)
    debt_at_event_wei: Optional[int] = None
    interest_earned_wei: Optional[int] = None

    # Actual transfer amounts from Pool (ground truth)
    actual_pool_transfer_wei: Optional[int] = None

    @property
    def principal(self) -> Decimal:
        """Principal in native units"""
        if self.lien_data:
            return self.lien_data.principal
        return Decimal(0)

    @property
    def interest_earned(self) -> Decimal:
        """Interest earned in native units"""
        if self.interest_earned_wei:
            return Decimal(self.interest_earned_wei) / WAD
        return Decimal(0)

    @property
    def debt_at_event(self) -> Decimal:
        """Total debt at event time"""
        if self.debt_at_event_wei:
            return Decimal(self.debt_at_event_wei) / WAD
        return self.principal

    @property
    def new_amount(self) -> Decimal:
        """New loan amount for refinance"""
        if self.new_amount_wei:
            return Decimal(self.new_amount_wei) / WAD
        return Decimal(0)

    @property
    def purchase_price(self) -> Decimal:
        """NFT purchase price for BuyLocked"""
        if self.purchase_price_wei:
            return Decimal(self.purchase_price_wei) / WAD
        return Decimal(0)

    @property
    def borrower_outflow(self) -> Decimal:
        """Borrower cash outflow for borrowerRefinance"""
        if self.borrower_outflow_wei:
            return Decimal(self.borrower_outflow_wei) / WAD
        return Decimal(0)

    def to_dict(self) -> Dict:
        """Convert to dictionary for DataFrame"""
        result = {
            'event': self.event_type,
            'transactionHash': self.tx_hash,
            'blockNumber': self.block_number,
            'logIndex': self.log_index,
            'transaction_timestamp': self.transaction_timestamp,
            'transaction_datetime': self.transaction_datetime,
            'lien_id': self.lien_id,
            'offer_hash': self.offer_hash,
            'function_name': self.function_name,
            'new_lender': self.new_lender,
            'new_amount_wei': self.new_amount_wei,
            'new_amount': float(self.new_amount) if self.new_amount_wei else None,
            'new_rate_bips': self.new_rate_bips,
            'new_auction_duration': self.new_auction_duration,
            'buyer': self.buyer,
            'seller': self.seller,
            'purchase_price_wei': self.purchase_price_wei,
            'purchase_price': float(self.purchase_price) if self.purchase_price_wei else None,
            'old_lender': self.old_lender,
            'borrower_outflow_wei': self.borrower_outflow_wei,
            'borrower_outflow': float(self.borrower_outflow) if self.borrower_outflow_wei else None,
            'debt_at_event_wei': self.debt_at_event_wei,
            'debt_at_event': float(self.debt_at_event) if self.debt_at_event_wei else None,
            'interest_earned_wei': self.interest_earned_wei,
            'interest_earned': float(self.interest_earned) if self.interest_earned_wei else None,
            'actual_pool_transfer_wei': self.actual_pool_transfer_wei,
        }

        # Add lien data fields if present
        if self.lien_data:
            result.update({
                'lender': self.lien_data.lender,
                'borrower': self.lien_data.borrower,
                'collection': self.lien_data.collection,
                'token_id': self.lien_data.token_id,
                'principal_wei': self.lien_data.amount_wei,
                'principal': float(self.lien_data.principal),
                'start_time': self.lien_data.start_time,
                'start_datetime': self.lien_data.start_datetime.isoformat(),
                'rate_bips': self.lien_data.rate_bips,
                'auction_start_block': self.lien_data.auction_start_block,
                'auction_duration': self.lien_data.auction_duration,
            })

        return result


# ============================================================================
# BLUR EVENT DECODER
# ============================================================================

class BlurEventDecoder:
    """
    Decodes Blur Blend events from transaction receipts.

    Key insight: The Repay event is minimal (only lienId, collection),
    but ALL functions that settle loans pass the full Lien struct in calldata.
    We decode function input to get loan details.
    """

    # Event signatures (keccak256 hashes)
    EVENT_SIGNATURES = {
        'LoanOfferTaken': 'LoanOfferTaken(bytes32,uint256,address,address,address,uint256,uint256,uint256,uint256)',
        'Repay': 'Repay(uint256,address)',
        'Refinance': 'Refinance(uint256,address,address,uint256,uint256,uint256)',
        'StartAuction': 'StartAuction(uint256,address)',
        'Seize': 'Seize(uint256,address)',
        'BuyLocked': 'BuyLocked(uint256,address,address,address,uint256)',
    }

    def __init__(
        self,
        w3: Web3,
        blend_contract,
        pool_contract,
        wallet_metadata: Dict[str, Dict],
        debug: bool = False,
    ):
        """
        Initialize decoder.

        Args:
            w3: Web3 instance
            blend_contract: Instantiated Blend contract (proxy with impl ABI)
            pool_contract: Instantiated Blur Pool contract
            wallet_metadata: Dict mapping addresses to wallet info
            debug: Enable verbose debug output
        """
        self.w3 = w3
        self.blend_contract = blend_contract
        self.pool_contract = pool_contract
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        self.fund_wallet_list = [
            addr.lower() for addr, info in wallet_metadata.items()
            if info.get('category', '').lower() == 'fund'
            ]
        #self.fund_wallet_list = list(self.wallet_metadata.keys())
        self.debug = debug

        # Precompute event topic hashes
        self.event_topics = {
            name: Web3.keccak(text=sig).hex()
            for name, sig in self.EVENT_SIGNATURES.items()
        }

        # Block timestamp cache
        self._block_cache: Dict[int, int] = {}

    @lru_cache(maxsize=10000)
    def _get_block_timestamp(self, block_number: int) -> int:
        """Get block timestamp with caching"""
        if block_number in self._block_cache:
            return self._block_cache[block_number]

        block = self.w3.eth.get_block(block_number)
        ts = block['timestamp']
        self._block_cache[block_number] = ts
        return ts

    def decode_transaction(self, tx_hash: str) -> List[DecodedBlurEvent]:
        """
        Decode all Blur Blend events from a transaction.

        Strategy:
        1. Decode function input to get Lien struct (has all loan details)
        2. Decode events to get event-specific data
        3. Calculate debt/interest using continuous compounding
        4. Extract actual pool transfers as ground truth

        Args:
            tx_hash: Transaction hash

        Returns:
            List of DecodedBlurEvent objects
        """
        tx = self.w3.eth.get_transaction(tx_hash)
        receipt = self.w3.eth.get_transaction_receipt(tx_hash)

        if receipt['status'] != 1:
            print(f"[!] Transaction failed: {tx_hash}")
            return []

        block_timestamp = self._get_block_timestamp(tx['blockNumber'])
        block_datetime = datetime.fromtimestamp(block_timestamp, tz=timezone.utc)

        # Step 1: Decode function input to get Lien data
        func_name, func_params, lien_data = self._decode_function_input(tx)

        if not func_name:
            print(f"[!] Could not decode function for {tx_hash}")
            # Continue anyway - events like LoanOfferTaken contain all needed data
            func_name = "unknown"
            func_params = {}

        # Debug output for settlement functions without lien_data
        settlement_functions = {'repay', 'refinance', 'refinanceAuction', 'seize', 'takeBid', 'takeBidV2', 'buyLocked'}
        if self.debug or (func_name in settlement_functions and lien_data is None):
            self._debug_print_lien_extraction(func_name, func_params, lien_data)

        # Step 2: Extract pool transfers (actual amounts)
        pool_transfers = self._decode_pool_transfers(receipt['logs'])

        # Step 3: Decode events
        decoded_events = []

        for log in receipt['logs']:
            # Only process Blend contract events
            if log['address'].lower() != BLUR_BLEND_PROXY.lower():
                continue

            if not log['topics']:
                continue

            topic0 = log['topics'][0].hex() if isinstance(log['topics'][0], bytes) else log['topics'][0]

            # Match event type
            event_type = None
            for name, topic_hash in self.event_topics.items():
                if topic0 == topic_hash:
                    event_type = name
                    break

            if not event_type:
                continue

            # Create decoded event
            event = self._decode_event(
                event_type=event_type,
                log=log,
                tx_hash=tx_hash,
                block_number=tx['blockNumber'],
                block_timestamp=block_timestamp,
                block_datetime=block_datetime,
                lien_data=lien_data,
                func_params=func_params,
                pool_transfers=pool_transfers,
                func_name=func_name,
            )

            if event:
                decoded_events.append(event)

        return decoded_events

    def _decode_function_input(self, tx) -> Tuple[Optional[str], Optional[Dict], Optional[LienData]]:
        """
        Decode function input to extract Lien data.

        Handles different function signatures:
        - Most functions: lien parameter directly
        - seize(): LienPointer[] array with nested lien

        Returns:
            (function_name, function_params, lien_data)
        """
        try:
            func_obj, func_params = self.blend_contract.decode_function_input(tx['input'])
            func_name = func_obj.fn_name
            # borrow creates new loan - no existing lien to extract
            # All data comes from LoanOfferTaken event
            if func_name == "borrow":
                return "borrow", dict(func_params), None  # lien_data is None, use event

            # Extract lien from params - handle different structures
            lien_data = None
            lien_param = None

            # Case 1: Direct 'lien' parameter (repay, refinanceAuction, startAuction, etc.)
            if 'lien' in func_params:
                lien_param = func_params.get('lien')

            # Case 2: 'lienPointers' array (seize function)
            # Case 2: 'lienPointers' array (seize function)
            # FIXED: Build lookup dict for ALL liens, not just first
            elif 'lienPointers' in func_params:
                lien_pointers = func_params.get('lienPointers')

                if lien_pointers and len(lien_pointers) > 0:
                    # Build a lookup dict: lienId -> LienData
                    lien_lookup = {}

                    for pointer in lien_pointers:
                        if isinstance(pointer, (tuple, list)):
                            # LienPointer tuple: (lien, lienId)
                            lien_tuple = pointer[0]
                            lien_id = int(pointer[1]) if len(pointer) > 1 else None
                        elif isinstance(pointer, dict):
                            lien_tuple = pointer.get('lien')
                            lien_id = pointer.get('lienId')
                        else:
                            continue

                        if lien_tuple and lien_id is not None:
                            # Parse lien tuple into LienData
                            if isinstance(lien_tuple, (tuple, list)):
                                lien_obj = LienData.from_tuple(lien_tuple)
                            elif isinstance(lien_tuple, dict):
                                lien_obj = LienData(
                                    lender=str(lien_tuple.get('lender', '')).lower(),
                                    borrower=str(lien_tuple.get('borrower', '')).lower(),
                                    collection=str(lien_tuple.get('collection', '')).lower(),
                                    token_id=int(lien_tuple.get('tokenId', 0)),
                                    amount_wei=int(lien_tuple.get('amount', 0)),
                                    start_time=int(lien_tuple.get('startTime', 0)),
                                    rate_bips=int(lien_tuple.get('rate', 0)),
                                    auction_start_block=int(lien_tuple.get('auctionStartBlock', 0)),
                                    auction_duration=int(lien_tuple.get('auctionDuration', 0)),
                                )
                            else:
                                continue

                            lien_lookup[lien_id] = lien_obj

                    # Store lookup in func_params for _process_seize to use
                    func_params['_lien_lookup'] = lien_lookup

                    # For backwards compatibility, set lien_param to first lien
                    first_pointer = lien_pointers[0]
                    if isinstance(first_pointer, (tuple, list)):
                        lien_param = first_pointer[0]
                    elif isinstance(first_pointer, dict):
                        lien_param = first_pointer.get('lien')











            #####################################


            #elif 'lienPointers' in func_params:
             #   lien_pointers = func_params.get('lienPointers')
              #  if lien_pointers and len(lien_pointers) > 0:
                    # Extract first LienPointer's lien (most seizes are single-lien)
               #     first_pointer = lien_pointers[0]
                #    if isinstance(first_pointer, (tuple, list)):
                        # LienPointer tuple: (lien, lienId)
                 #       lien_param = first_pointer[0]
                  #  elif isinstance(first_pointer, dict):
                   #     lien_param = first_pointer.get('lien')

            # Case 3: Check for other possible parameter names
            elif 'lien_' in func_params:
                lien_param = func_params.get('lien_')

            # Parse lien_param into LienData
            if lien_param:
                if isinstance(lien_param, (tuple, list)):
                    lien_data = LienData.from_tuple(lien_param)
                elif isinstance(lien_param, dict):
                    lien_data = LienData(
                        lender=str(lien_param.get('lender', '')).lower(),
                        borrower=str(lien_param.get('borrower', '')).lower(),
                        collection=str(lien_param.get('collection', '')).lower(),
                        token_id=int(lien_param.get('tokenId', 0)),
                        amount_wei=int(lien_param.get('amount', 0)),
                        start_time=int(lien_param.get('startTime', 0)),
                        rate_bips=int(lien_param.get('rate', 0)),
                        auction_start_block=int(lien_param.get('auctionStartBlock', 0)),
                        auction_duration=int(lien_param.get('auctionDuration', 0)),
                    )

            return func_name, dict(func_params), lien_data

        except Exception as e:
            print(f"[!] Could not decode function input: {e}")
            return None, None, None

    def _debug_print_lien_extraction(self, func_name: str, func_params: Dict, lien_data: Optional[LienData]):
        """Debug helper to print lien extraction details"""
        print(f"\n  [list] Function: {func_name}")
        print(f"     Parameters: {list(func_params.keys()) if func_params else 'None'}")
        if lien_data:
            print(f"     [OK] Lien extracted: lender={lien_data.lender[:10]}..., borrower={lien_data.borrower[:10]}...")
        else:
            print(f"     ❌ Lien NOT extracted")
            if func_params:
                for key, value in func_params.items():
                    print(f"        - {key}: {type(value).__name__}")

    def _decode_pool_transfers(self, logs: List) -> List[Dict]:
        """
        Decode Transfer events from Blur Pool contract.
        These represent actual ETH-equivalent flows.
        """
        transfers = []

        # ERC20 Transfer signature: Transfer(address,address,uint256)
        transfer_topic = Web3.keccak(text="Transfer(address,address,uint256)").hex()

        for log in logs:
            if log['address'].lower() != BLUR_POOL.lower():
                continue

            if not log['topics'] or len(log['topics']) < 3:
                continue

            topic0 = log['topics'][0].hex() if isinstance(log['topics'][0], bytes) else log['topics'][0]

            if topic0 != transfer_topic:
                continue

            # Decode from/to from indexed topics
            from_topic = log['topics'][1].hex() if isinstance(log['topics'][1], bytes) else log['topics'][1]
            to_topic = log['topics'][2].hex() if isinstance(log['topics'][2], bytes) else log['topics'][2]

            from_addr = '0x' + from_topic[-40:]
            to_addr = '0x' + to_topic[-40:]

            # Decode amount from data
            data = log['data']
            if isinstance(data, bytes):
                amount_wei = int.from_bytes(data, byteorder='big')
            else:
                data_hex = data.replace('0x', '') if data else '0'
                amount_wei = int(data_hex, 16) if data_hex else 0

            transfers.append({
                'from': from_addr.lower(),
                'to': to_addr.lower(),
                'amount_wei': amount_wei,
                'amount': Decimal(amount_wei) / WAD,
            })

        return transfers

    def _decode_event(
        self,
        event_type: str,
        log: Dict,
        tx_hash: str,
        block_number: int,
        block_timestamp: int,
        block_datetime: datetime,
        lien_data: Optional[LienData],
        func_params: Optional[Dict],
        pool_transfers: List[Dict],
        func_name: Optional[str] = None,
    ) -> Optional[DecodedBlurEvent]:
        """Decode a single event"""

        try:
            # Get log index
            log_index = log.get('logIndex', 0)
            if isinstance(log_index, str):
                log_index = int(log_index, 16) if log_index.startswith('0x') else int(log_index)

            # Base event
            event = DecodedBlurEvent(
                event_type=event_type,
                tx_hash=tx_hash,
                block_number=block_number,
                log_index=log_index,
                transaction_timestamp=block_timestamp,
                transaction_datetime=block_datetime,
                lien_data=lien_data,
                function_name=func_name,
            )

            # Extract lienId from func_params if available
            if func_params:
                event.lien_id = func_params.get('lienId')

            # Process event-specific data
            if event_type == 'LoanOfferTaken':
                self._process_loan_offer_taken(event, log, lien_data)

            elif event_type == 'Repay':
                self._process_repay(event, log, lien_data, block_timestamp, pool_transfers)

            elif event_type == 'Refinance':
                self._process_refinance(event, log, lien_data, block_timestamp, pool_transfers)

            elif event_type == 'StartAuction':
                self._process_start_auction(event, log)

            elif event_type == 'Seize':
                self._process_seize(event, log, lien_data, block_timestamp, func_params),

            elif event_type == 'BuyLocked':
                self._process_buy_locked(event, log, lien_data, block_timestamp, pool_transfers, func_params)

            return event

        except Exception as e:
            print(f"[!] Error decoding {event_type} event: {e}")
            return None

    def _process_loan_offer_taken(self, event: DecodedBlurEvent, log: Dict, lien_data: Optional[LienData]):
        """
        Process LoanOfferTaken event - new loan origination.

        IMPORTANT: For refinance transactions, the function calldata's 'lien' parameter
        contains OLD loan info, but the LoanOfferTaken event contains NEW loan info.
        We MUST use the event args to get the correct lender for the new loan.
        """
        args = None

        # Try ABI decoding first
        try:
            decoded = self.blend_contract.events.LoanOfferTaken().process_log(log)
            args = decoded['args']
        except Exception:
            # ABI doesn't have event - manually decode
            # LoanOfferTaken(bytes32 offerHash, uint256 lienId, address lender, address borrower,
            #                address collection, uint256 tokenId, uint256 loanAmount, uint256 rate, uint256 auctionDuration)
            # All fields are in data (no indexed parameters besides topic0)
            try:
                from eth_abi import decode
                data = log['data'] if isinstance(log['data'], bytes) else bytes.fromhex(log['data'][2:])
                decoded_data = decode(
                    ['bytes32', 'uint256', 'address', 'address', 'address', 'uint256', 'uint256', 'uint256', 'uint256'],
                    data
                )
                # Note: Based on actual data, tokenId and loanAmount appear swapped
                # collection, loanAmount, tokenId, rate, auctionDuration
                args = {
                    'offerHash': decoded_data[0],
                    'lienId': decoded_data[1],
                    'lender': decoded_data[2],
                    'borrower': decoded_data[3],
                    'collection': decoded_data[4],
                    'loanAmount': decoded_data[5],  # 0.1 ETH = 100000000000000000 wei
                    'tokenId': decoded_data[6],     # NFT token ID (e.g., 990)
                    'rate': decoded_data[7],
                    'auctionDuration': decoded_data[8],
                }
            except Exception as e:
                print(f"[!] Error manually decoding LoanOfferTaken: {e}")

        if args:
            event.offer_hash = args.get('offerHash', b'').hex() if isinstance(args.get('offerHash'), bytes) else args.get('offerHash')
            event.lien_id = args.get('lienId')

            # ALWAYS use event args for LoanOfferTaken - this is the NEW loan
            # Do NOT use lien_data from function calldata as it may be the OLD loan (for refinances)
            event.lien_data = LienData(
                lender=str(args.get('lender', '')).lower(),
                borrower=str(args.get('borrower', '')).lower(),
                collection=str(args.get('collection', '')).lower(),
                token_id=int(args.get('tokenId', 0)),
                amount_wei=int(args.get('loanAmount', 0)),
                start_time=event.transaction_timestamp,
                rate_bips=int(args.get('rate', 0)),
                auction_start_block=0,
                auction_duration=int(args.get('auctionDuration', 0)),
            )

    def _process_repay(
        self,
        event: DecodedBlurEvent,
        log: Dict,
        lien_data: Optional[LienData],
        block_timestamp: int,
        pool_transfers: List[Dict],
    ):
        """Process Repay event - loan repayment"""
        # Repay event is minimal: (lienId, collection)
        # All details come from function calldata (lien_data)

        try:
            decoded = self.blend_contract.events.Repay().process_log(log)
            args = decoded['args']
            event.lien_id = args.get('lienId')
        except:
            pass

        if lien_data:
            # Calculate debt at repayment time
            debt = lien_data.compute_debt_at(block_timestamp)
            interest = lien_data.compute_interest_at(block_timestamp)

            event.debt_at_event_wei = int(debt * WAD)
            event.interest_earned_wei = int(interest * WAD)

            # Find the actual pool transfer to lender (ground truth)
            lender_transfers = [
                t for t in pool_transfers
                if t['to'] == lien_data.lender
            ]
            if lender_transfers:
                # Sum all transfers to lender
                total_to_lender = sum(t['amount_wei'] for t in lender_transfers)
                event.actual_pool_transfer_wei = total_to_lender

    def _process_refinance(
        self,
        event: DecodedBlurEvent,
        log: Dict,
        lien_data: Optional[LienData],
        block_timestamp: int,
        pool_transfers: List[Dict],
    ):
        """
        Process Refinance event - loan refinanced to new lender

        IMPORTANT: For borrowerRefinance, the lien_data from func_params contains
        the OLD loan info (old lender, old principal, old rate). We need to preserve
        this for proper borrower accounting.
        """
        try:
            decoded = self.blend_contract.events.Refinance().process_log(log)
            args = decoded['args']

            event.lien_id = args.get('lienId')
            event.new_lender = str(args.get('newLender', '')).lower()
            event.new_amount_wei = int(args.get('newAmount', 0))
            event.new_rate_bips = int(args.get('newRate', 0))
            event.new_auction_duration = int(args.get('newAuctionDuration', 0))
        except Exception as e:
            print(f"[!] Error processing Refinance event data: {e}")

        if lien_data:
            # Calculate debt at refinance time (what old lender receives)
            debt = lien_data.compute_debt_at(block_timestamp)
            interest = lien_data.compute_interest_at(block_timestamp)

            event.debt_at_event_wei = int(debt * WAD)
            event.interest_earned_wei = int(interest * WAD)

            # For borrowerRefinance, store the old lender from lien_data
            # (the lien_data comes from function calldata, which has OLD loan info)
            if event.function_name == 'borrowerRefinance':
                event.old_lender = lien_data.lender

                # Extract borrower outflow from pool transfers
                # This is the amount borrower paid TO the old lender
                borrower_addr = lien_data.borrower
                old_lender_addr = lien_data.lender

                borrower_outflows = [
                    t for t in pool_transfers
                    if t['from'] == borrower_addr and t['to'] == old_lender_addr
                ]
                if borrower_outflows:
                    total_borrower_outflow = sum(t['amount_wei'] for t in borrower_outflows)
                    event.borrower_outflow_wei = total_borrower_outflow

            # Find transfer to old lender
            old_lender_transfers = [
                t for t in pool_transfers
                if t['to'] == lien_data.lender
            ]
            if old_lender_transfers:
                total_to_old_lender = sum(t['amount_wei'] for t in old_lender_transfers)
                event.actual_pool_transfer_wei = total_to_old_lender

    def _process_start_auction(self, event: DecodedBlurEvent, log: Dict):
        """Process StartAuction event - lender calls the loan"""
        try:
            decoded = self.blend_contract.events.StartAuction().process_log(log)
            args = decoded['args']
            event.lien_id = args.get('lienId')
        except:
            pass

    #def _process_seize(
        #self,
        #event: DecodedBlurEvent,
        #log: Dict,
        #lien_data: Optional[LienData],
       # block_timestamp: int,
    #):
        #"""Process Seize event - lender seizes NFT collateral"""
        #try:
            #decoded = self.blend_contract.events.Seize().process_log(log)
            #args = decoded['args']
            #event.lien_id = args.get('lienId')
        #except Exception as e:
            #print(f"[!] Error decoding Seize event args: {e}")

        #if lien_data:
            # Calculate debt at seize time (principal + accrued interest)
            #debt = lien_data.compute_debt_at(block_timestamp)
            #interest = lien_data.compute_interest_at(block_timestamp)

            #event.debt_at_event_wei = int(debt * WAD)
            #event.interest_earned_wei = int(interest * WAD)
    def _process_seize(
        self,
        event: DecodedBlurEvent,
        log: Dict,
        lien_data: Optional[LienData],
        block_timestamp: int,
        func_params: Optional[Dict] = None,
    ):
        """Process Seize event - lender seizes NFT collateral"""
        # First decode event to get lienId
        lien_id_from_event = None
        try:
            decoded = self.blend_contract.events.Seize().process_log(log)
            args = decoded['args']
            lien_id_from_event = args.get('lienId')
            event.lien_id = lien_id_from_event
        except Exception as e:
            print(f"[!] Error decoding Seize event args: {e}")

        # FIXED: Look up correct lien_data for THIS lienId
        if func_params and '_lien_lookup' in func_params and lien_id_from_event is not None:
            lien_lookup = func_params['_lien_lookup']
            if lien_id_from_event in lien_lookup:
                lien_data = lien_lookup[lien_id_from_event]
            else:
                print(f"  [!] Seize lienId {lien_id_from_event} not found in lookup")

        # Update event with correct lien_data
        if lien_data:
            event.lien_data = lien_data  # IMPORTANT: Update event's lien_data

            # Calculate debt at seize time (principal + accrued interest)
            debt = lien_data.compute_debt_at(block_timestamp)
            interest = lien_data.compute_interest_at(block_timestamp)

            event.debt_at_event_wei = int(debt * WAD)
            event.interest_earned_wei = int(interest * WAD)

    def _process_buy_locked(
        self,
        event: DecodedBlurEvent,
        log: Dict,
        lien_data: Optional[LienData],
        block_timestamp: int,
        pool_transfers: List[Dict],
        func_params: Optional[Dict] = None,
    ):
        """
        Process BuyLocked event - third party purchases locked NFT.

        The purchase price is in func_params['offer']['price'], NOT in the event.
        The event only contains: lienId, collection, buyer, seller, tokenId
        """
        try:
            decoded = self.blend_contract.events.BuyLocked().process_log(log)
            args = decoded['args']

            event.lien_id = args.get('lienId')
            event.buyer = str(args.get('buyer', '')).lower()
            event.seller = str(args.get('seller', '')).lower()  # This is the borrower
        except Exception as e:
            print(f"[!] Error processing BuyLocked event data: {e}")

        # Extract purchase price from offer in function params
        if func_params and 'offer' in func_params:
            offer = func_params['offer']
            # Offer can be dict or tuple
            if isinstance(offer, dict):
                price = offer.get('price', 0)
            elif isinstance(offer, (tuple, list)):
                # Offer structure: (borrower, lienId, price, expirationTime, salt, oracle, fees)
                price = offer[2] if len(offer) > 2 else 0
            else:
                price = 0

            event.purchase_price_wei = int(price)

            if self.debug:
                print(f"  💰 BuyLocked purchase price: {Decimal(price) / WAD:.6f} ETH")

        if lien_data:
            # Calculate debt at purchase time (what lender receives)
            debt = lien_data.compute_debt_at(block_timestamp)
            interest = lien_data.compute_interest_at(block_timestamp)

            event.debt_at_event_wei = int(debt * WAD)
            event.interest_earned_wei = int(interest * WAD)

            # Find transfer to lender (debt repayment)
            lender_transfers = [
                t for t in pool_transfers
                if t['to'] == lien_data.lender
            ]
            if lender_transfers:
                total_to_lender = sum(t['amount_wei'] for t in lender_transfers)
                event.actual_pool_transfer_wei = total_to_lender

    def decode_batch(
        self,
        tx_hashes: List[str],
        max_workers: int = 8,
        filter_fund_wallets: bool = True,
    ) -> pd.DataFrame:
        """
        Decode events from multiple transactions in parallel.

        Args:
            tx_hashes: List of transaction hashes
            max_workers: Parallel workers
            filter_fund_wallets: Only return events involving fund wallets

        Returns:
            DataFrame with decoded events
        """
        all_events = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_hash = {
                executor.submit(self.decode_transaction, tx_hash): tx_hash
                for tx_hash in tx_hashes
            }

            for future in tqdm(
                as_completed(future_to_hash),
                total=len(tx_hashes),
                desc="Decoding Blur events",
                colour="blue"
            ):
                tx_hash = future_to_hash[future]
                try:
                    events = future.result()
                    for event in events:
                        all_events.append(event.to_dict())
                except Exception as e:
                    print(f"[!] Failed to decode {tx_hash}: {e}")

        if not all_events:
            return pd.DataFrame()

        df = pd.DataFrame(all_events)

        # Filter for fund wallets if requested
        if filter_fund_wallets and not df.empty:
            mask = pd.Series([False] * len(df))

            if 'lender' in df.columns:
                mask |= df['lender'].str.lower().isin(self.fund_wallet_list)
            if 'borrower' in df.columns:
                mask |= df['borrower'].str.lower().isin(self.fund_wallet_list)
            if 'new_lender' in df.columns:
                mask |= df['new_lender'].str.lower().isin(self.fund_wallet_list)
            if 'buyer' in df.columns:
                mask |= df['buyer'].str.lower().isin(self.fund_wallet_list)

            df = df[mask]

        return df

#### NFT
    def decode_nft_marketplace_transaction(
        self,
        tx_hash: str,
    ) -> List[Dict]:
        """
        Decode NFT marketplace transactions (takeBidSingle, takeBid, takeAskSinglePool).

        These are NOT lending transactions - they're pure NFT buy/sell.

        Returns list of decoded trades (can be multiple for batch sales).

        Usage:
            trades = decoder.decode_nft_marketplace_transaction(tx_hash)
        """
        try:
            tx = self.w3.eth.get_transaction(tx_hash)
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)

            if receipt['status'] != 1:
                return []

            # Get function selector
            selector = tx['input'][:10].lower() if tx['input'] and len(tx['input']) >= 10 else ''
            func_name = NFT_MARKETPLACE_SELECTORS.get(selector)

            if not func_name:
                return []

            # Get block timestamp
            block = self.w3.eth.get_block(tx['blockNumber'])
            block_timestamp = block['timestamp']
            block_datetime = datetime.fromtimestamp(block_timestamp, tz=timezone.utc)

            tx_from = tx['from'].lower()

            # Extract NFT transfers and payment transfers
            nft_transfers = self._extract_nft_transfers_from_logs(receipt['logs'])
            payment_transfers = self._extract_payment_transfers_from_logs(receipt['logs'])

            if not nft_transfers:
                print(f"[!] No NFT transfers found in {tx_hash[:16]}...")
                return []

            trades = []

            for i, nft in enumerate(nft_transfers):
                seller = nft['from'].lower()
                buyer = nft['to'].lower()

                # Check if fund wallet is involved
                is_fund_seller = seller in self.fund_wallet_list
                is_fund_buyer = buyer in self.fund_wallet_list

                if not is_fund_seller and not is_fund_buyer:
                    continue  # Skip - neither party is fund

                # Determine trade type from fund perspective
                trade_type = 'NFT_SELL' if is_fund_seller else 'NFT_BUY'

                # Find payment amount
                if trade_type == 'NFT_SELL':
                    # Payment TO seller
                    payment_wei = self._find_payment_to_address(payment_transfers, seller)
                else:
                    # Payment FROM buyer or TO seller
                    payment_wei = self._find_payment_from_address(payment_transfers, buyer)
                    if not payment_wei:
                        payment_wei = self._find_payment_to_address(payment_transfers, seller)

                trade = {
                    'event': trade_type,
                    'transactionHash': tx_hash,
                    'blockNumber': tx['blockNumber'],
                    'logIndex': i,
                    'timestamp': block_timestamp,
                    'transaction_datetime': block_datetime,
                    'function_name': func_name,

                    # Parties
                    'buyer': buyer,
                    'seller': seller,

                    # Fund wallet (for journal entries)
                    'fund_wallet': seller if is_fund_seller else buyer,

                    # NFT details
                    'collection': nft['contract'].lower(),
                    'token_id': nft['token_id'],

                    # Price
                    'price_wei': payment_wei,
                    'price_eth': Decimal(payment_wei) / WAD if payment_wei else Decimal(0),

                    # Batch info
                    'is_batch': len(nft_transfers) > 1,
                    'batch_index': i,
                }

                trades.append(trade)

            return trades

        except Exception as e:
            print(f"[!] Error decoding marketplace tx {tx_hash[:16]}...: {e}")
            return []


    def _extract_nft_transfers_from_logs(self, logs: List) -> List[Dict]:
        """
        Extract ERC721 Transfer events from transaction logs.

        ERC721 Transfer has 4 topics: [signature, from, to, tokenId]
        """
        transfers = []

        for log in logs:
            # Skip Blur Pool (that's payment, not NFT)
            if log['address'].lower() in [BLUR_POOL_ADDRESS, WETH_ADDRESS]:
                continue

            if not log['topics'] or len(log['topics']) < 4:
                continue

            topic0 = log['topics'][0].hex() if isinstance(log['topics'][0], bytes) else log['topics'][0]

            if topic0.lower() != ERC721_TRANSFER_TOPIC.lower():
                continue

            try:
                from_topic = log['topics'][1].hex() if isinstance(log['topics'][1], bytes) else log['topics'][1]
                to_topic = log['topics'][2].hex() if isinstance(log['topics'][2], bytes) else log['topics'][2]
                token_id_topic = log['topics'][3].hex() if isinstance(log['topics'][3], bytes) else log['topics'][3]

                from_addr = '0x' + from_topic[-40:]
                to_addr = '0x' + to_topic[-40:]
                token_id = int(token_id_topic, 16)

                transfers.append({
                    'contract': log['address'].lower(),
                    'from': from_addr.lower(),
                    'to': to_addr.lower(),
                    'token_id': token_id,
                })
            except Exception as e:
                continue

        return transfers


    def _extract_payment_transfers_from_logs(self, logs: List) -> List[Dict]:
        """
        Extract Blur Pool and WETH Transfer events (payments).

        ERC20 Transfer has 3 topics: [signature, from, to] + data = amount
        """
        transfers = []

        for log in logs:
            addr = log['address'].lower()

            # Only Blur Pool and WETH
            if addr not in [BLUR_POOL_ADDRESS, WETH_ADDRESS]:
                continue

            if not log['topics'] or len(log['topics']) < 3:
                continue

            topic0 = log['topics'][0].hex() if isinstance(log['topics'][0], bytes) else log['topics'][0]

            if topic0.lower() != ERC721_TRANSFER_TOPIC.lower():
                continue

            try:
                from_topic = log['topics'][1].hex() if isinstance(log['topics'][1], bytes) else log['topics'][1]
                to_topic = log['topics'][2].hex() if isinstance(log['topics'][2], bytes) else log['topics'][2]

                from_addr = '0x' + from_topic[-40:]
                to_addr = '0x' + to_topic[-40:]

                # Decode amount from data
                data = log['data']
                if isinstance(data, bytes):
                    amount = int.from_bytes(data, byteorder='big')
                else:
                    data_hex = data.replace('0x', '') if data else '0'
                    amount = int(data_hex, 16) if data_hex else 0

                transfers.append({
                    'token': addr,
                    'from': from_addr.lower(),
                    'to': to_addr.lower(),
                    'amount_wei': amount,
                })
            except:
                continue

        return transfers


    def _find_payment_to_address(self, transfers: List[Dict], address: str) -> int:
        """Find total payment sent TO an address"""
        total = 0
        for t in transfers:
            if t['to'] == address.lower():
                total += t['amount_wei']
        return total


    def _find_payment_from_address(self, transfers: List[Dict], address: str) -> int:
        """Find total payment sent FROM an address"""
        total = 0
        for t in transfers:
            if t['from'] == address.lower():
                total += t['amount_wei']
        return total
####### NFT

# ============================================================================
# JOURNAL ENTRY GENERATOR
# ============================================================================

class BlurJournalEntryGenerator:
    """
    Generates GAAP-compliant journal entries from decoded Blur events.

    Key principle for Blur (callable loans):
    - At origination: Record principal only (no interest schedule)
    - At repayment: Calculate actual interest, generate BACKDATED daily accruals
    """

    def __init__(
        self,
        wallet_metadata: Dict[str, Dict],
    ):
        """
        Initialize generator.

        Args:
            wallet_metadata: Dict mapping addresses to wallet info
        """
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        self.fund_wallet_list = [
            addr.lower() for addr, info in wallet_metadata.items()
            if info.get('category', '').lower() == 'fund'
            ]
        #self.fund_wallet_list = list(self.wallet_metadata.keys())

    def _get_wallet_info(self, address: str) -> Dict:
        """Get wallet metadata for an address"""
        if not address:
            return {}
        return self.wallet_metadata.get(address.lower(), {})

    def _get_fund_id(self, address: str) -> str:
        """Get fund_id for an address"""
        info = self._get_wallet_info(address)
        return info.get('fund_id', '')

    def _is_fund_wallet(self, address: str) -> bool:
        """Check if address is a fund wallet"""
        return address.lower() in self.fund_wallet_list if address else False

    # =========================================================================
    # LOAN ORIGINATION (LoanOfferTaken)
    # =========================================================================

    def generate_loan_origination_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate journal entries for LoanOfferTaken events.

        Lender perspective:
        - Dr loan_receivable_cryptocurrency_blur_pool (principal)
        - Cr digital_assets_blur_pool (principal)

        Borrower perspective:
        - Dr digital_assets_blur_pool (principal)
        - Cr note_payable_cryptocurrency_cryptocurrency_blur_pool (principal)

        IMPORTANT: For borrowerRefinance where fund is the lender and stays the same lender,
        we SKIP lender entries here. The rollover doesn't involve new cash from lender.
        Those entries are handled by generate_lender_borrower_refinance_entries instead.
        """
        if df.empty:
            return pd.DataFrame()

        loan_events = df[df['event'] == 'LoanOfferTaken'].copy()
        if loan_events.empty:
            return pd.DataFrame()

        # Build a lookup for Refinance events to detect same-lender borrowerRefinance
        # Key: (tx_hash, lien_id) -> (old_lender, new_lender, function_name)
        refinance_lookup = {}
        refinance_events = df[df['event'] == 'Refinance']
        for _, refi_row in refinance_events.iterrows():
            tx_hash = refi_row.get('transactionHash', '')
            lien_id = refi_row.get('lien_id')
            old_lender = str(refi_row.get('lender', '')).lower()  # From lien_data
            new_lender = str(refi_row.get('new_lender', '')).lower()
            func_name = refi_row.get('function_name', '')
            refinance_lookup[(tx_hash, lien_id)] = (old_lender, new_lender, func_name)

        journal_rows = []

        for _, row in loan_events.iterrows():
            lender = str(row.get('lender', '')).lower()
            borrower = str(row.get('borrower', '')).lower()
            function_name = row.get('function_name', '')
            tx_hash = row.get('transactionHash', '')
            lien_id = row.get('lien_id')

            is_lender_fund = self._is_fund_wallet(lender)
            is_borrower_fund = self._is_fund_wallet(borrower)

            if not is_lender_fund and not is_borrower_fund:
                continue

            # Check if this is a borrowerRefinance with same lender
            # In that case, skip lender entries - no new cash goes out
            is_borrower_refinance = function_name == 'borrowerRefinance'
            is_same_lender_refinance = False

            if is_borrower_refinance and (tx_hash, lien_id) in refinance_lookup:
                old_lender, new_lender, _ = refinance_lookup[(tx_hash, lien_id)]
                is_same_lender_refinance = (old_lender == new_lender) and old_lender != ''

            # Extract data
            principal = Decimal(str(row.get('principal', 0)))
            tx_dt = row.get('transaction_datetime')
            if isinstance(tx_dt, str):
                tx_dt = pd.to_datetime(tx_dt, utc=True)

            rate_bips = row.get('rate_bips', 0)
            # Annual interest rate as decimal (e.g., 0.20 for 20%)
            annual_interest_rate = Decimal(str(rate_bips)) / Decimal(10000) if rate_bips else Decimal(0)

            # Common metadata (matching X2Y2 structure)
            common = {
                'date': tx_dt,
                'cryptocurrency': 'BLUR_POOL',
                'transaction_type': 'investments_lending',
                'platform': PLATFORM,
                'event': 'LoanOfferTaken',
                'hash': row.get('transactionHash', ''),
                'loan_id': row.get('lien_id'),  # Standardized name
                'lien_id': row.get('lien_id'),  # Also keep Blur-specific name
                'lender': lender,
                'borrower': borrower,
                'from': lender,
                'to': borrower,
                'contract_address': BLUR_BLEND_PROXY,
                'payable_currency': BLUR_POOL,  # Blur Pool address
                'collateral_address': row.get('collection', ''),
                'token_id': row.get('token_id'),
                'principal_crypto': principal,
                'principal_USD': None,  # Set later with ETH/USD price
                'annual_interest_rate': annual_interest_rate,
                'payoff_amount_crypto': None,  # Indeterminate for callable loans
                'payoff_amount_USD': None,
                'loan_due_date': None,  # Callable loans have no fixed due date
                'rate_bips': rate_bips,
                'start_time': row.get('start_time'),
                # USD pricing placeholders
                'eth_usd_price': None,
                'debit_USD': None,
                'credit_USD': None,
            }

            # LENDER ENTRIES
            # SKIP if this is a borrowerRefinance where lender stays the same
            # In that case, no new cash goes out - it's just a rollover
            # Entries are handled by generate_lender_borrower_refinance_entries instead
            if is_lender_fund and not is_same_lender_refinance:
                lender_fund = self._get_fund_id(lender)
                borrower_fund = self._get_fund_id(borrower)

                lender_common = {
                    **common,
                    'fund_id': lender_fund,
                    'counterparty_fund_id': borrower_fund,
                    'wallet_id': lender,
                }

                # Dr loan_receivable_cryptocurrency_blur_pool
                journal_rows.append({
                    **lender_common,
                    'account_name': BlurAccounts.LOAN_RECEIVABLE,
                    'debit_crypto': principal,
                    'credit_crypto': Decimal(0),
                })

                # Cr digital_assets_blur_pool
                journal_rows.append({
                    **lender_common,
                    'account_name': "deemed_cash_usd", #BlurAccounts.DIGITAL_ASSETS_BLUR_POOL,
                    'debit_crypto': Decimal(0),
                    'credit_crypto': principal,
                })

            # BORROWER ENTRIES
            # SKIP if this is a borrowerRefinance - the funds never touch borrower's wallet
            # (new lender pays old lender directly, borrower only pays their portion)
            # borrowerRefinance is handled separately by generate_borrower_refinance_entries
            if is_borrower_fund and not is_borrower_refinance:
                borrower_fund = self._get_fund_id(borrower)
                lender_fund = self._get_fund_id(lender)

                borrower_common = {
                    **common,
                    'fund_id': borrower_fund,
                    'counterparty_fund_id': lender_fund,
                    'wallet_id': borrower,
                    'transaction_type': 'financing_borrowings',
                }

                # Dr digital_assets_blur_pool
                journal_rows.append({
                    **borrower_common,
                    'account_name': "deemed_cash_usd", #BlurAccounts.DIGITAL_ASSETS_BLUR_POOL,
                    'debit_crypto': principal,
                    'credit_crypto': Decimal(0),
                })

                # Cr note_payable_cryptocurrency_cryptocurrency_blur_pool
                journal_rows.append({
                    **borrower_common,
                    'account_name': BlurAccounts.NOTE_PAYABLE,
                    'debit_crypto': Decimal(0),
                    'credit_crypto': principal,
                })

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    # =========================================================================
    # LOAN REPAYMENT (Repay)
    # =========================================================================

    def generate_loan_repayment_entries(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate journal entries for Repay events.

        Returns:
            (repayment_entries_df, interest_accrual_entries_df)

        Lender perspective:
        1. Backdated daily interest accruals from start_time -> repay_time
        2. Repayment entry:
           - Dr digital_assets_blur_pool (total received)
           - Cr loan_receivable_cryptocurrency_blur_pool (principal)
           - Cr interest_receivable_cryptocurrency_blur_pool (interest)

        Borrower perspective:
        1. Backdated daily interest expense accruals
        2. Repayment entry:
           - Dr note_payable_cryptocurrency_cryptocurrency_blur_pool (principal)
           - Dr interest_payable_cryptocurrency_blur_pool (interest)
           - Cr digital_assets_blur_pool (total paid)
        """
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        ##
        repay_events = df[df['event'] == 'Repay'].copy()
        if repay_events.empty:
            return pd.DataFrame(), pd.DataFrame()

        repayment_rows = []
        accrual_rows = []

        for idx, row in repay_events.iterrows():
            # Skip if missing critical lien data (borrow txs, aggregator txs, failed decodes)
            principal_wei = row.get('principal_wei')
            start_time = row.get('start_time')
            rate_bips = row.get('rate_bips')

            if pd.isna(principal_wei) or pd.isna(start_time) or pd.isna(rate_bips):
                tx_hash = row.get('transactionHash', 'unknown')[:16]
                print(f"[!] Skipping Repay {tx_hash}... - missing lien data")
                continue

            lender = str(row.get('lender', '')).lower()
        ##

            borrower = str(row.get('borrower', '')).lower()

            is_lender_fund = self._is_fund_wallet(lender)
            is_borrower_fund = self._is_fund_wallet(borrower)

            if not is_lender_fund and not is_borrower_fund:
                continue

            # Extract data - handle NaN values
            principal_raw = row.get('principal', 0)
            interest_raw = row.get('interest_earned', 0)

            # Convert to Decimal, treating NaN as 0
            principal = Decimal(str(principal_raw)) if pd.notna(principal_raw) else Decimal(0)
            interest = Decimal(str(interest_raw)) if pd.notna(interest_raw) else Decimal(0)
            total = principal + interest


            start_time = row.get('start_time')
            repay_timestamp = row.get('transaction_timestamp')
            repay_dt = row.get('transaction_datetime')
            if isinstance(repay_dt, str):
                repay_dt = pd.to_datetime(repay_dt, utc=True)

            rate_bips = row.get('rate_bips', 0)
            # Annual interest rate as decimal
            annual_interest_rate = Decimal(str(rate_bips)) / Decimal(10000) if rate_bips else Decimal(0)

            # Common metadata (matching X2Y2 structure)
            common = {
                'cryptocurrency': 'BLUR_POOL',
                'platform': PLATFORM,
                'hash': row.get('transactionHash', ''),
                'loan_id': row.get('lien_id'),  # Standardized name
                'lien_id': row.get('lien_id'),  # Also keep Blur-specific name
                'lender': lender,
                'borrower': borrower,
                'contract_address': BLUR_BLEND_PROXY,
                'payable_currency': BLUR_POOL,
                'collateral_address': row.get('collection', ''),
                'token_id': row.get('token_id'),
                'principal_crypto': principal,
                'principal_USD': None,
                'interest_crypto': interest,
                'interest_USD': None,
                'total_crypto': total,
                'total_USD': None,
                'annual_interest_rate': annual_interest_rate,
                'payoff_amount_crypto': total,  # At repayment, this is the actual payoff
                'payoff_amount_USD': None,
                'loan_due_date': repay_dt,  # Settled on this date
                'rate_bips': rate_bips,
                'start_time': start_time,
                'repay_timestamp': repay_timestamp,
                # USD pricing placeholders
                'eth_usd_price': None,
                'debit_USD': None,
                'credit_USD': None,
            }

            # LENDER ENTRIES
            if is_lender_fund:
                lender_fund = self._get_fund_id(lender)
                borrower_fund = self._get_fund_id(borrower)

                lender_common = {
                    **common,
                    'fund_id': lender_fund,
                    'counterparty_fund_id': borrower_fund,
                    'wallet_id': lender,
                    'from': borrower,
                    'to': lender,
                }

                # Generate backdated interest accruals
                if interest > 0 and start_time and repay_timestamp:
                    accruals = self._generate_daily_interest_accruals(
                        start_timestamp=start_time,
                        end_timestamp=repay_timestamp,
                        principal_wei=int(principal * WAD),
                        rate_bips=rate_bips,
                        is_lender=True,
                        common_metadata={**lender_common, 'transaction_type': 'income_interest_accruals'},
                    )
                    accrual_rows.extend(accruals)

                # Repayment entry
                repay_common = {
                    **lender_common,
                    'date': repay_dt,
                    'event': 'Repay',
                    'transaction_type': 'investments_lending',
                }

                # Dr digital_assets_blur_pool (total received)
                repayment_rows.append({
                    **repay_common,
                    'account_name': "deemed_cash_usd", #BlurAccounts.DIGITAL_ASSETS_BLUR_POOL,
                    'debit_crypto': total,
                    'credit_crypto': Decimal(0),
                })

                # Cr loan_receivable_cryptocurrency_blur_pool (principal)
                repayment_rows.append({
                    **repay_common,
                    'account_name': BlurAccounts.LOAN_RECEIVABLE,
                    'debit_crypto': Decimal(0),
                    'credit_crypto': principal,
                })

                # Cr interest_receivable_cryptocurrency_blur_pool (interest)
                if interest > 0:
                    repayment_rows.append({
                        **repay_common,
                        'account_name': BlurAccounts.INTEREST_RECEIVABLE,
                        'debit_crypto': Decimal(0),
                        'credit_crypto': interest,
                    })

            # BORROWER ENTRIES
            if is_borrower_fund:
                borrower_fund = self._get_fund_id(borrower)
                lender_fund = self._get_fund_id(lender)

                borrower_common = {
                    **common,
                    'fund_id': borrower_fund,
                    'counterparty_fund_id': lender_fund,
                    'wallet_id': borrower,
                    'from': borrower,
                    'to': lender,
                    'transaction_type': 'financing_borrowings',  # Override for borrower
                }

                # Generate backdated interest expense accruals
                if interest > 0 and start_time and repay_timestamp:
                    accruals = self._generate_daily_interest_accruals(
                        start_timestamp=start_time,
                        end_timestamp=repay_timestamp,
                        principal_wei=int(principal * WAD),
                        rate_bips=rate_bips,
                        is_lender=False,
                        common_metadata={**borrower_common, 'transaction_type': 'expense_interest_accruals'},
                    )
                    accrual_rows.extend(accruals)

                # Repayment entry
                repay_common = {
                    **borrower_common,
                    'date': repay_dt,
                    'event': 'Repay',
                    'transaction_type': 'financing_borrowings',
                }

                # Dr note_payable_cryptocurrency_cryptocurrency_blur_pool (principal)
                repayment_rows.append({
                    **repay_common,
                    'account_name': BlurAccounts.NOTE_PAYABLE,
                    'debit_crypto': principal,
                    'credit_crypto': Decimal(0),
                })

                # Dr interest_payable_cryptocurrency_blur_pool (interest)
                if interest > 0:
                    repayment_rows.append({
                        **repay_common,
                        'account_name': BlurAccounts.INTEREST_PAYABLE,
                        'debit_crypto': interest,
                        'credit_crypto': Decimal(0),
                    })

                # Cr digital_assets_blur_pool (total paid)
                repayment_rows.append({
                    **repay_common,
                    'account_name': "deemed_cash_usd", #BlurAccounts.DIGITAL_ASSETS_BLUR_POOL,
                    'debit_crypto': Decimal(0),
                    'credit_crypto': total,
                })

        repayment_df = pd.DataFrame(repayment_rows) if repayment_rows else pd.DataFrame()
        accrual_df = pd.DataFrame(accrual_rows) if accrual_rows else pd.DataFrame()

        return repayment_df, accrual_df

    # =========================================================================
    # REFINANCE
    # =========================================================================

    def generate_refinance_entries(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate journal entries for Refinance events.

        Returns:
            (refinance_entries_df, interest_accrual_entries_df)

        Old Lender perspective:
        1. Backdated daily interest accruals
        2. Payoff entry:
           - Dr digital_assets_blur_pool (debt received)
           - Cr loan_receivable_cryptocurrency_blur_pool (principal)
           - Cr interest_receivable_cryptocurrency_blur_pool (interest)

        New Lender perspective:
        1. New loan origination:
           - Dr loan_receivable_cryptocurrency_blur_pool (new amount)
           - Cr digital_assets_blur_pool (new amount)
        """
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        refinance_events = df[df['event'] == 'Refinance'].copy()
        if refinance_events.empty:
            return pd.DataFrame(), pd.DataFrame()

        refinance_rows = []
        accrual_rows = []

        for _, row in refinance_events.iterrows():
            old_lender = str(row.get('lender', '')).lower()
            new_lender = str(row.get('new_lender', '')).lower()
            borrower = str(row.get('borrower', '')).lower()
            function_name = row.get('function_name', '')

            is_old_lender_fund = self._is_fund_wallet(old_lender)
            is_new_lender_fund = self._is_fund_wallet(new_lender)
            is_borrower_fund = self._is_fund_wallet(borrower)

            if not is_old_lender_fund and not is_new_lender_fund and not is_borrower_fund:
                continue

            # Check for SELF-REFINANCE
            # OLD_LENDER == NEW_LENDER can mean two different things:
            # 1. refinanceAuction: Lender changing their own rate. No cash moves. SKIP.
            # 2. borrowerRefinance: Borrower paying down principal to SAME lender. Cash DOES move. PROCESS.
            is_same_lender = (old_lender == new_lender) and old_lender != ''
            is_borrower_initiated = function_name == 'borrowerRefinance'

            if is_same_lender and not is_borrower_initiated:
                # True self-refinance (lender-initiated rate change): No journal entries needed
                print(f"  ℹ Self-refinance detected (lien {row.get('lien_id')}): rate change only, no journal entries")
                continue

            # For borrowerRefinance where fund is both old AND new lender,
            # we handle it specially in generate_lender_borrower_refinance_entries
            if is_same_lender and is_borrower_initiated and is_old_lender_fund:
                # This will be handled by generate_lender_borrower_refinance_entries
                continue

            # Extract data
            principal = Decimal(str(row.get('principal', 0)))
            interest = Decimal(str(row.get('interest_earned', 0)))
            debt = principal + interest
            new_amount = Decimal(str(row.get('new_amount', 0)))

            start_time = row.get('start_time')
            refinance_timestamp = row.get('transaction_timestamp')
            refinance_dt = row.get('transaction_datetime')
            if isinstance(refinance_dt, str):
                refinance_dt = pd.to_datetime(refinance_dt, utc=True)

            rate_bips = row.get('rate_bips', 0)
            new_rate_bips = row.get('new_rate_bips', 0)
            # Annual interest rates as decimal
            annual_interest_rate = Decimal(str(rate_bips)) / Decimal(10000) if rate_bips else Decimal(0)
            new_annual_interest_rate = Decimal(str(new_rate_bips)) / Decimal(10000) if new_rate_bips else Decimal(0)

            # Common metadata (matching X2Y2 structure)
            common = {
                'cryptocurrency': 'BLUR_POOL',
                'platform': PLATFORM,
                'hash': row.get('transactionHash', ''),
                'loan_id': row.get('lien_id'),  # Standardized name
                'lien_id': row.get('lien_id'),
                'borrower': borrower,
                'contract_address': BLUR_BLEND_PROXY,
                'payable_currency': BLUR_POOL,
                'collateral_address': row.get('collection', ''),
                'token_id': row.get('token_id'),
                # USD pricing placeholders
                'eth_usd_price': None,
                'debit_USD': None,
                'credit_USD': None,
            }

            # OLD LENDER ENTRIES (receiving payoff)
            if is_old_lender_fund:
                old_lender_fund = self._get_fund_id(old_lender)

                old_lender_common = {
                    **common,
                    'fund_id': old_lender_fund,
                    'counterparty_fund_id': self._get_fund_id(new_lender),
                    'wallet_id': old_lender,
                    'lender': old_lender,
                    'from': new_lender,
                    'to': old_lender,
                    'principal_crypto': principal,
                    'principal_USD': None,
                    'interest_crypto': interest,
                    'interest_USD': None,
                    'annual_interest_rate': annual_interest_rate,
                    'payoff_amount_crypto': debt,
                    'payoff_amount_USD': None,
                    'loan_due_date': refinance_dt,  # Settled on this date
                    'rate_bips': rate_bips,
                    'start_time': start_time,
                }

                # Generate backdated interest accruals
                if interest > 0 and start_time and refinance_timestamp:
                    accruals = self._generate_daily_interest_accruals(
                        start_timestamp=start_time,
                        end_timestamp=refinance_timestamp,
                        principal_wei=int(principal * WAD),
                        rate_bips=rate_bips,
                        is_lender=True,
                        common_metadata={**old_lender_common, 'transaction_type': 'income_interest_accruals'},
                    )
                    accrual_rows.extend(accruals)

                # Payoff entry
                payoff_common = {
                    **old_lender_common,
                    'date': refinance_dt,
                    'event': 'Refinance',
                    'transaction_type': 'investments_lending',
                }

                # Dr digital_assets_blur_pool (debt received)
                refinance_rows.append({
                    **payoff_common,
                    'account_name': "deemed_cash_usd", #BlurAccounts.DIGITAL_ASSETS_BLUR_POOL,
                    'debit_crypto': debt,
                    'credit_crypto': Decimal(0),
                })

                # Cr loan_receivable_cryptocurrency_blur_pool (principal)
                refinance_rows.append({
                    **payoff_common,
                    'account_name': BlurAccounts.LOAN_RECEIVABLE,
                    'debit_crypto': Decimal(0),
                    'credit_crypto': principal,
                })

                # Cr interest_receivable_cryptocurrency_blur_pool (interest)
                if interest > 0:
                    refinance_rows.append({
                        **payoff_common,
                        'account_name': BlurAccounts.INTEREST_RECEIVABLE,
                        'debit_crypto': Decimal(0),
                        'credit_crypto': interest,
                    })

            # NEW LENDER ENTRIES
            # Only generate if:
            # 1. New lender is a fund wallet AND
            # 2. There's no corresponding LoanOfferTaken event for this lien_id
            #    (refinanceAuctionByOther emits both events, refinanceAuction only emits Refinance)
            if is_new_lender_fund:
                # Check if there's a LoanOfferTaken event for the same lien_id in this transaction
                tx_hash = row.get('transactionHash', '')
                lien_id = row.get('lien_id')

                # Look for matching LoanOfferTaken in the full dataframe
                has_loan_offer_taken = False
                if not df.empty and 'event' in df.columns:
                    matching_lot = df[
                        (df['event'] == 'LoanOfferTaken') &
                        (df['transactionHash'] == tx_hash) &
                        (df['lien_id'] == lien_id)
                    ]
                    has_loan_offer_taken = len(matching_lot) > 0

                if has_loan_offer_taken:
                    # LoanOfferTaken will handle origination - skip here to avoid duplicates
                    pass
                else:
                    # No LoanOfferTaken event - this is refinanceAuction, generate origination entries
                    new_lender_fund = self._get_fund_id(new_lender)

                    new_lender_common = {
                        **common,
                        'date': refinance_dt,
                        'event': 'Refinance',
                        'transaction_type': 'investments_lending',
                        'fund_id': new_lender_fund,
                        'counterparty_fund_id': self._get_fund_id(old_lender),
                        'wallet_id': new_lender,
                        'lender': new_lender,
                        'from': new_lender,
                        'to': borrower,
                        'principal_crypto': new_amount if new_amount > 0 else principal,
                        'principal_USD': None,
                        'interest_crypto': Decimal(0),
                        'interest_USD': None,
                        'annual_interest_rate': new_annual_interest_rate,
                        'payoff_amount_crypto': new_amount if new_amount > 0 else principal,
                        'payoff_amount_USD': None,
                        'loan_due_date': None,  # Callable loan - no fixed due date
                        'rate_bips': new_rate_bips,
                        'start_time': refinance_timestamp,  # New loan starts at refinance time
                    }

                    amount_to_record = new_amount if new_amount > 0 else principal

                    # Dr loan_receivable_cryptocurrency_blur_pool (new amount)
                    refinance_rows.append({
                        **new_lender_common,
                        'account_name': BlurAccounts.LOAN_RECEIVABLE,
                        'debit_crypto': amount_to_record,
                        'credit_crypto': Decimal(0),
                    })

                    # Cr digital_assets_blur_pool (new amount)
                    refinance_rows.append({
                        **new_lender_common,
                        'account_name': "deemed_cash_usd", #BlurAccounts.DIGITAL_ASSETS_BLUR_POOL,
                        'debit_crypto': Decimal(0),
                        'credit_crypto': amount_to_record,
                    })

        refinance_df = pd.DataFrame(refinance_rows) if refinance_rows else pd.DataFrame()
        accrual_df = pd.DataFrame(accrual_rows) if accrual_rows else pd.DataFrame()

        return refinance_df, accrual_df

    # =========================================================================
    # BORROWER REFINANCE (borrowerRefinance function)
    # =========================================================================

    def generate_borrower_refinance_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate journal entries for borrowerRefinance transactions.

        This handles the case where the FUND IS THE BORROWER and refinances
        their loan to a new lender, often paying down principal.

        Identifies borrowerRefinance by:
        - function_name == 'borrowerRefinance'
        - borrower is a fund wallet
        - Has Refinance event (and usually LoanOfferTaken for new loan)

        Borrower perspective (interest expense recognized at payoff):
        - DR note_payable_cryptocurrency_cryptocurrency_blur_pool (old principal - extinguish)
        - DR interest_expense_cryptocurrency_blur_pool (accrued interest)
            - CR note_payable_cryptocurrency_cryptocurrency_blur_pool (new principal - new liability)
            - CR digital_assets_blur_pool (borrower cash outflow)

        The balancing equation:
        old_principal + accrued_interest = new_principal + borrower_outflow

        Returns:
            DataFrame with journal entries
        """
        if df.empty:
            return pd.DataFrame()

        # Filter for borrowerRefinance transactions with Refinance events
        # The Refinance event has the old loan info from lien_data
        refinance_events = df[
            (df['event'] == 'Refinance') &
            (df['function_name'] == 'borrowerRefinance')
        ].copy()

        if refinance_events.empty:
            return pd.DataFrame()

        journal_rows = []

        for _, row in refinance_events.iterrows():
            borrower = str(row.get('borrower', '')).lower()

            if not self._is_fund_wallet(borrower):
                continue

            # Get lenders
            old_lender = str(row.get('old_lender', row.get('lender', ''))).lower()
            new_lender = str(row.get('new_lender', '')).lower()

            # Extract amounts
            old_principal = Decimal(str(row.get('principal', 0)))
            new_principal = Decimal(str(row.get('new_amount', 0)))

            # Calculate accrued interest from the actual pool transfer amounts
            # total_payoff = borrower_outflow + new_lender_contribution
            # interest = total_payoff - old_principal
            borrower_outflow = Decimal(str(row.get('borrower_outflow', 0)))
            actual_pool_transfer = Decimal(str(row.get('actual_pool_transfer', 0))) if row.get('actual_pool_transfer') else None

            # If we have actual_pool_transfer (total to old lender), use it
            if actual_pool_transfer and actual_pool_transfer > 0:
                total_payoff = actual_pool_transfer
            else:
                # Compute from components: new_principal sent by new lender + borrower outflow
                total_payoff = new_principal + borrower_outflow

            # Interest = total paid to old lender - old principal
            accrued_interest = total_payoff - old_principal
            if accrued_interest < 0:
                accrued_interest = Decimal(0)  # Should not happen but safety check

            # Get dates
            refinance_dt = row.get('transaction_datetime')
            if isinstance(refinance_dt, str):
                refinance_dt = pd.to_datetime(refinance_dt, utc=True)

            rate_bips = row.get('rate_bips', 0)
            new_rate_bips = row.get('new_rate_bips', 0)
            annual_interest_rate = Decimal(str(rate_bips)) / Decimal(10000) if rate_bips else Decimal(0)
            new_annual_interest_rate = Decimal(str(new_rate_bips)) / Decimal(10000) if new_rate_bips else Decimal(0)

            # Fund IDs
            borrower_fund = self._get_fund_id(borrower)
            old_lender_fund = self._get_fund_id(old_lender)
            new_lender_fund = self._get_fund_id(new_lender)

            # Common metadata
            common = {
                'date': refinance_dt,
                'fund_id': borrower_fund,
                'counterparty_fund_id': new_lender_fund,  # New lender is the new counterparty
                'wallet_id': borrower,
                'cryptocurrency': 'BLUR_POOL',
                'transaction_type': 'financing_borrowings',
                'platform': PLATFORM,
                'event': 'BorrowerRefinance',  # Special event name for clarity
                'hash': row.get('transactionHash', ''),
                'loan_id': row.get('lien_id'),
                'lien_id': row.get('lien_id'),
                'lender': new_lender,  # New lender is the current lender
                'old_lender': old_lender,
                'borrower': borrower,
                'from': borrower,
                'to': old_lender,  # Cash flows to old lender
                'contract_address': BLUR_BLEND_PROXY,
                'payable_currency': BLUR_POOL,
                'collateral_address': row.get('collection', ''),
                'token_id': row.get('token_id'),
                'principal_crypto': new_principal,
                'old_principal_crypto': old_principal,
                'principal_USD': None,
                'interest_expense_crypto': accrued_interest,
                'interest_expense_USD': None,
                'annual_interest_rate': new_annual_interest_rate,
                'old_annual_interest_rate': annual_interest_rate,
                'payoff_amount_crypto': total_payoff,
                'payoff_amount_USD': None,
                'borrower_outflow_crypto': borrower_outflow,
                'loan_due_date': None,  # Callable loan
                'rate_bips': new_rate_bips,
                'start_time': row.get('transaction_timestamp'),  # New loan starts now
                # USD pricing placeholders
                'eth_usd_price': None,
                'debit_USD': None,
                'credit_USD': None,
            }

            # ==========================================
            # JOURNAL ENTRIES (Borrower perspective)
            # ==========================================

            # DR note_payable_cryptocurrency_cryptocurrency_blur_pool (old principal - extinguish old liability)
            journal_rows.append({
                **common,
                'account_name': BlurAccounts.NOTE_PAYABLE,
                'debit_crypto': old_principal,
                'credit_crypto': Decimal(0),
                'description': f'Extinguish old loan liability (lien {row.get("lien_id")})',
            })

            # DR interest_expense_cryptocurrency_blur_pool (accrued interest)
            if accrued_interest > 0:
                journal_rows.append({
                    **common,
                    'account_name': BlurAccounts.INTEREST_EXPENSE,
                    'debit_crypto': accrued_interest,
                    'credit_crypto': Decimal(0),
                    'description': f'Interest expense on refinanced loan (lien {row.get("lien_id")})',
                })

            # CR note_payable_cryptocurrency_cryptocurrency_blur_pool (new principal - new liability)
            if new_principal > 0:
                journal_rows.append({
                    **common,
                    'account_name': BlurAccounts.NOTE_PAYABLE,
                    'debit_crypto': Decimal(0),
                    'credit_crypto': new_principal,
                    'description': f'New loan liability from refinance (lien {row.get("lien_id")})',
                })

            # CR digital_assets_blur_pool (borrower cash outflow)
            if borrower_outflow > 0:
                journal_rows.append({
                    **common,
                    'account_name': "deemed_cash_usd", #BlurAccounts.DIGITAL_ASSETS_BLUR_POOL,
                    'debit_crypto': Decimal(0),
                    'credit_crypto': borrower_outflow,
                    'description': f'Cash paid to refinance loan (lien {row.get("lien_id")})',
                })

            # Validation: debits should equal credits
            total_debits = old_principal + accrued_interest
            total_credits = new_principal + borrower_outflow
            if abs(total_debits - total_credits) > Decimal('0.000000001'):
                print(f"[!] Unbalanced borrower refinance entry for lien {row.get('lien_id')}:")
                print(f"   Debits: {total_debits} (old_principal={old_principal}, interest={accrued_interest})")
                print(f"   Credits: {total_credits} (new_principal={new_principal}, outflow={borrower_outflow})")

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    # =========================================================================
    # LENDER IN BORROWER-INITIATED REFINANCE (borrowerRefinance where fund is lender)
    # =========================================================================

    def generate_lender_borrower_refinance_entries(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate journal entries when FUND IS THE LENDER in a borrowerRefinance.

        This handles the case where:
        - Function is borrowerRefinance
        - Fund is BOTH old lender AND new lender (same lender continues)
        - Borrower pays down principal and/or interest

        Example:
        - Old principal: 121.4 ETH
        - New principal: 109.8 ETH
        - Principal paydown: 11.6 ETH
        - Accrued interest: X ETH
        - Cash received by lender: 11.6 + X ETH

        Lender perspective:
        1. Generate backdated interest accruals
        2. Settlement entry:
           - Dr digital_assets_blur_pool (cash received: paydown + interest)
           - Cr loan_receivable_cryptocurrency_blur_pool (principal reduction)
           - Cr interest_receivable_cryptocurrency_blur_pool (accrued interest)

        Returns:
            (settlement_entries_df, interest_accrual_entries_df)
        """
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # Filter for borrowerRefinance where fund is the lender (old == new)
        refinance_events = df[
            (df['event'] == 'Refinance') &
            (df['function_name'] == 'borrowerRefinance')
        ].copy()

        if refinance_events.empty:
            return pd.DataFrame(), pd.DataFrame()

        settlement_rows = []
        accrual_rows = []

        for _, row in refinance_events.iterrows():
            old_lender = str(row.get('lender', '')).lower()
            new_lender = str(row.get('new_lender', '')).lower()
            borrower = str(row.get('borrower', '')).lower()

            # Only process if fund is the lender AND old_lender == new_lender
            is_same_lender = (old_lender == new_lender) and old_lender != ''
            is_lender_fund = self._is_fund_wallet(old_lender)

            if not (is_same_lender and is_lender_fund):
                continue

            # Extract amounts
            old_principal = Decimal(str(row.get('principal', 0)))
            new_principal = Decimal(str(row.get('new_amount', 0)))

            # Principal change (positive = paydown, negative = increase)
            principal_change = old_principal - new_principal

            # Get ACTUAL cash received from borrower via pool transfers
            # This is the source of truth from the blockchain
            borrower_outflow_raw = row.get('borrower_outflow', 0)
            if borrower_outflow_raw is None or pd.isna(borrower_outflow_raw):
                borrower_outflow_raw = 0
            cash_received = Decimal(str(borrower_outflow_raw))

            # Derive interest from actual cash flow:
            # cash_received = principal_paydown + interest
            # interest = cash_received - principal_paydown
            if principal_change > 0:  # Paydown case
                interest = cash_received - principal_change
            else:  # Increase case or no change
                # If borrower is increasing loan, they might still pay accumulated interest
                interest = Decimal(str(row.get('interest_earned', 0)))

            # Sanity check: interest should be non-negative
            if interest < 0:
                # Something's off - fall back to computed interest
                interest = Decimal(str(row.get('interest_earned', 0)))
                print(f"  [!] Negative interest derived for lien {row.get('lien_id')}. Using computed interest: {interest}")

            # Get dates
            refinance_dt = row.get('transaction_datetime')
            if isinstance(refinance_dt, str):
                refinance_dt = pd.to_datetime(refinance_dt, utc=True)

            start_time = row.get('start_time')
            refinance_timestamp = row.get('transaction_timestamp')

            rate_bips = row.get('rate_bips', 0)
            new_rate_bips = row.get('new_rate_bips', 0)
            annual_interest_rate = Decimal(str(rate_bips)) / Decimal(10000) if rate_bips else Decimal(0)
            new_annual_interest_rate = Decimal(str(new_rate_bips)) / Decimal(10000) if new_rate_bips else Decimal(0)

            lender_fund = self._get_fund_id(old_lender)
            borrower_fund = self._get_fund_id(borrower)

            # Common metadata
            common = {
                'date': refinance_dt,
                'fund_id': lender_fund,
                'counterparty_fund_id': borrower_fund,
                'wallet_id': old_lender,
                'cryptocurrency': 'BLUR_POOL',
                'transaction_type': 'investments_lending',
                'platform': PLATFORM,
                'event': 'LenderBorrowerRefinance',  # Distinguishing event name
                'hash': row.get('transactionHash', ''),
                'loan_id': row.get('lien_id'),
                'lien_id': row.get('lien_id'),
                'lender': old_lender,
                'borrower': borrower,
                'from': borrower,
                'to': old_lender,
                'contract_address': BLUR_BLEND_PROXY,
                'payable_currency': BLUR_POOL,
                'collateral_address': row.get('collection', ''),
                'token_id': row.get('token_id'),
                'old_principal_crypto': old_principal,
                'new_principal_crypto': new_principal,
                'principal_change_crypto': principal_change,
                'interest_crypto': interest,
                'borrower_outflow_crypto': cash_received,
                'principal_USD': None,
                'interest_USD': None,
                'old_annual_interest_rate': annual_interest_rate,
                'new_annual_interest_rate': new_annual_interest_rate,
                'cash_received_crypto': cash_received,
                'loan_due_date': None,  # Callable loan
                'rate_bips': new_rate_bips,
                'start_time': refinance_timestamp,  # Loan continues from refinance time
                # USD pricing placeholders
                'eth_usd_price': None,
                'debit_USD': None,
                'credit_USD': None,
            }

            # Generate backdated interest accruals if there's interest
            if interest > 0 and start_time and refinance_timestamp:
                accruals = self._generate_daily_interest_accruals(
                    start_timestamp=start_time,
                    end_timestamp=refinance_timestamp,
                    principal_wei=int(old_principal * WAD),
                    rate_bips=rate_bips,
                    is_lender=True,
                    common_metadata={**common, 'transaction_type': 'income_interest_accruals'},
                )
                accrual_rows.extend(accruals)

            # ==========================================
            # JOURNAL ENTRIES (Lender perspective)
            # ==========================================

            # Handle principal PAYDOWN case (borrower reduces loan)
            if principal_change > 0 and cash_received > 0:
                # Dr digital_assets_blur_pool (cash received from borrower)
                settlement_rows.append({
                    **common,
                    'account_name': "deemed_cash_usd", #BlurAccounts.DIGITAL_ASSETS_BLUR_POOL,
                    'debit_crypto': cash_received,
                    'credit_crypto': Decimal(0),
                    'description': f'Cash received from borrower refinance (lien {row.get("lien_id")})',
                })

                # Cr loan_receivable_cryptocurrency_blur_pool (principal reduction)
                settlement_rows.append({
                    **common,
                    'account_name': BlurAccounts.LOAN_RECEIVABLE,
                    'debit_crypto': Decimal(0),
                    'credit_crypto': principal_change,
                    'description': f'Principal paydown from borrower refinance (lien {row.get("lien_id")})',
                })

                # Cr interest_receivable_cryptocurrency_blur_pool (accrued interest)
                if interest > 0:
                    settlement_rows.append({
                        **common,
                        'account_name': BlurAccounts.INTEREST_RECEIVABLE,
                        'debit_crypto': Decimal(0),
                        'credit_crypto': interest,
                        'description': f'Interest received from borrower refinance (lien {row.get("lien_id")})',
                    })

            elif principal_change <= 0 and cash_received > 0:
                # Interest-only payment OR loan increase with interest payment
                # Cash received but no principal reduction

                # Dr digital_assets_blur_pool (cash received)
                settlement_rows.append({
                    **common,
                    'account_name': "deemed_cash_usd", #BlurAccounts.DIGITAL_ASSETS_BLUR_POOL,
                    'debit_crypto': cash_received,
                    'credit_crypto': Decimal(0),
                    'description': f'Interest received from borrower refinance (lien {row.get("lien_id")})',
                })

                # If there's also a principal increase (borrower getting more money)
                principal_increase = abs(principal_change) if principal_change < 0 else Decimal(0)
                if principal_increase > 0:
                    settlement_rows.append({
                        **common,
                        'account_name': BlurAccounts.LOAN_RECEIVABLE,
                        'debit_crypto': principal_increase,
                        'credit_crypto': Decimal(0),
                        'description': f'Principal increase from borrower refinance (lien {row.get("lien_id")})',
                    })

                # Cr interest_receivable (the cash received minus any additional principal)
                interest_received = cash_received + principal_increase
                if interest_received > 0:
                    settlement_rows.append({
                        **common,
                        'account_name': BlurAccounts.INTEREST_RECEIVABLE,
                        'debit_crypto': Decimal(0),
                        'credit_crypto': interest_received,
                        'description': f'Interest received from borrower refinance (lien {row.get("lien_id")})',
                    })

            elif cash_received < 0:
                # Borrower is INCREASING loan - lender sends cash OUT
                cash_sent = abs(cash_received)
                principal_increase = new_principal - old_principal

                # Dr loan_receivable (principal increase)
                settlement_rows.append({
                    **common,
                    'account_name': BlurAccounts.LOAN_RECEIVABLE,
                    'debit_crypto': principal_increase,
                    'credit_crypto': Decimal(0),
                    'description': f'Principal increase from borrower refinance (lien {row.get("lien_id")})',
                })

                # Cr interest_receivable (if any - unlikely in increase scenario)
                if interest > 0:
                    settlement_rows.append({
                        **common,
                        'account_name': BlurAccounts.INTEREST_RECEIVABLE,
                        'debit_crypto': Decimal(0),
                        'credit_crypto': interest,
                        'description': f'Interest cleared in borrower refinance (lien {row.get("lien_id")})',
                    })

                # The net cash flow - borrower receives more, so:
                # Dr interest (already accrued) + Dr additional loan = Cr cash sent
                # This is handled by netting
                net_cash_sent = principal_increase - interest
                if net_cash_sent > 0:
                    settlement_rows.append({
                        **common,
                        'account_name': "deemed_cash_usd", #BlurAccounts.DIGITAL_ASSETS_BLUR_POOL,
                        'debit_crypto': Decimal(0),
                        'credit_crypto': net_cash_sent,
                        'description': f'Net cash sent for borrower refinance increase (lien {row.get("lien_id")})',
                    })

            # Validation
            total_debits = sum(Decimal(str(r['debit_crypto'])) for r in settlement_rows if r.get('lien_id') == row.get('lien_id'))
            total_credits = sum(Decimal(str(r['credit_crypto'])) for r in settlement_rows if r.get('lien_id') == row.get('lien_id'))
            if abs(total_debits - total_credits) > Decimal('0.000000001'):
                print(f"[!] Unbalanced lender borrower refinance entry for lien {row.get('lien_id')}:")
                print(f"   Debits: {total_debits}")
                print(f"   Credits: {total_credits}")

        settlement_df = pd.DataFrame(settlement_rows) if settlement_rows else pd.DataFrame()
        accrual_df = pd.DataFrame(accrual_rows) if accrual_rows else pd.DataFrame()

        return settlement_df, accrual_df

    # =========================================================================
    # SEIZE (Foreclosure)
    # =========================================================================

    def generate_seize_entries(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate journal entries for Seize events (lender forecloses on NFT).

        Returns:
            (seize_entries_df, interest_accrual_entries_df)

        Lender perspective:
        1. Backdated daily interest accruals (which become bad debt)
        2. Foreclosure entry:
           - Dr investments_nfts_seized_collateral (principal - valued at loan amount)
           - Dr bad_debt_expense_cryptocurrency_blur_pool (interest - written off)
           - Cr loan_receivable_cryptocurrency_blur_pool (principal)
           - Cr interest_receivable_cryptocurrency_blur_pool (interest)
        """
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        seize_events = df[df['event'] == 'Seize'].copy()
        if seize_events.empty:
            return pd.DataFrame(), pd.DataFrame()

        seize_rows = []
        accrual_rows = []

        for _, row in seize_events.iterrows():
            lender = str(row.get('lender', '')).lower()
            borrower = str(row.get('borrower', '')).lower()

            is_lender_fund = self._is_fund_wallet(lender)

            if not is_lender_fund:
                continue

            # Extract data
            principal = Decimal(str(row.get('principal', 0)))

            # For seize, calculate interest as of seize time
            start_time = row.get('start_time')
            seize_timestamp = row.get('transaction_timestamp')
            seize_dt = row.get('transaction_datetime')
            if isinstance(seize_dt, str):
                seize_dt = pd.to_datetime(seize_dt, utc=True)

            rate_bips = row.get('rate_bips', 0)

            # Calculate interest accrued
            interest = Decimal(str(row.get('interest_earned', 0)))
            if interest == 0 and start_time and seize_timestamp and rate_bips:
                # Recalculate if not provided
                lien = LienData(
                    lender=lender,
                    borrower=borrower,
                    collection=row.get('collection', ''),
                    token_id=row.get('token_id', 0),
                    amount_wei=int(principal * WAD),
                    start_time=start_time,
                    rate_bips=rate_bips,
                    auction_start_block=0,
                    auction_duration=0,
                )
                interest = lien.compute_interest_at(seize_timestamp)

            # Lender entries
            lender_fund = self._get_fund_id(lender)

            # Annual interest rate as decimal
            annual_interest_rate = Decimal(str(rate_bips)) / Decimal(10000) if rate_bips else Decimal(0)

            common = {
                'fund_id': lender_fund,
                'counterparty_fund_id': self._get_fund_id(borrower),
                'wallet_id': lender,
                'cryptocurrency': 'BLUR_POOL',
                'platform': PLATFORM,
                'hash': row.get('transactionHash', ''),
                'loan_id': row.get('lien_id'),  # Standardized name
                'lien_id': row.get('lien_id'),
                'lender': lender,
                'borrower': borrower,
                'from': borrower,
                'to': lender,
                'contract_address': BLUR_BLEND_PROXY,
                'payable_currency': BLUR_POOL,
                'collateral_address': row.get('collection', ''),
                'token_id': row.get('token_id'),
                'principal_crypto': principal,
                'principal_USD': None,
                'interest_crypto': interest,
                'interest_USD': None,
                'annual_interest_rate': annual_interest_rate,
                'payoff_amount_crypto': principal + interest,  # Total owed at foreclosure
                'payoff_amount_USD': None,
                'loan_due_date': seize_dt,  # Foreclosed on this date
                'rate_bips': rate_bips,
                'start_time': start_time,
                # USD pricing placeholders
                'eth_usd_price': None,
                'debit_USD': None,
                'credit_USD': None,
            }

            # Generate backdated interest accruals (will become bad debt)
            if interest > 0 and start_time and seize_timestamp:
                accruals = self._generate_daily_interest_accruals(
                    start_timestamp=start_time,
                    end_timestamp=seize_timestamp,
                    principal_wei=int(principal * WAD),
                    rate_bips=rate_bips,
                    is_lender=True,
                    common_metadata={**common, 'transaction_type': 'income_interest_accruals'},
                )
                accrual_rows.extend(accruals)

            # Foreclosure entry
            seize_common = {
                **common,
                'date': seize_dt,
                'event': 'Seize',
                'transaction_type': 'investments_foreclosures',
            }

            # Dr investments_nfts_seized_collateral (principal)
            seize_rows.append({
                **seize_common,
                'account_name': BlurAccounts.INVESTMENTS_NFTS_SEIZED,
                'debit_crypto': principal,
                'credit_crypto': Decimal(0),
            })

            # Dr bad_debt_expense_cryptocurrency_blur_pool (interest written off)
            if interest > 0:
                seize_rows.append({
                    **seize_common,
                    'account_name': BlurAccounts.BAD_DEBT_EXPENSE,
                    'debit_crypto': interest,
                    'credit_crypto': Decimal(0),
                })

            # Cr loan_receivable_cryptocurrency_blur_pool (principal)
            seize_rows.append({
                **seize_common,
                'account_name': BlurAccounts.LOAN_RECEIVABLE,
                'debit_crypto': Decimal(0),
                'credit_crypto': principal,
            })

            # Cr interest_receivable_cryptocurrency_blur_pool (interest)
            if interest > 0:
                seize_rows.append({
                    **seize_common,
                    'account_name': BlurAccounts.INTEREST_RECEIVABLE,
                    'debit_crypto': Decimal(0),
                    'credit_crypto': interest,
                })

        seize_df = pd.DataFrame(seize_rows) if seize_rows else pd.DataFrame()
        accrual_df = pd.DataFrame(accrual_rows) if accrual_rows else pd.DataFrame()

        return seize_df, accrual_df

    # =========================================================================
    # BUY LOCKED (NFT Purchase)
    # =========================================================================

    def generate_buy_locked_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate journal entries for BuyLocked events (fund purchases NFT from locked position).

        When a third party buys an NFT that's locked as collateral:
        1. Buyer pays the purchase price
        2. Lender receives debt repayment (principal + interest)
        3. Seller (borrower) receives the difference (price - debt)
        4. Buyer receives the NFT

        From BUYER (fund) perspective:
        - Dr investments_nfts (purchase price)
        - Cr digital_assets_blur_pool (cash paid)

        Note: Interest accruals are NOT generated for the buyer since they
        were never the lender. The lender's interest accruals would be handled
        separately if the lender is also a fund wallet.
        """
        if df.empty:
            return pd.DataFrame()

        buy_locked_events = df[df['event'] == 'BuyLocked'].copy()
        if buy_locked_events.empty:
            return pd.DataFrame()

        journal_rows = []

        for _, row in buy_locked_events.iterrows():
            buyer = str(row.get('buyer', '')).lower()
            seller = str(row.get('seller', '')).lower()
            lender = str(row.get('lender', '')).lower()

            # Only process if buyer is a fund wallet
            is_buyer_fund = self._is_fund_wallet(buyer)

            if not is_buyer_fund:
                continue

            # Extract purchase price
            purchase_price_wei = row.get('purchase_price_wei', 0)
            if not purchase_price_wei:
                # Try to get from actual_pool_transfer_wei as fallback
                purchase_price_wei = row.get('actual_pool_transfer_wei', 0)

            if not purchase_price_wei:
                print(f"[!] No purchase price for BuyLocked tx: {row.get('transactionHash', '')[:10]}...")
                continue

            purchase_price = Decimal(str(purchase_price_wei)) / WAD

            # Get transaction datetime
            buy_dt = row.get('transaction_datetime')
            if isinstance(buy_dt, str):
                buy_dt = pd.to_datetime(buy_dt, utc=True)

            # Get fund IDs
            buyer_fund = self._get_fund_id(buyer)
            seller_fund = self._get_fund_id(seller)

            # Collection and token info
            collection = row.get('collection', '')
            token_id = row.get('token_id')

            # Common metadata
            common = {
                'date': buy_dt,
                'fund_id': buyer_fund,
                'counterparty_fund_id': seller_fund,
                'wallet_id': buyer,
                'cryptocurrency': 'BLUR_POOL',
                'transaction_type': 'investments_nft_purchase',
                'platform': PLATFORM,
                'event': 'BuyLocked',
                'hash': row.get('transactionHash', ''),
                'loan_id': row.get('lien_id'),
                'lien_id': row.get('lien_id'),
                'lender': lender,
                'borrower': seller,  # Seller is the original borrower
                'buyer': buyer,
                'seller': seller,
                'from': buyer,
                'to': seller,
                'contract_address': BLUR_BLEND_PROXY,
                'payable_currency': BLUR_POOL,
                'collateral_address': collection,
                'token_id': token_id,
                'purchase_price_crypto': purchase_price,
                'purchase_price_USD': None,
                'principal_crypto': row.get('principal'),  # Original loan amount
                'principal_USD': None,
                'debt_crypto': row.get('debt_at_event'),  # What lender received
                'debt_USD': None,
                # USD pricing placeholders
                'eth_usd_price': None,
                'debit_USD': None,
                'credit_USD': None,
            }

            # Dr investments_nfts (purchase price)
            journal_rows.append({
                **common,
                'account_name': BlurAccounts.INVESTMENTS_NFTS,
                'debit_crypto': purchase_price,
                'credit_crypto': Decimal(0),
            })

            # Cr digital_assets_blur_pool (cash paid)
            journal_rows.append({
                **common,
                'account_name': "deemed_cash_usd", #BlurAccounts.DIGITAL_ASSETS_BLUR_POOL,
                'debit_crypto': Decimal(0),
                'credit_crypto': purchase_price,
            })

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    # =========================================================================
    # DAILY INTEREST ACCRUALS (Retrospective)
    # =========================================================================

    def _generate_daily_interest_accruals(
        self,
        start_timestamp: int,
        end_timestamp: int,
        principal_wei: int,
        rate_bips: int,
        is_lender: bool,
        common_metadata: Dict,
    ) -> List[Dict]:
        """
        Generate daily interest accrual entries from start to end.

        Uses Wei-precise allocation to avoid rounding errors.
        Accruals are backdated to each day of the loan.

        Args:
            start_timestamp: Loan start time (Unix)
            end_timestamp: Repayment/refinance time (Unix)
            principal_wei: Principal in wei
            rate_bips: Annual rate in basis points
            is_lender: True for income accruals, False for expense
            common_metadata: Common fields for entries

        Returns:
            List of journal entry dicts
        """
        if start_timestamp >= end_timestamp or principal_wei <= 0 or rate_bips <= 0:
            return []

        entries = []

        # Calculate total interest
        lien = LienData(
            lender='',
            borrower='',
            collection='',
            token_id=0,
            amount_wei=principal_wei,
            start_time=start_timestamp,
            rate_bips=rate_bips,
            auction_start_block=0,
            auction_duration=0,
        )
        total_interest = lien.compute_interest_at(end_timestamp)
        total_interest_wei = int(total_interest * WAD)

        if total_interest_wei <= 0:
            return []

        # Calculate total seconds
        total_secs = end_timestamp - start_timestamp

        # Generate daily slices with Wei precision
        start_dt = datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_timestamp, tz=timezone.utc)

        leftover = 0
        assigned_so_far = 0
        cursor = start_dt

        while cursor < end_dt:
            # Calculate next midnight
            tomorrow = cursor.date() + timedelta(days=1)
            naive_midnight = datetime.combine(tomorrow, dt_time(0, 0, 0))
            next_midnight = naive_midnight.replace(tzinfo=timezone.utc)

            # Slice ends at midnight or loan end
            segment_end = min(next_midnight, end_dt)
            slice_secs = int((segment_end - cursor).total_seconds())

            # Wei-precise interest allocation
            numer = (total_interest_wei * slice_secs) + leftover
            slice_interest_wei = numer // total_secs
            leftover = numer % total_secs
            assigned_so_far += slice_interest_wei

            # Label timestamp (end of day or loan end)
            if segment_end == next_midnight:
                accrual_label = next_midnight - timedelta(seconds=1)
            else:
                accrual_label = end_dt

            # Convert from wei to crypto
            slice_interest = Decimal(slice_interest_wei) / WAD

            if slice_interest > 0:
                if is_lender:
                    # Dr interest_receivable_cryptocurrency_blur_pool
                    entries.append({
                        **common_metadata,
                        'date': accrual_label,
                        'event': 'InterestAccrual',
                        'account_name': BlurAccounts.INTEREST_RECEIVABLE,
                        'debit_crypto': slice_interest,
                        'credit_crypto': Decimal(0),
                    })

                    # Cr interest_income_cryptocurrency_blur_pool
                    entries.append({
                        **common_metadata,
                        'date': accrual_label,
                        'event': 'InterestAccrual',
                        'account_name': BlurAccounts.INTEREST_INCOME,
                        'debit_crypto': Decimal(0),
                        'credit_crypto': slice_interest,
                    })
                else:
                    # Dr interest_expense_cryptocurrency_blur_pool
                    entries.append({
                        **common_metadata,
                        'date': accrual_label,
                        'event': 'InterestAccrual',
                        'account_name': BlurAccounts.INTEREST_EXPENSE,
                        'debit_crypto': slice_interest,
                        'credit_crypto': Decimal(0),
                    })

                    # Cr interest_payable_cryptocurrency_blur_pool
                    entries.append({
                        **common_metadata,
                        'date': accrual_label,
                        'event': 'InterestAccrual',
                        'account_name': BlurAccounts.INTEREST_PAYABLE,
                        'debit_crypto': Decimal(0),
                        'credit_crypto': slice_interest,
                    })

            cursor = segment_end

        # Final adjustment for any remaining wei
        if assigned_so_far < total_interest_wei and entries:
            shortfall_wei = total_interest_wei - assigned_so_far
            shortfall = Decimal(shortfall_wei) / WAD
            # Add to last entries (debit and credit)
            entries[-2]['debit_crypto'] += shortfall
            entries[-1]['credit_crypto'] += shortfall

        return entries

#### NFT
    def generate_nft_sale_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate journal entries for NFT sales (takeBidSingle, takeBid).

        Fund SELLS NFT, receives proceeds.

        Journal Entry:
            Dr deemed_cash_usd           [proceeds]
            Cr investments_nfts          [cost_basis]
            Dr/Cr realized_gain_loss_nfts [gain or loss]

        NOTE: Cost basis lookup required for proper gain/loss calculation.
              If not available, cost_basis = 0 and gain = proceeds.
        """
        if df.empty:
            return pd.DataFrame()

        # Filter for NFT_SELL events
        sell_events = df[df['event'] == 'NFT_SELL'].copy()
        if sell_events.empty:
            return pd.DataFrame()

        journal_rows = []

        for idx, row in sell_events.iterrows():
            seller = str(row.get('seller', '')).lower()
            buyer = str(row.get('buyer', '')).lower()

            # Skip if seller not a fund wallet
            if seller not in self.fund_wallet_list:
                continue

            # Get proceeds
            proceeds_raw = row.get('price_eth', 0)
            proceeds = Decimal(str(proceeds_raw)) if pd.notna(proceeds_raw) else Decimal(0)

            if proceeds <= 0:
                tx_hash = row.get('transactionHash', 'unknown')[:16]
                print(f"[!] Skipping NFT sale {tx_hash}... - no proceeds found")
                continue

            # Get cost basis (if available from external source)
            cost_basis_raw = row.get('cost_basis', 0)
            cost_basis = Decimal(str(cost_basis_raw)) if pd.notna(cost_basis_raw) else Decimal(0)
            cost_basis_missing = cost_basis == 0

            # Calculate gain/loss
            gain_loss = proceeds - cost_basis

            # Get fund info
            seller_fund = self._get_fund_id(seller)
            buyer_fund = self._get_fund_id(buyer)

            tx_dt = row.get('transaction_datetime')
            if isinstance(tx_dt, str):
                tx_dt = pd.to_datetime(tx_dt, utc=True)

            # Common metadata
            common = {
                'date': tx_dt,
                'fund_id': seller_fund,
                'counterparty_fund_id': buyer_fund,
                'wallet_id': seller,
                'cryptocurrency': 'BLUR_POOL',
                'transaction_type': 'investments_nft_sale',
                'platform': 'Blur',
                'event': 'NFT_SELL',
                'hash': row.get('transactionHash', ''),
                'function_name': row.get('function_name', ''),
                'buyer': buyer,
                'seller': seller,
                'from': seller,
                'to': buyer,
                'collection': row.get('collection', ''),
                'token_id': row.get('token_id'),
                'sale_proceeds_crypto': proceeds,
                'cost_basis_crypto': cost_basis,
                'gain_loss_crypto': gain_loss,
                'cost_basis_missing': cost_basis_missing,
            }

            # Dr deemed_cash_usd (proceeds received)
            journal_rows.append({
                **common,
                'account_name': 'deemed_cash_usd',
                'debit_crypto': proceeds,
                'credit_crypto': Decimal(0),
            })

            # Cr investments_nfts (cost basis removed)
            journal_rows.append({
                **common,
                'account_name': 'investments_nfts',
                'debit_crypto': Decimal(0),
                'credit_crypto': cost_basis,
            })

            # Gain/Loss entry (to balance)
            if gain_loss > 0:
                # GAIN: Credit realized_gain_loss_nfts
                journal_rows.append({
                    **common,
                    'account_name': 'realized_gain_loss_nfts',
                    'debit_crypto': Decimal(0),
                    'credit_crypto': gain_loss,
                })
            elif gain_loss < 0:
                # LOSS: Debit realized_gain_loss_nfts
                journal_rows.append({
                    **common,
                    'account_name': 'realized_gain_loss_nfts',
                    'debit_crypto': abs(gain_loss),
                    'credit_crypto': Decimal(0),
                })

        if not journal_rows:
            return pd.DataFrame()

        df_journal = pd.DataFrame(journal_rows)
        print(f"[OK] Generated {len(df_journal)} NFT sale journal entries")

        # Check for missing cost basis
        if 'cost_basis_missing' in df_journal.columns:
            missing_count = df_journal[df_journal['cost_basis_missing'] == True]['hash'].nunique()
            if missing_count > 0:
                print(f"[!] {missing_count} sales missing cost basis (gain = full proceeds)")

        return df_journal


    def generate_nft_purchase_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate journal entries for NFT purchases (takeAskSinglePool).

        Fund BUYS NFT, pays from Blur Pool balance.

        Journal Entry:
            Dr investments_nfts          [purchase_price]
            Cr digital_assets_blur_pool  [purchase_price]

        NOTE: This is similar to buyLocked but for pure marketplace purchases
              (no loan involved).
        """
        if df.empty:
            return pd.DataFrame()

        # Filter for NFT_BUY events
        buy_events = df[df['event'] == 'NFT_BUY'].copy()
        if buy_events.empty:
            return pd.DataFrame()

        journal_rows = []

        for idx, row in buy_events.iterrows():
            buyer = str(row.get('buyer', '')).lower()
            seller = str(row.get('seller', '')).lower()

            # Skip if buyer not a fund wallet
            if buyer not in self.fund_wallet_list:
                continue

            # Get purchase price
            price_raw = row.get('price_eth', 0)
            price = Decimal(str(price_raw)) if pd.notna(price_raw) else Decimal(0)

            if price <= 0:
                tx_hash = row.get('transactionHash', 'unknown')[:16]
                print(f"[!] Skipping NFT purchase {tx_hash}... - no price found")
                continue

            # Get fund info
            buyer_fund = self._get_fund_id(buyer)
            seller_fund = self._get_fund_id(seller)

            tx_dt = row.get('transaction_datetime')
            if isinstance(tx_dt, str):
                tx_dt = pd.to_datetime(tx_dt, utc=True)

            # Common metadata
            common = {
                'date': tx_dt,
                'fund_id': buyer_fund,
                'counterparty_fund_id': seller_fund,
                'wallet_id': buyer,
                'cryptocurrency': 'BLUR_POOL',
                'transaction_type': 'investments_nft_purchase',
                'platform': 'Blur',
                'event': 'NFT_BUY',
                'hash': row.get('transactionHash', ''),
                'function_name': row.get('function_name', ''),
                'buyer': buyer,
                'seller': seller,
                'from': buyer,
                'to': seller,
                'collection': row.get('collection', ''),
                'token_id': row.get('token_id'),
                'purchase_price_crypto': price,
            }

            # Dr investments_nfts (NFT acquired)
            journal_rows.append({
                **common,
                'account_name': 'investments_nfts',
                'debit_crypto': price,
                'credit_crypto': Decimal(0),
            })

            # Cr digital_assets_blur_pool (payment from pool)
            journal_rows.append({
                **common,
                'account_name': 'digital_assets_blur_pool',
                'debit_crypto': Decimal(0),
                'credit_crypto': price,
            })

        if not journal_rows:
            return pd.DataFrame()

        df_journal = pd.DataFrame(journal_rows)
        print(f"[OK] Generated {len(df_journal)} NFT purchase journal entries")

        return df_journal

#### NFT


# ============================================================================
# MAIN PROCESSOR
# ============================================================================

class BlurLendingProcessor:
    """
    Main processor for Blur Blend (NFT lending) transactions.

    Usage:
        processor = BlurLendingProcessor(w3, wallet_metadata, paths)

        # Process transactions
        results = processor.process_transactions(tx_hashes)
    """

    def __init__(
        self,
        w3: Web3,
        wallet_metadata: Dict[str, Dict],
        blend_contract,
        pool_contract,
        output_path: str = None,
        debug: bool = False,
    ):
        """
        Initialize processor.

        Args:
            w3: Web3 instance
            wallet_metadata: Dict mapping addresses to wallet info
            blend_contract: Instantiated Blend contract
            pool_contract: Instantiated Blur Pool contract
            output_path: Path to save outputs
            debug: Enable verbose debug output
        """
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        self.fund_wallet_list = [
            addr.lower() for addr, info in wallet_metadata.items()
            if info.get('category', '').lower() == 'fund'
            ]
        #self.fund_wallet_list = list(self.wallet_metadata.keys())
        self.blend_contract = blend_contract
        self.pool_contract = pool_contract
        self.output_path = output_path
        self.debug = debug

        # Initialize decoder and journal generator
        self.decoder = BlurEventDecoder(
            w3=w3,
            blend_contract=blend_contract,
            pool_contract=pool_contract,
            wallet_metadata=wallet_metadata,
            debug=debug,
        )

        self.journal_generator = BlurJournalEntryGenerator(
            wallet_metadata=wallet_metadata,
        )

    def process_transactions(
        self,
        tx_hashes: List[str],
        max_workers: int = 8,
    ) -> Dict[str, pd.DataFrame]:
        """
        Process Blur lending transactions end-to-end.

        Args:
            tx_hashes: List of transaction hashes
            max_workers: Parallel workers for decoding

        Returns:
            Dict with DataFrames:
            - 'decoded_events': All decoded events
            - 'journal_originations': Loan origination entries
            - 'journal_repayments': Loan repayment entries
            - 'journal_refinances': Refinance entries
            - 'journal_seizes': Foreclosure entries
            - 'interest_accruals': All interest accrual entries
            - 'all_journal_entries': Combined journal entries
        """
        print(f"\n{'='*80}")
        print(f"🔵 BLUR BLEND LENDING PROCESSOR")
        print(f"{'='*80}")
        print(f"Processing {len(tx_hashes)} transactions...")

        # Step 1: Decode all events
        df_events = self.decoder.decode_batch(
            tx_hashes=tx_hashes,
            max_workers=max_workers,
            filter_fund_wallets=True,
        )

        if df_events.empty:
            print("[!] No fund wallet events found")
            return {
                'decoded_events': pd.DataFrame(),
                'journal_originations': pd.DataFrame(),
                'journal_repayments': pd.DataFrame(),
                'journal_refinances': pd.DataFrame(),
                'journal_borrower_refinances': pd.DataFrame(),
                'journal_lender_borrower_refinances': pd.DataFrame(),
                'journal_seizes': pd.DataFrame(),
                'journal_buy_locked': pd.DataFrame(),
                'interest_accruals': pd.DataFrame(),
                'all_journal_entries': pd.DataFrame(),
            }

        print(f"[OK] Decoded {len(df_events)} events for fund wallets")

        # Event summary
        event_counts = df_events['event'].value_counts()
        for event, count in event_counts.items():
            print(f"   - {event}: {count}")

        # Step 2: Generate journal entries
        all_accruals = []
        all_journals = []

        # Originations
        journal_originations = self.journal_generator.generate_loan_origination_entries(df_events)
        if not journal_originations.empty:
            all_journals.append(journal_originations)
            print(f"[OK] Generated {len(journal_originations)} origination entries")

        # Repayments
        journal_repayments, accruals_repay = self.journal_generator.generate_loan_repayment_entries(df_events)
        if not journal_repayments.empty:
            all_journals.append(journal_repayments)
            print(f"[OK] Generated {len(journal_repayments)} repayment entries")
        if not accruals_repay.empty:
            all_accruals.append(accruals_repay)
            print(f"[OK] Generated {len(accruals_repay)} interest accrual entries (from repayments)")

        # Refinances
        journal_refinances, accruals_refi = self.journal_generator.generate_refinance_entries(df_events)
        if not journal_refinances.empty:
            all_journals.append(journal_refinances)
            print(f"[OK] Generated {len(journal_refinances)} refinance entries")
        if not accruals_refi.empty:
            all_accruals.append(accruals_refi)
            print(f"[OK] Generated {len(accruals_refi)} interest accrual entries (from refinances)")

        # Borrower Refinances (fund as borrower refinancing their loan)
        journal_borrower_refinances = self.journal_generator.generate_borrower_refinance_entries(df_events)
        if not journal_borrower_refinances.empty:
            all_journals.append(journal_borrower_refinances)
            print(f"[OK] Generated {len(journal_borrower_refinances)} borrower refinance entries")

        # Lender in BorrowerRefinance (fund as lender receiving paydown from borrower)
        journal_lender_borrower_refi, accruals_lender_borrower_refi = self.journal_generator.generate_lender_borrower_refinance_entries(df_events)
        if not journal_lender_borrower_refi.empty:
            all_journals.append(journal_lender_borrower_refi)
            print(f"[OK] Generated {len(journal_lender_borrower_refi)} lender-borrower refinance entries (fund as lender)")
        if not accruals_lender_borrower_refi.empty:
            all_accruals.append(accruals_lender_borrower_refi)
            print(f"[OK] Generated {len(accruals_lender_borrower_refi)} interest accrual entries (from lender-borrower refinances)")

        # Seizes
        journal_seizes, accruals_seize = self.journal_generator.generate_seize_entries(df_events)
        if not journal_seizes.empty:
            all_journals.append(journal_seizes)
            print(f"[OK] Generated {len(journal_seizes)} seize/foreclosure entries")
        if not accruals_seize.empty:
            all_accruals.append(accruals_seize)
            print(f"[OK] Generated {len(accruals_seize)} interest accrual entries (from seizes)")

        # BuyLocked (NFT purchases)
        journal_buy_locked = self.journal_generator.generate_buy_locked_entries(df_events)
        if not journal_buy_locked.empty:
            all_journals.append(journal_buy_locked)
            print(f"[OK] Generated {len(journal_buy_locked)} NFT purchase entries (BuyLocked)")

        # Combine all
        interest_accruals = pd.concat(all_accruals, ignore_index=True) if all_accruals else pd.DataFrame()
        all_journal_entries = pd.concat(all_journals + all_accruals, ignore_index=True) if (all_journals or all_accruals) else pd.DataFrame()

        # Sort by date
        if not all_journal_entries.empty and 'date' in all_journal_entries.columns:
            all_journal_entries = all_journal_entries.sort_values('date')

        print(f"\n{'='*80}")
        print(f"📊 PROCESSING COMPLETE")
        print(f"{'='*80}")
        print(f"Total journal entries: {len(all_journal_entries)}")
        print(f"Total interest accruals: {len(interest_accruals)}")

        results = {
            'decoded_events': df_events,
            'journal_originations': journal_originations,
            'journal_repayments': journal_repayments,
            'journal_refinances': journal_refinances,
            'journal_borrower_refinances': journal_borrower_refinances,
            'journal_lender_borrower_refinances': journal_lender_borrower_refi,
            'journal_seizes': journal_seizes,
            'journal_buy_locked': journal_buy_locked,
            'interest_accruals': interest_accruals,
            'all_journal_entries': all_journal_entries,
        }

        # Save if output path provided
        if self.output_path:
            self._save_results(results)

        return results

    def _save_results(self, results: Dict[str, pd.DataFrame]):
        """Save results to CSV files"""
        output_dir = Path(self.output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        for name, df in results.items():
            if not df.empty:
                filepath = output_dir / f"blur_{name}.csv"
                df.to_csv(filepath, index=False)
                print(f"💾 Saved: {filepath}")

    def get_standard_column_order(self) -> List[str]:
        """Get standard column order for journal entries (matching X2Y2 structure)"""
        return [
            # Core fields
            "date",
            "transaction_type",
            "platform",
            "fund_id",
            "counterparty_fund_id",
            "wallet_id",
            "cryptocurrency",
            "account_name",
            "debit_crypto",
            "credit_crypto",
            # USD pricing (populated later)
            "eth_usd_price",
            "debit_USD",
            "credit_USD",
            # Transaction identifiers
            "event",
            "hash",
            "loan_id",
            "lien_id",  # Blur-specific
            # Parties
            "lender",
            "borrower",
            "from",
            "to",
            # Contract details
            "contract_address",
            "payable_currency",
            "collateral_address",
            "token_id",
            # Loan terms
            "principal_crypto",
            "principal_USD",
            "interest_crypto",
            "interest_USD",
            "total_crypto",
            "total_USD",
            "annual_interest_rate",
            "rate_bips",
            "payoff_amount_crypto",
            "payoff_amount_USD",
            "loan_due_date",
            # Blur-specific
            "start_time",
            "repay_timestamp",
            "new_amount",
            "new_rate_bips",
        ]


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def compute_blur_debt(
    principal_wei: int,
    rate_bips: int,
    start_timestamp: int,
    end_timestamp: int,
) -> Tuple[Decimal, Decimal]:
    """
    Compute debt using Blur's continuous compounding formula.

    Args:
        principal_wei: Principal in wei
        rate_bips: Annual rate in basis points
        start_timestamp: Start time (Unix)
        end_timestamp: End time (Unix)

    Returns:
        (total_debt, interest) in native units
    """
    lien = LienData(
        lender='',
        borrower='',
        collection='',
        token_id=0,
        amount_wei=principal_wei,
        start_time=start_timestamp,
        rate_bips=rate_bips,
        auction_start_block=0,
        auction_duration=0,
    )

    debt = lien.compute_debt_at(end_timestamp)
    interest = lien.compute_interest_at(end_timestamp)

    return debt, interest


def validate_journal_balance(journal_df: pd.DataFrame) -> bool:
    """
    Validate that debits equal credits for each transaction hash.

    Returns:
        True if balanced, False otherwise
    """
    if journal_df.empty:
        return True

    if 'hash' not in journal_df.columns:
        return True

    unbalanced = []

    for tx_hash, group in journal_df.groupby('hash'):
        debits = group['debit_crypto'].sum()
        credits = group['credit_crypto'].sum()

        if abs(debits - credits) > Decimal('0.000000001'):
            unbalanced.append({
                'hash': tx_hash,
                'debits': debits,
                'credits': credits,
                'diff': debits - credits,
            })

    if unbalanced:
        print(f"[!] Found {len(unbalanced)} unbalanced entries:")
        for entry in unbalanced[:5]:
            print(f"   {entry['hash'][:10]}...: D={entry['debits']:.8f}, C={entry['credits']:.8f}, diff={entry['diff']:.8f}")
        return False

    return True

#### NFT
def process_nft_marketplace_batch(
    decoder,  # BlurEventDecoder instance
    journal_generator,  # BlurJournalEntryGenerator instance
    tx_hashes: List[str],
    nft_cost_basis_lookup: Optional[Dict] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process batch of NFT marketplace transactions.

    Args:
        decoder: BlurEventDecoder instance
        journal_generator: BlurJournalEntryGenerator instance
        tx_hashes: List of transaction hashes
        nft_cost_basis_lookup: Dict mapping (collection, token_id) -> cost_basis_eth

    Returns:
        (decoded_trades_df, journal_entries_df)

    Usage:
        df_trades, df_journals = process_nft_marketplace_batch(
            decoder=blur_decoder,
            journal_generator=journal_gen,
            tx_hashes=marketplace_tx_hashes,
            nft_cost_basis_lookup=cost_basis_dict,
        )
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from tqdm import tqdm

    # Filter to marketplace transactions only
    marketplace_txs = []
    for tx_hash in tx_hashes:
        try:
            tx = decoder.w3.eth.get_transaction(tx_hash)
            selector = tx['input'][:10].lower() if tx['input'] and len(tx['input']) >= 10 else ''
            if selector in NFT_MARKETPLACE_SELECTORS:
                marketplace_txs.append(tx_hash)
        except:
            continue

    print(f"🖼 Processing {len(marketplace_txs)} NFT marketplace transactions...")

    if not marketplace_txs:
        return pd.DataFrame(), pd.DataFrame()

    # Decode all trades
    all_trades = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_hash = {
            executor.submit(decoder.decode_nft_marketplace_transaction, tx_hash): tx_hash
            for tx_hash in marketplace_txs
        }

        for future in tqdm(
            as_completed(future_to_hash),
            total=len(marketplace_txs),
            desc="Decoding NFT trades",
            colour="green"
        ):
            try:
                trades = future.result()
                all_trades.extend(trades)
            except Exception as e:
                pass

    if not all_trades:
        print("[!] No fund wallet NFT trades found")
        return pd.DataFrame(), pd.DataFrame()

    df_trades = pd.DataFrame(all_trades)

    # Add cost basis if lookup provided
    if nft_cost_basis_lookup:
        def get_cost_basis(row):
            key = (row.get('collection', '').lower(), row.get('token_id'))
            return nft_cost_basis_lookup.get(key, 0)

        df_trades['cost_basis'] = df_trades.apply(get_cost_basis, axis=1)

    print(f"[OK] Decoded {len(df_trades)} NFT trades")

    # Generate journal entries
    df_sales = journal_generator.generate_nft_sale_entries(df_trades)
    df_purchases = journal_generator.generate_nft_purchase_entries(df_trades)

    # Combine
    journal_dfs = [df for df in [df_sales, df_purchases] if not df.empty]
    df_journals = pd.concat(journal_dfs, ignore_index=True) if journal_dfs else pd.DataFrame()

    print(f"[OK] Generated {len(df_journals)} total NFT journal entries")

    return df_trades, df_journals


#### NFT

# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    """
    Example usage of the Blur Blend processor.

    Prerequisites:
    - Web3 connection (w3)
    - wallet_metadata dict
    - Instantiated blend_contract and pool_contract
    """

    print("=" * 80)
    print("BLUR BLEND (NFT LENDING) DECODER - ONE-SHOT SOLUTION")
    print("=" * 80)
    print()
    print("Events Supported:")
    print("  - LoanOfferTaken (new loans)")
    print("  - Repay (repayments)")
    print("  - Refinance (loan refinancing)")
    print("  - StartAuction (lender calls loan)")
    print("  - Seize (foreclosure)")
    print("  - BuyLocked (NFT purchase from locked position)")
    print()
    print("Key Features:")
    print("  - Callable loans with no fixed term")
    print("  - Continuous compounding interest (matches Solidity exactly)")
    print("  - Retrospective daily interest accruals at settlement")
    print("  - Wei-precise calculations to avoid rounding errors")
    print()
    print("Account Names (exact from COA):")
    print(f"  - {BlurAccounts.DIGITAL_ASSETS_BLUR_POOL}")
    print(f"  - {BlurAccounts.LOAN_RECEIVABLE}")
    print(f"  - {BlurAccounts.INTEREST_RECEIVABLE}")
    print(f"  - {BlurAccounts.INTEREST_INCOME}")
    print(f"  - {BlurAccounts.NOTE_PAYABLE}")
    print(f"  - {BlurAccounts.INTEREST_PAYABLE}")
    print(f"  - {BlurAccounts.INTEREST_EXPENSE}")
    print(f"  - {BlurAccounts.BAD_DEBT_EXPENSE}")
    print(f"  - {BlurAccounts.INVESTMENTS_NFTS_SEIZED}")
    print()
    print("Usage:")
    print("""
    # Initialize
    processor = BlurLendingProcessor(
        w3=w3,
        wallet_metadata=wallet_metadata,
        blend_contract=blend_contract,
        pool_contract=pool_contract,
        output_path="/path/to/output",
    )

    # Process transactions
    results = processor.process_transactions(tx_hashes)

    # Access results
    df_events = results['decoded_events']
    df_journals = results['all_journal_entries']
    df_accruals = results['interest_accruals']

    # Validate balance
    is_balanced = validate_journal_balance(df_journals)
    """)