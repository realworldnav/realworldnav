"""
Arcade LoanCore Decoder - Complete One-Shot Solution
=====================================================

Decodes Arcade NFT lending protocol events and generates GAAP-compliant journal entries.

Contract Architecture:
- LoanCore: Stores loan state, emits events, holds collateral
- OriginationController: Starts loans (calls LoanCore.startLoan)
- RepaymentController: Repays/claims loans (calls LoanCore.repay/claim)
- PromissoryNote: ERC-721 tokens representing lender/borrower positions

Events Handled:
- LoanStarted: New loan origination (loanId, lender, borrower)
- LoanRepaid: Loan repayment (loanId only - enrich via getLoan)
- ForceRepay: Repayment held for lender withdrawal (loanId only)
- LoanClaimed: Foreclosure by lender (loanId only)
- LoanRolledOver: Atomic refinance (oldLoanId, newLoanId)
- NoteRedeemed: Lender claims ForceRepay funds

Interest Formula (exact Solidity match):
    interest = principal * proratedInterestRate / 1e22

Fee Mechanics (from RepaymentController):
    - On Repay: lender receives (principal + interest) - interestFee - principalFee
    - On Claim: lender pays claimFee = (principal + interest) * lenderDefaultFee / 10000
    - interestFee = interest * lenderInterestFee / 10000
    - principalFee = principal * lenderPrincipalFee / 10000

Author: Real World NAV
Version: 2.0.0
"""




from __future__ import annotations

import json
import math
from pathlib import Path
from decimal import Decimal, getcontext, ROUND_DOWN
from datetime import datetime, timedelta, time as dt_time, timezone
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
from tqdm import tqdm
from web3 import Web3
from web3.exceptions import ContractLogicError

# Set decimal precision for financial calculations
getcontext().prec = 28

# Module exports
__all__ = [
    'ArcadeEventDecoder',
    'ArcadeJournalGenerator',
    'DecodedArcadeEvent',
    'LoanData',
    'ArcadeEventType',
    'LoanState',
    'ARCADE_ORIGINATION_CONTROLLER',
    'ARCADE_REPAYMENT_CONTROLLER',
    'PLATFORM',
    'get_token_info',
    'get_token_symbol',
    'get_token_decimals',
    'standardize_dataframe',
]

# ============================================================================
# CONTRACT ADDRESSES (Arcade v3)
# ============================================================================

ARCADE_ORIGINATION_CONTROLLER = "0xB7BFcca7D7ff0f371867B770856FAc184B185878"
ARCADE_REPAYMENT_CONTROLLER = "0x74241e1A9c021643289476426B9B70229Ab40D53"

# Token Addresses
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
USDC_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
DAI_ADDRESS = "0x6B175474E89094C44Da98b954EedeAC495271d0F"

# Chainlink ETH/USD Price Feed
CHAINLINK_ETH_USD_FEED = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"

# ============================================================================
# CONSTANTS (from Solidity)
# ============================================================================

# Interest Calculation: interest = principal * proratedInterestRate / DENOMINATOR
INTEREST_RATE_DENOMINATOR = Decimal(10**18)
BASIS_POINTS_DENOMINATOR = Decimal(10**4)
DENOMINATOR = INTEREST_RATE_DENOMINATOR * BASIS_POINTS_DENOMINATOR  # 1e22

# Time Constants
SECONDS_PER_YEAR = Decimal(365 * 24 * 3600)  # 31,536,000
GRACE_PERIOD_SECS = 10 * 60  # 10 minutes (from LoanCore.sol)

# Token decimals
WAD = Decimal(10**18)
USDC_DECIMALS = Decimal(10**6)

# Platform Identifier
PLATFORM = "Arcade"


# ============================================================================
# TOKEN METADATA
# ============================================================================

TOKEN_METADATA = {
    WETH_ADDRESS.lower(): {"symbol": "WETH", "decimals": 18, "name": "Wrapped Ether"},
    USDC_ADDRESS.lower(): {"symbol": "USDC", "decimals": 6, "name": "USD Coin"},
    USDT_ADDRESS.lower(): {"symbol": "USDT", "decimals": 6, "name": "Tether USD"},
    DAI_ADDRESS.lower(): {"symbol": "DAI", "decimals": 18, "name": "Dai Stablecoin"},
}


def get_token_info(token_address: str) -> Dict:
    """Get token metadata by address."""
    addr_lower = token_address.lower() if token_address else ""
    return TOKEN_METADATA.get(addr_lower, {
        "symbol": "UNKNOWN",
        "decimals": 18,
        "name": "Unknown Token"
    })


def get_token_decimals(token_address: str) -> int:
    """Get token decimals."""
    return get_token_info(token_address)["decimals"]


def get_token_symbol(token_address: str) -> str:
    """Get token symbol."""
    return get_token_info(token_address)["symbol"]


# ============================================================================
# ENUMS
# ============================================================================

class LoanState(Enum):
    """Loan states from LoanLibrary.sol"""
    DUMMY_DO_NOT_USE = 0
    Active = 1
    Repaid = 2
    Defaulted = 3


class ArcadeEventType(Enum):
    """Arcade event types"""
    LOAN_STARTED = "LoanStarted"
    LOAN_REPAID = "LoanRepaid"
    FORCE_REPAY = "ForceRepay"
    LOAN_CLAIMED = "LoanClaimed"
    LOAN_ROLLED_OVER = "LoanRolledOver"
    NOTE_REDEEMED = "NoteRedeemed"


# ============================================================================
# ABI DEFINITIONS
# ============================================================================

LOANCORE_ABI = json.loads('''[
    {
        "inputs": [{"internalType": "uint256", "name": "loanId", "type": "uint256"}],
        "name": "getLoan",
        "outputs": [
            {
                "components": [
                    {"internalType": "enum LoanLibrary.LoanState", "name": "state", "type": "uint8"},
                    {"internalType": "uint160", "name": "startDate", "type": "uint160"},
                    {
                        "components": [
                            {"internalType": "uint256", "name": "proratedInterestRate", "type": "uint256"},
                            {"internalType": "uint256", "name": "principal", "type": "uint256"},
                            {"internalType": "address", "name": "collateralAddress", "type": "address"},
                            {"internalType": "uint96", "name": "durationSecs", "type": "uint96"},
                            {"internalType": "uint256", "name": "collateralId", "type": "uint256"},
                            {"internalType": "address", "name": "payableCurrency", "type": "address"},
                            {"internalType": "uint96", "name": "deadline", "type": "uint96"},
                            {"internalType": "bytes32", "name": "affiliateCode", "type": "bytes32"}
                        ],
                        "internalType": "struct LoanLibrary.LoanTerms",
                        "name": "terms",
                        "type": "tuple"
                    },
                    {
                        "components": [
                            {"internalType": "uint16", "name": "lenderDefaultFee", "type": "uint16"},
                            {"internalType": "uint16", "name": "lenderInterestFee", "type": "uint16"},
                            {"internalType": "uint16", "name": "lenderPrincipalFee", "type": "uint16"}
                        ],
                        "internalType": "struct LoanLibrary.FeeSnapshot",
                        "name": "feeSnapshot",
                        "type": "tuple"
                    }
                ],
                "internalType": "struct LoanLibrary.LoanData",
                "name": "loanData",
                "type": "tuple"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"internalType": "uint256", "name": "loanId", "type": "uint256"}],
        "name": "getNoteReceipt",
        "outputs": [
            {"internalType": "address", "name": "", "type": "address"},
            {"internalType": "uint256", "name": "", "type": "uint256"}
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "borrowerNote",
        "outputs": [{"internalType": "contract IPromissoryNote", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "lenderNote",
        "outputs": [{"internalType": "contract IPromissoryNote", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "anonymous": false,
        "inputs": [
            {"indexed": false, "internalType": "uint256", "name": "loanId", "type": "uint256"},
            {"indexed": false, "internalType": "address", "name": "lender", "type": "address"},
            {"indexed": false, "internalType": "address", "name": "borrower", "type": "address"}
        ],
        "name": "LoanStarted",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            {"indexed": false, "internalType": "uint256", "name": "loanId", "type": "uint256"}
        ],
        "name": "LoanRepaid",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            {"indexed": false, "internalType": "uint256", "name": "loanId", "type": "uint256"}
        ],
        "name": "ForceRepay",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            {"indexed": false, "internalType": "uint256", "name": "loanId", "type": "uint256"}
        ],
        "name": "LoanClaimed",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            {"indexed": false, "internalType": "uint256", "name": "oldLoanId", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "newLoanId", "type": "uint256"}
        ],
        "name": "LoanRolledOver",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            {"indexed": false, "internalType": "address", "name": "token", "type": "address"},
            {"indexed": false, "internalType": "address", "name": "lender", "type": "address"},
            {"indexed": false, "internalType": "address", "name": "to", "type": "address"},
            {"indexed": false, "internalType": "uint256", "name": "loanId", "type": "uint256"},
            {"indexed": false, "internalType": "uint256", "name": "amount", "type": "uint256"}
        ],
        "name": "NoteRedeemed",
        "type": "event"
    }
]''')

ORIGINATION_CONTROLLER_ABI = json.loads('''[
    {
        "inputs": [],
        "name": "loanCore",
        "outputs": [{"internalType": "contract ILoanCore", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function"
    }
]''')

PROMISSORY_NOTE_ABI = json.loads('''[
    {
        "inputs": [{"internalType": "uint256", "name": "tokenId", "type": "uint256"}],
        "name": "ownerOf",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"internalType": "address", "name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"internalType": "address", "name": "owner", "type": "address"}, {"internalType": "uint256", "name": "index", "type": "uint256"}],
        "name": "tokenOfOwnerByIndex",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    }
]''')

CHAINLINK_AGGREGATOR_ABI = json.loads('''[
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"}
        ],
        "stateMutability": "view",
        "type": "function"
    }
]''')


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class LoanTerms:
    """LoanTerms struct from LoanLibrary.sol"""
    proratedInterestRate: int  # Rate over loan lifetime, NOT APR
    principal: int  # Wei
    collateralAddress: str
    durationSecs: int
    collateralId: int
    payableCurrency: str
    deadline: int
    affiliateCode: bytes

    @classmethod
    def from_tuple(cls, data: tuple) -> 'LoanTerms':
        return cls(
            proratedInterestRate=int(data[0]),
            principal=int(data[1]),
            collateralAddress=str(data[2]),
            durationSecs=int(data[3]),
            collateralId=int(data[4]),
            payableCurrency=str(data[5]),
            deadline=int(data[6]),
            affiliateCode=data[7] if isinstance(data[7], bytes) else bytes.fromhex(str(data[7]).replace('0x', '').zfill(64)),
        )


@dataclass
class FeeSnapshot:
    """FeeSnapshot struct - fees applied on repay/claim (BPS)"""
    lenderDefaultFee: int   # Fee when lender claims defaulted collateral
    lenderInterestFee: int  # Fee on interest (deducted from lender's receipt)
    lenderPrincipalFee: int # Fee on principal (deducted from lender's receipt)

    @classmethod
    def from_tuple(cls, data: tuple) -> 'FeeSnapshot':
        return cls(
            lenderDefaultFee=int(data[0]),
            lenderInterestFee=int(data[1]),
            lenderPrincipalFee=int(data[2]),
        )


@dataclass
class LoanData:
    """LoanData struct from LoanLibrary.sol"""
    state: LoanState
    startDate: int
    terms: LoanTerms
    feeSnapshot: FeeSnapshot

    @classmethod
    def from_tuple(cls, data: tuple) -> 'LoanData':
        return cls(
            state=LoanState(int(data[0])),
            startDate=int(data[1]),
            terms=LoanTerms.from_tuple(data[2]),
            feeSnapshot=FeeSnapshot.from_tuple(data[3]),
        )


# ============================================================================
# INTEREST & FEE CALCULATOR (Exact Solidity Match)
# ============================================================================

class ArcadeInterestCalculator:
    """
    Interest and fee calculations matching Arcade's Solidity exactly.

    From InterestCalculator.sol:
        interest = principal * proratedInterestRate / (1e18 * 1e4)

    From RepaymentController.sol:
        amountFromBorrower = principal + interest
        interestFee = interest * lenderInterestFee / 10000
        principalFee = principal * lenderPrincipalFee / 10000
        amountToLender = amountFromBorrower - interestFee - principalFee

    From claim():
        claimFee = (principal + interest) * lenderDefaultFee / 10000
    """

    @staticmethod
    def get_interest_amount(principal_wei: int, prorated_interest_rate: int) -> Decimal:
        """Calculate interest amount - exact Solidity match."""
        return Decimal(principal_wei) * Decimal(prorated_interest_rate) / DENOMINATOR

    @staticmethod
    def get_payoff_amount(principal_wei: int, prorated_interest_rate: int) -> Decimal:
        """Calculate total payoff amount (principal + interest)."""
        interest = ArcadeInterestCalculator.get_interest_amount(principal_wei, prorated_interest_rate)
        return Decimal(principal_wei) + interest

    @staticmethod
    def get_repayment_breakdown(
        principal_wei: int,
        prorated_interest_rate: int,
        lender_interest_fee_bps: int,
        lender_principal_fee_bps: int,
    ) -> Dict[str, Decimal]:
        """
        Calculate full repayment breakdown matching RepaymentController._prepareRepay().

        Returns dict with:
            - amount_from_borrower: What borrower pays
            - interest: Total interest
            - interest_fee: Platform fee on interest
            - principal_fee: Platform fee on principal
            - amount_to_lender: What lender receives (net)
            - total_platform_fee: Total fees to protocol
        """
        interest = ArcadeInterestCalculator.get_interest_amount(principal_wei, prorated_interest_rate)
        principal = Decimal(principal_wei)

        amount_from_borrower = principal + interest

        # Fees are deducted from lender's receipt (lender pays these implicitly)
        interest_fee = interest * Decimal(lender_interest_fee_bps) / BASIS_POINTS_DENOMINATOR
        principal_fee = principal * Decimal(lender_principal_fee_bps) / BASIS_POINTS_DENOMINATOR

        amount_to_lender = amount_from_borrower - interest_fee - principal_fee
        total_platform_fee = interest_fee + principal_fee

        return {
            'amount_from_borrower': amount_from_borrower,
            'interest': interest,
            'interest_fee': interest_fee,
            'principal_fee': principal_fee,
            'amount_to_lender': amount_to_lender,
            'total_platform_fee': total_platform_fee,
            'gross_to_lender_before_fees': amount_from_borrower,
        }

    @staticmethod
    def get_claim_fee(
        principal_wei: int,
        prorated_interest_rate: int,
        lender_default_fee_bps: int,
    ) -> Decimal:
        """
        Calculate claim fee matching RepaymentController.claim().

        claimFee = (principal + interest) * lenderDefaultFee / 10000
        """
        interest = ArcadeInterestCalculator.get_interest_amount(principal_wei, prorated_interest_rate)
        total_owed = Decimal(principal_wei) + interest
        return total_owed * Decimal(lender_default_fee_bps) / BASIS_POINTS_DENOMINATOR

    @staticmethod
    def prorated_rate_to_annual(prorated_interest_rate: int, duration_secs: int) -> Decimal:
        """
        Convert prorated rate to annualized rate.

        The proratedInterestRate represents the rate over the loan term.
        To get APR: annual_rate = term_rate * (seconds_per_year / duration_secs)
        """
        if duration_secs <= 0:
            return Decimal(0)

        # Term rate as decimal (e.g., 500e18 / 1e22 = 0.05 = 5%)
        term_rate = Decimal(prorated_interest_rate) / DENOMINATOR

        # Annualize
        annual_rate = term_rate * (SECONDS_PER_YEAR / Decimal(duration_secs))
        return annual_rate


# ============================================================================
# EVENT DECODER
# ============================================================================

class ArcadeEventDecoder:
    """
    Decodes Arcade LoanCore events from transaction receipts.

    Events are "lean" - most only contain loanId. We enrich via:
    1. getLoan(loanId) for loan terms and fee snapshot
    2. PromissoryNote.ownerOf(loanId) for lender/borrower addresses

    Note: For closed loans, note ownership may have changed or notes burned.
    We try to get parties from the event first, then fall back to notes.
    """

    SUPPORTED_EVENTS = {
        'LoanStarted', 'LoanRepaid', 'ForceRepay',
        'LoanClaimed', 'LoanRolledOver', 'NoteRedeemed'
    }

    def __init__(
        self,
        w3: Web3,
        wallet_metadata: Dict[str, Dict],
        loancore_address: str = None,
    ):
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        # Only include wallets where category == "fund"
        self.fund_wallet_list = [
            addr for addr, info in self.wallet_metadata.items()
            if str(info.get('category', '')).lower() == 'fund'
        ]

        # Get LoanCore address from OriginationController
        if loancore_address:
            self.loancore_address = Web3.to_checksum_address(loancore_address)
        else:
            self.loancore_address = self._get_loancore_address()

        # Initialize contracts
        self.loancore_contract = w3.eth.contract(
            address=self.loancore_address,
            abi=LOANCORE_ABI
        )

        # Get note contracts
        self.borrower_note = None
        self.lender_note = None
        self._init_note_contracts()

        # Caches
        self._block_cache: Dict[int, int] = {}
        self._loan_cache: Dict[int, LoanData] = {}

        print(f"[OK] Arcade EventDecoder initialized")
        print(f"   LoanCore: {self.loancore_address}")
        print(f"   Total wallets in metadata: {len(self.wallet_metadata)}")
        print(f"   Fund wallets (category='fund'): {len(self.fund_wallet_list)}")

    def _get_loancore_address(self) -> str:
        """Fetch LoanCore address from OriginationController."""
        try:
            oc = self.w3.eth.contract(
                address=Web3.to_checksum_address(ARCADE_ORIGINATION_CONTROLLER),
                abi=ORIGINATION_CONTROLLER_ABI
            )
            loancore = oc.functions.loanCore().call()
            print(f"[OK] LoanCore address: {loancore}")
            return loancore
        except Exception as e:
            raise ValueError(f"Could not fetch LoanCore address: {e}")

    def _init_note_contracts(self):
        """Initialize PromissoryNote contracts."""
        try:
            borrower_note_addr = self.loancore_contract.functions.borrowerNote().call()
            lender_note_addr = self.loancore_contract.functions.lenderNote().call()

            self.borrower_note = self.w3.eth.contract(
                address=borrower_note_addr,
                abi=PROMISSORY_NOTE_ABI
            )
            self.lender_note = self.w3.eth.contract(
                address=lender_note_addr,
                abi=PROMISSORY_NOTE_ABI
            )
            print(f"   BorrowerNote: {borrower_note_addr}")
            print(f"   LenderNote: {lender_note_addr}")
        except Exception as e:
            print(f"[\!] Could not fetch note addresses: {e}")

    @lru_cache(maxsize=10000)
    def _get_block_timestamp(self, block_number: int) -> datetime:
        """Get block timestamp with caching."""
        block = self.w3.eth.get_block(block_number)
        return datetime.fromtimestamp(block['timestamp'], tz=timezone.utc)

    def _get_loan_data(self, loan_id: int, block_number: int = None) -> Optional[LoanData]:
        """
        Get loan data from contract with caching.

        Note: For closed loans, state will be Repaid/Defaulted but terms are preserved.
        """
        cache_key = loan_id
        if cache_key in self._loan_cache:
            return self._loan_cache[cache_key]

        try:
            # Try to get historical state if block provided
            if block_number:
                raw_data = self.loancore_contract.functions.getLoan(loan_id).call(
                    block_identifier=block_number
                )
            else:
                raw_data = self.loancore_contract.functions.getLoan(loan_id).call()

            loan_data = LoanData.from_tuple(raw_data)
            self._loan_cache[cache_key] = loan_data
            return loan_data
        except Exception as e:
            print(f"[\!] Could not fetch loan {loan_id}: {e}")
            return None

    def _get_note_owner(self, note_contract, loan_id: int, block_number: int = None) -> Optional[str]:
        """Get owner of a promissory note (may fail for burned notes)."""
        if not note_contract:
            return None
        try:
            if block_number:
                owner = note_contract.functions.ownerOf(loan_id).call(block_identifier=block_number)
            else:
                owner = note_contract.functions.ownerOf(loan_id).call()
            return owner.lower()
        except Exception:
            return None

    def decode_transaction(
        self,
        tx_hash: str,
        relevant_events: List[str] = None,
    ) -> List[Dict]:
        """
        Decode all relevant events from a transaction.

        For each event:
        1. Parse event data
        2. Enrich with loan terms from getLoan()
        3. Identify lender/borrower from event or notes
        4. Calculate derived fields (interest, fees, due date)
        """
        if relevant_events is None:
            relevant_events = list(self.SUPPORTED_EVENTS)

        try:
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)
        except Exception as e:
            print(f"[\!] Could not fetch receipt for {tx_hash}: {e}")
            return []

        block_number = receipt['blockNumber']
        tx_datetime = self._get_block_timestamp(block_number)

        decoded_events = []

        # Process each log
        for log in receipt['logs']:
            # Check if log is from LoanCore
            if log['address'].lower() != self.loancore_address.lower():
                continue

            # Try to match each event type
            for event_name in relevant_events:
                if event_name not in self.SUPPORTED_EVENTS:
                    continue

                try:
                    event = getattr(self.loancore_contract.events, event_name)()
                    decoded = event.process_log(log)

                    # Successfully decoded - process it
                    event_dict = self._process_decoded_event(
                        decoded, event_name, tx_hash, block_number, tx_datetime
                    )
                    if event_dict:
                        decoded_events.append(event_dict)
                    break  # Found matching event, no need to try others
                except Exception:
                    continue  # Not this event type

        return decoded_events

    def _process_decoded_event(
        self,
        decoded,
        event_name: str,
        tx_hash: str,
        block_number: int,
        tx_datetime: datetime,
    ) -> Optional[Dict]:
        """Process a decoded event and enrich with loan data."""
        args = dict(decoded['args'])

        event_dict = {
            'event': event_name,
            'transactionHash': tx_hash,
            'blockNumber': block_number,
            'logIndex': decoded['logIndex'],
            'transaction_datetime': tx_datetime,
        }

        # Extract loan IDs based on event type
        if event_name == 'LoanRolledOver':
            old_loan_id = int(args.get('oldLoanId', 0))
            new_loan_id = int(args.get('newLoanId', 0))
            event_dict['oldLoanId'] = old_loan_id
            event_dict['newLoanId'] = new_loan_id
            event_dict['loanId'] = new_loan_id  # Primary reference is new loan
            loan_id = new_loan_id
        elif event_name == 'NoteRedeemed':
            loan_id = int(args.get('loanId', 0))
            event_dict['loanId'] = loan_id
            event_dict['redeemToken'] = str(args.get('token', '')).lower()
            event_dict['redeemLender'] = str(args.get('lender', '')).lower()
            event_dict['redeemTo'] = str(args.get('to', '')).lower()
            event_dict['redeemAmount'] = int(args.get('amount', 0))
        else:
            loan_id = int(args.get('loanId', 0))
            event_dict['loanId'] = loan_id

        # For LoanStarted, get lender/borrower from event
        if event_name == 'LoanStarted':
            event_dict['lender'] = str(args.get('lender', '')).lower()
            event_dict['borrower'] = str(args.get('borrower', '')).lower()

        # Enrich with loan data
        # For LoanClaimed/LoanRepaid, get loan data at block BEFORE (state changes during tx)
        if event_name in ['LoanClaimed', 'LoanRepaid']:
            # Get loan data at block BEFORE to see original state
            loan_data = self._get_loan_data(loan_id, block_number - 1)
        else:
            loan_data = self._get_loan_data(loan_id, block_number)

        if loan_data:
            self._enrich_with_loan_data(event_dict, loan_data, event_name)

        # Get lender/borrower from notes if not in event
        # For LoanClaimed/LoanRepaid: notes are BURNED during the transaction
        # So we must query at block BEFORE to get ownership
        if event_name in ['LoanClaimed', 'LoanRepaid']:
            note_block = block_number - 1  # Before notes are burned
        else:
            note_block = block_number

        if 'lender' not in event_dict or not event_dict.get('lender'):
            event_dict['lender'] = self._get_note_owner(self.lender_note, loan_id, note_block)
        if 'borrower' not in event_dict or not event_dict.get('borrower'):
            event_dict['borrower'] = self._get_note_owner(self.borrower_note, loan_id, note_block)

        # Fallback: for LoanClaimed, tx sender is typically the lender (or authorized)
        if event_name == 'LoanClaimed' and not event_dict.get('lender'):
            try:
                tx = self.w3.eth.get_transaction(tx_hash)
                event_dict['lender'] = tx['from'].lower()
                print(f"   [\!] Used tx sender as lender for claim: {event_dict['lender']}")
            except Exception:
                pass

        # For rollovers, get old loan data AND old lender (owner before rollover)
        if event_name == 'LoanRolledOver' and 'oldLoanId' in event_dict:
            old_loan_id = event_dict['oldLoanId']

            # Get old loan data at block BEFORE rollover (to see original state)
            old_loan_data = self._get_loan_data(old_loan_id, block_number - 1)
            if old_loan_data:
                event_dict['oldPrincipal'] = old_loan_data.terms.principal
                event_dict['oldProratedInterestRate'] = old_loan_data.terms.proratedInterestRate
                event_dict['oldDurationSecs'] = old_loan_data.terms.durationSecs
                event_dict['oldPayableCurrency'] = old_loan_data.terms.payableCurrency.lower()
                # Old loan fee snapshot (for interest/principal fee calculations)
                event_dict['oldLenderInterestFee'] = old_loan_data.feeSnapshot.lenderInterestFee
                event_dict['oldLenderPrincipalFee'] = old_loan_data.feeSnapshot.lenderPrincipalFee

            # CRITICAL: Get OLD LENDER from note ownership BEFORE rollover
            # (The lender note for oldLoanId is burned during rollover)
            old_lender = self._get_note_owner(self.lender_note, old_loan_id, block_number - 1)
            event_dict['oldLender'] = old_lender

            # The 'lender' field already has NEW LENDER from LoanStarted event
            # (populated via note lookup on newLoanId above)
            # Rename for clarity
            event_dict['newLender'] = event_dict.get('lender', '')

            # Borrower stays the same (from newLoanId)
            # Get old borrower too (should be same but let's be safe)
            old_borrower = self._get_note_owner(self.borrower_note, old_loan_id, block_number - 1)
            event_dict['oldBorrower'] = old_borrower

        return event_dict

    def _enrich_with_loan_data(self, event_dict: Dict, loan_data: LoanData, event_name: str):
        """Add loan terms and calculated fields to event dict."""
        terms = loan_data.terms
        fees = loan_data.feeSnapshot

        # Raw loan terms
        event_dict['principal'] = terms.principal
        event_dict['proratedInterestRate'] = terms.proratedInterestRate
        event_dict['durationSecs'] = terms.durationSecs
        event_dict['payableCurrency'] = terms.payableCurrency.lower()
        event_dict['collateralAddress'] = terms.collateralAddress.lower()
        event_dict['collateralId'] = terms.collateralId
        event_dict['startDate'] = loan_data.startDate
        event_dict['loanState'] = loan_data.state.value

        # Fee snapshot
        event_dict['lenderDefaultFee'] = fees.lenderDefaultFee
        event_dict['lenderInterestFee'] = fees.lenderInterestFee
        event_dict['lenderPrincipalFee'] = fees.lenderPrincipalFee

        # Calculated fields
        interest = ArcadeInterestCalculator.get_interest_amount(
            terms.principal, terms.proratedInterestRate
        )
        event_dict['interestAmount'] = float(interest)
        event_dict['grossPayoffAmount'] = float(Decimal(terms.principal) + interest)

        # Annual rate for reporting
        event_dict['annualInterestRate'] = float(
            ArcadeInterestCalculator.prorated_rate_to_annual(
                terms.proratedInterestRate, terms.durationSecs
            )
        )

        # Calculate due date (startDate + durationSecs)
        if loan_data.startDate and terms.durationSecs:
            due_timestamp = loan_data.startDate + terms.durationSecs
            event_dict['loanDueDate'] = datetime.fromtimestamp(due_timestamp, tz=timezone.utc)
            # Claim allowed after due date + grace period
            event_dict['claimableAfter'] = datetime.fromtimestamp(
                due_timestamp + GRACE_PERIOD_SECS, tz=timezone.utc
            )

        # Calculate repayment breakdown (for repaid events)
        if event_name in ['LoanRepaid', 'ForceRepay', 'LoanRolledOver']:
            breakdown = ArcadeInterestCalculator.get_repayment_breakdown(
                terms.principal,
                terms.proratedInterestRate,
                fees.lenderInterestFee,
                fees.lenderPrincipalFee,
            )
            event_dict['amountFromBorrower'] = float(breakdown['amount_from_borrower'])
            event_dict['amountToLender'] = float(breakdown['amount_to_lender'])
            event_dict['interestFee'] = float(breakdown['interest_fee'])
            event_dict['principalFee'] = float(breakdown['principal_fee'])
            event_dict['totalPlatformFee'] = float(breakdown['total_platform_fee'])

        # Calculate claim fee (for claimed events)
        if event_name == 'LoanClaimed':
            claim_fee = ArcadeInterestCalculator.get_claim_fee(
                terms.principal,
                terms.proratedInterestRate,
                fees.lenderDefaultFee,
            )
            event_dict['claimFee'] = float(claim_fee)

    def decode_batch(
        self,
        tx_hashes: List[str],
        relevant_events: List[str] = None,
        max_workers: int = 8,
        filter_fund_wallets: bool = True,
        debug: bool = False,
    ) -> pd.DataFrame:
        """Decode events from multiple transactions in parallel."""
        all_events = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_hash = {
                executor.submit(self.decode_transaction, tx_hash, relevant_events): tx_hash
                for tx_hash in tx_hashes
            }

            for future in tqdm(
                as_completed(future_to_hash),
                total=len(tx_hashes),
                desc="Decoding Arcade events",
                colour="blue"
            ):
                tx_hash = future_to_hash[future]
                try:
                    events = future.result()
                    for event in events:
                        event['tx_hash'] = tx_hash
                        all_events.append(event)
                        if debug:
                            print(f"   📝 Decoded {event.get('event', 'unknown')}: lender={event.get('lender', 'None')}, borrower={event.get('borrower', 'None')}")
                except Exception as e:
                    print(f"[\!] Failed to decode {tx_hash}: {e}")

        if not all_events:
            print("[\!] No events decoded from any transactions")
            return pd.DataFrame()

        df = pd.DataFrame(all_events)

        if debug:
            print(f"\n📊 Pre-filter: {len(df)} events")
            print(f"   Events by type: {df['event'].value_counts().to_dict()}")
            print(f"   Unique lenders: {df['lender'].dropna().unique().tolist()}")
            print(f"   Unique borrowers: {df['borrower'].dropna().unique().tolist()}")
            print(f"   Fund wallet list ({len(self.fund_wallet_list)} wallets): {self.fund_wallet_list[:5]}{'...' if len(self.fund_wallet_list) > 5 else ''}")

        # Filter for fund wallets
        if filter_fund_wallets and not df.empty:
            pre_filter_count = len(df)
            lender_mask = df['lender'].fillna('').str.lower().isin(self.fund_wallet_list)
            borrower_mask = df['borrower'].fillna('').str.lower().isin(self.fund_wallet_list)

            # For rollovers, also check oldLender and newLender
            old_lender_mask = df['oldLender'].fillna('').str.lower().isin(self.fund_wallet_list) if 'oldLender' in df.columns else pd.Series([False] * len(df))
            new_lender_mask = df['newLender'].fillna('').str.lower().isin(self.fund_wallet_list) if 'newLender' in df.columns else pd.Series([False] * len(df))

            df = df[lender_mask | borrower_mask | old_lender_mask | new_lender_mask]

            if debug:
                filtered_count = pre_filter_count - len(df)
                print(f"   Filtered out {filtered_count} events (not matching fund wallets)")
                print(f"📊 Post-filter: {len(df)} events")
                if len(df) > 0:
                    print(f"   Events by type: {df['event'].value_counts().to_dict()}")

        return df


# ============================================================================
# JOURNAL ENTRY GENERATOR
# ============================================================================

class ArcadeJournalEntryGenerator:
    """
    Generates GAAP-compliant journal entries from decoded Arcade events.

    Account naming convention (preserved from X2Y2):
    - loan_receivable_cryptocurrency_{token}
    - interest_receivable_cryptocurrency_{token}
    - interest_income_cryptocurrency_{token}
    - deemed_cash_usd / deemed_cash_eth
    - investments_nfts_seized_collateral
    - bad_debt_expense_cryptocurrency_{token}
    - note_payable_cryptocurrency_{token}
    - interest_payable_cryptocurrency_{token}
    - platform_fee_expense_{token} (NEW - for lender fees)
    """

    def __init__(self, wallet_metadata: Dict[str, Dict]):
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        # Only include wallets where category == "fund"
        self.fund_wallet_list = [
            addr for addr, info in self.wallet_metadata.items()
            if str(info.get('category', '')).lower() == 'fund'
        ]

    def _get_fund_id(self, address: str) -> str:
        if not address:
            return ''
        return self.wallet_metadata.get(address.lower(), {}).get('fund_id', '')

    def _get_wallet_id(self, address: str) -> str:
        if not address:
            return ''
        info = self.wallet_metadata.get(address.lower(), {})
        return info.get('wallet_id', address.lower())

    def _get_token_suffix(self, payable_currency: str) -> str:
        symbol = get_token_symbol(payable_currency).lower()
        return 'weth' if symbol in ['weth', 'eth'] else symbol

    def _to_human_amount(self, wei_amount: int, payable_currency: str) -> Decimal:
        decimals = get_token_decimals(payable_currency)
        return Decimal(wei_amount) / Decimal(10 ** decimals)

    def _build_common_metadata(self, row: pd.Series, event_name: str) -> Dict:
        """Build common metadata for all journal entries."""
        lender = str(row.get('lender', '')).lower()
        borrower = str(row.get('borrower', '')).lower()
        payable_currency = str(row.get('payableCurrency', WETH_ADDRESS)).lower()
        symbol = get_token_symbol(payable_currency)

        tx_dt = row.get('transaction_datetime')
        if isinstance(tx_dt, str):
            tx_dt = pd.to_datetime(tx_dt, utc=True)

        loan_due_date = row.get('loanDueDate')
        if isinstance(loan_due_date, str):
            loan_due_date = pd.to_datetime(loan_due_date, utc=True)

        return {
            'date': tx_dt,
            'platform': PLATFORM,
            'counterparty_fund_id': '',
            'cryptocurrency': symbol,
            'event': event_name,
            'hash': row.get('tx_hash', row.get('transactionHash', '')),
            'loan_id': str(row.get('loanId', '')),
            'lender': lender,
            'borrower': borrower,
            'contract_address': row.get('loancore_address', ''),
            'payable_currency': payable_currency,
            'collateral_address': str(row.get('collateralAddress', '')).lower(),
            'token_id': str(row.get('collateralId', '')),
            'loan_due_date': loan_due_date,
        }

    # =========================================================================
    # LOAN STARTED
    # =========================================================================

    def generate_loan_started_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate journal entries for LoanStarted events.

        Lender perspective:
            Dr loan_receivable_cryptocurrency_{token}  (principal)
            Cr deemed_cash_usd                         (principal)

        Borrower perspective:
            Dr deemed_cash_usd                         (principal)
            Cr note_payable_cryptocurrency_{token}                    (principal)
        """
        if df.empty:
            return pd.DataFrame()

        loan_started = df[df['event'] == 'LoanStarted'].copy()
        if loan_started.empty:
            return pd.DataFrame()

        journal_rows = []
        for _, row in loan_started.iterrows():
            entries = self._process_loan_started_row(row)
            journal_rows.extend(entries)

        return pd.DataFrame(journal_rows) if journal_rows else pd.DataFrame()

    def _process_loan_started_row(self, row: pd.Series) -> List[Dict]:
        """Process single LoanStarted event."""
        entries = []

        lender = str(row.get('lender', '')).lower()
        borrower = str(row.get('borrower', '')).lower()
        is_lender_fund = lender in self.fund_wallet_list
        is_borrower_fund = borrower in self.fund_wallet_list

        payable_currency = str(row.get('payableCurrency', WETH_ADDRESS)).lower()
        token_suffix = self._get_token_suffix(payable_currency)

        principal_wei = int(row.get('principal', 0))
        principal = self._to_human_amount(principal_wei, payable_currency)

        # Calculate interest for payoff amount
        prorated_rate = int(row.get('proratedInterestRate', 0))
        duration_secs = int(row.get('durationSecs', 0))
        interest_wei = ArcadeInterestCalculator.get_interest_amount(principal_wei, prorated_rate)
        interest = self._to_human_amount(int(interest_wei), payable_currency)
        payoff = principal + interest
        annual_rate = ArcadeInterestCalculator.prorated_rate_to_annual(prorated_rate, duration_secs)

        common = self._build_common_metadata(row, 'LoanStarted')
        common.update({
            'principal': principal,
            'annual_interest_rate': annual_rate,
            'payoff_amount': payoff,
        })

        # Lender entries
        if is_lender_fund:
            lender_fund = self._get_fund_id(lender)
            lender_wallet = self._get_wallet_id(lender)

            entries.append({
                **common,
                'transaction_type': 'investments_lending',
                'fund_id': lender_fund,
                'wallet_id': lender_wallet,
                'from': lender,
                'to': borrower,
                'account_name': f'loan_receivable_cryptocurrency_{token_suffix}',
                'debit': principal,
                'credit': Decimal(0),
            })
            entries.append({
                **common,
                'transaction_type': 'investments_lending',
                'fund_id': lender_fund,
                'wallet_id': lender_wallet,
                'from': lender,
                'to': borrower,
                'account_name': 'deemed_cash_usd',
                'debit': Decimal(0),
                'credit': principal,
            })

        # Borrower entries
        if is_borrower_fund:
            borrower_fund = self._get_fund_id(borrower)
            borrower_wallet = self._get_wallet_id(borrower)

            entries.append({
                **common,
                'transaction_type': 'financing_borrowings',
                'fund_id': borrower_fund,
                'wallet_id': borrower_wallet,
                'from': lender,
                'to': borrower,
                'account_name': 'deemed_cash_usd',
                'debit': principal,
                'credit': Decimal(0),
            })
            entries.append({
                **common,
                'transaction_type': 'financing_borrowings',
                'fund_id': borrower_fund,
                'wallet_id': borrower_wallet,
                'from': lender,
                'to': borrower,
                'account_name': f'note_payable_cryptocurrency_{token_suffix}',
                'debit': Decimal(0),
                'credit': principal,
            })

        return entries

    # =========================================================================
    # LOAN REPAID (includes ForceRepay)
    # =========================================================================

    def generate_loan_repaid_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate journal entries for LoanRepaid/ForceRepay events.

        Key insight: Lender fees are deducted from lender's receipt!

        Lender perspective (receives NET):
            Dr deemed_cash_usd                              (net_to_lender)
            Dr platform_fee_expense_{token}                 (interest_fee + principal_fee)
            Cr loan_receivable_cryptocurrency_{token}       (principal)
            Cr interest_receivable_cryptocurrency_{token}   (interest)

        Borrower perspective (pays GROSS):
            Dr note_payable_cryptocurrency_{token}                         (principal)
            Dr interest_payable_cryptocurrency_{token}      (interest)
            Cr deemed_cash_usd                              (principal + interest)
        """
        if df.empty:
            return pd.DataFrame()

        loan_repaid = df[df['event'].isin(['LoanRepaid', 'ForceRepay'])].copy()
        if loan_repaid.empty:
            return pd.DataFrame()

        journal_rows = []
        for _, row in loan_repaid.iterrows():
            entries = self._process_loan_repaid_row(row)
            journal_rows.extend(entries)

        return pd.DataFrame(journal_rows) if journal_rows else pd.DataFrame()

    def _process_loan_repaid_row(self, row: pd.Series) -> List[Dict]:
        """Process single LoanRepaid/ForceRepay event."""
        entries = []

        lender = str(row.get('lender', '')).lower()
        borrower = str(row.get('borrower', '')).lower()
        is_lender_fund = lender in self.fund_wallet_list
        is_borrower_fund = borrower in self.fund_wallet_list

        payable_currency = str(row.get('payableCurrency', WETH_ADDRESS)).lower()
        token_suffix = self._get_token_suffix(payable_currency)

        principal_wei = int(row.get('principal', 0))
        prorated_rate = int(row.get('proratedInterestRate', 0))
        lender_interest_fee_bps = int(row.get('lenderInterestFee', 0))
        lender_principal_fee_bps = int(row.get('lenderPrincipalFee', 0))

        # Calculate full breakdown
        breakdown = ArcadeInterestCalculator.get_repayment_breakdown(
            principal_wei, prorated_rate, lender_interest_fee_bps, lender_principal_fee_bps
        )

        # Convert to human-readable
        principal = self._to_human_amount(principal_wei, payable_currency)
        interest = self._to_human_amount(int(breakdown['interest']), payable_currency)
        interest_fee = self._to_human_amount(int(breakdown['interest_fee']), payable_currency)
        principal_fee = self._to_human_amount(int(breakdown['principal_fee']), payable_currency)
        amount_to_lender = self._to_human_amount(int(breakdown['amount_to_lender']), payable_currency)
        gross_from_borrower = principal + interest
        total_platform_fee = interest_fee + principal_fee

        event_name = row.get('event', 'LoanRepaid')
        common = self._build_common_metadata(row, event_name)
        common.update({
            'principal': principal,
            'payoff_amount': gross_from_borrower,
            'annual_interest_rate': row.get('annualInterestRate', Decimal(0)),
        })

        # Lender entries (receives NET after fees)
        if is_lender_fund:
            lender_fund = self._get_fund_id(lender)
            lender_wallet = self._get_wallet_id(lender)

            lender_common = {
                **common,
                'transaction_type': 'investments_lending',
                'fund_id': lender_fund,
                'wallet_id': lender_wallet,
                'from': borrower,
                'to': lender,
            }

            # Dr cash received (net)
            entries.append({
                **lender_common,
                'account_name': 'deemed_cash_usd',
                'debit': amount_to_lender,
                'credit': Decimal(0),
            })

            # Dr platform fee expense (what lender "paid" in fees)
            if total_platform_fee > 0:
                entries.append({
                    **lender_common,
                    'account_name': f'platform_fee_expense_{token_suffix}',
                    'debit': total_platform_fee,
                    'credit': Decimal(0),
                })

            # Cr loan receivable (principal)
            entries.append({
                **lender_common,
                'account_name': f'loan_receivable_cryptocurrency_{token_suffix}',
                'debit': Decimal(0),
                'credit': principal,
            })

            # Cr interest receivable
            if interest > 0:
                entries.append({
                    **lender_common,
                    'account_name': f'interest_receivable_cryptocurrency_{token_suffix}',
                    'debit': Decimal(0),
                    'credit': interest,
                })

        # Borrower entries (pays GROSS)
        if is_borrower_fund:
            borrower_fund = self._get_fund_id(borrower)
            borrower_wallet = self._get_wallet_id(borrower)

            borrower_common = {
                **common,
                'transaction_type': 'financing_borrowings',
                'fund_id': borrower_fund,
                'wallet_id': borrower_wallet,
                'from': borrower,
                'to': lender,
            }

            # Dr note payable (principal)
            entries.append({
                **borrower_common,
                'account_name': f'note_payable_cryptocurrency_{token_suffix}',
                'debit': principal,
                'credit': Decimal(0),
            })

            # Dr interest payable
            if interest > 0:
                entries.append({
                    **borrower_common,
                    'account_name': f'interest_payable_cryptocurrency_{token_suffix}',
                    'debit': interest,
                    'credit': Decimal(0),
                })

            # Cr cash paid (gross)
            entries.append({
                **borrower_common,
                'account_name': 'deemed_cash_usd',
                'debit': Decimal(0),
                'credit': gross_from_borrower,
            })

        return entries

    # =========================================================================
    # LOAN CLAIMED (Foreclosure)
    # =========================================================================

    def generate_loan_claimed_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate journal entries for LoanClaimed (foreclosure) events.

        Lender perspective (seizes collateral, pays claim fee):
            Dr investments_nfts_seized_collateral           (principal - valued at cost basis)
            Dr bad_debt_expense_cryptocurrency_{token}      (accrued interest - written off)
            Dr platform_fee_expense_{token}                 (claim fee)
            Cr loan_receivable_cryptocurrency_{token}       (principal)
            Cr interest_receivable_cryptocurrency_{token}   (accrued interest)
            Cr deemed_cash_usd                              (claim fee paid)
        """
        if df.empty:
            return pd.DataFrame()

        loan_claimed = df[df['event'] == 'LoanClaimed'].copy()
        if loan_claimed.empty:
            return pd.DataFrame()

        journal_rows = []
        for _, row in loan_claimed.iterrows():
            entries = self._process_loan_claimed_row(row)
            journal_rows.extend(entries)

        return pd.DataFrame(journal_rows) if journal_rows else pd.DataFrame()

    def _process_loan_claimed_row(self, row: pd.Series) -> List[Dict]:
        """Process single LoanClaimed event."""
        entries = []

        lender = str(row.get('lender', '')).lower()
        borrower = str(row.get('borrower', '')).lower()

        # Only lender can claim
        if lender not in self.fund_wallet_list:
            return entries

        payable_currency = str(row.get('payableCurrency', WETH_ADDRESS)).lower()
        token_suffix = self._get_token_suffix(payable_currency)

        principal_wei = int(row.get('principal', 0))
        prorated_rate = int(row.get('proratedInterestRate', 0))
        lender_default_fee_bps = int(row.get('lenderDefaultFee', 0))

        principal = self._to_human_amount(principal_wei, payable_currency)
        interest = self._to_human_amount(
            int(ArcadeInterestCalculator.get_interest_amount(principal_wei, prorated_rate)),
            payable_currency
        )
        claim_fee = self._to_human_amount(
            int(ArcadeInterestCalculator.get_claim_fee(
                principal_wei, prorated_rate, lender_default_fee_bps
            )),
            payable_currency
        )

        lender_fund = self._get_fund_id(lender)
        lender_wallet = self._get_wallet_id(lender)

        common = self._build_common_metadata(row, 'LoanClaimed')
        common.update({
            'transaction_type': 'investments_foreclosures',
            'fund_id': lender_fund,
            'counterparty_fund_id': self._get_fund_id(borrower),
            'wallet_id': lender_wallet,
            'from': borrower,
            'to': lender,
            'principal': principal,
            'payoff_amount': principal + interest,
            'annual_interest_rate': row.get('annualInterestRate', Decimal(0)),
        })

        # Dr seized collateral (valued at principal as cost basis)
        entries.append({
            **common,
            'account_name': 'investments_nfts_seized_collateral',
            'debit': principal,
            'credit': Decimal(0),
        })

        # Dr bad debt expense (accrued interest written off)
        if interest > 0:
            entries.append({
                **common,
                'account_name': f'bad_debt_expense_cryptocurrency_{token_suffix}',
                'debit': interest,
                'credit': Decimal(0),
            })

        # Dr claim fee expense
        if claim_fee > 0:
            entries.append({
                **common,
                'account_name': f'platform_fee_expense_{token_suffix}',
                'debit': claim_fee,
                'credit': Decimal(0),
            })

        # Cr loan receivable (principal)
        entries.append({
            **common,
            'account_name': f'loan_receivable_cryptocurrency_{token_suffix}',
            'debit': Decimal(0),
            'credit': principal,
        })

        # Cr interest receivable (accrued interest)
        if interest > 0:
            entries.append({
                **common,
                'account_name': f'interest_receivable_cryptocurrency_{token_suffix}',
                'debit': Decimal(0),
                'credit': interest,
            })

        # Cr cash (claim fee paid)
        if claim_fee > 0:
            entries.append({
                **common,
                'account_name': 'deemed_cash_usd',
                'debit': Decimal(0),
                'credit': claim_fee,
            })

        return entries

    # =========================================================================
    # NOTE: ROLLOVER HANDLING
    # =========================================================================
    # Rollovers are NOT treated as special events. Instead:
    # - If fund owned the old loan -> LoanRepaid generates payoff entries
    # - If fund is the new lender -> LoanStarted generates origination entries
    # - If fund is both old and new lender (self-rollover) -> both events apply
    #   and cash flows net out correctly:
    #   LoanRepaid: DR deemed_cash (payoff), CR loan_receivable
    #   LoanStarted: DR loan_receivable, CR deemed_cash
    #   Net cash = payoff - new_principal
    # =========================================================================

    # =========================================================================
    # INTEREST ACCRUALS (Daily)
    # =========================================================================

    def generate_interest_income_accruals(
        self,
        df_loans: pd.DataFrame,
        df_closures: pd.DataFrame = None,
        cutoff_date: datetime = None,
    ) -> pd.DataFrame:
        """
        Generate daily interest income accruals for lender.

        For each day of the loan (or until closure/cutoff):
            Dr interest_receivable_cryptocurrency_{token}
            Cr interest_income_cryptocurrency_{token}

        Uses Wei-precise allocation to avoid rounding errors.
        """
        if df_loans.empty:
            return pd.DataFrame()

        # Filter for loans where fund is lender
        lender_loans = df_loans[
            df_loans['lender'].fillna('').str.lower().isin(self.fund_wallet_list)
        ].copy()

        if lender_loans.empty:
            return pd.DataFrame()

        journal_rows = []
        for _, loan_row in lender_loans.iterrows():
            entries = self._generate_daily_accruals(
                loan_row, df_closures, cutoff_date, is_lender=True
            )
            journal_rows.extend(entries)

        return pd.DataFrame(journal_rows) if journal_rows else pd.DataFrame()

    def generate_interest_expense_accruals(
        self,
        df_loans: pd.DataFrame,
        df_closures: pd.DataFrame = None,
        cutoff_date: datetime = None,
    ) -> pd.DataFrame:
        """
        Generate daily interest expense accruals for borrower.

        For each day of the loan:
            Dr interest_expense_cryptocurrency_{token}
            Cr interest_payable_cryptocurrency_{token}
        """
        if df_loans.empty:
            return pd.DataFrame()

        # Filter for loans where fund is borrower
        borrower_loans = df_loans[
            df_loans['borrower'].fillna('').str.lower().isin(self.fund_wallet_list)
        ].copy()

        if borrower_loans.empty:
            return pd.DataFrame()

        journal_rows = []
        for _, loan_row in borrower_loans.iterrows():
            entries = self._generate_daily_accruals(
                loan_row, df_closures, cutoff_date, is_lender=False
            )
            journal_rows.extend(entries)

        return pd.DataFrame(journal_rows) if journal_rows else pd.DataFrame()

    def _generate_daily_accruals(
        self,
        loan_row: pd.Series,
        df_closures: pd.DataFrame = None,
        cutoff_date: datetime = None,
        is_lender: bool = True,
    ) -> List[Dict]:
        """
        Generate daily interest accruals for a single loan.

        Uses Wei-precise leftover accumulation to match total interest exactly.
        """
        # Get loan start date
        tx_dt = loan_row.get('transaction_datetime')
        if isinstance(tx_dt, str):
            tx_dt = pd.to_datetime(tx_dt, utc=True)

        start_date = loan_row.get('startDate')
        if start_date:
            start_dt = datetime.fromtimestamp(int(start_date), tz=timezone.utc)
        else:
            start_dt = tx_dt

        duration_secs = int(loan_row.get('durationSecs', 0))
        if duration_secs <= 0:
            return []

        end_dt = start_dt + timedelta(seconds=duration_secs)

        # Check for early closure
        loan_id = str(loan_row.get('loanId', ''))
        if df_closures is not None and not df_closures.empty:
            closure = df_closures[df_closures['loanId'].astype(str) == loan_id]
            if not closure.empty:
                close_dt = closure.iloc[0].get('transaction_datetime')
                if isinstance(close_dt, str):
                    close_dt = pd.to_datetime(close_dt, utc=True)
                if close_dt and close_dt < end_dt:
                    end_dt = close_dt

        # Apply cutoff date
        if cutoff_date and cutoff_date < end_dt:
            end_dt = cutoff_date

        # Calculate total interest in Wei
        principal_wei = int(loan_row.get('principal', 0))
        prorated_rate = int(loan_row.get('proratedInterestRate', 0))
        total_interest_wei = int(ArcadeInterestCalculator.get_interest_amount(principal_wei, prorated_rate))

        if total_interest_wei <= 0:
            return []

        # Get token info
        payable_currency = str(loan_row.get('payableCurrency', WETH_ADDRESS)).lower()
        token_suffix = self._get_token_suffix(payable_currency)
        symbol = get_token_symbol(payable_currency)
        decimals = get_token_decimals(payable_currency)

        lender = str(loan_row.get('lender', '')).lower()
        borrower = str(loan_row.get('borrower', '')).lower()

        # Set up account names based on perspective
        if is_lender:
            fund_id = self._get_fund_id(lender)
            wallet_id = self._get_wallet_id(lender)
            transaction_type = 'income_interest_accruals'
            debit_account = f'interest_receivable_cryptocurrency_{token_suffix}'
            credit_account = f'interest_income_cryptocurrency_{token_suffix}'
        else:
            fund_id = self._get_fund_id(borrower)
            wallet_id = self._get_wallet_id(borrower)
            transaction_type = 'expense_interest_accruals'
            debit_account = f'interest_expense_cryptocurrency_{token_suffix}'
            credit_account = f'interest_payable_cryptocurrency_{token_suffix}'

        # Common metadata
        common = {
            'transaction_type': transaction_type,
            'platform': PLATFORM,
            'fund_id': fund_id,
            'counterparty_fund_id': '',
            'wallet_id': wallet_id,
            'cryptocurrency': symbol,
            'hash': loan_row.get('tx_hash', loan_row.get('transactionHash', '')),
            'loan_id': loan_id,
            'lender': lender,
            'borrower': borrower,
            'contract_address': '',
            'payable_currency': payable_currency,
            'collateral_address': str(loan_row.get('collateralAddress', '')).lower(),
            'token_id': str(loan_row.get('collateralId', '')),
        }

        # Generate daily slices with Wei precision
        entries = []
        total_secs = int((end_dt - start_dt).total_seconds())
        if total_secs <= 0:
            return []

        leftover = 0
        assigned = 0
        cursor = start_dt

        while cursor < end_dt:
            # Calculate next midnight
            tomorrow = cursor.date() + timedelta(days=1)
            next_midnight = datetime.combine(tomorrow, dt_time(0, 0, 0)).replace(tzinfo=cursor.tzinfo)

            # Segment ends at midnight or loan end
            segment_end = min(next_midnight, end_dt)
            slice_secs = int((segment_end - cursor).total_seconds())

            # Wei-precise allocation
            numer = (total_interest_wei * slice_secs) + leftover
            slice_wei = numer // total_secs
            leftover = numer % total_secs
            assigned += slice_wei

            # Timestamp for this accrual entry
            accrual_label = (next_midnight - timedelta(seconds=1)) if segment_end == next_midnight else end_dt

            # Convert to human-readable
            slice_human = Decimal(slice_wei) / Decimal(10 ** decimals)

            # Debit entry
            entries.append({
                **common,
                'date': accrual_label,
                'event': 'InterestAccrual',
                'account_name': debit_account,
                'debit': slice_human,
                'credit': Decimal(0),
            })

            # Credit entry
            entries.append({
                **common,
                'date': accrual_label,
                'event': 'InterestAccrual',
                'account_name': credit_account,
                'debit': Decimal(0),
                'credit': slice_human,
            })

            cursor = segment_end

        # Final adjustment for any remaining Wei
        if assigned < total_interest_wei and entries:
            shortfall = Decimal(total_interest_wei - assigned) / Decimal(10 ** decimals)
            entries[-2]['debit'] += shortfall
            entries[-1]['credit'] += shortfall

        return entries


# ============================================================================
# ETH/USD PRICING
# ============================================================================

@lru_cache(maxsize=10000)
def get_eth_usd_price_at_block(w3: Web3, block_number: int) -> Decimal:
    """Get ETH/USD price at a specific block using Chainlink."""
    try:
        aggregator = w3.eth.contract(
            address=Web3.to_checksum_address(CHAINLINK_ETH_USD_FEED),
            abi=CHAINLINK_AGGREGATOR_ABI
        )
        round_data = aggregator.functions.latestRoundData().call(block_identifier=block_number)
        price = Decimal(round_data[1]) / Decimal(10**8)
        return price
    except Exception as e:
        print(f"[\!] Could not fetch ETH/USD price at block {block_number}: {e}")
        return Decimal("3000")


def add_eth_usd_prices(
    df: pd.DataFrame,
    w3: Web3,
) -> pd.DataFrame:
    """Add ETH/USD prices to journal entries based on block number."""
    if df.empty or 'hash' not in df.columns:
        return df

    df = df.copy()

    # Get unique hashes and their block numbers
    unique_hashes = df['hash'].unique()
    hash_to_price = {}

    for tx_hash in tqdm(unique_hashes, desc="Fetching ETH/USD prices", colour="yellow"):
        try:
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            price = get_eth_usd_price_at_block(w3, receipt['blockNumber'])
            hash_to_price[tx_hash] = price
        except Exception as e:
            hash_to_price[tx_hash] = Decimal("3000")

    # Add price column
    df['eth_usd_price'] = df['hash'].map(hash_to_price).apply(
        lambda x: x if isinstance(x, Decimal) else Decimal("3000")
    )

    # Calculate USD values
    df['debit_USD'] = df.apply(
        lambda row: row.get('debit', Decimal(0)) * row['eth_usd_price']
        if row.get('cryptocurrency', '').upper() in ['WETH', 'ETH']
        else row.get('debit', Decimal(0)),
        axis=1
    )
    df['credit_USD'] = df.apply(
        lambda row: row.get('credit', Decimal(0)) * row['eth_usd_price']
        if row.get('cryptocurrency', '').upper() in ['WETH', 'ETH']
        else row.get('credit', Decimal(0)),
        axis=1
    )

    return df


# ============================================================================
# GAS FEE HANDLING
# ============================================================================

def process_gas_fees(
    tx_hashes: List[str],
    w3: Web3,
    wallet_metadata: Dict[str, Dict],
    fund_wallet_list: List[str],
    max_workers: int = 8,
) -> pd.DataFrame:
    """Process gas fees for transactions."""

    def get_wallet_info(address: str) -> Dict:
        return wallet_metadata.get(address.lower(), {})

    def process_single_tx(tx_hash: str) -> List[Dict]:
        try:
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            tx = w3.eth.get_transaction(tx_hash)
            block = w3.eth.get_block(receipt['blockNumber'])

            payer_addr = tx['from'].lower()

            # Only track if payer is fund wallet
            if payer_addr not in fund_wallet_list:
                return []

            # Calculate gas cost
            gas_used = receipt['gasUsed']
            gas_price = receipt.get('effectiveGasPrice', tx['gasPrice'])
            gas_cost_wei = gas_used * gas_price
            gas_cost_eth = Decimal(gas_cost_wei) / WAD

            if gas_cost_eth <= 0:
                return []

            tx_datetime = datetime.fromtimestamp(block['timestamp'], tz=timezone.utc)
            payer_info = get_wallet_info(payer_addr)

            common = {
                'date': tx_datetime,
                'transaction_type': 'expense_gas_fees',
                'platform': PLATFORM,
                'fund_id': payer_info.get('fund_id', ''),
                'counterparty_fund_id': '',
                'wallet_id': payer_info.get('wallet_id', payer_addr),
                'cryptocurrency': 'ETH',
                'event': 'GasFee',
                'hash': tx_hash,
                'from': payer_addr,
                'to': tx['to'].lower() if tx['to'] else '',
            }

            entries = []

            # Dr gas expense
            entries.append({
                **common,
                'account_name': 'expense_gas_fees',
                'debit': gas_cost_eth,
                'credit': Decimal(0),
            })

            # Cr cash
            entries.append({
                **common,
                'account_name': 'deemed_cash_eth',
                'debit': Decimal(0),
                'credit': gas_cost_eth,
            })

            return entries
        except Exception as e:
            print(f"[\!] Error processing gas for {tx_hash}: {e}")
            return []

    all_entries = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_hash = {
            executor.submit(process_single_tx, tx_hash): tx_hash
            for tx_hash in tx_hashes
        }

        for future in tqdm(
            as_completed(future_to_hash),
            total=len(tx_hashes),
            desc="Processing gas fees",
            colour="magenta"
        ):
            entries = future.result()
            all_entries.extend(entries)

    return pd.DataFrame(all_entries) if all_entries else pd.DataFrame()


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_fund_wallets_from_metadata(
    wallet_metadata: Dict[str, Dict],
    category: str = 'fund',
) -> List[str]:
    """
    Extract wallet addresses where category matches the specified value.

    Args:
        wallet_metadata: Dict mapping addresses to wallet info
        category: Category to filter by (default: 'fund')

    Returns:
        List of wallet addresses (lowercase) matching the category
    """
    return [
        addr.lower() for addr, info in wallet_metadata.items()
        if str(info.get('category', '')).lower() == category.lower()
    ]


# ============================================================================
# VALIDATION & RECONCILIATION
# ============================================================================

def validate_journal_balance(journal_df: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
    """Validate that debits equal credits for each transaction."""
    if journal_df.empty:
        return True, pd.DataFrame()

    debit_col = 'debit' if 'debit' in journal_df.columns else 'debit'
    credit_col = 'credit' if 'credit' in journal_df.columns else 'credit'

    if 'hash' not in journal_df.columns:
        return True, pd.DataFrame()

    summary = journal_df.groupby('hash').agg({
        debit_col: 'sum',
        credit_col: 'sum'
    }).reset_index()

    summary['difference'] = summary[debit_col] - summary[credit_col]
    summary['is_balanced'] = abs(summary['difference']) < Decimal('0.00000001')

    all_balanced = summary['is_balanced'].all()

    if not all_balanced:
        unbalanced = summary[~summary['is_balanced']]
        print(f"[\!] Found {len(unbalanced)} unbalanced entries:")
        for _, row in unbalanced.iterrows():
            print(f"   {row['hash'][:16]}... diff={row['difference']}")

    return all_balanced, summary


# ============================================================================
# MAIN PROCESSOR
# ============================================================================

class ArcadeLoanCoreProcessor:
    """
    Main processor for Arcade LoanCore transactions.

    Complete pipeline:
    1. Decode events from transaction hashes
    2. Generate journal entries for each event type
    3. Generate interest accruals
    4. Add ETH/USD pricing
    5. Process gas fees
    6. Validate and reconcile
    """

    def __init__(
        self,
        w3: Web3,
        wallet_metadata: Dict[str, Dict],
        loancore_address: str = None,
    ):
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        # Only include wallets where category == "fund"
        self.fund_wallet_list = [
            addr for addr, info in self.wallet_metadata.items()
            if str(info.get('category', '')).lower() == 'fund'
        ]

        # Initialize decoder
        self.decoder = ArcadeEventDecoder(
            w3=w3,
            wallet_metadata=wallet_metadata,
            loancore_address=loancore_address,
        )

        # Store loancore address
        self.loancore_address = self.decoder.loancore_address

        # Initialize journal generator
        self.journal_generator = ArcadeJournalEntryGenerator(
            wallet_metadata=wallet_metadata,
        )

        print(f"[OK] Arcade Processor initialized")
        print(f"   Total wallets in metadata: {len(self.wallet_metadata)}")
        print(f"   Fund wallets (category='fund'): {len(self.fund_wallet_list)}")

    def process_all(
        self,
        tx_hashes: List[str],
        max_workers: int = 8,
        cutoff_date: datetime = None,
        include_gas_fees: bool = True,
        include_eth_usd_prices: bool = True,
        debug: bool = False,
    ) -> Dict[str, pd.DataFrame]:
        """
        Full processing pipeline.

        ROLLOVER HANDLING:
        ==================
        Rollovers are NOT treated as special events. Instead:
        - LoanRepaid is processed if fund owned the old loan -> payoff entries
        - LoanStarted is processed if fund is new lender -> origination entries
        - If fund is both old and new lender, both entries are generated
        - Cash flows net out correctly (payoff DR - origination CR = net cash)

        Rollover detection (presence of LoanRepaid + LoanStarted in same tx) is only
        used for informational purposes and accrual cutoff determination.

        Args:
            tx_hashes: List of transaction hashes to process
            max_workers: Number of parallel workers for decoding
            cutoff_date: End date for interest accruals (defaults to last tx date)
            include_gas_fees: Whether to add gas fee journal entries
            include_eth_usd_prices: Whether to add ETH/USD price lookup
            debug: Enable debug logging for event decoding/filtering

        Returns dict with all DataFrames:
        - events: Raw decoded events
        - journal_new_loans: LoanStarted entries (includes rollovers where fund is new lender)
        - journal_repayments: LoanRepaid/ForceRepay entries (includes rollovers where fund owned old loan)
        - journal_foreclosures: LoanClaimed entries
        - journal_income_accruals: Daily interest income
        - journal_expense_accruals: Daily interest expense
        - journal_gas_fees: Gas fee entries
        - journal_all_activity: Combined non-accrual entries
        """
        print(f"\n{'='*80}")
        print(f"📊 ARCADE FULL PROCESSING PIPELINE")
        print(f"{'='*80}")
        print(f"   Transactions: {len(tx_hashes)}")
        if debug:
            print(f"   Debug mode: ENABLED")

        # 1. Decode all events (excluding LoanRolledOver - we use LoanRepaid/LoanStarted instead)
        df_events = self.decoder.decode_batch(
            tx_hashes=tx_hashes,
            relevant_events=['LoanStarted', 'LoanRepaid', 'ForceRepay', 'LoanClaimed'],
            max_workers=max_workers,
            filter_fund_wallets=True,
            debug=debug,
        )
        print(f"[OK] Decoded {len(df_events)} events")

        if df_events.empty:
            print("[\!] No fund wallet events found")
            return {'events': pd.DataFrame()}

        # 2. Split by event type - NO EXCLUSIONS
        # Process ALL LoanStarted and LoanRepaid events, including those in rollovers
        # The accounting will work correctly because:
        # - LoanRepaid: DR deemed_cash (payoff), CR loan_receivable
        # - LoanStarted: DR loan_receivable, CR deemed_cash
        # Net cash for self-rollover = payoff - new_principal

        df_new_loans = df_events[df_events['event'] == 'LoanStarted']
        df_repayments = df_events[df_events['event'].isin(['LoanRepaid', 'ForceRepay'])]
        df_foreclosures = df_events[df_events['event'] == 'LoanClaimed']

        # Detect rollovers for informational purposes
        # A rollover = same tx has both LoanRepaid and LoanStarted
        if not df_repayments.empty and not df_new_loans.empty:
            repaid_tx_hashes = set(df_repayments['tx_hash'].dropna().tolist())
            started_tx_hashes = set(df_new_loans['tx_hash'].dropna().tolist())
            rollover_tx_hashes = repaid_tx_hashes & started_tx_hashes
            rollover_count = len(rollover_tx_hashes)
        else:
            rollover_tx_hashes = set()
            rollover_count = 0

        print(f"   New loans: {len(df_new_loans)}")
        print(f"   Repayments: {len(df_repayments)}")
        print(f"   Foreclosures: {len(df_foreclosures)}")
        if rollover_count > 0:
            print(f"   (Including {rollover_count} rollover transactions)")

        # 3. Generate journal entries
        # Each event type is processed based on fund's role:
        # - LoanStarted: Only if fund is lender
        # - LoanRepaid: Only if fund owned the loan
        journal_new_loans = self.journal_generator.generate_loan_started_entries(df_new_loans)
        journal_repayments = self.journal_generator.generate_loan_repaid_entries(df_repayments)
        journal_foreclosures = self.journal_generator.generate_loan_claimed_entries(df_foreclosures)

        print(f"[OK] Generated {len(journal_new_loans)} new loan JEs")
        print(f"[OK] Generated {len(journal_repayments)} repayment JEs")
        print(f"[OK] Generated {len(journal_foreclosures)} foreclosure JEs")

        # 4. Generate interest accruals
        # Closures include all repayments and foreclosures
        # For rollovers, the old loan closure stops accruals, new loan starts new accruals
        df_closures = pd.concat([df_repayments, df_foreclosures], ignore_index=True)

        income_accruals = self.journal_generator.generate_interest_income_accruals(
            df_new_loans, df_closures, cutoff_date
        )
        expense_accruals = self.journal_generator.generate_interest_expense_accruals(
            df_new_loans, df_closures, cutoff_date
        )

        print(f"[OK] Generated {len(income_accruals)} income accrual JEs")
        print(f"[OK] Generated {len(expense_accruals)} expense accrual JEs")

        # 5. Process gas fees
        df_gas = pd.DataFrame()
        if include_gas_fees:
            df_gas = process_gas_fees(
                tx_hashes, self.w3, self.wallet_metadata, self.fund_wallet_list, max_workers
            )
            print(f"[OK] Generated {len(df_gas)} gas fee JEs")

        # 6. Add ETH/USD prices
        if include_eth_usd_prices:
            if not journal_new_loans.empty:
                journal_new_loans = add_eth_usd_prices(journal_new_loans, self.w3)
            if not journal_repayments.empty:
                journal_repayments = add_eth_usd_prices(journal_repayments, self.w3)
            if not journal_foreclosures.empty:
                journal_foreclosures = add_eth_usd_prices(journal_foreclosures, self.w3)
            if not income_accruals.empty:
                income_accruals = add_eth_usd_prices(income_accruals, self.w3)
            if not expense_accruals.empty:
                expense_accruals = add_eth_usd_prices(expense_accruals, self.w3)
            if not df_gas.empty:
                df_gas = add_eth_usd_prices(df_gas, self.w3)
            print(f"[OK] Added ETH/USD prices")

        # 7. Combine activity (non-accrual)
        all_activity = pd.concat([
            journal_new_loans, journal_repayments, journal_foreclosures, df_gas
        ], ignore_index=True)

        # 8. Validate
        if not all_activity.empty:
            is_balanced, _ = validate_journal_balance(all_activity)
            print(f"[OK] Balance validation: {'PASSED' if is_balanced else 'FAILED'}")

        return {
            'events': df_events,
            'journal_new_loans': journal_new_loans,
            'journal_repayments': journal_repayments,
            'journal_foreclosures': journal_foreclosures,
            'journal_income_accruals': income_accruals,
            'journal_expense_accruals': expense_accruals,
            'journal_gas_fees': df_gas,
            'journal_all_activity': all_activity,
            'rollover_tx_hashes': list(rollover_tx_hashes),  # For debugging/reference
        }

    def save_outputs(
        self,
        results: Dict[str, pd.DataFrame],
        output_dir: str,
        period: str = None,
    ):
        """Save all outputs to files."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        prefix = f"{PLATFORM}_{period}_" if period else f"{PLATFORM}_"

        for name, value in results.items():
            # Skip non-DataFrame items (like rollover_tx_hashes list)
            if not isinstance(value, pd.DataFrame):
                continue

            if value.empty:
                continue

            csv_path = output_path / f"{prefix}{name}.csv"
            value.to_csv(csv_path, index=False)
            print(f"💾 Saved: {csv_path.name}")


# ============================================================================
# STANDARD COLUMN ORDER
# ============================================================================

def get_standard_column_order() -> List[str]:
    """Get standard column order for journal entries."""
    return [
        "date", "transaction_type", "platform", "fund_id", "counterparty_fund_id",
        "wallet_id", "cryptocurrency", "account_name", "debit", "credit",
        "eth_usd_price", "debit_USD", "credit_USD", "event", "hash", "loan_id",
        "lender", "borrower", "from", "to", "contract_address", "payable_currency",
        "collateral_address", "token_id", "principal", "principal_USD",
        "annual_interest_rate", "payoff_amount", "payoff_amount_USD", "loan_due_date",
    ]


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("ARCADE LOANCORE DECODER - COMPLETE ONE-SHOT SOLUTION")
    print("=" * 80)
    print()
    print("Contract Architecture:")
    print(f"  OriginationController: {ARCADE_ORIGINATION_CONTROLLER}")
    print(f"  RepaymentController: {ARCADE_REPAYMENT_CONTROLLER}")
    print()
    print("Events Supported:")
    print("  [OK] LoanStarted (new loans)")
    print("  [OK] LoanRepaid (normal repayment)")
    print("  [OK] ForceRepay (deferred receipt)")
    print("  [OK] LoanClaimed (foreclosure)")
    print("  [OK] LoanRolledOver (atomic refinance)")
    print()
    print("Key Features:")
    print("  [OK] Exact Solidity interest calculation match")
    print("  [OK] Fee deductions (lenderInterestFee, lenderPrincipalFee, lenderDefaultFee)")
    print("  [OK] Wei-precise daily interest accruals")
    print("  [OK] ETH/USD pricing via Chainlink")
    print("  [OK] Gas fee handling")
    print("  [OK] Rollover net settlement accounting")
    print("  [OK] Balance validation")
    print()
    print("Usage:")
    print("""
    from arcade_loancore_decoder import ArcadeLoanCoreProcessor

    # Initialize
    processor = ArcadeLoanCoreProcessor(
        w3=w3,
        wallet_metadata=wallet_metadata,
        loancore_address="0x..."  # Optional - auto-detected
    )

    # Process transactions
    results = processor.process_all(
        tx_hashes=tx_hashes,
        cutoff_date=datetime(2025, 7, 31, tzinfo=timezone.utc),
        include_gas_fees=True,
        include_eth_usd_prices=True,
    )

    # Save outputs
    processor.save_outputs(results, "/path/to/output", period="202507")

    # Access DataFrames
    events = results['events']
    new_loans = results['journal_new_loans']
    repayments = results['journal_repayments']
    foreclosures = results['journal_foreclosures']
    rollovers = results['journal_rollovers']
    income_accruals = results['journal_income_accruals']
    expense_accruals = results['journal_expense_accruals']
    """)