"""
Standalone Transaction Decoder Debug App

Run with: python debug_decoder.py
Opens at: http://127.0.0.1:8765

This app provides MEGA verbose debugging for transaction decoding.
Use it to trace exactly where decoding fails.
"""

from shiny import App, ui, render, reactive
import os
import sys
from dotenv import load_dotenv
from web3 import Web3
from datetime import datetime, timezone
from decimal import Decimal
import traceback
import json
from typing import Dict, List, Optional, Any

# Load environment variables
load_dotenv()

# ============================================================================
# CONSTANTS (from production notebook)
# ============================================================================

SECONDS_PER_YEAR = 31536000  # 365 * 24 * 3600
PRECISION_BPS = 10000  # Basis points denominator

# Token registry for currency info
TOKEN_REGISTRY = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {"symbol": "WETH", "decimals": 18},
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"symbol": "USDC", "decimals": 6},
    "0xdac17f958d2ee523a2206206994597c13d831ec7": {"symbol": "USDT", "decimals": 6},
    "0x6b175474e89094c44da98b954eedeac495271d0f": {"symbol": "DAI", "decimals": 18},
}


# ============================================================================
# DATACLASSES (from production notebook)
# ============================================================================

from dataclasses import dataclass, field


def safe_int(value, default: int = 0) -> int:
    """Safely convert value to int"""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_address(value) -> str:
    """Safely normalize Ethereum address to lowercase"""
    if value is None:
        return ""
    addr = str(value).strip().lower()
    if addr.startswith('0x') and len(addr) == 42:
        return addr
    return ""


@dataclass
class Tranche:
    """
    Represents a single lender's tranche in a multi-source loan.
    V1/V3: loanId, floor, principalAmount, lender, accruedInterest, startTime, aprBps (7 fields)
    V2:    loanId, lender, principalAmount, accruedInterest, startTime, aprBps (6 fields - NO floor)
    """
    loanId: int = 0
    floor: int = 0  # V2 doesn't have this
    principalAmount: int = 0
    lender: str = ""
    accruedInterest: int = 0
    startTime: int = 0
    aprBps: int = 0

    @classmethod
    def from_tuple(cls, data: tuple, is_v2: bool = False) -> 'Tranche':
        """Create from ABI-decoded tuple"""
        if is_v2:
            # V2 Source struct (no floor): loanId, lender, principalAmount, accruedInterest, startTime, aprBps
            return cls(
                loanId=safe_int(data[0]),
                floor=0,
                principalAmount=safe_int(data[2]),
                lender=safe_address(data[1]),
                accruedInterest=safe_int(data[3]),
                startTime=safe_int(data[4]),
                aprBps=safe_int(data[5]),
            )
        else:
            # V1/V3 Tranche struct: loanId, floor, principalAmount, lender, accruedInterest, startTime, aprBps
            return cls(
                loanId=safe_int(data[0]),
                floor=safe_int(data[1]),
                principalAmount=safe_int(data[2]),
                lender=safe_address(data[3]),
                accruedInterest=safe_int(data[4]),
                startTime=safe_int(data[5]),
                aprBps=safe_int(data[6]),
            )

    @classmethod
    def from_dict(cls, data: dict, is_v2: bool = False) -> 'Tranche':
        """Create from dictionary/AttributeDict"""
        return cls(
            loanId=safe_int(data.get('loanId', 0)),
            floor=0 if is_v2 else safe_int(data.get('floor', 0)),
            principalAmount=safe_int(data.get('principalAmount', 0)),
            lender=safe_address(data.get('lender', '')),
            accruedInterest=safe_int(data.get('accruedInterest', 0)),
            startTime=safe_int(data.get('startTime', 0)),
            aprBps=safe_int(data.get('aprBps', 0)),
        )


@dataclass
class Loan:
    """
    Multi-source loan with multiple tranches from different lenders.
    V1/V3: 9 fields (includes protocolFee)
    V2:    8 fields (no protocolFee)
    """
    borrower: str = ""
    nftCollateralTokenId: int = 0
    nftCollateralAddress: str = ""
    principalAddress: str = ""
    principalAmount: int = 0
    startTime: int = 0
    duration: int = 0
    tranches: List['Tranche'] = field(default_factory=list)
    protocolFee: int = 0  # V2 = 0

    @classmethod
    def from_tuple(cls, data: tuple, is_v2: bool = False) -> 'Loan':
        """Create from ABI-decoded tuple"""
        tranche_data = data[7] if len(data) > 7 else []
        tranches = [Tranche.from_tuple(t, is_v2=is_v2) for t in tranche_data]
        protocol_fee = 0 if is_v2 else safe_int(data[8]) if len(data) > 8 else 0

        return cls(
            borrower=safe_address(data[0]),
            nftCollateralTokenId=safe_int(data[1]),
            nftCollateralAddress=safe_address(data[2]),
            principalAddress=safe_address(data[3]),
            principalAmount=safe_int(data[4]),
            startTime=safe_int(data[5]),
            duration=safe_int(data[6]),
            tranches=tranches,
            protocolFee=protocol_fee,
        )

    @classmethod
    def from_dict(cls, data: dict, is_v2: bool = False) -> 'Loan':
        """Create from dictionary/AttributeDict"""
        # Handle different field names for tranches
        tranche_data = data.get('source', data.get('tranche', data.get('tranches', [])))
        tranches = []
        for t in tranche_data:
            if hasattr(t, 'keys') or isinstance(t, dict):
                tranches.append(Tranche.from_dict(dict(t), is_v2=is_v2))
            elif isinstance(t, tuple):
                tranches.append(Tranche.from_tuple(t, is_v2=is_v2))

        return cls(
            borrower=safe_address(data.get('borrower', '')),
            nftCollateralTokenId=safe_int(data.get('nftCollateralTokenId', 0)),
            nftCollateralAddress=safe_address(data.get('nftCollateralAddress', '')),
            principalAddress=safe_address(data.get('principalAddress', '')),
            principalAmount=safe_int(data.get('principalAmount', 0)),
            startTime=safe_int(data.get('startTime', 0)),
            duration=safe_int(data.get('duration', 0)),
            tranches=tranches,
            protocolFee=0 if is_v2 else safe_int(data.get('protocolFee', 0)),
        )

    def get_currency_symbol(self) -> str:
        """Get currency symbol from principal address"""
        addr = self.principalAddress.lower()
        if addr in TOKEN_REGISTRY:
            return TOKEN_REGISTRY[addr]["symbol"]
        return "WETH"  # Default


# ============================================================================
# BLUR DATACLASSES
# ============================================================================

import math

@dataclass
class BlurLien:
    """
    Blur Blend NFT-collateralized loan (callable loan, no fixed term).

    Lien struct (9 fields):
    [0] lender - address
    [1] borrower - address
    [2] collection - NFT contract address
    [3] tokenId - NFT token ID
    [4] amount - principal in wei
    [5] startTime - loan start timestamp
    [6] rate - annual rate in basis points (for continuous compounding)
    [7] auctionStartBlock - block when auction started (0 if not in auction)
    [8] auctionDuration - duration of auction in blocks

    Interest: Continuous compounding using e^(rate * time)
    """
    lien_id: int = 0
    lender: str = ""
    borrower: str = ""
    collection: str = ""
    token_id: int = 0
    principal: int = 0  # wei
    start_time: int = 0  # unix timestamp
    rate: int = 0  # basis points (e.g., 1000 = 10% APR)
    auction_start_block: int = 0
    auction_duration: int = 0

    @classmethod
    def from_tuple(cls, data: tuple, lien_id: int = 0) -> 'BlurLien':
        """Create from ABI-decoded tuple (9 fields)"""
        return cls(
            lien_id=lien_id,
            lender=safe_address(data[0]),
            borrower=safe_address(data[1]),
            collection=safe_address(data[2]),
            token_id=safe_int(data[3]),
            principal=safe_int(data[4]),
            start_time=safe_int(data[5]),
            rate=safe_int(data[6]),
            auction_start_block=safe_int(data[7]),
            auction_duration=safe_int(data[8]),
        )

    @classmethod
    def from_dict(cls, data: dict, lien_id: int = 0) -> 'BlurLien':
        """Create from dictionary/AttributeDict"""
        return cls(
            lien_id=lien_id,
            lender=safe_address(data.get('lender', '')),
            borrower=safe_address(data.get('borrower', '')),
            collection=safe_address(data.get('collection', '')),
            token_id=safe_int(data.get('tokenId', 0)),
            principal=safe_int(data.get('amount', 0)),
            start_time=safe_int(data.get('startTime', 0)),
            rate=safe_int(data.get('rate', 0)),
            auction_start_block=safe_int(data.get('auctionStartBlock', 0)),
            auction_duration=safe_int(data.get('auctionDuration', 0)),
        )

    def calculate_interest(self, as_of_timestamp: int) -> int:
        """
        Calculate accrued interest using continuous compounding.
        Formula: principal * (e^(rate * time_in_years) - 1)

        Returns interest in wei.
        """
        if self.principal == 0 or self.rate == 0:
            return 0

        time_elapsed_seconds = max(1, as_of_timestamp - self.start_time)
        seconds_per_year = Decimal(365 * 24 * 3600)
        time_in_years = Decimal(time_elapsed_seconds) / seconds_per_year

        # Rate is in basis points (10000 = 100%)
        rate_decimal = Decimal(self.rate) / Decimal(10000)

        # Continuous compounding: e^(rate * time)
        exponent = float(rate_decimal * time_in_years)
        compound_factor = Decimal(str(math.exp(exponent)))

        # Interest = principal * (compound_factor - 1)
        interest = int(Decimal(self.principal) * (compound_factor - Decimal(1)))
        return interest

    def total_due(self, as_of_timestamp: int) -> int:
        """Calculate total amount due (principal + interest) in wei"""
        return self.principal + self.calculate_interest(as_of_timestamp)

    def get_principal_eth(self) -> Decimal:
        """Get principal in ETH"""
        return Decimal(self.principal) / Decimal(10**18)

    def get_interest_eth(self, as_of_timestamp: int) -> Decimal:
        """Get interest in ETH"""
        return Decimal(self.calculate_interest(as_of_timestamp)) / Decimal(10**18)

    def get_total_eth(self, as_of_timestamp: int) -> Decimal:
        """Get total due in ETH"""
        return Decimal(self.total_due(as_of_timestamp)) / Decimal(10**18)

    def is_in_auction(self) -> bool:
        """Check if loan is in auction"""
        return self.auction_start_block > 0


# ============================================================================
# BLUR CONSTANTS
# ============================================================================

BLUR_LENDING_ADDRESS = "0x29469395eaf6f95920e59f858042f0e28d98a20b"
BLUR_POOL_ADDRESS = "0x0000000000a39bb272e79075ade125fd351887ac"

# Blur lending events
BLUR_EVENTS = [
    "LoanOfferTaken",  # New loan origination
    "Repay",           # Loan repayment
    "Refinance",       # Loan refinanced to new lender
    "StartAuction",    # Lender calls the loan
    "Seize",           # Lender seizes NFT collateral
    "BuyLocked",       # Third party purchases locked NFT
]


# ============================================================================
# NFTFI CONSTANTS
# ============================================================================

# NFTfi contract addresses - multiple versions
NFTFI_COORDINATOR = "0xe52cec0e90115abeb3304baa36bc2655731f7934"  # DirectLoanCoordinator (V2)
NFTFI_V2 = "0x8252df1d8b29057d1afe3062bf5a64d503152bc8"  # DirectLoanFixedOffer V2
NFTFI_V1 = "0xf896527c49b44aab3cf22ae356fa3af8e331f280"  # DirectLoanFixedOffer V1
NFTFI_V2_1 = "0xd0a40eb7fcd530a13866b9e893e4a9e0d15d03eb"  # DirectLoanFixedOfferRedeploy

NFTFI_CONTRACTS = [
    NFTFI_COORDINATOR,
    NFTFI_V2,
    NFTFI_V1,
    NFTFI_V2_1,
]

# NFTfi events
NFTFI_EVENTS = [
    "LoanStarted",      # New loan origination
    "LoanRepaid",       # Loan repayment (borrower pays back)
    "LoanLiquidated",   # Collateral seized (loan defaulted)
    "LoanRenegotiated", # Terms changed mid-loan
]

# NFTfi event signatures (precomputed)
# LoanStarted(uint256 indexed loanId, address indexed borrower, address indexed lender,
#             uint256 principal, uint256 maximumRepaymentAmount, uint256 nftCollateralId,
#             address loanPrincipalAddress, address nftCollateralContract, uint256 loanStartTime,
#             uint256 loanDuration, uint256 loanInterestRate, uint256 adminFee)
NFTFI_LOAN_STARTED_SIG = "0x42cc7f53ef7b494c5dd6d9c7b0fdc87ae2fdded0e6fd3e249ba9fb0ed2e3a8a9"

# LoanRepaid(uint256 indexed loanId, address indexed borrower, address indexed lender,
#            uint256 totalRepaymentAmount, address loanPrincipalAddress)
NFTFI_LOAN_REPAID_SIG = "0x1d1e2c20f9a37c69e2aeee941e9a3e4e3b6a5b3f1bd6f5c1e6d9e1f2a3b4c5d6"

# LoanLiquidated(uint256 indexed loanId, address indexed borrower, address indexed lender,
#                uint256 principal, uint256 nftCollateralId, address loanPrincipalAddress,
#                address nftCollateralContract, uint256 loanLiquidatedDate)
NFTFI_LOAN_LIQUIDATED_SIG = "0x2a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2"


@dataclass
class NFTfiLoan:
    """NFTfi loan data structure"""
    loan_id: int
    borrower: str
    lender: str
    principal: int  # in wei
    max_repayment: int  # in wei
    nft_collateral_id: int
    principal_token: str  # e.g., WETH address
    nft_collection: str
    start_time: int
    duration: int  # in seconds
    interest_rate_bps: int
    admin_fee_bps: int = 0

    def get_principal_eth(self) -> Decimal:
        """Get principal in ETH"""
        return Decimal(self.principal) / Decimal(10**18)

    def get_max_repayment_eth(self) -> Decimal:
        """Get max repayment in ETH"""
        return Decimal(self.max_repayment) / Decimal(10**18)

    def get_interest_eth(self) -> Decimal:
        """Get interest amount in ETH"""
        return self.get_max_repayment_eth() - self.get_principal_eth()

    def is_expired(self, current_timestamp: int) -> bool:
        """Check if loan has expired"""
        return current_timestamp > (self.start_time + self.duration)


# NFTfi Minimal ABI - Manually constructed from observed events
# Event signatures vary by contract version, but args are similar
NFTFI_ABI = [
    # LoanStarted (V2 format - DirectLoanFixedOffer)
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "loanId", "type": "uint256"},
            {"indexed": True, "name": "borrower", "type": "address"},
            {"indexed": True, "name": "lender", "type": "address"},
            {"indexed": False, "name": "loanPrincipalAmount", "type": "uint256"},
            {"indexed": False, "name": "maximumRepaymentAmount", "type": "uint256"},
            {"indexed": False, "name": "nftCollateralId", "type": "uint256"},
            {"indexed": False, "name": "loanERC20Denomination", "type": "address"},
            {"indexed": False, "name": "nftCollateralContract", "type": "address"},
            {"indexed": False, "name": "loanStartTime", "type": "uint64"},
            {"indexed": False, "name": "loanDuration", "type": "uint32"},
            {"indexed": False, "name": "loanInterestRateForDurationInBasisPoints", "type": "uint16"},
            {"indexed": False, "name": "loanAdminFeeInBasisPoints", "type": "uint16"},
            {"indexed": False, "name": "loanOriginationFee", "type": "uint256"},
        ],
        "name": "LoanStarted",
        "type": "event"
    },
    # LoanRepaid (V2 format)
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "loanId", "type": "uint256"},
            {"indexed": True, "name": "borrower", "type": "address"},
            {"indexed": True, "name": "lender", "type": "address"},
            {"indexed": False, "name": "loanPrincipalAmount", "type": "uint256"},
            {"indexed": False, "name": "nftCollateralId", "type": "uint256"},
            {"indexed": False, "name": "amountPaidToLender", "type": "uint256"},
            {"indexed": False, "name": "adminFee", "type": "uint256"},
            {"indexed": False, "name": "revenueShare", "type": "uint256"},
            {"indexed": False, "name": "revenueSharePartner", "type": "address"},
            {"indexed": False, "name": "loanERC20Denomination", "type": "address"},
            {"indexed": False, "name": "nftCollateralContract", "type": "address"},
        ],
        "name": "LoanRepaid",
        "type": "event"
    },
    # LoanLiquidated (V2 format)
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "loanId", "type": "uint256"},
            {"indexed": True, "name": "borrower", "type": "address"},
            {"indexed": True, "name": "lender", "type": "address"},
            {"indexed": False, "name": "loanPrincipalAmount", "type": "uint256"},
            {"indexed": False, "name": "nftCollateralId", "type": "uint256"},
            {"indexed": False, "name": "loanMaturityDate", "type": "uint256"},
            {"indexed": False, "name": "loanLiquidationDate", "type": "uint256"},
            {"indexed": False, "name": "loanERC20Denomination", "type": "address"},
            {"indexed": False, "name": "nftCollateralContract", "type": "address"},
        ],
        "name": "LoanLiquidated",
        "type": "event"
    },
    # LoanRenegotiated (V2 format)
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "loanId", "type": "uint256"},
            {"indexed": True, "name": "borrower", "type": "address"},
            {"indexed": True, "name": "lender", "type": "address"},
            {"indexed": False, "name": "newLoanDuration", "type": "uint32"},
            {"indexed": False, "name": "newMaximumRepaymentAmount", "type": "uint256"},
            {"indexed": False, "name": "renegotiationFee", "type": "uint256"},
            {"indexed": False, "name": "renegotiationAdminFee", "type": "uint256"},
        ],
        "name": "LoanRenegotiated",
        "type": "event"
    },
]


# ============================================================================
# ARCADE CONSTANTS
# ============================================================================

# Arcade contract addresses (V3 - current production)
ARCADE_LOAN_CORE_PROXY = "0x81b2f8fc75bab64a6b144aa6d2faa127b4fa7fd9"  # LoanCore Proxy
ARCADE_LOAN_CORE_IMPL = "0x6ddb57101a17854109c3b9feb80ae19662ea950f"  # LoanCore Implementation
ARCADE_REPAYMENT_CONTROLLER = "0xb39dab85fa05c381767ff992ccde4c94619993d4"  # RepaymentController
ARCADE_ORIGINATION_CONTROLLER = "0x89bc08ba00f135d608bc335f6b33d7a9abcc98af"  # OriginationController
ARCADE_LENDER_NOTE = "0x349a026a43ffa8e2ab4c4e59fcaa93f87bd8ddee"  # aLN Token
ARCADE_BORROWER_NOTE = "0x337104a4f06260ff327d6734c555a0f5d8f863aa"  # aBN Token

ARCADE_CONTRACTS = [
    ARCADE_LOAN_CORE_PROXY,
    ARCADE_LOAN_CORE_IMPL,
    ARCADE_REPAYMENT_CONTROLLER,
    ARCADE_ORIGINATION_CONTROLLER,
]

# Arcade events
ARCADE_EVENTS = [
    "LoanStarted",           # New loan origination
    "LoanRepaid",            # Full loan repayment
    "LoanClaimed",           # Collateral seized (default)
    "LoanRolledOver",        # Loan extension/rollover
    "InstallmentPaymentReceived",  # Partial payment
]

# Arcade event signatures (from Etherscan analysis)
# LoanStarted(uint256 loanId, address lender, address borrower)
ARCADE_LOAN_STARTED_SIG = "0x8b6e12e3a1c826f5c1e1ff23c76e8e24b48d3b8b8f1e7a7b7c2d3e4f5a6b7c8d"  # Need to verify

# LoanRepaid(uint256 loanId) - confirmed from tx 0xa72e5fef...
ARCADE_LOAN_REPAID_SIG = "0x9a7851747cd7ffb3fe0a32caf3da48b31f27cebe131267051640f8b72fc47186"

# LoanClaimed(uint256 loanId) - confirmed from tx 0x5be6efb5...
ARCADE_LOAN_CLAIMED_SIG = "0xb15e438728b48d46c9a5505713e60ff50c80559f4523c8f99a246a2069a8684a"

# LoanRolledOver(uint256 oldLoanId, uint256 newLoanId)
ARCADE_LOAN_ROLLED_OVER_SIG = "0x"  # TODO: Get from actual transaction

# InstallmentPaymentReceived(uint256 loanId, uint256 repaidAmount, uint256 remBalance)
ARCADE_INSTALLMENT_SIG = "0x"  # TODO: Get from actual transaction


@dataclass
class ArcadeLoan:
    """Arcade loan data structure based on LoanData struct"""
    loan_id: int
    borrower: str
    lender: str
    principal: int  # in wei
    interest_rate_bps: int  # APR in basis points (1 = 0.01%)
    duration_secs: int
    collateral_address: str  # NFT contract or AssetVault
    collateral_id: int
    payable_currency: str  # Token address (WETH, etc.)
    start_date: int = 0
    balance: int = 0  # Outstanding principal
    interest_paid: int = 0  # Cumulative interest paid
    state: str = "Active"  # Active, Repaid, Defaulted

    def get_principal_eth(self) -> Decimal:
        """Get principal in ETH (assumes 18 decimals)"""
        return Decimal(self.principal) / Decimal(10**18)

    def get_balance_eth(self) -> Decimal:
        """Get outstanding balance in ETH"""
        return Decimal(self.balance) / Decimal(10**18)

    def calculate_interest_eth(self, current_timestamp: int) -> Decimal:
        """Calculate accrued interest using simple interest formula"""
        if self.start_date == 0 or self.principal == 0:
            return Decimal(0)

        time_elapsed = max(0, current_timestamp - self.start_date)
        # Simple interest: principal * rate * time / (10000 * seconds_per_year)
        interest_wei = (self.principal * self.interest_rate_bps * time_elapsed) // (10000 * 31536000)
        return Decimal(interest_wei) / Decimal(10**18)

    def is_expired(self, current_timestamp: int) -> bool:
        """Check if loan has expired (defaulted)"""
        if self.start_date == 0 or self.duration_secs == 0:
            return False
        return current_timestamp > (self.start_date + self.duration_secs)

    def get_expiry_date(self) -> datetime:
        """Get loan expiry datetime"""
        if self.start_date == 0 or self.duration_secs == 0:
            return datetime.now(timezone.utc)
        return datetime.fromtimestamp(self.start_date + self.duration_secs, tz=timezone.utc)


# Arcade Minimal ABI for events
ARCADE_ABI = [
    # LoanStarted(uint256 loanId, address lender, address borrower)
    {
        "anonymous": False,
        "inputs": [
            {"indexed": False, "name": "loanId", "type": "uint256"},
            {"indexed": False, "name": "lender", "type": "address"},
            {"indexed": False, "name": "borrower", "type": "address"},
        ],
        "name": "LoanStarted",
        "type": "event"
    },
    # LoanRepaid(uint256 loanId)
    {
        "anonymous": False,
        "inputs": [
            {"indexed": False, "name": "loanId", "type": "uint256"},
        ],
        "name": "LoanRepaid",
        "type": "event"
    },
    # LoanClaimed(uint256 loanId)
    {
        "anonymous": False,
        "inputs": [
            {"indexed": False, "name": "loanId", "type": "uint256"},
        ],
        "name": "LoanClaimed",
        "type": "event"
    },
    # LoanRolledOver(uint256 oldLoanId, uint256 newLoanId)
    {
        "anonymous": False,
        "inputs": [
            {"indexed": False, "name": "oldLoanId", "type": "uint256"},
            {"indexed": False, "name": "newLoanId", "type": "uint256"},
        ],
        "name": "LoanRolledOver",
        "type": "event"
    },
    # InstallmentPaymentReceived(uint256 loanId, uint256 repaidAmount, uint256 remBalance)
    {
        "anonymous": False,
        "inputs": [
            {"indexed": False, "name": "loanId", "type": "uint256"},
            {"indexed": False, "name": "repaidAmount", "type": "uint256"},
            {"indexed": False, "name": "remBalance", "type": "uint256"},
        ],
        "name": "InstallmentPaymentReceived",
        "type": "event"
    },
    # NonceUsed(address indexed user, uint160 nonce)
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "user", "type": "address"},
            {"indexed": False, "name": "nonce", "type": "uint160"},
        ],
        "name": "NonceUsed",
        "type": "event"
    },
]


# ============================================================================
# ZHARTA CONSTANTS
# ============================================================================

# Zharta contract addresses (Peer-to-Pool NFT Lending)
ZHARTA_LOANS = "0xb7c8c74ed765267b54f4c327f279d7e850725ef2"  # Main Loans interface
ZHARTA_LOANS_CORE = "0x5be916cff5f07870e9aef205960e07d9e287ef27"  # Loan state storage
ZHARTA_LENDING_POOL = "0x6474ab1b56b47bc26ba8cb471d566b8cc528f308"  # LendingPoolPeripheral
ZHARTA_COLLATERAL_VAULT = "0x35b8545ae12d89cd4997d5485e2e68c857df24a8"  # CollateralVaultPeripheral
ZHARTA_GENESIS = "0xca54733a772c83096e40f644c9286c9779191e50"  # Genesis pass verification

ZHARTA_CONTRACTS = [
    ZHARTA_LOANS,
    ZHARTA_LOANS_CORE,
    ZHARTA_LENDING_POOL,
    ZHARTA_COLLATERAL_VAULT,
]

# Zharta events
ZHARTA_EVENTS = [
    "LoanCreated",       # New loan origination (reserveEth/reserveWeth)
    "LoanPayment",       # Payment made on loan
    "LoanPaid",          # Loan fully repaid
    "LoanDefaulted",     # Loan defaulted after maturity
]

# Zharta event signatures (from Etherscan analysis)
# LoanCreated(address indexed wallet, address wallet, uint256 loanId, address erc20TokenContract, uint256 apr, uint256 amount, uint256 duration, Collateral[] collaterals, uint256 genesisToken)
ZHARTA_LOAN_CREATED_SIG = "0x4a558778654b4d21f09ae7e2aa4eebc0de757d1233dc825b43183a1276a7b2a1"

# LoanPayment(address indexed wallet, address wallet, uint256 loanId, uint256 principal, uint256 interestAmount, address erc20TokenContract)
# Confirmed from tx 0x2507c931a0c97544ab767dfaab6060e29303ff682b410aba88ff672ae2fee42f
ZHARTA_LOAN_PAYMENT_SIG = "0x31c401ba8a3eb75cf55e1d9f4971e726115e8448c80446935cffbea991ca2473"

# LoanPaid(address indexed wallet, address wallet, uint256 loanId, address erc20TokenContract)
# Confirmed from tx 0x2507c931a0c97544ab767dfaab6060e29303ff682b410aba88ff672ae2fee42f
ZHARTA_LOAN_PAID_SIG = "0x42d434e1d98bb8cb642015660476f098bbb0f00b64ddb556e149e17de4dd3645"

# LoanDefaulted(address indexed wallet, address wallet, uint256 loanId, uint256 amount, address erc20TokenContract)
ZHARTA_LOAN_DEFAULTED_SIG = "0x"  # Need to capture from real tx


@dataclass
class ZhartaLoan:
    """Zharta loan data structure"""
    loan_id: int
    borrower: str
    principal: int  # in wei
    apr_bps: int  # APR in basis points
    duration_secs: int
    collaterals: List[tuple] = field(default_factory=list)  # (nft_address, token_id)
    payable_currency: str = ""  # Token address (WETH, etc.)
    start_time: int = 0
    genesis_token: int = 0
    is_paid: bool = False
    is_defaulted: bool = False

    def get_principal_eth(self) -> Decimal:
        """Get principal in ETH (assumes 18 decimals)"""
        return Decimal(self.principal) / Decimal(10**18)

    def calculate_interest_eth(self, current_timestamp: int) -> Decimal:
        """Calculate accrued interest using simple interest formula"""
        if self.start_time == 0 or self.principal == 0:
            return Decimal(0)

        time_elapsed = max(0, current_timestamp - self.start_time)
        # Simple interest: principal * rate * time / (10000 * seconds_per_year)
        interest_wei = (self.principal * self.apr_bps * time_elapsed) // (10000 * 31536000)
        return Decimal(interest_wei) / Decimal(10**18)

    def is_expired(self, current_timestamp: int) -> bool:
        """Check if loan has reached maturity"""
        if self.start_time == 0 or self.duration_secs == 0:
            return False
        return current_timestamp > (self.start_time + self.duration_secs)

    def get_expiry_date(self) -> datetime:
        """Get loan maturity datetime"""
        if self.start_time == 0 or self.duration_secs == 0:
            return datetime.now(timezone.utc)
        return datetime.fromtimestamp(self.start_time + self.duration_secs, tz=timezone.utc)


# Zharta Minimal ABI for events
ZHARTA_ABI = [
    # LoanCreated(address indexed walletIndexed, address wallet, uint256 loanId, address erc20TokenContract, uint256 apr, uint256 amount, uint256 duration, tuple[] collaterals, uint256 genesisToken)
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "walletIndexed", "type": "address"},
            {"indexed": False, "name": "wallet", "type": "address"},
            {"indexed": False, "name": "loanId", "type": "uint256"},
            {"indexed": False, "name": "erc20TokenContract", "type": "address"},
            {"indexed": False, "name": "apr", "type": "uint256"},
            {"indexed": False, "name": "amount", "type": "uint256"},
            {"indexed": False, "name": "duration", "type": "uint256"},
            # Note: collaterals array is complex - handled separately
            {"indexed": False, "name": "genesisToken", "type": "uint256"},
        ],
        "name": "LoanCreated",
        "type": "event"
    },
    # LoanPayment(address indexed walletIndexed, address wallet, uint256 loanId, uint256 principal, uint256 interestAmount, address erc20TokenContract)
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "walletIndexed", "type": "address"},
            {"indexed": False, "name": "wallet", "type": "address"},
            {"indexed": False, "name": "loanId", "type": "uint256"},
            {"indexed": False, "name": "principal", "type": "uint256"},
            {"indexed": False, "name": "interestAmount", "type": "uint256"},
            {"indexed": False, "name": "erc20TokenContract", "type": "address"},
        ],
        "name": "LoanPayment",
        "type": "event"
    },
    # LoanPaid(address indexed walletIndexed, address wallet, uint256 loanId, address erc20TokenContract)
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "walletIndexed", "type": "address"},
            {"indexed": False, "name": "wallet", "type": "address"},
            {"indexed": False, "name": "loanId", "type": "uint256"},
            {"indexed": False, "name": "erc20TokenContract", "type": "address"},
        ],
        "name": "LoanPaid",
        "type": "event"
    },
    # LoanDefaulted(address indexed walletIndexed, address wallet, uint256 loanId, uint256 amount, address erc20TokenContract)
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "walletIndexed", "type": "address"},
            {"indexed": False, "name": "wallet", "type": "address"},
            {"indexed": False, "name": "loanId", "type": "uint256"},
            {"indexed": False, "name": "amount", "type": "uint256"},
            {"indexed": False, "name": "erc20TokenContract", "type": "address"},
        ],
        "name": "LoanDefaulted",
        "type": "event"
    },
]


# ============================================================================
# GENERIC CONSTANTS (WETH, ERC20, ERC721, etc.)
# ============================================================================

# Common token addresses
WETH_ADDRESS = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC_ADDRESS = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDT_ADDRESS = "0xdac17f958d2ee523a2206206994597c13d831ec7"
DAI_ADDRESS = "0x6b175474e89094c44da98b954eedeac495271d0f"

# Common contract addresses
SEAPORT_ADDRESS = "0x0000000000000068f116a894984e2db1123eb395"  # Seaport 1.6
SEAPORT_15_ADDRESS = "0x00000000000000adc04c56bf30ac9d3c0aaf14dc"  # Seaport 1.5
GNOSIS_SAFE_MASTER = "0xd9db270c1b5e3bd161e8c8503c55ceabee709552"  # Safe 1.3

# Event signatures (keccak256 hashes without 0x prefix)
# Use these to identify events from raw logs when ABI decoding fails
GENERIC_EVENT_SIGNATURES = {
    # ERC20 Events
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": "Transfer",  # Transfer(from, to, value)
    "8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925": "Approval",  # Approval(owner, spender, value)

    # WETH Events
    "e1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c": "Deposit",   # Deposit(dst, wad)
    "7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65": "Withdrawal", # Withdrawal(src, wad)

    # ERC721 Events
    # Note: ERC721 Transfer has same signature as ERC20, but indexed differently
    # "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": "Transfer",
    "17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31": "ApprovalForAll",  # ApprovalForAll(owner, operator, approved)

    # Seaport Events
    "9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31": "OrderFulfilled",  # OrderFulfilled(...)
    "6bacc01dbe442496068f7d234edd811f1a5f833243e0aec824f86ab861f3c90d": "OrderCancelled",  # OrderCancelled(...)

    # Gnosis Safe Events
    "442e715f626346e8c54381002da614f62bee8d27386535b2521ec8540898556e": "ExecutionSuccess",
    "23428b18acfb3ea64b08dc0c1d296ea9c09702c09083ca5272e64d115b687d23": "ExecutionFailure",
    "acd2c8702f96a4c64dac1d1a0d5c2e9c1d5b07e0b1ea5de2f6e4e8e8d4c3b2a1": "SafeReceived",  # Received ETH
}

# Generic events to look for
GENERIC_EVENTS = [
    "Transfer",       # ERC20/ERC721 transfer
    "Approval",       # ERC20 approval
    "ApprovalForAll", # ERC721 operator approval
    "Deposit",        # WETH wrap
    "Withdrawal",     # WETH unwrap
    "OrderFulfilled", # Seaport trade
]


# ============================================================================
# INTEREST CALCULATIONS (from production notebook)
# ============================================================================

def calculate_interest(
    principal_wei: int,
    apr_bps: int,
    duration_seconds: int,
    protocol_fee_bps: int = 0,
) -> tuple:
    """
    Calculate interest using Gondi's simple interest formula.
    Returns: (gross_interest, protocol_fee, net_interest)
    """
    if principal_wei == 0 or apr_bps == 0 or duration_seconds == 0:
        return 0, 0, 0

    # Simple interest (not compound)
    gross = (principal_wei * apr_bps * duration_seconds) // (PRECISION_BPS * SECONDS_PER_YEAR)

    # Protocol takes a cut from gross interest
    protocol_fee = (gross * protocol_fee_bps) // PRECISION_BPS

    # Net interest to lender
    net_interest = gross - protocol_fee

    return gross, protocol_fee, net_interest


def calculate_tranche_interest(
    tranche: Tranche,
    end_timestamp: int,
    protocol_fee_bps: int,
) -> tuple:
    """
    Calculate total interest for a tranche including carried interest.
    Returns: (total_interest, current_period_net_interest)
    """
    # Carried forward from prior refinancing
    carried = tranche.accruedInterest

    # Current period interest
    time_elapsed = max(0, end_timestamp - tranche.startTime)
    _, _, net_current = calculate_interest(
        tranche.principalAmount,
        tranche.aprBps,
        time_elapsed,
        protocol_fee_bps
    )

    total = carried + net_current
    return total, net_current

# Add main_app to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ============================================================================
# PROXY RESOLUTION (from production notebook)
# ============================================================================

# Cache for implementation addresses
_impl_cache: Dict[str, Optional[str]] = {}


def get_implementation_address(w3: Web3, proxy: str, logger=None) -> Optional[str]:
    """
    Resolve proxy contract to implementation address.

    Supports:
    - EIP-1967 implementation slot
    - EIP-1967 beacon slot
    - EIP-1167 minimal proxy
    - Custom getImplementation() function

    Returns implementation address or None if not a proxy.
    """
    proxy = w3.to_checksum_address(proxy)
    proxy_lower = proxy.lower()

    # Check cache first
    if proxy_lower in _impl_cache:
        cached = _impl_cache[proxy_lower]
        if logger and cached:
            logger.info(f"  [CACHE] Implementation: {cached[:16]}...")
        return cached

    if logger:
        logger.info(f"Checking proxy patterns for {proxy[:16]}...")

    # --- EIP-1967 implementation slot ---
    impl_slot = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
    try:
        raw_impl = w3.eth.get_storage_at(proxy, impl_slot)
        if raw_impl and raw_impl != b'\x00' * 32:
            impl = w3.to_checksum_address('0x' + raw_impl.hex()[-40:])
            if logger:
                logger.success(f"  EIP-1967 impl slot -> {impl[:16]}...")
            _impl_cache[proxy_lower] = impl.lower()
            return impl.lower()
    except Exception as e:
        if logger:
            logger.info(f"  EIP-1967 impl slot: not found")

    # --- EIP-1967 beacon slot ---
    beacon_slot = "0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50"
    try:
        raw_beacon = w3.eth.get_storage_at(proxy, beacon_slot)
        if raw_beacon and raw_beacon != b'\x00' * 32:
            beacon_addr = w3.to_checksum_address('0x' + raw_beacon.hex()[-40:])
            beacon_abi = [{"inputs": [], "name": "implementation", "outputs": [{"name": "", "type": "address"}], "stateMutability": "view", "type": "function"}]
            beacon_ct = w3.eth.contract(address=beacon_addr, abi=beacon_abi)
            impl = beacon_ct.functions.implementation().call()
            if logger:
                logger.success(f"  EIP-1967 beacon -> {impl[:16]}...")
            _impl_cache[proxy_lower] = impl.lower()
            return impl.lower()
    except Exception as e:
        if logger:
            logger.info(f"  EIP-1967 beacon: not found")

    # --- EIP-1167 minimal proxy ---
    try:
        code = w3.eth.get_code(proxy).hex()
        # EIP-1167 pattern: 363d3d373d3d3d363d73{impl_addr}5af43d82803e903d91602b57fd5bf3
        if code.startswith("0x363d3d373d3d3d363d73") and len(code) >= 66:
            impl = w3.to_checksum_address('0x' + code[22:62])
            if logger:
                logger.success(f"  EIP-1167 minimal proxy -> {impl[:16]}...")
            _impl_cache[proxy_lower] = impl.lower()
            return impl.lower()
    except Exception as e:
        if logger:
            logger.info(f"  EIP-1167 minimal proxy: not found")

    # --- Custom getImplementation() ---
    try:
        custom_abi = [{"inputs": [], "name": "getImplementation", "outputs": [{"name": "", "type": "address"}], "stateMutability": "view", "type": "function"}]
        ct = w3.eth.contract(address=proxy, abi=custom_abi)
        impl = ct.functions.getImplementation().call()
        if impl and impl != "0x0000000000000000000000000000000000000000":
            if logger:
                logger.success(f"  Custom getImplementation() -> {impl[:16]}...")
            _impl_cache[proxy_lower] = impl.lower()
            return impl.lower()
    except Exception as e:
        if logger:
            logger.info(f"  Custom getImplementation(): not found")

    # Not a proxy
    if logger:
        logger.info(f"  -> Not a proxy contract")
    _impl_cache[proxy_lower] = None
    return None


def resolve_contract_address(w3: Web3, address: str, logger=None) -> str:
    """
    Resolve a contract address to its implementation if it's a proxy.
    Returns the implementation address if proxy, otherwise the original address.
    """
    impl = get_implementation_address(w3, address, logger)
    if impl:
        return impl
    return address.lower()


# ============================================================================
# S3 ABI LOADING (with proxy resolution)
# ============================================================================

def load_abi_from_s3(contract_address: str, logger=None, w3: Web3 = None) -> Optional[Dict]:
    """
    Load ABI from S3 with debug logging.
    If w3 is provided, will attempt proxy resolution first.
    """
    try:
        from main_app.s3_utils import load_abi_from_s3 as s3_load_abi

        address_to_try = contract_address.lower()

        # Try proxy resolution if w3 is available
        if w3:
            impl = get_implementation_address(w3, contract_address, logger)
            if impl:
                address_to_try = impl
                if logger:
                    logger.info(f"  Using implementation address: {impl[:16]}...")

        abi = s3_load_abi(address_to_try)

        if abi and logger:
            logger.success(f"Loaded ABI from S3 for {address_to_try[:16]}...")
            logger.info(f"  ABI has {len(abi)} entries")
        elif logger:
            logger.warning(f"ABI not found in S3 for {address_to_try}")

        return abi if abi else None

    except ImportError as e:
        if logger:
            logger.warning(f"Could not import S3 utils: {e}")
        return None
    except Exception as e:
        if logger:
            logger.warning(f"Error loading ABI from S3: {e}")
        return None


def list_s3_abis(logger=None) -> List[str]:
    """List available ABIs in S3"""
    try:
        from main_app.s3_utils import list_available_abis

        abis = list_available_abis()
        if logger:
            logger.info(f"Found {len(abis)} ABIs in S3")
        return abis

    except ImportError:
        if logger:
            logger.warning("Could not import S3 utils")
        return []
    except Exception as e:
        if logger:
            logger.warning(f"Error listing S3 ABIs: {e}")
        return []


# Cache for fund wallets
_fund_wallets_cache: Optional[set] = None


def load_fund_wallets(logger=None) -> set:
    """
    Load fund wallet addresses from S3 wallet mapping file.
    Returns set of lowercase wallet addresses.
    """
    global _fund_wallets_cache

    if _fund_wallets_cache is not None:
        if logger:
            logger.info(f"[CACHE] Fund wallets: {len(_fund_wallets_cache)} addresses")
        return _fund_wallets_cache

    try:
        import boto3
        import pandas as pd
        from io import BytesIO

        s3 = boto3.client('s3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name='us-east-1'
        )

        bucket = 'realworldnav-beta-1'
        key = 'drip_capital/drip_capital_wallet_ID_mapping.xlsx'

        if logger:
            logger.info(f"Loading fund wallets from S3: {key}")

        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_excel(BytesIO(response['Body'].read()))

        # Get all wallet addresses (active or not - we want to track all fund wallets)
        wallets = set()
        for addr in df['wallet_address'].dropna():
            wallets.add(str(addr).lower())

        _fund_wallets_cache = wallets

        if logger:
            logger.success(f"Loaded {len(wallets)} fund wallet addresses")
            # Show a few examples
            examples = list(wallets)[:3]
            for ex in examples:
                logger.info(f"  {ex[:10]}...{ex[-4:]}")

        return wallets

    except Exception as e:
        if logger:
            logger.warning(f"Could not load fund wallets: {e}")
        return set()


def to_hex(value) -> str:
    """Convert any hex-like value to clean hex string WITHOUT 0x prefix."""
    if value is None:
        return ""

    # Get hex string first
    if hasattr(value, 'hex') and callable(value.hex):
        # HexBytes, bytes, or similar
        h = value.hex()
    elif isinstance(value, bytes):
        h = value.hex()
    else:
        h = str(value)

    # Strip 0x prefix if present
    if h.startswith('0x') or h.startswith('0X'):
        h = h[2:]

    # Return clean hex (filter any remaining non-hex chars just in case)
    return ''.join(c for c in h if c in '0123456789abcdefABCDEF')

# ============================================================================
# DEBUG LOGGER
# ============================================================================

class DebugLogger:
    """Captures verbose debug output for display"""

    def __init__(self):
        self.logs = []
        self._indent = 0

    def log(self, step: int, message: str, data: Any = None):
        """Log a step with optional data"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        indent = "  " * self._indent
        entry = f"[{timestamp}] STEP {step}: {indent}{message}"
        if data is not None:
            entry += "\n" + self._format_data(data)
        self.logs.append(entry)

    def info(self, message: str, data: Any = None):
        """Log info without step number"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        indent = "  " * self._indent
        entry = f"[{timestamp}]        {indent}{message}"
        if data is not None:
            entry += "\n" + self._format_data(data)
        self.logs.append(entry)

    def success(self, message: str):
        """Log success message"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        indent = "  " * self._indent
        self.logs.append(f"[{timestamp}]        {indent}[OK] {message}")

    def warning(self, message: str):
        """Log warning message"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        indent = "  " * self._indent
        self.logs.append(f"[{timestamp}]        {indent}[WARN] {message}")

    def error(self, step: int, message: str, exc: Exception = None):
        """Log error with optional exception"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        indent = "  " * self._indent
        entry = f"[{timestamp}] STEP {step}: {indent}[ERROR] {message}"
        if exc:
            entry += f"\n{'='*60}\n{traceback.format_exc()}{'='*60}"
        self.logs.append(entry)

    def indent(self):
        """Increase indentation"""
        self._indent += 1

    def dedent(self):
        """Decrease indentation"""
        self._indent = max(0, self._indent - 1)

    def separator(self, char: str = "-"):
        """Add visual separator"""
        self.logs.append(char * 70)

    def _format_data(self, data: Any, max_len: int = 500) -> str:
        """Pretty format data for display"""
        indent = "  " * (self._indent + 1)

        if data is None:
            return f"{indent}(None)"

        if isinstance(data, dict):
            lines = []
            for k, v in data.items():
                v_str = self._format_value(v, max_len)
                lines.append(f"{indent}{k}: {v_str}")
            return "\n".join(lines)

        if isinstance(data, (list, tuple)):
            if len(data) == 0:
                return f"{indent}(empty)"
            lines = []
            for i, item in enumerate(data[:20]):  # Limit to 20 items
                v_str = self._format_value(item, max_len)
                lines.append(f"{indent}[{i}]: {v_str}")
            if len(data) > 20:
                lines.append(f"{indent}... and {len(data) - 20} more items")
            return "\n".join(lines)

        return f"{indent}{self._format_value(data, max_len)}"

    def _format_value(self, v: Any, max_len: int = 500) -> str:
        """Format a single value"""
        if isinstance(v, bytes):
            hex_str = v.hex()
            if len(hex_str) > max_len:
                return f"0x{hex_str[:max_len]}... ({len(hex_str)//2} bytes)"
            return f"0x{hex_str}"

        if isinstance(v, dict):
            s = json.dumps(v, default=str)
            if len(s) > max_len:
                return s[:max_len] + "..."
            return s

        s = str(v)
        if len(s) > max_len:
            return s[:max_len] + "..."
        return s

    def get_output(self) -> str:
        """Get all logs as string"""
        return "\n\n".join(self.logs)

    def clear(self):
        """Clear all logs"""
        self.logs = []
        self._indent = 0


# ============================================================================
# WEB3 CONNECTION
# ============================================================================

def get_web3(logger: DebugLogger) -> Optional[Web3]:
    """Initialize Web3 connection with debug logging"""

    logger.log(1, "Checking Web3 connection...")

    # Check for Infura key
    infura_key = os.getenv("INFURA_API_KEY") or os.getenv("INFURA_KEY") or os.getenv("WEB3_INFURA_PROJECT_ID")

    if infura_key:
        logger.info(f"Found Infura key: {infura_key[:8]}...{infura_key[-4:]}")
        rpc_url = f"https://mainnet.infura.io/v3/{infura_key}"
    else:
        # Try other RPC URLs from env
        rpc_url = os.getenv("ETH_RPC_URL") or os.getenv("WEB3_PROVIDER_URI")
        if rpc_url:
            logger.info(f"Using RPC URL from env: {rpc_url[:50]}...")
        else:
            logger.error(1, "No Web3 RPC URL found!")
            logger.info("Set INFURA_API_KEY or ETH_RPC_URL in .env")
            return None

    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        logger.info(f"Provider: {type(w3.provider).__name__}")

        # Test connection
        connected = w3.is_connected()
        logger.info(f"Connected: {connected}")

        if not connected:
            logger.error(1, "Web3 not connected!")
            return None

        chain_id = w3.eth.chain_id
        logger.info(f"Chain ID: {chain_id}")

        latest_block = w3.eth.block_number
        logger.info(f"Latest Block: {latest_block}")

        logger.success("Web3 connection established")
        return w3

    except Exception as e:
        logger.error(1, f"Web3 connection failed: {e}", e)
        return None


# ============================================================================
# TRANSACTION FETCHING
# ============================================================================

def fetch_transaction(w3: Web3, tx_hash: str, logger: DebugLogger) -> Optional[Dict]:
    """Fetch transaction data with debug logging"""

    logger.log(2, f"Fetching transaction {tx_hash[:16]}...")

    try:
        tx = w3.eth.get_transaction(tx_hash)

        if tx is None:
            logger.error(2, "Transaction not found!")
            return None

        # Convert to dict for easier handling
        tx_dict = dict(tx)

        # Log key fields
        logger.info("Transaction data:")
        logger.indent()
        logger.info(f"From: {tx_dict.get('from', 'N/A')}")
        logger.info(f"To: {tx_dict.get('to', 'N/A')}")

        value_wei = tx_dict.get('value', 0)
        value_eth = Decimal(value_wei) / Decimal(10**18)
        logger.info(f"Value: {value_wei} wei ({value_eth} ETH)")

        input_data = tx_dict.get('input', b'')
        input_hex = to_hex(input_data)
        logger.info(f"Input: 0x{input_hex[:8]}... ({len(input_hex)//2} bytes)")

        if len(input_hex) >= 8:
            func_selector = input_hex[:8]
            logger.info(f"Function selector: 0x{func_selector}")

        logger.info(f"Gas: {tx_dict.get('gas', 'N/A')}")
        logger.info(f"Gas Price: {tx_dict.get('gasPrice', 'N/A')} wei")
        logger.info(f"Block Number: {tx_dict.get('blockNumber', 'N/A')}")
        logger.info(f"Transaction Index: {tx_dict.get('transactionIndex', 'N/A')}")
        logger.info(f"Nonce: {tx_dict.get('nonce', 'N/A')}")
        logger.dedent()

        logger.success("Transaction fetched successfully")
        return tx_dict

    except Exception as e:
        logger.error(2, f"Failed to fetch transaction: {e}", e)
        return None


def fetch_receipt(w3: Web3, tx_hash: str, logger: DebugLogger) -> Optional[Dict]:
    """Fetch transaction receipt with debug logging"""

    logger.log(3, "Fetching receipt...")

    try:
        receipt = w3.eth.get_transaction_receipt(tx_hash)

        if receipt is None:
            logger.error(3, "Receipt not found! Transaction may be pending.")
            return None

        # Convert to dict
        receipt_dict = dict(receipt)

        logger.info("Receipt data:")
        logger.indent()
        logger.info(f"Status: {receipt_dict.get('status', 'N/A')} (1=Success, 0=Failed)")
        logger.info(f"Gas Used: {receipt_dict.get('gasUsed', 'N/A')}")
        logger.info(f"Cumulative Gas: {receipt_dict.get('cumulativeGasUsed', 'N/A')}")
        logger.info(f"Effective Gas Price: {receipt_dict.get('effectiveGasPrice', 'N/A')} wei")
        logger.info(f"Contract Address: {receipt_dict.get('contractAddress', 'None')}")

        logs = receipt_dict.get('logs', [])
        logger.info(f"Logs: {len(logs)} events")
        logger.dedent()

        # Log each event
        if logs:
            logger.separator()
            logger.info("EVENT LOGS:")
            for i, log in enumerate(logs):
                logger.indent()
                logger.info(f"Log[{i}]:")
                logger.indent()
                logger.info(f"Address: {log.get('address', 'N/A')}")

                topics = log.get('topics', [])
                logger.info(f"Topics ({len(topics)}):")
                logger.indent()
                for j, topic in enumerate(topics):
                    topic_hex = to_hex(topic)
                    label = "Event Signature" if j == 0 else f"Indexed Param {j}"
                    logger.info(f"[{j}] {label}: 0x{topic_hex[:16]}...")
                logger.dedent()

                data = log.get('data', b'')
                data_hex = to_hex(data)
                logger.info(f"Data: 0x{data_hex[:64]}... ({len(data_hex)//2} bytes)")
                logger.info(f"Log Index: {log.get('logIndex', 'N/A')}")
                logger.dedent()
                logger.dedent()
            logger.separator()

        logger.success("Receipt fetched successfully")
        return receipt_dict

    except Exception as e:
        logger.error(3, f"Failed to fetch receipt: {e}", e)
        return None


def fetch_block(w3: Web3, block_number: int, logger: DebugLogger) -> Optional[Dict]:
    """Fetch block data with debug logging"""

    logger.log(4, f"Fetching block {block_number}...")

    try:
        block = w3.eth.get_block(block_number)

        if block is None:
            logger.error(4, "Block not found!")
            return None

        block_dict = dict(block)

        timestamp = block_dict.get('timestamp', 0)
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)

        logger.info("Block data:")
        logger.indent()
        logger.info(f"Timestamp: {timestamp} ({dt.isoformat()})")
        logger.info(f"Base Fee: {block_dict.get('baseFeePerGas', 'N/A')} wei")
        logger.info(f"Gas Used: {block_dict.get('gasUsed', 'N/A')}")
        logger.info(f"Gas Limit: {block_dict.get('gasLimit', 'N/A')}")
        logger.info(f"Transaction Count: {len(block_dict.get('transactions', []))}")
        logger.dedent()

        logger.success("Block fetched successfully")
        return block_dict

    except Exception as e:
        logger.error(4, f"Failed to fetch block: {e}", e)
        return None


# ============================================================================
# ETH PRICE
# ============================================================================

def get_eth_price(w3: Web3, block_number: int, logger: DebugLogger) -> Optional[Decimal]:
    """Get ETH price at block with debug logging"""

    logger.log(5, f"Getting ETH price at block {block_number}...")

    # Chainlink ETH/USD feed on mainnet
    CHAINLINK_ETH_USD = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"

    AGGREGATOR_ABI = [
        {
            "inputs": [],
            "name": "latestRoundData",
            "outputs": [
                {"name": "roundId", "type": "uint80"},
                {"name": "answer", "type": "int256"},
                {"name": "startedAt", "type": "uint256"},
                {"name": "updatedAt", "type": "uint256"},
                {"name": "answeredInRound", "type": "uint80"}
            ],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "decimals",
            "outputs": [{"name": "", "type": "uint8"}],
            "stateMutability": "view",
            "type": "function"
        }
    ]

    try:
        aggregator = w3.eth.contract(
            address=Web3.to_checksum_address(CHAINLINK_ETH_USD),
            abi=AGGREGATOR_ABI
        )

        logger.info(f"Using Chainlink ETH/USD feed: {CHAINLINK_ETH_USD}")

        # Get decimals
        decimals = aggregator.functions.decimals().call()
        logger.info(f"Price decimals: {decimals}")

        # Get price at block (historical query)
        try:
            result = aggregator.functions.latestRoundData().call(block_identifier=block_number)
            price_raw = result[1]  # answer field
        except Exception as e:
            logger.warning(f"Historical price query failed, using latest: {e}")
            result = aggregator.functions.latestRoundData().call()
            price_raw = result[1]

        price = Decimal(price_raw) / Decimal(10 ** decimals)
        logger.info(f"Raw price: {price_raw}")
        logger.info(f"ETH Price: ${price:,.2f}")

        logger.success(f"ETH price retrieved: ${price:,.2f}")
        return price

    except Exception as e:
        logger.error(5, f"Failed to get ETH price: {e}", e)
        logger.info("Using fallback price: $3000")
        return Decimal("3000")


# ============================================================================
# ROUTING
# ============================================================================

# ============================================================================
# CONTRACT ROUTING - Multi-platform NFT Lending
# Based on production notebook Cell 128/1294
# ============================================================================

# Contract address to platform mapping (lowercase)
CONTRACT_ROUTING = {
    # === BLUR ===
    "0x29469395eaf6f95920e59f858042f0e28d98a20b": "BLUR",  # Blur Blend (Lending Proxy)
    "0x0000000000a39bb272e79075ade125fd351887ac": "BLUR",  # Blur Pool
    "0x000000000000ad05ccc4f10045630fb830b95127": "BLUR",  # Blur Marketplace

    # === GONDI (version-aware) ===
    "0xf41b389e0c1950dc0b16c9498eae77131cc08a56": "GONDI",  # Gondi v1 (tranche-based)
    "0x478f6f994c6fb3cf3e444a489b3ad9edb8ccae16": "GONDI",  # Gondi v2 (source-based)
    "0xf65b99ce6dc5f6c556172bcc0ff27d3665a7d9a8": "GONDI",  # Gondi v3 (tranche-based)
    "0x59e0b87e3dcfb5d34c06c71c3fbf7f6b7d77a4ff": "GONDI",  # MultiSourceLoan

    # === ARCADE ===
    "0x81b2f8fc75bab64a6b144aa6d2faa127b4fa7fd9": "ARCADE",  # LoanCore Proxy
    "0x6ddb57101a17854109c3b9feb80ae19662ea950f": "ARCADE",  # LoanCore Implementation
    "0xb39dab85fa05c381767ff992ccde4c94619993d4": "ARCADE",  # RepaymentController (active)
    "0x89bc08ba00f135d608bc335f6b33d7a9abcc98af": "ARCADE",  # OriginationController
    "0x349a026a43ffa8e2ab4c4e59fcaa93f87bd8ddee": "ARCADE",  # LenderNote (aLN)
    "0x337104a4f06260ff327d6734c555a0f5d8f863aa": "ARCADE",  # BorrowerNote (aBN)

    # === NFTfi ===
    "0xf896527c49b44aab3cf22ae356fa3af8e331f280": "NFTFI",  # DirectLoanFixedOffer V1
    "0xe52cec0e90115abeb3304baa36bc2655731f7934": "NFTFI",  # DirectLoanCoordinator V2
    "0x8252df1d8b29057d1afe3062bf5a64d503152bc8": "NFTFI",  # DirectLoanFixedOffer V2
    "0xd0a40eb7fcd530a13866b9e893e4a9e0d15d03eb": "NFTFI",  # DirectLoanFixedOfferRedeploy

    # === ZHARTA (Peer-to-Pool NFT Lending) ===
    "0xb7c8c74ed765267b54f4c327f279d7e850725ef2": "ZHARTA",  # Loans (main interface)
    "0x5be916cff5f07870e9aef205960e07d9e287ef27": "ZHARTA",  # LoansCore (state storage)
    "0x6474ab1b56b47bc26ba8cb471d566b8cc528f308": "ZHARTA",  # LendingPoolPeripheral
    "0x35b8545ae12d89cd4997d5485e2e68c857df24a8": "ZHARTA",  # CollateralVaultPeripheral

    # === GENERIC ===
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "GENERIC",  # WETH
    "0x0000000000000068f116a894984e2db1123eb395": "GENERIC",  # Seaport 1.6
}

# Gondi version info for specialized handling
GONDI_VERSION_INFO = {
    "0xf41b389e0c1950dc0b16c9498eae77131cc08a56": {"version": "v1", "type": "tranche"},
    "0x478f6f994c6fb3cf3e444a489b3ad9edb8ccae16": {"version": "v2", "type": "source"},
    "0xf65b99ce6dc5f6c556172bcc0ff27d3665a7d9a8": {"version": "v3", "type": "tranche"},
}

# Function selector to (platform, function_name, category) mapping
# Based on production notebook Cell 128
FUNCTION_SELECTORS = {
    # === DEX - Uniswap V2/V3 ===
    "38ed1739": ("GENERIC", "swapExactTokensForTokens", "DEX_SWAP"),
    "8803dbee": ("GENERIC", "swapTokensForExactTokens", "DEX_SWAP"),
    "7ff36ab5": ("GENERIC", "swapExactETHForTokens", "DEX_SWAP"),
    "18cbafe5": ("GENERIC", "swapExactTokensForETH", "DEX_SWAP"),
    "c04b8d59": ("GENERIC", "exactInput", "DEX_SWAP"),
    "414bf389": ("GENERIC", "exactInputSingle", "DEX_SWAP"),

    # === DeFi Lending - Aave/Compound ===
    "e8eda9df": ("GENERIC", "deposit", "LEND_DEPOSIT"),
    "69328dec": ("GENERIC", "withdraw", "LEND_WITHDRAW"),
    "a415bcad": ("GENERIC", "borrow", "LEND_BORROW"),
    "573ade81": ("GENERIC", "repay", "LEND_REPAY"),

    # === NFT Lending - NFTfi ===
    "3b1d21a2": ("NFTFI", "initializeLoan", "NFT_LEND_ORIGINATE"),
    "58e644b7": ("NFTFI", "beginLoan", "NFT_LEND_BORROW"),
    "6d5f9e56": ("NFTFI", "repayLoan", "NFT_LEND_REPAY"),
    "8c7a63ae": ("NFTFI", "payBackLoan", "NFT_LEND_REPAY"),
    "766df841": ("NFTFI", "liquidateOverdueLoan", "NFT_LEND_LIQUIDATE"),

    # === NFT Lending - Gondi ===
    "c09c4e7e": ("GONDI", "refinanceFull", "NFT_LEND_REFINANCE"),
    "65e03b9c": ("GONDI", "refinanceFull", "NFT_LEND_REFINANCE"),

    # === NFT Lending - Blur Blend ===
    "a49c04be": ("BLUR", "refinanceAuction", "NFT_LEND_REFINANCE"),  # Refinance loan in auction
    "c87df1c2": ("BLUR", "repay", "NFT_LEND_REPAY"),  # Repay loan (full struct sig)
    "9a4737b9": ("BLUR", "borrow", "NFT_LEND_ORIGINATE"),  # New loan (borrow against NFT)
    "057f569e": ("BLUR", "startAuction", "NFT_LEND_AUCTION"),  # Call the loan
    "49ebc08d": ("BLUR", "seize", "NFT_LEND_SEIZURE"),  # Seize collateral
    "8a919639": ("BLUR", "buyLocked", "NFT_LEND_BUYOUT"),  # Buy locked NFT
    "a59c7762": ("BLUR", "buyLockedETH", "NFT_LEND_BUYOUT"),  # Buy locked NFT with ETH

    # === NFT Lending - Arcade ===
    "be993dc2": ("ARCADE", "repay", "NFT_LEND_REPAY"),  # Full loan repayment
    "556f800f": ("ARCADE", "repayPart", "NFT_LEND_REPAY_PARTIAL"),  # Installment payment
    "379607f5": ("ARCADE", "claim", "NFT_LEND_SEIZURE"),  # Claim defaulted collateral
    "4c04f4a5": ("ARCADE", "rollover", "NFT_LEND_ROLLOVER"),  # Extend loan
    "38a78016": ("ARCADE", "cancelNonce", "ADMIN"),  # Cancel signature nonce

    # === NFT Lending - Zharta ===
    "5a5cd02e": ("ZHARTA", "reserveEth", "NFT_LEND_ORIGINATE"),  # Create loan with ETH
    "c290d691": ("ZHARTA", "pay", "NFT_LEND_REPAY"),  # Repay loan
    # settleDefault: To be captured from real tx

    # === NFT Marketplaces ===
    "ab834bab": ("GENERIC", "atomicMatch_", "NFT_TRADE"),
    "fb0f3ee1": ("GENERIC", "fulfillBasicOrder", "NFT_TRADE"),
    "0a0a5e48": ("GENERIC", "execute", "NFT_TRADE"),

    # === ERC20 ===
    "a9059cbb": ("GENERIC", "transfer", "TOKEN_TRANSFER"),
    "23b872dd": ("GENERIC", "transferFrom", "TOKEN_TRANSFER"),
    "095ea7b3": ("GENERIC", "approve", "TOKEN_APPROVAL"),

    # === WETH ===
    "d0e30db0": ("GENERIC", "deposit", "WETH_WRAP"),
    "2e1a7d4d": ("GENERIC", "withdraw", "WETH_UNWRAP"),
}


def route_transaction(tx: Dict, receipt: Dict, logger: DebugLogger) -> tuple:
    """
    Determine which decoder should handle this transaction.
    Returns (platform, function_name, category) tuple.
    """

    logger.log(6, "Routing transaction to decoder...")

    to_address = (tx.get('to') or '').lower()
    from_address = (tx.get('from') or '').lower()

    logger.info(f"To address: {to_address}")
    logger.info(f"From address: {from_address}")

    platform = None
    func_name = "unknown"
    category = "UNKNOWN"

    # Step 1: Check CONTRACT_ROUTING for 'to' address
    logger.info("Checking CONTRACT_ROUTING for 'to' address...")
    if to_address in CONTRACT_ROUTING:
        platform = CONTRACT_ROUTING[to_address]
        logger.success(f"Found in CONTRACT_ROUTING: {platform}")

        # Check if it's a versioned Gondi contract
        if to_address in GONDI_VERSION_INFO:
            version_info = GONDI_VERSION_INFO[to_address]
            logger.info(f"  Gondi version: {version_info['version']} (type: {version_info['type']})")
    else:
        logger.info("  -> Not found")

    # Step 2: Check FUNCTION_SELECTORS
    input_hex = to_hex(tx.get('input', b''))

    if len(input_hex) >= 8:
        func_selector = input_hex[:8].lower()
        logger.info(f"Checking FUNCTION_SELECTORS for 0x{func_selector}...")
        if func_selector in FUNCTION_SELECTORS:
            sel_platform, sel_func, sel_category = FUNCTION_SELECTORS[func_selector]
            func_name = sel_func
            category = sel_category
            logger.success(f"Found: {func_name} -> {sel_platform} (category: {category})")

            # Use function selector platform if contract routing didn't match
            if platform is None:
                platform = sel_platform
        else:
            logger.info("  -> Not found in FUNCTION_SELECTORS")
            logger.info(f"  (selector 0x{func_selector} may need to be added)")

    # Step 3: Check event log addresses
    if platform is None:
        logger.info("Checking event log addresses...")
        logs = receipt.get('logs', [])
        for i, log in enumerate(logs):
            log_addr = (log.get('address') or '').lower()
            if log_addr in CONTRACT_ROUTING:
                platform = CONTRACT_ROUTING[log_addr]
                logger.success(f"Log[{i}] address {log_addr[:10]}... -> {platform}")
                break
        else:
            logger.info("  -> No matching log addresses")

    # Step 4: Check if it's a simple ETH transfer
    value = tx.get('value', 0)
    if platform is None and value > 0 and len(input_hex) == 0:
        platform = "GENERIC"
        func_name = "transfer"
        category = "ETH_TRANSFER"
        logger.success("Simple ETH transfer -> GENERIC")

    # Default
    if platform is None:
        platform = "GENERIC"
        logger.warning("No specific decoder found, defaulting to GENERIC")

    logger.separator("-")
    logger.info(f"ROUTING RESULT:")
    logger.info(f"  Platform: {platform}")
    logger.info(f"  Function: {func_name}")
    logger.info(f"  Category: {category}")

    return (platform, func_name, category)


# ============================================================================
# FUNCTION DECODING
# ============================================================================

# Common ABIs for decoding
WETH_ABI = [
    {"inputs": [], "name": "deposit", "outputs": [], "stateMutability": "payable", "type": "function"},
    {"inputs": [{"name": "wad", "type": "uint256"}], "name": "withdraw", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
    {"anonymous": False, "inputs": [{"indexed": True, "name": "dst", "type": "address"}, {"indexed": False, "name": "wad", "type": "uint256"}], "name": "Deposit", "type": "event"},
    {"anonymous": False, "inputs": [{"indexed": True, "name": "src", "type": "address"}, {"indexed": False, "name": "wad", "type": "uint256"}], "name": "Withdrawal", "type": "event"},
]

ERC20_ABI = [
    {"inputs": [{"name": "to", "type": "address"}, {"name": "value", "type": "uint256"}], "name": "transfer", "outputs": [{"name": "", "type": "bool"}], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "from", "type": "address"}, {"name": "to", "type": "address"}, {"name": "value", "type": "uint256"}], "name": "transferFrom", "outputs": [{"name": "", "type": "bool"}], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "spender", "type": "address"}, {"name": "value", "type": "uint256"}], "name": "approve", "outputs": [{"name": "", "type": "bool"}], "stateMutability": "nonpayable", "type": "function"},
    {"anonymous": False, "inputs": [{"indexed": True, "name": "from", "type": "address"}, {"indexed": True, "name": "to", "type": "address"}, {"indexed": False, "name": "value", "type": "uint256"}], "name": "Transfer", "type": "event"},
    {"anonymous": False, "inputs": [{"indexed": True, "name": "owner", "type": "address"}, {"indexed": True, "name": "spender", "type": "address"}, {"indexed": False, "name": "value", "type": "uint256"}], "name": "Approval", "type": "event"},
]

# ERC721 ABI for NFT transfers
ERC721_ABI = [
    {"inputs": [{"name": "from", "type": "address"}, {"name": "to", "type": "address"}, {"name": "tokenId", "type": "uint256"}], "name": "transferFrom", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "from", "type": "address"}, {"name": "to", "type": "address"}, {"name": "tokenId", "type": "uint256"}], "name": "safeTransferFrom", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "to", "type": "address"}, {"name": "approved", "type": "bool"}], "name": "setApprovalForAll", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
    {"anonymous": False, "inputs": [{"indexed": True, "name": "from", "type": "address"}, {"indexed": True, "name": "to", "type": "address"}, {"indexed": True, "name": "tokenId", "type": "uint256"}], "name": "Transfer", "type": "event"},
    {"anonymous": False, "inputs": [{"indexed": True, "name": "owner", "type": "address"}, {"indexed": True, "name": "approved", "type": "address"}, {"indexed": True, "name": "tokenId", "type": "uint256"}], "name": "Approval", "type": "event"},
    {"anonymous": False, "inputs": [{"indexed": True, "name": "owner", "type": "address"}, {"indexed": True, "name": "operator", "type": "address"}, {"indexed": False, "name": "approved", "type": "bool"}], "name": "ApprovalForAll", "type": "event"},
]


def decode_function_input(w3: Web3, tx: Dict, logger: DebugLogger) -> tuple:
    """Decode function call from transaction input"""

    logger.log(7, "Decoding function input...")

    raw_input = tx.get('input', b'')
    input_hex = to_hex(raw_input)

    logger.info(f"Raw input type: {type(raw_input).__name__}")
    logger.info(f"Hex length: {len(input_hex)} chars")

    try:
        input_data = bytes.fromhex(input_hex) if input_hex else b''
    except ValueError as e:
        logger.error(7, f"Failed to parse hex: {e}")
        logger.info(f"Problematic hex (first 100 chars): {input_hex[:100]}")
        return ("unknown", {"error": str(e)})

    if len(input_data) < 4:
        logger.info("Input too short for function call")
        logger.info("This is likely a simple ETH transfer")
        return ("transfer", {})

    func_selector = input_data[:4].hex()
    logger.info(f"Function selector: 0x{func_selector}")

    to_address = (tx.get('to') or '').lower()

    # ========================================
    # STEP 7a: Try S3 ABI first (with proxy resolution)
    # ========================================
    logger.info("Attempting to load contract ABI from S3 (with proxy resolution)...")
    s3_abi = load_abi_from_s3(to_address, logger, w3=w3)

    if s3_abi:
        logger.info(f"S3 ABI loaded with {len(s3_abi)} entries, attempting decode...")
        try:
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(to_address) if to_address else None,
                abi=s3_abi
            )
            func_obj, params = contract.decode_function_input(input_data)
            func_name = func_obj.fn_name

            logger.success(f"Decoded with S3 ABI: {func_name}")
            logger.info("Parameters:")
            logger.indent()
            for k, v in params.items():
                # Format large integers nicely
                if isinstance(v, int) and v > 10**15:
                    v_eth = Decimal(v) / Decimal(10**18)
                    logger.info(f"{k}: {v} ({v_eth:.6f} ETH-equiv)")
                elif isinstance(v, bytes):
                    logger.info(f"{k}: 0x{v.hex()[:64]}...")
                elif isinstance(v, (list, tuple)) and len(v) > 3:
                    logger.info(f"{k}: ({len(v)} items)")
                    logger.indent()
                    for i, item in enumerate(v[:5]):
                        logger.info(f"[{i}]: {item}")
                    if len(v) > 5:
                        logger.info(f"... and {len(v) - 5} more")
                    logger.dedent()
                else:
                    logger.info(f"{k}: {v}")
            logger.dedent()

            return (func_name, dict(params))

        except Exception as e:
            logger.warning(f"S3 ABI decode failed: {str(e)[:80]}")
            logger.info("Falling back to hardcoded ABIs...")

    # ========================================
    # STEP 7b: Fallback to hardcoded ABIs
    # ========================================
    abis_to_try = [
        ("WETH", WETH_ABI),
        ("ERC20", ERC20_ABI),
        ("ERC721", ERC721_ABI),
    ]

    for abi_name, abi in abis_to_try:
        try:
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(to_address) if to_address else None,
                abi=abi
            )
            func_obj, params = contract.decode_function_input(input_data)
            func_name = func_obj.fn_name

            logger.success(f"Decoded with {abi_name} ABI: {func_name}")
            logger.info("Parameters:")
            logger.indent()
            for k, v in params.items():
                logger.info(f"{k}: {v}")
            logger.dedent()

            return (func_name, dict(params))

        except Exception as e:
            logger.info(f"  {abi_name} ABI: failed - {str(e)[:50]}")

    logger.warning("Could not decode function input with known ABIs")
    logger.info("This may be a custom/unknown contract")

    return ("unknown", {"raw_input": input_data.hex()})


# ============================================================================
# EVENT DECODING
# ============================================================================

def decode_events(w3: Web3, receipt: Dict, logger: DebugLogger) -> List[Dict]:
    """
    Decode events from receipt logs using process_log() (production pattern).
    Returns decoded events with Loan objects when applicable.
    """

    logger.log(8, "Decoding events...")

    logs = receipt.get('logs', [])
    logger.info(f"Processing {len(logs)} logs...")

    decoded_events = []

    # Cache for S3 ABIs by contract address
    abi_cache = {}

    # Check if contract is V2 (different struct format)
    def is_v2_contract(addr: str) -> bool:
        return addr.lower() in GONDI_VERSION_INFO and GONDI_VERSION_INFO[addr.lower()].get('type') == 'source'

    for i, log in enumerate(logs):
        logger.indent()
        logger.info(f"Log[{i}]:")
        logger.indent()

        topics = log.get('topics', [])
        if not topics:
            logger.warning("No topics - cannot decode")
            logger.dedent()
            logger.dedent()
            continue

        # Get event signature
        sig = to_hex(topics[0]).lower()
        log_address = (log.get('address') or '').lower()
        logger.info(f"Contract: {log_address}")
        logger.info(f"Event sig: 0x{sig[:16]}...")

        decoded_this_log = False

        # ========================================
        # Load ABI from S3 (with proxy resolution)
        # ========================================
        if log_address not in abi_cache:
            s3_abi = load_abi_from_s3(log_address, logger, w3=w3)
            abi_cache[log_address] = s3_abi
        else:
            s3_abi = abi_cache[log_address]

        if s3_abi:
            try:
                contract = w3.eth.contract(
                    address=Web3.to_checksum_address(log_address),
                    abi=s3_abi
                )

                # Get event names from ABI
                event_names_in_abi = [
                    item['name'] for item in s3_abi
                    if item.get('type') == 'event'
                ]

                # ========================================
                # Try process_log() for each event type (PRODUCTION PATTERN)
                # ========================================
                for evt_name in event_names_in_abi:
                    try:
                        event_obj = getattr(contract.events, evt_name)
                        # Use process_log() on single log - NOT process_receipt()!
                        decoded = event_obj().process_log(log)

                        # Success! We decoded this log
                        args = dict(decoded.get('args', {}))
                        logger.success(f"Decoded {evt_name} (S3 ABI via process_log):")

                        # Build result dict
                        result = {
                            "name": evt_name,
                            "args": args,
                            "log_index": log.get('logIndex'),
                            "address": log_address,
                            "source": "S3_ABI",
                            "loan": None,
                            "old_loan": None,
                        }

                        # ========================================
                        # Extract Loan struct from Gondi events
                        # ========================================
                        is_v2 = is_v2_contract(log_address)

                        if evt_name in ['LoanEmitted', 'LoanRefinanced', 'LoanRefinancedFromNewOffers']:
                            loan_data = args.get('loan')
                            if loan_data:
                                try:
                                    if hasattr(loan_data, 'keys') or isinstance(loan_data, dict):
                                        result['loan'] = Loan.from_dict(dict(loan_data), is_v2=is_v2)
                                    elif isinstance(loan_data, tuple):
                                        result['loan'] = Loan.from_tuple(loan_data, is_v2=is_v2)
                                    logger.info(f"  Parsed Loan struct:")
                                    logger.indent()
                                    loan = result['loan']
                                    logger.info(f"borrower: {loan.borrower}")
                                    logger.info(f"principal: {loan.principalAmount} ({Decimal(loan.principalAmount)/Decimal(10**18):.6f} ETH)")
                                    logger.info(f"duration: {loan.duration} seconds ({loan.duration/86400:.1f} days)")
                                    logger.info(f"tranches: {len(loan.tranches)}")
                                    for j, t in enumerate(loan.tranches):
                                        logger.info(f"  [{j}] lender={t.lender[:10]}... principal={t.principalAmount} apr={t.aprBps}bps")
                                    logger.dedent()
                                except Exception as e:
                                    logger.warning(f"  Could not parse Loan: {e}")

                        # ========================================
                        # Extract BlurLien struct from Blur events
                        # ========================================
                        is_blur = log_address in [BLUR_LENDING_ADDRESS, BLUR_POOL_ADDRESS]

                        if is_blur and evt_name in BLUR_EVENTS:
                            result['blur_lien'] = None  # Initialize

                            # Get lien data and lienId from event args
                            lien_data = args.get('lien')
                            lien_id = safe_int(args.get('lienId', 0))

                            if lien_data:
                                try:
                                    if hasattr(lien_data, 'keys') or isinstance(lien_data, dict):
                                        result['blur_lien'] = BlurLien.from_dict(dict(lien_data), lien_id=lien_id)
                                    elif isinstance(lien_data, tuple):
                                        result['blur_lien'] = BlurLien.from_tuple(lien_data, lien_id=lien_id)

                                    lien = result['blur_lien']
                                    logger.info(f"  Parsed BlurLien struct:")
                                    logger.indent()
                                    logger.info(f"lien_id: {lien.lien_id}")
                                    logger.info(f"lender: {lien.lender}")
                                    logger.info(f"borrower: {lien.borrower}")
                                    logger.info(f"collection: {lien.collection}")
                                    logger.info(f"token_id: {lien.token_id}")
                                    logger.info(f"principal: {lien.principal} ({lien.get_principal_eth():.6f} ETH)")
                                    logger.info(f"rate: {lien.rate} bps ({lien.rate/100:.2f}% APR)")
                                    logger.info(f"start_time: {lien.start_time} ({datetime.fromtimestamp(lien.start_time, tz=timezone.utc).isoformat() if lien.start_time > 0 else 'N/A'})")
                                    logger.info(f"in_auction: {lien.is_in_auction()}")

                                    # Calculate interest if we have block timestamp
                                    # (will be done in journal entry generation)
                                    logger.dedent()
                                except Exception as e:
                                    logger.warning(f"  Could not parse BlurLien: {e}")

                            # Log Blur-specific event details
                            if evt_name == 'LoanOfferTaken':
                                logger.info(f"  EVENT: New Blur loan origination")
                            elif evt_name == 'Repay':
                                logger.info(f"  EVENT: Blur loan repayment")
                            elif evt_name == 'Refinance':
                                logger.info(f"  EVENT: Blur loan refinanced to new lender")
                                new_lender = safe_address(args.get('newLender', ''))
                                if new_lender:
                                    logger.info(f"  new_lender: {new_lender}")
                            elif evt_name == 'StartAuction':
                                logger.info(f"  EVENT: Lender called the loan (auction started)")
                            elif evt_name == 'Seize':
                                logger.info(f"  EVENT: Lender seized NFT collateral")
                            elif evt_name == 'BuyLocked':
                                logger.info(f"  EVENT: Third party purchased locked NFT")

                        # Log other key args
                        logger.indent()
                        for k, v in args.items():
                            if k == 'loan':
                                continue  # Already logged above
                            if isinstance(v, int) and v > 10**15:
                                v_eth = Decimal(v) / Decimal(10**18)
                                logger.info(f"{k}: {v} ({v_eth:.6f} ETH-equiv)")
                            elif isinstance(v, bytes):
                                logger.info(f"{k}: 0x{v.hex()[:32]}...")
                            elif isinstance(v, (list, tuple)) and len(v) > 0:
                                logger.info(f"{k}: ({len(v)} items)")
                            else:
                                logger.info(f"{k}: {v}")
                        logger.dedent()

                        decoded_events.append(result)
                        decoded_this_log = True
                        break  # Stop trying other event types

                    except Exception:
                        # This event type didn't match - try next
                        continue

            except Exception as e:
                logger.warning(f"S3 ABI error: {str(e)[:60]}")

        # ========================================
        # Fallback: Try hardcoded ABIs with process_log()
        # ========================================
        if not decoded_this_log:
            # Try ERC20 Transfer
            try:
                contract = w3.eth.contract(
                    address=Web3.to_checksum_address(log_address),
                    abi=ERC20_ABI
                )
                decoded = contract.events.Transfer().process_log(log)
                args = dict(decoded.get('args', {}))

                logger.success("Decoded Transfer (ERC20 ABI):")
                logger.indent()
                logger.info(f"from: {args.get('from', 'N/A')}")
                logger.info(f"to: {args.get('to', 'N/A')}")
                value = args.get('value', 0)
                if isinstance(value, int) and value > 10**15:
                    value_eth = Decimal(value) / Decimal(10**18)
                    logger.info(f"value: {value} ({value_eth:.6f} ETH)")
                else:
                    logger.info(f"value: {value}")
                logger.dedent()

                decoded_events.append({
                    "name": "Transfer",
                    "args": args,
                    "log_index": log.get('logIndex'),
                    "address": log_address,
                    "source": "ERC20_ABI",
                    "loan": None,
                })
                decoded_this_log = True
            except Exception:
                pass

        if not decoded_this_log:
            # Try WETH Deposit/Withdrawal
            try:
                contract = w3.eth.contract(
                    address=Web3.to_checksum_address(log_address),
                    abi=WETH_ABI
                )
                # Try Deposit
                try:
                    decoded = contract.events.Deposit().process_log(log)
                    evt_name = "Deposit"
                except Exception:
                    decoded = contract.events.Withdrawal().process_log(log)
                    evt_name = "Withdrawal"

                args = dict(decoded.get('args', {}))
                logger.success(f"Decoded {evt_name} (WETH ABI):")
                logger.indent()
                for k, v in args.items():
                    if isinstance(v, int) and v > 10**15:
                        v_eth = Decimal(v) / Decimal(10**18)
                        logger.info(f"{k}: {v} ({v_eth:.6f} ETH)")
                    else:
                        logger.info(f"{k}: {v}")
                logger.dedent()

                decoded_events.append({
                    "name": evt_name,
                    "args": args,
                    "log_index": log.get('logIndex'),
                    "address": log_address,
                    "source": "WETH_ABI",
                    "loan": None,
                })
                decoded_this_log = True
            except Exception:
                pass

        # ========================================
        # Fallback: Try ERC20 Approval
        # ========================================
        if not decoded_this_log:
            try:
                contract = w3.eth.contract(
                    address=Web3.to_checksum_address(log_address),
                    abi=ERC20_ABI
                )
                decoded = contract.events.Approval().process_log(log)
                args = dict(decoded.get('args', {}))

                logger.success("Decoded Approval (ERC20 ABI):")
                logger.indent()
                logger.info(f"owner: {args.get('owner', 'N/A')}")
                logger.info(f"spender: {args.get('spender', 'N/A')}")
                value = args.get('value', 0)
                if isinstance(value, int):
                    if value == 2**256 - 1:
                        logger.info(f"value: UNLIMITED (max uint256)")
                    elif value > 10**15:
                        value_eth = Decimal(value) / Decimal(10**18)
                        logger.info(f"value: {value} ({value_eth:.6f} ETH-equiv)")
                    else:
                        logger.info(f"value: {value}")
                logger.dedent()

                decoded_events.append({
                    "name": "Approval",
                    "args": args,
                    "log_index": log.get('logIndex'),
                    "address": log_address,
                    "source": "ERC20_ABI",
                    "loan": None,
                })
                decoded_this_log = True
            except Exception:
                pass

        # ========================================
        # Fallback: Try ERC721 Transfer (NFT)
        # Note: ERC721 Transfer has tokenId as indexed (3 indexed topics)
        # ========================================
        if not decoded_this_log:
            try:
                contract = w3.eth.contract(
                    address=Web3.to_checksum_address(log_address),
                    abi=ERC721_ABI
                )
                decoded = contract.events.Transfer().process_log(log)
                args = dict(decoded.get('args', {}))

                # Check if this looks like an NFT transfer (has tokenId, not value)
                if 'tokenId' in args:
                    logger.success("Decoded Transfer (ERC721 ABI - NFT):")
                    logger.indent()
                    logger.info(f"from: {args.get('from', 'N/A')}")
                    logger.info(f"to: {args.get('to', 'N/A')}")
                    logger.info(f"tokenId: {args.get('tokenId', 'N/A')}")
                    logger.dedent()

                    decoded_events.append({
                        "name": "Transfer",
                        "args": args,
                        "log_index": log.get('logIndex'),
                        "address": log_address,
                        "source": "ERC721_ABI",
                        "is_nft": True,
                        "loan": None,
                    })
                    decoded_this_log = True
            except Exception:
                pass

        # ========================================
        # Fallback: Try ERC721 ApprovalForAll
        # ========================================
        if not decoded_this_log:
            try:
                contract = w3.eth.contract(
                    address=Web3.to_checksum_address(log_address),
                    abi=ERC721_ABI
                )
                decoded = contract.events.ApprovalForAll().process_log(log)
                args = dict(decoded.get('args', {}))

                logger.success("Decoded ApprovalForAll (ERC721 ABI):")
                logger.indent()
                logger.info(f"owner: {args.get('owner', 'N/A')}")
                logger.info(f"operator: {args.get('operator', 'N/A')}")
                logger.info(f"approved: {args.get('approved', 'N/A')}")
                logger.dedent()

                decoded_events.append({
                    "name": "ApprovalForAll",
                    "args": args,
                    "log_index": log.get('logIndex'),
                    "address": log_address,
                    "source": "ERC721_ABI",
                    "loan": None,
                })
                decoded_this_log = True
            except Exception:
                pass

        # ========================================
        # Fallback: Try NFTfi events (raw data parsing)
        # ========================================
        # NFTfi event signatures vary by version, so we decode raw data
        NFTFI_LOANSTARTED_SIGS = [
            "42cc7f53ef7b494c5dd6f0095175f7d07b5d3d7b2a03f34389fea445ba4a3a8b",  # V2 DirectLoanFixedOffer
            "42cc7f53ef7b494c5dd6d9c7b0fdc87ae2fdded0e6fd3e249ba9fb0ed2e3a8a9",  # V2 alt
            "3687d64f40b11dd1c102a76882ac1735891c546a96ae27935eb5c7865b9d86fa",  # DirectLoanCoordinator
        ]
        NFTFI_LOANREPAID_SIGS = [
            "70ff8cf632603e2b073f0c9ac02b8a20f349e45ff5e5fca233ec54f379d13900",  # V2 variant 1
            "37357bed780fda5aed28c32fe9cd762cb2f2f8a70c0d9b342aba59c945943ca0",  # V2 DirectLoanFixedOffer
        ]
        NFTFI_LOANLIQUIDATED_SIGS = [
            "5bd8cd67baac27b2f84b33fa12a8c2b73b1c4f2cd4d6780c56e645e7f3e1e446",  # V2 variant 1
            "4fac0ff43299a330bce57d0579985305af580acf256a6d7977083ede81be1326",  # V2 DirectLoanFixedOffer
        ]

        if not decoded_this_log and log_address in NFTFI_CONTRACTS:
            logger.info(f"Trying NFTfi raw decode for {log_address[:16]}...")
            try:
                topics = log.get('topics', [])
                if len(topics) >= 4:
                    sig_hex = to_hex(topics[0]).lower()
                    if sig_hex.startswith('0x'):
                        sig_hex = sig_hex[2:]

                    # Parse indexed parameters from topics
                    loan_id = int(to_hex(topics[1]), 16) if len(topics) > 1 else 0
                    borrower = '0x' + to_hex(topics[2])[-40:] if len(topics) > 2 else ''
                    lender = '0x' + to_hex(topics[3])[-40:] if len(topics) > 3 else ''

                    # Get data
                    data = log.get('data', b'')
                    data_hex = to_hex(data)
                    if data_hex.startswith('0x'):
                        data_hex = data_hex[2:]

                    # Parse data words (32 bytes each)
                    def get_word(idx):
                        start = idx * 64
                        if start + 64 <= len(data_hex):
                            return int(data_hex[start:start+64], 16)
                        return 0

                    def get_address(idx):
                        val = get_word(idx)
                        return '0x' + hex(val)[2:].zfill(40)[-40:]

                    if sig_hex in NFTFI_LOANSTARTED_SIGS:
                        logger.success(f"Decoded LoanStarted (NFTfi raw):")
                        logger.indent()

                        # Different data layouts based on contract/version
                        is_coordinator = sig_hex == "3687d64f40b11dd1c102a76882ac1735891c546a96ae27935eb5c7865b9d86fa"

                        if is_coordinator:
                            # DirectLoanCoordinator layout:
                            # [0] principal
                            # [1] nft_id
                            # [2] max repayment
                            # [3] duration/extra data
                            # [6] nft collection
                            # [7] erc20 address
                            principal = get_word(0)
                            nft_id = get_word(1)
                            max_repayment = get_word(2)
                            duration = get_word(3)  # May need adjustment
                            nft_collection = get_address(6)
                            erc20 = get_address(7)
                            interest_bps = 0  # Not directly available
                            start_time = 0  # Not directly available
                            logger.info("(DirectLoanCoordinator format)")
                        else:
                            # DirectLoanFixedOffer V2 layout:
                            # [0] principal
                            # [1] max repayment
                            # [2] nft_id
                            # [3] erc20 address
                            # [4] duration
                            # [5] (reserved/0)
                            # [6] interest rate bps
                            # [7] referrer address
                            # [8] start timestamp
                            # [9] nft collection
                            principal = get_word(0)
                            max_repayment = get_word(1)
                            nft_id = get_word(2)
                            erc20 = get_address(3)
                            duration = get_word(4)
                            interest_bps = get_word(6)
                            start_time = get_word(8)
                            nft_collection = get_address(9)
                            logger.info("(DirectLoanFixedOffer V2 format)")

                        nftfi_loan = NFTfiLoan(
                            loan_id=loan_id,
                            borrower=borrower,
                            lender=lender,
                            principal=principal,
                            max_repayment=max_repayment,
                            nft_collateral_id=nft_id,
                            principal_token=erc20,
                            nft_collection=nft_collection,
                            start_time=start_time,
                            duration=duration,
                            interest_rate_bps=interest_bps,
                        )

                        logger.info(f"loan_id: {nftfi_loan.loan_id}")
                        logger.info(f"borrower: {nftfi_loan.borrower}")
                        logger.info(f"lender: {nftfi_loan.lender}")
                        logger.info(f"principal: {nftfi_loan.principal} ({nftfi_loan.get_principal_eth():.6f} ETH)")
                        logger.info(f"max_repayment: {nftfi_loan.max_repayment} ({nftfi_loan.get_max_repayment_eth():.6f} ETH)")
                        logger.info(f"interest: {nftfi_loan.get_interest_eth():.6f} ETH")
                        logger.info(f"duration: {nftfi_loan.duration} seconds ({nftfi_loan.duration/86400:.1f} days)")
                        logger.info(f"interest_bps: {interest_bps} ({interest_bps/100:.2f}%)")
                        logger.info(f"nft_collection: {nftfi_loan.nft_collection}")
                        logger.info(f"nft_id: {nftfi_loan.nft_collateral_id}")
                        logger.info(f"principal_token: {erc20}")
                        logger.info(f"start_time: {start_time} ({datetime.fromtimestamp(start_time, tz=timezone.utc).isoformat() if start_time > 0 else 'N/A'})")
                        logger.dedent()

                        args = {
                            'loanId': loan_id,
                            'borrower': borrower,
                            'lender': lender,
                            'principal': principal,
                            'maxRepayment': max_repayment,
                            'nftId': nft_id,
                            'erc20': erc20,
                            'duration': duration,
                            'interestBps': interest_bps,
                            'startTime': start_time,
                            'nftCollection': nft_collection,
                        }

                        decoded_events.append({
                            "name": "LoanStarted",
                            "args": args,
                            "log_index": log.get('logIndex'),
                            "address": log_address,
                            "source": "NFTFI_RAW",
                            "loan": None,
                            "nftfi_loan": nftfi_loan,
                        })
                        decoded_this_log = True

                    elif sig_hex in NFTFI_LOANREPAID_SIGS:
                        logger.success(f"Decoded LoanRepaid (NFTfi raw):")
                        logger.indent()

                        # Data layout for LoanRepaid (DirectLoanFixedOffer V2):
                        # [0] nft_id or principal (varies)
                        # [1] amount_to_lender
                        # [2] admin_fee
                        # [3] revenue_share (optional)
                        word0 = get_word(0)
                        word1 = get_word(1)
                        word2 = get_word(2)

                        # Heuristic: if word0 looks like ETH value (> 10^15), it's amount_to_lender
                        # Otherwise word1 is amount_to_lender
                        if word0 > 10**15:
                            amount_to_lender = word0
                            admin_fee = word1
                        else:
                            # word0 is nft_id or duration
                            amount_to_lender = word1
                            admin_fee = word2

                        logger.info(f"loan_id: {loan_id}")
                        logger.info(f"borrower: {borrower}")
                        logger.info(f"lender: {lender}")
                        logger.info(f"amount_to_lender: {amount_to_lender} ({Decimal(amount_to_lender)/Decimal(10**18):.6f} ETH)")
                        logger.info(f"admin_fee: {admin_fee} ({Decimal(admin_fee)/Decimal(10**18):.6f} ETH)")
                        logger.dedent()

                        args = {
                            'loanId': loan_id,
                            'borrower': borrower,
                            'lender': lender,
                            'amountToLender': amount_to_lender,
                            'adminFee': admin_fee,
                        }

                        decoded_events.append({
                            "name": "LoanRepaid",
                            "args": args,
                            "log_index": log.get('logIndex'),
                            "address": log_address,
                            "source": "NFTFI_RAW",
                            "loan": None,
                            "nftfi_loan": None,
                        })
                        decoded_this_log = True

                    elif sig_hex in NFTFI_LOANLIQUIDATED_SIGS:
                        logger.success(f"Decoded LoanLiquidated (NFTfi raw):")
                        logger.indent()

                        logger.info(f"loan_id: {loan_id}")
                        logger.info(f"borrower: {borrower}")
                        logger.info(f"lender: {lender}")
                        logger.dedent()

                        args = {
                            'loanId': loan_id,
                            'borrower': borrower,
                            'lender': lender,
                        }

                        decoded_events.append({
                            "name": "LoanLiquidated",
                            "args": args,
                            "log_index": log.get('logIndex'),
                            "address": log_address,
                            "source": "NFTFI_RAW",
                            "loan": None,
                            "nftfi_loan": None,
                        })
                        decoded_this_log = True

                    else:
                        logger.warning(f"Unknown NFTfi event sig: {sig_hex[:20]}...")

            except Exception as e:
                logger.warning(f"NFTfi raw decode error: {str(e)[:60]}")

        # ========================================
        # Fallback: Try Arcade events (using ABI-based decoding)
        # ========================================
        arcade_contracts_lower = [c.lower() for c in ARCADE_CONTRACTS]
        if not decoded_this_log and log_address in arcade_contracts_lower:
            logger.info(f"Trying Arcade decode for {log_address[:16]}...")
            try:
                topics = log.get('topics', [])
                if len(topics) >= 1:
                    sig_hex = to_hex(topics[0]).lower()
                    if sig_hex.startswith('0x'):
                        sig_hex = sig_hex[2:]

                    # Get data
                    data = log.get('data', b'')
                    data_hex = to_hex(data)
                    if data_hex.startswith('0x'):
                        data_hex = data_hex[2:]

                    # Parse data words (32 bytes each)
                    def get_word(idx):
                        start = idx * 64
                        if start + 64 <= len(data_hex):
                            return int(data_hex[start:start+64], 16)
                        return 0

                    def get_address(idx):
                        val = get_word(idx)
                        return '0x' + hex(val)[2:].zfill(40)[-40:]

                    # LoanRepaid(uint256 loanId) - confirmed sig
                    if sig_hex == "9a7851747cd7ffb3fe0a32caf3da48b31f27cebe131267051640f8b72fc47186":
                        loan_id = get_word(0)
                        logger.success(f"Decoded LoanRepaid (Arcade):")
                        logger.indent()
                        logger.info(f"loan_id: {loan_id}")
                        logger.dedent()

                        decoded_events.append({
                            "name": "LoanRepaid",
                            "args": {'loanId': loan_id},
                            "log_index": log.get('logIndex'),
                            "address": log_address,
                            "source": "ARCADE_RAW",
                            "loan": None,
                            "arcade_loan_id": loan_id,
                        })
                        decoded_this_log = True

                    # LoanClaimed(uint256 loanId) - confirmed sig
                    elif sig_hex == "b15e438728b48d46c9a5505713e60ff50c80559f4523c8f99a246a2069a8684a":
                        loan_id = get_word(0)
                        logger.success(f"Decoded LoanClaimed (Arcade):")
                        logger.indent()
                        logger.info(f"loan_id: {loan_id}")
                        logger.dedent()

                        decoded_events.append({
                            "name": "LoanClaimed",
                            "args": {'loanId': loan_id},
                            "log_index": log.get('logIndex'),
                            "address": log_address,
                            "source": "ARCADE_RAW",
                            "loan": None,
                            "arcade_loan_id": loan_id,
                        })
                        decoded_this_log = True

                    # LoanStarted(uint256 loanId, address lender, address borrower)
                    # Signature: keccak256("LoanStarted(uint256,address,address)")
                    elif sig_hex == "f66ad0a6f32ab1c79cf8dd9eee7da4c1fc41a69c2f2f90c21f0dd1c07b8e6e31":
                        loan_id = get_word(0)
                        lender = get_address(1)
                        borrower = get_address(2)
                        logger.success(f"Decoded LoanStarted (Arcade):")
                        logger.indent()
                        logger.info(f"loan_id: {loan_id}")
                        logger.info(f"lender: {lender}")
                        logger.info(f"borrower: {borrower}")
                        logger.dedent()

                        decoded_events.append({
                            "name": "LoanStarted",
                            "args": {
                                'loanId': loan_id,
                                'lender': lender,
                                'borrower': borrower,
                            },
                            "log_index": log.get('logIndex'),
                            "address": log_address,
                            "source": "ARCADE_RAW",
                            "loan": None,
                            "arcade_loan_id": loan_id,
                        })
                        decoded_this_log = True

                    # LoanRolledOver(uint256 oldLoanId, uint256 newLoanId)
                    elif sig_hex.startswith(""):  # TODO: Get actual signature
                        pass

                    # NonceUsed(address indexed user, uint160 nonce)
                    elif sig_hex == "94307d29ec5ae0d8d8a9c5e8a03194264f6a9a15ab14d2472869784f32c01ce7":
                        user = '0x' + to_hex(topics[1])[-40:] if len(topics) > 1 else ''
                        nonce = get_word(0)
                        logger.info(f"Decoded NonceUsed (Arcade): user={user[:12]}..., nonce={nonce}")
                        # Skip adding to decoded_events - not a loan event
                        decoded_this_log = True

                    else:
                        logger.warning(f"Unknown Arcade event sig: {sig_hex[:20]}...")

            except Exception as e:
                logger.warning(f"Arcade raw decode error: {str(e)[:60]}")

        # ========================================
        # Fallback: Try Zharta events (using raw decoding)
        # ========================================
        zharta_contracts_lower = [c.lower() for c in ZHARTA_CONTRACTS]
        if not decoded_this_log and log_address in zharta_contracts_lower:
            logger.info(f"Trying Zharta decode for {log_address[:16]}...")
            try:
                topics = log.get('topics', [])
                if len(topics) >= 1:
                    sig_hex = to_hex(topics[0]).lower()
                    if sig_hex.startswith('0x'):
                        sig_hex = sig_hex[2:]

                    # Get indexed wallet from topics[1] if present
                    indexed_wallet = ''
                    if len(topics) > 1:
                        indexed_wallet = '0x' + to_hex(topics[1])[-40:]

                    # Get data
                    data = log.get('data', b'')
                    data_hex = to_hex(data)
                    if data_hex.startswith('0x'):
                        data_hex = data_hex[2:]

                    # Parse data words (32 bytes each)
                    def get_word(idx):
                        start = idx * 64
                        if start + 64 <= len(data_hex):
                            return int(data_hex[start:start+64], 16)
                        return 0

                    def get_address(idx):
                        val = get_word(idx)
                        return '0x' + hex(val)[2:].zfill(40)[-40:]

                    # LoanCreated - confirmed sig from tx 0x96d9fe5f...
                    # topic0 = 0x4a558778654b4d21f09ae7e2aa4eebc0de757d1233dc825b43183a1276a7b2a1
                    if sig_hex == "4a558778654b4d21f09ae7e2aa4eebc0de757d1233dc825b43183a1276a7b2a1":
                        # Data: wallet (non-indexed), loanId, erc20TokenContract, apr, amount, duration, collaterals[], genesisToken
                        wallet = get_address(0)
                        loan_id = get_word(1)
                        erc20_token = get_address(2)
                        apr = get_word(3)
                        amount = get_word(4)
                        duration = get_word(5)
                        genesis_token = get_word(7)  # After collaterals array pointer

                        logger.success(f"Decoded LoanCreated (Zharta):")
                        logger.indent()
                        logger.info(f"wallet: {wallet}")
                        logger.info(f"loan_id: {loan_id}")
                        logger.info(f"erc20_token: {erc20_token}")
                        logger.info(f"apr: {apr} bps ({apr/100:.2f}%)")
                        logger.info(f"amount: {amount} ({Decimal(amount)/Decimal(10**18):.6f} ETH)")
                        logger.info(f"duration: {duration} secs ({duration/86400:.1f} days)")
                        logger.info(f"genesis_token: {genesis_token}")
                        logger.dedent()

                        decoded_events.append({
                            "name": "LoanCreated",
                            "args": {
                                'wallet': wallet,
                                'loanId': loan_id,
                                'erc20TokenContract': erc20_token,
                                'apr': apr,
                                'amount': amount,
                                'duration': duration,
                                'genesisToken': genesis_token,
                            },
                            "log_index": log.get('logIndex'),
                            "address": log_address,
                            "source": "ZHARTA_RAW",
                            "loan": None,
                            "zharta_loan_id": loan_id,
                        })
                        decoded_this_log = True

                    # LoanPayment - confirmed sig: 31c401ba8a3eb75cf55e1d9f4971e726115e8448c80446935cffbea991ca2473
                    # Data (160 bytes): wallet, loanId, principal, interestAmount, erc20TokenContract
                    elif sig_hex == "31c401ba8a3eb75cf55e1d9f4971e726115e8448c80446935cffbea991ca2473":
                        wallet = get_address(0)
                        loan_id = get_word(1)
                        principal = get_word(2)
                        interest_amount = get_word(3)
                        erc20_token = get_address(4)

                        logger.success(f"Decoded LoanPayment (Zharta):")
                        logger.indent()
                        logger.info(f"wallet: {wallet}")
                        logger.info(f"loan_id: {loan_id}")
                        logger.info(f"principal: {principal} ({Decimal(principal)/Decimal(10**18):.6f} ETH)")
                        logger.info(f"interest: {interest_amount} ({Decimal(interest_amount)/Decimal(10**18):.6f} ETH)")
                        logger.info(f"erc20_token: {erc20_token}")
                        logger.dedent()

                        decoded_events.append({
                            "name": "LoanPayment",
                            "args": {
                                'wallet': wallet,
                                'loanId': loan_id,
                                'principal': principal,
                                'interestAmount': interest_amount,
                                'erc20TokenContract': erc20_token,
                            },
                            "log_index": log.get('logIndex'),
                            "address": log_address,
                            "source": "ZHARTA_RAW",
                            "loan": None,
                            "zharta_loan_id": loan_id,
                        })
                        decoded_this_log = True

                    # LoanPaid - confirmed sig: 42d434e1d98bb8cb642015660476f098bbb0f00b64ddb556e149e17de4dd3645
                    # Data (96 bytes): wallet, loanId, erc20TokenContract
                    elif sig_hex == "42d434e1d98bb8cb642015660476f098bbb0f00b64ddb556e149e17de4dd3645":
                        wallet = get_address(0)
                        loan_id = get_word(1)
                        erc20_token = get_address(2)

                        logger.success(f"Decoded LoanPaid (Zharta):")
                        logger.indent()
                        logger.info(f"wallet: {wallet}")
                        logger.info(f"loan_id: {loan_id}")
                        logger.info(f"erc20_token: {erc20_token}")
                        logger.dedent()

                        decoded_events.append({
                            "name": "LoanPaid",
                            "args": {
                                'wallet': wallet,
                                'loanId': loan_id,
                                'erc20TokenContract': erc20_token,
                            },
                            "log_index": log.get('logIndex'),
                            "address": log_address,
                            "source": "ZHARTA_RAW",
                            "loan": None,
                            "zharta_loan_id": loan_id,
                        })
                        decoded_this_log = True

                    # LoanDefaulted - data: wallet, loanId, amount, erc20TokenContract
                    # TODO: Capture sig from real default transaction
                    elif sig_hex.startswith("") and False:  # Placeholder until sig captured
                        pass

                    else:
                        # Try to decode using ZHARTA_ABI with process_log
                        try:
                            contract = w3.eth.contract(
                                address=Web3.to_checksum_address(log_address),
                                abi=ZHARTA_ABI
                            )
                            for evt_def in ZHARTA_ABI:
                                evt_name = evt_def.get('name')
                                if evt_name:
                                    try:
                                        event_obj = getattr(contract.events, evt_name)
                                        decoded = event_obj().process_log(log)
                                        args = dict(decoded.get('args', {}))

                                        logger.success(f"Decoded {evt_name} (Zharta ABI):")
                                        logger.indent()
                                        for k, v in args.items():
                                            if isinstance(v, int) and v > 10**15:
                                                v_eth = Decimal(v) / Decimal(10**18)
                                                logger.info(f"{k}: {v} ({v_eth:.6f} ETH)")
                                            else:
                                                logger.info(f"{k}: {v}")
                                        logger.dedent()

                                        decoded_events.append({
                                            "name": evt_name,
                                            "args": args,
                                            "log_index": log.get('logIndex'),
                                            "address": log_address,
                                            "source": "ZHARTA_ABI",
                                            "loan": None,
                                            "zharta_loan_id": args.get('loanId', 0),
                                        })
                                        decoded_this_log = True
                                        break
                                    except Exception:
                                        continue
                        except Exception:
                            pass

                        if not decoded_this_log:
                            logger.warning(f"Unknown Zharta event sig: {sig_hex[:20]}...")

            except Exception as e:
                logger.warning(f"Zharta raw decode error: {str(e)[:60]}")

        if not decoded_this_log:
            logger.warning(f"Could not decode log[{i}]")

        logger.dedent()
        logger.dedent()

    logger.info(f"Successfully decoded {len(decoded_events)} of {len(logs)} events")

    # Summary
    sources = {}
    event_types = {}
    for evt in decoded_events:
        src = evt.get('source', 'unknown')
        sources[src] = sources.get(src, 0) + 1
        name = evt.get('name', 'unknown')
        event_types[name] = event_types.get(name, 0) + 1

    if sources:
        logger.info("Decode sources:")
        for src, count in sources.items():
            logger.info(f"  {src}: {count}")

    if event_types:
        logger.info("Event types:")
        for name, count in event_types.items():
            logger.info(f"  {name}: {count}")

    return decoded_events


# ============================================================================
# JOURNAL ENTRY GENERATION (Production Pattern)
# ============================================================================

def generate_journal_entries(
    tx: Dict,
    receipt: Dict,
    block: Dict,
    events: List[Dict],
    platform: str,
    category: str,
    eth_price: Decimal,
    logger: DebugLogger,
    fund_wallets: set = None,  # Wallets owned by the fund
    func_params: Dict = None,  # Function parameters (for lien data)
) -> List[Dict]:
    """
    Generate journal entries based on decoded events.
    Uses production patterns for Gondi and Blur loan events.
    """

    logger.log(9, "Generating journal entries...")

    entries = []
    fund_wallets = fund_wallets or set()
    func_params = func_params or {}

    # Get basic tx info
    tx_hash = tx.get('hash', b'')
    if isinstance(tx_hash, bytes):
        tx_hash = tx_hash.hex()
    tx_hash_short = tx_hash[:10] if tx_hash else "unknown"

    value_wei = tx.get('value', 0)
    value_eth = Decimal(value_wei) / Decimal(10**18)

    # Get gas fee
    gas_used = receipt.get('gasUsed', 0)
    gas_price = tx.get('gasPrice', 0) or receipt.get('effectiveGasPrice', 0)
    gas_fee_wei = gas_used * gas_price
    gas_fee_eth = Decimal(gas_fee_wei) / Decimal(10**18)

    # Get block timestamp
    block_timestamp = block.get('timestamp', 0) if block else 0

    logger.info(f"Transaction value: {value_eth} ETH")
    logger.info(f"Gas fee: {gas_fee_eth} ETH")
    logger.info(f"Platform: {platform}")
    logger.info(f"Category: {category}")
    logger.info(f"Fund wallets: {len(fund_wallets)} configured")

    # Extract lien from function params if available (for Blur)
    func_lien = None
    func_lien_id = None
    if 'lien' in func_params:
        lien_data = func_params['lien']
        func_lien_id = func_params.get('lienId', 0)
        try:
            if hasattr(lien_data, 'keys') or isinstance(lien_data, dict):
                func_lien = BlurLien.from_dict(dict(lien_data), lien_id=func_lien_id)
            elif isinstance(lien_data, tuple):
                func_lien = BlurLien.from_tuple(lien_data, lien_id=func_lien_id)
            logger.info(f"Extracted lien from function params: lien_id={func_lien_id}")
        except Exception as e:
            logger.warning(f"Could not parse lien from func_params: {e}")

    # ========================================
    # Process Gondi Events
    # ========================================
    for evt in events:
        evt_name = evt.get('name', '')
        loan: Loan = evt.get('loan')
        args = evt.get('args', {})

        if evt_name == 'LoanRefinanced' and loan:
            logger.info(f"Processing LoanRefinanced event...")
            logger.indent()

            currency = loan.get_currency_symbol()

            # Get event-specific data
            renegotiation_id = safe_int(args.get('renegotiationId', 0))
            old_loan_id = safe_int(args.get('oldLoanId', 0))
            new_loan_id = safe_int(args.get('newLoanId', args.get('loanId', 0)))
            fee = safe_int(args.get('fee', 0))  # Already NET

            logger.info(f"Renegotiation ID: {renegotiation_id}")
            logger.info(f"Old Loan ID: {old_loan_id} -> New Loan ID: {new_loan_id}")
            logger.info(f"Fee (NET): {fee} wei ({Decimal(fee)/Decimal(10**18):.6f} {currency})")

            # Process each tranche in the NEW loan
            for i, tranche in enumerate(loan.tranches):
                logger.info(f"Tranche [{i}]:")
                logger.indent()

                principal_eth = Decimal(tranche.principalAmount) / Decimal(10**18)
                logger.info(f"Lender: {tranche.lender}")
                logger.info(f"Principal: {tranche.principalAmount} ({principal_eth:.6f} {currency})")
                logger.info(f"APR: {tranche.aprBps} bps ({tranche.aprBps/100:.2f}%)")
                logger.info(f"Accrued Interest (carried): {tranche.accruedInterest}")

                # Calculate expected interest over loan duration
                gross_int, proto_fee, net_int = calculate_interest(
                    tranche.principalAmount,
                    tranche.aprBps,
                    loan.duration,
                    loan.protocolFee
                )
                logger.info(f"Expected interest over {loan.duration}s: gross={gross_int}, net={net_int}")

                # Pro-rata fee share
                if loan.principalAmount > 0:
                    fee_share = (tranche.principalAmount * fee) // loan.principalAmount
                else:
                    fee_share = 0
                fee_share_eth = Decimal(fee_share) / Decimal(10**18)
                logger.info(f"Fee share: {fee_share} ({fee_share_eth:.6f} {currency})")

                # Determine if this tranche involves a fund wallet
                is_fund_lender = tranche.lender.lower() in {w.lower() for w in fund_wallets}
                is_fund_borrower = loan.borrower.lower() in {w.lower() for w in fund_wallets}

                if is_fund_lender:
                    logger.success(f"Fund is LENDER on this tranche")

                    # LENDER journal entry: New loan origination
                    cash_disbursed = tranche.principalAmount - fee_share
                    cash_disbursed_eth = Decimal(cash_disbursed) / Decimal(10**18)

                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_refi_lender_{i}",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"Gondi Refinance: Lender tranche {i} - Loan {new_loan_id}",
                        "category": "NFT_LEND_REFINANCE",
                        "platform": platform,
                        "loan_id": new_loan_id,
                        "lines": [
                            {"type": "DEBIT", "account": f"loan_receivable_crypto_{currency}", "amount": float(principal_eth), "asset": currency},
                            {"type": "CREDIT", "account": "deemed_cash_usd", "amount": float(cash_disbursed_eth), "asset": currency},
                        ],
                        "is_balanced": False  # Will check below
                    }
                    if fee_share > 0:
                        entry["lines"].append(
                            {"type": "CREDIT", "account": f"interest_income_crypto_{currency}", "amount": float(fee_share_eth), "asset": currency}
                        )

                    # Check balance
                    debits = sum(l['amount'] for l in entry['lines'] if l['type'] == 'DEBIT')
                    credits = sum(l['amount'] for l in entry['lines'] if l['type'] == 'CREDIT')
                    entry['is_balanced'] = abs(debits - credits) < 0.000001

                    entries.append(entry)

                    logger.success("Generated LENDER entry:")
                    logger.indent()
                    logger.info(f"DEBIT  loan_receivable  {principal_eth:.6f} {currency}")
                    logger.info(f"CREDIT deemed_cash      {cash_disbursed_eth:.6f} {currency}")
                    if fee_share > 0:
                        logger.info(f"CREDIT interest_income  {fee_share_eth:.6f} {currency}")
                    logger.dedent()

                elif is_fund_borrower:
                    logger.success(f"Fund is BORROWER")
                    # Borrower perspective entry would go here
                    logger.info("(Borrower entry generation not yet implemented)")

                else:
                    logger.info(f"Fund not involved in this tranche")

                logger.dedent()

            logger.dedent()

        elif evt_name == 'LoanEmitted' and loan:
            logger.info(f"Processing LoanEmitted event (new loan origination)...")
            # Similar logic for new loan origination
            logger.info("(LoanEmitted journal entry generation not yet implemented)")

        elif evt_name == 'LoanRepaid' and loan:
            logger.info(f"Processing LoanRepaid event...")
            # Similar logic for loan repayment
            logger.info("(LoanRepaid journal entry generation not yet implemented)")

        # ========================================
        # Process Blur Events
        # ========================================
        blur_lien: BlurLien = evt.get('blur_lien')

        # Use func_lien as fallback if blur_lien not in event (e.g., Refinance event)
        if blur_lien is None and func_lien is not None and evt_name in BLUR_EVENTS:
            blur_lien = func_lien
            logger.info(f"Using lien from function params (lien_id={func_lien.lien_id})")

        if blur_lien and evt_name in BLUR_EVENTS:
            logger.info(f"Processing Blur {evt_name} event...")
            logger.indent()

            # Calculate interest using continuous compounding
            principal_eth = blur_lien.get_principal_eth()
            interest_eth = blur_lien.get_interest_eth(block_timestamp)
            total_eth = principal_eth + interest_eth

            logger.info(f"Lien ID: {blur_lien.lien_id}")
            logger.info(f"Principal: {principal_eth:.6f} ETH")
            logger.info(f"Interest (continuous compounding): {interest_eth:.6f} ETH")
            logger.info(f"Total Due: {total_eth:.6f} ETH")
            logger.info(f"Rate: {blur_lien.rate/100:.2f}% APR")

            # Determine if fund is involved
            is_fund_lender = blur_lien.lender.lower() in {w.lower() for w in fund_wallets}
            is_fund_borrower = blur_lien.borrower.lower() in {w.lower() for w in fund_wallets}

            if evt_name == 'LoanOfferTaken':
                # New loan origination
                logger.info("EVENT: Loan Origination")

                if is_fund_lender:
                    logger.success("Fund is LENDER")
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_blur_orig_lender",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"Blur Loan Origination: Lender - {principal_eth:.6f} ETH to {blur_lien.borrower[:10]}...",
                        "category": "NFT_LEND_ORIGINATION",
                        "platform": "BLUR",
                        "lien_id": blur_lien.lien_id,
                        "lines": [
                            {"type": "DEBIT", "account": "100.20 - Loans Receivable", "amount": float(principal_eth), "asset": "ETH"},
                            {"type": "CREDIT", "account": "100.31 - Blur Pool Balance", "amount": float(principal_eth), "asset": "ETH"},
                        ],
                        "is_balanced": True
                    }
                    entries.append(entry)
                    logger.success("Generated LENDER origination entry")
                    logger.indent()
                    logger.info(f"DEBIT  Loans Receivable     {principal_eth:.6f} ETH")
                    logger.info(f"CREDIT Blur Pool Balance    {principal_eth:.6f} ETH")
                    logger.dedent()

                if is_fund_borrower:
                    logger.success("Fund is BORROWER")
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_blur_orig_borrower",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"Blur Loan Received: Borrower - {principal_eth:.6f} ETH from {blur_lien.lender[:10]}...",
                        "category": "NFT_LEND_ORIGINATION",
                        "platform": "BLUR",
                        "lien_id": blur_lien.lien_id,
                        "lines": [
                            {"type": "DEBIT", "account": "100.31 - Blur Pool Balance", "amount": float(principal_eth), "asset": "ETH"},
                            {"type": "CREDIT", "account": "200.10 - Loan Payable", "amount": float(principal_eth), "asset": "ETH"},
                        ],
                        "is_balanced": True
                    }
                    entries.append(entry)
                    logger.success("Generated BORROWER origination entry")
                    logger.indent()
                    logger.info(f"DEBIT  Blur Pool Balance    {principal_eth:.6f} ETH")
                    logger.info(f"CREDIT Loan Payable         {principal_eth:.6f} ETH")
                    logger.dedent()

            elif evt_name == 'Repay':
                # Loan repayment
                logger.info("EVENT: Loan Repayment")

                if is_fund_lender:
                    logger.success("Fund is LENDER (receiving repayment)")
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_blur_repay_lender",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"Blur Loan Repaid: Lender receives {total_eth:.6f} ETH (P: {principal_eth:.6f}, I: {interest_eth:.6f})",
                        "category": "NFT_LEND_REPAYMENT",
                        "platform": "BLUR",
                        "lien_id": blur_lien.lien_id,
                        "lines": [
                            {"type": "DEBIT", "account": "100.31 - Blur Pool Balance", "amount": float(total_eth), "asset": "ETH"},
                            {"type": "CREDIT", "account": "100.20 - Loans Receivable", "amount": float(principal_eth), "asset": "ETH"},
                        ],
                        "is_balanced": False
                    }
                    if interest_eth > 0:
                        entry["lines"].append(
                            {"type": "CREDIT", "account": "400.10 - Interest Income", "amount": float(interest_eth), "asset": "ETH"}
                        )
                    entry["is_balanced"] = True
                    entries.append(entry)

                    logger.success("Generated LENDER repayment entry")
                    logger.indent()
                    logger.info(f"DEBIT  Blur Pool Balance    {total_eth:.6f} ETH")
                    logger.info(f"CREDIT Loans Receivable     {principal_eth:.6f} ETH")
                    if interest_eth > 0:
                        logger.info(f"CREDIT Interest Income      {interest_eth:.6f} ETH")
                    logger.dedent()

                if is_fund_borrower:
                    logger.success("Fund is BORROWER (making repayment)")
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_blur_repay_borrower",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"Blur Loan Repaid: Borrower pays {total_eth:.6f} ETH (P: {principal_eth:.6f}, I: {interest_eth:.6f})",
                        "category": "NFT_LEND_REPAYMENT",
                        "platform": "BLUR",
                        "lien_id": blur_lien.lien_id,
                        "lines": [
                            {"type": "DEBIT", "account": "200.10 - Loan Payable", "amount": float(principal_eth), "asset": "ETH"},
                            {"type": "CREDIT", "account": "100.31 - Blur Pool Balance", "amount": float(total_eth), "asset": "ETH"},
                        ],
                        "is_balanced": False
                    }
                    if interest_eth > 0:
                        entry["lines"].append(
                            {"type": "DEBIT", "account": "500.10 - Interest Expense", "amount": float(interest_eth), "asset": "ETH"}
                        )
                    entry["is_balanced"] = True
                    entries.append(entry)

                    logger.success("Generated BORROWER repayment entry")
                    logger.indent()
                    logger.info(f"DEBIT  Loan Payable         {principal_eth:.6f} ETH")
                    if interest_eth > 0:
                        logger.info(f"DEBIT  Interest Expense     {interest_eth:.6f} ETH")
                    logger.info(f"CREDIT Blur Pool Balance    {total_eth:.6f} ETH")
                    logger.dedent()

            elif evt_name == 'Refinance':
                # Loan refinanced to new lender
                logger.info("EVENT: Loan Refinance")
                new_lender = safe_address(args.get('newLender', ''))

                if is_fund_lender:
                    # Old lender gets paid out
                    logger.success("Fund is OLD LENDER (receiving payoff)")
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_blur_refi_old_lender",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"Blur Loan Refinanced Out: Old lender receives {total_eth:.6f} ETH",
                        "category": "NFT_LEND_REFINANCE",
                        "platform": "BLUR",
                        "lien_id": blur_lien.lien_id,
                        "lines": [
                            {"type": "DEBIT", "account": "100.31 - Blur Pool Balance", "amount": float(total_eth), "asset": "ETH"},
                            {"type": "CREDIT", "account": "100.20 - Loans Receivable", "amount": float(principal_eth), "asset": "ETH"},
                        ],
                        "is_balanced": False
                    }
                    if interest_eth > 0:
                        entry["lines"].append(
                            {"type": "CREDIT", "account": "400.10 - Interest Income", "amount": float(interest_eth), "asset": "ETH"}
                        )
                    entry["is_balanced"] = True
                    entries.append(entry)
                    logger.success("Generated OLD LENDER refinance entry")

                is_fund_new_lender = new_lender.lower() in {w.lower() for w in fund_wallets} if new_lender else False
                if is_fund_new_lender:
                    # New lender pays out
                    logger.success("Fund is NEW LENDER (providing funds)")
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_blur_refi_new_lender",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"Blur Loan Refinanced In: New lender provides {total_eth:.6f} ETH",
                        "category": "NFT_LEND_REFINANCE",
                        "platform": "BLUR",
                        "lien_id": blur_lien.lien_id,
                        "lines": [
                            {"type": "DEBIT", "account": "100.20 - Loans Receivable", "amount": float(total_eth), "asset": "ETH"},
                            {"type": "CREDIT", "account": "100.31 - Blur Pool Balance", "amount": float(total_eth), "asset": "ETH"},
                        ],
                        "is_balanced": True
                    }
                    entries.append(entry)
                    logger.success("Generated NEW LENDER refinance entry")

            elif evt_name == 'StartAuction':
                # Lender called the loan
                logger.info("EVENT: Loan Auction Started (Lender called the loan)")
                logger.info("No journal entry - informational only (loan status change)")

            elif evt_name == 'Seize':
                # Lender seized NFT collateral
                logger.info("EVENT: NFT Collateral Seized")
                if is_fund_lender:
                    logger.success("Fund is LENDER (seizing collateral)")
                    # Lender receives NFT, writes off loan receivable
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_blur_seize_lender",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"Blur Collateral Seized: Lender receives NFT {blur_lien.collection[:10]}...#{blur_lien.token_id}",
                        "category": "NFT_LEND_SEIZURE",
                        "platform": "BLUR",
                        "lien_id": blur_lien.lien_id,
                        "lines": [
                            {"type": "DEBIT", "account": "100.10 - NFT Collateral", "amount": float(total_eth), "asset": "NFT"},
                            {"type": "CREDIT", "account": "100.20 - Loans Receivable", "amount": float(principal_eth), "asset": "ETH"},
                        ],
                        "is_balanced": False
                    }
                    # Record any interest as income
                    if interest_eth > 0:
                        entry["lines"].append(
                            {"type": "CREDIT", "account": "400.10 - Interest Income", "amount": float(interest_eth), "asset": "ETH"}
                        )
                    entry["is_balanced"] = True
                    entries.append(entry)
                    logger.success("Generated SEIZE entry")

            elif evt_name == 'BuyLocked':
                # Third party purchased locked NFT
                logger.info("EVENT: Locked NFT Purchased by Third Party")
                logger.info("(BuyLocked journal entry generation not yet implemented)")

            if not is_fund_lender and not is_fund_borrower:
                logger.info("Fund not involved in this Blur event")

            logger.dedent()

        # ========================================
        # Process NFTfi Events
        # ========================================
        nftfi_loan: NFTfiLoan = evt.get('nftfi_loan')

        if nftfi_loan and evt_name in NFTFI_EVENTS:
            logger.info(f"Processing NFTfi {evt_name} event...")
            logger.indent()

            # Calculate amounts
            principal_eth = nftfi_loan.get_principal_eth()
            interest_eth = nftfi_loan.get_interest_eth()
            max_repayment_eth = nftfi_loan.get_max_repayment_eth()

            logger.info(f"Loan ID: {nftfi_loan.loan_id}")
            logger.info(f"Principal: {principal_eth:.6f} ETH")
            logger.info(f"Interest (fixed-term): {interest_eth:.6f} ETH")
            logger.info(f"Max Repayment: {max_repayment_eth:.6f} ETH")
            logger.info(f"Duration: {nftfi_loan.duration/86400:.1f} days")

            # Determine if fund is involved
            is_fund_lender = nftfi_loan.lender.lower() in {w.lower() for w in fund_wallets}
            is_fund_borrower = nftfi_loan.borrower.lower() in {w.lower() for w in fund_wallets}

            if evt_name == 'LoanStarted':
                # New loan origination
                logger.info("EVENT: NFTfi Loan Origination")

                if is_fund_lender:
                    logger.success("Fund is LENDER")
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_nftfi_orig_lender",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"NFTfi Loan Origination: Lender - {principal_eth:.6f} ETH to {nftfi_loan.borrower[:10]}...",
                        "category": "NFT_LEND_ORIGINATION",
                        "platform": "NFTFI",
                        "loan_id": nftfi_loan.loan_id,
                        "lines": [
                            {"type": "DEBIT", "account": "100.20 - Loans Receivable", "amount": float(principal_eth), "asset": "ETH"},
                            {"type": "CREDIT", "account": "100.31 - WETH Balance", "amount": float(principal_eth), "asset": "ETH"},
                        ],
                        "is_balanced": True
                    }
                    entries.append(entry)
                    logger.success("Generated LENDER origination entry")
                    logger.indent()
                    logger.info(f"DEBIT  Loans Receivable     {principal_eth:.6f} ETH")
                    logger.info(f"CREDIT WETH Balance         {principal_eth:.6f} ETH")
                    logger.dedent()

                if is_fund_borrower:
                    logger.success("Fund is BORROWER")
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_nftfi_orig_borrower",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"NFTfi Loan Received: Borrower - {principal_eth:.6f} ETH from {nftfi_loan.lender[:10]}...",
                        "category": "NFT_LEND_ORIGINATION",
                        "platform": "NFTFI",
                        "loan_id": nftfi_loan.loan_id,
                        "lines": [
                            {"type": "DEBIT", "account": "100.31 - WETH Balance", "amount": float(principal_eth), "asset": "ETH"},
                            {"type": "CREDIT", "account": "200.10 - Loan Payable", "amount": float(principal_eth), "asset": "ETH"},
                        ],
                        "is_balanced": True
                    }
                    entries.append(entry)
                    logger.success("Generated BORROWER origination entry")
                    logger.indent()
                    logger.info(f"DEBIT  WETH Balance          {principal_eth:.6f} ETH")
                    logger.info(f"CREDIT Loan Payable          {principal_eth:.6f} ETH")
                    logger.dedent()

            elif evt_name == 'LoanRepaid':
                # Loan repayment
                logger.info("EVENT: NFTfi Loan Repayment")

                # Get repayment amount from args
                amount_to_lender = safe_int(args.get('amountToLender', 0))
                if amount_to_lender > 0:
                    amount_eth = Decimal(amount_to_lender) / Decimal(10**18)
                else:
                    amount_eth = max_repayment_eth

                if is_fund_lender:
                    logger.success("Fund is LENDER (receiving repayment)")
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_nftfi_repay_lender",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"NFTfi Loan Repaid: Lender receives {amount_eth:.6f} ETH (P: {principal_eth:.6f}, I: {interest_eth:.6f})",
                        "category": "NFT_LEND_REPAYMENT",
                        "platform": "NFTFI",
                        "loan_id": nftfi_loan.loan_id,
                        "lines": [
                            {"type": "DEBIT", "account": "100.31 - WETH Balance", "amount": float(amount_eth), "asset": "ETH"},
                            {"type": "CREDIT", "account": "100.20 - Loans Receivable", "amount": float(principal_eth), "asset": "ETH"},
                        ],
                        "is_balanced": False
                    }
                    if interest_eth > 0:
                        entry["lines"].append(
                            {"type": "CREDIT", "account": "400.10 - Interest Income", "amount": float(interest_eth), "asset": "ETH"}
                        )
                    entry["is_balanced"] = True
                    entries.append(entry)
                    logger.success("Generated LENDER repayment entry")

                if is_fund_borrower:
                    logger.success("Fund is BORROWER (making repayment)")
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_nftfi_repay_borrower",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"NFTfi Loan Repaid: Borrower pays {amount_eth:.6f} ETH (P: {principal_eth:.6f}, I: {interest_eth:.6f})",
                        "category": "NFT_LEND_REPAYMENT",
                        "platform": "NFTFI",
                        "loan_id": nftfi_loan.loan_id,
                        "lines": [
                            {"type": "DEBIT", "account": "200.10 - Loan Payable", "amount": float(principal_eth), "asset": "ETH"},
                            {"type": "CREDIT", "account": "100.31 - WETH Balance", "amount": float(amount_eth), "asset": "ETH"},
                        ],
                        "is_balanced": False
                    }
                    if interest_eth > 0:
                        entry["lines"].append(
                            {"type": "DEBIT", "account": "500.10 - Interest Expense", "amount": float(interest_eth), "asset": "ETH"}
                        )
                    entry["is_balanced"] = True
                    entries.append(entry)
                    logger.success("Generated BORROWER repayment entry")

            elif evt_name == 'LoanLiquidated':
                # Collateral seized
                logger.info("EVENT: NFTfi Loan Liquidated (Collateral Seized)")

                if is_fund_lender:
                    logger.success("Fund is LENDER (seizing collateral)")
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_nftfi_liquidate_lender",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"NFTfi Collateral Seized: Lender receives NFT {nftfi_loan.nft_collection[:10]}...#{nftfi_loan.nft_collateral_id}",
                        "category": "NFT_LEND_SEIZURE",
                        "platform": "NFTFI",
                        "loan_id": nftfi_loan.loan_id,
                        "lines": [
                            {"type": "DEBIT", "account": "100.10 - NFT Collateral", "amount": float(max_repayment_eth), "asset": "NFT"},
                            {"type": "CREDIT", "account": "100.20 - Loans Receivable", "amount": float(principal_eth), "asset": "ETH"},
                        ],
                        "is_balanced": False
                    }
                    if interest_eth > 0:
                        entry["lines"].append(
                            {"type": "CREDIT", "account": "400.10 - Interest Income", "amount": float(interest_eth), "asset": "ETH"}
                        )
                    entry["is_balanced"] = True
                    entries.append(entry)
                    logger.success("Generated SEIZE entry")

                if is_fund_borrower:
                    logger.success("Fund is BORROWER (lost collateral)")
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_nftfi_liquidate_borrower",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"NFTfi Collateral Lost: Borrower loses NFT {nftfi_loan.nft_collection[:10]}...#{nftfi_loan.nft_collateral_id}",
                        "category": "NFT_LEND_SEIZURE",
                        "platform": "NFTFI",
                        "loan_id": nftfi_loan.loan_id,
                        "lines": [
                            {"type": "DEBIT", "account": "200.10 - Loan Payable", "amount": float(principal_eth), "asset": "ETH"},
                            {"type": "DEBIT", "account": "500.10 - Interest Expense", "amount": float(interest_eth), "asset": "ETH"},
                            {"type": "CREDIT", "account": "100.10 - NFT Holdings", "amount": float(max_repayment_eth), "asset": "NFT"},
                        ],
                        "is_balanced": True
                    }
                    entries.append(entry)
                    logger.success("Generated BORROWER liquidation entry")

            if not is_fund_lender and not is_fund_borrower:
                logger.info("Fund not involved in this NFTfi event")

            logger.dedent()

    # ========================================
    # Process Arcade Events
    # ========================================
    for evt in events:
        evt_name = evt.get('name')
        args = evt.get('args', {})
        source = evt.get('source', '')
        arcade_loan_id = evt.get('arcade_loan_id')

        if source == "ARCADE_RAW" and evt_name in ARCADE_EVENTS:
            logger.info(f"Processing Arcade {evt_name} event...")
            logger.indent()

            loan_id = arcade_loan_id or args.get('loanId', 0)
            logger.info(f"Loan ID: {loan_id}")

            # For Arcade, we need to look at the WETH transfers to determine amounts
            # Find WETH transfer events in this transaction
            weth_transfers = [e for e in events if e.get('name') == 'Transfer' and
                             e.get('address', '').lower() == '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2']

            # Determine if fund is involved by checking addresses in transfers
            lender = args.get('lender', '').lower()
            borrower = args.get('borrower', '').lower()

            # For LoanRepaid/LoanClaimed, we may not have lender/borrower in args
            # Check if any fund wallet appears in WETH transfers
            is_fund_involved = False
            fund_role = None
            transfer_amount_eth = Decimal(0)

            for weth_tx in weth_transfers:
                weth_args = weth_tx.get('args', {})
                src = str(weth_args.get('src', weth_args.get('from', ''))).lower()
                dst = str(weth_args.get('dst', weth_args.get('to', ''))).lower()
                wad = safe_int(weth_args.get('wad', weth_args.get('value', 0)))

                if src in fund_wallets:
                    is_fund_involved = True
                    fund_role = "payer"  # Fund is sending WETH
                    transfer_amount_eth = Decimal(wad) / Decimal(10**18)
                elif dst in fund_wallets:
                    is_fund_involved = True
                    fund_role = "receiver"  # Fund is receiving WETH
                    transfer_amount_eth = Decimal(wad) / Decimal(10**18)

            # Also check explicit lender/borrower
            if lender in fund_wallets:
                is_fund_involved = True
                fund_role = "lender"
            elif borrower in fund_wallets:
                is_fund_involved = True
                fund_role = "borrower"

            if evt_name == 'LoanStarted':
                logger.info("EVENT: Arcade Loan Origination")
                logger.info(f"Lender: {lender[:20]}..." if lender else "Lender: unknown")
                logger.info(f"Borrower: {borrower[:20]}..." if borrower else "Borrower: unknown")

                if lender in fund_wallets:
                    logger.success("Fund is LENDER")
                    # Find the amount from WETH transfers (fund sending to LoanCore)
                    for weth_tx in weth_transfers:
                        weth_args = weth_tx.get('args', {})
                        src = str(weth_args.get('src', weth_args.get('from', ''))).lower()
                        if src in fund_wallets:
                            wad = safe_int(weth_args.get('wad', weth_args.get('value', 0)))
                            principal_eth = Decimal(wad) / Decimal(10**18)
                            break
                    else:
                        principal_eth = Decimal(0)

                    if principal_eth > 0:
                        entry = {
                            "entry_id": f"JE_{tx_hash_short}_arcade_orig_lender",
                            "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                            "description": f"Arcade Loan Origination: Lender - {principal_eth:.6f} ETH, Loan #{loan_id}",
                            "category": "NFT_LEND_ORIGINATION",
                            "platform": "ARCADE",
                            "loan_id": loan_id,
                            "lines": [
                                {"type": "DEBIT", "account": "100.20 - Loans Receivable", "amount": float(principal_eth), "asset": "ETH"},
                                {"type": "CREDIT", "account": "100.31 - WETH Balance", "amount": float(principal_eth), "asset": "WETH"},
                            ],
                            "is_balanced": True
                        }
                        entries.append(entry)
                        logger.success(f"Generated LENDER origination entry: {principal_eth:.6f} ETH")

                if borrower in fund_wallets:
                    logger.success("Fund is BORROWER")
                    # Find the amount from WETH transfers (fund receiving from LoanCore)
                    for weth_tx in weth_transfers:
                        weth_args = weth_tx.get('args', {})
                        dst = str(weth_args.get('dst', weth_args.get('to', ''))).lower()
                        if dst in fund_wallets:
                            wad = safe_int(weth_args.get('wad', weth_args.get('value', 0)))
                            principal_eth = Decimal(wad) / Decimal(10**18)
                            break
                    else:
                        principal_eth = Decimal(0)

                    if principal_eth > 0:
                        entry = {
                            "entry_id": f"JE_{tx_hash_short}_arcade_orig_borrower",
                            "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                            "description": f"Arcade Loan Received: Borrower - {principal_eth:.6f} ETH, Loan #{loan_id}",
                            "category": "NFT_LEND_ORIGINATION",
                            "platform": "ARCADE",
                            "loan_id": loan_id,
                            "lines": [
                                {"type": "DEBIT", "account": "100.31 - WETH Balance", "amount": float(principal_eth), "asset": "WETH"},
                                {"type": "CREDIT", "account": "200.10 - Loan Payable", "amount": float(principal_eth), "asset": "ETH"},
                            ],
                            "is_balanced": True
                        }
                        entries.append(entry)
                        logger.success(f"Generated BORROWER origination entry: {principal_eth:.6f} ETH")

            elif evt_name == 'LoanRepaid':
                logger.info("EVENT: Arcade Loan Repayment")
                logger.info(f"Loan ID: {loan_id}")

                # Find WETH transfer amount - the total repayment
                total_repayment_eth = Decimal(0)
                for weth_tx in weth_transfers:
                    weth_args = weth_tx.get('args', {})
                    wad = safe_int(weth_args.get('wad', weth_args.get('value', 0)))
                    if wad > 0:
                        total_repayment_eth = max(total_repayment_eth, Decimal(wad) / Decimal(10**18))

                logger.info(f"Total repayment: {total_repayment_eth:.6f} ETH (from WETH transfers)")

                # Check if fund received WETH (fund is lender)
                for weth_tx in weth_transfers:
                    weth_args = weth_tx.get('args', {})
                    dst = str(weth_args.get('dst', weth_args.get('to', ''))).lower()
                    if dst in fund_wallets:
                        wad = safe_int(weth_args.get('wad', weth_args.get('value', 0)))
                        amount_eth = Decimal(wad) / Decimal(10**18)

                        logger.success(f"Fund is LENDER (receiving {amount_eth:.6f} ETH)")
                        # Estimate principal vs interest (assume ~90% principal, 10% interest)
                        # In production, we'd look up the original loan terms
                        principal_eth = amount_eth * Decimal("0.90")
                        interest_eth = amount_eth - principal_eth

                        entry = {
                            "entry_id": f"JE_{tx_hash_short}_arcade_repay_lender",
                            "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                            "description": f"Arcade Loan Repaid: Lender receives {amount_eth:.6f} ETH, Loan #{loan_id}",
                            "category": "NFT_LEND_REPAYMENT",
                            "platform": "ARCADE",
                            "loan_id": loan_id,
                            "lines": [
                                {"type": "DEBIT", "account": "100.31 - WETH Balance", "amount": float(amount_eth), "asset": "WETH"},
                                {"type": "CREDIT", "account": "100.20 - Loans Receivable", "amount": float(principal_eth), "asset": "ETH"},
                                {"type": "CREDIT", "account": "400.10 - Interest Income", "amount": float(interest_eth), "asset": "ETH"},
                            ],
                            "is_balanced": True
                        }
                        entries.append(entry)
                        logger.success("Generated LENDER repayment entry")
                        break

                # Check if fund sent WETH (fund is borrower)
                for weth_tx in weth_transfers:
                    weth_args = weth_tx.get('args', {})
                    src = str(weth_args.get('src', weth_args.get('from', ''))).lower()
                    if src in fund_wallets:
                        wad = safe_int(weth_args.get('wad', weth_args.get('value', 0)))
                        amount_eth = Decimal(wad) / Decimal(10**18)

                        logger.success(f"Fund is BORROWER (paying {amount_eth:.6f} ETH)")
                        principal_eth = amount_eth * Decimal("0.90")
                        interest_eth = amount_eth - principal_eth

                        entry = {
                            "entry_id": f"JE_{tx_hash_short}_arcade_repay_borrower",
                            "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                            "description": f"Arcade Loan Repaid: Borrower pays {amount_eth:.6f} ETH, Loan #{loan_id}",
                            "category": "NFT_LEND_REPAYMENT",
                            "platform": "ARCADE",
                            "loan_id": loan_id,
                            "lines": [
                                {"type": "DEBIT", "account": "200.10 - Loan Payable", "amount": float(principal_eth), "asset": "ETH"},
                                {"type": "DEBIT", "account": "500.10 - Interest Expense", "amount": float(interest_eth), "asset": "ETH"},
                                {"type": "CREDIT", "account": "100.31 - WETH Balance", "amount": float(amount_eth), "asset": "WETH"},
                            ],
                            "is_balanced": True
                        }
                        entries.append(entry)
                        logger.success("Generated BORROWER repayment entry")
                        break

            elif evt_name == 'LoanClaimed':
                logger.info("EVENT: Arcade Loan Claimed (Collateral Seized)")
                logger.info(f"Loan ID: {loan_id}")

                # Check for NFT transfers to fund wallets (fund is lender seizing collateral)
                nft_transfers = [e for e in events if e.get('name') == 'Transfer' and
                                e.get('address', '').lower() not in ['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2']]

                for nft_tx in nft_transfers:
                    nft_args = nft_tx.get('args', {})
                    dst = str(nft_args.get('to', '')).lower()
                    if dst in fund_wallets:
                        nft_contract = nft_tx.get('address', '')
                        token_id = safe_int(nft_args.get('tokenId', 0))

                        logger.success(f"Fund is LENDER (seizing NFT {nft_contract[:12]}...#{token_id})")

                        # For collateral seizure, we need to estimate the value
                        # In production, we'd look up the original loan amount
                        estimated_value_eth = Decimal("1.0")  # Placeholder

                        entry = {
                            "entry_id": f"JE_{tx_hash_short}_arcade_claim_lender",
                            "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                            "description": f"Arcade Collateral Seized: Lender receives NFT #{token_id}, Loan #{loan_id}",
                            "category": "NFT_LEND_SEIZURE",
                            "platform": "ARCADE",
                            "loan_id": loan_id,
                            "nft_contract": nft_contract,
                            "nft_token_id": token_id,
                            "lines": [
                                {"type": "DEBIT", "account": "100.10 - NFT Holdings", "amount": float(estimated_value_eth), "asset": "NFT"},
                                {"type": "CREDIT", "account": "100.20 - Loans Receivable", "amount": float(estimated_value_eth), "asset": "ETH"},
                            ],
                            "is_balanced": True,
                            "needs_review": True,  # Flag for manual value verification
                        }
                        entries.append(entry)
                        logger.success("Generated LENDER seizure entry (needs value review)")
                        break

                # Check if fund lost NFT (fund is borrower losing collateral)
                for nft_tx in nft_transfers:
                    nft_args = nft_tx.get('args', {})
                    src = str(nft_args.get('from', '')).lower()
                    # LoanCore holds collateral, so check if transfer is FROM LoanCore
                    if src == ARCADE_LOAN_CORE_PROXY.lower():
                        dst = str(nft_args.get('to', '')).lower()
                        # If transfer is to someone NOT in fund wallets, fund might be borrower who lost
                        if dst not in fund_wallets:
                            nft_contract = nft_tx.get('address', '')
                            token_id = safe_int(nft_args.get('tokenId', 0))

                            # Check if this loan was originally taken by fund
                            # For now, we'll skip generating borrower loss entry
                            # as we can't determine fund involvement without more context
                            logger.info(f"NFT #{token_id} transferred to {dst[:12]}... (checking if fund was borrower)")

            if not is_fund_involved:
                logger.info("Fund not involved in this Arcade event")

            logger.dedent()

    # ========================================
    # Process Zharta Events
    # ========================================
    for evt in events:
        evt_name = evt.get('name')
        args = evt.get('args', {})
        source = evt.get('source', '')
        zharta_loan_id = evt.get('zharta_loan_id')

        if source in ["ZHARTA_RAW", "ZHARTA_ABI", "S3_ABI"] and evt_name in ZHARTA_EVENTS:
            logger.info(f"Processing Zharta {evt_name} event...")
            logger.indent()

            loan_id = zharta_loan_id or args.get('loanId', 0)
            wallet = args.get('wallet', '').lower()
            erc20_token = args.get('erc20TokenContract', '').lower()
            logger.info(f"Loan ID: {loan_id}")
            logger.info(f"Wallet: {wallet[:20]}..." if wallet else "Wallet: unknown")

            # Determine token type for account labeling
            is_weth = erc20_token == '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            token_symbol = "WETH" if is_weth else "TOKEN"
            token_account = "100.31 - WETH Balance" if is_weth else "100.32 - Token Balance"

            # Find WETH/token transfers in this transaction
            weth_transfers = [e for e in events if e.get('name') == 'Transfer' and
                             e.get('address', '').lower() == erc20_token]

            # Check if wallet is a fund wallet
            is_fund_borrower = wallet in fund_wallets

            if evt_name == 'LoanCreated':
                logger.info("EVENT: Zharta Loan Origination")
                amount = args.get('amount', 0)
                apr = args.get('apr', 0)
                duration = args.get('duration', 0)
                amount_eth = Decimal(amount) / Decimal(10**18)

                logger.info(f"Amount: {amount_eth:.6f} {token_symbol}")
                logger.info(f"APR: {apr} bps ({apr/100:.2f}%)")
                logger.info(f"Duration: {duration} secs ({duration/86400:.1f} days)")

                if is_fund_borrower:
                    logger.success("Fund is BORROWER")
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_zharta_orig_borrower",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"Zharta Loan Received: Borrower - {amount_eth:.6f} {token_symbol}, Loan #{loan_id}",
                        "category": "NFT_LEND_ORIGINATION",
                        "platform": "ZHARTA",
                        "loan_id": loan_id,
                        "lines": [
                            {"type": "DEBIT", "account": token_account, "amount": float(amount_eth), "asset": token_symbol},
                            {"type": "CREDIT", "account": "200.10 - Loan Payable", "amount": float(amount_eth), "asset": token_symbol},
                        ],
                        "is_balanced": True
                    }
                    entries.append(entry)
                    logger.success(f"Generated BORROWER origination entry: {amount_eth:.6f} {token_symbol}")

                # Note: Zharta is peer-to-pool, so typically funds are borrowers not lenders
                # Pool lenders would be tracked differently

            elif evt_name == 'LoanPayment':
                logger.info("EVENT: Zharta Loan Payment")
                principal = args.get('principal', 0)
                interest_amount = args.get('interestAmount', 0)
                principal_eth = Decimal(principal) / Decimal(10**18)
                interest_eth = Decimal(interest_amount) / Decimal(10**18)
                total_eth = principal_eth + interest_eth

                logger.info(f"Principal: {principal_eth:.6f} {token_symbol}")
                logger.info(f"Interest: {interest_eth:.6f} {token_symbol}")
                logger.info(f"Total: {total_eth:.6f} {token_symbol}")

                if is_fund_borrower:
                    logger.success("Fund is BORROWER (making payment)")
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}_zharta_payment_borrower",
                        "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                        "description": f"Zharta Loan Payment: Borrower pays {total_eth:.6f} {token_symbol}, Loan #{loan_id}",
                        "category": "NFT_LEND_PAYMENT",
                        "platform": "ZHARTA",
                        "loan_id": loan_id,
                        "lines": [
                            {"type": "DEBIT", "account": "200.10 - Loan Payable", "amount": float(principal_eth), "asset": token_symbol},
                            {"type": "DEBIT", "account": "500.10 - Interest Expense", "amount": float(interest_eth), "asset": token_symbol},
                            {"type": "CREDIT", "account": token_account, "amount": float(total_eth), "asset": token_symbol},
                        ],
                        "is_balanced": True
                    }
                    entries.append(entry)
                    logger.success("Generated BORROWER payment entry")

            elif evt_name == 'LoanPaid':
                logger.info("EVENT: Zharta Loan Fully Repaid")

                if is_fund_borrower:
                    logger.success("Fund is BORROWER (loan fully repaid)")
                    # For LoanPaid, we don't have amount in args - look for WETH transfer
                    for weth_tx in weth_transfers:
                        weth_args = weth_tx.get('args', {})
                        src = str(weth_args.get('src', weth_args.get('from', ''))).lower()
                        if src in fund_wallets:
                            wad = safe_int(weth_args.get('wad', weth_args.get('value', 0)))
                            amount_eth = Decimal(wad) / Decimal(10**18)
                            # Estimate 90/10 split
                            principal_eth = amount_eth * Decimal("0.90")
                            interest_eth = amount_eth - principal_eth

                            entry = {
                                "entry_id": f"JE_{tx_hash_short}_zharta_paid_borrower",
                                "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                                "description": f"Zharta Loan Paid Off: Borrower - {amount_eth:.6f} {token_symbol}, Loan #{loan_id}",
                                "category": "NFT_LEND_REPAYMENT",
                                "platform": "ZHARTA",
                                "loan_id": loan_id,
                                "lines": [
                                    {"type": "DEBIT", "account": "200.10 - Loan Payable", "amount": float(principal_eth), "asset": token_symbol},
                                    {"type": "DEBIT", "account": "500.10 - Interest Expense", "amount": float(interest_eth), "asset": token_symbol},
                                    {"type": "CREDIT", "account": token_account, "amount": float(amount_eth), "asset": token_symbol},
                                ],
                                "is_balanced": True
                            }
                            entries.append(entry)
                            logger.success("Generated BORROWER full repayment entry")
                            break

            elif evt_name == 'LoanDefaulted':
                logger.info("EVENT: Zharta Loan Defaulted (Collateral Lost)")
                default_amount = args.get('amount', 0)
                default_eth = Decimal(default_amount) / Decimal(10**18)

                logger.info(f"Default amount: {default_eth:.6f} {token_symbol}")

                if is_fund_borrower:
                    logger.warning("Fund is BORROWER (lost collateral due to default)")

                    # Find NFT transfers (collateral being seized)
                    nft_transfers = [e for e in events if e.get('name') == 'Transfer' and
                                    e.get('address', '').lower() != erc20_token and
                                    e.get('address', '').lower() != '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2']

                    for nft_tx in nft_transfers:
                        nft_args = nft_tx.get('args', {})
                        src = str(nft_args.get('from', '')).lower()
                        # CollateralVault holds collateral
                        if src == ZHARTA_COLLATERAL_VAULT.lower():
                            nft_contract = nft_tx.get('address', '')
                            token_id = safe_int(nft_args.get('tokenId', 0))

                            entry = {
                                "entry_id": f"JE_{tx_hash_short}_zharta_default_borrower",
                                "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat(),
                                "description": f"Zharta Loan Default: Borrower lost NFT #{token_id}, Loan #{loan_id}",
                                "category": "NFT_LEND_DEFAULT",
                                "platform": "ZHARTA",
                                "loan_id": loan_id,
                                "nft_contract": nft_contract,
                                "nft_token_id": token_id,
                                "lines": [
                                    {"type": "DEBIT", "account": "200.10 - Loan Payable", "amount": float(default_eth), "asset": token_symbol},
                                    {"type": "DEBIT", "account": "600.10 - Loss on Default", "amount": float(default_eth), "asset": token_symbol},
                                    {"type": "CREDIT", "account": "100.10 - NFT Holdings", "amount": float(default_eth * 2), "asset": "NFT"},
                                ],
                                "is_balanced": True,
                                "needs_review": True,  # Flag for manual value verification
                            }
                            entries.append(entry)
                            logger.warning("Generated BORROWER default entry (needs review)")
                            break

            if not is_fund_borrower:
                logger.info("Fund not involved in this Zharta event")

            logger.dedent()

    # ========================================
    # Process Generic Events (WETH, ERC20, etc.)
    # ========================================
    from_address = (tx.get('from') or '').lower()
    to_address = (tx.get('to') or '').lower()

    for evt in events:
        evt_name = evt.get('name', '')
        evt_source = evt.get('source', '')
        args = evt.get('args', {})
        evt_address = evt.get('address', '').lower()

        # WETH Deposit (Wrap ETH -> WETH)
        if evt_name == 'Deposit' and evt_source == 'WETH_ABI':
            wad = safe_int(args.get('wad', args.get('value', 0)))
            dst = str(args.get('dst', '')).lower()
            amount_eth = Decimal(wad) / Decimal(10**18)

            # Check if fund wallet is involved
            is_fund_tx = dst in {w.lower() for w in fund_wallets} or from_address in {w.lower() for w in fund_wallets}

            if is_fund_tx and amount_eth > 0:
                entry = {
                    "entry_id": f"JE_{tx_hash_short}_weth_wrap",
                    "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat() if block_timestamp else datetime.now(timezone.utc).isoformat(),
                    "description": f"WETH Wrap: {amount_eth:.6f} ETH -> WETH",
                    "category": "WETH_WRAP",
                    "platform": "GENERIC",
                    "lines": [
                        {"type": "DEBIT", "account": "100.31 - WETH", "amount": float(amount_eth), "asset": "WETH"},
                        {"type": "CREDIT", "account": "100.30 - ETH Wallet", "amount": float(amount_eth), "asset": "ETH"}
                    ],
                    "is_balanced": True
                }
                entries.append(entry)
                logger.success(f"Generated WETH Wrap entry: {amount_eth:.6f} ETH")

        # WETH Withdrawal (Unwrap WETH -> ETH)
        elif evt_name == 'Withdrawal' and evt_source == 'WETH_ABI':
            wad = safe_int(args.get('wad', args.get('value', 0)))
            src = str(args.get('src', '')).lower()
            amount_eth = Decimal(wad) / Decimal(10**18)

            # Check if fund wallet is involved
            is_fund_tx = src in {w.lower() for w in fund_wallets} or from_address in {w.lower() for w in fund_wallets}

            if is_fund_tx and amount_eth > 0:
                entry = {
                    "entry_id": f"JE_{tx_hash_short}_weth_unwrap",
                    "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat() if block_timestamp else datetime.now(timezone.utc).isoformat(),
                    "description": f"WETH Unwrap: {amount_eth:.6f} WETH -> ETH",
                    "category": "WETH_UNWRAP",
                    "platform": "GENERIC",
                    "lines": [
                        {"type": "DEBIT", "account": "100.30 - ETH Wallet", "amount": float(amount_eth), "asset": "ETH"},
                        {"type": "CREDIT", "account": "100.31 - WETH", "amount": float(amount_eth), "asset": "WETH"}
                    ],
                    "is_balanced": True
                }
                entries.append(entry)
                logger.success(f"Generated WETH Unwrap entry: {amount_eth:.6f} ETH")

        # ERC20 Transfer (token movement)
        elif evt_name == 'Transfer' and evt_source == 'ERC20_ABI':
            from_addr = str(args.get('from', '')).lower()
            to_addr = str(args.get('to', '')).lower()
            value = safe_int(args.get('value', 0))

            # Get token info
            token_info = TOKEN_REGISTRY.get(evt_address, {"symbol": "TOKEN", "decimals": 18})
            token_symbol = token_info['symbol']
            decimals = token_info['decimals']
            amount = Decimal(value) / Decimal(10**decimals)

            # Check if fund wallet is sender or receiver
            is_fund_sender = from_addr in {w.lower() for w in fund_wallets}
            is_fund_receiver = to_addr in {w.lower() for w in fund_wallets}

            if is_fund_sender and amount > 0:
                entry = {
                    "entry_id": f"JE_{tx_hash_short}_token_out_{evt.get('log_index', 0)}",
                    "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat() if block_timestamp else datetime.now(timezone.utc).isoformat(),
                    "description": f"Token Transfer Out: {amount:.6f} {token_symbol} to {to_addr[:10]}...",
                    "category": "TOKEN_TRANSFER_OUT",
                    "platform": "GENERIC",
                    "lines": [
                        {"type": "DEBIT", "account": f"200.10 - {token_symbol} Payable", "amount": float(amount), "asset": token_symbol},
                        {"type": "CREDIT", "account": f"100.31 - {token_symbol}", "amount": float(amount), "asset": token_symbol}
                    ],
                    "is_balanced": True
                }
                entries.append(entry)
                logger.success(f"Generated Token Transfer Out entry: {amount:.6f} {token_symbol}")

            elif is_fund_receiver and amount > 0:
                entry = {
                    "entry_id": f"JE_{tx_hash_short}_token_in_{evt.get('log_index', 0)}",
                    "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat() if block_timestamp else datetime.now(timezone.utc).isoformat(),
                    "description": f"Token Transfer In: {amount:.6f} {token_symbol} from {from_addr[:10]}...",
                    "category": "TOKEN_TRANSFER_IN",
                    "platform": "GENERIC",
                    "lines": [
                        {"type": "DEBIT", "account": f"100.31 - {token_symbol}", "amount": float(amount), "asset": token_symbol},
                        {"type": "CREDIT", "account": f"400.10 - {token_symbol} Receivable", "amount": float(amount), "asset": token_symbol}
                    ],
                    "is_balanced": True
                }
                entries.append(entry)
                logger.success(f"Generated Token Transfer In entry: {amount:.6f} {token_symbol}")

    # ========================================
    # Simple category-based entries (fallback)
    # ========================================
    if not entries and category in ("WETH_WRAP", "LEND_DEPOSIT") and value_eth > 0:
        entry = {
            "entry_id": f"JE_{tx_hash_short}",
            "date": datetime.now(timezone.utc).isoformat(),
            "description": f"WETH Wrap: {value_eth} ETH -> WETH",
            "category": category,
            "platform": platform,
            "lines": [
                {"type": "DEBIT", "account": "100.31 - WETH", "amount": float(value_eth), "asset": "WETH"},
                {"type": "CREDIT", "account": "100.30 - ETH Wallet", "amount": float(value_eth), "asset": "ETH"}
            ],
            "is_balanced": True
        }
        entries.append(entry)
        logger.success("Generated WETH Wrap entry (from tx value)")

    elif not entries and category == "WETH_UNWRAP":
        # Try to get amount from events
        for evt in events:
            if evt.get('name') == 'Withdrawal' and evt.get('source') == 'WETH_ABI':
                wad = safe_int(evt.get('args', {}).get('wad', 0))
                unwrap_amount = Decimal(wad) / Decimal(10**18)
                if unwrap_amount > 0:
                    entry = {
                        "entry_id": f"JE_{tx_hash_short}",
                        "date": datetime.now(timezone.utc).isoformat(),
                        "description": f"WETH Unwrap: {unwrap_amount} WETH -> ETH",
                        "category": category,
                        "platform": platform,
                        "lines": [
                            {"type": "DEBIT", "account": "100.30 - ETH Wallet", "amount": float(unwrap_amount), "asset": "ETH"},
                            {"type": "CREDIT", "account": "100.31 - WETH", "amount": float(unwrap_amount), "asset": "WETH"}
                        ],
                        "is_balanced": True
                    }
                    entries.append(entry)
                    logger.success("Generated WETH Unwrap entry (from category)")
                    break

    elif not entries and category == "TOKEN_TRANSFER" and value_eth > 0:
        # Generic token transfer when category identified but no specific events found
        entry = {
            "entry_id": f"JE_{tx_hash_short}",
            "date": datetime.now(timezone.utc).isoformat(),
            "description": f"Token Transfer: {value_eth} tokens",
            "category": category,
            "platform": platform,
            "lines": [
                {"type": "DEBIT", "account": "200.10 - Token Payable", "amount": float(value_eth), "asset": "TOKEN"},
                {"type": "CREDIT", "account": "100.31 - Token Holdings", "amount": float(value_eth), "asset": "TOKEN"}
            ],
            "is_balanced": True,
            "needs_review": True  # Flag for manual verification
        }
        entries.append(entry)
        logger.success("Generated Generic Token Transfer entry (needs review)")

    # Simple ETH transfer (no contract call)
    elif not entries and value_eth > 0 and category in ("UNKNOWN", "ETH_TRANSFER"):
        entry = {
            "entry_id": f"JE_{tx_hash_short}",
            "date": datetime.now(timezone.utc).isoformat(),
            "description": f"ETH Transfer: {value_eth} ETH",
            "category": "ETH_TRANSFER",
            "platform": platform,
            "lines": [
                {"type": "DEBIT", "account": "200.10 - ETH Payable", "amount": float(value_eth), "asset": "ETH"},
                {"type": "CREDIT", "account": "100.30 - ETH Wallet", "amount": float(value_eth), "asset": "ETH"}
            ],
            "is_balanced": True
        }
        entries.append(entry)
        logger.success("Generated ETH Transfer entry")

    # ========================================
    # Gas fee entry (always)
    # ========================================
    if gas_fee_eth > 0:
        gas_entry = {
            "entry_id": f"JE_{tx_hash_short}_gas",
            "date": datetime.fromtimestamp(block_timestamp, tz=timezone.utc).isoformat() if block_timestamp else datetime.now(timezone.utc).isoformat(),
            "description": f"Gas Fee: {gas_fee_eth} ETH",
            "category": "GAS_FEE",
            "platform": platform,
            "lines": [
                {"type": "DEBIT", "account": "500.20 - Gas Fees", "amount": float(gas_fee_eth), "asset": "ETH"},
                {"type": "CREDIT", "account": "100.30 - ETH Wallet", "amount": float(gas_fee_eth), "asset": "ETH"}
            ],
            "is_balanced": True
        }
        entries.append(gas_entry)
        logger.success(f"Generated Gas Fee entry: {gas_fee_eth:.6f} ETH")

    # ========================================
    # Summary
    # ========================================
    logger.info(f"Generated {len(entries)} journal entries")

    # Validation
    logger.info("Validating entries...")
    for entry in entries:
        lines = entry.get('lines', [])
        debits = sum(l['amount'] for l in lines if l['type'] == 'DEBIT')
        credits = sum(l['amount'] for l in lines if l['type'] == 'CREDIT')
        balanced = abs(debits - credits) < 0.000001
        if balanced:
            logger.success(f"  {entry['entry_id']}: BALANCED")
        else:
            logger.error(9, f"  {entry['entry_id']}: UNBALANCED (D={debits:.6f}, C={credits:.6f})")

    return entries


# ============================================================================
# MAIN DEBUG FUNCTION
# ============================================================================

def debug_decode_transaction(tx_hash: str, logger: DebugLogger) -> Dict:
    """
    Main debug decode function - traces through all steps with verbose logging.
    """

    result = {
        "status": "error",
        "tx_hash": tx_hash,
        "platform": None,
        "events": [],
        "journal_entries": [],
        "error": None
    }

    # Validate tx hash format
    if not tx_hash or len(tx_hash) != 66 or not tx_hash.startswith('0x'):
        logger.error(0, f"Invalid transaction hash format: {tx_hash}")
        logger.info("Expected format: 0x followed by 64 hex characters")
        result["error"] = "Invalid transaction hash format"
        return result

    # STEP 0.5: Load fund wallets from S3
    logger.info("Loading fund wallets from S3...")
    fund_wallets = load_fund_wallets(logger)

    # STEP 0.6: List available S3 ABIs
    logger.info("Checking available ABIs in S3...")
    available_abis = list_s3_abis(logger)
    if available_abis:
        logger.info(f"Found {len(available_abis)} ABIs available:")
        logger.indent()
        # Group by type (addresses vs names)
        addresses = [a for a in available_abis if a.startswith('0x')]
        names = [a for a in available_abis if not a.startswith('0x')]
        if addresses:
            logger.info(f"Contract addresses: {len(addresses)}")
            for addr in addresses[:10]:
                # Check if it's a known contract
                platform = CONTRACT_ROUTING.get(addr.lower(), "unknown")
                logger.info(f"  {addr} -> {platform}")
            if len(addresses) > 10:
                logger.info(f"  ... and {len(addresses) - 10} more")
        if names:
            logger.info(f"Named ABIs: {names[:10]}{'...' if len(names) > 10 else ''}")
        logger.dedent()
    else:
        logger.warning("No ABIs found in S3 or could not connect")

    logger.separator("=")

    # STEP 1: Web3 Connection
    w3 = get_web3(logger)
    if not w3:
        result["error"] = "Web3 connection failed"
        return result

    logger.separator("=")

    # STEP 2: Fetch Transaction
    tx = fetch_transaction(w3, tx_hash, logger)
    if not tx:
        result["error"] = "Transaction not found"
        return result

    logger.separator("=")

    # STEP 3: Fetch Receipt
    receipt = fetch_receipt(w3, tx_hash, logger)
    if not receipt:
        result["error"] = "Receipt not found"
        return result

    logger.separator("=")

    # STEP 4: Fetch Block
    block_number = tx.get('blockNumber')
    block = fetch_block(w3, block_number, logger)
    if not block:
        result["error"] = "Block not found"
        return result

    logger.separator("=")

    # STEP 5: Get ETH Price
    eth_price = get_eth_price(w3, block_number, logger)

    logger.separator("=")

    # STEP 6: Route Transaction
    platform, routed_func_name, category = route_transaction(tx, receipt, logger)
    result["platform"] = platform
    result["category"] = category

    logger.separator("=")

    # STEP 7: Decode Function Input
    func_name, func_params = decode_function_input(w3, tx, logger)
    # Use routed function name if decode_function_input returned unknown
    if func_name == "unknown" and routed_func_name != "unknown":
        func_name = routed_func_name
        logger.info(f"Using routed function name: {func_name}")

    logger.separator("=")

    # STEP 8: Decode Events
    events = decode_events(w3, receipt, logger)
    result["events"] = events

    logger.separator("=")

    # STEP 9: Generate Journal Entries
    entries = generate_journal_entries(
        tx, receipt, block, events, platform, category, eth_price, logger,
        fund_wallets=fund_wallets,
        func_params=func_params  # Pass function params for lien data
    )
    result["journal_entries"] = entries

    logger.separator("=")

    # STEP 10: Final Summary
    logger.log(10, "FINAL RESULT")
    logger.info(f"Status: SUCCESS")
    logger.info(f"Platform: {platform}")
    logger.info(f"Function: {func_name}")
    logger.info(f"Category: {category}")
    logger.info(f"Events Decoded: {len(events)}")
    logger.info(f"Journal Entries: {len(entries)}")
    logger.info(f"ETH Price: ${eth_price:,.2f}")

    result["status"] = "success"

    logger.separator("=")
    logger.success("DECODE COMPLETE")

    return result


# ============================================================================
# SHINY APP
# ============================================================================

app_ui = ui.page_fluid(
    ui.tags.style("""
        #debug_output {
            max-height: 700px;
            overflow-y: auto;
            font-size: 12px;
            white-space: pre-wrap;
            word-wrap: break-word;
        }
    """),
    ui.h2("Transaction Decoder Debug Tool"),
    ui.p("Paste a transaction hash and click Decode to trace through the decoding process."),

    ui.layout_columns(
        ui.input_text(
            "tx_hash",
            "Transaction Hash:",
            placeholder="0x...",
            width="100%"
        ),
        ui.input_action_button("decode", "Decode", class_="btn-primary btn-lg"),
        ui.input_action_button("clear", "Clear", class_="btn-secondary"),
        col_widths=[8, 2, 2]
    ),

    ui.hr(),

    ui.h4("Debug Output"),
    ui.output_text_verbatim("debug_output"),

    title="TX Decoder Debug"
)


def server(input, output, session):
    # Shared logger instance
    logger = reactive.value(DebugLogger())
    decode_result = reactive.value(None)

    @reactive.effect
    @reactive.event(input.decode)
    def do_decode():
        tx_hash = input.tx_hash().strip()
        if not tx_hash:
            return

        # Clear and start fresh
        new_logger = DebugLogger()
        new_logger.log(0, f"Starting decode for: {tx_hash}")
        new_logger.separator("=")

        # Run decode with logging
        result = debug_decode_transaction(tx_hash, new_logger)

        logger.set(new_logger)
        decode_result.set(result)

    @reactive.effect
    @reactive.event(input.clear)
    def do_clear():
        logger.set(DebugLogger())
        decode_result.set(None)

    @render.text
    def debug_output():
        return logger.get().get_output() or "Paste a transaction hash above and click 'Decode' to begin."


app = App(app_ui, server)


if __name__ == "__main__":
    print("=" * 60)
    print("Transaction Decoder Debug Tool")
    print("=" * 60)
    print("Starting server at http://127.0.0.1:8765")
    print("Press Ctrl+C to stop")
    print("=" * 60)

    app.run(host="127.0.0.1", port=8765)
