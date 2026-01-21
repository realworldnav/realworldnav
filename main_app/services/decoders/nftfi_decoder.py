"""
NFTfi Unified Decoder V2 - Complete Multi-Version Solution with Refinancing Rollover
=====================================================================================

CHANGELOG from V1:
- Added proper wallet metadata loading with category = "fund" filter
- Added RefinancingContext for tracking rollover transactions
- Added same-lender rollover detection (fund is both old and new lender)
- Added rollover journal entry generation with net cash settlement
- Updated process_transactions to handle refinancing specially

Supported Contracts:
- V3 AssetOfferLoan (0x9F10D706...) - 15-field LoanTerms
- V3 CollectionOfferLoan (0xB6adEc2A...) - 15-field LoanTerms
- V2.3 DirectLoanFixedOffer (0xd0a40eB7...) - 11-field LoanTerms + LoanExtras
- V2.3 DirectLoanFixedCollectionOffer (0xD0C6e59B...) - 11-field LoanTerms + LoanExtras
- Refinancing Contract (0x6701B1D2...) - Linkage events + ROLLOVER DETECTION

Events Handled:
- LoanStarted: New loan origination
- LoanRepaid: Loan repayment by borrower
- LoanLiquidated: Foreclosure by lender (with on-chain interest calc)
- LoanRenegotiated: Term modification
- Refinanced: Old loan -> New loan linkage + ROLLOVER DETECTION

Author: Real World NAV
Version: 2.0.0
"""




from __future__ import annotations

import math
import json
import pandas as pd
import numpy as np
from pathlib import Path
from decimal import Decimal, getcontext
from datetime import datetime, timedelta, time as dt_time, timezone
from typing import Dict, List, Tuple, Optional, Any, Union, Callable, Set
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
    'NFTfiEventDecoder',
    'NFTfiJournalGenerator',
    'NFTfiLoanState',
    'DecodedNFTfiEvent',
    'RefinancingContext',
    'NFTfiEventType',
    'ALL_NFTFI_LOAN_CONTRACTS',
    'NFTFI_REFINANCING_CONTRACT',
    'NFTFI_REFINANCING_CONTRACTS',
    'NFTFI_ADDITIONAL_CONTRACTS',
    'NFTFI_V3_CONTRACTS',
    'NFTFI_V23_CONTRACTS',
    'load_wallet_metadata',
    'get_fund_wallet_list',
    'standardize_dataframe',
]

# ============================================================================
# WALLET METADATA LOADING
# ============================================================================

def load_wallet_metadata(filepath: str) -> Dict[str, Dict]:
    """
    Load wallet metadata from Excel file.
    Only includes wallets where category = "fund".

    Expected columns:
    - address (or wallet_address)
    - fund_id
    - wallet_name (or name)
    - category
    - wallet_type (optional)

    Args:
        filepath: Path to Excel file (e.g., drip_capital_wallet_ID_mapping.xlsx)

    Returns:
        Dict mapping lowercase addresses to metadata dicts

    Usage:
        wallet_ID_mapping_file = "path/to/wallet_mapping.xlsx"  # Update to your path
        wallet_metadata = load_wallet_metadata(wallet_ID_mapping_file)
        fund_wallet_list = get_fund_wallet_list(wallet_metadata)
    """
    try:
        df = pd.read_excel(filepath)
    except Exception as e:
        print(f"[\!] Error loading wallet metadata from {filepath}: {e}")
        return {}

    # Normalize column names
    df.columns = df.columns.str.lower().str.strip()

    # Find address column
    addr_col = None
    for col in ['address', 'wallet_address', 'wallet']:
        if col in df.columns:
            addr_col = col
            break

    if addr_col is None:
        print(f"[\!] No address column found in {filepath}")
        return {}

    # Filter for category = "fund" only
    if 'category' in df.columns:
        # Convert to lowercase and strip whitespace for comparison
        df['_category_normalized'] = df['category'].astype(str).str.lower().str.strip()
        df = df[df['_category_normalized'] == 'fund']
        df = df.drop('_category_normalized', axis=1)
    else:
        print(f"[\!] No 'category' column found - loading all wallets")

    if df.empty:
        print(f"[\!] No fund wallets found in {filepath}")
        return {}

    # Build metadata dict
    wallet_metadata = {}

    for _, row in df.iterrows():
        addr = str(row[addr_col]).strip().lower()

        # Skip invalid addresses
        if not addr or addr == 'nan' or len(addr) < 40:
            continue

        # Ensure 0x prefix
        if not addr.startswith('0x'):
            addr = '0x' + addr

        # Normalize to lowercase
        addr = addr.lower()

        metadata = {
            'fund_id': str(row.get('fund_id', '')).strip() if pd.notna(row.get('fund_id')) else '',
            'wallet_name': str(row.get('wallet_name', row.get('name', ''))).strip() if pd.notna(row.get('wallet_name', row.get('name'))) else '',
            'category': str(row.get('category', 'fund')).strip() if pd.notna(row.get('category')) else 'fund',
            'wallet_type': str(row.get('wallet_type', '')).strip() if pd.notna(row.get('wallet_type')) else '',
        }

        wallet_metadata[addr] = metadata

    print(f"[OK] Loaded {len(wallet_metadata)} fund wallets from {filepath}")

    return wallet_metadata


def get_fund_wallet_list(wallet_metadata: Dict[str, Dict]) -> Set[str]:
    """
    Extract set of fund wallet addresses (lowercase) from wallet_metadata.

    Args:
        wallet_metadata: Dict from load_wallet_metadata()

    Returns:
        Set of lowercase addresses
    """
    return set(wallet_metadata.keys())


def get_fund_wallet_list_checksummed(wallet_metadata: Dict[str, Dict]) -> Set[str]:
    """
    Extract set of fund wallet addresses (checksummed) from wallet_metadata.

    Args:
        wallet_metadata: Dict from load_wallet_metadata()

    Returns:
        Set of checksummed addresses
    """
    return {Web3.to_checksum_address(addr) for addr in wallet_metadata.keys()}


# ============================================================================
# CONSTANTS
# ============================================================================

# NFTfi Contract Addresses - ALL VERSIONS
NFTFI_V3_CONTRACTS = {
    "0x9f10d706d789e4c76a1a6434cd1a9841c875c0a6": "AssetOfferLoan_v3",
    "0xb6adec2acc851d30d5fb64f3137234bcdcbbad0d": "CollectionOfferLoan_v3",
}

NFTFI_V23_CONTRACTS = {
    "0xd0a40eb7fd94ee97102ba8e9342243a2b2e22207": "DirectLoanFixedOffer_v2.3",
    "0xd0c6e59b50c32530c627107f50acc71958c4341f": "DirectLoanFixedCollectionOffer_v2.3",
}

NFTFI_V21_CONTRACTS = {
    "0x8252df1d8b29057d1afe3062bf5a64d503152bc8": "DirectLoanFixedOfferRedeploy_v2.1",
}

NFTFI_V2_CONTRACTS = {
    "0xf896527c49b44aab3cf22ae356fa3af8e331f280": "DirectLoanFixedOffer_v2",
    "0xe52cec0e90115abeb3304baa36bc2655731f7934": "DirectLoanFixedCollectionOffer_v2",
}

# NFTfi refinancing contracts (multiple versions)
NFTFI_REFINANCING_CONTRACTS = {
    "0x6701b1d2d4246d94c94e8227a8619f0b56280115": "Refinancing_v1",
    "0x4bc5fa56f2931e7a37417fa55dda71e4b7c2f2a3": "Refinancing_v2",
}
# Keep single for backward compatibility
NFTFI_REFINANCING_CONTRACT = "0x6701b1d2d4246d94c94e8227a8619f0b56280115"

# Additional loan contracts not in the versioned sets above
NFTFI_ADDITIONAL_CONTRACTS = {
    "0x1e0447b19bb6ecfdae1e4ae1694b0c3659614e4e": "DirectLoanFixedCollectionOffer_v2.3_newer",
}

# All loan contracts (for scanning)
ALL_NFTFI_LOAN_CONTRACTS = {
    **NFTFI_V3_CONTRACTS,
    **NFTFI_V23_CONTRACTS,
    **NFTFI_V21_CONTRACTS,
    **NFTFI_V2_CONTRACTS,
    **NFTFI_ADDITIONAL_CONTRACTS,
}

# Supporting contracts
NFTFI_LOAN_COORDINATOR = "0xa3ed5b592855635c1fbfe1f59486578050234964"
NFTFI_PROMISSORY_NOTE = "0x77b53beb6ea9d8d41e7e7bda0543ee70b0c96af7"
NFTFI_OBLIGATION_RECEIPT = "0x48ed998e9e0a7a8cadf7e930c77e5d73c3bd9e68"

# Token Addresses
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
USDC_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
DAI_ADDRESS = "0x6B175474E89094C44Da98b954EedeAC495271d0F"

# Conversion Constants
WAD = Decimal(10**18)  # 18 decimals (ETH/WETH/DAI)
USDC_DECIMALS = Decimal(10**6)  # 6 decimals (USDC)
SECS_PER_YEAR = Decimal(365 * 24 * 3600)  # 31,536,000 seconds
HUNDRED_PERCENT = Decimal(10000)  # Basis points

# Platform Identifier
PLATFORM = "ORNFI"


# ============================================================================
# ENUMS
# ============================================================================

class NFTfiVersion(Enum):
    """NFTfi contract versions"""
    V3 = "v3"
    V23 = "v2.3"
    V21 = "v2.1"
    V2 = "v2"
    UNKNOWN = "unknown"


class NFTfiEventType(Enum):
    """NFTfi event types"""
    LOAN_STARTED = "LoanStarted"
    LOAN_REPAID = "LoanRepaid"
    LOAN_LIQUIDATED = "LoanLiquidated"
    LOAN_RENEGOTIATED = "LoanRenegotiated"
    REFINANCED = "Refinanced"


class RefinancingRole(Enum):
    """Role of fund in refinancing transaction"""
    OLD_LENDER_ONLY = "old_lender_only"         # Fund was old lender, different new lender
    NEW_LENDER_ONLY = "new_lender_only"         # Fund is new lender, different old lender
    ROLLOVER = "rollover"                        # Fund is BOTH old and new lender
    BORROWER = "borrower"                        # Fund is the borrower
    NOT_INVOLVED = "not_involved"                # Fund not involved


# ============================================================================
# VERSION DETECTION
# ============================================================================

def detect_contract_version(contract_address: str) -> NFTfiVersion:
    """Detect NFTfi contract version from address"""
    addr = contract_address.lower()

    if addr in NFTFI_V3_CONTRACTS:
        return NFTfiVersion.V3
    elif addr in NFTFI_V23_CONTRACTS:
        return NFTfiVersion.V23
    elif addr in NFTFI_V21_CONTRACTS:
        return NFTfiVersion.V21
    elif addr in NFTFI_V2_CONTRACTS:
        return NFTfiVersion.V2
    else:
        return NFTfiVersion.UNKNOWN


def is_v3_contract(contract_address: str) -> bool:
    """Check if contract is v3"""
    return contract_address.lower() in NFTFI_V3_CONTRACTS


def is_v23_contract(contract_address: str) -> bool:
    """Check if contract is v2.3"""
    return contract_address.lower() in NFTFI_V23_CONTRACTS


# ============================================================================
# DATA CLASSES - V3 LOAN TERMS (15 fields)
# ============================================================================

@dataclass
class LoanTermsV3:
    """
    NFTfi V3 LoanTerms struct (15 fields).

    Field order from ABI (CRITICAL - must match exactly):
    0: loanPrincipalAmount
    1: maximumRepaymentAmount
    2: nftCollateralId
    3: loanERC20Denomination
    4: loanDuration
    5: loanInterestRateForDurationInBasisPoints
    6: loanAdminFeeInBasisPoints
    7: originationFee
    8: nftCollateralWrapper
    9: loanStartTime
    10: nftCollateralContract
    11: borrower
    12: lender
    13: escrow
    14: isProRata
    """
    loan_principal_amount: int          # Wei (index 0)
    maximum_repayment_amount: int       # Wei (index 1)
    nft_collateral_id: int              # (index 2)
    loan_erc20_denomination: str        # (index 3)
    loan_duration: int                  # Seconds (index 4)
    loan_interest_rate_bps: int         # Basis points (index 5)
    loan_admin_fee_bps: int             # Basis points (index 6)
    origination_fee: int                # Wei (index 7)
    nft_collateral_wrapper: str         # (index 8)
    loan_start_time: int                # Unix timestamp (index 9)
    nft_collateral_contract: str        # (index 10)
    borrower: str                       # (index 11)
    lender: str                         # (index 12)
    escrow: str                         # (index 13)
    is_pro_rata: bool                   # (index 14)

    @classmethod
    def from_tuple(cls, data: tuple) -> 'LoanTermsV3':
        """Create from event/contract call tuple - V3 field order"""
        return cls(
            loan_principal_amount=int(data[0]),
            maximum_repayment_amount=int(data[1]),
            nft_collateral_id=int(data[2]),
            loan_erc20_denomination=str(data[3]).lower(),
            loan_duration=int(data[4]),
            loan_interest_rate_bps=int(data[5]),
            loan_admin_fee_bps=int(data[6]),
            origination_fee=int(data[7]),
            nft_collateral_wrapper=str(data[8]).lower(),
            loan_start_time=int(data[9]),
            nft_collateral_contract=str(data[10]).lower(),
            borrower=str(data[11]).lower(),
            lender=str(data[12]).lower(),
            escrow=str(data[13]).lower(),
            is_pro_rata=bool(data[14]),
        )

    def get_cryptocurrency_symbol(self) -> str:
        """Determine cryptocurrency symbol from denomination address"""
        if self.loan_erc20_denomination.lower() == USDC_ADDRESS.lower():
            return "USDC"
        elif self.loan_erc20_denomination.lower() == DAI_ADDRESS.lower():
            return "DAI"
        return "WETH"

    def get_decimals(self) -> Decimal:
        """Get decimal divisor for the loan's currency"""
        if self.get_cryptocurrency_symbol() == "USDC":
            return USDC_DECIMALS
        return WAD

    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'loanPrincipalAmount': self.loan_principal_amount,
            'maximumRepaymentAmount': self.maximum_repayment_amount,
            'nftCollateralId': self.nft_collateral_id,
            'loanERC20Denomination': self.loan_erc20_denomination,
            'loanDuration': self.loan_duration,
            'loanInterestRateForDurationInBasisPoints': self.loan_interest_rate_bps,
            'loanAdminFeeInBasisPoints': self.loan_admin_fee_bps,
            'originationFee': self.origination_fee,
            'nftCollateralWrapper': self.nft_collateral_wrapper,
            'loanStartTime': self.loan_start_time,
            'nftCollateralContract': self.nft_collateral_contract,
            'borrower': self.borrower,
            'lender': self.lender,
            'escrow': self.escrow,
            'isProRata': self.is_pro_rata,
        }


# ============================================================================
# DATA CLASSES - V2.3 LOAN TERMS (11 fields) + LOAN EXTRAS
# ============================================================================

@dataclass
class LoanTermsV23:
    """
    NFTfi V2.3 LoanTerms struct (11 fields).

    Field order from ABI:
    0: loanPrincipalAmount
    1: maximumRepaymentAmount
    2: nftCollateralId
    3: loanERC20Denomination
    4: loanDuration
    5: loanInterestRateForDurationInBasisPoints
    6: loanAdminFeeInBasisPoints
    7: nftCollateralWrapper
    8: loanStartTime
    9: nftCollateralContract
    10: borrower

    NOTE: V2.3 is ALWAYS fixed interest (no isProRata field)
    """
    loan_principal_amount: int          # Wei (index 0)
    maximum_repayment_amount: int       # Wei (index 1)
    nft_collateral_id: int              # (index 2)
    loan_erc20_denomination: str        # (index 3)
    loan_duration: int                  # Seconds (index 4)
    loan_interest_rate_bps: int         # Basis points (index 5)
    loan_admin_fee_bps: int             # Basis points (index 6)
    nft_collateral_wrapper: str         # (index 7)
    loan_start_time: int                # Unix timestamp (index 8)
    nft_collateral_contract: str        # (index 9)
    borrower: str                       # (index 10)

    # V2.3 is ALWAYS fixed interest
    is_pro_rata: bool = False

    # V2.3 doesn't have these in struct
    origination_fee: int = 0
    lender: str = ""
    escrow: str = ""

    @classmethod
    def from_tuple(cls, data: tuple) -> 'LoanTermsV23':
        """Create from event/contract call tuple - V2.3 field order"""
        return cls(
            loan_principal_amount=int(data[0]),
            maximum_repayment_amount=int(data[1]),
            nft_collateral_id=int(data[2]),
            loan_erc20_denomination=str(data[3]).lower(),
            loan_duration=int(data[4]),
            loan_interest_rate_bps=int(data[5]),
            loan_admin_fee_bps=int(data[6]),
            nft_collateral_wrapper=str(data[7]).lower(),
            loan_start_time=int(data[8]),
            nft_collateral_contract=str(data[9]).lower(),
            borrower=str(data[10]).lower(),
        )

    def get_cryptocurrency_symbol(self) -> str:
        """Determine cryptocurrency symbol from denomination address"""
        if self.loan_erc20_denomination.lower() == USDC_ADDRESS.lower():
            return "USDC"
        elif self.loan_erc20_denomination.lower() == DAI_ADDRESS.lower():
            return "DAI"
        return "WETH"

    def get_decimals(self) -> Decimal:
        """Get decimal divisor for the loan's currency"""
        if self.get_cryptocurrency_symbol() == "USDC":
            return USDC_DECIMALS
        return WAD

    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'loanPrincipalAmount': self.loan_principal_amount,
            'maximumRepaymentAmount': self.maximum_repayment_amount,
            'nftCollateralId': self.nft_collateral_id,
            'loanERC20Denomination': self.loan_erc20_denomination,
            'loanDuration': self.loan_duration,
            'loanInterestRateForDurationInBasisPoints': self.loan_interest_rate_bps,
            'loanAdminFeeInBasisPoints': self.loan_admin_fee_bps,
            'nftCollateralWrapper': self.nft_collateral_wrapper,
            'loanStartTime': self.loan_start_time,
            'nftCollateralContract': self.nft_collateral_contract,
            'borrower': self.borrower,
            'isProRata': self.is_pro_rata,
            'originationFee': self.origination_fee,
            'lender': self.lender,
        }


@dataclass
class LoanExtrasV23:
    """
    NFTfi V2.3 LoanExtras struct (revenue sharing).
    Only present in v2.3, removed in v3.
    """
    revenue_share_partner: str
    revenue_share_bps: int
    referral_fee_bps: int

    @classmethod
    def from_tuple(cls, data: tuple) -> 'LoanExtrasV23':
        """Create from event tuple"""
        return cls(
            revenue_share_partner=str(data[0]).lower(),
            revenue_share_bps=int(data[1]),
            referral_fee_bps=int(data[2]),
        )


# ============================================================================
# UNIFIED LOAN TERMS (works with both versions)
# ============================================================================

@dataclass
class UnifiedLoanTerms:
    """
    Unified loan terms that work across all NFTfi versions.
    Normalizes differences between v2.3 and v3.
    """
    loan_principal_amount: int
    maximum_repayment_amount: int
    nft_collateral_id: int
    loan_erc20_denomination: str
    loan_duration: int
    loan_interest_rate_bps: int
    loan_admin_fee_bps: int
    nft_collateral_wrapper: str
    loan_start_time: int
    nft_collateral_contract: str
    borrower: str
    lender: str
    is_pro_rata: bool
    origination_fee: int
    escrow: str

    # Version metadata
    version: NFTfiVersion = NFTfiVersion.UNKNOWN
    contract_address: str = ""

    # V2.3 specific (revenue sharing)
    revenue_share_partner: Optional[str] = None
    revenue_share_bps: Optional[int] = None
    referral_fee_bps: Optional[int] = None

    @classmethod
    def from_v3(cls, terms: LoanTermsV3, contract_address: str = "") -> 'UnifiedLoanTerms':
        """Create from V3 terms"""
        return cls(
            loan_principal_amount=terms.loan_principal_amount,
            maximum_repayment_amount=terms.maximum_repayment_amount,
            nft_collateral_id=terms.nft_collateral_id,
            loan_erc20_denomination=terms.loan_erc20_denomination,
            loan_duration=terms.loan_duration,
            loan_interest_rate_bps=terms.loan_interest_rate_bps,
            loan_admin_fee_bps=terms.loan_admin_fee_bps,
            nft_collateral_wrapper=terms.nft_collateral_wrapper,
            loan_start_time=terms.loan_start_time,
            nft_collateral_contract=terms.nft_collateral_contract,
            borrower=terms.borrower,
            lender=terms.lender,
            is_pro_rata=terms.is_pro_rata,
            origination_fee=terms.origination_fee,
            escrow=terms.escrow,
            version=NFTfiVersion.V3,
            contract_address=contract_address,
        )

    @classmethod
    def from_v23(
        cls,
        terms: LoanTermsV23,
        lender: str,
        extras: Optional[LoanExtrasV23] = None,
        contract_address: str = "",
    ) -> 'UnifiedLoanTerms':
        """Create from V2.3 terms (need lender from event indexed param)"""
        result = cls(
            loan_principal_amount=terms.loan_principal_amount,
            maximum_repayment_amount=terms.maximum_repayment_amount,
            nft_collateral_id=terms.nft_collateral_id,
            loan_erc20_denomination=terms.loan_erc20_denomination,
            loan_duration=terms.loan_duration,
            loan_interest_rate_bps=terms.loan_interest_rate_bps,
            loan_admin_fee_bps=terms.loan_admin_fee_bps,
            nft_collateral_wrapper=terms.nft_collateral_wrapper,
            loan_start_time=terms.loan_start_time,
            nft_collateral_contract=terms.nft_collateral_contract,
            borrower=terms.borrower,
            lender=lender.lower(),
            is_pro_rata=False,  # V2.3 always fixed
            origination_fee=0,  # V2.3 doesn't have this
            escrow="",
            version=NFTfiVersion.V23,
            contract_address=contract_address,
        )

        if extras:
            result.revenue_share_partner = extras.revenue_share_partner
            result.revenue_share_bps = extras.revenue_share_bps
            result.referral_fee_bps = extras.referral_fee_bps

        return result

    def get_cryptocurrency_symbol(self) -> str:
        """Determine cryptocurrency symbol from denomination address"""
        if self.loan_erc20_denomination.lower() == USDC_ADDRESS.lower():
            return "USDC"
        elif self.loan_erc20_denomination.lower() == DAI_ADDRESS.lower():
            return "DAI"
        return "WETH"

    def get_decimals(self) -> Decimal:
        """Get decimal divisor for the loan's currency"""
        if self.get_cryptocurrency_symbol() == "USDC":
            return USDC_DECIMALS
        return WAD


# ============================================================================
# DECODED EVENT DATA CLASS
# ============================================================================

@dataclass
class DecodedNFTfiEvent:
    """Decoded NFTfi event with all fields normalized across versions"""
    event_type: str
    tx_hash: str
    block_number: int
    log_index: int
    transaction_datetime: datetime
    contract_address: str
    version: NFTfiVersion

    # Common fields
    loan_id: Optional[int] = None
    borrower: Optional[str] = None
    lender: Optional[str] = None

    # Loan terms (unified)
    loan_terms: Optional[UnifiedLoanTerms] = None

    # Amounts (in wei)
    loan_principal_amount: Optional[int] = None
    maximum_repayment_amount: Optional[int] = None
    amount_paid_to_lender: Optional[int] = None
    admin_fee: Optional[int] = None
    origination_fee: Optional[int] = None

    # NFT info
    nft_collateral_contract: Optional[str] = None
    nft_collateral_id: Optional[int] = None

    # Asset info
    loan_erc20_denomination: Optional[str] = None
    cryptocurrency: Optional[str] = None

    # LoanLiquidated specific
    loan_maturity_date: Optional[int] = None
    loan_liquidation_date: Optional[int] = None

    # LoanRenegotiated specific
    new_loan_duration: Optional[int] = None
    new_maximum_repayment_amount: Optional[int] = None
    renegotiation_fee: Optional[int] = None
    renegotiation_admin_fee: Optional[int] = None
    is_pro_rata: Optional[bool] = None

    # LoanRepaid V2.3 specific (revenue share)
    revenue_share: Optional[int] = None
    revenue_share_partner: Optional[str] = None

    # Refinanced specific
    old_loan_contract: Optional[str] = None
    old_loan_id: Optional[int] = None
    new_loan_id: Optional[int] = None

    # Computed fields
    loan_duration: Optional[int] = None
    loan_start_time: Optional[int] = None
    loan_admin_fee_bps: Optional[int] = None

    def to_dict(self) -> Dict:
        """Convert to dictionary for DataFrame"""
        # Helper to convert integer fields (non-ID) - still use int for amounts/timestamps
        def safe_int(val):
            if val is None:
                return None
            return int(val)

        # Helper to convert ID fields to string (avoids .0 suffix in pandas)
        # IDs are identifiers, not numbers for arithmetic, so string is appropriate
        def safe_id_str(val):
            if val is None:
                return None
            try:
                return str(int(val))
            except (ValueError, TypeError):
                return str(val)

        result = {
            'event': self.event_type,
            'transactionHash': self.tx_hash,
            'blockNumber': safe_int(self.block_number),
            'logIndex': safe_int(self.log_index),
            'transaction_datetime': self.transaction_datetime,
            'contract_address': self.contract_address,
            'version': self.version.value,
            'loan_id': safe_id_str(self.loan_id),
            'borrower': self.borrower,
            'lender': self.lender,
            'loanPrincipalAmount': safe_int(self.loan_principal_amount),
            'maximumRepaymentAmount': safe_int(self.maximum_repayment_amount),
            'amountPaidToLender': safe_int(self.amount_paid_to_lender),
            'adminFee': safe_int(self.admin_fee),
            'originationFee': safe_int(self.origination_fee),
            'nftCollateralContract': self.nft_collateral_contract,
            'nftCollateralId': safe_id_str(self.nft_collateral_id),
            'loanERC20Denomination': self.loan_erc20_denomination,
            'cryptocurrency': self.cryptocurrency,
            'loanMaturityDate': safe_int(self.loan_maturity_date),
            'loanLiquidationDate': safe_int(self.loan_liquidation_date),
            'newLoanDuration': safe_int(self.new_loan_duration),
            'newMaximumRepaymentAmount': safe_int(self.new_maximum_repayment_amount),
            'renegotiationFee': safe_int(self.renegotiation_fee),
            'renegotiationAdminFee': safe_int(self.renegotiation_admin_fee),
            'isProRata': self.is_pro_rata,
            'revenueShare': safe_int(self.revenue_share),
            'revenueSharePartner': self.revenue_share_partner,
            'oldLoanContract': self.old_loan_contract,
            'oldLoanId': safe_id_str(self.old_loan_id),
            'newLoanId': safe_id_str(self.new_loan_id),
            'loanDuration': safe_int(self.loan_duration),
            'loanStartTime': safe_int(self.loan_start_time),
            'loanAdminFeeInBasisPoints': safe_int(self.loan_admin_fee_bps),
        }

        # Add loan terms fields if present
        if self.loan_terms:
            result['loanDuration'] = safe_int(self.loan_terms.loan_duration)
            result['loanStartTime'] = safe_int(self.loan_terms.loan_start_time)
            result['loanAdminFeeInBasisPoints'] = safe_int(self.loan_terms.loan_admin_fee_bps)
            result['isProRata'] = self.loan_terms.is_pro_rata

        return result


# ============================================================================
# ACCRUED INTEREST RESULT
# ============================================================================

@dataclass
class AccruedInterestResult:
    """Result of accrued interest calculation"""
    gross_interest_wei: int             # Total interest before admin fee
    admin_fee_wei: int                  # Platform fee portion
    net_interest_wei: int               # Interest to lender (gross - admin)

    gross_interest_crypto: Decimal      # In native units (ETH/USDC)
    admin_fee_crypto: Decimal
    net_interest_crypto: Decimal

    elapsed_seconds: int
    is_pro_rata: bool
    calculation_method: str             # "PRO_RATA" or "FIXED"

    # For reference
    principal_wei: int
    maximum_repayment_wei: int
    loan_duration_seconds: int
    admin_fee_bps: int


# ============================================================================
# ABI DEFINITIONS
# ============================================================================

# V3 getLoanTerms ABI
LOAN_TERMS_V3_ABI = json.loads('''[
    {
        "inputs": [{"internalType": "uint32", "name": "_loanId", "type": "uint32"}],
        "name": "getLoanTerms",
        "outputs": [
            {
                "components": [
                    {"internalType": "uint256", "name": "loanPrincipalAmount", "type": "uint256"},
                    {"internalType": "uint256", "name": "maximumRepaymentAmount", "type": "uint256"},
                    {"internalType": "uint256", "name": "nftCollateralId", "type": "uint256"},
                    {"internalType": "address", "name": "loanERC20Denomination", "type": "address"},
                    {"internalType": "uint32", "name": "loanDuration", "type": "uint32"},
                    {"internalType": "uint16", "name": "loanInterestRateForDurationInBasisPoints", "type": "uint16"},
                    {"internalType": "uint16", "name": "loanAdminFeeInBasisPoints", "type": "uint16"},
                    {"internalType": "uint256", "name": "originationFee", "type": "uint256"},
                    {"internalType": "address", "name": "nftCollateralWrapper", "type": "address"},
                    {"internalType": "uint64", "name": "loanStartTime", "type": "uint64"},
                    {"internalType": "address", "name": "nftCollateralContract", "type": "address"},
                    {"internalType": "address", "name": "borrower", "type": "address"},
                    {"internalType": "address", "name": "lender", "type": "address"},
                    {"internalType": "address", "name": "escrow", "type": "address"},
                    {"internalType": "bool", "name": "isProRata", "type": "bool"}
                ],
                "internalType": "struct LoanData.LoanTerms",
                "name": "",
                "type": "tuple"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"internalType": "uint32", "name": "_loanId", "type": "uint32"}],
        "name": "loanRepaidOrLiquidated",
        "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function"
    }
]''')

# V2.3 loanIdToLoan ABI (public mapping)
LOAN_TERMS_V23_ABI = json.loads('''[
    {
        "inputs": [{"internalType": "uint32", "name": "", "type": "uint32"}],
        "name": "loanIdToLoan",
        "outputs": [
            {"internalType": "uint256", "name": "loanPrincipalAmount", "type": "uint256"},
            {"internalType": "uint256", "name": "maximumRepaymentAmount", "type": "uint256"},
            {"internalType": "uint256", "name": "nftCollateralId", "type": "uint256"},
            {"internalType": "address", "name": "loanERC20Denomination", "type": "address"},
            {"internalType": "uint32", "name": "loanDuration", "type": "uint32"},
            {"internalType": "uint16", "name": "loanInterestRateForDurationInBasisPoints", "type": "uint16"},
            {"internalType": "uint16", "name": "loanAdminFeeInBasisPoints", "type": "uint16"},
            {"internalType": "address", "name": "nftCollateralWrapper", "type": "address"},
            {"internalType": "uint64", "name": "loanStartTime", "type": "uint64"},
            {"internalType": "address", "name": "nftCollateralContract", "type": "address"},
            {"internalType": "address", "name": "borrower", "type": "address"}
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"internalType": "uint32", "name": "", "type": "uint32"}],
        "name": "loanIdToLoanExtras",
        "outputs": [
            {"internalType": "address", "name": "revenueSharePartner", "type": "address"},
            {"internalType": "uint16", "name": "revenueShareInBasisPoints", "type": "uint16"},
            {"internalType": "uint16", "name": "referralFeeInBasisPoints", "type": "uint16"}
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"internalType": "uint32", "name": "", "type": "uint32"}],
        "name": "loanRepaidOrLiquidated",
        "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function"
    }
]''')


# ============================================================================
# ON-CHAIN QUERY CLASS
# ============================================================================

class NFTfiOnChainQuery:
    """
    Query NFTfi loan state directly from blockchain.
    Supports both V3 (getLoanTerms) and V2.3 (loanIdToLoan).
    """

    def __init__(self, w3: Web3):
        self.w3 = w3
        self._contracts_v3: Dict[str, Any] = {}
        self._contracts_v23: Dict[str, Any] = {}
        self._block_cache: Dict[int, int] = {}

    def _get_contract_v3(self, contract_address: str) -> Any:
        """Get or create V3 contract instance"""
        addr_lower = contract_address.lower()
        if addr_lower not in self._contracts_v3:
            checksum_addr = Web3.to_checksum_address(contract_address)
            self._contracts_v3[addr_lower] = self.w3.eth.contract(
                address=checksum_addr,
                abi=LOAN_TERMS_V3_ABI
            )
        return self._contracts_v3[addr_lower]

    def _get_contract_v23(self, contract_address: str) -> Any:
        """Get or create V2.3 contract instance"""
        addr_lower = contract_address.lower()
        if addr_lower not in self._contracts_v23:
            checksum_addr = Web3.to_checksum_address(contract_address)
            self._contracts_v23[addr_lower] = self.w3.eth.contract(
                address=checksum_addr,
                abi=LOAN_TERMS_V23_ABI
            )
        return self._contracts_v23[addr_lower]

    @lru_cache(maxsize=10000)
    def get_block_timestamp(self, block_number: int) -> datetime:
        """Get block timestamp with caching"""
        if block_number in self._block_cache:
            ts = self._block_cache[block_number]
        else:
            block = self.w3.eth.get_block(block_number)
            ts = block['timestamp']
            self._block_cache[block_number] = ts
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    def get_loan_terms(
        self,
        loan_id: int,
        contract_address: str,
        block_identifier: int = 'latest'
    ) -> Optional[UnifiedLoanTerms]:
        """
        Query loan terms from on-chain at a specific block.
        Automatically handles V3 vs V2.3 differences.
        """
        version = detect_contract_version(contract_address)

        if version == NFTfiVersion.V3:
            return self._get_loan_terms_v3(loan_id, contract_address, block_identifier)
        elif version in [NFTfiVersion.V23, NFTfiVersion.V21, NFTfiVersion.V2]:
            return self._get_loan_terms_v23(loan_id, contract_address, block_identifier)
        else:
            print(f"[\!] Unknown contract version for {contract_address}")
            return None

    def _get_loan_terms_v3(
        self,
        loan_id: int,
        contract_address: str,
        block_identifier: int = 'latest'
    ) -> Optional[UnifiedLoanTerms]:
        """Query V3 loan terms using getLoanTerms()"""
        try:
            contract = self._get_contract_v3(contract_address)
            result = contract.functions.getLoanTerms(int(loan_id)).call(
                block_identifier=block_identifier
            )
            terms_v3 = LoanTermsV3.from_tuple(result)
            return UnifiedLoanTerms.from_v3(terms_v3, contract_address)
        except Exception as e:
            print(f"[\!] Error querying V3 loan {loan_id}: {e}")
            return None

    def _get_loan_terms_v23(
        self,
        loan_id: int,
        contract_address: str,
        block_identifier: int = 'latest'
    ) -> Optional[UnifiedLoanTerms]:
        """Query V2.3 loan terms using loanIdToLoan() mapping"""
        try:
            contract = self._get_contract_v23(contract_address)

            # Get main loan data
            result = contract.functions.loanIdToLoan(int(loan_id)).call(
                block_identifier=block_identifier
            )
            terms_v23 = LoanTermsV23.from_tuple(result)

            # Get extras (revenue sharing)
            try:
                extras_result = contract.functions.loanIdToLoanExtras(int(loan_id)).call(
                    block_identifier=block_identifier
                )
                extras = LoanExtrasV23.from_tuple(extras_result)
            except:
                extras = None

            # Note: V2.3 doesn't store lender in struct, need from event
            # Return without lender, caller must set from event
            return UnifiedLoanTerms.from_v23(
                terms_v23,
                lender="",  # Must be set from event
                extras=extras,
                contract_address=contract_address
            )
        except Exception as e:
            print(f"[\!] Error querying V2.3 loan {loan_id}: {e}")
            return None

    def is_loan_resolved(
        self,
        loan_id: int,
        contract_address: str,
        block_identifier: int = 'latest'
    ) -> Optional[bool]:
        """Check if loan has been repaid or liquidated"""
        try:
            version = detect_contract_version(contract_address)

            if version == NFTfiVersion.V3:
                contract = self._get_contract_v3(contract_address)
            else:
                contract = self._get_contract_v23(contract_address)

            result = contract.functions.loanRepaidOrLiquidated(int(loan_id)).call(
                block_identifier=block_identifier
            )
            return bool(result)
        except Exception as e:
            print(f"[\!] Error checking loan status {loan_id}: {e}")
            return None


# ============================================================================
# INTEREST CALCULATION ENGINE
# ============================================================================

class NFTfiInterestCalculator:
    """
    Calculate accrued interest using NFTfi's exact on-chain logic.

    Mirrors _computeInterestDue() from AssetOfferLoan.sol:

    For FIXED loans (isProRata = false):
        interest = maximumRepaymentAmount - loanPrincipalAmount

    For PRO_RATA loans (isProRata = true):
        interest = (maxInterest * elapsedTime) / totalDuration
        capped at maxInterest
    """

    @staticmethod
    def compute_interest_due(
        loan_principal_amount: int,
        maximum_repayment_amount: int,
        loan_duration_so_far_seconds: int,
        loan_total_duration_seconds: int,
        is_pro_rata: bool
    ) -> int:
        """
        Compute interest due in Wei - mirrors Solidity logic exactly.
        """
        # Fixed rate: full interest regardless of time elapsed
        if not is_pro_rata:
            return maximum_repayment_amount - loan_principal_amount

        # Pro-rata: interest prorated by elapsed time
        max_interest = maximum_repayment_amount - loan_principal_amount

        if loan_total_duration_seconds == 0:
            return 0

        # Pro-rate by elapsed time
        interest_due = (max_interest * loan_duration_so_far_seconds) // loan_total_duration_seconds

        # Cap at maximum interest
        return min(interest_due, max_interest)

    @staticmethod
    def compute_admin_fee(interest_due: int, admin_fee_bps: int) -> int:
        """Calculate admin fee from interest"""
        return (interest_due * admin_fee_bps) // 10000

    def calculate_accrued_interest(
        self,
        loan_terms: UnifiedLoanTerms,
        calculation_timestamp: int,
    ) -> AccruedInterestResult:
        """
        Calculate accrued interest at a specific timestamp.
        """
        # Calculate elapsed time
        elapsed_seconds = calculation_timestamp - loan_terms.loan_start_time
        elapsed_seconds = max(0, elapsed_seconds)

        # Calculate gross interest using Solidity-equivalent logic
        gross_interest_wei = self.compute_interest_due(
            loan_principal_amount=loan_terms.loan_principal_amount,
            maximum_repayment_amount=loan_terms.maximum_repayment_amount,
            loan_duration_so_far_seconds=elapsed_seconds,
            loan_total_duration_seconds=loan_terms.loan_duration,
            is_pro_rata=loan_terms.is_pro_rata,
        )

        # Calculate admin fee
        admin_fee_wei = self.compute_admin_fee(
            interest_due=gross_interest_wei,
            admin_fee_bps=loan_terms.loan_admin_fee_bps,
        )

        # Net interest to lender
        net_interest_wei = gross_interest_wei - admin_fee_wei

        # Convert to crypto units
        decimals = loan_terms.get_decimals()

        return AccruedInterestResult(
            gross_interest_wei=gross_interest_wei,
            admin_fee_wei=admin_fee_wei,
            net_interest_wei=net_interest_wei,
            gross_interest_crypto=Decimal(gross_interest_wei) / decimals,
            admin_fee_crypto=Decimal(admin_fee_wei) / decimals,
            net_interest_crypto=Decimal(net_interest_wei) / decimals,
            elapsed_seconds=elapsed_seconds,
            is_pro_rata=loan_terms.is_pro_rata,
            calculation_method="PRO_RATA" if loan_terms.is_pro_rata else "FIXED",
            principal_wei=loan_terms.loan_principal_amount,
            maximum_repayment_wei=loan_terms.maximum_repayment_amount,
            loan_duration_seconds=loan_terms.loan_duration,
            admin_fee_bps=loan_terms.loan_admin_fee_bps,
        )


# ============================================================================
# FORECLOSURE INTEREST HANDLER
# ============================================================================

class NFTfiForeclosureInterestHandler:
    """
    Complete handler for foreclosure interest calculation using on-chain data.
    """

    def __init__(self, w3: Web3):
        self.w3 = w3
        self.query = NFTfiOnChainQuery(w3)
        self.calculator = NFTfiInterestCalculator()

    def get_accrued_interest_for_foreclosure(
        self,
        loan_id: int,
        contract_address: str,
        foreclosure_block: int,
        foreclosure_timestamp: int,
        lender: str = "",  # Needed for V2.3
    ) -> Optional[AccruedInterestResult]:
        """
        Get accrued interest at foreclosure using on-chain data.

        Queries loan state at block N-1 (before foreclosure executed)
        to get the loan terms, then calculates interest.
        """
        # Query loan state at block BEFORE foreclosure
        query_block = foreclosure_block - 1

        loan_terms = self.query.get_loan_terms(
            loan_id=loan_id,
            contract_address=contract_address,
            block_identifier=query_block,
        )

        if loan_terms is None:
            print(f"[\!] Could not retrieve loan terms for loan {loan_id}")
            return None

        # For V2.3, set lender if provided (not in struct)
        if loan_terms.version == NFTfiVersion.V23 and lender:
            loan_terms.lender = lender.lower()

        # Verify loan wasn't already resolved
        was_resolved = self.query.is_loan_resolved(
            loan_id=loan_id,
            contract_address=contract_address,
            block_identifier=query_block,
        )

        if was_resolved:
            print(f"[\!] Loan {loan_id} was already resolved at block {query_block}")
            return None

        # Calculate accrued interest
        result = self.calculator.calculate_accrued_interest(
            loan_terms=loan_terms,
            calculation_timestamp=foreclosure_timestamp,
        )

        return result


# ============================================================================
# EVENT DECODER
# ============================================================================

class NFTfiEventDecoder:
    """
    Decodes NFTfi events from transaction receipts.
    Handles all versions with unified output.
    """

    # Event signatures (keccak256 of event signature)
    EVENT_SIGNATURES = {
        # V3 LoanStarted (15-field tuple)
        'LoanStarted_v3': Web3.keccak(
            text="LoanStarted(uint32,address,address,(uint256,uint256,uint256,address,uint32,uint16,uint16,uint256,address,uint64,address,address,address,address,bool))"
        ),
        # V2.3 LoanStarted (11-field tuple + extras)
        'LoanStarted_v23': Web3.keccak(
            text="LoanStarted(uint32,address,address,(uint256,uint256,uint256,address,uint32,uint16,uint16,address,uint64,address,address),(address,uint16,uint16))"
        ),
        # LoanRepaid V3
        'LoanRepaid_v3': Web3.keccak(
            text="LoanRepaid(uint32,address,address,uint256,uint256,uint256,uint256,address,address)"
        ),
        # LoanRepaid V2.3 (with revenue share)
        'LoanRepaid_v23': Web3.keccak(
            text="LoanRepaid(uint32,address,address,uint256,uint256,uint256,uint256,uint256,address,address,address)"
        ),
        # LoanLiquidated (same across versions)
        'LoanLiquidated': Web3.keccak(
            text="LoanLiquidated(uint32,address,address,uint256,uint256,uint256,uint256,address)"
        ),
        # LoanRenegotiated V3 (with isProRata)
        'LoanRenegotiated_v3': Web3.keccak(
            text="LoanRenegotiated(uint32,address,address,uint32,uint256,uint256,uint256,bool)"
        ),
        # LoanRenegotiated V2.3 (without isProRata)
        'LoanRenegotiated_v23': Web3.keccak(
            text="LoanRenegotiated(uint32,address,address,uint32,uint256,uint256,uint256)"
        ),
        # Refinanced
        'Refinanced': Web3.keccak(
            text="Refinanced(address,uint256,uint32)"
        ),
    }

    def __init__(
        self,
        w3: Web3,
        wallet_metadata: Dict[str, Dict],
    ):
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        # Only include wallets with category == "fund"
        self.fund_wallet_list = [
            addr for addr, info in self.wallet_metadata.items()
            if info.get('category', '').lower() == 'fund'
        ]
        self.query = NFTfiOnChainQuery(w3)

    def decode_transaction(
        self,
        tx_hash: str,
        relevant_events: List[str] = None,
    ) -> List[DecodedNFTfiEvent]:
        """
        Decode all relevant NFTfi events from a transaction.
        """
        if relevant_events is None:
            relevant_events = ['LoanStarted', 'LoanRepaid', 'LoanLiquidated',
                             'LoanRenegotiated', 'Refinanced']

        try:
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)
        except Exception as e:
            print(f"[\!] Could not get receipt for {tx_hash}: {e}")
            return []

        block_number = receipt['blockNumber']
        tx_datetime = self.query.get_block_timestamp(block_number)

        decoded_events = []

        for log in receipt['logs']:
            if len(log['topics']) == 0:
                continue

            topic0 = log['topics'][0]
            contract_addr = log['address'].lower()
            version = detect_contract_version(contract_addr)

            # Try to match event
            for event_name in relevant_events:
                event = self._try_decode_log(
                    log, topic0, event_name, contract_addr, version,
                    tx_hash, block_number, tx_datetime
                )
                if event:
                    decoded_events.append(event)
                    break

        return decoded_events

    def _try_decode_log(
        self,
        log: Dict,
        topic0: bytes,
        event_name: str,
        contract_addr: str,
        version: NFTfiVersion,
        tx_hash: str,
        block_number: int,
        tx_datetime: datetime,
    ) -> Optional[DecodedNFTfiEvent]:
        """Try to decode a log as a specific event type"""

        if event_name == 'LoanStarted':
            return self._decode_loan_started(
                log, topic0, contract_addr, version, tx_hash, block_number, tx_datetime
            )
        elif event_name == 'LoanRepaid':
            return self._decode_loan_repaid(
                log, topic0, contract_addr, version, tx_hash, block_number, tx_datetime
            )
        elif event_name == 'LoanLiquidated':
            return self._decode_loan_liquidated(
                log, topic0, contract_addr, version, tx_hash, block_number, tx_datetime
            )
        elif event_name == 'LoanRenegotiated':
            return self._decode_loan_renegotiated(
                log, topic0, contract_addr, version, tx_hash, block_number, tx_datetime
            )
        elif event_name == 'Refinanced':
            return self._decode_refinanced(
                log, topic0, contract_addr, tx_hash, block_number, tx_datetime
            )

        return None

    def _decode_loan_started(
        self,
        log: Dict,
        topic0: bytes,
        contract_addr: str,
        version: NFTfiVersion,
        tx_hash: str,
        block_number: int,
        tx_datetime: datetime,
    ) -> Optional[DecodedNFTfiEvent]:
        """Decode LoanStarted event (handles V3 and V2.3 differences)"""

        # Check signature match
        if version == NFTfiVersion.V3:
            expected_sig = self.EVENT_SIGNATURES['LoanStarted_v3']
        else:
            expected_sig = self.EVENT_SIGNATURES['LoanStarted_v23']

        if topic0 != expected_sig:
            return None

        # Decode indexed parameters from topics
        # topics[0] = event signature
        # topics[1] = loanId (uint32, padded to 32 bytes)
        # topics[2] = borrower (address)
        # topics[3] = lender (address)

        if len(log['topics']) < 4:
            return None

        loan_id = int.from_bytes(log['topics'][1], byteorder='big')
        borrower = '0x' + log['topics'][2].hex()[-40:]
        lender = '0x' + log['topics'][3].hex()[-40:]

        # Decode non-indexed data
        data = log['data']
        if isinstance(data, str):
            data = bytes.fromhex(data.replace('0x', ''))

        if version == NFTfiVersion.V3:
            # V3: loanTerms tuple (15 fields)
            loan_terms = self._decode_loan_terms_v3_from_data(data)
            if loan_terms:
                # Override lender from event (more reliable)
                loan_terms.lender = lender.lower()
                unified = UnifiedLoanTerms.from_v3(loan_terms, contract_addr)
        else:
            # V2.3: loanTerms tuple (11 fields) + loanExtras tuple (3 fields)
            loan_terms_v23, extras = self._decode_loan_terms_v23_from_data(data)
            if loan_terms_v23:
                unified = UnifiedLoanTerms.from_v23(loan_terms_v23, lender, extras, contract_addr)
            else:
                unified = None

        if not unified:
            return None

        return DecodedNFTfiEvent(
            event_type='LoanStarted',
            tx_hash=tx_hash,
            block_number=block_number,
            log_index=log['logIndex'],
            transaction_datetime=tx_datetime,
            contract_address=contract_addr,
            version=version,
            loan_id=loan_id,
            borrower=borrower.lower(),
            lender=lender.lower(),
            loan_terms=unified,
            loan_principal_amount=unified.loan_principal_amount,
            maximum_repayment_amount=unified.maximum_repayment_amount,
            origination_fee=unified.origination_fee,
            nft_collateral_contract=unified.nft_collateral_contract,
            nft_collateral_id=unified.nft_collateral_id,
            loan_erc20_denomination=unified.loan_erc20_denomination,
            cryptocurrency=unified.get_cryptocurrency_symbol(),
            is_pro_rata=unified.is_pro_rata,
            loan_duration=unified.loan_duration,
            loan_start_time=unified.loan_start_time,
            loan_admin_fee_bps=unified.loan_admin_fee_bps,
            revenue_share_partner=unified.revenue_share_partner,
        )

    def _decode_loan_terms_v3_from_data(self, data: bytes) -> Optional[LoanTermsV3]:
        """Decode V3 LoanTerms from event data"""
        try:
            # V3 LoanTerms: 15 fields, each 32 bytes = 480 bytes total
            # Event data encodes static structs DIRECTLY (no offset pointer)
            offset = 0

            # NO offset pointer skip for event data with static structs

            fields = []
            for i in range(15):
                chunk = data[offset:offset+32]
                if i in [3, 8, 10, 11, 12, 13]:  # Address fields
                    fields.append('0x' + chunk[-20:].hex())
                elif i == 14:  # bool
                    fields.append(int.from_bytes(chunk, 'big') != 0)
                else:  # uint fields
                    fields.append(int.from_bytes(chunk, 'big'))
                offset += 32

            return LoanTermsV3.from_tuple(tuple(fields))
        except Exception as e:
            print(f"[\!] Error decoding V3 LoanTerms: {e}")
            return None

    def _decode_loan_terms_v23_from_data(self, data: bytes) -> Tuple[Optional[LoanTermsV23], Optional[LoanExtrasV23]]:
        """Decode V2.3 LoanTerms + LoanExtras from event data"""
        try:
            offset = 0

            # V2.3 LoanStarted event has TWO static structs concatenated directly
            # NO offset pointers for static structs in event data
            # LoanTerms: 11 fields x 32 = 352 bytes
            # LoanExtras: 3 fields x 32 = 96 bytes
            # Total: 448 bytes

            # LoanTerms: 11 fields
            loan_fields = []
            for i in range(11):
                chunk = data[offset:offset+32]
                if i in [3, 7, 9, 10]:  # Address fields
                    loan_fields.append('0x' + chunk[-20:].hex())
                else:
                    loan_fields.append(int.from_bytes(chunk, 'big'))
                offset += 32

            loan_terms = LoanTermsV23.from_tuple(tuple(loan_fields))

            # LoanExtras: 3 fields
            extra_fields = []
            for i in range(3):
                chunk = data[offset:offset+32]
                if i == 0:  # Address
                    extra_fields.append('0x' + chunk[-20:].hex())
                else:
                    extra_fields.append(int.from_bytes(chunk, 'big'))
                offset += 32

            extras = LoanExtrasV23.from_tuple(tuple(extra_fields))

            return loan_terms, extras
        except Exception as e:
            print(f"[\!] Error decoding V2.3 LoanTerms: {e}")
            return None, None

    def _decode_loan_repaid(
        self,
        log: Dict,
        topic0: bytes,
        contract_addr: str,
        version: NFTfiVersion,
        tx_hash: str,
        block_number: int,
        tx_datetime: datetime,
    ) -> Optional[DecodedNFTfiEvent]:
        """Decode LoanRepaid event"""

        # Check signature
        if version == NFTfiVersion.V3:
            expected_sig = self.EVENT_SIGNATURES['LoanRepaid_v3']
        else:
            expected_sig = self.EVENT_SIGNATURES['LoanRepaid_v23']

        if topic0 != expected_sig:
            return None

        if len(log['topics']) < 4:
            return None

        # Indexed params
        loan_id = int.from_bytes(log['topics'][1], byteorder='big')
        borrower = '0x' + log['topics'][2].hex()[-40:]
        lender = '0x' + log['topics'][3].hex()[-40:]

        # Non-indexed data
        data = log['data']
        if isinstance(data, str):
            data = bytes.fromhex(data.replace('0x', ''))

        offset = 0

        def read_uint():
            nonlocal offset
            val = int.from_bytes(data[offset:offset+32], 'big')
            offset += 32
            return val

        def read_addr():
            nonlocal offset
            val = '0x' + data[offset+12:offset+32].hex()
            offset += 32
            return val

        principal = read_uint()
        nft_collateral_id = read_uint()
        amount_paid_to_lender = read_uint()
        admin_fee = read_uint()

        if version == NFTfiVersion.V3:
            # V3: nftCollateralContract, loanERC20Denomination
            nft_contract = read_addr()
            loan_denomination = read_addr()
            revenue_share = None
            revenue_share_partner = None
        else:
            # V2.3: revenueShare, revenueSharePartner, nftCollateralContract, loanERC20Denomination
            revenue_share = read_uint()
            revenue_share_partner = read_addr()
            nft_contract = read_addr()
            loan_denomination = read_addr()

        # Determine cryptocurrency
        if loan_denomination.lower() == USDC_ADDRESS.lower():
            crypto = "USDC"
        else:
            crypto = "WETH"

        return DecodedNFTfiEvent(
            event_type='LoanRepaid',
            tx_hash=tx_hash,
            block_number=block_number,
            log_index=log['logIndex'],
            transaction_datetime=tx_datetime,
            contract_address=contract_addr,
            version=version,
            loan_id=loan_id,
            borrower=borrower.lower(),
            lender=lender.lower(),
            loan_principal_amount=principal,
            nft_collateral_id=nft_collateral_id,
            amount_paid_to_lender=amount_paid_to_lender,
            admin_fee=admin_fee,
            nft_collateral_contract=nft_contract.lower(),
            loan_erc20_denomination=loan_denomination.lower(),
            cryptocurrency=crypto,
            revenue_share=revenue_share,
            revenue_share_partner=revenue_share_partner.lower() if revenue_share_partner else None,
        )

    def _decode_loan_liquidated(
        self,
        log: Dict,
        topic0: bytes,
        contract_addr: str,
        version: NFTfiVersion,
        tx_hash: str,
        block_number: int,
        tx_datetime: datetime,
    ) -> Optional[DecodedNFTfiEvent]:
        """Decode LoanLiquidated event (same across versions)"""

        if topic0 != self.EVENT_SIGNATURES['LoanLiquidated']:
            return None

        if len(log['topics']) < 4:
            return None

        # Indexed params
        loan_id = int.from_bytes(log['topics'][1], byteorder='big')
        borrower = '0x' + log['topics'][2].hex()[-40:]
        lender = '0x' + log['topics'][3].hex()[-40:]

        # Non-indexed data
        data = log['data']
        if isinstance(data, str):
            data = bytes.fromhex(data.replace('0x', ''))

        offset = 0

        def read_uint():
            nonlocal offset
            val = int.from_bytes(data[offset:offset+32], 'big')
            offset += 32
            return val

        def read_addr():
            nonlocal offset
            val = '0x' + data[offset+12:offset+32].hex()
            offset += 32
            return val

        principal = read_uint()
        nft_collateral_id = read_uint()
        loan_maturity_date = read_uint()
        loan_liquidation_date = read_uint()
        nft_contract = read_addr()

        # NOTE: LoanLiquidated does NOT include loanERC20Denomination
        # Must query on-chain for the loan terms

        return DecodedNFTfiEvent(
            event_type='LoanLiquidated',
            tx_hash=tx_hash,
            block_number=block_number,
            log_index=log['logIndex'],
            transaction_datetime=tx_datetime,
            contract_address=contract_addr,
            version=version,
            loan_id=loan_id,
            borrower=borrower.lower(),
            lender=lender.lower(),
            loan_principal_amount=principal,
            nft_collateral_id=nft_collateral_id,
            loan_maturity_date=loan_maturity_date,
            loan_liquidation_date=loan_liquidation_date,
            nft_collateral_contract=nft_contract.lower(),
            # loan_erc20_denomination will be set by on-chain query
        )

    def _decode_loan_renegotiated(
        self,
        log: Dict,
        topic0: bytes,
        contract_addr: str,
        version: NFTfiVersion,
        tx_hash: str,
        block_number: int,
        tx_datetime: datetime,
    ) -> Optional[DecodedNFTfiEvent]:
        """Decode LoanRenegotiated event"""

        if version == NFTfiVersion.V3:
            expected_sig = self.EVENT_SIGNATURES['LoanRenegotiated_v3']
        else:
            expected_sig = self.EVENT_SIGNATURES['LoanRenegotiated_v23']

        if topic0 != expected_sig:
            return None

        if len(log['topics']) < 4:
            return None

        # Indexed params
        loan_id = int.from_bytes(log['topics'][1], byteorder='big')
        borrower = '0x' + log['topics'][2].hex()[-40:]
        lender = '0x' + log['topics'][3].hex()[-40:]

        # Non-indexed data
        data = log['data']
        if isinstance(data, str):
            data = bytes.fromhex(data.replace('0x', ''))

        offset = 0

        def read_uint():
            nonlocal offset
            val = int.from_bytes(data[offset:offset+32], 'big')
            offset += 32
            return val

        new_duration = read_uint()
        new_max_repayment = read_uint()
        reneg_fee = read_uint()
        reneg_admin_fee = read_uint()

        is_pro_rata = None
        if version == NFTfiVersion.V3:
            is_pro_rata = read_uint() != 0

        return DecodedNFTfiEvent(
            event_type='LoanRenegotiated',
            tx_hash=tx_hash,
            block_number=block_number,
            log_index=log['logIndex'],
            transaction_datetime=tx_datetime,
            contract_address=contract_addr,
            version=version,
            loan_id=loan_id,
            borrower=borrower.lower(),
            lender=lender.lower(),
            new_loan_duration=new_duration,
            new_maximum_repayment_amount=new_max_repayment,
            renegotiation_fee=reneg_fee,
            renegotiation_admin_fee=reneg_admin_fee,
            is_pro_rata=is_pro_rata,
        )

    def _decode_refinanced(
        self,
        log: Dict,
        topic0: bytes,
        contract_addr: str,
        tx_hash: str,
        block_number: int,
        tx_datetime: datetime,
    ) -> Optional[DecodedNFTfiEvent]:
        """Decode Refinanced event (linkage only)"""

        if topic0 != self.EVENT_SIGNATURES['Refinanced']:
            return None

        # Refinanced event:
        # - oldLoanContract (not indexed)
        # - oldLoanId (indexed)
        # - newLoanId (indexed)

        if len(log['topics']) < 3:
            return None

        old_loan_id = int.from_bytes(log['topics'][1], byteorder='big')
        new_loan_id = int.from_bytes(log['topics'][2], byteorder='big')

        # Non-indexed: oldLoanContract
        data = log['data']
        if isinstance(data, str):
            data = bytes.fromhex(data.replace('0x', ''))

        old_loan_contract = '0x' + data[12:32].hex()

        return DecodedNFTfiEvent(
            event_type='Refinanced',
            tx_hash=tx_hash,
            block_number=block_number,
            log_index=log['logIndex'],
            transaction_datetime=tx_datetime,
            contract_address=contract_addr,
            version=NFTfiVersion.UNKNOWN,  # Refinancing contract
            old_loan_contract=old_loan_contract.lower(),
            old_loan_id=old_loan_id,
            new_loan_id=new_loan_id,
        )

    def decode_batch(
        self,
        tx_hashes: List[str],
        relevant_events: List[str] = None,
        max_workers: int = 8,
        filter_fund_wallets: bool = True,
    ) -> pd.DataFrame:
        """Decode events from multiple transactions in parallel"""
        all_events = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_hash = {
                executor.submit(
                    self.decode_transaction,
                    tx_hash,
                    relevant_events
                ): tx_hash
                for tx_hash in tx_hashes
            }

            for future in tqdm(
                as_completed(future_to_hash),
                total=len(tx_hashes),
                desc="Decoding NFTfi events",
                colour="cyan"
            ):
                tx_hash = future_to_hash[future]
                try:
                    events = future.result()
                    for event in events:
                        event_dict = event.to_dict()
                        all_events.append(event_dict)
                except Exception as e:
                    print(f"[\!] Failed to decode {tx_hash}: {e}")

        if not all_events:
            return pd.DataFrame()

        df = pd.DataFrame(all_events)

        # Filter for fund wallets if requested
        if filter_fund_wallets and not df.empty:
            lender_mask = df['lender'].str.lower().isin(self.fund_wallet_list)
            borrower_mask = df['borrower'].str.lower().isin(self.fund_wallet_list)
            df = df[lender_mask | borrower_mask]

        return df


# ============================================================================
# JOURNAL ENTRY GENERATOR
# ============================================================================

class NFTfiJournalEntryGenerator:
    """
    Generates GAAP-compliant journal entries from decoded NFTfi events.
    """

    def __init__(
        self,
        w3: Web3,
        wallet_metadata: Dict[str, Dict],
        path_for_income_interest_accruals: str = None,
    ):
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        # Only include wallets with category == "fund"
        self.fund_wallet_list = [
            addr for addr, info in self.wallet_metadata.items()
            if info.get('category', '').lower() == 'fund'
        ]
        self.path_for_income_interest_accruals = path_for_income_interest_accruals

        # Initialize interest handlers
        self.foreclosure_handler = NFTfiForeclosureInterestHandler(w3)
        self.interest_calculator = NFTfiInterestCalculator()

    def _get_fund_id(self, address: str) -> str:
        info = self.wallet_metadata.get(address.lower(), {})
        return info.get('fund_id', '')

    def _safe_id_str(self, value) -> Optional[str]:
        """
        Safely convert ID values to string, avoiding float formatting like '2054.0'.

        IDs (loan_id, token_id) should be stored as strings since they're identifiers,
        not numbers for arithmetic. This prevents pandas from converting to float.
        """
        if value is None:
            return None
        if pd.isna(value):
            return None
        try:
            return str(int(value))
        except (ValueError, TypeError):
            return str(value)

    def _get_decimals(self, cryptocurrency: str) -> Decimal:
        if cryptocurrency == "USDC":
            return USDC_DECIMALS
        return WAD

    def _detect_currency_from_denomination(self, loan_denomination: str) -> Tuple[str, Decimal]:
        """
        Detect cryptocurrency symbol and decimals from loanERC20Denomination address.

        Args:
            loan_denomination: The ERC20 token contract address (lowercase)

        Returns:
            (cryptocurrency_symbol, decimals)
        """
        loan_denom_lower = str(loan_denomination).lower() if loan_denomination else ""

        if loan_denom_lower == USDC_ADDRESS.lower():
            return "USDC", USDC_DECIMALS
        elif loan_denom_lower == DAI_ADDRESS.lower():
            return "DAI", WAD
        else:
            # Default to WETH (also covers actual WETH address)
            return "WETH", WAD

    # =========================================================================
    # LOAN STARTED - New Loan Origination
    # =========================================================================

    def generate_loan_started_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate journal entries for LoanStarted events"""
        if df.empty:
            return pd.DataFrame()

        loan_started = df[df['event'] == 'LoanStarted'].copy()
        if loan_started.empty:
            return pd.DataFrame()

        journal_rows = []

        for _, row in loan_started.iterrows():
            lender = str(row.get('lender', '')).lower()
            borrower = str(row.get('borrower', '')).lower()

            is_lender_fund = lender in self.fund_wallet_list
            is_borrower_fund = borrower in self.fund_wallet_list

            if is_lender_fund:
                entries = self._generate_lender_new_loan_entries(row)
                journal_rows.extend(entries)

            if is_borrower_fund:
                entries = self._generate_borrower_new_loan_entries(row)
                journal_rows.extend(entries)

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    def _generate_lender_new_loan_entries(self, row: pd.Series) -> List[Dict]:
        """Generate entries for lender originating a loan"""
        lender = str(row.get('lender', '')).lower()
        borrower = str(row.get('borrower', '')).lower()

        # CRITICAL: Detect cryptocurrency from loanERC20Denomination
        loan_denomination = row.get('loanERC20Denomination', '')
        cryptocurrency, decimals = self._detect_currency_from_denomination(loan_denomination)

        # Principal
        principal_wei = int(row.get('loanPrincipalAmount', 0))
        principal = Decimal(principal_wei) / decimals

        # Origination fee (V3 only)
        origination_fee_wei = int(row.get('originationFee', 0) or 0)
        origination_fee = Decimal(origination_fee_wei) / decimals

        # Net cash out = principal (origination fee is paid by borrower, stays with lender)
        cash_out = principal

        # Calculate annual interest rate
        max_repay = int(row.get('maximumRepaymentAmount', 0))
        duration = int(row.get('loanDuration', 1))
        is_pro_rata = row.get('isProRata', False)
        annual_rate = self._compute_annual_rate(principal_wei, max_repay, duration, is_pro_rata)

        # Loan due date
        tx_dt = row.get('transaction_datetime')
        if isinstance(tx_dt, str):
            tx_dt = pd.to_datetime(tx_dt, utc=True)
        loan_due_date = tx_dt + timedelta(seconds=duration)

        # Net payoff (what lender receives at repayment)
        admin_fee_bps = int(row.get('loanAdminFeeInBasisPoints', 500))
        max_interest = max_repay - principal_wei
        admin_fee = max_interest * admin_fee_bps // 10000
        net_payoff_wei = max_repay - admin_fee
        net_payoff = Decimal(net_payoff_wei) / decimals

        common = {
            'date': tx_dt,
            'fund_id': self._get_fund_id(lender),
            'counterparty_fund_id': self._get_fund_id(borrower),
            'wallet_id': lender,
            'cryptocurrency': cryptocurrency,
            'transaction_type': 'investments_lending',
            'platform': PLATFORM,
            'event': 'LoanStarted',
            'hash': row.get('transactionHash', ''),
            'loan_id': self._safe_id_str(row.get('loan_id')),
            'lender': lender,
            'borrower': borrower,
            'from': lender,
            'to': borrower,
            'contract_address': row.get('contract_address', ''),
            'payable_currency': row.get('loanERC20Denomination', WETH_ADDRESS),
            'collateral_address': row.get('nftCollateralContract', ''),
            'token_id': self._safe_id_str(row.get('nftCollateralId')),
            'principal': principal,
            'principal_USD': None,
            'annual_interest_rate': annual_rate,
            'payoff_amount': net_payoff,
            'payoff_amount_USD': None,
            'loan_due_date': loan_due_date,
            'isProRata': is_pro_rata,
            'version': row.get('version', ''),
        }

        entries = []

        # Dr loan_receivable_cryptocurrency (principal)
        entries.append({
            **common,
            'account_name': f'loan_receivable_cryptocurrency_{cryptocurrency.lower()}',
            'debit': principal,
            'credit': Decimal(0),
        })

        # Cr deemed_cash_usd (cash out)
        entries.append({
            **common,
            'account_name': 'deemed_cash_usd',
            'debit': Decimal(0),
            'credit': cash_out,
        })

        return entries

    def _generate_borrower_new_loan_entries(self, row: pd.Series) -> List[Dict]:
        """Generate entries for borrower receiving a loan"""
        lender = str(row.get('lender', '')).lower()
        borrower = str(row.get('borrower', '')).lower()

        # CRITICAL: Detect cryptocurrency from loanERC20Denomination
        loan_denomination = row.get('loanERC20Denomination', '')
        cryptocurrency, decimals = self._detect_currency_from_denomination(loan_denomination)

        principal_wei = int(row.get('loanPrincipalAmount', 0))
        principal = Decimal(principal_wei) / decimals

        origination_fee_wei = int(row.get('originationFee', 0) or 0)
        origination_fee = Decimal(origination_fee_wei) / decimals

        # Net cash received = principal - origination_fee
        cash_received = principal - origination_fee

        tx_dt = row.get('transaction_datetime')
        if isinstance(tx_dt, str):
            tx_dt = pd.to_datetime(tx_dt, utc=True)

        common = {
            'date': tx_dt,
            'fund_id': self._get_fund_id(borrower),
            'counterparty_fund_id': self._get_fund_id(lender),
            'wallet_id': borrower,
            'cryptocurrency': cryptocurrency,
            'transaction_type': 'financing_borrowings',
            'platform': PLATFORM,
            'event': 'LoanStarted',
            'hash': row.get('transactionHash', ''),
            'loan_id': self._safe_id_str(row.get('loan_id')),
            'lender': lender,
            'borrower': borrower,
            'from': lender,
            'to': borrower,
            'contract_address': row.get('contract_address', ''),
            'payable_currency': row.get('loanERC20Denomination', WETH_ADDRESS),
            'collateral_address': row.get('nftCollateralContract', ''),
            'token_id': self._safe_id_str(row.get('nftCollateralId')),
            'principal': principal,
        }

        entries = []

        # Dr deemed_cash_usd (cash received)
        entries.append({
            **common,
            'account_name': 'deemed_cash_usd',
            'debit': cash_received,
            'credit': Decimal(0),
        })

        # Dr loan_origination_fee (if any)
        if origination_fee > 0:
            entries.append({
                **common,
                'account_name': f'loan_origination_fee_expense_{cryptocurrency.lower()}',
                'debit': origination_fee,
                'credit': Decimal(0),
            })

        # Cr note_payable_cryptocurrency (principal)
        entries.append({
            **common,
            'account_name': f'note_payable_cryptocurrency_{cryptocurrency.lower()}',
            'debit': Decimal(0),
            'credit': principal,
        })

        return entries

    # =========================================================================
    # LOAN REPAID - Loan Repayment
    # =========================================================================

    def generate_loan_repaid_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate journal entries for LoanRepaid events"""
        if df.empty:
            return pd.DataFrame()

        loan_repaid = df[df['event'] == 'LoanRepaid'].copy()
        if loan_repaid.empty:
            return pd.DataFrame()

        journal_rows = []

        for _, row in loan_repaid.iterrows():
            lender = str(row.get('lender', '')).lower()
            borrower = str(row.get('borrower', '')).lower()

            is_lender_fund = lender in self.fund_wallet_list
            is_borrower_fund = borrower in self.fund_wallet_list

            if is_lender_fund:
                entries = self._generate_lender_repaid_entries(row)
                journal_rows.extend(entries)

            if is_borrower_fund:
                entries = self._generate_borrower_repaid_entries(row)
                journal_rows.extend(entries)

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    def _generate_lender_repaid_entries(self, row: pd.Series) -> List[Dict]:
        """Generate entries for lender receiving repayment"""
        lender = str(row.get('lender', '')).lower()
        borrower = str(row.get('borrower', '')).lower()

        # CRITICAL: Detect cryptocurrency from loanERC20Denomination
        loan_denomination = row.get('loanERC20Denomination', '')
        cryptocurrency, decimals = self._detect_currency_from_denomination(loan_denomination)

        principal_wei = int(row.get('loanPrincipalAmount', 0))
        amount_paid_wei = int(row.get('amountPaidToLender', 0))
        admin_fee_wei = int(row.get('adminFee', 0))

        principal = Decimal(principal_wei) / decimals
        amount_paid = Decimal(amount_paid_wei) / decimals

        # Interest received = amount paid - principal
        # Note: admin fee already deducted from amountPaidToLender
        interest_received = amount_paid - principal

        tx_dt = row.get('transaction_datetime')
        if isinstance(tx_dt, str):
            tx_dt = pd.to_datetime(tx_dt, utc=True)

        common = {
            'date': tx_dt,
            'fund_id': self._get_fund_id(lender),
            'counterparty_fund_id': self._get_fund_id(borrower),
            'wallet_id': lender,
            'cryptocurrency': cryptocurrency,
            'transaction_type': 'investments_lending',
            'platform': PLATFORM,
            'event': 'LoanRepaid',
            'hash': row.get('transactionHash', ''),
            'loan_id': self._safe_id_str(row.get('loan_id')),
            'lender': lender,
            'borrower': borrower,
            'from': borrower,
            'to': lender,
            'contract_address': row.get('contract_address', ''),
            'payable_currency': row.get('loanERC20Denomination', WETH_ADDRESS),
            'collateral_address': row.get('nftCollateralContract', ''),
            'token_id': self._safe_id_str(row.get('nftCollateralId')),
            'principal': principal,
        }

        entries = []

        # Dr deemed_cash_usd (total received)
        entries.append({
            **common,
            'account_name': 'deemed_cash_usd',
            'debit': amount_paid,
            'credit': Decimal(0),
        })

        # Cr loan_receivable (principal)
        entries.append({
            **common,
            'account_name': f'loan_receivable_cryptocurrency_{cryptocurrency.lower()}',
            'debit': Decimal(0),
            'credit': principal,
        })

        # Cr interest_receivable (interest)
        if interest_received > 0:
            entries.append({
                **common,
                'account_name': f'interest_receivable_cryptocurrency_{cryptocurrency.lower()}',
                'debit': Decimal(0),
                'credit': interest_received,
            })

        return entries

    def _generate_borrower_repaid_entries(self, row: pd.Series) -> List[Dict]:
        """Generate entries for borrower repaying loan"""
        lender = str(row.get('lender', '')).lower()
        borrower = str(row.get('borrower', '')).lower()

        # CRITICAL: Detect cryptocurrency from loanERC20Denomination
        loan_denomination = row.get('loanERC20Denomination', '')
        cryptocurrency, decimals = self._detect_currency_from_denomination(loan_denomination)

        principal_wei = int(row.get('loanPrincipalAmount', 0))
        amount_paid_wei = int(row.get('amountPaidToLender', 0))
        admin_fee_wei = int(row.get('adminFee', 0))

        principal = Decimal(principal_wei) / decimals
        amount_paid = Decimal(amount_paid_wei) / decimals
        admin_fee = Decimal(admin_fee_wei) / decimals

        # Total paid by borrower = amount to lender + admin fee
        total_paid = amount_paid + admin_fee
        interest_paid = total_paid - principal

        tx_dt = row.get('transaction_datetime')
        if isinstance(tx_dt, str):
            tx_dt = pd.to_datetime(tx_dt, utc=True)

        common = {
            'date': tx_dt,
            'fund_id': self._get_fund_id(borrower),
            'counterparty_fund_id': self._get_fund_id(lender),
            'wallet_id': borrower,
            'cryptocurrency': cryptocurrency,
            'transaction_type': 'financing_borrowings',
            'platform': PLATFORM,
            'event': 'LoanRepaid',
            'hash': row.get('transactionHash', ''),
            'loan_id': self._safe_id_str(row.get('loan_id')),
            'lender': lender,
            'borrower': borrower,
            'from': borrower,
            'to': lender,
            'contract_address': row.get('contract_address', ''),
        }

        entries = []

        # Dr note_payable (principal)
        entries.append({
            **common,
            'account_name': f'note_payable_cryptocurrency_{cryptocurrency.lower()}',
            'debit': principal,
            'credit': Decimal(0),
        })

        # Dr interest_payable (interest)
        if interest_paid > 0:
            entries.append({
                **common,
                'account_name': f'interest_payable_cryptocurrency_{cryptocurrency.lower()}',
                'debit': interest_paid,
                'credit': Decimal(0),
            })

        # Cr deemed_cash_usd (total paid)
        entries.append({
            **common,
            'account_name': 'deemed_cash_usd',
            'debit': Decimal(0),
            'credit': total_paid,
        })

        return entries

    # =========================================================================
    # LOAN LIQUIDATED - Foreclosure (with on-chain interest)
    # =========================================================================

    def generate_loan_liquidated_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate journal entries for LoanLiquidated events with on-chain interest"""
        if df.empty:
            return pd.DataFrame()

        loan_liquidated = df[df['event'] == 'LoanLiquidated'].copy()
        if loan_liquidated.empty:
            return pd.DataFrame()

        # Filter for lender = fund wallet
        loan_liquidated = loan_liquidated[
            loan_liquidated['lender'].str.lower().isin(self.fund_wallet_list)
        ]

        if loan_liquidated.empty:
            return pd.DataFrame()

        print(f"📊 Processing {len(loan_liquidated)} foreclosures with on-chain interest")

        journal_rows = []

        for _, row in tqdm(loan_liquidated.iterrows(), total=len(loan_liquidated), desc="Processing foreclosures"):
            entries = self._process_single_foreclosure(row)
            journal_rows.extend(entries)

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    def _process_single_foreclosure(self, row: pd.Series) -> List[Dict]:
        """Process a single foreclosure with on-chain interest calculation"""
        loan_id_raw = row.get('loan_id')
        loan_id = self._safe_id_str(loan_id_raw)
        loan_id_int = int(loan_id_raw) if pd.notna(loan_id_raw) else 0
        block_number = int(row.get('blockNumber', 0))
        foreclosure_ts = int(row.get('loanLiquidationDate', 0))
        contract_address = row.get('contract_address', '')
        lender = str(row.get('lender', '')).lower()
        borrower = str(row.get('borrower', '')).lower()

        # Get on-chain interest calculation
        interest_result = self.foreclosure_handler.get_accrued_interest_for_foreclosure(
            loan_id=loan_id_int,
            contract_address=contract_address,
            foreclosure_block=block_number,
            foreclosure_timestamp=foreclosure_ts,
            lender=lender,
        )

        # Determine cryptocurrency (need to query if not in event)
        if interest_result and interest_result.principal_wei:
            # Get from on-chain query
            loan_terms = self.foreclosure_handler.query.get_loan_terms(
                loan_id=loan_id_int,
                contract_address=contract_address,
                block_identifier=block_number - 1,
            )
            if loan_terms:
                cryptocurrency = loan_terms.get_cryptocurrency_symbol()
                decimals = loan_terms.get_decimals()
            else:
                cryptocurrency = "WETH"
                decimals = WAD
        else:
            cryptocurrency = "WETH"
            decimals = WAD

        # Principal
        principal_wei = int(row.get('loanPrincipalAmount', 0))
        principal = Decimal(principal_wei) / decimals

        # Interest
        if interest_result:
            interest_receivable = interest_result.net_interest_crypto
        else:
            interest_receivable = Decimal(0)

        tx_dt = row.get('transaction_datetime')
        if isinstance(tx_dt, str):
            tx_dt = pd.to_datetime(tx_dt, utc=True)

        loan_maturity = row.get('loanMaturityDate', 0)
        if loan_maturity:
            loan_due_date = datetime.fromtimestamp(int(loan_maturity), tz=timezone.utc)
        else:
            loan_due_date = tx_dt

        common = {
            'date': tx_dt,
            'fund_id': self._get_fund_id(lender),
            'counterparty_fund_id': self._get_fund_id(borrower),
            'wallet_id': lender,
            'cryptocurrency': cryptocurrency,
            'transaction_type': 'investments_foreclosures',
            'platform': PLATFORM,
            'event': 'LoanLiquidated',
            'hash': row.get('transactionHash', ''),
            'loan_id': loan_id,
            'lender': lender,
            'borrower': borrower,
            'from': borrower,
            'to': lender,
            'contract_address': contract_address,
            'collateral_address': row.get('nftCollateralContract', ''),
            'token_id': self._safe_id_str(row.get('nftCollateralId')),
            'principal': principal,
            'interest_calculation_method': interest_result.calculation_method if interest_result else '',
            'loan_due_date': loan_due_date,
        }

        entries = []

        # 1. Dr investments_nfts_seized_collateral (principal)
        entries.append({
            **common,
            'account_name': 'investments_nfts_seized_collateral',
            'debit': principal,
            'credit': Decimal(0),
        })

        # 2. Dr bad_debt_expense (interest)
        if interest_receivable > 0:
            entries.append({
                **common,
                'account_name': f'bad_debt_expense_{cryptocurrency.lower()}',
                'debit': interest_receivable,
                'credit': Decimal(0),
            })

        # 3. Cr loan_receivable (principal)
        entries.append({
            **common,
            'account_name': f'loan_receivable_cryptocurrency_{cryptocurrency.lower()}',
            'debit': Decimal(0),
            'credit': principal,
        })

        # 4. Cr interest_receivable (interest)
        if interest_receivable > 0:
            entries.append({
                **common,
                'account_name': f'interest_receivable_cryptocurrency_{cryptocurrency.lower()}',
                'debit': Decimal(0),
                'credit': interest_receivable,
            })

        return entries

    # =========================================================================
    # LOAN RENEGOTIATED
    # =========================================================================

    def generate_loan_renegotiated_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate journal entries for LoanRenegotiated events"""
        if df.empty:
            return pd.DataFrame()

        loan_reneg = df[df['event'] == 'LoanRenegotiated'].copy()
        if loan_reneg.empty:
            return pd.DataFrame()

        journal_rows = []

        for _, row in loan_reneg.iterrows():
            lender = str(row.get('lender', '')).lower()

            if lender not in self.fund_wallet_list:
                continue

            # Renegotiation fee is paid to lender
            reneg_fee_wei = int(row.get('renegotiationFee', 0) or 0)
            reneg_admin_fee_wei = int(row.get('renegotiationAdminFee', 0) or 0)

            if reneg_fee_wei == 0:
                continue

            # Net fee to lender
            net_fee_wei = reneg_fee_wei - reneg_admin_fee_wei

            # Need to look up cryptocurrency from loan
            # For simplicity, assume WETH
            cryptocurrency = "WETH"
            decimals = WAD

            net_fee = Decimal(net_fee_wei) / decimals

            tx_dt = row.get('transaction_datetime')
            if isinstance(tx_dt, str):
                tx_dt = pd.to_datetime(tx_dt, utc=True)

            borrower = str(row.get('borrower', '')).lower()

            common = {
                'date': tx_dt,
                'fund_id': self._get_fund_id(lender),
                'counterparty_fund_id': self._get_fund_id(borrower),
                'wallet_id': lender,
                'cryptocurrency': cryptocurrency,
                'transaction_type': 'income_interest_accruals',
                'platform': PLATFORM,
                'event': 'LoanRenegotiated',
                'hash': row.get('transactionHash', ''),
                'loan_id': self._safe_id_str(row.get('loan_id')),
                'lender': lender,
                'borrower': borrower,
                'contract_address': row.get('contract_address', ''),
            }

            # Dr deemed_cash_usd (net fee received)
            journal_rows.append({
                **common,
                'account_name': 'deemed_cash_usd',
                'debit': net_fee,
                'credit': Decimal(0),
            })

            # Cr interest_income (net fee)
            journal_rows.append({
                **common,
                'account_name': f'interest_income_cryptocurrency_{cryptocurrency.lower()}',
                'debit': Decimal(0),
                'credit': net_fee,
            })

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    # =========================================================================
    # INTEREST ACCRUALS
    # =========================================================================

    def generate_interest_accruals(self, df_loans: pd.DataFrame) -> pd.DataFrame:
        """Generate daily interest accrual journal entries"""
        if df_loans.empty:
            return pd.DataFrame()

        journal_rows = []

        for _, loan_row in df_loans.iterrows():
            entries = self._generate_loan_interest_accruals(loan_row)
            journal_rows.extend(entries)

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    def _generate_loan_interest_accruals(self, loan_row: pd.Series) -> List[Dict]:
        """Generate daily interest accruals for a single loan (lender) using Wei precision"""
        tx_dt = loan_row.get('transaction_datetime')
        if isinstance(tx_dt, str):
            tx_dt = pd.to_datetime(tx_dt, utc=True)

        loan_duration = int(loan_row.get('loanDuration', 0))
        if loan_duration <= 0:
            return []

        end_dt = tx_dt + timedelta(seconds=loan_duration)

        # CRITICAL: Detect cryptocurrency from loanERC20Denomination
        loan_denomination = loan_row.get('loanERC20Denomination', '')
        cryptocurrency, decimals = self._detect_currency_from_denomination(loan_denomination)

        # Calculate net interest to lender
        principal_wei = int(loan_row.get('loanPrincipalAmount', 0))
        max_repay_wei = int(loan_row.get('maximumRepaymentAmount', 0))
        admin_fee_bps = int(loan_row.get('loanAdminFeeInBasisPoints', 500))
        is_pro_rata = loan_row.get('isProRata', False)

        gross_interest_wei = max_repay_wei - principal_wei
        admin_fee_wei = gross_interest_wei * admin_fee_bps // 10000
        net_interest_wei = gross_interest_wei - admin_fee_wei

        if net_interest_wei <= 0:
            return []

        lender = str(loan_row.get('lender', '')).lower()
        borrower = str(loan_row.get('borrower', '')).lower()

        common = {
            'fund_id': self._get_fund_id(lender),
            'counterparty_fund_id': self._get_fund_id(borrower),
            'wallet_id': lender,
            'cryptocurrency': cryptocurrency,
            'transaction_type': 'income_interest_accruals',
            'platform': PLATFORM,
            'hash': loan_row.get('transactionHash', ''),
            'loan_id': self._safe_id_str(loan_row.get('loan_id')),
            'lender': lender,
            'borrower': borrower,
            'contract_address': loan_row.get('contract_address', ''),
            'collateral_address': loan_row.get('nftCollateralContract', ''),
            'token_id': self._safe_id_str(loan_row.get('nftCollateralId')),
        }

        entries = []
        total_secs = int((end_dt - tx_dt).total_seconds())
        total_interest_wei = int(net_interest_wei)

        leftover = 0
        assigned_so_far = 0
        cursor = tx_dt

        while cursor < end_dt:
            # Calculate next midnight
            tomorrow = cursor.date() + timedelta(days=1)
            naive_midnight = datetime.combine(tomorrow, dt_time(0, 0, 0))
            next_midnight = naive_midnight.replace(tzinfo=cursor.tzinfo)

            segment_end = min(next_midnight, end_dt)
            slice_secs = int((segment_end - cursor).total_seconds())

            # Wei-precise interest allocation
            numer = (total_interest_wei * slice_secs) + leftover
            slice_interest_wei = numer // total_secs
            leftover = numer % total_secs
            assigned_so_far += slice_interest_wei

            # Label timestamp
            if segment_end == next_midnight:
                accrual_label = next_midnight - timedelta(seconds=1)
            else:
                accrual_label = end_dt

            # Convert using correct decimals (USDC=10^6, WETH/DAI=10^18)
            slice_interest = Decimal(slice_interest_wei) / decimals

            # Dr interest_receivable
            entries.append({
                **common,
                'date': accrual_label,
                'account_name': f'interest_receivable_cryptocurrency_{cryptocurrency.lower()}',
                'debit': slice_interest,
                'credit': Decimal(0),
            })

            # Cr interest_income
            entries.append({
                **common,
                'date': accrual_label,
                'account_name': f'interest_income_cryptocurrency_{cryptocurrency.lower()}',
                'debit': Decimal(0),
                'credit': slice_interest,
            })

            cursor = segment_end

        # Final adjustment
        if assigned_so_far < total_interest_wei:
            shortfall_wei = total_interest_wei - assigned_so_far
            shortfall = Decimal(shortfall_wei) / decimals  # Use correct decimals
            if entries:
                entries[-2]['debit'] += shortfall
                entries[-1]['credit'] += shortfall

        return entries

    def generate_interest_expense_accruals(self, df_loans: pd.DataFrame) -> pd.DataFrame:
        """
        Generate daily interest EXPENSE accrual journal entries for borrower.

        When fund is borrower, it accrues interest expense over the loan term:
        - Dr interest_expense_cryptocurrency_xxx
        - Cr interest_payable_cryptocurrency_xxx

        Args:
            df_loans: DataFrame of LoanStarted events where fund is BORROWER

        Returns:
            DataFrame with daily interest expense accrual entries
        """
        if df_loans.empty:
            return pd.DataFrame()

        journal_rows = []

        for _, loan_row in df_loans.iterrows():
            entries = self._generate_borrower_interest_expense_accruals(loan_row)
            journal_rows.extend(entries)

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    def _generate_borrower_interest_expense_accruals(self, loan_row: pd.Series) -> List[Dict]:
        """
        Generate daily interest expense accruals for a borrower loan using Wei precision.

        Borrower accrues GROSS interest (before admin fee), as they pay the full amount.
        """
        tx_dt = loan_row.get('transaction_datetime')
        if isinstance(tx_dt, str):
            tx_dt = pd.to_datetime(tx_dt, utc=True)

        loan_duration = int(loan_row.get('loanDuration', 0))
        if loan_duration <= 0:
            return []

        end_dt = tx_dt + timedelta(seconds=loan_duration)

        # CRITICAL: Detect cryptocurrency from loanERC20Denomination
        loan_denomination = loan_row.get('loanERC20Denomination', '')
        cryptocurrency, decimals = self._detect_currency_from_denomination(loan_denomination)

        # Calculate GROSS interest (what borrower owes - includes admin fee)
        principal_wei = int(loan_row.get('loanPrincipalAmount', 0))
        max_repay_wei = int(loan_row.get('maximumRepaymentAmount', 0))

        # Borrower pays gross interest (admin fee is part of their expense)
        gross_interest_wei = max_repay_wei - principal_wei

        if gross_interest_wei <= 0:
            return []

        lender = str(loan_row.get('lender', '')).lower()
        borrower = str(loan_row.get('borrower', '')).lower()

        common = {
            'fund_id': self._get_fund_id(borrower),  # Fund is borrower
            'counterparty_fund_id': self._get_fund_id(lender),
            'wallet_id': borrower,
            'cryptocurrency': cryptocurrency,
            'transaction_type': 'expense_interest_accruals',
            'platform': PLATFORM,
            'hash': loan_row.get('transactionHash', ''),
            'loan_id': self._safe_id_str(loan_row.get('loan_id')),
            'lender': lender,
            'borrower': borrower,
            'contract_address': loan_row.get('contract_address', ''),
            'collateral_address': loan_row.get('nftCollateralContract', ''),
            'token_id': self._safe_id_str(loan_row.get('nftCollateralId')),
        }

        entries = []
        total_secs = int((end_dt - tx_dt).total_seconds())
        total_interest_wei = int(gross_interest_wei)

        leftover = 0
        assigned_so_far = 0
        cursor = tx_dt

        while cursor < end_dt:
            # Calculate next midnight
            tomorrow = cursor.date() + timedelta(days=1)
            naive_midnight = datetime.combine(tomorrow, dt_time(0, 0, 0))
            next_midnight = naive_midnight.replace(tzinfo=cursor.tzinfo)

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

            # Convert from wei to crypto units using correct decimals
            slice_interest = Decimal(slice_interest_wei) / decimals

            # Dr interest_expense (borrower incurs expense)
            entries.append({
                **common,
                'date': accrual_label,
                'account_name': f'interest_expense_cryptocurrency_{cryptocurrency.lower()}',
                'debit': slice_interest,
                'credit': Decimal(0),
            })

            # Cr interest_payable (liability accrues)
            entries.append({
                **common,
                'date': accrual_label,
                'account_name': f'interest_payable_cryptocurrency_{cryptocurrency.lower()}',
                'debit': Decimal(0),
                'credit': slice_interest,
            })

            cursor = segment_end

        # Final adjustment for any remaining wei
        if assigned_so_far < total_interest_wei:
            shortfall_wei = total_interest_wei - assigned_so_far
            shortfall = Decimal(shortfall_wei) / decimals
            if entries:
                entries[-2]['debit'] += shortfall
                entries[-1]['credit'] += shortfall

        return entries

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _compute_annual_rate(
        self,
        principal_wei: int,
        max_repay_wei: int,
        duration_secs: int,
        is_pro_rata: bool,
    ) -> Decimal:
        """Compute annual interest rate"""
        if principal_wei <= 0 or duration_secs <= 0:
            return Decimal(0)

        gross_interest = max_repay_wei - principal_wei
        if gross_interest <= 0:
            return Decimal(0)

        factor = Decimal(1) + (Decimal(gross_interest) / Decimal(principal_wei))
        T_years = Decimal(duration_secs) / SECS_PER_YEAR

        try:
            r = Decimal(math.log(float(factor))) / T_years
            return r
        except:
            return Decimal(0)


# ============================================================================
# REFINANCING CONTEXT - Tracks Rollover Transactions
# ============================================================================

@dataclass
class RefinancingContext:
    """
    Context for a refinancing transaction.

    Captures all events in a single refinancing tx and determines
    if it's a rollover (same wallet is both old and new lender).

    A refinancing transaction contains:
    1. Refinanced event - provides linkage (oldLoanId -> newLoanId)
    2. LoanRepaid event - closes old loan (shows what old lender receives)
    3. LoanStarted event - opens new loan (shows what new lender sends)
    """
    tx_hash: str
    block_number: int
    transaction_datetime: datetime

    # Linkage from Refinanced event
    old_loan_contract: str
    old_loan_id: int
    new_loan_id: int
    new_loan_contract: str = ""  # V3 target contract

    # Decoded events
    refinanced_event: Optional[DecodedNFTfiEvent] = None
    loan_repaid_event: Optional[DecodedNFTfiEvent] = None
    loan_started_event: Optional[DecodedNFTfiEvent] = None

    # Participants (lowercase)
    old_lender: str = ""
    new_lender: str = ""
    borrower: str = ""

    # Fund's role in this refinancing
    refinancing_role: RefinancingRole = RefinancingRole.NOT_INVOLVED

    # Rollover detection
    is_rollover: bool = False          # Same wallet is both old and new lender
    is_fund_old_lender: bool = False   # Fund was the old lender
    is_fund_new_lender: bool = False   # Fund is the new lender
    is_fund_borrower: bool = False     # Fund is the borrower

    # Cash flows (in wei)
    old_loan_principal_wei: int = 0
    old_loan_amount_paid_to_lender_wei: int = 0  # What old lender actually receives
    old_loan_admin_fee_wei: int = 0               # Admin fee from old loan repayment
    new_loan_principal_wei: int = 0

    # Computed values
    old_loan_interest_received_wei: int = 0  # = amount_paid - principal
    old_loan_total_received_wei: int = 0     # = amount_paid_to_lender

    # Net cash flow for rollover (positive = fund receives, negative = fund pays out)
    net_cash_flow_wei: int = 0

    # Cryptocurrency info
    cryptocurrency: str = "WETH"
    loan_erc20_denomination: str = WETH_ADDRESS
    decimals: Decimal = WAD

    def determine_fund_role(self, fund_wallet_list: Set[str]) -> RefinancingRole:
        """
        Determine the fund's role in this refinancing transaction.

        Args:
            fund_wallet_list: Set of fund wallet addresses (lowercase)

        Returns:
            RefinancingRole enum
        """
        fund_wallets_lower = {w.lower() for w in fund_wallet_list}

        self.is_fund_old_lender = self.old_lender.lower() in fund_wallets_lower
        self.is_fund_new_lender = self.new_lender.lower() in fund_wallets_lower
        self.is_fund_borrower = self.borrower.lower() in fund_wallets_lower

        # CRITICAL: Check for rollover (same wallet is both lenders)
        if self.is_fund_old_lender and self.is_fund_new_lender:
            if self.old_lender.lower() == self.new_lender.lower():
                self.is_rollover = True
                self.refinancing_role = RefinancingRole.ROLLOVER
                return self.refinancing_role

        # Not a rollover - determine which role
        if self.is_fund_old_lender and not self.is_fund_new_lender:
            self.refinancing_role = RefinancingRole.OLD_LENDER_ONLY
        elif self.is_fund_new_lender and not self.is_fund_old_lender:
            self.refinancing_role = RefinancingRole.NEW_LENDER_ONLY
        elif self.is_fund_borrower:
            self.refinancing_role = RefinancingRole.BORROWER
        else:
            self.refinancing_role = RefinancingRole.NOT_INVOLVED

        return self.refinancing_role

    def calculate_amounts(self):
        """Calculate derived amounts from events"""
        # Old loan amounts from LoanRepaid
        if self.loan_repaid_event:
            self.old_loan_principal_wei = self.loan_repaid_event.loan_principal_amount or 0
            self.old_loan_amount_paid_to_lender_wei = self.loan_repaid_event.amount_paid_to_lender or 0
            self.old_loan_admin_fee_wei = self.loan_repaid_event.admin_fee or 0

            self.old_loan_total_received_wei = self.old_loan_amount_paid_to_lender_wei
            self.old_loan_interest_received_wei = self.old_loan_total_received_wei - self.old_loan_principal_wei

            # Get currency info
            if self.loan_repaid_event.loan_erc20_denomination:
                self.loan_erc20_denomination = self.loan_repaid_event.loan_erc20_denomination
                if self.loan_erc20_denomination.lower() == USDC_ADDRESS.lower():
                    self.cryptocurrency = "USDC"
                    self.decimals = USDC_DECIMALS
                else:
                    self.cryptocurrency = "WETH"
                    self.decimals = WAD

        # New loan amounts from LoanStarted
        if self.loan_started_event:
            self.new_loan_principal_wei = self.loan_started_event.loan_principal_amount or 0

        # Calculate net cash flow for rollover
        if self.is_rollover:
            # Net = what we receive from old loan - what we send for new loan
            self.net_cash_flow_wei = self.old_loan_total_received_wei - self.new_loan_principal_wei

    def get_amounts_in_crypto(self) -> Dict[str, Decimal]:
        """Get all amounts in native crypto units"""
        return {
            'old_loan_principal': Decimal(self.old_loan_principal_wei) / self.decimals,
            'old_loan_interest_received': Decimal(self.old_loan_interest_received_wei) / self.decimals,
            'old_loan_total_received': Decimal(self.old_loan_total_received_wei) / self.decimals,
            'new_loan_principal': Decimal(self.new_loan_principal_wei) / self.decimals,
            'net_cash_flow': Decimal(self.net_cash_flow_wei) / self.decimals,
        }

    def to_dict(self) -> Dict:
        """Convert to dictionary for logging/debugging"""
        amounts = self.get_amounts_in_crypto()
        return {
            'tx_hash': self.tx_hash,
            'block_number': self.block_number,
            'transaction_datetime': self.transaction_datetime,
            'old_loan_contract': self.old_loan_contract,
            'old_loan_id': self.old_loan_id,
            'new_loan_id': self.new_loan_id,
            'old_lender': self.old_lender,
            'new_lender': self.new_lender,
            'borrower': self.borrower,
            'refinancing_role': self.refinancing_role.value,
            'is_rollover': self.is_rollover,
            'is_fund_old_lender': self.is_fund_old_lender,
            'is_fund_new_lender': self.is_fund_new_lender,
            'cryptocurrency': self.cryptocurrency,
            'old_loan_principal': float(amounts['old_loan_principal']),
            'old_loan_interest_received_crypto': float(amounts['old_loan_interest_received']),
            'new_loan_principal': float(amounts['new_loan_principal']),
            'net_cash_flow_crypto': float(amounts['net_cash_flow']),
        }


# ============================================================================
# REFINANCING TRANSACTION PROCESSOR
# ============================================================================

class RefinancingProcessor:
    """
    Processes refinancing transactions with rollover detection.

    Key insight: In a refinancing transaction, there are 3 events:
    1. Refinanced - provides linkage (oldLoanId -> newLoanId)
    2. LoanRepaid - closes old loan
    3. LoanStarted - opens new loan

    When the same fund wallet is both old AND new lender, this is a ROLLOVER:
    - Fund RECEIVES: old_loan_principal + interest from LoanRepaid
    - Fund SENDS: new_loan_principal from LoanStarted
    - NET CASH FLOW: received - sent (can be positive or negative)

    Rollover accounting should use NET cash settlement, not gross flows.
    """

    def __init__(
        self,
        w3: Web3,
        wallet_metadata: Dict[str, Dict],
    ):
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        self.fund_wallet_list = set(self.wallet_metadata.keys())

        # Cache for block timestamps
        self._block_cache: Dict[int, int] = {}

    @lru_cache(maxsize=10000)
    def _get_block_timestamp(self, block_number: int) -> datetime:
        """Get block timestamp with caching"""
        if block_number in self._block_cache:
            ts = self._block_cache[block_number]
        else:
            block = self.w3.eth.get_block(block_number)
            ts = block['timestamp']
            self._block_cache[block_number] = ts

        return datetime.fromtimestamp(ts, tz=timezone.utc)

    def identify_refinancing_transactions(
        self,
        events_df: pd.DataFrame,
    ) -> Dict[str, RefinancingContext]:
        """
        Identify and group refinancing transactions from decoded events.

        Args:
            events_df: DataFrame with decoded events (must include Refinanced, LoanRepaid, LoanStarted)

        Returns:
            Dict mapping tx_hash to RefinancingContext for refinancing transactions
        """
        if events_df.empty:
            return {}

        # Find all Refinanced events
        refinanced_events = events_df[events_df['event'] == 'Refinanced']

        if refinanced_events.empty:
            return {}

        contexts = {}

        for _, ref_row in refinanced_events.iterrows():
            tx_hash = ref_row.get('transactionHash', '')

            # Get all events in this transaction
            tx_events = events_df[events_df['transactionHash'] == tx_hash]

            # Build context
            context = self._build_refinancing_context(ref_row, tx_events)

            if context:
                # Determine fund's role
                context.determine_fund_role(self.fund_wallet_list)

                # Calculate amounts
                context.calculate_amounts()

                contexts[tx_hash] = context

        return contexts

    def _build_refinancing_context(
        self,
        refinanced_row: pd.Series,
        tx_events: pd.DataFrame,
    ) -> Optional[RefinancingContext]:
        """Build RefinancingContext from events in a transaction"""

        tx_hash = refinanced_row.get('transactionHash', '')
        block_number = int(refinanced_row.get('blockNumber', 0))

        # Get transaction datetime
        tx_dt = refinanced_row.get('transaction_datetime')
        if isinstance(tx_dt, str):
            tx_dt = pd.to_datetime(tx_dt, utc=True)
        elif tx_dt is None:
            tx_dt = self._get_block_timestamp(block_number)

        # Create context
        context = RefinancingContext(
            tx_hash=tx_hash,
            block_number=block_number,
            transaction_datetime=tx_dt,
            old_loan_contract=str(refinanced_row.get('oldLoanContract', '')).lower(),
            old_loan_id=int(refinanced_row.get('oldLoanId', 0)),
            new_loan_id=int(refinanced_row.get('newLoanId', 0)),
        )

        # Find LoanRepaid event (old loan closing)
        repaid_events = tx_events[tx_events['event'] == 'LoanRepaid']
        if not repaid_events.empty:
            repaid_row = repaid_events.iloc[0]
            context.loan_repaid_event = self._row_to_decoded_event(repaid_row)
            context.old_lender = str(repaid_row.get('lender', '')).lower()
            context.borrower = str(repaid_row.get('borrower', '')).lower()

        # Find LoanStarted event (new loan opening)
        started_events = tx_events[tx_events['event'] == 'LoanStarted']
        if not started_events.empty:
            started_row = started_events.iloc[0]
            context.loan_started_event = self._row_to_decoded_event(started_row)
            context.new_lender = str(started_row.get('lender', '')).lower()
            context.new_loan_contract = str(started_row.get('contract_address', '')).lower()

            # Borrower should be same, but update just in case
            if not context.borrower:
                context.borrower = str(started_row.get('borrower', '')).lower()

        return context

    def _row_to_decoded_event(self, row: pd.Series) -> DecodedNFTfiEvent:
        """Convert DataFrame row to DecodedNFTfiEvent"""
        tx_dt = row.get('transaction_datetime')
        if isinstance(tx_dt, str):
            tx_dt = pd.to_datetime(tx_dt, utc=True)

        version_str = row.get('version', 'unknown')
        try:
            version = NFTfiVersion(version_str)
        except:
            version = NFTfiVersion.UNKNOWN

        return DecodedNFTfiEvent(
            event_type=str(row.get('event', '')),
            tx_hash=str(row.get('transactionHash', '')),
            block_number=int(row.get('blockNumber', 0)),
            log_index=int(row.get('logIndex', 0)),
            transaction_datetime=tx_dt,
            contract_address=str(row.get('contract_address', '')).lower(),
            version=version,
            loan_id=row.get('loan_id'),
            borrower=str(row.get('borrower', '')).lower() if row.get('borrower') else None,
            lender=str(row.get('lender', '')).lower() if row.get('lender') else None,
            loan_principal_amount=int(row.get('loanPrincipalAmount', 0)) if pd.notna(row.get('loanPrincipalAmount')) else None,
            maximum_repayment_amount=int(row.get('maximumRepaymentAmount', 0)) if pd.notna(row.get('maximumRepaymentAmount')) else None,
            amount_paid_to_lender=int(row.get('amountPaidToLender', 0)) if pd.notna(row.get('amountPaidToLender')) else None,
            admin_fee=int(row.get('adminFee', 0)) if pd.notna(row.get('adminFee')) else None,
            loan_erc20_denomination=str(row.get('loanERC20Denomination', '')).lower() if row.get('loanERC20Denomination') else None,
            cryptocurrency=row.get('cryptocurrency'),
            loan_duration=int(row.get('loanDuration', 0)) if pd.notna(row.get('loanDuration')) else None,
            loan_start_time=int(row.get('loanStartTime', 0)) if pd.notna(row.get('loanStartTime')) else None,
            loan_admin_fee_bps=int(row.get('loanAdminFeeInBasisPoints', 0)) if pd.notna(row.get('loanAdminFeeInBasisPoints')) else None,
            is_pro_rata=bool(row.get('isProRata')) if pd.notna(row.get('isProRata')) else None,
            nft_collateral_contract=str(row.get('nftCollateralContract', '')).lower() if row.get('nftCollateralContract') else None,
            nft_collateral_id=int(row.get('nftCollateralId', 0)) if pd.notna(row.get('nftCollateralId')) else None,
        )

    def is_refinancing_transaction(self, tx_hash: str, events_df: pd.DataFrame) -> bool:
        """Check if a transaction is a refinancing transaction"""
        tx_events = events_df[events_df['transactionHash'] == tx_hash]
        has_refinanced = 'Refinanced' in tx_events['event'].values
        return has_refinanced


# ============================================================================
# ROLLOVER JOURNAL ENTRY GENERATOR
# ============================================================================

class RolloverJournalEntryGenerator:
    """
    Generates journal entries for refinancing rollovers.

    When the same fund wallet is both old and new lender:

    ROLLOVER ACCOUNTING (single balanced entry set):
    1. Close old loan receivable (Cr loan_receivable)
    2. Close accrued interest receivable (Cr interest_receivable)
    3. Open new loan receivable (Dr loan_receivable)
    4. Settle net cash (Dr or Cr deemed_cash based on direction)

    The key insight: We do NOT record gross cash flows.
    We only record NET cash settlement.

    Example:
    - Old loan payoff: 10.5 WETH (10 principal + 0.5 interest)
    - New loan principal: 12 WETH
    - Net: Fund pays out 1.5 WETH (12 - 10.5)

    Journal:
    Dr loan_receivable_weth       12.0   (new loan)
    Cr loan_receivable_weth       10.0   (close old loan principal)
    Cr interest_receivable_weth    0.5   (close old loan interest)
    Cr deemed_cash_usd             1.5   (net cash OUT)
                                  -----
    Debits: 12.0  Credits: 12.0   [ok] Balanced
    """

    def __init__(
        self,
        wallet_metadata: Dict[str, Dict],
    ):
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        self.fund_wallet_list = set(self.wallet_metadata.keys())

    def _get_fund_id(self, address: str) -> str:
        """Get fund_id for an address"""
        if not address:
            return ""
        info = self.wallet_metadata.get(address.lower(), {})
        return info.get('fund_id', '')

    def generate_rollover_entries(
        self,
        context: RefinancingContext,
    ) -> List[Dict]:
        """
        Generate journal entries for a rollover refinancing.

        Args:
            context: RefinancingContext with is_rollover=True

        Returns:
            List of journal entry dicts (balanced)
        """
        if not context.is_rollover:
            raise ValueError("Context is not a rollover - use standard entries")

        entries = []
        amounts = context.get_amounts_in_crypto()

        old_principal = amounts['old_loan_principal']
        old_interest = amounts['old_loan_interest_received']
        new_principal = amounts['new_loan_principal']
        net_cash = amounts['net_cash_flow']  # Positive = fund receives, negative = fund pays

        cryptocurrency = context.cryptocurrency

        # Common metadata
        common = {
            'date': context.transaction_datetime,
            'fund_id': self._get_fund_id(context.old_lender),
            'counterparty_fund_id': self._get_fund_id(context.borrower),
            'wallet_id': context.old_lender,  # Same as new_lender for rollover
            'cryptocurrency': cryptocurrency,
            'transaction_type': 'investments_lending_rollover',
            'platform': PLATFORM,
            'event': 'Rollover',
            'hash': context.tx_hash,
            'loan_id': context.new_loan_id,
            'old_loan_id': context.old_loan_id,
            'lender': context.old_lender,  # Same wallet
            'borrower': context.borrower,
            'from': context.old_lender,
            'to': context.borrower,
            'contract_address': context.new_loan_contract,
            'old_contract_address': context.old_loan_contract,
            'payable_currency': context.loan_erc20_denomination,
            'collateral_address': context.loan_started_event.nft_collateral_contract if context.loan_started_event else '',
            'token_id': context.loan_started_event.nft_collateral_id if context.loan_started_event else None,
            'is_rollover': True,
            'old_loan_principal': old_principal,
            'old_loan_interest_crypto': old_interest,
            'new_loan_principal': new_principal,
            'net_cash_flow_crypto': net_cash,
        }

        # 1. Dr loan_receivable (new loan principal)
        entries.append({
            **common,
            'account_name': f'loan_receivable_cryptocurrency_{cryptocurrency.lower()}',
            'debit': new_principal,
            'credit': Decimal(0),
            'line_description': f'New loan #{context.new_loan_id} principal',
        })

        # 2. Cr loan_receivable (old loan principal)
        entries.append({
            **common,
            'account_name': f'loan_receivable_cryptocurrency_{cryptocurrency.lower()}',
            'debit': Decimal(0),
            'credit': old_principal,
            'line_description': f'Close old loan #{context.old_loan_id} principal',
        })

        # 3. Cr interest_receivable (old loan interest)
        if old_interest > 0:
            entries.append({
                **common,
                'account_name': f'interest_receivable_cryptocurrency_{cryptocurrency.lower()}',
                'debit': Decimal(0),
                'credit': old_interest,
                'line_description': f'Close old loan #{context.old_loan_id} interest',
            })

        # 4. Net cash settlement
        # net_cash > 0: Fund receives net cash (DR deemed_cash)
        # net_cash < 0: Fund pays net cash (CR deemed_cash)
        if net_cash > 0:
            # Fund receives more than it sends (rare in rollover with increased principal)
            entries.append({
                **common,
                'account_name': 'deemed_cash_usd',
                'debit': net_cash,
                'credit': Decimal(0),
                'line_description': f'Net cash IN from rollover',
            })
        elif net_cash < 0:
            # Fund sends more than it receives (typical when increasing principal)
            entries.append({
                **common,
                'account_name': 'deemed_cash_usd',
                'debit': Decimal(0),
                'credit': abs(net_cash),
                'line_description': f'Net cash OUT for rollover',
            })
        # If net_cash == 0, no cash entry needed (perfect rollover)

        # Validate balance
        total_debits = sum(e['debit'] for e in entries)
        total_credits = sum(e['credit'] for e in entries)

        if abs(total_debits - total_credits) > Decimal('0.000000001'):
            print(f"[\!] UNBALANCED ROLLOVER ENTRY for {context.tx_hash}")
            print(f"   Debits: {total_debits}, Credits: {total_credits}")

        return entries

    def generate_non_rollover_refinancing_entries(
        self,
        context: RefinancingContext,
    ) -> List[Dict]:
        """
        Generate journal entries for refinancing where fund is NOT both lenders.

        This handles:
        - OLD_LENDER_ONLY: Fund was old lender, receives repayment
        - NEW_LENDER_ONLY: Fund is new lender, sends new loan
        - BORROWER: Fund is borrower, closes old debt, opens new debt
        """
        entries = []
        amounts = context.get_amounts_in_crypto()
        cryptocurrency = context.cryptocurrency

        # Common metadata base
        common_base = {
            'date': context.transaction_datetime,
            'cryptocurrency': cryptocurrency,
            'platform': PLATFORM,
            'hash': context.tx_hash,
            'borrower': context.borrower,
            'payable_currency': context.loan_erc20_denomination,
            'is_rollover': False,
            'refinanced_from_loan_id': context.old_loan_id if context.refinancing_role == RefinancingRole.NEW_LENDER_ONLY else None,
        }

        if context.refinancing_role == RefinancingRole.OLD_LENDER_ONLY:
            # Fund was old lender - receives repayment (like normal LoanRepaid)
            entries.extend(self._generate_old_lender_entries(context, common_base, amounts))

        elif context.refinancing_role == RefinancingRole.NEW_LENDER_ONLY:
            # Fund is new lender - sends new loan (like normal LoanStarted)
            entries.extend(self._generate_new_lender_entries(context, common_base, amounts))

        elif context.refinancing_role == RefinancingRole.BORROWER:
            # Fund is borrower - refinances debt
            entries.extend(self._generate_borrower_refinancing_entries(context, common_base, amounts))

        return entries

    def _generate_old_lender_entries(
        self,
        context: RefinancingContext,
        common_base: Dict,
        amounts: Dict[str, Decimal],
    ) -> List[Dict]:
        """Generate entries for fund as old lender receiving repayment"""
        entries = []
        cryptocurrency = context.cryptocurrency

        old_principal = amounts['old_loan_principal']
        old_total_received = amounts['old_loan_total_received']
        old_interest = amounts['old_loan_interest_received']

        common = {
            **common_base,
            'fund_id': self._get_fund_id(context.old_lender),
            'counterparty_fund_id': self._get_fund_id(context.borrower),
            'wallet_id': context.old_lender,
            'transaction_type': 'investments_lending',
            'event': 'LoanRepaid_Refinancing',
            'loan_id': context.old_loan_id,
            'lender': context.old_lender,
            'from': context.borrower,
            'to': context.old_lender,
            'contract_address': context.old_loan_contract,
            'principal': old_principal,
        }

        # Dr deemed_cash (total received)
        entries.append({
            **common,
            'account_name': 'deemed_cash_usd',
            'debit': old_total_received,
            'credit': Decimal(0),
        })

        # Cr loan_receivable (principal)
        entries.append({
            **common,
            'account_name': f'loan_receivable_cryptocurrency_{cryptocurrency.lower()}',
            'debit': Decimal(0),
            'credit': old_principal,
        })

        # Cr interest_receivable (interest)
        if old_interest > 0:
            entries.append({
                **common,
                'account_name': f'interest_receivable_cryptocurrency_{cryptocurrency.lower()}',
                'debit': Decimal(0),
                'credit': old_interest,
            })

        return entries

    def _generate_new_lender_entries(
        self,
        context: RefinancingContext,
        common_base: Dict,
        amounts: Dict[str, Decimal],
    ) -> List[Dict]:
        """Generate entries for fund as new lender sending loan"""
        entries = []
        cryptocurrency = context.cryptocurrency

        new_principal = amounts['new_loan_principal']

        common = {
            **common_base,
            'fund_id': self._get_fund_id(context.new_lender),
            'counterparty_fund_id': self._get_fund_id(context.borrower),
            'wallet_id': context.new_lender,
            'transaction_type': 'investments_lending',
            'event': 'LoanStarted_Refinancing',
            'loan_id': context.new_loan_id,
            'lender': context.new_lender,
            'from': context.new_lender,
            'to': context.borrower,
            'contract_address': context.new_loan_contract,
            'principal': new_principal,
        }

        # Dr loan_receivable (principal)
        entries.append({
            **common,
            'account_name': f'loan_receivable_cryptocurrency_{cryptocurrency.lower()}',
            'debit': new_principal,
            'credit': Decimal(0),
        })

        # Cr deemed_cash (principal sent)
        entries.append({
            **common,
            'account_name': 'deemed_cash_usd',
            'debit': Decimal(0),
            'credit': new_principal,
        })

        return entries

    def _generate_borrower_refinancing_entries(
        self,
        context: RefinancingContext,
        common_base: Dict,
        amounts: Dict[str, Decimal],
    ) -> List[Dict]:
        """Generate entries for fund as borrower refinancing debt"""
        entries = []
        cryptocurrency = context.cryptocurrency

        old_principal = amounts['old_loan_principal']
        old_interest = amounts['old_loan_interest_received']
        new_principal = amounts['new_loan_principal']

        # Close old debt
        common_old = {
            **common_base,
            'fund_id': self._get_fund_id(context.borrower),
            'counterparty_fund_id': self._get_fund_id(context.old_lender),
            'wallet_id': context.borrower,
            'transaction_type': 'financing_borrowings',
            'event': 'LoanRepaid_Refinancing',
            'loan_id': context.old_loan_id,
            'lender': context.old_lender,
            'from': context.borrower,
            'to': context.old_lender,
            'contract_address': context.old_loan_contract,
        }

        # Dr note_payable (old principal)
        entries.append({
            **common_old,
            'account_name': f'note_payable_cryptocurrency_{cryptocurrency.lower()}',
            'debit': old_principal,
            'credit': Decimal(0),
        })

        # Dr interest_payable (old interest)
        if old_interest > 0:
            entries.append({
                **common_old,
                'account_name': f'interest_payable_cryptocurrency_{cryptocurrency.lower()}',
                'debit': old_interest,
                'credit': Decimal(0),
            })

        # Open new debt
        common_new = {
            **common_base,
            'fund_id': self._get_fund_id(context.borrower),
            'counterparty_fund_id': self._get_fund_id(context.new_lender),
            'wallet_id': context.borrower,
            'transaction_type': 'financing_borrowings',
            'event': 'LoanStarted_Refinancing',
            'loan_id': context.new_loan_id,
            'lender': context.new_lender,
            'from': context.new_lender,
            'to': context.borrower,
            'contract_address': context.new_loan_contract,
        }

        # Cr note_payable (new principal)
        entries.append({
            **common_new,
            'account_name': f'note_payable_cryptocurrency_{cryptocurrency.lower()}',
            'debit': Decimal(0),
            'credit': new_principal,
        })

        # Net cash settlement
        # If new_principal > old_total_paid: borrower receives net cash
        # If new_principal < old_total_paid: borrower pays net cash
        old_total_paid = old_principal + old_interest
        net_cash = new_principal - old_total_paid

        if net_cash > 0:
            # Borrower receives net cash
            entries.append({
                **common_new,
                'account_name': 'deemed_cash_usd',
                'debit': net_cash,
                'credit': Decimal(0),
            })
        elif net_cash < 0:
            # Borrower pays net cash
            entries.append({
                **common_new,
                'account_name': 'deemed_cash_usd',
                'debit': Decimal(0),
                'credit': abs(net_cash),
            })

        return entries


# ============================================================================
# MAIN PROCESSOR
# ============================================================================

class NFTfiLoanProcessor:
    """
    Main processor for NFTfi transactions.
    Integrates decoding, on-chain queries, and journal entry generation.
    """

    def __init__(
        self,
        w3: Web3,
        wallet_metadata: Dict[str, Dict],
        path_for_JEs: str = None,
        path_for_income_interest_accruals: str = None,
        enable_eth_usd_pricing: bool = True,
        enable_gas_fee_handling: bool = False,
        eth_usd_price_func: Callable = None,
        gas_fee_handler_func: Callable = None,
    ):
        """
        Initialize processor.

        Args:
            w3: Web3 instance
            wallet_metadata: Dict mapping addresses to wallet info
            path_for_JEs: Path to save journal entries
            path_for_income_interest_accruals: Path to interest accruals
            enable_eth_usd_pricing: Toggle for ETH/USD price lookup
            enable_gas_fee_handling: Toggle for gas fee journal entries
            eth_usd_price_func: Function to get ETH/USD prices (eth_usd_df_with_eod)
            gas_fee_handler_func: Function to handle gas fees (handle_gas_fee_direct_and_related_party)
        """
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        # Only include wallets with category == "fund"
        self.fund_wallet_list = [
            addr for addr, info in self.wallet_metadata.items()
            if info.get('category', '').lower() == 'fund'
        ]
        self.path_for_JEs = path_for_JEs
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in wallet_metadata.items()}
        self.fund_wallet_list = list(self.wallet_metadata.keys())
        self.path_for_JEs = path_for_JEs
        self.path_for_income_interest_accruals = path_for_income_interest_accruals

        # Feature toggles
        self.enable_eth_usd_pricing = enable_eth_usd_pricing
        self.enable_gas_fee_handling = enable_gas_fee_handling
        self.eth_usd_price_func = eth_usd_price_func
        self.gas_fee_handler_func = gas_fee_handler_func

        # Initialize components
        self.decoder = NFTfiEventDecoder(w3, wallet_metadata)
        self.journal_generator = NFTfiJournalEntryGenerator(
            w3, wallet_metadata, path_for_income_interest_accruals
        )
        self.query = NFTfiOnChainQuery(w3)

    def process_transactions(
        self,
        tx_hashes: List[str],
        max_workers: int = 8,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process NFTfi transactions end-to-end.

        Returns:
            (decoded_events_df, journal_entries_df)
        """
        print(f"\n{'='*80}")
        print(f"📊 PROCESSING {len(tx_hashes)} NFTfi TRANSACTIONS")
        print(f"{'='*80}")

        # Decode events
        df_events = self.decoder.decode_batch(
            tx_hashes=tx_hashes,
            max_workers=max_workers,
            filter_fund_wallets=True,
        )

        if df_events.empty:
            print("[\!] No NFTfi events found for fund wallets")
            return pd.DataFrame(), pd.DataFrame()

        print(f"[OK] Decoded {len(df_events)} events")

        # Generate journal entries for each event type
        all_journals = []

        # LoanStarted
        j_started = self.journal_generator.generate_loan_started_entries(df_events)
        if not j_started.empty:
            all_journals.append(j_started)
            print(f"   LoanStarted: {len(j_started)} entries")

        # LoanRepaid
        j_repaid = self.journal_generator.generate_loan_repaid_entries(df_events)
        if not j_repaid.empty:
            all_journals.append(j_repaid)
            print(f"   LoanRepaid: {len(j_repaid)} entries")

        # LoanLiquidated (with on-chain interest)
        j_liquidated = self.journal_generator.generate_loan_liquidated_entries(df_events)
        if not j_liquidated.empty:
            all_journals.append(j_liquidated)
            print(f"   LoanLiquidated: {len(j_liquidated)} entries")

        # LoanRenegotiated
        j_reneg = self.journal_generator.generate_loan_renegotiated_entries(df_events)
        if not j_reneg.empty:
            all_journals.append(j_reneg)
            print(f"   LoanRenegotiated: {len(j_reneg)} entries")

        if not all_journals:
            print("[\!] No journal entries generated")
            return df_events, pd.DataFrame()

        journal_df = pd.concat(all_journals, ignore_index=True)

        # Add ETH/USD pricing if enabled
        if self.enable_eth_usd_pricing and self.eth_usd_price_func:
            journal_df = self._add_eth_usd_pricing(journal_df)

        # Add gas fees if enabled
        if self.enable_gas_fee_handling and self.gas_fee_handler_func:
            journal_df = self._add_gas_fees(journal_df, tx_hashes)

        # Validate balance
        self._validate_journal_balance(journal_df)

        print(f"\n[OK] Generated {len(journal_df)} total journal entry lines")

        return df_events, journal_df

    def process_transactions_with_refinancing(
        self,
        events_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Process transactions with special handling for refinancing/rollovers.

        This method:
        1. Identifies refinancing transactions (Refinanced + LoanRepaid + LoanStarted)
        2. Detects rollovers (same wallet is both old and new lender)
        3. Generates net cash settlement entries for rollovers
        4. Processes non-refinancing transactions normally

        Args:
            events_df: DataFrame with decoded NFTfi events

        Returns:
            DataFrame with all journal entries
        """
        print(f"\n{'='*80}")
        print(f"📊 PROCESSING TRANSACTIONS WITH REFINANCING DETECTION")
        print(f"{'='*80}")

        if events_df.empty:
            print("[\!] No events to process")
            return pd.DataFrame()

        # Initialize refinancing processors
        refinancing_processor = RefinancingProcessor(self.w3, self.wallet_metadata)
        rollover_generator = RolloverJournalEntryGenerator(self.wallet_metadata)

        # Step 1: Identify refinancing transactions
        refinancing_contexts = refinancing_processor.identify_refinancing_transactions(events_df)
        refinancing_tx_hashes = set(refinancing_contexts.keys())

        # Categorize refinancing by type
        rollovers = {h: c for h, c in refinancing_contexts.items() if c.is_rollover}
        non_rollovers = {h: c for h, c in refinancing_contexts.items() if not c.is_rollover}

        print(f"   Total transactions: {events_df['transactionHash'].nunique()}")
        print(f"   Refinancing transactions: {len(refinancing_contexts)}")
        print(f"   - Rollovers (same lender): {len(rollovers)}")
        print(f"   - Non-rollovers: {len(non_rollovers)}")

        all_journal_entries = []

        # Step 2: Process ROLLOVER transactions (net cash settlement)
        if rollovers:
            print(f"\n🔄 Processing {len(rollovers)} ROLLOVER transactions...")
            for tx_hash, context in rollovers.items():
                try:
                    entries = rollover_generator.generate_rollover_entries(context)
                    all_journal_entries.extend(entries)

                    amounts = context.get_amounts_in_crypto()
                    net_flow = amounts['net_cash_flow']
                    direction = "IN" if net_flow > 0 else "OUT"
                    print(f"   [OK] Rollover {tx_hash[:10]}... Old #{context.old_loan_id} -> New #{context.new_loan_id} (net: {net_flow:.4f} {context.cryptocurrency} {direction})")
                except Exception as e:
                    print(f"   [\!] Error processing rollover {tx_hash[:10]}...: {e}")

        # Step 3: Process NON-ROLLOVER refinancing (standard entries based on role)
        if non_rollovers:
            print(f"\n[list] Processing {len(non_rollovers)} non-rollover refinancing transactions...")
            for tx_hash, context in non_rollovers.items():
                try:
                    entries = rollover_generator.generate_non_rollover_refinancing_entries(context)
                    all_journal_entries.extend(entries)
                    print(f"   [OK] {context.refinancing_role.value}: {tx_hash[:10]}...")
                except Exception as e:
                    print(f"   [\!] Error processing refinancing {tx_hash[:10]}...: {e}")

        # Step 4: Process STANDARD transactions (non-refinancing)
        standard_events = events_df[~events_df['transactionHash'].isin(refinancing_tx_hashes)]

        if not standard_events.empty:
            print(f"\n[list] Processing {standard_events['transactionHash'].nunique()} standard transactions...")

            # LoanStarted
            j_started = self.journal_generator.generate_loan_started_entries(standard_events)
            if not j_started.empty:
                all_journal_entries.extend(j_started.to_dict('records'))
                print(f"   LoanStarted: {len(j_started)} entries")

            # LoanRepaid
            j_repaid = self.journal_generator.generate_loan_repaid_entries(standard_events)
            if not j_repaid.empty:
                all_journal_entries.extend(j_repaid.to_dict('records'))
                print(f"   LoanRepaid: {len(j_repaid)} entries")

            # LoanLiquidated
            j_liquidated = self.journal_generator.generate_loan_liquidated_entries(standard_events)
            if not j_liquidated.empty:
                all_journal_entries.extend(j_liquidated.to_dict('records'))
                print(f"   LoanLiquidated: {len(j_liquidated)} entries")

            # LoanRenegotiated
            j_reneg = self.journal_generator.generate_loan_renegotiated_entries(standard_events)
            if not j_reneg.empty:
                all_journal_entries.extend(j_reneg.to_dict('records'))
                print(f"   LoanRenegotiated: {len(j_reneg)} entries")

        # Step 5: Combine all entries
        if not all_journal_entries:
            print("\n[\!] No journal entries generated")
            return pd.DataFrame()

        journal_df = pd.DataFrame(all_journal_entries)

        # Step 6: Add ETH/USD pricing if enabled
        if self.enable_eth_usd_pricing and self.eth_usd_price_func:
            journal_df = self._add_eth_usd_pricing(journal_df)

        # Step 7: Validate balance
        is_balanced = self._validate_journal_balance(journal_df)

        print(f"\n[OK] Generated {len(journal_df)} total journal entry lines")
        print(f"   Rollovers processed: {len(rollovers)}")
        print(f"   Standard transactions: {standard_events['transactionHash'].nunique() if not standard_events.empty else 0}")

        return journal_df

    def process_new_loans(
        self,
        tx_hashes: List[str],
        max_workers: int = 8,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Process only LoanStarted events"""
        df_events = self.decoder.decode_batch(
            tx_hashes=tx_hashes,
            relevant_events=['LoanStarted'],
            max_workers=max_workers,
            filter_fund_wallets=True,
        )

        if df_events.empty:
            return pd.DataFrame(), pd.DataFrame()

        journal_df = self.journal_generator.generate_loan_started_entries(df_events)

        if self.enable_eth_usd_pricing and self.eth_usd_price_func:
            journal_df = self._add_eth_usd_pricing(journal_df)

        return df_events, journal_df

    def process_repayments(
        self,
        tx_hashes: List[str],
        max_workers: int = 8,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Process only LoanRepaid events"""
        df_events = self.decoder.decode_batch(
            tx_hashes=tx_hashes,
            relevant_events=['LoanRepaid'],
            max_workers=max_workers,
            filter_fund_wallets=True,
        )

        if df_events.empty:
            return pd.DataFrame(), pd.DataFrame()

        journal_df = self.journal_generator.generate_loan_repaid_entries(df_events)

        if self.enable_eth_usd_pricing and self.eth_usd_price_func:
            journal_df = self._add_eth_usd_pricing(journal_df)

        return df_events, journal_df

    def process_foreclosures(
        self,
        tx_hashes: List[str],
        max_workers: int = 8,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Process only LoanLiquidated events with on-chain interest"""
        df_events = self.decoder.decode_batch(
            tx_hashes=tx_hashes,
            relevant_events=['LoanLiquidated'],
            max_workers=max_workers,
            filter_fund_wallets=True,
        )

        if df_events.empty:
            return pd.DataFrame(), pd.DataFrame()

        journal_df = self.journal_generator.generate_loan_liquidated_entries(df_events)

        if self.enable_eth_usd_pricing and self.eth_usd_price_func:
            journal_df = self._add_eth_usd_pricing(journal_df)

        return df_events, journal_df

    def generate_interest_accruals(
        self,
        df_loans: pd.DataFrame,
    ) -> pd.DataFrame:
        """Generate interest INCOME accruals for loans where fund is LENDER"""
        return self.journal_generator.generate_interest_accruals(df_loans)

    def generate_interest_expense_accruals(
        self,
        df_loans: pd.DataFrame,
    ) -> pd.DataFrame:
        """Generate interest EXPENSE accruals for loans where fund is BORROWER"""
        return self.journal_generator.generate_interest_expense_accruals(df_loans)

    def generate_all_interest_accruals(
        self,
        df_loans: pd.DataFrame,
        current_period: str = None,
    ) -> pd.DataFrame:
        """
        Generate BOTH interest income and expense accruals based on fund role.
        Automatically saves to separate CSV files.

        This is the recommended entry point for interest accrual generation.
        It automatically routes loans to the correct generator based on whether
        the fund is lender (income) or borrower (expense).

        GAAP treatment:
        - Fund as LENDER: Accrues NET interest income (after admin fee)
        - Fund as BORROWER: Accrues GROSS interest expense (full amount owed)

        Args:
            df_loans: DataFrame of LoanStarted events
            current_period: Period string for filename (e.g., "2025_02")

        Returns:
            DataFrame with all interest accrual entries (both income and expense)

        Side Effects:
            Saves income accruals to: {path_for_JEs}/{current_period}_NFTfi_journal_interest_income_accruals.csv
            Saves expense accruals to: {path_for_JEs}/{current_period}_NFTfi_journal_interest_expense_accruals.csv
        """
        if df_loans.empty:
            return pd.DataFrame()

        fund_wallets = set(self.fund_wallet_list)

        # Split loans by fund role
        lender_mask = df_loans['lender'].str.lower().isin(fund_wallets)
        borrower_mask = df_loans['borrower'].str.lower().isin(fund_wallets)

        lender_loans = df_loans[lender_mask].copy()
        borrower_loans = df_loans[borrower_mask].copy()

        # Safety check: warn if same loan has fund as both lender and borrower
        # (This would be an internal fund-to-fund loan - handle separately)
        both_roles = df_loans[lender_mask & borrower_mask]
        if not both_roles.empty:
            print(f"[\!] {len(both_roles)} loans have fund as BOTH lender and borrower (internal loans)")
            # For internal loans, we still generate both sides

        dfs = []
        income_df = pd.DataFrame()
        expense_df = pd.DataFrame()

        # Generate INCOME accruals for loans where fund is LENDER
        if not lender_loans.empty:
            print(f"📊 Generating interest INCOME accruals for {len(lender_loans)} loans (fund as lender)")
            income_df = self.generate_interest_accruals(lender_loans)
            if not income_df.empty:
                dfs.append(income_df)
                print(f"   [OK] Generated {len(income_df)} income accrual entries")

                # Auto-save income accruals
                if self.path_for_JEs:
                    if current_period:
                        income_filepath = f"{self.path_for_JEs}/{current_period}_NFTfi_journal_interest_income_accruals.csv"
                    else:
                        income_filepath = f"{self.path_for_JEs}/NFTfi_journal_interest_income_accruals.csv"
                    income_df.to_csv(income_filepath, index=False)
                    print(f"   💾 Saved income accruals to: {income_filepath}")

        # Generate EXPENSE accruals for loans where fund is BORROWER
        if not borrower_loans.empty:
            print(f"📊 Generating interest EXPENSE accruals for {len(borrower_loans)} loans (fund as borrower)")
            expense_df = self.generate_interest_expense_accruals(borrower_loans)
            if not expense_df.empty:
                dfs.append(expense_df)
                print(f"   [OK] Generated {len(expense_df)} expense accrual entries")

                # Auto-save expense accruals
                if self.path_for_JEs:
                    if current_period:
                        expense_filepath = f"{self.path_for_JEs}/{current_period}_NFTfi_journal_interest_expense_accruals.csv"
                    else:
                        expense_filepath = f"{self.path_for_JEs}/NFTfi_journal_interest_expense_accruals.csv"
                    expense_df.to_csv(expense_filepath, index=False)
                    print(f"   💾 Saved expense accruals to: {expense_filepath}")

        if not dfs:
            print("[\!] No interest accruals generated")
            return pd.DataFrame()

        combined = pd.concat(dfs, ignore_index=True)
        print(f"[OK] Total interest accrual entries: {len(combined)}")

        return combined

    def _add_eth_usd_pricing(self, journal_df: pd.DataFrame) -> pd.DataFrame:
        """Add ETH/USD pricing to journal entries"""
        if journal_df.empty or not self.eth_usd_price_func:
            return journal_df

        try:
            tx_hashes = journal_df['hash'].unique().tolist()
            # Assuming eth_usd_price_func returns df with 'hash' and 'TX_ETH_USD_price' columns
            # This integrates with your existing eth_usd_df_with_eod function
            price_df = self.eth_usd_price_func(tx_hashes, self.w3, None)  # aggregator passed separately

            if not price_df.empty and 'TX_ETH_USD_price' in price_df.columns:
                journal_df = journal_df.merge(
                    price_df[['hash', 'TX_ETH_USD_price']],
                    left_on='hash',
                    right_on='hash',
                    how='left'
                )

                # Calculate USD values
                journal_df['debit_USD'] = journal_df.apply(
                    lambda r: r['debit'] * Decimal(str(r.get('TX_ETH_USD_price', 0) or 0))
                    if r.get('cryptocurrency') == 'WETH' else r['debit'],
                    axis=1
                )
                journal_df['credit_USD'] = journal_df.apply(
                    lambda r: r['credit'] * Decimal(str(r.get('TX_ETH_USD_price', 0) or 0))
                    if r.get('cryptocurrency') == 'WETH' else r['credit'],
                    axis=1
                )
        except Exception as e:
            print(f"[\!] Error adding ETH/USD pricing: {e}")

        return journal_df

    def _add_gas_fees(self, journal_df: pd.DataFrame, tx_hashes: List[str]) -> pd.DataFrame:
        """Add gas fee journal entries"""
        if not self.gas_fee_handler_func:
            return journal_df

        try:
            # This would integrate with your handle_gas_fee_direct_and_related_party function
            # Implementation depends on your specific function signature
            pass
        except Exception as e:
            print(f"[\!] Error adding gas fees: {e}")

        return journal_df

    def _validate_journal_balance(self, df: pd.DataFrame) -> bool:
        """Validate that debits equal credits per transaction"""
        if df.empty:
            return True

        unbalanced = []
        for tx_hash, group in df.groupby('hash'):
            debits = sum(group['debit'])
            credits = sum(group['credit'])

            if abs(debits - credits) > Decimal('0.000000001'):
                unbalanced.append(tx_hash)

        if unbalanced:
            print(f"[\!] {len(unbalanced)} unbalanced entries")
            return False

        print("[OK] All journal entries balance")
        return True

    def save_journal_entries(
        self,
        journal_df: pd.DataFrame,
        filename: str,
        current_period: str = None,
    ):
        """Save journal entries to CSV"""
        if journal_df.empty:
            print("[\!] No journal entries to save")
            return

        if self.path_for_JEs:
            if current_period:
                filepath = f"{self.path_for_JEs}/{current_period}_{filename}"
            else:
                filepath = f"{self.path_for_JEs}/{filename}"

            journal_df.to_csv(filepath, index=False)
            print(f"💾 Saved to: {filepath}")

    def get_standard_column_order(self) -> List[str]:
        """Get standard column order for journal entries"""
        return [
            "date",
            "transaction_type",
            "platform",
            "fund_id",
            "counterparty_fund_id",
            "wallet_id",
            "cryptocurrency",
            "account_name",
            "debit",
            "credit",
            "TX_ETH_USD_price",
            "debit_USD",
            "credit_USD",
            "event",
            "hash",
            "loan_id",
            "lender",
            "borrower",
            "from",
            "to",
            "contract_address",
            "payable_currency",
            "collateral_address",
            "token_id",
            "principal",
            "principal_USD",
            "annual_interest_rate",
            "payoff_amount",
            "payoff_amount_USD",
            "loan_due_date",
            "isProRata",
            "version",
        ]


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def validate_journal_balance(journal_df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate that debits equal credits for each transaction hash.

    Returns:
        (is_balanced, list_of_unbalanced_hashes)
    """
    if journal_df.empty:
        return True, []

    unbalanced = []
    for tx_hash, group in journal_df.groupby('hash'):
        debits = sum(group['debit'])
        credits = sum(group['credit'])

        if abs(debits - credits) > Decimal('0.000000001'):
            unbalanced.append(tx_hash)

    return len(unbalanced) == 0, unbalanced


def filter_fund_transactions(
    df: pd.DataFrame,
    fund_wallet_list: List[str],
    role: str = 'both',
) -> pd.DataFrame:
    """Filter DataFrame for fund wallet transactions"""
    if df.empty:
        return df

    fund_wallets_lower = [w.lower() for w in fund_wallet_list]

    if role == 'lender':
        return df[df['lender'].str.lower().isin(fund_wallets_lower)]
    elif role == 'borrower':
        return df[df['borrower'].str.lower().isin(fund_wallets_lower)]
    else:
        lender_mask = df['lender'].str.lower().isin(fund_wallets_lower)
        borrower_mask = df['borrower'].str.lower().isin(fund_wallets_lower)
        return df[lender_mask | borrower_mask]


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("NFTfi UNIFIED DECODER - MULTI-VERSION SUPPORT")
    print("=" * 80)
    print()
    print("Supported Contracts:")
    print("  V3:")
    for addr, name in NFTFI_V3_CONTRACTS.items():
        print(f"    - {name}: {addr}")
    print("  V2.3:")
    for addr, name in NFTFI_V23_CONTRACTS.items():
        print(f"    - {name}: {addr}")
    print()
    print("Events Handled:")
    print("  - LoanStarted (new loans)")
    print("  - LoanRepaid (repayments)")
    print("  - LoanLiquidated (foreclosures with on-chain interest)")
    print("  - LoanRenegotiated (term modifications)")
    print("  - Refinanced (loan linkage)")
    print()
    print("Key Features:")
    print("  [OK] Multi-version support (V3, V2.3, V2.1, V2)")
    print("  [OK] On-chain interest calculation for foreclosures")
    print("  [OK] Wei-precise daily interest accruals")
    print("  [OK] Pro-rata and Fixed interest support")
    print("  [OK] GAAP-compliant journal entries")
    print("  [OK] Toggles for ETH/USD pricing and gas fees")
    print()
    print("Usage:")
    print("""
    from nftfi_unified_decoder import NFTfiLoanProcessor

    # Initialize processor with toggles
    processor = NFTfiLoanProcessor(
        w3=w3,
        wallet_metadata=wallet_metadata,
        path_for_JEs='/path/to/save',
        enable_eth_usd_pricing=True,      # Toggle ON/OFF
        enable_gas_fee_handling=False,    # Toggle ON/OFF
        eth_usd_price_func=eth_usd_df_with_eod,
        gas_fee_handler_func=handle_gas_fee_direct_and_related_party,
    )

    # Process all events
    df_events, journal_df = processor.process_transactions(tx_hashes)

    # Or process specific event types
    df_events, journal_df = processor.process_new_loans(tx_hashes)
    df_events, journal_df = processor.process_repayments(tx_hashes)
    df_events, journal_df = processor.process_foreclosures(tx_hashes)

    # Generate interest accruals (auto-saves to separate files)
    df_loans = df_events[df_events['event'] == 'LoanStarted'].copy()
    accruals_df = processor.generate_all_interest_accruals(df_loans, current_period="2025_02")
    # This auto-saves:
    #   - 2025_02_NFTfi_journal_interest_income_accruals.csv (lender income)
    #   - 2025_02_NFTfi_journal_interest_expense_accruals.csv (borrower expense)

    # Validate and save journal entries
    is_balanced, unbalanced = validate_journal_balance(journal_df)
    processor.save_journal_entries(journal_df, "journal_NFTfi.csv", "2025_02")
    """)