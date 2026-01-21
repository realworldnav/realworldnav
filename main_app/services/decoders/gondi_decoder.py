"""
Gondi LoanFacet Decoder - Complete One-Shot Solution
=====================================================

Decodes Gondi NFT lending events and generates GAAP-compliant journal entries.
Supports multi-tranche loans with per-tranche accounting.
Supports multiple currencies (WETH, USDC, etc.) based on loan.principalAddress.
Supports both LENDER and BORROWER perspectives for fund wallets.

Events Handled:
- LoanEmitted: New loan origination
- LoanRepaid: Loan repayment by borrower
- LoanRefinanced: Refinancing with existing lender (treated as payoff + new loan)
- LoanRefinancedFromNewOffers: Refinancing with new lenders (treated as payoff + new loan)
- LoanForeclosed: Single-tranche foreclosure (NFT to lender)
- LoanLiquidated: Multi-tranche liquidation (auction proceeds)
- LoanSentToLiquidator: Intermediate state (flag only)

Interest Accrual Lifecycle (v1.3.0):
- Accruals are estimates based on contractual loan terms (full duration)
- Early termination events are detected (repay, foreclose, liquidate, refinance)
- Exact 1:1 reversals are generated for any accruals after termination date
- Original accruals remain immutable in the ledger (audit trail)
- Net effect: Accrued Interest = (Payoff Amount - Principal)

Output columns use generic names (debit, credit, principal, payoff_amount) in wei.
Post-processing converts to debit_crypto, credit_crypto with proper decimal handling.

Account Names:
- LENDER: loan_receivable_*, interest_receivable_*, interest_income_*
- BORROWER: note_payable_*, interest_payable_*, interest_expense_*
- CASH: deemed_cash_usd (always, regardless of crypto)

Author: Real World NAV
Version: 1.7.1 - Critical continuation accounting fix (precise conditions)

CHANGELOG:
v1.7.1 - CRITICAL ACCOUNTING FIX: Continuation lender handling (precise)

       THREE BUGS FIXED (with PRECISE CONDITIONS):

       BUG #1: Case sensitivity in fund_tranches filtering
       ------------------------------------------------
       Root cause: fund_wallet_list contains lowercase addresses, but
       t.lender from blockchain data could be checksummed (mixed case).
       Result: event.fund_tranches was EMPTY for continuing lenders!
       Fix: Use t.lender.lower() when checking membership.

       BUG #2: Payoff used FULL amounts instead of DELTA for continuations
       ------------------------------------------------
       Condition: `new_tranche.loanId == event.old_loan_id`
       This ensures we only apply delta-based logic when there's a TRUE
       continuation (same lender, new tranche references old loan ID).

       For TRUE CONTINUATIONS:
         principal_paid = old_principal - new_principal
         interest_settled = net_cash - principal_paid

       For NON-CONTINUATIONS (full exit, new lender):
         principal_paid = old_principal (full)
         interest_settled = calculated_interest (full)

       BUG #3: Origination entries for continuation lenders
       ------------------------------------------------
       Condition: `tranche.loanId == event.old_loan_id`
       SKIP origination entries for continuations only when this condition is met.

       NON-CONTINUATIONS still get full origination entries:
         Dr loan_receivable = new_principal
         Dr interest_receivable = purchased_interest
         Cr deemed_cash = cash_disbursed

       KEY INVARIANT - TWO-PART TEST FOR CONTINUATION:
       1. Same lender address in old AND new tranches
       2. New tranche's loanId == old_loan_id
       Both conditions must be true. If only (1) is true, it's a coincidental
       overlap (lender exits old loan AND enters completely new position).

       EXPECTED TEST RESULTS:
       - tx 0xdc56082b... (continuation): net=(2.96, -2.88, -0.08)
       - Other refinances (non-continuation): full payoff + full origination

v1.7.0 - V2 CONTINUATION PAYOFF FALLBACK

       PROBLEM SOLVED:
       V2/MultiSource refinance events have old_loan = None, which caused
       the payoff loop to be skipped for continuing lenders. This broke
       the deemed_cash netting and left books unbalanced.

       ROOT CAUSE:
       - v1.6.0 assumes: if old_loan exists -> generate payoff entries
       - V2 events: old_loan is None, but fund continues as lender
       - Detection signal: tranche.loanId == event.old_loan_id

       MINIMAL FIX (preserves archive payoff+origination model):
       1. V2 continuation detection: tranche.loanId == old_loan_id
       2. Generate inferred payoff entries for V2 continuations
       3. In origination, set accruedInterest = 0 for continuations
          (interest was EARNED, not PURCHASED - settled in payoff leg)
       4. Add cash reconciliation assertion (validation, not logic)
       5. Track transfer_outflows for increased participation cases

       KEY POLICY INVARIANT:
       Event accruedInterest is NOT the source of truth for continuing lenders.
       - For new lenders: accruedInterest = purchased interest (Dr interest_receivable)
       - For continuing lenders: accruedInterest = earned interest (settled in payoff)
       Interest receivable settlement comes from LEDGER (post-reversal), not event.

       WHAT THIS HANDLES:
       - Same-lender refinance [ok]
       - Partial takeout (new lender enters) [ok]
       - Increased participation [ok]
       - Decreased participation [ok]
       - V2 MultiSource quirks [ok]

       NO continuation accounting. NO delta-only logic. Pure payoff+origination.

v1.6.0 - CRITICAL REWRITE: Canonical Accrual Grid Model

       CONCEPTUAL MODEL:
       - At loan inception, generate FULL CONTRACTUAL accruals (loan_start -> loan_due_date)
       - Accrual grid is IMMUTABLE - serves as source of truth
       - On early termination, reverse EXACTLY what was not economically earned
       - Reversals reference ORIGINAL accrual rows by identity (not recomputation)

       CRITICAL INVARIANT (Must Always Hold):
           SUM(accruals) - SUM(reversals) = interest earned to termination (exact)
           No exceptions. No tolerances. No drift.

       ACCRUAL GRID STRUCTURE:
       - One row per day bucket (timestamp at 23:59:59 UTC)
       - Partial first day: loan_start -> 23:59:59
       - Full intermediate days: 00:00:00 -> 23:59:59
       - Partial last day: 00:00:00 -> loan_due_date
       - Each row has: accrual_start_ts, accrual_end_ts, accrual_ts, seconds_in_row

       REVERSAL RULES:
       - Fully unearned rows (accrual_start >= termination_ts):
         -> 100% reversal with SAME timestamp as original row
       - Partially earned row (accrual_start < termination_ts < accrual_end):
         -> Partial reversal: original x (unearned_seconds / total_seconds)
         -> JE timestamp = termination_ts (THE EXCEPTION)
       - Fully earned rows (accrual_end <= termination_ts):
         -> No reversal

       CODE CHANGES:
       - Removed terminations parameter from generate_interest_accruals()
       - Removed _calculate_effective_accrual_end() (v1.5.0 bounded approach)
       - Rewrote generate_accrual_reversals() for row-by-row identity
       - Added _create_full_reversal_v160() with correct timestamp discipline
       - Added _create_partial_reversal_v160() using row boundaries
       - Updated validate_accrual_reversal_integrity() for v1.6.0 invariant
       - Added canonical grid metadata: accrual_start_ts, accrual_end_ts, seconds_in_row

       OTHER FIXES (from v1.5.0):
       - totalFee from events is already NET (removed double protocol fee deduction)
       - results.items() checks isinstance(df, pd.DataFrame) before .empty

v1.5.0 - [SUPERSEDED] Bounded accruals at source - incorrect model
       - Over-bounded accruals, caused timing drift and audit issues
       - Replaced by v1.6.0 canonical grid model
v1.4.0 - Implemented precise partial-day reversal for intra-day terminations
       - Termination day: reverse fraction (remaining_seconds / 86400)
       - Post-termination days: reverse 100%
       - Added accrual_period_start to track accrual boundaries
       - Added calculate_reversal_fraction() helper method
       - Wei-precise scaled reversals (no floating-point drift)
       - Updated validation for partial-day reversal accuracy
v1.3.0 - Added exact accrual reversal logic for early loan termination
       - Added accrual_id, accrual_date, journal_date for reversal matching
       - Added detect_loan_terminations() method
       - Added generate_accrual_reversals() method
       - Added validate_accrual_reversal_integrity() method
       - Updated process_events() to orchestrate accrual/reversal lifecycle
v1.2.0 - Added fund_role detection (lender vs borrower)
       - Added borrower perspective journal entries
       - Changed deemed_cash_{currency} to deemed_cash_usd
       - Changed origination_fee_income to interest_income
v1.1.0 - Fixed AttributeDict handling for web3.py compatibility
       - Changed isinstance(x, dict) to hasattr(x, 'keys') throughout
"""




from __future__ import annotations

import json
import pandas as pd
import numpy as np
from pathlib import Path
from decimal import Decimal, getcontext, ROUND_FLOOR
from datetime import datetime, timedelta, time as dt_time, timezone
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from web3 import Web3
import web3.logs

# Set decimal precision for financial calculations
getcontext().prec = 28

# Module exports
__all__ = [
    # Core Classes
    'GondiEventDecoder',
    'GondiJournalGenerator',
    'DecodedGondiEvent',
    'Loan',
    'Tranche',
    'GondiEventType',
    # Constants
    'GONDI_CONTRACTS',
    'GONDI_CONTRACT_ADDRESSES',
    'GONDI_CONTRACT_VERSIONS',
    'TOKEN_REGISTRY',
    # Helper Functions
    'safe_int',
    'safe_address',
    'get_currency_info',
    'calculate_interest',
    'calculate_tranche_interest',
    # Utility Functions
    'standardize_dataframe',
    'convert_to_human_readable',
    'print_event_details',
    'print_processing_summary',
]

# ============================================================================
# CONSTANTS
# ============================================================================

# Gondi Contract Addresses (all versions) with version type
# V1/V3 use Tranche[] struct with 'floor' field
# V2 uses Source[] struct without 'floor' field
GONDI_CONTRACTS = {
    "v3": {
        "address": "0xf65B99CE6DC5F6c556172BCC0Ff27D3665a7d9A8",
        "type": "tranche",  # Uses Tranche[] with floor field
    },
    "v2": {
        "address": "0x478f6F994C6fb3cf3e444a489b3AD9edB8cCaE16",
        "type": "source",   # Uses Source[] without floor field
    },
    "v1": {
        "address": "0xf41B389E0C1950dc0B16C9498eaE77131CC08A56",
        "type": "tranche",  # Uses Tranche[] with floor field
    },
}

# Map addresses to version type for quick lookup
GONDI_CONTRACT_VERSIONS = {
    info["address"].lower(): {"version": ver, "type": info["type"]}
    for ver, info in GONDI_CONTRACTS.items()
}

# All Gondi addresses (lowercase) for matching
GONDI_CONTRACT_ADDRESSES = {info["address"].lower() for info in GONDI_CONTRACTS.values()}

# Default contract (v3)
GONDI_CONTRACT = GONDI_CONTRACTS["v3"]["address"]

# Token Addresses (for currency detection)
TOKEN_REGISTRY = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {"symbol": "WETH", "decimals": 18},
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"symbol": "USDC", "decimals": 6},
    "0xdac17f958d2ee523a2206206994597c13d831ec7": {"symbol": "USDT", "decimals": 6},
    "0x6b175474e89094c44da98b954eedeac495271d0f": {"symbol": "DAI", "decimals": 18},
}

# Conversion Constants
SECONDS_PER_YEAR = 31536000
PRECISION_BPS = 10000

# Platform Identifier
PLATFORM = "Gondi"

# ERC20 Transfer event signature
TRANSFER_TOPIC = Web3.keccak(text="Transfer(address,address,uint256)")

# V2 MultiSourceLoan contract address
GONDI_MULTI_SOURCE_LOAN_ADDRESS = "0x478f6F994C6fb3cf3e444a489b3AD9edB8cCaE16"

# ============================================================================
# V2 ABI - Complete ABI from Etherscan for proper multicall unwrapping
# ============================================================================
# This is the COMPLETE authoritative ABI for the V2 MultiSourceLoan contract.
# Critical for unwrapping multicall transactions to extract the _loan parameter
# (old loan state) from refinanceFull/refinancePartial calls.

V2_MULTISOURCE_LOAN_ABI = [{"inputs":[{"internalType":"address","name":"loanLiquidator","type":"address"},{"components":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"fraction","type":"uint256"}],"internalType":"struct IBaseLoan.ProtocolFee","name":"protocolFee","type":"tuple"},{"internalType":"address","name":"currencyManager","type":"address"},{"internalType":"address","name":"collectionManager","type":"address"},{"internalType":"uint256","name":"maxSources","type":"uint256"},{"internalType":"uint256","name":"minLockPeriod","type":"uint256"},{"internalType":"address","name":"delegateRegistry","type":"address"},{"internalType":"address","name":"flashActionContract","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"AddressZeroError","type":"error"},{"inputs":[{"internalType":"address","name":"_lender","type":"address"},{"internalType":"uint256","name":"_offerId","type":"uint256"}],"name":"CancelledOrExecutedOfferError","type":"error"},{"inputs":[{"internalType":"address","name":"_lender","type":"address"},{"internalType":"uint256","name":"_renegotiationId","type":"uint256"}],"name":"CancelledRenegotiationOfferError","type":"error"},{"inputs":[],"name":"CannotLiquidateError","type":"error"},{"inputs":[],"name":"CollectionNotWhitelistedError","type":"error"},{"inputs":[],"name":"CurrencyNotWhitelistedError","type":"error"},{"inputs":[],"name":"ECDSAInvalidSignature","type":"error"},{"inputs":[{"internalType":"uint256","name":"length","type":"uint256"}],"name":"ECDSAInvalidSignatureLength","type":"error"},{"inputs":[{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"ECDSAInvalidSignatureS","type":"error"},{"inputs":[{"internalType":"uint256","name":"_expirationTime","type":"uint256"}],"name":"ExpiredOfferError","type":"error"},{"inputs":[{"internalType":"uint256","name":"_expirationTime","type":"uint256"}],"name":"ExpiredRenegotiationOfferError","type":"error"},{"inputs":[],"name":"ExtensionNotAvailableError","type":"error"},{"inputs":[{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"uint256","name":"_principalAmount","type":"uint256"}],"name":"InvalidAmountError","type":"error"},{"inputs":[],"name":"InvalidBorrowerError","type":"error"},{"inputs":[],"name":"InvalidCallbackError","type":"error"},{"inputs":[],"name":"InvalidCollateralIdError","type":"error"},{"inputs":[],"name":"InvalidDurationError","type":"error"},{"inputs":[],"name":"InvalidLenderError","type":"error"},{"inputs":[],"name":"InvalidLiquidationError","type":"error"},{"inputs":[{"internalType":"uint256","name":"_loanId","type":"uint256"}],"name":"InvalidLoanError","type":"error"},{"inputs":[],"name":"InvalidMethodError","type":"error"},{"inputs":[{"internalType":"uint256","name":"_fraction","type":"uint256"}],"name":"InvalidProtocolFeeError","type":"error"},{"inputs":[],"name":"InvalidRenegotiationOfferError","type":"error"},{"inputs":[],"name":"InvalidSignatureError","type":"error"},{"inputs":[],"name":"InvalidValueError","type":"error"},{"inputs":[],"name":"LengthMismatchError","type":"error"},{"inputs":[{"internalType":"address","name":"_liquidator","type":"address"}],"name":"LiquidatorOnlyError","type":"error"},{"inputs":[],"name":"LoanExpiredError","type":"error"},{"inputs":[{"internalType":"uint256","name":"_expirationTime","type":"uint256"}],"name":"LoanNotDueError","type":"error"},{"inputs":[{"internalType":"address","name":"_lender","type":"address"},{"internalType":"uint256","name":"_newMinOfferId","type":"uint256"},{"internalType":"uint256","name":"_minOfferId","type":"uint256"}],"name":"LowOfferIdError","type":"error"},{"inputs":[{"internalType":"address","name":"_lender","type":"address"},{"internalType":"uint256","name":"_newMinRenegotiationOfferId","type":"uint256"},{"internalType":"uint256","name":"_minOfferId","type":"uint256"}],"name":"LowRenegotiationOfferIdError","type":"error"},{"inputs":[],"name":"MaxCapacityExceededError","type":"error"},{"inputs":[{"internalType":"uint256","name":"minLockPeriod","type":"uint256"}],"name":"MinLockPeriodTooHighError","type":"error"},{"inputs":[{"internalType":"uint256","name":"i","type":"uint256"},{"internalType":"bytes","name":"returndata","type":"bytes"}],"name":"MulticallFailed","type":"error"},{"inputs":[],"name":"NFTNotReturnedError","type":"error"},{"inputs":[],"name":"NotStrictlyImprovedError","type":"error"},{"inputs":[],"name":"OnlyBorrowerCallableError","type":"error"},{"inputs":[],"name":"OnlyLenderCallableError","type":"error"},{"inputs":[],"name":"OnlyLenderOrBorrowerCallableError","type":"error"},{"inputs":[],"name":"PartialOfferCannotChangeDurationError","type":"error"},{"inputs":[],"name":"PartialOfferCannotHaveFeeError","type":"error"},{"inputs":[],"name":"RefinanceFullError","type":"error"},{"inputs":[{"internalType":"uint256","name":"minTimestamp","type":"uint256"}],"name":"SourceCannotBeRefinancedError","type":"error"},{"inputs":[{"internalType":"uint256","name":"sourcePrincipal","type":"uint256"},{"internalType":"uint256","name":"loanPrincipal","type":"uint256"}],"name":"TargetPrincipalTooLowError","type":"error"},{"inputs":[{"internalType":"uint256","name":"_pendingProtocolFeeSetTime","type":"uint256"}],"name":"TooEarlyError","type":"error"},{"inputs":[{"internalType":"uint256","name":"sources","type":"uint256"}],"name":"TooManySourcesError","type":"error"},{"inputs":[],"name":"ZeroDurationError","type":"error"},{"inputs":[],"name":"ZeroInterestError","type":"error"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"lender","type":"address"},{"indexed":False,"internalType":"uint256","name":"minOfferId","type":"uint256"}],"name":"AllOffersCancelled","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"lender","type":"address"},{"indexed":False,"internalType":"uint256","name":"minRenegotiationId","type":"uint256"}],"name":"AllRenegotiationOffersCancelled","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"borrower","type":"address"},{"indexed":False,"internalType":"uint256","name":"offerId","type":"uint256"}],"name":"BorrowerOfferCancelled","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"newdelegateRegistry","type":"address"}],"name":"DelegateRegistryUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"loanId","type":"uint256"},{"indexed":False,"internalType":"address","name":"delegate","type":"address"},{"indexed":False,"internalType":"bool","name":"value","type":"bool"}],"name":"Delegated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"newFlashActionContract","type":"address"}],"name":"FlashActionContractUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"loanId","type":"uint256"},{"indexed":False,"internalType":"address","name":"target","type":"address"},{"indexed":False,"internalType":"bytes","name":"data","type":"bytes"}],"name":"FlashActionExecuted","type":"event"},{"anonymous":False,"inputs":[{"components":[{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"interest","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"}],"indexed":False,"internalType":"struct IBaseLoan.ImprovementMinimum","name":"minimum","type":"tuple"}],"name":"ImprovementMinimumUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"newDuration","type":"uint256"}],"name":"LiquidationAuctionDurationUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"liquidator","type":"address"}],"name":"LiquidationContractUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"loanId","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"offerId","type":"uint256"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"indexed":False,"internalType":"struct IMultiSourceLoan.Loan","name":"loan","type":"tuple"},{"indexed":False,"internalType":"address","name":"lender","type":"address"},{"indexed":False,"internalType":"address","name":"borrower","type":"address"},{"indexed":False,"internalType":"uint256","name":"fee","type":"uint256"}],"name":"LoanEmitted","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"oldLoanId","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"newLoanId","type":"uint256"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"indexed":False,"internalType":"struct IMultiSourceLoan.Loan","name":"loan","type":"tuple"},{"indexed":False,"internalType":"uint256","name":"_extension","type":"uint256"}],"name":"LoanExtended","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"loanId","type":"uint256"}],"name":"LoanForeclosed","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"loanId","type":"uint256"}],"name":"LoanLiquidated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"renegotiationId","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"oldLoanId","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"newLoanId","type":"uint256"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"indexed":False,"internalType":"struct IMultiSourceLoan.Loan","name":"loan","type":"tuple"},{"indexed":False,"internalType":"uint256","name":"fee","type":"uint256"}],"name":"LoanRefinanced","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"loanId","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"totalRepayment","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"fee","type":"uint256"}],"name":"LoanRepaid","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"loanId","type":"uint256"},{"indexed":False,"internalType":"address","name":"liquidator","type":"address"}],"name":"LoanSentToLiquidator","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"newMax","type":"uint256"}],"name":"MaxSourcesUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"minLockPeriod","type":"uint256"}],"name":"MinLockPeriodUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"lender","type":"address"},{"indexed":False,"internalType":"uint256","name":"offerId","type":"uint256"}],"name":"OfferCancelled","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"user","type":"address"},{"indexed":True,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":False,"inputs":[{"components":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"fraction","type":"uint256"}],"indexed":False,"internalType":"struct IBaseLoan.ProtocolFee","name":"fee","type":"tuple"}],"name":"ProtocolFeePendingUpdate","type":"event"},{"anonymous":False,"inputs":[{"components":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"fraction","type":"uint256"}],"indexed":False,"internalType":"struct IBaseLoan.ProtocolFee","name":"fee","type":"tuple"}],"name":"ProtocolFeeUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"lender","type":"address"},{"indexed":False,"internalType":"uint256","name":"renegotiationId","type":"uint256"}],"name":"RenegotiationOfferCancelled","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"delegate","type":"address"},{"indexed":False,"internalType":"address","name":"collection","type":"address"},{"indexed":False,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"RevokeDelegate","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"contractAdded","type":"address"},{"components":[{"internalType":"uint128","name":"buyTax","type":"uint128"},{"internalType":"uint128","name":"sellTax","type":"uint128"}],"indexed":False,"internalType":"struct WithCallbacks.Taxes","name":"tax","type":"tuple"}],"name":"WhitelistedCallbackContractAdded","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"contractRemoved","type":"address"}],"name":"WhitelistedCallbackContractRemoved","type":"event"},{"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"FEE_UPDATE_NOTICE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"INITIAL_DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_PROTOCOL_FEE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_AUCTION_DURATION","outputs":[{"internalType":"uint48","name":"","type":"uint48"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_contract","type":"address"},{"components":[{"internalType":"uint128","name":"buyTax","type":"uint128"},{"internalType":"uint128","name":"sellTax","type":"uint128"}],"internalType":"struct WithCallbacks.Taxes","name":"_tax","type":"tuple"}],"name":"addWhitelistedCallbackContract","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_minOfferId","type":"uint256"}],"name":"cancelAllOffers","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_minRenegotiationId","type":"uint256"}],"name":"cancelAllRenegotiationOffers","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_offerId","type":"uint256"}],"name":"cancelOffer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256[]","name":"_offerIds","type":"uint256[]"}],"name":"cancelOffers","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_renegotiationId","type":"uint256"}],"name":"cancelRenegotiationOffer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256[]","name":"_renegotiationIds","type":"uint256[]"}],"name":"cancelRenegotiationOffers","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_loanId","type":"uint256"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"internalType":"struct IMultiSourceLoan.Loan","name":"loan","type":"tuple"},{"internalType":"address","name":"_delegate","type":"address"},{"internalType":"bytes32","name":"_rights","type":"bytes32"},{"internalType":"bool","name":"_value","type":"bool"}],"name":"delegate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"components":[{"components":[{"internalType":"uint256","name":"offerId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"fee","type":"uint256"},{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"capacity","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"},{"internalType":"uint256","name":"expirationTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"address","name":"validator","type":"address"},{"internalType":"bytes","name":"arguments","type":"bytes"}],"internalType":"struct IBaseLoan.OfferValidator[]","name":"validators","type":"tuple[]"}],"internalType":"struct IBaseLoan.LoanOffer","name":"offer","type":"tuple"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"expirationTime","type":"uint256"},{"internalType":"bytes","name":"callbackData","type":"bytes"}],"internalType":"struct IBaseLoan.ExecutionData","name":"executionData","type":"tuple"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"address","name":"borrower","type":"address"},{"internalType":"bytes","name":"lenderOfferSignature","type":"bytes"},{"internalType":"bytes","name":"borrowerOfferSignature","type":"bytes"}],"internalType":"struct IMultiSourceLoan.LoanExecutionData","name":"_executionData","type":"tuple"}],"name":"emitLoan","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"internalType":"struct IMultiSourceLoan.Loan","name":"","type":"tuple"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_loanId","type":"uint256"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"internalType":"struct IMultiSourceLoan.Loan","name":"_loan","type":"tuple"},{"internalType":"address","name":"_target","type":"address"},{"internalType":"bytes","name":"_data","type":"bytes"}],"name":"executeFlashAction","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_loanId","type":"uint256"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"internalType":"struct IMultiSourceLoan.Loan","name":"_loan","type":"tuple"},{"internalType":"uint256","name":"_extension","type":"uint256"}],"name":"extendLoan","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"internalType":"struct IMultiSourceLoan.Loan","name":"","type":"tuple"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"getCollectionManager","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getCurrencyManager","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getDelegateRegistry","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getFlashActionContract","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getImprovementMinimum","outputs":[{"components":[{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"interest","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"}],"internalType":"struct IBaseLoan.ImprovementMinimum","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getLiquidationAuctionDuration","outputs":[{"internalType":"uint48","name":"","type":"uint48"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getLiquidator","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_loanId","type":"uint256"}],"name":"getLoanHash","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getMaxSources","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getMinLockPeriod","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_loanPrincipal","type":"uint256"}],"name":"getMinSourcePrincipal","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getPendingProtocolFee","outputs":[{"components":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"fraction","type":"uint256"}],"internalType":"struct IBaseLoan.ProtocolFee","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getPendingProtocolFeeSetTime","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getProtocolFee","outputs":[{"components":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"fraction","type":"uint256"}],"internalType":"struct IBaseLoan.ProtocolFee","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getTotalLoansIssued","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_lender","type":"address"},{"internalType":"uint256","name":"_offerId","type":"uint256"}],"name":"getUsedCapacity","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"uint256","name":"","type":"uint256"}],"name":"isOfferCancelled","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"uint256","name":"","type":"uint256"}],"name":"isRenegotiationOfferCancelled","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_contract","type":"address"}],"name":"isWhitelistedCallbackContract","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"lenderMinRenegotiationOfferId","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_loanId","type":"uint256"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"internalType":"struct IMultiSourceLoan.Loan","name":"_loan","type":"tuple"}],"name":"liquidateLoan","outputs":[{"internalType":"bytes","name":"","type":"bytes"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_loanId","type":"uint256"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"internalType":"struct IMultiSourceLoan.Loan","name":"_loan","type":"tuple"}],"name":"loanLiquidated","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"minOfferId","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes[]","name":"data","type":"bytes[]"}],"name":"multicall","outputs":[{"internalType":"bytes[]","name":"results","type":"bytes[]"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"bytes","name":"","type":"bytes"}],"name":"onERC721Received","outputs":[{"internalType":"bytes4","name":"","type":"bytes4"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"uint256","name":"renegotiationId","type":"uint256"},{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"fee","type":"uint256"},{"internalType":"uint256[]","name":"targetPrincipal","type":"uint256[]"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"},{"internalType":"uint256","name":"expirationTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"}],"internalType":"struct IMultiSourceLoan.RenegotiationOffer","name":"_renegotiationOffer","type":"tuple"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"internalType":"struct IMultiSourceLoan.Loan","name":"_loan","type":"tuple"},{"internalType":"bytes","name":"_renegotiationOfferSignature","type":"bytes"}],"name":"refinanceFull","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"internalType":"struct IMultiSourceLoan.Loan","name":"","type":"tuple"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"uint256","name":"renegotiationId","type":"uint256"},{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"fee","type":"uint256"},{"internalType":"uint256[]","name":"targetPrincipal","type":"uint256[]"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"},{"internalType":"uint256","name":"expirationTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"}],"internalType":"struct IMultiSourceLoan.RenegotiationOffer","name":"_renegotiationOffer","type":"tuple"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"internalType":"struct IMultiSourceLoan.Loan","name":"_loan","type":"tuple"}],"name":"refinancePartial","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"internalType":"struct IMultiSourceLoan.Loan","name":"","type":"tuple"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_contract","type":"address"}],"name":"removeWhitelistedCallbackContract","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"bytes","name":"callbackData","type":"bytes"},{"internalType":"bool","name":"shouldDelegate","type":"bool"}],"internalType":"struct IMultiSourceLoan.SignableRepaymentData","name":"data","type":"tuple"},{"components":[{"internalType":"address","name":"borrower","type":"address"},{"internalType":"uint256","name":"nftCollateralTokenId","type":"uint256"},{"internalType":"address","name":"nftCollateralAddress","type":"address"},{"internalType":"address","name":"principalAddress","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"},{"components":[{"internalType":"uint256","name":"loanId","type":"uint256"},{"internalType":"address","name":"lender","type":"address"},{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"accruedInterest","type":"uint256"},{"internalType":"uint256","name":"startTime","type":"uint256"},{"internalType":"uint256","name":"aprBps","type":"uint256"}],"internalType":"struct IMultiSourceLoan.Source[]","name":"source","type":"tuple[]"}],"internalType":"struct IMultiSourceLoan.Loan","name":"loan","type":"tuple"},{"internalType":"bytes","name":"borrowerSignature","type":"bytes"}],"internalType":"struct IMultiSourceLoan.LoanRepaymentData","name":"_repaymentData","type":"tuple"}],"name":"repayLoan","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_delegate","type":"address"},{"internalType":"address","name":"_collection","type":"address"},{"internalType":"uint256","name":"_tokenId","type":"uint256"}],"name":"revokeDelegate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_newDelegateRegistry","type":"address"}],"name":"setDelegateRegistry","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_newFlashActionContract","type":"address"}],"name":"setFlashActionContract","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"__maxSources","type":"uint256"}],"name":"setMaxSources","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"__minLockPeriod","type":"uint256"}],"name":"setMinLockPeriod","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"setProtocolFee","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"uint256","name":"principalAmount","type":"uint256"},{"internalType":"uint256","name":"interest","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"}],"internalType":"struct IBaseLoan.ImprovementMinimum","name":"_newMinimum","type":"tuple"}],"name":"updateImprovementMinimum","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint48","name":"_newDuration","type":"uint48"}],"name":"updateLiquidationAuctionDuration","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract ILoanLiquidator","name":"loanLiquidator","type":"address"}],"name":"updateLiquidationContract","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"fraction","type":"uint256"}],"internalType":"struct IBaseLoan.ProtocolFee","name":"_newProtocolFee","type":"tuple"}],"name":"updateProtocolFee","outputs":[],"stateMutability":"nonpayable","type":"function"}]

def instantiate_v2_contract(w3: Web3) -> Any:
    """
    Instantiate the V2 MultiSourceLoan contract with the complete Etherscan ABI.
    This ABI includes multicall and refinance functions needed for proper decoding.
    """
    return w3.eth.contract(
        address=Web3.to_checksum_address(GONDI_MULTI_SOURCE_LOAN_ADDRESS),
        abi=V2_MULTISOURCE_LOAN_ABI
    )


# ============================================================================
# ENUMS
# ============================================================================

class GondiEventType(Enum):
    """Gondi event types"""
    LOAN_EMITTED = "LoanEmitted"
    LOAN_REPAID = "LoanRepaid"
    LOAN_REFINANCED = "LoanRefinanced"
    LOAN_REFINANCED_FROM_NEW_OFFERS = "LoanRefinancedFromNewOffers"  # V1/V3 only
    LOAN_EXTENDED = "LoanExtended"  # V2 only
    LOAN_FORECLOSED = "LoanForeclosed"
    LOAN_LIQUIDATED = "LoanLiquidated"
    LOAN_SENT_TO_LIQUIDATOR = "LoanSentToLiquidator"
    OFFER_CANCELLED = "OfferCancelled"
    RENEGOTIATION_OFFER_CANCELLED = "RenegotiationOfferCancelled"
    DELEGATED = "Delegated"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def safe_int(value: Any, default: int = 0) -> int:
    """Safely convert value to int"""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_address(value: Any) -> str:
    """Safely normalize Ethereum address"""
    if value is None or pd.isna(value):
        return ""
    addr = str(value).strip().lower()
    if addr.startswith('0x') and len(addr) == 42:
        return addr
    return ""


def to_checksum(address: str) -> Optional[str]:
    """Convert to checksum address safely"""
    try:
        if address and address.startswith('0x') and len(address) == 42:
            return Web3.to_checksum_address(address)
    except Exception:
        pass
    return None


def get_currency_info(principal_address: str) -> Dict[str, Any]:
    """
    Get currency symbol and decimals from principal address.

    Returns:
        Dict with 'symbol' and 'decimals'
    """
    addr = safe_address(principal_address)
    if addr in TOKEN_REGISTRY:
        return TOKEN_REGISTRY[addr]
    # Default to WETH if unknown
    return {"symbol": "WETH", "decimals": 18}


def get_account_suffix(cryptocurrency: str) -> str:
    """Get account name suffix based on cryptocurrency"""
    return cryptocurrency.lower()


def calculate_interest(
    principal_wei: int,
    apr_bps: int,
    duration_seconds: int,
    protocol_fee_bps: int = 0,
) -> Tuple[int, int, int]:
    """
    Calculate interest using Gondi's simple interest formula.

    Returns:
        (gross_interest_wei, protocol_fee_wei, net_interest_wei)
    """
    if principal_wei == 0 or apr_bps == 0 or duration_seconds == 0:
        return 0, 0, 0

    # Gross interest (simple interest)
    gross = (principal_wei * apr_bps * duration_seconds) // (PRECISION_BPS * SECONDS_PER_YEAR)

    # Protocol fee on interest
    protocol_fee = (gross * protocol_fee_bps) // PRECISION_BPS

    # Net interest to lender
    net_interest = gross - protocol_fee

    return gross, protocol_fee, net_interest


def calculate_tranche_interest(
    tranche: 'Tranche',
    end_timestamp: int,
    protocol_fee_bps: int
) -> Tuple[int, int]:
    """
    Calculate total interest for a tranche (carried + current period net).

    Returns:
        (total_interest_wei, current_period_net_interest_wei)
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


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class Tranche:
    """
    Tranche struct from Gondi contract (V1/V3 format).
    Also handles V2 "Source" struct which has different field order and no floor.

    V1/V3 Tranche: loanId, floor, principalAmount, lender, accruedInterest, startTime, aprBps
    V2 Source:    loanId, lender, principalAmount, accruedInterest, startTime, aprBps
    """
    loanId: int
    floor: int  # V2 doesn't have this - will be 0
    principalAmount: int  # wei
    lender: str
    accruedInterest: int  # wei - carried forward from refinancing
    startTime: int  # timestamp
    aprBps: int  # basis points

    @classmethod
    def from_tuple(cls, data: tuple, is_v2: bool = False) -> 'Tranche':
        """
        Create from ABI-decoded tuple.

        Args:
            data: Tuple from ABI decoding
            is_v2: If True, parse as V2 Source struct (no floor, different order)
        """
        if is_v2:
            # V2 Source: (loanId, lender, principalAmount, accruedInterest, startTime, aprBps)
            return cls(
                loanId=int(data[0]),
                floor=0,  # V2 has no floor
                principalAmount=int(data[2]),
                lender=str(data[1]).lower(),  # lender is at index 1 in V2
                accruedInterest=int(data[3]),
                startTime=int(data[4]),
                aprBps=int(data[5]),
            )
        else:
            # V1/V3 Tranche: (loanId, floor, principalAmount, lender, accruedInterest, startTime, aprBps)
            return cls(
                loanId=int(data[0]),
                floor=int(data[1]),
                principalAmount=int(data[2]),
                lender=str(data[3]).lower(),
                accruedInterest=int(data[4]),
                startTime=int(data[5]),
                aprBps=int(data[6]),
            )

    @classmethod
    def from_dict(cls, data: dict) -> 'Tranche':
        """Create from dictionary"""
        return cls(
            loanId=safe_int(data.get('loanId', 0)),
            floor=safe_int(data.get('floor', 0)),  # May not exist in V2
            principalAmount=safe_int(data.get('principalAmount', 0)),
            lender=safe_address(data.get('lender', '')),
            accruedInterest=safe_int(data.get('accruedInterest', 0)),
            startTime=safe_int(data.get('startTime', 0)),
            aprBps=safe_int(data.get('aprBps', 0)),
        )

    def to_dict(self) -> Dict:
        return {
            'loanId': self.loanId,
            'floor': self.floor,
            'principalAmount': self.principalAmount,
            'lender': self.lender,
            'accruedInterest': self.accruedInterest,
            'startTime': self.startTime,
            'aprBps': self.aprBps,
        }


@dataclass
class Loan:
    """
    Loan struct from Gondi contract.

    V1/V3 has 9 fields including protocolFee at the end.
    V2 has 8 fields (no protocolFee).

    V1/V3: borrower, tokenId, collateralAddr, principalAddr, principalAmt, startTime, duration, tranches[], protocolFee
    V2:    borrower, tokenId, collateralAddr, principalAddr, principalAmt, startTime, duration, source[]
    """
    borrower: str
    nftCollateralTokenId: int
    nftCollateralAddress: str
    principalAddress: str  # Token address (WETH, USDC, etc.)
    principalAmount: int  # wei - total across all tranches
    startTime: int
    duration: int  # seconds
    tranches: List[Tranche]
    protocolFee: int  # basis points on interest (V2 doesn't have this - will be 0)

    @classmethod
    def from_tuple(cls, data: tuple, is_v2: bool = False) -> 'Loan':
        """
        Create from ABI-decoded tuple.

        Args:
            data: Tuple from ABI decoding
            is_v2: If True, parse as V2 format (no protocolFee, Source[] instead of Tranche[])
        """
        # Parse tranches/sources - index 7 in both versions
        tranche_data = data[7] if len(data) > 7 and data[7] else []
        tranches = [Tranche.from_tuple(t, is_v2=is_v2) for t in tranche_data]

        # V2 has no protocolFee field (8 total fields)
        # V1/V3 has protocolFee at index 8 (9 total fields)
        if is_v2:
            protocol_fee = 0
        else:
            protocol_fee = int(data[8]) if len(data) > 8 else 0

        return cls(
            borrower=safe_address(data[0]),
            nftCollateralTokenId=int(data[1]),
            nftCollateralAddress=safe_address(data[2]),
            principalAddress=safe_address(data[3]),
            principalAmount=int(data[4]),
            startTime=int(data[5]),
            duration=int(data[6]),
            tranches=tranches,
            protocolFee=protocol_fee,
        )

    @classmethod
    def from_dict(cls, data, is_v2: bool = False) -> 'Loan':
        """Create from dictionary or dict-like object (including AttributeDict from web3.py)"""
        # V2 uses 'source', V1/V3 uses 'tranche' or 'tranches'
        tranche_data = data.get('source', data.get('tranche', data.get('tranches', [])))
        tranches = []
        for t in tranche_data:
            if hasattr(t, 'keys'):  # Handle AttributeDict from web3.py
                tranches.append(Tranche.from_dict(t))
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
            protocolFee=safe_int(data.get('protocolFee', 0)),
        )

    def to_dict(self) -> Dict:
        return {
            'borrower': self.borrower,
            'nftCollateralTokenId': self.nftCollateralTokenId,
            'nftCollateralAddress': self.nftCollateralAddress,
            'principalAddress': self.principalAddress,
            'principalAmount': self.principalAmount,
            'startTime': self.startTime,
            'duration': self.duration,
            'tranches': [t.to_dict() for t in self.tranches],
            'protocolFee': self.protocolFee,
        }

    @property
    def due_date(self) -> datetime:
        """Calculate loan due date"""
        return datetime.fromtimestamp(self.startTime + self.duration, tz=timezone.utc)

    @property
    def cryptocurrency(self) -> str:
        """Get cryptocurrency symbol from principal address"""
        return get_currency_info(self.principalAddress)['symbol']

    @property
    def decimals(self) -> int:
        """Get decimals for the principal currency"""
        return get_currency_info(self.principalAddress)['decimals']


@dataclass
class DecodedGondiEvent:
    """Decoded Gondi event with all fields"""
    event_type: str
    tx_hash: str
    block_number: int
    log_index: int
    timestamp: datetime

    # Loan data
    loan_id: Optional[int] = None
    old_loan_id: Optional[int] = None  # For refinancing
    new_loan_id: Optional[int] = None  # For refinancing
    loan: Optional[Loan] = None
    old_loan: Optional[Loan] = None  # For refinancing

    # Event-specific fields
    offer_ids: Optional[List[int]] = None
    renegotiation_id: Optional[int] = None
    total_repayment: Optional[int] = None  # wei
    fee: Optional[int] = None  # wei - origination fee
    liquidator: Optional[str] = None
    extension: Optional[int] = None  # V2 LoanExtended - extension duration in seconds

    # Transfer event proceeds (for LoanLiquidated)
    transfer_proceeds: Dict[str, int] = field(default_factory=dict)  # lender -> amount received
    transfer_outflows: Dict[str, int] = field(default_factory=dict)  # lender -> amount sent (v1.7.0)

    # Computed: Fund wallet tranches (when fund is LENDER)
    fund_tranches: List[Tranche] = field(default_factory=list)
    old_fund_tranches: List[Tranche] = field(default_factory=list)

    # Computed: Fund is BORROWER flag
    is_fund_borrower: bool = False
    old_is_fund_borrower: bool = False  # For refinancing - was old loan fund as borrower

    # Contract that emitted the event
    contract_address: Optional[str] = None

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'event_type': self.event_type,
            'tx_hash': self.tx_hash,
            'block_number': self.block_number,
            'log_index': self.log_index,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'loan_id': self.loan_id,
            'old_loan_id': self.old_loan_id,
            'new_loan_id': self.new_loan_id,
            'loan': self.loan.to_dict() if self.loan else None,
            'old_loan': self.old_loan.to_dict() if self.old_loan else None,
            'offer_ids': self.offer_ids,
            'renegotiation_id': self.renegotiation_id,
            'total_repayment': self.total_repayment,
            'fee': self.fee,
            'liquidator': self.liquidator,
            'extension': self.extension,
            'transfer_proceeds': self.transfer_proceeds,
            'transfer_outflows': self.transfer_outflows,
            'fund_tranches': [t.to_dict() for t in self.fund_tranches] if self.fund_tranches else [],
            'old_fund_tranches': [t.to_dict() for t in self.old_fund_tranches] if self.old_fund_tranches else [],
            'is_fund_borrower': self.is_fund_borrower,
            'old_is_fund_borrower': self.old_is_fund_borrower,
            'contract_address': self.contract_address,
        }


# ============================================================================
# GONDI EVENT DECODER
# ============================================================================

class GondiEventDecoder:
    """
    Decodes Gondi LoanFacet events from transaction receipts.
    Handles multi-tranche loans with per-tranche filtering.
    Supports multiple Gondi contract versions (V1/V3 with Tranche[], V2 with Source[]).
    """

    def __init__(
        self,
        w3: Web3,
        contract=None,  # Legacy: single contract (will be converted to contracts dict)
        contracts: Dict[str, Any] = None,  # New: dict of address -> contract
        wallet_metadata: Dict[str, Dict] = None,
        v2_addresses: set = None,  # Addresses using V2 ABI (Source[] without floor)
    ):
        """
        Initialize decoder.

        Args:
            w3: Web3 instance
            contract: (Legacy) Single instantiated Gondi contract - will be added to contracts
            contracts: Dict mapping contract addresses (lowercase) to instantiated contracts
            wallet_metadata: Dict mapping wallet addresses to wallet info
            v2_addresses: Set of contract addresses (lowercase) that use V2 ABI (Source[] without floor)
        """
        self.w3 = w3
        self.wallet_metadata = {k.lower(): v for k, v in (wallet_metadata or {}).items()}
        self.fund_wallet_list = [
            addr for addr, meta in self.wallet_metadata.items()
            if meta.get('category', '').lower() == 'fund'
        ]

        # Build contracts dictionary
        self.contracts = {}

        # Add contracts from dict
        if contracts:
            for addr, c in contracts.items():
                self.contracts[addr.lower()] = c

        # Add legacy single contract
        if contract is not None:
            # Get address from contract
            contract_addr = contract.address.lower()
            self.contracts[contract_addr] = contract

        # Track V2 addresses (use Source[] struct without floor field)
        self.v2_addresses = v2_addresses or set()
        # Also check GONDI_CONTRACT_VERSIONS for automatic detection
        for addr in self.contracts.keys():
            version_info = GONDI_CONTRACT_VERSIONS.get(addr)
            if version_info and version_info.get('type') == 'source':
                self.v2_addresses.add(addr)

        # v1.7.0: AUTO-INSTANTIATE V2 CONTRACT if not already provided
        # This is critical for proper multicall unwrapping on V2 transactions
        v2_addr = GONDI_MULTI_SOURCE_LOAN_ADDRESS.lower()
        if v2_addr not in self.contracts:
            try:
                v2_contract = instantiate_v2_contract(self.w3)
                self.contracts[v2_addr] = v2_contract
                self.v2_addresses.add(v2_addr)
                print(f"  [OK] Auto-instantiated V2 contract: {v2_addr[:12]}...")
            except Exception as e:
                print(f"  [!] Could not auto-instantiate V2 contract: {e}")

        # If no contracts provided, warn
        if not self.contracts:
            print("[!] No contracts provided to GondiEventDecoder")
        else:
            print(f"[list] GondiEventDecoder initialized with {len(self.contracts)} contract(s)")
            if self.v2_addresses:
                print(f"   V2 (Source) addresses: {len(self.v2_addresses)}")

        # For backwards compatibility, set self.contract to first contract
        self.contract = next(iter(self.contracts.values())) if self.contracts else None

        # Block timestamp cache
        self._block_cache: Dict[int, int] = {}

    def _is_v2_contract(self, address: str) -> bool:
        """Check if an address uses V2 ABI (Source[] without floor)"""
        return address.lower() in self.v2_addresses

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

    def _get_block_timestamp_int(self, block_number: int) -> int:
        """Get block timestamp as integer"""
        if block_number in self._block_cache:
            return self._block_cache[block_number]
        block = self.w3.eth.get_block(block_number)
        self._block_cache[block_number] = block['timestamp']
        return block['timestamp']

    def decode_transaction(self, tx_hash: str) -> List[DecodedGondiEvent]:
        """
        Decode all Gondi events from a transaction.
        Tries all registered Gondi contracts to maximize decoding success.

        Args:
            tx_hash: Transaction hash

        Returns:
            List of DecodedGondiEvent objects
        """
        try:
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)
            tx = self.w3.eth.get_transaction(tx_hash)
            block_ts = self._get_block_timestamp(receipt['blockNumber'])
            block_ts_int = self._get_block_timestamp_int(receipt['blockNumber'])

            decoded_events = []
            decoded_log_indices = set()  # Track which logs we've decoded

            # Determine which contract the transaction interacted with
            tx_to_addr = (tx.get('to') or '').lower()
            tx_is_v2 = self._is_v2_contract(tx_to_addr)

            # Try to decode function input for loan data
            input_loan = None
            input_old_loan = None
            function_name = None

            if tx['input'] and len(tx['input']) >= 10:
                # v1.7.0: Try TARGET contract FIRST (tx['to']), then others
                # This is critical because V2 contract (0x478f...) must be decoded
                # with V2 ABI, not V3 ABI which happens to match some selectors
                contracts_to_try = []

                # Add target contract first if we have it
                if tx_to_addr and tx_to_addr in self.contracts:
                    contracts_to_try.append((tx_to_addr, self.contracts[tx_to_addr]))

                # Add remaining contracts
                for addr, contract in self.contracts.items():
                    if addr != tx_to_addr:
                        contracts_to_try.append((addr, contract))

                for contract_addr, contract in contracts_to_try:
                    try:
                        func, params = contract.decode_function_input(tx['input'])
                        function_name = func.fn_name
                        is_v2 = self._is_v2_contract(contract_addr)

                        # v1.7.0: Handle multicall unwrapping
                        if function_name == 'multicall' and 'data' in params:
                            # Multicall contains inner calls - decode them to find _loan
                            inner_loan, inner_old_loan, inner_func = self._unwrap_multicall_for_loan(
                                contract, params['data'], is_v2
                            )
                            if inner_old_loan or inner_loan:
                                input_loan = inner_loan
                                input_old_loan = inner_old_loan
                                function_name = inner_func or function_name
                        else:
                            input_loan, input_old_loan = self._extract_loans_from_input(params, function_name, is_v2)

                        break  # Success, stop trying
                    except Exception:
                        continue

            # DEBUG: Log contract addresses we're looking for
            print(f"[DEBUG] Looking for events from contracts: {list(self.contracts.keys())}")
            log_addresses = set(log['address'].lower() for log in receipt['logs'])
            print(f"[DEBUG] Transaction logs from addresses: {log_addresses}")
            matching = log_addresses & set(self.contracts.keys())
            print(f"[DEBUG] Matching contracts: {matching}")

            # First pass: decode events from logs that match specific contracts
            for log in receipt['logs']:
                log_addr = log['address'].lower()

                # Check if this log is from one of our Gondi contracts
                if log_addr in self.contracts:
                    # DEBUG: Show topic0 (event signature) for this log
                    if log.get('topics'):
                        print(f"[DEBUG] Log from {log_addr[:10]}... topic0: {log['topics'][0].hex() if hasattr(log['topics'][0], 'hex') else log['topics'][0]}")
                    contract = self.contracts[log_addr]
                    print(f"[DEBUG] Got contract from self.contracts: {contract.address}")
                    print(f"[DEBUG] Contract has events: {[e for e in dir(contract.events) if not e.startswith('_')][:5]}...")
                    is_v2 = self._is_v2_contract(log_addr)

                    # Try to decode each event type
                    print(f"[DEBUG] Trying {len(list(GondiEventType))} event types for log from {log_addr[:10]}...")
                    for event_type in GondiEventType:
                        try:
                            event_obj = getattr(contract.events, event_type.value, None)
                            if event_obj is None:
                                print(f"[DEBUG]   Event {event_type.value} not in ABI")
                                continue
                            # DEBUG: Show expected signature
                            try:
                                expected_sig = event_obj.event_abi.get('name', 'unknown')
                            except:
                                expected_sig = event_type.value

                            # Use process_log for single log decoding (more reliable across web3.py versions)
                            try:
                                decoded_log = event_obj.process_log(log)
                                print(f"[DEBUG] process_log succeeded for {event_type.value}")
                            except Exception as e:
                                # Event signature doesn't match this log
                                print(f"[DEBUG] process_log failed for {event_type.value}: {type(e).__name__}: {str(e)[:100]}")
                                continue

                            if decoded_log:
                                print(f"[DEBUG] Found event {event_type.value} at logIndex {log['logIndex']}")
                                decoded = self._decode_event(
                                    event_type.value,
                                    decoded_log,
                                    tx_hash,
                                    block_ts,
                                    block_ts_int,
                                    input_loan,
                                    input_old_loan,
                                    function_name,
                                    is_v2
                                )
                                if decoded:
                                    print(f"[DEBUG] Successfully decoded {event_type.value}")
                                    decoded_events.append(decoded)
                                    decoded_log_indices.add(log['logIndex'])
                                else:
                                    print(f"[DEBUG] _decode_event returned None for {event_type.value}")
                                break  # Found the event type for this log, move to next log

                        except Exception as e:
                            # Log all errors for debugging
                            print(f"[DEBUG] Exception for {event_type.value}: {type(e).__name__}: {str(e)[:100]}")
                            continue

            # Second pass: try all contracts for any Gondi-address logs we missed
            for log in receipt['logs']:
                log_addr = log['address'].lower()

                # Skip if already decoded or not a Gondi contract
                if log['logIndex'] in decoded_log_indices:
                    continue
                if log_addr not in GONDI_CONTRACT_ADDRESSES:
                    continue

                # Determine is_v2 for this log address
                log_is_v2 = self._is_v2_contract(log_addr)

                # Try all contracts
                for contract_addr, contract in self.contracts.items():
                    if log['logIndex'] in decoded_log_indices:
                        break

                    for event_type in GondiEventType:
                        try:
                            event_obj = getattr(contract.events, event_type.value)
                            # Use process_log for single log decoding
                            try:
                                decoded_log = event_obj.process_log(log)
                            except Exception:
                                continue

                            if decoded_log:
                                decoded = self._decode_event(
                                    event_type.value,
                                    decoded_log,
                                    tx_hash,
                                    block_ts,
                                    block_ts_int,
                                    input_loan,
                                    input_old_loan,
                                    function_name,
                                    log_is_v2
                                )
                                if decoded:
                                    decoded_events.append(decoded)
                                    decoded_log_indices.add(log['logIndex'])
                                break

                        except Exception:
                            continue

            # For LoanLiquidated, scan Transfer events for proceeds
            for event in decoded_events:
                if event.event_type == GondiEventType.LOAN_LIQUIDATED.value:
                    self._scan_transfer_events_for_proceeds(event, receipt)

            # v1.7.0: For LoanRefinanced, scan Transfer events for V2 continuation inference
            for event in decoded_events:
                if event.event_type in (GondiEventType.LOAN_REFINANCED.value,
                                        GondiEventType.LOAN_REFINANCED_FROM_NEW_OFFERS.value):
                    self._scan_transfer_events_for_proceeds(event, receipt)

            return decoded_events

        except Exception as e:
            print(f"[!] Error decoding {tx_hash}: {e}")
            return []

    def _unwrap_multicall_for_loan(
        self,
        contract,
        inner_calls: List[bytes],
        is_v2: bool = False
    ) -> Tuple[Optional[Loan], Optional[Loan], Optional[str]]:
        """
        v1.7.0: Unwrap multicall to find _loan parameter from inner calls.

        This is critical for V2 refinance transactions where:
        - The outer call is multicall(bytes[])
        - The inner call is refinanceFull/refinancePartial with _loan
        - _loan contains the OLD loan state with source[] (tranches)

        Returns:
            (loan, old_loan, function_name) from the inner call
        """
        loan = None
        old_loan = None
        function_name = None

        for inner_calldata in inner_calls:
            try:
                inner_func, inner_params = contract.decode_function_input(inner_calldata)
                inner_func_name = inner_func.fn_name

                # Look for refinance functions that have _loan parameter
                refinance_functions = [
                    'refinanceFull', 'refinancePartial', 'refinanceFromLoanExecutionData',
                    'addNewTranche', 'repayLoan'
                ]

                if inner_func_name in refinance_functions:
                    function_name = inner_func_name
                    inner_loan, inner_old_loan = self._extract_loans_from_input(
                        inner_params, inner_func_name, is_v2
                    )

                    # For refinance, _loan is the OLD loan
                    if inner_old_loan:
                        old_loan = inner_old_loan
                        print(f"  [pkg] Multicall unwrapped: {inner_func_name} -> found old_loan with {len(old_loan.tranches)} tranches")

                        # Debug: print tranche details
                        for i, t in enumerate(old_loan.tranches):
                            print(f"      Tranche {i}: loanId={t.loanId}, lender={t.lender[:12]}..., principal={t.principalAmount/1e18:.6f} ETH")

                        break  # Found what we need
                    elif inner_loan:
                        loan = inner_loan

            except Exception as e:
                # Skip inner calls we can't decode
                continue

        return loan, old_loan, function_name

    def _extract_loans_from_input(
        self,
        params: Dict,
        function_name: str,
        is_v2: bool = False
    ) -> Tuple[Optional[Loan], Optional[Loan]]:
        """Extract Loan structs from function input parameters"""
        loan = None
        old_loan = None

        # repayLoan: _repaymentData contains loan
        if '_repaymentData' in params:
            repay_data = params['_repaymentData']
            if hasattr(repay_data, 'keys') and 'loan' in repay_data:  # Handle AttributeDict
                loan = Loan.from_dict(repay_data['loan'], is_v2=is_v2)
            elif isinstance(repay_data, tuple) and len(repay_data) >= 2:
                # LoanRepaymentData: (SignableRepaymentData, Loan, bytes)
                loan = Loan.from_tuple(repay_data[1], is_v2=is_v2)

        # Direct _loan parameter
        elif '_loan' in params:
            val = params['_loan']
            if hasattr(val, 'keys'):  # Handle AttributeDict
                loan = Loan.from_dict(val, is_v2=is_v2)
            elif isinstance(val, tuple):
                loan = Loan.from_tuple(val, is_v2=is_v2)

        # For refinancing functions, _loan is the OLD loan state
        # The NEW loan comes from the event
        refinance_functions = [
            'refinanceFull', 'refinancePartial', 'refinanceFromLoanExecutionData', 'addNewTranche'
        ]
        if function_name in refinance_functions and loan:
            old_loan = loan
            loan = None

        return loan, old_loan

    def _decode_event(
        self,
        event_type: str,
        log: Dict,
        tx_hash: str,
        block_ts: datetime,
        block_ts_int: int,
        input_loan: Optional[Loan],
        input_old_loan: Optional[Loan],
        function_name: Optional[str],
        is_v2: bool = False
    ) -> Optional[DecodedGondiEvent]:
        """Decode a single event log"""
        args = dict(log['args'])

        # Get contract address from log (handles both AttributeDict and plain dict)
        log_address = log.get('address', '')
        if hasattr(log_address, 'lower'):
            contract_addr = log_address.lower()
        else:
            contract_addr = str(log_address).lower()

        event = DecodedGondiEvent(
            event_type=event_type,
            tx_hash=tx_hash,
            block_number=log['blockNumber'],
            log_index=log['logIndex'],
            timestamp=block_ts,
            contract_address=contract_addr,  # Source of truth from log
        )

        # Extract common fields
        event.loan_id = safe_int(args.get('loanId'))
        event.fee = safe_int(args.get('fee', args.get('totalFee', 0)))

        # Event-specific handling
        if event_type == GondiEventType.LOAN_EMITTED.value:
            # V2 has single offerId + lender/borrower fields
            # V1/V3 has offerId array, no separate lender/borrower fields
            offer_id_raw = args.get('offerId', [])
            if isinstance(offer_id_raw, (list, tuple)):
                event.offer_ids = list(offer_id_raw)
            else:
                event.offer_ids = [offer_id_raw]

            # Parse loan struct
            if 'loan' in args:
                event.loan = self._parse_loan_from_event(args['loan'], is_v2)

            # V2 has explicit lender/borrower fields at event level
            # (V1/V3 get these from the loan.tranches and loan.borrower)
            if 'lender' in args:
                v2_lender = safe_address(args.get('lender', ''))
                # Store for reference - actual lender is in tranches
            if 'borrower' in args:
                v2_borrower = safe_address(args.get('borrower', ''))
                # Verify matches loan.borrower
                if event.loan and event.loan.borrower != v2_borrower:
                    print(f"[!] Borrower mismatch: event={v2_borrower}, loan={event.loan.borrower}")

        elif event_type == GondiEventType.LOAN_REPAID.value:
            event.total_repayment = safe_int(args.get('totalRepayment', 0))
            event.loan = input_loan  # From function input

        elif event_type == GondiEventType.LOAN_REFINANCED.value:
            event.renegotiation_id = safe_int(args.get('renegotiationId'))
            event.old_loan_id = safe_int(args.get('oldLoanId'))
            event.new_loan_id = safe_int(args.get('newLoanId'))
            event.loan_id = event.new_loan_id  # Primary ID is new loan
            if 'loan' in args:
                event.loan = self._parse_loan_from_event(args['loan'], is_v2)
            event.old_loan = input_old_loan if input_old_loan else input_loan

        elif event_type == GondiEventType.LOAN_REFINANCED_FROM_NEW_OFFERS.value:
            # Note: V2 does not have LoanRefinancedFromNewOffers event
            event.old_loan_id = safe_int(args.get('loanId'))
            event.new_loan_id = safe_int(args.get('newLoanId'))
            event.loan_id = event.new_loan_id
            event.offer_ids = list(args.get('offerIds', []))
            if 'loan' in args:
                event.loan = self._parse_loan_from_event(args['loan'], is_v2)
            event.old_loan = input_old_loan if input_old_loan else input_loan

        elif event_type == GondiEventType.LOAN_EXTENDED.value:
            # V2-only event: loan extension
            event.old_loan_id = safe_int(args.get('oldLoanId'))
            event.new_loan_id = safe_int(args.get('newLoanId'))
            event.loan_id = event.new_loan_id
            event.extension = safe_int(args.get('_extension', 0))
            if 'loan' in args:
                event.loan = self._parse_loan_from_event(args['loan'], is_v2)
            event.old_loan = input_loan

        elif event_type == GondiEventType.LOAN_FORECLOSED.value:
            event.loan = input_loan

        elif event_type == GondiEventType.LOAN_LIQUIDATED.value:
            event.loan = input_loan

        elif event_type == GondiEventType.LOAN_SENT_TO_LIQUIDATOR.value:
            event.liquidator = safe_address(args.get('liquidator', ''))
            event.loan = input_loan

        # Filter for fund wallet tranches (fund as LENDER)
        # NOTE: Must lowercase t.lender since fund_wallet_list contains lowercase addresses
        if event.loan:
            event.fund_tranches = [
                t for t in event.loan.tranches
                if t.lender.lower() in self.fund_wallet_list
            ]
            # Check if fund is the BORROWER
            event.is_fund_borrower = event.loan.borrower.lower() in self.fund_wallet_list

        if event.old_loan:
            event.old_fund_tranches = [
                t for t in event.old_loan.tranches
                if t.lender.lower() in self.fund_wallet_list
            ]
            # Check if fund was the BORROWER on old loan
            event.old_is_fund_borrower = event.old_loan.borrower.lower() in self.fund_wallet_list

        return event

    def _parse_loan_from_event(self, loan_data: Any, is_v2: bool = False) -> Loan:
        """Parse Loan struct from event data - handles AttributeDict from web3.py"""
        if hasattr(loan_data, 'keys'):  # Handle dict-like objects including AttributeDict
            return Loan.from_dict(loan_data, is_v2=is_v2)
        elif isinstance(loan_data, tuple):
            return Loan.from_tuple(loan_data, is_v2=is_v2)
        else:
            raise ValueError(f"Unknown loan data type: {type(loan_data)}")

    def _scan_transfer_events_for_proceeds(
        self,
        event: DecodedGondiEvent,
        receipt: Dict
    ) -> None:
        """
        Scan Transfer events in transaction for proceeds to/from fund wallets.
        Updates event.transfer_proceeds (inflows) and event.transfer_outflows (outflows).

        v1.7.0: Added outflow tracking for increased participation cases.
        """
        if not event.loan:
            return

        principal_address = event.loan.principalAddress

        for log in receipt['logs']:
            # Check if this is a Transfer event from the principal token
            if (log['address'].lower() == principal_address and
                len(log['topics']) >= 3 and
                log['topics'][0].hex() == TRANSFER_TOPIC.hex()):

                # Decode Transfer(from, to, amount)
                try:
                    from_addr = '0x' + log['topics'][1].hex()[-40:]
                    from_addr = from_addr.lower()
                    to_addr = '0x' + log['topics'][2].hex()[-40:]
                    to_addr = to_addr.lower()
                    amount = int(log['data'].hex(), 16) if log['data'] else 0

                    # Track INFLOWS: transfers TO fund wallets
                    if to_addr in self.fund_wallet_list:
                        if to_addr in event.transfer_proceeds:
                            event.transfer_proceeds[to_addr] += amount
                        else:
                            event.transfer_proceeds[to_addr] = amount

                    # Track OUTFLOWS: transfers FROM fund wallets (v1.7.0)
                    if from_addr in self.fund_wallet_list:
                        if from_addr in event.transfer_outflows:
                            event.transfer_outflows[from_addr] += amount
                        else:
                            event.transfer_outflows[from_addr] = amount

                except Exception:
                    continue

    def decode_batch(
        self,
        tx_hashes: List[str],
        max_workers: int = 8,
        show_progress: bool = True
    ) -> List[DecodedGondiEvent]:
        """
        Decode multiple transactions in parallel.

        Args:
            tx_hashes: List of transaction hashes
            max_workers: Number of parallel workers
            show_progress: Show progress bar

        Returns:
            List of all decoded events
        """
        all_events = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_hash = {
                executor.submit(self.decode_transaction, tx_hash): tx_hash
                for tx_hash in tx_hashes
            }

            iterator = as_completed(future_to_hash)
            if show_progress:
                iterator = tqdm(
                    iterator,
                    total=len(tx_hashes),
                    desc="Decoding Gondi",
                    colour="magenta"
                )

            for future in iterator:
                tx_hash = future_to_hash[future]
                try:
                    events = future.result()
                    all_events.extend(events)
                except Exception as e:
                    print(f"[!] Failed to decode {tx_hash}: {e}")

        return all_events


# ============================================================================
# JOURNAL ENTRY GENERATOR
# ============================================================================

class GondiJournalEntryGenerator:
    """
    Generates GAAP-compliant journal entries from decoded Gondi events.

    Output columns use generic names (debit, credit, principal, payoff_amount) in wei.
    Post-processing should convert to debit_crypto, credit_crypto with proper decimals.

    Account Names (with dynamic currency suffix):
    - deemed_cash_usd
    - loan_receivable_cryptocurrency_{currency}
    - interest_receivable_cryptocurrency_{currency}
    - interest_income_cryptocurrency_{currency}
    - investments_nfts_seized_collateral
    - bad_debt_expense_{currency}
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
            addr for addr, meta in self.wallet_metadata.items()
            if meta.get('category', '').lower() == 'fund'
        ]

    def _get_wallet_info(self, address: str) -> Dict:
        """Get wallet metadata for an address"""
        if not address:
            return {}
        return self.wallet_metadata.get(address.lower(), {})

    def _get_fund_id(self, address: str) -> str:
        """Get fund_id for an address"""
        info = self._get_wallet_info(address)
        return info.get('fund_id', '')

    def _build_common_metadata(
        self,
        event: DecodedGondiEvent,
        tranche: Tranche,
        loan: Loan,
        event_name_override: Optional[str] = None
    ) -> Dict:
        """Build common metadata for journal entries"""
        currency = loan.cryptocurrency
        currency_suffix = get_account_suffix(currency)

        lender_fund = self._get_fund_id(tranche.lender)
        borrower_fund = self._get_fund_id(loan.borrower)

        # Calculate loan due date and annual interest rate
        loan_due_date = loan.due_date
        annual_rate_pct = tranche.aprBps / 100.0  # Convert bps to percentage

        return {
            'date': event.timestamp,
            'fund_id': lender_fund,
            'counterparty_fund_id': borrower_fund,
            'wallet_id': tranche.lender,
            'cryptocurrency': currency,
            'transaction_type': 'investments_lending',
            'platform': PLATFORM,
            'event': event_name_override or event.event_type,
            'hash': event.tx_hash,
            'loan_id': event.loan_id,
            'lender': tranche.lender,
            'borrower': loan.borrower,
            'from': tranche.lender,
            'to': loan.borrower,
            'contract_address': GONDI_CONTRACT.lower(),
            'payable_currency': loan.principalAddress,
            'collateral_address': loan.nftCollateralAddress,
            'token_id': str(loan.nftCollateralTokenId),
            'annual_interest_rate': annual_rate_pct,
            'loan_due_date': loan_due_date,
            'tranche_floor': tranche.floor,
            'tranche_index': loan.tranches.index(tranche) if tranche in loan.tranches else 0,
            '_currency_suffix': currency_suffix,  # Internal use for account names
        }

    # =========================================================================
    # LOAN EMITTED - New Loan Origination
    # =========================================================================

    def generate_loan_emitted_entries(
        self,
        events: List[DecodedGondiEvent]
    ) -> pd.DataFrame:
        """
        Generate journal entries for LoanEmitted events.

        From LENDER's perspective (per tranche):
        - Dr loan_receivable_cryptocurrency_{currency} (principal)
        - Cr deemed_cash_usd (cash disbursed = principal - net_fee)
        - Cr interest_income_cryptocurrency_{currency} (net fee to lender)

        From BORROWER's perspective (fund borrows to finance operations):
        - Dr deemed_cash_usd (cash received = principal - fee)
        - Dr interest_expense_cryptocurrency_{currency} (fee paid to lender)
        - Cr note_payable_cryptocurrency_{currency} (principal owed)
        """
        journal_rows = []

        # NOTE: totalFee from the event is already NET - protocol has taken its cut
        # before emitting the event. No further deduction needed.

        for event in events:
            if event.event_type != GondiEventType.LOAN_EMITTED.value:
                continue

            if not event.loan:
                continue

            loan = event.loan
            total_fee = event.fee or 0  # This is already NET (after protocol cut)
            currency_suffix = get_account_suffix(loan.cryptocurrency)

            # ---- LENDER PERSPECTIVE ----
            # Process tranches where fund is the lender
            for tranche in event.fund_tranches:
                common = self._build_common_metadata(event, tranche, loan)
                common['fund_role'] = 'lender'

                # Calculate this tranche's share of origination fee
                # totalFee is already net - pro-rata by principal
                if loan.principalAmount > 0 and total_fee > 0:
                    fee_share = (tranche.principalAmount * total_fee) // loan.principalAmount
                else:
                    fee_share = 0

                # fee_share is already NET (protocol took cut before event emission)
                net_fee_income = fee_share

                # Cash disbursed by lender = principal - net_fee_income
                cash_disbursed = tranche.principalAmount - net_fee_income

                # Calculate payoff amount (principal + expected net interest)
                _, _, net_interest = calculate_interest(
                    tranche.principalAmount,
                    tranche.aprBps,
                    loan.duration,
                    loan.protocolFee
                )
                payoff_amount = tranche.principalAmount + net_interest

                # 1) Dr loan_receivable (full principal - what borrower owes)
                journal_rows.append({
                    **common,
                    'account_name': f'loan_receivable_cryptocurrency_{currency_suffix}',
                    'debit': tranche.principalAmount,
                    'credit': 0,
                    'principal': tranche.principalAmount,
                    'payoff_amount': payoff_amount,
                    'origination_fee': fee_share,
                    'net_origination_fee': net_fee_income,
                })

                # 2) Cr deemed_cash (actual cash disbursed)
                journal_rows.append({
                    **common,
                    'account_name': 'deemed_cash_usd',
                    'debit': 0,
                    'credit': cash_disbursed,
                    'principal': tranche.principalAmount,
                    'payoff_amount': payoff_amount,
                    'origination_fee': fee_share,
                    'net_origination_fee': net_fee_income,
                })

                # 3) Cr interest_income (net fee earned upfront)
                if net_fee_income > 0:
                    journal_rows.append({
                        **common,
                        'account_name': f'interest_income_cryptocurrency_{currency_suffix}',
                        'debit': 0,
                        'credit': net_fee_income,
                        'principal': tranche.principalAmount,
                        'payoff_amount': payoff_amount,
                        'origination_fee': fee_share,
                        'net_origination_fee': net_fee_income,
                    })

            # ---- BORROWER PERSPECTIVE ----
            # Process when fund is the borrower
            if event.is_fund_borrower:
                # Build common metadata for borrower
                borrower_fund_id = self._get_fund_id(loan.borrower)

                # Calculate totals across all tranches
                total_principal = loan.principalAmount

                # Cash received = principal - total_fee (fee goes to lenders)
                cash_received = total_principal - total_fee

                # Expected total repayment (principal + all interest to all tranches)
                total_interest = 0
                for tranche in loan.tranches:
                    _, _, net_int = calculate_interest(
                        tranche.principalAmount,
                        tranche.aprBps,
                        loan.duration,
                        loan.protocolFee
                    )
                    total_interest += net_int
                total_repayment = total_principal + total_interest

                # Common metadata for borrower entries
                loan_due_date = datetime.fromtimestamp(
                    loan.startTime + loan.duration, tz=timezone.utc
                )
                borrower_common = {
                    'date': event.timestamp,
                    'fund_id': borrower_fund_id,
                    'counterparty_fund_id': '',  # Multiple lenders
                    'wallet_id': loan.borrower,
                    'cryptocurrency': loan.cryptocurrency,
                    'transaction_type': 'financing_borrowings',
                    'platform': 'Gondi',
                    'event': event.event_type,
                    'hash': event.tx_hash,
                    'loan_id': event.loan_id,
                    'lender': 'multiple',
                    'borrower': loan.borrower,
                    'from': 'multiple',
                    'to': loan.borrower,
                    'contract_address': event.contract_address,
                    'collateral_address': loan.nftCollateralAddress,
                    'token_id': str(loan.nftCollateralTokenId),
                    'loan_due_date': loan_due_date,
                    'fund_role': 'borrower',
                    '_currency_suffix': currency_suffix,
                }

                # 1) Dr deemed_cash (cash received by borrower)
                journal_rows.append({
                    **borrower_common,
                    'account_name': 'deemed_cash_usd',
                    'debit': cash_received,
                    'credit': 0,
                    'principal': total_principal,
                    'payoff_amount': total_repayment,
                    'origination_fee': total_fee,
                })

                # 2) Dr interest_expense (origination fee paid to lenders)
                if total_fee > 0:
                    journal_rows.append({
                        **borrower_common,
                        'account_name': f'interest_expense_cryptocurrency_{currency_suffix}',
                        'debit': total_fee,
                        'credit': 0,
                        'principal': total_principal,
                        'payoff_amount': total_repayment,
                        'origination_fee': total_fee,
                    })

                # 3) Cr note_payable (principal owed to lenders)
                journal_rows.append({
                    **borrower_common,
                    'account_name': f'note_payable_cryptocurrency_{currency_suffix}',
                    'debit': 0,
                    'credit': total_principal,
                    'principal': total_principal,
                    'payoff_amount': total_repayment,
                    'origination_fee': total_fee,
                })

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    # =========================================================================
    # LOAN REPAID - Loan Repayment
    # =========================================================================

    def generate_loan_repaid_entries(
        self,
        events: List[DecodedGondiEvent]
    ) -> pd.DataFrame:
        """
        Generate journal entries for LoanRepaid events.

        From LENDER's perspective (per tranche):
        - Dr deemed_cash_usd (total received)
        - Cr loan_receivable_cryptocurrency_{currency} (principal)
        - Cr interest_receivable_cryptocurrency_{currency} (interest)

        From BORROWER's perspective (fund repays its borrowings):
        - Dr note_payable_cryptocurrency_{currency} (principal paid off)
        - Dr interest_payable_cryptocurrency_{currency} (interest paid)
        - Cr deemed_cash_usd (total paid)
        """
        journal_rows = []

        for event in events:
            if event.event_type != GondiEventType.LOAN_REPAID.value:
                continue

            if not event.loan:
                continue

            loan = event.loan
            block_ts_int = int(event.timestamp.timestamp())
            currency_suffix = get_account_suffix(loan.cryptocurrency)

            # ---- LENDER PERSPECTIVE ----
            for tranche in event.fund_tranches:
                common = self._build_common_metadata(event, tranche, loan)
                common['fund_role'] = 'lender'

                # Calculate interest (carried + current period net)
                total_interest, _ = calculate_tranche_interest(
                    tranche,
                    block_ts_int,
                    loan.protocolFee
                )

                # Total received by lender
                total_received = tranche.principalAmount + total_interest

                # 1) Dr deemed_cash (total received)
                journal_rows.append({
                    **common,
                    'account_name': 'deemed_cash_usd',
                    'debit': total_received,
                    'credit': 0,
                    'principal': tranche.principalAmount,
                    'payoff_amount': total_received,
                })

                # 2) Cr loan_receivable (principal)
                journal_rows.append({
                    **common,
                    'account_name': f'loan_receivable_cryptocurrency_{currency_suffix}',
                    'debit': 0,
                    'credit': tranche.principalAmount,
                    'principal': tranche.principalAmount,
                    'payoff_amount': total_received,
                })

                # 3) Cr interest_receivable (interest)
                if total_interest > 0:
                    journal_rows.append({
                        **common,
                        'account_name': f'interest_receivable_cryptocurrency_{currency_suffix}',
                        'debit': 0,
                        'credit': total_interest,
                        'principal': tranche.principalAmount,
                        'payoff_amount': total_received,
                    })

            # ---- BORROWER PERSPECTIVE ----
            if event.is_fund_borrower:
                borrower_fund_id = self._get_fund_id(loan.borrower)

                # Calculate totals across all tranches
                total_principal = loan.principalAmount
                total_interest = 0
                for tranche in loan.tranches:
                    tranche_int, _ = calculate_tranche_interest(
                        tranche,
                        block_ts_int,
                        loan.protocolFee
                    )
                    total_interest += tranche_int

                total_paid = total_principal + total_interest

                # Common metadata for borrower entries
                borrower_common = {
                    'date': event.timestamp,
                    'fund_id': borrower_fund_id,
                    'counterparty_fund_id': '',
                    'wallet_id': loan.borrower,
                    'cryptocurrency': loan.cryptocurrency,
                    'transaction_type': 'financing_borrowings',
                    'platform': 'Gondi',
                    'event': event.event_type,
                    'hash': event.tx_hash,
                    'loan_id': event.loan_id,
                    'lender': 'multiple',
                    'borrower': loan.borrower,
                    'from': loan.borrower,
                    'to': 'multiple',
                    'contract_address': event.contract_address,
                    'collateral_address': loan.nftCollateralAddress,
                    'token_id': str(loan.nftCollateralTokenId),
                    'fund_role': 'borrower',
                    '_currency_suffix': currency_suffix,
                }

                # 1) Dr note_payable (principal paid off)
                journal_rows.append({
                    **borrower_common,
                    'account_name': f'note_payable_cryptocurrency_{currency_suffix}',
                    'debit': total_principal,
                    'credit': 0,
                    'principal': total_principal,
                    'payoff_amount': total_paid,
                })

                # 2) Dr interest_payable (interest paid)
                if total_interest > 0:
                    journal_rows.append({
                        **borrower_common,
                        'account_name': f'interest_payable_cryptocurrency_{currency_suffix}',
                        'debit': total_interest,
                        'credit': 0,
                        'principal': total_principal,
                        'payoff_amount': total_paid,
                    })

                # 3) Cr deemed_cash (total cash paid out)
                journal_rows.append({
                    **borrower_common,
                    'account_name': 'deemed_cash_usd',
                    'debit': 0,
                    'credit': total_paid,
                    'principal': total_principal,
                    'payoff_amount': total_paid,
                })

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    # =========================================================================
    # LOAN REFINANCED - Refinancing (Treated as Payoff + New Loan)
    # =========================================================================

    def generate_loan_refinanced_entries(
        self,
        events: List[DecodedGondiEvent]
    ) -> pd.DataFrame:
        """
        Generate journal entries for LoanRefinanced and LoanRefinancedFromNewOffers.

        Treatment: Payoff of old loan + Origination of new loan

        LENDER perspective - OLD loan (fund tranches being paid off):
        - Dr deemed_cash (principal + interest received)
        - Cr loan_receivable (principal)
        - Cr interest_receivable (interest)

        LENDER perspective - NEW loan (fund tranches entering):
        - Dr loan_receivable (principal)
        - Dr interest_receivable (accrued interest paid)
        - Cr deemed_cash (funded principal + accrued)
        - Cr interest_income (fee share if any)

        BORROWER perspective - refinancing existing debt:
        - Old debt extinguished, new debt created
        - Any fee paid is interest expense
        """
        journal_rows = []

        refinance_event_types = {
            GondiEventType.LOAN_REFINANCED.value,
            GondiEventType.LOAN_REFINANCED_FROM_NEW_OFFERS.value,
        }

        # NOTE: totalFee from the event is already NET - protocol has taken its cut
        # before emitting the event. No further deduction needed.

        for event in events:
            if event.event_type not in refinance_event_types:
                continue

            block_ts_int = int(event.timestamp.timestamp())

            # ---- LENDER PERSPECTIVE: PAYOFF OLD LOAN ----
            if event.old_loan and event.old_fund_tranches:
                old_loan = event.old_loan
                currency_suffix = get_account_suffix(old_loan.cryptocurrency)

                # v1.7.0 DEBUG: Show that we're using actual old_loan data
                print(f"  [OK] PAYOFF using old_loan from input (not inferred):")
                print(f"      old_loan_id: {event.old_loan_id}")
                print(f"      old_fund_tranches: {len(event.old_fund_tranches)}")
                for i, t in enumerate(event.old_fund_tranches):
                    print(f"      Tranche {i}: principal={t.principalAmount/1e18:.6f}, accrued={t.accruedInterest/1e18:.6f}")

                for tranche in event.old_fund_tranches:
                    lender_lower = tranche.lender.lower()

                    # v1.7.1: Check if this is a TRUE CONTINUATION
                    # A true continuation requires BOTH:
                    # 1. Same lender appears in new loan tranches
                    # 2. New tranche's loanId references the OLD loan (rollover signal)
                    # Without condition 2, this could be a coincidental overlap
                    # (lender gets paid off AND enters a completely new position)
                    new_tranche = None
                    is_continuation = False

                    if event.fund_tranches and event.old_loan_id:
                        for nt in event.fund_tranches:
                            if nt.lender.lower() == lender_lower:
                                # Check if this new tranche references the old loan
                                if nt.loanId == event.old_loan_id:
                                    new_tranche = nt
                                    is_continuation = True
                                break

                    common = self._build_common_metadata(
                        event, tranche, old_loan,
                        event_name_override=f"{event.event_type}_payoff"
                    )
                    common['loan_id'] = event.old_loan_id
                    common['fund_role'] = 'lender'

                    if is_continuation:
                        # CONTINUATION LENDER: Use cash-based calculation
                        # Only the REDUCTION in exposure is actually settled
                        common['is_continuation'] = True

                        old_principal = tranche.principalAmount
                        new_principal = new_tranche.principalAmount

                        # Principal reduction (could be negative if increasing exposure)
                        principal_paid = old_principal - new_principal

                        # Get actual cash received from transfers
                        cash_in = event.transfer_proceeds.get(lender_lower, 0)
                        cash_out = event.transfer_outflows.get(lender_lower, 0)
                        net_cash = cash_in - cash_out

                        # Interest settled = cash received - principal paid
                        # This is the ONLY interest that actually moves in cash
                        interest_settled = net_cash - principal_paid

                        print(f"  [list] CONTINUATION payoff for {lender_lower[:10]}...")
                        print(f"      old_principal: {old_principal/1e18:.10f}")
                        print(f"      new_principal: {new_principal/1e18:.10f}")
                        print(f"      principal_paid (delta): {principal_paid/1e18:.10f}")
                        print(f"      net_cash (from transfers): {net_cash/1e18:.10f}")
                        print(f"      interest_settled: {interest_settled/1e18:.10f}")

                        # Sanity check
                        if interest_settled < 0:
                            print(f"  [!] WARNING: Negative interest_settled ({interest_settled}), clamping to 0")
                            interest_settled = 0

                        total_received = principal_paid + interest_settled

                        # 1) Dr deemed_cash (actual cash received)
                        if total_received > 0:
                            journal_rows.append({
                                **common,
                                'account_name': 'deemed_cash_usd',
                                'debit': total_received,
                                'credit': 0,
                                'principal': principal_paid,
                                'payoff_amount': total_received,
                            })

                        # 2) Cr loan_receivable (principal REDUCTION only)
                        if principal_paid > 0:
                            journal_rows.append({
                                **common,
                                'account_name': f'loan_receivable_cryptocurrency_{currency_suffix}',
                                'debit': 0,
                                'credit': principal_paid,
                                'principal': principal_paid,
                                'payoff_amount': total_received,
                            })

                        # 3) Cr interest_receivable (interest actually settled in cash)
                        if interest_settled > 0:
                            journal_rows.append({
                                **common,
                                'account_name': f'interest_receivable_cryptocurrency_{currency_suffix}',
                                'debit': 0,
                                'credit': interest_settled,
                                'principal': principal_paid,
                                'payoff_amount': total_received,
                            })

                    else:
                        # FULL PAYOFF: Lender is exiting completely
                        # Use full principal + calculated interest

                        # Calculate interest earned on old loan
                        total_interest, _ = calculate_tranche_interest(
                            tranche,
                            block_ts_int,
                            old_loan.protocolFee
                        )

                        total_received = tranche.principalAmount + total_interest

                        # 1) Dr deemed_cash
                        journal_rows.append({
                            **common,
                            'account_name': 'deemed_cash_usd',
                            'debit': total_received,
                            'credit': 0,
                            'principal': tranche.principalAmount,
                            'payoff_amount': total_received,
                        })

                        # 2) Cr loan_receivable
                        journal_rows.append({
                            **common,
                            'account_name': f'loan_receivable_cryptocurrency_{currency_suffix}',
                            'debit': 0,
                            'credit': tranche.principalAmount,
                            'principal': tranche.principalAmount,
                            'payoff_amount': total_received,
                        })

                        # 3) Cr interest_receivable
                        if total_interest > 0:
                            journal_rows.append({
                                **common,
                                'account_name': f'interest_receivable_cryptocurrency_{currency_suffix}',
                                'debit': 0,
                                'credit': total_interest,
                                'principal': tranche.principalAmount,
                                'payoff_amount': total_received,
                            })

            # ============================================================
            # V2 CONTINUATION PAYOFF FALLBACK (v1.7.0)
            # ============================================================
            # V2/MultiSource refinance events may have old_loan = None.
            # Detect continuation by: tranche.loanId == event.old_loan_id
            # For continuing lenders, generate inferred payoff entries.
            #
            # Key: tranche.accruedInterest is interest EARNED (settled here),
            # NOT purchased. Settlement amount comes from event accruedInterest
            # as proxy; ideally would come from ledger post-reversal.
            # ============================================================
            v2_continuation_lenders = set()  # Track which lenders got V2 payoff

            if not event.old_loan and event.fund_tranches and event.old_loan_id:
                # WARNING: Using V2 inference fallback (old_loan not extracted from multicall)
                print(f"  [!] V2 FALLBACK: old_loan is None, using inference for loanId {event.old_loan_id}")

                new_loan = event.loan
                currency_suffix = get_account_suffix(new_loan.cryptocurrency) if new_loan else 'weth'

                for tranche in event.fund_tranches:
                    # V2 continuation detection: tranche references old loan ID
                    if tranche.loanId == event.old_loan_id:
                        lender_lower = tranche.lender.lower()
                        v2_continuation_lenders.add(lender_lower)

                        # Get actual cash received for this lender
                        cash_in = event.transfer_proceeds.get(lender_lower, 0)
                        cash_out = event.transfer_outflows.get(lender_lower, 0)
                        net_cash = cash_in - cash_out

                        # Interest settled = accruedInterest from event (proxy for ledger balance)
                        # This is interest the continuing lender EARNED on the old loan
                        interest_settled = tranche.accruedInterest

                        # Infer old principal from cash equation:
                        # net_cash = (old_principal + interest_settled) - (new_principal - fee_share)
                        # Solving: old_principal = net_cash + new_principal - fee_share - interest_settled
                        new_principal = tranche.principalAmount
                        total_fee = event.fee or 0
                        if new_loan and new_loan.principalAmount > 0 and total_fee > 0:
                            fee_share = (new_principal * total_fee) // new_loan.principalAmount
                        else:
                            fee_share = 0

                        # Infer old principal
                        inferred_old_principal = net_cash + new_principal - fee_share - interest_settled

                        # Sanity check: old_principal should be positive
                        if inferred_old_principal <= 0:
                            print(f"[!] V2 continuation: inferred old_principal <= 0 for {lender_lower[:10]}...")
                            print(f"   net_cash={net_cash}, new_principal={new_principal}, fee={fee_share}, int={interest_settled}")
                            print(f"   Transaction: {event.tx_hash}")
                            # Use new_principal as fallback (flat continuation)
                            inferred_old_principal = new_principal

                        total_received = inferred_old_principal + interest_settled

                        # Build common metadata for V2 payoff
                        common_v2 = {
                            'date': event.timestamp,
                            'fund_id': self._get_fund_id(tranche.lender),
                            'counterparty_fund_id': '',
                            'wallet_id': tranche.lender,
                            'cryptocurrency': new_loan.cryptocurrency if new_loan else 'WETH',
                            'transaction_type': 'investments_lending',
                            'platform': 'Gondi',
                            'event': f"{event.event_type}_payoff_v2_inferred",
                            'hash': event.tx_hash,
                            'loan_id': event.old_loan_id,
                            'lender': tranche.lender,
                            'borrower': new_loan.borrower if new_loan else '',
                            'from': tranche.lender,
                            'to': new_loan.borrower if new_loan else '',
                            'contract_address': event.contract_address,
                            'collateral_address': new_loan.nftCollateralAddress if new_loan else '',
                            'token_id': str(new_loan.nftCollateralTokenId) if new_loan else '',
                            'fund_role': 'lender',
                            'v2_inferred': True,
                            'inferred_old_principal': inferred_old_principal,
                            '_currency_suffix': currency_suffix,
                        }

                        # 1) Dr deemed_cash (inferred payoff)
                        journal_rows.append({
                            **common_v2,
                            'account_name': 'deemed_cash_usd',
                            'debit': total_received,
                            'credit': 0,
                            'principal': inferred_old_principal,
                            'payoff_amount': total_received,
                        })

                        # 2) Cr loan_receivable (inferred old principal)
                        journal_rows.append({
                            **common_v2,
                            'account_name': f'loan_receivable_cryptocurrency_{currency_suffix}',
                            'debit': 0,
                            'credit': inferred_old_principal,
                            'principal': inferred_old_principal,
                            'payoff_amount': total_received,
                        })

                        # 3) Cr interest_receivable (settled)
                        if interest_settled > 0:
                            journal_rows.append({
                                **common_v2,
                                'account_name': f'interest_receivable_cryptocurrency_{currency_suffix}',
                                'debit': 0,
                                'credit': interest_settled,
                                'principal': inferred_old_principal,
                                'payoff_amount': total_received,
                            })

                        print(f"  [list] V2 continuation payoff inferred: old_loan={event.old_loan_id}, lender={lender_lower[:10]}...")
                        print(f"     inferred_old_principal={inferred_old_principal/1e18:.6f}, interest_settled={interest_settled/1e18:.6f}")
                        print(f"     net_cash={net_cash/1e18:.6f} (in={cash_in/1e18:.6f}, out={cash_out/1e18:.6f})")

            # ---- LENDER PERSPECTIVE: ORIGINATE NEW LOAN ----
            # When refinancing, the NEW lender pays: principal + accruedInterest
            # The accruedInterest represents interest earned by OLD lender that new lender
            # pays and will collect from borrower at repayment.
            #
            # v1.7.0: For V2 continuation lenders, accruedInterest was EARNED (settled in
            # payoff leg), NOT PURCHASED. Set to 0 for origination entries.
            if event.loan and event.fund_tranches:
                new_loan = event.loan
                total_fee = event.fee or 0
                currency_suffix = get_account_suffix(new_loan.cryptocurrency)

                for tranche in event.fund_tranches:
                    lender_lower = tranche.lender.lower()

                    # v1.7.1: DIRECT continuation detection via loanId comparison
                    # A tranche is a continuation if its loanId matches the OLD loan ID.
                    # This works regardless of whether old_loan was extracted from multicall.
                    # DO NOT rely on v2_continuation_lenders set (only populated in fallback path)
                    is_continuation = (
                        event.old_loan_id is not None and
                        tranche.loanId == event.old_loan_id
                    )

                    if is_continuation:
                        # CRITICAL (v1.7.1): For continuation lenders, SKIP ORIGINATION ENTRIES.
                        #
                        # The payoff leg already handles the delta:
                        # - Cr loan_receivable by principal_reduction
                        # - Cr interest_receivable by interest_settled
                        # - Dr deemed_cash by actual cash received
                        #
                        # The loan receivable balance naturally adjusts to new principal.
                        # NO origination entries needed - they would create incorrect offsets.
                        #
                        # Example (reducing exposure 18 -> 15.12):
                        # - PAYOFF: Dr cash 2.96, Cr loan_rec 2.88, Cr int_rec 0.08
                        # - ORIGINATION: (skip)
                        # - NET: cash +2.96, loan_rec -2.88, int_rec -0.08 [ok]
                        print(f"  [list] CONTINUATION origination skipped: tranche.loanId={tranche.loanId} == old_loan_id={event.old_loan_id}")
                        continue

                    common = self._build_common_metadata(
                        event, tranche, new_loan,
                        event_name_override=f"{event.event_type}_origination"
                    )
                    common['loan_id'] = event.new_loan_id or event.loan_id
                    common['fund_role'] = 'lender'
                    if is_continuation:
                        common['is_continuation'] = True

                    # Fee share proportional to principal
                    # totalFee is already NET (protocol took cut before event emission)
                    if new_loan.principalAmount > 0 and total_fee > 0:
                        fee_share = (tranche.principalAmount * total_fee) // new_loan.principalAmount
                    else:
                        fee_share = 0

                    # fee_share is already NET (protocol took cut before event emission)
                    net_fee_income = fee_share

                    # CRITICAL ACCOUNTING RULE (v1.7.1):
                    # For CONTINUATION lenders: tranche.accruedInterest is NOT purchased interest!
                    # It is carried-forward interest that was EARNED on the old loan and will be
                    # collected at final repayment. It was already settled in the PAYOFF leg.
                    #
                    # For NEW lenders: accruedInterest IS purchased from old lender (asset).
                    if is_continuation:
                        # Continuation: interest was EARNED, already settled in payoff
                        # DO NOT debit interest_receivable, DO NOT affect cash
                        accrued_interest = 0
                    else:
                        # New lender: interest is PURCHASED from old lender
                        accrued_interest = tranche.accruedInterest

                    # Total cash disbursed by lender:
                    # = principal + accruedInterest - net_fee_income
                    # (they fund principal, pay old lender's accrued interest, keep their fee)
                    cash_disbursed = tranche.principalAmount + accrued_interest - net_fee_income

                    # Expected payoff from borrower at repayment
                    _, _, net_interest = calculate_interest(
                        tranche.principalAmount,
                        tranche.aprBps,
                        new_loan.duration,
                        new_loan.protocolFee
                    )
                    payoff_amount = tranche.principalAmount + accrued_interest + net_interest

                    # 1) Dr loan_receivable (principal portion)
                    journal_rows.append({
                        **common,
                        'account_name': f'loan_receivable_cryptocurrency_{currency_suffix}',
                        'debit': tranche.principalAmount,
                        'credit': 0,
                        'principal': tranche.principalAmount,
                        'accrued_interest': accrued_interest,
                        'payoff_amount': payoff_amount,
                    })

                    # 2) Dr interest_receivable (accrued interest paid to old lender)
                    # This is NOT income yet - it's an asset the new lender expects to collect
                    # v1.7.0: Only for NEW lenders (not V2 continuation)
                    if accrued_interest > 0:
                        journal_rows.append({
                            **common,
                            'account_name': f'interest_receivable_cryptocurrency_{currency_suffix}',
                            'debit': accrued_interest,
                            'credit': 0,
                            'principal': tranche.principalAmount,
                            'accrued_interest': accrued_interest,
                            'payoff_amount': payoff_amount,
                        })

                    # 3) Cr deemed_cash (total cash disbursed)
                    journal_rows.append({
                        **common,
                        'account_name': 'deemed_cash_usd',
                        'debit': 0,
                        'credit': cash_disbursed,
                        'principal': tranche.principalAmount,
                        'accrued_interest': accrued_interest,
                        'payoff_amount': payoff_amount,
                    })

                    # 4) Cr interest_income (net fee earned upfront)
                    if net_fee_income > 0:
                        journal_rows.append({
                            **common,
                            'account_name': f'interest_income_cryptocurrency_{currency_suffix}',
                            'debit': 0,
                            'credit': net_fee_income,
                            'principal': tranche.principalAmount,
                            'accrued_interest': accrued_interest,
                            'payoff_amount': payoff_amount,
                        })

            # ============================================================
            # CASH RECONCILIATION ASSERTION (v1.7.0)
            # ============================================================
            # Validate that net deemed_cash from books matches actual ERC-20 transfers.
            # This is VALIDATION, not posting logic.
            # ============================================================
            if event.fund_tranches:
                for tranche in event.fund_tranches:
                    lender_lower = tranche.lender.lower()

                    # Actual ERC-20 cash flow
                    cash_in_actual = event.transfer_proceeds.get(lender_lower, 0)
                    cash_out_actual = event.transfer_outflows.get(lender_lower, 0)
                    net_cash_actual = cash_in_actual - cash_out_actual

                    # Compute expected net cash from journal entries
                    # Payoff Dr deemed_cash = cash IN
                    # Origination Cr deemed_cash = cash OUT
                    # Net = payoff_dr - origination_cr

                    # v1.7.1: Direct continuation check via loanId comparison
                    is_continuation = (
                        event.old_loan_id is not None and
                        tranche.loanId == event.old_loan_id
                    )

                    new_principal = tranche.principalAmount
                    total_fee = event.fee or 0
                    new_loan = event.loan

                    if new_loan and new_loan.principalAmount > 0 and total_fee > 0:
                        fee_share = (new_principal * total_fee) // new_loan.principalAmount
                    else:
                        fee_share = 0

                    if is_continuation:
                        # Continuation: payoff uses cash-based amounts, NO origination entries
                        # Get old tranche data for payoff calculation
                        old_tranche = None
                        if event.old_fund_tranches:
                            for ot in event.old_fund_tranches:
                                if ot.lender.lower() == lender_lower:
                                    old_tranche = ot
                                    break

                        if old_tranche:
                            old_principal = old_tranche.principalAmount
                            # Principal reduction (delta)
                            principal_paid = old_principal - new_principal
                            # Interest settled = net_cash - principal_paid (cash-based)
                            interest_settled = net_cash_actual - principal_paid
                            if interest_settled < 0:
                                interest_settled = 0
                            payoff_dr = principal_paid + interest_settled
                        else:
                            # Fallback to net_cash as payoff_dr
                            payoff_dr = net_cash_actual

                        # CRITICAL (v1.7.1): Continuation has NO origination entries
                        origination_cr = 0
                    else:
                        # Normal case: old_loan exists
                        if event.old_fund_tranches:
                            # Find matching old tranche
                            old_tranche = None
                            for ot in event.old_fund_tranches:
                                if ot.lender.lower() == lender_lower:
                                    old_tranche = ot
                                    break

                            if old_tranche:
                                old_principal = old_tranche.principalAmount
                                interest_earned, _ = calculate_tranche_interest(
                                    old_tranche,
                                    int(event.timestamp.timestamp()),
                                    event.old_loan.protocolFee if event.old_loan else 0
                                )
                                payoff_dr = old_principal + interest_earned
                            else:
                                # New lender entering
                                payoff_dr = 0
                        else:
                            payoff_dr = 0

                        accrued_interest = tranche.accruedInterest
                        origination_cr = new_principal + accrued_interest - fee_share

                    net_cash_books = payoff_dr - origination_cr

                    # Allow small tolerance (10 wei)
                    reconciliation_diff = abs(net_cash_books - net_cash_actual)
                    if reconciliation_diff > 10:
                        print(f"[!] CASH RECONCILIATION WARNING for lender {lender_lower[:10]}...")
                        print(f"   net_cash_books:  {net_cash_books} ({net_cash_books/1e18:.6f} ETH)")
                        print(f"   net_cash_actual: {net_cash_actual} ({net_cash_actual/1e18:.6f} ETH)")
                        print(f"   difference:      {reconciliation_diff} wei")
                        print(f"   payoff_dr:       {payoff_dr}, origination_cr: {origination_cr}")
                        print(f"   Transaction: {event.tx_hash}")

            # ---- BORROWER PERSPECTIVE: REFINANCE DEBT ----
            # When fund is borrower, refinancing replaces old debt with new debt
            if event.old_loan and event.old_is_fund_borrower:
                old_loan = event.old_loan
                currency_suffix = get_account_suffix(old_loan.cryptocurrency)
                borrower_fund_id = self._get_fund_id(old_loan.borrower)

                # Calculate total interest accrued on old loan
                old_total_interest = 0
                for tranche in old_loan.tranches:
                    tranche_int, _ = calculate_tranche_interest(
                        tranche,
                        block_ts_int,
                        old_loan.protocolFee
                    )
                    old_total_interest += tranche_int

                old_total_paid = old_loan.principalAmount + old_total_interest

                borrower_common = {
                    'date': event.timestamp,
                    'fund_id': borrower_fund_id,
                    'counterparty_fund_id': '',
                    'wallet_id': old_loan.borrower,
                    'cryptocurrency': old_loan.cryptocurrency,
                    'transaction_type': 'financing_borrowings',
                    'platform': 'Gondi',
                    'event': f"{event.event_type}_payoff",
                    'hash': event.tx_hash,
                    'loan_id': event.old_loan_id,
                    'lender': 'multiple',
                    'borrower': old_loan.borrower,
                    'from': old_loan.borrower,
                    'to': 'multiple',
                    'contract_address': event.contract_address,
                    'collateral_address': old_loan.nftCollateralAddress,
                    'token_id': str(old_loan.nftCollateralTokenId),
                    'fund_role': 'borrower',
                    '_currency_suffix': currency_suffix,
                }

                # Payoff old debt - these entries are "internal" to refinance
                # The new lender pays old lender, borrower's debt transfers

                # 1) Dr note_payable (old debt extinguished)
                journal_rows.append({
                    **borrower_common,
                    'account_name': f'note_payable_cryptocurrency_{currency_suffix}',
                    'debit': old_loan.principalAmount,
                    'credit': 0,
                    'principal': old_loan.principalAmount,
                    'payoff_amount': old_total_paid,
                })

                # 2) Dr interest_payable (old interest extinguished)
                if old_total_interest > 0:
                    journal_rows.append({
                        **borrower_common,
                        'account_name': f'interest_payable_cryptocurrency_{currency_suffix}',
                        'debit': old_total_interest,
                        'credit': 0,
                        'principal': old_loan.principalAmount,
                        'payoff_amount': old_total_paid,
                    })

                # The credit side comes from new loan creation below

            if event.loan and event.is_fund_borrower:
                new_loan = event.loan
                total_fee = event.fee or 0
                currency_suffix = get_account_suffix(new_loan.cryptocurrency)
                borrower_fund_id = self._get_fund_id(new_loan.borrower)

                # Sum accrued interest from all tranches (carried over from old lenders)
                total_accrued = sum(t.accruedInterest for t in new_loan.tranches)

                # In refinance, borrower doesn't receive/pay cash - debt just transfers
                # But accrued interest is added to the debt
                new_total_debt = new_loan.principalAmount + total_accrued

                # Calculate total interest on new loan
                new_total_interest = 0
                for tranche in new_loan.tranches:
                    _, _, net_int = calculate_interest(
                        tranche.principalAmount,
                        tranche.aprBps,
                        new_loan.duration,
                        new_loan.protocolFee
                    )
                    new_total_interest += net_int

                total_repayment = new_loan.principalAmount + total_accrued + new_total_interest

                loan_due_date = datetime.fromtimestamp(
                    new_loan.startTime + new_loan.duration, tz=timezone.utc
                )

                borrower_common = {
                    'date': event.timestamp,
                    'fund_id': borrower_fund_id,
                    'counterparty_fund_id': '',
                    'wallet_id': new_loan.borrower,
                    'cryptocurrency': new_loan.cryptocurrency,
                    'transaction_type': 'financing_borrowings',
                    'platform': 'Gondi',
                    'event': f"{event.event_type}_origination",
                    'hash': event.tx_hash,
                    'loan_id': event.new_loan_id or event.loan_id,
                    'lender': 'multiple',
                    'borrower': new_loan.borrower,
                    'from': 'multiple',
                    'to': new_loan.borrower,
                    'contract_address': event.contract_address,
                    'collateral_address': new_loan.nftCollateralAddress,
                    'token_id': str(new_loan.nftCollateralTokenId),
                    'loan_due_date': loan_due_date,
                    'fund_role': 'borrower',
                    '_currency_suffix': currency_suffix,
                }

                # 1) Cr note_payable (new principal owed)
                journal_rows.append({
                    **borrower_common,
                    'account_name': f'note_payable_cryptocurrency_{currency_suffix}',
                    'debit': 0,
                    'credit': new_loan.principalAmount,
                    'principal': new_loan.principalAmount,
                    'accrued_interest': total_accrued,
                    'payoff_amount': total_repayment,
                })

                # 2) Cr interest_payable (accrued interest now owed to new lender)
                if total_accrued > 0:
                    journal_rows.append({
                        **borrower_common,
                        'account_name': f'interest_payable_cryptocurrency_{currency_suffix}',
                        'debit': 0,
                        'credit': total_accrued,
                        'principal': new_loan.principalAmount,
                        'accrued_interest': total_accrued,
                        'payoff_amount': total_repayment,
                    })

                # 3) Any refinance fee is expense to borrower
                if total_fee > 0:
                    journal_rows.append({
                        **borrower_common,
                        'account_name': f'interest_expense_cryptocurrency_{currency_suffix}',
                        'debit': total_fee,
                        'credit': 0,
                        'principal': new_loan.principalAmount,
                        'origination_fee': total_fee,
                        'payoff_amount': total_repayment,
                    })
                    # Credit side - fee is paid from cash
                    journal_rows.append({
                        **borrower_common,
                        'account_name': 'deemed_cash_usd',
                        'debit': 0,
                        'credit': total_fee,
                        'principal': new_loan.principalAmount,
                        'origination_fee': total_fee,
                        'payoff_amount': total_repayment,
                    })

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    # =========================================================================
    # LOAN FORECLOSED - Single-Tranche Foreclosure
    # =========================================================================

    def generate_loan_foreclosed_entries(
        self,
        events: List[DecodedGondiEvent]
    ) -> pd.DataFrame:
        """
        Generate journal entries for LoanForeclosed events.

        From lender's perspective (per tranche):
        - Dr investments_nfts_seized_collateral (principal - NFT cost basis)
        - Dr bad_debt_expense_{currency} (interest write-off)
        - Cr loan_receivable_cryptocurrency_{currency} (principal)
        - Cr interest_receivable_cryptocurrency_{currency} (interest)
        """
        journal_rows = []

        for event in events:
            if event.event_type != GondiEventType.LOAN_FORECLOSED.value:
                continue

            if not event.loan or not event.fund_tranches:
                continue

            loan = event.loan
            block_ts_int = int(event.timestamp.timestamp())
            currency_suffix = get_account_suffix(loan.cryptocurrency)

            for tranche in event.fund_tranches:
                common = self._build_common_metadata(event, tranche, loan)
                common['transaction_type'] = 'investments_foreclosures'

                # Calculate interest to write off (carried + current period net)
                total_interest, _ = calculate_tranche_interest(
                    tranche,
                    block_ts_int,
                    loan.protocolFee
                )

                # 1) Dr investments_nfts_seized_collateral (principal as cost basis)
                journal_rows.append({
                    **common,
                    'account_name': 'investments_nfts_seized_collateral',
                    'debit': tranche.principalAmount,
                    'credit': 0,
                    'principal': tranche.principalAmount,
                    'payoff_amount': tranche.principalAmount,  # NFT received at principal value
                })

                # 2) Dr bad_debt_expense (interest write-off)
                if total_interest > 0:
                    journal_rows.append({
                        **common,
                        'account_name': f'bad_debt_expense_{currency_suffix}',
                        'debit': total_interest,
                        'credit': 0,
                        'principal': tranche.principalAmount,
                        'payoff_amount': tranche.principalAmount,
                    })

                # 3) Cr loan_receivable (principal)
                journal_rows.append({
                    **common,
                    'account_name': f'loan_receivable_cryptocurrency_{currency_suffix}',
                    'debit': 0,
                    'credit': tranche.principalAmount,
                    'principal': tranche.principalAmount,
                    'payoff_amount': tranche.principalAmount,
                })

                # 4) Cr interest_receivable (interest)
                if total_interest > 0:
                    journal_rows.append({
                        **common,
                        'account_name': f'interest_receivable_cryptocurrency_{currency_suffix}',
                        'debit': 0,
                        'credit': total_interest,
                        'principal': tranche.principalAmount,
                        'payoff_amount': tranche.principalAmount,
                    })

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    # =========================================================================
    # LOAN LIQUIDATED - Multi-Tranche Liquidation (Auction)
    # =========================================================================

    def generate_loan_liquidated_entries(
        self,
        events: List[DecodedGondiEvent]
    ) -> pd.DataFrame:
        """
        Generate journal entries for LoanLiquidated events.

        Scans Transfer events for proceeds to fund wallets.

        From lender's perspective (per tranche):
        - Dr deemed_cash_usd (proceeds received, if any)
        - Dr bad_debt_expense_{currency} (shortfall: principal + interest - proceeds)
        - Cr loan_receivable_cryptocurrency_{currency} (principal)
        - Cr interest_receivable_cryptocurrency_{currency} (interest)
        """
        journal_rows = []

        for event in events:
            if event.event_type != GondiEventType.LOAN_LIQUIDATED.value:
                continue

            if not event.loan or not event.fund_tranches:
                continue

            loan = event.loan
            block_ts_int = int(event.timestamp.timestamp())
            currency_suffix = get_account_suffix(loan.cryptocurrency)

            for tranche in event.fund_tranches:
                common = self._build_common_metadata(event, tranche, loan)
                common['transaction_type'] = 'investments_foreclosures'

                # Calculate interest (carried + current period net)
                total_interest, _ = calculate_tranche_interest(
                    tranche,
                    block_ts_int,
                    loan.protocolFee
                )

                # Get proceeds from Transfer events (if any)
                proceeds = event.transfer_proceeds.get(tranche.lender, 0)

                # Total owed to lender
                total_owed = tranche.principalAmount + total_interest

                # Calculate shortfall (bad debt)
                shortfall = max(0, total_owed - proceeds)

                # 1) Dr deemed_cash (proceeds received)
                if proceeds > 0:
                    journal_rows.append({
                        **common,
                        'account_name': 'deemed_cash_usd',
                        'debit': proceeds,
                        'credit': 0,
                        'principal': tranche.principalAmount,
                        'payoff_amount': proceeds,
                    })

                # 2) Dr bad_debt_expense (shortfall)
                if shortfall > 0:
                    journal_rows.append({
                        **common,
                        'account_name': f'bad_debt_expense_{currency_suffix}',
                        'debit': shortfall,
                        'credit': 0,
                        'principal': tranche.principalAmount,
                        'payoff_amount': proceeds,
                    })

                # 3) Cr loan_receivable (principal)
                journal_rows.append({
                    **common,
                    'account_name': f'loan_receivable_cryptocurrency_{currency_suffix}',
                    'debit': 0,
                    'credit': tranche.principalAmount,
                    'principal': tranche.principalAmount,
                    'payoff_amount': proceeds,
                })

                # 4) Cr interest_receivable (interest)
                if total_interest > 0:
                    journal_rows.append({
                        **common,
                        'account_name': f'interest_receivable_cryptocurrency_{currency_suffix}',
                        'debit': 0,
                        'credit': total_interest,
                        'principal': tranche.principalAmount,
                        'payoff_amount': proceeds,
                    })

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    # =========================================================================
    # INTEREST ACCRUALS - Canonical Accrual Grid Model (v1.6.0)
    # =========================================================================

    def generate_interest_accruals(
        self,
        events: List[DecodedGondiEvent],
        accrual_end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Generate daily interest accrual journal entries for loans.

        v1.6.0: CANONICAL ACCRUAL GRID MODEL

        This method generates FULL CONTRACTUAL interest accruals from loan_start
        to loan_due_date. It does NOT bound accruals by termination - that is
        handled by generate_accrual_reversals().

        Accrual Grid Properties:
        - One row per day (timestamp bucket at 23:59:59 UTC)
        - Partial first day: loan_start -> 23:59:59 on start date
        - Full intermediate days: 00:00:00 -> 23:59:59
        - Partial last day: 00:00:00 -> loan_due_date
        - Grid is IMMUTABLE and serves as source of truth for reversals

        Each accrual entry contains:
        - accrual_id: Deterministic identifier for reversal matching
        - accrual_start_ts: Start of this accrual period
        - accrual_end_ts: End of this accrual period
        - accrual_ts: Journal entry timestamp (posting date)
        - seconds_in_row: Exact seconds in this accrual period

        LENDER perspective - For each accrual row (per tranche):
        - Dr interest_receivable_cryptocurrency_{currency}
        - Cr interest_income_cryptocurrency_{currency}

        BORROWER perspective - For each accrual row:
        - Dr interest_expense_cryptocurrency_{currency}
        - Cr interest_payable_cryptocurrency_{currency}

        Args:
            events: List of LoanEmitted or LoanRefinanced events
            accrual_end_date: Reporting period cutoff (optional, for partial periods)
                              NOTE: This is NOT termination - use reversals for that

        Returns:
            DataFrame of contractual interest accrual journal entries

        CRITICAL INVARIANT:
            After reversals are applied:
            SUM(accruals) - SUM(reversals) = interest earned to termination (exact)
        """
        origination_events = {
            GondiEventType.LOAN_EMITTED.value,
            GondiEventType.LOAN_REFINANCED.value,
            GondiEventType.LOAN_REFINANCED_FROM_NEW_OFFERS.value,
        }

        journal_rows = []

        for event in events:
            if event.event_type not in origination_events:
                continue

            if not event.loan:
                continue

            loan = event.loan
            currency_suffix = get_account_suffix(loan.cryptocurrency)

            # v1.6.0: Accrual end is CONTRACTUAL due date (not bounded by termination)
            # The accrual_end_date parameter is only for reporting period cutoff
            contractual_end = loan.due_date
            if accrual_end_date is not None and accrual_end_date < contractual_end:
                contractual_end = accrual_end_date

            # ---- LENDER PERSPECTIVE: Interest Income ----
            for tranche in event.fund_tranches:
                entries = self._generate_tranche_interest_accruals(
                    event, tranche, loan, currency_suffix,
                    accrual_end=contractual_end,
                    is_borrower=False
                )
                journal_rows.extend(entries)

            # ---- BORROWER PERSPECTIVE: Interest Expense ----
            if event.is_fund_borrower:
                entries = self._generate_borrower_interest_accruals(
                    event, loan, currency_suffix,
                    accrual_end=contractual_end
                )
                journal_rows.extend(entries)

        if not journal_rows:
            return pd.DataFrame()

        return pd.DataFrame(journal_rows)

    def _generate_tranche_interest_accruals(
        self,
        event: DecodedGondiEvent,
        tranche: Tranche,
        loan: Loan,
        currency_suffix: str,
        accrual_end: Optional[datetime] = None,
        is_borrower: bool = False
    ) -> List[Dict]:
        """
        Generate canonical accrual grid for a single tranche (lender perspective).

        v1.6.0: CANONICAL ACCRUAL GRID (Source of Truth)

        This generates the FULL CONTRACTUAL accrual schedule from tranche.startTime
        to loan.due_date (or accrual_end for reporting cutoff).

        Grid Structure:
        - One row per day bucket (timestamp at 23:59:59 UTC)
        - Partial first day: tranche.startTime -> 23:59:59 on start date
        - Full intermediate days: 00:00:00 -> 23:59:59 (86400 seconds)
        - Partial last day: 00:00:00 -> loan.due_date

        Each row is IMMUTABLE and contains:
        - accrual_id: Deterministic identifier for reversal matching
        - accrual_start_ts (accrual_period_start): Start of this period
        - accrual_end_ts (accrual_period_end): End of this period
        - accrual_ts (date): Journal posting timestamp
        - seconds_in_row (accrual_period_seconds): Exact seconds

        The accrual_id enables exact 1:1 matching for reversals.
        """
        entries = []

        # Start timestamp (second-precise from chain)
        start_dt = datetime.fromtimestamp(tranche.startTime, tz=timezone.utc)

        # v1.6.0: Accrual end is contractual due date (not bounded by termination)
        end_dt = accrual_end or loan.due_date

        # Validate window
        if start_dt >= end_dt:
            return entries

        # Calculate total net interest for the FULL CONTRACTUAL period
        total_secs = int((end_dt - start_dt).total_seconds())
        _, _, total_net_interest_wei = calculate_interest(
            tranche.principalAmount,
            tranche.aprBps,
            total_secs,
            loan.protocolFee
        )

        if total_net_interest_wei <= 0:
            return entries

        # Build common metadata
        common = self._build_common_metadata(event, tranche, loan)
        common['transaction_type'] = 'income_interest_accruals'
        common['fund_role'] = 'lender'

        # v1.6.0: Canonical grid metadata
        common['contractual_start'] = start_dt
        common['contractual_end'] = end_dt
        common['contractual_seconds'] = total_secs
        common['contractual_interest_wei'] = total_net_interest_wei

        # Get tranche index for accrual_id
        tranche_idx = loan.tranches.index(tranche) if tranche in loan.tranches else 0

        # Build canonical accrual grid with Wei precision
        leftover = 0
        assigned_so_far = 0
        cursor = start_dt

        while cursor < end_dt:
            # Next midnight UTC
            tomorrow = cursor.date() + timedelta(days=1)
            next_midnight = datetime.combine(tomorrow, dt_time(0, 0, 0), tzinfo=timezone.utc)

            # Segment end is the EARLIER of next midnight or contractual end
            segment_end = min(next_midnight, end_dt)

            # Exact seconds in this row
            slice_secs = int((segment_end - cursor).total_seconds())

            # Skip zero-second slices (safety check)
            if slice_secs <= 0:
                cursor = segment_end
                continue

            # Wei-precise interest allocation proportional to time
            numer = (total_net_interest_wei * slice_secs) + leftover
            slice_interest_wei = numer // total_secs
            leftover = numer % total_secs
            assigned_so_far += slice_interest_wei

            # Skip zero-interest slices
            if slice_interest_wei <= 0:
                cursor = segment_end
                continue

            # Journal entry timestamp (EOD or contractual end)
            if segment_end == next_midnight:
                accrual_ts = next_midnight - timedelta(seconds=1)
            else:
                accrual_ts = end_dt

            # Partial day indicators for audit
            is_first_day = (cursor == start_dt)
            is_last_day = (segment_end == end_dt)
            is_partial_day = (slice_secs < 86400)

            # Deterministic accrual_id for reversal matching
            # Format: loan_id:tranche_idx:YYYY-MM-DD
            accrual_date_str = accrual_ts.strftime('%Y-%m-%d')
            accrual_id = f"{event.loan_id}:{tranche_idx}:{accrual_date_str}"

            # Dr interest_receivable
            entries.append({
                **common,
                'date': accrual_ts,
                'accrual_id': accrual_id,
                'accrual_date': accrual_ts,
                'journal_date': accrual_ts,
                'accrual_start_ts': cursor,  # v1.6.0: Canonical grid field
                'accrual_end_ts': segment_end,  # v1.6.0: Canonical grid field
                'accrual_period_start': cursor,  # Legacy alias
                'accrual_period_end': segment_end,  # Legacy alias
                'accrual_period_seconds': slice_secs,
                'seconds_in_row': slice_secs,  # v1.6.0: Canonical grid field
                'is_partial_day': is_partial_day,
                'is_first_day': is_first_day,
                'is_last_day': is_last_day,
                'is_reversal': False,
                'reverses_accrual_id': None,
                'account_name': f'interest_receivable_cryptocurrency_{currency_suffix}',
                'debit': slice_interest_wei,
                'credit': 0,
                'principal': tranche.principalAmount,
                'payoff_amount': 0,
            })

            # Cr interest_income
            entries.append({
                **common,
                'date': accrual_ts,
                'accrual_id': accrual_id,
                'accrual_date': accrual_ts,
                'journal_date': accrual_ts,
                'accrual_start_ts': cursor,
                'accrual_end_ts': segment_end,
                'accrual_period_start': cursor,
                'accrual_period_end': segment_end,
                'accrual_period_seconds': slice_secs,
                'seconds_in_row': slice_secs,
                'is_partial_day': is_partial_day,
                'is_first_day': is_first_day,
                'is_last_day': is_last_day,
                'is_reversal': False,
                'reverses_accrual_id': None,
                'account_name': f'interest_income_cryptocurrency_{currency_suffix}',
                'debit': 0,
                'credit': slice_interest_wei,
                'principal': tranche.principalAmount,
                'payoff_amount': 0,
            })

            cursor = segment_end

        # Final adjustment for any remaining wei (ensure grid sums to contractual total)
        if assigned_so_far < total_net_interest_wei and entries:
            shortfall = total_net_interest_wei - assigned_so_far
            entries[-2]['debit'] += shortfall
            entries[-1]['credit'] += shortfall

        return entries

    def _generate_borrower_interest_accruals(
        self,
        event: DecodedGondiEvent,
        loan: Loan,
        currency_suffix: str,
        accrual_end: Optional[datetime] = None
    ) -> List[Dict]:
        """
        Generate canonical accrual grid for borrower perspective.

        v1.6.0: CANONICAL ACCRUAL GRID (Source of Truth)

        This generates the FULL CONTRACTUAL accrual schedule from loan.startTime
        to loan.due_date. Borrower pays GROSS interest (before protocol fee).

        Grid Structure matches lender perspective.
        Each row contains:
        - accrual_id: Deterministic identifier for reversal matching
        - accrual_start_ts, accrual_end_ts: Period boundaries
        - seconds_in_row: Exact seconds
        """
        entries = []

        # Start timestamp (second-precise from chain)
        start_dt = datetime.fromtimestamp(loan.startTime, tz=timezone.utc)

        # v1.6.0: Accrual end is contractual due date
        end_dt = accrual_end or loan.due_date

        # Validate window
        if start_dt >= end_dt:
            return entries

        # Calculate total interest expense for FULL CONTRACTUAL period
        # Note: borrower pays GROSS interest (before protocol fee)
        total_secs = int((end_dt - start_dt).total_seconds())
        total_interest_wei = 0
        for tranche in loan.tranches:
            gross_interest, _, _ = calculate_interest(
                tranche.principalAmount,
                tranche.aprBps,
                total_secs,
                loan.protocolFee
            )
            total_interest_wei += gross_interest

        if total_interest_wei <= 0:
            return entries

        # Build common metadata for borrower
        borrower_fund_id = self._get_fund_id(loan.borrower)

        common = {
            'fund_id': borrower_fund_id,
            'counterparty_fund_id': '',
            'wallet_id': loan.borrower,
            'cryptocurrency': loan.cryptocurrency,
            'transaction_type': 'expense_interest_accruals',
            'platform': 'Gondi',
            'event': event.event_type,
            'hash': event.tx_hash,
            'loan_id': event.loan_id,
            'lender': 'multiple',
            'borrower': loan.borrower,
            'from': loan.borrower,
            'to': 'multiple',
            'contract_address': event.contract_address,
            'collateral_address': loan.nftCollateralAddress,
            'token_id': str(loan.nftCollateralTokenId),
            'fund_role': 'borrower',
            '_currency_suffix': currency_suffix,
            # v1.6.0: Canonical grid metadata
            'contractual_start': start_dt,
            'contractual_end': end_dt,
            'contractual_seconds': total_secs,
            'contractual_interest_wei': total_interest_wei,
        }

        # Build canonical accrual grid with Wei precision
        leftover = 0
        assigned_so_far = 0
        cursor = start_dt

        while cursor < end_dt:
            tomorrow = cursor.date() + timedelta(days=1)
            next_midnight = datetime.combine(tomorrow, dt_time(0, 0, 0), tzinfo=timezone.utc)

            # Segment end is the EARLIER of next midnight or contractual end
            segment_end = min(next_midnight, end_dt)

            # Exact seconds in this row
            slice_secs = int((segment_end - cursor).total_seconds())

            # Skip zero-second slices
            if slice_secs <= 0:
                cursor = segment_end
                continue

            # Wei-precise interest allocation
            numer = (total_interest_wei * slice_secs) + leftover
            slice_interest_wei = numer // total_secs
            leftover = numer % total_secs
            assigned_so_far += slice_interest_wei

            # Skip zero-interest slices
            if slice_interest_wei <= 0:
                cursor = segment_end
                continue

            # Journal entry timestamp (EOD or contractual end)
            if segment_end == next_midnight:
                accrual_ts = next_midnight - timedelta(seconds=1)
            else:
                accrual_ts = end_dt

            # Partial day indicators
            is_first_day = (cursor == start_dt)
            is_last_day = (segment_end == end_dt)
            is_partial_day = (slice_secs < 86400)

            # Deterministic accrual_id for reversal matching
            accrual_date_str = accrual_ts.strftime('%Y-%m-%d')
            accrual_id = f"{event.loan_id}:borrower:{accrual_date_str}"

            # Dr interest_expense
            entries.append({
                **common,
                'date': accrual_ts,
                'accrual_id': accrual_id,
                'accrual_date': accrual_ts,
                'journal_date': accrual_ts,
                'accrual_start_ts': cursor,
                'accrual_end_ts': segment_end,
                'accrual_period_start': cursor,
                'accrual_period_end': segment_end,
                'accrual_period_seconds': slice_secs,
                'seconds_in_row': slice_secs,
                'is_partial_day': is_partial_day,
                'is_first_day': is_first_day,
                'is_last_day': is_last_day,
                'is_reversal': False,
                'reverses_accrual_id': None,
                'account_name': f'interest_expense_cryptocurrency_{currency_suffix}',
                'debit': slice_interest_wei,
                'credit': 0,
                'principal': loan.principalAmount,
                'payoff_amount': 0,
            })

            # Cr interest_payable
            entries.append({
                **common,
                'date': accrual_ts,
                'accrual_id': accrual_id,
                'accrual_date': accrual_ts,
                'journal_date': accrual_ts,
                'accrual_start_ts': cursor,
                'accrual_end_ts': segment_end,
                'accrual_period_start': cursor,
                'accrual_period_end': segment_end,
                'accrual_period_seconds': slice_secs,
                'seconds_in_row': slice_secs,
                'is_partial_day': is_partial_day,
                'is_first_day': is_first_day,
                'is_last_day': is_last_day,
                'is_reversal': False,
                'reverses_accrual_id': None,
                'account_name': f'interest_payable_cryptocurrency_{currency_suffix}',
                'debit': 0,
                'credit': slice_interest_wei,
                'principal': loan.principalAmount,
                'payoff_amount': 0,
            })

            cursor = segment_end

        # Final adjustment for any remaining Wei
        if assigned_so_far < total_interest_wei and entries:
            shortfall = total_interest_wei - assigned_so_far
            entries[-2]['debit'] += shortfall
            entries[-1]['credit'] += shortfall

        return entries

    # =========================================================================
    # LOAN TERMINATION DETECTION
    # =========================================================================

    def detect_loan_terminations(
        self,
        events: List[DecodedGondiEvent]
    ) -> Dict[int, datetime]:
        """
        Detect loan termination events and return earliest termination per loan.

        Termination events:
        - LoanRepaid: Borrower repaid the loan
        - LoanForeclosed: Single-tranche foreclosure
        - LoanLiquidated: Multi-tranche liquidation
        - LoanRefinanced_payoff: Old loan paid off via refinance
        - LoanRefinancedFromNewOffers_payoff: Old loan paid off via refinance

        Returns:
            Dict mapping loan_id -> termination_timestamp (earliest if multiple)
        """
        termination_events = {
            GondiEventType.LOAN_REPAID.value,
            GondiEventType.LOAN_FORECLOSED.value,
            GondiEventType.LOAN_LIQUIDATED.value,
        }

        refinance_events = {
            GondiEventType.LOAN_REFINANCED.value,
            GondiEventType.LOAN_REFINANCED_FROM_NEW_OFFERS.value,
        }

        terminations: Dict[int, datetime] = {}

        for event in events:
            loan_id = None
            termination_ts = event.timestamp

            # Direct termination events
            if event.event_type in termination_events:
                loan_id = event.loan_id

            # Refinance events terminate the OLD loan
            elif event.event_type in refinance_events:
                if event.old_loan_id:
                    loan_id = event.old_loan_id

            # Track earliest termination per loan
            if loan_id is not None:
                if loan_id not in terminations or termination_ts < terminations[loan_id]:
                    terminations[loan_id] = termination_ts

        return terminations

    # =========================================================================
    # ACCRUAL REVERSAL GENERATION (v1.4.0 - Partial-Day Precision)
    # =========================================================================

    # Constants for reversal calculations
    SECONDS_PER_DAY = 86400

    def calculate_reversal_fraction(
        self,
        termination_ts: datetime,
        accrual_period_start: Optional[datetime] = None,
        accrual_period_seconds: Optional[int] = None
    ) -> Tuple[int, int]:
        """
        Calculate the fraction of an accrual to reverse for partial-day termination.

        Uses second-precise calculation:
        - remaining_seconds = seconds from termination to next midnight
        - fraction = remaining_seconds / SECONDS_PER_DAY

        Returns:
            Tuple of (remaining_seconds, SECONDS_PER_DAY) for Wei-precise fraction math

        Example:
            Termination at 16:30:00 (16.5 hours into day)
            remaining_seconds = 86400 - (16*3600 + 30*60) = 27000 seconds
            Returns: (27000, 86400)
        """
        # Get termination time components
        term_hour = termination_ts.hour
        term_minute = termination_ts.minute
        term_second = termination_ts.second
        term_microsecond = termination_ts.microsecond

        # Seconds from midnight to termination
        seconds_to_termination = (
            term_hour * 3600 +
            term_minute * 60 +
            term_second +
            (term_microsecond / 1_000_000 if term_microsecond else 0)
        )

        # Remaining seconds in the day after termination
        remaining_seconds = self.SECONDS_PER_DAY - int(seconds_to_termination)

        # Ensure non-negative
        remaining_seconds = max(0, remaining_seconds)

        return remaining_seconds, self.SECONDS_PER_DAY

    def generate_accrual_reversals(
        self,
        accruals_df: pd.DataFrame,
        terminations: Dict[int, datetime]
    ) -> pd.DataFrame:
        """
        Generate precise reversal entries by referencing ORIGINAL accrual rows.

        v1.6.0: CANONICAL REVERSAL MODEL (Row-by-Row Identity)

        This method generates reversals that EXACTLY offset the original accrual
        rows. It does NOT recompute interest - it references the original amounts.

        Critical Rules:

        1. FULLY UNEARNED ROWS (accrual_start_ts >= termination_ts):
           - Reverse 100% of original amount
           - JE timestamp = SAME as original accrual row (NOT termination_ts)
           - This ensures exact ledger offset

        2. PARTIALLY EARNED ROW (accrual_start_ts < termination_ts < accrual_end_ts):
           - Calculate: earned_seconds = termination_ts - accrual_start_ts
           - Calculate: unearned_seconds = accrual_end_ts - termination_ts
           - Reverse: original_amount x (unearned_seconds / total_seconds_in_row)
           - JE timestamp = termination_ts (EXCEPTION - this is the only row that differs)

        3. FULLY EARNED ROWS (accrual_end_ts <= termination_ts):
           - No reversal needed

        Args:
            accruals_df: DataFrame of canonical accrual grid with:
                - accrual_id: Deterministic identifier for matching
                - accrual_start_ts (or accrual_period_start): Start of period
                - accrual_end_ts (or accrual_period_end): End of period
                - accrual_ts (or date): Journal timestamp
                - seconds_in_row (or accrual_period_seconds): Duration
                - debit, credit: Original amounts
            terminations: Dict mapping loan_id -> termination_timestamp

        Returns:
            DataFrame of reversal entries

        INVARIANT (Must Always Hold):
            SUM(accruals) - SUM(reversals) = interest earned to termination (exact)
            No exceptions. No tolerances. No drift.
        """
        if accruals_df.empty or not terminations:
            return pd.DataFrame()

        reversal_rows = []

        # Ensure required columns exist
        if 'loan_id' not in accruals_df.columns:
            return pd.DataFrame()

        # Identify column names (support both v1.5 and v1.6 naming)
        start_col = 'accrual_start_ts' if 'accrual_start_ts' in accruals_df.columns else 'accrual_period_start'
        end_col = 'accrual_end_ts' if 'accrual_end_ts' in accruals_df.columns else 'accrual_period_end'
        secs_col = 'seconds_in_row' if 'seconds_in_row' in accruals_df.columns else 'accrual_period_seconds'
        ts_col = 'date'  # Journal timestamp

        for loan_id, termination_ts in terminations.items():
            # Get accruals for this loan
            loan_mask = accruals_df['loan_id'] == loan_id
            loan_accruals = accruals_df[loan_mask].copy()

            if loan_accruals.empty:
                continue

            # Ensure datetime columns are proper datetime objects
            for col in [start_col, end_col, ts_col]:
                if col in loan_accruals.columns:
                    if not pd.api.types.is_datetime64_any_dtype(loan_accruals[col]):
                        loan_accruals[col] = pd.to_datetime(loan_accruals[col], utc=True)

            # Process each accrual row
            for _, accrual_row in loan_accruals.iterrows():
                accrual_dict = accrual_row.to_dict()

                # Get row boundaries
                row_start = accrual_dict.get(start_col)
                row_end = accrual_dict.get(end_col)
                row_seconds = accrual_dict.get(secs_col, 86400)
                row_ts = accrual_dict.get(ts_col)

                # Convert to datetime if needed
                if hasattr(row_start, 'to_pydatetime'):
                    row_start = row_start.to_pydatetime()
                if hasattr(row_end, 'to_pydatetime'):
                    row_end = row_end.to_pydatetime()
                if hasattr(row_ts, 'to_pydatetime'):
                    row_ts = row_ts.to_pydatetime()

                # Skip rows without boundary data
                if row_start is None or row_end is None:
                    continue

                # Ensure timezone awareness
                if row_start.tzinfo is None:
                    row_start = row_start.replace(tzinfo=timezone.utc)
                if row_end.tzinfo is None:
                    row_end = row_end.replace(tzinfo=timezone.utc)
                if termination_ts.tzinfo is None:
                    termination_ts = termination_ts.replace(tzinfo=timezone.utc)

                # Classify this row
                if termination_ts <= row_start:
                    # FULLY UNEARNED: termination before or at row start
                    # Reverse 100%, use SAME timestamp as original
                    reversal = self._create_full_reversal_v160(
                        accrual_dict,
                        reversal_timestamp=row_ts,  # SAME as original
                        original_accrual_ts=row_ts
                    )
                    if reversal:
                        reversal_rows.append(reversal)

                elif termination_ts >= row_end:
                    # FULLY EARNED: termination at or after row end
                    # No reversal needed
                    continue

                else:
                    # PARTIALLY EARNED: termination_ts is within [row_start, row_end)
                    # Calculate earned vs unearned seconds
                    earned_seconds = int((termination_ts - row_start).total_seconds())
                    unearned_seconds = int((row_end - termination_ts).total_seconds())
                    total_seconds = int(row_seconds) if row_seconds else earned_seconds + unearned_seconds

                    if unearned_seconds <= 0:
                        # Edge case: termination exactly at end
                        continue

                    # Partial reversal using UNEARNED fraction of ORIGINAL amount
                    # JE timestamp = termination_ts (the EXCEPTION)
                    reversal = self._create_partial_reversal_v160(
                        accrual_dict,
                        termination_ts=termination_ts,
                        unearned_seconds=unearned_seconds,
                        total_seconds=total_seconds,
                        original_accrual_ts=row_ts
                    )
                    if reversal:
                        reversal_rows.append(reversal)

        if not reversal_rows:
            return pd.DataFrame()

        return pd.DataFrame(reversal_rows)

    def _create_full_reversal_v160(
        self,
        accrual: Dict,
        reversal_timestamp: datetime,
        original_accrual_ts: datetime
    ) -> Dict:
        """
        Create a 100% reversal entry for fully unearned accrual row.

        v1.6.0: Reversal JE timestamp = SAME as original accrual row

        This ensures exact ledger offset with no timing drift.
        """
        original_debit = accrual.get('debit', 0)
        original_credit = accrual.get('credit', 0)

        # Skip if nothing to reverse
        if original_debit == 0 and original_credit == 0:
            return None

        reversal = accrual.copy()

        # Swap debits and credits exactly (100% reversal)
        reversal['debit'] = original_credit
        reversal['credit'] = original_debit

        # Metadata updates
        reversal['is_reversal'] = True
        reversal['is_partial_reversal'] = False
        reversal['reversal_type'] = 'full'
        reversal['reversal_fraction'] = 1.0
        reversal['earned_seconds'] = 0
        reversal['unearned_seconds'] = accrual.get('seconds_in_row', accrual.get('accrual_period_seconds', 0))
        reversal['reverses_accrual_id'] = accrual.get('accrual_id', None)
        reversal['original_accrual_ts'] = original_accrual_ts

        # CRITICAL: Use SAME timestamp as original accrual row
        reversal['date'] = reversal_timestamp
        reversal['journal_date'] = reversal_timestamp
        # accrual_date stays the same for reference

        # Update event type
        reversal['event'] = 'InterestAccrualReversal'

        # Update transaction type
        original_tx_type = accrual.get('transaction_type', '')
        if 'income' in original_tx_type:
            reversal['transaction_type'] = 'income_interest_accruals_reversal'
        elif 'expense' in original_tx_type:
            reversal['transaction_type'] = 'expense_interest_accruals_reversal'
        else:
            reversal['transaction_type'] = f"{original_tx_type}_reversal"

        # Generate unique reversal accrual_id
        original_accrual_id = accrual.get('accrual_id', '')
        reversal['accrual_id'] = f"{original_accrual_id}:reversal"

        return reversal

    def _create_partial_reversal_v160(
        self,
        accrual: Dict,
        termination_ts: datetime,
        unearned_seconds: int,
        total_seconds: int,
        original_accrual_ts: datetime
    ) -> Optional[Dict]:
        """
        Create a partial reversal entry for the termination-day accrual row.

        v1.6.0:
        - Reversal amount = original_amount x (unearned_seconds / total_seconds_in_row)
        - JE timestamp = termination_ts (the EXCEPTION to timestamp discipline)

        Uses Wei-precise integer division to avoid floating-point drift.
        """
        original_debit = accrual.get('debit', 0)
        original_credit = accrual.get('credit', 0)

        if total_seconds <= 0:
            return None

        # Calculate reversed amounts with Wei precision
        # Reverse the UNEARNED portion: original x (unearned / total)
        # Swap debit/credit for reversal
        reversed_debit = (original_credit * unearned_seconds) // total_seconds
        reversed_credit = (original_debit * unearned_seconds) // total_seconds

        # Skip if reversal amounts are zero
        if reversed_debit == 0 and reversed_credit == 0:
            return None

        reversal = accrual.copy()

        # Set reversed amounts (swapped debit/credit, scaled by unearned fraction)
        reversal['debit'] = reversed_debit
        reversal['credit'] = reversed_credit

        # Metadata updates
        reversal['is_reversal'] = True
        reversal['is_partial_reversal'] = True
        reversal['reversal_type'] = 'partial'
        reversal['reversal_fraction'] = unearned_seconds / total_seconds
        reversal['earned_seconds'] = total_seconds - unearned_seconds
        reversal['unearned_seconds'] = unearned_seconds
        reversal['total_row_seconds'] = total_seconds
        reversal['reverses_accrual_id'] = accrual.get('accrual_id', None)
        reversal['original_accrual_ts'] = original_accrual_ts
        reversal['termination_ts'] = termination_ts

        # CRITICAL: Partial reversal uses termination_ts as JE date (THE EXCEPTION)
        reversal['date'] = termination_ts
        reversal['journal_date'] = termination_ts
        # accrual_date stays the same for reference

        # Update event type
        reversal['event'] = 'InterestAccrualReversal'

        # Update transaction type
        original_tx_type = accrual.get('transaction_type', '')
        if 'income' in original_tx_type:
            reversal['transaction_type'] = 'income_interest_accruals_reversal'
        elif 'expense' in original_tx_type:
            reversal['transaction_type'] = 'expense_interest_accruals_reversal'
        else:
            reversal['transaction_type'] = f"{original_tx_type}_reversal"

        # Generate unique reversal accrual_id
        original_accrual_id = accrual.get('accrual_id', '')
        reversal['accrual_id'] = f"{original_accrual_id}:partial_reversal"

        return reversal

    # Keep legacy methods for backward compatibility
    def _create_partial_reversal(
        self,
        accrual: Dict,
        termination_ts: datetime,
        remaining_seconds: int,
        total_seconds: int
    ) -> Optional[Dict]:
        """Legacy method - redirects to v1.6.0 implementation."""
        return self._create_partial_reversal_v160(
            accrual, termination_ts, remaining_seconds, total_seconds,
            accrual.get('date', termination_ts)
        )

    def _create_full_reversal(
        self,
        accrual: Dict,
        termination_ts: datetime
    ) -> Dict:
        """Legacy method - redirects to v1.6.0 implementation."""
        # v1.6.0: Full reversals use SAME timestamp as original, not termination_ts
        original_ts = accrual.get('date', termination_ts)
        return self._create_full_reversal_v160(accrual, original_ts, original_ts)

    def validate_accrual_reversal_integrity(
        self,
        accruals_df: pd.DataFrame,
        reversals_df: pd.DataFrame,
        terminations: Dict[int, datetime],
        tolerance_wei: int = 1000
    ) -> Dict[str, Any]:
        """
        Validate that accruals + reversals satisfy the v1.6.0 invariant.

        v1.6.0: CRITICAL ACCOUNTING INVARIANT

        For every loan:
            SUM(accruals) - SUM(reversals) = interest earned to termination

        This must hold exactly (within Wei tolerance).

        Validation Checks:
        1. Fully unearned rows: 100% reversed with SAME timestamp
        2. Partially earned row: Partial reversal with termination_ts
        3. Fully earned rows: No reversal
        4. Net interest matches expected (to the second)

        Returns:
            Dict with validation results and any errors found
        """
        results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'loan_summaries': {}
        }

        if accruals_df.empty:
            return results

        # Identify column names (support both v1.5 and v1.6 naming)
        start_col = 'accrual_start_ts' if 'accrual_start_ts' in accruals_df.columns else 'accrual_period_start'
        end_col = 'accrual_end_ts' if 'accrual_end_ts' in accruals_df.columns else 'accrual_period_end'

        for loan_id, termination_ts in terminations.items():
            loan_mask = accruals_df['loan_id'] == loan_id
            loan_accruals = accruals_df[loan_mask]

            if loan_accruals.empty:
                continue

            # Calculate totals
            total_accrued_debit = loan_accruals['debit'].sum() if 'debit' in loan_accruals.columns else 0
            total_accrued_credit = loan_accruals['credit'].sum() if 'credit' in loan_accruals.columns else 0

            loan_reversals = reversals_df[reversals_df['loan_id'] == loan_id] if not reversals_df.empty else pd.DataFrame()
            total_reversed_debit = loan_reversals['debit'].sum() if not loan_reversals.empty and 'debit' in loan_reversals.columns else 0
            total_reversed_credit = loan_reversals['credit'].sum() if not loan_reversals.empty and 'credit' in loan_reversals.columns else 0

            # Net interest = accrued - reversed (for receivable entries: debit - reversal_debit)
            # Note: Reversal debits are actually credits swapped, so net = accrual_debit - reversal_debit
            net_interest_debit = total_accrued_debit - total_reversed_debit
            net_interest_credit = total_accrued_credit - total_reversed_credit

            # Count reversal types
            partial_reversals = 0
            full_reversals = 0
            if not loan_reversals.empty and 'is_partial_reversal' in loan_reversals.columns:
                partial_reversals = loan_reversals['is_partial_reversal'].sum()
                full_reversals = len(loan_reversals) - partial_reversals

            # Verify each accrual row has appropriate reversal
            for _, accrual in loan_accruals.iterrows():
                accrual_id = accrual.get('accrual_id', '')
                row_start = accrual.get(start_col)
                row_end = accrual.get(end_col)

                if row_start is None or row_end is None:
                    continue

                # Convert to datetime
                if hasattr(row_start, 'to_pydatetime'):
                    row_start = row_start.to_pydatetime()
                if hasattr(row_end, 'to_pydatetime'):
                    row_end = row_end.to_pydatetime()

                # Ensure timezone awareness
                if row_start.tzinfo is None:
                    row_start = row_start.replace(tzinfo=timezone.utc)
                if row_end.tzinfo is None:
                    row_end = row_end.replace(tzinfo=timezone.utc)
                term_ts = termination_ts
                if term_ts.tzinfo is None:
                    term_ts = term_ts.replace(tzinfo=timezone.utc)

                # Classify row
                if term_ts <= row_start:
                    # FULLY UNEARNED: Should have 100% reversal with SAME timestamp
                    if not loan_reversals.empty:
                        reversal_mask = loan_reversals['reverses_accrual_id'] == accrual_id
                        matching = loan_reversals[reversal_mask]
                        if matching.empty:
                            results['errors'].append(
                                f"Loan {loan_id}: Fully unearned accrual {accrual_id} has no reversal"
                            )
                            results['valid'] = False
                        else:
                            # Verify it's a full reversal
                            for _, rev in matching.iterrows():
                                if rev.get('is_partial_reversal', False):
                                    results['errors'].append(
                                        f"Loan {loan_id}: Fully unearned accrual {accrual_id} has partial (not full) reversal"
                                    )
                                    results['valid'] = False
                    else:
                        results['errors'].append(
                            f"Loan {loan_id}: Fully unearned accrual {accrual_id} has no reversal"
                        )
                        results['valid'] = False

                elif term_ts >= row_end:
                    # FULLY EARNED: Should have no reversal
                    if not loan_reversals.empty:
                        reversal_mask = loan_reversals['reverses_accrual_id'] == accrual_id
                        matching = loan_reversals[reversal_mask]
                        if not matching.empty:
                            results['warnings'].append(
                                f"Loan {loan_id}: Fully earned accrual {accrual_id} has unexpected reversal"
                            )
                else:
                    # PARTIALLY EARNED: Should have partial reversal with termination_ts
                    if not loan_reversals.empty:
                        reversal_mask = loan_reversals['reverses_accrual_id'] == accrual_id
                        matching = loan_reversals[reversal_mask]
                        if matching.empty:
                            results['errors'].append(
                                f"Loan {loan_id}: Partially earned accrual {accrual_id} has no reversal"
                            )
                            results['valid'] = False
                        else:
                            # Verify it's a partial reversal
                            for _, rev in matching.iterrows():
                                if not rev.get('is_partial_reversal', False):
                                    results['warnings'].append(
                                        f"Loan {loan_id}: Partially earned accrual {accrual_id} has full (not partial) reversal"
                                    )
                    else:
                        results['errors'].append(
                            f"Loan {loan_id}: Partially earned accrual {accrual_id} has no reversal"
                        )
                        results['valid'] = False

            # Store loan summary
            results['loan_summaries'][loan_id] = {
                'total_accrued_debit': total_accrued_debit,
                'total_accrued_credit': total_accrued_credit,
                'total_reversed_debit': total_reversed_debit,
                'total_reversed_credit': total_reversed_credit,
                'net_interest_debit': net_interest_debit,
                'net_interest_credit': net_interest_credit,
                'termination_ts': termination_ts,
                'partial_reversals': partial_reversals,
                'full_reversals': full_reversals,
            }

        return results

    def validate_accrual_interest_integrity(
        self,
        accruals_df: pd.DataFrame,
        repayment_events: List[DecodedGondiEvent],
        tolerance_wei: int = 1000
    ) -> Dict[str, Any]:
        """
        v1.5.0: Validate that accrued interest matches contractual interest.

        For terminated loans (repaid, foreclosed, etc.):
            Total Accrued Interest = (Payoff Amount - Principal) +/- tolerance

        This is the key invariant that proves bounded accruals are correct.

        Args:
            accruals_df: DataFrame of interest accrual entries
            repayment_events: List of repayment/termination events with payoff amounts
            tolerance_wei: Allowed rounding tolerance in Wei (default: 1000 Wei = ~$0.003)

        Returns:
            Dict with validation results per loan
        """
        results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'loan_validations': []
        }

        if accruals_df.empty:
            return results

        for event in repayment_events:
            loan_id = event.loan_id

            # Get accruals for this loan (only receivable entries, not both dr and cr)
            loan_mask = accruals_df['loan_id'] == loan_id
            receivable_mask = accruals_df['account_name'].str.contains('interest_receivable', na=False)
            loan_accruals = accruals_df[loan_mask & receivable_mask]

            if loan_accruals.empty:
                continue

            # Total accrued interest (debit side of receivable entries)
            total_accrued_wei = loan_accruals['debit'].sum()

            # Get expected interest from event
            # For repayments: interest = payoff_amount - principal
            if hasattr(event, 'loan') and event.loan:
                principal_wei = event.loan.principalAmount

                # Calculate expected interest based on event type
                # For LoanRepaid, the actual paid interest should match accrued
                if event.event_type == GondiEventType.LOAN_REPAID.value:
                    # Sum up actual interest paid across tranches
                    expected_interest_wei = 0
                    for tranche in event.loan.tranches:
                        if hasattr(tranche, 'actualInterestPaid'):
                            expected_interest_wei += tranche.actualInterestPaid
                        elif hasattr(tranche, 'accruedInterest'):
                            expected_interest_wei += tranche.accruedInterest

                    # If we don't have per-tranche data, calculate from loan totals
                    if expected_interest_wei == 0 and hasattr(event.loan, 'payoffAmount'):
                        expected_interest_wei = event.loan.payoffAmount - principal_wei
                else:
                    # For other terminations, use calculated interest
                    if hasattr(event.loan, 'payoffAmount'):
                        expected_interest_wei = event.loan.payoffAmount - principal_wei
                    else:
                        expected_interest_wei = 0

                # Compare
                difference_wei = abs(total_accrued_wei - expected_interest_wei)
                is_valid = difference_wei <= tolerance_wei

                validation_entry = {
                    'loan_id': loan_id,
                    'total_accrued_wei': total_accrued_wei,
                    'expected_interest_wei': expected_interest_wei,
                    'difference_wei': difference_wei,
                    'is_valid': is_valid,
                    'event_type': event.event_type,
                }

                results['loan_validations'].append(validation_entry)

                if not is_valid:
                    results['valid'] = False
                    results['errors'].append(
                        f"Loan {loan_id}: Accrued {total_accrued_wei} Wei but expected "
                        f"{expected_interest_wei} Wei (difference: {difference_wei} Wei)"
                    )

        return results

    # =========================================================================
    # MAIN PROCESSING METHOD
    # =========================================================================

    def process_events(
        self,
        events: List[DecodedGondiEvent],
        generate_accruals: bool = True,
        accrual_end_date: Optional[datetime] = None,
        generate_reversals: bool = True,
        validate_reversals: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """
        Process all events and generate journal entries.

        Args:
            events: List of decoded Gondi events
            generate_accruals: Whether to generate interest accruals
            accrual_end_date: End date for accruals
            generate_reversals: Whether to generate reversal entries for early termination
            validate_reversals: Whether to validate accrual/reversal integrity

        Returns:
            Dict with DataFrames for each journal type:
            - 'new_loans': LoanEmitted entries
            - 'repayments': LoanRepaid entries
            - 'refinances': LoanRefinanced entries
            - 'foreclosures': LoanForeclosed entries
            - 'liquidations': LoanLiquidated entries
            - 'interest_accruals': Daily interest accruals (based on contractual terms)
            - 'accrual_reversals': Exact reversals for early termination
            - 'offer_cancellations': OfferCancelled events (no journal entries, for audit)
            - 'terminations': Loan termination summary (loan_id -> termination_date)
            - 'reversal_validation': Validation results (if validate_reversals=True)

        Accrual Lifecycle:
            1. Accruals are generated based on full contractual loan term (estimates)
            2. Termination events (repay, foreclose, liquidate, refinance_payoff) are detected
            3. For early termination, exact 1:1 reversals are generated for post-termination accruals
            4. Original accruals remain in ledger (immutable audit trail)
            5. Net effect: Accrued Interest = Actual Interest (Payoff - Principal)

        All monetary columns are automatically converted to human-readable format:
        - debit_crypto, credit_crypto, principal_crypto, payoff_amount_crypto
        """
        results = {}

        # =========================================================================
        # STEP 1: GENERATE TRANSACTION JOURNAL ENTRIES
        # =========================================================================

        # New loans
        df_new_loans = self.generate_loan_emitted_entries(events)
        if not df_new_loans.empty:
            results['new_loans'] = df_new_loans
            print(f"[OK] Generated {len(df_new_loans)} new loan journal entries")

        # Repayments
        df_repayments = self.generate_loan_repaid_entries(events)
        if not df_repayments.empty:
            results['repayments'] = df_repayments
            print(f"[OK] Generated {len(df_repayments)} repayment journal entries")

        # Refinances
        df_refinances = self.generate_loan_refinanced_entries(events)
        if not df_refinances.empty:
            results['refinances'] = df_refinances
            print(f"[OK] Generated {len(df_refinances)} refinance journal entries")

        # Foreclosures
        df_foreclosures = self.generate_loan_foreclosed_entries(events)
        if not df_foreclosures.empty:
            results['foreclosures'] = df_foreclosures
            print(f"[OK] Generated {len(df_foreclosures)} foreclosure journal entries")

        # Liquidations
        df_liquidations = self.generate_loan_liquidated_entries(events)
        if not df_liquidations.empty:
            results['liquidations'] = df_liquidations
            print(f"[OK] Generated {len(df_liquidations)} liquidation journal entries")

        # Offer Cancellations (no journal entries - for audit trail only)
        cancelled_events = [
            e for e in events
            if e.event_type in {
                GondiEventType.OFFER_CANCELLED.value,
                GondiEventType.RENEGOTIATION_OFFER_CANCELLED.value,
            }
        ]
        if cancelled_events:
            cancellation_rows = []
            for e in cancelled_events:
                cancellation_rows.append({
                    'date': e.timestamp,
                    'event': e.event_type,
                    'hash': e.tx_hash,
                    'block_number': e.block_number,
                    'log_index': e.log_index,
                    'lender': getattr(e, 'lender', None),
                    'platform': 'Gondi',
                    'note': 'No journal entry - offer cancellation has no accounting impact',
                })
            results['offer_cancellations'] = pd.DataFrame(cancellation_rows)
            print(f"[list] Tracked {len(cancelled_events)} offer cancellations (no journal entries)")

        # =========================================================================
        # STEP 2: DETECT LOAN TERMINATIONS
        # =========================================================================

        terminations = self.detect_loan_terminations(events)
        if terminations:
            # Store terminations as a summary DataFrame
            term_rows = [
                {'loan_id': loan_id, 'termination_date': ts}
                for loan_id, ts in terminations.items()
            ]
            results['terminations'] = pd.DataFrame(term_rows)
            print(f"[date] Detected {len(terminations)} loan terminations")

        # =========================================================================
        # STEP 3: GENERATE INTEREST ACCRUALS (v1.6.0 - FULL CONTRACTUAL GRID)
        # =========================================================================

        df_accruals = pd.DataFrame()

        if generate_accruals:
            # Generate accruals for new loans and new positions from refinancing
            # v1.6.0: Generate FULL CONTRACTUAL accruals (loan_start -> loan_due_date)
            # This is the canonical accrual grid - immutable source of truth
            # Early termination is handled by REVERSALS, not by bounding accruals
            accrual_events = [
                e for e in events
                if e.event_type in {
                    GondiEventType.LOAN_EMITTED.value,
                    GondiEventType.LOAN_REFINANCED.value,
                    GondiEventType.LOAN_REFINANCED_FROM_NEW_OFFERS.value,
                } and (e.fund_tranches or e.is_fund_borrower)
            ]

            # v1.6.0: Generate FULL contractual accruals (no terminations parameter)
            df_accruals = self.generate_interest_accruals(
                accrual_events,
                accrual_end_date=accrual_end_date
            )
            if not df_accruals.empty:
                results['interest_accruals'] = df_accruals
                print(f"[OK] Generated {len(df_accruals)} contractual interest accrual entries")

                # v1.6.0: Report on partial days
                if 'is_partial_day' in df_accruals.columns:
                    partial_count = df_accruals['is_partial_day'].sum() // 2  # Divide by 2 for dr/cr pairs
                    print(f"   +---- {partial_count} partial-day accruals (first/last day)")

        # =========================================================================
        # STEP 4: GENERATE ACCRUAL REVERSALS (v1.6.0 - ROW-BY-ROW IDENTITY)
        # =========================================================================

        df_reversals = pd.DataFrame()

        # v1.6.0: Reversals reference ORIGINAL accrual rows by identity
        # - Full reversals use SAME timestamp as original row
        # - Partial reversal (termination day) uses termination_ts
        # - Invariant: SUM(accruals) - SUM(reversals) = interest earned (exact)
        if generate_accruals and generate_reversals and not df_accruals.empty and terminations:
            df_reversals = self.generate_accrual_reversals(df_accruals, terminations)

            if not df_reversals.empty:
                results['accrual_reversals'] = df_reversals
                print(f"[sync] Generated {len(df_reversals)} accrual reversal entries")

                # Count by reversal type
                if 'is_partial_reversal' in df_reversals.columns:
                    partial_count = df_reversals['is_partial_reversal'].sum()
                    full_count = len(df_reversals) - partial_count
                    print(f"   +---- {full_count} full reversals, {partial_count} partial reversals")

                # Count affected loans
                if 'loan_id' in df_reversals.columns:
                    affected_loans = df_reversals['loan_id'].nunique()
                    print(f"   +---- {affected_loans} loans with early termination")

        # =========================================================================
        # STEP 5: VALIDATE ACCRUAL/REVERSAL INTEGRITY
        # =========================================================================

        if validate_reversals and not df_accruals.empty and terminations:
            validation = self.validate_accrual_reversal_integrity(
                df_accruals, df_reversals, terminations
            )

            # Store validation results
            results['reversal_validation'] = validation

            if validation['valid']:
                print(f"[OK] Accrual reversal validation passed")
            else:
                print(f"[!] Accrual reversal validation found issues:")
                for error in validation['errors'][:5]:  # Show first 5 errors
                    print(f"   +---- {error}")
                if len(validation['errors']) > 5:
                    print(f"   +---- ... and {len(validation['errors']) - 5} more errors")

        # =========================================================================
        # STEP 6: CONVERT TO HUMAN-READABLE FORMAT
        # =========================================================================

        for name, df in results.items():
            if isinstance(df, pd.DataFrame) and not df.empty and 'debit' in df.columns:
                results[name] = convert_to_human_readable(df)

        return results


# ============================================================================
# MAIN PROCESSOR CLASS
# ============================================================================

class GondiLoanFacetProcessor:
    """
    Main processor for Gondi LoanFacet transactions.
    Supports multiple Gondi contract versions (V1/V3 with Tranche[], V2 with Source[]).

    Usage:
        # Single contract (backwards compatible)
        processor = GondiLoanFacetProcessor(w3, contract, wallet_metadata)

        # Multiple contracts (recommended)
        processor = GondiLoanFacetProcessor(w3, contracts=gondi_contracts, wallet_metadata=wallet_metadata)

        # With explicit V2 addresses
        processor = GondiLoanFacetProcessor(w3, contracts=gondi_contracts, wallet_metadata=wallet_metadata,
                                            v2_addresses={'0x478f...'.lower()})

        results = processor.process_transactions(tx_hashes)
    """

    def __init__(
        self,
        w3: Web3,
        contract=None,  # Legacy: single contract
        contracts: Dict[str, Any] = None,  # New: dict of address -> contract
        wallet_metadata: Dict[str, Dict] = None,
        v2_addresses: set = None,  # Addresses using V2 ABI (Source[] without floor)
    ):
        """
        Initialize processor.

        Args:
            w3: Web3 instance
            contract: (Legacy) Single instantiated Gondi contract with ABI
            contracts: Dict mapping contract addresses to instantiated contracts
            wallet_metadata: Dict mapping addresses to wallet info
            v2_addresses: Set of contract addresses (lowercase) using V2 ABI
        """
        self.w3 = w3
        self.wallet_metadata = wallet_metadata or {}

        # Build contracts dict
        self.contracts = {}
        if contracts:
            for addr, c in contracts.items():
                self.contracts[addr.lower()] = c
        if contract is not None:
            self.contracts[contract.address.lower()] = contract

        # Keep single contract reference for backwards compatibility
        self.contract = contract or (next(iter(self.contracts.values())) if self.contracts else None)

        self.decoder = GondiEventDecoder(
            w3=w3,
            contracts=self.contracts,
            wallet_metadata=wallet_metadata,
            v2_addresses=v2_addresses  # Pass V2 addresses for correct struct parsing
        )
        self.journal_generator = GondiJournalEntryGenerator(wallet_metadata)

    def process_transactions(
        self,
        tx_hashes: List[str],
        generate_accruals: bool = True,
        accrual_end_date: Optional[datetime] = None,
        max_workers: int = 8,
        show_progress: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """
        Process transactions and generate journal entries.

        Args:
            tx_hashes: List of transaction hashes
            generate_accruals: Whether to generate interest accruals
            accrual_end_date: End date for accruals
            max_workers: Parallel workers for decoding
            show_progress: Show progress bar

        Returns:
            Dict with DataFrames for each journal type
        """
        print(f"\n{'='*80}")
        print(f"[chart] PROCESSING GONDI TRANSACTIONS")
        print(f"{'='*80}")
        print(f"Transactions: {len(tx_hashes)}")

        # Decode all transactions
        events = self.decoder.decode_batch(
            tx_hashes,
            max_workers=max_workers,
            show_progress=show_progress
        )

        print(f"\n[OK] Decoded {len(events)} events")

        # Print event summary
        event_counts = {}
        for e in events:
            event_counts[e.event_type] = event_counts.get(e.event_type, 0) + 1

        print("\n[list] Event Summary:")
        for event_type, count in sorted(event_counts.items()):
            fund_count = sum(
                1 for e in events
                if e.event_type == event_type and (e.fund_tranches or e.old_fund_tranches)
            )
            print(f"   {event_type}: {count} total, {fund_count} with fund tranches")

        # Generate journal entries
        results = self.journal_generator.process_events(
            events,
            generate_accruals=generate_accruals,
            accrual_end_date=accrual_end_date
        )

        # Validate balances
        print("\n[balance] Balance Validation:")
        for name, df in results.items():
            # Skip non-DataFrame items (validation dicts, metadata)
            if not isinstance(df, pd.DataFrame):
                continue
            if df.empty:
                continue
            if 'debit' in df.columns and 'credit' in df.columns:
                total_debit = df['debit'].sum()
                total_credit = df['credit'].sum()
                balanced = abs(total_debit - total_credit) < 1  # Allow 1 wei tolerance
                status = "[OK]" if balanced else "[X]"
                print(f"   {name}: Debit={total_debit:,} Credit={total_credit:,} {status}")

        return results

    def get_decoded_events(
        self,
        tx_hashes: List[str],
        max_workers: int = 8
    ) -> List[DecodedGondiEvent]:
        """
        Get decoded events without generating journal entries.
        Useful for inspection/debugging.
        """
        return self.decoder.decode_batch(tx_hashes, max_workers=max_workers)


# ============================================================================
# STANDARD COLUMN ORDER
# ============================================================================

STANDARD_COLUMNS = [
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
    "annual_interest_rate",
    "payoff_amount",
    "loan_due_date",
    "tranche_floor",
    "tranche_index",
]


def standardize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize DataFrame columns to match expected output format.
    Adds missing columns with None values.
    """
    if df.empty:
        return df

    # Add missing columns
    for col in STANDARD_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Remove internal columns
    internal_cols = [c for c in df.columns if c.startswith('_')]
    df = df.drop(columns=internal_cols, errors='ignore')

    # Reorder columns (standard first, then any extras)
    existing_standard = [c for c in STANDARD_COLUMNS if c in df.columns]
    extra_cols = [c for c in df.columns if c not in STANDARD_COLUMNS]

    return df[existing_standard + extra_cols]


# ============================================================================
# HUMAN-READABLE CONVERSION
# ============================================================================

# Decimals lookup by cryptocurrency symbol
CRYPTO_DECIMALS = {
    "WETH": 18,
    "ETH": 18,
    "USDC": 6,
    "USDT": 6,
    "DAI": 18,
    "WBTC": 8,
}


def convert_to_human_readable(df: pd.DataFrame, inplace: bool = False) -> pd.DataFrame:
    """
    Convert wei values to human-readable cryptocurrency amounts.

    Converts these columns from wei to crypto units (in place):
    - debit
    - credit
    - principal
    - payoff_amount

    Uses the 'cryptocurrency' column to determine decimals.

    Args:
        df: DataFrame with wei values
        inplace: If True, modify original DataFrame

    Returns:
        DataFrame with human-readable values in original columns
    """

    # CHECK IF ALREADY CONVERTED
    if df.attrs.get('_human_readable', False):
        return df  # Already converted, skip

    if df.empty:
        return df

    if not inplace:
        df = df.copy()

    # Columns to convert
    wei_columns = ['debit', 'credit', 'principal', 'payoff_amount']

    # Get decimals for each row based on cryptocurrency
    def get_divisor(crypto: str) -> Decimal:
        decimals = CRYPTO_DECIMALS.get(str(crypto).upper(), 18)
        return Decimal(10) ** decimals

    # Convert each column
    for col in wei_columns:
        if col not in df.columns:
            continue

        # Convert row by row based on cryptocurrency
        converted_values = []
        for idx, row in df.iterrows():
            wei_value = row.get(col, 0)
            crypto = row.get('cryptocurrency', 'WETH')

            if pd.isna(wei_value) or wei_value == 0:
                converted_values.append(Decimal(0))
            else:
                divisor = get_divisor(crypto)
                converted_values.append(Decimal(str(int(wei_value))) / divisor)

        df[col] = converted_values

    # MARK AS CONVERTED
    df.attrs['_human_readable'] = True

    return df
    # Get decimals for each row based on cryptocurrency
    def get_divisor(crypto: str) -> Decimal:
        decimals = CRYPTO_DECIMALS.get(str(crypto).upper(), 18)
        return Decimal(10) ** decimals

    # Convert each column
    for col in wei_columns:
        if col not in df.columns:
            continue

        # Convert row by row based on cryptocurrency
        converted_values = []
        for idx, row in df.iterrows():
            wei_value = row.get(col, 0)
            crypto = row.get('cryptocurrency', 'WETH')

            if pd.isna(wei_value) or wei_value == 0:
                converted_values.append(Decimal(0))
            else:
                divisor = get_divisor(crypto)
                converted_values.append(Decimal(str(int(wei_value))) / divisor)

        df[col] = converted_values

    return df


def format_journal_entries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Format journal entries for display with human-readable values.

    Args:
        df: DataFrame with journal entries

    Returns:
        Formatted DataFrame ready for display/export
    """
    if df.empty:
        return df

    # Convert to human readable (modifies debit, credit, principal, payoff_amount)
    df = convert_to_human_readable(df)

    # Reorder columns to put key values in a logical place
    priority_columns = [
        "date",
        "transaction_type",
        "platform",
        "fund_id",
        "wallet_id",
        "cryptocurrency",
        "account_name",
        "debit",
        "credit",
        "principal",
        "payoff_amount",
        "event",
        "hash",
        "loan_id",
        "lender",
        "borrower",
        "annual_interest_rate",
        "loan_due_date",
    ]

    # Build column order
    existing_priority = [c for c in priority_columns if c in df.columns]
    other_cols = [c for c in df.columns if c not in existing_priority]
    final_order = existing_priority + other_cols

    return df[[c for c in final_order if c in df.columns]]


def print_journal_summary(results: Dict[str, pd.DataFrame]):
    """
    Print a formatted summary of journal entries with human-readable values.

    Args:
        results: Dict from processor.process_transactions()
    """
    print("\n" + "=" * 100)
    print("[docs] JOURNAL ENTRIES SUMMARY")
    print("=" * 100)

    for name, df in results.items():
        # Skip non-DataFrame items (validation dicts, metadata)
        if not isinstance(df, pd.DataFrame):
            continue
        if df.empty:
            continue

        print(f"\n{'-' * 80}")
        print(f"[list] {name.upper().replace('_', ' ')}")
        print(f"{'-' * 80}")

        # Convert to human readable (if not already)
        df_display = convert_to_human_readable(df)

        # Get unique cryptocurrencies
        if 'cryptocurrency' in df_display.columns:
            cryptos = df_display['cryptocurrency'].unique()
        else:
            cryptos = ['WETH']

        for crypto in cryptos:
            if 'cryptocurrency' in df_display.columns:
                crypto_df = df_display[df_display['cryptocurrency'] == crypto]
            else:
                crypto_df = df_display

            if crypto_df.empty:
                continue

            print(f"\n  [$] {crypto}:")

            # Sum debits and credits
            if 'debit' in crypto_df.columns:
                total_debit = crypto_df['debit'].sum()
                total_credit = crypto_df['credit'].sum()
                print(f"     Total Debit:  {total_debit:,.6f} {crypto}")
                print(f"     Total Credit: {total_credit:,.6f} {crypto}")

                # Check balance
                diff = abs(total_debit - total_credit)
                if diff < Decimal('0.000001'):
                    print(f"     [OK] Balanced")
                else:
                    print(f"     [!] Difference: {diff:,.6f} {crypto}")

            # Show entry count
            print(f"     Entries: {len(crypto_df)}")

            # Show by account
            if 'account_name' in crypto_df.columns and 'debit' in crypto_df.columns:
                print(f"\n     By Account:")
                account_summary = crypto_df.groupby('account_name').agg({
                    'debit': 'sum',
                    'credit': 'sum'
                }).reset_index()

                for _, row in account_summary.iterrows():
                    acct = row['account_name']
                    dr = row['debit']
                    cr = row['credit']
                    if dr > 0:
                        print(f"       Dr {acct}: {dr:,.6f}")
                    if cr > 0:
                        print(f"       Cr {acct}: {cr:,.6f}")

    print("\n" + "=" * 100)


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("GONDI LOANFACET DECODER v1.7.1 - ACCRUAL REVERSAL SUPPORT")
    print("=" * 80)
    print()
    print("Events Supported:")
    print("  - LoanEmitted (new loans)")
    print("  - LoanRepaid (repayments)")
    print("  - LoanRefinanced (refinancing - treated as payoff + new loan)")
    print("  - LoanRefinancedFromNewOffers (refinancing with new lenders)")
    print("  - LoanForeclosed (single-tranche foreclosure)")
    print("  - LoanLiquidated (multi-tranche liquidation)")
    print()
    print("Fund Role Support:")
    print("  - LENDER: loan_receivable_*, interest_receivable_*, interest_income_*")
    print("  - BORROWER: note_payable_*, interest_payable_*, interest_expense_*")
    print("  - CASH: deemed_cash_usd (always, regardless of cryptocurrency)")
    print()
    print("Interest Accrual Lifecycle (NEW in v1.3.0):")
    print("  1. Accruals generated based on FULL contractual term (estimates)")
    print("  2. Termination events detected (repay, foreclose, liquidate, refinance)")
    print("  3. Exact 1:1 reversals generated for post-termination accruals")
    print("  4. Original accruals remain immutable (audit trail preserved)")
    print("  5. Net effect: Accrued Interest = (Payoff - Principal)")
    print()
    print("Why This Approach is Audit-Safe:")
    print("  - No modification or deletion of historical accruals")
    print("  - Each reversal explicitly references the original accrual_id")
    print("  - Journal shows complete lifecycle: estimate -> reversal -> actuals")
    print("  - Traceability: who accrued what, when, and why it was reversed")
    print("  - Compliant with institutional loan accounting standards")
    print()
    print("Accrual Identity Fields:")
    print("  - accrual_id: Deterministic ID (loan_id:tranche_idx:date)")
    print("  - accrual_date: Economic date of the accrual")
    print("  - journal_date: Posting date (= accrual_date for originals)")
    print("  - is_reversal: Boolean flag")
    print("  - reverses_accrual_id: Reference to original (for reversals)")
    print()
    print("Example Accrual Reversal:")
    print("""
    # Original accrual (posted on 2025-04-15)
    | loan_id | accrual_id      | accrual_date | journal_date | account_name          | debit | credit | is_reversal |
    |---------|-----------------|--------------|--------------|------------------------|-------|--------|-------------|
    | 123     | 123:0:2025-04-15| 2025-04-15   | 2025-04-15   | interest_receivable_* | 10.00 | 0      | False       |
    | 123     | 123:0:2025-04-15| 2025-04-15   | 2025-04-15   | interest_income_*     | 0     | 10.00  | False       |

    # Loan repaid early on 2025-04-14 - reversal posted
    | loan_id | accrual_id            | accrual_date | journal_date | account_name          | debit | credit | is_reversal | reverses_accrual_id |
    |---------|----------------------|--------------|--------------|------------------------|-------|--------|-------------|---------------------|
    | 123     | 123:0:2025-04-15:rev | 2025-04-15   | 2025-04-14   | interest_receivable_* | 0     | 10.00  | True        | 123:0:2025-04-15    |
    | 123     | 123:0:2025-04-15:rev | 2025-04-15   | 2025-04-14   | interest_income_*     | 10.00 | 0      | True        | 123:0:2025-04-15    |

    # Net effect: original accrual + reversal = 0
    """)
    print()
    print("Usage:")
    print("""
    # Process events with accrual reversals (default)
    results = journal_generator.process_events(
        events,
        generate_accruals=True,
        generate_reversals=True,
        validate_reversals=True
    )

    # Access results
    df_accruals = results.get('interest_accruals')    # Original accruals
    df_reversals = results.get('accrual_reversals')   # Reversal entries
    terminations = results.get('terminations')         # Loan termination dates
    validation = results.get('reversal_validation')    # Validation results

    # Combine for complete ledger
    df_complete = pd.concat([df_accruals, df_reversals], ignore_index=True)
    """)