"""
Centralized Chart of Accounts (COA) for all decoders.

This module provides a single source of truth for account names and GL numbers.
All decoders should import from here instead of defining their own account mappings.

Account names match exactly what's in the S3 COA file:
s3://realworldnav-beta-1/drip_capital/drip_capital_COA.csv
"""

from dataclasses import dataclass
from typing import Dict, Tuple, Optional
from functools import lru_cache


@dataclass(frozen=True)
class GLAccount:
    """Immutable GL Account with number and name."""
    number: int
    name: str
    account_key: str  # The lookup key (e.g., "deemed_cash_usd")

    def __str__(self) -> str:
        return self.account_key


# =============================================================================
# MASTER CHART OF ACCOUNTS
# =============================================================================
# This dict maps account_key -> (GL_Acct_Number, GL_Acct_Name)
# Source: drip_capital_COA.csv uploaded to S3
# =============================================================================

COA: Dict[str, Tuple[int, str]] = {
    # ASSETS - Cash & Equivalents (10xxx)
    "deemed_cash_usd": (10000, "Deemed cash - USD"),
    "operating_account_usd": (10001, "Operating account - USD"),
    "brokerage_coinbase": (10002, "Brokerage account - Coinbase (Fund I)"),
    "brokerage_coinbase_ii": (10003, "Brokerage account - Coinbase (Fund II)"),

    # ASSETS - Digital Assets (101xx-103xx)
    "digital_assets_usdc": (10100, "Digital Assets - USDC"),
    "digital_assets_usdt": (10101, "Digital Assets - USDT"),
    "digital_assets_eth": (10200, "Digital Assets - ETH"),
    "digital_assets_weth": (10201, "Digital Assets - WETH"),
    "digital_assets_blur_pool": (10202, "Digital Assets - Blur Pool"),
    "digital_assets_other": (10250, "Digital Assets - Other"),
    "digital_assets_L2_deversifi": (10300, "Digital Assets - L2 - DeversiFi"),
    "digital_assets_bridge_arbitrum_eth": (10301, "Digital Assets - Arbitrum - ETH"),
    "digital_assets_bridge_orbiter_eth": (10302, "Digital Assets - Orbiter - ETH"),
    "digital_assets_bridge_lifi_relay_eth": (10303, "Digital Assets - LiFi Diamond - ETH"),
    "digital_assets_pengu": (10304, "Digital Assets - PENGU"),

    # ASSETS - Investments (106xx)
    "investments_contributions_property": (10600, "Investments - Contributions - Property"),
    "investments_unrealized_gain_loss": (10601, "Investments - Unrealized gain/loss"),

    # ASSETS - Auction Deposits (115xx)
    "auction_deposits": (11500, "Auction Deposits"),

    # ASSETS - Trade Settlement (122xx)
    "trade_settlement_receivable_cryptocurrency_weth": (12200, "Trade settlement receivable - Cryptocurrency - WETH"),

    # ASSETS - Interest Receivable (125xx)
    "interest_receivable_cryptocurrency_blur_pool": (12500, "Interest receivable - Cryptocurrency - Blur Pool"),
    "interest_receivable_cryptocurrency_weth": (12510, "Interest receivable - Cryptocurrency - WETH"),
    "interest_receivable_cryptocurrency_usdc": (12515, "Interest receivable - Cryptocurrency - USDC"),

    # ASSETS - Loan Receivable (135xx)
    "loan_receivable_cryptocurrency_blur_pool": (13500, "Loan receivable - Cryptocurrency - Blur Pool"),
    "loan_receivable_cryptocurrency_blur_pool_provision_for_bad_debt": (13501, "Provision for change in fair value - Blur Pool"),
    "loan_receivable_cryptocurrency_weth": (13510, "Loan receivable - Cryptocurrency - WETH"),
    "loan_receivable_cryptocurrency_weth_provision_for_bad_debt": (13511, "Provision for change in fair value - WETH"),
    "loan_receivable_cryptocurrency_usdc": (13515, "Loan receivable - Cryptocurrency - USDC"),

    # ASSETS - Yield Pool Investments (136xx-137xx)
    "digital_assets_mwet_ppg_5": (13600, "Investments - MetaStreet Pool Staking - PPG 5"),
    "digital_assets_aethweth": (13700, "Investments - Aave yield token - WETH"),
    "digital_assets_wsteth": (13701, "Investments - Wrapped liqued staked Ether 2.0"),
    "digital_assets_wweth": (13702, "Investments - Wrapped WETH"),
    "digital_assets_aethusdc": (13703, "Investments - Aave yield token - USDC"),
    "digital_assets_other_yield_pools": (13704, "Investments - Other yield pools - ETH"),

    # ASSETS - NFT Investments (140xx)
    "investments_nfts": (14000, "Investments - NFTs"),
    "investments_nfts_seized_collateral": (14010, "Investments - NFTs - Seized collateral"),

    # ASSETS - Unrealized Gain/Loss (149xx)
    "unrealized_gain_loss_cryptocurrencies": (14900, "Unrealized gain/loss - ETH"),
    "unrealized_gain_loss_usdc": (14901, "Unrealized gain/loss - USDC"),

    # ASSETS - Prepaid & Other (159xx)
    "prepaid_expenses": (15900, "Prepaid expenses"),

    # ASSETS - Related Party (188xx)
    "due_from_related_party_affiliated_funds": (18800, "Due from related party - Affiliated funds"),
    "due_from_related_party_LP": (18801, "Due from related party - LP"),
    "due_from_related_party_GP": (18802, "Due from related party - GP"),

    # ASSETS - Intercompany (199xx)
    "intercompany_funds_in_transit": (19990, "Intercompany funds in transit"),
    "suspense": (19999, "Suspense"),

    # LIABILITIES - Accrued (200xx-201xx)
    "accrued_expenses": (20010, "Accrued expenses"),
    "due_to_brokerage": (20015, "Due to brokeage"),
    "management_fee_payable": (20200, "Management fee payable"),

    # LIABILITIES - Interest Payable (209xx)
    "interest_payable_cryptocurrency_usdc": (20900, "Interest payable - Cryptocurrency - USDC"),
    "interest_payable_cryptocurrency_eth": (20901, "Interest payable - Cryptocurrency - ETH"),
    "interest_payable_cryptocurrency_weth": (20902, "Interest payable - Cryptocurrency - WETH"),
    "interest_payable_cryptocurrency_blur_pool": (20903, "Interest payable - Cryptocurrency - Blur Pool"),

    # LIABILITIES - Notes Payable (250xx)
    "note_payable_cryptocurrency_usdc": (25000, "Note payable - Cryptocurrency - USDC"),
    "note_payable_cryptocurrency_eth": (25001, "Note payable - Cryptocurrency - ETH"),
    "note_payable_cryptocurrency_weth": (25002, "Note payable - Cryptocurrency - WETH"),
    "note_payable_cryptocurrency_blur_pool": (25003, "Note payable - Cryptocurrency - Blur Pool"),

    # LIABILITIES - Related Party (290xx)
    "due_to_related_party_affiliated_funds": (29000, "Due to related party - Affiliated funds"),
    "due_to_related_party_LP": (29001, "Due to related party - LP"),
    "due_to_related_party_GP": (29002, "Due to related party - GP"),
    "funds_held_in_trust": (29999, "Funds held in trust"),

    # EQUITY - Capital (301xx-310xx)
    "capital_contributions_property": (30110, "Capital contributions - Property - ETH"),
    "capital_contributions_usd": (30111, "Capital contributions - USD"),
    "capital_distributions_property": (30210, "Capital distributions - Property - ETH"),
    "capital_incentive_allocation_GP_property": (31000, "Incentive allocation to General Partner (GP)"),
    "capital_incentive_allocation_LP_property": (31001, "Incentive allocation to General Partner (LP)"),

    # REVENUE - Other Income (410xx-420xx)
    "other_income": (41000, "Other income"),
    "other_income_rewards": (41000, "Other income"),
    "origination_fee_income": (42000, "Other income"),

    # EXPENSES - Operating (800xx)
    "gas_fee_expense": (80001, "Gas fee expense"),
    "fund_administration_expense": (80002, "Fund administration expense"),
    "bank_fees": (80003, "Bank and brokerage fees"),
    "legal_fees": (80004, "Legal fees"),
    "travel_meals_entertainment": (80005, "Travel - meals and entertainment"),
    "travel_transportation": (80006, "Travel - transportation"),
    "travel_lodging": (80007, "Travel - lodging"),
    "conference_fees": (80008, "Conference fees"),
    "bridge_fee_expense": (80009, "Bridge fee expense"),
    "miscellaneous_expense": (80100, "Miscellaneous expense"),
    "organizational_expense": (80110, "Organizational and fund formation expense"),

    # EXPENSES - Bad Debt / Fair Value (805xx)
    "bad_debt_expense_cryptocurrency_blur_pool": (80501, "Change in fair value - Blur Pool"),
    "bad_debt_expense_cryptocurrency_weth": (80511, "Change in fair value - WETH"),
    "bad_debt_expense_cryptocurrency_usdc": (80512, "Change in fair value - USDC"),

    # EXPENSES - Interest (809xx)
    "interest_expense_cryptocurrency_usdc": (80900, "Interest expense - Cryptocurrency - USDC"),
    "interest_expense_cryptocurrency_eth": (80901, "Interest expense - Cryptocurrency - ETH"),
    "interest_expense_cryptocurrency_weth": (80902, "Interest expense - Cryptocurrency - WETH"),
    "interest_expense_cryptocurrency_blur_pool": (80903, "Interest expense - Cryptocurrency - Blur Pool"),

    # EXPENSES - Fees (809xx continued)
    "origination_fee_expense_usdc": (80910, "Origination fee expense - Cryptocurrency - USDC"),
    "origination_fee_expense_weth": (80911, "Origination fee expense - Cryptocurrency - WETH"),
    "platform_fee_expense_usdc": (80915, "Platform fee expense - Cryptocurrency - USDC"),
    "platform_fee_expense_weth": (80916, "Platform fee expense - Cryptocurrency - WETH"),
    "platform_fee_expense_eth": (80917, "Platform fee expense - Cryptocurrency - ETH"),

    # EXPENSES - Management Fee (890xx)
    "management_fee_expense": (89001, "Management fee expense"),

    # INCOME - Interest (905xx)
    "interest_income_cryptocurrency_blur_pool": (90500, "Interest income - Cryptocurrency - Blur Pool"),
    "interest_income_cryptocurrency_weth": (90510, "Interest income - Cryptocurrency - WETH"),
    "interest_income_yield_pools_weth": (90511, "Interest income - Yield pools - Cryptocurrency - WETH"),
    "interest_income_cryptocurrency_usdc": (90515, "Interest income - Cryptocurrency - USDC"),

    # INCOME/EXPENSE - Realized Gain/Loss (905xx)
    "realized_gain_loss_cryptocurrency": (90520, "Realized gain/loss - Cryptocurrency"),
    "realized_gain_loss_eth": (90521, "Realized gain/loss - ETH"),
    "realized_gain_loss_digital_assets": (90522, "Realized gain/loss - Digital assets"),
    "realized_gain_loss_yield_pools": (90530, "Realized gain/loss - Yield pools"),
    "realized_gain_loss_nft": (90540, "Realized gain/loss - NFTs"),
    "realized_gain_loss": (90589, "Realized gain/loss - Deemed cash USD"),

    # INCOME/EXPENSE - Unrealized (905xx)
    "change_in_unrealized_gain_loss_eth": (90590, "Change in unrealized gain/loss - Cryptocurrencies"),
    "change_in_unrealized_gain_loss_usdc": (90591, "Change in unrealized gain/loss - USDC"),
    "net_change_in_unrealized_appreciation_depreciation": (90599, "Net change in unrealized appreciation/depreciation"),

    # INCOME - Investment (906xx)
    "income_allocated_from_investments": (90601, "Investment income - Allocated"),

    # TAX (909xx)
    "tax_realized_gain_loss_adjustment": (90900, "Tax realized gain/loss adjustment"),
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_account(account_key: str) -> Tuple[int, str]:
    """
    Get GL account number and name from account key.

    Args:
        account_key: The account key (e.g., "deemed_cash_usd")

    Returns:
        Tuple of (GL_Acct_Number, GL_Acct_Name)

    Raises:
        KeyError: If account_key not found in COA
    """
    if account_key not in COA:
        raise KeyError(f"Account '{account_key}' not found in COA. Available accounts: {list(COA.keys())}")
    return COA[account_key]


def get_account_number(account_key: str) -> int:
    """Get just the GL account number."""
    return get_account(account_key)[0]


def get_account_name(account_key: str) -> str:
    """Get just the GL account name."""
    return get_account(account_key)[1]


def get_account_safe(account_key: str, default: Tuple[int, str] = (19999, "Suspense")) -> Tuple[int, str]:
    """
    Get GL account with fallback to default (Suspense) if not found.

    Args:
        account_key: The account key
        default: Default tuple if not found (defaults to Suspense account)

    Returns:
        Tuple of (GL_Acct_Number, GL_Acct_Name)
    """
    return COA.get(account_key, default)


@lru_cache(maxsize=1)
def build_coa_map() -> Dict[str, Tuple[int, str]]:
    """
    Build a COA map for JournalEntry.to_gl_records().

    Returns:
        Dict mapping account_key to (GL_Acct_Number, GL_Acct_Name)
    """
    return COA.copy()


# =============================================================================
# PLATFORM-SPECIFIC ACCOUNT HELPERS
# =============================================================================

class BlurAccounts:
    """Account keys for Blur Pool lending platform."""

    # Assets
    DIGITAL_ASSETS = "digital_assets_blur_pool"
    LOAN_RECEIVABLE = "loan_receivable_cryptocurrency_blur_pool"
    LOAN_RECEIVABLE_PROVISION = "loan_receivable_cryptocurrency_blur_pool_provision_for_bad_debt"
    INTEREST_RECEIVABLE = "interest_receivable_cryptocurrency_blur_pool"
    INVESTMENTS_NFTS_SEIZED = "investments_nfts_seized_collateral"

    # Liabilities
    NOTE_PAYABLE = "note_payable_cryptocurrency_blur_pool"
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


class WETHAccounts:
    """Account keys for WETH-based lending (Gondi, Arcade, NFTfi)."""

    # Assets
    DIGITAL_ASSETS = "digital_assets_weth"
    LOAN_RECEIVABLE = "loan_receivable_cryptocurrency_weth"
    LOAN_RECEIVABLE_PROVISION = "loan_receivable_cryptocurrency_weth_provision_for_bad_debt"
    INTEREST_RECEIVABLE = "interest_receivable_cryptocurrency_weth"

    # Liabilities
    NOTE_PAYABLE = "note_payable_cryptocurrency_weth"
    INTEREST_PAYABLE = "interest_payable_cryptocurrency_weth"

    # Income
    INTEREST_INCOME = "interest_income_cryptocurrency_weth"

    # Expenses
    INTEREST_EXPENSE = "interest_expense_cryptocurrency_weth"
    BAD_DEBT_EXPENSE = "bad_debt_expense_cryptocurrency_weth"
    GAS_FEE_EXPENSE = "gas_fee_expense"

    # Clearing
    DEEMED_CASH_USD = "deemed_cash_usd"


class USDCAccounts:
    """Account keys for USDC-based lending."""

    # Assets
    DIGITAL_ASSETS = "digital_assets_usdc"
    LOAN_RECEIVABLE = "loan_receivable_cryptocurrency_usdc"
    INTEREST_RECEIVABLE = "interest_receivable_cryptocurrency_usdc"

    # Liabilities
    NOTE_PAYABLE = "note_payable_cryptocurrency_usdc"
    INTEREST_PAYABLE = "interest_payable_cryptocurrency_usdc"

    # Income
    INTEREST_INCOME = "interest_income_cryptocurrency_usdc"

    # Expenses
    INTEREST_EXPENSE = "interest_expense_cryptocurrency_usdc"
    BAD_DEBT_EXPENSE = "bad_debt_expense_cryptocurrency_usdc"
    GAS_FEE_EXPENSE = "gas_fee_expense"

    # Clearing
    DEEMED_CASH_USD = "deemed_cash_usd"


class GenericAccounts:
    """Account keys for generic transactions (transfers, wraps, etc.)."""

    # Digital Assets
    ETH = "digital_assets_eth"
    WETH = "digital_assets_weth"
    USDC = "digital_assets_usdc"
    USDT = "digital_assets_usdt"
    OTHER = "digital_assets_other"

    # Yield Pool Investments
    YIELD_POOLS = "digital_assets_other_yield_pools"
    AAVE_WETH = "digital_assets_aethweth"
    AAVE_USDC = "digital_assets_aethusdc"

    # Expenses
    GAS_FEE_EXPENSE = "gas_fee_expense"
    MISCELLANEOUS_EXPENSE = "miscellaneous_expense"

    # Income
    OTHER_INCOME = "other_income"

    # Related Party / Receivables
    DUE_FROM_RELATED_PARTY = "due_from_related_party_affiliated_funds"

    # Clearing
    DEEMED_CASH_USD = "deemed_cash_usd"
    SUSPENSE = "suspense"


# =============================================================================
# INTEREST ACCRUAL ACCOUNT LOOKUP
# =============================================================================

def get_interest_accrual_accounts(platform: str) -> Dict[str, Tuple[int, str]]:
    """
    Get the interest accrual accounts for a given platform.

    Args:
        platform: Platform name (blur, weth, usdc, gondi, arcade, nftfi)

    Returns:
        Dict with keys: interest_receivable, interest_income, interest_expense, interest_payable
    """
    platform_lower = platform.lower() if platform else "blur"

    if "blur" in platform_lower:
        return {
            'interest_receivable': get_account("interest_receivable_cryptocurrency_blur_pool"),
            'interest_income': get_account("interest_income_cryptocurrency_blur_pool"),
            'interest_expense': get_account("interest_expense_cryptocurrency_blur_pool"),
            'interest_payable': get_account("interest_payable_cryptocurrency_blur_pool"),
        }
    elif "weth" in platform_lower or "gondi" in platform_lower or "arcade" in platform_lower or "nftfi" in platform_lower:
        return {
            'interest_receivable': get_account("interest_receivable_cryptocurrency_weth"),
            'interest_income': get_account("interest_income_cryptocurrency_weth"),
            'interest_expense': get_account("interest_expense_cryptocurrency_weth"),
            'interest_payable': get_account("interest_payable_cryptocurrency_weth"),
        }
    elif "usdc" in platform_lower:
        return {
            'interest_receivable': get_account("interest_receivable_cryptocurrency_usdc"),
            'interest_income': get_account("interest_income_cryptocurrency_usdc"),
            'interest_expense': get_account("interest_expense_cryptocurrency_usdc"),
            'interest_payable': get_account("interest_payable_cryptocurrency_usdc"),
        }
    else:
        # Default to Blur Pool
        return {
            'interest_receivable': get_account("interest_receivable_cryptocurrency_blur_pool"),
            'interest_income': get_account("interest_income_cryptocurrency_blur_pool"),
            'interest_expense': get_account("interest_expense_cryptocurrency_blur_pool"),
            'interest_payable': get_account("interest_payable_cryptocurrency_blur_pool"),
        }


def get_loan_accounts(platform: str) -> Dict[str, Tuple[int, str]]:
    """
    Get the loan-related accounts for a given platform.

    Args:
        platform: Platform name (blur, weth, usdc, gondi, arcade, nftfi)

    Returns:
        Dict with keys: loan_receivable, loan_provision, note_payable, deemed_cash
    """
    platform_lower = platform.lower() if platform else "blur"

    if "blur" in platform_lower:
        return {
            'loan_receivable': get_account("loan_receivable_cryptocurrency_blur_pool"),
            'loan_provision': get_account("loan_receivable_cryptocurrency_blur_pool_provision_for_bad_debt"),
            'note_payable': get_account("note_payable_cryptocurrency_blur_pool"),
            'deemed_cash': get_account("deemed_cash_usd"),
        }
    elif "weth" in platform_lower or "gondi" in platform_lower or "arcade" in platform_lower or "nftfi" in platform_lower:
        return {
            'loan_receivable': get_account("loan_receivable_cryptocurrency_weth"),
            'loan_provision': get_account("loan_receivable_cryptocurrency_weth_provision_for_bad_debt"),
            'note_payable': get_account("note_payable_cryptocurrency_weth"),
            'deemed_cash': get_account("deemed_cash_usd"),
        }
    elif "usdc" in platform_lower:
        return {
            'loan_receivable': get_account("loan_receivable_cryptocurrency_usdc"),
            'loan_provision': get_account_safe("loan_receivable_cryptocurrency_usdc_provision_for_bad_debt"),
            'note_payable': get_account("note_payable_cryptocurrency_usdc"),
            'deemed_cash': get_account("deemed_cash_usd"),
        }
    else:
        # Default to Blur Pool
        return {
            'loan_receivable': get_account("loan_receivable_cryptocurrency_blur_pool"),
            'loan_provision': get_account("loan_receivable_cryptocurrency_blur_pool_provision_for_bad_debt"),
            'note_payable': get_account("note_payable_cryptocurrency_blur_pool"),
            'deemed_cash': get_account("deemed_cash_usd"),
        }
