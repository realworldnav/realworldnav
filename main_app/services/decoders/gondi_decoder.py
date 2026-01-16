"""
Gondi NFT Lending Decoder

Decodes transactions from Gondi's multi-source loan protocol.
Tested and validated using debug_decoder.py workbench.

Contract Versions:
- v1: 0xf41B389E0C1950dc0B16C9498eaE77131CC08A56 (tranche-based, 7-field struct)
- v2: 0x478f6F994C6fb3cf3e444a489b3AD9edB8cCaE16 (source-based, 6-field struct, no floor)
- v3: 0xf65B99CE6DC5F6c556172BCC0Ff27D3665a7d9A8 (tranche-based, 7-field struct)

Events Handled:
- LoanEmitted: New loan origination
- LoanRepaid: Loan repayment by borrower
- LoanRefinanced: Refinancing with existing/new lender
- LoanRefinancedFromNewOffers: Refinancing with new lenders
- LoanForeclosed: Foreclosure (NFT to lender)
- LoanLiquidated: Liquidation (auction proceeds)
"""

from web3 import Web3
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
import logging

from .base import (
    BaseDecoder,
    DecodedTransaction,
    DecodedEvent,
    JournalEntry,
    LoanPosition,
    TransactionCategory,
    PostingStatus,
    Platform,
    wei_to_eth,
    calculate_gas_fee,
)

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

SECONDS_PER_YEAR = 31536000  # 365 * 24 * 3600
PRECISION_BPS = 10000  # Basis points denominator

# Gondi contract addresses and versions
GONDI_CONTRACTS = {
    "0xf41b389e0c1950dc0b16c9498eae77131cc08a56": {"version": "v1", "type": "tranche"},
    "0x478f6f994c6fb3cf3e444a489b3ad9edb8ccae16": {"version": "v2", "type": "source"},
    "0xf65b99ce6dc5f6c556172bcc0ff27d3665a7d9a8": {"version": "v3", "type": "tranche"},
    "0x59e0b87e3dcfb5d34c06c71c3fbf7f6b7d77a4ff": {"version": "v3", "type": "tranche"},  # MultiSourceLoan
}

# Token registry for currency info
TOKEN_REGISTRY = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {"symbol": "WETH", "decimals": 18},
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"symbol": "USDC", "decimals": 6},
    "0xdac17f958d2ee523a2206206994597c13d831ec7": {"symbol": "USDT", "decimals": 6},
    "0x6b175474e89094c44da98b954eedeac495271d0f": {"symbol": "DAI", "decimals": 18},
}

# Events we decode from Gondi contracts
GONDI_EVENTS = [
    "LoanEmitted",
    "LoanRepaid",
    "LoanRefinanced",
    "LoanRefinancedFromNewOffers",
    "LoanForeclosed",
    "LoanLiquidated",
    "LoanSentToLiquidator",
]


# ============================================================================
# DATA CLASSES - Gondi Loan Structures
# ============================================================================

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
class GondiTranche:
    """
    Represents a single lender's tranche in a multi-source loan.

    V1/V3 Tranche struct (7 fields):
        loanId, floor, principalAmount, lender, accruedInterest, startTime, aprBps

    V2 Source struct (6 fields - NO floor):
        loanId, lender, principalAmount, accruedInterest, startTime, aprBps
    """
    loanId: int = 0
    floor: int = 0  # V2 doesn't have this
    principalAmount: int = 0
    lender: str = ""
    accruedInterest: int = 0
    startTime: int = 0
    aprBps: int = 0

    @classmethod
    def from_tuple(cls, data: tuple, is_v2: bool = False) -> 'GondiTranche':
        """Create from ABI-decoded tuple"""
        if is_v2:
            # V2 Source struct: loanId, lender, principalAmount, accruedInterest, startTime, aprBps
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
    def from_dict(cls, data: dict, is_v2: bool = False) -> 'GondiTranche':
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

    @property
    def principal_eth(self) -> Decimal:
        return Decimal(self.principalAmount) / Decimal(10**18)

    @property
    def apr_percent(self) -> float:
        return self.aprBps / 100


@dataclass
class GondiLoan:
    """
    Multi-source loan with multiple tranches from different lenders.

    V1/V3 Loan struct (9 fields):
        borrower, nftCollateralTokenId, nftCollateralAddress, principalAddress,
        principalAmount, startTime, duration, tranche[], protocolFee

    V2 Loan struct (8 fields - no protocolFee):
        borrower, nftCollateralTokenId, nftCollateralAddress, principalAddress,
        principalAmount, startTime, duration, source[]
    """
    borrower: str = ""
    nftCollateralTokenId: int = 0
    nftCollateralAddress: str = ""
    principalAddress: str = ""
    principalAmount: int = 0
    startTime: int = 0
    duration: int = 0
    tranches: List[GondiTranche] = field(default_factory=list)
    protocolFee: int = 0  # V2 = 0

    @classmethod
    def from_tuple(cls, data: tuple, is_v2: bool = False) -> 'GondiLoan':
        """Create from ABI-decoded tuple"""
        tranche_data = data[7] if len(data) > 7 else []
        tranches = [GondiTranche.from_tuple(t, is_v2=is_v2) for t in tranche_data]
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
    def from_dict(cls, data: dict, is_v2: bool = False) -> 'GondiLoan':
        """Create from dictionary/AttributeDict"""
        # Handle different field names for tranches (source vs tranche)
        tranche_data = data.get('source', data.get('tranche', data.get('tranches', [])))
        tranches = []
        for t in tranche_data:
            if hasattr(t, 'keys') or isinstance(t, dict):
                tranches.append(GondiTranche.from_dict(dict(t), is_v2=is_v2))
            elif isinstance(t, tuple):
                tranches.append(GondiTranche.from_tuple(t, is_v2=is_v2))

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

    @property
    def principal_eth(self) -> Decimal:
        return Decimal(self.principalAmount) / Decimal(10**18)

    @property
    def duration_days(self) -> float:
        return self.duration / 86400

    def get_currency_symbol(self) -> str:
        """Get currency symbol from principal address"""
        addr = self.principalAddress.lower()
        if addr in TOKEN_REGISTRY:
            return TOKEN_REGISTRY[addr]["symbol"]
        return "WETH"  # Default


# ============================================================================
# INTEREST CALCULATIONS
# ============================================================================

def calculate_simple_interest(
    principal_wei: int,
    apr_bps: int,
    duration_seconds: int,
    protocol_fee_bps: int = 0,
) -> Tuple[int, int, int]:
    """
    Calculate interest using Gondi's simple interest formula.

    Args:
        principal_wei: Principal amount in wei
        apr_bps: Annual percentage rate in basis points (e.g., 1500 = 15%)
        duration_seconds: Loan duration in seconds
        protocol_fee_bps: Protocol fee in basis points (taken from gross interest)

    Returns:
        Tuple of (gross_interest, protocol_fee, net_interest) all in wei
    """
    if principal_wei == 0 or apr_bps == 0 or duration_seconds == 0:
        return 0, 0, 0

    # Simple interest (not compound): I = P * r * t
    gross = (principal_wei * apr_bps * duration_seconds) // (PRECISION_BPS * SECONDS_PER_YEAR)

    # Protocol takes a cut from gross interest
    protocol_fee = (gross * protocol_fee_bps) // PRECISION_BPS

    # Net interest to lender
    net_interest = gross - protocol_fee

    return gross, protocol_fee, net_interest


def calculate_tranche_interest(
    tranche: GondiTranche,
    end_timestamp: int,
    protocol_fee_bps: int,
) -> Tuple[int, int]:
    """
    Calculate total interest for a tranche including carried interest.

    Args:
        tranche: The tranche to calculate interest for
        end_timestamp: End timestamp for interest calculation
        protocol_fee_bps: Protocol fee in basis points

    Returns:
        Tuple of (total_interest, current_period_net_interest) in wei
    """
    # Carried forward from prior refinancing
    carried = tranche.accruedInterest

    # Current period interest
    time_elapsed = max(0, end_timestamp - tranche.startTime)
    _, _, net_current = calculate_simple_interest(
        tranche.principalAmount,
        tranche.aprBps,
        time_elapsed,
        protocol_fee_bps
    )

    total = carried + net_current
    return total, net_current


# ============================================================================
# GONDI DECODER
# ============================================================================

class GondiDecoder(BaseDecoder):
    """
    Gondi multi-source NFT lending decoder.

    Handles loan origination, repayment, refinancing, and liquidation events
    across all Gondi contract versions (v1, v2, v3).
    """

    PLATFORM = Platform.GONDI

    CONTRACT_ADDRESSES = list(GONDI_CONTRACTS.keys())

    # Gondi-specific chart of accounts
    ACCOUNTS = {
        "eth_wallet": "100.30 - ETH Wallet",
        "weth_wallet": "100.31 - WETH Wallet",
        "loan_receivable": "120.10 - Loans Receivable - Crypto",
        "loan_payable": "210.10 - Loans Payable - Crypto",
        "interest_income": "400.10 - Interest Income - Crypto",
        "interest_expense": "500.10 - Interest Expense - Crypto",
        "protocol_fees": "600.20 - Protocol Fees",
        "gas_expense": "600.10 - Gas Expense",
        "deemed_cash": "100.40 - Deemed Cash USD",
    }

    # Categories that auto-post for Gondi
    AUTO_POST_CATEGORIES = {
        TransactionCategory.LOAN_ORIGINATION,
        TransactionCategory.LOAN_REPAYMENT,
        TransactionCategory.LOAN_REFINANCE,
    }

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        self._abi_cache: Dict[str, Any] = {}  # Initialize before parent calls _load_abis()
        super().__init__(w3, fund_wallets)

    def _load_abis(self):
        """Load Gondi contract ABIs from S3"""
        try:
            from ..decoders.abis import load_abi

            for address in self.CONTRACT_ADDRESSES:
                abi = load_abi(address, platform="gondi")
                if abi:
                    self._abi_cache[address] = abi
                    logger.info(f"Loaded Gondi ABI for {address[:16]}...")
        except Exception as e:
            logger.warning(f"Could not load Gondi ABIs: {e}")

    def _load_abi_from_s3(self, address: str) -> Optional[List]:
        """Load ABI from S3 with caching"""
        address = address.lower()

        if address in self._abi_cache:
            return self._abi_cache[address]

        try:
            from ...s3_utils import load_abi_from_s3
            abi = load_abi_from_s3(address)
            if abi:
                self._abi_cache[address] = abi
                return abi
        except Exception as e:
            logger.debug(f"Could not load ABI from S3 for {address}: {e}")

        return None

    def _is_v2_contract(self, address: str) -> bool:
        """Check if contract is V2 (different struct format)"""
        info = GONDI_CONTRACTS.get(address.lower(), {})
        return info.get('type') == 'source'

    def _get_contract_version(self, address: str) -> str:
        """Get contract version string"""
        info = GONDI_CONTRACTS.get(address.lower(), {})
        return info.get('version', 'unknown')

    def can_decode(self, tx: Dict, receipt: Dict) -> bool:
        """Check if transaction involves Gondi contracts"""
        to_address = (tx.get('to') or '').lower()

        # Direct call to Gondi contract
        if to_address in GONDI_CONTRACTS:
            return True

        # Check if any logs are from Gondi contracts
        for log in receipt.get('logs', []):
            log_addr = (log.get('address') or '').lower()
            if log_addr in GONDI_CONTRACTS:
                return True

        return False

    def decode(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal) -> DecodedTransaction:
        """
        Decode Gondi transaction and generate journal entries.

        Uses process_log() pattern for reliable event decoding.
        """
        tx_hash = tx.get('hash', b'')
        if isinstance(tx_hash, bytes):
            tx_hash = tx_hash.hex()
        if not tx_hash.startswith('0x'):
            tx_hash = '0x' + tx_hash

        to_address = (tx.get('to') or '').lower()
        from_address = (tx.get('from') or '').lower()
        block_number = tx.get('blockNumber', 0)
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
        value = wei_to_eth(tx.get('value', 0))
        gas_fee = calculate_gas_fee(receipt, tx)

        # Decode function input
        func_name, func_params = self._decode_function(tx, to_address)

        # Determine category from function name
        category = self._get_category_from_function(func_name)

        # Decode events using process_log() pattern
        events, loans = self._decode_gondi_events(receipt)

        # Generate journal entries
        journal_entries = self._generate_journal_entries(
            tx_hash=tx_hash,
            timestamp=timestamp,
            events=events,
            loans=loans,
            category=category,
            eth_price=eth_price,
            gas_fee=gas_fee,
        )

        return DecodedTransaction(
            status="success",
            tx_hash=tx_hash,
            platform=self.PLATFORM,
            category=category,
            block=block_number,
            timestamp=timestamp,
            eth_price=eth_price,
            gas_used=receipt.get('gasUsed', 0),
            gas_fee=gas_fee,
            from_address=from_address,
            to_address=to_address,
            value=value,
            function_name=func_name,
            function_params=func_params,
            events=events,
            journal_entries=journal_entries,
            wallet_roles=self._determine_wallet_roles(events, loans),
        )

    def _decode_function(self, tx: Dict, to_address: str) -> Tuple[str, Dict]:
        """Decode function call from transaction input"""
        input_data = tx.get('input', b'')
        if isinstance(input_data, str):
            if input_data.startswith('0x'):
                input_data = bytes.fromhex(input_data[2:])
            else:
                input_data = bytes.fromhex(input_data)

        if len(input_data) < 4:
            return "unknown", {}

        # Try to load ABI and decode
        abi = self._load_abi_from_s3(to_address)
        if abi:
            try:
                contract = self.w3.eth.contract(
                    address=Web3.to_checksum_address(to_address),
                    abi=abi
                )
                func, params = contract.decode_function_input(input_data)
                return func.fn_name, dict(params)
            except Exception as e:
                logger.debug(f"Could not decode function: {e}")

        return "unknown", {}

    def _get_category_from_function(self, func_name: str) -> TransactionCategory:
        """Map function name to transaction category"""
        mapping = {
            "emitLoan": TransactionCategory.LOAN_ORIGINATION,
            "refinanceFull": TransactionCategory.LOAN_REFINANCE,
            "refinancePartial": TransactionCategory.LOAN_REFINANCE,
            "refinanceFromLoanExecutionData": TransactionCategory.LOAN_REFINANCE,
            "repayLoan": TransactionCategory.LOAN_REPAYMENT,
            "liquidateLoan": TransactionCategory.LOAN_LIQUIDATION,
            "claim": TransactionCategory.COLLATERAL_SEIZURE,
        }
        return mapping.get(func_name, TransactionCategory.UNKNOWN)

    def _decode_gondi_events(self, receipt: Dict) -> Tuple[List[DecodedEvent], Dict[str, GondiLoan]]:
        """
        Decode Gondi events using process_log() pattern.

        Returns:
            Tuple of (decoded_events, loans_by_event_name)
        """
        events = []
        loans = {}

        for log in receipt.get('logs', []):
            log_address = (log.get('address') or '').lower()

            # Load ABI for this contract
            abi = self._load_abi_from_s3(log_address)
            if not abi:
                continue

            try:
                contract = self.w3.eth.contract(
                    address=Web3.to_checksum_address(log_address),
                    abi=abi
                )

                # Get event names from ABI
                event_names = [item['name'] for item in abi if item.get('type') == 'event']

                # Try each event type with process_log()
                for evt_name in event_names:
                    try:
                        event_obj = getattr(contract.events, evt_name)
                        decoded = event_obj().process_log(log)

                        # Successfully decoded
                        args = dict(decoded.get('args', {}))

                        # Extract Loan struct if present
                        is_v2 = self._is_v2_contract(log_address)
                        loan = None

                        if evt_name in GONDI_EVENTS:
                            loan_data = args.get('loan')
                            if loan_data:
                                if hasattr(loan_data, 'keys') or isinstance(loan_data, dict):
                                    loan = GondiLoan.from_dict(dict(loan_data), is_v2=is_v2)
                                elif isinstance(loan_data, tuple):
                                    loan = GondiLoan.from_tuple(loan_data, is_v2=is_v2)

                                if loan:
                                    loans[evt_name] = loan

                        # Convert args to JSON-safe format
                        safe_args = {}
                        for k, v in args.items():
                            if k == 'loan':
                                continue  # Already extracted
                            if isinstance(v, bytes):
                                safe_args[k] = v.hex()
                            elif isinstance(v, int):
                                safe_args[k] = str(v)
                            else:
                                safe_args[k] = v

                        events.append(DecodedEvent(
                            name=evt_name,
                            args=safe_args,
                            log_index=log.get('logIndex', -1),
                            contract_address=log_address,
                        ))

                        break  # Found matching event, stop trying others

                    except Exception:
                        continue  # This event type didn't match

            except Exception as e:
                logger.debug(f"Could not decode log from {log_address}: {e}")

        return events, loans

    def _determine_wallet_roles(
        self,
        events: List[DecodedEvent],
        loans: Dict[str, GondiLoan]
    ) -> Dict[str, str]:
        """Determine which fund wallets are involved and their roles"""
        roles = {}

        for loan in loans.values():
            # Check borrower
            if self.is_fund_wallet(loan.borrower):
                roles[loan.borrower] = "borrower"

            # Check each tranche lender
            for tranche in loan.tranches:
                if self.is_fund_wallet(tranche.lender):
                    roles[tranche.lender] = "lender"

        return roles

    def _generate_journal_entries(
        self,
        tx_hash: str,
        timestamp: datetime,
        events: List[DecodedEvent],
        loans: Dict[str, GondiLoan],
        category: TransactionCategory,
        eth_price: Decimal,
        gas_fee: Decimal,
    ) -> List[JournalEntry]:
        """
        Generate journal entries for Gondi events.

        Processes each event type with appropriate accounting treatment.
        """
        entries = []
        tx_hash_short = tx_hash[2:12] if tx_hash.startswith('0x') else tx_hash[:10]

        # Process LoanRefinanced
        if 'LoanRefinanced' in loans:
            loan = loans['LoanRefinanced']
            event = next((e for e in events if e.name == 'LoanRefinanced'), None)

            if event and loan:
                entries.extend(self._process_loan_refinanced(
                    tx_hash=tx_hash,
                    tx_hash_short=tx_hash_short,
                    timestamp=timestamp,
                    loan=loan,
                    event=event,
                    eth_price=eth_price,
                ))

        # Process LoanRefinancedFromNewOffers (similar to LoanRefinanced but with new lenders)
        if 'LoanRefinancedFromNewOffers' in loans:
            loan = loans['LoanRefinancedFromNewOffers']
            event = next((e for e in events if e.name == 'LoanRefinancedFromNewOffers'), None)

            if event and loan:
                entries.extend(self._process_loan_refinanced_from_new_offers(
                    tx_hash=tx_hash,
                    tx_hash_short=tx_hash_short,
                    timestamp=timestamp,
                    loan=loan,
                    event=event,
                    eth_price=eth_price,
                ))

        # Process LoanEmitted (new origination)
        if 'LoanEmitted' in loans:
            loan = loans['LoanEmitted']
            event = next((e for e in events if e.name == 'LoanEmitted'), None)

            if event and loan:
                entries.extend(self._process_loan_emitted(
                    tx_hash=tx_hash,
                    tx_hash_short=tx_hash_short,
                    timestamp=timestamp,
                    loan=loan,
                    event=event,
                    eth_price=eth_price,
                ))

        # Process LoanRepaid
        if 'LoanRepaid' in loans:
            loan = loans['LoanRepaid']
            event = next((e for e in events if e.name == 'LoanRepaid'), None)

            if event and loan:
                entries.extend(self._process_loan_repaid(
                    tx_hash=tx_hash,
                    tx_hash_short=tx_hash_short,
                    timestamp=timestamp,
                    loan=loan,
                    event=event,
                    eth_price=eth_price,
                ))

        # FALLBACK: Handle refinance events without loan struct
        # LoanRefinanced and LoanRefinancedFromNewOffers often don't include
        # the full loan struct - just IDs. In this case, parse Transfer events
        # to find WETH payments to fund wallets.
        if not entries:
            refinance_event = None
            for evt in events:
                if evt.name in ('LoanRefinanced', 'LoanRefinancedFromNewOffers'):
                    refinance_event = evt
                    break

            if refinance_event:
                entries.extend(self._process_refinance_from_transfers(
                    tx_hash=tx_hash,
                    tx_hash_short=tx_hash_short,
                    timestamp=timestamp,
                    refinance_event=refinance_event,
                    transfer_events=[e for e in events if e.name == 'Transfer'],
                    eth_price=eth_price,
                ))

        # Gas fee entry (always generate for fund transactions)
        if gas_fee > 0 and entries:  # Only if we generated other entries
            gas_entry = JournalEntry(
                entry_id=f"JE_{tx_hash_short}_gas",
                date=timestamp,
                description=f"Gas Fee: {float(gas_fee):.6f} ETH",
                tx_hash=tx_hash,
                category=TransactionCategory.CONTRACT_CALL,
                platform=self.PLATFORM,
                eth_usd_price=eth_price,
                posting_status=PostingStatus.AUTO_POST,
            )
            gas_entry.add_debit(self.ACCOUNTS["gas_expense"], gas_fee, "ETH")
            gas_entry.add_credit(self.ACCOUNTS["eth_wallet"], gas_fee, "ETH")
            entries.append(gas_entry)

        return entries

    def _process_loan_refinanced(
        self,
        tx_hash: str,
        tx_hash_short: str,
        timestamp: datetime,
        loan: GondiLoan,
        event: DecodedEvent,
        eth_price: Decimal,
    ) -> List[JournalEntry]:
        """
        Process LoanRefinanced event.

        For LENDER: Record new loan receivable + interest income from fee
        For BORROWER: Record loan payable rollover
        """
        entries = []
        currency = loan.get_currency_symbol()

        # Get event args
        args = event.args
        renegotiation_id = safe_int(args.get('renegotiationId', 0))
        old_loan_id = safe_int(args.get('oldLoanId', 0))
        new_loan_id = safe_int(args.get('newLoanId', args.get('loanId', 0)))
        fee = safe_int(args.get('fee', 0))  # NET fee (already after protocol cut)

        for i, tranche in enumerate(loan.tranches):
            is_fund_lender = self.is_fund_wallet(tranche.lender)
            is_fund_borrower = self.is_fund_wallet(loan.borrower)

            if is_fund_lender:
                # LENDER perspective: New loan to borrower
                principal_eth = tranche.principal_eth

                # Pro-rata fee share
                fee_share = 0
                if loan.principalAmount > 0:
                    fee_share = (tranche.principalAmount * fee) // loan.principalAmount
                fee_share_eth = Decimal(fee_share) / Decimal(10**18)

                # Cash disbursed = principal - fee received
                cash_disbursed_eth = principal_eth - fee_share_eth

                entry = JournalEntry(
                    entry_id=f"JE_{tx_hash_short}_refi_lender_{i}",
                    date=timestamp,
                    description=f"Gondi Refinance: Lender tranche {i} - Loan {new_loan_id}",
                    tx_hash=tx_hash,
                    category=TransactionCategory.LOAN_REFINANCE,
                    platform=self.PLATFORM,
                    wallet_address=tranche.lender,
                    wallet_role="lender",
                    eth_usd_price=eth_price,
                    posting_status=PostingStatus.AUTO_POST,
                )

                # Debit loan receivable (new loan out)
                entry.add_debit(self.ACCOUNTS["loan_receivable"], principal_eth, currency)

                # Credit cash (amount actually sent)
                entry.add_credit(self.ACCOUNTS["deemed_cash"], cash_disbursed_eth, currency)

                # Credit interest income (fee received)
                if fee_share_eth > 0:
                    entry.add_credit(self.ACCOUNTS["interest_income"], fee_share_eth, currency)

                entries.append(entry)

            elif is_fund_borrower:
                # BORROWER perspective: Loan payable rollover
                principal_eth = loan.principal_eth

                entry = JournalEntry(
                    entry_id=f"JE_{tx_hash_short}_refi_borrower",
                    date=timestamp,
                    description=f"Gondi Refinance: Borrower - Loan {new_loan_id}",
                    tx_hash=tx_hash,
                    category=TransactionCategory.LOAN_REFINANCE,
                    platform=self.PLATFORM,
                    wallet_address=loan.borrower,
                    wallet_role="borrower",
                    eth_usd_price=eth_price,
                    posting_status=PostingStatus.AUTO_POST,
                )

                # Debit old loan payable (closed out)
                entry.add_debit(self.ACCOUNTS["loan_payable"], principal_eth, currency)

                # Credit new loan payable (new obligation)
                entry.add_credit(self.ACCOUNTS["loan_payable"], principal_eth, currency)

                entries.append(entry)
                break  # Only one borrower entry needed

        return entries

    def _process_loan_refinanced_from_new_offers(
        self,
        tx_hash: str,
        tx_hash_short: str,
        timestamp: datetime,
        loan: GondiLoan,
        event: DecodedEvent,
        eth_price: Decimal,
    ) -> List[JournalEntry]:
        """
        Process LoanRefinancedFromNewOffers event.

        Similar to LoanRefinanced but with new lender offers.
        For LENDER: Record new loan receivable + interest income from fee
        For BORROWER: Record loan payable rollover
        """
        entries = []
        currency = loan.get_currency_symbol()

        # Get event args
        args = event.args
        old_loan_id = safe_int(args.get('loanId', 0))
        new_loan_id = safe_int(args.get('newLoanId', 0))
        total_fee = safe_int(args.get('totalFee', 0))

        for i, tranche in enumerate(loan.tranches):
            is_fund_lender = self.is_fund_wallet(tranche.lender)
            is_fund_borrower = self.is_fund_wallet(loan.borrower)

            if is_fund_lender:
                # LENDER perspective: New loan to borrower
                principal_eth = tranche.principal_eth

                # Pro-rata fee share
                fee_share = 0
                if loan.principalAmount > 0:
                    fee_share = (tranche.principalAmount * total_fee) // loan.principalAmount
                fee_share_eth = Decimal(fee_share) / Decimal(10**18)

                # Cash disbursed = principal - fee received
                cash_disbursed_eth = principal_eth - fee_share_eth

                entry = JournalEntry(
                    entry_id=f"JE_{tx_hash_short}_refi_new_{i}",
                    date=timestamp,
                    description=f"Gondi Refinance (New Offers): Lender tranche {i} - Loan {new_loan_id}",
                    tx_hash=tx_hash,
                    category=TransactionCategory.LOAN_REFINANCE,
                    platform=self.PLATFORM,
                    wallet_address=tranche.lender,
                    wallet_role="lender",
                    eth_usd_price=eth_price,
                    posting_status=PostingStatus.AUTO_POST,
                )

                # Debit loan receivable (new loan out)
                entry.add_debit(self.ACCOUNTS["loan_receivable"], principal_eth, currency)

                # Credit cash (amount actually sent)
                entry.add_credit(self.ACCOUNTS["deemed_cash"], cash_disbursed_eth, currency)

                # Credit interest income (fee received)
                if fee_share_eth > 0:
                    entry.add_credit(self.ACCOUNTS["interest_income"], fee_share_eth, currency)

                entries.append(entry)

            elif is_fund_borrower:
                # BORROWER perspective: Loan payable rollover
                principal_eth = loan.principal_eth

                entry = JournalEntry(
                    entry_id=f"JE_{tx_hash_short}_refi_new_borrower",
                    date=timestamp,
                    description=f"Gondi Refinance (New Offers): Borrower - Loan {new_loan_id}",
                    tx_hash=tx_hash,
                    category=TransactionCategory.LOAN_REFINANCE,
                    platform=self.PLATFORM,
                    wallet_address=loan.borrower,
                    wallet_role="borrower",
                    eth_usd_price=eth_price,
                    posting_status=PostingStatus.AUTO_POST,
                )

                # Debit old loan payable (closed out)
                entry.add_debit(self.ACCOUNTS["loan_payable"], principal_eth, currency)

                # Credit new loan payable (new obligation)
                entry.add_credit(self.ACCOUNTS["loan_payable"], principal_eth, currency)

                entries.append(entry)
                break  # Only one borrower entry needed

        return entries

    def _process_refinance_from_transfers(
        self,
        tx_hash: str,
        tx_hash_short: str,
        timestamp: datetime,
        refinance_event: DecodedEvent,
        transfer_events: List[DecodedEvent],
        eth_price: Decimal,
    ) -> List[JournalEntry]:
        """
        Fallback handler for refinance events without loan struct.

        When LoanRefinanced or LoanRefinancedFromNewOffers events don't include
        the full loan struct, we parse Transfer events to find WETH payments
        to fund wallets and generate interest income entries.
        """
        entries = []

        # Get refinance event details
        args = refinance_event.args
        event_name = refinance_event.name
        new_loan_id = safe_int(args.get('newLoanId', args.get('loanId', 0)))
        fee = safe_int(args.get('fee', args.get('totalFee', 0)))

        # Find Transfer events where fund wallet is recipient
        for i, transfer in enumerate(transfer_events):
            t_args = transfer.args

            # Get src/dst from Transfer event
            src = safe_address(t_args.get('src', t_args.get('from', '')))
            dst = safe_address(t_args.get('dst', t_args.get('to', '')))
            wad = safe_int(t_args.get('wad', t_args.get('value', 0)))

            # Check if fund wallet received WETH
            if self.is_fund_wallet(dst) and wad > 0:
                amount_eth = Decimal(wad) / Decimal(10**18)

                entry = JournalEntry(
                    entry_id=f"JE_{tx_hash_short}_refi_income_{i}",
                    date=timestamp,
                    description=f"Gondi Refinance Interest: Loan {new_loan_id} ({event_name})",
                    tx_hash=tx_hash,
                    category=TransactionCategory.LOAN_REFINANCE,
                    platform=self.PLATFORM,
                    wallet_address=dst,
                    wallet_role="lender",
                    eth_usd_price=eth_price,
                    posting_status=PostingStatus.AUTO_POST,
                )

                # Debit WETH (received)
                entry.add_debit(self.ACCOUNTS["deemed_cash"], amount_eth, "WETH")

                # Credit interest income
                entry.add_credit(self.ACCOUNTS["interest_income"], amount_eth, "WETH")

                entries.append(entry)
                logger.info(f"Generated refinance income entry: {float(amount_eth):.6f} WETH to {dst[:10]}...")

        return entries

    def _process_loan_emitted(
        self,
        tx_hash: str,
        tx_hash_short: str,
        timestamp: datetime,
        loan: GondiLoan,
        event: DecodedEvent,
        eth_price: Decimal,
    ) -> List[JournalEntry]:
        """
        Process LoanEmitted event (new loan origination).

        For LENDER: Debit loan receivable, Credit cash
        For BORROWER: Debit cash, Credit loan payable
        """
        entries = []
        currency = loan.get_currency_symbol()

        args = event.args
        loan_id = safe_int(args.get('loanId', 0))

        for i, tranche in enumerate(loan.tranches):
            is_fund_lender = self.is_fund_wallet(tranche.lender)
            is_fund_borrower = self.is_fund_wallet(loan.borrower)

            if is_fund_lender:
                principal_eth = tranche.principal_eth

                entry = JournalEntry(
                    entry_id=f"JE_{tx_hash_short}_orig_lender_{i}",
                    date=timestamp,
                    description=f"Gondi Loan Origination: Lender tranche {i} - Loan {loan_id}",
                    tx_hash=tx_hash,
                    category=TransactionCategory.LOAN_ORIGINATION,
                    platform=self.PLATFORM,
                    wallet_address=tranche.lender,
                    wallet_role="lender",
                    eth_usd_price=eth_price,
                    posting_status=PostingStatus.AUTO_POST,
                )

                entry.add_debit(self.ACCOUNTS["loan_receivable"], principal_eth, currency)
                entry.add_credit(self.ACCOUNTS["deemed_cash"], principal_eth, currency)

                entries.append(entry)

            elif is_fund_borrower:
                principal_eth = loan.principal_eth

                entry = JournalEntry(
                    entry_id=f"JE_{tx_hash_short}_orig_borrower",
                    date=timestamp,
                    description=f"Gondi Loan Origination: Borrower - Loan {loan_id}",
                    tx_hash=tx_hash,
                    category=TransactionCategory.LOAN_ORIGINATION,
                    platform=self.PLATFORM,
                    wallet_address=loan.borrower,
                    wallet_role="borrower",
                    eth_usd_price=eth_price,
                    posting_status=PostingStatus.AUTO_POST,
                )

                entry.add_debit(self.ACCOUNTS["deemed_cash"], principal_eth, currency)
                entry.add_credit(self.ACCOUNTS["loan_payable"], principal_eth, currency)

                entries.append(entry)
                break

        return entries

    def _process_loan_repaid(
        self,
        tx_hash: str,
        tx_hash_short: str,
        timestamp: datetime,
        loan: GondiLoan,
        event: DecodedEvent,
        eth_price: Decimal,
    ) -> List[JournalEntry]:
        """
        Process LoanRepaid event.

        For LENDER: Debit cash, Credit loan receivable + interest income
        For BORROWER: Debit loan payable + interest expense, Credit cash
        """
        entries = []
        currency = loan.get_currency_symbol()

        args = event.args
        loan_id = safe_int(args.get('loanId', 0))
        total_repayment = safe_int(args.get('totalRepayment', 0))
        total_repayment_eth = Decimal(total_repayment) / Decimal(10**18)

        for i, tranche in enumerate(loan.tranches):
            is_fund_lender = self.is_fund_wallet(tranche.lender)
            is_fund_borrower = self.is_fund_wallet(loan.borrower)

            if is_fund_lender:
                principal_eth = tranche.principal_eth

                # Calculate interest (approximate - actual comes from totalRepayment)
                # Pro-rata share of repayment based on principal
                if loan.principalAmount > 0:
                    tranche_share = (tranche.principalAmount * total_repayment) // loan.principalAmount
                else:
                    tranche_share = total_repayment
                tranche_repayment_eth = Decimal(tranche_share) / Decimal(10**18)
                interest_eth = tranche_repayment_eth - principal_eth
                interest_eth = max(Decimal(0), interest_eth)  # Can't be negative

                entry = JournalEntry(
                    entry_id=f"JE_{tx_hash_short}_repay_lender_{i}",
                    date=timestamp,
                    description=f"Gondi Loan Repayment: Lender tranche {i} - Loan {loan_id}",
                    tx_hash=tx_hash,
                    category=TransactionCategory.LOAN_REPAYMENT,
                    platform=self.PLATFORM,
                    wallet_address=tranche.lender,
                    wallet_role="lender",
                    eth_usd_price=eth_price,
                    posting_status=PostingStatus.AUTO_POST,
                )

                # Debit cash received
                entry.add_debit(self.ACCOUNTS["deemed_cash"], tranche_repayment_eth, currency)

                # Credit loan receivable (principal returned)
                entry.add_credit(self.ACCOUNTS["loan_receivable"], principal_eth, currency)

                # Credit interest income
                if interest_eth > 0:
                    entry.add_credit(self.ACCOUNTS["interest_income"], interest_eth, currency)

                entries.append(entry)

            elif is_fund_borrower:
                principal_eth = loan.principal_eth
                interest_eth = total_repayment_eth - principal_eth
                interest_eth = max(Decimal(0), interest_eth)

                entry = JournalEntry(
                    entry_id=f"JE_{tx_hash_short}_repay_borrower",
                    date=timestamp,
                    description=f"Gondi Loan Repayment: Borrower - Loan {loan_id}",
                    tx_hash=tx_hash,
                    category=TransactionCategory.LOAN_REPAYMENT,
                    platform=self.PLATFORM,
                    wallet_address=loan.borrower,
                    wallet_role="borrower",
                    eth_usd_price=eth_price,
                    posting_status=PostingStatus.AUTO_POST,
                )

                # Debit loan payable (debt cleared)
                entry.add_debit(self.ACCOUNTS["loan_payable"], principal_eth, currency)

                # Debit interest expense
                if interest_eth > 0:
                    entry.add_debit(self.ACCOUNTS["interest_expense"], interest_eth, currency)

                # Credit cash paid out
                entry.add_credit(self.ACCOUNTS["deemed_cash"], total_repayment_eth, currency)

                entries.append(entry)
                break

        return entries
