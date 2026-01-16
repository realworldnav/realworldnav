"""
Blur Blend (NFT Lending) Decoder

Decodes Blur Blend NFT lending events and generates GAAP-compliant journal entries.
Tested and validated using debug_decoder.py workbench.

Key Characteristics:
- Callable loans with NO fixed term (open-ended)
- Interest calculated using continuous compounding: e^(rate * time)
- Proxy contract pattern - requires implementation ABI resolution

Events Handled:
- LoanOfferTaken: New loan origination
- Repay: Loan repayment by borrower
- Refinance: Loan refinanced to new lender
- StartAuction: Lender calls the loan
- Seize: Lender seizes NFT collateral
- BuyLocked: Third party purchases locked NFT

Contract Addresses:
- Blur Lending (Proxy): 0x29469395eAf6f95920E59F858042f0e28D98a20B
- Blur Lending (Impl):  0xB258CA5559b11cD702f363796522b04D7722ea56
- Blur Pool:            0x0000000000A39bb272e79075ade125fd351887Ac
"""

from web3 import Web3
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import logging
import math

from .base import (
    BaseDecoder,
    DecodedTransaction,
    DecodedEvent,
    JournalEntry,
    LoanPosition,
    TransactionCategory,
    PostingStatus,
    TaxTreatment,
    Platform,
    wei_to_eth,
    calculate_gas_fee,
)

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

# Contract addresses (lowercase)
BLUR_LENDING_PROXY = "0x29469395eaf6f95920e59f858042f0e28d98a20b"
BLUR_LENDING_IMPL = "0xb258ca5559b11cd702f363796522b04d7722ea56"
BLUR_POOL = "0x0000000000a39bb272e79075ade125fd351887ac"

# EIP-1967 implementation slot for proxy resolution
EIP1967_IMPL_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"

# Blur lending events
BLUR_EVENTS = [
    "LoanOfferTaken",  # New loan origination
    "Repay",           # Loan repayment
    "Refinance",       # Loan refinanced to new lender
    "StartAuction",    # Lender calls the loan
    "Seize",           # Lender seizes NFT collateral
    "BuyLocked",       # Third party purchases locked NFT
]

# Function selectors for Blur Lending
BLUR_FUNCTION_SELECTORS = {
    "a49c04be": "refinanceAuction",
    "c87df1c2": "repay",
    "9a4737b9": "borrow",
    "057f569e": "startAuction",
    "49ebc08d": "seize",
    "8a919639": "buyLocked",
    "a59c7762": "buyLockedETH",
    "82afab0e": "refinance",
    "5ba02ca4": "refinanceAuctionByOther",
}


# ============================================================================
# BLUR LIEN DATA CLASS
# ============================================================================

@dataclass
class BlurLien:
    """
    Blur Lien struct (9 fields):
    [0] lender - address
    [1] borrower - address
    [2] collection - NFT contract address
    [3] tokenId - NFT token ID
    [4] amount - principal in wei
    [5] startTime - loan start timestamp
    [6] rate - annual rate in basis points (continuous compounding)
    [7] auctionStartBlock - block when auction started (0 if not)
    [8] auctionDuration - duration of auction in blocks
    """
    lien_id: int = 0
    lender: str = ""
    borrower: str = ""
    collection: str = ""
    token_id: int = 0
    principal: int = 0  # wei
    start_time: int = 0  # unix timestamp
    rate: int = 0  # basis points
    auction_start_block: int = 0
    auction_duration: int = 0

    @classmethod
    def from_tuple(cls, data: tuple, lien_id: int = 0) -> 'BlurLien':
        """Create from ABI-decoded tuple"""
        return cls(
            lien_id=lien_id,
            lender=str(data[0]).lower() if data[0] else "",
            borrower=str(data[1]).lower() if data[1] else "",
            collection=str(data[2]).lower() if data[2] else "",
            token_id=int(data[3]) if data[3] else 0,
            principal=int(data[4]) if data[4] else 0,
            start_time=int(data[5]) if data[5] else 0,
            rate=int(data[6]) if data[6] else 0,
            auction_start_block=int(data[7]) if data[7] else 0,
            auction_duration=int(data[8]) if data[8] else 0,
        )

    @classmethod
    def from_dict(cls, data: dict, lien_id: int = 0) -> 'BlurLien':
        """Create from dictionary/AttributeDict"""
        return cls(
            lien_id=lien_id,
            lender=str(data.get('lender', '')).lower(),
            borrower=str(data.get('borrower', '')).lower(),
            collection=str(data.get('collection', '')).lower(),
            token_id=int(data.get('tokenId', 0)),
            principal=int(data.get('amount', 0)),
            start_time=int(data.get('startTime', 0)),
            rate=int(data.get('rate', 0)),
            auction_start_block=int(data.get('auctionStartBlock', 0)),
            auction_duration=int(data.get('auctionDuration', 0)),
        )

    def get_principal_eth(self) -> Decimal:
        """Get principal in ETH"""
        return Decimal(self.principal) / Decimal(10**18)

    def calculate_interest(self, as_of_timestamp: int) -> int:
        """
        Calculate accrued interest using continuous compounding.
        Formula: principal * (e^(rate * time_in_years) - 1)
        """
        if self.principal == 0 or self.rate == 0 or self.start_time == 0:
            return 0

        time_elapsed_seconds = max(0, as_of_timestamp - self.start_time)
        if time_elapsed_seconds == 0:
            return 0

        seconds_per_year = Decimal(365 * 24 * 3600)
        rate_decimal = Decimal(self.rate) / Decimal(10000)
        time_in_years = Decimal(time_elapsed_seconds) / seconds_per_year

        # Continuous compounding: e^(r*t)
        exponent = float(rate_decimal * time_in_years)
        compound_factor = Decimal(str(math.exp(exponent)))

        # Interest = principal * (e^(r*t) - 1)
        interest = int(Decimal(self.principal) * (compound_factor - Decimal(1)))
        return interest

    def get_interest_eth(self, as_of_timestamp: int) -> Decimal:
        """Get interest in ETH"""
        interest_wei = self.calculate_interest(as_of_timestamp)
        return Decimal(interest_wei) / Decimal(10**18)

    def get_total_due(self, as_of_timestamp: int) -> int:
        """Get total amount due (principal + interest) in wei"""
        return self.principal + self.calculate_interest(as_of_timestamp)

    def get_total_due_eth(self, as_of_timestamp: int) -> Decimal:
        """Get total amount due in ETH"""
        return Decimal(self.get_total_due(as_of_timestamp)) / Decimal(10**18)

    def to_loan_position(self, timestamp: datetime) -> LoanPosition:
        """Convert to LoanPosition for compatibility with base classes"""
        return LoanPosition(
            lien_id=self.lien_id,
            lender=self.lender,
            borrower=self.borrower,
            collection=self.collection,
            token_id=self.token_id,
            principal=self.get_principal_eth(),
            rate=Decimal(self.rate),
            start_time=datetime.fromtimestamp(self.start_time, tz=timezone.utc),
            duration=0,  # Blur loans are callable, no fixed duration
            auction_duration=self.auction_duration,
            status="IN_AUCTION" if self.auction_start_block > 0 else "ACTIVE",
            platform=Platform.BLUR
        )


# ============================================================================
# BLUR DECODER
# ============================================================================

class BlurDecoder(BaseDecoder):
    """
    Blur Blend NFT lending decoder.

    Decodes Blur lending protocol events and generates GAAP-compliant journal entries
    for fund accounting. Uses process_log() pattern for reliable event decoding.
    """

    PLATFORM = Platform.BLUR
    CONTRACT_ADDRESSES = [BLUR_LENDING_PROXY, BLUR_POOL]

    # Chart of Accounts
    ACCOUNTS = {
        "nft_collateral": "100.10 - NFT Collateral",
        "loans_receivable": "100.20 - Loans Receivable",
        "eth_wallet": "100.30 - ETH Wallet",
        "blur_pool": "100.31 - Blur Pool Balance",
        "accrued_interest": "100.40 - Accrued Interest Receivable",
        "loan_payable": "200.10 - Loan Payable",
        "accrued_interest_payable": "200.20 - Accrued Interest Payable",
        "interest_income": "400.10 - Interest Income",
        "liquidation_gains": "400.20 - Liquidation Gains",
        "interest_expense": "500.10 - Interest Expense",
        "gas_fees": "500.20 - Gas Fees",
        "liquidation_losses": "500.30 - Liquidation Losses",
        "protocol_fees": "500.40 - Protocol Fees"
    }

    # Auto-post categories for Blur
    AUTO_POST_CATEGORIES = {
        TransactionCategory.LOAN_ORIGINATION,
        TransactionCategory.LOAN_REPAYMENT,
        TransactionCategory.LOAN_REFINANCE,
    }

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        self._abi_cache: Dict[str, Any] = {}
        self._impl_cache: Dict[str, str] = {}  # Proxy -> implementation mapping
        self.blur_lending_contract = None
        self.blur_pool_contract = None
        super().__init__(w3, fund_wallets)

    def _load_abis(self):
        """Load Blur contract ABIs with proxy resolution"""
        try:
            # Resolve proxy to implementation
            impl_address = self._get_implementation_address(BLUR_LENDING_PROXY)
            if impl_address:
                logger.info(f"Blur Lending proxy -> implementation: {impl_address[:16]}...")
            else:
                impl_address = BLUR_LENDING_IMPL  # Fallback to known implementation

            # Load implementation ABI
            abi = self._load_abi_from_s3(impl_address)
            if abi:
                self.blur_lending_contract = self.w3.eth.contract(
                    address=Web3.to_checksum_address(BLUR_LENDING_PROXY),
                    abi=abi
                )
                logger.info(f"Blur Lending contract loaded with {len(abi)} ABI entries")

            # Load Blur Pool ABI (also a proxy)
            pool_impl = self._get_implementation_address(BLUR_POOL)
            pool_abi = self._load_abi_from_s3(pool_impl or BLUR_POOL)
            if pool_abi:
                self.blur_pool_contract = self.w3.eth.contract(
                    address=Web3.to_checksum_address(BLUR_POOL),
                    abi=pool_abi
                )
                logger.info("Blur Pool contract loaded")

        except Exception as e:
            logger.error(f"Failed to load Blur contracts: {e}")

    def _get_implementation_address(self, proxy_address: str) -> Optional[str]:
        """Resolve proxy contract to implementation address"""
        proxy_address = proxy_address.lower()

        # Check cache
        if proxy_address in self._impl_cache:
            return self._impl_cache[proxy_address]

        try:
            proxy_checksum = Web3.to_checksum_address(proxy_address)

            # EIP-1967 implementation slot
            raw_impl = self.w3.eth.get_storage_at(proxy_checksum, EIP1967_IMPL_SLOT)
            if raw_impl and raw_impl != b'\x00' * 32:
                impl = '0x' + raw_impl.hex()[-40:]
                if impl != '0x' + '0' * 40:
                    impl = impl.lower()
                    self._impl_cache[proxy_address] = impl
                    return impl

        except Exception as e:
            logger.debug(f"Could not resolve proxy {proxy_address}: {e}")

        return None

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

    def can_decode(self, tx: Dict, receipt: Dict) -> bool:
        """Check if transaction involves Blur contracts"""
        to_address = (tx.get('to') or '').lower()

        # Direct contract interaction
        if to_address in [BLUR_LENDING_PROXY, BLUR_POOL]:
            return True

        # Check function selector
        input_data = tx.get('input', '0x')
        if isinstance(input_data, bytes):
            input_data = input_data.hex()
        if len(input_data) >= 10:
            selector = input_data[2:10] if input_data.startswith('0x') else input_data[:8]
            if selector in BLUR_FUNCTION_SELECTORS:
                return True

        # Check logs for Blur events
        for log in receipt.get('logs', []):
            log_addr = (log.get('address') or '').lower()
            if log_addr in [BLUR_LENDING_PROXY, BLUR_POOL]:
                return True

        return False

    def decode(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal) -> DecodedTransaction:
        """Decode Blur transaction with full journal entry generation"""
        try:
            timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)
            block_timestamp = block.get('timestamp', 0)
            gas_fee = calculate_gas_fee(receipt, tx)
            tx_hash = tx.get('hash', b'')
            if isinstance(tx_hash, bytes):
                tx_hash = tx_hash.hex()
            if not tx_hash.startswith('0x'):
                tx_hash = '0x' + tx_hash

            # Decode function call
            func_name, func_params = self._decode_function(tx)

            # Extract lien from function params
            lien = self._extract_lien_from_params(func_params)

            # Decode events using process_log() pattern
            events = self._decode_events(receipt, block_timestamp, lien)

            # Determine category
            category = self._determine_category(func_name, events)

            # Determine wallet roles
            wallet_roles = self._determine_wallet_roles(tx, events, lien)

            # Generate journal entries
            journal_entries = self._generate_journal_entries(
                tx=tx,
                receipt=receipt,
                timestamp=timestamp,
                block_timestamp=block_timestamp,
                eth_price=eth_price,
                events=events,
                lien=lien,
                wallet_roles=wallet_roles,
                tx_hash=tx_hash,
            )

            # Store lien as position
            positions = {}
            if lien:
                positions[lien.lien_id] = lien.to_loan_position(timestamp)

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
                function_name=func_name,
                function_params=func_params,
                events=events,
                journal_entries=journal_entries,
                wallet_roles=wallet_roles,
                positions=positions,
            )

        except Exception as e:
            logger.error(f"Error decoding Blur transaction: {e}")
            return self._create_error_result(
                tx.get('hash', b'').hex() if isinstance(tx.get('hash'), bytes) else str(tx.get('hash', '')),
                str(e)
            )

    def _decode_function(self, tx: Dict) -> Tuple[str, Dict]:
        """Decode function call from transaction input"""
        if not self.blur_lending_contract:
            return "unknown", {}

        input_data = tx.get('input', '0x')
        if isinstance(input_data, bytes):
            input_data = '0x' + input_data.hex()
        elif not input_data.startswith('0x'):
            input_data = '0x' + input_data

        if len(input_data) <= 10:
            return "unknown", {}

        try:
            func_obj, func_params = self.blur_lending_contract.decode_function_input(input_data)
            return func_obj.fn_name, dict(func_params)
        except Exception as e:
            logger.debug(f"Failed to decode Blur function: {e}")

            # Fallback: try selector lookup
            selector = input_data[2:10]
            if selector in BLUR_FUNCTION_SELECTORS:
                return BLUR_FUNCTION_SELECTORS[selector], {}

            return "unknown", {}

    def _extract_lien_from_params(self, func_params: Dict) -> Optional[BlurLien]:
        """Extract BlurLien from function parameters"""
        if not func_params:
            return None

        lien_data = func_params.get('lien')
        lien_id = func_params.get('lienId', 0)

        if not lien_data:
            return None

        try:
            if hasattr(lien_data, 'keys') or isinstance(lien_data, dict):
                return BlurLien.from_dict(dict(lien_data), lien_id=lien_id)
            elif isinstance(lien_data, tuple):
                return BlurLien.from_tuple(lien_data, lien_id=lien_id)
        except Exception as e:
            logger.warning(f"Could not parse lien from func_params: {e}")

        return None

    def _decode_events(self, receipt: Dict, block_timestamp: int,
                       func_lien: Optional[BlurLien] = None) -> List[DecodedEvent]:
        """
        Decode events from transaction receipt using process_log() pattern.
        """
        events = []

        for log in receipt.get('logs', []):
            log_address = (log.get('address') or '').lower()

            # Get appropriate contract/ABI for this log
            contract = None
            if log_address == BLUR_LENDING_PROXY:
                contract = self.blur_lending_contract
            elif log_address == BLUR_POOL:
                contract = self.blur_pool_contract
            else:
                # Try to load ABI for unknown contract
                impl = self._get_implementation_address(log_address) or log_address
                abi = self._load_abi_from_s3(impl)
                if abi:
                    try:
                        contract = self.w3.eth.contract(
                            address=Web3.to_checksum_address(log_address),
                            abi=abi
                        )
                    except Exception:
                        pass

            if not contract:
                continue

            # Get event names from ABI
            try:
                abi = contract.abi if hasattr(contract, 'abi') else []
                event_names = [item['name'] for item in abi if item.get('type') == 'event']
            except Exception:
                event_names = BLUR_EVENTS + ['Transfer', 'Approval', 'Deposit', 'Withdraw']

            # Try each event type with process_log()
            for evt_name in event_names:
                try:
                    event_obj = getattr(contract.events, evt_name, None)
                    if not event_obj:
                        continue

                    decoded = event_obj().process_log(log)
                    args = dict(decoded.get('args', {}))

                    # Convert args to JSON-safe format
                    safe_args = {}
                    for k, v in args.items():
                        if isinstance(v, bytes):
                            safe_args[k] = v.hex()
                        elif isinstance(v, int):
                            safe_args[k] = v
                        else:
                            safe_args[k] = str(v) if v else ""

                    # Create decoded event
                    event = DecodedEvent(
                        name=evt_name,
                        args=safe_args,
                        log_index=log.get('logIndex', -1),
                        contract_address=log_address,
                        topic=log['topics'][0].hex() if log.get('topics') else None
                    )

                    # Attach lien data for Blur events
                    if evt_name in BLUR_EVENTS and func_lien:
                        # Store lien reference in args for journal generation
                        event.args['_blur_lien'] = func_lien

                    events.append(event)
                    break  # Found matching event

                except Exception:
                    continue

        return events

    def _determine_category(self, func_name: str, events: List[DecodedEvent]) -> TransactionCategory:
        """Determine transaction category from function and events"""
        # Event-based categorization (primary)
        for event in events:
            if event.name == 'LoanOfferTaken':
                return TransactionCategory.LOAN_ORIGINATION
            elif event.name == 'Repay':
                return TransactionCategory.LOAN_REPAYMENT
            elif event.name == 'Refinance':
                return TransactionCategory.LOAN_REFINANCE
            elif event.name == 'StartAuction':
                return TransactionCategory.LOAN_AUCTION
            elif event.name == 'Seize':
                return TransactionCategory.COLLATERAL_SEIZURE
            elif event.name == 'BuyLocked':
                return TransactionCategory.CONTRACT_CALL

        # Function-based fallback
        func_categories = {
            'borrow': TransactionCategory.LOAN_ORIGINATION,
            'repay': TransactionCategory.LOAN_REPAYMENT,
            'refinance': TransactionCategory.LOAN_REFINANCE,
            'refinanceAuction': TransactionCategory.LOAN_REFINANCE,
            'refinanceAuctionByOther': TransactionCategory.LOAN_REFINANCE,
            'startAuction': TransactionCategory.LOAN_AUCTION,
            'seize': TransactionCategory.COLLATERAL_SEIZURE,
            'buyLocked': TransactionCategory.CONTRACT_CALL,
            'buyLockedETH': TransactionCategory.CONTRACT_CALL,
        }

        return func_categories.get(func_name, TransactionCategory.CONTRACT_CALL)

    def _determine_wallet_roles(self, tx: Dict, events: List[DecodedEvent],
                                 lien: Optional[BlurLien]) -> Dict[str, str]:
        """Determine fund wallet roles in the transaction"""
        roles = {}
        from_addr = tx.get('from', '').lower()

        # Check lien participants
        if lien:
            if self.is_fund_wallet(lien.lender):
                roles[lien.lender] = "LENDER"
            if self.is_fund_wallet(lien.borrower):
                roles[lien.borrower] = "BORROWER"

        # Check event participants
        for event in events:
            args = event.args

            if event.name == 'Refinance':
                new_lender = str(args.get('newLender', '')).lower()
                if self.is_fund_wallet(new_lender):
                    roles[new_lender] = "NEW_LENDER"

            elif event.name == 'LoanOfferTaken':
                lender = str(args.get('lender', '')).lower()
                borrower = str(args.get('borrower', '')).lower()
                if self.is_fund_wallet(lender):
                    roles[lender] = "LENDER"
                if self.is_fund_wallet(borrower):
                    roles[borrower] = "BORROWER"

        # Transaction sender role
        if from_addr in self.fund_wallets and from_addr not in roles:
            roles[from_addr] = "SENDER"

        return roles

    def _generate_journal_entries(
        self,
        tx: Dict,
        receipt: Dict,
        timestamp: datetime,
        block_timestamp: int,
        eth_price: Decimal,
        events: List[DecodedEvent],
        lien: Optional[BlurLien],
        wallet_roles: Dict[str, str],
        tx_hash: str,
    ) -> List[JournalEntry]:
        """Generate double-entry journal entries for all events"""
        entries = []
        tx_hash_short = tx_hash[2:12] if tx_hash.startswith('0x') else tx_hash[:10]

        for event in events:
            if event.name == 'LoanOfferTaken':
                entries.extend(self._journal_loan_origination(
                    event, timestamp, block_timestamp, eth_price, tx_hash, tx_hash_short, lien
                ))
            elif event.name == 'Repay':
                entries.extend(self._journal_loan_repayment(
                    event, timestamp, block_timestamp, eth_price, tx_hash, tx_hash_short, lien
                ))
            elif event.name == 'Refinance':
                entries.extend(self._journal_loan_refinance(
                    event, timestamp, block_timestamp, eth_price, tx_hash, tx_hash_short, lien
                ))
            elif event.name == 'Seize':
                entries.extend(self._journal_collateral_seizure(
                    event, timestamp, block_timestamp, eth_price, tx_hash, tx_hash_short, lien
                ))

        # Gas fee entry (if fund initiated transaction)
        gas_entry = self._generate_gas_entry(tx, receipt, timestamp, eth_price, tx_hash, tx_hash_short)
        if gas_entry:
            entries.append(gas_entry)

        return entries

    def _journal_loan_origination(
        self, event: DecodedEvent, timestamp: datetime, block_timestamp: int,
        eth_price: Decimal, tx_hash: str, tx_hash_short: str,
        lien: Optional[BlurLien]
    ) -> List[JournalEntry]:
        """Generate journal entries for loan origination (LoanOfferTaken)"""
        entries = []
        args = event.args

        # Get lien from event args or function params
        blur_lien = args.get('_blur_lien') or lien
        if not blur_lien:
            return entries

        principal_eth = blur_lien.get_principal_eth()
        lien_id = blur_lien.lien_id

        # LENDER perspective
        if self.is_fund_wallet(blur_lien.lender):
            entry = JournalEntry(
                entry_id=f"JE_{tx_hash_short}_blur_orig_lender",
                date=timestamp,
                description=f"Blur Loan Origination: Lender - {principal_eth:.6f} ETH to {blur_lien.borrower[:10]}...",
                tx_hash=tx_hash,
                category=TransactionCategory.LOAN_ORIGINATION,
                platform=Platform.BLUR,
                wallet_address=blur_lien.lender,
                wallet_role="LENDER",
                eth_usd_price=eth_price,
                posting_status=PostingStatus.AUTO_POST,
            )
            entry.add_debit(self.ACCOUNTS["loans_receivable"], principal_eth, "ETH")
            entry.add_credit(self.ACCOUNTS["blur_pool"], principal_eth, "ETH")
            entries.append(entry)

        # BORROWER perspective
        if self.is_fund_wallet(blur_lien.borrower):
            entry = JournalEntry(
                entry_id=f"JE_{tx_hash_short}_blur_orig_borrower",
                date=timestamp,
                description=f"Blur Loan Received: Borrower - {principal_eth:.6f} ETH from {blur_lien.lender[:10]}...",
                tx_hash=tx_hash,
                category=TransactionCategory.LOAN_ORIGINATION,
                platform=Platform.BLUR,
                wallet_address=blur_lien.borrower,
                wallet_role="BORROWER",
                eth_usd_price=eth_price,
                posting_status=PostingStatus.AUTO_POST,
            )
            entry.add_debit(self.ACCOUNTS["blur_pool"], principal_eth, "ETH")
            entry.add_credit(self.ACCOUNTS["loan_payable"], principal_eth, "ETH")
            entry.add_tax_implication(TaxTreatment.NON_TAXABLE, principal_eth, "Loan proceeds received")
            entries.append(entry)

        return entries

    def _journal_loan_repayment(
        self, event: DecodedEvent, timestamp: datetime, block_timestamp: int,
        eth_price: Decimal, tx_hash: str, tx_hash_short: str,
        lien: Optional[BlurLien]
    ) -> List[JournalEntry]:
        """Generate journal entries for loan repayment (Repay)"""
        entries = []
        args = event.args

        # Get lien from event args or function params
        blur_lien = args.get('_blur_lien') or lien
        if not blur_lien:
            return entries

        principal_eth = blur_lien.get_principal_eth()
        interest_eth = blur_lien.get_interest_eth(block_timestamp)
        total_eth = principal_eth + interest_eth

        # LENDER perspective (receiving repayment)
        if self.is_fund_wallet(blur_lien.lender):
            entry = JournalEntry(
                entry_id=f"JE_{tx_hash_short}_blur_repay_lender",
                date=timestamp,
                description=f"Blur Loan Repaid: Lender receives {total_eth:.6f} ETH (P: {principal_eth:.6f}, I: {interest_eth:.6f})",
                tx_hash=tx_hash,
                category=TransactionCategory.LOAN_REPAYMENT,
                platform=Platform.BLUR,
                wallet_address=blur_lien.lender,
                wallet_role="LENDER",
                eth_usd_price=eth_price,
                posting_status=PostingStatus.AUTO_POST,
            )
            entry.add_debit(self.ACCOUNTS["blur_pool"], total_eth, "ETH")
            entry.add_credit(self.ACCOUNTS["loans_receivable"], principal_eth, "ETH")
            if interest_eth > 0:
                entry.add_credit(self.ACCOUNTS["interest_income"], interest_eth, "ETH")
                entry.add_tax_implication(TaxTreatment.TAXABLE_INCOME, interest_eth, "Interest income on loan")
            entries.append(entry)

        # BORROWER perspective (making repayment)
        if self.is_fund_wallet(blur_lien.borrower):
            entry = JournalEntry(
                entry_id=f"JE_{tx_hash_short}_blur_repay_borrower",
                date=timestamp,
                description=f"Blur Loan Repaid: Borrower pays {total_eth:.6f} ETH (P: {principal_eth:.6f}, I: {interest_eth:.6f})",
                tx_hash=tx_hash,
                category=TransactionCategory.LOAN_REPAYMENT,
                platform=Platform.BLUR,
                wallet_address=blur_lien.borrower,
                wallet_role="BORROWER",
                eth_usd_price=eth_price,
                posting_status=PostingStatus.AUTO_POST,
            )
            entry.add_debit(self.ACCOUNTS["loan_payable"], principal_eth, "ETH")
            if interest_eth > 0:
                entry.add_debit(self.ACCOUNTS["interest_expense"], interest_eth, "ETH")
                entry.add_tax_implication(TaxTreatment.DEDUCTIBLE_EXPENSE, interest_eth, "Interest expense on loan")
            entry.add_credit(self.ACCOUNTS["blur_pool"], total_eth, "ETH")
            entries.append(entry)

        return entries

    def _journal_loan_refinance(
        self, event: DecodedEvent, timestamp: datetime, block_timestamp: int,
        eth_price: Decimal, tx_hash: str, tx_hash_short: str,
        lien: Optional[BlurLien]
    ) -> List[JournalEntry]:
        """Generate journal entries for loan refinance (Refinance)"""
        entries = []
        args = event.args

        # Get lien from event args or function params
        blur_lien = args.get('_blur_lien') or lien
        if not blur_lien:
            return entries

        principal_eth = blur_lien.get_principal_eth()
        interest_eth = blur_lien.get_interest_eth(block_timestamp)
        total_eth = principal_eth + interest_eth

        # Get new lender from event
        new_lender = str(args.get('newLender', '')).lower()
        old_lender = blur_lien.lender

        # OLD LENDER perspective (getting paid off)
        if self.is_fund_wallet(old_lender):
            entry = JournalEntry(
                entry_id=f"JE_{tx_hash_short}_blur_refi_old_lender",
                date=timestamp,
                description=f"Blur Loan Refinanced Out: Old lender receives {total_eth:.6f} ETH",
                tx_hash=tx_hash,
                category=TransactionCategory.LOAN_REFINANCE,
                platform=Platform.BLUR,
                wallet_address=old_lender,
                wallet_role="OLD_LENDER",
                eth_usd_price=eth_price,
                posting_status=PostingStatus.AUTO_POST,
            )
            entry.add_debit(self.ACCOUNTS["blur_pool"], total_eth, "ETH")
            entry.add_credit(self.ACCOUNTS["loans_receivable"], principal_eth, "ETH")
            if interest_eth > 0:
                entry.add_credit(self.ACCOUNTS["interest_income"], interest_eth, "ETH")
                entry.add_tax_implication(TaxTreatment.TAXABLE_INCOME, interest_eth, "Interest income on refinanced loan")
            entries.append(entry)

        # NEW LENDER perspective (providing funds)
        if self.is_fund_wallet(new_lender):
            # New loan amount may be different - check event args
            new_amount = args.get('newAmount')
            if new_amount:
                new_amount_eth = Decimal(new_amount) / Decimal(10**18)
            else:
                new_amount_eth = total_eth

            entry = JournalEntry(
                entry_id=f"JE_{tx_hash_short}_blur_refi_new_lender",
                date=timestamp,
                description=f"Blur Loan Refinanced In: New lender provides {new_amount_eth:.6f} ETH",
                tx_hash=tx_hash,
                category=TransactionCategory.LOAN_REFINANCE,
                platform=Platform.BLUR,
                wallet_address=new_lender,
                wallet_role="NEW_LENDER",
                eth_usd_price=eth_price,
                posting_status=PostingStatus.AUTO_POST,
            )
            entry.add_debit(self.ACCOUNTS["loans_receivable"], new_amount_eth, "ETH")
            entry.add_credit(self.ACCOUNTS["blur_pool"], new_amount_eth, "ETH")
            entries.append(entry)

        return entries

    def _journal_collateral_seizure(
        self, event: DecodedEvent, timestamp: datetime, block_timestamp: int,
        eth_price: Decimal, tx_hash: str, tx_hash_short: str,
        lien: Optional[BlurLien]
    ) -> List[JournalEntry]:
        """Generate journal entries for collateral seizure (Seize)"""
        entries = []
        args = event.args

        # Get lien from event args or function params
        blur_lien = args.get('_blur_lien') or lien
        if not blur_lien:
            return entries

        principal_eth = blur_lien.get_principal_eth()
        interest_eth = blur_lien.get_interest_eth(block_timestamp)
        total_eth = principal_eth + interest_eth

        # LENDER perspective (seizing collateral)
        if self.is_fund_wallet(blur_lien.lender):
            entry = JournalEntry(
                entry_id=f"JE_{tx_hash_short}_blur_seize_lender",
                date=timestamp,
                description=f"Blur Collateral Seized: Lender receives NFT {blur_lien.collection[:10]}...#{blur_lien.token_id}",
                tx_hash=tx_hash,
                category=TransactionCategory.COLLATERAL_SEIZURE,
                platform=Platform.BLUR,
                wallet_address=blur_lien.lender,
                wallet_role="LENDER",
                eth_usd_price=eth_price,
                posting_status=PostingStatus.REVIEW_QUEUE,  # NFT valuation needs review
            )
            # NFT valued at total due amount
            entry.add_debit(self.ACCOUNTS["nft_collateral"], total_eth, "NFT")
            entry.add_credit(self.ACCOUNTS["loans_receivable"], principal_eth, "ETH")
            if interest_eth > 0:
                entry.add_credit(self.ACCOUNTS["interest_income"], interest_eth, "ETH")
            entries.append(entry)

        return entries

    def _generate_gas_entry(
        self, tx: Dict, receipt: Dict, timestamp: datetime,
        eth_price: Decimal, tx_hash: str, tx_hash_short: str
    ) -> Optional[JournalEntry]:
        """Generate gas fee journal entry"""
        gas_fee = calculate_gas_fee(receipt, tx)

        if gas_fee <= 0:
            return None

        from_addr = tx.get('from', '').lower()
        if from_addr not in self.fund_wallets:
            return None

        entry = JournalEntry(
            entry_id=f"JE_{tx_hash_short}_gas",
            date=timestamp,
            description=f"Gas Fee: {gas_fee:.6f} ETH",
            tx_hash=tx_hash,
            category=TransactionCategory.ETH_TRANSFER,
            platform=Platform.BLUR,
            wallet_address=from_addr,
            wallet_role="PAYER",
            eth_usd_price=eth_price,
            posting_status=PostingStatus.AUTO_POST,
        )
        entry.add_debit(self.ACCOUNTS["gas_fees"], gas_fee, "ETH")
        entry.add_credit(self.ACCOUNTS["eth_wallet"], gas_fee, "ETH")
        entry.add_tax_implication(TaxTreatment.DEDUCTIBLE_EXPENSE, gas_fee, "Gas fees")

        return entry
