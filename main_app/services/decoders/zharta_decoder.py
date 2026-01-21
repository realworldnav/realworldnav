"""
Zharta Complete Decoder - All Contract Types
=============================================

Handles:
- Loans WETH Pool (LoanCreated, LoanPayment, LoanPaid, LoanDefaulted)
- USDC Loans V2 (LoanCreated, LoanPaid, LoanReplaced)
- P2PLendingNfts (LoanPaid, LoanReplaced, LoanReplacedByLender, LoanCollateralClaimed)
- Liquidations (LiquidationRemoved, AdminWithdrawal)

Author: Real World NAV
Version: 3.0.0
"""

from web3 import Web3
from decimal import Decimal, getcontext
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import pandas as pd


# Module exports
__all__ = [
    'ZhartaDecoder',
    'ZhartaJournalGenerator',
    'ZhartaEvent',
    'ZhartaEventType',
    'ZHARTA_CONTRACTS',
    'EVENT_SIGS',
    'decode_zharta_transactions',
]


# Set decimal precision
getcontext().prec = 28

# ============================================================================
# CONSTANTS
# ============================================================================

# Contract Addresses
ZHARTA_CONTRACTS = {
    # Lending Contracts
    "0x1Cf3DAB407aa14389f9C79b80B16E48cbc7246EE".lower(): {
        "name": "Loans_WETH_Pool",
        "type": "pool_lending",
        "token": "WETH",
        "decimals": 18,
    },
    "0x5F19431BC8A3eb21222771c6C867a63a119DeDA7".lower(): {
        "name": "Loans_USDC_V2",
        "type": "p2p_lending",
        "token": "USDC",
        "decimals": 6,
    },
    "0x8D0f9C9FA4c1b265cd5032FE6BA4FEfC9D94bAdb".lower(): {
        "name": "P2PLendingNfts",
        "type": "p2p_lending",
        "token": "WETH",
        "decimals": 18,
    },
    # Infrastructure
    "0x5Be916Cff5f07870e9Aef205960e07d9e287eF27".lower(): {
        "name": "LoansCore_WETH",
        "type": "core",
        "token": "WETH",
        "decimals": 18,
    },
    "0x04fc02deeee6f4fa51e11cc762e2e47ab8873ecc".lower(): {
        "name": "Liquidations",
        "type": "liquidations",
    },
    "0x7CA34cF45a119bEBEf4D106318402964a331DfeD".lower(): {
        "name": "CollateralVault",
        "type": "vault",
    },
}

# Event Signatures
EVENT_SIGS = {
    # Loans WETH Pool
    "0x4a558778654b4d21f09ae7e2aa4eebc0de757d1233dc825b43183a1276a7b2a1": ("Loans_WETH_Pool", "LoanCreated"),
    "0x31c401ba8a3eb75cf55e1d9f4971e726115e8448c80446935cffbea991ca2473": ("Loans_WETH_Pool", "LoanPayment"),
    "0x42d434e1d98bb8cb642015660476f098bbb0f00b64ddb556e149e17de4dd3645": ("Loans_WETH_Pool", "LoanPaid"),
    "0x098169d32c0f83653c192e3fe5e7da2ae5d6e98615fd0f767785098bea1f51b7": ("Loans_WETH_Pool", "LoanDefaulted"),

    # USDC Loans V2
    "0x6827a33d0a24e36314681156d8d9a7d20d6a0548c169735fe25e00c9d38ac5a9": ("Loans_USDC_V2", "LoanCreated"),
    "0x3104dd99ab576a709e2bea4bedb076e17210d16fdbc54a86b7db45e9f3be8284": ("Loans_USDC_V2", "LoanPaid"),
    "0xadf0e5d2eb7098352961e41ff94c8d5bd1e0d24910d7c8e7ae147610146fef21": ("Loans_USDC_V2", "LoanReplaced"),
    "0x08f7f4fedc8c9bd3165579676da5b715f2babe388ed555519fcae0e56c2e507d": ("Loans_USDC_V2", "OfferUsed"),

    # Liquidations Contract
    "0x91b4379267310bb0956d1c9efc9c2662461c9cf3b8cb6c3eeb3164caf5e41b43": ("Liquidations", "LiquidationRemoved"),
    "0xbd2b8aa77d05fddf73107d5d7f287cd596dd7e3f481879e7fced8a7dfeefca0c": ("Liquidations", "AdminWithdrawal"),
}

# Token addresses
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
USDC_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower()

TOKEN_DECIMALS = {
    WETH_ADDRESS: 18,
    USDC_ADDRESS: 6,
}

TOKEN_SYMBOLS = {
    WETH_ADDRESS: "WETH",
    USDC_ADDRESS: "USDC",
}


# ============================================================================
# DATA CLASSES
# ============================================================================

class ZhartaEventType(Enum):
    """All Zharta event types"""
    # Loans WETH Pool
    LOAN_CREATED_POOL = "LoanCreated_Pool"
    LOAN_PAYMENT = "LoanPayment"
    LOAN_PAID_POOL = "LoanPaid_Pool"
    LOAN_DEFAULTED = "LoanDefaulted"

    # USDC Loans V2
    LOAN_CREATED_P2P = "LoanCreated_P2P"
    LOAN_PAID_P2P = "LoanPaid_P2P"
    LOAN_REPLACED = "LoanReplaced"

    # Liquidations
    LIQUIDATION_REMOVED = "LiquidationRemoved"
    ADMIN_WITHDRAWAL = "AdminWithdrawal"


@dataclass
class ZhartaEvent:
    """Decoded Zharta event with all fields"""
    tx_hash: str
    block_number: int
    log_index: int
    timestamp: datetime
    contract_name: str
    event_type: str

    # Loan identification
    loan_id: str = ""
    liquidation_id: str = ""

    # Parties
    borrower: Optional[str] = None
    lender: Optional[str] = None

    # Amounts (in token units, not wei)
    principal: Decimal = Decimal(0)
    interest: Decimal = Decimal(0)
    total_amount: Decimal = Decimal(0)

    # Token info
    payment_token: str = ""
    token_symbol: str = ""
    token_decimals: int = 18

    # Collateral
    collateral_contract: Optional[str] = None
    collateral_token_id: Optional[int] = None

    # Loan terms
    duration_seconds: Optional[int] = None
    apr_bps: Optional[int] = None
    maturity: Optional[datetime] = None
    start_time: Optional[datetime] = None

    # Liquidation specific
    loans_core_contract: Optional[str] = None

    # Fund wallet flags
    is_fund_borrower: bool = False
    is_fund_lender: bool = False

    def to_dict(self) -> Dict:
        """Convert to dictionary for DataFrame"""
        return {
            'tx_hash': self.tx_hash,
            'block_number': self.block_number,
            'log_index': self.log_index,
            'timestamp': self.timestamp,
            'contract_name': self.contract_name,
            'event_type': self.event_type,
            'loan_id': self.loan_id,
            'liquidation_id': self.liquidation_id,
            'borrower': self.borrower,
            'lender': self.lender,
            'principal': float(self.principal),
            'interest': float(self.interest),
            'total_amount': float(self.total_amount),
            'payment_token': self.payment_token,
            'token_symbol': self.token_symbol,
            'collateral_contract': self.collateral_contract,
            'collateral_token_id': self.collateral_token_id,
            'duration_seconds': self.duration_seconds,
            'apr_bps': self.apr_bps,
            'maturity': self.maturity,
            'start_time': self.start_time,
            'loans_core_contract': self.loans_core_contract,
            'is_fund_borrower': self.is_fund_borrower,
            'is_fund_lender': self.is_fund_lender,
        }


# ============================================================================
# ZHARTA DECODER
# ============================================================================

class ZhartaDecoder:
    """
    Complete Zharta event decoder for all contract types.

    Handles:
    - Loans WETH Pool
    - USDC Loans V2
    - P2PLendingNfts
    - Liquidations
    """

    def __init__(self, w3: Web3, fund_wallets: List[str]):
        self.w3 = w3
        self.fund_wallets = [w.lower() for w in fund_wallets]
        self._block_cache: Dict[int, int] = {}

    def _get_block_timestamp(self, block_number: int) -> datetime:
        """Get block timestamp with caching"""
        if block_number not in self._block_cache:
            block = self.w3.eth.get_block(block_number)
            self._block_cache[block_number] = block['timestamp']
        return datetime.fromtimestamp(self._block_cache[block_number], tz=timezone.utc)

    def _parse_data_words(self, data: bytes) -> List[str]:
        """Parse log data into 32-byte hex words"""
        if isinstance(data, bytes):
            data_hex = data.hex()
        else:
            data_hex = str(data).replace('0x', '')
        return [data_hex[i:i+64] for i in range(0, len(data_hex), 64)]

    def _get_address(self, word: str) -> str:
        """Extract checksummed address from 32-byte word"""
        return Web3.to_checksum_address("0x" + word[-40:])

    def _get_int(self, word: str) -> int:
        """Convert hex word to integer"""
        return int(word, 16)

    def _is_fund_wallet(self, address: Optional[str]) -> bool:
        """Check if address is a fund wallet"""
        if not address:
            return False
        return address.lower() in self.fund_wallets

    def decode_transaction(self, tx_hash: str) -> List[ZhartaEvent]:
        """Decode all Zharta events from a transaction"""
        receipt = self.w3.eth.get_transaction_receipt(tx_hash)
        timestamp = self._get_block_timestamp(receipt['blockNumber'])

        events = []

        for log_idx, log in enumerate(receipt['logs']):
            contract_addr = log['address'].lower()

            # Skip if not a known Zharta contract
            if contract_addr not in ZHARTA_CONTRACTS:
                continue

            # Get event signature
            if not log['topics']:
                continue

            sig = log['topics'][0].hex()
            if not sig.startswith('0x'):
                sig = '0x' + sig

            # Look up event type
            event_info = EVENT_SIGS.get(sig)
            if not event_info:
                continue

            contract_name, event_type = event_info

            # Decode based on contract and event type
            try:
                if contract_name == "Loans_WETH_Pool":
                    event = self._decode_loans_weth_event(
                        log, event_type, tx_hash, receipt['blockNumber'], log_idx, timestamp
                    )
                elif contract_name == "Loans_USDC_V2":
                    event = self._decode_loans_usdc_event(
                        log, event_type, tx_hash, receipt['blockNumber'], log_idx, timestamp
                    )
                elif contract_name == "Liquidations":
                    event = self._decode_liquidation_event(
                        log, event_type, tx_hash, receipt['blockNumber'], log_idx, timestamp
                    )
                else:
                    continue

                if event:
                    # Set fund wallet flags
                    event.is_fund_borrower = self._is_fund_wallet(event.borrower)
                    event.is_fund_lender = self._is_fund_wallet(event.lender)
                    events.append(event)

            except Exception as e:
                print(f"Error decoding {event_type} in {tx_hash}: {e}")
                continue

        return events

    # =========================================================================
    # LOANS WETH POOL DECODER
    # =========================================================================

    def _decode_loans_weth_event(
        self, log, event_type: str, tx_hash: str, block: int, log_idx: int, timestamp: datetime
    ) -> Optional[ZhartaEvent]:
        """Decode Loans WETH Pool events"""
        words = self._parse_data_words(log['data'])

        if event_type == "LoanCreated":
            # Fields: wallet[0], loanId[1], erc20[2], apr[3], amount[4], duration[5],
            #         collaterals_offset[6], genesisToken[last]
            borrower = self._get_address(words[0])
            loan_id = self._get_int(words[1])
            apr_bps = self._get_int(words[3])
            amount_wei = self._get_int(words[4])
            duration = self._get_int(words[5])

            principal = Decimal(amount_wei) / Decimal(10**18)

            return ZhartaEvent(
                tx_hash=tx_hash,
                block_number=block,
                log_index=log_idx,
                timestamp=timestamp,
                contract_name="Loans_WETH_Pool",
                event_type="LoanCreated",
                loan_id=str(loan_id),
                borrower=borrower,
                principal=principal,
                total_amount=principal,
                payment_token=WETH_ADDRESS,
                token_symbol="WETH",
                token_decimals=18,
                apr_bps=apr_bps,
                duration_seconds=duration,
            )

        elif event_type == "LoanPayment":
            # Fields: wallet[0], loanId[1], principal[2], interestAmount[3], erc20[4]
            borrower = self._get_address(words[0])
            loan_id = self._get_int(words[1])
            principal_wei = self._get_int(words[2])
            interest_wei = self._get_int(words[3])

            principal = Decimal(principal_wei) / Decimal(10**18)
            interest = Decimal(interest_wei) / Decimal(10**18)

            return ZhartaEvent(
                tx_hash=tx_hash,
                block_number=block,
                log_index=log_idx,
                timestamp=timestamp,
                contract_name="Loans_WETH_Pool",
                event_type="LoanPayment",
                loan_id=str(loan_id),
                borrower=borrower,
                principal=principal,
                interest=interest,
                total_amount=principal + interest,
                payment_token=WETH_ADDRESS,
                token_symbol="WETH",
                token_decimals=18,
            )

        elif event_type == "LoanPaid":
            # Fields: wallet[0], loanId[1], erc20[2]
            borrower = self._get_address(words[0])
            loan_id = self._get_int(words[1])

            return ZhartaEvent(
                tx_hash=tx_hash,
                block_number=block,
                log_index=log_idx,
                timestamp=timestamp,
                contract_name="Loans_WETH_Pool",
                event_type="LoanPaid",
                loan_id=str(loan_id),
                borrower=borrower,
                payment_token=WETH_ADDRESS,
                token_symbol="WETH",
                token_decimals=18,
            )

        elif event_type == "LoanDefaulted":
            # Fields: wallet[0], loanId[1], amount[2], erc20[3]
            borrower = self._get_address(words[0])
            loan_id = self._get_int(words[1])
            amount_wei = self._get_int(words[2])

            amount = Decimal(amount_wei) / Decimal(10**18)

            return ZhartaEvent(
                tx_hash=tx_hash,
                block_number=block,
                log_index=log_idx,
                timestamp=timestamp,
                contract_name="Loans_WETH_Pool",
                event_type="LoanDefaulted",
                loan_id=str(loan_id),
                borrower=borrower,
                principal=amount,
                total_amount=amount,
                payment_token=WETH_ADDRESS,
                token_symbol="WETH",
                token_decimals=18,
            )

        return None

    # =========================================================================
    # USDC LOANS V2 DECODER
    # =========================================================================

    def _decode_loans_usdc_event(
        self, log, event_type: str, tx_hash: str, block: int, log_idx: int, timestamp: datetime
    ) -> Optional[ZhartaEvent]:
        """Decode USDC Loans V2 events"""
        words = self._parse_data_words(log['data'])

        if event_type == "LoanCreated":
            # Fields: loan_id[0], amount[1], interest[2], payment_token[3], maturity[4],
            #         start[5], collateral[6], token_id[7], borrower[8], lender[9], ...
            loan_id = "0x" + words[0]
            amount = self._get_int(words[1])
            interest = self._get_int(words[2])
            payment_token = self._get_address(words[3])
            maturity_ts = self._get_int(words[4])
            start_ts = self._get_int(words[5])
            collateral = self._get_address(words[6]) if len(words) > 6 else None
            token_id = self._get_int(words[7]) if len(words) > 7 else None
            borrower = self._get_address(words[8]) if len(words) > 8 else None
            lender = self._get_address(words[9]) if len(words) > 9 else None

            decimals = TOKEN_DECIMALS.get(payment_token.lower(), 6)
            symbol = TOKEN_SYMBOLS.get(payment_token.lower(), "USDC")

            principal = Decimal(amount) / Decimal(10**decimals)
            interest_amt = Decimal(interest) / Decimal(10**decimals)

            maturity = datetime.fromtimestamp(maturity_ts, tz=timezone.utc) if maturity_ts > 0 else None
            start_time = datetime.fromtimestamp(start_ts, tz=timezone.utc) if start_ts > 0 else None

            return ZhartaEvent(
                tx_hash=tx_hash,
                block_number=block,
                log_index=log_idx,
                timestamp=timestamp,
                contract_name="Loans_USDC_V2",
                event_type="LoanCreated",
                loan_id=loan_id,
                borrower=borrower,
                lender=lender,
                principal=principal,
                interest=interest_amt,
                total_amount=principal + interest_amt,
                payment_token=payment_token,
                token_symbol=symbol,
                token_decimals=decimals,
                collateral_contract=collateral,
                collateral_token_id=token_id,
                maturity=maturity,
                start_time=start_time,
            )

        elif event_type == "LoanPaid":
            # Fields: loan_id[0], paid_principal[1], paid_interest[2], payment_token[3], ...
            loan_id = "0x" + words[0]
            principal_raw = self._get_int(words[1])
            interest_raw = self._get_int(words[2])
            payment_token = self._get_address(words[3])

            decimals = TOKEN_DECIMALS.get(payment_token.lower(), 6)
            symbol = TOKEN_SYMBOLS.get(payment_token.lower(), "USDC")

            principal = Decimal(principal_raw) / Decimal(10**decimals)
            interest = Decimal(interest_raw) / Decimal(10**decimals)

            return ZhartaEvent(
                tx_hash=tx_hash,
                block_number=block,
                log_index=log_idx,
                timestamp=timestamp,
                contract_name="Loans_USDC_V2",
                event_type="LoanPaid",
                loan_id=loan_id,
                principal=principal,
                interest=interest,
                total_amount=principal + interest,
                payment_token=payment_token,
                token_symbol=symbol,
                token_decimals=decimals,
            )

        elif event_type == "LoanReplaced":
            # Fields: new_loan_id[0], new_principal[1], new_interest[2], payment_token[3],
            #         maturity[4], start[5], collateral[6], token_id[7], borrower[8], new_lender[9],
            #         offset[10], pro_rata[11], OLD_loan_id[12], paid_principal[13], paid_interest[14]

            # We care about the OLD loan being paid off
            old_loan_id = "0x" + words[12]
            paid_principal_raw = self._get_int(words[13])
            paid_interest_raw = self._get_int(words[14])
            payment_token = self._get_address(words[3])
            borrower = self._get_address(words[8]) if len(words) > 8 else None
            collateral = self._get_address(words[6]) if len(words) > 6 else None
            token_id = self._get_int(words[7]) if len(words) > 7 else None

            decimals = TOKEN_DECIMALS.get(payment_token.lower(), 6)
            symbol = TOKEN_SYMBOLS.get(payment_token.lower(), "USDC")

            principal = Decimal(paid_principal_raw) / Decimal(10**decimals)
            interest = Decimal(paid_interest_raw) / Decimal(10**decimals)

            return ZhartaEvent(
                tx_hash=tx_hash,
                block_number=block,
                log_index=log_idx,
                timestamp=timestamp,
                contract_name="Loans_USDC_V2",
                event_type="LoanReplaced",
                loan_id=old_loan_id,
                borrower=borrower,
                principal=principal,
                interest=interest,
                total_amount=principal + interest,
                payment_token=payment_token,
                token_symbol=symbol,
                token_decimals=decimals,
                collateral_contract=collateral,
                collateral_token_id=token_id,
            )

        return None

    # =========================================================================
    # LIQUIDATIONS DECODER
    # =========================================================================

    def _decode_liquidation_event(
        self, log, event_type: str, tx_hash: str, block: int, log_idx: int, timestamp: datetime
    ) -> Optional[ZhartaEvent]:
        """Decode Liquidations contract events"""
        words = self._parse_data_words(log['data'])

        if event_type == "LiquidationRemoved":
            # Indexed: erc20TokenContract[topic1], collateralAddress[topic2]
            # Data: liquidationId[0], collateralAddress[1], tokenId[2], erc20TokenContract[3],
            #       loansCoreContract[4], loanId[5], borrower[6]

            # Get indexed topics
            erc20_token = self._get_address(log['topics'][1].hex()) if len(log['topics']) > 1 else None

            liquidation_id = "0x" + words[0]
            collateral_contract = self._get_address(words[1])
            token_id = self._get_int(words[2])
            loans_core = self._get_address(words[4])
            loan_id = self._get_int(words[5])
            borrower = self._get_address(words[6])

            # Determine token
            symbol = TOKEN_SYMBOLS.get(erc20_token.lower(), "WETH") if erc20_token else "WETH"
            decimals = TOKEN_DECIMALS.get(erc20_token.lower(), 18) if erc20_token else 18

            return ZhartaEvent(
                tx_hash=tx_hash,
                block_number=block,
                log_index=log_idx,
                timestamp=timestamp,
                contract_name="Liquidations",
                event_type="LiquidationRemoved",
                loan_id=str(loan_id),
                liquidation_id=liquidation_id,
                borrower=borrower,
                collateral_contract=collateral_contract,
                collateral_token_id=token_id,
                loans_core_contract=loans_core,
                payment_token=erc20_token or WETH_ADDRESS,
                token_symbol=symbol,
                token_decimals=decimals,
            )

        elif event_type == "AdminWithdrawal":
            # Indexed: collateralAddress[topic1]
            # Data: liquidationId[0], collateralAddress[1], tokenId[2], wallet[3]

            liquidation_id = "0x" + words[0]
            collateral_contract = self._get_address(words[1])
            token_id = self._get_int(words[2])
            wallet = self._get_address(words[3])

            return ZhartaEvent(
                tx_hash=tx_hash,
                block_number=block,
                log_index=log_idx,
                timestamp=timestamp,
                contract_name="Liquidations",
                event_type="AdminWithdrawal",
                liquidation_id=liquidation_id,
                lender=wallet,  # The wallet receiving the NFT
                collateral_contract=collateral_contract,
                collateral_token_id=token_id,
            )

        return None

    # =========================================================================
    # BATCH PROCESSING
    # =========================================================================

    def decode_batch(
        self,
        tx_hashes: List[str],
        filter_fund_only: bool = True,
        show_progress: bool = True,
    ) -> pd.DataFrame:
        """
        Decode events from multiple transactions.

        Args:
            tx_hashes: List of transaction hashes
            filter_fund_only: Only return events involving fund wallets
            show_progress: Show progress bar

        Returns:
            DataFrame with decoded events
        """
        all_events = []

        iterator = tx_hashes
        if show_progress:
            from tqdm import tqdm
            iterator = tqdm(tx_hashes, desc="Decoding Zharta events")

        for tx_hash in iterator:
            try:
                events = self.decode_transaction(tx_hash)

                for event in events:
                    if filter_fund_only:
                        if not (event.is_fund_borrower or event.is_fund_lender):
                            continue
                    all_events.append(event.to_dict())

            except Exception as e:
                print(f"Error processing {tx_hash}: {e}")
                continue

        if not all_events:
            return pd.DataFrame()

        return pd.DataFrame(all_events)


# ============================================================================
# JOURNAL ENTRY GENERATOR
# ============================================================================

class ZhartaJournalGenerator:
    """
    Generate GAAP-compliant journal entries from decoded Zharta events.

    Account Names:
    - deemed_cash_weth / deemed_cash_usdc
    - loan_receivable_cryptocurrency_weth / _usdc
    - note_payable_cryptocurrency_weth / _usdc
    - interest_receivable_cryptocurrency_weth / _usdc
    - interest_payable_cryptocurrency_weth / _usdc
    - interest_income_cryptocurrency_weth / _usdc
    - interest_expense_cryptocurrency_weth / _usdc
    - investments_nfts_seized_collateral
    - bad_debt_expense_weth / _usdc
    """

    def __init__(self, fund_wallets: List[str]):
        self.fund_wallets = [w.lower() for w in fund_wallets]

    def generate_entries(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """Generate journal entries from decoded events DataFrame"""
        if events_df.empty:
            return pd.DataFrame()

        entries = []

        for _, row in events_df.iterrows():
            event_entries = self._process_event(row)
            entries.extend(event_entries)

        if not entries:
            return pd.DataFrame()

        return pd.DataFrame(entries)

    def _process_event(self, row: pd.Series) -> List[Dict]:
        """Process a single event and generate journal entries"""
        event_type = row['event_type']
        contract = row['contract_name']

        # Determine perspective (lender or borrower)
        is_lender = row.get('is_fund_lender', False)
        is_borrower = row.get('is_fund_borrower', False)

        entries = []

        # Loan Created
        if event_type == "LoanCreated":
            if is_lender:
                entries.extend(self._loan_created_lender(row))
            elif is_borrower:
                entries.extend(self._loan_created_borrower(row))

        # Loan Payment / LoanPaid (repayment)
        elif event_type in ["LoanPayment", "LoanPaid", "LoanReplaced"]:
            if is_lender:
                entries.extend(self._loan_repaid_lender(row))
            elif is_borrower:
                entries.extend(self._loan_repaid_borrower(row))

        # Loan Defaulted (foreclosure)
        elif event_type == "LoanDefaulted":
            if is_borrower:
                entries.extend(self._loan_defaulted_borrower(row))

        # Liquidation Removed (NFT claimed by lender)
        elif event_type == "LiquidationRemoved":
            if is_borrower:
                entries.extend(self._liquidation_borrower(row))

        return entries

    def _get_account_suffix(self, token_symbol: str) -> str:
        """Get account name suffix based on token"""
        return token_symbol.lower() if token_symbol else "weth"

    def _common_fields(self, row: pd.Series) -> Dict:
        """Extract common fields for journal entry"""
        return {
            'date': row['timestamp'],
            'tx_hash': row['tx_hash'],
            'block_number': row['block_number'],
            'log_index': row['log_index'],
            'contract_name': row['contract_name'],
            'event_type': row['event_type'],
            'loan_id': row.get('loan_id', ''),
            'borrower': row.get('borrower', ''),
            'lender': row.get('lender', ''),
            'token_symbol': row.get('token_symbol', 'WETH'),
            'collateral_contract': row.get('collateral_contract', ''),
            'collateral_token_id': row.get('collateral_token_id', ''),
            'platform': 'Zharta',
        }

    # =========================================================================
    # LENDER PERSPECTIVE
    # =========================================================================

    def _loan_created_lender(self, row: pd.Series) -> List[Dict]:
        """
        Lender originates a loan (cash out, receivable in)

        Dr loan_receivable_cryptocurrency_xxx  [principal]
        Cr deemed_cash_xxx                     [principal]
        """
        common = self._common_fields(row)
        suffix = self._get_account_suffix(row['token_symbol'])
        principal = Decimal(str(row['principal']))

        return [
            {
                **common,
                'account_name': f'loan_receivable_cryptocurrency_{suffix}',
                'debit_crypto': principal,
                'credit_crypto': Decimal(0),
                'transaction_type': 'investments_lending',
            },
            {
                **common,
                'account_name': f'deemed_cash_{suffix}',
                'debit_crypto': Decimal(0),
                'credit_crypto': principal,
                'transaction_type': 'investments_lending',
            },
        ]

    def _loan_repaid_lender(self, row: pd.Series) -> List[Dict]:
        """
        Lender receives repayment (cash in, receivable out, interest income)

        Dr deemed_cash_xxx                          [principal + interest]
        Cr loan_receivable_cryptocurrency_xxx       [principal]
        Cr interest_income_cryptocurrency_xxx       [interest]
        """
        common = self._common_fields(row)
        suffix = self._get_account_suffix(row['token_symbol'])
        principal = Decimal(str(row['principal']))
        interest = Decimal(str(row.get('interest', 0)))
        total = principal + interest

        entries = [
            {
                **common,
                'account_name': f'deemed_cash_{suffix}',
                'debit_crypto': total,
                'credit_crypto': Decimal(0),
                'transaction_type': 'investments_lending',
            },
            {
                **common,
                'account_name': f'loan_receivable_cryptocurrency_{suffix}',
                'debit_crypto': Decimal(0),
                'credit_crypto': principal,
                'transaction_type': 'investments_lending',
            },
        ]

        if interest > 0:
            entries.append({
                **common,
                'account_name': f'interest_income_cryptocurrency_{suffix}',
                'debit_crypto': Decimal(0),
                'credit_crypto': interest,
                'transaction_type': 'income_interest',
            })

        return entries

    # =========================================================================
    # BORROWER PERSPECTIVE
    # =========================================================================

    def _loan_created_borrower(self, row: pd.Series) -> List[Dict]:
        """
        Borrower receives loan (cash in, payable out)

        Dr deemed_cash_xxx                     [principal]
        Cr note_payable_cryptocurrency_xxx     [principal]
        """
        common = self._common_fields(row)
        suffix = self._get_account_suffix(row['token_symbol'])
        principal = Decimal(str(row['principal']))

        return [
            {
                **common,
                'account_name': f'deemed_cash_{suffix}',
                'debit_crypto': principal,
                'credit_crypto': Decimal(0),
                'transaction_type': 'financing_borrowings',
            },
            {
                **common,
                'account_name': f'note_payable_cryptocurrency_{suffix}',
                'debit_crypto': Decimal(0),
                'credit_crypto': principal,
                'transaction_type': 'financing_borrowings',
            },
        ]

    def _loan_repaid_borrower(self, row: pd.Series) -> List[Dict]:
        """
        Borrower repays loan (cash out, payable cleared, interest expense)

        Dr note_payable_cryptocurrency_xxx     [principal]
        Dr interest_expense_cryptocurrency_xxx [interest]
        Cr deemed_cash_xxx                     [principal + interest]
        """
        common = self._common_fields(row)
        suffix = self._get_account_suffix(row['token_symbol'])
        principal = Decimal(str(row['principal']))
        interest = Decimal(str(row.get('interest', 0)))
        total = principal + interest

        entries = [
            {
                **common,
                'account_name': f'note_payable_cryptocurrency_{suffix}',
                'debit_crypto': principal,
                'credit_crypto': Decimal(0),
                'transaction_type': 'financing_borrowings',
            },
        ]

        if interest > 0:
            entries.append({
                **common,
                'account_name': f'interest_expense_cryptocurrency_{suffix}',
                'debit_crypto': interest,
                'credit_crypto': Decimal(0),
                'transaction_type': 'expense_interest',
            })

        entries.append({
            **common,
            'account_name': f'deemed_cash_{suffix}',
            'debit_crypto': Decimal(0),
            'credit_crypto': total,
            'transaction_type': 'financing_borrowings',
        })

        return entries

    def _loan_defaulted_borrower(self, row: pd.Series) -> List[Dict]:
        """
        Borrower's loan defaults (payable cleared, NFT collateral forfeited)

        Dr note_payable_cryptocurrency_xxx         [principal]
        Dr interest_payable_cryptocurrency_xxx     [accrued interest if any]
        Cr investments_nfts                        [NFT book value]
        Cr gain_on_debt_extinguishment             [difference if any]

        Simplified: Just clear the payable
        Dr note_payable_cryptocurrency_xxx         [principal]
        Cr investments_nfts_collateral_forfeited   [principal]
        """
        common = self._common_fields(row)
        suffix = self._get_account_suffix(row['token_symbol'])
        principal = Decimal(str(row.get('principal', 0)))

        return [
            {
                **common,
                'account_name': f'note_payable_cryptocurrency_{suffix}',
                'debit_crypto': principal,
                'credit_crypto': Decimal(0),
                'transaction_type': 'financing_foreclosure',
            },
            {
                **common,
                'account_name': 'investments_nfts_collateral_forfeited',
                'debit_crypto': Decimal(0),
                'credit_crypto': principal,
                'transaction_type': 'financing_foreclosure',
            },
        ]

    def _liquidation_borrower(self, row: pd.Series) -> List[Dict]:
        """
        Borrower's collateral is liquidated (NFT transferred to lender)

        This event captures the NFT transfer, the loan payable should have
        been cleared in the LoanDefaulted event.

        For now, we just record the NFT forfeiture event.
        """
        common = self._common_fields(row)

        return [
            {
                **common,
                'account_name': 'investments_nfts_collateral_forfeited_memo',
                'debit_crypto': Decimal(0),
                'credit_crypto': Decimal(0),
                'transaction_type': 'memo_foreclosure',
                'memo': f"NFT {row.get('collateral_contract', '')}#{row.get('collateral_token_id', '')} transferred to lender",
            },
        ]


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def decode_zharta_transactions(
    w3: Web3,
    tx_hashes: List[str],
    fund_wallets: List[str],
    generate_journals: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main function to decode Zharta transactions and generate journal entries.

    Args:
        w3: Web3 instance
        tx_hashes: List of transaction hashes
        fund_wallets: List of fund wallet addresses
        generate_journals: Whether to generate journal entries

    Returns:
        (events_df, journals_df)
    """
    # Decode events
    decoder = ZhartaDecoder(w3, fund_wallets)
    events_df = decoder.decode_batch(tx_hashes)

    journals_df = pd.DataFrame()

    if generate_journals and not events_df.empty:
        generator = ZhartaJournalGenerator(fund_wallets)
        journals_df = generator.generate_entries(events_df)

    return events_df, journals_df
