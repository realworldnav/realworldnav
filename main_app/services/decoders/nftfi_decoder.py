"""
NFTfi NFT Lending Decoder

Decodes NFTfi NFT-collateralized loan transactions including:
- LoanStarted: New loan origination
- LoanRepaid: Loan repayment
- LoanLiquidated: Collateral seized (default)

Supports multiple contract versions:
- DirectLoanFixedOffer V1, V2, V2.1
- DirectLoanCoordinator

Uses raw event data parsing since event signatures vary by version.
Fixed-term loans with simple interest (not continuous compounding like Blur).
"""

from web3 import Web3
from web3.datastructures import AttributeDict
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import logging

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
# NFTFI CONSTANTS
# ============================================================================

# Contract addresses (multiple versions)
NFTFI_COORDINATOR = "0xe52cec0e90115abeb3304baa36bc2655731f7934"  # DirectLoanCoordinator
NFTFI_V2 = "0x8252df1d8b29057d1afe3062bf5a64d503152bc8"  # DirectLoanFixedOffer V2
NFTFI_V1 = "0xf896527c49b44aab3cf22ae356fa3af8e331f280"  # DirectLoanFixedOffer V1
NFTFI_V2_1 = "0xd0a40eb7fcd530a13866b9e893e4a9e0d15d03eb"  # DirectLoanFixedOfferRedeploy

NFTFI_CONTRACTS = [
    NFTFI_COORDINATOR,
    NFTFI_V2,
    NFTFI_V1,
    NFTFI_V2_1,
]

# Event names
NFTFI_EVENTS = [
    "LoanStarted",
    "LoanRepaid",
    "LoanLiquidated",
    "LoanRenegotiated",
]

# Event signatures (vary by contract version)
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

# Chart of accounts for NFTfi
NFTFI_ACCOUNTS = {
    "loans_receivable": "100.20 - Loans Receivable",
    "loan_payable": "200.10 - Loan Payable",
    "weth_balance": "100.31 - WETH Balance",
    "interest_income": "400.10 - Interest Income",
    "interest_expense": "500.10 - Interest Expense",
    "nft_collateral": "100.10 - NFT Collateral",
    "nft_holdings": "100.10 - NFT Holdings",
    "gas_expense": "600.10 - Gas Expense",
}


# ============================================================================
# NFTFI LOAN DATA STRUCTURE
# ============================================================================

@dataclass
class NFTfiLoan:
    """NFTfi loan data structure with fixed-term interest."""
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
        """Get principal in ETH."""
        return Decimal(self.principal) / Decimal(10**18)

    def get_max_repayment_eth(self) -> Decimal:
        """Get max repayment in ETH."""
        return Decimal(self.max_repayment) / Decimal(10**18)

    def get_interest_eth(self) -> Decimal:
        """Get interest amount in ETH (fixed-term, not compounded)."""
        return self.get_max_repayment_eth() - self.get_principal_eth()

    def is_expired(self, current_timestamp: int) -> bool:
        """Check if loan has expired."""
        return current_timestamp > (self.start_time + self.duration)

    def to_dict(self) -> dict:
        return {
            'loan_id': self.loan_id,
            'borrower': self.borrower,
            'lender': self.lender,
            'principal': self.principal,
            'principal_eth': float(self.get_principal_eth()),
            'max_repayment': self.max_repayment,
            'max_repayment_eth': float(self.get_max_repayment_eth()),
            'interest_eth': float(self.get_interest_eth()),
            'nft_collateral_id': self.nft_collateral_id,
            'principal_token': self.principal_token,
            'nft_collection': self.nft_collection,
            'start_time': self.start_time,
            'duration': self.duration,
            'duration_days': self.duration / 86400,
            'interest_rate_bps': self.interest_rate_bps,
        }


# ============================================================================
# NFTFI DECODER
# ============================================================================

class NFTfiDecoder(BaseDecoder):
    """
    NFTfi NFT lending decoder.

    Handles:
    - LoanStarted: New loan origination
    - LoanRepaid: Loan repayment by borrower
    - LoanLiquidated: Collateral seized by lender

    Uses raw event data parsing since signatures vary by contract version.
    """

    PLATFORM = Platform.NFTFI
    CONTRACT_ADDRESSES = NFTFI_CONTRACTS
    ACCOUNTS = NFTFI_ACCOUNTS

    def _load_abis(self):
        """
        NFTfi uses raw event parsing, no ABI needed.
        Event signatures vary by version so we parse topics/data directly.
        """
        pass

    def can_decode(self, tx: Dict, receipt: Dict) -> bool:
        """Check if transaction involves NFTfi contracts."""
        to_address = (tx.get('to') or '').lower()

        # Check if to-address is NFTfi contract
        if to_address in NFTFI_CONTRACTS:
            return True

        # Check if any log is from NFTfi contract
        logs = receipt.get('logs', [])
        for log in logs:
            log_address = (log.get('address') or '').lower()
            if log_address in NFTFI_CONTRACTS:
                return True

        return False

    def decode(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal) -> DecodedTransaction:
        """
        Decode NFTfi transaction and generate journal entries.

        Args:
            tx: Transaction data from w3.eth.get_transaction()
            receipt: Transaction receipt
            block: Block data
            eth_price: ETH/USD price at block

        Returns:
            DecodedTransaction with events, journal entries, and metadata
        """
        tx_hash = tx.get('hash', b'')
        if isinstance(tx_hash, bytes):
            tx_hash = tx_hash.hex()
        tx_hash = tx_hash if tx_hash.startswith('0x') else f'0x{tx_hash}'

        # Extract basic tx info
        from_address = (tx.get('from') or '').lower()
        to_address = (tx.get('to') or '').lower()
        value = wei_to_eth(tx.get('value', 0))
        gas_fee = calculate_gas_fee(receipt, tx)
        block_number = tx.get('blockNumber', 0)
        timestamp = datetime.fromtimestamp(block.get('timestamp', 0), tz=timezone.utc)

        # Parse events
        events, nftfi_loans = self._decode_nftfi_events(receipt)

        # Determine category
        category = self._determine_category(events)

        # Generate journal entries
        journal_entries = self._generate_journal_entries(
            events=events,
            nftfi_loans=nftfi_loans,
            tx_hash=tx_hash,
            timestamp=timestamp,
            eth_price=eth_price,
            gas_fee=gas_fee,
            from_address=from_address,
        )

        # Determine wallet roles
        wallet_roles = {}
        for wallet in self.fund_wallets:
            for loan in nftfi_loans.values():
                if wallet == loan.lender.lower():
                    wallet_roles[wallet] = "lender"
                elif wallet == loan.borrower.lower():
                    wallet_roles[wallet] = "borrower"

        # Determine function name from selector
        input_data = tx.get('input', b'')
        if isinstance(input_data, str):
            function_selector = input_data[:10] if len(input_data) >= 10 else ''
        else:
            function_selector = '0x' + input_data[:4].hex() if len(input_data) >= 4 else ''

        function_name = self._get_function_name(function_selector)

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
            function_name=function_name,
            function_params={},
            events=events,
            journal_entries=journal_entries,
            wallet_roles=wallet_roles,
            positions={},
            raw_data={'nftfi_loans': {k: v.to_dict() for k, v in nftfi_loans.items()}},
        )

    def _decode_nftfi_events(self, receipt: Dict) -> Tuple[List[DecodedEvent], Dict[int, NFTfiLoan]]:
        """
        Decode NFTfi events from receipt using raw data parsing.

        Returns:
            Tuple of (decoded events, loan_id -> NFTfiLoan mapping)
        """
        events = []
        nftfi_loans = {}

        logs = receipt.get('logs', [])

        for log in logs:
            log_address = (log.get('address') or '').lower()

            if log_address not in NFTFI_CONTRACTS:
                continue

            topics = log.get('topics', [])
            if len(topics) < 4:
                continue

            # Get event signature
            sig_hex = self._to_hex(topics[0]).lower()
            if sig_hex.startswith('0x'):
                sig_hex = sig_hex[2:]

            # Parse indexed parameters from topics
            loan_id = int(self._to_hex(topics[1]), 16) if len(topics) > 1 else 0
            borrower = '0x' + self._to_hex(topics[2])[-40:] if len(topics) > 2 else ''
            lender = '0x' + self._to_hex(topics[3])[-40:] if len(topics) > 3 else ''

            # Get data
            data = log.get('data', b'')
            data_hex = self._to_hex(data)
            if data_hex.startswith('0x'):
                data_hex = data_hex[2:]

            def get_word(idx):
                start = idx * 64
                if start + 64 <= len(data_hex):
                    return int(data_hex[start:start+64], 16)
                return 0

            def get_address(idx):
                val = get_word(idx)
                return '0x' + hex(val)[2:].zfill(40)[-40:]

            # Decode based on event signature
            if sig_hex in NFTFI_LOANSTARTED_SIGS:
                event, loan = self._decode_loan_started(
                    sig_hex, loan_id, borrower, lender,
                    get_word, get_address, log
                )
                events.append(event)
                nftfi_loans[loan_id] = loan

            elif sig_hex in NFTFI_LOANREPAID_SIGS:
                event = self._decode_loan_repaid(
                    loan_id, borrower, lender, get_word, log
                )
                events.append(event)

            elif sig_hex in NFTFI_LOANLIQUIDATED_SIGS:
                event = self._decode_loan_liquidated(
                    loan_id, borrower, lender, log
                )
                events.append(event)

        return events, nftfi_loans

    def _decode_loan_started(
        self,
        sig_hex: str,
        loan_id: int,
        borrower: str,
        lender: str,
        get_word,
        get_address,
        log: Dict
    ) -> Tuple[DecodedEvent, NFTfiLoan]:
        """Decode LoanStarted event."""

        # Different data layouts based on contract/version
        is_coordinator = sig_hex == "3687d64f40b11dd1c102a76882ac1735891c546a96ae27935eb5c7865b9d86fa"

        if is_coordinator:
            # DirectLoanCoordinator layout
            principal = get_word(0)
            nft_id = get_word(1)
            max_repayment = get_word(2)
            duration = get_word(3)
            nft_collection = get_address(6)
            erc20 = get_address(7)
            interest_bps = 0  # Not directly available
            start_time = 0
        else:
            # DirectLoanFixedOffer V2 layout
            principal = get_word(0)
            max_repayment = get_word(1)
            nft_id = get_word(2)
            erc20 = get_address(3)
            duration = get_word(4)
            interest_bps = get_word(6)
            start_time = get_word(8)
            nft_collection = get_address(9)

        loan = NFTfiLoan(
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

        args = {
            'loanId': str(loan_id),
            'borrower': borrower,
            'lender': lender,
            'principal': str(principal),
            'maxRepayment': str(max_repayment),
            'nftId': str(nft_id),
            'erc20': erc20,
            'duration': str(duration),
            'interestBps': str(interest_bps),
            'startTime': str(start_time),
            'nftCollection': nft_collection,
        }

        event = DecodedEvent(
            name="LoanStarted",
            args=args,
            log_index=log.get('logIndex', -1),
            contract_address=log.get('address', ''),
            topic=f"0x{sig_hex}",
        )

        logger.info(f"Decoded NFTfi LoanStarted: loan_id={loan_id}, "
                   f"principal={loan.get_principal_eth():.4f} ETH, "
                   f"lender={lender[:10]}...")

        return event, loan

    def _decode_loan_repaid(
        self,
        loan_id: int,
        borrower: str,
        lender: str,
        get_word,
        log: Dict
    ) -> DecodedEvent:
        """Decode LoanRepaid event."""

        word0 = get_word(0)
        word1 = get_word(1)
        word2 = get_word(2)

        # Heuristic: if word0 looks like ETH value (> 10^15), it's amount_to_lender
        if word0 > 10**15:
            amount_to_lender = word0
            admin_fee = word1
        else:
            amount_to_lender = word1
            admin_fee = word2

        args = {
            'loanId': str(loan_id),
            'borrower': borrower,
            'lender': lender,
            'amountToLender': str(amount_to_lender),
            'adminFee': str(admin_fee),
        }

        topics = log.get('topics', [])
        sig_hex = self._to_hex(topics[0]).lower() if topics else ''

        event = DecodedEvent(
            name="LoanRepaid",
            args=args,
            log_index=log.get('logIndex', -1),
            contract_address=log.get('address', ''),
            topic=sig_hex,
        )

        amount_eth = Decimal(amount_to_lender) / Decimal(10**18)
        logger.info(f"Decoded NFTfi LoanRepaid: loan_id={loan_id}, "
                   f"amount={amount_eth:.4f} ETH")

        return event

    def _decode_loan_liquidated(
        self,
        loan_id: int,
        borrower: str,
        lender: str,
        log: Dict
    ) -> DecodedEvent:
        """Decode LoanLiquidated event."""

        args = {
            'loanId': str(loan_id),
            'borrower': borrower,
            'lender': lender,
        }

        topics = log.get('topics', [])
        sig_hex = self._to_hex(topics[0]).lower() if topics else ''

        event = DecodedEvent(
            name="LoanLiquidated",
            args=args,
            log_index=log.get('logIndex', -1),
            contract_address=log.get('address', ''),
            topic=sig_hex,
        )

        logger.info(f"Decoded NFTfi LoanLiquidated: loan_id={loan_id}")

        return event

    def _determine_category(self, events: List[DecodedEvent]) -> TransactionCategory:
        """Determine transaction category from decoded events."""
        for event in events:
            if event.name == "LoanStarted":
                return TransactionCategory.LOAN_ORIGINATION
            elif event.name == "LoanRepaid":
                return TransactionCategory.LOAN_REPAYMENT
            elif event.name == "LoanLiquidated":
                return TransactionCategory.LOAN_LIQUIDATION
        return TransactionCategory.UNKNOWN

    def _generate_journal_entries(
        self,
        events: List[DecodedEvent],
        nftfi_loans: Dict[int, NFTfiLoan],
        tx_hash: str,
        timestamp: datetime,
        eth_price: Decimal,
        gas_fee: Decimal,
        from_address: str,
    ) -> List[JournalEntry]:
        """Generate journal entries for NFTfi events."""

        entries = []
        tx_hash_short = tx_hash[:10]

        for event in events:
            loan_id = int(event.args.get('loanId', 0))
            borrower = event.args.get('borrower', '').lower()
            lender = event.args.get('lender', '').lower()

            # Get loan data if available
            loan = nftfi_loans.get(loan_id)

            # Determine if fund is involved
            is_fund_lender = lender in self.fund_wallets
            is_fund_borrower = borrower in self.fund_wallets

            if not is_fund_lender and not is_fund_borrower:
                continue

            if event.name == "LoanStarted" and loan:
                principal_eth = loan.get_principal_eth()

                if is_fund_lender:
                    entry = JournalEntry(
                        entry_id=f"JE_{tx_hash_short}_nftfi_orig_lender",
                        date=timestamp,
                        description=f"NFTfi Loan Origination: Lender - {principal_eth:.6f} ETH to {borrower[:10]}...",
                        tx_hash=tx_hash,
                        category=TransactionCategory.LOAN_ORIGINATION,
                        platform=self.PLATFORM,
                        wallet_address=lender,
                        wallet_role="lender",
                        posting_status=self.determine_posting_status(
                            TransactionCategory.LOAN_ORIGINATION, principal_eth
                        ),
                        eth_usd_price=eth_price,
                    )
                    entry.add_debit(self.ACCOUNTS["loans_receivable"], principal_eth, "ETH")
                    entry.add_credit(self.ACCOUNTS["weth_balance"], principal_eth, "WETH")
                    entries.append(entry)
                    logger.info(f"Generated NFTfi LENDER origination entry: {principal_eth:.4f} ETH")

                if is_fund_borrower:
                    entry = JournalEntry(
                        entry_id=f"JE_{tx_hash_short}_nftfi_orig_borrower",
                        date=timestamp,
                        description=f"NFTfi Loan Received: Borrower - {principal_eth:.6f} ETH from {lender[:10]}...",
                        tx_hash=tx_hash,
                        category=TransactionCategory.LOAN_ORIGINATION,
                        platform=self.PLATFORM,
                        wallet_address=borrower,
                        wallet_role="borrower",
                        posting_status=self.determine_posting_status(
                            TransactionCategory.LOAN_ORIGINATION, principal_eth
                        ),
                        eth_usd_price=eth_price,
                    )
                    entry.add_debit(self.ACCOUNTS["weth_balance"], principal_eth, "WETH")
                    entry.add_credit(self.ACCOUNTS["loan_payable"], principal_eth, "ETH")
                    entries.append(entry)
                    logger.info(f"Generated NFTfi BORROWER origination entry: {principal_eth:.4f} ETH")

            elif event.name == "LoanRepaid":
                # Get repayment amount
                amount_to_lender = int(event.args.get('amountToLender', 0))
                amount_eth = Decimal(amount_to_lender) / Decimal(10**18)

                # Try to get principal/interest from loan if available
                if loan:
                    principal_eth = loan.get_principal_eth()
                    interest_eth = loan.get_interest_eth()
                else:
                    # Estimate: assume interest is ~10% of payment
                    principal_eth = amount_eth * Decimal("0.9")
                    interest_eth = amount_eth - principal_eth

                if is_fund_lender:
                    entry = JournalEntry(
                        entry_id=f"JE_{tx_hash_short}_nftfi_repay_lender",
                        date=timestamp,
                        description=f"NFTfi Loan Repaid: Lender receives {amount_eth:.6f} ETH",
                        tx_hash=tx_hash,
                        category=TransactionCategory.LOAN_REPAYMENT,
                        platform=self.PLATFORM,
                        wallet_address=lender,
                        wallet_role="lender",
                        posting_status=self.determine_posting_status(
                            TransactionCategory.LOAN_REPAYMENT, amount_eth
                        ),
                        eth_usd_price=eth_price,
                    )
                    entry.add_debit(self.ACCOUNTS["weth_balance"], amount_eth, "WETH")
                    entry.add_credit(self.ACCOUNTS["loans_receivable"], principal_eth, "ETH")
                    if interest_eth > 0:
                        entry.add_credit(self.ACCOUNTS["interest_income"], interest_eth, "ETH")
                        entry.add_tax_implication(
                            TaxTreatment.TAXABLE_INCOME, interest_eth, "Interest income"
                        )
                    entries.append(entry)
                    logger.info(f"Generated NFTfi LENDER repayment entry: {amount_eth:.4f} ETH")

                if is_fund_borrower:
                    entry = JournalEntry(
                        entry_id=f"JE_{tx_hash_short}_nftfi_repay_borrower",
                        date=timestamp,
                        description=f"NFTfi Loan Repaid: Borrower pays {amount_eth:.6f} ETH",
                        tx_hash=tx_hash,
                        category=TransactionCategory.LOAN_REPAYMENT,
                        platform=self.PLATFORM,
                        wallet_address=borrower,
                        wallet_role="borrower",
                        posting_status=self.determine_posting_status(
                            TransactionCategory.LOAN_REPAYMENT, amount_eth
                        ),
                        eth_usd_price=eth_price,
                    )
                    entry.add_debit(self.ACCOUNTS["loan_payable"], principal_eth, "ETH")
                    if interest_eth > 0:
                        entry.add_debit(self.ACCOUNTS["interest_expense"], interest_eth, "ETH")
                        entry.add_tax_implication(
                            TaxTreatment.DEDUCTIBLE_EXPENSE, interest_eth, "Interest expense"
                        )
                    entry.add_credit(self.ACCOUNTS["weth_balance"], amount_eth, "WETH")
                    entries.append(entry)
                    logger.info(f"Generated NFTfi BORROWER repayment entry: {amount_eth:.4f} ETH")

            elif event.name == "LoanLiquidated":
                # Use loan data if available, otherwise use zeros
                if loan:
                    principal_eth = loan.get_principal_eth()
                    interest_eth = loan.get_interest_eth()
                    max_repayment_eth = loan.get_max_repayment_eth()
                    nft_id = loan.nft_collateral_id
                    nft_collection = loan.nft_collection
                else:
                    principal_eth = Decimal(0)
                    interest_eth = Decimal(0)
                    max_repayment_eth = Decimal(0)
                    nft_id = 0
                    nft_collection = "unknown"

                if is_fund_lender:
                    entry = JournalEntry(
                        entry_id=f"JE_{tx_hash_short}_nftfi_liquidate_lender",
                        date=timestamp,
                        description=f"NFTfi Collateral Seized: Lender receives NFT {nft_collection[:10]}...#{nft_id}",
                        tx_hash=tx_hash,
                        category=TransactionCategory.LOAN_LIQUIDATION,
                        platform=self.PLATFORM,
                        wallet_address=lender,
                        wallet_role="lender",
                        posting_status=PostingStatus.REVIEW_QUEUE,  # Liquidations always need review
                        eth_usd_price=eth_price,
                    )
                    # Value NFT at max repayment amount
                    entry.add_debit(self.ACCOUNTS["nft_collateral"], max_repayment_eth, "NFT")
                    entry.add_credit(self.ACCOUNTS["loans_receivable"], principal_eth, "ETH")
                    if interest_eth > 0:
                        entry.add_credit(self.ACCOUNTS["interest_income"], interest_eth, "ETH")
                    entries.append(entry)
                    logger.info(f"Generated NFTfi LENDER liquidation entry: NFT #{nft_id}")

                if is_fund_borrower:
                    entry = JournalEntry(
                        entry_id=f"JE_{tx_hash_short}_nftfi_liquidate_borrower",
                        date=timestamp,
                        description=f"NFTfi Collateral Lost: Borrower loses NFT {nft_collection[:10]}...#{nft_id}",
                        tx_hash=tx_hash,
                        category=TransactionCategory.LOAN_LIQUIDATION,
                        platform=self.PLATFORM,
                        wallet_address=borrower,
                        wallet_role="borrower",
                        posting_status=PostingStatus.REVIEW_QUEUE,
                        eth_usd_price=eth_price,
                    )
                    entry.add_debit(self.ACCOUNTS["loan_payable"], principal_eth, "ETH")
                    if interest_eth > 0:
                        entry.add_debit(self.ACCOUNTS["interest_expense"], interest_eth, "ETH")
                    entry.add_credit(self.ACCOUNTS["nft_holdings"], max_repayment_eth, "NFT")
                    entries.append(entry)
                    logger.info(f"Generated NFTfi BORROWER liquidation entry: NFT #{nft_id}")

        # Add gas expense if fund paid gas
        if from_address in self.fund_wallets and gas_fee > 0:
            gas_entry = JournalEntry(
                entry_id=f"JE_{tx_hash_short}_gas",
                date=timestamp,
                description=f"Gas expense: {gas_fee:.6f} ETH",
                tx_hash=tx_hash,
                category=TransactionCategory.CONTRACT_CALL,
                platform=self.PLATFORM,
                wallet_address=from_address,
                wallet_role="sender",
                posting_status=PostingStatus.AUTO_POST,
                eth_usd_price=eth_price,
            )
            gas_entry.add_debit(self.ACCOUNTS["gas_expense"], gas_fee, "ETH")
            gas_entry.add_credit("100.30 - ETH Wallet", gas_fee, "ETH")
            gas_entry.add_tax_implication(
                TaxTreatment.DEDUCTIBLE_EXPENSE, gas_fee, "Gas fee"
            )
            entries.append(gas_entry)

        return entries

    def _get_function_name(self, selector: str) -> str:
        """Map function selector to name."""
        function_map = {
            "0x3b1d21a2": "initializeLoan",
            "0x58e644b7": "beginLoan",
            "0x6d5f9e56": "repayLoan",
            "0x8c7a63ae": "payBackLoan",
            "0x766df841": "liquidateOverdueLoan",
        }
        return function_map.get(selector.lower(), "unknown")

    def _to_hex(self, value) -> str:
        """Convert bytes or hex string to hex string."""
        if isinstance(value, bytes):
            return '0x' + value.hex()
        elif isinstance(value, str):
            return value if value.startswith('0x') else '0x' + value
        elif hasattr(value, 'hex'):
            return '0x' + value.hex()
        else:
            return hex(value) if isinstance(value, int) else str(value)
