"""
Arcade LoanCore NFT Lending Decoder

Decodes Arcade.xyz NFT-collateralized loan transactions including:
- LoanStarted: New loan origination
- LoanRepaid: Loan repayment by borrower
- LoanClaimed: Collateral seized by lender (default)
- LoanRolledOver: Loan extension/rollover
- InstallmentPaymentReceived: Partial payment (for installment loans)

Arcade uses promissory notes (LenderNote and BorrowerNote) that are minted
on loan origination and burned on loan completion.

Uses simple interest (not continuous compounding like Blur).
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
# ARCADE CONSTANTS
# ============================================================================

# Contract addresses (V3 - current production)
ARCADE_LOAN_CORE_PROXY = "0x81b2f8fc75bab64a6b144aa6d2faa127b4fa7fd9"
ARCADE_LOAN_CORE_IMPL = "0x6ddb57101a17854109c3b9feb80ae19662ea950f"
ARCADE_REPAYMENT_CONTROLLER = "0xb39dab85fa05c381767ff992ccde4c94619993d4"
ARCADE_ORIGINATION_CONTROLLER = "0x89bc08ba00f135d608bc335f6b33d7a9abcc98af"
ARCADE_LENDER_NOTE = "0x349a026a43ffa8e2ab4c4e59fcaa93f87bd8ddee"
ARCADE_BORROWER_NOTE = "0x337104a4f06260ff327d6734c555a0f5d8f863aa"

ARCADE_CONTRACTS = [
    ARCADE_LOAN_CORE_PROXY,
    ARCADE_LOAN_CORE_IMPL,
    ARCADE_REPAYMENT_CONTROLLER,
    ARCADE_ORIGINATION_CONTROLLER,
]

# Event names
ARCADE_EVENTS = [
    "LoanStarted",
    "LoanRepaid",
    "LoanClaimed",
    "LoanRolledOver",
    "InstallmentPaymentReceived",
]

# Event signatures (confirmed from Etherscan)
# LoanRepaid(uint256 loanId) - confirmed from tx 0xa72e5fef...
ARCADE_LOAN_REPAID_SIG = "9a7851747cd7ffb3fe0a32caf3da48b31f27cebe131267051640f8b72fc47186"

# LoanClaimed(uint256 loanId) - confirmed from tx 0x5be6efb5...
ARCADE_LOAN_CLAIMED_SIG = "b15e438728b48d46c9a5505713e60ff50c80559f4523c8f99a246a2069a8684a"

# LoanStarted(uint256 loanId, address lender, address borrower)
ARCADE_LOAN_STARTED_SIG = "f66ad0a6f32ab1c79cf8dd9eee7da4c1fc41a69c2f2f90c21f0dd1c07b8e6e31"

# LoanRolledOver(uint256 oldLoanId, uint256 newLoanId)
ARCADE_LOAN_ROLLED_OVER_SIG = ""  # TODO: Get from actual transaction

# NonceUsed(address indexed user, uint160 nonce) - admin event, not loan-related
ARCADE_NONCE_USED_SIG = "94307d29ec5ae0d8d8a9c5e8a03194264f6a9a15ab14d2472869784f32c01ce7"

# WETH address for transfer tracking
WETH_ADDRESS = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
WETH_TRANSFER_SIG = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Chart of accounts for Arcade
ARCADE_ACCOUNTS = {
    "loans_receivable": "100.20 - Loans Receivable",
    "loan_payable": "200.10 - Loan Payable",
    "weth_balance": "100.31 - WETH Balance",
    "eth_wallet": "100.30 - ETH Wallet",
    "interest_income": "400.10 - Interest Income",
    "interest_expense": "500.10 - Interest Expense",
    "nft_holdings": "100.10 - NFT Holdings",
    "gas_expense": "500.20 - Gas Fees",
}


# ============================================================================
# ARCADE LOAN DATA STRUCTURE
# ============================================================================

@dataclass
class ArcadeLoan:
    """
    Arcade loan data structure based on LoanData struct.

    LoanTerms struct (from contract):
    - interestRate: APR in basis points (1 = 0.01%)
    - durationSecs: Loan duration in seconds
    - collateralAddress: NFT contract or AssetVault
    - deadline: Signature expiration timestamp
    - payableCurrency: Repayment token address (e.g., WETH)
    - principal: Loan amount in payableCurrency
    - collateralId: Token ID of collateral
    - affiliateCode: Referral code
    """
    loan_id: int
    borrower: str = ""
    lender: str = ""
    principal: int = 0  # in wei
    interest_rate_bps: int = 0  # APR in basis points (1 = 0.01%)
    duration_secs: int = 0
    collateral_address: str = ""  # NFT contract or AssetVault
    collateral_id: int = 0
    payable_currency: str = ""  # Token address (WETH, etc.)
    start_date: int = 0
    balance: int = 0  # Outstanding principal
    interest_paid: int = 0  # Cumulative interest paid
    state: str = "Active"  # Active, Repaid, Defaulted

    def get_principal_eth(self) -> Decimal:
        """Get principal in ETH (assumes 18 decimals)."""
        return Decimal(self.principal) / Decimal(10**18)

    def get_balance_eth(self) -> Decimal:
        """Get outstanding balance in ETH."""
        return Decimal(self.balance) / Decimal(10**18)

    def calculate_interest_eth(self, current_timestamp: int) -> Decimal:
        """Calculate accrued interest using simple interest formula."""
        if self.start_date == 0 or self.principal == 0:
            return Decimal(0)

        time_elapsed = max(0, current_timestamp - self.start_date)
        # Simple interest: principal * rate * time / (10000 * seconds_per_year)
        seconds_per_year = 31536000
        interest_wei = (self.principal * self.interest_rate_bps * time_elapsed) // (10000 * seconds_per_year)
        return Decimal(interest_wei) / Decimal(10**18)

    def is_expired(self, current_timestamp: int) -> bool:
        """Check if loan has expired (defaulted)."""
        if self.start_date == 0 or self.duration_secs == 0:
            return False
        return current_timestamp > (self.start_date + self.duration_secs)

    def get_expiry_date(self) -> datetime:
        """Get loan expiry datetime."""
        if self.start_date == 0 or self.duration_secs == 0:
            return datetime.now(timezone.utc)
        return datetime.fromtimestamp(self.start_date + self.duration_secs, tz=timezone.utc)

    def to_dict(self) -> dict:
        return {
            'loan_id': self.loan_id,
            'borrower': self.borrower,
            'lender': self.lender,
            'principal': self.principal,
            'principal_eth': float(self.get_principal_eth()),
            'interest_rate_bps': self.interest_rate_bps,
            'duration_secs': self.duration_secs,
            'duration_days': self.duration_secs / 86400 if self.duration_secs else 0,
            'collateral_address': self.collateral_address,
            'collateral_id': self.collateral_id,
            'payable_currency': self.payable_currency,
            'start_date': self.start_date,
            'state': self.state,
        }


# ============================================================================
# ARCADE DECODER
# ============================================================================

class ArcadeDecoder(BaseDecoder):
    """
    Arcade LoanCore NFT lending decoder.

    Handles:
    - LoanStarted: New loan origination
    - LoanRepaid: Full loan repayment by borrower
    - LoanClaimed: Collateral seized by lender on default
    - LoanRolledOver: Loan extension/rollover

    Uses raw event data parsing since we decode directly from topics/data.
    """

    PLATFORM = Platform.ARCADE
    CONTRACT_ADDRESSES = ARCADE_CONTRACTS
    ACCOUNTS = ARCADE_ACCOUNTS

    def _load_abis(self):
        """
        Arcade uses raw event parsing, no ABI needed.
        Event signatures are confirmed via Etherscan analysis.
        """
        pass

    def can_decode(self, tx: Dict, receipt: Dict) -> bool:
        """Check if transaction involves Arcade contracts."""
        to_address = (tx.get('to') or '').lower()

        # Check if to-address is Arcade contract
        if to_address in ARCADE_CONTRACTS:
            return True

        # Check if any log is from Arcade contract
        logs = receipt.get('logs', [])
        for log in logs:
            log_address = (log.get('address') or '').lower()
            if log_address in ARCADE_CONTRACTS:
                return True

        return False

    def _get_hex(self, value) -> str:
        """Convert bytes/HexBytes to hex string without 0x prefix."""
        if hasattr(value, 'hex'):
            return value.hex()
        if isinstance(value, str):
            return value[2:] if value.startswith('0x') else value
        return str(value)

    def _parse_data_word(self, data_hex: str, idx: int) -> int:
        """Parse a 32-byte word from data at given index."""
        start = idx * 64
        if start + 64 <= len(data_hex):
            return int(data_hex[start:start+64], 16)
        return 0

    def _parse_address(self, data_hex: str, idx: int) -> str:
        """Parse an address from data at given index."""
        val = self._parse_data_word(data_hex, idx)
        return '0x' + hex(val)[2:].zfill(40)[-40:]

    def decode(self, tx: Dict, receipt: Dict, block: Dict, eth_price: Decimal) -> DecodedTransaction:
        """
        Decode Arcade transaction and generate journal entries.

        Args:
            tx: Transaction data from w3.eth.get_transaction()
            receipt: Transaction receipt from w3.eth.get_transaction_receipt()
            block: Block data from w3.eth.get_block()
            eth_price: ETH/USD price at block time

        Returns:
            DecodedTransaction with events, journal entries, and metadata
        """
        tx_hash = self._get_hex(tx.get('hash', b''))
        from_address = (tx.get('from') or '').lower()
        to_address = (tx.get('to') or '').lower()
        block_number = tx.get('blockNumber', 0)
        block_timestamp = block.get('timestamp', 0)
        timestamp = datetime.fromtimestamp(block_timestamp, tz=timezone.utc)

        # Calculate gas fee
        gas_fee = calculate_gas_fee(receipt, tx)
        gas_used = receipt.get('gasUsed', 0)
        value = wei_to_eth(tx.get('value', 0))

        # Decode events from logs
        events = self._decode_arcade_events(receipt)

        # Find WETH transfers for amount tracking
        weth_transfers = self._find_weth_transfers(receipt)

        # Determine transaction category
        category = self._determine_category(events, tx)

        # Generate journal entries
        journal_entries = self._generate_journal_entries(
            tx_hash, timestamp, events, weth_transfers, eth_price, gas_fee, category, receipt
        )

        # Determine wallet roles
        wallet_roles = {}
        for wallet in self.fund_wallets:
            if wallet == from_address:
                wallet_roles[wallet] = "sender"
            elif wallet == to_address:
                wallet_roles[wallet] = "recipient"

        return DecodedTransaction(
            status="success",
            tx_hash=tx_hash,
            platform=self.PLATFORM,
            category=category,
            block=block_number,
            timestamp=timestamp,
            eth_price=eth_price,
            gas_used=gas_used,
            gas_fee=gas_fee,
            from_address=from_address,
            to_address=to_address,
            value=value,
            function_name=self._get_function_name(tx),
            events=[DecodedEvent(
                name=e['name'],
                args=e.get('args', {}),
                log_index=e.get('log_index', -1),
                contract_address=e.get('address', ''),
            ) for e in events],
            journal_entries=journal_entries,
            wallet_roles=wallet_roles,
        )

    def _get_function_name(self, tx: Dict) -> str:
        """Extract function name from transaction input."""
        input_data = tx.get('input', '0x')
        if isinstance(input_data, bytes):
            input_data = input_data.hex()

        # Function selector mappings
        selectors = {
            "be993dc2": "repay",
            "556f800f": "repayPart",
            "379607f5": "claim",
            "4c04f4a5": "rollover",
            "38a78016": "cancelNonce",
        }

        if len(input_data) >= 10:
            selector = input_data[2:10] if input_data.startswith('0x') else input_data[:8]
            return selectors.get(selector.lower(), "unknown")
        return "unknown"

    def _decode_arcade_events(self, receipt: Dict) -> List[Dict]:
        """Decode Arcade-specific events from receipt logs."""
        events = []
        logs = receipt.get('logs', [])

        for log in logs:
            log_address = (log.get('address') or '').lower()

            # Only process logs from Arcade contracts
            if log_address not in ARCADE_CONTRACTS:
                continue

            topics = log.get('topics', [])
            if not topics:
                continue

            sig_hex = self._get_hex(topics[0])
            data = log.get('data', b'')
            data_hex = self._get_hex(data)

            # LoanRepaid(uint256 loanId)
            if sig_hex == ARCADE_LOAN_REPAID_SIG:
                loan_id = self._parse_data_word(data_hex, 0)
                events.append({
                    'name': 'LoanRepaid',
                    'args': {'loanId': loan_id},
                    'log_index': log.get('logIndex', -1),
                    'address': log_address,
                    'loan': ArcadeLoan(loan_id=loan_id),
                })
                logger.info(f"Decoded LoanRepaid: loan_id={loan_id}")

            # LoanClaimed(uint256 loanId)
            elif sig_hex == ARCADE_LOAN_CLAIMED_SIG:
                loan_id = self._parse_data_word(data_hex, 0)
                events.append({
                    'name': 'LoanClaimed',
                    'args': {'loanId': loan_id},
                    'log_index': log.get('logIndex', -1),
                    'address': log_address,
                    'loan': ArcadeLoan(loan_id=loan_id, state='Defaulted'),
                })
                logger.info(f"Decoded LoanClaimed: loan_id={loan_id}")

            # LoanStarted(uint256 loanId, address lender, address borrower)
            elif sig_hex == ARCADE_LOAN_STARTED_SIG:
                loan_id = self._parse_data_word(data_hex, 0)
                lender = self._parse_address(data_hex, 1)
                borrower = self._parse_address(data_hex, 2)
                events.append({
                    'name': 'LoanStarted',
                    'args': {
                        'loanId': loan_id,
                        'lender': lender,
                        'borrower': borrower,
                    },
                    'log_index': log.get('logIndex', -1),
                    'address': log_address,
                    'loan': ArcadeLoan(loan_id=loan_id, lender=lender, borrower=borrower),
                })
                logger.info(f"Decoded LoanStarted: loan_id={loan_id}, lender={lender[:12]}..., borrower={borrower[:12]}...")

            # NonceUsed - skip, not a loan event
            elif sig_hex == ARCADE_NONCE_USED_SIG:
                logger.debug("Skipping NonceUsed event (admin)")

        return events

    def _find_weth_transfers(self, receipt: Dict) -> List[Dict]:
        """Find WETH Transfer events in receipt."""
        transfers = []
        logs = receipt.get('logs', [])

        for log in logs:
            log_address = (log.get('address') or '').lower()
            if log_address != WETH_ADDRESS:
                continue

            topics = log.get('topics', [])
            if len(topics) < 3:
                continue

            sig_hex = self._get_hex(topics[0])
            if sig_hex != WETH_TRANSFER_SIG:
                continue

            # Transfer(address indexed src, address indexed dst, uint256 wad)
            src = '0x' + self._get_hex(topics[1])[-40:]
            dst = '0x' + self._get_hex(topics[2])[-40:]
            data_hex = self._get_hex(log.get('data', b''))
            wad = int(data_hex, 16) if data_hex else 0
            amount_eth = Decimal(wad) / Decimal(10**18)

            transfers.append({
                'src': src.lower(),
                'dst': dst.lower(),
                'amount_wei': wad,
                'amount_eth': amount_eth,
            })

        return transfers

    def _find_nft_transfers(self, receipt: Dict) -> List[Dict]:
        """Find NFT Transfer events in receipt (excluding WETH)."""
        transfers = []
        logs = receipt.get('logs', [])

        for log in logs:
            log_address = (log.get('address') or '').lower()
            # Skip WETH and Arcade note contracts
            if log_address in [WETH_ADDRESS, ARCADE_LENDER_NOTE.lower(), ARCADE_BORROWER_NOTE.lower()]:
                continue

            topics = log.get('topics', [])
            if len(topics) < 4:
                continue

            sig_hex = self._get_hex(topics[0])
            if sig_hex != WETH_TRANSFER_SIG:
                continue

            # ERC721 Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
            src = '0x' + self._get_hex(topics[1])[-40:]
            dst = '0x' + self._get_hex(topics[2])[-40:]
            token_id = int(self._get_hex(topics[3]), 16)

            transfers.append({
                'contract': log_address,
                'from': src.lower(),
                'to': dst.lower(),
                'token_id': token_id,
            })

        return transfers

    def _determine_category(self, events: List[Dict], tx: Dict) -> TransactionCategory:
        """Determine transaction category from decoded events."""
        event_names = {e['name'] for e in events}

        if 'LoanStarted' in event_names:
            return TransactionCategory.LOAN_ORIGINATION
        elif 'LoanRepaid' in event_names:
            return TransactionCategory.LOAN_REPAYMENT
        elif 'LoanClaimed' in event_names:
            return TransactionCategory.COLLATERAL_SEIZURE
        elif 'LoanRolledOver' in event_names:
            return TransactionCategory.LOAN_EXTENSION

        return TransactionCategory.CONTRACT_CALL

    def _generate_journal_entries(
        self,
        tx_hash: str,
        timestamp: datetime,
        events: List[Dict],
        weth_transfers: List[Dict],
        eth_price: Decimal,
        gas_fee: Decimal,
        category: TransactionCategory,
        receipt: Dict,
    ) -> List[JournalEntry]:
        """Generate journal entries based on decoded events."""
        entries = []
        tx_hash_short = tx_hash[:10]

        for evt in events:
            event_name = evt.get('name')
            args = evt.get('args', {})
            loan = evt.get('loan')
            loan_id = args.get('loanId', loan.loan_id if loan else 0)

            # === LoanStarted ===
            if event_name == 'LoanStarted':
                lender = args.get('lender', '').lower()
                borrower = args.get('borrower', '').lower()

                # Check if fund is lender
                if lender in self.fund_wallets:
                    # Find WETH transfer from fund to contract
                    for transfer in weth_transfers:
                        if transfer['src'] in self.fund_wallets:
                            principal_eth = transfer['amount_eth']

                            entry = JournalEntry(
                                entry_id=f"JE_{tx_hash_short}_arcade_orig_lender",
                                date=timestamp,
                                description=f"Arcade Loan Origination: Lender - {principal_eth:.6f} ETH, Loan #{loan_id}",
                                tx_hash=tx_hash,
                                category=TransactionCategory.LOAN_ORIGINATION,
                                platform=self.PLATFORM,
                                wallet_address=lender,
                                wallet_role="lender",
                                eth_usd_price=eth_price,
                            )
                            entry.add_debit(self.ACCOUNTS["loans_receivable"], principal_eth, "ETH")
                            entry.add_credit(self.ACCOUNTS["weth_balance"], principal_eth, "WETH")
                            entries.append(entry)
                            logger.info(f"Generated LENDER origination entry: {principal_eth:.6f} ETH")
                            break

                # Check if fund is borrower
                if borrower in self.fund_wallets:
                    # Find WETH transfer to fund from contract
                    for transfer in weth_transfers:
                        if transfer['dst'] in self.fund_wallets:
                            principal_eth = transfer['amount_eth']

                            entry = JournalEntry(
                                entry_id=f"JE_{tx_hash_short}_arcade_orig_borrower",
                                date=timestamp,
                                description=f"Arcade Loan Received: Borrower - {principal_eth:.6f} ETH, Loan #{loan_id}",
                                tx_hash=tx_hash,
                                category=TransactionCategory.LOAN_ORIGINATION,
                                platform=self.PLATFORM,
                                wallet_address=borrower,
                                wallet_role="borrower",
                                eth_usd_price=eth_price,
                            )
                            entry.add_debit(self.ACCOUNTS["weth_balance"], principal_eth, "WETH")
                            entry.add_credit(self.ACCOUNTS["loan_payable"], principal_eth, "ETH")
                            entries.append(entry)
                            logger.info(f"Generated BORROWER origination entry: {principal_eth:.6f} ETH")
                            break

            # === LoanRepaid ===
            elif event_name == 'LoanRepaid':
                # Check if fund received WETH (fund is lender)
                for transfer in weth_transfers:
                    if transfer['dst'] in self.fund_wallets:
                        amount_eth = transfer['amount_eth']

                        # Estimate principal vs interest (90/10 split as placeholder)
                        # In production, we'd look up original loan terms
                        principal_eth = amount_eth * Decimal("0.90")
                        interest_eth = amount_eth - principal_eth

                        entry = JournalEntry(
                            entry_id=f"JE_{tx_hash_short}_arcade_repay_lender",
                            date=timestamp,
                            description=f"Arcade Loan Repaid: Lender receives {amount_eth:.6f} ETH, Loan #{loan_id}",
                            tx_hash=tx_hash,
                            category=TransactionCategory.LOAN_REPAYMENT,
                            platform=self.PLATFORM,
                            wallet_address=transfer['dst'],
                            wallet_role="lender",
                            eth_usd_price=eth_price,
                        )
                        entry.add_debit(self.ACCOUNTS["weth_balance"], amount_eth, "WETH")
                        entry.add_credit(self.ACCOUNTS["loans_receivable"], principal_eth, "ETH")
                        entry.add_credit(self.ACCOUNTS["interest_income"], interest_eth, "ETH")

                        # Tax implications
                        entry.add_tax_implication(
                            TaxTreatment.TAXABLE_INCOME,
                            interest_eth,
                            f"Interest income from Arcade loan #{loan_id}"
                        )

                        entries.append(entry)
                        logger.info(f"Generated LENDER repayment entry: {amount_eth:.6f} ETH")
                        break

                # Check if fund sent WETH (fund is borrower)
                for transfer in weth_transfers:
                    if transfer['src'] in self.fund_wallets:
                        amount_eth = transfer['amount_eth']
                        principal_eth = amount_eth * Decimal("0.90")
                        interest_eth = amount_eth - principal_eth

                        entry = JournalEntry(
                            entry_id=f"JE_{tx_hash_short}_arcade_repay_borrower",
                            date=timestamp,
                            description=f"Arcade Loan Repaid: Borrower pays {amount_eth:.6f} ETH, Loan #{loan_id}",
                            tx_hash=tx_hash,
                            category=TransactionCategory.LOAN_REPAYMENT,
                            platform=self.PLATFORM,
                            wallet_address=transfer['src'],
                            wallet_role="borrower",
                            eth_usd_price=eth_price,
                        )
                        entry.add_debit(self.ACCOUNTS["loan_payable"], principal_eth, "ETH")
                        entry.add_debit(self.ACCOUNTS["interest_expense"], interest_eth, "ETH")
                        entry.add_credit(self.ACCOUNTS["weth_balance"], amount_eth, "WETH")

                        entry.add_tax_implication(
                            TaxTreatment.DEDUCTIBLE_EXPENSE,
                            interest_eth,
                            f"Interest expense on Arcade loan #{loan_id}"
                        )

                        entries.append(entry)
                        logger.info(f"Generated BORROWER repayment entry: {amount_eth:.6f} ETH")
                        break

            # === LoanClaimed ===
            elif event_name == 'LoanClaimed':
                # Find NFT transfers to fund wallets (fund is lender seizing collateral)
                nft_transfers = self._find_nft_transfers(receipt)

                for nft_transfer in nft_transfers:
                    if nft_transfer['to'] in self.fund_wallets:
                        nft_contract = nft_transfer['contract']
                        token_id = nft_transfer['token_id']

                        # Placeholder value - in production, look up original loan principal
                        estimated_value_eth = Decimal("1.0")

                        entry = JournalEntry(
                            entry_id=f"JE_{tx_hash_short}_arcade_claim_lender",
                            date=timestamp,
                            description=f"Arcade Collateral Seized: Lender receives NFT #{token_id}, Loan #{loan_id}",
                            tx_hash=tx_hash,
                            category=TransactionCategory.COLLATERAL_SEIZURE,
                            platform=self.PLATFORM,
                            wallet_address=nft_transfer['to'],
                            wallet_role="lender",
                            eth_usd_price=eth_price,
                            posting_status=PostingStatus.REVIEW_QUEUE,  # Needs manual value review
                        )
                        entry.add_debit(self.ACCOUNTS["nft_holdings"], estimated_value_eth, "NFT")
                        entry.add_credit(self.ACCOUNTS["loans_receivable"], estimated_value_eth, "ETH")
                        entries.append(entry)
                        logger.info(f"Generated LENDER seizure entry: NFT #{token_id}")
                        break

        # Gas fee entry (always add if fund paid gas)
        if gas_fee > 0:
            # Check if any fund wallet is the transaction sender
            from_address = receipt.get('from', '').lower() if isinstance(receipt.get('from'), str) else ''
            if not from_address:
                # Get from original tx
                pass

            gas_entry = JournalEntry(
                entry_id=f"JE_{tx_hash_short}_gas",
                date=timestamp,
                description=f"Gas Fee: {gas_fee:.8f} ETH",
                tx_hash=tx_hash,
                category=TransactionCategory.CONTRACT_CALL,
                platform=self.PLATFORM,
                eth_usd_price=eth_price,
            )
            gas_entry.add_debit(self.ACCOUNTS["gas_expense"], gas_fee, "ETH")
            gas_entry.add_credit(self.ACCOUNTS["eth_wallet"], gas_fee, "ETH")
            entries.append(gas_entry)

        return entries
