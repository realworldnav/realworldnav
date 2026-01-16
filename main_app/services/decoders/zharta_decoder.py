"""
Zharta Peer-to-Pool NFT Lending Decoder

Decodes Zharta NFT-collateralized loan transactions including:
- LoanCreated: New pool loan origination
- LoanPayment: Payment made on loan (principal + interest)
- LoanPaid: Loan fully repaid
- LoanDefaulted: Loan defaulted after maturity

Zharta is a peer-to-pool lending protocol where borrowers interact with
liquidity pools rather than individual lenders. Contracts are written in Vyper.

Uses simple interest (not continuous compounding like Blur).
"""

from web3 import Web3
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
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
    TaxTreatment,
    Platform,
    wei_to_eth,
    calculate_gas_fee,
)

logger = logging.getLogger(__name__)

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

# Event names
ZHARTA_EVENTS = [
    "LoanCreated",       # New loan origination
    "LoanPayment",       # Payment made on loan
    "LoanPaid",          # Loan fully repaid
    "LoanDefaulted",     # Loan defaulted after maturity
]

# Event signatures (confirmed from Etherscan analysis)
# LoanCreated - confirmed from tx 0x96d9fe5f317185febefe9a75df186374035a921f3b16426a45d58574cfe67f2b
ZHARTA_LOAN_CREATED_SIG = "4a558778654b4d21f09ae7e2aa4eebc0de757d1233dc825b43183a1276a7b2a1"

# LoanPayment - confirmed from tx 0x2507c931a0c97544ab767dfaab6060e29303ff682b410aba88ff672ae2fee42f
ZHARTA_LOAN_PAYMENT_SIG = "31c401ba8a3eb75cf55e1d9f4971e726115e8448c80446935cffbea991ca2473"

# LoanPaid - confirmed from tx 0x2507c931a0c97544ab767dfaab6060e29303ff682b410aba88ff672ae2fee42f
ZHARTA_LOAN_PAID_SIG = "42d434e1d98bb8cb642015660476f098bbb0f00b64ddb556e149e17de4dd3645"

# LoanDefaulted - TODO: capture from real default transaction
ZHARTA_LOAN_DEFAULTED_SIG = ""  # Not yet captured

# WETH address for transfer tracking
WETH_ADDRESS = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
WETH_TRANSFER_SIG = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Chart of accounts for Zharta
ZHARTA_ACCOUNTS = {
    "loan_payable": "200.10 - Loan Payable",
    "weth_balance": "100.31 - WETH Balance",
    "token_balance": "100.32 - Token Balance",
    "eth_wallet": "100.30 - ETH Wallet",
    "interest_expense": "500.10 - Interest Expense",
    "nft_holdings": "100.10 - NFT Holdings",
    "loss_on_default": "600.10 - Loss on Default",
    "gas_expense": "500.20 - Gas Fees",
}


# ============================================================================
# ZHARTA LOAN DATA STRUCTURE
# ============================================================================

@dataclass
class ZhartaLoan:
    """
    Zharta loan data structure.

    Based on Zharta's Vyper contract loan struct.
    """
    loan_id: int
    borrower: str = ""
    principal: int = 0  # in wei
    apr_bps: int = 0  # APR in basis points
    duration_secs: int = 0
    collaterals: List[tuple] = field(default_factory=list)  # (nft_address, token_id)
    payable_currency: str = ""  # Token address (WETH, etc.)
    start_time: int = 0
    genesis_token: int = 0
    is_paid: bool = False
    is_defaulted: bool = False

    def get_principal_eth(self) -> Decimal:
        """Get principal in ETH (assumes 18 decimals)."""
        return Decimal(self.principal) / Decimal(10**18)

    def calculate_interest_eth(self, current_timestamp: int) -> Decimal:
        """Calculate accrued interest using simple interest formula."""
        if self.start_time == 0 or self.principal == 0:
            return Decimal(0)

        time_elapsed = max(0, current_timestamp - self.start_time)
        # Simple interest: principal * rate * time / (10000 * seconds_per_year)
        seconds_per_year = 31536000
        interest_wei = (self.principal * self.apr_bps * time_elapsed) // (10000 * seconds_per_year)
        return Decimal(interest_wei) / Decimal(10**18)

    def is_expired(self, current_timestamp: int) -> bool:
        """Check if loan has reached maturity."""
        if self.start_time == 0 or self.duration_secs == 0:
            return False
        return current_timestamp > (self.start_time + self.duration_secs)

    def get_expiry_date(self) -> datetime:
        """Get loan maturity datetime."""
        if self.start_time == 0 or self.duration_secs == 0:
            return datetime.now(timezone.utc)
        return datetime.fromtimestamp(self.start_time + self.duration_secs, tz=timezone.utc)

    def to_dict(self) -> dict:
        return {
            'loan_id': self.loan_id,
            'borrower': self.borrower,
            'principal': self.principal,
            'principal_eth': float(self.get_principal_eth()),
            'apr_bps': self.apr_bps,
            'duration_secs': self.duration_secs,
            'duration_days': self.duration_secs / 86400 if self.duration_secs else 0,
            'payable_currency': self.payable_currency,
            'start_time': self.start_time,
            'genesis_token': self.genesis_token,
            'is_paid': self.is_paid,
            'is_defaulted': self.is_defaulted,
        }


# ============================================================================
# ZHARTA DECODER
# ============================================================================

class ZhartaDecoder(BaseDecoder):
    """
    Zharta peer-to-pool NFT lending decoder.

    Handles:
    - LoanCreated: New pool loan origination
    - LoanPayment: Partial or full payment on loan
    - LoanPaid: Loan fully repaid
    - LoanDefaulted: Collateral seized on default

    Uses raw event data parsing since Zharta uses Vyper contracts.
    Event signatures are confirmed via Etherscan analysis.
    """

    PLATFORM = Platform.ZHARTA
    CONTRACT_ADDRESSES = ZHARTA_CONTRACTS
    ACCOUNTS = ZHARTA_ACCOUNTS

    def _load_abis(self):
        """
        Zharta uses raw event parsing, no ABI needed.
        Event signatures are confirmed via Etherscan analysis.
        """
        pass

    def can_decode(self, tx: Dict, receipt: Dict) -> bool:
        """Check if transaction involves Zharta contracts."""
        to_address = (tx.get('to') or '').lower()

        # Check if to-address is Zharta contract
        if to_address in ZHARTA_CONTRACTS:
            return True

        # Check if any log is from Zharta contract
        logs = receipt.get('logs', [])
        for log in logs:
            log_address = (log.get('address') or '').lower()
            if log_address in ZHARTA_CONTRACTS:
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
        Decode Zharta transaction and generate journal entries.

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
        events = self._decode_zharta_events(receipt)

        # Find WETH transfers for amount tracking
        weth_transfers = self._find_weth_transfers(receipt)

        # Determine transaction category
        category = self._determine_category(events, tx)

        # Generate journal entries
        journal_entries = self._generate_journal_entries(
            tx_hash, timestamp, events, weth_transfers, eth_price, gas_fee, category, receipt, block_timestamp
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

        # Function selector mappings for Zharta
        selectors = {
            "5a5cd02e": "reserveEth",  # Create loan with ETH
            "c290d691": "pay",  # Repay loan
        }

        if len(input_data) >= 10:
            selector = input_data[2:10] if input_data.startswith('0x') else input_data[:8]
            return selectors.get(selector.lower(), "unknown")
        return "unknown"

    def _decode_zharta_events(self, receipt: Dict) -> List[Dict]:
        """Decode Zharta-specific events from receipt logs."""
        events = []
        logs = receipt.get('logs', [])

        for log in logs:
            log_address = (log.get('address') or '').lower()

            # Only process logs from Zharta contracts
            if log_address not in ZHARTA_CONTRACTS:
                continue

            topics = log.get('topics', [])
            if not topics:
                continue

            sig_hex = self._get_hex(topics[0])
            data = log.get('data', b'')
            data_hex = self._get_hex(data)

            # Get indexed wallet from topics[1] if present
            indexed_wallet = ''
            if len(topics) > 1:
                indexed_wallet = '0x' + self._get_hex(topics[1])[-40:]

            # LoanCreated(address indexed walletIndexed, address wallet, uint256 loanId,
            #             address erc20TokenContract, uint256 apr, uint256 amount,
            #             uint256 duration, tuple[] collaterals, uint256 genesisToken)
            if sig_hex == ZHARTA_LOAN_CREATED_SIG:
                wallet = self._parse_address(data_hex, 0)
                loan_id = self._parse_data_word(data_hex, 1)
                erc20_token = self._parse_address(data_hex, 2)
                apr = self._parse_data_word(data_hex, 3)
                amount = self._parse_data_word(data_hex, 4)
                duration = self._parse_data_word(data_hex, 5)
                genesis_token = self._parse_data_word(data_hex, 7)  # After collaterals array pointer

                events.append({
                    'name': 'LoanCreated',
                    'args': {
                        'wallet': wallet,
                        'loanId': loan_id,
                        'erc20TokenContract': erc20_token,
                        'apr': apr,
                        'amount': amount,
                        'duration': duration,
                        'genesisToken': genesis_token,
                    },
                    'log_index': log.get('logIndex', -1),
                    'address': log_address,
                    'loan': ZhartaLoan(
                        loan_id=loan_id,
                        borrower=wallet,
                        principal=amount,
                        apr_bps=apr,
                        duration_secs=duration,
                        payable_currency=erc20_token,
                        genesis_token=genesis_token,
                    ),
                })
                logger.info(f"Decoded LoanCreated: loan_id={loan_id}, amount={wei_to_eth(amount):.6f} ETH")

            # LoanPayment(address indexed walletIndexed, address wallet, uint256 loanId,
            #             uint256 principal, uint256 interestAmount, address erc20TokenContract)
            elif sig_hex == ZHARTA_LOAN_PAYMENT_SIG:
                wallet = self._parse_address(data_hex, 0)
                loan_id = self._parse_data_word(data_hex, 1)
                principal = self._parse_data_word(data_hex, 2)
                interest_amount = self._parse_data_word(data_hex, 3)
                erc20_token = self._parse_address(data_hex, 4)

                events.append({
                    'name': 'LoanPayment',
                    'args': {
                        'wallet': wallet,
                        'loanId': loan_id,
                        'principal': principal,
                        'interestAmount': interest_amount,
                        'erc20TokenContract': erc20_token,
                    },
                    'log_index': log.get('logIndex', -1),
                    'address': log_address,
                    'loan': ZhartaLoan(loan_id=loan_id, borrower=wallet, payable_currency=erc20_token),
                })
                logger.info(f"Decoded LoanPayment: loan_id={loan_id}, principal={wei_to_eth(principal):.6f}, interest={wei_to_eth(interest_amount):.6f}")

            # LoanPaid(address indexed walletIndexed, address wallet, uint256 loanId,
            #          address erc20TokenContract)
            elif sig_hex == ZHARTA_LOAN_PAID_SIG:
                wallet = self._parse_address(data_hex, 0)
                loan_id = self._parse_data_word(data_hex, 1)
                erc20_token = self._parse_address(data_hex, 2)

                events.append({
                    'name': 'LoanPaid',
                    'args': {
                        'wallet': wallet,
                        'loanId': loan_id,
                        'erc20TokenContract': erc20_token,
                    },
                    'log_index': log.get('logIndex', -1),
                    'address': log_address,
                    'loan': ZhartaLoan(loan_id=loan_id, borrower=wallet, payable_currency=erc20_token, is_paid=True),
                })
                logger.info(f"Decoded LoanPaid: loan_id={loan_id}")

            # LoanDefaulted (signature not yet captured from real transaction)
            # Data: wallet, loanId, amount, erc20TokenContract
            elif ZHARTA_LOAN_DEFAULTED_SIG and sig_hex == ZHARTA_LOAN_DEFAULTED_SIG:
                wallet = self._parse_address(data_hex, 0)
                loan_id = self._parse_data_word(data_hex, 1)
                amount = self._parse_data_word(data_hex, 2)
                erc20_token = self._parse_address(data_hex, 3)

                events.append({
                    'name': 'LoanDefaulted',
                    'args': {
                        'wallet': wallet,
                        'loanId': loan_id,
                        'amount': amount,
                        'erc20TokenContract': erc20_token,
                    },
                    'log_index': log.get('logIndex', -1),
                    'address': log_address,
                    'loan': ZhartaLoan(loan_id=loan_id, borrower=wallet, payable_currency=erc20_token, is_defaulted=True),
                })
                logger.info(f"Decoded LoanDefaulted: loan_id={loan_id}, amount={wei_to_eth(amount):.6f}")

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

    def _find_nft_transfers(self, receipt: Dict, exclude_token: str = "") -> List[Dict]:
        """Find NFT Transfer events in receipt (excluding WETH and specified token)."""
        transfers = []
        logs = receipt.get('logs', [])
        exclude_addresses = [WETH_ADDRESS, exclude_token.lower()] if exclude_token else [WETH_ADDRESS]

        for log in logs:
            log_address = (log.get('address') or '').lower()
            if log_address in exclude_addresses:
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

        if 'LoanCreated' in event_names:
            return TransactionCategory.LOAN_ORIGINATION
        elif 'LoanPayment' in event_names or 'LoanPaid' in event_names:
            return TransactionCategory.LOAN_REPAYMENT
        elif 'LoanDefaulted' in event_names:
            return TransactionCategory.COLLATERAL_SEIZURE

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
        block_timestamp: int,
    ) -> List[JournalEntry]:
        """Generate journal entries based on decoded events."""
        entries = []
        tx_hash_short = tx_hash[:10]

        for evt in events:
            event_name = evt.get('name')
            args = evt.get('args', {})
            loan = evt.get('loan')
            loan_id = args.get('loanId', loan.loan_id if loan else 0)
            wallet = args.get('wallet', '').lower()
            erc20_token = args.get('erc20TokenContract', '').lower()

            # Determine token type for account labeling
            is_weth = erc20_token == WETH_ADDRESS
            token_symbol = "WETH" if is_weth else "TOKEN"
            token_account = self.ACCOUNTS["weth_balance"] if is_weth else self.ACCOUNTS["token_balance"]

            # Check if wallet is a fund wallet (Zharta funds are typically borrowers)
            is_fund_borrower = wallet in self.fund_wallets

            # === LoanCreated (Borrower receives funds) ===
            if event_name == 'LoanCreated':
                amount = args.get('amount', 0)
                amount_eth = wei_to_eth(amount)

                if is_fund_borrower:
                    entry = JournalEntry(
                        entry_id=f"JE_{tx_hash_short}_zharta_orig_borrower",
                        date=timestamp,
                        description=f"Zharta Loan Received: Borrower - {amount_eth:.6f} {token_symbol}, Loan #{loan_id}",
                        tx_hash=tx_hash,
                        category=TransactionCategory.LOAN_ORIGINATION,
                        platform=self.PLATFORM,
                        wallet_address=wallet,
                        wallet_role="borrower",
                        eth_usd_price=eth_price,
                    )
                    entry.add_debit(token_account, amount_eth, token_symbol)
                    entry.add_credit(self.ACCOUNTS["loan_payable"], amount_eth, token_symbol)
                    entries.append(entry)
                    logger.info(f"Generated BORROWER origination entry: {amount_eth:.6f} {token_symbol}")

            # === LoanPayment (Borrower makes partial payment) ===
            elif event_name == 'LoanPayment':
                principal = args.get('principal', 0)
                interest_amount = args.get('interestAmount', 0)
                principal_eth = wei_to_eth(principal)
                interest_eth = wei_to_eth(interest_amount)
                total_eth = principal_eth + interest_eth

                if is_fund_borrower:
                    entry = JournalEntry(
                        entry_id=f"JE_{tx_hash_short}_zharta_payment_borrower",
                        date=timestamp,
                        description=f"Zharta Loan Payment: Borrower pays {total_eth:.6f} {token_symbol}, Loan #{loan_id}",
                        tx_hash=tx_hash,
                        category=TransactionCategory.LOAN_REPAYMENT,
                        platform=self.PLATFORM,
                        wallet_address=wallet,
                        wallet_role="borrower",
                        eth_usd_price=eth_price,
                    )
                    entry.add_debit(self.ACCOUNTS["loan_payable"], principal_eth, token_symbol)
                    entry.add_debit(self.ACCOUNTS["interest_expense"], interest_eth, token_symbol)
                    entry.add_credit(token_account, total_eth, token_symbol)

                    # Tax implications
                    entry.add_tax_implication(
                        TaxTreatment.DEDUCTIBLE_EXPENSE,
                        interest_eth,
                        f"Interest expense on Zharta loan #{loan_id}"
                    )

                    entries.append(entry)
                    logger.info(f"Generated BORROWER payment entry: {total_eth:.6f} {token_symbol}")

            # === LoanPaid (Loan fully repaid) ===
            elif event_name == 'LoanPaid':
                if is_fund_borrower:
                    # LoanPaid doesn't have amount - look for WETH transfer
                    for transfer in weth_transfers:
                        if transfer['src'] in self.fund_wallets:
                            amount_eth = transfer['amount_eth']
                            # Estimate 90/10 split (in production, use actual loan terms)
                            principal_eth = amount_eth * Decimal("0.90")
                            interest_eth = amount_eth - principal_eth

                            entry = JournalEntry(
                                entry_id=f"JE_{tx_hash_short}_zharta_paid_borrower",
                                date=timestamp,
                                description=f"Zharta Loan Paid Off: Borrower - {amount_eth:.6f} {token_symbol}, Loan #{loan_id}",
                                tx_hash=tx_hash,
                                category=TransactionCategory.LOAN_REPAYMENT,
                                platform=self.PLATFORM,
                                wallet_address=wallet,
                                wallet_role="borrower",
                                eth_usd_price=eth_price,
                            )
                            entry.add_debit(self.ACCOUNTS["loan_payable"], principal_eth, token_symbol)
                            entry.add_debit(self.ACCOUNTS["interest_expense"], interest_eth, token_symbol)
                            entry.add_credit(token_account, amount_eth, token_symbol)

                            entry.add_tax_implication(
                                TaxTreatment.DEDUCTIBLE_EXPENSE,
                                interest_eth,
                                f"Interest expense on Zharta loan #{loan_id} payoff"
                            )

                            entries.append(entry)
                            logger.info(f"Generated BORROWER full repayment entry: {amount_eth:.6f} {token_symbol}")
                            break

            # === LoanDefaulted (Borrower loses collateral) ===
            elif event_name == 'LoanDefaulted':
                default_amount = args.get('amount', 0)
                default_eth = wei_to_eth(default_amount)

                if is_fund_borrower:
                    # Find NFT transfers (collateral being seized)
                    nft_transfers = self._find_nft_transfers(receipt, erc20_token)

                    for nft_transfer in nft_transfers:
                        # CollateralVault holds collateral, check if transfer is from vault
                        if nft_transfer['from'] == ZHARTA_COLLATERAL_VAULT:
                            nft_contract = nft_transfer['contract']
                            token_id = nft_transfer['token_id']

                            entry = JournalEntry(
                                entry_id=f"JE_{tx_hash_short}_zharta_default_borrower",
                                date=timestamp,
                                description=f"Zharta Loan Default: Borrower lost NFT #{token_id}, Loan #{loan_id}",
                                tx_hash=tx_hash,
                                category=TransactionCategory.COLLATERAL_SEIZURE,
                                platform=self.PLATFORM,
                                wallet_address=wallet,
                                wallet_role="borrower",
                                eth_usd_price=eth_price,
                                posting_status=PostingStatus.REVIEW_QUEUE,  # Needs manual value review
                            )
                            # Debit loan payable (loan forgiven) and loss
                            # Credit NFT holdings (NFT seized)
                            entry.add_debit(self.ACCOUNTS["loan_payable"], default_eth, token_symbol)
                            entry.add_debit(self.ACCOUNTS["loss_on_default"], default_eth, token_symbol)
                            entry.add_credit(self.ACCOUNTS["nft_holdings"], default_eth * 2, "NFT")

                            entries.append(entry)
                            logger.warning(f"Generated BORROWER default entry (needs review): NFT #{token_id}")
                            break

        # Gas fee entry (always add if fund paid gas)
        if gas_fee > 0:
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
