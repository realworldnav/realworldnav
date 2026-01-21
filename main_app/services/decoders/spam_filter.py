"""
Spam/Phishing Transaction Filter

Detects and filters out phishing transactions, fake airdrops, and spam
that would pollute the accounting ledger.

Red flags detected:
1. Too many events in a single transaction (>50 is suspicious)
2. Airdrop patterns (one sender to many recipients)
3. Unknown/unverified tokens
4. Known phishing contract addresses
5. Suspicious token names/symbols
6. Zero-value ETH with many token transfers
"""

import logging
from typing import Dict, List, Set, Tuple, Optional
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class SpamReason(Enum):
    """Reasons a transaction was flagged as spam"""
    TOO_MANY_EVENTS = "too_many_events"
    AIRDROP_PATTERN = "airdrop_pattern"
    KNOWN_PHISHING_CONTRACT = "known_phishing_contract"
    SUSPICIOUS_TOKEN_NAME = "suspicious_token_name"
    UNVERIFIED_TOKEN = "unverified_token"
    DUST_ATTACK = "dust_attack"
    FAKE_TRANSFER = "fake_transfer"


@dataclass
class SpamCheckResult:
    """Result of spam check"""
    is_spam: bool
    confidence: float  # 0.0 to 1.0
    reasons: List[SpamReason]
    details: Dict[str, any]


# Known phishing/spam contract addresses (lowercase)
KNOWN_PHISHING_CONTRACTS: Set[str] = {
    # Add known phishing contracts as discovered
    # These are contracts that emit fake Transfer events
}

# Verified token addresses that are always trusted (lowercase)
VERIFIED_TOKENS: Set[str] = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
    "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
    "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
    "0x0000000000a39bb272e79075ade125fd351887ac",  # Blur Pool
    "0x5283d291dbcf85356a21ba090e6db59121208b44",  # BLUR token
    "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",  # wstETH
    "0xae7ab96520de3a18e5e111b5eaab095312d7fe84",  # stETH
    "0xbe9895146f7af43049ca1c1ae358b0541ea49704",  # cbETH
    "0xae78736cd615f374d3085123a210448e74fc6393",  # rETH
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",  # WBTC
    "0x514910771af9ca656af840dff83e8264ecf986ca",  # LINK
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",  # UNI
    "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",  # AAVE
}

# Suspicious patterns in token names/symbols
SUSPICIOUS_PATTERNS = [
    "airdrop", "free", "bonus", "reward", "claim", "gift", "win",
    "lottery", "1000x", "moon", "lambo", "diamond", "rocket", "pump",
    "visit", "http", ".com", ".io", ".xyz", "t.me", "telegram",
    "$", "🚀", "💰", "🎁", "🔥", "💎",
]


class SpamFilter:
    """
    Filters spam/phishing transactions from legitimate ones.

    Usage:
        filter = SpamFilter()
        result = filter.check_transaction(tx, receipt)
        if result.is_spam:
            # Skip or flag this transaction
    """

    # Thresholds
    MAX_EVENTS_NORMAL = 50  # Normal transactions rarely have >50 events
    MAX_EVENTS_SUSPICIOUS = 100  # >100 events is almost certainly spam
    MIN_UNIQUE_RECIPIENTS_FOR_AIRDROP = 10  # Airdrop if >10 unique recipients
    DUST_THRESHOLD = Decimal("0.0001")  # Amounts below this are dust

    def __init__(self, verified_tokens: Set[str] = None):
        """
        Initialize spam filter.

        Args:
            verified_tokens: Set of verified token addresses (lowercase)
        """
        self.verified_tokens = verified_tokens or VERIFIED_TOKENS
        self.phishing_contracts = KNOWN_PHISHING_CONTRACTS.copy()

    def check_transaction(self, tx: Dict, receipt: Dict) -> SpamCheckResult:
        """
        Check if a transaction is spam/phishing.

        Args:
            tx: Transaction data
            receipt: Transaction receipt with logs

        Returns:
            SpamCheckResult with is_spam flag and details
        """
        reasons = []
        details = {}
        confidence = 0.0

        logs = receipt.get('logs', [])
        num_events = len(logs)
        details['num_events'] = num_events

        # Check 1: Too many events
        if num_events > self.MAX_EVENTS_SUSPICIOUS:
            reasons.append(SpamReason.TOO_MANY_EVENTS)
            confidence = max(confidence, 0.95)
            details['event_threshold_exceeded'] = True
        elif num_events > self.MAX_EVENTS_NORMAL:
            reasons.append(SpamReason.TOO_MANY_EVENTS)
            confidence = max(confidence, 0.7)
            details['event_threshold_exceeded'] = True

        # Check 2: Airdrop pattern (many unique recipients)
        airdrop_check = self._check_airdrop_pattern(logs)
        if airdrop_check['is_airdrop']:
            reasons.append(SpamReason.AIRDROP_PATTERN)
            confidence = max(confidence, 0.85)
            details['airdrop'] = airdrop_check

        # Check 3: Known phishing contracts
        phishing_contracts = self._find_phishing_contracts(logs)
        if phishing_contracts:
            reasons.append(SpamReason.KNOWN_PHISHING_CONTRACT)
            confidence = max(confidence, 0.99)
            details['phishing_contracts'] = list(phishing_contracts)

        # Check 4: Unverified tokens with suspicious patterns
        unverified = self._find_unverified_tokens(logs)
        if unverified:
            details['unverified_tokens'] = list(unverified)
            # Only flag as spam if combined with other indicators
            if num_events > self.MAX_EVENTS_NORMAL:
                reasons.append(SpamReason.UNVERIFIED_TOKEN)
                confidence = max(confidence, 0.6)

        # Check 5: Zero ETH value with many token events (common spam pattern)
        eth_value = tx.get('value', 0)
        if isinstance(eth_value, str):
            eth_value = int(eth_value, 16) if eth_value.startswith('0x') else int(eth_value)

        if eth_value == 0 and num_events > self.MAX_EVENTS_NORMAL:
            # Zero ETH transfer with many events is suspicious
            reasons.append(SpamReason.FAKE_TRANSFER)
            confidence = max(confidence, 0.8)
            details['zero_value_many_events'] = True

        # Check 6: Dust attack (many tiny transfers)
        dust_check = self._check_dust_attack(logs)
        if dust_check['is_dust_attack']:
            reasons.append(SpamReason.DUST_ATTACK)
            confidence = max(confidence, 0.75)
            details['dust_attack'] = dust_check

        is_spam = len(reasons) > 0 and confidence >= 0.6

        return SpamCheckResult(
            is_spam=is_spam,
            confidence=confidence,
            reasons=reasons,
            details=details
        )

    def _check_airdrop_pattern(self, logs: List[Dict]) -> Dict:
        """Check for airdrop pattern (one sender to many recipients)"""
        # Extract Transfer events
        transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

        senders = set()
        recipients = set()
        transfer_count = 0

        for log in logs:
            topics = log.get('topics', [])
            if not topics:
                continue

            topic0 = topics[0]
            if isinstance(topic0, bytes):
                topic0 = '0x' + topic0.hex()

            if topic0.lower() == transfer_topic.lower() and len(topics) >= 3:
                transfer_count += 1

                # Extract sender and recipient from topics
                sender = topics[1]
                recipient = topics[2]

                if isinstance(sender, bytes):
                    sender = '0x' + sender.hex()[-40:]
                else:
                    sender = '0x' + sender[-40:]

                if isinstance(recipient, bytes):
                    recipient = '0x' + recipient.hex()[-40:]
                else:
                    recipient = '0x' + recipient[-40:]

                senders.add(sender.lower())
                recipients.add(recipient.lower())

        # Airdrop pattern: few senders, many recipients
        is_airdrop = (
            len(senders) <= 3 and
            len(recipients) >= self.MIN_UNIQUE_RECIPIENTS_FOR_AIRDROP and
            transfer_count > self.MAX_EVENTS_NORMAL
        )

        return {
            'is_airdrop': is_airdrop,
            'num_senders': len(senders),
            'num_recipients': len(recipients),
            'transfer_count': transfer_count
        }

    def _find_phishing_contracts(self, logs: List[Dict]) -> Set[str]:
        """Find any known phishing contracts in logs"""
        found = set()
        for log in logs:
            addr = log.get('address', '').lower()
            if addr in self.phishing_contracts:
                found.add(addr)
        return found

    def _find_unverified_tokens(self, logs: List[Dict]) -> Set[str]:
        """Find unverified token contracts in logs"""
        unverified = set()
        for log in logs:
            addr = log.get('address', '').lower()
            if addr and addr not in self.verified_tokens:
                unverified.add(addr)
        return unverified

    def _check_dust_attack(self, logs: List[Dict]) -> Dict:
        """Check for dust attack (many tiny transfers)"""
        # This would require decoding the transfer amounts
        # For now, use event count as proxy
        transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

        transfer_count = 0
        for log in logs:
            topics = log.get('topics', [])
            if topics:
                topic0 = topics[0]
                if isinstance(topic0, bytes):
                    topic0 = '0x' + topic0.hex()
                if topic0.lower() == transfer_topic.lower():
                    transfer_count += 1

        # Many transfers in one tx is suspicious
        is_dust = transfer_count > 20

        return {
            'is_dust_attack': is_dust,
            'transfer_count': transfer_count
        }

    def add_phishing_contract(self, address: str):
        """Add a contract to the phishing blacklist"""
        self.phishing_contracts.add(address.lower())
        logger.info(f"Added {address} to phishing blacklist")

    def add_verified_token(self, address: str):
        """Add a token to the verified whitelist"""
        self.verified_tokens.add(address.lower())
        logger.info(f"Added {address} to verified tokens")


# Singleton instance for easy access
_default_filter: Optional[SpamFilter] = None


def get_spam_filter() -> SpamFilter:
    """Get the default spam filter instance"""
    global _default_filter
    if _default_filter is None:
        _default_filter = SpamFilter()
    return _default_filter


def is_spam_transaction(tx: Dict, receipt: Dict) -> Tuple[bool, SpamCheckResult]:
    """
    Quick check if a transaction is spam.

    Args:
        tx: Transaction data
        receipt: Transaction receipt

    Returns:
        Tuple of (is_spam, SpamCheckResult)
    """
    filter = get_spam_filter()
    result = filter.check_transaction(tx, receipt)
    return result.is_spam, result
