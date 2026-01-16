"""
Unit tests for decoder reliability fixes.

Tests:
- JournalEntry JSON serialization (no datetime objects)
- JournalEntry.validate() passes for WETH wrap/unwrap
- GL posting deduplication logic
- LegacyRegistryAdapter consistency
"""
import pytest
from decimal import Decimal
from datetime import datetime, timezone
import json
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main_app.services.decoders.base import (
    JournalEntry,
    TransactionCategory,
    Platform,
    PostingStatus,
    DecodedTransaction,
    ETH_EQUIVALENT_ASSETS,
)


class TestJournalEntrySerialization:
    """Test JournalEntry JSON serialization."""

    def test_to_dict_no_datetime_objects(self):
        """Verify to_dict() returns JSON-safe types (no datetime objects)."""
        entry = JournalEntry(
            entry_id="test_001",
            date=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            description="Test entry",
            tx_hash="0xabc123def456",
            category=TransactionCategory.WETH_WRAP,
            platform=Platform.GENERIC,
            eth_usd_price=Decimal("3000.00")
        )
        entry.add_debit("100.31 - WETH", Decimal("1.0"), "WETH")
        entry.add_credit("100.30 - ETH Wallet", Decimal("1.0"), "ETH")

        result = entry.to_dict()

        # Check date is string (ISO format)
        assert isinstance(result['date'], str), "date should be string"
        assert 'T' in result['date'], "date should be ISO format with T separator"

        # Check enums are strings
        assert isinstance(result['category'], str), "category should be string"
        assert isinstance(result['platform'], str), "platform should be string"
        assert isinstance(result['posting_status'], str), "posting_status should be string"

        # Check Decimal is float
        assert isinstance(result['eth_usd_price'], float), "eth_usd_price should be float"

        # Verify entire dict is JSON serializable
        try:
            json_str = json.dumps(result)
            assert json_str is not None
        except TypeError as e:
            pytest.fail(f"to_dict() result is not JSON serializable: {e}")

    def test_to_dict_includes_is_balanced(self):
        """Verify to_dict() includes is_balanced field."""
        entry = JournalEntry(
            entry_id="test_002",
            date=datetime.now(timezone.utc),
            description="Test",
            tx_hash="0x123",
            category=TransactionCategory.ETH_TRANSFER,
            platform=Platform.GENERIC,
            eth_usd_price=Decimal("3000")
        )
        entry.add_debit("100.30 - ETH", Decimal("1.0"), "ETH")
        entry.add_credit("200.10 - Payable", Decimal("1.0"), "ETH")

        result = entry.to_dict()

        assert 'is_balanced' in result, "to_dict should include is_balanced field"
        assert result['is_balanced'] is True, "balanced entry should have is_balanced=True"

    def test_to_dict_handles_missing_isoformat(self):
        """Verify to_dict() handles date without isoformat method."""
        entry = JournalEntry(
            entry_id="test_003",
            date="2024-01-15T12:00:00",  # String instead of datetime
            description="Test",
            tx_hash="0x123",
            category=TransactionCategory.ETH_TRANSFER,
            platform=Platform.GENERIC,
            eth_usd_price=Decimal("3000")
        )

        result = entry.to_dict()

        # Should not raise, and date should be string
        assert isinstance(result['date'], str)


class TestJournalEntryValidation:
    """Test JournalEntry balance validation."""

    def test_weth_wrap_validates(self):
        """WETH wrap (debit WETH, credit ETH) should pass validation."""
        entry = JournalEntry(
            entry_id="wrap_001",
            date=datetime.now(timezone.utc),
            description="WETH Wrap: 1.5 ETH -> WETH",
            tx_hash="0x123abc",
            category=TransactionCategory.WETH_WRAP,
            platform=Platform.GENERIC,
            eth_usd_price=Decimal("3000")
        )
        entry.add_debit("100.31 - WETH", Decimal("1.5"), "WETH")
        entry.add_credit("100.30 - ETH Wallet", Decimal("1.5"), "ETH")

        assert entry.validate() is True, "WETH wrap should validate (WETH=ETH equivalent)"

    def test_weth_unwrap_validates(self):
        """WETH unwrap (debit ETH, credit WETH) should pass validation."""
        entry = JournalEntry(
            entry_id="unwrap_001",
            date=datetime.now(timezone.utc),
            description="WETH Unwrap: 2.0 WETH -> ETH",
            tx_hash="0x456def",
            category=TransactionCategory.WETH_UNWRAP,
            platform=Platform.GENERIC,
            eth_usd_price=Decimal("3000")
        )
        entry.add_debit("100.30 - ETH Wallet", Decimal("2.0"), "ETH")
        entry.add_credit("100.31 - WETH", Decimal("2.0"), "WETH")

        assert entry.validate() is True, "WETH unwrap should validate (WETH=ETH equivalent)"

    def test_eth_transfer_validates(self):
        """Simple ETH transfer (same asset) should pass validation."""
        entry = JournalEntry(
            entry_id="transfer_001",
            date=datetime.now(timezone.utc),
            description="ETH Transfer Out",
            tx_hash="0x789ghi",
            category=TransactionCategory.ETH_TRANSFER,
            platform=Platform.GENERIC,
            eth_usd_price=Decimal("3000")
        )
        entry.add_debit("100.30 - ETH Wallet", Decimal("1.0"), "ETH")
        entry.add_credit("200.10 - ETH Payable", Decimal("1.0"), "ETH")

        assert entry.validate() is True, "Same-asset entry should validate"

    def test_empty_entries_fails(self):
        """Entry with no line items should fail validation."""
        entry = JournalEntry(
            entry_id="empty_001",
            date=datetime.now(timezone.utc),
            description="Empty entry",
            tx_hash="0xempty",
            category=TransactionCategory.UNKNOWN,
            platform=Platform.GENERIC,
            eth_usd_price=Decimal("3000")
        )

        assert entry.validate() is False, "Empty entry should fail validation"

    def test_unbalanced_entry_fails(self):
        """Unbalanced entry (debits != credits) should fail."""
        entry = JournalEntry(
            entry_id="unbal_001",
            date=datetime.now(timezone.utc),
            description="Unbalanced",
            tx_hash="0xunbal",
            category=TransactionCategory.ETH_TRANSFER,
            platform=Platform.GENERIC,
            eth_usd_price=Decimal("3000")
        )
        entry.add_debit("100.30 - ETH", Decimal("1.0"), "ETH")
        entry.add_credit("200.10 - Payable", Decimal("0.5"), "ETH")  # Wrong amount!

        assert entry.validate() is False, "Unbalanced entry should fail"

    def test_mixed_assets_non_eth_fails(self):
        """Non-ETH-equivalent cross-asset entry should fail."""
        entry = JournalEntry(
            entry_id="mixed_001",
            date=datetime.now(timezone.utc),
            description="Mixed assets",
            tx_hash="0xmixed",
            category=TransactionCategory.ERC20_TRANSFER,
            platform=Platform.GENERIC,
            eth_usd_price=Decimal("3000")
        )
        entry.add_debit("100.40 - USDC", Decimal("3000"), "USDC")
        entry.add_credit("100.30 - ETH", Decimal("1.0"), "ETH")  # Different asset!

        # This should fail because USDC and ETH are not equivalent
        assert entry.validate() is False, "Non-equivalent cross-asset should fail"

    def test_eth_equivalent_constant_exists(self):
        """Verify ETH_EQUIVALENT_ASSETS constant is defined correctly."""
        assert 'ETH' in ETH_EQUIVALENT_ASSETS
        assert 'WETH' in ETH_EQUIVALENT_ASSETS
        assert 'USDC' not in ETH_EQUIVALENT_ASSETS


class TestGLPostingIdempotency:
    """Test GL posting deduplication logic."""

    def test_generate_row_key_deterministic(self):
        """Same data should produce same key."""
        # Import the helper function
        from main_app.modules.home.decoded_transactions_outputs import _generate_gl_row_key

        row = {
            'hash': '0xabc123def456789',
            'account_name': '100.30 - ETH Wallet',
            'transaction_type': 'WETH_WRAP',
            'debit_crypto': 1.5,
            'credit_crypto': 0
        }

        key1 = _generate_gl_row_key(row)
        key2 = _generate_gl_row_key(row)

        assert key1 == key2, "Same row should produce same key"

    def test_generate_row_key_different_for_different_data(self):
        """Different data should produce different keys."""
        from main_app.modules.home.decoded_transactions_outputs import _generate_gl_row_key

        row1 = {
            'hash': '0xabc123',
            'account_name': '100.30 - ETH',
            'transaction_type': 'WETH_WRAP',
            'debit_crypto': 1.0,
            'credit_crypto': 0
        }
        row2 = {
            'hash': '0xdef456',  # Different hash
            'account_name': '100.30 - ETH',
            'transaction_type': 'WETH_WRAP',
            'debit_crypto': 1.0,
            'credit_crypto': 0
        }

        assert _generate_gl_row_key(row1) != _generate_gl_row_key(row2), \
            "Different tx hash should produce different key"

    def test_generate_row_key_different_accounts(self):
        """Same tx but different accounts should have different keys."""
        from main_app.modules.home.decoded_transactions_outputs import _generate_gl_row_key

        row1 = {
            'hash': '0xsame',
            'account_name': '100.30 - ETH',
            'transaction_type': 'WETH_WRAP',
            'debit_crypto': 1.0,
            'credit_crypto': 0
        }
        row2 = {
            'hash': '0xsame',  # Same hash
            'account_name': '100.31 - WETH',  # Different account
            'transaction_type': 'WETH_WRAP',
            'debit_crypto': 0,
            'credit_crypto': 1.0
        }

        assert _generate_gl_row_key(row1) != _generate_gl_row_key(row2), \
            "Different accounts should produce different keys (both sides of entry)"

    def test_generate_row_key_handles_missing_fields(self):
        """Key generation should handle missing fields gracefully."""
        from main_app.modules.home.decoded_transactions_outputs import _generate_gl_row_key

        row = {'hash': '0x123'}  # Missing most fields

        # Should not raise
        key = _generate_gl_row_key(row)
        assert key is not None
        assert '0x123' in key


class TestLegacyAdapter:
    """Test LegacyRegistryAdapter consistency."""

    def test_adapter_returns_consistent_stats_format(self):
        """Adapter stats should match DecoderRegistry.stats format."""
        from main_app.services.decoders.adapter import LegacyRegistryAdapter

        mock_cache = {
            'tx1': {
                'status': 'success',
                'platform': 'blur',
                'tx_hash': 'tx1',
                'timestamp': '2024-01-01T00:00:00+00:00',
                'value': 1.5
            },
            'tx2': {
                'status': 'error',
                'platform': 'generic',
                'tx_hash': 'tx2',
                'timestamp': '2024-01-02T00:00:00+00:00',
                'error': 'Failed to decode'
            }
        }

        adapter = LegacyRegistryAdapter(lambda: mock_cache)
        stats = adapter.stats

        # Check required fields exist
        assert 'total_decoded' in stats
        assert 'success_count' in stats
        assert 'error_count' in stats
        assert 'auto_post_ready' in stats
        assert 'review_queue' in stats
        assert 'platforms' in stats
        assert 'decoders_loaded' in stats

        # Check types
        assert isinstance(stats['platforms'], dict)
        assert isinstance(stats['decoders_loaded'], list)

        # Check values
        assert stats['total_decoded'] == 2
        assert stats['success_count'] == 1
        assert stats['error_count'] == 1

    def test_adapter_decoded_cache_returns_transactions(self):
        """Adapter decoded_cache should return DecodedTransaction objects."""
        from main_app.services.decoders.adapter import LegacyRegistryAdapter

        mock_cache = {
            'tx1': {
                'status': 'success',
                'platform': 'blur',
                'tx_hash': '0x123',
                'timestamp': '2024-01-01T00:00:00',
                'value': 1.0,
                'block': 12345
            }
        }

        adapter = LegacyRegistryAdapter(lambda: mock_cache)
        cache = adapter.decoded_cache

        assert 'tx1' in cache
        tx = cache['tx1']
        assert isinstance(tx, DecodedTransaction)
        assert tx.tx_hash == '0x123'
        assert tx.status == 'success'

    def test_adapter_auto_post_ready_empty(self):
        """Legacy adapter should return empty auto_post_ready (all go to review)."""
        from main_app.services.decoders.adapter import LegacyRegistryAdapter

        mock_cache = {
            'tx1': {'status': 'success', 'platform': 'blur', 'tx_hash': 'tx1', 'timestamp': '2024-01-01T00:00:00'}
        }

        adapter = LegacyRegistryAdapter(lambda: mock_cache)

        assert adapter.get_auto_post_ready() == [], "Legacy adapter should have no auto-post ready"

    def test_adapter_review_queue_returns_success_txs(self):
        """Legacy adapter review_queue should return successful transactions."""
        from main_app.services.decoders.adapter import LegacyRegistryAdapter

        mock_cache = {
            'tx1': {'status': 'success', 'platform': 'blur', 'tx_hash': 'tx1', 'timestamp': '2024-01-01T00:00:00'},
            'tx2': {'status': 'error', 'platform': 'generic', 'tx_hash': 'tx2', 'timestamp': '2024-01-01T00:00:00'}
        }

        adapter = LegacyRegistryAdapter(lambda: mock_cache)
        review = adapter.get_review_queue()

        assert len(review) == 1, "Only successful txs should be in review queue"
        assert review[0].tx_hash == 'tx1'

    def test_adapter_handles_empty_cache(self):
        """Adapter should handle empty cache gracefully."""
        from main_app.services.decoders.adapter import LegacyRegistryAdapter

        adapter = LegacyRegistryAdapter(lambda: {})

        assert adapter.decoded_cache == {}
        assert adapter.stats['total_decoded'] == 0
        assert adapter.get_auto_post_ready() == []
        assert adapter.get_review_queue() == []

    def test_adapter_handles_none_cache(self):
        """Adapter should handle None cache gracefully."""
        from main_app.services.decoders.adapter import LegacyRegistryAdapter

        adapter = LegacyRegistryAdapter(lambda: None)

        assert adapter.decoded_cache == {}
        assert adapter.stats['total_decoded'] == 0


class TestDecodedTransactionSerialization:
    """Test DecodedTransaction serialization."""

    def test_to_dict_json_safe(self):
        """DecodedTransaction.to_dict() should be JSON serializable."""
        tx = DecodedTransaction(
            status="success",
            tx_hash="0xabc123",
            platform=Platform.BLUR,
            category=TransactionCategory.LOAN_ORIGINATION,
            block=12345,
            timestamp=datetime.now(timezone.utc),
            eth_price=Decimal("3000.50"),
            gas_used=150000,
            gas_fee=Decimal("0.015"),
            from_address="0xsender",
            to_address="0xreceiver",
            value=Decimal("1.5"),
            function_name="borrow"
        )

        result = tx.to_dict()

        # Verify JSON serializable
        try:
            json_str = json.dumps(result)
            assert json_str is not None
        except TypeError as e:
            pytest.fail(f"DecodedTransaction.to_dict() not JSON serializable: {e}")

        # Check timestamp is string
        assert isinstance(result['timestamp'], str)

        # Check Decimals are floats
        assert isinstance(result['eth_price'], float)
        assert isinstance(result['gas_fee'], float)
        assert isinstance(result['value'], float)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
