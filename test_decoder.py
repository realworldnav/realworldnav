"""
Production Decoder Test Harness

Run: python test_decoder.py <tx_hash>
Or:  python test_decoder.py (uses sample hashes)

Uses the full production decoding engine to test transactions.
"""
import os
import sys

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from dotenv import load_dotenv
from web3 import Web3
from datetime import datetime, timezone
import json
from typing import List, Optional

# Load environment
load_dotenv()

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main_app.services.decoders import (
    DecoderRegistry,
    PostingStatus,
    TransactionCategory,
)

# ============================================================================
# CONFIGURATION
# ============================================================================

INFURA_URL = os.getenv("INFURA_HTTP_URL") or f"https://mainnet.infura.io/v3/{os.getenv('INFURA_API_KEY')}"

# Fund wallets to track
FUND_WALLETS = [
    "0xf9b64dc47dbe8c75f6ffc573cbc7599404bfe5a7",  # Main fund wallet
]

# Sample test hashes - update these as needed
SAMPLE_HASHES = [
    # Review queue transactions
    "0x08ca4d21ad40a7a43888e062809112761a7192991872a3d839b2725093ae6331",
    "0x890015f2c0f143030230c9066e7346b97db99ffcffade54054b35878d329465e",
    "0x49caa7f7bb01b83f5c18723ce1c8037e89dbfd7e39126cd6876339e8924ae8c8",
]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_eth(wei_value: int) -> str:
    """Format wei as ETH with 6 decimal places"""
    eth = wei_value / 1e18
    return f"{eth:.6f} ETH"


def format_posting_status(status: PostingStatus) -> str:
    """Format posting status with color indicator"""
    colors = {
        PostingStatus.AUTO_POST: "\033[92m",    # Green
        PostingStatus.REVIEW_QUEUE: "\033[93m",  # Yellow
        PostingStatus.POSTED: "\033[94m",        # Blue
    }
    reset = "\033[0m"
    return f"{colors.get(status, '')}{status.value}{reset}"


def print_separator(char="=", length=80):
    print(char * length)


def print_header(text: str):
    print_separator()
    print(f"  {text}")
    print_separator()


# ============================================================================
# MAIN DECODER TEST
# ============================================================================

def test_transaction(w3: Web3, registry: DecoderRegistry, tx_hash: str) -> dict:
    """
    Decode a single transaction and print detailed output.
    Returns the decoded transaction dict for further analysis.
    """
    print_header(f"TX: {tx_hash}")

    try:
        # First peek at raw transaction for context
        tx = w3.eth.get_transaction(tx_hash)
        receipt = w3.eth.get_transaction_receipt(tx_hash)

        print(f"\n📋 RAW TRANSACTION:")
        print(f"   From: {tx['from']}")
        print(f"   To:   {tx['to']}")
        print(f"   Value: {format_eth(tx['value'])}")
        print(f"   Block: {tx['blockNumber']}")
        print(f"   Gas Used: {receipt['gasUsed']:,}")
        print(f"   Status: {'SUCCESS' if receipt['status'] == 1 else 'FAILED'}")
        print(f"   Logs: {len(receipt['logs'])}")

        if tx['input'] and len(tx['input']) >= 10:
            selector = tx['input'][:10] if isinstance(tx['input'], str) else '0x' + tx['input'][:4].hex()
            print(f"   Function: {selector}")

        # Decode using registry (it fetches data internally)
        print(f"\n🔍 DECODING...")
        decoded = registry.decode_transaction(tx_hash)

        if not decoded:
            print("   ❌ Decoder returned None - transaction not recognized")
            return {"hash": tx_hash, "status": "not_decoded"}

        print(f"\n✅ DECODED RESULT:")
        print(f"   Status: {decoded.status}")
        print(f"   Platform: {decoded.platform.value}")
        print(f"   Category: {decoded.category.value}")
        print(f"   Posting Status: {format_posting_status(decoded.posting_status)}")
        print(f"   Function: {decoded.function_name}")
        print(f"   Value: {float(decoded.value):.6f} ETH")
        print(f"   Gas Fee: {float(decoded.gas_fee):.6f} ETH")

        if decoded.error:
            print(f"   ⚠️ Error: {decoded.error}")

        # Events
        print(f"\n📡 DECODED EVENTS ({len(decoded.events)}):")
        if decoded.events:
            for i, evt in enumerate(decoded.events):
                print(f"   [{i}] {evt.name} @ log {evt.log_index}")
                for k, v in evt.args.items():
                    val_str = str(v)
                    if len(val_str) > 60:
                        val_str = val_str[:60] + "..."
                    print(f"       {k}: {val_str}")
        else:
            print("   (none)")

        # Journal Entries
        print(f"\n📒 JOURNAL ENTRIES ({len(decoded.journal_entries)}):")
        if decoded.journal_entries:
            for i, je in enumerate(decoded.journal_entries):
                balanced = "✓" if je.validate() else "✗ IMBALANCED"
                print(f"\n   Entry {i+1}: {je.description[:60]}")
                print(f"   Status: {format_posting_status(je.posting_status)} | Balanced: {balanced}")
                print(f"   Category: {je.category.value if hasattr(je.category, 'value') else je.category}")

                for entry in je.entries:
                    # Handle both dict and object formats
                    if isinstance(entry, dict):
                        entry_type = entry.get('type', 'UNKNOWN')
                        account = entry.get('account', 'N/A')
                        amount = entry.get('amount', 0)
                        asset = entry.get('asset', 'ETH')
                    else:
                        entry_type = entry.type.value
                        account = entry.account
                        amount = entry.amount
                        asset = entry.asset
                    symbol = "+" if entry_type == "DEBIT" else "-"
                    print(f"      {symbol} {entry_type:6} {account:40} {float(amount):.6f} {asset}")
        else:
            print("   ❌ NO JOURNAL ENTRIES - This is why it's in review queue!")

        # Why review queue?
        if decoded.posting_status == PostingStatus.REVIEW_QUEUE:
            print(f"\n🔶 REVIEW QUEUE REASON:")
            if not decoded.journal_entries:
                print("   - No journal entries generated")
            else:
                imbalanced = [je for je in decoded.journal_entries if not je.validate()]
                if imbalanced:
                    print(f"   - {len(imbalanced)} imbalanced journal entries")
                pending = [je for je in decoded.journal_entries if je.posting_status == PostingStatus.REVIEW_QUEUE]
                if pending:
                    print(f"   - {len(pending)} entries with REVIEW_QUEUE status")

        return decoded.to_dict()

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {"hash": tx_hash, "error": str(e)}


def main(hashes: List[str] = None):
    """Main entry point"""

    # Initialize Web3
    print("🔌 Connecting to Ethereum...")
    w3 = Web3(Web3.HTTPProvider(INFURA_URL))
    if not w3.is_connected():
        print("❌ Failed to connect to Ethereum")
        sys.exit(1)
    print(f"   ✓ Connected to chain {w3.eth.chain_id}")

    # Initialize decoder registry
    print("\n🔧 Initializing decoder registry...")
    registry = DecoderRegistry(
        w3=w3,
        fund_wallets=FUND_WALLETS,
        fund_id="test_fund"
    )
    print(f"   ✓ Registry initialized with {len(registry._decoder_classes)} platform decoders available")

    # Use provided hashes or samples
    test_hashes = hashes if hashes else SAMPLE_HASHES

    print(f"\n📝 Testing {len(test_hashes)} transaction(s)...\n")

    results = []
    for tx_hash in test_hashes:
        result = test_transaction(w3, registry, tx_hash)
        results.append(result)
        print("\n")

    # Summary
    print_header("SUMMARY")

    auto_post = [r for r in results if r.get('posting_status') == 'auto_post']
    review = [r for r in results if r.get('posting_status') == 'review_queue']
    no_entries = [r for r in results if not r.get('journal_entries')]
    errors = [r for r in results if r.get('error')]

    print(f"   Total: {len(results)}")
    print(f"   Auto-Post Ready: {len(auto_post)}")
    print(f"   Review Queue: {len(review)}")
    print(f"   No Journal Entries: {len(no_entries)}")
    print(f"   Errors: {len(errors)}")

    # Group by platform/category for insights
    print(f"\n   By Platform:")
    platforms = {}
    for r in results:
        p = r.get('platform', 'unknown')
        platforms[p] = platforms.get(p, 0) + 1
    for p, count in sorted(platforms.items(), key=lambda x: -x[1]):
        print(f"      {p}: {count}")

    print(f"\n   By Category:")
    categories = {}
    for r in results:
        c = r.get('category', 'unknown')
        categories[c] = categories.get(c, 0) + 1
    for c, count in sorted(categories.items(), key=lambda x: -x[1]):
        print(f"      {c}: {count}")


if __name__ == "__main__":
    # Get hashes from command line or use samples
    if len(sys.argv) > 1:
        # Handle comma-separated or space-separated hashes
        hashes = []
        for arg in sys.argv[1:]:
            # Split on commas if present
            for h in arg.split(','):
                h = h.strip()
                if h:
                    # Ensure 0x prefix
                    if not h.startswith('0x'):
                        h = '0x' + h
                    hashes.append(h)
        main(hashes)
    else:
        print("Usage: python test_decoder.py <tx_hash1> [tx_hash2] ...")
        print("       python test_decoder.py (runs sample hashes)")
        print()
        main()
