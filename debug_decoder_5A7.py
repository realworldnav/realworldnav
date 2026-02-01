"""
Debug script to test decoder on 5A7 wallet transactions
"""
import os
import sys
import csv
from decimal import Decimal

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load environment
from dotenv import load_dotenv
load_dotenv()

from web3 import Web3
from main_app.services.decoders import DecoderRegistry

# Initialize Web3
INFURA_URL = f"https://mainnet.infura.io/v3/{os.getenv('INFURA_API_KEY')}"
w3 = Web3(Web3.HTTPProvider(INFURA_URL))
print(f"Web3 connected: {w3.is_connected()}")

# Fund wallets from the CSV
FUND_WALLETS = [
    "0xF9B64dc47dbE8c75f6FFC573cbC7599404bfe5A7",  # Main wallet in this export
    "0x3b2A51FEC517BBc7fEaf68AcFdb068b57870713F",
    "0xCd8826a0298cfCd4eeB3a0c118a8aa664316Cd7A",
    "0x943b28A07259b7F368657e47185693aC0021B639",
    "0x50Cd286B00bDa2243EfF0148d46ea817E54C5411",
]

# Normalize addresses
FUND_WALLETS = [w.lower() for w in FUND_WALLETS]

# Initialize decoder registry
registry = DecoderRegistry(w3, FUND_WALLETS, fund_id="drip_capital")

def test_transaction(tx_hash: str, expected_platform: str = None, expected_category: str = None):
    """Test a single transaction"""
    print(f"\n{'='*80}")
    print(f"TX: {tx_hash}")
    print(f"Expected: platform={expected_platform}, category={expected_category}")
    print("-" * 80)

    try:
        decoded = registry.decode_transaction(tx_hash)
        result = decoded.to_dict()

        print(f"Status: {result.get('status')}")
        print(f"Platform: {result.get('platform')}")
        print(f"Category: {result.get('category')}")
        print(f"Function: {result.get('function_name')}")

        # Check journal entries
        jes = result.get('journal_entries', [])
        print(f"Journal Entries: {len(jes)}")

        for i, je in enumerate(jes):
            print(f"  JE {i+1}:")
            debits = je.get('debits', [])
            credits = je.get('credits', [])
            for d in debits:
                print(f"    DR: {d.get('account')} ${d.get('amount_usd', 0):.2f}")
            for c in credits:
                print(f"    CR: {c.get('account')} ${c.get('amount_usd', 0):.2f}")

        # Check events
        events = result.get('events', [])
        print(f"Events: {len(events)}")
        for evt in events[:5]:  # Show first 5
            print(f"  - {evt.get('event_name')}: {evt.get('decoded_data', {})}")

        return {
            'tx_hash': tx_hash,
            'platform': result.get('platform'),
            'category': result.get('category'),
            'function': result.get('function_name'),
            'status': result.get('status'),
            'je_count': len(jes),
            'events_count': len(events),
        }

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {
            'tx_hash': tx_hash,
            'platform': 'error',
            'category': 'error',
            'function': str(e)[:50],
            'status': 'error',
            'je_count': 0,
            'events_count': 0,
        }

def main():
    # Read CSV
    csv_path = "raw_transactions_export_5A7.csv"

    # Collect problematic transactions to test
    test_cases = []

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Focus on transactions that need work
            if row['posting_status'] == 'review_queue' or int(row['journal_entries_count']) == 0:
                test_cases.append({
                    'tx_hash': row['tx_hash'],
                    'platform': row['platform'],
                    'category': row['category'],
                    'function': row['function_name'],
                    'current_je_count': int(row['journal_entries_count']),
                })

    print(f"\nFound {len(test_cases)} transactions needing review")

    # Group by platform/category for analysis
    by_type = {}
    for tc in test_cases:
        key = f"{tc['platform']}/{tc['category']}"
        if key not in by_type:
            by_type[key] = []
        by_type[key].append(tc)

    print("\nBreakdown by type:")
    for key, items in sorted(by_type.items()):
        print(f"  {key}: {len(items)} transactions")

    # Test ALL transactions (not just a sample)
    results = []

    for tc in test_cases:
        result = test_transaction(tc['tx_hash'], tc['platform'], tc['category'])
        results.append(result)

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    success = sum(1 for r in results if r['je_count'] > 0)
    failed = sum(1 for r in results if r['je_count'] == 0)

    print(f"Tested: {len(results)}")
    print(f"With JEs: {success}")
    print(f"Without JEs: {failed}")

    # Write results
    with open('decoder_debug_results_5A7.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['tx_hash', 'platform', 'category', 'function', 'status', 'je_count', 'events_count'])
        writer.writeheader()
        writer.writerows(results)

    print(f"\nResults written to decoder_debug_results_5A7.csv")

    # List failures
    print("\n" + "=" * 80)
    print("FAILURES (0 JEs)")
    print("=" * 80)
    for r in results:
        if r['je_count'] == 0:
            print(f"  {r['tx_hash'][:16]}... {r['platform']}/{r['category']} - {r['function']}")

if __name__ == "__main__":
    main()
