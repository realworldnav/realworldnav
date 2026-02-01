"""
Debug Decoder Engine - Run exact same process as app.py

This script replicates the EXACT decoding process from blockchain_listener.py
to debug why transactions aren't generating journal entries.
"""

import os
import sys
import pandas as pd
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

def init_web3():
    """Initialize Web3 exactly as the app does"""
    from web3 import Web3

    infura_key = os.getenv('INFURA_API_KEY')
    if not infura_key:
        print("[ERROR] No INFURA_API_KEY found in environment")
        return None

    infura_url = f"https://mainnet.infura.io/v3/{infura_key}"
    w3 = Web3(Web3.HTTPProvider(infura_url))

    if w3.is_connected():
        chain_id = w3.eth.chain_id
        print(f"[OK] Web3 connected to chain {chain_id}")
        return w3
    else:
        print("[ERROR] Web3 not connected")
        return None

def load_fund_wallets():
    """Load fund wallets exactly as the app does"""
    try:
        from main_app.s3_utils import load_WALLET_file
        wallet_df = load_WALLET_file()
        if wallet_df is not None and not wallet_df.empty:
            wallets = wallet_df['wallet_address'].str.strip().tolist()
            print(f"[OK] Loaded {len(wallets)} fund wallets")
            return wallets
    except Exception as e:
        print(f"[ERROR] Could not load wallets: {e}")
    return []

def decode_single_transaction(registry, tx_hash: str, verbose: bool = True):
    """
    Decode a single transaction using the EXACT same process as the app.

    This mirrors blockchain_listener.py lines 901-919
    """
    try:
        # This is the EXACT call from blockchain_listener.py line 904
        decoded = registry.decode_transaction(tx_hash)

        # Convert to dict (same as line 905)
        result = decoded.to_dict()

        if verbose:
            print(f"\n{'='*80}")
            print(f"TX: {tx_hash}")
            print(f"{'='*80}")
            print(f"  Platform: {result.get('platform')} | Category: {result.get('category')}")
            print(f"  Function: {result.get('function_name')} | Block: {result.get('block')}")
            print(f"  ETH Price: ${result.get('eth_price', 0):,.2f} | Gas: {result.get('gas_fee', 0):.8f} ETH")

            # Show events
            events = result.get('events', [])
            print(f"\n  EVENTS ({len(events)}):")
            for i, evt in enumerate(events):
                print(f"    {i+1}. {evt.get('name', 'Unknown')}")
                args = evt.get('args', {})
                for k, v in list(args.items())[:3]:  # Show first 3 args
                    v_str = str(v)[:40] + ('...' if len(str(v)) > 40 else '')
                    print(f"       {k}: {v_str}")

            # Show journal entries
            journal_entries = result.get('journal_entries', [])
            print(f"\n  JOURNAL ENTRIES ({len(journal_entries)}):")

            if not journal_entries:
                print("    >>> NO JOURNAL ENTRIES GENERATED <<<")
            else:
                for i, je in enumerate(journal_entries):
                    print(f"\n    Entry {i+1}: {je.get('description', 'N/A')[:50]}")
                    print(f"      Status: {je.get('posting_status')} | Balanced: {je.get('is_balanced')}")

                    entries = je.get('entries', [])
                    for line in entries:
                        print(f"        {line.get('type'):6} | {line.get('account'):35} | {line.get('amount'):>12.6f} {line.get('asset', 'ETH')}")

        return result

    except Exception as e:
        if verbose:
            print(f"\n{'='*80}")
            print(f"TX: {tx_hash}")
            print(f"{'='*80}")
            print(f"  [ERROR] Decoding failed: {e}")
            import traceback
            traceback.print_exc()
        return {"status": "error", "error": str(e)}

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--all', action='store_true', help='Show all transactions')
    parser.add_argument('--failures-only', action='store_true', help='Only show transactions without JEs')
    parser.add_argument('--limit', type=int, default=50, help='Max transactions to process')
    parser.add_argument('--hash', type=str, help='Process single tx hash')
    args = parser.parse_args()

    # Read transaction hashes from CSV
    csv_path = os.path.join(os.path.dirname(__file__), "raw_transactions_export.csv")
    if not os.path.exists(csv_path):
        print(f"[ERROR] CSV not found: {csv_path}")
        return

    df = pd.read_csv(csv_path)

    if args.hash:
        tx_hashes = [args.hash]
    else:
        tx_hashes = df['tx_hash'].unique().tolist()[:args.limit]

    print(f"\n[INFO] Processing {len(tx_hashes)} transactions")

    # Initialize Web3 (same as app)
    w3 = init_web3()
    if not w3:
        return

    # Load fund wallets (same as app)
    fund_wallets = load_fund_wallets()

    # Initialize DecoderRegistry (same as blockchain_listener.py line 849)
    print("\n[INFO] Initializing DecoderRegistry...")
    try:
        from main_app.services.decoders import DecoderRegistry
        registry = DecoderRegistry(w3, fund_wallets, fund_id="holdings_class_B_ETH")
        print(f"[OK] DecoderRegistry initialized with {len(fund_wallets)} wallets")
    except Exception as e:
        print(f"[ERROR] Failed to initialize DecoderRegistry: {e}")
        import traceback
        traceback.print_exc()
        return

    # Process transactions
    results = []
    failures = []

    for i, tx_hash in enumerate(tx_hashes):
        print(f"\n[{i+1}/{len(tx_hashes)}] Processing {tx_hash[:16]}...")

        # Decide whether to show verbose output
        if args.hash:
            verbose = True
        elif args.failures_only:
            # First decode quietly to check
            result = decode_single_transaction(registry, tx_hash, verbose=False)
            je_count = len(result.get('journal_entries', []))
            if je_count == 0:
                # Now show verbose for failures
                print(f"  FAILURE - No JEs generated, showing details:")
                result = decode_single_transaction(registry, tx_hash, verbose=True)
                failures.append(tx_hash)
            else:
                print(f"  OK - {je_count} JEs generated")
            verbose = False
        else:
            verbose = True
            result = decode_single_transaction(registry, tx_hash, verbose=verbose)

        if not args.failures_only or args.hash:
            result = decode_single_transaction(registry, tx_hash, verbose=verbose) if verbose else result

        je_count = len(result.get('journal_entries', []))
        results.append({
            'tx_hash': tx_hash,
            'platform': result.get('platform', 'error'),
            'category': result.get('category', 'ERROR'),
            'function': result.get('function_name', 'unknown'),
            'status': result.get('status', 'error'),
            'je_count': je_count,
            'events_count': len(result.get('events', [])),
        })

    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)

    results_df = pd.DataFrame(results)
    total = len(results_df)
    with_jes = len(results_df[results_df['je_count'] > 0])
    without_jes = len(results_df[results_df['je_count'] == 0])

    print(f"\nTotal processed: {total}")
    print(f"With JEs:        {with_jes} ({100*with_jes/total:.1f}%)")
    print(f"Without JEs:     {without_jes} ({100*without_jes/total:.1f}%)")

    print("\n--- By Platform ---")
    platform_summary = results_df.groupby('platform').agg({
        'je_count': ['count', 'sum'],
        'tx_hash': 'count'
    })
    platform_summary.columns = ['total', 'total_jes', 'tx_count']
    platform_summary['avg_jes'] = platform_summary['total_jes'] / platform_summary['tx_count']
    print(platform_summary)

    print("\n--- By Category ---")
    category_summary = results_df.groupby('category').agg({
        'je_count': ['count', 'sum'],
        'tx_hash': 'count'
    })
    category_summary.columns = ['total', 'total_jes', 'tx_count']
    category_summary['avg_jes'] = category_summary['total_jes'] / category_summary['tx_count']
    print(category_summary)

    print("\n--- Failures (No JEs) ---")
    no_je = results_df[results_df['je_count'] == 0]
    if len(no_je) > 0:
        print(no_je[['tx_hash', 'platform', 'category', 'function']].to_string())
    else:
        print("None! All transactions generated journal entries.")

    # Save results
    results_df.to_csv("decoder_debug_results.csv", index=False)
    print(f"\nResults saved to decoder_debug_results.csv")

if __name__ == "__main__":
    main()
