#!/usr/bin/env python
"""
Simple CLI Decoder Test Script

Usage:
    python test_decoder.py <tx_hash>
    python test_decoder.py 0xb16a2469c46234198fad56a7e1c2c94044c4fa5cb1be515443b58c059a021546
    python test_decoder.py --list-accounts   # Show all COA accounts
    python test_decoder.py --test-blur       # Test a known Blur transaction
    python test_decoder.py --test-gondi      # Test a known Gondi transaction

This uses the actual decoder modules for accurate testing.
"""

import os
import sys
import json
import argparse
from decimal import Decimal
from datetime import datetime, timezone
from typing import List
from dotenv import load_dotenv
from pprint import pprint

load_dotenv()


def get_web3():
    """Initialize Web3 connection."""
    from web3 import Web3

    infura_key = os.getenv("INFURA_API_KEY") or os.getenv("WEB3_INFURA_PROJECT_ID")
    if not infura_key:
        print("[ERROR] No INFURA_API_KEY found in environment")
        sys.exit(1)

    url = f"https://mainnet.infura.io/v3/{infura_key}"
    w3 = Web3(Web3.HTTPProvider(url))

    if not w3.is_connected():
        print("[ERROR] Failed to connect to Ethereum")
        sys.exit(1)

    print(f"[OK] Connected to Ethereum (chain {w3.eth.chain_id})")
    return w3


def get_eth_price(w3, block_number: int) -> Decimal:
    """Get ETH price at block (simplified - uses current price for testing)."""
    # For production, use historical price API
    # For testing, use a reasonable estimate
    return Decimal("3300.00")


def list_coa_accounts():
    """List all accounts in the Chart of Accounts."""
    from main_app.services.decoders.accounts import COA

    print("\n" + "=" * 80)
    print("CHART OF ACCOUNTS (COA)")
    print("=" * 80)

    # Group by account number prefix
    groups = {
        "10xxx": "Assets - Cash & Digital",
        "11xxx": "Assets - Deposits",
        "12xxx": "Assets - Receivables",
        "13xxx": "Assets - Loans & Investments",
        "14xxx": "Assets - NFTs & Unrealized",
        "15xxx": "Assets - Prepaid",
        "18xxx": "Assets - Related Party",
        "19xxx": "Assets - Intercompany/Suspense",
        "20xxx": "Liabilities - Payables",
        "25xxx": "Liabilities - Notes Payable",
        "29xxx": "Liabilities - Related Party",
        "30xxx": "Equity",
        "41xxx": "Income - Other",
        "80xxx": "Expenses",
        "89xxx": "Expenses - Management",
        "90xxx": "Income/Gain-Loss",
    }

    sorted_accounts = sorted(COA.items(), key=lambda x: x[1][0])

    current_group = None
    for account_key, (gl_num, gl_name) in sorted_accounts:
        # Determine group
        prefix = str(gl_num)[:2] + "xxx"
        if prefix != current_group:
            current_group = prefix
            group_name = groups.get(prefix, "Other")
            print(f"\n--- {group_name} ({prefix}) ---")

        print(f"  {gl_num:5d}  {account_key:<55} {gl_name}")

    print(f"\nTotal accounts: {len(COA)}")


def decode_transaction(tx_hash: str, fund_wallets: list = None):
    """Decode a single transaction and show results."""
    from main_app.services.decoders import DecoderRegistry
    from main_app.services.decoders.accounts import COA, get_account

    w3 = get_web3()

    # Default fund wallets for testing
    if fund_wallets is None:
        fund_wallets = [
            "0x3b2A51FEC517BBc7fEaf68AcFdb068b57870713F",  # Example wallet
        ]

    print(f"\n[INFO] Decoding transaction: {tx_hash}")
    print(f"[INFO] Fund wallets: {fund_wallets[:3]}{'...' if len(fund_wallets) > 3 else ''}")

    # Initialize registry
    registry = DecoderRegistry(w3, fund_wallets)
    print(f"[OK] Registry initialized")

    # Decode (registry fetches tx data internally)
    print("\n--- DECODING ---")
    try:
        result = registry.decode_transaction(tx_hash)
    except Exception as e:
        print(f"[ERROR] Decoding failed: {e}")
        import traceback
        traceback.print_exc()
        return None

    # Display transaction info from result
    print("\n--- TRANSACTION INFO ---")
    print(f"  Block: {result.block}")
    print(f"  From: {result.from_address}")
    print(f"  To: {result.to_address}")
    print(f"  Value: {result.value} ETH")
    print(f"  Gas Used: {result.gas_used:,}")
    print(f"  Gas Fee: {result.gas_fee} ETH")
    print(f"  ETH Price: ${result.eth_price}")

    # Display results
    print("\n--- DECODED RESULT ---")
    print(f"  Status: {result.status}")
    print(f"  Platform: {result.platform.value if hasattr(result.platform, 'value') else result.platform}")
    print(f"  Category: {result.category.value if hasattr(result.category, 'value') else result.category}")
    print(f"  Function: {result.function_name}")
    print(f"  Value: {result.value} ETH")
    print(f"  Gas Fee: {result.gas_fee} ETH")

    if result.error:
        print(f"  [ERROR] {result.error}")

    # Events
    print(f"\n--- DECODED EVENTS ({len(result.events)}) ---")
    for i, event in enumerate(result.events):
        print(f"  [{i}] {event.name} @ {event.contract_address[:10]}...")
        if event.args:
            for k, v in list(event.args.items())[:5]:
                print(f"       {k}: {str(v)[:50]}")

    # Journal Entries
    print(f"\n--- JOURNAL ENTRIES ({len(result.journal_entries)}) ---")
    for i, je in enumerate(result.journal_entries):
        print(f"\n  [{i}] {je.entry_id}")
        print(f"      Date: {je.date}")
        print(f"      Description: {je.description}")
        print(f"      Category: {je.category.value if hasattr(je.category, 'value') else je.category}")
        print(f"      Posting Status: {je.posting_status.value if hasattr(je.posting_status, 'value') else je.posting_status}")
        print(f"      Balanced: {je.validate()}")

        print(f"      Entries:")
        for entry in je.entries:
            dr_cr = "DR" if entry['type'] == 'DEBIT' else "CR"
            account = entry['account']
            amount = entry['amount']
            asset = entry['asset']

            # Look up GL number if account is in COA
            gl_info = ""
            if account in COA:
                gl_num, gl_name = COA[account]
                gl_info = f" ({gl_num})"

            print(f"        {dr_cr} {account}{gl_info}: {amount} {asset}")

    # Validate all entries use COA accounts
    print("\n--- ACCOUNT VALIDATION ---")
    non_coa_accounts = set()
    for je in result.journal_entries:
        for entry in je.entries:
            account = entry['account']
            if account not in COA:
                non_coa_accounts.add(account)

    if non_coa_accounts:
        print(f"  [WARNING] Non-COA accounts found:")
        for acc in sorted(non_coa_accounts):
            print(f"    - {acc}")
    else:
        print(f"  [OK] All accounts are in COA")

    return result


def test_blur():
    """Test with a known Blur transaction."""
    # Blur Repay transaction
    tx_hash = "0x1c16d468e458a30ff42aefc95b3d9ee7a1bbfc8e70326d82a1aad909f3844f87"
    print("\n" + "=" * 80)
    print("TESTING BLUR DECODER")
    print("=" * 80)
    decode_transaction(tx_hash)


def test_gondi():
    """Test with a known Gondi transaction."""
    # Gondi LoanRepaid transaction
    tx_hash = "0xdc56082b96e93a06e73e6eb25baff5a9ac0b3b0a8d2c8f0e0e8f9a7d4c3b2a1f"
    print("\n" + "=" * 80)
    print("TESTING GONDI DECODER")
    print("=" * 80)
    decode_transaction(tx_hash)


def test_generic():
    """Test with a generic ERC20 transfer."""
    tx_hash = "0xb16a2469c46234198fad56a7e1c2c94044c4fa5cb1be515443b58c059a021546"
    print("\n" + "=" * 80)
    print("TESTING GENERIC DECODER (ERC20 Transfer)")
    print("=" * 80)
    decode_transaction(tx_hash)


def fetch_wallet_transactions(wallet_address: str, limit: int = 100) -> List[str]:
    """
    Fetch transaction hashes for a wallet from Etherscan.
    """
    import requests
    from dotenv import load_dotenv
    load_dotenv()

    api_key = os.getenv('ETHERSCAN_API_KEY', '')
    if not api_key:
        print("[ERROR] ETHERSCAN_API_KEY not found in environment")
        return []

    print(f"[INFO] Fetching transactions for wallet: {wallet_address}")

    url = "https://api.etherscan.io/v2/api"
    params = {
        'chainid': 1,
        'module': 'account',
        'action': 'txlist',
        'address': wallet_address,
        'startblock': 0,
        'endblock': 99999999,
        'page': 1,
        'offset': limit,
        'sort': 'desc',
        'apikey': api_key
    }

    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == '1':
                txs = data.get('result', [])
                hashes = [tx['hash'] for tx in txs]
                print(f"[OK] Found {len(hashes)} transactions")
                return hashes
            else:
                print(f"[ERROR] API error: {data.get('message', 'Unknown')}")
        else:
            print(f"[ERROR] HTTP {response.status_code}")
    except Exception as e:
        print(f"[ERROR] {e}")

    return []


def decode_wallet(wallet_address: str, limit: int = 50, output_path: str = None):
    """
    Fetch and decode all transactions for a wallet.
    """
    print("\n" + "=" * 80)
    print(f"DECODING WALLET: {wallet_address}")
    print("=" * 80)

    # Fetch transaction hashes
    tx_hashes = fetch_wallet_transactions(wallet_address, limit)
    if not tx_hashes:
        print("[WARN] No transactions found")
        return

    # Decode all transactions
    decode_multiple(tx_hashes, fund_wallets=[wallet_address], export_path=output_path)


def export_from_registry(output_format: str = "json", output_path: str = None):
    """
    Export all decoded transactions from an existing registry.
    This is useful after running the main app to export what was decoded.
    """
    from main_app.services.decoders import DecoderRegistry

    w3 = get_web3()

    # Get fund wallets from s3
    try:
        from main_app.s3_utils import load_WALLET_file
        wallet_df = load_WALLET_file()
        fund_wallets = wallet_df['wallet_address'].tolist() if not wallet_df.empty else []
        print(f"[INFO] Loaded {len(fund_wallets)} fund wallets from S3")
    except Exception as e:
        print(f"[WARN] Could not load wallets: {e}")
        fund_wallets = []

    registry = DecoderRegistry(w3, fund_wallets)

    print(f"\n[INFO] Registry has {len(registry.decoded_cache)} cached transactions")

    if len(registry.decoded_cache) == 0:
        print("[WARN] No transactions in cache. Run the main app first to decode transactions.")
        return

    # Export
    if output_format == "json":
        path = registry.export_decoded_transactions(output_path, format="json")
    elif output_format == "csv":
        path = registry.export_decoded_transactions(output_path, format="csv")
    elif output_format == "journal":
        path = registry.export_journal_entries(output_path)
    else:
        print(f"[ERROR] Unknown format: {output_format}")
        return

    print(f"[OK] Exported to: {path}")


def decode_multiple(tx_hashes: List[str], fund_wallets: list = None, export_path: str = None):
    """
    Decode multiple transactions and optionally export results.
    """
    from main_app.services.decoders import DecoderRegistry
    from main_app.services.decoders.accounts import COA

    w3 = get_web3()

    if fund_wallets is None:
        try:
            from main_app.s3_utils import load_WALLET_file
            wallet_df = load_WALLET_file()
            fund_wallets = wallet_df['wallet_address'].tolist() if not wallet_df.empty else []
            print(f"[INFO] Loaded {len(fund_wallets)} fund wallets from S3")
        except:
            fund_wallets = ["0x3b2A51FEC517BBc7fEaf68AcFdb068b57870713F"]

    registry = DecoderRegistry(w3, fund_wallets)
    print(f"[OK] Registry initialized")
    print(f"\n{'='*80}")
    print(f"DECODING {len(tx_hashes)} TRANSACTIONS")
    print(f"{'='*80}")

    results = []
    errors = []
    non_coa_accounts = set()

    for i, tx_hash in enumerate(tx_hashes):
        print(f"\n[{i+1}/{len(tx_hashes)}] Decoding: {tx_hash[:16]}...")
        try:
            result = registry.decode_transaction(tx_hash)
            results.append(result)

            status_symbol = "[OK]" if result.status == "success" else "[ERR]"
            je_count = len(result.journal_entries)
            balanced = "balanced" if result.entries_balanced else "IMBALANCED"

            print(f"  {status_symbol} {result.platform.value}/{result.category.value} - {je_count} JEs ({balanced})")

            # Check for non-COA accounts
            for je in result.journal_entries:
                for entry in je.entries:
                    if entry['account'] not in COA:
                        non_coa_accounts.add(entry['account'])

        except Exception as e:
            print(f"  [ERR] ERROR: {e}")
            errors.append((tx_hash, str(e)))

    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"  Total: {len(tx_hashes)}")
    print(f"  Success: {len(results)}")
    print(f"  Errors: {len(errors)}")

    if non_coa_accounts:
        print(f"\n  [WARNING] Non-COA accounts found:")
        for acc in sorted(non_coa_accounts):
            print(f"    - {acc}")

    # Export if requested
    if export_path:
        print(f"\n[INFO] Exporting to {export_path}...")
        registry.export_journal_entries(export_path)
        print(f"[OK] Export complete")

    return results


def load_tx_hashes_from_file(filepath: str) -> List[str]:
    """Load transaction hashes from a file (one per line or JSON array)."""
    import json
    from pathlib import Path

    path = Path(filepath)
    if not path.exists():
        print(f"[ERROR] File not found: {filepath}")
        return []

    content = path.read_text().strip()

    # Try JSON first
    try:
        data = json.loads(content)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and 'transactions' in data:
            return [tx.get('tx_hash', tx.get('hash', '')) for tx in data['transactions']]
    except json.JSONDecodeError:
        pass

    # Fall back to line-by-line
    hashes = []
    for line in content.split('\n'):
        line = line.strip()
        if line and line.startswith('0x'):
            hashes.append(line)

    return hashes


def main():
    parser = argparse.ArgumentParser(description="Test transaction decoders")
    parser.add_argument("tx_hash", nargs="?", help="Transaction hash to decode")
    parser.add_argument("--list-accounts", action="store_true", help="List all COA accounts")
    parser.add_argument("--test-blur", action="store_true", help="Test Blur decoder")
    parser.add_argument("--test-gondi", action="store_true", help="Test Gondi decoder")
    parser.add_argument("--test-generic", action="store_true", help="Test Generic decoder")
    parser.add_argument("--wallets", nargs="+", help="Fund wallet addresses")
    parser.add_argument("--export", choices=["json", "csv", "journal"], help="Export format")
    parser.add_argument("--output", "-o", help="Output file path")
    parser.add_argument("--file", "-f", help="File containing transaction hashes (one per line or JSON)")
    parser.add_argument("--batch", action="store_true", help="Process multiple transactions from --file")
    parser.add_argument("--decode-wallet", help="Fetch and decode all transactions for a wallet address")
    parser.add_argument("--limit", type=int, default=50, help="Limit number of transactions to fetch (default: 50)")

    args = parser.parse_args()

    if args.decode_wallet:
        decode_wallet(args.decode_wallet, limit=args.limit, output_path=args.output)
    elif args.list_accounts:
        list_coa_accounts()
    elif args.export:
        export_from_registry(args.export, args.output)
    elif args.batch and args.file:
        tx_hashes = load_tx_hashes_from_file(args.file)
        if tx_hashes:
            decode_multiple(tx_hashes, args.wallets, args.output)
    elif args.file:
        tx_hashes = load_tx_hashes_from_file(args.file)
        if tx_hashes:
            decode_multiple(tx_hashes, args.wallets, args.output)
    elif args.test_blur:
        test_blur()
    elif args.test_gondi:
        test_gondi()
    elif args.test_generic:
        test_generic()
    elif args.tx_hash:
        decode_transaction(args.tx_hash, args.wallets)
    else:
        parser.print_help()
        print("\n\nExamples:")
        print("  python test_decoder.py 0x1234...abcd              # Decode single transaction")
        print("  python test_decoder.py --test-generic             # Test with known tx")
        print("  python test_decoder.py --list-accounts            # Show COA")
        print("  python test_decoder.py --decode-wallet 0xABC...   # Decode all wallet txs")
        print("  python test_decoder.py --decode-wallet 0xABC... --limit 100 -o results.csv")
        print("  python test_decoder.py --file tx_hashes.txt -o journal_entries.csv")


if __name__ == "__main__":
    main()
