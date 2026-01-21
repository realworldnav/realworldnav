"""
Batch Decoder - Fetch and decode all recent transactions for fund wallets

Usage:
    python batch_decode.py                    # Last 24 hours (default)
    python batch_decode.py --hours 48         # Last 48 hours
    python batch_decode.py --limit 50         # Limit to 50 transactions
    python batch_decode.py --verbose          # Enable debug logging
    python batch_decode.py --output results.csv  # Export results to CSV
"""

import os
import sys
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
import argparse

# Load environment
from dotenv import load_dotenv
load_dotenv()

from web3 import Web3


def get_rpc_url() -> str:
    """Get RPC URL from environment"""
    return os.getenv("WEB3_HTTP_URL", os.getenv("MAINNET_RPC_URL", "https://eth.llamarpc.com"))


def load_fund_wallets() -> List[str]:
    """Load fund wallet addresses from S3"""
    try:
        from main_app.s3_utils import load_WALLET_file
        wallet_df = load_WALLET_file()
        if wallet_df is not None and not wallet_df.empty:
            addresses = wallet_df['wallet_address'].dropna().tolist()
            return [addr.lower().strip() for addr in addresses if addr]
    except Exception as e:
        print(f"[!] Could not load wallets from S3: {e}")
    return []


def fetch_transactions_from_etherscan(
    wallet: str,
    hours: int = 24,
    limit: int = 100
) -> List[Dict]:
    """Fetch transactions from Etherscan API for a wallet"""
    api_key = os.getenv('ETHERSCAN_API_KEY', '')
    if not api_key:
        print("[!] ETHERSCAN_API_KEY not set")
        return []

    # Calculate timestamp for hours ago
    cutoff_time = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())

    transactions = []

    # Fetch normal transactions
    url = "https://api.etherscan.io/v2/api"
    params = {
        'chainid': 1,
        'module': 'account',
        'action': 'txlist',
        'address': wallet,
        'startblock': 0,
        'endblock': 99999999,
        'page': 1,
        'offset': limit,
        'sort': 'desc',
        'apikey': api_key
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == '1':
                for tx in data.get('result', []):
                    tx_time = int(tx.get('timeStamp', 0))
                    if tx_time >= cutoff_time:
                        transactions.append({
                            'hash': tx.get('hash'),
                            'timestamp': tx_time,
                            'from': tx.get('from', '').lower(),
                            'to': tx.get('to', '').lower(),
                            'value': int(tx.get('value', 0)),
                            'block': int(tx.get('blockNumber', 0)),
                            'type': 'normal'
                        })
    except Exception as e:
        print(f"[!] Error fetching normal txs: {e}")

    time.sleep(0.25)  # Rate limit

    # Fetch internal transactions (for contract interactions)
    params['action'] = 'txlistinternal'
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == '1':
                for tx in data.get('result', []):
                    tx_time = int(tx.get('timeStamp', 0))
                    if tx_time >= cutoff_time:
                        # Only add if hash not already present
                        if not any(t['hash'] == tx.get('hash') for t in transactions):
                            transactions.append({
                                'hash': tx.get('hash'),
                                'timestamp': tx_time,
                                'from': tx.get('from', '').lower(),
                                'to': tx.get('to', '').lower(),
                                'value': int(tx.get('value', 0)),
                                'block': int(tx.get('blockNumber', 0)),
                                'type': 'internal'
                            })
    except Exception as e:
        print(f"[!] Error fetching internal txs: {e}")

    time.sleep(0.25)  # Rate limit

    # Fetch ERC20 transfers
    params['action'] = 'tokentx'
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == '1':
                for tx in data.get('result', []):
                    tx_time = int(tx.get('timeStamp', 0))
                    if tx_time >= cutoff_time:
                        # Only add if hash not already present
                        if not any(t['hash'] == tx.get('hash') for t in transactions):
                            transactions.append({
                                'hash': tx.get('hash'),
                                'timestamp': tx_time,
                                'from': tx.get('from', '').lower(),
                                'to': tx.get('to', '').lower(),
                                'value': int(tx.get('value', 0)),
                                'block': int(tx.get('blockNumber', 0)),
                                'type': 'erc20',
                                'token': tx.get('tokenSymbol', 'UNKNOWN')
                            })
    except Exception as e:
        print(f"[!] Error fetching token txs: {e}")

    return transactions


def print_section(title: str, char: str = "="):
    """Print a section header"""
    print(f"\n{char * 70}")
    print(f" {title}")
    print(f"{char * 70}")


def format_eth(value: Decimal) -> str:
    """Format ETH value"""
    return f"{float(value):.6f} ETH"


def main():
    parser = argparse.ArgumentParser(description='Batch decode recent transactions')
    parser.add_argument('--hours', type=int, default=24, help='Hours to look back (default: 24)')
    parser.add_argument('--limit', type=int, default=100, help='Max transactions per wallet (default: 100)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable debug logging')
    parser.add_argument('--output', '-o', type=str, help='Output CSV file path')
    parser.add_argument('--wallet', type=str, help='Specific wallet to check (overrides loading all)')
    args = parser.parse_args()

    # Enable debug logging if requested
    if args.verbose:
        os.environ['DECODER_DEBUG'] = '1'
        from main_app.logging_config import setup_decoder_debug_logging, DEBUG_LOG_PATH
        setup_decoder_debug_logging()
        print(f"[DEBUG] Verbose logging enabled -> {DEBUG_LOG_PATH}")

    print_section("BATCH TRANSACTION DECODER")
    print(f"Time range: Last {args.hours} hours")
    print(f"Limit per wallet: {args.limit}")

    # Initialize Web3
    rpc_url = get_rpc_url()
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        print(f"[!] Failed to connect to RPC: {rpc_url}")
        sys.exit(1)
    print(f"[+] Connected to Ethereum (block {w3.eth.block_number})")

    # Load wallets
    if args.wallet:
        fund_wallets = [args.wallet.lower()]
    else:
        fund_wallets = load_fund_wallets()

    if not fund_wallets:
        print("[!] No fund wallets loaded")
        sys.exit(1)

    print(f"[+] Loaded {len(fund_wallets)} fund wallets")

    # Initialize decoder registry
    from main_app.services.decoders import DecoderRegistry
    registry = DecoderRegistry(w3, fund_wallets, fund_id="drip_capital")
    print(f"[+] Initialized DecoderRegistry with {len(registry._decoder_classes)} decoders")

    # Fetch transactions for all wallets
    print_section("FETCHING TRANSACTIONS")
    all_transactions = []
    seen_hashes = set()

    for i, wallet in enumerate(fund_wallets):
        print(f"[{i+1}/{len(fund_wallets)}] Fetching for {wallet[:10]}...", end=" ")
        txs = fetch_transactions_from_etherscan(wallet, hours=args.hours, limit=args.limit)

        # Deduplicate
        new_txs = [tx for tx in txs if tx['hash'] not in seen_hashes]
        for tx in new_txs:
            seen_hashes.add(tx['hash'])
            tx['wallet'] = wallet

        all_transactions.extend(new_txs)
        print(f"{len(new_txs)} new transactions")
        time.sleep(0.5)  # Rate limit between wallets

    # Sort by timestamp descending
    all_transactions.sort(key=lambda x: x['timestamp'], reverse=True)
    print(f"\n[+] Total unique transactions: {len(all_transactions)}")

    if not all_transactions:
        print("[!] No transactions found in the specified time range")
        sys.exit(0)

    # Decode all transactions
    print_section("DECODING TRANSACTIONS")

    results = []
    platform_counts = {}
    category_counts = {}
    error_count = 0
    spam_count = 0

    for i, tx in enumerate(all_transactions):
        tx_hash = tx['hash']
        tx_time = datetime.fromtimestamp(tx['timestamp'], tz=timezone.utc)

        print(f"[{i+1}/{len(all_transactions)}] {tx_hash[:16]}... ({tx_time.strftime('%m/%d %H:%M')})", end=" ")

        try:
            result = registry.decode_transaction(tx_hash)

            platform = result.platform.value
            category = result.category.value
            status = result.status

            platform_counts[platform] = platform_counts.get(platform, 0) + 1
            category_counts[category] = category_counts.get(category, 0) + 1

            if status == "error":
                error_count += 1
                print(f"ERROR: {result.error[:50] if result.error else 'Unknown'}")
            elif result.is_spam:
                spam_count += 1
                print(f"SPAM ({platform})")
            else:
                je_count = len(result.journal_entries)
                print(f"{platform}/{category} - {je_count} JEs")

            # Collect result data
            results.append({
                'tx_hash': tx_hash,
                'timestamp': tx_time.isoformat(),
                'block': tx['block'],
                'wallet': tx['wallet'][:10] + '...',
                'platform': platform,
                'category': category,
                'status': status,
                'is_spam': result.is_spam,
                'events': len(result.events),
                'journal_entries': len(result.journal_entries),
                'function': result.function_name,
                'eth_price': float(result.eth_price),
                'gas_fee': float(result.gas_fee),
                'error': result.error or ''
            })

        except Exception as e:
            error_count += 1
            print(f"EXCEPTION: {str(e)[:50]}")
            results.append({
                'tx_hash': tx_hash,
                'timestamp': tx_time.isoformat(),
                'block': tx['block'],
                'wallet': tx['wallet'][:10] + '...',
                'platform': 'error',
                'category': 'error',
                'status': 'exception',
                'is_spam': False,
                'events': 0,
                'journal_entries': 0,
                'function': '',
                'eth_price': 0,
                'gas_fee': 0,
                'error': str(e)
            })

    # Print summary
    print_section("SUMMARY")
    print(f"\nTotal Transactions: {len(all_transactions)}")
    print(f"Errors: {error_count}")
    print(f"Spam Filtered: {spam_count}")
    print(f"Successfully Decoded: {len(all_transactions) - error_count}")

    print(f"\nBy Platform:")
    for platform, count in sorted(platform_counts.items(), key=lambda x: -x[1]):
        print(f"  {platform}: {count}")

    print(f"\nBy Category:")
    for category, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        print(f"  {category}: {count}")

    # Journal entry summary
    total_jes = sum(r['journal_entries'] for r in results)
    print(f"\nTotal Journal Entries Generated: {total_jes}")

    # Export to CSV if requested
    if args.output:
        df = pd.DataFrame(results)
        df.to_csv(args.output, index=False)
        print(f"\n[+] Results exported to: {args.output}")

    # Show transactions that need attention (errors or review queue)
    needs_attention = [r for r in results if r['status'] == 'error' or r['journal_entries'] == 0]
    if needs_attention:
        print_section("NEEDS ATTENTION")
        for r in needs_attention[:10]:  # Show first 10
            print(f"  {r['tx_hash'][:16]}... - {r['platform']}/{r['category']} - {r['error'][:30] if r['error'] else 'No JEs'}")
        if len(needs_attention) > 10:
            print(f"  ... and {len(needs_attention) - 10} more")

    print_section("DONE")

    if args.verbose:
        from main_app.logging_config import DEBUG_LOG_PATH
        print(f"\nDebug log: {DEBUG_LOG_PATH}")


if __name__ == "__main__":
    main()
