"""
Batch Decoder with Full Journal Entry Details - For JE verification

Outputs complete journal entry details for each transaction to a log file.

Usage:
    python batch_decode_detailed.py --hours 24 --output full_decode.log
"""

import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List
import argparse

from dotenv import load_dotenv
load_dotenv()

from web3 import Web3


def get_rpc_url() -> str:
    return os.getenv("WEB3_HTTP_URL", os.getenv("MAINNET_RPC_URL", "https://eth.llamarpc.com"))


def load_fund_wallets() -> List[str]:
    try:
        from main_app.s3_utils import load_WALLET_file
        wallet_df = load_WALLET_file()
        if wallet_df is not None and not wallet_df.empty:
            addresses = wallet_df['wallet_address'].dropna().tolist()
            return [addr.lower().strip() for addr in addresses if addr]
    except Exception as e:
        print(f"[!] Could not load wallets from S3: {e}")
    return []


def fetch_transactions_from_etherscan(wallet: str, hours: int = 24, limit: int = 100) -> List[Dict]:
    """Fetch transactions from Etherscan API"""
    api_key = os.getenv('ETHERSCAN_API_KEY', '')
    if not api_key:
        return []

    cutoff_time = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())
    transactions = []
    url = "https://api.etherscan.io/v2/api"

    # Fetch normal transactions
    params = {
        'chainid': 1, 'module': 'account', 'action': 'txlist',
        'address': wallet, 'startblock': 0, 'endblock': 99999999,
        'page': 1, 'offset': limit, 'sort': 'desc', 'apikey': api_key
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
                            'block': int(tx.get('blockNumber', 0)),
                        })
    except Exception as e:
        print(f"[!] Error fetching txs: {e}")

    time.sleep(0.25)

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
                        if not any(t['hash'] == tx.get('hash') for t in transactions):
                            transactions.append({
                                'hash': tx.get('hash'),
                                'timestamp': tx_time,
                                'from': tx.get('from', '').lower(),
                                'to': tx.get('to', '').lower(),
                                'block': int(tx.get('blockNumber', 0)),
                            })
    except Exception as e:
        print(f"[!] Error fetching token txs: {e}")

    return transactions


def format_eth(value) -> str:
    """Format ETH value"""
    try:
        return f"{float(value):.6f}"
    except:
        return str(value)


def write_transaction_detail(f, tx_hash: str, result, tx_time: datetime):
    """Write full transaction details to file"""
    f.write(f"\n{'='*80}\n")
    f.write(f"TX: {tx_hash}\n")
    f.write(f"{'='*80}\n")
    f.write(f"Timestamp:  {tx_time.strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
    f.write(f"Block:      {result.block}\n")
    f.write(f"Platform:   {result.platform.value}\n")
    f.write(f"Category:   {result.category.value}\n")
    f.write(f"Status:     {result.status}\n")
    f.write(f"Function:   {result.function_name}\n")
    f.write(f"ETH Price:  ${float(result.eth_price):,.2f}\n")
    f.write(f"Gas Fee:    {format_eth(result.gas_fee)} ETH\n")
    f.write(f"Is Spam:    {result.is_spam}\n")

    if result.error:
        f.write(f"ERROR:      {result.error}\n")

    # Events
    f.write(f"\n--- EVENTS ({len(result.events)}) ---\n")
    for i, event in enumerate(result.events):
        f.write(f"\n[Event {i+1}] {event.name}\n")
        f.write(f"  Contract: {event.contract_address}\n")
        if event.args:
            for key, value in event.args.items():
                # Skip None values and internal fields
                if value is None or key.startswith('_'):
                    continue
                # Format large numbers as ETH
                if isinstance(value, (int, float)) and value > 10**15:
                    f.write(f"  {key}: {value} ({format_eth(Decimal(value) / Decimal(10**18))} ETH)\n")
                else:
                    val_str = str(value)[:80]
                    f.write(f"  {key}: {val_str}\n")

    # Journal Entries - THE KEY PART
    f.write(f"\n--- JOURNAL ENTRIES ({len(result.journal_entries)}) ---\n")

    if not result.journal_entries:
        if result.is_spam:
            f.write("  (No entries - transaction filtered as spam)\n")
        else:
            f.write("  (No entries generated)\n")
    else:
        total_debits = Decimal(0)
        total_credits = Decimal(0)

        for i, je in enumerate(result.journal_entries):
            f.write(f"\n[JE {i+1}] {je.description}\n")
            f.write(f"  Date:     {je.date.strftime('%Y-%m-%d %H:%M')}\n")
            f.write(f"  Category: {je.category.value}\n")
            f.write(f"  Platform: {je.platform.value}\n")
            f.write(f"  Wallet:   {je.wallet_address}\n")
            f.write(f"  Role:     {je.wallet_role}\n")
            f.write(f"  Entries:\n")

            for entry in je.entries:
                entry_type = entry.get('type', 'UNKNOWN')
                account = entry.get('account', 'Unknown')
                amount = Decimal(str(entry.get('amount', 0)))
                asset = entry.get('asset', 'ETH')

                if entry_type == 'DEBIT':
                    total_debits += amount
                    f.write(f"    DR {account}: {format_eth(amount)} {asset}\n")
                else:
                    total_credits += amount
                    f.write(f"    CR {account}: {format_eth(amount)} {asset}\n")

        f.write(f"\n  TOTALS:\n")
        f.write(f"    Debits:  {format_eth(total_debits)} ETH\n")
        f.write(f"    Credits: {format_eth(total_credits)} ETH\n")
        balanced = abs(total_debits - total_credits) < Decimal('0.000001')
        f.write(f"    Balanced: {'YES' if balanced else 'NO - MISMATCH!'}\n")

    # Wallet Roles
    if result.wallet_roles:
        f.write(f"\n--- WALLET ROLES ---\n")
        for wallet, role in result.wallet_roles.items():
            f.write(f"  {wallet}: {role}\n")

    f.write(f"\n")


def main():
    parser = argparse.ArgumentParser(description='Batch decode with full JE details')
    parser.add_argument('--hours', type=int, default=24, help='Hours to look back')
    parser.add_argument('--limit', type=int, default=100, help='Max transactions per wallet')
    parser.add_argument('--output', '-o', type=str, default='full_decode.log', help='Output log file')
    parser.add_argument('--wallet', type=str, help='Specific wallet to check')
    args = parser.parse_args()

    print(f"[+] Batch Decoder - Full JE Details")
    print(f"[+] Time range: Last {args.hours} hours")
    print(f"[+] Output: {args.output}")

    # Initialize Web3
    w3 = Web3(Web3.HTTPProvider(get_rpc_url()))
    if not w3.is_connected():
        print("[!] Failed to connect to RPC")
        sys.exit(1)
    print(f"[+] Connected to Ethereum (block {w3.eth.block_number})")

    # Load wallets
    fund_wallets = [args.wallet.lower()] if args.wallet else load_fund_wallets()
    if not fund_wallets:
        print("[!] No wallets loaded")
        sys.exit(1)
    print(f"[+] Loaded {len(fund_wallets)} fund wallets")

    # Initialize decoder
    from main_app.services.decoders import DecoderRegistry
    registry = DecoderRegistry(w3, fund_wallets, fund_id="drip_capital")
    print(f"[+] Initialized DecoderRegistry")

    # Fetch transactions
    print(f"[+] Fetching transactions...")
    all_transactions = []
    seen_hashes = set()

    for i, wallet in enumerate(fund_wallets):
        print(f"  [{i+1}/{len(fund_wallets)}] {wallet[:10]}...", end=" ", flush=True)
        txs = fetch_transactions_from_etherscan(wallet, hours=args.hours, limit=args.limit)
        new_txs = [tx for tx in txs if tx['hash'] not in seen_hashes]
        for tx in new_txs:
            seen_hashes.add(tx['hash'])
            tx['wallet'] = wallet
        all_transactions.extend(new_txs)
        print(f"{len(new_txs)} txs")
        time.sleep(0.5)

    all_transactions.sort(key=lambda x: x['timestamp'], reverse=True)
    print(f"\n[+] Total unique transactions: {len(all_transactions)}")

    if not all_transactions:
        print("[!] No transactions found")
        sys.exit(0)

    # Decode and write to log file
    print(f"[+] Decoding and writing to {args.output}...")

    stats = {
        'total': 0, 'success': 0, 'error': 0, 'spam': 0,
        'platforms': {}, 'categories': {}, 'total_jes': 0
    }

    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(f"BATCH DECODE REPORT\n")
        f.write(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        f.write(f"Time Range: Last {args.hours} hours\n")
        f.write(f"Total Transactions: {len(all_transactions)}\n")
        f.write(f"Fund Wallets: {len(fund_wallets)}\n")

        for i, tx in enumerate(all_transactions):
            tx_hash = tx['hash']
            tx_time = datetime.fromtimestamp(tx['timestamp'], tz=timezone.utc)

            print(f"  [{i+1}/{len(all_transactions)}] {tx_hash[:16]}...", end=" ", flush=True)
            stats['total'] += 1

            try:
                result = registry.decode_transaction(tx_hash)

                # Update stats
                platform = result.platform.value
                category = result.category.value
                stats['platforms'][platform] = stats['platforms'].get(platform, 0) + 1
                stats['categories'][category] = stats['categories'].get(category, 0) + 1
                stats['total_jes'] += len(result.journal_entries)

                if result.status == 'error':
                    stats['error'] += 1
                    print(f"ERROR")
                elif result.is_spam:
                    stats['spam'] += 1
                    print(f"SPAM")
                else:
                    stats['success'] += 1
                    print(f"{platform}/{category} ({len(result.journal_entries)} JEs)")

                # Write full details
                write_transaction_detail(f, tx_hash, result, tx_time)

            except Exception as e:
                stats['error'] += 1
                print(f"EXCEPTION: {e}")
                f.write(f"\n{'='*80}\n")
                f.write(f"TX: {tx_hash}\n")
                f.write(f"EXCEPTION: {str(e)}\n")

        # Write summary at end
        f.write(f"\n\n{'='*80}\n")
        f.write(f"SUMMARY\n")
        f.write(f"{'='*80}\n")
        f.write(f"Total Transactions: {stats['total']}\n")
        f.write(f"Successfully Decoded: {stats['success']}\n")
        f.write(f"Errors: {stats['error']}\n")
        f.write(f"Spam Filtered: {stats['spam']}\n")
        f.write(f"Total Journal Entries: {stats['total_jes']}\n")
        f.write(f"\nBy Platform:\n")
        for p, c in sorted(stats['platforms'].items(), key=lambda x: -x[1]):
            f.write(f"  {p}: {c}\n")
        f.write(f"\nBy Category:\n")
        for cat, c in sorted(stats['categories'].items(), key=lambda x: -x[1]):
            f.write(f"  {cat}: {c}\n")

    print(f"\n[+] Done! Full details written to: {args.output}")
    print(f"[+] Summary: {stats['success']} decoded, {stats['spam']} spam, {stats['error']} errors")
    print(f"[+] Total JEs: {stats['total_jes']}")


if __name__ == "__main__":
    main()
