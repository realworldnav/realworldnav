"""
Transaction Explorer - Debug tool using the exact same decoder engine as app.py

Usage:
    python explore_tx.py <tx_hash>
    python explore_tx.py 0xd50691eaa0a1783676d3a59de8db06b8ceca0b07acaa44c35d1544efce026ff2
    python explore_tx.py <tx_hash> --raw       # Show raw event data
    python explore_tx.py <tx_hash> --skip-spam # Skip spam detection
    python explore_tx.py <tx_hash> --verbose   # Enable debug logging (write to decoder_debug.log)
    python explore_tx.py <tx_hash> -v          # Short for --verbose

Debug Mode:
    Set DECODER_DEBUG=1 environment variable to enable verbose logging to decoder_debug.log
    Or use --verbose flag to enable debug mode for this run only

Output:
    Normal mode: Formatted summary of decoded transaction
    Verbose mode: Also writes detailed routing/decoding steps to decoder_debug.log
"""

import os
import sys
import json
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional

# Load environment
from dotenv import load_dotenv
load_dotenv()

from web3 import Web3


def get_rpc_url() -> str:
    """Get RPC URL from environment (same logic as app.py)"""
    return os.getenv("WEB3_HTTP_URL", os.getenv("MAINNET_RPC_URL", "https://eth.llamarpc.com"))


def load_fund_wallets() -> List[str]:
    """Load fund wallet addresses from S3 (same as app.py)"""
    try:
        from main_app.s3_utils import load_WALLET_file
        wallet_df = load_WALLET_file()
        if wallet_df is not None and not wallet_df.empty:
            addresses = wallet_df['wallet_address'].dropna().tolist()
            return [addr.lower() for addr in addresses if addr]
    except Exception as e:
        print(f"[!] Could not load wallets from S3: {e}")

    # Fallback to placeholder
    return ["0x1234567890123456789012345678901234567890"]


def format_eth(value: Decimal) -> str:
    """Format ETH value nicely"""
    if value == 0:
        return "0 ETH"
    return f"{float(value):.6f} ETH"


def format_usd(value: Decimal) -> str:
    """Format USD value nicely"""
    return f"${float(value):,.2f}"


def print_section(title: str, char: str = "="):
    """Print a section header"""
    print(f"\n{char * 60}")
    print(f" {title}")
    print(f"{char * 60}")


def explore_transaction(tx_hash: str, show_raw: bool = False, skip_spam: bool = False, verbose: bool = False):
    """
    Decode and explore a transaction using the exact same engine as app.py

    Args:
        tx_hash: Transaction hash to decode
        show_raw: Show raw data in output
        skip_spam: Skip spam detection
        verbose: Enable verbose debug logging to file
    """
    # If verbose mode, enable debug logging
    if verbose:
        os.environ['DECODER_DEBUG'] = '1'
        from main_app.logging_config import setup_decoder_debug_logging, DEBUG_LOG_PATH
        setup_decoder_debug_logging()
        print(f"[DEBUG] Verbose mode enabled - detailed logs will be written to:")
        print(f"        {DEBUG_LOG_PATH}")
        print(f"        Use: tail -f decoder_debug.log (or Read tool) to view")
        print()

    # Initialize Web3
    rpc_url = get_rpc_url()
    w3 = Web3(Web3.HTTPProvider(rpc_url))

    if not w3.is_connected():
        print(f"[!] Failed to connect to RPC: {rpc_url}")
        sys.exit(1)

    print(f"[+] Connected to Ethereum (block {w3.eth.block_number})")

    # Load wallets (same as app.py)
    fund_wallets = load_fund_wallets()
    print(f"[+] Loaded {len(fund_wallets)} fund wallets")
    if verbose:
        for i, w in enumerate(fund_wallets[:5]):
            print(f"    [{i}] {w}")
        if len(fund_wallets) > 5:
            print(f"    ... and {len(fund_wallets) - 5} more")

    # Initialize DecoderRegistry (exact same as app.py)
    from main_app.services.decoders import DecoderRegistry
    registry = DecoderRegistry(w3, fund_wallets, fund_id="drip_capital")
    print(f"[+] Initialized DecoderRegistry with {len(registry._decoder_classes)} decoders")
    if verbose:
        print(f"    Loaded: {list(registry._decoder_classes.keys())}")

    # Fetch raw transaction first for context
    print_section(f"TRANSACTION: {tx_hash[:16]}...")

    try:
        tx = w3.eth.get_transaction(tx_hash)
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        block = w3.eth.get_block(tx.blockNumber)
    except Exception as e:
        print(f"[!] Failed to fetch transaction: {e}")
        sys.exit(1)

    # Basic tx info
    timestamp = datetime.utcfromtimestamp(block.timestamp)
    print(f"\nBlock:      {tx.blockNumber}")
    print(f"Timestamp:  {timestamp.isoformat()} UTC")
    print(f"From:       {tx['from']}")
    print(f"To:         {tx.get('to', 'Contract Creation')}")
    print(f"Value:      {format_eth(Decimal(tx.value) / Decimal(10**18))}")
    print(f"Gas Used:   {receipt.gasUsed:,}")
    print(f"Status:     {'Success' if receipt.status == 1 else 'Failed'}")
    print(f"Logs:       {len(receipt.logs)}")

    # Decode using registry (exact same as app.py)
    print_section("DECODING TRANSACTION")

    result = registry.decode_transaction(tx_hash, skip_spam_check=skip_spam)

    # Show result summary
    print(f"\nStatus:          {result.status}")
    print(f"Platform:        {result.platform.value}")
    print(f"Category:        {result.category.value}")
    print(f"Posting Status:  {result.posting_status.value}")
    print(f"Is Spam:         {result.is_spam}")
    print(f"Function:        {result.function_name}")
    print(f"ETH Price:       {format_usd(result.eth_price)}")
    print(f"Gas Fee:         {format_eth(result.gas_fee)}")

    if result.error:
        print(f"Error:           {result.error}")

    # Spam detection details
    if result.is_spam:
        print_section("SPAM DETECTION DETAILS", "-")
        spam_data = result.raw_data.get('spam_detection', {})
        print(f"Confidence:  {spam_data.get('confidence', 0):.0%}")
        print(f"Reasons:     {spam_data.get('reasons', '')}")
        print(f"Events:      {spam_data.get('num_events', 0)}")
        details = spam_data.get('details', {})
        if details:
            print(f"\nDetails:")
            for k, v in details.items():
                if k != 'num_events':  # Already shown above
                    print(f"  {k}: {v}")

    # Decoded events
    if result.events:
        print_section(f"DECODED EVENTS ({len(result.events)})", "-")
        for i, event in enumerate(result.events):
            print(f"\n[Event {i+1}] {event.name}")
            print(f"  Contract: {event.contract_address[:20]}..." if event.contract_address else "  Contract: N/A")
            print(f"  Log Index: {event.log_index}")
            # Show event args
            for key, value in (event.args or {}).items():
                if isinstance(value, (int, float, Decimal)):
                    if value > 10**15:  # Likely wei
                        print(f"  {key}: {value} ({format_eth(Decimal(value) / Decimal(10**18))})")
                    else:
                        print(f"  {key}: {value}")
                else:
                    val_str = str(value)
                    if len(val_str) > 66:
                        val_str = val_str[:66] + "..."
                    print(f"  {key}: {val_str}")

    # Journal entries
    if result.journal_entries:
        print_section(f"JOURNAL ENTRIES ({len(result.journal_entries)})", "-")

        total_debits = Decimal(0)
        total_credits = Decimal(0)

        for i, je in enumerate(result.journal_entries):
            print(f"\n[Entry {i+1}] {je.description[:60]}..." if len(je.description) > 60 else f"\n[Entry {i+1}] {je.description}")
            print(f"    Date:     {je.date.strftime('%Y-%m-%d %H:%M')}")
            print(f"    Category: {je.category.value}")
            print(f"    Platform: {je.platform.value}")
            print(f"    Wallet:   {je.wallet_address[:20]}..." if je.wallet_address else "    Wallet: N/A")
            print(f"    Role:     {je.wallet_role}")

            # Print individual entries (debits and credits)
            for entry in je.entries:
                entry_type = entry.get('type', 'UNKNOWN')
                account = entry.get('account', 'Unknown Account')
                amount = Decimal(str(entry.get('amount', 0)))
                asset = entry.get('asset', 'ETH')

                if entry_type == 'DEBIT':
                    total_debits += amount
                    print(f"      DR {account}: {format_eth(amount)} {asset}")
                else:
                    total_credits += amount
                    print(f"      CR {account}: {format_eth(amount)} {asset}")

        print(f"\n{'-' * 40}")
        print(f"Total Debits:  {format_eth(total_debits)}")
        print(f"Total Credits: {format_eth(total_credits)}")
        print(f"Balanced:      {'YES' if abs(total_debits - total_credits) < Decimal('0.000001') else 'NO - MISMATCH!'}")
    else:
        print_section("JOURNAL ENTRIES", "-")
        print("No journal entries generated")
        if result.is_spam:
            print("(Transaction filtered as spam - no entries created)")

    # Wallet roles
    if result.wallet_roles:
        print_section("WALLET ROLES", "-")
        for wallet, role in result.wallet_roles.items():
            is_fund = wallet.lower() in [w.lower() for w in fund_wallets]
            marker = " [FUND]" if is_fund else ""
            print(f"  {wallet[:20]}... -> {role}{marker}")

    # Loan positions
    if result.positions:
        print_section(f"LOAN POSITIONS ({len(result.positions)})", "-")
        for loan_id, position in result.positions.items():
            print(f"\n[Loan {loan_id}]")
            print(f"  Principal:    {format_eth(position.principal)}")
            print(f"  Rate (bps):   {position.rate}")
            print(f"  Start:        {position.start_time}")
            print(f"  Duration:     {position.duration}s ({position.duration / 86400:.1f} days)")
            print(f"  Platform:     {position.platform.value}")
            print(f"  Status:       {position.status}")

    # Raw data (if requested)
    if show_raw and result.raw_data:
        print_section("RAW DATA", "-")
        # Filter out spam_detection (already shown)
        raw_filtered = {k: v for k, v in result.raw_data.items() if k != 'spam_detection'}
        if raw_filtered:
            print(json.dumps(raw_filtered, default=str, indent=2)[:2000])
            if len(json.dumps(raw_filtered, default=str)) > 2000:
                print("\n... (truncated)")

    print_section("DONE")

    return result


def show_raw_logs(w3: Web3, tx_hash: str):
    """Show raw transaction logs for debugging"""
    receipt = w3.eth.get_transaction_receipt(tx_hash)
    print_section("RAW LOGS ANALYSIS")

    # Common event signatures
    EVENT_SIGNATURES = {
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": "Transfer(address,address,uint256)",
        "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925": "Approval(address,address,uint256)",
        "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c": "Deposit(address,uint256)",
        "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65": "Withdrawal(address,uint256)",
        "0x2ecd071e4d10ed2221b04636ed0724cce66a873aa98c1a31b4bb0e6846d3aab4": "LoanRepaid(uint256,address,uint256,uint256)",
        "0x9c3c9d30a9e9d4e7c1bf1c18c75b82d3e3b4a77b8a9c1d2e3f4a5b6c7d8e9f0a": "LoanStarted(uint256)",
    }

    for i, log in enumerate(receipt.logs):
        print(f"\n[Log {i}]")
        print(f"  Address: {log['address']}")

        # Identify known contracts
        addr_lower = log['address'].lower()
        known_contracts = {
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH",
            "0x29469395eaf6f95920e59f858042f0e28d98a20b": "Blur Blend",
            "0x0000000000a39bb272e79075ade125fd351887ac": "Blur Pool",
            "0x4bc5fa56f2931e7a37417fa55dda71e4b7c2f2a3": "NFTfi Refinancing",
        }
        if addr_lower in known_contracts:
            print(f"           ({known_contracts[addr_lower]})")

        # Show topics
        if log['topics']:
            topic0 = log['topics'][0].hex() if isinstance(log['topics'][0], bytes) else log['topics'][0]
            event_name = EVENT_SIGNATURES.get(topic0, "Unknown")
            print(f"  Topic[0]: {topic0[:18]}... ({event_name})")
            for j, topic in enumerate(log['topics'][1:], 1):
                topic_hex = topic.hex() if isinstance(topic, bytes) else topic
                # Try to extract address from topic
                if len(topic_hex) >= 66:
                    addr = "0x" + topic_hex[-40:]
                    print(f"  Topic[{j}]: {topic_hex[:18]}... (addr: {addr[:10]}...)")
                else:
                    print(f"  Topic[{j}]: {topic_hex[:18]}...")

        # Show data
        data = log['data'].hex() if isinstance(log['data'], bytes) else log['data']
        if data and data != '0x':
            print(f"  Data: {data[:66]}..." if len(data) > 66 else f"  Data: {data}")
            # If it looks like a uint256, decode it
            if len(data) == 66:  # 0x + 64 hex chars = 32 bytes
                value = int(data, 16)
                if value > 10**15:  # Likely wei
                    eth_value = Decimal(value) / Decimal(10**18)
                    print(f"        (decoded: {eth_value:.6f} ETH)")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    tx_hash = sys.argv[1]

    # Parse flags
    show_raw = "--raw" in sys.argv
    skip_spam = "--skip-spam" in sys.argv
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    show_logs = "--logs" in sys.argv

    # Validate tx hash format
    if not tx_hash.startswith("0x") or len(tx_hash) != 66:
        print(f"[!] Invalid transaction hash: {tx_hash}")
        print("    Expected format: 0x followed by 64 hex characters")
        sys.exit(1)

    # Show raw logs if requested (doesn't need full decode)
    if show_logs:
        rpc_url = get_rpc_url()
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        show_raw_logs(w3, tx_hash)
        return

    explore_transaction(tx_hash, show_raw=show_raw, skip_spam=skip_spam, verbose=verbose)

    # If verbose mode was used, remind about the log file
    if verbose:
        from main_app.logging_config import DEBUG_LOG_PATH
        print()
        print_section("DEBUG LOG LOCATION")
        print(f"Detailed decode trace written to: {DEBUG_LOG_PATH}")
        print(f"View with: type {DEBUG_LOG_PATH}")  # Windows
        print(f"Or in WSL: tail -f decoder_debug.log")


if __name__ == "__main__":
    main()
