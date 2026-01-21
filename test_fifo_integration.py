# -*- coding: utf-8 -*-
"""
Test FIFO Integration with Decoder Registry

This script demonstrates the full integration of:
1. DecoderRegistry - decodes blockchain transactions
2. DecoderFIFOIntegrator - tracks cost basis via deemed_cash entries
3. CostBasisTracker - manages tax lots and gain/loss calculations

Usage:
    python test_fifo_integration.py
"""

import os
from dotenv import load_dotenv
load_dotenv()

from decimal import Decimal
from datetime import datetime, timedelta
from web3 import Web3

def test_integration():
    """Test the full decoder -> FIFO integration"""

    # Initialize Web3
    rpc_url = os.getenv("WEB3_HTTP_URL", os.getenv("MAINNET_RPC_URL", "https://eth.llamarpc.com"))
    w3 = Web3(Web3.HTTPProvider(rpc_url))

    if not w3.is_connected():
        print(f"[!] Failed to connect to RPC: {rpc_url}")
        return

    print(f"[+] Connected to Ethereum (block {w3.eth.block_number})")

    # Load fund wallets
    try:
        from main_app.s3_utils import load_WALLET_file
        wallet_df = load_WALLET_file()
        if wallet_df is not None and not wallet_df.empty:
            fund_wallets = wallet_df['wallet_address'].dropna().tolist()
            fund_wallets = [addr.lower() for addr in fund_wallets if addr]
        else:
            fund_wallets = ["0x1234567890123456789012345678901234567890"]
    except Exception as e:
        print(f"[!] Could not load wallets: {e}")
        fund_wallets = ["0x1234567890123456789012345678901234567890"]

    print(f"[+] Loaded {len(fund_wallets)} fund wallets")

    # Create FIFO integrator
    from main_app.services.fifo_tracker import CostBasisTracker, CostBasisMethod
    from main_app.services.decoder_fifo_integrator import DecoderFIFOIntegrator

    tracker = CostBasisTracker(method=CostBasisMethod.FIFO, fund_id="drip_capital")
    integrator = DecoderFIFOIntegrator(tracker=tracker, fund_id="drip_capital")

    print("[+] Created FIFO integrator")

    # Create registry with FIFO integration
    from main_app.services.decoders import DecoderRegistry

    registry = DecoderRegistry(
        w3=w3,
        fund_wallets=fund_wallets,
        fund_id="drip_capital",
        fifo_integrator=integrator
    )

    print(f"[+] Created registry with {len(registry._decoder_classes)} decoders")
    print(f"[+] FIFO enabled: {registry.stats['fifo_enabled']}")

    # Simulate a prior acquisition to have cost basis for testing
    print("\n" + "="*60)
    print(" STEP 1: Simulate Prior Acquisitions")
    print("="*60)

    # Add some ETH (simulating prior ETH purchase 60 days ago)
    prior_date = datetime.now() - timedelta(days=60)
    lot = tracker.add_acquisition(
        asset="ETH",
        amount=Decimal("50"),
        cost_usd=Decimal("150000"),  # 50 ETH @ $3000
        date=prior_date,
        tx_hash="0x_prior_eth_purchase",
        wallet_id=fund_wallets[0] if fund_wallets else "0xtest",
        fund_id="drip_capital"
    )
    print(f"  Added: {lot.amount} ETH @ ${lot.cost_per_unit}/unit = ${lot.cost_basis_usd}")

    # Test with a Gondi lending transaction
    print("\n" + "="*60)
    print(" STEP 2: Decode Gondi Lending Transaction")
    print("="*60)

    # Recent Gondi v3 transaction
    test_tx = "0xfee0bbd4832ac9e644f94cdc3ebc654f6174cafdb5f8cb213cb7e92e4054f103"
    print(f"  TX: {test_tx[:20]}...")

    result = registry.decode_transaction(test_tx, skip_spam_check=True)

    print(f"\n  Status:     {result.status}")
    print(f"  Platform:   {result.platform.value}")
    print(f"  Category:   {result.category.value}")
    print(f"  ETH Price:  ${result.eth_price:,.2f}")

    # Check FIFO tracking data
    fifo_data = result.raw_data.get('fifo_tracking', {})
    if fifo_data:
        print(f"\n  FIFO Tracking:")
        print(f"    Acquisitions: {fifo_data.get('acquisitions', 0)}")
        print(f"    Disposals:    {fifo_data.get('disposals', 0)}")
        print(f"    Gain/Loss:    ${fifo_data.get('total_gain_loss_usd', 0):,.2f}")
        if fifo_data.get('errors'):
            print(f"    Errors:       {fifo_data['errors']}")
    else:
        print("\n  [No FIFO tracking data - transaction may not have deemed_cash entries]")

    # Show enriched entries if available
    enriched = result.raw_data.get('fifo_enriched_entries', [])
    if enriched:
        print(f"\n  Enriched Entries ({len(enriched)}):")
        for entry in enriched[:3]:  # Show first 3
            fifo_action = entry.get('fifo_action', 'N/A')
            if fifo_action != 'N/A':
                print(f"    - {entry.get('description', 'No description')[:50]}...")
                print(f"      Action: {fifo_action}")
                if 'fifo_gain_loss_usd' in entry:
                    print(f"      Gain/Loss: ${entry['fifo_gain_loss_usd']:,.2f}")

    # Show journal entries if available
    if result.journal_entries:
        print(f"\n  Journal Entries ({len(result.journal_entries)}):")
        for i, je in enumerate(result.journal_entries[:3]):
            desc = je.description[:50] + "..." if len(je.description) > 50 else je.description
            print(f"    [{i+1}] {desc}")
            # Show entries (debits/credits)
            for entry in je.entries[:4]:
                entry_type = entry.get('type', 'UNK')
                account = entry.get('account', 'unknown')[:30]
                amount = entry.get('amount', 0)
                asset = entry.get('asset', 'ETH')
                prefix = "DR" if entry_type == 'DEBIT' else "CR"
                print(f"        {prefix} {account}: {amount:.6f} {asset}")
    else:
        print("\n  No journal entries generated")

    # Show current positions
    print("\n" + "="*60)
    print(" STEP 3: Current FIFO Positions (after swap)")
    print("="*60)

    positions = registry.get_fifo_positions()
    if positions and positions.get('positions'):
        for pos in positions['positions']:
            print(f"  {pos.get('asset', 'Unknown')}: "
                  f"{pos.get('amount', 0):.6f} units, "
                  f"cost basis: ${pos.get('cost_basis_usd', 0):,.2f}")
        print(f"\n  Total Cost Basis: ${positions.get('total_cost_basis_usd', 0):,.2f}")
    else:
        print("  No positions tracked")

    # Show registry stats
    print("\n" + "="*60)
    print(" Registry Stats")
    print("="*60)
    stats = registry.stats
    for key, value in stats.items():
        print(f"  {key}: {value}")

    print("\n" + "="*60)
    print(" DONE")
    print("="*60)


if __name__ == "__main__":
    test_integration()
