# Decoder Debugging & JE Verification Workflow

This document describes the process for debugging transaction decoders and verifying journal entry generation accuracy.

## Overview

The decoder system transforms on-chain transactions into double-entry journal entries. This workflow helps identify and fix issues where JEs don't accurately reflect what happened on-chain.

## Debugging Tools

### 1. Single Transaction Debugging: `explore_tx.py`

For deep-diving into a single transaction:

```bash
# Basic decode
python explore_tx.py 0x1234...

# Verbose mode - shows all events, routing decisions, JE generation
python explore_tx.py 0x1234... --verbose

# With debug log file
python explore_tx.py 0x1234... --verbose --logs
```

**Output includes:**
- Transaction metadata (block, timestamp, gas)
- All decoded events with arguments
- Routing decision (which decoder handles it)
- Generated journal entries with accounts and amounts

### 2. Batch Transaction Analysis: `batch_decode.py`

For analyzing all recent transactions across fund wallets:

```bash
# Last 24 hours (default)
python batch_decode.py

# Custom time range
python batch_decode.py --hours 48

# Export to CSV for analysis
python batch_decode.py --output results.csv

# Specific wallet only
python batch_decode.py --wallet 0x1234...

# With verbose logging to file
python batch_decode.py --verbose
```

**Output includes:**
- Summary by platform and category
- Error count and spam count
- Transactions needing attention (errors or no JEs)

### 3. Full JE Detail Logging: `batch_decode_detailed.py`

For complete journal entry verification:

```bash
# Generate full detail log
python batch_decode_detailed.py --hours 24 --output full_decode.log

# Specific wallet
python batch_decode_detailed.py --wallet 0x1234... --output wallet_decode.log
```

**Output includes (per transaction):**
- All events with full arguments
- Each journal entry with:
  - Description
  - Category and platform
  - Wallet and role
  - All debit/credit entries with accounts and amounts
- Balance verification (debits = credits)
- Summary statistics

### 4. Debug Log File

Enable verbose file logging for decoder internals:

```bash
# Set environment variable
set DECODER_DEBUG=1  # Windows
export DECODER_DEBUG=1  # Linux/Mac

# Or in Python
import os
os.environ['DECODER_DEBUG'] = '1'
from main_app.logging_config import setup_decoder_debug_logging
setup_decoder_debug_logging()
```

Log file location: `decoder_debug.log` in project root

**Log contents:**
- Contract routing decisions
- Proxy resolution attempts
- Event processing details
- JE generation logic

## Debugging Process

### Step 1: Run Batch Analysis

```bash
python batch_decode_detailed.py --hours 24 --output full_decode.log
```

### Step 2: Review Log for Issues

Look for these patterns in the log:

1. **Unknown tokens with 0 amounts:**
   ```
   DR 100.XX - Token Wallet: 0.000000 UNKNOWN
   ```
   Fix: Add token to `KNOWN_TOKENS` in `generic_decoder.py` or skip unknown tokens

2. **Misclassified vault interactions:**
   ```
   CR 400.20 - Other Income: 8500.000000 USDC
   ```
   Should be: `CR 120.10 - DeFi Vault Investments`
   Fix: Add contract to `DEFI_VAULT_CONTRACTS` in `generic_decoder.py`

3. **Dust/zero-value transactions creating JEs:**
   ```
   [JE 1] ETH received: 0.000000
   ```
   Fix: Adjust `MIN_AMOUNT_ETH` or `MIN_AMOUNT_USDC` thresholds

4. **Wrong category classification:**
   ```
   Category: other
   ```
   Should be: loan_origination, repayment, etc.
   Fix: Update platform-specific decoder logic

5. **Unbalanced entries:**
   ```
   Balanced: NO - MISMATCH!
   ```
   Fix: Debug the specific decoder's JE generation

### Step 3: Deep-Dive Specific Transactions

For transactions with issues:

```bash
python explore_tx.py 0xproblem_tx_hash --verbose
```

Compare:
- Events that fired vs JEs generated
- Amounts in events vs amounts in JEs
- Accounts used vs expected accounts

### Step 4: Implement Fixes

Common fix locations:

| Issue | File | Section |
|-------|------|---------|
| Unknown tokens | `generic_decoder.py` | `KNOWN_TOKENS` dict |
| Vault detection | `generic_decoder.py` | `DEFI_VAULT_CONTRACTS` dict |
| Amount thresholds | `generic_decoder.py` | `MIN_AMOUNT_*` constants |
| Platform routing | `registry.py` | `CONTRACT_ROUTING` dict |
| Platform-specific JEs | `{platform}_decoder.py` | `_create_journal_entries()` |

### Step 5: Verify Fixes

Re-run batch analysis and compare:

```bash
python batch_decode_detailed.py --hours 24 --output full_decode_fixed.log
```

Check:
- Total JE count (should decrease if filtering spam)
- Correct account classifications
- All entries balanced

## Common Issues & Fixes

### Issue: Unknown Token Spam

**Symptom:** JEs with 0.000000 amounts for random tokens

**Cause:** Token not in `KNOWN_TOKENS`, decimals unknown

**Fix in `generic_decoder.py`:**
```python
KNOWN_TOKENS = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {"symbol": "WETH", "decimals": 18},
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"symbol": "USDC", "decimals": 6},
    # Add new tokens here
}

# In _decode_erc20():
if evt_token_addr not in KNOWN_TOKENS:
    continue  # Skip unknown tokens
```

### Issue: Vault Interactions as Income/Expense

**Symptom:** Deposits/withdrawals to DeFi vaults showing as Other Income/Expense

**Cause:** Contract not recognized as a vault

**Fix in `generic_decoder.py`:**
```python
DEFI_VAULT_CONTRACTS = {
    "0x98c23e9d8f34fefb1b7bd6a91b7ff122f4e16f5c": "Morpho USDC Vault",
    # Add new vaults here
}

# In _decode_erc20():
is_vault = any(cp in DEFI_VAULT_CONTRACTS for cp in counterparties)
if is_vault:
    account = "120.10 - DeFi Vault Investments"
```

### Issue: Dust Transactions Creating JEs

**Symptom:** Many JEs for tiny amounts (gas refunds, rounding dust)

**Fix in `generic_decoder.py`:**
```python
MIN_AMOUNT_ETH = Decimal("0.00001")  # ~$0.03 at $3k ETH
MIN_AMOUNT_USDC = Decimal("0.01")    # 1 cent

# In decode methods:
if amount < MIN_AMOUNT_ETH:
    continue  # Skip dust
```

### Issue: Wrong Platform Detection

**Symptom:** Transaction decoded by wrong decoder

**Fix in `registry.py`:**
```python
CONTRACT_ROUTING = {
    "0xcontract_address": Platform.CORRECT_PLATFORM,
}
```

## File Reference

| File | Purpose |
|------|---------|
| `explore_tx.py` | Single transaction deep-dive |
| `batch_decode.py` | Batch analysis with summary |
| `batch_decode_detailed.py` | Full JE detail logging |
| `main_app/logging_config.py` | Debug logging setup |
| `main_app/services/decoders/registry.py` | Transaction routing |
| `main_app/services/decoders/generic_decoder.py` | WETH/ERC20/ETH transfers |
| `main_app/services/decoders/{platform}_decoder.py` | Platform-specific decoders |

## Metrics to Track

After each debugging session, record:

1. **Total transactions processed**
2. **JE count** (should reflect real economic activity)
3. **Error rate** (target: <1%)
4. **Spam filter rate** (known spam tokens filtered)
5. **Platform coverage** (% decoded by specific vs generic decoder)

## Example Debugging Session

```bash
# 1. Run batch analysis
python batch_decode_detailed.py --hours 24 --output decode_audit.log

# 2. Check summary at end of log
tail -50 decode_audit.log

# 3. Search for issues
grep "0.000000" decode_audit.log  # Unknown tokens
grep "Other Income" decode_audit.log  # Misclassified
grep "MISMATCH" decode_audit.log  # Unbalanced

# 4. Deep-dive problem transaction
python explore_tx.py 0xproblem_hash --verbose

# 5. Make fixes to decoder

# 6. Re-run and verify
python batch_decode_detailed.py --hours 24 --output decode_audit_fixed.log

# 7. Compare results
# Before: 115 JEs, 10 unknown tokens, 5 misclassified vaults
# After: 105 JEs, 0 unknown tokens, 0 misclassified vaults
```
