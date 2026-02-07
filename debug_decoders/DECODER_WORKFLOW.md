# Transaction Decoder Development Workflow

## Overview

This document describes the workflow for building and testing transaction decoders for NFT lending platforms. Each decoder extracts blockchain events and generates double-entry journal entries for fund accounting.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DEVELOPMENT WORKFLOW                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. DEBUG DECODER (debug_decoder.py)                            │
│     └── Test transactions with mega verbose logging             │
│     └── Fix decoding issues step by step                        │
│     └── Validate event parsing and journal entries              │
│                                                                  │
│                          │                                       │
│                          ▼ Once working...                       │
│                                                                  │
│  2. EXTRACT TO MODULE (main_app/services/decoders/)             │
│     └── Create clean, production-ready decoder class            │
│     └── Follow BaseDecoder pattern                              │
│     └── Add to __init__.py exports                              │
│                                                                  │
│                          │                                       │
│                          ▼ Once all platforms complete...        │
│                                                                  │
│  3. COMBINE IN REGISTRY (registry.py)                           │
│     └── Route transactions to correct decoder                   │
│     └── Unified interface for all platforms                     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Key Files

| File | Purpose |
|------|---------|
| `debug_decoder.py` | Shiny app workbench for testing/debugging (run with `python debug_decoder.py`) |
| `main_app/services/decoders/base.py` | Base classes: `BaseDecoder`, `DecodedTransaction`, `JournalEntry`, etc. |
| `main_app/services/decoders/gondi_decoder.py` | **COMPLETE** - Gondi multi-source lending |
| `main_app/services/decoders/blur_decoder.py` | **COMPLETE** - Blur Blend NFT lending |
| `main_app/services/decoders/nftfi_decoder.py` | **COMPLETE** - NFTfi fixed-term lending (V1/V2/Coordinator) |
| `main_app/services/decoders/arcade_decoder.py` | **COMPLETE** - Arcade LoanCore v3 lending |
| `main_app/services/decoders/zharta_decoder.py` | **COMPLETE** - Zharta peer-to-pool NFT lending |
| `main_app/services/decoders/registry.py` | Routes transactions to correct decoder |
| `main_app/s3_utils.py` | S3 functions including `load_abi_from_s3()` |

## Decoder Status

| Platform | Status | Events Handled |
|----------|--------|----------------|
| **Gondi** | ✅ COMPLETE | LoanEmitted, LoanRepaid, LoanRefinanced, LoanForeclosed, LoanLiquidated |
| **Blur** | ✅ COMPLETE | LoanOfferTaken, Repay, Refinance, StartAuction, Seize, BuyLocked |
| **NFTfi** | ✅ COMPLETE | LoanStarted, LoanRepaid, LoanLiquidated (V1, V2, V2.1, Coordinator) |
| **Arcade** | ✅ COMPLETE | LoanStarted, LoanRepaid, LoanClaimed (collateral seizure) |
| **Zharta** | ✅ COMPLETE | LoanCreated, LoanPayment, LoanPaid, LoanDefaulted (pending sig) |
| **Generic** | ✅ COMPLETE | WETH wrap/unwrap, ETH transfers, ERC20 transfers, ERC721 transfers, Approvals |

## Critical Technical Details

### The `process_log()` Pattern (IMPORTANT!)

**WRONG** (broken in web3.py):
```python
# This fails with cryptic errors about "Error flag must be one of..."
decoded = contract.events.SomeEvent().process_receipt(receipt, errors="discard")
```

**CORRECT** (working pattern):
```python
# Process each log individually with process_log()
for log in receipt['logs']:
    for event_name in event_names_from_abi:
        try:
            decoded = contract.events[event_name]().process_log(log)
            # Success! This log matches this event type
            break
        except Exception:
            continue  # Try next event type
```

### ABI Loading from S3 (with Proxy Resolution)

ABIs are stored in S3 at: `s3://realworldnav-beta-1/drip_capital/smart_contract_ABIs/`

The debug_decoder.py automatically resolves proxy contracts to their implementation before loading ABIs:

```python
# debug_decoder.py handles this automatically:
# 1. Checks EIP-1967 implementation slot
# 2. Checks EIP-1967 beacon slot
# 3. Checks EIP-1167 minimal proxy pattern
# 4. Checks custom getImplementation() function
# 5. Loads ABI from implementation address

# Example: Blur Lending
# Proxy:          0x29469395eaf6f95920e59f858042f0e28d98a20b (6 entries - just proxy events)
# Implementation: 0xb258ca5559b11cd702f363796522b04d7722ea56 (78 entries - full ABI)
```

```python
from main_app.s3_utils import load_abi_from_s3

abi = load_abi_from_s3("0xf41b389e0c1950dc0b16c9498eae77131cc08a56")
if abi:
    contract = w3.eth.contract(address=addr, abi=abi)
```

### Gondi Contract Versions

Gondi has 3 contract versions with different struct formats:

| Version | Address | Struct Type | Tranche Fields |
|---------|---------|-------------|----------------|
| V1 | `0xf41b389e0c1950dc0b16c9498eae77131cc08a56` | tranche | 7 fields (has `floor`) |
| V2 | `0x478f6f994c6fb3cf3e444a489b3ad9edb8ccae16` | source | 6 fields (NO `floor`) |
| V3 | `0xf65b99ce6dc5f6c556172bcc0ff27d3665a7d9a8` | tranche | 7 fields (has `floor`) |

V2 uses "source" instead of "tranche" in field names and has different tuple indexing.

### Blur Contract Details

Blur has a proxy contract pattern:

| Contract | Address | Purpose |
|----------|---------|---------|
| Blur Lending (Proxy) | `0x29469395eaf6f95920e59f858042f0e28d98a20b` | Main lending entry point |
| Blur Pool | `0x0000000000a39bb272e79075ade125fd351887ac` | ETH pool for liquidity |

**Blur Lien struct (9 fields):**
```
[0] lender - address
[1] borrower - address
[2] collection - NFT contract address
[3] tokenId - NFT token ID
[4] amount - principal in wei
[5] startTime - loan start timestamp
[6] rate - annual rate in basis points (for continuous compounding)
[7] auctionStartBlock - block when auction started (0 if not in auction)
[8] auctionDuration - duration of auction in blocks
```

**Blur Interest Calculation (Continuous Compounding):**
```python
# Unlike Gondi's simple interest, Blur uses continuous compounding
# Formula: principal * (e^(rate * time_in_years) - 1)
import math
rate_decimal = rate_bps / 10000  # e.g., 1000 bps = 10%
time_in_years = time_elapsed_seconds / (365 * 24 * 3600)
compound_factor = math.exp(rate_decimal * time_in_years)
interest = principal * (compound_factor - 1)
total_due = principal + interest
```

**Key Difference from Gondi:**
- Gondi: Fixed-term loans with simple interest, multi-tranche support
- Blur: Callable loans (no fixed term), continuous compounding, single lender per lien

### Journal Entry Structure

Each decoder generates `JournalEntry` objects with:
- Debits and credits that must balance
- Wallet role (lender/borrower)
- Category (LOAN_ORIGINATION, LOAN_REPAYMENT, LOAN_REFINANCE, etc.)
- ETH/USD price at transaction time

Example for lender on refinance:
```
DEBIT  120.10 - Loans Receivable    0.09 WETH  (new loan out)
CREDIT 100.40 - Deemed Cash         0.09 WETH  (principal sent)
CREDIT 400.10 - Interest Income     0.001 WETH (fee received, if any)
```

## Testing a New Decoder

1. **Find a test transaction** for the platform
2. **Run debug_decoder.py**: `python debug_decoder.py`
3. **Paste the tx hash** and click Decode
4. **Observe the verbose output** to identify where decoding fails
5. **Fix issues** in debug_decoder.py until it works
6. **Extract** working code into `main_app/services/decoders/{platform}_decoder.py`

## Test Transactions

| Platform | Transaction | Type |
|----------|-------------|------|
| Gondi V1 | `0x3fac920ae33e0934c34f54cd0e4297aceb67ef638c8973a791f7342af93cff5c` | refinanceFull |
| Blur | `0x9a8dd4d75de0926bd73943b3d9fc152b6f5cccddebc693b54d1b5bea255da2bc` | Repay |
| Blur | `0x051d87bef301e901739d64aad60bfba7568ba223bdd4f6e3501cc5055c33a605` | BuyLockedETH |
| Arcade | `0xa72e5fef164d3b34af51994ed889dbeb1d1a35e0106d8364e944cce18233d9d5` | Repay (156 WETH) |
| Arcade | `0x5be6efb519e6cd6350f625577e7b7ef3b878ff8f88e0b6e3b33367de449a0e85` | Claim (collateral seized) |
| Arcade | `0xd4eb59956d30d65dccd80dfa6abb0a46ff660275ed76a9666334ace9561357f0` | Claim |
| Arcade | `0x8cfdf1880d6bfaf4ccb8147dac8f4c1bc7e66999e94bd276ba88a0ce04c75032` | Claim |

## Common Issues

1. **"Error flag must be one of..."** - Using `process_receipt()` instead of `process_log()`
2. **ABI not found** - Check S3 path and contract address format (lowercase)
3. **Struct parsing fails** - Check V1/V2/V3 version and use correct field indices
4. **Journal entries unbalanced** - Check asset types (WETH vs ETH) and amounts

## Next Steps (as of Jan 2025)

1. ~~Build Gondi decoder~~ ✅ COMPLETE
2. ~~Add Blur to debug_decoder.py~~ ✅ COMPLETE (uses `process_log()` pattern)
3. ~~Test Blur decoder with real transactions~~ ✅ COMPLETE
4. ~~Extract Blur decoder to `main_app/services/decoders/blur_decoder.py`~~ ✅ COMPLETE
5. ~~Build NFTfi decoder~~ ✅ COMPLETE (extracted to nftfi_decoder.py)
6. ~~Build Arcade decoder~~ ✅ COMPLETE (LoanCore v3, RepaymentController)
7. ~~Build Zharta decoder~~ ✅ COMPLETE (LoanCreated, LoanPayment, LoanPaid; LoanDefaulted pending signature capture)
8. Clean up registry routing
9. Integration testing with production data

### Arcade Contract Details

| Contract | Address | Purpose |
|----------|---------|---------|
| LoanCore v3 (Proxy) | `0x81b2f8fc75bab64a6b144aa6d2faa127b4fa7fd9` | Main loan storage/events |
| LoanCore v3 (Impl) | `0x6ddb57101a17854109c3b9feb80ae19662ea950f` | Implementation |
| RepaymentController | `0xb39dab85fa05c381767ff992ccde4c94619993d4` | Handles repayments |
| OriginationController | `0x89bc08ba00f135d608bc335f6b33d7a9abcc98af` | Loan origination |
| Lender Note (aLN) | `0x349a026a43ffa8e2ab4c4e59fcaa93f87bd8ddee` | Promissory note NFT |
| Borrower Note (aBN) | `0x337104a4f06260ff327d6734c555a0f5d8f863aa` | Obligation note NFT |

**Event Signatures:**
- `LoanRepaid`: `0x9a7851747cd7ffb3fe0a32caf3da48b31f27cebe131267051640f8b72fc47186`
- `LoanClaimed`: `0xb15e438728b48d46c9a5505713e60ff50c80559f4523c8f99a246a2069a8684a`
- `LoanStarted`: `0x7bf4c3eff5f6fca4eb18f47d3c8ad58c9b9d44f64b61bb8b3836c1182c6c0dca`

### Zharta Contract Details

Zharta is a peer-to-pool NFT lending protocol using Vyper smart contracts.

| Contract | Address | Purpose |
|----------|---------|---------|
| Loans | `0xb7c8c74ed765267b54f4c327f279d7e850725ef2` | Main Loans interface |
| LoansCore | `0x5be916cff5f07870e9aef205960e07d9e287ef27` | Loan state storage |
| LendingPoolPeripheral | `0x6474ab1b56b47bc26ba8cb471d566b8cc528f308` | Pool interactions |
| CollateralVaultPeripheral | `0x35b8545ae12d89cd4997d5485e2e68c857df24a8` | Collateral storage |

**Event Signatures:**
- `LoanCreated`: `0x4a558778654b4d21f09ae7e2aa4eebc0de757d1233dc825b43183a1276a7b2a1`
- `LoanPayment`: `0x31c401ba8a3eb75cf55e1d9f4971e726115e8448c80446935cffbea991ca2473`
- `LoanPaid`: `0x42d434e1d98bb8cb642015660476f098bbb0f00b64ddb556e149e17de4dd3645`
- `LoanDefaulted`: *(pending - needs capture from real default transaction)*

**Test Transactions:**
- LoanCreated: `0x96d9fe5f317185febefe9a75df186374035a921f3b16426a45d58574cfe67f2b`
- LoanPayment + LoanPaid: `0x2507c931a0c97544ab767dfaab6060e29303ff682b410aba88ff672ae2fee42f`
