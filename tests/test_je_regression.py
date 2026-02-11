"""
Regression test for LoanRefinancedFromNewOffers journal entry generation.

Decodes tx 0x3e5973d4... on Gondi V3 and verifies the CSV output matches
perfect_journal_entries.csv exactly (ignoring only the source_file column).

Requirements:
    - INFURA_API_KEY environment variable
    - Gondi V3 ABI (loaded from S3 or fetched from Etherscan)
"""

import os
import sys
import json
import pytest
import pandas as pd
import numpy as np

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---- Test constants ----

TX_HASH = "0x3e5973d4ca4d0b9fe62dba93891c86cca8643f08136e6ebe5e2cda381b5406a6"
EXPECTED_CSV = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "perfect_journal_entries.csv",
)
ETH_PRICE = 2350.0997  # Hardcoded from expected CSV to avoid CoinGecko dependency

# Fund wallet involved in this transaction
FUND_WALLET = "0xaf06781d59a84ddef1bc126a14a098e0d3bce905"
FUND_ID = "holdings_class_B_ETH"

# Gondi V3 contract
V3_ADDRESS = "0xf65b99ce6dc5f6c556172bcc0ff27d3665a7d9a8"

# Skip entire module if no API key
pytestmark = pytest.mark.skipif(
    not os.getenv("INFURA_API_KEY"),
    reason="INFURA_API_KEY not set",
)


def _load_v3_abi():
    """
    Load Gondi V3 ABI. Tries S3 first, then Etherscan as fallback.
    Returns ABI list or None.
    """
    # Try S3
    try:
        from main_app.services.decoders.abis import load_abi

        abi = load_abi(V3_ADDRESS, "gondi_v3")
        if abi:
            return abi
    except Exception:
        pass

    # Fallback: Etherscan (no API key needed for basic usage)
    try:
        import requests
        from web3 import Web3

        resp = requests.get(
            "https://api.etherscan.io/api"
            f"?module=contract&action=getabi"
            f"&address={Web3.to_checksum_address(V3_ADDRESS)}",
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "1":
                return json.loads(data["result"])
    except Exception:
        pass

    return None


def test_loan_refinanced_from_new_offers_csv():
    """
    End-to-end regression: decode tx, generate JE CSV, diff against expected.

    The only allowed difference is the 'source_file' column.
    """
    from web3 import Web3
    from main_app.services.decoders.gondi_decoder import (
        GondiEventDecoder,
        GondiJournalEntryGenerator,
        format_journal_entries_csv,
    )

    # ---- Setup ----

    w3 = Web3(
        Web3.HTTPProvider(
            f"https://mainnet.infura.io/v3/{os.getenv('INFURA_API_KEY')}"
        )
    )
    assert w3.is_connected(), "Cannot connect to Ethereum node"

    v3_abi = _load_v3_abi()
    if not v3_abi:
        pytest.skip("Cannot load Gondi V3 ABI (no S3 access and Etherscan fallback failed)")

    contracts = {
        V3_ADDRESS: w3.eth.contract(
            address=Web3.to_checksum_address(V3_ADDRESS),
            abi=v3_abi,
        )
    }

    wallet_metadata = {
        FUND_WALLET: {
            "fund_id": FUND_ID,
            "friendly_name": "Holdings Class B ETH",
            "category": "fund",
        }
    }

    # ---- Decode ----

    decoder = GondiEventDecoder(
        w3=w3, contracts=contracts, wallet_metadata=wallet_metadata
    )
    events = decoder.decode_transaction(TX_HASH)
    assert events, f"No events decoded from {TX_HASH}"

    # ---- Generate journal entries ----

    journal_gen = GondiJournalEntryGenerator(wallet_metadata=wallet_metadata)
    results = journal_gen.process_events(
        events, generate_accruals=False, generate_reversals=False
    )

    je_dfs = [
        df
        for df in results.values()
        if isinstance(df, pd.DataFrame) and not df.empty and "debit" in df.columns
    ]
    assert je_dfs, "No journal entries generated"
    combined = pd.concat(je_dfs, ignore_index=True)

    # ---- Format for CSV ----

    actual = format_journal_entries_csv(combined, ETH_PRICE)

    # ---- Load expected ----

    expected = pd.read_csv(EXPECTED_CSV)

    # ---- Drop source_file (the only allowed difference) ----

    actual = actual.drop(columns=["source_file"], errors="ignore")
    expected = expected.drop(columns=["source_file"], errors="ignore")

    # ---- Assert column names and order ----

    assert list(actual.columns) == list(expected.columns), (
        f"Column mismatch.\n"
        f"Actual:   {list(actual.columns)}\n"
        f"Expected: {list(expected.columns)}"
    )

    # ---- Assert row count ----

    assert len(actual) == len(expected), (
        f"Row count mismatch: {len(actual)} actual vs {len(expected)} expected"
    )

    # ---- Assert values ----

    NUMERIC_COLS = {
        "debit_crypto",
        "credit_crypto",
        "eth_usd_price",
        "debit_USD",
        "credit_USD",
        "principal_crypto",
        "principal_USD",
        "payoff_amount_crypto",
        "payoff_amount_USD",
        "annual_interest_rate",
        "loan_id",
        "token_id",
        "tranche_floor",
        "tranche_index",
    }

    mismatches = []

    for col in actual.columns:
        for i in range(len(actual)):
            a_val = actual.iloc[i][col]
            e_val = expected.iloc[i][col]

            if col in NUMERIC_COLS:
                # Numeric comparison with relative tolerance
                a_num = _safe_float(a_val)
                e_num = _safe_float(e_val)
                if abs(e_num) > 1e-10:
                    rel_diff = abs(a_num - e_num) / abs(e_num)
                    if rel_diff >= 1e-6:
                        mismatches.append(
                            f"Row {i}, col '{col}': {a_num} != {e_num} "
                            f"(rel diff: {rel_diff:.2e})"
                        )
                elif abs(a_num) > 1e-10:
                    mismatches.append(
                        f"Row {i}, col '{col}': {a_num} != 0"
                    )
            else:
                # String comparison (handle NaN / None / empty)
                a_str = _safe_str(a_val)
                e_str = _safe_str(e_val)
                if a_str != e_str:
                    mismatches.append(
                        f"Row {i}, col '{col}': '{a_str}' != '{e_str}'"
                    )

    assert not mismatches, (
        f"Found {len(mismatches)} value mismatch(es):\n"
        + "\n".join(mismatches[:20])
    )


# ---- Helpers ----

def _safe_float(val) -> float:
    """Convert a value to float, treating NaN/None/empty as 0."""
    if pd.isna(val):
        return 0.0
    s = str(val).strip()
    if s == "":
        return 0.0
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _safe_str(val) -> str:
    """Convert a value to string, treating NaN/None as empty."""
    if pd.isna(val):
        return ""
    return str(val).strip()
