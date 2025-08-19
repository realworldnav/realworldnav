
## load and rip
"""

df_for_group1 = df_combined
# 1) Ensure your column is UTC-aware
if df_for_group1['transaction_datetime'].dt.tz is None:
    # was naive → localize to UTC
    df_for_group1['transaction_datetime'] = df_for_group1['transaction_datetime'].dt.tz_localize('UTC')
else:
    # already aware → convert (no-op if already UTC)
    df_for_group1['transaction_datetime'] = df_for_group1['transaction_datetime'].dt.tz_convert('UTC')

# 2) Build UNIX-seconds column
df_for_group1['timeStamp'] = (
    df_for_group1['transaction_datetime'].astype('int64') // 10**9
).astype('Int64')

# 3) Normalize to midnight UTC for your “date” column
df_for_group1['date'] = df_for_group1['transaction_datetime'].dt.normalize()  # still tz-aware

# 4) Sort and inspect
df_for_group1.sort_values('timeStamp', inplace=True)
print(df_for_group1[['transaction_datetime','timeStamp']].dtypes)
print(df_for_group1[['transaction_datetime','date','timeStamp']].tail())

# Use tokenSymbol instead of tokenName
df_for_group1['tokenName'] = df_for_group1['tokenSymbol']

if "blockNumber" not in df_for_group1.columns:
    df_for_group1["blockNumber"] = BLOCK_END

hashes_to_update = [
    "0x39a7bbae22e0a6e97ca074851fa274b0eec04f9ef349ca13c7c55d151886de45",
    "0xda7b6b7daac89eedb7d605fe697a1f553aaa5cd6551a9fab19e121eeff3cc109"
]

df_for_group1.loc[df_for_group1["hash"].isin(hashes_to_update), "tokenDecimal"] = None
df_for_group1["tokenDecimal"] = pd.to_numeric(df_for_group1["tokenDecimal"], errors="coerce")

print(df_for_group1.loc[df_for_group1["tokenDecimal"] == "Unknown", "hash"].unique())

"""## Decimal Work"""

dec_map = {
    'ETH': 18,
    'WETH': 18,
    'DAI': 18,
    'USDC': 6,
    'USDT': 6,
    # add any others…
}

# 1) ensure tokenSymbol is normalized
df_for_group1['tokenSymbol'] = (
    df_for_group1['tokenSymbol']
        .fillna('ETH')   # default missing → ETH
        .str.upper()
)

# 2) preserve any on-chain decimal, then try your map for missing
df_for_group1['tokenDecimal'] = df_for_group1['tokenDecimal'].astype(float)  # ensure float to allow NaN
# 3) final fallback: fill remaining NaNs with 18
df_for_group1['tokenDecimal'] = df_for_group1['tokenDecimal'].fillna(18)

# mask for rows where decimal wasn’t set on-chain
mask_missing = df_for_group1['tokenDecimal'].isna()

# fill from your map wherever you have an entry
df_for_group1.loc[mask_missing, 'tokenDecimal'] = (
    df_for_group1.loc[mask_missing, 'tokenSymbol']
         .map(dec_map)
)
df_for_group1

columns_to_keep = [
    "hash", "from", "to", "value",
    "tokenName", "tokenSymbol", "TOKEN_SYMBOL", "transaction_datetime", "tokenDecimal"
]

hash_checker4 = df_for_group1.loc[
    df_for_group1["hash"] == "0xc158502b173b162fa4acc720cb7d20122a64b2687fde95a55d5cbbdc973dbc61",
    columns_to_keep
].copy()

hash_checker4

from decimal import Decimal, InvalidOperation

df_for_group2 = df_for_group1.copy()

# Force value as Decimal safely, default to 0 if invalid
def to_decimal_safe(x):
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(0)

df_for_group2['value'] = df_for_group2['value'].map(to_decimal_safe)

def scale_to_raw(v, decimals):
    try:
        d = Decimal(str(v))
        scale = Decimal(10) ** int(decimals)
        result = d * scale
        return result.quantize(Decimal('1'))  # Round to integer, no fractional part
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(0)

mask_human = (df_for_group2['value'] < Decimal('1e6')) & (df_for_group2['value'] != 0)

df_for_group2.loc[mask_human, 'value'] = df_for_group2.loc[mask_human].apply(
    lambda row: scale_to_raw(row['value'], row['tokenDecimal']),
    axis=1
)

print(df_for_group2.loc[mask_human, 'value'].map(type).unique())

df_for_group2.columns

from decimal import Decimal
stablecoins = ["USDC", "USDT", "VARIABLEDEBTETHUSDC", "VARIABLEDEBTETHUSDT"]

# Ensure tokenSymbol is uppercase and fill any missing values first
#df_for_group1['tokenSymbol'] = df_for_group1['tokenSymbol'].fillna("ETH").str.upper()

df_for_group2['tx_price_usd'] = df_for_group2.apply(
    lambda row: (
        Decimal("1.00") if row['tokenSymbol'] in stablecoins
        else Decimal(row['TX_ETH_USD_price'])
    ).quantize(SCALE_USD, rounding=ROUND_HALF_EVEN),
    axis=1
)

df_for_group2['eod_price_usd'] = df_for_group2.apply(
    lambda row: (
        Decimal("1.00") if row['tokenSymbol'] in stablecoins
        else Decimal(row['EOD_ETH_USD_price'])
    ).quantize(SCALE_USD, rounding=ROUND_HALF_EVEN),
    axis=1
)

df_for_group2.drop(columns=['date',], inplace=True)
df_for_group2.rename(columns={'transaction_datetime': 'date',}, inplace=True)
df_for_group2

"""## Fund Wallet ID"""

fund_wallet_ids_lower = [w.lower() for w in fund_wallet_ids]
fund_ids = set(w.lower() for w in fund_wallet_ids_lower)

# 1) pick the fund wallet into its own column
mask_from = df_for_group2['from'].str.lower().isin(fund_ids)
mask_to   = df_for_group2['to'].str.lower().isin(fund_ids)

df_for_group2.loc[mask_from, 'wallet_address'] = df_for_group2.loc[mask_from, 'from']
df_for_group2.loc[mask_to,   'wallet_address'] = df_for_group2.loc[mask_to,   'to']

# 2) checksum it so it lines up with your metadata keys
df_for_group2['wallet_cs'] = (
    df_for_group2['wallet_address']
      .dropna()
      .str.lower()
      .map(Web3.to_checksum_address)
)

# 3) now map in fund_id and platform_variable_name
df_for_group2['friendly_name']            = df_for_group2['wallet_cs'].map(lambda a: wallet_metadata.get(a, {}).get('friendly_name'))
df_for_group2['fund_id']                = df_for_group2['wallet_cs'].map(lambda a: wallet_metadata.get(a, {}).get('fund_id'))
df_for_group2['platform_variable_name'] = df_for_group2['wallet_cs'].map(lambda a: wallet_metadata.get(a, {}).get('platform_variable_name'))


# (optional) drop the helper column if you like
df_for_group2.drop(columns='wallet_cs', inplace=True)
df_for_group2

hash_checker6 = df_for_group2[df_for_group2["hash"] == "0xf20302e87dc24b5df630fa91a334b941f3dc912e5658887a0c8e6798ff448743"]
hash_checker6

"""## Create FIFO buy/sell"""

df = df_for_group2.copy()
print(df.columns.tolist())

df['wallet_address'] = df['wallet_address'].str.lower()
df['from'] = df['from'].str.lower()
df['to']   = df['to'].str.lower()
fund_ids   = [f.lower() for f in fund_ids]

# 1. Force positive qty always
df['qty'] = df.apply(
    lambda r: (
        abs(Decimal(str(r['value']))) / (Decimal(10) ** int(r['tokenDecimal']))
    ).quantize(SCALE_CRYPTO),
    axis=1
)




# 3) identify internal vs external
internal_mask = (
    df['from'].str.lower().isin(fund_ids) &
    df['to'].str.lower().isin(fund_ids)
)
external_df = df[~internal_mask].copy()
internal_df = df[ internal_mask].copy()

# 4) sign & side for externals just as before
external_df['wallet_address'] = external_df['wallet_address'].str.lower()
external_df['qty'] = external_df.apply(
    lambda r: (
        r['qty'] if str(r['to']).lower() == str(r['wallet_address']).lower()
        else -r['qty']
    ).quantize(SCALE_CRYPTO),
    axis=1
)


external_df['side'] = external_df['qty'].apply(lambda q: 'buy' if q>0 else 'sell')

# 5) split internals into two legs

# 5a) sell-leg out of the `from` wallet
sell_leg = internal_df.copy()
sell_leg['wallet_address'] = sell_leg['from'].str.lower()
sell_leg['qty'] = -sell_leg['qty']
sell_leg['side']           = 'sell'

# 5b) buy-leg into the `to` wallet
buy_leg = internal_df.copy()
buy_leg['wallet_address'] = buy_leg['to'].str.lower()
buy_leg['qty']  =  buy_leg['qty']
buy_leg['side']           = 'buy'

# 6) concatenate and carry forward the asset label
result = (
    pd.concat([external_df, sell_leg, buy_leg], ignore_index=True)
      .assign(asset=lambda d: d['tokenSymbol'])
)

result.sort_values(by=['timeStamp', 'side', 'qty'], ascending=[True, True, False], inplace=True)

result.columns

# Rename columns
result.rename(columns={
    "transaction_datetime": "date",
    "TX_ETH_USD_price": "eth_usd_price"
}, inplace=True)

# Fill missing asset names with "ETH"
result["asset"] = result["asset"].fillna("ETH")

# Sort by date, side, and descending qty
result.sort_values(by=["date", "side", "qty"], ascending=[True, True, False], inplace=True)

# Final DataFrame
result.tail(5)

# Always map wallet_original fresh from wallet_address → wallet_metadata key
result['wallet_address'] = result['wallet_address'].str.lower()

result['wallet_cs'] = result['wallet_address'].map(
    lambda x: Web3.to_checksum_address(x) if pd.notna(x) and x else None
)

result['wallet_original'] = result['wallet_cs']

result['fund_id'] = result['wallet_cs'].map(lambda a: wallet_metadata.get(a, {}).get('fund_id'))
result['platform_variable_name'] = result['wallet_cs'].map(lambda a: wallet_metadata.get(a, {}).get('platform_variable_name'))

result.drop(columns=['wallet_cs'], inplace=True)

hash_checker3 = result[result["hash"] == "0x671f49679b562c4f1261c9ad89ad9f70d4bd9fd9a26af4dbecea1904383cf889"].copy()#Wrapped Ether
hash_checker3

result.columns

"""## Add manual entries"""

import pandas as pd
from decimal import Decimal

# Load CSV
true_up_df = pd.read_csv("/content/drive/MyDrive/Drip_Capital/accounting_records/investments/cryptocurrency/dfs/manual_entries_for_fifo.csv")

# Convert all numeric-looking columns to Decimal
for col in true_up_df.columns:
    if pd.api.types.is_numeric_dtype(true_up_df[col]):
        true_up_df[col] = true_up_df[col].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)

# Append to result DataFrame
result = pd.concat([result, true_up_df], ignore_index=True)

# Confirm
print(f"✅ Appended {len(true_up_df)} true-up rows. New total: {len(result)} rows.")

print(result.columns[result.columns.duplicated()])
print(true_up_df.columns[true_up_df.columns.duplicated()])

"""## Test FIFO Ending Balances"""

# 4c) Build the exact DataFrame your FIFO engine expects
fifo_input = result[[
    'hash',
    'date',
    'wallet_address',
    'fund_id',
    'asset',
    'side',
    'qty',
    'eth_usd_price'
]].copy()
fifo_input.sort_values(by=['date', 'side', 'qty'], ascending=[True, True, False], inplace=True)
fifo_input = fifo_input.loc[:, ~fifo_input.columns.duplicated()].copy()

fifo_input.tail(5)

"""### checka"""

# Ensure full column values are shown
pd.set_option('display.max_colwidth', None)
from datetime import datetime
fifo_input['date'] = pd.to_datetime(fifo_input['date'])

# Define your date range
start_date = pd.to_datetime("2024-12-01 00:00+00:00")
end_date = pd.to_datetime("2024-12-31 23:59:59+00:00")

# Apply filters including the date range
weth_check = fifo_input[
    (fifo_input['asset'] == 'WETH') &
    (fifo_input['wallet_address'] == '0xef732b402abcf15df684e0e9c5795022a8696d9d') &
    (fifo_input['date'] >= start_date) &
    (fifo_input['date'] <= end_date)
    & (fifo_input["side"] == "sell")
]
qty_sum = weth_check.groupby("side")["qty"].sum()
#net = qty_sum.loc["buy"] + qty_sum.loc["sell"]
print(qty_sum)
#print(f"net: {net}")

weth_check.tail(40)

from google.colab import files
weth_check.to_csv("fuwah.csv", index=False)
#files.download("fuwah.csv")

hash_checker5 = fifo_input[fifo_input["hash"] == "0xddf9057ae7174ba5800e7d53fd5a2a68cf5af1fe0e5ff3584681791de4024645"].copy()#Wrapped Ether
hash_checker5

# Step 1: Normalize asset column
fifo_input["asset"] = fifo_input["asset"].str.upper()

# Step 2: Find hashes that have BLUR POOL
blur_hashes = set(fifo_input[fifo_input["asset"] == "BLUR POOL"]["hash"])

# Step 3: Find hashes that have ETH
eth_hashes = set(fifo_input[fifo_input["asset"] == "ETH"]["hash"])

# Step 4: Find hashes that have BOTH
shared_hashes = blur_hashes & eth_hashes

# Step 5: Filter original df
blur_eth_pairs = fifo_input[fifo_input["hash"].isin(shared_hashes)]

# Step 6: Optional - Sort for clarity
blur_eth_pairs = blur_eth_pairs.sort_values(["hash", "asset", "qty"], ascending=[True, True, False])

# ✅ Show result
print(f"✅ Found {len(shared_hashes)} hashes with both BLUR POOL and ETH entries.")

# Only rows where `qty != 0` and `hash` is duplicated
weth_check_dupes = weth_check[
    (weth_check["qty"] != 0) &
    (weth_check.duplicated(subset=["hash"], keep=False))
]
weth_check_dupes.head(30)

df['wallet_address'] = df['wallet_address'].str.lower()
df['from'] = df['from'].str.lower()
df['to']   = df['to'].str.lower()
fund_ids   = [f.lower() for f in fund_ids]

# 1. Force positive qty always
df['qty'] = df.apply(
    lambda r: (
        abs(Decimal(str(r['value']))) / (Decimal(10) ** int(r['tokenDecimal']))
    ).quantize(SCALE_CRYPTO),
    axis=1
)




# 3) identify internal vs external
internal_mask = (
    df['from'].str.lower().isin(fund_ids) &
    df['to'].str.lower().isin(fund_ids)
)
external_df = df[~internal_mask].copy()
internal_df = df[ internal_mask].copy()

# 4) sign & side for externals just as before
external_df['wallet_address'] = external_df['wallet_address'].str.lower()
external_df['qty'] = external_df.apply(
    lambda r: (
        r['qty'] if str(r['to']).lower() == str(r['wallet_address']).lower()
        else -r['qty']
    ).quantize(SCALE_CRYPTO),
    axis=1
)


external_df['side'] = external_df['qty'].apply(lambda q: 'buy' if q>0 else 'sell')

# 5a) sell-leg out of the `from` wallet
sell_leg = internal_df.copy()
sell_leg['wallet_address'] = sell_leg['from'].str.lower()
sell_leg['qty'] = sell_leg['qty'].apply(lambda x: (-abs(x)).quantize(SCALE_CRYPTO))
sell_leg['side'] = 'sell'

# 5b) buy-leg into the `to` wallet
buy_leg = internal_df.copy()
buy_leg['wallet_address'] = buy_leg['to'].str.lower()
buy_leg['qty'] = buy_leg['qty'].apply(lambda x: abs(x).quantize(SCALE_CRYPTO))
buy_leg['side'] = 'buy'


# 6) concatenate and carry forward the asset label
result = (
    pd.concat([external_df, sell_leg, buy_leg], ignore_index=True)
      .assign(asset=lambda d: d['tokenSymbol'])
).copy()

"""# grab onchain balance"""

import json, pandas as pd, requests
from web3 import Web3
from datetime import timezone

# === Setup ===
INFURA_URL = "https://mainnet.infura.io/v3/16f12641c1db46beb60e95cf4c88cbe1"
w3 = Web3(Web3.HTTPProvider(INFURA_URL))
assert w3.is_connected() and w3.eth.chain_id == 1

ETHERSCAN_API_KEY = "P13CVTCP43NWU9GX5D9VBA2QMUTJDDS941"

def get_token_balance_etherscan(address, token_address, block, api_key):
    url = "https://api.etherscan.io/api"
    params = {
        "module": "account",
        "action": "tokenbalance",
        "contractaddress": token_address,
        "address": address,
        "tag": block,
        "apikey": api_key
    }
    response = requests.get(url, params=params).json()
    if response.get("status") == "1":
        return int(response["result"])
    return 0

# === Get target block at month end ===
target_dt = (end - pd.Timedelta(days=1)).replace(hour=23, minute=59, second=59)
ts = int(target_dt.timestamp())
res = requests.get(
    "https://api.etherscan.io/api",
    params={
        "module":    "block",
        "action":    "getblocknobytime",
        "timestamp": ts,
        "closest":   "before",
        "apikey":    ETHERSCAN_API_KEY
    }
)
blk = int(res.json()["result"])
print(f"Querying block {blk} ({target_dt.isoformat()})")

# === Tokens ===
TOKENS = [
    dict(symbol="ETH",       address=None,                                                            decimals=18),
    dict(symbol="WETH",      address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",                     decimals=18),
    dict(symbol="BLUR POOL",      address="0x0000000000A39bb272e79075ade125fd351887Ac",                     decimals=18),
    dict(symbol="USDC",      address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",                     decimals=6 ),
    dict(symbol="AETHWETH",  address="0x4d5F47FA6A74757f35C14fD3a6Ef8E3C9BC514E8",                     decimals=18),
    dict(symbol="ARCD",      address="0x7De71BC6694ca827e043b18102CAf01518C0b54D",                     decimals=18),
]

ERC20_ABI = json.loads("""[
  { "constant":true, "inputs":[{"name":"_owner","type":"address"}],
    "name":"balanceOf", "outputs":[{"name":"balance","type":"uint256"}],
    "type":"function" }
]""")

wallets = [Web3.to_checksum_address(w) for w in fund_wallet_ids]
records = []

for w in wallets:
    # ETH
    wei = w3.eth.get_balance(w, block_identifier=blk)
    eth = float(w3.from_wei(wei, "ether"))
    records.append({
        "wallet_address": w.lower(),
        "asset": "ETH",
        "onchain": round(eth, 6)
    })

    # ERC-20s
    for t in TOKENS[1:]:
        symbol = t["symbol"]
        try:
            contract = w3.eth.contract(address=Web3.to_checksum_address(t["address"]), abi=ERC20_ABI)
            raw = contract.functions.balanceOf(w).call(block_identifier=blk)
        except Exception:
            print(f"⚠️ balanceOf() failed for {symbol}, falling back to Etherscan…")
            raw = get_token_balance_etherscan(w, t["address"], hex(blk), ETHERSCAN_API_KEY)

        try:
            bal = raw / 10**t["decimals"]
        except Exception:
            bal = 0

        records.append({
            "wallet_address": w.lower(),
            "asset": symbol,
            "onchain": round(bal, 6)
        })

# === DataFrame output ===
onchain_df = pd.DataFrame(records)
onchain_df = onchain_df.query("onchain != 0").reset_index(drop=True)

export_path = f"/content/drive/MyDrive/Drip_Capital/accounting_records/investments/cryptocurrency/generated/onchain_balances_{end}.csv"
# onchain_df.to_csv(export_path, index=False)

onchain_df

"""# target wallet"""

# ------------------------------------------------------------------
# Compare on-chain token balances with FIFO-derived balances
# ------------------------------------------------------------------

# 1. Filter FIFO to just fund_wallet_ids
df_target = fifo_input[
    fifo_input["wallet_address"].str.lower().isin([w.lower() for w in fund_wallet_ids])
]

# 2. Compute ending balances from FIFO
ending_balances = (
    df_target
      .groupby(["wallet_address", "asset"])["qty"]
      .sum()
      .reset_index()
      .rename(columns={"qty": "ending_balance"})
)

ending_balances["wallet_address"] = ending_balances["wallet_address"].str.lower()
ending_balances["ending_balance"] = ending_balances["ending_balance"].astype(float).round(6)

# 3. Merge with onchain balances
combined = pd.merge(
    ending_balances,
    onchain_df,
    on=["wallet_address", "asset"],
    how="outer"
).fillna(0)

# 4. Compute difference
combined["difference"] = (combined["onchain"] - combined["ending_balance"]).round(6)

# 5. Optional: filter to rows with any nonzero value
#combined = combined[
#    (combined["ending_balance"] != 0) |
#    (combined["onchain"] != 0)
#].reset_index(drop=True)

# 6. Display result
combined

print(fifo_input["eth_usd_price"].unique())

combined.to_csv("/content/drive/MyDrive/Drip_Capital/accounting_records/investments/cryptocurrency/generated/fifo_ending_balances.csv", index=False)

"""# FIFO GENERATOR"""

import os
import pandas as pd
from collections import deque
from decimal import Decimal, InvalidOperation, DivisionByZero
ts = datetime.now().strftime("%H%M%S")
# ────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ────────────────────────────────────────────────────────────────────
MASTER_PATH_ETH = f"/content/drive/MyDrive/Drip_Capital/accounting_records/investments/cryptocurrency/generated/master_fifo_ledger_eth_{ts}.csv"
MASTER_PATH_USD = f"/content/drive/MyDrive/Drip_Capital/accounting_records/investments/cryptocurrency/generated/master_fifo_ledger_usd_{ts}.csv"

STABLECOINS = {"USDC", "USDT", "DAI"}
ETH_one_for_one    = {"ETH", "BLUR POOL", "BLUR", "WETH", "AETHWETH"}
WSTETH       = {"WSTETH"}
WSTETH_RATE_DEFAULT  = Decimal("1.18045433553113")
MEME_COIN    = {"MEME"}
MEME_RATE    = Decimal("0.0000006")


# ────────────────────────────────────────────────────────────────────
# FIFO LOT TRACKING CLASS
# ────────────────────────────────────────────────────────────────────
class FIFOTracker:
    def __init__(self):
        self.lots = {}
        self.logs = []

    def process(self, fund_id, wallet, asset, side, qty, price_usd, date, tx_hash, log=False, eth_usd_price=None):
        try:
            q = Decimal(str(qty))
            p = Decimal(str(price_usd))
        except (InvalidOperation, TypeError):
            return

        key = (fund_id, wallet, asset)
        dq = self.lots.setdefault(key, deque())

        proceeds = cost_basis = gain = Decimal("0")

        if side.lower() == "buy":
            dq.append([q, p])
        else:
            to_sell = abs(q)
            while to_sell > 0 and dq:
                lq, lp = dq[0]
                take = min(lq, to_sell)
                proceeds   += take * p
                cost_basis += take * lp
                gain       += take * (p - lp)
                lq -= take
                to_sell -= take
                if lq > 0:
                    dq[0][0] = lq
                else:
                    dq.popleft()

            if to_sell > 0:
                proceeds   += to_sell * p
                cost_basis += to_sell * p
                dq.appendleft([-to_sell, p])

        remaining_qty  = sum(l for l, _ in dq)
        remaining_cost = sum(l * pr for l, pr in dq)

        if log:
            self.logs.append({
                "fund_id": fund_id,
                "wallet_address": wallet,
                "asset": asset,
                "date": date,
                "hash": tx_hash,
                "side": side,
                "qty": q,
                "price_eth": eth_usd_price,
                "proceeds_usd": proceeds,
                "cost_basis_sold_usd": cost_basis,
                "realized_gain_usd": gain,
                "remaining_qty": remaining_qty,
                "remaining_cost_basis_usd": remaining_cost,
            })

    def to_dataframe(self):
        return pd.DataFrame(self.logs)

# ────────────────────────────────────────────────────────────────────
# WSTETH ↔ ETH rate per hash (carry-forward only)
# ────────────────────────────────────────────────────────────────────
def build_wsteth_rate_map(df):
    df = df.copy()
    df["asset_upper"] = df["asset"].str.upper()
    df["date"]        = pd.to_datetime(df["date"])

    # keep only rows that mention ETH or WSTETH
    conv = df[df["asset_upper"].isin({"ETH", "WSTETH"})]

    # visit hashes in chronological order (earliest timestamp in hash)
    hash_order = (
        conv.groupby("hash")["date"]
            .min()
            .sort_values()
            .index
    )

    rate_map  = {}
    last_rate = WSTETH_RATE_DEFAULT       # fallback for very first hashes

    for h in hash_order:
        grp     = conv[conv["hash"] == h]
        assets  = set(grp["asset_upper"])

        # if both sides present, update the running rate
        if {"ETH", "WSTETH"} <= assets:
            eth_qty = grp.loc[grp["asset_upper"] == "ETH",     "qty"] \
                        .apply(lambda x: abs(Decimal(str(x)))).sum()
            wst_qty = grp.loc[grp["asset_upper"] == "WSTETH", "qty"] \
                        .apply(lambda x: abs(Decimal(str(x)))).sum()
            if wst_qty:
                last_rate = eth_qty / wst_qty

        rate_map[h] = last_rate

    return rate_map

# ────────────────────────────────────────────────────────────────────
# UNIT PRICE COMPUTATION HELPERS
# ────────────────────────────────────────────────────────────────────

def compute_unit_price_eth(row):
    try:
        asset = row["asset"].upper()
        eth_usd_price = Decimal(row["eth_usd_price"])

        if asset in ETH_one_for_one:
            return Decimal("1")

        if asset in STABLECOINS:
            return Decimal("1") / eth_usd_price

        if asset in WSTETH:
            rate = row.get("wst_rate")
            rate = Decimal(str(rate)) if rate not in (None, "") else WSTETH_RATE_DEFAULT
            return rate

        if asset in MEME_COIN:
            return MEME_RATE * eth_usd_price

    except (InvalidOperation, TypeError, DivisionByZero):
        pass

    return Decimal("0")

def compute_unit_price_usd(row):
    try:
        asset = row["asset"].upper()
        eth_usd_price = Decimal(row["eth_usd_price"])

        if asset in STABLECOINS:
            return Decimal("1")

        if asset in WSTETH:
            rate = row.get("wst_rate")
            rate = Decimal(str(rate)) if rate not in (None, "") else WSTETH_RATE_DEFAULT
            return rate * eth_usd_price

        return eth_usd_price
    except (InvalidOperation, TypeError):
        return Decimal("0")

# ────────────────────────────────────────────────────────────────────
# LEDGER BUILDER
# ────────────────────────────────────────────────────────────────────

def build_fifo_ledger(df_input, price_column, output_suffix):
    tracker = FIFOTracker()
    for r in df_input.itertuples(index=False):
        tracker.process(
            fund_id     = r.fund_id,
            wallet      = r.wallet_address,
            asset       = r.asset,
            side        = r.side,
            price_usd   = getattr(r, price_column),
            qty         = r.qty,
            date        = r.date,
            tx_hash     = r.hash,
            log         = True,
            eth_usd_price = r.eth_usd_price,
        )

    df = tracker.to_dataframe()
    df = df[df["qty"] != 0].rename(columns={"eth_usd_price": "price_eth"})

    if output_suffix == "eth":
        df = df.rename(columns={
            "proceeds_usd": "proceeds_eth",
            "cost_basis_sold_usd": "cost_basis_sold_eth",
            "realized_gain_usd": "realized_gain_eth",
            "remaining_cost_basis_usd": "remaining_cost_basis_eth",
        })
    return df

# ────────────────────────────────────────────────────────────────────
# CSV OUTPUT
# ────────────────────────────────────────────────────────────────────

def save_master_ledger(path, df):
    df.to_csv(path, index=False)

def insert_after(df, col, after):
    cols = [c for c in df.columns if c != col]
    idx  = cols.index(after) + 1
    cols.insert(idx, col)
    return df[cols]

# ────────────────────────────────────────────────────────────────────
# MAIN SCRIPT
# ────────────────────────────────────────────────────────────────────

cols_to_keep = ["fund_id", "wallet_address", "asset", "date", "hash",
                "side", "qty", "eth_usd_price"]

for path in [MASTER_PATH_ETH, MASTER_PATH_USD]:
    if not os.path.exists(path):
        pd.DataFrame(columns=cols_to_keep).to_csv(path, index=False)

df_base = fifo_input[cols_to_keep].copy()

# Build dynamic WSTETH rate map and attach
wst_rate_map = build_wsteth_rate_map(df_base)
df_base["wst_rate"] = df_base["hash"].map(wst_rate_map)

# Timestamps & ordering
df_base["date"] = pd.to_datetime(df_base["date"])
df_base = df_base.sort_values("date")

# Unit prices (updated)
df_base["unit_price_eth"] = df_base.apply(compute_unit_price_eth, axis=1)
df_base["unit_price_usd"] = df_base.apply(compute_unit_price_usd, axis=1)

# ──────────────────────────────────────────────────────────────
# 1) Build ledgers
# ──────────────────────────────────────────────────────────────
df_ledger_eth = build_fifo_ledger(df_base, "unit_price_eth", "eth")
df_ledger_usd = build_fifo_ledger(df_base, "unit_price_usd", "usd")

# ──────────────────────────────────────────────────────────────
# 2) Bring unit_price_eth into ETH ledger + total_eth
# ──────────────────────────────────────────────────────────────
merge_cols = ["fund_id", "wallet_address", "asset", "date", "hash", "side"]

unit_price_patch = (
    df_base[merge_cols + ["unit_price_eth"]]
    .drop_duplicates(merge_cols)
)

df_ledger_eth = df_ledger_eth.merge(unit_price_patch, on=merge_cols, how="left")

# Fill any remaining unit_price_eth using price_eth for ETH-denominated tokens
df_ledger_eth["unit_price_eth"] = df_ledger_eth["unit_price_eth"].fillna(
    df_ledger_eth["price_eth"]
)

# Correct total_eth calculation
mask_one_for_one = df_ledger_eth["asset"].str.upper().isin(ETH_one_for_one)

# For ETH_one_for_one assets, total_eth = abs(qty)
df_ledger_eth["total_eth"] = Decimal("0")  # default init
df_ledger_eth.loc[mask_one_for_one, "total_eth"] = (
    df_ledger_eth.loc[mask_one_for_one, "qty"]
    .apply(lambda x: abs(Decimal(str(x))))
)

# For others, total_eth = abs(qty) * unit_price_eth
df_ledger_eth.loc[~mask_one_for_one, "total_eth"] = (
    df_ledger_eth.loc[~mask_one_for_one, "qty"]
    .apply(lambda x: abs(Decimal(str(x))))
    * df_ledger_eth.loc[~mask_one_for_one, "unit_price_eth"]
    .apply(lambda x: Decimal(str(x)))
)
# ──────────────────────────────────────────────────────────────
# 2b) Patch USD ledger with total_eth, total_usd
# ──────────────────────────────────────────────────────────────
# Add unit_price_usd
unit_price_usd_patch = (
    df_base[merge_cols + ["unit_price_usd"]]
    .drop_duplicates(merge_cols)
)
df_ledger_usd = df_ledger_usd.merge(unit_price_usd_patch, on=merge_cols, how="left")

# Add eth_usd_price
eth_price_patch = (
    df_base[merge_cols + ["eth_usd_price"]]
    .drop_duplicates(merge_cols)
)
df_ledger_usd = df_ledger_usd.merge(eth_price_patch, on=merge_cols, how="left")

# Add total_eth
total_eth_patch = (
    df_ledger_eth[merge_cols + ["total_eth"]]
    .drop_duplicates(merge_cols)
)
df_ledger_usd = df_ledger_usd.merge(total_eth_patch, on=merge_cols, how="left")

# total_usd = total_eth * eth_usd_price
df_ledger_usd["total_usd"] = (
    df_ledger_usd["total_eth"]
    .apply(lambda x: Decimal(str(x)))
    * df_ledger_usd["eth_usd_price"]
    .apply(lambda x: Decimal(str(x)))
)

# Order: qty → total_eth → eth_usd_price → total_usd → unit_price_usd
df_ledger_usd = insert_after(df_ledger_usd, "total_eth", "qty")
df_ledger_usd = insert_after(df_ledger_usd, "eth_usd_price", "total_eth")
df_ledger_usd = insert_after(df_ledger_usd, "total_usd", "eth_usd_price")
df_ledger_usd = insert_after(df_ledger_usd, "unit_price_usd", "total_usd")


# ──────────────────────────────────────────────────────────────
# 3) Patch one-for-one conversions (WETH ➜ MWETH-PPG, etc.)
# ──────────────────────────────────────────────────────────────
def patch_one_for_one_conversions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    grp_cols = ["fund_id", "wallet_address", "hash", "date"]

    for _, g in df.groupby(grp_cols):
        if len(g) != 2:
            continue

        buy  = g[g["side"].str.lower() == "buy"]
        sell = g[g["side"].str.lower() == "sell"]
        if buy.empty or sell.empty:
            continue

        buy  = buy.iloc[0]
        sell = sell.iloc[0]

        # Skip if same asset (not a conversion)
        if buy["asset"].upper() == sell["asset"].upper():
            continue

        # Skip if the buy leg is ETH-one-for-one (preserve unit price = 1)
        if buy["asset"].upper() in ETH_one_for_one:
            continue

        # Skip if sell leg isn't ETH-based
        if sell["asset"].upper() not in ETH_one_for_one:
            continue

        idx = buy.name
        total_eth_used = abs(Decimal(str(sell["total_eth"])))
        qty_received   = abs(Decimal(str(buy["qty"])))
        new_unit_price = total_eth_used / qty_received

        df.loc[idx, "total_eth"]                = total_eth_used
        df.loc[idx, "unit_price_eth"]           = new_unit_price
        df.loc[idx, "remaining_cost_basis_eth"] = total_eth_used

    return df


# ──────────────────────────────────────────────────────────────
# 4) Set cost basis = qty for pure 1:1 ETH-based tokens
# ──────────────────────────────────────────────────────────────
mask_one_for_one = df_ledger_eth["asset"].str.upper().isin(ETH_one_for_one)
df_ledger_eth.loc[mask_one_for_one, "remaining_cost_basis_eth"] = (
    df_ledger_eth.loc[mask_one_for_one, "remaining_qty"]
)

# ──────────────────────────────────────────────────────────────
# 5) Reorder: qty → total_eth → price_eth → unit_price_eth
# ──────────────────────────────────────────────────────────────

df_ledger_eth = insert_after(df_ledger_eth, "total_eth", "qty")
df_ledger_eth = insert_after(df_ledger_eth, "unit_price_eth", "price_eth")

if "price_eth" in df_ledger_usd.columns:
    df_ledger_usd["eth_usd_price"] = df_ledger_usd["price_eth"]
    df_ledger_usd.drop(columns=["price_eth"], inplace=True)
    df_ledger_usd.rename(columns={"eth_usd_price": "price_eth"}, inplace=True)

# ──────────────────────────────────────────────────────────────
# 6) Save
# ──────────────────────────────────────────────────────────────
save_master_ledger(MASTER_PATH_ETH, df_ledger_eth)
save_master_ledger(MASTER_PATH_USD, df_ledger_usd)

print(f"✅ Built ETH ledger: {len(df_ledger_eth)} rows")
print(f"✅ Built USD ledger: {len(df_ledger_usd)} rows")

from google.colab import files
files.download(MASTER_PATH_ETH)
#files.download(MASTER_PATH_USD)

target_hash = "0x39a7bbae22e0a6e97ca074851fa274b0eec04f9ef349ca13c7c55d151886de45"

# Pull the relevant rows
df_tx = df_ledger_usd[df_ledger_usd["hash"] == target_hash].copy()
display(df_tx[["date", "asset", "side", "qty", "price_eth"]])
