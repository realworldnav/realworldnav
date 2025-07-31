import boto3
import pandas as pd
from functools import lru_cache
from io import BytesIO
from io import StringIO
import io

# -- Configure your bucket and TB file here
BUCKET_NAME = "realworldnav-beta"
TB_KEY = "drip_capital/fund/holdings_class_B_ETH/master_tb/20241231_master_tb_for_holdings_class_B_ETH.csv"
GL_KEY = "drip_capital/all_posted_journal_entries.parquet"
COA_KEY = "drip_capital/drip_capital_COA.csv"
WALLET_KEY = "drip_capital/drip_capital_wallet_ID_mapping.xlsx"

# -- Create a reusable S3 client
s3 = boto3.client("s3")

def get_master_tb_key() -> str:
    """Return the fixed key for the master trial balance."""
    return TB_KEY

def get_master_gl_key() -> str:
    """Return the fixed key for the master general ledger."""
    return GL_KEY

@lru_cache(maxsize=32)
def load_tb_file(key: str = TB_KEY) -> pd.DataFrame:
    """Load a TB file from S3 as a DataFrame."""
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    content = obj["Body"].read()
    if key.endswith(".csv"):
        return pd.read_csv(BytesIO(content))
    elif key.endswith(".xlsx"):
        return pd.read_excel(BytesIO(content))
    else:
        raise ValueError(f"Unsupported file type for key: {key}")
    
@lru_cache
def load_COA_file(key: str = COA_KEY) -> pd.DataFrame:
    s3 = boto3.client("s3", region_name="us-east-2")
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    data = obj["Body"].read().decode("utf-8")

    df_coa = pd.read_csv(StringIO(data))
    df_coa.columns = df_coa.columns.str.strip()
    df_coa["GL_Acct_Number"] = df_coa["GL_Acct_Number"].astype(int)
    df_coa["GL_Acct_Name"] = df_coa["GL_Acct_Name"].astype(str)
    return df_coa


    
def save_GL_file(df: pd.DataFrame, key: str = GL_KEY):
    # Retrieve stored dtypes
    original_dtypes = df.attrs.get("dtypes")
    if original_dtypes:
        for col, dtype in original_dtypes.items():
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                print(f"⚠️ Could not cast {col} to {dtype}: {e}")
    else:
        print("⚠️ No dtypes found in attrs — skipping conversion.")

    # Save to Parquet
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buffer.getvalue())

@lru_cache(maxsize=32)
def load_GL_file(key: str = GL_KEY) -> pd.DataFrame:
    """Load a GL file from S3 as a DataFrame and assign unique transaction IDs."""
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    content = obj["Body"].read()

    if key.endswith(".parquet"):
        df = pd.read_parquet(BytesIO(content))
        df.attrs["dtypes"] = df.dtypes.to_dict()
    elif key.endswith(".xlsx"):
        df = pd.read_excel(BytesIO(content))
    else:
        raise ValueError(f"Unsupported file type for key: {key}")

    # ✅ Assign unique transaction_id if not already present
    if "transaction_id" not in df.columns:
        df = df.reset_index(drop=True)
        df["transaction_id"] = df.index.astype(str)
        print("🆕 Assigned transaction_id to GL DataFrame")

    return df


def append_audit_log(changes_df: pd.DataFrame, key="drip_capital/gl_edit_log.csv"):
    """Append GL changes to a running audit log in S3."""
    try:
        # Download existing log (if exists)
        existing = pd.read_csv(BytesIO(s3.get_object(Bucket=BUCKET_NAME, Key=key)["Body"].read()))
    except s3.exceptions.NoSuchKey:
        existing = pd.DataFrame()

    final_df = pd.concat([existing, changes_df], ignore_index=True)

    buf = BytesIO()
    final_df.to_csv(buf, index=False)
    buf.seek(0)
    s3.upload_fileobj(buf, Bucket=BUCKET_NAME, Key=key)

@lru_cache(maxsize=32)
def load_WALLET_file(key: str = WALLET_KEY) -> pd.DataFrame:
    """Load a TB file from S3 as a DataFrame."""
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    content = obj["Body"].read()
    if key.endswith(".xlsx"):
        return pd.read_excel(BytesIO(content))
