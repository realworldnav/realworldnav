import boto3
import pandas as pd
from functools import lru_cache
from io import BytesIO
from io import StringIO

# -- Configure your bucket and TB file here
BUCKET_NAME = "realworldnav-prod"
TB_KEY = "drip_capital/fund/holdings_class_B_ETH/master_tb/20241231_master_tb_for_holdings_class_B_ETH.csv"

# -- Create a reusable S3 client
s3 = boto3.client("s3")

def get_master_tb_key() -> str:
    """Return the fixed key for the master trial balance."""
    return TB_KEY

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
def load_coa_file():
    bucket = "realworldnav-prod"
    key = "drip_capital/drip_capital_COA.csv"

    s3 = boto3.client("s3", region_name="us-east-2")
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read().decode("utf-8")

    df_coa = pd.read_csv(StringIO(data))
    df_coa.columns = df_coa.columns.str.strip()
    df_coa["GL_Acct_Number"] = df_coa["GL_Acct_Number"].astype(int)
    df_coa["GL_Acct_Name"] = df_coa["GL_Acct_Name"].astype(str)
    return df_coa
