import boto3
import pandas as pd
from functools import lru_cache
from io import BytesIO
from io import StringIO
import io
import pyarrow.parquet as pq
from decimal import Decimal, InvalidOperation
import re
import json
import logging

logger = logging.getLogger(__name__)

def safe_to_decimal(value):
    """Safely convert a value to Decimal, handling various edge cases"""
    if pd.isna(value) or value is None:
        return Decimal('0')
    
    # Convert to string and clean up
    str_value = str(value).strip().lower()
    
    # Handle common invalid cases
    if str_value in ['', 'nan', 'none', 'null', 'na']:
        return Decimal('0')
    
    try:
        # Try direct conversion
        return Decimal(str_value)
    except (ValueError, TypeError, InvalidOperation):
        # If conversion fails, try to extract numeric part
        # Extract numeric characters, decimal point, and minus sign
        numeric_part = re.sub(r'[^0-9.-]', '', str_value)
        
        if numeric_part and numeric_part not in ['-', '.', '-.']:
            try:
                return Decimal(numeric_part)
            except (ValueError, TypeError, InvalidOperation):
                pass
        
        # If all else fails, return 0
        return Decimal('0')

# -- Configure your bucket and TB file here
BUCKET_NAME = "realworldnav-beta"
TB_KEY = "drip_capital/fund/holdings_class_B_ETH/master_tb/20241231_master_tb_for_holdings_class_B_ETH.csv"
GL_KEY = "drip_capital/all_posted_journal_entries.parquet"
COA_KEY = "drip_capital/drip_capital_COA.csv"
WALLET_KEY = "drip_capital/drip_capital_wallet_ID_mapping.xlsx"
NFT_LEDGER_KEY = "drip_capital/master_nft_ledger_ETH.csv"
LP_COMMITMENTS_KEY = "drip_capital/LP_commitments.csv"
GP_INCENTIVE_AUDIT_KEY = "drip_capital/20240731_fund_i_class_B_ETH_GP_incentive_audit_trail.xlsx"
APPROVED_TOKENS_KEY = "drip_capital/user_approved_tokens.csv"
REJECTED_TOKENS_KEY = "drip_capital/user_rejected_tokens.csv"
PCAP_EXCEL_PREFIX = "drip_capital/PCAP/"  # Prefix for PCAP Excel files
FIFO_LEDGER_KEY = "drip_capital/fifo_ledger_results.parquet"
ABI_PREFIX = "drip_capital/smart_contract_abis/"  # Prefix for contract ABIs

# -- Create a reusable S3 client
# Create S3 client - will be initialized when first used
s3 = None

def get_s3_client():
    """Get or create S3 client"""
    global s3
    if s3 is None:
        s3 = boto3.client("s3")
    return s3

def get_master_tb_key() -> str:
    """Return the fixed key for the master trial balance."""
    return TB_KEY

def get_master_gl_key() -> str:
    """Return the fixed key for the master general ledger."""
    return GL_KEY

@lru_cache(maxsize=128)
def load_abi_from_s3(contract_address: str) -> dict:
    """
    Load contract ABI from S3.
    ABIs are stored as JSON files named by contract address.

    Args:
        contract_address: Ethereum contract address (checksummed or not)

    Returns:
        Contract ABI as dict, or empty dict if not found
    """
    # Normalize address (lowercase, remove 0x prefix if present)
    address = contract_address.lower().replace('0x', '')

    # Try different naming conventions
    possible_keys = [
        f"{ABI_PREFIX}{address}.json",
        f"{ABI_PREFIX}0x{address}.json",
        f"{ABI_PREFIX}{address.upper()}.json",
        f"{ABI_PREFIX}0x{address.upper()}.json",
    ]

    s3_client = get_s3_client()

    for key in possible_keys:
        try:
            obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
            abi_content = obj["Body"].read().decode("utf-8")
            abi = json.loads(abi_content)
            logger.info(f"Loaded ABI from S3: {key}")
            return abi
        except s3_client.exceptions.NoSuchKey:
            continue
        except Exception as e:
            logger.warning(f"Error loading ABI from {key}: {e}")
            continue

    logger.warning(f"ABI not found in S3 for address: {contract_address}")
    return {}

def list_available_abis() -> list:
    """List all available ABIs in S3"""
    s3_client = get_s3_client()
    try:
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=ABI_PREFIX
        )

        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
        return []
    except Exception as e:
        logger.error(f"Error listing ABIs: {e}")
        return []

@lru_cache(maxsize=32)
def load_tb_file(key: str = TB_KEY) -> pd.DataFrame:
    """Load a TB file from S3 as a DataFrame."""
    obj = get_s3_client().get_object(Bucket=BUCKET_NAME, Key=key)
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
    obj = get_s3_client().get_object(Bucket=BUCKET_NAME, Key=key)
    data = obj["Body"].read().decode("utf-8")

    df_coa = pd.read_csv(StringIO(data))
    df_coa.columns = df_coa.columns.str.strip()
    df_coa["GL_Acct_Number"] = df_coa["GL_Acct_Number"].astype(int)
    df_coa["GL_Acct_Name"] = df_coa["GL_Acct_Name"].astype(str)
    return df_coa

def save_COA_file(df: pd.DataFrame, key: str = COA_KEY) -> bool:
    """Save COA DataFrame back to S3 as CSV."""
    try:
        # Convert DataFrame to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        # Upload to S3
        get_s3_client().put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        # Clear the cache since we've updated the data
        load_COA_file.cache_clear()
        
        return True
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return False
    
def save_GL_file(df: pd.DataFrame, key: str = GL_KEY):
    # Retrieve stored dtypes
    original_dtypes = df.attrs.get("dtypes")
    if original_dtypes:
        for col, dtype in original_dtypes.items():
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                # Ignore casting errors
                pass
    else:
        # No dtypes specified, skip conversion
        pass

    # Save to Parquet
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    get_s3_client().put_object(Bucket=BUCKET_NAME, Key=key, Body=buffer.getvalue())

@lru_cache(maxsize=32)
def load_GL_file(key: str = GL_KEY) -> pd.DataFrame:
    """Load a GL file from S3 as a DataFrame and assign unique transaction IDs."""
    obj = get_s3_client().get_object(Bucket=BUCKET_NAME, Key=key)
    content = obj["Body"].read()

    if key.endswith(".parquet"):
        # Use PyArrow for better control over data types
        table = pq.read_table(BytesIO(content))
        df = table.to_pandas()
        
        # Fix datetime columns to be UTC-aware
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], utc=True)
        if "operating_date" in df.columns:
            df["operating_date"] = pd.to_datetime(df["operating_date"], utc=True)
        
        # Cast financial columns to Decimal for precision
        decimal_cols = [
            'debit_crypto', 'credit_crypto', 'debit_USD', 'credit_USD',
            'net_debit_credit_crypto', 'net_debit_credit_USD',
            'eth_usd_price', 'principal_crypto', 'principal_USD',
            'interest_rec_crypto', 'interest_rec_USD',
            'payoff_amount_crypto', 'payoff_amount_USD',
            'annual_interest_rate'
        ]
        
        for col in decimal_cols:
            if col in df.columns:
                df[col] = df[col].apply(safe_to_decimal)
        
        df.attrs["dtypes"] = df.dtypes.to_dict()
    elif key.endswith(".xlsx"):
        df = pd.read_excel(BytesIO(content))
    else:
        raise ValueError(f"Unsupported file type for key: {key}")

    # ✅ Assign unique transaction_id if not already present
    if "transaction_id" not in df.columns:
        df = df.reset_index(drop=True)
        df["transaction_id"] = df.index.astype(str)

    return df


def append_audit_log(changes_df: pd.DataFrame, key="drip_capital/gl_edit_log.csv"):
    """Append GL changes to a running audit log in S3."""
    try:
        # Download existing log (if exists)
        existing = pd.read_csv(BytesIO(get_s3_client().get_object(Bucket=BUCKET_NAME, Key=key)["Body"].read()))
    except get_s3_client().exceptions.NoSuchKey:
        existing = pd.DataFrame()

    final_df = pd.concat([existing, changes_df], ignore_index=True)

    buf = BytesIO()
    final_df.to_csv(buf, index=False)
    buf.seek(0)
    get_s3_client().upload_fileobj(buf, Bucket=BUCKET_NAME, Key=key)

@lru_cache(maxsize=32)
def load_WALLET_file(key: str = WALLET_KEY) -> pd.DataFrame:
    """Load a TB file from S3 as a DataFrame."""
    obj = get_s3_client().get_object(Bucket=BUCKET_NAME, Key=key)
    content = obj["Body"].read()
    if key.endswith(".xlsx"):
        return pd.read_excel(BytesIO(content))

@lru_cache(maxsize=32)
def load_NFT_LEDGER_file(key: str = NFT_LEDGER_KEY) -> pd.DataFrame:
    """
    Load the master NFT ledger from S3 and process it for current NFT holdings.
    
    The ledger contains NFT transaction history with columns:
    ['fund_id', 'wallet_id', 'collateral_address', 'token_id', 'asset',
     'date', 'hash_origination', 'hash_foreclosure', 'hash_monetize', 'hash',
     'side', 'qty', 'unit_price_eth', 'proceeds_eth', 'cost_basis_sold_eth',
     'realized_gain_loss_nft', 'remaining_qty', 'remaining_cost_basis_eth',
     'account_name']
    
    Returns:
        pd.DataFrame: Processed NFT ledger data with proper date handling
    """
    try:
        obj = get_s3_client().get_object(Bucket=BUCKET_NAME, Key=key)
        content = obj["Body"].read()
        
        # Load the CSV file
        df = pd.read_csv(BytesIO(content))
        
        
        # Convert date column to datetime with UTC timezone awareness
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce')
        
        # Convert numeric columns
        numeric_columns = ['qty', 'unit_price_eth', 'proceeds_eth', 'cost_basis_sold_eth', 
                          'realized_gain_loss_nft', 'remaining_qty', 'remaining_cost_basis_eth']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Clean string columns
        string_columns = ['fund_id', 'wallet_id', 'collateral_address', 'token_id', 
                         'asset', 'side', 'account_name']
        
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        
        # Sort by date to ensure proper chronological order
        if 'date' in df.columns:
            df = df.sort_values('date', ascending=True)
        
        return df
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def get_current_nft_holdings(fund_id: str = None) -> pd.DataFrame:
    """
    Get current NFT holdings by analyzing the NFT ledger.
    
    Logic: If remaining_qty > 0 for a token_id, we currently own that NFT.
    Groups by token_id to get the current state of each NFT.
    
    Args:
        fund_id (str, optional): Filter by specific fund_id
    
    Returns:
        pd.DataFrame: Current NFT holdings with latest transaction info
    """
    try:
        df = load_NFT_LEDGER_file()
        
        if df.empty:
            return pd.DataFrame()
        
        # Filter by fund_id if specified
        if fund_id:
            df = df[df['fund_id'] == fund_id]
        
        # Filter for NFTs we currently own (remaining_qty > 0)
        owned_nfts = df[df['remaining_qty'] > 0].copy()
        
        if owned_nfts.empty:
            return pd.DataFrame()
        
        # Group by token_id and get the most recent transaction for each NFT
        current_holdings = (
            owned_nfts.sort_values('date', ascending=False)
            .groupby(['collateral_address', 'token_id'])
            .first()
            .reset_index()
        )
        
        
        return current_holdings
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

@lru_cache(maxsize=32)
def load_LP_commitments_file(key: str = LP_COMMITMENTS_KEY) -> pd.DataFrame:
    """Load LP commitments from S3 as a DataFrame."""
    try:
        obj = get_s3_client().get_object(Bucket=BUCKET_NAME, Key=key)
        content = obj["Body"].read()
        
        if key.endswith(".csv"):
            df = pd.read_csv(BytesIO(content))
        elif key.endswith(".xlsx"):
            df = pd.read_excel(BytesIO(content))
        else:
            raise ValueError(f"Unsupported file type for key: {key}")
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        
        return df
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

@lru_cache(maxsize=32)
def load_GP_incentive_audit_file(key: str = GP_INCENTIVE_AUDIT_KEY) -> pd.DataFrame:
    """Load GP incentive audit trail from S3 as a DataFrame."""
    try:
        obj = get_s3_client().get_object(Bucket=BUCKET_NAME, Key=key)
        content = obj["Body"].read()
        
        if key.endswith(".xlsx"):
            df = pd.read_excel(BytesIO(content))
        elif key.endswith(".csv"):
            df = pd.read_csv(BytesIO(content))
        else:
            raise ValueError(f"Unsupported file type for key: {key}")
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        
        return df
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

# Token Classification Storage Functions

@lru_cache(maxsize=8)
def load_approved_tokens_file(key: str = APPROVED_TOKENS_KEY) -> set:
    """Load user-approved tokens from S3 as a set of addresses."""
    try:
        obj = get_s3_client().get_object(Bucket=BUCKET_NAME, Key=key)
        content = obj["Body"].read()
        df = pd.read_csv(BytesIO(content))
        
        # Return set of token addresses
        return set(df['token_address'].dropna().astype(str))
        
    except get_s3_client().exceptions.NoSuchKey:
        # File doesn't exist yet, return empty set
        return set()
    except Exception as e:
        print(f"Warning: Failed to load approved tokens from S3: {e}")
        return set()

def save_approved_tokens_file(token_addresses: set, key: str = APPROVED_TOKENS_KEY) -> bool:
    """Save user-approved tokens to S3 as CSV."""
    try:
        # Convert set to DataFrame with metadata
        data = []
        from datetime import datetime, timezone
        current_time = datetime.now(timezone.utc).isoformat()
        
        for addr in token_addresses:
            data.append({
                'token_address': addr,
                'decision_date': current_time,
                'user_action': 'approved'
            })
        
        df = pd.DataFrame(data)
        
        # Save to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        get_s3_client().put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        # Clear cache since we've updated the data
        load_approved_tokens_file.cache_clear()
        return True
        
    except Exception as e:
        print(f"Error saving approved tokens to S3: {e}")
        return False

@lru_cache(maxsize=8)
def load_rejected_tokens_file(key: str = REJECTED_TOKENS_KEY) -> set:
    """Load user-rejected tokens from S3 as a set of addresses."""
    try:
        obj = get_s3_client().get_object(Bucket=BUCKET_NAME, Key=key)
        content = obj["Body"].read()
        df = pd.read_csv(BytesIO(content))
        
        # Return set of token addresses
        return set(df['token_address'].dropna().astype(str))
        
    except get_s3_client().exceptions.NoSuchKey:
        # File doesn't exist yet, return empty set
        return set()
    except Exception as e:
        print(f"Warning: Failed to load rejected tokens from S3: {e}")
        return set()

def save_rejected_tokens_file(token_addresses: set, key: str = REJECTED_TOKENS_KEY) -> bool:
    """Save user-rejected tokens to S3 as CSV."""
    try:
        # Convert set to DataFrame with metadata
        data = []
        from datetime import datetime, timezone
        current_time = datetime.now(timezone.utc).isoformat()
        
        for addr in token_addresses:
            data.append({
                'token_address': addr,
                'decision_date': current_time,
                'user_action': 'rejected'
            })
        
        df = pd.DataFrame(data)
        
        # Save to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        get_s3_client().put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        # Clear cache since we've updated the data
        load_rejected_tokens_file.cache_clear()
        return True
        
    except Exception as e:
        print(f"Error saving rejected tokens to S3: {e}")
        return False

# FIFO Ledger Storage Functions

def save_fifo_ledger_file(fifo_df: pd.DataFrame, positions_df: pd.DataFrame, 
                         journal_df: pd.DataFrame, metadata: dict, 
                         key: str = FIFO_LEDGER_KEY) -> bool:
    """Save FIFO ledger results to S3 with proper dtype preservation."""
    try:
        from datetime import datetime, timezone
        import json
        
        # Base key without extension
        base_key = key.replace('.parquet', '')
        
        # Save each DataFrame separately to preserve dtypes
        # 1. Save FIFO transactions
        if not fifo_df.empty:
            buffer = BytesIO()
            fifo_df.to_parquet(buffer, index=False)
            buffer.seek(0)
            get_s3_client().put_object(
                Bucket=BUCKET_NAME,
                Key=f"{base_key}_transactions.parquet",
                Body=buffer.getvalue()
            )
        
        # 2. Save positions
        if not positions_df.empty:
            buffer = BytesIO()
            positions_df.to_parquet(buffer, index=False)
            buffer.seek(0)
            get_s3_client().put_object(
                Bucket=BUCKET_NAME,
                Key=f"{base_key}_positions.parquet",
                Body=buffer.getvalue()
            )
        
        # 3. Save journal entries
        if not journal_df.empty:
            buffer = BytesIO()
            journal_df.to_parquet(buffer, index=False)
            buffer.seek(0)
            get_s3_client().put_object(
                Bucket=BUCKET_NAME,
                Key=f"{base_key}_journal.parquet",
                Body=buffer.getvalue()
            )
        
        # 4. Save metadata as JSON
        metadata_full = {
            'save_timestamp': datetime.now(timezone.utc).isoformat(),
            'fifo_transaction_count': len(fifo_df),
            'position_count': len(positions_df),
            'journal_entry_count': len(journal_df),
            'has_transactions': not fifo_df.empty,
            'has_positions': not positions_df.empty,
            'has_journal': not journal_df.empty,
            **metadata
        }
        
        metadata_json = json.dumps(metadata_full, indent=2)
        get_s3_client().put_object(
            Bucket=BUCKET_NAME,
            Key=f"{base_key}_metadata.json",
            Body=metadata_json.encode('utf-8'),
            ContentType='application/json'
        )
        
        # Clear cache since we've updated the data
        if hasattr(load_fifo_ledger_file, 'cache_clear'):
            load_fifo_ledger_file.cache_clear()
        
        print(f"Successfully saved FIFO ledger: {len(fifo_df)} transactions, {len(positions_df)} positions")
        return True
        
    except Exception as e:
        print(f"Error saving FIFO ledger to S3: {e}")
        import traceback
        traceback.print_exc()
        return False

@lru_cache(maxsize=8)
def load_fifo_ledger_file(key: str = FIFO_LEDGER_KEY) -> dict:
    """Load FIFO ledger results from S3 with proper dtype preservation."""
    try:
        import json
        
        # Base key without extension
        base_key = key.replace('.parquet', '')
        
        # Load metadata first to check what files exist
        try:
            obj = get_s3_client().get_object(Bucket=BUCKET_NAME, Key=f"{base_key}_metadata.json")
            metadata_json = obj["Body"].read().decode('utf-8')
            metadata = json.loads(metadata_json)
        except get_s3_client().exceptions.NoSuchKey:
            # No metadata file means no saved ledger
            return {
                'fifo_transactions': pd.DataFrame(),
                'fifo_positions': pd.DataFrame(),
                'journal_entries': pd.DataFrame(),
                'metadata': {}
            }
        
        # Load each DataFrame if it exists
        fifo_df = pd.DataFrame()
        positions_df = pd.DataFrame()
        journal_df = pd.DataFrame()
        
        # Define decimal columns for each DataFrame type
        fifo_decimal_cols = [
            'quantity', 'unit_price_eth', 'unit_price_usd', 
            'total_value_eth', 'total_value_usd',
            'cost_basis_eth', 'cost_basis_usd',
            'realized_gain_loss_eth', 'realized_gain_loss_usd'
        ]
        
        position_decimal_cols = [
            'quantity', 'avg_cost_eth', 'avg_cost_usd',
            'current_value_eth', 'current_value_usd',
            'unrealized_pnl_eth', 'unrealized_pnl_usd'
        ]
        
        journal_decimal_cols = [
            'debit_crypto', 'credit_crypto', 'debit_USD', 'credit_USD',
            'net_debit_credit_crypto', 'net_debit_credit_USD',
            'eth_usd_price', 'principal_crypto', 'principal_USD',
            'interest_rec_crypto', 'interest_rec_USD',
            'payoff_amount_crypto', 'payoff_amount_USD',
            'annual_interest_rate'
        ]
        
        # 1. Load FIFO transactions
        if metadata.get('has_transactions', False):
            try:
                obj = get_s3_client().get_object(Bucket=BUCKET_NAME, Key=f"{base_key}_transactions.parquet")
                table = pq.read_table(BytesIO(obj["Body"].read()))
                fifo_df = table.to_pandas()
                
                # Fix datetime columns
                for col in ['date', 'created_at', 'updated_at']:
                    if col in fifo_df.columns:
                        fifo_df[col] = pd.to_datetime(fifo_df[col], utc=True)
                
                # Cast decimal columns
                for col in fifo_decimal_cols:
                    if col in fifo_df.columns:
                        fifo_df[col] = fifo_df[col].apply(lambda x: Decimal(str(x)) if pd.notna(x) else Decimal('0'))
                        
            except Exception as e:
                print(f"Warning: Could not load FIFO transactions: {e}")
        
        # 2. Load positions
        if metadata.get('has_positions', False):
            try:
                obj = get_s3_client().get_object(Bucket=BUCKET_NAME, Key=f"{base_key}_positions.parquet")
                table = pq.read_table(BytesIO(obj["Body"].read()))
                positions_df = table.to_pandas()
                
                # Fix datetime columns
                for col in ['last_update', 'first_purchase_date']:
                    if col in positions_df.columns:
                        positions_df[col] = pd.to_datetime(positions_df[col], utc=True)
                
                # Cast decimal columns
                for col in position_decimal_cols:
                    if col in positions_df.columns:
                        positions_df[col] = positions_df[col].apply(lambda x: Decimal(str(x)) if pd.notna(x) else Decimal('0'))
                        
            except Exception as e:
                print(f"Warning: Could not load positions: {e}")
        
        # 3. Load journal entries
        if metadata.get('has_journal', False):
            try:
                obj = get_s3_client().get_object(Bucket=BUCKET_NAME, Key=f"{base_key}_journal.parquet")
                table = pq.read_table(BytesIO(obj["Body"].read()))
                journal_df = table.to_pandas()
                
                # Fix datetime columns
                for col in ['date', 'operating_date']:
                    if col in journal_df.columns:
                        journal_df[col] = pd.to_datetime(journal_df[col], utc=True)
                
                # Cast decimal columns
                for col in journal_decimal_cols:
                    if col in journal_df.columns:
                        journal_df[col] = journal_df[col].apply(lambda x: Decimal(str(x)) if pd.notna(x) else Decimal('0'))
                        
            except Exception as e:
                print(f"Warning: Could not load journal entries: {e}")
        
        print(f"Successfully loaded FIFO ledger: {len(fifo_df)} transactions, {len(positions_df)} positions")
        
        return {
            'fifo_transactions': fifo_df,
            'fifo_positions': positions_df,
            'journal_entries': journal_df,
            'metadata': metadata
        }
        
    except Exception as e:
        print(f"Warning: Failed to load FIFO ledger from S3: {e}")
        import traceback
        traceback.print_exc()
        return {
            'fifo_transactions': pd.DataFrame(),
            'fifo_positions': pd.DataFrame(),
            'journal_entries': pd.DataFrame(),
            'metadata': {}
        }

def check_fifo_ledger_exists(key: str = FIFO_LEDGER_KEY) -> bool:
    """Check if FIFO ledger metadata file exists in S3."""
    try:
        base_key = key.replace('.parquet', '')
        get_s3_client().head_object(Bucket=BUCKET_NAME, Key=f"{base_key}_metadata.json")
        return True
    except get_s3_client().exceptions.NoSuchKey:
        return False
    except Exception as e:
        print(f"Error checking FIFO ledger existence: {e}")
        return False

def delete_fifo_ledger_file(key: str = FIFO_LEDGER_KEY) -> bool:
    """Delete all FIFO ledger files from S3."""
    try:
        base_key = key.replace('.parquet', '')
        success = True
        
        # Delete all related files
        files_to_delete = [
            f"{base_key}_transactions.parquet",
            f"{base_key}_positions.parquet", 
            f"{base_key}_journal.parquet",
            f"{base_key}_metadata.json"
        ]
        
        for file_key in files_to_delete:
            try:
                get_s3_client().delete_object(Bucket=BUCKET_NAME, Key=file_key)
            except get_s3_client().exceptions.NoSuchKey:
                # File doesn't exist, that's okay
                pass
            except Exception as e:
                print(f"Warning: Could not delete {file_key}: {e}")
                success = False
        
        # Clear cache since we've deleted the data
        if hasattr(load_fifo_ledger_file, 'cache_clear'):
            load_fifo_ledger_file.cache_clear()
        
        return success
    except Exception as e:
        print(f"Error deleting FIFO ledger from S3: {e}")
        return False

# ============= PCAP Excel File Functions =============

def list_pcap_excel_files():
    """List all PCAP Excel files available in S3"""
    try:
        response = get_s3_client().list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=PCAP_EXCEL_PREFIX
        )
        
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                # Only include Excel files
                if key.endswith('.xlsx') or key.endswith('.xls'):
                    filename = key.replace(PCAP_EXCEL_PREFIX, '')
                    # Try to extract date, fund, and info from filename
                    # Expected format: YYYYMMDD_fund_id_PCAP_All_Partners.xlsx
                    parts = filename.split('_')
                    if len(parts) >= 3:
                        date_str = parts[0]
                        # Convert YYYYMMDD to readable date
                        try:
                            date_obj = pd.to_datetime(date_str, format='%Y%m%d')
                            date_formatted = date_obj.strftime('%B %d, %Y')
                        except:
                            date_formatted = date_str
                        
                        # Extract fund ID (everything between date and PCAP)
                        pcap_index = next((i for i, p in enumerate(parts) if 'PCAP' in p), -1)
                        if pcap_index > 0:
                            fund_id = '_'.join(parts[1:pcap_index])
                        else:
                            fund_id = '_'.join(parts[1:-2]) if len(parts) > 3 else 'unknown'
                        
                        files.append({
                            'key': key,
                            'filename': filename,
                            'date': date_str,
                            'date_formatted': date_formatted,
                            'fund_id': fund_id,
                            'size': obj.get('Size', 0),
                            'last_modified': obj.get('LastModified')
                        })
        
        # Sort by date descending (most recent first)
        files.sort(key=lambda x: x['date'], reverse=True)
        return files
        
    except Exception as e:
        print(f"Error listing PCAP Excel files: {e}")
        return []

@lru_cache(maxsize=8)
def load_pcap_excel_file(key: str = None, date: str = None, fund_id: str = None):
    """
    Load a PCAP Excel file from S3
    
    Args:
        key: Full S3 key to the file
        date: Date in YYYYMMDD format (used with fund_id to construct filename)
        fund_id: Fund identifier (used with date to construct filename)
    
    Returns:
        Dictionary with sheet names as keys and DataFrames as values
    """
    try:
        # Determine the key to use
        if key is None:
            if date and fund_id:
                # Construct the filename
                filename = f"{date}_{fund_id}_PCAP_All_Partners.xlsx"
                key = PCAP_EXCEL_PREFIX + filename
            else:
                # Try to get the most recent file
                files = list_pcap_excel_files()
                if files:
                    key = files[0]['key']
                else:
                    print("No PCAP Excel files found in S3")
                    return None
        
        print(f"Loading PCAP Excel file: {key}")
        
        # Download the Excel file from S3
        response = get_s3_client().get_object(Bucket=BUCKET_NAME, Key=key)
        excel_data = BytesIO(response['Body'].read())
        
        # Read all sheets from the Excel file
        excel_file = pd.ExcelFile(excel_data)
        sheets_data = {}
        
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            sheets_data[sheet_name] = df
            print(f"  Loaded sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")
        
        return sheets_data
        
    except Exception as e:
        print(f"Error loading PCAP Excel file: {e}")
        import traceback
        traceback.print_exc()
        return None

def parse_pcap_excel_to_json(sheets_data, lp_id=None):
    """
    Parse PCAP Excel data and convert to JSON format for PDF generation
    
    Args:
        sheets_data: Dictionary of DataFrames from Excel file
        lp_id: Optional LP identifier to filter data for specific LP
    
    Returns:
        Dictionary formatted for PDF generation
    """
    try:
        if not sheets_data:
            return None
        
        # Get the main data sheet (usually first sheet or 'Summary')
        main_sheet = None
        for sheet_name in ['Summary', 'All Partners', 'PCAP']:
            if sheet_name in sheets_data:
                main_sheet = sheets_data[sheet_name]
                break
        
        if main_sheet is None:
            # Use the first sheet if no standard name found
            main_sheet = sheets_data[list(sheets_data.keys())[0]]
        
        # If LP-specific sheet exists, use that
        if lp_id and lp_id in sheets_data:
            main_sheet = sheets_data[lp_id]
        
        # Extract data from the sheet
        # This will need to be customized based on actual Excel structure
        # For now, creating a template structure
        
        # Try to find date from sheet data or use current date
        date_str = "December 31, 2024"  # Default, should extract from Excel
        
        # Extract statement of changes data
        statement_of_changes = []
        
        # Map Excel rows to statement items
        # This mapping will need to be adjusted based on actual Excel structure
        line_items_map = {
            'Beginning Balance': 'Beginning Balance',
            'Capital contributions': 'Capital contributions',
            'Management fees': 'Management fees',
            'Interest expense': 'Interest expense',
            'Capital distributions': 'Capital distributions',
            'Other income': 'Other income',
            'Operating expenses': 'Operating expenses',
            'Interest income': 'Interest income',
            'Provision for bad debt': 'Provision for bad debt',
            'Income allocated from investments': 'Income allocated from investments',
            'Realized gain (loss)': 'Realized gain (loss)',
            'Change in unrealized gain (loss)': 'Change in unrealized gain (loss)',
            'Ending Capital': 'Ending Capital'
        }
        
        # Process each line item
        for excel_label, json_label in line_items_map.items():
            # Find matching row in Excel
            matching_rows = main_sheet[main_sheet.iloc[:, 0].astype(str).str.contains(excel_label, case=False, na=False)]
            
            if not matching_rows.empty:
                row = matching_rows.iloc[0]
                # Assuming columns are: Description, MTD, QTD, YTD, ITD
                statement_of_changes.append({
                    'label': json_label,
                    'mtd': float(row.iloc[1]) if len(row) > 1 and pd.notna(row.iloc[1]) else 0.0,
                    'qtd': float(row.iloc[2]) if len(row) > 2 and pd.notna(row.iloc[2]) else 0.0,
                    'ytd': float(row.iloc[3]) if len(row) > 3 and pd.notna(row.iloc[3]) else 0.0,
                    'itd': float(row.iloc[4]) if len(row) > 4 and pd.notna(row.iloc[4]) else 0.0
                })
            else:
                # Add zero values if not found
                statement_of_changes.append({
                    'label': json_label,
                    'mtd': 0.0,
                    'qtd': 0.0,
                    'ytd': 0.0,
                    'itd': 0.0
                })
        
        # Extract commitment summary
        commitment_summary = {
            'Total commitments': '0.000000',
            'Capital called': '0.000000',
            'Remaining commitments': '0.000000'
        }
        
        # Extract performance metrics
        performance_metrics = {
            'Net IRR': '0.00%',
            'Gross MOIC': '0.000000',
            'NAV per unit': '0.000000'
        }
        
        # Build the JSON structure
        json_data = {
            'main_date': date_str,
            'currency': 'ETH',
            'statement_of_changes': statement_of_changes,
            'commitment_summary': commitment_summary,
            'performance_metrics': performance_metrics
        }
        
        return json_data
        
    except Exception as e:
        print(f"Error parsing PCAP Excel to JSON: {e}")
        import traceback
        traceback.print_exc()
        return None
