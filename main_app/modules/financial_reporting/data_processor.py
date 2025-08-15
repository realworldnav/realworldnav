"""
Data Processor for Financial Reporting
Handles period calculations, ETH price conversions, and common data transformations
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
from functools import lru_cache


def get_period_dates(current_date: datetime) -> Dict[str, Tuple[datetime, datetime]]:
    """
    Calculate start and end dates for different reporting periods.
    
    Args:
        current_date: The reporting date
        
    Returns:
        Dictionary with period names as keys and (start_date, end_date) tuples
    """
    # Month to Date
    mtd_start = current_date.replace(day=1)
    
    # Quarter to Date
    quarter = ((current_date.month - 1) // 3) + 1
    qtd_start = datetime(current_date.year, (quarter - 1) * 3 + 1, 1)
    
    # Year to Date
    ytd_start = datetime(current_date.year, 1, 1)
    
    return {
        'MTD': (mtd_start, current_date),
        'QTD': (qtd_start, current_date),
        'YTD': (ytd_start, current_date),
        'ITD': (None, current_date)  # ITD start will be determined from data
    }


def get_previous_period_date(current_date: datetime, period_type: str = 'month') -> datetime:
    """
    Get the last day of the previous period.
    
    Args:
        current_date: Current reporting date
        period_type: 'month', 'quarter', or 'year'
    """
    if period_type == 'month':
        # Last day of previous month
        return current_date.replace(day=1) - timedelta(days=1)
    elif period_type == 'quarter':
        quarter = ((current_date.month - 1) // 3) + 1
        quarter_start = datetime(current_date.year, (quarter - 1) * 3 + 1, 1)
        return quarter_start - timedelta(days=1)
    elif period_type == 'year':
        return datetime(current_date.year - 1, 12, 31)
    else:
        return current_date.replace(day=1) - timedelta(days=1)


def format_currency(
    amount: float,
    currency: str = 'ETH',
    decimal_places: Optional[int] = None
) -> str:
    """
    Format currency amount for display.
    
    Args:
        amount: The amount to format
        currency: 'ETH' or 'USD'
        decimal_places: Override default decimal places
    """
    if currency == 'ETH':
        decimals = decimal_places or 6
        return f"{amount:,.{decimals}f}"
    else:  # USD
        decimals = decimal_places or 2
        return f"${amount:,.{decimals}f}"


def get_account_category(acct_num: int) -> str:
    """
    Determine account category based on account number.
    
    Account number ranges:
    - 1xxx: Assets
    - 2xxx: Liabilities
    - 3xxx: Partners' Capital / Equity
    - 4xxx: Income / Revenue
    - 8xxx: Expenses
    - 9xxx: Other Income
    """
    acct_str = str(acct_num)
    
    if acct_str.startswith('1'):
        return "Assets"
    elif acct_str.startswith('2'):
        return "Liabilities"
    elif acct_str.startswith('3'):
        return "Partners' Capital"
    elif acct_str.startswith('4'):
        return "Income"
    elif acct_str.startswith('8'):
        return "Expenses"
    elif acct_str.startswith('9'):
        return "Income"
    else:
        return "Other"


def calculate_account_balance(
    debit_total: float,
    credit_total: float,
    account_type: str
) -> float:
    """
    Calculate account balance based on normal balance rules.
    
    Args:
        debit_total: Sum of debits
        credit_total: Sum of credits
        account_type: Account category
        
    Returns:
        Net balance with proper sign
    """
    if account_type == "Assets":
        # Assets have normal debit balance
        return debit_total - credit_total
    elif account_type in ["Liabilities", "Partners' Capital", "Income"]:
        # These have normal credit balance
        return credit_total - debit_total
    elif account_type == "Expenses":
        # Expenses have normal debit balance
        return debit_total - credit_total
    else:
        # Default to debit normal
        return debit_total - credit_total


def filter_zero_balances(
    df: pd.DataFrame,
    balance_columns: list,
    threshold: float = 0.000001
) -> pd.DataFrame:
    """
    Filter out rows where all balance columns are effectively zero.
    
    Args:
        df: DataFrame to filter
        balance_columns: List of column names to check
        threshold: Minimum absolute value to consider non-zero
    """
    if df.empty:
        return df
    
    # Check if any balance column has a non-zero value
    mask = pd.Series(False, index=df.index)
    for col in balance_columns:
        if col in df.columns:
            mask |= (np.abs(df[col]) > threshold)
    
    return df[mask].copy()


def get_eth_price(date: datetime, prices_df: Optional[pd.DataFrame] = None) -> float:
    """
    Get ETH price for a specific date.
    
    Args:
        date: Date to get price for
        prices_df: Optional DataFrame with ETH prices
        
    Returns:
        ETH price in USD
    """
    # For now, return a default price
    # In production, this would load from S3 or external API
    default_price = 2400.0
    
    if prices_df is not None and not prices_df.empty:
        # Try to find price for specific date
        date_str = date.strftime('%Y-%m-%d')
        if date_str in prices_df.index:
            return prices_df.loc[date_str, 'price']
    
    return default_price


def aggregate_by_period(
    df: pd.DataFrame,
    date_column: str,
    value_columns: list,
    period: str = 'M'
) -> pd.DataFrame:
    """
    Aggregate data by time period.
    
    Args:
        df: DataFrame with time series data
        date_column: Name of date column
        value_columns: List of columns to aggregate
        period: Pandas period string ('D', 'M', 'Q', 'Y')
    """
    if df.empty:
        return df
    
    df = df.copy()
    df[date_column] = pd.to_datetime(df[date_column])
    df.set_index(date_column, inplace=True)
    
    # Resample and sum
    aggregated = df[value_columns].resample(period).sum()
    
    return aggregated.reset_index()


def calculate_percentage_change(
    current: float,
    previous: float,
    decimals: int = 2
) -> Optional[float]:
    """
    Calculate percentage change between two values.
    
    Args:
        current: Current value
        previous: Previous value
        decimals: Number of decimal places
        
    Returns:
        Percentage change or None if previous is 0
    """
    if previous == 0:
        return None
    
    pct_change = ((current - previous) / abs(previous)) * 100
    return round(pct_change, decimals)


def consolidate_accounts(
    df: pd.DataFrame,
    consolidation_rules: Dict[str, list],
    account_column: str = 'GL_Acct_Number'
) -> pd.DataFrame:
    """
    Consolidate multiple accounts into single line items.
    
    Args:
        df: DataFrame with account data
        consolidation_rules: Dict mapping new account names to list of account numbers
        account_column: Name of account number column
    """
    if df.empty:
        return df
    
    consolidated_data = []
    processed_accounts = set()
    
    for new_name, account_list in consolidation_rules.items():
        # Find matching accounts
        mask = df[account_column].isin(account_list)
        matching = df[mask]
        
        if not matching.empty:
            # Sum numeric columns
            numeric_cols = matching.select_dtypes(include=[np.number]).columns
            consolidated_row = {col: matching[col].sum() for col in numeric_cols}
            consolidated_row['GL_Acct_Name'] = new_name
            consolidated_data.append(consolidated_row)
            processed_accounts.update(matching[account_column].tolist())
    
    # Add unconsolidated accounts
    unconsolidated = df[~df[account_column].isin(processed_accounts)]
    
    result = pd.concat([
        pd.DataFrame(consolidated_data),
        unconsolidated
    ], ignore_index=True)
    
    return result


def validate_gl_data(gl_df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Validate that GL data has required columns and data.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    required_columns = ['date', 'GL_Acct_Number', 'debit_crypto', 'credit_crypto']
    
    # Check for required columns
    missing_columns = [col for col in required_columns if col not in gl_df.columns]
    if missing_columns:
        return False, f"Missing required columns: {', '.join(missing_columns)}"
    
    # Check for data
    if gl_df.empty:
        return False, "GL data is empty"
    
    # Check for valid dates
    if gl_df['date'].isna().all():
        return False, "No valid dates in GL data"
    
    return True, ""


def safe_date_compare(df_date_column: pd.Series, compare_date) -> pd.Series:
    """
    Safely compare pandas datetime column with datetime object.
    Handles timezone-aware datetime comparisons properly.
    
    Args:
        df_date_column: Pandas Series with datetime data
        compare_date: datetime, pd.Timestamp, or date object to compare against
        
    Returns:
        Boolean Series for filtering
    """
    import pandas as pd
    from datetime import datetime, date
    
    # Ensure the datetime column is in pandas datetime format
    if not pd.api.types.is_datetime64_any_dtype(df_date_column):
        df_date_column = pd.to_datetime(df_date_column)
    
    # Convert compare_date to pandas Timestamp with timezone handling
    if isinstance(compare_date, date) and not isinstance(compare_date, datetime):
        # Handle pure date objects
        compare_date = pd.Timestamp(compare_date)
    elif isinstance(compare_date, datetime):
        # Handle datetime objects
        compare_date = pd.Timestamp(compare_date)
    elif isinstance(compare_date, str):
        # Handle string dates
        compare_date = pd.Timestamp(compare_date)
    elif not isinstance(compare_date, pd.Timestamp):
        # Try to convert anything else
        compare_date = pd.Timestamp(compare_date)
    
    # If the df column has timezone info, make sure compare_date is compatible
    try:
        if df_date_column.dt.tz is not None:
            if compare_date.tz is None:
                # Add UTC timezone to compare_date if df column has timezone
                compare_date = compare_date.tz_localize('UTC')
            elif compare_date.tz != df_date_column.dt.tz:
                # Convert timezone if they don't match
                compare_date = compare_date.tz_convert(df_date_column.dt.tz)
        elif compare_date.tz is not None:
            # Remove timezone from compare_date if df column has no timezone
            compare_date = compare_date.tz_localize(None)
        
        return df_date_column <= compare_date
    except Exception as e:
        print(f"DEBUG - Date comparison error: df_tz={df_date_column.dt.tz}, compare_date={compare_date} (type: {type(compare_date)})")
        print(f"DEBUG - Error details: {e}")
        # Fallback - try to convert both to naive datetime for comparison
        try:
            df_naive = df_date_column.dt.tz_localize(None) if df_date_column.dt.tz is not None else df_date_column
            compare_naive = compare_date.tz_localize(None) if hasattr(compare_date, 'tz') and compare_date.tz is not None else compare_date
            return df_naive <= compare_naive
        except Exception as fallback_error:
            print(f"DEBUG - Fallback comparison also failed: {fallback_error}")
            raise e


def get_amount_columns(currency: str = "ETH") -> tuple:
    """
    Get the appropriate debit/credit column names based on currency.
    
    Args:
        currency: Either 'ETH' or 'USD'
        
    Returns:
        Tuple of (debit_column, credit_column)
    """
    if currency.upper() == "USD":
        return ("debit_USD", "credit_USD")
    else:
        return ("debit_crypto", "credit_crypto")