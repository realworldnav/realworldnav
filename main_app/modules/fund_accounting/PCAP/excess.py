"""
Utility functions extracted from PCAP calculations
Contains general-purpose financial, date, and formatting utilities
"""

import pandas as pd
import numpy as np
from decimal import Decimal, InvalidOperation, getcontext
from datetime import datetime, timedelta
import math
import warnings
import os
from typing import Dict, List, Optional, Union

# Report generation imports
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

# Optional reportlab imports for PDF generation
try:
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.pagesizes import LETTER
    from reportlab.lib.units import inch
    from reportlab.lib import colors
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False
    print("ReportLab not available - PDF generation will be disabled")

# Set high precision for financial calculations
getcontext().prec = 28


def normalize_to_eod_utc(series):
    """
    Force datetime series to UTC, strip time, then set to one second before midnight
    """
    return (
        pd.to_datetime(series, utc=True)
          .dt.tz_convert('UTC')
          .dt.normalize()
          .add(pd.Timedelta(hours=23, minutes=59, seconds=59))
    )


def ensure_timezone_aware(dt_obj, default_tz='UTC'):
    """
    Ensure a datetime object is timezone-aware, adding default timezone if naive
    """
    if dt_obj is None:
        return None
    
    # Handle pandas Timestamp
    if isinstance(dt_obj, pd.Timestamp):
        if dt_obj.tz is None:
            return dt_obj.tz_localize(default_tz)
        return dt_obj
    
    # Handle regular datetime
    if isinstance(dt_obj, datetime):
        if dt_obj.tzinfo is None:
            return pd.Timestamp(dt_obj).tz_localize(default_tz)
        return pd.Timestamp(dt_obj)
    
    # Handle string dates
    if isinstance(dt_obj, str):
        parsed = pd.to_datetime(dt_obj)
        if parsed.tz is None:
            return parsed.tz_localize(default_tz)
        return parsed
    
    # Handle date objects
    if hasattr(dt_obj, 'date'):
        dt_combined = datetime.combine(dt_obj, datetime.min.time())
        return pd.Timestamp(dt_combined).tz_localize(default_tz)
    
    return dt_obj


def localize_date_columns_to_utc(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Localize all date columns in DataFrame to UTC timezone
    """
    df_copy = df.copy()
    date_columns = []
    
    for col in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            date_columns.append(col)
            if df_copy[col].dt.tz is None:
                df_copy[col] = df_copy[col].dt.tz_localize('UTC')
            else:
                df_copy[col] = df_copy[col].dt.tz_convert('UTC')
    
    if verbose and date_columns:
        print(f"Localized {len(date_columns)} date columns to UTC: {date_columns}")
    
    return df_copy


def safe_decimal(val, fallback=Decimal("0")):
    """Safely convert value to Decimal with fallback"""
    try:
        if val is None:
            return fallback
        if isinstance(val, float) and math.isnan(val):
            return fallback
        return Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return fallback


def safe_decimal_mul(x, rate):
    """Safely multiply values converting to Decimal"""
    if x is None or rate is None:
        return Decimal('0')
    
    try:
        if isinstance(x, float) and math.isnan(x):
            return Decimal('0')
        if isinstance(rate, float) and math.isnan(rate):
            return Decimal('0')
        
        return Decimal(str(x)) * Decimal(str(rate))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal('0')


def safe_pct_change(curr, prev):
    """Safely calculate percentage change between values"""
    try:
        curr_dec = safe_decimal(curr)
        prev_dec = safe_decimal(prev)
        
        if prev_dec == 0:
            return Decimal('0') if curr_dec == 0 else Decimal('1')  # 100% change if prev was 0
        
        return (curr_dec - prev_dec) / prev_dec
    except:
        return Decimal('0')


def xnpv(rate: float, cashflows, as_of_date):
    """
    Calculate extended net present value with actual dates
    """
    xnpv_val = Decimal('0')
    for cf_date, amount in cashflows:
        days_diff = (cf_date - as_of_date).days
        discount_factor = (1 + Decimal(str(rate))) ** (Decimal(str(days_diff)) / Decimal('365'))
        xnpv_val += Decimal(str(amount)) / discount_factor
    return xnpv_val


def xirr(cashflows: list, as_of_date: datetime, guess: Decimal = Decimal("0.1")) -> Decimal:
    """
    Calculate extended internal rate of return using Newton-Raphson method
    """
    x0 = guess
    for _ in range(100):  # Max iterations
        fval = xnpv(float(x0), cashflows, as_of_date)
        
        # Calculate derivative
        h = Decimal("0.0001")
        fval_h = xnpv(float(x0 + h), cashflows, as_of_date)
        fder = (fval_h - fval) / h
        
        if abs(fder) < Decimal("1e-12"):
            break
            
        x1 = x0 - fval / fder
        
        if abs(x1 - x0) < Decimal("1e-12"):
            return x1
            
        x0 = x1
    
    return x0


def compound_forward(value: Decimal, start_date: datetime, end_date: datetime, annual_rate: Decimal) -> Decimal:
    """Compound a value forward between dates at annual rate"""
    days = (end_date - start_date).days
    return value * ((1 + annual_rate) ** (Decimal(str(days)) / Decimal('365')))


def get_active_gp_carry_rate(commit_row, as_of_date):
    """Get the active GP carry rate for a given commitment and date"""
    # Default carry rate if not specified
    default_rate = Decimal('0.20')  # 20%
    
    if commit_row is None or commit_row.empty:
        return default_rate
    
    # Extract carry rate from commitment data
    carry_rate = commit_row.get('carry_rate', default_rate)
    
    # Convert to Decimal if needed
    if not isinstance(carry_rate, Decimal):
        carry_rate = safe_decimal(carry_rate, default_rate)
    
    return carry_rate


def get_account_classification(scpc):
    """Classify SCPC by account type for accounting treatment"""
    if pd.isna(scpc):
        return "Other"
    
    scpc_str = str(scpc).lower()
    
    if any(keyword in scpc_str for keyword in ['asset', 'cash', 'receivable', 'investment']):
        return "Asset"
    elif any(keyword in scpc_str for keyword in ['liability', 'payable', 'debt']):
        return "Liability"
    elif any(keyword in scpc_str for keyword in ['equity', 'capital', 'retained']):
        return "Equity"
    elif any(keyword in scpc_str for keyword in ['revenue', 'income', 'gain']):
        return "Revenue"
    elif any(keyword in scpc_str for keyword in ['expense', 'cost', 'loss']):
        return "Expense"
    else:
        return "Other"


def get_normal_balance(account_type):
    """Return normal balance type for account classification"""
    normal_balances = {
        "Asset": "Debit",
        "Expense": "Debit",
        "Liability": "Credit",
        "Equity": "Credit",
        "Revenue": "Credit",
        "Other": "Credit"  # Default conservative approach
    }
    return normal_balances.get(account_type, "Credit")


def format_accounting_amount(amount, account_type=None):
    """Format amounts using standard accounting notation"""
    if pd.isna(amount) or amount == 0:
        return "-"
    
    abs_amount = abs(float(amount))
    formatted = f"${abs_amount:,.2f}"
    
    if amount < 0:
        return f"({formatted})"
    return formatted


def validate_account_balance_nature(amount, account_type):
    """Check if account balance aligns with normal balance"""
    if pd.isna(amount) or amount == 0:
        return True
    
    normal_balance = get_normal_balance(account_type)
    
    if normal_balance == "Debit" and amount < 0:
        return False  # Debit accounts shouldn't have negative balances typically
    elif normal_balance == "Credit" and amount > 0:
        return False  # Credit accounts shouldn't have positive balances typically
    
    return True


def save_cash_flow_waterfall_image_from_row(lp_id, as_of_date, lp_row, save_dir, unit_label="ETH", preview=False):
    """Generate professional waterfall charts for GP/LP breakdown"""
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Extract values from lp_row
    contrib = float(lp_row.get('total_contributions', 0))
    distributions = float(lp_row.get('total_distributions', 0))
    unrealized = float(lp_row.get('unrealized_gains', 0))
    fees = float(lp_row.get('management_fees', 0))
    carry = float(lp_row.get('carried_interest', 0))
    
    # Create waterfall data
    categories = ['Contributions', 'Distributions', 'Unrealized\nGains', 'Mgmt Fees', 'Carried\nInterest', 'Net Position']
    values = [contrib, -distributions, unrealized, -fees, -carry, 0]
    
    # Calculate cumulative for final position
    values[-1] = sum(values[:-1])
    
    # Create colors
    colors = ['green' if v >= 0 else 'red' for v in values[:-1]] + ['blue']
    
    # Create waterfall chart
    x_pos = range(len(categories))
    cumulative = 0
    bar_heights = []
    bar_bottoms = []
    
    for i, val in enumerate(values):
        if i == len(values) - 1:  # Final bar
            bar_bottoms.append(0)
            bar_heights.append(val)
        else:
            bar_bottoms.append(cumulative if val < 0 else cumulative)
            bar_heights.append(abs(val))
            cumulative += val
    
    # Plot bars
    bars = ax.bar(x_pos, bar_heights, bottom=bar_bottoms, color=colors, alpha=0.7, edgecolor='black')
    
    # Add value labels
    for i, (bar, val) in enumerate(zip(bars, values)):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., bar.get_y() + height/2.,
                f'{val:,.2f}', ha='center', va='center', fontweight='bold')
    
    # Customize chart
    ax.set_xlabel('Cash Flow Components')
    ax.set_ylabel(f'Amount ({unit_label})')
    ax.set_title(f'LP {lp_id} - Cash Flow Waterfall as of {as_of_date.strftime("%Y-%m-%d")}')
    ax.set_xticks(x_pos)
    ax.set_xticklabels(categories, rotation=45, ha='right')
    
    # Add grid
    ax.grid(True, alpha=0.3)
    ax.axhline(y=0, color='black', linewidth=0.8)
    
    plt.tight_layout()
    
    if preview:
        plt.show()
    else:
        # Save to file
        filename = f"waterfall_{lp_id}_{as_of_date.strftime('%Y%m%d')}.png"
        filepath = os.path.join(save_dir, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        return filepath


def build_lp_pdf_report_clean(lp_row, fund_id, as_of_date, save_dir):
    """Build comprehensive LP PDF reports with waterfall analysis"""
    if not REPORTLAB_AVAILABLE:
        print("ReportLab not available - cannot generate PDF reports")
        return None
    
    # Create filename
    lp_id = lp_row.get('limited_partner_ID', 'Unknown')
    filename = f"LP_Report_{lp_id}_{as_of_date.strftime('%Y%m%d')}.pdf"
    filepath = os.path.join(save_dir, filename)
    
    # Create PDF document
    doc = SimpleDocTemplate(filepath, pagesize=LETTER)
    story = []
    styles = getSampleStyleSheet()
    
    # Title
    title = Paragraph(f"Limited Partner Report<br/>{fund_id}", styles['Title'])
    story.append(title)
    story.append(Spacer(1, 12))
    
    # LP Information
    lp_info = Paragraph(f"<b>Limited Partner:</b> {lp_id}<br/>"
                       f"<b>Report Date:</b> {as_of_date.strftime('%B %d, %Y')}", 
                       styles['Normal'])
    story.append(lp_info)
    story.append(Spacer(1, 20))
    
    # Summary Table
    summary_data = [
        ['Metric', 'Amount'],
        ['Total Contributions', f"${float(lp_row.get('total_contributions', 0)):,.2f}"],
        ['Total Distributions', f"${float(lp_row.get('total_distributions', 0)):,.2f}"],
        ['Unrealized Gains', f"${float(lp_row.get('unrealized_gains', 0)):,.2f}"],
        ['Management Fees', f"${float(lp_row.get('management_fees', 0)):,.2f}"],
        ['Carried Interest', f"${float(lp_row.get('carried_interest', 0)):,.2f}"],
        ['Net Position', f"${float(lp_row.get('net_position', 0)):,.2f}"]
    ]
    
    summary_table = Table(summary_data)
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 14),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    
    story.append(summary_table)
    story.append(Spacer(1, 20))
    
    # Performance metrics
    if 'irr' in lp_row:
        perf_text = Paragraph(f"<b>Performance Metrics:</b><br/>"
                             f"IRR: {float(lp_row.get('irr', 0)) * 100:.2f}%<br/>"
                             f"Multiple: {float(lp_row.get('multiple', 0)):.2f}x", 
                             styles['Normal'])
        story.append(perf_text)
    
    # Build PDF
    doc.build(story)
    return filepath