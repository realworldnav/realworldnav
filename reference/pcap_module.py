"""PCAP Module - Partner Capital Account Projections"""

import pandas as pd
from decimal import Decimal, getcontext
from datetime import datetime, timedelta, time
import numpy as np
import pytz
from collections import defaultdict

def normalize_to_eod_utc(series):
    """Force to UTC, strip time, then set to one second before midnight"""
    return (
        pd.to_datetime(series, utc=True)
          .dt.tz_convert('UTC')
          .dt.normalize()
          .add(pd.Timedelta(hours=23, minutes=59, seconds=59))
    )

def to_dec(x):
    """Convert to Decimal with proper handling"""
    if pd.isna(x) or x == '':
        return Decimal('0')
    try:
        return Decimal(str(x))
    except:
        return Decimal('0')

class SimplifiedCapitalTiming:
    """
    Capital timing classifier - maintains partner capital accounting conventions
    Partner Capital (Equity Account): Credits increase (+), Debits decrease (-)
    """

    def __init__(self, bod_cutoff_utc="09:00", timezone="UTC"):
        if isinstance(bod_cutoff_utc, tuple):
            hour, minute = bod_cutoff_utc
            self.bod_cutoff = time(hour, minute)
        elif isinstance(bod_cutoff_utc, str):
            self.bod_cutoff = datetime.strptime(bod_cutoff_utc, "%H:%M").time()
        elif isinstance(bod_cutoff_utc, time):
            self.bod_cutoff = bod_cutoff_utc
        else:
            raise ValueError(f"bod_cutoff_utc must be string 'HH:MM', tuple (hour, minute), or time object")

        self.timezone = pytz.timezone(timezone)

        print(f"📅 Capital Activity Cutoff (UTC): {self.bod_cutoff}")
        print(f"   • Before {self.bod_cutoff}: BOD (affects current day P&L allocation)")
        print(f"   • After {self.bod_cutoff}: EOD (affects ending balance, no P&L allocation)")
        print(f"   📚 PARTNER CAPITAL ACCOUNTING:")
        print(f"   • Credits (-) = Increase partner capital")
        print(f"   • Debits (+) = Decrease partner capital")

    def classify_timing(self, transaction_datetime):
        """Classify timing: BOD vs EOD"""
        if transaction_datetime.tzinfo is None:
            transaction_datetime = pytz.utc.localize(transaction_datetime)
        elif transaction_datetime.tzinfo != pytz.utc:
            transaction_datetime = transaction_datetime.astimezone(pytz.utc)

        transaction_time = transaction_datetime.time()

        if transaction_time <= self.bod_cutoff:
            return 'BOD'
        else:
            return 'EOD'

def process_capital_with_partner_accounting(gl, grid, timing_classifier):
    """
    Process capital flows with proper partner capital accounting conventions
    """

    print(f"\n💰 PROCESSING PARTNER CAPITAL WITH TIMING")
    print(f"Using cutoff: {timing_classifier.bod_cutoff} UTC")
    print("📚 Partner Capital Account Rules:")
    print("• Contributions: Credits (-) increase partner capital")
    print("• Distributions: Debits (+) decrease partner capital")
    print("• BOD flows: Affect P&L allocation")
    print("• EOD flows: Affect ending balance only")
    print("=" * 60)

    # Filter for capital transactions
    capital_accounts = ['capital_contributions_property', 'capital_distributions_property']
    capital_gl = gl[gl['account_name'].isin(capital_accounts)].copy()

    print(f"Found {len(capital_gl)} capital transactions to process")

    # Ensure we have transaction_datetime
    if 'transaction_datetime' not in capital_gl.columns:
        print("⚠️ Warning: No transaction_datetime column found")
        if 'date' in capital_gl.columns:
            print("Using 'date' column as fallback for transaction timing")
            capital_gl['transaction_datetime'] = capital_gl['date']
        else:
            raise ValueError("Need either 'transaction_datetime' or 'date' column")

    # Initialize timing columns for partner capital
    timing_columns = ['cap_contrib_bod', 'cap_contrib_eod', 'cap_dist_bod', 'cap_dist_eod']
    for col in timing_columns:
        if col not in grid.columns:
            grid[col] = Decimal('0')

    # Track statistics
    timing_stats = {'BOD': 0, 'EOD': 0}

    print(f"\nProcessing transactions...")

    # Process each capital transaction
    for idx, tx in capital_gl.iterrows():
        try:
            lp_id = tx['limited_partner_ID']
            tx_datetime = pd.to_datetime(tx['transaction_datetime'])
            normalized_date = tx['date']  # Your 23:59:59 format for grid matching

            # Get the net amount (preserves debit/credit signs)
            amount = Decimal(str(tx['net_debit_credit_crypto']))
            account_name = tx['account_name']

            # Classify timing: BOD or EOD
            timing = timing_classifier.classify_timing(tx_datetime)
            timing_stats[timing] += 1

            # Determine transaction type for partner capital
            if 'contribution' in account_name.lower():
                # Contributions are typically CREDITS (negative) to partner capital
                col_prefix = 'cap_contrib'
                tx_type = 'contribution'
                expected_sign = "Cr" if amount < 0 else "Dr (unusual)"
            else:
                # Distributions are typically DEBITS (positive) to partner capital
                col_prefix = 'cap_dist'
                tx_type = 'distribution'
                expected_sign = "Dr" if amount > 0 else "Cr (unusual)"

            target_col = f"{col_prefix}_{timing.lower()}"

            # Find grid row using normalized date
            grid_mask = (grid['limited_partner_ID'] == lp_id) & (grid['date'] == normalized_date)
            grid_idx = grid.index[grid_mask]

            if len(grid_idx) == 1:
                # Add to appropriate timing bucket (preserving partner capital signs)
                grid.at[grid_idx[0], target_col] += amount

                timing_emoji = "🌅" if timing == 'BOD' else "🌆"
                print(f"  {timing_emoji} {tx_datetime.strftime('%Y-%m-%d %H:%M:%S')}: {lp_id}")
                print(f"     {tx_type.title()}: {abs(amount):,.8f} ({expected_sign}) → {timing}")
                print(f"     Partner Capital Impact: {'Increases' if amount < 0 else 'Decreases'}")

            else:
                print(f"  ⚠️ Warning: No grid match for {lp_id} on {normalized_date}")

        except Exception as e:
            print(f"  ❌ Error processing transaction {idx}: {e}")
            continue

    # Summary with partner capital context
    print(f"\n📊 TIMING CLASSIFICATION SUMMARY:")
    print(f"BOD transactions (≤ {timing_classifier.bod_cutoff}): {timing_stats['BOD']}")
    print(f"EOD transactions (> {timing_classifier.bod_cutoff}): {timing_stats['EOD']}")

    print(f"\n💰 PARTNER CAPITAL IMPACT BY TIMING:")
    for timing in ['bod', 'eod']:
        contrib_total = grid[f'cap_contrib_{timing}'].sum()
        dist_total = grid[f'cap_dist_{timing}'].sum()

        if contrib_total != 0 or dist_total != 0:
            timing_label = "BOD" if timing == 'bod' else "EOD"
            print(f"{timing_label}:")
            if contrib_total != 0:
                contrib_impact = "Increases" if contrib_total < 0 else "Decreases"
                print(f"  Contributions: {abs(contrib_total):,.8f} ({contrib_impact} partner capital)")
            if dist_total != 0:
                dist_impact = "Decreases" if dist_total > 0 else "Increases"
                print(f"  Distributions: {abs(dist_total):,.8f} ({dist_impact} partner capital)")

    return grid

def run_partner_capital_pcap_allocation(grid, fund_pnl_by_group):
    """
    PCAP allocation respecting partner capital accounting conventions
    """

    print("🚀 STARTING PARTNER CAPITAL PCAP ALLOCATION")
    print("📚 PARTNER CAPITAL ACCOUNT CONVENTIONS:")
    print("• Normal Balance: CREDIT (negative values)")
    print("• Contributions: Credits (-) increase capital")
    print("• Distributions: Debits (+) decrease capital")
    print("• Management Fees: Debits (+) decrease capital")
    print("=" * 60)

    # PREP
    grid = grid.sort_values(['limited_partner_ID','date']).reset_index(drop=True)

    # Initialize accounting columns
    for col in ['beg_bal', 'allocated_pnl', 'end_bal', 'pnl_allocation_pct', 'allocation_base']:
        if col not in grid.columns:
            grid[col] = Decimal('0')

    # Ensure BOD/EOD columns exist
    partner_capital_columns = ['cap_contrib_bod', 'cap_contrib_eod', 'cap_dist_bod', 'cap_dist_eod']
    for col in partner_capital_columns:
        if col not in grid.columns:
            grid[col] = Decimal('0')

    alloc_rows = []
    all_days = sorted(grid['date'].unique())
    deferred = defaultdict(Decimal)

    print(f"📅 Processing {len(all_days)} days with partner capital conventions")

    # Track daily allocation percentages for analysis
    daily_allocation_summary = []

    for d in all_days:
        day_mask = grid['date'].eq(d)
        day_idx = grid.index[day_mask]

        if not len(day_idx):
            continue

        print(f"\n📅 {d.strftime('%Y-%m-%d')}")

        # 1) Add today's fund P&L to deferred bucket
        todays_groups = fund_pnl_by_group[fund_pnl_by_group['date'] == d]
        daily_fund_pnl = Decimal('0')

        for _, g in todays_groups.iterrows():
            key = (g['SCPC'], g['schedule_ranking'])
            amount = Decimal(str(g['fund_pnl_amt']))
            deferred[key] += amount
            daily_fund_pnl += amount

        if daily_fund_pnl != 0:
            pnl_nature = "Income" if daily_fund_pnl < 0 else "Expense"
            print(f"  📊 Fund P&L: {abs(daily_fund_pnl):,.8f} ({pnl_nature})")

        # 2) Calculate allocation base using COMPLETE PARTNER CAPITAL ACCOUNT BALANCE
        # The allocation should be based on each LP's TOTAL capital account balance,
        # including: contributions, distributions, fees, AND all previously allocated P&L
        allocation_bases = []
        lps = grid.loc[day_idx, 'limited_partner_ID'].tolist()

        for i in day_idx:
            # COMPLETE PARTNER CAPITAL BALANCE = Everything that affects partner equity
            complete_capital_balance = (
                grid.at[i, 'beg_bal']                      # Beginning balance (includes all prior P&L)
                # BOD capital activity (affects today's allocation):
                + grid.at[i, 'cap_contrib_bod']            # BOD contributions (credits, negative)
                + grid.at[i, 'cap_dist_bod']               # BOD distributions (debits, positive)
                # Note: Previously allocated P&L is already in beg_bal
                # Note: Management fees are typically allocated separately, not included in allocation base
            )

            allocation_bases.append(complete_capital_balance)
            grid.at[i, 'allocation_base'] = complete_capital_balance

        # Convert complete capital balances to ownership percentages
        # Partner capital is normally negative (credit balance), so we work with absolute values
        absolute_capital_values = [abs(base) for base in allocation_bases]
        total_absolute_capital = sum(absolute_capital_values)

        # Calculate ownership percentages based on complete capital account balances
        if total_absolute_capital > 0:
            ownership_percentages = [abs_val / total_absolute_capital for abs_val in absolute_capital_values]
        else:
            ownership_percentages = [Decimal('0')] * len(allocation_bases)

        # Log activity by timing
        total_bod_contrib = grid.loc[day_idx, 'cap_contrib_bod'].sum()
        total_bod_dist = grid.loc[day_idx, 'cap_dist_bod'].sum()
        total_eod_contrib = grid.loc[day_idx, 'cap_contrib_eod'].sum()
        total_eod_dist = grid.loc[day_idx, 'cap_dist_eod'].sum()

        if total_bod_contrib != 0 or total_bod_dist != 0:
            print(f"  🌅 BOD flows (affect allocation):")
            if total_bod_contrib != 0:
                contrib_impact = "+" if total_bod_contrib < 0 else "-"
                print(f"    Contributions: {abs(total_bod_contrib):,.8f} ({contrib_impact} to capital)")
            if total_bod_dist != 0:
                dist_impact = "-" if total_bod_dist > 0 else "+"
                print(f"    Distributions: {abs(total_bod_dist):,.8f} ({dist_impact} to capital)")

        if total_eod_contrib != 0 or total_eod_dist != 0:
            print(f"  🌆 EOD flows (ending balance only):")
            if total_eod_contrib != 0:
                contrib_impact = "+" if total_eod_contrib < 0 else "-"
                print(f"    Contributions: {abs(total_eod_contrib):,.8f} ({contrib_impact} to capital)")
            if total_eod_dist != 0:
                dist_impact = "-" if total_eod_dist > 0 else "+"
                print(f"    Distributions: {abs(total_eod_dist):,.8f} ({dist_impact} to capital)")

        print(f"  🎯 Total partner capital for allocation: {total_absolute_capital:,.8f}")

        if total_absolute_capital == 0:
            # 3) No partner capital - defer P&L
            grid.loc[day_idx, 'allocated_pnl'] = Decimal('0')
            grid.loc[day_idx, 'pnl_allocation_pct'] = Decimal('0')
            print(f"  ⏸️ Zero partner capital - P&L deferred")

        else:
            # 4) Allocate P&L based on partner capital ownership percentages
            lp_to_ix = {lp: ix for lp, ix in zip(lps, day_idx)}

            # Store allocation percentages (based on capital account balances)
            for pct, i, lp in zip(ownership_percentages, day_idx, lps):
                grid.at[i, 'pnl_allocation_pct'] = pct

                # Track daily allocation percentage for summary
                daily_allocation_summary.append({
                    'date': d,
                    'limited_partner_ID': lp,
                    'capital_balance': allocation_bases[lps.index(lp)],
                    'allocation_percentage': pct,
                    'has_pnl_to_allocate': daily_fund_pnl != 0
                })

            print(f"  📈 Capital-based allocation weights: {[f'{pct:.4%}' for pct in ownership_percentages]}")

            # Show complete capital balances for transparency
            for lp, balance, pct in zip(lps, allocation_bases, ownership_percentages):
                print(f"    {lp}: Complete Capital {balance:,.8f} → {pct:.4%} allocation")
                print(f"      (Includes: contributions + distributions + prior P&L + fees)")

            # Reset and allocate P&L
            grid.loc[day_idx, 'allocated_pnl'] = Decimal('0')
            allocated_today = Decimal('0')

            for (scpc, rank), amt in list(deferred.items()):
                if amt == 0:
                    continue

                for pct, lp in zip(ownership_percentages, lps):
                    alloc_amt = amt * pct
                    grid.loc[lp_to_ix[lp], 'allocated_pnl'] += alloc_amt
                    allocated_today += alloc_amt

                    alloc_rows.append({
                        'date': d,
                        'limited_partner_ID': lp,
                        'SCPC': scpc,
                        'schedule_ranking': rank,
                        'allocated_amt': alloc_amt,
                        'allocation_weight': pct,
                        'allocation_base': allocation_bases[lps.index(lp)],
                        'capital_balance': allocation_bases[lps.index(lp)]
                    })

                deferred[(scpc, rank)] = Decimal('0')

            if allocated_today != 0:
                pnl_impact = "Favorable" if allocated_today < 0 else "Unfavorable"
                print(f"  ✅ Total allocated: {abs(allocated_today):,.8f} ({pnl_impact} to capital)")

        # 5) ENDING BALANCE: Partner capital equation
        # Ending = Beginning + Contributions(Cr-) + Distributions(Dr+) + Expenses(Dr+) + P&L(varies)
        grid.loc[day_idx, 'end_bal'] = (
            grid.loc[day_idx, 'beg_bal'] +                 # Beginning balance (normally negative)
            grid.loc[day_idx, 'cap_contrib_bod'] +         # BOD contributions (credits, negative)
            grid.loc[day_idx, 'cap_contrib_eod'] +         # EOD contributions (credits, negative)
            grid.loc[day_idx, 'cap_dist_bod'] +            # BOD distributions (debits, positive)
            grid.loc[day_idx, 'cap_dist_eod'] +            # EOD distributions (debits, positive)
            grid.loc[day_idx, 'mgmt_fee_amt'] +            # Management fees (debits, positive)
            (grid.loc[day_idx, 'gp_incentive_amt'] if 'gp_incentive_amt' in grid.columns else Decimal('0')) +
            grid.loc[day_idx, 'allocated_pnl']             # P&L allocation (varies)
        )

        # 6) Roll forward to next day
        pos = all_days.index(d)
        if pos < len(all_days) - 1:
            d_next = all_days[pos + 1]
            nmask = grid['date'].eq(d_next)

            for i in day_idx:
                lp = grid.at[i, 'limited_partner_ID']
                j = grid.index[nmask & grid['limited_partner_ID'].eq(lp)]
                if len(j) == 1:
                    grid.at[j[0], 'beg_bal'] = grid.at[i, 'end_bal']

    print(f"\n✅ PARTNER CAPITAL PCAP ALLOCATION COMPLETE")
    print(f"📊 Final Summary:")
    print(f"  • Deferred P&L: {sum(deferred.values()):,.8f}")
    print(f"  • Allocation records: {len(alloc_rows):,}")
    print(f"  📚 All partner capital accounting conventions maintained")

    # Create daily allocation percentage summary
    allocation_summary_df = pd.DataFrame(daily_allocation_summary)

    return grid, alloc_rows, allocation_summary_df

def create_complete_fund_pcap_with_gp(final_grid_enhanced, allocations_df_enhanced, df_coa=None):
    """
    Complete PCAP generator with Month, Quarter, Year, ITD, and GP Incentive Fees
    Always shows 6 decimal places and includes ALL line items (even if zero)
    Start year is automatically derived from the earliest date in final_grid_enhanced['date'].
    """
    print(f"🏦 CREATING COMPLETE FUND PCAP WITH GP INCENTIVE")
    print(f"📅 Time Periods: Current Month | Current Quarter | Current Year | ITD")
    print("=" * 80)

    if final_grid_enhanced.empty:
        print("❌ No grid data provided")
        return pd.DataFrame()

    # Handle timezone issues
    final_grid_enhanced = final_grid_enhanced.copy()

    # ✅ Ensure date column is datetime and get dynamic start year
    if not pd.api.types.is_datetime64_any_dtype(final_grid_enhanced["date"]):
        final_grid_enhanced["date"] = pd.to_datetime(final_grid_enhanced["date"], errors="coerce")

    # ✅ Get earliest year from 'date'
    min_date = final_grid_enhanced["date"].min()
    if pd.isna(min_date):
        raise ValueError("No valid dates found in final_grid_enhanced['date']")
    start_year = min_date.year
    print(f"📅 Start year automatically set to {start_year}")

    if final_grid_enhanced['date'].dt.tz is not None:
        final_grid_enhanced['date'] = final_grid_enhanced['date'].dt.tz_localize(None)

    if not allocations_df_enhanced.empty:
        allocations_df_enhanced = allocations_df_enhanced.copy()
        allocations_df_enhanced['date'] = pd.to_datetime(allocations_df_enhanced['date'])
        if allocations_df_enhanced['date'].dt.tz is not None:
            allocations_df_enhanced['date'] = allocations_df_enhanced['date'].dt.tz_localize(None)

    fund_start = final_grid_enhanced['date'].min()
    fund_end = final_grid_enhanced['date'].max()

    print(f"📊 Fund inception: {fund_start.strftime('%Y-%m-%d')}")
    print(f"📊 Current date: {fund_end.strftime('%Y-%m-%d')}")

    # SCPC ranking setup - use the rank from COA data
    scpc_to_rank = {}

    if df_coa is not None and not df_coa.empty:
        df_COA = df_coa.copy()
        df_COA = df_COA.dropna(subset=['SCPC', 'schedule_ranking'])

        if not df_COA.empty:
            # Extract rank from schedule_ranking by splitting at "_" and taking the integer
            def extract_rank(schedule_ranking):
                try:
                    if pd.isna(schedule_ranking):
                        return 999
                    parts = str(schedule_ranking).split('_')
                    if len(parts) > 1:
                        return int(parts[-1])
                    return 999
                except:
                    return 999

            df_COA["rank"] = df_COA["schedule_ranking"].apply(extract_rank)

            # Create mapping from SCPC to rank
            for _, row in df_COA.iterrows():
                if pd.notna(row.get('SCPC')) and pd.notna(row.get('rank')):
                    scpc_to_rank[row['SCPC']] = row['rank']

    def get_sort_key(scpc_or_item):
        """Get sort key from the rank in COA data"""
        # Handle special fixed positions
        if 'beginning' in str(scpc_or_item).lower():
            return 0
        if 'ending' in str(scpc_or_item).lower():
            return 999

        # GP Incentive always second-to-last (before ending balance)
        if any(keyword in str(scpc_or_item).lower() for keyword in ['incentive', 'carry', 'gp']):
            return 998

        # Use the rank from COA data
        return scpc_to_rank.get(scpc_or_item, 500)

    # Time period definitions
    current_month_start = fund_end.replace(day=1)
    current_month_end = fund_end

    current_quarter = (fund_end.month - 1) // 3 + 1
    current_quarter_start = datetime(fund_end.year, (current_quarter-1)*3 + 1, 1)
    current_quarter_end = fund_end

    current_year_start = datetime(fund_end.year, 1, 1)
    current_year_end = fund_end

    itd_start = fund_start
    itd_end = fund_end

    unique_lps = final_grid_enhanced['limited_partner_ID'].unique()
    print(f"👥 Processing {len(unique_lps)} LPs")

    all_results = []

    for lp_id in unique_lps:
        print(f"\n👤 Processing {lp_id}")

        lp_grid = final_grid_enhanced[final_grid_enhanced['limited_partner_ID'] == lp_id].copy()
        lp_allocs = allocations_df_enhanced[allocations_df_enhanced['limited_partner_ID'] == lp_id].copy()

        def calculate_period_amounts(start_date, end_date, period_name):
            """Calculate amounts for a specific time period INCLUDING GP INCENTIVE"""
            print(f"  📊 Calculating {period_name}...")

            period_mask = (lp_grid['date'] >= start_date) & (lp_grid['date'] <= end_date)
            period_grid = lp_grid[period_mask]

            if not lp_allocs.empty:
                alloc_mask = (lp_allocs['date'] >= start_date) & (lp_allocs['date'] <= end_date)
                period_allocs = lp_allocs[alloc_mask]
            else:
                period_allocs = pd.DataFrame()

            # Calculate all fee types
            contributions = Decimal('0')
            distributions = Decimal('0')
            mgmt_fees = Decimal('0')
            gp_incentive_fees = Decimal('0')  # GP INCENTIVE

            if not period_grid.empty:
                contributions = (
                    period_grid.get('cap_contrib_bod', pd.Series([0])).sum() +
                    period_grid.get('cap_contrib_eod', pd.Series([0])).sum()
                )
                distributions = (
                    period_grid.get('cap_dist_bod', pd.Series([0])).sum() +
                    period_grid.get('cap_dist_eod', pd.Series([0])).sum()
                )
                mgmt_fees = period_grid.get('mgmt_fee_amt', pd.Series([0])).sum()
                gp_incentive_fees = period_grid.get('gp_incentive_amt', pd.Series([0])).sum()

                # Also check for GP incentive in allocations if not in grid
                if gp_incentive_fees == 0 and not period_allocs.empty:
                    gp_allocs = period_allocs[
                        period_allocs['SCPC'].str.contains('Incentive allocation', case=False, na=False)
                    ]
                    if not gp_allocs.empty:
                        gp_incentive_fees = gp_allocs['allocated_amt'].sum()

            # P&L by SCPC (excluding GP incentive if already captured above)
            pnl_by_scpc = {}
            if not period_allocs.empty:
                # Filter out GP incentive allocations if we found them in grid to avoid double counting
                if gp_incentive_fees != 0:
                    filtered_allocs = period_allocs[
                        ~period_allocs['SCPC'].str.contains('Incentive allocation', case=False, na=False)
                    ]
                else:
                    filtered_allocs = period_allocs

                scpc_groups = filtered_allocs.groupby('SCPC').agg({
                    'allocated_amt': 'sum',
                    'account_type': 'first'
                }).reset_index()

                for _, group in scpc_groups.iterrows():
                    pnl_by_scpc[group['SCPC']] = {
                        'amount': group['allocated_amt'],
                        'type': group.get('account_type', 'Other')
                    }

            return {
                'contributions': contributions,
                'distributions': distributions,
                'mgmt_fees': mgmt_fees,
                'gp_incentive_fees': gp_incentive_fees,
                'pnl_by_scpc': pnl_by_scpc
            }

        # Calculate amounts for all time periods
        current_month = calculate_period_amounts(current_month_start, current_month_end, "Current Month")
        current_quarter = calculate_period_amounts(current_quarter_start, current_quarter_end, "Current Quarter")
        current_year = calculate_period_amounts(current_year_start, current_year_end, "Current Year")
        itd = calculate_period_amounts(itd_start, itd_end, "ITD")

        base_info = {
            'limited_partner_ID': lp_id,
            'Period_End': fund_end,
            'Current_Month_Label': fund_end.strftime('%b %Y'),
            'Current_Quarter_Label': f"Q{(fund_end.month - 1) // 3 + 1} {fund_end.year}",
            'Current_Year_Label': str(fund_end.year),
            'ITD_Label': f"Since {fund_start.strftime('%b %Y')}"
        }

        # Beginning Capital
        beginning_balance = lp_grid.iloc[0]['beg_bal'] if not lp_grid.empty else Decimal('0')

        all_results.append({
            **base_info,
            'Line_Item': 'Beginning Capital',
            'SCPC': 'Beginning Balance',
            'Sort_Order': 0,
            'Current_Month': -float(beginning_balance) if current_month_start <= fund_start else 0.0,
            'Current_Quarter': -float(beginning_balance) if current_quarter_start <= fund_start else 0.0,
            'Current_Year': -float(beginning_balance) if current_year_start <= fund_start else 0.0,
            'ITD': 0.0,
            'Category': 'Capital',
            'Description': 'Capital balance at period start'
        })

        # Capital Contributions
        all_results.append({
            **base_info,
            'Line_Item': 'Capital Contributions',
            'SCPC': 'Capital contributions',
            'Sort_Order': get_sort_key('Capital contributions'),
            'Current_Month': -float(current_month['contributions']),
            'Current_Quarter': -float(current_quarter['contributions']),
            'Current_Year': -float(current_year['contributions']),
            'ITD': -float(itd['contributions']),
            'Category': 'Capital',
            'Description': 'Capital contributed by investors'
        })

        # P&L Allocations by SCPC - SHOW ALL SCPCS (even if zero) - GROUP BY SCPC
        all_scpcs = set()
        # Collect all SCPCs that have data
        for period in [current_month, current_quarter, current_year, itd]:
            all_scpcs.update(period['pnl_by_scpc'].keys())

        # Group by SCPC designation and create line items
        for scpc in sorted(all_scpcs, key=get_sort_key):
            # Skip items that are handled separately
            if any(skip_term in scpc.lower() for skip_term in ['capital contributions', 'capital distributions', 'management fees']):
                continue
            # Skip GP incentive here - it's handled separately at the end
            if any(keyword in scpc.lower() for keyword in ['incentive', 'carry', 'gp']):
                continue

            account_type = 'Other'
            for period in [current_month, current_quarter, current_year, itd]:
                if scpc in period['pnl_by_scpc']:
                    account_type = period['pnl_by_scpc'][scpc]['type']
                    break

            all_results.append({
                **base_info,
                'Line_Item': scpc,
                'SCPC': scpc,
                'Sort_Order': get_sort_key(scpc),
                'Current_Month': -float(current_month['pnl_by_scpc'].get(scpc, {}).get('amount', 0)),
                'Current_Quarter': -float(current_quarter['pnl_by_scpc'].get(scpc, {}).get('amount', 0)),
                'Current_Year': -float(current_year['pnl_by_scpc'].get(scpc, {}).get('amount', 0)),
                'ITD': -float(itd['pnl_by_scpc'].get(scpc, {}).get('amount', 0)),
                'Category': account_type.title(),
                'Description': scpc
            })

        # Management Fees (ALWAYS include, even if zero)
        all_results.append({
            **base_info,
            'Line_Item': 'Management Fees',
            'SCPC': 'Management fees',
            'Sort_Order': get_sort_key('Management fees'),
            'Current_Month': -float(current_month['mgmt_fees']),
            'Current_Quarter': -float(current_quarter['mgmt_fees']),
            'Current_Year': -float(current_year['mgmt_fees']),
            'ITD': -float(itd['mgmt_fees']),
            'Category': 'Fees',
            'Description': 'Management fees charged'
        })

        # Capital Distributions (ALWAYS include, even if zero)
        all_results.append({
            **base_info,
            'Line_Item': 'Capital Distributions',
            'SCPC': 'Capital distributions',
            'Sort_Order': get_sort_key('Capital distributions'),
            'Current_Month': -float(current_month['distributions']),
            'Current_Quarter': -float(current_quarter['distributions']),
            'Current_Year': -float(current_year['distributions']),
            'ITD': -float(itd['distributions']),
            'Category': 'Capital',
            'Description': 'Capital distributed to investors'
        })

        # GP Incentive Fees (ALWAYS include, even if zero - ALWAYS LAST before ending balance)
        all_results.append({
            **base_info,
            'Line_Item': 'GP Incentive Fees',
            'SCPC': 'Incentive allocation to General Partner',
            'Sort_Order': 998,  # Always second-to-last (before ending balance which is 999)
            'Current_Month': -float(current_month['gp_incentive_fees']),
            'Current_Quarter': -float(current_quarter['gp_incentive_fees']),
            'Current_Year': -float(current_year['gp_incentive_fees']),
            'ITD': -float(itd['gp_incentive_fees']),
            'Category': 'Fees',
            'Description': 'GP incentive allocation (carry)'
        })

        # Ending Capital
        ending_balance = lp_grid.iloc[-1]['end_bal'] if not lp_grid.empty else Decimal('0')

        all_results.append({
            **base_info,
            'Line_Item': 'Ending Capital',
            'SCPC': 'Ending Balance',
            'Sort_Order': 999,
            'Current_Month': -float(ending_balance),
            'Current_Quarter': -float(ending_balance),
            'Current_Year': -float(ending_balance),
            'ITD': -float(ending_balance),
            'Category': 'Capital',
            'Description': 'Capital balance at period end'
        })

    # Create final DataFrame
    if all_results:
        df = pd.DataFrame(all_results)
        df = df.sort_values(['limited_partner_ID', 'Sort_Order']).reset_index(drop=True)

        # Round to 6 decimal places
        amount_columns = ['Current_Month', 'Current_Quarter', 'Current_Year', 'ITD']
        for col in amount_columns:
            df[col] = df[col].round(6)

        print(f"\n✅ Created complete PCAP with GP incentive: {len(df)} records")
        return df
    else:
        print("❌ No data to process")
        return pd.DataFrame()

def create_combined_pcap_excel(final_grid_enhanced, allocations_df_enhanced, df_coa=None,
                              filename="All_LP_PCAP_Reports.xlsx", path_for_GP_incentive=".", 
                              fund_id="fund_id", current_period="current"):
    """
    Create one Excel file with separate sheets for each LP
    """
    unique_lps = final_grid_enhanced['limited_partner_ID'].unique()
    print(f"📊 Creating combined Excel file for {len(unique_lps)} LPs...")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    full_filename = f"{path_for_GP_incentive}/{fund_id}/{current_period}_{fund_id}_PCAP_All_LPs.xlsx"

    successful_sheets = []
    failed_sheets = []

    with pd.ExcelWriter(full_filename, engine='openpyxl') as writer:
        # Create summary sheet first
        all_lp_pcap = create_complete_fund_pcap_with_gp(final_grid_enhanced, allocations_df_enhanced, df_coa)
        if not all_lp_pcap.empty:
            all_lp_pcap.to_excel(writer, sheet_name='All_LPs_Combined', index=False)
            print(f"  ✅ Created combined sheet: All_LPs_Combined")

        # Individual LP sheets
        for i, lp_id in enumerate(unique_lps, 1):
            print(f"  📄 [{i}/{len(unique_lps)}] Creating sheet for LP: {lp_id}")

            try:
                # Filter data for this LP
                lp_grid = final_grid_enhanced[final_grid_enhanced['limited_partner_ID'] == lp_id].copy()
                lp_allocs = allocations_df_enhanced[allocations_df_enhanced['limited_partner_ID'] == lp_id].copy()

                if lp_grid.empty:
                    print(f"    ⚠️  No data found for LP: {lp_id}")
                    failed_sheets.append(f"{lp_id} - No data")
                    continue

                # Create PCAP for this LP
                lp_pcap = create_complete_fund_pcap_with_gp(lp_grid, lp_allocs, df_coa)

                if lp_pcap.empty:
                    print(f"    ❌ Failed to create PCAP for LP: {lp_id}")
                    failed_sheets.append(f"{lp_id} - PCAP creation failed")
                    continue

                # Clean sheet name (Excel has 31 char limit and special char restrictions)
                safe_sheet_name = "".join(c for c in str(lp_id) if c.isalnum() or c in (' ', '-', '_'))[:31]

                # Save to sheet
                display_cols = ['Line_Item', 'SCPC', 'Category', 'Current_Month', 'Current_Quarter', 'Current_Year', 'ITD']
                lp_pcap[display_cols].to_excel(writer, sheet_name=safe_sheet_name, index=False)

                print(f"    ✅ Created sheet: {safe_sheet_name}")
                successful_sheets.append(safe_sheet_name)

            except Exception as e:
                print(f"    ❌ Error creating sheet for LP {lp_id}: {str(e)}")
                failed_sheets.append(f"{lp_id} - Error: {str(e)}")

    print(f"\n" + "="*60)
    print(f"📈 COMBINED EXCEL FILE SUMMARY")
    print(f"="*60)
    print(f"📁 File created: {full_filename}")
    print(f"✅ Successfully created: {len(successful_sheets)} LP sheets")
    print(f"❌ Failed: {len(failed_sheets)} LP sheets")

    return {
        'filename': full_filename,
        'successful_sheets': successful_sheets,
        'failed_sheets': failed_sheets,
        'dataframe': all_lp_pcap if 'all_lp_pcap' in locals() else pd.DataFrame()
    }