"""
Management Fee Module
Management fee calculation based on LP commitments and GL entries
"""

from shiny import ui, render, reactive
import pandas as pd
from datetime import datetime
from .data_processor import format_currency, safe_date_compare
from ...s3_utils import load_LP_commitments_file


def management_fee_ui():
    """Create UI for management fee calculation"""
    return ui.card(
        ui.card_header("Management Fee Calculation"),
        ui.p("Note: This report requires LP commitment data to be available."),
        ui.output_table("management_fee_table"),
        ui.hr(),
        ui.download_button("download_management_fee", "Download as CSV")
    )


def load_lp_commitments(fund_id: str = None) -> pd.DataFrame:
    """
    Load LP commitment data from S3 and filter by fund if specified.
    """
    try:
        lp_df = load_LP_commitments_file()
        
        if lp_df.empty:
            print(f"DEBUG - No LP commitments found")
            return pd.DataFrame()
        
        # Filter by fund if specified
        if fund_id:
            # Use fund_id column for exact matching (not fund column)
            if 'fund_id' in lp_df.columns:
                lp_df = lp_df[lp_df['fund_id'] == fund_id]
                print(f"DEBUG - Filtered LP commitments for fund {fund_id}: {len(lp_df)} records")
            elif 'fund' in lp_df.columns:
                # Fallback to fund column if fund_id doesn't exist
                lp_df = lp_df[lp_df['fund'] == fund_id]
                print(f"DEBUG - Filtered LP commitments for fund {fund_id} using 'fund' column: {len(lp_df)} records")
        
        return lp_df
        
    except Exception as e:
        print(f"ERROR - Failed to load LP commitments: {e}")
        return pd.DataFrame()


def register_outputs(output, input, gl_data, selected_date, selected_fund=None):
    """Register server outputs for management fee"""
    
    @reactive.calc
    def management_fee_data():
        """Calculate management fee data"""
        df = gl_data()
        if df.empty:
            return pd.DataFrame()
        
        # Get the selected fund
        fund_id = selected_fund() if selected_fund else "fund_i_class_B_ETH"
        
        # Load LP commitments from S3
        lp_commitments = load_lp_commitments(fund_id)
        
        if lp_commitments.empty:
            return pd.DataFrame()
        
        # Get the selected reporting date
        report_date = selected_date() if selected_date else datetime.now()
        
        # Filter GL entries for management fee expense up to report date
        gl_filtered = df[safe_date_compare(df['date'], report_date)].copy()
        
        # Look for management fee expense entries
        mgmt_fee_entries = gl_filtered[
            gl_filtered['account_name'].str.contains('management.*fee.*expense', case=False, na=False)
        ] if 'account_name' in gl_filtered.columns else pd.DataFrame()
        
        management_fee_calc = []
        
        for _, lp_row in lp_commitments.iterrows():
            # Handle different possible column names from the actual data
            lp_id = lp_row.get('limited_partner_ID', lp_row.get('LP_ID', lp_row.get('investor_id', 0)))
            reporting_lp_id = lp_row.get('reporting_limited_partner_ID', lp_id)
            lp_name = lp_row.get('lp_name', lp_row.get('investor_name', lp_row.get('name', 'Unknown')))
            share_class = lp_row.get('share_class', lp_row.get('class', 'Class B'))
            commitment = lp_row.get('commitment_amount', lp_row.get('commitment', 0))
            
            # Try different rate column names
            rate = (
                lp_row.get('mgt_fee_1_rate', 0) or 
                lp_row.get('management_fee_rate', 0) or 
                lp_row.get('fee_rate', 0.02)  # Default 2%
            )
            
            # Convert percentage if needed (if > 1, assume it's already a percentage)
            if rate > 1:
                rate = rate / 100
            
            # Get management fee expenses for this LP
            if not mgmt_fee_entries.empty and 'limited_partner_ID' in mgmt_fee_entries.columns:
                lp_fee_entries = mgmt_fee_entries[
                    mgmt_fee_entries['limited_partner_ID'] == lp_id
                ]
                total_mgmt_fee = lp_fee_entries['debit_crypto'].sum() if not lp_fee_entries.empty else 0
            else:
                # Calculate theoretical fee if no GL entries
                # Assume monthly fee calculation (annual rate / 12)
                total_mgmt_fee = float(commitment) * (float(rate) / 12)
            
            management_fee_calc.append({
                'Investor_No': reporting_lp_id,
                'Investor_Name': lp_name,
                'Class': share_class,
                'Management_Fee_Base': float(commitment),
                'Management_Fee_Rate': f"{float(rate) * 100:.2f}%",
                'Management_Fee': total_mgmt_fee
            })
        
        return pd.DataFrame(management_fee_calc)
    
    @output
    @render.table
    def management_fee_table():
        """Render the management fee table"""
        fee_df = management_fee_data()
        
        if fee_df.empty:
            return pd.DataFrame({
                'Message': ['No management fee data available. LP commitment data may be missing.']
            })
        
        # Format for display
        display_df = fee_df.copy()
        
        # Format numeric columns
        display_df['Management_Fee_Base'] = display_df['Management_Fee_Base'].apply(
            lambda x: format_currency(x, 'ETH')
        )
        display_df['Management_Fee'] = display_df['Management_Fee'].apply(
            lambda x: format_currency(x, 'ETH')
        )
        
        # Add totals row
        total_base = fee_df['Management_Fee_Base'].sum()
        total_fee = fee_df['Management_Fee'].sum()
        
        totals_row = pd.DataFrame([{
            'Investor_No': '',
            'Investor_Name': 'TOTAL',
            'Class': '',
            'Management_Fee_Base': format_currency(total_base, 'ETH'),
            'Management_Fee_Rate': '',
            'Management_Fee': format_currency(total_fee, 'ETH')
        }])
        
        result = pd.concat([display_df, totals_row], ignore_index=True)
        
        # Rename columns for display
        result.columns = [
            'Investor No', 'Investor Name', 'Class',
            'Management Fee Base', 'Management Fee Rate %', 'Management Fee'
        ]
        
        return result
    
    @output
    @render.download(filename=lambda: f"management_fee_{datetime.now().strftime('%Y%m%d')}.csv")
    def download_management_fee():
        """Download management fee as CSV"""
        import io
        fee_df = management_fee_data()
        
        if fee_df.empty:
            csv_buffer = io.StringIO()
            csv_buffer.write("No management fee data available")
            csv_buffer.seek(0)
            return csv_buffer
        
        csv_buffer = io.StringIO()
        fee_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        return csv_buffer