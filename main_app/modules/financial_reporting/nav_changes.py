"""
NAV Changes Module
Statement of Changes in Net Asset Value
"""

from shiny import ui, render, reactive
import pandas as pd
from datetime import datetime
from .tb_generator import calculate_nav_changes
from .data_processor import format_currency


def nav_changes_ui():
    """Create UI for NAV changes statement"""
    return ui.card(
        ui.card_header("Statement of Changes in Net Asset Value"),
        ui.output_table("nav_changes_table"),
        ui.hr(),
        ui.download_button("download_nav_changes", "Download as CSV")
    )


def register_outputs(output, input, gl_data, selected_date):
    """Register server outputs for NAV changes"""
    
    @reactive.calc
    def nav_changes_data():
        """Calculate NAV changes data"""
        df = gl_data()
        if df.empty:
            # Return empty DataFrame with expected structure
            return pd.DataFrame({
                'Period': ['Month to Date', 'Quarter to Date', 'Year to Date', 'Inception to Date'],
                'Beginning Balance': [0, 0, 0, 0],
                'Capital Contributions': [0, 0, 0, 0],
                'Distributions': [0, 0, 0, 0],
                'Net Income (Loss)': [0, 0, 0, 0],
                'Ending Balance': [0, 0, 0, 0]
            })
        
        # Get the selected reporting date
        report_date = selected_date() if selected_date else datetime.now()
        
        # Calculate NAV changes
        nav_df = calculate_nav_changes(df, report_date)
        
        if nav_df.empty:
            # Return DataFrame with structure but zero values
            return pd.DataFrame({
                'Period': ['Month to Date', 'Quarter to Date', 'Year to Date', 'Inception to Date'],
                'Beginning Balance': [0, 0, 0, 0],
                'Capital Contributions': [0, 0, 0, 0],
                'Distributions': [0, 0, 0, 0],
                'Net Income (Loss)': [0, 0, 0, 0],
                'Ending Balance': [0, 0, 0, 0]
            })
        
        return nav_df
    
    @output
    @render.table
    def nav_changes_table():
        """Render the NAV changes table"""
        nav_df = nav_changes_data()
        
        # Format for display
        display_df = nav_df.copy()
        
        # Format numeric columns
        numeric_cols = ['Beginning Balance', 'Capital Contributions', 'Distributions', 
                       'Net Income (Loss)', 'Ending Balance']
        
        for col in numeric_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda x: format_currency(x, 'ETH'))
        
        # Transpose to show periods as columns (like Excel format)
        display_df = display_df.set_index('Period').T
        display_df.reset_index(inplace=True)
        display_df.rename(columns={'index': 'Line Item'}, inplace=True)
        
        return display_df
    
    @output
    @render.download(filename=lambda: f"nav_changes_{datetime.now().strftime('%Y%m%d')}.csv")
    def download_nav_changes():
        """Download NAV changes as CSV"""
        import io
        nav_df = nav_changes_data()
        
        if nav_df.empty:
            csv_buffer = io.StringIO()
            csv_buffer.write("No NAV changes data available")
            csv_buffer.seek(0)
            return csv_buffer
        
        csv_buffer = io.StringIO()
        nav_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        return csv_buffer