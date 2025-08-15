# Dashboard functions that use MTD calculations for selected fund
# These will replace the existing ones in server.py

@output
@render.ui
def dashboard_total_revenue():
    try:
        from .s3_utils import load_GL_file
        from .modules.financial_reporting.tb_generator import get_income_expense_changes
        
        # Get GL data for selected fund
        gl_df = load_GL_file()
        if gl_df.empty:
            return "No Data"
        
        # Filter by selected fund
        fund_id = selected_fund() if selected_fund else None
        if fund_id and 'fund_id' in gl_df.columns:
            gl_df = gl_df[gl_df['fund_id'] == fund_id]
        
        if gl_df.empty:
            return "No Fund Data"
        
        # Use current date for MTD calculation
        report_date = datetime.now()
        
        # Get income data using the same method as financial reporting
        income_df, _ = get_income_expense_changes(gl_df, report_date)
        
        if income_df.empty:
            return "0.0000 ETH"
        
        # Sum MTD income
        total_revenue = income_df['MTD'].sum() if 'MTD' in income_df.columns else 0
        
        return f"{total_revenue:,.4f} ETH"
    except Exception as e:
        print(f"Error in dashboard_total_revenue: {e}")
        import traceback
        traceback.print_exc()
        return "Error"

@output
@render.ui
def dashboard_net_income():
    try:
        from .s3_utils import load_GL_file
        from .modules.financial_reporting.tb_generator import get_income_expense_changes
        
        # Get GL data for selected fund
        gl_df = load_GL_file()
        if gl_df.empty:
            return "No Data"
        
        # Filter by selected fund
        fund_id = selected_fund() if selected_fund else None
        if fund_id and 'fund_id' in gl_df.columns:
            gl_df = gl_df[gl_df['fund_id'] == fund_id]
        
        if gl_df.empty:
            return "No Fund Data"
        
        # Use current date for MTD calculation
        report_date = datetime.now()
        
        # Get income and expense data using the same method as financial reporting
        income_df, expense_df = get_income_expense_changes(gl_df, report_date)
        
        # Calculate MTD net income
        total_income = income_df['MTD'].sum() if not income_df.empty and 'MTD' in income_df.columns else 0
        total_expenses = expense_df['MTD'].sum() if not expense_df.empty and 'MTD' in expense_df.columns else 0
        net_income = total_income - total_expenses
        
        return f"{net_income:,.4f} ETH"
    except Exception as e:
        print(f"Error in dashboard_net_income: {e}")
        import traceback
        traceback.print_exc()
        return "Error"

@output
@render.ui
def dashboard_assets():
    try:
        from .s3_utils import load_GL_file
        from .modules.financial_reporting.tb_generator import generate_trial_balance_from_gl
        
        # Get GL data for selected fund
        gl_df = load_GL_file()
        if gl_df.empty:
            return "No Data"
        
        # Filter by selected fund
        fund_id = selected_fund() if selected_fund else None
        if fund_id and 'fund_id' in gl_df.columns:
            gl_df = gl_df[gl_df['fund_id'] == fund_id]
        
        if gl_df.empty:
            return "No Fund Data"
        
        # Use current date for calculation
        report_date = datetime.now()
        
        # Generate trial balance and get assets
        tb_df = generate_trial_balance_from_gl(gl_df, report_date)
        
        if tb_df.empty:
            return "0.0000 ETH"
        
        # Sum all asset accounts (Category == 'Assets')
        assets_df = tb_df[tb_df['Category'] == 'Assets'] if 'Category' in tb_df.columns else pd.DataFrame()
        total_assets = assets_df['Balance'].sum() if not assets_df.empty and 'Balance' in assets_df.columns else 0
        
        return f"{total_assets:,.4f} ETH"
    except Exception as e:
        print(f"Error in dashboard_assets: {e}")
        import traceback
        traceback.print_exc()
        return "Error"

@output
@render.ui
def dashboard_roi():
    try:
        from .s3_utils import load_GL_file
        from .modules.financial_reporting.tb_generator import get_income_expense_changes, generate_trial_balance_from_gl
        
        # Get GL data for selected fund
        gl_df = load_GL_file()
        if gl_df.empty:
            return "No Data"
        
        # Filter by selected fund
        fund_id = selected_fund() if selected_fund else None
        if fund_id and 'fund_id' in gl_df.columns:
            gl_df = gl_df[gl_df['fund_id'] == fund_id]
        
        if gl_df.empty:
            return "No Fund Data"
        
        # Use current date for MTD calculation
        report_date = datetime.now()
        
        # Get income and expense data
        income_df, expense_df = get_income_expense_changes(gl_df, report_date)
        
        # Get assets from trial balance
        tb_df = generate_trial_balance_from_gl(gl_df, report_date)
        assets_df = tb_df[tb_df['Category'] == 'Assets'] if not tb_df.empty and 'Category' in tb_df.columns else pd.DataFrame()
        
        # Calculate MTD net income and total assets
        total_income = income_df['MTD'].sum() if not income_df.empty and 'MTD' in income_df.columns else 0
        total_expenses = expense_df['MTD'].sum() if not expense_df.empty and 'MTD' in expense_df.columns else 0
        total_assets = assets_df['Balance'].sum() if not assets_df.empty and 'Balance' in assets_df.columns else 0
        
        net_income = total_income - total_expenses
        
        # Calculate ROI as a percentage
        if total_assets != 0:
            roi = (net_income / total_assets) * 100
            return f"{roi:.2f}%"
        else:
            return "N/A"
            
    except Exception as e:
        print(f"Error in dashboard_roi: {e}")
        import traceback
        traceback.print_exc()
        return "Error"