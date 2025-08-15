from .ui import fund_accounting_ui
from .nav_chart import register_nav_outputs
from .net_income import register_net_income_outputs
from .kpis import register_kpi_outputs
from .helpers import gl_data, daily_balances, trial_balance, melted_tb, apply_plotly_theme

# Import unified approach as well
from .fund_accounting import register_outputs as register_unified_outputs
from .trial_balance import register_outputs as register_trial_balance_outputs

# Note: PCAP GL server functionality removed during cleanup

def register_outputs(output, input, selected_fund=None, selected_report_date=None, session=None):
    """
    Register fund accounting outputs - can use either modular or unified approach
    Currently using unified approach for consistency with main app
    """
    # Use unified approach (fund_accounting.py) which has all the GL data integration
    from .fund_accounting import register_outputs as register_fund_accounting_server_outputs
    register_fund_accounting_server_outputs(output, input, selected_fund, selected_report_date, session)
    
    # Register trial balance outputs
    register_trial_balance_outputs(output, input, selected_fund)
    
    # Note: GL-based PCAP outputs removed during cleanup - PCAP functionality now handled in unified approach
    
    # Alternative modular approach (uncomment if preferred):
    # register_nav_outputs(output, input)
    # register_net_income_outputs(output, input)
    # register_kpi_outputs(output, input)
