from .ui import fund_accounting_ui
from .nav_chart import register_nav_outputs
from .net_income import register_net_income_outputs
from .kpis import register_kpi_outputs
from .helpers import trial_balance, melted_tb

def register_outputs(output, input, selected_fund=None, selected_report_date=None):
    register_nav_outputs(output, input)
    register_net_income_outputs(output, input)
    register_kpi_outputs(output, input)
