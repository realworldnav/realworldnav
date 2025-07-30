from shiny import reactive, render

print("DEBUG — server.py loaded")

from .modules.fund_accounting import register_outputs as register_fund_accounting_outputs
from .modules.financial_reporting.financial_reporting import register_outputs as register_financial_reporting_outputs
from .modules.general_ledger.general_ledger import register_outputs as register_gl_outputs

def server(input, output, session):
    print("DEBUG — server() function started")

    selected_fund = reactive.calc(lambda: input.fund())
    selected_report_date = reactive.calc(lambda: input.report_date())

    print("DEBUG — Registering Fund Accounting outputs")
    register_fund_accounting_outputs(output, input)

    print("DEBUG — Registering Financial Reporting outputs")
    register_financial_reporting_outputs(output, selected_fund, selected_report_date)

    print("DEBUG — Registering General Ledger outputs")
    register_gl_outputs(output, input, session, selected_fund)


    @output
    @render.code
    def nav_selection_debug():
        return input.selected_navset_bar()
