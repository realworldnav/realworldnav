from shiny import reactive, render
from .modules.fund_accounting import register_outputs as register_fund_accounting_outputs
from .modules.financial_reporting import register_outputs as register_financial_reporting_outputs

def server(input, output, session):
    # Global inputs (still useful for future fund/date support)
    selected_fund = reactive.calc(lambda: input.fund())
    selected_report_date = reactive.calc(lambda: input.report_date())

    # Fund Accounting: now self-contained (uses full trial balance)
    register_fund_accounting_outputs(output, input)

    # Financial Reporting: still tied to fund + report_date
    register_financial_reporting_outputs(output, selected_fund, selected_report_date)

    # Optional debug
    @output
    @render.code
    def nav_selection_debug():
        return input.selected_navset_bar()
