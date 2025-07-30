from shiny import ui as shiny_ui

print("DEBUG — main_app/ui.py loaded")

from .modules.financial_reporting.financial_reporting import financial_reporting_ui
from .modules.fund_accounting import fund_accounting_ui
from .modules.general_ledger.general_ledger import general_ledger_ui

def inventory_ui():
    print("DEBUG — inventory_ui() called")
    return shiny_ui.page_fluid(
        shiny_ui.h2("Investments — Coming soon")
    )

# Universal selectors bar
universal_header = shiny_ui.layout_columns(
    shiny_ui.input_select("fund", "Select Fund", {
        "fund_i_class_B_ETH": "Fund I - Class B",
        "fund_ii_class_B_ETH": "Fund II - Class B",
        "holdings_class_B_ETH": "Holdings - Class B"
    }),
    shiny_ui.input_date("report_date", "Reporting Date")
)

# Main app UI
print("DEBUG — Constructing app_ui")
app_ui = shiny_ui.page_fluid(
    shiny_ui.navset_bar(
        shiny_ui.nav_panel("Fund Accounting", fund_accounting_ui()),
        shiny_ui.nav_panel("Investments", inventory_ui()),
        shiny_ui.nav_panel("General Ledger", general_ledger_ui()),
        shiny_ui.nav_panel("Financial Reporting", financial_reporting_ui()),
        id="selected_navset_bar",
        title=shiny_ui.TagList(
            shiny_ui.h4("RealWorldNAV"),
            universal_header
        )
    ),
)
