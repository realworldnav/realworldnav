from shiny import ui as shiny_ui
from .modules.financial_reporting import financial_reporting_ui
from .modules.fund_accounting import fund_accounting_ui

def inventory_ui():
    return shiny_ui.page_fluid(
        shiny_ui.h2("Investments — Coming soon")
    )

def gl_account_ui():
    return shiny_ui.page_fluid(
        shiny_ui.h2("GL Account — Coming soon")
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

app_ui = shiny_ui.page_fluid(
    shiny_ui.navset_bar(
        shiny_ui.nav_panel("Fund Accounting", fund_accounting_ui()),
        shiny_ui.nav_panel("Investments", inventory_ui()),
        shiny_ui.nav_panel("GL Account", gl_account_ui()),
        shiny_ui.nav_panel("Financial Reporting", financial_reporting_ui()),  # ✅ now this points to your real UI
        id="selected_navset_bar",
        title=shiny_ui.TagList(
            shiny_ui.h4("RealWorldNAV"),
            universal_header
        )
    ),
    shiny_ui.h5("Selected:"),
    shiny_ui.output_code("nav_selection_debug")
)