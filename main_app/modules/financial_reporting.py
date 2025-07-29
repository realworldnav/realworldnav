from shiny import ui as shiny_ui, render
from ..s3_utils import load_tb_file
from .financial_reporting_sub_modules.balance_sheet import (
    balance_sheet_ui,
    register_outputs as bs_register_outputs
)
from .financial_reporting_sub_modules.income_statement import (
    income_statement_ui,
    register_outputs as is_register_outputs
)

def register_outputs(output, selected_fund, selected_report_date):
    bs_register_outputs(output, selected_fund, selected_report_date)
    is_register_outputs(output, selected_fund, selected_report_date)

    @output
    @render.data_frame
    def trial_balance_preview():
        df = load_tb_file()
        return df.head(50)


def financial_reporting_ui():
    return shiny_ui.page_fluid(
        shiny_ui.card(
            shiny_ui.h2("📊 Financial Reporting"),

            shiny_ui.input_radio_buttons(
                "report_view",
                label=None,
                choices={
                    "bs": "📑 Balance Sheet",
                    "is": "📈 Income Statement",
                    "soi": "📊 Schedule of Investments",
                    "notes": "📝 Notes",
                    "cap": "👥 Statement of Capital"
                },
                inline=True  # horizontal layout
            ),

            shiny_ui.panel_conditional(
                "input.report_view === 'bs'",
                shiny_ui.card(balance_sheet_ui())
            ),
            shiny_ui.panel_conditional(
                "input.report_view === 'is'",
                shiny_ui.card(income_statement_ui())
            ),
            shiny_ui.panel_conditional(
                "input.report_view === 'soi'",
                shiny_ui.card(shiny_ui.h4("📊 Schedule of Investments — Coming soon"))
            ),
            shiny_ui.panel_conditional(
                "input.report_view === 'notes'",
                shiny_ui.card(shiny_ui.h4("📝 Notes — Coming soon"))
            ),
            shiny_ui.panel_conditional(
                "input.report_view === 'cap'",
                shiny_ui.card(shiny_ui.h4("👥 Statement of Capital — Coming soon"))
            ),
            shiny_ui.accordion(
                shiny_ui.accordion_panel(
                    "📂 Master Trial Balance",
                    shiny_ui.card(shiny_ui.output_data_frame("trial_balance_preview"))
                )
            )
        )
    )
