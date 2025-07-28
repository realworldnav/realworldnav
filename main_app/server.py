from shiny import reactive, render, ui
from .modules.financial_reporting import register_outputs as financial_reporting_register_outputs

def server(input, output, session):
    # Fund Accounting outputs
    @output
    @render.text
    def capital_summary():
        return "Capital summary goes here"

    @output
    @render.text
    def nav_snapshot():
        return "NAV snapshot data goes here"

    @output
    @render.text
    def alert_count():
        return "3 pending alerts"

    @output
    @render.ui
    def alert_modal():
        return ui.tags.div("Alert modal content (future)")

    @output
    @render.code
    def nav_selection_debug():
        return input.selected_navset_bar()
    financial_reporting_register_outputs(output)
