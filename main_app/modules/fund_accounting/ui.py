from shiny import ui
from shinywidgets import output_widget
from faicons import icon_svg

def fund_accounting_ui():
    return ui.page_fluid(
        ui.h2("📈 NAV Dashboard", class_="mt-3"),

        ui.input_radio_buttons(
            id="nav_range_mode",
            label="Select NAV Range:",
            choices={
                "7D": "One Week Ago",
                "MTD": "Month to Date",
                "QTD": "Quarter to Date",
                "YTD": "Year to Date",
                "ITD": "Inception to Date",
                "Custom": "Custom Range"
            },
            selected="MTD",
            inline=True
        ),

        ui.input_date_range("nav_custom_range", "Custom date range", start=None, end=None),

        ui.layout_column_wrap(
            ui.value_box("Current NAV", ui.output_ui("nav_current"), showcase=icon_svg("sack-dollar")),
            ui.value_box("Change", ui.output_ui("nav_change"), showcase=ui.output_ui("nav_change_icon")),
            ui.value_box("Percent Change", ui.output_ui("nav_change_percent"), showcase=icon_svg("percent")),
            fill=False,
        ),

        ui.layout_columns(
            ui.card(
                ui.card_header("NAV Over Time"),
                output_widget("nav_chart"),
                full_screen=True,
            ),
            ui.card(
                ui.card_header("Latest Data"),
                ui.output_data_frame("latest_summary"),
                ui.value_box(
                    "Contributed Capital",
                    ui.output_ui("contributed_capital_box"),
                    showcase=icon_svg("hand-holding-dollar"),
                ),
            ),
            col_widths=[9, 3]
        ),

        ui.card(
            ui.card_header("Net Income Over Time"),
            output_widget("net_income_chart"),
            full_screen=True,
        ),

        ui.card(
            ui.card_header("Melted TB Preview"),
            ui.output_data_frame("tb_preview")
        ),
    )
