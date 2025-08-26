from shiny import ui
from shinywidgets import output_widget
from faicons import icon_svg

def fund_accounting_ui():
    return ui.page_fluid(
        ui.h2("NAV Dashboard", class_="mt-3"),

        # Date range selection with uniform styling
        ui.card(
            ui.card_header("Select NAV Range"),
            ui.card_body(
                ui.input_radio_buttons(
                    id="nav_range_mode",
                    label=None,
                    choices={
                        "MTD": "Month to Date",
                        "QTD": "Quarter to Date",
                        "YTD": "Year to Date",
                        "ITD": "Inception to Date",
                        "Custom": "Custom Range"
                    },
                    selected="MTD",
                    inline=True
                ),
                ui.output_ui("custom_date_range_ui")
            )
        ),

        ui.layout_column_wrap(
            ui.value_box("Current NAV", ui.output_ui("nav_current"), showcase=icon_svg("sack-dollar")),
            ui.value_box("Change", ui.output_ui("nav_change"), showcase=ui.output_ui("nav_change_icon")),
            ui.value_box("Percent Change", ui.output_ui("nav_change_percent"), showcase=icon_svg("percent")),
            fill=False,
        ),

        ui.layout_columns(
            ui.card(
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
            output_widget("net_income_chart"),
            full_screen=True,
        ),

    )

def pcap_ui():
    return ui.page_fluid(
        ui.h3("Statement of Changes in Partners' Capital", class_="mt-3"),
        
        # PCAP Controls
        ui.card(
            ui.card_header("PCAP Generation Controls"),
            ui.card_body(
                ui.row(
                    ui.column(
                        3,
                        ui.output_ui("fund_selection")
                    ),
                    ui.column(
                        3,
                        ui.output_ui("lp_selection")
                    ),
                    ui.column(
                        3,
                        ui.output_ui("pcap_date_input")
                    ),
                    ui.column(
                        3,
                        ui.div(
                            ui.input_select(
                                "pcap_currency",
                                "Currency",
                                choices={"ETH": "ETH", "USD": "USD"},
                                selected="ETH"
                            ),
                            class_="custom-dropdown"
                        )
                    )
                ),
                ui.row(
                    ui.column(
                        6,
                        ui.input_action_button(
                            "generate_pcap",
                            "Generate PCAP",
                            class_="btn-secondary w-100 mt-3"
                        )
                    ),
                    ui.column(
                        6,
                        ui.download_button(
                            "export_pcap_pdf",
                            "Export PDF",
                            class_="btn-secondary w-100 mt-3"
                        )
                    )
                )
            )
        ),
        
        # Results Display Area
        ui.div(
            ui.output_ui("pcap_results_header"),
            ui.output_ui("pcap_detailed_results"),
            ui.output_ui("pcap_summary_charts"),
            class_="mt-4"
        ),
        
        # View Mode Selection - moved to bottom
        ui.card(
            ui.card_header("View Options"),
            ui.card_body(
                ui.div(
                    ui.input_select(
                        "pcap_view_mode",
                        "View Mode:",
                        choices={
                            "detailed": "Detailed Line Items",
                            "summary": "Summary View",
                            "comparison": "LP Comparison"
                        },
                        selected="detailed"
                    ),
                    class_="custom-dropdown"
                )
            ),
            class_="mt-4"
        )
    )
