from shiny import ui, reactive, render
from faicons import icon_svg
from .nft_collateral import get_nft_metadata_with_fallback, get_nft_collection_info, safe_str

# === UI ===
def investments_ui():
    return ui.page_fluid(
        # Navigation Tabs
        ui.navset_tab(
            ui.nav_panel("Loan Portfolio", loan_portfolio_ui()),
            ui.nav_panel("NFT Portfolio", nft_portfolio_ui()),
            ui.nav_panel("Cryptocurrencies", cryptocurrencies_ui()),
            ui.nav_panel("Staking", staking_ui()),
            ui.nav_panel("Real Estate", real_estate_ui()),
            ui.nav_panel("Equity", equity_ui()),
            ui.nav_panel("Reports", reports_ui()),
        )
    )

def loan_portfolio_ui():
    return ui.page_fluid(
        # Stats Cards
        ui.layout_column_wrap(
            ui.value_box("Total Portfolio Value", ui.output_ui("loan_portfolio_value")),
            ui.value_box("Active Loans", ui.output_ui("active_loans_count")),
            ui.value_box("NFT Collateral", ui.output_ui("nft_collateral_count")),
            ui.value_box("Performance", ui.output_ui("portfolio_performance")),
            fill=False,
        ),
        
        # Filters Section
        ui.card(
            ui.card_header("Filters"),
            ui.card_body(
                ui.layout_columns(
                    ui.input_selectize("loan_select", "Loan ID:", ["All Loans"], options={"create": False}),
                    ui.output_ui("loan_date_range_ui"),
                    ui.input_selectize("event_filter", "Event Type:", ["All Events"], options={"create": False}),
                    ui.input_selectize("function_filter", "Function:", ["All Functions"], options={"create": False}),
                    ui.input_selectize("platform_filter", "Platform:", ["All Platforms"], options={"create": False}),
                    col_widths=[2, 3, 2, 2, 3]
                )
            )
        ),
        
        # Loan Table with enhanced styling
        ui.card(
            ui.card_header("Loan Portfolio"),
            ui.card_body(
                ui.tags.style("""
                    .loan-portfolio-table .grid-row.selected {
                        background-color: var(--bs-primary-bg-subtle) !important;
                        border-left: 3px solid var(--bs-primary) !important;
                    }
                    .loan-portfolio-table .grid-row:hover {
                        background-color: var(--bs-secondary-bg) !important;
                        cursor: pointer;
                    }
                    .loan-portfolio-table .grid-cell {
                        padding: 0.5rem;
                        border-bottom: 1px solid var(--bs-border-color-translucent);
                    }
                """),
                ui.div(
                    ui.output_data_frame("loan_portfolio_table"),
                    class_="loan-portfolio-table"
                )
            ),
            full_screen=True
        ),
        
        ui.layout_columns(
            # Selected Loan Information
            ui.card(
                ui.card_header("Loan Information"),
                ui.card_body(ui.output_ui("loan_row_inspector")),
            ),
            
            # NFT Collateral
            ui.card(
                ui.card_header("NFT Collateral"),
                ui.card_body(ui.output_ui("nft_collateral_display")),
            ),
            col_widths=[6, 6]
        )
    )

def nft_portfolio_ui():
    """NFT Portfolio UI"""
    return ui.page_fluid(
        # Compact NFT Portfolio Summary
        ui.card(
            ui.card_header("NFT Portfolio Overview"),
            ui.card_body(
                ui.output_ui("nft_portfolio_summary"),
                style="padding: 1rem;"
            ),
            style="margin-bottom: 1rem;"
        ),
        
        # Compact Filters
        ui.card(
            ui.card_header("Filters"),
            ui.card_body(
                ui.layout_columns(
                    ui.output_ui("nft_date_range_ui"),
                    ui.input_selectize("nft_acquisition_filter", "Acquisition Type:", ["All Types"], options={"create": False}),
                    ui.input_selectize("nft_collection_filter", "Collection:", ["All Collections"], options={"create": False}),
                    col_widths=[4, 4, 4]
                ),
                style="padding: 0.75rem 1rem;"
            ),
            style="margin-bottom: 1rem;"
        ),
        
        # NFT Holdings and Details in columns
        ui.layout_columns(
            ui.card(
                ui.card_header("NFT Holdings"),
                ui.card_body(
                    ui.tags.style("""
                        .nft-portfolio-table .grid-row.selected {
                            background-color: var(--bs-primary-bg-subtle) !important;
                            border-left: 3px solid var(--bs-primary) !important;
                        }
                        .nft-portfolio-table .grid-row:hover {
                            background-color: var(--bs-secondary-bg) !important;
                            cursor: pointer;
                        }
                    """),
                    ui.div(
                        ui.output_data_frame("nft_portfolio_table"),
                        class_="nft-portfolio-table"
                    ),
                    style="padding: 0.5rem;"
                ),
                full_screen=True
            ),
            
            ui.card(
                ui.card_header("NFT Details"),
                ui.card_body(
                    ui.output_ui("nft_detail_display"),
                    style="padding: 1rem;"
                ),
                full_screen=True
            ),
            col_widths=[8, 4]
        )
    )

def cryptocurrencies_ui():
    """Cryptocurrencies UI - Complete cryptocurrency portfolio dashboard"""
    return ui.page_fluid(
        # Header Section
        ui.div(
            ui.h2("Digital Assets Portfolio", class_="mb-1", style="color: var(--bs-primary); font-weight: 600;"),
            ui.p("Comprehensive view of cryptocurrency holdings and performance", class_="text-muted mb-4"),
            class_="mb-4"
        ),
        
        # KPI Cards Row
        ui.layout_column_wrap(
            ui.value_box(
                "Total Portfolio Value", 
                ui.output_ui("crypto_total_value"), 
                showcase=ui.div(
                    ui.img(
                        src="https://assets.coingecko.com/coins/images/279/large/ethereum.png",
                        alt="ETH logo",
                        style="width: 32px; height: 32px; border-radius: 50%;"
                    )
                )
            ),
            ui.value_box(
                "Asset Count", 
                ui.output_ui("crypto_asset_count"),
                showcase=ui.div(icon_svg("chart-line"), style="color: var(--bs-info);")
            ),
            ui.value_box(
                "Largest Holding", 
                ui.output_ui("crypto_largest_holding"),
                showcase=ui.output_ui("crypto_largest_holding_image")
            ),
            fill=False
        ),
        
        # Holdings Table
        ui.card(
            ui.card_header(
                ui.div(
                    ui.h6("Digital Asset Holdings", class_="mb-0", style="color: var(--bs-dark); font-weight: 600;"),
                    ui.div(
                        ui.input_select(
                            "crypto_filter", 
                            "", 
                            choices={"all": "All Assets", "major": "Major Assets (>1 ETH)", "minor": "Minor Assets (<1 ETH)"}, 
                            selected="all",
                            width="180px"
                        ),
                        style="margin-left: auto;"
                    ),
                    style="display: flex; align-items: center; justify-content: space-between;"
                ),
                style="padding: 0.75rem 1rem;"
            ),
            ui.card_body(
                ui.tags.style("""
                    .crypto-portfolio-table .grid-row.selected,
                    .crypto-portfolio-table tr.selected,
                    .crypto-portfolio-table [aria-selected="true"] {
                        background-color: var(--bs-primary) !important;
                        color: white !important;
                        border-left: 3px solid var(--bs-primary-dark) !important;
                    }
                    .crypto-portfolio-table .grid-row.selected td,
                    .crypto-portfolio-table tr.selected td,
                    .crypto-portfolio-table [aria-selected="true"] td {
                        background-color: var(--bs-primary) !important;
                        color: white !important;
                    }
                    .crypto-portfolio-table .grid-row:hover {
                        background-color: var(--bs-secondary-bg) !important;
                        cursor: pointer;
                    }
                """),
                ui.div(
                    ui.output_data_frame("cryptocurrency_portfolio_table"),
                    class_="crypto-portfolio-table"
                ),
                style="padding: 0.25rem;"
            ),
            full_screen=True,
            style="height: 500px; margin-bottom: 1rem;"
        ),
        
        # Layout for Asset Details and Top Holdings
        ui.layout_columns(
            # Asset Details - Full Card
            ui.card(
                ui.card_header(
                    ui.h6("Asset Details", class_="mb-0", style="font-weight: 600;"),
                    style="padding: 0.75rem 1rem;"
                ),
                ui.card_body(
                    ui.output_ui("cryptocurrency_detail_display"),
                    style="padding: 1rem;"
                ),
                full_screen=True
            ),
            
            # Top Holdings
            ui.card(
                ui.card_header(
                    ui.h6("Top Holdings", class_="mb-0", style="font-weight: 600;"),
                    style="padding: 0.75rem 1rem;"
                ),
                ui.card_body(
                    ui.output_ui("crypto_portfolio_composition"),
                    style="padding: 1rem;"
                ),
                full_screen=True
            ),
            col_widths=[6, 6]
        ),
        
        style="background-color: var(--bs-body-bg); padding: 1rem;"
    )

def staking_ui():
    """Staking UI"""
    return ui.page_fluid(
        ui.card(
            ui.card_header("Staking"),
            ui.card_body(
                ui.div(
                    ui.h5("Coming Soon", class_="mb-2"),
                    ui.p("Staking features are under development", class_="text-muted"),
                    style="text-align: center; padding: 3rem 1rem;"
                )
            )
        )
    )

def real_estate_ui():
    """Real Estate UI"""
    return ui.page_fluid(
        ui.card(
            ui.card_header("Real Estate"),
            ui.card_body(
                ui.div(
                    ui.h5("Coming Soon", class_="mb-2"),
                    ui.p("Real Estate features are under development", class_="text-muted"),
                    style="text-align: center; padding: 3rem 1rem;"
                )
            )
        )
    )

def equity_ui():
    """Equity UI"""
    return ui.page_fluid(
        ui.card(
            ui.card_header("Equity"),
            ui.card_body(
                ui.div(
                    ui.h5("Coming Soon", class_="mb-2"),
                    ui.p("Equity features are under development", class_="text-muted"),
                    style="text-align: center; padding: 3rem 1rem;"
                )
            )
        )
    )

def reports_ui():
    """Reports UI"""
    return ui.page_fluid(
        ui.card(
            ui.card_header("Reports"),
            ui.card_body(
                ui.div(
                    ui.h5("Coming Soon", class_="mb-2"),
                    ui.p("Reporting features are under development", class_="text-muted"),
                    style="text-align: center; padding: 3rem 1rem;"
                )
            )
        )
    )