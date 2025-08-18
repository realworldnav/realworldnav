from shiny import ui as shiny_ui


from .modules.financial_reporting.financial_reporting import financial_reporting_ui
from .modules.fund_accounting import fund_accounting_ui
from .modules.general_ledger.ui import general_ledger_ui
from .modules.investments.ui import investments_ui
from .theme_manager import theme_manager


# Dashboard UIs for each main section
def fund_accounting_dashboard_ui():
    """Fund Accounting Dashboard with overview metrics"""
    return shiny_ui.page_fluid(
        shiny_ui.h2("Fund Accounting Dashboard"),
        shiny_ui.p("Overview of fund performance and key metrics", class_="text-muted mb-4"),
        
        # Quick stats cards
        shiny_ui.layout_column_wrap(
            shiny_ui.value_box("Current NAV", shiny_ui.output_ui("dashboard_nav_current")),
            shiny_ui.value_box("Monthly Performance", shiny_ui.output_ui("dashboard_nav_performance")),
            shiny_ui.value_box("Total Assets", shiny_ui.output_ui("dashboard_total_assets")),
            shiny_ui.value_box("Active Investments", shiny_ui.output_ui("dashboard_active_investments")),
            fill=False,
        ),
        
        shiny_ui.layout_columns(
            shiny_ui.card(
                shiny_ui.card_header("NAV Trend"),
                shiny_ui.output_ui("dashboard_nav_chart"),
                full_screen=True,
            ),
            shiny_ui.card(
                shiny_ui.card_header("Recent Activity"),
                shiny_ui.output_ui("dashboard_recent_activity"),
            ),
            col_widths=[8, 4]
        )
    )

def investments_dashboard_ui():
    """Investments Dashboard with portfolio overview"""
    return shiny_ui.page_fluid(
        shiny_ui.h2("Investments Dashboard"),
        shiny_ui.p("Portfolio overview and investment performance", class_="text-muted mb-4"),
        
        # Portfolio summary
        shiny_ui.layout_column_wrap(
            shiny_ui.value_box("Total Portfolio Value", shiny_ui.output_ui("dashboard_portfolio_value")),
            shiny_ui.value_box("Active Loans", shiny_ui.output_ui("dashboard_active_loans")),
            shiny_ui.value_box("NFT Holdings", shiny_ui.output_ui("dashboard_nft_count")),
            shiny_ui.value_box("Crypto Holdings", shiny_ui.output_ui("dashboard_crypto_count")),
            fill=False,
        ),
        
        shiny_ui.layout_columns(
            shiny_ui.card(
                shiny_ui.card_header("Portfolio Allocation"),
                shiny_ui.output_ui("dashboard_portfolio_allocation"),
                full_screen=True,
            ),
            shiny_ui.card(
                shiny_ui.card_header("Top Performing Assets"),
                shiny_ui.output_ui("dashboard_top_assets"),
            ),
            col_widths=[8, 4]
        )
    )

def general_ledger_dashboard_ui():
    """General Ledger Dashboard with accounting overview"""
    return shiny_ui.page_fluid(
        shiny_ui.h2("General Ledger Dashboard"),
        shiny_ui.p("Accounting overview and transaction summary", class_="text-muted mb-4"),
        
        # Accounting summary
        shiny_ui.layout_column_wrap(
            shiny_ui.value_box("Total Entries", shiny_ui.output_ui("dashboard_total_entries")),
            shiny_ui.value_box("Current Month", shiny_ui.output_ui("dashboard_month_entries")),
            shiny_ui.value_box("Account Balance", shiny_ui.output_ui("dashboard_account_balance")),
            shiny_ui.value_box("Pending Items", shiny_ui.output_ui("dashboard_pending_items")),
            fill=False,
        ),
        
        shiny_ui.layout_columns(
            shiny_ui.card(
                shiny_ui.card_header("Recent Transactions"),
                shiny_ui.output_ui("dashboard_recent_transactions"),
                full_screen=True,
            ),
            shiny_ui.card(
                shiny_ui.card_header("Account Summary"),
                shiny_ui.output_ui("dashboard_account_summary"),
            ),
            col_widths=[8, 4]
        )
    )

def financial_reporting_dashboard_ui():
    """Financial Reporting Dashboard with comprehensive overview"""
    return shiny_ui.page_fluid(
        shiny_ui.h2("Financial Reporting Dashboard"),
        shiny_ui.p("Comprehensive financial analysis and key performance indicators", class_="text-muted mb-4"),
        
        # Key Financial Metrics
        shiny_ui.layout_column_wrap(
            shiny_ui.value_box(
                "Total Revenue (MTD)", 
                shiny_ui.output_ui("dashboard_total_revenue"),
                theme="primary"
            ),
            shiny_ui.value_box(
                "Net Income (MTD)", 
                shiny_ui.output_ui("dashboard_net_income"),
                theme="success"
            ),
            shiny_ui.value_box(
                "Total Assets", 
                shiny_ui.output_ui("dashboard_assets"),
                theme="info"
            ),
            shiny_ui.value_box(
                "Return on Investment", 
                shiny_ui.output_ui("dashboard_roi"),
                theme="warning"
            ),
            fill=False,
        ),
        
        # Financial Analysis
        shiny_ui.layout_columns(
            shiny_ui.card(
                shiny_ui.card_header("Income Trend Analysis"),
                shiny_ui.output_ui("dashboard_income_trend"),
                full_screen=True,
            ),
            shiny_ui.card(
                shiny_ui.card_header("Balance Sheet Summary"),
                shiny_ui.output_ui("dashboard_balance_summary"),
            ),
            col_widths=[8, 4]
        ),
        
        # Quick Actions
        shiny_ui.layout_column_wrap(
            shiny_ui.card(
                shiny_ui.card_header("Generate Reports"),
                shiny_ui.p("Access detailed financial reports and statements", class_="text-muted mb-3"),
                shiny_ui.input_action_button("goto_reports", "View Detailed Reports", class_="btn btn-primary w-100"),
            ),
            shiny_ui.card(
                shiny_ui.card_header("Export Data"),
                shiny_ui.p("Download comprehensive Excel reports with all modules", class_="text-muted mb-3"),
                shiny_ui.div("Navigate to Reports tab to access Excel download", class_="alert alert-info"),
            ),
            fill=False,
        )
    )

# Enhanced section UIs with sub-navigation
def enhanced_fund_accounting_ui():
    """Fund Accounting with NAV Analysis as main view"""
    # Import UIs here to avoid circular imports
    from .modules.fund_accounting.ui import pcap_ui
    from .modules.fund_accounting.trial_balance import trial_balance_ui
    
    return shiny_ui.navset_tab(
        shiny_ui.nav_panel("NAV Analysis", fund_accounting_ui()),
        shiny_ui.nav_panel("Trial Balance", trial_balance_ui()),
        shiny_ui.nav_panel("Performance", shiny_ui.div("Performance analysis coming soon...")),
        shiny_ui.nav_panel("KPI Dashboard", fund_accounting_dashboard_ui()),
        shiny_ui.nav_panel("PCAP", pcap_ui()),
        id="fund_accounting_tabs"
    )

def enhanced_investments_ui():
    """Investments with Dashboard + sub-navigation"""
    return shiny_ui.navset_tab(
        shiny_ui.nav_panel("Dashboard", investments_dashboard_ui()),
        shiny_ui.nav_panel("Portfolio Overview", investments_ui()),
        id="investments_tabs"
    )

def enhanced_general_ledger_ui():
    """General Ledger with Dashboard + sub-navigation"""
    # Import here to avoid circular imports
    from .modules.general_ledger.chart_of_accounts import chart_of_accounts_ui
    
    return shiny_ui.navset_tab(
        shiny_ui.nav_panel("Dashboard", general_ledger_dashboard_ui()),
        shiny_ui.nav_panel("General Ledger", general_ledger_ui()),
        shiny_ui.nav_panel("Chart of Accounts", chart_of_accounts_ui()),
        id="general_ledger_tabs"
    )

def enhanced_financial_reporting_ui():
    """Financial Reporting with Dashboard + sub-navigation"""
    return shiny_ui.navset_tab(
        shiny_ui.nav_panel("Dashboard", financial_reporting_dashboard_ui()),
        shiny_ui.nav_panel("Reports", financial_reporting_ui()),
        shiny_ui.nav_panel("Analytics", shiny_ui.div("Advanced analytics coming soon...")),
        id="financial_reporting_tabs"
    )

# Client and Fund selectors
import os
default_client = os.environ.get('REALWORLDNAV_CLIENT', 'drip_capital')

client_selector = shiny_ui.input_select("client", "Select Client", {
    "drip_capital": "Drip Capital"
}, selected=default_client, width="100%")

# Fund selector will be reactive based on client selection
fund_selector = shiny_ui.output_ui("dynamic_fund_selector")

# Legacy CSS removed - now using YAML-based theme manager

# Main app UI with sidebar layout
app_ui = shiny_ui.page_sidebar(
    # Sidebar
    shiny_ui.sidebar(
        # RealWorldNav title
        shiny_ui.div(
            shiny_ui.h3("RealWorldNAV", class_="app-title"),
            class_="title-container"
        ),
        
        # Client selector
        shiny_ui.div(
            shiny_ui.h6("Client Selection", class_="section-header"),
            client_selector,
            class_="selector-section"
        ),
        
        # Dynamic client display
        shiny_ui.div(
            shiny_ui.output_ui("client_display"),
            class_="client-display"
        ),
        
        # Fund selector
        shiny_ui.div(
            shiny_ui.h6("Fund Selection", class_="section-header"),
            fund_selector,
            class_="fund-selector-section"
        ),
        
        # Navigation menu
        shiny_ui.div(
            shiny_ui.h6("Navigation", class_="section-header"),
            shiny_ui.output_ui("nav_links"),
            class_="navigation-section"
        ),
        
        # Theme selector
        theme_manager.get_theme_selector_ui(),
        
        # Additional info
        shiny_ui.div(
            shiny_ui.hr(),
            shiny_ui.p("Debug Selection:", class_="debug-label"),
            shiny_ui.div(
                shiny_ui.output_code("nav_selection_debug"),
                class_="debug-selection"
            ),
            class_="debug-section"
        ),
        
        width=280,
        position="left"
    ),
    
    # Main content area
    shiny_ui.div(
        # Dynamic theme styles - make this reactive
        shiny_ui.output_ui("theme_styles"),
        
        # Dynamic content based on sidebar selection
        shiny_ui.output_ui("main_content_area"),
        
        class_="main-content"
    ),
    
    theme=shiny_ui.Theme(theme_manager.get_bootstrap_theme())
) #('bootstrap', 'shiny', 'cerulean', 'cosmo', 'cyborg', 'darkly', 'flatly', 'journal', 'litera', 'lumen', 'lux', 'materia', 'minty', 'morph', 'pulse', 'quartz', 'sandstone', 'simplex', 'sketchy', 'slate', 'solar', 'spacelab', 'superhero', 'united', 'vapor', 'yeti', 'zephyr')