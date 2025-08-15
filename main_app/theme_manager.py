"""
Theme Manager for RealWorldNAV
Simplified version that uses only YAML configuration
"""

import yaml
import os
from typing import Dict, Any
from shiny import ui


class ThemeManager:
    def __init__(self, config_path: str = None):
        """Initialize theme manager with YAML config file"""
        if config_path is None:
            # Default to themes_config.yaml in project root
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_path = os.path.join(project_root, "themes_config.yaml")
        
        self.config_path = config_path
        self.config = self._load_config()
        self.current_theme = self.config.get("default_theme", "light")
        
    def _load_config(self) -> Dict[str, Any]:
        """Load theme configuration from YAML file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            print(f"WARNING: Theme config file not found at {self.config_path}")
            return self._get_fallback_config()
        except Exception as e:
            print(f"ERROR: Failed to load theme config: {e}")
            return self._get_fallback_config()
    
    def _get_fallback_config(self) -> Dict[str, Any]:
        """Fallback configuration if YAML loading fails"""
        return {
            "default_theme": "light",
            "themes": {
                "light": {
                    "name": "Light",
                    "bootstrap_theme": "bootstrap",
                    "colors": {
                        "primary": "#2563eb",
                        "text_primary": "#0f172a",
                        "background_primary": "#ffffff"
                    }
                }
            }
        }
    
    def get_available_themes(self) -> Dict[str, str]:
        """Get list of available themes for selector"""
        themes = {}
        for theme_id, theme_data in self.config.get("themes", {}).items():
            themes[theme_id] = theme_data.get("name", theme_id.title())
        return themes
    
    def set_current_theme(self, theme_id: str):
        """Set the current active theme"""
        if theme_id in self.config.get("themes", {}):
            self.current_theme = theme_id
        else:
            print(f"WARNING: Theme {theme_id} not found, keeping current theme")
    
    def get_current_theme_data(self) -> Dict[str, Any]:
        """Get current theme configuration"""
        return self.config.get("themes", {}).get(self.current_theme, {})
    
    def get_bootstrap_theme(self) -> str:
        """Get Bootstrap theme name for current theme"""
        theme_data = self.get_current_theme_data()
        return theme_data.get("bootstrap_theme", "zephyr")
    
    def generate_css_variables(self) -> str:
        """Generate CSS custom properties from current theme"""
        theme_data = self.get_current_theme_data()
        colors = theme_data.get("colors", {})
        
        css_vars = [":root {"]
        
        # Color variables
        for color_name, color_value in colors.items():
            css_vars.append(f"    --{color_name.replace('_', '-')}: {color_value};")
        
        css_vars.append("}")
        
        return "\n".join(css_vars)
    
    def generate_component_styles(self) -> str:
        """Generate component-specific CSS from current theme"""
        theme_data = self.get_current_theme_data()
        components = theme_data.get("components", {})
        
        styles = []
        
        # Sidebar styles
        if "sidebar" in components:
            sidebar = components["sidebar"]
            sidebar_styles = [
                ".bslib-sidebar-layout > .sidebar {",
                f"    background: {sidebar.get('background', 'transparent')} !important;",
                f"    border-right: {sidebar.get('border', 'none')} !important;",
                f"    box-shadow: {sidebar.get('box_shadow', 'none')} !important;",
                f"    padding: {sidebar.get('padding', '24px')} !important;",
                "}"
            ]
            styles.extend(sidebar_styles)
        
        # Card styles
        if "cards" in components:
            cards = components["cards"]
            card_styles = [
                ".card, .valuebox {",
                f"    background: {cards.get('background', 'transparent')} !important;",
                f"    border: {cards.get('border', 'none')} !important;",
                f"    border-radius: {cards.get('border_radius', '0')} !important;",
                f"    box-shadow: {cards.get('box_shadow', 'none')} !important;",
                "}",
                "",
                ".card-header {",
                f"    background: {cards.get('background', 'transparent')} !important;",
                f"    border-bottom: {cards.get('border', 'none')} !important;",
                f"    padding: 16px 20px !important;",
                "}"
            ]
            styles.extend(card_styles)
        
        # Button styles
        if "buttons" in components:
            buttons = components["buttons"]
            button_styles = [
                ".btn {",
                f"    border-radius: {buttons.get('border_radius', '4px')} !important;",
                f"    font-weight: {buttons.get('font_weight', '500')} !important;",
                f"    box-shadow: {buttons.get('box_shadow', 'none')} !important;",
                f"    padding: {buttons.get('padding', '8px 16px')} !important;",
                "}"
            ]
            styles.extend(button_styles)
        
        # Form styles
        if "forms" in components:
            forms = components["forms"]
            form_styles = [
                ".form-control, .form-select, input, textarea, select {",
                f"    background: {forms.get('background', 'transparent')} !important;",
                f"    border: {forms.get('border', 'none')} !important;",
                f"    border-radius: {forms.get('border_radius', '0')} !important;",
                f"    box-shadow: {forms.get('box_shadow', 'none')} !important;",
                f"    padding: {forms.get('padding', '8px 12px')} !important;",
                "    color: var(--text-primary) !important;",
                "}",
                "",
                ".form-label, label, legend, .form-check-label {",
                "    color: var(--text-primary) !important;",
                "}"
            ]
            styles.extend(form_styles)
        
        return "\n".join(styles)
    
    def generate_full_css(self) -> str:
        """Generate complete CSS for current theme"""
        base_styles = """
        /* Base Layout and Typography */
        * {
            font-family: system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif !important;
        }
        
        body {
            background-color: var(--background-primary) !important;
            color: var(--text-primary) !important;
            line-height: 1.5 !important;
            font-size: 14px !important;
            margin: 0 !important;
        }
        
        /* Typography - Override all text elements */
        h1, h2, h3, h4, h5, h6 {
            color: var(--text-primary) !important;
            font-weight: 600 !important;
            line-height: 1.3 !important;
            margin-bottom: 16px !important;
            margin-top: 0 !important;
        }
        
        p, span, div, label, legend {
            color: var(--text-primary) !important;
        }
        
        /* Card headers specifically */
        .card-header, .card-header h1, .card-header h2, .card-header h3, .card-header h4, .card-header h5, .card-header h6 {
            color: var(--text-primary) !important;
        }
        
        /* Bootstrap overrides */
        .form-label, .form-check-label, fieldset legend {
            color: var(--text-primary) !important;
        }
        
        .text-muted {
            color: var(--text-muted) !important;
        }
        
        .text-center {
            text-align: center !important;
        }
        
        /* Custom Component Classes */
        
        /* Sidebar Components */
        .app-title {
            color: var(--text-primary) !important;
            font-weight: 600 !important;
            margin-bottom: 1rem !important;
        }
        
        .title-container {
            text-align: center !important;
            padding-bottom: 1rem !important;
            border-bottom: 1px solid var(--border-light) !important;
        }
        
        .section-header {
            margin-top: 1rem !important;
            margin-bottom: 0.5rem !important;
            color: var(--text-secondary) !important;
            font-size: 12px !important;
            font-weight: 500 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.5px !important;
        }
        
        .selector-section {
            margin-bottom: 1rem !important;
        }
        
        .fund-selector-section {
            margin-bottom: 1.5rem !important;
        }
        
        .navigation-section {
            margin-bottom: 1rem !important;
        }
        
        .client-display {
            text-align: center !important;
            padding: 1rem 0 !important;
            border-bottom: 1px solid var(--border-light) !important;
            margin-bottom: 1rem !important;
        }
        
        .client-display-content {
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
        }
        
        .client-logo {
            margin-right: 12px !important;
            vertical-align: middle !important;
        }
        
        .client-name {
            display: inline-block !important;
            margin: 0 !important;
            vertical-align: middle !important;
            color: var(--text-primary) !important;
            font-weight: 600 !important;
        }
        
        .debug-section {
            margin-top: auto !important;
        }
        
        .debug-label {
            font-size: 0.8rem !important;
            color: var(--text-muted) !important;
            margin-bottom: 0.2rem !important;
        }
        
        .debug-selection {
            background: var(--surface-secondary) !important;
            border-radius: 4px !important;
            padding: 8px !important;
            font-family: monospace !important;
            font-size: 12px !important;
            color: var(--text-secondary) !important;
        }
        
        /* Navigation Links */
        .nav-link {
            cursor: pointer !important;
            padding: 8px 12px !important;
            border-radius: 4px !important;
            text-decoration: none !important;
            display: block !important;
            transition: all 0.15s ease !important;
            font-weight: 500 !important;
            color: var(--text-secondary) !important;
        }
        
        .nav-link:hover {
            color: var(--primary) !important;
            background-color: var(--surface-secondary) !important;
        }
        
        .nav-link-active {
            font-weight: 600 !important;
            color: var(--primary) !important;
        }
        
        /* Main Content */
        .main-content {
            min-height: 100vh !important;
        }
        
        /* Dashboard States */
        .empty-state {
            padding: 2rem !important;
            text-align: center !important;
        }
        
        .error-state {
            padding: 2rem !important;
            text-align: center !important;
        }
        
        /* Portfolio Components */
        .portfolio-allocation-content {
            background: var(--surface-primary) !important;
            padding: 1rem !important;
            border-radius: 6px !important;
            border: 1px solid var(--border-light) !important;
        }
        
        .allocation-item {
            display: flex !important;
            justify-content: space-between !important;
            align-items: center !important;
            padding: 0.75rem 0 !important;
            border-bottom: 1px solid var(--border-light) !important;
        }
        
        .top-assets-content {
            background: var(--surface-primary) !important;
            padding: 1rem !important;
            border-radius: 6px !important;
            border: 1px solid var(--border-light) !important;
        }
        
        .asset-item {
            padding: 0.5rem 0 !important;
            border-bottom: 1px solid var(--border-light) !important;
        }
        
        /* Custom Date Range */
        .custom-date-container {
            margin-top: 16px !important;
            padding: 12px !important;
            background-color: var(--surface-secondary) !important;
            border-radius: 4px !important;
            border: 1px solid var(--border-default) !important;
        }
        
        /* Radio Buttons */
        .form-check-input[type="radio"] {
            width: 18px !important;
            height: 18px !important;
            border: 2px solid var(--border-default) !important;
            background-color: var(--surface-primary) !important;
            margin-right: 8px !important;
            position: relative !important;
        }
        
        .form-check-input[type="radio"]:checked {
            background-color: var(--primary) !important;
            border-color: var(--primary) !important;
            background-image: none !important;
        }
        
        .form-check-input[type="radio"]:checked::after {
            content: '' !important;
            position: absolute !important;
            top: 50% !important;
            left: 50% !important;
            width: 8px !important;
            height: 8px !important;
            border-radius: 50% !important;
            background-color: white !important;
            transform: translate(-50%, -50%) !important;
        }
        
        .form-check-label {
            font-weight: 500 !important;
            cursor: pointer !important;
            margin-left: 0 !important;
            color: var(--text-primary) !important;
        }
        
        /* Inline Radio Button Groups */
        .shiny-input-radiogroup-inline .form-check {
            display: inline-flex !important;
            align-items: center !important;
            margin-right: 16px !important;
            margin-bottom: 8px !important;
            padding: 10px 16px !important;
            background-color: var(--surface-primary) !important;
            border: 1px solid var(--border-light) !important;
            border-radius: 6px !important;
            min-height: 40px !important;
        }
        
        .shiny-input-radiogroup-inline .form-check:hover {
            background-color: var(--surface-secondary) !important;
            border-color: var(--border-default) !important;
        }
        
        .shiny-input-radiogroup-inline .form-check-input[type="radio"]:checked + .form-check-label {
            color: var(--primary) !important;
            font-weight: 600 !important;
        }
        
        /* Tables */
        .dataframe, .table {
            margin: 12px 0 !important;
            border-collapse: collapse !important;
            width: 100% !important;
            background: var(--surface-primary) !important;
        }
        
        .dataframe td, .dataframe th, .table td, .table th {
            padding: 10px 12px !important;
            border-bottom: 1px solid var(--border-light) !important;
            text-align: left !important;
            color: var(--text-primary) !important;
        }
        
        .dataframe thead th, .table thead th {
            font-weight: 600 !important;
            font-size: 12px !important;
            text-transform: uppercase !important;
            letter-spacing: 0.5px !important;
            color: var(--text-secondary) !important;
            background-color: var(--surface-secondary) !important;
        }
        """
        
        # Additional comprehensive overrides for dark theme
        comprehensive_overrides = """
        
        /* Comprehensive text color overrides */
        .form-control, .form-select, input, textarea, select {
            color: var(--text-primary) !important;
            background-color: var(--surface-primary) !important;
        }
        
        .form-control:focus, .form-select:focus, input:focus, textarea:focus, select:focus {
            color: var(--text-primary) !important;
            border-color: var(--primary) !important;
            box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.2) !important;
        }
        
        /* All form labels and text */
        .form-label, label, legend, .form-check-label, fieldset legend {
            color: var(--text-primary) !important;
        }
        
        /* Bootstrap components */
        .btn {
            color: var(--text-primary) !important;
        }
        
        .btn-primary {
            background-color: var(--primary) !important;
            border-color: var(--primary) !important;
            color: white !important;
        }
        
        .btn-secondary {
            background-color: var(--secondary) !important;
            border-color: var(--secondary) !important;
            color: white !important;
        }
        
        /* Value boxes */
        .valuebox, .valuebox * {
            color: var(--text-primary) !important;
        }
        
        .valuebox-title {
            color: var(--text-secondary) !important;
        }
        
        /* Cards */
        .card, .card * {
            color: var(--text-primary) !important;
        }
        
        .card-header, .card-header * {
            color: var(--text-primary) !important;
        }
        
        /* Override any dark text specifically */
        div, span, p, a, strong, em, small, code, pre {
            color: var(--text-primary) !important;
        }
        
        /* Ensure muted text uses the proper variable */
        .text-muted, .text-secondary {
            color: var(--text-secondary) !important;
        }
        """
        
        # Additional specific overrides for tabs, tables, and other elements
        additional_overrides = """
        
        /* Bootstrap Tab Navigation */
        .nav-tabs {
            border-bottom: 1px solid var(--border-default) !important;
            background: transparent !important;
        }
        
        .nav-tabs .nav-link {
            color: var(--text-secondary) !important;
            background: transparent !important;
            border: none !important;
            border-bottom: 2px solid transparent !important;
        }
        
        .nav-tabs .nav-link:hover {
            color: var(--primary) !important;
            background: var(--surface-secondary) !important;
        }
        
        .nav-tabs .nav-link.active {
            color: var(--primary) !important;
            background: var(--surface-primary) !important;
            border-bottom-color: var(--primary) !important;
            border-top: none !important;
            border-left: none !important;
            border-right: none !important;
        }
        
        .nav-tabs .nav-item .nav-link.active {
            color: var(--primary) !important;
            background-color: var(--surface-primary) !important;
            border-color: var(--border-light) var(--border-light) var(--surface-primary) !important;
        }
        
        /* Tab Content */
        .tab-content {
            background: var(--surface-primary) !important;
            color: var(--text-primary) !important;
        }
        
        .tab-pane {
            color: var(--text-primary) !important;
        }
        
        /* Date Input Elements */
        input[type="date"], input[type="datetime-local"], input[type="time"] {
            color: var(--text-primary) !important;
            background-color: var(--surface-primary) !important;
            border: 1px solid var(--border-default) !important;
        }
        
        input[type="date"]:focus, input[type="datetime-local"]:focus, input[type="time"]:focus {
            color: var(--text-primary) !important;
            background-color: var(--surface-primary) !important;
            border-color: var(--primary) !important;
        }
        
        /* Date range "to" text and labels */
        .shiny-date-range-input .shiny-date-range-separator {
            color: var(--text-primary) !important;
        }
        
        .input-group-text, .input-group-addon {
            color: var(--text-primary) !important;
            background-color: var(--surface-secondary) !important;
            border-color: var(--border-default) !important;
        }
        
        /* Shiny Specific Elements */
        .shiny-input-container, .shiny-input-container * {
            color: var(--text-primary) !important;
        }
        
        .shiny-input-container label {
            color: var(--text-primary) !important;
        }
        
        .shiny-input-container .control-label {
            color: var(--text-primary) !important;
        }
        
        /* DataTables and Shiny Tables */
        .dataTable, .dataTable * {
            color: var(--text-primary) !important;
            background: var(--surface-primary) !important;
        }
        
        .dataTable thead th, .dataTable thead td {
            color: var(--text-primary) !important;
            background-color: var(--surface-secondary) !important;
            border-bottom: 1px solid var(--border-default) !important;
        }
        
        .dataTable tbody tr {
            color: var(--text-primary) !important;
            background-color: var(--surface-primary) !important;
        }
        
        .dataTable tbody tr:nth-child(even) {
            background-color: var(--surface-secondary) !important;
        }
        
        .dataTable tbody tr:hover {
            background-color: var(--surface-secondary) !important;
            color: var(--text-primary) !important;
        }
        
        /* Dropdown Arrow Enhancement */
        .dropdown-with-arrow {
            position: relative !important;
        }
        
        .dropdown-with-arrow select,
        .dropdown-with-arrow .form-select {
            appearance: none !important;
            -webkit-appearance: none !important;
            -moz-appearance: none !important;
            background-image: none !important;
            padding-right: 35px !important;
        }
        
        .dropdown-with-arrow::after {
            content: '▼' !important;
            position: absolute !important;
            top: 50% !important;
            right: 12px !important;
            transform: translateY(-50%) !important;
            pointer-events: none !important;
            color: var(--text-secondary) !important;
            font-size: 12px !important;
            z-index: 1 !important;
        }
        
        .dropdown-with-arrow:hover::after {
            color: var(--primary) !important;
        }
        
        /* Shiny Output Elements */
        .shiny-html-output, .shiny-text-output, .shiny-table-output {
            color: var(--text-primary) !important;
        }
        
        .shiny-output-error {
            color: var(--danger) !important;
        }
        
        /* Generic table styling */
        table, table * {
            color: var(--text-primary) !important;
            background: var(--surface-primary) !important;
        }
        
        table thead, table thead * {
            color: var(--text-primary) !important;
            background-color: var(--surface-secondary) !important;
        }
        
        table tbody tr:nth-child(even) {
            background-color: var(--surface-secondary) !important;
        }
        
        /* DataGrid Row Selection - Comprehensive coverage for all possible class names */
        .grid-row.selected, [class*="selected"], tr.selected, .selected {
            background-color: var(--primary) !important;
            color: white !important;
            border-left: 3px solid var(--primary-dark) !important;
        }
        
        .grid-row.selected td, .grid-row.selected th, 
        [class*="selected"] td, [class*="selected"] th,
        tr.selected td, tr.selected th,
        .selected td, .selected th {
            background-color: var(--primary) !important;
            color: white !important;
        }
        
        .grid-row:hover, [class*="grid-row"]:hover, tr:hover {
            background-color: var(--surface-secondary) !important;
            cursor: pointer !important;
        }
        
        .grid-row:hover td, .grid-row:hover th,
        [class*="grid-row"]:hover td, [class*="grid-row"]:hover th,
        tr:hover td, tr:hover th {
            background-color: var(--surface-secondary) !important;
        }
        
        /* Additional DataGrid specific styling */
        .grid-container, [class*="grid-container"] {
            color: var(--text-primary) !important;
            background: var(--surface-primary) !important;
        }
        
        .grid-header, [class*="grid-header"] {
            background-color: var(--surface-secondary) !important;
            color: var(--text-primary) !important;
            font-weight: 600 !important;
        }
        
        /* DataGrid table elements - Force row selection visibility */
        [data-testid*="dataframe"] tr.selected,
        [data-testid*="datagrid"] tr.selected,
        .dataframe tr.selected,
        .datagrid tr.selected {
            background-color: var(--primary) !important;
            color: white !important;
        }
        
        [data-testid*="dataframe"] tr.selected td,
        [data-testid*="datagrid"] tr.selected td,
        .dataframe tr.selected td,
        .datagrid tr.selected td {
            background-color: var(--primary) !important;
            color: white !important;
        }
        
        /* Bootstrap Form Elements */
        .form-floating label {
            color: var(--text-secondary) !important;
        }
        
        .form-floating input:focus ~ label {
            color: var(--primary) !important;
        }
        
        /* Progress bars */
        .progress {
            background-color: var(--surface-secondary) !important;
        }
        
        .progress-bar {
            background-color: var(--primary) !important;
        }
        """
        
        # Plotly chart styling - no CSS overrides, pure programmatic theming
        plotly_overrides = ""
        
        css_parts = [
            self.generate_css_variables(),
            base_styles,
            self.generate_component_styles(),
            comprehensive_overrides,
            additional_overrides,
            plotly_overrides
        ]
        
        return "\n\n".join(css_parts)
    
    def get_theme_ui_element(self) -> ui.TagChild:
        """Generate UI element containing theme styles"""
        css_content = self.generate_full_css()
        return ui.tags.style(css_content)
    
    def get_theme_selector_ui(self) -> ui.TagChild:
        """Generate theme selector dropdown UI"""
        if not self.config.get("theme_selector", {}).get("enabled", True):
            return ui.div()  # Empty div if disabled
            
        themes = self.get_available_themes()
        selector_config = self.config.get("theme_selector", {})
        
        if not themes:
            return ui.div()  # Return empty if no themes available
        
        return ui.div(
            ui.h6(selector_config.get("label", "Theme"), 
                  class_="section-header"),
            ui.div(
                ui.input_select(
                    "theme_selector",
                    "",
                    choices=themes,
                    selected=self.current_theme,
                    width="100%"
                ),
                class_="dropdown-with-arrow"
            ),
            class_="selector-section"
        )
    
    def get_plotly_layout_config(self) -> dict:
        """Get complete Plotly layout configuration that matches YAML theme exactly"""
        theme_data = self.get_current_theme_data()
        colors = theme_data.get("colors", {})
        
        # Extract all theme colors
        text_primary = colors.get("text_primary", "#0f172a")
        text_secondary = colors.get("text_secondary", "#475569") 
        surface_primary = colors.get("surface_primary", "#ffffff")
        border_light = colors.get("border_light", "#e2e8f0")
        primary = colors.get("primary", "#2563eb")
        
        return {
            # Paper and plot backgrounds
            "paper_bgcolor": surface_primary,
            "plot_bgcolor": surface_primary,
            
            # Global font settings
            "font": {
                "family": "system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif",
                "size": 12,
                "color": text_primary
            },
            
            # Title styling
            "title": {
                "font": {"size": 16, "color": text_primary},
                "x": 0.5,
                "xanchor": "center"
            },
            
            # X-axis complete configuration
            "xaxis": {
                "tickfont": {"size": 10, "color": text_primary},
                "titlefont": {"size": 12, "color": text_primary},
                "linecolor": border_light,
                "gridcolor": border_light,
                "tickcolor": text_primary,
                "zerolinecolor": border_light,
                "showgrid": True,
                "showline": True,
                "zeroline": True,
                "title": {"standoff": 20}
            },
            
            # Y-axis complete configuration  
            "yaxis": {
                "tickfont": {"size": 10, "color": text_primary},
                "titlefont": {"size": 12, "color": text_primary},
                "linecolor": border_light,
                "gridcolor": border_light,
                "tickcolor": text_primary,
                "zerolinecolor": border_light,
                "showgrid": True,
                "showline": True,
                "zeroline": True,
                "title": {"standoff": 20}
            },
            
            # Legend styling
            "legend": {
                "bgcolor": surface_primary,
                "bordercolor": border_light,
                "borderwidth": 1,
                "font": {"size": 11, "color": text_primary}
            },
            
            # Hover label styling
            "hoverlabel": {
                "bgcolor": surface_primary,
                "bordercolor": border_light,
                "font": {"color": text_primary}
            },
            
            # Color palette
            "colorway": [primary, "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#06b6d4", "#84cc16", "#f97316", "#ec4899"]
        }


# Global theme manager instance
theme_manager = ThemeManager()
