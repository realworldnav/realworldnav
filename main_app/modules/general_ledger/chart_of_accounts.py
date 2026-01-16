from shiny import ui, reactive, render, req
from shiny import App, Inputs, Outputs, Session
from shiny.render import DataGrid

from ...s3_utils import load_COA_file, save_COA_file
import pandas as pd
from typing import Dict, Optional

# === Helpers ===
def safe_str(v):
    return "" if pd.isna(v) or v is None else str(v)

def format_account_number(num):
    """Format account numbers as plain integers"""
    try:
        return str(int(num))
    except:
        return str(num)

def get_account_type_color(account_type):
    """Get color coding for different account types"""
    color_map = {
        "Asset Accounts": "#28a745",   # Green
        "Liabilities": "#dc3545",      # Red  
        "Capital": "#6f42c1",          # Purple
        "Other Income": "#17a2b8",     # Teal
        "Expenses": "#fd7e14",         # Orange
        "Income": "#007bff",           # Blue
    }
    
    for key, color in color_map.items():
        if key.lower() in str(account_type).lower():
            return color
    
    return "#6c757d"  # Default gray

def get_account_category_from_number(account_number):
    """Determine account category based on account number ranges"""
    try:
        num = int(account_number)
        first_digit = int(str(num)[0])
        
        if first_digit == 1:
            return "Asset Accounts"
        elif first_digit == 2:
            return "Liabilities"
        elif first_digit == 3:
            return "Capital"
        elif first_digit == 4:
            return "Other Income"
        elif first_digit == 8:
            return "Expenses"
        elif first_digit == 9:
            return "Income"
        else:
            return "Other"
    except:
        return "Unknown"

# === Server ===
def register_chart_of_accounts_outputs(output: Outputs, input: Inputs, session: Session, selected_fund):
    selected_account_store = reactive.Value(None)
    refresh_trigger = reactive.Value(0)  # Counter to trigger data refresh

    @reactive.calc
    def chart_of_accounts_data():
        """Load and process Chart of Accounts data"""
        try:
            # Add dependency on refresh trigger to force refresh after adds/edits
            refresh_trigger()
            print("DEBUG - Chart of Accounts: Loading COA data")
            coa = load_COA_file()
            
            if coa.empty:
                print("DEBUG - Chart of Accounts: No COA data found")
                return pd.DataFrame()
            
            print(f"DEBUG - Chart of Accounts: Loaded {len(coa)} accounts")
            
            # Add computed columns for better display
            coa_enhanced = coa.copy()
            
            # Add account category based on number ranges
            coa_enhanced["Account_Category"] = coa_enhanced["GL_Acct_Number"].apply(get_account_category_from_number)
            
            # Ensure string formatting
            coa_enhanced["GL_Acct_Name"] = coa_enhanced["GL_Acct_Name"].astype(str)
            coa_enhanced["GL_Acct_Number"] = coa_enhanced["GL_Acct_Number"].astype(int)
            
            # Add formatted account number for display
            coa_enhanced["Formatted_Account_Number"] = coa_enhanced["GL_Acct_Number"].apply(format_account_number)
            
            # Sort by account number
            coa_enhanced = coa_enhanced.sort_values("GL_Acct_Number")
            
            print(f"DEBUG - Chart of Accounts: Enhanced data has {len(coa_enhanced)} accounts")
            return coa_enhanced

        except Exception as e:
            print(f"Error in chart_of_accounts_data: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    @reactive.calc
    def filtered_coa_data():
        """Apply filters to Chart of Accounts data"""
        try:
            df = chart_of_accounts_data()
            if df.empty:
                print("DEBUG - Chart of Accounts: No data available")
                return pd.DataFrame()
            
            print(f"DEBUG - Chart of Accounts: Starting with {len(df)} accounts")
            
            # Debug: Show first few account numbers and their categories
            if not df.empty:
                print("DEBUG - Sample account data:")
                for i in range(min(3, len(df))):
                    row = df.iloc[i]
                    print(f"  Account {row['GL_Acct_Number']} ({type(row['GL_Acct_Number'])}): {row['GL_Acct_Name']} - Category: {row['Account_Category']}")
            
            # Apply account category filter based on first digit
            try:
                category_filter = input.account_category_filter()
                print(f"DEBUG - Category filter input: '{category_filter}' (type: {type(category_filter)})")
            except Exception as e:
                print(f"DEBUG - Error getting category filter: {e}")
                category_filter = None
            
            if category_filter and category_filter != "All Categories":
                print(f"DEBUG - Applying category filter: {category_filter}")
                
                # Map category names back to first digits
                digit_map = {
                    "Asset Accounts": "1",
                    "Liabilities": "2", 
                    "Capital": "3",
                    "Other Income": "4",
                    "Expenses": "8",
                    "Income": "9"
                }
                
                if category_filter in digit_map:
                    target_digit = digit_map[category_filter]
                    print(f"DEBUG - Looking for accounts starting with digit '{target_digit}'")
                    
                    # Convert account numbers to string and check first digit
                    account_strings = df["GL_Acct_Number"].astype(str)
                    print(f"DEBUG - Sample account strings: {list(account_strings.head(3))}")
                    
                    mask = account_strings.str.startswith(target_digit)
                    df = df[mask]
                    print(f"DEBUG - After digit filter: {len(df)} accounts remain")
                else:
                    print(f"DEBUG - Category '{category_filter}' not found in digit map")
            else:
                print("DEBUG - No category filter applied or 'All Categories' selected")
            
            # Apply search filter
            try:
                search_term = input.account_search()
                print(f"DEBUG - Search term: '{search_term}'")
                
                if search_term and search_term.strip():
                    search_term = search_term.strip().lower()
                    print(f"DEBUG - Applying search filter: '{search_term}'")
                    
                    search_mask = (
                        df["GL_Acct_Name"].astype(str).str.lower().str.contains(search_term, na=False) |
                        df["GL_Acct_Number"].astype(str).str.contains(search_term, na=False)
                    )
                    df = df[search_mask]
                    print(f"DEBUG - After search filter: {len(df)} accounts remain")
            except Exception as e:
                print(f"DEBUG - Error applying search filter: {e}")
            
            # Apply number range filters
            try:
                min_account = input.min_account_number()
                max_account = input.max_account_number()
                
                if min_account is not None:
                    df = df[df["GL_Acct_Number"] >= min_account]
                    print(f"DEBUG - Applied min filter {min_account}: {len(df)} accounts remain")
                
                if max_account is not None:
                    df = df[df["GL_Acct_Number"] <= max_account]
                    print(f"DEBUG - Applied max filter {max_account}: {len(df)} accounts remain")
            except Exception as e:
                print(f"DEBUG - Error applying number range filters: {e}")
            
            print(f"DEBUG - Chart of Accounts: Final filtered result: {len(df)} accounts")
            return df

        except Exception as e:
            print(f"ERROR in filtered_coa_data: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    # Note: Category filter options are now defined directly in the UI

    @output
    @render.data_frame
    def chart_of_accounts_preview():
        """Display COA preview table with just 3 key columns"""
        df = filtered_coa_data()
        if df.empty:
            return pd.DataFrame({"Message": ["No Chart of Accounts data available"]})
        
        # Show only the 3 key columns for preview
        preview_cols = ["GL_Acct_Number", "account_name", "Account_Category"]
        available_cols = [col for col in preview_cols if col in df.columns]
        
        if not available_cols:
            return pd.DataFrame({"Message": ["No preview data available"]})
        
        display_df = df[available_cols].copy()
        
        # Rename columns for better display
        rename_map = {
            "GL_Acct_Number": "Account Number",
            "account_name": "Account Name", 
            "Account_Category": "Category"
        }
        
        for old_name, new_name in rename_map.items():
            if old_name in display_df.columns:
                display_df = display_df.rename(columns={old_name: new_name})
        
        return DataGrid(display_df, selection_mode="row")

    @output
    @render.data_frame  
    def chart_of_accounts_detailed():
        """Display detailed COA table with ALL columns"""
        df = filtered_coa_data()
        if df.empty:
            return pd.DataFrame({"Message": ["No Chart of Accounts data available"]})
        
        # Show all original columns plus computed ones, excluding internal formatting columns
        exclude_cols = ["Formatted_Account_Number"]  # Exclude internal helper columns
        display_cols = [col for col in df.columns if col not in exclude_cols]
        display_df = df[display_cols].copy()
        
        # Rename key columns for better display
        rename_map = {
            "GL_Acct_Number": "Account Number",
            "account_name": "Account Name",
            "GL_Acct_Name": "GL Account Name", 
            "Account_Category": "Category"
        }
        
        for old_name, new_name in rename_map.items():
            if old_name in display_df.columns:
                display_df = display_df.rename(columns={old_name: new_name})
        
        return DataGrid(display_df, selection_mode="row")

    @reactive.effect
    def capture_selected_account_preview():
        """Capture selected account row from preview table"""
        selection = chart_of_accounts_preview.cell_selection()
        df = filtered_coa_data()

        if not selection or "rows" not in selection or not selection["rows"]:
            selected_account_store.set(None)
            return

        row_idx = selection["rows"][0]
        if df is None or row_idx >= len(df):
            selected_account_store.set(None)
            return

        row = df.iloc[row_idx].to_dict()
        selected_account_store.set(row)

    @reactive.effect
    def capture_selected_account_detailed():
        """Capture selected account row from detailed table"""
        selection = chart_of_accounts_detailed.cell_selection()
        df = filtered_coa_data()

        if not selection or "rows" not in selection or not selection["rows"]:
            selected_account_store.set(None)
            return

        row_idx = selection["rows"][0]
        if df is None or row_idx >= len(df):
            selected_account_store.set(None)
            return

        row = df.iloc[row_idx].to_dict()
        selected_account_store.set(row)

    @output
    @render.ui
    def chart_of_accounts_summary():
        """Display Chart of Accounts summary statistics"""
        try:
            df = chart_of_accounts_data()
            filtered_df = filtered_coa_data()
            
            if df.empty:
                return ui.tags.div(
                    ui.tags.div(class_="empty-icon"),
                    ui.tags.h5("No Chart of Accounts", class_="mb-2"),
                    ui.tags.p("No Chart of Accounts data available", class_="text-muted"),
                    class_="empty-state"
                )
            
            # Calculate summary metrics
            total_accounts = len(df)
            filtered_accounts = len(filtered_df)
            categories = df["Account_Category"].nunique()
            
            # Count by category
            category_counts = df["Account_Category"].value_counts()
            
            return ui.tags.div(
                ui.layout_column_wrap(
                    ui.value_box(
                        "Total Accounts", 
                        f"{total_accounts:,}", 
                    ),
                    ui.value_box(
                        "Filtered Results", 
                        f"{filtered_accounts:,}", 
                    ),
                    ui.value_box(
                        "Categories", 
                        f"{categories}", 
                    ),
                    ui.value_box(
                        "Account Range", 
                        f"{int(df['GL_Acct_Number'].min())} - {int(df['GL_Acct_Number'].max())}", 
                    ),
                    fill=False,
                )
            )
            
        except Exception as e:
            print(f"Error in chart_of_accounts_summary: {e}")
            return ui.tags.div(
                ui.tags.div(class_="empty-icon"),
                ui.tags.h5("Error Loading Summary", class_="mb-2"),
                ui.tags.p("Unable to load Chart of Accounts data", class_="text-muted"),
                class_="empty-state"
            )

    @output
    @render.ui
    def selected_account_details():
        """Display editable form for selected account"""
        row = selected_account_store.get()
        if not row:
            return ui.tags.div(
                ui.tags.div(class_="empty-icon"),
                ui.tags.h5("Select an Account", class_="mb-2"),
                ui.tags.p("Click on an account row to edit details", class_="text-muted"),
                class_="empty-state"
            )

        try:
            account_number = row.get("GL_Acct_Number", "")
            account_name = row.get("account_name", "") or row.get("GL_Acct_Name", "")  # Use account_name or fallback to GL_Acct_Name
            account_category = row.get("Account_Category", "")
            
            # Get color for the category
            category_color = get_account_type_color(account_category)

            return ui.tags.div(
                ui.tags.h4("Edit Account", class_="mb-3"),
                
                # Editable form
                ui.card(
                    ui.card_header("Account Information"),
                    ui.tags.div(
                        ui.input_numeric(
                            "edit_account_number", 
                            "Account Number:", 
                            value=int(account_number) if account_number else None,
                            min=0,
                            max=999999
                        ),
                        ui.input_text(
                            "form_edit_account_name", 
                            "Account Name:", 
                            value=safe_str(account_name)
                        ),
                        ui.input_text(
                            "edit_gl_acct_name",
                            "GL Account Name:",
                            value=safe_str(row.get("GL_Acct_Name", ""))
                        ),
                        ui.input_selectize(
                            "edit_account_category",
                            "Category:",
                            {
                                "Asset Accounts": "Asset Accounts",
                                "Liabilities": "Liabilities", 
                                "Capital": "Capital",
                                "Other Income": "Other Income",
                                "Expenses": "Expenses",
                                "Income": "Income"
                            },
                            selected=account_category
                        ),
                        ui.input_text(
                            "form_edit_cryptocurrency",
                            "Cryptocurrency:",
                            value=safe_str(row.get("Cryptocurrency", ""))
                        ),
                        ui.input_text(
                            "edit_account_field",
                            "Account:",
                            value=safe_str(row.get("account", ""))
                        ),
                        ui.input_selectize(
                            "edit_form_1065",
                            "Form 1065:",
                            {"": "", "Yes": "Yes", "No": "No"},
                            selected=safe_str(row.get("Form_1065", ""))
                        ),
                        ui.input_text(
                            "edit_ilpa",
                            "ILPA:",
                            value=safe_str(row.get("ILPA", ""))
                        ),
                        ui.input_text(
                            "edit_schedule_ranking",
                            "Schedule Ranking:",
                            value=safe_str(row.get("schedule_ranking", ""))
                        ),
                        ui.input_text(
                            "edit_scpc",
                            "SCPC:",
                            value=safe_str(row.get("SCPC", ""))
                        ),
                        ui.input_text(
                            "edit_soi_long_short",
                            "SOI Long/Short:",
                            value=safe_str(row.get("SOI_long_short", ""))
                        ),
                        ui.input_text(
                            "edit_soi_instrument",
                            "SOI Instrument:",
                            value=safe_str(row.get("SOI_Instrument", ""))
                        ),
                        ui.input_text(
                            "edit_soi_sub_asset_class",
                            "SOI Sub Asset Class:",
                            value=safe_str(row.get("SOI_Sub_asset_class", ""))
                        ),
                        ui.input_text(
                            "edit_soi_country",
                            "SOI Country:",
                            value=safe_str(row.get("SOI_Country", ""))
                        ),
                        ui.input_text(
                            "edit_soi_sector",
                            "SOI Sector:",
                            value=safe_str(row.get("SOI_Sector", ""))
                        ),
                        ui.input_text(
                            "edit_soi_currency",
                            "SOI Currency:",
                            value=safe_str(row.get("SOI_Currency", ""))
                        ),
                        ui.input_text(
                            "edit_soi_description",
                            "SOI Description:",
                            value=safe_str(row.get("SOI_Description", ""))
                        ),
                        
                        # Action buttons - Save Edit, Save to S3, Delete, Cancel
                        ui.div(
                            ui.input_action_button("save_account_edit", ui.HTML('<i class="bi bi-floppy me-1"></i> Save Edit'), class_="btn btn-primary me-2"),
                            ui.input_action_button("save_edit_to_s3", "Save to S3", class_="btn btn-success me-2"),
                            ui.input_action_button("delete_account", "Delete", class_="btn btn-danger me-2"),
                            ui.input_action_button("cancel_account_edit", "Cancel", class_="btn btn-secondary"),
                            style="margin-top: 1rem; text-align: right;"
                        ),
                        
                        style="padding: 1rem;"
                    )
                ),
                
                # Current values display - show ALL fields from the COA
                ui.div(
                    ui.h6("All Account Details:", class_="mt-3 mb-2 text-muted"),
                    *[ui.div(
                        ui.tags.div(
                            ui.span(f"{key}:", class_="detail-label"),
                            ui.span(safe_str(value), class_="detail-value", 
                                   style=f"color: {category_color}; font-weight: bold;" if key == "Account_Category" else ""),
                            class_="detail-item"
                        )
                    ) for key, value in row.items() if not key.startswith("Formatted_")],
                    style="background: white; padding: 1rem; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);"
                ),
                
                class_="account-edit-card"
            )

        except Exception as e:
            print(f"Error displaying account edit form: {e}")
            return ui.tags.div(
                ui.tags.div(class_="empty-icon"),
                ui.tags.h5("Error Loading Account", class_="mb-2"),
                ui.tags.p(f"Unable to load account details: {e}", class_="text-muted"),
                class_="empty-state"
            )

    @output
    @render.ui
    def category_breakdown():
        """Display breakdown by account category"""
        try:
            df = chart_of_accounts_data()
            if df.empty:
                return ui.tags.p("No data available for category breakdown")
            
            # Get category counts
            category_counts = df["Account_Category"].value_counts()
            
            breakdown_items = []
            for category, count in category_counts.items():
                color = get_account_type_color(category)
                percentage = (count / len(df)) * 100
                
                breakdown_items.append(
                    ui.tags.div(
                        ui.tags.div(
                            ui.tags.span(category, style=f"color: {color}; font-weight: bold;"),
                            ui.tags.span(f"{count} accounts ({percentage:.1f}%)", class_="text-muted", style="float: right;"),
                            style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid #eee;"
                        )
                    )
                )
            
            return ui.tags.div(
                ui.tags.h5("Category Breakdown", class_="mb-3"),
                *breakdown_items,
                style="background: white; padding: 1rem; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);"
            )
            
        except Exception as e:
            print(f"Error in category_breakdown: {e}")
            return ui.tags.p(f"Error loading category breakdown: {e}")

    # === Modal Logic ===
    @reactive.effect
    @reactive.event(input.show_add_account_modal)
    def show_add_account_modal():
        """Show the add account modal"""
        m = ui.modal(
            ui.div(
                ui.h4(ui.HTML('<i class="bi bi-plus-lg me-2"></i>Add New Account'), class_="mb-4"),
                
                # Basic Information Section
                ui.h6("Basic Information", class_="mt-3 mb-2", style="color: #6c757d; border-bottom: 1px solid #eee; padding-bottom: 0.5rem;"),
                ui.layout_columns(
                    ui.input_numeric(
                        "new_account_number",
                        "Account Number:",
                        value=None,
                        min=0,
                        max=999999
                    ),
                    ui.input_text(
                        "new_account_name",
                        "Account Name:",
                        value=""
                    ),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_text(
                        "new_gl_acct_name", 
                        "GL Account Name:",
                        value=""
                    ),
                    ui.input_selectize(
                        "new_account_category",
                        "Category:",
                        {
                            "Asset Accounts": "Asset Accounts",
                            "Liabilities": "Liabilities", 
                            "Capital": "Capital",
                            "Other Income": "Other Income",
                            "Expenses": "Expenses",
                            "Income": "Income"
                        },
                        selected="Asset Accounts"
                    ),
                    col_widths=[6, 6]
                ),
                
                # Additional Fields Section
                ui.h6("Additional Fields", class_="mt-4 mb-2", style="color: #6c757d; border-bottom: 1px solid #eee; padding-bottom: 0.5rem;"),
                ui.layout_columns(
                    ui.input_text("new_cryptocurrency", "Cryptocurrency:", value=""),
                    ui.input_text("new_account_field", "Account:", value=""),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_selectize(
                        "new_form_1065",
                        "Form 1065:",
                        {"": "", "Yes": "Yes", "No": "No"},
                        selected=""
                    ),
                    ui.input_text("new_ilpa", "ILPA:", value=""),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_text("new_schedule_ranking", "Schedule Ranking:", value=""),
                    ui.input_text("new_scpc", "SCPC:", value=""),
                    col_widths=[6, 6]
                ),
                
                # SOI Fields Section
                ui.h6("SOI (Statement of Investment) Fields", class_="mt-4 mb-2", style="color: #6c757d; border-bottom: 1px solid #eee; padding-bottom: 0.5rem;"),
                ui.layout_columns(
                    ui.input_text("new_soi_long_short", "SOI Long/Short:", value=""),
                    ui.input_text("new_soi_instrument", "SOI Instrument:", value=""),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_text("new_soi_sub_asset_class", "SOI Sub Asset Class:", value=""),
                    ui.input_text("new_soi_country", "SOI Country:", value=""),
                    col_widths=[6, 6]
                ),
                ui.layout_columns(
                    ui.input_text("new_soi_sector", "SOI Sector:", value=""),
                    ui.input_text("new_soi_currency", "SOI Currency:", value=""),
                    col_widths=[6, 6]
                ),
                ui.div(
                    ui.input_text("new_soi_description", "SOI Description:", value=""),
                    style="width: 100%;"
                ),
                
                # Footer with buttons
                ui.div(
                    ui.input_action_button("cancel_add_account", "Cancel", class_="btn btn-secondary me-2"),
                    ui.input_action_button("add_new_account", "Add Account", class_="btn btn-primary"),
                    style="text-align: right; margin-top: 2rem; padding-top: 1rem; border-top: 1px solid #eee;"
                ),
                
                style="padding: 1.5rem;"
            ),
            title="Add New Account",
            size="xl"
        )
        ui.modal_show(m)

    @reactive.effect
    @reactive.event(input.cancel_add_account)
    def cancel_add_account():
        """Hide the add account modal"""
        ui.modal_remove()

    # === Account Editing Logic ===
    @reactive.effect
    @reactive.event(input.save_account_edit)
    def handle_save_account_edit():
        """Handle saving account changes"""
        try:
            print("DEBUG - COA: Saving account changes")
            
            # Get current account data
            row = selected_account_store.get()
            if not row:
                print("DEBUG - COA: No account selected for saving")
                return
            
            # Get form values
            new_account_number = input.edit_account_number()
            new_account_name = input.form_edit_account_name()
            new_gl_acct_name = input.edit_gl_acct_name()
            new_account_category = input.edit_account_category()
            
            # Get all additional fields
            new_cryptocurrency = input.form_edit_cryptocurrency()
            new_account_field = input.edit_account_field()
            new_form_1065 = input.edit_form_1065()
            new_ilpa = input.edit_ilpa()
            new_schedule_ranking = input.edit_schedule_ranking()
            new_scpc = input.edit_scpc()
            new_soi_long_short = input.edit_soi_long_short()
            new_soi_instrument = input.edit_soi_instrument()
            new_soi_sub_asset_class = input.edit_soi_sub_asset_class()
            new_soi_country = input.edit_soi_country()
            new_soi_sector = input.edit_soi_sector()
            new_soi_currency = input.edit_soi_currency()
            new_soi_description = input.edit_soi_description()
            
            if not all([new_account_number, new_account_name]):
                print("DEBUG - COA: Missing required fields (account number and name) for save")
                return
            
            # Load current COA data
            coa_df = load_COA_file()
            if coa_df.empty:
                print("DEBUG - COA: Could not load COA data for saving")
                return
            
            # Find the row to update
            original_account_number = row.get("GL_Acct_Number")
            mask = coa_df["GL_Acct_Number"] == original_account_number
            
            if not mask.any():
                print(f"DEBUG - COA: Account {original_account_number} not found for updating")
                return
            
            # Update all the fields
            coa_df.loc[mask, "GL_Acct_Number"] = int(new_account_number)
            coa_df.loc[mask, "account_name"] = str(new_account_name)
            coa_df.loc[mask, "GL_Acct_Name"] = str(new_gl_acct_name)
            coa_df.loc[mask, "Cryptocurrency"] = str(new_cryptocurrency) if new_cryptocurrency else ""
            coa_df.loc[mask, "account"] = str(new_account_field) if new_account_field else ""
            coa_df.loc[mask, "Form_1065"] = str(new_form_1065) if new_form_1065 else ""
            coa_df.loc[mask, "ILPA"] = str(new_ilpa) if new_ilpa else ""
            coa_df.loc[mask, "schedule_ranking"] = str(new_schedule_ranking) if new_schedule_ranking else ""
            coa_df.loc[mask, "SCPC"] = str(new_scpc) if new_scpc else ""
            coa_df.loc[mask, "SOI_long_short"] = str(new_soi_long_short) if new_soi_long_short else ""
            coa_df.loc[mask, "SOI_Instrument"] = str(new_soi_instrument) if new_soi_instrument else ""
            coa_df.loc[mask, "SOI_Sub_asset_class"] = str(new_soi_sub_asset_class) if new_soi_sub_asset_class else ""
            coa_df.loc[mask, "SOI_Country"] = str(new_soi_country) if new_soi_country else ""
            coa_df.loc[mask, "SOI_Sector"] = str(new_soi_sector) if new_soi_sector else ""
            coa_df.loc[mask, "SOI_Currency"] = str(new_soi_currency) if new_soi_currency else ""
            coa_df.loc[mask, "SOI_Description"] = str(new_soi_description) if new_soi_description else ""
            
            # Just update in memory, don't save to S3 yet
            print(f"DEBUG - COA: Account edit saved in memory for account {new_account_number}")
            # Clear selection to refresh the display
            selected_account_store.set(None)
                
        except Exception as e:
            print(f"ERROR in handle_save_account_edit: {e}")
            import traceback
            traceback.print_exc()

    @reactive.effect
    @reactive.event(input.save_edit_to_s3)
    def handle_save_edit_to_s3():
        """Handle saving account edits to S3"""
        try:
            print("DEBUG - COA: Saving account edit to S3")
            
            # Get current account data
            row = selected_account_store.get()
            if not row:
                print("DEBUG - COA: No account selected for S3 save")
                return
            
            # Get form values
            new_account_number = input.edit_account_number()
            new_account_name = input.form_edit_account_name()
            new_gl_acct_name = input.edit_gl_acct_name()
            
            # Get all additional fields
            new_cryptocurrency = input.form_edit_cryptocurrency()
            new_account_field = input.edit_account_field()
            new_form_1065 = input.edit_form_1065()
            new_ilpa = input.edit_ilpa()
            new_schedule_ranking = input.edit_schedule_ranking()
            new_scpc = input.edit_scpc()
            new_soi_long_short = input.edit_soi_long_short()
            new_soi_instrument = input.edit_soi_instrument()
            new_soi_sub_asset_class = input.edit_soi_sub_asset_class()
            new_soi_country = input.edit_soi_country()
            new_soi_sector = input.edit_soi_sector()
            new_soi_currency = input.edit_soi_currency()
            new_soi_description = input.edit_soi_description()
            
            if not all([new_account_number, new_account_name]):
                print("DEBUG - COA: Missing required fields (account number and name) for S3 save")
                return
            
            # Load current COA data
            coa_df = load_COA_file()
            if coa_df.empty:
                print("DEBUG - COA: Could not load COA data for S3 saving")
                return
            
            # Find the row to update
            original_account_number = row.get("GL_Acct_Number")
            mask = coa_df["GL_Acct_Number"] == original_account_number
            
            if not mask.any():
                print(f"DEBUG - COA: Account {original_account_number} not found for S3 updating")
                return
            
            # Update all the fields
            coa_df.loc[mask, "GL_Acct_Number"] = int(new_account_number)
            coa_df.loc[mask, "account_name"] = str(new_account_name)
            coa_df.loc[mask, "GL_Acct_Name"] = str(new_gl_acct_name)
            coa_df.loc[mask, "Cryptocurrency"] = str(new_cryptocurrency) if new_cryptocurrency else ""
            coa_df.loc[mask, "account"] = str(new_account_field) if new_account_field else ""
            coa_df.loc[mask, "Form_1065"] = str(new_form_1065) if new_form_1065 else ""
            coa_df.loc[mask, "ILPA"] = str(new_ilpa) if new_ilpa else ""
            coa_df.loc[mask, "schedule_ranking"] = str(new_schedule_ranking) if new_schedule_ranking else ""
            coa_df.loc[mask, "SCPC"] = str(new_scpc) if new_scpc else ""
            coa_df.loc[mask, "SOI_long_short"] = str(new_soi_long_short) if new_soi_long_short else ""
            coa_df.loc[mask, "SOI_Instrument"] = str(new_soi_instrument) if new_soi_instrument else ""
            coa_df.loc[mask, "SOI_Sub_asset_class"] = str(new_soi_sub_asset_class) if new_soi_sub_asset_class else ""
            coa_df.loc[mask, "SOI_Country"] = str(new_soi_country) if new_soi_country else ""
            coa_df.loc[mask, "SOI_Sector"] = str(new_soi_sector) if new_soi_sector else ""
            coa_df.loc[mask, "SOI_Currency"] = str(new_soi_currency) if new_soi_currency else ""
            coa_df.loc[mask, "SOI_Description"] = str(new_soi_description) if new_soi_description else ""
            
            # Save back to S3
            success = save_COA_file(coa_df)
            if success:
                print(f"DEBUG - COA: Successfully saved edit to S3 for account {new_account_number}")
                # Clear selection to refresh the display
                selected_account_store.set(None)
                # Trigger refresh
                current_count = refresh_trigger.get()
                refresh_trigger.set(current_count + 1)
                print("DEBUG - COA: Edit saved to S3, refresh triggered!")
            else:
                print("DEBUG - COA: Failed to save edit to S3")
                
        except Exception as e:
            print(f"ERROR in handle_save_edit_to_s3: {e}")
            import traceback
            traceback.print_exc()

    @reactive.effect
    @reactive.event(input.delete_account)
    def handle_delete_account():
        """Handle deleting an account with confirmation"""
        try:
            print("DEBUG - COA: Delete account requested")
            
            # Get current account data
            row = selected_account_store.get()
            if not row:
                print("DEBUG - COA: No account selected for deletion")
                return
            
            account_number = row.get("GL_Acct_Number")
            account_name = row.get("account_name", "") or row.get("GL_Acct_Name", "")
            
            # Show confirmation modal
            confirm_modal = ui.modal(
                ui.div(
                    ui.h4("Confirm Account Deletion", class_="mb-4 text-danger"),
                    ui.p(f"Are you sure you want to delete the following account?", class_="mb-3"),
                    ui.div(
                        ui.strong(f"Account #{account_number}: {account_name}"),
                        class_="mb-3 p-3",
                        style="background-color: #f8d7da; border: 1px solid #f5c6cb; border-radius: 5px;"
                    ),
                    ui.div(
                        ui.tags.strong("Warning: ", class_="text-danger"),
                        "This action cannot be undone. The account will be permanently removed from your Chart of Accounts.",
                        class_="alert alert-warning"
                    ),
                    
                    # Confirmation buttons
                    ui.div(
                        ui.input_action_button("confirm_delete", "Yes, Delete Account", class_="btn btn-danger me-3"),
                        ui.input_action_button("cancel_delete", "Cancel", class_="btn btn-secondary"),
                        style="text-align: right; margin-top: 2rem;"
                    ),
                    
                    style="padding: 1.5rem;"
                ),
                title="Delete Account",
                size="lg"
            )
            ui.modal_show(confirm_modal)
                
        except Exception as e:
            print(f"ERROR in handle_delete_account: {e}")
            import traceback
            traceback.print_exc()

    @reactive.effect
    @reactive.event(input.confirm_delete)
    def handle_confirm_delete():
        """Handle confirmed account deletion"""
        try:
            print("DEBUG - COA: Deleting account confirmed")
            
            # Get current account data
            row = selected_account_store.get()
            if not row:
                print("DEBUG - COA: No account selected for confirmed deletion")
                return
            
            account_number = row.get("GL_Acct_Number")
            account_name = row.get("account_name", "") or row.get("GL_Acct_Name", "")
            
            # Load current COA data
            coa_df = load_COA_file()
            if coa_df.empty:
                print("DEBUG - COA: Could not load COA data for deletion")
                return
            
            # Find and remove the row
            mask = coa_df["GL_Acct_Number"] == account_number
            
            if not mask.any():
                print(f"DEBUG - COA: Account {account_number} not found for deletion")
                return
            
            # Remove the account
            updated_coa = coa_df[~mask].copy()
            print(f"DEBUG - COA: Removing account {account_number} - {account_name}")
            print(f"DEBUG - COA: Accounts before deletion: {len(coa_df)}, after: {len(updated_coa)}")
            
            # Save back to S3
            success = save_COA_file(updated_coa)
            if success:
                print(f"DEBUG - COA: Successfully deleted account {account_number} from S3!")
                
                # Clear selection and close modal
                selected_account_store.set(None)
                ui.modal_remove()
                
                # Trigger refresh
                current_count = refresh_trigger.get()
                refresh_trigger.set(current_count + 1)
                print("DEBUG - COA: Account deleted, refresh triggered!")
            else:
                print("DEBUG - COA: Failed to delete account from S3")
                
        except Exception as e:
            print(f"ERROR in handle_confirm_delete: {e}")
            import traceback
            traceback.print_exc()

    @reactive.effect
    @reactive.event(input.cancel_delete)
    def handle_cancel_delete():
        """Handle canceling account deletion"""
        try:
            print("DEBUG - COA: Account deletion cancelled")
            ui.modal_remove()
        except Exception as e:
            print(f"Error in handle_cancel_delete: {e}")

    @reactive.effect
    @reactive.event(input.cancel_account_edit)
    def handle_cancel_edit():
        """Handle canceling account edit"""
        try:
            print("DEBUG - COA: Canceling account edit")
            selected_account_store.set(None)
        except Exception as e:
            print(f"Error in handle_cancel_edit: {e}")

    @reactive.effect
    @reactive.event(input.add_new_account)
    def handle_add_new_account():
        """Handle adding a new account"""
        try:
            print("DEBUG - COA: Add new account button clicked!")
            
            # Get all form values
            new_account_number = input.new_account_number()
            new_account_name = input.new_account_name()
            new_gl_acct_name = input.new_gl_acct_name()
            new_account_category = input.new_account_category()
            
            print(f"DEBUG - COA: Form values - Number: {new_account_number}, Name: {new_account_name}, GL Name: {new_gl_acct_name}")
            
            # Get additional fields
            new_cryptocurrency = input.new_cryptocurrency()
            new_account_field = input.new_account_field()
            new_form_1065 = input.new_form_1065()
            new_ilpa = input.new_ilpa()
            new_schedule_ranking = input.new_schedule_ranking()
            new_scpc = input.new_scpc()
            new_soi_long_short = input.new_soi_long_short()
            new_soi_instrument = input.new_soi_instrument()
            new_soi_sub_asset_class = input.new_soi_sub_asset_class()
            new_soi_country = input.new_soi_country()
            new_soi_sector = input.new_soi_sector()
            new_soi_currency = input.new_soi_currency()
            new_soi_description = input.new_soi_description()
            
            if not all([new_account_number, new_account_name]):
                print("DEBUG - COA: Missing required fields (account number and name) for new account")
                return
            
            # Load current COA data
            coa_df = load_COA_file()
            
            # Check if account number already exists
            if not coa_df.empty and new_account_number in coa_df["GL_Acct_Number"].values:
                print(f"DEBUG - COA: Account number {new_account_number} already exists")
                return
            
            # Create new row with all form values
            new_row = pd.DataFrame({
                "account_name": [str(new_account_name)],
                "GL_Acct_Number": [int(new_account_number)],
                "GL_Acct_Name": [str(new_gl_acct_name) if new_gl_acct_name else ""],
                "Cryptocurrency": [str(new_cryptocurrency) if new_cryptocurrency else ""],
                "account": [str(new_account_field) if new_account_field else ""],
                "Form_1065": [str(new_form_1065) if new_form_1065 else ""],
                "ILPA": [str(new_ilpa) if new_ilpa else ""],
                "schedule_ranking": [str(new_schedule_ranking) if new_schedule_ranking else ""],
                "SCPC": [str(new_scpc) if new_scpc else ""],
                "SOI_long_short": [str(new_soi_long_short) if new_soi_long_short else ""],
                "SOI_Instrument": [str(new_soi_instrument) if new_soi_instrument else ""],
                "SOI_Sub_asset_class": [str(new_soi_sub_asset_class) if new_soi_sub_asset_class else ""],
                "SOI_Country": [str(new_soi_country) if new_soi_country else ""],
                "SOI_Sector": [str(new_soi_sector) if new_soi_sector else ""],
                "SOI_Currency": [str(new_soi_currency) if new_soi_currency else ""],
                "SOI_Description": [str(new_soi_description) if new_soi_description else ""]
            })
            
            # Append to existing data
            if coa_df.empty:
                updated_coa = new_row
            else:
                updated_coa = pd.concat([coa_df, new_row], ignore_index=True)
            
            # Save back to S3
            print(f"DEBUG - COA: About to save to S3 with {len(updated_coa)} total accounts")
            success = save_COA_file(updated_coa)
            if success:
                print(f"DEBUG - COA: Successfully added new account {new_account_number} to S3!")
                print(f"DEBUG - COA: Clearing cache and refreshing data...")
                # Force cache clear to refresh data
                load_COA_file.cache_clear()
                
                # Clear all form inputs
                ui.update_numeric("new_account_number", value=None)
                ui.update_text("new_account_name", value="")
                ui.update_text("new_gl_acct_name", value="")
                ui.update_selectize("new_account_category", selected="Asset Accounts")
                ui.update_text("new_cryptocurrency", value="")
                ui.update_text("new_account_field", value="")
                ui.update_selectize("new_form_1065", selected="")
                ui.update_text("new_ilpa", value="")
                ui.update_text("new_schedule_ranking", value="")
                ui.update_text("new_scpc", value="")
                ui.update_text("new_soi_long_short", value="")
                ui.update_text("new_soi_instrument", value="")
                ui.update_text("new_soi_sub_asset_class", value="")
                ui.update_text("new_soi_country", value="")
                ui.update_text("new_soi_sector", value="")
                ui.update_text("new_soi_currency", value="")
                ui.update_text("new_soi_description", value="")
                
                # Close the modal
                ui.modal_remove()
                
                # Trigger a refresh by incrementing the trigger counter
                current_count = refresh_trigger.get()
                refresh_trigger.set(current_count + 1)
                print("DEBUG - COA: Account added successfully, modal closed, refresh triggered!")
            else:
                print("DEBUG - COA: Failed to save new account to S3")
                
        except Exception as e:
            print(f"ERROR in handle_add_new_account: {e}")
            import traceback
            traceback.print_exc()

    @reactive.effect
    @reactive.event(input.save_push_to_s3)
    def handle_save_push_to_s3():
        """Handle save and push all changes to S3"""
        try:
            print("DEBUG - COA: Save and push to S3 triggered")
            
            # Load current COA data
            coa_df = load_COA_file()
            if coa_df.empty:
                print("DEBUG - COA: No COA data to save")
                return
            
            # Force save to S3 and clear cache
            success = save_COA_file(coa_df)
            if success:
                print("DEBUG - COA: Successfully pushed all changes to S3")
                # Clear cache to force refresh
                load_COA_file.cache_clear()
                # Trigger refresh
                current_count = refresh_trigger.get()
                refresh_trigger.set(current_count + 1)
                print("DEBUG - COA: Push to S3 complete, refresh triggered!")
            else:
                print("DEBUG - COA: Failed to push changes to S3")
                
        except Exception as e:
            print(f"ERROR in handle_save_push_to_s3: {e}")
            import traceback
            traceback.print_exc()

    @output
    @render.ui
    def add_account_trigger():
        """Button to trigger Add Account modal"""
        return ui.card(
            ui.card_header(ui.HTML('<i class="bi bi-plus-lg me-2"></i>Account Management')),
            ui.div(
                ui.p("Create new Chart of Accounts entries with all required fields.", class_="text-muted mb-3"),
                ui.input_action_button("show_add_account_modal", ui.HTML('<i class="bi bi-plus-lg me-1"></i> Add New Account'), class_="btn btn-primary btn-lg"),
                style="text-align: center; padding: 2rem;"
            )
        )


# === Chart of Accounts UI ===
def chart_of_accounts_ui():
    """Chart of Accounts interface with filters and nice styling"""
    return ui.tags.div(
        # Filters Section
        ui.card(
            ui.card_header("Filters & Search"),
            ui.layout_columns(
                ui.input_selectize(
                    "account_category_filter", 
                    "Category:", 
                    {
                        "All Categories": "All Categories",
                        "Asset Accounts": "Asset Accounts", 
                        "Liabilities": "Liabilities",
                        "Capital": "Capital", 
                        "Other Income": "Other Income",
                        "Expenses": "Expenses",
                        "Income": "Income"
                    }, 
                    selected="All Categories",
                    options={"create": False}
                ),
                ui.input_text("account_search", "Search Accounts:", placeholder="Search by name or number..."),
                ui.input_numeric("min_account_number", "Min Account #:", value=None, min=0),
                ui.input_numeric("max_account_number", "Max Account #:", value=None, min=0),
                col_widths=[3, 4, 2, 3]
            )
        ),
        
        # Summary Statistics
        ui.output_ui("chart_of_accounts_summary"),
        
        # Main Content Layout
        ui.layout_columns(
            # Left column - Tables
            ui.tags.div(
                # COA Preview Table
                ui.card(
                    ui.card_header(ui.HTML('<i class="bi bi-list-ul me-2"></i>Chart of Accounts Preview')),
                    ui.p("Click on any row to see details and edit below", class_="text-muted mb-3"),
                    ui.div(
                        ui.output_data_frame("chart_of_accounts_preview"),
                        style="display: flex; justify-content: center; width: 100%;"
                    ),
                    full_screen=True,
                ),
                
                ui.br(),
                
                # Detailed View
                ui.card(
                    ui.card_header(ui.HTML('<i class="bi bi-search me-2"></i>Complete Chart of Accounts Details')),
                    ui.p("All columns from your COA file", class_="text-muted mb-3"),
                    ui.div(
                        ui.output_data_frame("chart_of_accounts_detailed"),
                        style="display: flex; justify-content: center; width: 100%;"
                    ),
                    full_screen=True,
                )
            ),
            
            # Right column - Forms and controls
            ui.tags.div(
                # Add account trigger button
                ui.output_ui("add_account_trigger"),
                ui.br(),
                
                # Selected account edit form (if any account is selected)
                ui.output_ui("selected_account_details"),
                ui.br(),
                
                # Category breakdown
                ui.output_ui("category_breakdown")
            ),
            
            col_widths=[8, 4]
        ),
        
        # Apple-inspired CSS styling
        ui.tags.style("""
            /* Global Typography and Base Styles */
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                line-height: 1.6;
                color: #1d1d1f;
                background-color: #f5f5f7;
            }
            
            /* Card Enhancements */
            .card {
                background: rgba(255, 255, 255, 0.8);
                backdrop-filter: blur(10px);
                border: 1px solid rgba(0, 0, 0, 0.05);
                border-radius: 16px;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
                transition: all 0.3s ease;
                margin-bottom: 24px;
            }
            
            .card:hover {
                transform: translateY(-2px);
                box-shadow: 0 8px 30px rgba(0, 0, 0, 0.12);
            }
            
            .card-header {
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
                border-bottom: 1px solid rgba(0, 0, 0, 0.08);
                border-radius: 16px 16px 0 0 !important;
                padding: 20px 24px;
                font-weight: 600;
                font-size: 18px;
                color: #1d1d1f;
            }
            
            /* Detail Items */
            .detail-item {
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 16px 0;
                border-bottom: 1px solid rgba(0, 0, 0, 0.06);
                transition: background-color 0.2s ease;
            }
            
            .detail-item:hover {
                background-color: rgba(0, 122, 255, 0.02);
                border-radius: 8px;
                margin: 0 -8px;
                padding: 16px 8px;
            }
            
            .detail-item:last-child {
                border-bottom: none;
            }
            
            .detail-label {
                font-weight: 500;
                color: #6e6e73;
                font-size: 15px;
                letter-spacing: -0.01em;
            }
            
            .detail-value {
                font-weight: 600;
                color: #1d1d1f;
                font-size: 15px;
                text-align: right;
            }
            
            /* Form Controls */
            .form-control, .form-select {
                background-color: rgba(255, 255, 255, 0.9);
                border: 1px solid rgba(0, 0, 0, 0.1);
                border-radius: 12px;
                padding: 12px 16px;
                font-size: 16px;
                transition: all 0.2s ease;
                box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
            }
            
            .form-control:focus, .form-select:focus {
                border-color: #007aff;
                box-shadow: 0 0 0 4px rgba(0, 122, 255, 0.1);
                background-color: rgba(255, 255, 255, 1);
                outline: none;
            }
            
            .form-label {
                font-weight: 500;
                color: #1d1d1f;
                margin-bottom: 8px;
                font-size: 15px;
                letter-spacing: -0.01em;
            }
            
            /* Buttons */
            .btn {
                border-radius: 12px;
                padding: 12px 24px;
                font-weight: 600;
                font-size: 15px;
                letter-spacing: -0.01em;
                border: none;
                transition: all 0.2s ease;
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
            }
            
            .btn-primary {
                background: linear-gradient(135deg, #007aff 0%, #0056cc 100%);
                color: white;
            }
            
            .btn-primary:hover {
                background: linear-gradient(135deg, #0056cc 0%, #004499 100%);
                transform: translateY(-1px);
                box-shadow: 0 4px 16px rgba(0, 122, 255, 0.3);
            }
            
            .btn-success {
                background: linear-gradient(135deg, #30d158 0%, #28a745 100%);
                color: white;
            }
            
            .btn-success:hover {
                background: linear-gradient(135deg, #28a745 0%, #1e7e34 100%);
                transform: translateY(-1px);
                box-shadow: 0 4px 16px rgba(48, 209, 88, 0.3);
            }
            
            .btn-danger {
                background: linear-gradient(135deg, #ff3b30 0%, #d70015 100%);
                color: white;
            }
            
            .btn-danger:hover {
                background: linear-gradient(135deg, #d70015 0%, #a50011 100%);
                transform: translateY(-1px);
                box-shadow: 0 4px 16px rgba(255, 59, 48, 0.3);
            }
            
            .btn-secondary {
                background: linear-gradient(135deg, #8e8e93 0%, #6d6d70 100%);
                color: white;
            }
            
            .btn-secondary:hover {
                background: linear-gradient(135deg, #6d6d70 0%, #48484a 100%);
                transform: translateY(-1px);
                box-shadow: 0 4px 16px rgba(142, 142, 147, 0.3);
            }
            
            /* Value Boxes */
            .valuebox {
                background: rgba(255, 255, 255, 0.9);
                backdrop-filter: blur(10px);
                border-radius: 16px;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
                border: 1px solid rgba(0, 0, 0, 0.05);
                transition: all 0.3s ease;
            }
            
            .valuebox:hover {
                transform: translateY(-2px);
                box-shadow: 0 8px 30px rgba(0, 0, 0, 0.12);
            }
            
            .valuebox-title {
                color: #6e6e73;
                font-size: 13px;
                font-weight: 500;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            
            .valuebox-value {
                color: #1d1d1f;
                font-size: 32px;
                font-weight: 700;
                letter-spacing: -0.02em;
            }
            
            /* Empty States */
            .empty-state {
                text-align: center;
                padding: 48px 24px;
                color: #6e6e73;
                background: rgba(255, 255, 255, 0.6);
                border-radius: 16px;
                border: 2px dashed rgba(0, 0, 0, 0.1);
            }
            
            .empty-icon {
                font-size: 48px;
                margin-bottom: 16px;
                opacity: 0.6;
                color: #6e6e73;
            }
            
            /* Modal Enhancements */
            .modal-content {
                background: rgba(255, 255, 255, 0.95);
                backdrop-filter: blur(20px);
                border-radius: 20px;
                border: 1px solid rgba(0, 0, 0, 0.08);
                box-shadow: 0 20px 60px rgba(0, 0, 0, 0.15);
            }
            
            .modal-header {
                border-bottom: 1px solid rgba(0, 0, 0, 0.08);
                border-radius: 20px 20px 0 0;
                padding: 24px;
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
            }
            
            .modal-title {
                font-weight: 700;
                font-size: 24px;
                color: #1d1d1f;
                letter-spacing: -0.02em;
            }
            
            /* Section Headers */
            h6 {
                color: #6e6e73;
                font-size: 13px;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 1px;
                margin-bottom: 16px;
                margin-top: 32px;
                padding-bottom: 8px;
                border-bottom: 2px solid rgba(0, 122, 255, 0.2);
            }
            
            h6:first-of-type {
                margin-top: 0;
            }
            
            /* Data Grid Enhancements */
            .table {
                background: rgba(255, 255, 255, 0.9);
                border-radius: 12px;
                overflow: hidden;
                box-shadow: 0 2px 12px rgba(0, 0, 0, 0.06);
            }
            
            .table th {
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
                color: #1d1d1f;
                font-weight: 600;
                font-size: 14px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                border-bottom: 2px solid rgba(0, 122, 255, 0.1);
            }
            
            .table td {
                color: #1d1d1f;
                font-size: 15px;
                vertical-align: middle;
                border-bottom: 1px solid rgba(0, 0, 0, 0.05);
            }
            
            .table tbody tr:hover {
                background-color: rgba(0, 122, 255, 0.04);
                cursor: pointer;
            }
            
            /* Alerts and Notifications */
            .alert {
                background: rgba(255, 255, 255, 0.9);
                backdrop-filter: blur(10px);
                border-radius: 12px;
                border: 1px solid rgba(0, 0, 0, 0.1);
                box-shadow: 0 4px 16px rgba(0, 0, 0, 0.08);
            }
            
            .alert-warning {
                background: linear-gradient(135deg, rgba(255, 149, 0, 0.1) 0%, rgba(255, 149, 0, 0.05) 100%);
                border-color: rgba(255, 149, 0, 0.2);
                color: #b85c00;
            }
            
            .alert-danger {
                background: linear-gradient(135deg, rgba(255, 59, 48, 0.1) 0%, rgba(255, 59, 48, 0.05) 100%);
                border-color: rgba(255, 59, 48, 0.2);
                color: #b91d1d;
            }
            
            /* Text and Typography */
            .text-muted {
                color: #6e6e73 !important;
                font-size: 14px;
            }
            
            .text-danger {
                color: #ff3b30 !important;
            }
            
            .text-success {
                color: #30d158 !important;
            }
            
            /* Spacing and Layout */
            .mb-4 {
                margin-bottom: 24px !important;
            }
            
            .mt-4 {
                margin-top: 24px !important;
            }
            
            .p-3 {
                padding: 20px !important;
            }
            
            /* Custom Account Cards */
            .account-details-card, .account-edit-card {
                margin-bottom: 24px;
                background: rgba(255, 255, 255, 0.9);
                backdrop-filter: blur(10px);
                border-radius: 16px;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
                border: 1px solid rgba(0, 0, 0, 0.05);
            }
            
            /* Search and Filter Controls */
            .filter-section {
                background: rgba(255, 255, 255, 0.8);
                backdrop-filter: blur(10px);
                border-radius: 16px;
                padding: 24px;
                margin-bottom: 24px;
                border: 1px solid rgba(0, 0, 0, 0.05);
                box-shadow: 0 2px 12px rgba(0, 0, 0, 0.06);
            }
        """)
    )