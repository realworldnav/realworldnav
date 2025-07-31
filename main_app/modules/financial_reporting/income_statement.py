from shiny import ui as shiny_ui, render
import pandas as pd
from datetime import datetime
from ...s3_utils import load_tb_file
from shiny.render import data_frame, DataGrid
from datetime import timedelta

def income_statement_ui():
    return shiny_ui.card(
        shiny_ui.card_header("📈 Income Statement"),
        shiny_ui.card_body(
            shiny_ui.output_data_frame("income_statement_table"),
            fill=True
        ),
        shiny_ui.card_footer(
            shiny_ui.output_text_verbatim("income_statement_debug")
        )
    )

def register_outputs(output, selected_fund, selected_report_date):
    @output
    @data_frame
    def income_statement_table():
        df_tb = load_tb_file()
        if df_tb is None or df_tb.empty:
            return pd.DataFrame({"Error": ["Trial balance is empty"]})

        current_period = selected_report_date()
        current_period_date = pd.to_datetime(current_period).date()
        start_of_month = current_period_date.replace(day=1)
        month = current_period_date.month
        quarter_start_month = ((month - 1) // 3) * 3 + 1
        start_qtd = datetime(current_period_date.year, quarter_start_month, 1).date() - timedelta(days=1)
        start_mtd = start_of_month - timedelta(days=1)
        start_ytd = datetime(current_period_date.year, 1, 1).date() - timedelta(days=1)

        # === Build date_mapping and parsed_dates ===
        date_mapping = {}
        for col in df_tb.columns[3:]:
            try:
                date_obj = pd.to_datetime(col, errors="raise").date()
                date_mapping[date_obj] = col
            except Exception:
                continue
        parsed_dates = sorted(date_mapping.keys())
        if not parsed_dates:
            return pd.DataFrame({"Error": ["No valid date-formatted columns found."]})
        start_itd = min(parsed_dates)

        def get_nearest_or_first(date_obj):
            eligible = [d for d in parsed_dates if d <= date_obj]
            return eligible[-1] if eligible else min(parsed_dates)

        def get_change(start_date, end_date, row):
            start_dt = get_nearest_or_first(start_date)
            end_dt = get_nearest_or_first(end_date)
            start_val = row.get(date_mapping[start_dt], 0.0)
            end_val = row.get(date_mapping[end_dt], 0.0)
            diff = end_val - start_val
            if str(row["GL_Acct_Number"]).startswith(("9", "4")):
                diff *= -1
            return round(diff, 6)

        # === Compute Change Table ===
        change_data = []
        for _, row in df_tb.iterrows():
            row_data = {
                "GL_Acct_Number": row["GL_Acct_Number"],
                "GL_Acct_Name": row["GL_Acct_Name"],
                "MTD": get_change(start_mtd, current_period_date, row),
                "QTD": get_change(start_qtd, current_period_date, row),
                "YTD": get_change(start_ytd, current_period_date, row),
                "ITD": get_change(start_itd, current_period_date, row),
            }
            # Exclude rows where all periods are zero
            if any(row_data[p] != 0 for p in ["MTD", "QTD", "YTD", "ITD"]):
                change_data.append(row_data)

        if not change_data:
            return pd.DataFrame({"Message": ["All accounts are zero across MTD/QTD/YTD/ITD."]})

        change_df = pd.DataFrame(change_data)

        # === Merge with original to get categories ===
        df_tb["Category"] = df_tb["GL_Acct_Number"].astype(str).str[0].map({
            "9": "Income",
            "4": "Income",
            "8": "Expenses"
        }).fillna("Other")

        # Only keep categorized Income and Expenses
        master = df_tb[df_tb["Category"].isin(["Income", "Expenses"])][
            ["GL_Acct_Number", "GL_Acct_Name", "Category"]
        ].drop_duplicates()

        # Merge for categorization
        merged = change_df.merge(master, on=["GL_Acct_Number", "GL_Acct_Name"], how="inner")
                # === Grouping
        pivot = merged.pivot_table(index=["Category", "GL_Acct_Name"], values=["MTD", "QTD", "YTD", "ITD"], aggfunc="sum").reset_index()

        # === Net Income
        income = pivot[pivot["Category"] == "Income"]
        expenses = pivot[pivot["Category"] == "Expenses"]
        net_income = (income[["MTD", "QTD", "YTD", "ITD"]].sum(numeric_only=True) -
                    expenses[["MTD", "QTD", "YTD", "ITD"]].sum(numeric_only=True)).to_frame().T

        net_income.insert(0, "GL_Acct_Name", "Net Income")
        net_income.insert(0, "Category", "🟢 TOTAL")

        final = pd.concat([pivot, net_income], ignore_index=True)

        # === Optional Sort
        cat_order = {"Income": 1, "Expenses": 2, "🟢 TOTAL": 3}
        final["CatSort"] = final["Category"].map(cat_order)
        final = final.sort_values(by=["CatSort", "GL_Acct_Name"]).drop(columns="CatSort")

        return DataGrid(
            final,
            width="100%",
            height=700,
            filters=True,
            summary=True,
            selection_mode="rows"
        )
