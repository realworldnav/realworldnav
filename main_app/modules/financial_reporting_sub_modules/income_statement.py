from shiny import ui as shiny_ui, render
import pandas as pd
from datetime import datetime
from ...s3_utils import load_tb_file
from shiny.render import data_frame, DataGrid

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

def register_outputs(output):
    @output
    @data_frame
    def income_statement_table():
        df_tb = load_tb_file()
        if df_tb is None or df_tb.empty:
            return pd.DataFrame({"Error": ["Trial balance is empty"]})

        # --- Melt if wide-form ---
        id_vars = ["GL_Acct_Number", "GL_Acct_Name", "Cryptocurrency"]
        value_vars = [col for col in df_tb.columns if col not in id_vars]
        df_tb = df_tb.melt(id_vars=id_vars, var_name="Date", value_name="Balance")
        df_tb["Date"] = pd.to_datetime(df_tb["Date"], errors="coerce")
        df_tb = df_tb.dropna(subset=["Date"])
        df_tb["Balance"] = pd.to_numeric(df_tb["Balance"], errors="coerce").fillna(0.0)

        df_tb["GL_Acct_Number"] = pd.to_numeric(df_tb["GL_Acct_Number"], errors="coerce").astype("Int64")
        df_tb["Category"] = df_tb["GL_Acct_Number"].astype(str).str[0].map({
            "4": "Other Income",
            "8": "Expenses",
            "9": "Revenue"
        })

        df_tb = df_tb[df_tb["Category"].notna()]

        latest_date = df_tb["Date"].max()
        start_of_month = latest_date.replace(day=1)
        start_of_qtr = latest_date.replace(month=((latest_date.month - 1)//3)*3 + 1, day=1)
        start_of_year = latest_date.replace(month=1, day=1)

        def summarize(label, start_date):
            df_filtered = df_tb[df_tb["Date"] >= start_date]
            summary = df_filtered.groupby(["Category", "GL_Acct_Name"])["Balance"].sum().reset_index()
            summary["Period"] = label
            return summary

        mtd = summarize("MTD", start_of_month)
        qtd = summarize("QTD", start_of_qtr)
        ytd = summarize("YTD", start_of_year)
        itd = summarize("ITD", df_tb["Date"].min())

        all_data = pd.concat([mtd, qtd, ytd, itd], ignore_index=True)

        # Pivot for display
        pivot = all_data.pivot_table(index=["Category", "GL_Acct_Name"], columns="Period", values="Balance", fill_value=0).reset_index()

        # Add Net Income row per period
        income = pivot[pivot["Category"].isin(["Revenue", "Other Income"])]
        expenses = pivot[pivot["Category"] == "Expenses"]

        net_income = (
            income.drop(columns=["Category", "GL_Acct_Name"])
            .sum(numeric_only=True) -
            expenses.drop(columns=["Category", "GL_Acct_Name"]).sum(numeric_only=True)
        ).to_frame().T
        net_income.insert(0, "GL_Acct_Name", "Net Income")
        net_income.insert(0, "Category", "🟢 TOTAL")

        final = pd.concat([pivot, net_income], ignore_index=True)
        final = final.sort_values(by=["Category", "GL_Acct_Name"])

        return DataGrid(
            final,
            width="100%",
            height=700,
            filters=True,
            summary=True,
            selection_mode="rows"
        )

    @output
    @render.text
    def income_statement_debug():
        return "Income Statement loaded with MTD/QTD/YTD/ITD periods and Net Income calculated."
