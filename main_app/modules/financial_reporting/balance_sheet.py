from shiny import ui as shiny_ui, render
import pandas as pd
from ...s3_utils import load_tb_file, load_COA_file

from shiny.render import DataGrid, data_frame
def balance_sheet_ui():
    return shiny_ui.card(
        shiny_ui.card_header("📑 Balance Sheet Snapshot"),

        shiny_ui.card_body(
            shiny_ui.output_data_frame("balance_sheet_table"),  # use standard placeholder
            fill=True
        ),

        shiny_ui.card_footer(
            shiny_ui.output_text_verbatim("balance_sheet_debug")
        )
    )


def register_outputs(output, selected_fund, selected_report_date):
    @output
    @data_frame
    def balance_sheet_table():
        df_tb = load_tb_file()

        if df_tb is None or df_tb.empty:
            return pd.DataFrame({"Error": ["Trial balance is empty"]})

        # === Melt wide-form TB with date columns ===
        try:
            id_vars = ["GL_Acct_Number", "GL_Acct_Name", "Cryptocurrency"]
            value_vars = [col for col in df_tb.columns if col not in id_vars]
            df_tb = df_tb.melt(
                id_vars=id_vars,
                value_name="Balance",
                var_name="Date"
            )
            df_tb["Date"] = pd.to_datetime(df_tb["Date"], errors="coerce")
            df_tb = df_tb.dropna(subset=["Date"])
            latest_date = df_tb["Date"].max()
            df_tb = df_tb[df_tb["Date"] == latest_date]
        except Exception as e:
            print("‼️ Failed to melt trial balance:", e)

        # Clean and cast
        df_tb["GL_Acct_Number"] = pd.to_numeric(df_tb["GL_Acct_Number"], errors="coerce")
        df_tb = df_tb[df_tb["GL_Acct_Number"].notnull()]
        df_tb["GL_Acct_Number"] = df_tb["GL_Acct_Number"].astype(int)
        df_tb["Balance"] = pd.to_numeric(df_tb["Balance"], errors="coerce").fillna(0.0).round(6)

        # COA
        df_coa = load_coa_file()
        df_coa["GL_Acct_Number"] = pd.to_numeric(df_coa["GL_Acct_Number"], errors="coerce").astype("Int64")
        coa_map = dict(zip(df_coa["GL_Acct_Number"], df_coa["GL_Acct_Name"]))
        df_tb["Label"] = df_tb["GL_Acct_Number"].map(coa_map).fillna("Unknown")

        # Classification
        df_tb["Category"] = df_tb["GL_Acct_Number"].astype(str).str[0].map({
            "1": "Assets", "2": "Liabilities", "3": "Partners’ Capital"
        }).fillna("Other")

        # Combine note payables
        note_mask = df_tb["GL_Acct_Number"].between(25000, 25099)
        if note_mask.any():
            note_total = df_tb.loc[note_mask, "Balance"].sum()
            df_tb = df_tb.loc[~note_mask]
            df_tb = pd.concat([df_tb, pd.DataFrame([{
                "GL_Acct_Number": 25000,
                "Label": "Note payable - net",
                "Category": "Liabilities",
                "Balance": note_total
            }])], ignore_index=True)

        df_tb = df_tb[df_tb["Category"].isin(["Assets", "Liabilities"])]

        summary = df_tb.groupby(["Category", "Label"], as_index=False)["Balance"].sum()
        summary = summary[summary["Balance"].abs() > 0].sort_values(["Category", "Label"])

        return DataGrid(
        summary[["Category", "Label", "Balance"]],
        width="100%",
        height="100%",
        filters=True,
        summary=True,
        selection_mode="rows"
    )

    @output
    @render.text
    def balance_sheet_debug():
        df_tb = load_tb_file()

        try:
            # Melt TB (wide → long)
            id_vars = ["GL_Acct_Number", "GL_Acct_Name", "Cryptocurrency"]
            value_vars = [col for col in df_tb.columns if col not in id_vars]
            df_tb = df_tb.melt(
                id_vars=id_vars,
                value_name="Balance",
                var_name="Date"
            )
            df_tb["Date"] = pd.to_datetime(df_tb["Date"], errors="coerce")
            df_tb = df_tb.dropna(subset=["Date"])
            latest_date = df_tb["Date"].max()
            df_tb = df_tb[df_tb["Date"] == latest_date]
        except:
            return "❌ Unable to prepare TB for net asset check"

        df_tb["GL_Acct_Number"] = pd.to_numeric(df_tb["GL_Acct_Number"], errors="coerce")
        df_tb = df_tb[df_tb["GL_Acct_Number"].notnull()]
        df_tb["GL_Acct_Number"] = df_tb["GL_Acct_Number"].astype(str)

        df_tb["Category"] = df_tb["GL_Acct_Number"].str[0].map({
            "1": "Assets",
            "2": "Liabilities",
            "3": "Partners’ Capital",
            "4": "Other Income",
            "8": "Expenses",
            "9": "Revenue"
        }).fillna("Other")

        df_tb["Balance"] = pd.to_numeric(df_tb["Balance"], errors="coerce").fillna(0.0)

        assets = df_tb[df_tb["Category"] == "Assets"]["Balance"].sum()
        liabilities = df_tb[df_tb["Category"] == "Liabilities"]["Balance"].sum()
        capital = -df_tb[df_tb["Category"] == "Partners’ Capital"]["Balance"].sum()
        net_income = -df_tb[df_tb["Category"].isin(["Revenue", "Expenses", "Other Income"])]["Balance"].sum()


        net_assets = assets + liabilities
        capital_plus_income = capital + net_income

        result = "\n".join([
            f"{'Assets:':<18} {assets:>15,.6f}",
            f"{'Liabilities:':<18} {liabilities:>15,.6f}",
            f"{'Net Assets:':<18} {net_assets:>15,.6f}",
            "",
            f"{'Capital:':<18} {capital:>15,.6f}",
            f"{'Net Income:':<18} {net_income:>15,.6f}",
            f"{'Capital + Income:':<18} {capital_plus_income:>15,.6f}",
            "",
            f"{'Status:':<18} {'✅ Balanced' if abs(net_assets - capital_plus_income) < 1e-6 else '❌ Imbalance'}"
        ])

        return result
