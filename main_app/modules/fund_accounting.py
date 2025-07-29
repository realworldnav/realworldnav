from shiny import ui, render, reactive
from shinywidgets import output_widget, render_plotly
from faicons import icon_svg
import pandas as pd
import plotly.graph_objects as go
from ..s3_utils import load_tb_file


# === UI ===
def fund_accounting_ui():
    return ui.page_fluid(
        ui.h2("📈 NAV Dashboard", class_="mt-3"),

        ui.input_radio_buttons(
            id="nav_range_mode",
            label="Select NAV Range:",
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
        ),            ),
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


# === SERVER ===
def register_outputs(output, input, selected_fund=None, selected_report_date=None):
    @reactive.calc
    def trial_balance():
        df = load_tb_file()
        date_col_map = {}
        for col in df.columns:
            try:
                parsed = pd.to_datetime(col, errors="raise", dayfirst=False)
                date_col_map[col] = parsed
            except Exception:
                continue
        df = df.rename(columns=date_col_map)
        return df

    @reactive.calc
    def melted_tb():
        df = trial_balance()
        date_cols = [col for col in df.columns if isinstance(col, pd.Timestamp)]
        id_vars = [col for col in df.columns if col not in date_cols]
        df_long = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=date_cols,
            var_name="Report Date",
            value_name="Balance"
        )
        df_long = df_long.dropna(subset=["Balance"])
        df_long = df_long[df_long["Balance"] != 0]
        return df_long

    @reactive.calc
    def nav_data():
        df = melted_tb()

        print(">>> NAV DEBUG: melted_tb shape:", df.shape)
        print(">>> NAV DEBUG: melted_tb date range:", df["Report Date"].min(), "→", df["Report Date"].max())

        df["GL_Acct_Number"] = df["GL_Acct_Number"].astype(str).str.strip()
        df = df[df["GL_Acct_Number"].str.match(r"^\d")].copy()

        df["Type"] = df["GL_Acct_Number"].str[0].map({"1": "Asset", "2": "Liability"})

        print(">>> NAV DEBUG: unique GL prefixes:", df["GL_Acct_Number"].str[0].unique())
        print(">>> NAV DEBUG: type counts:\n", df["Type"].value_counts(dropna=False))

        assets_by_date = df[df["Type"] == "Asset"].groupby("Report Date")["Balance"].sum()
        liabs_by_date = df[df["Type"] == "Liability"].groupby("Report Date")["Balance"].sum()

        print(">>> NAV DEBUG: asset dates:", assets_by_date.index.min(), "→", assets_by_date.index.max())
        print(">>> NAV DEBUG: liab dates:", liabs_by_date.index.min(), "→", liabs_by_date.index.max())

        all_dates = pd.to_datetime(sorted(df["Report Date"].unique()))
        assets_by_date = assets_by_date.reindex(all_dates, fill_value=0)
        liabs_by_date = liabs_by_date.reindex(all_dates, fill_value=0)

        nav_series = assets_by_date + liabs_by_date

        print(">>> NAV DEBUG: nav_series length:", len(nav_series))
        print(">>> NAV DEBUG: nav_series date range:", nav_series.index.min(), "→", nav_series.index.max())

        return nav_series.sort_index()

    @reactive.calc
    def nav_delta():
        nav = nav_data()
        if nav.empty or len(nav) < 2:
            return None, None, None

        period = input.nav_compare_period()
        delta_days = {"7D": 7, "30D": 30}.get(period, 7)
        latest_date = nav.index.max()
        compare_date = latest_date - pd.Timedelta(days=delta_days)
        past_date = nav.index[nav.index <= compare_date].max() if any(nav.index <= compare_date) else None

        if past_date is None:
            return nav.iloc[-1], None, None

        nav_now = nav.loc[latest_date]
        nav_then = nav.loc[past_date]
        delta = nav_now - nav_then
        return nav_now, nav_then, delta
    @reactive.calc
    def filtered_nav_data():
        nav = nav_data()
        if nav.empty:
            return nav

        start, end = nav_range()
        return nav[(nav.index >= start) & (nav.index <= end)]


    @output 
    @render.ui
    def nav_current():
        nav_now, _, _ = nav_delta()
        return f"{nav_now:,.2f}" if nav_now is not None else "N/A"

    @output
    @render.ui
    def nav_change():
        _, _, delta = nav_delta()
        return f"{delta:+,.2f}" if delta is not None else "N/A"

    @output
    @render.ui
    def nav_change_percent():
        nav_now, nav_then, delta = nav_delta()
        if None in (nav_now, nav_then, delta) or nav_then == 0:
            return "N/A"
        pct = (delta / nav_then) * 100
        return f"{pct:+.2f}%"

    @output
    @render.ui
    def nav_change_icon():
        _, _, delta = nav_delta()
        if delta is None:
            return icon_svg("circle-minus")
        icon = icon_svg("arrow-up") if delta >= 0 else icon_svg("arrow-down")
        icon.add_class(f"text-{'success' if delta >= 0 else 'danger'}")
        return icon

    @output
    @render_plotly
    def nav_chart():
        nav = filtered_nav_data()
        if nav.empty:
            return go.Figure()

        x_vals = nav.index.to_pydatetime()
        y_vals = nav.values

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=x_vals,
            y=y_vals,
            mode="lines+markers",
            name="NAV",
            line=dict(width=3),
        ))

        fig.update_layout(
            title="NAV Over Time",
            xaxis_title="Date",
            yaxis_title="Net Asset Value",
            hovermode="x unified",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(type="date", range=[x_vals[0], x_vals[-1]])
        )
        return fig
    
    @reactive.calc
    def nav_range():
        nav = nav_data()
        if nav.empty:
            return None, None

        mode = input.nav_range_mode()
        user_range = input.nav_custom_range()
        end = nav.index.max()

        if mode == "Custom" and user_range is not None:
            start = pd.to_datetime(user_range[0])
            end = pd.to_datetime(user_range[1])
        elif mode == "MTD":
            start = end.replace(day=1)
        elif mode == "QTD":
            start = pd.Timestamp(end.year, ((end.month - 1) // 3) * 3 + 1, 1)
        elif mode == "YTD":
            start = pd.Timestamp(end.year, 1, 1)
        elif mode == "ITD":
            start = nav.index.min()
        else:
            start = nav.index.min()

        return start, end


    @reactive.calc
    def nav_range():
        nav = nav_data()
        if nav.empty:
            return None, None

        mode = input.nav_range_mode()
        user_range = input.nav_custom_range()
        end = nav.index.max()

        if mode == "Custom" and user_range is not None:
            start = pd.to_datetime(user_range[0])
            end = pd.to_datetime(user_range[1])
        elif mode == "MTD":
            start = end.replace(day=1)
        elif mode == "QTD":
            start = pd.Timestamp(end.year, ((end.month - 1) // 3) * 3 + 1, 1)
        elif mode == "YTD":
            start = pd.Timestamp(end.year, 1, 1)
        elif mode == "ITD":
            start = nav.index.min()
        else:
            start = nav.index.min()

        return start, end

    @reactive.calc
    def contributed_capital():
        df = melted_tb()
        df["GL_Acct_Number"] = df["GL_Acct_Number"].astype(str)
        latest_date = df["Report Date"].max()
        cap_rows = df[
            (df["GL_Acct_Number"].str.startswith("3")) &
            (df["Report Date"] == latest_date)
        ]
        return cap_rows["Balance"].sum() * -1

    @output
    @render.data_frame
    def latest_summary():
        df = melted_tb()
        latest_date = df["Report Date"].max()
        df["GL_Acct_Number"] = df["GL_Acct_Number"].astype(str)
        df["Type"] = df["GL_Acct_Number"].str[0].map({"1": "Asset", "2": "Liability"})
        latest = df[df["Report Date"] == latest_date]
        total_assets = latest[latest["Type"] == "Asset"]["Balance"].sum()
        total_liabs = latest[latest["Type"] == "Liability"]["Balance"].sum()
        nav = total_assets + total_liabs
        summary = pd.DataFrame({
            "Metric": ["Latest Date", "Total Assets", "Total Liabilities", "NAV"],
            "Value": [latest_date.strftime("%Y-%m-%d"), total_assets, total_liabs, nav]
        })
        summary["Value"] = summary["Value"].apply(lambda v: f"{v:,.2f}" if isinstance(v, (int, float)) else v)
        return summary
    @output
    @render.ui
    def contributed_capital_box():
        cap = contributed_capital()
        return f"{cap:,.2f} ETH"
    
    @reactive.calc
    def net_income_series():
        df = melted_tb()
        df["GL_Acct_Number"] = df["GL_Acct_Number"].astype(str)
        df = df[df["GL_Acct_Number"].str.startswith(("4", "8", "9"))]
        daily_income = df.groupby("Report Date")["Balance"].sum() *-1
        return daily_income.sort_index()
    
    @output
    @render_plotly
    def net_income_chart():
        income = net_income_series()
        if income.empty:
            return go.Figure()
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=income.index.to_pydatetime(),
            y=income.values,
            mode="lines+markers",
            name="Net Income",
            line=dict(width=2, color="green")
        ))
        fig.update_layout(
            title="Net Income Over Time",
            xaxis_title="Date",
            yaxis_title="Net Income",
            hovermode="x unified",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(type="date")
        )
        return fig

    @output
    @render.data_frame
    def tb_preview():
        return melted_tb().head(50)
