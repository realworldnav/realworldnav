from shiny import reactive, render
import pandas as pd
from faicons import icon_svg
from .helpers import melted_tb

# === Shared reactive calculations ===
def init_nav_reactives(input):
    @reactive.calc
    def nav_data():
        df = melted_tb()
        df["GL_Acct_Number"] = df["GL_Acct_Number"].astype(str).str.strip()
        df = df[df["GL_Acct_Number"].str.match(r"^\d")].copy()
        df["Type"] = df["GL_Acct_Number"].str[0].map({"1": "Asset", "2": "Liability"})

        assets = df[df["Type"] == "Asset"].groupby("Report Date")["Balance"].sum()
        liabs = df[df["Type"] == "Liability"].groupby("Report Date")["Balance"].sum()

        all_dates = pd.to_datetime(sorted(df["Report Date"].unique()))
        nav = assets.reindex(all_dates, fill_value=0) + liabs.reindex(all_dates, fill_value=0)
        return nav.sort_index()

    @reactive.calc
    def nav_delta():
        nav = nav_data()
        if nav.empty or len(nav) < 2:
            return None, None, None

        mode = input.nav_range_mode()
        latest = nav.index.max()

        if mode == "7D":
            past = latest - pd.Timedelta(days=7)
            past = nav.index[nav.index <= past].max() if any(nav.index <= past) else None
        elif mode == "MTD":
            past = latest.replace(day=1)
        elif mode == "QTD":
            past = pd.Timestamp(latest.year, ((latest.month - 1) // 3) * 3 + 1, 1)
        elif mode == "YTD":
            past = pd.Timestamp(latest.year, 1, 1)
        elif mode == "ITD":
            past = nav.index.min()
        else:
            return nav.iloc[-1], None, None

        if past is None or past == latest:
            return nav.iloc[-1], None, None

        return nav[latest], nav[past], nav[latest] - nav[past]

    return nav_data, nav_delta

def register_kpi_outputs(output, input):
    nav_data_raw, nav_delta_raw = init_nav_reactives(input)

    @reactive.calc
    def nav_now_then_delta():
        return nav_delta_raw()

    @output
    @render.ui
    def nav_current():
        nav_now, _, _ = nav_now_then_delta()
        return f"{nav_now:,.2f}" if nav_now is not None else "N/A"

    @output
    @render.ui
    def nav_change():
        _, _, delta = nav_now_then_delta()
        return f"{delta:+,.2f}" if delta is not None else "N/A"

    @output
    @render.ui
    def nav_change_percent():
        nav_now, nav_then, delta = nav_now_then_delta()
        if None in (nav_now, nav_then, delta) or nav_then == 0:
            return "N/A"
        pct = (delta / nav_then) * 100
        return f"{pct:+.2f}%"

    @output
    @render.ui
    def nav_change_icon():
        _, _, delta = nav_now_then_delta()
        if delta is None:
            return icon_svg("circle-minus")
        icon = icon_svg("arrow-up") if delta >= 0 else icon_svg("arrow-down")
        icon.add_class(f"text-{'success' if delta >= 0 else 'danger'}")
        return icon

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
    @render.ui
    def contributed_capital_box():
        cap = contributed_capital()
        return f"{cap:,.2f} ETH"

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
