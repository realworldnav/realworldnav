from shiny import reactive, render
from shinywidgets import render_plotly
import pandas as pd
import plotly.graph_objects as go
from .helpers import melted_tb

# === Shared reactive calculations ===
def init_nav_reactives(input):
    @reactive.calc
    def nav_data():
        df = melted_tb()
        df["GL_Acct_Number"] = df["GL_Acct_Number"].astype(str).str.strip()
        df = df[df["GL_Acct_Number"].str.match(r"^\d")].copy()
        df["Type"] = df["GL_Acct_Number"].str[0].map({"1": "Asset", "2": "Liability"})

        assets_by_date = df[df["Type"] == "Asset"].groupby("Report Date")["Balance"].sum()
        liabs_by_date = df[df["Type"] == "Liability"].groupby("Report Date")["Balance"].sum()

        all_dates = pd.to_datetime(sorted(df["Report Date"].unique()))
        assets_by_date = assets_by_date.reindex(all_dates, fill_value=0)
        liabs_by_date = liabs_by_date.reindex(all_dates, fill_value=0)

        nav_series = assets_by_date + liabs_by_date
        return nav_series.sort_index()

    @reactive.calc
    def nav_delta():
        nav = nav_data()
        if nav.empty or len(nav) < 2:
            return None, None, None

        mode = input.nav_range_mode()
        delta_days = {"7D": 7, "30D": 30}.get(mode)

        if delta_days:
            latest_date = nav.index.max()
            compare_date = latest_date - pd.Timedelta(days=delta_days)
            past_date = nav.index[nav.index <= compare_date].max() if any(nav.index <= compare_date) else None
            if past_date is None:
                return nav.iloc[-1], None, None
            nav_now = nav.loc[latest_date]
            nav_then = nav.loc[past_date]
            delta = nav_now - nav_then
            return nav_now, nav_then, delta
        else:
            # For MTD/QTD/YTD/ITD/Custom just return latest value and no delta
            nav_now = nav.iloc[-1]
            return nav_now, None, None
    return nav_data, nav_delta


# === Plotly output (separate from shared calc) ===
def register_nav_outputs(output, input):
    nav_data, _ = init_nav_reactives(input)

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
    def filtered_nav_data():
        nav = nav_data()
        if nav.empty:
            return nav
        start, end = nav_range()
        return nav[(nav.index >= start) & (nav.index <= end)]

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
