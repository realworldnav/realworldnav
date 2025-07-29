from shiny import reactive, render
from shinywidgets import render_plotly
import plotly.graph_objects as go
from .helpers import melted_tb

def register_net_income_outputs(output, input):
    @reactive.calc
    def net_income_series():
        df = melted_tb()
        df["GL_Acct_Number"] = df["GL_Acct_Number"].astype(str)
        df = df[df["GL_Acct_Number"].str.startswith(("4", "8", "9"))]
        daily_income = df.groupby("Report Date")["Balance"].sum() * -1
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
