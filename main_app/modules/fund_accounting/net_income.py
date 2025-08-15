from shiny import reactive, render
from shinywidgets import render_plotly
import pandas as pd
import plotly.graph_objects as go
from .helpers import daily_balances, apply_plotly_theme

def register_net_income_outputs(output, input):
    @reactive.calc
    def net_income_series():
        df = daily_balances()
        if df.empty:
            return pd.Series(dtype=float)
        df["GL_Acct_Number"] = df["GL_Acct_Number"].astype(str)
        
        # Income accounts (4xxx and 9xxx) - these have credit normal balance
        income_accounts = df[df["GL_Acct_Number"].str.match(r'^[49]')]
        
        # Expense accounts (8xxx) - these have debit normal balance
        expense_accounts = df[df["GL_Acct_Number"].str.startswith('8')]
        
        # Group by date and calculate net income
        if not income_accounts.empty:
            daily_income = income_accounts.groupby("Report Date")["Balance"].sum()
        else:
            daily_income = pd.Series(dtype=float)
            
        if not expense_accounts.empty:
            daily_expenses = expense_accounts.groupby("Report Date")["Balance"].sum()
        else:
            daily_expenses = pd.Series(dtype=float)
        
        # Flip signs for positive display: positive gains show as positive, expenses as negative
        # Income accounts normally have credit balance, expenses have debit balance
        if not daily_income.empty:
            daily_income = daily_income  # Keep income as-is (positive gains are positive)
        if not daily_expenses.empty:
            daily_expenses = -daily_expenses  # Flip expenses (positive expenses show as negative)
        
        # Net income = Income - Expenses (now both properly signed)
        all_dates = sorted(set(daily_income.index) | set(daily_expenses.index))
        net_income = pd.Series(index=all_dates, dtype=float)
        
        for date in all_dates:
            income = daily_income.get(date, 0)
            expenses = daily_expenses.get(date, 0)  # Already flipped to negative
            net_income[date] = income - expenses
            
        return net_income.sort_index()

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

        # Apply unified theme
        apply_plotly_theme(fig, "Net Income Over Time")
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Net Income",
            hovermode="x unified"
        )
        fig.update_xaxes(type="date")
        return fig
