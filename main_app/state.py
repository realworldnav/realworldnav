# app/state.py

from shiny import reactive

selected_fund = reactive.Value("holdings_class_B_ETH")
selected_date = reactive.Value(None)
