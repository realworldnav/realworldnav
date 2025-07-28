# app.py
from shiny import App
from main_app.ui import ui
from main_app.server import server


app = App(ui, server)
