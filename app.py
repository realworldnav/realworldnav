# app.py
from shiny import App
from main_app.ui import app_ui
from main_app.server import server


app = App(app_ui, server)


#add investment statements, download monthly reporting package