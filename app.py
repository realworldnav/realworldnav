# app.py - RealWorldNAV Entry Point
import os

# Load environment variables FIRST
from dotenv import load_dotenv
load_dotenv()

# Set up clean logging BEFORE other imports
from main_app.logging_config import setup_logging
logger = setup_logging()

# Now import the app components
from shiny import App
from main_app.ui import app_ui
from main_app.server import server

# Configuration
APP_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(APP_DIR, "static")
CLIENT = os.environ.get('REALWORLDNAV_CLIENT', 'drip_capital')
FUND = os.environ.get('REALWORLDNAV_FUND', 'fund_i_class_B_ETH')

# Create app
app = App(app_ui, server, static_assets=STATIC_DIR)

if __name__ == "__main__":
    logger.info(f"RealWorldNAV starting - Client: {CLIENT}, Fund: {FUND}")
    app.run(host="127.0.0.1", port=8001, launch_browser=False)
