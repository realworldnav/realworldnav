# app.py
from shiny import App
from main_app.ui import app_ui
from main_app.server import server
import os
import uvicorn

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()  # This loads the .env file
    print("Environment variables loaded from .env file")
except ImportError:
    print("python-dotenv not installed, using system environment variables")

# Get the directory where the app.py file is located
app_dir = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(app_dir, "main_app", "assets")

# Check for launcher environment variables
selected_client = os.environ.get('REALWORLDNAV_CLIENT', 'drip_capital')
selected_fund = os.environ.get('REALWORLDNAV_FUND', 'fund_i_class_B_ETH')

print(f"Starting RealWorldNAV for client: {selected_client}")
print(f"Default fund: {selected_fund}")

app = App(app_ui, server, static_assets=static_dir)

def main():
    """Main entry point when running app.py directly"""
    try:
        print("=" * 60)
        print(">> STARTING REALWORLDNAV APPLICATION")
        print("=" * 60)
        print(f">> Client: {selected_client}")
        print(f">> Default fund: {selected_fund}")
        print(f">> Static directory: {static_dir}")
        print(f">> Application will be available at http://localhost:8001")
        
        # Test imports first
        print("\n>> TESTING IMPORTS...")
        try:
            from main_app.ui import app_ui
            print("[OK] main_app.ui imported successfully")
        except Exception as e:
            print(f"[ERROR] Failed to import main_app.ui: {e}")

        
        try:
            from main_app.server import server
            print("[OK] main_app.server imported successfully")
        except Exception as e:
            print(f"[ERROR] Failed to import main_app.server: {e}")
            import traceback
            traceback.print_exc()
            return
        
        print("[OK] All imports successful!")
        print("\n>> Starting Shiny application...")
        
        # Run the main app using Shiny's built-in method
        app.run(host="127.0.0.1", port=8001, launch_browser=False)
        
    except KeyboardInterrupt:
        print("\n\n[STOP] Application shutting down...")
    except Exception as e:
        print(f"\n[ERROR] ERROR RUNNING APPLICATION: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()