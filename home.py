#!/usr/bin/env python3
"""
Home.py - RealWorldNAV Launcher
A client selection launcher that opens in browser and launches app.py with selected client
"""

import os
import sys
import subprocess
import webbrowser
import threading
import time
from shiny import App, ui, render, reactive
from shiny.types import FileInfo
import uvicorn
from pathlib import Path

print("DEBUG — home.py loaded")

# Get application paths
app_dir = Path(__file__).parent
static_dir = app_dir / "static"
assets_dir = app_dir / "main_app" / "assets"

# Client configuration
CLIENTS = {
    "drip_capital": {
        "name": "Drip Capital",
        "logo": "clients/drip_capital/drip.png",
        "description": "Digital asset investment fund focused on DeFi lending and NFT collateral",
        "funds": {
            "fund_i_class_B_ETH": "Fund I - Class B",
            "fund_ii_class_B_ETH": "Fund II - Class B", 
            "holdings_class_B_ETH": "Holdings - Class B"
        }
    }
}

def create_launcher_ui():
    """Create the launcher UI with logo and client selection"""
    return ui.page_fluid(
        # Custom CSS for launcher styling
        ui.tags.style("""
            body {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                min-height: 100vh;
                margin: 0;
                padding: 20px;
            }
            
            .launcher-container {
                max-width: 800px;
                margin: 0 auto;
                padding-top: 60px;
            }
            
            .logo-container {
                text-align: center;
                margin-bottom: 50px;
            }
            
            .main-logo {
                max-width: 200px;
                height: auto;
                margin-bottom: 20px;
                filter: drop-shadow(0 4px 8px rgba(0,0,0,0.2));
            }
            
            .app-title {
                color: white;
                font-size: 3rem;
                font-weight: 300;
                margin-bottom: 10px;
                text-shadow: 0 2px 4px rgba(0,0,0,0.3);
            }
            
            .app-subtitle {
                color: rgba(255,255,255,0.8);
                font-size: 1.2rem;
                font-weight: 300;
                margin-bottom: 0;
            }
            
            .client-selection-card {
                background: white;
                border-radius: 16px;
                padding: 40px;
                box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                margin-bottom: 30px;
            }
            
            .selection-title {
                color: #333;
                font-size: 1.8rem;
                font-weight: 600;
                text-align: center;
                margin-bottom: 30px;
            }
            
            .client-option {
                border: 2px solid #e9ecef !important;
                border-radius: 12px !important;
                padding: 20px !important;
                margin-bottom: 20px !important;
                cursor: pointer !important;
                transition: all 0.3s ease !important;
                background: #f8f9fa !important;
                width: 100% !important;
                text-align: left !important;
            }
            
            .client-option:hover {
                border-color: #667eea !important;
                background: #f0f4ff !important;
                transform: translateY(-2px) !important;
                box-shadow: 0 8px 25px rgba(102, 126, 234, 0.15) !important;
            }
            
            .client-option.selected {
                border-color: #667eea !important;
                background: #f0f4ff !important;
                box-shadow: 0 0 20px rgba(102, 126, 234, 0.2) !important;
            }
            
            .client-option .btn {
                border: none !important;
                background: none !important;
                padding: 0 !important;
                margin: 0 !important;
                width: 100% !important;
            }
            
            .client-logo {
                width: 48px;
                height: 48px;
                object-fit: contain;
                margin-right: 15px;
                vertical-align: middle;
            }
            
            .client-info {
                display: inline-block;
                vertical-align: middle;
                width: calc(100% - 70px);
            }
            
            .client-name {
                font-size: 1.4rem;
                font-weight: 600;
                color: #333;
                margin: 0 0 5px 0;
            }
            
            .client-description {
                color: #666;
                font-size: 0.95rem;
                margin: 0;
                line-height: 1.4;
            }
            
            .launch-section {
                text-align: center;
                margin-top: 30px;
            }
            
            .launch-button {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                border: none;
                color: white;
                padding: 15px 40px;
                font-size: 1.1rem;
                font-weight: 600;
                border-radius: 50px;
                cursor: pointer;
                transition: all 0.3s ease;
                box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
            }
            
            .launch-button:hover {
                transform: translateY(-2px);
                box-shadow: 0 8px 25px rgba(102, 126, 234, 0.4);
            }
            
            .launch-button:disabled {
                background: #ccc;
                cursor: not-allowed;
                transform: none;
                box-shadow: none;
            }
            
            .status-message {
                margin-top: 20px;
                padding: 15px;
                border-radius: 8px;
                text-align: center;
                font-weight: 500;
            }
            
            .status-success {
                background: #d4edda;
                color: #155724;
                border: 1px solid #c3e6cb;
            }
            
            .status-error {
                background: #f8d7da;
                color: #721c24;
                border: 1px solid #f5c6cb;
            }
            
            .status-info {
                background: #cce7ff;
                color: #004085;
                border: 1px solid #99d6ff;
            }
        """),
        
        ui.div(
            # Logo and title section
            ui.div(
                ui.img(
                    src="/static/rwn_logo.png",
                    class_="main-logo",
                    alt="RealWorldNAV Logo"
                ),
                ui.h1("RealWorldNAV", class_="app-title"),
                ui.p("Financial Dashboard Launcher", class_="app-subtitle"),
                class_="logo-container"
            ),
            
            # Client selection card
            ui.div(
                ui.h2("Select Client", class_="selection-title"),
                ui.output_ui("client_options"),
                
                # Launch section
                ui.div(
                    ui.input_action_button(
                        "launch_app",
                        "Launch Application",
                        class_="launch-button",
                        disabled=True
                    ),
                    class_="launch-section"
                ),
                
                # Status messages
                ui.output_ui("status_message"),
                
                class_="client-selection-card"
            ),
            
            class_="launcher-container"
        ),
        
        title="RealWorldNAV Launcher"
    )

def launcher_server(input, output, session):
    """Server logic for the launcher"""
    
    # Reactive values
    selected_client = reactive.value(None)
    app_process = reactive.value(None)
    
    @output
    @render.ui
    def client_options():
        """Render client selection options using simple radio buttons"""
        # Create enhanced choices with logos and descriptions
        options = []
        
        for client_id, client_data in CLIENTS.items():
            logo_path = assets_dir / client_data["logo"]
            logo_src = f"/assets/{client_data['logo']}" if logo_path.exists() else "/static/rwn_logo.png"
            
            # Create a custom radio option with logo
            option_html = f"""
            <div class="client-option-wrapper" style="margin-bottom: 15px;">
                <label for="client_{client_id}" class="client-option" style="display: flex; align-items: center; cursor: pointer; padding: 20px; border: 2px solid #e9ecef; border-radius: 12px; background: #f8f9fa; transition: all 0.3s ease;">
                    <input type="radio" id="client_{client_id}" name="client_selection" value="{client_id}" style="margin-right: 15px; transform: scale(1.2);">
                    <img src="{logo_src}" class="client-logo" style="width: 48px; height: 48px; object-fit: contain; margin-right: 15px;">
                    <div class="client-info">
                        <h3 class="client-name" style="margin: 0 0 5px 0; font-size: 1.4rem; font-weight: 600; color: #333;">{client_data['name']}</h3>
                        <p class="client-description" style="margin: 0; color: #666; font-size: 0.95rem; line-height: 1.4;">{client_data['description']}</p>
                    </div>
                </label>
            </div>
            """
            options.append(ui.HTML(option_html))
        
        return ui.div(
            *options,
            ui.tags.script("""
                document.addEventListener('change', function(e) {
                    if (e.target.name === 'client_selection') {
                        Shiny.setInputValue('client_selection', e.target.value, {priority: 'event'});
                        
                        // Visual feedback
                        document.querySelectorAll('.client-option').forEach(el => {
                            el.style.borderColor = '#e9ecef';
                            el.style.background = '#f8f9fa';
                        });
                        e.target.closest('.client-option').style.borderColor = '#667eea';
                        e.target.closest('.client-option').style.background = '#f0f4ff';
                    }
                });
                
                // Hover effects
                document.querySelectorAll('.client-option').forEach(el => {
                    el.addEventListener('mouseenter', function() {
                        if (!this.querySelector('input').checked) {
                            this.style.borderColor = '#667eea';
                            this.style.background = '#f0f4ff';
                            this.style.transform = 'translateY(-2px)';
                            this.style.boxShadow = '0 8px 25px rgba(102, 126, 234, 0.15)';
                        }
                    });
                    el.addEventListener('mouseleave', function() {
                        if (!this.querySelector('input').checked) {
                            this.style.borderColor = '#e9ecef';
                            this.style.background = '#f8f9fa';
                            this.style.transform = 'translateY(0)';
                            this.style.boxShadow = 'none';
                        }
                    });
                });
            """)
        )
    
    # Handle client selection
    @reactive.effect
    @reactive.event(input.client_selection, ignore_none=True)
    def handle_client_selection():
        client_id = input.client_selection()
        if client_id:
            print(f"Client selected: {client_id}")
            selected_client.set(client_id)
            ui.update_action_button("launch_app", disabled=False)
            ui.notification_show(f"Selected {CLIENTS[client_id]['name']}", type="success", duration=2)
    
    @output
    @render.ui
    def status_message():
        """Show status messages"""
        return ui.div()  # Empty initially
    
    @reactive.effect
    @reactive.event(input.launch_app, ignore_none=True)
    def launch_main_app():
        """Launch the main application with selected client"""
        client_id = selected_client.get()
        
        if not client_id:
            ui.notification_show("Please select a client first", type="warning", duration=3)
            return
        
        try:
            print(f"Launching app for client: {client_id}")
            
            # Show launching message
            ui.notification_show(
                f"Launching RealWorldNAV for {CLIENTS[client_id]['name']}...",
                type="message",
                duration=3
            )
            
            # Get the path to app.py
            app_py_path = app_dir / "app.py"
            
            if not app_py_path.exists():
                ui.notification_show("app.py not found!", type="error", duration=5)
                return
            
            # Set environment variables for selected client
            env = os.environ.copy()
            env['REALWORLDNAV_CLIENT'] = client_id
            env['REALWORLDNAV_FUND'] = list(CLIENTS[client_id]['funds'].keys())[0]  # Default to first fund
            
            print(f"Environment: CLIENT={env.get('REALWORLDNAV_CLIENT')}, FUND={env.get('REALWORLDNAV_FUND')}")
            
            # Launch app.py in a new process
            def launch_app():
                try:
                    print("Starting app launch thread...")
                    
                    # Launch the main app
                    print(f"Executing: {sys.executable} {str(app_py_path)}")
                    process = subprocess.Popen([
                        sys.executable, str(app_py_path)
                    ], env=env, cwd=str(app_dir), 
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    
                    app_process.set(process)
                    print(f"Process started with PID: {process.pid}")
                    
                    # Wait a moment for the app to start
                    time.sleep(4)
                    
                    # Check if process is still running
                    if process.poll() is None:
                        print("App process is running, opening browser...")
                        # Open the main app in browser
                        webbrowser.open('http://localhost:8000')
                        
                        # Show success message
                        print("Showing success notification...")
                    else:
                        # Process died, get error output
                        stdout, stderr = process.communicate()
                        print(f"Process failed. stdout: {stdout.decode()}, stderr: {stderr.decode()}")
                        ui.notification_show(
                            f"Application failed to start. Check console for errors.",
                            type="error",
                            duration=10
                        )
                    
                except Exception as e:
                    print(f"Error in launch thread: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Run in separate thread to avoid blocking
            launch_thread = threading.Thread(target=launch_app, daemon=True)
            launch_thread.start()
            print("Launch thread started")
            
        except Exception as e:
            print(f"Launch error: {e}")
            import traceback
            traceback.print_exc()
            ui.notification_show(
                f"Failed to launch application: {str(e)}",
                type="error",
                duration=10
            )

# Create the launcher app
# Use a combined static assets mapping for both static and assets directories
static_assets_mapping = {
    "/static": str(static_dir),
    "/assets": str(assets_dir)
}

launcher_app = App(
    ui=create_launcher_ui(),
    server=launcher_server,
    static_assets=static_assets_mapping
)

def main():
    """Main entry point for the launcher"""
    print("Starting RealWorldNAV Launcher...")
    
    # Check if required files exist
    logo_path = static_dir / "rwn_logo.png"
    if not logo_path.exists():
        print(f"Warning: Logo not found at {logo_path}")
    
    app_py_path = app_dir / "app.py"
    if not app_py_path.exists():
        print(f"Error: app.py not found at {app_py_path}")
        sys.exit(1)
    
    try:
        # Start the launcher on a different port than the main app
        launcher_port = 3000
        print(f"Launcher will be available at http://localhost:{launcher_port}")
        
        # Open browser after a short delay
        def open_browser():
            time.sleep(1.5)
            webbrowser.open(f'http://localhost:{launcher_port}')
        
        threading.Thread(target=open_browser, daemon=True).start()
        
        # Run the launcher app
        uvicorn.run(
            launcher_app,
            host="127.0.0.1",
            port=launcher_port,
            log_level="info"
        )
        
    except KeyboardInterrupt:
        print("\nLauncher shutting down...")
    except Exception as e:
        print(f"Error running launcher: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()