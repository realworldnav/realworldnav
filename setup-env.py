#!/usr/bin/env python3
"""
Cross-platform virtual environment setup script for RealWorldNAV
Detects the platform and creates/activates the appropriate virtual environment
"""

import os
import sys
import platform
import subprocess
from pathlib import Path

def detect_platform():
    """Detect the current platform"""
    system = platform.system().lower()
    if system == "windows":
        return "windows"
    elif system in ["darwin", "linux"]:
        return "unix"
    else:
        return "unknown"

def check_brew_dependencies():
    """Check and install required Homebrew dependencies for WeasyPrint on macOS"""
    if platform.system().lower() != "darwin":
        return

    print("\nChecking WeasyPrint system dependencies...")

    # Check if Homebrew is installed
    try:
        subprocess.run(["brew", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("⚠️  Homebrew not found. Please install Homebrew first: https://brew.sh")
        return

    # Required packages for WeasyPrint
    required_packages = ["pango", "cairo", "gobject-introspection", "gdk-pixbuf"]
    missing_packages = []

    for package in required_packages:
        result = subprocess.run(["brew", "list", package], capture_output=True)
        if result.returncode != 0:
            missing_packages.append(package)

    if missing_packages:
        print(f"Installing missing WeasyPrint dependencies: {', '.join(missing_packages)}")
        try:
            subprocess.run(["brew", "install"] + missing_packages, check=True)
            print("✅ WeasyPrint dependencies installed successfully")
        except subprocess.CalledProcessError as e:
            print(f"⚠️  Failed to install some dependencies: {e}")
            print("   You may need to run manually: brew install " + " ".join(missing_packages))
    else:
        print("✅ All WeasyPrint dependencies are already installed")

def get_venv_paths(platform_type):
    """Get virtual environment paths based on platform"""
    if platform_type == "windows":
        venv_dir = ".venv-windows"
        activate_script = os.path.join(venv_dir, "Scripts", "activate.bat")
        python_exe = os.path.join(venv_dir, "Scripts", "python.exe")
        pip_exe = os.path.join(venv_dir, "Scripts", "pip.exe")
    else:  # unix (Mac/Linux)
        # Check if .venv exists first (standard name)
        if os.path.exists(".venv") and os.path.exists(os.path.join(".venv", "bin", "python")):
            venv_dir = ".venv"
        else:
            venv_dir = ".venv-mac"
        activate_script = os.path.join(venv_dir, "bin", "activate")
        python_exe = os.path.join(venv_dir, "bin", "python")
        pip_exe = os.path.join(venv_dir, "bin", "pip")

    return venv_dir, activate_script, python_exe, pip_exe

def create_venv(venv_dir, python_exe):
    """Create virtual environment if it doesn't exist"""
    if not os.path.exists(venv_dir):
        print(f"Creating virtual environment: {venv_dir}")
        if sys.platform == "win32":
            subprocess.run([sys.executable, "-m", "venv", venv_dir], check=True)
        else:
            subprocess.run([sys.executable, "-m", "venv", venv_dir], check=True)
        print(f"✅ Virtual environment created: {venv_dir}")
    else:
        print(f"✅ Virtual environment already exists: {venv_dir}")

def install_requirements(pip_exe):
    """Install requirements if requirements.txt exists"""
    if os.path.exists("requirements.txt"):
        print("Installing requirements...")
        subprocess.run([pip_exe, "install", "-r", "requirements.txt"], check=True)
        print("✅ Requirements installed successfully")
    else:
        print("⚠️  No requirements.txt found")

def setup_nvm_integration(venv_dir, platform_type):
    """Add nvm sourcing to the venv activate script for Claude CLI access"""
    if platform_type != "unix":
        return

    nvm_path = os.path.expanduser("~/.nvm/nvm.sh")
    if not os.path.exists(nvm_path):
        return

    activate_script = os.path.join(venv_dir, "bin", "activate")
    if not os.path.exists(activate_script):
        return

    # Check if nvm sourcing is already added
    with open(activate_script, 'r') as f:
        content = f.read()
        if "/.nvm/nvm.sh" in content:
            print("✅ NVM integration already configured")
            return

    # Add nvm sourcing at the end of the activate script
    nvm_source_code = """
# Added by setup-env.py for Claude CLI access
if [ -f "$HOME/.nvm/nvm.sh" ]; then
    source "$HOME/.nvm/nvm.sh"
fi
"""

    with open(activate_script, 'a') as f:
        f.write(nvm_source_code)

    print("✅ NVM integration added for Claude CLI access")

def print_activation_instructions(platform_type, activate_script):
    """Print platform-specific activation instructions"""
    print("\n" + "="*50)
    print("VIRTUAL ENVIRONMENT SETUP COMPLETE")
    print("="*50)

    if platform_type == "windows":
        print("\nTo activate the virtual environment on Windows:")
        print(f"  {activate_script}")
        print("\nOr use the provided batch file:")
        print("  activate-env.bat")
    else:
        print("\nTo activate the virtual environment on Mac/Linux:")
        print(f"  source {activate_script}")

        # Check if nvm is installed
        nvm_path = os.path.expanduser("~/.nvm/nvm.sh")
        if os.path.exists(nvm_path):
            print("\nIf using Claude CLI with nvm, also run:")
            print(f"  source ~/.nvm/nvm.sh")

        print("\nOr use the provided shell script:")
        print("  source activate-env.sh")

    print("\nTo run the application:")
    print("  python app.py")
    print("  # or")
    print("  shiny run app.py")

def main():
    """Main setup function"""
    print("RealWorldNAV - Cross-Platform Environment Setup")
    print("=" * 50)
    
    # Detect platform
    platform_type = detect_platform()
    print(f"Detected platform: {platform_type}")
    
    if platform_type == "unknown":
        print("❌ Unsupported platform detected")
        sys.exit(1)
    
    # Get platform-specific paths
    venv_dir, activate_script, python_exe, pip_exe = get_venv_paths(platform_type)
    
    try:
        # Check and install system dependencies for macOS
        check_brew_dependencies()

        # Create virtual environment
        create_venv(venv_dir, python_exe)

        # Install requirements
        install_requirements(pip_exe)

        # Setup NVM integration for Claude CLI access
        setup_nvm_integration(venv_dir, platform_type)

        # Print activation instructions
        print_activation_instructions(platform_type, activate_script)
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error during setup: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()