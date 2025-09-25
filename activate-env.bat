@echo off
REM Activation script for Windows - RealWorldNAV

echo RealWorldNAV - Activating Windows Environment
echo ==============================================

REM Check if Windows virtual environment exists
if exist ".venv-windows\" (
    echo ✅ Found Windows virtual environment
    call .venv-windows\Scripts\activate.bat
    echo ✅ Virtual environment activated
    echo.
    echo Environment: %VIRTUAL_ENV%
    python --version
    echo.
    echo To run the application:
    echo   python app.py
    echo   # or
    echo   shiny run app.py
) else (
    echo ❌ Windows virtual environment not found (.venv-windows)
    echo Run: python setup-env.py
    pause
    exit /b 1
)