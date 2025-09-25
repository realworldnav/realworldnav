#!/bin/bash
# Activation script for Mac/Linux - RealWorldNAV

echo "RealWorldNAV - Activating Mac/Linux Environment"
echo "=============================================="

# Check if Mac virtual environment exists
if [ -d ".venv-mac" ]; then
    echo "✅ Found Mac virtual environment"
    source .venv-mac/bin/activate
    echo "✅ Virtual environment activated"
    echo ""
    echo "Environment: $(which python)"
    echo "Python version: $(python --version)"
    echo ""
    echo "To run the application:"
    echo "  python app.py"
    echo "  # or"
    echo "  shiny run app.py"
else
    echo "❌ Mac virtual environment not found (.venv-mac)"
    echo "Run: python3 setup-env.py"
    exit 1
fi