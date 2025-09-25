#!/bin/bash

# Activate the Python virtual environment
if [ -d ".venv" ]; then
    source .venv/bin/activate
elif [ -d ".venv-mac" ]; then
    source .venv-mac/bin/activate
else
    echo "No virtual environment found. Please run: python setup-env.py"
    exit 1
fi

# Re-source nvm if available (for Claude CLI access)
if [ -f "$HOME/.nvm/nvm.sh" ]; then
    source "$HOME/.nvm/nvm.sh"
    echo "NVM reloaded - Claude CLI should now be available"
fi

echo "Virtual environment activated successfully!"
echo "You can now run: python app.py or shiny run app.py"
