#!/bin/bash
# Check if virtual environment directory exists
if [ ! -d "env" ]; then
    echo "Creating virtual environment..."
    sudo apt install python3.8-venv
    python3 -m venv env
fi

echo "Activating virtual environment..."
source env/bin/activate

echo "Installing dependencies from requirements.txt..."
pip install -r requirements.txt

echo "Done!"
