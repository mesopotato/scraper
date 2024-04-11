#!/bin/bash
# !!!!!!!!!!!!!
#before you start make sure you have python3.8 installed
# sudo apt-get install python3.8
# start wsl in powerwhell 
# type wsl and press enter
# make the setup.sh file executable
# chmod +x setup.sh
# run the setup.sh file with the following command
# source setup.sh
#
# if you get an error unexpected end of file install dos2unix
# sudo apt-get update && sudo apt-get install dos2unix
# then run dos2unix setup.sh
# and run the setup.sh file again
#!!!!!!!!!!!!!!
# Check if virtual environment directory exists
if [ ! -d "env" ]; then
    echo "Creating virtual environment..."
    sudo apt install python3.10-venv
    python3 -m venv env
fi

echo "Activating virtual environment..."
source env/bin/activate

echo "Installing dependencies from requirements.txt..."
pip install -r requirements.txt

echo "Done!"