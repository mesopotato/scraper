@echo off
REM Check if virtual environment directory exists
IF NOT EXIST "env" (
    echo Creating virtual environment...
    pip install virtualenv
    virtualenv env
)

echo Activating virtual environment...
CALL .\env\Scripts\activate

echo Installing dependencies from requirements.txt...
pip install -r requirements.txt

echo Done!
