# Project Setup Instructions

This project includes a setup script for Unix-based systems (such as Linux and macOS) and a setup script for Windows. These scripts are designed to create a virtual Python environment and install all required dependencies from the `requirements.txt` file.

## Using `setup.sh` on Unix-based Systems

The `setup.sh` script is a shell script that automates the setup of a Python virtual environment on Unix-based systems.

### Prerequisites

- Python 3
- pip

### Instructions

1. Open your terminal.
2. Navigate to the project's directory where the `setup.sh` file is located.
3. Run the following command to make the script executable:
` chmod +x setup.sh `

4. Execute the script:
activate wsl ( type `wls` in powershell )
run 
` source setup.sh  `

5. The script will install `virtualenv` if it's not already installed, create a virtual environment named `env`, activate it, and then install the dependencies listed in `requirements.txt`.

## Using `setup.bat` on Windows

The `setup.bat` script is a batch file that automates the setup of a Python virtual environment on Windows systems.

### Prerequisites

- Python 3
- pip


### Instructions

1. Open Command Prompt or PowerShell.
2. Navigate to the project's directory where the `setup.bat` file is located.
3. Execute the script:

`setup.bat`

4. The script will install `virtualenv` if it's not already installed, create a virtual environment named `env`, activate it, and then install the dependencies listed in `requirements.txt`.

After running the appropriate script for your operating system, the virtual environment will be set up, and you can begin working on the project.

---

Please ensure that you have administrative privileges when running these scripts, as they may require permission to install packages globally if `virtualenv` is not already installed.