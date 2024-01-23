-----------------------------------------
STEP 1 - Download and Install Python
-----------------------------------------
-Go to https://www.python.org/downloads/release/python-3114/ and scroll down to the bottom of the page.
-Click on Windows installer (64-bit).
-Run the downloaded file and follow the installation wizard.

-----------------------------------------
STEP 2 - Install Python Libraries
-----------------------------------------
-Open the start menu and type cmd, open Command Prompt
-Type "cd ", then the path of the Scripts folder of your Python install (most likely C:\Program Files\Python311\Scripts), then enter

cd C:\Program Files\Python311\Scripts

-type and enter "pip install sqlalchemy"
-type and enter "pip install pandas"
-type and enter "pip install openpyxl"
-type and enter "pip install pyodbc"
-type and enter "pip install xlrd"
-you can close Command Prompt

-----------------------------------------
STEP 3 - Download
-----------------------------------------
-Download scripts
-I recommend immediately going through each of the files and updating the server and database info as needed, so you would only have to do it once

-----------------------------------------
STEP 4 - How to use
-----------------------------------------
-To import:
	-place a file in the same directory as the relevant python script
	-run the script
	-wait until the script is finished, it will track the progress in the output window

-To export:
	-open the relevant python script
	-enter a list of tablenames or a regular expression to search for tables to export
	-modify other parameters as needed, but for most cases leave them as-is
	-save and close the python script
	-run the python script
	-wait until the script is finished, it will track the progress in the output window
	-the exported files will appear in the same directory as the script
