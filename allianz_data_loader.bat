@echo off
REM Set the path to your Python executable
set PYTHON_PATH="your python path"

REM Set the path to your Python script
set SCRIPT_PATH="your python script path"

REM Optional: Set the chunksize as an argument if needed
set CHUNKSIZE=1000

REM Run the Python script
%PYTHON_PATH% %SCRIPT_PATH%

pause
