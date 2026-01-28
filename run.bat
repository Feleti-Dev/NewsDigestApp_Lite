@echo off
SET "PROJECT_DIR=%~dp0"
SET "PYTHON_BIN=%PROJECT_DIR%python_dist\python.exe"

:: ??????????? ????????? ??????? ?? UTF-8
chcp 65001 >nul

echo Start News Digest App Embedded Python...
"%PYTHON_BIN%" "%PROJECT_DIR%tray.py"

if %ERRORLEVEL% NEQ 0 (
    echo Can't start app. Error in app. You can close the window
    pause
)
