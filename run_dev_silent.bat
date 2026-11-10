@echo off
SET "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"

where uv >nul 2>&1
if %ERRORLEVEL% NEQ 0 exit /b 1

start "" /B uv run tray.py
timeout /t 2 /nobreak >nul
tasklist | find /i "python" >nul
if %ERRORLEVEL% NEQ 0 (
    echo Can't start app. You may need to run run_dev.bat manually.
    pause
    exit /b 1
)
exit /b 0
