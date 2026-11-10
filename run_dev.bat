@echo off
SET "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"
chcp 65001 >nul

where uv >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo uv не найден. Запустите install_dev.bat
    pause
    exit /b 1
)

echo Start News Digest App (uv run)...
uv run tray.py

if %ERRORLEVEL% NEQ 0 (
    echo Can't start app. Error in app. You can close the window
    pause
)
