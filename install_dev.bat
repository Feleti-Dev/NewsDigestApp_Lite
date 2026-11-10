@echo off
SET "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"
chcp 65001 >nul

echo === NewsDigestApp Lite: установка окружения разработки (uv) ===
where uv >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo uv не найден в PATH.
    echo Установка: powershell -ExecutionPolicy Bypass -c "irm https://astral.sh/uv/install.ps1 | iex"
    pause
    exit /b 1
)

uv python install 3.10
uv sync --group dev
if %ERRORLEVEL% NEQ 0 (
    echo Ошибка uv sync
    pause
    exit /b 1
)

echo.
echo Готово. Запуск: run_dev.bat или run_dev_silent.bat
pause
