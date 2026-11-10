@echo off
cd /d "%~dp0"
chcp 65001 >nul

where uv >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo uv не найден в PATH
    pause
    exit /b 1
)

echo Export requirements.txt для portable-сборки (pip)...
uv export --format requirements-txt --no-dev --no-hashes --no-emit-project -o requirements.txt
if %ERRORLEVEL% NEQ 0 pause & exit /b 1
echo OK: requirements.txt обновлён
pause
