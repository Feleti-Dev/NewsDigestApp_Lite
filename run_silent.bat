@echo off
SET "PROJECT_DIR=%~dp0"
SET "PYTHON_BIN=%PROJECT_DIR%python_dist\pythonw.exe"

:: Запускаем приложение и проверяем ошибку
start "" /B "%PYTHON_BIN%" "%PROJECT_DIR%tray.py"

:: Небольшая пауза для проверки запуска
timeout /t 2 /nobreak >nul

:: Проверяем, запустилось ли приложение (упрощенная проверка)
tasklist | find /i "python" >nul
if %ERRORLEVEL% NEQ 0 (
    echo Can't start app. You may need to run manually.
    pause
    exit
)
exit