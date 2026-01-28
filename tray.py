#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Системный трей для News Digest Portable Application.
Оркестратор собирает логи от дочерних процессов и пишет их через центральный конфиг.
"""
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

# === ИМПОРТ КОНФИГУРАЦИИ ЛОГИРОВАНИЯ ===
try:
    from app.configs.logging_config import setup_logging
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    from app.configs.logging_config import setup_logging

# Настройка кодировки консоли для Windows
if sys.platform == 'win32':
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleOutputCP(65001)
    except:
        pass

# Инициализируем логгер (он настроит файл, ротацию и формат как в main.py)
logger = setup_logging()


class NewsDigestApp:
    def __init__(self, app_path: str = None, port: int = 5000, silent_mode: bool = False):
        self.port = port
        self.server_url = f"http://127.0.0.1:{self.port}"
        self.silent_mode = silent_mode  # Флаг тихого запуска
        self.app_path = Path(app_path) if app_path else Path(__file__).parent
        self.server_process = None
        self.tray_process = None

        self.server_shutdown_file = str(self.app_path / '.server_shutdown')
        self.tray_shutdown_pipe = str(self.app_path / '.tray_shutdown_pipe')

        mode_str = " (ТИХИЙ РЕЖИМ)" if self.silent_mode else ""
        logger.info(f"NewsDigestApp инициализирован (порт: {self.port}){mode_str}")

    def cleanup_files(self):
        for f in [self.server_shutdown_file, self.tray_shutdown_pipe]:
            try:
                if os.path.exists(f): os.remove(f)
            except:
                pass

    def _log_subprocess_output(self, source_name: str, line: str):
        """
        Анализирует строку от подпроцесса и пишет в общий лог с правильным уровнем.
        """
        line_clean = line.strip()
        if not line_clean:
            return

        line_upper = line_clean.upper()

        # Определяем уровень важности по ключевым словам
        if any(x in line_upper for x in ['ERROR', 'EXCEPTION', 'CRITICAL', 'TRACEBACK']):
            log_func = logger.error
        elif any(x in line_upper for x in ['WARN', 'WARNING']):
            log_func = logger.warning
        elif any(x in line_upper for x in ['DEBUG']):
            log_func = logger.debug
        else:
            log_func = logger.info
        # Пишем в лог: [Server] Текст сообщения
        log_func(f"[{source_name}] {line_clean}")

    def start_subprocess(self, script_name: str, args: list, source_label: str) -> subprocess.Popen:
        """Универсальная функция запуска процесса"""
        logger.info(f"🚀 Запуск {source_label}...")

        script_path = str(self.app_path / 'app' / 'scripts' / script_name)
        if not os.path.exists(script_path):
            logger.error(f"Скрипт не найден: {script_path}")
            return None

        # Формируем переменные окружения
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"

        # Формируем команду. Добавляем '-u' для отключения буферизации (важно для логов!)
        cmd = [sys.executable, '-u', script_path] + args

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Объединяем stderr и stdout
            cwd=str(self.app_path),
            env=env  # Принудительно отключаем буфер
        )

        def read_loop():
            try:
                # Читаем построчно
                for line in iter(process.stdout.readline, b''):
                    try:
                        decoded = line.decode('utf-8', errors='replace')
                        self._log_subprocess_output(source_label, decoded)
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"Ошибка чтения логов {source_label}: {e}")
            finally:
                if process and process.stdout:
                    process.stdout.close()

        thread = threading.Thread(target=read_loop, daemon=True)
        thread.start()

        return process

    def start_server(self):
        args = [str(self.port), self.server_shutdown_file, str(self.app_path)]
        self.server_process = self.start_subprocess('server_script.py', args, "Server")
        return self.server_process

    def start_tray(self):
        args = [str(self.port), self.tray_shutdown_pipe, str(self.app_path), self.server_url]
        self.tray_process = self.start_subprocess('tray_script.py', args, "TrayIcon")
        return self.tray_process

    def stop_process(self, process, shutdown_file, name):
        if process and process.poll() is None:
            logger.info(f"🛑 Остановка {name}...")
            try:
                # Создаем файл-сигнал
                with open(shutdown_file, 'w') as f:
                    f.write('stop')
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                logger.warning(f"{name} убит принудительно")
            except Exception as e:
                logger.error(f"Ошибка остановки {name}: {e}")

    def check_server_running(self) -> bool:
        try:
            import urllib.request
            req = urllib.request.Request(f"{self.server_url}/health", method='GET')
            with urllib.request.urlopen(req, timeout=2):
                return True
        except:
            return False

    def wait_for_server(self, timeout: int = 30) -> bool:
        for _ in range(timeout):
            if self.check_server_running():
                return True
            if self.server_process and self.server_process.poll() is not None:
                return False
            time.sleep(1)
        return False

    def run(self):
        self.cleanup_files()

        if not self.start_server():
            return

        if not self.wait_for_server():
            logger.error("Сервер не запустился (timeout)")
            self.stop_process(self.server_process, self.server_shutdown_file, "Server")
            return
        # === ЛОГИКА АВТОЗАПУСКА / ТИХОГО РЕЖИМА ===
        if not self.silent_mode:
            logger.info(f"🌐 Открываем браузер: {self.server_url}")
            import webbrowser
            webbrowser.open(self.server_url)
            logger.info(f"✅ Браузер открыт: {self.server_url}")
        else:
            logger.info("🤫 Тихий режим: Браузер не открывается автоматически")

        if not self.start_tray():
            self.stop_process(self.server_process, self.server_shutdown_file, "Server")
            return

        logger.info("✅ Приложение работает. Логи пишутся в центральный файл.")

        try:
            while True:
                time.sleep(1)
                # Если упал трей - закрываем всё
                if self.tray_process and self.tray_process.poll() is not None:
                    logger.info("Трей закрыт пользователем")
                    break
                # Если упал сервер - это ошибка
                if self.server_process and self.server_process.poll() is not None:
                    logger.error("Сервер неожиданно упал!")
                    break
        except KeyboardInterrupt:
            logger.info("Нажат Ctrl+C")

        self.stop_process(self.tray_process, self.tray_shutdown_pipe, "TrayIcon")
        self.stop_process(self.server_process, self.server_shutdown_file, "Server")
        self.cleanup_files()
        logger.info("👋 Завершение работы")


def main():
    if getattr(sys, 'frozen', False):
        app_path = Path(sys._MEIPASS)
    else:
        app_path = Path(__file__).parent

    port = 5000
    silent = False
    # Парсинг аргументов
    for arg in sys.argv[1:]:
        if arg.startswith('--port='):
            try:
                port = int(arg.split('=')[1])
            except ValueError:
                pass
        # Проверяем флаг silent
        if arg == '--silent':
            silent = True
    app = NewsDigestApp(app_path=str(app_path), port=port, silent_mode=silent)

    def signal_handler(sig, frame):
        logger.info("Signal received, shutting down...")
        app.stop_process(app.tray_process, app.tray_shutdown_pipe, "TrayIcon")
        app.stop_process(app.server_process, app.server_shutdown_file, "Server")
        app.cleanup_files()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    app.run()


if __name__ == '__main__':
    main()