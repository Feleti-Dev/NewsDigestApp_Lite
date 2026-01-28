#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import platform
import sys
import os
import winreg  # Для работы с реестром Windows

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Настраиваем простой логгер в stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
    stream=sys.stdout,
    force=True
)
logger = logging.getLogger("TraySubprocess")


def is_macos() -> bool:
    return platform.system() == 'Darwin'


def is_windows() -> bool:
    return platform.system() == 'Windows'


class AutostartManager:
    """Управление автозапуском в реестре Windows"""
    APP_NAME = "NewsDigestApp"

    def __init__(self, app_path):
        self.app_path = app_path
        # Путь к tray.py
        self.script_path = os.path.join(self.app_path, "tray.py")

        # Пытаемся найти pythonw.exe (безконсольный), если он лежит рядом с текущим python.exe
        current_dir = os.path.dirname(sys.executable)
        pythonw_path = os.path.join(current_dir, "pythonw.exe")

        if os.path.exists(pythonw_path):
            self.executable = pythonw_path
        else:
            self.executable = sys.executable

    def is_enabled(self) -> bool:
        """Проверяет, включен ли автозапуск"""
        try:
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                                 r"Software\Microsoft\Windows\CurrentVersion\Run",
                                 0, winreg.KEY_READ)
            winreg.QueryValueEx(key, self.APP_NAME)
            winreg.CloseKey(key)
            return True
        except FileNotFoundError:
            return False
        except Exception as e:
            logger.error(f"Ошибка проверки автозапуска: {e}")
            return False

    def set_autostart(self, enable: bool):
        """Включает или выключает автозапуск"""
        key_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
        try:
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_SET_VALUE)

            if enable:
                # Команда: "путь_к_python" "путь_к_tray.py" --silent
                # Кавычки важны, если в путях есть пробелы
                cmd = f'"{self.executable}" "{self.script_path}" --silent'
                winreg.SetValueEx(key, self.APP_NAME, 0, winreg.REG_SZ, cmd)
                logger.info(f"Автозапуск включен. Команда: {cmd}")
            else:
                try:
                    winreg.DeleteValue(key, self.APP_NAME)
                    logger.info("Автозапуск выключен")
                except FileNotFoundError:
                    pass  # Уже удалено

            winreg.CloseKey(key)
        except Exception as e:
            logger.error(f"Ошибка изменения автозапуска: {e}")


def main(port: int, shutdown_pipe: str, app_path: str, server_url: str = None):
    sys.path.insert(0, app_path)

    if server_url is None:
        server_url = f"http://127.0.0.1:{port}"

    # Инициализация менеджера автозапуска
    autostart = AutostartManager(app_path)

    try:
        from pystray import MenuItem as Item, Menu as Menu, Icon
        from PIL import Image, ImageDraw
    except ImportError:
        logger.error("Библиотека pystray или PIL не найдена!")
        sys.exit(1)

    # Генерация иконки
    def create_image():
        image = Image.new('RGBA', (64, 64), (0, 0, 0, 0))
        draw = ImageDraw.Draw(image)
        draw.rectangle([4, 4, 60, 60], fill=(33, 150, 243), outline=(25, 118, 210), width=2)
        return image

    def quit_action(icon, item):
        logger.info("Пользователь нажал 'Выход'")
        try:
            with open(shutdown_pipe, 'w') as f:
                f.write('quit')
        except:
            pass
        icon.stop()

    def open_browser(icon, item):
        import webbrowser
        logger.info(f"Открываем браузер: {server_url}")
        webbrowser.open(server_url)

    def toggle_autostart_action(icon, item):
        """Переключатель автозапуска"""
        new_state = not item.checked
        autostart.set_autostart(new_state)

    # Создание меню
    # checked=lambda item: autostart.is_enabled() - динамически проверяет состояние галочки
    menu = Menu(
        Item('Открыть News Digest', open_browser, default=True),
        Menu.SEPARATOR,
        Item('Запускать при старте', toggle_autostart_action, checked=lambda item: autostart.is_enabled()),
        Menu.SEPARATOR,
        Item('Выход', quit_action)
    )

    icon = Icon("news_digest", Image.open(os.path.join(app_path, "app", "web", "static", "favicons", "favicon_alt.ico")), "News Digest", menu=menu)

    logger.info("Иконка трея запущена и готова")
    icon.run()


if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.exit(1)
    try:
        server_url = sys.argv[4] if len(sys.argv) > 4 else None
        main(int(sys.argv[1]), sys.argv[2], sys.argv[3], server_url)
    except Exception as e:
        print(f"FATAL ERROR in tray_script: {e}")
        sys.exit(1)