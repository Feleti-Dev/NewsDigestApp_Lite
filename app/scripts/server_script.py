#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import logging
import os
import sys
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
# Настраиваем простейший логгер, который пишет ТОЛЬКО в консоль.
# Tray.py перехватит это и добавит форматирование и запись в файл.
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
    stream=sys.stdout,
    force=True
)
logger = logging.getLogger("ServerSubprocess")


def main(port: int, shutdown_file: str, app_path: str):
    sys.path.insert(0, app_path)

    # Здесь НЕ используем setup_logging из конфига, чтобы не создавать конфликтов файлов.
    # Просто пишем в logger, а tray.py это обработает.

    template_folder = os.path.join(app_path, "app", "web", "templates")
    static_folder = os.path.join(app_path, "app", "web", "static")

    from hypercorn.config import Config
    from hypercorn.asyncio import serve as hypercorn_serve
    from quart import Quart
    from quart_cors import cors

    logger.info(f"Инициализация сервера на порту {port}")

    app = Quart(__name__)
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key')
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    cors(app)
    app.template_folder = template_folder
    app.static_folder = static_folder

    from app.web.routes import bp
    app.register_blueprint(bp)

    # === Инициализация менеджеров ===
    parser_manager = None
    db_manager = None
    scheduler_manager = None

    async def initialize_managers():
        nonlocal parser_manager, db_manager, scheduler_manager

        # Перенаправляем stdout библиотек, чтобы tray.py тоже видел их логи
        # (Например, hypercorn logs)

        try:
            from app.database.db_utils import DatabaseManager
            db_manager = DatabaseManager()

            from app.parsers.parser_manager import ParserManager
            parser_manager = ParserManager(db_manager)

            from app.scheduler.scheduler_manager import SchedulerManager
            scheduler_manager = SchedulerManager()

            from app.configs import config
            bypassing_method = getattr(config.app, 'bypassing_method', 'NONE')

            scheduler_manager.initialize(parser_manager=parser_manager, bypassing_method=bypassing_method)
            await scheduler_manager.start_all()

            from app.web.routes import set_managers
            set_managers(parser_manager, db_manager, scheduler_manager.digest_scheduler, scheduler_manager)

            logger.info("Менеджеры инициализированы успешно")
        except Exception as e:
            logger.error(f"Ошибка инициализации менеджеров: {e}")
            raise

    @app.before_serving
    async def before_serving():
        await initialize_managers()
        if parser_manager:
            parser_manager.create_parsers()

    @app.after_serving
    async def after_serving():
        if scheduler_manager: await scheduler_manager.stop_all()
        if parser_manager: await parser_manager.close_parsers()

    async def shutdown_trigger():
        while not os.path.exists(shutdown_file):
            await asyncio.sleep(0.5)
        try:
            os.remove(shutdown_file)
        except:
            pass
        return True

    async def run_server():
        config = Config()
        config.bind = [f"127.0.0.1:{port}"]
        config.use_reloader = False
        config.loglevel = "INFO"  # Hypercorn будет писать в stdout, tray перехватит

        logger.info("Запуск Hypercorn...")
        try:
            await hypercorn_serve(app, config, shutdown_trigger=shutdown_trigger)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Critical Server Error: {e}")
            sys.exit(1)

    asyncio.run(run_server())


if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.exit(1)
    try:
        main(int(sys.argv[1]), sys.argv[2], sys.argv[3])
    except Exception as e:
        # Этот print тоже уйдет в tray.py как лог
        print(f"FATAL ERROR in server_script: {e}")
        sys.exit(1)