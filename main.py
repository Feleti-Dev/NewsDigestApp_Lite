"""
Основной файл приложения для Quart.
Заменяет Flask app factory для работы с асинхронным фреймворком.

Запуск:
    python main.py

Или с Hypercorn:
    hypercorn main:app --bind 0.0.0.0:5000 --reload
"""

import os
import sys

# Получаем абсолютный путь к папке, где лежит main.py
project_root = os.path.dirname(os.path.abspath(__file__))

# Добавляем этот путь в список поиска модулей, если его там нет
if project_root not in sys.path:
    sys.path.insert(0, project_root)


import asyncio
from pathlib import Path

from quart import Quart
from quart_cors import cors

# import logging
from app.configs.logging_config import setup_logging
from app.parsers import ParserManager
from app.scheduler.scheduler_manager import SchedulerManager

# Настройка логирования
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.StreamHandler(sys.stdout)  # stdout вместо stderr
#     ]
# )
# logger = logging.getLogger(__name__)
logger = setup_logging()
# Отключаем избыточное логирование от библиотек
# logging.getLogger("hypercorn").setLevel(logging.WARNING)
# logging.getLogger("hypercorn.access").setLevel(logging.WARNING)
# logging.getLogger("hypercorn.error").setLevel(logging.WARNING)
# logging.getLogger("telethon").setLevel(logging.WARNING)
# logging.getLogger("googleapiclient").setLevel(logging.WARNING)
# ===========================================


def create_app() -> Quart:
    """
    Фабрика приложений Quart.

    Returns:
        Quart: Настроенное приложение Quart
    """
    app = Quart(__name__)

    # Конфигурация
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-key")
    app.config["TEMPLATES_AUTO_RELOAD"] = True

    # Включаем CORS для API
    cors(app)

    # Регистрация blueprints
    from app.web.routes import bp

    app.register_blueprint(bp)

    # Логируем успешную инициализацию
    logger.info("✅ Quart приложение создано")

    return app


# Глобальные менеджеры (будут инициализированы при запуске)
parser_manager: ParserManager | None = None
db_manager = None
digest_scheduler = None
scheduler_manager: SchedulerManager | None = None
restart_manager = None
app = create_app()


async def initialize_managers(app: Quart):
    """
    Инициализация глобальных менеджеров приложения.

    Args:
        app: Экземпляр Quart приложения
    """
    global parser_manager, db_manager, digest_scheduler, scheduler_manager, restart_manager

    logger.info("🚀 Инициализация менеджеров...")

    # Инициализация DatabaseManager
    from app.database.db_utils import DatabaseManager

    db_manager = DatabaseManager()
    logger.info("✅ DatabaseManager инициализирован")

    # Инициализация ParserManager
    from app.parsers.parser_manager import ParserManager

    parser_manager = ParserManager(db_manager)
    logger.info("✅ ParserManager инициализирован")

    # Инициализация SchedulerManager
    from app.scheduler.scheduler_manager import SchedulerManager

    scheduler_manager = SchedulerManager()

    # Определяем режим работы из конфигурации
    from app.configs import config

    bypassing_method = getattr(config.app, "bypassing_method", "NONE")

    scheduler_manager.initialize(parser_manager=parser_manager, bypassing_method=bypassing_method)
    await scheduler_manager.start_all()
    logger.info(f"✅ SchedulerManager инициализирован (режим: {bypassing_method})")

    # Установка менеджеров в routes
    from app.web.routes import set_managers

    set_managers(
        out_parser_manager=parser_manager,
        out_db_manager=db_manager,
        out_digest_scheduler=scheduler_manager.digest_scheduler,
        out_scheduler_manager=scheduler_manager,
    )

    logger.info("✅ Все менеджеры инициализированы")


# Startup и Shutdown обработчики
@app.before_serving
async def before_serving():
    """Выполняется перед запуском сервера"""
    logger.info("🔄 Запуск инициализации...")
    await initialize_managers(app)

    # Создаём парсеры при старте
    if parser_manager:
        parser_manager.create_parsers()
        logger.info("✅ Парсеры созданы")

    logger.info("✅ Приложение готово к работе")


@app.after_serving
async def after_serving():
    """Выполняется при остановке сервера"""
    logger.info("🔄 Остановка приложения...")

    # Останавливаем планировщики
    if scheduler_manager:
        await scheduler_manager.stop_all()
        logger.info("✅ Планировщики остановлены")

    # Закрываем парсеры
    if parser_manager:
        await parser_manager.close_parsers()
        logger.info("✅ Парсеры закрыты")

    logger.info("✅ Приложение остановлено")


if __name__ == "__main__":
    """
    Запуск приложения через Hypercorn (ASGI сервер).

    Hypercorn рекомендуется для Quart, так как он разработан специально
    для асинхронных фреймворков и поддерживает HTTP/2 и WebSocket.
    """

    from hypercorn.asyncio import serve
    from hypercorn.config import Config

    template_folder = Path(__file__).parent / "app" / "web" / "templates"
    app.template_folder = str(template_folder)
    app.static_folder = str(Path(__file__).parent / "app" / "web" / "static")

    config = Config()
    config.bind = ["127.0.0.1:5000"]
    config.use_reloader = True  # Автоперезагрузка при изменении кода
    config.loglevel = "DEBUG"  # Логи доступа в stdout

    # Запуск с помощью hypercorn
    logger.info("🚀 Запуск Quart приложения с Hypercorn...")
    logger.info(f"📝 Доступно по адресу: {config.bind}")
    logger.info("🛑 Для остановки нажмите Ctrl+C")
    asyncio.run(serve(app, config))
