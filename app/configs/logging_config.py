import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler

from .config import config


def setup_logging():
    """Настройка логирования для приложения"""

    # Создаем директорию для логов если не существует
    os.makedirs(config.app.logs_dir, exist_ok=True)

    # Формат логов
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Базовый логгер
    logger = logging.getLogger()

    # Устанавливаем уровень логирования
    log_level = getattr(logging, config.app.log_level.upper(), logging.INFO)
    logger.setLevel(log_level)

    # Удаляем существующие обработчики
    logger.handlers.clear()

    # Консольный обработчик
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)

    # --- Файловый обработчик с кастомной ротацией ---
    # Базовое имя файла
    base_filename = f"news_digest_{datetime.now().strftime('%Y%m%d')}.log"
    log_file = os.path.join(config.app.logs_dir, base_filename)

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8"
    )

    # КАСТОМИЗАЦИЯ ИМЕНИ РОТАЦИИ
    # Превращаем name.log.1 -> name_1.log
    def custom_namer(default_name):
        base, ext, num = default_name.rsplit('.', 2)  # Разбиваем путь.log.1
        return f"{base}_{num}.log"

    file_handler.namer = custom_namer

    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    file_handler.setLevel(log_level)
    logger.addHandler(file_handler)

    # Логирование запуска
    logger.info("=" * 60)
    logger.info("Запуск News Digest Application")
    logger.info(f"Уровень логирования: {config.app.log_level}")
    logger.info(f"Логи будут сохранены в: {config.app.logs_dir}")
    logger.info("=" * 60)

    return logger