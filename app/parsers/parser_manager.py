"""
Менеджер для управления всеми парсерами и планировщиком.
"""
import logging
from typing import Any, Dict

from app.database.db_utils import DatabaseManager
from app.configs.config import config

from .telegram_parser import TelegramParser
from .twitter_parser import TwitterParser
from .youtube_parser import YouTubeParser


logger = logging.getLogger(__name__)


class ParserManager:
    """Менеджер для управления всеми парсерами и планировщиком"""

    def __init__(self,db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.parsers = {}  # Инициализируем пустым
        self._parser_classes = {  # Добавляем словарь классов
            "twitter": TwitterParser,
            "youtube": YouTubeParser,
            "telegram": TelegramParser,
        }

    # Добавляем метод:
    def create_parsers(self):
        """Создание парсеров на основе текущих настроек статусов"""
        if not self.parsers:
            # await self.close_parsers()
            self.parsers = {}
            for name, active in config.app.parser_status.items():
                if not active: continue
                try:
                    self.parsers[name] = self._parser_classes[name]()
                    logger.info(f"В ParserManager создан парсер: {self._parser_classes[name]}")
                except Exception as e:
                    logger.error(f"Ошибка создания парсера {name}: {e}")
                    self.parsers[name] = None
        else:
            return

    # Добавляем метод:
    async def close_parsers(self):
        """Закрытие всех парсеров и клиентов (асинхронно!)"""
        # Закрываем парсеры с асинхронным отключением
        for name, parser in list(self.parsers.items()):
            if parser:
                try:
                    await parser.close()
                except Exception as e:
                    logger.error(f"Ошибка закрытия {name}: {e}")
        self.parsers = {}
        logger.info("Все парсеры и клиенты закрыты")

    async def restart_parsers(self) -> Dict[str, Any]:
        """
        Перезапуск всех парсеров на основе актуальных настроек.
        Закрывает текущие парсеры и создаёт новые.

        Returns:
            Словарь со статусами парсеров
        """
        logger.info("🔄 Перезапуск парсеров...")
        # Закрываем текущие парсеры
        await self.close_parsers()
        # Создаём новые парсеры
        self.create_parsers()
        logger.info("✅ Парсеры перезапущены")
        return self.get_parsers_status()

    def get_parsers_status(self) -> Dict[str, Any]:
        """Проверяем статус API для каждого парсера"""

        # Проверяем статус API для каждого парсера
        api_status = {}
        for name, parser in self.parsers.items():
            if name == "twitter":
                api_status[name] = (
                    "✅ Инициализирован"
                    if parser.client
                    else "❌ Требуются API ID/Hash"
                )
            elif name == "telegram":
                api_status[name] = (
                    "✅ Инициализирован"
                    if parser.client
                    else "❌ Требуются API ID/Hash"
                )
            elif name == "youtube":
                api_status[name] = (
                    "✅ Инициализирован" if parser.youtube else "❌ Требуется API ключ"
                )
            elif name == "reddit":
                api_status[name] = (
                    "✅ Инициализирован"
                    if parser.reddit
                    else "❌ Требуются Client ID/Secret"
                )


        return {
            "api_status": api_status,
        }

    def is_parser_active(self, parser_name: str) -> bool:
        """
        Проверка, активен ли парсер.

        Args:
            parser_name: имя парсера (twitter, telegram, youtube, reddit)

        Returns:
            True если парсер активен в конфигурации
        """
        return config.app.parser_status.get(parser_name, False)