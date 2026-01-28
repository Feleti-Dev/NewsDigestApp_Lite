"""
Менеджер синхронизации с Google Sheets.
Работает через БД, кэш в JSON-файлах не используется.
"""

import logging
from datetime import datetime
from typing import Dict, List

from app.google_sheets.parser import SheetsParser

logger = logging.getLogger(__name__)


class SheetsSyncManager:
    """
    Менеджер для синхронизации каналов с Google Sheets.

    Ключевые изменения:
    - Не использует JSON-кэш
    - Все данные хранятся в БД (таблица channel_sources)
    - Приватный метод _get_db_manager() для ленивой инициализации
    """

    def __init__(self, db_manager=None):
        self._db_manager = db_manager  # Может быть None при инициализации
        self.sheets_parser = SheetsParser()
        self.last_sync_time = None
        self.is_running = True
        self.start_time = datetime.now()

    @property
    def db_manager(self):
        """Ленивое получение DatabaseManager"""
        if self._db_manager is None:
            from app.database.db_utils import DatabaseManager
            self._db_manager = DatabaseManager()
        return self._db_manager

    def needs_sync(self, force: bool = False) -> bool:
        """
        Проверка необходимости синхронизации.

        Args:
            force: принудительная синхронизация

        Returns:
            True если синхронизация нужна
        """
        if force:
            return True

        # Синхронизируем при первом запуске
        if not self.last_sync_time:
            return True

        # Синхронизируем раз в 24 часа
        time_since_sync = datetime.now() - self.last_sync_time
        return time_since_sync.total_seconds() >= 24 * 3600

    def sync_channels(self, force: bool = False) -> Dict[str, List]:
        """
        Синхронизация каналов с Google Sheets.

        Args:
            force: принудительная синхронизация

        Returns:
            Словарь {source_type: [ChannelSource, ...]} из БД
        """
        if not self.needs_sync(force):
            logger.info("Синхронизация не требуется, загружаем из БД")
            return self._load_from_db()

        try:
            logger.info("🔄 Синхронизация с Google Sheets...")

            # Получаем каналы из Google Sheets
            all_channels_raw = self.sheets_parser.get_all_channels()

            if not all_channels_raw:
                logger.error("Не удалось получить каналы из Google Sheets")
                return self._load_from_db()

            # Сохраняем в БД
            created_count = self.db_manager.sync_channels_from_sheets(all_channels_raw)
            logger.info(f"Сохранено {created_count} новых каналов в БД")

            self.last_sync_time = datetime.now()
            logger.info("✅ Синхронизация завершена")

            return self._load_from_db()

        except Exception as e:
            logger.error(f"❌ Ошибка синхронизации: {e}")
            return self._load_from_db()

    def _load_from_db(self) -> Dict[str, List]:
        """
        Загрузка каналов из БД.

        Returns:
            Словарь {source_type: [ChannelSource, ...]}
        """
        try:
            channels = self.db_manager.get_all_active_channels()
            total = sum(len(ch_list) for ch_list in channels.values())
            logger.info(f"Загружено {total} каналов из БД")
            return channels
        except Exception as e:
            logger.error(f"Ошибка загрузки из БД: {e}")
            return {}