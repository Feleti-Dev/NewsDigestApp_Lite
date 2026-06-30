import logging
import re
from typing import Any, Dict, List
from urllib.parse import urlparse

from .client import GoogleSheetsClient

logger = logging.getLogger(__name__)


class SheetsParser:
    """Парсер данных из Google Sheets"""

    def __init__(self, client: GoogleSheetsClient = None):
        self.client = client or GoogleSheetsClient()
        self.sheets_mapping = {
            "X(Twitter)": "twitter",
            "Telegram": "telegram",
            "YouTube": "youtube",
            "Reddit": "reddit",
        }

    def extract_channels_from_sheet(self, sheet_name: str) -> List[Dict[str, Any]]:
        """
        Извлечение каналов из указанного листа

        Args:
            sheet_name: Название листа (например, "Telegram", "YouTube")

        Returns:
            Список словарей с информацией о каналах
        """
        # Получаем сырые данные (первый столбец)
        raw_data = self.client.get_sheet_data(sheet_name, "A:A")

        channels = []
        source_type = self.sheets_mapping.get(sheet_name, sheet_name.lower())

        for i, row in enumerate(raw_data):
            if not row or not row[0]:
                continue  # Пропускаем пустые строки

            url = row[0].strip()

            # Валидация URL
            if not self._is_valid_url(url):
                logger.warning(f"Некорректный URL в строке {i+1}: {url}")
                continue

            # Извлекаем идентификатор канала
            channel_id = self._extract_channel_id(url, source_type)

            if channel_id:
                channel_info = {
                    "source_type": source_type,
                    "url": url,
                    "channel_id": channel_id,
                    "sheet_name": sheet_name,
                    "row_number": i + 1,
                }
                channels.append(channel_info)
                logger.debug(f"Добавлен канал: {source_type} - {channel_id}")

        logger.info(f"📑 Из листа '{sheet_name}' извлечено {len(channels)} каналов")
        return channels

    def _is_valid_url(self, url: str) -> bool:
        """Проверка валидности URL"""
        if not url or not isinstance(url, str):
            return False

        # Базовые проверки
        url = url.strip()
        if not url.startswith(("http://", "https://")):
            return False

        # Проверяем структуру URL
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False

    def _extract_channel_id(self, url: str, source_type: str) -> str:
        """
        Извлечение идентификатора канала из URL

        Args:
            url: Полный URL канала
            source_type: Тип источника

        Returns:
            Идентификатор канала или None
        """
        try:
            if source_type == "telegram":
                # Пример: https://t.me/channelname
                match = re.search(r"t\.me/(.+)", url)
                if "c/" in match.group(1):
                    return "-100"+match.group(1)[2:]
                return str(match.group(1).strip("/")) if match else None

            elif source_type == "youtube":
                # Пример: https://www.youtube.com/@channelname или https://youtube.com/channel/UC...
                patterns = [
                    r"youtube\.com/@([a-zA-Z0-9_\-]+)",
                    r"youtube\.com/channel/([a-zA-Z0-9_\-]+)",
                    r"youtube\.com/c/([a-zA-Z0-9_\-]+)",
                    r"youtube\.com/user/([a-zA-Z0-9_\-]+)",
                ]
                for pattern in patterns:
                    match = re.search(pattern, url)
                    if match:
                        return match.group(1)
                return None

            elif source_type == "reddit":
                # Пример: https://www.reddit.com/r/subredditname/
                match = re.search(r"reddit\.com/r/([a-zA-Z0-9_]+)", url)
                return match.group(1) if match else None

            elif source_type == "twitter":
                # Пример: https://twitter.com/username или https://x.com/username
                patterns = [r"twitter\.com/([a-zA-Z0-9_]+)", r"x\.com/([a-zA-Z0-9_]+)"]
                for pattern in patterns:
                    match = re.search(pattern, url)
                    if match:
                        return match.group(1)
                return None

            else:
                # Для неизвестных источников возвращаем домен
                parsed = urlparse(url)
                return parsed.netloc

        except Exception as e:
            logger.error(f"Ошибка при извлечении ID из {url}: {e}")
            return None

    def get_all_channels(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Получение всех каналов из всех листов

        Returns:
            Словарь {тип_источника: список_каналов}
        """
        all_channels = {}

        # Получаем список всех листов
        sheets = self.client.get_available_sheets()

        if not sheets:
            logger.error("Не удалось получить список листов")
            return all_channels

        # Сразу отбираем только листы, которые есть в sheets_mapping
        expected_sheets = set(self.sheets_mapping.keys())
        matching_sheets = {
            name: data for name, data in sheets.items()
            if name in expected_sheets
        }

        # Обрабатываем только отобранные листы
        for sheet_name in matching_sheets:
            channels = self.extract_channels_from_sheet(sheet_name)
            source_type = self.sheets_mapping[sheet_name]
            all_channels[source_type] = channels if channels else []

            if not channels:
                logger.warning(f"Лист '{sheet_name}' не содержит валидных каналов")

        # Подсчет статистики
        total_channels = sum(len(channels) for channels in all_channels.values())
        logger.info(f"📈 Всего извлечено каналов: {total_channels}")

        for source_type, channels in all_channels.items():
            logger.info(f"  {source_type}: {len(channels)} каналов")

        return all_channels
