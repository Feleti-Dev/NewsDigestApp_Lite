import logging
import os
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.configs import config

logger = logging.getLogger(__name__)


class GoogleSheetsClient:
    """Клиент для работы с Google Sheets API"""

    def __init__(self):
        self.credentials_path = config.google_sheets.credentials_path
        self.service = None
        self._authenticate()

    def _authenticate(self) -> None:
        """Аутентификация в Google Sheets API"""
        try:
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(
                    f"Файл с учетными данными не найден: {self.credentials_path}\n"
                    f"Создайте сервисный аккаунт и скачайте JSON ключ."
                )

            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
            )

            self.service = build("sheets", "v4", credentials=credentials)
            logger.info("✅ Успешная аутентификация в Google Sheets API")

        except Exception as e:
            logger.error(f"❌ Ошибка аутентификации в Google Sheets: {e}")
            self.service = None

    def get_sheet_data(self, sheet_name: str, range_name: str = "A:A") -> list[list[Any]]:
        """
        Получение данных из указанного листа и диапазона

        Args:
            sheet_name: Название листа
            range_name: Диапазон данных (по умолчанию первый столбец)

        Returns:
            Список строк с данными
        """
        if not self.service:
            raise ConnectionError("Сервис Google Sheets не инициализирован")

        try:
            # Формируем полный диапазон
            full_range = f"{sheet_name}!{range_name}"

            # Получаем данные
            result = (
                self.service.spreadsheets()
                .values()
                .get(
                    spreadsheetId=config.google_sheets.google_spreadsheet_id,
                    range=full_range,
                )
                .execute()
            )

            values = result.get("values", [])

            if not values:
                logger.warning(f"Лист '{sheet_name}' пуст или не найден")
                return []

            logger.info(f"📊 Получено {len(values)} строк из листа '{sheet_name}'")
            return values

        except HttpError as e:
            logger.error(f"Ошибка при получении данных из листа '{sheet_name}': {e}")
            return []
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            return []

    def get_available_sheets(self) -> dict[str, str]:
        """Получение списка доступных листов в таблице"""
        if not self.service or not config.google_sheets.google_spreadsheet_id:
            return {}

        try:
            spreadsheet = (
                self.service.spreadsheets()
                .get(spreadsheetId=config.google_sheets.google_spreadsheet_id)
                .execute()
            )

            sheets = {}
            for sheet in spreadsheet.get("sheets", []):
                properties = sheet.get("properties", {})
                sheet_id = properties.get("sheetId")
                sheet_title = properties.get("title")
                sheets[sheet_title] = sheet_id

            logger.info(f"📋 Найдено листов: {len(sheets)}")
            return sheets

        except Exception as e:
            logger.error(f"Ошибка при получении списка листов: {e}")
            return {}
