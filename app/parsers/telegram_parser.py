import getpass
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from telethon.errors import SessionPasswordNeededError
from app.configs import config
from .base_parser import BaseParser

logger = logging.getLogger(__name__)


class TelegramParser(BaseParser):
    """Парсер для Telegram с использованием Telethon"""

    def __init__(self):
        super().__init__("Telegram")
        self.client = None
        self.session_file = "data/telegram_session.session"
        self._phone: Optional[str] = None
        self._two_fa_password: Optional[str] = None
        self._init_client()

    def _init_client(self):
        """Инициализация клиента Telethon с сохраненной сессией"""


        api_id = config.api.telegram_api_id
        api_hash = config.api.telegram_api_hash

        # Загружаем настройки 2FA из конфигурации
        self._phone = config.api.telegram_phone
        self._two_fa_password = config.api.telegram_2fa_password

        if not api_id or not api_hash:
            logger.warning(
                "Telegram API ID/Hash не указаны. Telegram парсер будет работать в тестовом режиме."
            )
            return

        try:
            from telethon import TelegramClient

            # Создаем директорию для сессии, если её нет
            os.makedirs(os.path.dirname(self.session_file), exist_ok=True)

            # Используем постоянное имя сессии
            self.client = TelegramClient(
                session=self.session_file,
                api_id=int(api_id),
                api_hash=api_hash,
                device_model="News Digest Bot",
                system_version="1.0",
                app_version="1.0",
                system_lang_code="ru",
                lang_code="ru",
            )

            logger.info(f"Telethon клиент инициализирован. Сессия: {self.session_file}")

        except ImportError:
            logger.error("Telethon не установлен. Установите: pip install telethon")
            self.client = None
        except Exception as e:
            logger.error(f"Ошибка инициализации Telethon: {e}")
            self.client = None

    def _get_2fa_password(self) -> Optional[str]:
        """
        Получение пароля 2FA из конфигурации или другого источника

        Returns:
            Пароль 2FA или None, если не требуется
        """
        # Приоритет источников пароля:
        # 1. Уже загруженный из конфигурации
        # 2. Переменная окружения


        if self._two_fa_password:
            return self._two_fa_password

        # Пробуем из переменной окружения
        env_password = os.getenv('TELEGRAM_2FA_PASSWORD')
        if env_password:
            logger.info("Пароль 2FA загружен из переменной окружения TELEGRAM_2FA_PASSWORD")
            return env_password


        return None

    def _code_callback(self) -> str:
        """Колбэк для получения кода из консоли (интерактивный режим)"""
        return input("Введите код из Telegram: ")

    def _password_callback(self) -> str:
        """Колбэк для получения пароля 2FA из консоли (интерактивный режим)"""
        password = self._get_2fa_password()
        if password:
            return password
        return getpass.getpass("Введите пароль 2FA: ")

    def _phone_callback(self) -> str:
        """Колбэк для получения номера телефона"""
        if self._phone:
            return self._phone
        return input("Введите номер телефона (+79001234567): ")

    async def _authenticate(self) -> bool:
        """
        Аутентификация с поддержкой 2FA

        Returns:
            True если успешно авторизован
        """
        if not self.client:
            return False

        try:
            # Проверяем, не авторизованы ли мы уже
            if await self.client.is_user_authorized():
                logger.debug("Используем сохраненную сессию")
                return True

            logger.info("Требуется авторизация в Telegram...")

            # Если есть номер телефона, используем его
            phone = self._phone or None

            # Если есть пароль 2FA, используем start с паролем
            two_fa_password = self._get_2fa_password()

            if phone != "" and two_fa_password != "":
                # Полный автоматический режим: телефон и 2FA пароль известны
                try:
                    await self.client.send_code_request(phone)
                    logger.info(f"Код отправлен на номер {phone}")

                    # В автоматическом режиме без кода - используем QR или ждем ручного ввода
                    # Для полной автоматизации нужно получить код из другого источника
                    # Пока используем интерактивный ввод кода
                    code = self._code_callback()

                    await self.client.sign_in(phone, code, password=two_fa_password)
                    logger.info("Авторизация успешна (с 2FA)")
                    return True

                except SessionPasswordNeededError:
                    # Пароль требуется, но не был передан
                    if two_fa_password:
                        await self.client.sign_in(phone, password=two_fa_password)
                        logger.info("Авторизация успешна (2FA)")
                        return True
                    else:
                        logger.error("Требуется пароль 2FA, но он не настроен")
                        return False

            elif phone != "":
                # Есть телефон, но нет 2FA пароля - может 2FA не включен
                try:
                    await self.client.send_code_request(phone)
                    code = self._code_callback()
                    await self.client.sign_in(phone, code)
                    logger.info("Авторизация успешна (без 2FA)")
                    return True
                except SessionPasswordNeededError:
                    # 2FA включен, запрашиваем пароль
                    password = self._password_callback()
                    if password:
                        await self.client.sign_in(phone, password=password)
                        logger.info("Авторизация успешна (2FA введён)")
                        # Сохраняем пароль для будущих сессий
                        self._two_fa_password = password
                        return True
                    return False
            else:
                # Нет номера телефона в конфигурации - полностью интерактивный режим
                await self.client.start(
                    phone=self._phone_callback,
                    password=self._password_callback,
                    code_callback=self._code_callback,
                )
                logger.info("Авторизация успешна (интерактивный режим)")
                return True

        except Exception as e:
            logger.error(f"Ошибка авторизации: {e}")
            return False

        return False

    async def fetch_channel_news(
            self, channel_url: str, channel_id: str
    ) -> List[Dict[str, Any]]:
        """
        Получение сообщений с Telegram канала
        """
        # Если нет клиента Telethon, возвращаем тестовые данные
        if not self.client:
            return []

        try:
            # Используем контекстный менеджер для автоматического управления соединением
            if not self.client.is_connected():
                await self.client.connect()
                logger.info("Telethon клиент подключен")

            # Авторизация с поддержкой 2FA
            auth_success = await self._authenticate()
            if not auth_success:
                logger.error("Не удалось авторизоваться в Telegram")
                return []

            # Получаем сущность канала
            try:
                entity = None
                # Пробуем разные форматы
                if channel_id.startswith("@"):
                    entity = await self.client.get_entity(channel_id)
                    logger.debug(f"Найден канал по @username: {channel_id}")
                elif channel_id.startswith("https://t.me/"):
                    # Извлекаем username из URL
                    username = channel_id.replace("https://t.me/", "").split("/")[0]
                    entity = await self.client.get_entity(f"@{username}")
                    logger.debug(f"Найден канал по URL: {channel_id}")
                else:
                    # Пробуем как username
                    entity = await self.client.get_entity(f"@{channel_id}")
                    logger.debug(f"Найден канал по ID: {channel_id}")

            except Exception as e:
                logger.error(f"Не удалось найти канал {channel_id}: {e}")
                return []

            # Получаем сообщения (сырые объекты Telethon)
            messages = []
            try:
                time_limit = datetime.now(tz=timezone.utc) - self.collection_period
                async for message in self.client.iter_messages(
                        entity, limit=config.app.max_news_per_channel, reverse=False
                ):
                    # Проверяем возраст сообщения
                    message_date = (
                        message.date.replace(tzinfo=timezone.utc)
                        if message.date
                        else None
                    )
                    if message_date and message_date < time_limit:
                        continue

                    # Пропускаем сообщения без текста
                    if not (message.text or message.message):
                        continue

                    messages.append(message)  # Сырой объект сообщения

                    if len(messages) >= self.max_items_per_channel:
                        break

            except Exception as e:
                logger.error(f"Ошибка при получении сообщений: {e}")
                return []

            return messages  # Возвращаем СЫРЫЕ данные

        except Exception as e:
            logger.error(f"Ошибка при получении сообщений из {channel_id}: {e}")
            return []

    def extract_news_data(self, raw_data: Any) -> Dict[str, Any]:
        """Извлечение данных из сообщения Telegram"""
        try:
            message = raw_data
            # Формируем URL сообщения
            url = ""
            if hasattr(message, "chat") and message.chat:
                if hasattr(message.chat, "username") and message.chat.username:
                    url = f"https://t.me/{message.chat.username}/{message.id}"
                elif hasattr(message.chat, "id"):
                    # Для каналов без username используем c/
                    url = f"https://t.me/c/{abs(message.chat.id)}/{message.id}"

            if not url and hasattr(message, "peer_id"):
                # Пробуем через peer_id
                try:
                    if hasattr(message.peer_id, "channel_id"):
                        channel_id = message.peer_id.channel_id
                        url = f"https://t.me/c/{channel_id}/{message.id}"
                except:
                    pass

            # Получаем текст сообщения
            text = getattr(message, "text", getattr(message, "message", ""))

            # Удаляем форматирование, если есть
            import re

            text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)  # Удаляем **жирный**
            text = re.sub(r"__(.*?)__", r"\1", text)  # Удаляем __подчеркнутый__

            # Создаем заголовок из первых 50 символов текста
            title = text[:150] + "..." if len(text) > 50 else text

            # ВАЖНО: Правильно обрабатываем дату
            pub_date = None
            if hasattr(message, "date"):
                pub_date = message.date
                # Если дата уже с timezone, конвертируем в naive datetime
                if hasattr(pub_date, "tzinfo") and pub_date.tzinfo is not None:
                    pub_date = pub_date.astimezone(timezone.utc).replace(tzinfo=None)

            if not pub_date:
                pub_date = datetime.now(timezone.utc).replace(tzinfo=None)

            news_data = {
                "url": url,
                "title": title,
                "text": text,
                "publication_date": pub_date,  # Теперь это объект datetime
                "has_image": False,
                "image_url": None,
                "note": f"Telegram message; message_id={message.id}",
            }

            # Проверяем наличие медиа
            if hasattr(message, "media") and message.media:
                if hasattr(message.media, "photo"):
                    news_data["has_image"] = True
                    news_data["note"] += "; has_photo=True"
                elif hasattr(message.media, "document"):
                    if hasattr(
                            message.media.document, "mime_type"
                    ) and message.media.document.mime_type.startswith("image/"):
                        news_data["has_image"] = True
                        news_data["note"] += "; has_document_image=True"

            # Ограничиваем длину текста
            # if news_data["text"] and len(news_data["text"]) > 3000:
            #     news_data["text"] = news_data["text"][:3000] + "..."

            return news_data

        except Exception as e:
            logger.error(f"Ошибка извлечения данных из сообщения Telegram: {e}")
            import traceback

            logger.error(f"Traceback: {traceback.format_exc()}")
            return {}

    async def close(self):
        if self.client and self.client.is_connected():
            try:
                await self.client.disconnect()
            except Exception as e:
                logger.error("Ошибка при закрытии парсера Telegram ", e)
        self.client = None
