# app/telegram/publisher.py
"""
Публикация дайджестов в Telegram канал
Используем HTML разметку вместо Markdown
"""

import asyncio
import logging
from typing import Any

import aiohttp

from app.configs import config

logger = logging.getLogger(__name__)


class TelegramPublisher:
    """Класс для публикации дайджестов в Telegram канал"""

    def __init__(self):
        self.bot_token = config.api.telegram_bot_token
        self.channel_id = config.api.telegram_channel_id
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

        # Парсим ID канала и ID топика
        if "@" in self.channel_id:
            pass
        elif "/" in self.channel_id:
            cid, tid = self.channel_id.split("/")
            # Добавляем -100, если его нет (для закрытых каналов)
            self.channel_id = cid if cid.startswith("-") else f"-100{cid}"
            self.thread_id = int(tid)
        else:
            self.channel_id = (
                self.channel_id if self.channel_id.startswith("-") else f"-100{self.channel_id}"
            )
            self.thread_id = None

        # Настройки повторных попыток
        self.max_retries = 3
        self.retry_delay = 5  # секунды

        # Проверка конфигурации
        self._validate_config()

    def _validate_config(self):
        """Проверка конфигурации Telegram"""
        if not self.bot_token:
            logger.error("❌ Токен Telegram бота не указан в конфигурации")
            raise ValueError("TELEGRAM_BOT_TOKEN не указан")

        if not self.channel_id:
            logger.error("❌ ID Telegram канала не указан в конфигурации")
            raise ValueError("TELEGRAM_CHANNEL_ID не указан")

        logger.info(
            f"✅ Конфигурация Telegram: бот токен {'установлен' if self.bot_token else 'отсутствует'}, канал: {self.channel_id}"
        )

    async def publish_digest(self, digest_data: dict[str, Any], type: str = "HTML") -> bool:
        """
        Публикация дайджеста в Telegram канал

        Args:
            digest_data: Данные дайджеста

        Returns:
            True если публикация успешна
        """
        logger.info(f"📤 Публикация {digest_data['type']} дайджеста в Telegram...")

        try:
            # Сначала отправляем полный текст
            text_sent = await self._send_text_only(digest_data["text"], type)

            if not text_sent:
                logger.error("❌ Не удалось отправить текст дайджеста")
                return False

            # Затем отправляем изображение (если есть)
            # image_sent = True
            # if digest_data.get('image_url'):
            #     # Для изображения делаем короткую подпись
            #     image_caption = self._prepare_image_caption(digest_data['text'])
            #     image_sent = await self._send_image_with_caption(
            #         digest_data['image_url'],
            #         image_caption
            #     )

            if text_sent:
                logger.info(
                    f"✅ {digest_data['type'].capitalize()} дайджест опубликован в Telegram"
                )
                return True
            else:
                logger.warning("⚠️  Дайджест опубликован, но изображение не отправлено")
                return True  # Все равно считаем успехом, т.к. текст отправлен

        except Exception as e:
            logger.error(f"❌ Ошибка публикации дайджеста: {e}")
            return False

    async def _send_image_with_caption(self, image_url: str, caption: str) -> bool:
        """
        Отправка изображения с подписью

        Args:
            image_url: URL изображения
            caption: Подпись

        Returns:
            True если отправка успешна
        """
        # Проверяем, что URL изображения валидный
        if not await self._is_valid_image_url(image_url):
            logger.warning(f"⚠️  Некорректный URL изображения: {image_url}")
            return False

        for attempt in range(self.max_retries):
            try:
                logger.info(f"🖼️  Попытка {attempt + 1}: отправка изображения")

                # Формируем URL для API
                url = f"{self.base_url}/sendPhoto"

                # Параметры запроса
                params = {
                    "chat_id": self.channel_id,
                    "photo": image_url,
                    "caption": caption[:1024]
                    if len(caption) > 1024
                    else caption,  # Ограничение Telegram
                    "parse_mode": "HTML",
                    "disable_web_page_preview": "false",
                }
                # ДОБАВЬТЕ ЭТО:
                if self.thread_id:
                    params["message_thread_id"] = self.thread_id

                # Отправляем запрос
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, params=params, timeout=30) as response:
                        response_text = await response.text()

                        if response.status == 200:
                            logger.info("✅ Изображение отправлено успешно")
                            return True
                        else:
                            logger.error(
                                f"❌ Ошибка API Telegram: {response.status} - {response_text}"
                            )

                            # Пробуем отправить без разметки
                            if "can't parse entities" in response_text.lower():
                                logger.warning("⚠️  Проблема с HTML, пробуем без разметки")
                                params["parse_mode"] = "HTML"

                                async with session.post(
                                    url, params=params, timeout=30
                                ) as retry_response:
                                    if retry_response.status == 200:
                                        logger.info("✅ Изображение отправлено без разметки")
                                        return True
                                    else:
                                        retry_text = await retry_response.text()
                                        logger.error(f"❌ Ошибка повторной отправки: {retry_text}")

                            if attempt < self.max_retries - 1:
                                await asyncio.sleep(self.retry_delay)
                            else:
                                return False

            except asyncio.TimeoutError:
                logger.error(f"⏱️  Таймаут при отправке изображения (попытка {attempt + 1})")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)

            except Exception as e:
                logger.error(f"❌ Ошибка при отправке изображения: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)

        return False

    async def _send_with_image(self, digest_data: dict[str, Any]) -> bool:
        """
        Отправка дайджеста с изображением

        Args:
            digest_data: Данные дайджеста

        Returns:
            True если отправка успешна
        """
        image_url = digest_data["image_url"]
        text = digest_data["text"]

        # Подготавливаем подпись для изображения
        caption = self._prepare_image_caption(text)

        # Проверяем, что URL изображения валидный
        if not await self._is_valid_image_url(image_url):
            logger.warning(f"⚠️  Некорректный URL изображения: {image_url}")
            return False

        # Пробуем отправить с HTML разметкой
        parse_mode = "HTML"

        for attempt in range(self.max_retries):
            try:
                logger.info(f"🖼️  Попытка {attempt + 1}: отправка изображения")

                # Формируем URL для API
                url = f"{self.base_url}/sendPhoto"

                # Параметры запроса с HTML
                params = {
                    "chat_id": self.channel_id,
                    "photo": image_url,
                    "caption": caption[:1024]
                    if len(caption) > 1024
                    else caption,  # Ограничение Telegram для подписи
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": "false",
                }

                # Отправляем запрос
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, params=params, timeout=30) as response:
                        response_text = await response.text()

                        if response.status == 200:
                            logger.info("✅ Изображение отправлено успешно (HTML)")
                            return True
                        else:
                            logger.error(
                                f"❌ Ошибка API Telegram: {response.status} - {response_text}"
                            )

                            # Пробуем отправить без разметки
                            if (
                                "can't parse entities" in response_text.lower()
                                or "bad request" in response_text.lower()
                            ):
                                logger.warning("⚠️  Проблема с HTML, пробуем без разметки")
                                params["parse_mode"] = "HTML"

                                async with session.post(
                                    url, params=params, timeout=30
                                ) as retry_response:
                                    if retry_response.status == 200:
                                        logger.info("✅ Изображение отправлено без разметки")
                                        return True
                                    else:
                                        retry_text = await retry_response.text()
                                        logger.error(f"❌ Ошибка повторной отправки: {retry_text}")

                            if attempt < self.max_retries - 1:
                                await asyncio.sleep(self.retry_delay)
                            else:
                                return False

            except asyncio.TimeoutError:
                logger.error(f"⏱️  Таймаут при отправке изображения (попытка {attempt + 1})")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)

            except Exception as e:
                logger.error(f"❌ Ошибка при отправке изображения: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)

        return False

    async def _send_text_only(self, text: str, type: str = "HTML") -> bool:
        """
        Отправка только текста дайджеста

        Args:
            text: Текст дайджеста

        Returns:
            True если отправка успешна
        """
        # Проверяем длину текста (Telegram ограничение 4096 символов)
        if len(text) > 4096:
            logger.warning("⚠️  Текст дайджеста превышает 4096 символов, обрезаем")
            text = text[:4090] + "..."
        # пробуем  HTML и Markdown
        parse_mode = type

        for attempt in range(self.max_retries):
            try:
                logger.info(f"📝 Попытка {attempt + 1}: отправка текста ({len(text)} символов)")

                # Формируем URL для API
                url = f"{self.base_url}/sendMessage"

                # Параметры запроса
                params = {
                    "chat_id": str(self.channel_id),
                    "text": str(text),
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": "false",
                }
                # ДОБАВЬТЕ ЭТО:
                if self.thread_id:
                    params["message_thread_id"] = self.thread_id

                # Отправляем запрос
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, params=params, timeout=30) as response:
                        response_text = await response.text()

                        if response.status == 200:
                            logger.info("✅ Текст отправлен успешно")
                            return True
                        else:
                            logger.error(
                                f"❌ Ошибка API Telegram: {response.status} - {response_text}"
                            )

                            # Проверяем конкретные ошибки
                            if (
                                "can't parse entities" in response_text.lower()
                                or "bad request" in response_text.lower()
                            ):
                                logger.warning("⚠️  Проблема с HTML, пробуем без разметки")

                                params["parse_mode"] = "HTML"

                                async with session.post(
                                    url, params=params, timeout=30
                                ) as retry_response:
                                    if retry_response.status == 200:
                                        logger.info("✅ Текст отправлен без разметки")
                                        return True
                                    else:
                                        retry_text = await retry_response.text()
                                        logger.error(f"❌ Ошибка повторной отправки: {retry_text}")

                            if "message is too long" in response_text.lower():
                                logger.error("❌ Сообщение слишком длинное для Telegram")
                                return False

                            if attempt < self.max_retries - 1:
                                await asyncio.sleep(self.retry_delay)
                            else:
                                return False

            except asyncio.TimeoutError:
                logger.error(f"⏱️  Таймаут при отправке текста (попытка {attempt + 1})")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)

            except Exception as e:
                logger.error(f"❌ Ошибка при отправке текста: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)

        return False

    def _prepare_image_caption(self, full_text: str, max_length: int = 1024) -> str:
        """
        Подготовка подписи для изображения

        Args:
            full_text: Полный текст дайджеста
            max_length: Максимальная длина подписи

        Returns:
            Подпись для изображения
        """
        try:
            # Берем начало текста (заголовок и первую новость)
            lines = full_text.split("\n")

            if len(lines) < 10:
                # Если текст короткий, возвращаем его весь
                caption = full_text
            else:
                # Берем заголовок и первую новость
                caption_lines = lines
                caption = "\n".join(caption_lines)

            # Ограничиваем длину
            if len(caption) > max_length:
                # Находим хорошее место для обрезки
                cut_position = caption[:max_length].rfind("\n")
                if cut_position > 0:
                    caption = caption[:cut_position]
                else:
                    caption = caption[:max_length]
                caption += "..."

            return caption

        except Exception as e:
            logger.error(f"Ошибка подготовки подписи: {e}")
            return "📰 <b>Дайджест новостей ИИ</b>\n\n📖 <b>Читайте полный дайджест в сообщении выше</b> 👆"

    async def _is_valid_image_url(self, url: str) -> bool:
        """
        Проверка валидности URL изображения

        Args:
            url: URL для проверки

        Returns:
            True если URL валидный
        """
        if not url or not url.startswith("http"):
            return False

        # Проверяем расширения файлов
        valid_extensions = [".jpg", ".jpeg", ".png", ".gif", ".webp"]
        url_lower = url.lower()

        # Проверяем по расширению
        if any(url_lower.endswith(ext) for ext in valid_extensions):
            return True

        # Для YouTube и других сервисов проверяем наличие ключевых слов
        if "youtube.com" in url_lower or "youtu.be" in url_lower:
            # YouTube превью обычно содержат эти строки
            if any(keyword in url_lower for keyword in ["maxresdefault", "hqdefault", "mqdefault"]):
                return True

        # Для других сервисов можно добавить дополнительные проверки
        return bool("imgur.com" in url_lower or "i.redd.it" in url_lower)

    async def test_connection(self) -> bool:
        """
        Тестирование подключения к Telegram API

        Returns:
            True если подключение успешно
        """
        logger.info("🔗 Тестирование подключения к Telegram API...")

        try:
            url = f"{self.base_url}/getMe"

            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("ok"):
                            bot_info = data["result"]
                            logger.info(
                                f"✅ Бот подключен: @{bot_info['username']} ({bot_info['first_name']})"
                            )
                            return True
                        else:
                            logger.error(f"❌ Ошибка в ответе Telegram: {data}")
                            return False
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Ошибка HTTP: {response.status} - {error_text}")
                        return False

        except Exception as e:
            logger.error(f"❌ Ошибка тестирования подключения: {e}")
            return False

    async def send_test_message(self, message: str | None = None) -> bool:
        """
        Отправка тестового сообщения в канал

        Args:
            message: Текст сообщения

        Returns:
            True если отправка успешна
        """
        from datetime import datetime

        test_message = (
            message
            or f"✅ <b>Тестовое сообщение от News Digest Bot</b>\n\n🤖 Система работает корректно!\n📅 Время: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        )

        logger.info("🧪 Отправка тестового сообщения в Telegram...")

        try:
            url = f"{self.base_url}/sendMessage"

            params = {
                "chat_id": self.channel_id,
                "text": test_message,
                "parse_mode": "HTML",
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, params=params, timeout=10) as response:
                    if response.status == 200:
                        logger.info("✅ Тестовое сообщение отправлено")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Ошибка отправки тестового сообщения: {error_text}")

                        # Пробуем без разметки
                        if (
                            "can't parse entities" in error_text.lower()
                            or "bad request" in error_text.lower()
                        ):
                            logger.warning("⚠️  Проблема с HTML, пробуем без разметки")
                            params["parse_mode"] = "HTML"

                            async with session.post(
                                url, params=params, timeout=10
                            ) as retry_response:
                                if retry_response.status == 200:
                                    logger.info("✅ Тестовое сообщение отправлено без разметки")
                                    return True

                        return False

        except Exception as e:
            logger.error(f"❌ Ошибка при отправке тестового сообщения: {e}")
            return False
