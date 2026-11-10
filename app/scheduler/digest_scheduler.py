"""
Планировщик задач для формирования и отправки дайджестов
Использует APScheduler для периодического выполнения задач
"""

import asyncio
import logging
import traceback
from datetime import datetime

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.configs.config import config
from app.database.db_utils import DatabaseManager
from app.digest.creator import DigestCreator
from app.telegram.publisher import TelegramPublisher

logger = logging.getLogger(__name__)


class DigestScheduler:
    """Планировщик для периодического формирования и отправки дайджестов"""

    def __init__(self, schedule_publish=True):
        self.scheduler: AsyncIOScheduler | None = None
        self.digest_creator = DigestCreator()
        self.db_manager = DatabaseManager()
        self.moscow_tz = pytz.timezone("Europe/Moscow")
        self.schedule_publish = schedule_publish

        # Настройки повторных попыток
        self.max_retries = 3
        self.retry_delay = 300  # 5 минут

        # Флаг для предотвращения параллельного выполнения
        self.is_processing = False

        # Флаги для статистики
        self.start_time = None
        self.is_running = False

        try:
            self.telegram_publisher = TelegramPublisher()
        except Exception:
            self.telegram_publisher = None

    async def start(self):
        """Запуск планировщика"""
        logger.info("🚀 Запуск планировщика дайджестов...")

        self.is_running = True
        self.start_time = datetime.now()

        if not self.schedule_publish:
            return

        # Инициализация планировщика
        self.scheduler = AsyncIOScheduler(timezone=self.moscow_tz)

        if config.scheduler.daily_digest["enabled"]:
            # 1. ЕЖЕДНЕВНЫЙ дайджест
            self.scheduler.add_job(
                self._execute_daily_digest,
                trigger=CronTrigger(
                    hour=config.scheduler.daily_digest["hour"],
                    minute=config.scheduler.daily_digest["minute"],
                ),
                #  trigger="interval",
                #  minutes=2,
                id="daily_digest",
                name=f"Ежедневный дайджест в {config.scheduler.daily_digest['hour']:02d}:{config.scheduler.daily_digest['minute']:02d} МСК",
                replace_existing=True,
                misfire_grace_time=30,
                max_instances=1,
            )
        if config.scheduler.weekly_digest["enabled"]:
            # 2. ЕЖЕНЕДЕЛЬНЫЙ дайджест
            self.scheduler.add_job(
                self._execute_weekly_digest,
                trigger=CronTrigger(
                    day_of_week=config.scheduler.weekly_digest["day_of_week"],
                    hour=config.scheduler.weekly_digest["hour"],
                    minute=config.scheduler.weekly_digest["minute"],
                ),
                id="weekly_digest",
                name=f"Еженедельный дайджест ({config.scheduler.weekly_digest['day_of_week']}, {config.scheduler.weekly_digest['hour']:02d}:{config.scheduler.weekly_digest['minute']:02d} МСК)",
                replace_existing=True,
                misfire_grace_time=30,
                max_instances=1,
            )
        if config.scheduler.monthly_digest["enabled"]:
            # 3. ЕЖЕМЕСЯЧНЫЙ дайджест
            self.scheduler.add_job(
                self._execute_monthly_digest,
                trigger=CronTrigger(
                    day=config.scheduler.monthly_digest["day"],
                    hour=config.scheduler.monthly_digest["hour"],
                    minute=config.scheduler.monthly_digest["minute"],
                ),
                id="monthly_digest",
                name=f"Ежемесячный дайджест ({config.scheduler.monthly_digest['day']} число, {config.scheduler.monthly_digest['hour']:02d}:{config.scheduler.monthly_digest['minute']:02d} МСК)",
                replace_existing=True,
                misfire_grace_time=30,
                max_instances=1,
            )

        # 4. Тестовый триггер для отладки (каждые 10 минут)
        # Раскомментировать только для тестирования
        # self.scheduler.add_job(
        #     self._execute_test_digest,
        #     trigger='interval',
        #     minutes=1,
        #     id='test_digest',
        #     name='Тестовый дайджест (каждые 10 минут)'
        # )

        # Запуск планировщика
        self.scheduler.start()
        logger.info("✅ Планировщик дайджестов запущен")

        # Выводим информацию о запланированных задачах
        self._log_scheduled_jobs()

    def _log_scheduled_jobs(self):
        """Логирование запланированных задач"""
        if not self.scheduler:
            return

        logger.info("📅 Запланированные задачи дайджестов:")
        for job in self.scheduler.get_jobs():
            next_run = (
                job.next_run_time.astimezone(self.moscow_tz)
                if job.next_run_time
                else "Не запланировано"
            )
            logger.info(f"  • {job.name}: следующее выполнение {next_run}")

    async def stop(self):
        """Корректная остановка планировщика"""
        logger.info("🛑 Остановка планировщика дайджестов...")

        if self.scheduler:
            try:
                self.scheduler.shutdown(wait=False)
            except Exception as e:
                logger.warning(f"Попытка закрыть несуществующие планировщики публикации: {e}")
            logger.info("✅ Планировщик дайджестов остановлен")

        self.is_running = False
        self.start_time = None

    async def _execute_daily_digest(self):
        """Выполнение ежедневного дайджеста"""
        await self.execute_digest_with_retry("daily")

    async def _execute_weekly_digest(self):
        """Выполнение еженедельного дайджеста"""
        await self.execute_digest_with_retry("weekly")

    async def _execute_monthly_digest(self):
        """Выполнение ежемесячного дайджеста"""
        await self.execute_digest_with_retry("monthly")

    async def _execute_test_digest(self):
        """Тестовый дайджест для отладки"""
        logger.info("🧪 Выполнение тестового дайджеста...")
        await self.execute_digest_with_retry("daily", is_test=True)

    async def _run_single_digest_attempt(self, digest_type: str, is_test: bool = False) -> str:
        """
        Одна попытка публикации дайджеста с ретраями.

        Returns:
            'success' | 'no_data' | 'failed'
        """
        operation_name = f"тестовый {digest_type}" if is_test else digest_type

        for attempt in range(self.max_retries):
            try:
                logger.info(
                    "Попытка %s/%s: %s дайджест",
                    attempt + 1,
                    self.max_retries,
                    operation_name,
                )
                digest_data = await self.digest_creator.create_digest(
                    digest_type=digest_type, is_test=is_test
                )
                if not digest_data or not digest_data.get("news_items"):
                    logger.warning("Нет данных для %s дайджеста", operation_name)
                    return "no_data"

                success = await self.telegram_publisher.publish_digest(digest_data)
                if success:
                    if not is_test:
                        news_ids = [item["id"] for item in digest_data["news_items"]]
                        updated = self.db_manager.mark_as_used(news_ids, digest_type)
                        logger.info(
                            "Помечено %s новостей как использованные в %s",
                            updated,
                            digest_type,
                        )
                    logger.info("%s дайджест успешно обработан", operation_name)
                    return "success"

            except Exception as e:
                logger.error("Ошибка при выполнении %s дайджеста: %s", operation_name, e)
                if attempt < self.max_retries - 1:
                    logger.info("Повторная попытка через %s секунд...", self.retry_delay)
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(
                        "Не удалось выполнить %s после %s попыток",
                        operation_name,
                        self.max_retries,
                    )
                    logger.error("Трассировка:\n%s", traceback.format_exc())

        return "failed"

    async def execute_digest_batch(
        self, digest_type: str, count: int, is_test: bool = False
    ) -> int:
        """Последовательная публикация до count дайджестов (single-pass и т.п.)."""
        if self.telegram_publisher is None:
            logger.error("Невозможно запустить дайджест без данных телеграмма")
            return 0

        if self.is_processing:
            logger.warning("Дайджест %s уже выполняется, пропускаем batch", digest_type)
            return 0

        self.is_processing = True
        published = 0
        try:
            limit = max(1, min(20, int(count)))
            for index in range(limit):
                outcome = await self._run_single_digest_attempt(digest_type, is_test)
                if outcome == "success":
                    published += 1
                elif outcome == "no_data":
                    logger.info(
                        "Остановка batch: нет новостей на дайджест %s/%s",
                        index + 1,
                        limit,
                    )
                    break
                else:
                    logger.warning(
                        "Остановка batch: ошибка на дайджест %s/%s",
                        index + 1,
                        limit,
                    )
                    break
            logger.info(
                "Batch %s: опубликовано %s из %s дайджестов",
                digest_type,
                published,
                limit,
            )
        finally:
            self.is_processing = False
        return published

    async def execute_digest_with_retry(self, digest_type: str, is_test: bool = False):
        """
        Выполнение одного дайджеста с повторными попытками при ошибках.
        """
        if self.telegram_publisher is None:
            logger.error("Невозможно запустить дайджест без данных телеграмма")
            return

        if self.is_processing:
            logger.warning("Дайджест %s уже выполняется, пропускаем")
            return

        self.is_processing = True
        try:
            await self._run_single_digest_attempt(digest_type, is_test)
        finally:
            self.is_processing = False

    async def force_execute_digest(self, digest_type: str):
        """
        Принудительное выполнение дайджеста (для ручного запуска)

        Args:
            digest_type: Тип дайджеста ('daily', 'weekly', 'monthly')
        """
        logger.info(f"🔄 Принудительный запуск {digest_type} дайджеста...")
        await self.execute_digest_with_retry(digest_type)

    async def _send_error_notification(self, digest_type: str, error_message: str):
        """Отправка уведомления об ошибке (заглушка для расширения)"""
        # Здесь можно реализовать отправку уведомления в Telegram или другую систему
        logger.error(f"🚨 Критическая ошибка в {digest_type} дайджесте: {error_message}")
