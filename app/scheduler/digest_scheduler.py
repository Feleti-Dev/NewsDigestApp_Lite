"""
Планировщик задач для формирования и отправки дайджестов
Использует APScheduler для периодического выполнения задач
"""
import asyncio
import logging
import traceback
from datetime import datetime
from typing import Optional

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
        self.scheduler: Optional[AsyncIOScheduler] = None
        self.digest_creator = DigestCreator()
        self.telegram_publisher = TelegramPublisher()
        self.db_manager = DatabaseManager()
        self.moscow_tz = pytz.timezone('Europe/Moscow')
        self.schedule_publish = schedule_publish

        # Настройки повторных попыток
        self.max_retries = 3
        self.retry_delay = 300  # 5 минут

        # Флаг для предотвращения параллельного выполнения
        self.is_processing = False

        # Флаги для статистики
        self.start_time = None
        self.is_running = False

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
                trigger=CronTrigger(hour=config.scheduler.daily_digest["hour"],
                                    minute=config.scheduler.daily_digest["minute"]),
                #  trigger="interval",
                #  minutes=2,
                id='daily_digest',
                name=f'Ежедневный дайджест в {config.scheduler.daily_digest["hour"]:02d}:{config.scheduler.daily_digest["minute"]:02d} МСК',
                replace_existing=True,
                misfire_grace_time=30,
                max_instances=1
            )
        if config.scheduler.weekly_digest["enabled"]:
            # 2. ЕЖЕНЕДЕЛЬНЫЙ дайджест
            self.scheduler.add_job(
                self._execute_weekly_digest,
                trigger=CronTrigger(day_of_week=config.scheduler.weekly_digest["day_of_week"],
                                    hour=config.scheduler.weekly_digest["hour"],
                                    minute=config.scheduler.weekly_digest["minute"]),
                id='weekly_digest',
                name=f'Еженедельный дайджест ({config.scheduler.weekly_digest["day_of_week"]}, {config.scheduler.weekly_digest["hour"]:02d}:{config.scheduler.weekly_digest["minute"]:02d} МСК)',
                replace_existing=True,
                misfire_grace_time=30,
                max_instances=1
            )
        if config.scheduler.monthly_digest["enabled"]:
            # 3. ЕЖЕМЕСЯЧНЫЙ дайджест
            self.scheduler.add_job(
                self._execute_monthly_digest,
                trigger=CronTrigger(day=config.scheduler.monthly_digest["day"],
                                    hour=config.scheduler.monthly_digest["hour"],
                                    minute=config.scheduler.monthly_digest["minute"]),
                id='monthly_digest',
                name=f'Ежемесячный дайджест ({config.scheduler.monthly_digest["day"]} число, {config.scheduler.monthly_digest["hour"]:02d}:{config.scheduler.monthly_digest["minute"]:02d} МСК)',
                replace_existing=True,
                misfire_grace_time=30,
                max_instances=1
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
            next_run = job.next_run_time.astimezone(self.moscow_tz) if job.next_run_time else "Не запланировано"
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
        await self.execute_digest_with_retry('daily')

    async def _execute_weekly_digest(self):
        """Выполнение еженедельного дайджеста"""
        await self.execute_digest_with_retry('weekly')

    async def _execute_monthly_digest(self):
        """Выполнение ежемесячного дайджеста"""
        await self.execute_digest_with_retry('monthly')

    async def _execute_test_digest(self):
        """Тестовый дайджест для отладки"""
        logger.info("🧪 Выполнение тестового дайджеста...")
        await self.execute_digest_with_retry('daily', is_test=True)

    async def execute_digest_with_retry(self, digest_type: str, is_test: bool = False):
        """
        Выполнение дайджеста с повторными попытками при ошибках
        
        Args:
            digest_type: Тип дайджеста ('daily', 'weekly', 'monthly')
            is_test: Флаг тестового режима
        """
        # Проверка на параллельное выполнение
        if self.is_processing:
            logger.warning(f"⚠️  Дайджест {digest_type} уже выполняется, пропускаем")
            return

        self.is_processing = True
        operation_name = f"тестовый {digest_type}" if is_test else digest_type

        for attempt in range(self.max_retries):
            try:
                logger.info(f"🔄 Попытка {attempt + 1}/{self.max_retries}: {operation_name} дайджест")

                # 1. Формирование дайджеста
                digest_data = await self.digest_creator.create_digest(
                    digest_type=digest_type,
                    is_test=is_test
                )

                if not digest_data or not digest_data.get('news_items'):
                    logger.warning(f"📭 Нет данных для {operation_name} дайджеста")
                    self.is_processing = False
                    return

                # 2. Отправка в Telegram
                success = await self.telegram_publisher.publish_digest(digest_data)

                if success:
                    # 3. Обновление флагов в БД (только для не тестовых)
                    news_ids = [item['id'] for item in digest_data['news_items']]
                    logger.info(f"news_ids: {news_ids}")
                    updated = self.db_manager.mark_as_used(news_ids, digest_type)
                    logger.info(f"✅ Помечено {updated} новостей как использованные в {digest_type} дайджесте")

                logger.info(f"✅ {operation_name.capitalize()} дайджест успешно обработан")
                self.is_processing = False
                return

            except Exception as e:
                logger.error(f"❌ Ошибка при выполнении {operation_name} дайджеста: {e}")

                if attempt < self.max_retries - 1:
                    logger.info(f"⏳ Повторная попытка через {self.retry_delay} секунд...")
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(f"🚫 Не удалось выполнить {operation_name} дайджест после {self.max_retries} попыток")

                    # Логирование полной трассировки для последней ошибки
                    logger.error(f"Трассировка ошибки:\n{traceback.format_exc()}")

                    # Можно добавить отправку уведомления об ошибке
                    # await self._send_error_notification(digest_type, str(e))

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
