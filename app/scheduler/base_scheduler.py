"""
Базовый класс для планировщиков.
Содержит общую логику: инициализацию, обработку каналов, статистику, синхронизацию.
Конкретные планировщики настраиваются через параметры cycle и digest_publisher.
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from app.configs.config import config
from app.database.models import ChannelSource
from app.parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class BaseScheduler:
    """
    Базовый класс для всех планировщиков.

    Общий функционал:
    - Инициализация и загрузка каналов из БД
    - Унифицированная логика выбора каналов (циклический/последовательный)
    - Обработка одного канала
    - Статистика и статус для фронтенда
    - Синхронизация с Google Sheets
    - Обработка сигналов завершения
    - Опциональная публикация дайджестов

    Наследники настраивают:
    - cycle: True для бесконечного цикла (Continuous), False для однократного (SinglePass)
    - digest_publisher: опциональный объект для публикации дайджестов
    """

    def __init__(
        self,
        parser_manager,
        sync_manager,
        scheduler_type: str = "base",
        cycle: bool = False,
        digest_publisher=None
    ):
        """
        Инициализация базового планировщика.

        Args:
            parser_manager: менеджер парсеров
            scheduler_type: тип планировщика для статистики (continuous / single_pass)
            cycle: True - циклический режим (Continuous), False - однократный (SinglePass)
            digest_publisher: объект для публикации дайджестов (опционально)
        """
        self.parser_manager = parser_manager
        self.scheduler_type = scheduler_type
        self.cycle = cycle
        self.digest_publisher = digest_publisher

        # Менеджер для работы с каналами в БД
        self.db_manager = parser_manager.db_manager

        # Менеджер синхронизации с Google Sheets
        self.sync_manager = sync_manager

        # Храним каналы в памяти для быстрого доступа
        # {source_type: [ChannelSource, ...]}
        self._channels_by_source: Dict[str, list[ChannelSource]] = {}

        # Индексы каналов для всех источников
        self._channel_indices: Dict[str, int] = {}

        self.tasks: Dict[str, asyncio.Task] = {}
        self.is_running = False
        self.start_time: Optional[datetime] = None
        self.loop = None

    async def initialize(self):
        """Инициализация планировщика"""
        logger.info(f"[Scheduler]🚀 Инициализация {self.scheduler_type.upper()} Scheduler...")

        # Загружаем каналы из БД (синхронизация с Google Sheets при необходимости)
        logger.info("[Scheduler]🔄 Загрузка каналов из БД...")
        self._channels_by_source = self.sync_manager.sync_channels(force=True)

        # Инициализируем индексы каналов
        for source_type in self._channels_by_source:
            self._channel_indices[source_type] = 0

        # Логируем количество каналов по источникам
        for source_type, channels in self._channels_by_source.items():
            logger.info(f"[Scheduler]  {source_type}: {len(channels)} каналов")

        # Проверяем состояние парсеров
        logger.info("[Scheduler]🔍 Проверка состояния парсеров...")
        parser_status = self.parser_manager.get_parsers_status()["api_status"]
        if parser_status:
            for source_type, status in parser_status.items():
                logger.info(f"[Scheduler]  {source_type}: {status}")

        logger.info("[Scheduler]✅ Инициализация завершена")

    def _get_next_channel(self, source_type: str) -> Optional[ChannelSource]:
        """
        Получение следующего канала для обработки.

        Unified logic for both cyclic and sequential modes:
        - cycle=True: Returns next channel, wraps to start when end reached
        - cycle=False: Returns next channel, returns None when all channels processed

        Args:
            source_type: тип источника

        Returns:
            Следующий канал или None (если cycle=False и каналы закончились)
        """
        channels = self._channels_by_source.get(source_type, [])
        if not channels:
            return None

        # Фильтруем только активные каналы
        active_channels = [ch for ch in channels if ch.is_active]
        if not active_channels:
            logger.warning(f"[Scheduler]Нет активных каналов для {source_type}")
            return None

        current_index = self._channel_indices.get(source_type, 0)

        if current_index >= len(active_channels):
            if self.cycle:
            # Циклический режим: переходим на начало при достижении конца
                current_index = 0
                self._channel_indices[source_type] = 0
            else:
            # Последовательный режим: возвращаем None когда каналы закончились
                logger.debug(f"[Scheduler]Все каналы для {source_type} обработаны")
                return None

        next_channel = active_channels[current_index]
        self._channel_indices[source_type] = current_index + 1

        logger.debug(
            f"Следующий канал для {source_type}: {next_channel.channel_id} "
            f"(индекс {current_index}/{len(active_channels)}, однократный)"
        )

        return next_channel

    def _check_finished(self) -> bool:
        """
        Проверка завершения работы планировщика.

        Base implementation:
        - cycle=True: Never finishes automatically (relies on is_running flag)
        - cycle=False: Finishes when all channels are processed

        Returns:
            True если планировщик должен завершиться
        """
        if self.cycle:
            # В циклическом режиме полагаемся на is_running
            return False

        # В однократном режиме проверяем все ли источники обработаны
        active_sources = [
            source_type
            for source_type, is_active in config.app.parser_status.items()
            if is_active and source_type in self._channels_by_source
        ]
        # logger.info(f"[Scheduler]Active_sources: {active_sources}, indexes: {self._channel_indices}")

        for source_type in active_sources:
            channels = self._channels_by_source.get(source_type, [])
            active_channels = [ch for ch in channels if ch.is_active]
            current_index = self._channel_indices.get(source_type, 0)
            # logger.info(f"[Scheduler]source_type: {source_type} current_index: {current_index}. active_channels: {len(active_channels)}")
            if current_index < len(active_channels):
                return False  # Есть необработанные каналы
        return True  # Все каналы обработаны

    async def _process_single_channel(self, source_type: str, channel: ChannelSource) -> int:
        """
        Обработка одного канала.
        Единая логика для всех типов планировщиков.

        Args:
            source_type: тип источника
            channel: объект канала из БД

        Returns:
            Количество собранных новостей
        """
        try:
            logger.info(f"[Scheduler]🔍 Обработка канала {source_type}: {channel.channel_id}")

            # Обновляем время последней обработки
            channel.last_processed = datetime.now()

            # Получаем парсер для этого типа источника
            parser: BaseParser = self.parser_manager.parsers.get(source_type)
            if not parser:
                logger.error(f"[Scheduler]Парсер для {source_type} не найден")
                self.db_manager.update_channel_result(
                    channel.channel_id, source_type, success=False
                )
                return 0

            # Обрабатываем канал
            news_items = await parser.process_channel(channel.url, channel.channel_id)
            logger.info(f"[Scheduler]📊 Предварительный результат: {len(news_items)} новостей")

            if not news_items:
                # Успешно, но новостей нет
                self.db_manager.update_channel_result(
                    channel.channel_id,
                    source_type,
                    success=True,
                    news_count=0
                )
                logger.info(f"[Scheduler]📭 {channel.channel_id}: новостей не найдено")
                return 0

            # Фильтрация по порогу полезности
            logger.info(f"[Scheduler]🔍 Фильтрация новостей по порогу полезности {config.app.interest_threshold}...")
            filtered_news = await parser.filter_by_interest_threshold(news_items)

            # Сохранение в базу данных
            logger.debug(f"[Scheduler]Сохранение {len(filtered_news)} новостей в БД")
            saved_count = parser.save_to_database(filtered_news)

            # Расчет средней оценки
            avg_score = 0.0
            if filtered_news:
                scores = [
                    n.get('Interest_score', 0)
                    for n in filtered_news
                    if n.get('Interest_score')
                ]
                avg_score = sum(scores) / len(scores) if scores else 0.0

            # Обновляем состояние канала в БД
            self.db_manager.update_channel_result(
                channel.channel_id,
                source_type,
                success=True,
                news_count=saved_count,
                avg_score=avg_score
            )

            logger.info(f"[Scheduler]✅ Канал {channel.channel_id}: собрано {saved_count} новостей, средний балл {avg_score:.4f}")
            return saved_count

        except Exception as e:
            logger.error(f"[Scheduler]❌ Ошибка обработки канала {channel.channel_id}: {e}")
            self.db_manager.update_channel_result(
                channel.channel_id, source_type, success=False
            )
            return 0

    def _check_parser_needs_restart(self, source_type: str) -> bool:
        """
        Проверка, требуется ли перезапуск парсера.

        Args:
            source_type: тип источника

        Returns:
            True если парсер требует перезапуска
        """
        channels = self._channels_by_source.get(source_type, [])
        if not channels:
            return False

        # Считаем недавние ошибки
        recent_failures = sum(1 for channel in channels if channel.failure_count >= 3)

        # Если более половины каналов имеют ошибки, парсер проблемный
        return recent_failures > len(channels) / 2

    def get_status(self) -> Dict[str, Any]:
        """
        Получение текущего статуса планировщика для фронтенда.

        Returns:
            Словарь со статусом и статистикой
        """
        # Получаем список всех каналов с деталями
        all_channels = self.db_manager.get_channels_list()

        # Группируем каналы по источникам
        channels_by_source: Dict[str, list[Dict]] = {}
        for ch in all_channels:
            st = ch['source_type']
            if st not in channels_by_source:
                channels_by_source[st] = []
            channels_by_source[st].append(ch)

        # Формируем статистику по источникам
        stats: Dict[str, Any] = {}
        for src_type, channels in channels_by_source.items():
            active_channels = [ch for ch in channels if ch['is_active']]
            success = sum(ch['success_count'] for ch in channels)
            failure = sum(ch['failure_count'] for ch in channels)
            total_ops = success + failure

            stats[src_type] = {
                'total_channels': len(channels),
                'active_channels': len(active_channels),
                'inactive_channels': len(channels) - len(active_channels),
                'success_rate': round((success / total_ops * 100) if total_ops > 0 else 0, 1),
                'total_success': success,
                'total_failure': failure,
                'total_news_collected': sum(ch['news_collected'] for ch in channels),
                'channels': channels,
            }

        # Общая статистика
        stats["overall"] = {
            "total_sources": len(stats),
            "scheduler_type": self.scheduler_type,
            "cycle_mode": self.cycle,
            "uptime": (
                str(datetime.now() - self.start_time)
                if self.start_time
                else None
            ),
        }

        # Формируем активные задачи
        active_tasks = []
        for task_name, task in self.tasks.items():
            active_tasks.append({
                "name": task_name,
                "running": not task.done(),
                "cancelled": task.cancelled(),
            })

        # Полный статус
        status = {
            "scheduler_type": self.scheduler_type,
            "is_running": self.is_running,
            "is_finished": self._check_finished(),
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "uptime": (
                str(datetime.now() - self.start_time) if self.start_time else None
            ),
            "tasks_running": len([t for t in self.tasks.values() if not t.done()]),
            "statistics": stats,
            "active_tasks": active_tasks,
        }

        return status

    async def stop(self):
        """Остановка планировщика"""
        logger.info(f"[Scheduler]🛑 Остановка {self.scheduler_type.upper()} Scheduler...")
        self.is_running = False

        # Закрываем парсеры
        # await self.parser_manager.close_parsers()

        # 1. Сначала отменяем все задачи
        for task_name, task in self.tasks.items():
            if not task.done():
                logger.info(f"[Scheduler]🛑 Отмена задачи {task_name}...")
                task.cancel()
        self.tasks = {}
        logger.info(f"[Scheduler]✅ {self.scheduler_type.upper()} Scheduler остановлен")


    def _start_source_task(self, source_type: str):
        """
        Создание и запуск задачи обработки источника.
        Единая логика для всех планировщиков.

        Args:
            source_type: тип источника
        """

        task = asyncio.create_task(self._process_source_task(source_type), name=f"{source_type}")
        self.tasks[source_type] = task
        logger.info(f"[Scheduler]✅ Задача для {source_type},{self.is_running} запущена: {task}")

    async def _process_source_task(self, source_type: str):
        """
        Основная задача обработки каналов одного источника.

        Unified loop that works for both cyclic and sequential modes:
        - cycle=True: loops forever until stop() called
        - cycle=False: processes all channels once and exits

        Args:
            source_type: тип источника
        """
        logger.info(f"[Scheduler]📋 Запуск обработки источника {source_type}")

        if not self.is_running:
            logger.warning(f"[Scheduler]Планировщик не запущен, задача {source_type} пропущена")
            return

        while True:
            try:
                # Проверяем завершение (для однократного режима)
                if not self.cycle and self._check_finished():
                    logger.info(f"[Scheduler]Все каналы {source_type} обработаны")
                    break

                # Проверяем, не требуется ли перезапуск парсера
                if self._check_parser_needs_restart(source_type):
                    logger.warning(
                        f"Парсер {source_type} требует внимания. Пропускаем."
                    )
                    await asyncio.sleep(60)
                    continue

                # Получаем следующий канал
                channel = self._get_next_channel(source_type)

                if not channel:
                    # Каналы закончились (для однократного режима)
                    logger.info(f"[Scheduler]Все каналы {source_type} обработаны")
                    break

                # Обрабатываем канал
                await self._process_single_channel(source_type, channel)

                # Ждем указанный интервал между каналами
                interval = config.scheduler.get_interval(source_type)
                logger.debug(
                    f"Ждем {interval} секунд до следующего канала {source_type}"
                )
                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                logger.info(f"[Scheduler]Задача {source_type} отменена")
                break
            except Exception as e:
                logger.error(f"[Scheduler]Ошибка при обработке {source_type}: {e}")
                await asyncio.sleep(60)
                break

    async def _sync_loop(self):
        """Цикл синхронизации с Google Sheets"""
        logger.info("[Scheduler]🔄 Запуск цикла синхронизации")

        while self.is_running:
            try:
                # Синхронизируем раз в сутки
                await asyncio.sleep(24 * 3600)  # 24 часа

                if self.is_running:
                    logger.info("[Scheduler]🔄 Плановое обновление каналов из Google Sheets...")
                    channels_by_source = self.sync_manager.sync_channels()
                    self._channels_by_source = channels_by_source

                    # Сбрасываем индексы для новых списков каналов
                    for source_type in channels_by_source:
                        self._channel_indices[source_type] = 0

                    logger.info("[Scheduler]✅ Синхронизация завершена")

            except asyncio.CancelledError:
                logger.info("[Scheduler]Цикл синхронизации отменен")
                break
            except Exception as e:
                logger.error(f"[Scheduler]Ошибка синхронизации: {e}")
                await asyncio.sleep(3600)  # Ждем час при ошибке

    async def _monitoring_loop(self):
        """Цикл мониторинга и статистики"""
        logger.info("[Scheduler]📊 Запуск цикла мониторинга")

        while self.is_running:
            try:
                # Ждем 5 минут между обновлениями статистики
                await asyncio.sleep(300)

                if self.is_running:
                    status = self.get_status()

                    # Логируем общую статистику
                    for source_type, source_stats in status["statistics"].items():
                        if source_type != "overall":
                            total_news = sum(
                                ch.get('news_collected', 0)
                                for ch in source_stats.get('channels', [])
                            )
                            logger.info(
                                f"📊 {source_type}: "
                                f"Активных: {source_stats['active_channels']}/"
                                f"{source_stats['total_channels']}, "
                                f"Успешно: {source_stats['success_rate']:.1f}%, "
                                f"Новостей: {total_news}"
                            )

            except asyncio.CancelledError:
                logger.info("[Scheduler]Цикл мониторинга отменен")
                break
            except Exception as e:
                logger.error(f"[Scheduler]Ошибка мониторинга: {e}")

    async def restart_source(self, source_type: str):
        """Перезапуск парсера источника"""
        logger.info(f"[Scheduler]🔄 Перезапуск парсера {source_type}...")

        # Сбрасываем индекс каналов
        self._channel_indices[source_type] = 0

        # Сбрасываем статистику каналов в БД
        channels = self._channels_by_source.get(source_type, [])
        for channel in channels:
            channel.failure_count = 0
            channel.is_active = True

        # Перезапускаем задачу
        if source_type in self.tasks and not self.tasks[source_type].done():
            self.tasks[source_type].cancel()

        # Создаем новую задачу
        self._start_source_task(source_type)

        logger.info(f"[Scheduler]✅ Парсер {source_type} перезапущен")

    async def force_sync(self):
        """Принудительная синхронизация с Google Sheets"""
        logger.info("[Scheduler]🔄 Принудительная синхронизация...")
        channels_by_source = self.sync_manager.sync_channels(force=True)
        self._channels_by_source = channels_by_source

        # Сбрасываем индексы
        for source_type in channels_by_source:
            self._channel_indices[source_type] = 0

        logger.info("[Scheduler]✅ Принудительная синхронизация завершена")

    async def _on_all_sources_finished(self):
        """
        Hook called when all sources are finished (for single-pass mode).
        Override in subclasses to add custom behavior (e.g., digest publishing).
        """
        logger.info("[Scheduler]📋 Все источники обработаны")
        pass

    async def _execute_digest_with_retry(self, digest_type: str, is_test: bool = False):
        """
        Выполнение дайджеста с повторными попытками при ошибках.
        Делегирует выполнение digest_publisher если он предоставлен.

        Args:
            digest_type: Тип дайджеста ('daily', 'weekly', 'monthly')
            is_test: Флаг тестового режима
        """
        if not self.digest_publisher:
            logger.debug("Digest publisher не настроен, пропуск публикации дайджеста")
            return

        await self.digest_publisher.execute_digest_with_retry(digest_type, is_test)

    async def start(self):
        """
        Запуск планировщика.
        Единая реализация для всех типов планировщиков.
        """
        self.tasks = {}
        self._channel_indices={}

        self.loop = asyncio.get_running_loop()

        if self.is_running:
            logger.warning("[Scheduler]Планировщик уже запущен")
            return

        logger.info(f"[Scheduler]🚀 Запуск {self.scheduler_type.upper()} Scheduler...")
        self.is_running = True
        self.start_time = datetime.now()

        # if self.parser_manager.parsers == {}:
        self.parser_manager.create_parsers()

        try:
            # Инициализация
            await self.initialize()

            # Получаем активные источники
            source_types = [
                source_type
                for source_type, is_active in config.app.parser_status.items()
                if is_active
            ]

            # Запускаем задачи для всех источников
            for source_type in source_types:
                self._start_source_task(source_type)

            logger.info(f"[Scheduler]✅ {self.scheduler_type.upper()} Scheduler: запущено {len(source_types)} источников")

            # Для циклического режима запускаем доп. задачи
            if self.cycle:
                # Запускаем задачу синхронизации
                sync_task = asyncio.create_task(self._sync_loop())
                self.tasks["sync"] = sync_task

                # Запускаем задачу мониторинга
                monitor_task = asyncio.create_task(self._monitoring_loop())
                self.tasks["monitor"] = monitor_task

            # Ожидаем завершения
            while self.is_running:
                # Проверяем завершение для однократного режима
                if not self.cycle and self._check_finished():
                    logger.info("[Scheduler]завершение для однократного режима")
                    await self._on_all_sources_finished()
                    break

                await asyncio.sleep(1)

            logger.info(f"[Scheduler]✅ {self.scheduler_type.upper()} Scheduler завершил работу")

        except KeyboardInterrupt:
            logger.info("[Scheduler]Получен сигнал прерывания")
        except Exception as e:
            logger.error(f"[Scheduler]❌ Критическая ошибка в планировщике: {e}")
        finally:
            await self.stop()
