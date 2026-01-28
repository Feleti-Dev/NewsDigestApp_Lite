"""
Менеджер для управления всеми планировщиками системы.
Реализует паттерн Singleton для глобального доступа.
"""
import asyncio
import logging
from asyncio import Task
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from app.parsers import ParserManager
from app.scheduler import SheetsSyncManager
from app.scheduler.continuous_scheduler import ContinuousScheduler
from app.scheduler.digest_scheduler import DigestScheduler
from app.scheduler.single_pass_scheduler import SinglePassScheduler

logger = logging.getLogger(__name__)


class SchedulerManager:
    """
    Единый менеджер для управления всеми планировщиками:
    - ContinuousScheduler (цикличный сборщик)
    - SinglePassScheduler (единичный сборщик)
    - DigestScheduler (планировщик публикаций)
    - Синхронизация с Google Sheets

    Все задачи хранятся в словаре self.tasks для централизованного управления.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            # Экземпляры планировщиков
            self.continuous_scheduler: ContinuousScheduler = None
            self.single_pass_scheduler: SinglePassScheduler = None
            self.digest_scheduler: DigestScheduler = None
            self.parser_manager: ParserManager = None
            self.sync_manager: SheetsSyncManager = None
            # Словарь асинхронных задач {scheduler_type: asyncio.Task}
            self.tasks :  dict[str, Task] = {}

            self._initialized = True
            logger.info("SchedulerManager инициализирован")

    def set_dependencies(self, parser_manager, digest_scheduler,
                         continuous_scheduler=None, single_pass_scheduler=None):
        """
        Установка зависимостей планировщиков.

        Args:
            parser_manager: экземпляр ParserManager
            digest_scheduler: экземпляр DigestScheduler
            continuous_scheduler: экземпляр ContinuousScheduler (опционально)
            single_pass_scheduler: экземпляр SinglePassScheduler (опционально)
        """
        self.parser_manager = parser_manager
        self.digest_scheduler = digest_scheduler

        logger.info("SYNC MANAGER INIT",self.sync_manager)
        # Устанавливаем существующие scheduler-ы если переданы
        if continuous_scheduler is not None:
            self.continuous_scheduler = continuous_scheduler
            logger.info("ContinuousScheduler установлен в SchedulerManager")

        if single_pass_scheduler is not None:
            self.single_pass_scheduler = single_pass_scheduler
            logger.info("SinglePassScheduler установлен в SchedulerManager")

        logger.info("Зависимости SchedulerManager установлены")

    def initialize(self, parser_manager, bypassing_method: str = "NONE"):
        """
        Инициализация менеджера с автоматическим созданием всех планировщиков.

        Args:
            parser_manager: экземпляр ParserManager
            db_manager: экземпляр DatabaseManager
            bypassing_method: режим работы ("LOOP" или "ONCE")
        """
        self.parser_manager = parser_manager

        logger.info(f"📦 Инициализация SchedulerManager с режимом: {bypassing_method}")

        # Создаём DigestScheduler (всегда нужен)
        self.digest_scheduler = self._create_digest_scheduler()
        self.sync_manager = SheetsSyncManager(self.parser_manager.db_manager)

        # Создаём сборщики в зависимости от режима
        if bypassing_method == "LOOP":
            self.continuous_scheduler = self._create_continuous_scheduler()
            logger.info("✅ ContinuousScheduler создан")
        elif bypassing_method == "ONCE":
            self.single_pass_scheduler = self._create_single_pass_scheduler()
            logger.info("✅ SinglePassScheduler создан")
        else:
            logger.warning(f"⚠️ Неизвестный режим работы: {bypassing_method}")

        logger.info("✅ SchedulerManager полностью инициализирован")

    def _create_digest_scheduler(self):
        """Создание DigestScheduler"""
        from app.scheduler.digest_scheduler import DigestScheduler

        schedule_publish = True #(self.continuous_scheduler is not None)  # Включаем если есть цикличный парсер
        scheduler = DigestScheduler(schedule_publish=schedule_publish)
        logger.info("✅ DigestScheduler создан")
        return scheduler

    def _create_continuous_scheduler(self):
        """Создание ContinuousScheduler"""
        from app.scheduler.continuous_scheduler import ContinuousScheduler

        if not self.parser_manager:
            logger.error("❌ ParserManager не инициализирован для создания ContinuousScheduler")
            return None

        scheduler = ContinuousScheduler(self.parser_manager, self.sync_manager)
        return scheduler

    def _create_single_pass_scheduler(self):
        """Создание SinglePassScheduler"""
        from app.scheduler.single_pass_scheduler import SinglePassScheduler

        if not self.parser_manager:
            logger.error("❌ ParserManager не инициализирован для создания SinglePassScheduler")
            return None

        scheduler = SinglePassScheduler(
            self.parser_manager,
            self.sync_manager,
            digest_publisher=self.digest_scheduler
        )
        return scheduler

    async def _run_scheduler(self, scheduler, name: str) -> None:
        """
        Вспомогательный метод для запуска планировщика в асинхронном режиме.

        Args:
            scheduler: экземпляр планировщика
            name: имя планировщика для логов
        """
        try:
            logger.info(f"🔄 Запуск {name}...")
            await scheduler.start()
            logger.info(f"✅ {name} завершил инициализацию или работу ")
        except asyncio.CancelledError:
            await scheduler.stop()
            logger.info(f"🛑 {name} отменён")
            raise
        except Exception as e:
            logger.error(f"❌ Ошибка {name}: {e}")
            raise

    async def start_all(self) -> Dict[str, Any]:
        """
        Запуск всех доступных планировщиков.
        DigestScheduler запускается всегда, остальные - в зависимости от конфигурации.

        Returns:
            Словарь со статусами всех запущенных планировщиков
        """
        results = {}

        # Digest Publisher (всегда запущен)
        if self.digest_scheduler:
            digest_task = asyncio.create_task(
                self._run_scheduler(self.digest_scheduler, "digest_scheduler"),
                name="digest_scheduler"
            )
            self.tasks['digest_publisher'] = digest_task
            logger.info("🔄 Digest scheduler задача создана")

        # Continuous Scheduler (если есть и не в режиме ONCE)
        if self.continuous_scheduler:
            continuous_task = asyncio.create_task(
                self._run_scheduler(self.continuous_scheduler, "continuous_scheduler"),
                name="continuous_scheduler"
            )
            self.tasks['continuous'] = continuous_task
            logger.info("🔄 Continuous scheduler задача создана")

        # Single Pass Scheduler (если есть и в режиме ONCE)
        if self.single_pass_scheduler:
            single_pass_task = asyncio.create_task(
                self._run_scheduler(self.single_pass_scheduler, "single_pass_scheduler"),
                name="single_pass_scheduler"
            )
            self.tasks['single_pass'] = single_pass_task
            logger.info("🔄 Single pass scheduler задача создана")

        logger.info(f"🚀 Запущено {len(self.tasks)} задач планировщиков")

        # Возвращаем статусы
        for scheduler_type in self.tasks:
            results[scheduler_type] = self._get_scheduler_status(scheduler_type)

        return results

    async def stop_all(self) -> None:
        """Остановка всех планировщиков"""
        logger.info(f"🛑 Остановка {len(self.tasks)} задач планировщиков...")

        # Отменяем все задачи
        for scheduler_type, task in list(self.tasks.items()):
            if not task.done():
                logger.info(f"🛑 Отмена задачи {scheduler_type}...")
                task.cancel()
                # Даём время на корректное завершение
                await asyncio.sleep(0.5)


        # # Останавливаем каждый планировщик индивидуально
        # if self.continuous_scheduler:
        #     await self.continuous_scheduler.stop()
        # if self.single_pass_scheduler:
        #     await self.single_pass_scheduler.stop()
        # if self.digest_scheduler:
        #     await self.digest_scheduler.stop()

        # Очищаем словарь задач
        self.tasks.clear()
        logger.info("✅ Все задачи планировщиков остановлены")

    async def restart_digest_scheduler(self) -> Dict[str, Any]:
        """
        Перезапуск DigestScheduler с новыми настройками расписания.

        Returns:
            Словарь со статусом планировщика
        """
        logger.info("🔄 Перезапуск DigestScheduler...")

        # Останавливаем текущий
        if 'digest_publisher' in self.tasks:
            task = self.tasks['digest_publisher']
            if not task.done():
                task.cancel()
            del self.tasks['digest_publisher']

        if self.digest_scheduler:
            await self.digest_scheduler.stop()

        # Создаём новый с актуальными настройками
        self.digest_scheduler = self._create_digest_scheduler()

        # Запускаем
        if self.digest_scheduler:
            digest_task = asyncio.create_task(
                self._run_scheduler(self.digest_scheduler, "digest_scheduler"),
                name="digest_scheduler"
            )
            self.tasks['digest_publisher'] = digest_task
            logger.info("✅ DigestScheduler перезапущен")

        return self._get_scheduler_status('digest_publisher')

    async def start_continuous(self) -> Dict[str, Any]:
        """
        Запуск цикличного сборщика.
        Создаёт задачу через asyncio.create_task и хранит в self.tasks.

        Returns:
            Словарь со статусом планировщика

        Raises:
            ValueError: при конфликте mutual exclusion
        """
        # Проверка mutual exclusion
        is_allowed, error_msg, current = await self._check_mutual_exclusion('continuous', 'start')
        if not is_allowed:
            raise ValueError(error_msg)

        # Создаём если не существует
        if not self.continuous_scheduler:
            from app.scheduler.continuous_scheduler import ContinuousScheduler
            self.continuous_scheduler = ContinuousScheduler(self.parser_manager,self.sync_manager)
            logger.info("✅ ContinuousScheduler создан")

        # Создаём асинхронную задачу и сохраняем в словарь
        task = asyncio.create_task(
            self._run_scheduler(self.continuous_scheduler, "continuous_scheduler"),
            name="continuous_scheduler"
        )
        self.tasks['continuous'] = task

        logger.info("🔄 Continuous scheduler задача создана")

        # Возвращаем статус планировщика
        status = self._get_scheduler_status('continuous')
        logger.info(f"Continuous scheduler запущен: {status}")
        return status

    async def stop_continuous(self) -> Dict[str, Any]:
        """
        Остановка цикличного сборщика.
        Отменяет задачу из self.tasks.

        Returns:
            Словарь со статусом планировщика
        """
        # Отменяем задачу если она существует
        if 'continuous' in self.tasks:
            task = self.tasks['continuous']
            if not task.done():
                logger.info("🛑 Отмена задачи continuous...")
                task.cancel()
            # Не ждём завершения, так как задача из другого event loop
            del self.tasks['continuous']

        # Останавливаем планировщик
        if self.continuous_scheduler:
            await self.continuous_scheduler.stop()

        status = {'type': 'continuous', 'running': False}
        logger.info("Continuous scheduler остановлен")
        return status

    async def start_single_pass(self) -> Dict[str, Any]:
        """
        Запуск единичного сборщика.
        Создаёт задачу через asyncio.create_task и хранит в self.tasks.

        Returns:
            Словарь со статусом планировщика

        Raises:
            ValueError: при конфликте mutual exclusion
        """
        # Проверка mutual exclusion
        is_allowed, error_msg, current = await self._check_mutual_exclusion('single_pass', 'start')
        if not is_allowed:
            raise ValueError(error_msg)

        # Создаём если не существует
        if not self.single_pass_scheduler:
            self.single_pass_scheduler = self._create_single_pass_scheduler()
            logger.info("✅ SinglePassScheduler создан")

        # Создаём асинхронную задачу и сохраняем в словарь
        task = asyncio.create_task(
            self._run_scheduler(self.single_pass_scheduler, "single_pass_scheduler"),
            name="single_pass_scheduler"
        )
        self.tasks['single_pass'] = task

        logger.info("🔄 Single pass scheduler задача создана")

        # Возвращаем статус планировщика
        status = self._get_scheduler_status('single_pass')
        logger.info(f"Single pass scheduler запущен: {status}")
        return status

    async def stop_single_pass(self) -> Dict[str, Any]:
        """
        Остановка единичного сборщика.
        Отменяет задачу из self.tasks.

        Returns:
            Словарь со статусом планировщика
        """
        # # Останавливаем планировщик
        if self.single_pass_scheduler:
            await self.single_pass_scheduler.stop()

        # Отменяем задачу если она существует
        if 'single_pass' in self.tasks:
            task = self.tasks['single_pass']
            if not task.done():
                logger.info("🛑 Отмена задачи single_pass...")
                task.cancel()
            # Не ждём завершения, так как задача из другого event loop
            del self.tasks['single_pass']

        self.single_pass_scheduler = None
        status = {'type': 'single_pass', 'running': False}
        logger.info("Single pass scheduler остановлен")
        return status

    async def start_digest_publisher(self) -> Dict[str, Any]:
        """
        Запуск планировщика публикаций.
        Создаёт задачу через asyncio.create_task и хранит в self.tasks.

        Returns:
            Словарь со статусом планировщика
        """
        # Создаём если не существует
        if not self.digest_scheduler:
            from app.scheduler.digest_scheduler import DigestScheduler
            self.digest_scheduler = DigestScheduler(schedule_publish=True)
            logger.info("✅ DigestScheduler создан")

        # Создаём асинхронную задачу и сохраняем в словарь
        task = asyncio.create_task(
            self._run_scheduler(self.digest_scheduler, "digest_scheduler"),
            name="digest_scheduler"
        )
        self.tasks['digest_publisher'] = task

        logger.info("🔄 Digest publisher задача создана")

        # Возвращаем статус планировщика
        status = self._get_scheduler_status('digest_publisher')
        logger.info(f"Digest publisher запущен: {status}")
        return status

    async def stop_digest_publisher(self) -> Dict[str, Any]:
        """
        Остановка планировщика публикаций.
        Отменяет задачу из self.tasks.

        Returns:
            Словарь со статусом планировщика
        """
        # Отменяем задачу если она существует
        if 'digest_publisher' in self.tasks:
            task = self.tasks['digest_publisher']
            if not task.done():
                logger.info("🛑 Отмена задачи digest_publisher...")
                task.cancel()
            # Не ждём завершения, так как задача из другого event loop
            del self.tasks['digest_publisher']

        # Останавливаем планировщик
        if self.digest_scheduler:
            await self.digest_scheduler.stop()
        status = {'type': 'digest_publisher', 'running': False}
        logger.info("Digest publisher остановлен")
        return status

    async def start_sheets_sync(self) -> Dict[str, Any]:
        """
        Запуск синхронизации с Google Sheets.

        Returns:
            Словарь со статусом
        """
        self.sync_manager.sync_channels(force=True)
        status = self._get_scheduler_status('sheets_sync')
        logger.info("Sheets sync запущен")
        return status

    def get_all_statuses(self) -> Dict[str, Any]:
        """
        Получение статуса всех планировщиков.

        Returns:
            Словарь со статусами всех планировщиков
        """
        return {
            'continuous': self._get_scheduler_status('continuous'),
            'single_pass': self._get_scheduler_status('single_pass'),
            'digest_publisher': self._get_scheduler_status('digest_publisher'),
            'sheets_sync': self._get_scheduler_status('sheets_sync')
        }

    async def is_news_parsing_running(self) -> bool:
        """
        Проверка, запущен ли парсинг новостей (continuous или single_pass).

        Returns:
            True если хотя бы один из них запущен
        """
        mutual_status = self.get_mutual_exclusion_status()
        return mutual_status.get('active_scheduler') is not None

    async def _check_mutual_exclusion(self, scheduler_type: str, action: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Проверка mutual exclusion.
        Правило: может быть запущен только один из (continuous, single_pass).

        Args:
            scheduler_type: тип планировщика
            action: действие (start/stop)

        Returns:
            (is_allowed: bool, error_message: str or None, current_running: str or None)
        """
        # Mutual exclusion проверяется только при запуске
        if action != "start":
            return True, None, None

        # Определяем группу исключения
        exclusive_group = ["continuous", "single_pass"]

        if scheduler_type not in exclusive_group:
            return True, None, None

        # Проверяем какой scheduler уже запущен в этой группе
        current_running = self.get_mutual_exclusion_status()["active_scheduler"]

        # Если уже запущен другой scheduler из группы - возвращаем ошибку
        if current_running and current_running != scheduler_type:

            error_msg = (
                f"Нельзя запустить {self._get_scheduler_name(scheduler_type)}: "
                f"{self._get_scheduler_name(current_running)} уже запущен. "
                f"Остановите его сначала."
            )
            return False, error_msg, current_running

        return True, None, None

    def get_mutual_exclusion_status(self) -> Dict[str, Any]:
        """
        Получение статуса mutual exclusion.

        Returns:
            Словарь со статусом взаимного исключения
        """
        current_running = None

        # Проверяем по словарю задач
        if 'continuous' in self.tasks and not self.tasks['continuous'].done():
            current_running = "continuous"
        elif 'single_pass' in self.tasks and not self.tasks['single_pass'].done():
            current_running = "single_pass"
        # Также проверяем по атрибуту is_running планировщика
        elif (self.continuous_scheduler and
              hasattr(self.continuous_scheduler, 'is_running') and
              self.continuous_scheduler.is_running):
            current_running = "continuous"
        elif (self.single_pass_scheduler and
              hasattr(self.single_pass_scheduler, 'is_running') and
              self.single_pass_scheduler.is_running):
            current_running = "single_pass"

        return {
            "exclusive_group": ["continuous", "single_pass"],
            "description": "Может быть активен только один из: цикличный или единичный сборщик",
            "active_scheduler": current_running
        }

    def _get_scheduler_status(self, scheduler_type: str) -> Dict[str, Any]:
        """
        Получение статуса конкретного планировщика.

        Args:
            scheduler_type: тип планировщика

        Returns:
            Словарь со статусом
        """
        scheduler_map = {
            'continuous': self.continuous_scheduler,
            'single_pass': self.single_pass_scheduler,
            'digest_publisher': self.digest_scheduler,
            'sheets_sync': self.sync_manager
        }

        scheduler = scheduler_map.get(scheduler_type)
        if scheduler_type == "single_pass" or scheduler_type == "continuous":
            # Проверяем запущена ли задача в словаре
            task_running = False
            if scheduler_type in self.tasks:
                task = self.tasks[scheduler_type]
                task_running = not task.done()

            if not scheduler and not task_running:
                return {
                    'type': scheduler_type,
                    'running': False,
                    'uptime': None,
                    'start_time': None,
                    'name': self._get_scheduler_name(scheduler_type),
                    'description': self._get_scheduler_description(scheduler_type)
                }
        else:
            task_running = True if scheduler_map.get(scheduler_type) is not None else False
        # Определяем запущен ли scheduler
        is_running = task_running
        start_time = None

        if scheduler and hasattr(scheduler, 'is_running'):
            is_running = scheduler.is_running

        if scheduler and hasattr(scheduler, 'start_time') and scheduler.start_time:
            start_time = scheduler.start_time

        # Расчёт uptime
        uptime = None
        if start_time and is_running:
            uptime = self._calculate_uptime(start_time)

        return {
            'type': scheduler_type,
            'running': is_running,
            'uptime': uptime,
            'start_time': start_time.isoformat() if start_time else None,
            'name': self._get_scheduler_name(scheduler_type),
            'description': self._get_scheduler_description(scheduler_type)
        }

    def _calculate_uptime(self, start_time) -> str:
        """
        Расчёт времени работы в формате HH:MM:SS.

        Args:
            start_time: время запуска

        Returns:
            Строка с временем работы
        """
        if not start_time:
            return None

        if isinstance(start_time, str):
            start_time = datetime.fromisoformat(start_time)

        delta = datetime.now() - start_time
        total_seconds = int(delta.total_seconds())

        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def _get_scheduler_name(self, scheduler_type: str) -> str:
        """
        Получение названия планировщика.

        Args:
            scheduler_type: тип планировщика

        Returns:
            Название планировщика
        """
        names = {
            'continuous': 'Цикличный сборщик',
            'single_pass': 'Единичный сборщик',
            'digest_publisher': 'Планировщик публикаций',
            'sheets_sync': 'Синхронизация с Google Sheets'
        }
        return names.get(scheduler_type, scheduler_type)

    def _get_scheduler_description(self, scheduler_type: str) -> str:
        """
        Получение описания планировщика.

        Args:
            scheduler_type: тип планировщика

        Returns:
            Описание планировщика
        """
        descriptions = {
            'continuous': 'Непрерывный сбор новостей по всем активным каналам',
            'single_pass': 'Однократный проход по всем активным каналам',
            'digest_publisher': 'Автоматическая публикация дайджестов по расписанию',
            'sheets_sync': 'Синхронизация списка каналов из Google Sheets'
        }
        return descriptions.get(scheduler_type, '')
