"""
Менеджер для перезапуска приложения.
Реализует паттерн Singleton для глобального доступа.
"""

import asyncio
import gc
import logging
import threading
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class RestartManager:
    """
    Менеджер для перезапуска приложения.

    Функционал:
    - Полный перезапуск приложения через создание нового экземпляра
    - Мягкий перезапуск (только перезагрузка конфигурации)
    - Фоновый перезапуск без блокировки API
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self.app_instance = None
            self._restart_requested = False
            self._shutdown_requested = False
            self._initialized = True
            logger.info("RestartManager инициализирован")

    def set_app_instance(self, app):
        """
        Установка текущего экземпляра приложения.

        Args:
            app: экземпляр NewsDigestApplication
        """
        self.app_instance = app
        logger.info("Ссылка на приложение установлена")

    @property
    def restart_requested(self) -> bool:
        """
        Проверка, запрошен ли перезапуск.

        Returns:
            True если перезапуск запрошен
        """
        return self._restart_requested

    @property
    def shutdown_requested(self) -> bool:
        """
        Проверка, запрошено ли завершение работы.

        Returns:
            True если завершение работы запрошено
        """
        return self._shutdown_requested

    def clear_restart_flag(self):
        """Сброс флага перезапуска"""
        self._restart_requested = False
        logger.info("Флаг перезапуска сброшен")

    async def _stop_current_app(self):
        """
        Остановка текущего приложения перед перезапуском.
        """
        if self.app_instance and hasattr(self.app_instance, "is_running"):
            if self.app_instance.is_running:
                logger.info("🛑 Остановка текущего приложения...")
                await self.app_instance.stop()

    async def _cleanup_resources(self):
        """
        Очистка ресурсов перед перезапуском.
        """
        # Принудительная сборка мусора
        gc.collect()

        # Небольшая пауза перед перезапуском
        await asyncio.sleep(1)

        logger.info("Ресурсы очищены")

    async def _create_new_app_instance(self) -> bool:
        """
        Создание нового экземпляра приложения.
        Это аналогично коду в async def main().

        Returns:
            True если успешно
        """

        try:
            logger.info("🚀 Создание нового экземпляра приложения...")

            # Создаём новый экземпляр приложения
            new_app = NewsDigestApplication()

            # Инициализация компонентов
            await new_app.initialize()

            # Сохраняем ссылку
            self.app_instance = new_app

            # Запускаем в фоновом режиме
            async def run_app():
                await new_app.start()

            asyncio.create_task(run_app())

            logger.info("✅ Новое приложение запущено")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при создании нового приложения: {e}")
            import traceback

            logger.error(f"Трассировка:\n{traceback.format_exc()}")
            return False

    async def restart_full(self) -> dict[str, Any]:
        """
        Полный перезапуск приложения.
        Перезапускает приложение путём создания нового экземпляра.

        Returns:
            Словарь с результатом
        """
        logger.info("🔄 Инициирован полный перезапуск системы...")

        try:
            # Остановка текущего приложения
            await self._stop_current_app()

            # Очистка ресурсов
            await self._cleanup_resources()

            # Создание нового приложения
            success = await self._create_new_app_instance()

            if success:
                return {
                    "success": True,
                    "message": "Приложение успешно перезапущено",
                    "timestamp": datetime.now().isoformat(),
                }
            else:
                return {
                    "success": False,
                    "message": "Ошибка при перезапуске приложения",
                    "timestamp": datetime.now().isoformat(),
                }

        except Exception as e:
            logger.error(f"❌ Критическая ошибка при перезапуске: {e}")
            return {
                "success": False,
                "message": f"Критическая ошибка: {e!s}",
                "timestamp": datetime.now().isoformat(),
            }

    def restart_from_api(self) -> dict[str, Any]:
        """
        Инициация перезапуска из API.
        Запускает перезапуск в отдельном потоке и сразу возвращает ответ.

        Returns:
            Словарь с результатом (всегда success=true, так как перезапуск в фоне)
        """
        logger.info("📡 Запрос перезапуска через API")

        # Устанавливаем флаг перезапуска
        self._restart_requested = True

        def do_restart():
            asyncio.run(self.restart_full())

        # Запуск в отдельном потоке для возврата ответа клиенту
        thread = threading.Thread(target=do_restart, daemon=True)
        thread.start()

        return {
            "success": True,
            "message": "Система перезапускается...",
            "timestamp": datetime.now().isoformat(),
        }

    def shutdown_from_api(self) -> dict[str, Any]:
        """
        Инициация завершения работы из API.
        Устанавливает флаг завершения и возвращает ответ.

        Returns:
            Словарь с результатом
        """
        logger.info("📡 Запрос завершения работы через API")

        # Устанавливаем флаг завершения работы
        self._shutdown_requested = True

        return {
            "success": True,
            "message": "Система завершает работу...",
            "timestamp": datetime.now().isoformat(),
        }

    async def restart_soft(self) -> dict[str, Any]:
        """
        Мягкий перезапуск.
        Перезагружает только конфигурацию без остановки компонентов.

        Returns:
            Словарь с результатом
        """
        logger.info("🔄 Мягкий перезапуск (перезагрузка конфигурации)...")

        # Устанавливаем флаг перезапуска
        self._restart_requested = True

        try:
            # Перезагрузка конфигурации
            from app.configs import config, llm_prompts

            # Перезагрузка config
            # (пересоздаём объекты конфигурации)
            config.app = None
            config.api = None
            config.google_sheets = None
            config.scheduler = None

            # Перезагрузка промптов
            llm_prompts.load_prompts()

            logger.info("✅ Конфигурация перезагружена")

            return {
                "success": True,
                "message": "Конфигурация перезагружена",
                "timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"❌ Ошибка мягкого перезапуска: {e}")
            return {
                "success": False,
                "message": f"Ошибка: {e!s}",
                "timestamp": datetime.now().isoformat(),
            }


class NewsDigestApplication:
    """
    Основной класс приложения с интеграцией RestartManager.
    """

    def __init__(self):
        self.parser_manager = None
        self.continuous_scheduler = None
        self.single_pass_scheduler = None
        self.digest_scheduler = None
        self.telegram_publisher = None
        self.flask_app = None
        self.flask_thread = None
        self.is_running = False

        # Регистрация в RestartManager
        restart_manager.set_app_instance(self)

    async def initialize(self):
        """Инициализация всех компонентов приложения"""
        from app.configs.config import config
        from app.parsers.parser_manager import ParserManager
        from app.scheduler.continuous_scheduler import ContinuousScheduler
        from app.scheduler.digest_scheduler import DigestScheduler
        from app.scheduler.scheduler_manager import SchedulerManager
        from app.scheduler.single_pass_scheduler import SinglePassScheduler
        from app.web import create_app

        logger.info("🚀 Инициализация News Digest Application...")

        # 1. Инициализация менеджера парсеров
        self.parser_manager = ParserManager()

        # 2. Инициализация планировщиков в зависимости от режима
        if config.app.bypassing_method == "LOOP":
            self.continuous_scheduler = ContinuousScheduler(self.parser_manager)
            self.digest_scheduler = DigestScheduler(schedule_publish=True)
        elif config.app.bypassing_method == "ONCE":
            self.single_pass_scheduler = SinglePassScheduler(
                self.parser_manager, digest_publisher=self.digest_scheduler
            )
            self.digest_scheduler = DigestScheduler(schedule_publish=False)

        # 3. Инициализация SchedulerManager
        scheduler_manager = SchedulerManager()
        scheduler_manager.set_dependencies(self.parser_manager, self.digest_scheduler)

        # 4. Инициализация Flask приложения
        self.flask_app = create_app(self.digest_scheduler, self.parser_manager, scheduler_manager)

        logger.info("✅ Все компоненты приложения инициализированы")

    async def start(self):
        """Запуск всех компонентов приложения"""
        import asyncio
        import threading

        from app.configs.config import config

        if self.is_running:
            logger.warning("Приложение уже запущено")
            return

        logger.info("🚀 Запуск всех компонентов приложения...")
        self.is_running = True

        # Настройка обработчиков сигналов
        self._setup_signal_handlers()

        try:
            # Запускаем Flask в отдельном потоке
            self.flask_thread = threading.Thread(target=self.run_flask, daemon=True)
            self.flask_thread.start()

            # Создаём задачи для всех планировщиков
            tasks = [
                # Digest Scheduler (периодические дайджесты)
                asyncio.create_task(
                    self._run_scheduler(self.digest_scheduler, "digest_scheduler"),
                    name="digest_scheduler",
                )
            ]

            if config.app.bypassing_method == "LOOP":
                tasks.append(
                    asyncio.create_task(
                        self._run_scheduler(self.continuous_scheduler, "continuous_scheduler"),
                        name="continuous_scheduler",
                    ),
                )
            elif config.app.bypassing_method == "ONCE":
                tasks.append(
                    asyncio.create_task(
                        self._run_scheduler(self.single_pass_scheduler, "single_pass_scheduler"),
                        name="single_pass_scheduler",
                    ),
                )
                # Ожидаем завершения всех задач
                await asyncio.gather(*tasks, return_exceptions=True)

        except KeyboardInterrupt:
            logger.info("🛑 Получен сигнал прерывания (Ctrl+C)")
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в главном цикле: {e}")
        finally:
            await self.stop()

    def run_flask(self):
        """Запуск Flask в отдельном потоке"""
        from app.configs.config import config

        logger.info(f"🌐 Запуск веб-интерфейса на порту {config.app.web_port}...")
        self.flask_app.run(
            host=config.app.web_host,
            port=config.app.web_port,
            debug=config.app.debug,
            use_reloader=False,
        )

    async def _run_scheduler(self, scheduler, name):
        """Запуск Scheduler"""
        try:
            logger.info(f"🔄 Запуск {name}...")
            await scheduler.start()
            logger.info(f"✅ {name} завершил работу")
        except Exception as e:
            logger.error(f"❌ Ошибка {name}: {e}")
            raise

    async def stop(self):
        """Корректная остановка приложения"""
        if not self.is_running:
            return

        logger.info("🛑 Остановка приложения...")
        self.is_running = False

        # Останавливаем планировщики
        if self.continuous_scheduler:
            await self.continuous_scheduler.stop()

        if self.single_pass_scheduler:
            await self.single_pass_scheduler.stop()

        if self.digest_scheduler:
            await self.digest_scheduler.stop()

        # Закрываем менеджер парсеров
        if self.parser_manager:
            await self.parser_manager.close_parsers()

        logger.info("✅ Приложение остановлено корректно")

    def _setup_signal_handlers(self):
        """Настройка обработчиков сигналов ОС"""
        import signal

        def signal_handler(signum, frame):
            logger.info(f"📡 Получен сигнал {signum}")
            asyncio.create_task(self.stop())

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)


# Глобальный экземпляр RestartManager
restart_manager = RestartManager()
