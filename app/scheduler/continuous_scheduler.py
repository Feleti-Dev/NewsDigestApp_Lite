"""
Непрерывный планировщик для циклического обхода каналов.
Наследует общую логику от BaseScheduler.
Отличительная особенность: cycle=True для бесконечного цикла.
"""
import logging

from app.scheduler.base_scheduler import BaseScheduler

logger = logging.getLogger(__name__)


class ContinuousScheduler(BaseScheduler):
    """
    Непрерывный планировщик для циклического обхода каналов.

    Особенности:
    - cycle=True: работает в бесконечном цикле
    - Использует общую реализацию из BaseScheduler
    - Работает пока не получен сигнал завершения
    """

    def __init__(self, parser_manager, sync_manager):
        """
        Инициализация непрерывного планировщика.

        Args:
            parser_manager: менеджер парсеров
        """
        super().__init__(
            parser_manager,
            sync_manager,
            scheduler_type="continuous",
            cycle=True
        )
