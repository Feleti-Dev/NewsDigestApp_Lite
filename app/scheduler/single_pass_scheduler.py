"""
Планировщик для единичного прохода по всем каналам.
Наследует общую логику от BaseScheduler.
Отличительная особенность: cycle=False, автоматическая публикация дайджеста.
"""

import logging
from typing import Optional, Any

from app.scheduler.base_scheduler import BaseScheduler

logger = logging.getLogger(__name__)


class SinglePassScheduler(BaseScheduler):
    """
    Планировщик для единичного прохода по всем каналам.

    Особенности:
    - cycle=False: обрабатывает все каналы один раз
    - Принимает digest_publisher для публикации дайджеста
    - Автоматически публикует daily дайджест после завершения
    """

    def __init__(
        self,
        parser_manager,
        sync_manager,
        digest_publisher: Optional[Any] = None
    ):
        """
        Инициализация планировщика единичного прохода.

        Args:
            parser_manager: менеджер парсеров
            digest_publisher: объект с методом execute_digest_with_retry (опционально)
        """
        super().__init__(
            parser_manager,
            sync_manager,
            scheduler_type="single_pass",
            cycle=False,
            digest_publisher=digest_publisher
        )

    async def _on_all_sources_finished(self):
        """
        Hook: публикация дайджеста после завершения всех источников.
        """
        logger.info("📝 Публикация дайджеста после завершения сбора...")

        # Публикуем daily дайджест через digest_publisher
        await self._execute_digest_with_retry(digest_type='daily')

        logger.info("📋 Все источники обработаны, дайджест опубликован")