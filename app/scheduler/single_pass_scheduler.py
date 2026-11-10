"""
Планировщик для единичного прохода по всем каналам.
Наследует общую логику от BaseScheduler.
Отличительная особенность: cycle=False, автоматическая публикация дайджеста.
"""

import logging
from typing import Any

from app.configs import config
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

    def __init__(self, parser_manager, sync_manager, digest_publisher: Any | None = None):
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
            digest_publisher=digest_publisher,
        )

    async def _on_all_sources_finished(self):
        """
        Hook: публикация дайджеста после завершения всех источников.
        """
        logger.info("📝 Публикация дайджеста после завершения сбора...")

        count = max(1, config.app.once_digest_count)
        if self.digest_publisher and hasattr(self.digest_publisher, "execute_digest_batch"):
            await self.digest_publisher.execute_digest_batch("daily", count)
        else:
            await self._execute_digest_with_retry(digest_type="daily")

        logger.info(
            "Все источники обработаны, опубликовано до %s daily-дайджест(ов)",
            count,
        )
