import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from app.configs.config import config
from sqlalchemy import and_, desc, func, not_, text

from .models import ChannelSource, NewsItem, SessionLocal

logger = logging.getLogger(__name__)




class DatabaseManager:
    """Менеджер для работы с базой данных"""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self.session = SessionLocal()
            self._initialized = True

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    def close(self):
        """Закрытие сессии"""
        if self.session:
            self.session.close()

    # ===== CRUD для NewsItem =====

    def add_news(self, news_data: Dict[str, Any]) -> Optional[NewsItem]:
        """Добавление новости в базу данных"""
        try:
            # Проверяем дубликат по URL
            existing = (
                self.session.query(NewsItem)
                .filter(NewsItem.News_URL == news_data["News_URL"])
                .first()
            )

            if existing:
                logger.debug(f"Дубликат новости пропущен: {news_data['News_URL']}")
                return existing

            logger.debug(
                f"Добавление новой новости: {news_data.get('Headline', '')[:50]}..."
            )

            # ВАЖНО: Обработка даты - гарантируем, что это naive datetime
            pub_date = news_data.get("Publication_date")
            logger.debug(
                f"Получена дата для сохранения: {pub_date}, тип: {type(pub_date)}"
            )

            if isinstance(pub_date, datetime):
                # Убеждаемся, что это naive datetime (без timezone)
                if pub_date.tzinfo is not None:
                    # Конвертируем в UTC и удаляем timezone
                    pub_date = pub_date.astimezone().replace(tzinfo=None)
                    logger.debug(f"Преобразована дата с timezone в naive: {pub_date}")
                # Убираем микросекунды для SQLite
                pub_date = pub_date.replace(microsecond=0)
            elif isinstance(pub_date, str):
                # Пробуем преобразовать строку в datetime
                try:
                    # Убираем микросекунды если есть
                    if "." in pub_date:
                        pub_date = pub_date.split(".")[0]

                    # Пробуем разные форматы
                    formats = [
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%d %H:%M:%S.%f",
                        "%Y-%m-%dT%H:%M:%S.%f",
                        "%d.%m.%Y %H:%M:%S",
                        "%d/%m/%Y %H:%M:%S",
                    ]

                    for fmt in formats:
                        try:
                            pub_date = datetime.strptime(pub_date, fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        raise ValueError(f"Не удалось распарсить дату: {pub_date}")

                    # Убираем микросекунды
                    pub_date = pub_date.replace(microsecond=0)
                except Exception as e:
                    logger.warning(
                        f"Не удалось распарсить дату: {pub_date}, ошибка: {e}"
                    )
                    pub_date = datetime.now().replace(microsecond=0)
            else:
                logger.warning(
                    f"Неправильный тип даты: {type(pub_date)}, использую текущее время"
                )
                pub_date = datetime.now().replace(microsecond=0)

            logger.debug(f"Финальная дата для сохранения: {pub_date}")

            # Создаем объект новости
            news_item = NewsItem(
                Source=news_data.get("Source"),
                News_URL=news_data.get("News_URL"),
                Headline=(news_data.get("Headline", "") or "")[:500],
                News_text=news_data.get("News_text", ""),
                Publication_date=pub_date,  # Теперь это точно naive datetime
                Has_image=news_data.get("Has_image", False),
                Image_URL=news_data.get("Image_URL"),
                Interest_score=news_data.get("Interest_score"),
                Daily_used=news_data.get("Daily_used", False),
                Weekly_used=news_data.get("Weekly_used", False),
                Monthly_used=news_data.get("Monthly_used", False),
                Publication_error=news_data.get("Publication_error", False),
                Note=news_data.get("Note", ""),
            )

            self.session.add(news_item)
            self.session.commit()
            logger.info(
                f"✅ Новость добавлена: {news_item.ID} - {news_item.Headline[:50]}..."
            )
            return news_item

        except Exception as e:
            self.session.rollback()
            logger.error(f"❌ Ошибка при добавлении новости: {e}")
            import traceback

            logger.error(f"Трассировка: {traceback.format_exc()}")

            # Детальный лог данных, вызвавших ошибку
            logger.error(f"Данные новости: {news_data}")
            return None

    def get_news_by_url(self, url: str) -> Optional[NewsItem]:
        """Получение новости по URL"""
        return self.session.query(NewsItem).filter(NewsItem.News_URL == url).first()

    def get_news_for_digest(self, digest_type: str, limit: int = 7) -> List[NewsItem]:
        """
        Получение новостей для дайджеста

        Args:
            digest_type: 'daily', 'weekly', 'monthly'
            limit: количество новостей (по умолчанию 7)

        Returns:
            Список новостей, отсортированных по оценке полезности
        """
        # Определяем флаг использования
        flag_map = {
            "daily": NewsItem.Daily_used,
            "weekly": NewsItem.Weekly_used,
            "monthly": NewsItem.Monthly_used,
        }

        used_flag = flag_map.get(digest_type)
        if not used_flag:
            raise ValueError(f"Неверный тип дайджеста: {digest_type}")

        # Определяем временной диапазон
        now = datetime.now()
        if digest_type == "daily":
            time_filter = NewsItem.Publication_date >= (now - timedelta(days=1))
        elif digest_type == "weekly":
            time_filter = NewsItem.Publication_date >= (now - timedelta(days=7))
        elif digest_type == "monthly":
            time_filter = NewsItem.Publication_date >= (now - timedelta(days=30))
        else:
            time_filter = True

        try:
            # Получаем новости: не использованные, с оценкой выше порога, за период
            news_items = (
                self.session.query(NewsItem)
                .filter(
                    and_(
                        time_filter,
                        NewsItem.Interest_score >= config.app.interest_threshold,  # Порог полезности
                        not_(used_flag),  # Еще не использованы
                        not_(NewsItem.Publication_error),  # Без ошибок публикации
                    )
                )
                .order_by(desc(NewsItem.Interest_score))
                .limit(limit)
                .all()
            )

            logger.info(
                f"Найдено {len(news_items)} новостей для {digest_type} дайджеста"
            )
            return news_items

        except Exception as e:
            logger.error(f"Ошибка при получении новостей для дайджеста: {e}")
            return []

    def mark_as_used(self, news_ids: List[int], digest_type: str = 'daily') -> int:
        """
        Пометка новостей как использованные в дайджесте

        Args:
            news_ids: список ID новостей
            digest_type: тип дайджеста ('daily', 'weekly', 'monthly')

        Returns:
            Количество обновленных записей
        """
        try:
            if not news_ids:
                return 0

            # Определяем поле для обновления
            if digest_type == 'daily':
                field = NewsItem.Daily_used
            elif digest_type == 'weekly':
                field = NewsItem.Weekly_used
            elif digest_type == 'monthly':
                field = NewsItem.Monthly_used
            else:
                logger.error(f"Неизвестный тип дайджеста: {digest_type}")
                return 0

            # Обновляем записи
            updated_count = self.session.query(NewsItem).filter(
                NewsItem.ID.in_(news_ids)
            ).update(
                {field: True},
                synchronize_session=False
            )

            self.session.commit()
            logger.info(f"Помечено {updated_count} новостей как использованные в {digest_type} дайджесте")
            return updated_count

        except Exception as e:
            self.session.rollback()
            logger.error(f"Ошибка пометки новостей как использованные: {e}")
            return 0

    def get_news_paginated(
            self,
            page: int = 1,
            per_page: int = 20,
            sort_by: str = "date",
            sort_order: str = "desc",
            exclude_sources: Optional[List[str]] = None,
            exclude_source_types: Optional[List[str]] = None,
    ):
        query = self.session.query(NewsItem)

        # Исключаем источники
        if exclude_sources:
            query = query.filter(not_(NewsItem.Source.in_(exclude_sources)))

        # Исключаем типы источников (telegram / youtube / twitter)
        if exclude_source_types:
            for st in exclude_source_types:
                query = query.filter(not_(NewsItem.Source.ilike(f"%{st}%")))

        # Сортировка
        if sort_by == "date":
            order_col = NewsItem.Publication_date
        elif sort_by == "source":
            order_col = NewsItem.Source
        elif sort_by == "score":
            order_col = NewsItem.Interest_score
        else:
            order_col = NewsItem.Publication_date

        if sort_order == "desc":
            order_col = desc(order_col)

        total = query.count()

        items = (
            query
            .order_by(order_col)
            .offset((page - 1) * per_page)
            .limit(per_page)
            .all()
        )

        return total, items

    def delete_news(self, news_id: int) -> bool:
        try:
            news = self.session.query(NewsItem).filter(NewsItem.ID == news_id).first()
            if not news:
                return False

            self.session.delete(news)
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            logger.error(f"Ошибка удаления новости {news_id}: {e}")
            return False

    # ===== Статистика и аналитика =====
    def is_connected(self) -> bool:
        """Проверка подключения к БД"""
        try:
            if not self.session:
                return False

            # Простая проверка - пытаемся выполнить простой запрос
            result = self.session.execute(text("SELECT 1")).scalar()
            return result == 1
        except Exception as e:
            logger.error(f"Ошибка проверки подключения к БД: {e}")
            return False

    def update_channel_stats(self, channel_id: str, source_type: str, success: bool, 
                           news_count: int = 0, avg_score: float = 0.0):
        """Обновление статистики канала"""
        try:
            # Ищем канал по channel_id и source_type
            channel = self.session.query(ChannelSource).filter(
                ChannelSource.channel_id == channel_id,
                ChannelSource.source_type == source_type
            ).first()
            
            if not channel:
                # Если не нашли по channel_id, пробуем найти по другим критериям
                logger.warning(f"Канал {channel_id} не найден в БД для обновления статистики")
                return
            
            if success:
                channel.success_count += 1
                channel.news_collected += news_count
                
                # Обновляем средний балл
                if news_count > 0 and avg_score > 0:
                    total_news_before = channel.news_collected - news_count
                    if total_news_before > 0:
                        total_score_before = channel.avg_interest_score * total_news_before
                        total_score_now = total_score_before + (avg_score * news_count)
                        channel.avg_interest_score = total_score_now / channel.news_collected
                    else:
                        channel.avg_interest_score = avg_score
            else:
                channel.failure_count += 1
            
            channel.last_processed = datetime.now()
            self.session.commit()
            logger.debug(f"Статистика канала {channel_id} обновлена")
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Ошибка обновления статистики канала: {e}")

    def get_channel_statistics(self) -> Dict[str, Any]:
        """Получение статистики по каналам"""
        try:
            from sqlalchemy import func

            # Общая статистика по каналам
            total_channels = self.session.query(func.count(ChannelSource.id)).scalar() or 0
            active_channels = self.session.query(func.count(ChannelSource.id)).filter(
                ChannelSource.is_active == True
            ).scalar() or 0

            # Суммарная статистика по новостям в каналах
            total_news_in_channels = self.session.query(
                func.sum(ChannelSource.news_collected)
            ).scalar() or 0

            # Средний балл по каналам (только у тех, где есть новости)
            avg_score_channels = self.session.query(
                func.avg(ChannelSource.avg_interest_score)
            ).filter(ChannelSource.news_collected > 0).scalar() or 0.0

            if avg_score_channels:
                avg_score_channels = round(float(avg_score_channels), 4)

            # Статистика по источникам
            source_stats = {}
            sources = self.session.query(
                ChannelSource.source_type,
                func.count(ChannelSource.id).label('count'),
                func.sum(ChannelSource.news_collected).label('total_news'),
                func.avg(ChannelSource.avg_interest_score).label('avg_score')
            ).group_by(ChannelSource.source_type).all()

            for source_type, count, total_news, avg_score in sources:
                source_stats[source_type] = {
                    'count': count or 0,
                    'total_news': total_news or 0,
                    'avg_score': round(float(avg_score or 0), 4)
                }

            return {
                'total_channels': total_channels,
                'active_channels': active_channels,
                'inactive_channels': total_channels - active_channels,
                'total_news_in_channels': total_news_in_channels,
                'avg_channel_score': avg_score_channels,
                'source_stats': source_stats,
                'migration_date': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Ошибка получения статистики по каналам: {e}")
            return {}

    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики по базе данных (объединенная версия)"""
        try:
            if not self.is_connected():
                return {
                    "total_news": 0,
                    "news_with_score": 0,
                    "source_stats": {},
                    "daily_used": 0,
                    "weekly_used": 0,
                    "monthly_used": 0,
                    "total_channels": 0,
                    "active_channels": 0,
                    "database_path": "Не подключено",
                    "news_last_24h": 0,
                    "news_last_7d": 0,
                    "news_last_30d": 0,
                    "average_score": 0.0,
                    "timestamp": datetime.now().isoformat()
                }

            # Основная статистика (старый get_stats)
            total_news = self.session.query(func.count(NewsItem.ID)).scalar() or 0
            news_with_score = (
                    self.session.query(func.count(NewsItem.ID))
                    .filter(NewsItem.Interest_score >= 0.1500)
                    .scalar() or 0
            )

            # Статистика по источникам
            source_stats = {}
            try:
                sources = (
                    self.session.query(NewsItem.Source, func.count(NewsItem.ID))
                    .group_by(NewsItem.Source)
                    .all()
                )
                for source, count in sources:
                    source_stats[source] = count
            except:
                pass

            # Использованные новости
            daily_used = (
                    self.session.query(func.count(NewsItem.ID))
                    .filter(NewsItem.Daily_used == True)
                    .scalar() or 0
            )
            weekly_used = (
                    self.session.query(func.count(NewsItem.ID))
                    .filter(NewsItem.Weekly_used == True)
                    .scalar() or 0
            )
            monthly_used = (
                    self.session.query(func.count(NewsItem.ID))
                    .filter(NewsItem.Monthly_used == True)
                    .scalar() or 0
            )

            # Каналы
            total_channels = self.session.query(func.count(ChannelSource.id)).scalar() or 0
            active_channels = (
                    self.session.query(func.count(ChannelSource.id))
                    .filter(ChannelSource.is_active == True)
                    .scalar() or 0
            )

            # Статистика по периодам (из get_news_statistics)
            now = datetime.utcnow()
            news_last_24h = (
                    self.session.query(func.count(NewsItem.ID))
                    .filter(NewsItem.Publication_date >= now - timedelta(hours=24))
                    .scalar() or 0
            )
            news_last_7d = (
                    self.session.query(func.count(NewsItem.ID))
                    .filter(NewsItem.Publication_date >= now - timedelta(days=7))
                    .scalar() or 0
            )
            news_last_30d = (
                    self.session.query(func.count(NewsItem.ID))
                    .filter(NewsItem.Publication_date >= now - timedelta(days=30))
                    .scalar() or 0
            )

            # Средняя оценка
            avg_score_result = (
                self.session.query(func.avg(NewsItem.Interest_score))
                .filter(NewsItem.Interest_score.isnot(None))
                .scalar()
            )
            average_score = round(float(avg_score_result), 4) if avg_score_result else 0.0

            return {
                "total_news": total_news,
                "news_with_score": news_with_score,
                "source_stats": source_stats,
                "daily_used": daily_used,
                "weekly_used": weekly_used,
                "monthly_used": monthly_used,
                "total_channels": total_channels,
                "active_channels": active_channels,
                "database_path": str(self.session.bind.url) if self.session.bind else None,
                # Новые поля из get_news_statistics
                "news_last_24h": news_last_24h,
                "news_last_7d": news_last_7d,
                "news_last_30d": news_last_30d,
                "average_score": average_score,
                "timestamp": now.isoformat()
            }

        except Exception as e:
            logger.error(f"Ошибка при получении статистики: {e}")
            return {}

    def get_parser_statistics(self) -> List[Dict[str, Any]]:
        """
        Получение статистики по парсерам (источникам) для отображения в админке.

        Returns:
            Список статистики по каждому типу источника
        """
        try:
            result = []

            for source_type in ['telegram', 'twitter', 'youtube', 'reddit']:
                # Количество каналов
                total_channels = self.session.query(func.count(ChannelSource.id)).filter(
                    ChannelSource.source_type == source_type
                ).scalar() or 0

                active_channels = self.session.query(func.count(ChannelSource.id)).filter(
                    ChannelSource.source_type == source_type,
                    ChannelSource.is_active == True
                ).scalar() or 0

                # Сумма собранных новостей
                news_collected = self.session.query(
                    func.sum(ChannelSource.news_collected)
                ).filter(
                    ChannelSource.source_type == source_type
                ).scalar() or 0

                # Сумма успешных и неудачных сборов для расчёта success_rate
                total_success = self.session.query(
                    func.sum(ChannelSource.success_count)
                ).filter(
                    ChannelSource.source_type == source_type
                ).scalar() or 0

                total_failures = self.session.query(
                    func.sum(ChannelSource.failure_count)
                ).filter(
                    ChannelSource.source_type == source_type
                ).scalar() or 0

                total_attempts = total_success + total_failures
                success_rate = (total_success / total_attempts * 100) if total_attempts > 0 else 0.0

                result.append({
                    'type': source_type.capitalize(),
                    'channels': total_channels,
                    'active_channels': active_channels,
                    'success_rate': round(success_rate, 1),
                    'news_collected': news_collected
                })

            return result

        except Exception as e:
            logger.error(f"Ошибка получения статистики парсеров: {e}")
            return []

    def get_recent_news(self, hours: int = 24, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Получение недавних новостей за указанный период.

        Args:
            hours: количество часов для фильтрации (по умолчанию 24)
            limit: максимальное количество новостей (по умолчанию 20)

        Returns:
            Список словарей с данными новостей
        """
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)

            news_items = self.session.query(NewsItem).filter(
                NewsItem.Publication_date >= cutoff_time
            ).order_by(
                desc(NewsItem.Interest_score)
            ).limit(limit).all()

            news_data = []
            for item in news_items:
                news_data.append({
                    'id': item.ID,
                    'source': item.Source,
                    'headline': item.Headline,
                    'url': item.News_URL,
                    'publication_date': item.Publication_date.isoformat() if item.Publication_date else None,
                    'interest_score': float(item.Interest_score) if item.Interest_score else 0,
                    'has_image': item.Has_image,
                    'daily_used': item.Daily_used,
                    'weekly_used': item.Weekly_used,
                    'monthly_used': item.Monthly_used
                })

            return news_data

        except Exception as e:
            logger.error(f"Ошибка получения недавних новостей: {e}")
            return []

    def get_channels_list(self, source_type: str = None) -> List[Dict[str, Any]]:
        """
        Получение списка каналов с деталями для фронтенда.

        Args:
            source_type: фильтр по типу источника (None = все)

        Returns:
            Список словарей с данными каналов
        """
        try:
            query = self.session.query(ChannelSource)

            if source_type:
                query = query.filter(ChannelSource.source_type == source_type)

            channels = query.all()

            result = []
            for ch in channels:
                result.append({
                    'url': ch.url,
                    'channel_id': ch.channel_id,
                    'source_type': ch.source_type,
                    'is_active': ch.is_active,
                    'success_count': ch.success_count,
                    'failure_count': ch.failure_count,
                    'news_collected': ch.news_collected,
                    'avg_interest_score': ch.avg_interest_score,
                    'last_processed': (
                        ch.last_processed.isoformat()
                        if ch.last_processed
                        else None
                    ),
                    'sheet_name': ch.sheet_name,
                })

            return result

        except Exception as e:
            logger.error(f"Ошибка получения списка каналов: {e}")
            return []


    # ===== Операции с каналами (замена StateManager) =====

    def get_all_active_channels(self) -> Dict[str, List[ChannelSource]]:
        """
        Получение всех активных каналов, сгруппированных по типу источника.

        Returns:
            Словарь {source_type: [ChannelSource, ...]}
        """
        channels = self.session.query(ChannelSource).filter(
            ChannelSource.is_active == True
        ).all()

        result = {}
        for channel in channels:
            if channel.source_type not in result:
                result[channel.source_type] = []
            result[channel.source_type].append(channel)

        total = sum(len(ch_list) for ch_list in result.values())
        logger.info(f"Загружено {total} активных каналов из БД")
        return result

    def get_channels_by_source(self, source_type: str) -> List[ChannelSource]:
        """
        Получение каналов конкретного источника.

        Args:
            source_type: тип источника (telegram, twitter, youtube, reddit)

        Returns:
            Список активных каналов
        """
        return self.session.query(ChannelSource).filter(
            and_(
                ChannelSource.source_type == source_type,
                ChannelSource.is_active == True
            )
        ).all()

    def update_channel_result(
            self,
            channel_id: str,
            source_type: str,
            success: bool,
            news_count: int = 0,
            avg_score: float = 0.0
    ) -> bool:
        """
        Обновление результата обработки канала.
        Аналог record_channel_result из StateManager.

        Args:
            channel_id: идентификатор канала
            source_type: тип источника
            success: успешная ли обработка
            news_count: количество собранных новостей
            avg_score: средний балл интереса

        Returns:
            True если канал обновлён
        """
        try:
            channel = self.session.query(ChannelSource).filter(
                and_(
                    ChannelSource.channel_id == channel_id,
                    ChannelSource.source_type == source_type
                )
            ).first()

            if not channel:
                logger.warning(f"Канал не найден: {source_type}/{channel_id}")
                return False

            channel.last_processed = datetime.now()

            if success:
                channel.success_count += 1
                channel.news_collected += news_count

                if news_count > 0 and avg_score > 0:
                    total_score = channel.avg_interest_score * (channel.news_collected - news_count)
                    total_score += avg_score * news_count
                    channel.avg_interest_score = total_score / channel.news_collected
            else:
                channel.failure_count += 1
                if channel.failure_count >= 3:
                    channel.is_active = False
                    logger.warning(f"Канал {channel_id} деактивирован: 3 ошибки")

            self.session.commit()
            return True

        except Exception as e:
            self.session.rollback()
            logger.error(f"Ошибка обновления канала {channel_id}: {e}")
            return False

    def sync_channels_from_sheets(self, sheets_data: Dict[str, List[Dict]]) -> int:
        """
        Синхронизация каналов из Google Sheets в БД.
        Аналог _save_channels_to_db из SheetsSyncManager.

        Args:
            sheets_data: словарь {source_type: [{url, channel_id, sheet_name, row_number}]}

        Returns:
            Количество созданных каналов
        """
        try:
            existing_channels = {}
            for ch in self.session.query(ChannelSource).all():
                key = f"{ch.source_type}:{ch.url}"
                existing_channels[key] = ch

            created_count = 0
            current_keys = set()

            for source_type, raw_channels in sheets_data.items():
                for raw in raw_channels:
                    key = f"{source_type}:{raw['url']}"
                    current_keys.add(key)

                    if key in existing_channels:
                        ch = existing_channels[key]
                        ch.channel_id = raw.get('channel_id')
                        ch.sheet_name = raw.get('sheet_name', 'unknown')
                        ch.row_number = raw.get('row_number', 0)
                        ch.last_checked = datetime.now()
                        ch.is_active = True
                    else:
                        new_channel = ChannelSource(
                            source_type=source_type,
                            url=raw['url'],
                            channel_id=raw.get('channel_id'),
                            sheet_name=raw.get('sheet_name', 'unknown'),
                            row_number=raw.get('row_number', 0),
                            is_active=True,
                            last_checked=datetime.now(),
                            news_collected=0,
                            success_count=0,
                            failure_count=0,
                            avg_interest_score=0.0
                        )
                        self.session.add(new_channel)
                        created_count += 1

            for key, ch in existing_channels.items():
                if key not in current_keys:
                    ch.is_active = False

            self.session.commit()
            logger.info(f"Синхронизация: создано {created_count} каналов")
            return created_count

        except Exception as e:
            self.session.rollback()
            logger.error(f"Ошибка синхронизации: {e}")
            return 0

