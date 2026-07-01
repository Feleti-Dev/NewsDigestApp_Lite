# app/digest/creator.py
"""
Создатель дайджестов: логика отбора и обработки новостей
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from app.database.db_utils import DatabaseManager
from .formatter import HTMLDigestFormatter  # Изменено на HTML форматтер
from app.configs.config import config
from app.ml.llm_client import get_llm_client


logger = logging.getLogger(__name__)


class DigestCreator:
    """Основной класс для создания дайджестов"""

    def __init__(self):
        self.db_manager = DatabaseManager()
        self.formatter = HTMLDigestFormatter()  # Используем HTML форматтер

    async def create_digest(self, digest_type: str, is_test: bool = False) -> Optional[Dict[str, Any]]:
        """
        Создание дайджеста указанного типа

        Args:
            digest_type: 'daily', 'weekly' или 'monthly'
            is_test: Флаг тестового режима

        Returns:
            Словарь с данными дайджеста или None
        """
        logger.info(f"🛠️  Создание {digest_type} дайджеста...")
        llm_client = await get_llm_client()
        try:
            # 1. Получение топ новостей из БД
            news_items = await self._get_top_news(digest_type, is_test)
            if not news_items:
                logger.warning(f"📭 Нет подходящих новостей для {digest_type} дайджеста")
                return None

            logger.info(f"📊 Получено {len(news_items)} новостей для дайджеста")

            # 2. Обработка новостей (перевод, сжатие, форматирование)

            # Используем LLM API для комплексной обработки

            result = await llm_client.process_digest_news(news_items, digest_type)
            digest_text = result.get("digest_text", "")

            if not digest_text:
                logger.warning("⚠️  LLM не вернула текст дайджеста")
                return None

            best_image = self._select_best_image(news_items)

            digest_data = {
                'type': digest_type,
                'text': digest_text,
                'image_url': best_image,
                'news_items': news_items,  # Оригинальные новости
                'news_count': len(news_items),
                'created_at': datetime.now().isoformat(),
                'is_test': is_test,
                'processing_method': 'LLM_API'
            }

            logger.info(
                f"✅ {digest_type.capitalize()} дайджест создан с помощью LLM API ({len(news_items)} новостей)")

            return digest_data

        except Exception as e:
            logger.error(f"❌ Ошибка создания дайджеста: {e}")
            return None

    async def _get_top_news(self, digest_type: str, is_test: bool = False) -> List[Dict[str, Any]]:
        """
        Получение топ новостей из базы данных

        Args:
            digest_type: Тип дайджеста
            is_test: Игнорировать флаги использования в тестовом режиме

        Returns:
            Список новостей
        """
        try:
            # Получаем неиспользованные новости за период
            news_objects = self.db_manager.get_news_for_digest(
                digest_type=digest_type,
                limit=config.app.max_news_per_digest
            )


            # Преобразуем объекты SQLAlchemy в словари
            news_items = []
            for news in news_objects:
                news_dict = {
                    'id': news.ID,
                    'source': news.Source,
                    'title': news.Headline,
                    'text': news.News_text,
                    'url': news.News_URL,
                    'image_url': news.Image_URL,
                    'publication_date': news.Publication_date.isoformat() if news.Publication_date else None,
                    'interest_score': float(news.Interest_score) if news.Interest_score else 0.0,
                    'has_image': bool(news.Has_image)
                }
                news_items.append(news_dict)
            
            return news_items
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения новостей из БД: {e}")
            return []
    

    def _select_best_image(self, news_items: List[Dict[str, Any]]) -> Optional[str]:
        """
        Выбор лучшего изображения для дайджеста
        
        Args:
            news_items: Список новостей
            
        Returns:
            URL лучшего изображения или None
        """
        try:
            # Фильтруем новости с изображениями
            news_with_images = [
                news for news in news_items
                if news.get('image_url') and 
                news['image_url'].startswith('http') and
                not news['image_url'].endswith(('.mp4', '.avi', '.mov'))  # Исключаем видео
            ]
            
            if not news_with_images:
                return None
            
            # Выбираем изображение из новости с наибольшей оценкой
            best_news = max(news_with_images, key=lambda x: x.get('interest_score', 0))
            
            # Проверяем, что это изображение (не видео ютуба)
            if 'youtube.com' in best_news['image_url'] or 'youtu.be' in best_news['image_url']:
                # Для YouTube берем превью
                if 'maxresdefault' in best_news['image_url']:
                    return best_news['image_url']
                else:
                    # Пробуем найти другие изображения
                    for news in news_with_images:
                        if 'youtube.com' not in news['image_url'] and 'youtu.be' not in news['image_url']:
                            return news['image_url']
                    return None
            
            return best_news['image_url']
            
        except Exception as e:
            logger.error(f"Ошибка выбора изображения: {e}")
            return None