import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

import tweepy

from .base_parser import BaseParser

logger = logging.getLogger(__name__)


class TwitterParser(BaseParser):
    """Парсер для Twitter/X с использованием официального API v2"""

    def __init__(self):
        super().__init__("Twitter")
        self.client = None
        self.api_available = self._init_api()
        self._current_media_dict = {}  # Для хранения медиа текущего запроса

    def _init_api(self) -> bool:
        """Инициализация Twitter API v2 клиента"""
        from app.configs import config

        bearer_token = config.api.twitter_bearer_token

        if not bearer_token:
            logger.warning(
                "Twitter Bearer Token не указан. Twitter парсер будет отключен."
            )
            return False

        try:
            self.client = tweepy.Client(
                bearer_token=bearer_token, wait_on_rate_limit=False
            )

            logger.info("✅ Twitter API v2 клиент создан с Bearer Token")
            return True
        except Exception as e:
            logger.error(f"Ошибка инициализации Twitter API: {e}")
            return False

    async def fetch_channel_news(
        self, channel_url: str, channel_id: str
    ) -> List[Dict[str, Any]]:
        """
        Получение твитов с канала Twitter/X через API v2
        """
        if not self.api_available or not self.client:
            logger.warning("Twitter API недоступен, пропускаем сбор")
            return []

        try:
            username = channel_id.lstrip("@")

            # 1. Получаем ID пользователя по username
            logger.debug(f"Получение ID пользователя @{username}...")
            user_response = self.client.get_user(
                username=username, user_fields=["profile_image_url", "description"]
            )

            if not user_response.data:
                logger.error(f"Пользователь @{username} не найден")
                return []

            user_id = user_response.data.id
            logger.debug(f"Найден пользователь @{username}, ID: {user_id}")

            # 2. Получаем твиты (СЫРЫЕ ДАННЫЕ)
            tweets_response = self.client.get_users_tweets(
                id=user_id,
                max_results=10,
                exclude=["replies"],
                tweet_fields=[
                    "created_at",
                    "public_metrics",
                    "attachments",
                    "text",
                    "entities",
                ],
                expansions=["attachments.media_keys", "referenced_tweets.id"],
                media_fields=["url", "preview_image_url", "type", "alt_text"],
                start_time=datetime.now(timezone.utc) - self.collection_period,
            )

            if not tweets_response.data:
                logger.info(
                    f"Нет новых твитов от @{username} за последние {self.collection_period}"
                )
                return []

            # 3. Обрабатываем медиа для использования в extract_news_data
            media_dict = {}
            if tweets_response.includes and "media" in tweets_response.includes:
                for media in tweets_response.includes["media"]:
                    if hasattr(media, "media_key"):
                        media_dict[media.media_key] = media

            # Сохраняем media_dict для использования в extract_news_data
            self._current_media_dict = media_dict

            # Возвращаем СЫРЫЕ твиты
            raw_tweets = list(tweets_response.data) if tweets_response.data else []
            return raw_tweets  # Возвращаем СЫРЫЕ данные

        except tweepy.TooManyRequests as e:
            logger.warning(
                f"Превышен лимит Twitter API для @{channel_id}. Подождите 15 минут."
            )
            return []

        except tweepy.TweepyException as e:
            logger.error(f"Ошибка Twitter API для @{channel_id}: {e}")
            return []

        except Exception as e:
            logger.error(f"Общая ошибка при получении твитов с @{channel_id}: {e}")
            return []

    def _extract_tweet_data(self, tweet, media_dict: dict) -> Dict[str, Any]:
        """Извлечение данных из твита API v2"""
        try:
            # Безопасно получаем текст твита
            text = getattr(tweet, "text", "")
            if not text:
                return {}

            # Базовые данные
            tweet_url = f"https://twitter.com/i/web/status/{tweet.id}"

            # Определяем тип твита
            tweet_type = "твит"
            referenced_tweets = getattr(tweet, "referenced_tweets", None)
            if referenced_tweets:
                for ref_tweet in referenced_tweets:
                    ref_type = getattr(ref_tweet, "type", "")
                    if ref_type == "retweeted":
                        tweet_type = "ретвит"
                    elif ref_type == "quoted":
                        tweet_type = "цитата"

            # Безопасно получаем метрики
            metrics = getattr(tweet, "public_metrics", {})
            likes = metrics.get("like_count", 0) if metrics else 0

            # Обрабатываем дату - ВАЖНО: создаем naive datetime
            pub_date = getattr(tweet, "created_at", None)
            if pub_date:
                # Если у даты есть timezone, убираем его
                if hasattr(pub_date, "tzinfo") and pub_date.tzinfo is not None:
                    pub_date = pub_date.astimezone(timezone.utc).replace(tzinfo=None)
            else:
                pub_date = datetime.now(timezone.utc).replace(tzinfo=None)

            news_data = {
                "url": tweet_url,
                "title": text[:150] + "..." if len(text) > 100 else text,
                "text": text,
                "publication_date": pub_date,
                "has_image": False,
                "image_url": None,
                "note": f"Twitter; тип={tweet_type}; лайки={likes}",
            }

            # Проверяем наличие медиа
            attachments = getattr(tweet, "attachments", None)
            if attachments:
                media_keys = getattr(attachments, "media_keys", [])
                for media_key in media_keys:
                    if media_key in media_dict:
                        media = media_dict[media_key]
                        media_type = getattr(media, "type", "")
                        if media_type in ["photo", "animated_gif"]:
                            news_data["has_image"] = True
                            # Берем URL изображения или превью
                            image_url = getattr(media, "url", None) or getattr(
                                media, "preview_image_url", None
                            )
                            if image_url:
                                news_data["image_url"] = image_url
                            break

            # Ограничиваем длину текста
            # if news_data["text"] and len(news_data["text"]) > 2000:
            #     news_data["text"] = news_data["text"][:2000] + "..."

            return news_data

        except Exception as e:
            logger.error(
                f"Ошибка извлечения данных из твита {getattr(tweet, 'id', 'unknown')}: {e}"
            )
            return {}

    # Обновляем extract_news_data для использования сохраненного media_dict
    def extract_news_data(self, raw_data: Any) -> Dict[str, Any]:
        """Извлечение данных из СЫРОГО твита Twitter API"""
        return self._extract_tweet_data(raw_data, self._current_media_dict)
