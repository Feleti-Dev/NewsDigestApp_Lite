import logging
from datetime import datetime
from typing import Any, Dict, List

from .base_parser import BaseParser

logger = logging.getLogger(__name__)


class YouTubeParser(BaseParser):
    """Парсер для YouTube с улучшенным поиском каналов"""

    def __init__(self):
        super().__init__("YouTube")
        self.api_key = None
        self.youtube = None
        self._init_api()
        # Инициализируем кэш
        self._cache_file = "data/youtube_channel_cache.json"

    def _init_api(self):
        """Инициализация YouTube API"""
        from app.configs import config

        self.api_key = config.api.youtube_api_key

        if not self.api_key:
            logger.warning(
                "YouTube API ключ не указан. YouTube парсер будет работать в тестовом режиме."
            )
            return

        try:
            from googleapiclient.discovery import build

            self.youtube = build("youtube", "v3", developerKey=self.api_key)
            logger.info("YouTube API клиент инициализирован")
        except Exception as e:
            logger.error(f"Ошибка инициализации YouTube API: {e}")
            self.youtube = None

    async def fetch_channel_news(
        self, channel_url: str, channel_id: str
    ) -> List[Dict[str, Any]]:
        """
        Получение видео с канала YouTube с оптимизацией квоты
        """
        # Если нет клиента API, возвращаем тестовые СЫРЫЕ данные
        if not self.youtube:
            return []
        try:
            from googleapiclient.errors import HttpError
            try:
                # Используем дешевый метод channels.list вместо search
                channels_response = self.youtube.channels().list(
                    part="contentDetails,id",
                    # id=channel_id if not channel_id.startswith('@') else None,
                    forHandle=channel_id,
                    fields="items(contentDetails/relatedPlaylists/uploads)"
                ).execute()

                if not channels_response.get("items"):
                    logger.warning(f"Канал {channel_id} не найден")
                    return []

                uploads_playlist_id = channels_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

            except HttpError as e:
                logger.error(f"Ошибка получения playlist_id для {channel_id}: {e}")
                return []

            # 3. ПОЛУЧАЕМ ВИДЕО ЧЕРЕЗ PLAYLISTITEMS.LIST (1 единица)
            try:
                playlist_response = self.youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=uploads_playlist_id,
                    maxResults=5,
                    fields="items(snippet(publishedAt,title,description,channelTitle,thumbnails),contentDetails/videoId)"
                ).execute()

                if not playlist_response.get("items"):
                    logger.info(f"Нет видео в плейлисте загрузок {channel_id}")
                    return []

                # Собираем видео (уже есть вся нужная информация!)
                videos = []
                video_ids = []
                for item in playlist_response["items"]:
                    videos.append({
                        "id": item["contentDetails"]["videoId"],
                        "snippet": item["snippet"]
                    })
                    video_ids.append(item["contentDetails"]["videoId"])
                return videos

            except HttpError as e:
                logger.error(f"Ошибка получения видео для {channel_id}: {e}")
                return []

        except Exception as e:
            logger.error(f"Общая ошибка для канала {channel_id}: {e}")
            return []

    def extract_news_data(self, raw_data: Any) -> Dict[str, Any]:
        """
        Извлечение данных из видео YouTube

        Args:
            raw_data: Объект видео от YouTube API

        Returns:
            Словарь с данными новости
        """
        try:
            video = raw_data
            snippet = video["snippet"]
            # Базовые данные
            news_data = {
                "url": f"https://www.youtube.com/watch?v={video['id']}",
                "title": snippet.get("title", ""),
                "text": snippet.get("description", ""),
                "publication_date": datetime.strptime(
                    snippet["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"
                ),
                "has_image": True,
                "image_url": snippet.get("thumbnails", {}).get("high", {}).get("url"),
                "note": f"YouTube video; channel={snippet.get('channelTitle', '')}",
            }
            return news_data

        except Exception as e:
            logger.error(f"Ошибка извлечения данных из видео YouTube: {e}")
            return {}
