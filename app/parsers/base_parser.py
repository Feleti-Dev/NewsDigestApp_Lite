import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from app.configs.config import config
from app.database.db_utils import DatabaseManager
from app.ml.llm_client import get_llm_client

logger = logging.getLogger(__name__)


class BaseParser(ABC):
    """Абстрактный базовый класс для всех парсеров"""

    def __init__(self, source_type: str):
        self.source_type = source_type
        self.db_manager = None  # Инициализируем позже
        self.max_items_per_channel = config.app.max_news_per_channel
        self.collection_period = timedelta(hours=int(config.app.max_news_time_period))
        # Ключевые слова для определения рекламы (русский и английский)
        self.ad_keywords = [
            # Русские ключевые слова
            'реклама', 'рекламный', 'рекламы', 'рекламу', 'рекламой',
            # 'спонсор', 'спонсорский', 'спонсорская', 'спонсорское',
            # 'партнер', 'партнерский', 'партнерская', 'партнерское',
            # 'вакансия', 'вакансии', 'вакансий', 'работа', 'работы',
            # 'набор', 'набор в команду', 'ищем', 'требуется',
            # 'заработок', 'зарабатывать', 'заработать',
            # 'заказ', 'заказать', 'оформление заказа',
            # 'купить', 'продать', 'покупка', 'продажа',
            # 'скидка', 'акция', 'распродажа', 'спецпредложение',
            # 'бесплатно', 'бесплатный', 'бесплатная',
            # 'промокод', 'купон', 'скидочный купон',
            # 'курс', 'обучение', 'тренинг', 'мастер-класс',
            # 'рассылка', 'рассылки', 'email-рассылка',
            # 'маркетинг', 'продвижение', 'seo', 'контекст',
            # 'отзыв', 'отзывы', 'оставить отзыв',
            # 'подписка', 'подписаться', 'оформить подписку',
            # 'лид', 'лидогенерация', 'привлечение клиентов',

            # Английские ключевые слова
            'advertisement', 'ad',
            # 'promotion', 'sponsored', 'sponsor',
            # 'promo', 'promotional',
            'advert', 'advertising',
            # 'vacancy', 'job', 'jobs',
            # 'hiring', 'career', 'recruitment',
            # 'work with us', 'we are hiring', 'looking for',
            # 'earn', 'earn money', 'make money', 'income',
            # 'order', 'buy', 'purchase', 'sell', 'sale',
            # 'discount', 'offer', 'deal', 'special offer',
            # 'free', 'trial', 'free trial',
            # 'coupon', 'voucher', 'promo code', 'discount code',
            # 'course', 'training', 'workshop', 'masterclass',
            # 'newsletter', 'email newsletter', 'mailing list',
            # 'marketing', 'seo', 'digital marketing',
            # 'review', 'reviews', 'leave a review',
            # 'subscription', 'subscribe', 'sign up',
            # 'lead', 'lead generation', 'client acquisition'
        ]

        # Регулярное выражение для поиска рекламных ключевых слов
        self.ad_pattern = re.compile(
            r'\b(' + '|'.join(re.escape(kw) for kw in self.ad_keywords) + r')\b',
            re.IGNORECASE
        )

        # Дополнительные паттерны для определения рекламы
        self.additional_patterns = [
            re.compile(r'#[а-яa-z]*реклам[а-я]*', re.IGNORECASE),  # Хэштеги с рекламой
            re.compile(r'#[а-яa-z]*ad[а-яa-z]*', re.IGNORECASE),  # Хэштеги с ad
            #re.compile(r'#[а-яa-z]*job[а-яa-z]*', re.IGNORECASE),  # Хэштеги с job
            #re.compile(r'#[а-яa-z]*ваканс[а-я]*', re.IGNORECASE),  # Хэштеги с вакансией
            #re.compile(r'\b(звони|позвони|call|contact)\b', re.IGNORECASE),  # Призывы к действию
            #re.compile(r'\b(телефон|phone|тел\.?)\s*[+\d\s\-()]+', re.IGNORECASE),  # Телефоны
            #re.compile(r'\b(email|почта|e-mail)\s*[:=]?\s*[\w\.-]+@[\w\.-]+', re.IGNORECASE),  # Email
        ]
    def _init_db_manager(self):
        """Инициализация менеджера БД (отложенная)"""
        if not self.db_manager:
            self.db_manager = DatabaseManager()

    @abstractmethod
    async def fetch_channel_news(
        self, channel_url: str, channel_id: str
    ) -> List[Dict[str, Any]]:
        """Получение новостей с конкретного канала"""
        pass

    @abstractmethod
    def extract_news_data(self, raw_data: Any) -> Dict[str, Any]:
        """Извлечение данных новости из сырых данных API"""
        pass

    def normalize_news_data(self, news_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Нормализация данных новости в единый формат для БД
        """
        # Проверяем обязательные поля
        if "url" not in news_data:
            logger.warning(f"Новость без URL: {news_data.get('title', 'Unknown')}")
            news_data["url"] = ""

        # Обрабатываем дату публикации
        pub_date = news_data.get("publication_date")

        # Если дата уже является datetime, используем её
        if isinstance(pub_date, datetime):
            # Если у даты есть timezone, убираем его
            if pub_date.tzinfo is not None:
                pub_date = pub_date.astimezone(timezone.utc).replace(tzinfo=None)
        else:
            # Пробуем преобразовать строку в datetime
            try:
                if isinstance(pub_date, str):
                    # Пробуем разные форматы
                    formats = [
                        "%Y-%m-%dT%H:%M:%S%z",  # ISO с таймзоной
                        "%Y-%m-%dT%H:%M:%S",  # ISO без таймзоны
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d %H:%M:%S.%f",
                        "%d.%m.%Y %H:%M:%S",
                        "%d/%m/%Y %H:%M:%S",
                    ]
                    for fmt in formats:
                        try:
                            pub_date = datetime.strptime(pub_date, fmt)
                            # Если есть timezone, убираем
                            if pub_date.tzinfo is not None:
                                pub_date = pub_date.astimezone(timezone.utc).replace(
                                    tzinfo=None
                                )
                            break
                        except ValueError:
                            continue
            except Exception as e:
                logger.warning(f"Не удалось распарсить дату {pub_date}: {e}")
                pub_date = datetime.now()

        # Если дата всё ещё не определена, используем текущее время
        if not pub_date:
            pub_date = datetime.now()

        # Базовые обязательные поля
        normalized = {
            "Source": self.source_type,
            "News_URL": news_data.get("url", ""),
            "Headline": news_data.get("title", "")[:150],
            "News_text": news_data.get("text", ""),
            "Publication_date": pub_date,  # Теперь точно datetime
            "Has_image": news_data.get("has_image", False),
            "Image_URL": news_data.get("image_url"),
            "Interest_score": None,
            "Daily_used": False,
            "Weekly_used": False,
            "Monthly_used": False,
            "Publication_error": False,
            "Note": news_data.get("note", ""),
        }

        logger.debug(f"Нормализована новость: {normalized['Headline'][:50]}...")
        logger.debug(
            f"Дата публикации: {normalized['Publication_date']}, тип: {type(normalized['Publication_date'])}"
        )

        return normalized

    def is_duplicate(self, news_url: str) -> bool:
        """Проверка на дубликат по URL"""
        self._init_db_manager()
        existing = self.db_manager.get_news_by_url(news_url)
        return existing is not None

    async def process_channel(
        self, channel_url: str, channel_id: str
    ) -> List[Dict[str, Any]]:
        """
        Обработка одного канала с задержкой и обработкой ошибок

        Returns:
            Список нормализованных новостей для БД
        """
        try:
            logger.debug(f"Сбор новостей с {self.source_type}: {channel_id}")

            # 1. Получаем СЫРЫЕ новости
            raw_news = await self.fetch_channel_news(channel_url, channel_id)

            if not raw_news:
                # logger.warning(f"Нет новых новостей с {channel_id}")
                return []

            # 2. Извлекаем и нормализуем данные
            normalized_news = []
            cutoff_time = datetime.now() - self.collection_period
            
            for raw_item in raw_news:
                # 2.1. Извлекаем данные из СЫРОГО формата
                extracted_data = self.extract_news_data(raw_item)
                if not extracted_data:
                    logger.debug(
                        f"Не удалось извлечь данные из сырого элемента: {type(raw_item)}"
                    )
                    continue

                # 2.2. Нормализуем в формат БД
                normalized = self.normalize_news_data(extracted_data)

                # Сначала проверяем по времени (самая дешевая проверка)
                pub_date = normalized["Publication_date"]
                if pub_date < cutoff_time:
                    logger.debug(f"Новость слишком старая: {pub_date}")
                    continue

                # Затем проверяем дубликат (вторая по стоимости проверка)
                if self.is_duplicate(normalized["News_URL"]):
                    logger.debug(f"Дубликат новости: {normalized['News_URL']}")
                    continue

                normalized_news.append(normalized)

                # Ограничиваем количество
                if len(normalized_news) >= self.max_items_per_channel:
                    break

            # Если новостей нет, нет смысла проверять дальше
            if not len(normalized_news):
                return []

            # 3. Пакетная проверка на рекламу (самая дорогая проверка - LLM)
            logger.info(f"🔍 Пакетная проверка на рекламу: {len(normalized_news)} новостей...")

            # Подготавливаем данные для проверки на рекламу
            news_for_ads_check = [
                {
                    "title": news.get("Headline", ""),
                    "text": news.get("News_text", ""),
                    "url": news.get("News_URL", "")
                }
                for news in normalized_news
            ]


            # Пакетный вызов LLM
            llm_client = await get_llm_client()
            ads_results = await llm_client.detect_advertisement(news_for_ads_check)

            # Фильтруем рекламу
            filtered_news = []
            for i, news in enumerate(normalized_news):
                if i < len(ads_results):
                    if ads_results[i]["is_advertisement"] and ads_results[i]["confidence"] > 0.75:
                        logger.info(
                            f"⚠️  LLM определила рекламу (confidence: {ads_results[i]['confidence']:.2f}): "
                            f"{news.get('Headline', '')[:50]}..."
                        )
                        continue  # Пропускаем рекламу
                filtered_news.append(news)

            normalized_news = filtered_news
            logger.info(f"После фильтрации рекламы: {len(normalized_news)} новостей")


            logger.info(f"Обработано {len(normalized_news)} новостей с {channel_id}")
            return normalized_news[: self.max_items_per_channel]

        except Exception as e:
            logger.error(f"Ошибка при обработке канала {channel_id}: {e}")
            return []

    def save_to_database(self, news_items: List[Dict[str, Any]]) -> int:
        """
        Сохранение новостей в базу данных

        Args:
            news_items: Список НОРМАЛИЗОВАННЫХ новостей (с ключами для БД)

        Returns:
            Количество сохраненных новостей
        """
        self._init_db_manager()
        saved_count = 0

        for news_data in news_items:
            try:
                # ДЕБАГ: Проверяем что передаем
                logger.debug(
                    f"Сохраняем новость: {news_data.get('Headline', '')[:50]}..."
                )
                logger.debug(
                    f"Тип даты: {type(news_data.get('Publication_date'))}, значение: {news_data.get('Publication_date')}"
                )

                # Проверяем, что news_data содержит нужные ключи
                required_keys = ["Source", "News_URL", "Headline", "Publication_date"]
                missing_keys = [key for key in required_keys if key not in news_data]

                if missing_keys:
                    logger.error(f"Отсутствуют ключи в новости: {missing_keys}")
                    logger.error(f"Доступные ключи: {list(news_data.keys())}")
                    continue

                news_item = self.db_manager.add_news(news_data)
                if news_item:
                    saved_count += 1
                    logger.debug(
                        f"Сохранена новость: {news_item.ID} - {news_item.Headline[:50]}..."
                    )
            except Exception as e:
                logger.error(f"Ошибка при сохранении новости: {e}")
                import traceback

                logger.error(f"Трассировка: {traceback.format_exc()}")

        logger.info(f"Сохранено в БД новостей из {self.source_type}: {saved_count}")
        return saved_count


    async def close(self):
        """Закрытие ресурсов"""
        pass


    async def calculate_llm_interest_score(self, news_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Расчет оценки полезности новости с помощью ML модели

        Args:
            news_data: данные новости

        Returns:
            Оценка от 0.0000 до 1.0000
        """
        try:
            llm_client = await get_llm_client()
            # Используем LLM API если настроено
            result = await llm_client.calculate_interest_score(news_data, config.app.topic)

            logger.debug(
                f"LLM оценка новости '{result[0].get('Headline', '')[:50]}...': {result[0].get('Interest_score',''):.4f} (причина: {result[0]['reason']})")
            return result

        except Exception as e:
            logger.error(f"Ошибка расчета оценки полезности: {e}")
            return news_data

    async def filter_by_interest_threshold(self, news_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Фильтрация новостей по порогу полезности
        
        Args:
            news_items: список нормализованных новостей
            
        Returns:
            Отфильтрованный список новостей
        """
        threshold = config.app.interest_threshold
        try:
            # Используем LLM API если настроено
            filtered = []
            news_items = await self.calculate_llm_interest_score(news_items)
            for news in news_items:
                # Фильтруем по порогу
                if news.get('Interest_score', 0) >= threshold:
                    filtered.append(news)

            logger.info(f"LLM фильтрация: {len(news_items)} -> {len(filtered)} новостей (порог: {threshold})")
            return filtered

        except Exception as e:
            logger.error(f"Ошибка фильтрации новостей: {e}")
            return news_items