"""
Конфигурация приложения.
Содержит все настройки, загружаемые из переменных окружения.
"""
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Dict, Any

from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


@dataclass
class GoogleSheetsConfig:
    """Конфигурация Google Sheets"""

    credentials_path: str = os.getenv(
        "GOOGLE_SHEETS_CREDENTIALS_PATH", "./data/credentials/google_credentials.json"
    )
    google_spreadsheet_id: str = os.getenv("GOOGLE_SPREADSHEET_ID", "")
    sheets_mapping: dict = field(
        default_factory=lambda: {
            "X(Twitter)": "twitter",
            "Telegram": "telegram",
            "YouTube": "youtube",
            "Reddit": "reddit",
        }
    )


@dataclass
class APIConfig:
    """Конфигурация API ключей"""

    # Yandex Translate
    # yandex_translate_api_key: str = os.getenv("YANDEX_TRANSLATE_API_KEY", "")

    # Telegram
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_channel_id: str = os.getenv("TELEGRAM_CHANNEL_ID", "")
    telegram_api_id: str = os.getenv("TELEGRAM_API_ID", "")
    telegram_api_hash: str = os.getenv("TELEGRAM_API_HASH", "")
    telegram_2fa_password: str = os.getenv("TELEGRAM_2FA_PASSWORD", "")
    telegram_phone: str  = os.getenv("TELEGRAM_PHONE", "")

    # Twitter/X API v2 (Essential или Elevated access)
    twitter_bearer_token: str = os.getenv("TWITTER_BEARER_TOKEN", "")

    # Reddit (пока опускаем, потом добавим)
    # reddit_client_id: str = os.getenv("REDDIT_CLIENT_ID", "")
    # reddit_client_secret: str = os.getenv("REDDIT_CLIENT_SECRET", "")
    # reddit_user_agent: str = os.getenv("REDDIT_USER_AGENT", "news_digest_bot/1.0")

    # YouTube
    youtube_api_key: str = os.getenv("YOUTUBE_API_KEY", "")
    groq_api_key: str = os.getenv("GROQ_API_KEY", "")


@dataclass
class AppConfig:
    """Основная конфигурация приложения"""

    # Пути - ВАЖНО: вычисляем от корня проекта
    base_dir: str = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # news_digest_app/
    data_dir: str = os.path.join(base_dir, "data")
    logs_dir: str = os.path.join(data_dir, "logs")

    # База данных - исправляем путь
    database_url: str = f"sqlite:///{os.path.join(data_dir, 'news.db')}"

    # Настройки логирования
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    # Время запуска
    timezone: str = os.getenv("TIMEZONE", "Europe/Moscow")

    # Пороги
    interest_threshold: float = float(os.getenv("INTEREST_THRESHOLD", "0.1500"))
    max_news_per_channel: int = int(os.getenv("MAX_NEWS_PER_CHANNEL", "5"))
    max_news_per_digest: int = int(os.getenv("MAX_NEWS_PER_DIGEST", "5"))
    max_news_time_period: int = int(os.getenv("MAX_NEWS_TIME_PERIOD", "24"))

    # Веб-интерфейс, ПОКА НЕ РАБОЧИЕ
    web_host: str = os.getenv("WEB_HOST", "0.0.0.0")
    web_port: int = os.getenv("WEB_PORT", "5000")
    debug: bool = os.getenv("DEBUG", "False").lower() == "true"

    # Метод обхода каналов
    bypassing_method = os.getenv("BYPASSING_METHOD", "NONE")

    # ML модель
    model_type: str = os.getenv("MODEL_TYPE", "LOCAL")

    # Топик(тематика)
    topic: str = os.getenv("TOPIC", 'Искусственный интеллект')

    # Статусы парсеров
    parser_status: dict = field(
        default_factory=lambda: {
            "twitter": os.getenv("TWITTER_ACTIVE", "FALSE") == "TRUE",
            "telegram": os.getenv("TELEGRAM_ACTIVE", "FALSE") == "TRUE",
            "youtube": os.getenv("YOUTUBE_ACTIVE", "FALSE") == "TRUE",
            "reddit": os.getenv("REDDIT_ACTIVE", "FALSE") == "TRUE",
        }
    )

    def update_from_dict(self, updates: Dict[str, Any]) -> None:
        """Обновление полей из словаря"""
        for key, value in updates.items():
            if hasattr(self, key):
                # Преобразование типов для специфических полей
                if key == 'parser_status' and isinstance(value, dict):
                    self.parser_status.update(value)
                elif key == 'debug' and isinstance(value, str):
                    setattr(self, key, value.lower() == "true")
                elif key == 'bypassing_method':
                    setattr(self, key, value)
                elif key in ('interest_threshold', 'max_news_per_channel',
                            'max_news_per_digest', 'max_news_time_period',
                            'web_port', 'model_type', 'topic'):
                    setattr(self, key, type(getattr(self, key))(value))
                else:
                    logger.warning(f"Спорный ключ {key}:{value}")
                    setattr(self, key, value)

    def __post_init__(self):
        # Создаем необходимые директории
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)


@dataclass
class SchedulerConfig:
    """Конфигурация непрерывного планировщика"""

    # Интервалы между каналами в секундах
    intervals: Dict[str, int] = None

    # Файлы для сохранения состояния
    state_dir: str = None
    state_file: str = None
    channels_file: str = None
    sync_lock_file: str = None

    # Настройки синхронизации
    sync_interval_hours: int = 24  # Синхронизация с Google Sheets раз в 24 часа
    max_failures_per_channel: int = 3  # Максимум ошибок на канал до его деактивации
    max_consecutive_failures: int = 5  # Максимум последовательных ошибок до перезапуска парсера

    # Статистика
    stats_window: int = 100  # Количество запросов для расчета статистики
    min_requests_for_stats: int = 10  # Минимальное количество запросов для статистики

    # Время публикации
    daily_digest = json.loads(os.getenv("DAILY_DIGEST", "{}"))
    weekly_digest = json.loads(os.getenv("WEEKLY_DIGEST", "{}"))
    monthly_digest = json.loads(os.getenv("MONTHLY_DIGEST", "{}"))

    def __init__(self):
        # Временные промежутки между каналами источника
        self.intervals = {
            "twitter": int(os.getenv("TWITTER_INTERVAL", "900")),  # 900 секунд
            "telegram": int(os.getenv("TELEGRAM_INTERVAL", "5")),  # 5 секунд
            "youtube": int(os.getenv("YOUTUBE_INTERVAL", "5")),  # 5 секунд
            # "reddit": int(os.getenv("REDDIT_INTERVAL","1")) * 60,  # 1 минут
        }

        # Создаем директорию для состояния
        self.state_dir = os.path.join(
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data"),
            "scheduler_state")
        os.makedirs(self.state_dir, exist_ok=True)

        self.state_file = os.path.join(self.state_dir, "scheduler_state.json")
        self.channels_file = os.path.join(self.state_dir, "channels_cache.json")
        self.sync_lock_file = os.path.join(self.state_dir, "sync.lock")

    def get_interval(self, source_type: str) -> int:
        """Получение интервала для типа источника"""
        return self.intervals.get(source_type, 10)  # По умолчанию 10 секунд

    def update_intervals(self, new_intervals: Dict[str, int]) -> None:
        """Обновление интервалов"""
        for source, value in new_intervals.items():
            if source in self.intervals:
                self.intervals[source] = int(value)

    def update_digest_schedule(self, digest_type: str, schedule: Dict[str, Any]) -> None:
        """Обновление расписания дайджеста"""
        logger.info(f"update_digest_schedule: {digest_type}, {schedule}")
        if digest_type == 'daily':
            self.daily_digest.update(json.loads(schedule))
        elif digest_type == 'weekly':
            self.weekly_digest.update(json.loads(schedule))
        elif digest_type == 'monthly':
            self.monthly_digest.update(json.loads(schedule))

class Config:
    """Главный класс конфигурации"""

    def __init__(self):
        self.app = AppConfig()
        self.api = APIConfig()
        self.google_sheets = GoogleSheetsConfig()
        self.scheduler = SchedulerConfig()

    def update_config(self, section: str, updates: Dict[str, Any]) -> bool:
        """Унифицированное обновление конфигурации"""
        # Если передан один ключ-значение (не словарь), преобразуем в словарь
        if not isinstance(updates, dict):
            logger.warning(f"updates не является словарём: {type(updates)}")
            return False
        try:
            if section == 'app':
                self.app.update_from_dict(updates)
            elif section == 'scheduler':
                if 'daily_digest' in updates:
                    self.scheduler.update_digest_schedule('daily', updates['daily_digest'])
                if 'weekly_digest' in updates:
                    self.scheduler.update_digest_schedule('weekly', updates['weekly_digest'])
                if 'monthly_digest' in updates:
                    self.scheduler.update_digest_schedule('monthly', updates['monthly_digest'])
            elif section == 'api':
                for key, value in updates.items():
                    if hasattr(self.api, key):
                        setattr(self.api, key, value)
                    elif hasattr(self.google_sheets, key):
                        setattr(self.google_sheets, key, value)
                    else:
                        logger.warning(f"Спорная Api настройка: {key}:{value}")
            elif section == 'parsers':
                    logger.info(updates)
                    short_update = transform_to_short_name(updates, "INTERVAL")
                    logger.info(short_update)
                    if short_update:
                        self.scheduler.intervals.update(short_update)
                    short_update = transform_to_short_name(updates, "ACTIVE")
                    logger.info(short_update)
                    if short_update:
                        self.app.parser_status.update(short_update)
            return True
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"Ошибка обновления конфигурации: {e}")
            return False

    def reload_from_env(self) -> None:
        """Перезагрузка конфигурации из окружения"""
        self.app = AppConfig()
        self.api = APIConfig()
        self.google_sheets = GoogleSheetsConfig()
        self.scheduler = SchedulerConfig()


# Глобальный экземпляр конфигурации
config = Config()

def transform_to_short_name(updates: Dict[str, Any], type_key: str) -> Dict[str, int]:
    """
    Трансформация ключей парсеров из формата пользователя в формат config.

    Вход: {"TWITTER_INTERVAL": 910, "TELEGRAM_INTERVAL": 7, "YOUTUBE_INTERVAL": 5}, "INTERVAL"
    Выход: {"twitter": 910, "telegram": 7, "youtube": 5}
    """
    SOURCE_MAPPING = {
        'TWITTER': 'twitter',
        'TELEGRAM': 'telegram',
        'YOUTUBE': 'youtube',
        'REDDIT': 'reddit',
    }
    result = {}
    for key, value in updates.items():
        # Проверяем, начинается ли ключ с названия источника
        for source_key, config_key in SOURCE_MAPPING.items():
            if key.upper().startswith(f"{source_key}_{type_key}") :
                try:
                    result[config_key] = value=="TRUE" if type_key == 'ACTIVE' else int(value)
                except (ValueError, TypeError):
                    logger.warning(f"Не удалось преобразовать интервал {key}={value} в число")
                break
    return result