from datetime import datetime

from sqlalchemy import (Boolean, Column, DateTime, Float, Integer, String,
                        Text, create_engine)
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

from app.configs import config

Base = declarative_base()

class NewsItem(Base):
    """Модель новости в базе данных"""

    __tablename__ = "news_items"

    # Обязательные поля согласно спецификации
    ID = Column(
        "id",
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="ID новости (порядковый номер)",
    )
    Source = Column(
        "source",
        String(50),
        nullable=False,
        comment="Источник: Telegram/Reddit/X/YouTube",
    )
    News_URL = Column(
        "news_url", String(500), nullable=False, unique=True, comment="URL новости"
    )
    Headline = Column(
        "headline", String(500), nullable=True, comment="Заголовок новости"
    )
    News_text = Column("news_text", Text, nullable=True, comment="Текст новости")
    Publication_date = Column(
        "publication_date", DateTime, nullable=False, comment="Дата публикации (ISO)"
    )
    Has_image = Column(
        "has_image", Boolean, default=False, comment="Есть изображение TRUE/FALSE"
    )
    Image_URL = Column(
        "image_url", String(500), nullable=True, comment="URL изображения"
    )
    Interest_score = Column(
        "interest_score",
        Float,
        nullable=True,
        comment="Оценка полезности 0.0000-1.0000",
    )
    Daily_used = Column(
        "daily_used", Boolean, default=False, comment="Использована в дневном дайджесте"
    )
    Weekly_used = Column(
        "weekly_used",
        Boolean,
        default=False,
        comment="Использована в недельном дайджесте",
    )
    Monthly_used = Column(
        "monthly_used",
        Boolean,
        default=False,
        comment="Использована в месячном дайджесте",
    )
    Publication_error = Column(
        "publication_error",
        Boolean,
        default=False,
        comment="Ошибка публикации TRUE/FALSE",
    )
    Note = Column(
        "note", Text, nullable=True, comment="Примечание (ошибки/дубликаты/пояснения)"
    )

    # Дополнительные технические поля
    created_at = Column(DateTime, default=datetime.now, comment="Дата создания записи")
    updated_at = Column(
        DateTime,
        default=datetime.now,
        onupdate=datetime.now,
        comment="Дата обновления записи",
    )

    def __repr__(self):
        return f"<NewsItem(id={self.ID}, source='{self.Source}', headline='{self.Headline[:30]}...')>"

class ChannelSource(Base):
    """Модель для хранения источников (каналов) из Google Sheets"""

    __tablename__ = "channel_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_type = Column(
        String(50),
        nullable=False,
        comment="Тип источника: twitter, telegram, youtube, reddit",
    )
    url = Column(String(500), nullable=False, comment="URL канала")
    channel_id = Column(String(200), nullable=True, comment="ID канала (извлеченный)")
    sheet_name = Column(
        String(100), nullable=False, comment="Название листа в Google Sheets"
    )
    row_number = Column(Integer, nullable=False, comment="Номер строки в листе")
    is_active = Column(Boolean, default=True, comment="Активен ли канал для сбора")
    last_checked = Column(
        DateTime, nullable=True, comment="Когда последний раз проверяли"
    )
    # НОВЫЕ ПОЛЯ ДЛЯ СТАТИСТИКИ
    news_collected = Column(Integer, default=0, comment="Количество собранных новостей")
    success_count = Column(Integer, default=0, comment="Количество успешных сборов")
    failure_count = Column(Integer, default=0, comment="Количество неудачных сборов")
    avg_interest_score = Column(Float, default=0.0, comment="Средний балл интереса")
    last_processed = Column(DateTime, nullable=True, comment="Когда последний раз обрабатывался")

    def __repr__(self):
        return f"<ChannelSource(id={self.id}, type='{self.source_type}', channel='{self.channel_id}')>"


def init_database():
    """Инициализация базы данных"""
    engine_local = create_engine(config.app.database_url, echo=False)
    Base.metadata.create_all(bind=engine_local)

    # Форсируем создание файла БД
    with engine_local.connect() as conn:
        pass

    return engine_local


# Инициализируем БД
engine = init_database()
# Создаем сессию
SessionLocal = sessionmaker(bind=engine, autoflush=True)





