# app/digest/formatter.py
"""
Форматирование дайджеста для Telegram HTML
HTML разметка более стабильна и предсказуема в Telegram
"""
import logging
import re
import html
from typing import List, Dict, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class HTMLDigestFormatter:
    """Класс для форматирования дайджеста в текст для Telegram с HTML разметкой"""

    def __init__(self):
        # Эмодзи для разных элементов
        self.digest_emojis = {
            'daily': '📅',
            'weekly': '📊',
            'monthly': '🎯'
        }

        self.source_emojis = {
            'Twitter': '🐦',
            'Telegram': '📢',
            'YouTube': '🎬',
            'Reddit': '👾',
            'X': '🐦'
        }

        # Эмодзи для номеров
        self.number_emojis = ['1️⃣', '2️⃣', '3️⃣', '4️⃣', '5️⃣', '6️⃣', '7️⃣']

        # Регулярные выражения для очистки
        self.url_pattern = re.compile(
            r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+[/\w .?=&%+~#-]*'
        )
        self.angle_bracket_pattern = re.compile(r'[<>]')

    def escape_html(self, text: str) -> str:
        """
        Экранирование HTML сущностей

        Args:
            text: Текст для экранирования

        Returns:
            Экранированный текст
        """
        if not text:
            return ""

        # Сначала заменяем угловые скобки на безопасные символы
        text = self.angle_bracket_pattern.sub('', text)

        # Удаляем все URL из текста
        text = self.url_pattern.sub('', text)

        # Экранируем HTML сущности
        escaped = html.escape(text)

        return escaped

    def format_digest(self, news_items: List[Dict[str, Any]], digest_type: str) -> str:
        """
        Форматирование дайджеста в текст для Telegram HTML

        Args:
            news_items: Список обработанных новостей
            digest_type: Тип дайджеста ('daily', 'weekly', 'monthly')

        Returns:
            Отформатированный текст дайджеста
        """
        try:
            # 1. Заголовок дайджеста
            header = self._create_header(digest_type)

            # 2. Список новостей
            news_list = self._create_news_list(news_items)

            # 3. Собираем все части
            digest_text = f"{header}\n\n{news_list}"

            # 4. Проверяем длину (ограничение Telegram - 4096 символов)
            if len(digest_text) > 4096:
                digest_text = self._truncate_digest(digest_text)

            logger.info(f"📝 Сформирован дайджест: {len(digest_text)} символов")

            return digest_text

        except Exception as e:
            logger.error(f"❌ Ошибка форматирования дайджеста: {e}")
            return self._create_fallback_digest(news_items, digest_type)

    def _create_header(self, digest_type: str) -> str:
        """Создание заголовка дайджеста"""
        digest_names = {
            'daily': 'ЕЖЕДНЕВНЫЙ',
            'weekly': 'ЕЖЕНЕДЕЛЬНЫЙ',
            'monthly': 'ЕЖЕМЕСЯЧНЫЙ'
        }

        digest_name = digest_names.get(digest_type, 'ДАЙДЖЕСТ')
        digest_emoji = self.digest_emojis.get(digest_type, '📰')

        # Текущая дата
        today = datetime.now()
        date_str = today.strftime("%d.%m.%Y")

        # Для недельного дайджеста добавляем период
        if digest_type == 'weekly':
            week_start = today - timedelta(days=today.weekday())
            week_end = week_start + timedelta(days=6)
            period_str = f"{week_start.strftime('%d.%m')} - {week_end.strftime('%d.%m.%Y')}"
            date_line = f"<b>Период:</b> {period_str}"
        elif digest_type == 'monthly':
            month_name = today.strftime("%B").upper()
            date_line = f"<b>Месяц:</b> {month_name} {today.year}"
        else:
            date_line = f"<b>Дата:</b> {date_str}"

        header = f"{digest_emoji} <b>{self.escape_html(digest_name)} ДАЙДЖЕСТ НОВОСТЕЙ ИИ</b>\n"
        header += f"══════════════════════\n"
        header += f"{date_line}\n"
        header += f"══════════════════════"

        return header

    def _create_news_list(self, news_items: List[Dict[str, Any]]) -> str:
        """Создание списка новостей с HTML разметкой"""
        news_lines = []

        for i, news in enumerate(news_items):
            if i < len(self.number_emojis):
                news_line = self._format_news_item(news, i + 1, self.number_emojis[i])
                news_lines.append(news_line)

        return "\n\n".join(news_lines)

    def _format_news_item(self, news: Dict[str, Any], index: int, number_emoji: str) -> str:
        """Форматирование одной новости с HTML"""
        try:
            # Эмодзи источника
            source_emoji = news.get('source_emoji', '📰')

            # Заголовок
            title = news.get('title', 'Без названия').strip()
            url = news.get('url', '')

            # Экранируем заголовок
            safe_title = self.escape_html(title)

            # Формируем HTML ссылку
            if url and url.startswith('http'):
                # Очищаем URL от проблемных символов
                clean_url = self._make_url_safe(url)
                title_line = f"{number_emoji} {source_emoji} <a href=\"{clean_url}\"><b>{safe_title}</b></a>"
            else:
                title_line = f"{number_emoji} {source_emoji} <b>{safe_title}</b>"

            # Сводка текста
            summary = news.get('summary', news.get('text', ''))
            safe_summary = self.escape_html(summary)
            summary_line = f"   {safe_summary.strip()}"

            # Оценка полезности
            # score = news.get('interest_score', 0)
            # if score > 0:
            #     score_str = f"   ⚡ <b>Полезность:</b> {score:.3f}"
            # else:
            #     score_str = ""

            # Формируем итоговую строку
            news_line = f"{title_line}\n{summary_line}"

            # if score_str:
            #     news_line += f"\n{score_str}"

            return news_line

        except Exception as e:
            logger.error(f"Ошибка форматирования новости {index}: {e}")
            return f"{number_emoji} <b>Ошибка обработки новости</b>"

    def _make_url_safe(self, url: str) -> str:
        """Создание безопасного URL для HTML"""
        # Убираем все пробелы и небезопасные символы
        safe_url = url.strip()
        # Экранируем амперсанды для HTML
        safe_url = safe_url.replace('&', '&amp;')
        return safe_url

    def _truncate_digest(self, digest_text: str, max_length: int = 4000) -> str:
        """Усечение дайджеста если он слишком длинный"""
        if len(digest_text) <= max_length:
            return digest_text

        # Находим последнюю новость, которую можем включить
        lines = digest_text.split('\n\n')
        truncated_text = ""

        for line in lines:
            if len(truncated_text + line + '\n\n') <= max_length:
                truncated_text += line + '\n\n'
            else:
                break

        # Добавляем сообщение об усечении
        truncated_text += f"\n📝 <b>Дайджест сокращен из-за ограничений Telegram</b>\n"

        return truncated_text

    def _create_fallback_digest(self, news_items: List[Dict[str, Any]], digest_type: str) -> str:
        """Создание простого дайджеста в случае ошибки"""
        try:
            header = f"<b>{digest_type.upper()} ДАЙДЖЕСТ НОВОСТЕЙ ИИ</b>"
            header += f"\n<b>Дата:</b> {datetime.now().strftime('%d.%m.%Y')}"
            header += f"\n═══════════════════════════════\n"

            news_lines = []
            for i, news in enumerate(news_items[:5]):
                title = news.get('title', 'Без названия')
                url = news.get('url', '')

                safe_title = self.escape_html(title)
                if url:
                    news_line = f"{i + 1}. <a href=\"{url}\">{safe_title}</a>"
                else:
                    news_line = f"{i + 1}. {safe_title}"

                news_lines.append(news_line)

            return f"{header}\n" + "\n".join(news_lines)

        except Exception:
            # Максимально простой fallback
            return f"{digest_type.upper()} ДАЙДЖЕСТ НОВОСТЕЙ ИИ\n\n" + "\n".join(
                [f"{i + 1}. Новость {i + 1}" for i in range(min(3, len(news_items)))])
