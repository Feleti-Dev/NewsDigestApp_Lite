# app/llm/llm_client.py
"""
Клиент для работы с LLM API (Groq)
"""
import json
import logging
from datetime import datetime
from typing import Dict, Any
from groq import Groq
import requests
from typing import List, Optional
from app.configs.config import config
from app.configs.llm_prompts import AD_DETECTION_PROMPT, INTEREST_SCORING_PROMPT, DIGEST_PROCESSING_PROMPT
import asyncio

logger = logging.getLogger(__name__)


def get_available_groq_models(api_key: str) -> List[str]:
    """
    Получение списка доступных моделей Groq

    Args:
        api_key: Groq API ключ

    Returns:
        Список ID доступных моделей
    """
    try:
        url = "https://api.groq.com/openai/v1/models"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        data = response.json()
        models = [model["id"] for model in data.get("data", [])]

        logger.debug(f"Найдено доступных моделей Groq: {models}")
        return models

    except Exception as e:
        logger.error(f"Ошибка получения списка моделей: {e}")
        # Возвращаем дефолтный список моделей
        return [
            "meta-llama/llama-4-scout-17b-16e-instruct"
        ]


def get_next_model(current_model: str, available_models: List[str]) -> Optional[str]:
    """
    Получение следующей модели для ротации

    Args:
        current_model: Текущая модель
        available_models: Список доступных моделей

    Returns:
        Следующая модель или None если это последняя
    """
    if not available_models:
        return None

    try:
        current_index = available_models.index(current_model)
        if current_index < len(available_models) - 1:
            return available_models[current_index + 1]
        else:
            return None  # Достигли конца списка
    except ValueError:
        # Если текущей модели нет в списке, начинаем с первой
        return available_models[0] if available_models else None


class LLMClient:
    """Клиент для работы с Groq API"""

    def __init__(self):
        self.api_key = config.api.groq_api_key
        self.current_model = "meta-llama/llama-4-scout-17b-16e-instruct"
        self.available_models = get_available_groq_models(self.api_key)
        self.client = Groq(api_key=self.api_key)
        # Счётчик ошибок для текущей модели
        self.model_errors = {}

    async def _rotate_model(self) -> bool:
        """
        Переключение на следующую модель при ошибках

        Args:
            error_type: Тип ошибки ("rate_limit" или "general")

        Returns:
            True если удалось переключить модель, False если модели закончились
        """
        # Увеличиваем счётчик ошибок для текущей модели
        self.model_errors[self.current_model] = self.model_errors.get(self.current_model, 0) + 1

        # Если это rate limit, ищем следующую модель
        next_model = get_next_model(self.current_model, self.available_models)

        if next_model:
            logger.warning(f"Rate limit достигнут для модели {self.current_model}. "
                           f"Переключаюсь на {next_model}")
            self.current_model = next_model
            return True
        else:
            logger.error(f"Все доступные модели исчерпали лимиты. "
                         f"Последняя модель: {self.current_model}")
            return False

    async def _call_groq_with_retry(self, messages: List[Dict],
                                    max_retries: int = 10) -> Optional[str]:
        """
        Вызов Groq API с ротацией моделей при лимитах

        Args:
            messages: Сообщения для LLM
            max_retries: Максимальное количество попыток (с разными моделями)

        Returns:
            Ответ от LLM или None если все попытки провалились
        """
        retry_count = 0
        original_model = self.current_model

        while retry_count < max_retries:
            try:
                # logger.info(f"Использую модель: {self.current_model}")

                chat_completion = self.client.chat.completions.create(
                    messages=messages,
                    model=self.current_model,
                    temperature=0.01,
                    max_tokens=4000,
                    response_format={"type": "json_object"}
                )

                # Если успешно, сбрасываем счётчик ошибок для этой модели
                if self.current_model in self.model_errors:
                    del self.model_errors[self.current_model]
                return (chat_completion.choices[0].message.content
                        .replace("```", "")
                        .replace("json", "")
                        .replace("\n\n","\n"))

            except Exception as e:
                error_str = str(e)
                logger.warning(f"Ошибка при вызове модели {self.current_model}: {error_str}")

                # Проверяем, это rate limit или другая ошибка
                if "429" in error_str or "rate_limit" in error_str.lower():
                    # Пробуем переключить модель
                    success = await self._rotate_model()
                    if not success:
                        logger.error("Не удалось переключить модель, все лимиты исчерпаны")
                        return None
                    retry_count = -1 # Если переключились на следующую модель, то сбрасываем счётчик ошибок
                else:
                    # Для других ошибок пробуем переподключиться к той же модели
                    logger.error(f"Ошибка модели {self.current_model}: {e}")
                    if retry_count < max_retries - 1:
                        await asyncio.sleep(2)  # Экспоненциальная задержка
                    else:
                        # Если это последняя попытка, пробуем сменить модель
                        success = await self._rotate_model()
                        if not success:
                            return None
                        retry_count = -1 # Если переключились на следующую модель, то сбрасываем счётчик ошибок
                retry_count += 1

        # Если все попытки провалились, возвращаемся к исходной модели
        self.current_model = original_model
        return None

    async def detect_advertisement(self, news_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Определение рекламы в тексте

        Args:
            title: Заголовок новости
            text: Текст новости

        Returns:
            Dict с ключами: is_advertisement (bool), confidence (float)
        """
        if not news_items:
            return []
        try:
            # Подготавливаем данные для промта
            news_for_prompt = []
            for i, news in enumerate(news_items):
                news_for_prompt.append({
                    "index": i,
                    "title": news.get('title', ''),
                    "text": news.get('text', ''),
                    "url": news.get('url', '')
                })

            prompt = AD_DETECTION_PROMPT.format(
                news_data=json.dumps(news_for_prompt, ensure_ascii=False, indent=2)
            )

            response_text = await self._call_groq_with_retry([
                {"role": "system", "content": "Ты - эксперт по определению рекламного контента."},
                {"role": "user", "content": prompt}
            ])

            if not response_text:
                logger.error("Не удалось получить ответ от LLM для детекции рекламы")
                # Возвращаем дефолтные значения (не реклама)
                return [{"is_advertisement": True, "confidence": -1.0} for _ in news_items]

            try:
                results = json.loads(response_text)
                # Приводим к списку если вернулся одиночный объект
                if "results" in results:
                    results = results["results"]
                if isinstance(results, dict):
                    results = [results]
                elif not isinstance(results, list):
                    results = [results]
                # logger.info(f"LLM ADVERT RESULT: {results}")
                return results
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка парсинга JSON в детекции рекламы: {e}")
                return [{"is_advertisement": True, "confidence": -1.0} for _ in news_items]

        except Exception as e:
            logger.error(f"Ошибка при детекции рекламы: {e}")
            return [{"is_advertisement": True, "confidence": -1.0} for _ in news_items]

    async def calculate_interest_score(self, news_data: List[Dict[str, Any]], topic: str = "Искусственный интеллект") -> \
            List[Dict[str, Any]]:
        """
        Расчет оценки полезности новости

        Args:
            news_data: Данные новости в формате normalize_news_data
            topic: Тематика для оценки (по умолчанию "Искусственный интеллект")

        Returns:
            Dict с ключами: interest_score (float), reason (str)
        """
        try:
            # Подготавливаем данные для промта
            news_for_prompt = []
            for news in news_data:
                news_for_prompt.append({
                    "source": news.get("Source", ""),
                    "headline": news.get("Headline", ""),
                    "text": news.get("News_text", ""),
                    "url": news.get("News_URL", ""),
                    "publication_date": str(news.get("Publication_date", "")),
                    "has_image": news.get("Has_image", False)
                })

            prompt = INTEREST_SCORING_PROMPT.format(
                topic=topic,
                news_data=json.dumps(news_for_prompt, ensure_ascii=False, indent=2)
            )
            response_text = await self._call_groq_with_retry([
                {"role": "system", "content": f"Ты - эксперт по оценке новостей в теме: {topic}"},
                {"role": "user", "content": prompt}
            ])
            if not response_text:
                logger.error("Не удалось получить ответ от LLM после всех попыток")
                # Возвращаем дефолтные оценки
                for news in news_data:
                    news["Interest_score"] = float(0.00)
                    news["reason"] = "Ошибка получения оценки"
                return news_data
            results = json.loads(response_text)
            if isinstance(results, dict):
                results = [results]
            elif not isinstance(results, list):
                results = [results]
            # logger.info(f"LLM SCORE RESULT: {results}")
            for news, result in zip(news_data, results):
                news["Interest_score"] = float(result["interest_score"])
                news["reason"] = result["reason"]
            return news_data

        except Exception as e:
            logger.error(f"Ошибка при расчете оценки полезности: {e}")
            for news in news_data:
                news["Interest_score"] = float(0.00)
                news["reason"] = "Ошибка в расчёте"
            return news_data

    async def process_digest_news(self, news_items: List[Dict[str, Any]], digest_type: str = 'daily') -> Dict[str, Any]:
        """
        Обработка новостей для дайджеста (перевод, суммаризация, форматирование)

        Args:
            news_items: Список новостей в формате creator.py
            digest_type: Дневной\Недельный\Месячный дайджест

        Returns:
            Dict с ключами: digest_text (str) - готовый дайджест в Markdown
        """
        try:
            current_date = datetime.now().strftime("%d.%m.%Y")
            # Подготавливаем данные для промта
            news_for_prompt = []
            for news in news_items:
                news_for_prompt.append({
                    "id": news.get("id", ""),
                    "source": news.get("source", ""),
                    "title": news.get("title", ""),
                    "text": news.get("text", ""),
                    "url": news.get("url", ""),
                    "image_url": news.get("image_url", ""),
                    "interest_score": news.get("interest_score", 0.0)
                })
            digest_type_format = {"daily": "Дневной", "weekly": "Недельный", "monthly": "Месячный"}
            prompt = DIGEST_PROCESSING_PROMPT.format(
                date=current_date,
                digest_type=digest_type_format.get(digest_type, 'Дневной'),
                news_data=json.dumps(news_for_prompt, ensure_ascii=False, indent=2)
            )
            response_text = await self._call_groq_with_retry([
                {"role": "system", "content": "Ты - профессиональный редактор дайджестов новостей."},
                {"role": "user", "content": prompt}
            ])

            result = json.loads(response_text)
            return {
                "digest_text": result.get("digest_text", "")
            }

        except Exception as e:
            logger.error(f"Ошибка при обработке дайджеста: {e}")
            return {"digest_text": ""}


# Глобальный экземпляр для использования во всем приложении
llm_client = LLMClient()
