# app/llm/llm_client.py
"""
Клиент для работы с LLM API (Groq)
"""
import json, logging, time, asyncio, re
from datetime import datetime
from typing import Dict, Any, List, Optional, Set
# from groq import AsyncGroq
# from groq._exceptions import (
#     RateLimitError,
#     APITimeoutError,
#     PermissionDeniedError,
#     NotFoundError,
#     BadRequestError,
#     AuthenticationError,
#     InternalServerError,
#     APIStatusError
#
# )

from openai import AsyncOpenAI
from openai._exceptions import (
    RateLimitError,
    APITimeoutError,
    PermissionDeniedError,
    NotFoundError,
    BadRequestError,
    AuthenticationError,
    InternalServerError,
    APIStatusError
)
import httpx
from app.configs.config import config
from app.configs.llm_prompts import AD_DETECTION_PROMPT, INTEREST_SCORING_PROMPT, DIGEST_PROCESSING_PROMPT

logger = logging.getLogger(__name__)

BLOCKED_MODELS = frozenset({
    "canopylabs/orpheus-arabic-saudi",
    "canopylabs/orpheus-v1-english",
    "groq/compound-mini",
    "llama-3.1-8b-instant",
    "meta-llama/llama-prompt-guard-2-22m",
    "meta-llama/llama-prompt-guard-2-86m",
    "openai/gpt-oss-safeguard-20b",
    "whisper-large-v3",
    "whisper-large-v3-turbo"})

JSON_MODE_BLOCKED = frozenset({
    # "llama-3.3-70b-versatile",
    # "gemma2-9b-it",
})

class LLMClient:
    """Клиент для работы с Groq API"""

    def __init__(self):
        self.base_url = "https://api.groq.com/openai/v1" #"https://api.groq.com/openai/v1" #"https://ai.feleti.by/llm/v1"
        self.api_key = config.api.groq_api_key or ""
        self._client: Optional[AsyncOpenAI] = None
        self.available_models: List[str] = []
        self.current_model: str = ""
        self._banned_models: Set[str] = set()
        self._models_last_fetch: float = 0
        self._models_cache_ttl: int = 3600  # 1 час
        self._semaphore = asyncio.Semaphore(1)

    async def _ensure_client(self) -> AsyncOpenAI:
        if self._client is None:
            self._client = AsyncOpenAI(
                base_url=self.base_url,
                api_key=self.api_key,
                max_retries=6,
                timeout=httpx.Timeout(60.0, connect=10.0),
                default_headers={"User-Agent": "NewsDigestApp/1.0"},
            )
        return self._client

    async def _fetch_available_models(self) -> List[str]:
        now = time.time()
        if self.available_models and (now - self._models_last_fetch) < self._models_cache_ttl:
            return self.available_models

        try:
            client = await self._ensure_client()
            data = await client.models.list()
            models = [m.id for m in data.data if m.id not in BLOCKED_MODELS]
            models.sort()
            # Фильтруем модели, не поддерживающие JSON mode
            json_supported = [m for m in models if m not in JSON_MODE_BLOCKED]
            if json_supported:
                models = json_supported
            self.available_models = models
            self._models_last_fetch = now
            if not self.current_model and models:
                self.current_model = models[0]
            logger.info(f"[LLM] Найдено доступных моделей Groq: {models}")
        except Exception as e:
            logger.error(f"[LLM] Ошибка получения списка моделей: {e}")
            if not self.available_models:
                self.available_models = []
        return self.available_models

    async def _rotate_model(self) -> bool:
        self._banned_models.add(self.current_model)
        candidates = [m for m in self.available_models if m not in self._banned_models]
        if not candidates:
            logger.error(f"[LLM] Все модели забанены. Сбрасываем banned.")
            self._banned_models.clear()
            candidates = self.available_models
            if not candidates:
                return False
        self.current_model = candidates[0]
        logger.warning(f"[LLM] Ротация на модель: {self.current_model}")
        return True

    async def _call_groq(self, messages: List[Dict], response_format: Optional[Dict[str,str]] = None) -> Optional[str]:
        client = await self._ensure_client()
        kwargs = dict(
            messages=messages,
            model=self.current_model,
            max_tokens=4096,
            seed=42,
        )
        if response_format:
            kwargs["response_format"] = response_format

        start = time.monotonic()
        chat_completion = await client.chat.completions.create(**kwargs)
        elapsed = time.monotonic() - start
        logger.info(
            f"[LLM] Groq {self.current_model}: {elapsed:.2f}s, {chat_completion.choices[0].message} ,{chat_completion.usage.total_tokens if hasattr(chat_completion, 'usage') else '?'} tokens")

        content = chat_completion.choices[0].message.content
        if content:
            content = content.replace("```", "").replace("json", "").replace("\n\n", "\n")
        return content

    async def _call_with_rotation(self, messages: List[Dict], response_format: Optional[Dict[str,str]] = None) -> \
            Optional[str]:
        max_rotations = len(self.available_models) or 1
        for attempt in range(max_rotations):
            try:
                async with self._semaphore:
                    return await self._call_groq(messages, response_format)
            except (PermissionDeniedError, NotFoundError) as e:
                logger.warning(f"[LLM] Модель {self.current_model} недоступна ({e}). Ротация.")
                if not await self._rotate_model():
                    return None
            except RateLimitError as e:
                logger.warning(f"[LLM] Rate limit на {self.current_model}: {e}")
                if not await self._rotate_model():
                    return None
            except APITimeoutError:
                logger.warning(f"[LLM] Таймаут на {self.current_model}")
                # if attempt == max_rotations - 1:
                if not await self._rotate_model():
                    return None
            except BadRequestError as e:
                logger.error(f"[LLM] BadRequest: {e}")
                if not await self._rotate_model():
                    return None
            except AuthenticationError as e:
                logger.error(f"[LLM] AuthenticationError (не ретраим): {e}")
                return None
            except InternalServerError as e:
                logger.warning(f"[LLM] 5xx на {self.current_model}: {e}. Ротация.")
                if not await self._rotate_model():
                    return None
            except APIStatusError as e:
                logger.warning(f"[LLM] {e.status_code} на {self.current_model}: {e}. Ротация.")
                if not await self._rotate_model():
                    return None
            except Exception as e:
                logger.error(f"[LLM] Неизвестная ошибка на {self.current_model}: {e}")
                if attempt == max_rotations - 1:
                    return None
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
            news_for_prompt = [
                {
                    "id": i,
                    "title": news.get('title', ''),
                    "text": news.get('text', ''),
                    "url": news.get('url', '')
                }
                for i, news in enumerate(news_items)
            ]

            prompt = AD_DETECTION_PROMPT.format(
                news_data=json.dumps(news_for_prompt, ensure_ascii=False, indent=2)
            )

            response_text = await self._call_with_rotation(
                [
                    {"role": "system", "content": "Ты - эксперт по определению рекламного контента."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
            )



            if not response_text:
                logger.error("[LLM] Не удалось получить ответ от LLM для детекции рекламы")
                # Возвращаем дефолтные значения (не реклама)
                return [{"is_advertisement": True, "confidence": -1.0} for _ in news_items]

            try:
                results = json.loads(response_text)
                logger.info(f"[LLM] Реклама: {results}")
                # Приводим к списку если вернулся одиночный объект
                if results:
                    results = results.get("verdicts",[])

                # logger.info(f"[LLM] LLM ADVERT RESULT: {results}")
                return results

            except json.JSONDecodeError as e:
                logger.error(f"[LLM] Ошибка парсинга JSON в детекции рекламы: {e}")
                return [{"is_advertisement": True, "confidence": -1.0} for _ in news_items]

        except Exception as e:
            logger.error(f"[LLM] Ошибка при детекции рекламы: {e}")
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
            news_for_prompt = [
                {
                    "source": news.get("Source", ""),
                    "headline": news.get("Headline", ""),
                    "text": news.get("News_text", ""),
                    "url": news.get("News_URL", ""),
                    "publication_date": str(news.get("Publication_date", "")),
                    "has_image": news.get("Has_image", False)
                }
                for news in news_data
            ]

            prompt = INTEREST_SCORING_PROMPT.format(
                topic=topic,
                news_data=json.dumps(news_for_prompt, ensure_ascii=False, indent=2)
            )
            response_text = await self._call_with_rotation([
                {"role": "system", "content": f"Ты - эксперт по оценке новостей в теме: {topic}"},
                {"role": "user", "content": prompt}
            ],
                response_format={"type": "json_object"})

            if not response_text:
                logger.error("[LLM] Не удалось получить ответ от LLM после всех попыток")
                # Возвращаем дефолтные оценки
                for news in news_data:
                    news["Interest_score"] = float(0.00)
                    news["reason"] = "Ошибка получения оценки"
                return news_data

            logger.info(f"[LLM] Расчёт оценки полезности: {response_text}")
            results = json.loads(response_text)


            # if isinstance(results, dict):
            #     if "results" in results:
            #         results = results["results"]
            #     else:
            #         results = [results]
            # if not isinstance(results, list):
            #     results = [results]
            if results:
                results = results.get("verdicts", [])

            for news, result in zip(news_data, results):
                news["Interest_score"] = float(result["interest_score"])
                news["reason"] = result["reason"]
            return news_data

        except Exception as e:
            logger.error(f"[LLM] Ошибка при расчете оценки полезности: {e.args, e.__traceback__}")
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
            news_for_prompt = [
                {
                    "id": news.get("id", ""),
                    "source": news.get("source", ""),
                    "title": news.get("title", ""),
                    "text": news.get("text", ""),
                    "url": news.get("url", ""),
                    "image_url": news.get("image_url", ""),
                    "interest_score": news.get("interest_score", 0.0)
                }
                for news in news_items
            ]
            digest_type_format = {"daily": "Дневной", "weekly": "Недельный", "monthly": "Месячный"}
            prompt = DIGEST_PROCESSING_PROMPT.format(
                date=current_date,
                digest_type=digest_type_format.get(digest_type, 'Дневной'),
                news_data=json.dumps(news_for_prompt, ensure_ascii=False, indent=2)
            )
            response_text = await self._call_with_rotation([
                {"role": "system", "content": "Ты - профессиональный редактор дайджестов новостей."},
                {"role": "user", "content": prompt}
            ],
                response_format={"type": "json_object"})



            if not response_text:
                return {"digest_text": ""}

            result = json.loads(response_text)
            logger.info(f"[LLM] Дайджест: {result}")
            return {
                "digest_text": result.get("digest_text", "")
            }

        except Exception as e:
            logger.error(f"[LLM] Ошибка при обработке дайджеста: {e}")
            return {"digest_text": ""}


# Глобальный экземпляр для использования во всем приложении
# llm_client = LLMClient()

# Lazy initialization — не блокирует event loop при импорте
llm_client: Optional[LLMClient] = None


async def get_llm_client() -> LLMClient:
    global llm_client
    if llm_client is None:
        llm_client = LLMClient()
        await llm_client._fetch_available_models()
    return llm_client
