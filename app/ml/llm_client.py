# app/llm/llm_client.py
"""
Клиент для работы с LLM API (Groq)
"""

import json, logging, time, asyncio, re
from datetime import datetime
from typing import Dict, Any, List, Optional, Set

from instructor import Instructor
from instructor.v2.core.errors import InstructorRetryException
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
import instructor
from pydantic import BaseModel, Field
import httpx

import sys, pathlib
sys.path.append(pathlib.Path(__file__).parent.parent.parent.absolute().__str__())

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



class DigestPart(BaseModel):
    header: str = Field(description="Заголовок одной новости дайджеста",
                        min_length=30,
                        max_length=100,
                        examples=["🚀 <b>Вышла GigaChat 3.5 Ultra в открытом доступе</b>"])
    body: str = Field(description="Основное тело новости",
                      min_length=50,
                      max_length=400,
                      examples=["Сбер выпустил в open source новую модель GigaChat 3.5 Ultra под лицензией MIT. Модель весит 432 млрд параметров,генерирует длинные тексты в 4 раза быстрее. Архитектура, сочетающая MLA и GatedDeltaNet, увеличивает вдвое доступный контекст при том же объёме памяти. Модель превосходит аналоги в задачах кодинга, математики и автономных агентских сценариях. Веса доступны на HuggingFace и GitVerse."])
    source: str = Field(description="Источник новости",
                        min_length=30,
                        max_length=80,
                        examples=["Источник: <a href='https://t.me/c/1848122804/11334'>Telegram</a>"])

class Advert(BaseModel):
    id: int = Field(description="Порядковый номер новости в наборе")
    is_advertisement: bool = Field(description="Является ли новость рекламой")
    confidence: float = Field(description="Показатель уверенности", ge=0.0, le=1.0)


class Score(BaseModel):
    id: int = Field(description="Порядковый номер новости в наборе")
    interest_score: float = Field(description="Показатель интереса новости", ge=0.0, le=1.0)
    reason: str = Field(description="Краткое объяснение выставленного показателя")


class Digest(BaseModel):
    reasoning: str = Field(description="Рассуждение модели", min_length=10)
    digest: list[DigestPart] = Field(description="Готовые к публикации новости дайджеста в формате HTML. В общей сумме не более 4096 символов.",
                                     min_length=1,
                                     max_length=config.app.max_news_per_digest)
    # mainHeader: str = Field(description="Главный заголовок дайджеста",
    #                         examples=["📰 <b> Месячный дайджест новостей за 22.01.2026: </b>"],
    #                         min_length=40,
    #                         max_length=60)
class AdvertOutput(BaseModel):
    reasoning: str = Field(description="Рассуждение модели", min_length=10)
    verdicts: list[Advert] = Field(description="Набор вердиктов определения рекламы", min_length=1)


class ScoreOutput(BaseModel):
    reasoning: str = Field(description="Рассуждение модели", min_length=10)
    verdicts: list[Score] = Field(description="Набор вердиктов интересов новостей", min_length=1)


class LLMClient:
    """Клиент для работы с Groq API"""

    def __init__(self):
        self.base_url = "http://localhost:8080/v1" #"https://api.groq.com/openai/v1" #"http://localhost:8080/v1"  #  #"https://ai.feleti.by/llm/v1"
        self.api_key: str = config.api.groq_api_key or ""
        self._client: AsyncOpenAI = None
        self._instructor_client:  instructor.AsyncInstructor = None
        self.available_models: List[str] = []
        self.current_model: str = ""
        self._banned_models: Set[str] = set()
        self._models_last_fetch: float = 0
        self._models_cache_ttl: int = 3600  # 1 час
        self._semaphore = asyncio.Semaphore(2)

    async def _ensure_client(self) -> instructor.AsyncInstructor:
        if self._client is None:
            self._client =  AsyncOpenAI(
                base_url=self.base_url,
                api_key=self.api_key,
                max_retries=6,
                timeout=httpx.Timeout(120.0, connect=10.0),
                default_headers={"User-Agent": "NewsDigestApp/1.0"},
            )
            self._instructor_client: instructor.AsyncInstructor =\
                instructor.from_openai(self._client,
                mode=instructor.Mode.TOOLS
                # mode=instructor.Mode.JSON_SCHEMA
                )

        return self._instructor_client

    async def _fetch_available_models(self) -> List[str]:
        now = time.time()
        if self.available_models and (now - self._models_last_fetch) < self._models_cache_ttl:
            return self.available_models

        try:
            await self._ensure_client()
            data = await self._client.models.with_raw_response.list()
            json_data = data.http_response.json().get("data",{})
            models = [m["id"]
                              for m in json_data
                              if m["id"] not in BLOCKED_MODELS
                              # and m.get('context_window',None)
                              # and m.get('max_output_length', No
                      # ne)
                              # and m['context_window'] > 8191
                              # and m['max_output_length'] > 8191
                      ]

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

    async def _call_groq(self, messages: List[Dict], response_format: type[BaseModel]) -> type[BaseModel] | list | None:
        client = await self._ensure_client()
        kwargs = dict(
            messages=messages,
            model=self.current_model,
            # max_tokens=4096,
            seed=42,
            response_model=response_format,
            temperature=0.2,
            max_retries=3
        )
        # if response_format:
            # kwargs["response_format"] = response_format

        start = time.monotonic()
        chat_completion = await client.completions.create(**kwargs)
        elapsed = time.monotonic() - start
        logger.info(
            f"[LLM] Response: {self.current_model}: {elapsed:.2f}s, {chat_completion}" #{chat_completion.choices[0].message} ,{chat_completion.usage.total_tokens if hasattr(chat_completion, 'usage') else '?'} tokens")
        )
        content = chat_completion
        if isinstance(content, str):
            content = content.replace("```", "").replace("json", "").replace("\n\n", "\n")
        return content

    async def _call_with_rotation(self, messages: List[Dict], response_format: type[BaseModel]) -> \
            type[BaseModel] | list | None:
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
            except InstructorRetryException as e:

                logger.warning(f"[LLM] Failed after {e.n_attempts} attempts")
                logger.warning(f"[LLM] Total tokens used: {e.total_usage}")
                logger.warning(f"[LLM] Model used: {e.create_kwargs.get('model')}")
                # Inspect failed attempts
                for attempt in e.failed_attempts:
                    logger.warning(f"[LLM] Attempt {attempt.attempt_number}: {attempt.exception}")

                if attempt == max_rotations - 1:
                    return None
                if not await self._rotate_model():
                    return None

            except Exception as e:
                logger.error(f"[LLM] Неизвестная ошибка на {self.current_model}: {e}")
                if attempt == max_rotations - 1:
                    return None
                if not await self._rotate_model():
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

            response_text: AdvertOutput = await self._call_with_rotation(
                [
                    {"role": "system", "content": "Ты - эксперт по определению рекламного контента."},
                    {"role": "user", "content": prompt}
                ],
                response_format=AdvertOutput,
            )

            if not response_text:
                logger.error("[LLM] Не удалось получить ответ от LLM для детекции рекламы")
                # Возвращаем дефолтные значения (не реклама)
                return [{"is_advertisement": True, "confidence": -1.0} for _ in news_items]

            # try:
                # results = json.loads(response_text)
            logger.info(f"[LLM] Реклама: {response_text}")
                # Приводим к списку если вернулся одиночный объект
                # if results:
                #     results = results.get("verdicts", [])

                # logger.info(f"[LLM] LLM ADVERT RESULT: {results}")
            return [advert.model_dump() for advert in response_text.verdicts]

            # except json.JSONDecodeError as e:
            #     logger.error(f"[LLM] Ошибка парсинга JSON в детекции рекламы: {e}")
            #     return [{"is_advertisement": True, "confidence": -1.0} for _ in news_items]

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
                    "id": i,
                    "source": news.get("Source", ""),
                    "headline": news.get("Headline", ""),
                    "text": news.get("News_text", ""),
                    "url": news.get("News_URL", ""),
                    "publication_date": str(news.get("Publication_date", "")),
                    "has_image": news.get("Has_image", False)
                }
                for i, news in enumerate(news_data)
            ]

            prompt = INTEREST_SCORING_PROMPT.format(
                topic=topic,
                news_data=json.dumps(news_for_prompt, ensure_ascii=False, indent=2)
            )
            response_text: ScoreOutput = await self._call_with_rotation([
                {"role": "system", "content": f"Ты - эксперт по оценке новостей в теме: {topic}"},
                {"role": "user", "content": prompt}
            ],
                response_format=ScoreOutput)

            if not response_text:
                logger.error("[LLM] Не удалось получить ответ от LLM после всех попыток")
                # Возвращаем дефолтные оценки
                for news in news_data:
                    news["Interest_score"] = float(0.00)
                    news["reason"] = "Ошибка получения оценки"
                return news_data

            logger.info(f"[LLM] Расчёт оценки полезности: {response_text}")
            # results = json.loads(response_text)

            # if isinstance(results, dict):
            #     if "results" in results:
            #         results = results["results"]
            #     else:
            #         results = [results]
            # if not isinstance(results, list):
            #     results = [results]
            # if results:
            #     results = results.get("verdicts", [])

            for news, result in zip(news_data, response_text.verdicts):
                news["Interest_score"] = float(result.interest_score)
                news["reason"] = result.reason
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
            response_text: Digest = await self._call_with_rotation([
                {"role": "system", "content": "Ты - профессиональный редактор дайджестов новостей."},
                {"role": "user", "content": prompt}
            ],
                response_format=Digest)
            #{"type": "json_object"})

            if not response_text.digest:
                return {"digest_text": ""}

            # result = json.loads(response_text)
            #logger.info(f"[LLM] Дайджест: {response_text}")

            digest_text = f"📰 <b> {digest_type_format.get(digest_type, 'Дневной')} дайджест новостей за {current_date}: </b>" + "\n\n"
            for digest_part in response_text.digest:
                digest_text += digest_part.header + "\n" + digest_part.body + "\n" + digest_part.source + "\n\n"

            logger.info(f"[LLM] Собранный дайджест:\n {digest_text}")
            return {
                "digest_text": digest_text
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
        await llm_client._ensure_client()
        await llm_client._fetch_available_models()
    return llm_client

if __name__ == "__main__":
    async def main():
        llm_client = await get_llm_client()


    asyncio.run(main=main())

    print(json.dumps(AdvertOutput.model_json_schema(), indent=3))
    # print(AdvertOutput.schema())
    # print(AdvertOutput.schema_json())
    # # print(AdvertOutput.json())
    # print(AdvertOutput.model_config)
    # print(AdvertOutput.model_dump())
    # print(AdvertOutput.model_dump_json())