"""
Промпты для LLM API.
Загружаются из файла app/configs/prompts.json при инициализации.
"""

import json
import logging
import os

logger = logging.getLogger(__name__)

# Путь к файлу промптов
PROMPTS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "configs",
    "prompts.json",
)

# Глобальные переменные для промптов
AD_DETECTION_PROMPT: str = ""
INTEREST_SCORING_PROMPT: str = ""
DIGEST_PROCESSING_PROMPT: str = ""

# Кэш загруженных промптов
_prompts_cache: dict[str, str] = {}


def load_prompts() -> dict[str, str]:
    """
    Загрузка промптов из JSON файла.

    Returns:
        Словарь с промптами
    """
    global AD_DETECTION_PROMPT, INTEREST_SCORING_PROMPT, DIGEST_PROCESSING_PROMPT
    global _prompts_cache

    prompts = {}

    if not os.path.exists(PROMPTS_FILE):
        logger.warning(
            f"Файл промптов не найден: {PROMPTS_FILE}. Создаём с значениями по умолчанию..."
        )
        save_prompts(DEFAULT_PROMPTS.copy())
        prompts = DEFAULT_PROMPTS.copy()
    else:
        try:
            with open(PROMPTS_FILE, encoding="utf-8") as f:
                prompts = json.load(f)

            # Проверяем наличие всех ключей
            for key in [
                "AD_DETECTION_PROMPT",
                "INTEREST_SCORING_PROMPT",
                "DIGEST_PROCESSING_PROMPT",
            ]:
                if key not in prompts:
                    prompts[key] = DEFAULT_PROMPTS.get(key, "")
                    logger.warning(
                        f"Ключ {key} не найден в файле, используется значение по умолчанию"
                    )

        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON файла промптов: {e}")
            prompts = DEFAULT_PROMPTS.copy()
        except Exception as e:
            logger.error(f"Ошибка загрузки промптов: {e}")
            prompts = DEFAULT_PROMPTS.copy()

    # Присваиваем глобальным переменным
    AD_DETECTION_PROMPT = prompts.get("AD_DETECTION_PROMPT", "")
    INTEREST_SCORING_PROMPT = prompts.get("INTEREST_SCORING_PROMPT", "")
    DIGEST_PROCESSING_PROMPT = prompts.get("DIGEST_PROCESSING_PROMPT", "")

    _prompts_cache = prompts
    logger.info(f"Загружено {len(prompts)} промптов из {PROMPTS_FILE}")

    return prompts


def save_prompts(prompts: dict[str, str]) -> bool:
    """
    Сохранение промптов в JSON файл.

    Args:
        prompts: словарь с обновлениями промптов

    Returns:
        True если успешно
    """
    global _prompts_cache
    global AD_DETECTION_PROMPT, INTEREST_SCORING_PROMPT, DIGEST_PROCESSING_PROMPT
    # Существующие промпты
    existing_prompts = {}
    if os.path.exists(PROMPTS_FILE):
        try:
            with open(PROMPTS_FILE, encoding="utf-8") as f:
                existing_prompts = json.load(f)
        except Exception as e:
            logger.warning(f"Не удалось прочитать существующий файл промптов: {e}")
            existing_prompts = DEFAULT_PROMPTS.copy()

    # Обновляем значения (только разрешённые ключи)
    allowed_keys = [
        "AD_DETECTION_PROMPT",
        "INTEREST_SCORING_PROMPT",
        "DIGEST_PROCESSING_PROMPT",
    ]
    for key, value in prompts.items():
        if key in allowed_keys:
            existing_prompts[key] = value

    # Сохраняем в JSON
    try:
        with open(PROMPTS_FILE, "w", encoding="utf-8") as f:
            json.dump(existing_prompts, f, ensure_ascii=False, indent=4)

        logger.info(f"Сохранено {len(prompts)} промптов в {PROMPTS_FILE}")

        # Немедленно обновляем глобальные переменные
        AD_DETECTION_PROMPT = existing_prompts.get("AD_DETECTION_PROMPT", "")
        INTEREST_SCORING_PROMPT = existing_prompts.get("INTEREST_SCORING_PROMPT", "")
        DIGEST_PROCESSING_PROMPT = existing_prompts.get("DIGEST_PROCESSING_PROMPT", "")

        # Обновляем кэш
        _prompts_cache = existing_prompts

        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения промптов: {e}")
        return False


def update_prompt(key: str, value: str) -> bool:
    """
    Обновление одного промпта с немедленным применением.

    Args:
        key: имя промпта (AD_DETECTION_PROMPT, INTEREST_SCORING_PROMPT, DIGEST_PROCESSING_PROMPT)
        value: новое значение промпта

    Returns:
        True если успешно
    """
    global AD_DETECTION_PROMPT, INTEREST_SCORING_PROMPT, DIGEST_PROCESSING_PROMPT

    if key not in [
        "AD_DETECTION_PROMPT",
        "INTEREST_SCORING_PROMPT",
        "DIGEST_PROCESSING_PROMPT",
    ]:
        logger.error(f"Недопустимый ключ промпта: {key}")
        return False

    result = save_prompts({key: value})

    # if result:
    #     # Немедленно обновляем соответствующую глобальную переменную
    #     if key == 'AD_DETECTION_PROMPT':
    #         AD_DETECTION_PROMPT = value
    #     elif key == 'INTEREST_SCORING_PROMPT':
    #         INTEREST_SCORING_PROMPT = value
    #     elif key == 'DIGEST_PROCESSING_PROMPT':
    #         DIGEST_PROCESSING_PROMPT = value

    return result


def get_prompt(key: str) -> str | None:
    """
    Получение конкретного промпта.

    Args:
        key: имя промпта

    Returns:
        Значение промпта или None
    """
    global _prompts_cache

    if _prompts_cache:
        return _prompts_cache.get(key)

    load_prompts()
    return _prompts_cache.get(key)


def get_all_prompts() -> dict[str, str]:
    """
    Получение всех промптов.

    Returns:
        Словарь со всеми промптами
    """
    global _prompts_cache

    if not _prompts_cache:
        load_prompts()

    return _prompts_cache.copy()


def reset_prompts_to_default() -> bool:
    """
    Сброс промптов к значениям по умолчанию.

    Returns:
        True если успешно
    """
    return save_prompts(DEFAULT_PROMPTS.copy())


# Автоматическая загрузка при импорте модуля
_initialized = False


def initialize():
    """
    Инициализация промптов.
    """
    global _initialized

    if not _initialized:
        load_prompts()
        _initialized = True


# Значения по умолчанию для промптов - С АНГЛИЙСКИМИ ИНСТРУКЦИЯМИ
DEFAULT_PROMPTS = {
    "AD_DETECTION_PROMPT": """Ты - модератор технических новостных каналов. Определи, является ли следующий текст коммерческой рекламой.

Ты анализируешь контент из источников: Telegram каналы, YouTube каналы, Twitter/X разработчиков.

КОНТЕКСТ ИСТОЧНИКОВ:
1. Telegram каналы часто делятся полезными инструментами, open-source проектами
2. Разработчики делятся своими проектами на GitHub/Hugging Face
3. Образовательные каналы делятся бесплатными материалами
4. Это НЕ коммерческие площадки, а сообщества разработчиков

ВАЖНО ПРО YOUTUBE:
⚠️ Если YouTube-ролик по заголовку и основному тексту (описанию содержания) НЕ является прямой рекламой товара или услуги,
а в описании присутствуют:
- партнёрские ссылки
- реферальные ссылки
- ссылки на спонсоров канала
- любые рекламные ссылки, НЕ связанные напрямую с темой ролика
➡️ ТО такой контент НЕ считается рекламой.
➡️ В этом случае возвращай "is_advertisement": false и низкое значение confidence (например, 0.0–0.2).

РЕКЛАМА (должно быть 2+ признака):
✅ Прямой призыв к покупке: "Купите наш курс", "Закажите услугу"
✅ Цены и скидки: "Всего 999 рублей", "Скидка 50%"
✅ Ограниченное предложение: "Только до конца месяца", "Успейте записаться"
✅ Призыв к действию: "Жмите на ссылку чтобы купить", "Оформляйте подписку на сервис уже сейчас"

НЕ РЕКЛАМА:
❌ Упоминание open-source проектов (GitHub, Hugging Face, Kaggle)
❌ Ссылки на бесплатные инструменты и библиотеки, курсы и книги
❌ Образовательные материалы, туториалы, документация
❌ Анонсы новых технологий, релизов, обновлений
❌ Обзоры, сравнения технологий без предложения купить
❌ YouTube-видео, где реклама присутствует только в виде сторонних ссылок и не является сутью ролика

ПРИМЕРЫ НЕ РЕКЛАМЫ из нашего контекста:
- "Resemble AI на Hugging Face: улучшение качества звука"
- "Новый курс от DeepLearning.AI на Coursera"
- "Вышел TensorFlow 2.15 с новыми фичами"
- "Бесплатный воркшоп по LangChain"
- "Обзор новой архитектуры LLM (с партнёрскими ссылками в описании)"

Примеры рекламы:
- "Купите наш курс по Python за 9990 рублей"
- "Закажите настройку нейросети: telegram @sales"
- "Скидка 50% на подписку до конца месяца"
- "Нужны 5 разработчик для создания приложения"

Проанализируй следующий список новостей и определи, какие из них являются рекламой:

{news_data}

Анализируй КРИТИЧЕСКИ. Если есть сомнения - не реклама.

Верни результат в виде JSON массива объектов.

Пример ответа:
[
  {{"is_advertisement": false, "confidence": 0.95}},
  {{"is_advertisement": true, "confidence": 0.85}}
]

Не добавляй никаких пояснений, других переменных, только JSON.

**ВАЖНО**:
1. Верни ровно столько элементов в массиве, сколько новостей было передано
2. Порядок элементов в ответе должен соответствовать порядку новостей во входных данных
3. Не добавляй никаких пояснений, только JSON массив.
4. Не используй блоки кода ```json или ```
5. Не добавляй никакого текста кроме JSON
""",
    "INTEREST_SCORING_PROMPT": """Ты - эксперт по оценке новостей в теме: {topic}.
Оцени полезность каждой новости относительно данной тематики по шкале от 0.000 до 0.999.

Шкала оценок:
0.900-0.999: Прорывные технологии, новые продукты, сервисы и архитектуры связанные конкретно с заданной тематикой
0.700-0.899: Лайфхаки, полезные инструменты, готовые шаблоны связанные конкретно с заданной тематикой
0.500-0.699: Полезные уроки, видео, книги, бесплатные курсы связанные конкретно с заданной тематикой
0.300-0.499: Платные курсы, не особо важные новости связанные конкретно с заданной тематикой
0.100-0.299: Около-тематические новости, общие обсуждения связанные конкретно с заданной тематикой
0.000-0.099: Не связанные с темой новости, спам, реклама, смешные ситуации, мемы, анекдоты, юмор

Если новость не относится к данной тематике: {topic}, то ставить ей 0.000-0.099.

Данные новостей (массив JSON):
{news_data}

Верни ответ ТОЛЬКО в формате JSON массива: [{{"interest_score": число от 0.000 до 0.999, "reason": "краткое объяснение"}},{{...}}]
Не добавляй никаких пояснений, только JSON.

ВАЖНО:
1. Верни ровно столько элементов в массиве, сколько новостей было передано
2. Порядок элементов в ответе должен соответствовать порядку новостей во входных данных
3. Не добавляй никаких пояснений, только JSON массив.
4. Не используй блоки кода ```json или ```
5. Не добавляй никакого текста кроме JSON
""",
    "DIGEST_PROCESSING_PROMPT": """Ты - профессиональный редактор дайджестов. Обработай следующие новости:
1. Переведи весь текст новостей и заголовки новостей на русский язык. Английских новостей быть не должно
2. Убери дублирующие новости
3. Суммаризируй каждую новость до 3-5 предложений. Если нет текста у новости, сгенерируй в таком-же объеме на основе заголовка.
4. Соблюди все требования к формату
5. Отформатируй в Telegram HTML для Telegram


Требования к формату:
- Для каждой новости такая последовательная структура: любое эмодзи подходящее под заголовок в начале заголовка , заголовок новости(жирный текст, обязательно переведённый на русский язык), абзац, краткое содержание (суммаризация новости до 3-5 предложений), абзац, ссылка на исходник новости (4 варианта: Telegram, Youtube, Twitter, Reddit) в виде прямой ссылки
- Общий заголовок: "📰 {digest_type} дайджест новостей за {date}:"
- Ограничение в телеграмме  4000 символов, поэтому можно сильно не сжимать текст
- Если видишь текст в таком формате "[Higgsfield Relight Studio](https://higgsfield.ai/app/relight)", то переделай в такой формат "<a href="https://higgsfield.ai/app/relight">Higgsfield Relight Studio</a>"
- Не делай лишние абзацы. 1 Абзац только после общего заголовка и 1 абзац между новостями.
Пример готовых новостей (нельзя отходит от этого стиля):
"📰 <b> Месячный дайджест новостей за 22.01.2026: </b>

🚀 <b>GLM-4.7 Flash: новая модель для локального запуска</b>
GLM-4.7 Flash - это облегченная версия GLM-4.7 на 30 млрд. параметров с контекстным окном в 128К на архитектуре MoE. Модель должна занять нишу между сегментом SLM и проприетарными мастодонтами, предлагая SOTA-уровень в кодинге. Модель имеет 3 млрд активных параметров на токен и поддерживает Interleaved Thinking.
Источник: <a href="https://t.me/ai_machinelearning_big_data/9409">Telegram</a>

🤖 <b>Jensen Huang: компьютер преобразуется в самообучающуюся систему</b>
Jensen Huang считает, что компьютер преобразуется из системы, которую мы программируем, в систему, которая сама себя программирует. Скоро мы сможем просто сказать компьютеру, чтобы он пошел учиться.
Источник: <a href="https://t.me/c/1526709058/5686">Telegram</a>

👀 <b><a href="https://higgsfield.ai/app/relight">Higgsfield Relight Studio</a> — нейросеть меняет направление света на любой фотке</b>
Нейросеть Higgsfield Relight Studio позволяет менять направление света на любой фотографии. Просто загрузите свою фотографию, выберите цвет, направление и мягкость — и получите фотографию с новым освещением.
Источник: <a href="https://t.me/ii_community/6078">Telegram</a>

💻 <b>Zhipu AI: GLM-4.7-Flash — MoE-модель на 30B параметров</b>
Zhipu AI выложила GLM-4.7-Flash — MoE-модель на 30B параметров с 3B активными. Позиционируют как лучшую в своём классе. Модель бьёт Qwen3-30B-A3B почти везде, особенно на агентских задачах.
Источник: <a href="https://www.youtube.com/watch?v=f70Qro3eOpo">YouTube</a>

💸 <b>О локальной Claude Code</b>
Ollama с версии 0.14 поддерживает Anthropic Messages API, что позволяет использовать Claude Code с локальными моделями.
Источник: <a href="https://t.me/llm_under_hood/734">Telegram</a>

🤔 <b>Nvidia и KV-кэш: решение проблемы масштабирования контекста</b>
Nvidia разработала решение для проблемы KV-кэша, которая возникает при масштабировании контекста в трансформерах. Решение позволяет сжимать кэш до 4 раз без потерь качества.
Источник: <a href="https://t.me/devsp/5446">Telegram</a>"

Новости для обработки (в формате JSON):
{news_data}

**ИНСТРУКЦИЯ ПО ФОРМАТУ ОТВЕТА (КРИТИЧЕСКИ ВАЖНО):**
1. Strict JSON: Верни ответ СТРОГО в формате JSON. Запрещено использовать Markdown-разметку (```json), комментарии или пояснительный текст. Ответ должен начинаться с {{ и заканчиваться }}.
2. Экранирование (JSON Escaping): Весь текст внутри значения "digest_text" должен быть корректно подготовлен для функции json.loads() в Python:
   - Все двойные кавычки внутри текста ОБЯЗАТЕЛЬНО экранируй: \\".
   - ЗАПРЕЩЕНЫ реальные переносы строк. Вместо них используй литерал \\n.
   - ЗАПРЕЩЕНЫ реальные символы табуляции.
3. Telegram HTML:
   - Разрешены ТОЛЬКО теги:
     <b>жирный</b>
     <i>Курсив</i>
     <code>код</code>
     <s>перечеркнутый</s>
     <u>подчеркнутый</u>
     <pre language="c++">код</pre>
     <a href="smth.ru">Сайт</a>.
   - Любые другие теги (h1-h6, br, div) ЗАПРЕЩЕНЫ.
   - Используй ОДИНАРНЫЕ кавычки для HTML-атрибутов внутри текста, например: <a href='url'>. Это предотвратит конфликт с двойными кавычками JSON.
4. Валидация: Ответ будет обработан функцией json.loads(). Любая ошибка синтаксиса сделает ответ бесполезным. Если возникнет ошибка JSONDecodeError, задача считается проваленной.

Верни ответ ТОЛЬКО в формате JSON: {{"digest_text": "полный текст дайджеста в Telegram HTML"}}""",
}

# Вызываем инициализацию при импорте
initialize()
