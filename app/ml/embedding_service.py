"""
Локальные dense-эмбеддинги (fastembed) для семантической дедупликации новостей.
"""

from __future__ import annotations

import asyncio
import logging
import re

import numpy as np

from app.configs.config import config

logger = logging.getLogger(__name__)

_model = None
_model_name_loaded: str | None = None


def reset_embedding_model() -> None:
    """Сброс кэша модели (после смены DEDUP_EMBEDDING_MODEL в конфиге)."""
    global _model, _model_name_loaded
    _model = None
    _model_name_loaded = None


def _get_model():
    global _model, _model_name_loaded
    model_name = config.app.dedup_embedding_model
    if _model is None or _model_name_loaded != model_name:
        from fastembed import TextEmbedding

        logger.info("Загрузка модели эмбеддингов fastembed: %s", model_name)
        _model = TextEmbedding(model_name=model_name, lazy_load=True)
        _model_name_loaded = model_name
    return _model


def normalize_news_text(headline: str, body: str, max_len: int = 2000) -> str:
    text = f"{headline or ''}\n{body or ''}".strip()
    text = re.sub(r"\s+", " ", text)
    return text[:max_len]


def _e5_passage(text: str) -> str:
    return f"passage: {text}"


def embed_batch(texts: list[str]) -> list[np.ndarray]:
    """Синхронный батч-эмбеддинг (вызывать из thread pool)."""
    if not texts:
        return []
    passages = [_e5_passage(t) for t in texts]
    model = _get_model()
    return [np.asarray(vec, dtype=np.float32) for vec in model.embed(passages)]


async def aembed_batch(texts: list[str]) -> list[np.ndarray]:
    return await asyncio.to_thread(embed_batch, texts)
