"""ML модули для оценки новостей"""

import logging

from .llm_client import LLMClient, get_llm_client, llm_client

logging.getLogger("instructor.v2").setLevel(logging.DEBUG)
logging.getLogger("httpx").setLevel(logging.DEBUG)


__all__ = ["LLMClient", "get_llm_client", "llm_client"]
