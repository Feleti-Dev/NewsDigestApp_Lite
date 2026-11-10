from enum import Enum


class AddNewsResult(str, Enum):
    INSERTED = "inserted"
    DUPLICATE_URL = "duplicate_url"
    DUPLICATE_SEMANTIC = "duplicate_semantic"
    ERROR = "error"
