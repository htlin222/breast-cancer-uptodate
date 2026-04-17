"""Load all configuration from source/ YAML files."""

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

SOURCE_DIR = Path(__file__).parent.parent / "source"


def _load(filename: str) -> Any:
    return yaml.safe_load((SOURCE_DIR / filename).read_text())


@lru_cache(maxsize=None)
def keywords() -> list[str]:
    return _load("keywords.yml")["breast_cancer_keywords"]


@lru_cache(maxsize=None)
def drug_groups() -> dict[str, list[str]]:
    return _load("drug_groups.yml")["drug_groups"]


@lru_cache(maxsize=None)
def conference_keywords() -> list[str]:
    return _load("drug_groups.yml")["conference_keywords"]


@lru_cache(maxsize=None)
def search_queries() -> list[str]:
    return _load("search_queries.yml")["search_queries"]


@lru_cache(maxsize=None)
def web_sources() -> list[dict]:
    return _load("web_sources.yml")["sources"]


@lru_cache(maxsize=None)
def http_headers() -> dict[str, str]:
    return _load("web_sources.yml")["http_headers"]


@lru_cache(maxsize=None)
def twitter() -> dict:
    return _load("twitter.yml")["twitter"]


@lru_cache(maxsize=None)
def journals() -> list[dict]:
    return _load("journals.yml").get("journals", [])


@lru_cache(maxsize=None)
def crossref_email() -> str:
    return _load("journals.yml").get("crossref_email", "")
