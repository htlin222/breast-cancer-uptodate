"""Fetch recent journal articles from CrossRef API and digest abstracts."""

import asyncio
import re
import yaml
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import httpx

from . import config

SOURCE_DIR = Path(__file__).parent.parent / "source"

JATS_TAG = re.compile(r"<[^>]+>")


@dataclass
class JournalArticle:
    title: str
    doi: str
    journal: str
    authors: list[str]
    published: Optional[str]
    abstract: str
    abstract_digest: str      # condensed key sentences
    tags: list[str] = field(default_factory=list)
    url: str = ""


def _load_journals() -> list[dict]:
    data = yaml.safe_load((SOURCE_DIR / "journals.yml").read_text())
    return data.get("journals", [])


def _crossref_email() -> str:
    data = yaml.safe_load((SOURCE_DIR / "journals.yml").read_text())
    return data.get("crossref_email", "")


def _clean_abstract(raw: str) -> str:
    """Strip JATS XML tags from CrossRef abstracts."""
    return re.sub(r"\s+", " ", JATS_TAG.sub("", raw)).strip()


def _digest_abstract(abstract: str, max_chars: int = 400) -> str:
    """Extract the most informative sentences from an abstract."""
    if not abstract:
        return ""
    # Prefer sentences containing result-signal words
    signal_words = [
        "significantly", "improved", "reduced", "increased", "demonstrated",
        "showed", "resulted", "HR ", "hazard ratio", "OS ", "PFS ", "ORR",
        "p=", "p<", "p >", "95% CI", "median", "months", "year",
        "approved", "primary endpoint", "statistically",
    ]
    sentences = re.split(r"(?<=[.!?])\s+", abstract)
    scored = []
    for s in sentences:
        score = sum(1 for w in signal_words if w.lower() in s.lower())
        scored.append((score, s))
    scored.sort(key=lambda x: -x[0])
    # Take highest-scoring sentences up to max_chars
    digest_parts = []
    total = 0
    for _, s in scored:
        if total + len(s) > max_chars:
            break
        digest_parts.append(s)
        total += len(s)
    return " ".join(digest_parts).strip() if digest_parts else abstract[:max_chars]


def _extract_tags(text: str) -> list[str]:
    kws = config.keywords()
    tl = text.lower()
    return list(dict.fromkeys(k for k in kws if k.lower() in tl))


def _is_bc_relevant(text: str) -> bool:
    tl = text.lower()
    return any(k.lower() in tl for k in config.keywords())


def _pub_date(item: dict) -> Optional[str]:
    parts = (
        item.get("published", {}).get("date-parts")
        or item.get("published-print", {}).get("date-parts")
        or item.get("published-online", {}).get("date-parts")
        or [[]]
    )
    dp = parts[0] if parts else []
    if len(dp) >= 3:
        return f"{dp[0]:04d}-{dp[1]:02d}-{dp[2]:02d}"
    if len(dp) == 2:
        return f"{dp[0]:04d}-{dp[1]:02d}"
    if len(dp) == 1:
        return f"{dp[0]:04d}"
    return None


async def _fetch_journal(
    client: httpx.AsyncClient,
    journal: dict,
    email: str,
) -> list[JournalArticle]:
    issn = journal["issn"]
    days_back = journal.get("days_back", 14)
    max_items = journal.get("max_items", 30)
    bc_filter = journal.get("bc_filter", True)
    from_date = (date.today() - timedelta(days=days_back)).isoformat()

    params = {
        "filter": f"issn:{issn},from-pub-date:{from_date}",
        "rows": max_items,
        "sort": "published",
        "order": "desc",
        "select": "DOI,title,author,abstract,published,published-print,published-online,URL,container-title",
    }
    headers = {
        "User-Agent": f"breast-cancer-uptodate/1.0 (mailto:{email})",
    }

    try:
        r = await client.get(
            "https://api.crossref.org/works",
            params=params,
            headers=headers,
            timeout=25,
        )
        r.raise_for_status()
    except Exception:
        return []

    articles = []
    for item in r.json().get("message", {}).get("items", []):
        title_list = item.get("title", [])
        title = title_list[0] if title_list else ""
        if not title or len(title) < 10:
            continue

        raw_abstract = item.get("abstract", "")
        abstract = _clean_abstract(raw_abstract)
        combined = title + " " + abstract

        if bc_filter and not _is_bc_relevant(combined):
            continue

        # Authors: last name only, max 4
        authors_raw = item.get("author", [])
        authors = [
            f"{a.get('family', '')} {a.get('given', '')[:1]}".strip()
            for a in authors_raw[:4]
        ]
        if len(authors_raw) > 4:
            authors.append("et al.")

        doi = item.get("DOI", "")
        pub = _pub_date(item)
        url = f"https://doi.org/{doi}" if doi else item.get("URL", "")
        journal_name = (item.get("container-title") or [journal.get("full_name", journal["issn"])])[0]

        articles.append(JournalArticle(
            title=title,
            doi=doi,
            journal=journal_name,
            authors=authors,
            published=pub,
            abstract=abstract,
            abstract_digest=_digest_abstract(abstract),
            tags=_extract_tags(combined),
            url=url,
        ))

    return articles


async def fetch_all() -> dict[str, list[JournalArticle]]:
    """Fetch articles from all configured journals. Returns {journal_name: [JournalArticle]}."""
    journals = _load_journals()
    email = _crossref_email()

    async with httpx.AsyncClient() as client:
        tasks = [_fetch_journal(client, j, email) for j in journals]
        results = await asyncio.gather(*tasks)

    return {j["name"]: arts for j, arts in zip(journals, results)}


def format_articles_md(results: dict[str, list[JournalArticle]]) -> str:
    """Render journal articles as a markdown section for the weekly report."""
    if not any(results.values()):
        return ""

    lines = ["\n## 文獻速報 — CrossRef 期刊\n"]
    lines.append("> 資料來源：CrossRef API · 僅顯示含摘要之乳癌相關論文\n")

    for journal_name, articles in results.items():
        if not articles:
            lines.append(f"\n### {journal_name}\n\n_本期未取得相關論文_\n")
            continue

        with_abstract = [a for a in articles if a.abstract_digest]
        without = [a for a in articles if not a.abstract_digest]

        lines.append(f"\n### {journal_name}（{len(articles)} 篇乳癌相關）\n")

        if with_abstract:
            for a in with_abstract[:10]:
                authors_str = ", ".join(a.authors)
                lines.append(f"#### [{a.title}]({a.url})")
                lines.append(f"_{authors_str}_ · {a.published or '—'} · {a.journal}")
                lines.append("")
                lines.append(f"> {a.abstract_digest}")
                if a.tags:
                    lines.append(f"\n`{'` `'.join(a.tags[:5])}`")
                lines.append("")

        if without:
            lines.append("**摘要未提供（CrossRef 未收錄）：**\n")
            for a in without[:8]:
                authors_str = ", ".join(a.authors)
                lines.append(f"- [{a.title}]({a.url}) — _{authors_str}_ ({a.published or '—'})")
            lines.append("")

    return "\n".join(lines)
