"""Scrape latest breast cancer articles from configured web sources."""

import asyncio
import re
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from . import config


@dataclass
class Article:
    title: str
    url: str
    source: str
    published: Optional[str] = None
    summary: str = ""
    tags: list[str] = field(default_factory=list)


def _is_bc_relevant(text: str) -> bool:
    tl = text.lower()
    return any(kw.lower() in tl for kw in config.keywords())


def _extract_tags(text: str) -> list[str]:
    tl = text.lower()
    return list(dict.fromkeys(kw for kw in config.keywords() if kw.lower() in tl))


def _rfc_to_iso(rfc_date: str) -> Optional[str]:
    try:
        return parsedate_to_datetime(rfc_date).date().isoformat()
    except Exception:
        return None


def _parse_rss_items(xml_text: str, source_name: str, bc_filter: bool = True) -> list[Article]:
    soup = BeautifulSoup(xml_text, "lxml-xml")
    articles = []
    for item in soup.find_all("item"):
        title_el = item.find("title")
        title = title_el.get_text(strip=True) if title_el else ""
        if not title or len(title) < 10:
            continue

        link_el = item.find("link")
        url = ""
        if link_el:
            url = link_el.get_text(strip=True)
            if not url:
                sib = link_el.next_sibling
                url = sib.strip() if isinstance(sib, str) else ""

        pubdate_el = item.find("pubDate")
        pub = _rfc_to_iso(pubdate_el.get_text(strip=True)) if pubdate_el else None

        desc_el = item.find("description") or item.find("content:encoded")
        summary = ""
        if desc_el:
            raw = desc_el.get_text(strip=True)
            summary = BeautifulSoup(raw, "html.parser").get_text()[:300]

        combined = title + " " + summary
        if bc_filter and not _is_bc_relevant(combined):
            continue

        articles.append(Article(
            title=title,
            url=url,
            source=source_name,
            published=pub,
            summary=summary.strip(),
            tags=_extract_tags(combined),
        ))
    return articles


async def _fetch_rss(client: httpx.AsyncClient, src: dict) -> list[Article]:
    try:
        r = await client.get(src["url"], timeout=20)
        if r.status_code != 200:
            return []
        return _parse_rss_items(
            r.text,
            src["name"],
            bc_filter=src.get("bc_filter", True),
        )
    except Exception:
        return []


async def _fetch_google_news(client: httpx.AsyncClient, src: dict) -> list[Article]:
    domain = src["domain"]
    max_items = src.get("max_items", 20)
    noise_pat = re.compile(src["noise_filter"], re.I) if src.get("noise_filter") else None

    feed_url = (
        f"https://news.google.com/rss/search"
        f"?q=site:{domain}+breast+cancer&hl=en-US&gl=US&ceid=US:en"
    )
    try:
        r = await client.get(feed_url, timeout=20)
        if r.status_code != 200:
            return []
    except Exception:
        return []

    soup = BeautifulSoup(r.text, "lxml-xml")
    articles = []

    for item in soup.find_all("item")[:max_items]:
        title_el = item.find("title")
        title = title_el.get_text(strip=True) if title_el else ""
        if not title or len(title) < 10:
            continue

        # Strip " - SourceName" suffix appended by Google News
        title = re.sub(r"\s*-\s*[^-]{3,40}$", "", title).strip()

        link_el = item.find("link")
        url = ""
        if link_el:
            sib = link_el.next_sibling
            url = sib.strip() if isinstance(sib, str) else link_el.get_text(strip=True)

        pubdate_el = item.find("pubDate")
        pub = _rfc_to_iso(pubdate_el.get_text(strip=True)) if pubdate_el else None

        if noise_pat and noise_pat.search(title):
            continue
        if not _is_bc_relevant(title):
            continue

        articles.append(Article(
            title=title,
            url=url,
            source=src["name"],
            published=pub,
            summary="",
            tags=_extract_tags(title),
        ))

    return articles


async def fetch_all(days: int = 7) -> dict[str, list[Article]]:
    """Fetch articles from all configured sources. Returns {source_name: [Article]}."""
    headers = config.http_headers()
    sources = config.web_sources()

    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        tasks = []
        for src in sources:
            if src["type"] == "rss":
                tasks.append(_fetch_rss(client, src))
            elif src["type"] == "google_news":
                tasks.append(_fetch_google_news(client, src))

        results_list = await asyncio.gather(*tasks)

    return {src["name"]: arts for src, arts in zip(sources, results_list)}


def format_articles_md(results: dict[str, list[Article]]) -> str:
    """Render scraped articles as a markdown section for the weekly report."""
    source_names = " / ".join(results.keys())
    lines = [f"\n## 媒體動態 — {source_names}\n"]
    for source, articles in results.items():
        if not articles:
            lines.append(f"\n### {source}\n\n_本週未取得相關文章_\n")
            continue
        lines.append(f"\n### {source}（{len(articles)} 篇乳癌相關）\n")
        lines.append("| 標題 | 日期 | 關鍵詞 |")
        lines.append("|------|------|--------|")
        for a in articles[:15]:
            date_str = a.published or "—"
            tags_str = ", ".join(a.tags[:4])
            title_md = f"[{a.title[:80]}]({a.url})"
            lines.append(f"| {title_md} | {date_str} | {tags_str} |")
        lines.append("")
    return "\n".join(lines)
