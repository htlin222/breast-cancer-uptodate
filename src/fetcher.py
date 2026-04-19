import asyncio
import json
import random
from pathlib import Path
from twscrape import AccountsPool, API
from twscrape.xclid import XClIdGen
from twscrape.queue_client import XClIdGenStore
import twscrape.api as _twapi
from rich.console import Console
from . import db, config

console = Console()
POOL_DB = Path(__file__).parent.parent / "data" / "accounts_pool.db"

# Per-query timeout: if no tweets arrive within this many seconds, give up on that query.
_QUERY_TIMEOUT_SEC = 30
# Probe timeout: quick check before running all queries.
_PROBE_TIMEOUT_SEC = 20


def _build_cookie_string(auth_token: str, ct0: str) -> str:
    cookies_file = Path(__file__).parent.parent / "cookies.json"
    skip = set(config.twitter()["cookie_skip"])
    if cookies_file.exists():
        raw = json.loads(cookies_file.read_text())
        return "; ".join(f"{c['name']}={c['value']}" for c in raw if c["name"] not in skip)
    return f"auth_token={auth_token}; ct0={ct0}"


def _patch_twscrape():
    op_id = config.twitter()["op_id"]
    _twapi.OP_SearchTimeline = f"{op_id}/SearchTimeline"


def _make_stub(username: str) -> XClIdGen:
    stub_vk = [random.randint(0, 255) for _ in range(32)]
    gen = XClIdGen(stub_vk, "stub_anim_key")
    XClIdGenStore.items[username] = gen
    return gen


def _patch_xclid_store(username: str):
    """
    Monkeypatch XClIdGenStore.get to catch ALL exceptions (not just HTTPStatusError)
    when refreshing the transaction ID, and fall back to the stub already in items[].
    This prevents IndexError in xclid.get_scripts_list() from causing infinite
    15-minute retry loops when Twitter returns 404 for SearchTimeline.
    """
    original_get = XClIdGenStore.get.__func__

    @classmethod  # type: ignore[misc]
    async def _safe_get(cls, uname: str, fresh: bool = False) -> XClIdGen:
        if uname in cls.items and not fresh:
            return cls.items[uname]
        try:
            clid_gen = await XClIdGen.create()
            cls.items[uname] = clid_gen
            return clid_gen
        except Exception as e:
            console.print(f"[yellow]  ⚠ XClIdGen refresh failed ({type(e).__name__}), using stub[/yellow]")
            if uname in cls.items:
                return cls.items[uname]
            return _make_stub(uname)

    XClIdGenStore.get = _safe_get


async def _init_xclid(username: str, cookie_dict: dict):
    import httpx
    headers = {
        "User-Agent": config.http_headers()["User-Agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        async with httpx.AsyncClient(headers=headers, cookies=cookie_dict, follow_redirects=True) as client:
            gen = await XClIdGen.create(clt=client)
            XClIdGenStore.items[username] = gen
            console.print("[green]  ✓ x-client-transaction-id computed[/green]")
    except Exception as e:
        console.print(f"[yellow]  ⚠ XClIdGen failed ({type(e).__name__}: {e}), using stub[/yellow]")
        _make_stub(username)

    # Patch store AFTER stub is guaranteed to be set
    _patch_xclid_store(username)


async def _collect(api: API, query: str, limit: int) -> list:
    tweets = []
    async for tw in api.search(query, limit=limit):
        tweets.append(tw)
    return tweets


async def _search_query(api: API, query: str, limit: int) -> list:
    try:
        return await asyncio.wait_for(_collect(api, query, limit), timeout=_QUERY_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        console.print(f"[yellow]  ⚠ query timed out after {_QUERY_TIMEOUT_SEC}s[/yellow]")
        return []
    except Exception as e:
        console.print(f"[yellow]  ⚠ query failed: {type(e).__name__}: {e}[/yellow]")
        return []


async def _probe(api: API) -> bool:
    """Return True if Twitter search responds within timeout."""
    try:
        result = await asyncio.wait_for(_collect(api, "breast cancer", limit=1), timeout=_PROBE_TIMEOUT_SEC)
        return True  # even empty list is OK — at least it didn't hang
    except asyncio.TimeoutError:
        return False
    except Exception:
        return False


async def _run_fetch(username: str, email: str, auth_token: str, ct0: str):
    db.init_db()
    _patch_twscrape()

    skip = set(config.twitter()["cookie_skip"])
    cookies_file = Path(__file__).parent.parent / "cookies.json"
    if cookies_file.exists():
        raw = json.loads(cookies_file.read_text())
        cookie_dict = {c["name"]: c["value"] for c in raw if c["name"] not in skip}
    else:
        cookie_dict = {"auth_token": auth_token, "ct0": ct0}

    console.print("[cyan]Computing x-client-transaction-id...[/cyan]")
    await _init_xclid(username, cookie_dict)

    api = await _setup_pool(username, email, auth_token, ct0)

    console.print("[cyan]Probing Twitter search...[/cyan]")
    if not await _probe(api):
        console.print(
            "[red]✗ Twitter search blocked or unreachable "
            "(IP restriction / expired session / changed API).[/red]\n"
            "[dim]Skipping fetch. Other data sources (CrossRef, OncDaily, ESMO) still work.[/dim]"
        )
        return

    console.print("[green]  ✓ Search responsive[/green]")
    queries = config.search_queries()
    limit = config.twitter().get("per_query_limit", 100)

    total = 0
    for i, query in enumerate(queries, 1):
        console.print(f"[cyan]Query {i}/{len(queries)}:[/cyan] {query[:70]}...")
        tweets = await _search_query(api, query, limit=limit)
        for tw in tweets:
            author = tw.user.username if tw.user else "unknown"
            db.upsert_account(
                handle=author,
                display_name=tw.user.displayname if tw.user else "",
                bio=tw.user.rawDescription if tw.user else "",
                followers=tw.user.followersCount if tw.user else 0,
                discovered_via="search",
            )
            db.upsert_tweet(
                tweet_id=str(tw.id),
                author=author,
                content=tw.rawContent,
                created_at=tw.date.isoformat(),
                likes=tw.likeCount or 0,
                retweets=tw.retweetCount or 0,
                url=tw.url,
            )
        console.print(f"  [green]{len(tweets)} tweets[/green]")
        total += len(tweets)

    console.print(f"\n[bold green]✓ Fetched {total} tweets total[/bold green]")


async def _setup_pool(username: str, email: str, auth_token: str, ct0: str) -> API:
    if POOL_DB.exists():
        POOL_DB.unlink()
    pool = AccountsPool(POOL_DB)
    cookies = _build_cookie_string(auth_token, ct0)
    await pool.add_account(username, "placeholder", email, "", cookies=cookies)
    return API(pool)


def fetch(username: str, email: str, auth_token: str, ct0: str):
    asyncio.run(_run_fetch(username, email, auth_token, ct0))
