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


async def _init_xclid(username: str, cookie_dict: dict):
    import httpx
    headers = {
        "User-Agent": config.http_headers()["User-Agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    }
    try:
        async with httpx.AsyncClient(headers=headers, cookies=cookie_dict, follow_redirects=True) as client:
            gen = await XClIdGen.create(clt=client)
            XClIdGenStore.items[username] = gen
            console.print("[green]  ✓ x-client-transaction-id computed[/green]")
    except Exception as e:
        console.print(f"[yellow]  ⚠ XClIdGen failed ({e}), using stub[/yellow]")
        stub_vk = [random.randint(0, 255) for _ in range(32)]
        XClIdGenStore.items[username] = XClIdGen(stub_vk, "stub_anim_key")


async def _setup_pool(username: str, email: str, auth_token: str, ct0: str) -> API:
    if POOL_DB.exists():
        POOL_DB.unlink()
    pool = AccountsPool(POOL_DB)
    cookies = _build_cookie_string(auth_token, ct0)
    await pool.add_account(username, "placeholder", email, "", cookies=cookies)
    return API(pool)


async def _search_query(api: API, query: str, limit: int) -> list:
    tweets = []
    try:
        async for tw in api.search(query, limit=limit):
            tweets.append(tw)
    except Exception as e:
        console.print(f"[yellow]  ⚠ query failed: {e}[/yellow]")
    return tweets


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


def fetch(username: str, email: str, auth_token: str, ct0: str):
    asyncio.run(_run_fetch(username, email, auth_token, ct0))
