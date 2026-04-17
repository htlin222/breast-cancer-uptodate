import re
from collections import Counter
from . import db
from rich.console import Console

console = Console()

BC_BIO_KEYWORDS = [
    "oncol", "breast", "cancer", "hematol", "tumor", "oncology",
    "clinical trial", "MD", "physician", "researcher",
]


def _looks_like_bc_kol(bio: str) -> bool:
    bl = bio.lower()
    return sum(kw in bl for kw in BC_BIO_KEYWORDS) >= 2


def extract_mentions(tweets: list[dict]) -> list[str]:
    handles = []
    for tw in tweets:
        handles.extend(re.findall(r"@(\w+)", tw["content"]))
    return handles


def discover_new_accounts(tweets: list[dict], top_n: int = 20):
    known = {a["handle"].lower() for a in db.get_accounts()}
    mentions = extract_mentions(tweets)
    counts = Counter(h.lower() for h in mentions if h.lower() not in known)
    candidates = [handle for handle, _ in counts.most_common(top_n * 3)]

    console.print(f"[cyan]Discovered {len(candidates)} candidate accounts from mentions[/cyan]")
    added = 0
    for handle in candidates[:top_n]:
        db.upsert_account(handle, discovered_via="mention")
        added += 1

    console.print(f"[green]Added {added} new accounts for next fetch[/green]")
