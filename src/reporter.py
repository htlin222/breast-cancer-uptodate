from datetime import datetime, date
from pathlib import Path
from collections import Counter, defaultdict
import re
from . import db, config

REPORTS_DIR = Path(__file__).parent.parent / "reports"
TRIAL_PATTERN = re.compile(r"\b(NCT\d{8}|[A-Z][A-Z0-9]+-\d+(?:-\d+)?|[A-Z]{2,}\s*\d{3,})\b")


def _match_group(text: str) -> list[str]:
    tl = text.lower()
    groups = config.drug_groups()
    return [grp for grp, kws in groups.items() if any(kw.lower() in tl for kw in kws)]


def _extract_trials(text: str) -> list[str]:
    return TRIAL_PATTERN.findall(text)


def build_report(days: int = 7) -> str:
    tweets = db.get_tweets_since(days)
    accounts = {a["handle"]: a for a in db.get_accounts()}
    week = date.today().isocalendar()
    week_label = f"{week[0]}-W{week[1]:02d}"

    if not tweets:
        return f"# Breast Cancer Twitter Trend Report {week_label}\n\nNo tweets found in last {days} days.\n"

    conf_kws = config.conference_keywords()
    group_tweets: dict[str, list[dict]] = defaultdict(list)
    trial_mentions: Counter = Counter()
    conference_tweets: list[dict] = []
    author_counts: Counter = Counter()

    for tw in tweets:
        for g in _match_group(tw["content"]):
            group_tweets[g].append(tw)
        trial_mentions.update(_extract_trials(tw["content"]))
        if any(kw.lower() in tw["content"].lower() for kw in conf_kws):
            conference_tweets.append(tw)
        author_counts[tw["author"]] += 1

    top_tweets = sorted(tweets, key=lambda t: t["likes"] + t["retweets"] * 2, reverse=True)
    active_groups = sorted(group_tweets.items(), key=lambda x: len(x[1]), reverse=True)

    lines = []
    lines.append(f"# Breast Cancer Twitter Trend Report — {week_label}")
    lines.append(f"\n> Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} | "
                 f"{len(tweets)} BC-relevant tweets | {len(accounts)} tracked accounts\n")

    lines.append("## Overview\n")
    lines.append("### Trending Topics by Volume\n")
    lines.append("| Topic | Tweet Count | Top Account |")
    lines.append("|-------|-------------|-------------|")
    for grp, tws in active_groups[:8]:
        top_auth = Counter(t["author"] for t in tws).most_common(1)[0][0] if tws else "—"
        lines.append(f"| {grp} | {len(tws)} | @{top_auth} |")

    lines.append("")
    lines.append("### Most Active KOLs This Week\n")
    lines.append("| Handle | BC Tweets |")
    lines.append("|--------|-----------|")
    for auth, cnt in author_counts.most_common(10):
        lines.append(f"| @{auth} | {cnt} |")

    if conference_tweets:
        lines.append("\n## Conference & Abstract Activity\n")
        lines.append(f"_{len(conference_tweets)} tweets mentioning conferences or abstracts_\n")
        for tw in sorted(conference_tweets, key=lambda t: t["likes"], reverse=True)[:5]:
            lines.append(f"**@{tw['author']}** — {tw['created_at'][:10]}")
            lines.append(f"> {tw['content'][:280].strip()}")
            lines.append(f"👍 {tw['likes']} | 🔁 {tw['retweets']} | [link]({tw['url']})\n")

    lines.append("## Drug & Target Deep Dives\n")
    for grp, tws in active_groups:
        if not tws:
            continue
        lines.append(f"### {grp} ({len(tws)} tweets)\n")
        top = sorted(tws, key=lambda t: t["likes"] + t["retweets"] * 2, reverse=True)[:4]
        for tw in top:
            lines.append(f"**@{tw['author']}** — {tw['created_at'][:10]}")
            lines.append(f"> {tw['content'][:300].strip()}")
            lines.append(f"👍 {tw['likes']} | 🔁 {tw['retweets']} | [link]({tw['url']})\n")

    if trial_mentions:
        lines.append("## Clinical Trial Mentions\n")
        lines.append("| Trial | Mentions |")
        lines.append("|-------|----------|")
        for trial, cnt in trial_mentions.most_common(15):
            lines.append(f"| {trial} | {cnt} |")
        lines.append("")

    lines.append("## Most Engaging Tweets\n")
    for tw in top_tweets[:10]:
        lines.append(f"**@{tw['author']}** — {tw['created_at'][:10]}")
        lines.append(f"> {tw['content'][:300].strip()}")
        lines.append(f"👍 {tw['likes']} | 🔁 {tw['retweets']} | [link]({tw['url']})\n")

    lines.append("---")
    lines.append(f"_Report covers the last {days} days. {len(accounts)} accounts tracked._")
    return "\n".join(lines)


def write_report(days: int = 7) -> Path:
    REPORTS_DIR.mkdir(exist_ok=True)
    week = date.today().isocalendar()
    fname = REPORTS_DIR / f"{week[0]}-W{week[1]:02d}.md"
    fname.write_text(build_report(days))
    return fname
