import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent.parent / "data" / "tweets.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS accounts (
            handle TEXT PRIMARY KEY,
            display_name TEXT,
            bio TEXT,
            followers INTEGER,
            discovered_via TEXT,
            added_at TEXT
        );

        CREATE TABLE IF NOT EXISTS tweets (
            id TEXT PRIMARY KEY,
            author TEXT,
            content TEXT,
            created_at TEXT,
            likes INTEGER DEFAULT 0,
            retweets INTEGER DEFAULT 0,
            url TEXT,
            fetched_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_tweets_author ON tweets(author);
        CREATE INDEX IF NOT EXISTS idx_tweets_created ON tweets(created_at);
        """)


def upsert_account(handle: str, display_name: str = "", bio: str = "",
                   followers: int = 0, discovered_via: str = "seed"):
    with get_conn() as conn:
        conn.execute("""
        INSERT INTO accounts(handle, display_name, bio, followers, discovered_via, added_at)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(handle) DO UPDATE SET
            display_name=excluded.display_name,
            bio=excluded.bio,
            followers=excluded.followers
        """, (handle, display_name, bio, followers, discovered_via,
              datetime.utcnow().isoformat()))


def upsert_tweet(tweet_id: str, author: str, content: str, created_at: str,
                 likes: int, retweets: int, url: str):
    with get_conn() as conn:
        conn.execute("""
        INSERT OR IGNORE INTO tweets(id, author, content, created_at, likes, retweets, url, fetched_at)
        VALUES(?,?,?,?,?,?,?,?)
        """, (tweet_id, author, content, created_at, likes, retweets, url,
              datetime.utcnow().isoformat()))


def get_accounts() -> list[dict]:
    with get_conn() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM accounts ORDER BY followers DESC")]


def get_tweets_since(days: int = 7) -> list[dict]:
    with get_conn() as conn:
        return [dict(r) for r in conn.execute("""
        SELECT * FROM tweets
        WHERE created_at >= datetime('now', ? || ' days')
        ORDER BY created_at DESC
        """, (f"-{days}",))]
