import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional
import asyncio

_DB_PATH: Optional[Path] = None

def has_db() -> bool:
    global _DB_PATH
    if _DB_PATH is not None:
        return True
    # Prefer DATABASE_URL if sqlite:///path.db
    url = os.getenv("DATABASE_URL", "").strip()
    if url.startswith("sqlite:///"):
        _DB_PATH = Path(url.replace("sqlite:///", "", 1)).expanduser().resolve()
    else:
        p = os.getenv("BOT_DB_PATH", "").strip()
        _DB_PATH = Path(p).expanduser().resolve() if p else None
    return _DB_PATH is not None

def _connect():
    assert _DB_PATH is not None
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(_DB_PATH)

async def init_db() -> None:
    if not has_db():
        return
    def _init():
        with _connect() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS users(
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                is_admin INTEGER,
                is_allowed INTEGER,
                locale TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )""")
            conn.execute("""
            CREATE TABLE IF NOT EXISTS downloads(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                url TEXT,
                title TEXT,
                artist TEXT,
                album TEXT,
                art_url TEXT,
                preset TEXT,
                mode TEXT,
                status TEXT,
                error TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )""")
            conn.commit()
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _init)

async def upsert_user(*, user_id: int, username: Optional[str], is_admin: bool, is_allowed: bool, locale: str) -> None:
    if not has_db():
        return
    def _upsert():
        with _connect() as conn:
            conn.execute("""
            INSERT INTO users(user_id, username, is_admin, is_allowed, locale, updated_at)
            VALUES(?,?,?,?,?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
              username=excluded.username,
              is_admin=excluded.is_admin,
              is_allowed=excluded.is_allowed,
              locale=excluded.locale,
              updated_at=CURRENT_TIMESTAMP
            """, (user_id, username, int(is_admin), int(is_allowed), locale))
            conn.commit()
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _upsert)

async def add_download_log(payload: Dict[str, Any]) -> None:
    if not has_db():
        return
    def _add():
        with _connect() as conn:
            conn.execute("""
            INSERT INTO downloads(user_id, url, title, artist, album, art_url, preset, mode, status, error)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """, (
                payload.get("user_id"),
                payload.get("url"),
                payload.get("title"),
                payload.get("artist"),
                payload.get("album"),
                payload.get("art_url"),
                payload.get("preset"),
                payload.get("mode"),
                payload.get("status"),
                payload.get("error"),
            ))
            conn.commit()
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _add)
