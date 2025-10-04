import os
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_ENGINE: Optional[Engine] = None
_DB_URL: Optional[str] = None


def _normalize_url(url: str) -> str:
    # SQLAlchemy recommends postgresql:// instead of postgres://
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def _resolve_url() -> Optional[str]:
    global _DB_URL
    if _DB_URL is not None:
        return _DB_URL

    env_url = (os.getenv("DATABASE_URL") or "").strip()
    if env_url:
        _DB_URL = _normalize_url(env_url)
        return _DB_URL

    # Fallback to file path if provided (SQLite)
    file_path = (os.getenv("BOT_DB_PATH") or "").strip()
    if file_path:
        p = Path(file_path).expanduser().resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        _DB_URL = f"sqlite:///{p}"
        return _DB_URL

    return None


def has_db() -> bool:
    return _resolve_url() is not None


def _get_engine() -> Engine:
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    url = _resolve_url()
    if not url:
        raise RuntimeError("No DATABASE_URL or BOT_DB_PATH set")

    connect_args = {}
    if url.startswith("sqlite:///"):
        # Needed for SQLite when used in multi-thread contexts
        connect_args = {"check_same_thread": False}

    _ENGINE = create_engine(url, pool_pre_ping=True, connect_args=connect_args, future=True)
    return _ENGINE


async def init_db() -> None:
    """
    Create tables if they don't exist. Works for PostgreSQL and SQLite.
    """
    if not has_db():
        return
    eng = _get_engine()
    ddl_users = """
    CREATE TABLE IF NOT EXISTS users(
        user_id BIGINT PRIMARY KEY,
        username TEXT,
        is_admin INTEGER,
        is_allowed INTEGER,
        locale TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    ddl_downloads = """
    CREATE TABLE IF NOT EXISTS downloads(
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT,
        url TEXT,
        title TEXT,
        artist TEXT,
        album TEXT,
        art_url TEXT,
        preset TEXT,
        mode TEXT,
        status TEXT,
        error TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    # Note: BIGSERIAL will be mapped to INTEGER AUTOINCREMENT on SQLite without issue
    with eng.begin() as conn:
        conn.execute(text(ddl_users))
        conn.execute(text(ddl_downloads))


async def upsert_user(*, user_id: int, username: Optional[str], is_admin: bool, is_allowed: bool, locale: str) -> None:
    if not has_db():
        return
    eng = _get_engine()
    # Use SQLite/Postgres compatible upsert
    stmt = text("""
        INSERT INTO users(user_id, username, is_admin, is_allowed, locale, updated_at)
        VALUES (:user_id, :username, :is_admin, :is_allowed, :locale, CURRENT_TIMESTAMP)
        ON CONFLICT (user_id) DO UPDATE SET
            username = EXCLUDED.username,
            is_admin = EXCLUDED.is_admin,
            is_allowed = EXCLUDED.is_allowed,
            locale = EXCLUDED.locale,
            updated_at = CURRENT_TIMESTAMP
    """)
    with eng.begin() as conn:
        conn.execute(stmt, {
            "user_id": user_id,
            "username": username,
            "is_admin": int(is_admin),
            "is_allowed": int(is_allowed),
            "locale": locale,
        })


async def add_download_log(payload: Dict[str, Any]) -> None:
    if not has_db():
        return
    eng = _get_engine()
    stmt = text("""
        INSERT INTO downloads(user_id, url, title, artist, album, art_url, preset, mode, status, error)
        VALUES (:user_id, :url, :title, :artist, :album, :art_url, :preset, :mode, :status, :error)
    """)
    with eng.begin() as conn:
        conn.execute(stmt, {
            "user_id": payload.get("user_id"),
            "url": payload.get("url"),
            "title": payload.get("title"),
            "artist": payload.get("artist"),
            "album": payload.get("album"),
            "art_url": payload.get("art_url"),
            "preset": payload.get("preset"),
            "mode": payload.get("mode"),
            "status": payload.get("status"),
            "error": payload.get("error"),
        })
