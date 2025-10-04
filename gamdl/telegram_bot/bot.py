import os
import re
import shlex
import json
import asyncio
import tempfile
import subprocess
import zipfile
import logging
import io
import shutil
import time
import warnings
import fcntl
from contextlib import contextmanager
from datetime import timedelta
from uuid import uuid4
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional, Iterable, Callable, Union

import requests
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaAudio,
    InputFile,
)
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.error import BadRequest, Forbidden
from telegram.warnings import PTBUserWarning
from dotenv import load_dotenv

# Optional: tag reader (for title/artist/duration)
try:
    from mutagen import File as MutagenFile  # type: ignore
    from mutagen.mp4 import MP4  # type: ignore
except Exception:
    MutagenFile = None  # type: ignore
    MP4 = None  # type: ignore

# DB (optional)
try:
    from .db import init_db, has_db, upsert_user, add_download_log
except Exception:
    # allow local run without package layout
    async def init_db() -> None: ...
    def has_db() -> bool: return False
    async def upsert_user(*args, **kwargs) -> None: ...
    async def add_download_log(*args, **kwargs) -> None: ...

# Load .env
load_dotenv()

# === Environment & Config ===
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
# Default to container path commonly used; override via .env if needed
COOKIES_PATH = os.getenv("COOKIES_PATH", "/app/telegram_bot/secrets/cookies.txt")
OUTPUT_ROOT = Path(os.getenv("OUTPUT_ROOT", "./downloads")).resolve()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
CONCURRENCY = int(os.getenv("CONCURRENCY", "1"))
LOCALE = os.getenv("LOCALE", "en").lower()
GAMDL_EXTRA_ARGS = os.getenv("GAMDL_EXTRA_ARGS", "")  # e.g. --save-cover

# Caption options
CAPTION_SHOW_URL = os.getenv("CAPTION_SHOW_URL", "false").lower() in ("1", "true", "yes")

# Cleanup controls
TEMP_TTL_HOURS = float(os.getenv("TEMP_TTL_HOURS", "24"))
CLEANUP_INTERVAL_HOURS = float(os.getenv("CLEANUP_INTERVAL_HOURS", "12"))
TEMP_DIR_PREFIX = os.getenv("TEMP_DIR_PREFIX", "gamdl_")

# Force subscribe
FORCE_SUB_ENABLED = os.getenv("FORCE_SUB_ENABLED", "false").lower() in ("1", "true", "yes")
FORCE_SUB_CHANNEL = os.getenv("FORCE_SUB_CHANNEL", "").strip()  # @channel or -100...
FORCE_SUB_JOIN_URL = os.getenv("FORCE_SUB_JOIN_URL", "").strip()  # optional invite link

# Single-instance lock (host-level)
LOCK_FILE = Path(os.getenv("LOCK_FILE", "/tmp/gamdl_telegram_bot.lock"))

# Logging
LOG_LEVEL_PY = getattr(logging, (LOG_LEVEL or "INFO").upper(), logging.INFO)
logging.basicConfig(level=LOG_LEVEL_PY)
logging.getLogger("httpx").setLevel(logging.WARNING)  # reduce polling noise
logger = logging.getLogger("gamdl-bot")

# Access control
def _parse_ids_csv(val: Optional[str]) -> set[int]:
    if not val:
        return set()
    out = set()
    for p in val.split(","):
        p = p.strip()
        if p.isdigit():
            out.add(int(p))
    return out

ADMIN_USER_IDS = _parse_ids_csv(os.getenv("ADMIN_USER_IDS"))
ALLOWED_USER_IDS = _parse_ids_csv(os.getenv("ALLOWED_USER_IDS"))
PUBLIC_MODE = os.getenv("PUBLIC_MODE", "true").lower() in ("1", "true", "yes")

# Paths
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# Regex
APPLE_MUSIC_URL_RE = re.compile(r"https?://music\.apple\.com/\S+", re.IGNORECASE)

# Telegram size limit (~2GB hard limit)
MAX_TELEGRAM_FILE_BYTES = 2 * 1024 * 1024 * 1024
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(CONCURRENCY)

# Sessions
SESSIONS_KEY = "sessions"  # context.bot_data[SESSIONS_KEY] = {token: {...}}

# === i18n ===
DEFAULTS: Dict[str, str] = {
    "start_greeting": "Hi! Send me an Apple Music URL (song/album/playlist/video). I'll show artwork and details, then let you choose quality and send the files.",
    "help_text": (
        "Usage:\n"
        "1) Send an Apple Music URL\n"
        "2) Choose a quality preset\n"
        "3) Choose to send as files or ZIP\n\n"
        "Admin Commands:\n"
        "/status — show mode and lists\n"
        "/public_on — enable Public mode (Admin only)\n"
        "/public_off — switch to Private mode (Admin only)\n"
        "/allow <user_id> — allow a user in Private mode (Admin only)\n"
        "/deny <user_id> — revoke a user in Private mode (Admin only)\n"
    ),
    "private_only": "This bot is currently in Private mode — only approved users can use it.",
    "status_format": "Mode: {mode}\nAdmins: {admins}\nAllowed users: {allowed}",
    "public_on_ok": "Public mode enabled.",
    "public_off_ok": "Switched to Private mode. Only admins/allowed users can use it.",
    "admin_only": "Admin only.",
    "usage_allow": "Usage: /allow <user_id>",
    "usage_deny": "Usage: /deny <user_id>",
    "session_not_found": "Session not found.",
    "cancel_ok": "Cancelled.",
    "downloading_with_preset": "Downloading... ({preset})",
    "uploading_files": "Uploading files...",
    "uploading_zip": "Uploading ZIP...",
    "download_failed": "Download failed.",
    "no_files_found": "No files found — gamdl did not produce output.",
    "file_too_large": "File is too large for Telegram: {name} (~{mb:.1f}MB)",
    "send_complete": "Done ✔️",
    "send_failed": "Failed to send file: {error}",
    "choose_quality": "Choose a quality preset:",
    "choose_send_mode": "Choose how to send:",
    "zip_too_big": "ZIP exceeds 2GB — sending as individual files.",
    "caption_title_prefix": "Title: {title}",
    "caption_artist_prefix": "Artist: {artist}",
    "caption_album_prefix": "Album: {album}",
    "caption_date_prefix": "Release: {date}",
    "caption_type_prefix": "Type: {kind}",
    "caption_tracks_prefix": "Tracks: {count}",
    "caption_link": "{url}",
    "prompt_choose_quality": "Select a quality preset to proceed:",
    "btn_quality_default": "Default",
    "btn_quality_audio_aac256": "Audio (AAC 256kbps)",
    "btn_quality_video_1080p": "Video 1080p",
    "btn_quality_video_4k": "Video 4K",
    "btn_cancel": "Cancel",
    "btn_send_files": "Send as files",
    "btn_send_zip": "Send as ZIP",
    "btn_back_quality": "Back",
    "join_required": "Please join our channel to use this bot.",
    "btn_join_channel": "Join channel",
    "btn_ive_joined": "I've joined",
}

I18N: Dict[str, str] = {}

def _load_locale() -> None:
    global I18N
    try:
        loc_path = Path(__file__).resolve().parent / "locales" / f"{LOCALE}.json"
        if loc_path.exists():
            I18N = json.loads(loc_path.read_text(encoding="utf-8"))
        else:
            I18N = {}
    except Exception:
        I18N = {}

def t(key: str, **kwargs) -> str:
    template = I18N.get(key) or DEFAULTS.get(key) or key
    try:
        return template.format(**kwargs)
    except Exception:
        return template

_load_locale()

# === Presets mapping to gamdl flags ===
PRESETS: Dict[str, List[str]] = {
    "default": [],
    "audio_aac256": ["--codec-song", "aac-legacy"],
    "video_1080p": ["--quality-post", "best", "--codec-music-video", "h264,h265", "--resolution", "1080p"],
    "video_4k": ["--codec-music-video", "h265,h264", "--resolution", "2160p"],
}

PRESET_LABELS = {
    "default": DEFAULTS["btn_quality_default"],
    "audio_aac256": DEFAULTS["btn_quality_audio_aac256"],
    "video_1080p": DEFAULTS["btn_quality_video_1080p"],
    "video_4k": DEFAULTS["btn_quality_video_4k"],
}

# === Single-instance lock ===
@contextmanager
def single_instance_lock(lock_path: Path):
    """
    Prevent multiple instances on the same host. Does not prevent other hosts/containers.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.write(fd, str(os.getpid()).encode(errors="ignore"))
        os.fsync(fd)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except Exception:
            pass
        os.close(fd)

# === Authorization / Force-sub ===
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS

def is_authorized(user_id: int) -> bool:
    if PUBLIC_MODE:
        return True
    return user_id in ALLOWED_USER_IDS or is_admin(user_id)

def _normalize_channel_id(raw: str) -> Union[str, int]:
    s = raw.strip()
    if not s:
        return s
    if s.startswith("http"):
        m = re.search(r"t\.me/(@?[\w\d_]+)", s)
        if m:
            s = m.group(1)
    if s.startswith("@"):
        return s
    if s.lstrip("-").isdigit():
        try:
            return int(s)
        except Exception:
            return s
    return s

async def _is_user_subscribed(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> Optional[bool]:
    if not FORCE_SUB_ENABLED or not FORCE_SUB_CHANNEL:
        return True
    chat_id = _normalize_channel_id(FORCE_SUB_CHANNEL)
    try:
        member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        status = getattr(member, "status", "")
        return status in {"member", "administrator", "creator"}
    except Forbidden:
        logger.error("Bot cannot access FORCE_SUB_CHANNEL; make the bot an admin there.")
        return None
    except BadRequest as e:
        logger.warning("get_chat_member failed: %s", e)
        return False
    except Exception as e:
        logger.warning("get_chat_member error: %s", e)
        return False

def _join_keyboard() -> InlineKeyboardMarkup:
    url = FORCE_SUB_JOIN_URL
    if not url and FORCE_SUB_CHANNEL.startswith("@"):
        url = f"https://t.me/{FORCE_SUB_CHANNEL[1:]}"
    buttons = []
    if url:
        buttons.append([InlineKeyboardButton(DEFAULTS["btn_join_channel"], url=url)])
    buttons.append([InlineKeyboardButton(DEFAULTS["btn_ive_joined"], callback_data="checksub")])
    return InlineKeyboardMarkup(buttons)

async def guard_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    if not user:
        return False

    # DB: upsert user (best-effort)
    try:
        await upsert_user(
            user_id=user.id,
            username=user.username,
            is_admin=(user.id in ADMIN_USER_IDS),
            is_allowed=(user.id in ALLOWED_USER_IDS),
            locale=LOCALE,
        )
    except Exception as e:
        logger.debug("DB upsert_user failed: %s", e)

    # Force subscribe
    if FORCE_SUB_ENABLED and FORCE_SUB_CHANNEL:
        sub = await _is_user_subscribed(context, user.id)
        if sub is None:
            await (update.effective_message or update.effective_chat).reply_text(
                "Bot misconfigured: cannot access Force-Subscribe channel (bot must be admin)."
            )
            return False
        if not sub:
            await (update.effective_message or update.effective_chat).reply_text(
                t("join_required"), reply_markup=_join_keyboard()
            )
            return False

    # Private/public mode
    if is_authorized(user.id):
        return True
    await (update.effective_message or update.effective_chat).reply_text(t("private_only"))
    return False

# === Apple metadata (via iTunes Lookup API) ===
def _extract_ids_from_url(url: str) -> Tuple[str, Optional[str]]:
    m_store = re.search(r"music\.apple\.com/([a-zA-Z\-]{2,})/", url)
    storefront = (m_store.group(1) if m_store else "us").split("-")[0]
    m_track = re.search(r"[&?]i=(\d+)", url)
    if m_track:
        return storefront, m_track.group(1)
    m_last = re.search(r"/(\d+)(?:\?|$)", url)
    return storefront, (m_last.group(1) if m_last else None)

def itunes_lookup(storefront: str, id_: str) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(
            "https://itunes.apple.com/lookup",
            params={"id": id_, "country": storefront},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("resultCount", 0) > 0:
            return data["results"][0]
        return None
    except Exception:
        return None

def inflate_artwork(url: str, size: int = 1200) -> str:
    return re.sub(r"/\d+x\d+(bb)?(?=[\.-])", f"/{size}x{size}\\1", url)

def resolve_artwork_url(raw_url: str) -> str:
    ua = {"User-Agent": "Mozilla/5.0"}
    candidates = [
        inflate_artwork(raw_url, 1200),
        inflate_artwork(raw_url, 600),
        inflate_artwork(raw_url, 300),
        raw_url,
    ]
    for u in candidates:
        try:
            resp = requests.get(u, timeout=6, headers=ua, stream=True)
            if resp.status_code == 200:
                resp.close()
                return u
        except Exception:
            pass
    return raw_url

# --- OG fallback ---
OG_IMAGE_RE = re.compile(r'<meta\s+(?:property|name)=["\']og:image["\']\s+content=["\']([^"\']+)["\']', re.I)
OG_TITLE_RE = re.compile(r'<meta\s+(?:property|name)=["\']og:title["\']\s+content=["\']([^"\']+)["\']', re.I)
OG_DESC_RE = re.compile(r'<meta\s+(?:property|name)=["\']og:description["\']\s+content=["\']([^"\']+)["\']', re.I)

def fetch_og_meta(url: str, timeout: int = 8) -> Dict[str, str]:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        html = r.text
        og_image = (OG_IMAGE_RE.search(html) or [None, None])[1]
        og_title = (OG_TITLE_RE.search(html) or [None, None])[1]
        og_desc = (OG_DESC_RE.search(html) or [None, None])[1]
        out: Dict[str, str] = {}
        if og_image:
            out["image"] = og_image
        if og_title:
            out["title"] = og_title
        if og_desc:
            out["description"] = og_desc
        return out
    except Exception:
        return {}

def build_caption(meta: Optional[Dict[str, Any]], url: str, og: Optional[Dict[str, str]] = None) -> str:
    """
    Build caption. By default, URL is hidden (CAPTION_SHOW_URL=false).
    """
    if not meta and not og:
        return f"{t('prompt_choose_quality')}"
    title = (
        (meta or {}).get("trackName")
        or (meta or {}).get("collectionName")
        or (og or {}).get("title")
        or (meta or {}).get("artistName")
        or "Unknown"
    )
    artist = (meta or {}).get("artistName") or ""
    album = (meta or {}).get("collectionName") or ""
    release_date = (meta or {}).get("releaseDate", "")[:10] if (meta or {}).get("releaseDate") else ""
    kind = (meta or {}).get("kind") or (meta or {}).get("wrapperType") or ""
    track_count = (meta or {}).get("trackCount") or (meta or {}).get("trackNumber") or ""

    lines: List[str] = []
    if title:
        lines.append(t("caption_title_prefix", title=title))
    if artist:
        lines.append(t("caption_artist_prefix", artist=artist))
    if album:
        lines.append(t("caption_album_prefix", album=album))
    if release_date:
        lines.append(t("caption_date_prefix", date=release_date))
    if kind:
        lines.append(t("caption_type_prefix", kind=kind))
    if track_count:
        lines.append(t("caption_tracks_prefix", count=track_count))

    if CAPTION_SHOW_URL:
        lines += ["", t("caption_link", url=url)]

    if lines:
        lines += ["", t("choose_quality")]
    else:
        lines = [t("choose_quality")]
    return "\n".join(lines)

# === gamdl command builder ===
def _find_gamdl_binary() -> List[str]:
    # Prefer entrypoint; fallback to python -m if not found
    import shutil as _sh
    if _sh.which("gamdl"):
        return ["gamdl"]
    return ["python", "-m", "gamdl"]

def _build_gamdl_cmd(urls: List[str], out_dir: Path, preset_args: List[str]) -> List[str]:
    cmd = [
        *_find_gamdl_binary(),
        "--cookies-path", str(COOKIES_PATH),
        "--output-path", str(out_dir),
        "--no-config-file",
        "--log-level", LOG_LEVEL,
    ]
    if GAMDL_EXTRA_ARGS:
        cmd += shlex.split(GAMDL_EXTRA_ARGS)
    if preset_args:
        cmd += preset_args
    cmd += urls
    return cmd

# ---------- Cleanup policy ----------
def _is_older_than(path: Path, cutoff: float) -> bool:
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        return False
    return mtime < cutoff

def cleanup_temp_dirs(base: Path, prefix: str, ttl_hours: float) -> Tuple[int, int]:
    removed = 0
    errors = 0
    cutoff = time.time() - ttl_hours * 3600.0
    for child in base.iterdir():
        if not child.is_dir():
            continue
        if not child.name.startswith(prefix):
            continue
        if not _is_older_than(child, cutoff):
            continue
        try:
            shutil.rmtree(child, ignore_errors=False)
            removed += 1
            logger.info("cleanup: removed %s", child)
        except Exception as e:
            errors += 1
            logger.warning("cleanup: failed to remove %s: %s", child, e)
    return removed, errors

async def cleanup_job(_: ContextTypes.DEFAULT_TYPE) -> None:
    removed, errors = cleanup_temp_dirs(OUTPUT_ROOT, TEMP_DIR_PREFIX, TEMP_TTL_HOURS)
    if removed or errors:
        logger.info("cleanup summary: removed=%d errors=%d", removed, errors)

# ---------- Progress-ish runner ----------
class _Throttle:
    def __init__(self, interval_sec: float = 2.0):
        self.interval = interval_sec
        self._last = 0.0

    def ok(self) -> bool:
        now = time.monotonic()
        if now - self._last >= self.interval:
            self._last = now
            return True
        return False

def _summarize_line(line: str) -> str:
    s = line.strip()
    s = re.sub(r"\s+", " ", s)
    return s[:200] if len(s) > 200 else s

async def _run_gamdl_stream(
    urls: List[str],
    out_dir: Path,
    preset_args: List[str],
    on_progress: Callable[[str], asyncio.Future | None],
) -> Tuple[int, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = _build_gamdl_cmd(urls, out_dir, preset_args)
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    buf_tail: List[str] = []
    throttle = _Throttle(2.0)

    assert proc.stdout is not None
    while True:
        line_bytes = await proc.stdout.readline()
        if not line_bytes:
            break
        line = line_bytes.decode(errors="ignore")
        buf_tail.append(line.strip())
        if len(buf_tail) > 50:
            buf_tail = buf_tail[-50:]

        if throttle.ok():
            msg = _summarize_line(line)
            try:
                await on_progress(msg)
            except Exception:
                pass

    returncode = await proc.wait()
    tail_text = "\n".join(buf_tail[-10:])
    return returncode, tail_text

# ---------- ZIP naming helpers ----------
INVALID_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1F]')

def sanitize_filename(name: str, max_len: int = 120) -> str:
    cleaned = INVALID_CHARS_RE.sub(" ", name)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .-_")
    if not cleaned:
        cleaned = "download"
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len].rstrip(" .-_")
    return cleaned

def build_zip_basename(sess: Dict[str, Any]) -> str:
    meta = (sess.get("meta") or {})
    og = (sess.get("og") or {})
    artist = (meta.get("artistName") or "").strip()
    album = (meta.get("collectionName") or "").strip()
    title = (meta.get("trackName") or "").strip()
    og_title = (og.get("title") or "").strip()
    second = album or title or og_title or ""
    if artist and second:
        base = f"{artist} - {second}"
    else:
        base = artist or second or "download"
    return sanitize_filename(base)

def _zip_directory(src_dir: Path, zip_basename: Optional[str] = None) -> Path:
    zip_name = (zip_basename or src_dir.name) + ".zip"
    zip_path = src_dir.parent / zip_name
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in src_dir.rglob("*"):
            if p.is_file():
                zf.write(p, arcname=p.relative_to(src_dir))
    return zip_path

# === Metadata helpers for outgoing files ===
def _parse_from_filename(name: str) -> Tuple[Optional[str], Optional[str]]:
    base = Path(name).stem
    base = re.sub(r"^\s*\d{1,2}\s*[-_.]\s*", "", base)
    parts = [p.strip() for p in re.split(r"\s*-\s*", base, maxsplit=2) if p.strip()]
    if len(parts) >= 2:
        return parts[1], parts[0]  # title, artist
    return base or None, None

def _extract_audio_tags(path: Path) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    title: Optional[str] = None
    artist: Optional[str] = None
    duration: Optional[int] = None
    if MutagenFile:
        try:
            audio = MutagenFile(str(path))
            if audio and getattr(audio, "info", None):
                try:
                    duration = int(audio.info.length)
                except Exception:
                    duration = None
            tags = getattr(audio, "tags", None)
            if tags:
                if MP4 and isinstance(audio, MP4):
                    title = (tags.get("\xa9nam", [None]) or [None])[0]
                    artist = (tags.get("\xa9ART", [None]) or [None])[0]
                else:
                    t_title = getattr(tags, "get", lambda *_: None)("TIT2") or getattr(tags, "get", lambda *_: None)("title")
                    if t_title is not None:
                        title = getattr(t_title, "text", [t_title])[0] if hasattr(t_title, "text") else t_title
                    t_artist = getattr(tags, "get", lambda *_: None)("TPE1") or getattr(tags, "get", lambda *_: None)("artist")
                    if t_artist is not None:
                        artist = getattr(t_artist, "text", [t_artist])[0] if hasattr(t_artist, "text") else t_artist
        except Exception:
            pass
    if not title or not artist:
        fn_title, fn_artist = _parse_from_filename(path.name)
        title = title or fn_title
        artist = artist or fn_artist
    return title, artist, duration

def _find_cover_image(root: Path) -> Optional[Path]:
    candidates = []
    for name in ("cover", "folder", "artwork", "Artwork", "Front", "front"):
        candidates.extend(root.glob(f"{name}.*"))
    candidates = [p for p in candidates if p.suffix.lower() in {".jpg", ".jpeg", ".png"}]
    if candidates:
        return candidates[0]
    imgs = [p for p in root.rglob("*") if p.suffix.lower() in {".jpg", ".jpeg", ".png"} and p.is_file()]
    if not imgs:
        return None
    imgs.sort(key=lambda p: p.stat().st_size, reverse=True)
    return imgs[0]

def _download_bytes(url: str, timeout: int = 10) -> Optional[bytes]:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        return r.content
    except Exception as e:
        logger.debug("download thumbnail failed: %s", e)
        return None

# === Telegram helpers ===
def _get_sessions(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Dict[str, Any]]:
    if SESSIONS_KEY not in context.bot_data:
        context.bot_data[SESSIONS_KEY] = {}
    return context.bot_data[SESSIONS_KEY]  # type: ignore

async def _reply_photo_resilient(msg, photo_url: str, caption: str, reply_markup: Optional[InlineKeyboardMarkup]) -> None:
    await msg.chat.send_action(ChatAction.UPLOAD_PHOTO)
    try:
        await msg.reply_photo(photo=photo_url, caption=caption, reply_markup=reply_markup)
        return
    except Exception as e:
        logger.debug("reply_photo URL failed: %s", e)
    data = _download_bytes(photo_url)
    if data:
        await msg.reply_photo(photo=io.BytesIO(data), caption=caption, reply_markup=reply_markup, filename="art.jpg")
    else:
        await msg.reply_text(caption, reply_markup=reply_markup)

# ---- Status message helpers (separate line under caption) ----
async def _create_status_message(query, text: str, sess: Dict[str, Any]) -> None:
    chat = query.message.chat
    status = await query.message.reply_text(text, reply_to_message_id=query.message.message_id)
    sess["status_chat_id"] = chat.id
    sess["status_msg_id"] = status.message_id

async def _set_status_message(context: ContextTypes.DEFAULT_TYPE, sess: Dict[str, Any], text: str) -> None:
    chat_id = sess.get("status_chat_id")
    msg_id = sess.get("status_msg_id")
    if not chat_id or not msg_id:
        return
    try:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text)
    except BadRequest:
        pass

async def _append_status_message(context: ContextTypes.DEFAULT_TYPE, sess: Dict[str, Any], line: str) -> None:
    base = sess.get("progress_base") or ""
    if not base:
        base = "⌛ "
        sess["progress_base"] = base
    text = f"{base}{line}"
    await _set_status_message(context, sess, text)

async def _clear_status_message(context: ContextTypes.DEFAULT_TYPE, sess: Dict[str, Any]) -> None:
    chat_id = sess.get("status_chat_id")
    msg_id = sess.get("status_msg_id")
    if not chat_id or not msg_id:
        return
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except BadRequest:
        pass
    finally:
        sess.pop("status_chat_id", None)
        sess.pop("status_msg_id", None)
        sess.pop("progress_base", None)

# === Telegram handlers ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard_access(update, context):
        return
    await update.message.reply_text(t("start_greeting"))

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard_access(update, context):
        return
    await update.message.reply_text(t("help_text"))

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard_access(update, context):
        return
    allowed = ", ".join(map(str, sorted(ALLOWED_USER_IDS))) or "(none)"
    admins = ", ".join(map(str, sorted(ADMIN_USER_IDS))) or "(none)"
    await update.message.reply_text(
        t("status_format", mode=("Public" if PUBLIC_MODE else "Private"), admins=admins, allowed=allowed)
    )

async def public_on_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global PUBLIC_MODE
    user = update.effective_user
    if not user or not is_admin(user.id):
        await update.message.reply_text(t("admin_only"))
        return
    PUBLIC_MODE = True
    await update.message.reply_text(t("public_on_ok"))

async def public_off_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global PUBLIC_MODE
    user = update.effective_user
    if not user or not is_admin(user.id):
        await update.message.reply_text(t("admin_only"))
        return
    PUBLIC_MODE = False
    await update.message.reply_text(t("public_off_ok"))

async def allow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not is_admin(user.id):
        await update.message.reply_text(t("admin_only"))
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(t("usage_allow"))
        return
    uid = int(context.args[0])
    ALLOWED_USER_IDS.add(uid)
    await update.message.reply_text(f"User {uid} allowed.")

async def deny_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not is_admin(user.id):
        await update.message.reply_text(t("admin_only"))
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(t("usage_deny"))
        return
    uid = int(context.args[0])
    if uid in ALLOWED_USER_IDS:
        ALLOWED_USER_IDS.remove(uid)
    await update.message.reply_text(f"User {uid} removed.")

def _quality_keyboard(token: str) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(DEFAULTS["btn_quality_default"], callback_data=f"q:{token}:default"),
            InlineKeyboardButton(DEFAULTS["btn_quality_audio_aac256"], callback_data=f"q:{token}:audio_aac256"),
        ],
        [
            InlineKeyboardButton(DEFAULTS["btn_quality_video_1080p"], callback_data=f"q:{token}:video_1080p"),
            InlineKeyboardButton(DEFAULTS["btn_quality_video_4k"], callback_data=f"q:{token}:video_4k"),
        ],
        [InlineKeyboardButton(DEFAULTS["btn_cancel"], callback_data=f"cancel:{token}")],
    ]
    return InlineKeyboardMarkup(rows)

def _sendmode_keyboard(token: str, preset: str) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(DEFAULTS["btn_send_files"], callback_data=f"go:{token}:{preset}:files"),
            InlineKeyboardButton(DEFAULTS["btn_send_zip"], callback_data=f"go:{token}:{preset}:zip"),
        ],
        [
            InlineKeyboardButton(DEFAULTS["btn_back_quality"], callback_data=f"back:{token}"),
            InlineKeyboardButton(DEFAULTS["btn_cancel"], callback_data=f"cancel:{token}"),
        ],
    ]
    return InlineKeyboardMarkup(rows)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard_access(update, context):
        return
    msg = update.message
    if not msg or not msg.text:
        return

    urls = APPLE_MUSIC_URL_RE.findall(msg.text)
    if not urls:
        return

    token = uuid4().hex
    sessions = _get_sessions(context)
    sessions[token] = {"urls": urls, "user_id": update.effective_user.id}

    storefront, maybe_id = _extract_ids_from_url(urls[0])
    meta: Optional[Dict[str, Any]] = None
    og: Dict[str, str] = {}

    if maybe_id:
        loop = asyncio.get_running_loop()
        meta = await loop.run_in_executor(None, itunes_lookup, storefront, maybe_id)

    if not meta or not meta.get("artworkUrl100"):
        loop = asyncio.get_running_loop()
        og = await loop.run_in_executor(None, fetch_og_meta, urls[0])

    caption = build_caption(meta, urls[0], og=og)

    poster_url: Optional[str] = None
    if meta and meta.get("artworkUrl100"):
        poster_url = resolve_artwork_url(meta["artworkUrl100"])
    elif og.get("image"):
        poster_url = resolve_artwork_url(og["image"])

    # Save in session for later (sending & ZIP naming)
    sessions[token]["meta"] = meta or {}
    sessions[token]["og"] = og or {}
    sessions[token]["poster_url"] = poster_url

    if poster_url:
        await _reply_photo_resilient(msg, poster_url, caption, _quality_keyboard(token))
    else:
        await msg.reply_text(caption, reply_markup=_quality_keyboard(token))

async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # handle "I've joined" check first
    query = update.callback_query
    if query and (query.data or "").startswith("checksub"):
        user = query.from_user
        sub = await _is_user_subscribed(context, user.id)
        if sub:
            await query.message.reply_text("Thanks! You can use the bot now.")
        elif sub is None:
            await query.message.reply_text("Bot cannot access the channel. Please make the bot an admin there.")
        else:
            await query.message.reply_text(t("join_required"), reply_markup=_join_keyboard())
        return

    if not await guard_access(update, context):
        return
    if not query:
        return
    await query.answer()

    data = query.data or ""
    sessions = _get_sessions(context)

    if data.startswith("cancel:"):
        token = data.split(":", 1)[1]
        sess = sessions.pop(token, None)
        try:
            await query.edit_message_reply_markup(None)
        except BadRequest:
            pass
        if sess:
            await _clear_status_message(context, sess)
        await query.message.reply_text(t("cancel_ok"), reply_to_message_id=query.message.message_id)
        return

    if data.startswith("back:"):
        token = data.split(":", 1)[1]
        if token not in sessions:
            await query.message.reply_text(t("session_not_found"), reply_to_message_id=query.message.message_id)
            return
        await query.edit_message_reply_markup(_quality_keyboard(token))
        return

    if data.startswith("q:"):
        _, token, preset = data.split(":")
        sess = sessions.get(token)
        if not sess:
            await query.message.reply_text(t("session_not_found"), reply_to_message_id=query.message.message_id)
            return
        sess["preset"] = preset
        await query.edit_message_reply_markup(_sendmode_keyboard(token, preset))
        return

    if data.startswith("go:"):
        _, token, preset, mode = data.split(":")
        sess = sessions.get(token)
        if not sess:
            await query.message.reply_text(t("session_not_found"), reply_to_message_id=query.message.message_id)
            return

        urls: List[str] = sess["urls"]
        preset_args = PRESETS.get(preset, [])
        send_zip = (mode == "zip")

        try:
            await query.edit_message_reply_markup(None)
        except BadRequest:
            pass

        # Create a separate status line
        await _create_status_message(
            query,
            t("downloading_with_preset", preset=PRESET_LABELS.get(preset, preset)),
            sess,
        )

        async def on_progress(line: str) -> None:
            await _append_status_message(context, sess, line)

        status = "ok"
        err_text: Optional[str] = None

        async with DOWNLOAD_SEMAPHORE:
            with tempfile.TemporaryDirectory(prefix=TEMP_DIR_PREFIX, dir=OUTPUT_ROOT) as td:
                out_dir = Path(td)
                # Stream gamdl and update status
                returncode, tail_text = await _run_gamdl_stream(urls, out_dir, preset_args, on_progress)

                if returncode != 0:
                    text = t("download_failed")
                    if tail_text:
                        text += "\n" + tail_text[:1500]
                    await _set_status_message(context, sess, text)
                    status = "failed"
                    err_text = tail_text[:1500] if tail_text else "failed"
                else:
                    chat = query.message.chat
                    try:
                        if send_zip:
                            await _set_status_message(context, sess, t("uploading_zip"))
                            zip_base = build_zip_basename(sess)
                            loop = asyncio.get_running_loop()
                            zip_path = await loop.run_in_executor(None, _zip_directory, out_dir, zip_base)
                            if zip_path.stat().st_size >= MAX_TELEGRAM_FILE_BYTES:
                                await _set_status_message(context, sess, t("zip_too_big"))
                                await _send_files_from_dir(chat, out_dir, sess)
                            else:
                                await chat.send_action(ChatAction.UPLOAD_DOCUMENT)
                                await chat.send_document(document=zip_path.open("rb"), filename=zip_path.name)
                                await chat.send_message(t("send_complete"))
                        else:
                            await _set_status_message(context, sess, t("uploading_files"))
                            await _send_files_from_dir(chat, out_dir, sess)
                    finally:
                        pass

                # Always clear the status message after sending or error
                await _clear_status_message(context, sess)

        # DB: log attempt (best-effort)
        try:
            meta = sess.get("meta") or {}
            await add_download_log({
                "user_id": sess.get("user_id"),
                "url": (sess.get("urls") or [""])[0],
                "title": meta.get("trackName") or meta.get("collectionName"),
                "artist": meta.get("artistName"),
                "album": meta.get("collectionName"),
                "art_url": sess.get("poster_url"),
                "preset": preset,
                "mode": mode,
                "status": status,
                "error": err_text,
            })
        except Exception as e:
            logger.debug("DB add_download_log failed: %s", e)

        # Clear session after run
        sessions.pop(token, None)

# ------- Media group batching for audio + file sending -------
def _chunked(it: Iterable[Path], size: int) -> Iterable[List[Path]]:
    batch: List[Path] = []
    for x in it:
        batch.append(x)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch

async def _send_audios_as_groups(chat, audio_files: List[Path], shared_thumb_path: Optional[Path], thumb_bytes_cache: Optional[bytes], sess: Dict[str, Any]) -> bool:
    if not audio_files:
        return False

    sent_any = False
    for group in _chunked(audio_files, 10):
        medias: List[InputMediaAudio] = []
        open_handles: List[Any] = []

        shared_thumb_input: Optional[InputFile] = None
        shared_thumb_io: Optional[io.BytesIO] = None
        if shared_thumb_path and shared_thumb_path.exists():
            fh = shared_thumb_path.open("rb")
            open_handles.append(fh)
            shared_thumb_input = InputFile(fh, filename=shared_thumb_path.name)
        elif thumb_bytes_cache:
            shared_thumb_io = io.BytesIO(thumb_bytes_cache)
            open_handles.append(shared_thumb_io)
            shared_thumb_input = InputFile(shared_thumb_io, filename="thumb.jpg")

        try:
            for f in group:
                title, artist, duration = _extract_audio_tags(f)
                audio_fh = f.open("rb")
                open_handles.append(audio_fh)
                media_kwargs = dict(
                    media=InputFile(audio_fh, filename=f.name),
                    title=title or Path(f.name).stem,
                    performer=artist or None,
                    duration=duration or None,
                )
                if shared_thumb_input is not None:
                    media_kwargs["thumbnail"] = shared_thumb_input
                medias.append(InputMediaAudio(**media_kwargs))

            await chat.send_media_group(media=medias)
            sent_any = True

        except BadRequest as e:
            logger.warning("send_media_group failed; falling back to single sends: %s", e)
            for f in group:
                try:
                    await chat.send_action(ChatAction.UPLOAD_DOCUMENT)
                    title, artist, duration = _extract_audio_tags(f)
                    kwargs = dict(
                        audio=f.open("rb"),
                        filename=f.name,
                        title=title or Path(f.name).stem,
                        performer=artist or None,
                        duration=duration or None,
                    )
                    thumb_io: Optional[io.BytesIO] = None
                    if shared_thumb_path and shared_thumb_path.exists():
                        thumb_io = io.BytesIO(shared_thumb_path.read_bytes())
                    elif thumb_bytes_cache:
                        thumb_io = io.BytesIO(thumb_bytes_cache)
                    if thumb_io:
                        try:
                            await chat.send_audio(**{**kwargs, "thumbnail": thumb_io})
                        except TypeError:
                            thumb_io.seek(0)
                            await chat.send_audio(**{**kwargs, "thumb": thumb_io})
                    else:
                        await chat.send_audio(**kwargs)
                    sent_any = True
                except Exception as se:
                    await chat.send_message(t("send_failed", error=str(se)))
        finally:
            for h in open_handles:
                try:
                    h.close()
                except Exception:
                    pass

    return sent_any

async def _send_files_from_dir(chat, out_dir: Path, sess: Dict[str, Any]):
    files = sorted(p for p in out_dir.rglob("*") if p.is_file())
    if not files:
        await chat.send_message(t("no_files_found"))
        return

    shared_thumb_path = _find_cover_image(out_dir)
    thumb_bytes_cache: Optional[bytes] = None
    if not shared_thumb_path:
        poster_url = sess.get("poster_url")
        if poster_url:
            thumb_bytes_cache = _download_bytes(poster_url)

    audio_exts = {".m4a", ".mp3", ".flac", ".wav"}
    video_exts = {".mp4", ".m4v", ".mov"}

    audio_files = [f for f in files if f.suffix.lower() in audio_exts]
    video_files = [f for f in files if f.suffix.lower() in video_exts]
    other_files = [f for f in files if f not in audio_files and f not in video_files]

    sent_any = False

    sent_any |= await _send_audios_as_groups(chat, audio_files, shared_thumb_path, thumb_bytes_cache, sess)

    for f in video_files:
        size = f.stat().st_size
        if size >= MAX_TELEGRAM_FILE_BYTES:
            await chat.send_message(t("file_too_large", name=f.name, mb=size / 1024 / 1024))
            continue
        try:
            await chat.send_action(ChatAction.UPLOAD_DOCUMENT)
            thumb_io: Optional[io.BytesIO] = None
            if shared_thumb_path and shared_thumb_path.exists():
                thumb_io = io.BytesIO(shared_thumb_path.read_bytes())
            elif thumb_bytes_cache:
                thumb_io = io.BytesIO(thumb_bytes_cache)

            kwargs = dict(
                video=f.open("rb"),
                filename=f.name,
                supports_streaming=True,
            )
            if thumb_io:
                try:
                    await chat.send_video(**{**kwargs, "thumbnail": thumb_io})
                except TypeError:
                    thumb_io.seek(0)
                    await chat.send_video(**{**kwargs, "thumb": thumb_io})
            else:
                await chat.send_video(**kwargs)
            sent_any = True
        except Exception as e:
            await chat.send_message(t("send_failed", error=str(e)))

    for f in other_files:
        size = f.stat().st_size
        if size >= MAX_TELEGRAM_FILE_BYTES:
            await chat.send_message(t("file_too_large", name=f.name, mb=size / 1024 / 1024))
            continue
        try:
            await chat.send_action(ChatAction.UploadDocument)
        except AttributeError:
            await chat.send_action(ChatAction.UPLOAD_DOCUMENT)
        try:
            await chat.send_document(document=f.open("rb"), filename=f.name)
            sent_any = True
        except Exception as e:
            await chat.send_message(t("send_failed", error=str(e)))

    if sent_any:
        await chat.send_message(t("send_complete"))

# === Global error handler ===
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Exception while handling update: %s", context.error)

# === Async main (Py3.12-safe) ===
async def main_async() -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    if not Path(COOKIES_PATH).exists():
        print(f"Warning: COOKIES_PATH not found at {COOKIES_PATH}. Gamdl may fail.")

    # Acquire single-instance lock on this host (prevents two local processes)
    with single_instance_lock(LOCK_FILE):
        # Init DB (no-op if DATABASE_URL missing)
        try:
            await init_db()
            if has_db():
                logger.info("Database initialized.")
            else:
                logger.info("DATABASE_URL/BOT_DB_PATH not set; running without DB.")
        except Exception as e:
            logger.error("DB init failed: %s", e)

        # Startup cleanup
        removed, errors = cleanup_temp_dirs(OUTPUT_ROOT, TEMP_DIR_PREFIX, TEMP_TTL_HOURS)
        if removed or errors:
            logger.info("startup cleanup: removed=%d errors=%d", removed, errors)

        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("help", help_cmd))
        app.add_handler(CommandHandler("status", status_cmd))
        app.add_handler(CommandHandler("public_on", public_on_cmd))
        app.add_handler(CommandHandler("public_off", public_off_cmd))
        app.add_handler(CommandHandler("allow", allow_cmd))
        app.add_handler(CommandHandler("deny", deny_cmd))

        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        app.add_handler(CallbackQueryHandler(callbacks))

        # Schedule periodic cleanup (safe if job-queue extra not installed)
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", PTBUserWarning)
                jq = getattr(app, "job_queue", None)
            if jq is not None:
                jq.run_repeating(
                    cleanup_job,
                    interval=timedelta(hours=CLEANUP_INTERVAL_HOURS),
                    first=timedelta(seconds=30),
                    name="cleanup_temp_dirs",
                )
            else:
                logger.info("JobQueue not available; periodic cleanup is disabled.")
        except Exception as e:
            logger.warning("job_queue scheduling failed: %s", e)

        app.add_error_handler(on_error)

        # Fully-async lifecycle (no Updater.idle in PTB v20+)
        await app.initialize()

        # IMPORTANT: clear any leftover webhook before polling to avoid conflicts
        try:
            await app.bot.delete_webhook(drop_pending_updates=False)
            logger.info("Cleared webhook before starting polling.")
        except Exception as e:
            logger.debug("delete_webhook failed (non-fatal): %s", e)

        await app.start()
        await app.updater.start_polling()

        # Block forever until process is stopped
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            pass
        finally:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()

def main() -> None:
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
