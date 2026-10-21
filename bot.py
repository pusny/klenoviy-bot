import subprocess
import sys

def _ensure(pkg_spec: str, import_name: str) -> None:
    try:
        __import__(import_name)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg_spec])

_ensure("python-telegram-bot>=20,<22", "telegram")
_ensure("python-dotenv", "dotenv")

import os
import gc
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

def _req(key: str) -> str:
    val = os.getenv(key)
    if val is None or val.strip() == "":
        raise RuntimeError(f"Не задана обязательная переменная окружения: {key}. Проверь .env в рабочей директории ({Path.cwd()})")
    return val.strip()

def _req_int(key: str) -> int:
    val = _req(key)
    try:
        return int(val)
    except ValueError:
        raise RuntimeError(f"{key} должен быть числом, получено: {val!r}")

def _opt(key: str, default: str = "") -> str:
    val = os.getenv(key)
    return default if val is None else val.strip()

def _opt_int(key: str, default: Optional[int] = None) -> Optional[int]:
    val = os.getenv(key)
    if val is None or val.strip() == "":
        return default
    try:
        return int(val.strip())
    except ValueError:
        return default

def _opt_float(key: str, default: float) -> float:
    val = os.getenv(key)
    if val is None or val.strip() == "":
        return default
    try:
        return float(val.strip())
    except ValueError:
        return default

BOT_TOKEN        = _req("BOT_TOKEN")
ADMIN_CHAT_ID    = _req_int("ADMIN_CHAT_ID")
TARGET_CHAT_ID   = _req_int("TARGET_CHAT_ID")
CHAT_INVITE_LINK = _opt("CHAT_INVITE_LINK", "")
RULES_LINK       = _req("RULES_LINK")

TOPIC_THREAD_ID:      Optional[int] = _opt_int("TOPIC_THREAD_ID", None)
APPEAL_THREAD_ID:     Optional[int] = _opt_int("APPEAL_THREAD_ID", None)
MODERATOR_THREAD_ID:  Optional[int] = _opt_int("MODERATOR_THREAD_ID", None)
MOD_APPROVED_CHAT_ID: Optional[int] = _opt_int("MOD_APPROVED_CHAT_ID", None)

_data_dir = (os.getenv("DATA_DIR") or "").strip()
if _data_dir:
    DB_FILE = Path(_data_dir) / "bot.db"
else:
    DB_FILE = Path(_opt("DB_PATH", "bot.db"))

DB_PATH = str(DB_FILE)

THROTTLE_SECONDS      = _opt_float("THROTTLE_SECONDS", 2.0)
PING_COOLDOWN_SECONDS = _opt_float("PING_COOLDOWN_SECONDS", 3600.0)
CONNECT_TIMEOUT       = _opt_float("CONNECT_TIMEOUT", 30.0)
READ_TIMEOUT          = _opt_float("READ_TIMEOUT", 30.0)
WRITE_TIMEOUT         = _opt_float("WRITE_TIMEOUT", 30.0)
POOL_TIMEOUT          = _opt_float("POOL_TIMEOUT", 30.0)
START_RETRY_DELAY     = _opt_float("START_RETRY_DELAY", 10.0)

APPEAL_BLOCK_DAYS = int(_opt_float("APPEAL_BLOCK_DAYS", 14.0))

MAX_CONCURRENT_UPDATES = int(_opt_float("MAX_CONCURRENT_UPDATES", 16))
WATCHDOG_INTERVAL      = _opt_float("WATCHDOG_INTERVAL", 60.0)
WATCHDOG_MAX_FAILS     = int(_opt_float("WATCHDOG_MAX_FAILS", 5.0))
RESTART_MIN_DELAY      = _opt_float("RESTART_MIN_DELAY", 3.0)
RESTART_MAX_DELAY      = _opt_float("RESTART_MAX_DELAY", 30.0)

SOFT_PARALLEL_LIMIT    = int(_opt_float("SOFT_PARALLEL_LIMIT", 10))
SESSION_TIMEOUT        = _opt_float("SESSION_TIMEOUT", 900.0)

QUEUE_NOTIFY_COOLDOWN  = _opt_float("QUEUE_NOTIFY_COOLDOWN", 10.0)
QUEUE_NOTIFY_GAP       = _opt_float("QUEUE_NOTIFY_GAP", 0.05)
QUEUE_START_REPEAT_SEC = _opt_float("QUEUE_START_REPEAT_SEC", 20.0)

GC_INTERVAL            = _opt_float("GC_INTERVAL", 300.0)
MESSAGES_RETENTION_DAYS = _opt_float("MESSAGES_RETENTION_DAYS", 30.0)

import asyncio
import html
import logging
import re
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler

from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import NetworkError, TimedOut, RetryAfter, Forbidden, BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

Path("logs").mkdir(exist_ok=True)
_fmt = logging.Formatter(
    "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    "%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("klenoviy")
log.setLevel(logging.INFO)
if not log.handlers:
    _c = logging.StreamHandler()
    _c.setFormatter(_fmt)
    log.addHandler(_c)
    _f = RotatingFileHandler("logs/bot.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    _f.setFormatter(_fmt)
    log.addHandler(_f)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

DB_DIR = DB_FILE.parent if str(DB_FILE.parent) not in ("", ".") else Path(".")
try:
    DB_DIR.mkdir(parents=True, exist_ok=True)
except Exception as _e:
    raise RuntimeError(f"Не могу создать директорию БД {DB_DIR}: {_e}")

def _persist_marker() -> Path:
    return DB_DIR / ".persist_marker"

def _check_persistence() -> bool:
    m = _persist_marker()
    now = str(int(time.time()))
    try:
        prev = m.read_text().strip() if m.exists() else None
        m.write_text(now)
        return prev is not None
    except Exception:
        return False

def _backup_rotate(keep: int = 5) -> None:
    if not DB_FILE.exists():
        return
    ts = time.strftime("%Y%m%d_%H%M%S")
    for suffix in ("", "-wal", "-shm", "-journal"):
        src = Path(str(DB_FILE) + suffix)
        if not src.exists():
            continue
        dst = Path(str(DB_FILE) + f".{ts}.bak{suffix}")
        try:
            shutil.copy2(src, dst)
        except Exception:
            pass
    try:
        for suffix in ("", "-wal", "-shm", "-journal"):
            pattern = DB_FILE.name + ".*.bak" + suffix
            files = sorted(DB_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
            for old in files[keep:]:
                try:
                    old.unlink()
                except Exception:
                    pass
    except Exception:
        pass

_persist_ok = _check_persistence()

log.info("Config loaded | cwd=%s | admin_chat=%s | target_chat=%s | thread=%s | appeal_thread=%s | mod_thread=%s | mod_chat=%s",
         Path.cwd(), ADMIN_CHAT_ID, TARGET_CHAT_ID, TOPIC_THREAD_ID, APPEAL_THREAD_ID,
         MODERATOR_THREAD_ID, MOD_APPROVED_CHAT_ID)
log.info("DB path → %s (dir=%s, persist=%s)", DB_FILE, DB_DIR, "yes" if _persist_ok else "NEW/UNKNOWN")
if not _persist_ok:
    log.warning("Маркер персистентности отсутствует — первый запуск либо volume НЕ сохраняется.")

SLOT_STATES = ("new", "awaiting_form", "awaiting_video")

_active_sessions: dict[int, float] = {}
_queue_order: list[int] = []
_queue_bots: dict[int, object] = {}
_queue_lock = asyncio.Lock()

_queue_positions: dict[int, int] = {}
_queue_last_shown: dict[int, float] = {}
_queue_notify_at: dict[int, float] = {}
_notify_task: Optional[asyncio.Task] = None

_restart_event: Optional[asyncio.Event] = None
_watchdog_fails = 0

def _request_restart(reason: str = "") -> None:
    global _restart_event
    if _restart_event is not None and not _restart_event.is_set():
        log.warning("restart requested: %s", reason)
        _restart_event.set()

async def _try_acquire_slot(user_id: int) -> tuple[bool, int]:
    async with _queue_lock:
        if user_id in _active_sessions:
            _active_sessions[user_id] = time.monotonic()
            return True, 0
        if user_id in _queue_order:
            return False, _queue_order.index(user_id) + 1
        if len(_active_sessions) < SOFT_PARALLEL_LIMIT:
            _active_sessions[user_id] = time.monotonic()
            return True, 0
        _queue_order.append(user_id)
        return False, len(_queue_order)

async def _queue_set_bot(user_id: int, bot) -> None:
    async with _queue_lock:
        _queue_bots[user_id] = bot

async def _release_slot(user_id: int) -> None:
    async with _queue_lock:
        _active_sessions.pop(user_id, None)
        if user_id in _queue_order:
            _queue_order.remove(user_id)
        _queue_bots.pop(user_id, None)
        _queue_positions.pop(user_id, None)
        _queue_last_shown.pop(user_id, None)
        _queue_notify_at.pop(user_id, None)
    await _advance_queue()

async def _advance_queue() -> None:
    promoted = False
    rotated_without_bot = 0
    while True:
        async with _queue_lock:
            if not _queue_order or len(_active_sessions) >= SOFT_PARALLEL_LIMIT:
                break
            if rotated_without_bot >= len(_queue_order):
                break
            next_uid = _queue_order[0]
            next_bot = _queue_bots.get(next_uid)
            if next_bot is None:
                _queue_order.pop(0)
                _queue_order.append(next_uid)
                rotated_without_bot += 1
                continue
            _queue_order.pop(0)
            rotated_without_bot = 0
            _queue_bots.pop(next_uid, None)
            _active_sessions[next_uid] = time.monotonic()
            _queue_positions.pop(next_uid, None)
            _queue_last_shown.pop(next_uid, None)
            _queue_notify_at.pop(next_uid, None)
            promoted = True

        try:
            db_user = await get_user(next_uid)
        except Exception:
            db_user = None

        state = db_user["state"] if db_user else "new"
        try:
            if state == "awaiting_form":
                await send_msg(next_bot, next_uid, text=FORM_TEXT, parse_mode=ParseMode.HTML)
            elif state == "awaiting_video":
                await send_msg(next_bot, next_uid, text=VIDEO_REQUEST_TEXT, parse_mode=ParseMode.HTML)
            else:
                await send_msg(
                    next_bot, next_uid,
                    text=RULES_TEXT,
                    parse_mode=ParseMode.HTML,
                    reply_markup=rules_keyboard(),
                )
            log.info("Слот выдан юзеру %s из очереди (state=%s)", next_uid, state)
        except Exception as e:
            log.warning("Не удалось выдать слот %s: %s", next_uid, e)

    if promoted:
        _schedule_queue_notify()

async def _queue_notify_one(bot, user_id: int, pos: int) -> None:
    _queue_positions[user_id] = pos
    _queue_last_shown[user_id] = time.monotonic()
    _queue_notify_at[user_id] = time.monotonic() + QUEUE_NOTIFY_COOLDOWN
    await send_msg(bot, user_id, text=_queue_text(pos), parse_mode=ParseMode.HTML)

async def _notify_queue_positions() -> None:
    async with _queue_lock:
        snapshot = list(_queue_order)
        bots = dict(_queue_bots)

    now = time.monotonic()
    sent = 0
    for i, uid in enumerate(snapshot):
        pos = i + 1
        if _queue_positions.get(uid) == pos:
            continue
        if now < _queue_notify_at.get(uid, 0.0):
            continue
        bot = bots.get(uid)
        if bot is None:
            continue
        try:
            await _queue_notify_one(bot, uid, pos)
            sent += 1
        except Exception as e:
            log.warning("Не смог обновить позицию %s: %s", uid, e)
        await asyncio.sleep(QUEUE_NOTIFY_GAP)

    if sent:
        log.info("Обновлено позиций в очереди: %d (всего в очереди: %d)", sent, len(snapshot))

def _schedule_queue_notify() -> None:
    global _notify_task
    if _notify_task is not None and not _notify_task.done():
        return
    _notify_task = asyncio.create_task(_notify_queue_positions())

_last_seen: dict[int, float] = defaultdict(float)
_last_ping: dict[int, float] = defaultdict(float)
_last_cb: dict[int, float] = defaultdict(float)

def _gc_throttle_dicts() -> None:
    now = time.monotonic()
    for d, ttl in (
        (_last_seen, THROTTLE_SECONDS * 20),
        (_last_ping, PING_COOLDOWN_SECONDS),
        (_last_cb, 120.0),
    ):
        for k in [k for k, v in d.items() if now - v > ttl]:
            d.pop(k, None)

async def _housekeeping() -> None:
    while True:
        await asyncio.sleep(GC_INTERVAL)
        try:
            now = time.monotonic()
            stale: list[int] = []
            async with _queue_lock:
                for uid, ts in list(_active_sessions.items()):
                    if now - ts > SESSION_TIMEOUT:
                        stale.append(uid)
                        _active_sessions.pop(uid, None)
            if stale:
                log.info("Освобождено %d устаревших сессий: %s", len(stale), stale)
                await _advance_queue()

            _gc_throttle_dicts()

            try:
                cutoff_days = int(MESSAGES_RETENTION_DAYS)
                if cutoff_days > 0:
                    await db_exec(
                        "DELETE FROM applications WHERE status IN ('approved','rejected') "
                        "AND created_at < datetime('now', ?)",
                        (f"-{cutoff_days} days",),
                    )
                    await db_exec(
                        "DELETE FROM appeals WHERE status IN ('approved','rejected','new_link') "
                        "AND created_at < datetime('now', ?)",
                        (f"-{cutoff_days} days",),
                    )
                    await db_exec(
                        "DELETE FROM moderator_applications WHERE status IN ('approved','rejected') "
                        "AND created_at < datetime('now', ?)",
                        (f"-{cutoff_days} days",),
                    )
            except Exception as e:
                log.debug("housekeeping retention: %s", e)

            try:
                gc.collect()
            except Exception:
                pass

        except Exception as e:
            log.debug("housekeeping loop err: %s", e)

def _queue_text(pos: int) -> str:
    return (
        "⏳ <b>Вы в очереди</b>\n\n"
        "Сейчас бот перегружен — много людей заполняют заявки одновременно.\n\n"
        f"📊 <b>Ваша позиция:</b> {pos}\n\n"
        "Как только освободится место — вам автоматически придёт следующий шаг.\n"
        "Повторно писать не нужно 🙏"
    )

def esc(value) -> str:
    if value is None:
        return ""
    return html.escape(str(value), quote=False)

async def safe(coro_fn, *args, retries: int = 3, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return await coro_fn(*args, **kwargs)
        except RetryAfter as e:
            wait = getattr(e, "retry_after", 3)
            log.warning("safe(): Telegram RetryAfter — ждём %.1f сек (попытка %d/%d)", wait, attempt, retries)
            await asyncio.sleep(wait + 1)
            continue
        except Forbidden as e:
            log.warning("safe(): Forbidden — %s", e)
            return None
        except BadRequest as e:
            log.warning("safe(): BadRequest — %s", e)
            return None
        except (TimedOut, NetworkError) as e:
            if attempt == retries:
                log.warning("safe(): окончательно упало — %s", e)
                return None
            await asyncio.sleep(1.5 * attempt)
        except Exception as e:
            log.warning("safe(): non-network error — %s: %s", type(e).__name__, e, exc_info=True)
            return None

def _topic_kwargs() -> dict:
    if TOPIC_THREAD_ID:
        return {"message_thread_id": TOPIC_THREAD_ID}
    return {}

def _appeal_topic_kwargs() -> dict:
    if APPEAL_THREAD_ID:
        return {"message_thread_id": APPEAL_THREAD_ID}
    return {}

def _mod_topic_kwargs() -> dict:
    if MODERATOR_THREAD_ID:
        return {"message_thread_id": MODERATOR_THREAD_ID}
    if TOPIC_THREAD_ID:
        return {"message_thread_id": TOPIC_THREAD_ID}
    return {}

def _mod_chat_id() -> int:
    return MOD_APPROVED_CHAT_ID or ADMIN_CHAT_ID

async def send_msg(bot, chat_id: int, *, text: str, **kwargs):
    return await safe(bot.send_message, chat_id=chat_id, text=text, **kwargs)

async def strip_buttons(bot, chat_id: int, message_id: int) -> None:
    await safe(
        bot.edit_message_reply_markup,
        chat_id=chat_id,
        message_id=message_id,
        reply_markup=None,
    )

TOTAL_STEPS = 3

def step_header(step: int, title: str) -> str:
    return f"<b>Шаг {step}/{TOTAL_STEPS}</b> · <i>{title}</i>"

def step_done(step: int) -> str:
    left = TOTAL_STEPS - step
    if left <= 0:
        return f"✅ <b>Шаг {step}/{TOTAL_STEPS} пройден</b> · <i>всё готово</i>"
    return f"✅ <b>Шаг {step}/{TOTAL_STEPS} пройден</b> · <i>осталось {left}</i>"

def _future_utc_str(days: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

def _pretty_dt(db_ts: str) -> str:
    if not db_ts:
        return "—"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(db_ts, fmt).replace(tzinfo=timezone.utc)
            return dt.strftime("%d.%m.%Y %H:%M UTC")
        except Exception:
            continue
    return str(db_ts)

_admins_cache: dict[str, object] = {"mentions": "", "fetched_at": 0.0}

async def _fetch_admins_mentions(bot) -> str:
    now = time.monotonic()
    if now - float(_admins_cache["fetched_at"]) < 300.0 and _admins_cache["mentions"]:
        return str(_admins_cache["mentions"])

    mentions: list[str] = []
    try:
        admins = await bot.get_chat_administrators(ADMIN_CHAT_ID)
        for a in admins:
            u = a.user
            if u is None or u.is_bot:
                continue
            if u.username:
                mentions.append(f"@{u.username}")
    except Exception as e:
        log.warning("Не могу получить список админов: %s", e)

    _admins_cache["mentions"] = " ".join(mentions)
    _admins_cache["fetched_at"] = now
    return str(_admins_cache["mentions"])

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id          INTEGER PRIMARY KEY,
    username         TEXT,
    first_name       TEXT,
    state            TEXT NOT NULL DEFAULT 'new',
    pre_appeal_state TEXT,
    mod_state        TEXT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS applications (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id               INTEGER NOT NULL,
    username_at_submit    TEXT,
    first_name_at_submit  TEXT,
    name                  TEXT NOT NULL,
    age                   TEXT NOT NULL,
    diseases              TEXT NOT NULL,
    reason                TEXT NOT NULL,
    about                 TEXT,
    status                TEXT NOT NULL DEFAULT 'pending',
    admin_msg_id          INTEGER,
    video_note_file_id    TEXT,
    video_admin_msg_id    INTEGER,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    decided_by            INTEGER,
    decided_at            TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_applications_user_status
    ON applications (user_id, status);

CREATE TABLE IF NOT EXISTS appeals (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id               INTEGER NOT NULL,
    username_at_submit    TEXT,
    first_name_at_submit  TEXT,
    name                  TEXT NOT NULL,
    age                   TEXT NOT NULL,
    punishment_type       TEXT NOT NULL,
    description           TEXT NOT NULL,
    status                TEXT NOT NULL DEFAULT 'pending',
    admin_msg_id          INTEGER,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    decided_by            INTEGER,
    decided_at            TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_appeals_user_status
    ON appeals (user_id, status);

CREATE TABLE IF NOT EXISTS blacklist (
    user_id     INTEGER PRIMARY KEY,
    reason      TEXT,
    blocked_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS invite_links (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    invite_link TEXT NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    used        INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_invite_links_user
    ON invite_links (user_id, used);

CREATE INDEX IF NOT EXISTS idx_invite_links_link
    ON invite_links (invite_link);

CREATE TABLE IF NOT EXISTS moderator_recruitment (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    is_open          INTEGER NOT NULL DEFAULT 0,
    round_started_at TIMESTAMP,
    opened_at        TIMESTAMP,
    closed_at        TIMESTAMP,
    opened_by        INTEGER,
    closed_by        INTEGER
);

INSERT OR IGNORE INTO moderator_recruitment (id, is_open) VALUES (1, 0);

CREATE TABLE IF NOT EXISTS moderator_applications (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id              INTEGER NOT NULL,
    round_started_at     TIMESTAMP NOT NULL,
    username_at_submit   TEXT,
    first_name_at_submit TEXT,
    name                 TEXT NOT NULL,
    age                  TEXT NOT NULL,
    about                TEXT NOT NULL,
    motivation           TEXT NOT NULL,
    video_note_file_id   TEXT,
    status               TEXT NOT NULL DEFAULT 'pending',
    admin_msg_id         INTEGER,
    video_admin_msg_id   INTEGER,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    decided_by           INTEGER,
    decided_at           TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_mod_apps_user_round
    ON moderator_applications (user_id, round_started_at);

CREATE TABLE IF NOT EXISTS moderator_responses (
    user_id          INTEGER NOT NULL,
    round_started_at TIMESTAMP NOT NULL,
    notified         INTEGER NOT NULL DEFAULT 0,
    refused          INTEGER NOT NULL DEFAULT 0,
    applied          INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, round_started_at)
);
"""

def _sync_init_db() -> None:
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    try:
        conn.execute("PRAGMA journal_mode=DELETE;")
        conn.execute("PRAGMA synchronous=FULL;")
        conn.execute("PRAGMA busy_timeout=15000;")
        conn.executescript(SCHEMA)

        u_cols = {row[1] for row in conn.execute("PRAGMA table_info(users)")}
        if "pre_appeal_state" not in u_cols:
            conn.execute("ALTER TABLE users ADD COLUMN pre_appeal_state TEXT")
        if "mod_state" not in u_cols:
            conn.execute("ALTER TABLE users ADD COLUMN mod_state TEXT")

        cols = {row[1] for row in conn.execute("PRAGMA table_info(applications)")}
        if "video_note_file_id" not in cols:
            conn.execute("ALTER TABLE applications ADD COLUMN video_note_file_id TEXT")
        if "video_admin_msg_id" not in cols:
            conn.execute("ALTER TABLE applications ADD COLUMN video_admin_msg_id INTEGER")

        bl_cols = {row[1] for row in conn.execute("PRAGMA table_info(blacklist)")}
        if "expires_at" not in bl_cols:
            conn.execute("ALTER TABLE blacklist ADD COLUMN expires_at TIMESTAMP")

        conn.commit()
    finally:
        conn.close()

def _sync_exec(query: str, params: tuple = (), fetch: Optional[str] = None):
    last_err = None
    for attempt in range(5):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30.0)
            try:
                conn.execute("PRAGMA journal_mode=DELETE;")
                conn.execute("PRAGMA synchronous=FULL;")
                conn.execute("PRAGMA busy_timeout=15000;")
                conn.row_factory = sqlite3.Row
                cur = conn.execute(query, params)
                if fetch == "one":
                    return cur.fetchone()
                if fetch == "all":
                    return cur.fetchall()
                conn.commit()
                return cur.lastrowid
            finally:
                conn.close()
        except sqlite3.OperationalError as e:
            last_err = e
            msg = str(e).lower()
            if ("locked" in msg or "busy" in msg) and attempt < 4:
                time.sleep(0.25 * (attempt + 1))
                continue
            raise
    if last_err:
        raise last_err

async def db_exec(query: str, params: tuple = (), fetch: Optional[str] = None):
    return await asyncio.to_thread(_sync_exec, query, params, fetch)

async def upsert_user(user_id: int, username: Optional[str], first_name: Optional[str]) -> None:
    await db_exec(
        """
        INSERT INTO users (user_id, username, first_name, state, created_at, updated_at)
        VALUES (?, ?, ?, 'new', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            updated_at = CURRENT_TIMESTAMP
        """,
        (user_id, username, first_name),
    )

async def set_user_state(user_id: int, state: str) -> None:
    await db_exec("UPDATE users SET state = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?", (state, user_id))

async def set_pre_appeal_state(user_id: int, state: Optional[str]) -> None:
    await db_exec("UPDATE users SET pre_appeal_state = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?", (state, user_id))

async def set_mod_state(user_id: int, state: Optional[str]) -> None:
    await db_exec("UPDATE users SET mod_state = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?", (state, user_id))

async def get_user(user_id: int):
    return await db_exec("SELECT * FROM users WHERE user_id = ?", (user_id,), fetch="one")

async def is_blacklisted(user_id: int) -> bool:
    row = await db_exec(
        "SELECT 1 FROM blacklist WHERE user_id = ? AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
        (user_id,), fetch="one",
    )
    return row is not None

async def get_blacklist_row(user_id: int):
    return await db_exec("SELECT * FROM blacklist WHERE user_id = ?", (user_id,), fetch="one")

async def add_to_blacklist(user_id: int, reason: str, expires_at: Optional[str] = None) -> None:
    await db_exec(
        """
        INSERT INTO blacklist (user_id, reason, blocked_at, expires_at)
        VALUES (?, ?, CURRENT_TIMESTAMP, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            reason = excluded.reason,
            blocked_at = excluded.blocked_at,
            expires_at = excluded.expires_at
        """,
        (user_id, reason, expires_at),
    )

async def remove_from_blacklist(user_id: int) -> None:
    await db_exec("DELETE FROM blacklist WHERE user_id = ?", (user_id,))
    await db_exec("UPDATE users SET state = 'new', updated_at = CURRENT_TIMESTAMP WHERE user_id = ?", (user_id,))

async def remove_permanent_blacklist(user_id: int) -> bool:
    row = await db_exec("SELECT 1 FROM blacklist WHERE user_id = ? AND expires_at IS NULL", (user_id,), fetch="one")
    if row is None:
        return False
    await db_exec("DELETE FROM blacklist WHERE user_id = ? AND expires_at IS NULL", (user_id,))
    await db_exec("UPDATE users SET state = 'new', updated_at = CURRENT_TIMESTAMP WHERE user_id = ?", (user_id,))
    return True

async def has_active_application(user_id: int) -> bool:
    row = await db_exec(
        "SELECT 1 FROM applications WHERE user_id = ? AND status IN ('pending', 'awaiting_video')",
        (user_id,), fetch="one",
    )
    return row is not None

async def get_latest_application(user_id: int):
    return await db_exec("SELECT * FROM applications WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user_id,), fetch="one")

async def create_application(
    user_id: int,
    username_at_submit: Optional[str],
    first_name_at_submit: Optional[str],
    name: str, age: str, diseases: str, reason: str, about: Optional[str],
    status: str = "pending",
) -> int:
    return await db_exec(
        """
        INSERT INTO applications
            (user_id, username_at_submit, first_name_at_submit, name, age, diseases, reason, about, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (user_id, username_at_submit, first_name_at_submit, name, age, diseases, reason, about, status),
    )

async def get_application(app_id: int):
    return await db_exec("SELECT * FROM applications WHERE id = ?", (app_id,), fetch="one")

async def set_application_status(app_id: int, status: str) -> None:
    await db_exec("UPDATE applications SET status = ? WHERE id = ?", (status, app_id))

async def set_video_note(app_id: int, file_id: str) -> None:
    await db_exec("UPDATE applications SET video_note_file_id = ? WHERE id = ?", (file_id, app_id))

async def set_admin_msg_id(app_id: int, admin_msg_id: int) -> None:
    await db_exec("UPDATE applications SET admin_msg_id = ? WHERE id = ?", (admin_msg_id, app_id))

async def set_video_admin_msg_id(app_id: int, msg_id: int) -> None:
    await db_exec("UPDATE applications SET video_admin_msg_id = ? WHERE id = ?", (msg_id, app_id))

async def try_decide_application(app_id: int, status: str, decided_by: int) -> bool:
    row = await db_exec(
        """
        UPDATE applications
        SET status = ?, decided_by = ?, decided_at = CURRENT_TIMESTAMP
        WHERE id = ? AND status = 'pending'
        RETURNING id
        """,
        (status, decided_by, app_id), fetch="one",
    )
    return row is not None

async def create_appeal(
    user_id: int,
    username_at_submit: Optional[str],
    first_name_at_submit: Optional[str],
    name: str, age: str, punishment_type: str, description: str,
    status: str = "pending",
) -> int:
    return await db_exec(
        """
        INSERT INTO appeals
            (user_id, username_at_submit, first_name_at_submit, name, age, punishment_type, description, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (user_id, username_at_submit, first_name_at_submit, name, age, punishment_type, description, status),
    )

async def get_appeal(appeal_id: int):
    return await db_exec("SELECT * FROM appeals WHERE id = ?", (appeal_id,), fetch="one")

async def get_latest_appeal(user_id: int):
    return await db_exec("SELECT * FROM appeals WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user_id,), fetch="one")

async def set_appeal_admin_msg_id(appeal_id: int, msg_id: int) -> None:
    await db_exec("UPDATE appeals SET admin_msg_id = ? WHERE id = ?", (msg_id, appeal_id))

async def try_decide_appeal(appeal_id: int, status: str, decided_by: int) -> bool:
    row = await db_exec(
        """
        UPDATE appeals
        SET status = ?, decided_by = ?, decided_at = CURRENT_TIMESTAMP
        WHERE id = ? AND status = 'pending'
        RETURNING id
        """,
        (status, decided_by, appeal_id), fetch="one",
    )
    return row is not None

async def save_invite_link(user_id: int, invite_link: str) -> None:
    await db_exec("UPDATE invite_links SET used = 1 WHERE user_id = ? AND used = 0", (user_id,))
    await db_exec("INSERT INTO invite_links (user_id, invite_link, used) VALUES (?, ?, 0)", (user_id, invite_link))

async def has_active_invite(user_id: int) -> bool:
    row = await db_exec("SELECT 1 FROM invite_links WHERE user_id = ? AND used = 0 LIMIT 1", (user_id,), fetch="one")
    return row is not None

async def get_active_invite_link(user_id: int):
    row = await db_exec(
        "SELECT invite_link FROM invite_links WHERE user_id = ? AND used = 0 ORDER BY id DESC LIMIT 1",
        (user_id,), fetch="one",
    )
    return row["invite_link"] if row else None

async def get_invite_owner(invite_link: str):
    return await db_exec("SELECT user_id FROM invite_links WHERE invite_link = ? LIMIT 1", (invite_link,), fetch="one")

async def mark_invite_used(user_id: int) -> None:
    await db_exec("UPDATE invite_links SET used = 1 WHERE user_id = ? AND used = 0", (user_id,))

async def get_recruitment():
    return await db_exec("SELECT * FROM moderator_recruitment WHERE id = 1", fetch="one")

async def open_recruitment(admin_id: int) -> bool:
    rec = await get_recruitment()
    if rec and int(rec["is_open"]) == 1:
        return False
    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    await db_exec(
        """
        INSERT INTO moderator_recruitment (id, is_open, round_started_at, opened_at, opened_by)
        VALUES (1, 1, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            is_open = 1,
            round_started_at = excluded.round_started_at,
            opened_at = excluded.opened_at,
            opened_by = excluded.opened_by,
            closed_at = NULL,
            closed_by = NULL
        """,
        (now_ts, now_ts, admin_id),
    )
    return True

async def close_recruitment(admin_id: int) -> bool:
    rec = await get_recruitment()
    if not rec or int(rec["is_open"]) == 0:
        return False
    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    await db_exec(
        "UPDATE moderator_recruitment SET is_open = 0, closed_at = ?, closed_by = ? WHERE id = 1",
        (now_ts, admin_id),
    )
    return True

async def get_mod_response(user_id: int, round_started_at: str):
    return await db_exec(
        "SELECT * FROM moderator_responses WHERE user_id = ? AND round_started_at = ?",
        (user_id, round_started_at), fetch="one",
    )

async def mark_mod_notified(user_id: int, round_started_at: str) -> None:
    await db_exec(
        "INSERT OR IGNORE INTO moderator_responses (user_id, round_started_at, notified) VALUES (?, ?, 1)",
        (user_id, round_started_at),
    )

async def mark_mod_refused(user_id: int, round_started_at: str) -> None:
    await db_exec(
        """
        INSERT INTO moderator_responses (user_id, round_started_at, notified, refused)
        VALUES (?, ?, 1, 1)
        ON CONFLICT(user_id, round_started_at) DO UPDATE SET refused = 1
        """,
        (user_id, round_started_at),
    )

async def mark_mod_applied(user_id: int, round_started_at: str) -> None:
    await db_exec(
        """
        INSERT INTO moderator_responses (user_id, round_started_at, notified, applied)
        VALUES (?, ?, 1, 1)
        ON CONFLICT(user_id, round_started_at) DO UPDATE SET applied = 1
        """,
        (user_id, round_started_at),
    )

async def get_approved_users_ids() -> list[int]:
    rows = await db_exec("SELECT user_id FROM users WHERE state = 'approved'", fetch="all")
    return [int(r["user_id"]) for r in rows] if rows else []

async def get_notified_users_ids(round_started_at: str) -> list[int]:
    rows = await db_exec(
        "SELECT user_id FROM moderator_responses WHERE round_started_at = ? AND notified = 1",
        (round_started_at,), fetch="all",
    )
    return [int(r["user_id"]) for r in rows] if rows else []

async def has_pending_mod_application(user_id: int) -> bool:
    row = await db_exec(
        "SELECT 1 FROM moderator_applications WHERE user_id = ? AND status = 'pending' LIMIT 1",
        (user_id,), fetch="one",
    )
    return row is not None

async def has_active_mod_application(user_id: int) -> bool:
    row = await db_exec(
        "SELECT 1 FROM moderator_applications WHERE user_id = ? AND status IN ('pending', 'awaiting_video') LIMIT 1",
        (user_id,), fetch="one",
    )
    return row is not None

async def has_approved_mod_application(user_id: int) -> bool:
    row = await db_exec(
        "SELECT 1 FROM moderator_applications WHERE user_id = ? AND status = 'approved' LIMIT 1",
        (user_id,), fetch="one",
    )
    return row is not None

async def create_mod_application(
    user_id: int,
    round_started_at: str,
    username_at_submit: Optional[str],
    first_name_at_submit: Optional[str],
    name: str,
    age: str,
    about: str,
    motivation: str,
    status: str = "awaiting_video",
) -> int:
    return await db_exec(
        """
        INSERT INTO moderator_applications
            (user_id, round_started_at, username_at_submit, first_name_at_submit, name, age, about, motivation, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (user_id, round_started_at, username_at_submit, first_name_at_submit, name, age, about, motivation, status),
    )

async def get_mod_application(app_id: int):
    return await db_exec("SELECT * FROM moderator_applications WHERE id = ?", (app_id,), fetch="one")

async def get_latest_mod_application(user_id: int):
    return await db_exec("SELECT * FROM moderator_applications WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user_id,), fetch="one")

async def set_mod_video_note(app_id: int, file_id: str) -> None:
    await db_exec("UPDATE moderator_applications SET video_note_file_id = ? WHERE id = ?", (file_id, app_id))

async def set_mod_application_status(app_id: int, status: str) -> None:
    await db_exec("UPDATE moderator_applications SET status = ? WHERE id = ?", (status, app_id))

async def set_mod_admin_msg_id(app_id: int, msg_id: int) -> None:
    await db_exec("UPDATE moderator_applications SET admin_msg_id = ? WHERE id = ?", (msg_id, app_id))

async def set_mod_video_admin_msg_id(app_id: int, msg_id: int) -> None:
    await db_exec("UPDATE moderator_applications SET video_admin_msg_id = ? WHERE id = ?", (msg_id, app_id))

async def try_decide_mod_application(app_id: int, status: str, decided_by: int) -> bool:
    row = await db_exec(
        """
        UPDATE moderator_applications
        SET status = ?, decided_by = ?, decided_at = CURRENT_TIMESTAMP
        WHERE id = ? AND status = 'pending'
        RETURNING id
        """,
        (status, decided_by, app_id), fetch="one",
    )
    return row is not None

async def reset_stuck_mod_states() -> int:
    row = await db_exec(
        """
        UPDATE users
        SET mod_state = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE mod_state IN ('mod_awaiting_form', 'mod_awaiting_video')
        RETURNING user_id
        """,
        fetch="all",
    )
    return len(row) if row else 0

REQUIRED_FIELDS = ("name", "age", "diseases", "reason")

FIELD_PATTERNS = {
    "name":     r"имя",
    "age":      r"возраст",
    "diseases": r"болезни(?:\s+физические\s*/\s*психические)?",
    "reason":   r"почему\s+хотите\s+зайти\s+к\s+нам",
    "about":    r"о\s+себе(?:\s+по\s+желанию)?",
}

APPEAL_REQUIRED_FIELDS = ("name", "age", "punishment_type", "description")

APPEAL_FIELD_PATTERNS = {
    "name":            r"имя",
    "age":             r"возраст",
    "punishment_type": r"тип\s+наказания",
    "description":     r"краткое\s+описание",
}

MOD_REQUIRED_FIELDS = ("name", "age", "about", "motivation")

MOD_FIELD_PATTERNS = {
    "name":       r"имя",
    "age":        r"возраст",
    "about":      r"о\s+себе(?:\s*\(\s*кто\s+вы\s*\))?",
    "motivation": r"мотивация(?:\s*\(\s*зачем\s+и\s+почему\s*\))?",
}

@dataclass
class ParsedForm:
    name: str
    age: str
    diseases: str
    reason: str
    about: Optional[str]

def _parse_fields(text: str, patterns: dict, required: tuple) -> Optional[dict]:
    if not text:
        return None
    lines = [ln.rstrip() for ln in text.replace("\r", "").split("\n")]
    collected: dict[str, str] = {}
    current_key: Optional[str] = None

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            if current_key and current_key in collected:
                collected[current_key] += "\n"
            continue

        matched_key = None
        matched_value = ""
        if ":" in line:
            head, _, tail = line.partition(":")
            head_norm = head.strip().lower()
            for key, pattern in patterns.items():
                if re.fullmatch(pattern, head_norm, flags=re.IGNORECASE):
                    matched_key = key
                    matched_value = tail.strip()
                    break

        if matched_key:
            collected[matched_key] = matched_value
            current_key = matched_key
        elif current_key:
            collected[current_key] = (collected.get(current_key, "") + " " + line).strip()

    for key in required:
        if not collected.get(key, "").strip():
            return None
    return collected

def parse_form(text: str) -> Optional[ParsedForm]:
    data = _parse_fields(text, FIELD_PATTERNS, REQUIRED_FIELDS)
    if data is None:
        return None
    return ParsedForm(
        name=data["name"].strip(),
        age=data["age"].strip(),
        diseases=data["diseases"].strip(),
        reason=data["reason"].strip(),
        about=data.get("about", "").strip() or None,
    )

def parse_appeal(text: str) -> Optional[dict]:
    data = _parse_fields(text, APPEAL_FIELD_PATTERNS, APPEAL_REQUIRED_FIELDS)
    if data is None:
        return None
    return {
        "name": data["name"].strip(),
        "age": data["age"].strip(),
        "punishment_type": data["punishment_type"].strip(),
        "description": data["description"].strip(),
    }

def parse_mod_form(text: str) -> Optional[dict]:
    data = _parse_fields(text, MOD_FIELD_PATTERNS, MOD_REQUIRED_FIELDS)
    if data is None:
        return None
    return {
        "name": data["name"].strip(),
        "age": data["age"].strip(),
        "about": data["about"].strip(),
        "motivation": data["motivation"].strip(),
    }

def is_throttled(user_id: int) -> bool:
    now = time.monotonic()
    if now - _last_seen[user_id] < THROTTLE_SECONDS:
        return True
    _last_seen[user_id] = now
    return False

def is_cb_throttled(user_id: int) -> bool:
    now = time.monotonic()
    if now - _last_cb[user_id] < 1.0:
        return True
    _last_cb[user_id] = now
    return False

def can_ping(user_id: int) -> bool:
    now = time.monotonic()
    if now - _last_ping[user_id] < PING_COOLDOWN_SECONDS:
        return False
    _last_ping[user_id] = now
    return True

def ping_cooldown_left(user_id: int) -> int:
    now = time.monotonic()
    delta = PING_COOLDOWN_SECONDS - (now - _last_ping[user_id])
    return max(0, int(delta))

def rules_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Согласиться", callback_data="agree"), InlineKeyboardButton("❌ Отказаться", callback_data="refuse")]
    ])

def decision_keyboard(application_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Принять", callback_data=f"approve:{application_id}"), InlineKeyboardButton("❌ Отклонить", callback_data=f"reject:{application_id}")]
    ])

def waiting_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📣 Упомянуть администрацию", callback_data="ping_admins")],
    ])

def appeal_button_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚖️ Подать апелляцию", callback_data="appeal_start")],
    ])

def appeal_decision_keyboard(appeal_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Принять", callback_data=f"appeal_approve:{appeal_id}"), InlineKeyboardButton("❌ Отклонить", callback_data=f"appeal_reject:{appeal_id}")],
        [InlineKeyboardButton("🔁 Выдать новую ссылку", callback_data=f"appeal_newlink:{appeal_id}")],
        [InlineKeyboardButton("🛡 Выдать ссылку на админ-чат", callback_data=f"appeal_modlink:{appeal_id}")],
    ])

def mod_invite_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛡 Подать заявку", callback_data="mod_apply"), InlineKeyboardButton("❌ Отказаться", callback_data="mod_refuse")]
    ])

def mod_decision_keyboard(app_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Принять", callback_data=f"mod_approve:{app_id}"), InlineKeyboardButton("❌ Отклонить", callback_data=f"mod_reject:{app_id}")]
    ])

RULES_TEXT = (
    f"{step_header(1, 'Правила чата')}\n\n"
    "📋 <b>Правила чата Кленовый букетик</b>\n\n"
    "Ознакомьтесь с правилами по ссылке ниже и подтвердите согласие.\n\n"
    f"🔗 {RULES_LINK}"
)

FORM_TEXT = (
    f"{step_done(1)}\n\n"
    f"{step_header(2, 'Анкета')}\n\n"
    "Скопируйте шаблон ниже — <i>тап по тексту = копирование</i>.\n"
    "Заполните все поля и отправьте <b>одним сообщением</b>.\n\n"
    "━━━━━━━━━━━━━━━━━━━━\n"
    "<code>"
    "Имя: \n"
    "Возраст: \n"
    "Болезни физические/психические: \n"
    "Почему хотите зайти к нам: \n"
    "О себе по желанию: "
    "</code>\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "<i>Обязательные поля: Имя, Возраст, Болезни, Причина.</i>"
)

INVALID_FORM_TEXT = (
    "❌ <b>Анкета оформлена не по шаблону</b>\n\n"
    "Скопируйте шаблон из сообщения выше, заполните все обязательные поля и отправьте <b>одним сообщением</b>.\n\n"
    "Обязательные: <b>Имя</b>, <b>Возраст</b>, <b>Болезни</b>, <b>Причина</b>."
)

VIDEO_REQUEST_TEXT = (
    f"{step_done(2)}\n\n"
    f"{step_header(3, 'Видео-подтверждение')}\n\n"
    "📹 <b>Подтверждение возраста</b>\n\n"
    "Запишите <b>видео-кружок</b> (до 60 секунд), в котором вы называете свой возраст и имя, указанное в анкете.\n\n"
    "Значок 🎥 — <b>справа</b> от поля ввода, рядом с кнопкой отправки. Обычное видео или фото не подойдут.\n\n"
    "После получения кружка заявка уйдёт на рассмотрение."
)

VIDEO_REMINDER_TEXT = (
    "⚠️ <b>Нужен именно видео-кружок</b>\n\n"
    "Нажмите 🎥 <b>справа</b> от поля ввода и запишите короткое видео до 60 секунд, где называете свой возраст и имя из анкеты.\n\n"
    "Текст, фото или обычное видео не принимаются."
)

WAITING_TEXT = (
    f"{step_done(3)}\n\n"
    "⏳ <b>Заявка на рассмотрении</b>\n\n"
    "Ожидайте — администрация изучает вашу заявку.\n"
    "Если ответа нет долго — нажмите кнопку ниже, чтобы привлечь внимание."
)

REJECTED_MSG = (
    "❌ Вашу заявку <b>отклонили</b>.\n\n"
    "Если считаете решение ошибочным — можете подать апелляцию ниже."
)

APPEAL_FORM_TEXT = (
    "⚖️ <b>Апелляция</b>\n\n"
    "Скопируйте шаблон ниже — <i>тап по тексту = копирование</i>.\n"
    "Заполните все поля и отправьте <b>одним сообщением</b>.\n\n"
    "━━━━━━━━━━━━━━━━━━━━\n"
    "<code>"
    "Имя: \n"
    "Возраст: \n"
    "Тип наказания: \n"
    "Краткое описание: "
    "</code>\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "<i>Обязательные поля: Имя, Возраст, Тип наказания, Описание.</i>"
)

INVALID_APPEAL_TEXT = (
    "❌ <b>Апелляция оформлена не по шаблону</b>\n\n"
    "Скопируйте шаблон из сообщения выше, заполните все обязательные поля и отправьте <b>одним сообщением</b>.\n\n"
    "Обязательные: <b>Имя</b>, <b>Возраст</b>, <b>Тип наказания</b>, <b>Описание</b>."
)

APPEAL_WAITING_TEXT = (
    "⚖️ <b>Апелляция на рассмотрении</b>\n\n"
    "Администрация изучит её и сообщит решение. Ожидайте.\n"
    "Если ответа нет долго — нажмите кнопку ниже."
)

MOD_OPEN_TEXT = (
    "🛡 <b>Набор модераторов открыт!</b>\n\n"
    "Администрация чата открыла набор модераторов.\n\n"
    "Если хотите попробовать — нажмите «Подать заявку».\n\n"
    "<i>Если нажмёте «Отказаться» — заявку в этом наборе подать уже не сможете. Следующая возможность будет только когда администрация откроет новый набор.</i>"
)

MOD_FORM_TEXT = (
    "🛡 <b>Заявка на модератора</b>\n\n"
    "Скопируйте шаблон ниже — <i>тап по тексту = копирование</i>.\n"
    "Заполните все поля и отправьте <b>одним сообщением</b>.\n\n"
    "━━━━━━━━━━━━━━━━━━━━\n"
    "<code>"
    "Имя: \n"
    "Возраст: \n"
    "О себе (кто вы): \n"
    "Мотивация (зачем и почему): "
    "</code>\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "<i>Обязательные поля: Имя, Возраст, О себе, Мотивация.</i>"
)

MOD_INVALID_FORM_TEXT = (
    "❌ <b>Анкета оформлена не по шаблону</b>\n\n"
    "Скопируйте шаблон из сообщения выше, заполните все обязательные поля и отправьте <b>одним сообщением</b>."
)

MOD_VIDEO_REQUEST_TEXT = (
    "🎥 <b>Видео-подтверждение</b>\n\n"
    "Запишите <b>видео-кружок</b> (до 60 секунд), в котором коротко представитесь и подтвердите, что вы реальный человек.\n\n"
    "Значок 🎥 — <b>справа</b> от поля ввода, рядом с кнопкой отправки. Обычное видео или фото не подойдут."
)

MOD_VIDEO_REMINDER_TEXT = (
    "⚠️ <b>Нужен именно видео-кружок</b>\n\n"
    "Нажмите 🎥 <b>справа</b> от поля ввода и запишите короткое видео до 60 секунд."
)

MOD_WAITING_TEXT = (
    "⏳ <b>Заявка на модератора отправлена</b>\n\n"
    "Администрация рассмотрит её и сообщит решение."
)

MOD_REFUSED_TEXT = (
    "❌ Вы отказались от подачи заявки на модератора.\n\n"
    "В этом наборе заявку подать уже нельзя. Следующая возможность — когда администрация снова откроет набор."
)

MOD_CLOSED_TEXT = (
    "📪 <b>Набор модераторов закрыт</b>\n\n"
    "Администрация закрыла набор. Спасибо всем, кто откликнулся!"
)

MOD_APPROVED_MSG = (
    "✅ <b>Ваша заявка на модератора принята!</b>\n\n"
    "🛡 Добро пожаловать в команду."
)

MOD_APPROVED_MSG_NO_LINK = (
    "✅ <b>Ваша заявка на модератора принята!</b>\n\n"
    "🛡 Добро пожаловать в команду.\n\n"
    "Ссылку на админ-чат вам выдаст администратор."
)

MOD_REJECTED_MSG = (
    "❌ <b>Заявка на модератора отклонена</b>\n\n"
    "Спасибо за проявленный интерес."
)

async def _send_appeal_form(bot, user_id: int) -> None:
    await send_msg(bot, user_id, text=APPEAL_FORM_TEXT, parse_mode=ParseMode.HTML)

def _blocked_text(until_str: str) -> str:
    return (
        "🚫 <b>Доступ заблокирован</b>\n\n"
        f"Вы не можете подать апелляцию в течение {APPEAL_BLOCK_DAYS} дней.\n\n"
        f"🕒 <b>Блокировка снимется:</b> {until_str}\n\n"
        "<i>Когда срок истечёт — напишите /start, чтобы подать апелляцию снова.</i>"
    )

async def _try_unban(bot, chat_id: int, user_id: int) -> None:
    try:
        await bot.unban_chat_member(chat_id=chat_id, user_id=user_id, only_if_banned=True)
        log.info("Unbanned (if banned) %s in chat %s", user_id, chat_id)
    except Exception as e:
        log.debug("unban %s in chat %s skipped/failed: %s", user_id, chat_id, e)

async def _generate_invite_link(bot, user_id: int) -> Optional[str]:
    expire_at = datetime.now(timezone.utc) + timedelta(hours=1)
    await _try_unban(bot, TARGET_CHAT_ID, user_id)
    try:
        invite = await bot.create_chat_invite_link(
            chat_id=TARGET_CHAT_ID, member_limit=1, creates_join_request=True,
            expire_date=expire_at, name=f"u{user_id}",
        )
        link = invite.invite_link
        await save_invite_link(user_id, link)
        log.info("Invite link created (limit+request) for %s: %s", user_id, link)
        return link
    except Exception as e:
        log.warning("limit+request failed (%s), retry without member_limit", e)

    try:
        invite = await bot.create_chat_invite_link(
            chat_id=TARGET_CHAT_ID, creates_join_request=True,
            expire_date=expire_at, name=f"u{user_id}",
        )
        link = invite.invite_link
        await save_invite_link(user_id, link)
        log.info("Invite link created (request-only) for %s: %s", user_id, link)
        return link
    except Exception as e2:
        log.error("create_chat_invite_link failed for %s: %s", user_id, e2)
        return None

async def _generate_mod_chat_invite(bot, user_id: int) -> Optional[str]:
    expire_at = datetime.now(timezone.utc) + timedelta(hours=1)
    chat_id = _mod_chat_id()
    await _try_unban(bot, chat_id, user_id)
    try:
        invite = await bot.create_chat_invite_link(
            chat_id=chat_id, member_limit=1, creates_join_request=True,
            expire_date=expire_at, name=f"mod_u{user_id}",
        )
        log.info("Mod chat invite (limit+request) for %s: %s", user_id, invite.invite_link)
        return invite.invite_link
    except Exception as e:
        log.warning("mod invite limit+request failed (%s), retry plain", e)

    try:
        invite = await bot.create_chat_invite_link(
            chat_id=chat_id, member_limit=1, expire_date=expire_at, name=f"mod_u{user_id}",
        )
        log.info("Mod chat invite (plain) for %s: %s", user_id, invite.invite_link)
        return invite.invite_link
    except Exception as e2:
        log.error("mod invite failed for %s: %s", user_id, e2)
        return None

async def _build_approved_text(bot, user_id: int) -> str:
    link = await _generate_invite_link(bot, user_id)
    if not link:
        link = CHAT_INVITE_LINK or "(ссылка недоступна, обратитесь к администрации)"
    return (
        "🎉 Вашу заявку <b>приняли</b>, добро пожаловать 🍁\n\n"
        f"🔗 <b>Ссылка на чат:</b> {link}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "⚖️ Если в чате вы получили наказание и считаете его несправедливым — можете подать апелляцию кнопкой ниже."
    )

async def _build_new_link_text(bot, user_id: int) -> str:
    link = await _generate_invite_link(bot, user_id)
    if not link:
        link = CHAT_INVITE_LINK or "(ссылка недоступна, обратитесь к администрации)"
    return (
        "🔁 <b>Вам выдана новая ссылка на чат</b>\n\n"
        f"🔗 <b>Ссылка:</b> {link}\n\n"
        "<i>Ссылка одноразовая, действует 1 час.</i>\n\n"
        "<i>Чтобы подать апелляцию снова — напишите /start.</i>"
    )

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return
    if message.chat.type != "private":
        return
    log.info("/start from %s (@%s)", user.id, user.username or "-")

    if await is_blacklisted(user.id):
        row = await get_blacklist_row(user.id)
        if row is not None and row["expires_at"]:
            until_str = _pretty_dt(row["expires_at"])
            await send_msg(ctx.bot, user.id, text=_blocked_text(until_str), parse_mode=ParseMode.HTML)
        else:
            await send_msg(
                ctx.bot, user.id,
                text="🚫 <b>Доступ заблокирован</b>\n\nОбратитесь к администрации чата, если считаете это ошибкой.",
                parse_mode=ParseMode.HTML,
            )
        return

    db_user = await get_user(user.id)
    state = db_user["state"] if db_user else "new"

    if state in SLOT_STATES:
        got_slot, pos = await _try_acquire_slot(user.id)
        if not got_slot:
            await _queue_set_bot(user.id, ctx.bot)
            await _advance_queue()
            if user.id in _active_sessions:
                log.info("User %s промоутнут из очереди сразу после /start", user.id)
                return
            now = time.monotonic()
            last_pos = _queue_positions.get(user.id, 0)
            last_shown = _queue_last_shown.get(user.id, 0.0)
            if last_pos == pos and (now - last_shown) < QUEUE_START_REPEAT_SEC:
                log.info("User %s повторно /start, позиция %d не изменилась — игнор", user.id, pos)
                return
            await _queue_notify_one(ctx.bot, user.id, pos)
            log.info("User %s в очереди, позиция %d (state=%s)", user.id, pos, state)
            return

    if state == "awaiting_video":
        await send_msg(ctx.bot, user.id, text=VIDEO_REQUEST_TEXT, parse_mode=ParseMode.HTML)
        return
    if state == "pending":
        await send_msg(ctx.bot, user.id, text=WAITING_TEXT, parse_mode=ParseMode.HTML, reply_markup=waiting_keyboard())
        return
    if state == "awaiting_form":
        await send_msg(ctx.bot, user.id, text=FORM_TEXT, parse_mode=ParseMode.HTML)
        return
    if state == "awaiting_appeal_form":
        await _send_appeal_form(ctx.bot, user.id)
        return
    if state == "appeal_pending":
        await send_msg(ctx.bot, user.id, text=APPEAL_WAITING_TEXT, parse_mode=ParseMode.HTML, reply_markup=waiting_keyboard())
        return
    if state == "approved":
        active_link = await get_active_invite_link(user.id)
        if active_link:
            text = (
                "🎉 Вашу заявку <b>приняли</b>, добро пожаловать 🍁\n\n"
                f"🔗 <b>Ссылка на чат:</b> {active_link}\n\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "⚖️ Если в чате вы получили наказание и считаете его несправедливым — можете подать апелляцию кнопкой ниже."
            )
        else:
            text = (
                "🎉 Вашу заявку <b>уже приняли</b> 🍁\n\n"
                "Активной ссылки нет. Если нужна — попросите администрацию, вам выдадут новую."
            )
        await send_msg(ctx.bot, user.id, text=text, parse_mode=ParseMode.HTML, reply_markup=appeal_button_keyboard(), disable_web_page_preview=True)
        return
    if state == "rejected":
        await send_msg(ctx.bot, user.id, text=REJECTED_MSG, parse_mode=ParseMode.HTML, reply_markup=appeal_button_keyboard())
        return

    await upsert_user(user.id, user.username, user.first_name)
    await send_msg(ctx.bot, user.id, text=RULES_TEXT, parse_mode=ParseMode.HTML, reply_markup=rules_keyboard(), disable_web_page_preview=False)

async def cb_refuse(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        return
    if is_cb_throttled(user.id):
        await safe(query.answer)
        return
    if await is_blacklisted(user.id):
        await safe(query.answer)
        return

    await add_to_blacklist(user.id, reason="refused_rules", expires_at=None)
    await set_user_state(user.id, "rejected")
    await _release_slot(user.id)
    if query.message is not None:
        await strip_buttons(ctx.bot, user.id, query.message.message_id)
    await send_msg(ctx.bot, user.id, text="❌ Вы отказались от правил чата.\nДоступ к боту закрыт.", parse_mode=ParseMode.HTML)
    await safe(query.answer)
    log.info("User %s refused rules, blacklisted, slot released", user.id)

async def cb_agree(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        return
    if is_cb_throttled(user.id):
        await safe(query.answer)
        return
    if await is_blacklisted(user.id):
        await safe(query.answer)
        return

    await set_user_state(user.id, "awaiting_form")
    if query.message is not None:
        await strip_buttons(ctx.bot, user.id, query.message.message_id)
    await send_msg(ctx.bot, user.id, text=FORM_TEXT, parse_mode=ParseMode.HTML)
    await safe(query.answer)

async def cb_ping_admins(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        return
    if is_cb_throttled(user.id):
        await safe(query.answer)
        return
    if await is_blacklisted(user.id):
        await safe(query.answer)
        return

    db_user = await get_user(user.id)
    if not db_user or db_user["state"] not in ("pending", "appeal_pending"):
        await safe(query.answer, "Заявка уже рассмотрена.", show_alert=True)
        return

    if not can_ping(user.id):
        left = ping_cooldown_left(user.id)
        if left >= 60:
            mins = left // 60
            await safe(query.answer, f"Слишком часто. Подожди {mins} мин.", show_alert=True)
        else:
            await safe(query.answer, f"Слишком часто. Подожди {left} сек.", show_alert=True)
        return

    if db_user["state"] == "appeal_pending":
        appeal_row = await get_latest_appeal(user.id)
        entity_id = appeal_row["id"] if appeal_row else "—"
        label = "Апелляция"
        header = "🔔 <b>Просьба обратить внимание на апелляцию</b>"
        topic_kwargs = _appeal_topic_kwargs()
    else:
        app_row = await get_latest_application(user.id)
        entity_id = app_row["id"] if app_row else "—"
        label = "Заявка"
        header = "🔔 <b>Просьба обратить внимание</b>"
        topic_kwargs = _topic_kwargs()

    mentions = await _fetch_admins_mentions(ctx.bot)
    mention_line = mentions if mentions else "(у админов нет username)"
    ping_text = (
        f"{header}\n\n"
        f"👤 {('@' + esc(user.username)) if user.username else esc(user.first_name)}\n"
        f"🆔 <code>{user.id}</code>\n"
        f"📋 {label}: <b>#{entity_id}</b>\n\n"
        f"{mention_line}"
    )
    sent = await safe(
        ctx.bot.send_message, chat_id=ADMIN_CHAT_ID, text=ping_text,
        parse_mode=ParseMode.HTML, disable_web_page_preview=True, **topic_kwargs,
    )
    if sent is not None:
        await safe(query.answer, "Администрация уведомлена 📣")
        log.info("User %s pinged admins (%s %s)", user.id, label, entity_id)
    else:
        await safe(query.answer, "Не получилось отправить, попробуй позже.", show_alert=True)

def _build_admin_text(app_row, tg_user) -> str:
    username = tg_user.username
    user_id = tg_user.id
    first_name = (tg_user.first_name or "").strip()
    username_display = f"@{esc(username)}" if username else "без username"
    safe_name = esc(app_row["name"])
    about_value = esc(app_row["about"]) if app_row["about"] else "—"
    anchor = esc(first_name) if first_name else "Открыть профиль"
    mention = f'<a href="tg://user?id={user_id}">{anchor}</a>'

    return (
        "📩 <b>Новая заявка</b>\n\n"
        f"👤 <b>Отправитель:</b> {username_display} ({safe_name})\n"
        f"🆔 <b>user_id:</b> <code>{user_id}</code>\n"
        f"🔗 <b>Профиль:</b> {mention}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📋 <b>Анкета:</b>\n\n"
        f"<b>Имя:</b> {esc(app_row['name'])}\n"
        f"<b>Возраст:</b> {esc(app_row['age'])}\n"
        f"<b>Болезни:</b> {esc(app_row['diseases'])}\n"
        f"<b>Причина:</b> {esc(app_row['reason'])}\n"
        f"<b>О себе:</b> {about_value}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

def _build_appeal_admin_text(appeal_row, tg_user) -> str:
    username = tg_user.username
    user_id = tg_user.id
    first_name = (tg_user.first_name or "").strip()
    username_display = f"@{esc(username)}" if username else "без username"
    anchor = esc(first_name) if first_name else "Открыть профиль"
    mention = f'<a href="tg://user?id={user_id}">{anchor}</a>'

    return (
        "⚖️ <b>Новая апелляция</b>\n\n"
        f"👤 <b>Отправитель:</b> {username_display} ({esc(appeal_row['name'])})\n"
        f"🆔 <b>user_id:</b> <code>{user_id}</code>\n"
        f"🔗 <b>Профиль:</b> {mention}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📋 <b>Апелляция:</b>\n\n"
        f"<b>Имя:</b> {esc(appeal_row['name'])}\n"
        f"<b>Возраст:</b> {esc(appeal_row['age'])}\n"
        f"<b>Тип наказания:</b> {esc(appeal_row['punishment_type'])}\n"
        f"<b>Описание:</b> {esc(appeal_row['description'])}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

def _build_mod_admin_text(app_row, tg_user) -> str:
    username = tg_user.username
    user_id = tg_user.id
    first_name = (tg_user.first_name or "").strip()
    username_display = f"@{esc(username)}" if username else "без username"
    anchor = esc(first_name) if first_name else "Открыть профиль"
    mention = f'<a href="tg://user?id={user_id}">{anchor}</a>'

    return (
        "🛡 <b>Новая заявка на модератора</b>\n\n"
        f"👤 <b>Отправитель:</b> {username_display} ({esc(app_row['name'])})\n"
        f"🆔 <b>user_id:</b> <code>{user_id}</code>\n"
        f"🔗 <b>Профиль:</b> {mention}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📋 <b>Анкета:</b>\n\n"
        f"<b>Имя:</b> {esc(app_row['name'])}\n"
        f"<b>Возраст:</b> {esc(app_row['age'])}\n"
        f"<b>О себе:</b> {esc(app_row['about'])}\n"
        f"<b>Мотивация:</b> {esc(app_row['motivation'])}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

async def on_user_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None or message.text is None:
        return
    if message.chat.type != "private":
        return
    if is_throttled(user.id):
        return
    if user.id in _active_sessions:
        _active_sessions[user.id] = time.monotonic()
    if await is_blacklisted(user.id):
        return

    db_user = await get_user(user.id)
    if not db_user:
        return

    state = db_user["state"]
    mod_state = db_user["mod_state"]

    if mod_state == "mod_awaiting_form":
        rec = await get_recruitment()
        if not rec or int(rec["is_open"]) == 0:
            await set_mod_state(user.id, None)
            await send_msg(ctx.bot, user.id, text=MOD_CLOSED_TEXT, parse_mode=ParseMode.HTML)
            return
        parsed = parse_mod_form(message.text)
        if parsed is None:
            await send_msg(ctx.bot, user.id, text=MOD_INVALID_FORM_TEXT, parse_mode=ParseMode.HTML)
            return
        if await has_active_mod_application(user.id):
            await set_mod_state(user.id, None)
            await send_msg(ctx.bot, user.id, text=MOD_WAITING_TEXT, parse_mode=ParseMode.HTML)
            return
        try:
            app_id = await create_mod_application(
                user_id=user.id, round_started_at=rec["round_started_at"],
                username_at_submit=user.username, first_name_at_submit=user.first_name,
                name=parsed["name"], age=parsed["age"], about=parsed["about"],
                motivation=parsed["motivation"], status="awaiting_video",
            )
        except Exception as e:
            log.exception("create_mod_application failed for %s: %s", user.id, e)
            await send_msg(ctx.bot, user.id, text="⚠️ Не удалось сохранить заявку. Попробуйте позже.", parse_mode=ParseMode.HTML)
            return
        await set_mod_state(user.id, "mod_awaiting_video")
        await send_msg(ctx.bot, user.id, text=MOD_VIDEO_REQUEST_TEXT, parse_mode=ParseMode.HTML)
        log.info("Mod app %s from %s — awaiting video", app_id, user.id)
        return

    if mod_state == "mod_awaiting_video":
        await send_msg(ctx.bot, user.id, text=MOD_VIDEO_REMINDER_TEXT, parse_mode=ParseMode.HTML)
        return
    if mod_state == "mod_pending":
        await send_msg(ctx.bot, user.id, text=MOD_WAITING_TEXT, parse_mode=ParseMode.HTML)
        return
    if state in ("rejected", "approved"):
        return
    if state == "awaiting_video":
        await send_msg(ctx.bot, user.id, text=VIDEO_REMINDER_TEXT, parse_mode=ParseMode.HTML)
        return
    if state == "appeal_pending":
        await send_msg(ctx.bot, user.id, text=APPEAL_WAITING_TEXT, parse_mode=ParseMode.HTML, reply_markup=waiting_keyboard())
        return

    if state == "awaiting_appeal_form":
        parsed = parse_appeal(message.text)
        if parsed is None:
            await send_msg(ctx.bot, user.id, text=INVALID_APPEAL_TEXT, parse_mode=ParseMode.HTML)
            return
        try:
            appeal_id = await create_appeal(
                user_id=user.id, username_at_submit=user.username,
                first_name_at_submit=user.first_name, name=parsed["name"],
                age=parsed["age"], punishment_type=parsed["punishment_type"],
                description=parsed["description"],
            )
        except Exception as e:
            log.exception("create_appeal failed for user %s: %s", user.id, e)
            await send_msg(ctx.bot, user.id, text="⚠️ Не удалось сохранить апелляцию. Попробуйте отправить ещё раз через минуту.", parse_mode=ParseMode.HTML)
            return
        await set_user_state(user.id, "appeal_pending")
        appeal_row = await get_appeal(appeal_id)
        if appeal_row is None:
            log.error("Appeal %s created but not readable right after insert", appeal_id)
            await send_msg(ctx.bot, user.id, text="⚠️ Ошибка сохранения. Попробуйте ещё раз.", parse_mode=ParseMode.HTML)
            return
        admin_text = _build_appeal_admin_text(appeal_row, user)
        admin_msg = await safe(
            ctx.bot.send_message, chat_id=ADMIN_CHAT_ID, text=admin_text,
            parse_mode=ParseMode.HTML, reply_markup=appeal_decision_keyboard(appeal_id),
            disable_web_page_preview=True, **_appeal_topic_kwargs(),
        )
        if admin_msg is not None:
            await set_appeal_admin_msg_id(appeal_id, admin_msg.message_id)
        else:
            log.warning("Appeal %s — не доставлено в ветку, но в БД сохранено", appeal_id)
        await send_msg(ctx.bot, user.id, text=APPEAL_WAITING_TEXT, parse_mode=ParseMode.HTML, reply_markup=waiting_keyboard())
        log.info("Appeal %s from user %s submitted to thread %s", appeal_id, user.id, APPEAL_THREAD_ID)
        return

    if state != "awaiting_form":
        return
    if await has_active_application(user.id):
        await set_user_state(user.id, "pending")
        await _release_slot(user.id)
        await send_msg(ctx.bot, user.id, text=WAITING_TEXT, parse_mode=ParseMode.HTML, reply_markup=waiting_keyboard())
        return

    parsed = parse_form(message.text)
    if parsed is None:
        await send_msg(ctx.bot, user.id, text=INVALID_FORM_TEXT, parse_mode=ParseMode.HTML)
        return
    try:
        app_id = await create_application(
            user_id=user.id, username_at_submit=user.username,
            first_name_at_submit=user.first_name, name=parsed.name,
            age=parsed.age, diseases=parsed.diseases, reason=parsed.reason,
            about=parsed.about, status="awaiting_video",
        )
    except Exception as e:
        log.exception("create_application failed for user %s: %s", user.id, e)
        await send_msg(ctx.bot, user.id, text="⚠️ Не удалось сохранить анкету. Попробуйте отправить ещё раз через минуту.", parse_mode=ParseMode.HTML)
        return
    await set_user_state(user.id, "awaiting_video")
    await send_msg(ctx.bot, user.id, text=VIDEO_REQUEST_TEXT, parse_mode=ParseMode.HTML)
    log.info("Application %s from user %s — awaiting video note", app_id, user.id)

async def on_user_video_note(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None or message.video_note is None:
        return
    if message.chat.type != "private":
        return
    if user.id in _active_sessions:
        _active_sessions[user.id] = time.monotonic()
    if await is_blacklisted(user.id):
        return

    db_user = await get_user(user.id)
    if not db_user:
        return

    if db_user["mod_state"] == "mod_awaiting_video":
        rec = await get_recruitment()
        if not rec or int(rec["is_open"]) == 0:
            await set_mod_state(user.id, None)
            await send_msg(ctx.bot, user.id, text=MOD_CLOSED_TEXT, parse_mode=ParseMode.HTML)
            return
        app_row = await get_latest_mod_application(user.id)
        if app_row is None or app_row["status"] != "awaiting_video":
            await set_mod_state(user.id, None)
            return
        app_id = app_row["id"]
        file_id = message.video_note.file_id
        try:
            await set_mod_video_note(app_id, file_id)
            await set_mod_application_status(app_id, "pending")
            await set_mod_state(user.id, "mod_pending")
            await mark_mod_applied(user.id, app_row["round_started_at"])
        except Exception as e:
            log.exception("Failed to finalize mod app %s: %s", app_id, e)
            await send_msg(ctx.bot, user.id, text="⚠️ Ошибка сохранения. Попробуйте ещё раз.", parse_mode=ParseMode.HTML)
            return
        await send_msg(ctx.bot, user.id, text=MOD_WAITING_TEXT, parse_mode=ParseMode.HTML)
        video_msg = await safe(ctx.bot.send_video_note, chat_id=ADMIN_CHAT_ID, video_note=file_id, **_mod_topic_kwargs())
        if video_msg is not None:
            await set_mod_video_admin_msg_id(app_id, video_msg.message_id)
        admin_text = _build_mod_admin_text(app_row, user)
        send_kwargs = dict(
            chat_id=ADMIN_CHAT_ID, text=admin_text, parse_mode=ParseMode.HTML,
            reply_markup=mod_decision_keyboard(app_id), disable_web_page_preview=True,
            **_mod_topic_kwargs(),
        )
        if video_msg is not None:
            send_kwargs["reply_to_message_id"] = video_msg.message_id
            send_kwargs["allow_sending_without_reply"] = True
        admin_msg = await safe(ctx.bot.send_message, **send_kwargs)
        if admin_msg is not None:
            await set_mod_admin_msg_id(app_id, admin_msg.message_id)
        else:
            log.warning("Mod app %s — не доставлено в ветку, но в БД сохранено", app_id)
        log.info("Mod app %s from %s fully submitted", app_id, user.id)
        return

    if db_user["state"] != "awaiting_video":
        return
    app_row = await get_latest_application(user.id)
    if app_row is None or app_row["status"] != "awaiting_video":
        return
    app_id = app_row["id"]
    file_id = message.video_note.file_id
    try:
        await set_video_note(app_id, file_id)
        await set_application_status(app_id, "pending")
        await set_user_state(user.id, "pending")
    except Exception as e:
        log.exception("Failed to finalize application %s: %s", app_id, e)
        await send_msg(ctx.bot, user.id, text="⚠️ Ошибка сохранения. Попробуйте ещё раз.", parse_mode=ParseMode.HTML)
        return
    await _release_slot(user.id)
    await send_msg(ctx.bot, user.id, text=WAITING_TEXT, parse_mode=ParseMode.HTML, reply_markup=waiting_keyboard())
    video_msg = await safe(ctx.bot.send_video_note, chat_id=ADMIN_CHAT_ID, video_note=file_id, **_topic_kwargs())
    if video_msg is not None:
        await set_video_admin_msg_id(app_id, video_msg.message_id)
    admin_text = _build_admin_text(app_row, user)
    send_kwargs = dict(
        chat_id=ADMIN_CHAT_ID, text=admin_text, parse_mode=ParseMode.HTML,
        reply_markup=decision_keyboard(app_id), disable_web_page_preview=True,
        **_topic_kwargs(),
    )
    if video_msg is not None:
        send_kwargs["reply_to_message_id"] = video_msg.message_id
        send_kwargs["allow_sending_without_reply"] = True
    admin_msg = await safe(ctx.bot.send_message, **send_kwargs)
    if admin_msg is not None:
        await set_admin_msg_id(app_id, admin_msg.message_id)
    else:
        log.warning("App %s — не доставлено в ветку, но в БД сохранено", app_id)
    log.info("Application %s from user %s fully submitted, slot released", app_id, user.id)

async def on_join_request(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    join_request = update.chat_join_request
    if join_request is None:
        return
    user_id = join_request.from_user.id
    chat_id = join_request.chat.id
    invite_link = getattr(join_request, "invite_link", None)
    link_str = invite_link.invite_link if invite_link else None
    log.info("Join request from %s to chat %s via %s", user_id, chat_id, link_str)

    if chat_id == _mod_chat_id():
        if not await has_approved_mod_application(user_id):
            try:
                await ctx.bot.decline_chat_join_request(chat_id=chat_id, user_id=user_id)
                log.warning("Mod join from %s declined: no approved app", user_id)
            except Exception as e:
                log.error("decline mod join failed for %s: %s", user_id, e)
            return
        try:
            await ctx.bot.approve_chat_join_request(chat_id=chat_id, user_id=user_id)
            log.info("Mod join from %s approved", user_id)
        except Exception as e:
            log.error("approve mod join failed for %s: %s", user_id, e)
        return

    async def _decline(reason: str) -> None:
        try:
            await ctx.bot.decline_chat_join_request(chat_id=chat_id, user_id=user_id)
            log.warning("Join request from %s declined: %s", user_id, reason)
        except Exception as e:
            log.error("decline_chat_join_request failed for %s: %s", user_id, e)

    if link_str:
        owner_row = await get_invite_owner(link_str)
        if owner_row is not None and int(owner_row["user_id"]) != user_id:
            await _decline(f"link belongs to {owner_row['user_id']}, clicked by {user_id}")
            return

    db_user = await get_user(user_id)
    if not db_user or db_user["state"] != "approved":
        await _decline(f"state={db_user['state'] if db_user else 'none'}, expected approved")
        return
    try:
        await ctx.bot.approve_chat_join_request(chat_id=chat_id, user_id=user_id)
        await mark_invite_used(user_id)
        log.info("Join request from %s auto-approved in chat %s", user_id, chat_id)
    except Exception as e:
        log.error("Не удалось одобрить заявку %s: %s", user_id, e)

async def _is_admin(bot, chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except Exception:
        return False
    return member.status in ("administrator", "creator", "member", "restricted")

def _render_decision_text(original_text: str, approved: bool, admin_username: str) -> str:
    if approved:
        header = f"✅ <b>ЗАЯВКА ПРИНЯТА</b> ✅\n<i>Решение: @{esc(admin_username)}</i>"
    else:
        header = f"❌ <b>ЗАЯВКА ОТКЛОНЕНА</b> ❌\n<i>Решение: @{esc(admin_username)}</i>"
    clean_text = original_text.replace("📩 <b>Новая заявка</b>", "").strip()
    if clean_text.startswith("━━━━━━━━━━━━━━━━━━━━"):
        clean_text = clean_text.replace("━━━━━━━━━━━━━━━━━━━━", "", 1).strip()
    return f"{header}\n\n━━━━━━━━━━━━━━━━━━━━\n\n{clean_text}"

def _render_appeal_decision_text(original_text: str, status: str, admin_username: str) -> str:
    if status == "approved":
        header = "✅ <b>АПЕЛЛЯЦИЯ ПРИНЯТА</b> ✅"
    elif status == "rejected":
        header = "❌ <b>АПЕЛЛЯЦИЯ ОТКЛОНЕНА</b> ❌"
    elif status == "new_link":
        header = "🔁 <b>ВЫДАНА НОВАЯ ССЫЛКА</b>"
    elif status == "mod_link":
        header = "🛡 <b>ВЫДАНА ССЫЛКА НА АДМИН-ЧАТ</b>"
    else:
        header = "❓ <b>СТАТУС НЕИЗВЕСТЕН</b>"
    clean_text = original_text.replace("⚖️ <b>Новая апелляция</b>", "").strip()
    if clean_text.startswith("━━━━━━━━━━━━━━━━━━━━"):
        clean_text = clean_text.replace("━━━━━━━━━━━━━━━━━━━━", "", 1).strip()
    return f"{header}\n<i>Решение: @{esc(admin_username)}</i>\n\n━━━━━━━━━━━━━━━━━━━━\n\n{clean_text}"

def _render_mod_decision_text(original_text: str, approved: bool, admin_username: str) -> str:
    header = "✅ <b>ЗАЯВКА НА МОДЕРАТОРА ПРИНЯТА</b> ✅" if approved else "❌ <b>ЗАЯВКА НА МОДЕРАТОРА ОТКЛОНЕНА</b> ❌"
    clean_text = original_text.replace("🛡 <b>Новая заявка на модератора</b>", "").strip()
    if clean_text.startswith("━━━━━━━━━━━━━━━━━━━━"):
        clean_text = clean_text.replace("━━━━━━━━━━━━━━━━━━━━", "", 1).strip()
    return f"{header}\n<i>Решение: @{esc(admin_username)}</i>\n\n━━━━━━━━━━━━━━━━━━━━\n\n{clean_text}"

async def _finalize_admin_message(bot, msg_id: Optional[int], original_text: str, approved: bool, admin_username: str) -> None:
    if not msg_id:
        return
    new_text = _render_decision_text(original_text, approved, admin_username)
    edited = await safe(
        bot.edit_message_text, chat_id=ADMIN_CHAT_ID, message_id=msg_id, text=new_text,
        parse_mode=ParseMode.HTML, reply_markup=None, disable_web_page_preview=True,
    )
    if edited is not None:
        return
    await safe(bot.edit_message_reply_markup, chat_id=ADMIN_CHAT_ID, message_id=msg_id, reply_markup=None)

async def _finalize_appeal_admin_message(bot, msg_id: Optional[int], original_text: str, status: str, admin_username: str) -> None:
    if not msg_id:
        return
    new_text = _render_appeal_decision_text(original_text, status, admin_username)
    edited = await safe(
        bot.edit_message_text, chat_id=ADMIN_CHAT_ID, message_id=msg_id, text=new_text,
        parse_mode=ParseMode.HTML, reply_markup=None, disable_web_page_preview=True,
    )
    if edited is not None:
        return
    await safe(bot.edit_message_reply_markup, chat_id=ADMIN_CHAT_ID, message_id=msg_id, reply_markup=None)

async def _finalize_mod_admin_message(bot, msg_id: Optional[int], original_text: str, approved: bool, admin_username: str) -> None:
    if not msg_id:
        return
    new_text = _render_mod_decision_text(original_text, approved, admin_username)
    edited = await safe(
        bot.edit_message_text, chat_id=ADMIN_CHAT_ID, message_id=msg_id, text=new_text,
        parse_mode=ParseMode.HTML, reply_markup=None, disable_web_page_preview=True,
    )
    if edited is not None:
        return
    await safe(bot.edit_message_reply_markup, chat_id=ADMIN_CHAT_ID, message_id=msg_id, reply_markup=None)

async def _strip_buttons_from_query(ctx, query) -> None:
    if query is None or query.message is None:
        return
    await safe(
        ctx.bot.edit_message_reply_markup, chat_id=query.message.chat_id,
        message_id=query.message.message_id, reply_markup=None,
    )

async def cb_approve(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or query.message is None:
        return
    if is_cb_throttled(user.id):
        await safe(query.answer)
        return
    if query.message.chat_id != ADMIN_CHAT_ID:
        await safe(query.answer, "Недоступно", show_alert=False)
        return
    if not await _is_admin(ctx.bot, ADMIN_CHAT_ID, user.id):
        await safe(query.answer, "Только для участников админ-чата", show_alert=True)
        return

    app_id = int(query.data.split(":", 1)[1])
    app_row = await get_application(app_id)
    if not app_row:
        await safe(query.answer, "Заявка не найдена в БД. Обработайте вручную.", show_alert=True)
        await _strip_buttons_from_query(ctx, query)
        return

    target_user_id = app_row["user_id"]
    won = await try_decide_application(app_id, "approved", user.id)
    if not won:
        await safe(query.answer, "Заявка уже обработана", show_alert=False)
        await _strip_buttons_from_query(ctx, query)
        return

    await set_user_state(target_user_id, "approved")
    approved_text = await _build_approved_text(ctx.bot, target_user_id)
    await safe(
        ctx.bot.send_message, chat_id=target_user_id, text=approved_text,
        parse_mode=ParseMode.HTML, disable_web_page_preview=True,
        reply_markup=appeal_button_keyboard(),
    )
    admin_username = user.username or user.full_name
    await _finalize_admin_message(ctx.bot, app_row["admin_msg_id"], query.message.text or query.message.caption or "", approved=True, admin_username=admin_username)
    await safe(query.answer, "Одобрено")
    log.info("App %s approved by %s (user %s)", app_id, user.id, target_user_id)

async def cb_reject(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or query.message is None:
        return
    if is_cb_throttled(user.id):
        await safe(query.answer)
        return
    if query.message.chat_id != ADMIN_CHAT_ID:
        await safe(query.answer, "Недоступно", show_alert=False)
        return
    if not await _is_admin(ctx.bot, ADMIN_CHAT_ID, user.id):
        await safe(query.answer, "Только для участников админ-чата", show_alert=True)
        return

    app_id = int(query.data.split(":", 1)[1])
    app_row = await get_application(app_id)
    if not app_row:
        await safe(query.answer, "Заявка не найдена в БД. Обработайте вручную.", show_alert=True)
        await _strip_buttons_from_query(ctx, query)
        return

    target_user_id = app_row["user_id"]
    won = await try_decide_application(app_id, "rejected", user.id)
    if not won:
        await safe(query.answer, "Заявка уже обработана", show_alert=False)
        await _strip_buttons_from_query(ctx, query)
        return

    await set_user_state(target_user_id, "rejected")
    await safe(
        ctx.bot.send_message, chat_id=target_user_id, text=REJECTED_MSG,
        parse_mode=ParseMode.HTML, disable_web_page_preview=True,
        reply_markup=appeal_button_keyboard(),
    )
    admin_username = user.username or user.full_name
    await _finalize_admin_message(ctx.bot, app_row["admin_msg_id"], query.message.text or query.message.caption or "", approved=False, admin_username=admin_username)
    await safe(query.answer, "Отклонено")
    log.info("App %s rejected by %s", app_id, user.id)

async def cb_appeal_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        return
    if is_cb_throttled(user.id):
        await safe(query.answer)
        return
    if await is_blacklisted(user.id):
        row = await get_blacklist_row(user.id)
        if row is not None and row["expires_at"]:
            until_str = _pretty_dt(row["expires_at"])
            await safe(query.answer, f"Апелляция недоступна до {until_str}", show_alert=True)
        else:
            await safe(query.answer, "Доступ заблокирован.", show_alert=True)
        return

    db_user = await get_user(user.id)
    if db_user is None:
        await safe(query.answer, "Сначала /start", show_alert=True)
        return
    if db_user["state"] == "appeal_pending":
        await safe(query.answer, "Апелляция уже на рассмотрении.", show_alert=True)
        return
    if db_user["state"] == "awaiting_appeal_form":
        await safe(query.answer, "Заполни шаблон выше и отправь одним сообщением.", show_alert=True)
        return

    current_state = db_user["state"]
    if current_state not in ("rejected", "approved"):
        await safe(query.answer, "Апелляцию можно подать после рассмотрения заявки.", show_alert=True)
        return

    await set_pre_appeal_state(user.id, current_state)
    await set_user_state(user.id, "awaiting_appeal_form")
    if query.message is not None:
        await strip_buttons(ctx.bot, user.id, query.message.message_id)
    await _send_appeal_form(ctx.bot, user.id)
    await safe(query.answer)

async def cb_appeal_approve(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or query.message is None:
        return
    if is_cb_throttled(user.id):
        await safe(query.answer)
        return
    if query.message.chat_id != ADMIN_CHAT_ID:
        await safe(query.answer, "Недоступно", show_alert=False)
        return
    if not await _is_admin(ctx.bot, ADMIN_CHAT_ID, user.id):
        await safe(query.answer, "Только для участников админ-чата", show_alert=True)
        return

    appeal_id = int(query.data.split(":", 1)[1])
    appeal_row = await get_appeal(appeal_id)
    if not appeal_row:
        await safe(query.answer, "Апелляция не найдена в БД. Обработайте вручную.", show_alert=True)
        await _strip_buttons_from_query(ctx, query)
        return

    target_user_id = appeal_row["user_id"]
    won = await try_decide_appeal(appeal_id, "approved", user.id)
    if not won:
        await safe(query.answer, "Апелляция уже обработана", show_alert=False)
        await _strip_buttons_from_query(ctx, query)
        return

    target_user = await get_user(target_user_id)
    prev_state = target_user["pre_appeal_state"] if target_user else None
    await remove_from_blacklist(target_user_id)
    await set_pre_appeal_state(target_user_id, None)

    if prev_state == "approved":
        await set_user_state(target_user_id, "approved")
        await safe(
            ctx.bot.send_message, chat_id=target_user_id,
            text="✅ <b>Апелляция одобрена</b>\n\nС вас <b>будут сняты все ограничения</b>. Спасибо за понимание 🍁\n\n<i>Чтобы подать апелляцию снова — напишите /start.</i>",
            parse_mode=ParseMode.HTML,
        )
        log.info("Appeal %s approved: user %s was already in chat, restrictions lifted", appeal_id, target_user_id)
    else:
        await set_user_state(target_user_id, "awaiting_form")
        await safe(
            ctx.bot.send_message, chat_id=target_user_id,
            text="✅ <b>Апелляция одобрена</b>\n\nВы можете повторно заполнить анкету.\nШаблон — ниже 👇",
            parse_mode=ParseMode.HTML,
        )
        await safe(ctx.bot.send_message, chat_id=target_user_id, text=FORM_TEXT, parse_mode=ParseMode.HTML)
        log.info("Appeal %s approved: user %s was rejected, sent form", appeal_id, target_user_id)

    admin_username = user.username or user.full_name
    await _finalize_appeal_admin_message(ctx.bot, appeal_row["admin_msg_id"], query.message.text or query.message.caption or "", status="approved", admin_username=admin_username)
    await safe(query.answer, "Апелляция принята")

async def cb_appeal_reject(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or query.message is None:
        return
    if is_cb_throttled(user.id):
        await safe(query.answer)
        return
    if query.message.chat_id != ADMIN_CHAT_ID:
        await safe(query.answer, "Недоступно", show_alert=False)
        return
    if not await _is_admin(ctx.bot, ADMIN_CHAT_ID, user.id):
        await safe(query.answer, "Только для участников админ-чата", show_alert=True)
        return

    appeal_id = int(query.data.split(":", 1)[1])
    appeal_row = await get_appeal(appeal_id)
    if not appeal_row:
        await safe(query.answer, "Апелляция не найдена в БД. Обработайте вручную.", show_alert=True)
        await _strip_buttons_from_query(ctx, query)
        return

    target_user_id = appeal_row["user_id"]
    won = await try_decide_appeal(appeal_id, "rejected", user.id)
    if not won:
        await safe(query.answer, "Апелляция уже обработана", show_alert=False)
        await _strip_buttons_from_query(ctx, query)
        return

    until_str = _future_utc_str(APPEAL_BLOCK_DAYS)
    await add_to_blacklist(target_user_id, reason="appeal_rejected", expires_at=until_str)
    await set_pre_appeal_state(target_user_id, None)
    await set_user_state(target_user_id, "rejected")
    await safe(
        ctx.bot.send_message, chat_id=target_user_id,
        text=(
            "❌ <b>В апелляции отказано</b>\n\n"
            f"Вы не можете подать апелляцию в течение {APPEAL_BLOCK_DAYS} дней.\n\n"
            f"🕒 <b>Блокировка снимется:</b> {_pretty_dt(until_str)}\n\n"
            "<i>Когда срок истёчёт — напишите /start, чтобы подать апелляцию снова.</i>"
        ),
        parse_mode=ParseMode.HTML,
    )
    admin_username = user.username or user.full_name
    await _finalize_appeal_admin_message(ctx.bot, appeal_row["admin_msg_id"], query.message.text or query.message.caption or "", status="rejected", admin_username=admin_username)
    await safe(query.answer, "Апелляция отклонена")
    log.info("Appeal %s rejected by %s (user %s, blocked until %s)", appeal_id, user.id, target_user_id, until_str)

async def cb_appeal_newlink(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or query.message is None:
        return
    if is_cb_throttled(user.id):
        await safe(query.answer)
        return
    if query.message.chat_id != ADMIN_CHAT_ID:
        await safe(query.answer, "Недоступно", show_alert=False)
        return
    if not await _is_admin(ctx.bot, ADMIN_CHAT_ID, user.id):
        await safe(query.answer, "Только для участников админ-чата", show_alert=True)
        return

    appeal_id = int(query.data.split(":", 1)[1])
    appeal_row = await get_appeal(appeal_id)
    if not appeal_row:
        await safe(query.answer, "Апелляция не найдена в БД. Обработайте вручную.", show_alert=True)
        await _strip_buttons_from_query(ctx, query)
        return

    target_user_id = appeal_row["user_id"]
    won = await try_decide_appeal(appeal_id, "new_link", user.id)
    if not won:
        await safe(query.answer, "Апелляция уже обработана", show_alert=False)
        await _strip_buttons_from_query(ctx, query)
        return

    await remove_from_blacklist(target_user_id)
    await set_pre_appeal_state(target_user_id, None)
    await set_user_state(target_user_id, "approved")
    new_link_text = await _build_new_link_text(ctx.bot, target_user_id)
    await safe(
        ctx.bot.send_message, chat_id=target_user_id, text=new_link_text,
        parse_mode=ParseMode.HTML, disable_web_page_preview=True,
    )
    admin_username = user.username or user.full_name
    await _finalize_appeal_admin_message(ctx.bot, appeal_row["admin_msg_id"], query.message.text or query.message.caption or "", status="new_link", admin_username=admin_username)
    await safe(query.answer, "Ссылка отправлена")
    log.info("Appeal %s: new link sent to user %s by %s", appeal_id, target_user_id, user.id)

async def cb_appeal_modlink(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or query.message is None:
        return
    if is_cb_throttled(user.id):
        await safe(query.answer)
        return
    if query.message.chat_id != ADMIN_CHAT_ID:
        await safe(query.answer, "Недоступно", show_alert=False)
        return
    if not await _is_admin(ctx.bot, ADMIN_CHAT_ID, user.id):
        await safe(query.answer, "Только для участников админ-чата", show_alert=True)
        return

    appeal_id = int(query.data.split(":", 1)[1])
    appeal_row = await get_appeal(appeal_id)
    if not appeal_row:
        await safe(query.answer, "Апелляция не найдена в БД. Обработайте вручную.", show_alert=True)
        await _strip_buttons_from_query(ctx, query)
        return

    target_user_id = appeal_row["user_id"]
    link = await _generate_mod_chat_invite(ctx.bot, target_user_id)
    if not link:
        await safe(query.answer, "Не удалось создать ссылку", show_alert=True)
        return

    await safe(
        ctx.bot.send_message, chat_id=target_user_id,
        text=f"🔁 <b>Вам выдана ссылка на админ-чат</b>\n\n🔗 <b>Ссылка:</b> {link}\n\n<i>Ссылка одноразовая, действует 1 час.</i>",
        parse_mode=ParseMode.HTML, disable_web_page_preview=True,
    )
    await _strip_buttons_from_query(ctx, query)
    await safe(query.answer, "Ссылка отправлена")
    log.info("Appeal %s: mod chat link sent to user %s by %s", appeal_id, target_user_id, user.id)

async def cb_mod_apply(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        return
    if is_cb_throttled(user.id):
        await safe(query.answer)
        return
    if await is_blacklisted(user.id):
        await safe(query.answer, "Доступ заблокирован.", show_alert=True)
        return

    rec = await get_recruitment()
    if not rec or int(rec["is_open"]) == 0:
        await safe(query.answer, "Набор закрыт.", show_alert=True)
        if query.message is not None:
            await strip_buttons(ctx.bot, user.id, query.message.message_id)
        return

    db_user = await get_user(user.id)
    if not db_user or db_user["state"] != "approved":
        await safe(query.answer, "Подать заявку можно только участникам чата.", show_alert=True)
        return

    if db_user["mod_state"] in ("mod_awaiting_form", "mod_awaiting_video"):
        await safe(query.answer, "Ты уже заполняешь заявку. Продолжай по шаблону.", show_alert=True)
        return
    if db_user["mod_state"] == "mod_pending":
        await safe(query.answer, "Заявка уже на рассмотрении.", show_alert=True)
        return
    if await has_active_mod_application(user.id):
        await safe(query.answer, "Заявка уже на рассмотрении.", show_alert=True)
        return

    resp = await get_mod_response(user.id, rec["round_started_at"])
    if resp is not None:
        if int(resp["refused"]) == 1:
            await safe(query.answer, "Ты отказался в этом наборе. Жди следующий.", show_alert=True)
            return
        if int(resp["applied"]) == 1:
            await safe(query.answer, "Ты уже подал заявку в этом наборе.", show_alert=True)
            return

    await set_mod_state(user.id, "mod_awaiting_form")
    if query.message is not None:
        await strip_buttons(ctx.bot, user.id, query.message.message_id)
    await send_msg(ctx.bot, user.id, text=MOD_FORM_TEXT, parse_mode=ParseMode.HTML)
    await safe(query.answer)
    log.info("User %s started mod form (round %s)", user.id, rec["round_started_at"])

async def cb_mod_refuse(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        return
    if is_cb_throttled(user.id):
        await safe(query.answer)
        return

    rec = await get_recruitment()
    if not rec or int(rec["is_open"]) == 0:
        await safe(query.answer, "Набор уже закрыт.", show_alert=True)
        if query.message is not None:
            await strip_buttons(ctx.bot, user.id, query.message.message_id)
        return

    resp = await get_mod_response(user.id, rec["round_started_at"])
    if resp is not None and int(resp["refused"]) == 1:
        await safe(query.answer, "Ты уже отказался.", show_alert=True)
        return
    if resp is not None and int(resp["applied"]) == 1:
        await safe(query.answer, "Ты уже подал заявку.", show_alert=True)
        return

    await mark_mod_refused(user.id, rec["round_started_at"])
    await set_mod_state(user.id, None)
    if query.message is not None:
        await strip_buttons(ctx.bot, user.id, query.message.message_id)
    await send_msg(ctx.bot, user.id, text=MOD_REFUSED_TEXT, parse_mode=ParseMode.HTML)
    await safe(query.answer, "Ок, в этом наборе больше не напомним.")
    log.info("User %s refused mod recruitment (round %s)", user.id, rec["round_started_at"])

async def cb_mod_approve(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or query.message is None:
        return
    if is_cb_throttled(user.id):
        await safe(query.answer)
        return
    if query.message.chat_id != ADMIN_CHAT_ID:
        await safe(query.answer, "Недоступно", show_alert=False)
        return
    if not await _is_admin(ctx.bot, ADMIN_CHAT_ID, user.id):
        await safe(query.answer, "Только для участников админ-чата", show_alert=True)
        return

    app_id = int(query.data.split(":", 1)[1])
    app_row = await get_mod_application(app_id)
    if not app_row:
        await safe(query.answer, "Заявка не найдена в БД.", show_alert=True)
        await _strip_buttons_from_query(ctx, query)
        return

    target_user_id = app_row["user_id"]
    won = await try_decide_mod_application(app_id, "approved", user.id)
    if not won:
        await safe(query.answer, "Уже обработана", show_alert=False)
        await _strip_buttons_from_query(ctx, query)
        return

    await set_mod_state(target_user_id, None)
    invite = await _generate_mod_chat_invite(ctx.bot, target_user_id)
    if invite:
        approved_msg = f"{MOD_APPROVED_MSG}\n\n🔗 <b>Ссылка на админ-чат:</b> {invite}\n\n<i>Ссылка одноразовая, действует 1 час.</i>"
    else:
        approved_msg = MOD_APPROVED_MSG_NO_LINK
    await safe(
        ctx.bot.send_message, chat_id=target_user_id, text=approved_msg,
        parse_mode=ParseMode.HTML, disable_web_page_preview=True,
    )
    admin_username = user.username or user.full_name
    await _finalize_mod_admin_message(ctx.bot, app_row["admin_msg_id"], query.message.text or query.message.caption or "", approved=True, admin_username=admin_username)
    await safe(query.answer, "Одобрено")
    log.info("Mod app %s approved by %s (user %s)", app_id, user.id, target_user_id)

async def cb_mod_reject(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or query.message is None:
        return
    if is_cb_throttled(user.id):
        await safe(query.answer)
        return
    if query.message.chat_id != ADMIN_CHAT_ID:
        await safe(query.answer, "Недоступно", show_alert=False)
        return
    if not await _is_admin(ctx.bot, ADMIN_CHAT_ID, user.id):
        await safe(query.answer, "Только для участников админ-чата", show_alert=True)
        return

    app_id = int(query.data.split(":", 1)[1])
    app_row = await get_mod_application(app_id)
    if not app_row:
        await safe(query.answer, "Заявка не найдена в БД.", show_alert=True)
        await _strip_buttons_from_query(ctx, query)
        return

    target_user_id = app_row["user_id"]
    won = await try_decide_mod_application(app_id, "rejected", user.id)
    if not won:
        await safe(query.answer, "Уже обработана", show_alert=False)
        await _strip_buttons_from_query(ctx, query)
        return

    await set_mod_state(target_user_id, None)
    await safe(ctx.bot.send_message, chat_id=target_user_id, text=MOD_REJECTED_MSG, parse_mode=ParseMode.HTML)
    admin_username = user.username or user.full_name
    await _finalize_mod_admin_message(ctx.bot, app_row["admin_msg_id"], query.message.text or query.message.caption or "", approved=False, admin_username=admin_username)
    await safe(query.answer, "Отклонено")
    log.info("Mod app %s rejected by %s (user %s)", app_id, user.id, target_user_id)

async def _broadcast_mod_open(bot, user_ids: list[int], round_started_at: str) -> None:
    sent = 0
    failed = 0
    for uid in user_ids:
        try:
            msg = await safe(
                bot.send_message, chat_id=uid, text=MOD_OPEN_TEXT,
                parse_mode=ParseMode.HTML, reply_markup=mod_invite_keyboard(),
            )
            if msg is not None:
                await mark_mod_notified(uid, round_started_at)
                sent += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
            log.warning("mod broadcast to %s failed: %s", uid, e)
        await asyncio.sleep(0.05)
    try:
        await safe(
            bot.send_message, chat_id=ADMIN_CHAT_ID,
            text=f"📣 <b>Рассылка о наборе завершена</b>\n\n✅ Доставлено: <b>{sent}</b>\n⚠️ Ошибок: <b>{failed}</b>",
            parse_mode=ParseMode.HTML, **_mod_topic_kwargs(),
        )
    except Exception:
        pass
    log.info("Mod broadcast done: sent=%d, failed=%d", sent, failed)

async def _broadcast_mod_closed(bot, user_ids: list[int]) -> None:
    sent = 0
    failed = 0
    for uid in user_ids:
        try:
            msg = await safe(bot.send_message, chat_id=uid, text=MOD_CLOSED_TEXT, parse_mode=ParseMode.HTML)
            if msg is not None:
                sent += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
            log.warning("mod close broadcast to %s failed: %s", uid, e)
        await asyncio.sleep(0.05)
    try:
        await safe(
            bot.send_message, chat_id=ADMIN_CHAT_ID,
            text=f"📪 <b>Рассылка о закрытии завершена</b>\n\n✅ Доставлено: <b>{sent}</b>\n⚠️ Ошибок: <b>{failed}</b>",
            parse_mode=ParseMode.HTML, **_mod_topic_kwargs(),
        )
    except Exception:
        pass
    log.info("Mod close broadcast done: sent=%d, failed=%d", sent, failed)

async def cmd_unnabor(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return
    if message.chat_id != ADMIN_CHAT_ID:
        return
    if not await _is_admin(ctx.bot, ADMIN_CHAT_ID, user.id):
        return

    opened = await open_recruitment(user.id)
    if not opened:
        await safe(message.reply_text, "⚠️ Набор уже открыт. Сначала закройте текущий: /offnabor", **_mod_topic_kwargs())
        return

    rec = await get_recruitment()
    round_started_at = rec["round_started_at"] if rec else None
    if not round_started_at:
        await safe(message.reply_text, "⚠️ Не удалось определить раунд набора.")
        return

    approved_ids = await get_approved_users_ids()
    eligible: list[int] = []
    for uid in approved_ids:
        if await has_pending_mod_application(uid):
            continue
        eligible.append(uid)

    await safe(
        message.reply_text,
        f"🛡 <b>Набор модераторов открыт</b>\n\n👥 Получателей: <b>{len(eligible)}</b>\n⏳ Рассылка идёт в фоне — я пришлю сводку, когда закончу.",
        parse_mode=ParseMode.HTML, **_mod_topic_kwargs(),
    )
    asyncio.create_task(_broadcast_mod_open(ctx.bot, eligible, round_started_at))
    log.info("/unnabor: opened round %s, eligible=%d", round_started_at, len(eligible))

async def cmd_offnabor(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return
    if message.chat_id != ADMIN_CHAT_ID:
        return
    if not await _is_admin(ctx.bot, ADMIN_CHAT_ID, user.id):
        return

    rec = await get_recruitment()
    if not rec or int(rec["is_open"]) == 0:
        await safe(message.reply_text, "⚠️ Набор не открыт.", **_mod_topic_kwargs())
        return

    round_started_at = rec["round_started_at"]
    closed = await close_recruitment(user.id)
    if not closed:
        await safe(message.reply_text, "⚠️ Не удалось закрыть набор.")
        return

    reset_count = await reset_stuck_mod_states()
    notified_ids = await get_notified_users_ids(round_started_at) if round_started_at else []
    await safe(
        message.reply_text,
        f"📪 <b>Набор модераторов закрыт</b>\n\n👥 Получателей: <b>{len(notified_ids)}</b>\n🧹 Сброшено незавершённых заявок: <b>{reset_count}</b>\n⏳ Рассылка идёт в фоне.",
        parse_mode=ParseMode.HTML, **_mod_topic_kwargs(),
    )
    if notified_ids:
        asyncio.create_task(_broadcast_mod_closed(ctx.bot, notified_ids))
    log.info("/offnabor: closed round %s, notified=%d, reset=%d", round_started_at, len(notified_ids), reset_count)

async def cmd_untimeban(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return
    if message.chat_id != ADMIN_CHAT_ID:
        return
    if not await _is_admin(ctx.bot, ADMIN_CHAT_ID, user.id):
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await safe(message.reply_text, "Использование: /untimeban &lt;user_id&gt;", parse_mode=ParseMode.HTML, **_topic_kwargs())
        return

    target_id = int(parts[1])
    await remove_from_blacklist(target_id)
    await safe(message.reply_text, f"Пользователь <code>{target_id}</code> разблокирован (временный бан снят).", parse_mode=ParseMode.HTML, **_topic_kwargs())
    log.info("User %s untime-banned by %s", target_id, user.id)

async def cmd_unperm(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return
    if message.chat_id != ADMIN_CHAT_ID:
        return
    if not await _is_admin(ctx.bot, ADMIN_CHAT_ID, user.id):
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await safe(message.reply_text, "Использование: /unperm &lt;user_id&gt;", parse_mode=ParseMode.HTML, **_topic_kwargs())
        return

    target_id = int(parts[1])
    ok = await remove_permanent_blacklist(target_id)
    if ok:
        text = f"Пользователь <code>{target_id}</code> разблокирован (перманентный бан снят)."
        log.info("User %s permanent-unbanned by %s", target_id, user.id)
    else:
        text = f"У пользователя <code>{target_id}</code> нет перманентного бана."
    await safe(message.reply_text, text, parse_mode=ParseMode.HTML, **_topic_kwargs())

async def _post_init(app: Application) -> None:
    try:
        await app.bot.set_my_commands([BotCommand("start", "Начать")])
    except Exception as e:
        log.warning("set_my_commands failed: %s", e)
    asyncio.create_task(_housekeeping())

async def _error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    err = ctx.error
    if err is None:
        return
    name = type(err).__name__
    if name in ("TimedOut", "NetworkError", "ConnectTimeout", "ReadTimeout", "RetryAfter"):
        log.warning("Сеть глюкнула (%s) — продолжаю.", name)
        return
    log.exception("Unhandled error in handler:", exc_info=err)

async def _build_app() -> Application:
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_TIMEOUT)
        .write_timeout(WRITE_TIMEOUT)
        .pool_timeout(POOL_TIMEOUT)
        .get_updates_connect_timeout(CONNECT_TIMEOUT)
        .get_updates_read_timeout(READ_TIMEOUT)
        .concurrent_updates(MAX_CONCURRENT_UPDATES)
        .post_init(_post_init)
        .build()
    )

    app.add_handler(ChatJoinRequestHandler(on_join_request))
    app.add_handler(CallbackQueryHandler(cb_approve, pattern=r"^approve:"))
    app.add_handler(CallbackQueryHandler(cb_reject,  pattern=r"^reject:"))
    app.add_handler(CallbackQueryHandler(cb_appeal_approve, pattern=r"^appeal_approve:"))
    app.add_handler(CallbackQueryHandler(cb_appeal_reject,  pattern=r"^appeal_reject:"))
    app.add_handler(CallbackQueryHandler(cb_appeal_newlink, pattern=r"^appeal_newlink:"))
    app.add_handler(CallbackQueryHandler(cb_appeal_modlink, pattern=r"^appeal_modlink:"))
    app.add_handler(CallbackQueryHandler(cb_ping_admins, pattern=r"^ping_admins$"))
    app.add_handler(CallbackQueryHandler(cb_agree,   pattern=r"^agree$"))
    app.add_handler(CallbackQueryHandler(cb_refuse,  pattern=r"^refuse$"))
    app.add_handler(CallbackQueryHandler(cb_appeal_start, pattern=r"^appeal_start$"))
    app.add_handler(CallbackQueryHandler(cb_mod_apply,   pattern=r"^mod_apply$"))
    app.add_handler(CallbackQueryHandler(cb_mod_refuse,  pattern=r"^mod_refuse$"))
    app.add_handler(CallbackQueryHandler(cb_mod_approve, pattern=r"^mod_approve:"))
    app.add_handler(CallbackQueryHandler(cb_mod_reject,  pattern=r"^mod_reject:"))

    app.add_handler(CommandHandler("start", cmd_start, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("untimeban", cmd_untimeban, filters=filters.Chat(ADMIN_CHAT_ID)))
    app.add_handler(CommandHandler("unperm", cmd_unperm, filters=filters.Chat(ADMIN_CHAT_ID)))
    app.add_handler(CommandHandler("unnabor", cmd_unnabor, filters=filters.Chat(ADMIN_CHAT_ID)))
    app.add_handler(CommandHandler("offnabor", cmd_offnabor, filters=filters.Chat(ADMIN_CHAT_ID)))

    app.add_handler(MessageHandler(filters.VIDEO_NOTE & filters.ChatType.PRIVATE, on_user_video_note))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, on_user_text))
    app.add_error_handler(_error_handler)
    return app

async def _start_with_retry() -> Application:
    attempt = 0
    while True:
        attempt += 1
        app = await _build_app()
        try:
            log.info("Подключение к Telegram (попытка %d)…", attempt)
            await app.initialize()
            await app.start()
            await app.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
                poll_interval=1.0,
                timeout=30,
            )
            log.info("Bot running. App thread: %s. Appeal thread: %s. Mod thread: %s. Mod chat: %s. Target chat: %s. Slot limit: %d, session timeout: %.0f сек.",
                     TOPIC_THREAD_ID, APPEAL_THREAD_ID, MODERATOR_THREAD_ID,
                     _mod_chat_id(), TARGET_CHAT_ID, SOFT_PARALLEL_LIMIT, SESSION_TIMEOUT)
            return app
        except (TimedOut, NetworkError) as e:
            log.warning("Сеть недоступна (%s). Жду %.0f сек…", type(e).__name__, START_RETRY_DELAY)
            try:
                await app.shutdown()
            except Exception:
                pass
            await asyncio.sleep(START_RETRY_DELAY)
        except Exception as e:
            log.exception("Фатальная ошибка при старте: %s", e)
            raise

async def _watchdog(app: Application) -> None:
    global _watchdog_fails
    while True:
        try:
            try:
                await asyncio.wait_for(app.bot.get_me(), timeout=25)
            except Exception as e:
                _watchdog_fails += 1
                log.warning("watchdog: getMe fail (%d/%d): %s", _watchdog_fails, WATCHDOG_MAX_FAILS, e)
                if _watchdog_fails >= WATCHDOG_MAX_FAILS:
                    _request_restart(f"{_watchdog_fails} getMe фейлов подряд")
                    return
            else:
                _watchdog_fails = 0
                if app.updater is None or not app.updater.running:
                    _request_restart("updater.running = False")
                    return
                log.debug("watchdog: alive. Активных сессий: %d, в очереди: %d", len(_active_sessions), len(_queue_order))
        except Exception as e:
            log.debug("watchdog loop err: %s", e)
        await asyncio.sleep(WATCHDOG_INTERVAL)

async def _shutdown_app(app: Application) -> None:
    log.info("shutting down app…")
    steps = []
    if app.updater is not None:
        steps.append(app.updater.stop)
    steps.append(app.stop)
    steps.append(app.shutdown)
    for step in steps:
        try:
            await step()
        except Exception as e:
            log.debug("shutdown step: %s", e)
    log.info("app shut down.")

async def _run_bot_once() -> None:
    global _active_sessions, _queue_order, _queue_bots
    global _queue_positions, _queue_last_shown, _queue_notify_at, _notify_task
    global _restart_event, _watchdog_fails
    _active_sessions = {}
    _queue_order = []
    _queue_bots = {}
    _queue_positions = {}
    _queue_last_shown = {}
    _queue_notify_at = {}
    _notify_task = None
    _restart_event = asyncio.Event()
    _watchdog_fails = 0

    app = await _start_with_retry()
    watchdog_task = asyncio.create_task(_watchdog(app))
    try:
        await _restart_event.wait()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        watchdog_task.cancel()
        try:
            await watchdog_task
        except (asyncio.CancelledError, Exception):
            pass
        if _notify_task is not None and not _notify_task.done():
            _notify_task.cancel()
        await _shutdown_app(app)

async def _run_bot() -> None:
    backoff = RESTART_MIN_DELAY
    while True:
        try:
            await _run_bot_once()
            log.info("Bot остановлен корректно. Перезапуск через %.0f сек…", RESTART_MIN_DELAY)
            backoff = RESTART_MIN_DELAY
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Bot упал (%s: %s). Перезапуск через %.0f сек…", type(e).__name__, e, backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 1.5, RESTART_MAX_DELAY)

def main() -> None:
    try:
        _backup_rotate(keep=5)
    except Exception as e:
        log.warning("backup rotate failed: %s", e)
    _sync_init_db()
    log.info("Bot starting…")
    try:
        asyncio.run(_run_bot())
    except (KeyboardInterrupt, SystemExit):
        log.info("Bot stopped.")

if __name__ == "__main__":
    main()
