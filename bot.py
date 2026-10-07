import subprocess
import sys

try:
    import telegram
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "python-telegram-bot>=20,<22"])


import asyncio
import html
import logging
import re
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import NetworkError, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

BOT_TOKEN        = "8863364842:AAHDABiyJvPp7RKmdx6sDA1JS1eBMlPvtKA"
ADMIN_CHAT_ID    = -1004441293896
CHAT_INVITE_LINK = "https://t.me/+Ri7977iweXdiMzMy"
RULES_LINK       = "https://telegra.ph/Pravila-Klenovogo-buketika-10-07"
DB_PATH          = "bot.db"
THROTTLE_SECONDS = 2.0
TOPIC_THREAD_ID: Optional[int] = 422
PING_COOLDOWN_SECONDS = 3600.0
CONNECT_TIMEOUT  = 30.0
READ_TIMEOUT     = 30.0
WRITE_TIMEOUT    = 30.0
POOL_TIMEOUT     = 30.0
START_RETRY_DELAY = 10.0

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


def esc(value) -> str:
    if value is None:
        return ""
    return html.escape(str(value), quote=False)


async def safe(coro_fn, *args, retries: int = 3, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return await coro_fn(*args, **kwargs)
        except (TimedOut, NetworkError) as e:
            if attempt == retries:
                log.warning("safe(): окончательно упало — %s", e)
                return None
            await asyncio.sleep(1.5 * attempt)
        except Exception as e:
            log.debug("safe(): non-network error — %s", e)
            return None


def _topic_kwargs() -> dict:
    if TOPIC_THREAD_ID:
        return {"message_thread_id": TOPIC_THREAD_ID}
    return {}


_last_bot_msg: dict[int, int] = {}


async def _send_tracked(bot, chat_id: int, *, text: str, **kwargs):
    prev = _last_bot_msg.get(chat_id)
    if prev:
        await safe(bot.delete_message, chat_id=chat_id, message_id=prev)
        _last_bot_msg.pop(chat_id, None)

    msg = await safe(bot.send_message, chat_id=chat_id, text=text, **kwargs)
    if msg is not None:
        _last_bot_msg[chat_id] = msg.message_id
    return msg


async def _delete_prev_bot_msg(bot, chat_id: int) -> None:
    prev = _last_bot_msg.pop(chat_id, None)
    if prev:
        await safe(bot.delete_message, chat_id=chat_id, message_id=prev)


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
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS users (
    user_id     INTEGER PRIMARY KEY,
    username    TEXT,
    first_name  TEXT,
    state       TEXT NOT NULL DEFAULT 'new',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    decided_by            INTEGER,
    decided_at            TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_applications_user_status
    ON applications (user_id, status);

CREATE TABLE IF NOT EXISTS blacklist (
    user_id     INTEGER PRIMARY KEY,
    reason      TEXT,
    blocked_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def _sync_init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(SCHEMA)
        conn.commit()


def _sync_exec(query: str, params: tuple = (), fetch: Optional[str] = None):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(query, params)
        if fetch == "one":
            return cur.fetchone()
        if fetch == "all":
            return cur.fetchall()
        conn.commit()
        return cur.lastrowid


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
    await db_exec(
        "UPDATE users SET state = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
        (state, user_id),
    )


async def get_user(user_id: int):
    return await db_exec("SELECT * FROM users WHERE user_id = ?", (user_id,), fetch="one")


async def is_blacklisted(user_id: int) -> bool:
    row = await db_exec("SELECT 1 FROM blacklist WHERE user_id = ?", (user_id,), fetch="one")
    return row is not None


async def add_to_blacklist(user_id: int, reason: str) -> None:
    await db_exec(
        """
        INSERT INTO blacklist (user_id, reason, blocked_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET reason = excluded.reason
        """,
        (user_id, reason),
    )


async def remove_from_blacklist(user_id: int) -> None:
    await db_exec("DELETE FROM blacklist WHERE user_id = ?", (user_id,))
    await db_exec(
        "UPDATE users SET state = 'new', updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
        (user_id,),
    )


async def has_active_application(user_id: int) -> bool:
    row = await db_exec(
        "SELECT 1 FROM applications WHERE user_id = ? AND status = 'pending'",
        (user_id,), fetch="one",
    )
    return row is not None


async def get_latest_application(user_id: int):
    return await db_exec(
        "SELECT * FROM applications WHERE user_id = ? ORDER BY id DESC LIMIT 1",
        (user_id,), fetch="one",
    )


async def create_application(
    user_id: int,
    username_at_submit: Optional[str],
    first_name_at_submit: Optional[str],
    name: str, age: str, diseases: str, reason: str, about: Optional[str],
) -> int:
    return await db_exec(
        """
        INSERT INTO applications
            (user_id, username_at_submit, first_name_at_submit,
             name, age, diseases, reason, about, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)
        """,
        (user_id, username_at_submit, first_name_at_submit,
         name, age, diseases, reason, about),
    )


async def get_application(app_id: int):
    return await db_exec("SELECT * FROM applications WHERE id = ?", (app_id,), fetch="one")


async def set_admin_msg_id(app_id: int, admin_msg_id: int) -> None:
    await db_exec(
        "UPDATE applications SET admin_msg_id = ? WHERE id = ?",
        (admin_msg_id, app_id),
    )


async def decide_application(app_id: int, status: str, decided_by: int) -> None:
    await db_exec(
        """
        UPDATE applications
        SET status = ?, decided_by = ?, decided_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (status, decided_by, app_id),
    )


REQUIRED_FIELDS = ("name", "age", "diseases", "reason")

FIELD_PATTERNS = {
    "name":     r"имя",
    "age":      r"возраст",
    "diseases": r"болезни(?:\s+физические\s*/\s*психические)?",
    "reason":   r"почему\s+хотите\s+зайти\s+к\s+нам",
    "about":    r"о\s+себе(?:\s+по\s+желанию)?",
}


@dataclass
class ParsedForm:
    name: str
    age: str
    diseases: str
    reason: str
    about: Optional[str]


def parse_form(text: str) -> Optional[ParsedForm]:
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
            for key, pattern in FIELD_PATTERNS.items():
                if re.fullmatch(pattern, head_norm, flags=re.IGNORECASE):
                    matched_key = key
                    matched_value = tail.strip()
                    break

        if matched_key:
            collected[matched_key] = matched_value
            current_key = matched_key
        elif current_key:
            collected[current_key] = (collected.get(current_key, "") + " " + line).strip()

    for key in REQUIRED_FIELDS:
        if not collected.get(key, "").strip():
            return None

    return ParsedForm(
        name=collected["name"].strip(),
        age=collected["age"].strip(),
        diseases=collected["diseases"].strip(),
        reason=collected["reason"].strip(),
        about=collected.get("about", "").strip() or None,
    )


_last_seen: dict[int, float] = defaultdict(float)
_last_ping: dict[int, float] = defaultdict(float)


def is_throttled(user_id: int) -> bool:
    now = time.monotonic()
    if now - _last_seen[user_id] < THROTTLE_SECONDS:
        return True
    _last_seen[user_id] = now
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
        [
            InlineKeyboardButton("✅ Согласиться", callback_data="agree"),
            InlineKeyboardButton("❌ Отказаться", callback_data="refuse"),
        ]
    ])


def decision_keyboard(application_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Принять", callback_data=f"approve:{application_id}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"reject:{application_id}"),
        ]
    ])


def waiting_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📣 Упомянуть администрацию", callback_data="ping_admins")],
    ])


RULES_TEXT = (
    "📋 <b>Правила чата Кленовый букетик</b>\n\n"
    f"Полный текст правил доступен по ссылке:\n{RULES_LINK}"
)

FORM_TEXT = (
    "заполните анкету и отправьте её одним сообщением:\n\n"
    "<b>Имя:</b>\n"
    "<b>Возраст:</b>\n"
    "<b>Болезни физические/психические:</b>\n"
    "<b>Почему хотите зайти к нам:</b>\n"
    "<b>О себе по желанию:</b>"
)

INVALID_FORM_TEXT = (
    "❌ Ваша заявка оформлена не по шаблону.\n"
    "Пожалуйста, заполните все обязательные поля и отправьте анкету одним сообщением."
)

WAITING_TEXT = (
    "⏳ Ожидайте, вашу заявку рассматривают.\n\n"
    "Если ответа нет долго — нажмите кнопку ниже, чтобы привлечь внимание администрации."
)


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return

    log.info("/start from %s (@%s)", user.id, user.username or "-")

    if await is_blacklisted(user.id):
        return

    db_user = await get_user(user.id)

    if db_user and db_user["state"] in ("approved", "rejected"):
        return

    if db_user and db_user["state"] == "pending":
        await _send_tracked(
            ctx.bot, user.id,
            text=WAITING_TEXT,
            parse_mode=ParseMode.HTML,
            reply_markup=waiting_keyboard(),
        )
        return

    await upsert_user(user.id, user.username, user.first_name)
    await _send_tracked(
        ctx.bot, user.id,
        text=RULES_TEXT,
        parse_mode=ParseMode.HTML,
        reply_markup=rules_keyboard(),
        disable_web_page_preview=False,
    )


async def cb_refuse(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        return

    if await is_blacklisted(user.id):
        await safe(query.answer)
        return

    await add_to_blacklist(user.id, reason="refused_rules")
    await set_user_state(user.id, "rejected")

    _last_bot_msg.pop(user.id, None)

    await safe(
        query.edit_message_text,
        "Вы отказались от правил чата.\nДоступ к боту закрыт.",
    )
    await safe(query.answer)
    log.info("User %s refused rules, blacklisted", user.id)


async def cb_agree(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        return

    if await is_blacklisted(user.id):
        await safe(query.answer)
        return

    await set_user_state(user.id, "awaiting_form")

    _last_bot_msg.pop(user.id, None)
    if query.message is not None:
        await safe(ctx.bot.delete_message, chat_id=user.id, message_id=query.message.message_id)

    await _send_tracked(
        ctx.bot, user.id,
        text=FORM_TEXT,
        parse_mode=ParseMode.HTML,
    )
    await safe(query.answer)


async def cb_ping_admins(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        return

    if await is_blacklisted(user.id):
        await safe(query.answer)
        return

    db_user = await get_user(user.id)
    if not db_user or db_user["state"] != "pending":
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

    app_row = await get_latest_application(user.id)
    app_id = app_row["id"] if app_row else "—"

    mentions = await _fetch_admins_mentions(ctx.bot)
    mention_line = mentions if mentions else "(у админов нет username)"

    ping_text = (
        "🔔 <b>Просьба обратить внимание</b>\n\n"
        f"👤 {('@' + esc(user.username)) if user.username else esc(user.first_name)}\n"
        f"🆔 <code>{user.id}</code>\n"
        f"📋 Заявка: <b>#{app_id}</b>\n\n"
        f"{mention_line}"
    )

    sent = await safe(
        ctx.bot.send_message,
        chat_id=ADMIN_CHAT_ID,
        text=ping_text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        **_topic_kwargs(),
    )

    if sent is not None:
        await safe(query.answer, "Администрация уведомлена 📣")
        log.info("User %s pinged admins for app %s", user.id, app_id)
    else:
        await safe(query.answer, "Не получилось отправить, попробуй позже.", show_alert=True)


_session_msgs: dict[int, list[int]] = {}


def _track(user_id: int, *ids: int) -> None:
    bucket = _session_msgs.setdefault(user_id, [])
    for mid in ids:
        if mid:
            bucket.append(mid)


def _pop(user_id: int) -> list[int]:
    return _session_msgs.pop(user_id, [])


def _build_admin_text(app_row, tg_user) -> str:
    username = tg_user.username
    user_id = tg_user.id
    first_name = tg_user.first_name or "—"

    username_display = f"@{esc(username)}" if username else "без username"
    safe_name = esc(app_row["name"])
    about_value = esc(app_row["about"]) if app_row["about"] else "—"

    # HTML-упоминание — кликабельно всегда, даже если нет username
    mention = f'<a href="tg://user?id={user_id}">{esc(first_name)}</a>'

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
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🕒 <b>Отправлено:</b> {app_row['created_at']}"
    )


async def on_user_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None or message.text is None:
        return

    if is_throttled(user.id):
        return

    if await is_blacklisted(user.id):
        await _try_delete(message)
        return

    db_user = await get_user(user.id)
    if not db_user:
        return

    state = db_user["state"]

    if state in ("rejected", "approved"):
        await _try_delete(message)
        return

    if state != "awaiting_form":
        return

    await _delete_prev_bot_msg(ctx.bot, user.id)
    _track(user.id, message.message_id)

    if await has_active_application(user.id):
        await set_user_state(user.id, "pending")
        await _send_tracked(
            ctx.bot, user.id,
            text=WAITING_TEXT,
            parse_mode=ParseMode.HTML,
            reply_markup=waiting_keyboard(),
        )
        return

    parsed = parse_form(message.text)
    if parsed is None:
        await _send_tracked(
            ctx.bot, user.id,
            text=INVALID_FORM_TEXT,
            parse_mode=ParseMode.HTML,
        )
        return

    app_id = await create_application(
        user_id=user.id,
        username_at_submit=user.username,
        first_name_at_submit=user.first_name,
        name=parsed.name,
        age=parsed.age,
        diseases=parsed.diseases,
        reason=parsed.reason,
        about=parsed.about,
    )
    await set_user_state(user.id, "pending")

    app_row = await get_application(app_id)

    for mid in _pop(user.id):
        await safe(ctx.bot.delete_message, chat_id=message.chat_id, message_id=mid)
    await safe(ctx.bot.delete_message, chat_id=message.chat_id, message_id=message.message_id)

    await _send_tracked(
        ctx.bot, user.id,
        text=WAITING_TEXT,
        parse_mode=ParseMode.HTML,
        reply_markup=waiting_keyboard(),
    )

    admin_text = _build_admin_text(app_row, user)
    admin_msg = await safe(
        ctx.bot.send_message,
        chat_id=ADMIN_CHAT_ID,
        text=admin_text,
        parse_mode=ParseMode.HTML,
        reply_markup=decision_keyboard(app_id),
        disable_web_page_preview=True,
        **_topic_kwargs(),
    )
    if admin_msg is not None:
        await set_admin_msg_id(app_id, admin_msg.message_id)
    else:
        log.warning("App %s — не доставлено в ветку, но в БД сохранено", app_id)

    log.info("Application %s from user %s submitted to thread %s",
             app_id, user.id, TOPIC_THREAD_ID)


async def _try_delete(message) -> None:
    try:
        await message.delete()
    except Exception:
        pass


async def _is_admin(bot, chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except Exception:
        return False
    return member.status in ("administrator", "creator")


def _render_decision_text(original_text: str, approved: bool, admin_username: str) -> str:
    if approved:
        header = (
            "✅ <b>ЗАЯВКА ПРИНЯТА</b> ✅\n"
            f"<i>Решение: @{esc(admin_username)}</i>"
        )
    else:
        header = (
            "❌ <b>ЗАЯВКА ОТКЛОНЕНА</b> ❌\n"
            f"<i>Решение: @{esc(admin_username)}</i>"
        )
    return f"{header}\n\n━━━━━━━━━━━━━━━━━━━━\n\n{original_text}"


async def _finalize_admin_message(bot, msg_id: Optional[int], original_text: str,
                                  approved: bool, admin_username: str) -> None:
    if not msg_id:
        return

    new_text = _render_decision_text(original_text, approved, admin_username)

    edited = await safe(
        bot.edit_message_text,
        chat_id=ADMIN_CHAT_ID,
        message_id=msg_id,
        text=new_text,
        parse_mode=ParseMode.HTML,
        reply_markup=None,
        disable_web_page_preview=True,
    )
    if edited is not None:
        return

    await safe(
        bot.edit_message_reply_markup,
        chat_id=ADMIN_CHAT_ID,
        message_id=msg_id,
        reply_markup=None,
    )


async def _notify_user_decision(bot, user_id: int, approved: bool) -> None:
    _last_bot_msg.pop(user_id, None)

    if approved:
        text = (
            "🎉 Вашу заявку <b>приняли</b>, добро пожаловать 🍁\n\n"
            f"Ссылка на чат: {CHAT_INVITE_LINK}"
        )
    else:
        text = "❌ Вашу заявку <b>отклонили</b>. Доступ к боту закрыт."

    await safe(
        bot.send_message,
        chat_id=user_id,
        text=text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cb_approve(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or query.message is None:
        return

    if query.message.chat_id != ADMIN_CHAT_ID:
        await safe(query.answer, "Недоступно", show_alert=False)
        return
    if not await _is_admin(ctx.bot, ADMIN_CHAT_ID, user.id):
        await safe(query.answer, "Только для администраторов", show_alert=True)
        return

    app_id = int(query.data.split(":", 1)[1])
    app_row = await get_application(app_id)
    if not app_row:
        await safe(query.answer, "Заявка не найдена", show_alert=True)
        return
    if app_row["status"] != "pending":
        await safe(query.answer, "Заявка уже обработана", show_alert=False)
        return

    await decide_application(app_id, "approved", user.id)
    await set_user_state(app_row["user_id"], "approved")

    await _notify_user_decision(ctx.bot, app_row["user_id"], approved=True)

    admin_username = user.username or user.full_name
    await _finalize_admin_message(
        ctx.bot,
        app_row["admin_msg_id"],
        query.message.text or query.message.caption or "",
        approved=True,
        admin_username=admin_username,
    )
    await safe(query.answer, "Одобрено")
    log.info("App %s approved by %s", app_id, user.id)


async def cb_reject(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or query.message is None:
        return

    if query.message.chat_id != ADMIN_CHAT_ID:
        await safe(query.answer, "Недоступно", show_alert=False)
        return
    if not await _is_admin(ctx.bot, ADMIN_CHAT_ID, user.id):
        await safe(query.answer, "Только для администраторов", show_alert=True)
        return

    app_id = int(query.data.split(":", 1)[1])
    app_row = await get_application(app_id)
    if not app_row:
        await safe(query.answer, "Заявка не найдена", show_alert=True)
        return
    if app_row["status"] != "pending":
        await safe(query.answer, "Заявка уже обработана", show_alert=False)
        return

    await decide_application(app_id, "rejected", user.id)
    await set_user_state(app_row["user_id"], "rejected")
    await add_to_blacklist(app_row["user_id"], reason="application_rejected")

    await _notify_user_decision(ctx.bot, app_row["user_id"], approved=False)

    admin_username = user.username or user.full_name
    await _finalize_admin_message(
        ctx.bot,
        app_row["admin_msg_id"],
        query.message.text or query.message.caption or "",
        approved=False,
        admin_username=admin_username,
    )
    await safe(query.answer, "Отклонено")
    log.info("App %s rejected by %s", app_id, user.id)


async def cmd_unban(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
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
        await safe(
            message.reply_text,
            "Использование: /unban &lt;user_id&gt;",
            parse_mode=ParseMode.HTML,
            **_topic_kwargs(),
        )
        return

    target_id = int(parts[1])
    await remove_from_blacklist(target_id)
    await safe(
        message.reply_text,
        f"Пользователь <code>{target_id}</code> разблокирован.",
        parse_mode=ParseMode.HTML,
        **_topic_kwargs(),
    )
    log.info("User %s unbanned by %s", target_id, user.id)


async def _post_init(app: Application) -> None:
    try:
        await app.bot.set_my_commands([
            BotCommand("start", "Начать"),
        ])
    except Exception as e:
        log.warning("set_my_commands failed: %s", e)


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
        .post_init(_post_init)
        .build()
    )

    app.add_handler(CallbackQueryHandler(cb_approve, pattern=r"^approve:"))
    app.add_handler(CallbackQueryHandler(cb_reject,  pattern=r"^reject:"))
    app.add_handler(CallbackQueryHandler(cb_ping_admins, pattern=r"^ping_admins$"))
    app.add_handler(CallbackQueryHandler(cb_agree,   pattern=r"^agree$"))
    app.add_handler(CallbackQueryHandler(cb_refuse,  pattern=r"^refuse$"))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("unban", cmd_unban))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_user_text))
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
            await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
            log.info("Bot running. Thread for applications: %s. Ctrl+C to stop.", TOPIC_THREAD_ID)
            return app
        except (TimedOut, NetworkError) as e:
            log.warning("Сеть недоступна (%s). Жду %.0f сек и повторяю…",
                        type(e).__name__, START_RETRY_DELAY)
            try:
                await app.shutdown()
            except Exception:
                pass
            await asyncio.sleep(START_RETRY_DELAY)
        except Exception as e:
            log.exception("Фатальная ошибка при старте: %s", e)
            raise


async def _run_bot() -> None:
    app = await _start_with_retry()

    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        log.info("Shutting down…")
        for step in (app.updater.stop, app.stop, app.shutdown):
            try:
                await step()
            except Exception:
                pass


def main() -> None:
    _sync_init_db()
    log.info("Bot starting…")
    try:
        asyncio.run(_run_bot())
    except (KeyboardInterrupt, SystemExit):
        log.info("Bot stopped.")


if __name__ == "__main__":
    main()
