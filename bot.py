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
import re
import random
import sqlite3
import asyncio
import logging
import html
from datetime import datetime, timedelta, timezone
from typing import Optional

from dotenv import load_dotenv
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from telegram.constants import ParseMode
from telegram.error import NetworkError, TimedOut, RetryAfter, Forbidden, BadRequest
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
SUPERADMIN_ID = int(os.getenv("SUPERADMIN_ID", "0"))
PAYMENT_LINK = os.getenv("PAYMENT_LINK", "")

DATA_DIR = os.getenv("DATA_DIR", "")
DB_PATH = os.getenv("DB_PATH", "data.db")

if DATA_DIR and not os.path.isabs(DB_PATH):
    DB_PATH = os.path.join(DATA_DIR, DB_PATH)

os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

PRICES = {
    "vip_low": (300, "🟢 VIP Низкий"),
    "vip_mid": (500, "🟡 VIP Средний"),
    "vip_high": (700, "🔴 VIP Высокий"),
    "chars_600": (40, "📝 Символы до 600"),
    "unlimited_likes": (200, "❤️ Безлимит лайков (1 день)"),
    "prev_anketa": (80, "⏪ Возврат анкеты"),
    "second_media": (60, "📸 Второе фото/видео"),
    "incognito_day": (30, "🕵️ Чат инкогнито (1 день)"),
}

VIP_LIMITS = {
    0: {"likes": 20, "chars": 300, "photos": 1, "video": False, "priority": 0, "prev": False},
    1: {"likes": 40, "chars": 600, "photos": 1, "video": False, "priority": 1, "prev": False},
    2: {"likes": 60, "chars": 800, "photos": 2, "video": False, "priority": 2, "prev": False},
    3: {"likes": 999999, "chars": 1000, "photos": 2, "video": True, "priority": 3, "prev": True},
}

VERIFY_WORDS = ["КЛЕН", "ЛИСТ", "БУКЕТ", "ОСЕНЬ", "ВЕТЕР", "ЗАКАТ"]

STATUS_RU = {
    "new": "🥚 Новичок",
    "awaiting_contact": "📱 Ждёт номер",
    "awaiting_video": "🎥 Ждёт видео",
    "awaiting_form": "📝 Заполняет анкету",
    "pending_review": "⏳ На модерации",
    "active": "✅ Активен",
    "banned": "🚫 Забанен",
    "rejected": "❌ Отклонён",
}

def get_status_ru(status: str) -> str:
    return STATUS_RU.get(status, status)

logging.basicConfig(
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("klenchik")

WAITING_USERS = set()
ACTIVE_CHATS = {}

def get_payment_contact() -> str:
    pl = (PAYMENT_LINK or "").strip()
    if not pl:
        return "@admin"
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if pl.startswith(prefix):
            username = pl[len(prefix):].split("/")[0].split("?")[0]
            return "@" + username
    if not pl.startswith("@"):
        return "@" + pl
    return pl

SCHEMA = """
CREATE TABLE IF NOT EXISTS users(
 tg_id INTEGER PRIMARY KEY, username TEXT, phone TEXT,
 status TEXT DEFAULT 'new', banned INTEGER DEFAULT 0, ban_reason TEXT,
 vip_level INTEGER DEFAULT 0, vip_until TEXT,
 incognito_until TEXT,
 likes_left INTEGER DEFAULT 20, likes_reset_at TEXT,
 photos_used INTEGER DEFAULT 0, video_used INTEGER DEFAULT 0,
 chars_limit INTEGER DEFAULT 300, prev_anketa_id INTEGER,
 attempts_left INTEGER DEFAULT 3, verify_words TEXT, has_prev_addon INTEGER DEFAULT 0,
 pending_action TEXT, last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP, created_at TEXT
);
CREATE TABLE IF NOT EXISTS profiles(
 id INTEGER PRIMARY KEY AUTOINCREMENT, tg_id INTEGER UNIQUE,
 name TEXT, age INTEGER, pronouns TEXT, city TEXT, diagnosis TEXT,
 reason TEXT, hobby TEXT, photo1 TEXT, photo2 TEXT, video TEXT,
 approved INTEGER DEFAULT 0, created_at TEXT
);
CREATE TABLE IF NOT EXISTS likes(from_tg INTEGER, to_tg INTEGER, created_at TEXT);
CREATE TABLE IF NOT EXISTS reports(id INTEGER PRIMARY KEY AUTOINCREMENT, reporter INTEGER, target INTEGER, reason TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS admin_states(tg_id INTEGER PRIMARY KEY, action TEXT, target_id INTEGER, payload TEXT);
CREATE TABLE IF NOT EXISTS support_tickets(admin_msg_id INTEGER PRIMARY KEY, user_id INTEGER, created_at TEXT);
"""

def _sync_exec(query: str, params: tuple = (), fetch: Optional[str] = None):
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        if fetch == "one":
            return cur.fetchone()
        if fetch == "all":
            return cur.fetchall()
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()

async def db_exec(query: str, params: tuple = (), fetch: Optional[str] = None):
    return await asyncio.to_thread(_sync_exec, query, params, fetch)

def now_iso():
    return datetime.utcnow().isoformat()

async def db_init():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    try: conn.execute("ALTER TABLE users ADD COLUMN verify_words TEXT")
    except: pass
    try: conn.execute("ALTER TABLE users ADD COLUMN has_prev_addon INTEGER DEFAULT 0")
    except: pass
    try: conn.execute("ALTER TABLE users ADD COLUMN pending_action TEXT")
    except: pass
    try: conn.execute("ALTER TABLE users ADD COLUMN incognito_until TEXT")
    except: pass
    try: conn.execute("ALTER TABLE users ADD COLUMN last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    except: pass
    conn.commit()
    conn.close()

async def get_user(tg_id: int):
    return await db_exec("SELECT * FROM users WHERE tg_id=?", (tg_id,), fetch="one")

async def upsert_user(tg_id: int, **kw):
    u = await get_user(tg_id)
    if not u:
        await db_exec(
            "INSERT INTO users(tg_id, status, likes_left, likes_reset_at, chars_limit, attempts_left, created_at, last_active) VALUES(?,?,?,?,?,?,?,?)",
            (tg_id, "new", 20, now_iso(), 300, 3, now_iso(), now_iso())
        )
        u = await get_user(tg_id)
    if kw:
        sets = ", ".join(f"{k}=?" for k in kw)
        await db_exec(f"UPDATE users SET {sets} WHERE tg_id=?", (*kw.values(), tg_id))
        u = await get_user(tg_id)
    return u

async def update_activity(tg_id: int):
    await db_exec("UPDATE users SET last_active=CURRENT_TIMESTAMP WHERE tg_id=?", (tg_id,))

async def vip_limits(u):
    lvl = u["vip_level"] or 0
    if u["vip_until"]:
        try:
            if datetime.fromisoformat(u["vip_until"]) < datetime.utcnow():
                lvl = 0
                await upsert_user(u["tg_id"], vip_level=0, vip_until=None)
        except: pass
    return VIP_LIMITS.get(lvl, VIP_LIMITS[0])

async def refresh_likes(tg_id: int):
    u = await get_user(tg_id)
    if not u: return
    lim = await vip_limits(u)
    reset = u["likes_reset_at"]
    if not reset or datetime.fromisoformat(reset) < datetime.utcnow() - timedelta(days=1):
        await upsert_user(tg_id, likes_left=lim["likes"], likes_reset_at=now_iso())

async def safe(func, *args, **kwargs):
    try:
        return await func(*args, **kwargs)
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after + 1)
        return await safe(func, *args, **kwargs)
    except (TimedOut, NetworkError):
        return None
    except (Forbidden, BadRequest) as e:
        log.warning(f"TG API Error: {e}")
        return None
    except Exception as e:
        log.error(f"Unexpected error: {e}")
        return None

def esc(text) -> str:
    return html.escape(str(text), quote=False) if text else ""

def main_menu_kb(user_id: int = 0):
    kb_list = [
        [InlineKeyboardButton("🗂 Смотреть анкеты", callback_data="menu:ankety")],
        [InlineKeyboardButton("💬 Чат инкогнито", callback_data="menu:incognito")],
        [InlineKeyboardButton("🛒 Магазин", callback_data="menu:shop")],
        [InlineKeyboardButton("🆘 Поддержка", callback_data="menu:support")],
        [InlineKeyboardButton("👤 Моя анкета", callback_data="menu:profile")],
    ]
    if user_id == SUPERADMIN_ID:
        kb_list.append([InlineKeyboardButton("🛠 Админ-панель", callback_data="a:main")])
    return InlineKeyboardMarkup(kb_list)

def incognito_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Искать собеседника", callback_data="incog:search")],
        [InlineKeyboardButton("⬅️ В меню", callback_data="menu:back")]
    ])

def incog_active_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛑 Остановить чат", callback_data="incog:stop")]
    ])

async def send_main_menu(bot, chat_id):
    text = (
        "🍁 <b>Главное меню Кленчика</b>\n\n"
        "Выбери, что хочешь сделать:\n\n"
        "🗂 <b>Смотреть анкеты</b> — листай анкеты других пользователей и ставь лайки\n"
        "💬 <b>Чат инкогнито</b> — анонимное общение со случайным человеком\n"
        "🛒 <b>Магазин</b> — VIP-статусы и дополнения для анкеты\n"
        "🆘 <b>Поддержка</b> — напиши нам, если что-то не работает\n"
        "👤 <b>Моя анкета</b> — посмотреть свою анкету и VIP-статус"
    )
    await safe(bot.send_message, chat_id=chat_id, text=text, reply_markup=main_menu_kb(chat_id), parse_mode=ParseMode.HTML)

def admin_main_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 Пользователи", callback_data="a:users:0")],
        [InlineKeyboardButton("📝 На модерации", callback_data="a:pending")],
        [InlineKeyboardButton("⚙️ Выдать по ID", callback_data="a:givebyid")],
        [InlineKeyboardButton("🗑 Забрать по ID", callback_data="a:takebyid")],
        [InlineKeyboardButton("🚫 Баны", callback_data="a:bans")],
        [InlineKeyboardButton("📊 Статистика", callback_data="a:stats")],
        [InlineKeyboardButton("⬅️ В главное меню", callback_data="menu:back")]
    ])

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user: return

    await update_activity(user.id)

    if user.id in ACTIVE_CHATS:
        partner = ACTIVE_CHATS.pop(user.id, None)
        if partner:
            ACTIVE_CHATS.pop(partner, None)
            await safe(ctx.bot.send_message, partner, "🛑 Собеседник покинул чат. Ты можешь найти нового.", reply_markup=incognito_menu_kb())
        WAITING_USERS.discard(user.id)

    await db_exec("DELETE FROM admin_states WHERE tg_id=?", (user.id,))
    await db_exec("UPDATE users SET pending_action=NULL WHERE tg_id=?", (user.id,))

    u = await upsert_user(user.id, username=user.username or "")

    if u["banned"]:
        return await update.message.reply_text(f"🚫 <b>Доступ к боту заблокирован.</b>\n\nПричина: {u['ban_reason'] or 'не указана'}\n\nЕсли считаешь это ошибкой — напиши администратору.", parse_mode=ParseMode.HTML)

    if u["status"] == "active":
        return await send_main_menu(ctx.bot, user.id)

    if u["status"] == "pending_review":
        return await update.message.reply_text(
            "⏳ <b>Анкета на модерации</b> 🍁\n\n"
            "Твоя анкета уже у модератора. Обычно проверка занимает немного времени.\n\n"
            "<i>Мы напишем тебе, как только примем решение. Просто подожди немного.</i>",
            parse_mode=ParseMode.HTML
        )

    if u["status"] == "new":
        await upsert_user(user.id, status="awaiting_contact")
        kb = ReplyKeyboardMarkup([[KeyboardButton("📱 Поделиться номером", request_contact=True)]], resize_keyboard=True, one_time_keyboard=True)
        return await update.message.reply_text(
            "🍁 <b>Добро пожаловать в Кленчик!</b> 🍁\n\n"
            "Здесь ты можешь найти друзей, пообщаться в анонимном чате и просто провести время в уютной осенней атмосфере.\n\n"
            "📋 <b>Шаг 1 из 3: Знакомство</b>\n"
            "Пожалуйста, поделись своим номером телефона — нажми на кнопку ниже.\n\n"
            "🔒 <i>Твой номер останется внутри бота и никуда не передаётся. Он нужен только для защиты от мультиаккаунтов и проверки возраста.</i>",
            reply_markup=kb, parse_mode=ParseMode.HTML
        )

    if u["status"] == "awaiting_contact":
        kb = ReplyKeyboardMarkup([[KeyboardButton("📱 Поделиться номером", request_contact=True)]], resize_keyboard=True, one_time_keyboard=True)
        return await update.message.reply_text("🍁 Пожалуйста, поделись номером с помощью кнопки ниже.", reply_markup=kb)

    if u["status"] == "awaiting_video":
        words = u["verify_words"] or "—"
        return await update.message.reply_text(
            "🎥 <b>Шаг 2 из 3: Видео-кружок</b> 🍁\n\n"
            "Чтобы убедиться, что ты реальный человек, запиши <b>видео-кружок</b> (нажми на скрепку 📎 → «Видео-кружок»).\n\n"
            f"🎥 В кружке нужно чётко произнести вслух: <b>{words}</b>\n\n"
            "Значок 🎥 — справа от поля ввода, рядом с кнопкой отправки. Обычное видео или фото не подойдут.\n\n"
            "⚠️ <i>У тебя 3 попытки. Если пришлёшь не кружок — попытка сгорит. После 3 промахов бот заблокирует доступ.</i>",
            reply_markup=ReplyKeyboardRemove(), parse_mode=ParseMode.HTML
        )

    if u["status"] == "awaiting_form":
        lim = await vip_limits(u)
        chars = max(u["chars_limit"] or 300, lim["chars"])
        template = (
            "<code>1. Имя/Псевдоним:\n2. Возраст:\n3. Местоимения:\n"
            "4. Город:\n5. Диагноз:\n6. Зачем пришёл/ла:\n7. Хобби:</code>"
        )
        return await update.message.reply_text(
            f"📝 <b>Шаг 3 из 3: Анкета</b> 🍁\n\n"
            "Отлично, видео принято! Остался последний шаг — заполни анкету о себе.\n\n"
            "Скопируй шаблон ниже (тапни по нему), заполни все поля и отправь <b>одним сообщением</b>:\n\n"
            f"{template}\n\n"
            f"⚠️ <b>Лимит символов:</b> {chars}\n"
            "📸 Можешь прикрепить одно фото к сообщению — анкеты с фото получают больше лайков!\n"
            "🔞 Возраст от 12 лет.",
            parse_mode=ParseMode.HTML, reply_markup=ReplyKeyboardRemove()
        )

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update_activity(user_id)
    
    if user_id in WAITING_USERS:
        WAITING_USERS.remove(user_id)
        return await update.message.reply_text("🛑 Поиск собеседника отменён.", reply_markup=incognito_menu_kb())
        
    if user_id in ACTIVE_CHATS:
        partner_id = ACTIVE_CHATS.pop(user_id)
        ACTIVE_CHATS.pop(partner_id, None)
        
        await update.message.reply_text("🛑 Ты покинул анонимный чат.", reply_markup=incognito_menu_kb())
        await safe(ctx.bot.send_message, partner_id, "🛑 Собеседник покинул чат. Ты можешь найти нового.", reply_markup=incognito_menu_kb())
        return

    await update.message.reply_text("Вы сейчас не в чате и не в поиске.")

async def on_contact(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.contact: return
    await update_activity(msg.from_user.id)
    
    if msg.contact.user_id != msg.from_user.id:
        return await msg.reply_text("🍁 Пожалуйста, используй кнопку ниже.")

    words = random.sample(VERIFY_WORDS, 3)
    words_str = ", ".join(words)

    await upsert_user(msg.from_user.id, phone=msg.contact.phone_number, status="awaiting_video", verify_words=words_str, attempts_left=3)

    await msg.reply_text(
        "✅ Спасибо! Номер принят. 🍁\n\n"
        "🎥 <b>Шаг 2 из 3: Подтверждение личности</b>\n\n"
        "Запиши <b>видео-кружок</b> (нажми на скрепку 📎 → «Видео-кружок»).\n\n"
        "🎥 В кружке должно быть:\n"
        "• Лицо чётко видно, без размытия и бликов\n"
        f"• Ты произносишь вслух: <b>{words_str}</b>\n\n"
        "Значок 🎥 — справа от поля ввода, рядом с кнопкой отправки. Обычное видео или фото не подойдут.\n\n"
        "⚠️ <i>У тебя 3 попытки. Если пришлёшь не кружок — попытка сгорит.</i>",
        parse_mode=ParseMode.HTML, reply_markup=ReplyKeyboardRemove()
    )

async def on_video(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.video_note: return
    user_id = msg.from_user.id
    await update_activity(user_id)
    
    if user_id in ACTIVE_CHATS:
        partner = ACTIVE_CHATS[user_id]
        await safe(ctx.bot.copy_message, chat_id=partner, from_chat_id=user_id, message_id=msg.message_id)
        return

    u = await get_user(user_id)
    if not u: return
    if u["banned"]: return

    if u["status"] == "awaiting_video":
        await upsert_user(user_id, status="awaiting_form")

        await safe(
            db_exec,
            "INSERT OR IGNORE INTO profiles(tg_id, video, approved, created_at) VALUES(?,?,0,?)",
            (user_id, msg.video_note.file_id, now_iso())
        )

        lim = await vip_limits(u)
        chars = max(u["chars_limit"] or 300, lim["chars"])
        template = (
            "<code>1. Имя/Псевдоним:\n2. Возраст:\n3. Местоимения:\n"
            "4. Город:\n5. Диагноз:\n6. Зачем пришёл/ла:\n7. Хобби:</code>"
        )

        await msg.reply_text("✅ Видео принято! 🍁")
        return await msg.reply_text(
            f"📝 <b>Шаг 3 из 3: Анкета</b> 🍁\n\n"
            "Отлично, остался последний шаг — заполни анкету о себе.\n\n"
            "Скопируй шаблон ниже (тапни по нему), заполни все поля и отправь <b>одним сообщением</b>:\n\n"
            f"{template}\n\n"
            f"⚠️ <b>Лимит символов:</b> {chars}\n"
            "📸 Можешь прикрепить одно фото к сообщению — анкеты с фото получают больше лайков!\n"
            "🔞 Возраст от 12 лет.",
            parse_mode=ParseMode.HTML
        )
        
    if u["status"] == "active":
        await send_main_menu(ctx.bot, user_id)

def parse_form(text: str):
    lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
    if len(lines) != 7: return None
    out = {}
    keys = ["name", "age", "pronouns", "city", "diagnosis", "reason", "hobby"]
    field_names = ["Имя/Псевдоним", "Возраст", "Местоимения", "Город", "Диагноз", "Зачем пришёл/ла", "Хобби"]

    for i, key in enumerate(keys):
        line = lines[i]
        if not line.startswith(f"{i+1}."): return None
        val = line.split(".", 1)[1].strip()
        if val.startswith(field_names[i]):
            val = val[len(field_names[i]):].strip()
        val = val.lstrip(":").strip()
        if not val: return None
        out[key] = val

    try:
        age_str = re.sub(r'\D', '', out["age"])
        age = int(age_str)
    except: return None
    if not (12 <= age <= 99): return None
    out["age"] = age
    return out

async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.text: return
    user_id = msg.from_user.id
    text = msg.text
    await update_activity(user_id)

    if user_id in ACTIVE_CHATS:
        partner = ACTIVE_CHATS[user_id]
        await safe(ctx.bot.copy_message, chat_id=partner, from_chat_id=user_id, message_id=msg.message_id)
        return

    admin_state = await db_exec("SELECT * FROM admin_states WHERE tg_id=?", (user_id,), fetch="one")
    if admin_state:
        await handle_admin_input(msg, ctx, admin_state)
        return

    u = await get_user(user_id)
    if not u: return
    if u["banned"]: return

    if u["pending_action"] == "support":
        await db_exec("UPDATE users SET pending_action=NULL WHERE tg_id=?", (user_id,))
        
        sent_msg = await safe(
            ctx.bot.send_message,
            ADMIN_CHAT_ID,
            f"🆘 <b>Обращение в поддержку</b>\n\n"
            f"👤 @{esc(u['username']) if u['username'] else 'без username'}\n"
            f"🆔 <code>{user_id}</code>\n\n"
            f"💬 <b>Сообщение:</b>\n{esc(text)}\n\n"
            f"<i>Чтобы ответить, зажми это сообщение и выбери «Ответить».</i>",
            parse_mode=ParseMode.HTML
        )
        
        if sent_msg:
            await db_exec("INSERT INTO support_tickets(admin_msg_id, user_id, created_at) VALUES(?,?,?)", (sent_msg.message_id, user_id, now_iso()))
            
        return await msg.reply_text("✅ Обращение отправлено! 🍁\n\n<i>Администратор ответит тебе прямо здесь, как только освободится.</i>", parse_mode=ParseMode.HTML)

    if u["status"] == "awaiting_video":
        left = (u["attempts_left"] or 1) - 1
        if left <= 0:
            await upsert_user(user_id, banned=1, ban_reason="Не прошёл верификацию", status="banned")
            return await msg.reply_text("🚫 Вы исчерпали попытки. Доступ заблокирован.")
        await upsert_user(user_id, attempts_left=left)
        return await msg.reply_text(f"❌ Ты прислал не видео-кружок. Осталось попыток: <b>{left}</b>. После этого доступ будет заблокирован.", parse_mode=ParseMode.HTML)

    if u["status"] == "awaiting_form":
        data = parse_form(text)
        if not data:
            return await msg.reply_text(
                "❌ <b>Анкета заполнена не по форме.</b>\n\n"
                "Скопируй шаблон ниже, заполни все поля и отправь <b>одним сообщением</b>:\n\n"
                "<code>1. Имя/Псевдоним:\n2. Возраст:\n3. Местоимения:\n4. Город:\n5. Диагноз:\n6. Зачем пришёл/ла:\n7. Хобби:</code>",
                parse_mode=ParseMode.HTML
            )

        lim = await vip_limits(u)
        chars_limit = max(u["chars_limit"] or 300, lim["chars"])
        for k in ["reason", "hobby", "diagnosis"]:
            if len(data[k]) > chars_limit:
                return await msg.reply_text(f"❌ Поле «{k}» превышает лимит {chars_limit} символов. Сократи, пожалуйста.")

        photo1 = msg.photo[-1].file_id if msg.photo else None

        await db_exec(
            "UPDATE profiles SET name=?, age=?, pronouns=?, city=?, diagnosis=?, reason=?, hobby=?, photo1=COALESCE(?,photo1), approved=0 WHERE tg_id=?",
            (data["name"], data["age"], data["pronouns"], data["city"], data["diagnosis"], data["reason"], data["hobby"], photo1, user_id)
        )
        prof = await db_exec("SELECT * FROM profiles WHERE tg_id=?", (user_id,), fetch="one")

        await upsert_user(user_id, status="pending_review")
        await msg.reply_text(
            "📨 <b>Анкета отправлена на модерацию!</b> 🍁\n\n"
            "Спасибо за регистрацию! Твоя анкета сейчас на проверке у модератора.\n\n"
            "🕒 <i>Обычно это занимает немного времени. Мы напишем тебе, как только примем решение — жди сообщение от бота.</i>\n\n"
            "Пока можешь отдохнуть. Скоро увидимся! 🍁",
            parse_mode=ParseMode.HTML
        )
        await send_to_admin_chat(ctx.bot, prof, u)
        return
        
    if u["status"] == "active":
        await send_main_menu(ctx.bot, user_id)
    elif u["status"] == "pending_review":
        await msg.reply_text("⏳ Твоя анкета всё ещё на модерации. Пожалуйста, подожди немного 🍁")

async def on_media(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg: return
    user_id = msg.from_user.id
    await update_activity(user_id)
    
    if user_id in ACTIVE_CHATS:
        partner = ACTIVE_CHATS[user_id]
        await safe(ctx.bot.copy_message, chat_id=partner, from_chat_id=user_id, message_id=msg.message_id)
        return
        
    if msg.photo and msg.caption:
        await on_text(update, ctx)
        return
        
    u = await get_user(user_id)
    if u and u["status"] == "active":
        await send_main_menu(ctx.bot, user_id)

async def send_to_admin_chat(bot, prof, u):
    text = (
        f"🆕 <b>Новая анкета на модерацию</b>\n\n"
        f"👤 <b>Пользователь:</b> <code>{prof['tg_id']}</code>\n"
        f"📞 <b>Телефон:</b> {u['phone']}\n"
        f"🗣 <b>Слова для кружка:</b> {u['verify_words']}\n\n"
        f"<b>Анкета:</b>\n"
        f"👤 <b>{prof['name']}</b>, {prof['age']}\n"
        f"🏳️‍🌈 Местоимения: {prof['pronouns']}\n"
        f"📍 Город: {prof['city']}\n"
        f"🩺 Диагноз: {prof['diagnosis']}\n"
        f"💭 Зачем пришёл/ла: {prof['reason']}\n"
        f"🎨 Хобби: {prof['hobby']}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Принять", callback_data=f"adm:approve:{prof['tg_id']}"),
         InlineKeyboardButton("❌ Отклонить", callback_data=f"adm:reject:{prof['tg_id']}")],
        [InlineKeyboardButton("✏️ Переделать", callback_data=f"adm:rework:{prof['tg_id']}")]
    ])
    try:
        if prof["video"]: await bot.send_video_note(ADMIN_CHAT_ID, prof["video"])
        if prof["photo1"]: await bot.send_photo(ADMIN_CHAT_ID, prof["photo1"])
        await bot.send_message(ADMIN_CHAT_ID, text, reply_markup=kb, parse_mode=ParseMode.HTML)
    except Exception as e:
        log.error(f"Err admin chat: {e}")

async def on_admin_reply(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.text or not msg.reply_to_message: return
    
    admin_msg_id = msg.reply_to_message.message_id
    ticket = await db_exec("SELECT user_id FROM support_tickets WHERE admin_msg_id=?", (admin_msg_id,), fetch="one")
    
    if ticket:
        user_id = ticket["user_id"]
        sent = await safe(ctx.bot.send_message, user_id, f"🆘 <b>Ответ поддержки:</b>\n\n{esc(msg.text)}", parse_mode=ParseMode.HTML)
        if sent:
            await msg.reply_text("✅ Ответ отправлен пользователю.")
        else:
            await msg.reply_text("❌ Не удалось отправить (пользователь заблокировал бота?).")

async def cmd_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != SUPERADMIN_ID: return
    await update.message.reply_text("🛠 <b>Админ-панель</b>", reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML)

async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id
    await update_activity(user_id)

    if data.startswith("adm:"):
        if user_id != SUPERADMIN_ID: return
        _, action, tid = data.split(":")
        tid = int(tid)

        if action == "approve":
            await db_exec("UPDATE profiles SET approved=1 WHERE tg_id=?", (tid,))
            await db_exec("UPDATE users SET status='active', banned=0 WHERE tg_id=?", (tid,))
            await safe(ctx.bot.send_message, tid, "🎉 <b>Твоя анкета одобрена!</b> 🍁\n\nДобро пожаловать в Кленчик. Теперь тебе доступны все функции.\n\nВыбирай раздел ниже 👇", reply_markup=main_menu_kb(tid), parse_mode=ParseMode.HTML)
            await query.edit_message_reply_markup(reply_markup=None)

        elif action == "reject":
            await db_exec("UPDATE users SET status='banned', banned=1, ban_reason='Отклонено' WHERE tg_id=?", (tid,))
            await safe(ctx.bot.send_message, tid, "🚫 <b>Твоя анкета отклонена.</b>\n\nК сожалению, модератор отклонил твою заявку. Доступ к боту заблокирован.", parse_mode=ParseMode.HTML)
            await query.edit_message_reply_markup(reply_markup=None)

        elif action == "rework":
            await db_exec("INSERT OR REPLACE INTO admin_states(tg_id, action, target_id) VALUES(?,?,?)", (user_id, "rework", tid))
            await query.message.reply_text(f"Введи комментарий для {tid} (что именно нужно исправить):")
        return

    if data.startswith("a:"):
        if user_id != SUPERADMIN_ID: return
        parts = data.split(":")
        act = parts[1]

        if act == "main" or act == "back":
            await query.edit_message_text("🛠 <b>Админ-панель</b>", reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML)

        elif act == "users":
            offset = int(parts[2])
            users = await db_exec("SELECT * FROM users ORDER BY last_active DESC LIMIT 10 OFFSET ?", (offset,), fetch="all")
            rows = [[InlineKeyboardButton(f"{u['tg_id']} | {u['status']} | VIP{u['vip_level']}", callback_data=f"a:u:{u['tg_id']}")] for u in users]
            nav = []
            if offset > 0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"a:users:{offset-10}"))
            if len(users) == 10: nav.append(InlineKeyboardButton("➡️", callback_data=f"a:users:{offset+10}"))
            if nav: rows.append(nav)
            rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="a:back")])
            await query.edit_message_text(f"👥 Пользователи ({offset+1}-{offset+len(users)}):", reply_markup=InlineKeyboardMarkup(rows))

        elif act == "u":
            tid = int(parts[2])
            u = await get_user(tid)
            if not u: return await query.answer("Пользователь не найден.", show_alert=True)
            p = await db_exec("SELECT * FROM profiles WHERE tg_id=?", (tid,), fetch="one")
            txt = f"👤 <b>{tid}</b>\nstatus={u['status']} banned={u['banned']}\nVIP={u['vip_level']} до {u['vip_until'] or '—'}\nlikes={u['likes_left']}, chars={u['chars_limit']}\nhas_prev={u['has_prev_addon']}\nlast_active={u['last_active']}"
            if p: txt += f"\n\n<b>{p['name']}</b>, {p['age']}\n{p['diagnosis']}"

            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🚫 Забанить", callback_data=f"a:ban:{tid}"),
                 InlineKeyboardButton("✅ Разбанить", callback_data=f"a:unban:{tid}")],
                [InlineKeyboardButton("👑 Выдать VIP", callback_data=f"a:givevip:{tid}"),
                 InlineKeyboardButton("🚫 Забрать VIP", callback_data=f"a:takevip:{tid}")],
                [InlineKeyboardButton("🎁 Выдать доп", callback_data=f"a:givedop:{tid}"),
                 InlineKeyboardButton("🚫 Забрать доп", callback_data=f"a:takedop:{tid}")],
                [InlineKeyboardButton("🗑 Удалить анкету", callback_data=f"a:delprof:{tid}")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="a:users:0")]
            ])
            await query.edit_message_text(txt, reply_markup=kb, parse_mode=ParseMode.HTML)

        elif act == "givebyid":
            await db_exec("INSERT OR REPLACE INTO admin_states(tg_id, action, target_id) VALUES(?,?,?)", (user_id, "givebyid", 0))
            await query.message.reply_text(
                "⚙️ <b>Выдача по ID</b>\n\n"
                "Отправь строку в формате:\n"
                "<code><tg_id> тип значение1 значение2</code>\n\n"
                "<b>Примеры:</b>\n"
                "<code>123456789 вип средний 30</code> — VIP Средний на 30 дней\n"
                "<code>123456789 доп символы 0</code> — символы до 600\n"
                "<code>123456789 доп лайки 0</code> — безлимит лайков\n"
                "<code>123456789 доп возврат 0</code> — возврат анкеты\n"
                "<code>123456789 доп фото 0</code> — второе фото\n"
                "<code>123456789 доп инкогнито 1</code> — инкогнито на 1 день",
                parse_mode=ParseMode.HTML
            )

        elif act == "takebyid":
            await db_exec("INSERT OR REPLACE INTO admin_states(tg_id, action, target_id) VALUES(?,?,?)", (user_id, "takebyid", 0))
            await query.message.reply_text(
                "🗑 <b>Забрать по ID</b>\n\n"
                "Отправь строку в формате:\n"
                "<code><tg_id> тип</code>\n\n"
                "<b>Примеры:</b>\n"
                "<code>123456789 вип</code> — снять VIP\n"
                "<code>123456789 символы</code> — снять доп на символы\n"
                "<code>123456789 лайки</code> — снять безлимит\n"
                "<code>123456789 возврат</code> — снять возврат анкеты\n"
                "<code>123456789 фото</code> — снять второе фото\n"
                "<code>123456789 инкогнито</code> — снять инкогнито\n"
                "<code>123456789 все</code> — снять все допы",
                parse_mode=ParseMode.HTML
            )

        elif act == "ban":
            tid = int(parts[2])
            await db_exec("INSERT OR REPLACE INTO admin_states(tg_id, action, target_id) VALUES(?,?,?)", (user_id, "ban", tid))
            await query.message.reply_text("Введи причину бана:")

        elif act == "unban":
            tid = int(parts[2])
            await upsert_user(tid, banned=0, status="active")
            await query.message.reply_text("✅ Разбанен.")

        elif act == "givevip":
            tid = int(parts[2])
            await db_exec("INSERT OR REPLACE INTO admin_states(tg_id, action, target_id) VALUES(?,?,?)", (user_id, "vip", tid))
            await query.message.reply_text("Введи уровень (1/2/3) и дней, например: 2 30")

        elif act == "takevip":
            tid = int(parts[2])
            await upsert_user(tid, vip_level=0, vip_until=None, chars_limit=300, likes_left=20)
            await safe(ctx.bot.send_message, tid, "⚠️ Твой VIP был снят администратором.")
            await query.message.reply_text("✅ VIP снят.")

        elif act == "givedop":
            tid = int(parts[2])
            await db_exec("INSERT OR REPLACE INTO admin_states(tg_id, action, target_id) VALUES(?,?,?)", (user_id, "dop", tid))
            await query.message.reply_text("Введи доп (символы / лайки / возврат / фото / инкогнито) и дней через пробел:")

        elif act == "takedop":
            tid = int(parts[2])
            await db_exec("INSERT OR REPLACE INTO admin_states(tg_id, action, target_id) VALUES(?,?,?)", (user_id, "takedop", tid))
            await query.message.reply_text(
                "Что снять? Отправь одно слово:\n"
                "символы / лайки / возврат / фото / инкогнито / все"
            )

        elif act == "delprof":
            tid = int(parts[2])
            await db_exec("INSERT OR REPLACE INTO admin_states(tg_id, action, target_id) VALUES(?,?,?)", (user_id, "delprof", tid))
            await query.message.reply_text("Причина удаления анкеты:")

        elif act == "pending":
            pending = await db_exec("SELECT * FROM profiles WHERE approved=0 ORDER BY id DESC LIMIT 20", fetch="all")
            if not pending: return await query.message.reply_text("Пусто.")
            for p in pending:
                u = await get_user(p["tg_id"])
                await send_to_admin_chat(ctx.bot, p, u)
            await query.message.reply_text("Отправлено в чат.")

        elif act == "bans":
            bans = await db_exec("SELECT * FROM users WHERE banned=1 LIMIT 20", fetch="all")
            if not bans: return await query.message.reply_text("Нет забаненных.")
            txt = "\n".join(f"{u['tg_id']} — {u['ban_reason']}" for u in bans)
            await query.message.reply_text(txt)

        elif act == "stats":
            total = (await db_exec("SELECT COUNT(*) FROM users", fetch="one"))[0]
            active = (await db_exec("SELECT COUNT(*) FROM users WHERE status='active'", fetch="one"))[0]
            banned = (await db_exec("SELECT COUNT(*) FROM users WHERE banned=1", fetch="one"))[0]
            vip = (await db_exec("SELECT COUNT(*) FROM users WHERE vip_level>0", fetch="one"))[0]
            await query.message.reply_text(f"📊 Всего: {total}\nАктивных: {active}\nЗабанено: {banned}\nС VIP: {vip}")

        return

    u = await get_user(user_id)
    if not u: return

    if data == "menu:back":
        WAITING_USERS.discard(user_id)
        await query.message.edit_text(
            "🍁 <b>Главное меню Кленчика</b>\n\nВыбери, что хочешь сделать:",
            reply_markup=main_menu_kb(user_id),
            parse_mode=ParseMode.HTML
        )

    elif data == "menu:ankety":
        await refresh_likes(user_id)
        u = await get_user(user_id)
        if u["likes_left"] <= 0: return await query.answer("❤️ Лимит лайков исчерпан. Купи доп в магазине.", show_alert=True)
        await show_next_profile(query, u, 0)

    elif data.startswith("skip:"):
        parts = data.split(":")
        offset = int(parts[1])
        skipped_id = int(parts[2])
        await upsert_user(user_id, prev_anketa_id=skipped_id)
        await show_next_profile(query, u, offset)

    elif data.startswith("prev:"):
        target_id = int(data.split(":")[1])
        has_prev = (u["vip_level"] == 3) or (u["has_prev_addon"] == 1)
        if not has_prev:
            return await query.answer("⏪ Доступно только с VIP Высоким или допом «Возврат анкеты».", show_alert=True)
        prof = await db_exec("SELECT * FROM profiles WHERE tg_id=?", (target_id,), fetch="one")
        if not prof:
            return await query.answer("Анкета больше недоступна.", show_alert=True)
        text = f"👤 <b>{prof['name']}</b>, {prof['age']} ({prof['pronouns']})\n📍 {prof['city']}\n\n🩺 {prof['diagnosis']}\n💭 {prof['reason']}\n🎨 {prof['hobby']}"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("❤️ Лайк", callback_data=f"like:{prof['tg_id']}"),
             InlineKeyboardButton("➡️ Скип", callback_data=f"skip:0:{prof['tg_id']}")],
            [InlineKeyboardButton("⬅️ В меню", callback_data="menu:back")]
        ])
        if prof["photo1"]:
            await query.message.reply_photo(prof["photo1"], caption=text, reply_markup=kb, parse_mode=ParseMode.HTML)
        else:
            await query.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)

    elif data.startswith("like:"):
        target = int(data.split(":")[1])
        await refresh_likes(user_id)
        u = await get_user(user_id)
        if u["likes_left"] <= 0: return await query.answer("❤️ Лимит лайков исчерпан.", show_alert=True)
        await upsert_user(user_id, likes_left=u["likes_left"] - 1)
        await db_exec("INSERT INTO likes(from_tg, to_tg, created_at) VALUES(?,?,?)", (user_id, target, now_iso()))

        mutual = await db_exec("SELECT 1 FROM likes WHERE from_tg=? AND to_tg=?", (target, user_id), fetch="one")
        if mutual:
            target_user = await get_user(target)
            
            t_un = target_user["username"]
            c_un = u["username"]
            
            t_display = f"@{esc(t_un)}" if t_un else f'<a href="tg://user?id={target}">Написать в ЛС</a>'
            c_display = f"@{esc(c_un)}" if c_un else f'<a href="tg://user?id={user_id}">Написать в ЛС</a>'
            
            await safe(ctx.bot.send_message, target, f"💞 <b>Взаимная симпатия!</b> 🍁\n\nКому-то понравилась твоя анкета, и вы совпали!\nКонтакт для связи: {c_display}\n\n<i>Напиши первым, не стесняйся 🍁</i>", parse_mode=ParseMode.HTML)
            await safe(ctx.bot.send_message, user_id, f"💞 <b>Взаимная симпатия!</b> 🍁\n\nКому-то понравилась твоя анкета, и вы совпали!\nКонтакт для связи: {t_display}\n\n<i>Напиши первым, не стесняйся 🍁</i>", parse_mode=ParseMode.HTML)
            
        await query.answer("❤️ Лайк отправлен")
        await show_next_profile(query, u, 0)

    elif data == "menu:shop":
        rows = [[InlineKeyboardButton(text=v[1], callback_data=f"buyinfo:{k}")] for k, v in PRICES.items()]
        rows.append([InlineKeyboardButton("⬅️ Меню", callback_data="menu:back")])
        await query.message.edit_text(
            "🛒 <b>Магазин Кленчика</b> 🍁\n\nВыбери товар для подробностей:",
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=ParseMode.HTML
        )

    elif data.startswith("buyinfo:"):
        key = data.split(":")[1]
        price, name = PRICES[key]
        desc = {
            "vip_low": "🟢 <b>40 лайков в день</b>\n• Лимит символов: 600\n• Приоритет в выдаче анкет: средний",
            "vip_mid": "🟡 <b>60 лайков в день</b>\n• Лимит символов: 800\n• Возможность загрузить 2 фото\n• Приоритет в выдаче анкет: повышенный",
            "vip_high": "🔴 <b>Безлимитные лайки</b>\n• Лимит символов: 1000\n• 2 фото + видео (15 сек)\n• Возврат к предыдущей анкете\n• Приоритет в выдаче анкет: максимальный",
            "chars_600": "📝 Разблокирует возможность писать более длинные тексты в анкете (до 600 символов вместо 300).",
            "unlimited_likes": "❤️ Позволяет ставить безлимитное количество лайков на 1 день.",
            "prev_anketa": "⏪ Если ты случайно скипнул интересного человека, эта функция позволит вернуться к его анкете.",
            "second_media": "📸 Позволяет прикрепить дополнительное фото (или видео) к твоей анкете.",
            "incognito_day": "🕵️ Открывает доступ к анонимному чату со случайным собеседником на 1 день."
        }
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("💳 Купить", callback_data=f"buy:{key}")], [InlineKeyboardButton("⬅️ Назад", callback_data="menu:shop")]])
        await query.message.edit_text(f"{desc.get(key, '')}\n\n💰 Цена: <b>{price}₽</b>", reply_markup=kb, parse_mode=ParseMode.HTML)

    elif data.startswith("buy:"):
        key = data.split(":")[1]
        price, name = PRICES[key]
        contact = get_payment_contact()
        
        await query.message.edit_text(
            f"💳 <b>Оплата</b> 🍁\n\n"
            f"Товар: <b>{name}</b>\n"
            f"Стоимость: <b>{price}₽</b>\n\n"
            f"Чтобы купить, напиши нашему менеджеру: <b>{esc(contact)}</b>\n\n"
            f"<i>После оплаты менеджер вручную выдаст тебе доступ. Это займёт немного времени.</i>",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="menu:shop")]]),
            parse_mode=ParseMode.HTML
        )
        await safe(
            ctx.bot.send_message, SUPERADMIN_ID,
            f"💰 <b>Покупка</b>\n"
            f"tg_id: <code>{user_id}</code>\n"
            f"@{esc(u['username']) if u['username'] else '—'}\n"
            f"Товар: {name}\n"
            f"Цена: {price}₽\n\n"
            f"<i>Выдать через /admin → «Выдать по ID»</i>",
            parse_mode=ParseMode.HTML
        )

    elif data == "menu:incognito":
        until = u["incognito_until"]
        has_access = until and datetime.fromisoformat(until) > datetime.utcnow()
        
        if not has_access:
            return await query.answer("🕵️ У тебя нет доступа. Купи в магазине за 30₽/день.", show_alert=True)
            
        if user_id in ACTIVE_CHATS:
            return await query.answer("Ты уже в чате. Напиши /stop, чтобы выйти.", show_alert=True)
            
        await query.message.edit_text(
            "💬 <b>Анонимный чат</b> 🍁\n\n"
            "Здесь ты можешь анонимно пообщаться со случайным человеком. Никто не увидит твоё имя или фото — только сообщения.\n\n"
            "Нажми «Искать собеседника», и мы найдём тебе пару.\n\n"
            "⚠️ <i>Чтобы выйти из чата, напиши /stop или нажми «Остановить чат».</i>",
            reply_markup=incognito_menu_kb(),
            parse_mode=ParseMode.HTML
        )

    elif data == "incog:search":
        until = u["incognito_until"]
        has_access = until and datetime.fromisoformat(until) > datetime.utcnow()
        if not has_access:
            return await query.answer("Доступ истёк.", show_alert=True)

        if user_id in ACTIVE_CHATS:
            return await query.answer("Ты уже в чате. Напиши /stop.", show_alert=True)
            
        if user_id in WAITING_USERS:
            return await query.answer("Ты уже в поиске. Нажми /stop.", show_alert=True)

        if WAITING_USERS:
            partner_id = WAITING_USERS.pop()
            ACTIVE_CHATS[user_id] = partner_id
            ACTIVE_CHATS[partner_id] = user_id

            msg_pair = "✅ <b>Собеседник найден!</b> 🍁\n\nТы общаешься анонимно. Отправь сообщение.\nЧтобы выйти, нажми /stop."
            await safe(query.bot.send_message, user_id, msg_pair, reply_markup=incog_active_kb(), parse_mode=ParseMode.HTML)
            await safe(query.bot.send_message, partner_id, msg_pair, reply_markup=incog_active_kb(), parse_mode=ParseMode.HTML)
            await query.message.delete()
        else:
            WAITING_USERS.add(user_id)
            await query.message.edit_text(
                "🔍 <b>Ищем собеседника...</b> 🍁\n\n"
                "Пожалуйста, подожди. Как только кто-то зайдёт, вы сразу начнёте общение.\n\n"
                "Чтобы отменить поиск, напиши /stop или нажми кнопку ниже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Отменить поиск", callback_data="incog:stop")]]),
                parse_mode=ParseMode.HTML
            )

    elif data == "incog:stop":
        WAITING_USERS.discard(user_id)
        
        if user_id in ACTIVE_CHATS:
            partner_id = ACTIVE_CHATS.pop(user_id)
            ACTIVE_CHATS.pop(partner_id, None)
            
            await query.message.edit_text("🛑 Ты покинул чат.", reply_markup=incognito_menu_kb())
            await safe(query.bot.send_message, partner_id, "🛑 Собеседник покинул чат. Ты можешь найти нового.", reply_markup=incognito_menu_kb())
        else:
            await query.message.edit_text("🛑 Поиск отменён.", reply_markup=incognito_menu_kb())

    elif data == "menu:support":
        await db_exec("UPDATE users SET pending_action='support' WHERE tg_id=?", (user_id,))
        await query.message.edit_text(
            "🆘 <b>Поддержка</b> 🍁\n\n"
            "Есть вопрос или проблема? Напиши свой вопрос <b>одним сообщением</b>, и администратор ответит тебе прямо здесь.\n\n"
            "<i>Отвечаем быстро, если мы онлайн 🍁</i>",
            parse_mode=ParseMode.HTML
        )

    elif data == "menu:profile":
        prof = await db_exec("SELECT * FROM profiles WHERE tg_id=?", (user_id,), fetch="one")
        if not prof:
            return await query.answer("Анкета не найдена.", show_alert=True)
        text = (
            f"👤 <b>Моя анкета</b> 🍁\n\n"
            f"<b>{prof['name']}</b>, {prof['age']} ({prof['pronouns']})\n"
            f"📍 {prof['city']}\n"
            f"🩺 {prof['diagnosis']}\n"
            f"💭 {prof['reason']}\n"
            f"🎨 {prof['hobby']}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👑 <b>VIP:</b> {u['vip_level']} (до {u['vip_until'] or '—'})\n"
            f"❤️ <b>Лайков:</b> {u['likes_left']}\n"
            f"⚙️ <b>Статус:</b> {get_status_ru(u['status'])}"
        )
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В меню", callback_data="menu:back")]]), parse_mode=ParseMode.HTML)

async def show_next_profile(query, u, offset):
    async with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        prof = conn.execute(
            "SELECT p.*, u.vip_level FROM profiles p JOIN users u ON u.tg_id=p.tg_id WHERE p.approved=1 AND p.tg_id!=? AND u.last_active > datetime('now', '-7 days') ORDER BY u.vip_level DESC, p.id DESC LIMIT 1 OFFSET ?",
            (query.from_user.id, offset)
        ).fetchone()

    has_prev = (u["vip_level"] == 3) or (u["has_prev_addon"] == 1)

    if not prof:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В меню", callback_data="menu:back")]])
        if u["prev_anketa_id"] and has_prev:
            kb.inline_keyboard.insert(0, [InlineKeyboardButton("⏪ Вернуть пропущенную", callback_data=f"prev:{u['prev_anketa_id']}")])
        return await query.message.edit_text("🍁 На данный момент анкет больше нет.", reply_markup=kb)

    text = f"👤 <b>{prof['name']}</b>, {prof['age']} ({prof['pronouns']})\n📍 {prof['city']}\n\n🩺 <b>Диагноз:</b> {prof['diagnosis']}\n💭 <b>Зачем пришёл/ла:</b> {prof['reason']}\n🎨 <b>Хобби:</b> {prof['hobby']}"

    kb_list = [
        [InlineKeyboardButton("❤️ Лайк", callback_data=f"like:{prof['tg_id']}"),
         InlineKeyboardButton("➡️ Пропустить", callback_data=f"skip:{offset+1}:{prof['tg_id']}")],
        [InlineKeyboardButton("⬅️ В меню", callback_data="menu:back")]
    ]
    if u["prev_anketa_id"] and has_prev:
        kb_list.insert(1, [InlineKeyboardButton("⏪ Вернуть пропущенную", callback_data=f"prev:{u['prev_anketa_id']}")])

    kb = InlineKeyboardMarkup(kb_list)

    if prof["photo1"]:
        await query.message.reply_photo(prof["photo1"], caption=text, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        await query.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    try:
        await query.message.delete()
    except:
        pass

def translate_dop(dop: str) -> str:
    dop = dop.lower().strip()
    mapping = {
        "символы": "chars600", "символы600": "chars600", "chars600": "chars600",
        "лайки": "unlimited", "безлимит": "unlimited", "unlimited": "unlimited",
        "возврат": "prev", "prev": "prev",
        "фото": "second", "second": "second",
        "инкогнито": "incognito", "incognito": "incognito",
        "все": "all", "all": "all"
    }
    return mapping.get(dop, dop)

async def _grant_dop(tid: int, dop: str, days: int):
    dop = translate_dop(dop)
    if dop == "chars600":
        await upsert_user(tid, chars_limit=600)
    elif dop == "unlimited":
        await upsert_user(tid, likes_left=999999)
    elif dop == "prev":
        await upsert_user(tid, has_prev_addon=1)
    elif dop == "second":
        await upsert_user(tid, photos_used=0)
    elif dop == "incognito":
        until = (datetime.utcnow() + timedelta(days=max(days, 1))).isoformat()
        await upsert_user(tid, incognito_until=until)

async def _take_dop(tid: int, dop: str):
    dop = translate_dop(dop)
    if dop == "chars600":
        await upsert_user(tid, chars_limit=300)
    elif dop == "unlimited":
        await upsert_user(tid, likes_left=20)
    elif dop == "prev":
        await upsert_user(tid, has_prev_addon=0, prev_anketa_id=None)
    elif dop == "second":
        await upsert_user(tid, photos_used=0)
    elif dop == "incognito":
        await upsert_user(tid, incognito_until=None)
    elif dop == "all":
        await upsert_user(tid, chars_limit=300, likes_left=20, has_prev_addon=0, prev_anketa_id=None, incognito_until=None)

async def handle_admin_input(msg, ctx, state_row):
    action = state_row["action"]
    tid = state_row["target_id"]
    text = msg.text.strip()

    await db_exec("DELETE FROM admin_states WHERE tg_id=?", (msg.from_user.id,))

    if action == "rework":
        await db_exec("UPDATE profiles SET approved=0 WHERE tg_id=?", (tid,))
        await db_exec("UPDATE users SET status='awaiting_form' WHERE tg_id=?", (tid,))
        await safe(ctx.bot.send_message, tid, f"✏️ <b>Твою анкету нужно переделать.</b>\n\n💬 <b>Комментарий модератора:</b>\n{text}\n\nОтправь /start, чтобы заполнить анкету заново.", parse_mode=ParseMode.HTML)
        await msg.reply_text("✅ Комментарий отправлен.")

    elif action == "ban":
        await upsert_user(tid, banned=1, ban_reason=text, status="banned")
        await safe(ctx.bot.send_message, tid, f"🚫 <b>Доступ закрыт.</b>\n\nПричина: {text}", parse_mode=ParseMode.HTML)
        await msg.reply_text("🚫 Пользователь забанен.")

    elif action == "delprof":
        await db_exec("DELETE FROM profiles WHERE tg_id=?", (tid,))
        await db_exec("UPDATE users SET status='rejected' WHERE tg_id=?", (tid,))
        await safe(ctx.bot.send_message, tid, f"🗑 <b>Твоя анкета удалена модератором.</b>\n\nПричина: {text}", parse_mode=ParseMode.HTML)
        await msg.reply_text("🗑 Анкета удалена.")

    elif action == "vip":
        try:
            lvl, days = map(int, text.split())
            until = (datetime.utcnow() + timedelta(days=days)).isoformat()
            lim = VIP_LIMITS.get(lvl, VIP_LIMITS[1])
            await upsert_user(tid, vip_level=lvl, vip_until=until, chars_limit=lim["chars"], likes_left=lim["likes"])
            await safe(ctx.bot.send_message, tid, f"👑 <b>Тебе выдан VIP {lvl} на {days} дней!</b> 🍁", parse_mode=ParseMode.HTML)
            await msg.reply_text("✅ VIP выдан.")
        except:
            await msg.reply_text("❌ Ошибка формата.")

    elif action == "dop":
        parts = text.split()
        if len(parts) != 2: return await msg.reply_text("❌ Формат: доп дней")
        dop, days = parts[0], int(parts[1])
        await _grant_dop(tid, dop, days)
        await safe(ctx.bot.send_message, tid, f"🎁 <b>Тебе выдан доп: {dop}</b> 🍁", parse_mode=ParseMode.HTML)
        await msg.reply_text("✅ Выдано.")

    elif action == "takedop":
        await _take_dop(tid, text.strip())
        await safe(ctx.bot.send_message, tid, "⚠️ Один из твоих допов был снят администратором.")
        await msg.reply_text("✅ Снято.")

    elif action == "givebyid":
        parts = text.split()
        if len(parts) < 2: return await msg.reply_text("❌ Формат: <tg_id> <тип> ...")
        try:
            target = int(parts[0])
        except:
            return await msg.reply_text("❌ tg_id должен быть числом.")
        kind = parts[1].lower()
        target_user = await get_user(target)
        if not target_user:
            return await msg.reply_text("❌ Пользователь не найден в базе.")

        if kind in ("вип", "vip"):
            try:
                lvl_str = parts[2].lower()
                if lvl_str in ("низкий", "1"): lvl = 1
                elif lvl_str in ("средний", "2"): lvl = 2
                elif lvl_str in ("высокий", "3"): lvl = 3
                else: lvl = int(lvl_str)
                days = int(parts[3])
            except:
                return await msg.reply_text("❌ Формат: <tg_id> вип <уровень> <дней>")
            until = (datetime.utcnow() + timedelta(days=days)).isoformat()
            lim = VIP_LIMITS.get(lvl, VIP_LIMITS[1])
            await upsert_user(target, vip_level=lvl, vip_until=until, chars_limit=lim["chars"], likes_left=lim["likes"])
            await safe(ctx.bot.send_message, target, f"👑 <b>Тебе выдан VIP {lvl} на {days} дней!</b> 🍁", parse_mode=ParseMode.HTML)
            await msg.reply_text(f"✅ VIP {lvl} на {days} дн. выдан {target}")

        elif kind in ("доп", "dop"):
            if len(parts) < 3: return await msg.reply_text("❌ Формат: <tg_id> доп <тип> [дней]")
            dop = parts[2].lower()
            days = int(parts[3]) if len(parts) > 3 else 0
            await _grant_dop(target, dop, days)
            await safe(ctx.bot.send_message, target, f"🎁 <b>Тебе выдан доп: {dop}</b> 🍁", parse_mode=ParseMode.HTML)
            await msg.reply_text(f"✅ Доп {dop} выдан {target}")
        else:
            await msg.reply_text("❌ Тип должен быть 'вип' или 'доп'.")

    elif action == "takebyid":
        parts = text.split()
        if len(parts) != 2: return await msg.reply_text("❌ Формат: <tg_id> <тип>")
        try:
            target = int(parts[0])
        except:
            return await msg.reply_text("❌ tg_id должен быть числом.")
        kind = parts[1].lower()
        target_user = await get_user(target)
        if not target_user:
            return await msg.reply_text("❌ Пользователь не найден.")

        if kind in ("вип", "vip"):
            await upsert_user(target, vip_level=0, vip_until=None, chars_limit=300, likes_left=20)
            await safe(ctx.bot.send_message, target, "⚠️ Твой VIP был снят администратором.")
            await msg.reply_text(f"✅ VIP снят у {target}")
        else:
            await _take_dop(target, kind)
            await safe(ctx.bot.send_message, target, "⚠️ Один из твоих допов был снят администратором.")
            await msg.reply_text(f"✅ Доп {kind} снят у {target}")

async def post_init(app: Application):
    await app.bot.set_my_commands([])
    await app.bot.delete_my_commands()
    await db_init()

def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("admin", cmd_admin))

    app.add_handler(MessageHandler(filters.Chat(ADMIN_CHAT_ID) & filters.REPLY & filters.TEXT, on_admin_reply))
    
    app.add_handler(MessageHandler(filters.CONTACT, on_contact))
    app.add_handler(MessageHandler(filters.VIDEO_NOTE, on_video))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, on_text))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND & ~filters.CONTACT & ~filters.VIDEO_NOTE & ~filters.TEXT, on_media)) 
    
    app.add_handler(CallbackQueryHandler(on_callback))

    log.info("Бот Кленчик запущен на PTB.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
