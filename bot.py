import os
import threading
import time
import sqlite3
from contextlib import closing
from datetime import datetime, timezone, timedelta

from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from notion_client import Client

# ——— Конфигурация ———
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "300"))

PROP_TITLE_CANDIDATES = ["Название", "Name"]
PROP_GROUP = "Группа"
PROP_STATUS = "Статус"
TRIGGER_STATUSES = {"Заканчивается", "ЗАКОНЧИЛОСЬ"}

# ——— Инициализация ———
notion = Client(auth=NOTION_TOKEN)
app = Flask(__name__)

DB_PATH = "state.db"

def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS page_status (
                page_id TEXT PRIMARY KEY,
                last_status TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

def register_user(chat_id: int):
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        conn.execute("INSERT OR IGNORE INTO users(chat_id) VALUES (?)", (chat_id,))

def get_subscribers():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT chat_id FROM users")
        return [r[0] for r in cur.fetchall()]

def get_last_status(page_id: str):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT last_status FROM page_status WHERE page_id=?", (page_id,))
        row = cur.fetchone()
    return row[0] if row else None

def upsert_status(page_id: str, status: str):
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        conn.execute("""
            INSERT INTO page_status(page_id, last_status)
            VALUES (?, ?)
            ON CONFLICT(page_id) DO UPDATE SET last_status=excluded.last_status
        """, (page_id, status))

def set_meta(key: str, value: str):
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        conn.execute("""
            INSERT INTO meta(key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """, (key, value))

def get_meta(key: str):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT value FROM meta WHERE key=?", (key,))
        r = cur.fetchone()
    return r[0] if r else None

# ——— Notion API вспомогательные ———
def get_db_meta():
    return notion.databases.retrieve(database_id=NOTION_DATABASE_ID)

def get_title_prop_name(db_meta):
    props = db_meta["properties"]
    for c in PROP_TITLE_CANDIDATES:
        if c in props and props[c]["type"] == "title":
            return c
    for name, meta in props.items():
        if meta["type"] == "title":
            return name
    raise RuntimeError("Не найдено title-поле")

def get_group_options(db_meta):
    prop = db_meta["properties"].get(PROP_GROUP)
    if not prop:
        return []
    t = prop["type"]
    if t == "select":
        return [o["name"] for o in prop["select"]["options"]]
    if t == "multi_select":
        return [o["name"] for o in prop["multi_select"]["options"]]
    return []

def extract_title(page, title_prop):
    try:
        items = page["properties"][title_prop]["title"]
        if items:
            return items[0]["plain_text"]
    except:
        pass
    return "(без названия)"

def extract_status(page):
    try:
        prop = page["properties"][PROP_STATUS]
        if prop["type"] == "select" and prop["select"]:
            return prop["select"]["name"]
    except:
        pass
    return None

def query_all():
    pages = []
    start_cursor = None
    while True:
        resp = notion.databases.query(
            database_id=NOTION_DATABASE_ID,
            start_cursor=start_cursor,
            page_size=100
        )
        pages.extend(resp["results"])
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return pages

def query_by_group(group_name: str):
    pages = []
    start_cursor = None
    flt = {"property": PROP_GROUP, "select": {"equals": group_name}}
    while True:
        resp = notion.databases.query(
            database_id=NOTION_DATABASE_ID,
            filter=flt,
            start_cursor=start_cursor,
            page_size=100
        )
        pages.extend(resp["results"])
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return pages

def query_since(since_iso: str):
    pages = []
    start_cursor = None
    time_filter = {
        "timestamp": "last_edited_time",
        "last_edited_time": {"on_or_after": since_iso}
    }
    while True:
        resp = notion.databases.query(
            database_id=NOTION_DATABASE_ID,
            filter=time_filter,
            start_cursor=start_cursor,
            page_size=100
        )
        pages.extend(resp["results"])
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return pages

# ——— Telegram части ———
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    init_db()
    register_user(update.effective_chat.id)

    db_meta = get_db_meta()
    title_prop = get_title_prop_name(db_meta)
    groups = get_group_options(db_meta)

    keyboard = [[InlineKeyboardButton("Все", callback_data="all")]]
    row = []
    for g in groups:
        row.append(InlineKeyboardButton(g, callback_data=f"group:{g}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    await update.message.reply_text(
        "Выберите группу или «Все»:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def callback_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    db_meta = get_db_meta()
    title_prop = get_title_prop_name(db_meta)
    pages = query_all()

    if not pages:
        await context.bot.send_message(chat_id, "В базе нет записей.")
        await update.callback_query.answer()
        return

    lines = []
    for p in pages:
        title = extract_title(p, title_prop)
        status = extract_status(p) or "—"
        url = p.get("url", "")
        lines.append(f"• <b>{title}</b>\nСтатус: {status}\n{url}")

    text = "\n\n".join(lines)
    await context.bot.send_message(chat_id, text, parse_mode="HTML")
    await update.callback_query.answer()

async def callback_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    group_name = update.callback_query.data.split(":", 1)[1]
    db_meta = get_db_meta()
    title_prop = get_title_prop_name(db_meta)
    pages = query_by_group(group_name)

    if not pages:
        await context.bot.send_message(chat_id, f"Для группы «{group_name}» нет записей.")
        await update.callback_query.answer()
        return

    lines = []
    for p in pages:
        title = extract_title(p, title_prop)
        status = extract_status(p) or "—"
        url = p.get("url", "")
        lines.append(f"• <b>{title}</b>\nСтатус: {status}\n{url}")

    text = "\n\n".join(lines)
    await context.bot.send_message(chat_id, text, parse_mode="HTML")
    await update.callback_query.answer()

async def periodic_check(context: ContextTypes.DEFAULT_TYPE):
    last_checked = get_meta("last_checked_iso")
    if not last_checked:
        dt = datetime.now(timezone.utc) - timedelta(days=1)
        last_checked = dt.isoformat()
        set_meta("last_checked_iso", last_checked)

    try:
        changed = query_since(last_checked)
        db_meta = get_db_meta()
        title_prop = get_title_prop_name(db_meta)

        now_iso = datetime.now(timezone.utc).isoformat()

        for p in changed:
            page_id = p["id"]
            curr = extract_status(p)
            prev = get_last_status(page_id)
            upsert_status(page_id, curr)

            if curr in TRIGGER_STATUSES and curr != prev:
                title = extract_title(p, title_prop)
                url = p.get("url", "")
                text = f"⚠️ <b>{title}</b>\nСтатус: {curr}\n{url}"
                for cid in get_subscribers():
                    try:
                        await context.bot.send_message(cid, text, parse_mode="HTML")
                    except:
                        pass

        set_meta("last_checked_iso", now_iso)
    except Exception as e:
        print("Ошибка в periodic_check:", e)

def start_polling_bot():
    """Запуск бота через Application (async) — внутри потока."""
    app_bot = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app_bot.add_handler(CommandHandler("start", start_command))
    app_bot.add_handler(CallbackQueryHandler(callback_all, pattern="^all$"))
    app_bot.add_handler(CallbackQueryHandler(callback_group, pattern="^group:"))

    # JobQueue — встроенный планировщик
    app_bot.job_queue.run_repeating(periodic_check, interval=POLL_INTERVAL_SECONDS, first=5)

    app_bot.run_polling()

# ——— Flask маршрут, чтобы Render считал приложение живым ———
@app.route("/")
def health_check():
    return "OK", 200

def bot_thread_func():
    start_polling_bot()

if __name__ == "__main__":
    init_db()

    # Запускаем Telegram-бота в отдельном потоке
    t = threading.Thread(target=bot_thread_func, daemon=True)
    t.start()

    # Запускаем веб-сервис (Flask) для поддержания HTTP
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
