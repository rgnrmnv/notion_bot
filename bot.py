import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from notion_client import Client
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes
)

# ------------ ENV ------------
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "300"))

# Имена свойств в базе
PROP_TITLE_CANDIDATES = ["Название", "Name"]   # заголовок записи
PROP_GROUP = "Группа"                           # Select
PROP_STATUS = "Статус"                          # Select
TRIGGER_STATUSES = {"Заканчивается", "ЗАКОНЧИЛОСЬ"}

notion = Client(auth=NOTION_TOKEN)

DB_PATH = "state.db"


# ------------ SQLite ------------
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

def get_subscribers() -> List[int]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT chat_id FROM users")
        return [r[0] for r in cur.fetchall()]

def get_last_status(page_id: str) -> Optional[str]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT last_status FROM page_status WHERE page_id=?", (page_id,))
        r = cur.fetchone()
    return r[0] if r else None

def upsert_status(page_id: str, status: Optional[str]):
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

def get_meta(key: str) -> Optional[str]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT value FROM meta WHERE key=?", (key,))
        r = cur.fetchone()
    return r[0] if r else None


# ------------ Notion helpers ------------
def get_db_meta(database_id: str) -> dict:
    return notion.databases.retrieve(database_id=database_id)

def get_title_prop_name(db_meta: dict) -> str:
    props = db_meta["properties"]
    for candidate in PROP_TITLE_CANDIDATES:
        if candidate in props and props[candidate]["type"] == "title":
            return candidate
    for name, meta in props.items():
        if meta["type"] == "title":
            return name
    raise RuntimeError("Не найдено title-свойство (title) в базе.")

def get_group_options(db_meta: dict) -> List[str]:
    prop = db_meta["properties"].get(PROP_GROUP)
    if not prop:
        return []
    t = prop["type"]
    if t == "select":
        return [o["name"] for o in prop["select"]["options"]]
    elif t == "multi_select":
        return [o["name"] for o in prop["multi_select"]["options"]]
    return []

def extract_title(page: dict, title_prop: str) -> str:
    try:
        items = page["properties"][title_prop]["title"]
        if items:
            return items[0]["plain_text"]
    except Exception:
        pass
    return "(без названия)"

def extract_status(page: dict) -> Optional[str]:
    try:
        prop = page["properties"][PROP_STATUS]
        if prop["type"] == "select" and prop["select"]:
            return prop["select"]["name"]
    except Exception:
        pass
    return None

def query_all(database_id: str) -> List[dict]:
    pages, start_cursor = [], None
    while True:
        resp = notion.databases.query(
            database_id=database_id, start_cursor=start_cursor, page_size=100
        )
        pages.extend(resp["results"])
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return pages

def query_by_group(database_id: str, group_name: str) -> List[dict]:
    pages, start_cursor = [], None
    flt = {"property": PROP_GROUP, "select": {"equals": group_name}}
    while True:
        resp = notion.databases.query(
            database_id=database_id,
            filter=flt,
            start_cursor=start_cursor,
            page_size=100
        )
        pages.extend(resp["results"])
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return pages

def query_since(database_id: str, since_iso: str) -> List[dict]:
    """Все страницы, отредактированные с момента since_iso (UTC)."""
    pages, start_cursor = [], None
    filter_by_time = {
        "timestamp": "last_edited_time",
        "last_edited_time": {"on_or_after": since_iso}
    }
    while True:
        resp = notion.databases.query(
            database_id=database_id,
            filter=filter_by_time,
            start_cursor=start_cursor,
            page_size=100
        )
        pages.extend(resp["results"])
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return pages


# ------------ Telegram handlers ------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    init_db()
    register_user(update.effective_chat.id)

    db_meta = get_db_meta(NOTION_DATABASE_ID)
    title_prop = get_title_prop_name(db_meta)
    groups = get_group_options(db_meta)

    # Кнопки: "Все" + группы
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
        "Выбери группу или покажи всю таблицу:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def cb_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    db_meta = get_db_meta(NOTION_DATABASE_ID)
    title_prop = get_title_prop_name(db_meta)
    pages = query_all(NOTION_DATABASE_ID)

    if not pages:
        await context.bot.send_message(chat_id, "В базе пусто.")
        await update.callback_query.answer()
        return

    lines = []
    for p in pages:
        title = extract_title(p, title_prop)
        status = extract_status(p) or "—"
        url = p.get("url", "")
        lines.append(f"• <b>{title}</b>\nСтатус: {status}\n{url}")

    await context.bot.send_message(chat_id, "\n".join(lines[:50]), parse_mode="HTML")
    await update.callback_query.answer()

async def cb_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    group_name = update.callback_query.data.split(":", 1)[1]

    db_meta = get_db_meta(NOTION_DATABASE_ID)
    title_prop = get_title_prop_name(db_meta)
    pages = query_by_group(NOTION_DATABASE_ID, group_name)

    if not pages:
        await context.bot.send_message(chat_id, f"Для «{group_name}» записей не найдено.")
        await update.callback_query.answer()
        return

    lines = []
    for p in pages:
        title = extract_title(p, title_prop)
        status = extract_status(p) or "—"
        url = p.get("url", "")
        lines.append(f"• <b>{title}</b>\nСтатус: {status}\n{url}")

    await context.bot.send_message(chat_id, "\n".join(lines[:50]), parse_mode="HTML")
    await update.callback_query.answer()

async def check_updates(context: ContextTypes.DEFAULT_TYPE):
    """Периодически: находим страницы, которые изменились с прошлого раза.
       Если статус перешёл в один из триггерных — шлём уведомление всем подписчикам.
    """
    # якорь времени
    last_checked = get_meta("last_checked_iso")
    if not last_checked:
        # стартуем «чуть в прошлое», чтобы подобрать старые изменения
        last_checked_dt = datetime.now(timezone.utc) - timedelta(days=1)
        last_checked = last_checked_dt.isoformat()
        set_meta("last_checked_iso", last_checked)

    try:
        changed_pages = query_since(NOTION_DATABASE_ID, last_checked)
        db_meta = get_db_meta(NOTION_DATABASE_ID)
        title_prop = get_title_prop_name(db_meta)

        now_iso = datetime.now(timezone.utc).isoformat()

        for p in changed_pages:
            page_id = p["id"]
            current_status = extract_status(p)
            prev_status = get_last_status(page_id)
            upsert_status(page_id, current_status)

            became_trigger = current_status in TRIGGER_STATUSES and current_status != prev_status
            if became_trigger:
                title = extract_title(p, title_prop)
                url = p.get("url", "")
                text = f"⚠️ <b>{title}</b>\nСтатус: {current_status}\n{url}"
                for chat_id in get_subscribers():
                    try:
                        await context.bot.send_message(chat_id, text, parse_mode="HTML")
                    except Exception:
                        pass

        set_meta("last_checked_iso", now_iso)

    except Exception as e:
        # в лог + молча продолжим в следующий тик
        print("check_updates error:", e)


def main():
    init_db()
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Команды/колбэки
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(cb_all, pattern="^all$"))
    app.add_handler(CallbackQueryHandler(cb_group, pattern="^group:"))

    # Периодическая проверка Notion через JobQueue (официальная механика PTB) :contentReference[oaicite:5]{index=5}
    app.job_queue.run_repeating(check_updates, interval=POLL_INTERVAL_SECONDS, first=5)

    app.run_polling()


if __name__ == "__main__":
    main()
