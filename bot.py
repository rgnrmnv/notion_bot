import os
import logging
import asyncio
import sqlite3
from contextlib import closing
from datetime import datetime, timezone, timedelta

from aiohttp import web
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from notion_client import Client
from notion_client import APIResponseError

# ===== Логирование =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# ===== Конфигурация =====
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "300"))

PROP_TITLE_CANDIDATES = ["Название", "Name"]
PROP_GROUP = "Группа"
PROP_STATUS = "Статус"
TRIGGER_STATUSES = {"Заканчивается", "ZAKONCHILОСЬ", "ЗАКОНЧИЛОСЬ"}  # Учти точное написание

# ===== Notion клиент с указанием версии API =====
notion = Client(auth=NOTION_TOKEN, notion_version="2025-09-03")

# ===== SQLite хранилище =====
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
    logger.info("База данных инициализирована")

def register_user(chat_id: int):
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        conn.execute("INSERT OR IGNORE INTO users(chat_id) VALUES (?)", (chat_id,))

def get_subscribers():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT chat_id FROM users")
        return [row[0] for row in cur.fetchall()]

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
        row = cur.fetchone()
    return row[0] if row else None

# ===== Notion вспомогательные =====
def get_db_meta():
    return notion.databases.retrieve(database_id=NOTION_DATABASE_ID)

def get_title_prop_name(db_meta):
    props = db_meta["properties"]
    for cand in PROP_TITLE_CANDIDATES:
        if cand in props and props[cand]["type"] == "title":
            return cand
    for name, meta in props.items():
        if meta["type"] == "title":
            return name
    raise RuntimeError("Не найдено title-свойство")

def get_group_options(db_meta):
    prop = db_meta["properties"].get(PROP_GROUP)
    if not prop:
        return []
    t = prop["type"]
    if t == "select":
        return [opt["name"] for opt in prop["select"]["options"]]
    if t == "multi_select":
        return [opt["name"] for opt in prop["multi_select"]["options"]]
    return []

def extract_title(page, title_prop):
    try:
        items = page["properties"][title_prop]["title"]
        if items:
            return items[0]["plain_text"]
    except Exception as e:
        logger.warning("Ошибка extract_title: %s", e)
    return "(без названия)"

def extract_status(page):
    try:
        prop = page["properties"][PROP_STATUS]
        if prop["type"] == "select" and prop["select"]:
            return prop["select"]["name"]
    except Exception as e:
        logger.warning("Ошибка extract_status: %s", e)
    return None

async def query_all():
    kwargs = {}
    # Если база требует указания data_source_id — можно тут подставить, если знаешь
    pages = []
    start_cursor = None
    while True:
        resp = notion.databases.query(
            database_id=NOTION_DATABASE_ID,
            **kwargs,
            start_cursor=start_cursor,
            page_size=100
        )
        pages.extend(resp["results"])
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return pages

async def query_by_group(group_name: str):
    kwargs = {"filter": {"property": PROP_GROUP, "select": {"equals": group_name}}}
    pages = []
    start_cursor = None
    while True:
        resp = notion.databases.query(
            database_id=NOTION_DATABASE_ID,
            **kwargs,
            start_cursor=start_cursor,
            page_size=100
        )
        pages.extend(resp["results"])
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return pages

async def query_since(since_iso: str):
    kwargs = {"filter": {
        "timestamp": "last_edited_time",
        "last_edited_time": {"on_or_after": since_iso}
    }}
    pages = []
    start_cursor = None
    while True:
        resp = notion.databases.query(
            database_id=NOTION_DATABASE_ID,
            **kwargs,
            start_cursor=start_cursor,
            page_size=100
        )
        pages.extend(resp["results"])
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return pages

# ===== Telegram хендлеры =====
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_chat.id)
    logger.info("Пользователь %s нажал /start", update.effective_chat.id)

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

async def cb_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    logger.info("Пользователь %s запросил Все", chat_id)

    db_meta = get_db_meta()
    title_prop = get_title_prop_name(db_meta)
    pages = await query_all()

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

async def cb_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    group_name = update.callback_query.data.split(":", 1)[1]
    logger.info("Пользователь %s выбрал группу %s", chat_id, group_name)

    db_meta = get_db_meta()
    title_prop = get_title_prop_name(db_meta)
    pages = await query_by_group(group_name)

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

# ===== Фоновая задача проверки =====
async def check_loop(app_bot: Application):
    last_checked = get_meta("last_checked_iso")
    if not last_checked:
        dt0 = datetime.now(timezone.utc) - timedelta(days=1)
        last_checked = dt0.isoformat()
        set_meta("last_checked_iso", last_checked)

    while True:
        try:
            changed = await query_since(last_checked)
            db_meta = get_db_meta()
            title_prop = get_title_prop_name(db_meta)

            now_iso = datetime.now(timezone.utc).isoformat()
            for p in changed:
                page_id = p["id"]
                curr = extract_status(p)
                prev = get_last_status(page_id)
                upsert_status(page_id, curr)

                if curr is not None and curr in TRIGGER_STATUSES and curr != prev:
                    title = extract_title(p, title_prop)
                    url = p.get("url", "")
                    text = f"⚠️ <b>{title}</b>\nСтатус: {curr}\n{url}"
                    subs = get_subscribers()
                    logger.info("Уведомление: %s → %s, подписчики: %s", title, curr, subs)
                    for cid in subs:
                        try:
                            await app_bot.bot.send_message(cid, text, parse_mode="HTML")
                        except Exception as e:
                            logger.error("Ошибка отправки уведомления %s: %s", cid, e)

            set_meta("last_checked_iso", now_iso)
        except APIResponseError as er:
            logger.error("Notion API ошибка: %s", er)
        except Exception as e:
            logger.error("Ошибка в check_loop: %s", e)
        await asyncio.sleep(POLL_INTERVAL)

# ===== HTTP через aiohttp =====
async def health(request):
    return web.Response(text="OK")

async def run_app():
    init_db()

    app_bot = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app_bot.add_handler(CommandHandler("start", start_handler))
    app_bot.add_handler(CallbackQueryHandler(cb_all, pattern="^all$"))
    app_bot.add_handler(CallbackQueryHandler(cb_group, pattern="^group:"))

    await app_bot.initialize()
    await app_bot.start()
    logger.info("Telegram бот запущен")

    asyncio.create_task(check_loop(app_bot))
    logger.info("Запущен check_loop task")

    aio = web.Application()
    aio.add_routes([web.get("/", health)])
    runner = web.AppRunner(aio)
    await runner.setup()
    port = int(os.environ.get("PORT", 8000))
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    logger.info("HTTP сервер запущен на порту %s", port)

    # Ждём вечно, чтобы приложение не завершилось
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(run_app())
