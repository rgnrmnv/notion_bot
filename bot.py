import os
import logging
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
import requests
from dotenv import load_dotenv

load_dotenv()

# Настройки из переменных окружения
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Глобальное хранилище данных и подписчиков
last_known_items = {}
SUBSCRIBERS_FILE = "subscribers.txt"

# Заголовки для Notion API
headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# Получить данные из Notion
def get_notion_data():
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    try:
        response = requests.post(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print("Notion API error:", response.text)
            return []
        data = response.json()
        items = []
        for row in data.get("results", []):
            props = row["properties"]
            group = props.get("Группа", {}).get("select", {}).get("name", "Без группы")
            status = props.get("Статус", {}).get("select", {}).get("name", "Без статуса")
            name = props.get("Name", {}).get("title", [{}])[0].get("plain_text", "Без названия")
            items.append({
                "id": row["id"],
                "name": name,
                "group": group,
                "status": status
            })
        return items
    except Exception as e:
        print("Ошибка при получении данных из Notion:", e)
        return []

# Сохранить chat_id подписчика
def save_subscriber(chat_id):
    try:
        with open(SUBSCRIBERS_FILE, "r") as f:
            existing = set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        existing = set()
    
    if str(chat_id) not in existing:
        with open(SUBSCRIBERS_FILE, "a") as f:
            f.write(f"{chat_id}\n")

# Форматирование списка
def format_items(items):
    if not items:
        return "Ничего не найдено."
    text = ""
    for item in items:
        text += f"🔹 {item['name']}\n   Группа: {item['group']}\n   Статус: {item['status']}\n\n"
    return text.strip()

# /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    save_subscriber(chat_id)
    keyboard = [["Здоровье", "Работа", "Учёба", "Всё"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text(
        "✅ Ты подписан на уведомления!\nВыбери группу или 'Всё':",
        reply_markup=reply_markup
    )

# Обработка сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = update.message.text.strip()
    items = get_notion_data()
    
    if user_text == "Всё":
        filtered = items
    else:
        filtered = [item for item in items if item["group"] == user_text]
    
    msg = format_items(filtered)
    await update.message.reply_text(msg or "Нет записей в этой группе.")

# Проверка обновлений
async def check_for_updates(context: ContextTypes.DEFAULT_TYPE):
    global last_known_items
    items = get_notion_data()
    current_items = {item["id"]: item for item in items}

    # Загрузить всех подписчиков
    try:
        with open(SUBSCRIBERS_FILE, "r") as f:
            chat_ids = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        chat_ids = []

    for item_id, item in current_items.items():
        if item_id in last_known_items:
            old_status = last_known_items[item_id]["status"]
            new_status = item["status"]
            if old_status != new_status and new_status in ["Заканчивается", "Закончилось"]:
                for chat_id in chat_ids:
                    try:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=f"⚠️ Обновление статуса!\n\n{item['name']}\nСтатус: {new_status}"
                        )
                    except Exception as e:
                        print(f"Не отправлено {chat_id}: {e}")
        last_known_items[item_id] = item

# Запуск
def main():
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Проверка каждые 2 минуты (120 секунд)
    application.job_queue.run_repeating(check_for_updates, interval=120, first=10)

    application.run_polling()

if __name__ == "__main__":
    main()