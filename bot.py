import asyncio
import json
import logging
import os
import random
import re
import secrets
import sqlite3
import time
from copy import copy
from datetime import datetime
from pathlib import Path
from shutil import copy2
from urllib.parse import urljoin

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import FSInputFile, KeyboardButton, Message, ReplyKeyboardMarkup
from bs4 import BeautifulSoup
from dotenv import load_dotenv


load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = 7541282558
BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "bot.db"
USERS_FILE = BASE_DIR / "users.json"
WELCOME_IMAGE = BASE_DIR / "Dobro.png"
ORDER_IMAGE = BASE_DIR / "strana.png"
TARIFF_IMAGE = BASE_DIR / "tarif.png"
SUPPORT_IMAGE = BASE_DIR / "teh.png"
INFO_IMAGE = BASE_DIR / "info.png"
CARGO_TEMPLATE_XLSX = BASE_DIR / "каргос.xlsx"
CARGO_ORDERS_DIR = BASE_DIR / "cargo_orders"
CARGO_PHOTO_DIR = BASE_DIR / "cargo_photos"
CARGO_SHEET_NAME = "Лист1"
CARGO_START_ROW = 6
CARGO_NAME_COLUMN = "C"
CARGO_LINK_COLUMN = "D"
CARGO_DETAILS_COLUMN = "E"
CARGO_PRICE_COLUMN = "F"
CARGO_QUANTITY_COLUMN = "G"
CARGO_TOTAL_COLUMN = "H"
CARGO_DELIVERY_COLUMN = "I"
CARGO_PHOTO_COLUMN = "J"
CARGO_IMAGE_WIDTH = 95
CARGO_IMAGE_HEIGHT = 95
ADMIN_FUN_QUOTES = [
    "Сегодня бот слушается. Пользуйся моментом.",
    "Админ-режим активирован. Кофе мысленно налит.",
    "Все под контролем. Даже если таблица делает вид, что нет.",
    "Иногда лучший фикс это просто открыть нужную таблицу с первого раза.",
    "Бот работает. Магия и немного Python.",
]
SEARCH_SOURCES = [
    "https://www.goofish.com/",
    "https://mxjstore.x.yupoo.com/albums",
    "https://wwfake100.x.yupoo.com/albums",
    "https://yolo66.x.yupoo.com/albums",
    "https://vx798134596.x.yupoo.com/",
    "https://konng-gonng.x.yupoo.com/",
    "https://goat-official.x.yupoo.com/categories",
    "https://huskyreps.x.yupoo.com/",
    "https://powerball.x.yupoo.com/",
    "https://anniestudio.x.yupoo.com/",
    "https://repsking.x.yupoo.com/",
    "https://loganhere.x.yupoo.com/",
    "https://martinreps.x.yupoo.com/",
    "https://windvane168.x.yupoo.com/",
    "https://lufilostudio.x.yupoo.com/",
    "https://rainbowreps.x.yupoo.com/",
    "https://angelking47.x.yupoo.com/",
    "https://pikachushop.x.yupoo.com/",
    "https://tiger-official.x.yupoo.com/",
    "https://scorpio-reps.x.yupoo.com/",
    "https://thethunder.x.yupoo.com/",
    "https://1to1.x.yupoo.com/",
    "https://madebykungfu.x.yupoo.com/",
    "https://repsunofficial.x.yupoo.com/",
    "https://dragonrep333.x.yupoo.com/",
    "https://scarlettluxury.x.zhidian-inc.cn/",
    "https://noghost.x.zhidian-inc.cn/",
    "https://west42.x.yupoo.com/albums",
    "https://summer-sneaker.x.yupoo.com/albums",
    "https://407900329.x.yupoo.com/albums",
]
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru,en-US;q=0.9,en;q=0.8",
}
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=12)
MAX_RESULTS = 8
MAX_MESSAGES_PER_WINDOW = 8
RATE_WINDOW_SECONDS = 10
RATE_LIMIT_NOTICE_COOLDOWN = 5
TRACKING_STAGES = {
    "1": "Выкуплен",
    "2": "Приехал на склад",
    "3": "Отправлен",
    "4": "Приехал в РФ",
}
ACTIVE_ORDER_STATUSES = {
    "Оформляет заказ",
    "Отправляет данные",
    "Заявка отправлена",
    "Заказ обработан",
}

if not TOKEN:
    raise RuntimeError("Не найден BOT_TOKEN. Добавь токен в файл .env.")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

dp = Dispatcher()
china_order_users: set[int] = set()
admin_search_users: set[int] = set()
paid_search_users: set[int] = set()
tracking_lookup_users: set[int] = set()
admin_cargo_order_states: dict[int, dict] = {}
admin_cargo_active_workbooks: dict[int, str] = {}
admin_message_order_states: dict[int, dict] = {}
search_semaphore = asyncio.Semaphore(3)
user_request_times: dict[int, list[float]] = {}
user_last_limit_notice: dict[int, float] = {}

user_main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Сделать заказ")],
        [KeyboardButton(text="Тарифы")],
        [KeyboardButton(text="Инфо")],
        [KeyboardButton(text="Тех. поддержка")],
    ],
    resize_keyboard=True,
    input_field_placeholder="Напиши сообщение или выбери кнопку",
)

admin_main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Сделать заказ")],
        [KeyboardButton(text="Оформить заказ")],
        [KeyboardButton(text="Найти эту вещь в Китае")],
        [KeyboardButton(text="Неодобренные заказы")],
        [KeyboardButton(text="Все заказы и их статусы")],
        [KeyboardButton(text="Тарифы")],
        [KeyboardButton(text="Инфо")],
        [KeyboardButton(text="Тех. поддержка")],
    ],
    resize_keyboard=True,
    input_field_placeholder="Напиши сообщение или выбери кнопку",
)

user_order_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Китай")],
        [KeyboardButton(text="Найти эту вещь в Китае")],
        [KeyboardButton(text="Сша")],
        [KeyboardButton(text="Корея")],
        [KeyboardButton(text="Назад")],
    ],
    resize_keyboard=True,
    input_field_placeholder="Выбери раздел",
)

china_submit_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Я все скинул")],
        [KeyboardButton(text="Назад")],
    ],
    resize_keyboard=True,
    input_field_placeholder="Отправь данные по заказу или нажми кнопку ниже",
)

info_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Мой заказ")],
        [KeyboardButton(text="Мои попытки поиска")],
        [KeyboardButton(text="Узнать статус заказа")],
        [KeyboardButton(text="Назад")],
    ],
    resize_keyboard=True,
    input_field_placeholder="Выбери действие",
)

admin_cargo_order_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Назад")],
    ],
    resize_keyboard=True,
    input_field_placeholder="Отправь данные для таблицы или нажми 'Назад'",
)


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_storage_dirs() -> None:
    CARGO_ORDERS_DIR.mkdir(parents=True, exist_ok=True)
    CARGO_PHOTO_DIR.mkdir(parents=True, exist_ok=True)


def get_db() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_FILE)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    with get_db() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                first_name TEXT NOT NULL DEFAULT '',
                last_name TEXT NOT NULL DEFAULT '',
                username TEXT NOT NULL DEFAULT '',
                first_seen TEXT NOT NULL DEFAULT '',
                last_seen TEXT NOT NULL DEFAULT '',
                country TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                note TEXT NOT NULL DEFAULT '',
                search_access INTEGER NOT NULL DEFAULT 0,
                order_number INTEGER NOT NULL DEFAULT 0,
                tracking_code TEXT NOT NULL DEFAULT '',
                tracking_stage TEXT NOT NULL DEFAULT '',
                banned INTEGER NOT NULL DEFAULT 0,
                ban_reason TEXT NOT NULL DEFAULT ''
            )
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_users_tracking_code
            ON users(tracking_code)
            WHERE tracking_code <> ''
            """
        )
        columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(users)").fetchall()
        }
        if "search_access" not in columns:
            connection.execute(
                "ALTER TABLE users ADD COLUMN search_access INTEGER NOT NULL DEFAULT 0"
            )
        if "banned" not in columns:
            connection.execute(
                "ALTER TABLE users ADD COLUMN banned INTEGER NOT NULL DEFAULT 0"
            )
        if "ban_reason" not in columns:
            connection.execute(
                "ALTER TABLE users ADD COLUMN ban_reason TEXT NOT NULL DEFAULT ''"
            )


def get_setting(key: str, default: str = "") -> str:
    with get_db() as connection:
        row = connection.execute(
            "SELECT value FROM settings WHERE key = ?",
            (key,),
        ).fetchone()
    if not row:
        return default
    return row["value"] or default


def set_setting(key: str, value: str) -> None:
    with get_db() as connection:
        connection.execute(
            """
            INSERT INTO settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )


def is_maintenance_mode() -> bool:
    return get_setting("maintenance_mode", "0") == "1"


def set_maintenance_mode(enabled: bool) -> None:
    set_setting("maintenance_mode", "1" if enabled else "0")


def row_to_user(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "first_name": row["first_name"] or "",
        "last_name": row["last_name"] or "",
        "username": row["username"] or "",
        "first_seen": row["first_seen"] or "",
        "last_seen": row["last_seen"] or "",
        "country": row["country"] or "",
        "status": row["status"] or "",
        "note": row["note"] or "",
        "search_access": int(row["search_access"] or 0),
        "order_number": int(row["order_number"] or 0),
        "tracking_code": row["tracking_code"] or "",
        "tracking_stage": row["tracking_stage"] or "",
        "banned": bool(row["banned"] or 0),
        "ban_reason": row["ban_reason"] or "",
    }


def load_legacy_users() -> dict[str, dict]:
    if not USERS_FILE.exists():
        return {}

    try:
        data = json.loads(USERS_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        logging.warning("Не удалось прочитать users.json, пропускаю миграцию.")
        return {}


def migrate_legacy_users() -> None:
    legacy_users = load_legacy_users()
    if not legacy_users:
        return

    with get_db() as connection:
        existing_count = connection.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if existing_count:
            return

        for raw_user in legacy_users.values():
            user_id = int(raw_user.get("id", 0) or 0)
            if not user_id:
                continue

            connection.execute(
                """
                INSERT OR REPLACE INTO users (
                    id, first_name, last_name, username,
                    first_seen, last_seen, country, status,
                    note, search_access, order_number, tracking_code, tracking_stage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    raw_user.get("first_name", "") or "",
                    raw_user.get("last_name", "") or "",
                    raw_user.get("username", "") or "",
                    raw_user.get("first_seen", "") or now_str(),
                    raw_user.get("last_seen", "") or now_str(),
                    raw_user.get("country", "") or "",
                    raw_user.get("status", "Новый") or "Новый",
                    raw_user.get("note", "") or "",
                    int(raw_user.get("search_access", 0) or 0),
                    int(raw_user.get("order_number", 0) or 0),
                    raw_user.get("tracking_code", "") or "",
                    raw_user.get("tracking_stage", "") or "",
                ),
            )


def load_users() -> dict[str, dict]:
    try:
        with get_db() as connection:
            rows = connection.execute("SELECT * FROM users ORDER BY id").fetchall()
    except sqlite3.OperationalError as error:
        if "no such table: users" not in str(error).lower():
            raise
        init_db()
        with get_db() as connection:
            rows = connection.execute("SELECT * FROM users ORDER BY id").fetchall()
    return {str(row["id"]): row_to_user(row) for row in rows}


def save_users(users: dict[str, dict]) -> None:
    with get_db() as connection:
        for user in users.values():
            user_id = int(user.get("id", 0) or 0)
            if not user_id:
                continue

            connection.execute(
                """
                INSERT INTO users (
                    id, first_name, last_name, username,
                    first_seen, last_seen, country, status,
                    note, search_access, order_number, tracking_code, tracking_stage
                    , banned, ban_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    first_name=excluded.first_name,
                    last_name=excluded.last_name,
                    username=excluded.username,
                    first_seen=excluded.first_seen,
                    last_seen=excluded.last_seen,
                    country=excluded.country,
                    status=excluded.status,
                    note=excluded.note,
                    search_access=excluded.search_access,
                    order_number=excluded.order_number,
                    tracking_code=excluded.tracking_code,
                    tracking_stage=excluded.tracking_stage,
                    banned=excluded.banned,
                    ban_reason=excluded.ban_reason
                """,
                (
                    user_id,
                    user.get("first_name", "") or "",
                    user.get("last_name", "") or "",
                    user.get("username", "") or "",
                    user.get("first_seen", "") or now_str(),
                    user.get("last_seen", "") or now_str(),
                    user.get("country", "") or "",
                    user.get("status", "Новый") or "Новый",
                    user.get("note", "") or "",
                    int(user.get("search_access", 0) or 0),
                    int(user.get("order_number", 0) or 0),
                    user.get("tracking_code", "") or "",
                    user.get("tracking_stage", "") or "",
                    1 if user.get("banned") else 0,
                    user.get("ban_reason", "") or "",
                ),
            )

def update_user(
    message: Message,
    *,
    country: str | None = None,
    status: str | None = None,
) -> dict | None:
    if not message.from_user:
        return None

    users = load_users()
    user_id = str(message.from_user.id)
    existing = users.get(user_id, {})

    user_data = {
        "id": message.from_user.id,
        "first_name": message.from_user.first_name or "",
        "last_name": message.from_user.last_name or "",
        "username": message.from_user.username or "",
        "first_seen": existing.get("first_seen", now_str()),
        "last_seen": now_str(),
        "country": country if country is not None else existing.get("country", ""),
        "status": status if status is not None else existing.get("status", "Новый"),
        "note": existing.get("note", ""),
        "search_access": int(existing.get("search_access", 0) or 0),
        "order_number": existing.get("order_number", 0),
        "tracking_code": existing.get("tracking_code", ""),
        "tracking_stage": existing.get("tracking_stage", ""),
        "banned": bool(existing.get("banned", False)),
        "ban_reason": existing.get("ban_reason", ""),
    }

    users[user_id] = user_data
    save_users(users)
    return user_data


def get_user(user_id: int) -> dict | None:
    return load_users().get(str(user_id))


def get_user_by_tracking_code(tracking_code: str) -> tuple[str | None, dict | None]:
    users = load_users()
    normalized = tracking_code.strip().upper()
    for key, user in users.items():
        if (user.get("tracking_code", "") or "").upper() == normalized:
            return key, user
    return None, None


def get_user_by_order_number(order_number: int) -> dict | None:
    users = load_users()
    for user in users.values():
        if int(user.get("order_number", 0) or 0) == order_number:
            return user
    return None


def set_user_status(user_id: int, status: str) -> None:
    users = load_users()
    key = str(user_id)
    if key not in users:
        return
    users[key]["status"] = status
    users[key]["last_seen"] = now_str()
    save_users(users)


def set_user_note(user_id: int, note: str) -> bool:
    users = load_users()
    key = str(user_id)
    if key not in users:
        return False
    users[key]["note"] = note
    users[key]["last_seen"] = now_str()
    save_users(users)
    return True


def ban_user(user_id: int, reason: str) -> dict | None:
    users = load_users()
    key = str(user_id)
    if key not in users:
        return None

    users[key]["banned"] = True
    users[key]["ban_reason"] = reason.strip() or "Причина не указана"
    users[key]["status"] = "Забанен"
    users[key]["last_seen"] = now_str()
    save_users(users)
    return users[key]


def unban_user(user_id: int) -> dict | None:
    users = load_users()
    key = str(user_id)
    if key not in users:
        return None

    users[key]["banned"] = False
    users[key]["ban_reason"] = ""
    if users[key].get("status") == "Забанен":
        users[key]["status"] = "Новый"
    users[key]["last_seen"] = now_str()
    save_users(users)
    return users[key]


def is_user_banned(user_id: int) -> bool:
    user = get_user(user_id)
    return bool(user and user.get("banned"))


def get_banned_users() -> list[dict]:
    users = [user for user in load_users().values() if user.get("banned")]
    users.sort(key=lambda item: item.get("last_seen", ""), reverse=True)
    return users


def get_admin_dashboard_stats() -> dict[str, int]:
    users = list(load_users().values())
    orders = get_all_orders()
    tracked = [user for user in orders if user.get("tracking_code")]
    active_excel_files = len(get_cargo_workbook_files())
    banned_users = sum(1 for user in users if user.get("banned"))
    search_users_count = sum(1 for user in users if int(user.get("search_access", 0) or 0) > 0)
    return {
        "users_total": len(users),
        "orders_total": len(orders),
        "tracked_total": len(tracked),
        "banned_total": banned_users,
        "search_users_total": search_users_count,
        "excel_total": active_excel_files,
    }


def get_cargo_tables_summary() -> str:
    workbook_files = get_cargo_workbook_files()
    if not workbook_files:
        return "Таблиц карго пока нет."

    latest_file = workbook_files[0]
    latest_time = datetime.fromtimestamp(latest_file.stat().st_mtime).strftime("%d.%m.%Y %H:%M")
    return (
        f"Всего таблиц карго: {len(workbook_files)}\n"
        f"Последняя таблица: {latest_file.name}\n"
        f"Последнее изменение: {latest_time}"
    )


def clear_admin_cargo_state_for_workbook(user_id: int, workbook_path: Path) -> None:
    state = admin_cargo_order_states.get(user_id)
    if not state or state.get("workbook_path") != str(workbook_path):
        return

    photo_path = state.get("photo_path")
    if photo_path:
        try:
            Path(photo_path).unlink()
        except OSError:
            pass
    admin_cargo_order_states.pop(user_id, None)


def delete_cargo_workbook(user_id: int, workbook_path: Path) -> bool:
    clear_admin_cargo_state_for_workbook(user_id, workbook_path)
    active_workbook = get_active_cargo_workbook(user_id)
    if active_workbook and active_workbook == workbook_path:
        admin_cargo_active_workbooks.pop(user_id, None)

    try:
        workbook_path.unlink()
        return True
    except OSError:
        return False


def normalize_cargo_field_name(raw_field: str) -> str | None:
    field = raw_field.strip().lower()
    mapping = {
        "name": "name",
        "товар": "name",
        "название": "name",
        "size": "size",
        "размер": "size",
        "color": "color",
        "цвет": "color",
        "price": "price",
        "цена": "price",
        "link": "link",
        "url": "link",
        "ссылка": "link",
        "qty": "quantity",
        "quantity": "quantity",
        "количество": "quantity",
        "delivery": "delivery",
        "доставка": "delivery",
    }
    return mapping.get(field)


def update_cargo_details_cell(
    sheet,
    row_number: int,
    *,
    size: str | None = None,
    color: str | None = None,
) -> None:
    current_size, current_color = parse_cargo_details(sheet[f"{CARGO_DETAILS_COLUMN}{row_number}"].value)
    final_size = current_size if size is None else size.strip()
    final_color = current_color if color is None else color.strip()
    sheet[f"{CARGO_DETAILS_COLUMN}{row_number}"] = f"Размер: {final_size}\nЦвет: {final_color}"


def edit_cargo_workbook_item(
    workbook_path: Path,
    *,
    row_number: int,
    field_name: str,
    value: str,
) -> str:
    try:
        from openpyxl import load_workbook
    except ImportError as error:
        raise RuntimeError(
            "Не хватает библиотек для редактирования Excel. Установи зависимости из requirements.txt."
        ) from error

    workbook = load_workbook(workbook_path)
    try:
        sheet = workbook[CARGO_SHEET_NAME] if CARGO_SHEET_NAME in workbook.sheetnames else workbook.active
        if row_number < CARGO_START_ROW:
            raise ValueError(f"Строка должна быть не меньше {CARGO_START_ROW}.")
        if not is_cargo_row_filled(sheet, row_number):
            raise ValueError("В этой строке нет заполненного товара.")

        normalized_field = normalize_cargo_field_name(field_name)
        if not normalized_field:
            raise ValueError("Неизвестное поле. Доступно: name, size, color, link, price, quantity, delivery.")

        cleaned_value = value.strip()
        if normalized_field == "name":
            if not cleaned_value:
                raise ValueError("Название товара не может быть пустым.")
            sheet[f"{CARGO_NAME_COLUMN}{row_number}"] = cleaned_value
        elif normalized_field == "size":
            update_cargo_details_cell(sheet, row_number, size=cleaned_value)
        elif normalized_field == "color":
            update_cargo_details_cell(sheet, row_number, color=cleaned_value)
        elif normalized_field == "link":
            sheet[f"{CARGO_LINK_COLUMN}{row_number}"] = cleaned_value
            sheet[f"{CARGO_LINK_COLUMN}{row_number}"].hyperlink = cleaned_value
        elif normalized_field == "price":
            parsed_price = parse_price_value(cleaned_value)
            if parsed_price is None:
                raise ValueError("Цену не удалось распознать.")
            sheet[f"{CARGO_PRICE_COLUMN}{row_number}"] = parsed_price
        elif normalized_field == "quantity":
            quantity = parse_price_value(cleaned_value)
            if quantity is None or int(quantity) != quantity or int(quantity) <= 0:
                raise ValueError("Количество должно быть целым числом больше нуля.")
            sheet[f"{CARGO_QUANTITY_COLUMN}{row_number}"] = int(quantity)
        elif normalized_field == "delivery":
            delivery = parse_price_value(cleaned_value)
            if delivery is None:
                raise ValueError("Доставку не удалось распознать.")
            sheet[f"{CARGO_DELIVERY_COLUMN}{row_number}"] = delivery

        sheet[f"{CARGO_TOTAL_COLUMN}{row_number}"] = f"=F{row_number}*G{row_number}"
        workbook.save(workbook_path)
        return normalized_field
    finally:
        workbook.close()


def get_random_user() -> dict | None:
    users = list(load_users().values())
    if not users:
        return None
    return random.choice(users)


def assign_order_number(user_id: int) -> int | None:
    users = load_users()
    key = str(user_id)
    if key not in users:
        return None

    existing_number = users[key].get("order_number", 0)
    if existing_number:
        return existing_number

    last_number = max((int(user.get("order_number", 0)) for user in users.values()), default=0)
    new_number = last_number + 1
    users[key]["order_number"] = new_number
    users[key]["last_seen"] = now_str()
    save_users(users)
    return new_number


def can_approve_order(user: dict | None) -> bool:
    if not user:
        return False
    return user.get("status") in {"Заявка отправлена", "Отправляет данные"}


def can_cancel_order(user: dict | None) -> bool:
    if not user:
        return False
    if user.get("status") == "Заказ отменен":
        return False
    return bool(
        user.get("order_number", 0)
        or user.get("tracking_code")
        or user.get("status") in {
            "Оформляет заказ",
            "Отправляет данные",
            "Заявка отправлена",
            "Заказ обработан",
        }
    )


def get_pending_orders() -> list[dict]:
    users = load_users()
    pending = [
        user for user in users.values()
        if user.get("status") in {"Заявка отправлена", "Отправляет данные"}
    ]
    pending.sort(key=lambda item: item.get("last_seen", ""), reverse=True)
    return pending


def get_all_orders() -> list[dict]:
    users = load_users()
    orders = [
        user for user in users.values()
        if (
            user.get("order_number", 0)
            or user.get("tracking_code")
            or user.get("status") in ACTIVE_ORDER_STATUSES
        )
    ]
    orders.sort(
        key=lambda item: (
            int(item.get("order_number", 0) or 0),
            item.get("last_seen", ""),
        ),
        reverse=True,
    )
    return orders


def get_active_orders() -> list[dict]:
    return [
        user for user in get_all_orders()
        if user.get("tracking_stage") != TRACKING_STAGES["4"]
    ]


def get_tracked_orders() -> list[dict]:
    orders = [user for user in get_all_orders() if user.get("tracking_code")]
    orders.sort(key=lambda item: item.get("last_seen", ""), reverse=True)
    return orders


def get_recent_users(limit: int = 10) -> list[dict]:
    users = load_users()
    recent = sorted(users.values(), key=lambda item: item.get("last_seen", ""), reverse=True)
    return recent[:limit]


def get_today_stats() -> dict[str, int]:
    today_prefix = datetime.now().strftime("%Y-%m-%d")
    users = list(load_users().values())
    return {
        "new_users": sum(1 for user in users if (user.get("first_seen", "") or "").startswith(today_prefix)),
        "active_today": sum(1 for user in users if (user.get("last_seen", "") or "").startswith(today_prefix)),
        "orders_today": sum(
            1
            for user in users
            if int(user.get("order_number", 0) or 0) > 0 and (user.get("last_seen", "") or "").startswith(today_prefix)
        ),
        "processed_today": sum(
            1
            for user in users
            if user.get("status") == "Заказ обработан" and (user.get("last_seen", "") or "").startswith(today_prefix)
        ),
    }


def get_search_access_users() -> list[dict]:
    users = load_users()
    access_users = [user for user in users.values() if int(user.get("search_access", 0) or 0) > 0]
    access_users.sort(key=lambda item: item.get("last_seen", ""), reverse=True)
    return access_users


def search_users(query: str, limit: int = 12) -> list[dict]:
    normalized_query = normalize_query(query)
    if not normalized_query:
        return []

    results: list[dict] = []
    for user in load_users().values():
        haystack = normalize_query(
            " ".join(
                [
                    str(user.get("id", "")),
                    user.get("first_name", "") or "",
                    user.get("last_name", "") or "",
                    user.get("username", "") or "",
                    user.get("country", "") or "",
                    user.get("status", "") or "",
                    user.get("note", "") or "",
                    str(user.get("order_number", 0) or ""),
                    user.get("tracking_code", "") or "",
                    user.get("tracking_stage", "") or "",
                ]
            )
        )
        if normalized_query in haystack:
            results.append(user)

    results.sort(key=lambda item: item.get("last_seen", ""), reverse=True)
    return results[:limit]


def add_search_credits(user_id: int, amount: int) -> int | None:
    users = load_users()
    key = str(user_id)
    if key not in users or amount <= 0:
        return None

    current = int(users[key].get("search_access", 0) or 0)
    users[key]["search_access"] = current + amount
    users[key]["last_seen"] = now_str()
    save_users(users)
    return int(users[key]["search_access"])


def set_search_credits(user_id: int, amount: int) -> int | None:
    users = load_users()
    key = str(user_id)
    if key not in users:
        return None

    users[key]["search_access"] = max(0, int(amount))
    users[key]["last_seen"] = now_str()
    save_users(users)
    return int(users[key]["search_access"])


def consume_search_credit(user_id: int) -> int | None:
    users = load_users()
    key = str(user_id)
    if key not in users:
        return None

    current = int(users[key].get("search_access", 0) or 0)
    if current <= 0:
        return None

    users[key]["search_access"] = current - 1
    users[key]["last_seen"] = now_str()
    save_users(users)
    return int(users[key]["search_access"])


def format_order_card(user: dict, *, include_action_hint: bool = False) -> str:
    username = f"@{user['username']}" if user.get("username") else "без username"
    full_name = " ".join(
        part for part in [user.get("first_name", ""), user.get("last_name", "")] if part
    ).strip() or "Без имени"

    lines = [
        f"Заказ №{user.get('order_number', 0) or '-'}",
        f"ID: {user['id']}",
        f"Имя: {full_name}",
        f"Username: {username}",
        f"Страна: {user.get('country', '') or 'Не выбрана'}",
        f"Статус: {user.get('status', '') or 'Новый'}",
        f"Попыток поиска: {int(user.get('search_access', 0) or 0)}",
        f"Трек-код: {user.get('tracking_code', '') or 'Нет'}",
        f"Этап: {user.get('tracking_stage', '') or 'Нет'}",
        f"Обновлен: {user.get('last_seen', '-')}",
    ]

    if include_action_hint and user.get("status") in {"Заявка отправлена", "Отправляет данные"}:
        lines.append(f"Для одобрения: /done {user['id']}")
    if include_action_hint and user.get("status") != "Заказ отменен":
        lines.append(f"Для отмены: /cancel {user['id']}")
    if user.get("banned"):
        lines.append(f"Бан: Да ({user.get('ban_reason', '') or 'Причина не указана'})")

    return "\n".join(lines)


def generate_tracking_code() -> str:
    random_code = secrets.token_hex(5).upper()
    return f"GEEKLOGK-{random_code[:5]}-{random_code[5:]}"


def assign_tracking_code(user_id: int) -> str | None:
    users = load_users()
    key = str(user_id)
    if key not in users:
        return None

    existing_codes = {
        (user.get("tracking_code", "") or "").upper()
        for user in users.values()
        if user.get("tracking_code")
    }

    tracking_code = generate_tracking_code()
    while tracking_code.upper() in existing_codes:
        tracking_code = generate_tracking_code()

    users[key]["tracking_code"] = tracking_code
    users[key]["tracking_stage"] = TRACKING_STAGES["1"]
    users[key]["last_seen"] = now_str()
    save_users(users)
    return tracking_code


def set_tracking_stage(tracking_code: str, stage_key: str) -> dict | None:
    user_key, user = get_user_by_tracking_code(tracking_code)
    if not user_key or not user:
        return None

    stage = TRACKING_STAGES.get(stage_key)
    if not stage:
        return None

    users = load_users()
    users[user_key]["tracking_stage"] = stage
    users[user_key]["last_seen"] = now_str()
    save_users(users)
    return users[user_key]


def cancel_user_order(user_id: int) -> dict | None:
    users = load_users()
    key = str(user_id)
    if key not in users:
        return None

    users[key]["country"] = ""
    users[key]["status"] = "Новый"
    users[key]["order_number"] = 0
    users[key]["tracking_code"] = ""
    users[key]["tracking_stage"] = ""
    users[key]["last_seen"] = now_str()
    save_users(users)
    return users[key]


def purge_canceled_orders() -> None:
    users = load_users()
    changed = False

    for user in users.values():
        if user.get("status") != "Заказ отменен":
            continue

        user["country"] = ""
        user["status"] = "Новый"
        user["order_number"] = 0
        user["tracking_code"] = ""
        user["tracking_stage"] = ""
        user["last_seen"] = now_str()
        changed = True

    if changed:
        save_users(users)


def is_admin(message: Message) -> bool:
    return bool(message.from_user and message.from_user.id == ADMIN_ID)


async def maintenance_guard(message: Message) -> bool:
    if is_admin(message):
        return False
    user = get_user(message.from_user.id) if message.from_user else None
    if user and user.get("banned"):
        reason = user.get("ban_reason", "") or "Причина не указана"
        await message.answer(
            "Доступ к боту ограничен.\n"
            f"Причина: {reason}\n"
            "Если это ошибка, свяжись с администратором."
        )
        return True
    if not is_maintenance_mode():
        return False

    await message.answer("Бот временно находится на техобслуживании. Попробуй написать позже.")
    return True


def is_rate_limited(message: Message) -> bool:
    if not message.from_user:
        return False
    if is_admin(message):
        return False

    user_id = message.from_user.id
    now = time.monotonic()
    history = user_request_times.get(user_id, [])
    history = [stamp for stamp in history if now - stamp <= RATE_WINDOW_SECONDS]
    history.append(now)
    user_request_times[user_id] = history
    return len(history) > MAX_MESSAGES_PER_WINDOW


async def maybe_send_rate_limit_notice(message: Message) -> None:
    if not message.from_user:
        return

    user_id = message.from_user.id
    now = time.monotonic()
    last_notice = user_last_limit_notice.get(user_id, 0)
    if now - last_notice < RATE_LIMIT_NOTICE_COOLDOWN:
        return

    user_last_limit_notice[user_id] = now
    await message.answer("Слишком много сообщений подряд. Подожди пару секунд и отправь снова.")


def current_main_keyboard(message: Message) -> ReplyKeyboardMarkup:
    return admin_main_keyboard if is_admin(message) else user_main_keyboard


def clear_admin_cargo_mode(message: Message) -> None:
    if not message.from_user:
        return
    reset_admin_cargo_order_state(message.from_user.id)
    reset_admin_message_order_state(message.from_user.id)


def reset_admin_cargo_order_state(user_id: int) -> None:
    state = admin_cargo_order_states.pop(user_id, None)
    if not state:
        return

    photo_path = state.get("photo_path")
    if photo_path:
        try:
            Path(photo_path).unlink()
        except OSError:
            pass

    workbook_path = state.get("workbook_path")
    active_workbook_path = admin_cargo_active_workbooks.get(user_id)
    is_filled = bool(state.get("workbook_filled"))
    should_keep_workbook = workbook_path and workbook_path == active_workbook_path
    if not workbook_path or is_filled or should_keep_workbook:
        return

    try:
        Path(workbook_path).unlink()
    except OSError:
        pass


def reset_admin_message_order_state(user_id: int) -> None:
    state = admin_message_order_states.pop(user_id, None)
    if not state:
        return

    photo_path = state.get("photo_path")
    if photo_path:
        try:
            Path(photo_path).unlink()
        except OSError:
            pass


def get_cargo_excel_error() -> str | None:
    if not CARGO_TEMPLATE_XLSX.exists():
        return f"Файл шаблона не найден: {CARGO_TEMPLATE_XLSX}"

    try:
        import openpyxl  # noqa: F401
        from openpyxl.drawing.image import Image as XLImage  # noqa: F401
    except ImportError:
        return (
            "Для работы с таблицей нужны библиотеки openpyxl и pillow.\n"
            "Установи зависимости командой: python -m pip install -r requirements.txt"
        )

    return None


def create_cargo_workbook_copy() -> Path:
    CARGO_ORDERS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    random_suffix = secrets.token_hex(2).upper()
    target_path = CARGO_ORDERS_DIR / f"каргос_{stamp}_{random_suffix}.xlsx"
    copy2(CARGO_TEMPLATE_XLSX, target_path)
    return target_path


def get_active_cargo_workbook(user_id: int) -> Path | None:
    workbook_path = admin_cargo_active_workbooks.get(user_id)
    if not workbook_path:
        return None

    path = Path(workbook_path)
    if path.exists():
        return path

    admin_cargo_active_workbooks.pop(user_id, None)
    return None


def create_new_active_cargo_workbook(user_id: int) -> Path:
    workbook_path = create_cargo_workbook_copy()
    admin_cargo_active_workbooks[user_id] = str(workbook_path)
    return workbook_path


def set_active_cargo_workbook(user_id: int, workbook_path: Path) -> None:
    admin_cargo_active_workbooks[user_id] = str(workbook_path)


def clear_active_cargo_workbook(user_id: int) -> Path | None:
    workbook_path = admin_cargo_active_workbooks.pop(user_id, None)
    return Path(workbook_path) if workbook_path else None


def get_cargo_workbook_files() -> list[Path]:
    if not CARGO_ORDERS_DIR.exists():
        return []

    return sorted(
        (path for path in CARGO_ORDERS_DIR.glob("*.xlsx") if path.is_file()),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def resolve_cargo_workbook_reference(user_id: int, raw_reference: str | None) -> Path | None:
    reference = (raw_reference or "").strip()
    if not reference or reference.lower() in {"current", "active", "текущая", "текущий"}:
        return get_active_cargo_workbook(user_id)

    workbook_files = get_cargo_workbook_files()
    if reference.isdigit():
        index = int(reference)
        if 1 <= index <= len(workbook_files):
            return workbook_files[index - 1]

    candidate = CARGO_ORDERS_DIR / reference
    if candidate.exists() and candidate.is_file():
        return candidate

    candidate_with_suffix = candidate.with_suffix(".xlsx")
    if candidate_with_suffix.exists() and candidate_with_suffix.is_file():
        return candidate_with_suffix

    for workbook_path in workbook_files:
        if workbook_path.name == reference:
            return workbook_path

    return None


def parse_cargo_details(value: object) -> tuple[str, str]:
    if value in (None, ""):
        return "", ""

    size = ""
    color = ""
    for line in str(value).splitlines():
        normalized = line.strip()
        lower = normalized.lower()
        if lower.startswith("размер:"):
            size = normalized.split(":", 1)[1].strip()
        elif lower.startswith("цвет:"):
            color = normalized.split(":", 1)[1].strip()

    return size, color


def is_cargo_row_filled(sheet, row_number: int) -> bool:
    cells_to_check = [
        sheet[f"{CARGO_NAME_COLUMN}{row_number}"].value,
        sheet[f"{CARGO_LINK_COLUMN}{row_number}"].value,
        sheet[f"{CARGO_DETAILS_COLUMN}{row_number}"].value,
        sheet[f"{CARGO_PRICE_COLUMN}{row_number}"].value,
        sheet[f"{CARGO_PHOTO_COLUMN}{row_number}"].value,
    ]
    return any(str(value).strip() for value in cells_to_check if value not in (None, ""))


def format_cargo_workbook_preview(workbook_path: Path) -> str:
    try:
        from openpyxl import load_workbook
    except ImportError as error:
        raise RuntimeError(
            "Не хватает библиотек для просмотра Excel. Установи зависимости из requirements.txt."
        ) from error

    workbook = load_workbook(workbook_path, data_only=False)
    try:
        sheet = workbook[CARGO_SHEET_NAME] if CARGO_SHEET_NAME in workbook.sheetnames else workbook.active
        lines = [
            f"Таблица: {workbook_path.name}",
            f"Лист: {sheet.title}",
            "",
        ]

        item_lines: list[str] = []
        for row_number in range(CARGO_START_ROW, sheet.max_row + 1):
            if not is_cargo_row_filled(sheet, row_number):
                continue

            item_name = sheet[f"{CARGO_NAME_COLUMN}{row_number}"].value
            link = sheet[f"{CARGO_LINK_COLUMN}{row_number}"].value
            details = sheet[f"{CARGO_DETAILS_COLUMN}{row_number}"].value
            price = sheet[f"{CARGO_PRICE_COLUMN}{row_number}"].value
            quantity = sheet[f"{CARGO_QUANTITY_COLUMN}{row_number}"].value
            total = sheet[f"{CARGO_TOTAL_COLUMN}{row_number}"].value
            delivery = sheet[f"{CARGO_DELIVERY_COLUMN}{row_number}"].value

            size, color = parse_cargo_details(details)
            item_lines.append(
                "\n".join(
                    [
                        f"Строка {row_number}",
                        f"Товар: {item_name or 'Не указан'}",
                        f"Размер: {size or 'Не указан'}",
                        f"Цвет: {color or 'Не указан'}",
                        f"Ссылка: {link or 'Не указана'}",
                        f"Цена: {price if price not in (None, '') else 'Не указана'}",
                        f"Количество: {quantity if quantity not in (None, '') else 'Не указано'}",
                        f"Сумма: {total if total not in (None, '') else 'Не указана'}",
                        f"Доставка: {delivery if delivery not in (None, '') else 'Не указана'}",
                    ]
                )
            )

        if not item_lines:
            lines.append("В таблице пока нет заполненных товаров.")
        else:
            lines.append(f"Товаров в таблице: {len(item_lines)}")
            lines.append("")
            lines.extend(item_lines)

        return "\n\n".join(lines)
    finally:
        workbook.close()


def is_expected_total_formula(value: object, row_number: int) -> bool:
    if value in (None, ""):
        return True

    formula = str(value).replace("$", "").replace(" ", "").upper()
    return formula in {
        f"=F{row_number}*G{row_number}",
        f"F{row_number}*G{row_number}",
    }


def is_cargo_row_available(sheet, row_number: int) -> bool:
    cells_to_check = [
        sheet[f"{CARGO_NAME_COLUMN}{row_number}"].value,
        sheet[f"{CARGO_LINK_COLUMN}{row_number}"].value,
        sheet[f"{CARGO_DETAILS_COLUMN}{row_number}"].value,
        sheet[f"{CARGO_PRICE_COLUMN}{row_number}"].value,
        sheet[f"{CARGO_PHOTO_COLUMN}{row_number}"].value,
    ]
    if any(str(value).strip() for value in cells_to_check if value not in (None, "")):
        return False

    return is_expected_total_formula(
        sheet[f"{CARGO_TOTAL_COLUMN}{row_number}"].value,
        row_number,
    )


def find_next_cargo_row(sheet) -> int | None:
    for row_number in range(CARGO_START_ROW, sheet.max_row + 1):
        if is_cargo_row_available(sheet, row_number):
            return row_number
    return None


def parse_price_value(text: str) -> int | float | None:
    normalized = text.replace(" ", "").replace(",", ".")
    match = re.search(r"\d+(?:\.\d+)?", normalized)
    if not match:
        return None

    value = float(match.group(0))
    if value.is_integer():
        return int(value)
    return round(value, 2)


def copy_cargo_row_style(sheet, source_row: int, target_row: int) -> None:
    for column in range(1, sheet.max_column + 1):
        source_cell = sheet.cell(row=source_row, column=column)
        target_cell = sheet.cell(row=target_row, column=column)
        target_cell._style = copy(source_cell._style)
        if source_cell.number_format:
            target_cell.number_format = source_cell.number_format
        if source_cell.font:
            target_cell.font = copy(source_cell.font)
        if source_cell.fill:
            target_cell.fill = copy(source_cell.fill)
        if source_cell.border:
            target_cell.border = copy(source_cell.border)
        if source_cell.alignment:
            target_cell.alignment = copy(source_cell.alignment)
        if source_cell.protection:
            target_cell.protection = copy(source_cell.protection)

    if source_row in sheet.row_dimensions:
        sheet.row_dimensions[target_row].height = sheet.row_dimensions[source_row].height


def prepare_new_cargo_row(sheet) -> int:
    row_number = find_next_cargo_row(sheet)
    if row_number is not None:
        return row_number

    insert_at = 19
    sheet.insert_rows(insert_at)
    copy_cargo_row_style(sheet, 18, insert_at)
    sheet[f"{CARGO_QUANTITY_COLUMN}{insert_at}"] = 1
    sheet[f"{CARGO_TOTAL_COLUMN}{insert_at}"] = f"=F{insert_at}*G{insert_at}"
    sheet[f"{CARGO_DELIVERY_COLUMN}{insert_at}"] = 0
    return insert_at


def write_cargo_order_to_excel(
    *,
    workbook_path: Path,
    photo_path: Path,
    size: str,
    item_name: str,
    color: str,
    link: str,
    price: int | float,
) -> int:
    try:
        from openpyxl import load_workbook
        from openpyxl.drawing.image import Image as XLImage
    except ImportError as error:
        raise RuntimeError(
            "Не хватает библиотек для работы с Excel. Установи зависимости из requirements.txt."
        ) from error

    workbook = load_workbook(workbook_path)
    try:
        sheet = workbook[CARGO_SHEET_NAME] if CARGO_SHEET_NAME in workbook.sheetnames else workbook.active
        row_number = prepare_new_cargo_row(sheet)

        sheet[f"{CARGO_NAME_COLUMN}{row_number}"] = item_name.strip()
        sheet[f"{CARGO_LINK_COLUMN}{row_number}"] = link.strip()
        sheet[f"{CARGO_LINK_COLUMN}{row_number}"].hyperlink = link.strip()
        sheet[f"{CARGO_DETAILS_COLUMN}{row_number}"] = (
            f"Размер: {size.strip()}\n"
            f"Цвет: {color.strip()}"
        )
        sheet[f"{CARGO_PRICE_COLUMN}{row_number}"] = price
        sheet[f"{CARGO_QUANTITY_COLUMN}{row_number}"] = sheet[f"{CARGO_QUANTITY_COLUMN}{row_number}"].value or 1
        sheet[f"{CARGO_TOTAL_COLUMN}{row_number}"] = f"=F{row_number}*G{row_number}"
        sheet[f"{CARGO_DELIVERY_COLUMN}{row_number}"] = sheet[f"{CARGO_DELIVERY_COLUMN}{row_number}"].value or 0
        sheet.row_dimensions[row_number].height = 78

        image = XLImage(str(photo_path))
        image.width = CARGO_IMAGE_WIDTH
        image.height = CARGO_IMAGE_HEIGHT
        sheet.add_image(image, f"{CARGO_PHOTO_COLUMN}{row_number}")

        workbook.save(workbook_path)
        return row_number
    finally:
        workbook.close()


async def download_cargo_photo(bot: Bot, message: Message) -> Path | None:
    if not message.from_user:
        return None

    telegram_file = None
    suffix = ".jpg"

    if message.photo:
        telegram_file = await bot.get_file(message.photo[-1].file_id)
    elif message.document and (message.document.mime_type or "").startswith("image/"):
        telegram_file = await bot.get_file(message.document.file_id)
        suffix = Path(message.document.file_name or "").suffix or ".jpg"

    if telegram_file is None:
        return None

    CARGO_PHOTO_DIR.mkdir(parents=True, exist_ok=True)
    destination = CARGO_PHOTO_DIR / f"cargo_{message.from_user.id}_{int(time.time() * 1000)}{suffix}"
    with destination.open("wb") as output:
        await bot.download(telegram_file, destination=output)
    return destination


async def start_admin_message_order_flow(message: Message) -> None:
    if not message.from_user:
        return

    user_id = message.from_user.id
    reset_admin_cargo_order_state(user_id)
    reset_admin_message_order_state(user_id)
    china_order_users.discard(user_id)
    admin_search_users.discard(user_id)
    paid_search_users.discard(user_id)
    tracking_lookup_users.discard(user_id)
    admin_message_order_states[user_id] = {
        "step": "photo",
    }
    update_user(message, status="Админ оформляет заказ сообщением")
    await message.answer(
        "Оформление заказа сообщением начато.\n"
        "Скиньте фотографию.",
        reply_markup=admin_cargo_order_keyboard,
    )


async def handle_admin_message_order_step(message: Message, bot: Bot) -> bool:
    if not message.from_user or not is_admin(message):
        return False

    state = admin_message_order_states.get(message.from_user.id)
    if not state:
        return False

    step = state.get("step", "photo")

    if step == "photo":
        photo_path = await download_cargo_photo(bot, message)
        if not photo_path:
            await message.answer(
                "Нужна именно фотография товара.\n"
                "Пришли фото как изображение или файлом-картинкой.",
                reply_markup=admin_cargo_order_keyboard,
            )
            return True

        state["photo_path"] = str(photo_path)
        state["step"] = "size"
        await message.answer("Размер", reply_markup=admin_cargo_order_keyboard)
        return True

    text = (message.text or "").strip()
    if not text:
        prompts = {
            "size": "Отправь размер текстом.",
            "name": "Отправь наименование товара текстом.",
            "color": "Отправь цвет товара текстом.",
            "link": "Отправь ссылку на товар текстом.",
        }
        await message.answer(prompts.get(step, "Отправь данные текстом."), reply_markup=admin_cargo_order_keyboard)
        return True

    if step == "size":
        state["size"] = text
        state["step"] = "name"
        await message.answer("Наименование товара", reply_markup=admin_cargo_order_keyboard)
        return True

    if step == "name":
        state["name"] = text
        state["step"] = "color"
        await message.answer("Цвет товара", reply_markup=admin_cargo_order_keyboard)
        return True

    if step == "color":
        state["color"] = text
        state["step"] = "link"
        await message.answer("Ссылка на товар", reply_markup=admin_cargo_order_keyboard)
        return True

    if step != "link":
        return False

    state["link"] = text
    photo_path = Path(state["photo_path"])
    caption = (
        "Оформленный заказ:\n\n"
        f"Товар: {state.get('name', '')}\n"
        f"Размер: {state.get('size', '')}\n"
        f"Цвет: {state.get('color', '')}\n"
        f"Ссылка: {state.get('link', '')}"
    )

    try:
        await bot.send_photo(
            ADMIN_ID,
            photo=FSInputFile(str(photo_path)),
            caption=caption,
        )
    except Exception as error:
        await message.answer(
            "Не удалось отправить собранный заказ одним сообщением.\n"
            f"Ошибка: {error}",
            reply_markup=current_main_keyboard(message),
        )
        reset_admin_message_order_state(message.from_user.id)
        return True

    reset_admin_message_order_state(message.from_user.id)
    update_user(message, status="Админ оформил заказ сообщением")
    await message.answer(
        "Готово. Заказ отправлен одним сообщением.\n"
        f"Товар: {state.get('name', '')}\n"
        f"Размер: {state.get('size', '')}\n"
        f"Цвет: {state.get('color', '')}\n"
        f"Ссылка: {state.get('link', '')}",
        reply_markup=current_main_keyboard(message),
    )
    return True


async def start_admin_cargo_order_flow(message: Message, *, force_new_workbook: bool = False) -> None:
    if not message.from_user:
        return

    excel_error = get_cargo_excel_error()
    if excel_error:
        await message.answer(excel_error)
        return

    user_id = message.from_user.id
    reset_admin_cargo_order_state(user_id)
    workbook_path = (
        create_new_active_cargo_workbook(user_id)
        if force_new_workbook
        else get_active_cargo_workbook(user_id) or create_new_active_cargo_workbook(user_id)
    )
    china_order_users.discard(user_id)
    admin_search_users.discard(user_id)
    paid_search_users.discard(user_id)
    tracking_lookup_users.discard(user_id)
    admin_cargo_order_states[user_id] = {
        "step": "photo",
        "workbook_path": str(workbook_path),
    }
    update_user(message, status="Админ оформляет заказ в Excel")
    if force_new_workbook:
        text = (
            "Оформление заказа в новой таблице начато.\n"
            f"Файл: {workbook_path.name}\n"
            "Скиньте фотографию."
        )
    else:
        text = (
            "Оформление заказа открыто.\n"
            f"Текущий файл: {workbook_path.name}\n"
            "Новый товар будет добавлен в эту же таблицу.\n"
            "Скиньте фотографию."
        )

    await message.answer(text, reply_markup=admin_cargo_order_keyboard)


async def handle_admin_cargo_order_step(message: Message, bot: Bot) -> bool:
    if not message.from_user or not is_admin(message):
        return False

    state = admin_cargo_order_states.get(message.from_user.id)
    if not state:
        return False

    step = state.get("step", "photo")

    if step == "photo":
        photo_path = await download_cargo_photo(bot, message)
        if not photo_path:
            await message.answer(
                "Нужна именно фотография товара.\n"
                "Пришли фото как изображение или файлом-картинкой.",
                reply_markup=admin_cargo_order_keyboard,
            )
            return True

        state["photo_path"] = str(photo_path)
        state["step"] = "size"
        await message.answer("Размер", reply_markup=admin_cargo_order_keyboard)
        return True

    text = (message.text or "").strip()
    if not text:
        prompts = {
            "size": "Отправь размер текстом.",
            "name": "Отправь наименование товара текстом.",
            "color": "Отправь цвет товара текстом.",
            "link": "Отправь ссылку на товар текстом.",
            "price": "Отправь ценник числом, например 199 или 199.5.",
        }
        await message.answer(prompts.get(step, "Отправь данные текстом."), reply_markup=admin_cargo_order_keyboard)
        return True

    if step == "size":
        state["size"] = text
        state["step"] = "name"
        await message.answer("Наименование товара", reply_markup=admin_cargo_order_keyboard)
        return True

    if step == "name":
        state["name"] = text
        state["step"] = "color"
        await message.answer("Цвет товара", reply_markup=admin_cargo_order_keyboard)
        return True

    if step == "color":
        state["color"] = text
        state["step"] = "link"
        await message.answer("Ссылка на товар", reply_markup=admin_cargo_order_keyboard)
        return True

    if step == "link":
        state["link"] = text
        state["step"] = "price"
        await message.answer("Ценник", reply_markup=admin_cargo_order_keyboard)
        return True

    if step != "price":
        return False

    price_value = parse_price_value(text)
    if price_value is None:
        await message.answer(
            "Не смог распознать ценник.\n"
            "Отправь его числом, например 299 или 299.5.",
            reply_markup=admin_cargo_order_keyboard,
        )
        return True

    photo_path = Path(state["photo_path"])
    workbook_path = Path(state["workbook_path"])
    try:
        row_number = write_cargo_order_to_excel(
            workbook_path=workbook_path,
            photo_path=photo_path,
            size=str(state.get("size", "")),
            item_name=str(state.get("name", "")),
            color=str(state.get("color", "")),
            link=str(state.get("link", "")),
            price=price_value,
        )
    except Exception as error:
        await message.answer(
            "Не удалось записать позицию в Excel.\n"
            f"Ошибка: {error}",
            reply_markup=current_main_keyboard(message),
        )
        reset_admin_cargo_order_state(message.from_user.id)
        return True

    state["workbook_filled"] = True
    reset_admin_cargo_order_state(message.from_user.id)
    update_user(message, status="Админ оформил заказ в Excel")
    await message.answer(
        "Готово. Позиция добавлена в таблицу.\n"
        f"Файл: {workbook_path.name}\n"
        f"Строка: {row_number}\n"
        f"Товар: {state.get('name', '')}\n"
        f"Размер: {state.get('size', '')}\n"
        f"Цвет: {state.get('color', '')}\n"
        f"Ссылка: {state.get('link', '')}\n"
        f"Цена: {price_value}\n\n"
        "Чтобы добавить ещё товар в эту же таблицу, снова нажми 'Оформить заказ' или используй /excelorder.\n"
        "Чтобы начать новую таблицу, используй /newexcelorder.",
        reply_markup=current_main_keyboard(message),
    )
    return True


def normalize_query(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def tokenize_query(text: str) -> list[str]:
    return [token for token in re.split(r"\W+", normalize_query(text)) if len(token) > 1]


def build_result_text(results: list[dict]) -> str:
    if not results:
        return "Ничего не нашел по этому запросу. Попробуй написать точнее модель, бренд или цвет."

    lines = ["Вот что удалось найти:"]
    for index, result in enumerate(results[:MAX_RESULTS], start=1):
        lines.append(f"{index}. {result['title'][:120]}\n{result['url']}")
    return "\n\n".join(lines)


def split_text_chunks(text: str, limit: int = 3500) -> list[str]:
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current = ""
    for block in text.split("\n\n"):
        candidate = block if not current else f"{current}\n\n{block}"
        if len(candidate) <= limit:
            current = candidate
            continue

        if current:
            chunks.append(current)
            current = ""

        while len(block) > limit:
            chunks.append(block[:limit])
            block = block[limit:]
        current = block

    if current:
        chunks.append(current)
    return chunks


async def send_long_text(message: Message, text: str) -> None:
    for chunk in split_text_chunks(text):
        await message.answer(chunk)


async def apply_tracking_stage_change(
    message: Message,
    bot: Bot,
    tracking_code: str,
    stage_key: str,
) -> bool:
    updated_user = set_tracking_stage(tracking_code, stage_key)
    if not updated_user:
        await message.answer("Заказ с таким кодом не найден.")
        return False

    username = f"@{updated_user['username']}" if updated_user.get("username") else "без username"
    stage = TRACKING_STAGES[stage_key]
    await message.answer(
        f"Этап обновлен.\n"
        f"Код: {tracking_code}\n"
        f"Владелец: {username}\n"
        f"Новый этап: {stage}"
    )
    try:
        await bot.send_message(
            updated_user["id"],
            "Обновление по заказу.\n"
            f"Трек-код: {tracking_code}\n"
            f"Новый этап: {stage}"
        )
    except Exception:
        pass
    return True


async def send_photo_or_text(
    message: Message,
    image_path: Path,
    caption: str,
    reply_markup: ReplyKeyboardMarkup | None = None,
) -> None:
    if image_path.exists():
        await message.answer_photo(
            photo=FSInputFile(str(image_path)),
            caption=caption,
            reply_markup=reply_markup,
        )
        return
    await message.answer(caption, reply_markup=reply_markup)


async def send_main_menu(message: Message, user_name: str) -> None:
    await send_photo_or_text(
        message,
        WELCOME_IMAGE,
        f"Привет, {user_name}! Это бот для заказа вещей из разных стран. Выбери нужный раздел в меню ниже:",
        reply_markup=current_main_keyboard(message),
    )


async def notify_admin(bot: Bot, message: Message) -> None:
    if not message.from_user or message.from_user.id == ADMIN_ID:
        return

    username = f"@{message.from_user.username}" if message.from_user.username else "без username"
    text = message.text or message.caption or "[не текстовое сообщение]"
    await bot.send_message(
        ADMIN_ID,
        "Новые данные по заказу:\n\n"
        f"ID: {message.from_user.id}\n"
        f"Имя: {message.from_user.full_name}\n"
        f"Username: {username}\n"
        f"Текст: {text}\n\n"
        "Чтобы ответить, используй:\n"
        "/send user_id текст\n"
        "/done user_id",
    )


async def fetch_site_html(session: aiohttp.ClientSession, url: str) -> str:
    try:
        async with session.get(url, headers=HTTP_HEADERS, ssl=False) as response:
            if response.status != 200:
                return ""
            return await response.text()
    except Exception:
        return ""


def extract_matches_from_html(html: str, base_url: str, query: str) -> list[dict]:
    if not html:
        return []

    tokens = tokenize_query(query)
    if not tokens:
        return []

    soup = BeautifulSoup(html, "html.parser")
    results: list[dict] = []
    seen_urls: set[str] = set()

    for link in soup.find_all("a", href=True):
        title_parts = [
            link.get_text(" ", strip=True),
            link.get("title", ""),
            link.get("aria-label", ""),
            link.get("alt", ""),
            link["href"],
        ]
        haystack = normalize_query(" ".join(part for part in title_parts if part))
        if not haystack:
            continue

        token_matches = sum(1 for token in tokens if token in haystack)
        if token_matches == 0:
            continue

        href = urljoin(base_url, link["href"])
        if href in seen_urls:
            continue

        seen_urls.add(href)
        title = link.get_text(" ", strip=True) or link.get("title") or href
        results.append({"title": title, "url": href, "score": token_matches})

    results.sort(key=lambda item: item["score"], reverse=True)
    return results[:MAX_RESULTS]


async def search_source(session: aiohttp.ClientSession, source_url: str, query: str) -> list[dict]:
    html = await fetch_site_html(session, source_url)
    return extract_matches_from_html(html, source_url, query)


async def search_item_in_sources(query: str) -> list[dict]:
    connector = aiohttp.TCPConnector(limit=12, limit_per_host=2, ssl=False)
    async with search_semaphore:
        async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT, connector=connector) as session:
            batches = await asyncio.gather(
                *(search_source(session, url, query) for url in SEARCH_SOURCES),
                return_exceptions=False,
            )

    results = [item for batch in batches for item in batch]
    results.sort(key=lambda item: item["score"], reverse=True)

    unique: list[dict] = []
    seen_urls: set[str] = set()
    for result in results:
        if result["url"] in seen_urls:
            continue
        seen_urls.add(result["url"])
        unique.append(result)
        if len(unique) >= MAX_RESULTS:
            break
    return unique


@dp.message(Command("start"))
async def cmd_start(message: Message) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return
    clear_admin_cargo_mode(message)
    update_user(message, status="Запустил бота")
    user_name = message.from_user.first_name if message.from_user else "друг"
    await send_main_menu(message, user_name)


@dp.message(Command("help"))
async def cmd_help(message: Message) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return
    clear_admin_cargo_mode(message)
    update_user(message)
    await message.answer(
        "Команды:\n"
        "/start - запустить бота\n"
        "/help - показать помощь\n"
        "/track КОД - посмотреть статус заказа\n\n"
        "Доступные разделы:\n"
        "- Сделать заказ\n"
        "- Тарифы\n"
        "- Инфо\n"
        "- Тех. поддержка\n\n"
        "В разделе 'Инфо' можно быстро посмотреть свой заказ и попытки поиска."
    )


@dp.message(Command("admin"))
async def admin_panel(message: Message) -> None:
    clear_admin_cargo_mode(message)
    update_user(message)
    if not is_admin(message):
        await message.answer("У тебя нет доступа к админ-панели.")
        return

    maintenance_status = "включено" if is_maintenance_mode() else "выключено"
    await message.answer(
        "Админ-панель:\n\n"
        f"Техобслуживание: {maintenance_status}\n\n"
        "Кнопка 'Оформить заказ' - добавить товар в Excel пошагово\n"
        "/users - список пользователей\n"
        "/orders - все заказы и их статусы\n"
        "/active - только активные заказы\n"
        "/tracks - заказы с трек-кодами\n"
        "/orderno номер - найти заказ по номеру\n"
        "/today - короткая сводка за сегодня\n"
        "/recent - последние 10 пользователей\n"
        "/search запрос - поиск по пользователям и заказам\n"
        "/grantsearch user_id [кол-во] - выдать попытки поиска\n"
        "/setsearch user_id кол-во - выставить точное число попыток\n"
        "/revokesearch user_id - удалить все попытки поиска\n"
        "/searchlist - у кого есть попытки поиска\n"
        "/user user_id - карточка пользователя\n"
        "/owner код - узнать владельца трек-кода\n"
        "/status user_id текст - вручную сменить статус\n"
        "/stats - статистика по боту\n"
        "/dashboard - быстрый дашборд по боту\n"
        "/note user_id текст - заметка по пользователю\n"
        "/clearnote user_id - очистить заметку\n"
        "/ban user_id причина - забанить пользователя\n"
        "/unban user_id - разбанить пользователя\n"
        "/banned - список забаненных пользователей\n"
        "/send user_id текст - отправить сообщение пользователю\n"
        "/broadcast текст - отправить сообщение всем пользователям\n"
        "/maintenance on|off - включить или выключить техобслуживание\n"
        "/excelorder - добавить товар в текущую таблицу карго\n"
        "/newexcelorder - начать новую таблицу карго\n"
        "/messageorder - оформить заказ и отправить его одним сообщением\n"
        "/excelfiles - показать все таблицы карго\n"
        "/activeexcel - показать текущую активную таблицу\n"
        "/setactiveexcel [номер|имя] - сделать таблицу текущей\n"
        "/closeexcel - закрыть текущую активную таблицу\n"
        "/excelview [номер|имя|current] - показать содержимое таблицы\n"
        "/exceldelete [номер|имя|current] - удалить таблицу карго\n"
        "/exceledit [номер|имя|current] строка поле значение - изменить товар в таблице\n"
        "/cargostats - короткая сводка по таблицам карго\n"
        "/randomuser - случайный пользователь\n"
        "/adminfun - случайная админ-фраза\n"
        "/done user_id - обработать заказ и выдать трек-код\n"
        "/cancel user_id - отменить заказ клиента\n"
        "/track код - посмотреть этап заказа\n"
        "/trackset код этап - изменить этап заказа\n"
        "/vykup код - быстро поставить этап 'Выкуплен'\n"
        "/sklad код - быстро поставить этап 'Приехал на склад'\n"
        "/otpravlen код - быстро поставить этап 'Отправлен'\n"
        "/rf код - быстро поставить этап 'Приехал в РФ'\n"
        "/find текст - поиск вещи по твоим ссылкам\n\n"
        "Поиск для клиентов:\n"
        "Поиск по фото доступен только админу.\n"
        "Обычным пользователям можно выдать платные попытки поиска по названию.\n\n"
        "Этапы доставки:\n"
        "1 - Выкуплен\n"
        "2 - Приехал на склад\n"
        "3 - Отправлен\n"
        "4 - Приехал в РФ"
    )


@dp.message(Command("excelorder"))
async def admin_excel_order_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    await start_admin_cargo_order_flow(message)


@dp.message(Command("newexcelorder"))
async def admin_new_excel_order_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    await start_admin_cargo_order_flow(message, force_new_workbook=True)


@dp.message(Command("messageorder"))
async def admin_message_order_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    await start_admin_message_order_flow(message)


@dp.message(Command("excelfiles"))
async def admin_excel_files_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    workbook_files = get_cargo_workbook_files()
    if not workbook_files:
        await message.answer("Таблиц карго пока нет.")
        return

    active_workbook = get_active_cargo_workbook(message.from_user.id) if message.from_user else None
    lines = ["Таблицы карго:", ""]
    for index, workbook_path in enumerate(workbook_files, start=1):
        modified_at = datetime.fromtimestamp(workbook_path.stat().st_mtime).strftime("%d.%m.%Y %H:%M")
        marker = " [текущая]" if active_workbook and workbook_path == active_workbook else ""
        lines.append(f"{index}. {workbook_path.name}{marker}")
        lines.append(f"Изменена: {modified_at}")

    lines.append("")
    lines.append("Чтобы посмотреть содержимое, используй /excelview номер")
    lines.append("Или /excelview current для текущей таблицы.")
    await send_long_text(message, "\n".join(lines))


@dp.message(Command("activeexcel"))
async def admin_active_excel_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    if not message.from_user:
        return

    workbook_path = get_active_cargo_workbook(message.from_user.id)
    if not workbook_path:
        await message.answer(
            "Текущая активная таблица не выбрана.\n"
            "Используй /setactiveexcel номер или /excelorder для создания/открытия таблицы."
        )
        return

    modified_at = datetime.fromtimestamp(workbook_path.stat().st_mtime).strftime("%d.%m.%Y %H:%M")
    await message.answer(
        "Текущая активная таблица:\n\n"
        f"Файл: {workbook_path.name}\n"
        f"Изменена: {modified_at}"
    )


@dp.message(Command("setactiveexcel"))
async def admin_set_active_excel_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Используй формат: /setactiveexcel номер|имя")
        return

    if not message.from_user:
        return

    workbook_path = resolve_cargo_workbook_reference(message.from_user.id, parts[1].strip())
    if not workbook_path:
        await message.answer("Таблица не найдена. Используй /excelfiles, чтобы посмотреть список.")
        return

    set_active_cargo_workbook(message.from_user.id, workbook_path)
    await message.answer(
        "Текущая таблица переключена.\n"
        f"Файл: {workbook_path.name}\n"
        "Теперь /excelorder будет дописывать товары именно в неё."
    )


@dp.message(Command("closeexcel"))
async def admin_close_excel_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    if not message.from_user:
        return

    workbook_path = clear_active_cargo_workbook(message.from_user.id)
    if not workbook_path:
        await message.answer("Сейчас нет активной таблицы, которую нужно закрыть.")
        return

    await message.answer(
        "Активная таблица закрыта.\n"
        f"Файл: {workbook_path.name}\n"
        "Следующий /excelorder создаст новую таблицу или попросит выбрать другую через /setactiveexcel."
    )


@dp.message(Command("excelview"))
async def admin_excel_view_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    raw_reference = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            raw_reference = parts[1]

    user_id = message.from_user.id if message.from_user else 0
    workbook_path = resolve_cargo_workbook_reference(user_id, raw_reference)
    if not workbook_path:
        await message.answer(
            "Не нашёл таблицу.\n"
            "Используй /excelfiles, чтобы посмотреть список.\n"
            "Для текущей таблицы можно написать /excelview current."
        )
        return

    try:
        preview_text = format_cargo_workbook_preview(workbook_path)
    except Exception as error:
        await message.answer(
            "Не удалось прочитать таблицу.\n"
            f"Ошибка: {error}"
        )
        return

    await send_long_text(message, preview_text)


@dp.message(Command("exceldelete"))
async def admin_excel_delete_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Используй формат: /exceldelete номер|имя|current")
        return

    user_id = message.from_user.id if message.from_user else 0
    workbook_path = resolve_cargo_workbook_reference(user_id, parts[1].strip())
    if not workbook_path:
        await message.answer("Таблица не найдена. Используй /excelfiles, чтобы посмотреть список.")
        return

    if not delete_cargo_workbook(user_id, workbook_path):
        await message.answer("Не удалось удалить таблицу.")
        return

    await message.answer(f"Таблица удалена: {workbook_path.name}")


@dp.message(Command("exceledit"))
async def admin_excel_edit_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=4)
    if len(parts) < 5:
        await message.answer(
            "Используй формат: /exceledit номер|имя|current строка поле значение\n"
            "Поля: name, size, color, link, price, quantity, delivery"
        )
        return

    reference = parts[1].strip()
    row_raw = parts[2].strip()
    field_name = parts[3].strip()
    value = parts[4].strip()

    try:
        row_number = int(row_raw)
    except ValueError:
        await message.answer("Номер строки должен быть числом.")
        return

    user_id = message.from_user.id if message.from_user else 0
    workbook_path = resolve_cargo_workbook_reference(user_id, reference)
    if not workbook_path:
        await message.answer("Таблица не найдена. Используй /excelfiles, чтобы посмотреть список.")
        return

    try:
        normalized_field = edit_cargo_workbook_item(
            workbook_path,
            row_number=row_number,
            field_name=field_name,
            value=value,
        )
    except Exception as error:
        await message.answer(
            "Не удалось изменить таблицу.\n"
            f"Ошибка: {error}"
        )
        return

    await message.answer(
        "Таблица обновлена.\n"
        f"Файл: {workbook_path.name}\n"
        f"Строка: {row_number}\n"
        f"Поле: {normalized_field}\n"
        f"Новое значение: {value}"
    )


@dp.message(Command("cargostats"))
async def admin_cargo_stats_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    await message.answer(get_cargo_tables_summary())


@dp.message(Command("dashboard"))
async def admin_dashboard_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    stats = get_admin_dashboard_stats()
    await message.answer(
        "Быстрый дашборд:\n\n"
        f"Всего пользователей: {stats['users_total']}\n"
        f"Всего заказов: {stats['orders_total']}\n"
        f"С трек-кодами: {stats['tracked_total']}\n"
        f"Пользователей с поиском: {stats['search_users_total']}\n"
        f"Забанено пользователей: {stats['banned_total']}\n"
        f"Таблиц карго: {stats['excel_total']}\n"
        f"Техобслуживание: {'включено' if is_maintenance_mode() else 'выключено'}"
    )


@dp.message(Command("randomuser"))
async def admin_random_user_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    user = get_random_user()
    if not user:
        await message.answer("Пользователей пока нет.")
        return

    await message.answer("Случайный пользователь:\n\n" + format_order_card(user, include_action_hint=True))


@dp.message(Command("adminfun"))
async def admin_fun_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    quote = random.choice(ADMIN_FUN_QUOTES)
    stats = get_admin_dashboard_stats()
    await message.answer(
        f"{quote}\n\n"
        f"На сейчас: пользователей {stats['users_total']}, заказов {stats['orders_total']}, таблиц {stats['excel_total']}."
    )


@dp.message(Command("ban"))
async def admin_ban_user(message: Message, bot: Bot) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Используй формат: /ban user_id причина")
        return

    try:
        user_id = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return

    if user_id == ADMIN_ID:
        await message.answer("Главного админа забанить нельзя.")
        return

    reason = parts[2].strip() if len(parts) >= 3 and parts[2].strip() else "Причина не указана"
    user = ban_user(user_id, reason)
    if not user:
        await message.answer("Пользователь не найден. Сначала он должен написать боту.")
        return

    try:
        await bot.send_message(
            user_id,
            "Тебе ограничили доступ к боту.\n"
            f"Причина: {reason}"
        )
    except Exception:
        pass

    await message.answer(
        f"Пользователь {user_id} забанен.\n"
        f"Причина: {reason}"
    )


@dp.message(Command("unban"))
async def admin_unban_user(message: Message, bot: Bot) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Используй формат: /unban user_id")
        return

    try:
        user_id = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return

    user = unban_user(user_id)
    if not user:
        await message.answer("Пользователь не найден.")
        return

    try:
        await bot.send_message(
            user_id,
            "Твой доступ к боту восстановлен. Можешь пользоваться им снова."
        )
    except Exception:
        pass

    await message.answer(f"Пользователь {user_id} разбанен.")


@dp.message(Command("banned"))
async def admin_banned_users(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    banned_users = get_banned_users()
    if not banned_users:
        await message.answer("Сейчас забаненных пользователей нет.")
        return

    lines = [format_order_card(user) for user in banned_users]
    await send_long_text(message, "Забаненные пользователи:\n\n" + "\n\n".join(lines))


@dp.message(Command("stats"))
async def admin_stats(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    users = load_users()
    total = len(users)
    orders = sum(1 for user in users.values() if user.get("order_number", 0))
    china = sum(1 for user in users.values() if user.get("country") == "Китай")
    sent = sum(1 for user in users.values() if user.get("status") == "Заявка отправлена")
    pending = sum(1 for user in users.values() if user.get("status") == "Отправляет данные")
    processed = sum(1 for user in users.values() if user.get("status") == "Заказ обработан")
    with_codes = sum(1 for user in users.values() if user.get("tracking_code"))
    paid_search = sum(1 for user in users.values() if int(user.get("search_access", 0) or 0) > 0)
    total_search_credits = sum(int(user.get("search_access", 0) or 0) for user in users.values())
    in_russia = sum(1 for user in users.values() if user.get("tracking_stage") == TRACKING_STAGES["4"])

    await message.answer(
        "Статистика:\n\n"
        f"Всего пользователей: {total}\n"
        f"Всего заказов: {orders}\n"
        f"Китай: {china}\n"
        f"Отправляют данные: {pending}\n"
        f"Заявка отправлена: {sent}\n"
        f"Заказ обработан: {processed}\n"
        f"С трек-кодом: {with_codes}\n"
        f"Пользователей с поиском: {paid_search}\n"
        f"Всего попыток поиска: {total_search_credits}\n"
        f"Приехали в РФ: {in_russia}"
    )


@dp.message(Command("maintenance"))
async def admin_maintenance(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        status = "включено" if is_maintenance_mode() else "выключено"
        await message.answer(
            "Используй формат: /maintenance on|off\n"
            f"Сейчас техобслуживание: {status}"
        )
        return

    mode = parts[1].strip().lower()
    if mode not in {"on", "off"}:
        await message.answer("Используй формат: /maintenance on|off")
        return

    enabled = mode == "on"
    set_maintenance_mode(enabled)
    await message.answer(
        "Техобслуживание включено. Теперь бот отвечает только админу."
        if enabled
        else "Техобслуживание выключено. Бот снова отвечает всем пользователям."
    )


@dp.message(Command("broadcast"))
async def admin_broadcast(message: Message, bot: Bot) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Используй формат: /broadcast текст")
        return

    users = load_users()
    recipients = [
        user["id"]
        for user in users.values()
        if int(user.get("id", 0) or 0) != ADMIN_ID
    ]
    if not recipients:
        await message.answer("Нет пользователей для рассылки.")
        return

    text = parts[1].strip()
    sent = 0
    failed = 0

    await message.answer(f"Начинаю рассылку. Получателей: {len(recipients)}")
    for user_id in recipients:
        try:
            await bot.send_message(user_id, text)
            sent += 1
        except Exception:
            failed += 1

    await message.answer(
        "Рассылка завершена.\n"
        f"Отправлено: {sent}\n"
        f"Не доставлено: {failed}"
    )


@dp.message(Command("pending"))
async def pending_orders(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    pending = get_pending_orders()
    if not pending:
        await message.answer("Неодобренных заказов сейчас нет.")
        return

    lines = [format_order_card(user, include_action_hint=True) for user in pending]
    await send_long_text(message, "Неодобренные заказы:\n\n" + "\n\n".join(lines))


@dp.message(Command("orders"))
async def all_orders(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    orders = get_all_orders()
    if not orders:
        await message.answer("Заказов пока нет.")
        return

    lines = [format_order_card(user, include_action_hint=True) for user in orders]
    await send_long_text(message, "Все заказы и их статусы:\n\n" + "\n\n".join(lines))


@dp.message(Command("active"))
async def active_orders(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    orders = get_active_orders()
    if not orders:
        await message.answer("Активных заказов сейчас нет.")
        return

    lines = [format_order_card(user, include_action_hint=True) for user in orders]
    await send_long_text(message, "Активные заказы:\n\n" + "\n\n".join(lines))


@dp.message(Command("tracks"))
async def tracked_orders(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    orders = get_tracked_orders()
    if not orders:
        await message.answer("Заказов с трек-кодами пока нет.")
        return

    lines = [format_order_card(user) for user in orders]
    await send_long_text(message, "Заказы с трек-кодами:\n\n" + "\n\n".join(lines))


@dp.message(Command("orderno"))
async def order_by_number(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Используй формат: /orderno номер")
        return

    try:
        order_number = int(parts[1].strip())
    except ValueError:
        await message.answer("Номер заказа должен быть числом.")
        return

    user = get_user_by_order_number(order_number)
    if not user:
        await message.answer("Заказ с таким номером не найден.")
        return

    await message.answer("Заказ найден:\n\n" + format_order_card(user, include_action_hint=True))


@dp.message(Command("today"))
async def today_summary(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    stats = get_today_stats()
    await message.answer(
        "Сегодня:\n\n"
        f"Новых пользователей: {stats['new_users']}\n"
        f"Активных сегодня: {stats['active_today']}\n"
        f"Заказов обновлялось сегодня: {stats['orders_today']}\n"
        f"Обработано сегодня: {stats['processed_today']}"
    )


@dp.message(Command("recent"))
async def recent_users(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    recent = get_recent_users()
    if not recent:
        await message.answer("Пока нет пользователей.")
        return

    lines = [format_order_card(user) for user in recent]
    await send_long_text(message, "Последние 10 пользователей:\n\n" + "\n\n".join(lines))


@dp.message(Command("search"))
async def admin_search_command(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Используй формат: /search запрос")
        return

    results = search_users(parts[1].strip())
    if not results:
        await message.answer("Ничего не найдено.")
        return

    lines = [format_order_card(user) for user in results]
    await send_long_text(message, "Результаты поиска:\n\n" + "\n\n".join(lines))


@dp.message(Command("grantsearch"))
async def admin_grant_search(message: Message, bot: Bot) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Используй формат: /grantsearch user_id [кол-во]")
        return

    try:
        user_id = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return

    amount = 1
    if len(parts) >= 3 and parts[2].strip():
        try:
            amount = int(parts[2].strip())
        except ValueError:
            await message.answer("Количество должно быть числом.")
            return
    if amount <= 0:
        await message.answer("Количество попыток должно быть больше нуля.")
        return

    total_credits = add_search_credits(user_id, amount)
    if total_credits is None:
        await message.answer("Пользователь не найден. Сначала он должен написать боту.")
        return

    try:
        await bot.send_message(
            user_id,
            "Тебе открыт доступ к платному поиску вещи в Китае.\n"
            f"Выдано попыток: {amount}\n"
            f"Всего попыток осталось: {total_credits}\n"
            "Теперь можно зайти в 'Сделать заказ' -> 'Найти эту вещь в Китае'.\n"
            "Поиск по фото доступен только админу, тебе доступен поиск по названию вещи."
        )
    except Exception:
        pass

    await message.answer(
        f"Пользователю {user_id} выдано попыток: {amount}\n"
        f"Всего попыток у него: {total_credits}"
    )


@dp.message(Command("setsearch"))
async def admin_set_search(message: Message, bot: Bot) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Используй формат: /setsearch user_id кол-во")
        return

    try:
        user_id = int(parts[1].strip())
        amount = int(parts[2].strip())
    except ValueError:
        await message.answer("user_id и количество должны быть числами.")
        return

    if amount < 0:
        await message.answer("Количество попыток не может быть отрицательным.")
        return

    remaining = set_search_credits(user_id, amount)
    if remaining is None:
        await message.answer("Пользователь не найден. Сначала он должен написать боту.")
        return

    if remaining <= 0:
        paid_search_users.discard(user_id)

    try:
        await bot.send_message(
            user_id,
            "Обновлен доступ к поиску вещи в Китае.\n"
            f"Сейчас попыток поиска: {remaining}"
        )
    except Exception:
        pass

    await message.answer(f"Пользователю {user_id} выставлено попыток поиска: {remaining}")


@dp.message(Command("revokesearch"))
async def admin_revoke_search(message: Message, bot: Bot) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Используй формат: /revokesearch user_id")
        return

    try:
        user_id = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return

    remaining = set_search_credits(user_id, 0)
    if remaining is None:
        await message.answer("Пользователь не найден.")
        return

    paid_search_users.discard(user_id)
    try:
        await bot.send_message(
            user_id,
            "Все попытки поиска вещи в Китае были удалены."
        )
    except Exception:
        pass

    await message.answer(f"Все попытки поиска удалены у пользователя {user_id}.")


@dp.message(Command("searchlist"))
async def admin_search_list(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    users = get_search_access_users()
    if not users:
        await message.answer("Сейчас ни у кого нет доступа к платному поиску.")
        return

    lines = [format_order_card(user) for user in users]
    await send_long_text(message, "Платные попытки поиска есть у:\n\n" + "\n\n".join(lines))


@dp.message(Command("user"))
async def admin_user_card(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Используй формат: /user user_id")
        return

    try:
        user_id = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return

    user = get_user(user_id)
    if not user:
        await message.answer("Пользователь не найден.")
        return

    username = f"@{user['username']}" if user.get("username") else "без username"
    full_name = " ".join(
        part for part in [user.get("first_name", ""), user.get("last_name", "")] if part
    ).strip() or "Без имени"

    await message.answer(
        "Карточка пользователя:\n\n"
        f"Номер заказа: {user.get('order_number', 0) or 'Нет'}\n"
        f"ID: {user['id']}\n"
        f"Имя: {full_name}\n"
        f"Username: {username}\n"
        f"Дата первого входа: {user.get('first_seen', '-')}\n"
        f"Последняя активность: {user.get('last_seen', '-')}\n"
        f"Страна: {user.get('country', '') or 'Не выбрана'}\n"
        f"Статус: {user.get('status', '') or 'Новый'}\n"
        f"Бан: {'Да' if user.get('banned') else 'Нет'}\n"
        f"Причина бана: {user.get('ban_reason', '') or 'Нет'}\n"
        f"Попыток поиска: {int(user.get('search_access', 0) or 0)}\n"
        f"Трек-код: {user.get('tracking_code', '') or 'Нет'}\n"
        f"Этап доставки: {user.get('tracking_stage', '') or 'Нет'}\n"
        f"Заметка: {user.get('note', '') or 'Нет'}"
    )


@dp.message(Command("owner"))
async def tracking_owner(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Используй формат: /owner код")
        return

    tracking_code = parts[1].strip().upper()
    _, user = get_user_by_tracking_code(tracking_code)
    if not user:
        await message.answer("Заказ с таким кодом не найден.")
        return

    await message.answer(
        "Владелец трек-кода:\n\n"
        f"{format_order_card(user)}"
    )


@dp.message(Command("note"))
async def admin_note(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Используй формат: /note user_id текст")
        return

    try:
        user_id = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return

    note = parts[2].strip()
    if not note:
        await message.answer("Заметка не должна быть пустой.")
        return

    if not set_user_note(user_id, note):
        await message.answer("Пользователь не найден.")
        return

    await message.answer(f"Заметка для пользователя {user_id} сохранена.")


@dp.message(Command("clearnote"))
async def admin_clear_note(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Используй формат: /clearnote user_id")
        return

    try:
        user_id = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return

    if not set_user_note(user_id, ""):
        await message.answer("Пользователь не найден.")
        return

    await message.answer(f"Заметка пользователя {user_id} очищена.")


@dp.message(Command("status"))
async def admin_status(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Используй формат: /status user_id новый_статус")
        return

    try:
        user_id = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return

    user = get_user(user_id)
    if not user:
        await message.answer("Пользователь не найден.")
        return

    new_status = parts[2].strip()
    if not new_status:
        await message.answer("Статус не должен быть пустым.")
        return

    set_user_status(user_id, new_status)
    await message.answer(f"Статус пользователя {user_id} обновлен: {new_status}")


@dp.message(Command("find"))
async def admin_find(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Используй формат: /find название вещи")
        return

    query = parts[1].strip()
    await message.answer("Ищу по твоим ссылкам, подожди немного...")
    results = await search_item_in_sources(query)
    await message.answer(build_result_text(results))


@dp.message(Command("track"))
async def track_order(message: Message) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Используй формат: /track код")
        return

    tracking_code = parts[1].strip().upper()
    _, user = get_user_by_tracking_code(tracking_code)
    if not user:
        await message.answer("Заказ с таким кодом не найден.")
        return

    await message.answer(
        f"Трек-код: {tracking_code}\n"
        f"Текущий этап: {user.get('tracking_stage', '') or 'Этап пока не назначен'}"
    )


@dp.message(Command("trackset"))
async def track_set(message: Message, bot: Bot) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Используй формат: /trackset код этап")
        return

    tracking_code = parts[1].strip().upper()
    stage_key = parts[2].strip()
    if stage_key not in TRACKING_STAGES:
        await message.answer(
            "Доступные этапы:\n"
            "1 - Выкуплен\n"
            "2 - Приехал на склад\n"
            "3 - Отправлен\n"
            "4 - Приехал в РФ"
        )
        return

    await apply_tracking_stage_change(message, bot, tracking_code, stage_key)


async def handle_quick_stage(message: Message, bot: Bot, stage_key: str) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Отправь команду с трек-кодом.")
        return

    tracking_code = parts[1].strip().upper()
    await apply_tracking_stage_change(message, bot, tracking_code, stage_key)


@dp.message(Command("vykup"))
async def quick_vykup(message: Message, bot: Bot) -> None:
    await handle_quick_stage(message, bot, "1")


@dp.message(Command("sklad"))
async def quick_sklad(message: Message, bot: Bot) -> None:
    await handle_quick_stage(message, bot, "2")


@dp.message(Command("otpravlen"))
async def quick_otpravlen(message: Message, bot: Bot) -> None:
    await handle_quick_stage(message, bot, "3")


@dp.message(Command("rf"))
async def quick_rf(message: Message, bot: Bot) -> None:
    await handle_quick_stage(message, bot, "4")


@dp.message(Command("users"))
async def list_users(message: Message) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    users = load_users()
    if not users:
        await message.answer("Пока нет пользователей, которые писали боту.")
        return

    lines = []
    for user in sorted(users.values(), key=lambda item: item.get("last_seen", ""), reverse=True):
        username = f"@{user['username']}" if user.get("username") else "без username"
        full_name = " ".join(
            part for part in [user.get("first_name", ""), user.get("last_name", "")] if part
        ).strip() or "Без имени"
        lines.append(
            f"Заказ №{user.get('order_number', 0) or '-'} | {user['id']} | {full_name} | {username}\n"
            f"Дата: {user.get('first_seen', '-')}\n"
            f"Страна: {user.get('country', '') or 'Не выбрана'}\n"
            f"Статус: {user.get('status', '') or 'Новый'}\n"
            f"Попыток поиска: {int(user.get('search_access', 0) or 0)}\n"
            f"Трек-код: {user.get('tracking_code', '') or 'Нет'}\n"
            f"Этап: {user.get('tracking_stage', '') or 'Нет'}"
        )

    await send_long_text(message, "Пользователи:\n\n" + "\n\n".join(lines))


@dp.message(Command("send"))
async def send_to_user(message: Message, bot: Bot) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Используй формат: /send user_id текст")
        return

    try:
        user_id = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return

    try:
        await bot.send_message(user_id, parts[2].strip())
        await message.answer(f"Сообщение отправлено пользователю {user_id}.")
    except Exception as error:
        await message.answer(f"Не удалось отправить сообщение: {error}")


@dp.message(Command("done"))
async def send_done_message(message: Message, bot: Bot) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Используй формат: /done user_id")
        return

    try:
        user_id = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return

    user = get_user(user_id)
    if not can_approve_order(user):
        await message.answer(
            "Нельзя одобрить этот заказ. Код выдается только если пользователь уже отправил заявку."
        )
        return

    try:
        tracking_code = assign_tracking_code(user_id)
        set_user_status(user_id, "Заказ обработан")
        await bot.send_message(
            user_id,
            "Ваш заказ обработан.\n"
            f"Ваш код для отслеживания заказа: {tracking_code}\n"
            f"Текущий этап: {TRACKING_STAGES['1']}"
        )
        await message.answer(
            f"Заказ обработан.\n"
            f"Код: {tracking_code}\n"
            f"Пользователь: {user_id}"
        )
    except Exception as error:
        await message.answer(f"Не удалось отправить сообщение: {error}")


@dp.message(Command("cancel"))
async def cancel_order(message: Message, bot: Bot) -> None:
    if not is_admin(message):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Используй формат: /cancel user_id")
        return

    try:
        user_id = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return

    user = get_user(user_id)
    if not can_cancel_order(user):
        await message.answer("Этот заказ нельзя отменить или пользователь не найден.")
        return

    order_number = user.get("order_number", 0) if user else 0
    tracking_code = user.get("tracking_code", "") if user else ""
    canceled_user = cancel_user_order(user_id)
    china_order_users.discard(user_id)
    admin_search_users.discard(user_id)
    tracking_lookup_users.discard(user_id)

    if not canceled_user:
        await message.answer("Не удалось отменить заказ.")
        return

    try:
        lines = ["Ваш заказ отменен."]
        if order_number:
            lines.append(f"Номер заказа: {order_number}")
        if tracking_code:
            lines.append(f"Трек-код: {tracking_code}")

        await bot.send_message(
            user_id,
            "\n".join(lines),
            reply_markup=user_main_keyboard,
        )
    except Exception:
        pass

    await message.answer(
        "Заказ отменен.\n"
        f"Пользователь: {user_id}\n"
        f"Номер заказа: {order_number or 'Нет'}\n"
        f"Трек-код: {tracking_code or 'Нет'}"
    )


@dp.message(F.text == "Неодобренные заказы")
async def pending_orders_button(message: Message) -> None:
    if not is_admin(message):
        return

    clear_admin_cargo_mode(message)
    pending = get_pending_orders()
    if not pending:
        await message.answer("Неодобренных заказов сейчас нет.")
        return

    lines = [format_order_card(user, include_action_hint=True) for user in pending]
    await send_long_text(message, "Неодобренные заказы:\n\n" + "\n\n".join(lines))


@dp.message(F.text == "Все заказы и их статусы")
async def all_orders_button(message: Message) -> None:
    if not is_admin(message):
        return

    clear_admin_cargo_mode(message)
    orders = get_all_orders()
    if not orders:
        await message.answer("Заказов пока нет.")
        return

    lines = [format_order_card(user, include_action_hint=True) for user in orders]
    await send_long_text(message, "Все заказы и их статусы:\n\n" + "\n\n".join(lines))


@dp.message(F.text == "Оформить заказ")
async def admin_excel_order_button(message: Message) -> None:
    if not is_admin(message):
        return

    await start_admin_cargo_order_flow(message)


@dp.message(F.text == "Сделать заказ")
async def make_order(message: Message) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return
    clear_admin_cargo_mode(message)
    update_user(message, status="Выбирает раздел заказа")
    await send_photo_or_text(
        message,
        ORDER_IMAGE,
        "Выбери страну, из которой хочешь сделать заказ:",
        reply_markup=user_order_keyboard,
    )


@dp.message(F.text == "Найти эту вещь в Китае")
async def search_item_tab(message: Message) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return

    clear_admin_cargo_mode(message)
    user = update_user(message)
    if not message.from_user or not user:
        return

    user_id = message.from_user.id
    china_order_users.discard(user_id)
    tracking_lookup_users.discard(user_id)

    if is_admin(message):
        paid_search_users.discard(user_id)
        admin_search_users.add(user_id)
        await message.answer(
            "Пришли название вещи текстом или фото с подписью для поиска по твоим ссылкам.\n"
            "Чтобы выйти из режима поиска, нажми 'Назад'."
        )
        return

    admin_search_users.discard(user_id)
    credits = int(user.get("search_access", 0) or 0)
    if credits <= 0:
        await message.answer(
            "Поиск вещи в Китае для пользователей доступен только после оплаты.\n"
            "Чтобы получить доступ, напиши в техподдержку: @ichov",
            reply_markup=current_main_keyboard(message),
        )
        return

    paid_search_users.add(user_id)
    await message.answer(
        "Доступ к поиску активирован.\n"
        f"Осталось попыток поиска: {credits}\n"
        "Пришли название вещи текстом, и я начну поиск.\n"
        "Поиск по фото доступен только админу.\n"
        "Чтобы выйти из режима поиска, нажми 'Назад'."
    )


@dp.message(F.text == "Тарифы")
async def tariffs(message: Message) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return
    clear_admin_cargo_mode(message)
    update_user(message)
    await send_photo_or_text(
        message,
        TARIFF_IMAGE,
        "Пока что существует 1 единственный тариф, это 20-30 дней "
        "с момента отправки со склада - 6 долларов за кг",
    )


@dp.message(F.text == "Инфо")
async def info(message: Message) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return
    clear_admin_cargo_mode(message)
    update_user(message)
    tracking_lookup_users.discard(message.from_user.id if message.from_user else -1)
    await send_photo_or_text(
        message,
        INFO_IMAGE,
        "Фотоотчет отправляется прямо в бот\n" 
        "время работы бота 8 00 - 20 00(временно)\n" 
        "а также ты здесь можешь узнать статус заказа,\n"
        "посмотреть свой заказ и остаток попыток поиска",
        reply_markup=info_keyboard,
    )


@dp.message(F.text == "Мой заказ")
async def my_order(message: Message) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return
    update_user(message)
    if not message.from_user:
        return

    user = get_user(message.from_user.id)
    if not user:
        await message.answer("Пока нет данных по твоему заказу.", reply_markup=info_keyboard)
        return

    has_order = bool(
        user.get("order_number", 0)
        or user.get("tracking_code")
        or user.get("status") in ACTIVE_ORDER_STATUSES
    )
    if not has_order:
        await message.answer(
            "У тебя пока нет активного заказа.\n"
            "Если хочешь оформить заказ, зайди в раздел 'Сделать заказ'.",
            reply_markup=info_keyboard,
        )
        return

    await message.answer(
        "Твой заказ:\n\n"
        f"Номер заказа: {user.get('order_number', 0) or 'Еще не присвоен'}\n"
        f"Статус: {user.get('status', '') or 'Новый'}\n"
        f"Трек-код: {user.get('tracking_code', '') or 'Пока нет'}\n"
        f"Этап: {user.get('tracking_stage', '') or 'Пока не назначен'}",
        reply_markup=info_keyboard,
    )


@dp.message(F.text == "Мои попытки поиска")
async def my_search_attempts(message: Message) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return
    update_user(message)
    if not message.from_user:
        return

    if is_admin(message):
        await message.answer(
            "У тебя как у админа поиск по фото и по названию доступен без ограничений.",
            reply_markup=info_keyboard,
        )
        return

    user = get_user(message.from_user.id)
    credits = int((user or {}).get("search_access", 0) or 0)
    if credits <= 0:
        await message.answer(
            "У тебя сейчас нет попыток поиска.\n"
            "Чтобы получить платный доступ, напиши в техподдержку: @ichov",
            reply_markup=info_keyboard,
        )
        return

    await message.answer(
        f"У тебя осталось попыток поиска: {credits}\n"
        "Использовать их можно в разделе 'Сделать заказ' -> 'Найти эту вещь в Китае'.",
        reply_markup=info_keyboard,
    )


@dp.message(F.text == "Узнать статус заказа")
async def ask_tracking_code(message: Message) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return
    update_user(message)
    if message.from_user:
        tracking_lookup_users.add(message.from_user.id)
    await message.answer(
        "Отправь свой трек-код, и я покажу текущий статус заказа.",
        reply_markup=info_keyboard,
    )


@dp.message(F.text == "Тех. поддержка")
async def support(message: Message) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return
    clear_admin_cargo_mode(message)
    update_user(message)
    await send_photo_or_text(
        message,
        SUPPORT_IMAGE,
        "По всем вопросам техподдержки пиши: @ichov",
    )


@dp.message(F.text == "Китай")
async def china_order(message: Message, bot: Bot) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return
    clear_admin_cargo_mode(message)
    user = update_user(message, country="Китай", status="Оформляет заказ")
    if not message.from_user or not user:
        return

    china_order_users.add(message.from_user.id)
    tracking_lookup_users.discard(message.from_user.id)
    await message.answer(
        "Тогда скидывай ссылку товара, фото товара, размер, цвет.",
        reply_markup=china_submit_keyboard,
    )
    await bot.send_message(
        ADMIN_ID,
        f"Пользователь {message.from_user.full_name} "
        f"(ID: {message.from_user.id}) начал заказ из Китая.",
    )


@dp.message(F.text.in_(["Сша", "Корея"]))
async def other_country_order(message: Message) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return
    clear_admin_cargo_mode(message)
    update_user(message, country=message.text, status="Временно закрыто")
    await message.answer("Временно закрыто.", reply_markup=current_main_keyboard(message))


@dp.message(F.text == "Я все скинул")
async def finish_china_order(message: Message, bot: Bot) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return
    if not message.from_user or message.from_user.id not in china_order_users:
        update_user(message)
        await message.answer("Сначала выбери раздел 'Сделать заказ' и затем 'Китай'.")
        return

    order_number = assign_order_number(message.from_user.id)
    update_user(message, country="Китай", status="Заявка отправлена")
    china_order_users.discard(message.from_user.id)
    await message.answer(
        f"Готово, ожидайте. Ваш номер заказа: {order_number}",
        reply_markup=current_main_keyboard(message),
    )
    await bot.send_message(
        ADMIN_ID,
        f"Заказ №{order_number}\n"
        f"Пользователь {message.from_user.full_name} "
        f"(ID: {message.from_user.id}) завершил отправку данных по заказу из Китая.",
    )


@dp.message(F.text == "Назад")
async def back_to_main_menu(message: Message) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return
    update_user(message, status="В главном меню")
    if message.from_user:
        china_order_users.discard(message.from_user.id)
        admin_search_users.discard(message.from_user.id)
        paid_search_users.discard(message.from_user.id)
        tracking_lookup_users.discard(message.from_user.id)
        reset_admin_cargo_order_state(message.from_user.id)
    user_name = message.from_user.first_name if message.from_user else "друг"
    await send_main_menu(message, user_name)


@dp.message()
async def handle_messages(message: Message, bot: Bot) -> None:
    if await maintenance_guard(message):
        return
    if is_rate_limited(message):
        await maybe_send_rate_limit_notice(message)
        return

    update_user(message)

    if not message.from_user:
        return

    if await handle_admin_message_order_step(message, bot):
        return

    if await handle_admin_cargo_order_step(message, bot):
        return

    if message.from_user.id in admin_search_users and is_admin(message):
        query = (message.text or message.caption or "").strip()
        if not query:
            await message.answer("Для поиска пришли название вещи текстом или фото с подписью.")
            return

        await message.answer("Ищу по твоим ссылкам, подожди немного...")
        results = await search_item_in_sources(query)
        await message.answer(build_result_text(results))
        return

    if message.from_user.id in paid_search_users:
        user = get_user(message.from_user.id)
        credits = int((user or {}).get("search_access", 0) or 0)
        if credits <= 0:
            paid_search_users.discard(message.from_user.id)
            await message.answer(
                "У тебя закончились попытки поиска.\n"
                "Чтобы получить новые, напиши в техподдержку: @ichov",
                reply_markup=current_main_keyboard(message),
            )
            return

        if message.photo:
            await message.answer("Поиск по фото доступен только админу. Пришли название вещи текстом.")
            return

        query = (message.text or "").strip()
        if not query:
            await message.answer("Для поиска пришли название вещи текстом.")
            return

        remaining = consume_search_credit(message.from_user.id)
        if remaining is None:
            paid_search_users.discard(message.from_user.id)
            await message.answer(
                "У тебя закончились попытки поиска.\n"
                "Чтобы получить новые, напиши в техподдержку: @ichov",
                reply_markup=current_main_keyboard(message),
            )
            return

        update_user(message, status="Ищет вещь в Китае")
        await message.answer("Ищу по твоим ссылкам, подожди немного...")
        results = await search_item_in_sources(query)
        await message.answer(
            build_result_text(results)
            + f"\n\nОсталось попыток поиска: {remaining}"
        )
        if remaining <= 0:
            paid_search_users.discard(message.from_user.id)
        return

    if message.from_user.id in tracking_lookup_users:
        tracking_code = (message.text or "").strip().upper()
        _, user = get_user_by_tracking_code(tracking_code)
        if not tracking_code:
            await message.answer("Отправь трек-код текстом.")
            return
        if not user:
            await message.answer("Заказ с таким кодом не найден.")
            return

        await message.answer(
            f"Трек-код: {tracking_code}\n"
            f"Текущий этап: {user.get('tracking_stage', '') or 'Этап пока не назначен'}",
            reply_markup=info_keyboard,
        )
        return

    if is_admin(message):
        if message.text:
            await message.answer(
                "Для работы используй команды:\n"
                "/send user_id текст\n"
                "/done user_id\n"
                "/cancel user_id\n"
                "/orders\n"
                "/active\n"
                "/tracks\n"
                "/orderno номер\n"
                "/today\n"
                "/recent\n"
                "/search запрос\n"
                "/grantsearch user_id [кол-во]\n"
                "/setsearch user_id кол-во\n"
                "/revokesearch user_id\n"
                "/searchlist\n"
                "/status user_id статус\n"
                "/dashboard\n"
                "/ban user_id причина\n"
                "/unban user_id\n"
                "/banned\n"
                "/owner код\n"
                "/broadcast текст\n"
                "/maintenance on|off\n"
                "/excelorder\n"
                "/newexcelorder\n"
                "/messageorder\n"
                "/excelfiles\n"
                "/activeexcel\n"
                "/setactiveexcel номер\n"
                "/closeexcel\n"
                "/excelview номер\n"
                "/exceldelete номер\n"
                "/exceledit current строка поле значение\n"
                "/cargostats\n"
                "/randomuser\n"
                "/adminfun\n"
                "/find название вещи\n"
                "/track код\n"
                "/trackset код этап\n"
                "/vykup код\n"
                "/sklad код\n"
                "/otpravlen код\n"
                "/rf код"
            )
        return

    if message.from_user.id in china_order_users:
        update_user(message, country="Китай", status="Отправляет данные")
        await notify_admin(bot, message)
        await bot.forward_message(ADMIN_ID, message.chat.id, message.message_id)
        await message.answer(
            "Принял. Если все отправил, нажми кнопку 'Я все скинул'.",
            reply_markup=china_submit_keyboard,
        )


async def main() -> None:
    init_db()
    migrate_legacy_users()
    purge_canceled_orders()
    ensure_storage_dirs()
    bot = Bot(token=TOKEN)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
