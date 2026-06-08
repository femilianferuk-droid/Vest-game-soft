import asyncio
import logging
import os
import random
import re
from datetime import datetime
from typing import Optional, List, Dict, Any

import asyncpg
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.methods import DeleteWebhook
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Переменные окружения ---
DATABASE_URL = os.getenv('DATABASE_URL')
BOT_TOKEN = os.getenv('BOT_TOKEN')
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')

ADMIN_IDS = [7973988177]

# --- Инициализация бота и диспетчера ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# --- Пул соединений с БД ---
db_pool: Optional[asyncpg.Pool] = None

# --- Хранилище активных Telethon клиентов ---
active_clients: Dict[int, TelegramClient] = {}
active_auto_responders: Dict[int, Dict[int, asyncio.Task]] = {}
active_broadcasts: Dict[int, asyncio.Task] = {}
broadcast_stop_flags: Dict[int, bool] = {}

# --- ID премиум эмодзи ---
class Emoji:
    SETTINGS = "5870982283724328568"
    PROFILE = "5870994129244131212"
    PEOPLE = "5870772616305839506"
    USER_CHECK = "5891207662678317861"
    USER_CROSS = "5893192487324880883"
    FILE = "5870528606328852614"
    SMILE = "5870764288364252592"
    CHART_UP = "5870930636742595124"
    CHART = "5870921681735781843"
    HOUSE = "5873147866364514353"
    LOCK_CLOSED = "6037249452824072506"
    LOCK_OPEN = "6037496202990194718"
    MEGAPHONE = "6039422865189638057"
    CHECK = "5870633910337015697"
    CROSS = "5870657884844462243"
    PENCIL = "5870676941614354370"
    TRASH = "5870875489362513438"
    DOWN = "5893057118545646106"
    PAPERCLIP = "6039451237743595514"
    LINK = "5769289093221454192"
    INFO = "6028435952299413210"
    BOT = "6030400221232501136"
    EYE = "6037397706505195857"
    EYE_OFF = "6037243349675544634"
    SEND = "5963103826075456248"
    DOWNLOAD = "6039802767931871481"
    BELL = "6039486778597970865"
    GIFT = "6032644646587338669"
    CLOCK = "5983150113483134607"
    PARTY = "6041731551845159060"
    FONT = "5870801517140775623"
    WRITE = "5870753782874246579"
    MEDIA = "6035128606563241721"
    LOCATION = "6042011682497106307"
    WALLET = "5769126056262898415"
    BOX = "5884479287171485878"
    CRYPTO_BOT = "5260752406890711732"
    CALENDAR = "5890937706803894250"
    TAG = "5886285355279193209"
    TIME_PAST = "5775896410780079073"
    APPS = "5778672437122045013"
    BRUSH = "6050679691004612757"
    ADD_TEXT = "5771851822897566479"
    FORMAT = "5778479949572738874"
    MONEY = "5904462880941545555"
    MONEY_SEND = "5890848474563352982"
    MONEY_ACCEPT = "5879814368572478751"
    CODE = "5940433880585605708"
    LOADING = "5345906554510012647"
    BACK = "5775417808636156714"
    STATS = "5870921681735781843"
    PLAY = "6041731551845159060"
    STOP = "6037249452824072506"
    REFRESH = "5345906554510012647"
    DELETE = "5870875489362513438"
    PHONE = "5870994129244131212"
    GEAR = "5870982283724328568"
    MAIL = "5963103826075456248"

def e(id: str) -> str:
    """Форматирует премиум эмодзи"""
    return f'<tg-emoji emoji-id="{id}">...</tg-emoji>'

# --- Состояния FSM ---
class AccountStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_2fa = State()

class BroadcastStates(StatesGroup):
    waiting_for_account = State()
    selecting_chats = State()
    waiting_for_delay = State()
    waiting_for_count = State()
    waiting_for_message = State()
    preview = State()

class AutoResponderStates(StatesGroup):
    waiting_for_account = State()
    waiting_for_trigger = State()
    waiting_for_response = State()
    preview = State()

class AdminStates(StatesGroup):
    waiting_for_broadcast_message = State()

# --- Инициализация БД ---
async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    
    async with db_pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                joined_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                phone TEXT NOT NULL,
                session_string TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS broadcasts (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                account_id INTEGER REFERENCES accounts(id),
                chat_ids TEXT[] NOT NULL,
                delay INTEGER NOT NULL,
                message_count INTEGER NOT NULL,
                message_text TEXT,
                message_media TEXT[] DEFAULT '{}',
                mode TEXT NOT NULL DEFAULT 'simultaneous',
                status TEXT NOT NULL DEFAULT 'active',
                progress INTEGER DEFAULT 0,
                total_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW(),
                started_at TIMESTAMP,
                stopped_at TIMESTAMP
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS auto_responders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                account_id INTEGER REFERENCES accounts(id),
                trigger TEXT NOT NULL,
                response_text TEXT,
                response_media TEXT[] DEFAULT '{}',
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')

# --- Регистрация пользователя ---
async def register_user(user_id: int, username: str, first_name: str):
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO users (user_id, username, first_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE 
            SET username = $2, first_name = $3
        ''', user_id, username, first_name)

# --- Вспомогательные функции ---
async def get_user_accounts(user_id: int) -> List[Dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT id, phone, is_active FROM accounts WHERE user_id = $1',
            user_id
        )
        return [dict(row) for row in rows]

async def get_account(account_id: int) -> Optional[Dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM accounts WHERE id = $1', account_id)
        return dict(row) if row else None

async def delete_account(account_id: int) -> bool:
    async with db_pool.acquire() as conn:
        result = await conn.execute('DELETE FROM accounts WHERE id = $1', account_id)
        return result != "DELETE 0"

async def create_telethon_client(session_string: str) -> TelegramClient:
    return TelegramClient(StringSession(session_string), API_ID, API_HASH)

async def get_client_for_account(account_id: int) -> Optional[TelegramClient]:
    if account_id in active_clients:
        return active_clients[account_id]
    
    account = await get_account(account_id)
    if not account:
        return None
    
    client = await create_telethon_client(account['session_string'])
    await client.connect()
    
    if await client.is_user_authorized():
        active_clients[account_id] = client
        return client
    else:
        await client.disconnect()
        return None

async def get_chats_from_client(client: TelegramClient, limit: int = 200) -> List[Dict]:
    chats = []
    async for dialog in client.iter_dialogs(limit=limit):
        if dialog.is_user or dialog.is_group or dialog.is_channel:
            entity = dialog.entity
            chat_info = {
                'id': dialog.id,
                'name': dialog.name,
                'type': 'user' if dialog.is_user else 'group' if dialog.is_group else 'channel'
            }
            chats.append(chat_info)
    return chats

async def send_message_to_chat(client: TelegramClient, chat_id: int, text: str, media_paths: List[str] = None):
    try:
        if media_paths:
            if len(media_paths) == 1:
                await client.send_file(chat_id, media_paths[0], caption=text, parse_mode='html')
            else:
                await client.send_file(chat_id, media_paths, caption=text, parse_mode='html')
        else:
            await client.send_message(chat_id, text, parse_mode='html')
        return True
    except Exception as e:
        logger.error(f"Error sending message to {chat_id}: {e}")
        return False

async def get_broadcast_stats():
    async with db_pool.acquire() as conn:
        total_users = await conn.fetchval('SELECT COUNT(*) FROM users')
        total_broadcasts = await conn.fetchval('SELECT COUNT(*) FROM broadcasts')
        active_broadcasts_count = await conn.fetchval("SELECT COUNT(*) FROM broadcasts WHERE status = 'active'")
        total_accounts = await conn.fetchval('SELECT COUNT(*) FROM accounts')
        return {
            'total_users': total_users,
            'total_broadcasts': total_broadcasts,
            'active_broadcasts': active_broadcasts_count,
            'total_accounts': total_accounts
        }

async def get_user_broadcasts(user_id: int) -> List[Dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT * FROM broadcasts WHERE user_id = $1 ORDER BY created_at DESC',
            user_id
        )
        return [dict(row) for row in rows]

async def get_user_auto_responders(user_id: int) -> List[Dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT * FROM auto_responders WHERE user_id = $1 ORDER BY created_at DESC',
            user_id
        )
        return [dict(row) for row in rows]

async def get_auto_responder(responder_id: int) -> Optional[Dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM auto_responders WHERE id = $1', responder_id)
        return dict(row) if row else None

async def start_auto_responder(responder_id: int, user_id: int):
    responder = await get_auto_responder(responder_id)
    if not responder or not responder['is_active']:
        return
    
    account_id = responder['account_id']
    
    if user_id in active_auto_responders and account_id in active_auto_responders[user_id]:
        active_auto_responders[user_id][account_id].cancel()
    
    task = asyncio.create_task(auto_responder_worker(responder))
    if user_id not in active_auto_responders:
        active_auto_responders[user_id] = {}
    active_auto_responders[user_id][account_id] = task

async def auto_responder_worker(responder: Dict):
    account_id = responder['account_id']
    trigger = responder['trigger']
    response_text = responder['response_text']
    response_media = responder.get('response_media', [])
    
    client = await get_client_for_account(account_id)
    if not client:
        return
    
    @client.on(events.NewMessage(incoming=True))
    async def handler(event):
        if event.is_private:
            message_text = event.message.text or ""
            if trigger == "-" or trigger.lower() in message_text.lower():
                try:
                    if response_media:
                        if len(response_media) == 1:
                            await client.send_file(event.chat_id, response_media[0], caption=response_text, parse_mode='html')
                        else:
                            await client.send_file(event.chat_id, response_media, caption=response_text, parse_mode='html')
                    else:
                        await client.send_message(event.chat_id, response_text, parse_mode='html')
                except Exception as e:
                    logger.error(f"Auto responder error: {e}")
    
    try:
        while True:
            if not client.is_connected():
                break
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        client.remove_event_handler(handler)

async def execute_broadcast(broadcast_id: int, user_id: int):
    """Выполняет рассылку"""
    async with db_pool.acquire() as conn:
        broadcast = await conn.fetchrow('SELECT * FROM broadcasts WHERE id = $1', broadcast_id)
        if not broadcast:
            return
        
        broadcast = dict(broadcast)
    
    account_id = broadcast['account_id']
    chat_ids = broadcast['chat_ids']
    delay = broadcast['delay']
    message_count = broadcast['message_count']
    message_text = broadcast['message_text']
    message_media = broadcast.get('message_media', [])
    mode = broadcast['mode']
    
    client = await get_client_for_account(account_id)
    if not client:
        return
    
    broadcast_stop_flags[broadcast_id] = False
    
    total_messages = len(chat_ids) * message_count
    sent = 0
    
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE broadcasts SET status = 'active', started_at = NOW(), total_count = $1 WHERE id = $2",
            total_messages, broadcast_id
        )
    
    try:
        for msg_num in range(message_count):
            if broadcast_stop_flags.get(broadcast_id, False):
                break
            
            if mode == 'simultaneous':
                tasks = []
                for chat_id in chat_ids:
                    task = asyncio.create_task(
                        send_message_to_chat(client, chat_id, message_text, message_media)
                    )
                    tasks.append(task)
                await asyncio.gather(*tasks)
                sent += len(chat_ids)
                
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE broadcasts SET progress = $1 WHERE id = $2",
                        sent, broadcast_id
                    )
                
                if msg_num < message_count - 1:
                    await asyncio.sleep(delay)
                    
            else:
                for chat_id in chat_ids:
                    if broadcast_stop_flags.get(broadcast_id, False):
                        break
                    
                    random_chat = random.choice(chat_ids)
                    await send_message_to_chat(client, random_chat, message_text, message_media)
                    sent += 1
                    
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE broadcasts SET progress = $1 WHERE id = $2",
                            sent, broadcast_id
                        )
                    
                    await asyncio.sleep(delay)
        
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE broadcasts SET status = 'completed', stopped_at = NOW(), progress = $1 WHERE id = $2",
                sent, broadcast_id
            )
            
    except Exception as e:
        logger.error(f"Broadcast error: {e}")
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE broadcasts SET status = 'stopped', stopped_at = NOW() WHERE id = $1",
                broadcast_id
            )

# --- Клавиатуры ---
def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Менеджер аккаунтов",
            callback_data="account_manager",
            style='primary',
            icon_custom_emoji_id=Emoji.PEOPLE
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Функции",
            callback_data="functions",
            style='primary',
            icon_custom_emoji_id=Emoji.APPS
        )
    )
    return builder.as_markup()

def get_account_manager_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Добавить аккаунт",
            callback_data="add_account",
            style='primary',
            icon_custom_emoji_id=Emoji.ADD_TEXT
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Мои аккаунты",
            callback_data="my_accounts",
            style='primary',
            icon_custom_emoji_id=Emoji.PEOPLE
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="main_menu",
            style='default',
            icon_custom_emoji_id=Emoji.BACK
        )
    )
    return builder.as_markup()

def get_functions_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Рассылка",
            callback_data="broadcast",
            style='primary',
            icon_custom_emoji_id=Emoji.SEND
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Автоответчик",
            callback_data="auto_responder",
            style='primary',
            icon_custom_emoji_id=Emoji.BELL
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Мои рассылки",
            callback_data="my_broadcasts",
            style='default',
            icon_custom_emoji_id=Emoji.CHART
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="main_menu",
            style='default',
            icon_custom_emoji_id=Emoji.BACK
        )
    )
    return builder.as_markup()

def get_broadcast_mode_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Одновременный",
            callback_data="mode_simultaneous",
            style='primary',
            icon_custom_emoji_id=Emoji.MONEY_SEND
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Рандомный",
            callback_data="mode_random",
            style='primary',
            icon_custom_emoji_id=Emoji.TIME_PAST
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="functions",
            style='default',
            icon_custom_emoji_id=Emoji.BACK
        )
    )
    return builder.as_markup()

def get_broadcast_preview_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Запустить",
            callback_data="start_broadcast",
            style='success',
            icon_custom_emoji_id=Emoji.PLAY
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Отмена",
            callback_data="functions",
            style='danger',
            icon_custom_emoji_id=Emoji.CROSS
        )
    )
    return builder.as_markup()

def get_broadcast_control_keyboard(broadcast_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Остановить",
            callback_data=f"stop_broadcast_{broadcast_id}",
            style='danger',
            icon_custom_emoji_id=Emoji.STOP
        ),
        InlineKeyboardButton(
            text="Возобновить",
            callback_data=f"resume_broadcast_{broadcast_id}",
            style='success',
            icon_custom_emoji_id=Emoji.PLAY
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Удалить",
            callback_data=f"delete_broadcast_{broadcast_id}",
            style='default',
            icon_custom_emoji_id=Emoji.DELETE
        )
    )
    return builder.as_markup()

def get_accounts_list_keyboard(accounts: List[Dict], callback_prefix: str = "select_broadcast_account") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for acc in accounts:
        builder.row(
            InlineKeyboardButton(
                text=f"{acc['phone']} {'✅' if acc['is_active'] else '❌'}",
                callback_data=f"{callback_prefix}_{acc['id']}",
                style='default',
                icon_custom_emoji_id=Emoji.PROFILE
            )
        )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="main_menu",
            style='default',
            icon_custom_emoji_id=Emoji.BACK
        )
    )
    return builder.as_markup()

def get_account_actions_keyboard(account_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Удалить аккаунт",
            callback_data=f"delete_account_{account_id}",
            style='danger',
            icon_custom_emoji_id=Emoji.DELETE
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="my_accounts",
            style='default',
            icon_custom_emoji_id=Emoji.BACK
        )
    )
    return builder.as_markup()

def get_auto_responder_preview_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Создать",
            callback_data="create_auto_responder",
            style='success',
            icon_custom_emoji_id=Emoji.PLAY
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Отмена",
            callback_data="functions",
            style='danger',
            icon_custom_emoji_id=Emoji.CROSS
        )
    )
    return builder.as_markup()

def get_chat_selection_keyboard(chats: List[Dict], page: int = 0, selected_chats: List[int] = None) -> InlineKeyboardMarkup:
    if selected_chats is None:
        selected_chats = []
    
    builder = InlineKeyboardBuilder()
    per_page = 10
    start_idx = page * per_page
    end_idx = start_idx + per_page
    page_chats = chats[start_idx:end_idx]
    
    for chat in page_chats:
        is_selected = chat['id'] in selected_chats
        prefix = "✅ " if is_selected else ""
        builder.row(
            InlineKeyboardButton(
                text=f"{prefix}{chat['name'][:30]}",
                callback_data=f"toggle_chat_{chat['id']}",
                style='success' if is_selected else 'default',
                icon_custom_emoji_id=Emoji.CHECK if is_selected else Emoji.PEOPLE
            )
        )
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"chats_page_{page-1}",
                style='default',
                icon_custom_emoji_id=Emoji.BACK
            )
        )
    if end_idx < len(chats):
        nav_buttons.append(
            InlineKeyboardButton(
                text="Вперед ➡️",
                callback_data=f"chats_page_{page+1}",
                style='default',
                icon_custom_emoji_id=Emoji.CHART_UP
            )
        )
    if nav_buttons:
        builder.row(*nav_buttons)
    
    builder.row(
        InlineKeyboardButton(
            text=f"Готово (выбрано: {len(selected_chats)})",
            callback_data="chats_done",
            style='success',
            icon_custom_emoji_id=Emoji.CHECK
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Отмена",
            callback_data="functions",
            style='danger',
            icon_custom_emoji_id=Emoji.CROSS
        )
    )
    
    return builder.as_markup()

# --- Хендлеры ---
@dp.message(Command("start"))
async def cmd_start(message: Message):
    user = message.from_user
    await register_user(user.id, user.username, user.first_name)
    
    welcome_text = f"""
{e(Emoji.SMILE)} <b>Добро пожаловать в Vest Game Soft!</b>

{e(Emoji.BOT)} Я помогу вам управлять аккаунтами и делать рассылки.

{e(Emoji.PEOPLE)} <b>Менеджер аккаунтов</b> — добавление и управление аккаунтами
{e(Emoji.APPS)} <b>Функции</b> — рассылка и автоответчик

Выберите действие:
    """
    
    await message.answer(welcome_text, reply_markup=get_main_menu_keyboard())

@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    stats = await get_broadcast_stats()
    
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Рассылка всем пользователям",
            callback_data="admin_broadcast_all",
            style='primary',
            icon_custom_emoji_id=Emoji.MEGAPHONE
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Обновить статистику",
            callback_data="admin_refresh_stats",
            style='default',
            icon_custom_emoji_id=Emoji.REFRESH
        )
    )
    
    admin_text = f"""
{e(Emoji.BOT)} <b>Админ-панель</b>

{e(Emoji.PEOPLE)} Пользователей: <b>{stats['total_users']}</b>
{e(Emoji.PROFILE)} Аккаунтов: <b>{stats['total_accounts']}</b>
{e(Emoji.MEGAPHONE)} Всего рассылок: <b>{stats['total_broadcasts']}</b>
{e(Emoji.PLAY)} Активных: <b>{stats['active_broadcasts']}</b>
    """
    
    await message.answer(admin_text, reply_markup=builder.as_markup())

# --- Главное меню ---
@dp.callback_query(F.data == "main_menu")
async def back_to_main(callback: CallbackQuery):
    await callback.message.edit_text(
        f"{e(Emoji.SMILE)} <b>Главное меню</b>\n\nВыберите действие:",
        reply_markup=get_main_menu_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "account_manager")
async def account_manager(callback: CallbackQuery):
    await callback.message.edit_text(
        f"{e(Emoji.PEOPLE)} <b>Менеджер аккаунтов</b>\n\nВыберите действие:",
        reply_markup=get_account_manager_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "functions")
async def functions(callback: CallbackQuery):
    await callback.message.edit_text(
        f"{e(Emoji.APPS)} <b>Функции</b>\n\nВыберите функцию:",
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()

# --- Менеджер аккаунтов ---
@dp.callback_query(F.data == "add_account")
async def add_account(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"{e(Emoji.PHONE)} <b>Добавление аккаунта</b>\n\n"
        f"Введите номер телефона в формате:\n"
        f"<code>+79991234567</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Отмена",
                callback_data="account_manager",
                style='default',
                icon_custom_emoji_id=Emoji.BACK
            )]
        ])
    )
    await state.set_state(AccountStates.waiting_for_phone)
    await callback.answer()

@dp.message(AccountStates.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    
    if not re.match(r'^\+\d{10,15}$', phone):
        await message.answer(
            f"{e(Emoji.CROSS)} Неверный формат номера. Попробуйте снова.\n"
            f"Пример: <code>+79991234567</code>"
        )
        return
    
    try:
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        
        sent_code = await client.send_code_request(phone)
        await state.update_data(
            phone=phone,
            client_session=client.session.save(),
            phone_code_hash=sent_code.phone_code_hash
        )
        
        await message.answer(
            f"{e(Emoji.CHECK)} Код подтверждения отправлен!\n\n"
            f"Введите код из Telegram:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="Отмена",
                    callback_data="account_manager",
                    style='default',
                    icon_custom_emoji_id=Emoji.BACK
                )]
            ])
        )
        await state.set_state(AccountStates.waiting_for_code)
        
    except Exception as e:
        await message.answer(f"{e(Emoji.CROSS)} Ошибка: {str(e)}")
        await state.clear()

@dp.message(AccountStates.waiting_for_code)
async def process_code(message: Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    
    try:
        client = TelegramClient(StringSession(data['client_session']), API_ID, API_HASH)
        await client.connect()
        
        try:
            await client.sign_in(
                phone=data['phone'],
                code=code,
                phone_code_hash=data['phone_code_hash']
            )
        except SessionPasswordNeededError:
            await state.update_data(code=code)
            await message.answer(
                f"{e(Emoji.LOCK_CLOSED)} Введите пароль 2FA:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text="Отмена",
                        callback_data="account_manager",
                        style='default',
                        icon_custom_emoji_id=Emoji.BACK
                    )]
                ])
            )
            await state.set_state(AccountStates.waiting_for_2fa)
            return
        
        session_string = client.session.save()
        async with db_pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO accounts (user_id, phone, session_string) VALUES ($1, $2, $3)',
                message.from_user.id, data['phone'], session_string
            )
        
        active_clients[message.from_user.id] = client
        
        await message.answer(
            f"{e(Emoji.CHECK)} Аккаунт успешно добавлен!",
            reply_markup=get_account_manager_keyboard()
        )
        await state.clear()
        
    except Exception as e:
        await message.answer(f"{e(Emoji.CROSS)} Ошибка: {str(e)}")
        await state.clear()

@dp.message(AccountStates.waiting_for_2fa)
async def process_2fa(message: Message, state: FSMContext):
    password = message.text.strip()
    data = await state.get_data()
    
    try:
        client = TelegramClient(StringSession(data['client_session']), API_ID, API_HASH)
        await client.connect()
        
        await client.sign_in(password=password)
        
        session_string = client.session.save()
        async with db_pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO accounts (user_id, phone, session_string) VALUES ($1, $2, $3)',
                message.from_user.id, data['phone'], session_string
            )
        
        active_clients[message.from_user.id] = client
        
        await message.answer(
            f"{e(Emoji.CHECK)} Аккаунт успешно добавлен!",
            reply_markup=get_account_manager_keyboard()
        )
        await state.clear()
        
    except Exception as e:
        await message.answer(f"{e(Emoji.CROSS)} Ошибка: {str(e)}")
        await state.clear()

@dp.callback_query(F.data == "my_accounts")
async def my_accounts(callback: CallbackQuery):
    accounts = await get_user_accounts(callback.from_user.id)
    
    if not accounts:
        await callback.message.edit_text(
            f"{e(Emoji.INFO)} У вас пока нет аккаунтов.\n\n"
            f"Нажмите 'Добавить аккаунт' чтобы добавить новый.",
            reply_markup=get_account_manager_keyboard()
        )
    else:
        await callback.message.edit_text(
            f"{e(Emoji.PEOPLE)} <b>Ваши аккаунты:</b>\n\n"
            f"Выберите аккаунт для управления:",
            reply_markup=get_accounts_list_keyboard(accounts, "manage_account")
        )
    await callback.answer()

@dp.callback_query(F.data.startswith("manage_account_"))
async def manage_account(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[2])
    account = await get_account(account_id)
    
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    
    text = f"""
{e(Emoji.PROFILE)} <b>Аккаунт:</b>
{e(Emoji.PHONE)} Телефон: <code>{account['phone']}</code>
{e(Emoji.EYE)} Статус: {'✅ Активен' if account['is_active'] else '❌ Неактивен'}
{e(Emoji.CLOCK)} Создан: {account['created_at'].strftime('%d.%m.%Y %H:%M')}
    """
    
    await callback.message.edit_text(text, reply_markup=get_account_actions_keyboard(account_id))
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_account_"))
async def delete_account_handler(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[2])
    
    if account_id in active_clients:
        await active_clients[account_id].disconnect()
        del active_clients[account_id]
    
    await delete_account(account_id)
    
    await callback.message.edit_text(
        f"{e(Emoji.CHECK)} Аккаунт успешно удален!",
        reply_markup=get_account_manager_keyboard()
    )
    await callback.answer()

# --- Рассылка ---
@dp.callback_query(F.data == "broadcast")
async def broadcast(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"{e(Emoji.SEND)} <b>Рассылка</b>\n\nВыберите режим рассылки:",
        reply_markup=get_broadcast_mode_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("mode_"))
async def select_mode(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.split("_")[1]
    await state.update_data(mode=mode)
    
    accounts = await get_user_accounts(callback.from_user.id)
    if not accounts:
        await callback.message.edit_text(
            f"{e(Emoji.CROSS)} У вас нет аккаунтов. Сначала добавьте аккаунт.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    await callback.message.edit_text(
        f"{e(Emoji.PROFILE)} <b>Выберите аккаунт для рассылки:</b>",
        reply_markup=get_accounts_list_keyboard(accounts, "select_broadcast_account")
    )
    await state.set_state(BroadcastStates.waiting_for_account)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_broadcast_account_"))
async def select_broadcast_account(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[3])
    await state.update_data(account_id=account_id)
    
    client = await get_client_for_account(account_id)
    if not client:
        await callback.answer("Не удалось подключиться к аккаунту", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"{e(Emoji.LOADING)} Загружаю чаты...",
        reply_markup=None
    )
    
    chats = await get_chats_from_client(client)
    await state.update_data(chats=chats, selected_chats=[], current_page=0)
    
    await callback.message.edit_text(
        f"{e(Emoji.PEOPLE)} <b>Выберите чаты для рассылки</b> (макс. 200)\n"
        f"Страница 1 из {(len(chats) - 1) // 10 + 1}",
        reply_markup=get_chat_selection_keyboard(chats, 0, [])
    )
    await state.set_state(BroadcastStates.selecting_chats)
    await callback.answer()

@dp.callback_query(F.data.startswith("toggle_chat_"))
async def toggle_chat(callback: CallbackQuery, state: FSMContext):
    chat_id = int(callback.data.split("_")[2])
    data = await state.get_data()
    selected_chats = data.get('selected_chats', [])
    chats = data.get('chats', [])
    current_page = data.get('current_page', 0)
    
    if len(selected_chats) >= 200 and chat_id not in selected_chats:
        await callback.answer("Максимум 200 чатов", show_alert=True)
        return
    
    if chat_id in selected_chats:
        selected_chats.remove(chat_id)
    else:
        selected_chats.append(chat_id)
    
    await state.update_data(selected_chats=selected_chats)
    
    await callback.message.edit_text(
        f"{e(Emoji.PEOPLE)} <b>Выберите чаты для рассылки</b> (макс. 200)\n"
        f"Выбрано: {len(selected_chats)}\n"
        f"Страница {current_page + 1} из {(len(chats) - 1) // 10 + 1}",
        reply_markup=get_chat_selection_keyboard(chats, current_page, selected_chats)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("chats_page_"))
async def chats_page(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split("_")[2])
    data = await state.get_data()
    chats = data.get('chats', [])
    selected_chats = data.get('selected_chats', [])
    
    await state.update_data(current_page=page)
    
    await callback.message.edit_text(
        f"{e(Emoji.PEOPLE)} <b>Выберите чаты для рассылки</b> (макс. 200)\n"
        f"Выбрано: {len(selected_chats)}\n"
        f"Страница {page + 1} из {(len(chats) - 1) // 10 + 1}",
        reply_markup=get_chat_selection_keyboard(chats, page, selected_chats)
    )
    await callback.answer()

@dp.callback_query(F.data == "chats_done")
async def chats_done(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_chats = data.get('selected_chats', [])
    
    if not selected_chats:
        await callback.answer("Выберите хотя бы один чат", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"{e(Emoji.CLOCK)} <b>Введите задержку между сообщениями</b>\n\n"
        f"От 10 до 300000 секунд:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Назад",
                callback_data="broadcast",
                style='default',
                icon_custom_emoji_id=Emoji.BACK
            )]
        ])
    )
    await state.set_state(BroadcastStates.waiting_for_delay)
    await callback.answer()

@dp.message(BroadcastStates.waiting_for_delay)
async def process_delay(message: Message, state: FSMContext):
    try:
        delay = int(message.text.strip())
        if delay < 10 or delay > 300000:
            raise ValueError
    except ValueError:
        await message.answer(
            f"{e(Emoji.CROSS)} Введите число от 10 до 300000:"
        )
        return
    
    await state.update_data(delay=delay)
    
    await message.answer(
        f"{e(Emoji.MAIL)} <b>Введите количество сообщений в каждый чат</b>\n\n"
        f"От 1 до 200000:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Назад",
                callback_data="broadcast",
                style='default',
                icon_custom_emoji_id=Emoji.BACK
            )]
        ])
    )
    await state.set_state(BroadcastStates.waiting_for_count)

@dp.message(BroadcastStates.waiting_for_count)
async def process_count(message: Message, state: FSMContext):
    try:
        count = int(message.text.strip())
        if count < 1 or count > 200000:
            raise ValueError
    except ValueError:
        await message.answer(
            f"{e(Emoji.CROSS)} Введите число от 1 до 200000:"
        )
        return
    
    await state.update_data(message_count=count)
    
    await message.answer(
        f"{e(Emoji.WRITE)} <b>Введите сообщение для рассылки:</b>\n\n"
        f"Поддерживается HTML и премиум эмодзи.\n"
        f"Можно прикрепить медиа (фото, видео, документы).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Назад",
                callback_data="broadcast",
                style='default',
                icon_custom_emoji_id=Emoji.BACK
            )]
        ])
    )
    await state.set_state(BroadcastStates.waiting_for_message)

@dp.message(BroadcastStates.waiting_for_message)
async def process_broadcast_message(message: Message, state: FSMContext):
    text = message.text or message.caption or ""
    media_paths = []
    
    if message.photo:
        file_path = f"media/{message.photo[-1].file_id}.jpg"
        await message.bot.download(message.photo[-1], file_path)
        media_paths.append(file_path)
    elif message.video:
        file_path = f"media/{message.video.file_id}.mp4"
        await message.bot.download(message.video, file_path)
        media_paths.append(file_path)
    elif message.document:
        file_path = f"media/{message.document.file_id}"
        await message.bot.download(message.document, file_path)
        media_paths.append(file_path)
    
    await state.update_data(message_text=text, message_media=media_paths)
    
    data = await state.get_data()
    preview_text = f"""
{e(Emoji.EYE)} <b>Предпросмотр рассылки:</b>

{e(Emoji.PROFILE)} Аккаунт ID: {data['account_id']}
{e(Emoji.PEOPLE)} Чатов: {len(data['selected_chats'])}
{e(Emoji.CLOCK)} Задержка: {data['delay']} сек
{e(Emoji.MAIL)} Сообщений в чат: {data['message_count']}
{e(Emoji.GEAR)} Режим: {'Одновременный' if data['mode'] == 'simultaneous' else 'Рандомный'}

{e(Emoji.WRITE)} <b>Сообщение:</b>
{text[:200]}{'...' if len(text) > 200 else ''}

{e(Emoji.MEDIA)} Медиа: {len(media_paths)} файлов
    """
    
    await message.answer(preview_text, reply_markup=get_broadcast_preview_keyboard())
    await state.set_state(BroadcastStates.preview)

@dp.callback_query(F.data == "start_broadcast", BroadcastStates.preview)
async def start_broadcast(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        broadcast_id = await conn.fetchval(
            '''INSERT INTO broadcasts 
               (user_id, account_id, chat_ids, delay, message_count, message_text, message_media, mode)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               RETURNING id''',
            user_id, data['account_id'], data['selected_chats'],
            data['delay'], data['message_count'], data['message_text'],
            data['message_media'], data['mode']
        )
    
    task = asyncio.create_task(execute_broadcast(broadcast_id, user_id))
    active_broadcasts[broadcast_id] = task
    
    await callback.message.edit_text(
        f"{e(Emoji.PLAY)} <b>Рассылка запущена!</b>\n\n"
        f"ID: {broadcast_id}\n"
        f"Чатов: {len(data['selected_chats'])}\n"
        f"Сообщений в чат: {data['message_count']}",
        reply_markup=get_broadcast_control_keyboard(broadcast_id)
    )
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data.startswith("stop_broadcast_"))
async def stop_broadcast(callback: CallbackQuery):
    broadcast_id = int(callback.data.split("_")[2])
    broadcast_stop_flags[broadcast_id] = True
    
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE broadcasts SET status = 'stopped', stopped_at = NOW() WHERE id = $1",
            broadcast_id
        )
    
    await callback.message.edit_text(
        f"{e(Emoji.STOP)} <b>Рассылка остановлена!</b>\n\nID: {broadcast_id}",
        reply_markup=get_broadcast_control_keyboard(broadcast_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("resume_broadcast_"))
async def resume_broadcast(callback: CallbackQuery):
    broadcast_id = int(callback.data.split("_")[2])
    broadcast_stop_flags[broadcast_id] = False
    
    task = asyncio.create_task(execute_broadcast(broadcast_id, callback.from_user.id))
    active_broadcasts[broadcast_id] = task
    
    await callback.message.edit_text(
        f"{e(Emoji.PLAY)} <b>Рассылка возобновлена!</b>\n\nID: {broadcast_id}",
        reply_markup=get_broadcast_control_keyboard(broadcast_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_broadcast_"))
async def delete_broadcast(callback: CallbackQuery):
    broadcast_id = int(callback.data.split("_")[2])
    
    broadcast_stop_flags[broadcast_id] = True
    if broadcast_id in active_broadcasts:
        active_broadcasts[broadcast_id].cancel()
        del active_broadcasts[broadcast_id]
    
    async with db_pool.acquire() as conn:
        await conn.execute('DELETE FROM broadcasts WHERE id = $1', broadcast_id)
    
    await callback.message.edit_text(
        f"{e(Emoji.CHECK)} Рассылка удалена!",
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "my_broadcasts")
async def my_broadcasts(callback: CallbackQuery):
    broadcasts = await get_user_broadcasts(callback.from_user.id)
    
    if not broadcasts:
        await callback.message.edit_text(
            f"{e(Emoji.INFO)} У вас пока нет рассылок.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for bc in broadcasts[:10]:
        status_emoji = {
            'active': e(Emoji.PLAY),
            'stopped': e(Emoji.STOP),
            'completed': e(Emoji.CHECK)
        }.get(bc['status'], e(Emoji.INFO))
        
        progress = f"{bc['progress']}/{bc['total_count']}" if bc['total_count'] > 0 else "0/0"
        
        builder.row(
            InlineKeyboardButton(
                text=f"{status_emoji} ID:{bc['id']} | {progress} | {bc['status']}",
                callback_data=f"show_broadcast_{bc['id']}",
                style='default',
                icon_custom_emoji_id=Emoji.CHART
            )
        )
    
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="functions",
            style='default',
            icon_custom_emoji_id=Emoji.BACK
        )
    )
    
    await callback.message.edit_text(
        f"{e(Emoji.CHART)} <b>Мои рассылки:</b>",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("show_broadcast_"))
async def show_broadcast(callback: CallbackQuery):
    broadcast_id = int(callback.data.split("_")[2])
    
    async with db_pool.acquire() as conn:
        bc = await conn.fetchrow('SELECT * FROM broadcasts WHERE id = $1', broadcast_id)
    
    if not bc:
        await callback.answer("Рассылка не найдена", show_alert=True)
        return
    
    bc = dict(bc)
    progress = f"{bc['progress']}/{bc['total_count']}" if bc['total_count'] > 0 else "0/0"
    
    text = f"""
{e(Emoji.CHART)} <b>Рассылка ID: {bc['id']}</b>

{e(Emoji.GEAR)} Статус: {bc['status']}
{e(Emoji.STATS)} Прогресс: {progress}
{e(Emoji.CLOCK)} Задержка: {bc['delay']} сек
{e(Emoji.MAIL)} Сообщений в чат: {bc['message_count']}
{e(Emoji.PEOPLE)} Чатов: {len(bc['chat_ids'])}
{e(Emoji.CALENDAR)} Создана: {bc['created_at'].strftime('%d.%m.%Y %H:%M')}
    """
    
    await callback.message.edit_text(text, reply_markup=get_broadcast_control_keyboard(bc['id']))
    await callback.answer()

# --- Автоответчик ---
@dp.callback_query(F.data == "auto_responder")
async def auto_responder(callback: CallbackQuery, state: FSMContext):
    accounts = await get_user_accounts(callback.from_user.id)
    if not accounts:
        await callback.message.edit_text(
            f"{e(Emoji.CROSS)} У вас нет аккаунтов. Сначала добавьте аккаунт.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    await callback.message.edit_text(
        f"{e(Emoji.PROFILE)} <b>Выберите аккаунт для автоответчика:</b>",
        reply_markup=get_accounts_list_keyboard(accounts, "select_responder_account")
    )
    await state.set_state(AutoResponderStates.waiting_for_account)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_responder_account_"))
async def select_responder_account(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[3])
    await state.update_data(account_id=account_id)
    
    await callback.message.edit_text(
        f"{e(Emoji.WRITE)} <b>Введите слово-триггер:</b>\n\n"
        f"Или напишите <code>-</code> чтобы отвечать на все сообщения в ЛС.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Назад",
                callback_data="functions",
                style='default',
                icon_custom_emoji_id=Emoji.BACK
            )]
        ])
    )
    await state.set_state(AutoResponderStates.waiting_for_trigger)
    await callback.answer()

@dp.message(AutoResponderStates.waiting_for_trigger)
async def process_trigger(message: Message, state: FSMContext):
    trigger = message.text.strip()
    
    if not trigger:
        await message.answer(f"{e(Emoji.CROSS)} Введите слово-триггер или '-'")
        return
    
    await state.update_data(trigger=trigger)
    
    await message.answer(
        f"{e(Emoji.WRITE)} <b>Введите ответ:</b>\n\n"
        f"Поддерживается HTML и премиум эмодзи.\n"
        f"Можно прикрепить медиа.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Назад",
                callback_data="functions",
                style='default',
                icon_custom_emoji_id=Emoji.BACK
            )]
        ])
    )
    await state.set_state(AutoResponderStates.waiting_for_response)

@dp.message(AutoResponderStates.waiting_for_response)
async def process_auto_response(message: Message, state: FSMContext):
    text = message.text or message.caption or ""
    media_paths = []
    
    if message.photo:
        file_path = f"media/{message.photo[-1].file_id}.jpg"
        await message.bot.download(message.photo[-1], file_path)
        media_paths.append(file_path)
    elif message.video:
        file_path = f"media/{message.video.file_id}.mp4"
        await message.bot.download(message.video, file_path)
        media_paths.append(file_path)
    elif message.document:
        file_path = f"media/{message.document.file_id}"
        await message.bot.download(message.document, file_path)
        media_paths.append(file_path)
    
    await state.update_data(response_text=text, response_media=media_paths)
    
    data = await state.get_data()
    preview_text = f"""
{e(Emoji.EYE)} <b>Предпросмотр автоответчика:</b>

{e(Emoji.TAG)} Триггер: {data['trigger']}
{e(Emoji.WRITE)} Ответ:
{text[:200]}{'...' if len(text) > 200 else ''}

{e(Emoji.MEDIA)} Медиа: {len(media_paths)} файлов
    """
    
    await message.answer(preview_text, reply_markup=get_auto_responder_preview_keyboard())
    await state.set_state(AutoResponderStates.preview)

@dp.callback_query(F.data == "create_auto_responder", AutoResponderStates.preview)
async def create_auto_responder(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        responder_id = await conn.fetchval(
            '''INSERT INTO auto_responders 
               (user_id, account_id, trigger, response_text, response_media)
               VALUES ($1, $2, $3, $4, $5)
               RETURNING id''',
            user_id, data['account_id'], data['trigger'],
            data['response_text'], data['response_media']
        )
    
    await start_auto_responder(responder_id, user_id)
    
    await callback.message.edit_text(
        f"{e(Emoji.CHECK)} <b>Автоответчик создан и запущен!</b>\n\n"
        f"ID: {responder_id}\n"
        f"Триггер: {data['trigger']}",
        reply_markup=get_functions_keyboard()
    )
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data == "my_auto_responders")
async def my_auto_responders(callback: CallbackQuery):
    responders = await get_user_auto_responders(callback.from_user.id)
    
    if not responders:
        await callback.message.edit_text(
            f"{e(Emoji.INFO)} У вас пока нет автоответчиков.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for resp in responders:
        status = "✅" if resp['is_active'] else "❌"
        builder.row(
            InlineKeyboardButton(
                text=f"{status} ID:{resp['id']} | Триггер: {resp['trigger'][:20]}",
                callback_data=f"show_responder_{resp['id']}",
                style='default',
                icon_custom_emoji_id=Emoji.BELL
            )
        )
    
    builder.row(
        InlineKeyboardButton(
            text="Создать новый",
            callback_data="auto_responder",
            style='primary',
            icon_custom_emoji_id=Emoji.ADD_TEXT
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="functions",
            style='default',
            icon_custom_emoji_id=Emoji.BACK
        )
    )
    
    await callback.message.edit_text(
        f"{e(Emoji.BELL)} <b>Мои автоответчики:</b>",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("show_responder_"))
async def show_responder(callback: CallbackQuery):
    responder_id = int(callback.data.split("_")[2])
    responder = await get_auto_responder(responder_id)
    
    if not responder:
        await callback.answer("Автоответчик не найден", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    if responder['is_active']:
        builder.row(
            InlineKeyboardButton(
                text="Остановить",
                callback_data=f"stop_responder_{responder_id}",
                style='danger',
                icon_custom_emoji_id=Emoji.STOP
            )
        )
    else:
        builder.row(
            InlineKeyboardButton(
                text="Запустить",
                callback_data=f"start_responder_{responder_id}",
                style='success',
                icon_custom_emoji_id=Emoji.PLAY
            )
        )
    
    builder.row(
        InlineKeyboardButton(
            text="Удалить",
            callback_data=f"delete_responder_{responder_id}",
            style='default',
            icon_custom_emoji_id=Emoji.DELETE
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="my_auto_responders",
            style='default',
            icon_custom_emoji_id=Emoji.BACK
        )
    )
    
    text = f"""
{e(Emoji.BELL)} <b>Автоответчик ID: {responder['id']}</b>

{e(Emoji.EYE)} Статус: {'✅ Активен' if responder['is_active'] else '❌ Остановлен'}
{e(Emoji.TAG)} Триггер: {responder['trigger']}
{e(Emoji.WRITE)} Ответ: {responder['response_text'][:100]}...
{e(Emoji.CLOCK)} Создан: {responder['created_at'].strftime('%d.%m.%Y %H:%M')}
    """
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("stop_responder_"))
async def stop_responder(callback: CallbackQuery):
    responder_id = int(callback.data.split("_")[2])
    responder = await get_auto_responder(responder_id)
    
    if responder and responder['user_id'] == callback.from_user.id:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE auto_responders SET is_active = FALSE WHERE id = $1",
                responder_id
            )
        
        if callback.from_user.id in active_auto_responders:
            if responder['account_id'] in active_auto_responders[callback.from_user.id]:
                active_auto_responders[callback.from_user.id][responder['account_id']].cancel()
        
        await callback.answer("Автоответчик остановлен", show_alert=True)
        await show_responder(callback)

@dp.callback_query(F.data.startswith("start_responder_"))
async def start_responder(callback: CallbackQuery):
    responder_id = int(callback.data.split("_")[2])
    responder = await get_auto_responder(responder_id)
    
    if responder and responder['user_id'] == callback.from_user.id:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE auto_responders SET is_active = TRUE WHERE id = $1",
                responder_id
            )
        
        await start_auto_responder(responder_id, callback.from_user.id)
        
        await callback.answer("Автоответчик запущен", show_alert=True)
        await show_responder(callback)

@dp.callback_query(F.data.startswith("delete_responder_"))
async def delete_responder(callback: CallbackQuery):
    responder_id = int(callback.data.split("_")[2])
    responder = await get_auto_responder(responder_id)
    
    if responder and responder['user_id'] == callback.from_user.id:
        if callback.from_user.id in active_auto_responders:
            if responder['account_id'] in active_auto_responders[callback.from_user.id]:
                active_auto_responders[callback.from_user.id][responder['account_id']].cancel()
        
        async with db_pool.acquire() as conn:
            await conn.execute('DELETE FROM auto_responders WHERE id = $1', responder_id)
        
        await callback.answer("Автоответчик удален", show_alert=True)
        await my_auto_responders(callback)

# --- Админ-панель ---
@dp.callback_query(F.data == "admin_refresh_stats")
async def admin_refresh_stats(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    stats = await get_broadcast_stats()
    
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Рассылка всем пользователям",
            callback_data="admin_broadcast_all",
            style='primary',
            icon_custom_emoji_id=Emoji.MEGAPHONE
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Обновить статистику",
            callback_data="admin_refresh_stats",
            style='default',
            icon_custom_emoji_id=Emoji.REFRESH
        )
    )
    
    admin_text = f"""
{e(Emoji.BOT)} <b>Админ-панель</b>

{e(Emoji.PEOPLE)} Пользователей: <b>{stats['total_users']}</b>
{e(Emoji.PROFILE)} Аккаунтов: <b>{stats['total_accounts']}</b>
{e(Emoji.MEGAPHONE)} Всего рассылок: <b>{stats['total_broadcasts']}</b>
{e(Emoji.PLAY)} Активных: <b>{stats['active_broadcasts']}</b>
    """
    
    await callback.message.edit_text(admin_text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_broadcast_all")
async def admin_broadcast_all(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"{e(Emoji.MEGAPHONE)} <b>Рассылка всем пользователям</b>\n\n"
        f"Введите сообщение для рассылки:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Отмена",
                callback_data="admin_refresh_stats",
                style='default',
                icon_custom_emoji_id=Emoji.BACK
            )]
        ])
    )
    await state.set_state(AdminStates.waiting_for_broadcast_message)
    await callback.answer()

@dp.message(AdminStates.waiting_for_broadcast_message)
async def process_admin_broadcast(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    async with db_pool.acquire() as conn:
        users = await conn.fetch('SELECT user_id FROM users')
    
    broadcast_text = message.text or message.caption or ""
    
    success = 0
    for user in users:
        try:
            await bot.send_message(user['user_id'], broadcast_text)
            success += 1
        except Exception as e:
            logger.error(f"Failed to send admin broadcast to {user['user_id']}: {e}")
    
    await message.answer(
        f"{e(Emoji.CHECK)} <b>Рассылка завершена!</b>\n\n"
        f"Отправлено: {success}/{len(users)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="В админ-панель",
                callback_data="admin_refresh_stats",
                style='primary',
                icon_custom_emoji_id=Emoji.BACK
            )]
        ])
    )
    await state.clear()

# --- Запуск бота ---
async def on_startup():
    await init_db()
    
    async with db_pool.acquire() as conn:
        responders = await conn.fetch("SELECT * FROM auto_responders WHERE is_active = TRUE")
        for responder in responders:
            responder = dict(responder)
            await start_auto_responder(responder['id'], responder['user_id'])

async def main():
    await on_startup()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
