import asyncio
import logging
import os
import random
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from html import escape

import asyncpg
import pytz
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.methods import DeleteWebhook
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession
from telethon.tl.types import User

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Переменные окружения ---
DATABASE_URL = os.getenv('DATABASE_URL')
BOT_TOKEN = os.getenv('BOT_TOKEN')
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')

ADMIN_IDS = [7973988177]
SUPPORT_USERNAME = "@VestGameSupport"
MSK_TZ = pytz.timezone('Europe/Moscow')

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

# --- Премиум эмодзи (символ, id) ---
EMOJI = {
    "SETTINGS": ("⚙", "5870982283724328568"),
    "PROFILE": ("👤", "5870994129244131212"),
    "PEOPLE": ("👥", "5870772616305839506"),
    "SMILE": ("🙂", "5870764288364252592"),
    "CHART_UP": ("📊", "5870930636742595124"),
    "CHART": ("📊", "5870921681735781843"),
    "HOUSE": ("🏘", "5873147866364514353"),
    "LOCK_CLOSED": ("🔒", "6037249452824072506"),
    "LOCK_OPEN": ("🔓", "6037496202990194718"),
    "MEGAPHONE": ("📣", "6039422865189638057"),
    "CHECK": ("✅", "5870633910337015697"),
    "CROSS": ("❌", "5870657884844462243"),
    "TRASH": ("🗑", "5870875489362513438"),
    "INFO": ("ℹ", "6028435952299413210"),
    "BOT": ("🤖", "6030400221232501136"),
    "EYE": ("👁", "6037397706505195857"),
    "SEND": ("⬆", "5963103826075456248"),
    "BELL": ("🔔", "6039486778597970865"),
    "CLOCK": ("⏰", "5983150113483134607"),
    "WRITE": ("✍", "5870753782874246579"),
    "MEDIA": ("🖼", "6035128606563241721"),
    "LOCATION": ("📍", "6042011682497106307"),
    "TAG": ("🏷", "5886285355279193209"),
    "TIME_PAST": ("🕓", "5775896410780079073"),
    "APPS": ("📦", "5778672437122045013"),
    "ADD_TEXT": ("🔡", "5771851822897566479"),
    "MONEY_SEND": ("🪙", "5890848474563352982"),
    "MONEY_ACCEPT": ("🪙", "5879814368572478751"),
    "CODE": ("🔨", "5940433880585605708"),
    "LOADING": ("🔄", "5345906554510012647"),
    "BACK": ("◁", "5775417808636156714"),
    "STATS": ("📊", "5870921681735781843"),
    "PLAY": ("▶", "6041731551845159060"),
    "STOP": ("⏹", "6037249452824072506"),
    "REFRESH": ("🔄", "5345906554510012647"),
    "DELETE": ("🗑", "5870875489362513438"),
    "PHONE": ("📱", "5870994129244131212"),
    "GEAR": ("⚙", "5870982283724328568"),
    "MAIL": ("📨", "5963103826075456248"),
    "CALENDAR": ("📅", "5890937706803894250"),
    "SUPPORT": ("🎧", "6039486778597970865"),
    "CASINO": ("🎰", "5873147866364514353"),
    "FIRE": ("🔥", "5870930636742595124"),
    "USERS": ("👥", "5870772616305839506"),
    "GLOBE": ("🌐", "6042011682497106307"),
    "ID": ("🆔", "5870801517140775623"),
    "NAMES": ("📝", "5870753782874246579"),
}

def emoji(name: str) -> str:
    if name in EMOJI:
        symbol, emoji_id = EMOJI[name]
        return f'<tg-emoji emoji-id="{emoji_id}">{symbol}</tg-emoji>'
    return name

def get_icon(name: str) -> str:
    if name in EMOJI:
        return EMOJI[name][1]
    return None

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

class ScheduledBroadcastStates(StatesGroup):
    waiting_for_account = State()
    selecting_chats = State()
    waiting_for_delay = State()
    waiting_for_count = State()
    waiting_for_message = State()
    waiting_for_datetime = State()
    preview = State()

class AutoResponderStates(StatesGroup):
    waiting_for_account = State()
    waiting_for_trigger = State()
    waiting_for_response = State()
    preview = State()

class AdminStates(StatesGroup):
    waiting_for_broadcast_message = State()

class ParsingStates(StatesGroup):
    waiting_for_account = State()
    waiting_for_chat = State()
    waiting_for_mode = State()

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

        await conn.execute('''
            CREATE TABLE IF NOT EXISTS account_logs (
                id SERIAL PRIMARY KEY,
                account_id INTEGER REFERENCES accounts(id),
                chat_name TEXT,
                chat_id BIGINT,
                direction TEXT NOT NULL,
                message_text TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Миграции
        try:
            await conn.execute('ALTER TABLE accounts ADD COLUMN IF NOT EXISTS warming_enabled BOOLEAN DEFAULT FALSE')
        except:
            pass
        
        try:
            await conn.execute('ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS scheduled_at TIMESTAMP')
        except:
            pass

# --- Регистрация пользователя ---
async def register_user(user_id: int, username: str, first_name: str):
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO users (user_id, username, first_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE 
            SET username = $2, first_name = $3
        ''', user_id, username, first_name)

# --- Логирование ---
async def add_account_log(account_id: int, chat_name: str, chat_id: int, direction: str, message_text: str = ""):
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO account_logs (account_id, chat_name, chat_id, direction, message_text) VALUES ($1, $2, $3, $4, $5)',
                account_id, chat_name, chat_id, direction, message_text[:100]
            )
    except:
        pass

# --- Вспомогательные функции ---
async def get_user_accounts(user_id: int) -> List[Dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT id, phone, is_active, COALESCE(warming_enabled, FALSE) as warming_enabled FROM accounts WHERE user_id = $1',
            user_id
        )
        return [dict(row) for row in rows]

async def get_account(account_id: int) -> Optional[Dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow('SELECT *, COALESCE(warming_enabled, FALSE) as warming_enabled FROM accounts WHERE id = $1', account_id)
        return dict(row) if row else None

async def delete_account(account_id: int) -> bool:
    async with db_pool.acquire() as conn:
        result = await conn.execute('DELETE FROM accounts WHERE id = $1', account_id)
        return result != "DELETE 0"

async def update_account_warming(account_id: int, enabled: bool):
    async with db_pool.acquire() as conn:
        await conn.execute('UPDATE accounts SET warming_enabled = $1 WHERE id = $2', enabled, account_id)

async def create_telethon_client(session_string: str) -> TelegramClient:
    return TelegramClient(StringSession(session_string), API_ID, API_HASH)

async def get_client_for_account(account_id: int) -> Optional[TelegramClient]:
    if account_id in active_clients:
        client = active_clients[account_id]
        if client.is_connected():
            return client
    
    account = await get_account(account_id)
    if not account:
        return None
    
    try:
        client = await create_telethon_client(account['session_string'])
        await client.connect()
        
        if await client.is_user_authorized():
            active_clients[account_id] = client
            return client
        else:
            await client.disconnect()
            return None
    except Exception as ex:
        logger.error(f"Error connecting client: {ex}")
        return None

async def get_chats_from_client(client: TelegramClient, limit: int = 200) -> List[Dict]:
    chats = []
    async for dialog in client.iter_dialogs(limit=limit):
        if dialog.is_user or dialog.is_group or dialog.is_channel:
            chat_info = {
                'id': str(dialog.id),
                'name': dialog.name if dialog.name else "Без названия",
                'type': 'user' if dialog.is_user else 'group' if dialog.is_group else 'channel'
            }
            chats.append(chat_info)
    return chats

async def send_message_to_chat(client: TelegramClient, account_id: int, chat_id: str, text: str, media_paths: List[str] = None):
    try:
        chat_id_int = int(chat_id) if chat_id.lstrip('-').isdigit() else chat_id
        
        if media_paths and len(media_paths) > 0:
            if len(media_paths) == 1:
                await client.send_file(chat_id_int, media_paths[0], caption=text, parse_mode='html')
            else:
                await client.send_file(chat_id_int, media_paths, caption=text, parse_mode='html')
        else:
            await client.send_message(chat_id_int, text, parse_mode='html')
        
        await add_account_log(account_id, str(chat_id_int), chat_id_int, 'sent', text[:100])
        return True
    except Exception as ex:
        logger.error(f"Error sending message to {chat_id}: {ex}")
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

async def get_account_logs(account_id: int, limit: int = 50) -> List[Dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT * FROM account_logs WHERE account_id = $1 ORDER BY created_at DESC LIMIT $2',
            account_id, limit
        )
        return [dict(row) for row in rows]

# --- Обработка переменных ---
def process_variables(text: str, user_data: Dict) -> str:
    if not text:
        return text
    
    replacements = {
        '{username}': user_data.get('username', ''),
        '{first_name}': user_data.get('first_name', ''),
        '{last_name}': user_data.get('last_name', ''),
        '{user_id}': str(user_data.get('user_id', '')),
    }
    
    for key, value in replacements.items():
        text = text.replace(key, str(value))
    
    return text

async def start_auto_responder(responder_id: int, user_id: int):
    responder = await get_auto_responder(responder_id)
    if not responder or not responder['is_active']:
        return
    
    account_id = responder['account_id']
    
    if user_id in active_auto_responders and account_id in active_auto_responders[user_id]:
        active_auto_responders[user_id][account_id].cancel()
    
    task = asyncio.create_task(auto_responder_worker(responder, user_id))
    if user_id not in active_auto_responders:
        active_auto_responders[user_id] = {}
    active_auto_responders[user_id][account_id] = task

async def auto_responder_worker(responder: Dict, user_id: int):
    account_id = responder['account_id']
    trigger = responder['trigger']
    response_text = responder['response_text']
    response_media = responder.get('response_media', [])
    
    while True:
        try:
            client = await get_client_for_account(account_id)
            if not client:
                await asyncio.sleep(30)
                continue
            
            @client.on(events.NewMessage(incoming=True))
            async def handler(event):
                if event.is_private:
                    message_text = event.message.text or ""
                    if trigger == "-" or trigger.lower() in message_text.lower():
                        try:
                            sender = await event.get_sender()
                            user_data = {
                                'username': sender.username or '',
                                'first_name': sender.first_name or '',
                                'last_name': sender.last_name or '',
                                'user_id': sender.id,
                            }
                            
                            processed_text = process_variables(response_text, user_data)
                            
                            chat_name = sender.first_name or str(sender.id)
                            await add_account_log(account_id, chat_name, sender.id, 'received', message_text[:100])
                            
                            if response_media and len(response_media) > 0:
                                if len(response_media) == 1 and os.path.exists(response_media[0]):
                                    await client.send_file(event.chat_id, response_media[0], caption=processed_text, parse_mode='html')
                                else:
                                    await client.send_file(event.chat_id, response_media, caption=processed_text, parse_mode='html')
                            else:
                                await client.send_message(event.chat_id, processed_text, parse_mode='html')
                            
                            await add_account_log(account_id, chat_name, sender.id, 'sent', processed_text[:100])
                            
                        except Exception as ex:
                            logger.error(f"Auto responder error: {ex}")
            
            while client.is_connected():
                await asyncio.sleep(5)
            
            client.remove_event_handler(handler)
            
        except asyncio.CancelledError:
            break
        except Exception as ex:
            logger.error(f"Auto responder worker error: {ex}")
            await asyncio.sleep(30)

async def execute_broadcast(broadcast_id: int, user_id: int):
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
    
    account = await get_account(account_id)
    if account and account.get('warming_enabled'):
        if mode == 'simultaneous':
            mode = 'random'
        if delay < 1800:
            delay = 1800
    
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
                        send_message_to_chat(client, account_id, chat_id, message_text, message_media)
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
                for _ in chat_ids:
                    if broadcast_stop_flags.get(broadcast_id, False):
                        break
                    
                    random_chat = random.choice(chat_ids)
                    await send_message_to_chat(client, account_id, random_chat, message_text, message_media)
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
            
    except Exception as ex:
        logger.error(f"Broadcast error: {ex}")
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE broadcasts SET status = 'stopped', stopped_at = NOW() WHERE id = $1",
                broadcast_id
            )

async def check_scheduled_broadcasts():
    while True:
        try:
            now = datetime.now(MSK_TZ)
            async with db_pool.acquire() as conn:
                try:
                    scheduled = await conn.fetch(
                        "SELECT * FROM broadcasts WHERE status = 'scheduled' AND scheduled_at <= $1",
                        now
                    )
                    for bc in scheduled:
                        bc = dict(bc)
                        task = asyncio.create_task(execute_broadcast(bc['id'], bc['user_id']))
                        active_broadcasts[bc['id']] = task
                except:
                    pass
        except Exception as ex:
            logger.error(f"Scheduled broadcast check error: {ex}")
        
        await asyncio.sleep(30)

# --- Клавиатуры ---
def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Менеджер аккаунтов",
            callback_data="account_manager",
            style='primary',
            icon_custom_emoji_id=get_icon("PEOPLE")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Функции",
            callback_data="functions",
            style='primary',
            icon_custom_emoji_id=get_icon("APPS")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="КАЗИНО В ТЕЛЕГРАММ",
            url="https://t.me/VestGamebot",
            style='danger',
            icon_custom_emoji_id=get_icon("FIRE")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Поддержка",
            url=f"https://t.me/{SUPPORT_USERNAME.replace('@', '')}",
            style='default',
            icon_custom_emoji_id=get_icon("SUPPORT")
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
            icon_custom_emoji_id=get_icon("ADD_TEXT")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Мои аккаунты",
            callback_data="my_accounts",
            style='primary',
            icon_custom_emoji_id=get_icon("PEOPLE")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="main_menu",
            style='default',
            icon_custom_emoji_id=get_icon("BACK")
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
            icon_custom_emoji_id=get_icon("SEND")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Отложенная рассылка",
            callback_data="scheduled_broadcast",
            style='primary',
            icon_custom_emoji_id=get_icon("CLOCK")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Автоответчик",
            callback_data="auto_responder",
            style='primary',
            icon_custom_emoji_id=get_icon("BELL")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Парсинг чата",
            callback_data="parsing",
            style='primary',
            icon_custom_emoji_id=get_icon("USERS")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Мои рассылки",
            callback_data="my_broadcasts",
            style='default',
            icon_custom_emoji_id=get_icon("CHART")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Мои автоответчики",
            callback_data="my_auto_responders",
            style='default',
            icon_custom_emoji_id=get_icon("BELL")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="main_menu",
            style='default',
            icon_custom_emoji_id=get_icon("BACK")
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
            icon_custom_emoji_id=get_icon("MONEY_SEND")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Рандомный",
            callback_data="mode_random",
            style='primary',
            icon_custom_emoji_id=get_icon("TIME_PAST")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="functions",
            style='default',
            icon_custom_emoji_id=get_icon("BACK")
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
            icon_custom_emoji_id=get_icon("PLAY")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Отмена",
            callback_data="functions",
            style='danger',
            icon_custom_emoji_id=get_icon("CROSS")
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
            icon_custom_emoji_id=get_icon("STOP")
        ),
        InlineKeyboardButton(
            text="Возобновить",
            callback_data=f"resume_broadcast_{broadcast_id}",
            style='success',
            icon_custom_emoji_id=get_icon("PLAY")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Удалить",
            callback_data=f"delete_broadcast_{broadcast_id}",
            style='default',
            icon_custom_emoji_id=get_icon("DELETE")
        )
    )
    return builder.as_markup()

def get_accounts_list_keyboard(accounts: List[Dict], callback_prefix: str = "select_broadcast_account") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for acc in accounts:
        warming = "🔥" if acc.get('warming_enabled') else ""
        status = "✅" if acc['is_active'] else "❌"
        builder.row(
            InlineKeyboardButton(
                text=f"{acc['phone']} {status} {warming}",
                callback_data=f"{callback_prefix}_{acc['id']}",
                style='default',
                icon_custom_emoji_id=get_icon("PROFILE")
            )
        )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="main_menu",
            style='default',
            icon_custom_emoji_id=get_icon("BACK")
        )
    )
    return builder.as_markup()

def get_account_actions_keyboard(account_id: int, warming_enabled: bool = False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Логи аккаунта",
            callback_data=f"account_logs_{account_id}",
            style='default',
            icon_custom_emoji_id=get_icon("EYE")
        )
    )
    warming_text = "Выключить прогрев 🔥" if warming_enabled else "Включить прогрев"
    builder.row(
        InlineKeyboardButton(
            text=warming_text,
            callback_data=f"toggle_warming_{account_id}",
            style='default',
            icon_custom_emoji_id=get_icon("FIRE")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Удалить аккаунт",
            callback_data=f"delete_account_{account_id}",
            style='danger',
            icon_custom_emoji_id=get_icon("DELETE")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="my_accounts",
            style='default',
            icon_custom_emoji_id=get_icon("BACK")
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
            icon_custom_emoji_id=get_icon("PLAY")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Отмена",
            callback_data="functions",
            style='danger',
            icon_custom_emoji_id=get_icon("CROSS")
        )
    )
    return builder.as_markup()

def get_parsing_mode_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Все данные",
            callback_data="parse_mode_all",
            style='primary',
            icon_custom_emoji_id=get_icon("USERS")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Только юзернеймы",
            callback_data="parse_mode_usernames",
            style='primary',
            icon_custom_emoji_id=get_icon("TAG")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Только имена",
            callback_data="parse_mode_names",
            style='primary',
            icon_custom_emoji_id=get_icon("NAMES")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Имена + юзернеймы",
            callback_data="parse_mode_names_usernames",
            style='primary',
            icon_custom_emoji_id=get_icon("PROFILE")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="functions",
            style='default',
            icon_custom_emoji_id=get_icon("BACK")
        )
    )
    return builder.as_markup()

def get_chat_selection_keyboard(chats: List[Dict], page: int = 0, selected_chats: List[str] = None) -> InlineKeyboardMarkup:
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
                icon_custom_emoji_id=get_icon("CHECK") if is_selected else get_icon("PEOPLE")
            )
        )
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton(
                text="⬅ Назад",
                callback_data=f"chats_page_{page-1}",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        )
    if end_idx < len(chats):
        nav_buttons.append(
            InlineKeyboardButton(
                text="Вперед ➡",
                callback_data=f"chats_page_{page+1}",
                style='default',
                icon_custom_emoji_id=get_icon("CHART_UP")
            )
        )
    if nav_buttons:
        builder.row(*nav_buttons)
    
    builder.row(
        InlineKeyboardButton(
            text=f"Готово (выбрано: {len(selected_chats)})",
            callback_data="chats_done",
            style='success',
            icon_custom_emoji_id=get_icon("CHECK")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Отмена",
            callback_data="functions",
            style='danger',
            icon_custom_emoji_id=get_icon("CROSS")
        )
    )
    
    return builder.as_markup()

# --- Хендлеры ---
@dp.message(Command("start"))
async def cmd_start(message: Message):
    user = message.from_user
    await register_user(user.id, user.username, user.first_name)
    
    welcome_text = f"""
{emoji('SMILE')} <b>Добро пожаловать в Vest Game Soft!</b>

{emoji('BOT')} Я помогу вам управлять аккаунтами и делать рассылки.

{emoji('PEOPLE')} <b>Менеджер аккаунтов</b> — добавление и управление аккаунтами
{emoji('APPS')} <b>Функции</b> — рассылка, автоответчик, парсинг
{emoji('SUPPORT')} <b>Поддержка:</b> {SUPPORT_USERNAME}

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
            icon_custom_emoji_id=get_icon("MEGAPHONE")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Обновить статистику",
            callback_data="admin_refresh_stats",
            style='default',
            icon_custom_emoji_id=get_icon("REFRESH")
        )
    )
    
    admin_text = f"""
{emoji('BOT')} <b>Админ-панель</b>

{emoji('PEOPLE')} Пользователей: <b>{stats['total_users']}</b>
{emoji('PROFILE')} Аккаунтов: <b>{stats['total_accounts']}</b>
{emoji('MEGAPHONE')} Всего рассылок: <b>{stats['total_broadcasts']}</b>
{emoji('PLAY')} Активных: <b>{stats['active_broadcasts']}</b>
    """
    
    await message.answer(admin_text, reply_markup=builder.as_markup())

# --- Главное меню ---
@dp.callback_query(F.data == "main_menu")
async def back_to_main(callback: CallbackQuery):
    await callback.message.edit_text(
        f"{emoji('SMILE')} <b>Главное меню</b>\n\nВыберите действие:",
        reply_markup=get_main_menu_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "account_manager")
async def account_manager(callback: CallbackQuery):
    await callback.message.edit_text(
        f"{emoji('PEOPLE')} <b>Менеджер аккаунтов</b>\n\nВыберите действие:",
        reply_markup=get_account_manager_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "functions")
async def functions(callback: CallbackQuery):
    await callback.message.edit_text(
        f"{emoji('APPS')} <b>Функции</b>\n\nВыберите функцию:",
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()

# --- Менеджер аккаунтов ---
@dp.callback_query(F.data == "add_account")
async def add_account(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"{emoji('PHONE')} <b>Добавление аккаунта</b>\n\n"
        f"Введите номер телефона в формате:\n"
        f"<code>+79991234567</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Отмена",
                callback_data="account_manager",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
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
            f"{emoji('CROSS')} Неверный формат номера. Попробуйте снова.\n"
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
            f"{emoji('CHECK')} Код подтверждения отправлен!\n\n"
            f"Введите код из Telegram:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="Отмена",
                    callback_data="account_manager",
                    style='default',
                    icon_custom_emoji_id=get_icon("BACK")
                )]
            ])
        )
        await state.set_state(AccountStates.waiting_for_code)
        
    except Exception as ex:
        await message.answer(f"{emoji('CROSS')} Ошибка: {str(ex)}")
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
                f"{emoji('LOCK_CLOSED')} Введите пароль 2FA:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text="Отмена",
                        callback_data="account_manager",
                        style='default',
                        icon_custom_emoji_id=get_icon("BACK")
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
            f"{emoji('CHECK')} Аккаунт успешно добавлен!",
            reply_markup=get_account_manager_keyboard()
        )
        await state.clear()
        
    except Exception as ex:
        await message.answer(f"{emoji('CROSS')} Ошибка: {str(ex)}")
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
            f"{emoji('CHECK')} Аккаунт успешно добавлен!",
            reply_markup=get_account_manager_keyboard()
        )
        await state.clear()
        
    except Exception as ex:
        await message.answer(f"{emoji('CROSS')} Ошибка: {str(ex)}")
        await state.clear()

@dp.callback_query(F.data == "my_accounts")
async def my_accounts(callback: CallbackQuery):
    accounts = await get_user_accounts(callback.from_user.id)
    
    if not accounts:
        await callback.message.edit_text(
            f"{emoji('INFO')} У вас пока нет аккаунтов.\n\n"
            f"Нажмите 'Добавить аккаунт' чтобы добавить новый.",
            reply_markup=get_account_manager_keyboard()
        )
    else:
        await callback.message.edit_text(
            f"{emoji('PEOPLE')} <b>Ваши аккаунты:</b>\n\n"
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
    
    warming_status = "✅ Включен" if account.get('warming_enabled') else "❌ Выключен"
    
    text = f"""
{emoji('PROFILE')} <b>Аккаунт:</b>
{emoji('PHONE')} Телефон: <code>{account['phone']}</code>
{emoji('EYE')} Статус: {'✅ Активен' if account['is_active'] else '❌ Неактивен'}
{emoji('FIRE')} Прогрев: {warming_status}
{emoji('CLOCK')} Создан: {account['created_at'].strftime('%d.%m.%Y %H:%M')}
    """
    
    await callback.message.edit_text(text, reply_markup=get_account_actions_keyboard(account_id, account.get('warming_enabled', False)))
    await callback.answer()

@dp.callback_query(F.data.startswith("account_logs_"))
async def account_logs(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[2])
    logs = await get_account_logs(account_id, 30)
    
    if not logs:
        await callback.answer("Логи пусты", show_alert=True)
        return
    
    log_text = f"{emoji('EYE')} <b>Логи аккаунта (последние 30):</b>\n\n"
    
    for log in logs:
        time_str = log['created_at'].astimezone(MSK_TZ).strftime('%d.%m %H:%M')
        direction = "📤" if log['direction'] == 'sent' else "📥"
        chat_name = escape(log['chat_name'] or str(log['chat_id']))
        msg_preview = escape((log['message_text'] or '')[:50])
        
        log_text += f"<code>{time_str}</code> {direction} <b>{chat_name}</b>"
        if msg_preview:
            log_text += f": {msg_preview}"
        log_text += "\n"
    
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Обновить",
            callback_data=f"account_logs_{account_id}",
            style='default',
            icon_custom_emoji_id=get_icon("REFRESH")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data=f"manage_account_{account_id}",
            style='default',
            icon_custom_emoji_id=get_icon("BACK")
        )
    )
    
    await callback.message.edit_text(log_text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("toggle_warming_"))
async def toggle_warming(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[2])
    account = await get_account(account_id)
    
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    
    new_state = not account.get('warming_enabled', False)
    await update_account_warming(account_id, new_state)
    
    status_text = "включен" if new_state else "выключен"
    await callback.answer(f"Прогрев {status_text}", show_alert=True)
    await manage_account(callback)

@dp.callback_query(F.data.startswith("delete_account_"))
async def delete_account_handler(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[2])
    
    if account_id in active_clients:
        try:
            await active_clients[account_id].disconnect()
        except:
            pass
        del active_clients[account_id]
    
    await delete_account(account_id)
    
    await callback.message.edit_text(
        f"{emoji('CHECK')} Аккаунт успешно удален!",
        reply_markup=get_account_manager_keyboard()
    )
    await callback.answer()

# --- Рассылка ---
@dp.callback_query(F.data == "broadcast")
async def broadcast(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"{emoji('SEND')} <b>Рассылка</b>\n\nВыберите режим рассылки:",
        reply_markup=get_broadcast_mode_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "scheduled_broadcast")
async def scheduled_broadcast_menu(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"{emoji('CLOCK')} <b>Отложенная рассылка</b>\n\nВыберите режим рассылки:",
        reply_markup=get_broadcast_mode_keyboard()
    )
    await state.update_data(is_scheduled=True)
    await callback.answer()

@dp.callback_query(F.data.startswith("mode_"))
async def select_mode(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.split("_")[1]
    data = await state.get_data()
    is_scheduled = data.get('is_scheduled', False)
    
    await state.update_data(mode=mode)
    
    accounts = await get_user_accounts(callback.from_user.id)
    if not accounts:
        await callback.message.edit_text(
            f"{emoji('CROSS')} У вас нет аккаунтов. Сначала добавьте аккаунт.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    await callback.message.edit_text(
        f"{emoji('PROFILE')} <b>Выберите аккаунт для рассылки:</b>",
        reply_markup=get_accounts_list_keyboard(accounts, "select_broadcast_account")
    )
    
    if is_scheduled:
        await state.set_state(ScheduledBroadcastStates.waiting_for_account)
    else:
        await state.set_state(BroadcastStates.waiting_for_account)
    
    await callback.answer()

async def handle_broadcast_account_selection(callback: CallbackQuery, state: FSMContext, is_scheduled: bool = False):
    account_id = int(callback.data.split("_")[3])
    await state.update_data(account_id=account_id)
    
    client = await get_client_for_account(account_id)
    if not client:
        await callback.answer("Не удалось подключиться к аккаунту", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"{emoji('LOADING')} Загружаю чаты...",
        reply_markup=None
    )
    
    chats = await get_chats_from_client(client)
    await state.update_data(chats=chats, selected_chats=[], current_page=0)
    
    await callback.message.edit_text(
        f"{emoji('PEOPLE')} <b>Выберите чаты для рассылки</b> (макс. 200)\n"
        f"Страница 1 из {(len(chats) - 1) // 10 + 1}",
        reply_markup=get_chat_selection_keyboard(chats, 0, [])
    )
    
    if is_scheduled:
        await state.set_state(ScheduledBroadcastStates.selecting_chats)
    else:
        await state.set_state(BroadcastStates.selecting_chats)
    
    await callback.answer()

@dp.callback_query(F.data.startswith("select_broadcast_account_"), BroadcastStates.waiting_for_account)
async def select_broadcast_account(callback: CallbackQuery, state: FSMContext):
    await handle_broadcast_account_selection(callback, state)

@dp.callback_query(F.data.startswith("select_broadcast_account_"), ScheduledBroadcastStates.waiting_for_account)
async def select_scheduled_broadcast_account(callback: CallbackQuery, state: FSMContext):
    await handle_broadcast_account_selection(callback, state, is_scheduled=True)

@dp.callback_query(F.data.startswith("toggle_chat_"))
async def toggle_chat(callback: CallbackQuery, state: FSMContext):
    chat_id = callback.data.split("toggle_chat_")[1]
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
        f"{emoji('PEOPLE')} <b>Выберите чаты для рассылки</b> (макс. 200)\n"
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
        f"{emoji('PEOPLE')} <b>Выберите чаты для рассылки</b> (макс. 200)\n"
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
        f"{emoji('CLOCK')} <b>Введите задержку между сообщениями</b>\n\n"
        f"От 10 до 300000 секунд:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Назад",
                callback_data="broadcast",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
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
        await message.answer(f"{emoji('CROSS')} Введите число от 10 до 300000:")
        return
    
    await state.update_data(delay=delay)
    
    await message.answer(
        f"{emoji('MAIL')} <b>Введите количество сообщений в каждый чат</b>\n\n"
        f"От 1 до 200000:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Назад",
                callback_data="broadcast",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
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
        await message.answer(f"{emoji('CROSS')} Введите число от 1 до 200000:")
        return
    
    await state.update_data(message_count=count)
    
    await message.answer(
        f"{emoji('WRITE')} <b>Введите сообщение для рассылки:</b>\n\n"
        f"Поддерживается HTML и премиум эмодзи.\n"
        f"Можно прикрепить медиа (фото, видео, документы).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Назад",
                callback_data="broadcast",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )]
        ])
    )
    await state.set_state(BroadcastStates.waiting_for_message)

@dp.message(BroadcastStates.waiting_for_message)
async def process_broadcast_message(message: Message, state: FSMContext):
    text = message.html_text if message.html_text else (message.text or message.caption or "")
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
{emoji('EYE')} <b>Предпросмотр рассылки:</b>

{emoji('PROFILE')} Аккаунт ID: {data['account_id']}
{emoji('PEOPLE')} Чатов: {len(data['selected_chats'])}
{emoji('CLOCK')} Задержка: {data['delay']} сек
{emoji('MAIL')} Сообщений в чат: {data['message_count']}
{emoji('GEAR')} Режим: {'Одновременный' if data['mode'] == 'simultaneous' else 'Рандомный'}
    """
    
    await message.answer(preview_text, reply_markup=get_broadcast_preview_keyboard())
    
    if media_paths and len(media_paths) > 0:
        if len(media_paths) == 1 and os.path.exists(media_paths[0]):
            await message.answer_document(
                FSInputFile(media_paths[0]),
                caption=text,
                parse_mode='HTML'
            )
    else:
        await message.answer(text, parse_mode='HTML')
    
    await state.set_state(BroadcastStates.preview)

@dp.callback_query(F.data == "start_broadcast", BroadcastStates.preview)
async def start_broadcast(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    
    chat_ids_str = [str(x) for x in data['selected_chats']]
    
    try:
        async with db_pool.acquire() as conn:
            broadcast_id = await conn.fetchval(
                '''INSERT INTO broadcasts 
                   (user_id, account_id, chat_ids, delay, message_count, message_text, message_media, mode)
                   VALUES ($1, $2, $3::text[], $4, $5, $6, $7::text[], $8)
                   RETURNING id''',
                user_id, data['account_id'], chat_ids_str,
                data['delay'], data['message_count'], data['message_text'],
                data['message_media'], data['mode']
            )
        
        task = asyncio.create_task(execute_broadcast(broadcast_id, user_id))
        active_broadcasts[broadcast_id] = task
        
        await callback.message.edit_text(
            f"{emoji('PLAY')} <b>Рассылка запущена!</b>\n\n"
            f"ID: {broadcast_id}\n"
            f"Чатов: {len(data['selected_chats'])}\n"
            f"Сообщений в чат: {data['message_count']}",
            reply_markup=get_broadcast_control_keyboard(broadcast_id)
        )
        await state.clear()
        
    except Exception as ex:
        logger.error(f"Error starting broadcast: {ex}")
        await callback.message.edit_text(
            f"{emoji('CROSS')} Ошибка: {str(ex)}",
            reply_markup=get_functions_keyboard()
        )
    
    await callback.answer()

# --- Отложенная рассылка ---
@dp.message(ScheduledBroadcastStates.waiting_for_delay)
async def scheduled_process_delay(message: Message, state: FSMContext):
    try:
        delay = int(message.text.strip())
        if delay < 10 or delay > 300000:
            raise ValueError
    except ValueError:
        await message.answer(f"{emoji('CROSS')} Введите число от 10 до 300000:")
        return
    
    await state.update_data(delay=delay)
    await message.answer(
        f"{emoji('MAIL')} <b>Введите количество сообщений в каждый чат</b> (1-200000):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="scheduled_broadcast", style='default', icon_custom_emoji_id=get_icon("BACK"))]
        ])
    )
    await state.set_state(ScheduledBroadcastStates.waiting_for_count)

@dp.message(ScheduledBroadcastStates.waiting_for_count)
async def scheduled_process_count(message: Message, state: FSMContext):
    try:
        count = int(message.text.strip())
        if count < 1 or count > 200000:
            raise ValueError
    except ValueError:
        await message.answer(f"{emoji('CROSS')} Введите число от 1 до 200000:")
        return
    
    await state.update_data(message_count=count)
    await message.answer(
        f"{emoji('WRITE')} <b>Введите сообщение для рассылки:</b>\n\nПоддерживается HTML и премиум эмодзи.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="scheduled_broadcast", style='default', icon_custom_emoji_id=get_icon("BACK"))]
        ])
    )
    await state.set_state(ScheduledBroadcastStates.waiting_for_message)

@dp.message(ScheduledBroadcastStates.waiting_for_message)
async def scheduled_process_message(message: Message, state: FSMContext):
    text = message.html_text if message.html_text else (message.text or message.caption or "")
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
    
    await message.answer(
        f"{emoji('CALENDAR')} <b>Введите дату и время отправки (МСК):</b>\n\n"
        f"Формат: <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n"
        f"Пример: <code>15.06.2026 14:30</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="scheduled_broadcast", style='default', icon_custom_emoji_id=get_icon("BACK"))]
        ])
    )
    await state.set_state(ScheduledBroadcastStates.waiting_for_datetime)

@dp.message(ScheduledBroadcastStates.waiting_for_datetime)
async def scheduled_process_datetime(message: Message, state: FSMContext):
    try:
        dt_str = message.text.strip()
        scheduled_dt = MSK_TZ.localize(datetime.strptime(dt_str, '%d.%m.%Y %H:%M'))
        
        if scheduled_dt <= datetime.now(MSK_TZ):
            await message.answer(f"{emoji('CROSS')} Дата должна быть в будущем!")
            return
        
        data = await state.get_data()
        chat_ids_str = [str(x) for x in data['selected_chats']]
        user_id = message.from_user.id
        
        async with db_pool.acquire() as conn:
            broadcast_id = await conn.fetchval(
                '''INSERT INTO broadcasts 
                   (user_id, account_id, chat_ids, delay, message_count, message_text, message_media, mode, status, scheduled_at)
                   VALUES ($1, $2, $3::text[], $4, $5, $6, $7::text[], $8, 'scheduled', $9)
                   RETURNING id''',
                user_id, data['account_id'], chat_ids_str,
                data['delay'], data['message_count'], data['message_text'],
                data['message_media'], data['mode'], scheduled_dt
            )
        
        await message.answer(
            f"{emoji('CHECK')} <b>Рассылка запланирована!</b>\n\n"
            f"ID: {broadcast_id}\n"
            f"Дата: {scheduled_dt.strftime('%d.%m.%Y %H:%M')} МСК\n"
            f"Чатов: {len(data['selected_chats'])}",
            reply_markup=get_functions_keyboard()
        )
        await state.clear()
        
    except ValueError:
        await message.answer(f"{emoji('CROSS')} Неверный формат. Используйте: ДД.ММ.ГГГГ ЧЧ:ММ")
    except Exception as ex:
        await message.answer(f"{emoji('CROSS')} Ошибка: {str(ex)}")
        await state.clear()

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
        f"{emoji('STOP')} <b>Рассылка остановлена!</b>\n\nID: {broadcast_id}",
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
        f"{emoji('PLAY')} <b>Рассылка возобновлена!</b>\n\nID: {broadcast_id}",
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
        f"{emoji('CHECK')} Рассылка удалена!",
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "my_broadcasts")
async def my_broadcasts(callback: CallbackQuery):
    broadcasts = await get_user_broadcasts(callback.from_user.id)
    
    if not broadcasts:
        await callback.message.edit_text(
            f"{emoji('INFO')} У вас пока нет рассылок.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for bc in broadcasts[:10]:
        status_text = {
            'active': '▶',
            'stopped': '⏹',
            'completed': '✅',
            'scheduled': '🕐'
        }.get(bc['status'], 'ℹ')
        
        progress = f"{bc['progress']}/{bc['total_count']}" if bc['total_count'] > 0 else "0/0"
        
        scheduled_info = ""
        if bc.get('scheduled_at'):
            scheduled_info = f" | {bc['scheduled_at'].strftime('%d.%m %H:%M')}"
        
        builder.row(
            InlineKeyboardButton(
                text=f"{status_text} ID:{bc['id']} | {progress} | {bc['status']}{scheduled_info}",
                callback_data=f"show_broadcast_{bc['id']}",
                style='default',
                icon_custom_emoji_id=get_icon("CHART")
            )
        )
    
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="functions",
            style='default',
            icon_custom_emoji_id=get_icon("BACK")
        )
    )
    
    await callback.message.edit_text(
        f"{emoji('CHART')} <b>Мои рассылки:</b>",
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
    
    scheduled_text = ""
    if bc.get('scheduled_at'):
        scheduled_text = f"\n{emoji('CALENDAR')} Запланирована: {bc['scheduled_at'].astimezone(MSK_TZ).strftime('%d.%m.%Y %H:%M')} МСК"
    
    text = f"""
{emoji('CHART')} <b>Рассылка ID: {bc['id']}</b>

{emoji('GEAR')} Статус: {bc['status']}{scheduled_text}
{emoji('STATS')} Прогресс: {progress}
{emoji('CLOCK')} Задержка: {bc['delay']} сек
{emoji('MAIL')} Сообщений в чат: {bc['message_count']}
{emoji('PEOPLE')} Чатов: {len(bc['chat_ids'])}
{emoji('CALENDAR')} Создана: {bc['created_at'].astimezone(MSK_TZ).strftime('%d.%m.%Y %H:%M')}
    """
    
    await callback.message.edit_text(text, reply_markup=get_broadcast_control_keyboard(bc['id']))
    await callback.answer()

# --- Автоответчик ---
@dp.callback_query(F.data == "auto_responder")
async def auto_responder(callback: CallbackQuery, state: FSMContext):
    accounts = await get_user_accounts(callback.from_user.id)
    if not accounts:
        await callback.message.edit_text(
            f"{emoji('CROSS')} У вас нет аккаунтов.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    await callback.message.edit_text(
        f"{emoji('PROFILE')} <b>Выберите аккаунт для автоответчика:</b>",
        reply_markup=get_accounts_list_keyboard(accounts, "select_responder_account")
    )
    await state.set_state(AutoResponderStates.waiting_for_account)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_responder_account_"))
async def select_responder_account(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[3])
    await state.update_data(account_id=account_id)
    
    await callback.message.edit_text(
        f"{emoji('WRITE')} <b>Введите слово-триггер:</b>\n\n"
        f"Или напишите <code>-</code> чтобы отвечать на все сообщения в ЛС.\n\n"
        f"{emoji('INFO')} <b>Доступные переменные:</b>\n"
        f"<code>{'{username}'}</code> - юзернейм\n"
        f"<code>{'{first_name}'}</code> - имя\n"
        f"<code>{'{last_name}'}</code> - фамилия\n"
        f"<code>{'{user_id}'}</code> - ID пользователя",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="functions", style='default', icon_custom_emoji_id=get_icon("BACK"))]
        ])
    )
    await state.set_state(AutoResponderStates.waiting_for_trigger)
    await callback.answer()

@dp.message(AutoResponderStates.waiting_for_trigger)
async def process_trigger(message: Message, state: FSMContext):
    trigger = message.text.strip()
    if not trigger:
        await message.answer(f"{emoji('CROSS')} Введите слово-триггер или '-'")
        return
    
    await state.update_data(trigger=trigger)
    await message.answer(
        f"{emoji('WRITE')} <b>Введите ответ:</b>\n\n"
        f"Поддерживается HTML, премиум эмодзи и переменные.\n"
        f"Можно прикрепить медиа.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="functions", style='default', icon_custom_emoji_id=get_icon("BACK"))]
        ])
    )
    await state.set_state(AutoResponderStates.waiting_for_response)

@dp.message(AutoResponderStates.waiting_for_response)
async def process_auto_response(message: Message, state: FSMContext):
    text = message.html_text if message.html_text else (message.text or message.caption or "")
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
{emoji('EYE')} <b>Предпросмотр автоответчика:</b>

{emoji('TAG')} Триггер: {escape(data['trigger'])}
{emoji('MEDIA')} Медиа: {len(media_paths)} файлов
    """
    
    await message.answer(preview_text, reply_markup=get_auto_responder_preview_keyboard())
    
    if media_paths and len(media_paths) > 0:
        if len(media_paths) == 1 and os.path.exists(media_paths[0]):
            await message.answer_document(FSInputFile(media_paths[0]), caption=text, parse_mode='HTML')
    else:
        await message.answer(text, parse_mode='HTML')
    
    await state.set_state(AutoResponderStates.preview)

@dp.callback_query(F.data == "create_auto_responder", AutoResponderStates.preview)
async def create_auto_responder(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        responder_id = await conn.fetchval(
            '''INSERT INTO auto_responders 
               (user_id, account_id, trigger, response_text, response_media)
               VALUES ($1, $2, $3, $4, $5::text[])
               RETURNING id''',
            user_id, data['account_id'], data['trigger'],
            data['response_text'], data['response_media']
        )
    
    await start_auto_responder(responder_id, user_id)
    
    await callback.message.edit_text(
        f"{emoji('CHECK')} <b>Автоответчик создан и запущен!</b>\n\n"
        f"ID: {responder_id}\n"
        f"Триггер: {escape(data['trigger'])}",
        reply_markup=get_functions_keyboard()
    )
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data == "my_auto_responders")
async def my_auto_responders(callback: CallbackQuery):
    responders = await get_user_auto_responders(callback.from_user.id)
    
    if not responders:
        await callback.message.edit_text(
            f"{emoji('INFO')} У вас пока нет автоответчиков.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for resp in responders:
        status = "✅" if resp['is_active'] else "❌"
        builder.row(
            InlineKeyboardButton(
                text=f"{status} ID:{resp['id']} | Триггер: {escape(resp['trigger'][:20])}",
                callback_data=f"show_responder_{resp['id']}",
                style='default',
                icon_custom_emoji_id=get_icon("BELL")
            )
        )
    
    builder.row(
        InlineKeyboardButton(
            text="Создать новый",
            callback_data="auto_responder",
            style='primary',
            icon_custom_emoji_id=get_icon("ADD_TEXT")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="functions",
            style='default',
            icon_custom_emoji_id=get_icon("BACK")
        )
    )
    
    await callback.message.edit_text(
        f"{emoji('BELL')} <b>Мои автоответчики:</b>",
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
                icon_custom_emoji_id=get_icon("STOP")
            )
        )
    else:
        builder.row(
            InlineKeyboardButton(
                text="Запустить",
                callback_data=f"start_responder_{responder_id}",
                style='success',
                icon_custom_emoji_id=get_icon("PLAY")
            )
        )
    
    builder.row(
        InlineKeyboardButton(
            text="Удалить",
            callback_data=f"delete_responder_{responder_id}",
            style='default',
            icon_custom_emoji_id=get_icon("DELETE")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Назад",
            callback_data="my_auto_responders",
            style='default',
            icon_custom_emoji_id=get_icon("BACK")
        )
    )
    
    text = f"""
{emoji('BELL')} <b>Автоответчик ID: {responder['id']}</b>

{emoji('EYE')} Статус: {'✅ Активен' if responder['is_active'] else '❌ Остановлен'}
{emoji('TAG')} Триггер: <code>{escape(responder['trigger'])}</code>
{emoji('WRITE')} Ответ: {escape((responder['response_text'] or '')[:100])}
{emoji('CLOCK')} Создан: {responder['created_at'].astimezone(MSK_TZ).strftime('%d.%m.%Y %H:%M')}
    """
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("stop_responder_"))
async def stop_responder(callback: CallbackQuery):
    responder_id = int(callback.data.split("_")[2])
    responder = await get_auto_responder(responder_id)
    
    if responder and responder['user_id'] == callback.from_user.id:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE auto_responders SET is_active = FALSE WHERE id = $1", responder_id)
        
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
            await conn.execute("UPDATE auto_responders SET is_active = TRUE WHERE id = $1", responder_id)
        
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

# --- Парсинг ---
@dp.callback_query(F.data == "parsing")
async def parsing_menu(callback: CallbackQuery, state: FSMContext):
    accounts = await get_user_accounts(callback.from_user.id)
    if not accounts:
        await callback.message.edit_text(
            f"{emoji('CROSS')} У вас нет аккаунтов.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    await callback.message.edit_text(
        f"{emoji('PROFILE')} <b>Выберите аккаунт для парсинга:</b>",
        reply_markup=get_accounts_list_keyboard(accounts, "select_parsing_account")
    )
    await state.set_state(ParsingStates.waiting_for_account)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_parsing_account_"))
async def select_parsing_account(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[3])
    await state.update_data(account_id=account_id)
    
    await callback.message.edit_text(
        f"{emoji('GLOBE')} <b>Введите юзернейм или ссылку на чат:</b>\n\n"
        f"Пример: <code>@chatname</code> или <code>https://t.me/chatname</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="functions", style='default', icon_custom_emoji_id=get_icon("BACK"))]
        ])
    )
    await state.set_state(ParsingStates.waiting_for_chat)
    await callback.answer()

@dp.message(ParsingStates.waiting_for_chat)
async def process_parsing_chat(message: Message, state: FSMContext):
    chat_input = message.text.strip()
    data = await state.get_data()
    
    if chat_input.startswith('@'):
        chat_username = chat_input
    elif 't.me/' in chat_input:
        chat_username = '@' + chat_input.split('t.me/')[-1].split('/')[0]
    else:
        chat_username = '@' + chat_input
    
    await state.update_data(chat_username=chat_username)
    
    await message.answer(
        f"{emoji('GLOBE')} <b>Выберите режим парсинга:</b>\n\n"
        f"Чат: <code>{chat_username}</code>",
        reply_markup=get_parsing_mode_keyboard()
    )
    await state.set_state(ParsingStates.waiting_for_mode)

@dp.callback_query(ParsingStates.waiting_for_mode, F.data.startswith("parse_mode_"))
async def process_parsing_mode(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.replace("parse_mode_", "")
    data = await state.get_data()
    account_id = data['account_id']
    chat_username = data['chat_username']
    
    client = await get_client_for_account(account_id)
    if not client:
        await callback.message.edit_text(f"{emoji('CROSS')} Не удалось подключиться к аккаунту.")
        await state.clear()
        return
    
    await callback.message.edit_text(f"{emoji('LOADING')} Собираю участников из <code>{chat_username}</code>...\nПроверяю последние 5000 сообщений...")
    
    try:
        entity = await client.get_entity(chat_username)
        
        users = []
        count = 0
        async for msg in client.iter_messages(entity, limit=5000):
            if msg.sender_id and not any(u['user_id'] == msg.sender_id for u in users):
                try:
                    sender = await msg.get_sender()
                    if sender and isinstance(sender, User):
                        user_data = {
                            'user_id': sender.id,
                            'username': ('@' + sender.username) if sender.username else '',
                            'first_name': sender.first_name or '',
                            'last_name': sender.last_name or '',
                        }
                        users.append(user_data)
                        count += 1
                except:
                    pass
        
        # Формируем файл в зависимости от режима
        filename = f"parsed_{chat_username.replace('@', '')}_{mode}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        filepath = f"media/{filename}"
        
        mode_names = {
            'all': 'Все данные',
            'usernames': 'Только юзернеймы',
            'names': 'Только имена',
            'names_usernames': 'Имена + юзернеймы'
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for user in users:
                if mode == 'all':
                    f.write(f"{user['user_id']}|{user['username']}|{user['first_name']}|{user['last_name']}\n")
                elif mode == 'usernames':
                    if user['username']:
                        f.write(f"{user['username']}\n")
                elif mode == 'names':
                    name = ' '.join(filter(None, [user['first_name'], user['last_name']]))
                    if name:
                        f.write(f"{name}\n")
                elif mode == 'names_usernames':
                    name = ' '.join(filter(None, [user['first_name'], user['last_name']]))
                    f.write(f"{name}|{user['username']}\n")
        
        await callback.message.answer_document(
            FSInputFile(filepath),
            caption=f"{emoji('CHECK')} <b>Парсинг завершён!</b>\n\n"
                    f"Чат: <code>{chat_username}</code>\n"
                    f"Режим: {mode_names.get(mode, mode)}\n"
                    f"Собрано пользователей: <b>{len(users)}</b>\n"
                    f"Проверено сообщений: {count}",
            parse_mode='HTML'
        )
        
        os.remove(filepath)
        
    except Exception as ex:
        await callback.message.edit_text(f"{emoji('CROSS')} Ошибка: {str(ex)}")
    
    await state.clear()
    await callback.answer()

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
            icon_custom_emoji_id=get_icon("MEGAPHONE")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Обновить статистику",
            callback_data="admin_refresh_stats",
            style='default',
            icon_custom_emoji_id=get_icon("REFRESH")
        )
    )
    
    admin_text = f"""
{emoji('BOT')} <b>Админ-панель</b>

{emoji('PEOPLE')} Пользователей: <b>{stats['total_users']}</b>
{emoji('PROFILE')} Аккаунтов: <b>{stats['total_accounts']}</b>
{emoji('MEGAPHONE')} Всего рассылок: <b>{stats['total_broadcasts']}</b>
{emoji('PLAY')} Активных: <b>{stats['active_broadcasts']}</b>
    """
    
    await callback.message.edit_text(admin_text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_broadcast_all")
async def admin_broadcast_all(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"{emoji('MEGAPHONE')} <b>Рассылка всем пользователям</b>\n\n"
        f"Введите сообщение для рассылки:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="admin_refresh_stats", style='default', icon_custom_emoji_id=get_icon("BACK"))]
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
    
    broadcast_text = message.html_text if message.html_text else (message.text or message.caption or "")
    
    success = 0
    for user in users:
        try:
            await bot.send_message(user['user_id'], broadcast_text)
            success += 1
            await asyncio.sleep(0.05)
        except Exception as ex:
            logger.error(f"Failed to send to {user['user_id']}: {ex}")
    
    await message.answer(
        f"{emoji('CHECK')} <b>Рассылка завершена!</b>\n\n"
        f"Отправлено: {success}/{len(users)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="В админ-панель", callback_data="admin_refresh_stats", style='primary', icon_custom_emoji_id=get_icon("BACK"))]
        ])
    )
    await state.clear()

# --- Запуск бота ---
async def on_startup():
    os.makedirs("media", exist_ok=True)
    await init_db()
    
    async with db_pool.acquire() as conn:
        responders = await conn.fetch("SELECT * FROM auto_responders WHERE is_active = TRUE")
        for responder in responders:
            responder = dict(responder)
            await start_auto_responder(responder['id'], responder['user_id'])
    
    asyncio.create_task(check_scheduled_broadcasts())

async def main():
    await on_startup()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
