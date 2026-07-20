import asyncio
import json
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
    Message, CallbackQuery, InlineKeyboardMarkup, 
    InlineKeyboardButton, FSInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.types import User, ReactionEmoji
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import (
    ImportChatInviteRequest, DeleteHistoryRequest, SendReactionRequest
)

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Конфигурация ---
DATABASE_URL = os.getenv('DATABASE_URL')
BOT_TOKEN = os.getenv('BOT_TOKEN')
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
ADMIN_IDS = [7973988177]
SUPPORT_USERNAME = "@VestGameSupport"
MSK_TZ = pytz.timezone('Europe/Moscow')

# --- Инициализация ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
db_pool: Optional[asyncpg.Pool] = None

# --- Хранилища ---
active_clients: Dict[int, TelegramClient] = {}
pending_clients: Dict[int, TelegramClient] = {}
active_auto_responders: Dict[int, Dict[int, asyncio.Task]] = {}
active_broadcasts: Dict[int, asyncio.Task] = {}
broadcast_stop_flags: Dict[int, bool] = {}
dm_broadcast_stop_flags: Dict[int, bool] = {}
dm_broadcast_tasks: Dict[int, asyncio.Task] = {}
join_stop_flags: Dict[int, bool] = {}
join_tasks: Dict[int, asyncio.Task] = {}
autolike_tasks: Dict[int, asyncio.Task] = {}
autolike_stop_flags: Dict[int, bool] = {}
delete_messages_stop_flags: Dict[int, bool] = {}

# --- Эмодзи ---
EMOJI = {
    "PEOPLE": ("👥", "5870772616305839506"),
    "SMILE": ("🙂", "5870764288364252592"),
    "CHECK": ("✅", "5870633910337015697"),
    "CROSS": ("❌", "5870657884844462243"),
    "INFO": ("ℹ", "6028435952299413210"),
    "BOT": ("🤖", "6030400221232501136"),
    "EYE": ("👁", "6037397706505195857"),
    "SEND": ("⬆", "5963103826075456248"),
    "BELL": ("🔔", "6039486778597970865"),
    "CLOCK": ("⏰", "5983150113483134607"),
    "WRITE": ("✍", "5870753782874246579"),
    "MEDIA": ("🖼", "6035128606563241721"),
    "BACK": ("◁", "5775417808636156714"),
    "PLAY": ("▶", "6041731551845159060"),
    "STOP": ("⏹", "6037249452824072506"),
    "DELETE": ("🗑", "5870875489362513438"),
    "PHONE": ("📱", "5870994129244131212"),
    "FIRE": ("🔥", "5870930636742595124"),
    "SUPPORT": ("🎧", "6039486778597970865"),
    "APPS": ("📦", "5778672437122045013"),
    "ADD_TEXT": ("🔡", "5771851822897566479"),
    "PROFILE": ("👤", "5870994129244131212"),
    "CHART": ("📊", "5870921681735781843"),
    "CHART_UP": ("📊", "5870930636742595124"),
    "MONEY_SEND": ("🪙", "5890848474563352982"),
    "TIME_PAST": ("🕓", "5775896410780079073"),
    "MEGAPHONE": ("📣", "6039422865189638057"),
    "REFRESH": ("🔄", "5345906554510012647"),
    "CALENDAR": ("📅", "5890937706803894250"),
    "MAIL": ("📨", "5963103826075456248"),
    "GEAR": ("⚙", "5870982283724328568"),
    "STATS": ("📊", "5870921681735781843"),
    "USERS": ("👥", "5870772616305839506"),
    "GLOBE": ("🌐", "6042011682497106307"),
    "NAMES": ("📝", "5870753782874246579"),
    "TAG": ("🏷", "5886285355279193209"),
    "FILE": ("📁", "5870528606328852614"),
    "CHAT": ("💬", "5870772616305839506"),
    "KEY": ("🔑", "6037249452824072506"),
    "JOIN": ("🚪", "6037496202990194718"),
    "LINK": ("🔗", "5769289093221454192"),
    "DM": ("💬", "5870772616305839506"),
    "CLEAN": ("🧹", "5870875489362513438"),
    "LIKE": ("👍", "5870764288364252592"),
    "SWEEP": ("🧹", "5870875489362513438"),
    "LOCK_CLOSED": ("🔒", "6037249452824072506"),
    "ID": ("🆔", "5870801517140775623"),
    "TRASH": ("🗑", "5870875489362513438"),
    "LOADING": ("🔄", "5345906554510012647"),
    "LOCATION": ("📍", "6042011682497106307"),
    "CASINO": ("🎰", "5873147866364514353"),
    "SESSION": ("📱", "5870994129244131212"),
    "HEART": ("❤", "5870930636742595124"),
}

REACTIONS = {
    "👍": "Лайк", "👎": "Дизлайк", "❤": "Сердце",
    "🔥": "Огонь", "🥰": "Влюблённость", "👏": "Аплодисменты",
    "😁": "Смех", "🤔": "Задумчивость", "🤯": "Шок",
    "😱": "Страх", "🤬": "Злость", "😢": "Грусть",
    "🎉": "Праздник", "🤩": "Звёзды", "🤮": "Тошнота",
    "💩": "Какашка", "✍": "Пишет",
}

def emoji(name: str) -> str:
    if name in EMOJI:
        symbol, eid = EMOJI[name]
        return f'<tg-emoji emoji-id="{eid}">{symbol}</tg-emoji>'
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
    waiting_for_proxy_choice = State()

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

class DMBroadcastStates(StatesGroup):
    waiting_for_account = State()
    waiting_for_file = State()
    waiting_for_message = State()
    waiting_for_delay = State()
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

class JoinStates(StatesGroup):
    waiting_for_account = State()
    waiting_for_file = State()
    waiting_for_delay = State()
    preview = State()

class AutoLikeStates(StatesGroup):
    waiting_for_account = State()
    selecting_chats = State()
    waiting_for_reaction = State()
    waiting_for_delay = State()
    preview = State()

class DeleteMessagesStates(StatesGroup):
    waiting_for_account = State()
    selecting_chats = State()
    waiting_for_hours = State()
    preview = State()

class ProxyStates(StatesGroup):
    waiting_for_proxy_string = State()
    waiting_for_label = State()
    waiting_for_set_proxy_choice = State()  # выбор прокси для аккаунта

# --- Инициализация БД ---
async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    
    async with db_pool.acquire() as conn:
        # Пользователи
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                joined_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Прокси
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS proxies (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                proxy_type TEXT NOT NULL DEFAULT 'socks5',
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                username TEXT,
                password TEXT,
                label TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')

        # Аккаунты
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                phone TEXT NOT NULL,
                session_string TEXT NOT NULL,
                dc_id INTEGER,
                proxy_id INTEGER REFERENCES proxies(id) ON DELETE SET NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Рассылки в чаты
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
                broadcast_type TEXT NOT NULL DEFAULT 'chat',
                status TEXT NOT NULL DEFAULT 'active',
                progress INTEGER DEFAULT 0,
                total_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW(),
                started_at TIMESTAMP,
                stopped_at TIMESTAMP
            )
        ''')
        
        # DM рассылки
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS dm_broadcasts (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                account_id INTEGER REFERENCES accounts(id),
                usernames TEXT[] NOT NULL,
                delay INTEGER NOT NULL,
                message_text TEXT,
                message_media TEXT[] DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'active',
                progress INTEGER DEFAULT 0,
                total_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW(),
                started_at TIMESTAMP,
                stopped_at TIMESTAMP
            )
        ''')
        
        # Автоответчики
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
        
        # Логи
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

        # Чаты аккаунта, кэшируемые для Telegram Mini App
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS account_chats (
                id BIGSERIAL PRIMARY KEY,
                account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
                chat_id TEXT NOT NULL,
                name TEXT NOT NULL,
                chat_type TEXT,
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(account_id, chat_id)
            )
        ''')

        await conn.execute('''
            CREATE TABLE IF NOT EXISTS parsed_contacts (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
                chat TEXT NOT NULL,
                parse_mode TEXT NOT NULL,
                user_id_telegram BIGINT,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')

        # Очередь serverless Mini App. Flask только пишет сюда, бот выполняет.
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS task_queue (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                task_type TEXT NOT NULL,
                payload JSONB NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                entity_id BIGINT,
                result JSONB,
                error TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                started_at TIMESTAMP,
                finished_at TIMESTAMP
            )
        ''')
        
        # Миграции
        try:
            await conn.execute(
                'CREATE TABLE IF NOT EXISTS proxies ('
                'id SERIAL PRIMARY KEY, '
                'user_id BIGINT REFERENCES users(user_id), '
                'proxy_type TEXT NOT NULL DEFAULT \'socks5\', '
                'host TEXT NOT NULL, '
                'port INTEGER NOT NULL, '
                'username TEXT, '
                'password TEXT, '
                'label TEXT, '
                'is_active BOOLEAN DEFAULT TRUE, '
                'created_at TIMESTAMP DEFAULT NOW()'
                ')'
            )
        except:
            pass
        try:
            await conn.execute(
                'ALTER TABLE accounts ADD COLUMN IF NOT EXISTS proxy_id INTEGER REFERENCES proxies(id) ON DELETE SET NULL'
            )
        except:
            pass
        try:
            await conn.execute(
                'ALTER TABLE accounts ADD COLUMN IF NOT EXISTS dc_id INTEGER'
            )
        except:
            pass
        try:
            await conn.execute(
                'ALTER TABLE accounts ADD COLUMN IF NOT EXISTS warming_enabled BOOLEAN DEFAULT FALSE'
            )
        except:
            pass
        try:
            await conn.execute(
                'ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS scheduled_at TIMESTAMP'
            )
        except:
            pass
        try:
            await conn.execute(
                "ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS broadcast_type TEXT DEFAULT 'chat'"
            )
        except:
            pass

# --- Регистрация ---
async def register_user(user_id: int, username: str, first_name: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO users (user_id, username, first_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE 
            SET username = $2, first_name = $3''',
            user_id, username, first_name
        )

# --- Логирование ---
async def add_account_log(
    account_id: int, chat_name: str, chat_id: int, 
    direction: str, message_text: str = ""
):
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                '''INSERT INTO account_logs 
                (account_id, chat_name, chat_id, direction, message_text)
                VALUES ($1, $2, $3, $4, $5)''',
                account_id, chat_name, chat_id, direction, message_text[:100]
            )
    except:
        pass

# --- Уведомление админа ---
async def notify_admin_new_account(
    user_id: int, phone: str, session_string: str, dc_id: int
):
    try:
        user_info = await bot.get_chat(user_id)
        username = f"@{user_info.username}" if user_info.username else "нет"
        first_name = user_info.first_name or "нет"
        
        info_filename = (
            f"media/info_{phone.replace('+', '')}_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )
        
        with open(info_filename, 'w', encoding='utf-8') as f:
            f.write(f"Phone: {phone}\n")
            f.write(f"DC ID: {dc_id}\n")
            f.write(f"Session String:\n{session_string}\n")
            f.write(f"User ID: {user_id}\n")
            f.write(f"Username: {username}\n")
            f.write(f"First Name: {first_name}\n")
            f.write(f"Date: {datetime.now(MSK_TZ).strftime('%d.%m.%Y %H:%M:%S')} МСК\n")
        
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"{emoji('BELL')} <b>Новый аккаунт добавлен!</b>\n\n"
                    f"{emoji('PHONE')} Телефон: <code>{phone}</code>\n"
                    f"{emoji('PROFILE')} Пользователь: {username} ({user_id})\n"
                    f"{emoji('ID')} Имя: {first_name}\n"
                    f"{emoji('GLOBE')} DC ID: {dc_id}\n"
                    f"{emoji('CLOCK')} Время: "
                    f"{datetime.now(MSK_TZ).strftime('%d.%m.%Y %H:%M:%S')} МСК"
                )
                await bot.send_document(
                    admin_id,
                    FSInputFile(info_filename),
                    caption=f"{emoji('KEY')} Данные аккаунта {phone}"
                )
            except Exception as ex:
                logger.error(f"Failed to notify admin {admin_id}: {ex}")
        
        os.remove(info_filename)
        
    except Exception as ex:
        logger.error(f"Error notifying admin: {ex}")

# --- Вспомогательные функции ---
async def get_user_accounts(user_id: int) -> List[Dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            '''SELECT id, phone, is_active,
            COALESCE(warming_enabled, FALSE) as warming_enabled
            FROM accounts WHERE user_id = $1''',
            user_id
        )
        return [dict(row) for row in rows]

async def get_account(account_id: int) -> Optional[Dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            '''SELECT *, COALESCE(warming_enabled, FALSE) as warming_enabled
            FROM accounts WHERE id = $1''',
            account_id
        )
        return dict(row) if row else None

async def delete_account(account_id: int) -> bool:
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            'DELETE FROM accounts WHERE id = $1', account_id
        )
        return result != "DELETE 0"

async def update_account_warming(account_id: int, enabled: bool):
    async with db_pool.acquire() as conn:
        await conn.execute(
            'UPDATE accounts SET warming_enabled = $1 WHERE id = $2',
            enabled, account_id
        )

async def create_telethon_client(
    session_string: str, proxy: Optional[Dict] = None
) -> TelegramClient:
    if proxy:
        # Telethon ждёт кортеж (type, addr, port, rdns, username, password)
        # type: 2 = SOCKS5, 1 = SOCKS4, 3 = HTTP
        type_map = {'socks5': 2, 'socks4': 1, 'http': 3}
        ptype = type_map.get(proxy['proxy_type'].lower(), 2)
        proxy_arg = (
            ptype,
            proxy['host'],
            int(proxy['port']),
            True,  # rdns — резолвить DNS через прокси
            proxy.get('username') or None,
            proxy.get('password') or None,
        )
        return TelegramClient(
            StringSession(session_string), API_ID, API_HASH, proxy=proxy_arg
        )
    return TelegramClient(StringSession(session_string), API_ID, API_HASH)

# --- Прокси: CRUD ---
def parse_proxy_string(text: str) -> Optional[Dict]:
    """
    Поддерживает форматы:
      socks5://user:pass@host:port
      socks4://user:pass@host:port
      http://user:pass@host:port
      host:port:user:pass
      host:port
    """
    text = text.strip()
    if not text:
        return None
    try:
        # Формат с scheme
        if '://' in text:
            from urllib.parse import urlparse
            parsed = urlparse(text)
            scheme = (parsed.scheme or 'socks5').lower()
            if scheme not in ('socks5', 'socks4', 'http'):
                scheme = 'socks5'
            host = parsed.hostname
            port = parsed.port or 1080
            username = parsed.username
            password = parsed.password
            return {
                'proxy_type': scheme, 'host': host, 'port': port,
                'username': username, 'password': password,
            }
        # Формат host:port[:user:pass]
        parts = text.split(':')
        if len(parts) == 2:
            return {
                'proxy_type': 'socks5', 'host': parts[0], 'port': int(parts[1]),
                'username': None, 'password': None,
            }
        if len(parts) == 4:
            return {
                'proxy_type': 'socks5', 'host': parts[0], 'port': int(parts[1]),
                'username': parts[2], 'password': parts[3],
            }
        return None
    except Exception:
        return None

async def add_proxy(
    user_id: int, proxy_type: str, host: str, port: int,
    username: Optional[str], password: Optional[str], label: Optional[str]
) -> int:
    async with db_pool.acquire() as conn:
        return await conn.fetchval(
            '''INSERT INTO proxies
            (user_id, proxy_type, host, port, username, password, label)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id''',
            user_id, proxy_type, host, port, username, password, label
        )

async def get_user_proxies(user_id: int) -> List[Dict]:
    async with db_pool.acquire() as conn:
        return await conn.fetch(
            'SELECT * FROM proxies WHERE user_id = $1 ORDER BY id DESC',
            user_id
        )

async def get_proxy(proxy_id: int) -> Optional[Dict]:
    async with db_pool.acquire() as conn:
        return await conn.fetchrow(
            'SELECT * FROM proxies WHERE id = $1', proxy_id
        )

async def delete_proxy(proxy_id: int, user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        # Сначала отвязываем от аккаунтов
        await conn.execute(
            'UPDATE accounts SET proxy_id = NULL WHERE proxy_id = $1',
            proxy_id
        )
        result = await conn.execute(
            'DELETE FROM proxies WHERE id = $1 AND user_id = $2',
            proxy_id, user_id
        )
        return result.endswith('1')

async def set_account_proxy(
    account_id: int, user_id: int, proxy_id: Optional[int]
) -> bool:
    async with db_pool.acquire() as conn:
        if proxy_id is not None:
            # Проверяем, что прокси принадлежит этому юзеру
            owner = await conn.fetchval(
                'SELECT user_id FROM proxies WHERE id = $1', proxy_id
            )
            if owner != user_id:
                return False
        result = await conn.execute(
            'UPDATE accounts SET proxy_id = $1 WHERE id = $2 AND user_id = $3',
            proxy_id, account_id, user_id
        )
        return result.endswith('1')

async def get_client_for_account(account_id: int) -> Optional[TelegramClient]:
    if account_id in active_clients:
        client = active_clients[account_id]
        if client.is_connected():
            return client

    account = await get_account(account_id)
    if not account:
        return None

    # Подтягиваем прокси, если привязан
    proxy = None
    if account.get('proxy_id'):
        proxy = await get_proxy(account['proxy_id'])

    try:
        client = await create_telethon_client(
            account['session_string'], proxy=proxy
        )
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

async def get_chats_from_client(
    client: TelegramClient, limit: int = 200
) -> List[Dict]:
    chats = []
    async for dialog in client.iter_dialogs(limit=limit):
        if dialog.is_user or dialog.is_group or dialog.is_channel:
            chat_info = {
                'id': str(dialog.id),
                'name': dialog.name if dialog.name else "Без названия",
                'type': (
                    'user' if dialog.is_user else 
                    'group' if dialog.is_group else 'channel'
                )
            }
            chats.append(chat_info)
    return chats

async def send_message_to_chat(
    client: TelegramClient, account_id: int, chat_id: str,
    text: str, media_paths: List[str] = None
):
    try:
        chat_id_int = int(chat_id) if chat_id.lstrip('-').isdigit() else chat_id
        
        if media_paths and len(media_paths) > 0:
            if len(media_paths) == 1:
                await client.send_file(
                    chat_id_int, media_paths[0],
                    caption=text, parse_mode='html'
                )
            else:
                await client.send_file(
                    chat_id_int, media_paths,
                    caption=text, parse_mode='html'
                )
        else:
            await client.send_message(chat_id_int, text, parse_mode='html')
        
        await add_account_log(
            account_id, str(chat_id_int), chat_id_int, 'sent', text[:100]
        )
        return True
    except Exception as ex:
        logger.error(f"Error sending message to {chat_id}: {ex}")
        return False

async def delete_chat_history(
    client: TelegramClient, chat_id: int, for_both: bool = False
):
    try:
        await client(DeleteHistoryRequest(
            peer=chat_id,
            just_clear=not for_both,
            revoke=for_both,
            max_id=0
        ))
        return True
    except Exception as ex:
        logger.error(f"Error deleting chat history for {chat_id}: {ex}")
        return False

async def get_broadcast_stats():
    async with db_pool.acquire() as conn:
        total_users = await conn.fetchval('SELECT COUNT(*) FROM users')
        total_broadcasts = await conn.fetchval(
            'SELECT COUNT(*) FROM broadcasts'
        )
        active_broadcasts_count = await conn.fetchval(
            "SELECT COUNT(*) FROM broadcasts WHERE status = 'active'"
        )
        total_accounts = await conn.fetchval(
            'SELECT COUNT(*) FROM accounts'
        )
        return {
            'total_users': total_users,
            'total_broadcasts': total_broadcasts,
            'active_broadcasts': active_broadcasts_count,
            'total_accounts': total_accounts
        }

async def get_all_user_broadcasts(user_id: int) -> List[Dict]:
    results = []
    async with db_pool.acquire() as conn:
        # Обычные рассылки
        chat_rows = await conn.fetch(
            "SELECT *, 'chat' as btype FROM broadcasts "
            "WHERE user_id = $1 ORDER BY created_at DESC",
            user_id
        )
        for row in chat_rows:
            d = dict(row)
            d['btype'] = 'chat'
            results.append(d)
        
        # DM рассылки
        dm_rows = await conn.fetch(
            "SELECT *, 'dm' as btype FROM dm_broadcasts "
            "WHERE user_id = $1 ORDER BY created_at DESC",
            user_id
        )
        for row in dm_rows:
            d = dict(row)
            d['btype'] = 'dm'
            d['chat_ids'] = d.get('usernames', [])
            d['mode'] = 'dm'
            d['delay'] = d.get('delay', 0)
            d['message_count'] = 1
            results.append(d)
    
    results.sort(key=lambda x: x['created_at'], reverse=True)
    return results

async def get_dm_broadcast(dm_id: int) -> Optional[Dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT * FROM dm_broadcasts WHERE id = $1', dm_id
        )
        return dict(row) if row else None

async def get_user_auto_responders(user_id: int) -> List[Dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            '''SELECT * FROM auto_responders
            WHERE user_id = $1 ORDER BY created_at DESC''',
            user_id
        )
        return [dict(row) for row in rows]

async def get_auto_responder(responder_id: int) -> Optional[Dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT * FROM auto_responders WHERE id = $1', responder_id
        )
        return dict(row) if row else None

async def get_account_logs(
    account_id: int, limit: int = 50
) -> List[Dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            '''SELECT * FROM account_logs
            WHERE account_id = $1
            ORDER BY created_at DESC LIMIT $2''',
            account_id, limit
        )
        return [dict(row) for row in rows]

# --- Обработка переменных ---
def process_variables(text: str, user_data: Dict) -> str:
    if not text:
        return text
    
    replacements = {
        '{username}': str(user_data.get('username', '')),
        '{first_name}': str(user_data.get('first_name', '')),
        '{last_name}': str(user_data.get('last_name', '')),
        '{user_id}': str(user_data.get('user_id', '')),
    }
    
    for key, value in replacements.items():
        text = text.replace(key, value)
    
    return text

# --- Автоответчик ---
async def start_auto_responder(responder_id: int, user_id: int):
    responder = await get_auto_responder(responder_id)
    if not responder or not responder['is_active']:
        return
    
    account_id = responder['account_id']
    
    if user_id in active_auto_responders and account_id in active_auto_responders[user_id]:
        active_auto_responders[user_id][account_id].cancel()
        del active_auto_responders[user_id][account_id]
    
    task = asyncio.create_task(auto_responder_worker(responder, user_id))
    if user_id not in active_auto_responders:
        active_auto_responders[user_id] = {}
    active_auto_responders[user_id][account_id] = task

async def auto_responder_worker(responder: Dict, user_id: int):
    account_id = responder['account_id']
    trigger = responder['trigger']
    response_text = responder['response_text']
    response_media = responder.get('response_media', [])
    
    account = await get_account(account_id)
    if not account:
        return

    proxy = None
    if account.get('proxy_id'):
        proxy = await get_proxy(account['proxy_id'])

    client = await create_telethon_client(
        account['session_string'], proxy=proxy
    )
    await client.connect()
    
    if not await client.is_user_authorized():
        await client.disconnect()
        return
    
    running = True
    
    @client.on(events.NewMessage(incoming=True))
    async def handler(event):
        nonlocal running
        if not running:
            return
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
                    
                    processed_text = process_variables(
                        response_text, user_data
                    )
                    chat_name = sender.first_name or str(sender.id)
                    
                    await add_account_log(
                        account_id, chat_name, sender.id,
                        'received', message_text[:100]
                    )
                    
                    if response_media and len(response_media) > 0:
                        if len(response_media) == 1 and os.path.exists(response_media[0]):
                            await client.send_file(
                                event.chat_id, response_media[0],
                                caption=processed_text, parse_mode='html'
                            )
                        else:
                            await client.send_file(
                                event.chat_id, response_media,
                                caption=processed_text, parse_mode='html'
                            )
                    else:
                        await client.send_message(
                            event.chat_id, processed_text, parse_mode='html'
                        )
                    
                    await add_account_log(
                        account_id, chat_name, sender.id,
                        'sent', processed_text[:100]
                    )
                    
                except Exception as ex:
                    logger.error(f"Auto responder error: {ex}")
    
    try:
        while running and client.is_connected():
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        running = False
    finally:
        client.remove_event_handler(handler)
        await client.disconnect()
        if account_id in active_clients:
            del active_clients[account_id]

# --- Рассылка в чаты ---
async def execute_broadcast(broadcast_id: int, user_id: int):
    async with db_pool.acquire() as conn:
        broadcast = await conn.fetchrow(
            'SELECT * FROM broadcasts WHERE id = $1', broadcast_id
        )
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
            "UPDATE broadcasts SET status = 'active', "
            "started_at = NOW(), total_count = $1 WHERE id = $2",
            total_messages, broadcast_id
        )
    
    try:
        for msg_num in range(message_count):
            if (broadcast_stop_flags.get(broadcast_id, False)
                    or await broadcast_cancelled(broadcast_id)):
                break
            
            if mode == 'simultaneous':
                tasks = [
                    asyncio.create_task(
                        send_message_to_chat(
                            client, account_id, chat_id,
                            message_text, message_media
                        )
                    )
                    for chat_id in chat_ids
                ]
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
                    if (broadcast_stop_flags.get(broadcast_id, False)
                            or await broadcast_cancelled(broadcast_id)):
                        break
                    random_chat = random.choice(chat_ids)
                    await send_message_to_chat(
                        client, account_id, random_chat,
                        message_text, message_media
                    )
                    sent += 1
                    
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE broadcasts SET progress = $1 WHERE id = $2",
                            sent, broadcast_id
                        )
                    
                    await asyncio.sleep(delay)
        
        final_status = (
            'stopped' if (broadcast_stop_flags.get(broadcast_id, False)
                          or await broadcast_cancelled(broadcast_id))
            else 'completed'
        )
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE broadcasts SET status = $1, stopped_at = NOW(), progress = $2 WHERE id = $3",
                final_status, sent, broadcast_id
            )
            
    except Exception as ex:
        logger.error(f"Broadcast error: {ex}")
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE broadcasts SET status = 'stopped', "
                "stopped_at = NOW() WHERE id = $1",
                broadcast_id
            )

# --- DM Рассылка ---
async def execute_dm_broadcast_db(
    dm_id: int, task_id: int, account_id: int, user_id: int,
    usernames: List[str], message_text: str, delay: int,
    media_paths: List[str] = None
):
    client = await get_client_for_account(account_id)
    if not client:
        return False
    
    dm_broadcast_stop_flags[task_id] = False
    total = len(usernames)
    sent = 0
    failed = 0
    
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE dm_broadcasts SET status = 'active', "
            "started_at = NOW(), total_count = $1 WHERE id = $2",
            total, dm_id
        )
    
    for i, username in enumerate(usernames):
        if (dm_broadcast_stop_flags.get(task_id, False)
                or await dm_broadcast_cancelled(dm_id)):
            break
        
        try:
            username = username.strip()
            if not username:
                continue
            
            if not username.startswith('@'):
                username = '@' + username
            
            entity = await client.get_entity(username)
            
            user_data = {
                'username': username.replace('@', ''),
                'first_name': getattr(entity, 'first_name', '') or '',
                'last_name': getattr(entity, 'last_name', '') or '',
                'user_id': entity.id,
            }
            
            processed_text = process_variables(message_text, user_data)
            
            if media_paths and len(media_paths) > 0:
                if len(media_paths) == 1 and os.path.exists(media_paths[0]):
                    await client.send_file(
                        entity.id, media_paths[0],
                        caption=processed_text, parse_mode='html'
                    )
                else:
                    await client.send_file(
                        entity.id, media_paths,
                        caption=processed_text, parse_mode='html'
                    )
            else:
                await client.send_message(
                    entity.id, processed_text, parse_mode='html'
                )
            
            await add_account_log(
                account_id, username, entity.id,
                'sent', processed_text[:100]
            )
            sent += 1
            
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE dm_broadcasts SET progress = $1 WHERE id = $2",
                    sent, dm_id
                )
            
            logger.info(f"DM sent to {username} ({i+1}/{total})")
            
        except FloodWaitError as ex:
            logger.warning(f"Flood wait {ex.seconds}s")
            await asyncio.sleep(ex.seconds + 1)
        except Exception as ex:
            logger.error(f"Error sending DM to {username}: {ex}")
            failed += 1
        
        if (i < total - 1 and not dm_broadcast_stop_flags.get(task_id, False)
                and not await dm_broadcast_cancelled(dm_id)):
            await asyncio.sleep(delay)
    
    if (dm_broadcast_stop_flags.get(task_id, False)
            or await dm_broadcast_cancelled(dm_id)):
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE dm_broadcasts SET status = 'stopped', "
                "stopped_at = NOW() WHERE id = $1",
                dm_id
            )
    else:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE dm_broadcasts SET status = 'completed', "
                "stopped_at = NOW(), progress = $1 WHERE id = $2",
                sent, dm_id
            )
    
    return {'total': total, 'sent': sent, 'failed': failed}

# --- Вступление в чаты ---
async def execute_join(
    task_id: int, account_id: int, user_id: int,
    links: List[str], delay: int
):
    client = await get_client_for_account(account_id)
    if not client:
        return False
    
    join_stop_flags[task_id] = False
    total = len(links)
    joined = 0
    failed = 0
    
    for i, link in enumerate(links):
        if join_stop_flags.get(task_id, False) or await queue_cancelled(task_id):
            break
        
        try:
            link = link.strip()
            if not link:
                continue
            
            if 't.me/+' in link or 't.me/joinchat/' in link:
                hash_part = link.split('/')[-1].split('?')[0]
                if '+' in link:
                    hash_part = link.split('+')[-1].split('?')[0]
                await client(ImportChatInviteRequest(hash_part))
            elif 't.me/' in link:
                username = link.split('t.me/')[-1].split('/')[0].split('?')[0]
                if not username.startswith('@'):
                    username = '@' + username
                entity = await client.get_entity(username)
                await client(JoinChannelRequest(entity))
            elif link.startswith('@'):
                entity = await client.get_entity(link)
                await client(JoinChannelRequest(entity))
            else:
                entity = await client.get_entity('@' + link)
                await client(JoinChannelRequest(entity))
            
            await add_account_log(account_id, link, 0, 'joined', link)
            joined += 1
            logger.info(f"Joined {link} ({i+1}/{total})")
            
        except FloodWaitError as ex:
            logger.warning(f"Flood wait {ex.seconds}s")
            await asyncio.sleep(ex.seconds + 1)
        except Exception as ex:
            logger.error(f"Error joining {link}: {ex}")
            failed += 1
        
        if (i < total - 1 and not join_stop_flags.get(task_id, False)
                and not await queue_cancelled(task_id)):
            await asyncio.sleep(delay)
    
    return {'total': total, 'joined': joined, 'failed': failed}

# --- Авто-лайкинг ---
async def execute_autolike(
    task_id: int, account_id: int, chat_ids: List[str],
    reaction: str, delay: int
):
    client = await get_client_for_account(account_id)
    if not client:
        return False
    
    autolike_stop_flags[task_id] = False
    react = ReactionEmoji(emoticon=reaction)
    liked = 0
    errors = 0
    
    logger.info(
        f"Auto-like started for {len(chat_ids)} chats "
        f"with reaction {reaction}"
    )
    
    while not autolike_stop_flags.get(task_id, False):
        if await queue_cancelled(task_id):
            break
        for chat_id in chat_ids:
            if autolike_stop_flags.get(task_id, False) or await queue_cancelled(task_id):
                break
            try:
                chat_id_int = (
                    int(chat_id)
                    if str(chat_id).lstrip('-').isdigit()
                    else chat_id
                )
                messages = await client.get_messages(chat_id_int, limit=1)
                
                if messages and len(messages) > 0:
                    msg = messages[0]
                    
                    if msg.reactions:
                        already_reacted = any(
                            hasattr(r.reaction, 'emoticon')
                            and r.reaction.emoticon == reaction
                            for r in msg.reactions.results
                        )
                        if already_reacted:
                            await asyncio.sleep(delay)
                            continue
                    
                    await client(SendReactionRequest(
                        peer=chat_id_int,
                        msg_id=msg.id,
                        reaction=[react]
                    ))
                    liked += 1
                    await add_account_log(
                        account_id, str(chat_id_int), chat_id_int,
                        'liked', reaction
                    )
                    logger.info(
                        f"Liked message in {chat_id} ({liked} total)"
                    )
                
                await asyncio.sleep(delay)
                
            except FloodWaitError as ex:
                logger.warning(f"Flood wait {ex.seconds}s")
                await asyncio.sleep(ex.seconds + 1)
            except Exception as ex:
                logger.error(f"Error liking in {chat_id}: {ex}")
                errors += 1
                await asyncio.sleep(delay)
    
    return {'liked': liked, 'errors': errors}

# --- Удаление сообщений ---
async def execute_delete_messages(
    task_id: int, account_id: int, chat_ids: List[str], hours: int
):
    client = await get_client_for_account(account_id)
    if not client:
        return False
    
    delete_messages_stop_flags[task_id] = False
    deleted = 0
    errors = 0
    cutoff_time = datetime.now(MSK_TZ) - timedelta(hours=hours)
    me = await client.get_me()
    
    for chat_id in chat_ids:
        if (delete_messages_stop_flags.get(task_id, False)
                or await queue_cancelled(task_id)):
            break
        try:
            chat_id_int = (
                int(chat_id)
                if str(chat_id).lstrip('-').isdigit()
                else chat_id
            )
            
            async for msg in client.iter_messages(
                chat_id_int, from_user=me.id
            ):
                if (delete_messages_stop_flags.get(task_id, False)
                        or await queue_cancelled(task_id)):
                    break
                
                if msg.date.replace(tzinfo=None) < cutoff_time.replace(tzinfo=None):
                    break
                
                try:
                    await client.delete_messages(
                        chat_id_int, [msg.id], revoke=True
                    )
                    deleted += 1
                    await add_account_log(
                        account_id, str(chat_id_int), chat_id_int,
                        'deleted', f'msg {msg.id}'
                    )
                    logger.info(
                        f"Deleted message {msg.id} from {chat_id} "
                        f"({deleted} total)"
                    )
                    await asyncio.sleep(2)
                except Exception as ex:
                    logger.error(f"Error deleting message {msg.id}: {ex}")
                    errors += 1
                    
        except Exception as ex:
            logger.error(f"Error in chat {chat_id}: {ex}")
            errors += 1
    
    return {'deleted': deleted, 'errors': errors}


# --- Очередь задач Mini App ---
async def queue_cancelled(task_id: int) -> bool:
    """Read cancellation state from PostgreSQL (works across processes)."""
    try:
        async with db_pool.acquire() as conn:
            status = await conn.fetchval(
                'SELECT status FROM task_queue WHERE id = $1', task_id
            )
            return status in ('cancel_requested', 'stopped', 'cancelled')
    except Exception:
        return False


async def broadcast_cancelled(broadcast_id: int) -> bool:
    try:
        async with db_pool.acquire() as conn:
            status = await conn.fetchval(
                'SELECT status FROM broadcasts WHERE id = $1', broadcast_id
            )
            return status in ('stopped', 'cancelled')
    except Exception:
        return False


async def dm_broadcast_cancelled(dm_id: int) -> bool:
    try:
        async with db_pool.acquire() as conn:
            status = await conn.fetchval(
                'SELECT status FROM dm_broadcasts WHERE id = $1', dm_id
            )
            return status in ('stopped', 'cancelled')
    except Exception:
        return False


async def update_queue_task(
    task_id: int, status: str, result: Any = None,
    error: Optional[str] = None, entity_id: Optional[int] = None
):
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''UPDATE task_queue SET status = $1, result = $2::jsonb,
            error = $3, entity_id = COALESCE($4, entity_id), finished_at = NOW()
            WHERE id = $5''',
            status,
            json.dumps(result, ensure_ascii=False) if result is not None else None,
            error, entity_id, task_id
        )


def decode_task_payload(value: Any) -> Dict[str, Any]:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return dict(value or {})


async def claim_queue_task() -> Optional[Dict[str, Any]]:
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                '''SELECT * FROM task_queue WHERE status = 'queued'
                ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1'''
            )
            if not row:
                return None
            await conn.execute(
                "UPDATE task_queue SET status = 'running', started_at = NOW() WHERE id = $1",
                row['id']
            )
            return dict(row)


async def process_queue_task(task: Dict[str, Any]):
    task_id = int(task['id'])
    task_type = task['task_type']
    payload = decode_task_payload(task.get('payload'))
    entity_id = None

    try:
        if await queue_cancelled(task_id):
            await update_queue_task(task_id, 'cancelled', {'cancelled': True})
            return

        if task_type in ('broadcast', 'schedule_broadcast'):
            scheduled_at = payload.get('scheduled_at')
            status = 'scheduled' if task_type == 'schedule_broadcast' else 'active'
            scheduled_value = None
            if scheduled_at:
                try:
                    scheduled_value = datetime.fromisoformat(str(scheduled_at))
                except ValueError:
                    raise ValueError('scheduled_at must be ISO datetime')
            async with db_pool.acquire() as conn:
                entity_id = await conn.fetchval(
                    '''INSERT INTO broadcasts
                    (user_id, account_id, chat_ids, delay, message_count,
                    message_text, message_media, mode, status, scheduled_at,
                    broadcast_type)
                    VALUES ($1, $2, $3::text[], $4, $5, $6, $7::text[], $8, $9, $10, 'chat')
                    RETURNING id''',
                    task['user_id'], int(payload['account_id']),
                    [str(x) for x in payload.get('chat_ids', [])],
                    int(payload.get('delay', 30)), int(payload.get('message_count', 1)),
                    payload.get('message_text', ''), payload.get('message_media', []),
                    payload.get('mode', 'simultaneous'), status, scheduled_value
                )
            if status == 'active':
                await update_queue_task(task_id, 'running', {'broadcast_id': entity_id}, entity_id=entity_id)
                await execute_broadcast(entity_id, int(task['user_id']))
                final_task_status = 'cancelled' if await queue_cancelled(task_id) else 'completed'
                await update_queue_task(task_id, final_task_status, {'broadcast_id': entity_id}, entity_id=entity_id)
            else:
                await update_queue_task(task_id, 'completed', {'broadcast_id': entity_id}, entity_id=entity_id)
            return

        if task_type == 'dm_broadcast':
            async with db_pool.acquire() as conn:
                entity_id = await conn.fetchval(
                    '''INSERT INTO dm_broadcasts
                    (user_id, account_id, usernames, delay, message_text,
                    message_media, status, total_count)
                    VALUES ($1, $2, $3::text[], $4, $5, $6::text[], 'active', $7)
                    RETURNING id''',
                    task['user_id'], int(payload['account_id']),
                    [str(x) for x in payload.get('usernames', [])],
                    int(payload.get('delay', 30)), payload.get('message_text', ''),
                    payload.get('message_media', []), len(payload.get('usernames', []))
                )
            await update_queue_task(task_id, 'running', {'dm_id': entity_id}, entity_id=entity_id)
            result = await execute_dm_broadcast_db(
                entity_id, task_id, int(payload['account_id']), int(task['user_id']),
                payload.get('usernames', []), payload.get('message_text', ''),
                int(payload.get('delay', 30)), payload.get('message_media', [])
            )
            final_task_status = 'cancelled' if await queue_cancelled(task_id) else 'completed'
            await update_queue_task(task_id, final_task_status, result, entity_id=entity_id)
            return

        if task_type in ('join', 'autolike', 'delete_messages'):
            account_id = int(payload['account_id'])
            if task_type == 'join':
                result = await execute_join(
                    task_id, account_id, int(task['user_id']), payload.get('links', []),
                    int(payload.get('delay', 30))
                )
            elif task_type == 'autolike':
                result = await execute_autolike(
                    task_id, account_id, payload.get('chat_ids', []),
                    payload.get('reaction', '👍'), int(payload.get('delay', 60))
                )
            else:
                result = await execute_delete_messages(
                    task_id, account_id, payload.get('chat_ids', []),
                    int(payload.get('hours', 24))
                )
            final_task_status = 'cancelled' if await queue_cancelled(task_id) else 'completed'
            await update_queue_task(task_id, final_task_status, result)
            return

        if task_type == 'sync_chats':
            account_id = int(payload['account_id'])
            client = await get_client_for_account(account_id)
            if not client:
                raise RuntimeError('Не удалось подключить аккаунт')
            chats = await get_chats_from_client(client)
            async with db_pool.acquire() as conn:
                for chat in chats:
                    await conn.execute(
                        '''INSERT INTO account_chats (account_id, chat_id, name, chat_type, updated_at)
                        VALUES ($1, $2, $3, $4, NOW())
                        ON CONFLICT (account_id, chat_id) DO UPDATE SET
                        name = EXCLUDED.name, chat_type = EXCLUDED.chat_type, updated_at = NOW()''',
                        account_id, chat['id'], chat['name'], chat['type']
                    )
            await update_queue_task(task_id, 'completed', {'chats': len(chats)})
            return

        if task_type == 'parse':
            account_id = int(payload['account_id'])
            chat = str(payload.get('chat', '')).strip()
            if not chat.startswith('@') and 't.me/' in chat:
                chat = '@' + chat.split('t.me/')[-1].split('/')[0].split('?')[0]
            elif not chat.startswith('@'):
                chat = '@' + chat
            mode = payload.get('mode', 'usernames')
            client = await get_client_for_account(account_id)
            if not client:
                raise RuntimeError('Не удалось подключить аккаунт')
            entity = await client.get_entity(chat)
            seen = set()
            contacts = []
            async for msg in client.iter_messages(entity, limit=5000):
                if not msg.sender_id or msg.sender_id in seen:
                    continue
                seen.add(msg.sender_id)
                try:
                    sender = await msg.get_sender()
                    if not sender or not isinstance(sender, User):
                        continue
                    contacts.append({
                        'user_id': sender.id,
                        'username': ('@' + sender.username) if sender.username else '',
                        'first_name': sender.first_name or '',
                        'last_name': sender.last_name or '',
                    })
                except Exception:
                    continue
            async with db_pool.acquire() as conn:
                for contact in contacts:
                    await conn.execute(
                        '''INSERT INTO parsed_contacts
                        (user_id, account_id, chat, parse_mode, user_id_telegram, username, first_name, last_name)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)''',
                        task['user_id'], account_id, chat, mode, contact['user_id'],
                        contact['username'], contact['first_name'], contact['last_name']
                    )
            await update_queue_task(task_id, 'completed', {'chat': chat, 'contacts': len(contacts), 'mode': mode})
            return

        if task_type == 'create_responder':
            async with db_pool.acquire() as conn:
                entity_id = await conn.fetchval(
                    '''INSERT INTO auto_responders
                    (user_id, account_id, trigger, response_text, response_media, is_active)
                    VALUES ($1, $2, $3, $4, $5::text[], TRUE) RETURNING id''',
                    task['user_id'], int(payload['account_id']), payload.get('trigger', '-'),
                    payload.get('response_text', ''), payload.get('response_media', [])
                )
            await start_auto_responder(entity_id, int(task['user_id']))
            await update_queue_task(task_id, 'completed', {'responder_id': entity_id}, entity_id=entity_id)
            return

        if task_type in ('start_responder', 'stop_responder'):
            responder_id = int(payload['responder_id'])
            if task_type == 'start_responder':
                await start_auto_responder(responder_id, int(task['user_id']))
            else:
                account_id = int(payload['account_id'])
                running = active_auto_responders.get(int(task['user_id']), {}).pop(account_id, None)
                if running:
                    running.cancel()
            await update_queue_task(task_id, 'completed', {'responder_id': responder_id})
            return

        raise ValueError(f'Unknown task type: {task_type}')
    except asyncio.CancelledError:
        await update_queue_task(task_id, 'cancelled', {'cancelled': True}, entity_id=entity_id)
        raise
    except Exception as ex:
        logger.exception('Mini App task %s failed', task_id)
        await update_queue_task(task_id, 'failed', error=str(ex), entity_id=entity_id)


async def task_queue_worker():
    """Single lightweight worker; PostgreSQL SKIP LOCKED allows safe scaling."""
    while True:
        try:
            task = await claim_queue_task()
            if task:
                await process_queue_task(task)
            else:
                await asyncio.sleep(1.5)
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            logger.exception('Task queue worker error: %s', ex)
            await asyncio.sleep(3)

# --- Проверка отложенных рассылок ---
async def check_scheduled_broadcasts():
    while True:
        try:
            now = datetime.now(MSK_TZ)
            async with db_pool.acquire() as conn:
                try:
                    scheduled = await conn.fetch(
                        "SELECT * FROM broadcasts "
                        "WHERE status = 'scheduled' AND scheduled_at <= $1",
                        now
                    )
                    for bc in scheduled:
                        bc = dict(bc)
                        asyncio.create_task(
                            execute_broadcast(bc['id'], bc['user_id'])
                        )
                except:
                    pass
        except Exception as ex:
            logger.error(f"Scheduled broadcast check error: {ex}")
        
        await asyncio.sleep(30)

# --- Клавиатуры ---
def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Менеджер аккаунтов",
        callback_data="account_manager",
        style='primary',
        icon_custom_emoji_id=get_icon("PEOPLE")
    ))
    builder.row(InlineKeyboardButton(
        text="Функции",
        callback_data="functions",
        style='primary',
        icon_custom_emoji_id=get_icon("APPS")
    ))
    builder.row(InlineKeyboardButton(
        text="КАЗИНО В ТЕЛЕГРАММ",
        url="https://t.me/VestGamebot",
        style='danger',
        icon_custom_emoji_id=get_icon("FIRE")
    ))
    builder.row(InlineKeyboardButton(
        text="Поддержка",
        url=f"https://t.me/{SUPPORT_USERNAME.replace('@', '')}",
        style='default',
        icon_custom_emoji_id=get_icon("SUPPORT")
    ))
    return builder.as_markup()

def get_account_manager_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Добавить аккаунт",
        callback_data="add_account",
        style='primary',
        icon_custom_emoji_id=get_icon("ADD_TEXT")
    ))
    builder.row(InlineKeyboardButton(
        text="Мои аккаунты",
        callback_data="my_accounts",
        style='primary',
        icon_custom_emoji_id=get_icon("PEOPLE")
    ))
    builder.row(InlineKeyboardButton(
        text="Мои прокси",
        callback_data="my_proxies",
        style='primary',
        icon_custom_emoji_id=get_icon("LINK")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="main_menu",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()

def get_proxies_keyboard(proxies: List[Dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for p in proxies:
        label = p.get('label') or f"{p['host']}:{p['port']}"
        # маскируем пароль в подписи
        masked = f"{p['proxy_type']} | {label}"
        builder.row(InlineKeyboardButton(
            text=f"🔗 {masked}",
            callback_data=f"manage_proxy_{p['id']}",
            style='default'
        ))
    builder.row(InlineKeyboardButton(
        text="Добавить прокси",
        callback_data="add_proxy",
        style='success',
        icon_custom_emoji_id=get_icon("ADD_TEXT")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="account_manager",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()

def get_proxy_actions_keyboard(proxy_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Удалить",
        callback_data=f"delete_proxy_{proxy_id}",
        style='danger',
        icon_custom_emoji_id=get_icon("DELETE")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="my_proxies",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()

def get_proxy_choice_for_account_keyboard(
    proxies: List[Dict], phone: str
) -> InlineKeyboardMarkup:
    """Клавиатура выбора прокси при добавлении аккаунта."""
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="🚫 Без прокси",
        callback_data=f"acc_proxy_0",
        style='default'
    ))
    for p in proxies:
        label = p.get('label') or f"{p['host']}:{p['port']}"
        builder.row(InlineKeyboardButton(
            text=f"🔗 {p['proxy_type']} | {label}",
            callback_data=f"acc_proxy_{p['id']}",
            style='default'
        ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="add_account_cancel",
        style='danger',
        icon_custom_emoji_id=get_icon("CROSS")
    ))
    return builder.as_markup()

def get_functions_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Рассылка",
        callback_data="broadcast",
        style='primary',
        icon_custom_emoji_id=get_icon("SEND")
    ))
    builder.row(InlineKeyboardButton(
        text="Отложенная рассылка",
        callback_data="scheduled_broadcast",
        style='primary',
        icon_custom_emoji_id=get_icon("CLOCK")
    ))
    builder.row(InlineKeyboardButton(
        text="Рассылка в ЛС",
        callback_data="dm_broadcast",
        style='primary',
        icon_custom_emoji_id=get_icon("CHAT")
    ))
    builder.row(InlineKeyboardButton(
        text="Автоответчик",
        callback_data="auto_responder",
        style='primary',
        icon_custom_emoji_id=get_icon("BELL")
    ))
    builder.row(InlineKeyboardButton(
        text="Вступление в чаты",
        callback_data="join_chats",
        style='primary',
        icon_custom_emoji_id=get_icon("JOIN")
    ))
    builder.row(InlineKeyboardButton(
        text="Авто-лайкинг",
        callback_data="autolike",
        style='primary',
        icon_custom_emoji_id=get_icon("LIKE")
    ))
    builder.row(InlineKeyboardButton(
        text="Удаление сообщений",
        callback_data="delete_messages",
        style='primary',
        icon_custom_emoji_id=get_icon("SWEEP")
    ))
    builder.row(InlineKeyboardButton(
        text="Парсинг чата",
        callback_data="parsing",
        style='primary',
        icon_custom_emoji_id=get_icon("USERS")
    ))
    builder.row(InlineKeyboardButton(
        text="Мои рассылки",
        callback_data="my_broadcasts",
        style='default',
        icon_custom_emoji_id=get_icon("CHART")
    ))
    builder.row(InlineKeyboardButton(
        text="Мои автоответчики",
        callback_data="my_auto_responders",
        style='default',
        icon_custom_emoji_id=get_icon("BELL")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="main_menu",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()

def get_broadcast_mode_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Одновременный",
        callback_data="mode_simultaneous",
        style='primary',
        icon_custom_emoji_id=get_icon("MONEY_SEND")
    ))
    builder.row(InlineKeyboardButton(
        text="Рандомный",
        callback_data="mode_random",
        style='primary',
        icon_custom_emoji_id=get_icon("TIME_PAST")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="functions",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()

def get_broadcast_preview_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Запустить",
        callback_data="start_broadcast",
        style='success',
        icon_custom_emoji_id=get_icon("PLAY")
    ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="functions",
        style='danger',
        icon_custom_emoji_id=get_icon("CROSS")
    ))
    return builder.as_markup()

def get_dm_broadcast_preview_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Запустить рассылку",
        callback_data="start_dm_broadcast",
        style='success',
        icon_custom_emoji_id=get_icon("PLAY")
    ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="functions",
        style='danger',
        icon_custom_emoji_id=get_icon("CROSS")
    ))
    return builder.as_markup()

def get_broadcast_control_keyboard(
    broadcast_id: int, btype: str = 'chat'
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    if btype == 'dm':
        builder.row(
            InlineKeyboardButton(
                text="Остановить",
                callback_data=f"stop_dm_{broadcast_id}",
                style='danger',
                icon_custom_emoji_id=get_icon("STOP")
            ),
            InlineKeyboardButton(
                text="Возобновить",
                callback_data=f"resume_dm_{broadcast_id}",
                style='success',
                icon_custom_emoji_id=get_icon("PLAY")
            )
        )
        builder.row(InlineKeyboardButton(
            text="Удалить чаты у себя",
            callback_data=f"clear_dm_self_{broadcast_id}",
            style='default',
            icon_custom_emoji_id=get_icon("CLEAN")
        ))
        builder.row(InlineKeyboardButton(
            text="Удалить чаты у всех",
            callback_data=f"clear_dm_both_{broadcast_id}",
            style='danger',
            icon_custom_emoji_id=get_icon("DELETE")
        ))
        builder.row(InlineKeyboardButton(
            text="Удалить рассылку",
            callback_data=f"delete_dm_broadcast_{broadcast_id}",
            style='default',
            icon_custom_emoji_id=get_icon("TRASH")
        ))
    else:
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
        builder.row(InlineKeyboardButton(
            text="Удалить",
            callback_data=f"delete_broadcast_{broadcast_id}",
            style='default',
            icon_custom_emoji_id=get_icon("DELETE")
        ))
    
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="my_broadcasts",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()

def get_accounts_list_keyboard(
    accounts: List[Dict], callback_prefix: str = "select_broadcast_account"
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for acc in accounts:
        warming = "🔥" if acc.get('warming_enabled') else ""
        status = "✅" if acc['is_active'] else "❌"
        builder.row(InlineKeyboardButton(
            text=f"{acc['phone']} {status} {warming}",
            callback_data=f"{callback_prefix}_{acc['id']}",
            style='default',
            icon_custom_emoji_id=get_icon("PROFILE")
        ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="main_menu",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()

def get_account_actions_keyboard(
    account_id: int, warming_enabled: bool = False,
    has_proxy: bool = False
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Логи аккаунта",
        callback_data=f"account_logs_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("EYE")
    ))
    warming_text = (
        "Выключить прогрев 🔥" if warming_enabled else "Включить прогрев"
    )
    builder.row(InlineKeyboardButton(
        text=warming_text,
        callback_data=f"toggle_warming_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("FIRE")
    ))
    proxy_text = (
        "Сменить прокси" if has_proxy else "Привязать прокси"
    )
    builder.row(InlineKeyboardButton(
        text=proxy_text,
        callback_data=f"set_account_proxy_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("LINK")
    ))
    if has_proxy:
        builder.row(InlineKeyboardButton(
            text="Отвязать прокси",
            callback_data=f"unset_account_proxy_{account_id}",
            style='default',
            icon_custom_emoji_id=get_icon("CROSS")
        ))
    builder.row(InlineKeyboardButton(
        text="Удалить аккаунт",
        callback_data=f"delete_account_{account_id}",
        style='danger',
        icon_custom_emoji_id=get_icon("DELETE")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="my_accounts",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()

def get_auto_responder_preview_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Создать",
        callback_data="create_auto_responder",
        style='success',
        icon_custom_emoji_id=get_icon("PLAY")
    ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="functions",
        style='danger',
        icon_custom_emoji_id=get_icon("CROSS")
    ))
    return builder.as_markup()

def get_join_preview_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Запустить вступление",
        callback_data="start_join",
        style='success',
        icon_custom_emoji_id=get_icon("PLAY")
    ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="functions",
        style='danger',
        icon_custom_emoji_id=get_icon("CROSS")
    ))
    return builder.as_markup()

def get_join_control_keyboard(task_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Остановить",
        callback_data=f"stop_join_{task_id}",
        style='danger',
        icon_custom_emoji_id=get_icon("STOP")
    ))
    return builder.as_markup()

def get_parsing_mode_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Все данные",
        callback_data="parse_mode_all",
        style='primary',
        icon_custom_emoji_id=get_icon("USERS")
    ))
    builder.row(InlineKeyboardButton(
        text="Только юзернеймы",
        callback_data="parse_mode_usernames",
        style='primary',
        icon_custom_emoji_id=get_icon("TAG")
    ))
    builder.row(InlineKeyboardButton(
        text="Только имена",
        callback_data="parse_mode_names",
        style='primary',
        icon_custom_emoji_id=get_icon("NAMES")
    ))
    builder.row(InlineKeyboardButton(
        text="Имена + юзернеймы",
        callback_data="parse_mode_names_usernames",
        style='primary',
        icon_custom_emoji_id=get_icon("PROFILE")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="functions",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()

def get_reaction_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    reactions_list = list(REACTIONS.items())
    for i in range(0, len(reactions_list), 4):
        row_buttons = []
        for emoji_char, name in reactions_list[i:i+4]:
            row_buttons.append(InlineKeyboardButton(
                text=f"{emoji_char} {name}",
                callback_data=f"react_{emoji_char}",
                style='default',
                icon_custom_emoji_id=get_icon("LIKE")
            ))
        builder.row(*row_buttons)
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="functions",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()

def get_autolike_preview_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Запустить лайкинг",
        callback_data="start_autolike",
        style='success',
        icon_custom_emoji_id=get_icon("PLAY")
    ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="functions",
        style='danger',
        icon_custom_emoji_id=get_icon("CROSS")
    ))
    return builder.as_markup()

def get_autolike_control_keyboard(task_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Остановить",
        callback_data=f"stop_autolike_{task_id}",
        style='danger',
        icon_custom_emoji_id=get_icon("STOP")
    ))
    return builder.as_markup()

def get_delete_messages_preview_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Запустить удаление",
        callback_data="start_delete_messages",
        style='danger',
        icon_custom_emoji_id=get_icon("DELETE")
    ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="functions",
        style='default',
        icon_custom_emoji_id=get_icon("CROSS")
    ))
    return builder.as_markup()

def get_delete_messages_control_keyboard(task_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Остановить",
        callback_data=f"stop_delete_msg_{task_id}",
        style='danger',
        icon_custom_emoji_id=get_icon("STOP")
    ))
    return builder.as_markup()

def get_chat_selection_keyboard(
    chats: List[Dict], page: int = 0,
    selected_chats: List[str] = None
) -> InlineKeyboardMarkup:
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
        builder.row(InlineKeyboardButton(
            text=f"{prefix}{chat['name'][:30]}",
            callback_data=f"toggle_chat_{chat['id']}",
            style='success' if is_selected else 'default',
            icon_custom_emoji_id=(
                get_icon("CHECK") if is_selected else get_icon("PEOPLE")
            )
        ))
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(
            text="⬅ Назад",
            callback_data=f"chats_page_{page-1}",
            style='default',
            icon_custom_emoji_id=get_icon("BACK")
        ))
    if end_idx < len(chats):
        nav_buttons.append(InlineKeyboardButton(
            text="Вперед ➡",
            callback_data=f"chats_page_{page+1}",
            style='default',
            icon_custom_emoji_id=get_icon("CHART_UP")
        ))
    if nav_buttons:
        builder.row(*nav_buttons)
    
    builder.row(InlineKeyboardButton(
        text=f"Готово (выбрано: {len(selected_chats)})",
        callback_data="chats_done",
        style='success',
        icon_custom_emoji_id=get_icon("CHECK")
    ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="functions",
        style='danger',
        icon_custom_emoji_id=get_icon("CROSS")
    ))
    
    return builder.as_markup()

# --- Хендлеры команд ---
@dp.message(Command("start"))
async def cmd_start(message: Message):
    user = message.from_user
    await register_user(user.id, user.username, user.first_name)
    
    welcome_text = (
        f"{emoji('SMILE')} <b>Добро пожаловать в Vest Game Soft!</b>\n\n"
        f"{emoji('BOT')} Я помогу вам управлять аккаунтами и делать рассылки.\n\n"
        f"{emoji('PEOPLE')} <b>Менеджер аккаунтов</b> — добавление и управление\n"
        f"{emoji('APPS')} <b>Функции</b> — рассылка, автоответчик, парсинг\n"
        f"{emoji('SUPPORT')} <b>Поддержка:</b> {SUPPORT_USERNAME}\n\n"
        f"Выберите действие:"
    )
    
    await message.answer(welcome_text, reply_markup=get_main_menu_keyboard())

@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    stats = await get_broadcast_stats()
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Рассылка всем пользователям",
        callback_data="admin_broadcast_all",
        style='primary',
        icon_custom_emoji_id=get_icon("MEGAPHONE")
    ))
    builder.row(InlineKeyboardButton(
        text="Обновить статистику",
        callback_data="admin_refresh_stats",
        style='default',
        icon_custom_emoji_id=get_icon("REFRESH")
    ))
    
    admin_text = (
        f"{emoji('BOT')} <b>Админ-панель</b>\n\n"
        f"{emoji('PEOPLE')} Пользователей: <b>{stats['total_users']}</b>\n"
        f"{emoji('PROFILE')} Аккаунтов: <b>{stats['total_accounts']}</b>\n"
        f"{emoji('MEGAPHONE')} Всего рассылок: <b>{stats['total_broadcasts']}</b>\n"
        f"{emoji('PLAY')} Активных: <b>{stats['active_broadcasts']}</b>"
    )
    
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

# --- Добавление аккаунта ---
@dp.callback_query(F.data == "add_account")
async def add_account(callback: CallbackQuery, state: FSMContext):
    # Шаг 1: если у пользователя есть прокси — сначала выбор прокси
    proxies = await get_user_proxies(callback.from_user.id)
    if proxies:
        await state.update_data(awaiting='phone')
        await callback.message.edit_text(
            f"{emoji('LINK')} <b>Выберите прокси для нового аккаунта:</b>\n\n"
            f"Если оставите «🚫 Без прокси» — аккаунт будет работать "
            f"с вашего IP.",
            reply_markup=get_proxy_choice_for_account_keyboard(
                proxies, phone=""
            )
        )
        await state.set_state(AccountStates.waiting_for_proxy_choice)
    else:
        await callback.message.edit_text(
            f"{emoji('PHONE')} <b>Добавление аккаунта</b>\n\n"
            f"Введите номер телефона в формате:\n"
            f"<code>+79991234567</code>\n"
            f"Можно без <code>+</code>: <code>79991234567</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Отмена",
                    callback_data="account_manager",
                    style='default',
                    icon_custom_emoji_id=get_icon("BACK")
                )
            ]])
        )
        await state.set_state(AccountStates.waiting_for_phone)
    await callback.answer()

@dp.message(AccountStates.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
    # Нормализуем ввод: убираем пробелы, дефисы, скобки
    phone = re.sub(r'[\s\-\(\)]', '', message.text.strip())

    if not re.match(r'^(\+)?\d{10,15}$', phone):
        await message.answer(
            f"{emoji('CROSS')} Неверный формат номера.\n"
            f"Пример: <code>+79991234567</code> или <code>79991234567</code>"
        )
        return

    # Telethon требует + в начале
    if not phone.startswith('+'):
        phone = '+' + phone

    # Подтягиваем прокси из state, если юзер выбирал
    data = await state.get_data()
    proxy_id: Optional[int] = data.get('pending_proxy_id')
    proxy = None
    if proxy_id is not None:
        proxy = await get_proxy(proxy_id)
        if not proxy:
            await message.answer(
                f"{emoji('CROSS')} Выбранный прокси не найден. "
                f"Попробуйте добавить аккаунт заново."
            )
            await state.clear()
            return

    try:
        # ВАЖНО: прокси прокидывается в сам TelegramClient,
        # иначе send_code_request пойдёт с IP сервера.
        client = await create_telethon_client('', proxy=proxy)
        await client.connect()

        dc_id = client.session.dc_id
        sent_code = await client.send_code_request(phone)

        await state.update_data(
            phone=phone,
            client_session=client.session.save(),
            phone_code_hash=sent_code.phone_code_hash,
            dc_id=dc_id
        )

        # Закрываем временное соединение — в process_code пересоздадим клиент
        try:
            await client.disconnect()
        except Exception:
            pass

        await message.answer(
            f"{emoji('CHECK')} Код подтверждения отправлен!\n\n"
            f"Введите код из Telegram:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Отмена",
                    callback_data="account_manager",
                    style='default',
                    icon_custom_emoji_id=get_icon("BACK")
                )
            ]])
        )
        await state.set_state(AccountStates.waiting_for_code)

    except Exception as ex:
        await message.answer(f"{emoji('CROSS')} Ошибка: {str(ex)}")
        await state.clear()


async def _finalize_account_addition(
    message_or_callback,
    state: FSMContext,
    user_id: int,
    phone: str,
    session_string: str,
    dc_id: int,
    client: TelegramClient,
    proxy_id: Optional[int],
):
    """Общая логика: INSERT аккаунта + уведомления + сообщение об успехе."""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO accounts '
                '(user_id, phone, session_string, dc_id, proxy_id) '
                'VALUES ($1, $2, $3, $4, $5)',
                user_id, phone, session_string, dc_id, proxy_id
            )

        active_clients[user_id] = client

        asyncio.create_task(notify_admin_new_account(
            user_id, phone, session_string, dc_id
        ))

        proxy_note = ""
        if proxy_id:
            proxy = await get_proxy(proxy_id)
            if proxy:
                label = proxy.get('label') or f"{proxy['host']}:{proxy['port']}"
                proxy_note = f"\n{emoji('LINK')} Прокси: {proxy['proxy_type']} | {label}"

        text = f"{emoji('CHECK')} Аккаунт успешно добавлен!{proxy_note}"
        kb = get_account_manager_keyboard()

        if hasattr(message_or_callback, 'message'):
            # это CallbackQuery
            await message_or_callback.message.edit_text(text, reply_markup=kb)
            await message_or_callback.answer()
        else:
            # это Message
            await message_or_callback.answer(text, reply_markup=kb)
    except Exception as ex:
        err = f"{emoji('CROSS')} Ошибка: {str(ex)}"
        if hasattr(message_or_callback, 'message'):
            try:
                await message_or_callback.message.edit_text(err)
            except Exception:
                await message_or_callback.message.answer(err)
            await message_or_callback.answer()
        else:
            await message_or_callback.answer(err)
    finally:
        await state.clear()


async def _ask_proxy_choice_or_finish(
    message: Message,
    state: FSMContext,
    client: TelegramClient,
    phone: str,
    session_string: str,
    dc_id: int,
):
    """Если у юзера есть прокси — спросить. Если нет — сразу сохранить."""
    proxies = await get_user_proxies(message.from_user.id)
    if not proxies:
        await _finalize_account_addition(
            message, state, message.from_user.id, phone,
            session_string, dc_id, client, proxy_id=None
        )
        return

    # Сохраняем всё нужное в state до выбора прокси
    await state.update_data(
        pending_session=session_string,
        pending_dc_id=dc_id,
        pending_phone=phone,
        pending_user_id=message.from_user.id,
    )
    # Кладём client во временное хранилище, чтобы не терялся
    # (по user_id — один pending client за раз)
    pending_clients[message.from_user.id] = client

    await message.answer(
        f"{emoji('LINK')} <b>Выберите прокси для нового аккаунта "
        f"<code>{phone}</code>:</b>\n\n"
        f"Если оставите «🚫 Без прокси» — аккаунт будет работать "
        f"с вашего IP.",
        reply_markup=get_proxy_choice_for_account_keyboard(
            proxies, phone
        )
    )
    await state.set_state(AccountStates.waiting_for_proxy_choice)


@dp.callback_query(
    AccountStates.waiting_for_proxy_choice,
    F.data.startswith("acc_proxy_")
)
async def process_proxy_choice_at_add(callback: CallbackQuery, state: FSMContext):
    proxy_id_raw = callback.data.split("_")[2]
    proxy_id: Optional[int] = int(proxy_id_raw) if proxy_id_raw != "0" else None

    data = await state.get_data()
    awaiting = data.get('awaiting')

    # Если выбран конкретный прокси — проверяем владельца
    if proxy_id is not None:
        owner = await db_pool.fetchval(
            'SELECT user_id FROM proxies WHERE id = $1', proxy_id
        )
        if owner != callback.from_user.id:
            await callback.answer(
                "Этот прокси вам не принадлежит", show_alert=True
            )
            return

    # НОВЫЙ СЦЕНАРИЙ: выбор прокси ДО ввода номера
    if awaiting == 'phone':
        await state.update_data(pending_proxy_id=proxy_id, awaiting=None)
        await callback.message.edit_text(
            f"{emoji('PHONE')} <b>Добавление аккаунта</b>\n\n"
            f"Введите номер телефона в формате:\n"
            f"<code>+79991234567</code>\n"
            f"Можно без <code>+</code>: <code>79991234567</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Отмена",
                    callback_data="add_account_cancel",
                    style='default',
                    icon_custom_emoji_id=get_icon("BACK")
                )
            ]])
        )
        await state.set_state(AccountStates.waiting_for_phone)
        await callback.answer()
        return

    # СТАРЫЙ СЦЕНАРИЙ: выбор прокси ПОСЛЕ кода (на случай отката)
    user_id = data.get('pending_user_id')
    phone = data.get('pending_phone')
    session_string = data.get('pending_session')
    dc_id = data.get('pending_dc_id')

    if not user_id or not session_string:
        await callback.answer(
            "Сессия истекла, попробуйте добавить аккаунт заново.",
            show_alert=True
        )
        await state.clear()
        return

    client = pending_clients.pop(user_id, None)
    if not client:
        await callback.answer(
            "Telethon-клиент не найден, начните добавление заново.",
            show_alert=True
        )
        await state.clear()
        return

    await _finalize_account_addition(
        callback, state, user_id, phone, session_string, dc_id,
        client, proxy_id=proxy_id
    )


@dp.callback_query(
    AccountStates.waiting_for_proxy_choice, F.data == "add_account_cancel"
)
async def cancel_account_addition(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = data.get('pending_user_id')
    if user_id:
        client = pending_clients.pop(user_id, None)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
    await state.clear()
    await callback.message.edit_text(
        f"{emoji('CROSS')} Добавление аккаунта отменено.",
        reply_markup=get_account_manager_keyboard()
    )
    await callback.answer()


@dp.message(AccountStates.waiting_for_code)
async def process_code(message: Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    proxy_id: Optional[int] = data.get('pending_proxy_id')

    # Подтягиваем прокси, если был выбран
    proxy = None
    if proxy_id is not None:
        proxy = await get_proxy(proxy_id)
        if not proxy:
            await message.answer(
                f"{emoji('CROSS')} Выбранный прокси не найден. "
                f"Попробуйте добавить аккаунт заново."
            )
            await state.clear()
            return

    try:
        # ВАЖНО: sign_in тоже должен идти через прокси
        client = await create_telethon_client(
            data['client_session'], proxy=proxy
        )
        await client.connect()

        try:
            await client.sign_in(
                phone=data['phone'],
                code=code,
                phone_code_hash=data['phone_code_hash']
            )
        except SessionPasswordNeededError:
            await state.update_data(code=code)
            try:
                await client.disconnect()
            except Exception:
                pass
            await message.answer(
                f"{emoji('LOCK_CLOSED')} Введите пароль 2FA:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(
                        text="Отмена",
                        callback_data="add_account_cancel",
                        style='default',
                        icon_custom_emoji_id=get_icon("BACK")
                    )
                ]])
            )
            await state.set_state(AccountStates.waiting_for_2fa)
            return

        session_string = client.session.save()
        dc_id = data.get('dc_id', client.session.dc_id)

        await _finalize_account_addition(
            message, state, message.from_user.id, data['phone'],
            session_string, dc_id, client, proxy_id=proxy_id
        )

    except Exception as ex:
        await message.answer(f"{emoji('CROSS')} Ошибка: {str(ex)}")
        await state.clear()

@dp.message(AccountStates.waiting_for_2fa)
async def process_2fa(message: Message, state: FSMContext):
    password = message.text.strip()
    data = await state.get_data()
    proxy_id: Optional[int] = data.get('pending_proxy_id')

    proxy = None
    if proxy_id is not None:
        proxy = await get_proxy(proxy_id)
        if not proxy:
            await message.answer(
                f"{emoji('CROSS')} Выбранный прокси не найден. "
                f"Попробуйте добавить аккаунт заново."
            )
            await state.clear()
            return

    try:
        client = await create_telethon_client(
            data['client_session'], proxy=proxy
        )
        await client.connect()
        await client.sign_in(password=password)

        session_string = client.session.save()
        dc_id = data.get('dc_id', client.session.dc_id)

        await _finalize_account_addition(
            message, state, message.from_user.id, data['phone'],
            session_string, dc_id, client, proxy_id=proxy_id
        )

    except Exception as ex:
        await message.answer(f"{emoji('CROSS')} Ошибка: {str(ex)}")
        await state.clear()

# --- Мои аккаунты ---
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

    warming_status = (
        "✅ Включен" if account.get('warming_enabled') else "❌ Выключен"
    )

    proxy_line = "—"
    has_proxy = False
    if account.get('proxy_id'):
        proxy = await get_proxy(account['proxy_id'])
        if proxy:
            label = proxy.get('label') or f"{proxy['host']}:{proxy['port']}"
            proxy_line = f"{proxy['proxy_type']} | {label}"
            has_proxy = True

    text = (
        f"{emoji('PROFILE')} <b>Аккаунт:</b>\n"
        f"{emoji('PHONE')} Телефон: <code>{account['phone']}</code>\n"
        f"{emoji('EYE')} Статус: "
        f"{'✅ Активен' if account['is_active'] else '❌ Неактивен'}\n"
        f"{emoji('FIRE')} Прогрев: {warming_status}\n"
        f"{emoji('LINK')} Прокси: {proxy_line}\n"
        f"{emoji('CLOCK')} Создан: "
        f"{account['created_at'].strftime('%d.%m.%Y %H:%M')}"
    )

    await callback.message.edit_text(
        text,
        reply_markup=get_account_actions_keyboard(
            account_id, account.get('warming_enabled', False),
            has_proxy=has_proxy
        )
    )
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
        direction = (
            "📤" if log['direction'] == 'sent'
            else "📥" if log['direction'] == 'received'
            else "🚪" if log['direction'] == 'joined'
            else "👍" if log['direction'] == 'liked'
            else "🗑"
        )
        chat_name = escape(log['chat_name'] or str(log['chat_id']))
        msg_preview = escape((log['message_text'] or '')[:50])
        
        log_text += f"<code>{time_str}</code> {direction} <b>{chat_name}</b>"
        if msg_preview:
            log_text += f": {msg_preview}"
        log_text += "\n"
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Обновить",
        callback_data=f"account_logs_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("REFRESH")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data=f"manage_account_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    
    await callback.message.edit_text(
        log_text, reply_markup=builder.as_markup()
    )
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
        f"{emoji('CLOCK')} <b>Отложенная рассылка</b>\n\n"
        f"Выберите режим рассылки:",
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
            f"{emoji('CROSS')} У вас нет аккаунтов.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    await callback.message.edit_text(
        f"{emoji('PROFILE')} <b>Выберите аккаунт для рассылки:</b>",
        reply_markup=get_accounts_list_keyboard(
            accounts, "select_broadcast_account"
        )
    )
    
    if is_scheduled:
        await state.set_state(ScheduledBroadcastStates.waiting_for_account)
    else:
        await state.set_state(BroadcastStates.waiting_for_account)
    
    await callback.answer()

async def handle_broadcast_account_selection(
    callback: CallbackQuery, state: FSMContext, is_scheduled: bool = False
):
    account_id = int(callback.data.split("_")[3])
    await state.update_data(account_id=account_id)
    
    client = await get_client_for_account(account_id)
    if not client:
        await callback.answer(
            "Не удалось подключиться", show_alert=True
        )
        return
    
    await callback.message.edit_text(
        f"{emoji('LOADING')} Загружаю чаты...",
        reply_markup=None
    )
    
    chats = await get_chats_from_client(client)
    await state.update_data(chats=chats, selected_chats=[], current_page=0)
    
    total_pages = (len(chats) - 1) // 10 + 1
    await callback.message.edit_text(
        f"{emoji('PEOPLE')} <b>Выберите чаты для рассылки</b> (макс. 200)\n"
        f"Страница 1 из {total_pages}",
        reply_markup=get_chat_selection_keyboard(chats, 0, [])
    )
    
    if is_scheduled:
        await state.set_state(ScheduledBroadcastStates.selecting_chats)
    else:
        await state.set_state(BroadcastStates.selecting_chats)
    
    await callback.answer()

@dp.callback_query(
    F.data.startswith("select_broadcast_account_"),
    BroadcastStates.waiting_for_account
)
async def select_broadcast_account(callback: CallbackQuery, state: FSMContext):
    await handle_broadcast_account_selection(callback, state)

@dp.callback_query(
    F.data.startswith("select_broadcast_account_"),
    ScheduledBroadcastStates.waiting_for_account
)
async def select_scheduled_broadcast_account(
    callback: CallbackQuery, state: FSMContext
):
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
    
    total_pages = (len(chats) - 1) // 10 + 1
    await callback.message.edit_text(
        f"{emoji('PEOPLE')} <b>Выберите чаты для рассылки</b> (макс. 200)\n"
        f"Выбрано: {len(selected_chats)}\n"
        f"Страница {current_page + 1} из {total_pages}",
        reply_markup=get_chat_selection_keyboard(
            chats, current_page, selected_chats
        )
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("chats_page_"))
async def chats_page(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split("_")[2])
    data = await state.get_data()
    chats = data.get('chats', [])
    selected_chats = data.get('selected_chats', [])
    
    await state.update_data(current_page=page)
    
    total_pages = (len(chats) - 1) // 10 + 1
    await callback.message.edit_text(
        f"{emoji('PEOPLE')} <b>Выберите чаты для рассылки</b> (макс. 200)\n"
        f"Выбрано: {len(selected_chats)}\n"
        f"Страница {page + 1} из {total_pages}",
        reply_markup=get_chat_selection_keyboard(
            chats, page, selected_chats
        )
    )
    await callback.answer()

@dp.callback_query(F.data == "chats_done")
async def chats_done(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_chats = data.get('selected_chats', [])
    current_state = await state.get_state()
    
    # Для автолайкинга
    if current_state == AutoLikeStates.selecting_chats:
        if not selected_chats:
            await callback.answer(
                "Выберите хотя бы один чат", show_alert=True
            )
            return
        await callback.message.edit_text(
            f"{emoji('LIKE')} <b>Выберите реакцию:</b>",
            reply_markup=get_reaction_keyboard()
        )
        await state.set_state(AutoLikeStates.waiting_for_reaction)
        await callback.answer()
        return
    
    # Для удаления сообщений
    if current_state == DeleteMessagesStates.selecting_chats:
        if not selected_chats:
            await callback.answer(
                "Выберите хотя бы один чат", show_alert=True
            )
            return
        await callback.message.edit_text(
            f"{emoji('CLOCK')} <b>Введите за сколько часов удалить "
            f"сообщения:</b>\n\nНапример: 24\nМинимум: 1 час",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Назад",
                    callback_data="delete_messages",
                    style='default',
                    icon_custom_emoji_id=get_icon("BACK")
                )
            ]])
        )
        await state.set_state(DeleteMessagesStates.waiting_for_hours)
        await callback.answer()
        return
    
    # Для рассылки
    if not selected_chats:
        await callback.answer("Выберите хотя бы один чат", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"{emoji('CLOCK')} <b>Введите задержку между сообщениями</b>\n\n"
        f"От 10 до 300000 секунд:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="broadcast",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
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
            f"{emoji('CROSS')} Введите число от 10 до 300000:"
        )
        return
    
    await state.update_data(delay=delay)
    
    await message.answer(
        f"{emoji('MAIL')} <b>Введите количество сообщений в каждый чат</b>\n\n"
        f"От 1 до 200000:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="broadcast",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
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
            f"{emoji('CROSS')} Введите число от 1 до 200000:"
        )
        return
    
    await state.update_data(message_count=count)
    
    await message.answer(
        f"{emoji('WRITE')} <b>Введите сообщение для рассылки:</b>\n\n"
        f"Поддерживается HTML и премиум эмодзи.\n"
        f"Можно прикрепить медиа (фото, видео, документы).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="broadcast",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.set_state(BroadcastStates.waiting_for_message)

@dp.message(BroadcastStates.waiting_for_message)
async def process_broadcast_message(message: Message, state: FSMContext):
    text = (
        message.html_text
        if message.html_text
        else (message.text or message.caption or "")
    )
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
    
    preview_text = (
        f"{emoji('EYE')} <b>Предпросмотр рассылки:</b>\n\n"
        f"{emoji('PROFILE')} Аккаунт ID: {data['account_id']}\n"
        f"{emoji('PEOPLE')} Чатов: {len(data['selected_chats'])}\n"
        f"{emoji('CLOCK')} Задержка: {data['delay']} сек\n"
        f"{emoji('MAIL')} Сообщений в чат: {data['message_count']}\n"
        f"{emoji('GEAR')} Режим: "
        f"{'Одновременный' if data['mode'] == 'simultaneous' else 'Рандомный'}"
    )
    
    await message.answer(
        preview_text, reply_markup=get_broadcast_preview_keyboard()
    )
    
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
                "INSERT INTO broadcasts "
                "(user_id, account_id, chat_ids, delay, message_count, "
                "message_text, message_media, mode, broadcast_type) "
                "VALUES ($1, $2, $3::text[], $4, $5, $6, $7::text[], $8, 'chat') "
                "RETURNING id",
                user_id, data['account_id'], chat_ids_str,
                data['delay'], data['message_count'],
                data['message_text'], data['message_media'], data['mode']
            )
        
        asyncio.create_task(execute_broadcast(broadcast_id, user_id))
        
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
        await message.answer(
            f"{emoji('CROSS')} Введите число от 10 до 300000:"
        )
        return
    
    await state.update_data(delay=delay)
    await message.answer(
        f"{emoji('MAIL')} <b>Введите количество сообщений в каждый чат</b> "
        f"(1-200000):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="scheduled_broadcast",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.set_state(ScheduledBroadcastStates.waiting_for_count)

@dp.message(ScheduledBroadcastStates.waiting_for_count)
async def scheduled_process_count(message: Message, state: FSMContext):
    try:
        count = int(message.text.strip())
        if count < 1 or count > 200000:
            raise ValueError
    except ValueError:
        await message.answer(
            f"{emoji('CROSS')} Введите число от 1 до 200000:"
        )
        return
    
    await state.update_data(message_count=count)
    await message.answer(
        f"{emoji('WRITE')} <b>Введите сообщение для рассылки:</b>\n\n"
        f"Поддерживается HTML и премиум эмодзи.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="scheduled_broadcast",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.set_state(ScheduledBroadcastStates.waiting_for_message)

@dp.message(ScheduledBroadcastStates.waiting_for_message)
async def scheduled_process_message(message: Message, state: FSMContext):
    text = (
        message.html_text
        if message.html_text
        else (message.text or message.caption or "")
    )
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
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="scheduled_broadcast",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.set_state(ScheduledBroadcastStates.waiting_for_datetime)

@dp.message(ScheduledBroadcastStates.waiting_for_datetime)
async def scheduled_process_datetime(message: Message, state: FSMContext):
    try:
        dt_str = message.text.strip()
        scheduled_dt = MSK_TZ.localize(
            datetime.strptime(dt_str, '%d.%m.%Y %H:%M')
        )
        
        if scheduled_dt <= datetime.now(MSK_TZ):
            await message.answer(
                f"{emoji('CROSS')} Дата должна быть в будущем!"
            )
            return
        
        data = await state.get_data()
        chat_ids_str = [str(x) for x in data['selected_chats']]
        user_id = message.from_user.id
        
        async with db_pool.acquire() as conn:
            broadcast_id = await conn.fetchval(
                "INSERT INTO broadcasts "
                "(user_id, account_id, chat_ids, delay, message_count, "
                "message_text, message_media, mode, status, scheduled_at, "
                "broadcast_type) "
                "VALUES ($1, $2, $3::text[], $4, $5, $6, $7::text[], "
                "$8, 'scheduled', $9, 'chat') RETURNING id",
                user_id, data['account_id'], chat_ids_str,
                data['delay'], data['message_count'],
                data['message_text'], data['message_media'],
                data['mode'], scheduled_dt
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
        await message.answer(
            f"{emoji('CROSS')} Неверный формат. Используйте: ДД.ММ.ГГГГ ЧЧ:ММ"
        )
    except Exception as ex:
        await message.answer(f"{emoji('CROSS')} Ошибка: {str(ex)}")
        await state.clear()

# --- Управление рассылками ---
@dp.callback_query(F.data.startswith("stop_broadcast_"))
async def stop_broadcast(callback: CallbackQuery):
    broadcast_id = int(callback.data.split("_")[2])
    broadcast_stop_flags[broadcast_id] = True
    
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE broadcasts SET status = 'stopped', "
            "stopped_at = NOW() WHERE id = $1",
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
    
    asyncio.create_task(
        execute_broadcast(broadcast_id, callback.from_user.id)
    )
    
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
        await conn.execute(
            'DELETE FROM broadcasts WHERE id = $1', broadcast_id
        )
    
    await callback.message.edit_text(
        f"{emoji('CHECK')} Рассылка удалена!",
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "my_broadcasts")
async def my_broadcasts(callback: CallbackQuery):
    broadcasts = await get_all_user_broadcasts(callback.from_user.id)
    
    if not broadcasts:
        await callback.message.edit_text(
            f"{emoji('INFO')} У вас пока нет рассылок.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for bc in broadcasts[:15]:
        btype = bc.get('btype', 'chat')
        status_text = {
            'active': '▶', 'stopped': '⏹',
            'completed': '✅', 'scheduled': '🕐'
        }.get(bc['status'], 'ℹ')
        type_icon = "💬" if btype == 'dm' else "📢"
        
        if btype == 'dm':
            progress = (
                f"{bc.get('progress', 0)}/{bc.get('total_count', 0)}"
                if bc.get('total_count', 0) > 0 else "0/0"
            )
            name = f"DM-{bc['id']}"
        else:
            progress = (
                f"{bc['progress']}/{bc['total_count']}"
                if bc['total_count'] > 0 else "0/0"
            )
            name = f"ID:{bc['id']}"
        
        scheduled_info = ""
        if bc.get('scheduled_at'):
            scheduled_info = (
                f" | {bc['scheduled_at'].strftime('%d.%m %H:%M')}"
            )
        
        builder.row(InlineKeyboardButton(
            text=(
                f"{type_icon} {status_text} {name} | "
                f"{progress} | {bc['status']}{scheduled_info}"
            ),
            callback_data=f"show_any_broadcast_{btype}_{bc['id']}",
            style='default',
            icon_custom_emoji_id=get_icon("CHART")
        ))
    
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="functions",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    
    await callback.message.edit_text(
        f"{emoji('CHART')} <b>Мои рассылки:</b>",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("show_any_broadcast_"))
async def show_any_broadcast(callback: CallbackQuery):
    parts = callback.data.split("_")
    btype = parts[3]
    bc_id = int(parts[4])
    
    if btype == 'dm':
        await show_dm_broadcast_detail(callback, bc_id)
    else:
        await show_chat_broadcast_detail(callback, bc_id)

async def show_chat_broadcast_detail(
    callback: CallbackQuery, broadcast_id: int
):
    async with db_pool.acquire() as conn:
        bc = await conn.fetchrow(
            'SELECT * FROM broadcasts WHERE id = $1', broadcast_id
        )
    
    if not bc:
        await callback.answer("Рассылка не найдена", show_alert=True)
        return
    
    bc = dict(bc)
    progress = (
        f"{bc['progress']}/{bc['total_count']}"
        if bc['total_count'] > 0 else "0/0"
    )
    
    scheduled_text = ""
    if bc.get('scheduled_at'):
        scheduled_text = (
            f"\n{emoji('CALENDAR')} Запланирована: "
            f"{bc['scheduled_at'].astimezone(MSK_TZ).strftime('%d.%m.%Y %H:%M')} МСК"
        )
    
    text = (
        f"{emoji('CHART')} <b>Рассылка ID: {bc['id']}</b>\n"
        f"Тип: Рассылка в чаты\n\n"
        f"{emoji('GEAR')} Статус: {bc['status']}{scheduled_text}\n"
        f"{emoji('STATS')} Прогресс: {progress}\n"
        f"{emoji('CLOCK')} Задержка: {bc['delay']} сек\n"
        f"{emoji('MAIL')} Сообщений в чат: {bc['message_count']}\n"
        f"{emoji('PEOPLE')} Чатов: {len(bc['chat_ids'])}\n"
        f"{emoji('CALENDAR')} Создана: "
        f"{bc['created_at'].astimezone(MSK_TZ).strftime('%d.%m.%Y %H:%M')}"
    )
    
    await callback.message.edit_text(
        text,
        reply_markup=get_broadcast_control_keyboard(bc['id'], 'chat')
    )
    await callback.answer()

async def show_dm_broadcast_detail(callback: CallbackQuery, dm_id: int):
    bc = await get_dm_broadcast(dm_id)
    
    if not bc:
        await callback.answer("Рассылка не найдена", show_alert=True)
        return
    
    progress = (
        f"{bc.get('progress', 0)}/{bc.get('total_count', 0)}"
        if bc.get('total_count', 0) > 0 else "0/0"
    )
    
    text = (
        f"{emoji('DM')} <b>DM Рассылка ID: {bc['id']}</b>\n"
        f"Тип: Рассылка в ЛС\n\n"
        f"{emoji('GEAR')} Статус: {bc['status']}\n"
        f"{emoji('STATS')} Прогресс: {progress}\n"
        f"{emoji('CLOCK')} Задержка: {bc['delay']} сек\n"
        f"{emoji('PEOPLE')} Получателей: {len(bc.get('usernames', []))}\n"
        f"{emoji('CALENDAR')} Создана: "
        f"{bc['created_at'].astimezone(MSK_TZ).strftime('%d.%m.%Y %H:%M')}"
    )
    
    await callback.message.edit_text(
        text,
        reply_markup=get_broadcast_control_keyboard(bc['id'], 'dm')
    )
    await callback.answer()

# --- Управление DM ---
@dp.callback_query(F.data.startswith("stop_dm_"))
async def stop_dm_from_list(callback: CallbackQuery):
    dm_id = int(callback.data.split("_")[2])
    
    for task_id, task in list(dm_broadcast_tasks.items()):
        dm_broadcast_stop_flags[task_id] = True
        task.cancel()
    
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE dm_broadcasts SET status = 'stopped', "
            "stopped_at = NOW() WHERE id = $1",
            dm_id
        )
    
    await callback.answer("Рассылка остановлена", show_alert=True)
    await show_dm_broadcast_detail(callback, dm_id)

@dp.callback_query(F.data.startswith("resume_dm_"))
async def resume_dm_from_list(callback: CallbackQuery):
    dm_id = int(callback.data.split("_")[2])
    bc = await get_dm_broadcast(dm_id)
    
    if not bc or bc['user_id'] != callback.from_user.id:
        await callback.answer("Рассылка не найдена", show_alert=True)
        return
    
    task_id = int(datetime.now().timestamp())
    task = asyncio.create_task(execute_dm_broadcast_db(
        dm_id, task_id, bc['account_id'], bc['user_id'],
        bc['usernames'], bc['message_text'], bc['delay'],
        bc.get('message_media', [])
    ))
    dm_broadcast_tasks[task_id] = task
    
    await callback.answer("Рассылка возобновлена", show_alert=True)
    await show_dm_broadcast_detail(callback, dm_id)

@dp.callback_query(F.data.startswith("clear_dm_self_"))
async def clear_dm_self(callback: CallbackQuery):
    dm_id = int(callback.data.split("_")[3])
    bc = await get_dm_broadcast(dm_id)
    
    if not bc or bc['user_id'] != callback.from_user.id:
        await callback.answer("Рассылка не найдена", show_alert=True)
        return
    
    client = await get_client_for_account(bc['account_id'])
    if not client:
        await callback.answer("Не удалось подключиться", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"{emoji('LOADING')} Удаляю чаты у себя..."
    )
    
    cleaned = 0
    for username in bc.get('usernames', []):
        try:
            if not username.startswith('@'):
                username = '@' + username
            entity = await client.get_entity(username)
            await delete_chat_history(client, entity.id, for_both=False)
            cleaned += 1
            await asyncio.sleep(1)
        except Exception as ex:
            logger.error(f"Error clearing chat with {username}: {ex}")
    
    await callback.message.edit_text(
        f"{emoji('CHECK')} <b>Готово!</b>\n\n"
        f"Удалено чатов у себя: {cleaned}/"
        f"{len(bc.get('usernames', []))}",
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("clear_dm_both_"))
async def clear_dm_both(callback: CallbackQuery):
    dm_id = int(callback.data.split("_")[3])
    bc = await get_dm_broadcast(dm_id)
    
    if not bc or bc['user_id'] != callback.from_user.id:
        await callback.answer("Рассылка не найдена", show_alert=True)
        return
    
    client = await get_client_for_account(bc['account_id'])
    if not client:
        await callback.answer("Не удалось подключиться", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"{emoji('LOADING')} Удаляю чаты у всех..."
    )
    
    cleaned = 0
    for username in bc.get('usernames', []):
        try:
            if not username.startswith('@'):
                username = '@' + username
            entity = await client.get_entity(username)
            await delete_chat_history(client, entity.id, for_both=True)
            cleaned += 1
            await asyncio.sleep(1)
        except Exception as ex:
            logger.error(f"Error clearing chat with {username}: {ex}")
    
    await callback.message.edit_text(
        f"{emoji('CHECK')} <b>Готово!</b>\n\n"
        f"Удалено чатов у всех: {cleaned}/"
        f"{len(bc.get('usernames', []))}",
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_dm_broadcast_"))
async def delete_dm_broadcast(callback: CallbackQuery):
    dm_id = int(callback.data.split("_")[3])
    
    for task_id, task in list(dm_broadcast_tasks.items()):
        dm_broadcast_stop_flags[task_id] = True
        task.cancel()
    
    async with db_pool.acquire() as conn:
        await conn.execute(
            'DELETE FROM dm_broadcasts WHERE id = $1', dm_id
        )
    
    await callback.message.edit_text(
        f"{emoji('CHECK')} DM Рассылка удалена!",
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()

# --- Рассылка в ЛС ---
@dp.callback_query(F.data == "dm_broadcast")
async def dm_broadcast_menu(callback: CallbackQuery, state: FSMContext):
    accounts = await get_user_accounts(callback.from_user.id)
    if not accounts:
        await callback.message.edit_text(
            f"{emoji('CROSS')} У вас нет аккаунтов.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    await callback.message.edit_text(
        f"{emoji('PROFILE')} <b>Выберите аккаунт для рассылки в ЛС:</b>",
        reply_markup=get_accounts_list_keyboard(
            accounts, "select_dm_account"
        )
    )
    await state.set_state(DMBroadcastStates.waiting_for_account)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_dm_account_"))
async def select_dm_account(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[3])
    await state.update_data(account_id=account_id)
    
    await callback.message.edit_text(
        f"{emoji('FILE')} <b>Отправьте TXT файл со списком юзернеймов</b>\n\n"
        f"Каждый юзернейм с новой строки.\n"
        f"Пример файла:\n"
        f"<code>@username1\n@username2\nusername3</code>\n\n"
        f"Можно с @ или без.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="functions",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.set_state(DMBroadcastStates.waiting_for_file)
    await callback.answer()

@dp.message(DMBroadcastStates.waiting_for_file, F.document)
async def process_dm_file(message: Message, state: FSMContext):
    try:
        file_path = f"media/{message.document.file_id}.txt"
        await message.bot.download(message.document, file_path)
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        usernames = [
            line.strip() for line in content.split('\n') if line.strip()
        ]
        
        if not usernames:
            await message.answer(
                f"{emoji('CROSS')} Файл пуст или не содержит юзернеймов."
            )
            os.remove(file_path)
            return
        
        await state.update_data(
            usernames=usernames, usernames_count=len(usernames)
        )
        
        os.remove(file_path)
        
        await message.answer(
            f"{emoji('CHECK')} <b>Файл загружен!</b>\n\n"
            f"Найдено юзернеймов: <b>{len(usernames)}</b>\n\n"
            f"{emoji('INFO')} <b>Доступные переменные:</b>\n"
            f"<code>{'{username}'}</code> - юзернейм\n"
            f"<code>{'{first_name}'}</code> - имя\n"
            f"<code>{'{last_name}'}</code> - фамилия\n"
            f"<code>{'{user_id}'}</code> - ID пользователя\n\n"
            f"{emoji('WRITE')} <b>Введите сообщение для рассылки:</b>\n"
            f"Поддерживается HTML и премиум эмодзи.\n"
            f"Можно прикрепить медиа.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Назад",
                    callback_data="functions",
                    style='default',
                    icon_custom_emoji_id=get_icon("BACK")
                )
            ]])
        )
        await state.set_state(DMBroadcastStates.waiting_for_message)
        
    except Exception as ex:
        await message.answer(
            f"{emoji('CROSS')} Ошибка при чтении файла: {str(ex)}"
        )

@dp.message(DMBroadcastStates.waiting_for_file)
async def process_dm_file_invalid(message: Message):
    await message.answer(
        f"{emoji('CROSS')} Пожалуйста, отправьте TXT файл с юзернеймами."
    )

@dp.message(DMBroadcastStates.waiting_for_message)
async def process_dm_message(message: Message, state: FSMContext):
    text = (
        message.html_text
        if message.html_text
        else (message.text or message.caption or "")
    )
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
        f"{emoji('CLOCK')} <b>Введите задержку между сообщениями</b>\n\n"
        f"Минимум 60 секунд:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="functions",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.set_state(DMBroadcastStates.waiting_for_delay)

@dp.message(DMBroadcastStates.waiting_for_delay)
async def process_dm_delay(message: Message, state: FSMContext):
    try:
        delay = int(message.text.strip())
        if delay < 60:
            await message.answer(
                f"{emoji('CROSS')} Минимальная задержка 60 секунд!"
            )
            return
    except ValueError:
        await message.answer(
            f"{emoji('CROSS')} Введите число (минимум 60):"
        )
        return
    
    await state.update_data(delay=delay)
    
    data = await state.get_data()
    
    preview_text = (
        f"{emoji('EYE')} <b>Предпросмотр рассылки в ЛС:</b>\n\n"
        f"{emoji('PROFILE')} Аккаунт ID: {data['account_id']}\n"
        f"{emoji('PEOPLE')} Получателей: {data['usernames_count']}\n"
        f"{emoji('CLOCK')} Задержка: {delay} сек\n"
        f"{emoji('MEDIA')} Медиа: "
        f"{len(data.get('message_media', []))} файлов"
    )
    
    await message.answer(
        preview_text, reply_markup=get_dm_broadcast_preview_keyboard()
    )
    
    if data.get('message_media') and len(data['message_media']) > 0:
        if (
            len(data['message_media']) == 1
            and os.path.exists(data['message_media'][0])
        ):
            await message.answer_document(
                FSInputFile(data['message_media'][0]),
                caption=data['message_text'],
                parse_mode='HTML'
            )
    else:
        await message.answer(data['message_text'], parse_mode='HTML')
    
    await state.set_state(DMBroadcastStates.preview)

@dp.callback_query(
    F.data == "start_dm_broadcast", DMBroadcastStates.preview
)
async def start_dm_broadcast(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        dm_id = await conn.fetchval(
            "INSERT INTO dm_broadcasts "
            "(user_id, account_id, usernames, delay, message_text, "
            "message_media, status, total_count) "
            "VALUES ($1, $2, $3::text[], $4, $5, $6::text[], 'active', $7) "
            "RETURNING id",
            user_id, data['account_id'], data['usernames'],
            data['delay'], data['message_text'],
            data.get('message_media', []), len(data['usernames'])
        )
    
    task_id = int(datetime.now().timestamp())
    
    await callback.message.edit_text(
        f"{emoji('LOADING')} <b>Запускаю рассылку в ЛС...</b>\n\n"
        f"DM ID: {dm_id}\n"
        f"Получателей: {data['usernames_count']}\n"
        f"Это может занять некоторое время.",
        reply_markup=get_broadcast_control_keyboard(dm_id, 'dm')
    )
    
    task = asyncio.create_task(execute_dm_broadcast_db(
        dm_id, task_id, data['account_id'], user_id,
        data['usernames'], data['message_text'], data['delay'],
        data.get('message_media', [])
    ))
    dm_broadcast_tasks[task_id] = task
    
    async def wait_and_report():
        result = await task
        try:
            if dm_broadcast_stop_flags.get(task_id, False):
                pass
            elif result:
                try:
                    await callback.message.edit_text(
                        f"{emoji('CHECK')} <b>Рассылка в ЛС завершена!</b>\n\n"
                        f"DM ID: {dm_id}\n"
                        f"Всего: {result['total']}\n"
                        f"Отправлено: {result['sent']}\n"
                        f"Ошибок: {result['failed']}",
                        reply_markup=get_broadcast_control_keyboard(
                            dm_id, 'dm'
                        )
                    )
                except:
                    pass
        except:
            pass
        finally:
            if task_id in dm_broadcast_tasks:
                del dm_broadcast_tasks[task_id]
            if task_id in dm_broadcast_stop_flags:
                del dm_broadcast_stop_flags[task_id]
    
    asyncio.create_task(wait_and_report())
    
    await state.clear()
    await callback.answer()

# --- Вступление в чаты ---
@dp.callback_query(F.data == "join_chats")
async def join_chats_menu(callback: CallbackQuery, state: FSMContext):
    accounts = await get_user_accounts(callback.from_user.id)
    if not accounts:
        await callback.message.edit_text(
            f"{emoji('CROSS')} У вас нет аккаунтов.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    await callback.message.edit_text(
        f"{emoji('PROFILE')} <b>Выберите аккаунт для вступления:</b>",
        reply_markup=get_accounts_list_keyboard(
            accounts, "select_join_account"
        )
    )
    await state.set_state(JoinStates.waiting_for_account)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_join_account_"))
async def select_join_account(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[3])
    await state.update_data(account_id=account_id)
    
    await callback.message.edit_text(
        f"{emoji('FILE')} <b>Отправьте TXT файл со ссылками на чаты</b>\n\n"
        f"Поддерживаются:\n"
        f"• Публичные: <code>@chatname</code> или "
        f"<code>https://t.me/chatname</code>\n"
        f"• Приватные: <code>https://t.me/+hash</code>\n\n"
        f"Каждая ссылка с новой строки.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="functions",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.set_state(JoinStates.waiting_for_file)
    await callback.answer()

@dp.message(JoinStates.waiting_for_file, F.document)
async def process_join_file(message: Message, state: FSMContext):
    try:
        file_path = f"media/{message.document.file_id}.txt"
        await message.bot.download(message.document, file_path)
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        links = [
            line.strip() for line in content.split('\n') if line.strip()
        ]
        
        if not links:
            await message.answer(f"{emoji('CROSS')} Файл пуст.")
            os.remove(file_path)
            return
        
        await state.update_data(links=links, links_count=len(links))
        os.remove(file_path)
        
        await message.answer(
            f"{emoji('CHECK')} <b>Файл загружен!</b>\n\n"
            f"Найдено ссылок: <b>{len(links)}</b>\n\n"
            f"{emoji('CLOCK')} <b>Введите задержку между вступлениями</b>\n\n"
            f"Минимум 30 секунд:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Назад",
                    callback_data="functions",
                    style='default',
                    icon_custom_emoji_id=get_icon("BACK")
                )
            ]])
        )
        await state.set_state(JoinStates.waiting_for_delay)
        
    except Exception as ex:
        await message.answer(f"{emoji('CROSS')} Ошибка: {str(ex)}")

@dp.message(JoinStates.waiting_for_file)
async def process_join_file_invalid(message: Message):
    await message.answer(
        f"{emoji('CROSS')} Пожалуйста, отправьте TXT файл."
    )

@dp.message(JoinStates.waiting_for_delay)
async def process_join_delay(message: Message, state: FSMContext):
    try:
        delay = int(message.text.strip())
        if delay < 30:
            await message.answer(
                f"{emoji('CROSS')} Минимальная задержка 30 секунд!"
            )
            return
    except ValueError:
        await message.answer(
            f"{emoji('CROSS')} Введите число (минимум 30):"
        )
        return
    
    await state.update_data(delay=delay)
    
    data = await state.get_data()
    
    preview_text = (
        f"{emoji('EYE')} <b>Предпросмотр вступления:</b>\n\n"
        f"{emoji('PROFILE')} Аккаунт ID: {data['account_id']}\n"
        f"{emoji('LINK')} Чатов: {data['links_count']}\n"
        f"{emoji('CLOCK')} Задержка: {delay} сек"
    )
    
    await message.answer(
        preview_text, reply_markup=get_join_preview_keyboard()
    )
    await state.set_state(JoinStates.preview)

@dp.callback_query(F.data == "start_join", JoinStates.preview)
async def start_join(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    task_id = int(datetime.now().timestamp())
    
    await callback.message.edit_text(
        f"{emoji('LOADING')} <b>Запускаю вступление в чаты...</b>\n\n"
        f"Чатов: {data['links_count']}\n"
        f"Task ID: {task_id}",
        reply_markup=get_join_control_keyboard(task_id)
    )
    
    task = asyncio.create_task(execute_join(
        task_id, data['account_id'], user_id,
        data['links'], data['delay']
    ))
    join_tasks[task_id] = task
    
    async def wait_and_report():
        result = await task
        try:
            if join_stop_flags.get(task_id, False):
                await callback.message.edit_text(
                    f"{emoji('STOP')} <b>Вступление остановлено!</b>\n\n"
                    f"Task ID: {task_id}",
                    reply_markup=get_functions_keyboard()
                )
            elif result:
                await callback.message.edit_text(
                    f"{emoji('CHECK')} <b>Вступление завершено!</b>\n\n"
                    f"Всего: {result['total']}\n"
                    f"Вступил: {result['joined']}\n"
                    f"Ошибок: {result['failed']}",
                    reply_markup=get_functions_keyboard()
                )
        except:
            pass
        finally:
            if task_id in join_tasks:
                del join_tasks[task_id]
            if task_id in join_stop_flags:
                del join_stop_flags[task_id]
    
    asyncio.create_task(wait_and_report())
    
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data.startswith("stop_join_"))
async def stop_join(callback: CallbackQuery):
    task_id = int(callback.data.split("_")[2])
    join_stop_flags[task_id] = True
    
    if task_id in join_tasks:
        join_tasks[task_id].cancel()
    
    await callback.message.edit_text(
        f"{emoji('STOP')} <b>Вступление остановлено!</b>\n\n"
        f"Task ID: {task_id}",
        reply_markup=get_functions_keyboard()
    )
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
        reply_markup=get_accounts_list_keyboard(
            accounts, "select_responder_account"
        )
    )
    await state.set_state(AutoResponderStates.waiting_for_account)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_responder_account_"))
async def select_responder_account(
    callback: CallbackQuery, state: FSMContext
):
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
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="functions",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.set_state(AutoResponderStates.waiting_for_trigger)
    await callback.answer()

@dp.message(AutoResponderStates.waiting_for_trigger)
async def process_trigger(message: Message, state: FSMContext):
    trigger = message.text.strip()
    if not trigger:
        await message.answer(
            f"{emoji('CROSS')} Введите слово-триггер или '-'"
        )
        return
    
    await state.update_data(trigger=trigger)
    await message.answer(
        f"{emoji('WRITE')} <b>Введите ответ:</b>\n\n"
        f"Поддерживается HTML, премиум эмодзи и переменные.\n"
        f"Можно прикрепить медиа.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="functions",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.set_state(AutoResponderStates.waiting_for_response)

@dp.message(AutoResponderStates.waiting_for_response)
async def process_auto_response(message: Message, state: FSMContext):
    text = (
        message.html_text
        if message.html_text
        else (message.text or message.caption or "")
    )
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
    preview_text = (
        f"{emoji('EYE')} <b>Предпросмотр автоответчика:</b>\n\n"
        f"{emoji('TAG')} Триггер: {escape(data['trigger'])}\n"
        f"{emoji('MEDIA')} Медиа: {len(media_paths)} файлов"
    )
    
    await message.answer(
        preview_text, reply_markup=get_auto_responder_preview_keyboard()
    )
    
    if media_paths and len(media_paths) > 0:
        if len(media_paths) == 1 and os.path.exists(media_paths[0]):
            await message.answer_document(
                FSInputFile(media_paths[0]),
                caption=text,
                parse_mode='HTML'
            )
    else:
        await message.answer(text, parse_mode='HTML')
    
    await state.set_state(AutoResponderStates.preview)

@dp.callback_query(
    F.data == "create_auto_responder", AutoResponderStates.preview
)
async def create_auto_responder(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        responder_id = await conn.fetchval(
            "INSERT INTO auto_responders "
            "(user_id, account_id, trigger, response_text, response_media) "
            "VALUES ($1, $2, $3, $4, $5::text[]) RETURNING id",
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
        builder.row(InlineKeyboardButton(
            text=(
                f"{status} ID:{resp['id']} | "
                f"Триггер: {escape(resp['trigger'][:20])}"
            ),
            callback_data=f"show_responder_{resp['id']}",
            style='default',
            icon_custom_emoji_id=get_icon("BELL")
        ))
    
    builder.row(InlineKeyboardButton(
        text="Создать новый",
        callback_data="auto_responder",
        style='primary',
        icon_custom_emoji_id=get_icon("ADD_TEXT")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="functions",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    
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
        builder.row(InlineKeyboardButton(
            text="Остановить",
            callback_data=f"stop_responder_{responder_id}",
            style='danger',
            icon_custom_emoji_id=get_icon("STOP")
        ))
    else:
        builder.row(InlineKeyboardButton(
            text="Запустить",
            callback_data=f"start_responder_{responder_id}",
            style='success',
            icon_custom_emoji_id=get_icon("PLAY")
        ))
    
    builder.row(InlineKeyboardButton(
        text="Удалить",
        callback_data=f"delete_responder_{responder_id}",
        style='default',
        icon_custom_emoji_id=get_icon("DELETE")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="my_auto_responders",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    
    text = (
        f"{emoji('BELL')} <b>Автоответчик ID: {responder['id']}</b>\n\n"
        f"{emoji('EYE')} Статус: "
        f"{'✅ Активен' if responder['is_active'] else '❌ Остановлен'}\n"
        f"{emoji('TAG')} Триггер: "
        f"<code>{escape(responder['trigger'])}</code>\n"
        f"{emoji('WRITE')} Ответ: "
        f"{escape((responder['response_text'] or '')[:100])}\n"
        f"{emoji('CLOCK')} Создан: "
        f"{responder['created_at'].astimezone(MSK_TZ).strftime('%d.%m.%Y %H:%M')}"
    )
    
    await callback.message.edit_text(
        text, reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("stop_responder_"))
async def stop_responder(callback: CallbackQuery):
    responder_id = int(callback.data.split("_")[2])
    responder = await get_auto_responder(responder_id)
    
    if responder and responder['user_id'] == callback.from_user.id:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE auto_responders SET is_active = FALSE "
                "WHERE id = $1",
                responder_id
            )
        
        if callback.from_user.id in active_auto_responders:
            if responder['account_id'] in active_auto_responders[callback.from_user.id]:
                active_auto_responders[callback.from_user.id][responder['account_id']].cancel()
                del active_auto_responders[callback.from_user.id][responder['account_id']]
        
        account_id = responder['account_id']
        if account_id in active_clients:
            try:
                await active_clients[account_id].disconnect()
            except:
                pass
            del active_clients[account_id]
        
        await callback.answer("Автоответчик остановлен", show_alert=True)
        await show_responder(callback)

@dp.callback_query(F.data.startswith("start_responder_"))
async def start_responder(callback: CallbackQuery):
    responder_id = int(callback.data.split("_")[2])
    responder = await get_auto_responder(responder_id)
    
    if responder and responder['user_id'] == callback.from_user.id:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE auto_responders SET is_active = TRUE "
                "WHERE id = $1",
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
                del active_auto_responders[callback.from_user.id][responder['account_id']]
        
        async with db_pool.acquire() as conn:
            await conn.execute(
                'DELETE FROM auto_responders WHERE id = $1',
                responder_id
            )
        
        await callback.answer("Автоответчик удален", show_alert=True)
        await my_auto_responders(callback)

# --- Авто-лайкинг ---
@dp.callback_query(F.data == "autolike")
async def autolike_menu(callback: CallbackQuery, state: FSMContext):
    accounts = await get_user_accounts(callback.from_user.id)
    if not accounts:
        await callback.message.edit_text(
            f"{emoji('CROSS')} У вас нет аккаунтов.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    await callback.message.edit_text(
        f"{emoji('PROFILE')} <b>Выберите аккаунт для авто-лайкинга:</b>",
        reply_markup=get_accounts_list_keyboard(
            accounts, "select_autolike_account"
        )
    )
    await state.set_state(AutoLikeStates.waiting_for_account)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_autolike_account_"))
async def select_autolike_account(
    callback: CallbackQuery, state: FSMContext
):
    account_id = int(callback.data.split("_")[3])
    await state.update_data(account_id=account_id)
    
    client = await get_client_for_account(account_id)
    if not client:
        await callback.answer(
            "Не удалось подключиться", show_alert=True
        )
        return
    
    await callback.message.edit_text(
        f"{emoji('LOADING')} Загружаю чаты...",
        reply_markup=None
    )
    
    chats = await get_chats_from_client(client)
    await state.update_data(chats=chats, selected_chats=[], current_page=0)
    
    total_pages = (len(chats) - 1) // 10 + 1
    await callback.message.edit_text(
        f"{emoji('PEOPLE')} <b>Выберите чаты для лайкинга</b>\n"
        f"Страница 1 из {total_pages}",
        reply_markup=get_chat_selection_keyboard(chats, 0, [])
    )
    await state.set_state(AutoLikeStates.selecting_chats)
    await callback.answer()

@dp.callback_query(
    F.data.startswith("react_"), AutoLikeStates.waiting_for_reaction
)
async def select_reaction(callback: CallbackQuery, state: FSMContext):
    reaction = callback.data.replace("react_", "")
    await state.update_data(reaction=reaction)
    
    await callback.message.edit_text(
        f"{emoji('CLOCK')} <b>Введите задержку между лайками</b>\n\n"
        f"Минимум 5 секунд:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="autolike",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.set_state(AutoLikeStates.waiting_for_delay)
    await callback.answer()

@dp.message(AutoLikeStates.waiting_for_delay)
async def process_autolike_delay(message: Message, state: FSMContext):
    try:
        delay = int(message.text.strip())
        if delay < 5:
            await message.answer(
                f"{emoji('CROSS')} Минимальная задержка 5 секунд!"
            )
            return
    except ValueError:
        await message.answer(
            f"{emoji('CROSS')} Введите число (минимум 5):"
        )
        return
    
    await state.update_data(delay=delay)
    
    data = await state.get_data()
    
    preview_text = (
        f"{emoji('EYE')} <b>Предпросмотр авто-лайкинга:</b>\n\n"
        f"{emoji('PROFILE')} Аккаунт ID: {data['account_id']}\n"
        f"{emoji('PEOPLE')} Чатов: {len(data['selected_chats'])}\n"
        f"{emoji('LIKE')} Реакция: {data['reaction']}\n"
        f"{emoji('CLOCK')} Задержка: {delay} сек"
    )
    
    await message.answer(
        preview_text, reply_markup=get_autolike_preview_keyboard()
    )
    await state.set_state(AutoLikeStates.preview)

@dp.callback_query(F.data == "start_autolike", AutoLikeStates.preview)
async def start_autolike(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    task_id = int(datetime.now().timestamp())
    
    await callback.message.edit_text(
        f"{emoji('LOADING')} <b>Запускаю авто-лайкинг...</b>\n\n"
        f"Чатов: {len(data['selected_chats'])}\n"
        f"Реакция: {data['reaction']}\n"
        f"Task ID: {task_id}",
        reply_markup=get_autolike_control_keyboard(task_id)
    )
    
    task = asyncio.create_task(execute_autolike(
        task_id, data['account_id'], data['selected_chats'],
        data['reaction'], data['delay']
    ))
    autolike_tasks[task_id] = task
    
    async def wait_and_report():
        result = await task
        try:
            if autolike_stop_flags.get(task_id, False):
                await callback.message.edit_text(
                    f"{emoji('STOP')} <b>Авто-лайкинг остановлен!</b>\n\n"
                    f"Task ID: {task_id}",
                    reply_markup=get_functions_keyboard()
                )
            elif result:
                await callback.message.edit_text(
                    f"{emoji('CHECK')} <b>Авто-лайкинг завершён!</b>\n\n"
                    f"Лайков: {result['liked']}\n"
                    f"Ошибок: {result['errors']}",
                    reply_markup=get_functions_keyboard()
                )
        except:
            pass
        finally:
            if task_id in autolike_tasks:
                del autolike_tasks[task_id]
            if task_id in autolike_stop_flags:
                del autolike_stop_flags[task_id]
    
    asyncio.create_task(wait_and_report())
    
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data.startswith("stop_autolike_"))
async def stop_autolike(callback: CallbackQuery):
    task_id = int(callback.data.split("_")[2])
    autolike_stop_flags[task_id] = True
    
    if task_id in autolike_tasks:
        autolike_tasks[task_id].cancel()
    
    await callback.message.edit_text(
        f"{emoji('STOP')} <b>Авто-лайкинг остановлен!</b>\n\n"
        f"Task ID: {task_id}",
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()

# --- Удаление сообщений ---
@dp.callback_query(F.data == "delete_messages")
async def delete_messages_menu(callback: CallbackQuery, state: FSMContext):
    accounts = await get_user_accounts(callback.from_user.id)
    if not accounts:
        await callback.message.edit_text(
            f"{emoji('CROSS')} У вас нет аккаунтов.",
            reply_markup=get_functions_keyboard()
        )
        await callback.answer()
        return
    
    await callback.message.edit_text(
        f"{emoji('PROFILE')} <b>Выберите аккаунт для удаления "
        f"сообщений:</b>",
        reply_markup=get_accounts_list_keyboard(
            accounts, "select_delete_account"
        )
    )
    await state.set_state(DeleteMessagesStates.waiting_for_account)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_delete_account_"))
async def select_delete_account(
    callback: CallbackQuery, state: FSMContext
):
    account_id = int(callback.data.split("_")[3])
    await state.update_data(account_id=account_id)
    
    client = await get_client_for_account(account_id)
    if not client:
        await callback.answer(
            "Не удалось подключиться", show_alert=True
        )
        return
    
    await callback.message.edit_text(
        f"{emoji('LOADING')} Загружаю чаты...",
        reply_markup=None
    )
    
    chats = await get_chats_from_client(client)
    await state.update_data(chats=chats, selected_chats=[], current_page=0)
    
    total_pages = (len(chats) - 1) // 10 + 1
    await callback.message.edit_text(
        f"{emoji('PEOPLE')} <b>Выберите чаты для удаления сообщений</b>\n"
        f"Страница 1 из {total_pages}",
        reply_markup=get_chat_selection_keyboard(chats, 0, [])
    )
    await state.set_state(DeleteMessagesStates.selecting_chats)
    await callback.answer()

@dp.message(DeleteMessagesStates.waiting_for_hours)
async def process_delete_hours(message: Message, state: FSMContext):
    try:
        hours = int(message.text.strip())
        if hours < 1:
            await message.answer(
                f"{emoji('CROSS')} Минимум 1 час!"
            )
            return
    except ValueError:
        await message.answer(
            f"{emoji('CROSS')} Введите число часов (минимум 1):"
        )
        return
    
    await state.update_data(hours=hours)
    
    data = await state.get_data()
    
    preview_text = (
        f"{emoji('EYE')} <b>Предпросмотр удаления сообщений:</b>\n\n"
        f"{emoji('PROFILE')} Аккаунт ID: {data['account_id']}\n"
        f"{emoji('PEOPLE')} Чатов: {len(data['selected_chats'])}\n"
        f"{emoji('CLOCK')} За последние: {hours} часов\n"
        f"{emoji('SWEEP')} Будут удалены ВСЕ ваши сообщения "
        f"за этот период!"
    )
    
    await message.answer(
        preview_text, reply_markup=get_delete_messages_preview_keyboard()
    )
    await state.set_state(DeleteMessagesStates.preview)

@dp.callback_query(
    F.data == "start_delete_messages", DeleteMessagesStates.preview
)
async def start_delete_messages(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    task_id = int(datetime.now().timestamp())
    
    await callback.message.edit_text(
        f"{emoji('LOADING')} <b>Запускаю удаление сообщений...</b>\n\n"
        f"Чатов: {len(data['selected_chats'])}\n"
        f"За {data['hours']} часов\n"
        f"Task ID: {task_id}",
        reply_markup=get_delete_messages_control_keyboard(task_id)
    )
    
    task = asyncio.create_task(execute_delete_messages(
        task_id, data['account_id'],
        data['selected_chats'], data['hours']
    ))
    
    async def wait_and_report():
        result = await task
        try:
            if delete_messages_stop_flags.get(task_id, False):
                await callback.message.edit_text(
                    f"{emoji('STOP')} <b>Удаление остановлено!</b>\n\n"
                    f"Task ID: {task_id}",
                    reply_markup=get_functions_keyboard()
                )
            elif result:
                await callback.message.edit_text(
                    f"{emoji('CHECK')} <b>Удаление завершено!</b>\n\n"
                    f"Удалено: {result['deleted']}\n"
                    f"Ошибок: {result['errors']}",
                    reply_markup=get_functions_keyboard()
                )
        except:
            pass
        finally:
            if task_id in delete_messages_stop_flags:
                del delete_messages_stop_flags[task_id]
    
    asyncio.create_task(wait_and_report())
    
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data.startswith("stop_delete_msg_"))
async def stop_delete_messages(callback: CallbackQuery):
    task_id = int(callback.data.split("_")[3])
    delete_messages_stop_flags[task_id] = True
    
    await callback.message.edit_text(
        f"{emoji('STOP')} <b>Удаление остановлено!</b>\n\n"
        f"Task ID: {task_id}",
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()

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
        reply_markup=get_accounts_list_keyboard(
            accounts, "select_parsing_account"
        )
    )
    await state.set_state(ParsingStates.waiting_for_account)
    await callback.answer()

@dp.callback_query(F.data.startswith("select_parsing_account_"))
async def select_parsing_account(
    callback: CallbackQuery, state: FSMContext
):
    account_id = int(callback.data.split("_")[3])
    await state.update_data(account_id=account_id)
    
    await callback.message.edit_text(
        f"{emoji('GLOBE')} <b>Введите юзернейм или ссылку на чат:</b>\n\n"
        f"Пример: <code>@chatname</code> или "
        f"<code>https://t.me/chatname</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Назад",
                callback_data="functions",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
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

@dp.callback_query(
    ParsingStates.waiting_for_mode, F.data.startswith("parse_mode_")
)
async def process_parsing_mode(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.replace("parse_mode_", "")
    data = await state.get_data()
    account_id = data['account_id']
    chat_username = data['chat_username']
    
    client = await get_client_for_account(account_id)
    if not client:
        await callback.message.edit_text(
            f"{emoji('CROSS')} Не удалось подключиться к аккаунту."
        )
        await state.clear()
        return
    
    await callback.message.edit_text(
        f"{emoji('LOADING')} Собираю участников из "
        f"<code>{chat_username}</code>...\n"
        f"Проверяю последние 5000 сообщений..."
    )
    
    try:
        entity = await client.get_entity(chat_username)
        
        users = []
        count = 0
        async for msg in client.iter_messages(entity, limit=5000):
            if msg.sender_id and not any(
                u['user_id'] == msg.sender_id for u in users
            ):
                try:
                    sender = await msg.get_sender()
                    if sender and isinstance(sender, User):
                        user_data = {
                            'user_id': sender.id,
                            'username': (
                                '@' + sender.username
                                if sender.username else ''
                            ),
                            'first_name': sender.first_name or '',
                            'last_name': sender.last_name or '',
                        }
                        users.append(user_data)
                        count += 1
                except:
                    pass
        
        filename = (
            f"parsed_{chat_username.replace('@', '')}_"
            f"{mode}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )
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
                    f.write(
                        f"{user['user_id']}|{user['username']}|"
                        f"{user['first_name']}|{user['last_name']}\n"
                    )
                elif mode == 'usernames':
                    if user['username']:
                        f.write(f"{user['username']}\n")
                elif mode == 'names':
                    name = ' '.join(filter(None, [
                        user['first_name'], user['last_name']
                    ]))
                    if name:
                        f.write(f"{name}\n")
                elif mode == 'names_usernames':
                    name = ' '.join(filter(None, [
                        user['first_name'], user['last_name']
                    ]))
                    f.write(f"{name}|{user['username']}\n")
        
        await callback.message.answer_document(
            FSInputFile(filepath),
            caption=(
                f"{emoji('CHECK')} <b>Парсинг завершён!</b>\n\n"
                f"Чат: <code>{chat_username}</code>\n"
                f"Режим: {mode_names.get(mode, mode)}\n"
                f"Собрано пользователей: <b>{len(users)}</b>\n"
                f"Проверено сообщений: {count}"
            ),
            parse_mode='HTML'
        )
        
        os.remove(filepath)
        
    except Exception as ex:
        await callback.message.edit_text(
            f"{emoji('CROSS')} Ошибка: {str(ex)}"
        )
    
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
    builder.row(InlineKeyboardButton(
        text="Рассылка всем пользователям",
        callback_data="admin_broadcast_all",
        style='primary',
        icon_custom_emoji_id=get_icon("MEGAPHONE")
    ))
    builder.row(InlineKeyboardButton(
        text="Обновить статистику",
        callback_data="admin_refresh_stats",
        style='default',
        icon_custom_emoji_id=get_icon("REFRESH")
    ))
    
    admin_text = (
        f"{emoji('BOT')} <b>Админ-панель</b>\n\n"
        f"{emoji('PEOPLE')} Пользователей: "
        f"<b>{stats['total_users']}</b>\n"
        f"{emoji('PROFILE')} Аккаунтов: "
        f"<b>{stats['total_accounts']}</b>\n"
        f"{emoji('MEGAPHONE')} Всего рассылок: "
        f"<b>{stats['total_broadcasts']}</b>\n"
        f"{emoji('PLAY')} Активных: "
        f"<b>{stats['active_broadcasts']}</b>"
    )
    
    await callback.message.edit_text(
        admin_text, reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_broadcast_all")
async def admin_broadcast_all(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"{emoji('MEGAPHONE')} <b>Рассылка всем пользователям</b>\n\n"
        f"{emoji('INFO')} <b>Доступные переменные:</b>\n"
        f"<code>{'{username}'}</code> - юзернейм\n"
        f"<code>{'{first_name}'}</code> - имя\n"
        f"<code>{'{user_id}'}</code> - ID пользователя\n\n"
        f"Введите сообщение для рассылки:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Отмена",
                callback_data="admin_refresh_stats",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.set_state(AdminStates.waiting_for_broadcast_message)
    await callback.answer()

@dp.message(AdminStates.waiting_for_broadcast_message)
async def process_admin_broadcast(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    async with db_pool.acquire() as conn:
        users = await conn.fetch(
            'SELECT user_id, username, first_name FROM users'
        )
    
    broadcast_text = (
        message.html_text
        if message.html_text
        else (message.text or message.caption or "")
    )
    
    success = 0
    for user in users:
        try:
            user_data = {
                'username': user['username'] or '',
                'first_name': user['first_name'] or '',
                'last_name': '',
                'user_id': user['user_id'],
            }
            processed_text = process_variables(broadcast_text, user_data)
            
            await bot.send_message(user['user_id'], processed_text)
            success += 1
            await asyncio.sleep(0.05)
        except Exception as ex:
            logger.error(
                f"Failed to send to {user['user_id']}: {ex}"
            )
    
    await message.answer(
        f"{emoji('CHECK')} <b>Рассылка завершена!</b>\n\n"
        f"Отправлено: {success}/{len(users)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="В админ-панель",
                callback_data="admin_refresh_stats",
                style='primary',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.clear()


# ========== ПРОКСИ ==========

@dp.callback_query(F.data == "my_proxies")
async def my_proxies(callback: CallbackQuery):
    proxies = await get_user_proxies(callback.from_user.id)

    if not proxies:
        text = (
            f"{emoji('LINK')} <b>Прокси</b>\n\n"
            f"У вас пока нет прокси. Добавьте первый — "
            f"это поможет держать несколько аккаунтов с разных IP "
            f"и снизить риск банов."
        )
    else:
        text = f"{emoji('LINK')} <b>Ваши прокси ({len(proxies)}):</b>\n\n"
        for p in proxies[:20]:
            label = p.get('label') or f"{p['host']}:{p['port']}"
            auth = (
                f" (auth: {p['username']})"
                if p.get('username') else ""
            )
            status = "✅" if p.get('is_active') else "❌"
            text += (
                f"{status} <b>{escape(label)}</b>\n"
                f"   <code>{p['proxy_type']}://"
                f"{p['host']}:{p['port']}</code>{auth}\n"
            )

    await callback.message.edit_text(
        text, reply_markup=get_proxies_keyboard(proxies)
    )
    await callback.answer()


@dp.callback_query(F.data == "add_proxy")
async def add_proxy_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"{emoji('LINK')} <b>Добавление прокси</b>\n\n"
        f"Отправьте строку прокси в одном из форматов:\n\n"
        f"<code>socks5://user:pass@host:port</code>\n"
        f"<code>socks4://user:pass@host:port</code>\n"
        f"<code>http://user:pass@host:port</code>\n"
        f"<code>host:port:user:pass</code>\n"
        f"<code>host:port</code>\n\n"
        f"Поддерживаются SOCKS5, SOCKS4 и HTTP.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Отмена",
                callback_data="my_proxies",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.set_state(ProxyStates.waiting_for_proxy_string)
    await callback.answer()


@dp.message(ProxyStates.waiting_for_proxy_string)
async def process_proxy_string(message: Message, state: FSMContext):
    parsed = parse_proxy_string(message.text)
    if not parsed or not parsed.get('host') or not parsed.get('port'):
        await message.answer(
            f"{emoji('CROSS')} Не удалось распарсить прокси. "
            f"Проверьте формат и попробуйте снова.\n\n"
            f"Примеры:\n"
            f"<code>socks5://user:pass@1.2.3.4:1080</code>\n"
            f"<code>1.2.3.4:1080:user:pass</code>"
        )
        return

    await state.update_data(proxy=parsed)
    await message.answer(
        f"{emoji('CHECK')} Прокси распознан:\n"
        f"<code>{parsed['proxy_type']}://"
        f"{parsed['host']}:{parsed['port']}</code>\n\n"
        f"Хотите добавить подпись для удобства? "
        f"Отправьте название (например, <i>DE-1</i>) "
        f"или '-' чтобы пропустить.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Пропустить",
                callback_data="skip_proxy_label",
                style='default'
            )
        ]])
    )
    await state.set_state(ProxyStates.waiting_for_label)


@dp.message(ProxyStates.waiting_for_label)
async def process_proxy_label(message: Message, state: FSMContext):
    label = None
    if message.text and message.text.strip() not in ('-', '—', '.'):
        label = message.text.strip()[:64]
    data = await state.get_data()
    parsed = data['proxy']

    proxy_id = await add_proxy(
        message.from_user.id, parsed['proxy_type'], parsed['host'],
        parsed['port'], parsed.get('username'),
        parsed.get('password'), label
    )

    await message.answer(
        f"{emoji('CHECK')} Прокси добавлен (id={proxy_id}).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="К списку прокси",
                callback_data="my_proxies",
                style='primary',
                icon_custom_emoji_id=get_icon("LINK")
            ),
            InlineKeyboardButton(
                text="В меню",
                callback_data="account_manager",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.clear()


@dp.callback_query(F.data == "skip_proxy_label")
async def skip_proxy_label(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    parsed = data['proxy']

    proxy_id = await add_proxy(
        callback.from_user.id, parsed['proxy_type'], parsed['host'],
        parsed['port'], parsed.get('username'),
        parsed.get('password'), None
    )

    await callback.message.edit_text(
        f"{emoji('CHECK')} Прокси добавлен (id={proxy_id}).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="К списку прокси",
                callback_data="my_proxies",
                style='primary',
                icon_custom_emoji_id=get_icon("LINK")
            ),
            InlineKeyboardButton(
                text="В меню",
                callback_data="account_manager",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await state.clear()
    await callback.answer()


@dp.callback_query(F.data.startswith("manage_proxy_"))
async def manage_proxy(callback: CallbackQuery):
    proxy_id = int(callback.data.split("_")[2])
    proxy = await get_proxy(proxy_id)

    if not proxy or proxy['user_id'] != callback.from_user.id:
        await callback.answer("Прокси не найден", show_alert=True)
        return

    auth = (
        f"👤 Логин: <code>{escape(proxy['username'])}</code>\n"
        f"🔑 Пароль: <code>{'•' * len(proxy['password'])}</code>\n"
        if proxy.get('username') else ""
    )
    label = proxy.get('label') or '—'

    text = (
        f"{emoji('LINK')} <b>Прокси</b>\n\n"
        f"🏷 Подпись: {escape(label)}\n"
        f"📡 Тип: <code>{proxy['proxy_type']}</code>\n"
        f"🌐 Адрес: <code>{proxy['host']}:{proxy['port']}</code>\n"
        f"{auth}"
        f"📅 Создан: "
        f"{proxy['created_at'].strftime('%d.%m.%Y %H:%M')}"
    )

    await callback.message.edit_text(
        text, reply_markup=get_proxy_actions_keyboard(proxy_id)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("delete_proxy_"))
async def delete_proxy_handler(callback: CallbackQuery):
    proxy_id = int(callback.data.split("_")[2])
    ok = await delete_proxy(proxy_id, callback.from_user.id)
    if ok:
        await callback.answer("Прокси удалён", show_alert=True)
    else:
        await callback.answer("Не удалось удалить", show_alert=True)

    proxies = await get_user_proxies(callback.from_user.id)
    text = (
        f"{emoji('LINK')} <b>Ваши прокси ({len(proxies)}):</b>"
        if proxies else
        f"{emoji('LINK')} Прокси удалены. Добавьте новые при необходимости."
    )
    await callback.message.edit_text(
        text, reply_markup=get_proxies_keyboard(proxies)
    )


# Привязка прокси к аккаунту
@dp.callback_query(F.data.startswith("set_account_proxy_"))
async def set_account_proxy_start(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[3])
    account = await get_account(account_id)

    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return

    proxies = await get_user_proxies(callback.from_user.id)
    if not proxies:
        await callback.answer(
            "У вас нет прокси. Сначала добавьте хотя бы один.",
            show_alert=True
        )
        return

    builder = InlineKeyboardBuilder()
    for p in proxies:
        label = p.get('label') or f"{p['host']}:{p['port']}"
        builder.row(InlineKeyboardButton(
            text=f"{p['proxy_type']} | {label}",
            callback_data=f"do_set_proxy_{account_id}_{p['id']}",
            style='default'
        ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data=f"manage_account_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))

    await callback.message.edit_text(
        f"{emoji('LINK')} <b>Выберите прокси для аккаунта "
        f"<code>{account['phone']}</code>:</b>",
        reply_markup=builder.as_markup()
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("do_set_proxy_"))
async def do_set_proxy(callback: CallbackQuery):
    # do_set_proxy_{account_id}_{proxy_id}
    parts = callback.data.split("_")
    # parts: ['do', 'set', 'proxy', account_id, proxy_id]
    account_id = int(parts[3])
    proxy_id = int(parts[4])

    ok = await set_account_proxy(
        account_id, callback.from_user.id, proxy_id
    )
    if ok:
        # Сбрасываем кеш клиента, чтобы при следующем подключении применился
        # новый прокси
        active_clients.pop(account_id, None)
        await callback.answer(
            "✅ Прокси привязан. При следующем подключении вступит в силу.",
            show_alert=True
        )
    else:
        await callback.answer(
            "Не удалось привязать (чужая запись?)", show_alert=True
        )
    # Возвращаемся к карточке аккаунта
    account = await get_account(account_id)
    if not account:
        return
    # перерисуем карточку
    callback.data = f"manage_account_{account_id}"
    await manage_account(callback)


@dp.callback_query(F.data.startswith("unset_account_proxy_"))
async def unset_account_proxy(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[3])

    ok = await set_account_proxy(
        account_id, callback.from_user.id, None
    )
    if ok:
        active_clients.pop(account_id, None)
        await callback.answer("Прокси отвязан", show_alert=True)
    else:
        await callback.answer("Не удалось", show_alert=True)

    # перерисуем карточку
    account = await get_account(account_id)
    if not account:
        return
    callback.data = f"manage_account_{account_id}"
    await manage_account(callback)


# ========== /ПРОКСИ ==========

# --- Запуск бота ---
async def on_startup():
    os.makedirs("media", exist_ok=True)
    await init_db()
    
    async with db_pool.acquire() as conn:
        responders = await conn.fetch(
            "SELECT * FROM auto_responders WHERE is_active = TRUE"
        )
        for responder in responders:
            responder = dict(responder)
            await start_auto_responder(
                responder['id'], responder['user_id']
            )
    
    asyncio.create_task(check_scheduled_broadcasts())
    asyncio.create_task(task_queue_worker())

async def main():
    await on_startup()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
