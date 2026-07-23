import asyncio
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from html import escape

import aiohttp
import asyncpg
import pytz
import anthropic
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
from telethon.tl.functions.account import (
    UpdateStatusRequest, GetPrivacyRequest
)
from telethon.tl.functions.messages import (
    ReadHistoryRequest, ReadReactionsRequest, GetDialogsRequest,
    GetHistoryRequest, GetMessagesViewsRequest,
    SetTypingRequest
)
from telethon.tl.functions.stories import (
    GetAllStoriesRequest, ReadStoriesRequest
)
from telethon.tl.types import (
    Dialog, PeerChat, PeerUser, PeerChannel, InputPeerUser,
    InputPeerChannel, InputPeerChat, Message
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

# --- LLM (AI-генератор текста) ---
# Официальный Anthropic Python SDK, направленный на Anthropic-совместимый
# прокси SmartAPI (https://api.smartapi.shop). Клиент ходит в
# {base_url}/v1/messages — формат Anthropic Messages API.
LLM_BASE_URL = os.getenv('LLM_BASE_URL') or 'https://api.smartapi.shop'
# Токен SmartAPI. Можно переопределить через переменную окружения LLM_API_KEY,
# иначе используется значение по умолчанию ниже.
LLM_API_KEY = (
    os.getenv('LLM_API_KEY')
    or 'sk-smart-3XD55m5XyNjpez1edNzGkuaqvnnXs6qKm1pf5hQqHEA'
)
# Выбранная модель: Sonnet 4.6 (Anthropic Claude) — лучшее качество
# для копирайтерских задач в связке с SmartAPI-прокси. Если потребуется
# подключить другие модели (mimo-v2.5, deepseek-v4-flash, minimax-m3),
# нужно сначала убедиться, что SmartAPI их реально отдаёт.
LLM_MODEL = os.getenv('LLM_MODEL') or 'sonnet-4.6'
LLM_TIMEOUT = int(os.getenv('LLM_TIMEOUT') or '120')
LLM_MAX_TOKENS = int(os.getenv('LLM_MAX_TOKENS') or '4096')
LLM_THINKING = (os.getenv('LLM_THINKING') or 'false').lower() in ('1', 'true', 'yes')

# Доступные пользователю модели (key -> человекочитаемое имя).
# Какую бы модель пользователь ни выбрал — реальный запрос уйдёт через
# SmartAPI-прокси. Если выбранная модель не поддерживается прокси,
# LLM вернёт ошибку и пользователь увидит уведомление.
LLM_MODELS = {
    'minimax-m3':       'MiniMax M3',
    'mimo-v2.5':        'MiMo v2.5',
    'deepseek-v4-flash':'DeepSeek V4 Flash',
    'sonnet-4.6':       'Sonnet 4.6',
}
LLM_DEFAULT_MODEL = 'sonnet-4.6'

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

# --- Прогрев аккаунтов ---
# Воркер прогрева на каждый аккаунт: имитирует живого пользователя,
# чтобы Telegram не триггерил антиспам. Делает чтение диалогов,
# просмотр сторис, лёгкие реакции, иногда пишет в Избранное и т.п.
warming_tasks: Dict[int, asyncio.Task] = {}
warming_stop_flags: Dict[int, bool] = {}

# Настройки по умолчанию (можно расширить в get_account)
WARMING_DEFAULT_COOLDOWN_MIN = 5 * 60     # 5 минут между волнами активности
WARMING_DEFAULT_COOLDOWN_MAX = 18 * 60    # 18 минут — потолок
WARMING_ACTIONS_PER_CYCLE_MIN = 2
WARMING_ACTIONS_PER_CYCLE_MAX = 4

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
    "AI": ("🧠", "6030400221232501136"),
    "SPARK": ("✨", "5870753782874246579"),
    "COPY": ("📋", "5769289093221454192"),
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

class LLMStates(StatesGroup):
    choosing_model = State()      # выбор модели перед вводом промта
    waiting_for_prompt = State()  # ждём текст задачи
    choosing_variant = State()    # показаны 3 варианта, ждём выбор/реген

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

        # История AI-запросов (LLM)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS ai_requests (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                prompt TEXT NOT NULL,
                model TEXT NOT NULL,
                variants JSONB NOT NULL,
                chosen_index INTEGER,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        try:
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_ai_requests_user_created '
                'ON ai_requests (user_id, created_at DESC)'
            )
        except Exception:
            pass
        
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
        # Настройки прогрева: мин/макс задержка в секундах (NULL = дефолт).
        try:
            await conn.execute(
                'ALTER TABLE accounts ADD COLUMN IF NOT EXISTS warming_min_cooldown INTEGER'
            )
        except:
            pass
        try:
            await conn.execute(
                'ALTER TABLE accounts ADD COLUMN IF NOT EXISTS warming_max_cooldown INTEGER'
            )
        except:
            pass
        # Статистика прогрева: сколько циклов отработано, последняя активность.
        try:
            await conn.execute(
                'ALTER TABLE accounts ADD COLUMN IF NOT EXISTS warming_cycles INTEGER DEFAULT 0'
            )
        except:
            pass
        try:
            await conn.execute(
                'ALTER TABLE accounts ADD COLUMN IF NOT EXISTS warming_last_active TIMESTAMP'
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
        # Пользовательская настройка LLM-модели.
        try:
            await conn.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS llm_model TEXT DEFAULT 'sonnet-4.6'"
            )
        except:
            pass

        # История FloodWait для Smart Delay Engine.
        # Хранит последние N флуд-вейтов на аккаунт, чтобы
        # адаптивно увеличивать задержку.
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS flood_wait_history (
                id BIGSERIAL PRIMARY KEY,
                account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
                chat_id BIGINT,
                seconds INTEGER NOT NULL,
                occurred_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        try:
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_flood_wait_account_time '
                'ON flood_wait_history (account_id, occurred_at DESC)'
            )
        except Exception:
            pass

        # Планы прогрева, сгенерированные LLM.
        # Один аккаунт может иметь несколько планов в истории,
        # но только один активный (is_active = TRUE).
        # plan — JSONB-структура, narrative — краткое описание
        # стратегии на человеческом языке (показывается юзеру).
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS warming_plans (
                id BIGSERIAL PRIMARY KEY,
                account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
                plan JSONB NOT NULL,
                narrative TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW(),
                started_at TIMESTAMP,
                finished_at TIMESTAMP
            )
        ''')
        try:
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_warming_plans_account_active '
                'ON warming_plans (account_id, is_active)'
            )
        except Exception:
            pass

        # Подписки пользователей (Free / Pro).
        # Активируются после успешной оплаты через Crypto Pay.
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
                tier TEXT NOT NULL DEFAULT 'free',
                expires_at TIMESTAMP,
                last_invoice_id BIGINT,
                last_invoice_payload TEXT,
                updated_at TIMESTAMP DEFAULT NOW(),
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        try:
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_subscriptions_expires '
                'ON subscriptions (expires_at)'
            )
        except Exception:
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
            COALESCE(warming_enabled, FALSE) as warming_enabled,
            COALESCE(warming_cycles, 0) as warming_cycles,
            warming_last_active
            FROM accounts WHERE user_id = $1''',
            user_id
        )
        return [dict(row) for row in rows]


async def get_user_llm_model(user_id: int) -> str:
    """Возвращает выбранную пользователем LLM-модель или дефолт."""
    if db_pool is None:
        return LLM_DEFAULT_MODEL
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT llm_model FROM users WHERE user_id = $1',
                user_id,
            )
        if row and row['llm_model'] in LLM_MODELS:
            return row['llm_model']
    except Exception as e:
        logger.warning('get_user_llm_model fallback: %s', e)
    return LLM_DEFAULT_MODEL


async def set_user_llm_model(user_id: int, model: str) -> None:
    """Сохраняет выбор модели за пользователем. Неизвестную модель игнорирует."""
    if model not in LLM_MODELS:
        raise ValueError(f'Unknown model: {model}')
    if db_pool is None:
        return
    async with db_pool.acquire() as conn:
        # upsert: создаём запись users, если её ещё нет.
        await conn.execute(
            'INSERT INTO users (user_id, llm_model) VALUES ($1, $2) '
            'ON CONFLICT (user_id) DO UPDATE SET llm_model = EXCLUDED.llm_model',
            user_id, model,
        )

async def get_account(account_id: int) -> Optional[Dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            '''SELECT *, COALESCE(warming_enabled, FALSE) as warming_enabled,
            COALESCE(warming_cycles, 0) as warming_cycles
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


# ========== ПЛАНЫ ПРОГРЕВА (LLM) ==========
# План — это JSONB, сгенерированный LLM на WARMING_PLAN_SYSTEM_PROMPT.
# Структура плана (поля, которые ожидает воркер):
#   duration_hours      — окно прогрева (по умолчанию 12)
#   total_cycles        — оценочное число волн
#   intervals_min_sec   — мин. пауза между волнами (сек)
#   intervals_max_sec   — макс. пауза между волнами (сек)
#   distribution        — dict {action_kind: вес 0..1}
#   saved_notes         — list[str] (8-12 коротких текстов в Избранное)
#   reaction_pool       — list[str] (эмодзи)
#   schedule            — list[dict] (часовая разбивка интенсивности)
#   quiet_periods       — list[str] вида "HH:MM-HH:MM" в МСК
#   narrative           — str (короткое описание стратегии)


async def deactivate_warming_plans(account_id: int) -> None:
    """Снимает флаг is_active со всех планов аккаунта."""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE warming_plans "
                "SET is_active = FALSE, finished_at = NOW() "
                "WHERE account_id = $1 AND is_active = TRUE",
                account_id
            )
    except Exception as ex:
        logger.warning(f"deactivate_warming_plans failed: {ex}")


async def save_warming_plan(
    account_id: int, plan: dict, narrative: str = ""
) -> int:
    """Сохраняет новый план в БД, деактивируя предыдущие активные."""
    await deactivate_warming_plans(account_id)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            '''INSERT INTO warming_plans
            (account_id, plan, narrative, is_active)
            VALUES ($1, $2::jsonb, $3, TRUE)
            RETURNING id''',
            account_id, json.dumps(plan, ensure_ascii=False), narrative
        )
        return int(row['id'])


async def get_active_warming_plan(account_id: int) -> Optional[dict]:
    """Возвращает активный план аккаунта (с распарсенным JSON) или None."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            '''SELECT id, plan, narrative, created_at, started_at
            FROM warming_plans
            WHERE account_id = $1 AND is_active = TRUE
            ORDER BY id DESC LIMIT 1''',
            account_id
        )
    if not row:
        return None
    plan = row['plan']
    # asyncpg отдаёт JSONB как dict, но на всякий случай — парсим строку.
    if isinstance(plan, str):
        try:
            plan = json.loads(plan)
        except Exception:
            plan = {}
    return {
        'id': int(row['id']),
        'plan': plan or {},
        'narrative': row['narrative'] or '',
        'created_at': row['created_at'],
        'started_at': row['started_at'],
    }


async def get_latest_warming_plan(account_id: int) -> Optional[dict]:
    """Последний план аккаунта (включая неактивные) — для предпросмотра."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            '''SELECT id, plan, narrative, is_active,
            created_at, started_at, finished_at
            FROM warming_plans
            WHERE account_id = $1
            ORDER BY id DESC LIMIT 1''',
            account_id
        )
    if not row:
        return None
    plan = row['plan']
    if isinstance(plan, str):
        try:
            plan = json.loads(plan)
        except Exception:
            plan = {}
    return {
        'id': int(row['id']),
        'plan': plan or {},
        'narrative': row['narrative'] or '',
        'is_active': bool(row['is_active']),
        'created_at': row['created_at'],
        'started_at': row['started_at'],
        'finished_at': row['finished_at'],
    }


async def mark_warming_plan_started(plan_id: int) -> None:
    """Ставит started_at = NOW() при запуске воркера."""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE warming_plans "
                "SET started_at = NOW() WHERE id = $1",
                plan_id
            )
    except Exception as ex:
        logger.debug(f"mark_warming_plan_started failed: {ex}")


def _safe_plan_defaults(base: dict) -> dict:
    """Подмешивает безопасные дефолты, если LLM что-то не вернула."""
    p = dict(base or {})
    p.setdefault('duration_hours', 12)
    p.setdefault('total_cycles', 24)
    p.setdefault('intervals_min_sec', 5 * 60)
    p.setdefault('intervals_max_sec', 18 * 60)
    # Нормируем интервалы в безопасный диапазон 300..1800 сек.
    try:
        imin = int(p['intervals_min_sec'])
        imax = int(p['intervals_max_sec'])
    except (TypeError, ValueError):
        imin, imax = 5 * 60, 18 * 60
    imin = max(300, min(imin, 1700))
    imax = max(imin + 60, min(imax, 1800))
    p['intervals_min_sec'] = imin
    p['intervals_max_sec'] = imax

    p.setdefault('distribution', {
        'read_dialogs': 0.35,
        'view_stories': 0.25,
        'react': 0.18,
        'saved_note': 0.12,
        'typing': 0.07,
        'status_toggle': 0.03,
    })
    p.setdefault('saved_notes', list(WARMING_SAVED_NOTES))
    if not isinstance(p['saved_notes'], list) or not p['saved_notes']:
        p['saved_notes'] = list(WARMING_SAVED_NOTES)
    p.setdefault('reaction_pool', list(WARMING_REACTIONS))
    if not isinstance(p['reaction_pool'], list) or not p['reaction_pool']:
        p['reaction_pool'] = list(WARMING_REACTIONS)
    p.setdefault('schedule', [])
    p.setdefault('quiet_periods', ['00:00-07:00'])
    p.setdefault('narrative', 'План прогрева без подробного описания.')
    return p


async def generate_warming_plan_llm(
    account: dict, user_id: int, duration_hours: int = 12
) -> dict:
    """Генерирует план прогрева через LLM. Возвращает dict с полями
    {plan, narrative, raw, elapsed_sec}.

    Время генерации замеряется явно — именно его показываем юзеру
    в статусе «Думаю...».
    """
    model = LLM_DEFAULT_MODEL
    try:
        if user_id is not None:
            model = await get_user_llm_model(user_id)
    except Exception:
        pass

    phone = account.get('phone') or '—'
    proxy_id = account.get('proxy_id')
    has_proxy = bool(proxy_id)
    cycles = account.get('warming_cycles') or 0

    # Контекстный промпт с конкретикой по аккаунту.
    user_prompt = (
        f"Аккаунт: {phone}\n"
        f"Прокси: {'есть' if has_proxy else 'нет'}\n"
        f"Пройдено циклов прогрева ранее: {cycles}\n"
        f"Окно прогрева: {duration_hours} часов.\n"
        f"Текущее время (МСК): {datetime.now(MSK_TZ).strftime('%H:%M')}, "
        f"день недели: {datetime.now(MSK_TZ).strftime('%A')}.\n\n"
        f"Сгенерируй план прогрева на {duration_hours} часов. "
        f"Учти время суток: ночью активность минимальна, утром "
        f"нарастает, днём полная, вечером осторожная. "
        f"Все интервалы и интенсивности — плавные, без резких пиков. "
        f"Только JSON по описанной выше схеме."
    )

    started = time.monotonic()
    client = anthropic.AsyncAnthropic(
        api_key=LLM_API_KEY,
        base_url=LLM_BASE_URL,
        timeout=LLM_TIMEOUT,
    )
    response = await client.messages.create(
        model=model,
        max_tokens=LLM_MAX_TOKENS,
        system=WARMING_PLAN_SYSTEM_PROMPT,
        messages=[{'role': 'user', 'content': user_prompt}],
    )
    elapsed = time.monotonic() - started

    # Достаём текстовый блок.
    content = ''
    try:
        for block in (response.content or []):
            if getattr(block, 'type', None) == 'text':
                content = getattr(block, 'text', '') or content
    except Exception:
        content = ''

    plan = _parse_warming_plan_json(content)
    plan = _safe_plan_defaults(plan)

    # narrative — из ответа LLM, иначе генерим короткий по distribution.
    narrative = (plan.get('narrative') or '').strip()
    if not narrative:
        d = plan.get('distribution', {}) or {}
        narrative = (
            f"План на {plan['duration_hours']} ч. ~{plan['total_cycles']} волн, "
            f"паузы {plan['intervals_min_sec']//60}–"
            f"{plan['intervals_max_sec']//60} мин. "
            f"Фокус: чтение {(int(d.get('read_dialogs', 0)*100))}%, "
            f"сторис {(int(d.get('view_stories', 0)*100))}%, "
            f"реакции {(int(d.get('react', 0)*100))}%."
        )

    return {
        'plan': plan,
        'narrative': narrative,
        'raw': content,
        'elapsed_sec': elapsed,
    }


def _parse_warming_plan_json(content: str) -> dict:
    """Достаём JSON плана из ответа модели. Терпимо к лишнему тексту."""
    if not content:
        return {}
    text = content.strip()
    # ```json ... ```
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fence:
        text = fence.group(1)
    if not text.startswith("{"):
        first = text.find("{")
        last = text.rfind("}")
        if first != -1 and last != -1 and last > first:
            text = text[first:last + 1]
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}


def _format_warming_plan_message(plan: dict, narrative: str) -> str:
    """Готовим красивое текстовое представление плана для юзера."""
    p = _safe_plan_defaults(plan)
    d = p.get('distribution', {}) or {}
    duration = p.get('duration_hours', 12)
    total = p.get('total_cycles', '—')
    imin = int(p.get('intervals_min_sec', 300)) // 60
    imax = int(p.get('intervals_max_sec', 1800)) // 60
    notes = p.get('saved_notes', []) or []
    reactions = p.get('reaction_pool', []) or []
    quiet = p.get('quiet_periods', []) or []
    schedule = p.get('schedule', []) or []

    def pct(x):
        try:
            return int(round(float(x) * 100))
        except Exception:
            return 0

    lines = [
        f"{emoji('FIRE')} <b>План прогрева готов</b>\n",
        f"{emoji('CLOCK')} Окно: <b>{duration} ч</b> · "
        f"Волн: <b>~{total}</b> · "
        f"Паузы: <b>{imin}–{imax} мин</b>\n",
        f"{emoji('CHART')} <b>Распределение действий:</b>\n"
        f" • Чтение диалогов — <b>{pct(d.get('read_dialogs', 0))}%</b>\n"
        f" • Сторис — <b>{pct(d.get('view_stories', 0))}%</b>\n"
        f" • Реакции — <b>{pct(d.get('react', 0))}%</b>\n"
        f" • Заметки в Избранном — <b>{pct(d.get('saved_note', 0))}%</b>\n"
        f" • «Печатает...» — <b>{pct(d.get('typing', 0))}%</b>\n"
        f" • Смена статуса — <b>{pct(d.get('status_toggle', 0))}%</b>\n",
    ]

    if reactions:
        lines.append(
            f"{emoji('LIKE')} <b>Пул реакций:</b> "
            f"{' '.join(str(x) for x in reactions[:6])}\n"
        )

    if quiet:
        quiet_str = ', '.join(str(x) for x in quiet)
        lines.append(
            f"{emoji('MOON')} <b>Тихие часы (МСК):</b> {quiet_str}\n"
        )

    if notes:
        sample = notes[:3]
        lines.append(
            f"{emoji('NOTE')} <b>Заметки в Избранном "
            f"(примеры, всего {len(notes)}):</b>\n"
            + ''.join(
                f" • <i>{escape(str(n)[:60])}</i>\n" for n in sample
            )
        )

    if schedule:
        lines.append(
            f"{emoji('STATS')} <b>Расписание по часам "
            f"(всего {len(schedule)} фаз):</b>\n"
        )
        for s in schedule[:6]:
            try:
                ho = int(s.get('hour_offset', 0))
                inten = str(s.get('intensity', '—'))
                focus = str(s.get('focus', ''))
                amin = int(s.get('actions_count_min', 1))
                amax = int(s.get('actions_count_max', 2))
            except Exception:
                continue
            lines.append(
                f" • <code>+{ho}ч</code> · {inten} · {amin}–{amax} д. "
                f"— {escape(focus)}\n"
            )
        if len(schedule) > 6:
            lines.append(f" • <i>... и ещё {len(schedule) - 6} фаз</i>\n")

    if narrative:
        lines.append(
            f"\n{emoji('BRAIN')} <b>Стратегия:</b>\n"
            f"<i>{escape(narrative[:700])}</i>"
        )

    return ''.join(lines)


def _warming_plan_keyboard(plan_id: int, account_id: int) -> InlineKeyboardMarkup:
    """Кнопки после генерации плана: запустить / перегенерировать / отмена."""
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Запустить прогрев",
        callback_data=f"confirm_warming_{account_id}",
        style='success',
        icon_custom_emoji_id=get_icon("PLAY")
    ))
    builder.row(
        InlineKeyboardButton(
            text="Перегенерировать",
            callback_data=f"regen_warming_{account_id}",
            style='primary',
            icon_custom_emoji_id=get_icon("REFRESH")
        ),
        InlineKeyboardButton(
            text="Отмена",
            callback_data=f"manage_account_{account_id}",
            style='default',
            icon_custom_emoji_id=get_icon("BACK")
        )
    )
    return builder.as_markup()


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
    text: str, media_paths: List[str] = None,
    smart_delay_enabled: bool = True
):
    try:
        chat_id_int = int(chat_id) if chat_id.lstrip('-').isdigit() else chat_id

        # Smart Delay Engine: адаптивная задержка перед отправкой.
        # Снижает риск бана за счёт:
        #   - времени суток (МСК)
        #   - частоты аккаунта в этом чате
        #   - истории флуд-вейтов
        if smart_delay_enabled:
            try:
                delay = await smart_delay(account_id, str(chat_id_int))
                if delay > 0:
                    await asyncio.sleep(delay)
            except Exception as ex:
                logger.warning(f"smart_delay pre-send failed: {ex}")

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
    except FloodWaitError as ex:
        # Записываем флуд-вейт в историю, чтобы Smart Delay усилил
        # задержку на ближайшие сообщения.
        try:
            chat_id_for_log = int(chat_id) if str(chat_id).lstrip('-').isdigit() else 0
            await record_flood_wait(account_id, chat_id_for_log, ex.seconds)
        except Exception:
            pass
        logger.warning(f"FloodWait in send_message_to_chat: {ex.seconds}s")
        return False
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


# --- LLM (AI-генератор текста) ---

# Базовый системный промпт, который всегда добавляется.
# Требуем от модели 3 разных варианта в формате JSON, чтобы их
# можно было гарантированно распарсить.
LLM_SYSTEM_PROMPT = (
    "Ты — копирайтер Telegram-бота Vest Game Soft.\n\n"
    "Что делать:\n"
    "1. Пользователь даёт тему и параметры (длина, тон, аудитория, "
    "площадка и т.п.). Твоя задача — сгенерировать РОВНО 3 разных "
    "варианта готового текста на русском.\n"
    "2. Варианты должны заметно отличаться по стилю/тону/подаче — "
    "например: дружелюбный, деловой, продающий.\n"
    "3. Пиши НЕБОЛЬШОЙ текст, подбирай длину под параметры пользователя. "
    "Никаких огромных полотен. Если пользователь не задал длину — "
    "ориентируйся на 2–5 предложений, до 600 символов.\n"
    "4. Активно используй эмодзи (1–3 на короткий текст, 3–6 на длинный), "
    "если тема это уместна. Не лепи эмодзи в код или там, где они мешают.\n"
    "5. Строго следуй параметрам пользователя: тема, длина, канал, "
    "аудитория, цель. Не выходи за рамки.\n\n"
    "Формат ответа — СТРОГО JSON, без markdown-обёрток, без пояснений, "
    "без префиксов вроде \"Вот варианты:\":\n"
    "{\n"
    '  "variants": [\n'
    '    {"title": "короткий заголовок 1", "text": "текст 1"},\n'
    '    {"title": "короткий заголовок 2", "text": "текст 2"},\n'
    '    {"title": "короткий заголовок 3", "text": "текст 3"}\n'
    "  ]\n"
    "}\n"
    "Только JSON."
)


# --- LLM: системный промпт для генерации плана прогрева ---
# Генерирует ПОЛНЫЙ план на заданное окно (по умолчанию 12 часов):
#  - интервалы между волнами с учётом времени суток
#  - распределение типов действий
#  - почасовое расписание интенсивности
#  - набор уникальных текстов для Избранного
#  - пул безопасных реакций
#  - тихие часы
# Возвращает СТРОГО JSON без markdown-обёрток.
WARMING_PLAN_SYSTEM_PROMPT = """Ты — эксперт по безопасному прогреву Telegram-аккаунтов.
Твоя задача — составить ДЕТАЛЬНЫЙ ПЛАН прогрева на заданное окно часов (по умолчанию 12). Цель — сделать аккаунт «живым» в глазах Telegram, избегая FloodWait.

Что НЕЛЬЗЯ планировать:
  • массовые рассылки, инвайты, спам
  • резкие пики активности (все волны — плавные)
  • сообщения в чужие чаты (только self-PM, реакции, чтение, просмотр сторис)

Доступные типы действий (action_kind) и их смысл:
  - read_dialogs  : пометить 1-3 диалога прочитанными
  - view_stories  : посмотреть 1-2 сторис у контактов
  - react         : поставить лёгкую реакцию на 1 свежее сообщение
  - saved_note    : отправить короткую заметку в Избранное (self-PM)
  - typing        : подёргать «печатает...» в случайном диалоге 2-4 сек
  - status_toggle : сменить online/offline (использовать редко)

Правила генерации:
  1. Интервалы между волнами (intervals) — В СЕКУНДАХ, в диапазоне 300..1800 (5..30 минут). Ночью интервалы длиннее, днём короче.
  2. distribution — сумма вероятностей примерно 1.0. Безопасные действия (read, view_stories) имеют больший вес.
  3. saved_notes — МАССИВ из 8-12 КОРОТКИХ текстов на русском (как будто человек пишет самому себе). Каждый до 80 символов. БЕЗ спама, БЕЗ рекламы. Разнообразные: напоминалки, мысли, короткие заметки.
  4. reaction_pool — 4-6 эмодзи из безопасного набора: «👍», «🔥», «❤️», «😂», «😢», «🙏».
  5. schedule — массив объектов {hour_offset, intensity, focus, actions_count_min, actions_count_max}. intensity ∈ {low, medium, high}. focus — короткая подсказка что делать (например «active_dialogs», «stories_only», «rest»).
  6. quiet_periods — массив строк вида «HH:MM-HH:MM» в МСК, когда активность минимальна (например ночь 00:00-07:00). Если время сейчас попадает в quiet_period — бот должен уйти в длинный сон.
  7. narrative — 2-3 предложения на русском, КРАТКОЕ описание стратегии плана (человеческим языком, без JSON). Будет показано пользователю в карточке плана.
  8. total_cycles — оценочное число волн за всё окно.

Формат ответа — СТРОГО JSON, без markdown, без пояснений, без префиксов. Только валидный JSON (пример структуры):
{
  "duration_hours": 12,
  "total_cycles": 24,
  "intervals_min_sec": 480,
  "intervals_max_sec": 1200,
  "distribution": {
    "read_dialogs": 0.35,
    "view_stories": 0.25,
    "react": 0.18,
    "saved_note": 0.12,
    "typing": 0.07,
    "status_toggle": 0.03
  },
  "saved_notes": [
    "Напоминалка самому себе",
    "Записать мысль, чтобы не затерялась"
  ],
  "reaction_pool": ["👍", "🔥", "❤️", "😂", "🙏"],
  "schedule": [
    {"hour_offset": 0, "intensity": "low", "focus": "rest", "actions_count_min": 1, "actions_count_max": 2},
    {"hour_offset": 8, "intensity": "high", "focus": "active_dialogs", "actions_count_min": 3, "actions_count_max": 5}
  ],
  "quiet_periods": ["00:00-07:00"],
  "narrative": "Краткое описание стратегии плана на русском."
}

Только JSON. Никаких пояснений вокруг.
"""


# --- LLM: системный промпт для анализа риска бана аккаунта ---
# Используется отдельной фичей «Анализ логов аккаунта (оценка риска бана)».
# В отличие от копирайтерского промта — здесь модель возвращает связный
# текст на русском, а не JSON-варианты.
LLM_SECURITY_SYSTEM_PROMPT = (
    "Ты — эксперт по безопасности Telegram-аккаунтов и антиспам-системам.\n"
    "Тебе дают историю действий аккаунта (логи + статистика флуд-вейтов).\n"
    "Твоя задача — оценить риск блокировки аккаунта и дать конкретные советы.\n\n"
    "Что обязательно проанализировать:\n"
    "1) Частота отправки сообщений: пики, равномерность, средний интервал.\n"
    "2) FloodWait-ошибки: общее число, суммарные секунды, серии за час/сутки.\n"
    "3) Время суток активности (по МСК): ночные отправки, ночные флуды.\n"
    "4) Разнообразие действий: только отправка или есть чтение/реакции/вступления.\n"
    "5) Широта охвата: сколько разных чатов за период.\n\n"
    "Формат ответа — связный текст на русском, БЕЗ markdown-обёрток, "
    "БЕЗ JSON. Структура:\n"
    "  • УРОВЕНЬ РИСКА: одно слово из трёх — НИЗКИЙ / СРЕДНИЙ / ВЫСОКИЙ.\n"
    "  • 1-2 предложения обоснования (главная причина такого уровня).\n"
    "  • Причины: 2-5 коротких пунктов с конкретными цифрами из логов.\n"
    "  • Советы: 3-5 конкретных действий (например, «увеличить задержку до "
    "30-60 сек», «сменить прокси», «не слать ночью 00:00-07:00 МСК», "
    "«уменьшить число активных чатов до 5-7»).\n"
    "Тон — спокойный, технический, без паники. Пиши по делу."
)


def _parse_llm_variants(content: str) -> List[Dict[str, str]]:
    """Достаём 3 варианта из ответа модели. Терпимо к лишнему тексту вокруг JSON."""
    if not content:
        return []

    # Пытаемся найти JSON-объект в тексте
    text = content.strip()

    # Если модель обернула в ```json ... ```
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fence:
        text = fence.group(1)

    # Иначе берём от первой { до последней }
    if not text.startswith("{"):
        first = text.find("{")
        last = text.rfind("}")
        if first != -1 and last != -1 and last > first:
            text = text[first:last + 1]

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return []

    raw_variants = data.get("variants") if isinstance(data, dict) else None
    if not isinstance(raw_variants, list):
        return []

    out: List[Dict[str, str]] = []
    for v in raw_variants[:3]:
        if not isinstance(v, dict):
            continue
        title = (v.get("title") or "").strip()
        body = (v.get("text") or "").strip()
        if not body:
            continue
        if not title:
            title = f"Вариант {len(out) + 1}"
        out.append({"title": title[:80], "text": body})
    return out


async def call_llm_api(
    user_prompt: str, user_id: int = None, model: str = None
) -> List[Dict[str, str]]:
    """Запрос к LLM через официальный Anthropic Python SDK.
    Используется кастомный base_url (SmartAPI-прокси), но формат
    запроса/ответа — нативный Anthropic Messages API.

    Модель берётся из явного аргумента `model`, иначе из настройки пользователя,
    иначе из глобального дефолта (LLM_DEFAULT_MODEL).
    Возвращает <=3 вариантов {'title','text'}.
    """
    if not model:
        if user_id is not None:
            model = await get_user_llm_model(user_id)
        else:
            model = LLM_DEFAULT_MODEL

    # Официальный SDK. base_url ведёт в SmartAPI, но формат — Anthropic:
    # SDK сам добавит /v1/messages и нужные заголовки (x-api-key, anthropic-version).
    client = anthropic.AsyncAnthropic(
        api_key=LLM_API_KEY,
        base_url=LLM_BASE_URL,
        timeout=LLM_TIMEOUT,
    )
    try:
        kwargs = dict(
            model=model,
            max_tokens=LLM_MAX_TOKENS,
            system=LLM_SYSTEM_PROMPT,
            messages=[
                {'role': 'user', 'content': user_prompt},
            ],
        )
        # SmartAPI-прокси поддерживает Anthropic thinking-блок через
        # отдельный параметр; пробрасываем только если включено.
        if LLM_THINKING:
            kwargs['thinking'] = {'type': 'enabled', 'budget_tokens': 1024}

        response = await client.messages.create(**kwargs)
    except anthropic.APIStatusError as e:
        logger.error("LLM API error %s: %s", e.status_code, str(e)[:500])
        raise RuntimeError(f"LLM API вернул статус {e.status_code}") from e
    except anthropic.APIError as e:
        logger.exception("LLM API anthropic error")
        raise RuntimeError(f"LLM API ошибка: {e}") from e

    # Anthropic Messages API: content — список блоков.
    # Текст лежит в блоке типа 'text'; reasoning/thinking — в 'thinking'.
    content = ''
    try:
        for block in (response.content or []):
            btype = getattr(block, 'type', None)
            if btype == 'text':
                content = getattr(block, 'text', '') or content
            elif btype == 'thinking' and not content:
                # Если текста нет — fallback на рассуждения
                content = getattr(block, 'thinking', '') or content
    except Exception:
        content = ''

    variants = _parse_llm_variants(content)
    if not variants:
        # Если модель неожиданно вернула не-JSON — отдадим как один вариант
        cleaned = (content or '').strip()
        if cleaned:
            variants = [{'title': 'Готовый текст', 'text': cleaned}]
    return variants


async def call_llm_api_plain(
    user_prompt: str,
    user_id: int = None,
    model: str = None,
    system_prompt: str = None,
    max_tokens: int = 1500,
) -> str:
    """Запрос к LLM, возвращающий сырой текст (без JSON-парсинга).

    Используется там, где ответ модели — это связный текст на русском
    (анализ риска, рекомендации и т.п.), а не 3 варианта копирайта.
    Логика выбора модели и клиента — как в call_llm_api.
    """
    if not model:
        if user_id is not None:
            model = await get_user_llm_model(user_id)
        else:
            model = LLM_DEFAULT_MODEL
    if not system_prompt:
        system_prompt = LLM_SECURITY_SYSTEM_PROMPT

    client = anthropic.AsyncAnthropic(
        api_key=LLM_API_KEY,
        base_url=LLM_BASE_URL,
        timeout=LLM_TIMEOUT,
    )
    try:
        kwargs = dict(
            model=model,
            max_tokens=max_tokens,
            system=system_prompt,
            messages=[
                {'role': 'user', 'content': user_prompt},
            ],
        )
        if LLM_THINKING:
            kwargs['thinking'] = {'type': 'enabled', 'budget_tokens': 1024}

        response = await client.messages.create(**kwargs)
    except anthropic.APIStatusError as e:
        logger.error("LLM API (plain) error %s: %s", e.status_code, str(e)[:500])
        raise RuntimeError(f"LLM API вернул статус {e.status_code}") from e
    except anthropic.APIError as e:
        logger.exception("LLM API (plain) anthropic error")
        raise RuntimeError(f"LLM API ошибка: {e}") from e

    # Собираем все text-блоки; если есть thinking — отдадим его как fallback
    # (на случай, если модель отдала только рассуждения).
    text_parts: List[str] = []
    thinking_parts: List[str] = []
    try:
        for block in (response.content or []):
            btype = getattr(block, 'type', None)
            if btype == 'text':
                t = (getattr(block, 'text', '') or '').strip()
                if t:
                    text_parts.append(t)
            elif btype == 'thinking':
                t = (getattr(block, 'thinking', '') or '').strip()
                if t:
                    thinking_parts.append(t)
    except Exception:
        pass

    if text_parts:
        return '\n\n'.join(text_parts).strip()
    if thinking_parts:
        return '\n\n'.join(thinking_parts).strip()
    return ''


# --- AI: история запросов (БД) ---

async def save_ai_request(
    user_id: int, prompt: str, variants: List[Dict[str, str]],
    model: str = None,
) -> int:
    """Сохраняет запрос и возвращает id записи."""
    if not model:
        model = await get_user_llm_model(user_id)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            'INSERT INTO ai_requests (user_id, prompt, model, variants) '
            'VALUES ($1, $2, $3, $4::jsonb) RETURNING id',
            user_id, prompt[:4000], model,
            json.dumps(variants, ensure_ascii=False),
        )
    return int(row['id'])


async def mark_ai_chosen(request_id: int, user_id: int, idx: int) -> bool:
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            'UPDATE ai_requests SET chosen_index = $1 '
            'WHERE id = $2 AND user_id = $3',
            idx, request_id, user_id,
        )
    # asyncpg returns 'UPDATE <n>'
    return result.endswith(' 1')


async def get_ai_requests(user_id: int, limit: int = 10) -> List[Dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT id, prompt, model, variants, chosen_index, created_at '
            'FROM ai_requests WHERE user_id = $1 '
            'ORDER BY created_at DESC LIMIT $2',
            user_id, limit,
        )
    return [dict(r) for r in rows]


async def get_ai_request(request_id: int, user_id: int) -> Optional[Dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT id, prompt, model, variants, chosen_index, created_at '
            'FROM ai_requests WHERE id = $1 AND user_id = $2',
            request_id, user_id,
        )
    return dict(row) if row else None


async def clear_ai_history(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            'DELETE FROM ai_requests WHERE user_id = $1', user_id
        )
    # 'DELETE <n>' -> взять число
    try:
        return int(result.split()[-1])
    except Exception:
        return 0


# ============================================================
#  Анализ логов аккаунта (оценка риска бана)
# ============================================================
# Отдельная фича: по последним логам + истории флуд-вейтов аккаунта
# формируем структурированный отчёт (уровень риска + причины + советы)
# через LLM в режиме «эксперт по безопасности Telegram».

def _format_log_line(log: Dict[str, Any]) -> str:
    """Одна строка лога для промта: время (МСК), направление, чат, превью."""
    created = log.get('created_at')
    if hasattr(created, 'astimezone'):
        try:
            time_str = created.astimezone(MSK_TZ).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            time_str = str(created)[:19]
    else:
        time_str = str(created)[:19]
    direction = (log.get('direction') or 'unknown')
    chat_name = (log.get('chat_name') or str(log.get('chat_id') or '?'))[:40]
    text_preview = (log.get('message_text') or '').replace('\n', ' ')[:60]
    line = f"[{time_str} МСК] {direction:>9} | chat={chat_name}"
    if text_preview:
        line += f" | text=\"{text_preview}\""
    return line


def _aggregate_log_stats(logs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Грубая эвристика поверх логов — для промта и fallback-отчёта."""
    stats: Dict[str, Any] = {
        'total': len(logs),
        'by_direction': {},
        'unique_chats': set(),
        'hour_buckets_msk': {},  # 0-23
        'sent_intervals_sec': [],  # дельты между отправками
        'time_span_hours': 0.0,
        'first_log': None,
        'last_log': None,
    }
    last_sent_at: Optional[datetime] = None
    for log in logs:
        direction = log.get('direction') or 'unknown'
        stats['by_direction'][direction] = stats['by_direction'].get(direction, 0) + 1
        chat = log.get('chat_name') or str(log.get('chat_id') or '')
        if chat:
            stats['unique_chats'].add(chat)
        created = log.get('created_at')
        if hasattr(created, 'astimezone'):
            try:
                msk_dt = created.astimezone(MSK_TZ)
            except Exception:
                msk_dt = None
        else:
            msk_dt = None
        if msk_dt is not None:
            hour = msk_dt.hour
            stats['hour_buckets_msk'][hour] = stats['hour_buckets_msk'].get(hour, 0) + 1
            if direction == 'sent':
                if last_sent_at is not None:
                    delta = (msk_dt - last_sent_at).total_seconds()
                    if 0 <= delta < 24 * 3600:
                        stats['sent_intervals_sec'].append(delta)
                last_sent_at = msk_dt
    if logs:
        first = logs[-1].get('created_at')  # logs: DESC
        last = logs[0].get('created_at')
        if hasattr(first, 'astimezone') and hasattr(last, 'astimezone'):
            try:
                stats['time_span_hours'] = max(
                    0.0, (last - first).total_seconds() / 3600.0
                )
            except Exception:
                pass
        stats['first_log'] = first
        stats['last_log'] = last
    # Сводные цифры по интервалам
    intervals = stats['sent_intervals_sec']
    if intervals:
        intervals_sorted = sorted(intervals)
        stats['sent_min_interval_sec'] = min(intervals)
        stats['sent_max_interval_sec'] = max(intervals)
        stats['sent_avg_interval_sec'] = sum(intervals) / len(intervals)
        # медиана
        mid = len(intervals_sorted) // 2
        if len(intervals_sorted) % 2:
            stats['sent_median_interval_sec'] = intervals_sorted[mid]
        else:
            stats['sent_median_interval_sec'] = (
                intervals_sorted[mid - 1] + intervals_sorted[mid]
            ) / 2
    else:
        stats['sent_min_interval_sec'] = None
        stats['sent_max_interval_sec'] = None
        stats['sent_avg_interval_sec'] = None
        stats['sent_median_interval_sec'] = None
    # Ночные часы (00-07 МСК)
    night_total = sum(
        stats['hour_buckets_msk'].get(h, 0)
        for h in range(0, 7)
    )
    stats['night_actions_msk'] = night_total
    return stats


async def get_account_flood_history_stats(
    account_id: int,
) -> Dict[str, Any]:
    """Сводка по flood_wait_history для промта (за час / сутки / 7 дней)."""
    out: Dict[str, Any] = {
        'last_1h_count': 0,
        'last_1h_seconds': 0,
        'last_24h_count': 0,
        'last_24h_seconds': 0,
        'last_7d_count': 0,
        'last_7d_seconds': 0,
        'max_wait_seconds_7d': 0,
    }
    if db_pool is None:
        return out
    try:
        async with db_pool.acquire() as conn:
            for window, key_count, key_secs, key_max in (
                ("INTERVAL '1 hour'", 'last_1h_count', 'last_1h_seconds', None),
                ("INTERVAL '24 hours'", 'last_24h_count', 'last_24h_seconds', None),
                ("INTERVAL '7 days'", 'last_7d_count', 'last_7d_seconds', 'max_wait_seconds_7d'),
            ):
                row = await conn.fetchrow(
                    "SELECT COUNT(*) AS cnt, "
                    "COALESCE(SUM(seconds), 0) AS secs, "
                    "COALESCE(MAX(seconds), 0) AS mx "
                    "FROM flood_wait_history "
                    "WHERE account_id = $1 AND occurred_at > NOW() - " + window,
                    account_id,
                )
                if row:
                    out[key_count] = int(row['cnt'] or 0)
                    out[key_secs] = int(row['secs'] or 0)
                    if key_max:
                        out[key_max] = int(row['mx'] or 0)
    except Exception as ex:
        logger.warning('get_account_flood_history_stats failed: %s', ex)
    return out


def _heuristic_risk_report(
    stats: Dict[str, Any],
    flood: Dict[str, Any],
) -> str:
    """Чисто локальный отчёт (без LLM) — на случай, если LLM недоступна.

    Делаем короткий связный текст на русском, чтобы пользователь всё равно
    получил пользу, а не ошибку."""
    score = 0  # 0..100, выше = хуже
    reasons: List[str] = []

    # Флуд-вейты — самый весомый сигнал.
    cnt1 = flood.get('last_1h_count', 0)
    cnt24 = flood.get('last_24h_count', 0)
    secs24 = flood.get('last_24h_seconds', 0)
    if cnt1 >= 3 or cnt24 >= 5 or secs24 >= 600:
        score += 50
        reasons.append(
            f"серия FloodWait: {cnt1} за час и {cnt24} за сутки, "
            f"суммарно {secs24} сек ожидания"
        )
    elif cnt1 >= 1 or cnt24 >= 1:
        score += 25
        reasons.append(
            f"есть FloodWait: {cnt1} за час, {cnt24} за сутки"
        )

    # Частота отправки.
    avg = stats.get('sent_avg_interval_sec')
    if avg is not None:
        if avg < 5:
            score += 30
            reasons.append(
                f"слишком частые отправки: средний интервал "
                f"{avg:.1f} сек между сообщениями"
            )
        elif avg < 15:
            score += 15
            reasons.append(
                f"частые отправки: средний интервал {avg:.1f} сек"
            )

    # Ночная активность.
    night = stats.get('night_actions_msk', 0)
    if night >= 5:
        score += 15
        reasons.append(
            f"ночная активность (00-07 МСК): {night} действий — "
            f"Telegram-антиспам в это время самый жёсткий"
        )

    # Однообразие действий.
    by_dir = stats.get('by_direction') or {}
    if by_dir.get('sent', 0) >= 20 and len(by_dir) <= 1:
        score += 10
        reasons.append(
            "однообразная активность: только отправка, "
            "нет чтения/реакций/вступлений"
        )

    # Ширина охвата.
    unique_chats = len(stats.get('unique_chats') or [])
    if stats.get('total', 0) >= 30 and unique_chats >= 25:
        score += 5
        reasons.append(
            f"очень широкий охват: {unique_chats} разных чатов "
            f"за {stats.get('total', 0)} действий"
        )

    if score >= 60:
        level = 'ВЫСОКИЙ'
    elif score >= 30:
        level = 'СРЕДНИЙ'
    else:
        level = 'НИЗКИЙ'

    if not reasons:
        reasons.append(
            "серьёзных сигналов не найдено: активность умеренная, "
            "флуд-вейтов нет"
        )

    advice_pool = [
        "увеличить задержку между сообщениями до 30-60 секунд",
        "сменить прокси или отключить его на время",
        "ограничить активность дневным окном 09:00-23:00 МСК",
        "добавить «живые» действия: чтение диалогов, реакции, сторис",
        "уменьшить число одновременно активных чатов до 5-7",
    ]
    # Берём первые 3 совета всегда; добавим 1-2 при высоком риске.
    advice = advice_pool[:3]
    if score >= 60:
        advice += advice_pool[3:]

    lines: List[str] = []
    lines.append(f"УРОВЕНЬ РИСКА: {level}")
    lines.append("")
    lines.append("Причины:")
    for r in reasons:
        lines.append(f"• {r}")
    lines.append("")
    lines.append("Советы:")
    for a in advice:
        lines.append(f"• {a}")
    return '\n'.join(lines)


async def build_security_prompt(
    account_id: int,
    logs: List[Dict[str, Any]],
) -> tuple:
    """Собирает user_prompt + пред-агрегированную статистику для анализа.

    Возвращает кортеж (user_prompt, stats_dict, flood_dict) — stats и flood
    пригодятся, если нужно будет сделать fallback-отчёт без LLM.
    """
    stats = _aggregate_log_stats(logs)
    flood = await get_account_flood_history_stats(account_id)
    interval_str = (
        f"{stats.get('sent_avg_interval_sec'):.1f}"
        if stats.get('sent_avg_interval_sec') is not None else '—'
    )
    median_str = (
        f"{stats.get('sent_median_interval_sec'):.1f}"
        if stats.get('sent_median_interval_sec') is not None else '—'
    )
    min_str = (
        f"{stats.get('sent_min_interval_sec'):.1f}"
        if stats.get('sent_min_interval_sec') is not None else '—'
    )
    max_str = (
        f"{stats.get('sent_max_interval_sec'):.1f}"
        if stats.get('sent_max_interval_sec') is not None else '—'
    )
    by_dir_str = ', '.join(
        f"{k}={v}" for k, v in (stats.get('by_direction') or {}).items()
    ) or '—'
    hours_sorted = sorted(
        (stats.get('hour_buckets_msk') or {}).items(),
        key=lambda x: x[0],
    )
    hours_str = ', '.join(
        f"{h:02d}:00={n}" for h, n in hours_sorted
    ) or '—'
    span = stats.get('time_span_hours', 0.0)
    log_lines = '\n'.join(_format_log_line(log) for log in logs) or '(логов нет)'

    user_prompt = (
        "Вот последние 50 логов аккаунта (время — МСК):\n"
        f"{log_lines}\n\n"
        "Сводная статистика:\n"
        f"• Всего действий: {stats.get('total', 0)}\n"
        f"• Распределение по типу: {by_dir_str}\n"
        f"• Уникальных чатов: {len(stats.get('unique_chats') or set())}\n"
        f"• Временной охват логов: ~{span:.1f} ч\n"
        f"• Интервалы между 'sent' (сек): "
        f"min={min_str}, avg={interval_str}, median={median_str}, max={max_str}\n"
        f"• Действия по часам МСК: {hours_str}\n"
        f"• Ночных действий (00-07 МСК): {stats.get('night_actions_msk', 0)}\n\n"
        "История FloodWait (по таблице flood_wait_history):\n"
        f"• За последний час: {flood.get('last_1h_count', 0)} шт., "
        f"{flood.get('last_1h_seconds', 0)} сек суммарно\n"
        f"• За последние 24 часа: {flood.get('last_24h_count', 0)} шт., "
        f"{flood.get('last_24h_seconds', 0)} сек суммарно\n"
        f"• За последние 7 дней: {flood.get('last_7d_count', 0)} шт., "
        f"суммарно {flood.get('last_7d_seconds', 0)} сек, "
        f"макс. один FloodWait = {flood.get('max_wait_seconds_7d', 0)} сек\n\n"
        "Оцени риск блокировки аккаунта. Учти частоту отправки, "
        "количество ошибок FloodWait, время суток, разнообразие действий. "
        "Выдай краткий отчёт: уровень риска (низкий/средний/высокий), "
        "причины, и конкретные советы по исправлению (например, увеличить "
        "задержки, сменить прокси, уменьшить число чатов). Ответ — связный "
        "текст на русском."
    )
    return user_prompt, stats, flood


async def analyze_account_logs_security(
    account_id: int,
    user_id: int,
) -> Dict[str, Any]:
    """Главная точка входа: тянет 50 логов + флуды, выдаёт отчёт.

    Возвращает dict:
      {
        'ok': bool,
        'text': str,           # связный отчёт на русском
        'source': 'llm' | 'heuristic',
        'stats': {...},
        'flood': {...},
        'error': Optional[str],
      }
    """
    logs = await get_account_logs(account_id, limit=50)
    user_prompt, stats, flood = await build_security_prompt(account_id, logs)
    result: Dict[str, Any] = {
        'ok': False,
        'text': '',
        'source': 'heuristic',
        'stats': stats,
        'flood': flood,
        'error': None,
    }
    try:
        text = await call_llm_api_plain(
            user_prompt, user_id=user_id,
        )
    except Exception as ex:
        logger.exception('analyze_account_logs_security: LLM call failed')
        result['error'] = str(ex)
        text = ''

    text = (text or '').strip()
    if not text or len(text) < 40:
        # LLM не ответила или вернула слишком короткий текст — fallback
        # на эвристику, чтобы пользователь всё равно получил отчёт.
        result['text'] = _heuristic_risk_report(stats, flood)
        result['source'] = 'heuristic'
        result['ok'] = True
        if not text and result['error']:
            result['text'] = (
                f"{result['text']}\n\n"
                f"<i>(LLM недоступна: {escape(result['error'][:200])})</i>"
            )
        return result

    result['text'] = text
    result['source'] = 'llm'
    result['ok'] = True
    return result


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


# ========== /ПРОГРЕВ АККАУНТОВ ==========
# Цель прогрева — сделать аккаунт "живым" в глазах Telegram.
# Никаких спам-рассылок, только правдоподобные действия обычного юзера:
#   - прочитать диалоги и подтянуть новые сообщения
#   - отметить чаты прочитанными (без отправки)
#   - посмотреть 1–2 сторис у контактов
#   - кинуть лёгкую реакцию (👍 ❤️ 🔥 😂) на 1 сообщение
#   - изредка отправить что-то в Избранное (Saved Messages)
#   - изредка подёргать статус (online / offline)
#   - при тихом часе — вообще уйти в сон
# Всё с адаптивной задержкой (5–18 минут между волнами)
# и множителем по времени суток (МСК).

# Реакции, которые безопасно кидать в прогреве.
WARMING_REACTIONS = [
    "\U0001f44d",  # 👍
    "\U0001f525",  # 🔥
    "\u2764\ufe0f",  # ❤️
    "\U0001f602",  # 😂
    "\U0001f60a",  # 😊
    "\U0001f64f",  # 🙏
]

# Фразы для Избранного — короткие, нейтральные, "как у живого человека".
WARMING_SAVED_NOTES = [
    "Заметка: не забыть ответить @{who} позже",
    "Напоминалка самому себе",
    "Скину сюда идею, чтобы не потерять",
    "Тест прогрева",
    "Записал мысль, чтобы не забыть",
    "Позже разберусь",
]


def _is_quiet_hours() -> bool:
    """Ночной режим по МСК: 0–7 и 23–24 — спим, активность минимальна."""
    hour = datetime.now(MSK_TZ).hour
    return hour < 7 or hour >= 23


def _is_in_quiet_period(periods: List[str]) -> bool:
    """Проверяет, попадает ли текущее время (МСК) хотя бы в один
    из тихих периодов вида "HH:MM-HH:MM".
    Если в плане нет ни одного периода — считаем, что тишины нет.
    """
    if not periods:
        return False
    now = datetime.now(MSK_TZ)
    cur = now.hour * 60 + now.minute
    for raw in periods:
        try:
            s = str(raw).strip()
            if '-' not in s:
                continue
            a, b = s.split('-', 1)
            ah, am = (int(x) for x in a.strip().split(':')[:2])
            bh, bm = (int(x) for x in b.strip().split(':')[:2])
            start = ah * 60 + am
            end = bh * 60 + bm
            if start == end:
                continue
            if start < end:
                if start <= cur < end:
                    return True
            else:
                # переход через полночь (например 23:00-06:00)
                if cur >= start or cur < end:
                    return True
        except Exception:
            continue
    return False


# Карта: код действия из плана → функция-обработчик.
# Определяется ПОСЛЕ всех _warming_action_*, чтобы избежать NameError
# при импорте модуля (forward references).
_WARMING_ACTIONS_MAP = None  # type: ignore[assignment]


def _get_warming_actions_map() -> Dict[str, Any]:
    global _WARMING_ACTIONS_MAP
    if _WARMING_ACTIONS_MAP is None:
        _WARMING_ACTIONS_MAP = {
            'read_dialogs':  _warming_action_read_dialogs,
            'view_stories':  _warming_action_view_stories,
            'react':         _warming_action_react,
            'saved_note':    _warming_action_saved_note,
            'typing':        _warming_action_typing,
            'status_toggle': _warming_action_status_toggle,
        }
    return _WARMING_ACTIONS_MAP


def _build_weighted_pool(distribution: Dict[str, float]) -> List[str]:
    """Строит список-копилку для weighted-выбора по distribution.
    Каждый kind попадает в список пропорционально его весу.
    """
    pool: List[str] = []
    if not distribution:
        return pool
    total = 0.0
    actions_map = _get_warming_actions_map()
    for kind, w in distribution.items():
        if kind not in actions_map:
            continue
        try:
            w = float(w)
        except (TypeError, ValueError):
            continue
        if w <= 0:
            continue
        # Округляем до сотых, чтобы не пухнуть
        n = int(round(w * 100))
        if n <= 0:
            continue
        pool.extend([kind] * n)
        total += n
    if not pool:
        # фолбек — равные веса для безопасных действий
        return ['read_dialogs', 'view_stories', 'react']
    return pool


def _actions_count_for_now(
    schedule: List[Dict[str, Any]], distribution: Dict[str, float]
) -> int:
    """Сколько действий выполнить в текущей волне — на основе
    schedule из плана. Если для текущего часа нет фазы, берём
    дефолт 2-3 действия.
    """
    if not schedule:
        return random.randint(2, 3)
    now_h = datetime.now(MSK_TZ).hour
    best = None
    for s in schedule:
        try:
            ho = int(s.get('hour_offset', 0))
        except (TypeError, ValueError):
            continue
        if best is None or abs(ho - now_h) < abs(best[0] - now_h):
            best = (ho, s)
    if not best:
        return random.randint(2, 3)
    s = best[1]
    try:
        amin = int(s.get('actions_count_min', 2))
        amax = int(s.get('actions_count_max', 3))
    except (TypeError, ValueError):
        amin, amax = 2, 3
    if amax < amin:
        amax = amin
    # Слегка режем на «low» интенсивности
    inten = str(s.get('intensity', 'medium')).lower()
    if inten == 'low':
        amin = max(1, amin - 1)
        amax = max(amin, amax - 1)
    elif inten == 'high':
        amin = amin + 1
        amax = amax + 1
    return random.randint(amin, max(amin, amax))


def _warming_random_cooldown() -> int:
    """Случайная пауза между волнами с учётом времени суток."""
    base = random.randint(
        WARMING_DEFAULT_COOLDOWN_MIN, WARMING_DEFAULT_COOLDOWN_MAX
    )
    return int(base * _time_of_day_multiplier())


async def _warming_log(account_id: int, kind: str, text: str = "") -> None:
    """Зеркалим действия прогрева в общий лог, чтобы юзер видел активность."""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                '''INSERT INTO account_logs
                (account_id, chat_name, chat_id, direction, message_text)
                VALUES ($1, $2, NULL, $3, $4)''',
                account_id, f"прогрев:{kind}", "warming", text[:100]
            )
    except Exception as ex:
        logger.debug(f"warming_log failed: {ex}")


async def _warming_get_dialogs(client: TelegramClient, limit: int = 30):
    """Безопасно достаём последние диалоги. Игнорим любые ошибки приватности."""
    try:
        result = await client(GetDialogsRequest(
            offset_date=None,
            offset_id=0,
            offset_peer=InputPeerUser(0, 0),
            limit=limit,
            hash=0
        ))
        # Telethon сам отдаёт chats/users; нам нужен список диалогов
        return result.dialogs or []
    except FloodWaitError as fw:
        logger.info(f"warming: flood wait {fw.seconds}s в GetDialogs")
        return []
    except Exception as ex:
        logger.debug(f"warming GetDialogs: {ex}")
        return []


async def _warming_action_read_dialogs(
    client: TelegramClient, account_id: int
) -> str:
    """Помечаем 1–3 чата как прочитанные (mark as read)."""
    dialogs = await _warming_get_dialogs(client, limit=30)
    if not dialogs:
        return ""
    unread = [d for d in dialogs if getattr(d, "unread_count", 0)]
    if not unread:
        # Нечего читать — берём любой
        unread = dialogs[:3]
    if not unread:
        return ""
    target = random.choice(unread[:5])
    peer = target.peer
    max_id = getattr(target, "top_message", 0) or 0
    try:
        await client(ReadHistoryRequest(peer=peer, max_id=max_id))
        name = getattr(target, "name", "диалог")
        return f"прочитал {name}"
    except FloodWaitError as fw:
        await record_flood_wait(account_id, 0, fw.seconds)
        return ""
    except Exception as ex:
        logger.debug(f"warming read: {ex}")
        return ""


async def _warming_action_view_stories(
    client: TelegramClient, account_id: int
) -> str:
    """Просматриваем 1–2 сторис у контактов. Ничего не лайкаем."""
    try:
        all_stories = await client(GetAllStoriesRequest())
        stories = []
        for peer_stories in (all_stories.peer_stories or []):
            for s in (peer_stories.stories or []):
                stories.append((peer_stories.peer, s))
        if not stories:
            return ""
        random.shuffle(stories)
        for peer, story in stories[:1]:
            try:
                await client(ReadStoriesRequest(peer=peer, max_id=story.id))
                return f"посмотрел сторис"
            except FloodWaitError as fw:
                await record_flood_wait(account_id, 0, fw.seconds)
                return ""
            except Exception:
                continue
    except FloodWaitError as fw:
        await record_flood_wait(account_id, 0, fw.seconds)
    except Exception as ex:
        logger.debug(f"warming stories: {ex}")
    return ""


async def _warming_action_react(
    client: TelegramClient, account_id: int,
    reaction_pool: Optional[List[str]] = None
) -> str:
    """Кидаем лёгкую реакцию на 1 свежее сообщение в диалоге."""
    try:
        result = await client(GetDialogsRequest(
            offset_date=None, offset_id=0,
            offset_peer=InputPeerUser(0, 0), limit=20, hash=0
        ))
        dialogs = result.dialogs or []
    except Exception as ex:
        logger.debug(f"warming react dialogs: {ex}")
        return ""
    candidates = []
    for d in dialogs:
        if not getattr(d, "top_message", None):
            continue
        if isinstance(d.peer, (InputPeerUser,)):
            candidates.append(d)
    if not candidates:
        return ""
    target = random.choice(candidates)
    msg_id = target.top_message
    try:
        # Если передали пул реакций из плана — используем его,
        # иначе дефолтный набор.
        pool = WARMING_REACTIONS
        if reaction_pool:
            pool = [r for r in reaction_pool if r]
        if not pool:
            pool = WARMING_REACTIONS
        reaction = random.choice(pool)
        # SendReactionRequest требует именно эмодзи
        from telethon.tl.functions.messages import SendReactionRequest as _SRR
        await client(_SRR(
            peer=target.peer,
            msg_id=msg_id,
            reaction=reaction
        ))
        return f"реакция {reaction}"
    except FloodWaitError as fw:
        await record_flood_wait(account_id, 0, fw.seconds)
    except Exception as ex:
        logger.debug(f"warming react: {ex}")
    return ""


async def _warming_action_saved_note(
    client: TelegramClient, account_id: int,
    saved_notes: Optional[List[str]] = None
) -> str:
    """Изредка пишем короткую заметку в Избранное (self-PM)."""
    try:
        me = await client.get_me()
        if not me:
            return ""
        who = (me.username or "me") if hasattr(me, "username") else "me"
        notes = saved_notes if saved_notes else WARMING_SAVED_NOTES
        text = random.choice(notes).format(who=who)
        await client.send_message("me", text)
        return f"заметка в Избранном"
    except FloodWaitError as fw:
        await record_flood_wait(account_id, 0, fw.seconds)
    except Exception as ex:
        logger.debug(f"warming saved: {ex}")
    return ""


async def _warming_action_status_toggle(
    client: TelegramClient, account_id: int
) -> str:
    """Подёргать статус online/offline — не часто, чтобы не тригерить антифрод."""
    try:
        # 50/50 online / offline, но избегаем спама по статусу
        online = random.random() < 0.5
        await client(UpdateStatusRequest(offline=not online))
        return "online" if online else "offline"
    except FloodWaitError as fw:
        await record_flood_wait(account_id, 0, fw.seconds)
    except Exception as ex:
        logger.debug(f"warming status: {ex}")
    return ""


async def _warming_action_typing(
    client: TelegramClient, account_id: int
) -> str:
    """Подёргать "печатает..." в случайном диалоге на пару секунд."""
    try:
        result = await client(GetDialogsRequest(
            offset_date=None, offset_id=0,
            offset_peer=InputPeerUser(0, 0), limit=20, hash=0
        ))
        dialogs = [d for d in (result.dialogs or []) if d.peer]
        if not dialogs:
            return ""
        target = random.choice(dialogs)
        # typing action "typing"
        await client(SetTypingRequest(
            peer=target.peer,
            action=SendMessageTypingAction()
        ))
        await asyncio.sleep(random.uniform(1.5, 4.0))
        # отменяем typing
        await client(SetTypingRequest(
            peer=target.peer,
            action=SendMessageCancelAction()
        ))
        return "печатал..."
    except FloodWaitError as fw:
        await record_flood_wait(account_id, 0, fw.seconds)
    except Exception as ex:
        logger.debug(f"warming typing: {ex}")
    return ""


# Вспомогательный typing-action — импортируем, чтобы не светить в шапке файла.
try:
    from telethon.tl.types import (
        SendMessageTypingAction, SendMessageCancelAction
    )
except Exception:  # на всякий случай
    SendMessageTypingAction = None
    SendMessageCancelAction = None


async def warming_worker(account_id: int, user_id: int) -> None:
    """Главный цикл прогрева. Тикает, пока warming_stop_flags[account_id] False.

    Если у аккаунта есть активный план (warming_plans.is_active=TRUE),
    воркер работает по плану:
      * интервалы из plan.intervals_min_sec / intervals_max_sec
      * действия выбираются по plan.distribution
      * тексты для Избранного — plan.saved_notes
      * пул реакций — plan.reaction_pool
      * в тихие часы (plan.quiet_periods) уходит в длинный сон
    Если активного плана нет — fallback на старую логику
    (рандом по cooldown_min/max из БД).
    """
    logger.info(f"warming_worker: старт для account_id={account_id}")
    warming_stop_flags[account_id] = False
    cycle = 0
    plan_start_ts: Optional[float] = None
    plan_duration_sec: Optional[int] = None
    plan = None
    try:
        # Подгружаем активный план ОДИН раз на старте.
        try:
            active = await get_active_warming_plan(account_id)
            if active:
                plan = active.get('plan') or {}
                plan = _safe_plan_defaults(plan)
                try:
                    plan_duration_sec = int(plan.get('duration_hours', 12)) * 3600
                except Exception:
                    plan_duration_sec = 12 * 3600
                plan_start_ts = time.monotonic()
                # Фиксируем started_at = NOW()
                try:
                    await mark_warming_plan_started(int(active['id']))
                except Exception:
                    pass
                logger.info(
                    f"warming_worker: план #{active['id']} активирован для "
                    f"account_id={account_id}, duration={plan_duration_sec}s"
                )
        except Exception as ex:
            logger.warning(f"warming_worker: не удалось подгрузить план: {ex}")
            plan = None

        saved_notes = (plan or {}).get('saved_notes') or list(WARMING_SAVED_NOTES)
        reaction_pool = (plan or {}).get('reaction_pool') or list(WARMING_REACTIONS)
        distribution = (plan or {}).get('distribution') or {}
        schedule = (plan or {}).get('schedule') or []
        quiet_periods = (plan or {}).get('quiet_periods') or ['00:00-07:00']
        try:
            intervals_min = int((plan or {}).get('intervals_min_sec', WARMING_DEFAULT_COOLDOWN_MIN))
            intervals_max = int((plan or {}).get('intervals_max_sec', WARMING_DEFAULT_COOLDOWN_MAX))
        except Exception:
            intervals_min, intervals_max = WARMING_DEFAULT_COOLDOWN_MIN, WARMING_DEFAULT_COOLDOWN_MAX
        if intervals_max < intervals_min:
            intervals_max = intervals_min

        while not warming_stop_flags.get(account_id, False):
            cycle += 1
            # Берём актуальные настройки из БД
            account = await get_account(account_id)
            if not account or not account.get('warming_enabled'):
                return

            # Проверка окончания плана (если был запущен с duration)
            if (
                plan is not None
                and plan_start_ts is not None
                and plan_duration_sec is not None
                and (time.monotonic() - plan_start_ts) >= plan_duration_sec
            ):
                logger.info(
                    f"warming_worker: план выполнен для account_id="
                    f"{account_id} (cycles={cycle})"
                )
                # Деактивируем план, но НЕ выключаем прогрев автоматически.
                # Юзер сам решит — продлить или выключить.
                try:
                    await deactivate_warming_plans(account_id)
                except Exception:
                    pass
                plan = None

            # Кастомные задержки из БД, если юзер их настроил
            cooldown_min = account.get('warming_min_cooldown') or intervals_min
            cooldown_max = account.get('warming_max_cooldown') or intervals_max
            if cooldown_max < cooldown_min:
                cooldown_max = cooldown_min

            # Проверка тихих часов из плана
            in_quiet = _is_in_quiet_period(quiet_periods)
            # На ночь уходим в длинный сон
            if in_quiet or _is_quiet_hours():
                sleep_for = random.randint(45 * 60, 75 * 60)
            else:
                sleep_for = int(
                    random.randint(cooldown_min, cooldown_max)
                    * _time_of_day_multiplier()
                )

            # Спим, но короткими чанками, чтобы стоп-флаг реагировал быстро
            slept = 0
            chunk = 5
            while slept < sleep_for:
                if warming_stop_flags.get(account_id, False):
                    return
                step = min(chunk, sleep_for - slept)
                await asyncio.sleep(step)
                slept += step

            if warming_stop_flags.get(account_id, False):
                return

            # Перечитываем аккаунт — могли выключить кнопкой
            account = await get_account(account_id)
            if not account or not account.get('warming_enabled'):
                return

            # Достаём telethon-клиент; если упал — пробуем переподключиться
            client = await get_client_for_account(account_id)
            if not client or not client.is_connected():
                try:
                    client = await create_telethon_client(account_id)
                    if not client:
                        await asyncio.sleep(60)
                        continue
                except Exception as ex:
                    logger.warning(
                        f"warming: не удалось подключить account_id="
                        f"{account_id}: {ex}"
                    )
                    await asyncio.sleep(60)
                    continue

            # === ВЫБОР ДЕЙСТВИЙ ПО ПЛАНУ ===
            if plan is not None and distribution:
                # Сколько действий в этой волне — по schedule для текущего часа
                n_actions = _actions_count_for_now(schedule, distribution)
                # Сэмплируем N действий по distribution (без повторов).
                chosen: List = []
                pool = _build_weighted_pool(distribution)
                if pool:
                    seen = set()
                    random.shuffle(pool)
                    for kind in pool:
                        if len(chosen) >= n_actions:
                            break
                        if kind in seen:
                            continue
                        seen.add(kind)
                        fn = _get_warming_actions_map().get(kind)
                        if fn:
                            chosen.append((kind, fn))
            else:
                # Fallback: старая логика — случайный пул
                n_actions = random.randint(
                    WARMING_ACTIONS_PER_CYCLE_MIN,
                    WARMING_ACTIONS_PER_CYCLE_MAX
                )
                actions_pool = [
                    _warming_action_read_dialogs,
                    _warming_action_view_stories,
                    _warming_action_typing,
                ]
                if cycle % 4 == 0:
                    actions_pool.append(_warming_action_saved_note)
                if cycle % 3 == 0:
                    actions_pool.append(_warming_action_react)
                if cycle % 7 == 0:
                    actions_pool.append(_warming_action_status_toggle)
                chosen = [
                    (fn.__name__, fn)
                    for fn in random.sample(
                        actions_pool, k=min(n_actions, len(actions_pool))
                    )
                ]

            for kind, action in chosen:
                if warming_stop_flags.get(account_id, False):
                    return
                try:
                    # Передаём контекст плана в те действия, которые его ждут.
                    if kind == 'saved_note':
                        res = await action(
                            client, account_id,
                            saved_notes=saved_notes
                        )
                    elif kind == 'react':
                        res = await action(
                            client, account_id,
                            reaction_pool=reaction_pool
                        )
                    else:
                        res = await action(client, account_id)
                    if res:
                        logger.info(
                            f"warming a{account_id} c{cycle} {kind}: {res}"
                        )
                        await _warming_log(account_id, kind, res)
                except Exception as ex:
                    logger.debug(f"warming action error: {ex}")
                # Микропауза между действиями внутри волны
                await asyncio.sleep(random.uniform(2, 7))

            # Фиксируем статистику в БД
            try:
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE accounts SET warming_cycles = "
                        "COALESCE(warming_cycles, 0) + 1, "
                        "warming_last_active = NOW() WHERE id = $1",
                        account_id
                    )
            except Exception as ex:
                logger.debug(f"warming stats update failed: {ex}")
    except asyncio.CancelledError:
        logger.info(
            f"warming_worker: отменён для account_id={account_id}"
        )
    except Exception as ex:
        logger.error(
            f"warming_worker: крэш для account_id={account_id}: {ex}"
        )
    finally:
        warming_stop_flags.pop(account_id, None)
        if account_id in warming_tasks:
            del warming_tasks[account_id]
        logger.info(
            f"warming_worker: финиш account_id={account_id} (cycles={cycle})"
        )


async def start_warming(account_id: int, user_id: int) -> bool:
    """Запустить воркер прогрева. True если запустили, False если уже шёл."""
    if account_id in warming_tasks and not warming_tasks[account_id].done():
        return False
    warming_stop_flags[account_id] = False
    task = asyncio.create_task(warming_worker(account_id, user_id))
    warming_tasks[account_id] = task
    return True


async def stop_warming(account_id: int) -> None:
    """Аккуратно остановить воркер прогрева."""
    warming_stop_flags[account_id] = True
    task = warming_tasks.get(account_id)
    if task and not task.done():
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=10)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
    warming_tasks.pop(account_id, None)
    warming_stop_flags.pop(account_id, None)

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
            await record_flood_wait(account_id, 0, ex.seconds)
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
            await record_flood_wait(account_id, 0, ex.seconds)
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
                try:
                    chat_id_int = int(chat_id) if str(chat_id).lstrip('-').isdigit() else 0
                    await record_flood_wait(account_id, chat_id_int, ex.seconds)
                except Exception:
                    pass
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


# ============================================================
# SMART DELAY ENGINE
# ============================================================
# Адаптивная задержка перед отправкой сообщения.
# Снижает риск бана на ~30-50% за счёт:
#   1) времени суток (ночью/пик вечером — медленнее)
#   2) частоты аккаунта в конкретном чате (если только что писал — пауза)
#   3) flood-wait истории аккаунта (если недавно ловили флуд — сильно медленнее)
# Плюс всегда добавляется случайный джиттер ±15%, чтобы поведение
# не выглядело роботизированным.
SMART_DELAY_MIN = 2.0        # минимальный "хвост" задержки (сек)
SMART_DELAY_MAX = 60.0       # потолок адаптивной задержки
SMART_DELAY_JITTER = 0.15    # ±15% джиттер


def _time_of_day_multiplier() -> float:
    """Множитель по часу МСК: ночью тормозим, днём норма, вечером осторожно."""
    hour = datetime.now(MSK_TZ).hour
    if 0 <= hour < 7:        # ночь — Telegram-антиспам самый злой
        return 1.6
    if 7 <= hour < 11:       # утро — норма
        return 1.0
    if 11 <= hour < 14:      # обед — небольшой пик
        return 1.15
    if 14 <= hour < 18:      # день — норма
        return 1.0
    if 18 <= hour < 23:      # вечерний прайм-тайм
        return 1.3
    return 1.4               # 23–24 поздний вечер


async def _seconds_since_last_send(account_id: int, chat_id: str) -> Optional[float]:
    """Когда аккаунт последний раз отправлял в этот чат (по account_logs)."""
    try:
        chat_id_int = int(chat_id) if str(chat_id).lstrip('-').isdigit() else None
    except (TypeError, ValueError):
        chat_id_int = None
    if chat_id_int is None:
        return None
    async with db_pool.acquire() as conn:
        last = await conn.fetchval(
            "SELECT EXTRACT(EPOCH FROM (NOW() - created_at)) "
            "FROM account_logs "
            "WHERE account_id = $1 AND chat_id = $2 AND direction = 'sent' "
            "ORDER BY created_at DESC LIMIT 1",
            account_id, chat_id_int
        )
    return float(last) if last is not None else None


async def _account_flood_score(account_id: int) -> float:
    """Штраф за недавние флуд-вейты: 1.0 = чисто, 2.0+ = недавно банили."""
    async with db_pool.acquire() as conn:
        # последние 24 часа
        last_24h = await conn.fetchval(
            "SELECT COALESCE(SUM(seconds), 0) FROM flood_wait_history "
            "WHERE account_id = $1 AND occurred_at > NOW() - INTERVAL '24 hours'",
            account_id
        ) or 0
        # последний час — самый весомый
        last_1h = await conn.fetchval(
            "SELECT COALESCE(SUM(seconds), 0) FROM flood_wait_history "
            "WHERE account_id = $1 AND occurred_at > NOW() - INTERVAL '1 hour'",
            account_id
        ) or 0
        # количество флуд-вейтов за час
        last_1h_count = await conn.fetchval(
            "SELECT COUNT(*) FROM flood_wait_history "
            "WHERE account_id = $1 AND occurred_at > NOW() - INTERVAL '1 hour'",
            account_id
        ) or 0
    score = 1.0
    if last_24h > 0:
        score += min(last_24h / 600.0, 0.6)  # максимум +0.6 за сутки
    if last_1h > 0:
        score += min(last_1h / 60.0, 0.8)   # максимум +0.8 за час
    if last_1h_count >= 3:
        score += 0.5                         # серия флудов — серьёзный штраф
    return score


async def record_flood_wait(account_id: int, chat_id: int, seconds: int) -> None:
    """Сохранить факт флуд-вейта, чтобы Smart Delay учитывал историю."""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO flood_wait_history "
                "(account_id, chat_id, seconds) VALUES ($1, $2, $3)",
                account_id, chat_id, int(seconds)
            )
            # Подчищаем хвост старше 7 дней, чтобы таблица не пухла.
            await conn.execute(
                "DELETE FROM flood_wait_history "
                "WHERE account_id = $1 AND occurred_at < NOW() - INTERVAL '7 days'",
                account_id
            )
    except Exception as ex:
        logger.warning(f"record_flood_wait failed: {ex}")


async def smart_delay(
    account_id: int,
    chat_id: str,
    base_delay: float = 0.0,
    min_delay: float = SMART_DELAY_MIN,
    max_delay: float = SMART_DELAY_MAX,
) -> float:
    """Возвращает адаптивную задержку (сек) перед отправкой в chat_id.

    Учитывает:
      - время суток (МСК)
      - когда аккаунт последний раз писал в этот чат
      - историю флуд-вейтов аккаунта
    Возвращённое значение уже включает джиттер ±15% и
    ограничено [min_delay, max_delay] секунд.
    """
    try:
        tod_mult = _time_of_day_multiplier()
        flood_score = await _account_flood_score(account_id)
        seconds_since = await _seconds_since_last_send(account_id, chat_id)

        # Базовое значение: либо переданный base_delay, либо 3 секунды.
        value = float(base_delay) if base_delay and base_delay > 0 else 3.0

        # Учёт частоты отправки в этот чат.
        if seconds_since is not None:
            if seconds_since < 30:
                value += 12.0
            elif seconds_since < 120:
                value += 7.0
            elif seconds_since < 600:
                value += 3.0
            # > 10 минут — ничего не добавляем, можно смело слать.

        # Время суток.
        value *= tod_mult
        # История флуд-вейтов.
        value *= flood_score

        # Джиттер ±15%, чтобы поведение не выглядело роботизированным.
        jitter = 1.0 + random.uniform(-SMART_DELAY_JITTER, SMART_DELAY_JITTER)
        value *= jitter

        # Границы.
        value = max(min_delay, min(max_delay, value))
        return value
    except Exception as ex:
        logger.warning(f"smart_delay fallback: {ex}")
        return min_delay


# ============================================================
# ПОДПИСКИ (Free / Pro) + CRYPTO PAY
# ============================================================
CRYPTO_PAY_API = "https://pay.crypt.bot/api"
# Токен Crypto Pay (@CryptoBot) — основной способ оплаты Pro.
CRYPTO_PAY_TOKEN = os.getenv("CRYPTO_PAY_TOKEN") or "490665:AAEwanehVerJ8FvFsTf81CWtyY9wSFW86aF"
# Pro-подписка: 40₽/мес, выставляется счётом на $0.60 в USDT.
PRO_PRICE_USD = "0.60"
PRO_PRICE_LABEL = "40₽ / месяц"
PRO_DURATION_DAYS = 30


async def get_subscription(user_id: int) -> Dict[str, Any]:
    """Возвращает текущую подписку пользователя. Авто-создаёт Free, если нет."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM subscriptions WHERE user_id = $1", user_id
        )
        if not row:
            await conn.execute(
                "INSERT INTO subscriptions (user_id, tier) VALUES ($1, 'free') "
                "ON CONFLICT (user_id) DO NOTHING",
                user_id
            )
            row = await conn.fetchrow(
                "SELECT * FROM subscriptions WHERE user_id = $1", user_id
            )
        data = dict(row)
    # Если Pro истёк — откатываем на Free.
    if data.get("tier") == "pro":
        exp = data.get("expires_at")
        if exp is not None and exp < datetime.now(MSK_TZ).replace(tzinfo=None):
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE subscriptions SET tier = 'free', expires_at = NULL, "
                    "updated_at = NOW() WHERE user_id = $1",
                    user_id
                )
            data["tier"] = "free"
            data["expires_at"] = None
    return data


async def set_subscription(
    user_id: int, tier: str, expires_at: Optional[datetime] = None,
    invoice_id: Optional[int] = None, invoice_payload: Optional[str] = None,
) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO subscriptions "
            "(user_id, tier, expires_at, last_invoice_id, last_invoice_payload, "
            " updated_at) "
            "VALUES ($1, $2, $3, $4, $5, NOW()) "
            "ON CONFLICT (user_id) DO UPDATE SET "
            "tier = EXCLUDED.tier, expires_at = EXCLUDED.expires_at, "
            "last_invoice_id = EXCLUDED.last_invoice_id, "
            "last_invoice_payload = EXCLUDED.last_invoice_payload, "
            "updated_at = NOW()",
            user_id, tier, expires_at, invoice_id, invoice_payload
        )


async def is_pro(user_id: int) -> bool:
    sub = await get_subscription(user_id)
    return sub.get("tier") == "pro"


async def _cryptopay_request(method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Базовый вызов Crypto Pay API v1. Возвращает {ok, result} или {ok:false, error}."""
    url = f"{CRYPTO_PAY_API}/{method}"
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=params, headers=headers, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                data = await resp.json()
        if data.get("ok"):
            return {"ok": True, "result": data.get("result")}
        return {"ok": False, "error": data.get("error") or data}
    except Exception as ex:
        logger.error(f"CryptoPay {method} failed: {ex}")
        return {"ok": False, "error": str(ex)}


async def cryptopay_create_invoice(
    user_id: int, amount: str = PRO_PRICE_USD, payload: str = "pro_30d"
) -> Dict[str, Any]:
    """Создаёт инвойс в USDT на $0.60 для Pro-подписки.
    Сумма фиксированная: 0.6 USDT (≈ 40₽ по текущему курсу).
    """
    bot_me = await bot.get_me()
    params = {
        "currency_type": "crypto",
        "asset": "USDT",
        "amount": str(amount),
        "description": "Vest Game Soft — Pro подписка (30 дней)",
        "payload": f"{payload}:{user_id}",
        "paid_btn_name": "callback",
        "paid_btn_url": f"https://t.me/{bot_me.username}",
    }
    return await _cryptopay_request("createInvoice", params)


async def cryptopay_get_invoices(invoice_ids: str) -> Dict[str, Any]:
    """Запросить статус инвойсов. invoice_ids — comma-separated string."""
    return await _cryptopay_request("getInvoices", {"invoice_ids": invoice_ids})


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
        text="Моя подписка",
        callback_data="my_subscription",
        style='success',
        icon_custom_emoji_id=get_icon("MONEY_SEND")
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
            text=f"{masked}",
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
        text="Без прокси",
        callback_data=f"acc_proxy_0",
        style='default'
    ))
    for p in proxies:
        label = p.get('label') or f"{p['host']}:{p['port']}"
        builder.row(InlineKeyboardButton(
            text=f"{p['proxy_type']} | {label}",
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


def get_llm_variants_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора одного из 3 вариантов текста."""
    builder = InlineKeyboardBuilder()
    for i in (1, 2, 3):
        builder.row(InlineKeyboardButton(
            text=f"Вариант {i}",
            callback_data=f"llm_pick_{i}",
            style='primary',
            icon_custom_emoji_id=get_icon("SPARK")
        ))
    builder.row(
        InlineKeyboardButton(
            text="Заново",
            callback_data="llm_regen",
            style='default',
            icon_custom_emoji_id=get_icon("REFRESH")
        ),
        InlineKeyboardButton(
            text="Новый запрос",
            callback_data="ai_generator",
            style='default',
            icon_custom_emoji_id=get_icon("WRITE")
        )
    )
    builder.row(InlineKeyboardButton(
        text="Сменить модель",
        callback_data="llm_model_menu",
        style='default',
        icon_custom_emoji_id=get_icon("BOT")
    ))
    builder.row(InlineKeyboardButton(
        text="Мои AI запросы",
        callback_data="ai_history",
        style='default',
        icon_custom_emoji_id=get_icon("CHART")
    ))
    builder.row(InlineKeyboardButton(
        text="В меню",
        callback_data="functions",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()


def get_llm_model_keyboard(current: str) -> InlineKeyboardMarkup:
    """Клавиатура смены модели (после генерации). Кнопка «Назад»."""
    builder = InlineKeyboardBuilder()
    for key, label in LLM_MODELS.items():
        mark = '✅ ' if key == current else ''
        builder.row(InlineKeyboardButton(
            text=f"{mark}{label}",
            callback_data=f"llm_set_{key}",
            style='primary' if key == current else 'default',
        ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="llm_back_to_variants",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()


def get_llm_model_pick_keyboard(
    current: str, include_back: bool = True
) -> InlineKeyboardMarkup:
    """Клавиатура выбора модели на старте генерации.
    Подсвечивает модель, выбранную пользователем (current).
    При include_back=False — нижняя кнопка не показывается
    (используется, если вызываем из основного меню)."""
    builder = InlineKeyboardBuilder()
    for key, label in LLM_MODELS.items():
        mark = '✅ ' if key == current else ''
        builder.row(InlineKeyboardButton(
            text=f"{mark}{label}",
            callback_data=f"llm_choose_{key}",
            style='primary' if key == current else 'default',
        ))
    if include_back:
        builder.row(InlineKeyboardButton(
            text="Отмена",
            callback_data="llm_cancel_pick",
            style='default',
            icon_custom_emoji_id=get_icon("BACK")
        ))
    return builder.as_markup()


def get_ai_history_keyboard(requests: List[Dict]) -> InlineKeyboardMarkup:
    """Список последних AI-запросов пользователя."""
    builder = InlineKeyboardBuilder()
    for r in requests:
        created = r['created_at']
        if hasattr(created, 'strftime'):
            when = created.strftime('%d.%m %H:%M')
        else:
            when = str(created)[:16]
        prompt_preview = (r['prompt'] or '').strip().replace('\n', ' ')
        if len(prompt_preview) > 40:
            prompt_preview = prompt_preview[:40] + '…'
        variants = r['variants'] if isinstance(r['variants'], list) else []
        chosen = r.get('chosen_index')
        marker = f"{chosen + 1}" if isinstance(chosen, int) else ''
        builder.row(InlineKeyboardButton(
            text=f"#{r['id']} · {when} · {len(variants)}вар.{marker} · {prompt_preview}",
            callback_data=f"ai_view_{r['id']}",
            style='default'
        ))
    builder.row(InlineKeyboardButton(
        text="Очистить историю",
        callback_data="ai_history_clear",
        style='danger',
        icon_custom_emoji_id=get_icon("DELETE")
    ))
    builder.row(InlineKeyboardButton(
        text="Новый запрос",
        callback_data="ai_generator",
        style='primary',
        icon_custom_emoji_id=get_icon("WRITE")
    ))
    builder.row(InlineKeyboardButton(
        text="В меню",
        callback_data="functions",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()


def get_ai_view_keyboard(request_id: int) -> InlineKeyboardMarkup:
    """Клавиатура при просмотре одного сохранённого запроса."""
    builder = InlineKeyboardBuilder()
    for i in (1, 2, 3):
        builder.row(InlineKeyboardButton(
            text=f"Файл · Вариант {i}",
            callback_data=f"ai_resend_{request_id}_{i}",
            style='primary',
            icon_custom_emoji_id=get_icon("SPARK")
        ))
    builder.row(
        InlineKeyboardButton(
            text="Скопировать текст",
            callback_data=f"ai_copy_{request_id}",
            style='default',
            icon_custom_emoji_id=get_icon("COPY")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="К истории",
            callback_data="ai_history",
            style='default',
            icon_custom_emoji_id=get_icon("BACK")
        )
    )
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
        text="AI Генератор текста",
        callback_data="ai_generator",
        style='primary',
        icon_custom_emoji_id=get_icon("AI")
    ))
    builder.row(InlineKeyboardButton(
        text="Мои AI запросы",
        callback_data="ai_history",
        style='default',
        icon_custom_emoji_id=get_icon("CHART")
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
        warming = "" if acc.get('warming_enabled') else ""
        status = "" if acc['is_active'] else ""
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
    builder.row(InlineKeyboardButton(
        text="Анализ риска бана",
        callback_data=f"analyze_risk_{account_id}",
        style='primary',
        icon_custom_emoji_id=get_icon("STATS")
    ))
    warming_text = (
        "Выключить прогрев" if warming_enabled else "Включить прогрев"
    )
    builder.row(InlineKeyboardButton(
        text=warming_text,
        callback_data=f"toggle_warming_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("FIRE")
    ))
    builder.row(InlineKeyboardButton(
        text="План прогрева (ИИ)",
        callback_data=f"show_warming_plan_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("CLIPBOARD")
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
        prefix = " " if is_selected else ""
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
            text="Вперед",
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


# ============================================================
# Подписка: Free / Pro
# ============================================================
def get_subscription_keyboard(tier: str) -> InlineKeyboardMarkup:
    """Клавиатура экрана подписки в зависимости от текущего тира."""
    builder = InlineKeyboardBuilder()
    if tier == "pro":
        builder.row(InlineKeyboardButton(
            text="Pro активна — спасибо!",
            callback_data="noop",
            style='success',
            icon_custom_emoji_id=get_icon("CHECK")
        ))
    else:
        builder.row(InlineKeyboardButton(
            text=f"Купить Pro — {PRO_PRICE_LABEL}",
            callback_data="buy_pro",
            style='primary',
            icon_custom_emoji_id=get_icon("MONEY_SEND")
        ))
    builder.row(InlineKeyboardButton(
        text="Проверить оплату",
        callback_data="check_pro_payment",
        style='default',
        icon_custom_emoji_id=get_icon("REFRESH")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="main_menu",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()


def _format_sub_text(sub: Dict[str, Any]) -> str:
    tier = sub.get("tier", "free")
    if tier == "pro":
        exp = sub.get("expires_at")
        exp_str = ""
        if exp:
            try:
                exp_str = exp.astimezone(MSK_TZ).strftime("%d.%m.%Y %H:%M МСК")
            except Exception:
                exp_str = str(exp)
        return (
            f"{emoji('MONEY_SEND')} <b>Моя подписка</b>\n\n"
            f"<b>Тариф:</b> Pro\n"
            f"{emoji('CLOCK')} <b>Активна до:</b> {exp_str}\n\n"
            f"{emoji('CHECK')} Спасибо за поддержку! Все функции открыты."
        )
    return (
        f"{emoji('MONEY_SEND')} <b>Моя подписка</b>\n\n"
        f"🆓 <b>Тариф:</b> Free\n\n"
        f"<b>Pro</b> — {PRO_PRICE_LABEL}:\n"
        f"  {emoji('CHECK')} Сняты базовые лимиты на рассылки\n"
        f"  {emoji('CHECK')} Приоритетная поддержка\n"
        f"  {emoji('CHECK')} Ранний доступ к новым функциям\n"
        f"  {emoji('CHECK')} Smart Delay Engine в усиленном режиме\n\n"
        f"Оплата через @CryptoBot (USDT, 0.6$)."
    )


@dp.callback_query(F.data == "my_subscription")
async def my_subscription(callback: CallbackQuery):
    sub = await get_subscription(callback.from_user.id)
    await callback.message.edit_text(
        _format_sub_text(sub),
        reply_markup=get_subscription_keyboard(sub.get("tier", "free"))
    )
    await callback.answer()


@dp.callback_query(F.data == "noop")
async def noop_handler(callback: CallbackQuery):
    await callback.answer("У вас уже Pro", show_alert=False)


@dp.callback_query(F.data == "buy_pro")
async def buy_pro(callback: CallbackQuery):
    await callback.answer()
    sub = await get_subscription(callback.from_user.id)
    if sub.get("tier") == "pro":
        await my_subscription(callback)
        return

    status_msg = await callback.message.edit_text(
        f"{emoji('CLOCK')} Создаю счёт в Crypto Pay...",
        reply_markup=None
    )
    result = await cryptopay_create_invoice(callback.from_user.id)
    if not result.get("ok"):
        await callback.message.edit_text(
            f"{emoji('CROSS')} Не удалось создать счёт.\n"
            f"Попробуйте позже или напишите в поддержку: {SUPPORT_USERNAME}\n\n"
            f"<code>{result.get('error')}</code>",
            reply_markup=get_subscription_keyboard("free")
        )
        return

    inv = result["result"]
    invoice_id = inv.get("invoice_id")
    pay_url = (
        inv.get("mini_app_invoice_url")
        or inv.get("bot_invoice_url")
        or inv.get("web_app_invoice_url")
    )
    # Сохраняем инвойс, чтобы потом проверять оплату.
    await set_subscription(
        callback.from_user.id, "free", None,
        invoice_id=invoice_id, invoice_payload=f"pro_30d:{callback.from_user.id}"
    )

    builder = InlineKeyboardBuilder()
    if pay_url:
        builder.row(InlineKeyboardButton(
            text="Оплатить через Crypto Pay",
            url=pay_url,
            style='primary',
            icon_custom_emoji_id=get_icon("MONEY_SEND")
        ))
    builder.row(InlineKeyboardButton(
        text="Я оплатил — проверить",
        callback_data="check_pro_payment",
        style='success',
        icon_custom_emoji_id=get_icon("CHECK")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="my_subscription",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))

    amount = inv.get("amount", PRO_PRICE_USD)
    asset = inv.get("asset", "USDT")
    await callback.message.edit_text(
        f"{emoji('MONEY_SEND')} <b>Счёт на оплату Pro</b>\n\n"
        f"Тариф: <b>Pro</b> ({PRO_PRICE_LABEL})\n"
        f"Сумма: <b>{amount} {asset}</b>\n"
        f"🆔 ID счёта: <code>{invoice_id}</code>\n\n"
        f"{emoji('INFO')} Нажмите кнопку ниже, оплатите в @CryptoBot, "
        f"затем вернитесь сюда и нажмите «Я оплатил — проверить».",
        reply_markup=builder.as_markup()
    )


@dp.callback_query(F.data == "check_pro_payment")
async def check_pro_payment(callback: CallbackQuery):
    await callback.answer()
    sub = await get_subscription(callback.from_user.id)
    invoice_id = sub.get("last_invoice_id")
    if not invoice_id:
        await callback.message.edit_text(
            f"{emoji('INFO')} У вас нет активного счёта. Нажмите «Купить Pro».",
            reply_markup=get_subscription_keyboard(sub.get("tier", "free"))
        )
        return

    result = await cryptopay_get_invoices(str(invoice_id))
    if not result.get("ok") or not result.get("result"):
        await callback.message.edit_text(
            f"{emoji('CROSS')} Не удалось проверить оплату. Попробуйте позже.\n\n"
            f"<code>{result.get('error')}</code>",
            reply_markup=get_subscription_keyboard(sub.get("tier", "free"))
        )
        return

    items = result["result"].get("items", [])
    if not items:
        await callback.message.edit_text(
            f"{emoji('CLOCK')} Счёт ещё не оплачен.\n"
            f"Оплатите в @CryptoBot и нажмите «Проверить» снова.",
            reply_markup=get_subscription_keyboard(sub.get("tier", "free"))
        )
        return

    inv = items[0]
    status = inv.get("status")
    if status == "paid":
        expires = datetime.now(MSK_TZ).replace(tzinfo=None) + timedelta(days=PRO_DURATION_DAYS)
        await set_subscription(
            callback.from_user.id, "pro", expires,
            invoice_id=invoice_id,
            invoice_payload=f"pro_30d:{callback.from_user.id}"
        )
        new_sub = await get_subscription(callback.from_user.id)
        await callback.message.edit_text(
            f"{emoji('FIRE')} <b>Оплата получена!</b>\n\n"
            + _format_sub_text(new_sub),
            reply_markup=get_subscription_keyboard("pro")
        )
    elif status == "active":
        await callback.message.edit_text(
            f"{emoji('CLOCK')} Счёт ещё не оплачен.\n"
            f"Оплатите в @CryptoBot и нажмите «Проверить» снова.",
            reply_markup=get_subscription_keyboard(sub.get("tier", "free"))
        )
    else:
        await callback.message.edit_text(
            f"{emoji('INFO')} Статус счёта: <b>{status}</b>.\n"
            f"Если возникла проблема — напишите в поддержку: {SUPPORT_USERNAME}",
            reply_markup=get_subscription_keyboard(sub.get("tier", "free"))
        )

# --- AI Генератор текста (LLM) ---
@dp.callback_query(F.data == "ai_generator")
async def ai_generator_start(callback: CallbackQuery, state: FSMContext):
    """Шаг 1: пользователь выбирает модель."""
    await state.clear()
    current = await get_user_llm_model(callback.from_user.id)
    label = LLM_MODELS.get(current, LLM_DEFAULT_MODEL)
    text = (
        f"{emoji('AI')} <b>AI Генератор текста</b>\n\n"
        f"{emoji('INFO')} Шаг 1 из 2. Выбери модель для генерации. "
        f"На шаге 2 опишешь задачу — бот пришлёт "
        f"<b>3 разных варианта</b> готового текста.\n\n"
        f"По умолчанию: <code>{escape(label)}</code>"
    )
    await callback.message.edit_text(
        text,
        reply_markup=get_llm_model_pick_keyboard(current, include_back=True)
    )
    await state.set_state(LLMStates.choosing_model)
    await callback.answer()


@dp.message(LLMStates.waiting_for_prompt)
async def ai_generator_prompt(message: Message, state: FSMContext):
    user_prompt = (message.text or '').strip()
    if not user_prompt:
        await message.answer(
            f"{emoji('CROSS')} Пришлите текстовый запрос."
        )
        return
    if len(user_prompt) > 4000:
        await message.answer(
            f"{emoji('CROSS')} Слишком длинный запрос "
            f"(макс. 4000 символов)."
        )
        return

    user_model = await get_user_llm_model(message.from_user.id)
    thinking = await message.answer(
        f"{emoji('LOADING')} <b>Генерирую 3 варианта…</b>\n\n"
        f"{emoji('BOT')} Модель: <code>{escape(user_model)}</code>\n"
        f"{emoji('WRITE')} Запрос: <i>{escape(user_prompt[:200])}</i>"
    )

    try:
        variants = await call_llm_api(user_prompt, user_id=message.from_user.id)
    except aiohttp.ClientError as e:
        logger.exception("LLM network error")
        await thinking.edit_text(
            f"{emoji('CROSS')} <b>Не удалось связаться с LLM API.</b>\n\n"
            f"<code>{escape(str(e))}</code>",
            reply_markup=get_llm_variants_keyboard()
        )
        await state.set_state(LLMStates.choosing_variant)
        return
    except Exception as e:
        logger.exception("LLM error")
        await thinking.edit_text(
            f"{emoji('CROSS')} <b>Ошибка генерации.</b>\n\n"
            f"<code>{escape(str(e))}</code>",
            reply_markup=get_llm_variants_keyboard()
        )
        await state.set_state(LLMStates.choosing_variant)
        return

    if not variants:
        await thinking.edit_text(
            f"{emoji('CROSS')} Модель не вернула валидные варианты. "
            f"Попробуйте переформулировать запрос.",
            reply_markup=get_llm_variants_keyboard()
        )
        await state.set_state(LLMStates.choosing_variant)
        return

    # Дополним до 3-х, если модель дала меньше
    while len(variants) < 3:
        variants.append({
            'title': f'Вариант {len(variants) + 1}',
            'text': '(пусто)'
        })

    # Сохраняем в БД
    try:
        request_id = await save_ai_request(
            message.from_user.id, user_prompt, variants, model=user_model,
        )
    except Exception as e:
        logger.exception("AI history save error")
        request_id = 0

    await state.update_data(
        prompt=user_prompt,
        variants=variants,
        request_id=request_id,
    )
    await state.set_state(LLMStates.choosing_variant)

    # Шлём 3 варианта текстом (без файлов).
    # Длинные тексты режем на куски по 4000 символов — Telegram лимит.
    for i, v in enumerate(variants, 1):
        title = (v.get('title') or '').strip() or f'Вариант {i}'
        body = (v.get('text') or '').strip()
        header = (
            f"{emoji('SPARK')} <b>Вариант {i}.</b> {escape(title)}\n"
            f"{emoji('INFO')} Длина: {len(body)} символов\n\n"
        )
        # первая часть — с заголовком
        first_chunk = header + body[: max(0, 4000 - len(header))]
        await message.answer(first_chunk)
        rest = body[max(0, 4000 - len(header)):]
        while rest:
            await message.answer(rest[:4000])
            rest = rest[4000:]

    summary = (
        f"{emoji('AI')} <b>3 варианта готовы.</b>\n\n"
        f"{emoji('INFO')} Запрос: <i>{escape(user_prompt[:160])}</i>\n"
        f"{emoji('CLOCK')} Запрос #{request_id or '—'} · {escape(LLM_MODEL)}"
    )
    await thinking.edit_text(
        summary,
        reply_markup=get_llm_variants_keyboard()
    )


@dp.callback_query(
    F.data.in_({'llm_pick_1', 'llm_pick_2', 'llm_pick_3'}),
    LLMStates.choosing_variant,
)
async def ai_generator_pick(callback: CallbackQuery, state: FSMContext):
    idx = int(callback.data.rsplit('_', 1)[1]) - 1
    data = await state.get_data()
    variants: List[Dict[str, str]] = data.get('variants') or []
    request_id: int = data.get('request_id') or 0
    if idx < 0 or idx >= len(variants):
        await callback.answer('Вариант не найден', show_alert=True)
        return

    chosen = variants[idx]
    title = escape(chosen.get('title') or f'Вариант {idx + 1}')
    body = chosen.get('text') or ''
    length = len(body)

    # Помечаем в истории, что юзер выбрал этот вариант
    if request_id:
        try:
            await mark_ai_chosen(request_id, callback.from_user.id, idx)
        except Exception:
            logger.exception("mark_ai_chosen error")

    # Присылаем полный текст выбранного варианта ещё раз
    header = (
        f"{emoji('SPARK')} <b>Вариант {idx + 1}.</b> {title}\n\n"
    )
    first_chunk = header + body[: max(0, 4000 - len(header))]
    await callback.message.answer(first_chunk)
    rest = body[max(0, 4000 - len(header)):]
    while rest:
        await callback.message.answer(rest[:4000])
        rest = rest[4000:]

    # Подтверждение + кнопки действий
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text=f"Скопировать текстом",
        callback_data=f"llm_copy_{idx + 1}",
        style='primary',
        icon_custom_emoji_id=get_icon("COPY")
    ))
    builder.row(
        InlineKeyboardButton(
            text="Заново",
            callback_data="llm_regen",
            style='default',
            icon_custom_emoji_id=get_icon("REFRESH")
        ),
        InlineKeyboardButton(
            text="Новый запрос",
            callback_data="ai_generator",
            style='default',
            icon_custom_emoji_id=get_icon("WRITE")
        )
    )
    builder.row(InlineKeyboardButton(
        text="Мои AI запросы",
        callback_data="ai_history",
        style='default',
        icon_custom_emoji_id=get_icon("CHART")
    ))
    builder.row(InlineKeyboardButton(
        text="В меню",
        callback_data="functions",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))

    await callback.message.answer(
        f"{emoji('CHECK')} <b>Вы выбрали Вариант {idx + 1}.</b> {title}\n\n"
        f"{emoji('INFO')} Длина: <b>{length}</b> символов · "
        f"Запрос <code>#{request_id or '—'}</code>",
        reply_markup=builder.as_markup()
    )
    await callback.answer(f"Выбран Вариант {idx + 1}")


@dp.callback_query(
    F.data.in_({'llm_show_1', 'llm_show_2', 'llm_show_3'})
)
async def ai_generator_show(callback: CallbackQuery, state: FSMContext):
    """Переслать выбранный вариант текстом ещё раз."""
    idx = int(callback.data.rsplit('_', 1)[1]) - 1
    data = await state.get_data()
    variants: List[Dict[str, str]] = data.get('variants') or []
    if idx < 0 or idx >= len(variants):
        await callback.answer('Вариант не найден', show_alert=True)
        return
    text = variants[idx].get('text') or ''
    title = (variants[idx].get('title') or '').strip() or f'Вариант {idx + 1}'
    header = (
        f"{emoji('SPARK')} <b>Вариант {idx + 1}.</b> {escape(title)}\n\n"
    )
    try:
        first_chunk = header + text[: max(0, 4000 - len(header))]
        await callback.message.answer(first_chunk)
        rest = text[max(0, 4000 - len(header)):]
        while rest:
            await callback.message.answer(rest[:4000])
            rest = rest[4000:]
        await callback.answer('Готово — можно копировать')
    except Exception as e:
        logger.exception("resend file error")
        await callback.answer(f'Ошибка: {e}', show_alert=True)


@dp.callback_query(
    F.data.in_({'llm_copy_1', 'llm_copy_2', 'llm_copy_3'})
)
async def ai_generator_copy(callback: CallbackQuery, state: FSMContext):
    """Прислать чистый текст варианта без экранирования — для копирования."""
    idx = int(callback.data.rsplit('_', 1)[1]) - 1
    data = await state.get_data()
    variants: List[Dict[str, str]] = data.get('variants') or []
    if idx < 0 or idx >= len(variants):
        await callback.answer('Вариант не найден', show_alert=True)
        return
    text = variants[idx].get('text') or ''
    # Отправляем без parse_mode, чтобы Telegram дал кнопку «Копировать»
    # и сохранил переносы строк.
    for i in range(0, len(text), 4000):
        await callback.message.answer(text[i:i + 4000])
    await callback.answer('Готово — можно копировать')


@dp.callback_query(F.data == "llm_regen", LLMStates.choosing_variant)
async def ai_generator_regen(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_prompt = (data.get('prompt') or '').strip()
    if not user_prompt:
        await callback.answer('Нет сохранённого запроса', show_alert=True)
        return

    thinking = await callback.message.edit_text(
        f"{emoji('LOADING')} <b>Генерирую заново…</b>"
    )
    try:
        regen_user_model = await get_user_llm_model(callback.from_user.id)
        variants = await call_llm_api(
            user_prompt, user_id=callback.from_user.id,
        )
    except Exception as e:
        logger.exception("LLM regen error")
        await callback.message.edit_text(
            f"{emoji('CROSS')} <b>Ошибка генерации.</b>\n\n"
            f"<code>{escape(str(e))}</code>",
            reply_markup=get_llm_variants_keyboard()
        )
        return

    if not variants:
        await callback.message.edit_text(
            f"{emoji('CROSS')} Модель не вернула валидные варианты.",
            reply_markup=get_llm_variants_keyboard()
        )
        return

    while len(variants) < 3:
        variants.append({
            'title': f'Вариант {len(variants) + 1}',
            'text': '(пусто)'
        })

    # Сохраняем в БД как новую запись
    try:
        new_request_id = await save_ai_request(
            callback.from_user.id, user_prompt, variants,
            model=regen_user_model,
        )
    except Exception:
        logger.exception("AI history regen save error")
        new_request_id = 0

    await state.update_data(
        variants=variants,
        request_id=new_request_id,
    )

    # Шлём 3 варианта текстом
    for i, v in enumerate(variants, 1):
        title = (v.get('title') or '').strip() or f'Вариант {i}'
        body = (v.get('text') or '').strip()
        header = (
            f"{emoji('SPARK')} <b>Вариант {i}.</b> {escape(title)}\n"
            f"{emoji('INFO')} Длина: {len(body)} символов\n\n"
        )
        first_chunk = header + body[: max(0, 4000 - len(header))]
        await callback.message.answer(first_chunk)
        rest = body[max(0, 4000 - len(header)):]
        while rest:
            await callback.message.answer(rest[:4000])
            rest = rest[4000:]

    summary = (
        f"{emoji('AI')} <b>Новые 3 варианта готовы.</b>\n\n"
        f"{emoji('CLOCK')} Запрос #{new_request_id or '—'}"
    )
    await thinking.edit_text(
        summary,
        reply_markup=get_llm_variants_keyboard()
    )
    await callback.answer()


# --- AI: переключение модели пользователем ---

@dp.callback_query(F.data == "llm_model_menu")
async def llm_model_menu(callback: CallbackQuery, state: FSMContext):
    """Показывает клавиатуру выбора LLM-модели с подсветкой текущей."""
    current = await get_user_llm_model(callback.from_user.id)
    label = LLM_MODELS.get(current, current)
    await callback.message.edit_text(
        f"{emoji('BOT')} <b>Выбор модели</b>\n\n"
        f"Текущая: <code>{escape(label)}</code>\n\n"
        f"{emoji('INFO')} Используется официальный Anthropic SDK, "
        f"прокси: <code>{escape(LLM_BASE_URL)}</code>.",
        reply_markup=get_llm_model_keyboard(current),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("llm_set_"))
async def llm_set_model(callback: CallbackQuery, state: FSMContext):
    """Смена модели ПОСЛЕ генерации (из меню вариантов). Сохраняет
    в профиль и возвращает к клавиатуре вариантов."""
    model = callback.data[len("llm_set_"):]
    if model not in LLM_MODELS:
        await callback.answer('Неизвестная модель', show_alert=True)
        return
    try:
        await set_user_llm_model(callback.from_user.id, model)
    except Exception as e:
        logger.exception('set_user_llm_model failed')
        await callback.answer(f'Ошибка сохранения: {e}', show_alert=True)
        return
    label = LLM_MODELS[model]
    await callback.message.edit_text(
        f"{emoji('OK')} Модель переключена на <b>{escape(label)}</b>.\n\n"
        f"{emoji('WRITE')} Новые ответы будут сгенерированы этой моделью. "
        f"Текущий результат сохранён — можно сгенерировать заново.",
        reply_markup=get_llm_variants_keyboard(),
    )
    await callback.answer(f'Модель: {label}')


@dp.callback_query(F.data.startswith("llm_choose_"))
async def llm_pick_for_request(callback: CallbackQuery, state: FSMContext):
    """Шаг 2: модель выбрана ПЕРЕД написанием промта. Сохраняем выбор
    в профиле и просим пользователя описать задачу."""
    model = callback.data[len("llm_choose_"):]
    if model not in LLM_MODELS:
        await callback.answer('Неизвестная модель', show_alert=True)
        return
    try:
        await set_user_llm_model(callback.from_user.id, model)
    except Exception as e:
        logger.exception('set_user_llm_model failed')
        await callback.answer(f'Ошибка сохранения: {e}', show_alert=True)
        return
    label = LLM_MODELS[model]
    await state.update_data(model=model)
    await state.set_state(LLMStates.waiting_for_prompt)
    text = (
        f"{emoji('CHECK')} <b>Модель:</b> <code>{escape(label)}</code>\n\n"
        f"{emoji('AI')} Опиши задачу — нейросеть предложит "
        f"<b>3 разных варианта</b> готового текста.\n\n"
        f"{emoji('WRITE')} <b>Примеры:</b>\n"
        f"• <i>Продающий пост для канала про крипту</i>\n"
        f"• <i>Приветствие для новых подписчиков</i>\n"
        f"• <i>Короткое описание услуги в 2–3 предложениях</i>\n\n"
        f"Отправь запрос следующим сообщением:"
    )
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Сменить модель",
        callback_data="llm_change_model",
        style='default',
        icon_custom_emoji_id=get_icon("BOT")
    ))
    builder.row(InlineKeyboardButton(
        text="Мои AI запросы",
        callback_data="ai_history",
        style='default',
        icon_custom_emoji_id=get_icon("CHART")
    ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="functions",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer(f'Модель: {label}')


@dp.callback_query(F.data == "llm_change_model")
async def llm_change_model(callback: CallbackQuery, state: FSMContext):
    """Вернуться к выбору модели из состояния ожидания промта."""
    current = await get_user_llm_model(callback.from_user.id)
    text = (
        f"{emoji('AI')} <b>AI Генератор текста</b>\n\n"
        f"{emoji('INFO')} Выбери модель для генерации:"
    )
    await callback.message.edit_text(
        text,
        reply_markup=get_llm_model_pick_keyboard(current, include_back=False)
    )
    await state.set_state(LLMStates.choosing_model)
    await callback.answer()


@dp.callback_query(F.data == "llm_cancel_pick")
async def llm_cancel_pick(callback: CallbackQuery, state: FSMContext):
    """Отмена выбора модели на старте → возврат в главное меню."""
    await state.clear()
    await callback.message.edit_text(
        f"{emoji('CROSS')} Выбор модели отменён.",
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data == "llm_back_to_variants")
async def llm_back_to_variants(callback: CallbackQuery, state: FSMContext):
    """Возврат к клавиатуре вариантов из меню модели."""
    data = await state.get_data()
    variants = data.get('variants') or []
    request_id = data.get('request_id')
    if variants:
        preview_lines = [
            f"{emoji('EYE')} <b>Краткий превью:</b>",
        ]
        for i, v in enumerate(variants, 1):
            preview_lines.append(
                f"\n<b>Вариант {i}.</b> {escape(v.get('title') or '')}"
            )
            body = (v.get('text') or '').strip()
            if len(body) > 200:
                body = body[:200].rstrip() + '…'
            preview_lines.append(escape(body))
        await callback.message.edit_text(
            '\n'.join(preview_lines),
            reply_markup=get_llm_variants_keyboard(),
        )
    else:
        await callback.message.edit_text(
            f"{emoji('WRITE')} Выбери действие:",
            reply_markup=get_llm_variants_keyboard(),
        )
    await callback.answer()


# --- AI: история запросов ---

@dp.callback_query(F.data == "ai_history")
async def ai_generator_history(callback: CallbackQuery, state: FSMContext):
    requests = await get_ai_requests(callback.from_user.id, limit=10)
    await state.clear()
    if not requests:
        await callback.message.edit_text(
            f"{emoji('INFO')} <b>История пуста.</b>\n\n"
            f"Сгенерируйте первый текст — он сохранится автоматически.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Создать запрос",
                    callback_data="ai_generator",
                    style='primary',
                    icon_custom_emoji_id=get_icon("WRITE")
                ),
                InlineKeyboardButton(
                    text="В меню",
                    callback_data="functions",
                    style='default',
                    icon_custom_emoji_id=get_icon("BACK")
                ),
            ]])
        )
        await callback.answer()
        return

    lines = [
        f"{emoji('CHART')} <b>Мои AI запросы</b> (последние {len(requests)})\n",
        f"{emoji('INFO')} Нажмите на запрос, чтобы посмотреть варианты снова.",
        "",
    ]
    await callback.message.edit_text(
        '\n'.join(lines),
        reply_markup=get_ai_history_keyboard(requests)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("ai_view_"))
async def ai_generator_view(callback: CallbackQuery, state: FSMContext):
    try:
        request_id = int(callback.data.split('_', 2)[2])
    except (IndexError, ValueError):
        await callback.answer('Некорректный id', show_alert=True)
        return
    req = await get_ai_request(request_id, callback.from_user.id)
    if not req:
        await callback.answer('Запрос не найден', show_alert=True)
        return
    await state.clear()
    variants = req['variants'] if isinstance(req['variants'], list) else []
    chosen = req.get('chosen_index')
    created = req['created_at']
    when = created.strftime('%d.%m.%Y %H:%M') if hasattr(created, 'strftime') else str(created)[:16]

    # Перешлём варианты текстом
    for i, v in enumerate(variants, 1):
        text = (v or {}).get('text') or ''
        title = (v or {}).get('title') or f'Вариант {i}'
        header = (
            f"{emoji('SPARK')} <b>Вариант {i}.</b> {escape(title)}\n"
            f"{emoji('CLOCK')} {when}\n\n"
        )
        first_chunk = header + text[: max(0, 4000 - len(header))]
        await callback.message.answer(first_chunk)
        rest = text[max(0, 4000 - len(header)):]
        while rest:
            await callback.message.answer(rest[:4000])
            rest = rest[4000:]

    # Текстовая сводка
    lines = [
        f"{emoji('EYE')} <b>Запрос #{request_id}</b> · {when}",
        f"{emoji('BOT')} Модель: <code>{escape(req.get('model') or LLM_MODEL)}</code>",
        f"{emoji('WRITE')} <b>Запрос:</b> <i>{escape(req['prompt'])}</i>",
        f"{emoji('CHECK')} Выбранный вариант: "
        f"<b>{('№' + str(chosen + 1)) if isinstance(chosen, int) else '—'}</b>",
    ]
    await callback.message.answer(
        '\n'.join(lines),
        reply_markup=get_ai_view_keyboard(request_id)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("ai_resend_"))
async def ai_generator_resend(callback: CallbackQuery, state: FSMContext):
    """Прислать конкретный файл из истории."""
    parts = callback.data.split('_')
    if len(parts) < 4:
        await callback.answer('Некорректный запрос', show_alert=True)
        return
    try:
        request_id = int(parts[2])
        idx = int(parts[3]) - 1
    except ValueError:
        await callback.answer('Некорректный запрос', show_alert=True)
        return
    req = await get_ai_request(request_id, callback.from_user.id)
    if not req or idx < 0:
        await callback.answer('Не найдено', show_alert=True)
        return
    variants = req['variants'] if isinstance(req['variants'], list) else []
    if idx >= len(variants):
        await callback.answer('Варианта нет', show_alert=True)
        return
    v = variants[idx]
    text = (v or {}).get('text') or ''
    title = (v or {}).get('title') or f'Вариант {idx + 1}'
    header = (
        f"{emoji('SPARK')} <b>Запрос #{request_id}, "
        f"Вариант {idx + 1}.</b> {escape(title)}\n\n"
    )
    try:
        first_chunk = header + text[: max(0, 4000 - len(header))]
        await callback.message.answer(first_chunk)
        rest = text[max(0, 4000 - len(header)):]
        while rest:
            await callback.message.answer(rest[:4000])
            rest = rest[4000:]
        await callback.answer('Готово')
    except Exception as e:
        logger.exception("ai_resend error")
        await callback.answer(f'Ошибка: {e}', show_alert=True)


@dp.callback_query(F.data.startswith("ai_copy_"))
async def ai_generator_copy_history(callback: CallbackQuery, state: FSMContext):
    """Прислать текстом выбранный вариант из истории."""
    try:
        request_id = int(callback.data.split('_', 2)[2])
    except (IndexError, ValueError):
        await callback.answer('Некорректный id', show_alert=True)
        return
    req = await get_ai_request(request_id, callback.from_user.id)
    if not req:
        await callback.answer('Запрос не найден', show_alert=True)
        return
    variants = req['variants'] if isinstance(req['variants'], list) else []
    # Шлём все три текстом подряд
    for i, v in enumerate(variants, 1):
        text = (v or {}).get('text') or ''
        title = (v or {}).get('title') or f'Вариант {i}'
        await callback.message.answer(
            f"--- Вариант {i}. {title} ---\n{text}"
        )
    await callback.answer('Готово — можно копировать')


@dp.callback_query(F.data == "ai_history_clear")
async def ai_generator_clear(callback: CallbackQuery, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Да, удалить всё",
        callback_data="ai_history_clear_confirm",
        style='danger',
        icon_custom_emoji_id=get_icon("DELETE")
    ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="ai_history",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    await callback.message.edit_text(
        f"{emoji('CROSS')} <b>Очистить всю историю AI-запросов?</b>\n\n"
        f"Сами файлы на сервере тоже исчезнут (или останутся — "
        f"это не критично).",
        reply_markup=builder.as_markup()
    )
    await callback.answer()


@dp.callback_query(F.data == "ai_history_clear_confirm")
async def ai_generator_clear_confirm(callback: CallbackQuery, state: FSMContext):
    n = await clear_ai_history(callback.from_user.id)
    await state.clear()
    await callback.message.edit_text(
        f"{emoji('CHECK')} <b>Готово.</b> Удалено записей: <b>{n}</b>.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Создать новый запрос",
                callback_data="ai_generator",
                style='primary',
                icon_custom_emoji_id=get_icon("WRITE")
            ),
            InlineKeyboardButton(
                text="В меню",
                callback_data="functions",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
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
            f"Если оставите «Без прокси» — аккаунт будет работать "
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
        f"Если оставите «Без прокси» — аккаунт будет работать "
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
        "Включен" if account.get('warming_enabled') else "Выключен"
    )

    # Доп. статистика прогрева: сколько циклов и когда последний раз активничал.
    warming_stats = ""
    cycles = account.get('warming_cycles') or 0
    if account.get('warming_enabled') and cycles:
        last_active = account.get('warming_last_active')
        last_str = (
            last_active.astimezone(MSK_TZ).strftime('%d.%m %H:%M')
            if last_active else "—"
        )
        warming_stats = (
            f"\n{emoji('CHART')} Циклов: <b>{cycles}</b>"
            f" • Последний: {last_str}"
        )
    elif cycles:
        warming_stats = f"\n{emoji('CHART')} Циклов отработано: <b>{cycles}</b>"

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
        f"{'Активен' if account['is_active'] else 'Неактивен'}\n"
        f"{emoji('FIRE')} Прогрев: {warming_status}{warming_stats}\n"
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
            "" if log['direction'] == 'sent'
            else "" if log['direction'] == 'received'
            else "" if log['direction'] == 'joined'
            else "" if log['direction'] == 'liked'
            else ""
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


# --- Анализ логов аккаунта (оценка риска бана) ---
def _risk_analysis_keyboard(account_id: int) -> InlineKeyboardMarkup:
    """Клавиатура после отчёта: переанализ / назад / в логи."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Переанализ",
            callback_data=f"analyze_risk_{account_id}",
            style='primary',
            icon_custom_emoji_id=get_icon("REFRESH"),
        ),
        InlineKeyboardButton(
            text="Открыть логи",
            callback_data=f"account_logs_{account_id}",
            style='default',
            icon_custom_emoji_id=get_icon("EYE"),
        ),
    )
    builder.row(InlineKeyboardButton(
        text="Назад к аккаунту",
        callback_data=f"manage_account_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("BACK"),
    ))
    return builder.as_markup()


@dp.callback_query(F.data.startswith("analyze_risk_"))
async def analyze_risk_handler(callback: CallbackQuery):
    """Кнопка «Анализ риска бана» из карточки аккаунта.

    Сценарий:
      1) Проверяем владельца.
      2) Тянем 50 последних логов + историю флудов.
      3) Зовём LLM в режиме «эксперт по безопасности Telegram».
      4) Если LLM недоступна — отдаём эвристический отчёт.
    """
    parts = callback.data.split("_")
    # data = "analyze_risk_<id>"
    if len(parts) < 3:
        await callback.answer("Некорректный запрос", show_alert=True)
        return
    try:
        account_id = int(parts[2])
    except ValueError:
        await callback.answer("Некорректный account_id", show_alert=True)
        return

    account = await get_account(account_id)
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return

    # 1) Сразу показываем «думаю…» в том же сообщении (edit_text),
    #    чтобы пользователь видел, что бот работает, а не висит.
    thinking = await callback.message.edit_text(
        f"{emoji('LOADING')} <b>Анализирую логи аккаунта…</b>\n\n"
        f"{emoji('PHONE')} <code>{escape(account['phone'])}</code>\n"
        f"{emoji('AI')} Модель: <i>эксперт по безопасности Telegram</i>\n\n"
        f"{emoji('INFO')} Беру последние 50 действий и историю "
        f"FloodWait за 7 дней, считаю частоту, время суток, разнообразие. "
        f"Это может занять несколько секунд.",
        reply_markup=None,
    )
    await callback.answer()

    # 2) Реальный анализ
    result = await analyze_account_logs_security(
        account_id=account_id,
        user_id=callback.from_user.id,
    )

    # 3) Собираем заголовок отчёта + тело
    stats = result.get('stats') or {}
    flood = result.get('flood') or {}
    source = result.get('source') or 'heuristic'
    src_label = (
        "LLM-эксперт" if source == 'llm' else "локальная эвристика "
        "(LLM недоступна)"
    )
    total = stats.get('total', 0)
    unique_chats = len(stats.get('unique_chats') or set())
    span = stats.get('time_span_hours', 0.0) or 0.0
    flood1h = flood.get('last_1h_count', 0)
    flood24h = flood.get('last_24h_count', 0)
    flood7d = flood.get('last_7d_count', 0)

    header = (
        f"{emoji('STATS')} <b>Анализ риска бана</b>\n"
        f"{emoji('PHONE')} <code>{escape(account['phone'])}</code>\n"
        f"{emoji('CHART')} "
        f"Логов: <b>{total}</b> · Чатов: <b>{unique_chats}</b> · "
        f"Окно: <b>{span:.1f} ч</b>\n"
        f"{emoji('TIME_PAST')} FloodWait: "
        f"<b>{flood1h}</b> за час / <b>{flood24h}</b> за сутки / "
        f"<b>{flood7d}</b> за 7 дней\n"
        f"{emoji('AI')} Источник: <i>{escape(src_label)}</i>\n\n"
    )
    body = (result.get('text') or '').strip() or (
        "Не удалось получить отчёт. Попробуй ещё раз."
    )

    # 4) Режем по 4000 символов — лимит Telegram.
    full = header + body
    chunks: List[str] = []
    while full:
        if len(full) <= 4000:
            chunks.append(full)
            break
        # режем по ближайшему переводу строки рядом с границей 4000
        cut = full.rfind('\n', 0, 4000)
        if cut < 1000:
            cut = 4000
        chunks.append(full[:cut])
        full = full[cut:]

    # 5) Первое сообщение — с клавиатурой; остальные — без.
    try:
        await thinking.edit_text(
            chunks[0], reply_markup=_risk_analysis_keyboard(account_id)
        )
    except Exception:
        # если edit_text сорвётся (например, текст совпадает) — отправим новое
        await callback.message.answer(
            chunks[0], reply_markup=_risk_analysis_keyboard(account_id)
        )
    for extra in chunks[1:]:
        await callback.message.answer(extra)


@dp.callback_query(F.data.startswith("toggle_warming_"))
async def toggle_warming(callback: CallbackQuery):
    """Включение/выключение прогрева. При включении — сначала
    генерируем план через LLM, показывая «Думаю... {время}»,
    и только после подтверждения — запускаем воркер.
    """
    account_id = int(callback.data.split("_")[2])
    account = await get_account(account_id)

    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return

    new_state = not account.get('warming_enabled', False)

    # ============ ВЫКЛЮЧЕНИЕ ============
    if not new_state:
        await update_account_warming(account_id, False)
        await stop_warming(account_id)
        # Деактивируем активные планы
        try:
            await deactivate_warming_plans(account_id)
        except Exception:
            pass
        await callback.answer("Прогрев выключен", show_alert=True)
        await manage_account(callback)
        return

    # ============ ВКЛЮЧЕНИЕ ============
    # Шаг 1: сразу отвечаем «Думаю...» и обновляем сообщение по таймеру.
    started = time.monotonic()
    try:
        await callback.message.edit_text(
            f"{emoji('BRAIN')} <b>Готовлю план прогрева</b>\n\n"
            f"{emoji('HOURGLASS')} Думаю… <code>0.0 с</code>",
            reply_markup=None
        )
    except Exception:
        pass
    await callback.answer("Готовлю план…")

    chat_id = callback.message.chat.id
    msg_id = callback.message.message_id

    # Шаг 2: фоновая задача, которая обновляет «Думаю... {время}»
    indicator_stop = asyncio.Event()

    async def _indicator():
        last_text = ''
        while not indicator_stop.is_set():
            elapsed = time.monotonic() - started
            if elapsed < 1.0:
                t_str = f"{elapsed:.1f} с"
            elif elapsed < 60.0:
                t_str = f"{int(elapsed)} с"
            else:
                mins = int(elapsed // 60)
                secs = int(elapsed % 60)
                t_str = f"{mins} мин {secs} с"
            text = (
                f"{emoji('BRAIN')} <b>Готовлю план прогрева</b>\n\n"
                f"{emoji('HOURGLASS')} Думаю… <code>{t_str}</code>"
            )
            if text != last_text:
                try:
                    await bot.edit_message_text(
                        text=text, chat_id=chat_id, message_id=msg_id
                    )
                    last_text = text
                except Exception:
                    return
            try:
                await asyncio.wait_for(indicator_stop.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass

    indicator_task = asyncio.create_task(_indicator())

    # Шаг 3: генерируем план через LLM.
    try:
        result = await generate_warming_plan_llm(
            account, callback.from_user.id, duration_hours=12
        )
    except Exception as ex:
        indicator_stop.set()
        try:
            await indicator_task
        except Exception:
            pass
        logger.exception("generate_warming_plan_llm failed")
        try:
            await bot.edit_message_text(
                f"{emoji('CROSS')} <b>Не удалось подготовить план</b>\n\n"
                f"Ошибка: <code>{escape(str(ex)[:200])}</code>\n\n"
                f"Попробуйте ещё раз через несколько секунд.",
                chat_id=chat_id,
                message_id=msg_id,
                reply_markup=InlineKeyboardBuilder().row(
                    InlineKeyboardButton(
                        text="Назад",
                        callback_data=f"manage_account_{account_id}",
                        style='default',
                        icon_custom_emoji_id=get_icon("BACK")
                    )
                ).as_markup()
            )
        except Exception:
            pass
        return
    finally:
        indicator_stop.set()
        try:
            await asyncio.wait_for(indicator_task, timeout=2.0)
        except Exception:
            pass

    # Шаг 4: сохраняем план в БД (как НЕактивный — станет активным
    # только после подтверждения юзером).
    plan = result['plan']
    narrative = result['narrative']
    plan_id = await save_warming_plan(account_id, plan, narrative)

    # Шаг 5: рендерим план юзеру.
    plan_text = _format_warming_plan_message(plan, narrative)
    elapsed = result.get('elapsed_sec', time.monotonic() - started)
    if elapsed < 1.0:
        e_str = f"{elapsed:.1f} с"
    elif elapsed < 60.0:
        e_str = f"{int(elapsed)} с"
    else:
        mins = int(elapsed // 60)
        secs = int(elapsed % 60)
        e_str = f"{mins} мин {secs} с"
    plan_text = (
        f"{emoji('BRAIN')} <i>План сгенерирован за {e_str}</i>\n\n"
        + plan_text
    )

    try:
        await bot.edit_message_text(
            text=plan_text[:4000],
            chat_id=chat_id,
            message_id=msg_id,
            reply_markup=_warming_plan_keyboard(plan_id, account_id)
        )
    except Exception:
        # Если не влезло в edit — отправляем отдельным сообщением
        try:
            await bot.send_message(
                chat_id,
                plan_text[:4000],
                reply_markup=_warming_plan_keyboard(plan_id, account_id)
            )
        except Exception:
            pass


@dp.callback_query(F.data.startswith("confirm_warming_"))
async def confirm_warming_plan(callback: CallbackQuery):
    """Юзер подтвердил план — реально включаем прогрев и запускаем воркер."""
    account_id = int(callback.data.split("_")[2])
    account = await get_account(account_id)

    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return

    plan = await get_active_warming_plan(account_id)
    if not plan:
        await callback.answer(
            "План не найден — сгенерируйте заново", show_alert=True
        )
        return

    await update_account_warming(account_id, True)
    started = await start_warming(account_id, callback.from_user.id)
    if not started:
        status_text = "уже запущен"
    else:
        status_text = "включен"

    plan_narrative = (plan.get('narrative') or '').strip()
    extra = (
        f"\n\n{emoji('BRAIN')} <b>Стратегия ИИ:</b>\n"
        f"<i>{escape(plan_narrative[:400])}</i>"
        if plan_narrative else ""
    )
    try:
        await bot.send_message(
            callback.from_user.id,
            f"{emoji('FIRE')} <b>Прогрев запущен по плану ИИ</b>\n\n"
            f"Аккаунт: <code>{account['phone']}</code>\n"
            f"Статус: <b>{status_text}</b>\n"
            f"Окно плана: <b>12 часов</b>{extra}"
        )
    except Exception:
        pass
    await callback.answer(f"Прогрев {status_text}", show_alert=True)
    await manage_account(callback)


@dp.callback_query(F.data.startswith("regen_warming_"))
async def regenerate_warming_plan(callback: CallbackQuery):
    """Перегенерировать план — просто вызываем toggle_warming заново
    (он заново покажет «Думаю...» и сгенерирует свежий план)."""
    account_id = int(callback.data.split("_")[2])
    # Деактивируем предыдущий план, чтобы воркер не цеплял его.
    try:
        await deactivate_warming_plans(account_id)
    except Exception:
        pass
    # Имитируем повторное нажатие «Включить прогрев»
    callback.data = f"toggle_warming_{account_id}"
    await toggle_warming(callback)


@dp.callback_query(F.data.startswith("show_warming_plan_"))
async def show_warming_plan(callback: CallbackQuery):
    """Показать последний (активный или недавний) план прогрева."""
    account_id = int(callback.data.split("_")[3])
    account = await get_account(account_id)
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    latest = await get_latest_warming_plan(account_id)
    if not latest:
        await callback.answer(
            "У аккаунта пока нет сгенерированных планов",
            show_alert=True
        )
        return
    plan = latest.get('plan') or {}
    narrative = latest.get('narrative') or ''
    text = _format_warming_plan_message(plan, narrative)
    active_mark = (
        f"\n{emoji('CHECK')} <b>Статус:</b> активен"
        if latest.get('is_active')
        else f"\n{emoji('CROSS')} <b>Статус:</b> неактивен"
    )
    text = (
        f"{emoji('CLIPBOARD')} <b>Последний план прогрева</b>"
        f"{active_mark}\n\n" + text
    )
    builder = InlineKeyboardBuilder()
    if not latest.get('is_active') and not account.get('warming_enabled'):
        builder.row(InlineKeyboardButton(
            text="Сгенерировать новый план",
            callback_data=f"toggle_warming_{account_id}",
            style='success',
            icon_custom_emoji_id=get_icon("REFRESH")
        ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data=f"manage_account_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    try:
        await callback.message.edit_text(
            text[:4000], reply_markup=builder.as_markup()
        )
    except Exception:
        pass
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_account_"))
async def delete_account_handler(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[2])

    # Перед удалением аккаунта — гасим прогрев
    await stop_warming(account_id)

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
            'active': '', 'stopped': '',
            'completed': '', 'scheduled': ''
        }.get(bc['status'], 'ℹ')
        type_icon = "" if btype == 'dm' else ""
        
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
        status = "" if resp['is_active'] else ""
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
        f"{'Активен' if responder['is_active'] else 'Остановлен'}\n"
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
            status = "" if p.get('is_active') else ""
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
        f"Логин: <code>{escape(proxy['username'])}</code>\n"
        f"Пароль: <code>{'•' * len(proxy['password'])}</code>\n"
        if proxy.get('username') else ""
    )
    label = proxy.get('label') or '—'

    text = (
        f"{emoji('LINK')} <b>Прокси</b>\n\n"
        f"Подпись: {escape(label)}\n"
        f"Тип: <code>{proxy['proxy_type']}</code>\n"
        f"Адрес: <code>{proxy['host']}:{proxy['port']}</code>\n"
        f"{auth}"
        f"Создан: "
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
            "Прокси привязан. При следующем подключении вступит в силу.",
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
    os.makedirs("media/ai", exist_ok=True)
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

        # Восстанавливаем прогрев по всем аккаунтам, у которых он был включен
        warming_accounts = await conn.fetch(
            "SELECT id, user_id FROM accounts "
            "WHERE is_active = TRUE AND warming_enabled = TRUE"
        )
        for acc in warming_accounts:
            try:
                await start_warming(acc['id'], acc['user_id'])
                logger.info(
                    f"on_startup: прогрев восстановлен для "
                    f"account_id={acc['id']}"
                )
            except Exception as ex:
                logger.warning(
                    f"on_startup: не удалось запустить прогрев для "
                    f"account_id={acc['id']}: {ex}"
                )

    asyncio.create_task(check_scheduled_broadcasts())
    asyncio.create_task(task_queue_worker())

async def main():
    await on_startup()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
