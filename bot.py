import asyncio
import base64
import csv
import hashlib
import io
import json
import logging
import os
import random
import re
import tempfile
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from html import escape
from urllib.parse import urlparse, parse_qs

import aiohttp
import asyncpg
import pytz
import anthropic
import socks
from cryptography.fernet import Fernet, InvalidToken
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.methods import DeleteWebhook
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup,
    InlineKeyboardButton, WebAppInfo, FSInputFile, BufferedInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from telethon import TelegramClient, events, Button
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError, BadRequestError, RPCError,
)
from telethon.sessions import StringSession
from telethon.tl.types import User, ReactionEmoji
from telethon.tl.functions.channels import (
    CreateChannelRequest, JoinChannelRequest
)
from telethon.tl.functions.messages import (
    ImportChatInviteRequest, DeleteHistoryRequest, SendReactionRequest
)
from telethon.tl.functions.account import (
    UpdateStatusRequest, GetPrivacyRequest, UpdateProfileRequest
)
from telethon.tl.functions.photos import (
    DeletePhotosRequest, GetUserPhotosRequest, UploadProfilePhotoRequest
)
from telethon.tl.functions.users import GetFullUserRequest
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
    InputPeerChannel, InputPeerChat
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

# --- Платежи: СБП (Platega) ---
# Данные магазина Platega прописаны в открытом виде по требованию заказчика.
PLATEGA_API = "https://app.platega.io"
PLATEGA_MERCHANT_ID = "39cd4a01-a435-4c17-bff2-19519d043d6b"
# ВНИМАНИЕ: API-ключ пришёл в замаскированном виде — замените на реальный секрет.
PLATEGA_SECRET = "sh7BhDGLhBnqJxECAGBGkSd68hZ9Xdaqdb1Wmu1SXMbIAR6alPk5F9AyV34VRCx2AChkMoNvTkTEJ1WJo9PFb4aPsCbcwZQRVsl1"
# СБП (QR-код) + Sberpay.
PLATEGA_PAYMENT_METHOD_SBP = 2
# Pro-подписка по СБП: 40₽/мес.
PRO_PRICE_RUB = 40
# Курс пополнения баланса: 1 USDT = 80₽
TOPUP_RUB_PER_USDT = 80

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

# Встроенная конфигурация остаётся безопасным fallback: пока администратор
# не включил собственный базовый API, все запросы используют эти значения.
LLM_FALLBACK_MODELS = dict(LLM_MODELS)
LLM_FALLBACK_DEFAULT_MODEL = LLM_DEFAULT_MODEL
GLOBAL_LLM_RUNTIME: Dict[str, Any] = {
    'api_id': None,
    'name': 'Встроенный API',
    'base_url': LLM_BASE_URL,
    'api_key': LLM_API_KEY,
    'models': dict(LLM_MODELS),
    'default_model': LLM_DEFAULT_MODEL,
}
GLOBAL_LLM_RUNTIME_READY = False

# --- Чат с нейросетями ---
AI_CHAT_FREE_DAILY_LIMIT = 3
AI_CHAT_PRO_DAILY_LIMIT = 10          # чуть строже
AI_CHAT_MAX_DAILY_LIMIT = int(os.getenv('AI_CHAT_MAX_DAILY_LIMIT') or '40')
# Admin can override per-user limit via DB column ai_chat_limit_override
AI_CHAT_HISTORY_MESSAGES_LIMIT = 16  # 8 пар user/assistant
AI_CHAT_HISTORY_CONTENT_LIMIT = 2000
AI_CHAT_SYSTEM_PROMPT = (
    'Ты полезный русскоязычный AI-ассистент в Telegram. '
    'Отвечай по существу, дружелюбно и безопасно. '
    'Если данных недостаточно, задай уточняющий вопрос. '
    'Не выдумывай факты и не выдавай себя за человека.'
)

# Пять независимых разделов, для которых администратор может закрепить
# одно медиа Telegram (file_id хранится в БД, файл не копируется на диск).
MEDIA_SECTIONS = {
    'welcome': 'Приветствие',
    'account_manager': 'Менеджер аккаунтов',
    'functions': 'Функции',
    'subscription': 'Подписка',
    'ai': 'AI-раздел',
}


def _llm_fernet() -> Fernet:
    """Ключ шифрования токенов пользовательских AI API.

    В продакшене рекомендуется задать LLM_CONFIG_ENCRYPTION_KEY (Fernet).
    Без него используется детерминированный ключ от BOT_TOKEN, чтобы старые
    развёртывания не теряли функциональность после миграции.
    """
    configured = (os.getenv('LLM_CONFIG_ENCRYPTION_KEY') or '').strip()
    if configured:
        try:
            return Fernet(configured.encode())
        except Exception as ex:
            logger.warning('Invalid LLM_CONFIG_ENCRYPTION_KEY: %s', ex)
    seed = ('vest-game-soft:llm:' + (BOT_TOKEN or '')).encode()
    key = base64.urlsafe_b64encode(hashlib.sha256(seed).digest())
    return Fernet(key)


def _encrypt_llm_secret(value: str) -> str:
    return _llm_fernet().encrypt(value.encode('utf-8')).decode('ascii')


def _decrypt_llm_secret(value: str) -> str:
    try:
        return _llm_fernet().decrypt(value.encode('ascii')).decode('utf-8')
    except (InvalidToken, ValueError, TypeError, UnicodeDecodeError):
        # Позволяем мягко мигрировать ранее сохранённые значения, если они
        # были записаны до включения шифрования.
        return value

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
chat_creation_stop_flags: Dict[int, bool] = {}
chat_creation_tasks: Dict[int, asyncio.Task] = {}
autosub_tasks: Dict[int, asyncio.Task] = {}
autosub_stop_flags: Dict[int, bool] = {}
autolike_tasks: Dict[int, asyncio.Task] = {}
autolike_stop_flags: Dict[int, bool] = {}
# Нейрокомментинг: один фоновый воркер на сохранённую конфигурацию.
neurocomment_tasks: Dict[int, asyncio.Task] = {}
neurocomment_stop_flags: Dict[int, bool] = {}
neurocomment_event_locks: Dict[int, asyncio.Lock] = {}
delete_messages_stop_flags: Dict[int, bool] = {}
script_run_locks: Dict[int, asyncio.Lock] = {}
script_tasks: Dict[int, asyncio.Task] = {}
script_stop_flags: Dict[int, bool] = {}
SCRIPT_CYCLE_DELAY_SECONDS = 5
SCRIPT_RETRY_DELAY_SECONDS = 10
# Не допускаем одновременную ручную и плановую проверку одного аккаунта.
spam_check_locks: Dict[int, asyncio.Lock] = {}

# --- Проверка ограничений @SpamBot ---
SPAM_BLOCK_BOT_USERNAME = '@SpamBot'
SPAM_BLOCK_CHECK_INTERVAL_SECONDS = 12 * 60 * 60
SPAM_BLOCK_SCHEDULER_POLL_SECONDS = 15 * 60
SPAM_BLOCK_REQUEST_TIMEOUT_SECONDS = 35
SPAM_BLOCK_RESPONSE_LIMIT = 1600

# --- Регулярный мониторинг Telegram-аккаунтов ---
ACCOUNT_MONITORING_POLL_SECONDS = 5 * 60
ACCOUNT_VALIDITY_CHECK_INTERVAL_SECONDS = 60 * 60
ACCOUNT_AI_ANALYSIS_INTERVAL_SECONDS = 7 * 24 * 60 * 60
ACCOUNT_VALIDITY_BATCH_SIZE = 25
ACCOUNT_AI_ANALYSIS_BATCH_SIZE = 5
account_monitoring_locks: Dict[int, asyncio.Lock] = {}

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
    "STAR": ("⭐", "5890848474563352982"),
    "OK": ("✅", "5870633910337015697"),
    "RIGHT": ("▶", "6041731551845159060"),
    "BRAIN": ("🧠", "6030400221232501136"),
    "CLIPBOARD": ("📋", "5769289093221454192"),
    "HOURGLASS": ("⏳", "5775896410780079073"),
    "MOON": ("🌙", "5983150113483134607"),
    "NOTE": ("📝", "5870753782874246579"),
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


# --- Санитайзер стилей кнопок ---
# Telegram Bot API принимает у InlineKeyboardButton.style только
# 'primary', 'success' и 'danger'. Любое другое значение (например
# 'default' или 'destructive') даёт ошибку Bad Request, и всё сообщение
# не отправляется. Именно из-за этого молча не работал /admin.
VALID_BUTTON_STYLES = {'primary', 'success', 'danger'}

_STYLE_ALIASES = {
    'destructive': 'danger',
    'red': 'danger',
    'green': 'success',
    'blue': 'primary',
    'default': None,
    'secondary': None,
    'normal': None,
}


def _sanitize_button_style(value):
    """Возвращает корректный для Telegram style либо None."""
    if value is None:
        return None
    style = str(value).strip().lower()
    if style in VALID_BUTTON_STYLES:
        return style
    if style in _STYLE_ALIASES:
        return _STYLE_ALIASES[style]
    logger.warning(f"Unknown button style: {value!r} - style omitted")
    return None


# Оборачиваем InlineKeyboardButton один раз, чтобы правило работало во
# всём боте (кнопок ~250, править каждую вручную смысла нет).
_OriginalInlineKeyboardButton = InlineKeyboardButton

# Обратный индекс «символ обычного эмодзи → custom_emoji_id».
# Используется в обёртке ниже, чтобы убрать обычные эмодзи из текста
# кнопок (Telegram и так рисует premium-иконку через icon_custom_emoji_id,
# дублирование выглядит как «👥 👥 Менеджер аккаунтов»).
_EMOJI_SYMBOL_TO_ID: Dict[str, str] = {}
for _sym, _eid in (EMOJI[k] for k in EMOJI):
    # Один символ может встречаться у нескольких ключей (например, "▶" →
    # PLAY и RIGHT). Сохраняем первый встреченный id — для кнопки важна
    # именно иконка, а не её «семантический» ключ.
    _EMOJI_SYMBOL_TO_ID.setdefault(_sym, _eid)


_EMOJI_FORMATTING_CODEPOINTS = {
    0x200D,  # zero-width joiner in compound emoji
    0x20E3,  # keycap combining mark
    0xFE0E,  # text presentation selector
    0xFE0F,  # emoji presentation selector
}


def _strip_button_emojis(text: str) -> Tuple[str, Optional[str]]:
    """Удаляет обычные эмодзи из текста кнопки.

    Telegram получает премиум-иконку через ``icon_custom_emoji_id``.
    Обычные emoji в ``text`` больше не оставляем: они могут отображаться
    как цветные символы или как ромб с вопросительным знаком на клиенте.
    Если первый найденный символ есть в ``EMOJI``, его premium-id будет
    использован автоматически, когда кнопка не задала иконку сама.
    """
    if not text:
        return text, None

    clean_chars: List[str] = []
    found_id: Optional[str] = None
    for char in str(text):
        codepoint = ord(char)
        is_emoji = (
            _is_emoji_char(char)
            or codepoint in _EMOJI_FORMATTING_CODEPOINTS
        )
        if is_emoji:
            if found_id is None:
                found_id = _EMOJI_SYMBOL_TO_ID.get(char)
            continue
        clean_chars.append(char)

    # Удаление emoji часто оставляет двойные пробелы, например
    # ``"🤖 С ИИ ✅"`` → ``"  С ИИ  "``.
    clean_text = re.sub(r'\s+', ' ', ''.join(clean_chars)).strip()
    return clean_text, found_id


# Диапазоны codepoints, которые Unicode относит к эмодзи.
# Покрывают и «классические» эмодзи-блоки (U+1F000+), и «символьные»
# типа ⏰ (U+23F0), ▶ (U+25B6), ✍ (U+270D), ❤ (U+2764), ◁ (U+25C1).
_EMOJI_RANGES = (
    (0x2300, 0x27BF),     # символьные: ⏰, ▶, ◁, ✍, ❤ и пр.
    (0x1F000, 0x1FFFF),   # основной блок эмодзи
)


def _is_emoji_char(ch: str) -> bool:
    """Грубая проверка: похож ли символ на эмодзи?

    Проверяем два условия:
      1. Символ лежит в одном из известных Unicode-диапазонов эмодзи.
      2. Если диапазон общий (U+2300-U+27BF), то дополнительно смотрим
         категорию: берём только 'So' (Symbol, other) и 'Sm' (Symbol,
         math) — иначе зацепим крестики-палочки, плюсы, кавычки и пр.
    """
    import unicodedata
    cp = ord(ch)
    if cp >= 0x1F000:
        return True
    if 0x2300 <= cp <= 0x27BF:
        cat = unicodedata.category(ch)
        if cat in ('So', 'Sm'):
            return True
    return False


class InlineKeyboardButton(_OriginalInlineKeyboardButton):  # noqa: F811
    def __init__(self, **kwargs):
        if 'style' in kwargs:
            style = _sanitize_button_style(kwargs.get('style'))
            if style is None:
                kwargs.pop('style', None)
            else:
                kwargs['style'] = style

        # Убираем обычные emoji из любого места текста кнопки. Telegram
        # рисует только premium-иконку через icon_custom_emoji_id.
        text = kwargs.pop('text', None)
        if text:
            new_text, found_id = _strip_button_emojis(text)
            kwargs['text'] = new_text
            # Если иконка не задана явно — подставим premium-id из словаря.
            if found_id is not None and not kwargs.get('icon_custom_emoji_id'):
                kwargs['icon_custom_emoji_id'] = found_id

        if kwargs.get('icon_custom_emoji_id') in (None, ''):
            kwargs.pop('icon_custom_emoji_id', None)
        super().__init__(**kwargs)


# После прикрепления медиа Telegram считает текст caption, поэтому
# edit_text для такого сообщения возвращает BadRequest. Перехватываем этот
# случай централизованно: удаляем старое медиа-сообщение и отправляем новый
# текстовый экран с той же клавиатурой. Остальные ошибки не скрываем.
_original_message_edit_text = Message.edit_text


async def _safe_message_edit_text(self, *args, **kwargs):
    try:
        return await _original_message_edit_text(self, *args, **kwargs)
    except TelegramBadRequest as ex:
        error_text = str(ex).lower()
        if 'no text' not in error_text and 'can\'t be edited' not in error_text:
            raise
        try:
            await self.delete()
        except Exception:
            pass
        text = kwargs.get('text')
        if text is None and args:
            text = args[0]
            args = args[1:]
        return await self.answer(text or '', *args, **kwargs)


Message.edit_text = _safe_message_edit_text

# --- Состояния FSM ---
class AccountStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_2fa = State()
    waiting_for_proxy_choice = State()


class FingerprintRegenStates(StatesGroup):
    """Состояния переавторизации аккаунта с новым отпечатком устройства.

    Используются в flow «Сменить отпечаток»: после подтверждения бот
    отправляет код на телефон, юзер вводит его (и, если включена 2FA,
    пароль). В результате получаем НОВЫЙ auth_key и НОВЫЙ session_string
    — только так Telegram увидит смену устройства в «Активных сессиях».
    """
    waiting_for_code = State()
    waiting_for_2fa = State()

class BroadcastStates(StatesGroup):
    waiting_for_account = State()
    selecting_chats = State()
    waiting_for_delay = State()
    waiting_for_count = State()
    waiting_for_message = State()
    preview = State()
    waiting_for_button_text = State()
    waiting_for_button_url = State()

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
    waiting_for_button_text = State()
    waiting_for_button_url = State()

class AutoResponderStates(StatesGroup):
    waiting_for_account = State()
    waiting_for_trigger = State()
    waiting_for_response = State()
    preview = State()

class AdminStates(StatesGroup):
    waiting_for_broadcast_message = State()
    waiting_for_gift_user_id = State()
    waiting_for_gift_days = State()
    waiting_for_revoke_user_id = State()
    waiting_for_user_lookup_id = State()
    waiting_for_media = State()


class AdminLLMConfigStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_base_url = State()
    waiting_for_api_key = State()
    waiting_for_model_api_name = State()
    waiting_for_model_display_name = State()


class BalanceStates(StatesGroup):
    waiting_for_method = State()
    waiting_for_amount = State()       # USDT (Crypto Pay)
    waiting_for_amount_rub = State()   # рубли (СБП)


class UserLLMConfigStates(StatesGroup):
    waiting_for_base_url = State()
    waiting_for_api_key = State()
    waiting_for_models = State()


class AIChatStates(StatesGroup):
    waiting_for_message = State()


class BroadcastTemplateStates(StatesGroup):
    waiting_for_name = State()

class ParsingStates(StatesGroup):
    waiting_for_account = State()
    waiting_for_chat = State()
    waiting_for_mode = State()

class JoinStates(StatesGroup):
    waiting_for_account = State()
    waiting_for_file = State()
    waiting_for_delay = State()
    preview = State()


class AutoSubStates(StatesGroup):
    waiting_for_account = State()


class ChatCreationStates(StatesGroup):
    waiting_for_account = State()
    waiting_for_count = State()
    waiting_for_title = State()
    preview = State()

class AutoLikeStates(StatesGroup):
    waiting_for_account = State()
    selecting_chats = State()
    waiting_for_reaction = State()
    waiting_for_delay = State()
    preview = State()

class NeuroCommentStates(StatesGroup):
    waiting_for_account = State()
    selecting_channels = State()
    choosing_mode = State()
    choosing_model = State()
    collecting_templates = State()
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

class ScriptStates(StatesGroup):
    waiting_for_name = State()
    choosing_account = State()
    waiting_for_bot_url = State()
    choosing_captcha = State()
    choosing_button = State()
    confirming_step = State()

class LLMStates(StatesGroup):
    choosing_model = State()      # выбор модели перед вводом промта
    waiting_for_prompt = State()  # ждём текст задачи
    choosing_variant = State()    # показаны 3 варианта, ждём выбор/реген


class ProfileEditStates(StatesGroup):
    editing = State()
    waiting_for_first_name = State()
    waiting_for_last_name = State()
    waiting_for_about = State()
    waiting_for_avatar = State()
    waiting_for_ai_prompt = State()

# ============================================================
#  Per-account AI-автоответчик (живёт на аккаунте, а не в боте)
# ============================================================
class AccountAIResponderStates(StatesGroup):
    setting_system  = State()  # ждём новый system_prompt (личность ИИ) для аккаунта
    setting_model   = State()  # ждём выбор модели (название из LLM_MODELS)

# Режимы per-account AI-автоответчика. Всего два, как и просил юзер:
#   'off' — выключен (без ИИ)
#   'ai'  — ИИ отвечает на входящие ЛС на аккаунте
ACCT_AR_MODE_OFF = 'off'
ACCT_AR_MODE_AI  = 'ai'

ACCT_AR_MODE_LABELS = {
    ACCT_AR_MODE_OFF: '🔕 Без ИИ (выключен)',
    ACCT_AR_MODE_AI:  '🤖 С ИИ',
}

# Личность ИИ по умолчанию. Используется, если system_prompt пустой.
ACCT_AR_DEFAULT_SYSTEM_PROMPT = (
    'Ты дружелюбный Telegram-бот, ведущий диалог в личных сообщениях с пользователем. '
    'Отвечай кратко, по-человечески, по существу. '
    'Можешь использовать эмодзи, но без перебора. '
    'Если не знаешь ответа — честно признайся. '
    'Не выдумывай факты, не выдавай себя за реального человека или компанию.'
)

# Сколько последних пар user+assistant держать в контексте (на каждого собеседника).
ACCT_AR_HISTORY_PAIRS = 10

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

        # Миграция: пер-аккаунтный «отпечаток устройства» (A3).
        # Telethon-параметры device_model / system_version / app_version /
        # lang_code / system_lang_code. Хранятся в БД, чтобы можно было
        # в любой момент поменять и пересоздать Telethon-клиент.
        for _col in (
            'device_model TEXT',
            'system_version TEXT',
            'app_version TEXT',
            'lang_code TEXT',
            'system_lang_code TEXT',
            'fingerprint_updated_at TIMESTAMP',
            'telegram_premium BOOLEAN DEFAULT FALSE',
            'validation_status TEXT DEFAULT \'unknown\'',
            'last_validated_at TIMESTAMP',
        ):
            try:
                await conn.execute(
                    f'ALTER TABLE accounts ADD COLUMN IF NOT EXISTS {_col}'
                )
            except Exception:
                pass

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
        
        # Нейрокомментинг: мониторинг выбранных каналов и публикация
        # комментариев от выбранного аккаунта.
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS neurocomment_configs (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
                channel_ids TEXT[] NOT NULL,
                mode TEXT NOT NULL,
                model TEXT,
                message_variants JSONB NOT NULL DEFAULT '[]'::jsonb,
                delay_seconds INTEGER NOT NULL DEFAULT 60,
                is_active BOOLEAN NOT NULL DEFAULT FALSE,
                comments_sent INTEGER NOT NULL DEFAULT 0,
                errors_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                started_at TIMESTAMP,
                stopped_at TIMESTAMP
            )
        ''')
        try:
            await conn.execute(
                'ALTER TABLE neurocomment_configs ADD COLUMN IF NOT EXISTS model TEXT'
            )
        except Exception:
            pass
        try:
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_neurocomment_user_created '
                'ON neurocomment_configs (user_id, created_at DESC)'
            )
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_neurocomment_active '
                'ON neurocomment_configs (is_active, account_id)'
            )
        except Exception:
            pass

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

        # Сохранённые сценарии взаимодействия с Telegram-ботами.
        # Скрипт открывает бота через /start и нажимает одну выбранную
        # callback/text-кнопку. При каждом запуске меню загружается заново.
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_scripts (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                bot_url TEXT NOT NULL,
                bot_username TEXT NOT NULL,
                start_payload TEXT,
                button_row INTEGER NOT NULL,
                button_col INTEGER NOT NULL,
                button_text TEXT NOT NULL,
                button_kind TEXT NOT NULL,
                button_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
                steps JSONB NOT NULL DEFAULT '[]'::jsonb,
                captcha_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                is_public BOOLEAN NOT NULL DEFAULT FALSE,
                published_at TIMESTAMP,
                public_uses INTEGER NOT NULL DEFAULT 0,
                last_status TEXT NOT NULL DEFAULT 'never',
                last_error TEXT,
                last_run_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS script_runs (
                id BIGSERIAL PRIMARY KEY,
                script_id BIGINT REFERENCES user_scripts(id) ON DELETE CASCADE,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                account_id INTEGER REFERENCES accounts(id) ON DELETE SET NULL,
                status TEXT NOT NULL DEFAULT 'running',
                clicked_button TEXT,
                error TEXT,
                started_at TIMESTAMP DEFAULT NOW(),
                finished_at TIMESTAMP
            )
        ''')
        try:
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_user_scripts_user_created '
                'ON user_scripts (user_id, created_at DESC)'
            )
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_script_runs_script_started '
                'ON script_runs (script_id, started_at DESC)'
            )
        except Exception:
            pass
        try:
            await conn.execute(
                "ALTER TABLE user_scripts ADD COLUMN IF NOT EXISTS steps JSONB NOT NULL DEFAULT '[]'::jsonb"
            )
            await conn.execute(
                "ALTER TABLE user_scripts ADD COLUMN IF NOT EXISTS captcha_enabled BOOLEAN NOT NULL DEFAULT FALSE"
            )
            await conn.execute(
                "ALTER TABLE user_scripts ADD COLUMN IF NOT EXISTS is_public BOOLEAN NOT NULL DEFAULT FALSE"
            )
            await conn.execute(
                "ALTER TABLE user_scripts ADD COLUMN IF NOT EXISTS published_at TIMESTAMP"
            )
            await conn.execute(
                "ALTER TABLE user_scripts ADD COLUMN IF NOT EXISTS public_uses INTEGER NOT NULL DEFAULT 0"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_scripts_public "
                "ON user_scripts (is_public, published_at DESC)"
            )
        except Exception:
            pass

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

        # История диалогов и дневные лимиты отдельного «Чата с нейросетями».
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS ai_chat_sessions (
                user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
                history JSONB NOT NULL DEFAULT '[]'::jsonb,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS ai_chat_usage (
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                usage_date DATE NOT NULL,
                request_count INTEGER NOT NULL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (user_id, usage_date)
            )
        ''')
        try:
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_ai_chat_usage_date '
                'ON ai_chat_usage (usage_date, user_id)'
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
        # Список сообщений для рандомной рассылки (JSONB-массив объектов
        # {text, media}). Если заполнен — execute_*_broadcast будет
        # случайно выбирать одно из сообщений при каждой отправке.
        try:
            await conn.execute(
                "ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS message_texts JSONB DEFAULT '[]'::jsonb"
            )
        except:
            pass
        try:
            await conn.execute(
                "ALTER TABLE dm_broadcasts ADD COLUMN IF NOT EXISTS message_texts JSONB DEFAULT '[]'::jsonb"
            )
        except:
            pass
        try:
            await conn.execute(
                "ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS message_buttons JSONB DEFAULT '[]'::jsonb"
            )
            await conn.execute(
                "ALTER TABLE dm_broadcasts ADD COLUMN IF NOT EXISTS message_buttons JSONB DEFAULT '[]'::jsonb"
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
        # Персональные Anthropic-совместимые API пользователя. Токен
        # хранится только в зашифрованном виде и никогда не показывается.
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_llm_apis (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                base_url TEXT NOT NULL,
                api_key_ciphertext TEXT NOT NULL,
                models TEXT[] NOT NULL DEFAULT '{}',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        try:
            await conn.execute(
                'ALTER TABLE users ADD COLUMN IF NOT EXISTS llm_active_api_id BIGINT'
            )
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_user_llm_apis_user '
                'ON user_llm_apis (user_id, created_at DESC)'
            )
        except Exception:
            pass
        # Базовые AI API, которыми пользуются все пользователи без личного
        # API. Если активной записи нет, используется fallback из кода.
        # API-модель и подпись на кнопке хранятся раздельно: например,
        # `claude-3-5-sonnet` → «Claude Sonnet 3.5».
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS admin_llm_apis (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                base_url TEXT NOT NULL,
                api_key_ciphertext TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS admin_llm_models (
                id BIGSERIAL PRIMARY KEY,
                api_id BIGINT NOT NULL REFERENCES admin_llm_apis(id) ON DELETE CASCADE,
                api_model_name TEXT NOT NULL,
                display_name TEXT NOT NULL,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(api_id, api_model_name)
            )
        ''')
        try:
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_admin_llm_apis_active '
                'ON admin_llm_apis (is_active, updated_at DESC)'
            )
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_admin_llm_models_api_order '
                'ON admin_llm_models (api_id, sort_order, id)'
            )
        except Exception:
            pass

        # Состояние фоновой проверки аккаунтов: валидность каждый час и
        # AI-анализ активности раз в неделю.
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS account_monitoring_state (
                account_id INTEGER PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                last_validity_check_at TIMESTAMP,
                last_validity_status TEXT,
                last_validity_error TEXT,
                last_ai_analysis_at TIMESTAMP,
                last_ai_analysis_source TEXT,
                last_ai_analysis_text TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        try:
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_account_monitoring_validity_due '
                'ON account_monitoring_state (last_validity_check_at)'
            )
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_account_monitoring_analysis_due '
                'ON account_monitoring_state (last_ai_analysis_at)'
            )
        except Exception:
            pass

        # Медиа для пяти экранов админ-панели.
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS section_media (
                section TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                media_type TEXT NOT NULL,
                caption TEXT DEFAULT '',
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS broadcast_templates (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                message_variants JSONB NOT NULL DEFAULT '[]'::jsonb,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        try:
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_broadcast_templates_user '
                'ON broadcast_templates (user_id, created_at DESC)'
            )
        except Exception:
            pass
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS autosub_configs (
                account_id INTEGER PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        # Внутренний баланс и одноразовые Crypto Pay пополнения.
        try:
            await conn.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS balance NUMERIC(12, 2) "
                "NOT NULL DEFAULT 0"
            )
        except Exception:
            pass
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS balance_invoices (
                invoice_id BIGINT PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                amount_usdt NUMERIC(12, 6) NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TIMESTAMP DEFAULT NOW(),
                paid_at TIMESTAMP
            )
        ''')

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

        # Периодическая проверка ограничений аккаунта через @SpamBot.
        # Настройки хранятся отдельно от accounts, чтобы у уже добавленных
        # аккаунтов проверка по умолчанию была включена без миграции строк.
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS account_spam_checks (
                account_id INTEGER PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                notify_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                last_checked_at TIMESTAMP,
                last_status TEXT,
                last_response TEXT,
                last_error TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        try:
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_account_spam_checks_due '
                'ON account_spam_checks (is_enabled, last_checked_at)'
            )
        except Exception:
            pass

        # Дедлайны FloodWait для длительных действий. Это даёт воркеру
        # создания каналов возможность действительно приостановиться и
        # продолжить работу строго после указанного Telegram cooldown.
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS account_action_cooldowns (
                account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
                action TEXT NOT NULL,
                cooldown_until TIMESTAMP NOT NULL,
                source TEXT,
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (account_id, action)
            )
        ''')
        try:
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_account_action_cooldowns_until '
                'ON account_action_cooldowns (cooldown_until)'
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
        # СБП (Platega) — ID последней транзакции (UUID). Отдельно от Crypto Pay.
        try:
            await conn.execute(
                'ALTER TABLE subscriptions '
                'ADD COLUMN IF NOT EXISTS last_platega_id TEXT'
            )
        except Exception:
            pass
        # СБП пополнение баланса: Platega transaction id в balance_invoices.
        try:
            await conn.execute(
                'ALTER TABLE balance_invoices '
                'ADD COLUMN IF NOT EXISTS sbp_platega_id TEXT'
            )
        except Exception:
            pass
        # Сумма в рублях для СБП-пополнений.
        try:
            await conn.execute(
                'ALTER TABLE balance_invoices '
                'ADD COLUMN IF NOT EXISTS amount_rub NUMERIC(12, 2)'
            )
        except Exception:
            pass

        # Неизменяемый журнал подтверждённых платежей. Он нужен для
        # финансовой админ-панели: таблицы подписок хранят только последнее
        # состояние пользователя, а не всю историю оплат.
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS payment_events (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE SET NULL,
                kind TEXT NOT NULL,
                provider TEXT NOT NULL,
                external_id TEXT NOT NULL,
                amount_usdt NUMERIC(12, 6),
                amount_rub NUMERIC(12, 2),
                status TEXT NOT NULL DEFAULT 'paid',
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMP DEFAULT NOW(),
                paid_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(provider, external_id)
            )
        ''')
        try:
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_payment_events_paid_at '
                'ON payment_events (paid_at DESC)'
            )
            await conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_payment_events_kind_paid_at '
                'ON payment_events (kind, paid_at DESC)'
            )
        except Exception:
            pass

        # Безопасно переносим уже подтверждённые пополнения баланса из
        # старой таблицы. Историю Pro без отдельного журнала восстановить
        # полностью нельзя, поэтому новые оплаты Pro фиксируются в момент
        # подтверждения ниже.
        try:
            await conn.execute(
                '''INSERT INTO payment_events
                   (user_id, kind, provider, external_id, amount_usdt,
                    amount_rub, status, created_at, paid_at)
                   SELECT bi.user_id,
                          'wallet_topup',
                          CASE WHEN NULLIF(bi.sbp_platega_id, '') IS NOT NULL
                               THEN 'platega' ELSE 'cryptopay' END,
                          CASE WHEN NULLIF(bi.sbp_platega_id, '') IS NOT NULL
                               THEN bi.sbp_platega_id ELSE bi.invoice_id::TEXT END,
                          CASE WHEN NULLIF(bi.sbp_platega_id, '') IS NOT NULL
                               THEN NULL ELSE bi.amount_usdt END,
                          CASE WHEN NULLIF(bi.sbp_platega_id, '') IS NOT NULL
                               THEN COALESCE(
                                   bi.amount_rub,
                                   ROUND(bi.amount_usdt * $1::numeric, 2)
                               )
                               ELSE NULL END,
                          'paid', bi.created_at,
                          COALESCE(bi.paid_at, bi.created_at)
                   FROM balance_invoices bi
                   WHERE bi.status = 'paid'
                   ON CONFLICT (provider, external_id) DO NOTHING''',
                TOPUP_RUB_PER_USDT
            )
        except Exception as ex:
            logger.warning('Could not backfill payment events: %s', ex)

        # Для активных Pro безопасно переносим последнюю оплату: у такой
        # записи уже есть подтверждённый внешний ID. Старые истёкшие
        # подписки без журнала намеренно не угадываем.
        try:
            await conn.execute(
                '''INSERT INTO payment_events
                   (user_id, kind, provider, external_id, amount_usdt,
                    amount_rub, status, created_at, paid_at)
                   SELECT s.user_id,
                          'pro_subscription',
                          CASE WHEN NULLIF(s.last_platega_id, '') IS NOT NULL
                               THEN 'platega' ELSE 'cryptopay' END,
                          CASE WHEN NULLIF(s.last_platega_id, '') IS NOT NULL
                               THEN s.last_platega_id ELSE s.last_invoice_id::TEXT END,
                          CASE WHEN NULLIF(s.last_platega_id, '') IS NOT NULL
                               THEN NULL ELSE $1::numeric END,
                          CASE WHEN NULLIF(s.last_platega_id, '') IS NOT NULL
                               THEN $2::numeric ELSE NULL END,
                          'paid', s.updated_at, s.updated_at
                   FROM subscriptions s
                   WHERE s.tier = 'pro'
                     AND (
                         NULLIF(s.last_platega_id, '') IS NOT NULL
                         OR s.last_invoice_id IS NOT NULL
                     )
                   ON CONFLICT (provider, external_id) DO NOTHING''',
                float(PRO_PRICE_USD),
                PRO_PRICE_RUB,
            )
        except Exception as ex:
            logger.warning('Could not backfill active Pro payment events: %s', ex)

        # ===== Per-account AI-автоответчик (account_ai_responder) =====
        # Живёт на добавленном Telegram-аккаунте, а не в самом боте.
        # Режимы:
        #   'off' — выключен
        #   'ai'  — ИИ отвечает на входящие ЛС на аккаунте
        # system_prompt — личность ИИ для этого аккаунта.
        # model         — выбранная LLM (по умолчанию глобальная).
        # history       — JSONB, ключ = chat_id (str), значение = список пар
        #                 {role, content} последних ACCT_AR_HISTORY_PAIRS*2
        #                 сообщений для этого собеседника.
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS account_ai_responder (
                account_id     INTEGER PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
                mode           TEXT    NOT NULL DEFAULT 'off',
                system_prompt  TEXT    NOT NULL DEFAULT '',
                model          TEXT    NOT NULL DEFAULT '',
                history        JSONB   NOT NULL DEFAULT '{}'::jsonb,
                updated_at     TIMESTAMP DEFAULT NOW()
            )
        ''')

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
            warming_last_active,
            COALESCE(telegram_premium, FALSE) AS telegram_premium,
            COALESCE(validation_status, 'unknown') AS validation_status,
            last_validated_at
            FROM accounts WHERE user_id = $1''',
            user_id
        )
        return [dict(row) for row in rows]


async def refresh_global_llm_runtime() -> Dict[str, Any]:
    """Загружает активный базовый API администратора или fallback из кода.

    `LLM_MODELS` обновляется на месте, поэтому существующие синхронные
    клавиатуры сразу показывают подписи моделей, настроенные администратором.
    """
    global LLM_DEFAULT_MODEL, GLOBAL_LLM_RUNTIME, GLOBAL_LLM_RUNTIME_READY

    runtime: Dict[str, Any] = {
        'api_id': None,
        'name': 'Встроенный API',
        'base_url': LLM_BASE_URL,
        'api_key': LLM_API_KEY,
        'models': dict(LLM_FALLBACK_MODELS),
        'default_model': LLM_FALLBACK_DEFAULT_MODEL,
    }
    if db_pool is not None:
        try:
            async with db_pool.acquire() as conn:
                api = await conn.fetchrow(
                    '''SELECT id, name, base_url, api_key_ciphertext
                       FROM admin_llm_apis
                       WHERE is_active = TRUE
                       ORDER BY updated_at DESC, id DESC
                       LIMIT 1'''
                )
                if api:
                    rows = await conn.fetch(
                        '''SELECT api_model_name, display_name
                           FROM admin_llm_models
                           WHERE api_id = $1
                           ORDER BY sort_order, id''',
                        api['id'],
                    )
                    models = {
                        str(row['api_model_name']).strip(): str(row['display_name']).strip()
                        for row in rows
                        if str(row['api_model_name']).strip()
                        and str(row['display_name']).strip()
                    }
                    # Нельзя переключить весь бот на API без моделей: в этом
                    # случае оставляем проверенный встроенный fallback.
                    if models:
                        default_model = next(iter(models))
                        runtime = {
                            'api_id': int(api['id']),
                            'name': str(api['name']),
                            'base_url': str(api['base_url']).strip().rstrip('/'),
                            'api_key': _decrypt_llm_secret(api['api_key_ciphertext']),
                            'models': models,
                            'default_model': default_model,
                        }
        except Exception as ex:
            logger.warning('Could not refresh global LLM runtime: %s', ex)

    GLOBAL_LLM_RUNTIME = runtime
    GLOBAL_LLM_RUNTIME_READY = True
    LLM_MODELS.clear()
    LLM_MODELS.update(runtime['models'])
    LLM_DEFAULT_MODEL = runtime['default_model']
    return dict(runtime)


async def get_global_llm_runtime() -> Dict[str, Any]:
    if not GLOBAL_LLM_RUNTIME_READY and db_pool is not None:
        await refresh_global_llm_runtime()
    return dict(GLOBAL_LLM_RUNTIME)


async def get_user_llm_model(user_id: int) -> str:
    """Возвращает модель личного API пользователя либо базового API."""
    runtime = await get_global_llm_runtime()
    default_model = runtime['default_model']
    if db_pool is None:
        return default_model
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                '''SELECT u.llm_model, a.models
                   FROM users u
                   LEFT JOIN user_llm_apis a ON a.id = u.llm_active_api_id
                   WHERE u.user_id = $1''',
                user_id,
            )
        if row and row['models']:
            models = [str(x).strip() for x in row['models'] if str(x).strip()]
            if models:
                selected = (row['llm_model'] or '').strip()
                return selected if selected in models else models[0]
        if row and row['llm_model'] in runtime['models']:
            return row['llm_model']
    except Exception as e:
        logger.warning('get_user_llm_model fallback: %s', e)
    return default_model


async def set_user_llm_model(user_id: int, model: str) -> None:
    """Сохраняет модель из личного либо текущего базового API."""
    if db_pool is None:
        return
    runtime = await get_global_llm_runtime()
    async with db_pool.acquire() as conn:
        allowed = await conn.fetchval(
            '''SELECT models FROM user_llm_apis a
               JOIN users u ON u.llm_active_api_id = a.id
               WHERE u.user_id = $1''', user_id
        )
        custom_models = {str(x).strip() for x in (allowed or []) if str(x).strip()}
        if model not in runtime['models'] and model not in custom_models:
            raise ValueError(f'Unknown model: {model}')
        await conn.execute(
            'INSERT INTO users (user_id, llm_model) VALUES ($1, $2) '
            'ON CONFLICT (user_id) DO UPDATE SET llm_model = EXCLUDED.llm_model',
            user_id, model,
        )


async def get_user_llm_models(user_id: int) -> List[str]:
    """Список моделей для личного API или текущего базового API."""
    runtime = await get_global_llm_runtime()
    if db_pool is None:
        return list(runtime['models'])
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                '''SELECT a.models FROM users u
                   LEFT JOIN user_llm_apis a ON a.id = u.llm_active_api_id
                   WHERE u.user_id = $1''', user_id
            )
        models = [str(x).strip() for x in (row['models'] if row else []) or []]
        return [x for x in models if x] or list(runtime['models'])
    except Exception:
        return list(runtime['models'])


async def get_user_llm_runtime(
    user_id: Optional[int], requested_model: Optional[str] = None
) -> Tuple[str, str, str]:
    """Возвращает runtime личного или базового API без раскрытия ключа."""
    runtime = await get_global_llm_runtime()
    model = requested_model or runtime['default_model']
    base_url = runtime['base_url']
    api_key = runtime['api_key']
    using_custom_api = False
    if user_id is not None and db_pool is not None:
        try:
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    '''SELECT a.base_url, a.api_key_ciphertext, a.models
                       FROM users u
                       JOIN user_llm_apis a ON a.id = u.llm_active_api_id
                       WHERE u.user_id = $1''', user_id
                )
            if row:
                using_custom_api = True
                base_url = str(row['base_url']).strip().rstrip('/')
                api_key = _decrypt_llm_secret(row['api_key_ciphertext'])
                custom_models = [str(x).strip() for x in (row['models'] or []) if str(x).strip()]
                if custom_models and (not requested_model or requested_model not in custom_models):
                    model = custom_models[0]
        except Exception as ex:
            logger.warning('get_user_llm_runtime fallback: %s', ex)
    if not using_custom_api and model not in runtime['models']:
        model = runtime['default_model']
    return base_url, api_key, model


async def get_user_llm_apis(user_id: int) -> List[Dict[str, Any]]:
    if db_pool is None:
        return []
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            '''SELECT a.id, a.name, a.base_url, a.models,
                      (u.llm_active_api_id = a.id) AS is_active
               FROM user_llm_apis a JOIN users u ON u.user_id = a.user_id
               WHERE a.user_id = $1 ORDER BY a.created_at DESC''', user_id
        )
    return [dict(row) for row in rows]


async def has_active_custom_llm_api(user_id: int) -> bool:
    if db_pool is None:
        return False
    async with db_pool.acquire() as conn:
        return bool(await conn.fetchval(
            'SELECT 1 FROM users WHERE user_id = $1 AND llm_active_api_id IS NOT NULL',
            user_id
        ))


async def set_active_llm_api(user_id: int, api_id: Optional[int]) -> None:
    async with db_pool.acquire() as conn:
        if api_id is not None:
            owner = await conn.fetchval(
                'SELECT 1 FROM user_llm_apis WHERE id = $1 AND user_id = $2',
                api_id, user_id
            )
            if not owner:
                raise ValueError('API не найден')
            first_model = await conn.fetchval(
                'SELECT models[1] FROM user_llm_apis WHERE id = $1', api_id
            )
            await conn.execute(
                'UPDATE users SET llm_active_api_id = $1, llm_model = $2 '
                'WHERE user_id = $3', api_id, first_model, user_id
            )
        else:
            await conn.execute(
                'UPDATE users SET llm_active_api_id = NULL, llm_model = $1 '
                'WHERE user_id = $2', LLM_DEFAULT_MODEL, user_id
            )


async def save_user_llm_api(user_id: int, base_url: str, api_key: str, models: List[str]) -> int:
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                'INSERT INTO users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING',
                user_id
            )
            api_id = await conn.fetchval(
                '''INSERT INTO user_llm_apis
                   (user_id, name, base_url, api_key_ciphertext, models)
                   VALUES ($1, $2, $3, $4, $5::text[]) RETURNING id''',
                user_id, f'API пользователя #{user_id}', base_url.rstrip('/'),
                _encrypt_llm_secret(api_key), models
            )
            await conn.execute(
                'UPDATE users SET llm_active_api_id = $1, llm_model = $2 WHERE user_id = $3',
                api_id, models[0], user_id
            )
    return int(api_id)


async def get_admin_llm_apis() -> List[Dict[str, Any]]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            '''SELECT id, name, base_url, is_active, created_at, updated_at
               FROM admin_llm_apis
               ORDER BY is_active DESC, updated_at DESC, id DESC'''
        )
    return [dict(row) for row in rows]


async def get_admin_llm_api(api_id: int) -> Optional[Dict[str, Any]]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            '''SELECT id, name, base_url, is_active, created_at, updated_at
               FROM admin_llm_apis WHERE id = $1''',
            api_id,
        )
    return dict(row) if row else None


async def get_admin_llm_models(api_id: int) -> List[Dict[str, Any]]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            '''SELECT id, api_id, api_model_name, display_name, sort_order
               FROM admin_llm_models WHERE api_id = $1
               ORDER BY sort_order, id''',
            api_id,
        )
    return [dict(row) for row in rows]


async def create_admin_llm_api(name: str, base_url: str, api_key: str) -> int:
    async with db_pool.acquire() as conn:
        api_id = await conn.fetchval(
            '''INSERT INTO admin_llm_apis
               (name, base_url, api_key_ciphertext)
               VALUES ($1, $2, $3) RETURNING id''',
            name.strip(), base_url.strip().rstrip('/'), _encrypt_llm_secret(api_key),
        )
    return int(api_id)


async def add_admin_llm_model(
    api_id: int, api_model_name: str, display_name: str,
) -> int:
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            exists = await conn.fetchval(
                'SELECT 1 FROM admin_llm_apis WHERE id = $1', api_id,
            )
            if not exists:
                raise ValueError('API не найден')
            models_count = await conn.fetchval(
                'SELECT COUNT(*) FROM admin_llm_models WHERE api_id = $1', api_id,
            )
            if int(models_count or 0) >= 20:
                raise ValueError('Для одного API можно добавить максимум 20 моделей')
            sort_order = await conn.fetchval(
                'SELECT COALESCE(MAX(sort_order), 0) + 1 '
                'FROM admin_llm_models WHERE api_id = $1',
                api_id,
            )
            model_id = await conn.fetchval(
                '''INSERT INTO admin_llm_models
                   (api_id, api_model_name, display_name, sort_order)
                   VALUES ($1, $2, $3, $4) RETURNING id''',
                api_id,
                api_model_name.strip(),
                display_name.strip(),
                int(sort_order or 1),
            )
    await refresh_global_llm_runtime()
    return int(model_id)


async def activate_admin_llm_api(api_id: int) -> None:
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            models_count = await conn.fetchval(
                'SELECT COUNT(*) FROM admin_llm_models WHERE api_id = $1', api_id,
            )
            if not models_count:
                raise ValueError('Сначала добавьте хотя бы одну модель')
            exists = await conn.fetchval(
                'SELECT 1 FROM admin_llm_apis WHERE id = $1', api_id,
            )
            if not exists:
                raise ValueError('API не найден')
            await conn.execute('UPDATE admin_llm_apis SET is_active = FALSE')
            await conn.execute(
                'UPDATE admin_llm_apis SET is_active = TRUE, updated_at = NOW() '
                'WHERE id = $1', api_id,
            )
    await refresh_global_llm_runtime()


async def use_builtin_llm_api() -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            'UPDATE admin_llm_apis SET is_active = FALSE, updated_at = NOW() '
            'WHERE is_active = TRUE'
        )
    await refresh_global_llm_runtime()


async def delete_admin_llm_api(api_id: int) -> bool:
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            'DELETE FROM admin_llm_apis WHERE id = $1', api_id,
        )
    await refresh_global_llm_runtime()
    return result.endswith(' 1')


async def delete_admin_llm_model(model_id: int) -> Optional[int]:
    """Удаляет модель; если у активного API моделей не осталось — отключает его."""
    api_id: Optional[int] = None
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                'DELETE FROM admin_llm_models WHERE id = $1 RETURNING api_id',
                model_id,
            )
            if not row:
                return None
            api_id = int(row['api_id'])
            remaining = await conn.fetchval(
                'SELECT COUNT(*) FROM admin_llm_models WHERE api_id = $1', api_id,
            )
            if not remaining:
                await conn.execute(
                    'UPDATE admin_llm_apis SET is_active = FALSE, updated_at = NOW() '
                    'WHERE id = $1', api_id,
                )
    await refresh_global_llm_runtime()
    return api_id


async def get_admin_llm_api_secret(api_id: int) -> Optional[Dict[str, Any]]:
    """Возвращает секрет только для серверного теста модели, не для UI."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            '''SELECT id, name, base_url, api_key_ciphertext, is_active
               FROM admin_llm_apis WHERE id = $1''',
            api_id,
        )
    if not row:
        return None
    data = dict(row)
    data['api_key'] = _decrypt_llm_secret(data.pop('api_key_ciphertext'))
    return data


def _extract_llm_response_text(response: Any) -> str:
    parts: List[str] = []
    try:
        for block in (response.content or []):
            if getattr(block, 'type', None) == 'text':
                text = (getattr(block, 'text', '') or '').strip()
                if text:
                    parts.append(text)
    except Exception:
        pass
    return '\n'.join(parts).strip()


async def test_llm_provider_model(
    *,
    base_url: str,
    api_key: str,
    api_model_name: str,
    display_name: str,
) -> Dict[str, Any]:
    """Отправляет короткий тест модели и сохраняет конкретную ошибку API."""
    started = time.monotonic()
    result: Dict[str, Any] = {
        'api_model_name': api_model_name,
        'display_name': display_name,
        'ok': False,
        'response': '',
        'error': '',
        'elapsed': 0.0,
    }
    try:
        client = anthropic.AsyncAnthropic(
            api_key=api_key,
            base_url=base_url.rstrip('/'),
            timeout=min(LLM_TIMEOUT, 45),
        )
        response = await client.messages.create(
            model=api_model_name,
            max_tokens=32,
            system='You are a connection test. Reply with exactly: OK',
            messages=[{'role': 'user', 'content': 'Connection test'}],
        )
        result['response'] = _extract_llm_response_text(response) or 'Модель ответила без text-блока'
        result['ok'] = True
    except anthropic.APIStatusError as ex:
        result['error'] = f"HTTP {ex.status_code}: {str(ex)[:700]}"
    except anthropic.APIError as ex:
        result['error'] = f"{ex.__class__.__name__}: {str(ex)[:700]}"
    except asyncio.TimeoutError:
        result['error'] = 'Timeout: модель не ответила за отведённое время'
    except Exception as ex:
        result['error'] = f"{ex.__class__.__name__}: {str(ex)[:700]}"
    result['elapsed'] = time.monotonic() - started
    return result


async def test_admin_llm_model(model_id: int) -> Optional[Dict[str, Any]]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            '''SELECT m.id AS model_id, m.api_model_name, m.display_name,
                      a.id AS api_id, a.base_url, a.api_key_ciphertext
               FROM admin_llm_models m
               JOIN admin_llm_apis a ON a.id = m.api_id
               WHERE m.id = $1''',
            model_id,
        )
    if not row:
        return None
    result = await test_llm_provider_model(
        base_url=str(row['base_url']),
        api_key=_decrypt_llm_secret(row['api_key_ciphertext']),
        api_model_name=str(row['api_model_name']),
        display_name=str(row['display_name']),
    )
    result['api_id'] = int(row['api_id'])
    return result


async def test_admin_llm_api_models(api_id: int) -> Optional[List[Dict[str, Any]]]:
    api = await get_admin_llm_api_secret(api_id)
    if not api:
        return None
    models = await get_admin_llm_models(api_id)
    results = []
    for model in models:
        results.append(await test_llm_provider_model(
            base_url=str(api['base_url']),
            api_key=str(api['api_key']),
            api_model_name=str(model['api_model_name']),
            display_name=str(model['display_name']),
        ))
    return results


async def test_builtin_llm_models() -> List[Dict[str, Any]]:
    results = []
    for api_model_name, display_name in LLM_FALLBACK_MODELS.items():
        results.append(await test_llm_provider_model(
            base_url=LLM_BASE_URL,
            api_key=LLM_API_KEY,
            api_model_name=api_model_name,
            display_name=display_name,
        ))
    return results


def format_llm_models_test_report(
    title: str, results: List[Dict[str, Any]], detailed: bool = False,
) -> str:
    lines = [f"{emoji('AI')} <b>{escape(title)}</b>", '']
    if not results:
        lines.append('У провайдера нет сохранённых моделей для теста.')
        return '\n'.join(lines)

    error_limit = 650 if detailed else 110
    response_limit = 180 if detailed else 70
    for index, result in enumerate(results):
        display = escape(str(result.get('display_name') or result.get('api_model_name') or 'Модель'))
        api_name = escape(str(result.get('api_model_name') or '—'))
        elapsed = float(result.get('elapsed') or 0)
        if result.get('ok'):
            preview = str(result.get('response') or 'OK').replace('\n', ' ')[:response_limit]
            item = (
                f"{emoji('CHECK')} <b>{display}</b> (<code>{api_name}</code>) — OK за {elapsed:.1f}с\n"
                f"<i>{escape(preview)}</i>"
            )
        else:
            error = str(result.get('error') or 'Неизвестная ошибка')[:error_limit]
            item = (
                f"{emoji('CROSS')} <b>{display}</b> (<code>{api_name}</code>) — ошибка за {elapsed:.1f}с\n"
                f"<code>{escape(error)}</code>"
            )
        # Не режем HTML-теги посередине: для массового теста оставляем
        # место под понятную пометку о неприведённых строках.
        candidate = '\n\n'.join(lines + [item])
        if len(candidate) > 3700 and not detailed:
            lines.append(f"… ещё моделей: <b>{len(results) - index}</b>")
            break
        lines.append(item)
    return '\n\n'.join(lines)


async def get_account(account_id: int) -> Optional[Dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            '''SELECT *, COALESCE(warming_enabled, FALSE) as warming_enabled,
            COALESCE(warming_cycles, 0) as warming_cycles
            FROM accounts WHERE id = $1''',
            account_id
        )
        return dict(row) if row else None


SPAM_BLOCK_STATUS_LABELS = {
    'clear': 'Ограничений не найдено',
    'limited': 'Есть ограничения',
    'unknown': 'Ответ не распознан',
    'error': 'Не удалось проверить',
}


def _now_msk_naive() -> datetime:
    """В БД используются TIMESTAMP без timezone, поэтому храним МСК naive."""
    return datetime.now(MSK_TZ).replace(tzinfo=None)


def _format_msk_datetime(value: Any, empty: str = 'ещё не проверялось') -> str:
    if not value:
        return empty
    try:
        if getattr(value, 'tzinfo', None) is not None:
            value = value.astimezone(MSK_TZ)
        return value.strftime('%d.%m.%Y %H:%M МСК')
    except Exception:
        return str(value)[:16]


def classify_spam_block_response(text: str) -> str:
    """Классифицирует ответ @SpamBot на русском и английском.

    Сначала ищем явные фразы об отсутствии ограничений: в ответе может
    одновременно встречаться слово «ограничение», поэтому порядок важен.
    """
    normalized = ' '.join((text or '').casefold().split())
    if not normalized:
        return 'unknown'

    clear_markers = (
        'no limits are currently applied',
        'no limitations are currently applied',
        'no restrictions are currently applied',
        'your account is free of limitations',
        'you are free as a bird',
        'никаких ограничений',
        'ограничения не наложены',
        'нет ограничений на ваш аккаунт',
        'ваш аккаунт не ограничен',
    )
    if any(marker in normalized for marker in clear_markers):
        return 'clear'

    limited_markers = (
        'your account is limited',
        'account is limited',
        'you are limited',
        'limited until',
        'restrictions are currently applied',
        'account has been limited',
        'ваш аккаунт ограничен',
        'аккаунт ограничен до',
        'на ваш аккаунт наложены ограничения',
        'ограничения действуют',
    )
    if any(marker in normalized for marker in limited_markers):
        return 'limited'
    return 'unknown'


async def get_account_spam_check_settings(
    account_id: int, user_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Настройки проверки; без записи в БД используются безопасные дефолты."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT * FROM account_spam_checks WHERE account_id = $1',
            account_id,
        )
    if row:
        return dict(row)
    return {
        'account_id': account_id,
        'user_id': user_id,
        'is_enabled': True,
        'notify_enabled': True,
        'last_checked_at': None,
        'last_status': None,
        'last_response': None,
        'last_error': None,
    }


async def update_account_spam_check_settings(
    account_id: int,
    user_id: int,
    *,
    is_enabled: Optional[bool] = None,
    notify_enabled: Optional[bool] = None,
) -> Dict[str, Any]:
    """Меняет только указанные настройки, не стирая прошлый результат."""
    current = await get_account_spam_check_settings(account_id, user_id)
    enabled = bool(current.get('is_enabled', True)) if is_enabled is None else bool(is_enabled)
    notify = (
        bool(current.get('notify_enabled', True))
        if notify_enabled is None else bool(notify_enabled)
    )
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO account_spam_checks
               (account_id, user_id, is_enabled, notify_enabled, updated_at)
               VALUES ($1, $2, $3, $4, NOW())
               ON CONFLICT (account_id) DO UPDATE SET
                 user_id = EXCLUDED.user_id,
                 is_enabled = EXCLUDED.is_enabled,
                 notify_enabled = EXCLUDED.notify_enabled,
                 updated_at = NOW()''',
            account_id, user_id, enabled, notify,
        )
    return await get_account_spam_check_settings(account_id, user_id)


async def save_account_spam_check_result(
    account_id: int,
    user_id: int,
    status: str,
    response: str = '',
    error: str = '',
) -> None:
    """Сохраняет результат проверки, не меняя включённые пользователем тумблеры."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO account_spam_checks
               (account_id, user_id, last_checked_at, last_status,
                last_response, last_error, updated_at)
               VALUES ($1, $2, NOW(), $3, $4, $5, NOW())
               ON CONFLICT (account_id) DO UPDATE SET
                 user_id = EXCLUDED.user_id,
                 last_checked_at = EXCLUDED.last_checked_at,
                 last_status = EXCLUDED.last_status,
                 last_response = EXCLUDED.last_response,
                 last_error = EXCLUDED.last_error,
                 updated_at = NOW()''',
            account_id,
            user_id,
            status,
            (response or '')[:4000],
            (error or '')[:1000],
        )


def format_spam_block_result(result: Dict[str, Any]) -> str:
    status = result.get('status') or 'unknown'
    label = SPAM_BLOCK_STATUS_LABELS.get(status, 'Неизвестный статус')
    response = (result.get('response') or result.get('error') or '—').strip()
    if len(response) > SPAM_BLOCK_RESPONSE_LIMIT:
        response = response[:SPAM_BLOCK_RESPONSE_LIMIT - 1].rstrip() + '…'
    return (
        f"Статус: <b>{escape(label)}</b>\n"
        f"Ответ @SpamBot:\n<i>{escape(response)}</i>"
    )


async def notify_spam_block_result(
    user_id: int, account: Dict[str, Any], result: Dict[str, Any],
) -> None:
    """Отправляет владельцу результат плановой проверки, если он не выключил уведомления."""
    try:
        checked_at = _format_msk_datetime(result.get('checked_at'), 'только что')
        await bot.send_message(
            user_id,
            f"{emoji('BELL')} <b>Автопроверка ограничений</b>\n\n"
            f"{emoji('PHONE')} Аккаунт: <code>{escape(str(account.get('phone') or result.get('account_id')))}</code>\n"
            f"{format_spam_block_result(result)}\n\n"
            f"{emoji('CLOCK')} Проверено: <b>{checked_at}</b>",
        )
    except Exception as ex:
        logger.warning('Could not notify spam check result for account %s: %s',
                       result.get('account_id'), ex)


async def check_account_spam_block(
    account_id: int,
    user_id: int,
    *,
    notify: bool = False,
) -> Dict[str, Any]:
    """Запрашивает @SpamBot и сохраняет результат проверки аккаунта.

    Это именно проверка статуса и соблюдение ограничений Telegram: функция
    не пытается обходить блокировки и не повторяет запросы при FloodWait.
    """
    lock = spam_check_locks.setdefault(account_id, asyncio.Lock())
    async with lock:
        account = await get_account(account_id)
        checked_at = _now_msk_naive()
        result: Dict[str, Any] = {
            'account_id': account_id,
            'status': 'error',
            'response': '',
            'error': '',
            'checked_at': checked_at,
        }
        if not account or account.get('user_id') != user_id:
            result['error'] = 'Аккаунт не найден или недоступен'
            return result

        try:
            client = await get_client_for_account(account_id)
            if not client or not await client.is_user_authorized():
                raise RuntimeError('Не удалось подключиться к Telegram-аккаунту')

            async with client.conversation(
                SPAM_BLOCK_BOT_USERNAME,
                timeout=SPAM_BLOCK_REQUEST_TIMEOUT_SECONDS,
                exclusive=False,
            ) as conversation:
                await conversation.send_message('/start')
                message = await conversation.get_response()

            response = (
                getattr(message, 'raw_text', None)
                or getattr(message, 'message', None)
                or ''
            ).strip()
            result['response'] = response
            result['status'] = classify_spam_block_response(response)
        except FloodWaitError as ex:
            await record_flood_wait(account_id, 0, ex.seconds)
            result['error'] = f'FloodWait: Telegram просит подождать {int(ex.seconds)} сек.'
        except asyncio.TimeoutError:
            result['error'] = 'Не дождались ответа @SpamBot'
        except Exception as ex:
            logger.warning('Spam block check failed for account %s: %s', account_id, ex)
            result['error'] = str(ex)[:1000]

        await save_account_spam_check_result(
            account_id,
            user_id,
            result['status'],
            result['response'],
            result['error'],
        )
        try:
            preview = (result['response'] or result['error'] or '')[:90]
            await add_account_log(
                account_id,
                SPAM_BLOCK_BOT_USERNAME,
                0,
                'spam_check',
                f"{result['status']}: {preview}",
            )
        except Exception:
            pass

        if notify:
            settings = await get_account_spam_check_settings(account_id, user_id)
            if settings.get('notify_enabled', True):
                await notify_spam_block_result(user_id, account, result)
        return result


async def get_due_spam_block_checks(limit: int = 20) -> List[Dict[str, int]]:
    """Выбирает активные аккаунты, не проверявшиеся последние 12 часов."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            '''SELECT a.id AS account_id, a.user_id
               FROM accounts a
               LEFT JOIN account_spam_checks s ON s.account_id = a.id
               WHERE a.is_active = TRUE
                 AND COALESCE(s.is_enabled, TRUE) = TRUE
                 AND (
                   s.last_checked_at IS NULL
                   OR s.last_checked_at <= NOW() - INTERVAL '12 hours'
                 )
               ORDER BY s.last_checked_at NULLS FIRST, a.id
               LIMIT $1''',
            max(1, min(int(limit), 100)),
        )
    return [dict(row) for row in rows]


async def spam_block_check_worker() -> None:
    """Фоновый планировщик: проверка один раз в 12 часов на аккаунт."""
    while True:
        try:
            due_accounts = await get_due_spam_block_checks()
            for item in due_accounts:
                try:
                    await check_account_spam_block(
                        int(item['account_id']), int(item['user_id']), notify=True,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as ex:
                    logger.exception('Spam block scheduler check failed: %s', ex)
                # Не создаём всплеск запросов к @SpamBot для множества аккаунтов.
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            logger.exception('Spam block scheduler failed: %s', ex)
        await asyncio.sleep(SPAM_BLOCK_SCHEDULER_POLL_SECONDS)


async def save_account_validity_monitoring_state(
    account_id: int, user_id: int, status: str, error: str = '',
) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO account_monitoring_state
               (account_id, user_id, last_validity_check_at, last_validity_status,
                last_validity_error, updated_at)
               VALUES ($1, $2, NOW(), $3, $4, NOW())
               ON CONFLICT (account_id) DO UPDATE SET
                 user_id = EXCLUDED.user_id,
                 last_validity_check_at = EXCLUDED.last_validity_check_at,
                 last_validity_status = EXCLUDED.last_validity_status,
                 last_validity_error = EXCLUDED.last_validity_error,
                 updated_at = NOW()''',
            account_id, user_id, status, (error or '')[:1000],
        )


async def save_account_ai_analysis_monitoring_state(
    account_id: int, user_id: int, source: str, text: str,
) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO account_monitoring_state
               (account_id, user_id, last_ai_analysis_at, last_ai_analysis_source,
                last_ai_analysis_text, updated_at)
               VALUES ($1, $2, NOW(), $3, $4, NOW())
               ON CONFLICT (account_id) DO UPDATE SET
                 user_id = EXCLUDED.user_id,
                 last_ai_analysis_at = EXCLUDED.last_ai_analysis_at,
                 last_ai_analysis_source = EXCLUDED.last_ai_analysis_source,
                 last_ai_analysis_text = EXCLUDED.last_ai_analysis_text,
                 updated_at = NOW()''',
            account_id, user_id, source, (text or '')[:12000],
        )


async def get_due_account_validity_checks(
    limit: int = ACCOUNT_VALIDITY_BATCH_SIZE,
) -> List[Dict[str, int]]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            '''SELECT a.id AS account_id, a.user_id
               FROM accounts a
               LEFT JOIN account_monitoring_state m ON m.account_id = a.id
               WHERE a.is_active = TRUE
                 AND (
                   m.last_validity_check_at IS NULL
                   OR m.last_validity_check_at <= NOW() - INTERVAL '1 hour'
                 )
               ORDER BY m.last_validity_check_at NULLS FIRST, a.id
               LIMIT $1''',
            max(1, min(int(limit), 100)),
        )
    return [dict(row) for row in rows]


async def get_due_account_ai_analyses(
    limit: int = ACCOUNT_AI_ANALYSIS_BATCH_SIZE,
) -> List[Dict[str, int]]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            '''SELECT a.id AS account_id, a.user_id
               FROM accounts a
               LEFT JOIN account_monitoring_state m ON m.account_id = a.id
               WHERE a.is_active = TRUE
                 AND (
                   m.last_ai_analysis_at IS NULL
                   OR m.last_ai_analysis_at <= NOW() - INTERVAL '7 days'
                 )
               ORDER BY m.last_ai_analysis_at NULLS FIRST, a.id
               LIMIT $1''',
            max(1, min(int(limit), 30)),
        )
    return [dict(row) for row in rows]


async def shutdown_account_runtime(account_id: int, user_id: int) -> None:
    """Останавливает живые воркеры перед окончательным удалением аккаунта."""
    try:
        await stop_warming(account_id)
    except Exception:
        pass

    responders = active_auto_responders.get(user_id)
    if responders:
        task = responders.pop(account_id, None)
        if task:
            task.cancel()
        if not responders:
            active_auto_responders.pop(user_id, None)

    try:
        await stop_account_ai_responder(account_id, user_id)
    except Exception:
        pass

    autosub_stop_flags[account_id] = True
    autosub_task = autosub_tasks.pop(account_id, None)
    if autosub_task:
        autosub_task.cancel()
    autosub_stop_flags.pop(account_id, None)

    # Останавливаем живые задачи обычных рассылок этого аккаунта, если они
    # ещё держат клиент в памяти. Статус в БД будет снят в delete_account().
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT id FROM broadcasts WHERE account_id = $1', account_id,
            )
        for row in rows:
            task = active_broadcasts.pop(int(row['id']), None)
            if task:
                task.cancel()
    except Exception:
        pass

    # Конфигурации нейрокомментинга каскадно удалятся из БД вместе с
    # аккаунтом, но живые слушатели нужно остановить заранее.
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT id FROM neurocomment_configs WHERE account_id = $1', account_id,
            )
        for row in rows:
            await stop_neurocomment_worker(int(row['id']))
    except Exception:
        pass

    # Долгие маршруты скриптов также не должны продолжаться с удалённой
    # сессией аккаунта.
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT id FROM user_scripts WHERE account_id = $1', account_id,
            )
        for row in rows:
            await stop_script_runner(int(row['id']), user_id)
    except Exception:
        pass

    for storage in (active_clients, pending_clients):
        client = storage.pop(account_id, None)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass


async def delete_account(account_id: int) -> bool:
    """Удаляет аккаунт и зависимые активные настройки без нарушения FK."""
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            # Историю рассылок сохраняем, но отвязываем её от удаляемой сессии.
            await conn.execute(
                "UPDATE broadcasts SET account_id = NULL, "
                "status = CASE WHEN status IN ('active', 'scheduled') THEN 'stopped' ELSE status END, "
                "stopped_at = CASE WHEN status IN ('active', 'scheduled') THEN NOW() ELSE stopped_at END "
                "WHERE account_id = $1",
                account_id,
            )
            await conn.execute(
                "UPDATE dm_broadcasts SET account_id = NULL, "
                "status = CASE WHEN status = 'active' THEN 'stopped' ELSE status END, "
                "stopped_at = CASE WHEN status = 'active' THEN NOW() ELSE stopped_at END "
                "WHERE account_id = $1",
                account_id,
            )
            await conn.execute(
                'DELETE FROM auto_responders WHERE account_id = $1', account_id,
            )
            await conn.execute(
                'DELETE FROM account_logs WHERE account_id = $1', account_id,
            )
            result = await conn.execute(
                'DELETE FROM accounts WHERE id = $1', account_id,
            )
    return result != 'DELETE 0'


async def notify_invalid_account_removal(
    user_id: int, account: Dict[str, Any], error: str,
) -> None:
    try:
        await bot.send_message(
            user_id,
            f"{emoji('CROSS')} <b>Аккаунт удалён после проверки валидности</b>\n\n"
            f"{emoji('PHONE')} Аккаунт: <code>{escape(str(account.get('phone') or account.get('id')))}</code>\n"
            "Telegram не подтвердил авторизацию этой сессии, поэтому бот "
            "полностью снял аккаунт из работы.\n\n"
            f"Причина: <i>{escape((error or 'Сессия не авторизована')[:500])}</i>",
        )
    except Exception as ex:
        logger.warning('Could not notify account removal for %s: %s', account.get('id'), ex)


async def remove_invalid_account(
    account: Dict[str, Any], user_id: int, error: str,
) -> bool:
    account_id = int(account['id'])
    await shutdown_account_runtime(account_id, user_id)
    deleted = await delete_account(account_id)
    if deleted:
        await notify_invalid_account_removal(user_id, account, error)
    return deleted


def _split_monitoring_text(text: str, limit: int = 3200) -> List[str]:
    text = (text or '').strip() or 'Нет данных для анализа.'
    chunks: List[str] = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        cut = text.rfind('\n', 0, limit)
        if cut < limit // 2:
            cut = limit
        chunks.append(text[:cut])
        text = text[cut:].lstrip('\n')
    return chunks


async def notify_weekly_account_analysis(
    user_id: int, account: Dict[str, Any], result: Dict[str, Any],
) -> None:
    source = 'AI' if result.get('source') == 'llm' else 'локальная эвристика'
    chunks = _split_monitoring_text(result.get('text') or '')
    for index, chunk in enumerate(chunks):
        header = (
            f"{emoji('AI')} <b>Еженедельный анализ аккаунта</b>\n\n"
            f"{emoji('PHONE')} Аккаунт: <code>{escape(str(account.get('phone') or account.get('id')))}</code>\n"
            f"Источник: <b>{source}</b>\n\n"
            if index == 0 else f"{emoji('AI')} <b>Продолжение анализа</b>\n\n"
        )
        try:
            # LLM-текст не считаем доверенной HTML-разметкой.
            await bot.send_message(user_id, header + escape(chunk))
        except Exception as ex:
            logger.warning('Could not notify weekly analysis for account %s: %s',
                           account.get('id'), ex)
            return


async def run_account_validity_monitor(
    account_id: int, user_id: int,
) -> Dict[str, Any]:
    lock = account_monitoring_locks.setdefault(account_id, asyncio.Lock())
    async with lock:
        account = await get_account(account_id)
        if not account or account.get('user_id') != user_id:
            return {'status': 'missing'}
        result = await validate_account(account_id, user_id)
        status = result.get('status') or ('valid' if result.get('valid') else 'check_error')
        await save_account_validity_monitoring_state(
            account_id, user_id, status, result.get('error') or '',
        )
        if result.get('removable'):
            removed = await remove_invalid_account(
                account, user_id, result.get('error') or '',
            )
            result['removed'] = removed
        return result


async def run_weekly_account_ai_analysis(
    account_id: int, user_id: int,
) -> Dict[str, Any]:
    lock = account_monitoring_locks.setdefault(account_id, asyncio.Lock())
    async with lock:
        account = await get_account(account_id)
        if not account or account.get('user_id') != user_id or not account.get('is_active'):
            return {'status': 'missing'}
        # user_id=None намеренно: регулярный сервисный анализ использует
        # базовый API, выбранный администратором, а не личный API пользователя.
        result = await analyze_account_logs_security(account_id, user_id=None)
        await save_account_ai_analysis_monitoring_state(
            account_id,
            user_id,
            result.get('source') or 'heuristic',
            result.get('text') or '',
        )
        await notify_weekly_account_analysis(user_id, account, result)
        return result


async def account_monitoring_worker() -> None:
    """Почасовая валидация и недельный AI-анализ активных аккаунтов."""
    while True:
        try:
            for item in await get_due_account_validity_checks():
                try:
                    await run_account_validity_monitor(
                        int(item['account_id']), int(item['user_id']),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as ex:
                    logger.exception('Account validity monitor failed: %s', ex)
                await asyncio.sleep(1)

            for item in await get_due_account_ai_analyses():
                try:
                    await run_weekly_account_ai_analysis(
                        int(item['account_id']), int(item['user_id']),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as ex:
                    logger.exception('Weekly account AI analysis failed: %s', ex)
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            logger.exception('Account monitoring worker failed: %s', ex)
        await asyncio.sleep(ACCOUNT_MONITORING_POLL_SECONDS)


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
    runtime_url, runtime_key, model = await get_user_llm_runtime(user_id, model)
    client = anthropic.AsyncAnthropic(
        api_key=runtime_key,
        base_url=runtime_url,
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
    session_string: str, proxy: Optional[Dict] = None,
    fingerprint: Optional[Dict] = None
) -> TelegramClient:
    # Собираем «отпечаток устройства» для Telethon-клиента. Если не
    # задан — используем параметры по умолчанию, как делал Telegram
    # Desktop (это давно «палится» антифрод-системой TG).
    fp_kwargs: Dict[str, str] = {}
    if fingerprint:
        if fingerprint.get('device_model'):
            fp_kwargs['device_model'] = fingerprint['device_model']
        if fingerprint.get('system_version'):
            fp_kwargs['system_version'] = fingerprint['system_version']
        if fingerprint.get('app_version'):
            fp_kwargs['app_version'] = fingerprint['app_version']
        if fingerprint.get('lang_code'):
            fp_kwargs['lang_code'] = fingerprint['lang_code']
        if fingerprint.get('system_lang_code'):
            fp_kwargs['system_lang_code'] = fingerprint['system_lang_code']

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
            StringSession(session_string), API_ID, API_HASH,
            proxy=proxy_arg, **fp_kwargs
        )
    return TelegramClient(
        StringSession(session_string), API_ID, API_HASH, **fp_kwargs
    )

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
                return None
            host = parsed.hostname
            port = parsed.port or 1080
            username = parsed.username
            password = parsed.password
            if not host or not 1 <= int(port) <= 65535:
                return None
            return {
                'proxy_type': scheme, 'host': host, 'port': port,
                'username': username, 'password': password,
            }
        # Формат host:port[:user:pass]
        parts = text.split(':')
        if len(parts) == 2:
            port = int(parts[1])
            if not parts[0].strip() or not 1 <= port <= 65535:
                return None
            return {
                'proxy_type': 'socks5', 'host': parts[0].strip(), 'port': port,
                'username': None, 'password': None,
            }
        if len(parts) == 4:
            port = int(parts[1])
            if not parts[0].strip() or not 1 <= port <= 65535:
                return None
            return {
                'proxy_type': 'socks5', 'host': parts[0].strip(), 'port': port,
                'username': parts[2], 'password': parts[3],
            }
        return None
    except Exception:
        return None


try:
    PROXY_CHECK_TIMEOUT = max(
        2.0, min(30.0, float(os.getenv('PROXY_CHECK_TIMEOUT') or '8'))
    )
except (TypeError, ValueError):
    PROXY_CHECK_TIMEOUT = 8.0
PROXY_CHECK_TARGETS = (
    ('149.154.167.51', 443),
    ('149.154.175.50', 443),
)


def _check_proxy_connection_sync(proxy: Dict[str, Any]) -> Dict[str, Any]:
    """Проверить TCP-подключение к Telegram через указанный прокси."""
    proxy_types = {
        'socks5': socks.SOCKS5,
        'socks4': socks.SOCKS4,
        'http': socks.HTTP,
    }
    proxy_type = proxy_types.get(str(proxy.get('proxy_type', '')).lower())
    if proxy_type is None:
        return {'ok': False, 'error': 'Неподдерживаемый тип прокси'}

    last_error = 'Прокси не отвечает'
    for target_host, target_port in PROXY_CHECK_TARGETS:
        proxy_socket = socks.socksocket()
        proxy_socket.settimeout(PROXY_CHECK_TIMEOUT)
        proxy_socket.set_proxy(
            proxy_type,
            str(proxy['host']),
            int(proxy['port']),
            rdns=True,
            username=proxy.get('username') or None,
            password=proxy.get('password') or None,
        )
        started_at = time.monotonic()
        try:
            proxy_socket.connect((target_host, target_port))
            latency_ms = max(1, round((time.monotonic() - started_at) * 1000))
            return {
                'ok': True,
                'latency_ms': latency_ms,
                'target': f'{target_host}:{target_port}',
            }
        except Exception as ex:
            last_error = str(ex) or ex.__class__.__name__
        finally:
            try:
                proxy_socket.close()
            except Exception:
                pass

    return {'ok': False, 'error': last_error[:300]}


async def check_proxy_connection(proxy: Dict[str, Any]) -> Dict[str, Any]:
    """Асинхронная реальная проверка прокси без блокировки aiogram."""
    total_timeout = PROXY_CHECK_TIMEOUT * len(PROXY_CHECK_TARGETS) + 2
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_check_proxy_connection_sync, proxy),
            timeout=total_timeout,
        )
    except asyncio.TimeoutError:
        return {'ok': False, 'error': 'Истекло время ожидания подключения'}
    except Exception as ex:
        logger.exception('Proxy check failed unexpectedly')
        return {'ok': False, 'error': str(ex)[:300]}

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

    # Подтягиваем пер-аккаунтный «отпечаток устройства» (A3).
    # Если в БД его нет — авто-генерим и сохраняем, чтобы
    # последующие подключения были консистентны.
    fingerprint = await get_account_fingerprint(account_id)
    if not fingerprint:
        fingerprint = await regenerate_account_fingerprint(account_id)

    try:
        client = await create_telethon_client(
            account['session_string'], proxy=proxy,
            fingerprint=fingerprint
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


# =====================================================================
# A3 — Пер-аккаунтный «отпечаток устройства» (device fingerprint)
# =====================================================================
# Telethon позволяет задать пять полей, по которым Telegram-сервер
# различает клиентов: device_model, system_version, app_version,
# lang_code, system_lang_code. Если у всех аккаунтов они одинаковые
# (а у desktop-клиента по умолчанию одинаковые) — это легко
# детектируется антифродом и аккаунты банят пачкой.
#
# Поведение:
#   • При первом подключении аккаунта отпечаток авто-генерируется
#     из пула реалистичных устройств и сохраняется в БД.
#   • Пользователь может в любой момент сгенерировать новый
#     отпечаток через UI (кнопка «Отпечаток устройства»).
#   • После смены отпечатка активный Telethon-клиент сбрасывается
#     из active_clients, чтобы при следующем подключении
#     использовались новые параметры.
# =====================================================================

# Реалистичный пул устройств. Берём распространённые модели,
# актуальные версии ОС и популярные связки языков. Не включаем
# экзотику, чтобы не вызывать лишних подозрений.
FINGERPRINT_DEVICE_POOL: List[Dict[str, str]] = [
    # --- iOS (iPhone) ---
    {
        "device_model": "iPhone 14 Pro",
        "system_version": "iOS 16.5.1",
        "app_version": "8.9.3",
        "lang_code": "ru",
        "system_lang_code": "ru-RU",
    },
    {
        "device_model": "iPhone 14",
        "system_version": "iOS 16.4.1",
        "app_version": "8.8.4",
        "lang_code": "ru",
        "system_lang_code": "ru-RU",
    },
    {
        "device_model": "iPhone 13 Pro",
        "system_version": "iOS 15.7.2",
        "app_version": "8.7.4",
        "lang_code": "en",
        "system_lang_code": "en-US",
    },
    {
        "device_model": "iPhone 12",
        "system_version": "iOS 15.6",
        "app_version": "8.6.3",
        "lang_code": "ru",
        "system_lang_code": "ru-RU",
    },
    {
        "device_model": "iPhone 11",
        "system_version": "iOS 14.8.1",
        "app_version": "8.5.2",
        "lang_code": "en",
        "system_lang_code": "en-US",
    },
    # --- Android (Samsung) ---
    {
        "device_model": "Samsung Galaxy S22",
        "system_version": "Android 13",
        "app_version": "9.0.1",
        "lang_code": "ru",
        "system_lang_code": "ru-RU",
    },
    {
        "device_model": "Samsung Galaxy S21",
        "system_version": "Android 12",
        "app_version": "8.9.2",
        "lang_code": "en",
        "system_lang_code": "en-US",
    },
    {
        "device_model": "Samsung Galaxy A53",
        "system_version": "Android 13",
        "app_version": "9.1.3",
        "lang_code": "ru",
        "system_lang_code": "ru-RU",
    },
    {
        "device_model": "Samsung Galaxy S20",
        "system_version": "Android 11",
        "app_version": "8.6.1",
        "lang_code": "ru",
        "system_lang_code": "ru-RU",
    },
    # --- Android (Xiaomi) ---
    {
        "device_model": "Xiaomi Redmi Note 11",
        "system_version": "Android 12",
        "app_version": "8.8.3",
        "lang_code": "ru",
        "system_lang_code": "ru-RU",
    },
    {
        "device_model": "Xiaomi 13",
        "system_version": "Android 13",
        "app_version": "9.1.4",
        "lang_code": "en",
        "system_lang_code": "en-GB",
    },
    {
        "device_model": "Xiaomi Mi 11",
        "system_version": "Android 12",
        "app_version": "8.7.5",
        "lang_code": "ru",
        "system_lang_code": "ru-RU",
    },
    # --- Android (другие) ---
    {
        "device_model": "Pixel 7",
        "system_version": "Android 14",
        "app_version": "9.2.3",
        "lang_code": "en",
        "system_lang_code": "en-US",
    },
    {
        "device_model": "OnePlus 10 Pro",
        "system_version": "Android 13",
        "app_version": "9.0.2",
        "lang_code": "en",
        "system_lang_code": "en-US",
    },
    {
        "device_model": "Huawei P50 Pro",
        "system_version": "Android 12",
        "app_version": "8.7.4",
        "lang_code": "ru",
        "system_lang_code": "ru-RU",
    },
]


def generate_random_fingerprint() -> Dict[str, str]:
    """Случайно выбирает реалистичный отпечаток устройства из пула.

    Возвращает dict со всеми пятью Telethon-параметрами. Никаких
    внешних вызовов и БД — чистая функция, удобно для тестов.
    """
    return dict(random.choice(FINGERPRINT_DEVICE_POOL))


def format_fingerprint_text(fingerprint: Optional[Dict[str, Any]]) -> str:
    """Красиво отформатировать отпечаток для вывода в боте."""
    if not fingerprint:
        return (
            f"{emoji('WARNING')} Отпечаток не задан — "
            f"будет сгенерирован автоматически."
        )
    updated = fingerprint.get('fingerprint_updated_at')
    updated_str = "—"
    if updated:
        try:
            updated_str = updated.strftime('%d.%m.%Y %H:%M')
        except Exception:
            updated_str = "—"

    return (
        f"{emoji('PHONE')} <b>Модель:</b> "
        f"<code>{fingerprint.get('device_model') or '—'}</code>\n"
        f"{emoji('STATS')} <b>ОС:</b> "
        f"<code>{fingerprint.get('system_version') or '—'}</code>\n"
        f"{emoji('AI')} <b>App:</b> "
        f"<code>{fingerprint.get('app_version') or '—'}</code>\n"
        f"{emoji('GLOBE')} <b>Язык приложения:</b> "
        f"<code>{fingerprint.get('lang_code') or '—'}</code>\n"
        f"{emoji('GLOBE')} <b>Язык системы:</b> "
        f"<code>{fingerprint.get('system_lang_code') or '—'}</code>\n"
        f"{emoji('CLOCK')} <b>Обновлён:</b> {updated_str}"
    )


async def get_account_fingerprint(account_id: int) -> Optional[Dict[str, Any]]:
    """Достать сохранённый отпечаток из БД. None, если не задан."""
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                '''SELECT device_model, system_version, app_version,
                          lang_code, system_lang_code,
                          fingerprint_updated_at
                   FROM accounts WHERE id = $1''',
                account_id
            )
            if not row:
                return None
            data = dict(row)
            # Если все пять Telethon-полей пустые — считаем, что
            # отпечаток не задан (неактивный аккаунт или legacy).
            if not any([
                data.get('device_model'),
                data.get('system_version'),
                data.get('app_version'),
            ]):
                return None
            return data
    except Exception as ex:
        logger.error(f"get_account_fingerprint failed: {ex}")
        return None


async def set_account_fingerprint(
    account_id: int, fingerprint: Dict[str, str]
) -> bool:
    """Записать конкретный отпечаток в БД."""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                '''UPDATE accounts SET
                    device_model = $1,
                    system_version = $2,
                    app_version = $3,
                    lang_code = $4,
                    system_lang_code = $5,
                    fingerprint_updated_at = NOW()
                   WHERE id = $6''',
                fingerprint.get('device_model'),
                fingerprint.get('system_version'),
                fingerprint.get('app_version'),
                fingerprint.get('lang_code'),
                fingerprint.get('system_lang_code'),
                account_id,
            )
        return True
    except Exception as ex:
        logger.error(f"set_account_fingerprint failed: {ex}")
        return False


async def regenerate_account_fingerprint(
    account_id: int
) -> Optional[Dict[str, str]]:
    """Сгенерировать новый случайный отпечаток и сразу сохранить."""
    fp = generate_random_fingerprint()
    ok = await set_account_fingerprint(account_id, fp)
    if not ok:
        return None
    # Сбрасываем активный Telethon-клиент, чтобы при следующем
    # подключении использовались новые параметры.
    try:
        if account_id in active_clients:
            client = active_clients.pop(account_id)
            try:
                if client.is_connected():
                    await client.disconnect()
            except Exception:
                pass
    except Exception:
        pass
    return fp


def get_fingerprint_keyboard(account_id: int) -> InlineKeyboardMarkup:
    """Клавиатура меню отпечатка."""
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Сгенерировать новый",
        callback_data=f"fingerprint_regen_{account_id}",
        style='primary',
        icon_custom_emoji_id=get_icon("REPEAT")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data=f"manage_account_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()


def get_fingerprint_regen_keyboard(
    account_id: int
) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения перегенерации."""
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Да, сменить отпечаток",
        callback_data=f"fingerprint_regen_go_{account_id}",
        style='danger',
        icon_custom_emoji_id=get_icon("REPEAT")
    ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data=f"fingerprint_menu_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()


# =====================================================================
# B1 — Дашборд здоровья аккаунта
# =====================================================================
# На одной карточке показываем: 14-дневный график активности,
# последние flood-wait-ы, сколько отправлено/получено сообщений,
# риск-скор. Источники: account_logs, flood_wait_history, accounts.
# =====================================================================


async def get_account_health(
    account_id: int
) -> Optional[Dict[str, Any]]:
    """Собрать данные для дашборда. None, если аккаунт не найден."""
    try:
        async with db_pool.acquire() as conn:
            account = await conn.fetchrow(
                '''SELECT id, phone, is_active, created_at,
                          warming_enabled, warming_cycles
                   FROM accounts WHERE id = $1''',
                account_id
            )
            if not account:
                return None

            # 14-дневный разрез активности.
            daily = await conn.fetch(
                '''SELECT DATE(created_at AT TIME ZONE 'Europe/Moscow') AS day,
                          COUNT(*) AS cnt
                   FROM account_logs
                   WHERE account_id = $1
                     AND created_at > NOW() - INTERVAL '14 days'
                   GROUP BY day
                   ORDER BY day''',
                account_id
            )
            daily_map = {r['day']: r['cnt'] for r in daily}

            # Все 14 дней подряд, даже пустые.
            today = datetime.now(MSK_TZ).date()
            daily_series = []
            for i in range(13, -1, -1):
                d = today - timedelta(days=i)
                daily_series.append({
                    'day': d,
                    'cnt': daily_map.get(d, 0),
                })

            # Flood-wait-ы за 7 дней.
            floods = await conn.fetch(
                '''SELECT seconds, occurred_at,
                          COALESCE(chat_id, 0) AS chat_id
                   FROM flood_wait_history
                   WHERE account_id = $1
                     AND occurred_at > NOW() - INTERVAL '7 days'
                   ORDER BY occurred_at DESC
                   LIMIT 10''',
                account_id
            )

            # Суммарная статистика сообщений.
            sent_total = await conn.fetchval(
                '''SELECT COUNT(*) FROM account_logs
                   WHERE account_id = $1 AND direction = 'outgoing' ''',
                account_id
            )
            recv_total = await conn.fetchval(
                '''SELECT COUNT(*) FROM account_logs
                   WHERE account_id = $1 AND direction = 'incoming' ''',
                account_id
            )
            sent_today = await conn.fetchval(
                '''SELECT COUNT(*) FROM account_logs
                   WHERE account_id = $1 AND direction = 'outgoing'
                     AND created_at > NOW() - INTERVAL '24 hours' ''',
                account_id
            )
            sent_week = await conn.fetchval(
                '''SELECT COUNT(*) FROM account_logs
                   WHERE account_id = $1 AND direction = 'outgoing'
                     AND created_at > NOW() - INTERVAL '7 days' ''',
                account_id
            )

            # Суммарный «вес» flood-wait-ов за 7 дней.
            flood_sum_week = await conn.fetchval(
                '''SELECT COALESCE(SUM(seconds), 0) FROM flood_wait_history
                   WHERE account_id = $1
                     AND occurred_at > NOW() - INTERVAL '7 days' ''',
                account_id
            )

        # Простой риск-скор: чем больше flood-wait-ов, тем хуже.
        # 0  — всё чисто, 100 — вчера забанили бы.
        flood_count = len(floods)
        if flood_count == 0:
            risk_score = 0
        elif flood_count < 3:
            risk_score = min(20 + flood_sum_week // 60, 40)
        elif flood_count < 6:
            risk_score = 40 + min(flood_sum_week // 30, 30)
        else:
            risk_score = 70 + min(flood_sum_week // 20, 30)
        risk_score = min(risk_score, 100)

        return {
            'account': dict(account),
            'daily': daily_series,
            'floods': [dict(f) for f in floods],
            'flood_count_week': flood_count,
            'flood_sum_week': flood_sum_week,
            'sent_total': sent_total or 0,
            'recv_total': recv_total or 0,
            'sent_today': sent_today or 0,
            'sent_week': sent_week or 0,
            'risk_score': risk_score,
        }
    except Exception as ex:
        logger.error(f"get_account_health failed: {ex}")
        return None


def _risk_label(score: int) -> str:
    if score < 20:
        return "🟢 Низкий"
    if score < 50:
        return "🟡 Умеренный"
    if score < 75:
        return "🟠 Повышенный"
    return "🔴 Высокий"


def _format_activity_bars(daily: List[Dict[str, Any]]) -> str:
    """ASCII-бар-чарт активности за 14 дней."""
    if not daily:
        return "<i>недостаточно данных</i>"
    max_cnt = max((d['cnt'] for d in daily), default=0) or 1
    lines = []
    for d in daily:
        bar_len = int(round((d['cnt'] / max_cnt) * 10)) if max_cnt else 0
        bar = "▇" * bar_len + "·" * (10 - bar_len)
        day_label = d['day'].strftime('%d.%m')
        lines.append(f"<code>{day_label}</code> {bar} {d['cnt']}")
    return "\n".join(lines)


def format_health_dashboard(health: Dict[str, Any]) -> str:
    """Собрать финальный текст дашборда."""
    acc = health['account']
    status = "Активен" if acc['is_active'] else "Неактивен"
    created = acc['created_at'].strftime('%d.%m.%Y')
    warming = "Вкл" if acc.get('warming_enabled') else "Выкл"
    cycles = acc.get('warming_cycles') or 0

    floods_text = "—"
    if health['floods']:
        items = []
        for f in health['floods'][:5]:
            ts = f['occurred_at'].strftime('%d.%m %H:%M')
            items.append(f"  • {ts} — {f['seconds']}с")
        floods_text = "\n".join(items)
    elif health['flood_count_week'] == 0:
        floods_text = f"{emoji('CHECK')} за 7 дней — чисто"

    bar_chart = _format_activity_bars(health['daily'])

    text = (
        f"{emoji('STATS')} <b>Дашборд здоровья аккаунта</b>\n\n"
        f"{emoji('PHONE')} Телефон: <code>{acc['phone']}</code>\n"
        f"{emoji('EYE')} Статус: {status}\n"
        f"{emoji('CLOCK')} Создан: {created}\n"
        f"{emoji('FIRE')} Прогрев: {warming} (циклов: {cycles})\n\n"
        f"{emoji('STATS')} <b>Риск-скор: {health['risk_score']}/100</b> — "
        f"{_risk_label(health['risk_score'])}\n"
        f"{emoji('WARNING')} FloodWait за 7 дней: "
        f"<b>{health['flood_count_week']}</b> шт., "
        f"суммарно <b>{health['flood_sum_week']}с</b>\n"
        f"{floods_text}\n\n"
        f"{emoji('CHART')} <b>Активность (14 дней):</b>\n"
        f"{bar_chart}\n\n"
        f"{emoji('OUTBOX')} Отправлено:\n"
        f"  • сегодня: <b>{health['sent_today']}</b>\n"
        f"  • за 7 дней: <b>{health['sent_week']}</b>\n"
        f"  • всего: <b>{health['sent_total']}</b>\n"
        f"{emoji('INBOX')} Получено всего: <b>{health['recv_total']}</b>"
    )
    return text


def get_health_dashboard_keyboard(
    account_id: int
) -> InlineKeyboardMarkup:
    """Клавиатура дашборда."""
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Обновить",
        callback_data=f"account_dashboard_{account_id}",
        style='primary',
        icon_custom_emoji_id=get_icon("REPEAT")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data=f"manage_account_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()


# --- Хендлеры A3 и B1 ---

@dp.callback_query(F.data.startswith("fingerprint_menu_"))
async def fingerprint_menu(callback: CallbackQuery):
    """Показать текущий отпечаток аккаунта."""
    account_id = int(callback.data.split("_")[2])
    account = await get_account(account_id)
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    fingerprint = await get_account_fingerprint(account_id)
    text = (
        f"{emoji('PHONE')} <b>Отпечаток устройства</b>\n\n"
        f"Это параметры, под которыми Telegram видит этот аккаунт. "
        f"У разных аккаунтов должны быть разные — иначе антифрод "
        f"свяжет их между собой.\n\n"
        f"{format_fingerprint_text(fingerprint)}"
    )
    try:
        await callback.message.edit_text(
            text,
            reply_markup=get_fingerprint_keyboard(account_id)
        )
    except Exception:
        await callback.message.answer(
            text,
            reply_markup=get_fingerprint_keyboard(account_id)
        )
    await callback.answer()


@dp.callback_query(F.data.startswith("fingerprint_regen_go_"))
async def fingerprint_regen_confirm(
    callback: CallbackQuery, state: FSMContext
):
    """Запустить переавторизацию аккаунта с новым отпечатком.

    ВАЖНО: зарегистрирован РАНЬШЕ базового fingerprint_regen_,
    потому что fingerprint_regen_go_<id> тоже удовлетворяет
    startswith("fingerprint_regen_"). В aiogram 3 первый
    зарегистрированный подходящий хендлер забирает callback.

    Сам по себе отпечаток (device_model, system_version, app_version,
    lang_code, system_lang_code) — это только метаданные, которые
    Telethon отправляет в Telegram ОДИН раз при создании сессии.
    Чтобы в «Активных сессиях» в Telegram-клиенте появилось
    новое устройство, нужно создать НОВЫЙ auth_key — то есть
    пройти send_code_request → sign_in заново, с новыми
    параметрами устройства. Этот хендлер запускает именно это.
    """
    account_id = int(callback.data.split("_")[3])
    account = await get_account(account_id)
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return

    # 1. Генерируем и сразу сохраняем новый отпечаток — он
    #    применится к клиенту, через который будем логиниться.
    new_fp = generate_random_fingerprint()
    ok = await set_account_fingerprint(account_id, new_fp)
    if not ok:
        await callback.answer(
            "Не удалось сохранить отпечаток. Попробуй позже.",
            show_alert=True
        )
        return

    # 2. Сбрасываем активный Telethon-клиент, чтоб не висел
    #    со старыми параметрами.
    if account_id in active_clients:
        try:
            old_client = active_clients.pop(account_id)
            if old_client.is_connected():
                await old_client.disconnect()
        except Exception:
            pass

    # 3. Подтягиваем прокси аккаунта, если привязан.
    proxy = None
    if account.get('proxy_id'):
        proxy = await get_proxy(account['proxy_id'])

    # 4. Создаём клиент с ПУСТОЙ StringSession (новый auth_key)
    #    и новыми параметрами устройства, отправляем код.
    try:
        client = await create_telethon_client(
            '', proxy=proxy, fingerprint=new_fp
        )
        await client.connect()
        sent = await client.send_code_request(account['phone'])

        # Сохраняем в state всё, что понадобится в хендлерах
        # ввода кода / 2FA. Сам клиент не держим — он нам
        # не нужен до момента sign_in (Telethon переиспользует
        # сохранённый StringSession).
        await state.update_data(
            account_id=account_id,
            phone=account['phone'],
            client_session=client.session.save(),
            phone_code_hash=sent.phone_code_hash,
            dc_id=client.session.dc_id,
            proxy_id=account.get('proxy_id'),
        )
        try:
            await client.disconnect()
        except Exception:
            pass

        cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Отмена",
                callback_data=f"manage_account_{account_id}",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])

        await callback.message.edit_text(
            f"{emoji('PHONE')} <b>Переавторизация с новым отпечатком</b>\n\n"
            f"Новый девайс для Telegram:\n"
            f"  • <code>{new_fp['device_model']}</code>\n"
            f"  • <code>{new_fp['system_version']}</code>, "
            f"app <code>{new_fp['app_version']}</code>\n"
            f"  • язык <code>{new_fp['lang_code']}</code>, "
            f"система <code>{new_fp['system_lang_code']}</code>\n\n"
            f"{emoji('KEY')} Код отправлен на "
            f"<code>{account['phone']}</code>. Введи его из Telegram:",
            reply_markup=cancel_kb
        )
        await state.set_state(FingerprintRegenStates.waiting_for_code)
    except Exception as ex:
        logger.error(f"fingerprint_regen_confirm send_code failed: {ex}")
        # Откатываем отпечаток в БД, чтобы не было разъезда
        # между fingerprint (новый) и session_string (старый).
        await callback.message.edit_text(
            f"{emoji('CROSS')} Не удалось начать переавторизацию: "
            f"<code>{escape(str(ex))}</code>\n\n"
            f"Возможно, TG уже запрашивал код недавно — подожди "
            f"пару минут и попробуй снова.",
            reply_markup=get_fingerprint_keyboard(account_id)
        )
    await callback.answer()


@dp.message(FingerprintRegenStates.waiting_for_code)
async def fingerprint_reauth_code(message: Message, state: FSMContext):
    """Принять код и завершить переавторизацию, либо запросить 2FA."""
    code = message.text.strip()
    data = await state.get_data()
    account_id = data.get('account_id')

    proxy = None
    if data.get('proxy_id'):
        proxy = await get_proxy(data['proxy_id'])

    # Берём самый свежий отпечаток из БД — именно с ним
    # создавался клиент.
    fingerprint = await get_account_fingerprint(account_id)

    try:
        client = await create_telethon_client(
            data['client_session'], proxy=proxy,
            fingerprint=fingerprint
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
                        callback_data=f"manage_account_{account_id}",
                        style='default',
                        icon_custom_emoji_id=get_icon("BACK")
                    )
                ]])
            )
            await state.set_state(FingerprintRegenStates.waiting_for_2fa)
            return

        # Успех: сохраняем новый session_string.
        new_session = client.session.save()
        new_dc_id = data.get('dc_id') or client.session.dc_id
        try:
            await client.disconnect()
        except Exception:
            pass

        async with db_pool.acquire() as conn:
            await conn.execute(
                'UPDATE accounts SET session_string = $1, dc_id = $2 '
                'WHERE id = $3',
                new_session, new_dc_id, account_id
            )

        await state.clear()
        await message.answer(
            f"{emoji('CHECK')} <b>Готово!</b> Аккаунт переавторизован "
            f"с новым отпечатком.\n\n"
            f"Открой Telegram → Настройки → Устройства — там появится "
            f"новая сессия с именем "
            f"<code>{fingerprint.get('device_model') if fingerprint else 'новое устройство'}</code>.\n\n"
            f"<i>Старая сессия тоже останется в списке, пока не истечёт "
            f"или пока ты её не завершишь вручную — это нормально.</i>",
            reply_markup=get_fingerprint_keyboard(account_id)
        )
    except Exception as ex:
        logger.error(f"fingerprint_reauth_code failed: {ex}")
        await message.answer(
            f"{emoji('CROSS')} Ошибка: <code>{escape(str(ex))}</code>\n\n"
            f"Попробуй ещё раз или отмени операцию.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Отмена",
                    callback_data=f"manage_account_{account_id}",
                    style='default',
                    icon_custom_emoji_id=get_icon("BACK")
                )
            ]])
        )
        # state НЕ чистим — пусть юзер попробует ещё раз ввести код.


@dp.message(FingerprintRegenStates.waiting_for_2fa)
async def fingerprint_reauth_2fa(message: Message, state: FSMContext):
    """Принять 2FA-пароль и завершить переавторизацию."""
    password = message.text.strip()
    data = await state.get_data()
    account_id = data.get('account_id')

    proxy = None
    if data.get('proxy_id'):
        proxy = await get_proxy(data['proxy_id'])

    fingerprint = await get_account_fingerprint(account_id)

    try:
        client = await create_telethon_client(
            data['client_session'], proxy=proxy,
            fingerprint=fingerprint
        )
        await client.connect()
        await client.sign_in(password=password)

        new_session = client.session.save()
        new_dc_id = data.get('dc_id') or client.session.dc_id
        try:
            await client.disconnect()
        except Exception:
            pass

        async with db_pool.acquire() as conn:
            await conn.execute(
                'UPDATE accounts SET session_string = $1, dc_id = $2 '
                'WHERE id = $3',
                new_session, new_dc_id, account_id
            )

        await state.clear()
        # Стираем следы 2FA-пароля в чате.
        try:
            await message.delete()
        except Exception:
            pass
        # Шлём подтверждение отдельным сообщением.
        await message.answer(
            f"{emoji('CHECK')} <b>Готово!</b> Аккаунт переавторизован "
            f"с новым отпечатком.\n\n"
            f"Новое устройство: <code>"
            f"{fingerprint.get('device_model') if fingerprint else '—'}"
            f"</code>.\n"
            f"Старая сессия в Telegram ещё отображается — заверши "
            f"её вручную или дождись автоистечения.",
            reply_markup=get_fingerprint_keyboard(account_id)
        )
    except Exception as ex:
        logger.error(f"fingerprint_reauth_2fa failed: {ex}")
        await message.answer(
            f"{emoji('CROSS')} Ошибка: <code>{escape(str(ex))}</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Отмена",
                    callback_data=f"manage_account_{account_id}",
                    style='default',
                    icon_custom_emoji_id=get_icon("BACK")
                )
            ]])
        )


@dp.callback_query(F.data.startswith("fingerprint_regen_"))
async def fingerprint_regen(callback: CallbackQuery):
    """Запросить подтверждение перегенерации."""
    account_id = int(callback.data.split("_")[2])
    account = await get_account(account_id)
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    text = (
        f"{emoji('WARNING')} <b>Сменить отпечаток?</b>\n\n"
        f"Бот выберет новый отпечаток и попросит Telegram заново "
        f"прислать код на телефон — это единственный способ, "
        f"чтобы в «Активных сессиях» в клиенте Telegram "
        f"появилось новое устройство с новым именем.\n\n"
        f"<b>Что будет:</b>\n"
        f"  • старый Telethon-клиент отключится\n"
        f"  • тебе придёт SMS/код в Telegram\n"
        f"  • ты введёшь код (и 2FA, если включена)\n"
        f"  • аккаунт перезайдёт с новым устройством\n\n"
        f"<i>Запущенные рассылки/прогрев продолжатся после "
        f"переподключения. Старая сессия останется в списке "
        f"устройств до ручного завершения — это норма.</i>"
    )
    try:
        await callback.message.edit_text(
            text,
            reply_markup=get_fingerprint_regen_keyboard(account_id)
        )
    except Exception:
        pass
    await callback.answer()


@dp.callback_query(F.data.startswith("account_dashboard_"))
async def account_dashboard(callback: CallbackQuery):
    """Дашборд здоровья аккаунта (B1)."""
    account_id = int(callback.data.split("_")[2])
    account = await get_account(account_id)
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    health = await get_account_health(account_id)
    if not health:
        await callback.answer(
            "Не удалось собрать статистику.", show_alert=True
        )
        return
    text = format_health_dashboard(health)
    try:
        await callback.message.edit_text(
            text,
            reply_markup=get_health_dashboard_keyboard(account_id)
        )
    except Exception:
        pass
    await callback.answer()


# --- Сохранённые маршруты для Telegram-ботов ---
SCRIPT_ALLOWED_BUTTON_KINDS = {
    'KeyboardButtonCallback',
    'KeyboardButton',
}
SCRIPT_BOT_RESPONSE_TIMEOUT = 15.0
SCRIPT_CAPTCHA_IMAGE_MAX_BYTES = 4 * 1024 * 1024
SCRIPT_CAPTCHA_MAX_ATTEMPTS = 3
SCRIPT_TELEGRAM_HOSTS = {'t.me', 'telegram.me', 'telegram.dog'}
SCRIPT_CAPTCHA_SYSTEM_PROMPT = (
    'Ты решаешь капчу Telegram-бота. Тебе даны фото, текст сообщения и кнопки. '
    'Верни только JSON без markdown. Если ответ нужно выбрать кнопкой: '
    '{"action":"click","button_index":0}. Если нужно отправить текст: '
    '{"action":"send","answer":"текст"}. Не добавляй пояснений.'
)


def parse_telegram_bot_url(value: str) -> Dict[str, str]:
    """Разбирает @bot, botname или t.me/bot?start=payload."""
    raw = (value or '').strip()
    if not raw:
        raise ValueError('Укажите ссылку на Telegram-бота')

    username = ''
    payload = ''
    if raw.startswith('@'):
        username = raw[1:]
    elif re.fullmatch(r'[A-Za-z0-9_]{5,32}', raw):
        username = raw
    elif raw.lower().startswith('tg://'):
        parsed = urlparse(raw)
        query = parse_qs(parsed.query)
        if parsed.netloc.lower() != 'resolve':
            raise ValueError('Поддерживается только tg://resolve')
        username = (query.get('domain') or [''])[0]
        payload = (query.get('start') or [''])[0]
    else:
        candidate = raw if '://' in raw else 'https://' + raw
        parsed = urlparse(candidate)
        host = (parsed.hostname or '').lower().removeprefix('www.')
        if host not in SCRIPT_TELEGRAM_HOSTS:
            raise ValueError('Разрешены только ссылки t.me, telegram.me или @username')
        parts = [part for part in parsed.path.split('/') if part]
        if not parts:
            raise ValueError('В ссылке отсутствует username бота')
        username = parts[0].lstrip('@')
        query = parse_qs(parsed.query)
        payload = (query.get('start') or [''])[0]
        if query.get('startapp') and not payload:
            raise ValueError('Ссылки startapp открывают Mini App и не поддерживаются серверным скриптом')

    username = username.strip().lstrip('@')
    if not re.fullmatch(r'[A-Za-z0-9_]{5,32}', username):
        raise ValueError('Некорректный username Telegram-бота')
    if payload:
        payload = payload.strip()
        if len(payload) > 128 or not re.fullmatch(r'[A-Za-z0-9_-]+', payload):
            raise ValueError('Некорректный start-параметр в ссылке')
    return {'bot_username': username, 'start_payload': payload, 'bot_url': raw}


def _is_script_channel_url(url: str) -> bool:
    raw = (url or '').strip()
    if not raw:
        return False
    candidate = raw if '://' in raw else 'https://' + raw
    parsed = urlparse(candidate)
    host = (parsed.hostname or '').lower().removeprefix('www.')
    if host not in SCRIPT_TELEGRAM_HOSTS:
        return False
    path = parsed.path.strip('/')
    return bool(path and not path.startswith('share/'))


def extract_script_buttons(message: Any) -> List[Dict[str, Any]]:
    """Преобразует кнопки Telethon в снимок и отмечает доступное действие."""
    result: List[Dict[str, Any]] = []
    for row_index, row in enumerate(message.buttons or []):
        for col_index, button in enumerate(row):
            raw_button = getattr(button, 'button', None)
            kind = type(raw_button).__name__
            text = str(getattr(button, 'text', '') or '').strip() or 'Без названия'
            url = getattr(button, 'url', None) or getattr(raw_button, 'url', None) or ''
            url = str(url)[:500] if url else ''
            action = ''
            if kind in SCRIPT_ALLOWED_BUTTON_KINDS:
                action = 'click'
            elif _is_script_channel_url(url):
                action = 'join_channel'
            result.append({
                'row': row_index,
                'col': col_index,
                'text': text,
                'kind': kind,
                'url': url,
                'action': action,
                'selectable': action == 'click',
                'actionable': bool(action),
            })
    return result


def _script_message_text(message: Any) -> str:
    return (getattr(message, 'raw_text', None) or getattr(message, 'message', None) or '')[:1500]


def _script_menu_from_message(parsed: Dict[str, str], entity: Any, message: Any) -> Dict[str, Any]:
    return {
        **parsed,
        'entity': entity,
        'message': message,
        'message_id': int(message.id),
        'message_text': _script_message_text(message),
        'has_photo': bool(getattr(message, 'photo', None)),
        'buttons': extract_script_buttons(message),
    }


async def _get_script_bot_entity(client: TelegramClient, bot_username: str):
    try:
        entity = await client.get_entity(bot_username)
    except Exception as ex:
        raise RuntimeError(f'Бот @{bot_username} не найден') from ex
    if not isinstance(entity, User) or not getattr(entity, 'bot', False):
        raise ValueError(f'@{bot_username} не является Telegram-ботом')
    return entity


async def _script_message_snapshot(client: TelegramClient, entity: Any) -> Tuple[int, Dict[int, Any]]:
    before = await client.get_messages(entity, limit=15)
    return (
        max((int(item.id) for item in before), default=0),
        {int(item.id): getattr(item, 'edit_date', None) for item in before},
    )


async def _wait_for_script_bot_response(
    client: TelegramClient,
    entity: Any,
    parsed: Dict[str, str],
    before_id: int,
    before_edits: Dict[int, Any],
    *,
    require_buttons: bool = True,
) -> Dict[str, Any]:
    deadline = time.monotonic() + SCRIPT_BOT_RESPONSE_TIMEOUT
    latest_response = None
    while time.monotonic() < deadline:
        messages = await client.get_messages(entity, limit=15)
        for message in messages:
            message_id = int(message.id)
            was_edited = (
                message_id in before_edits
                and getattr(message, 'edit_date', None) is not None
                and getattr(message, 'edit_date', None) != before_edits[message_id]
            )
            if message.out or (message_id <= before_id and not was_edited):
                continue
            latest_response = message
            menu = _script_menu_from_message(parsed, entity, message)
            if menu['buttons'] or menu['has_photo'] or not require_buttons:
                return menu
        await asyncio.sleep(0.7)
    if latest_response is not None:
        raise RuntimeError('Бот ответил, но в новом сообщении нет кнопок')
    raise TimeoutError(f"Бот @{parsed['bot_username']} не ответил за {int(SCRIPT_BOT_RESPONSE_TIMEOUT)} секунд")


async def load_script_bot_menu(account_id: int, bot_url: str) -> Dict[str, Any]:
    """Открывает бота через /start и загружает первое актуальное меню."""
    parsed = parse_telegram_bot_url(bot_url)
    client = await get_client_for_account(account_id)
    if not client:
        raise RuntimeError('Не удалось подключиться к выбранному аккаунту')
    entity = await _get_script_bot_entity(client, parsed['bot_username'])
    before_id, before_edits = await _script_message_snapshot(client, entity)
    command = '/start' + (f" {parsed['start_payload']}" if parsed['start_payload'] else '')
    await client.send_message(entity, command)
    return await _wait_for_script_bot_response(
        client, entity, parsed, before_id, before_edits, require_buttons=True,
    )


async def _get_current_script_message(client: TelegramClient, entity: Any, message_id: int):
    message = await client.get_messages(entity, ids=message_id)
    if not message:
        raise RuntimeError('Текущее сообщение бота не найдено; начните маршрут заново')
    return message


def find_script_button(message: Any, step: Dict[str, Any]) -> Any:
    """Находит кнопку по позиции, а при изменении меню — по уникальному тексту."""
    row_index = int(step.get('row', -1))
    col_index = int(step.get('col', -1))
    expected_text = str(step.get('text') or '')
    rows = message.buttons or []
    if 0 <= row_index < len(rows) and 0 <= col_index < len(rows[row_index]):
        candidate = rows[row_index][col_index]
        kind = type(getattr(candidate, 'button', None)).__name__
        text = str(getattr(candidate, 'text', '') or '').strip() or 'Без названия'
        if kind in SCRIPT_ALLOWED_BUTTON_KINDS:
            # Приоритет у позиции: бот может переименовать кнопку, но если
            # на её прежнем месте появилась другая обычная кнопка — маршрут
            # продолжает работу по текущему расположению.
            if text != expected_text:
                logger.info(
                    'Script button text changed at %s:%s: %r -> %r; using position',
                    row_index, col_index, expected_text, text,
                )
            return candidate
    matches = []
    for row in rows:
        for candidate in row:
            kind = type(getattr(candidate, 'button', None)).__name__
            text = str(getattr(candidate, 'text', '') or '').strip() or 'Без названия'
            if kind in SCRIPT_ALLOWED_BUTTON_KINDS and text == expected_text:
                matches.append(candidate)
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise RuntimeError('Меню изменилось: найдено несколько кнопок с сохранённым текстом')
    raise RuntimeError('Сохранённая кнопка больше не найдена. Обновите маршрут скрипта')


async def _click_script_step(
    client: TelegramClient,
    menu: Dict[str, Any],
    step: Dict[str, Any],
    *,
    require_buttons: bool,
) -> Dict[str, Any]:
    message = menu['message']
    before_id, before_edits = await _script_message_snapshot(client, menu['entity'])
    selected = find_script_button(message, step)
    await selected.click()
    return await _wait_for_script_bot_response(
        client,
        menu['entity'],
        menu,
        before_id,
        before_edits,
        require_buttons=require_buttons,
    )


async def _script_join_channel_url(client: TelegramClient, url: str) -> None:
    parsed = urlparse(url if '://' in url else 'https://' + url)
    path = parsed.path.strip('/')
    if not path:
        raise ValueError('Кнопка не содержит ссылку на канал')
    try:
        if path.startswith('+') or path.startswith('joinchat/'):
            invite_hash = path[1:] if path.startswith('+') else path.split('/', 1)[1]
            await client(ImportChatInviteRequest(invite_hash))
        else:
            username = path.split('/', 1)[0]
            entity = await client.get_entity('@' + username.lstrip('@'))
            await client(JoinChannelRequest(entity))
    except RPCError as ex:
        code = str(ex).upper()
        already_joined = (
            ex.__class__.__name__ == 'UserAlreadyParticipantError'
            or 'USER_ALREADY_PARTICIPANT' in code
            or 'ALREADY A PARTICIPANT' in code
        )
        if not already_joined:
            raise RuntimeError(f'Не удалось подписаться по кнопке: {str(ex)[:500]}') from ex


def _script_captcha_json(raw: str) -> Dict[str, Any]:
    text = (raw or '').strip()
    fence = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.S)
    if fence:
        text = fence.group(1)
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


async def _solve_script_photo_captcha(
    client: TelegramClient,
    menu: Dict[str, Any],
    *,
    require_buttons: bool,
) -> Dict[str, Any]:
    message = menu['message']
    if not getattr(message, 'photo', None):
        raise RuntimeError('Указана фото-капча, но в сообщении бота нет фото')
    image = await client.download_media(message, bytes)
    if not isinstance(image, bytes) or not image:
        raise RuntimeError('Не удалось скачать фото капчи')
    if len(image) > SCRIPT_CAPTCHA_IMAGE_MAX_BYTES:
        raise RuntimeError('Фото капчи слишком большое для AI-анализа')

    buttons = extract_script_buttons(message)
    buttons_for_ai = [
        {
            'index': index,
            'text': item['text'],
            'kind': item['kind'],
            'action': item['action'],
            'url': item['url'],
        }
        for index, item in enumerate(buttons)
    ]
    runtime = await get_global_llm_runtime()
    content = [
        {
            'type': 'text',
            'text': (
                'Текст/подпись сообщения с капчей:\n'
                f"{_script_message_text(message) or '—'}\n\n"
                'Кнопки под сообщением:\n'
                + json.dumps(buttons_for_ai, ensure_ascii=False)
            ),
        },
        {
            'type': 'image',
            'source': {
                'type': 'base64',
                'media_type': 'image/jpeg',
                'data': base64.b64encode(image).decode('ascii'),
            },
        },
    ]
    try:
        ai_client = anthropic.AsyncAnthropic(
            api_key=runtime['api_key'],
            base_url=runtime['base_url'],
            timeout=min(LLM_TIMEOUT, 60),
        )
        response = await ai_client.messages.create(
            model=runtime['default_model'],
            max_tokens=160,
            system=SCRIPT_CAPTCHA_SYSTEM_PROMPT,
            messages=[{'role': 'user', 'content': content}],
        )
    except anthropic.APIStatusError as ex:
        raise RuntimeError(f'AI-капча HTTP {ex.status_code}: {str(ex)[:500]}') from ex
    except anthropic.APIError as ex:
        raise RuntimeError(f'AI-капча {ex.__class__.__name__}: {str(ex)[:500]}') from ex

    raw_answer = _extract_llm_response_text(response)
    data = _script_captcha_json(raw_answer)
    before_id, before_edits = await _script_message_snapshot(client, menu['entity'])
    action = str(data.get('action') or '').lower()
    if action == 'click':
        try:
            index = int(data.get('button_index'))
        except (TypeError, ValueError):
            raise RuntimeError(f'AI-капча не вернула корректный button_index: {raw_answer[:300]}')
        if not 0 <= index < len(buttons):
            raise RuntimeError(f'AI-капча выбрала несуществующую кнопку: {index}')
        step = buttons[index]
        if step.get('action') != 'click':
            raise RuntimeError('AI-капча выбрала кнопку, которую нельзя нажать сервером')
        selected = find_script_button(message, step)
        await selected.click()
    elif action == 'send':
        answer = str(data.get('answer') or '').strip()
        if not answer:
            raise RuntimeError(f'AI-капча не вернула текстовый ответ: {raw_answer[:300]}')
        await client.send_message(menu['entity'], answer[:500])
    else:
        raise RuntimeError(f'AI-капча вернула неизвестное действие: {raw_answer[:300]}')
    return await _wait_for_script_bot_response(
        client, menu['entity'], menu, before_id, before_edits,
        require_buttons=require_buttons,
    )


async def _resolve_script_captcha_chain(
    client: TelegramClient,
    menu: Dict[str, Any],
    *,
    require_buttons: bool,
) -> Tuple[Dict[str, Any], int]:
    solved = 0
    while menu.get('has_photo'):
        if solved >= SCRIPT_CAPTCHA_MAX_ATTEMPTS:
            raise RuntimeError('AI не смогла пройти фото-капчу за допустимое число попыток')
        menu = await _solve_script_photo_captcha(
            client, menu, require_buttons=require_buttons,
        )
        solved += 1
    return menu, solved


def normalize_script_steps(script: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw_steps = script.get('steps') or []
    if isinstance(raw_steps, str):
        try:
            raw_steps = json.loads(raw_steps)
        except Exception:
            raw_steps = []
    if not isinstance(raw_steps, list):
        raw_steps = []
    steps = []
    for item in raw_steps:
        if not isinstance(item, dict):
            continue
        action = item.get('action') or ('click' if item.get('kind') in SCRIPT_ALLOWED_BUTTON_KINDS else '')
        if action not in ('click', 'join_channel'):
            continue
        steps.append({
            'row': int(item.get('row', -1)),
            'col': int(item.get('col', -1)),
            'text': str(item.get('text') or 'Без названия'),
            'kind': str(item.get('kind') or ''),
            'url': str(item.get('url') or ''),
            'action': action,
            'final': bool(item.get('final', False)),
        })
    if steps:
        return steps

    # Обратная совместимость со старыми скриптами с одной кнопкой.
    legacy = {
        'row': int(script.get('button_row', -1)),
        'col': int(script.get('button_col', -1)),
        'text': str(script.get('button_text') or 'Без названия'),
        'kind': str(script.get('button_kind') or ''),
        'url': '',
        'action': 'click',
        'final': True,
    }
    snapshot = script.get('button_snapshot') or []
    if isinstance(snapshot, str):
        try:
            snapshot = json.loads(snapshot)
        except Exception:
            snapshot = []
    for item in snapshot if isinstance(snapshot, list) else []:
        if not isinstance(item, dict):
            continue
        if int(item.get('row', -2)) == legacy['row'] and int(item.get('col', -2)) == legacy['col']:
            legacy['url'] = str(item.get('url') or '')
            if item.get('action') == 'join_channel':
                legacy['action'] = 'join_channel'
            break
    return [legacy] if legacy['text'] else []


def script_step_label(step: Dict[str, Any]) -> str:
    prefix = 'Подписка' if step.get('action') == 'join_channel' else 'Кнопка'
    return f"{prefix}: {step.get('text') or 'Без названия'}"


async def get_user_scripts(user_id: int) -> List[Dict[str, Any]]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            '''SELECT s.*, a.phone
               FROM user_scripts s
               JOIN accounts a ON a.id = s.account_id
               WHERE s.user_id = $1
               ORDER BY s.created_at DESC''',
            user_id,
        )
    result = [dict(row) for row in rows]
    for script in result:
        script['steps'] = normalize_script_steps(script)
    return result


async def get_user_script(script_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            '''SELECT s.*, a.phone, a.is_active AS account_is_active
               FROM user_scripts s
               JOIN accounts a ON a.id = s.account_id
               WHERE s.id = $1 AND s.user_id = $2''',
            script_id, user_id,
        )
    if not row:
        return None
    script = dict(row)
    script['steps'] = normalize_script_steps(script)
    return script


async def set_script_public(script_id: int, user_id: int, is_public: bool) -> bool:
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            '''UPDATE user_scripts SET is_public = $1,
               published_at = CASE WHEN $1 THEN NOW() ELSE NULL END,
               updated_at = NOW()
               WHERE id = $2 AND user_id = $3''',
            is_public, script_id, user_id,
        )
    return result.endswith('1')


async def get_public_scripts(limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            '''SELECT s.id, s.name, s.bot_url, s.bot_username, s.start_payload,
                      s.button_row, s.button_col, s.button_text, s.button_kind,
                      s.button_snapshot, s.steps, s.captcha_enabled, s.public_uses,
                      s.published_at, u.username, u.first_name
               FROM user_scripts s
               JOIN users u ON u.user_id = s.user_id
               WHERE s.is_public = TRUE
               ORDER BY s.published_at DESC NULLS LAST, s.id DESC
               LIMIT $1 OFFSET $2''',
            max(1, min(int(limit), 50)), max(0, int(offset)),
        )
    result = [dict(row) for row in rows]
    for script in result:
        script['steps'] = normalize_script_steps(script)
    return result


async def get_public_script(script_id: int) -> Optional[Dict[str, Any]]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            '''SELECT s.id, s.name, s.bot_url, s.bot_username, s.start_payload,
                      s.button_row, s.button_col, s.button_text, s.button_kind,
                      s.button_snapshot, s.steps, s.captcha_enabled, s.public_uses,
                      s.published_at, u.username, u.first_name
               FROM user_scripts s
               JOIN users u ON u.user_id = s.user_id
               WHERE s.id = $1 AND s.is_public = TRUE''',
            script_id,
        )
    if not row:
        return None
    script = dict(row)
    script['steps'] = normalize_script_steps(script)
    return script


def normalize_script_snapshot(value: Any) -> List[Dict[str, Any]]:
    snapshot = value or []
    if isinstance(snapshot, str):
        try:
            snapshot = json.loads(snapshot)
        except Exception:
            snapshot = []
    return [dict(item) for item in snapshot if isinstance(item, dict)] if isinstance(snapshot, list) else []


async def apply_public_script(
    public_script_id: int, user_id: int, account_id: int,
) -> int:
    public_script = await get_public_script(public_script_id)
    if not public_script:
        raise ValueError('Публичный скрипт не найден или снят с публикации')
    name = f"{public_script['name']} (копия)"[:64]
    script_id = await save_user_script(
        user_id=user_id,
        account_id=account_id,
        name=name,
        bot_url=public_script['bot_url'],
        bot_username=public_script['bot_username'],
        start_payload=public_script.get('start_payload') or '',
        steps=public_script['steps'],
        snapshot=normalize_script_snapshot(public_script.get('button_snapshot')),
        captcha_enabled=bool(public_script.get('captcha_enabled')),
    )
    async with db_pool.acquire() as conn:
        await conn.execute(
            'UPDATE user_scripts SET public_uses = public_uses + 1 WHERE id = $1',
            public_script_id,
        )
    return script_id


async def save_user_script(
    user_id: int,
    account_id: int,
    name: str,
    bot_url: str,
    bot_username: str,
    start_payload: str,
    steps: List[Dict[str, Any]],
    snapshot: List[Dict[str, Any]],
    captcha_enabled: bool,
) -> int:
    if not steps:
        raise ValueError('Маршрут должен содержать хотя бы один шаг')
    legacy = steps[-1]
    async with db_pool.acquire() as conn:
        owner = await conn.fetchval('SELECT user_id FROM accounts WHERE id = $1', account_id)
        if owner != user_id:
            raise ValueError('Выбранный аккаунт не найден')
        return int(await conn.fetchval(
            '''INSERT INTO user_scripts
               (user_id, account_id, name, bot_url, bot_username, start_payload,
                button_row, button_col, button_text, button_kind, button_snapshot,
                steps, captcha_enabled)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12::jsonb,$13)
               RETURNING id''',
            user_id, account_id, name, bot_url, bot_username, start_payload or None,
            int(legacy['row']), int(legacy['col']), legacy['text'], legacy['kind'],
            json.dumps(snapshot, ensure_ascii=False),
            json.dumps(steps, ensure_ascii=False), bool(captcha_enabled),
        ))


async def update_user_script_route(
    script_id: int,
    user_id: int,
    bot_url: str,
    bot_username: str,
    start_payload: str,
    steps: List[Dict[str, Any]],
    snapshot: List[Dict[str, Any]],
    captcha_enabled: bool,
) -> bool:
    if not steps:
        raise ValueError('Маршрут должен содержать хотя бы один шаг')
    legacy = steps[-1]
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            '''UPDATE user_scripts SET
               bot_url = $1, bot_username = $2, start_payload = $3,
               button_row = $4, button_col = $5, button_text = $6,
               button_kind = $7, button_snapshot = $8::jsonb,
               steps = $9::jsonb, captcha_enabled = $10, updated_at = NOW()
               WHERE id = $11 AND user_id = $12''',
            bot_url, bot_username, start_payload or None,
            int(legacy['row']), int(legacy['col']), legacy['text'], legacy['kind'],
            json.dumps(snapshot, ensure_ascii=False), json.dumps(steps, ensure_ascii=False),
            bool(captcha_enabled), script_id, user_id,
        )
    return result.endswith('1')


async def delete_user_script(script_id: int, user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            'DELETE FROM user_scripts WHERE id = $1 AND user_id = $2', script_id, user_id,
        )
    script_run_locks.pop(script_id, None)
    script_stop_flags[script_id] = True
    task = script_tasks.pop(script_id, None)
    if task and not task.done():
        task.cancel()
    script_stop_flags.pop(script_id, None)
    return result.endswith('1')


async def _execute_script_route_once(script_id: int, user_id: int) -> Dict[str, Any]:
    """Один проход сохранённого маршрута без изменения состояния runner-а."""
    script = await get_user_script(script_id, user_id)
    if not script:
        raise ValueError('Скрипт не найден')
    if not script.get('account_is_active'):
        raise RuntimeError('Выбранный аккаунт отключён')
    steps = normalize_script_steps(script)
    if not steps:
        raise RuntimeError('В маршруте нет шагов')

    client = await get_client_for_account(int(script['account_id']))
    if not client:
        raise RuntimeError('Не удалось подключиться к выбранному аккаунту')
    menu = await load_script_bot_menu(int(script['account_id']), script['bot_url'])
    captcha_solved = 0
    if script.get('captcha_enabled') and menu.get('has_photo'):
        menu, solved = await _resolve_script_captcha_chain(client, menu, require_buttons=True)
        captcha_solved += solved

    completed: List[str] = []
    for index, step in enumerate(steps):
        if script_stop_flags.get(script_id, False):
            raise asyncio.CancelledError
        is_last = index == len(steps) - 1
        if step['action'] == 'join_channel':
            await _script_join_channel_url(client, step.get('url') or '')
            completed.append(script_step_label(step))
            await add_account_log(
                int(script['account_id']), f"@{script['bot_username']}", 0,
                'script', f"Подписка по кнопке: {step['text']}",
            )
            # Важно: остаёмся в текущем окружении бота. Повторный /start
            # не посылается, а следующая кнопка ищется в том же меню.
            if not is_last:
                await asyncio.sleep(SCRIPT_CYCLE_DELAY_SECONDS)
            continue

        try:
            menu = await _click_script_step(
                client, menu, step,
                require_buttons=(not is_last or bool(script.get('captcha_enabled'))),
            )
        except TimeoutError:
            if not is_last:
                raise
            menu = None
        completed.append(script_step_label(step))
        await add_account_log(
            int(script['account_id']), f"@{script['bot_username']}",
            int(getattr(menu.get('message'), 'chat_id', 0) or 0) if menu else 0,
            'script', f"Нажата кнопка: {step['text']}",
        )
        if menu and script.get('captcha_enabled') and menu.get('has_photo'):
            menu, solved = await _resolve_script_captcha_chain(
                client, menu, require_buttons=not is_last,
            )
            captcha_solved += solved

    return {
        'script_id': script_id,
        'bot_username': script['bot_username'],
        'completed_steps': completed,
        'captcha_solved': captcha_solved,
        'message_id': int(menu['message_id']) if menu else None,
    }


async def execute_user_script(script_id: int, user_id: int) -> Dict[str, Any]:
    """Выполняет один проход маршрута (используется очередью задач)."""
    lock = script_run_locks.setdefault(script_id, asyncio.Lock())
    if lock.locked():
        raise RuntimeError('Этот скрипт уже выполняется')
    async with lock:
        return await _execute_script_route_once(script_id, user_id)


async def _mark_script_runner_state(
    script_id: int,
    run_id: int,
    status: str,
    *,
    summary: str = '',
    error: str = '',
) -> None:
    async with db_pool.acquire() as conn:
        if status == 'running':
            await conn.execute(
                '''UPDATE script_runs SET clicked_button = NULL, error = NULL
                   WHERE id = $1''', run_id,
            )
            await conn.execute(
                '''UPDATE user_scripts SET last_status = 'running', last_error = NULL,
                   last_run_at = NOW(), updated_at = NOW() WHERE id = $1''', script_id,
            )
        else:
            await conn.execute(
                '''UPDATE script_runs SET status = $1, clicked_button = NULLIF($2, ''),
                   error = NULLIF($3, ''), finished_at = NOW() WHERE id = $4''',
                status, summary[:1000], error[:1000], run_id,
            )
            await conn.execute(
                '''UPDATE user_scripts SET last_status = $1, last_error = NULLIF($2, ''),
                   updated_at = NOW() WHERE id = $3''',
                status, error[:1000], script_id,
            )


async def _script_runner(script_id: int, user_id: int, run_id: int) -> None:
    cycles = 0
    last_summary = ''
    last_error = ''
    stopped = False
    try:
        while not script_stop_flags.get(script_id, False):
            try:
                result = await execute_user_script(script_id, user_id)
                cycles += 1
                last_summary = ' → '.join(result.get('completed_steps') or [])[:1000]
                last_error = ''
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        '''UPDATE script_runs SET clicked_button = NULLIF($1, ''), error = NULL
                           WHERE id = $2''', last_summary, run_id,
                    )
                    await conn.execute(
                        '''UPDATE user_scripts SET last_status = 'running', last_error = NULL,
                           updated_at = NOW() WHERE id = $1''', script_id,
                    )
                await asyncio.sleep(SCRIPT_CYCLE_DELAY_SECONDS)
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                last_error = str(ex)[:1000]
                logger.warning('Script %s cycle failed: %s', script_id, last_error)
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        '''UPDATE script_runs SET error = $1 WHERE id = $2''',
                        last_error, run_id,
                    )
                    await conn.execute(
                        '''UPDATE user_scripts SET last_status = 'running', last_error = $1,
                           updated_at = NOW() WHERE id = $2''', last_error, script_id,
                    )
                # Скрипт не прекращается от единичной ошибки: ждём и пробуем
                # следующий полный цикл до явной остановки пользователя.
                await asyncio.sleep(SCRIPT_RETRY_DELAY_SECONDS)
    except asyncio.CancelledError:
        stopped = True
        raise
    finally:
        stopped = stopped or script_stop_flags.get(script_id, False)
        final_status = 'stopped' if stopped else 'failed'
        try:
            await _mark_script_runner_state(
                script_id, run_id, final_status,
                summary=last_summary,
                error='' if stopped else last_error,
            )
        except Exception as ex:
            logger.warning('Could not finalize script runner %s: %s', script_id, ex)
        script_tasks.pop(script_id, None)
        script_stop_flags.pop(script_id, None)


async def start_script_runner(script_id: int, user_id: int) -> Tuple[bool, str]:
    task = script_tasks.get(script_id)
    if task and not task.done():
        return False, 'Этот скрипт уже запущен'
    script = await get_user_script(script_id, user_id)
    if not script:
        return False, 'Скрипт не найден'
    if not script.get('account_is_active'):
        return False, 'Выбранный аккаунт отключён'
    if not normalize_script_steps(script):
        return False, 'В маршруте нет шагов'
    async with db_pool.acquire() as conn:
        run_id = int(await conn.fetchval(
            '''INSERT INTO script_runs (script_id, user_id, account_id, status)
               VALUES ($1, $2, $3, 'running') RETURNING id''',
            script_id, user_id, script['account_id'],
        ))
    await _mark_script_runner_state(script_id, run_id, 'running')
    script_stop_flags[script_id] = False
    task = asyncio.create_task(_script_runner(script_id, user_id, run_id))
    script_tasks[script_id] = task
    return True, ''


async def stop_script_runner(script_id: int, user_id: int) -> bool:
    script = await get_user_script(script_id, user_id)
    if not script:
        return False
    script_stop_flags[script_id] = True
    task = script_tasks.get(script_id)
    if task and not task.done():
        task.cancel()
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''UPDATE user_scripts SET last_status = 'stopped', updated_at = NOW()
               WHERE id = $1 AND user_id = $2''', script_id, user_id,
        )
    return True
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
    buttons: Optional[List[Dict[str, str]]] = None,
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

        telethon_buttons = _build_telethon_url_buttons(buttons)
        if media_paths and len(media_paths) > 0:
            if len(media_paths) == 1:
                await client.send_file(
                    chat_id_int, media_paths[0],
                    caption=text, parse_mode='html', buttons=telethon_buttons
                )
            else:
                await client.send_file(
                    chat_id_int, media_paths,
                    caption=text, parse_mode='html', buttons=telethon_buttons
                )
        else:
            await client.send_message(chat_id_int, text, parse_mode='html', buttons=telethon_buttons)

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


def _build_telethon_url_buttons(buttons: Optional[List[Dict[str, str]]]):
    """Строит только безопасные URL-кнопки для Telethon."""
    result = []
    for item in buttons or []:
        if not isinstance(item, dict):
            continue
        label = (item.get('text') or '').strip()[:64]
        url = (item.get('url') or '').strip()
        parsed = urlparse(url)
        if label and parsed.scheme in ('http', 'https') and parsed.netloc:
            result.append([Button.url(label, url)])
    return result or None


async def _send_variant_to_chat(
    client: TelegramClient, account_id: int, chat_id: str,
    variants: list
):
    """Случайно выбирает один вариант сообщения из списка и отправляет
    его в чат. Используется execute_broadcast / execute_dm_broadcast_db
    для рандомной ротации сообщений внутри одной рассылки.
    """
    variant = _pick_random_variant(variants)
    return await send_message_to_chat(
        client, account_id, chat_id,
        variant.get('text') or '',
        variant.get('media') or [],
        variant.get('buttons') or [],
    )


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

async def get_admin_extended_stats() -> Dict[str, Any]:
    """Расширенная статистика для админ-панели: разбивка по подпискам."""
    async with db_pool.acquire() as conn:
        pro_count = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions WHERE tier IN ('pro', 'max')"
        ) or 0
        total_users = await conn.fetchval('SELECT COUNT(*) FROM users') or 0
        # Истекают в ближайшие 7 дней
        expiring_soon = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions "
            "WHERE tier IN ('pro', 'max') AND expires_at IS NOT NULL "
            "AND expires_at BETWEEN NOW() AND NOW() + INTERVAL '7 days'"
        ) or 0
        # Новые пользователи за последние 24 часа
        new_today = await conn.fetchval(
            "SELECT COUNT(*) FROM users "
            "WHERE joined_at >= NOW() - INTERVAL '24 hours'"
        ) or 0
        free_count = max(total_users - pro_count, 0)
        return {
            'pro_count': int(pro_count),
            'free_count': int(free_count),
            'expiring_soon': int(expiring_soon),
            'new_today': int(new_today),
        }


# Константы журнала подтверждённых платежей используются как в платёжных
# обработчиках, так и в финансовой админ-панели.
PAYMENT_KIND_WALLET_TOPUP = 'wallet_topup'
PAYMENT_KIND_PRO_SUBSCRIPTION = 'pro_subscription'
PAYMENT_PROVIDER_CRYPTOPAY = 'cryptopay'
PAYMENT_PROVIDER_PLATEGA = 'platega'

ADMIN_FINANCE_PERIODS = {
    1: '24 часа',
    7: '7 дней',
    30: '30 дней',
    0: 'всё время',
}

FINANCE_KIND_LABELS = {
    PAYMENT_KIND_WALLET_TOPUP: 'Пополнения баланса',
    PAYMENT_KIND_PRO_SUBSCRIPTION: 'Pro-подписки',
}

FINANCE_PROVIDER_LABELS = {
    PAYMENT_PROVIDER_CRYPTOPAY: 'Crypto Pay',
    PAYMENT_PROVIDER_PLATEGA: 'СБП / Platega',
}


def normalize_admin_finance_period(value: Any) -> int:
    """Возвращает разрешённый период отчёта; 30 дней — безопасный дефолт."""
    try:
        days = int(value)
    except (TypeError, ValueError):
        return 30
    return days if days in ADMIN_FINANCE_PERIODS else 30


def get_admin_finance_since(days: int) -> Optional[datetime]:
    if days <= 0:
        return None
    # В БД используются TIMESTAMP без timezone, поэтому передаём такое же
    # локальное время МСК, как в остальных финансовых экранах бота.
    return datetime.now(MSK_TZ).replace(tzinfo=None) - timedelta(days=days)


def format_finance_rub(value: Any) -> str:
    try:
        amount = float(value or 0)
    except (TypeError, ValueError):
        amount = 0.0
    return f'{amount:,.2f}'.replace(',', ' ')


def format_finance_usdt(value: Any) -> str:
    try:
        amount = float(value or 0)
    except (TypeError, ValueError):
        amount = 0.0
    result = f'{amount:,.6f}'.replace(',', ' ')
    return result.rstrip('0').rstrip('.') or '0'


def format_finance_amounts(amount_rub: Any, amount_usdt: Any) -> str:
    values = []
    try:
        if amount_rub is not None and float(amount_rub) != 0:
            values.append(f'{format_finance_rub(amount_rub)} ₽')
    except (TypeError, ValueError):
        pass
    try:
        if amount_usdt is not None and float(amount_usdt) != 0:
            values.append(f'{format_finance_usdt(amount_usdt)} USDT')
    except (TypeError, ValueError):
        pass
    return ' · '.join(values) if values else '0 ₽'


async def get_admin_finance_stats(days: int = 30) -> Dict[str, Any]:
    """Сводка подтверждённых платежей за выбранный период."""
    days = normalize_admin_finance_period(days)
    since = get_admin_finance_since(days)
    where = "p.status = 'paid'"
    params: List[Any] = []
    if since is not None:
        params.append(since)
        where += f' AND p.paid_at >= ${len(params)}'

    async with db_pool.acquire() as conn:
        totals = await conn.fetchrow(
            f'''SELECT
                    COUNT(*) AS payment_count,
                    COALESCE(SUM(p.amount_rub), 0) AS rub_total,
                    COALESCE(SUM(p.amount_usdt), 0) AS usdt_total,
                    COUNT(*) FILTER (
                        WHERE p.kind = '{PAYMENT_KIND_WALLET_TOPUP}'
                    ) AS topup_count,
                    COALESCE(SUM(p.amount_rub) FILTER (
                        WHERE p.kind = '{PAYMENT_KIND_WALLET_TOPUP}'
                    ), 0) AS topup_rub,
                    COALESCE(SUM(p.amount_usdt) FILTER (
                        WHERE p.kind = '{PAYMENT_KIND_WALLET_TOPUP}'
                    ), 0) AS topup_usdt,
                    COUNT(*) FILTER (
                        WHERE p.kind = '{PAYMENT_KIND_PRO_SUBSCRIPTION}'
                    ) AS pro_count,
                    COALESCE(SUM(p.amount_rub) FILTER (
                        WHERE p.kind = '{PAYMENT_KIND_PRO_SUBSCRIPTION}'
                    ), 0) AS pro_rub,
                    COALESCE(SUM(p.amount_usdt) FILTER (
                        WHERE p.kind = '{PAYMENT_KIND_PRO_SUBSCRIPTION}'
                    ), 0) AS pro_usdt
                FROM payment_events p
                WHERE {where}''',
            *params,
        )
        provider_rows = await conn.fetch(
            f'''SELECT p.provider,
                       COUNT(*) AS payment_count,
                       COALESCE(SUM(p.amount_rub), 0) AS rub_total,
                       COALESCE(SUM(p.amount_usdt), 0) AS usdt_total
                FROM payment_events p
                WHERE {where}
                GROUP BY p.provider
                ORDER BY payment_count DESC, p.provider''',
            *params,
        )
        wallet_balance = await conn.fetchval(
            'SELECT COALESCE(SUM(balance), 0) FROM users'
        )
        active_topups_24h = await conn.fetchval(
            "SELECT COUNT(*) FROM balance_invoices "
            "WHERE status = 'active' AND created_at >= $1",
            get_admin_finance_since(1),
        )
        active_pro = await conn.fetchval(
            "SELECT COUNT(*) FROM subscriptions "
            "WHERE tier IN ('pro', 'max') AND (expires_at IS NULL OR expires_at > $1)",
            datetime.now(MSK_TZ).replace(tzinfo=None),
        )

    result = dict(totals or {})
    result.update({
        'period_days': days,
        'provider_rows': [dict(row) for row in provider_rows],
        'wallet_balance': wallet_balance or 0,
        'active_topups_24h': int(active_topups_24h or 0),
        'active_pro': int(active_pro or 0),
    })
    return result


async def get_admin_payment_events(
    days: int = 30, limit: int = 15,
) -> List[Dict[str, Any]]:
    """Возвращает последние подтверждённые платежи для списка и CSV."""
    days = normalize_admin_finance_period(days)
    limit = max(1, min(int(limit), 10_000))
    since = get_admin_finance_since(days)
    where = "p.status = 'paid'"
    params: List[Any] = []
    if since is not None:
        params.append(since)
        where += f' AND p.paid_at >= ${len(params)}'
    params.append(limit)
    limit_arg = len(params)

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            f'''SELECT p.id, p.user_id, p.kind, p.provider, p.external_id,
                       p.amount_rub, p.amount_usdt, p.paid_at,
                       u.username, u.first_name
                FROM payment_events p
                LEFT JOIN users u ON u.user_id = p.user_id
                WHERE {where}
                ORDER BY p.paid_at DESC, p.id DESC
                LIMIT ${limit_arg}''',
            *params,
        )
    return [dict(row) for row in rows]


def build_admin_finance_keyboard(days: int) -> InlineKeyboardMarkup:
    days = normalize_admin_finance_period(days)
    builder = InlineKeyboardBuilder()
    period_buttons = []
    for period, label in ((1, '24 ч'), (7, '7 дн.'), (30, '30 дн.')):
        period_buttons.append(InlineKeyboardButton(
            text=label,
            callback_data=f'admin_finance:{period}',
            style='success' if days == period else 'default',
            icon_custom_emoji_id=get_icon('CALENDAR'),
        ))
    builder.row(*period_buttons)
    builder.row(InlineKeyboardButton(
        text='Всё время',
        callback_data='admin_finance:0',
        style='success' if days == 0 else 'default',
        icon_custom_emoji_id=get_icon('TIME_PAST'),
    ))
    builder.row(InlineKeyboardButton(
        text='Последние платежи',
        callback_data=f'admin_finance_recent:{days}',
        style='primary',
        icon_custom_emoji_id=get_icon('MONEY_SEND'),
    ))
    builder.row(InlineKeyboardButton(
        text='Скачать CSV',
        callback_data=f'admin_finance_export:{days}',
        style='default',
        icon_custom_emoji_id=get_icon('FILE'),
    ))
    builder.row(InlineKeyboardButton(
        text='В админ-панель',
        callback_data='admin_refresh_stats',
        style='default',
        icon_custom_emoji_id=get_icon('BACK'),
    ))
    return builder.as_markup()


async def render_admin_finance(days: int = 30) -> Tuple[str, InlineKeyboardMarkup]:
    """Строит экран финансовой сводки и соответствующую клавиатуру."""
    stats = await get_admin_finance_stats(days)
    days = stats['period_days']
    provider_lines = []
    for row in stats['provider_rows']:
        provider = FINANCE_PROVIDER_LABELS.get(row['provider'], row['provider'])
        provider_lines.append(
            f"• {escape(str(provider))}: <b>{row['payment_count']}</b> — "
            f"{format_finance_amounts(row['rub_total'], row['usdt_total'])}"
        )
    if not provider_lines:
        provider_lines.append('• Подтверждённых платежей пока нет')

    text = (
        f"{emoji('CHART')} <b>Финансы и платежи</b>\n\n"
        f"Период: <b>{ADMIN_FINANCE_PERIODS[days]}</b>\n\n"
        f"{emoji('MONEY_SEND')} <b>Выручка</b>\n"
        f"Подтверждённых платежей: <b>{stats.get('payment_count', 0)}</b>\n"
        f"Рубли (СБП): <b>{format_finance_rub(stats.get('rub_total'))} ₽</b>\n"
        f"USDT (Crypto Pay): <b>{format_finance_usdt(stats.get('usdt_total'))} USDT</b>\n\n"
        f"<b>По продуктам</b>\n"
        f"• Пополнения баланса: <b>{stats.get('topup_count', 0)}</b> — "
        f"{format_finance_amounts(stats.get('topup_rub'), stats.get('topup_usdt'))}\n"
        f"• Pro/MAX-подписки: <b>{stats.get('pro_count', 0)}</b> — "
        f"{format_finance_amounts(stats.get('pro_rub'), stats.get('pro_usdt'))}\n\n"
        f"<b>По способу оплаты</b>\n"
        + '\n'.join(provider_lines)
        + f"\n\n{emoji('MONEY_SEND')} "
        f"Баланс пользователей: <b>{format_finance_rub(stats['wallet_balance'])} ₽</b>\n"
        f"{emoji('STAR')} Активных Pro/MAX: <b>{stats['active_pro']}</b>\n"
        f"{emoji('CLOCK')} Незакрытых пополнений за 24 ч: "
        f"<b>{stats['active_topups_24h']}</b>\n\n"
        f"<i>В отчёт попадают только подтверждённые платежи.</i>"
    )
    return text, build_admin_finance_keyboard(days)


def finance_csv_cell(value: Any) -> str:
    """Защищает CSV от формул из пользовательских username/имён."""
    text = '' if value is None else str(value)
    return f"'{text}" if text.startswith(('=', '+', '-', '@')) else text


def build_admin_finance_csv(rows: List[Dict[str, Any]]) -> bytes:
    output = io.StringIO(newline='')
    writer = csv.writer(output, delimiter=';')
    writer.writerow([
        'Дата оплаты', 'Тип', 'Провайдер', 'Сумма, ₽', 'Сумма, USDT',
        'Telegram ID', 'Username', 'Имя', 'Внешний ID',
    ])
    for row in rows:
        paid_at = row.get('paid_at')
        paid_at_text = (
            paid_at.strftime('%Y-%m-%d %H:%M:%S')
            if hasattr(paid_at, 'strftime') else str(paid_at or '')
        )
        writer.writerow([
            paid_at_text,
            FINANCE_KIND_LABELS.get(row.get('kind'), row.get('kind') or ''),
            FINANCE_PROVIDER_LABELS.get(row.get('provider'), row.get('provider') or ''),
            format_finance_rub(row.get('amount_rub')) if row.get('amount_rub') is not None else '',
            format_finance_usdt(row.get('amount_usdt')) if row.get('amount_usdt') is not None else '',
            finance_csv_cell(row.get('user_id')),
            finance_csv_cell(row.get('username')),
            finance_csv_cell(row.get('first_name')),
            finance_csv_cell(row.get('external_id')),
        ])
    # BOM помогает Excel на Windows корректно определить UTF-8 и кириллицу.
    return ('\ufeff' + output.getvalue()).encode('utf-8')


async def get_users_page(offset: int, limit: int) -> Dict[str, Any]:
    """Возвращает страницу пользователей с их тарифом для админ-списка."""
    async with db_pool.acquire() as conn:
        total = await conn.fetchval('SELECT COUNT(*) FROM users') or 0
        rows = await conn.fetch(
            "SELECT u.user_id, u.username, u.first_name, u.joined_at, "
            "COALESCE(s.tier, 'free') AS tier "
            "FROM users u "
            "LEFT JOIN subscriptions s ON s.user_id = u.user_id "
            "ORDER BY u.joined_at DESC "
            "LIMIT $1 OFFSET $2",
            limit, offset
        )
        return {'total': int(total), 'rows': [dict(r) for r in rows]}


async def get_user_admin_card(user_id: int) -> Optional[Dict[str, Any]]:
    """Детальная карточка пользователя для админа."""
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow(
            'SELECT user_id, username, first_name, joined_at '
            'FROM users WHERE user_id = $1',
            user_id
        )
        if not user:
            return None
        accounts_count = await conn.fetchval(
            'SELECT COUNT(*) FROM accounts WHERE user_id = $1', user_id
        ) or 0
        proxies_count = await conn.fetchval(
            'SELECT COUNT(*) FROM proxies WHERE user_id = $1', user_id
        ) or 0
    sub = await get_subscription(user_id)
    card = dict(user)
    card['accounts_count'] = int(accounts_count)
    card['proxies_count'] = int(proxies_count)
    card['tier'] = sub.get('tier', 'free')
    card['expires_at'] = sub.get('expires_at')
    return card


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
    """Достаём 3 варианта из ответа модели. Терпимы к лишнему тексту вокруг JSON."""
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
    runtime_url, runtime_key, model = await get_user_llm_runtime(user_id, model)
    client = anthropic.AsyncAnthropic(
        api_key=runtime_key,
        base_url=runtime_url,
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

    runtime_url, runtime_key, model = await get_user_llm_runtime(user_id, model)
    client = anthropic.AsyncAnthropic(
        api_key=runtime_key,
        base_url=runtime_url,
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


async def call_llm_api_with_history(
    system_prompt: str,
    messages: List[Dict[str, str]],
    user_id: int = None,
    model: str = None,
    max_tokens: int = 1024,
) -> str:
    """Запрос к LLM с произвольной историей диалога.

    Используется AI-автоответчиком в ЛС: туда уходит кастомный
    системный промт (личность ИИ) и скользящее окно истории.
    Логика выбора модели — как в call_llm_api.
    Возвращает сырой текст (включая thinking fallback) или ''.
    """
    if not model:
        if user_id is not None:
            model = await get_user_llm_model(user_id)
        else:
            model = LLM_DEFAULT_MODEL

    runtime_url, runtime_key, model = await get_user_llm_runtime(user_id, model)
    client = anthropic.AsyncAnthropic(
        api_key=runtime_key,
        base_url=runtime_url,
        timeout=LLM_TIMEOUT,
    )
    try:
        kwargs = dict(
            model=model,
            max_tokens=max_tokens,
            system=system_prompt,
            messages=messages,
        )
        if LLM_THINKING:
            kwargs['thinking'] = {'type': 'enabled', 'budget_tokens': 1024}

        response = await client.messages.create(**kwargs)
    except anthropic.APIStatusError as e:
        logger.error(
            "LLM API (history) error %s: %s", e.status_code, str(e)[:500]
        )
        raise RuntimeError(f"LLM API вернул статус {e.status_code}") from e
    except anthropic.APIError as e:
        logger.exception("LLM API (history) anthropic error")
        raise RuntimeError(f"LLM API ошибка: {e}") from e

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


# --- Чат с нейросетями ---
async def get_ai_chat_limit(user_id: int) -> int:
    if db_pool is not None:
        try:
            async with db_pool.acquire() as conn:
                override = await conn.fetchval(
                    'SELECT ai_chat_limit_override FROM users WHERE user_id = $1',
                    user_id,
                )
                if override is not None and int(override) > 0:
                    return int(override)
        except Exception:
            pass
    sub = await get_subscription(user_id)
    tier = sub.get('tier', 'free')
    if tier == 'max':
        return AI_CHAT_MAX_DAILY_LIMIT
    if tier == 'pro':
        return AI_CHAT_PRO_DAILY_LIMIT
    return AI_CHAT_FREE_DAILY_LIMIT


def _ai_chat_usage_date():
    return datetime.now(MSK_TZ).date()


async def get_ai_chat_usage(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            'SELECT request_count FROM ai_chat_usage '
            'WHERE user_id = $1 AND usage_date = $2',
            user_id, _ai_chat_usage_date(),
        )
    return int(count or 0)


async def reserve_ai_chat_request(user_id: int) -> Tuple[bool, int, int]:
    """Атомарно резервирует запрос в дневном лимите чата."""
    limit = await get_ai_chat_limit(user_id)
    usage_date = _ai_chat_usage_date()
    async with db_pool.acquire() as conn:
        used = await conn.fetchval(
            '''INSERT INTO ai_chat_usage (user_id, usage_date, request_count, updated_at)
               VALUES ($1, $2, 1, NOW())
               ON CONFLICT (user_id, usage_date) DO UPDATE SET
                 request_count = ai_chat_usage.request_count + 1,
                 updated_at = NOW()
               WHERE ai_chat_usage.request_count < $3
               RETURNING request_count''',
            user_id, usage_date, limit,
        )
        if used is not None:
            return True, int(used), limit
        current = await conn.fetchval(
            'SELECT request_count FROM ai_chat_usage '
            'WHERE user_id = $1 AND usage_date = $2',
            user_id, usage_date,
        )
    return False, int(current or limit), limit


async def release_ai_chat_request(user_id: int) -> None:
    """Возвращает лимит, если API не дал ответа и пользователь его не получил."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''UPDATE ai_chat_usage
               SET request_count = GREATEST(request_count - 1, 0), updated_at = NOW()
               WHERE user_id = $1 AND usage_date = $2''',
            user_id, _ai_chat_usage_date(),
        )


async def get_ai_chat_history(user_id: int) -> List[Dict[str, str]]:
    async with db_pool.acquire() as conn:
        history = await conn.fetchval(
            'SELECT history FROM ai_chat_sessions WHERE user_id = $1', user_id,
        )
    if isinstance(history, str):
        try:
            history = json.loads(history)
        except Exception:
            history = []
    if not isinstance(history, list):
        return []
    result: List[Dict[str, str]] = []
    for item in history[-AI_CHAT_HISTORY_MESSAGES_LIMIT:]:
        if not isinstance(item, dict):
            continue
        role = item.get('role')
        content = item.get('content')
        if role in ('user', 'assistant') and isinstance(content, str) and content.strip():
            result.append({'role': role, 'content': content[:AI_CHAT_HISTORY_CONTENT_LIMIT]})
    return result


async def save_ai_chat_history(user_id: int, history: List[Dict[str, str]]) -> None:
    clean = [
        {'role': item['role'], 'content': item['content'][:AI_CHAT_HISTORY_CONTENT_LIMIT]}
        for item in history[-AI_CHAT_HISTORY_MESSAGES_LIMIT:]
        if item.get('role') in ('user', 'assistant') and item.get('content')
    ]
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO ai_chat_sessions (user_id, history, updated_at)
               VALUES ($1, $2::jsonb, NOW())
               ON CONFLICT (user_id) DO UPDATE SET
                 history = EXCLUDED.history, updated_at = NOW()''',
            user_id, json.dumps(clean, ensure_ascii=False),
        )


async def clear_ai_chat_history(user_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO ai_chat_sessions (user_id, history, updated_at)
               VALUES ($1, '[]'::jsonb, NOW())
               ON CONFLICT (user_id) DO UPDATE SET
                 history = '[]'::jsonb, updated_at = NOW()''',
            user_id,
        )


def get_ai_chat_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text='Очистить диалог',
        callback_data='ai_chat_clear',
        style='default',
        icon_custom_emoji_id=get_icon('SWEEP'),
    ))
    builder.row(InlineKeyboardButton(
        text='В главное меню',
        callback_data='ai_chat_exit',
        style='default',
        icon_custom_emoji_id=get_icon('BACK'),
    ))
    return builder.as_markup()


async def render_ai_chat_screen(user_id: int) -> str:
    used = await get_ai_chat_usage(user_id)
    limit = await get_ai_chat_limit(user_id)
    model = await get_user_llm_model(user_id)
    model_label = LLM_MODELS.get(model, model)
    sub = await get_subscription(user_id)
    tier = {'max': 'MAX', 'pro': 'Pro'}.get(sub.get('tier'), 'Free')
    return (
        f"{emoji('AI')} <b>Чат с нейросетями</b>\n\n"
        f"Модель: <b>{escape(str(model_label))}</b>\n"
        f"Тариф: <b>{tier}</b>\n"
        f"Запросов сегодня: <b>{used}/{limit}</b>\n\n"
        "Отправьте сообщение — я сохраню контекст последних реплик. "
        "Кнопка «Очистить диалог» удалит историю, но не дневной счётчик."
    )


def _split_ai_chat_answer(text: str, limit: int = 3000) -> List[str]:
    text = (text or '').strip()
    if not text:
        return []
    parts: List[str] = []
    while text:
        if len(text) <= limit:
            parts.append(text)
            break
        cut = text.rfind('\n', 0, limit)
        if cut < limit // 2:
            cut = limit
        parts.append(text[:cut])
        text = text[cut:].lstrip('\n')
    return parts


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
#  Per-account AI-автоответчик — хелперы БД
# ============================================================
# Всё per-account_id:
#   account_ai_responder: mode, system_prompt, model, history (per chat_id)
# Режимы всего два: 'off' (без ИИ) и 'ai' (с ИИ).


async def acct_ar_get(account_id: int) -> Dict[str, Any]:
    """Получить настройки per-account AI-автоответчика. Создаёт запись, если нет."""
    defaults = {
        'account_id': account_id,
        'mode': ACCT_AR_MODE_OFF,
        'system_prompt': '',
        'model': '',
        'history': {},
    }
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT * FROM account_ai_responder WHERE account_id = $1',
            account_id,
        )
        if not row:
            await conn.execute(
                'INSERT INTO account_ai_responder (account_id) '
                'VALUES ($1) ON CONFLICT (account_id) DO NOTHING',
                account_id,
            )
            return defaults

        history = row['history'] or {}
        if isinstance(history, str):
            try:
                history = json.loads(history)
            except Exception:
                history = {}
        if not isinstance(history, dict):
            history = {}

        return {
            'account_id':   row['account_id'],
            'mode':         row['mode'] or ACCT_AR_MODE_OFF,
            'system_prompt': row['system_prompt'] or '',
            'model':        row['model'] or '',
            'history':      history,
        }


async def acct_ar_set_mode(account_id: int, mode: str) -> None:
    if mode not in ACCT_AR_MODE_LABELS:
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO account_ai_responder (account_id, mode) '
            'VALUES ($1, $2) '
            'ON CONFLICT (account_id) DO UPDATE '
            'SET mode = EXCLUDED.mode, updated_at = NOW()',
            account_id, mode,
        )


async def acct_ar_set_system_prompt(account_id: int, prompt: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO account_ai_responder (account_id, system_prompt) '
            'VALUES ($1, $2) '
            'ON CONFLICT (account_id) DO UPDATE '
            'SET system_prompt = EXCLUDED.system_prompt, updated_at = NOW()',
            account_id, prompt,
        )


async def acct_ar_set_model(account_id: int, model: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO account_ai_responder (account_id, model) '
            'VALUES ($1, $2) '
            'ON CONFLICT (account_id) DO UPDATE '
            'SET model = EXCLUDED.model, updated_at = NOW()',
            account_id, model,
        )


async def acct_ar_reset_system_prompt(account_id: int) -> None:
    await acct_ar_set_system_prompt(account_id, '')


async def acct_ar_get_chat_history(
    account_id: int, chat_id: int,
) -> List[Dict[str, str]]:
    s = await acct_ar_get(account_id)
    hist = (s.get('history') or {}).get(str(chat_id), [])
    return list(hist)


async def acct_ar_push_chat_history(
    account_id: int, chat_id: int, role: str, content: str,
) -> List[Dict[str, str]]:
    """Дописать пару в историю по конкретному chat_id, обрезать до N*2."""
    s = await acct_ar_get(account_id)
    history_map = dict(s.get('history') or {})
    chat_hist = list(history_map.get(str(chat_id), []))
    chat_hist.append({'role': role, 'content': content})
    chat_hist = chat_hist[-(ACCT_AR_HISTORY_PAIRS * 2):]
    history_map[str(chat_id)] = chat_hist
    async with db_pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO account_ai_responder (account_id, history) '
            'VALUES ($1, $2::jsonb) '
            'ON CONFLICT (account_id) DO UPDATE '
            'SET history = EXCLUDED.history, updated_at = NOW()',
            account_id, json.dumps(history_map, ensure_ascii=False),
        )
    return chat_hist


async def acct_ar_reset_history(account_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO account_ai_responder (account_id, history) '
            'VALUES ($1, \'{}\'::jsonb) '
            'ON CONFLICT (account_id) DO UPDATE '
            'SET history = \'{}\'::jsonb, updated_at = NOW()',
            account_id,
        )


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
    user_id: Optional[int],
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
                    # Ответ @SpamBot нужен для фоновой проверки статуса,
                    # а не для автоответчика. Иначе триггер «-» мог бы
                    # отвечать самому сервисному боту Telegram.
                    if (getattr(sender, 'username', None) or '').casefold() == 'spambot':
                        return
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

def _normalize_message_variants(
    message_texts_raw, fallback_text: str, fallback_media
) -> list:
    """Превращает JSONB из базы (или список из FSM) в единый формат
    вариантов сообщений: [{"text": str, "media": [str, ...]}, ...].
    Если список пуст — собирает один вариант из fallback-полей, чтобы
    обратная совместимость со старыми рассылками сохранилась.
    """
    variants = []
    if message_texts_raw:
        if isinstance(message_texts_raw, str):
            try:
                message_texts_raw = json.loads(message_texts_raw)
            except (ValueError, TypeError):
                message_texts_raw = []
        for item in message_texts_raw or []:
            if not isinstance(item, dict):
                continue
            text = item.get('text') or ''
            media = item.get('media') or []
            if isinstance(media, str):
                media = [media]
            variants.append({
                'text': text, 'media': list(media or []),
                'buttons': list(item.get('buttons') or []),
            })
    if not variants:
        variants.append({
            'text': fallback_text or '',
            'media': list(fallback_media or []),
            'buttons': [],
        })
    return variants


def _pick_random_variant(variants: list) -> dict:
    """Случайно выбирает один из вариантов сообщений. Если список
    пуст (не должно случиться после _normalize_message_variants) —
    возвращает пустой текст без медиа.
    """
    if not variants:
        return {'text': '', 'media': []}
    return random.choice(variants)


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
    # Список сообщений для рандомной рассылки (новый формат). Каждый
    # элемент — {"text": str, "media": [str, ...]}. Если заполнен — он
    # используется вместо одиночных message_text / message_media.
    message_texts_raw = broadcast.get('message_texts') or []
    message_variants = _normalize_message_variants(
        message_texts_raw, message_text, message_media
    )
    
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
                        _send_variant_to_chat(
                            client, account_id, chat_id, message_variants
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
                    await _send_variant_to_chat(
                        client, account_id, random_chat, message_variants
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
    media_paths: List[str] = None,
    message_texts=None,
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

    # Список вариантов сообщений. Если передан message_texts (новый
    # формат с рандомной ротацией) — используем его, иначе собираем
    # один вариант из message_text / media_paths.
    message_variants = _normalize_message_variants(
        message_texts, message_text, media_paths
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

            # Каждому юзеру выбираем один вариант сообщения случайно.
            # Если вариантов несколько, у разных получателей может уйти
            # разный текст — это и есть «рандомная рассылка».
            variant = _pick_random_variant(message_variants)
            variant_text = process_variables(
                variant.get('text') or '', user_data
            )
            variant_media = variant.get('media') or []
            variant_buttons = _build_telethon_url_buttons(variant.get('buttons') or [])

            if variant_media and len(variant_media) > 0:
                if (
                    len(variant_media) == 1
                    and os.path.exists(variant_media[0])
                ):
                    await client.send_file(
                        entity.id, variant_media[0],
                        caption=variant_text, parse_mode='html', buttons=variant_buttons
                    )
                else:
                    await client.send_file(
                        entity.id, variant_media,
                        caption=variant_text, parse_mode='html', buttons=variant_buttons
                    )
            else:
                await client.send_message(
                    entity.id, variant_text, parse_mode='html', buttons=variant_buttons
                )
            
            await add_account_log(
                account_id, username, entity.id,
                'sent', variant_text[:100]
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


# --- Создание каналов и групп ---
CHAT_CREATION_DELAY = 20
CHAT_CREATION_FLOOD_ACTION = 'chat_creation'

CHAT_CREATION_LABELS = {
    'channel': {
        'title': 'Создание каналов',
        'items': 'каналов',
        'current': 'Текущий канал',
        'first': 'Первый канал',
        'plural': 'Каналы',
    },
    'group': {
        'title': 'Создание групп',
        'items': 'групп',
        'current': 'Текущая группа',
        'first': 'Первая группа',
        'plural': 'Группы',
    },
}


def get_chat_creation_labels(creation_kind: str) -> Dict[str, str]:
    """Названия для общего сценария создания каналов и групп."""
    return CHAT_CREATION_LABELS.get(
        creation_kind, CHAT_CREATION_LABELS['channel']
    )


def build_chat_title(base_title: str, index: int, total: int) -> str:
    """Вернуть название, сохранив лимит Telegram в 128 символов."""
    if total == 1:
        return base_title[:128]
    suffix = f" {index}"
    return f"{base_title[:128 - len(suffix)]}{suffix}"


async def set_account_action_cooldown(
    account_id: int, action: str, seconds: int, source: str = 'FloodWait',
) -> datetime:
    """Сохраняет дедлайн Telegram cooldown и никогда не сокращает его."""
    wait_seconds = max(1, int(seconds))
    requested_until = _now_msk_naive() + timedelta(seconds=wait_seconds)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            '''INSERT INTO account_action_cooldowns
               (account_id, action, cooldown_until, source, updated_at)
               VALUES ($1, $2, $3, $4, NOW())
               ON CONFLICT (account_id, action) DO UPDATE SET
                 cooldown_until = GREATEST(
                     account_action_cooldowns.cooldown_until,
                     EXCLUDED.cooldown_until
                 ),
                 source = EXCLUDED.source,
                 updated_at = NOW()
               RETURNING cooldown_until''',
            account_id, action, requested_until, source,
        )
    return row['cooldown_until'] if row else requested_until


async def get_account_action_cooldown(
    account_id: int, action: str,
) -> Optional[datetime]:
    """Возвращает активный дедлайн и очищает уже прошедшие записи."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            '''SELECT cooldown_until FROM account_action_cooldowns
               WHERE account_id = $1 AND action = $2''',
            account_id, action,
        )
        if not row:
            return None
        deadline = row['cooldown_until']
        compare_deadline = deadline
        if getattr(compare_deadline, 'tzinfo', None) is not None:
            compare_deadline = compare_deadline.astimezone(MSK_TZ).replace(tzinfo=None)
        if compare_deadline <= _now_msk_naive():
            await conn.execute(
                '''DELETE FROM account_action_cooldowns
                   WHERE account_id = $1 AND action = $2''',
                account_id, action,
            )
            return None
    return deadline


async def clear_account_action_cooldown(account_id: int, action: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''DELETE FROM account_action_cooldowns
               WHERE account_id = $1 AND action = $2''',
            account_id, action,
        )


def cooldown_remaining_seconds(deadline: datetime) -> int:
    comparable = deadline
    if getattr(comparable, 'tzinfo', None) is not None:
        comparable = comparable.astimezone(MSK_TZ).replace(tzinfo=None)
    return max(0, int((comparable - _now_msk_naive()).total_seconds() + 0.999))


async def wait_for_chat_creation_cooldown(
    account_id: int,
    user_id: int,
    labels: Dict[str, str],
    chat_title: str,
    created_count: int,
    total_count: int,
    progress_message: Message,
) -> bool:
    """Ждёт строго до окончания FloodWait, обновляя прогресс и реагируя на Stop."""
    while not chat_creation_stop_flags.get(user_id, False):
        deadline = await get_account_action_cooldown(
            account_id, CHAT_CREATION_FLOOD_ACTION,
        )
        if not deadline:
            return True
        remaining = cooldown_remaining_seconds(deadline)
        if remaining <= 0:
            await clear_account_action_cooldown(
                account_id, CHAT_CREATION_FLOOD_ACTION,
            )
            return True
        try:
            await progress_message.edit_text(
                f"{emoji('CLOCK')} <b>{labels['title']} приостановлено</b>\n\n"
                f"Telegram запросил паузу FloodWait.\n"
                f"До продолжения: <b>{remaining} сек.</b>\n"
                f"Продолжим после: <b>{_format_msk_datetime(deadline, '—')}</b>\n\n"
                f"Создано: <b>{created_count}/{total_count}</b>\n"
                f"{labels['current']}: <code>{escape(chat_title)}</code>",
                reply_markup=get_chat_creation_control_keyboard(),
            )
        except Exception:
            pass
        # Спим кусками максимум по минуте: так Stop работает без долгого
        # ожидания, а продлённый другим запросом дедлайн подхватывается.
        await asyncio.sleep(min(remaining, 60))
    return False


async def execute_chat_creation(
    account_id: int,
    user_id: int,
    count: int,
    base_title: str,
    creation_kind: str,
    progress_message: Message,
) -> Dict[str, Any]:
    """Создаёт каналы/группы, соблюдая и пережидая FloodWait Telegram."""
    labels = get_chat_creation_labels(creation_kind)
    is_group = creation_kind == 'group'

    account = await get_account(account_id)
    if not account or account['user_id'] != user_id:
        raise RuntimeError('Аккаунт не найден или не принадлежит пользователю')

    client = await get_client_for_account(account_id)
    if not client or not await client.is_user_authorized():
        raise RuntimeError('Не удалось подключить выбранный аккаунт')

    created: List[str] = []
    failed: List[Dict[str, str]] = []
    chat_creation_stop_flags[user_id] = False

    for index in range(1, count + 1):
        if chat_creation_stop_flags.get(user_id, False):
            break

        chat_title = build_chat_title(base_title, index, count)
        while not chat_creation_stop_flags.get(user_id, False):
            # Если Telegram уже выдал FloodWait этому аккаунту, в том числе
            # в предыдущей задаче, не делаем новый запрос до его окончания.
            may_continue = await wait_for_chat_creation_cooldown(
                account_id=account_id,
                user_id=user_id,
                labels=labels,
                chat_title=chat_title,
                created_count=len(created),
                total_count=count,
                progress_message=progress_message,
            )
            if not may_continue:
                break

            request = CreateChannelRequest(
                title=chat_title,
                about='',
                broadcast=not is_group,
                megagroup=is_group,
            )
            try:
                await client(request)
                created.append(chat_title)
                await clear_account_action_cooldown(
                    account_id, CHAT_CREATION_FLOOD_ACTION,
                )
                logger.info(
                    'Created %s %r for account_id=%s (%s/%s)',
                    creation_kind, chat_title, account_id, index, count,
                )
                break
            except FloodWaitError as ex:
                wait_seconds = max(1, int(ex.seconds))
                await record_flood_wait(account_id, 0, wait_seconds)
                deadline = await set_account_action_cooldown(
                    account_id,
                    CHAT_CREATION_FLOOD_ACTION,
                    wait_seconds,
                )
                logger.warning(
                    '%s creation FloodWait for account_id=%s: %ss, until %s',
                    creation_kind, account_id, wait_seconds, deadline,
                )
                # Не считаем текущий канал ошибкой: тот же запрос будет
                # повторён только после полного cooldown.
                continue
            except RPCError as ex:
                failed.append({'title': chat_title, 'error': str(ex)})
                logger.warning(
                    '%s creation failed for %r: %s',
                    creation_kind, chat_title, ex,
                )
                break
            except Exception as ex:
                failed.append({'title': chat_title, 'error': str(ex)})
                logger.exception(
                    'Unexpected %s creation error for %r',
                    creation_kind, chat_title,
                )
                break

        if chat_creation_stop_flags.get(user_id, False):
            break

        processed = len(created) + len(failed)
        try:
            next_line = (
                f"\nСледующий запуск через: <b>{CHAT_CREATION_DELAY} сек.</b>"
                if index < count else ''
            )
            await progress_message.edit_text(
                f"{emoji('LOADING')} <b>{labels['title']}...</b>\n\n"
                f"Создано: <b>{len(created)}/{count}</b>\n"
                f"Ошибок: <b>{len(failed)}</b>\n"
                f"Обработано: <b>{processed}/{count}</b>"
                f"{next_line}",
                reply_markup=get_chat_creation_control_keyboard(),
            )
        except Exception:
            pass

        if index < count and not chat_creation_stop_flags.get(user_id, False):
            await asyncio.sleep(CHAT_CREATION_DELAY)

    return {
        'total': count,
        'created': created,
        'failed': failed,
        'stopped': chat_creation_stop_flags.get(user_id, False),
        'creation_kind': creation_kind,
    }

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


# ============================================================
# Нейрокомментинг: мониторинг новых постов в каналах и публикация
# комментариев от выбранного Telegram-аккаунта.
# ============================================================
NEUROCOMMENT_MODE_AI = 'ai'
NEUROCOMMENT_MODE_TEMPLATES = 'templates'
NEUROCOMMENT_MAX_TEMPLATE_VARIANTS = 100
NEUROCOMMENT_IMAGE_MAX_BYTES = 4 * 1024 * 1024
NEUROCOMMENT_AI_SYSTEM_PROMPT = (
    'Ты пишешь один естественный, уместный комментарий к посту в Telegram. '
    'Комментарий должен быть на русском, конкретно относиться к посту, '
    'быть дружелюбным и не выглядеть как реклама или спам. '
    'Не используй ссылки, хэштеги, призывы купить что-либо, упоминания бота '
    'или фразы о том, что ты ИИ. Верни только текст комментария, максимум 500 символов.'
)


async def get_neurocomment_configs(user_id: int) -> List[Dict[str, Any]]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            '''SELECT n.*, a.phone
               FROM neurocomment_configs n
               LEFT JOIN accounts a ON a.id = n.account_id
               WHERE n.user_id = $1
               ORDER BY n.is_active DESC, n.updated_at DESC, n.id DESC''',
            user_id,
        )
    return [_normalize_neurocomment_config(dict(row)) for row in rows]


async def get_neurocomment_config(
    config_id: int, user_id: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    query = (
        '''SELECT n.*, a.phone
           FROM neurocomment_configs n
           LEFT JOIN accounts a ON a.id = n.account_id
           WHERE n.id = $1'''
    )
    args: List[Any] = [config_id]
    if user_id is not None:
        query += ' AND n.user_id = $2'
        args.append(user_id)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(query, *args)
    return _normalize_neurocomment_config(dict(row)) if row else None


def _normalize_neurocomment_config(config: Dict[str, Any]) -> Dict[str, Any]:
    variants = config.get('message_variants') or []
    if isinstance(variants, str):
        try:
            variants = json.loads(variants)
        except Exception:
            variants = []
    if not isinstance(variants, list):
        variants = []
    config['message_variants'] = [
        str(item).strip() for item in variants
        if isinstance(item, str) and item.strip()
    ][:NEUROCOMMENT_MAX_TEMPLATE_VARIANTS]
    config['channel_ids'] = [str(x) for x in (config.get('channel_ids') or [])]
    return config


async def create_neurocomment_config(
    user_id: int,
    account_id: int,
    channel_ids: List[str],
    mode: str,
    model: Optional[str],
    message_variants: List[str],
    delay_seconds: int,
) -> int:
    if mode not in (NEUROCOMMENT_MODE_AI, NEUROCOMMENT_MODE_TEMPLATES):
        raise ValueError('Некорректный режим')
    if not channel_ids:
        raise ValueError('Нужно выбрать хотя бы один канал')
    if mode == NEUROCOMMENT_MODE_TEMPLATES and not message_variants:
        raise ValueError('Нужен хотя бы один шаблон комментария')
    async with db_pool.acquire() as conn:
        config_id = await conn.fetchval(
            '''INSERT INTO neurocomment_configs
               (user_id, account_id, channel_ids, mode, model, message_variants, delay_seconds)
               VALUES ($1, $2, $3::text[], $4, $5, $6::jsonb, $7)
               RETURNING id''',
            user_id,
            account_id,
            [str(channel_id) for channel_id in channel_ids],
            mode,
            model.strip() if model else None,
            json.dumps(message_variants[:NEUROCOMMENT_MAX_TEMPLATE_VARIANTS], ensure_ascii=False),
            int(delay_seconds),
        )
    return int(config_id)


async def find_active_neurocomment_for_account(account_id: int) -> Optional[int]:
    async with db_pool.acquire() as conn:
        config_id = await conn.fetchval(
            '''SELECT id FROM neurocomment_configs
               WHERE account_id = $1 AND is_active = TRUE
               ORDER BY started_at DESC NULLS LAST, id DESC LIMIT 1''',
            account_id,
        )
    return int(config_id) if config_id is not None else None


async def set_neurocomment_active(config_id: int, active: bool, error: str = '') -> None:
    async with db_pool.acquire() as conn:
        if active:
            await conn.execute(
                '''UPDATE neurocomment_configs
                   SET is_active = TRUE, started_at = NOW(), stopped_at = NULL,
                       last_error = NULL, updated_at = NOW()
                   WHERE id = $1''',
                config_id,
            )
        else:
            await conn.execute(
                '''UPDATE neurocomment_configs
                   SET is_active = FALSE, stopped_at = NOW(),
                       last_error = NULLIF($2, ''), updated_at = NOW()
                   WHERE id = $1''',
                config_id, error[:1000],
            )


async def record_neurocomment_result(
    config_id: int, success: bool, error: str = '',
) -> None:
    async with db_pool.acquire() as conn:
        if success:
            await conn.execute(
                '''UPDATE neurocomment_configs
                   SET comments_sent = comments_sent + 1, last_error = NULL,
                       updated_at = NOW() WHERE id = $1''',
                config_id,
            )
        else:
            await conn.execute(
                '''UPDATE neurocomment_configs
                   SET errors_count = errors_count + 1, last_error = $2,
                       updated_at = NOW() WHERE id = $1''',
                config_id, error[:1000],
            )


def _neurocomment_channel_peers(channel_ids: List[str]) -> List[Any]:
    peers: List[Any] = []
    for channel_id in channel_ids:
        value = str(channel_id)
        peers.append(int(value) if value.lstrip('-').isdigit() else value)
    return peers


async def _neurocomment_post_payload(
    client: TelegramClient, message,
) -> Tuple[str, Optional[bytes]]:
    """Возвращает текст поста или изображение, отдавая тексту приоритет."""
    text = (getattr(message, 'raw_text', None) or getattr(message, 'message', None) or '').strip()
    if text:
        return text[:6000], None
    if getattr(message, 'photo', None):
        try:
            image = await client.download_media(message, bytes)
            if isinstance(image, bytes) and 0 < len(image) <= NEUROCOMMENT_IMAGE_MAX_BYTES:
                return '', image
        except Exception as ex:
            logger.info('Neurocomment could not download post image: %s', ex)
    return '', None


async def generate_neurocomment_ai_reply(
    user_id: int,
    post_text: str,
    image_bytes: Optional[bytes] = None,
    model: Optional[str] = None,
) -> str:
    selected_model = model or await get_user_llm_model(user_id)
    runtime_url, runtime_key, selected_model = await get_user_llm_runtime(
        user_id, selected_model,
    )
    instruction = (
        'Сформируй комментарий к следующему посту. '
        'Используй только сведения из поста и не повторяй его дословно.\n\n'
    )
    if post_text:
        content: Any = instruction + 'Текст поста:\n' + post_text
    elif image_bytes:
        content = [
            {'type': 'text', 'text': instruction + 'В посте нет текста, проанализируй изображение.'},
            {
                'type': 'image',
                'source': {
                    'type': 'base64',
                    'media_type': 'image/jpeg',
                    'data': base64.b64encode(image_bytes).decode('ascii'),
                },
            },
        ]
    else:
        raise ValueError('Пост не содержит текста или доступного изображения')

    client = anthropic.AsyncAnthropic(
        api_key=runtime_key,
        base_url=runtime_url,
        timeout=LLM_TIMEOUT,
    )
    try:
        response = await client.messages.create(
            model=selected_model,
            max_tokens=220,
            system=NEUROCOMMENT_AI_SYSTEM_PROMPT,
            messages=[{'role': 'user', 'content': content}],
        )
    except anthropic.APIStatusError as ex:
        raise RuntimeError(f'LLM HTTP {ex.status_code}: {str(ex)[:500]}') from ex
    except anthropic.APIError as ex:
        raise RuntimeError(f'LLM {ex.__class__.__name__}: {str(ex)[:500]}') from ex

    reply = _extract_llm_response_text(response).strip()
    if not reply:
        raise RuntimeError('LLM вернула пустой комментарий')
    return reply[:500]


async def _neurocomment_build_reply(
    client: TelegramClient, config: Dict[str, Any], event,
) -> str:
    if config['mode'] == NEUROCOMMENT_MODE_TEMPLATES:
        variants = config.get('message_variants') or []
        if not variants:
            raise RuntimeError('В конфигурации нет заготовленных комментариев')
        return random.choice(variants)
    post_text, image_bytes = await _neurocomment_post_payload(client, event.message)
    return await generate_neurocomment_ai_reply(
        int(config['user_id']),
        post_text,
        image_bytes,
        model=(config.get('model') or None),
    )


async def _neurocomment_process_post(
    client: TelegramClient, config: Dict[str, Any], event,
) -> None:
    config_id = int(config['id'])
    if neurocomment_stop_flags.get(config_id, False):
        return
    if getattr(event, 'out', False) or getattr(event.message, 'action', None):
        return
    # У событий канала post=True; оставляем None для совместимости с
    # различными версиями Telethon.
    if getattr(event.message, 'post', None) is False:
        return

    delay = max(0, int(config.get('delay_seconds') or 0))
    if delay:
        await asyncio.sleep(delay)
    if neurocomment_stop_flags.get(config_id, False):
        return

    try:
        reply = await _neurocomment_build_reply(client, config, event)
        if neurocomment_stop_flags.get(config_id, False):
            return
        await client.send_message(
            event.chat_id,
            reply,
            comment_to=event.message.id,
            parse_mode=None,
        )
        chat_name = getattr(getattr(event, 'chat', None), 'title', None) or str(event.chat_id)
        await add_account_log(
            int(config['account_id']), chat_name, int(event.chat_id),
            'neurocomment', reply[:100],
        )
        await record_neurocomment_result(config_id, True)
    except FloodWaitError as ex:
        await record_flood_wait(int(config['account_id']), int(event.chat_id), ex.seconds)
        await record_neurocomment_result(
            config_id, False, f'FloodWait: {int(ex.seconds)} сек',
        )
        # Соблюдаем выданный Telegram cooldown, но не пытаемся повторить
        # комментарий автоматически, чтобы не отправить дубль.
        await asyncio.sleep(int(ex.seconds) + 1)
    except Exception as ex:
        logger.info('Neurocomment failed for config %s: %s', config_id, ex)
        await record_neurocomment_result(config_id, False, str(ex))


async def neurocomment_worker(config_id: int) -> None:
    config = await get_neurocomment_config(config_id)
    if not config or not config.get('is_active'):
        return
    client = await get_client_for_account(int(config['account_id']))
    if not client:
        await set_neurocomment_active(config_id, False, 'Не удалось подключить аккаунт')
        return

    peers = _neurocomment_channel_peers(config['channel_ids'])
    if not peers:
        await set_neurocomment_active(config_id, False, 'Не выбраны каналы')
        return
    neurocomment_stop_flags[config_id] = False
    event_lock = neurocomment_event_locks.setdefault(config_id, asyncio.Lock())

    @client.on(events.NewMessage(chats=peers, incoming=True))
    async def handler(event):
        if neurocomment_stop_flags.get(config_id, False):
            return
        async with event_lock:
            await _neurocomment_process_post(client, config, event)

    worker_error = ''
    try:
        while not neurocomment_stop_flags.get(config_id, False) and client.is_connected():
            await asyncio.sleep(1)
        if not neurocomment_stop_flags.get(config_id, False) and not client.is_connected():
            worker_error = 'Соединение аккаунта с Telegram прервано'
    except asyncio.CancelledError:
        raise
    except Exception as ex:
        worker_error = str(ex)
        logger.exception('Neurocomment worker %s failed', config_id)
    finally:
        try:
            client.remove_event_handler(handler)
        except Exception:
            pass
        neurocomment_tasks.pop(config_id, None)
        neurocomment_stop_flags.pop(config_id, None)
        neurocomment_event_locks.pop(config_id, None)
        if worker_error:
            await set_neurocomment_active(config_id, False, worker_error)


async def start_neurocomment_worker(config_id: int) -> bool:
    task = neurocomment_tasks.get(config_id)
    if task and not task.done():
        return True
    config = await get_neurocomment_config(config_id)
    if not config or not config.get('is_active'):
        return False
    neurocomment_stop_flags[config_id] = False
    task = asyncio.create_task(neurocomment_worker(config_id))
    neurocomment_tasks[config_id] = task
    return True


async def stop_neurocomment_worker(config_id: int, error: str = '') -> None:
    neurocomment_stop_flags[config_id] = True
    task = neurocomment_tasks.get(config_id)
    if task and not task.done():
        task.cancel()
    await set_neurocomment_active(config_id, False, error)


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
#   3) flood-wait истории аккаунта (если недавно ловил флуд — сильно медленнее)
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
# ПОДПИСКИ (Free / Pro / MAX) + CRYPTO PAY
# ============================================================
CRYPTO_PAY_API = "https://pay.crypt.bot/api"
# Токен Crypto Pay (@CryptoBot) — основной способ оплаты Pro.
CRYPTO_PAY_TOKEN = os.getenv("CRYPTO_PAY_TOKEN") or "490665:AAEwanehVerJ8FvFsTf81CWtyY9wSFW86aF"
# Pro и MAX: несколько сроков на выбор (см. PRO_PLANS).
# Legacy-константы оставлены для совместимости со старым кодом:
# это параметры базового 30-дневного тарифа.
PRO_PRICE_USD = "0.60"
PRO_PRICE_LABEL = "40₽ / 30 дней"
PRO_DURATION_DAYS = 30
MAX_DURATION_DAYS = int(os.getenv('MAX_DURATION_DAYS') or '30')
MAX_PRICE_RUB = int(os.getenv('MAX_PRICE_RUB') or '100')
MAX_PRICE_USD = os.getenv('MAX_PRICE_USD') or '1.50'

# ------------------------------------------------------------
# Каталог тарифов Pro и MAX.
# code — он же payload платежа ("<code>:<user_id>"), поэтому менять
# коды у уже выпущенных счетов нельзя.
# rub — цена для СБП (Platega), usd — сумма счёта в USDT (Crypto Pay),
# курс ≈ 66.7₽ за 1 USDT (как у базового тарифа 40₽ / 0.60$).
# ------------------------------------------------------------
PRO_PLANS: Dict[str, Dict[str, Any]] = {
    "pro_1d": {
        "code": "pro_1d", "tier": "pro", "days": 1, "rub": 5, "usd": "0.08",
        "title": "1 день", "badge": "тест",
    },
    "pro_7d": {
        "code": "pro_7d", "tier": "pro", "days": 7, "rub": 15, "usd": "0.23",
        "title": "7 дней", "badge": "",
    },
    "pro_30d": {
        "code": "pro_30d", "tier": "pro", "days": 30, "rub": 40, "usd": "0.60",
        "title": "30 дней", "badge": "популярный",
    },
    "pro_90d": {
        "code": "pro_90d", "tier": "pro", "days": 90, "rub": 100, "usd": "1.50",
        "title": "90 дней", "badge": "выгодно",
    },
    "pro_365d": {
        "code": "pro_365d", "tier": "pro", "days": 365, "rub": 350, "usd": "5.25",
        "title": "365 дней", "badge": "выгодно",
    },
    # MAX — отдельный тариф
    "max_1d": {
        "code": "max_1d", "tier": "max", "days": 1, "rub": 20, "usd": "0.30",
        "title": "1 день", "badge": "MAX",
    },
    "max_30d": {
        "code": "max_30d", "tier": "max", "days": 30, "rub": 120, "usd": "1.80",
        "title": "30 дней", "badge": "MAX",
    },
    "max_90d": {
        "code": "max_90d", "tier": "max", "days": 90, "rub": 200, "usd": "3.00",
        "title": "90 дней", "badge": "MAX · выгодно",
    },
}
# Порядок вывода тарифов в интерфейсе.
PRO_PLAN_ORDER = ["pro_1d", "pro_7d", "pro_30d", "pro_90d", "pro_365d",
                  "max_1d", "max_30d", "max_90d"]
DEFAULT_PRO_PLAN = "pro_30d"


def get_pro_plan(code: Optional[str]) -> Dict[str, Any]:
    """Тариф по коду. Принимает и "pro_7d", и payload "pro_7d:12345".

    Неизвестный/пустой код -> тариф по умолчанию (30 дней).
    """
    if code:
        key = str(code).split(":", 1)[0].strip()
        if key in PRO_PLANS:
            return PRO_PLANS[key]
    return PRO_PLANS[DEFAULT_PRO_PLAN]


def pro_plan_button_text(plan: Dict[str, Any]) -> str:
    """Подпись кнопки выбора срока для Pro или MAX."""
    badge = f" · {plan['badge']}" if plan.get("badge") else ""
    tier = 'MAX · ' if plan.get('tier') == 'max' else ''
    return f"{tier}{plan['title']} — {plan['rub']}₽{badge}"


def pro_plans_text() -> str:
    """Список тарифов для текстовых экранов."""
    lines = []
    for code in PRO_PLAN_ORDER:
        plan = PRO_PLANS[code]
        badge = f" — {plan['badge']}" if plan.get("badge") else ""
        per_day = plan["rub"] / plan["days"]
        extra = f" (~{per_day:.1f}₽/день)" if plan["days"] > 1 else ""
        tier = 'MAX · ' if plan.get('tier') == 'max' else ''
        lines.append(
            f"  • <b>{tier}{plan['title']}</b> — {plan['rub']}₽{extra}{badge}"
        )
    return "\n".join(lines)


def pro_min_price_label() -> str:
    cheapest = min(PRO_PLANS.values(), key=lambda p: p["rub"])
    return f"от {cheapest['rub']}₽ / {cheapest['title']}"


async def record_payment_event(
    user_id: int,
    kind: str,
    provider: str,
    external_id: Any,
    *,
    amount_usdt: Optional[float] = None,
    amount_rub: Optional[float] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """Сохраняет подтверждённый платёж для финансовой админ-панели.

    Пара (provider, external_id) уникальна, поэтому повторная проверка
    одного и того же счёта не добавит выручку дважды.
    """
    event_id = str(external_id or '').strip()
    if not event_id:
        logger.warning('Payment event skipped: empty external id (%s)', kind)
        return False
    try:
        async with db_pool.acquire() as conn:
            result = await conn.execute(
                '''INSERT INTO payment_events
                   (user_id, kind, provider, external_id, amount_usdt,
                    amount_rub, metadata)
                   VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                   ON CONFLICT (provider, external_id) DO NOTHING''',
                user_id,
                kind,
                provider,
                event_id,
                amount_usdt,
                amount_rub,
                json.dumps(metadata or {}, ensure_ascii=False),
            )
        return result.endswith(' 1')
    except Exception as ex:
        # Ошибка статистики не должна мешать уже подтверждённой оплате.
        logger.exception('Could not record payment event %s/%s: %s', provider, event_id, ex)
        return False


async def payment_event_exists(provider: str, external_id: Any) -> bool:
    """True, если такой платёж уже занесён в журнал (значит, срок уже начислен)."""
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchval(
                'SELECT 1 FROM payment_events '
                'WHERE provider = $1 AND external_id = $2',
                provider, str(external_id)
            )
        return bool(row)
    except Exception as ex:
        logger.warning('payment_event_exists failed: %s', ex)
        return False


async def should_activate_pro(provider: str, external_id: Any, fresh: bool) -> bool:
    """Начислять ли срок за платёж.

    fresh=True  — платёж только что попал в журнал, срок начисляем.
    fresh=False — либо это повтор (срок уже начислен), либо журнал недоступен.
    Во втором случае лучше начислить: оплата пользователя важнее статистики.
    """
    if fresh:
        return True
    return not await payment_event_exists(provider, external_id)


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
    # Если Pro/MAX истёк — откатываем на Free.
    if data.get("tier") in ("pro", "max"):
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
    platega_id: Optional[str] = None,
) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO subscriptions "
            "(user_id, tier, expires_at, last_invoice_id, last_invoice_payload, "
            " last_platega_id, updated_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, NOW()) "
            "ON CONFLICT (user_id) DO UPDATE SET "
            "tier = EXCLUDED.tier, expires_at = EXCLUDED.expires_at, "
            "last_invoice_id = EXCLUDED.last_invoice_id, "
            "last_invoice_payload = EXCLUDED.last_invoice_payload, "
            "last_platega_id = EXCLUDED.last_platega_id, "
            "updated_at = NOW()",
            user_id, tier, expires_at, invoice_id, invoice_payload, platega_id
        )


async def is_pro(user_id: int) -> bool:
    """Совместимый хелпер: MAX тоже имеет все возможности Pro."""
    sub = await get_subscription(user_id)
    return sub.get("tier") in ("pro", "max")


def subscription_tier_label(tier: Optional[str]) -> str:
    return {'max': 'MAX', 'pro': 'Pro'}.get(tier or 'free', 'Free')


async def activate_pro_plan(
    user_id: int,
    plan: Dict[str, Any],
    *,
    invoice_id: Optional[int] = None,
    platega_id: Optional[str] = None,
) -> datetime:
    """Включает Pro/MAX на срок тарифа.

    MAX — отдельный тариф.
    Если у пользователя уже активен Pro, то MAX начнётся только после окончания Pro.
    """
    now = datetime.now(MSK_TZ).replace(tzinfo=None)
    base = now
    sub: Dict[str, Any] = {}
    try:
        sub = await get_subscription(user_id)
        if sub.get("tier") in ("pro", "max"):
            exp = sub.get("expires_at")
            if exp is not None:
                if getattr(exp, "tzinfo", None) is not None:
                    exp = exp.astimezone(MSK_TZ).replace(tzinfo=None)
                if exp > now:
                    base = exp
    except Exception as ex:
        logger.warning(f"activate_pro_plan: could not read current sub: {ex}")
    expires = base + timedelta(days=int(plan["days"]))
    target_tier = plan.get('tier', 'pro')

    # MAX — отдельный тариф.
    # Если уже есть Pro — MAX начнётся только после окончания Pro.
    if target_tier == 'max' and sub.get('tier') == 'pro' and base > now:
        # MAX начисляется после окончания Pro
        pass

    # Не понижаем действующий MAX, если пользователь купил обычный Pro.
    if target_tier == 'pro' and sub.get('tier') == 'max' and base > now:
        target_tier = 'max'

    await set_subscription(
        user_id, target_tier, expires,
        invoice_id=invoice_id,
        invoice_payload=f"{plan['code']}:{user_id}",
        platega_id=platega_id,
    )
    return expires


# ---- Free/Pro limit helpers ----

async def count_ai_requests_today(user_id: int) -> int:
    """Returns number of AI requests made by user today (UTC)."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS cnt FROM ai_requests "
            "WHERE user_id = $1 AND created_at >= CURRENT_DATE",
            user_id
        )
        return int(row["cnt"]) if row else 0


async def check_ai_limit(user_id: int) -> bool:
    """Returns True if user is allowed to make an AI request.
    Pro users: unlimited. Free users: max 1 per day."""
    if await is_pro(user_id) or await has_active_custom_llm_api(user_id):
        return True
    count = await count_ai_requests_today(user_id)
    return count < 1


async def get_user_broadcast_seconds_this_week(user_id: int) -> float:
    """Returns total seconds of broadcasts (chat + DM) the user has run
    in the last 7 days."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COALESCE(SUM("
            "  EXTRACT(EPOCH FROM ("
            "    COALESCE(stopped_at, NOW()) - started_at"
            "  ))"
            "), 0) AS total_seconds "
            "FROM broadcasts "
            "WHERE user_id = $1 "
            "  AND started_at >= NOW() - INTERVAL '7 days' "
            "  AND status IN ('active', 'completed', 'stopped')",
            user_id
        )
        seconds = float(row["total_seconds"]) if row else 0.0

    # Also count DM broadcasts
    async with db_pool.acquire() as conn:
        row2 = await conn.fetchrow(
            "SELECT COALESCE(SUM("
            "  EXTRACT(EPOCH FROM ("
            "    COALESCE(updated_at, NOW()) - created_at"
            "  ))"
            "), 0) AS total_seconds "
            "FROM dm_broadcasts "
            "WHERE user_id = $1 "
            "  AND created_at >= NOW() - INTERVAL '7 days' "
            "  AND status IN ('active', 'completed', 'stopped')",
            user_id
        )
        seconds += float(row2["total_seconds"]) if row2 else 0.0

    return seconds


FREE_BROADCAST_LIMIT_HOURS = 24


async def check_broadcast_limit(user_id: int) -> tuple:
    """Returns (allowed: bool, used_hours: float).
    Free users are limited to 24 hours of broadcast runtime per week.
    Pro users have no restriction."""
    if await is_pro(user_id):
        return (True, 0.0)
    used_seconds = await get_user_broadcast_seconds_this_week(user_id)
    used_hours = used_seconds / 3600.0
    return (used_hours < FREE_BROADCAST_LIMIT_HOURS, used_hours)


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
    user_id: int, amount: Optional[str] = None, payload: str = DEFAULT_PRO_PLAN
) -> Dict[str, Any]:
    """Создаёт инвойс в USDT для выбранного плана Pro или MAX."""
    bot_me = await bot.get_me()
    if payload == "wallet_topup":
        invoice_amount = str(amount)
        description = "Vest Game Soft — пополнение баланса"
    else:
        plan = get_pro_plan(payload)
        invoice_amount = str(amount) if amount else str(plan["usd"])
        description = f"Vest Game Soft — {subscription_tier_label(plan.get('tier'))} подписка ({plan['title']})"
    params = {
        "currency_type": "crypto",
        "asset": "USDT",
        "amount": invoice_amount,
        "description": description,
        "payload": f"{payload}:{user_id}",
        "paid_btn_name": "callback",
        "paid_btn_url": f"https://t.me/{bot_me.username}",
    }
    return await _cryptopay_request("createInvoice", params)


async def cryptopay_get_invoices(invoice_ids: str) -> Dict[str, Any]:
    """Запросить статус инвойсов. invoice_ids — comma-separated string."""
    return await _cryptopay_request("getInvoices", {"invoice_ids": invoice_ids})


# ============================================================
# БАЛАНС ПОЛЬЗОВАТЕЛЯ
# ============================================================
async def get_wallet_balance(user_id: int) -> float:
    async with db_pool.acquire() as conn:
        value = await conn.fetchval(
            'SELECT COALESCE(balance, 0) FROM users WHERE user_id = $1', user_id
        )
    return float(value or 0)


async def add_wallet_balance(user_id: int, amount: float) -> None:
    if amount <= 0:
        raise ValueError('Сумма должна быть положительной')
    async with db_pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO users (user_id, balance) VALUES ($1, $2) '
            'ON CONFLICT (user_id) DO UPDATE SET balance = users.balance + $2',
            user_id, amount
        )


def get_balance_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text='Пополнить баланс', callback_data='wallet_topup', style='primary',
        icon_custom_emoji_id=get_icon('MONEY_SEND')
    ))
    builder.row(InlineKeyboardButton(
        text='Назад', callback_data='main_menu', style='default',
        icon_custom_emoji_id=get_icon('BACK')
    ))
    return builder.as_markup()


@dp.callback_query(F.data == 'wallet')
async def wallet_screen(callback: CallbackQuery):
    balance = await get_wallet_balance(callback.from_user.id)
    await callback.message.edit_text(
        f"{emoji('MONEY_SEND')} <b>Баланс</b>\n\n"
        f"Доступно: <b>{balance:.2f} ₽</b>",
        reply_markup=get_balance_keyboard()
    )
    await callback.answer()


# ---- Выбор способа пополнения ----

@dp.callback_query(F.data == 'wallet_topup')
async def wallet_topup_start(callback: CallbackQuery, state: FSMContext):
    """Экран выбора метода пополнения: Crypto Pay (USDT) или СБП (₽)."""
    await state.set_state(BalanceStates.waiting_for_method)
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text='Crypto Pay (USDT)', callback_data='topup_method:crypto',
        style='primary', icon_custom_emoji_id=get_icon('MONEY_SEND')
    ))
    builder.row(InlineKeyboardButton(
        text='СБП (₽)', callback_data='topup_method:sbp',
        style='primary', icon_custom_emoji_id=get_icon('MONEY_SEND')
    ))
    builder.row(InlineKeyboardButton(
        text='Отмена', callback_data='wallet',
        style='default', icon_custom_emoji_id=get_icon('BACK')
    ))
    await callback.message.edit_text(
        f"{emoji('MONEY_SEND')} <b>Пополнение баланса</b>\n\n"
        f"Выберите способ пополнения:",
        reply_markup=builder.as_markup()
    )
    await callback.answer()


@dp.callback_query(F.data == 'topup_method:crypto', BalanceStates.waiting_for_method)
async def topup_choose_crypto(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BalanceStates.waiting_for_amount)
    await callback.message.edit_text(
        f"{emoji('MONEY_SEND')} <b>Пополнение через Crypto Pay</b>\n\n"
        "Введите сумму в USDT от 0.10 до 1000:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text='Отмена', callback_data='wallet',
                                 style='default', icon_custom_emoji_id=get_icon('BACK'))
        ]])
    )
    await callback.answer()


@dp.callback_query(F.data == 'topup_method:sbp', BalanceStates.waiting_for_method)
async def topup_choose_sbp(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BalanceStates.waiting_for_amount_rub)
    await callback.message.edit_text(
        f"{emoji('MONEY_SEND')} <b>Пополнение через СБП</b>\n\n"
        "Введите сумму в рублях от 10 до 80000:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text='Отмена', callback_data='wallet',
                                 style='default', icon_custom_emoji_id=get_icon('BACK'))
        ]])
    )
    await callback.answer()


# ---- Ввод суммы USDT → Crypto Pay ----

@dp.message(BalanceStates.waiting_for_amount)
async def wallet_topup_amount(message: Message, state: FSMContext):
    try:
        amount = round(float((message.text or '').replace(',', '.')), 6)
        if amount < 0.10 or amount > 1000:
            raise ValueError
    except ValueError:
        await message.answer('Введите сумму от 0.10 до 1000 USDT.')
        return
    result = await cryptopay_create_invoice(
        message.from_user.id, amount=f'{amount:.6f}', payload='wallet_topup'
    )
    if not result.get('ok'):
        await state.clear()
        await message.answer(
            f"{emoji('CROSS')} Не удалось создать счёт. Попробуйте позже.",
            reply_markup=get_balance_keyboard()
        )
        return
    inv = result.get('result') or {}
    invoice_id = inv.get('invoice_id')
    pay_url = inv.get('mini_app_invoice_url') or inv.get('bot_invoice_url') or inv.get('web_app_invoice_url')
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO balance_invoices (invoice_id, user_id, amount_usdt)
               VALUES ($1, $2, $3) ON CONFLICT (invoice_id) DO NOTHING''',
            int(invoice_id), message.from_user.id, amount
        )
    await state.clear()
    builder = InlineKeyboardBuilder()
    if pay_url:
        builder.row(InlineKeyboardButton(text='Оплатить через Crypto Pay', url=pay_url,
                                         style='primary', icon_custom_emoji_id=get_icon('MONEY_SEND')))
    builder.row(InlineKeyboardButton(text='Назад', callback_data='wallet', style='default',
                                     icon_custom_emoji_id=get_icon('BACK')))
    sent = await message.answer(
        f"{emoji('MONEY_SEND')} <b>Счёт создан</b>\n\n"
        f"Сумма: <b>{amount:.6f} USDT</b>\n\n"
        f"{emoji('CLOCK')} Оплата проверяется автоматически...",
        reply_markup=builder.as_markup()
    )
    # Запускаем автопроверку в фоне (каждые 2 сек, до 10 минут)
    asyncio.create_task(_auto_check_crypto_topup(
        user_id=message.from_user.id,
        invoice_id=int(invoice_id),
        amount_usdt=amount,
        chat_id=message.chat.id,
        msg_id=sent.message_id,
    ))


async def _auto_check_crypto_topup(
    user_id: int, invoice_id: int, amount_usdt: float,
    chat_id: int, msg_id: int
) -> None:
    """Автоматически проверяет оплату Crypto Pay каждые 2 сек до 10 минут."""
    deadline = time.monotonic() + 10 * 60  # 10 минут
    while time.monotonic() < deadline:
        await asyncio.sleep(2)
        try:
            # Проверяем, не закрыт ли уже инвойс в БД
            async with db_pool.acquire() as conn:
                status_db = await conn.fetchval(
                    "SELECT status FROM balance_invoices WHERE invoice_id = $1",
                    invoice_id
                )
            if status_db == 'paid':
                return  # уже зачислено другим путём

            result = await cryptopay_get_invoices(str(invoice_id))
            item = ((result.get('result') or {}).get('items') or [None])[0] if result.get('ok') else None
            if not item or item.get('status') != 'paid':
                continue

            async with db_pool.acquire() as conn:
                claimed = await conn.fetchrow(
                    "UPDATE balance_invoices SET status = 'paid', paid_at = NOW() "
                    "WHERE invoice_id = $1 AND status = 'active' RETURNING amount_usdt",
                    invoice_id
                )
            if claimed:
                # Баланс хранится в рублях: конвертируем USDT → ₽
                paid_usdt = float(claimed['amount_usdt'])
                credited_rub = round(paid_usdt * TOPUP_RUB_PER_USDT, 2)
                await add_wallet_balance(user_id, credited_rub)
                await record_payment_event(
                    user_id,
                    PAYMENT_KIND_WALLET_TOPUP,
                    PAYMENT_PROVIDER_CRYPTOPAY,
                    invoice_id,
                    amount_usdt=paid_usdt,
                    metadata={'credited_rub': credited_rub},
                )
            balance = await get_wallet_balance(user_id)
            try:
                await bot.edit_message_text(
                    chat_id=chat_id, message_id=msg_id,
                    text=(
                        f"{emoji('CHECK')} <b>Баланс пополнен!</b>\n\n"
                        f"Зачислено: <b>{amount_usdt:.6f} USDT</b>\n"
                        f"Текущий баланс: <b>{balance:.2f} ₽</b>"
                    ),
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text='Назад', callback_data='wallet',
                                             style='default', icon_custom_emoji_id=get_icon('BACK'))
                    ]])
                )
            except Exception:
                pass
            return
        except Exception as ex:
            logger.warning(f"[auto_check_crypto_topup] error: {ex}")
    # Время вышло — обновляем сообщение
    try:
        async with db_pool.acquire() as conn:
            status_db = await conn.fetchval(
                "SELECT status FROM balance_invoices WHERE invoice_id = $1", invoice_id
            )
        if status_db == 'paid':
            return
        await bot.edit_message_text(
            chat_id=chat_id, message_id=msg_id,
            text=(
                f"{emoji('CROSS')} Время ожидания оплаты истекло.\n"
                f"Если вы оплатили — напишите в поддержку: {SUPPORT_USERNAME}"
            ),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text='Назад', callback_data='wallet',
                                     style='default', icon_custom_emoji_id=get_icon('BACK'))
            ]])
        )
    except Exception:
        pass


# ---- Ввод суммы в рублях → СБП ----

@dp.message(BalanceStates.waiting_for_amount_rub)
async def wallet_topup_amount_rub(message: Message, state: FSMContext):
    try:
        amount_rub = round(float((message.text or '').replace(',', '.')), 2)
        if amount_rub < 10 or amount_rub > 80000:
            raise ValueError
    except ValueError:
        await message.answer('Введите сумму в рублях от 10 до 80 000.')
        return
    amount_usdt = round(amount_rub / TOPUP_RUB_PER_USDT, 6)

    result = await platega_create_transaction(
        user_id=message.from_user.id,
        amount=int(amount_rub),
        payload='wallet_topup_sbp'
    )
    if not result.get('ok'):
        await state.clear()
        await message.answer(
            f"{emoji('CROSS')} Не удалось создать счёт СБП. Попробуйте позже.",
            reply_markup=get_balance_keyboard()
        )
        return
    tx = result['result']
    transaction_id = tx.get('transactionId')
    pay_url = tx.get('redirect')

    # Создаём запись в balance_invoices с искусственным id = hash от transaction_id
    fake_invoice_id = abs(hash(transaction_id)) % (10 ** 15)
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO balance_invoices
               (invoice_id, user_id, amount_usdt, amount_rub, sbp_platega_id)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (invoice_id) DO NOTHING''',
            fake_invoice_id, message.from_user.id, amount_usdt, amount_rub, transaction_id
        )
    await state.clear()
    builder = InlineKeyboardBuilder()
    if pay_url:
        builder.row(InlineKeyboardButton(
            text='Оплатить по СБП', url=pay_url,
            style='primary', icon_custom_emoji_id=get_icon('MONEY_SEND')
        ))
    builder.row(InlineKeyboardButton(text='Назад', callback_data='wallet', style='default',
                                     icon_custom_emoji_id=get_icon('BACK')))
    sent = await message.answer(
        f"{emoji('MONEY_SEND')} <b>Счёт СБП создан</b>\n\n"
        f"Сумма: <b>{amount_rub:.0f} ₽</b>\n\n"
        f"{emoji('CLOCK')} Оплата проверяется автоматически...",
        reply_markup=builder.as_markup()
    )
    # Запускаем автопроверку в фоне (каждые 2 сек, до 30 минут)
    asyncio.create_task(_auto_check_sbp_topup(
        user_id=message.from_user.id,
        fake_invoice_id=fake_invoice_id,
        transaction_id=transaction_id,
        amount_rub=amount_rub,
        amount_usdt=amount_usdt,
        chat_id=message.chat.id,
        msg_id=sent.message_id,
    ))


async def _auto_check_sbp_topup(
    user_id: int, fake_invoice_id: int, transaction_id: str,
    amount_rub: float, amount_usdt: float,
    chat_id: int, msg_id: int
) -> None:
    """Автоматически проверяет оплату СБП (Platega) каждые 2 сек до 30 минут."""
    deadline = time.monotonic() + 30 * 60  # 30 минут
    while time.monotonic() < deadline:
        await asyncio.sleep(2)
        try:
            async with db_pool.acquire() as conn:
                status_db = await conn.fetchval(
                    "SELECT status FROM balance_invoices WHERE invoice_id = $1",
                    fake_invoice_id
                )
            if status_db == 'paid':
                return

            result = await platega_get_transaction(transaction_id)
            if not result.get('ok'):
                continue
            status = (result['result'].get('status') or '').upper()
            if status != 'CONFIRMED':
                continue

            async with db_pool.acquire() as conn:
                claimed = await conn.fetchrow(
                    "UPDATE balance_invoices SET status = 'paid', paid_at = NOW() "
                    "WHERE invoice_id = $1 AND status = 'active' RETURNING amount_rub, amount_usdt",
                    fake_invoice_id
                )
            if claimed:
                # Зачисляем рубли напрямую (баланс хранится в рублях)
                credited = float(claimed['amount_rub'] or amount_rub)
                await add_wallet_balance(user_id, credited)
                await record_payment_event(
                    user_id,
                    PAYMENT_KIND_WALLET_TOPUP,
                    PAYMENT_PROVIDER_PLATEGA,
                    transaction_id,
                    amount_rub=credited,
                    metadata={'credited_rub': credited},
                )
            balance = await get_wallet_balance(user_id)
            try:
                await bot.edit_message_text(
                    chat_id=chat_id, message_id=msg_id,
                    text=(
                        f"{emoji('CHECK')} <b>Баланс пополнен!</b>\n\n"
                        f"Зачислено: <b>{amount_rub:.0f} ₽</b>\n"
                        f"Текущий баланс: <b>{balance:.2f} ₽</b>"
                    ),
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text='Назад', callback_data='wallet',
                                             style='default', icon_custom_emoji_id=get_icon('BACK'))
                    ]])
                )
            except Exception:
                pass
            return
        except Exception as ex:
            logger.warning(f"[auto_check_sbp_topup] error: {ex}")
    # Время вышло
    try:
        async with db_pool.acquire() as conn:
            status_db = await conn.fetchval(
                "SELECT status FROM balance_invoices WHERE invoice_id = $1", fake_invoice_id
            )
        if status_db == 'paid':
            return
        await bot.edit_message_text(
            chat_id=chat_id, message_id=msg_id,
            text=(
                f"{emoji('CROSS')} Время ожидания оплаты истекло.\n"
                f"Если вы оплатили — напишите в поддержку: {SUPPORT_USERNAME}"
            ),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text='Назад', callback_data='wallet',
                                     style='default', icon_custom_emoji_id=get_icon('BACK'))
            ]])
        )
    except Exception:
        pass


# ------------------------------------------------------------
# СБП (Platega) — альтернативный способ оплаты Pro-подписки
# Конфигурация (merchant id / api key) вынесена в начало файла.
# ------------------------------------------------------------
def _platega_headers() -> Dict[str, str]:
    return {
        "X-MerchantId": PLATEGA_MERCHANT_ID,
        "X-Secret": PLATEGA_SECRET,
        "Content-Type": "application/json",
    }


async def platega_create_transaction(
    user_id: int, amount: Optional[int] = None, payload: str = DEFAULT_PRO_PLAN
) -> Dict[str, Any]:
    """Создаёт СБП-транзакцию в Platega. Возвращает {ok, result} либо {ok:false, error}.

    Для Pro/MAX сумма в рублях берётся из PRO_PLANS по payload, если не задана явно.
    """
    try:
        bot_me = await bot.get_me()
        return_url = f"https://t.me/{bot_me.username}"
    except Exception:
        return_url = "https://t.me"
    if payload == "wallet_topup_sbp":
        rub_amount = int(amount or 0)
        description = "Vest Game Soft — пополнение баланса"
    else:
        plan = get_pro_plan(payload)
        rub_amount = int(amount) if amount else int(plan["rub"])
        description = f"Vest Game Soft — {subscription_tier_label(plan.get('tier'))} подписка ({plan['title']})"
    body = {
        "paymentMethod": PLATEGA_PAYMENT_METHOD_SBP,
        "paymentDetails": {"amount": rub_amount, "currency": "RUB"},
        "description": description,
        "return": return_url,
        "failedUrl": return_url,
        "payload": f"{payload}:{user_id}",
        "metadata": {"userId": str(user_id), "userName": f"tg_{user_id}"},
    }
    url = f"{PLATEGA_API}/transaction/process"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=body, headers=_platega_headers(),
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                data = await resp.json(content_type=None)
                if resp.status == 200 and isinstance(data, dict) and data.get("transactionId"):
                    return {"ok": True, "result": data}
                return {"ok": False, "error": data}
    except Exception as ex:
        logger.error(f"Platega create failed: {ex}")
        return {"ok": False, "error": str(ex)}


async def platega_get_transaction(transaction_id: str) -> Dict[str, Any]:
    """Проверяет статус СБП-транзакции в Platega."""
    url = f"{PLATEGA_API}/transaction/{transaction_id}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, headers=_platega_headers(),
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                data = await resp.json(content_type=None)
                if resp.status == 200 and isinstance(data, dict):
                    return {"ok": True, "result": data}
                return {"ok": False, "error": data}
    except Exception as ex:
        logger.error(f"Platega status failed: {ex}")
        return {"ok": False, "error": str(ex)}


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

            # Список вариантов сообщений. Если payload прислал message_texts
            # (новый формат) — используем его, иначе собираем один из
            # message_text / message_media.
            raw_variants = payload.get('message_texts')
            if not raw_variants:
                raw_variants = [{
                    'text': payload.get('message_text', ''),
                    'media': list(payload.get('message_media') or []),
                }]
            variants_json = json.dumps(raw_variants, ensure_ascii=False)

            async with db_pool.acquire() as conn:
                entity_id = await conn.fetchval(
                    '''INSERT INTO broadcasts
                    (user_id, account_id, chat_ids, delay, message_count,
                    message_text, message_media, message_texts, mode, status,
                    scheduled_at, broadcast_type)
                    VALUES ($1, $2, $3::text[], $4, $5, $6, $7::text[], $8::jsonb,
                            $9, $10, $11, 'chat')
                    RETURNING id''',
                    task['user_id'], int(payload['account_id']),
                    [str(x) for x in payload.get('chat_ids', [])],
                    int(payload.get('delay', 30)), int(payload.get('message_count', 1)),
                    payload.get('message_text', ''), payload.get('message_media', []),
                    variants_json,
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
            # Список вариантов сообщений для DM-рассылки (новый формат).
            raw_variants = payload.get('message_texts')
            if not raw_variants:
                raw_variants = [{
                    'text': payload.get('message_text', ''),
                    'media': list(payload.get('message_media') or []),
                }]
            variants_json = json.dumps(raw_variants, ensure_ascii=False)

            async with db_pool.acquire() as conn:
                entity_id = await conn.fetchval(
                    '''INSERT INTO dm_broadcasts
                    (user_id, account_id, usernames, delay, message_text,
                    message_media, message_texts, status, total_count)
                    VALUES ($1, $2, $3::text[], $4, $5, $6::text[], $7::jsonb,
                            'active', $8)
                    RETURNING id''',
                    task['user_id'], int(payload['account_id']),
                    [str(x) for x in payload.get('usernames', [])],
                    int(payload.get('delay', 30)), payload.get('message_text', ''),
                    payload.get('message_media', []), variants_json,
                    len(payload.get('usernames', []))
                )
            await update_queue_task(task_id, 'running', {'dm_id': entity_id}, entity_id=entity_id)
            result = await execute_dm_broadcast_db(
                entity_id, task_id, int(payload['account_id']), int(task['user_id']),
                payload.get('usernames', []), payload.get('message_text', ''),
                int(payload.get('delay', 30)), payload.get('message_media', []),
                raw_variants
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

        if task_type == 'run_script':
            script_id = int(payload['script_id'])
            started, error = await start_script_runner(
                script_id, int(task['user_id'])
            )
            if not started:
                raise RuntimeError(error)
            await update_queue_task(
                task_id, 'completed', {'script_id': script_id, 'started': True}, entity_id=script_id
            )
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
        text="Открыть мини-апп",
        web_app=WebAppInfo(url="https://vestgamesoft.shop"),
        style='primary',
        icon_custom_emoji_id=get_icon("APPS")
    ))
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
    builder.row(
        InlineKeyboardButton(
            text="Чат с нейросетями",
            callback_data="ai_chat",
            style='primary',
            icon_custom_emoji_id=get_icon("AI")
        ),
        InlineKeyboardButton(
            text="Моя подписка",
            callback_data="my_subscription",
            style='success',
            icon_custom_emoji_id=get_icon("MONEY_SEND")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="Баланс",
            callback_data="wallet",
            style='default',
            icon_custom_emoji_id=get_icon("MONEY_SEND")
        ),
        InlineKeyboardButton(
            text="Пополнить баланс",
            callback_data="wallet_topup",
            style='primary',
            icon_custom_emoji_id=get_icon("MONEY_SEND")
        )
    )
    builder.row(InlineKeyboardButton(
        text="Помощь",
        callback_data="help",
        style='default',
        icon_custom_emoji_id=get_icon("INFO")
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

def get_proxies_keyboard(
    proxies: List[Dict],
    accounts_by_proxy: Optional[Dict[int, List[str]]] = None,
) -> InlineKeyboardMarkup:
    """Список прокси с пометкой о привязанных аккаунтах."""
    builder = InlineKeyboardBuilder()
    for p in proxies:
        label = p.get('label') or f"{p['host']}:{p['port']}"
        bound = (accounts_by_proxy or {}).get(p['id'], [])
        badge = f" [{len(bound)} акк.]" if bound else ""
        builder.row(InlineKeyboardButton(
            text=f"{p['proxy_type']} | {label}{badge}",
            callback_data=f"manage_proxy_{p['id']}",
            style='default'
        ))
    builder.row(
        InlineKeyboardButton(
            text="Добавить прокси",
            callback_data="add_proxy",
            style='success',
            icon_custom_emoji_id=get_icon("ADD_TEXT")
        ),
        InlineKeyboardButton(
            text="Проверить все",
            callback_data="check_all_proxies",
            style='primary',
            icon_custom_emoji_id=get_icon("REFRESH")
        )
    )
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
        text="Проверить соединение",
        callback_data=f"check_proxy_{proxy_id}",
        style='primary',
        icon_custom_emoji_id=get_icon("REFRESH")
    ))
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
        text="Настроить свой API",
        callback_data="ai_api_settings",
        style='default',
        icon_custom_emoji_id=get_icon("GEAR")
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
            text="Настроить свой API",
            callback_data="ai_api_settings",
            style='default',
            icon_custom_emoji_id=get_icon("GEAR")
        ))
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

    def button(
        text: str, callback_data: str, icon: str,
        style: str = 'primary',
    ) -> InlineKeyboardButton:
        return InlineKeyboardButton(
            text=text,
            callback_data=callback_data,
            style=style,
            icon_custom_emoji_id=get_icon(icon),
        )

    rows = (
        (
            button("Рассылка", "broadcast", "SEND"),
            button("Отложенная", "scheduled_broadcast", "CLOCK"),
        ),
        (
            button("Рассылка в ЛС", "dm_broadcast", "CHAT"),
            button("Автоответчик", "auto_responder", "BELL"),
        ),
        (
            button("Вступление", "join_chats", "JOIN"),
            button("Создать каналы", "create_channels", "GLOBE"),
        ),
        (
            button("Автосаб", "autosub", "JOIN"),
            button("Создать группы", "create_groups", "PEOPLE"),
        ),
        (
            button("Авто-лайкинг", "autolike", "LIKE"),
            button("Нейрокомментинг", "neurocomment", "AI"),
        ),
        (
            button("Удалить сообщения", "delete_messages", "SWEEP"),
            button("Парсинг чата", "parsing", "USERS"),
        ),
        (
            button("Скрипты", "scripts", "PLAY"),
        ),
        (
            button("AI Генератор", "ai_generator", "AI"),
            button("Мои AI запросы", "ai_history", "CHART", 'default'),
        ),
        (
            button("Мои рассылки", "my_broadcasts", "CHART", 'default'),
            button("Мои автоответчики", "my_auto_responders", "BELL", 'default'),
        ),
        (
            button("Шаблоны", "broadcast_templates", "CLIPBOARD", 'default'),
            button("Назад", "main_menu", "BACK", 'default'),
        ),
    )
    for row in rows:
        builder.row(*row)
    return builder.as_markup()


def get_scripts_keyboard(
    scripts: List[Dict[str, Any]]
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    status_icons = {
        'completed': '✅',
        'failed': '❌',
        'running': '⏳',
        'stopped': '⏹',
        'never': '•',
    }
    for script in scripts:
        status = status_icons.get(script.get('last_status'), '•')
        title = str(script.get('name') or f"Скрипт #{script['id']}")
        if len(title) > 32:
            title = title[:31] + '…'
        builder.row(InlineKeyboardButton(
            text=f"{status} {title} · @{script['bot_username']}",
            callback_data=f"script:view:{script['id']}",
            style='default',
        ))
    builder.row(InlineKeyboardButton(
        text="Создать скрипт",
        callback_data="script:create",
        style='success',
        icon_custom_emoji_id=get_icon("ADD_TEXT"),
    ))
    builder.row(InlineKeyboardButton(
        text="Публичные скрипты",
        callback_data="script:public",
        style='primary',
        icon_custom_emoji_id=get_icon("GLOBE"),
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="functions",
        style='default',
        icon_custom_emoji_id=get_icon("BACK"),
    ))
    return builder.as_markup()


def get_script_accounts_keyboard(
    accounts: List[Dict[str, Any]]
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for account in accounts:
        builder.row(InlineKeyboardButton(
            text=str(account['phone']),
            callback_data=f"script:account:{account['id']}",
            style='default',
            icon_custom_emoji_id=get_icon("PROFILE"),
        ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="script:cancel",
        style='danger',
        icon_custom_emoji_id=get_icon("CROSS"),
    ))
    return builder.as_markup()


def get_script_buttons_keyboard(
    buttons: List[Dict[str, Any]], steps_count: int = 0,
) -> InlineKeyboardMarkup:
    """Кнопки текущего экрана бота при построении маршрута скрипта."""
    builder = InlineKeyboardBuilder()
    for button in buttons:
        action = button.get('action') or ('click' if button.get('selectable') else '')
        prefix = '➡' if action == 'click' else ('📢' if action == 'join_channel' else '🔗')
        text = str(button.get('text') or 'Без названия')
        if len(text) > 42:
            text = text[:41] + '…'
        callback_data = (
            f"script:button:{button['row']}:{button['col']}"
            if action else 'script:unsupported'
        )
        builder.row(InlineKeyboardButton(
            text=f"{prefix} {text}",
            callback_data=callback_data,
            style='primary' if action else 'default',
        ))
    if steps_count:
        builder.row(InlineKeyboardButton(
            text=f"Сохранить маршрут ({steps_count})",
            callback_data='script:route_done',
            style='success',
            icon_custom_emoji_id=get_icon('CHECK'),
        ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="script:cancel",
        style='danger',
        icon_custom_emoji_id=get_icon("CROSS"),
    ))
    return builder.as_markup()


def get_script_captcha_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text='Да, есть фото-капча', callback_data='script:captcha:yes',
        style='primary', icon_custom_emoji_id=get_icon('MEDIA'),
    ))
    builder.row(InlineKeyboardButton(
        text='Нет фото-капчи', callback_data='script:captcha:no',
        style='default', icon_custom_emoji_id=get_icon('CHECK'),
    ))
    builder.row(InlineKeyboardButton(
        text='Отмена', callback_data='script:cancel',
        style='danger', icon_custom_emoji_id=get_icon('CROSS'),
    ))
    return builder.as_markup()


def get_script_step_confirmation_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text='Нажать и перейти дальше',
        callback_data='script:step:intermediate',
        style='primary', icon_custom_emoji_id=get_icon('RIGHT'),
    ))
    builder.row(InlineKeyboardButton(
        text='Сделать финальным шагом',
        callback_data='script:step:final',
        style='success', icon_custom_emoji_id=get_icon('CHECK'),
    ))
    builder.row(InlineKeyboardButton(
        text='Назад к кнопкам',
        callback_data='script:step:back',
        style='default', icon_custom_emoji_id=get_icon('BACK'),
    ))
    return builder.as_markup()


def get_script_actions_keyboard(
    script_id: int, is_running: bool = False, is_public: bool = False,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if is_running:
        builder.row(InlineKeyboardButton(
            text="Остановить скрипт",
            callback_data=f"script:stop:{script_id}",
            style='danger',
            icon_custom_emoji_id=get_icon("STOP"),
        ))
    else:
        builder.row(InlineKeyboardButton(
            text="Запустить бесконечно",
            callback_data=f"script:run:{script_id}",
            style='success',
            icon_custom_emoji_id=get_icon("PLAY"),
        ))
    builder.row(InlineKeyboardButton(
        text="Обновить маршрут",
        callback_data=f"script:refresh:{script_id}",
        style='primary',
        icon_custom_emoji_id=get_icon("REFRESH"),
    ))
    builder.row(InlineKeyboardButton(
        text="Снять с публикации" if is_public else "Выложить публично",
        callback_data=(
            f"script:unpublish:{script_id}" if is_public
            else f"script:publish:{script_id}"
        ),
        style='default',
        icon_custom_emoji_id=get_icon("GLOBE"),
    ))
    builder.row(InlineKeyboardButton(
        text="Удалить",
        callback_data=f"script:delete_ask:{script_id}",
        style='danger',
        icon_custom_emoji_id=get_icon("DELETE"),
    ))
    builder.row(InlineKeyboardButton(
        text="К списку",
        callback_data="scripts",
        style='default',
        icon_custom_emoji_id=get_icon("BACK"),
    ))
    return builder.as_markup()


def format_script_card(script: Dict[str, Any]) -> str:
    status_labels = {
        'completed': 'Выполнен',
        'failed': 'Ошибка',
        'running': 'Выполняется бесконечно',
        'stopped': 'Остановлен',
        'never': 'Ещё не запускался',
    }
    status = status_labels.get(
        script.get('last_status'), script.get('last_status') or '—'
    )
    payload = script.get('start_payload') or '—'
    last_run = script.get('last_run_at')
    last_run_text = (
        last_run.strftime('%d.%m.%Y %H:%M:%S')
        if hasattr(last_run, 'strftime') else '—'
    )
    error_line = ''
    if script.get('last_error'):
        error_line = (
            f"\n{emoji('CROSS')} Последняя ошибка: "
            f"<code>{escape(str(script['last_error'])[:500])}</code>"
        )
    steps = normalize_script_steps(script)
    route_preview = ' → '.join(
        escape(str(step.get('text') or 'Без названия')) for step in steps
    ) or '—'
    if len(route_preview) > 700:
        route_preview = route_preview[:699] + '…'
    captcha_label = 'включена' if script.get('captcha_enabled') else 'выключена'
    public_label = (
        f"опубликован · применений: {script.get('public_uses', 0)}"
        if script.get('is_public') else 'не опубликован'
    )
    return (
        f"{emoji('PLAY')} <b>{escape(str(script['name']))}</b>\n\n"
        f"{emoji('PHONE')} Аккаунт: "
        f"<code>{escape(str(script['phone']))}</code>\n"
        f"{emoji('BOT')} Бот: "
        f"<code>@{escape(str(script['bot_username']))}</code>\n"
        f"{emoji('LINK')} Ссылка: "
        f"<code>{escape(str(script['bot_url']))}</code>\n"
        f"{emoji('KEY')} Start-параметр: "
        f"<code>{escape(str(payload))}</code>\n"
        f"{emoji('CLIPBOARD')} Шагов маршрута: <b>{len(steps)}</b>\n"
        f"Маршрут: <i>{route_preview}</i>\n"
        f"{emoji('MEDIA')} Фото-капча: <b>{captcha_label}</b>\n"
        f"{emoji('GLOBE')} Публичный доступ: <b>{escape(str(public_label))}</b>\n"
        f"{emoji('INFO')} Статус: <b>{escape(str(status))}</b>\n"
        f"{emoji('CLOCK')} Последний запуск: "
        f"<code>{escape(last_run_text)}</code>"
        f"{error_line}"
    )


def format_public_script_card(script: Dict[str, Any]) -> str:
    author = (
        f"@{script['username']}" if script.get('username')
        else (script.get('first_name') or 'Автор')
    )
    steps = normalize_script_steps(script)
    route = ' → '.join(escape(str(step.get('text') or '—')) for step in steps) or '—'
    if len(route) > 700:
        route = route[:699] + '…'
    captcha = 'есть' if script.get('captcha_enabled') else 'нет'
    return (
        f"{emoji('GLOBE')} <b>{escape(str(script['name']))}</b>\n\n"
        f"Автор: <b>{escape(str(author))}</b>\n"
        f"Бот: <code>@{escape(str(script['bot_username']))}</code>\n"
        f"Шагов: <b>{len(steps)}</b>\n"
        f"Маршрут: <i>{route}</i>\n"
        f"Фото-капча: <b>{captcha}</b>\n"
        f"Применений: <b>{script.get('public_uses', 0)}</b>\n\n"
        "После применения маршрут будет сохранён у вас. Для запуска выберите свой Telegram-аккаунт."
    )


def get_public_scripts_keyboard(scripts: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for script in scripts:
        author = f"@{script['username']}" if script.get('username') else (script.get('first_name') or 'Автор')
        name = str(script.get('name') or 'Без названия')[:42]
        builder.row(InlineKeyboardButton(
            text=f"{name} · {str(author)[:16]}",
            callback_data=f"script:public_view:{script['id']}",
            style='default', icon_custom_emoji_id=get_icon('GLOBE'),
        ))
    builder.row(InlineKeyboardButton(
        text='К моим скриптам', callback_data='scripts',
        style='default', icon_custom_emoji_id=get_icon('BACK'),
    ))
    return builder.as_markup()


def get_public_script_actions_keyboard(script_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text='Применить себе', callback_data=f'script:apply:{script_id}',
        style='success', icon_custom_emoji_id=get_icon('CHECK'),
    ))
    builder.row(InlineKeyboardButton(
        text='К публичным скриптам', callback_data='script:public',
        style='default', icon_custom_emoji_id=get_icon('BACK'),
    ))
    return builder.as_markup()


def get_public_script_account_keyboard(public_script_id: int, accounts: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for account in accounts:
        builder.row(InlineKeyboardButton(
            text=str(account['phone']),
            callback_data=f"script:apply_account:{public_script_id}:{account['id']}",
            style='default', icon_custom_emoji_id=get_icon('PROFILE'),
        ))
    builder.row(InlineKeyboardButton(
        text='Отмена', callback_data=f'script:public_view:{public_script_id}',
        style='default', icon_custom_emoji_id=get_icon('BACK'),
    ))
    return builder.as_markup()


def get_help_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура экрана «Помощь» — список фичей с подробной инструкцией + документы."""
    builder = InlineKeyboardBuilder()
    # Управление аккаунтами
    builder.row(
        InlineKeyboardButton(
            text="👥 Менеджер аккаунтов",
            callback_data="help_accounts",
            style='primary',
            icon_custom_emoji_id=get_icon("PEOPLE")
        )
    )
    # Рассылки (чат + ЛС + отложенная)
    builder.row(
        InlineKeyboardButton(
            text="📨 Рассылка",
            callback_data="help_broadcast",
            style='primary',
            icon_custom_emoji_id=get_icon("SEND")
        ),
        InlineKeyboardButton(
            text="💬 Рассылка в ЛС",
            callback_data="help_dm_broadcast",
            style='primary',
            icon_custom_emoji_id=get_icon("CHAT")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="⏰ Отложенная рассылка",
            callback_data="help_scheduled",
            style='primary',
            icon_custom_emoji_id=get_icon("CLOCK")
        )
    )
    # Автоматизация
    builder.row(
        InlineKeyboardButton(
            text="🔔 Автоответчик",
            callback_data="help_autoresponder",
            style='primary',
            icon_custom_emoji_id=get_icon("BELL")
        ),
        InlineKeyboardButton(
            text="👍 Автолайкинг",
            callback_data="help_autolike",
            style='primary',
            icon_custom_emoji_id=get_icon("LIKE")
        )
    )
    # Работа с чатами
    builder.row(
        InlineKeyboardButton(
            text="🚪 Вступление в чаты",
            callback_data="help_join",
            style='primary',
            icon_custom_emoji_id=get_icon("JOIN")
        ),
        InlineKeyboardButton(
            text="🧹 Удаление сообщений",
            callback_data="help_delete",
            style='primary',
            icon_custom_emoji_id=get_icon("SWEEP")
        )
    )
    # Сбор данных
    builder.row(
        InlineKeyboardButton(
            text="👥 Парсинг чата",
            callback_data="help_parse",
            style='primary',
            icon_custom_emoji_id=get_icon("USERS")
        )
    )
    # Скрипты и AI
    builder.row(
        InlineKeyboardButton(
            text="▶ Скрипты",
            callback_data="help_scripts",
            style='primary',
            icon_custom_emoji_id=get_icon("PLAY")
        ),
        InlineKeyboardButton(
            text="🧠 AI Генератор",
            callback_data="help_ai",
            style='primary',
            icon_custom_emoji_id=get_icon("AI")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="💬 Чат с ИИ",
            callback_data="help_ai_chat",
            style='primary',
            icon_custom_emoji_id=get_icon("AI")
        ),
        InlineKeyboardButton(
            text="🤖 Нейрокомментинг",
            callback_data="help_neurocomment",
            style='primary',
            icon_custom_emoji_id=get_icon("AI")
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="⭐ Тарифы Free / Pro / MAX",
            callback_data="help_tariffs",
            style='primary',
            icon_custom_emoji_id=get_icon("STAR")
        ),
        InlineKeyboardButton(
            text="📊 Мониторинг аккаунтов",
            callback_data="help_monitoring",
            style='primary',
            icon_custom_emoji_id=get_icon("STATS")
        )
    )
    # Документы
    builder.row(InlineKeyboardButton(
        text="📜 Политика конфиденциальности",
        url="https://telegra.ph/POLITIKA-KONFIDENCIALNOSTI-07-23-54",
        style='default',
        icon_custom_emoji_id=get_icon("LOCK_CLOSED")
    ))
    builder.row(InlineKeyboardButton(
        text="📄 Пользовательское соглашение",
        url="https://telegra.ph/POLZOVATELSKOE-SOGLASHENIE-07-23-24",
        style='default',
        icon_custom_emoji_id=get_icon("FILE")
    ))
    builder.row(InlineKeyboardButton(
        text="◁ Назад",
        callback_data="main_menu",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()


def get_help_feature_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура «назад» для подробной страницы помощи по конкретной фиче."""
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="◁ Назад к помощи",
        callback_data="help",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    builder.row(InlineKeyboardButton(
        text="◁ В главное меню",
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
        text="Шаблоны рассылок",
        callback_data="broadcast_templates",
        style='default',
        icon_custom_emoji_id=get_icon("CLIPBOARD")
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
        text="Сохранить как шаблон",
        callback_data="broadcast_template_save",
        style='default',
        icon_custom_emoji_id=get_icon("CLIPBOARD")
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
        premium = " Premium" if acc.get('telegram_premium') else ""
        validity = " Проверен" if acc.get('validation_status') == 'valid' else ""
        builder.row(InlineKeyboardButton(
            text=f"{acc['phone']}{premium}{validity} {status} {warming}",
            callback_data=f"{callback_prefix}_{acc['id']}",
            style='default',
            icon_custom_emoji_id=get_icon("STAR" if acc.get('telegram_premium') else "PROFILE")
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
        text="Проверить валидность",
        callback_data=f"validate_account_{account_id}",
        style='primary',
        icon_custom_emoji_id=get_icon("CHECK")
    ))
    builder.row(InlineKeyboardButton(
        text="Спамблок и уведомления",
        callback_data=f"spam_check_menu:{account_id}",
        style='primary',
        icon_custom_emoji_id=get_icon("BELL")
    ))
    builder.row(InlineKeyboardButton(
        text="Логи аккаунта",
        callback_data=f"account_logs_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("EYE")
    ))
    builder.row(InlineKeyboardButton(
        text="📊 Дашборд здоровья",
        callback_data=f"account_dashboard_{account_id}",
        style='primary',
        icon_custom_emoji_id=get_icon("STATS")
    ))
    builder.row(InlineKeyboardButton(
        text="🛡 Отпечаток устройства",
        callback_data=f"fingerprint_menu_{account_id}",
        style='primary',
        icon_custom_emoji_id=get_icon("PHONE")
    ))
    builder.row(InlineKeyboardButton(
        text="ИИ-автоответчик",
        callback_data=f"acct_ar:home:{account_id}",
        style='primary',
        icon_custom_emoji_id=get_icon("AI")
    ))
    builder.row(InlineKeyboardButton(
        text="Изменить профиль",
        callback_data=f"edit_profile_{account_id}",
        style='primary',
        icon_custom_emoji_id=get_icon("PROFILE")
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


def get_chat_creation_accounts_keyboard(
    accounts: List[Dict], creation_kind: str,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for account in accounts:
        builder.row(InlineKeyboardButton(
            text=str(account['phone']),
            callback_data=(
                f"select_chat_create_account:{creation_kind}:{account['id']}"
            ),
            style='default',
            icon_custom_emoji_id=get_icon("PROFILE"),
        ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="chat_creation_cancel",
        style='danger',
        icon_custom_emoji_id=get_icon("CROSS"),
    ))
    return builder.as_markup()


def get_chat_creation_preview_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Начать создание",
        callback_data="start_chat_creation",
        style='success',
        icon_custom_emoji_id=get_icon("PLAY"),
    ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="chat_creation_cancel",
        style='danger',
        icon_custom_emoji_id=get_icon("CROSS"),
    ))
    return builder.as_markup()


def get_chat_creation_control_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Остановить",
        callback_data="stop_chat_creation",
        style='danger',
        icon_custom_emoji_id=get_icon("STOP"),
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
            text="Назад",
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


def get_neurocomment_channel_keyboard(
    channels: List[Dict[str, Any]], page: int = 0,
    selected_channels: Optional[List[str]] = None,
) -> InlineKeyboardMarkup:
    selected = {str(item) for item in (selected_channels or [])}
    per_page = 10
    page = max(0, page)
    start = page * per_page
    visible = channels[start:start + per_page]
    builder = InlineKeyboardBuilder()
    for channel in visible:
        channel_id = str(channel['id'])
        is_selected = channel_id in selected
        builder.row(InlineKeyboardButton(
            text=f"{'✓ ' if is_selected else ''}{str(channel.get('name') or 'Без названия')[:36]}",
            callback_data=f'neurocomm:toggle:{channel_id}',
            style='success' if is_selected else 'default',
            icon_custom_emoji_id=get_icon('CHECK') if is_selected else get_icon('GLOBE'),
        ))
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(
            text='Назад', callback_data=f'neurocomm:page:{page - 1}',
            style='default', icon_custom_emoji_id=get_icon('BACK'),
        ))
    if start + per_page < len(channels):
        nav.append(InlineKeyboardButton(
            text='Вперёд', callback_data=f'neurocomm:page:{page + 1}',
            style='default', icon_custom_emoji_id=get_icon('CHART_UP'),
        ))
    if nav:
        builder.row(*nav)
    builder.row(InlineKeyboardButton(
        text=f'Готово (выбрано: {len(selected)})',
        callback_data='neurocomm:channels_done',
        style='success', icon_custom_emoji_id=get_icon('CHECK'),
    ))
    builder.row(InlineKeyboardButton(
        text='Отмена', callback_data='neurocomm:cancel',
        style='danger', icon_custom_emoji_id=get_icon('CROSS'),
    ))
    return builder.as_markup()


def get_neurocomment_mode_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text='Только ИИ', callback_data='neurocomm:mode:ai',
        style='primary', icon_custom_emoji_id=get_icon('AI'),
    ))
    builder.row(InlineKeyboardButton(
        text='Заготовленные сообщения', callback_data='neurocomm:mode:templates',
        style='default', icon_custom_emoji_id=get_icon('CLIPBOARD'),
    ))
    builder.row(InlineKeyboardButton(
        text='Отмена', callback_data='neurocomm:cancel',
        style='danger', icon_custom_emoji_id=get_icon('CROSS'),
    ))
    return builder.as_markup()


def get_neurocomment_model_keyboard(
    models: List[str], current: str,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for index, model in enumerate(models):
        label = LLM_MODELS.get(model, model)
        builder.row(InlineKeyboardButton(
            text=f"{'✓ ' if model == current else ''}{str(label)[:52]}",
            callback_data=f'neurocomm:model:{index}',
            style='success' if model == current else 'default',
            icon_custom_emoji_id=get_icon('CHECK') if model == current else get_icon('AI'),
        ))
    builder.row(InlineKeyboardButton(
        text='Отмена', callback_data='neurocomm:cancel',
        style='danger', icon_custom_emoji_id=get_icon('CROSS'),
    ))
    return builder.as_markup()


def get_neurocomment_templates_keyboard(count: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text=f'Готово ({count})', callback_data='neurocomm:templates_done',
        style='success', icon_custom_emoji_id=get_icon('CHECK'),
    ))
    builder.row(InlineKeyboardButton(
        text='Отмена', callback_data='neurocomm:cancel',
        style='danger', icon_custom_emoji_id=get_icon('CROSS'),
    ))
    return builder.as_markup()


def get_neurocomment_preview_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text='Запустить нейрокомментинг', callback_data='neurocomm:start_new',
        style='success', icon_custom_emoji_id=get_icon('PLAY'),
    ))
    builder.row(InlineKeyboardButton(
        text='Отмена', callback_data='neurocomm:cancel',
        style='danger', icon_custom_emoji_id=get_icon('CROSS'),
    ))
    return builder.as_markup()


def get_neurocomment_config_keyboard(config: Dict[str, Any]) -> InlineKeyboardMarkup:
    config_id = int(config['id'])
    builder = InlineKeyboardBuilder()
    if config.get('is_active'):
        builder.row(InlineKeyboardButton(
            text='Остановить', callback_data=f'neurocomm:stop:{config_id}',
            style='danger', icon_custom_emoji_id=get_icon('STOP'),
        ))
    else:
        builder.row(InlineKeyboardButton(
            text='Запустить', callback_data=f'neurocomm:start:{config_id}',
            style='success', icon_custom_emoji_id=get_icon('PLAY'),
        ))
    builder.row(InlineKeyboardButton(
        text='Удалить конфигурацию', callback_data=f'neurocomm:delete:{config_id}',
        style='danger', icon_custom_emoji_id=get_icon('DELETE'),
    ))
    builder.row(InlineKeyboardButton(
        text='К списку', callback_data='neurocomment',
        style='default', icon_custom_emoji_id=get_icon('BACK'),
    ))
    return builder.as_markup()


def format_neurocomment_config(config: Dict[str, Any]) -> str:
    mode = config.get('mode')
    mode_label = 'Только ИИ' if mode == NEUROCOMMENT_MODE_AI else 'Заготовленные сообщения'
    status = 'Запущен' if config.get('is_active') else 'Остановлен'
    last_error = (config.get('last_error') or '').strip()
    error_block = (
        f"\n\n{emoji('CROSS')} Последняя ошибка:\n<code>{escape(last_error[:700])}</code>"
        if last_error else ''
    )
    templates = len(config.get('message_variants') or [])
    model = config.get('model') or ''
    model_label = LLM_MODELS.get(model, model) if model else 'По умолчанию'
    model_line = (
        f"{emoji('AI')} Модель: <b>{escape(str(model_label))}</b>\n"
        if mode == NEUROCOMMENT_MODE_AI else ''
    )
    return (
        f"{emoji('AI')} <b>Нейрокомментинг #{config['id']}</b>\n\n"
        f"{emoji('PHONE')} Аккаунт: <code>{escape(str(config.get('phone') or config.get('account_id')))}</code>\n"
        f"{emoji('GLOBE')} Каналов: <b>{len(config.get('channel_ids') or [])}</b>\n"
        f"{emoji('AI')} Режим: <b>{mode_label}</b>\n"
        f"{model_line}"
        f"{emoji('CLIPBOARD')} Шаблонов: <b>{templates}</b>\n"
        f"{emoji('CLOCK')} Задержка после поста: <b>{config.get('delay_seconds', 0)} сек.</b>\n"
        f"{emoji('CHECK')} Отправлено комментариев: <b>{config.get('comments_sent', 0)}</b>\n"
        f"{emoji('CROSS')} Ошибок: <b>{config.get('errors_count', 0)}</b>\n"
        f"{emoji('EYE')} Статус: <b>{status}</b>"
        f"{error_block}"
    )

# --- Хендлеры команд ---
@dp.message(Command("start"))
async def cmd_start(message: Message):
    user = message.from_user
    # Регистрация пользователя — критична. Если упадёт, всё равно
    # пробуем показать меню, но логируем реальную причину.
    try:
        await register_user(user.id, user.username, user.first_name)
    except Exception as ex:
        logger.error(f"register_user failed for {user.id}: {ex}")
    # Блок лимитов — косметический. Его сбой НЕ должен мешать
    # новому пользователю получить приветствие и меню.
    try:
        limits = await format_limits_text(user.id)
    except Exception as ex:
        logger.error(f"format_limits_text failed for {user.id}: {ex}")
        limits = ""
    welcome_text = (
        f"{emoji('SMILE')} <b>Добро пожаловать в Vest Game Soft!</b>\n\n"
        f"{emoji('BOT')} Я помогу вам управлять аккаунтами и делать рассылки.\n\n"
        f"{emoji('PEOPLE')} <b>Менеджер аккаунтов</b> — добавление и управление\n"
        f"{emoji('APPS')} <b>Функции</b> — рассылка, автоответчик, парсинг\n"
        f"{emoji('SUPPORT')} <b>Поддержка:</b> {SUPPORT_USERNAME}\n\n"
        f"{limits}\n\n"
        f"Выберите действие:"
    )
    await present_section(message, 'welcome', welcome_text, get_main_menu_keyboard())

async def get_section_media(section: str) -> Optional[Dict[str, Any]]:
    if section not in MEDIA_SECTIONS or db_pool is None:
        return None
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM section_media WHERE section = $1', section)
    return dict(row) if row else None


async def send_section_media(message: Message, section: str) -> None:
    """Отправляет закреплённое админом медиа, если оно настроено."""
    media = await get_section_media(section)
    if not media:
        return
    caption = media.get('caption') or None
    try:
        if media['media_type'] == 'photo':
            await message.answer_photo(media['file_id'], caption=caption)
        elif media['media_type'] == 'video':
            await message.answer_video(media['file_id'], caption=caption)
        else:
            await message.answer_document(media['file_id'], caption=caption)
    except Exception as ex:
        logger.warning('section media send failed (%s): %s', section, ex)


async def present_section(
    message: Message, section: str, text: str,
    reply_markup: InlineKeyboardMarkup, replace: bool = False,
) -> None:
    """Показывает экран одним сообщением: медиа используется как вложение,
    а текст становится caption и получает ту же inline-клавиатуру."""
    media = await get_section_media(section)
    if not media:
        if replace:
            await message.edit_text(text, reply_markup=reply_markup)
        else:
            await message.answer(text, reply_markup=reply_markup)
        return
    if replace:
        try:
            await message.delete()
        except Exception:
            pass
    caption = text[:1024]
    try:
        if media['media_type'] == 'photo':
            await message.answer_photo(media['file_id'], caption=caption, reply_markup=reply_markup)
        elif media['media_type'] == 'video':
            await message.answer_video(media['file_id'], caption=caption, reply_markup=reply_markup)
        else:
            await message.answer_document(media['file_id'], caption=caption, reply_markup=reply_markup)
    except Exception as ex:
        logger.warning('section media render failed (%s): %s', section, ex)
        await message.answer(text, reply_markup=reply_markup)


def get_admin_media_keyboard(media_map: Dict[str, Any]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for key, label in MEDIA_SECTIONS.items():
        status = 'Настроено' if media_map.get(key) else 'Не задано'
        builder.row(InlineKeyboardButton(
            text=f'{label}: {status}', callback_data=f'admin_media_set:{key}',
            style='primary' if media_map.get(key) else 'default',
            icon_custom_emoji_id=get_icon('MEDIA')
        ))
        if media_map.get(key):
            builder.row(InlineKeyboardButton(
                text=f'Удалить медиа: {label}', callback_data=f'admin_media_delete:{key}',
                style='destructive', icon_custom_emoji_id=get_icon('DELETE')
            ))
    builder.row(InlineKeyboardButton(
        text='Назад', callback_data='admin_refresh_stats', style='default',
        icon_custom_emoji_id=get_icon('BACK')
    ))
    return builder.as_markup()


@dp.callback_query(F.data == 'admin_media')
async def admin_media_menu(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer('Нет доступа', show_alert=True)
        return
    await state.clear()
    media_map = {key: await get_section_media(key) for key in MEDIA_SECTIONS}
    await callback.message.edit_text(
        f"{emoji('MEDIA')} <b>Медиа разделов</b>\n\n"
        "Выберите раздел, чтобы загрузить фото, видео или документ.",
        reply_markup=get_admin_media_keyboard(media_map)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith('admin_media_set:'))
async def admin_media_set_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer('Нет доступа', show_alert=True)
        return
    section = callback.data.split(':', 1)[1]
    if section not in MEDIA_SECTIONS:
        await callback.answer('Раздел не найден', show_alert=True)
        return
    await state.update_data(media_section=section)
    await state.set_state(AdminStates.waiting_for_media)
    await callback.message.edit_text(
        f"{emoji('MEDIA')} <b>{MEDIA_SECTIONS[section]}</b>\n\n"
        "Отправьте фото, видео или документ. Подпись к сообщению сохранится.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text='Отмена', callback_data='admin_media', style='default',
                                 icon_custom_emoji_id=get_icon('BACK'))
        ]])
    )
    await callback.answer()


@dp.message(AdminStates.waiting_for_media)
async def admin_media_receive(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    data = await state.get_data()
    section = data.get('media_section')
    file_id = media_type = None
    if message.photo:
        file_id, media_type = message.photo[-1].file_id, 'photo'
    elif message.video:
        file_id, media_type = message.video.file_id, 'video'
    elif message.document:
        file_id, media_type = message.document.file_id, 'document'
    if not section or not file_id:
        await message.answer('Отправьте фото, видео или документ.')
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO section_media (section, file_id, media_type, caption)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (section) DO UPDATE SET file_id = EXCLUDED.file_id,
                 media_type = EXCLUDED.media_type, caption = EXCLUDED.caption,
                 updated_at = NOW()''',
            section, file_id, media_type, message.caption or ''
        )
    await state.clear()
    await message.answer(f"{emoji('CHECK')} Медиа для раздела «{MEDIA_SECTIONS[section]}» сохранено.",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                             InlineKeyboardButton(text='К медиа разделов', callback_data='admin_media',
                                                  style='primary', icon_custom_emoji_id=get_icon('MEDIA'))
                         ]]))


@dp.callback_query(F.data.startswith('admin_media_delete:'))
async def admin_media_delete(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer('Нет доступа', show_alert=True)
        return
    section = callback.data.split(':', 1)[1]
    async with db_pool.acquire() as conn:
        await conn.execute('DELETE FROM section_media WHERE section = $1', section)
    await callback.answer('Медиа удалено')
    media_map = {key: await get_section_media(key) for key in MEDIA_SECTIONS}
    await callback.message.edit_text(
        f"{emoji('MEDIA')} <b>Медиа разделов</b>\n\n"
        "Выберите раздел, чтобы загрузить фото, видео или документ.",
        reply_markup=get_admin_media_keyboard(media_map)
    )


async def render_admin_llm_menu() -> Tuple[str, InlineKeyboardMarkup]:
    runtime = await get_global_llm_runtime()
    apis = await get_admin_llm_apis()
    active_id = runtime.get('api_id')
    source_label = (
        f"<b>{escape(str(runtime.get('name') or 'Базовый API'))}</b>"
        if active_id is not None else '<b>Встроенный API из кода</b>'
    )
    models_text = '\n'.join(
        f"• <code>{escape(str(model_id))}</code> → {escape(str(label))}"
        for model_id, label in (runtime.get('models') or {}).items()
    ) or '• моделей пока нет'
    text = (
        f"{emoji('AI')} <b>Базовый AI API</b>\n\n"
        f"Активный источник: {source_label}\n"
        f"URL: <code>{escape(str(runtime.get('base_url') or '—'))}</code>\n\n"
        "<b>Модели, доступные пользователям:</b>\n"
        f"{models_text}\n\n"
        "Токены не отображаются. У личных API пользователей приоритет выше "
        "базового API."
    )
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text='Добавить базовый API',
        callback_data='admin_llm_add',
        style='primary',
        icon_custom_emoji_id=get_icon('ADD_TEXT'),
    ))
    builder.row(InlineKeyboardButton(
        text='Проверить модели API из кода',
        callback_data='admin_llm_test_builtin',
        style='default',
        icon_custom_emoji_id=get_icon('REFRESH'),
    ))
    for api in apis:
        mark = 'Используется' if api.get('is_active') else 'Сохранён'
        builder.row(InlineKeyboardButton(
            text=f"{str(api['name'])[:48]} · {mark}",
            callback_data=f"admin_llm_api:{api['id']}",
            style='success' if api.get('is_active') else 'default',
            icon_custom_emoji_id=get_icon('AI'),
        ))
    if active_id is not None:
        builder.row(InlineKeyboardButton(
            text='Вернуться к API из кода',
            callback_data='admin_llm_builtin',
            style='default',
            icon_custom_emoji_id=get_icon('BACK'),
        ))
    builder.row(InlineKeyboardButton(
        text='В админ-панель',
        callback_data='admin_refresh_stats',
        style='default',
        icon_custom_emoji_id=get_icon('BACK'),
    ))
    return text, builder.as_markup()


async def render_admin_llm_api_card(api_id: int) -> Tuple[Optional[str], Optional[InlineKeyboardMarkup]]:
    api = await get_admin_llm_api(api_id)
    if not api:
        return None, None
    models = await get_admin_llm_models(api_id)
    models_lines = [
        f"• <code>{escape(str(item['api_model_name']))}</code> → "
        f"<b>{escape(str(item['display_name']))}</b>"
        f"{' <i>(по умолчанию)</i>' if index == 0 else ''}"
        for index, item in enumerate(models)
    ] or ['• Пока нет моделей']
    status = 'используется как базовый' if api.get('is_active') else 'не активен'
    text = (
        f"{emoji('AI')} <b>{escape(str(api['name']))}</b>\n\n"
        f"Статус: <b>{status}</b>\n"
        f"URL: <code>{escape(str(api['base_url']))}</code>\n\n"
        "<b>Модели:</b>\n" + '\n'.join(models_lines) + "\n\n"
        "Сначала указывается техническое имя модели для API, затем — "
        "подпись, которую увидят пользователи на кнопке."
    )
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text='Добавить модель',
        callback_data=f'admin_llm_model_add:{api_id}',
        style='primary',
        icon_custom_emoji_id=get_icon('ADD_TEXT'),
    ))
    if not api.get('is_active'):
        builder.row(InlineKeyboardButton(
            text='Сделать базовым',
            callback_data=f'admin_llm_activate:{api_id}',
            style='success',
            icon_custom_emoji_id=get_icon('CHECK'),
        ))
    if models:
        builder.row(InlineKeyboardButton(
            text='Проверить все модели',
            callback_data=f'admin_llm_test_all:{api_id}',
            style='primary',
            icon_custom_emoji_id=get_icon('REFRESH'),
        ))
        for item in models:
            builder.row(InlineKeyboardButton(
                text=f"Тест: {str(item['display_name'])[:48]}",
                callback_data=f"admin_llm_test_model:{item['id']}",
                style='default',
                icon_custom_emoji_id=get_icon('AI'),
            ))
    for item in models:
        builder.row(InlineKeyboardButton(
            text=f"Удалить модель: {item['display_name']}",
            callback_data=f"admin_llm_model_delete:{item['id']}",
            style='danger',
            icon_custom_emoji_id=get_icon('DELETE'),
        ))
    builder.row(InlineKeyboardButton(
        text='Удалить API',
        callback_data=f'admin_llm_api_delete:{api_id}',
        style='danger',
        icon_custom_emoji_id=get_icon('DELETE'),
    ))
    builder.row(InlineKeyboardButton(
        text='К списку API',
        callback_data='admin_llm_menu',
        style='default',
        icon_custom_emoji_id=get_icon('BACK'),
    ))
    return text, builder.as_markup()


def _is_admin(callback_or_message) -> bool:
    user = getattr(callback_or_message, 'from_user', None)
    return bool(user and user.id in ADMIN_IDS)


def _valid_admin_llm_base_url(value: str) -> bool:
    parsed = urlparse(value)
    return bool(
        parsed.scheme in ('http', 'https')
        and parsed.netloc
        and '@' not in parsed.netloc
    )


async def build_admin_panel() -> tuple:
    """Строит текст и клавиатуру главной админ-панели (единый источник)."""
    stats = await get_broadcast_stats()
    ext = await get_admin_extended_stats()

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Рассылка всем пользователям",
        callback_data="admin_broadcast_all",
        style='primary',
        icon_custom_emoji_id=get_icon("MEGAPHONE")
    ))
    builder.row(InlineKeyboardButton(
        text="Пользователи",
        callback_data="admin_users:0",
        style='default',
        icon_custom_emoji_id=get_icon("PEOPLE")
    ))
    builder.row(InlineKeyboardButton(
        text="Финансы и платежи",
        callback_data="admin_finance",
        style='primary',
        icon_custom_emoji_id=get_icon("CHART")
    ))
    builder.row(InlineKeyboardButton(
        text="Базовый AI API",
        callback_data="admin_llm_menu",
        style='primary',
        icon_custom_emoji_id=get_icon("AI")
    ))
    builder.row(InlineKeyboardButton(
        text="Подарить подписку",
        callback_data="admin_gift_sub",
        style='default',
        icon_custom_emoji_id=get_icon("STAR")
    ))
    builder.row(InlineKeyboardButton(
        text="Забрать подписку",
        callback_data="admin_revoke_sub",
        style='destructive',
        icon_custom_emoji_id=get_icon("LOCK_CLOSED")
    ))
    builder.row(InlineKeyboardButton(
        text="Медиа разделов",
        callback_data="admin_media",
        style='default',
        icon_custom_emoji_id=get_icon("MEDIA")
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
        f"{emoji('PLAY')} Активных рассылок: <b>{stats['active_broadcasts']}</b>\n\n"
        f"<b>Подписки</b>\n"
        f"Pro: <b>{ext['pro_count']}</b>\n"
        f"Free: <b>{ext['free_count']}</b>\n"
        f"Истекают за 7 дней: <b>{ext['expiring_soon']}</b>\n"
        f"Новых за 24ч: <b>{ext['new_today']}</b>"
    )
    return admin_text, builder.as_markup()


@dp.message(Command("admin"), StateFilter("*"))
async def cmd_admin(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        logger.info(f"/admin denied for user_id={message.from_user.id}")
        return

    # /admin должен работать всегда, даже если админ "застрял" в каком-то
    # FSM-состоянии (рассылка, подарок подписки и т.п.).
    if await state.get_state() is not None:
        await state.clear()

    try:
        admin_text, markup = await build_admin_panel()
        await message.answer(admin_text, reply_markup=markup)
    except Exception as ex:
        logger.exception(f"/admin failed: {ex}")
        await message.answer(
            f"{emoji('CROSS')} <b>Не удалось открыть админ-панель</b>\n\n"
            f"<code>{escape(str(ex))}</code>"
        )

# --- Главное меню ---
@dp.callback_query(F.data == "main_menu")
async def back_to_main(callback: CallbackQuery):
    limits = await format_limits_text(callback.from_user.id)
    text = (
        f"{emoji('SMILE')} <b>Главное меню</b>\n\n"
        f"{limits}\n\n"
        f"Выберите действие:"
    )
    await present_section(callback.message, 'welcome', text, get_main_menu_keyboard(), replace=True)
    await callback.answer()

@dp.callback_query(F.data == "account_manager")
async def account_manager(callback: CallbackQuery):
    text = f"{emoji('PEOPLE')} <b>Менеджер аккаунтов</b>\n\nВыберите действие:"
    await present_section(callback.message, 'account_manager', text,
                          get_account_manager_keyboard(), replace=True)
    await callback.answer()

@dp.callback_query(F.data == "functions")
async def functions(callback: CallbackQuery):
    text = f"{emoji('APPS')} <b>Функции</b>\n\nВыберите функцию:"
    await present_section(callback.message, 'functions', text,
                          get_functions_keyboard(), replace=True)
    await callback.answer()

# --- Скрипты: открыть бота, загрузить меню и нажать кнопку ---
@dp.callback_query(F.data == "scripts")
async def scripts_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    scripts = await get_user_scripts(callback.from_user.id)
    text = (
        f"{emoji('PLAY')} <b>Скрипты</b>\n\n"
        "Скрипт строится как маршрут из нескольких кнопок. После каждого "
        "перехода загружаются кнопки именно нового раздела бота, а не главное меню.\n\n"
        "Поддерживаются переходы по кнопкам, подписка по кнопке-ссылке на канал "
        "и опциональная фото-капча через базовую AI-модель.\n\n"
        f"{emoji('INFO')} Сохранено: <b>{len(scripts)}</b>"
    )
    await callback.message.edit_text(
        text, reply_markup=get_scripts_keyboard(scripts)
    )
    await callback.answer()


@dp.callback_query(F.data == 'script:public')
async def script_public_list(callback: CallbackQuery):
    scripts = await get_public_scripts(limit=20)
    if not scripts:
        await callback.message.edit_text(
            f"{emoji('INFO')} <b>Публичных скриптов пока нет.</b>\n\n"
            "Опубликуйте свой скрипт из его карточки — он появится здесь.",
            reply_markup=get_public_scripts_keyboard([]),
        )
        await callback.answer()
        return
    await callback.message.edit_text(
        f"{emoji('GLOBE')} <b>Публичные скрипты</b>\n\n"
        "Выберите маршрут, чтобы посмотреть детали и применить его к своему аккаунту.",
        reply_markup=get_public_scripts_keyboard(scripts),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith('script:public_view:'))
async def script_public_view(callback: CallbackQuery):
    try:
        script_id = int(callback.data.rsplit(':', 1)[1])
    except ValueError:
        await callback.answer('Некорректный скрипт', show_alert=True)
        return
    script = await get_public_script(script_id)
    if not script:
        await callback.answer('Скрипт снят с публикации или не найден', show_alert=True)
        return
    await callback.message.edit_text(
        format_public_script_card(script),
        reply_markup=get_public_script_actions_keyboard(script_id),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith('script:publish:'))
async def script_publish(callback: CallbackQuery):
    try:
        script_id = int(callback.data.rsplit(':', 1)[1])
    except ValueError:
        await callback.answer('Некорректный скрипт', show_alert=True)
        return
    if not await set_script_public(script_id, callback.from_user.id, True):
        await callback.answer('Скрипт не найден', show_alert=True)
        return
    script = await get_user_script(script_id, callback.from_user.id)
    await callback.message.edit_text(
        f"{emoji('GLOBE')} <b>Скрипт опубликован.</b>\n\n" + format_script_card(script),
        reply_markup=get_script_actions_keyboard(
            script_id, script.get('last_status') == 'running', True,
        ),
    )
    await callback.answer('Опубликовано')


@dp.callback_query(F.data.startswith('script:unpublish:'))
async def script_unpublish(callback: CallbackQuery):
    try:
        script_id = int(callback.data.rsplit(':', 1)[1])
    except ValueError:
        await callback.answer('Некорректный скрипт', show_alert=True)
        return
    if not await set_script_public(script_id, callback.from_user.id, False):
        await callback.answer('Скрипт не найден', show_alert=True)
        return
    script = await get_user_script(script_id, callback.from_user.id)
    await callback.message.edit_text(
        f"{emoji('CHECK')} Скрипт снят с публикации.\n\n" + format_script_card(script),
        reply_markup=get_script_actions_keyboard(
            script_id, script.get('last_status') == 'running', False,
        ),
    )
    await callback.answer('Снято с публикации')


@dp.callback_query(F.data.startswith('script:apply:'))
async def script_apply_public(callback: CallbackQuery):
    try:
        public_id = int(callback.data.rsplit(':', 1)[1])
    except ValueError:
        await callback.answer('Некорректный скрипт', show_alert=True)
        return
    script = await get_public_script(public_id)
    if not script:
        await callback.answer('Скрипт снят с публикации или не найден', show_alert=True)
        return
    accounts = [
        item for item in await get_user_accounts(callback.from_user.id)
        if item.get('is_active')
    ]
    if not accounts:
        await callback.answer('Сначала добавьте активный Telegram-аккаунт', show_alert=True)
        return
    await callback.message.edit_text(
        f"{emoji('PROFILE')} <b>Выберите свой аккаунт для применения скрипта</b>\n\n"
        "Сессия, номер и другие данные автора не копируются.",
        reply_markup=get_public_script_account_keyboard(public_id, accounts),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith('script:apply_account:'))
async def script_apply_public_account(callback: CallbackQuery):
    parts = callback.data.split(':')
    try:
        public_id = int(parts[2])
        account_id = int(parts[3])
    except (IndexError, ValueError):
        await callback.answer('Некорректный выбор', show_alert=True)
        return
    account = await get_account(account_id)
    if not account or account.get('user_id') != callback.from_user.id or not account.get('is_active'):
        await callback.answer('Аккаунт не найден или неактивен', show_alert=True)
        return
    try:
        script_id = await apply_public_script(public_id, callback.from_user.id, account_id)
    except Exception as ex:
        await callback.answer(f'Не удалось применить: {str(ex)[:200]}', show_alert=True)
        return
    script = await get_user_script(script_id, callback.from_user.id)
    await callback.message.edit_text(
        f"{emoji('CHECK')} <b>Скрипт применён к вашему аккаунту.</b>\n\n" + format_script_card(script),
        reply_markup=get_script_actions_keyboard(script_id, False, False),
    )
    await callback.answer('Готово')


@dp.callback_query(F.data == "script:create")
async def script_create_start(callback: CallbackQuery, state: FSMContext):
    accounts = [
        account for account in await get_user_accounts(callback.from_user.id)
        if account.get('is_active')
    ]
    if not accounts:
        await callback.answer(
            'Сначала добавьте активный Telegram-аккаунт',
            show_alert=True,
        )
        return
    await state.clear()
    await state.set_state(ScriptStates.waiting_for_name)
    await callback.message.edit_text(
        f"{emoji('WRITE')} <b>Название скрипта</b>\n\n"
        "Напишите короткое понятное название, например:\n"
        "<i>Ежедневный бонус</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Отмена",
                callback_data="script:cancel",
                style='danger',
                icon_custom_emoji_id=get_icon("CROSS"),
            )
        ]]),
    )
    await callback.answer()


@dp.message(ScriptStates.waiting_for_name)
async def script_process_name(message: Message, state: FSMContext):
    name = (message.text or '').strip()
    if not 2 <= len(name) <= 64:
        await message.answer(
            f"{emoji('CROSS')} Название должно содержать от 2 до 64 символов."
        )
        return
    accounts = [
        account for account in await get_user_accounts(message.from_user.id)
        if account.get('is_active')
    ]
    if not accounts:
        await state.clear()
        await message.answer(
            f"{emoji('CROSS')} Нет активных аккаунтов.",
            reply_markup=get_functions_keyboard(),
        )
        return
    await state.update_data(script_name=name, script_mode='create')
    await state.set_state(ScriptStates.choosing_account)
    await message.answer(
        f"{emoji('PHONE')} <b>Выберите аккаунт</b>\n\n"
        "Этот аккаунт будет открывать бота и нажимать кнопку.",
        reply_markup=get_script_accounts_keyboard(accounts),
    )


@dp.callback_query(
    F.data.startswith("script:account:"),
    ScriptStates.choosing_account,
)
async def script_select_account(
    callback: CallbackQuery, state: FSMContext
):
    try:
        account_id = int(callback.data.rsplit(':', 1)[1])
    except ValueError:
        await callback.answer('Некорректный аккаунт', show_alert=True)
        return
    account = await get_account(account_id)
    if (
        not account
        or account['user_id'] != callback.from_user.id
        or not account.get('is_active')
    ):
        await callback.answer('Аккаунт не найден', show_alert=True)
        return
    await state.update_data(script_account_id=account_id)
    await state.set_state(ScriptStates.waiting_for_bot_url)
    await callback.message.edit_text(
        f"{emoji('LINK')} <b>Отправьте ссылку на Telegram-бота</b>\n\n"
        "Поддерживаются форматы:\n"
        "• <code>https://t.me/example_bot</code>\n"
        "• <code>https://t.me/example_bot?start=code</code>\n"
        "• <code>@example_bot</code>\n\n"
        "После этого аккаунт отправит боту <code>/start</code> "
        "и загрузит доступные кнопки.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Отмена",
                callback_data="script:cancel",
                style='danger',
                icon_custom_emoji_id=get_icon("CROSS"),
            )
        ]]),
    )
    await callback.answer()


@dp.message(ScriptStates.waiting_for_bot_url)
async def script_process_bot_url(message: Message, state: FSMContext):
    bot_url = (message.text or '').strip()
    try:
        parsed = parse_telegram_bot_url(bot_url)
    except ValueError as ex:
        await message.answer(
            f"{emoji('CROSS')} {escape(str(ex))}\n\n"
            "Отправьте другую ссылку или нажмите «Отмена».",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Отмена", callback_data="script:cancel", style='danger')
            ]]),
        )
        return
    data = await state.get_data()
    if not data.get('script_account_id'):
        await state.clear()
        await message.answer(f"{emoji('CROSS')} Аккаунт не выбран. Начните создание заново.")
        return
    await state.update_data(
        script_bot_url=parsed['bot_url'],
        script_bot_username=parsed['bot_username'],
        script_start_payload=parsed['start_payload'],
    )
    await state.set_state(ScriptStates.choosing_captcha)
    await message.answer(
        f"{emoji('MEDIA')} <b>Фото-капча</b>\n\n"
        "Есть ли у этого бота капча с изображением? Если да, при запуске "
        "скрипта изображение, текст сообщения и кнопки будут переданы "
        "базовой AI-модели администратора для ответа.",
        reply_markup=get_script_captcha_keyboard(),
    )


async def _load_script_route_menu(
    account_id: int, bot_url: str, captcha_enabled: bool,
) -> Tuple[Dict[str, Any], int]:
    menu = await load_script_bot_menu(account_id, bot_url)
    solved = 0
    if captcha_enabled and menu.get('has_photo'):
        client = await get_client_for_account(account_id)
        if not client:
            raise RuntimeError('Не удалось подключиться к аккаунту')
        menu, solved = await _resolve_script_captcha_chain(
            client, menu, require_buttons=True,
        )
    return menu, solved


async def _render_script_route_menu(
    target: Message, state: FSMContext, menu: Dict[str, Any],
) -> None:
    buttons = menu.get('buttons') or []
    if not any(item.get('action') for item in buttons):
        if menu.get('has_photo'):
            raise RuntimeError(
                'Бот прислал фото с капчей. Включите опцию «Фото-капча» при создании скрипта'
            )
        raise RuntimeError('В текущем сообщении бота нет доступных кнопок')
    data = await state.get_data()
    steps = list(data.get('script_steps') or [])
    route = ' → '.join(escape(str(item.get('text') or '—')) for item in steps) or 'пока пуст'
    if len(route) > 500:
        route = route[:499] + '…'
    await state.update_data(
        script_current_message_id=int(menu['message_id']),
        script_buttons=buttons,
        script_current_message_text=menu.get('message_text') or '',
        script_current_has_photo=bool(menu.get('has_photo')),
    )
    await state.set_state(ScriptStates.choosing_button)
    preview = escape((menu.get('message_text') or '')[:600])
    photo_note = f"\n{emoji('MEDIA')} В сообщении есть фото." if menu.get('has_photo') else ''
    await target.edit_text(
        f"{emoji('CHECK')} <b>Текущий экран бота</b>\n\n"
        f"Маршрут: <b>{len(steps)} шаг.</b> · <i>{route}</i>\n"
        f"{emoji('CHAT')} Сообщение: <i>{preview or '—'}</i>"
        f"{photo_note}\n\n"
        "Выберите следующую кнопку. Для обычной кнопки можно указать, "
        "является ли она переходом к следующему экрану или финальным шагом.",
        reply_markup=get_script_buttons_keyboard(buttons, len(steps)),
    )


async def _get_current_route_menu(state: FSMContext) -> Tuple[TelegramClient, Dict[str, Any]]:
    data = await state.get_data()
    account_id = int(data.get('script_account_id') or 0)
    if not account_id:
        raise RuntimeError('Не выбран аккаунт')
    client = await get_client_for_account(account_id)
    if not client:
        raise RuntimeError('Не удалось подключиться к аккаунту')
    bot_username = str(data.get('script_bot_username') or '')
    entity = await _get_script_bot_entity(client, bot_username)
    message = await _get_current_script_message(
        client, entity, int(data.get('script_current_message_id') or 0),
    )
    parsed = {
        'bot_url': str(data.get('script_bot_url') or ''),
        'bot_username': bot_username,
        'start_payload': str(data.get('script_start_payload') or ''),
    }
    return client, _script_menu_from_message(parsed, entity, message)


@dp.callback_query(F.data.startswith('script:captcha:'), ScriptStates.choosing_captcha)
async def script_captcha_choice(callback: CallbackQuery, state: FSMContext):
    choice = callback.data.rsplit(':', 1)[1]
    if choice not in {'yes', 'no'}:
        await callback.answer('Некорректный выбор', show_alert=True)
        return
    await state.update_data(captcha_enabled=(choice == 'yes'), script_steps=[])
    data = await state.get_data()
    await callback.answer('Загружаю меню…')
    await callback.message.edit_text(
        f"{emoji('LOADING')} <b>Открываю бота и загружаю маршрут…</b>"
    )
    try:
        menu, solved = await _load_script_route_menu(
            int(data['script_account_id']), data['script_bot_url'], choice == 'yes',
        )
        await _render_script_route_menu(callback.message, state, menu)
        if solved:
            await callback.message.answer(
                f"{emoji('CHECK')} Фото-капча пройдена базовой AI-моделью."
            )
    except Exception as ex:
        logger.exception('Script route menu loading failed')
        await callback.message.edit_text(
            f"{emoji('CROSS')} <b>Не удалось открыть маршрут.</b>\n\n"
            f"<code>{escape(str(ex)[:700])}</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text='Отмена', callback_data='script:cancel', style='danger')
            ]]),
        )


@dp.callback_query(F.data == 'script:unsupported')
async def script_unsupported_button(callback: CallbackQuery):
    await callback.answer(
        'Эта URL/WebApp-кнопка не ведёт на канал и не может быть шагом серверного скрипта',
        show_alert=True,
    )


@dp.callback_query(F.data.startswith('script:button:'), ScriptStates.choosing_button)
async def script_choose_button(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(':')
    try:
        row_index, col_index = int(parts[2]), int(parts[3])
    except (IndexError, ValueError):
        await callback.answer('Некорректная кнопка', show_alert=True)
        return
    data = await state.get_data()
    selected = next(
        (
            item for item in (data.get('script_buttons') or [])
            if int(item.get('row', -1)) == row_index and int(item.get('col', -1)) == col_index
        ),
        None,
    )
    if not selected or not selected.get('action'):
        await callback.answer('Эту кнопку нельзя добавить в маршрут', show_alert=True)
        return
    step = dict(selected)
    step['final'] = False
    if step['action'] == 'join_channel':
        try:
            client, menu = await _get_current_route_menu(state)
            await _script_join_channel_url(client, step.get('url') or '')
            steps = list(data.get('script_steps') or []) + [step]
            await state.update_data(script_steps=steps)
            await _render_script_route_menu(callback.message, state, menu)
            await callback.answer('Подписка выполнена; окружение бота сохранено')
        except Exception as ex:
            await callback.answer(f'Не удалось подписаться: {str(ex)[:200]}', show_alert=True)
        return

    await state.update_data(script_pending_step=step)
    await state.set_state(ScriptStates.confirming_step)
    await callback.message.edit_text(
        f"{emoji('KEY')} <b>Выбрана кнопка</b>: <b>{escape(str(step['text']))}</b>\n\n"
        "Если это переход (например «Профиль») — нажмите «Нажать и перейти дальше». "
        "Тогда загрузятся кнопки именно из нового раздела.\n\n"
        "Если действие должно завершать маршрут — выберите финальный шаг.",
        reply_markup=get_script_step_confirmation_keyboard(),
    )
    await callback.answer()


@dp.callback_query(F.data == 'script:step:intermediate', ScriptStates.confirming_step)
async def script_step_intermediate(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    step = data.get('script_pending_step') or {}
    if not step:
        await callback.answer('Кнопка не выбрана', show_alert=True)
        return
    await callback.answer('Нажимаю и загружаю следующий экран…')
    await callback.message.edit_text(f"{emoji('LOADING')} <b>Перехожу к следующему экрану…</b>")
    try:
        client, menu = await _get_current_route_menu(state)
        next_menu = await _click_script_step(client, menu, step, require_buttons=True)
        solved = 0
        if data.get('captcha_enabled') and next_menu.get('has_photo'):
            next_menu, solved = await _resolve_script_captcha_chain(
                client, next_menu, require_buttons=True,
            )
        steps = list(data.get('script_steps') or []) + [{**step, 'final': False}]
        await state.update_data(script_steps=steps, script_pending_step=None)
        await _render_script_route_menu(callback.message, state, next_menu)
        if solved:
            await callback.message.answer(f"{emoji('CHECK')} Фото-капча пройдена AI.")
    except Exception as ex:
        logger.exception('Script intermediate step failed')
        await callback.message.edit_text(
            f"{emoji('CROSS')} <b>Не удалось перейти дальше.</b>\n\n"
            f"<code>{escape(str(ex)[:700])}</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text='Отмена', callback_data='script:cancel', style='danger')
            ]]),
        )


@dp.callback_query(F.data == 'script:step:final', ScriptStates.confirming_step)
async def script_step_final(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    step = data.get('script_pending_step') or {}
    if not step:
        await callback.answer('Кнопка не выбрана', show_alert=True)
        return
    steps = list(data.get('script_steps') or []) + [{**step, 'final': True}]
    await state.update_data(script_steps=steps, script_pending_step=None)
    await state.set_state(ScriptStates.choosing_button)
    route = ' → '.join(escape(str(item.get('text') or '—')) for item in steps)
    await callback.message.edit_text(
        f"{emoji('CHECK')} Финальный шаг добавлен.\n\n"
        f"Маршрут: <i>{route}</i>\n\n"
        "Нажмите «Сохранить маршрут» или вернитесь к кнопкам, чтобы добавить ещё шаги.",
        reply_markup=get_script_buttons_keyboard(data.get('script_buttons') or [], len(steps)),
    )
    await callback.answer()


@dp.callback_query(F.data == 'script:step:back', ScriptStates.confirming_step)
async def script_step_back(callback: CallbackQuery, state: FSMContext):
    await state.set_state(ScriptStates.choosing_button)
    data = await state.get_data()
    try:
        _, menu = await _get_current_route_menu(state)
        await _render_script_route_menu(callback.message, state, menu)
    except Exception as ex:
        await callback.answer(str(ex)[:200], show_alert=True)
        return
    await callback.answer()


async def _save_script_route(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    steps = list(data.get('script_steps') or [])
    if not steps:
        await callback.answer('Добавьте хотя бы один шаг маршрута', show_alert=True)
        return
    try:
        if data.get('script_mode') == 'edit':
            script_id = int(data['script_id'])
            updated = await update_user_script_route(
                script_id, callback.from_user.id,
                data['script_bot_url'], data['script_bot_username'],
                data.get('script_start_payload') or '', steps,
                data.get('script_buttons') or [], bool(data.get('captcha_enabled')),
            )
            if not updated:
                raise ValueError('Скрипт не найден')
        else:
            script_id = await save_user_script(
                callback.from_user.id, int(data['script_account_id']), str(data['script_name']),
                data['script_bot_url'], data['script_bot_username'],
                data.get('script_start_payload') or '', steps,
                data.get('script_buttons') or [], bool(data.get('captcha_enabled')),
            )
    except Exception as ex:
        logger.exception('Script route save failed')
        await callback.answer(f'Ошибка сохранения: {str(ex)[:200]}', show_alert=True)
        return
    await state.clear()
    script = await get_user_script(script_id, callback.from_user.id)
    await callback.message.edit_text(
        f"{emoji('CHECK')} <b>Маршрут скрипта сохранён.</b>\n\n" + format_script_card(script),
        reply_markup=get_script_actions_keyboard(
            script_id, script.get('last_status') == 'running', bool(script.get('is_public'))
        ),
    )
    await callback.answer('Готово')


@dp.callback_query(F.data == 'script:route_done', ScriptStates.choosing_button)
async def script_route_done(callback: CallbackQuery, state: FSMContext):
    await _save_script_route(callback, state)

@dp.callback_query(F.data.startswith("script:view:"))
async def script_view(callback: CallbackQuery):
    try:
        script_id = int(callback.data.rsplit(':', 1)[1])
    except ValueError:
        await callback.answer('Некорректный скрипт', show_alert=True)
        return
    script = await get_user_script(script_id, callback.from_user.id)
    if not script:
        await callback.answer('Скрипт не найден', show_alert=True)
        return
    await callback.message.edit_text(
        format_script_card(script),
        reply_markup=get_script_actions_keyboard(
            script_id, script.get('last_status') == 'running', bool(script.get('is_public'))
        ),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("script:run:"))
async def script_run(callback: CallbackQuery):
    try:
        script_id = int(callback.data.rsplit(':', 1)[1])
    except ValueError:
        await callback.answer('Некорректный скрипт', show_alert=True)
        return
    script = await get_user_script(script_id, callback.from_user.id)
    if not script:
        await callback.answer('Скрипт не найден', show_alert=True)
        return
    ok, error = await start_script_runner(script_id, callback.from_user.id)
    if not ok:
        await callback.answer(error, show_alert=True)
        return
    script = await get_user_script(script_id, callback.from_user.id)
    await callback.message.edit_text(
        f"{emoji('PLAY')} <b>Скрипт запущен в бесконечном цикле.</b>\n\n"
        "Каждый цикл начинает маршрут заново, выполняет все шаги и ждёт 5 секунд. "
        "Остановите его кнопкой «Остановить скрипт».\n\n"
        + format_script_card(script),
        reply_markup=get_script_actions_keyboard(script_id, True, bool(script.get('is_public'))),
    )
    await callback.answer('Запущено')


@dp.callback_query(F.data.startswith("script:stop:"))
async def script_stop(callback: CallbackQuery):
    try:
        script_id = int(callback.data.rsplit(':', 1)[1])
    except ValueError:
        await callback.answer('Некорректный скрипт', show_alert=True)
        return
    if not await stop_script_runner(script_id, callback.from_user.id):
        await callback.answer('Скрипт не найден', show_alert=True)
        return
    script = await get_user_script(script_id, callback.from_user.id)
    await callback.message.edit_text(
        f"{emoji('STOP')} <b>Остановка скрипта запрошена.</b>\n\n"
        + format_script_card(script),
        reply_markup=get_script_actions_keyboard(script_id, False, bool(script.get('is_public'))),
    )
    await callback.answer('Остановлено')

@dp.callback_query(F.data.startswith("script:refresh:"))
async def script_refresh_buttons(
    callback: CallbackQuery, state: FSMContext
):
    try:
        script_id = int(callback.data.rsplit(':', 1)[1])
    except ValueError:
        await callback.answer('Некорректный скрипт', show_alert=True)
        return
    script = await get_user_script(script_id, callback.from_user.id)
    if not script:
        await callback.answer('Скрипт не найден', show_alert=True)
        return
    if script.get('last_status') == 'running':
        await callback.answer('Сначала остановите запущенный скрипт', show_alert=True)
        return
    await callback.answer('Обновляю маршрут…')
    await callback.message.edit_text(
        f"{emoji('LOADING')} <b>Открываю бота для обновления маршрута…</b>\n\n"
        f"Бот: <code>@{escape(script['bot_username'])}</code>"
    )
    await state.clear()
    await state.update_data(
        script_mode='edit',
        script_id=script_id,
        script_account_id=int(script['account_id']),
        script_bot_url=script['bot_url'],
        script_bot_username=script['bot_username'],
        script_start_payload=script.get('start_payload') or '',
        captcha_enabled=bool(script.get('captcha_enabled')),
        script_steps=[],
    )
    try:
        menu, solved = await _load_script_route_menu(
            int(script['account_id']), script['bot_url'], bool(script.get('captcha_enabled')),
        )
        await _render_script_route_menu(callback.message, state, menu)
        if solved:
            await callback.message.answer(f"{emoji('CHECK')} Фото-капча пройдена AI.")
    except Exception as ex:
        await callback.message.edit_text(
            f"{emoji('CROSS')} <b>Не удалось загрузить маршрут.</b>\n\n"
            f"<code>{escape(str(ex)[:700])}</code>",
            reply_markup=get_script_actions_keyboard(script_id, False, bool(script.get('is_public'))),
        )

@dp.callback_query(F.data.startswith("script:delete_ask:"))
async def script_delete_ask(callback: CallbackQuery):
    try:
        script_id = int(callback.data.rsplit(':', 1)[1])
    except ValueError:
        await callback.answer('Некорректный скрипт', show_alert=True)
        return
    script = await get_user_script(script_id, callback.from_user.id)
    if not script:
        await callback.answer('Скрипт не найден', show_alert=True)
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Да, удалить",
            callback_data=f"script:delete:{script_id}",
            style='danger',
            icon_custom_emoji_id=get_icon("DELETE"),
        )],
        [InlineKeyboardButton(
            text="Отмена",
            callback_data=f"script:view:{script_id}",
            style='default',
            icon_custom_emoji_id=get_icon("BACK"),
        )],
    ])
    await callback.message.edit_text(
        f"{emoji('CROSS')} <b>Удалить скрипт?</b>\n\n"
        f"<b>{escape(script['name'])}</b>\n"
        "История запусков этого скрипта также будет удалена.",
        reply_markup=keyboard,
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("script:delete:"))
async def script_delete_confirm(callback: CallbackQuery, state: FSMContext):
    try:
        script_id = int(callback.data.rsplit(':', 1)[1])
    except ValueError:
        await callback.answer('Некорректный скрипт', show_alert=True)
        return
    deleted = await delete_user_script(script_id, callback.from_user.id)
    if not deleted:
        await callback.answer('Скрипт не найден', show_alert=True)
        return
    await state.clear()
    scripts = await get_user_scripts(callback.from_user.id)
    await callback.message.edit_text(
        f"{emoji('CHECK')} <b>Скрипт удалён.</b>",
        reply_markup=get_scripts_keyboard(scripts),
    )
    await callback.answer('Удалено')


@dp.callback_query(F.data == "script:cancel")
async def script_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    scripts = await get_user_scripts(callback.from_user.id)
    await callback.message.edit_text(
        f"{emoji('PLAY')} <b>Скрипты</b>\n\n"
        f"{emoji('INFO')} Сохранено: <b>{len(scripts)}</b>",
        reply_markup=get_scripts_keyboard(scripts),
    )
    await callback.answer('Отменено')

# --- Помощь ---
@dp.callback_query(F.data == "help")
async def help_handler(callback: CallbackQuery):
    help_text = (
        f"{emoji('INFO')} <b>Помощь — Vest Game Soft</b>\n\n"
        f"{emoji('PEOPLE')} <b>Менеджер аккаунтов</b> — добавление Telegram-аккаунтов и прокси.\n\n"
        f"{emoji('SEND')} <b>Рассылка</b> — сообщения в выбранные чаты.\n"
        f"{emoji('CLOCK')} <b>Отложенная рассылка</b> — запуск по расписанию.\n"
        f"{emoji('CHAT')} <b>Рассылка в ЛС</b> — личные сообщения пользователям.\n"
        f"{emoji('BELL')} <b>Автоответчик</b> — авто-ответ на ключевые слова или AI.\n"
        f"{emoji('JOIN')} <b>Вступление в чаты</b> — массовое подключение.\n"
        f"{emoji('LIKE')} <b>Авто-лайкинг</b> — реакции на новые сообщения.\n"
        f"{emoji('AI')} <b>Нейрокомментинг</b> — комментарии к новым постам в выбранных каналах.\n"
        f"{emoji('SWEEP')} <b>Удаление сообщений</b> — очистка истории.\n"
        f"{emoji('USERS')} <b>Парсинг чата</b> — сбор пользователей.\n"
        f"{emoji('PLAY')} <b>Скрипты</b> — маршруты из нескольких кнопок и фото-капча.\n"
        f"{emoji('AI')} <b>AI Генератор</b> — 3 варианта текста на выбор.\n"
        f"{emoji('AI')} <b>Чат с нейросетями</b> — диалог с сохранением контекста.\n\n"
        f"{emoji('SUPPORT')} <b>Поддержка:</b> {SUPPORT_USERNAME}"
    )
    await callback.message.edit_text(
        help_text,
        reply_markup=get_help_keyboard()
    )
    await callback.answer()


# --- Подробные страницы помощи по каждой фиче ---

@dp.callback_query(F.data == "help_accounts")
async def help_accounts_handler(callback: CallbackQuery):
    text = (
        f"{emoji('PEOPLE')} <b>Менеджер аккаунтов</b>\n\n"
        f"<b>Что это:</b> добавление Telegram-аккаунтов (Telethon-сессии) и "
        f"прокси к ним. Все остальные функции работают через эти аккаунты.\n\n"
        f"<b>Как запустить:</b>\n"
        f"1. Главное меню → «Менеджер аккаунтов».\n"
        f"2. Нажмите «Добавить аккаунт».\n"
        f"3. Отправьте номер телефона в международном формате "
        f"(например <code>+79991234567</code>).\n"
        f"4. Введите код из Telegram.\n"
        f"5. Если включена 2FA — введите облачный пароль.\n"
        f"6. После входа можно привязать прокси: «Настройки прокси» → "
        f"формат <code>type://user:pass@host:port</code> "
        f"(socks5 / http / mtproxy).\n\n"
        f"<b>Советы:</b>\n"
        f"• На один аккаунт — один прокси (иначе Telegram забанит).\n"
        f"• Прогрев нового аккаунта включается автоматически.\n"
        f"• Сессия хранится в строке (StringSession) — её можно выгрузить."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "help_broadcast")
async def help_broadcast_handler(callback: CallbackQuery):
    text = (
        f"{emoji('SEND')} <b>Рассылка в чаты</b>\n\n"
        f"<b>Что это:</b> отправка вашего текста/медиа сразу в несколько "
        f"выбранных чатов через один из добавленных аккаунтов.\n\n"
        f"<b>Как запустить:</b>\n"
        f"1. Главное меню → «Рассылка».\n"
        f"2. Выберите режим:\n"
        f"   • <b>Одновременный</b> — сообщения уходят пачкой.\n"
        f"   • <b>Рандомный</b> — случайные задержки между чатами "
        f"(безопаснее, похоже на человека).\n"
        f"3. Выберите аккаунт-отправитель.\n"
        f"4. Выберите чаты галочками (можно несколько).\n"
        f"5. Укажите задержку между сообщениями (в секундах, "
        f"например <code>30-90</code>).\n"
        f"6. Укажите, сколько раз пройтись по списку чатов (или «до конца»).\n"
        f"7. Отправьте сообщение: текст, фото, видео, документ — любой тип.\n"
        f"8. Откроется превью → «Запустить».\n\n"
        f"<b>Советы:</b>\n"
        f"• Для новых аккаунтов ставьте задержку от 60 секунд.\n"
        f"• Не шлите одинаковый текст в десятки чатов — Telegram склеит их в "
        f"одну цепочку и пометит как спам.\n"
        f"• Добавляйте эмодзи и ссылки в разном порядке."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "help_dm_broadcast")
async def help_dm_broadcast_handler(callback: CallbackQuery):
    text = (
        f"{emoji('CHAT')} <b>Рассылка в ЛС (личные сообщения)</b>\n\n"
        f"<b>Что это:</b> массовая отправка сообщений в личку пользователям, "
        f"которых вы спарсили из чата.\n\n"
        f"<b>Как запустить:</b>\n"
        f"1. Сначала соберите базу: «Парсинг чата» → выгрузите <code>.txt</code>.\n"
        f"2. Главное меню → «Рассылка в ЛС».\n"
        f"3. Выберите аккаунт-отправитель.\n"
        f"4. Загрузите файл со списком пользователей "
        f"(<code>@username</code> или ссылки <code>t.me/...</code>).\n"
        f"5. Укажите задержку (например <code>45-120</code> секунд).\n"
        f"6. Отправьте текст сообщения — поддерживается разметка Telegram.\n"
        f"7. Превью → «Запустить рассылку».\n\n"
        f"<b>Советы:</b>\n"
        f"• Лимиты: на Free — небольшое число получателей в день, "
        f"на Pro — без ограничений.\n"
        f"• Прогревайте аккаунт хотя бы 1–2 дня до первой рассылки.\n"
        f"• Не отправляйте сообщения людям, которые вас не знают — это "
        f"самый частый путь к бану."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "help_scheduled")
async def help_scheduled_handler(callback: CallbackQuery):
    text = (
        f"{emoji('CLOCK')} <b>Отложенная рассылка</b>\n\n"
        f"<b>Что это:</b> та же рассылка в чаты, но с запуском по дате и "
        f"времени (МСК). Можно ставить несколько отложенных задач.\n\n"
        f"<b>Как запустить:</b>\n"
        f"1. Главное меню → «Отложенная рассылка».\n"
        f"2. Выберите аккаунт.\n"
        f"3. Выберите чаты.\n"
        f"4. Задайте задержку между сообщениями.\n"
        f"5. Задайте число кругов.\n"
        f"6. Отправьте текст/медиа.\n"
        f"7. Укажите дату и время в формате "
        f"<code>ДД.ММ.ГГГГ ЧЧ:ММ</code> (Москва).\n"
        f"8. Подтвердите превью — задача уйдёт в очередь.\n\n"
        f"<b>Управление:</b> «Мои отложенные рассылки» — там можно "
        f"остановить, удалить или посмотреть статус задачи.\n\n"
        f"<b>Советы:</b>\n"
        f"• Лучшее время — будни, 10:00–12:00 и 19:00–22:00 МСК.\n"
        f"• Не планируйте больше 3–4 задач одновременно на один аккаунт."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "help_autoresponder")
async def help_autoresponder_handler(callback: CallbackQuery):
    text = (
        f"{emoji('BELL')} <b>Автоответчик</b>\n\n"
        f"<b>Что это:</b> автоматический ответ на входящие сообщения в ЛС. "
        f"Два режима: по ключевым словам или AI.\n\n"
        f"<b>Как запустить:</b>\n"
        f"1. Главное меню → «Автоответчик».\n"
        f"2. Выберите аккаунт.\n"
        f"3. Выберите режим:\n"
        f"   • <b>По ключевым словам</b> — задаёте пары «триггер → ответ». "
        f"Можно добавлять несколько через запятую.\n"
        f"   • <b>AI-ответ</b> — модель сама формирует ответ в заданном "
        f"стиле (введите системный промпт).\n"
        f"4. Укажите задержку ответа (например <code>5-25</code> сек).\n"
        f"5. Включите тумблер «Активен».\n\n"
        f"<b>Советы:</b>\n"
        f"• Не отвечайте на сообщения от ботов — включите фильтр в настройках.\n"
        f"• AI-режим тратит токены — учитывайте лимиты Free/Pro.\n"
        f"• Для AI используйте подписку Pro — там лимиты выше."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "help_autolike")
async def help_autolike_handler(callback: CallbackQuery):
    text = (
        f"{emoji('LIKE')} <b>Автолайкинг (реакции)</b>\n\n"
        f"<b>Что это:</b> аккаунт автоматически ставит выбранные реакции "
        f"на новые сообщения в указанных чатах. Имитирует живую активность.\n\n"
        f"<b>Как запустить:</b>\n"
        f"1. Главное меню → «Автолайкинг».\n"
        f"2. Выберите аккаунт.\n"
        f"3. Выберите чаты, где будем лайкать.\n"
        f"4. Выберите набор реакций (по умолчанию: 👍❤🔥).\n"
        f"5. Задайте вероятность реакции (например 70% — не на каждое сообщение).\n"
        f"6. Укажите задержку между реакциями "
        f"(например <code>10-40</code> сек).\n"
        f"7. Нажмите «Запустить».\n\n"
        f"<b>Управление:</b>\n"
        f"• Кнопка «Стоп» в карточке задачи — остановит воркер.\n"
        f"• В «Истории» видно, сколько реакций поставлено за сегодня.\n\n"
        f"<b>Советы:</b>\n"
        f"• Ставьте 2–3 разные реакции, чередуя — бот не лайкает всё подряд.\n"
        f"• Не запускайте автолайкинг одновременно с рассылкой на одном "
        f"аккаунте — это красный флаг для Telegram."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "help_join")
async def help_join_handler(callback: CallbackQuery):
    text = (
        f"{emoji('JOIN')} <b>Вступление в чаты</b>\n\n"
        f"<b>Что это:</b> массовое вступление аккаунтом в список чатов/каналов "
        f"по ссылкам или @username. Полезно как подготовка к рассылке.\n\n"
        f"<b>Как запустить:</b>\n"
        f"1. Главное меню → «Вступление в чаты».\n"
        f"2. Выберите аккаунт.\n"
        f"3. Загрузите <code>.txt</code> со ссылками (по одной в строке):\n"
        f"   • <code>https://t.me/chat_name</code>\n"
        f"   • <code>@chat_name</code>\n"
        f"   • приватные <code>https://t.me/+invite_hash</code>\n"
        f"4. Укажите задержку между вступлениями "
        f"(например <code>60-180</code> сек).\n"
        f"5. «Запустить» — воркер пойдёт по списку.\n\n"
        f"<b>Советы:</b>\n"
        f"• Не вступайте в десятки чатов за раз — Telegram склеит и забанит.\n"
        f"• Сначала прогрейте аккаунт хотя бы день.\n"
        f"• Вступайте в тематику, куда потом будете рассылать."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "help_delete")
async def help_delete_handler(callback: CallbackQuery):
    text = (
        f"{emoji('SWEEP')} <b>Удаление сообщений</b>\n\n"
        f"<b>Что это:</b> массовое удаление ваших сообщений в выбранных "
        f"чатах. Удаляются только СВОИ сообщения, не чужие.\n\n"
        f"<b>Как запустить:</b>\n"
        f"1. Главное меню → «Удаление сообщений».\n"
        f"2. Выберите аккаунт.\n"
        f"3. Выберите чаты для очистки.\n"
        f"4. Укажите глубину: за последние N часов/дней "
        f"(например за 24 часа).\n"
        f"5. Укажите лимит сообщений на чат (защита от перебора).\n"
        f"6. Задайте задержку (например <code>3-10</code> сек).\n"
        f"7. «Запустить».\n\n"
        f"<b>Советы:</b>\n"
        f"• Используйте перед/после рассылки, чтобы не оставлять следов.\n"
        f"• Не удаляйте слишком много за раз — Telegram может временно "
        f"ограничить аккаунт."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "help_parse")
async def help_parse_handler(callback: CallbackQuery):
    text = (
        f"{emoji('USERS')} <b>Парсинг чата</b>\n\n"
        f"<b>Что это:</b> сбор участников выбранных чатов с фильтрами. "
        f"Результат — <code>.txt</code> со списком пользователей.\n\n"
        f"<b>Как запустить:</b>\n"
        f"1. Главное меню → «Парсинг чата».\n"
        f"2. Выберите аккаунт.\n"
        f"3. Выберите чаты-источники.\n"
        f"4. Задайте фильтры:\n"
        f"   • <b>Только онлайн</b>.\n"
        f"   • <b>Только с @username</b> (нужно для ЛС-рассылки).\n"
        f"   • <b>Только с фото</b>.\n"
        f"   • <b>Исключить ботов</b>.\n"
        f"5. Укажите лимит (например 5000 пользователей).\n"
        f"6. «Запустить» — по завершении бот пришлёт файл.\n\n"
        f"<b>Куда дальше:</b> этот файл можно загрузить в «Рассылку в ЛС».\n\n"
        f"<b>Советы:</b>\n"
        f"• В больших чатах (>200k) Telegram отдаёт участников порциями — "
        f"парсинг может занять 10–30 минут.\n"
        f"• Не парсите один и тот же чат чаще раза в сутки."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "help_scripts")
async def help_scripts_handler(callback: CallbackQuery):
    text = (
        f"{emoji('PLAY')} <b>Скрипты и публичные маршруты</b>\n\n"
        "<b>Личный скрипт</b> — маршрут из нескольких кнопок Telegram-бота. "
        "Промежуточные шаги реально открывают новые разделы, поэтому после "
        "«Профиль» загружаются кнопки профиля, а не главное меню.\n\n"
        "<b>Как создать:</b>\n"
        "1. Функции → «Скрипты» → «Создать скрипт».\n"
        "2. Выберите свой аккаунт и ссылку на бота.\n"
        "3. Укажите, есть ли фото-капча.\n"
        "4. Для каждой кнопки выберите: перейти дальше или сделать финальным шагом.\n"
        "5. Сохраните маршрут и запустите его.\n\n"
        "<b>Публичные скрипты:</b>\n"
        "• В карточке своего скрипта нажмите «Выложить публично».\n"
        "• В «Публичных скриптах» можно посмотреть маршруты других пользователей.\n"
        "• «Применить себе» копирует только маршрут, настройки капчи и ссылку на бота. "
        "Чужие аккаунты, сессии и номера никогда не копируются.\n\n"
        "<b>Запуск:</b> скрипт повторяет полный маршрут бесконечно до кнопки «Остановить». "
        "После подписки по кнопке-каналу сохраняется то же окружение бота, без нового /start."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "help_ai")
async def help_ai_handler(callback: CallbackQuery):
    text = (
        f"{emoji('AI')} <b>AI Генератор текста</b>\n\n"
        f"<b>Что это:</b> генерирует 3 варианта рекламного/информационного "
        f"текста под вашу тему. Используется для быстрого наполнения "
        f"рассылок.\n\n"
        f"<b>Как запустить:</b>\n"
        f"1. Главное меню → «AI Генератор».\n"
        f"2. Опишите тему: что за товар/услуга, аудитория, тон "
        f"(формальный, дружеский, продающий).\n"
        f"3. Нажмите «Сгенерировать».\n"
        f"4. Получите 3 варианта, выберите лучший.\n"
        f"5. Кнопка «Скопировать» или «Отправить в рассылку».\n\n"
        f"<b>Советы:</b>\n"
        f"• Чем точнее тема — тем лучше результат. Добавьте: продукт, "
        f"целевую аудиторию, ограничение по длине.\n"
        f"• Free: ограниченное число генераций в день. Pro: без лимитов.\n"
        f"• Сгенерированный текст перед отправкой лучше подредактировать."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "help_ai_chat")
async def help_ai_chat_handler(callback: CallbackQuery):
    text = (
        f"{emoji('AI')} <b>Чат с нейросетями</b>\n\n"
        "Откройте кнопку «Чат с нейросетями» рядом с подпиской, отправьте сообщение "
        "и продолжайте диалог — последние реплики сохраняются в контексте.\n\n"
        "• Можно очистить историю диалога отдельной кнопкой.\n"
        "• Используется выбранная модель и личный API, если он настроен; иначе базовый API.\n"
        f"• Free: <b>{AI_CHAT_FREE_DAILY_LIMIT}</b> успешных запроса в день.\n"
        f"• Pro: <b>{AI_CHAT_PRO_DAILY_LIMIT}</b> успешных запросов в день.\n"
        f"• MAX: <b>{AI_CHAT_MAX_DAILY_LIMIT}</b> успешных запросов в день.\n\n"
        "Если API вернул ошибку или пустой ответ, лимит за такой запрос возвращается."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "help_neurocomment")
async def help_neurocomment_handler(callback: CallbackQuery):
    text = (
        f"{emoji('AI')} <b>Нейрокомментинг</b>\n\n"
        "Функции → «Нейрокомментинг» → выберите аккаунт и каналы. Можно выбрать "
        "любое количество каналов из загруженных, но для более безопасной нагрузки "
        "рекомендуется до 30.\n\n"
        "<b>Режимы:</b>\n"
        "• <b>Только ИИ</b> — выберите модель; текст поста передаётся в ИИ, а если текста нет — фото.\n"
        "• <b>Заготовленные сообщения</b> — добавьте от 1 до 100 вариантов; вариант выбирается случайно.\n\n"
        "Укажите задержку после выхода поста, затем запустите конфигурацию. "
        "Работают только новые посты. Комментарии доступны лишь там, где у канала включено обсуждение; "
        "возможные ошибки видны в карточке конфигурации."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "help_monitoring")
async def help_monitoring_handler(callback: CallbackQuery):
    text = (
        f"{emoji('STATS')} <b>Автоматический мониторинг аккаунтов</b>\n\n"
        "• Валидность активных Telegram-аккаунтов проверяется примерно раз в час.\n"
        "• Только подтверждённо отозванная/неавторизованная сессия удаляется из работы; "
        "временная ошибка сети или прокси не удаляет аккаунт.\n"
        "• Раз в 7 дней владелец получает AI-анализ логов и FloodWait.\n"
        "• Проверка ограничений через @SpamBot выполняется раз в 12 часов; уведомления можно выключить в карточке аккаунта.\n"
        "• Текущие FloodWait и результаты мониторинга отображаются в карточке аккаунта."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "help_tariffs")
async def help_tariffs_handler(callback: CallbackQuery):
    text = (
        f"{emoji('STAR')} <b>Тарифы Free, Pro и MAX</b>\n\n"
        "<b>Free</b>\n"
        "• Управление аккаунтами, прокси, парсинг, скрипты, нейрокомментинг и основные функции доступны.\n"
        "• AI-генератор: 1 запрос в день.\n"
        f"• Чат с нейросетями: {AI_CHAT_FREE_DAILY_LIMIT} успешных запроса в день.\n"
        f"• Рассылки: до {FREE_BROADCAST_LIMIT_HOURS} часов суммарного времени работы в неделю.\n"
        "• Базовые AI-функции используют доступную модель/базовый API.\n\n"
        "<b>Pro</b>\n"
        "• AI-генератор без дневного лимита.\n"
        f"• Чат с нейросетями: {AI_CHAT_PRO_DAILY_LIMIT} успешных запросов в день.\n"
        "• Без лимита по времени рассылок.\n"
        "• Ручной AI-анализ риска аккаунта.\n"
        "• AI-автоответчик на аккаунте.\n"
        "• Приоритетная поддержка и ранний доступ к новым функциям.\n\n"
        f"<b>Сроки и цены Pro/MAX:</b>\n{pro_plans_text()}\n\n"
        "Срок можно взять любой — при продлении дни складываются."
    )
    await callback.message.edit_text(text, reply_markup=get_help_feature_keyboard())
    await callback.answer()


# ============================================================
# Подписка: Free / Pro
# ============================================================
def get_subscription_keyboard(tier: str) -> InlineKeyboardMarkup:
    """Клавиатура экрана подписки в зависимости от текущего тира."""
    builder = InlineKeyboardBuilder()
    if tier in ('pro', 'max'):
        tier_label = subscription_tier_label(tier)
        builder.row(InlineKeyboardButton(
            text=f"{tier_label} активен — спасибо!",
            callback_data="noop",
            style='success',
            icon_custom_emoji_id=get_icon("CHECK")
        ))
        builder.row(InlineKeyboardButton(
            text=f"Продлить {tier_label}",
            callback_data="buy_pro",
            style='primary',
            icon_custom_emoji_id=get_icon("MONEY_SEND")
        ))
    else:
        builder.row(InlineKeyboardButton(
            text=f"Купить Pro/MAX — {pro_min_price_label()}",
            callback_data="buy_pro",
            style='primary',
            icon_custom_emoji_id=get_icon("MONEY_SEND")
        ))

    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="main_menu",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()


async def format_limits_text(user_id: int) -> str:
    """Returns a short usage-counter block to embed in subscription/menu messages."""
    sub = await get_subscription(user_id)
    tier = sub.get('tier', 'free')
    if tier in ('pro', 'max'):
        try:
            chat_used = await get_ai_chat_usage(user_id)
        except Exception:
            chat_used = 0
        chat_limit = AI_CHAT_MAX_DAILY_LIMIT if tier == 'max' else AI_CHAT_PRO_DAILY_LIMIT
        return (
            f"{emoji('STAR')} <b>Лимиты:</b> {subscription_tier_label(tier)}\n"
            f"  AI-генератор: без ограничений\n"
            f"  Чат с нейросетями: {chat_used}/{chat_limit} сегодня"
        )

    # Счётчики использования — не критичны. Если БД недоступна или
    # какой-то таблицы нет, показываем нули вместо падения обработчика.
    try:
        ai_used = await count_ai_requests_today(user_id)
    except Exception as ex:
        logger.error(f"count_ai_requests_today failed for {user_id}: {ex}")
        ai_used = 0
    ai_limit = 1
    ai_bar = "█" * ai_used + "░" * max(0, ai_limit - ai_used)

    try:
        broadcast_seconds = await get_user_broadcast_seconds_this_week(user_id)
    except Exception as ex:
        logger.error(
            f"get_user_broadcast_seconds_this_week failed for {user_id}: {ex}"
        )
        broadcast_seconds = 0.0
    broadcast_used_h = broadcast_seconds / 3600.0
    broadcast_limit_h = FREE_BROADCAST_LIMIT_HOURS
    bar_filled = min(round(broadcast_used_h), broadcast_limit_h)
    broadcast_bar = "█" * bar_filled + "░" * max(0, broadcast_limit_h - bar_filled)
    # Reset time: midnight UTC 7 days from the oldest broadcast start this week
    reset_info = "обновляется через 7 дней"
    try:
        chat_used = await get_ai_chat_usage(user_id)
    except Exception as ex:
        logger.error(f"get_ai_chat_usage failed for {user_id}: {ex}")
        chat_used = 0

    return (
        f"📊 <b>Использование (Free):</b>\n"
        f"  AI-запросы сегодня: <code>{ai_bar}</code> {ai_used}/{ai_limit}\n"
        f"  Чат с нейросетями: {chat_used}/{AI_CHAT_FREE_DAILY_LIMIT}\n"
        f"  Рассылка на неделе: {broadcast_used_h:.1f}/{broadcast_limit_h} ч "
        f"({reset_info})"
    )


def _format_sub_text_sync(sub: Dict[str, Any], limits_text: str = "") -> str:
    tier = sub.get("tier", "free")
    limits_block = f"\n\n{limits_text}" if limits_text else ""
    if tier in ('pro', 'max'):
        exp = sub.get("expires_at")
        exp_str = ""
        if exp:
            try:
                exp_str = exp.astimezone(MSK_TZ).strftime("%d.%m.%Y %H:%M МСК")
            except Exception:
                exp_str = str(exp)
        return (
            f"{emoji('MONEY_SEND')} <b>Моя подписка</b>\n\n"
            f"<b>Тариф:</b> {subscription_tier_label(tier)}\n"
            f"{emoji('CLOCK')} <b>Активна до:</b> {exp_str}\n\n"
            f"{emoji('CHECK')} Спасибо за поддержку! Все функции открыты.\n"
            f"Продление можно взять на любой срок — дни складываются."
            f"{limits_block}"
        )
    return (
        f"{emoji('MONEY_SEND')} <b>Моя подписка</b>\n\n"
        f"🆓 <b>Тариф:</b> Free\n\n"
        f"<b>Pro / MAX</b> — {pro_min_price_label()}:\n"
        f"  {emoji('CHECK')} Сняты базовые лимиты на рассылки\n"
        f"  {emoji('CHECK')} Приоритетная поддержка\n"
        f"  {emoji('CHECK')} Ранний доступ к новым функциям\n"
        f"  {emoji('CHECK')} Smart Delay Engine в усиленном режиме\n\n"
        f"<b>Сроки:</b>\n{pro_plans_text()}\n\n"
        f"Оплата: СБП (₽) или @CryptoBot (USDT)."
        f"{limits_block}"
    )


# Keep old name as thin wrapper for any call sites that still use it
def _format_sub_text(sub: Dict[str, Any]) -> str:
    return _format_sub_text_sync(sub)


@dp.callback_query(F.data == "my_subscription")
async def my_subscription(callback: CallbackQuery):
    user_id = callback.from_user.id
    sub = await get_subscription(user_id)
    limits = await format_limits_text(user_id)
    await present_section(
        callback.message, 'subscription', _format_sub_text_sync(sub, limits),
        get_subscription_keyboard(sub.get("tier", "free")), replace=True
    )
    await callback.answer()


@dp.callback_query(F.data == "noop")
async def noop_handler(callback: CallbackQuery):
    await callback.answer("У вас уже Pro — можно продлить срок", show_alert=False)


@dp.callback_query(F.data == "buy_pro")
async def buy_pro(callback: CallbackQuery):
    """Шаг 1 — выбор срока подписки."""
    await callback.answer()
    sub = await get_subscription(callback.from_user.id)
    is_active_pro = sub.get("tier") in ("pro", "max")
    active_label = subscription_tier_label(sub.get("tier"))

    builder = InlineKeyboardBuilder()
    for code in PRO_PLAN_ORDER:
        plan = PRO_PLANS[code]
        builder.row(InlineKeyboardButton(
            text=pro_plan_button_text(plan),
            callback_data=f"pro_plan:{code}",
            style='primary' if code == DEFAULT_PRO_PLAN else 'default',
            icon_custom_emoji_id=get_icon("MONEY_SEND")
        ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="my_subscription",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))

    header = (
        f"{emoji('MONEY_SEND')} <b>Продление {active_label}</b>\n\n"
        "Новый срок прибавится к текущей подписке.\n\n"
        if is_active_pro else
        f"{emoji('MONEY_SEND')} <b>Покупка Pro/MAX</b>\n\n"
    )

    note = ""
    if sub.get("tier") == "pro" and sub.get("expires_at") and sub["expires_at"] > datetime.now(MSK_TZ).replace(tzinfo=None):
        note = "\n⚠️ У вас уже есть <b>Pro</b>. При покупке <b>MAX</b> он начнётся только после окончания Pro-тарифа.\n"

    await callback.message.edit_text(
        header +
        "Выберите срок:\n"
        f"{pro_plans_text()}\n\n"
        f"Оплата: СБП (₽) или Crypto Pay (USDT).{note}",
        reply_markup=builder.as_markup()
    )


@dp.callback_query(F.data.startswith("pro_plan:"))
async def buy_pro_choose_method(callback: CallbackQuery):
    """Шаг 2 — выбор способа оплаты для выбранного срока."""
    await callback.answer()
    plan = get_pro_plan(callback.data.split(":", 1)[1])

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text=f"СБП — {plan['rub']}₽",
        callback_data=f"buy_pro_sbp:{plan['code']}",
        style='primary',
        icon_custom_emoji_id=get_icon("MONEY_SEND")
    ))
    builder.row(InlineKeyboardButton(
        text=f"Crypto Pay — {plan['usd']} USDT",
        callback_data=f"buy_pro_crypto:{plan['code']}",
        style='default',
        icon_custom_emoji_id=get_icon("MONEY_SEND")
    ))
    builder.row(InlineKeyboardButton(
        text="Другой срок",
        callback_data="buy_pro",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="my_subscription",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    await callback.message.edit_text(
        f"{emoji('MONEY_SEND')} <b>Оплата подписки</b>\n\n"
        f"Срок: <b>{plan['title']}</b>\n"
        f"Стоимость: <b>{plan['rub']}₽</b> / <b>{plan['usd']} USDT</b>\n\n"
        f"Выберите удобный способ оплаты:\n"
        f"  {emoji('CHECK')} <b>СБП</b> — оплата по QR-коду / Sberpay (₽)\n"
        f"  {emoji('CHECK')} <b>Crypto Pay</b> — оплата в USDT через @CryptoBot",
        reply_markup=builder.as_markup()
    )


@dp.callback_query(F.data.startswith("buy_pro_crypto"))
async def buy_pro_crypto(callback: CallbackQuery):
    await callback.answer()
    plan = get_pro_plan(
        callback.data.split(":", 1)[1] if ":" in callback.data else None
    )

    await callback.message.edit_text(
        f"{emoji('CLOCK')} Создаю счёт в Crypto Pay...",
        reply_markup=None
    )
    result = await cryptopay_create_invoice(
        callback.from_user.id, payload=plan["code"]
    )
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
    _cur = await get_subscription(callback.from_user.id)
    sub_tier_now = _cur.get("tier", "free")
    sub_expires_now = _cur.get("expires_at")
    # Сохраняем счёт и выбранный срок, чтобы «Проверить оплату» знала,
    # какой тариф активировать. Текущий tier не трогаем (может быть Pro).
    await set_subscription(
        callback.from_user.id, sub_tier_now, sub_expires_now,
        invoice_id=invoice_id,
        invoice_payload=f"{plan['code']}:{callback.from_user.id}"
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
        text="Назад",
        callback_data="my_subscription",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))

    amount = inv.get("amount", plan["usd"])
    asset = inv.get("asset", "USDT")
    edited = await callback.message.edit_text(
        f"{emoji('MONEY_SEND')} <b>Счёт на оплату подписки</b>\n\n"
        f"Тариф: <b>{subscription_tier_label(plan.get('tier'))} · {plan['title']}</b>\n"
        f"Сумма: <b>{amount} {asset}</b>\n\n"
        f"{emoji('CLOCK')} Оплата проверяется автоматически...",
        reply_markup=builder.as_markup()
    )
    asyncio.create_task(_auto_check_pro_crypto(
        user_id=callback.from_user.id,
        invoice_id=int(invoice_id),
        chat_id=callback.message.chat.id,
        msg_id=edited.message_id,
        plan_code=plan["code"],
    ))

# --- СБП (Platega) для Pro-подписки ---
@dp.callback_query(F.data.startswith("buy_pro_sbp"))
async def buy_pro_sbp(callback: CallbackQuery):
    await callback.answer()
    plan = get_pro_plan(
        callback.data.split(":", 1)[1] if ":" in callback.data else None
    )

    await callback.message.edit_text(
        f"{emoji('CLOCK')} Создаю счёт СБП...",
        reply_markup=None
    )
    result = await platega_create_transaction(
        callback.from_user.id, payload=plan["code"]
    )
    if not result.get("ok"):
        await callback.message.edit_text(
            f"{emoji('CROSS')} Не удалось создать счёт СБП.\n"
            f"Попробуйте позже или напишите в поддержку: {SUPPORT_USERNAME}\n\n"
            f"<code>{escape(str(result.get('error')))}</code>",
            reply_markup=get_subscription_keyboard("free")
        )
        return

    tx = result["result"]
    transaction_id = tx.get("transactionId")
    pay_url = tx.get("redirect")
    _cur = await get_subscription(callback.from_user.id)
    await set_subscription(
        callback.from_user.id, _cur.get("tier", "free"), _cur.get("expires_at"),
        invoice_payload=f"{plan['code']}:{callback.from_user.id}",
        platega_id=transaction_id
    )

    builder = InlineKeyboardBuilder()
    if pay_url:
        builder.row(InlineKeyboardButton(
            text="Оплатить по СБП",
            url=pay_url,
            style='primary',
            icon_custom_emoji_id=get_icon("MONEY_SEND")
        ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data="my_subscription",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    edited = await callback.message.edit_text(
        f"{emoji('MONEY_SEND')} <b>Счёт на оплату подписки (СБП)</b>\n\n"
        f"Тариф: <b>{subscription_tier_label(plan.get('tier'))} · {plan['title']}</b>\n"
        f"Сумма: <b>{plan['rub']} ₽</b>\n\n"
        f"{emoji('CLOCK')} Оплата проверяется автоматически...",
        reply_markup=builder.as_markup()
    )
    asyncio.create_task(_auto_check_pro_sbp(
        user_id=callback.from_user.id,
        transaction_id=transaction_id,
        chat_id=callback.message.chat.id,
        msg_id=edited.message_id,
        plan_code=plan["code"],
    ))


async def _auto_check_pro_crypto(
    user_id: int, invoice_id: int,
    chat_id: int, msg_id: int,
    plan_code: str = DEFAULT_PRO_PLAN
) -> None:
    """Автоматически проверяет оплату Pro через Crypto Pay каждые 2 сек до 10 минут."""
    plan = get_pro_plan(plan_code)
    deadline = time.monotonic() + 10 * 60
    while time.monotonic() < deadline:
        await asyncio.sleep(2)
        try:
            result = await cryptopay_get_invoices(str(invoice_id))
            items = (result.get('result') or {}).get('items', []) if result.get('ok') else []
            if not items:
                continue
            status = items[0].get('status')
            if status == 'paid':
                # Защита от двойной активации: платёж заносим первым,
                # и только новый (не дублирующий) платёж продлевает подписку.
                fresh = await record_payment_event(
                    user_id,
                    PAYMENT_KIND_PRO_SUBSCRIPTION,
                    PAYMENT_PROVIDER_CRYPTOPAY,
                    invoice_id,
                    amount_usdt=float(plan['usd']),
                    metadata={'duration_days': plan['days'], 'plan': plan['code']},
                )
                if await should_activate_pro(
                    PAYMENT_PROVIDER_CRYPTOPAY, invoice_id, fresh
                ):
                    await activate_pro_plan(
                        user_id, plan, invoice_id=invoice_id
                    )
                new_sub = await get_subscription(user_id)
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id, message_id=msg_id,
                        text=(
                            f"{emoji('FIRE')} <b>Оплата получена!</b>\n\n"
                            + _format_sub_text(new_sub)
                        ),
                        reply_markup=get_subscription_keyboard(new_sub.get('tier', 'free'))
                    )
                except Exception:
                    pass
                return
        except Exception as ex:
            logger.warning(f"[auto_check_pro_crypto] error: {ex}")
    # Время вышло
    try:
        sub = await get_subscription(user_id)
        if sub.get("tier") in ('pro', 'max'):
            return
        await bot.edit_message_text(
            chat_id=chat_id, message_id=msg_id,
            text=(
                f"{emoji('CROSS')} Время ожидания оплаты истекло.\n"
                f"Если вы оплатили — напишите в поддержку: {SUPPORT_USERNAME}"
            ),
            reply_markup=get_subscription_keyboard(sub.get("tier", "free"))
        )
    except Exception:
        pass


async def _auto_check_pro_sbp(
    user_id: int, transaction_id: str,
    chat_id: int, msg_id: int,
    plan_code: str = DEFAULT_PRO_PLAN
) -> None:
    """Автоматически проверяет оплату Pro через СБП (Platega) каждые 2 сек до 30 минут."""
    plan = get_pro_plan(plan_code)
    deadline = time.monotonic() + 30 * 60
    while time.monotonic() < deadline:
        await asyncio.sleep(2)
        try:
            result = await platega_get_transaction(transaction_id)
            if not result.get('ok'):
                continue
            status = (result['result'].get('status') or '').upper()
            if status == 'CONFIRMED':
                fresh = await record_payment_event(
                    user_id,
                    PAYMENT_KIND_PRO_SUBSCRIPTION,
                    PAYMENT_PROVIDER_PLATEGA,
                    transaction_id,
                    amount_rub=float(plan['rub']),
                    metadata={'duration_days': plan['days'], 'plan': plan['code']},
                )
                if await should_activate_pro(
                    PAYMENT_PROVIDER_PLATEGA, transaction_id, fresh
                ):
                    await activate_pro_plan(
                        user_id, plan, platega_id=transaction_id
                    )
                new_sub = await get_subscription(user_id)
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id, message_id=msg_id,
                        text=(
                            f"{emoji('FIRE')} <b>Оплата получена!</b>\n\n"
                            + _format_sub_text(new_sub)
                        ),
                        reply_markup=get_subscription_keyboard(new_sub.get('tier', 'free'))
                    )
                except Exception:
                    pass
                return
        except Exception as ex:
            logger.warning(f"[auto_check_pro_sbp] error: {ex}")
    # Время вышло
    try:
        sub = await get_subscription(user_id)
        if sub.get("tier") in ('pro', 'max'):
            return
        await bot.edit_message_text(
            chat_id=chat_id, message_id=msg_id,
            text=(
                f"{emoji('CROSS')} Время ожидания оплаты истекло.\n"
                f"Если вы оплатили — напишите в поддержку: {SUPPORT_USERNAME}"
            ),
            reply_markup=get_subscription_keyboard(sub.get("tier", "free"))
        )
    except Exception:
        pass


@dp.callback_query(F.data == "check_pro_payment")
async def check_pro_payment(callback: CallbackQuery):
    """Ручная проверка последнего счёта Pro (Crypto Pay и/или СБП)."""
    user_id = callback.from_user.id
    await callback.answer("Проверяю оплату...")
    sub = await get_subscription(user_id)
    plan = get_pro_plan(sub.get("last_invoice_payload"))
    activated = False

    invoice_id = sub.get("last_invoice_id")
    if invoice_id:
        try:
            result = await cryptopay_get_invoices(str(invoice_id))
            items = (result.get('result') or {}).get('items', []) if result.get('ok') else []
            if items and items[0].get('status') == 'paid':
                fresh = await record_payment_event(
                    user_id,
                    PAYMENT_KIND_PRO_SUBSCRIPTION,
                    PAYMENT_PROVIDER_CRYPTOPAY,
                    invoice_id,
                    amount_usdt=float(plan['usd']),
                    metadata={'duration_days': plan['days'], 'plan': plan['code'],
                              'source': 'manual'},
                )
                if await should_activate_pro(
                    PAYMENT_PROVIDER_CRYPTOPAY, invoice_id, fresh
                ):
                    await activate_pro_plan(user_id, plan, invoice_id=int(invoice_id))
                activated = True
        except Exception as ex:
            logger.warning(f"[check_pro_payment] cryptopay: {ex}")

    transaction_id = sub.get("last_platega_id")
    if not activated and transaction_id:
        try:
            result = await platega_get_transaction(str(transaction_id))
            status = (result.get('result') or {}).get('status') or '' if result.get('ok') else ''
            if status.upper() == 'CONFIRMED':
                fresh = await record_payment_event(
                    user_id,
                    PAYMENT_KIND_PRO_SUBSCRIPTION,
                    PAYMENT_PROVIDER_PLATEGA,
                    transaction_id,
                    amount_rub=float(plan['rub']),
                    metadata={'duration_days': plan['days'], 'plan': plan['code'],
                              'source': 'manual'},
                )
                if await should_activate_pro(
                    PAYMENT_PROVIDER_PLATEGA, transaction_id, fresh
                ):
                    await activate_pro_plan(user_id, plan, platega_id=str(transaction_id))
                activated = True
        except Exception as ex:
            logger.warning(f"[check_pro_payment] platega: {ex}")

    new_sub = await get_subscription(user_id)
    limits = await format_limits_text(user_id)
    prefix = ""
    if activated:
        prefix = f"{emoji('FIRE')} <b>Оплата получена!</b>\n\n"
    elif new_sub.get("tier") not in ('pro', 'max'):
        prefix = (
            f"{emoji('CLOCK')} Оплата пока не найдена. "
            f"Если вы уже оплатили — подождите минуту и нажмите ещё раз.\n\n"
        )
    try:
        await callback.message.edit_text(
            prefix + _format_sub_text_sync(new_sub, limits),
            reply_markup=get_subscription_keyboard(new_sub.get("tier", "free"))
        )
    except TelegramBadRequest:
        pass


# --- Пользовательские AI API ---
async def _render_llm_api_settings(target: Message, user_id: int) -> None:
    apis = await get_user_llm_apis(user_id)
    active = next((a for a in apis if a.get('is_active')), None)
    runtime = await get_global_llm_runtime()
    lines = [
        f"{emoji('GEAR')} <b>Настройки AI API</b>",
        "",
        f"Режим: <b>{'Ваш API' if active else 'Базовый API'}</b>",
    ]
    if not active:
        lines.append(f"Источник: <code>{escape(str(runtime.get('name') or 'Встроенный API'))}</code>")
    if active:
        lines.append(f"Модели: <code>{escape(', '.join(active.get('models') or []))}</code>")
    if apis:
        lines += ['', '<b>Сохранённые API:</b>']
        for api in apis:
            marker = 'активен' if api.get('is_active') else 'не активен'
            lines.append(f"{api['name']} — {marker}")
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text='Добавить новый API', callback_data='ai_api_add',
                                     style='primary', icon_custom_emoji_id=get_icon('ADD_TEXT')))
    for api in apis:
        if not api.get('is_active'):
            builder.row(InlineKeyboardButton(text=f"Применить: {api['name']}",
                                             callback_data=f"ai_api_use:{api['id']}", style='default',
                                             icon_custom_emoji_id=get_icon('CHECK')))
    if active:
        builder.row(InlineKeyboardButton(text='Вернуться к базовому API', callback_data='ai_api_builtin',
                                         style='default', icon_custom_emoji_id=get_icon('BACK')))
    builder.row(InlineKeyboardButton(text='Назад', callback_data='ai_generator', style='default',
                                     icon_custom_emoji_id=get_icon('BACK')))
    await target.edit_text('\n'.join(lines), reply_markup=builder.as_markup())


@dp.callback_query(F.data == 'ai_api_settings')
async def ai_api_settings(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await _render_llm_api_settings(callback.message, callback.from_user.id)
    await callback.answer()


@dp.callback_query(F.data == 'ai_api_add')
async def ai_api_add(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserLLMConfigStates.waiting_for_base_url)
    await callback.message.edit_text(
        f"{emoji('LINK')} <b>Новый AI API</b>\n\n"
        "Отправьте base URL Anthropic-совместимого API (https://...):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text='Отмена', callback_data='ai_api_settings', style='default',
                                 icon_custom_emoji_id=get_icon('BACK'))
        ]])
    )
    await callback.answer()


@dp.message(UserLLMConfigStates.waiting_for_base_url)
async def ai_api_base_url(message: Message, state: FSMContext):
    value = (message.text or '').strip().rstrip('/')
    parsed = urlparse(value)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc or '@' in parsed.netloc:
        await message.answer('Нужен корректный URL с http:// или https:// без логина и пароля.')
        return
    await state.update_data(base_url=value)
    await state.set_state(UserLLMConfigStates.waiting_for_api_key)
    await message.answer('Отправьте API-токен. Он будет зашифрован в базе и не показывается повторно.')


@dp.message(UserLLMConfigStates.waiting_for_api_key)
async def ai_api_key(message: Message, state: FSMContext):
    value = (message.text or '').strip()
    if len(value) < 8 or len(value) > 4096:
        await message.answer('Токен имеет некорректную длину. Отправьте его ещё раз.')
        return
    await state.update_data(api_key=value)
    await state.set_state(UserLLMConfigStates.waiting_for_models)
    await message.answer('Укажите модели через запятую, например: claude-3-5-sonnet, deepseek-chat')


@dp.message(UserLLMConfigStates.waiting_for_models)
async def ai_api_models(message: Message, state: FSMContext):
    models = [x.strip() for x in (message.text or '').split(',') if x.strip()]
    models = list(dict.fromkeys(models))
    if not models or len(models) > 20 or any(len(x) > 120 for x in models):
        await message.answer('Укажите от 1 до 20 моделей через запятую.')
        return
    data = await state.get_data()
    try:
        await save_user_llm_api(message.from_user.id, data['base_url'], data['api_key'], models)
    except Exception as ex:
        logger.exception('save user llm api failed')
        await state.clear()
        await message.answer(f"{emoji('CROSS')} Не удалось сохранить настройки. Попробуйте позже.")
        return
    await state.clear()
    await message.answer(f"{emoji('CHECK')} API применён. Активная модель: <code>{escape(models[0])}</code>")
    await _render_llm_api_settings(message, message.from_user.id)


@dp.callback_query(F.data.startswith('ai_api_use:'))
async def ai_api_use(callback: CallbackQuery):
    try:
        await set_active_llm_api(callback.from_user.id, int(callback.data.split(':', 1)[1]))
    except Exception:
        await callback.answer('API не найден', show_alert=True)
        return
    await _render_llm_api_settings(callback.message, callback.from_user.id)
    await callback.answer('API применён')


@dp.callback_query(F.data == 'ai_api_builtin')
async def ai_api_builtin(callback: CallbackQuery):
    await set_active_llm_api(callback.from_user.id, None)
    await _render_llm_api_settings(callback.message, callback.from_user.id)
    await callback.answer('Базовый API включён')


@dp.callback_query(F.data == 'llm_choose_custom', LLMStates.choosing_model)
async def llm_choose_custom(callback: CallbackQuery, state: FSMContext):
    await state.set_state(LLMStates.waiting_for_prompt)
    await callback.message.edit_text(
        f"{emoji('AI')} Опишите задачу для вашей модели:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text='Настройки API', callback_data='ai_api_settings',
                                 style='default', icon_custom_emoji_id=get_icon('GEAR'))
        ]])
    )
    await callback.answer()


# --- AI Генератор текста (LLM) ---
@dp.callback_query(F.data == "ai_generator")
async def ai_generator_start(callback: CallbackQuery, state: FSMContext):
    """Шаг 1: пользователь выбирает модель."""
    if not await check_ai_limit(callback.from_user.id):
        await callback.answer(
            "Лимит исчерпан: 1 AI-запрос в день на Free-тарифе. "
            "Обновитесь до Pro!",
            show_alert=True
        )
        return
    await state.clear()
    current = await get_user_llm_model(callback.from_user.id)
    label = LLM_MODELS.get(current, LLM_DEFAULT_MODEL)
    custom_api = await has_active_custom_llm_api(callback.from_user.id)
    text = (
        f"{emoji('AI')} <b>AI Генератор текста</b>\n\n"
        f"{emoji('INFO')} Шаг 1 из 2. Выбери модель для генерации. "
        f"На шаге 2 опишешь задачу — бот пришлёт "
        f"<b>3 разных варианта</b> готового текста.\n\n"
        f"Текущая модель: <code>{escape(label if not custom_api else current)}</code>"
    )
    if custom_api:
        builder = InlineKeyboardBuilder()
        builder.row(InlineKeyboardButton(text=f"Использовать {current}", callback_data='llm_choose_custom',
                                         style='primary', icon_custom_emoji_id=get_icon('AI')))
        builder.row(InlineKeyboardButton(text='Настройки API', callback_data='ai_api_settings', style='default',
                                         icon_custom_emoji_id=get_icon('GEAR')))
        builder.row(InlineKeyboardButton(text='Назад', callback_data='functions', style='default',
                                         icon_custom_emoji_id=get_icon('BACK')))
        markup = builder.as_markup()
    else:
        markup = get_llm_model_pick_keyboard(current, include_back=True)
    await present_section(callback.message, 'ai', text, markup, replace=True)
    await state.set_state(LLMStates.choosing_model)
    await callback.answer()


@dp.callback_query(F.data == 'ai_chat')
async def ai_chat_start(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(AIChatStates.waiting_for_message)
    await callback.message.edit_text(
        await render_ai_chat_screen(callback.from_user.id),
        reply_markup=get_ai_chat_keyboard(),
    )
    await callback.answer()


@dp.message(AIChatStates.waiting_for_message)
async def ai_chat_message(message: Message, state: FSMContext):
    user_id = message.from_user.id
    prompt = (message.text or '').strip()
    if not prompt:
        await message.answer('Отправьте сообщение текстом.')
        return
    if len(prompt) > 4000:
        await message.answer('Сообщение слишком длинное: максимум 4000 символов.')
        return

    allowed, used, limit = await reserve_ai_chat_request(user_id)
    if not allowed:
        await message.answer(
            f"{emoji('INFO')} Дневной лимит чата исчерпан: <b>{used}/{limit}</b>.\n"
            "Лимит обновится завтра по МСК.",
            reply_markup=get_ai_chat_keyboard(),
        )
        return

    history = await get_ai_chat_history(user_id)
    messages = history + [{'role': 'user', 'content': prompt}]
    model = await get_user_llm_model(user_id)
    thinking = await message.answer(
        f"{emoji('LOADING')} <b>Думаю…</b>\n"
        f"Модель: <code>{escape(str(LLM_MODELS.get(model, model)))}</code>",
    )
    try:
        answer = await call_llm_api_with_history(
            AI_CHAT_SYSTEM_PROMPT,
            messages,
            user_id=user_id,
            model=model,
            max_tokens=1200,
        )
    except Exception as ex:
        await release_ai_chat_request(user_id)
        logger.exception('AI chat request failed')
        await thinking.edit_text(
            f"{emoji('CROSS')} <b>Не удалось получить ответ.</b>\n\n"
            f"<code>{escape(str(ex)[:700])}</code>",
            reply_markup=get_ai_chat_keyboard(),
        )
        return

    answer = (answer or '').strip()
    if not answer:
        await release_ai_chat_request(user_id)
        await thinking.edit_text(
            f"{emoji('CROSS')} Модель вернула пустой ответ. Лимит не был потрачен.",
            reply_markup=get_ai_chat_keyboard(),
        )
        return

    messages.append({'role': 'assistant', 'content': answer})
    try:
        await save_ai_chat_history(user_id, messages)
    except Exception:
        logger.exception('Could not save AI chat history')

    chunks = _split_ai_chat_answer(answer)
    first = escape(chunks[0]) if chunks else '—'
    await thinking.edit_text(first, reply_markup=get_ai_chat_keyboard())
    for chunk in chunks[1:]:
        await message.answer(escape(chunk))
    await message.answer(
        f"Запросов сегодня: <b>{used}/{limit}</b>. Отправьте следующее сообщение.",
        reply_markup=get_ai_chat_keyboard(),
    )
    await state.set_state(AIChatStates.waiting_for_message)


@dp.callback_query(F.data == 'ai_chat_clear')
async def ai_chat_clear(callback: CallbackQuery, state: FSMContext):
    await clear_ai_chat_history(callback.from_user.id)
    await state.set_state(AIChatStates.waiting_for_message)
    screen = await render_ai_chat_screen(callback.from_user.id)
    await callback.message.edit_text(
        f"{emoji('CHECK')} История диалога очищена.\n\n{screen}",
        reply_markup=get_ai_chat_keyboard(),
    )
    await callback.answer('История очищена')


@dp.callback_query(F.data == 'ai_chat_exit')
async def ai_chat_exit(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    limits = await format_limits_text(callback.from_user.id)
    await callback.message.edit_text(
        f"{emoji('SMILE')} <b>Главное меню</b>\n\n{limits}\n\nВыберите действие:",
        reply_markup=get_main_menu_keyboard(),
    )
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
        f"{emoji('CLOCK')} Запрос #{request_id or '—'} · "
        f"{escape(LLM_MODELS.get(user_model, user_model))}"
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
    runtime = await get_global_llm_runtime()
    await callback.message.edit_text(
        f"{emoji('BOT')} <b>Выбор модели</b>\n\n"
        f"Текущая: <code>{escape(label)}</code>\n\n"
        f"{emoji('INFO')} Используется официальный Anthropic SDK, "
        f"базовый API: <code>{escape(str(runtime['base_url']))}</code>.",
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
CONFIRMED_INVALID_SESSION_ERROR_NAMES = {
    'AuthKeyUnregisteredError',
    'SessionRevokedError',
    'UserDeactivatedError',
    'UserDeactivatedBanError',
}


def _is_confirmed_invalid_session_error(ex: Exception) -> bool:
    """Только ошибки Telegram об отозванной сессии разрешают автоудаление."""
    if ex.__class__.__name__ in CONFIRMED_INVALID_SESSION_ERROR_NAMES:
        return True
    code = str(ex).upper()
    return any(marker in code for marker in (
        'AUTH_KEY_UNREGISTERED', 'SESSION_REVOKED',
        'USER_DEACTIVATED', 'USER_DEACTIVATED_BAN',
    ))


async def validate_account(account_id: int, user_id: int) -> Dict[str, Any]:
    """Проверяет авторизацию, не удаляя аккаунт при кратковременной ошибке сети.

    `removable=True` выставляется только если Telegram явно подтвердил, что
    сессия больше не авторизована. Это отличает невалидную сессию от сбоя
    прокси/сети и защищает рабочие аккаунты от ошибочного удаления.
    """
    account = await get_account(account_id)
    if not account or account.get('user_id') != user_id:
        return {
            'valid': False,
            'removable': False,
            'status': 'missing',
            'error': 'Аккаунт не найден',
        }

    client: Optional[TelegramClient] = active_clients.get(account_id)
    owns_probe = False
    premium = False
    username = ''
    result: Dict[str, Any] = {
        'valid': False,
        'removable': False,
        'status': 'check_error',
        'error': '',
        'premium': False,
        'username': '',
    }
    try:
        if client is None:
            proxy = await get_proxy(account['proxy_id']) if account.get('proxy_id') else None
            fingerprint = await get_account_fingerprint(account_id)
            if not fingerprint:
                fingerprint = await regenerate_account_fingerprint(account_id)
            client = await create_telethon_client(
                account['session_string'], proxy=proxy, fingerprint=fingerprint,
            )
            await client.connect()
            owns_probe = True
        elif not client.is_connected():
            await client.connect()

        authorized = await client.is_user_authorized()
        if not authorized:
            result.update({
                'status': 'invalid',
                'removable': True,
                'error': 'Сессия Telegram больше не авторизована.',
            })
        else:
            me = await client.get_me()
            premium = bool(getattr(me, 'premium', False))
            username = getattr(me, 'username', None) or ''
            result.update({
                'valid': True,
                'status': 'valid',
                'premium': premium,
                'username': username,
            })
    except RPCError as ex:
        if _is_confirmed_invalid_session_error(ex):
            result.update({
                'status': 'invalid',
                'removable': True,
                'error': str(ex) or 'Telegram отозвал авторизацию сессии.',
            })
        else:
            result['error'] = str(ex)[:1000]
    except Exception as ex:
        # Ошибка подключения не доказывает, что сессия умерла: оставляем
        # аккаунт и повторим проверку в следующий плановый час.
        result['error'] = str(ex)[:1000]
    finally:
        if owns_probe and client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass

    async with db_pool.acquire() as conn:
        if result['valid']:
            await conn.execute(
                '''UPDATE accounts SET is_active = TRUE, telegram_premium = $1,
                   validation_status = 'valid', last_validated_at = NOW()
                   WHERE id = $2''',
                premium, account_id,
            )
        elif result['removable']:
            await conn.execute(
                '''UPDATE accounts SET is_active = FALSE, telegram_premium = FALSE,
                   validation_status = 'invalid', last_validated_at = NOW()
                   WHERE id = $1''',
                account_id,
            )
        else:
            await conn.execute(
                '''UPDATE accounts SET validation_status = 'check_error',
                   last_validated_at = NOW() WHERE id = $1''',
                account_id,
            )
    return result


@dp.callback_query(F.data.startswith('validate_account_'))
async def validate_account_handler(callback: CallbackQuery):
    account_id = int(callback.data.rsplit('_', 1)[1])
    await callback.answer('Проверяю подключение...')
    result = await validate_account(account_id, callback.from_user.id)
    if not result['valid']:
        if result.get('removable'):
            title = 'Сессия невалидна'
            fallback = 'Требуется повторная авторизация.'
        else:
            title = 'Не удалось подтвердить валидность'
            fallback = 'Проверьте подключение или прокси и попробуйте позже.'
        await callback.message.edit_text(
            f"{emoji('CROSS')} <b>{title}</b>\n\n"
            f"{escape(result.get('error') or fallback)}",
            reply_markup=get_account_actions_keyboard(account_id)
        )
        return
    premium_text = 'Telegram Premium активен' if result.get('premium') else 'Telegram Premium отсутствует'
    await callback.message.edit_text(
        f"{emoji('CHECK')} <b>Аккаунт валиден</b>\n\n"
        f"Username: <code>@{escape(result.get('username') or 'нет')}</code>\n"
        f"{emoji('STAR') if result.get('premium') else emoji('INFO')} {premium_text}",
        reply_markup=get_account_actions_keyboard(account_id)
    )


def get_spam_check_keyboard(
    account_id: int, settings: Dict[str, Any],
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text='Проверить сейчас',
        callback_data=f'spam_check_run:{account_id}',
        style='primary',
        icon_custom_emoji_id=get_icon('REFRESH'),
    ))
    notifications_on = bool(settings.get('notify_enabled', True))
    builder.row(InlineKeyboardButton(
        text=(
            'Отключить уведомления'
            if notifications_on else 'Включить уведомления'
        ),
        callback_data=f'spam_check_toggle_notify:{account_id}',
        style='default',
        icon_custom_emoji_id=get_icon('BELL'),
    ))
    auto_check_on = bool(settings.get('is_enabled', True))
    builder.row(InlineKeyboardButton(
        text=(
            'Выключить автопроверку'
            if auto_check_on else 'Включить автопроверку'
        ),
        callback_data=f'spam_check_toggle_auto:{account_id}',
        style='default',
        icon_custom_emoji_id=get_icon('CLOCK'),
    ))
    builder.row(InlineKeyboardButton(
        text='Назад к аккаунту',
        callback_data=f'manage_account_{account_id}',
        style='default',
        icon_custom_emoji_id=get_icon('BACK'),
    ))
    return builder.as_markup()


def render_spam_check_screen(
    account: Dict[str, Any], settings: Dict[str, Any],
) -> str:
    enabled = bool(settings.get('is_enabled', True))
    notify_enabled = bool(settings.get('notify_enabled', True))
    last_checked = settings.get('last_checked_at')
    status = settings.get('last_status')
    status_label = SPAM_BLOCK_STATUS_LABELS.get(
        status, 'ещё не проверялось'
    )

    schedule_line = 'выключена'
    if enabled:
        schedule_line = 'включена: раз в 12 часов'
        if last_checked and hasattr(last_checked, '__add__'):
            try:
                schedule_line += (
                    f"\nСледующая проверка после: "
                    f"{_format_msk_datetime(last_checked + timedelta(seconds=SPAM_BLOCK_CHECK_INTERVAL_SECONDS))}"
                )
            except Exception:
                pass

    response = (settings.get('last_response') or settings.get('last_error') or '').strip()
    if len(response) > SPAM_BLOCK_RESPONSE_LIMIT:
        response = response[:SPAM_BLOCK_RESPONSE_LIMIT - 1].rstrip() + '…'
    response_block = (
        f"\n\n<b>Последний ответ @SpamBot:</b>\n<i>{escape(response)}</i>"
        if response else ''
    )
    return (
        f"{emoji('BELL')} <b>Проверка спамблока</b>\n\n"
        f"{emoji('PHONE')} Аккаунт: <code>{escape(str(account.get('phone') or account.get('id')))}</code>\n"
        f"Статус: <b>{escape(status_label)}</b>\n"
        f"Последняя проверка: <b>{_format_msk_datetime(last_checked)}</b>\n\n"
        f"{emoji('CLOCK')} Автопроверка: <b>{schedule_line}</b>\n"
        f"{emoji('BELL')} Уведомления: <b>{'включены' if notify_enabled else 'выключены'}</b>"
        f"{response_block}"
    )


async def _get_owned_spam_check_account(
    callback: CallbackQuery, account_id: int,
) -> Optional[Dict[str, Any]]:
    account = await get_account(account_id)
    if not account or account.get('user_id') != callback.from_user.id:
        await callback.answer('Аккаунт не найден', show_alert=True)
        return None
    return account


@dp.callback_query(F.data.startswith('spam_check_menu:'))
async def spam_check_menu(callback: CallbackQuery):
    try:
        account_id = int(callback.data.split(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректный аккаунт', show_alert=True)
        return
    account = await _get_owned_spam_check_account(callback, account_id)
    if not account:
        return
    settings = await get_account_spam_check_settings(
        account_id, callback.from_user.id,
    )
    await callback.message.edit_text(
        render_spam_check_screen(account, settings),
        reply_markup=get_spam_check_keyboard(account_id, settings),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith('spam_check_run:'))
async def spam_check_run(callback: CallbackQuery):
    try:
        account_id = int(callback.data.split(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректный аккаунт', show_alert=True)
        return
    account = await _get_owned_spam_check_account(callback, account_id)
    if not account:
        return

    await callback.answer('Проверяю @SpamBot…')
    try:
        await callback.message.edit_text(
            f"{emoji('LOADING')} <b>Проверяю ограничения через @SpamBot…</b>\n\n"
            f"{emoji('PHONE')} Аккаунт: <code>{escape(str(account.get('phone') or account_id))}</code>",
        )
    except Exception:
        pass

    await check_account_spam_block(account_id, callback.from_user.id, notify=False)
    settings = await get_account_spam_check_settings(
        account_id, callback.from_user.id,
    )
    await callback.message.edit_text(
        render_spam_check_screen(account, settings),
        reply_markup=get_spam_check_keyboard(account_id, settings),
    )


@dp.callback_query(F.data.startswith('spam_check_toggle_notify:'))
async def spam_check_toggle_notify(callback: CallbackQuery):
    try:
        account_id = int(callback.data.split(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректный аккаунт', show_alert=True)
        return
    account = await _get_owned_spam_check_account(callback, account_id)
    if not account:
        return
    settings = await get_account_spam_check_settings(account_id, callback.from_user.id)
    settings = await update_account_spam_check_settings(
        account_id,
        callback.from_user.id,
        notify_enabled=not bool(settings.get('notify_enabled', True)),
    )
    await callback.message.edit_text(
        render_spam_check_screen(account, settings),
        reply_markup=get_spam_check_keyboard(account_id, settings),
    )
    await callback.answer(
        'Уведомления включены' if settings.get('notify_enabled') else 'Уведомления выключены'
    )


@dp.callback_query(F.data.startswith('spam_check_toggle_auto:'))
async def spam_check_toggle_auto(callback: CallbackQuery):
    try:
        account_id = int(callback.data.split(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректный аккаунт', show_alert=True)
        return
    account = await _get_owned_spam_check_account(callback, account_id)
    if not account:
        return
    settings = await get_account_spam_check_settings(account_id, callback.from_user.id)
    settings = await update_account_spam_check_settings(
        account_id,
        callback.from_user.id,
        is_enabled=not bool(settings.get('is_enabled', True)),
    )
    await callback.message.edit_text(
        render_spam_check_screen(account, settings),
        reply_markup=get_spam_check_keyboard(account_id, settings),
    )
    await callback.answer(
        'Автопроверка включена' if settings.get('is_enabled') else 'Автопроверка выключена'
    )


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

    # --- Прокси ---
    proxy_line = "не привязан"
    has_proxy = False
    proxy_status_badge = ""
    if account.get('proxy_id'):
        proxy = await get_proxy(account['proxy_id'])
        if proxy:
            label = proxy.get('label') or f"{proxy['host']}:{proxy['port']}"
            proxy_line = f"<code>{proxy['proxy_type']} {label}</code>"
            has_proxy = True
            proxy_status_badge = " ✅"
        else:
            proxy_line = "<i>удалён</i>"

    # --- Прогрев ---
    cycles = account.get('warming_cycles') or 0
    last_active = account.get('warming_last_active')
    if account.get('warming_enabled'):
        warming_status = "включён"
        if cycles:
            last_str = (
                last_active.astimezone(MSK_TZ).strftime('%d.%m %H:%M')
                if last_active else "—"
            )
            warming_line = f"включён — {cycles} цикл(ов), последний: {last_str}"
        else:
            warming_line = "включён, ещё не запускался"
    else:
        warming_line = f"выключен" + (f" ({cycles} цикл(ов) всего)" if cycles else "")

    # --- Активность за 24ч / 7д ---
    async with db_pool.acquire() as conn:
        sent_today = await conn.fetchval(
            "SELECT COUNT(*) FROM account_logs "
            "WHERE account_id=$1 AND direction='outgoing' AND created_at > NOW()-INTERVAL '24 hours'",
            account_id
        ) or 0
        sent_week = await conn.fetchval(
            "SELECT COUNT(*) FROM account_logs "
            "WHERE account_id=$1 AND direction='outgoing' AND created_at > NOW()-INTERVAL '7 days'",
            account_id
        ) or 0
        flood_week = await conn.fetchval(
            "SELECT COUNT(*) FROM flood_wait_history "
            "WHERE account_id=$1 AND occurred_at > NOW()-INTERVAL '7 days'",
            account_id
        ) or 0
        flood_sum = await conn.fetchval(
            "SELECT COALESCE(SUM(seconds),0) FROM flood_wait_history "
            "WHERE account_id=$1 AND occurred_at > NOW()-INTERVAL '7 days'",
            account_id
        ) or 0
        last_log = await conn.fetchrow(
            "SELECT created_at, direction, chat_name FROM account_logs "
            "WHERE account_id=$1 ORDER BY created_at DESC LIMIT 1",
            account_id
        )
        # ИИ-автоответчик
        ai_row = await conn.fetchrow(
            "SELECT mode, system_prompt FROM account_ai_responder WHERE account_id=$1",
            account_id
        )
        spam_row = await conn.fetchrow(
            "SELECT is_enabled, notify_enabled, last_checked_at, last_status "
            "FROM account_spam_checks WHERE account_id = $1",
            account_id,
        )
        cooldown_row = await conn.fetchrow(
            "SELECT action, cooldown_until FROM account_action_cooldowns "
            "WHERE account_id = $1 AND cooldown_until > NOW() "
            "ORDER BY cooldown_until DESC LIMIT 1",
            account_id,
        )
        monitoring_row = await conn.fetchrow(
            "SELECT last_validity_check_at, last_validity_status, "
            "last_ai_analysis_at, last_ai_analysis_source "
            "FROM account_monitoring_state WHERE account_id = $1",
            account_id,
        )

    # --- Риск-скор ---
    if flood_week == 0:
        risk_score = 0
        risk_label = "🟢 Низкий"
    elif flood_week < 3:
        risk_score = min(20 + flood_sum // 60, 40)
        risk_label = "🟡 Умеренный"
    elif flood_week < 6:
        risk_score = 40 + min(flood_sum // 30, 30)
        risk_label = "🟠 Повышенный"
    else:
        risk_score = min(70 + flood_sum // 20, 100)
        risk_label = "🔴 Высокий"

    # --- ИИ-автоответчик ---
    if ai_row:
        mode = ai_row['mode'] or 'off'
        ai_line = "ИИ активен" if mode == 'ai' else "выключен"
        if mode == 'ai' and ai_row.get('system_prompt'):
            ai_preview = (ai_row['system_prompt'] or '')[:40].replace('\n', ' ')
            ai_line += f" — <i>{escape(ai_preview)}…</i>"
    else:
        ai_line = "не настроен"

    # --- Автопроверка ограничений ---
    if spam_row:
        spam_data = dict(spam_row)
        if spam_data.get('is_enabled'):
            spam_status = SPAM_BLOCK_STATUS_LABELS.get(
                spam_data.get('last_status'), 'ещё не проверялось'
            )
            notify_state = 'уведомления вкл.' if spam_data.get('notify_enabled') else 'уведомления выкл.'
            spam_line = f"{spam_status} ({notify_state})"
        else:
            spam_line = 'автопроверка выключена'
    else:
        spam_line = 'включена, первая проверка ожидается'

    # --- Текущий FloodWait cooldown ---
    if cooldown_row:
        cooldown_data = dict(cooldown_row)
        action_label = (
            'создание каналов/групп'
            if cooldown_data.get('action') == CHAT_CREATION_FLOOD_ACTION
            else str(cooldown_data.get('action') or 'действие')
        )
        remaining = cooldown_remaining_seconds(cooldown_data['cooldown_until'])
        cooldown_line = (
            f"{action_label}: ещё {remaining} сек. "
            f"(до {_format_msk_datetime(cooldown_data['cooldown_until'], '—')})"
        )
    else:
        cooldown_line = 'нет активных ограничений'

    # --- Плановый мониторинг ---
    if monitoring_row:
        monitor_data = dict(monitoring_row)
        validity_status = monitor_data.get('last_validity_status') or 'ещё не проверялась'
        validity_when = _format_msk_datetime(
            monitor_data.get('last_validity_check_at'), '—'
        )
        analysis_when = _format_msk_datetime(
            monitor_data.get('last_ai_analysis_at'), 'ещё не выполнялся'
        )
        source = monitor_data.get('last_ai_analysis_source')
        source_label = 'AI' if source == 'llm' else ('эвристика' if source else '—')
        monitoring_line = (
            f"валидность: {validity_status} ({validity_when}); "
            f"AI-анализ: {analysis_when} ({source_label})"
        )
    else:
        monitoring_line = 'ожидается первая почасовая проверка и AI-анализ'

    # --- Последнее действие ---
    if last_log:
        direction_icon = "→" if last_log['direction'] == 'outgoing' else "←"
        last_action_str = (
            f"{last_log['created_at'].astimezone(MSK_TZ).strftime('%d.%m %H:%M')} "
            f"{direction_icon} <b>{escape(last_log['chat_name'] or '—')}</b>"
        )
    else:
        last_action_str = "нет данных"

    # --- Отпечаток ---
    fingerprint = await get_account_fingerprint(account_id)
    fp_line = (
        f"<code>{escape(fingerprint.get('device_model','—'))}</code> / "
        f"<code>{escape(fingerprint.get('system_version','—'))}</code>"
        if fingerprint else "не задан"
    )

    status_icon = "🟢" if account['is_active'] else "🔴"

    premium_badge = (
        f"{emoji('STAR')} Telegram Premium: <b>активен</b>\n"
        if account.get('telegram_premium') else ""
    )
    validation_str = escape(account.get('validation_status') or 'unknown')
    last_check_str = (
        account['last_validated_at'].strftime('%d.%m.%Y %H:%M')
        if account.get('last_validated_at') else "ещё не выполнялась"
    )

    text = (
        f"{emoji('PROFILE')} <b>Аккаунт {escape(account['phone'])}</b>\n"
        f"{'─' * 30}\n"
        f"{status_icon} Статус: <b>{'Активен' if account['is_active'] else 'Неактивен'}</b>\n"
        f"{premium_badge}"
        f"{emoji('CHECK')} Проверка: <b>{validation_str}</b> (последняя: {last_check_str})\n"
        f"{emoji('BELL')} Спамблок: <b>{escape(spam_line)}</b>\n"
        f"{emoji('CLOCK')} FloodWait: <b>{escape(cooldown_line)}</b>\n"
        f"{emoji('AI')} Мониторинг: <b>{escape(monitoring_line)}</b>\n"
        f"{emoji('CLOCK')} Добавлен: <b>{account['created_at'].strftime('%d.%m.%Y %H:%M')}</b>\n\n"
        f"{emoji('LINK')} Прокси: {proxy_line}{proxy_status_badge}\n"
        f"{emoji('PHONE')} Отпечаток: {fp_line}\n\n"
        f"{emoji('FIRE')} Прогрев: {warming_line}\n\n"
        f"{emoji('CHART')} <b>Активность:</b>\n"
        f"  • отправлено за 24ч: <b>{sent_today}</b>\n"
        f"  • отправлено за 7д: <b>{sent_week}</b>\n"
        f"  • FloodWait за 7д: <b>{flood_week}</b> шт. (<b>{flood_sum}с</b>)\n"
        f"  • Риск-скор: <b>{risk_score}/100</b> — {risk_label}\n\n"
        f"{emoji('CLOCK')} Последнее действие: {last_action_str}\n"
        f"{emoji('AI')} ИИ-автоответчик: {ai_line}"
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
    if not await is_pro(callback.from_user.id):
        await callback.answer(
            "AI-анализ безопасности доступен только в Pro.",
            show_alert=True
        )
        return
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


# ============================================================
#  Редактирование профиля Telegram-аккаунта
# ============================================================
# Профиль (аватар, имя, фамилия, описание) хранится в самом
# Telegram, а не в нашей БД. Поэтому:
#   - при открытии редактора читаем актуальные данные из Telegram;
#   - все правки складываем в черновик внутри FSM;
#   - применяем к аккаунту только по кнопке «Сохранить».
# Текстовые поля можно сгенерировать через DeepSeek (модель
# deepseek-v4-flash), аватар генерацией не затрагивается.

PROFILE_FIRST_NAME_LIMIT = 64
PROFILE_LAST_NAME_LIMIT = 64
PROFILE_ABOUT_LIMIT = 70
PROFILE_AI_MODEL = 'deepseek-v4-flash'


def _profile_display(value: Optional[str], empty: str = "—") -> str:
    value = (value or '').strip()
    if not value:
        return empty
    return escape(value)


def _profile_editor_keyboard(account_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="Имя",
            callback_data=f"profedit_field_first:{account_id}",
            style='default',
            icon_custom_emoji_id=get_icon("WRITE"),
        ),
        InlineKeyboardButton(
            text="Фамилия",
            callback_data=f"profedit_field_last:{account_id}",
            style='default',
            icon_custom_emoji_id=get_icon("WRITE"),
        ),
    )
    builder.row(InlineKeyboardButton(
        text="Описание",
        callback_data=f"profedit_field_about:{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("ADD_TEXT"),
    ))
    builder.row(InlineKeyboardButton(
        text="Аватарка",
        callback_data=f"profedit_field_avatar:{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("MEDIA"),
    ))
    builder.row(InlineKeyboardButton(
        text="Сгенерировать через ИИ",
        callback_data=f"profedit_ai:{account_id}",
        style='primary',
        icon_custom_emoji_id=get_icon("AI"),
    ))
    builder.row(InlineKeyboardButton(
        text="Сохранить",
        callback_data=f"profedit_save:{account_id}",
        style='success',
        icon_custom_emoji_id=get_icon("CHECK"),
    ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data=f"profedit_cancel:{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("BACK"),
    ))
    return builder.as_markup()


def _profile_editor_text(data: Dict[str, Any]) -> str:
    if data.get('draft_avatar'):
        avatar_note = "новая — превью выше 👆"
    elif data.get('current_avatar'):
        avatar_note = "текущая — превью выше 👆"
    else:
        avatar_note = "не установлена"
    changed = []
    if (data.get('draft_first_name') or '') != (data.get('orig_first_name') or ''):
        changed.append("имя")
    if (data.get('draft_last_name') or '') != (data.get('orig_last_name') or ''):
        changed.append("фамилия")
    if (data.get('draft_about') or '') != (data.get('orig_about') or ''):
        changed.append("описание")
    if data.get('draft_avatar'):
        changed.append("аватар")
    changed_line = (
        f"\n{emoji('WRITE')} Не сохранено: <b>{', '.join(changed)}</b>"
        if changed else ""
    )
    return (
        f"{emoji('PROFILE')} <b>Изменение профиля</b>\n"
        f"{emoji('PHONE')} <code>{escape(data.get('phone') or '—')}</code>\n\n"
        f"{emoji('ID')} Имя: <b>{_profile_display(data.get('draft_first_name'))}</b>\n"
        f"{emoji('ID')} Фамилия: <b>{_profile_display(data.get('draft_last_name'))}</b>\n"
        f"{emoji('ADD_TEXT')} Описание: "
        f"{_profile_display(data.get('draft_about'), 'пусто')}\n"
        f"{emoji('MEDIA')} Аватарка: {avatar_note}"
        f"{changed_line}\n\n"
        f"{emoji('INFO')} Отредактируйте поля вручную или сгенерируйте "
        f"текст через ИИ, затем нажмите «Сохранить»."
    )


async def _fetch_profile_from_telegram(
    account_id: int,
) -> Optional[Dict[str, Optional[str]]]:
    """Читает актуальные имя/фамилию/описание из Telegram.

    Возвращает None, если аккаунт не авторизован / не подключается.
    """
    client = await get_client_for_account(account_id)
    if not client:
        return None
    me = await client.get_me()
    about = ''
    try:
        full = await client(GetFullUserRequest(id='me'))
        about = getattr(full.full_user, 'about', '') or ''
    except Exception as ex:
        logger.warning("GetFullUserRequest failed: %s", ex)
    avatar = None
    try:
        avatar = await client.download_profile_photo('me', file=bytes)
    except Exception as ex:
        logger.warning("download_profile_photo failed: %s", ex)
    return {
        'first_name': getattr(me, 'first_name', '') or '',
        'last_name': getattr(me, 'last_name', '') or '',
        'about': about,
        'avatar': avatar,
    }


async def _render_profile_editor(
    target: Any, state: FSMContext, edit: bool = True
) -> None:
    data = await state.get_data()
    account_id = data.get('profile_account_id')
    text = _profile_editor_text(data)
    markup = _profile_editor_keyboard(account_id)
    await state.set_state(ProfileEditStates.editing)
    # Чистим следы промпта — после возврата в редактор он не нужен.
    await state.update_data(
        prompt_chat_id=None,
        prompt_message_id=None,
    )
    if edit and isinstance(target, CallbackQuery):
        try:
            await target.message.edit_text(text, reply_markup=markup)
            return
        except Exception:
            await target.message.answer(text, reply_markup=markup)
            return
    msg = target.message if isinstance(target, CallbackQuery) else target
    await msg.answer(text, reply_markup=markup)


@dp.callback_query(F.data.startswith("edit_profile_"))
async def edit_profile_open(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[2])
    account = await get_account(account_id)
    if not account or int(account['user_id']) != int(callback.from_user.id):
        await callback.answer("Аккаунт не найден", show_alert=True)
        return

    await callback.answer()
    # Сразу гасим кнопки на исходном сообщении и превращаем его в «Загружаю…».
    # Это сообщение потом либо удалим, либо заменим на ошибку.
    try:
        loading_msg = await callback.message.edit_text(
            f"{emoji('LOADING')} <b>Загружаю профиль из Telegram…</b>",
            reply_markup=None,
        )
    except Exception:
        loading_msg = None

    profile = await _fetch_profile_from_telegram(account_id)
    if profile is None:
        # Сообщаем об ошибке и убираем «Загружаю…» из чата.
        error_text = (
            f"{emoji('CROSS')} Не удалось подключиться к аккаунту. "
            f"Возможно, сессия недействительна."
        )
        error_kb = InlineKeyboardBuilder().row(InlineKeyboardButton(
            text="Назад",
            callback_data=f"manage_account_{account_id}",
            style='default',
            icon_custom_emoji_id=get_icon("BACK"),
        )).as_markup()
        if loading_msg is not None:
            try:
                await loading_msg.edit_text(error_text, reply_markup=error_kb)
            except Exception:
                await callback.message.answer(error_text, reply_markup=error_kb)
        else:
            await callback.message.answer(error_text, reply_markup=error_kb)
        return

    await state.set_state(ProfileEditStates.editing)
    await state.update_data(
        profile_account_id=account_id,
        phone=account.get('phone'),
        orig_first_name=profile['first_name'],
        orig_last_name=profile['last_name'],
        orig_about=profile['about'],
        draft_first_name=profile['first_name'],
        draft_last_name=profile['last_name'],
        draft_about=profile['about'],
        draft_avatar=None,
        current_avatar=profile.get('avatar'),
    )

    # Прячем «Загружаю…» — оно своё отработало.
    if loading_msg is not None:
        try:
            await loading_msg.delete()
        except Exception:
            pass

    # Сначала отправляем превью текущей аватарки (если она есть), затем
    # отдельным сообщением — сам редактор. Так пользователь видит связку
    # «картинка + кнопки под ней» и понимает, что превью относится к редактору.
    if profile.get('avatar'):
        try:
            await callback.message.answer_photo(
                BufferedInputFile(
                    profile['avatar'], filename=f"profile_{account_id}.jpg"
                ),
                caption=(
                    f"{emoji('MEDIA')} <b>Текущая аватарка аккаунта</b>\n"
                    f"{emoji('INFO')} Это то, что сейчас стоит в Telegram. "
                    f"Ниже — редактор."
                ),
            )
        except Exception as ex:
            logger.warning("send current profile photo failed: %s", ex)

    await _render_profile_editor(callback, state, edit=False)


async def _guard_profile_owner(
    event: Any, state: FSMContext
) -> Optional[int]:
    """Проверяет, что аккаунт из FSM всё ещё принадлежит юзеру.

    FSM-хранилище (особенно Redis) сериализует все значения в строки,
    поэтому и account_id, и user_id приводим к int перед сравнением.

    Если состояние потерялось (рестарт бота, таймаут сессии и т.п.),
    пробуем вытащить account_id из callback_data — все кнопки редактора
    уже содержат его в формате ``profedit_*:<id>``.
    """
    data = await state.get_data()
    raw_id = data.get('profile_account_id')

    # Фолбэк: ищем account_id в callback_data вида "profedit_*:123".
    if not raw_id and isinstance(event, CallbackQuery):
        cd = event.data or ''
        if ':' in cd:
            tail = cd.rsplit(':', 1)[-1]
            if tail.isdigit():
                raw_id = tail

    if not raw_id:
        return None
    try:
        account_id = int(raw_id)
    except (TypeError, ValueError):
        return None

    user_id = None
    if isinstance(event, (CallbackQuery, Message)):
        user_id = event.from_user.id

    if user_id is None:
        return None

    account = await get_account(account_id)
    if not account:
        return None
    try:
        if int(account['user_id']) != int(user_id):
            return None
    except (TypeError, ValueError):
        return None

    # Состояние могло потеряться — восстанавливаем минимум, чтобы
    # следующие шаги (ожидание текста/картинки) не падали.
    if not data.get('profile_account_id'):
        await state.update_data(profile_account_id=account_id)
    if isinstance(event, CallbackQuery):
        current_state = await state.get_state()
        if current_state is None:
            await state.set_state(ProfileEditStates.editing)

    return account_id


@dp.callback_query(F.data.startswith("profedit_field_"))
async def profile_edit_field(callback: CallbackQuery, state: FSMContext):
    account_id = await _guard_profile_owner(callback, state)
    if account_id is None:
        await callback.answer("Аккаунт не найден", show_alert=True)
        await state.clear()
        return

    field = callback.data.split("_")[2].split(":")[0]
    prompts = {
        'first': (
            ProfileEditStates.waiting_for_first_name,
            f"{emoji('WRITE')} Отправьте новое <b>имя</b> "
            f"(до {PROFILE_FIRST_NAME_LIMIT} символов).",
        ),
        'last': (
            ProfileEditStates.waiting_for_last_name,
            f"{emoji('WRITE')} Отправьте новую <b>фамилию</b> "
            f"(до {PROFILE_LAST_NAME_LIMIT} символов). "
            f"Отправьте «-», чтобы очистить.",
        ),
        'about': (
            ProfileEditStates.waiting_for_about,
            f"{emoji('ADD_TEXT')} Отправьте новое <b>описание</b> "
            f"(до {PROFILE_ABOUT_LIMIT} символов). "
            f"Отправьте «-», чтобы очистить.",
        ),
        'avatar': (
            ProfileEditStates.waiting_for_avatar,
            f"{emoji('MEDIA')} Отправьте <b>изображение</b> для новой "
            f"аватарки. Старые фото профиля будут удалены при сохранении.",
        ),
    }
    if field not in prompts:
        await callback.answer("Неизвестное поле", show_alert=True)
        return

    new_state, prompt = prompts[field]
    await state.set_state(new_state)
    # Запоминаем id сообщения с промптом, чтобы потом убирать его из чата,
    # когда пользователь пришлёт картинку/текст.
    await state.update_data(
        prompt_chat_id=callback.message.chat.id,
        prompt_message_id=callback.message.message_id,
    )
    cancel_kb = InlineKeyboardBuilder().row(InlineKeyboardButton(
        text="Отмена",
        callback_data=f"profedit_back:{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("BACK"),
    )).as_markup()
    await callback.message.edit_text(prompt, reply_markup=cancel_kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("profedit_cancel:"))
async def profile_edit_cancel(callback: CallbackQuery, state: FSMContext):
    account_id = await _guard_profile_owner(callback, state)
    if account_id is None:
        await callback.answer("Аккаунт не найден", show_alert=True)
        await state.clear()
        return
    await state.clear()
    callback.data = f"manage_account_{account_id}"
    await manage_account(callback)


@dp.callback_query(F.data.startswith("profedit_back:"))
async def profile_edit_back(callback: CallbackQuery, state: FSMContext):
    account_id = await _guard_profile_owner(callback, state)
    if account_id is None:
        await callback.answer("Аккаунт не найден", show_alert=True)
        await state.clear()
        return
    await callback.answer()
    await _render_profile_editor(callback, state, edit=True)


@dp.message(ProfileEditStates.waiting_for_first_name)
async def profile_set_first_name(message: Message, state: FSMContext):
    account_id = await _guard_profile_owner(message, state)
    if account_id is None:
        await message.answer(
            f"{emoji('CROSS')} Сессия редактора истекла. Откройте "
            f"«Изменить профиль» заново."
        )
        await state.clear()
        return
    name = (message.text or '').strip()
    if not name:
        await message.answer(
            f"{emoji('CROSS')} Имя не может быть пустым. Отправьте текст."
        )
        return
    if len(name) > PROFILE_FIRST_NAME_LIMIT:
        await message.answer(
            f"{emoji('CROSS')} Слишком длинное имя "
            f"(макс. {PROFILE_FIRST_NAME_LIMIT})."
        )
        return
    await state.update_data(draft_first_name=name)
    await _render_profile_editor(message, state, edit=False)


@dp.message(ProfileEditStates.waiting_for_last_name)
async def profile_set_last_name(message: Message, state: FSMContext):
    account_id = await _guard_profile_owner(message, state)
    if account_id is None:
        await message.answer(
            f"{emoji('CROSS')} Сессия редактора истекла. Откройте "
            f"«Изменить профиль» заново."
        )
        await state.clear()
        return
    text = (message.text or '').strip()
    value = '' if text == '-' else text
    if len(value) > PROFILE_LAST_NAME_LIMIT:
        await message.answer(
            f"{emoji('CROSS')} Слишком длинная фамилия "
            f"(макс. {PROFILE_LAST_NAME_LIMIT})."
        )
        return
    await state.update_data(draft_last_name=value)
    await _render_profile_editor(message, state, edit=False)


@dp.message(ProfileEditStates.waiting_for_about)
async def profile_set_about(message: Message, state: FSMContext):
    account_id = await _guard_profile_owner(message, state)
    if account_id is None:
        await message.answer(
            f"{emoji('CROSS')} Сессия редактора истекла. Откройте "
            f"«Изменить профиль» заново."
        )
        await state.clear()
        return
    text = (message.text or '').strip()
    value = '' if text == '-' else text
    if len(value) > PROFILE_ABOUT_LIMIT:
        await message.answer(
            f"{emoji('CROSS')} Слишком длинное описание "
            f"(макс. {PROFILE_ABOUT_LIMIT})."
        )
        return
    await state.update_data(draft_about=value)
    await _render_profile_editor(message, state, edit=False)


@dp.message(ProfileEditStates.waiting_for_avatar)
async def profile_set_avatar(message: Message, state: FSMContext):
    account_id = await _guard_profile_owner(message, state)
    if account_id is None:
        await message.answer(
            f"{emoji('CROSS')} Сессия редактора истекла. Откройте "
            f"«Изменить профиль» заново."
        )
        await state.clear()
        return

    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_id
    elif (
        message.document and (message.document.mime_type or '').startswith('image/')
    ):
        file_id = message.document.file_id

    if not file_id:
        await message.answer(
            f"{emoji('CROSS')} Пришлите изображение (фото или картинку-файл)."
        )
        return

    try:
        file = await bot.get_file(file_id)
        buffer = await bot.download_file(file.file_path)
        avatar_bytes = buffer.read()
    except Exception as ex:
        logger.warning("avatar download failed: %s", ex)
        await message.answer(
            f"{emoji('CROSS')} Не удалось загрузить изображение. Попробуйте ещё раз."
        )
        return

    await state.update_data(draft_avatar=avatar_bytes)
    # Убираем промпт «Отправьте изображение…» из чата — он больше не нужен.
    data = await state.get_data()
    prompt_chat_id = data.get('prompt_chat_id')
    prompt_message_id = data.get('prompt_message_id')
    if prompt_chat_id and prompt_message_id:
        try:
            await bot.delete_message(prompt_chat_id, prompt_message_id)
        except Exception:
            pass
    # Прячем присланное фото — превью покажем сами.
    try:
        await message.delete()
    except Exception:
        pass
    # Превью новой аватарки — отдельным сообщением, чтобы пользователь
    # видел, что именно он только что загрузил.
    try:
        await message.answer_photo(
            BufferedInputFile(avatar_bytes, filename=f"draft_{account_id}.jpg"),
            caption=(
                f"{emoji('MEDIA')} <b>Новая аватарка в черновике</b>\n"
                f"{emoji('INFO')} Нажмите «Сохранить», чтобы применить."
            ),
        )
    except Exception as ex:
        logger.warning("send draft avatar preview failed: %s", ex)
    await _render_profile_editor(message, state, edit=False)


def _parse_ai_profile_json(content: str) -> Optional[Dict[str, str]]:
    """Достаёт {first_name,last_name,about} из ответа LLM."""
    if not content:
        return None
    text = content.strip()
    # Вырезаем markdown-ограждение ```json ... ```
    fence = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
    if fence:
        text = fence.group(1)
    else:
        brace = re.search(r'\{.*\}', text, re.DOTALL)
        if brace:
            text = brace.group(0)
    try:
        data = json.loads(text)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    return {
        'first_name': str(data.get('first_name') or '').strip(),
        'last_name': str(data.get('last_name') or '').strip(),
        'about': str(data.get('about') or '').strip(),
    }


async def _generate_profile_with_ai(prompt: str, user_id: Optional[int] = None) -> Optional[Dict[str, str]]:
    """Генерирует имя/фамилию/описание через DeepSeek (deepseek-v4-flash)."""
    system = (
        "Ты помогаешь оформить профиль Telegram-аккаунта. "
        "На основе запроса пользователя придумай реалистичные имя, "
        "фамилию и короткое описание (about). "
        f"Имя — до {PROFILE_FIRST_NAME_LIMIT} символов, фамилия — до "
        f"{PROFILE_LAST_NAME_LIMIT} символов, описание — до "
        f"{PROFILE_ABOUT_LIMIT} символов. "
        "Верни СТРОГО JSON без пояснений и без markdown в формате: "
        '{"first_name": "...", "last_name": "...", "about": "..."}'
    )
    runtime_url, runtime_key, runtime_model = await get_user_llm_runtime(
        user_id, PROFILE_AI_MODEL
    )
    client = anthropic.AsyncAnthropic(
        api_key=runtime_key,
        base_url=runtime_url,
        timeout=LLM_TIMEOUT,
    )
    response = await client.messages.create(
        model=runtime_model,
        max_tokens=512,
        system=system,
        messages=[{'role': 'user', 'content': prompt}],
    )
    content = ''
    try:
        for block in (response.content or []):
            if getattr(block, 'type', None) == 'text':
                content = getattr(block, 'text', '') or content
    except Exception:
        content = ''
    parsed = _parse_ai_profile_json(content)
    if not parsed or not parsed.get('first_name'):
        return None
    parsed['first_name'] = parsed['first_name'][:PROFILE_FIRST_NAME_LIMIT]
    parsed['last_name'] = parsed['last_name'][:PROFILE_LAST_NAME_LIMIT]
    parsed['about'] = parsed['about'][:PROFILE_ABOUT_LIMIT]
    return parsed


@dp.callback_query(F.data.startswith("profedit_ai:"))
async def profile_ai_prompt(callback: CallbackQuery, state: FSMContext):
    if not await is_pro(callback.from_user.id):
        await callback.answer(
            "AI-генерация профиля доступна только в Pro.",
            show_alert=True
        )
        return
    account_id = await _guard_profile_owner(callback, state)
    if account_id is None:
        await callback.answer("Аккаунт не найден", show_alert=True)
        await state.clear()
        return
    await state.set_state(ProfileEditStates.waiting_for_ai_prompt)
    cancel_kb = InlineKeyboardBuilder().row(InlineKeyboardButton(
        text="Отмена",
        callback_data=f"profedit_back:{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("BACK"),
    )).as_markup()
    await callback.message.edit_text(
        f"{emoji('AI')} <b>Генерация профиля через ИИ</b>\n\n"
        f"Опишите желаемый образ (например: «серьёзный юрист из Москвы» "
        f"или «весёлый геймер-стример»). ИИ придумает имя, фамилию и "
        f"описание. Аватарку ИИ не меняет.",
        reply_markup=cancel_kb,
    )
    await callback.answer()


@dp.message(ProfileEditStates.waiting_for_ai_prompt)
async def profile_ai_generate(message: Message, state: FSMContext):
    account_id = await _guard_profile_owner(message, state)
    if account_id is None:
        await message.answer(
            f"{emoji('CROSS')} Сессия редактора истекла. Откройте "
            f"«Изменить профиль» заново."
        )
        await state.clear()
        return
    prompt = (message.text or '').strip()
    if not prompt:
        await message.answer(
            f"{emoji('CROSS')} Опишите желаемый образ текстом."
        )
        return

    thinking = await message.answer(
        f"{emoji('LOADING')} Генерирую профиль через DeepSeek…"
    )
    try:
        result = await _generate_profile_with_ai(prompt, message.from_user.id)
    except Exception as ex:
        logger.exception("profile AI generation failed")
        await thinking.edit_text(
            f"{emoji('CROSS')} Ошибка генерации: "
            f"<code>{escape(str(ex)[:200])}</code>"
        )
        await _render_profile_editor(message, state, edit=False)
        return

    if not result:
        await thinking.edit_text(
            f"{emoji('CROSS')} ИИ вернул некорректный ответ. Попробуйте ещё раз."
        )
        await _render_profile_editor(message, state, edit=False)
        return

    await state.update_data(
        draft_first_name=result['first_name'],
        draft_last_name=result['last_name'],
        draft_about=result['about'],
    )
    try:
        await thinking.delete()
    except Exception:
        pass
    await message.answer(f"{emoji('CHECK')} Профиль сгенерирован в черновик.")
    await _render_profile_editor(message, state, edit=False)


# Тексты ошибок Telethon, при которых можно безопасно повторить запрос.
# Telegram иногда отвечает «Try again later / client expired» при
# серии частых изменений профиля. Это не бан, просто лимит на частоту —
# через секунду-другую запрос проходит.
_PROFILE_RETRYABLE_MSG_FRAGMENTS = (
    "TAKEOUT_INIT_FAIL",
    "TAKEOUT_REQUIRED",
    "FLOOD_WAIT",
    "FLOOD_PREMIUM_WAIT",
    "PEER_FLOOD",
    "try again later",
    "try again",
    "client expired",
    "CLIENT_EXPIRED",
    "SLOWMODE_WAIT",
    "Timeout",
    "ConnectionResetError",
    "ConnectionError",
)


def _is_retryable_rpc_error(ex: Exception) -> bool:
    """True, если ошибка Telethon похожа на «попробуйте снова»."""
    if isinstance(ex, FloodWaitError):
        # Сам FloodWaitError обрабатываем отдельно: там есть seconds.
        return True
    if isinstance(ex, BadRequestError):
        msg = (str(ex.message) if getattr(ex, 'message', None) else str(ex)).lower()
        return any(frag.lower() in msg for frag in _PROFILE_RETRYABLE_MSG_FRAGMENTS)
    if isinstance(ex, RPCError):
        msg = (str(ex.message) if getattr(ex, 'message', None) else str(ex)).lower()
        return any(frag.lower() in msg for frag in _PROFILE_RETRYABLE_MSG_FRAGMENTS)
    if isinstance(ex, (TimeoutError, ConnectionError)):
        return True
    return False


async def _call_profile_rpc(client, request, *, retries: int = 3,
                            base_delay: float = 1.0, op_label: str = "rpc"):
    """Выполняет Telethon-RPC с ретраем на «попробуйте снова».

    Не ловит FloodWaitError — её должен обработать вызывающий, чтобы
    показать пользователю конкретный срок ожидания.
    """
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return await client(request)
        except FloodWaitError:
            raise
        except Exception as ex:  # noqa: BLE001
            last_exc = ex
            if not _is_retryable_rpc_error(ex) or attempt >= retries:
                raise
            # Экспоненциальная задержка: 1с, 2с, 4с...
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                "profile %s: %s (attempt %s/%s), retry in %.1fs",
                op_label, ex, attempt, retries, delay,
            )
            await asyncio.sleep(delay)
    # На случай, если цикл не сработал (не должно произойти):
    raise last_exc  # type: ignore[misc]


@dp.callback_query(F.data.startswith("profedit_save:"))
async def profile_edit_save(callback: CallbackQuery, state: FSMContext):
    account_id = await _guard_profile_owner(callback, state)
    if account_id is None:
        await callback.answer("Аккаунт не найден", show_alert=True)
        await state.clear()
        return

    data = await state.get_data()
    new_first = (data.get('draft_first_name') or '').strip()
    new_last = (data.get('draft_last_name') or '').strip()
    new_about = (data.get('draft_about') or '').strip()
    avatar_bytes = data.get('draft_avatar')

    orig_first = (data.get('orig_first_name') or '').strip()
    orig_last = (data.get('orig_last_name') or '').strip()
    orig_about = (data.get('orig_about') or '').strip()

    if not new_first:
        await callback.answer(
            "Имя не может быть пустым", show_alert=True
        )
        return

    # Сравниваем с оригиналом: апдейтим только то, что реально изменилось.
    # Передавать ВСЕ три поля в UpdateProfileRequest при любом чихе —
    # верный путь получить «TAKEOUT_INIT_FAIL» / «FLOOD_WAIT» /
    # «client expired» от Telegram, потому что сервер видит «запись в
    # те же поля, что и так записаны», и ловит частые апдейты.
    name_changed = new_first != orig_first or new_last != orig_last
    about_changed = new_about != orig_about
    avatar_changed = bool(avatar_bytes)

    if not (name_changed or about_changed or avatar_changed):
        await callback.answer("Нет изменений для сохранения", show_alert=True)
        return

    await callback.answer()
    try:
        await callback.message.edit_text(
            f"{emoji('LOADING')} <b>Применяю изменения профиля…</b>",
            reply_markup=None,
        )
    except Exception:
        pass

    client = await get_client_for_account(account_id)
    if not client:
        await callback.message.edit_text(
            f"{emoji('CROSS')} Не удалось подключиться к аккаунту. "
            f"Изменения не применены.",
            reply_markup=InlineKeyboardBuilder().row(InlineKeyboardButton(
                text="Назад",
                callback_data=f"manage_account_{account_id}",
                style='default',
                icon_custom_emoji_id=get_icon("BACK"),
            )).as_markup(),
        )
        return

    # Сюда собираем красивое описание того, что было сделано, и любую
    # ошибку — чтобы показать пользователю, даже если финал сломался
    # на полпути (например, имя ушло, а аватарка — нет).
    steps_done: List[str] = []
    steps_failed: Optional[str] = None
    flood_wait_seconds: Optional[int] = None

    # 1) Имя / фамилия — отдельным запросом, только если изменились.
    if name_changed:
        try:
            await _call_profile_rpc(
                client,
                UpdateProfileRequest(
                    first_name=new_first,
                    last_name=new_last,
                ),
                op_label="update_name",
            )
            steps_done.append("имя")
        except FloodWaitError as ex:
            flood_wait_seconds = ex.seconds
        except Exception as ex:
            logger.exception("profile name update failed")
            steps_failed = f"имя: {ex}"

    # 2) Описание — отдельным запросом. Делаем это только если изменилось
    #    И если имя прошло (иначе нет смысла долбить сервер дальше).
    if not flood_wait_seconds and not steps_failed and about_changed:
        # Небольшая пауза между последовательными апдейтами профиля —
        # Telegram-сервер склонен отвечать «try again», если запросы
        # идут слишком часто.
        await asyncio.sleep(0.6)
        try:
            await _call_profile_rpc(
                client,
                UpdateProfileRequest(about=new_about),
                op_label="update_about",
            )
            steps_done.append("описание")
        except FloodWaitError as ex:
            flood_wait_seconds = ex.seconds
        except Exception as ex:
            logger.exception("profile about update failed")
            steps_failed = f"описание: {ex}"

    # 3) Аватарка — отдельный шаг, потому что это совсем другой RPC.
    #    GetUserPhotosRequest + DeletePhotosRequest мы раньше делали
    #    сразу после UpdateProfileRequest — это регулярно валилось
    #    «TAKEOUT_INIT_FAIL_X» / «FLOOD_WAIT_X». Ставим только новое
    #    фото, историю (если она есть) Telegram сам перетрёт главной
    #    аватаркой, а старая останется в photo-history без видимого
    #    эффекта на сам профиль.
    if not flood_wait_seconds and not steps_failed and avatar_changed:
        tmp_path = None
        try:
            await asyncio.sleep(0.6)
            with tempfile.NamedTemporaryFile(
                suffix='.jpg', delete=False
            ) as tmp:
                tmp.write(avatar_bytes)
                tmp_path = tmp.name
            uploaded = await client.upload_file(tmp_path)
            await _call_profile_rpc(
                client,
                UploadProfilePhotoRequest(file=uploaded),
                op_label="upload_avatar",
            )
            steps_done.append("аватарка")
        except FloodWaitError as ex:
            flood_wait_seconds = ex.seconds
        except Exception as ex:
            logger.exception("profile avatar update failed")
            steps_failed = f"аватарка: {ex}"
        finally:
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

    # Если ничего не успело — это «не применено вообще».
    if not steps_done and (flood_wait_seconds or steps_failed):
        back_kb = InlineKeyboardBuilder().row(InlineKeyboardButton(
            text="Назад",
            callback_data=f"manage_account_{account_id}",
            style='default',
            icon_custom_emoji_id=get_icon("BACK"),
        )).as_markup()
        if flood_wait_seconds:
            await callback.message.edit_text(
                f"{emoji('CLOCK')} Telegram просит подождать "
                f"<b>{flood_wait_seconds} сек</b> перед изменением профиля. "
                f"Попробуйте позже.",
                reply_markup=back_kb,
            )
        else:
            await callback.message.edit_text(
                f"{emoji('CROSS')} Не удалось сохранить профиль: "
                f"<code>{escape(str(steps_failed)[:200])}</code>\n\n"
                f"Чаще всего это значит, что аккаунт-клиент Telegram "
                f"попал в короткий rate-limit («попробуйте снова, клиент "
                f"истёк»). Откройте редактор через пару секунд и повторите.",
                reply_markup=back_kb,
            )
        return

    await state.clear()

    # Заново читаем профиль из Telegram, чтобы показать применённые данные.
    # Если что-то не успело — покажем то, что есть.
    fresh = await _fetch_profile_from_telegram(account_id) or {
        'first_name': new_first,
        'last_name': new_last,
        'about': new_about,
    }
    summary_bits: List[str] = []
    if "имя" in steps_done:
        summary_bits.append("имя")
    if "описание" in steps_done:
        summary_bits.append("описание")
    if "аватарка" in steps_done:
        summary_bits.append("аватарка")
    summary = ", ".join(summary_bits) if summary_bits else "без изменений"

    warning: Optional[str] = None
    if flood_wait_seconds:
        warning = (
            f"\n\n{emoji('CLOCK')} Не всё удалось: Telegram просит "
            f"подождать <b>{flood_wait_seconds} сек</b>."
        )
    elif steps_failed:
        warning = (
            f"\n\n{emoji('CROSS')} Не всё удалось: "
            f"<code>{escape(steps_failed[:200])}</code>"
        )

    await callback.message.edit_text(
        f"{emoji('CHECK')} <b>Профиль обновлён</b>\n\n"
        f"{emoji('ID')} Имя: <b>{_profile_display(fresh['first_name'])}</b>\n"
        f"{emoji('ID')} Фамилия: <b>{_profile_display(fresh['last_name'])}</b>\n"
        f"{emoji('ADD_TEXT')} Описание: "
        f"{_profile_display(fresh['about'], 'пусто')}\n"
        f"{emoji('MEDIA')} Аватарка: "
        f"{'обновлена' if 'аватарка' in steps_done else 'без изменений'}\n\n"
        f"Применено: <b>{summary}</b>{warning or ''}",
        reply_markup=InlineKeyboardBuilder().row(InlineKeyboardButton(
            text="Назад к аккаунту",
            callback_data=f"manage_account_{account_id}",
            style='default',
            icon_custom_emoji_id=get_icon("BACK"),
        )).as_markup(),
    )


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
    if not await is_pro(callback.from_user.id):
        await callback.answer(
            "Прогрев с AI-планом доступен только в Pro.",
            show_alert=True
        )
        return

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
    account = await get_account(account_id)
    if not account or account.get('user_id') != callback.from_user.id:
        await callback.answer('Аккаунт не найден', show_alert=True)
        return

    await shutdown_account_runtime(account_id, callback.from_user.id)
    deleted = await delete_account(account_id)
    if not deleted:
        await callback.answer('Не удалось удалить аккаунт', show_alert=True)
        return

    await callback.message.edit_text(
        f"{emoji('CHECK')} Аккаунт успешно удален!",
        reply_markup=get_account_manager_keyboard()
    )
    await callback.answer()

# --- Рассылка ---
async def get_broadcast_templates(user_id: int) -> List[Dict[str, Any]]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT id, name, message_variants, created_at FROM broadcast_templates '
            'WHERE user_id = $1 ORDER BY created_at DESC LIMIT 30', user_id
        )
    return [dict(row) for row in rows]


def get_broadcast_templates_keyboard(templates: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for item in templates:
        count = len(item.get('message_variants') or [])
        builder.row(InlineKeyboardButton(
            text=f"{item['name']} ({count})", callback_data=f"broadcast_template_use:{item['id']}",
            style='primary', icon_custom_emoji_id=get_icon('CLIPBOARD')
        ))
    builder.row(InlineKeyboardButton(text='Назад', callback_data='broadcast', style='default',
                                     icon_custom_emoji_id=get_icon('BACK')))
    return builder.as_markup()


@dp.callback_query(F.data == 'broadcast_templates')
async def broadcast_templates_menu(callback: CallbackQuery, state: FSMContext):
    templates = await get_broadcast_templates(callback.from_user.id)
    await callback.message.edit_text(
        f"{emoji('CLIPBOARD')} <b>Шаблоны рассылок</b>\n\n"
        f"Сохранено: <b>{len(templates)}</b>\nВыберите шаблон для применения.",
        reply_markup=get_broadcast_templates_keyboard(templates)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith('broadcast_template_use:'))
async def broadcast_template_use(callback: CallbackQuery, state: FSMContext):
    template_id = int(callback.data.split(':', 1)[1])
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT * FROM broadcast_templates WHERE id = $1 AND user_id = $2',
            template_id, callback.from_user.id
        )
    if not row:
        await callback.answer('Шаблон не найден', show_alert=True)
        return
    variants = list(row['message_variants'] or [])[:30]
    await state.clear()
    await state.update_data(message_texts=variants, applied_template=row['name'])
    await callback.message.edit_text(
        f"{emoji('CHECK')} Шаблон <b>{escape(row['name'])}</b> применён.\n\n"
        f"Сообщений: <b>{len(variants)}</b>. Выберите режим рассылки:",
        reply_markup=get_broadcast_mode_keyboard()
    )
    await callback.answer('Шаблон применён')


@dp.callback_query(F.data == 'broadcast_template_save')
async def broadcast_template_save_start(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('message_texts'):
        await callback.answer('Нет сообщений для шаблона', show_alert=True)
        return
    await state.set_state(BroadcastTemplateStates.waiting_for_name)
    await callback.message.edit_text(
        f"{emoji('CLIPBOARD')} <b>Сохранение шаблона</b>\n\nВведите название шаблона:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text='Отмена', callback_data='functions', style='default',
                                 icon_custom_emoji_id=get_icon('BACK'))
        ]])
    )
    await callback.answer()


@dp.message(BroadcastTemplateStates.waiting_for_name)
async def broadcast_template_save_name(message: Message, state: FSMContext):
    name = ' '.join((message.text or '').split())[:80]
    data = await state.get_data()
    variants = list(data.get('message_texts') or [])[:30]
    if not name or not variants:
        await message.answer('Введите название шаблона.')
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO broadcast_templates (user_id, name, message_variants)
               VALUES ($1, $2, $3::jsonb)''',
            message.from_user.id, name, json.dumps(variants, ensure_ascii=False)
        )
    await state.clear()
    await message.answer(
        f"{emoji('CHECK')} Шаблон <b>{escape(name)}</b> сохранён.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text='К шаблонам', callback_data='broadcast_templates',
                                 style='primary', icon_custom_emoji_id=get_icon('CLIPBOARD')),
            InlineKeyboardButton(text='В функции', callback_data='functions',
                                 style='default', icon_custom_emoji_id=get_icon('BACK'))
        ]])
    )


@dp.callback_query(F.data == "broadcast")
async def broadcast(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('message_texts'):
        await state.clear()
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
    existing = list((await state.get_data()).get('message_texts') or [])
    if existing:
        await message.answer(
            f"{emoji('CLIPBOARD')} <b>Шаблон загружен</b>\n\n"
            f"Сообщений: <b>{len(existing)}</b>. Они будут выбираться случайно. "
            f"Можно добавить ещё сообщения или сразу нажать «Готово».",
            reply_markup=_collecting_keyboard(len(existing), 30)
        )
        await state.set_state(BroadcastStates.waiting_for_message)
        return

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
async def _extract_message_payload(message: Message, state: FSMContext):
    """Вытаскивает текст + медиа из входящего сообщения и сохраняет
    файлы на диск. Возвращает кортеж (text, media_paths)."""
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
    return text, media_paths


def _collecting_keyboard(count: int, max_count: int = 30) -> InlineKeyboardMarkup:
    """Клавиатура для режима набора нескольких сообщений: кнопка
    «Готово» появляется только если уже есть минимум одно сообщение,
    и блокируется при достижении лимита."""
    builder = InlineKeyboardBuilder()
    if count >= 1:
        builder.row(InlineKeyboardButton(
            text=(
                f"✅ Готово ({count}/{max_count})"
                if count < max_count
                else f"✅ Готово (достигнут лимит {max_count})"
            ),
            callback_data="broadcast_messages_done",
            style='success',
            icon_custom_emoji_id=get_icon("CHECK")
        ))
        builder.row(InlineKeyboardButton(
            text="Добавить URL-кнопку к последнему",
            callback_data="broadcast_add_button",
            style='default',
            icon_custom_emoji_id=get_icon("LINK")
        ))
    builder.row(InlineKeyboardButton(
        text="Отмена",
        callback_data="broadcast",
        style='danger',
        icon_custom_emoji_id=get_icon("CROSS")
    ))
    return builder.as_markup()


async def process_broadcast_message(message: Message, state: FSMContext):
    """Набираем до 30 сообщений подряд. После каждого сообщения
    показываем счётчик и кнопку «Готово». Сами варианты хранятся в
    state['message_texts'] как список dict {text, media}."""
    text, media_paths = await _extract_message_payload(message, state)

    data = await state.get_data()
    variants = list(data.get('message_texts') or [])
    if len(variants) >= 30:
        # Лимит — просто показываем клавиатуру с Готово и не добавляем
        # ничего нового. Можно считать, что state уже не доверяет.
        await message.answer(
            f"{emoji('CROSS')} Достигнут лимит в 30 сообщений. "
            f"Нажмите «Готово», чтобы продолжить.",
            reply_markup=_collecting_keyboard(len(variants), 30)
        )
        return

    variants.append({'text': text, 'media': media_paths, 'buttons': []})
    await state.update_data(
        message_texts=variants,
        message_text=text,
        message_media=media_paths,
    )

    # Кратко показываем добавленное сообщение (превью).
    if media_paths and len(media_paths) > 0:
        if len(media_paths) == 1 and os.path.exists(media_paths[0]):
            await message.answer_document(
                FSInputFile(media_paths[0]),
                caption=text,
                parse_mode='HTML'
            )
    else:
        await message.answer(text or ' ', parse_mode='HTML')

    await message.answer(
        f"{emoji('MAIL')} <b>Сообщение #{len(variants)} добавлено.</b>\n\n"
        f"Отправьте следующее сообщение — оно будет добавлено в список "
        f"и при рассылке выберется случайно. Когда закончите — нажмите "
        f"«Готово». Можно добавить до 30 вариантов.",
        reply_markup=_collecting_keyboard(len(variants), 30)
    )


@dp.callback_query(F.data == "broadcast_add_button")
async def broadcast_add_button_start(callback: CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    if current_state == BroadcastStates.waiting_for_message.state:
        await state.set_state(BroadcastStates.waiting_for_button_text)
    elif current_state == DMBroadcastStates.waiting_for_message.state:
        await state.set_state(DMBroadcastStates.waiting_for_button_text)
    else:
        await callback.answer('Сначала добавьте сообщение', show_alert=True)
        return
    await callback.message.edit_text(
        f"{emoji('LINK')} <b>URL-кнопка</b>\n\nВведите текст кнопки (до 64 символов):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text='Отмена', callback_data='broadcast_button_cancel',
                                 style='default', icon_custom_emoji_id=get_icon('BACK'))
        ]])
    )
    await callback.answer()


async def _save_broadcast_button(message: Message, state: FSMContext, button_text: str, button_url: str):
    data = await state.get_data()
    variants = list(data.get('message_texts') or [])
    if not variants:
        await message.answer('Нет сообщения для добавления кнопки.')
        return
    variants[-1] = dict(variants[-1])
    variants[-1]['buttons'] = [{'text': button_text, 'url': button_url}]
    await state.update_data(message_texts=variants)
    await state.set_state(BroadcastStates.waiting_for_message if data.get('message_count') is not None
                          else DMBroadcastStates.waiting_for_message)
    await message.answer(
        f"{emoji('CHECK')} Кнопка добавлена: <b>{escape(button_text)}</b>\n"
        f"<code>{escape(button_url)}</code>",
        reply_markup=_collecting_keyboard(len(variants), 30)
    )


@dp.message(BroadcastStates.waiting_for_button_text)
async def broadcast_button_text(message: Message, state: FSMContext):
    text = (message.text or '').strip()
    if not text or len(text) > 64:
        await message.answer('Введите текст кнопки длиной от 1 до 64 символов.')
        return
    await state.update_data(button_text=text)
    await state.set_state(BroadcastStates.waiting_for_button_url)
    await message.answer('Теперь отправьте URL, начинающийся с http:// или https://')


@dp.message(BroadcastStates.waiting_for_button_url)
async def broadcast_button_url(message: Message, state: FSMContext):
    url = (message.text or '').strip()
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        await message.answer('Некорректный URL. Используйте http:// или https://')
        return
    data = await state.get_data()
    await _save_broadcast_button(message, state, data.get('button_text') or 'Открыть', url)


@dp.message(DMBroadcastStates.waiting_for_button_text)
async def dm_broadcast_button_text(message: Message, state: FSMContext):
    text = (message.text or '').strip()
    if not text or len(text) > 64:
        await message.answer('Введите текст кнопки длиной от 1 до 64 символов.')
        return
    await state.update_data(button_text=text)
    await state.set_state(DMBroadcastStates.waiting_for_button_url)
    await message.answer('Теперь отправьте URL, начинающийся с http:// или https://')


@dp.message(DMBroadcastStates.waiting_for_button_url)
async def dm_broadcast_button_url(message: Message, state: FSMContext):
    url = (message.text or '').strip()
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        await message.answer('Некорректный URL. Используйте http:// или https://')
        return
    data = await state.get_data()
    await _save_broadcast_button(message, state, data.get('button_text') or 'Открыть', url)


@dp.callback_query(F.data == "broadcast_button_cancel")
async def broadcast_button_cancel(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    variants = list(data.get('message_texts') or [])
    await state.set_state(BroadcastStates.waiting_for_message if data.get('message_count') is not None
                          else DMBroadcastStates.waiting_for_message)
    await callback.message.edit_text(
        f"{emoji('INFO')} Возврат к конструктору сообщений.",
        reply_markup=_collecting_keyboard(len(variants), 30)
    )
    await callback.answer()


@dp.callback_query(F.data == "broadcast_messages_done")
async def broadcast_messages_done(callback: CallbackQuery, state: FSMContext):
    """Завершаем набор сообщений и переходим к предпросмотру."""
    data = await state.get_data()
    variants = list(data.get('message_texts') or [])
    if not variants:
        await callback.answer(
            "Нужно добавить хотя бы одно сообщение", show_alert=True
        )
        return

    # Совместимость: legacy-поля заполняем первым вариантом.
    first = variants[0]
    await state.update_data(
        message_text=first.get('text') or '',
        message_media=first.get('media') or [],
    )

    preview_text = (
        f"{emoji('EYE')} <b>Предпросмотр рассылки:</b>\n\n"
        f"{emoji('PROFILE')} Аккаунт ID: {data['account_id']}\n"
        f"{emoji('PEOPLE')} Чатов: {len(data['selected_chats'])}\n"
        f"{emoji('CLOCK')} Задержка: {data['delay']} сек\n"
        f"{emoji('MAIL')} Сообщений в чат: {data['message_count']}\n"
        f"{emoji('GEAR')} Режим: "
        f"{'Одновременный' if data['mode'] == 'simultaneous' else 'Рандомный'}\n"
        f"{emoji('WRITE')} Вариантов сообщений: <b>{len(variants)}</b> "
        f"(рандом при отправке)"
    )
    await callback.message.edit_text(
        preview_text, reply_markup=get_broadcast_preview_keyboard()
    )
    await state.set_state(BroadcastStates.preview)
    await callback.answer()

@dp.callback_query(F.data == "start_broadcast", BroadcastStates.preview)
async def start_broadcast(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id

    allowed, used_hours = await check_broadcast_limit(user_id)
    if not allowed:
        remaining = max(0.0, FREE_BROADCAST_LIMIT_HOURS - used_hours)
        await callback.message.edit_text(
            f"<b>Лимит рассылки исчерпан</b>\n\n"
            f"На Free-тарифе доступно <b>{FREE_BROADCAST_LIMIT_HOURS} часов</b> "
            f"рассылок в неделю.\n"
            f"Использовано: <b>{used_hours:.1f} ч</b> "
            f"из {FREE_BROADCAST_LIMIT_HOURS} ч "
            f"(осталось: {remaining:.1f} ч).\n\n"
            f"Обновитесь до Pro для неограниченной рассылки!",
            reply_markup=get_subscription_keyboard("free")
        )
        await callback.answer()
        return

    chat_ids_str = [str(x) for x in data['selected_chats']]
    variants = list(data.get('message_texts') or [])
    if not variants:
        # Совместимость со старыми данными
        variants = [{
            'text': data.get('message_text') or '',
            'media': list(data.get('message_media') or []),
        }]
    variants_json = json.dumps(variants, ensure_ascii=False)

    try:
        async with db_pool.acquire() as conn:
            broadcast_id = await conn.fetchval(
                "INSERT INTO broadcasts "
                "(user_id, account_id, chat_ids, delay, message_count, "
                "message_text, message_media, message_texts, mode, broadcast_type) "
                "VALUES ($1, $2, $3::text[], $4, $5, $6, $7::text[], $8::jsonb, $9, 'chat') "
                "RETURNING id",
                user_id, data['account_id'], chat_ids_str,
                data['delay'], data['message_count'],
                data['message_text'], data['message_media'],
                variants_json, data['mode']
            )

        asyncio.create_task(execute_broadcast(broadcast_id, user_id))

        await callback.message.edit_text(
            f"{emoji('PLAY')} <b>Рассылка запущена!</b>\n\n"
            f"ID: {broadcast_id}\n"
            f"Чатов: {len(data['selected_chats'])}\n"
            f"Сообщений в чат: {data['message_count']}\n"
            f"Вариантов: {len(variants)} (случайный выбор)",
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
    """Набираем до 30 сообщений для отложенной рассылки."""
    text, media_paths = await _extract_message_payload(message, state)

    data = await state.get_data()
    variants = list(data.get('message_texts') or [])
    if len(variants) >= 30:
        await message.answer(
            f"{emoji('CROSS')} Достигнут лимит в 30 сообщений. "
            f"Нажмите «Готово», чтобы продолжить.",
            reply_markup=_collecting_keyboard(len(variants), 30)
        )
        return

    variants.append({'text': text, 'media': media_paths, 'buttons': []})
    await state.update_data(
        message_texts=variants,
        message_text=text,
        message_media=media_paths,
    )

    if media_paths and len(media_paths) > 0:
        if len(media_paths) == 1 and os.path.exists(media_paths[0]):
            await message.answer_document(
                FSInputFile(media_paths[0]),
                caption=text,
                parse_mode='HTML'
            )
    else:
        await message.answer(text or ' ', parse_mode='HTML')

    await message.answer(
        f"{emoji('MAIL')} <b>Сообщение #{len(variants)} добавлено.</b>\n\n"
        f"Отправьте следующее или нажмите «Готово», чтобы перейти к "
        f"выбору даты. Можно добавить до 30 вариантов.",
        reply_markup=_collecting_keyboard(len(variants), 30)
    )


@dp.callback_query(F.data == "broadcast_messages_done",
                   ScheduledBroadcastStates.waiting_for_message)
async def scheduled_messages_done(callback: CallbackQuery, state: FSMContext):
    """Завершаем набор сообщений для отложенной рассылки."""
    data = await state.get_data()
    variants = list(data.get('message_texts') or [])
    if not variants:
        await callback.answer(
            "Нужно добавить хотя бы одно сообщение", show_alert=True
        )
        return

    first = variants[0]
    await state.update_data(
        message_text=first.get('text') or '',
        message_media=first.get('media') or [],
    )

    await callback.message.edit_text(
        f"{emoji('CALENDAR')} <b>Введите дату и время отправки (МСК):</b>\n\n"
        f"Формат: <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n"
        f"Пример: <code>15.06.2026 14:30</code>\n\n"
        f"Добавлено вариантов: <b>{len(variants)}</b>",
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
    await callback.answer()

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
        variants = list(data.get('message_texts') or [])
        if not variants:
            variants = [{
                'text': data.get('message_text') or '',
                'media': list(data.get('message_media') or []),
            }]
        variants_json = json.dumps(variants, ensure_ascii=False)

        async with db_pool.acquire() as conn:
            broadcast_id = await conn.fetchval(
                "INSERT INTO broadcasts "
                "(user_id, account_id, chat_ids, delay, message_count, "
                "message_text, message_media, message_texts, mode, status, "
                "scheduled_at, broadcast_type) "
                "VALUES ($1, $2, $3::text[], $4, $5, $6, $7::text[], "
                "$8::jsonb, $9, 'scheduled', $10, 'chat') RETURNING id",
                user_id, data['account_id'], chat_ids_str,
                data['delay'], data['message_count'],
                data['message_text'], data['message_media'],
                variants_json, data['mode'], scheduled_dt
            )

        await message.answer(
            f"{emoji('CHECK')} <b>Рассылка запланирована!</b>\n\n"
            f"ID: {broadcast_id}\n"
            f"Дата: {scheduled_dt.strftime('%d.%m.%Y %H:%M')} МСК\n"
            f"Чатов: {len(data['selected_chats'])}\n"
            f"Вариантов: {len(variants)} (случайный выбор)",
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
    """Набираем до 30 вариантов сообщений для DM-рассылки."""
    text, media_paths = await _extract_message_payload(message, state)

    data = await state.get_data()
    variants = list(data.get('message_texts') or [])
    if len(variants) >= 30:
        await message.answer(
            f"{emoji('CROSS')} Достигнут лимит в 30 сообщений. "
            f"Нажмите «Готово», чтобы продолжить.",
            reply_markup=_collecting_keyboard(len(variants), 30)
        )
        return

    variants.append({'text': text, 'media': media_paths})
    await state.update_data(
        message_texts=variants,
        message_text=text,
        message_media=media_paths,
    )

    if media_paths and len(media_paths) > 0:
        if len(media_paths) == 1 and os.path.exists(media_paths[0]):
            await message.answer_document(
                FSInputFile(media_paths[0]),
                caption=text,
                parse_mode='HTML'
            )
    else:
        await message.answer(text or ' ', parse_mode='HTML')

    await message.answer(
        f"{emoji('MAIL')} <b>Сообщение #{len(variants)} добавлено.</b>\n\n"
        f"Отправьте следующее или нажмите «Готово», чтобы перейти к "
        f"выбору задержки. Можно добавить до 30 вариантов.",
        reply_markup=_collecting_keyboard(len(variants), 30)
    )


@dp.callback_query(F.data == "broadcast_messages_done",
                   DMBroadcastStates.waiting_for_message)
async def dm_messages_done(callback: CallbackQuery, state: FSMContext):
    """Завершаем набор сообщений для DM-рассылки."""
    data = await state.get_data()
    variants = list(data.get('message_texts') or [])
    if not variants:
        await callback.answer(
            "Нужно добавить хотя бы одно сообщение", show_alert=True
        )
        return

    first = variants[0]
    await state.update_data(
        message_text=first.get('text') or '',
        message_media=first.get('media') or [],
    )

    await callback.message.edit_text(
        f"{emoji('CLOCK')} <b>Введите задержку между сообщениями</b>\n\n"
        f"Минимум 60 секунд:\n\n"
        f"Добавлено вариантов: <b>{len(variants)}</b>",
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
    await callback.answer()

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
    variants = list(data.get('message_texts') or [])
    if not variants:
        variants = [{
            'text': data.get('message_text') or '',
            'media': list(data.get('message_media') or []),
        }]

    preview_text = (
        f"{emoji('EYE')} <b>Предпросмотр рассылки в ЛС:</b>\n\n"
        f"{emoji('PROFILE')} Аккаунт ID: {data['account_id']}\n"
        f"{emoji('PEOPLE')} Получателей: {data['usernames_count']}\n"
        f"{emoji('CLOCK')} Задержка: {delay} сек\n"
        f"{emoji('MEDIA')} Медиа: "
        f"{len(data.get('message_media', []))} файлов\n"
        f"{emoji('WRITE')} Вариантов сообщений: <b>{len(variants)}</b> "
        f"(рандом при отправке)"
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

    allowed, used_hours = await check_broadcast_limit(user_id)
    if not allowed:
        remaining = max(0.0, FREE_BROADCAST_LIMIT_HOURS - used_hours)
        await callback.message.edit_text(
            f"<b>Лимит рассылки исчерпан</b>\n\n"
            f"На Free-тарифе доступно <b>{FREE_BROADCAST_LIMIT_HOURS} часов</b> "
            f"рассылок в неделю.\n"
            f"Использовано: <b>{used_hours:.1f} ч</b> "
            f"из {FREE_BROADCAST_LIMIT_HOURS} ч "
            f"(осталось: {remaining:.1f} ч).\n\n"
            f"Обновитесь до Pro для неограниченной рассылки!",
            reply_markup=get_subscription_keyboard("free")
        )
        await callback.answer()
        return

    variants = list(data.get('message_texts') or [])
    if not variants:
        variants = [{
            'text': data.get('message_text') or '',
            'media': list(data.get('message_media') or []),
        }]
    variants_json = json.dumps(variants, ensure_ascii=False)

    async with db_pool.acquire() as conn:
        dm_id = await conn.fetchval(
            "INSERT INTO dm_broadcasts "
            "(user_id, account_id, usernames, delay, message_text, "
            "message_media, message_texts, status, total_count) "
            "VALUES ($1, $2, $3::text[], $4, $5, $6::text[], $7::jsonb, "
            "'active', $8) RETURNING id",
            user_id, data['account_id'], data['usernames'],
            data['delay'], data['message_text'],
            data.get('message_media', []), variants_json,
            len(data['usernames'])
        )

    task_id = int(datetime.now().timestamp())

    await callback.message.edit_text(
        f"{emoji('LOADING')} <b>Запускаю рассылку в ЛС...</b>\n\n"
        f"DM ID: {dm_id}\n"
        f"Получателей: {data['usernames_count']}\n"
        f"Вариантов: {len(variants)} (случайный выбор)\n"
        f"Это может занять некоторое время.",
        reply_markup=get_broadcast_control_keyboard(dm_id, 'dm')
    )

    task = asyncio.create_task(execute_dm_broadcast_db(
        dm_id, task_id, data['account_id'], user_id,
        data['usernames'], data['message_text'], data['delay'],
        data.get('message_media', []), variants
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

# --- Создание каналов и групп ---
async def open_chat_creation_menu(
    callback: CallbackQuery, state: FSMContext, creation_kind: str,
) -> None:
    labels = get_chat_creation_labels(creation_kind)
    user_id = callback.from_user.id
    running_task = chat_creation_tasks.get(user_id)
    if running_task and not running_task.done():
        await callback.answer(
            'Создание каналов или групп уже запущено',
            show_alert=True,
        )
        return

    accounts = [
        account for account in await get_user_accounts(user_id)
        if account.get('is_active')
    ]
    if not accounts:
        await callback.message.edit_text(
            f"{emoji('CROSS')} У вас нет активных аккаунтов.",
            reply_markup=get_functions_keyboard(),
        )
        await callback.answer()
        return

    await state.clear()
    await state.update_data(creation_kind=creation_kind)
    await state.set_state(ChatCreationStates.waiting_for_account)
    await callback.message.edit_text(
        f"{emoji('PROFILE')} <b>Выберите аккаунт</b>\n\n"
        f"{labels['plural']} будут создаваться от имени выбранного аккаунта.",
        reply_markup=get_chat_creation_accounts_keyboard(
            accounts, creation_kind
        ),
    )
    await callback.answer()


@dp.callback_query(F.data == "create_channels")
async def create_channels_menu(callback: CallbackQuery, state: FSMContext):
    await open_chat_creation_menu(callback, state, 'channel')


@dp.callback_query(F.data == "create_groups")
async def create_groups_menu(callback: CallbackQuery, state: FSMContext):
    await open_chat_creation_menu(callback, state, 'group')


@dp.callback_query(F.data.startswith("select_chat_create_account:"))
async def select_chat_create_account(
    callback: CallbackQuery, state: FSMContext
):
    try:
        _, creation_kind, raw_account_id = callback.data.split(':', 2)
        account_id = int(raw_account_id)
    except (AttributeError, ValueError):
        await callback.answer('Некорректный аккаунт', show_alert=True)
        return
    if creation_kind not in CHAT_CREATION_LABELS:
        await callback.answer('Некорректный тип', show_alert=True)
        return

    account = await get_account(account_id)
    if (
        not account
        or account['user_id'] != callback.from_user.id
        or not account.get('is_active')
    ):
        await callback.answer('Аккаунт не найден', show_alert=True)
        return

    labels = get_chat_creation_labels(creation_kind)
    await state.update_data(
        creation_kind=creation_kind,
        account_id=account_id,
        account_phone=account['phone'],
    )
    await state.set_state(ChatCreationStates.waiting_for_count)
    await callback.message.edit_text(
        f"{emoji('NAMES')} <b>Количество {labels['items']}</b>\n\n"
        "Введите число от <b>1</b> до <b>100</b>.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Отмена",
                callback_data="chat_creation_cancel",
                style='danger',
                icon_custom_emoji_id=get_icon("CROSS"),
            )
        ]]),
    )
    await callback.answer()


@dp.message(ChatCreationStates.waiting_for_count)
async def process_chat_creation_count(
    message: Message, state: FSMContext
):
    try:
        count = int((message.text or '').strip())
    except ValueError:
        await message.answer(
            f"{emoji('CROSS')} Введите целое число от 1 до 100."
        )
        return

    if not 1 <= count <= 100:
        await message.answer(
            f"{emoji('CROSS')} Количество должно быть от 1 до 100."
        )
        return

    data = await state.get_data()
    labels = get_chat_creation_labels(data.get('creation_kind', 'channel'))
    await state.update_data(count=count)
    await state.set_state(ChatCreationStates.waiting_for_title)
    await message.answer(
        f"{emoji('WRITE')} <b>Название {labels['items']}</b>\n\n"
        f"Введите базовое название. Если {labels['items']} несколько, бот "
        "добавит номера: <code>Название 1</code>, "
        "<code>Название 2</code> и т.д.\n\n"
        "Максимальная длина базового названия — 120 символов.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Отмена",
                callback_data="chat_creation_cancel",
                style='danger',
                icon_custom_emoji_id=get_icon("CROSS"),
            )
        ]]),
    )


@dp.message(ChatCreationStates.waiting_for_title)
async def process_chat_creation_title(
    message: Message, state: FSMContext
):
    title = ' '.join((message.text or '').split()).strip()
    if not title:
        await message.answer(
            f"{emoji('CROSS')} Отправьте название обычным текстом."
        )
        return
    if len(title) > 120:
        await message.answer(
            f"{emoji('CROSS')} Название слишком длинное. Максимум 120 символов."
        )
        return

    await state.update_data(base_title=title)
    data = await state.get_data()
    creation_kind = data.get('creation_kind', 'channel')
    labels = get_chat_creation_labels(creation_kind)
    count = data['count']
    if count == 1:
        names_preview = f"<code>{escape(title)}</code>"
    else:
        first_title = build_chat_title(title, 1, count)
        last_title = build_chat_title(title, count, count)
        names_preview = (
            f"<code>{escape(first_title)}</code> … "
            f"<code>{escape(last_title)}</code>"
        )

    entity_note = (
        "Группы будут приватными супергруппами."
        if creation_kind == 'group'
        else "Каналы будут приватными."
    )
    await state.set_state(ChatCreationStates.preview)
    await message.answer(
        f"{emoji('EYE')} <b>Проверьте параметры</b>\n\n"
        f"{emoji('PHONE')} Аккаунт: "
        f"<code>{escape(str(data['account_phone']))}</code>\n"
        f"{emoji('NAMES')} Количество {labels['items']}: <b>{count}</b>\n"
        f"{emoji('WRITE')} Названия: {names_preview}\n"
        f"{emoji('CLOCK')} Задержка: <b>{CHAT_CREATION_DELAY} секунд</b>\n\n"
        f"{entity_note} Telegram может применить собственные лимиты "
        "или дополнительную паузу.",
        reply_markup=get_chat_creation_preview_keyboard(),
    )


@dp.callback_query(
    F.data == "start_chat_creation", ChatCreationStates.preview
)
async def start_chat_creation(
    callback: CallbackQuery, state: FSMContext
):
    user_id = callback.from_user.id
    running_task = chat_creation_tasks.get(user_id)
    if running_task and not running_task.done():
        await callback.answer(
            'Создание каналов или групп уже запущено',
            show_alert=True,
        )
        return

    data = await state.get_data()
    creation_kind = data.get('creation_kind', 'channel')
    labels = get_chat_creation_labels(creation_kind)
    account = await get_account(data['account_id'])
    if not account or account['user_id'] != user_id:
        await callback.answer('Аккаунт не найден', show_alert=True)
        await state.clear()
        return

    await callback.message.edit_text(
        f"{emoji('LOADING')} <b>{labels['title']} запущено</b>\n\n"
        f"Количество: <b>{data['count']}</b>\n"
        f"Задержка: <b>{CHAT_CREATION_DELAY} секунд</b>\n"
        f"{labels['first']} создаётся сейчас.",
        reply_markup=get_chat_creation_control_keyboard(),
    )
    await state.clear()

    async def run_creation() -> None:
        try:
            result = await execute_chat_creation(
                account_id=data['account_id'],
                user_id=user_id,
                count=data['count'],
                base_title=data['base_title'],
                creation_kind=creation_kind,
                progress_message=callback.message,
            )
            failed = result['failed']
            error_text = ''
            if failed:
                items = []
                for item in failed[:5]:
                    error = item['error'].replace('\n', ' ')[:160]
                    items.append(
                        f"• <code>{escape(item['title'])}</code>: "
                        f"{escape(error)}"
                    )
                error_text = "\n\n<b>Первые ошибки:</b>\n" + "\n".join(items)

            status = 'остановлено' if result['stopped'] else 'завершено'
            await callback.message.edit_text(
                f"{emoji('CHECK')} <b>{labels['title']} {status}</b>\n\n"
                f"Запрошено: <b>{result['total']}</b>\n"
                f"Создано: <b>{len(result['created'])}</b>\n"
                f"Ошибок: <b>{len(failed)}</b>"
                f"{error_text}",
                reply_markup=get_functions_keyboard(),
            )
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            logger.exception(
                '%s creation task failed for user_id=%s',
                creation_kind, user_id,
            )
            try:
                await callback.message.edit_text(
                    f"{emoji('CROSS')} <b>Не удалось создать "
                    f"{labels['items']}</b>\n\n"
                    f"<code>{escape(str(ex))}</code>",
                    reply_markup=get_functions_keyboard(),
                )
            except Exception:
                pass
        finally:
            current_task = asyncio.current_task()
            if chat_creation_tasks.get(user_id) is current_task:
                chat_creation_tasks.pop(user_id, None)
            chat_creation_stop_flags.pop(user_id, None)

    task = asyncio.create_task(run_creation())
    chat_creation_tasks[user_id] = task
    await callback.answer()


@dp.callback_query(F.data == "stop_chat_creation")
async def stop_chat_creation(callback: CallbackQuery):
    user_id = callback.from_user.id
    task = chat_creation_tasks.get(user_id)
    if not task or task.done():
        await callback.answer('Активной задачи нет', show_alert=True)
        return

    chat_creation_stop_flags[user_id] = True
    task.cancel()
    try:
        await callback.message.edit_text(
            f"{emoji('STOP')} <b>Создание остановлено</b>",
            reply_markup=get_functions_keyboard(),
        )
    except Exception:
        pass
    await callback.answer()


@dp.callback_query(F.data == "chat_creation_cancel")
async def cancel_chat_creation(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        f"{emoji('APPS')} <b>Функции</b>\n\nВыберите функцию:",
        reply_markup=get_functions_keyboard(),
    )
    await callback.answer()

# --- Вступление в чаты ---
AUTOSUB_DELAY = 10


def get_autosub_keyboard(account_id: int, active: bool = False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if active:
        builder.row(InlineKeyboardButton(
            text='Отключить Автосаб', callback_data=f'autosub_stop:{account_id}',
            style='destructive', icon_custom_emoji_id=get_icon('STOP')
        ))
    builder.row(InlineKeyboardButton(
        text='Назад', callback_data='functions', style='default',
        icon_custom_emoji_id=get_icon('BACK')
    ))
    return builder.as_markup()


def _extract_button_urls(message) -> List[str]:
    urls = []
    markup = getattr(message, 'reply_markup', None)
    for row in getattr(markup, 'rows', []) or []:
        for button in getattr(row, 'buttons', []) or []:
            url = getattr(button, 'url', None)
            if isinstance(url, str) and url.startswith(('https://t.me/', 'http://t.me/')):
                urls.append(url)
    return list(dict.fromkeys(urls))


async def _autosub_join_url(client: TelegramClient, url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    if not path:
        return False
    if path.startswith('+') or path.startswith('joinchat/'):
        invite_hash = path[1:] if path.startswith('+') else path.split('/', 1)[1]
        await client(ImportChatInviteRequest(invite_hash))
    else:
        username = path.split('/', 1)[0]
        entity = await client.get_entity('@' + username.lstrip('@'))
        await client(JoinChannelRequest(entity))
    return True


async def autosub_worker(account_id: int, user_id: int):
    client = await get_client_for_account(account_id)
    if not client:
        return
    me = await client.get_me()
    username = (getattr(me, 'username', None) or '').lower()
    if not username:
        return
    seen_urls = set()

    @client.on(events.NewMessage(incoming=True))
    async def handler(event):
        if autosub_stop_flags.get(account_id):
            return
        text = event.raw_text or ''
        if not re.search(r'(?<!\w)@' + re.escape(username) + r'\b', text, re.I):
            return
        for url in _extract_button_urls(event.message):
            if url in seen_urls:
                continue
            seen_urls.add(url)
            try:
                await _autosub_join_url(client, url)
                await add_account_log(account_id, url, 0, 'autosub_join', text[:100])
            except FloodWaitError as ex:
                await record_flood_wait(account_id, 0, ex.seconds)
                # Telegram уже дал точный cooldown — не обрезаем его,
                # иначе следующая попытка снова нарушит ограничение.
                await asyncio.sleep(ex.seconds + 1)
            except Exception as ex:
                logger.info('Autosub skipped %s: %s', url, ex)
            await asyncio.sleep(AUTOSUB_DELAY)

    try:
        while not autosub_stop_flags.get(account_id) and client.is_connected():
            await asyncio.sleep(1)
    finally:
        client.remove_event_handler(handler)
        autosub_stop_flags.pop(account_id, None)
        autosub_tasks.pop(account_id, None)


@dp.callback_query(F.data == 'autosub')
async def autosub_menu(callback: CallbackQuery, state: FSMContext):
    accounts = await get_user_accounts(callback.from_user.id)
    if not accounts:
        await callback.message.edit_text('У вас нет аккаунтов.', reply_markup=get_functions_keyboard())
        await callback.answer()
        return
    await callback.message.edit_text(
        f"{emoji('JOIN')} <b>Автосаб</b>\n\nВыберите аккаунт для запуска:",
        reply_markup=get_accounts_list_keyboard(accounts, 'autosub_account')
    )
    await state.set_state(AutoSubStates.waiting_for_account)
    await callback.answer()


@dp.callback_query(F.data.startswith('autosub_account_'), AutoSubStates.waiting_for_account)
async def autosub_start(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.rsplit('_', 1)[1])
    account = await get_account(account_id)
    if not account or account.get('user_id') != callback.from_user.id:
        await callback.answer('Аккаунт не найден', show_alert=True)
        return
    old = autosub_tasks.get(account_id)
    if old and not old.done():
        await callback.answer('Автосаб уже запущен', show_alert=True)
        return
    autosub_stop_flags[account_id] = False
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO autosub_configs (account_id, user_id, is_active)
               VALUES ($1, $2, TRUE)
               ON CONFLICT (account_id) DO UPDATE SET is_active = TRUE, updated_at = NOW()''',
            account_id, callback.from_user.id
        )
    task = asyncio.create_task(autosub_worker(account_id, callback.from_user.id))
    autosub_tasks[account_id] = task
    await state.clear()
    await callback.message.edit_text(
        f"{emoji('CHECK')} <b>Автосаб запущен</b>\n\n"
        f"Аккаунт: <code>{escape(account.get('phone') or str(account_id))}</code>\n"
        f"Проверка упоминаний и URL-кнопок активна.\n"
        f"Задержка между вступлениями: <b>{AUTOSUB_DELAY} секунд</b>",
        reply_markup=get_autosub_keyboard(account_id, True)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith('autosub_stop:'))
async def autosub_stop(callback: CallbackQuery):
    account_id = int(callback.data.split(':', 1)[1])
    autosub_stop_flags[account_id] = True
    task = autosub_tasks.get(account_id)
    if task and not task.done():
        task.cancel()
    async with db_pool.acquire() as conn:
        await conn.execute('UPDATE autosub_configs SET is_active = FALSE, updated_at = NOW() WHERE account_id = $1', account_id)
    await callback.message.edit_text('Автосаб отключён.', reply_markup=get_functions_keyboard())
    await callback.answer('Отключено')


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

# --- Нейрокомментинг ---
def get_neurocomment_accounts_keyboard(accounts: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for account in accounts:
        builder.row(InlineKeyboardButton(
            text=str(account['phone']),
            callback_data=f"neurocomm:account:{account['id']}",
            style='default',
            icon_custom_emoji_id=get_icon('PROFILE'),
        ))
    builder.row(InlineKeyboardButton(
        text='Отмена', callback_data='neurocomm:cancel',
        style='danger', icon_custom_emoji_id=get_icon('CROSS'),
    ))
    return builder.as_markup()


async def render_neurocomment_menu(user_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    configs = await get_neurocomment_configs(user_id)
    text = (
        f"{emoji('AI')} <b>Нейрокомментинг</b>\n\n"
        "Мониторит новые посты выбранных каналов и публикует комментарии "
        "от имени выбранного аккаунта.\n\n"
        f"Сохранённых конфигураций: <b>{len(configs)}</b>"
    )
    builder = InlineKeyboardBuilder()
    for config in configs[:20]:
        status = '●' if config.get('is_active') else '○'
        mode = 'ИИ' if config.get('mode') == NEUROCOMMENT_MODE_AI else 'Шаблоны'
        phone = str(config.get('phone') or config.get('account_id'))
        builder.row(InlineKeyboardButton(
            text=f"{status} {phone} · {len(config.get('channel_ids') or [])} кан. · {mode}",
            callback_data=f"neurocomm:view:{config['id']}",
            style='success' if config.get('is_active') else 'default',
            icon_custom_emoji_id=get_icon('AI'),
        ))
    builder.row(InlineKeyboardButton(
        text='Создать нейрокомментинг', callback_data='neurocomm:new',
        style='primary', icon_custom_emoji_id=get_icon('ADD_TEXT'),
    ))
    builder.row(InlineKeyboardButton(
        text='Назад', callback_data='functions',
        style='default', icon_custom_emoji_id=get_icon('BACK'),
    ))
    return text, builder.as_markup()


def _neurocomment_channels_text(
    channels: List[Dict[str, Any]], page: int, selected: List[str],
) -> str:
    total_pages = max(1, (len(channels) - 1) // 10 + 1)
    warning = (
        f"\n{emoji('INFO')} Для более безопасной нагрузки рекомендуется выбрать до <b>30</b> каналов."
    )
    if len(selected) > 30:
        warning += f"\n{emoji('WARNING')} Сейчас выбрано: <b>{len(selected)}</b>. Лимит не блокируется, но риск ограничений выше."
    return (
        f"{emoji('GLOBE')} <b>Выберите каналы для нейрокомментинга</b>\n\n"
        f"Выбрано: <b>{len(selected)}</b>\n"
        f"Страница {page + 1} из {total_pages}\n"
        "Можно выбрать любое число каналов из загруженных."
        f"{warning}"
    )


async def _show_neurocomment_channels(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    channels = data.get('neurocomment_channels') or []
    selected = data.get('neurocomment_selected_channels') or []
    page = int(data.get('neurocomment_page') or 0)
    await callback.message.edit_text(
        _neurocomment_channels_text(channels, page, selected),
        reply_markup=get_neurocomment_channel_keyboard(channels, page, selected),
    )


async def _start_saved_neurocomment(config: Dict[str, Any]) -> Tuple[bool, str]:
    account_id = int(config['account_id'])
    account = await get_account(account_id)
    if not account or not account.get('is_active'):
        return False, 'Выбранный аккаунт не найден или неактивен'
    other_id = await find_active_neurocomment_for_account(account_id)
    if other_id is not None and other_id != int(config['id']):
        return False, f'На этом аккаунте уже запущен нейрокомментинг #{other_id}'
    await set_neurocomment_active(int(config['id']), True)
    if not await start_neurocomment_worker(int(config['id'])):
        await set_neurocomment_active(int(config['id']), False, 'Не удалось запустить воркер')
        return False, 'Не удалось запустить воркер'
    return True, ''


@dp.callback_query(F.data == 'neurocomment')
async def neurocomment_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    text, markup = await render_neurocomment_menu(callback.from_user.id)
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


@dp.callback_query(F.data == 'neurocomm:new')
async def neurocomment_new(callback: CallbackQuery, state: FSMContext):
    accounts = [
        item for item in await get_user_accounts(callback.from_user.id)
        if item.get('is_active')
    ]
    if not accounts:
        await callback.message.edit_text(
            f"{emoji('CROSS')} Нет активных аккаунтов для работы.",
            reply_markup=get_functions_keyboard(),
        )
        await callback.answer()
        return
    await state.clear()
    await state.set_state(NeuroCommentStates.waiting_for_account)
    await callback.message.edit_text(
        f"{emoji('PROFILE')} <b>Выберите аккаунт для нейрокомментинга</b>",
        reply_markup=get_neurocomment_accounts_keyboard(accounts),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith('neurocomm:account:'), NeuroCommentStates.waiting_for_account)
async def neurocomment_select_account(callback: CallbackQuery, state: FSMContext):
    try:
        account_id = int(callback.data.rsplit(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректный аккаунт', show_alert=True)
        return
    account = await get_account(account_id)
    if not account or account.get('user_id') != callback.from_user.id or not account.get('is_active'):
        await callback.answer('Аккаунт не найден или неактивен', show_alert=True)
        return
    client = await get_client_for_account(account_id)
    if not client:
        await callback.answer('Не удалось подключиться к аккаунту', show_alert=True)
        return
    await callback.message.edit_text(f"{emoji('LOADING')} Загружаю каналы…")
    try:
        channels = [
            item for item in await get_chats_from_client(client, limit=1000)
            if item.get('type') == 'channel'
        ]
    except Exception as ex:
        await callback.message.edit_text(
            f"{emoji('CROSS')} Не удалось загрузить каналы:\n<code>{escape(str(ex)[:500])}</code>",
            reply_markup=get_functions_keyboard(),
        )
        await callback.answer()
        return
    if not channels:
        await callback.message.edit_text(
            f"{emoji('INFO')} У аккаунта нет доступных каналов.",
            reply_markup=get_functions_keyboard(),
        )
        await callback.answer()
        return
    await state.update_data(
        neurocomment_account_id=account_id,
        neurocomment_channels=channels,
        neurocomment_selected_channels=[],
        neurocomment_page=0,
        neurocomment_templates=[],
    )
    await state.set_state(NeuroCommentStates.selecting_channels)
    await _show_neurocomment_channels(callback, state)
    await callback.answer()


@dp.callback_query(F.data.startswith('neurocomm:toggle:'), NeuroCommentStates.selecting_channels)
async def neurocomment_toggle_channel(callback: CallbackQuery, state: FSMContext):
    channel_id = callback.data.rsplit(':', 1)[1]
    data = await state.get_data()
    selected = [str(item) for item in (data.get('neurocomment_selected_channels') or [])]
    if channel_id in selected:
        selected.remove(channel_id)
    else:
        selected.append(channel_id)
    await state.update_data(neurocomment_selected_channels=selected)
    await _show_neurocomment_channels(callback, state)
    await callback.answer()


@dp.callback_query(F.data.startswith('neurocomm:page:'), NeuroCommentStates.selecting_channels)
async def neurocomment_channels_page(callback: CallbackQuery, state: FSMContext):
    try:
        page = max(0, int(callback.data.rsplit(':', 1)[1]))
    except (AttributeError, ValueError):
        await callback.answer('Некорректная страница', show_alert=True)
        return
    await state.update_data(neurocomment_page=page)
    await _show_neurocomment_channels(callback, state)
    await callback.answer()


@dp.callback_query(F.data == 'neurocomm:channels_done', NeuroCommentStates.selecting_channels)
async def neurocomment_channels_done(callback: CallbackQuery, state: FSMContext):
    selected = (await state.get_data()).get('neurocomment_selected_channels') or []
    if not selected:
        await callback.answer('Выберите хотя бы один канал', show_alert=True)
        return
    await state.set_state(NeuroCommentStates.choosing_mode)
    await callback.message.edit_text(
        f"{emoji('AI')} <b>Выберите режим комментариев</b>\n\n"
        "<b>Только ИИ</b> — текст поста отправляется в модель; если текста нет, "
        "используется изображение поста.\n\n"
        "<b>Заготовленные сообщения</b> — бот выбирает один из ваших вариантов.",
        reply_markup=get_neurocomment_mode_keyboard(),
    )
    await callback.answer()


async def _neurocomment_ask_delay(target, state: FSMContext) -> None:
    await target.answer(
        f"{emoji('CLOCK')} <b>Задержка перед комментарием</b>\n\n"
        "Введите число секунд между появлением поста и публикацией комментария.\n"
        "Допустимо: 0–86400. Для безопасности рекомендуется 30–300 секунд.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text='Отмена', callback_data='neurocomm:cancel', style='danger',
                icon_custom_emoji_id=get_icon('CROSS'),
            )
        ]]),
    )
    await state.set_state(NeuroCommentStates.waiting_for_delay)


@dp.callback_query(F.data == 'neurocomm:mode:ai', NeuroCommentStates.choosing_mode)
async def neurocomment_mode_ai(callback: CallbackQuery, state: FSMContext):
    models = await get_user_llm_models(callback.from_user.id)
    if not models:
        await callback.answer('Для выбранного API нет доступных моделей', show_alert=True)
        return
    current = await get_user_llm_model(callback.from_user.id)
    if current not in models:
        current = models[0]
    await state.update_data(
        neurocomment_mode=NEUROCOMMENT_MODE_AI,
        neurocomment_models=models,
        neurocomment_model=current,
    )
    await state.set_state(NeuroCommentStates.choosing_model)
    await callback.message.edit_text(
        f"{emoji('AI')} <b>Выберите модель для нейрокомментинга</b>\n\n"
        "Эта модель будет использоваться для генерации комментариев к новым постам.",
        reply_markup=get_neurocomment_model_keyboard(models, current),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith('neurocomm:model:'), NeuroCommentStates.choosing_model)
async def neurocomment_select_model(callback: CallbackQuery, state: FSMContext):
    try:
        index = int(callback.data.rsplit(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректная модель', show_alert=True)
        return
    models = (await state.get_data()).get('neurocomment_models') or []
    if not 0 <= index < len(models):
        await callback.answer('Модель не найдена', show_alert=True)
        return
    selected_model = str(models[index])
    await state.update_data(neurocomment_model=selected_model)
    await _neurocomment_ask_delay(callback.message, state)
    await callback.answer(f"Модель: {LLM_MODELS.get(selected_model, selected_model)}")


@dp.callback_query(F.data == 'neurocomm:mode:templates', NeuroCommentStates.choosing_mode)
async def neurocomment_mode_templates(callback: CallbackQuery, state: FSMContext):
    await state.update_data(neurocomment_mode=NEUROCOMMENT_MODE_TEMPLATES, neurocomment_templates=[])
    await state.set_state(NeuroCommentStates.collecting_templates)
    await callback.message.edit_text(
        f"{emoji('CLIPBOARD')} <b>Заготовленные комментарии</b>\n\n"
        "Отправьте первый вариант текста. Можно добавить от <b>1</b> до <b>100</b> вариантов.\n"
        "После каждого варианта отправляйте следующий или нажмите «Готово».",
        reply_markup=get_neurocomment_templates_keyboard(0),
    )
    await callback.answer()


@dp.message(NeuroCommentStates.collecting_templates)
async def neurocomment_collect_template(message: Message, state: FSMContext):
    text = (message.text or '').strip()
    if not text:
        await message.answer('Отправьте вариант комментария обычным текстом.')
        return
    if len(text) > 1000:
        await message.answer('Один вариант не должен превышать 1000 символов.')
        return
    data = await state.get_data()
    templates = list(data.get('neurocomment_templates') or [])
    if len(templates) >= NEUROCOMMENT_MAX_TEMPLATE_VARIANTS:
        await message.answer(
            f"Достигнут лимит {NEUROCOMMENT_MAX_TEMPLATE_VARIANTS} вариантов. Нажмите «Готово».",
            reply_markup=get_neurocomment_templates_keyboard(len(templates)),
        )
        return
    templates.append(text)
    await state.update_data(neurocomment_templates=templates)
    await message.answer(
        f"{emoji('CHECK')} Вариант <b>#{len(templates)}</b> сохранён.\n\n"
        "Отправьте следующий вариант или нажмите «Готово».",
        reply_markup=get_neurocomment_templates_keyboard(len(templates)),
    )


@dp.callback_query(F.data == 'neurocomm:templates_done', NeuroCommentStates.collecting_templates)
async def neurocomment_templates_done(callback: CallbackQuery, state: FSMContext):
    templates = (await state.get_data()).get('neurocomment_templates') or []
    if not templates:
        await callback.answer('Добавьте хотя бы один вариант текста', show_alert=True)
        return
    await _neurocomment_ask_delay(callback.message, state)
    await callback.answer()


@dp.message(NeuroCommentStates.waiting_for_delay)
async def neurocomment_delay(message: Message, state: FSMContext):
    try:
        delay = int((message.text or '').strip())
        if not 0 <= delay <= 86400:
            raise ValueError
    except ValueError:
        await message.answer('Введите целое число от 0 до 86400 секунд.')
        return
    await state.update_data(neurocomment_delay=delay)
    data = await state.get_data()
    mode = data.get('neurocomment_mode')
    mode_label = 'Только ИИ' if mode == NEUROCOMMENT_MODE_AI else 'Заготовленные сообщения'
    selected = data.get('neurocomment_selected_channels') or []
    templates = data.get('neurocomment_templates') or []
    model = data.get('neurocomment_model') or ''
    model_line = (
        f"Модель ИИ: <b>{escape(str(LLM_MODELS.get(model, model)))}</b>\n"
        if mode == NEUROCOMMENT_MODE_AI else ''
    )
    await state.set_state(NeuroCommentStates.preview)
    await message.answer(
        f"{emoji('EYE')} <b>Предпросмотр нейрокомментинга</b>\n\n"
        f"Каналов: <b>{len(selected)}</b>\n"
        f"Режим: <b>{mode_label}</b>\n"
        f"{model_line}"
        f"Заготовленных вариантов: <b>{len(templates)}</b>\n"
        f"Задержка после поста: <b>{delay} сек.</b>\n\n"
        "Бот будет обрабатывать только новые посты, которые выйдут после запуска.",
        reply_markup=get_neurocomment_preview_keyboard(),
    )


@dp.callback_query(F.data == 'neurocomm:start_new', NeuroCommentStates.preview)
async def neurocomment_start_new(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    account_id = int(data.get('neurocomment_account_id') or 0)
    account = await get_account(account_id)
    if not account or account.get('user_id') != callback.from_user.id:
        await callback.answer('Аккаунт не найден', show_alert=True)
        return
    active_id = await find_active_neurocomment_for_account(account_id)
    if active_id is not None:
        await callback.answer(
            f'На этом аккаунте уже работает конфигурация #{active_id}. Сначала остановите её.',
            show_alert=True,
        )
        return
    try:
        config_id = await create_neurocomment_config(
            callback.from_user.id,
            account_id,
            [str(item) for item in (data.get('neurocomment_selected_channels') or [])],
            data.get('neurocomment_mode') or NEUROCOMMENT_MODE_AI,
            data.get('neurocomment_model') or None,
            list(data.get('neurocomment_templates') or []),
            int(data.get('neurocomment_delay') or 0),
        )
        config = await get_neurocomment_config(config_id, callback.from_user.id)
        ok, error = await _start_saved_neurocomment(config)
    except Exception as ex:
        logger.exception('Could not create neurocomment config')
        await callback.answer(f'Ошибка: {str(ex)[:200]}', show_alert=True)
        return
    await state.clear()
    config = await get_neurocomment_config(config_id, callback.from_user.id)
    if not ok:
        await callback.message.edit_text(
            f"{emoji('CROSS')} <b>Конфигурация сохранена, но не запущена.</b>\n\n"
            f"{escape(error)}\n\n{format_neurocomment_config(config)}",
            reply_markup=get_neurocomment_config_keyboard(config),
        )
    else:
        await callback.message.edit_text(
            f"{emoji('CHECK')} <b>Нейрокомментинг запущен.</b>\n\n"
            f"{format_neurocomment_config(config)}",
            reply_markup=get_neurocomment_config_keyboard(config),
        )
    await callback.answer()


@dp.callback_query(F.data.startswith('neurocomm:view:'))
async def neurocomment_view(callback: CallbackQuery):
    try:
        config_id = int(callback.data.rsplit(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректная конфигурация', show_alert=True)
        return
    config = await get_neurocomment_config(config_id, callback.from_user.id)
    if not config:
        await callback.answer('Конфигурация не найдена', show_alert=True)
        return
    await callback.message.edit_text(
        format_neurocomment_config(config),
        reply_markup=get_neurocomment_config_keyboard(config),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith('neurocomm:start:'))
async def neurocomment_start_saved(callback: CallbackQuery):
    try:
        config_id = int(callback.data.rsplit(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректная конфигурация', show_alert=True)
        return
    config = await get_neurocomment_config(config_id, callback.from_user.id)
    if not config:
        await callback.answer('Конфигурация не найдена', show_alert=True)
        return
    ok, error = await _start_saved_neurocomment(config)
    config = await get_neurocomment_config(config_id, callback.from_user.id)
    if not ok:
        await callback.answer(error, show_alert=True)
    else:
        await callback.answer('Запущено')
    await callback.message.edit_text(
        format_neurocomment_config(config),
        reply_markup=get_neurocomment_config_keyboard(config),
    )


@dp.callback_query(F.data.startswith('neurocomm:stop:'))
async def neurocomment_stop(callback: CallbackQuery):
    try:
        config_id = int(callback.data.rsplit(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректная конфигурация', show_alert=True)
        return
    config = await get_neurocomment_config(config_id, callback.from_user.id)
    if not config:
        await callback.answer('Конфигурация не найдена', show_alert=True)
        return
    await stop_neurocomment_worker(config_id)
    config = await get_neurocomment_config(config_id, callback.from_user.id)
    await callback.message.edit_text(
        format_neurocomment_config(config),
        reply_markup=get_neurocomment_config_keyboard(config),
    )
    await callback.answer('Остановлено')


@dp.callback_query(F.data.startswith('neurocomm:delete:'))
async def neurocomment_delete(callback: CallbackQuery):
    try:
        config_id = int(callback.data.rsplit(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректная конфигурация', show_alert=True)
        return
    config = await get_neurocomment_config(config_id, callback.from_user.id)
    if not config:
        await callback.answer('Конфигурация не найдена', show_alert=True)
        return
    await stop_neurocomment_worker(config_id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            'DELETE FROM neurocomment_configs WHERE id = $1 AND user_id = $2',
            config_id, callback.from_user.id,
        )
    text, markup = await render_neurocomment_menu(callback.from_user.id)
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer('Удалено')


@dp.callback_query(F.data == 'neurocomm:cancel')
async def neurocomment_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    text, markup = await render_neurocomment_menu(callback.from_user.id)
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer('Отменено')


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
async def admin_refresh_stats(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.clear()
    admin_text, markup = await build_admin_panel()
    await callback.message.edit_text(admin_text, reply_markup=markup)
    await callback.answer()

@dp.callback_query(F.data == 'admin_llm_menu')
async def admin_llm_menu(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback):
        await callback.answer('Нет доступа', show_alert=True)
        return
    await state.clear()
    text, markup = await render_admin_llm_menu()
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


@dp.callback_query(F.data == 'admin_llm_add')
async def admin_llm_add_start(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback):
        await callback.answer('Нет доступа', show_alert=True)
        return
    await state.clear()
    await state.set_state(AdminLLMConfigStates.waiting_for_name)
    await callback.message.edit_text(
        f"{emoji('AI')} <b>Новый базовый AI API</b>\n\n"
        "Введите внутреннее название API, например: <code>Основной SmartAPI</code>.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text='Отмена', callback_data='admin_llm_cancel', style='default',
                icon_custom_emoji_id=get_icon('BACK'),
            )
        ]]),
    )
    await callback.answer()


@dp.message(AdminLLMConfigStates.waiting_for_name)
async def admin_llm_receive_name(message: Message, state: FSMContext):
    if not _is_admin(message):
        return
    name = ' '.join((message.text or '').split()).strip()
    if not 2 <= len(name) <= 48:
        await message.answer('Название должно содержать от 2 до 48 символов.')
        return
    await state.update_data(admin_llm_name=name)
    await state.set_state(AdminLLMConfigStates.waiting_for_base_url)
    await message.answer(
        'Отправьте base URL Anthropic-совместимого API, например <code>https://api.example.com</code>.',
    )


@dp.message(AdminLLMConfigStates.waiting_for_base_url)
async def admin_llm_receive_base_url(message: Message, state: FSMContext):
    if not _is_admin(message):
        return
    base_url = (message.text or '').strip().rstrip('/')
    if not _valid_admin_llm_base_url(base_url):
        await message.answer('Нужен корректный http:// или https:// URL без логина и пароля.')
        return
    await state.update_data(admin_llm_base_url=base_url)
    await state.set_state(AdminLLMConfigStates.waiting_for_api_key)
    await message.answer('Отправьте API-токен. Он будет зашифрован и не показывается повторно.')


@dp.message(AdminLLMConfigStates.waiting_for_api_key)
async def admin_llm_receive_api_key(message: Message, state: FSMContext):
    if not _is_admin(message):
        return
    api_key = (message.text or '').strip()
    if not 8 <= len(api_key) <= 4096:
        await message.answer('Токен имеет некорректную длину. Отправьте его ещё раз.')
        return
    data = await state.get_data()
    try:
        api_id = await create_admin_llm_api(
            data['admin_llm_name'], data['admin_llm_base_url'], api_key,
        )
    except Exception:
        logger.exception('Could not save admin LLM API')
        await state.clear()
        await message.answer('Не удалось сохранить API. Попробуйте позже.')
        return
    await state.clear()
    await state.update_data(admin_llm_api_id=api_id)
    await state.set_state(AdminLLMConfigStates.waiting_for_model_api_name)
    await message.answer(
        f"{emoji('AI')} API сохранён.\n\n"
        "Теперь отправьте <b>техническое имя модели</b>, которое нужно передавать "
        "в API, например <code>claude-3-5-sonnet</code>.\n\n"
        "Допустимы латинские буквы, цифры, <code>._:/-</code>; максимум 34 символа.",
    )


@dp.callback_query(F.data.startswith('admin_llm_model_add:'))
async def admin_llm_model_add_start(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback):
        await callback.answer('Нет доступа', show_alert=True)
        return
    try:
        api_id = int(callback.data.split(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректный API', show_alert=True)
        return
    if not await get_admin_llm_api(api_id):
        await callback.answer('API не найден', show_alert=True)
        return
    await state.clear()
    await state.update_data(admin_llm_api_id=api_id)
    await state.set_state(AdminLLMConfigStates.waiting_for_model_api_name)
    await callback.message.edit_text(
        f"{emoji('AI')} <b>Добавление модели</b>\n\n"
        "Отправьте техническое имя модели для API, например "
        "<code>claude-3-5-sonnet</code>.\n"
        "Максимум 34 символа; допустимы <code>._:/-</code>.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text='Отмена', callback_data=f'admin_llm_api:{api_id}', style='default',
                icon_custom_emoji_id=get_icon('BACK'),
            )
        ]]),
    )
    await callback.answer()


@dp.message(AdminLLMConfigStates.waiting_for_model_api_name)
async def admin_llm_receive_model_api_name(message: Message, state: FSMContext):
    if not _is_admin(message):
        return
    model_name = (message.text or '').strip()
    if not re.fullmatch(r'[A-Za-z0-9._:/-]{1,34}', model_name):
        await message.answer(
            'Некорректное имя. Используйте до 34 символов: латинские буквы, цифры, <code>._:/-</code>.',
        )
        return
    await state.update_data(admin_llm_model_api_name=model_name)
    await state.set_state(AdminLLMConfigStates.waiting_for_model_display_name)
    await message.answer(
        "Теперь отправьте название, которое будет показано пользователям на кнопке, "
        "например <code>Claude Sonnet 3.5</code>.",
    )


@dp.message(AdminLLMConfigStates.waiting_for_model_display_name)
async def admin_llm_receive_model_display_name(message: Message, state: FSMContext):
    if not _is_admin(message):
        return
    display_name = ' '.join((message.text or '').split()).strip()
    if not 1 <= len(display_name) <= 48:
        await message.answer('Название для кнопки должно содержать от 1 до 48 символов.')
        return
    data = await state.get_data()
    api_id = int(data.get('admin_llm_api_id') or 0)
    try:
        await add_admin_llm_model(
            api_id,
            data['admin_llm_model_api_name'],
            display_name,
        )
    except Exception as ex:
        logger.warning('Could not save admin LLM model: %s', ex)
        await message.answer('Не удалось сохранить модель. Возможно, она уже добавлена.')
        return
    await state.clear()
    text, markup = await render_admin_llm_api_card(api_id)
    await message.answer(
        f"{emoji('CHECK')} Модель сохранена.\n\n{text}",
        reply_markup=markup,
    )


@dp.callback_query(F.data.startswith('admin_llm_api:'))
async def admin_llm_api_card(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback):
        await callback.answer('Нет доступа', show_alert=True)
        return
    try:
        api_id = int(callback.data.split(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректный API', show_alert=True)
        return
    await state.clear()
    text, markup = await render_admin_llm_api_card(api_id)
    if text is None:
        await callback.answer('API не найден', show_alert=True)
        return
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


@dp.callback_query(F.data == 'admin_llm_test_builtin')
async def admin_llm_test_builtin(callback: CallbackQuery):
    if not _is_admin(callback):
        await callback.answer('Нет доступа', show_alert=True)
        return
    await callback.answer('Проверяю модели…')
    await callback.message.edit_text(
        f"{emoji('LOADING')} <b>Проверяю модели API из кода…</b>",
    )
    results = await test_builtin_llm_models()
    text = format_llm_models_test_report('Тест моделей API из кода', results)
    _, markup = await render_admin_llm_menu()
    await callback.message.edit_text(text, reply_markup=markup)


@dp.callback_query(F.data.startswith('admin_llm_test_all:'))
async def admin_llm_test_all(callback: CallbackQuery):
    if not _is_admin(callback):
        await callback.answer('Нет доступа', show_alert=True)
        return
    try:
        api_id = int(callback.data.split(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректный API', show_alert=True)
        return
    api = await get_admin_llm_api(api_id)
    if not api:
        await callback.answer('API не найден', show_alert=True)
        return
    await callback.answer('Проверяю все модели…')
    await callback.message.edit_text(
        f"{emoji('LOADING')} <b>Проверяю модели: {escape(str(api['name']))}</b>\n\n"
        'Запросы выполняются по очереди, чтобы получить отдельную ошибку каждой модели.',
    )
    results = await test_admin_llm_api_models(api_id)
    if results is None:
        await callback.message.edit_text(
            f"{emoji('CROSS')} API не найден.",
            reply_markup=(await render_admin_llm_menu())[1],
        )
        return
    text = format_llm_models_test_report(
        f"Тест всех моделей: {api['name']}", results,
    )
    _, markup = await render_admin_llm_api_card(api_id)
    await callback.message.edit_text(text, reply_markup=markup)


@dp.callback_query(F.data.startswith('admin_llm_test_model:'))
async def admin_llm_test_model(callback: CallbackQuery):
    if not _is_admin(callback):
        await callback.answer('Нет доступа', show_alert=True)
        return
    try:
        model_id = int(callback.data.split(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректная модель', show_alert=True)
        return
    await callback.answer('Проверяю модель…')
    await callback.message.edit_text(
        f"{emoji('LOADING')} <b>Проверяю модель…</b>",
    )
    result = await test_admin_llm_model(model_id)
    if result is None:
        await callback.message.edit_text(
            f"{emoji('CROSS')} Модель не найдена.",
            reply_markup=(await render_admin_llm_menu())[1],
        )
        return
    text = format_llm_models_test_report(
        'Тест модели', [result], detailed=True,
    )
    _, markup = await render_admin_llm_api_card(int(result['api_id']))
    await callback.message.edit_text(text, reply_markup=markup)


@dp.callback_query(F.data.startswith('admin_llm_activate:'))
async def admin_llm_activate(callback: CallbackQuery):
    if not _is_admin(callback):
        await callback.answer('Нет доступа', show_alert=True)
        return
    try:
        api_id = int(callback.data.split(':', 1)[1])
        await activate_admin_llm_api(api_id)
    except (ValueError, TypeError) as ex:
        await callback.answer(str(ex), show_alert=True)
        return
    except Exception:
        logger.exception('Could not activate admin LLM API')
        await callback.answer('Не удалось включить API', show_alert=True)
        return
    text, markup = await render_admin_llm_api_card(api_id)
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer('Базовый API изменён')


@dp.callback_query(F.data == 'admin_llm_builtin')
async def admin_llm_builtin(callback: CallbackQuery):
    if not _is_admin(callback):
        await callback.answer('Нет доступа', show_alert=True)
        return
    await use_builtin_llm_api()
    text, markup = await render_admin_llm_menu()
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer('Встроенный API включён')


@dp.callback_query(F.data.startswith('admin_llm_model_delete:'))
async def admin_llm_model_delete(callback: CallbackQuery):
    if not _is_admin(callback):
        await callback.answer('Нет доступа', show_alert=True)
        return
    try:
        model_id = int(callback.data.split(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректная модель', show_alert=True)
        return
    api_id = await delete_admin_llm_model(model_id)
    if api_id is None:
        await callback.answer('Модель не найдена', show_alert=True)
        return
    text, markup = await render_admin_llm_api_card(api_id)
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer('Модель удалена')


@dp.callback_query(F.data.startswith('admin_llm_api_delete:'))
async def admin_llm_api_delete(callback: CallbackQuery):
    if not _is_admin(callback):
        await callback.answer('Нет доступа', show_alert=True)
        return
    try:
        api_id = int(callback.data.split(':', 1)[1])
    except (AttributeError, ValueError):
        await callback.answer('Некорректный API', show_alert=True)
        return
    if not await delete_admin_llm_api(api_id):
        await callback.answer('API не найден', show_alert=True)
        return
    text, markup = await render_admin_llm_menu()
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer('API удалён')


@dp.callback_query(F.data == 'admin_llm_cancel')
async def admin_llm_cancel(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback):
        await callback.answer('Нет доступа', show_alert=True)
        return
    await state.clear()
    text, markup = await render_admin_llm_menu()
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer('Отменено')


@dp.callback_query(F.data == 'admin_finance')
async def admin_finance(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer('Нет доступа', show_alert=True)
        return
    await state.clear()
    text, markup = await render_admin_finance(30)
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


@dp.callback_query(F.data.startswith('admin_finance:'))
async def admin_finance_period(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer('Нет доступа', show_alert=True)
        return
    period = normalize_admin_finance_period(callback.data.split(':', 1)[1])
    text, markup = await render_admin_finance(period)
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


@dp.callback_query(F.data.startswith('admin_finance_recent:'))
async def admin_finance_recent(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer('Нет доступа', show_alert=True)
        return
    period = normalize_admin_finance_period(callback.data.split(':', 1)[1])
    rows = await get_admin_payment_events(period, limit=15)
    lines = [
        f"{emoji('MONEY_SEND')} <b>Последние платежи</b>",
        f"Период: <b>{ADMIN_FINANCE_PERIODS[period]}</b>",
        '',
    ]
    if not rows:
        lines.append('Подтверждённых платежей за этот период нет.')
    else:
        for row in rows:
            paid_at = row.get('paid_at')
            date_text = (
                paid_at.strftime('%d.%m.%Y %H:%M')
                if hasattr(paid_at, 'strftime') else '—'
            )
            kind = FINANCE_KIND_LABELS.get(row.get('kind'), row.get('kind') or 'Платёж')
            provider = FINANCE_PROVIDER_LABELS.get(
                row.get('provider'), row.get('provider') or '—'
            )
            username = row.get('username')
            user_label = f'@{username}' if username else 'без username'
            user_id = row.get('user_id')
            lines.append(
                f"<b>{escape(str(kind))}</b> · {escape(str(provider))}\n"
                f"{format_finance_amounts(row.get('amount_rub'), row.get('amount_usdt'))} · "
                f"{date_text}\n"
                f"{escape(user_label)} · <code>{escape(str(user_id or '—'))}</code>"
            )

    builder = InlineKeyboardBuilder()
    if rows:
        builder.row(InlineKeyboardButton(
            text='Скачать CSV',
            callback_data=f'admin_finance_export:{period}',
            style='default',
            icon_custom_emoji_id=get_icon('FILE'),
        ))
    builder.row(InlineKeyboardButton(
        text='К финансам',
        callback_data=f'admin_finance:{period}',
        style='primary',
        icon_custom_emoji_id=get_icon('BACK'),
    ))
    await callback.message.edit_text('\n\n'.join(lines), reply_markup=builder.as_markup())
    await callback.answer()


@dp.callback_query(F.data.startswith('admin_finance_export:'))
async def admin_finance_export(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer('Нет доступа', show_alert=True)
        return
    period = normalize_admin_finance_period(callback.data.split(':', 1)[1])
    rows = await get_admin_payment_events(period, limit=10_000)
    if not rows:
        await callback.answer('За этот период нет платежей для экспорта', show_alert=True)
        return

    try:
        period_slug = 'all' if period == 0 else f'{period}d'
        filename = (
            f"payments_{period_slug}_"
            f"{datetime.now(MSK_TZ).strftime('%Y-%m-%d')}.csv"
        )
        await callback.message.answer_document(
            BufferedInputFile(build_admin_finance_csv(rows), filename=filename),
            caption=(
                f"{emoji('FILE')} <b>Экспорт платежей</b>\n\n"
                f"Период: <b>{ADMIN_FINANCE_PERIODS[period]}</b>\n"
                f"Строк в файле: <b>{len(rows)}</b>"
            ),
        )
        await callback.answer('CSV-файл готов')
    except Exception as ex:
        logger.exception('Finance CSV export failed: %s', ex)
        await callback.answer('Не удалось создать CSV-файл', show_alert=True)


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


# ========== ПОДАРИТЬ ПОДПИСКУ (ADMIN) ==========

@dp.callback_query(F.data == "admin_gift_sub")
async def admin_gift_sub_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_for_gift_user_id)
    await callback.message.edit_text(
        f"{emoji('STAR')} <b>Подарить Pro-подписку</b>\n\n"
        f"Введите Telegram <b>user_id</b> пользователя, которому хотите "
        f"подарить подписку:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Отмена",
                callback_data="admin_refresh_stats",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await callback.answer()


@dp.message(AdminStates.waiting_for_gift_user_id)
async def admin_gift_user_id(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    raw = (message.text or '').strip()
    if not raw.lstrip('-').isdigit():
        await message.answer(
            f"{emoji('CROSS')} Некорректный user_id. "
            f"Введите числовой Telegram ID:"
        )
        return
    target_id = int(raw)
    await state.update_data(gift_target_id=target_id)
    await state.set_state(AdminStates.waiting_for_gift_days)
    await message.answer(
        f"{emoji('STAR')} <b>Подарить подписку</b>\n\n"
        f"User ID: <code>{target_id}</code>\n\n"
        f"На сколько дней выдать Pro? (1–3650):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Отмена",
                callback_data="admin_refresh_stats",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )


@dp.message(AdminStates.waiting_for_gift_days)
async def admin_gift_days(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    raw = (message.text or '').strip()
    try:
        days = int(raw)
        if days < 1 or days > 3650:
            raise ValueError
    except ValueError:
        await message.answer(
            f"{emoji('CROSS')} Введите целое число от 1 до 3650:"
        )
        return

    data = await state.get_data()
    target_id = data.get("gift_target_id")
    if not target_id:
        await message.answer(f"{emoji('CROSS')} Сессия истекла. Начните заново.")
        await state.clear()
        return

    expires_at = datetime.now() + timedelta(days=days)
    await set_subscription(target_id, "pro", expires_at)

    await state.clear()

    await message.answer(
        f"{emoji('CHECK')} <b>Подписка выдана!</b>\n\n"
        f"User ID: <code>{target_id}</code>\n"
        f"Тариф: <b>Pro</b>\n"
        f"Срок до: <b>{expires_at.strftime('%d.%m.%Y %H:%M')}</b> "
        f"(+{days} дн.)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="В админ-панель",
                callback_data="admin_refresh_stats",
                style='primary',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )

    # Notify the recipient
    try:
        await bot.send_message(
            target_id,
            f"{emoji('STAR')} <b>Вам подарена Pro-подписка!</b>\n\n"
            f"Тариф: <b>Pro</b>\n"
            f"Активна до: <b>{expires_at.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"Все AI-функции и неограниченные рассылки теперь доступны!"
        )
    except Exception as ex:
        logger.warning(f"Could not notify gift recipient {target_id}: {ex}")


# ========== ПРОСМОТР ПОЛЬЗОВАТЕЛЕЙ (ADMIN) ==========

ADMIN_USERS_PAGE_SIZE = 8


def _fmt_dt(dt) -> str:
    """Безопасно форматирует дату/время для админ-карточек."""
    if not dt:
        return "—"
    try:
        return dt.strftime('%d.%m.%Y %H:%M')
    except Exception:
        return str(dt)


@dp.callback_query(F.data.startswith("admin_users:"))
async def admin_users_list(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return

    try:
        offset = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        offset = 0
    if offset < 0:
        offset = 0

    page = await get_users_page(offset, ADMIN_USERS_PAGE_SIZE)
    total = page['total']
    rows = page['rows']

    builder = InlineKeyboardBuilder()
    for u in rows:
        name = u.get('first_name') or u.get('username') or 'Без имени'
        badge = "👑" if u.get('tier') == 'max' else ("🔥" if u.get('tier') == 'pro' else "•")
        builder.row(InlineKeyboardButton(
            text=f"{badge} {name} (ID {u['user_id']})",
            callback_data=f"admin_user_view:{u['user_id']}",
            style='default',
            icon_custom_emoji_id=get_icon("PROFILE")
        ))

    # Навигация по страницам
    nav = []
    if offset > 0:
        nav.append(InlineKeyboardButton(
            text="◀ Назад",
            callback_data=f"admin_users:{max(offset - ADMIN_USERS_PAGE_SIZE, 0)}",
            style='default'
        ))
    if offset + ADMIN_USERS_PAGE_SIZE < total:
        nav.append(InlineKeyboardButton(
            text="Вперёд ▶",
            callback_data=f"admin_users:{offset + ADMIN_USERS_PAGE_SIZE}",
            style='default'
        ))
    if nav:
        builder.row(*nav)

    builder.row(InlineKeyboardButton(
        text="Найти по ID",
        callback_data="admin_user_lookup",
        style='primary',
        icon_custom_emoji_id=get_icon("ID")
    ))
    builder.row(InlineKeyboardButton(
        text="В админ-панель",
        callback_data="admin_refresh_stats",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))

    page_from = offset + 1 if total else 0
    page_to = min(offset + ADMIN_USERS_PAGE_SIZE, total)
    text = (
        f"{emoji('PEOPLE')} <b>Пользователи</b>\n\n"
        f"Всего: <b>{total}</b>\n"
        f"Показаны: <b>{page_from}–{page_to}</b>\n\n"
        f"Нажмите на пользователя, чтобы открыть карточку."
    )
    if not rows:
        text = f"{emoji('PEOPLE')} <b>Пользователи</b>\n\nСписок пуст."

    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()


def _build_user_card_markup(user_id: int, tier: str, back_offset: int = 0):
    """Клавиатура карточки пользователя с действиями по подписке."""
    builder = InlineKeyboardBuilder()
    if tier in ('pro', 'max'):
        builder.row(InlineKeyboardButton(
            text=f"Забрать {subscription_tier_label(tier)}",
            callback_data=f"admin_user_revoke:{user_id}",
            style='destructive',
            icon_custom_emoji_id=get_icon("LOCK_CLOSED")
        ))
    else:
        builder.row(InlineKeyboardButton(
            text="Выдать Pro на 30 дней",
            callback_data=f"admin_user_grant30:{user_id}",
            style='primary',
            icon_custom_emoji_id=get_icon("STAR")
        ))
    builder.row(InlineKeyboardButton(
        text="К списку",
        callback_data=f"admin_users:{back_offset}",
        style='default',
        icon_custom_emoji_id=get_icon("BACK")
    ))
    return builder.as_markup()


async def _render_user_card(target_id: int) -> tuple:
    card = await get_user_admin_card(target_id)
    if not card:
        return None, None
    tier = card.get('tier', 'free')
    tier_label = "👑 MAX" if tier == 'max' else ("🔥 Pro" if tier == 'pro' else "Free")
    username = f"@{card['username']}" if card.get('username') else "—"
    expires = _fmt_dt(card.get('expires_at')) if tier in ('pro', 'max') else "—"
    text = (
        f"{emoji('PROFILE')} <b>Карточка пользователя</b>\n\n"
        f"{emoji('ID')} ID: <code>{card['user_id']}</code>\n"
        f"{emoji('PEOPLE')} Имя: <b>{escape(card.get('first_name') or '—')}</b>\n"
        f"{emoji('CHAT')} Username: {escape(username)}\n"
        f"{emoji('CALENDAR')} Регистрация: <b>{_fmt_dt(card.get('joined_at'))}</b>\n\n"
        f"{emoji('STAR')} Тариф: <b>{tier_label}</b>\n"
        f"{emoji('CLOCK')} Активна до: <b>{expires}</b>\n\n"
        f"{emoji('PROFILE')} Аккаунтов: <b>{card['accounts_count']}</b>\n"
        f"{emoji('LINK')} Прокси: <b>{card['proxies_count']}</b>"
    )
    return text, _build_user_card_markup(target_id, tier)


@dp.callback_query(F.data.startswith("admin_user_view:"))
async def admin_user_view(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return
    try:
        target_id = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректный ID", show_alert=True)
        return

    text, markup = await _render_user_card(target_id)
    if text is None:
        await callback.answer("Пользователь не найден", show_alert=True)
        return
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


@dp.callback_query(F.data == "admin_user_lookup")
async def admin_user_lookup_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_for_user_lookup_id)
    await callback.message.edit_text(
        f"{emoji('ID')} <b>Поиск пользователя</b>\n\n"
        f"Введите Telegram <b>user_id</b> пользователя:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Отмена",
                callback_data="admin_users:0",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await callback.answer()


@dp.message(AdminStates.waiting_for_user_lookup_id)
async def admin_user_lookup_process(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    raw = (message.text or '').strip()
    if not raw.lstrip('-').isdigit():
        await message.answer(
            f"{emoji('CROSS')} Некорректный user_id. Введите числовой Telegram ID:"
        )
        return
    await state.clear()
    text, markup = await _render_user_card(int(raw))
    if text is None:
        await message.answer(
            f"{emoji('CROSS')} Пользователь с таким ID не найден.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="К списку",
                    callback_data="admin_users:0",
                    style='default',
                    icon_custom_emoji_id=get_icon("BACK")
                )
            ]])
        )
        return
    await message.answer(text, reply_markup=markup)


@dp.callback_query(F.data.startswith("admin_user_grant30:"))
async def admin_user_grant30(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return
    try:
        target_id = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректный ID", show_alert=True)
        return

    expires_at = datetime.now() + timedelta(days=30)
    await set_subscription(target_id, "pro", expires_at)
    await callback.answer("Pro выдан на 30 дней", show_alert=True)

    text, markup = await _render_user_card(target_id)
    if text:
        await callback.message.edit_text(text, reply_markup=markup)

    try:
        await bot.send_message(
            target_id,
            f"{emoji('STAR')} <b>Вам подарена Pro-подписка!</b>\n\n"
            f"Активна до: <b>{expires_at.strftime('%d.%m.%Y %H:%M')}</b>"
        )
    except Exception as ex:
        logger.warning(f"Could not notify grant recipient {target_id}: {ex}")


# ========== ЗАБРАТЬ ПОДПИСКУ (ADMIN) ==========

async def _revoke_subscription(target_id: int) -> None:
    """Сбрасывает подписку пользователя обратно на Free."""
    await set_subscription(target_id, "free", None)


async def _notify_revoked(target_id: int) -> None:
    try:
        await bot.send_message(
            target_id,
            f"{emoji('INFO')} <b>Ваша Pro-подписка была отключена администратором.</b>\n\n"
            f"Тариф изменён на <b>Free</b>."
        )
    except Exception as ex:
        logger.warning(f"Could not notify revoke recipient {target_id}: {ex}")


@dp.callback_query(F.data.startswith("admin_user_revoke:"))
async def admin_user_revoke(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return
    try:
        target_id = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Некорректный ID", show_alert=True)
        return

    await _revoke_subscription(target_id)
    await callback.answer("Pro-подписка забрана", show_alert=True)

    text, markup = await _render_user_card(target_id)
    if text:
        await callback.message.edit_text(text, reply_markup=markup)

    await _notify_revoked(target_id)


@dp.callback_query(F.data == "admin_revoke_sub")
async def admin_revoke_sub_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_for_revoke_user_id)
    await callback.message.edit_text(
        f"{emoji('LOCK_CLOSED')} <b>Забрать Pro-подписку</b>\n\n"
        f"Введите Telegram <b>user_id</b> пользователя, у которого нужно "
        f"забрать подписку:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Отмена",
                callback_data="admin_refresh_stats",
                style='default',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )
    await callback.answer()


@dp.message(AdminStates.waiting_for_revoke_user_id)
async def admin_revoke_process(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    raw = (message.text or '').strip()
    if not raw.lstrip('-').isdigit():
        await message.answer(
            f"{emoji('CROSS')} Некорректный user_id. Введите числовой Telegram ID:"
        )
        return
    target_id = int(raw)
    await state.clear()

    sub = await get_subscription(target_id)
    if sub.get('tier') not in ('pro', 'max'):
        await message.answer(
            f"{emoji('INFO')} У пользователя <code>{target_id}</code> "
            f"нет активной Pro/MAX-подписки (тариф уже Free).",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="В админ-панель",
                    callback_data="admin_refresh_stats",
                    style='primary',
                    icon_custom_emoji_id=get_icon("BACK")
                )
            ]])
        )
        return

    await _revoke_subscription(target_id)

    await message.answer(
        f"{emoji('CHECK')} <b>Подписка забрана!</b>\n\n"
        f"User ID: <code>{target_id}</code>\n"
        f"Новый тариф: <b>Free</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="В админ-панель",
                callback_data="admin_refresh_stats",
                style='primary',
                icon_custom_emoji_id=get_icon("BACK")
            )
        ]])
    )

    await _notify_revoked(target_id)


# ========== ПРОКСИ ==========

@dp.callback_query(F.data == "my_proxies")
async def my_proxies(callback: CallbackQuery):
    proxies = await get_user_proxies(callback.from_user.id)

    # Загружаем привязки: proxy_id -> [phone, ...]
    accounts_by_proxy: Dict[int, List[str]] = {}
    if proxies:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT proxy_id, phone FROM accounts "
                "WHERE user_id=$1 AND proxy_id IS NOT NULL",
                callback.from_user.id
            )
        for r in rows:
            accounts_by_proxy.setdefault(r['proxy_id'], []).append(r['phone'])

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
            auth_badge = " 🔑" if p.get('username') else ""
            bound = accounts_by_proxy.get(p['id'], [])
            bound_str = (
                f"привязан к: {', '.join(bound[:3])}"
                + (" и др." if len(bound) > 3 else "")
                if bound else "не привязан"
            )
            text += (
                f"<b>{escape(label)}</b>{auth_badge}\n"
                f"   <code>{p['proxy_type']}://{p['host']}:{p['port']}</code>\n"
                f"   {bound_str}\n\n"
            )

    await callback.message.edit_text(
        text, reply_markup=get_proxies_keyboard(proxies, accounts_by_proxy)
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
    parsed = parse_proxy_string(message.text or '')
    if not parsed or not parsed.get('host') or not parsed.get('port'):
        await message.answer(
            f"{emoji('CROSS')} Не удалось распарсить прокси. "
            f"Проверьте формат и попробуйте снова.\n\n"
            f"Примеры:\n"
            f"<code>socks5://user:pass@1.2.3.4:1080</code>\n"
            f"<code>1.2.3.4:1080:user:pass</code>"
        )
        return

    checking_message = await message.answer(
        f"{emoji('LOADING')} <b>Проверяю прокси…</b>\n\n"
        f"<code>{parsed['proxy_type']}://"
        f"{escape(str(parsed['host']))}:{parsed['port']}</code>\n"
        "Проверка выполняется реальным подключением к Telegram через прокси."
    )
    check_result = await check_proxy_connection(parsed)
    if not check_result.get('ok'):
        await checking_message.edit_text(
            f"{emoji('CROSS')} <b>Прокси не работает</b>\n\n"
            f"<code>{parsed['proxy_type']}://"
            f"{escape(str(parsed['host']))}:{parsed['port']}</code>\n\n"
            f"Ошибка: <code>"
            f"{escape(str(check_result.get('error') or 'нет соединения'))}"
            f"</code>\n\n"
            "Прокси не сохранён. Отправьте другой прокси.",
        )
        return

    await state.update_data(proxy=parsed, proxy_check=check_result)
    await checking_message.edit_text(
        f"{emoji('CHECK')} <b>Прокси работает</b>\n\n"
        f"<code>{parsed['proxy_type']}://"
        f"{escape(str(parsed['host']))}:{parsed['port']}</code>\n"
        f"Отклик: <b>{check_result['latency_ms']} мс</b>\n"
        f"Проверено через Telegram: "
        f"<code>{escape(str(check_result['target']))}</code>\n\n"
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
    check_result = data.get('proxy_check') or {}

    proxy_id = await add_proxy(
        message.from_user.id, parsed['proxy_type'], parsed['host'],
        parsed['port'], parsed.get('username'),
        parsed.get('password'), label
    )

    await message.answer(
        f"{emoji('CHECK')} Прокси добавлен (id={proxy_id}).\n"
        f"Проверенный отклик: <b>"
        f"{check_result.get('latency_ms', '—')} мс</b>.",
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
    check_result = data.get('proxy_check') or {}

    proxy_id = await add_proxy(
        callback.from_user.id, parsed['proxy_type'], parsed['host'],
        parsed['port'], parsed.get('username'),
        parsed.get('password'), None
    )

    await callback.message.edit_text(
        f"{emoji('CHECK')} Прокси добавлен (id={proxy_id}).\n"
        f"Проверенный отклик: <b>"
        f"{check_result.get('latency_ms', '—')} мс</b>.",
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

    # Привязанные аккаунты
    async with db_pool.acquire() as conn:
        bound_accounts = await conn.fetch(
            "SELECT phone FROM accounts WHERE proxy_id=$1 AND user_id=$2",
            proxy_id, callback.from_user.id
        )

    auth_line = (
        f"{emoji('KEY')} Аутентификация: <code>{escape(proxy['username'])}</code> / "
        f"<code>{'•' * min(len(proxy.get('password') or ''), 8)}</code>\n"
        if proxy.get('username') else ""
    )
    label = proxy.get('label') or '—'
    bound_str = (
        ", ".join(f"<code>{r['phone']}</code>" for r in bound_accounts)
        if bound_accounts else "<i>ни один</i>"
    )

    text = (
        f"{emoji('LINK')} <b>Прокси</b>\n"
        f"{'─' * 28}\n"
        f"{emoji('INFO')} Подпись: <b>{escape(label)}</b>\n"
        f"{emoji('LINK')} Тип: <code>{proxy['proxy_type']}</code>\n"
        f"{emoji('LINK')} Адрес: <code>{proxy['host']}:{proxy['port']}</code>\n"
        f"{auth_line}"
        f"{emoji('CLOCK')} Добавлен: <b>{proxy['created_at'].strftime('%d.%m.%Y %H:%M')}</b>\n\n"
        f"{emoji('PEOPLE')} Привязан к аккаунтам: {bound_str}\n\n"
        f"{emoji('INFO')} Нажмите «Проверить соединение» чтобы убедиться, что прокси работает."
    )

    await callback.message.edit_text(
        text, reply_markup=get_proxy_actions_keyboard(proxy_id)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("check_proxy_"))
async def check_proxy_handler(callback: CallbackQuery):
    """Проверяет конкретный прокси в реальном времени."""
    proxy_id = int(callback.data.split("_")[2])
    proxy = await get_proxy(proxy_id)

    if not proxy or proxy['user_id'] != callback.from_user.id:
        await callback.answer("Прокси не найден", show_alert=True)
        return

    await callback.message.edit_text(
        f"{emoji('LOADING')} <b>Проверяю прокси…</b>\n\n"
        f"<code>{proxy['proxy_type']}://{proxy['host']}:{proxy['port']}</code>",
        reply_markup=None
    )
    await callback.answer()

    proxy_dict = {
        'proxy_type': proxy['proxy_type'],
        'host': proxy['host'],
        'port': proxy['port'],
        'username': proxy.get('username'),
        'password': proxy.get('password'),
    }
    result = await check_proxy_connection(proxy_dict)

    label = proxy.get('label') or f"{proxy['host']}:{proxy['port']}"
    if result.get('ok'):
        status_text = (
            f"{emoji('CHECK')} <b>Прокси работает</b>\n\n"
            f"<code>{proxy['proxy_type']}://{proxy['host']}:{proxy['port']}</code>\n"
            f"Подпись: <b>{escape(label)}</b>\n"
            f"Отклик: <b>{result['latency_ms']} мс</b>\n"
            f"Проверено через: <code>{escape(str(result.get('target', '—')))}</code>"
        )
    else:
        status_text = (
            f"{emoji('CROSS')} <b>Прокси не работает</b>\n\n"
            f"<code>{proxy['proxy_type']}://{proxy['host']}:{proxy['port']}</code>\n"
            f"Подпись: <b>{escape(label)}</b>\n"
            f"Ошибка: <code>{escape(str(result.get('error', 'нет соединения')))}</code>"
        )

    await callback.message.edit_text(
        status_text, reply_markup=get_proxy_actions_keyboard(proxy_id)
    )


@dp.callback_query(F.data == "check_all_proxies")
async def check_all_proxies_handler(callback: CallbackQuery):
    """Проверяет все прокси пользователя последовательно."""
    proxies = await get_user_proxies(callback.from_user.id)
    if not proxies:
        await callback.answer("У вас нет прокси", show_alert=True)
        return

    await callback.message.edit_text(
        f"{emoji('LOADING')} <b>Проверяю {len(proxies)} прокси…</b>\n\n"
        f"Это может занять до {len(proxies) * 10} сек.",
        reply_markup=None
    )
    await callback.answer()

    results = []
    for p in proxies:
        proxy_dict = {
            'proxy_type': p['proxy_type'],
            'host': p['host'],
            'port': p['port'],
            'username': p.get('username'),
            'password': p.get('password'),
        }
        res = await check_proxy_connection(proxy_dict)
        label = p.get('label') or f"{p['host']}:{p['port']}"
        if res.get('ok'):
            results.append(f"✅ <b>{escape(label)}</b> — {res['latency_ms']} мс")
        else:
            err = escape(str(res.get('error', '—'))[:40])
            results.append(f"❌ <b>{escape(label)}</b> — {err}")

    ok_count = sum(1 for r in results if r.startswith("✅"))
    text = (
        f"{emoji('LINK')} <b>Результаты проверки прокси</b>\n"
        f"Работают: <b>{ok_count}/{len(proxies)}</b>\n\n"
        + "\n".join(results)
    )

    # Получаем привязки для клавиатуры
    accounts_by_proxy: Dict[int, List[str]] = {}
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT proxy_id, phone FROM accounts "
            "WHERE user_id=$1 AND proxy_id IS NOT NULL",
            callback.from_user.id
        )
    for r in rows:
        accounts_by_proxy.setdefault(r['proxy_id'], []).append(r['phone'])

    await callback.message.edit_text(
        text, reply_markup=get_proxies_keyboard(proxies, accounts_by_proxy)
    )


@dp.callback_query(F.data.startswith("delete_proxy_"))
async def delete_proxy_handler(callback: CallbackQuery):
    proxy_id = int(callback.data.split("_")[2])
    ok = await delete_proxy(proxy_id, callback.from_user.id)
    if ok:
        await callback.answer("Прокси удалён", show_alert=True)
    else:
        await callback.answer("Не удалось удалить", show_alert=True)

    proxies = await get_user_proxies(callback.from_user.id)
    accounts_by_proxy: Dict[int, List[str]] = {}
    if proxies:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT proxy_id, phone FROM accounts "
                "WHERE user_id=$1 AND proxy_id IS NOT NULL",
                callback.from_user.id
            )
        for r in rows:
            accounts_by_proxy.setdefault(r['proxy_id'], []).append(r['phone'])

    text = (
        f"{emoji('LINK')} <b>Ваши прокси ({len(proxies)}):</b>"
        if proxies else
        f"{emoji('LINK')} Прокси удалены. Добавьте новые при необходимости."
    )
    await callback.message.edit_text(
        text, reply_markup=get_proxies_keyboard(proxies, accounts_by_proxy)
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


# ============================================================
#  Per-account AI-автоответчик (живёт на аккаунте, а не в боте)
#  ────────────────────
#  Два режима:
#    • «Без ИИ (выключен)» — автоответчик на аккаунте не работает
#    • «С ИИ»              — каждое входящее ЛС на аккаунте уходит в LLM
#                             с заданной личностью (system_prompt)
#  Настраивается в меню управления конкретным аккаунтом
#  («Мои аккаунты» → выбрать аккаунт → «🤖 ИИ-автоответчик»).
#  Воркер крутится на Telethon-клиенте аккаунта и слушает
#  входящие ЛС. История хранится per-account × per-chat (JSONB).
# ============================================================

# Глобальный реестр воркеров: user_id -> {account_id -> Task}
active_account_ai_responders: Dict[int, Dict[int, asyncio.Task]] = {}


def _is_acct_ar_worker_running(account_id: int, user_id: int) -> bool:
    """Жив ли воркер прямо сейчас (задача в реестре и не завершена)."""
    try:
        task = active_account_ai_responders.get(user_id, {}).get(account_id)
        if task is None:
            return False
        if task.done():
            return False
        return True
    except Exception:
        return False


def _acct_ar_status_text(
    settings: Dict[str, Any], phone: str,
    account_id: int = 0, user_id: int = 0,
) -> str:
    mode = settings.get('mode') or ACCT_AR_MODE_OFF
    sysp = (settings.get('system_prompt') or '').strip() \
        or ACCT_AR_DEFAULT_SYSTEM_PROMPT
    model = (settings.get('model') or '').strip() or LLM_DEFAULT_MODEL
    history = settings.get('history') or {}
    chats = len(history)
    total_msgs = sum(len(v) for v in history.values() if isinstance(v, list))
    # Статус воркера — чтобы юзер видел, крутится ли автоответчик на аккаунте
    if mode == ACCT_AR_MODE_AI:
        running = _is_acct_ar_worker_running(account_id, user_id)
        worker_status = (
            f"{emoji('CHECK')} <b>Воркер запущен</b>"
            if running
            else f"{emoji('CROSS')} <b>Воркер НЕ запущен</b> "
                 f"(перезапусти режим)"
        )
    else:
        worker_status = f"{emoji('CROSS')} Режим выключен"
    return (
        f"{emoji('AI')} <b>ИИ-автоответчик на аккаунте</b>\n\n"
        f"{emoji('PHONE')} Аккаунт: <code>{phone}</code>\n"
        f"{emoji('GEAR')} Режим: <b>{ACCT_AR_MODE_LABELS.get(mode, mode)}</b>\n"
        f"{emoji('EYE')} Статус: {worker_status}\n"
        f"{emoji('SPARK')} Модель: <b>{model}</b>\n"
        f"{emoji('CHAT')} Собеседников в истории: <b>{chats}</b> "
        f"(сообщений: {total_msgs})\n\n"
        f"{emoji('WRITE')} <b>Личность ИИ (system_prompt):</b>\n"
        f"<code>{escape(sysp[:500])}</code>"
        f"{'…' if len(sysp) > 500 else ''}"
    )


def _acct_ar_main_keyboard(
    account_id: int, mode: str, has_dialogs: bool = False
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    on_ai  = (mode == ACCT_AR_MODE_AI)
    on_off = (mode == ACCT_AR_MODE_OFF)
    builder.row(InlineKeyboardButton(
        text=("🤖 С ИИ ✅" if on_ai else "🤖 С ИИ"),
        callback_data=f"acct_ar:mode:{account_id}:ai",
        style='primary' if on_ai else 'default',
        icon_custom_emoji_id=get_icon("AI"),
    ))
    builder.row(InlineKeyboardButton(
        text=("🔕 Без ИИ (выключен) ✅" if on_off else "🔕 Без ИИ (выключен)"),
        callback_data=f"acct_ar:mode:{account_id}:off",
        style='primary' if on_off else 'default',
        icon_custom_emoji_id=get_icon("CROSS"),
    ))
    builder.row(InlineKeyboardButton(
        text=(
            f"💬 Диалоги ИИ ({'есть' if has_dialogs else 'пусто'})"
        ),
        callback_data=f"acct_ar:dialogs:{account_id}:0",
        style='primary' if has_dialogs else 'default',
        icon_custom_emoji_id=get_icon("CHAT"),
    ))
    builder.row(InlineKeyboardButton(
        text="Личность ИИ (system_prompt)",
        callback_data=f"acct_ar:system:{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("WRITE"),
    ))
    builder.row(InlineKeyboardButton(
        text="Сменить модель",
        callback_data=f"acct_ar:model:{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("AI"),
    ))
    builder.row(InlineKeyboardButton(
        text="Сбросить историю диалогов",
        callback_data=f"acct_ar:reset:{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("SWEEP"),
    ))
    builder.row(InlineKeyboardButton(
        text="Назад к аккаунту",
        callback_data=f"manage_account_{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("BACK"),
    ))
    return builder.as_markup()


def _acct_ar_model_keyboard(account_id: int, current: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for key, label in LLM_MODELS.items():
        mark = " ✅" if key == current else ""
        builder.row(InlineKeyboardButton(
            text=label + mark,
            callback_data=f"acct_ar:model_set:{account_id}:{key}",
            style='primary' if key == current else 'default',
            icon_custom_emoji_id=get_icon("CHECK") if key == current else get_icon("RIGHT"),
        ))
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data=f"acct_ar:home:{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("BACK"),
    ))
    return builder.as_markup()


# --- Запуск/остановка воркера на аккаунте ---
async def start_account_ai_responder(account_id: int, user_id: int):
    """Запустить воркер per-account AI-автоответчика, если режим = 'ai'."""
    s = await acct_ar_get(account_id)
    if s['mode'] != ACCT_AR_MODE_AI:
        # Режим не «ai» — воркер не нужен, на всякий случай остановим старый
        await stop_account_ai_responder(account_id, user_id)
        return

    # Если уже запущен — ничего
    if user_id in active_account_ai_responders \
            and account_id in active_account_ai_responders[user_id]:
        return

    task = asyncio.create_task(
        account_ai_responder_worker(account_id, user_id)
    )
    if user_id not in active_account_ai_responders:
        active_account_ai_responders[user_id] = {}
    active_account_ai_responders[user_id][account_id] = task


async def stop_account_ai_responder(account_id: int, user_id: int):
    if user_id in active_account_ai_responders \
            and account_id in active_account_ai_responders[user_id]:
        try:
            active_account_ai_responders[user_id][account_id].cancel()
        except Exception:
            pass
        try:
            del active_account_ai_responders[user_id][account_id]
        except KeyError:
            pass
        if not active_account_ai_responders[user_id]:
            del active_account_ai_responders[user_id]


# --- Сам воркер: Telethon listener на входящие ЛС ---
async def account_ai_responder_worker(account_id: int, user_id: int):
    """Слушает входящие ЛС на аккаунте и отвечает через LLM.

    • Только личные сообщения (event.is_private).
    • Только текст (стикеры/фото/голос пока игнорим).
    • Игнорим исходящие (event.outgoing).
    • Спим, пока режим != 'ai' (если кто-то выключил на ходу).

    Гарантии (чтобы юзер ВСЕГДА видел реакцию на входящее ЛС):
      1) Каждое входящее текстовое ЛС сначала пишется в account_logs
         (direction='received') — даже если LLM упадёт.
      2) Если LLM вернул пустой ответ или упал — юзер всё равно получит
         сообщение с объяснением (а не тишину).
      3) Любое исключение внутри handler'а ловится, логируется и
         НИКОГДА не роняет воркер.
      4) Если telethon-клиент отвалился — воркер переподключается
         (до 3 попыток) и снова навешивает handler.
    """
    client = None
    _handler = None
    # Сколько раз пытаемся переподключиться, прежде чем сдаться
    MAX_RECONNECT_TRIES = 3

    def _register_handler():
        """Навешивает обработчик на текущий client. Возвращает True если ок."""
        nonlocal _handler
        try:
            _handler = _make_handler()
            client.add_event_handler(_handler, events.NewMessage(incoming=True))
            logger.info(
                f"acct_ar: handler зарегистрирован для account_id={account_id}"
            )
            return True
        except Exception as ex:
            logger.exception(
                f"acct_ar: не удалось зарегистрировать handler: {ex}"
            )
            return False

    def _make_handler():
        """Создаёт замыкание handler'а с захватом client и account_id."""
        async def _handler(event):
            # Вся логика в одном try/except — чтобы handler никогда
            # не уронил воркер из-за неожиданного исключения.
            try:
                # Включён ли ещё режим?
                settings = await acct_ar_get(account_id)
                if settings.get('mode') != ACCT_AR_MODE_AI:
                    return

                # 1) Только входящие ЛС (текст).
                #    Игнорим группы/каналы/исходящие — что бы ни случилось.
                #    На некоторых версиях telethon у event нет .outgoing,
                #    поэтому используем getattr + пробуем .message.out.
                try:
                    _is_out = bool(
                        getattr(event, 'outgoing', False)
                        or getattr(getattr(event, 'message', None), 'out', False)
                    )
                except Exception:
                    _is_out = False
                if _is_out:
                    return
                # Жёсткая фильтрация: только личка (ЛС).
                # Сначала пробуем стандартный event.is_private,
                # если его нет — проверяем явно через chat_id.
                try:
                    _is_private = bool(
                        getattr(event, 'is_private', False)
                        or (
                            not getattr(event, 'is_group', False)
                            and not getattr(event, 'is_channel', False)
                        )
                    )
                except Exception:
                    _is_private = False
                if not _is_private:
                    return  # это группа/канал/что-то ещё — не наш клиент
                try:
                    _msg = getattr(event, 'message', None)
                    text = ((getattr(_msg, 'text', None) or '') if _msg else '').strip()
                except Exception:
                    text = ''
                if not text:
                    return  # стикеры/фото/голос пока не обрабатываем

                chat_id = event.chat_id
                try:
                    sender = await event.get_sender()
                    if (getattr(sender, 'username', None) or '').casefold() == 'spambot':
                        return
                    sender_name = (
                        getattr(sender, 'first_name', None)
                        or getattr(sender, 'username', None)
                        or str(getattr(sender, 'id', chat_id))
                    )
                except Exception:
                    sender_name = str(chat_id)

                # 2) СРАЗУ логируем входящее — чтобы оно появилось в
                #    «Логах аккаунта» даже если LLM упадёт. Это и есть
                #    та самая «история диалогов», которую ждёт юзер.
                try:
                    await add_account_log(
                        account_id, sender_name, chat_id,
                        'received', text[:100]
                    )
                except Exception as ex:
                    logger.warning(
                        f"acct_ar: не удалось залогировать входящее: {ex}"
                    )

                # 3) Зовём LLM. «Печатает…» пока думает.
                answer = ''
                llm_error_text = ''
                try:
                    system_prompt = (
                        (settings.get('system_prompt') or '').strip()
                        or ACCT_AR_DEFAULT_SYSTEM_PROMPT
                    )
                    model = (
                        (settings.get('model') or '').strip()
                        or LLM_DEFAULT_MODEL
                    )
                    history = list(
                        (settings.get('history') or {}).get(str(chat_id), [])
                    )
                    history.append({'role': 'user', 'content': text})
                    history = history[-(ACCT_AR_HISTORY_PAIRS * 2):]
                    try:
                        async with client.action(chat_id, 'typing'):
                            answer = await call_llm_api_with_history(
                                system_prompt=system_prompt,
                                messages=history,
                                user_id=user_id,
                                model=model,
                            )
                    except Exception:
                        # На некоторых версиях telethon .action() может
                        # не работать (например, peer ещё не зарезолвлен).
                        # В этом случае просто зовём LLM без typing-индикатора.
                        answer = await call_llm_api_with_history(
                            system_prompt=system_prompt,
                            messages=history,
                            user_id=user_id,
                            model=model,
                        )
                except Exception as ex:
                    logger.exception("acct_ar LLM call failed: %s", ex)
                    llm_error_text = str(ex)[:200]

                # 4) Гарантируем, что юзер получит хоть какой-то ответ.
                if not answer:
                    if llm_error_text:
                        answer = (
                            f"⚠️ ИИ-автоответчик: не удалось получить ответ "
                            f"от модели.\nПричина: {escape(llm_error_text)}"
                        )
                    else:
                        answer = (
                            "🤖 (ИИ вернул пустой ответ — попробуй "
                            "перефразировать сообщение)"
                        )

                # 5) Сохраняем в историю диалогов (до отправки, чтобы
                #    в случае флуд-вейта ответ не потерялся без следа).
                try:
                    await acct_ar_push_chat_history(
                        account_id, chat_id, 'user', text
                    )
                    await acct_ar_push_chat_history(
                        account_id, chat_id, 'assistant', answer
                    )
                except Exception:
                    logger.exception("acct_ar: не удалось сохранить историю")

                # 6) Шлём ответ (с разбивкой на куски > 4000 символов).
                sent_ok = False
                try:
                    if len(answer) <= 4000:
                        await client.send_message(chat_id, answer)
                    else:
                        for i in range(0, len(answer), 4000):
                            await client.send_message(
                                chat_id, answer[i:i + 4000]
                            )
                    sent_ok = True
                except FloodWaitError as fw:
                    # Флуд-вейт: подождём и попробуем ещё раз одним куском.
                    logger.warning(
                        f"acct_ar: FloodWait {fw.seconds}s на {chat_id}"
                    )
                    try:
                        await record_flood_wait(
                            account_id, int(chat_id), int(fw.seconds)
                        )
                    except Exception:
                        pass
                    try:
                        await asyncio.sleep(min(int(fw.seconds), 30))
                        await client.send_message(chat_id, answer[:4000])
                        sent_ok = True
                    except Exception as ex:
                        logger.exception(
                            f"acct_ar: повторная отправка тоже упала: {ex}"
                        )
                except Exception as ex:
                    logger.exception(
                        f"acct_ar: не удалось отправить ответ: {ex}"
                    )

                # 7) Лог аккаунта: что отправили
                if sent_ok:
                    try:
                        await add_account_log(
                            account_id, sender_name, chat_id,
                            'sent', answer[:100]
                        )
                    except Exception as ex:
                        logger.warning(
                            f"acct_ar: не удалось залогировать исходящее: {ex}"
                        )

            except Exception as ex:
                # Абсолютный catch-all: handler не должен ронять воркер.
                logger.exception(
                    f"acct_ar: необработанное исключение в handler: {ex}"
                )
                try:
                    await client.send_message(
                        event.chat_id,
                        f"⚠️ ИИ-автоответчик: внутренняя ошибка: "
                        f"{escape(str(ex))[:200]}",
                    )
                except Exception:
                    pass
        return _handler

    # --- основной цикл с авто-реконнектором ---
    try:
        account = await get_account(account_id)
        if not account:
            logger.warning(
                f"acct_ar: account {account_id} не найден, воркер выходит"
            )
            return

        proxy = None
        if account.get('proxy_id'):
            proxy = await get_proxy(account['proxy_id'])

        reconnect_tries = 0
        while reconnect_tries < MAX_RECONNECT_TRIES:
            try:
                client = await create_telethon_client(
                    account['session_string'], proxy=proxy
                )
                await client.connect()
                if not await client.is_user_authorized():
                    logger.warning(
                        f"acct_ar: account {account_id} не авторизован, "
                        f"воркер выходит"
                    )
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    return

                # Регистрируем handler
                if not _register_handler():
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    return

                logger.info(
                    f"acct_ar: воркер запущен для account_id={account_id}, "
                    f"phone={account.get('phone')}"
                )

                # Спим, пока не отменят или клиент не отвалится
                while True:
                    await asyncio.sleep(1)
                    if not client.is_connected():
                        logger.warning(
                            f"acct_ar: telethon-клиент отвалился "
                            f"(account_id={account_id}), пробуем "
                            f"переподключиться "
                            f"({reconnect_tries + 1}/{MAX_RECONNECT_TRIES})"
                        )
                        # Снимаем старый handler
                        try:
                            client.remove_event_handler(_handler)
                        except Exception:
                            pass
                        _handler = None
                        try:
                            await client.disconnect()
                        except Exception:
                            pass
                        client = None
                        break  # в outer while для реконнекта

                reconnect_tries += 1
                await asyncio.sleep(5)  # пауза перед реконнектом
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                logger.exception(
                    f"acct_ar: ошибка в outer-цикле воркера: {ex}"
                )
                reconnect_tries += 1
                try:
                    if client is not None:
                        try:
                            await client.disconnect()
                        except Exception:
                            pass
                        client = None
                except Exception:
                    pass
                await asyncio.sleep(5)

        if reconnect_tries >= MAX_RECONNECT_TRIES:
            logger.error(
                f"acct_ar: исчерпали {MAX_RECONNECT_TRIES} попыток "
                f"реконнекта для account_id={account_id}, воркер выходит"
            )
    except asyncio.CancelledError:
        logger.info(f"acct_ar: воркер остановлен для account_id={account_id}")
        raise
    except Exception as ex:
        logger.exception(
            f"acct_ar: воркер упал для account_id={account_id}: {ex}"
        )
    finally:
        if client is not None and _handler is not None:
            try:
                client.remove_event_handler(_handler)
            except Exception:
                pass
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass
        # Чистим реестр, если запись ещё наша
        if user_id in active_account_ai_responders:
            if account_id in active_account_ai_responders[user_id]:
                del active_account_ai_responders[user_id][account_id]
            if not active_account_ai_responders[user_id]:
                del active_account_ai_responders[user_id]


# --- Хендлеры UI per-account AI-автоответчика ---

@dp.callback_query(F.data.startswith("acct_ar:home:"))
async def cb_acct_ar_home(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    account_id = int(callback.data.split(":")[2])
    user_id = callback.from_user.id
    account = await get_account(account_id)
    if not account or account['user_id'] != user_id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    settings = await acct_ar_get(account_id)
    has_dialogs = bool(settings.get('history'))
    text = _acct_ar_status_text(
        settings, account['phone'], account_id=account_id, user_id=user_id
    )
    try:
        await callback.message.edit_text(
            text, reply_markup=_acct_ar_main_keyboard(
                account_id, settings['mode'], has_dialogs=has_dialogs
            )
        )
    except Exception:
        await callback.message.answer(
            text, reply_markup=_acct_ar_main_keyboard(
                account_id, settings['mode'], has_dialogs=has_dialogs
            )
        )
    await callback.answer()


@dp.callback_query(F.data.startswith("acct_ar:mode:"))
async def cb_acct_ar_mode(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    # acct_ar:mode:<account_id>:<mode>
    parts = callback.data.split(":")
    account_id = int(parts[2])
    new_mode = parts[3]
    if new_mode not in ACCT_AR_MODE_LABELS:
        await callback.answer("Неизвестный режим")
        return
    account = await get_account(account_id)
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return

    if new_mode == ACCT_AR_MODE_AI and not await is_pro(callback.from_user.id):
        await callback.answer(
            "AI-автоответчик доступен только в Pro.",
            show_alert=True
        )
        return

    await acct_ar_set_mode(account_id, new_mode)
    user_id = callback.from_user.id
    if new_mode == ACCT_AR_MODE_AI:
        await start_account_ai_responder(account_id, user_id)
    else:
        await stop_account_ai_responder(account_id, user_id)

    settings = await acct_ar_get(account_id)
    has_dialogs = bool(settings.get('history'))
    text = _acct_ar_status_text(
        settings, account['phone'], account_id=account_id, user_id=user_id
    )
    try:
        await callback.message.edit_text(
            text, reply_markup=_acct_ar_main_keyboard(
                account_id, settings['mode'], has_dialogs=has_dialogs
            )
        )
    except Exception:
        pass
    await callback.answer(f"Режим: {ACCT_AR_MODE_LABELS[new_mode]}")


@dp.callback_query(F.data.startswith("acct_ar:system:"))
async def cb_acct_ar_system_prompt(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split(":")[2])
    account = await get_account(account_id)
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    settings = await acct_ar_get(account_id)
    cur = (settings.get('system_prompt') or '').strip() \
        or ACCT_AR_DEFAULT_SYSTEM_PROMPT
    await state.set_state(AccountAIResponderStates.setting_system)
    await state.update_data(acct_ar_account_id=account_id)
    await callback.message.answer(
        f"🧠 <b>Личность ИИ для аккаунта "
        f"<code>{account['phone']}</code></b>\n\n"
        f"<b>Текущая:</b>\n<code>{escape(cur)}</code>\n\n"
        f"Отправь новый system_prompt одним сообщением.\n"
        f"Чтобы вернуть стандартную личность — пришли <code>default</code>.\n"
        f"Чтобы отменить — /cancel."
    )
    await callback.answer()


@dp.message(AccountAIResponderStates.setting_system)
async def process_acct_ar_system(message: Message, state: FSMContext):
    if message.text is None:
        await message.answer("Пришли промт текстом. /cancel — отмена.")
        return
    text = message.text.strip()
    data = await state.get_data()
    account_id = int(data.get('acct_ar_account_id') or 0)
    if not account_id:
        await state.clear()
        await message.answer("Не нашёл аккаунт. Открой меню заново.")
        return
    if not text:
        await message.answer("Промт не должен быть пустым. Или /cancel.")
        return
    if text.lower() == "default":
        await acct_ar_reset_system_prompt(account_id)
    else:
        await acct_ar_set_system_prompt(account_id, text)
    await state.clear()
    account = await get_account(account_id)
    user_id = message.from_user.id
    settings = await acct_ar_get(account_id)
    has_dialogs = bool(settings.get('history'))
    text_out = _acct_ar_status_text(
        settings, account['phone'],
        account_id=account_id, user_id=user_id,
    )
    await message.answer(
        "✅ Личность ИИ обновлена.\n\n" + text_out,
        reply_markup=_acct_ar_main_keyboard(
            account_id, settings['mode'], has_dialogs=has_dialogs
        ),
    )


@dp.callback_query(F.data.startswith("acct_ar:model:"))
async def cb_acct_ar_model(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    account_id = int(callback.data.split(":")[2])
    account = await get_account(account_id)
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    settings = await acct_ar_get(account_id)
    current = (settings.get('model') or '').strip() or LLM_DEFAULT_MODEL
    await callback.message.edit_text(
        f"{emoji('AI')} <b>Модель ИИ для аккаунта "
        f"<code>{account['phone']}</code></b>\n\n"
        f"Текущая: <b>{current}</b>\n\n"
        f"Выбери модель:",
        reply_markup=_acct_ar_model_keyboard(account_id, current),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("acct_ar:model_set:"))
async def cb_acct_ar_model_set(callback: CallbackQuery):
    # acct_ar:model_set:<account_id>:<model_key>
    parts = callback.data.split(":")
    account_id = int(parts[2])
    model_key = parts[3]
    if model_key not in LLM_MODELS:
        await callback.answer("Неизвестная модель")
        return
    account = await get_account(account_id)
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    await acct_ar_set_model(account_id, model_key)
    # Если воркер уже крутится — перезапустим, чтобы он подхватил модель
    user_id = callback.from_user.id
    await stop_account_ai_responder(account_id, user_id)
    settings = await acct_ar_get(account_id)
    if settings['mode'] == ACCT_AR_MODE_AI:
        await start_account_ai_responder(account_id, user_id)
    has_dialogs = bool(settings.get('history'))
    text = _acct_ar_status_text(
        settings, account['phone'],
        account_id=account_id, user_id=user_id,
    )
    try:
        await callback.message.edit_text(
            text, reply_markup=_acct_ar_main_keyboard(
                account_id, settings['mode'], has_dialogs=has_dialogs
            )
        )
    except Exception:
        pass
    await callback.answer(f"Модель: {LLM_MODELS[model_key]}")


@dp.callback_query(F.data.startswith("acct_ar:reset:"))
async def cb_acct_ar_reset(callback: CallbackQuery):
    account_id = int(callback.data.split(":")[2])
    account = await get_account(account_id)
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    await acct_ar_reset_history(account_id)
    await callback.answer("🧹 История диалогов очищена")
    settings = await acct_ar_get(account_id)
    user_id = callback.from_user.id
    has_dialogs = bool(settings.get('history'))
    text = _acct_ar_status_text(
        settings, account['phone'],
        account_id=account_id, user_id=user_id,
    )
    try:
        await callback.message.edit_text(
            text, reply_markup=_acct_ar_main_keyboard(
                account_id, settings['mode'], has_dialogs=has_dialogs
            )
        )
    except Exception:
        pass


# ============================================================
#  Диалоги ИИ-автоответчика: список собеседников + просмотр переписки
# ============================================================
# callback_data формат:
#   acct_ar:dialogs:<account_id>:<page>          — список собеседников
#   acct_ar:dialog_view:<account_id>:<chat_id>   — просмотр переписки
#   acct_ar:dialog_clear:<account_id>:<chat_id>  — очистить один диалог
#   acct_ar:dialog_back:<account_id>             — назад в меню ИИ-автоответчика
DIALOGS_PAGE_SIZE = 8
DIALOG_VIEW_TAIL = 20  # сколько последних сообщений показывать


async def _resolve_dialog_partner_name(
    account: Dict, chat_id: int
) -> str:
    """Пробуем достать имя собеседника через telethon.
    Если не вышло (клиент не подключен / peer не зарезолвлен) —
    возвращаем красиво отформатированный chat_id.
    """
    phone = account.get('phone')
    proxy_id = account.get('proxy_id')
    proxy = None
    if proxy_id:
        try:
            proxy = await get_proxy(proxy_id)
        except Exception:
            proxy = None
    client = None
    try:
        client = await create_telethon_client(
            account['session_string'], proxy=proxy
        )
        await client.connect()
        if not await client.is_user_authorized():
            return f"id{chat_id}"
        try:
            entity = await client.get_entity(int(chat_id))
        except Exception:
            return f"id{chat_id}"
        # Собираем красивое имя
        if getattr(entity, 'first_name', None):
            last = getattr(entity, 'last_name', None) or ''
            full = (entity.first_name + (' ' + last if last else '')).strip()
            return full or f"id{chat_id}"
        if getattr(entity, 'title', None):
            return entity.title
        if getattr(entity, 'username', None):
            return f"@{entity.username}"
        if getattr(entity, 'phone', None):
            return f"+{entity.phone}"
        return f"id{entity.id}"
    except Exception:
        return f"id{chat_id}"
    finally:
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass


def _dialogs_list_keyboard(
    account_id: int, page: int, dialogs: List[tuple]
) -> InlineKeyboardMarkup:
    """Клавиатура списка диалогов.
    dialogs — список кортежей (chat_id_str, last_user_msg, last_assistant_msg).
    """
    builder = InlineKeyboardBuilder()
    total = len(dialogs)
    pages = max(1, (total + DIALOGS_PAGE_SIZE - 1) // DIALOGS_PAGE_SIZE)
    page = max(0, min(page, pages - 1))
    start = page * DIALOGS_PAGE_SIZE
    end = start + DIALOGS_PAGE_SIZE
    for chat_id_str, preview in dialogs[start:end]:
        # Сокращаем превью до 32 символов для кнопки
        btn_text = preview[:32] + ('…' if len(preview) > 32 else '')
        builder.row(InlineKeyboardButton(
            text=btn_text or f"id{chat_id_str}",
            callback_data=(
                f"acct_ar:dialog_view:{account_id}:{chat_id_str}"
            ),
            style='default',
            icon_custom_emoji_id=get_icon("CHAT"),
        ))
    # Пагинация
    if pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton(
                text=" ",
                callback_data=(
                    f"acct_ar:dialogs:{account_id}:{page - 1}"
                ),
                style='default',
                icon_custom_emoji_id=get_icon("BACK"),
            ))
        nav_row.append(InlineKeyboardButton(
            text=f"· {page + 1}/{pages} ·",
            callback_data=f"acct_ar:dialogs:{account_id}:{page}",
            style='default',
            icon_custom_emoji_id=get_icon("INFO"),
        ))
        if page < pages - 1:
            nav_row.append(InlineKeyboardButton(
                text=" ",
                callback_data=(
                    f"acct_ar:dialogs:{account_id}:{page + 1}"
                ),
                style='default',
                icon_custom_emoji_id=get_icon("RIGHT"),
            ))
        builder.row(*nav_row)
    builder.row(InlineKeyboardButton(
        text="Назад",
        callback_data=f"acct_ar:dialog_back:{account_id}",
        style='default',
        icon_custom_emoji_id=get_icon("BACK"),
    ))
    return builder.as_markup()


@dp.callback_query(F.data.startswith("acct_ar:dialogs:"))
async def cb_acct_ar_dialogs(callback: CallbackQuery):
    # acct_ar:dialogs:<account_id>:<page>
    parts = callback.data.split(":")
    account_id = int(parts[2])
    try:
        page = int(parts[3]) if len(parts) > 3 else 0
    except ValueError:
        page = 0
    account = await get_account(account_id)
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    settings = await acct_ar_get(account_id)
    history: Dict[str, List[Dict[str, str]]] = settings.get('history') or {}
    if not history:
        await callback.answer(
            "Диалогов пока нет — придёт первое ЛС и появится",
            show_alert=True
        )
        return

    # Сортируем диалоги по последнему сообщению (свежие сверху)
    dialogs_meta: List[tuple] = []
    for chat_id_str, msgs in history.items():
        if not isinstance(msgs, list) or not msgs:
            continue
        last = msgs[-1]
        preview = (last.get('content') or '').strip().replace('\n', ' ')
        if not preview:
            preview = f"id{chat_id_str}"
        dialogs_meta.append((chat_id_str, preview))
    dialogs_meta.sort(
        key=lambda x: x[1],  # стабильная сортировка по превью
    )

    text = (
        f"{emoji('CHAT')} <b>Диалоги ИИ-автоответчика</b>\n"
        f"{emoji('PHONE')} Аккаунт: <code>{account['phone']}</code>\n\n"
        f"Всего собеседников: <b>{len(dialogs_meta)}</b>\n"
        f"Нажми на собеседника, чтобы посмотреть переписку."
    )
    try:
        await callback.message.edit_text(
            text,
            reply_markup=_dialogs_list_keyboard(
                account_id, page, dialogs_meta
            ),
        )
    except Exception:
        await callback.message.answer(
            text,
            reply_markup=_dialogs_list_keyboard(
                account_id, page, dialogs_meta
            ),
        )
    await callback.answer()


@dp.callback_query(F.data.startswith("acct_ar:dialog_view:"))
async def cb_acct_ar_dialog_view(callback: CallbackQuery):
    # acct_ar:dialog_view:<account_id>:<chat_id>
    parts = callback.data.split(":")
    account_id = int(parts[2])
    # chat_id может содержать ':' если у юзера был такой id, но в
    # Telegram chat_id — это число, так что безопасно.
    chat_id_str = parts[3]
    account = await get_account(account_id)
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return

    settings = await acct_ar_get(account_id)
    history_map: Dict[str, List[Dict[str, str]]] = settings.get('history') or {}
    msgs: List[Dict[str, str]] = list(
        history_map.get(chat_id_str, [])
    )
    if not msgs:
        await callback.answer("Диалог пуст", show_alert=True)
        return

    # Имя собеседника — пробуем через telethon
    try:
        chat_id_int = int(chat_id_str)
    except ValueError:
        chat_id_int = chat_id_str
    partner_name = await _resolve_dialog_partner_name(
        account, chat_id_int
    )

    # Берём хвост переписки
    tail = msgs[-DIALOG_VIEW_TAIL:]
    lines = []
    for m in tail:
        role = m.get('role') or '?'
        content = (m.get('content') or '').strip()
        if not content:
            continue
        if role == 'user':
            who = f"{emoji('PROFILE')} {escape(partner_name)}"
        elif role == 'assistant':
            who = f"{emoji('AI')} ИИ"
        else:
            who = f"❓ {escape(role)}"
        # Подрезаем длинные сообщения, чтобы влезли в лимит Telegram (~4096)
        if len(content) > 800:
            content = content[:800] + '…'
        lines.append(f"<b>{who}:</b>\n{escape(content)}")
    body = "\n\n".join(lines) if lines else "(пусто)"
    text = (
        f"{emoji('CHAT')} <b>Диалог с "
        f"{escape(partner_name)}</b>\n"
        f"{emoji('PHONE')} Аккаунт: <code>{account['phone']}</code>\n"
        f"{emoji('ID')} chat_id: <code>{escape(chat_id_str)}</code>\n"
        f"{emoji('CHART')} Сообщений: <b>{len(msgs)}</b>"
        f" (показано последних: {len(tail)})\n\n"
        f"{body}"
    )
    # Telegram limit 4096 на сообщение
    if len(text) > 3800:
        text = text[:3800] + "\n\n…(сообщение обрезано)"

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Очистить этот диалог",
        callback_data=(
            f"acct_ar:dialog_clear:{account_id}:{chat_id_str}"
        ),
        style='default',
        icon_custom_emoji_id=get_icon("SWEEP"),
    ))
    builder.row(InlineKeyboardButton(
        text="К списку диалогов",
        callback_data=f"acct_ar:dialogs:{account_id}:0",
        style='default',
        icon_custom_emoji_id=get_icon("BACK"),
    ))
    try:
        await callback.message.edit_text(
            text, reply_markup=builder.as_markup()
        )
    except Exception:
        await callback.message.answer(
            text, reply_markup=builder.as_markup()
        )
    await callback.answer()


@dp.callback_query(F.data.startswith("acct_ar:dialog_clear:"))
async def cb_acct_ar_dialog_clear(callback: CallbackQuery):
    # acct_ar:dialog_clear:<account_id>:<chat_id>
    parts = callback.data.split(":")
    account_id = int(parts[2])
    chat_id_str = parts[3]
    account = await get_account(account_id)
    if not account or account['user_id'] != callback.from_user.id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT history FROM account_ai_responder '
            'WHERE account_id = $1',
            account_id,
        )
        if not row:
            await callback.answer("Нет истории", show_alert=True)
            return
        history = row['history'] or {}
        if isinstance(history, str):
            try:
                history = json.loads(history)
            except Exception:
                history = {}
        if not isinstance(history, dict):
            history = {}
        if chat_id_str in history:
            del history[chat_id_str]
        await conn.execute(
            'UPDATE account_ai_responder '
            'SET history = $1::jsonb, updated_at = NOW() '
            'WHERE account_id = $2',
            json.dumps(history, ensure_ascii=False), account_id,
        )
    await callback.answer("🧹 Диалог очищен")
    # Возвращаемся к списку диалогов
    settings = await acct_ar_get(account_id)
    history_map: Dict[str, List[Dict[str, str]]] = settings.get('history') or {}
    if not history_map:
        # Историй больше нет — сразу в главное меню ИИ
        user_id = callback.from_user.id
        has_dialogs = False
        text = _acct_ar_status_text(
            settings, account['phone'],
            account_id=account_id, user_id=user_id,
        )
        try:
            await callback.message.edit_text(
                text, reply_markup=_acct_ar_main_keyboard(
                    account_id, settings['mode'], has_dialogs=has_dialogs
                )
            )
        except Exception:
            pass
        return
    # Иначе — список диалогов
    dialogs_meta: List[tuple] = []
    for cid, msgs in history_map.items():
        if not isinstance(msgs, list) or not msgs:
            continue
        last = msgs[-1]
        preview = (last.get('content') or '').strip().replace('\n', ' ')
        if not preview:
            preview = f"id{cid}"
        dialogs_meta.append((cid, preview))
    text = (
        f"{emoji('CHAT')} <b>Диалоги ИИ-автоответчика</b>\n"
        f"{emoji('PHONE')} Аккаунт: <code>{account['phone']}</code>\n\n"
        f"Всего собеседников: <b>{len(dialogs_meta)}</b>\n"
        f"Нажми на собеседника, чтобы посмотреть переписку."
    )
    try:
        await callback.message.edit_text(
            text,
            reply_markup=_dialogs_list_keyboard(
                account_id, 0, dialogs_meta
            ),
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("acct_ar:dialog_back:"))
async def cb_acct_ar_dialog_back(callback: CallbackQuery):
    # acct_ar:dialog_back:<account_id>
    account_id = int(callback.data.split(":")[2])
    user_id = callback.from_user.id
    account = await get_account(account_id)
    if not account or account['user_id'] != user_id:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    settings = await acct_ar_get(account_id)
    has_dialogs = bool(settings.get('history'))
    text = _acct_ar_status_text(
        settings, account['phone'],
        account_id=account_id, user_id=user_id,
    )
    try:
        await callback.message.edit_text(
            text, reply_markup=_acct_ar_main_keyboard(
                account_id, settings['mode'], has_dialogs=has_dialogs
            )
        )
    except Exception:
        pass
    await callback.answer()


@dp.message(Command("cancel"), StateFilter("*"))
async def cmd_cancel_acct_ar(message: Message, state: FSMContext):
    """Отмена настройки ИИ-автоответчика или создания скрипта."""
    current = await state.get_state()
    if current in {
        AccountAIResponderStates.setting_system.state,
        AccountAIResponderStates.setting_model.state,
        ScriptStates.waiting_for_name.state,
        ScriptStates.choosing_account.state,
        ScriptStates.waiting_for_bot_url.state,
        ScriptStates.choosing_captcha.state,
        ScriptStates.choosing_button.state,
        ScriptStates.confirming_step.state,
        AdminLLMConfigStates.waiting_for_name.state,
        AdminLLMConfigStates.waiting_for_base_url.state,
        AdminLLMConfigStates.waiting_for_api_key.state,
        AdminLLMConfigStates.waiting_for_model_api_name.state,
        AdminLLMConfigStates.waiting_for_model_display_name.state,
        AIChatStates.waiting_for_message.state,
        NeuroCommentStates.waiting_for_account.state,
        NeuroCommentStates.selecting_channels.state,
        NeuroCommentStates.choosing_mode.state,
        NeuroCommentStates.choosing_model.state,
        NeuroCommentStates.collecting_templates.state,
        NeuroCommentStates.waiting_for_delay.state,
        NeuroCommentStates.preview.state,
    }:
        await state.clear()
        await message.answer("Ок, отменил.")




# --- Запуск бота ---
async def on_startup():
    os.makedirs("media", exist_ok=True)
    os.makedirs("media/ai", exist_ok=True)
    await init_db()
    # Подхватываем базовый API/модели администратора до запуска воркеров.
    await refresh_global_llm_runtime()

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

        # Per-account AI-автоответчик: запускаем воркеры на аккаунтах,
        # у которых mode = 'ai'
        ai_accounts = await conn.fetch(
            "SELECT account_id FROM account_ai_responder "
            "WHERE mode = 'ai'"
        )
        for row in ai_accounts:
            acc = await get_account(row['account_id'])
            if not acc or not acc.get('is_active'):
                continue
            try:
                await start_account_ai_responder(
                    row['account_id'], acc['user_id']
                )
                logger.info(
                    f"on_startup: AI-автоответчик восстановлен для "
                    f"account_id={row['account_id']}"
                )
            except Exception as ex:
                logger.warning(
                    f"on_startup: не удалось запустить AI-автоответчик "
                    f"для account_id={row['account_id']}: {ex}"
                )

    # Нейрокомментинг сохраняется в БД, поэтому после перезапуска
    # восстанавливаем только конфигурации, которые пользователь не остановил.
    async with db_pool.acquire() as conn:
        neuro_configs = await conn.fetch(
            'SELECT id FROM neurocomment_configs WHERE is_active = TRUE'
        )
    for row in neuro_configs:
        try:
            await start_neurocomment_worker(int(row['id']))
        except Exception as ex:
            logger.warning(
                'on_startup: не удалось восстановить нейрокомментинг %s: %s',
                row['id'], ex,
            )

    # Бесконечные маршруты скриптов продолжаются после перезапуска, пока
    # пользователь явно не нажмёт «Остановить скрипт».
    async with db_pool.acquire() as conn:
        running_scripts = await conn.fetch(
            "SELECT id, user_id FROM user_scripts WHERE last_status = 'running'"
        )
    for row in running_scripts:
        try:
            await start_script_runner(int(row['id']), int(row['user_id']))
        except Exception as ex:
            logger.warning(
                'on_startup: не удалось восстановить скрипт %s: %s', row['id'], ex,
            )

    asyncio.create_task(check_scheduled_broadcasts())
    asyncio.create_task(task_queue_worker())
    asyncio.create_task(spam_block_check_worker())
    asyncio.create_task(account_monitoring_worker())

async def main():
    # --- Защита от запуска нескольких экземпляров ---
    import fcntl
    token_hash = hashlib.md5(BOT_TOKEN.encode()).hexdigest()[:12]
    lock_path = f"/tmp/bot_{token_hash}.lock"
    lock_file = open(lock_path, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logger.critical(
            "Другой экземпляр бота уже запущен (lock: %s). Завершение.", lock_path
        )
        return

    # Сбрасываем вебхук и накопившиеся апдейты
    await bot(DeleteWebhook(drop_pending_updates=True))
    await on_startup()

    # Полинг с автоматическим backoff при TelegramRetryAfter
    from aiogram.exceptions import TelegramRetryAfter as _TelegramRetryAfter
    backoff = 0
    while True:
        try:
            await dp.start_polling(
                bot,
                allowed_updates=dp.resolve_used_update_types(),
                handle_signals=True,
            )
            break  # нормальное завершение
        except _TelegramRetryAfter as e:
            backoff = max(e.retry_after, backoff) + 1
            logger.warning(
                "GetUpdates flood: ждём %d сек перед повторным запуском поллинга.",
                backoff,
            )
            await asyncio.sleep(backoff)
        except Exception as e:
            logger.exception("Критическая ошибка поллинга: %s", e)
            await asyncio.sleep(5)
            break

if __name__ == "__main__":
    asyncio.run(main())
