import os
import sys
import asyncio
import threading
import logging
import subprocess
import json
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
import signal
from functools import wraps, partial
import re
import time
# Load environment variables
from dotenv import load_dotenv

load_dotenv()
# MongoDB
from pymongo import MongoClient
from pymongo.errors import OperationFailure
# Pyrogram (Telegram Bot)
from pyrogram import Client, filters, enums, idle
from pyrogram.errors import UserNotParticipant, FloodWait
from pyrogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardRemove,
    InputMediaVideo,  # <-- Added
    InputMediaPhoto   # <-- Added
)
# Instagram Client
from instagrapi import Client as InstaClient
from instagrapi.exceptions import (
    LoginRequired,
    ChallengeRequired,
    BadPassword,
    PleaseWaitFewMinutes,
    ClientError,
    UserNotFound  # <-- Added
)
from instagrapi.types import Usertag, Location, StoryMention, StoryLocation, StoryHashtag, StoryLink
# System Utilities
import psutil
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log")
    ]
)
logger = logging.getLogger("BotUser")

# === Load and Validate Environment Variables ===
API_ID_STR = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
LOG_CHANNEL_STR = os.getenv("LOG_CHANNEL_ID")
MONGO_URI = os.getenv("MONGO_DB")
ADMIN_ID_STR = os.getenv("ADMIN_ID")
IS_WORKER_BOOL = os.getenv("IS_WORKER", "False").lower() == "true"
WORKER_CHANNEL_ID_STR = os.getenv("WORKER_CHANNEL_ID")

# --- Validate required variables ---
# Worker bot needs fewer variables
if IS_WORKER_BOOL:
    if not all([API_ID_STR, API_HASH, BOT_TOKEN, MONGO_URI, WORKER_CHANNEL_ID_STR]):
        logger.critical("FATAL ERROR (WORKER): Missing essential worker variables. Check API_ID, API_HASH, BOT_TOKEN, MONGO_URI, WORKER_CHANNEL_ID.")
        sys.exit(1)
    logger.info("Running in WORKER mode.")
# Main bot needs more variables
else:
    if not all([API_ID_STR, API_HASH, BOT_TOKEN, ADMIN_ID_STR, MONGO_URI, WORKER_CHANNEL_ID_STR]):
        logger.critical("FATAL ERROR (MAIN): One or more required environment variables are missing. Check API_ID, API_HASH, BOT_TOKEN, ADMIN_ID, MONGO_URI, WORKER_CHANNEL_ID.")
        sys.exit(1)
    logger.info("Running in MAIN mode.")


# Convert to correct types after validation
API_ID = int(API_ID_STR)
ADMIN_ID = int(ADMIN_ID_STR) if ADMIN_ID_STR else None # Admin ID is not vital for worker
LOG_CHANNEL = int(LOG_CHANNEL_STR) if LOG_CHANNEL_STR else None
WORKER_CHANNEL_ID = int(WORKER_CHANNEL_ID_STR)

# Instagram Client Credentials (for the bot's own primary account, if any)
INSTAGRAM_USERNAME = os.getenv("INSTAGRAM_USERNAME", "")
INSTAGRAM_PASSWORD = os.getenv("INSTAGRAM_PASSWORD", "")
INSTAGRAM_PROXY = os.getenv("INSTAGRAM_PROXY", "")
PROXY_SETTINGS = os.getenv("PROXY_SETTINGS", "")

# === Video Conversion Helpers ===

def needs_conversion(input_file: str) -> bool:
    """
    Checks if a video file needs conversion to be Instagram-compatible (MP4/AAC).
    Uses ffprobe to inspect the file's container and audio codec.
    Returns True if conversion is needed, False otherwise.
    """
    try:
        # Command to get stream info as JSON from ffprobe
        command = [
            'ffprobe',
            '-v', 'quiet',
            '-print_format', 'json',
            '-show_format',
            '-show_streams',
            input_file
        ]
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding='utf-8')
        data = json.loads(result.stdout)
        
        # Check container format
        format_name = data.get('format', {}).get('format_name', '')
        is_compatible_container = any(x in format_name for x in ['mp4', 'mov', '3gp'])

        # Check audio stream codec
        audio_codec = 'none' # Default for videos with no audio
        for stream in data.get('streams', []):
            if stream.get('codec_type') == 'audio':
                audio_codec = stream.get('codec_name')
                break # Found the first audio stream
        
        is_compatible_audio = (audio_codec == 'aac' or audio_codec == 'none')

        if is_compatible_container and is_compatible_audio:
            logger.info(f"'{input_file}' is already compatible (Container: {format_name}, Audio: {audio_codec}). No conversion needed.")
            return False
        else:
            logger.warning(f"'{input_file}' needs conversion (Container: {format_name}, Audio: {audio_codec}).")
            return True

    except FileNotFoundError:
        logger.error("ffprobe/ffmpeg is not installed. Cannot check video format. Assuming conversion is needed as a fallback.")
        return True # Failsafe: if we can't check, we should try to convert.
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        logger.error(f"Could not probe file '{input_file}'. It might be corrupted or not a valid video. Assuming conversion is needed.")
        return True # Failsafe for corrupted or non-media files

def fix_for_instagram(input_file: str, output_file: str) -> str:
    """
    Converts a video file to an Instagram-compatible format (MP4 container, AAC audio)
    by copying the video stream and re-encoding only the audio.
    """
    try:
        logger.info(f"Converting '{input_file}' to Instagram-compatible format...")
        command = [
            'ffmpeg',
            '-y',
            '-i', input_file,
            '-c:v', 'copy',
            '-c:a', 'aac',
            '-b:a', '192k',
            '-ar', '48000',
            '-movflags', '+faststart',
            output_file
        ]
        
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info(f"Successfully converted video to '{output_file}'.")
        return output_file
        
    except FileNotFoundError:
        logger.critical("ffmpeg is not installed or not found. Video conversion is not possible.")
        raise FileNotFoundError("ffmpeg is not installed. Cannot process video files.")
    except subprocess.CalledProcessError as e:
        logger.error(f"ffmpeg conversion failed for {input_file}. Error: {e.stderr}")
        raise ValueError(f"Video format is incompatible and conversion failed. Error: {e.stderr}")


# === Global Bot Settings ===
DEFAULT_GLOBAL_SETTINGS = {
    "special_event_toggle": False,
    "special_event_title": "🎉 Special Event!",
    "special_event_message": "Enjoy our special event features!",
    "max_concurrent_uploads": 15,
    "max_file_size_mb": 250,
    "payment_settings": {
        "google_play_qr_file_id": "",
        "upi": "",
        "usdt": "",
        "btc": "",
        "others": "",
        "custom_buttons": {}
    },
    "no_compression_admin": True
}

# --- Global State & DB Management ---
mongo = None
db = None
global_settings = {}
upload_semaphore = None
user_upload_locks = {}
MAX_FILE_SIZE_BYTES = 0
MAX_CONCURRENT_UPLOADS = 0
shutdown_event = asyncio.Event()
valid_log_channel = False

# Pyrogram Client
app = Client("upload_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
# Instagram Client
insta_client = InstaClient()
insta_client.delay_range = [1, 3]

# === Custom Filter for Bot Modes ===
def main_bot_only(_, __, ___):
    """Filter to ensure handler only runs on the Main Bot"""
    return not IS_WORKER_BOOL

main_bot_filter = filters.create(main_bot_only)

# --- Task Management ---
class TaskTracker:
    def __init__(self):
        self._tasks = set()
        self._user_specific_tasks = {}
        self.loop = None

    def create_task(self, coro, user_id=None, task_name=None):
        if self.loop is None:
            try:
                self.loop = asyncio.get_running_loop()
            except RuntimeError:
                logger.error("Could not create task: No running event loop.")
                return
        if user_id and task_name:
            self.cancel_user_task(user_id, task_name)
        task = self.loop.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        if user_id and task_name:
            if user_id not in self._user_specific_tasks:
                self._user_specific_tasks[user_id] = {}
            self._user_specific_tasks[user_id][task_name] = task
            logger.info(f"User-specific task '{task_name}' for user {user_id} created.")
        logger.info(f"Task {task.get_name()} created. Total tracked tasks: {len(self._tasks)}")
        return task

    def cancel_user_task(self, user_id, task_name):
        if user_id in self._user_specific_tasks and task_name in self._user_specific_tasks[user_id]:
            task_to_cancel = self._user_specific_tasks[user_id].pop(task_name)
            if not task_to_cancel.done():
                task_to_cancel.cancel()
                logger.info(f"Cancelled previous task '{task_name}' for user {user_id}.")
            if not self._user_specific_tasks[user_id]:
                del self._user_specific_tasks[user_id]

    async def cancel_all_user_tasks(self, user_id):
        if user_id in self._user_specific_tasks:
            user_tasks = self._user_specific_tasks.pop(user_id)
            for task_name, task in user_tasks.items():
                if not task.done():
                    task.cancel()
                    logger.info(f"Cancelled task '{task_name}' for user {user_id} during cleanup.")
            await asyncio.gather(*[t for t in user_tasks.values() if not t.done()], return_exceptions=True)

    async def cancel_and_wait_all(self):
        tasks_to_cancel = [t for t in self._tasks if not t.done()]
        if not tasks_to_cancel:
            return
        
        logger.info(f"Cancelling {len(tasks_to_cancel)} outstanding background tasks...")
        for t in tasks_to_cancel:
            t.cancel()
        
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        logger.info("All background tasks have been awaited.")

task_tracker = None

async def safe_task_wrapper(coro):
    """Wraps a coroutine to catch and log any exceptions, preventing crashes."""
    try:
        await coro
    except asyncio.CancelledError:
        logger.warning(f"Task {asyncio.current_task().get_name()} was cancelled.")
    except Exception:
        logger.exception(f"Unhandled exception in background task: {asyncio.current_task().get_name()}")


# ===================================================================
# ==================== FONT & TEXT HELPERS ==========================
# ===================================================================

def to_bold_sans(text: str) -> str:
    """Converts a string to bold sans-serif font, capitalizing the first letter of each word."""
    bold_sans_map = {
        'A': '𝗔', 'B': '𝗕', 'C': '𝗖', 'D': '𝗗', 'E': '𝗘', 'F': '𝗙', 'G': '𝗚', 'H': '𝗛', 'I': '𝗜',
        'J': '𝗝', 'K': '𝗞', 'L': '𝗟', 'M': '𝗠', 'N': '𝗡', 'O': '𝗢', 'P': '𝗣', 'Q': '𝗤', 'R': '𝗥',
        'S': '𝗦', 'T': '𝗧', 'U': '𝗨', 'V': '𝗩', 'W': '𝗪', 'X': '𝗫', 'Y': '𝗬', 'Z': '𝗭',
        'a': '𝗮', 'b': '𝗯', 'c': '𝗰', 'd': '𝗱', 'e': '𝗲', 'f': '𝗳', 'g': '𝗴', 'h': '𝗵', 'i': '𝗶',
        'j': '𝗷', 'k': '𝗸', 'l': '𝗹', 'm': '𝗺', 'n': '𝗻', 'o': '𝗼', 'p': '𝗽', 'q': '𝗾', 'r': '𝗿',
        's': '𝘀', 't': '𝘁', 'u': '𝘂', 'v': '𝘃', 'w': '𝘄', 'x': '𝘅', 'y': '𝘆', 'z': '𝘇',
        '0': '𝟬', '1': '𝟭', '2': '𝟮', '3': '𝟯', '4': '𝟰', '5': '𝟱', '6': '𝟲', '7': '𝟳', '8': '𝟴', '9': '𝟵'
    }
    sanitized_text = text.encode('utf-8', 'surrogatepass').decode('utf-8')
    capitalized_text = ' '.join(word.capitalize() for word in sanitized_text.split())
    return ''.join(bold_sans_map.get(char, char) for char in capitalized_text)

# State dictionary to hold user states
user_states = {}

PREMIUM_PLANS = {
    "6_hour_trial": {"duration": timedelta(hours=6), "price": "Free / Free"},
    "3_days": {"duration": timedelta(days=3), "price": "₹10 / $0.40"},
    "7_days": {"duration": timedelta(days=7), "price": "₹25 / $0.70"},
    "15_days": {"duration": timedelta(days=15), "price": "₹35 / $0.90"},
    "1_month": {"duration": timedelta(days=30), "price": "₹60 / $2.50"},
    "3_months": {"duration": timedelta(days=90), "price": "₹150 / $4.50"},
    "1_year": {"duration": timedelta(days=365), "price": "Negotiable / Negotiable"},
    "lifetime": {"duration": None, "price": "Negotiable / Negotiable"}
}
PREMIUM_PLATFORMS = ["instagram"]

# ===================================================================
# ==================== MARKUP GENERATORS ============================
# ===================================================================

def get_main_keyboard(user_id, premium_platforms):
    buttons = [
        [KeyboardButton("⚙️ ꜱᴇᴛᴛɪɴɢꜱ"), KeyboardButton("📊 ꜱᴛᴀᴛꜱ")]
    ]
    upload_buttons_row = []
    if "instagram" in premium_platforms:
        upload_buttons_row.extend([
            KeyboardButton("⚡ ɪɴꜱᴛᴀ ꜱᴛᴏʀy"),
            KeyboardButton("📸 ɪɴꜱᴛᴀ ᴩʜᴏᴛᴏ"),
            KeyboardButton("📤 ɪɴꜱᴛᴀ ʀᴇᴇʟ"),
            KeyboardButton("🗂️ ɪɴꜱᴛᴀ ᴀʟʙᴜᴍ")
        ])
    
    if upload_buttons_row:
        buttons.insert(0, upload_buttons_row)
    
    buttons.append([KeyboardButton("⭐ ᴩʀᴇᴍɪᴜᴍ"), KeyboardButton("/premiumdetails")])
    if is_admin(user_id):
        buttons.append([KeyboardButton("🛠 ᴀᴅᴍɪɴ ᴩᴀɴᴇʟ"), KeyboardButton("🔄 ʀᴇꜱᴛᴀʀᴛ ʙᴏᴛ")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True, selective=True)

def get_insta_settings_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 ᴄᴀᴩᴛɪᴏɴ", callback_data="set_caption_instagram")],
        [InlineKeyboardButton("🏷️ ʜᴀꜱʜᴛᴀɢꜱ", callback_data="set_hashtags_instagram")],
        [InlineKeyboardButton("🤝 ꜱᴇᴛ ᴅᴇꜰᴀᴜʟᴛ ᴄᴏʟʟᴀʙ", callback_data="set_collaborator_insta")], # <-- Changed
        [InlineKeyboardButton("📐 ᴀꜱᴩᴇᴄᴛ ʀᴀᴛɪᴏ (ᴠɪᴅᴇᴏ)", callback_data="set_aspect_ratio_instagram")],
        [InlineKeyboardButton("👤 ᴍᴀɴᴀɢᴇ ɪɢ ᴀᴄᴄᴏᴜɴᴛꜱ", callback_data="manage_ig_accounts")],
        [InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
    ])

async def get_insta_account_markup(user_id, logged_in_accounts):
    buttons = []
    user_settings = await get_user_settings(user_id)
    active_account = user_settings.get("active_ig_username")

    for account in logged_in_accounts:
        emoji = "✅" if active_account == account else "⬜"
        buttons.append([InlineKeyboardButton(f"{emoji} @{account}", callback_data=f"select_ig_account_{account}")])
    
    if active_account:
        buttons.append([InlineKeyboardButton("❌ ʟᴏɢᴏᴜᴛ ᴀᴄᴛɪᴠᴇ ᴀᴄᴄᴏᴜɴᴛ", callback_data=f"confirm_logout_ig_{active_account}")])

    buttons.append([InlineKeyboardButton("➕ ᴀᴅᴅ ɴᴇᴡ ᴀᴄᴄᴏᴜɴᴛ", callback_data="add_account_instagram")])
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ɪɢ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="hub_settings_instagram")])
    return InlineKeyboardMarkup(buttons)

def get_insta_logout_confirm_markup(username):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ Yes, Logout @{username}", callback_data=f"logout_ig_account_{username}")],
        [InlineKeyboardButton("❌ No, Cancel", callback_data="manage_ig_accounts")]
    ])

admin_markup = InlineKeyboardMarkup([
    [InlineKeyboardButton("👥 ᴜꜱᴇʀꜱ ʟɪꜱᴛ", callback_data="users_list"), InlineKeyboardButton("👤 ᴜꜱᴇʀ ᴅᴇᴛᴀɪʟꜱ", callback_data="admin_user_details")],
    [InlineKeyboardButton("➕ ᴍᴀɴᴀɢᴇ ᴩʀᴇᴍɪᴜᴍ", callback_data="manage_premium")],
    [InlineKeyboardButton("📢 ʙʀᴏᴀᴅᴄᴀꜱᴛ", callback_data="broadcast_message")],
    [InlineKeyboardButton("⚙️ ɢʟᴏʙᴀʟ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="global_settings_panel")],
    [InlineKeyboardButton("📊 ꜱᴛᴀᴛꜱ ᴩᴀɴᴇʟ", callback_data="admin_stats_panel")],
    [InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
])

def get_admin_global_settings_markup():
    event_status = "ON" if global_settings.get("special_event_toggle") else "OFF"
    compression_status = "ᴅɪꜱᴀʙʟᴇᴅ" if global_settings.get("no_compression_admin") else "ᴇɴᴀʙʟᴇᴅ"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📢 Special Event ({event_status})", callback_data="toggle_special_event")],
        [InlineKeyboardButton("✏️ Set Event Title", callback_data="set_event_title")],
        [InlineKeyboardButton("💬 Set Event Message", callback_data="set_event_message")],
        [InlineKeyboardButton("ᴍᴀx ᴜᴩʟᴏᴀᴅ ᴜꜱᴇʀꜱ", callback_data="set_max_uploads")],
        [InlineKeyboardButton("ʀᴇꜱᴇᴛ ꜱᴛᴀᴛꜱ", callback_data="reset_stats")],
        # [InlineKeyboardButton("ꜱʜᴏᴡ ꜱyꜱᴛᴇᴍ ꜱᴛᴀᴛꜱ", callback_data="show_system_stats")], <-- REMOVED
        [InlineKeyboardButton("🌐 ᴩʀᴏxʏ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="set_proxy_url")],
        [InlineKeyboardButton(f"🗜️ ᴄᴏᴍᴩʀᴇꜱꜱɪᴏɴ ({compression_status})", callback_data="toggle_compression_admin")],
        [InlineKeyboardButton("💰 ᴩᴀyᴍᴇɴᴛ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="payment_settings_panel")],
        [InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ", callback_data="admin_panel")]
    ])

payment_settings_markup = InlineKeyboardMarkup([
    [InlineKeyboardButton("🆕 ᴄʀᴇᴀᴛᴇ ᴩᴀyᴍᴇɴᴛ ʙᴜᴛᴛᴏɴ", callback_data="create_custom_payment_button")],
    [InlineKeyboardButton("ɢᴏᴏɢʟᴇ ᴩʟᴀy ǫʀ ᴄᴏᴅᴇ", callback_data="set_payment_google_play_qr")],
    [InlineKeyboardButton("ᴜᴩɪ", callback_data="set_payment_upi")],
    [InlineKeyboardButton("ᴜꜱᴅᴛ", callback_data="set_payment_usdt")],
    [InlineKeyboardButton("ʙᴛᴄ", callback_data="set_payment_btc")],
    [InlineKeyboardButton("ᴏᴛʜᴇʀꜱ", callback_data="set_payment_others")],
    [InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ɢʟᴏʙᴀʟ", callback_data="global_settings_panel")]
])

aspect_ratio_markup = InlineKeyboardMarkup([
    [InlineKeyboardButton("ᴏʀɪɢɪɴᴀʟ ᴀꜱᴩᴇᴄᴛ ʀᴀᴛɪᴏ", callback_data="set_ar_original")],
    [InlineKeyboardButton("9:16 (ᴄʀᴏᴩ/ғɪᴛ)", callback_data="set_ar_9_16")],
    [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="hub_settings_instagram")]
])

def get_platform_selection_markup(user_id, current_selection=None):
    if current_selection is None:
        current_selection = {}
    buttons = []
    for platform in PREMIUM_PLATFORMS:
        emoji = "✅" if current_selection.get(platform) else "⬜"
        buttons.append([InlineKeyboardButton(f"{emoji} {platform.capitalize()}", callback_data=f"select_platform_{platform}")])
    buttons.append([InlineKeyboardButton("➡️ ᴄᴏɴᴛɪɴᴜᴇ ᴛᴏ ᴩʟᴀɴꜱ", callback_data="confirm_platform_selection")])
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ", callback_data="admin_panel")])
    return InlineKeyboardMarkup(buttons)

def get_premium_plan_markup(user_id):
    buttons = []
    for key, value in PREMIUM_PLANS.items():
        buttons.append([InlineKeyboardButton(f"{key.replace('_', ' ').title()}", callback_data=f"show_plan_details_{key}")])
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="back_to_main_menu")])
    return InlineKeyboardMarkup(buttons)

def get_premium_details_markup(plan_key, is_admin_flow=False):
    plan_details = PREMIUM_PLANS[plan_key]
    buttons = []
    if is_admin_flow:
        buttons.append([InlineKeyboardButton(f"✅ Grant this Plan", callback_data=f"grant_plan_{plan_key}")])
    else:
        price_string = plan_details['price']
        buttons.append([InlineKeyboardButton(f"💰 ʙᴜy ɴᴏᴡ ({price_string})", callback_data="buy_now")])
        buttons.append([InlineKeyboardButton("➡️ ᴄʜᴇᴄᴋ ᴩᴀyᴍᴇɴᴛ ᴍᴇᴛʜᴏᴅꜱ", callback_data="show_payment_methods")])
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ᴩʟᴀɴꜱ", callback_data="back_to_premium_plans")])
    return InlineKeyboardMarkup(buttons)

def get_payment_methods_markup():
    payment_buttons = []
    settings = global_settings.get("payment_settings", {})
    
    if settings.get("google_play_qr_file_id"):
        payment_buttons.append([InlineKeyboardButton("ɢᴏᴏɢʟᴇ ᴩʟᴀy ǫʀ ᴄᴏᴅᴇ", callback_data="show_payment_qr_google_play")])
    if settings.get("upi"):
        payment_buttons.append([InlineKeyboardButton("ᴜᴩɪ", callback_data="show_payment_details_upi")])
    if settings.get("usdt"):
        payment_buttons.append([InlineKeyboardButton("ᴜꜱᴅᴛ", callback_data="show_payment_details_usdt")])
    if settings.get("btc"):
        payment_buttons.append([InlineKeyboardButton("ʙᴛᴄ", callback_data="show_payment_details_btc")])
    if settings.get("others"):
        payment_buttons.append([InlineKeyboardButton("ᴏᴛʜᴇʀꜱ", callback_data="show_payment_details_others")])

    # Add custom buttons
    custom_buttons = settings.get("custom_buttons", {})
    for btn_name in custom_buttons:
        payment_buttons.append([InlineKeyboardButton(btn_name.upper(), callback_data=f"show_custom_payment_{btn_name}")])

    payment_buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ᴩʀᴇᴍɪᴜᴍ ᴩʟᴀɴꜱ", callback_data="back_to_premium_plans")])
    return InlineKeyboardMarkup(payment_buttons)

def get_progress_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ", callback_data="cancel_upload")]
    ])

def get_upload_options_markup(is_album=False, is_premium=True):
    """Markup shown AFTER deferred download and caption set."""
    buttons = []
    if is_premium:
        # Replaced Tag and Location with Collaborator
        buttons.extend([
            [InlineKeyboardButton("🤝 ᴄᴏʟʟᴀʙᴏʀᴀᴛᴏʀ", callback_data="set_collaborator_insta")],
        ])
    
    # The primary action button
    buttons.append([InlineKeyboardButton("⬆️ ᴜᴩʟᴏᴀᴅ", callback_data="upload_now")])
    buttons.append([InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ", callback_data="cancel_upload")])
    return InlineKeyboardMarkup(buttons)

# ===================================================================
# ====================== HELPER FUNCTIONS ===========================
# ===================================================================

def is_admin(user_id):
    return user_id == ADMIN_ID

async def _get_user_data(user_id):
    if db is None:
        return {"_id": user_id, "premium": {}}
    return await asyncio.to_thread(db.users.find_one, {"_id": user_id})

async def _save_user_data(user_id, data_to_update):
    if db is None:
        logger.warning(f"DB not connected. Skipping save for user {user_id}.")
        return
    serializable_data = {}
    for key, value in data_to_update.items():
        if isinstance(value, dict):
            serializable_data[key] = {k: v for k, v in value.items() if not k.startswith('$')}
        else:
            serializable_data[key] = value
    await asyncio.to_thread(
        db.users.update_one,
        {"_id": user_id},
        {"$set": serializable_data},
        upsert=True
    )

async def _update_global_setting(key, value):
    global_settings[key] = value
    if db is None:
        logger.warning(f"DB not connected. Skipping save for global setting '{key}'.")
        return
    await asyncio.to_thread(db.settings.update_one, {"_id": "global_settings"}, {"$set": {key: value}}, upsert=True)

async def is_premium_for_platform(user_id, platform):
    if user_id == ADMIN_ID:
        return True
    
    if db is None:
        return False

    user = await _get_user_data(user_id)
    if not user:
        return False

    platform_premium = user.get("premium", {}).get(platform, {})
    
    if not platform_premium or platform_premium.get("status") == "expired":
        return False
        
    premium_type = platform_premium.get("type")
    premium_until = platform_premium.get("until")

    if premium_type == "lifetime":
        return True

    if premium_until and isinstance(premium_until, datetime) and premium_until > datetime.utcnow():
        return True

    if premium_type and premium_until and premium_until <= datetime.utcnow():
        await asyncio.to_thread(
            db.users.update_one,
            {"_id": user_id},
            {"$set": {f"premium.{platform}.status": "expired"}}
        )
        logger.info(f"Premium for {platform} expired for user {user_id}. Status updated in DB.")

    return False

# MODIFIED FUNCTION TO SAVE DEVICE SETTINGS
async def save_platform_session(user_id, platform, session_data, device_settings, username):
    if db is None: return
    await asyncio.to_thread(
        db.sessions.update_one,
        {"user_id": user_id, "platform": platform, "username": username},
        {"$set": {
            "session_data": session_data,
            "device_settings": device_settings,
            "logged_in_at": datetime.utcnow()
        }},
        upsert=True
    )

async def load_platform_sessions(user_id, platform):
    if db is None: return []
    sessions = await asyncio.to_thread(list, db.sessions.find({"user_id": user_id, "platform": platform}))
    return sessions

# MODIFIED FUNCTION TO LOAD DEVICE SETTINGS
async def load_platform_session_data(user_id, platform, username):
    if db is None: return None, None
    session = await asyncio.to_thread(db.sessions.find_one, {"user_id": user_id, "platform": platform, "username": username})
    if session:
        return session.get("session_data"), session.get("device_settings")
    return None, None

async def delete_platform_session(user_id, platform, username):
    if db is None: return
    await asyncio.to_thread(db.sessions.delete_one, {"user_id": user_id, "platform": platform, "username": username})

async def save_user_settings(user_id, settings):
    if db is None:
        logger.warning(f"DB not connected. Skipping user settings save for user {user_id}.")
        return
    await asyncio.to_thread(
        db.settings.update_one,
        {"_id": user_id},
        {"$set": settings},
        upsert=True
    )

async def get_user_settings(user_id):
    settings = {}
    if db is not None:
        settings = await asyncio.to_thread(db.settings.find_one, {"_id": user_id}) or {}
    
    settings.setdefault("aspect_ratio_instagram", "original")
    settings.setdefault("caption_instagram", "")
    settings.setdefault("hashtags_instagram", "")
    settings.setdefault("active_ig_username", None)
    settings.setdefault("default_ig_collaborator", "") # <-- Added Collab Memory
    
    return settings

async def safe_edit_message(message, text, reply_markup=None, parse_mode=enums.ParseMode.MARKDOWN):
    try:
        if not message:
            logger.warning("safe_edit_message called with a None message object.")
            return
        current_text = getattr(message, 'text', '') or getattr(message, 'caption', '')
        if current_text and hasattr(current_text, 'strip') and current_text.strip() == text.strip() and message.reply_markup == reply_markup:
            return
        await message.edit_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" not in str(e):
            logger.warning(f"Couldn't edit message: {e}")

async def safe_reply(message, text, **kwargs):
    """A helper to reply to a message, safely handling different message types."""
    try:
        await message.reply(text, **kwargs)
    except Exception as e:
        logger.error(f"Failed to reply to message {message.id}: {e}")
        try:
            await app.send_message(message.chat.id, text, **kwargs)
        except Exception as e2:
            logger.error(f"Fallback send_message also failed for chat {message.chat.id}: {e2}")

async def restart_bot(msg):
    restart_msg_log = (
        "🔄 **Bot Restart Initiated (Graceful)**\n\n"
        f"👤 **By**: {msg.from_user.mention} (ID: `{msg.from_user.id}`)"
    )
    logger.info(f"User {msg.from_user.id} initiated graceful restart.")
    await send_log_to_channel(app, LOG_CHANNEL, restart_msg_log)
    await msg.reply(
        to_bold_sans("Graceful Restart Initiated...") + "\n\n"
        "The bot will shut down cleanly. If running under a process manager "
        "(like Docker, Koyeb, or systemd), it will restart automatically."
    )
    shutdown_event.set()

_progress_updates = {}

def progress_callback_threaded(current, total, ud_type, msg_id, chat_id, start_time, last_update_time):
    now = time.time()
    if now - last_update_time[0] < 2 and current != total:
        return
    last_update_time[0] = now
    
    with threading.Lock():
        _progress_updates[(chat_id, msg_id)] = {
            "current": current, "total": total, "ud_type": ud_type, "start_time": start_time, "now": now
        }

async def monitor_progress_task(chat_id, msg_id, progress_msg):
    try:
        while True:
            await asyncio.sleep(2)
            with threading.Lock():
                update_data = _progress_updates.get((chat_id, msg_id))
            if update_data:
                current, total, ud_type, start_time, now = (
                    update_data['current'], update_data['total'], update_data['ud_type'],
                    update_data['start_time'], update_data['now']
                )
                percentage = current * 100 / total
                speed = current / (now - start_time) if (now - start_time) > 0 else 0
                eta_seconds = (total - current) / speed if speed > 0 else 0
                eta = timedelta(seconds=int(eta_seconds))
                progress_bar = f"[{'█' * int(percentage / 5)}{' ' * (20 - int(percentage / 5))}]"
                progress_text = (
                    f"{to_bold_sans(f'{ud_type} Progress')}: `{progress_bar}`\n"
                    f"📊 **Percentage**: `{percentage:.2f}%`\n"
                    f"✅ **Downloaded**: `{current / (1024 * 1024):.2f}` MB / `{total / (1024 * 1024):.2f}` MB\n"
                    f"🚀 **Speed**: `{speed / (1024 * 1024):.2f}` MB/s\n"
                    f"⏳ **ETA**: `{eta}`"
                )
                try:
                    await safe_edit_message(
                        progress_msg, progress_text,
                        reply_markup=get_progress_markup(),
                        parse_mode=None
                    )
                except Exception:
                    pass
            
            if update_data and update_data['current'] == update_data['total']:
                with threading.Lock():
                    _progress_updates.pop((chat_id, msg_id), None)
                break
    except asyncio.CancelledError:
        logger.info(f"Progress monitor task for msg {msg_id} was cancelled.")

async def cleanup_temp_files(files_to_delete):
    for file_path in files_to_delete:
        if file_path:
            try:
                # Run blocking I/O in a thread
                if await asyncio.to_thread(os.path.exists, file_path):
                    await asyncio.to_thread(os.remove, file_path)
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {e}")

def with_user_lock(func):
    @wraps(func)
    async def wrapper(client, message, *args, **kwargs):
        user_id = message.from_user.id
        if user_id not in user_upload_locks:
            user_upload_locks[user_id] = asyncio.Lock()

        if user_upload_locks[user_id].locked():
            return await message.reply("⚠️ " + to_bold_sans("Another Operation Is Already In Progress. Please Wait Until It's Finished Or Use The ❌ Cancel Button."))
        
        async with user_upload_locks[user_id]:
            return await func(client, message, *args, **kwargs)
    return wrapper

# ===================================================================
# ======================== COMMAND HANDLERS =========================
# ===================================================================

@app.on_message(filters.command("start") & main_bot_filter)
async def start(_, msg):
    user_id = msg.from_user.id
    user_first_name = msg.from_user.first_name or "there"
    
    is_ig_premium = await is_premium_for_platform(user_id, "instagram")
    premium_platforms = ["instagram"] if is_ig_premium or is_admin(user_id) else []

    if is_admin(user_id):
        welcome_msg = to_bold_sans("Welcome To The Direct Upload Bot!") + "\n\n"
        welcome_msg += "🛠️ " + to_bold_sans("You Have Admin Privileges.")
        await msg.reply(welcome_msg, reply_markup=get_main_keyboard(user_id, ["instagram"]))
        return

    user = await _get_user_data(user_id)
    is_new_user = not user or "added_by" not in user
    if is_new_user:
        await _save_user_data(user_id, {
            "_id": user_id, "premium": {}, "added_by": "self_start", 
            "added_at": datetime.utcnow(), "username": msg.from_user.username
        })
        logger.info(f"New user {user_id} added to database via start command.")
        await send_log_to_channel(app, LOG_CHANNEL, f"🌟 New user started bot: `{user_id}` (`{msg.from_user.username or 'N/A'}`)")
        welcome_msg = (
            f"👋 **Hi {user_first_name}!**\n\n"
            + to_bold_sans("This Bot Lets You Upload Content To Instagram Directly From Telegram.") + "\n\n"
            + to_bold_sans("To Get A Taste Of The Premium Features, You Can Activate A Free 6-hour Trial For Instagram Right Now!")
        )
        trial_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Activate FREE 6-hour trial", callback_data="activate_trial_instagram")],
            [InlineKeyboardButton("➡️ View premium plans", callback_data="buypypremium")]
        ])
        await msg.reply(welcome_msg, reply_markup=trial_markup, parse_mode=enums.ParseMode.MARKDOWN)
        return
    else:
        await _save_user_data(user_id, {"last_active": datetime.utcnow(), "username": msg.from_user.username})

    event_toggle = global_settings.get("special_event_toggle", False)
    if event_toggle:
        event_title = global_settings.get("special_event_title", "🎉 Special Event!")
        event_message = global_settings.get("special_event_message", "Enjoy our special event features!")
        event_text = f"**{event_title}**\n\n{event_message}"
        await msg.reply(event_text, reply_markup=get_main_keyboard(user_id, premium_platforms), parse_mode=enums.ParseMode.MARKDOWN)
        return

    user_premium = user.get("premium", {})
    ig_premium_data = user_premium.get("instagram", {})
    welcome_msg = to_bold_sans("Welcome Back To Telegram ➜ Direct Uploader") + "\n\n"
    premium_details_text = ""
    if is_ig_premium:
        ig_expiry = ig_premium_data.get("until")
        if ig_expiry:
            remaining_time = ig_expiry - datetime.utcnow()
            days, hours = remaining_time.days, remaining_time.seconds // 3600
            premium_details_text += f"⭐ Instagram premium expires in: `{days} days, {hours} hours`.\n"
    else:
        premium_details_text = (
            "🔥 **Key Features:**\n"
            "✅ Direct Login (No tokens needed)\n"
            "✅ Ultra-fast uploading & High Quality\n"
            "✅ No file size limit & unlimited uploads\n"
            "✅ Instagram Support\n\n"
            "👤 Contact Admin → [Admin Tom](https://t.me/CjjTom) to get premium\n"
            "🔐 Your data is fully encrypted\n\n"
            f"🆔 Your ID: `{user_id}`"
        )
    welcome_msg += premium_details_text
    await msg.reply(welcome_msg, reply_markup=get_main_keyboard(user_id, premium_platforms), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.command("restart") & filters.user(ADMIN_ID) & main_bot_filter)
async def restart_cmd(_, msg):
    await restart_bot(msg)

@app.on_message(filters.command(["instagramlogin", "iglogin"]) & main_bot_filter)
@with_user_lock
async def instagram_login_cmd(_, msg):
    user_id = msg.from_user.id
    if not await is_premium_for_platform(user_id, "instagram"):
        return await msg.reply("❌ " + to_bold_sans("Instagram Premium Access Is Required. Use /buypypremium To Upgrade."))

    user_states[user_id] = {"action": "waiting_for_instagram_username", "platform": "instagram"}
    await msg.reply("👤 " + to_bold_sans("Please Send Your Instagram Username."))

@app.on_message(filters.command("buypypremium") & main_bot_filter)
@app.on_message(filters.regex("⭐ ᴩʀᴇᴍɪᴜᴍ") & main_bot_filter)
async def show_premium_options(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})
    premium_plans_text = (
        "⭐ " + to_bold_sans("Upgrade To Premium!") + " ⭐\n\n"
        + to_bold_sans("Unlock Full Features And Upload Unlimited Content Without Restrictions.") + "\n\n"
        "**Available Plans:**"
    )
    await msg.reply(premium_plans_text, reply_markup=get_premium_plan_markup(user_id), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.command("premiumdetails") & main_bot_filter)
async def premium_details_cmd(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})
    user = await _get_user_data(user_id)
    if not user:
        return await msg.reply(to_bold_sans("You Are Not Registered With The Bot. Please Use /start."))
    if is_admin(user_id):
        return await msg.reply("👑 " + to_bold_sans("You Are The Admin. You Have Permanent Full Access To All Features!"), parse_mode=enums.ParseMode.MARKDOWN)

    status_text = "⭐ " + to_bold_sans("Your Premium Status:") + "\n\n"
    has_premium_any = False
    for platform in PREMIUM_PLATFORMS:
        if await is_premium_for_platform(user_id, platform):
            has_premium_any = True
            platform_premium = user.get("premium", {}).get(platform, {})
            premium_type = platform_premium.get("type")
            premium_until = platform_premium.get("until")
            status_text += f"**{platform.capitalize()} Premium:** "
            if premium_type == "lifetime":
                status_text += "🎉 **Lifetime!**\n"
            elif premium_until:
                remaining_time = premium_until - datetime.utcnow()
                days, hours, minutes = remaining_time.days, remaining_time.seconds // 3600, (remaining_time.seconds % 3600) // 60
                status_text += (
                    f"`{premium_type.replace('_', ' ').title()}` expires on: "
                    f"`{premium_until.strftime('%Y-%m-%d %H:%M:%S')} UTC`\n"
                    f"Time Remaining: `{days} days, {hours} hours, {minutes} minutes`\n"
                )
            status_text += "\n"
    
    if not has_premium_any:
        status_text = "😔 " + to_bold_sans("You Currently Have No Active Premium.") + "\n\n" + "To unlock all features, please contact **[Admin Tom](https://t.me/CjjTom)** to buy a premium plan."

    await msg.reply(status_text, parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.command("reset_profile") & main_bot_filter)
@with_user_lock
async def reset_profile_cmd(_, msg):
    user_id = msg.from_user.id
    await msg.reply("⚠️ **Warning!** " + to_bold_sans("This Will Clear All Your Saved Sessions And Settings. Are You Sure You Want To Proceed?"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Yes, reset my profile", callback_data="confirm_reset_profile")],
            [InlineKeyboardButton("❌ No, cancel", callback_data="back_to_main_menu")]
        ]),
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_message(filters.command("broadcast") & filters.user(ADMIN_ID) & main_bot_filter)
async def broadcast_cmd(_, msg):
    if db is None:
        return await msg.reply("⚠️ " + to_bold_sans("Database Is Unavailable. Cannot Fetch User List For Broadcast."))
    if len(msg.text.split(maxsplit=1)) < 2:
        return await msg.reply("Usage: `/broadcast <your message>`", parse_mode=enums.ParseMode.MARKDOWN)
    
    broadcast_message = msg.text.split(maxsplit=1)[1]
    users_cursor = await asyncio.to_thread(db.users.find, {})
    users = await asyncio.to_thread(list, users_cursor)
    sent_count, failed_count = 0, 0
    status_msg = await msg.reply("📢 " + to_bold_sans("Starting Broadcast..."))
    
    for user in users:
        try:
            if user["_id"] == ADMIN_ID: continue
            await app.send_message(user["_id"], broadcast_message, parse_mode=enums.ParseMode.MARKDOWN)
            sent_count += 1
            await asyncio.sleep(0.1)
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to send broadcast to user {user['_id']}: {e}")
            
    await status_msg.edit_text(f"✅ **Broadcast finished!**\nSent to `{sent_count}` users, failed for `{failed_count}` users.")
    await send_log_to_channel(app, LOG_CHANNEL,
        f"📢 Broadcast initiated by admin `{msg.from_user.id}`\n"
        f"Sent: `{sent_count}`, Failed: `{failed_count}`"
    )

@app.on_message(filters.command("skip") & filters.private & main_bot_filter)
@with_user_lock
async def handle_skip_command(_, msg):
    user_id = msg.from_user.id
    state_data = user_states.get(user_id)
    if not state_data or state_data.get('action') not in ['waiting_for_caption']:
        return

    file_info = state_data.get("file_info", {})
    file_info["custom_caption"] = None # Signal to use default
    user_states[user_id]["file_info"] = file_info

    await _deferred_download_and_show_options(msg, file_info)


@app.on_message(filters.command("done") & filters.private & main_bot_filter)
@with_user_lock
async def handle_done_command(_, msg):
    user_id = msg.from_user.id
    state_data = user_states.get(user_id)
    if not state_data or state_data.get('action') not in ['waiting_for_album_media']:
        return await msg.reply("❌ " + to_bold_sans("There Is No Active Multi-media Upload Process. Please Use The Appropriate Button To Start."))

    media_paths = state_data.get('media_paths', [])
    if not media_paths:
        return await msg.reply("❌ " + to_bold_sans("You Must Send At Least One Media File."))

    # Transition to caption state for the album
    file_info = {
        "platform": state_data['platform'],
        "upload_type": "album",
        "media_paths": media_paths,
        "original_msgs": state_data.get('media_msgs', []),
        "original_msg": msg
    }
    user_states[user_id] = {"action": "waiting_for_caption", "file_info": file_info}
    await msg.reply(
        to_bold_sans("Album Files Received. Now, Send Your Title/caption.") + "\n\n" +
        "• " + to_bold_sans("Send Text Now") + "\n" +
        "• Or use the `/skip` command to use your default caption."
    )

# ===================================================================
# ======================== REGEX HANDLERS ===========================
# ===================================================================

@app.on_message(filters.regex("🔄 ʀᴇꜱᴛᴀʀᴛ ʙᴏᴛ") & filters.user(ADMIN_ID) & main_bot_filter)
async def restart_button_handler(_, msg):
    await restart_bot(msg)

@app.on_message(filters.regex("⚙️ ꜱᴇᴛᴛɪɴɢꜱ") & main_bot_filter)
async def settings_menu(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})

    is_ig_premium = await is_premium_for_platform(user_id, "instagram")
    if not is_admin(user_id) and not is_ig_premium:
        return await msg.reply("❌ " + to_bold_sans("Instagram Premium Required To Access Ig Settings. Use /buypypremium To Upgrade."))
    
    await msg.reply(
        "⚙️ " + to_bold_sans("Configure Your Instagram Settings:"),
        reply_markup=get_insta_settings_markup()
    )

@app.on_message(filters.regex("🛠 ᴀᴅᴍɪɴ ᴩᴀɴᴇʟ") & filters.user(ADMIN_ID) & main_bot_filter)
async def admin_panel_button_handler(_, msg):
    await msg.reply(
        "🛠 " + to_bold_sans("Welcome To The Admin Panel!") + "\n\n"
        + to_bold_sans("Use The Buttons Below To Manage The Bot."),
        reply_markup=admin_markup,
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_message(filters.regex("📊 ꜱᴛᴀᴛꜱ") & filters.user(ADMIN_ID) & main_bot_filter)
async def show_stats(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})
    if db is None: return await msg.reply("⚠️ " + to_bold_sans("Database Is Currently Unavailable."))
    
    if not is_admin(user_id):
        return await msg.reply("❌ " + to_bold_sans("Admin Only."))

    total_users = await asyncio.to_thread(db.users.count_documents, {})
    
    pipeline = [
        {"$project": {
            "is_premium": {"$or": [
                {"$or": [
                    {"$eq": [f"$premium.{p}.type", "lifetime"]},
                    {"$gt": [f"$premium.{p}.until", datetime.utcnow()]}
                ]} for p in PREMIUM_PLATFORMS
            ]},
            "platforms": {p: {"$or": [
                {"$eq": [f"$premium.{p}.type", "lifetime"]},
                {"$gt": [f"$premium.{p}.until", datetime.utcnow()]}
            ]} for p in PREMIUM_PLATFORMS}
        }},
        {"$group": {
            "_id": None,
            "total_premium": {"$sum": {"$cond": ["$is_premium", 1, 0]}},
            **{f"{p}_premium": {"$sum": {"$cond": [f"$platforms.{p}", 1, 0]}} for p in PREMIUM_PLATFORMS}
        }}
    ]
    
    try:
        result = await asyncio.to_thread(list, db.users.aggregate(pipeline))
    except OperationFailure as e:
        logger.error(f"Stats aggregation failed: {e}")
        return await msg.reply("⚠️ " + to_bold_sans("Could Not Fetch Bot Statistics Due To A Database Error."))

    total_premium_users = 0
    premium_counts = {p: 0 for p in PREMIUM_PLATFORMS}
    if result:
        total_premium_users = result[0].get('total_premium', 0)
        for p in PREMIUM_PLATFORMS:
            premium_counts[p] = result[0].get(f'{p}_premium', 0)
            
    total_uploads = await asyncio.to_thread(db.uploads.count_documents, {})
    
    stats_text = (
        f"📊 **{to_bold_sans('Bot Statistics:')}**\n\n"
        f"**Users**\n"
        f"👥 Total Users: `{total_users}`\n"
        f"👑 Admin Users: `{await asyncio.to_thread(db.users.count_documents, {'_id': ADMIN_ID})}`\n"
        f"⭐ Premium Users: `{total_premium_users}` ({total_premium_users / total_users * 100 if total_users > 0 else 0:.2f}%)\n"
    )
    for p in PREMIUM_PLATFORMS:
        stats_text += f"       - {p.capitalize()} Premium: `{premium_counts[p]}` ({premium_counts[p] / total_users * 100 if total_users > 0 else 0:.2f}%)\n"
        
    stats_text += (
        f"\n**Uploads**\n"
        f"📈 Total Uploads: `{total_uploads}`\n"
        f"🎬 Instagram Reels: `{await asyncio.to_thread(db.uploads.count_documents, {'platform': 'instagram', 'upload_type': 'reel'})}`\n"
        f"📸 Instagram Posts: `{await asyncio.to_thread(db.uploads.count_documents, {'platform': 'instagram', 'upload_type': 'post'})}`\n"
        f"⚡ Instagram Story: `{await asyncio.to_thread(db.uploads.count_documents, {'platform': 'instagram', 'upload_type': 'story'})}`\n"
        f"🗂️ Instagram Albums: `{await asyncio.to_thread(db.uploads.count_documents, {'platform': 'instagram', 'upload_type': 'album'})}`\n"
    )
    await msg.reply(stats_text, parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.regex("📤 ɪɴꜱᴛᴀ ʀᴇᴇʟ|📸 ɪɴꜱᴛᴀ ᴩʜᴏᴛᴏ|🗂️ ɪɴꜱᴛᴀ ᴀʟʙᴜᴍ|⚡ ɪɴꜱᴛᴀ ꜱᴛᴏʀy") & main_bot_filter)
@with_user_lock
async def initiate_instagram_upload(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})

    if not await is_premium_for_platform(user_id, "instagram"):
        return await msg.reply("❌ " + to_bold_sans("Your Access Has Been Denied. Please Upgrade To Instagram Premium."))

    sessions = await load_platform_sessions(user_id, "instagram")
    if not sessions:
        return await msg.reply("❌ " + to_bold_sans("Please Login To Instagram First Using /instagramlogin"), parse_mode=enums.ParseMode.MARKDOWN)
    
    upload_type_map = {
        "📤 ɪɴꜱᴛᴀ ʀᴇᴇʟ": "reel",
        "📸 ɪɴꜱᴛᴀ ᴩʜᴏᴛᴏ": "post",
        "🗂️ ɪɴꜱᴛᴀ ᴀʟʙᴜᴍ": "album",
        "⚡ ɪɴꜱᴛᴀ ꜱᴛᴏʀy": "story"
    }
    upload_type = upload_type_map[msg.text]

    if upload_type == "album":
        user_states[user_id] = {
            "action": "waiting_for_album_media", "platform": "instagram",
            "upload_type": "album", "media_paths": [], "media_msgs": []
        }
        await msg.reply(
            "🗂️ " + to_bold_sans("Album Mode") + "\n\n"
            + to_bold_sans("Please Send Your Photos And Videos (up To 10).") + "\n"
            + "Once you are done, send the `/done` command to continue."
        )
    else:
        action = f"waiting_for_instagram_{upload_type}"
        user_states[user_id] = {"action": action, "platform": "instagram", "upload_type": upload_type}
        
        media_type = "photo or video"
        if upload_type == "reel": media_type = "video"
        if upload_type == "post": media_type = "photo"
        
        await msg.reply("✅ " + to_bold_sans(f"Send The {media_type} File, Ready When You Are!"))


# ===================================================================
# ======================== TEXT HANDLERS ============================
# ===================================================================

@app.on_message(filters.text & filters.private & ~filters.command("") & main_bot_filter)
@with_user_lock
async def handle_text_input(_, msg):
    user_id = msg.from_user.id
    state_data = user_states.get(user_id)
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})

    if not state_data:
        return await msg.reply(to_bold_sans("I Don't Understand That Command. Please Use The Menu Buttons To Interact With Me."))

    action = state_data.get("action")

    # --- Login Flow ---
    if action == "waiting_for_instagram_username":
        user_states[user_id]["username"] = msg.text
        user_states[user_id]["action"] = "waiting_for_instagram_password"
        return await msg.reply("🔑 " + to_bold_sans("Please Send Your Instagram Password."))
    
    elif action == "waiting_for_instagram_password":
        username = user_states[user_id]["username"]
        password = msg.text
        login_msg = await msg.reply("🔐 " + to_bold_sans("Attempting Instagram Login..."))
        
        # MODIFIED LOGIN TASK
        async def login_task():
            try:
                user_insta_client = InstaClient()
                user_insta_client.delay_range = [1, 3]
                proxy_url = global_settings.get("proxy_url")
                if proxy_url: user_insta_client.set_proxy(proxy_url)
                
                await asyncio.to_thread(user_insta_client.login, username, password)
                
                # Get both session and device settings
                session_data = user_insta_client.get_settings()
                device_settings = user_insta_client.device_settings

                # Save both to the database for persistence
                await save_platform_session(user_id, "instagram", session_data, device_settings, username)
                
                user_settings = await get_user_settings(user_id)
                user_settings["active_ig_username"] = username
                await save_user_settings(user_id, user_settings)
                
                await safe_edit_message(login_msg, f"✅ " + to_bold_sans(f"Instagram Login Successful For @{username}!"))
                log_text = (
                    f"📝 New Instagram Login\nUser: `{user_id}`\n"
                    f"Username: `{msg.from_user.username or 'N/A'}`\n"
                    f"Instagram: `{username}`"
                )
                await send_log_to_channel(app, LOG_CHANNEL, log_text)
                logger.info(f"Instagram login successful for user {user_id} ({username}).")
            except ChallengeRequired:
                await safe_edit_message(login_msg, "🔐 " + to_bold_sans("Challenge Required. Please Complete It In The Instagram App And Try Again."))
                logger.warning(f"Instagram Challenge Required for user {user_id} ({username}).")
            except (BadPassword, LoginRequired) as e:
                await safe_edit_message(login_msg, f"❌ " + to_bold_sans(f"Login Failed: {e}. Please Check Your Credentials."))
                logger.error(f"Instagram Login Failed for user {user_id} ({username}): {e}")
            except PleaseWaitFewMinutes:
                await safe_edit_message(login_msg, "⚠️ " + to_bold_sans("Instagram Is Asking To Wait A Few Minutes. Please Try Again Later."))
                logger.warning(f"Instagram 'Please Wait' for user {user_id} ({username}).")
            except Exception as e:
                await safe_edit_message(login_msg, f"❌ " + to_bold_sans(f"An Unexpected Error Occurred: {str(e)}"))
                logger.error(f"Unhandled error during Instagram login for {user_id} ({username}): {str(e)}", exc_info=True)
            finally:
                if user_id in user_states: del user_states[user_id]
        
        task_tracker.create_task(safe_task_wrapper(login_task()), user_id=user_id, task_name="login_instagram")
        return

    # --- Settings Flow ---
    elif action == "waiting_for_caption_instagram":
        platform = "instagram"
        settings = await get_user_settings(user_id)
        settings[f"caption_{platform}"] = msg.text
        await save_user_settings(user_id, settings)
        
        reply_msg = msg.reply_to_message or msg
        await safe_reply(reply_msg, "✅ " + to_bold_sans("Default Caption For Instagram Has Been Set."), reply_markup=get_insta_settings_markup())
        if user_id in user_states: del user_states[user_id]

    elif action == "waiting_for_hashtags_instagram":
        settings = await get_user_settings(user_id)
        settings["hashtags_instagram"] = msg.text
        await save_user_settings(user_id, settings)
        await safe_edit_message(msg.reply_to_message, "✅ " + to_bold_sans("Default Hashtags For Instagram Have Been Set."), reply_markup=get_insta_settings_markup())
        if user_id in user_states: del user_states[user_id]

    # --- Upload Flow ---
    elif action == "waiting_for_caption":
        file_info = state_data.get("file_info", {})
        is_premium = await is_premium_for_platform(user_id, file_info["platform"])
        caption = msg.text
        if not is_premium and len(caption) > 280:
            return await msg.reply("❌ " + to_bold_sans("For Free Accounts, The Caption Limit Is 280 Characters."))
        
        file_info["custom_caption"] = caption
        user_states[user_id]["file_info"] = file_info
        
        await _deferred_download_and_show_options(msg, file_info)

    # --- NEW/REPLACED SECTION for Collaborator ---
    elif action == "waiting_for_collaborator_insta" or action == "waiting_for_collaborator_insta_settings_only":
        collab_username = msg.text.strip().replace("@", "")
        
        # Get active session to verify username
        user_settings = await get_user_settings(user_id)
        active_username = user_settings.get("active_ig_username")
        if not active_username:
            return await msg.reply("❌ " + to_bold_sans("Instagram Session Expired. Please /login Again."))
        
        verify_msg = await msg.reply("🔄 " + to_bold_sans(f"Verifying Username @{collab_username}..."))
        
        try:
            user_upload_client = await get_insta_client_for_user(user_id, active_username)
            if not user_upload_client:
                raise LoginRequired("Could not validate session for collaborator search.")
            
            # Verify user exists
            await asyncio.to_thread(user_upload_client.user_info_by_username, collab_username)
            
            # Save as default
            user_settings["default_ig_collaborator"] = collab_username
            await save_user_settings(user_id, user_settings)
            
            reply_text = f"✅ " + to_bold_sans(f"Default Collaborator Set To: `{collab_username}`")
            reply_markup = get_insta_settings_markup()

            # If this was part of an upload flow, update the state
            if action == "waiting_for_collaborator_insta":
                file_info = state_data.get("file_info", {})
                file_info["collaborator_username"] = collab_username # For this upload
                user_states[user_id]["file_info"] = file_info
                
                reply_text = f"🤝 **" + to_bold_sans("Collaborator Set:") + f"** `{collab_username}`\n\n" + to_bold_sans("Continue With Other Options Or Upload Now.")
                reply_markup = get_upload_options_markup(is_album=file_info.get('upload_type') == 'album')
                user_states[user_id]['action'] = "waiting_for_upload_options"

            await safe_edit_message(verify_msg, reply_text, reply_markup=reply_markup, parse_mode=enums.ParseMode.MARKDOWN)

        except UserNotFound:
            await safe_edit_message(verify_msg, f"❌ " + to_bold_sans(f"User Not Found: `{collab_username}`. Please check the username and try again."), parse_mode=enums.ParseMode.MARKDOWN)
        except Exception as e:
            await safe_edit_message(verify_msg, f"❌ " + to_bold_sans(f"An Error Occurred: {e}"))
        finally:
            # Clear state only if it was a settings-only action
            if action == "waiting_for_collaborator_insta_settings_only":
                if user_id in user_states: del user_states[user_id]

    # --- DELETED ELIF BLOCKS for usertags and location ---

    # --- Admin Flow ---
    elif action == "waiting_for_target_user_id_premium_management":
        if not is_admin(user_id): return
        try:
            target_user_id = int(msg.text)
            user_states[user_id] = {"action": "select_platforms_for_premium", "target_user_id": target_user_id, "selected_platforms": {}}
            await msg.reply(
                f"✅ " + to_bold_sans(f"User Id `{target_user_id}` Received. Select Platforms For Premium:"),
                reply_markup=get_platform_selection_markup(user_id, {}),
                parse_mode=enums.ParseMode.MARKDOWN
            )
        except ValueError:
            await msg.reply("❌ " + to_bold_sans("Invalid User Id. Please Send A Valid Number."))
            if user_id in user_states: del user_states[user_id]

    elif action == "waiting_for_user_id_for_details":
        if not is_admin(user_id): return
        try:
            target_user_id = int(msg.text)
            await show_user_details(msg, target_user_id)
        except ValueError:
            await msg.reply("❌ " + to_bold_sans("Invalid User Id. Please Send A Valid Number."))
        finally:
            if user_id in user_states: del user_states[user_id]
            
    elif action == "waiting_for_max_uploads":
        if not is_admin(user_id): return
        try:
            new_limit = int(msg.text)
            if new_limit <= 0: return await msg.reply("❌ " + to_bold_sans("Limit Must Be A Positive Integer."))
            await _update_global_setting("max_concurrent_uploads", new_limit)
            global upload_semaphore
            upload_semaphore = asyncio.Semaphore(new_limit)
            await msg.reply(f"✅ " + to_bold_sans(f"Max Concurrent Uploads Set To `{new_limit}`."), reply_markup=get_admin_global_settings_markup())
            if user_id in user_states: del user_states[user_id]
        except ValueError:
            await msg.reply("❌ " + to_bold_sans("Invalid Input. Please Send A Valid Number."))

    elif action == "waiting_for_proxy_url":
        if not is_admin(user_id): return
        proxy_url = msg.text
        if proxy_url.lower() in ["none", "remove"]:
            await _update_global_setting("proxy_url", "")
            await msg.reply("✅ " + to_bold_sans("Bot Proxy Has Been Removed."))
        else:
            await _update_global_setting("proxy_url", proxy_url)
            await msg.reply(f"✅ " + to_bold_sans(f"Bot Proxy Set To: `{proxy_url}`."))
        if user_id in user_states: del user_states[user_id]
        if msg.reply_to_message:
            await safe_edit_message(msg.reply_to_message, to_bold_sans("Global Settings"), reply_markup=get_admin_global_settings_markup())

    elif action in ["waiting_for_event_title", "waiting_for_event_message"]:
        if not is_admin(user_id): return
        setting_key = "special_event_title" if action == "waiting_for_event_title" else "special_event_message"
        await _update_global_setting(setting_key, msg.text)
        await msg.reply(f"✅ " + to_bold_sans(f"Special Event `{setting_key.split('_')[-1]}` Updated!"), reply_markup=get_admin_global_settings_markup())
        if user_id in user_states: del user_states[user_id]

    elif action.startswith("waiting_for_payment_details_"):
        if not is_admin(user_id): return
        payment_method = action.replace("waiting_for_payment_details_", "")
        new_payment_settings = global_settings.get("payment_settings", {})
        new_payment_settings[payment_method] = msg.text
        await _update_global_setting("payment_settings", new_payment_settings)
        await msg.reply(f"✅ " + to_bold_sans(f"Payment Details For **{payment_method.upper()}** Updated."), reply_markup=payment_settings_markup, parse_mode=enums.ParseMode.MARKDOWN)
        if user_id in user_states: del user_states[user_id]

    elif action == "waiting_for_custom_button_name":
        if not is_admin(user_id): return
        user_states[user_id]['button_name'] = msg.text.strip()
        user_states[user_id]['action'] = "waiting_for_custom_button_details"
        await msg.reply("✍️ " + to_bold_sans("Enter Payment Details (text / Number / Address / Link):"))

    elif action == "waiting_for_custom_button_details":
        if not is_admin(user_id): return
        button_name = state_data['button_name']
        button_details = msg.text.strip()
        payment_settings = global_settings.get("payment_settings", {})
        if "custom_buttons" not in payment_settings:
            payment_settings["custom_buttons"] = {}
        payment_settings["custom_buttons"][button_name] = button_details
        await _update_global_setting("payment_settings", payment_settings)
        await msg.reply(f"✅ " + to_bold_sans(f"Payment Button `{button_name}` Created."), reply_markup=payment_settings_markup)
        if user_id in user_states: del user_states[user_id]


# ===================================================================
# =================== CALLBACK QUERY HANDLERS =======================
# ===================================================================

@app.on_callback_query(filters.regex("^confirm_reset_profile$") & main_bot_filter)
@with_user_lock
async def confirm_reset_profile_cb(_, query):
    user_id = query.from_user.id
    if db is not None:
        await asyncio.to_thread(db.users.delete_one, {"_id": user_id})
        await asyncio.to_thread(db.settings.delete_one, {"_id": user_id})
        await asyncio.to_thread(db.sessions.delete_many, {"user_id": user_id})
    
    if user_id in user_states:
        del user_states[user_id]
    
    await query.answer("✅ Your profile has been reset. Please use /start to begin again.", show_alert=True)
    await safe_edit_message(query.message, "✅ " + to_bold_sans("Your Profile Has Been Reset. Please Use /start To Begin Again."))

@app.on_callback_query(filters.regex("^hub_settings_instagram$") & main_bot_filter)
async def hub_settings_instagram_cb(_, query):
    await safe_edit_message(
        query.message, "⚙️ " + to_bold_sans("Configure Your Instagram Settings:"), reply_markup=get_insta_settings_markup()
    )

# --- Account Management Callbacks ---
@app.on_callback_query(filters.regex("^manage_ig_accounts$") & main_bot_filter)
async def manage_ig_accounts_cb(_, query):
    user_id = query.from_user.id
    sessions = await load_platform_sessions(user_id, "instagram")
    logged_in_accounts = [s['username'] for s in sessions]
    
    if not logged_in_accounts:
        await query.answer("You have no Instagram accounts logged in. Let's add one.", show_alert=True)
        user_states[user_id] = {"action": "waiting_for_instagram_username"}
        return await safe_edit_message(query.message, "👤 " + to_bold_sans("Please Send Your Instagram Username."))

    user_settings = await get_user_settings(user_id)
    active_account = user_settings.get("active_ig_username")
    
    await safe_edit_message(query.message, "👤 " + to_bold_sans("Select Your Uploading Account") + f"\n\nActive: `@{active_account or 'None'}`\n\n" + to_bold_sans("Select An Account To Make It Active, Or Manage Accounts."),
        reply_markup=await get_insta_account_markup(user_id, logged_in_accounts),
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_callback_query(filters.regex("^select_ig_account_") & main_bot_filter)
async def select_ig_account_cb(_, query):
    user_id = query.from_user.id
    username = query.data.split("select_ig_account_")[-1]
    
    user_settings = await get_user_settings(user_id)
    user_settings["active_ig_username"] = username
    await save_user_settings(user_id, user_settings)
    
    await query.answer(f"✅ @{username} is now your active Instagram account.", show_alert=True)
    await manage_ig_accounts_cb(app, query) # Refresh the panel

@app.on_callback_query(filters.regex("^confirm_logout_ig_") & main_bot_filter)
async def confirm_logout_ig_cb(_, query):
    username = query.data.split("confirm_logout_ig_")[-1]
    await safe_edit_message(
        query.message,
        to_bold_sans(f"Logout {username}? You Can Re-login Later."),
        reply_markup=get_insta_logout_confirm_markup(username)
    )

@app.on_callback_query(filters.regex("^logout_ig_account_") & main_bot_filter)
async def logout_ig_account_cb(_, query):
    user_id = query.from_user.id
    username_to_logout = query.data.split("logout_ig_account_")[-1]

    await delete_platform_session(user_id, "instagram", username_to_logout)
    
    user_settings = await get_user_settings(user_id)
    if user_settings.get("active_ig_username") == username_to_logout:
        sessions = await load_platform_sessions(user_id, "instagram")
        user_settings["active_ig_username"] = sessions[0]['username'] if sessions else None
        await save_user_settings(user_id, user_settings)
    
    await query.answer(f"✅ Logged out from @{username_to_logout}.", show_alert=True)
    await manage_ig_accounts_cb(app, query) # Refresh the panel

@app.on_callback_query(filters.regex("^add_account_") & main_bot_filter)
async def add_account_cb(_, query):
    user_id = query.from_user.id
    platform = query.data.split("add_account_")[-1]
    
    if not await is_premium_for_platform(user_id, platform) and not is_admin(user_id):
        return await query.answer("❌ This is a premium feature.", show_alert=True)
    
    user_states[user_id] = {"action": f"waiting_for_{platform}_username"}
    await safe_edit_message(query.message, f"👤 " + to_bold_sans(f"Please Send Your {platform.capitalize()} Username."))

# --- General Callbacks ---
@app.on_callback_query(filters.regex("^cancel_upload$") & main_bot_filter)
async def cancel_upload_cb(_, query):
    user_id = query.from_user.id
    await query.answer("Upload cancelled.", show_alert=True)
    await safe_edit_message(query.message, "❌ **" + to_bold_sans("Upload Cancelled") + "**\n\n" + to_bold_sans("Your Operation Has Been Successfully Cancelled."))

    state_data = user_states.get(user_id, {})
    files_to_clean = []
    file_info = state_data.get("file_info", {})
    if "media_paths" in file_info:
        files_to_clean.extend(file_info["media_paths"])
    if "downloaded_path" in file_info:
        files_to_clean.append(file_info.get("downloaded_path"))
    
    await cleanup_temp_files(files_to_clean)
    if user_id in user_states: del user_states[user_id]
    await task_tracker.cancel_all_user_tasks(user_id)
    logger.info(f"User {user_id} cancelled their upload.")

@app.on_callback_query(filters.regex("^upload_now$") & main_bot_filter)
async def upload_now_cb(_, query):
    user_id = query.from_user.id
    state_data = user_states.get(user_id)
    if not state_data or "file_info" not in state_data:
        return await query.answer("❌ Error: No upload process found to continue.", show_alert=True)
    
    file_info = state_data["file_info"]
    await safe_edit_message(query.message, "🚀 " + to_bold_sans("Starting Upload Now..."))
    await start_upload_task(query.message, file_info, user_id=query.from_user.id)

# --- NEW COLLAB HANDLER (replaces tag and location) ---
@app.on_callback_query(filters.regex("^set_collaborator_insta$") & main_bot_filter)
async def set_collaborator_insta_cb(_, query):
    user_id = query.from_user.id
    if not await is_premium_for_platform(user_id, "instagram"):
        return await query.answer("❌ This is a premium feature.", show_alert=True)

    state_data = user_states.get(user_id, {})
    
    # Check if this is part of an active upload or just changing settings
    is_upload_flow = state_data.get('action') in ["waiting_for_upload_options", "waiting_for_caption"]

    user_settings = await get_user_settings(user_id)
    default_collab = user_settings.get("default_ig_collaborator")

    if is_upload_flow:
        state_data['action'] = 'waiting_for_collaborator_insta'
        user_states[user_id] = state_data
    else:
        # This is from the main settings menu
        user_states[user_id] = {"action": "waiting_for_collaborator_insta_settings_only"}

    text = "🤝 " + to_bold_sans("Please Send The Instagram Username Of The Collaborator (e.g., `username`).")
    if default_collab:
        text += f"\n\nℹ️ " + to_bold_sans(f"Your Current Default Collaborator Is: `{default_collab}`. Sending a new username will update this.")
    else:
        text += "\n\nℹ️ " + to_bold_sans("This will also be saved as your default for future uploads.")

    await safe_edit_message(
        query.message,
        text,
        parse_mode=enums.ParseMode.MARKDOWN
    )

# --- DELETED HANDLERS for Tag, Location, Select Location, Cancel Location ---

# --- Premium & Payment Callbacks ---
@app.on_callback_query(filters.regex("^buypypremium$") & main_bot_filter)
async def buypypremium_cb(_, query):
    user_id = query.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})
    
    premium_plans_text = (
        "⭐ " + to_bold_sans("Upgrade To Premium!") + " ⭐\n\n"
        + to_bold_sans("Unlock Full Features And Upload Unlimited Content Without Restrictions.") + "\n\n"
        "**Available Plans:**"
    )
    await safe_edit_message(query.message, premium_plans_text, reply_markup=get_premium_plan_markup(user_id), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^show_plan_details_") & main_bot_filter)
async def show_plan_details_cb(_, query):
    user_id = query.from_user.id
    plan_key = query.data.split("show_plan_details_")[-1]
    
    state_data = user_states.get(user_id, {})
    is_admin_adding_premium = (is_admin(user_id) and state_data.get("action") == "select_premium_plan_for_platforms")
    
    plan_details = PREMIUM_PLANS[plan_key]
    plan_text = f"**{to_bold_sans(plan_key.replace('_', ' ').title() + ' Plan Details')}**\n\n**Duration**: "
    plan_text += f"{plan_details['duration'].days} days\n" if plan_details['duration'] else "Lifetime\n"
    plan_text += f"**Price**: {plan_details['price']}\n\n"
    
    if is_admin_adding_premium:
        target_user_id = state_data.get('target_user_id', 'Unknown User')
        plan_text += to_bold_sans(f"Click Below To Grant This Plan To User `{target_user_id}`.")
    else:
        plan_text += to_bold_sans("To Purchase, Click 'buy Now' Or Check The Available Payment Methods.")
        
    await safe_edit_message(
        query.message, plan_text,
        reply_markup=get_premium_details_markup(plan_key, is_admin_flow=is_admin_adding_premium),
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_callback_query(filters.regex("^show_payment_methods$") & main_bot_filter)
async def show_payment_methods_cb(_, query):
    payment_methods_text = "**" + to_bold_sans("Available Payment Methods") + "**\n\n"
    payment_methods_text += to_bold_sans("Choose Your Preferred Method To Proceed With Payment.")
    await safe_edit_message(query.message, payment_methods_text, reply_markup=get_payment_methods_markup(), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^show_payment_qr_google_play$") & main_bot_filter)
async def show_payment_qr_google_play_cb(_, query):
    qr_file_id = global_settings.get("payment_settings", {}).get("google_play_qr_file_id")
    if not qr_file_id:
        await query.answer("Google Pay QR code is not set by the admin yet.", show_alert=True)
        return
    
    caption_text = "**" + to_bold_sans("Scan & Pay Using Google Pay") + "**\n\n" + \
                     "Please send a screenshot of the payment to **[Admin Tom](https://t.me/CjjTom)** for activation."
    
    await query.message.reply_photo(
        photo=qr_file_id,
        caption=caption_text,
        parse_mode=enums.ParseMode.MARKDOWN
    )
    await query.answer()

@app.on_callback_query(filters.regex("^show_payment_details_") & main_bot_filter)
async def show_payment_details_cb(_, query):
    method = query.data.split("show_payment_details_")[1]
    payment_details = global_settings.get("payment_settings", {}).get(method, "No details available.")
    text = (
        f"**{to_bold_sans(f'{method.upper()} Payment Details')}**\n\n"
        f"`{payment_details}`\n\n"
        f"Please pay the required amount and contact **[Admin Tom](https://t.me/CjjTom)** with a screenshot of the payment for premium activation."
    )
    await safe_edit_message(query.message, text, reply_markup=get_payment_methods_markup(), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^show_custom_payment_") & main_bot_filter)
async def show_custom_payment_cb(_, query):
    button_name = query.data.split("show_custom_payment_")[1]
    payment_details = global_settings.get("payment_settings", {}).get("custom_buttons", {}).get(button_name, "No details available.")
    text = (
        f"**{to_bold_sans(f'{button_name.upper()} Payment Details')}**\n\n"
        f"`{payment_details}`\n\n"
        f"Please pay the required amount and contact **[Admin Tom](https://t.me/CjjTom)** with a screenshot of the payment for premium activation."
    )
    await safe_edit_message(query.message, text, reply_markup=get_payment_methods_markup(), parse_mode=enums.ParseMode.MARKDOWN)
    
    
@app.on_callback_query(filters.regex("^buy_now$") & main_bot_filter)
async def buy_now_cb(_, query):
    text = (
        f"**{to_bold_sans('Purchase Confirmation')}**\n\n"
        f"Please contact **[Admin Tom](https://t.me/CjjTom)** to complete the payment process."
    )
    await safe_edit_message(query.message, text, parse_mode=enums.ParseMode.MARKDOWN)

# --- Admin Panel Callbacks ---
@app.on_callback_query(filters.regex("^admin_panel$") & main_bot_filter)
async def admin_panel_cb(_, query):
    if not is_admin(query.from_user.id):
        return await query.answer("❌ Admin access required", show_alert=True)
    await safe_edit_message(
        query.message,
        "🛠 " + to_bold_sans("Welcome To The Admin Panel!"),
        reply_markup=admin_markup,
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_callback_query(filters.regex("^global_settings_panel$") & main_bot_filter)
async def global_settings_panel_cb(_, query):
    if not is_admin(query.from_user.id):
        return await query.answer("❌ Admin access required", show_alert=True)
    
    settings_text = (
        "⚙️ **" + to_bold_sans("Global Bot Settings") + "**\n\n"
        f"**📢 Special Event:** `{global_settings.get('special_event_toggle', False)}`\n"
        f"**Max concurrent uploads:** `{global_settings.get('max_concurrent_uploads')}`\n"
        f"**Global Proxy:** `{global_settings.get('proxy_url') or 'None'}`\n"
        f"**Global Compression:** `{'Disabled' if global_settings.get('no_compression_admin') else 'Enabled'}`"
    )
    await safe_edit_message(query.message, settings_text, reply_markup=get_admin_global_settings_markup(), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^payment_settings_panel$") & main_bot_filter)
async def payment_settings_panel_cb(_, query):
    if not is_admin(query.from_user.id):
        return await query.answer("❌ Admin access required", show_alert=True)

    await safe_edit_message(
        query.message,
        "💰 **" + to_bold_sans("Payment Settings") + "**\n\n" + to_bold_sans("Manage Payment Details For Premium Purchases."),
        reply_markup=payment_settings_markup,
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_callback_query(filters.regex("^back_to_") & main_bot_filter)
async def back_to_cb(_, query):
    data = query.data
    user_id = query.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})
    
    await task_tracker.cancel_all_user_tasks(user_id)
    if user_id in user_states: del user_states[user_id]
        
    if data == "back_to_main_menu":
        try:
            await query.message.delete()
        except Exception:
            pass
        is_ig_premium = await is_premium_for_platform(user_id, "instagram")
        premium_platforms = ["instagram"] if is_ig_premium or is_admin(user_id) else []
        await app.send_message(
            query.message.chat.id, "🏠 " + to_bold_sans("Main Menu"),
            reply_markup=get_main_keyboard(user_id, premium_platforms)
        )
    elif data == "back_to_settings":
        await safe_edit_message(query.message, "⚙️ " + to_bold_sans("Settings Panel"), reply_markup=get_main_settings_markup())
    elif data == "back_to_admin":
        await admin_panel_cb(app, query)
    elif data == "back_to_premium_plans":
        await buypypremium_cb(app, query)
    elif data == "back_to_global":
        await global_settings_panel_cb(app, query)
    else:
        await query.answer("❌ Unknown back action", show_alert=True)

@app.on_callback_query(filters.regex("^activate_trial_instagram$") & main_bot_filter)
async def activate_trial_instagram_cb(_, query):
    user_id = query.from_user.id
    user_first_name = query.from_user.first_name or "there"
    
    if await is_premium_for_platform(user_id, "instagram"):
        return await query.answer("Your Instagram trial is already active!", show_alert=True)

    premium_until = datetime.utcnow() + timedelta(hours=6)
    user_data = await _get_user_data(user_id) or {}
    user_premium_data = user_data.get("premium", {})
    user_premium_data["instagram"] = {
        "type": "6_hour_trial", "added_by": "callback_trial",
        "added_at": datetime.utcnow(), "until": premium_until,
        "status": "active"
    }
    await _save_user_data(user_id, {"premium": user_premium_data})

    logger.info(f"User {user_id} activated a 6-hour Instagram trial.")
    await send_log_to_channel(app, LOG_CHANNEL, f"✨ User `{user_id}` activated a 6-hour Instagram trial.")
    
    await query.answer("✅ Free 6-hour Instagram trial activated!", show_alert=True)
    welcome_msg = (
        f"🎉 **" + to_bold_sans(f"Congratulations, {user_first_name}!") + "**\n\n"
        + to_bold_sans("You Have Activated Your 6-hour Premium Trial For Instagram.") + "\n\n"
        + "To get started, please log in with: `/instagramlogin` or `/iglogin`"
    )
    premium_platforms = ["instagram"]
    await safe_edit_message(query.message, welcome_msg, reply_markup=get_main_keyboard(user_id, premium_platforms), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^toggle_special_event$") & main_bot_filter)
async def toggle_special_event_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    
    new_status = not global_settings.get("special_event_toggle", False)
    await _update_global_setting("special_event_toggle", new_status)
    await query.answer(f"Special Event toggled {'ON' if new_status else 'OFF'}.", show_alert=True)
    await global_settings_panel_cb(app, query)

@app.on_callback_query(filters.regex("^set_event_title$") & main_bot_filter)
async def set_event_title_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required.", show_alert=True)
    user_states[query.from_user.id] = {"action": "waiting_for_event_title"}
    await safe_edit_message(query.message, "✏️ " + to_bold_sans("Please Send The New Title For The Special Event."))

@app.on_callback_query(filters.regex("^set_event_message$") & main_bot_filter)
async def set_event_message_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required.", show_alert=True)
    user_states[query.from_user.id] = {"action": "waiting_for_event_message"}
    await safe_edit_message(query.message, "💬 " + to_bold_sans("Please Send The New Message For The Special Event."))

@app.on_callback_query(filters.regex("^toggle_compression_admin$") & main_bot_filter)
async def toggle_compression_admin_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    
    new_status = not global_settings.get("no_compression_admin", False)
    await _update_global_setting("no_compression_admin", new_status)
    await query.answer(f"Global compression toggled to: {'DISABLED' if new_status else 'ENABLED'}.", show_alert=True)
    await global_settings_panel_cb(app, query)

@app.on_callback_query(filters.regex("^set_max_uploads$") & main_bot_filter)
@with_user_lock
async def set_max_uploads_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    user_states[query.from_user.id] = {"action": "waiting_for_max_uploads"}
    current_limit = global_settings.get("max_concurrent_uploads")
    await safe_edit_message(
        query.message,
        to_bold_sans(f"Please Send The New Max Number Of Concurrent Uploads.\ncurrent Limit: `{current_limit}`"),
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_callback_query(filters.regex("^set_proxy_url$") & main_bot_filter)
@with_user_lock
async def set_proxy_url_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    user_states[query.from_user.id] = {"action": "waiting_for_proxy_url"}
    current_proxy = global_settings.get("proxy_url", "None set.")
    await safe_edit_message(
        query.message,
        "🌐 " + to_bold_sans("Please Send The New Proxy Url (e.g., `http://user:pass@ip:port`).") + "\n"
        + to_bold_sans(f"Type 'none' Or 'remove' To Disable.\ncurrent Proxy: `{current_proxy}`"),
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_callback_query(filters.regex("^reset_stats$") & main_bot_filter)
@with_user_lock
async def reset_stats_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    await safe_edit_message(query.message, "⚠️ **WARNING!** " + to_bold_sans("Are You Sure You Want To Reset All Upload Stats? This Is Irreversible."),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Yes, reset stats", callback_data="confirm_reset_stats")],
            [InlineKeyboardButton("❌ No, cancel", callback_data="admin_panel")]
        ]), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^confirm_reset_stats$") & main_bot_filter)
@with_user_lock
async def confirm_reset_stats_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    if db is None: return await query.answer("⚠️ Database unavailable.", show_alert=True)
    
    result = await asyncio.to_thread(db.uploads.delete_many, {})
    await query.answer(f"✅ All stats reset! Deleted {result.deleted_count} uploads.", show_alert=True)
    await admin_panel_cb(app, query)
    await send_log_to_channel(app, LOG_CHANNEL, f"📊 Admin `{query.from_user.id}` has reset all bot upload stats.")

# --- REMOVED show_system_stats_cb ---

@app.on_callback_query(filters.regex("^users_list$") & main_bot_filter)
async def users_list_cb(_, query):
    await _save_user_data(query.from_user.id, {"last_active": datetime.utcnow()})
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    if db is None: return await query.answer("⚠️ Database unavailable.", show_alert=True)
    
    users = await asyncio.to_thread(list, db.users.find({}))
    if not users:
        return await safe_edit_message(query.message, "👥 " + to_bold_sans("No Users Found."), reply_markup=admin_markup)
        
    user_list_text = "👥 **" + to_bold_sans("All Users:") + "**\n\n"
    for user in users:
        user_id = user["_id"]
        ig_sessions = await load_platform_sessions(user_id, "instagram")
        
        insta_usernames = [s["username"] for s in ig_sessions]
        added_at = user.get("added_at", "N/A").strftime("%Y-%m-%d") if isinstance(user.get("added_at"), datetime) else "N/A"
        last_active = user.get("last_active", "N/A").strftime("%Y-%m-%d %H:%M") if isinstance(user.get("last_active"), datetime) else "N/A"
        
        platform_statuses = []
        if user_id == ADMIN_ID:
            platform_statuses.append("👑 Admin")
        else:
            for platform in PREMIUM_PLATFORMS:
                if await is_premium_for_platform(user_id, platform):
                    platform_statuses.append(f"⭐ {platform.capitalize()}")
        status_line = " | ".join(platform_statuses) if platform_statuses else "❌ Free"
        
        user_list_text += (
            f"ID: `{user_id}` | {status_line}\n"
            f"IG Accounts: `{', '.join(insta_usernames) or 'N/A'}`\n"
            f"Added: `{added_at}` | Last Active: `{last_active}`\n"
            "-----------------------------------\n"
        )
    if len(user_list_text) > 4096:
        await safe_edit_message(query.message, to_bold_sans("User List Is Too Long, Sending As A File..."))
        with open("users.txt", "w", encoding="utf-8") as f:
            f.write(user_list_text.replace("`", ""))
        await app.send_document(query.message.chat.id, "users.txt", caption="👥 " + to_bold_sans("All Users List"))
        os.remove("users.txt")
        await safe_edit_message(query.message, "🛠 " + to_bold_sans("Admin Panel"), reply_markup=admin_markup)
    else:
        await safe_edit_message(query.message, user_list_text, reply_markup=admin_markup, parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^manage_premium$") & main_bot_filter)
@with_user_lock
async def manage_premium_cb(_, query):
    await _save_user_data(query.from_user.id, {"last_active": datetime.utcnow()})
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    
    user_states[query.from_user.id] = {"action": "waiting_for_target_user_id_premium_management"}
    await safe_edit_message(query.message, "➕ " + to_bold_sans("Please Send The User Id To Manage Their Premium Access."))

@app.on_callback_query(filters.regex("^admin_user_details$") & main_bot_filter)
@with_user_lock
async def admin_user_details_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    user_states[query.from_user.id] = {"action": "waiting_for_user_id_for_details"}
    await safe_edit_message(query.message, "👤 " + to_bold_sans("Please Send The Telegram User Id To View Their Details."))

async def show_user_details(message, target_user_id):
    """Helper function to fetch and display user details for an admin."""
    admin_id = message.from_user.id
    if db is None:
        return await message.reply("⚠️ " + to_bold_sans("Database Unavailable."))

    target_user = await _get_user_data(target_user_id)
    if not target_user:
        return await message.reply("❌ " + to_bold_sans(f"No User Found With Id `{target_user_id}`."))

    ig_sessions = await load_platform_sessions(target_user_id, "instagram")
    user_settings = await get_user_settings(target_user_id)
    active_ig = user_settings.get("active_ig_username")
    
    total_uploads = await asyncio.to_thread(db.uploads.count_documents, {"user_id": target_user_id})
    last_upload_doc = await asyncio.to_thread(db.uploads.find_one, {"user_id": target_user_id}, sort=[("timestamp", -1)])
    last_upload_time = last_upload_doc['timestamp'].strftime("%Y-%m-%d %H:%M") if last_upload_doc else "N/A"

    last_active = target_user.get("last_active", "N/A")
    if isinstance(last_active, datetime):
        last_active = last_active.strftime("%Y-%m-%d %H:%M")

    tg_username = target_user.get('username', 'N/A')
    
    details_text = f"👤 **{to_bold_sans('User Details')}**\n\n"
    details_text += f"**TG ID**: `{target_user_id}`\n"
    details_text += f"**TG Username**: ||`@{tg_username}`||\n"
    details_text += f"**Last Active**: `{last_active}`\n"
    details_text += f"**Total Uploads**: `{total_uploads}`\n"
    details_text += f"**Last Upload**: `{last_upload_time}`\n\n"
    details_text += f"🔗 **{to_bold_sans('Linked IG Accounts')}**\n"

    buttons = []
    if not ig_sessions:
        details_text += "`None`\n"
    else:
        for session in ig_sessions:
            username = session['username']
            logged_in_at = session.get('logged_in_at', 'N/A')
            if isinstance(logged_in_at, datetime):
                logged_in_at = logged_in_at.strftime("%Y-%m-%d %H:%M")
            
            active_marker = "✅" if username == active_ig else "⬜"
            details_text += f"{active_marker} **@{username}** (Logged in: `{logged_in_at}`)\n"
            
            if username != active_ig:
                buttons.append([InlineKeyboardButton(f"Set Active: @{username}", callback_data=f"admin_set_active_{target_user_id}_{username}")])
            buttons.append([InlineKeyboardButton(f"Logout: @{username}", callback_data=f"admin_logout_{target_user_id}_{username}")])
    
    buttons.append([InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="admin_panel")])
    reply_markup = InlineKeyboardMarkup(buttons)

    # Use the original message context if it's a callback query
    if hasattr(message, 'message') and message.message:
        await safe_edit_message(message.message, details_text, reply_markup, parse_mode=enums.ParseMode.MARKDOWN)
    else:
        await message.reply(details_text, reply_markup=reply_markup, parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^admin_set_active_") & main_bot_filter)
async def admin_set_active_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    _, target_user_id_str, username = query.data.split("_")
    target_user_id = int(target_user_id_str)
    
    settings = await get_user_settings(target_user_id)
    settings['active_ig_username'] = username
    await save_user_settings(target_user_id, settings)
    
    await query.answer(f"✅ Set @{username} as active for user {target_user_id}.", show_alert=True)
    await show_user_details(query, target_user_id)

@app.on_callback_query(filters.regex("^admin_logout_") & main_bot_filter)
async def admin_logout_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    _, target_user_id_str, username = query.data.split("_")
    target_user_id = int(target_user_id_str)

    await delete_platform_session(target_user_id, "instagram", username)
    
    settings = await get_user_settings(target_user_id)
    if settings.get('active_ig_username') == username:
        sessions = await load_platform_sessions(target_user_id, "instagram")
        settings['active_ig_username'] = sessions[0]['username'] if sessions else None
        await save_user_settings(target_user_id, settings)
        
    await query.answer(f"✅ Logged out @{username} for user {target_user_id}.", show_alert=True)
    await show_user_details(query, target_user_id)


@app.on_callback_query(filters.regex("^select_platform_") & main_bot_filter)
async def select_platform_cb(_, query):
    user_id = query.from_user.id
    if not is_admin(user_id): return await query.answer("❌ Admin access required", show_alert=True)
    
    state_data = user_states.get(user_id)
    if not isinstance(state_data, dict) or state_data.get("action") != "select_platforms_for_premium":
        return await query.answer("Error: State lost. Please try again.", show_alert=True)

    platform_to_toggle = query.data.split("select_platform_")[-1]
    selected_platforms = state_data.get("selected_platforms", {})
    selected_platforms[platform_to_toggle] = not selected_platforms.get(platform_to_toggle, False)
    
    state_data["selected_platforms"] = selected_platforms
    user_states[user_id] = state_data
    
    await safe_edit_message(
        query.message,
        f"✅ " + to_bold_sans(f"User Id `{state_data['target_user_id']}`. Select Platforms For Premium:"),
        reply_markup=get_platform_selection_markup(user_id, selected_platforms),
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_callback_query(filters.regex("^confirm_platform_selection$") & main_bot_filter)
async def confirm_platform_selection_cb(_, query):
    user_id = query.from_user.id
    if not is_admin(user_id): return await query.answer("❌ Admin access required", show_alert=True)
    
    state_data = user_states.get(user_id)
    if not isinstance(state_data, dict) or state_data.get("action") != "select_platforms_for_premium":
        return await query.answer("Error: State lost. Please restart.", show_alert=True)
        
    selected_platforms = [p for p, selected in state_data.get("selected_platforms", {}).items() if selected]
    if not selected_platforms:
        return await query.answer("Please select at least one platform!", show_alert=True)
        
    state_data["action"] = "select_premium_plan_for_platforms"
    state_data["final_selected_platforms"] = selected_platforms
    user_states[user_id] = state_data
    
    await safe_edit_message(
        query.message,
        f"✅ " + to_bold_sans(f"Platforms Selected: `{', '.join(p.capitalize() for p in selected_platforms)}`.\nnow, Select A Premium Plan For User `{state_data['target_user_id']}`:"),
        reply_markup=get_premium_plan_markup(user_id),
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_callback_query(filters.regex("^grant_plan_") & main_bot_filter)
async def grant_plan_cb(_, query):
    user_id = query.from_user.id
    if not is_admin(user_id): return await query.answer("❌ Admin access required", show_alert=True)
    if db is None: return await query.answer("⚠️ Database unavailable.", show_alert=True)
    
    state_data = user_states.get(user_id)
    if not isinstance(state_data, dict) or state_data.get("action") != "select_premium_plan_for_platforms":
        return await query.answer("❌ Error: State lost. Please start over.", show_alert=True)
        
    target_user_id = state_data["target_user_id"]
    selected_platforms = state_data["final_selected_platforms"]
    premium_plan_key = query.data.split("grant_plan_")[1]
    
    plan_details = PREMIUM_PLANS.get(premium_plan_key)
    if not plan_details:
        return await query.answer("Invalid premium plan selected.", show_alert=True)
    
    target_user_data = await _get_user_data(target_user_id) or {"_id": target_user_id, "premium": {}}
    premium_data = target_user_data.get("premium", {})
    
    for platform in selected_platforms:
        new_premium_until = None
        if plan_details["duration"] is not None:
            new_premium_until = datetime.utcnow() + plan_details["duration"]
        
        platform_premium_data = {
            "type": premium_plan_key, "added_by": user_id, "added_at": datetime.utcnow(), "status": "active"
        }
        if new_premium_until:
            platform_premium_data["until"] = new_premium_until
        
        premium_data[platform] = platform_premium_data
    
    await _save_user_data(target_user_id, {"premium": premium_data})
    
    admin_confirm_text = f"✅ " + to_bold_sans(f"Premium Granted To User `{target_user_id}` For:") + "\n"
    user_msg_text = "🎉 **" + to_bold_sans("Congratulations!") + "** 🎉\n\n" + to_bold_sans("You Have Been Granted Premium Access For:") + "\n"
    
    for platform in selected_platforms:
        updated_user = await _get_user_data(target_user_id)
        p_data = updated_user.get("premium", {}).get(platform, {})
        line = f"**{platform.capitalize()}**: `{p_data.get('type', 'N/A').replace('_', ' ').title()}`"
        if p_data.get("until"):
            line += f" (Expires: `{p_data['until'].strftime('%Y-%m-%d %H:%M')}` UTC)"
        admin_confirm_text += f"- {line}\n"
        user_msg_text += f"- {line}\n"
    
    user_msg_text += "\n" + to_bold_sans("Enjoy Your New Features! ✨")
    
    await safe_edit_message(query.message, admin_confirm_text, reply_markup=admin_markup, parse_mode=enums.ParseMode.MARKDOWN)
    await query.answer("Premium granted!", show_alert=False)
    if user_id in user_states: del user_states[user_id]
        
    try:
        await app.send_message(target_user_id, user_msg_text, parse_mode=enums.ParseMode.MARKDOWN)
        await send_log_to_channel(app, LOG_CHANNEL,
            f"💰 Premium granted to `{target_user_id}` by admin `{user_id}`. Platforms: `{', '.join(selected_platforms)}`, Plan: `{premium_plan_key}`"
        )
    except Exception as e:
        logger.error(f"Failed to notify user {target_user_id} about premium: {e}")
        await send_log_to_channel(app, LOG_CHANNEL,
            f"⚠️ Failed to notify user `{target_user_id}` about premium. Error: `{e}`"
        )

@app.on_callback_query(filters.regex("^broadcast_message$") & main_bot_filter)
async def broadcast_message_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    await safe_edit_message(
        query.message,
        "📢 " + to_bold_sans("Please Use The `/broadcast <message>` Command To Send A Message To All Users."),
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_callback_query(filters.regex("^admin_stats_panel$") & main_bot_filter)
async def admin_stats_panel_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    await safe_edit_message(query.message, to_bold_sans("Please Use The /stats Command To View Detailed Statistics."), reply_markup=admin_markup)

@app.on_callback_query(filters.regex("^set_caption_") & main_bot_filter)
async def set_caption_cb(_, query):
    user_id = query.from_user.id
    platform = query.data.split("set_caption_")[-1]
    user_states[user_id] = {"action": f"waiting_for_caption_{platform}"}
    await safe_edit_message(
        query.message,
        f"📝 " + to_bold_sans(f"Please Send Your New Default Caption For {platform.capitalize()}.")
    )

@app.on_callback_query(filters.regex("^set_hashtags_") & main_bot_filter)
async def set_hashtags_cb(_, query):
    user_id = query.from_user.id
    platform = query.data.split("set_hashtags_")[-1]
    if platform != "instagram":
        return await query.answer("Hashtags can only be set for Instagram.", show_alert=True)
    user_states[user_id] = {"action": f"waiting_for_hashtags_{platform}"}
    await safe_edit_message(
        query.message,
        f"🏷️ " + to_bold_sans(f"Please Send Your New Default Hashtags For {platform.capitalize()}.")
    )

@app.on_callback_query(filters.regex("^set_aspect_ratio_instagram$") & main_bot_filter)
async def set_aspect_ratio_cb(_, query):
    await safe_edit_message(
        query.message,
        "📐 " + to_bold_sans("Select The Aspect Ratio For Your Videos:"),
        reply_markup=aspect_ratio_markup,
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_callback_query(filters.regex("^set_ar_") & main_bot_filter)
async def set_aspect_ratio_value_cb(_, query):
    user_id = query.from_user.id
    aspect_ratio = query.data.split("set_ar_")[1]
    settings = await get_user_settings(user_id)
    settings["aspect_ratio_instagram"] = aspect_ratio
    await save_user_settings(user_id, settings)
    
    await query.answer(f"✅ Aspect ratio set to {aspect_ratio}.", show_alert=True)
    await safe_edit_message(query.message, "⚙️ " + to_bold_sans("Configure Your Instagram Settings:"), reply_markup=get_insta_settings_markup())

@app.on_callback_query(filters.regex("^create_custom_payment_button$") & main_bot_filter)
async def create_custom_payment_button_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    user_states[query.from_user.id] = {"action": "waiting_for_custom_button_name"}
    await safe_edit_message(query.message, "🆕 " + to_bold_sans("Enter Payment Button Name:"))

@app.on_callback_query(filters.regex("^set_payment_") & main_bot_filter)
async def set_payment_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    user_id = query.from_user.id
    method = query.data.split("set_payment_")[-1]
    
    if method == "google_play_qr":
        user_states[user_id] = {"action": "waiting_for_google_play_qr"}
        await safe_edit_message(query.message, "🖼️ " + to_bold_sans("Please Send The Qr Code Image For Google Pay."))
    else:
        user_states[user_id] = {"action": f"waiting_for_payment_details_{method}"}
        await safe_edit_message(query.message, f"✍️ " + to_bold_sans(f"Please Send The Details For {method.upper()}."))

# ===================================================================
# ======================== MEDIA HANDLERS ===========================
# ===================================================================

async def _deferred_download_and_show_options(msg, file_info):
    """Downloads the media and then shows the final upload options."""
    user_id = msg.from_user.id
    is_premium = await is_premium_for_platform(user_id, file_info['platform'])
    
    original_media_msg = file_info.get("original_media_msg")
    if not original_media_msg:
        logger.error(f"Critical error: original_media_msg not found in file_info for user {user_id}")
        return await msg.reply("❌ " + to_bold_sans("A Critical Error Occurred. Please Start Over."))

    processing_msg = await msg.reply("⏳ " + to_bold_sans("Starting Download..."))
    file_info["processing_msg"] = processing_msg
    
    try:
        start_time = time.time()
        last_update_time = [0]
        task_tracker.create_task(monitor_progress_task(msg.chat.id, processing_msg.id, processing_msg), user_id=user_id, task_name="progress_monitor")
        
        if file_info.get("upload_type") == "album":
            await asyncio.sleep(1) # For albums, download happens earlier.
        else:
            file_info["downloaded_path"] = await app.download_media(
                original_media_msg,
                progress=progress_callback_threaded,
                progress_args=("Download", processing_msg.id, msg.chat.id, start_time, last_update_time)
            )
        
        task_tracker.cancel_user_task(user_id, "progress_monitor")

        caption_preview = file_info.get('custom_caption') or '*(Using Default Caption)*'
        if len(caption_preview) > 100:
            caption_preview = caption_preview[:100] + "..."
            
        await safe_edit_message(
            processing_msg,
            "📝 " + to_bold_sans("Caption Ready. Choose Options Or Upload:") + f"\n\n**Preview:** `{caption_preview}`",
            reply_markup=get_upload_options_markup(is_album=file_info.get('upload_type') == 'album', is_premium=is_premium),
            parse_mode=enums.ParseMode.MARKDOWN
        )
        user_states[user_id] = {"action": "waiting_for_upload_options", "file_info": file_info}
        task_tracker.create_task(safe_task_wrapper(timeout_task(user_id, processing_msg.id)), user_id=user_id, task_name="timeout")

    except asyncio.CancelledError:
        logger.info(f"Deferred download cancelled by user {user_id}.")
        await cleanup_temp_files([file_info.get("downloaded_path")])
    except Exception as e:
        logger.error(f"Error during deferred file download for user {user_id}: {e}", exc_info=True)
        await safe_edit_message(processing_msg, f"❌ " + to_bold_sans(f"Download Failed: {e}"))
        await cleanup_temp_files([file_info.get("downloaded_path")])
        if user_id in user_states: del user_states[user_id]

@app.on_message(filters.media & filters.private & main_bot_filter)
@with_user_lock
async def handle_media_upload(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})
    state_data = user_states.get(user_id, {})

    if is_admin(user_id) and state_data and state_data.get("action") == "waiting_for_google_play_qr" and msg.photo:
        new_payment_settings = global_settings.get("payment_settings", {})
        new_payment_settings["google_play_qr_file_id"] = msg.photo.file_id
        await _update_global_setting("payment_settings", new_payment_settings)
        if user_id in user_states: del user_states[user_id]
        return await msg.reply("✅ " + to_bold_sans("Google Pay Qr Code Image Saved!"), reply_markup=payment_settings_markup)

    action = state_data.get("action")
    valid_actions = [
        "waiting_for_instagram_reel", "waiting_for_instagram_post",
        "waiting_for_instagram_story", "waiting_for_album_media"
    ]
    if not action or action not in valid_actions:
        return await msg.reply("❌ " + to_bold_sans("Please Use One Of The Upload Buttons First."))

    media = msg.video or msg.photo or msg.document
    if not media: return await msg.reply("❌ " + to_bold_sans("Unsupported Media Type."))

    if media.file_size > MAX_FILE_SIZE_BYTES:
        if user_id in user_states: del user_states[user_id]
        return await msg.reply(f"❌ " + to_bold_sans(f"File Size Exceeds The Limit Of `{MAX_FILE_SIZE_BYTES / (1024 * 1024):.2f}` Mb."))

    if action == "waiting_for_album_media":
        if len(state_data.get('media_paths', [])) >= 10:
            return await msg.reply("⚠️ " + to_bold_sans("Max 10 Items In An Album. Send `/done` To Finish."))
        
        processing_msg = await msg.reply("⏳ " + to_bold_sans("Downloading Media..."))
        try:
            file_path = await app.download_media(msg)
            state_data['media_paths'].append(file_path)
            state_data['media_msgs'].append(msg)
            
            num_files = len(state_data['media_paths'])
            await safe_edit_message(processing_msg, f"✅ " + to_bold_sans(f"Downloaded File {num_files} For Your Album. Send More Or Use `/done`."))
        except Exception as e:
            await safe_edit_message(processing_msg, f"❌ " + to_bold_sans(f"Download Failed: {e}"))
        return

    upload_type = state_data.get("upload_type")
    
    file_info = {
        "platform": state_data["platform"],
        "upload_type": upload_type,
        "original_media_msg": msg, 
        "collaborator_username": None, # <-- Changed from usertags
        "location": None
    }
    
    if upload_type == "story":
        user_states[user_id] = {"action": "finalizing_upload", "file_info": file_info}
        await start_upload_task(msg, file_info, user_id=msg.from_user.id)
        return
    
    user_states[user_id] = {"action": "waiting_for_caption", "file_info": file_info}
    await msg.reply(
        to_bold_sans("Media Received. First, Send Your Title/caption.") + "\n\n" +
        "• " + to_bold_sans("Send Text Now") + "\n" +
        "• Or use the `/skip` command to use your default caption."
    )


# ===================================================================
# ==================== UPLOAD PROCESSING ==========================
# ===================================================================

async def start_upload_task(msg, file_info, user_id):
    task_tracker.create_task(
        safe_task_wrapper(process_and_upload(msg, file_info, user_id)),
        user_id=user_id,
        task_name="upload"
    )

# CORRECTED HELPER FUNCTION TO RESTORE AND VALIDATE THE IG CLIENT
async def get_insta_client_for_user(user_id, username):
    """
    Creates and validates an Instagram client for a user using their saved session
    and device settings from the database for persistent sessions.
    """
    session_data, device_settings = await load_platform_session_data(user_id, "instagram", username)

    if not session_data or not device_settings:
        logger.error(f"Session or device settings not found for user {user_id} ({username}). Re-login required.")
        raise LoginRequired("Session data not found. Please log in again.")

    try:
        user_client = InstaClient()
        proxy_url = global_settings.get("proxy_url")
        if proxy_url:
            user_client.set_proxy(proxy_url)
        
        # CRITICAL FIX: Restore the device fingerprint FIRST
        user_client.device_settings = device_settings
        
        # Then, load the session cookies and other data
        await asyncio.to_thread(user_client.set_settings, session_data)
        
        # Re-login with the session ID to validate it and refresh internal state
        await asyncio.to_thread(user_client.login_by_sessionid, session_data['authorization_data']['sessionid'])
        
        # Make a test API call to ensure the session is fully functional
        await asyncio.to_thread(user_client.get_timeline_feed)
        
        logger.info(f"Successfully created and validated insta client for user {user_id} ({username})")
        return user_client
    except LoginRequired as e:
        logger.warning(f"Instagrapi reports LoginRequired for user {user_id} ({username}). Session may be expired. Error: {e}")
        raise # Re-raise the exception to be handled by the calling function
    except Exception as e:
        logger.error(f"Failed to create/validate insta client for user {user_id} ({username}). Error: {e}")
        # Raise LoginRequired to trigger a user-facing error message asking them to re-login.
        raise LoginRequired("IG session is invalid or expired. Please re-login.")


async def process_and_upload(msg, file_info, user_id, is_scheduled=False):
    platform = file_info["platform"]
    upload_type = file_info["upload_type"]
    processing_msg = file_info.get("processing_msg") or msg
    
    task_tracker.cancel_user_task(user_id, "timeout")

    async with upload_semaphore:
        logger.info(f"Semaphore acquired for user {user_id}. Starting upload to {platform}.")
        files_to_clean = []
        try:
            if upload_type == 'story' and 'downloaded_path' not in file_info:
                processing_msg = await msg.reply("⏳ " + to_bold_sans("Starting Download For Story..."))
                file_info['downloaded_path'] = await app.download_media(file_info['original_media_msg'])
                files_to_clean.append(file_info['downloaded_path']) # Add to cleanup

            user_settings = await get_user_settings(user_id)
            is_premium = await is_premium_for_platform(user_id, platform)
            
            default_caption = user_settings.get(f"caption_{platform}", "")
            hashtags = user_settings.get(f"hashtags_instagram", "") if platform == "instagram" else ""
            final_caption = file_info.get("custom_caption") if file_info.get("custom_caption") is not None else default_caption
            
            if hashtags:
                final_caption = f"{final_caption}\n\n{hashtags}"
            
            url, media_id, media_type_value = "N/A", "N/A", "N/A"

            # Helper to determine if media is a video
            def is_video(msg_context=None):
                if not msg_context: return False
                return msg_context.video is not None or (msg_context.document and 'video' in msg_context.document.mime_type)

            # === NEW CONVERSION-SPLIT LOGIC ===
            
            # Determine path/paths
            path = file_info.get("downloaded_path")
            paths = file_info.get("media_paths")
            original_media_msg = file_info.get("original_media_msg")
            original_album_msgs = file_info.get("original_msgs", [])

            # Add already downloaded paths to cleanup
            if path:
                files_to_clean.append(path)
            if paths:
                files_to_clean.extend(paths)

            # Check if any file needs conversion
            await safe_edit_message(processing_msg, "🤔 " + to_bold_sans("Checking file format..."), reply_markup=None)
            
            needs_any_conversion = False
            if upload_type == "album":
                for i, p in enumerate(paths):
                    msg_context = original_album_msgs[i] if i < len(original_album_msgs) else None
                    if is_video(msg_context) and await asyncio.to_thread(needs_conversion, p):
                        needs_any_conversion = True
                        break
            elif is_video(original_media_msg): # For reel, post (video), story
                if path and await asyncio.to_thread(needs_conversion, path):
                    needs_any_conversion = True

            # === WORKER BOT HANDOFF ===
            if needs_any_conversion:
                logger.info(f"Task for user {user_id} needs conversion. Offloading to worker.")
                await safe_edit_message(processing_msg, "⏳ " + to_bold_sans("File requires special processing... Sending to worker bot."))
                
                # We use processing_msg.id as a unique part of the task ID
                task_id = f"{user_id}_{processing_msg.id}"
                
                # Get collaborator username (either from flow or default settings)
                collab_username = file_info.get("collaborator_username") or user_settings.get("default_ig_collaborator")

                task_data = {
                    "_id": task_id,
                    "user_id": user_id,
                    "processing_msg_id": processing_msg.id,
                    "chat_id": processing_msg.chat.id,
                    "platform": platform,
                    "upload_type": upload_type,
                    "final_caption": final_caption,
                    "collaborator_username": collab_username, # <-- Added Collab
                    "is_premium": is_premium,
                    "status": "pending_conversion"
                }
                
                # Save to DB
                await asyncio.to_thread(db.tasks.update_one, {"_id": task_id}, {"$set": task_data}, upsert=True)
                
                # Forward files to worker channel
                if upload_type == "album":
                    media_msg_ids = [m.id for m in original_album_msgs]
                    forwarded_msgs = await app.forward_messages(
                        chat_id=WORKER_CHANNEL_ID,
                        from_chat_id=original_album_msgs[0].chat.id,
                        message_ids=media_msg_ids
                    )
                    # Reply to the last message of the forwarded group with the task_id
                    await app.send_message(WORKER_CHANNEL_ID, f"{task_id}", reply_to_message_id=forwarded_msgs[-1].id)
                
                else: # Single media
                    forwarded_msg = await app.forward_messages(
                        chat_id=WORKER_CHANNEL_ID,
                        from_chat_id=original_media_msg.chat.id,
                        message_ids=original_media_msg.id
                    )
                    # Reply to the forwarded message with the task_id
                    await app.send_message(WORKER_CHANNEL_ID, f"{task_id}", reply_to_message_id=forwarded_msg.id)

                # Clean up local files (worker will re-download from channel)
                # files_to_clean.append(path)
                # files_to_clean.extend(paths or [])
                # NOTE: Cleanup is now handled by the 'finally' block, no need to duplicate
                
                await safe_edit_message(processing_msg, "✅ " + to_bold_sans("File sent to processing worker. You will be notified upon completion."))
                
                # STOP execution for this task. The worker will pick it up.
                # The 'finally' block will still run.
                return 
            
            # === IF NO CONVERSION NEEDED (Original Logic) ===
            logger.info(f"Task for user {user_id} does not need conversion. Processing locally on Main Bot.")
            
            if platform == "instagram":
                active_username = user_settings.get("active_ig_username")
                if not active_username:
                    raise LoginRequired("No active IG account set. Please login and select an account.")
                
                await safe_edit_message(processing_msg, "🔑 " + to_bold_sans("Authenticating Session..."))
                
                user_upload_client = await get_insta_client_for_user(user_id, active_username)
                
                # Get collaborator username (either from flow or default settings)
                collab_username = file_info.get("collaborator_username") or user_settings.get("default_ig_collaborator")
                
                result = None # Define result here

                if upload_type == "reel":
                    # files_to_clean.append(path) # Already added
                    await safe_edit_message(processing_msg, "⬆️ " + to_bold_sans("Uploading To Instagram... Please Wait."))
                    result = await asyncio.to_thread(user_upload_client.clip_upload, path, final_caption, location=None) # Removed usertags/location
                    url = f"https://instagram.com/reel/{result.code}"

                elif upload_type == "post":
                    # files_to_clean.append(path) # Already added
                    await safe_edit_message(processing_msg, "⬆️ " + to_bold_sans("Uploading To Instagram... Please Wait."))
                    result = await asyncio.to_thread(user_upload_client.photo_upload, path, final_caption, location=None) # Removed usertags/location
                    url = f"https://instagram.com/p/{result.code}"

                elif upload_type == "album":
                    # files_to_clean.extend(paths) # Already added
                    await safe_edit_message(processing_msg, "⬆️ " + to_bold_sans("Uploading Album To Instagram... Please Wait."))
                    result = await asyncio.to_thread(user_upload_client.album_upload, paths, final_caption, location=None) # Removed usertags/location
                    url = f"https://instagram.com/p/{result.code}"

                elif upload_type == "story":
                    # files_to_clean.append(path) # Already added
                    uploader_func = user_upload_client.photo_upload_to_story
                    
                    if is_video(file_info.get('original_media_msg')):
                        uploader_func = user_upload_client.video_upload_to_story
                    
                    await safe_edit_message(processing_msg, "⬆️ " + to_bold_sans("Uploading Story..."))
                    result = await asyncio.to_thread(uploader_func, path)
                    url = f"https://instagram.com/stories/{active_username}/{result.pk}"
                
                media_id, media_type_value = result.pk, result.media_type

                # --- NEW COLLABORATOR LOGIC ---
                if collab_username and upload_type in ["reel", "post", "album"]:
                    try:
                        await safe_edit_message(processing_msg, "🤝 " + to_bold_sans(f"Inviting @{collab_username} as collaborator..."))
                        user_to_invite = await asyncio.to_thread(user_upload_client.user_info_by_username, collab_username)
                        await asyncio.to_thread(user_upload_client.media_invite_collaborator, media_id, user_to_invite.pk)
                        logger.info(f"Successfully invited {collab_username} to post {media_id}")
                    except UserNotFound:
                        logger.warning(f"Collaborator @{collab_username} not found. Skipping invite.")
                    except Exception as e:
                        logger.error(f"Failed to invite collaborator @{collab_username}: {e}")
                # --- END COLLABORATOR LOGIC ---

            if db is not None:
                await asyncio.to_thread(db.uploads.insert_one, {
                    "user_id": user_id, "media_id": str(media_id), "media_type": str(media_type_value),
                    "platform": platform, "upload_type": upload_type, "timestamp": datetime.utcnow(),
                    "url": url, "caption": final_caption
                })

            log_msg = f"📤 New {platform.capitalize()} {upload_type.capitalize()} Upload\n" \
                        f"👤 User: `{user_id}`\n🔗 URL: {url}\n📅 {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}"
            await safe_edit_message(processing_msg, f"✅ " + to_bold_sans("Uploaded Successfully!") + f"\n\n{url}", parse_mode=None)
            await send_log_to_channel(app, LOG_CHANNEL, log_msg)

        except asyncio.CancelledError:
            logger.warning(f"Upload process for user {user_id} was cancelled.")
            await safe_edit_message(processing_msg, "❌ " + to_bold_sans("Upload Process Cancelled."))
        except LoginRequired as e:
            error_msg = f"❌ " + to_bold_sans(f"Login Required. Session May Have Expired. Please Use /instagramlogin") + f".\nError: {e}"
            await safe_edit_message(processing_msg, error_msg, parse_mode=enums.ParseMode.MARKDOWN)
            logger.error(f"LoginRequired during upload for user {user_id}: {e}")
        except ClientError as e:
            error_msg = f"❌ " + to_bold_sans(f"Instagram Client Error: {e}. Please Try Again Later.")
            await safe_edit_message(processing_msg, error_msg, parse_mode=enums.ParseMode.MARKDOWN)
            logger.error(f"ClientError during upload for user {user_id}: {e}")
        except Exception as e:
            error_msg = f"❌ " + to_bold_sans(f"Upload Failed: {str(e)}")
            await safe_edit_message(processing_msg, error_msg, parse_mode=enums.ParseMode.MARKDOWN)
            logger.error(f"General upload failed for {user_id} on {platform}: {e}", exc_info=True)
        finally:
            await cleanup_temp_files(files_to_clean)
            if user_id in user_states: del user_states[user_id]
            logger.info(f"Semaphore released for user {user_id}.")

async def timeout_task(user_id, message_id):
    await asyncio.sleep(600)
    if user_id in user_states:
        del user_states[user_id]
        logger.info(f"Task for user {user_id} timed out and was canceled.")
        try:
            await app.edit_message_text(
                chat_id=user_id, message_id=message_id,
                text="⚠️ " + to_bold_sans("Timeout! The Operation Was Canceled Due To Inactivity.")
            )
        except Exception as e:
            logger.warning(f"Could not send timeout message to user {user_id}: {e}")

# === HTTP Server for Health Checks ===
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Bot is running")
    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()

def run_server():
    try:
        server = HTTPServer(('0.0.0.0', 8080), HealthHandler)
        logger.info("HTTP health check server started on port 8080.")
        server.serve_forever()
    except Exception as e:
        logger.error(f"HTTP server failed: {e}")

# ===================================================================
# ==================== WORKER BOT HANDLERS ========================
# ===================================================================

def worker_bot_only(_, __, msg):
    """Filter to ensure handler only runs on the Worker Bot in the WORKER_CHANNEL"""
    return IS_WORKER_BOOL and msg.chat.id == WORKER_CHANNEL_ID

worker_task_filter = filters.create(worker_bot_only)

def main_bot_worker_reply(_, __, msg):
    """Filter to ensure handler only runs on the Main Bot in the WORKER_CHANNEL"""
    return (not IS_WORKER_BOOL) and msg.chat.id == WORKER_CHANNEL_ID

main_bot_reply_filter = filters.create(main_bot_worker_reply)


# This handler is for the WORKER bot to RECEIVE tasks
@app.on_message(filters.text & worker_task_filter & filters.reply)
async def receive_task_handler(client, message):
    
    task_id = message.text.strip()
    if not task_id or task_id.startswith("done_"):
        return

    # Get the message this task_id is replying to
    replied_msg = message.reply_to_message
    if not replied_msg:
        logger.error(f"[WORKER] Task ID {task_id} is not a reply to any message.")
        return

    logger.info(f"[WORKER] Received task: {task_id}")
    
    media_messages = []
    try:
        # === FIX ===
        # Check if the REPLIED-TO message (the media) has a media_group_id
        if replied_msg.media_group_id:
            # It's an album
            logger.info(f"[WORKER] Task {task_id} is an album (group ID: {replied_msg.media_group_id}). Fetching group.")
            media_messages = await app.get_media_group(WORKER_CHANNEL_ID, replied_msg.id)
        else:
            # It's a single file
            logger.info(f"[WORKER] Task {task_id} is a single file.")
            media_messages.append(replied_msg)
        # === END FIX ===

    except FloodWait as e:
        logger.warning(f"[WORKER] FloodWait when getting media group for {task_id}. Sleeping for {e.value}s")
        await asyncio.sleep(e.value)
        return # Let it retry on next cycle
    except Exception as e:
        logger.error(f"[WORKER] Error getting media for task {task_id}: {e}")
        return

    if not media_messages:
        logger.error(f"[WORKER] No media found for task {task_id}")
        return

    # Fetch task data from DB
    task_data = await asyncio.to_thread(db.tasks.find_one, {"_id": task_id})
    if not task_data:
        logger.error(f"[WORKER] No DB entry found for task {task_id}")
        return
    
    if task_data.get("status") != "pending_conversion":
        logger.warning(f"[WORKER] Task {task_id} is not pending conversion (Status: {task_data.get('status')}). Skipping.")
        return

    await asyncio.to_thread(db.tasks.update_one, {"_id": task_id}, {"$set": {"status": "converting"}})
    
    converted_paths = []
    files_to_clean = []
    
    try:
        for i, media_msg in enumerate(media_messages):
            is_video_file = media_msg.video or (media_msg.document and 'video' in media_msg.document.mime_type)
            
            status_msg = f"Downloading file {i+1}/{len(media_messages)} for task {task_id}..."
            logger.info(f"[WORKER] {status_msg}")
            
            download_path = await client.download_media(media_msg)
            files_to_clean.append(download_path)
            
            if is_video_file and await asyncio.to_thread(needs_conversion, download_path):
                logger.info(f"[WORKER] Converting file: {download_path}")
                fixed_path = download_path.rsplit(".", 1)[0] + "_fixed.mp4"
                converted_path = await asyncio.to_thread(fix_for_instagram, download_path, fixed_path)
                
                converted_paths.append(converted_path)
                files_to_clean.append(converted_path)
            else:
                # It's a photo or a compatible video
                converted_paths.append(download_path)
        
        # Now, upload converted files back to the channel
        logger.info(f"[WORKER] Conversion complete for {task_id}. Uploading {len(converted_paths)} files back.")
        
        if len(converted_paths) > 1:
            media_group = []
            for path in converted_paths:
                if path.endswith((".mp4", ".mov", ".mkv")):
                    media_group.append(InputMediaVideo(path))
                else:
                    media_group.append(InputMediaPhoto(path))
            
            sent_msgs = await app.send_media_group(WORKER_CHANNEL_ID, media_group)
            await app.send_message(WORKER_CHANNEL_ID, f"done_{task_id}", reply_to_message_id=sent_msgs[-1].id)
        
        else: # Single file
            path = converted_paths[0]
            sent_msg = None
            if path.endswith((".mp4", ".mov", ".mkv")):
                sent_msg = await app.send_video(WORKER_CHANNEL_ID, path)
            else:
                sent_msg = await app.send_photo(WORKER_CHANNEL_ID, path)
            await app.send_message(WORKER_CHANNEL_ID, f"done_{task_id}", reply_to_message_id=sent_msg.id)

        await asyncio.to_thread(db.tasks.update_one, {"_id": task_id}, {"$set": {"status": "converted"}})
        logger.info(f"[WORKER] Task {task_id} finished and sent back.")

    except Exception as e:
        logger.error(f"[WORKER] Failed to process task {task_id}: {e}", exc_info=True)
        await asyncio.to_thread(db.tasks.update_one, {"_id": task_id}, {"$set": {"status": "failed", "error": str(e)}})
    finally:
        await cleanup_temp_files(files_to_clean)


# This handler is for the MAIN bot to RECEIVE completed files
@app.on_message(filters.text & main_bot_reply_filter & filters.reply & filters.regex("^done_"))
async def receive_converted_handler(client, message):
    task_id = message.text.split("done_")[-1].strip()
    if not task_id: return
    
    logger.info(f"[MAIN] Received converted task: {task_id}")
    
    # Get task data from DB
    task_data = await asyncio.to_thread(db.tasks.find_one, {"_id": task_id})
    if not task_data or task_data.get("status") == "uploading":
        logger.warning(f"[MAIN] Task {task_id} not found or already processing.")
        return

    await asyncio.to_thread(db.tasks.update_one, {"_id": task_id}, {"$set": {"status": "uploading"}})

    # Get user and message info
    user_id = task_data["user_id"]
    chat_id = task_data["chat_id"]
    processing_msg_id = task_data["processing_msg_id"]
    
    processing_msg = None
    try:
        processing_msg = await app.get_messages(chat_id, processing_msg_id)
    except Exception as e:
        logger.warning(f"[MAIN] Could not find original processing message for {task_id}. Will send new. Error: {e}")
    
    # Get media files from the worker's reply
    media_messages = []
    try:
        if message.reply_to_message_group_id:
            media_messages = await app.get_media_group(WORKER_CHANNEL_ID, message.reply_to_message_id)
        else:
            media_messages.append(message.reply_to_message)
    except FloodWait as e:
        logger.warning(f"[MAIN] FloodWait when getting media group for {task_id}. Sleeping for {e.value}s")
        await asyncio.sleep(e.value)
        await asyncio.to_thread(db.tasks.update_one, {"_id": task_id}, {"$set": {"status": "converted"}}) # Reset status
        return # Let it retry
    except Exception as e:
        logger.error(f"[MAIN] No media found for *converted* task {task_id}: {e}")
        await asyncio.to_thread(db.tasks.update_one, {"_id": task_id}, {"$set": {"status": "failed", "error": "Converted media not found"}})
        return

    files_to_clean = []
    downloaded_paths = []
    
    try:
        if processing_msg:
            await safe_edit_message(processing_msg, "✅ " + to_bold_sans("Processing complete. Downloading converted files..."))
        
        for media_msg in media_messages:
            path = await client.download_media(media_msg)
            downloaded_paths.append(path)
            files_to_clean.append(path)
        
        if processing_msg:
            await safe_edit_message(processing_msg, "🔑 " + to_bold_sans("Authenticating Session..."))
        
        # Get Instagram Client
        user_settings = await get_user_settings(user_id)
        active_username = user_settings.get("active_ig_username")
        if not active_username:
            raise LoginRequired("No active IG account set.")
        
        user_upload_client = await get_insta_client_for_user(user_id, active_username)
        
        # Get task details from DB
        final_caption = task_data["final_caption"]
        collab_username = task_data["collaborator_username"] # <-- Get Collab username
        is_premium = task_data["is_premium"]
        upload_type = task_data["upload_type"]
        
        # === FINAL UPLOAD (from Main Bot) ===
        url, media_id, media_type_value = "N/A", "N/A", "N/A"
        result = None
        
        if upload_type == "reel":
            path = downloaded_paths[0]
            if processing_msg: await safe_edit_message(processing_msg, "⬆️ " + to_bold_sans("Uploading To Instagram... Please Wait."))
            result = await asyncio.to_thread(user_upload_client.clip_upload, path, final_caption, location=None)
            url = f"https://instagram.com/reel/{result.code}"

        elif upload_type == "post":
            path = downloaded_paths[0]
            if processing_msg: await safe_edit_message(processing_msg, "⬆️ " + to_bold_sans("Uploading To Instagram... Please Wait."))
            result = await asyncio.to_thread(user_upload_client.photo_upload, path, final_caption, location=None)
            url = f"https://instagram.com/p/{result.code}"

        elif upload_type == "album":
            if processing_msg: await safe_edit_message(processing_msg, "⬆️ " + to_bold_sans("Uploading Album To Instagram... Please Wait."))
            result = await asyncio.to_thread(user_upload_client.album_upload, downloaded_paths, final_caption, location=None)
            url = f"https://instagram.com/p/{result.code}"

        elif upload_type == "story":
            path = downloaded_paths[0]
            uploader_func = user_upload_client.photo_upload_to_story
            if path.endswith((".mp4", ".mov")):
                uploader_func = user_upload_client.video_upload_to_story
            
            if processing_msg: await safe_edit_message(processing_msg, "⬆️ " + to_bold_sans("Uploading Story..."))
            result = await asyncio.to_thread(uploader_func, path)
            url = f"https://instagram.com/stories/{active_username}/{result.pk}"
        
        media_id, media_type_value = result.pk, result.media_type
        
        # --- COLLABORATOR LOGIC (for worker-processed files) ---
        if collab_username and upload_type in ["reel", "post", "album"]:
            try:
                if processing_msg:
                    await safe_edit_message(processing_msg, "🤝 " + to_bold_sans(f"Inviting @{collab_username} as collaborator..."))
                user_to_invite = await asyncio.to_thread(user_upload_client.user_info_by_username, collab_username)
                await asyncio.to_thread(user_upload_client.media_invite_collaborator, media_id, user_to_invite.pk)
                logger.info(f"[MAIN] Successfully invited {collab_username} to post {media_id}")
            except UserNotFound:
                logger.warning(f"[MAIN] Collaborator @{collab_username} not found. Skipping invite.")
            except Exception as e:
                logger.error(f"[MAIN] Failed to invite collaborator @{collab_username}: {e}")
        # --- END COLLABORATOR LOGIC ---
        
        # Log to DB
        await asyncio.to_thread(db.uploads.insert_one, {
            "user_id": user_id, "media_id": str(media_id), "media_type": str(media_type_value),
            "platform": "instagram", "upload_type": upload_type, "timestamp": datetime.utcnow(),
            "url": url, "caption": final_caption
        })
        
        log_msg = f"📤 New {upload_type.capitalize()} Upload (from worker)\n" \
                    f"👤 User: `{user_id}`\n🔗 URL: {url}\n📅 {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}"
        
        final_text = f"✅ " + to_bold_sans("Uploaded Successfully!") + f"\n\n{url}"
        if processing_msg:
            await safe_edit_message(processing_msg, final_text, parse_mode=None)
        else:
            await app.send_message(chat_id, final_text) # Send as new message if original was lost
            
        await send_log_to_channel(app, LOG_CHANNEL, log_msg)
        
        # Delete the task from DB
        await asyncio.to_thread(db.tasks.delete_one, {"_id": task_id})

    except Exception as e:
        logger.error(f"[MAIN] Failed to upload converted task {task_id}: {e}", exc_info=True)
        await asyncio.to_thread(db.tasks.update_one, {"_id": task_id}, {"$set": {"status": "upload_failed", "error": str(e)}})
        error_text = f"❌ " + to_bold_sans(f"Upload Failed After Conversion: {e}")
        if processing_msg:
            await safe_edit_message(processing_msg, error_text)
        else:
            await app.send_message(chat_id, error_text)
    finally:
        await cleanup_temp_files(files_to_clean)

async def send_log_to_channel(client, channel_id, text):
    global valid_log_channel
    if not valid_log_channel:
        return
    try:
        await client.send_message(channel_id, text, disable_web_page_preview=True, parse_mode=enums.ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Failed to log to channel {channel_id} (General Error): {e}")
        valid_log_channel = False

# ===================================================================
# ======================== BOT STARTUP ============================
# ===================================================================
async def start_bot():
    global mongo, db, global_settings, upload_semaphore, MAX_CONCURRENT_UPLOADS, MAX_FILE_SIZE_BYTES, task_tracker, valid_log_channel

    os.makedirs("sessions", exist_ok=True)
    logger.info("Session directories ensured.")

    try:
        mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo.admin.command('ping')
        db = mongo.NowTok
        logger.info("✅ Connected to MongoDB successfully.")
        
        settings_from_db = await asyncio.to_thread(db.settings.find_one, {"_id": "global_settings"}) or {}
        
        def merge_dicts(d1, d2):
            for k, v in d2.items():
                if k in d1 and isinstance(d1[k], dict) and isinstance(v, dict):
                    merge_dicts(d1[k], v)
                else:
                    d1[k] = v
        
        global_settings = DEFAULT_GLOBAL_SETTINGS.copy()
        merge_dicts(global_settings, settings_from_db)

        await asyncio.to_thread(db.settings.update_one, {"_id": "global_settings"}, {"$set": global_settings}, upsert=True)

        logger.info("Global settings loaded and synchronized.")
    except Exception as e:
        logger.critical(f"❌ DATABASE SETUP FAILED: {e}. Running in degraded mode.")
        db = None
        global_settings = DEFAULT_GLOBAL_SETTINGS

    MAX_CONCURRENT_UPLOADS = global_settings.get("max_concurrent_uploads")
    upload_semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
    MAX_FILE_SIZE_BYTES = global_settings.get("max_file_size_mb") * 1024 * 1024

    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    await app.start()
    
    task_tracker.loop = asyncio.get_running_loop()

    # === NEW STARTUP LOGIC ===
    if IS_WORKER_BOOL:
        logger.info(f"Bot starting in WORKER mode. Listening on channel {WORKER_CHANNEL_ID}")
        if LOG_CHANNEL:
            try:
                await app.send_message(LOG_CHANNEL, "🛠️ **Worker Bot is Online!**\nListening for conversion tasks...")
                valid_log_channel = True
            except Exception as e:
                logger.error(f"Could not log to channel {LOG_CHANNEL}. Invalid or bot isn't admin. Error: {e}")
                valid_log_channel = False
    else:
        logger.info("Bot starting in MAIN mode. Ready for users.")
        if LOG_CHANNEL:
            try:
                await app.send_message(LOG_CHANNEL, "✅ **" + to_bold_sans("Main Bot Is Now Online And Running!") + "**", parse_mode=enums.ParseMode.MARKDOWN)
                valid_log_channel = True
            except Exception as e:
                logger.error(f"Could not log to channel {LOG_CHANNEL}. Invalid or bot isn't admin. Error: {e}")
                valid_log_channel = False
    # === END NEW STARTUP LOGIC ===

    logger.info("Bot is now online! Waiting for tasks...")
    await idle()

    logger.info("Shutting down...")
    await task_tracker.cancel_and_wait_all()
    await app.stop()
    if mongo:
        mongo.close()
    logger.info("Bot has been shut down gracefully.")

if __name__ == "__main__":
    task_tracker = TaskTracker()
    try:
        app.run(start_bot())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.critical(f"Bot crashed during startup: {e}", exc_info=True)
    
