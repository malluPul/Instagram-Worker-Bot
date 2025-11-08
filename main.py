import os
import sys
import asyncio
import logging
import subprocess
import json
from datetime import datetime
from functools import wraps
import re
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

# Load environment variables
from dotenv import load_dotenv

load_dotenv()
# MongoDB
from pymongo import MongoClient
from pymongo.errors import OperationFailure
# Pyrogram (Telegram Bot)
from pyrogram import Client, filters, enums, idle
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import (
    InputMediaVideo,
    InputMediaPhoto
)
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
logger = logging.getLogger("BotWorker")

# === Load and Validate Environment Variables ===
API_ID_STR = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
LOG_CHANNEL_STR = os.getenv("LOG_CHANNEL_ID")
MONGO_URI = os.getenv("MONGO_DB")
ADMIN_ID_STR = os.getenv("ADMIN_ID")
WORKER_CHANNEL_ID_STR = os.getenv("WORKER_CHANNEL_ID")

# --- Validate required variables ---
if not all([API_ID_STR, API_HASH, BOT_TOKEN, MONGO_URI, WORKER_CHANNEL_ID_STR, ADMIN_ID_STR]):
    logger.critical(
        "FATAL ERROR (WORKER): Missing essential worker variables. Check API_ID, API_HASH, BOT_TOKEN, MONGO_URI, WORKER_CHANNEL_ID, ADMIN_ID.")
    sys.exit(1)
logger.info("Running in DEDICATED WORKER mode.")

# Convert to correct types after validation
API_ID = int(API_ID_STR)
ADMIN_ID = int(ADMIN_ID_STR)
LOG_CHANNEL = int(LOG_CHANNEL_STR) if LOG_CHANNEL_STR else None
WORKER_CHANNEL_ID = int(WORKER_CHANNEL_ID_STR)
IS_WORKER_BOOL = True

# === Video Conversion Helpers ===

def needs_conversion(input_file: str) -> bool:
    """
    Checks if a video file needs conversion (audio only).
    """
    try:
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

        # Check audio stream codec
        audio_codec = 'none'
        for stream in data.get('streams', []):
            if stream.get('codec_type') == 'audio':
                audio_codec = stream.get('codec_name')
                break

        is_compatible_audio = (audio_codec == 'aac')

        if is_compatible_audio:
            logger.info(f"'{input_file}' audio is already compatible (Audio: {audio_codec}). No conversion needed.")
            return False
        else:
            logger.warning(f"'{input_file}' needs audio conversion (Audio: {audio_codec}).")
            return True

    except Exception:
        logger.error(f"Could not probe file '{input_file}'. Assuming conversion is needed.")
        return True


def fix_for_instagram(input_file: str, output_file: str) -> str:
    """
    Converts a video file to an Instagram-compatible format by COPYING the video stream
    and re-encoding only the AUDIO. This is extremely fast and uses almost NO CPU.
    This PRESERVES 100% of the original video quality and prevents crashes on free servers.
    """
    try:
        logger.info(f"Starting FAST (Audio-Only) conversion for '{input_file}'...")
        command = [
            'ffmpeg',
            '-y',
            '-i', input_file,
            '-c:v', 'copy',          # 100% ഒറിജിനൽ വീഡിയോ ക്വാളിറ്റി (CPU ഉപയോഗം ഇല്ല)
            '-c:a', 'aac',          # ഓഡിയോ AAC ആക്കി മാറ്റുന്നു
            '-b:a', '192k',         # ഓഡിയോ ബിറ്റ്റേറ്റ് (192k മതിയാകും)
            '-ar', '48000',         # ഓഡിയോ സാമ്പിൾ റേറ്റ്
            '-pix_fmt', 'yuv420p',
            '-movflags', '+faststart',
            output_file
        ]

        result = subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info(f"Successfully converted video (audio only) to '{output_file}'.")
        return output_file

    except FileNotFoundError:
        logger.critical("ffmpeg is not installed or not found. Video conversion is not possible.")
        raise FileNotFoundError("ffmpeg is not installed. Cannot process video files.")
    except subprocess.CalledProcessError as e:
        logger.error(f"ffmpeg conversion failed for {input_file}. Error: {e.stderr}")
        # Fallback: If 'copy' fails (e.g., incompatible pixel format), try re-encoding with ultrafast preset
        logger.warning(f"Fallback: Trying re-encoding with 'ultrafast' for {input_file}...")
        try:
            command_fallback = [
                'ffmpeg',
                '-y',
                '-i', input_file,
                '-c:v', 'libx264',
                '-preset', 'ultrafast', # ഏറ്റവും വേഗതയേറിയതും CPU കുറഞ്ഞതുമായ എൻകോഡിംഗ്
                '-crf', '23',
                '-pix_fmt', 'yuv420p',
                '-c:a', 'aac',
                '-b:a', '192k',
                '-ar', '48000',
                '-movflags', '+faststart',
                output_file
            ]
            result = subprocess.run(command_fallback, check=True, capture_output=True, text=True)
            logger.info(f"Successfully converted video (Fallback) to '{output_file}'.")
            return output_file
        except Exception as fallback_e:
            logger.error(f"ffmpeg fallback conversion also failed for {input_file}. Error: {fallback_e}")
            raise ValueError(f"Video format is incompatible and conversion failed. Error: {e.stderr}")


# --- Global State & DB Management ---
mongo = None
db = None
shutdown_event = asyncio.Event()
valid_log_channel = False

# Pyrogram Client
app = Client("upload_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# === NEW: Admin Progress Monitoring ===
admin_progress_messages = {}  # Stores {task_id: message_id}


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
# ====================== HELPER FUNCTIONS ===========================
# ===================================================================

def is_admin(user_id):
    return user_id == ADMIN_ID


async def send_admin_status(task_id: str, text: str, edit: bool = False):
    """Sends or edits a progress message to the ADMIN_ID."""
    if not ADMIN_ID:
        return

    try:
        if edit and task_id in admin_progress_messages:
            msg_id = admin_progress_messages[task_id]
            await app.edit_message_text(ADMIN_ID, msg_id, text)
        else:
            msg = await app.send_message(ADMIN_ID, text)
            admin_progress_messages[task_id] = msg.id
    except FloodWait as e:
        logger.warning(f"FloodWait when sending admin status: {e.value}s")
        await asyncio.sleep(e.value)
    except MessageNotModified:
        pass # Ignore if the message is the same
    except Exception as e:
        logger.error(f"Failed to send admin status for task {task_id}: {e}")


async def admin_progress_callback(current, total, task_id, status_text):
    """Callback function for admin progress updates."""
    try:
        percentage = int(current * 100 / total)
        if percentage % 10 == 0 or current == total or current == 0:
            await send_admin_status(
                task_id,
                f"{status_text}\n`[{'█' * int(percentage / 5)}{' ' * (20 - int(percentage / 5))}] {percentage}%`",
                edit=True
            )
    except Exception:
        pass


async def update_conversion_heartbeat(task_id, admin_status_text, stop_event: asyncio.Event):
    """Updates the admin message with a spinner to show activity."""
    spinners = ["⢿", "⣻", "⣽", "⣾", "⣷", "⣯", "⣷", "⣾", "⣽", "⣻"]
    i = 0
    while not stop_event.is_set():
        try:
            spinner = spinners[i % len(spinners)]
            await send_admin_status(
                task_id,
                f"{admin_status_text} {spinner}",
                edit=True
            )
            i += 1
            await asyncio.sleep(2)  # Update spinner every 2 seconds
        except asyncio.CancelledError:
            break
        except Exception:
            break


@app.on_message(filters.command("restart") & filters.user(ADMIN_ID))
async def restart_worker_cmd(_, msg):
    """Gracefully restarts the worker bot."""
    await msg.reply("🛠 **Worker Bot Restarting...**")
    logger.info(f"Admin {msg.from_user.id} initiated worker restart.")
    
    # Send final log to admin DM
    await send_admin_status(
        "WORKER_RESTART",
        f"🛠 **Worker Restart Initiated**\nby Admin: `{msg.from_user.id}`\nThe bot will now shut down.",
        edit=False
    )

    sys.exit(0)


async def schedule_cleanup(files_to_clean, task_id, delay_seconds=300):
    """Waits for a delay and then cleans up files."""
    await asyncio.sleep(delay_seconds)
    logger.info(f"[CLEANUP] Cleaning up {len(files_to_clean)} files for task {task_id}.")
    await cleanup_temp_files(files_to_clean)

    if task_id in admin_progress_messages:
        try:
            await app.delete_messages(ADMIN_ID, admin_progress_messages[task_id])
            del admin_progress_messages[task_id]
        except Exception as e:
            logger.warning(f"[CLEANUP] Could not delete admin message for task {task_id}: {e}")


async def cleanup_temp_files(files_to_delete):
    for file_path in files_to_delete:
        if file_path:
            try:
                if await asyncio.to_thread(os.path.exists, file_path):
                    await asyncio.to_thread(os.remove, file_path)
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {e}")

# === HTTP Server for Health Checks (RE-ADDED) ===
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


# This handler is for the WORKER bot to RECEIVE tasks
@app.on_message(filters.text & worker_task_filter & filters.reply)
async def receive_task_handler(client, message):
    task_id = message.text.strip()
    if not task_id or task_id.startswith("done_"):
        return

    replied_msg = message.reply_to_message
    if not replied_msg:
        logger.error(f"[WORKK] Task ID {task_id} is not a reply.")
        return

    logger.info(f"[WORKER] Received task: {task_id}")

    media_messages = []
    try:
        if replied_msg.media_group_id:
            logger.info(f"[WORKER] Task {task_id} is an album.")
            media_messages = await app.get_media_group(WORKER_CHANNEL_ID, replied_msg.id)
        else:
            logger.info(f"[WORKER] Task {task_id} is a single file.")
            media_messages.append(replied_msg)
    except FloodWait as e:
        logger.warning(f"[WORKER] FloodWait: {e.value}s")
        await asyncio.sleep(e.value)
        return
    except Exception as e:
        logger.error(f"[WORKER] Error getting media: {e}")
        return

    if not media_messages:
        logger.error(f"[WORKER] No media found for task {task_id}")
        return

    task_data = await asyncio.to_thread(db.tasks.find_one, {"_id": task_id})
    if not task_data:
        logger.error(f"[WORKER] No DB entry found for task {task_id}")
        return

    if task_data.get("status") != "pending_conversion":
        logger.warning(f"[WORKER] Task {task_id} not pending (Status: {task_data.get('status')}). Skipping.")
        return
    
    user_id = task_data.get("user_id", "Unknown")

    await send_admin_status(task_id, f"👤 **New Task Started**\n**Task ID:** `{task_id}`\n**User:** `{user_id}`\n**Status:** 📥 Downloading...")

    await asyncio.to_thread(db.tasks.update_one, {"_id": task_id}, {"$set": {"status": "converting"}})

    converted_paths = []
    files_to_clean = []
    
    heartbeat_task = None
    stop_heartbeat = asyncio.Event()

    try:
        for i, media_msg in enumerate(media_messages):
            is_video_file = media_msg.video or (media_msg.document and 'video' in media_msg.document.mime_type)

            status_msg = f"Downloading file {i + 1}/{len(media_messages)} for task {task_id}..."
            logger.info(f"[WORKER] {status_msg}")

            admin_status_text = f"👤 **Task:** `{task_id}`\n**User:** `{user_id}`\n**Status:** 📥 Downloading file {i+1}/{len(media_messages)}..."
            
            download_path = await client.download_media(
                media_msg,
                progress=admin_progress_callback,
                progress_args=(task_id, admin_status_text)
            )
            files_to_clean.append(download_path)

            # === MODIFICATION: Check if audio conversion is needed ===
            if is_video_file and await asyncio.to_thread(needs_conversion, download_path):
                
                admin_status_text = f"👤 **Task:** `{task_id}`\n**User:** `{user_id}`\n**Status:** ⚙️ Converting (Fast audio)..."
                await send_admin_status(task_id, admin_status_text, edit=True)
                
                stop_heartbeat.clear()
                heartbeat_task = task_tracker.create_task(
                    update_conversion_heartbeat(task_id, admin_status_text, stop_heartbeat)
                )

                logger.info(f"[WORKER] File is a video. Sending to FAST (Audio) conversion: {download_path}")
                fixed_path = download_path.rsplit(".", 1)[0] + "_fixed.mp4"
                
                converted_path = await asyncio.to_thread(fix_for_instagram, download_path, fixed_path)
                
                stop_heartbeat.set()
                
                converted_paths.append(converted_path)
                files_to_clean.append(converted_path)
            else:
                # It's a photo or a compatible video
                logger.info(f"[WORKER] File is a photo or compatible video. No conversion needed.")
                converted_paths.append(download_path)

        stop_heartbeat.set()

        logger.info(f"[WORKER] Conversion complete. Uploading {len(converted_paths)} files back.")
        await send_admin_status(task_id,
                                f"👤 **Task:** `{task_id}`\n**User:** `{user_id}`\n**Status:** 📤 Uploading back to channel...",
                                edit=True)

        if len(converted_paths) > 1:
            media_group = []
            for path in converted_paths:
                if path.endswith((".mp4", ".mov", ".mkv")):
                    media_group.append(InputMediaVideo(path))
                else:
                    media_group.append(InputMediaPhoto(path))
            sent_msgs = await app.send_media_group(WORKER_CHANNEL_ID, media_group)
            await app.send_message(WORKER_CHANNEL_ID, f"done_{task_id}", reply_to_message_id=sent_msgs[-1].id)
        else:
            path = converted_paths[0]
            sent_msg = None
            if path.endswith((".mp4", ".mov", ".mkv")):
                sent_msg = await app.send_video(WORKER_CHANNEL_ID, path)
            else:
                sent_msg = await app.send_photo(WORKER_CHANNEL_ID, path)
            await app.send_message(WORKER_CHANNEL_ID, f"done_{task_id}", reply_to_message_id=sent_msg.id)

        await asyncio.to_thread(db.tasks.update_one, {"_id": task_id}, {"$set": {"status": "converted"}})
        logger.info(f"[WORKER] Task {task_id} finished and sent back.")
        await send_admin_status(task_id,
                                f"✅ **Task Complete**\n**Task ID:** `{task_id}`\n**User:** `{user_id}`\n**Status:** ✔️ Finished.",
                                edit=True)

    except Exception as e:
        logger.error(f"[WORKER] Failed to process task {task_id}: {e}", exc_info=True)
        await asyncio.to_thread(db.tasks.update_one, {"_id": task_id}, {"$set": {"status": "failed", "error": str(e)}})
        await send_admin_status(task_id,
                                f"❌ **Task Failed**\n**Task ID:** `{task_id}`\n**User:** `{user_id}`\n**Error:** `{e}`",
                                edit=True)

    finally:
        stop_heartbeat.set()
        if heartbeat_task:
            try: await asyncio.wait_for(heartbeat_task, timeout=0.1)
            except Exception: pass
        
        task_tracker.create_task(schedule_cleanup(files_to_clean, task_id, delay_seconds=300))
        logger.info(f"[WORKVER] Task {task_id} processing finished. Cleanup scheduled.")


async def send_log_to_channel(client, channel_id, text):
    global valid_log_channel
    if not valid_log_channel:
        return
    try:
        await client.send_message(channel_id, text, disable_web_page_preview=True, parse_mode=enums.ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Failed to log to channel {channel_id}: {e}")
        valid_log_channel = False


# ===================================================================
# ======================== BOT STARTUP ============================
# ===================================================================
async def start_bot():
    global mongo, db, task_tracker, valid_log_channel

    os.makedirs("sessions", exist_ok=True)
    logger.info("Session directories ensured.")

    try:
        mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo.admin.command('ping')
        db = mongo.NowTok
        logger.info("✅ Connected to MongoDB successfully.")
    except Exception as e:
        logger.critical(f"❌ DATABASE SETUP FAILED: {e}. Worker cannot function without DB.")
        db = None
        sys.exit(1)

    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()

    await app.start()

    task_tracker.loop = asyncio.get_running_loop()

    logger.info(f"Bot starting in WORKER mode. Listening on channel {WORKER_CHANNEL_ID}")
    
    # Send log to channel
    if LOG_CHANNEL:
        try:
            await app.send_message(LOG_CHANNEL, "🛠️ **Worker Bot is Online!**\nListening for conversion tasks...")
            valid_log_channel = True
        except Exception as e:
            logger.error(f"Could not log to channel {LOG_CHANNEL}: {e}")
            valid_log_channel = False
    
    # === NEW FEATURE: Send DM to ADMIN on startup ===
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        await app.send_message(ADMIN_ID, f"✅ **Worker Bot is ONLINE!**\nStarted/Restarted at: `{timestamp}`")
        logger.info(f"Sent startup notification to ADMIN ({ADMIN_ID})")
    except Exception as e:
        logger.error(f"Could not send startup DM to ADMIN ({ADMIN_ID}): {e}")
    # === END NEW FEATURE ===

    logger.info("Worker Bot is now online! Waiting for tasks...")
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
