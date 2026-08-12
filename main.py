#!/usr/bin/env python3
# ===================================================================
#  Instagram Compressing Worker Bot  —  v2.0
#  Rewritten with: dynamic Mongo-backed config, InstaData DB schema,
#  smart per-stream FFmpeg logic, hardware-accel fallback chain,
#  adaptive CRF, retry + deep error diagnostics, inline admin panel,
#  strict FIFO queue with immediate cleanup, and a watermark engine.
# ===================================================================
#
# NOTE ON DB ASSUMPTIONS
# -----------------------------------------------------------------
# The exact schema of the Main Bot was not provided to me, only the
# database name ("InstaData"), the task_id format
# ("7967568506_9759" -> "<user_id>_<random>") and the status values
# you listed ("pending_conversion" is inferred as the initial state
# because your original code checked for it; "converting",
# "converted", "failed" were given explicitly). I kept the
# collection name `tasks` from your original code since nothing
# contradicted it, and I added a `settings` collection for the
# dynamic config. If your Main Bot uses different collection/field
# names, they are ALL isolated in the "DATABASE LAYER" section below
# — change them there and nothing else needs to move.
# ===================================================================

import os
import sys
import re
import json
import time
import shutil
import asyncio
import logging
import subprocess
import threading
import uuid
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional, Dict, Any, List, Tuple

# --- Environment bootstrap -----------------------------------------
from dotenv import load_dotenv
load_dotenv()

# --- MongoDB ---------------------------------------------------------
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import PyMongoError

# --- Pyrogram (Telegram) ---------------------------------------------
from pyrogram import Client, filters, enums, idle
from pyrogram.errors import FloodWait, MessageNotModified, RPCError
from pyrogram.types import (
    InputMediaVideo,
    InputMediaPhoto,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    Message,
)

# --- System utilities --------------------------------------------------
import psutil

# ===================================================================
# ============================ LOGGING ==============================
# ===================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log"),
    ],
)
logger = logging.getLogger("BotWorker")

# ===================================================================
# ================ BOOTSTRAP ENV VARIABLES (MINIMAL SET) ============
# ===================================================================
# Only the credentials required to physically connect to Telegram and
# MongoDB live in the environment. Everything else (admin ids, worker
# channel, log channel, ffmpeg behaviour, watermark, retry counts...)
# is stored in MongoDB and can be changed live with bot commands, so
# losing the host's env vars again can never brick the bot's config.
API_ID_STR = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_DB")

# These are OPTIONAL bootstrap seeds. They are only used ONCE, to
# pre-populate the `settings` document the very first time the bot
# runs against a fresh database. After that, the database always wins.
SEED_ADMIN_ID = os.getenv("ADMIN_ID")
SEED_WORKER_CHANNEL_ID = os.getenv("WORKER_CHANNEL_ID")
SEED_LOG_CHANNEL_ID = os.getenv("LOG_CHANNEL_ID")

if not all([API_ID_STR, API_HASH, BOT_TOKEN, MONGO_URI]):
    logger.critical(
        "FATAL ERROR (WORKER): Missing essential bootstrap variables. "
        "TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_BOT_TOKEN and "
        "MONGO_DB are required to even reach the database."
    )
    sys.exit(1)

API_ID = int(API_ID_STR)
WORKER_INSTANCE_ID = uuid.uuid4().hex[:8]  # identifies this process for locking/logs
logger.info(f"Running in DEDICATED WORKER mode. Instance ID: {WORKER_INSTANCE_ID}")

# ===================================================================
# ========================= DATABASE LAYER ===========================
# ===================================================================
# Everything schema-specific lives here. If the Main Bot's real field
# names differ, this is the only place you should need to touch.

DB_NAME = "InstaData"
TASKS_COLLECTION = "tasks"
SETTINGS_COLLECTION = "settings"
SETTINGS_DOC_ID = "worker_config"

STATUS_PENDING = "pending_conversion"
STATUS_CONVERTING = "converting"
STATUS_CONVERTED = "converted"
STATUS_FAILED = "failed"

mongo: Optional[MongoClient] = None
db = None


def db_get_task(task_id: str) -> Optional[dict]:
    return db[TASKS_COLLECTION].find_one({"_id": task_id})


def db_claim_task(task_id: str) -> Optional[dict]:
    """
    Atomically claims a task: only succeeds if the task is still
    'pending_conversion'. This makes it safe to run more than one
    worker instance against the same database — two workers can never
    both grab the same task, since Mongo's find_one_and_update is
    atomic at the document level. Returns the ORIGINAL document if the
    claim succeeded, or None if someone/something already claimed it.
    """
    return db[TASKS_COLLECTION].find_one_and_update(
        {"_id": task_id, "status": STATUS_PENDING},
        {
            "$set": {
                "status": STATUS_CONVERTING,
                "worker_id": WORKER_INSTANCE_ID,
                "started_at": datetime.now(timezone.utc),
            }
        },
        return_document=ReturnDocument.BEFORE,
    )


def db_update_task(task_id: str, **fields):
    fields["updated_at"] = datetime.now(timezone.utc)
    db[TASKS_COLLECTION].update_one({"_id": task_id}, {"$set": fields})


def db_find_stuck_tasks(older_than_minutes: int = 10) -> List[dict]:
    """Tasks left in 'converting' from a previous process that crashed/restarted."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=older_than_minutes)
    return list(
        db[TASKS_COLLECTION].find(
            {"status": STATUS_CONVERTING, "started_at": {"$lt": cutoff}}
        )
    )


def db_task_stats() -> Dict[str, int]:
    stats = {}
    for status in (STATUS_PENDING, STATUS_CONVERTING, STATUS_CONVERTED, STATUS_FAILED):
        stats[status] = db[TASKS_COLLECTION].count_documents({"status": status})
    return stats


# ===================================================================
# ===================== DYNAMIC CONFIG (MongoDB) =====================
# ===================================================================

DEFAULT_CONFIG = {
    "_id": SETTINGS_DOC_ID,
    "admin_ids": [],
    "worker_channel_id": None,
    "log_channel_id": None,
    "max_retries": 3,
    "cleanup_immediately": True,
    "ffmpeg": {
        "hwaccel_enabled": True,
        "preset": "medium",
        "base_crf": 20,
        "adaptive_crf": True,
        "audio_bitrate": "192k",
        "audio_sample_rate": "48000",
    },
    "watermark": {
        "enabled": False,
        "type": "text",          # "text" or "image"
        "text": "",
        "image_path": "",
        "position": "bottom_right",  # top_left, top_right, bottom_left, bottom_right, center
        "opacity": 0.5,
        "scale": 0.15,            # relative to video width (image watermark only)
        "margin": 20,
    },
}


class ConfigManager:
    """
    Thin async-safe wrapper around a single MongoDB document that holds
    every tunable setting for the bot. Values are cached in memory for
    speed and refreshed on every write, so reads never hit the network.
    """

    def __init__(self):
        self._data: Dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def load(self):
        doc = await asyncio.to_thread(
            db[SETTINGS_COLLECTION].find_one, {"_id": SETTINGS_DOC_ID}
        )
        if doc is None:
            logger.info("No settings document found — seeding defaults into MongoDB.")
            doc = json.loads(json.dumps(DEFAULT_CONFIG))  # deep copy, safe for Mongo
            # Apply one-time bootstrap seeds from the environment, if present.
            if SEED_ADMIN_ID:
                doc["admin_ids"] = [int(x) for x in re.split(r"[,\s]+", SEED_ADMIN_ID) if x]
            if SEED_WORKER_CHANNEL_ID:
                doc["worker_channel_id"] = int(SEED_WORKER_CHANNEL_ID)
            if SEED_LOG_CHANNEL_ID:
                doc["log_channel_id"] = int(SEED_LOG_CHANNEL_ID)
            await asyncio.to_thread(db[SETTINGS_COLLECTION].insert_one, doc)
        else:
            # Merge in any NEW default keys that may not exist yet in an
            # older settings document (safe upgrade path across versions).
            doc = self._deep_merge_defaults(doc, DEFAULT_CONFIG)
        self._data = doc
        logger.info("Configuration loaded from MongoDB.")

    @staticmethod
    def _deep_merge_defaults(existing: dict, defaults: dict) -> dict:
        for key, value in defaults.items():
            if key not in existing:
                existing[key] = value
            elif isinstance(value, dict) and isinstance(existing.get(key), dict):
                existing[key] = ConfigManager._deep_merge_defaults(existing[key], value)
        return existing

    def get(self, dotted_key: str, default=None):
        node = self._data
        for part in dotted_key.split("."):
            if isinstance(node, dict) and part in node:
                node = node[part]
            else:
                return default
        return node

    async def set(self, dotted_key: str, value: Any):
        async with self._lock:
            parts = dotted_key.split(".")
            node = self._data
            for part in parts[:-1]:
                node = node.setdefault(part, {})
            node[parts[-1]] = value
            await asyncio.to_thread(
                db[SETTINGS_COLLECTION].update_one,
                {"_id": SETTINGS_DOC_ID},
                {"$set": {dotted_key: value}},
            )
        logger.info(f"Config updated: {dotted_key} = {value}")

    @property
    def admin_ids(self) -> List[int]:
        return self.get("admin_ids", []) or []

    @property
    def worker_channel_id(self) -> Optional[int]:
        v = self.get("worker_channel_id")
        return int(v) if v is not None else None

    @property
    def log_channel_id(self) -> Optional[int]:
        v = self.get("log_channel_id")
        return int(v) if v is not None else None

    def as_pretty_json(self) -> str:
        safe = json.loads(json.dumps(self._data, default=str))
        return json.dumps(safe, indent=2, ensure_ascii=False)


config = ConfigManager()


def is_admin(user_id: int) -> bool:
    return user_id in config.admin_ids


def admin_filter(_, __, message: Message):
    return bool(message.from_user) and is_admin(message.from_user.id)


admin_only = filters.create(admin_filter)


def worker_channel_filter(_, __, message: Message):
    return message.chat.id == config.worker_channel_id


worker_task_filter = filters.create(worker_channel_filter)

# ===================================================================
# ====================== FFPROBE / MEDIA ANALYSIS ====================
# ===================================================================

# Pixel formats that indicate more than 8 bits per channel.
_TENBIT_PIXFMT_HINTS = ("10le", "10be", "12le", "12be", "p010", "yuv420p10")


def probe_media(path: str) -> Optional[dict]:
    """Runs ffprobe once and returns a normalized dict describing the
    file's video and audio streams, or None if ffprobe fails outright."""
    try:
        command = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_format", "-show_streams", path,
        ]
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding="utf-8")
        data = json.loads(result.stdout)
    except Exception as e:
        logger.error(f"ffprobe failed for '{path}': {e}")
        return None

    info = {
        "duration": float(data.get("format", {}).get("duration", 0) or 0),
        "size_bytes": int(data.get("format", {}).get("size", 0) or 0),
        "video": None,
        "audio": None,
    }
    for stream in data.get("streams", []):
        if stream.get("codec_type") == "video" and info["video"] is None:
            info["video"] = {
                "codec_name": stream.get("codec_name"),
                "pix_fmt": stream.get("pix_fmt", "") or "",
                "bits_per_raw_sample": stream.get("bits_per_raw_sample"),
                "width": stream.get("width"),
                "height": stream.get("height"),
                "bit_rate": int(stream.get("bit_rate", 0) or 0),
            }
        elif stream.get("codec_type") == "audio" and info["audio"] is None:
            info["audio"] = {
                "codec_name": stream.get("codec_name"),
                "sample_rate": stream.get("sample_rate"),
                "bit_rate": int(stream.get("bit_rate", 0) or 0),
            }
    return info


def is_10bit(video_stream: dict) -> bool:
    if not video_stream:
        return False
    pix_fmt = (video_stream.get("pix_fmt") or "").lower()
    if any(hint in pix_fmt for hint in _TENBIT_PIXFMT_HINTS):
        return True
    bprs = video_stream.get("bits_per_raw_sample")
    try:
        return bprs is not None and int(bprs) > 8
    except (TypeError, ValueError):
        return False


def video_stream_is_compatible(video_stream: dict) -> bool:
    """Video is left untouched only if it's already H.264, 8-bit, yuv420p."""
    if not video_stream:
        return True  # no video stream (e.g. audio-only) -> nothing to fix
    if is_10bit(video_stream):
        return False
    if (video_stream.get("codec_name") or "").lower() != "h264":
        return False
    if (video_stream.get("pix_fmt") or "").lower() != "yuv420p":
        return False
    return True


def audio_stream_is_compatible(audio_stream: dict) -> bool:
    if not audio_stream:
        return True  # no audio track -> nothing to fix
    return (audio_stream.get("codec_name") or "").lower() == "aac"


# ===================================================================
# ============ NEW FEATURE #1: HARDWARE-ACCEL FALLBACK CHAIN =========
# ===================================================================
# Why: software libx264 encoding is the slowest and most CPU-hungry
# path. Most VPS/cloud hosts (and some free tiers) expose an NVENC,
# QSV or VAAPI encoder. Probing FFmpeg's own compiled encoder list
# ONCE at startup lets every subsequent conversion transparently use
# the fastest encoder the host actually supports, cutting compression
# time (and CPU cost) dramatically without any manual configuration —
# and if a hardware attempt fails at runtime (e.g. no GPU device node
# is actually present, which -encoders can't detect), the retry logic
# in `fix_for_instagram_v2` automatically falls back to libx264.

_HWACCEL_ENCODER_CHAIN = [
    ("h264_nvenc", "NVIDIA NVENC"),
    ("h264_qsv", "Intel QuickSync"),
    ("h264_vaapi", "VAAPI"),
]
_available_hw_encoders: List[Tuple[str, str]] = []


def detect_hwaccel_encoders():
    """Populates _available_hw_encoders by checking `ffmpeg -encoders` once."""
    global _available_hw_encoders
    try:
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            check=True, capture_output=True, text=True,
        )
        output = result.stdout
        _available_hw_encoders = [
            (name, label) for name, label in _HWACCEL_ENCODER_CHAIN if name in output
        ]
        if _available_hw_encoders:
            logger.info(
                "Hardware encoders detected: "
                + ", ".join(label for _, label in _available_hw_encoders)
            )
        else:
            logger.info("No hardware encoders detected. Will use libx264 (software).")
    except Exception as e:
        logger.warning(f"Could not probe FFmpeg encoders ({e}). Defaulting to software encoding.")
        _available_hw_encoders = []


def encoder_chain_for_attempt(attempt: int, hwaccel_enabled: bool) -> List[str]:
    """
    Returns an ordered list of video encoders to try for this attempt.
    Attempt 1 (if hwaccel enabled): [best hw encoder] -> libx264 as safety net.
    Attempt 2+: force libx264 only — hardware issues (driver/device not
    actually present) are the most likely cause of a first failure, so
    we stop wasting retries on it.
    """
    if attempt == 1 and hwaccel_enabled and _available_hw_encoders:
        return [_available_hw_encoders[0][0], "libx264"]
    return ["libx264"]


# ===================================================================
# ======== NEW FEATURE #2: CONTENT-AWARE ADAPTIVE CRF/BITRATE ========
# ===================================================================
# Why: a single fixed CRF is wrong for a huge range of source content.
# A screen-recording at 1080p30 compresses beautifully at a high CRF,
# but a busy, high-bitrate 4K action clip will visibly degrade at that
# same CRF. Instead of guessing, we look at the SOURCE's own bitrate
# and resolution and pick a CRF that preserves it proportionally —
# giving consistently good quality across very different inputs while
# never over-spending bits on simple content (which would blow past
# Instagram's ~50MB budget for no visual benefit).

def compute_adaptive_crf(video_stream: dict, duration: float, size_bytes: int, base_crf: int) -> int:
    if not video_stream or not duration:
        return base_crf

    width = video_stream.get("width") or 1280
    height = video_stream.get("height") or 720
    pixels = width * height

    # Prefer the stream's own reported bitrate; fall back to a
    # container-level estimate (bytes*8 / duration) if unavailable.
    source_kbps = video_stream.get("bit_rate", 0) / 1000
    if source_kbps <= 0 and size_bytes and duration:
        source_kbps = (size_bytes * 8 / 1000) / duration

    # Bits-per-pixel is a resolution-independent measure of source
    # complexity/quality — a good proxy for "how much detail is here".
    bpp = (source_kbps * 1000) / pixels if pixels else 0

    if bpp >= 0.12:        # high-bitrate / high-detail source
        crf = base_crf - 3
    elif bpp >= 0.06:      # typical phone-camera footage
        crf = base_crf
    else:                  # already heavily compressed / simple content
        crf = base_crf + 3

    return max(16, min(28, crf))  # keep CRF within a sane visual-quality band


# ===================================================================
# ================= WATERMARK ENGINE (disabled by default) ===========
# ===================================================================

_POSITION_EXPR = {
    "top_left": ("{margin}", "{margin}"),
    "top_right": ("W-w-{margin}", "{margin}"),
    "bottom_left": ("{margin}", "H-h-{margin}"),
    "bottom_right": ("W-w-{margin}", "H-h-{margin}"),
    "center": ("(W-w)/2", "(H-h)/2"),
}


def build_watermark_filter(wm_config: dict) -> Optional[str]:
    """
    Builds an ffmpeg -vf/-filter_complex fragment for the watermark,
    or None if disabled / misconfigured. Supports a text watermark
    (drawtext) or an image watermark (overlay), with position, size
    and opacity all driven by the `watermark` block in MongoDB config.
    """
    if not wm_config.get("enabled"):
        return None

    margin = int(wm_config.get("margin", 20))
    x_expr, y_expr = _POSITION_EXPR.get(wm_config.get("position", "bottom_right"), _POSITION_EXPR["bottom_right"])
    x_expr = x_expr.format(margin=margin)
    y_expr = y_expr.format(margin=margin)
    opacity = float(wm_config.get("opacity", 0.5))
    opacity = max(0.0, min(1.0, opacity))

    if wm_config.get("type") == "image" and wm_config.get("image_path") and os.path.exists(wm_config["image_path"]):
        scale = float(wm_config.get("scale", 0.15))
        # scale2ref keeps the watermark proportional to the MAIN video's width.
        return (
            f"movie={wm_config['image_path']}[wm];"
            f"[wm]format=rgba,colorchannelmixer=aa={opacity}[wm2];"
            f"[in][wm2]scale2ref=w=main_w*{scale}:h=ow/mdar[base][wm3];"
            f"[base][wm3]overlay=x={x_expr}:y={y_expr}[out]"
        )

    text = (wm_config.get("text") or "").strip()
    if not text:
        return None
    escaped = text.replace(":", "\\:").replace("'", "\\'")
    return (
        f"drawtext=text='{escaped}':fontcolor=white@{opacity}:fontsize=h*0.045:"
        f"box=1:boxcolor=black@{max(0.0, opacity - 0.2)}:boxborderw=8:"
        f"x={x_expr.replace('w-w', 'text_w').replace('W-w', 'w-text_w-0')}:"
        f"y={y_expr.replace('h-h', 'text_h').replace('H-h', 'h-text_h-0')}"
    )


# ===================================================================
# ==================== FFMPEG ERROR DIAGNOSTICS =======================
# ===================================================================

_ERROR_PATTERNS = [
    (r"no space left on device", "Server ran out of disk space during processing."),
    (r"unknown encoder", "Selected encoder is unavailable on this server (hardware acceleration codec not really present)."),
    (r"invalid data found when processing input", "Source file is corrupted or in an unsupported/unreadable format."),
    (r"moov atom not found", "Source video file is incomplete or was truncated during download."),
    (r"video too big|dimensions too large", "Video resolution exceeds what the encoder can handle."),
    (r"could not open encoder", "Failed to initialize the selected video encoder (driver/library issue)."),
    (r"conversion failed", "General FFmpeg conversion failure — see raw log for details."),
    (r"permission denied", "Worker process lacks filesystem permission to read/write the media file."),
]


def diagnose_ffmpeg_error(stderr_text: str) -> str:
    lowered = (stderr_text or "").lower()
    for pattern, human_reason in _ERROR_PATTERNS:
        if re.search(pattern, lowered):
            return human_reason
    snippet = (stderr_text or "").strip().splitlines()[-1:] or ["Unknown error"]
    return f"Unrecognized FFmpeg error: {snippet[0][:200]}"


# ===================================================================
# =============== CORE CONVERSION: SMART + RETRY + FALLBACK ==========
# ===================================================================

def _run_ffmpeg(command: List[str], timeout: int = 600) -> Tuple[bool, str]:
    """Blocking helper (always called through asyncio.to_thread)."""
    try:
        result = subprocess.run(
            command, check=True, capture_output=True, text=True, timeout=timeout
        )
        return True, result.stderr
    except subprocess.TimeoutExpired:
        return False, "FFmpeg process timed out"
    except subprocess.CalledProcessError as e:
        return False, e.stderr or str(e)
    except FileNotFoundError:
        return False, "ffmpeg binary not found on this system"


def _build_ffmpeg_command(
    input_file: str,
    output_file: str,
    video_ok: bool,
    audio_ok: bool,
    video_encoder: str,
    crf: int,
    preset: str,
    audio_bitrate: str,
    audio_sample_rate: str,
    watermark_filter: Optional[str],
    force_full_reencode: bool = False,
) -> List[str]:
    cmd = ["ffmpeg", "-y", "-i", input_file]

    encode_video = force_full_reencode or not video_ok or watermark_filter is not None
    encode_audio = force_full_reencode or not audio_ok

    if encode_video:
        cmd += ["-c:v", video_encoder]
        if video_encoder == "libx264":
            cmd += ["-preset", preset, "-crf", str(crf)]
        elif video_encoder == "h264_nvenc":
            cmd += ["-preset", "p5", "-cq", str(crf)]
        elif video_encoder in ("h264_qsv", "h264_vaapi"):
            cmd += ["-global_quality", str(crf)]
        cmd += ["-pix_fmt", "yuv420p"]
        if watermark_filter:
            if watermark_filter.startswith("movie="):
                cmd += ["-filter_complex", watermark_filter, "-map", "[out]"]
            else:
                cmd += ["-vf", watermark_filter]
    else:
        cmd += ["-c:v", "copy"]

    if encode_audio:
        cmd += ["-c:a", "aac", "-b:a", audio_bitrate, "-ar", audio_sample_rate]
    else:
        cmd += ["-c:a", "copy"]

    cmd += ["-movflags", "+faststart", output_file]
    return cmd


async def fix_for_instagram_v2(
    input_file: str,
    output_file: str,
    task_id: str,
    attempt: int,
) -> Tuple[str, dict]:
    """
    One conversion ATTEMPT. Raises RuntimeError(human_reason) on failure
    so the retry orchestrator below can decide what to try next. Returns
    (output_path, probe_info_used) on success.
    """
    probe = await asyncio.to_thread(probe_media, input_file)
    if probe is None:
        raise RuntimeError("Could not analyze the source file with ffprobe (file may be corrupted).")

    video_stream = probe["video"]
    audio_stream = probe["audio"]

    # From attempt 2 onward we stop trusting "safe copy" for video — if
    # the first smart attempt failed, force a real re-encode instead of
    # trying the same copy path twice.
    force_full = attempt >= 2
    video_ok = video_stream_is_compatible(video_stream) and not force_full
    audio_ok = audio_stream_is_compatible(audio_stream) and not force_full

    hwaccel_enabled = config.get("ffmpeg.hwaccel_enabled", True) and attempt == 1
    base_crf = int(config.get("ffmpeg.base_crf", 20))
    if attempt >= 3:
        # Last-resort attempt: prioritize SUCCESS over perfect quality,
        # use a fast safe preset and skip the watermark so nothing extra
        # can go wrong.
        preset = "ultrafast"
        crf = max(base_crf, 23)
        watermark_filter = None
    else:
        preset = config.get("ffmpeg.preset", "medium")
        if config.get("ffmpeg.adaptive_crf", True) and video_stream:
            crf = compute_adaptive_crf(video_stream, probe["duration"], probe["size_bytes"], base_crf)
        else:
            crf = base_crf
        watermark_filter = build_watermark_filter(config.get("watermark", {}))

    encoders_to_try = encoder_chain_for_attempt(attempt, hwaccel_enabled)
    last_error = "Unknown error"

    for encoder in encoders_to_try:
        command = _build_ffmpeg_command(
            input_file, output_file,
            video_ok=video_ok, audio_ok=audio_ok,
            video_encoder=encoder, crf=crf, preset=preset,
            audio_bitrate=config.get("ffmpeg.audio_bitrate", "192k"),
            audio_sample_rate=config.get("ffmpeg.audio_sample_rate", "48000"),
            watermark_filter=watermark_filter,
            force_full_reencode=force_full,
        )
        logger.info(f"[{task_id}] Attempt {attempt} via {encoder}: {' '.join(command)}")
        ok, stderr = await asyncio.to_thread(_run_ffmpeg, command)
        if ok:
            return output_file, {"encoder": encoder, "crf": crf, "video_ok": video_ok, "audio_ok": audio_ok}
        last_error = diagnose_ffmpeg_error(stderr)
        logger.warning(f"[{task_id}] Encoder '{encoder}' failed: {last_error}")

    raise RuntimeError(last_error)


async def process_video_with_retry(
    input_file: str,
    task_id: str,
    status_cb,
) -> str:
    """
    Retries the whole conversion up to `max_retries` times, each attempt
    using a progressively safer strategy (see fix_for_instagram_v2).
    Cleans up any partial output file between failed attempts.
    """
    max_retries = int(config.get("max_retries", 3))
    output_file = input_file.rsplit(".", 1)[0] + "_fixed.mp4"
    errors: List[str] = []

    for attempt in range(1, max_retries + 1):
        try:
            await status_cb(f"⚙️ Converting (attempt {attempt}/{max_retries})...")
            result_path, meta = await fix_for_instagram_v2(input_file, output_file, task_id, attempt)
            logger.info(f"[{task_id}] Conversion succeeded on attempt {attempt}: {meta}")
            return result_path
        except RuntimeError as e:
            errors.append(f"Attempt {attempt}: {e}")
            logger.error(f"[{task_id}] Attempt {attempt} failed: {e}")
            if await asyncio.to_thread(os.path.exists, output_file):
                await asyncio.to_thread(os.remove, output_file)
            if attempt < max_retries:
                await asyncio.sleep(2 * attempt)  # small backoff before retrying

    raise RuntimeError(" | ".join(errors))


# ===================================================================
# ============================ GLOBAL STATE ===========================
# ===================================================================

shutdown_event = asyncio.Event()
valid_log_channel = False
admin_progress_messages: Dict[str, int] = {}
task_start_times: Dict[str, float] = {}
current_task_id: Optional[str] = None

app = Client("upload_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# FIFO queue: only ONE task is ever converted at a time, guaranteeing
# predictable CPU/RAM usage on small hosts.
task_queue: "asyncio.Queue[dict]" = asyncio.Queue()


class TaskTracker:
    def __init__(self):
        self._tasks = set()
        self.loop = None

    def create_task(self, coro, name=None):
        if self.loop is None:
            try:
                self.loop = asyncio.get_running_loop()
            except RuntimeError:
                logger.error("Could not create task: no running event loop.")
                return
        task = self.loop.create_task(coro, name=name) if name else self.loop.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def cancel_and_wait_all(self):
        pending = [t for t in self._tasks if not t.done()]
        if not pending:
            return
        logger.info(f"Cancelling {len(pending)} outstanding background tasks...")
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


task_tracker = TaskTracker()

# ===================================================================
# ======================= ADMIN STATUS / PROGRESS ======================
# ===================================================================

async def send_admin_status(task_id: str, text: str, edit: bool = False):
    for admin_id in config.admin_ids:
        try:
            key = f"{admin_id}:{task_id}"
            if edit and key in admin_progress_messages:
                await app.edit_message_text(admin_id, admin_progress_messages[key], text)
            else:
                msg = await app.send_message(admin_id, text)
                admin_progress_messages[key] = msg.id
        except FloodWait as e:
            logger.warning(f"FloodWait sending admin status: {e.value}s")
            await asyncio.sleep(e.value)
        except MessageNotModified:
            pass
        except Exception as e:
            logger.error(f"Failed to send admin status for {task_id} to {admin_id}: {e}")


async def admin_progress_callback(current, total, task_id, status_text):
    try:
        percentage = int(current * 100 / total) if total else 0
        if percentage % 10 == 0 or current == total or current == 0:
            bar = "█" * int(percentage / 5) + " " * (20 - int(percentage / 5))
            await send_admin_status(task_id, f"{status_text}\n`[{bar}] {percentage}%`", edit=True)
    except Exception:
        pass


async def update_conversion_heartbeat(task_id, status_text, stop_event: asyncio.Event):
    spinners = ["⢿", "⣻", "⣽", "⣾", "⣷", "⣯", "⣷", "⣾", "⣽", "⣻"]
    i = 0
    while not stop_event.is_set():
        try:
            await send_admin_status(task_id, f"{status_text} {spinners[i % len(spinners)]}", edit=True)
            i += 1
            await asyncio.sleep(2)
        except asyncio.CancelledError:
            break
        except Exception:
            break


async def cleanup_temp_files(paths: List[str]):
    """Deletes files IMMEDIATELY — no delayed/scheduled cleanup, per spec."""
    for path in paths:
        if not path:
            continue
        try:
            if await asyncio.to_thread(os.path.exists, path):
                await asyncio.to_thread(os.remove, path)
                logger.info(f"[CLEANUP] Removed '{path}'.")
        except Exception as e:
            logger.error(f"[CLEANUP] Could not remove '{path}': {e}")


# ===================================================================
# ====================== HTTP HEALTH CHECK SERVER ======================
# ===================================================================

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        payload = {
            "status": "running",
            "instance_id": WORKER_INSTANCE_ID,
            "current_task": current_task_id,
            "queue_size": task_queue.qsize(),
        }
        self.wfile.write(json.dumps(payload).encode())

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass  # silence default HTTP access logging


def run_health_server():
    try:
        server = HTTPServer(("0.0.0.0", 8080), HealthHandler)
        logger.info("HTTP health check server started on port 8080.")
        server.serve_forever()
    except Exception as e:
        logger.error(f"HTTP server failed: {e}")


# ===================================================================
# ========================== QUEUE WORKER =============================
# ===================================================================

async def queue_worker():
    """Single consumer: guarantees strictly one conversion at a time."""
    global current_task_id
    while True:
        item = await task_queue.get()
        task_id = item["task_id"]
        current_task_id = task_id
        try:
            await process_task(item)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(f"[QUEUE] Unhandled error while processing {task_id}")
        finally:
            current_task_id = None
            task_queue.task_done()


async def process_task(item: dict):
    task_id = item["task_id"]
    chat_id = item["chat_id"]
    message_id = item["message_id"]
    media_group_id = item.get("media_group_id")

    task_data = await asyncio.to_thread(db_claim_task, task_id)
    if not task_data:
        logger.warning(f"[WORKER] Task {task_id} could not be claimed (already taken or not pending). Skipping.")
        return

    user_id = task_data.get("user_id", "Unknown")
    task_start_times[task_id] = time.time()

    async def status(text: str, edit: bool = True):
        await send_admin_status(task_id, f"👤 **Task:** `{task_id}`\n**User:** `{user_id}`\n**Status:** {text}", edit=edit)

    await status("📥 Downloading...", edit=False)

    try:
        if media_group_id:
            media_messages = await app.get_media_group(chat_id, message_id)
        else:
            media_messages = [await app.get_messages(chat_id, message_id)]
    except FloodWait as e:
        await asyncio.sleep(e.value)
        media_messages = []
    except Exception as e:
        logger.error(f"[WORKER] Could not fetch media for {task_id}: {e}")
        media_messages = []

    if not media_messages:
        await asyncio.to_thread(db_update_task, task_id, status=STATUS_FAILED, fail_reason="Could not retrieve source media from Telegram.")
        await status("❌ Failed — source media unavailable.")
        return

    files_to_clean: List[str] = []
    converted_paths: List[str] = []
    heartbeat_task = None
    stop_heartbeat = asyncio.Event()

    try:
        for i, media_msg in enumerate(media_messages):
            is_video_file = bool(media_msg.video) or (media_msg.document and "video" in (media_msg.document.mime_type or ""))

            dl_status = f"📥 Downloading file {i + 1}/{len(media_messages)}..."
            await status(dl_status)
            download_path = await app.download_media(
                media_msg, progress=admin_progress_callback, progress_args=(task_id, dl_status)
            )
            files_to_clean.append(download_path)

            if is_video_file:
                conv_status = f"⚙️ Converting file {i + 1}/{len(media_messages)}..."
                await status(conv_status)
                stop_heartbeat.clear()
                heartbeat_task = task_tracker.create_task(
                    update_conversion_heartbeat(task_id, conv_status, stop_heartbeat)
                )
                try:
                    converted_path = await process_video_with_retry(
                        download_path, task_id, lambda t: status(t)
                    )
                finally:
                    stop_heartbeat.set()
                converted_paths.append(converted_path)
                files_to_clean.append(converted_path)
            else:
                converted_paths.append(download_path)

        await status("📤 Uploading back to channel...")
        await upload_result(task_id, converted_paths)

        await asyncio.to_thread(db_update_task, task_id, status=STATUS_CONVERTED)
        elapsed = time.time() - task_start_times.get(task_id, time.time())
        await status(f"✔️ Finished in {elapsed:.1f}s.")
        logger.info(f"[WORKER] Task {task_id} completed in {elapsed:.1f}s.")

    except Exception as e:
        reason = str(e) if isinstance(e, RuntimeError) else diagnose_ffmpeg_error(str(e))
        logger.error(f"[WORKER] Task {task_id} failed: {reason}", exc_info=True)
        await asyncio.to_thread(
            db_update_task, task_id, status=STATUS_FAILED, fail_reason=reason,
            retry_count=int(config.get("max_retries", 3)),
        )
        await status(f"❌ Failed.\n**Reason:** `{reason}`")

    finally:
        stop_heartbeat.set()
        if heartbeat_task:
            try:
                await asyncio.wait_for(heartbeat_task, timeout=0.5)
            except Exception:
                pass
        # Strict immediate cleanup — no delayed scheduling.
        await cleanup_temp_files(files_to_clean)
        task_start_times.pop(task_id, None)


async def upload_result(task_id: str, converted_paths: List[str]):
    channel_id = config.worker_channel_id
    last_exc = None
    for attempt in range(1, 4):
        try:
            if len(converted_paths) > 1:
                media_group = [
                    InputMediaVideo(p) if p.endswith((".mp4", ".mov", ".mkv")) else InputMediaPhoto(p)
                    for p in converted_paths
                ]
                sent = await app.send_media_group(channel_id, media_group)
                await app.send_message(channel_id, f"done_{task_id}", reply_to_message_id=sent[-1].id)
            else:
                path = converted_paths[0]
                if path.endswith((".mp4", ".mov", ".mkv")):
                    sent = await app.send_video(channel_id, path)
                else:
                    sent = await app.send_photo(channel_id, path)
                await app.send_message(channel_id, f"done_{task_id}", reply_to_message_id=sent.id)
            return
        except FloodWait as e:
            logger.warning(f"[UPLOAD] FloodWait {e.value}s on attempt {attempt} for {task_id}")
            await asyncio.sleep(e.value)
            last_exc = e
        except RPCError as e:
            logger.warning(f"[UPLOAD] Attempt {attempt} failed for {task_id}: {e}")
            last_exc = e
            await asyncio.sleep(2 * attempt)
    raise RuntimeError(f"Upload failed after 3 attempts: {last_exc}")


# ===================================================================
# ==================== NEW FEATURE #3: STUCK-TASK ======================
# ==================== SELF-HEALING ON STARTUP    ======================
# ===================================================================
# Why: your bot has already lost its environment once on a redeploy.
# The same class of event (host restart, OOM-kill, crash) can happen
# again mid-conversion, leaving a task permanently stuck in
# "converting" — the Main Bot would wait forever for a `done_` reply
# that will never come. On every startup, the worker now scans for any
# task left in "converting" past a safety window and automatically
# fails it with a clear, honest reason so the Main Bot (and you) know
# to resubmit it, instead of it silently vanishing into limbo.

async def recover_stuck_tasks():
    stuck = await asyncio.to_thread(db_find_stuck_tasks, 10)
    if not stuck:
        return
    logger.warning(f"[RECOVERY] Found {len(stuck)} stuck task(s) from a previous run.")
    for task in stuck:
        task_id = task["_id"]
        await asyncio.to_thread(
            db_update_task, task_id, status=STATUS_FAILED,
            fail_reason="Worker process restarted while this task was converting. Please resubmit.",
        )
        logger.warning(f"[RECOVERY] Marked stuck task {task_id} as failed.")
    for admin_id in config.admin_ids:
        try:
            ids = ", ".join(t["_id"] for t in stuck)
            await app.send_message(
                admin_id,
                f"⚠️ **Recovered {len(stuck)} stuck task(s) after restart**\n`{ids}`\nThey were marked as failed — please resubmit if still needed.",
            )
        except Exception:
            pass


# ===================================================================
# ========================= TELEGRAM HANDLERS ==========================
# ===================================================================

@app.on_message(filters.text & worker_task_filter & filters.reply)
async def receive_task_handler(client, message: Message):
    """The Worker Channel receives a plain-text task_id reply — enqueue it."""
    task_id = message.text.strip()
    if not task_id or task_id.startswith("done_"):
        return

    replied = message.reply_to_message
    if not replied:
        logger.error(f"[WORKER] Task ID '{task_id}' message is not a reply — ignoring.")
        return

    task_data = await asyncio.to_thread(db_get_task, task_id)
    if not task_data:
        logger.error(f"[WORKER] No DB entry found for task {task_id}.")
        return
    if task_data.get("status") != STATUS_PENDING:
        logger.warning(f"[WORKER] Task {task_id} is not pending (status={task_data.get('status')}). Skipping enqueue.")
        return

    await task_queue.put({
        "task_id": task_id,
        "chat_id": replied.chat.id,
        "message_id": replied.id,
        "media_group_id": replied.media_group_id,
    })
    logger.info(f"[WORKER] Task {task_id} enqueued. Queue size: {task_queue.qsize()}")


# ------------------------- Admin Panel: /start ------------------------

def main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Stats", callback_data="menu_stats"),
         InlineKeyboardButton("⚙️ Settings", callback_data="menu_settings")],
        [InlineKeyboardButton("🎨 Watermark", callback_data="menu_watermark"),
         InlineKeyboardButton("🗑️ Clear Queue", callback_data="menu_clearqueue")],
        [InlineKeyboardButton("🔄 Restart", callback_data="menu_restart")],
    ])


def back_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu_back")]])


def confirm_markup(action: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes", callback_data=f"confirm_{action}"),
        InlineKeyboardButton("❌ No", callback_data="menu_back"),
    ]])


@app.on_message(filters.command("start") & admin_only)
async def start_cmd(_, message: Message):
    await message.reply(
        "🤖 **Instagram Worker Bot — Admin Panel**\n\nChoose an option below:",
        reply_markup=main_menu_markup(),
    )


@app.on_message(filters.command("help") & admin_only)
async def help_cmd(_, message: Message):
    await message.reply(
        "**Available commands**\n"
        "`/start` — open the admin panel\n"
        "`/stats` — quick queue & task stats\n"
        "`/getconfig [key]` — show current config (or one key)\n"
        "`/setconfig <key.path> <value>` — update a config value live\n"
        "`/addadmin <user_id>` / `/removeadmin <user_id>`\n"
        "`/watermark on|off` — toggle the watermark\n"
        "`/restart` — restart the worker process\n"
        "`/clearqueue` — drop all pending queued tasks\n\n"
        "_Examples:_\n"
        "`/setconfig ffmpeg.base_crf 22`\n"
        "`/setconfig watermark.position top_left`\n"
        "`/setconfig max_retries 5`"
    )


async def build_stats_text() -> str:
    db_stats = await asyncio.to_thread(db_task_stats)
    cpu = psutil.cpu_percent(interval=0.3)
    mem = psutil.virtual_memory()
    active = f"`{current_task_id}`" if current_task_id else "None"
    return (
        "📊 **Worker Stats**\n\n"
        f"**Instance:** `{WORKER_INSTANCE_ID}`\n"
        f"**Queue size:** `{task_queue.qsize()}`\n"
        f"**Currently processing:** {active}\n\n"
        f"**Tasks in DB**\n"
        f"⏳ Pending: `{db_stats.get(STATUS_PENDING, 0)}`\n"
        f"⚙️ Converting: `{db_stats.get(STATUS_CONVERTING, 0)}`\n"
        f"✅ Converted: `{db_stats.get(STATUS_CONVERTED, 0)}`\n"
        f"❌ Failed: `{db_stats.get(STATUS_FAILED, 0)}`\n\n"
        f"**System**\n"
        f"CPU: `{cpu}%` | RAM: `{mem.percent}%`\n"
        f"HW encoders: `{', '.join(l for _, l in _available_hw_encoders) or 'none (software only)'}`"
    )


@app.on_message(filters.command("stats") & admin_only)
async def stats_cmd(_, message: Message):
    await message.reply(await build_stats_text())


def build_settings_text() -> str:
    fcfg = config.get("ffmpeg", {})
    wcfg = config.get("watermark", {})
    return (
        "⚙️ **Current Settings**\n\n"
        f"Admins: `{config.admin_ids}`\n"
        f"Worker channel: `{config.worker_channel_id}`\n"
        f"Log channel: `{config.log_channel_id}`\n"
        f"Max retries: `{config.get('max_retries')}`\n\n"
        f"**FFmpeg**\n"
        f"HW accel: `{fcfg.get('hwaccel_enabled')}` | Preset: `{fcfg.get('preset')}`\n"
        f"Base CRF: `{fcfg.get('base_crf')}` | Adaptive CRF: `{fcfg.get('adaptive_crf')}`\n\n"
        f"**Watermark**\n"
        f"Enabled: `{wcfg.get('enabled')}` | Type: `{wcfg.get('type')}`\n"
        f"Position: `{wcfg.get('position')}` | Opacity: `{wcfg.get('opacity')}`\n\n"
        "Use `/setconfig <key.path> <value>` to change anything above."
    )


@app.on_message(filters.command("getconfig") & admin_only)
async def getconfig_cmd(_, message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) == 1:
        await message.reply(f"```json\n{config.as_pretty_json()}\n```")
    else:
        value = config.get(parts[1].strip())
        await message.reply(f"`{parts[1].strip()}` = `{value}`")


def _coerce_value(raw: str) -> Any:
    if raw.lower() in ("true", "false"):
        return raw.lower() == "true"
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    if "," in raw:
        return [_coerce_value(x.strip()) for x in raw.split(",") if x.strip()]
    return raw


@app.on_message(filters.command("setconfig") & admin_only)
async def setconfig_cmd(_, message: Message):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.reply("Usage: `/setconfig <key.path> <value>`")
        return
    key, raw_value = parts[1], parts[2]
    value = _coerce_value(raw_value)
    await config.set(key, value)
    await message.reply(f"✅ `{key}` updated to `{value}`.")


@app.on_message(filters.command("addadmin") & admin_only)
async def addadmin_cmd(_, message: Message):
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.reply("Usage: `/addadmin <user_id>`")
        return
    new_id = int(parts[1])
    ids = list(config.admin_ids)
    if new_id not in ids:
        ids.append(new_id)
        await config.set("admin_ids", ids)
    await message.reply(f"✅ `{new_id}` added as admin.")


@app.on_message(filters.command("removeadmin") & admin_only)
async def removeadmin_cmd(_, message: Message):
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.reply("Usage: `/removeadmin <user_id>`")
        return
    rem_id = int(parts[1])
    ids = [i for i in config.admin_ids if i != rem_id]
    await config.set("admin_ids", ids)
    await message.reply(f"✅ `{rem_id}` removed from admins.")


@app.on_message(filters.command("watermark") & admin_only)
async def watermark_cmd(_, message: Message):
    parts = message.text.split()
    if len(parts) == 2 and parts[1].lower() in ("on", "off"):
        await config.set("watermark.enabled", parts[1].lower() == "on")
        await message.reply(f"✅ Watermark turned **{parts[1].upper()}**.")
    else:
        await message.reply(
            "Usage: `/watermark on|off`\n"
            "For detailed changes use `/setconfig watermark.<field> <value>`\n"
            "Fields: `type` (text/image), `text`, `image_path`, `position` "
            "(top_left/top_right/bottom_left/bottom_right/center), `opacity` (0-1), "
            "`scale`, `margin`."
        )


@app.on_message(filters.command("clearqueue") & admin_only)
async def clearqueue_cmd(_, message: Message):
    n = await drain_queue()
    await message.reply(f"🗑️ Cleared `{n}` pending task(s) from the queue.")


async def drain_queue() -> int:
    count = 0
    while not task_queue.empty():
        try:
            item = task_queue.get_nowait()
            task_queue.task_done()
            await asyncio.to_thread(
                db_update_task, item["task_id"], status=STATUS_FAILED,
                fail_reason="Cleared from queue by admin.",
            )
            count += 1
        except asyncio.QueueEmpty:
            break
    return count


@app.on_message(filters.command("restart") & admin_only)
async def restart_cmd(_, message: Message):
    await message.reply("🛠 **Worker restarting...**")
    logger.info(f"Admin {message.from_user.id} initiated a restart.")
    await send_admin_status("WORKER_RESTART", "🛠 Worker restart requested by admin. Shutting down.", edit=False)
    sys.exit(0)


# ------------------------- Callback query dispatch ------------------------

@app.on_callback_query()
async def callback_dispatch(_, cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.answer("Not authorized.", show_alert=True)
        return

    data = cq.data
    try:
        if data == "menu_back":
            await cq.message.edit_text("🤖 **Instagram Worker Bot — Admin Panel**\n\nChoose an option below:", reply_markup=main_menu_markup())

        elif data == "menu_stats":
            await cq.message.edit_text(await build_stats_text(), reply_markup=back_markup())

        elif data == "menu_settings":
            await cq.message.edit_text(build_settings_text(), reply_markup=back_markup())

        elif data == "menu_watermark":
            wcfg = config.get("watermark", {})
            toggle_label = "🔴 Disable" if wcfg.get("enabled") else "🟢 Enable"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(toggle_label, callback_data="wm_toggle")],
                [InlineKeyboardButton("⬅️ Back", callback_data="menu_back")],
            ])
            await cq.message.edit_text(
                f"🎨 **Watermark**\n\nEnabled: `{wcfg.get('enabled')}`\nType: `{wcfg.get('type')}`\n"
                f"Position: `{wcfg.get('position')}`\nOpacity: `{wcfg.get('opacity')}`\n\n"
                "Use `/setconfig watermark.<field> <value>` for detailed edits.",
                reply_markup=kb,
            )

        elif data == "wm_toggle":
            new_state = not config.get("watermark.enabled", False)
            await config.set("watermark.enabled", new_state)
            await cq.answer(f"Watermark {'enabled' if new_state else 'disabled'}.")
            wcfg = config.get("watermark", {})
            toggle_label = "🔴 Disable" if wcfg.get("enabled") else "🟢 Enable"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(toggle_label, callback_data="wm_toggle")],
                [InlineKeyboardButton("⬅️ Back", callback_data="menu_back")],
            ])
            await cq.message.edit_text(
                f"🎨 **Watermark**\n\nEnabled: `{wcfg.get('enabled')}`\nType: `{wcfg.get('type')}`\n"
                f"Position: `{wcfg.get('position')}`\nOpacity: `{wcfg.get('opacity')}`",
                reply_markup=kb,
            )

        elif data == "menu_clearqueue":
            await cq.message.edit_text(
                f"🗑️ Clear all `{task_queue.qsize()}` pending task(s) from the queue?",
                reply_markup=confirm_markup("clearqueue"),
            )

        elif data == "confirm_clearqueue":
            n = await drain_queue()
            await cq.message.edit_text(f"✅ Cleared `{n}` task(s).", reply_markup=back_markup())

        elif data == "menu_restart":
            await cq.message.edit_text("🔄 Restart the worker bot now?", reply_markup=confirm_markup("restart"))

        elif data == "confirm_restart":
            await cq.message.edit_text("🛠 Restarting now...")
            logger.info(f"Admin {cq.from_user.id} confirmed restart via panel.")
            sys.exit(0)

        await cq.answer()
    except MessageNotModified:
        await cq.answer()
    except Exception as e:
        logger.error(f"[CALLBACK] Error handling '{data}': {e}")
        await cq.answer("Something went wrong.", show_alert=True)


# ===================================================================
# ============================ STARTUP ================================
# ===================================================================

async def start_bot():
    global mongo, db, valid_log_channel

    try:
        mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo.admin.command("ping")
        db = mongo[DB_NAME]
        logger.info(f"✅ Connected to MongoDB database '{DB_NAME}'.")
    except Exception as e:
        logger.critical(f"❌ DATABASE SETUP FAILED: {e}. Worker cannot function without DB.")
        sys.exit(1)

    await config.load()
    if not config.worker_channel_id:
        logger.critical(
            "FATAL: worker_channel_id is not set in MongoDB settings and no "
            "WORKER_CHANNEL_ID bootstrap seed was provided. Set it with "
            "/setconfig worker_channel_id <id> once you can reach the bot, "
            "or provide WORKER_CHANNEL_ID in the environment for the first run."
        )
        sys.exit(1)

    await asyncio.to_thread(detect_hwaccel_encoders)

    threading.Thread(target=run_health_server, daemon=True).start()

    await app.start()
    task_tracker.loop = asyncio.get_running_loop()

    await recover_stuck_tasks()

    # Start the single FIFO queue consumer.
    task_tracker.create_task(queue_worker(), name="queue_worker")

    logger.info(f"Worker listening on channel {config.worker_channel_id}.")

    if config.log_channel_id:
        try:
            await app.send_message(config.log_channel_id, "🛠️ **Worker Bot is Online!**\nListening for conversion tasks...")
            valid_log_channel = True
        except Exception as e:
            logger.error(f"Could not log to channel {config.log_channel_id}: {e}")

    for admin_id in config.admin_ids:
        try:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            await app.send_message(admin_id, f"✅ **Worker Bot is ONLINE!**\nInstance: `{WORKER_INSTANCE_ID}`\nStarted: `{ts}`")
        except Exception as e:
            logger.error(f"Could not send startup DM to admin {admin_id}: {e}")

    logger.info("Worker Bot is now online! Waiting for tasks...")
    await idle()

    logger.info("Shutting down...")
    await task_tracker.cancel_and_wait_all()
    await app.stop()
    if mongo:
        mongo.close()
    logger.info("Bot has been shut down gracefully.")


if __name__ == "__main__":
    try:
        app.run(start_bot())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.critical(f"Bot crashed during startup: {e}", exc_info=True)
