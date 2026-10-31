import os
import re
import sys
import time
import json
import asyncio
import traceback
import uuid
import random
import threading
import shutil
import gc
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Dict
import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
from discord.ui import View, Button, Modal, TextInput, Select

try:
    from PIL import Image
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    max_messages=100,
    chunk_guilds_at_startup=False
)

TOKEN = os.getenv("DISCORD_TOKEN")
INTRO_VIDEO_URL = os.getenv("INTRO_VIDEO_URL", "")
KICK_GREEN = 0x53FC18
TWITCH_PURPLE = 0x9146FF
TWITCH_CLIENT_ID = "kimne78kx3ncx6brgo4mv6wki5h1ko"

DEFAULT_CHANNELS_RAW = os.getenv("KICK_CHANNELS", "")
DEFAULT_CHANNEL_ID = os.getenv("ALERT_CHANNEL_ID", "")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ALERTS_FILE = os.path.join(BASE_DIR, "alerts.json")
CACHE_FILE = os.path.join(BASE_DIR, "live_cache.json")
DASHBOARD_FILE = os.path.join(BASE_DIR, "dashboard.json")
STATS_FILE = os.path.join(BASE_DIR, "stats.json")
ARCHIVE_FILE = os.path.join(BASE_DIR, "shots_archive.json")
ACTIVITY_FILE = os.path.join(BASE_DIR, "activity_feed.json")

EXCLUDED_CHANNELS = ["alicja_grabowiecka"]
EXCLUDED_USERS = ["latek"]

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

KICK_CLIP_REGEX = re.compile(
    r"https?://(?:www\.)?kick\.com/(?:([a-zA-Z0-9_\-]+)/(?:clips/|clip/)|(?:[^\s?#]+)?\?(?:[^&\s]*&)?clip=)(clip_[a-zA-Z0-9]+)",
    re.IGNORECASE
)
TWITCH_CLIP_REGEX = re.compile(
    r"https?://(?:www\.)?(?:clips\.twitch\.tv/([a-zA-Z0-9_\-]+)|twitch\.tv/([a-zA-Z0-9_\-]+)/clip/([a-zA-Z0-9_\-]+))",
    re.IGNORECASE
)

_render_sem: Optional[asyncio.Semaphore] = None
_check_sem: Optional[asyncio.Semaphore] = None

def get_render_sem() -> asyncio.Semaphore:
    global _render_sem
    if _render_sem is None:
        _render_sem = asyncio.Semaphore(1)
    return _render_sem

def get_check_sem() -> asyncio.Semaphore:
    global _check_sem
    if _check_sem is None:
        _check_sem = asyncio.Semaphore(4)
    return _check_sem

cached_channel_list = []
offline_counters = {}
initial_check_done = False
channel_meta_cache = {}

cached_search_clips: List[dict] = []
last_search_scan_time: float = 0

last_dashboard_signature: str = ""
last_dashboard_edit_time: float = 0
global_session: Optional[aiohttp.ClientSession] = None

cached_global_stats = {"shots": 1, "screens": 1, "gifs": 1, "intro_video_url": INTRO_VIDEO_URL}
cached_leaderboard_data = []
recent_activity_feed: List[dict] = []

_memory_cache = {"alerts": None, "dashboard": None, "archive": None}

# ==============================================================================
# 0. SERWER HTTP DLA STRONY WWW
# ==============================================================================

class ReusableHTTPServer(HTTPServer):
    allow_reuse_address = True

def load_activity_feed() -> List[dict]:
    global recent_activity_feed
    if recent_activity_feed:
        return recent_activity_feed
    if os.path.exists(ACTIVITY_FILE):
        try:
            with open(ACTIVITY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    recent_activity_feed = data[:15]
                    return recent_activity_feed
        except Exception:
            pass
    return []

def push_activity_event(user_name: str, action_text: str, pts_text: str, style: str = "", event_id: Optional[str] = None, time_str: Optional[str] = None):
    global recent_activity_feed
    if not user_name or user_name.lower() in ["automatyczny shot", "automatyczny", "kik-bot", "ziemia 2.0"]:
        return
    if normalize_name(user_name) in EXCLUDED_USERS:
        return

    load_activity_feed()
    ev_id = event_id or f"ev_{int(time.time() * 1000)}_{uuid.uuid4().hex[:4]}"
    if any(x.get("id") == ev_id for x in recent_activity_feed):
        return

    now_str = time_str or datetime.now().strftime("%H:%M:%S")
    entry = {
        "id": ev_id,
        "time": now_str,
        "user": user_name,
        "text": action_text,
        "pts": pts_text,
        "style": style
    }
    recent_activity_feed.insert(0, entry)
    recent_activity_feed = recent_activity_feed[:15]
    try:
        with open(ACTIVITY_FILE, "w", encoding="utf-8") as f:
            json.dump(recent_activity_feed, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

class StatsHandler(BaseHTTPRequestHandler):
    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        clean_path = self.path.split("?")[0].rstrip("/")
        if clean_path in ["", "/", "/health"]:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(b"OK")
        elif clean_path in ["/api/stats", "/stats"]:
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self._send_cors_headers()
            self.end_headers()
            cached_global_stats["intro_video_url"] = INTRO_VIDEO_URL
            self.wfile.write(json.dumps(cached_global_stats).encode("utf-8"))
        elif clean_path in ["/api/top", "/top"]:
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps(cached_leaderboard_data).encode("utf-8"))
        elif clean_path in ["/api/activity", "/activity"]:
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps(load_activity_feed()).encode("utf-8"))
        else:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()

    def log_message(self, format, *args):
        pass

def run_http_server():
    port = int(os.environ.get("PORT", 8080))
    try:
        server = ReusableHTTPServer(("0.0.0.0", port), StatsHandler)
        server.serve_forever()
    except Exception as e:
        print(f"[HTTP SERVER ERR] {e}")

threading.Thread(target=run_http_server, daemon=True).start()

# ==============================================================================
# 1. PROCESY, PLIKI TYMCZASOWE I ZNAK WODNY (LOGO)
# ==============================================================================

async def run_cmd_async(cmd: List[str], timeout: float = 45.0, capture_stdout: bool = False) -> Tuple[int, bytes, bytes]:
    proc = None
    out_target = asyncio.subprocess.PIPE if capture_stdout else asyncio.subprocess.DEVNULL
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=out_target,
            stderr=asyncio.subprocess.DEVNULL
        )
        if capture_stdout:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return proc.returncode or 0, stdout or b"", b""
        else:
            await asyncio.wait_for(proc.wait(), timeout=timeout)
            return proc.returncode or 0, b"", b""
    except Exception as e:
        if proc:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
        return -1, b"", str(e).encode("utf-8", errors="ignore")

def cleanup_orphaned_temp_files(max_age_sec: int = 300):
    now = time.time()
    prefixes = ("raw_", "master_", "shot_", "comp_", "temp_gif_", "gif_", "screen_", "clip_", "twclip_", "cut_", "trim_dl_")
    try:
        for fname in os.listdir(BASE_DIR):
            if fname.startswith(prefixes) and fname.endswith((".ts", ".mp4", ".png", ".gif")):
                fpath = os.path.join(BASE_DIR, fname)
                if now - os.path.getmtime(fpath) > max_age_sec:
                    try:
                        os.remove(fpath)
                    except Exception:
                        pass
    except Exception:
        pass

def find_logo_file() -> Optional[str]:
    for name in ("logo.png", "LOGO.png", "logo.webp"):
        p = os.path.join(BASE_DIR, name)
        if os.path.exists(p) and os.path.getsize(p) > 100:
            return p
    return None

def get_prepared_logo_path(target_w: int = 230) -> Optional[str]:
    src_logo = find_logo_file()
    if not src_logo:
        return None
    if HAS_PIL:
        try:
            src_mtime = int(os.path.getmtime(src_logo))
            src_size = os.path.getsize(src_logo)
            out_path = os.path.join(BASE_DIR, f"wm_ready_{target_w}_{src_size}.png")
            if os.path.exists(out_path) and os.path.getsize(out_path) > 100 and int(os.path.getmtime(out_path)) >= src_mtime:
                return out_path
            with Image.open(src_logo) as img:
                rgba = img.convert("RGBA")
                w_percent = target_w / float(rgba.size[0])
                target_h = max(int(float(rgba.size[1]) * w_percent), 2)
                if target_h % 2 != 0:
                    target_h += 1
                resized = rgba.resize((target_w, target_h), Image.Resampling.LANCZOS)
                resized.save(out_path, format="PNG")
            return out_path
        except Exception as e:
            print(f"[WM PREPARE ERR] {e}")
    return src_logo

def overlay_logo_on_image(image_path: str):
    src_logo = find_logo_file()
    if not src_logo or not os.path.exists(image_path) or not HAS_PIL:
        return
    try:
        with Image.open(image_path) as base_img:
            base = base_img.convert("RGBA")
            with Image.open(src_logo) as logo_img:
                logo = logo_img.convert("RGBA")
                target_w = max(int(base.size[0] * 0.18), 160)
                w_percent = target_w / float(logo.size[0])
                target_h = max(int(float(logo.size[1]) * w_percent), 2)
                logo_resized = logo.resize((target_w, target_h), Image.Resampling.LANCZOS)
                base.alpha_composite(logo_resized, dest=(25, 25))
                base.save(image_path, format="PNG")
    except Exception as e:
        print(f"[PIL OVERLAY ERR] {e}")

async def get_session() -> aiohttp.ClientSession:
    global global_session
    if global_session is None or global_session.closed:
        connector = aiohttp.TCPConnector(limit=8, ttl_dns_cache=600)
        global_session = aiohttp.ClientSession(
            connector=connector,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json", "Referer": "https://kick.com/"}
        )
    return global_session

def clean_channel_name(channel: str, force_platform: Optional[str] = None) -> str:
    if not channel:
        return ""
    ch = str(channel).strip().lower()
    ch = ch.replace("https://", "").replace("http://", "").replace("www.", "")
    is_twitch = (force_platform == "twitch")
    for pref in ("twitch.tv/", "twitch:", "tw:", "tw."):
        if ch.startswith(pref):
            is_twitch = True
            ch = ch[len(pref):]
            break
    for pref in ("kick.com/", "kick:"):
        if ch.startswith(pref):
            is_twitch = False
            ch = ch[len(pref):]
            break
    ch = ch.split("/")[0].split("?")[0].strip()
    if not ch:
        return ""
    return f"tw.{ch}" if is_twitch else ch

def get_platform(channel: str) -> str:
    return "twitch" if clean_channel_name(channel).startswith("tw.") else "kick"

def get_channel_login(channel: str) -> str:
    c = clean_channel_name(channel)
    return c[3:] if c.startswith("tw.") else c

def get_channel_display(channel: str) -> str:
    return get_channel_login(channel)

def get_channel_web_url(channel: str) -> str:
    login = get_channel_login(channel)
    return f"https://www.twitch.tv/{login}" if get_platform(channel) == "twitch" else f"https://kick.com/{login}"

def get_platform_color(channel: str) -> int:
    return TWITCH_PURPLE if get_platform(channel) == "twitch" else KICK_GREEN

def get_platform_badge(channel: str) -> str:
    return "🟣 Twitch" if get_platform(channel) == "twitch" else "🟢 Kick"

def normalize_name(name: str) -> str:
    if not name:
        return ""
    n = name.strip().lower().replace("_", " ")
    for k, v in {"ą": "a", "ć": "c", "ę": "e", "ł": "l", "ń": "n", "ó": "o", "ś": "s", "ź": "z", "ż": "z"}.items():
        n = n.replace(k, v)
    return re.sub(r"\s+", " ", n).strip()

def parse_iso_time(iso_str: str) -> Optional[float]:
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None

def format_uptime(seconds: float) -> str:
    secs = max(int(seconds), 0)
    h, m = secs // 3600, (secs % 3600) // 60
    return f"{h}h {m:02d}m" if h > 0 else f"{m} min"

def split_text_into_chunks(text_list: List[str], max_chars: int = 900) -> List[str]:
    chunks, current = [], ""
    for item in text_list:
        if len(current) + len(item) > max_chars:
            if current:
                chunks.append(current.strip())
            current = item
        else:
            current += item
    if current:
        chunks.append(current.strip())
    return chunks

def is_sleeping_stream(channel: str) -> bool:
    clean_ch = clean_channel_name(channel)
    meta = channel_meta_cache.get(clean_ch, {})
    combined = f"{(meta.get('title') or '').lower()} {(meta.get('category') or '').lower()}"
    if not combined.strip():
        return False
    if any(k in combined for k in ["sleeping", "sliping", "sleepstream", "just sleep"]):
        return True
    return bool(re.search(r"\b(sleep|spie|śpię)\b", combined))

async def fetch_twitch_channel_meta(channel_key: str) -> Tuple[bool, Optional[str], Optional[str], Optional[float]]:
    login = get_channel_login(channel_key)
    clean_key = f"tw.{login}"
    session = await get_session()
    gql_payload = {
        "query": "query($login: String!) { user(login: $login) { stream { id title createdAt type game { name } } } }",
        "variables": {"login": login}
    }
    headers = {"Client-ID": TWITCH_CLIENT_ID, "User-Agent": USER_AGENT, "Content-Type": "application/json"}
    try:
        async with session.post("https://gql.twitch.tv/gql", json=gql_payload, headers=headers, timeout=aiohttp.ClientTimeout(total=4.5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                user_data = (data.get("data") or {}).get("user")
                stream = (user_data or {}).get("stream")
                if not stream or stream.get("type") != "live":
                    return False, None, None, None
                title = stream.get("title") or "Transmisja na żywo"
                category = (stream.get("game") or {}).get("name") or "Twitch Stream"
                started_at = parse_iso_time(stream.get("createdAt"))
                channel_meta_cache.setdefault(clean_key, {})["title"] = title
                channel_meta_cache[clean_key]["category"] = category
                return True, title, category, started_at
    except Exception:
        pass
    return False, None, None, None

async def fetch_kick_channel_meta(channel: str, force_fresh: bool = False) -> Tuple[bool, Optional[str], Optional[str], Optional[float]]:
    clean_ch = clean_channel_name(channel)
    if get_platform(clean_ch) == "twitch":
        return await fetch_twitch_channel_meta(clean_ch)

    login = get_channel_login(clean_ch)
    session = await get_session()

    try:
        ch_url = f"https://kick.com/api/v1/channels/{login}"
        async with session.get(ch_url, timeout=aiohttp.ClientTimeout(total=4.5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                livestream = data.get("livestream")
                if not livestream or not isinstance(livestream, dict):
                    return False, None, None, None
                playback_url = livestream.get("playback_url") or data.get("playback_url")
                title = livestream.get("session_title") or "Transmisja na żywo"
                category = "Kick Stream"
                if livestream.get("categories") and len(livestream["categories"]) > 0:
                    category = livestream["categories"][0].get("name") or category
                channel_meta_cache.setdefault(clean_ch, {})["title"] = title
                channel_meta_cache[clean_ch]["category"] = category
                if playback_url:
                    channel_meta_cache[clean_ch]["playback_url"] = playback_url
                    channel_meta_cache[clean_ch]["url_time"] = time.time()
                return True, title, category, parse_iso_time(livestream.get("created_at"))
            elif resp.status == 404:
                return False, None, None, None
    except Exception:
        pass

    try:
        url = f"https://kick.com/api/v2/channels/{login}/livestream"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=4.5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                livestream = data.get("data") if isinstance(data, dict) else None
                if not livestream or not isinstance(livestream, dict):
                    return False, None, None, None
                title = livestream.get("session_title") or "Transmisja na żywo"
                category = "Kick Stream"
                cats = livestream.get("categories", [])
                if cats and isinstance(cats, list) and len(cats) > 0:
                    category = cats[0].get("name") or category
                playback_url = livestream.get("playback_url")
                channel_meta_cache.setdefault(clean_ch, {})["title"] = title
                channel_meta_cache[clean_ch]["category"] = category
                if playback_url:
                    channel_meta_cache[clean_ch]["playback_url"] = playback_url
                    channel_meta_cache[clean_ch]["url_time"] = time.time()
                return True, title, category, parse_iso_time(livestream.get("created_at") or livestream.get("start_time"))
    except Exception:
        pass
    return False, None, None, None

async def resolve_smart_channel(raw_input: str) -> str:
    cleaned = clean_channel_name(raw_input)
    if cleaned.startswith("tw.") or "kick.com" in raw_input.lower() or "twitch.tv" in raw_input.lower():
        return cleaned
    alerts = load_alerts()
    if cleaned in alerts:
        return cleaned
    if f"tw.{cleaned}" in alerts:
        return f"tw.{cleaned}"
    is_kick_live, _, _, _ = await fetch_kick_channel_meta(cleaned)
    if is_kick_live:
        return cleaned
    is_tw_live, _, _, _ = await fetch_twitch_channel_meta(f"tw.{cleaned}")
    if is_tw_live:
        return f"tw.{cleaned}"
    return cleaned

async def is_valid_video_file_async(file_path: str, min_duration: float = 1.0) -> bool:
    if not os.path.exists(file_path) or os.path.getsize(file_path) < 25000:
        return False
    probe_cmd = [
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=codec_name:format=duration", "-of", "json", file_path
    ]
    code, stdout, _ = await run_cmd_async(probe_cmd, timeout=8.0, capture_stdout=True)
    if code != 0 or not stdout:
        return False
    try:
        data = json.loads(stdout.decode("utf-8", errors="ignore"))
        if not data.get("streams"):
            return False
        return float(data.get("format", {}).get("duration", 0.0)) >= min_duration
    except Exception:
        return False

async def ensure_file_under_discord_limit(file_path: str, max_mb: float = 9.5) -> str:
    if not os.path.exists(file_path):
        return file_path
    if os.path.getsize(file_path) <= int(max_mb * 1024 * 1024):
        return file_path

    compressed_out = os.path.join(BASE_DIR, f"comp_{uuid.uuid4().hex[:6]}.mp4")
    probe_cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", file_path]
    code, stdout, _ = await run_cmd_async(probe_cmd, timeout=6.0, capture_stdout=True)
    try:
        duration = float(stdout.decode().strip()) if code == 0 else 30.0
    except Exception:
        duration = 30.0

    target_bytes = int(8.9 * 1024 * 1024)
    video_kbps = max(int((target_bytes * 8) / max(duration, 1.0) / 1000) - 128, 1100)
    target_h = 1080 if video_kbps >= 1750 else 720

    ffmpeg_compress = [
        "ffmpeg", "-nostdin", "-y", "-threads", "2", "-i", file_path,
        "-vf", f"scale=-2:{target_h}:flags=lanczos,format=yuv420p",
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-color_range", "tv", "-colorspace", "bt709", "-color_primaries", "bt709", "-color_trc", "bt709",
        "-preset", "veryfast", "-g", "30", "-crf", "19",
        "-b:v", f"{video_kbps}k", "-maxrate", f"{int(video_kbps * 1.1)}k", "-bufsize", f"{int(video_kbps * 2)}k",
        "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", compressed_out
    ]
    try:
        await run_cmd_async(ffmpeg_compress, timeout=75.0)
        if await is_valid_video_file_async(compressed_out, min_duration=1.0):
            os.replace(compressed_out, file_path)
    finally:
        if os.path.exists(compressed_out):
            try:
                os.remove(compressed_out)
            except Exception:
                pass
        gc.collect()
    return file_path

# ==============================================================================
# 2. BAZA DANYCH JSON I PAMIĘĆ PODRĘCZNA STATYSTYK
# ==============================================================================

def load_live_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return {
                        clean_channel_name(k): (v if isinstance(v, dict) else {"is_live": bool(v), "started_at": None})
                        for k, v in data.items() if clean_channel_name(k)
                    }
        except Exception:
            pass
    return {}

def save_live_cache(data: dict):
    try:
        clean_data = {
            clean_channel_name(k): (v if isinstance(v, dict) else {"is_live": bool(v), "started_at": None})
            for k, v in data.items() if clean_channel_name(k)
        }
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(clean_data, f, indent=4)
    except Exception:
        pass

live_status_cache = load_live_cache()

def is_channel_live(channel: str) -> bool:
    info = live_status_cache.get(clean_channel_name(channel))
    return bool(info.get("is_live", False)) if isinstance(info, dict) else bool(info)

def get_channel_started_at(channel: str) -> Optional[float]:
    info = live_status_cache.get(clean_channel_name(channel))
    return info.get("started_at") if isinstance(info, dict) else None

def set_channel_status(channel: str, status: bool, started_at: Optional[float] = None):
    c_ch = clean_channel_name(channel)
    current = live_status_cache.get(c_ch)
    if status:
        final_start = started_at or (current.get("started_at") if isinstance(current, dict) else None) or time.time()
        live_status_cache[c_ch] = {"is_live": True, "started_at": final_start}
    else:
        live_status_cache[c_ch] = {"is_live": False, "started_at": None}

def load_dashboard_config() -> dict:
    if _memory_cache["dashboard"] is not None:
        return _memory_cache["dashboard"]
    if os.path.exists(DASHBOARD_FILE):
        try:
            with open(DASHBOARD_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    _memory_cache["dashboard"] = data
                    return data
        except Exception:
            pass
    return {}

def save_dashboard_config(data: dict):
    _memory_cache["dashboard"] = data
    try:
        with open(DASHBOARD_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
    except Exception:
        pass

def load_stats() -> dict:
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
    return {}

def save_stats(data: dict):
    try:
        with open(STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception:
        pass

def rebuild_leaderboard_from_local_stats():
    global cached_leaderboard_data, cached_global_stats
    stats = load_stats()
    if not stats:
        return

    main_guild = bot.guilds[0] if bot.guilds else None
    valid_users = []

    for norm_key, u_data in stats.items():
        if norm_key in EXCLUDED_USERS:
            continue
        shots = int(u_data.get("shots", 0))
        screens = int(u_data.get("screens", 0))
        gifs = int(u_data.get("gifs", 0))
        if (shots + screens + gifs) <= 0:
            continue

        avatar_url = u_data.get("avatar") or "https://cdn.discordapp.com/embed/avatars/0.png"
        display_name = u_data.get("name", norm_key)

        for cand_id in u_data.get("user_ids", []):
            if str(cand_id).isdigit() and main_guild:
                member = main_guild.get_member(int(cand_id))
                if member:
                    display_name = member.display_name
                    if member.display_avatar:
                        avatar_url = str(member.display_avatar.url)
                    break

        score = (shots * 3) + (gifs * 2) + screens
        valid_users.append({
            "name": display_name,
            "avatar": avatar_url,
            "shots": shots,
            "screens": screens,
            "gifs": gifs,
            "score": score
        })

    valid_users.sort(key=lambda x: (x["score"], x["shots"], x["gifs"], x["screens"]), reverse=True)
    cached_leaderboard_data = [dict(u, rank=idx) for idx, u in enumerate(valid_users[:25], start=1)]

    u_shots = sum(x["shots"] for x in valid_users)
    u_screens = sum(x["screens"] for x in valid_users)
    u_gifs = sum(x["gifs"] for x in valid_users)

    cached_global_stats = {
        "shots": max(cached_global_stats.get("shots", 1), u_shots, len(load_archive()), 1),
        "screens": max(cached_global_stats.get("screens", 1), u_screens, 1),
        "gifs": max(cached_global_stats.get("gifs", 1), u_gifs, 1),
        "intro_video_url": INTRO_VIDEO_URL
    }

def record_activity(user_id: int, user_name: str, activity_type: str, seconds: int = 0, avatar_url: Optional[str] = None, channel_name: str = ""):
    global cached_global_stats
    if bot.user and user_id == bot.user.id:
        if activity_type == "shot":
            cached_global_stats["shots"] = cached_global_stats.get("shots", 1) + 1
        elif activity_type == "screen":
            cached_global_stats["screens"] = cached_global_stats.get("screens", 1) + 1
        elif activity_type == "gif":
            cached_global_stats["gifs"] = cached_global_stats.get("gifs", 1) + 1
        return

    stats = load_stats()
    norm_key = normalize_name(user_name) or str(user_id)

    if norm_key not in stats:
        stats[norm_key] = {
            "name": user_name,
            "user_ids": [str(user_id)],
            "avatar": avatar_url or "https://cdn.discordapp.com/embed/avatars/0.png",
            "shots": 0,
            "screens": 0,
            "gifs": 0,
            "total_seconds": 0
        }
    stats[norm_key]["name"] = user_name
    if avatar_url:
        stats[norm_key]["avatar"] = avatar_url
    u_ids = set(stats[norm_key].get("user_ids", []))
    u_ids.add(str(user_id))
    stats[norm_key]["user_ids"] = list(u_ids)

    if activity_type == "shot":
        stats[norm_key]["shots"] = stats[norm_key].get("shots", 0) + 1
        stats[norm_key]["total_seconds"] = stats[norm_key].get("total_seconds", 0) + seconds
        cached_global_stats["shots"] = cached_global_stats.get("shots", 1) + 1
        push_activity_event(user_name, f"wyciął nowego shota 1080p na serwerze Discord! (Łącznie: 🎬 {stats[norm_key]['shots']})", "+3 PKT")
    elif activity_type == "screen":
        stats[norm_key]["screens"] = stats[norm_key].get("screens", 0) + 1
        cached_global_stats["screens"] = cached_global_stats.get("screens", 1) + 1
        push_activity_event(user_name, f"zapisał nowy zrzut ekranu PNG HD! (Łącznie: 📸 {stats[norm_key]['screens']})", "+1 PKT")
    elif activity_type == "gif":
        stats[norm_key]["gifs"] = stats[norm_key].get("gifs", 0) + 1
        cached_global_stats["gifs"] = cached_global_stats.get("gifs", 1) + 1
        push_activity_event(user_name, f"stworzył nową animację GIF HD ze streama! (Łącznie: 🎞️ {stats[norm_key]['gifs']})", "+2 PKT", style="purple-line")

    save_stats(stats)
    rebuild_leaderboard_from_local_stats()

def load_archive() -> list:
    if _memory_cache["archive"] is not None:
        return _memory_cache["archive"]
    if os.path.exists(ARCHIVE_FILE):
        try:
            with open(ARCHIVE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    _memory_cache["archive"] = data
                    return data
        except Exception:
            pass
    return []

def save_archive(data: list):
    _memory_cache["archive"] = data
    try:
        with open(ARCHIVE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception:
        pass

def add_shot_to_archive(shot_id: str, channel: str, author_id: int, author_name: str, duration: int, quality: str, description: str, message_jump_url: str, media_url: str):
    archive = load_archive()
    item = {
        "id": shot_id,
        "channel": clean_channel_name(channel),
        "author_id": author_id,
        "author_name": author_name,
        "duration": duration,
        "quality": quality,
        "description": description or "",
        "jump_url": message_jump_url,
        "media_url": media_url,
        "timestamp": time.time(),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    archive.append(item)
    if len(archive) > 1200:
        archive = archive[-1200:]
    save_archive(archive)
    if cached_search_clips:
        cached_search_clips.insert(0, item)

def update_shot_description_in_archive(message_jump_url: str, new_desc: str):
    archive = load_archive()
    for item in archive:
        if item.get("jump_url") == message_jump_url:
            item["description"] = new_desc
            save_archive(archive)
            break
    for item in cached_search_clips:
        if item.get("jump_url") == message_jump_url:
            item["description"] = new_desc
            break

def count_session_shots_in_archive(channel: str, started_at: float) -> int:
    clean_ch = clean_channel_name(channel)
    return sum(1 for item in load_archive() if item.get("channel") == clean_ch and item.get("timestamp", 0) >= started_at)

def get_target_channel_for_type(guild: discord.Guild, media_type: str) -> Optional[discord.TextChannel]:
    dash_config = load_dashboard_config()
    shot_channel_id = dash_config.get("shot_channel_id")

    if media_type == "screen":
        ch = discord.utils.find(lambda c: any(k in c.name.lower() for k in ["screen", "zdję", "zrzut"]), guild.text_channels)
        if ch:
            return ch
    elif media_type == "gif":
        ch = discord.utils.find(lambda c: "gif" in c.name.lower(), guild.text_channels)
        if ch:
            return ch
    elif media_type == "shot":
        ch = discord.utils.find(lambda c: any(k in c.name.lower() for k in ["shot", "klip"]), guild.text_channels)
        if ch:
            return ch

    if shot_channel_id:
        target = guild.get_channel(int(shot_channel_id))
        if target:
            return target
    return discord.utils.find(lambda c: any(k in c.name.lower() for k in ["shot", "screen", "gif", "klip"]), guild.text_channels)

def load_alerts() -> dict:
    global cached_channel_list
    if _memory_cache["alerts"] is not None:
        return _memory_cache["alerts"]

    alerts = {}
    if DEFAULT_CHANNELS_RAW and DEFAULT_CHANNEL_ID:
        try:
            cid = int(DEFAULT_CHANNEL_ID.strip())
            for ch in DEFAULT_CHANNELS_RAW.split(","):
                clean = clean_channel_name(ch)
                if clean:
                    alerts[clean] = cid
        except ValueError:
            pass

    if os.path.exists(ALERTS_FILE):
        try:
            with open(ALERTS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    for k, v in data.items():
                        cleaned_k = clean_channel_name(k)
                        if cleaned_k:
                            alerts[cleaned_k] = v
        except Exception:
            pass

    cached_channel_list = sorted(list(alerts.keys()))
    _memory_cache["alerts"] = alerts
    return alerts

def save_alerts(data: dict):
    global cached_channel_list
    try:
        clean_data = {clean_channel_name(k): v for k, v in data.items() if clean_channel_name(k)}
        with open(ALERTS_FILE, "w", encoding="utf-8") as f:
            json.dump(clean_data, f, indent=4, ensure_ascii=False)
        _memory_cache["alerts"] = clean_data
        cached_channel_list = sorted(list(clean_data.keys()))
    except Exception:
        pass

async def channel_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    current_low = current.strip().lower()
    choices = []
    for ch in cached_channel_list:
        login = get_channel_login(ch)
        plat = "Twitch" if get_platform(ch) == "twitch" else "Kick"
        icon = "🟣" if plat == "Twitch" else "🟢"
        label = f"{icon} {login} ({plat})"
        if not current_low or current_low in login or current_low in ch:
            choices.append(app_commands.Choice(name=label[:100], value=ch))

    if current_low and not choices:
        clean_curr = clean_channel_name(current)
        if clean_curr:
            login = get_channel_login(clean_curr)
            choices.append(app_commands.Choice(name=f"🟢 {login} (Kick)", value=login))
            choices.append(app_commands.Choice(name=f"🟣 {login} (Twitch)", value=f"tw.{login}"))
    return choices[:25]

async def get_live_hls_url(channel: str, force_fresh: bool = False) -> Optional[str]:
    clean_ch = clean_channel_name(channel)
    plat = get_platform(clean_ch)
    web_url = get_channel_web_url(clean_ch)

    meta = channel_meta_cache.get(clean_ch, {})
    if not force_fresh and meta.get("playback_url") and (time.time() - meta.get("url_time", 0) < 25):
        return meta.get("playback_url")

    if plat == "kick":
        is_live, _, _, _ = await fetch_kick_channel_meta(clean_ch, force_fresh=True)
        if is_live:
            fresh_url = channel_meta_cache.get(clean_ch, {}).get("playback_url")
            if fresh_url:
                return fresh_url

    cmd = ["streamlink", web_url, "best", "--stream-url", "--http-header", f"User-Agent={USER_AGENT}"]
    if plat == "twitch":
        cmd.append("--twitch-disable-ads")
    else:
        cmd.extend(["--http-header", "Referer=https://kick.com/"])

    code, stdout, _ = await run_cmd_async(cmd, timeout=10.0, capture_stdout=True)
    if code == 0 and stdout:
        stream_url = stdout.decode("utf-8", errors="ignore").strip()
        if stream_url.startswith("http"):
            channel_meta_cache.setdefault(clean_ch, {})["playback_url"] = stream_url
            channel_meta_cache[clean_ch]["url_time"] = time.time()
            return stream_url
    return None

# ==============================================================================
# 3. DOKŁADNE ZLICZANIE RANKINGU HALL OF FAME Z KANAŁÓW SHOTERSKI, SCREEN, GIF
# ==============================================================================

def extract_button_meta_from_message(msg: discord.Message) -> Tuple[Optional[str], Optional[str]]:
    """Wyciąga nazwę streamera oraz ID autora zapisane w custom_id przycisków pod shotem."""
    found_channel = None
    found_author_id = None
    try:
        for row in getattr(msg, "components", []) or []:
            for child in getattr(row, "children", []) or []:
                cid = getattr(child, "custom_id", "") or ""
                if cid.startswith("shot_cont:"):
                    parts = cid.split(":")
                    if len(parts) >= 5:
                        found_channel = clean_channel_name(parts[1])
                        if parts[4].isdigit():
                            found_author_id = parts[4]
                elif cid.startswith(("shot_rewind:", "shot_trim:", "shot_trim_end:")):
                    parts = cid.split(":")
                    if len(parts) >= 7:
                        found_channel = found_channel or clean_channel_name(parts[1])
                        cand_uid = parts[6] if cid.startswith("shot_rewind:") else parts[5]
                        if cand_uid.isdigit():
                            found_author_id = found_author_id or cand_uid
                elif cid.startswith("shot_desc:"):
                    parts = cid.split(":")
                    if len(parts) >= 2 and parts[1].isdigit():
                        found_author_id = found_author_id or parts[1]
    except Exception:
        pass
    return found_channel, found_author_id

async def refresh_complete_global_stats(history_limit: int = 2500):
    global cached_global_stats, cached_leaderboard_data

    aggregated: Dict[str, dict] = {}
    known_avatars: Dict[str, str] = {}
    known_user_ids: Dict[str, str] = {}
    uid_to_norm_map: Dict[str, str] = {}
    recent_history_events: List[Tuple[float, dict]] = []

    global_shots_count = 0
    global_screens_count = 0
    global_gifs_count = 0

    main_guild = bot.guilds[0] if bot.guilds else None
    if main_guild:
        for m in main_guild.members:
            if not m.bot:
                n_disp = normalize_name(m.display_name)
                n_user = normalize_name(m.name)
                uid_str = str(m.id)
                av_str = str(m.display_avatar.url) if m.display_avatar else None
                if n_disp:
                    known_user_ids[n_disp] = uid_str
                    uid_to_norm_map[uid_str] = n_disp
                    if av_str:
                        known_avatars[n_disp] = av_str
                if n_user:
                    known_user_ids[n_user] = uid_str
                    if av_str:
                        known_avatars[n_user] = av_str
                if av_str:
                    known_avatars[uid_str] = av_str

    def resolve_member_by_uid(uid: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if not uid or not str(uid).isdigit() or not main_guild:
            return None, None
        m = main_guild.get_member(int(uid))
        if m and not m.bot:
            return m.display_name, (str(m.display_avatar.url) if m.display_avatar else None)
        return None, None

    def add_entry(uid: Optional[str], uname: Optional[str], avatar_url: Optional[str], act: str, secs: int = 0, msg_ts: float = 0, msg_id_str: str = ""):
        if uid and (not uname or uname.isdigit()):
            m_name, m_av = resolve_member_by_uid(uid)
            if m_name:
                uname = m_name
            if m_av and not avatar_url:
                avatar_url = m_av

        if not uname and not uid:
            return

        resolved_name = (uname or str(uid)).strip()
        low = resolved_name.lower()

        if bot.user and ((uid and str(uid) == str(bot.user.id)) or low == bot.user.name.lower()):
            return
        if any(bad in low for bad in ["kik-bot", "automatyczny", "ziemia 2.0"]):
            return

        norm = normalize_name(resolved_name)
        if uid and str(uid) in uid_to_norm_map:
            norm = uid_to_norm_map[str(uid)]
        if not norm or norm in EXCLUDED_USERS:
            return

        if uid and str(uid).isdigit():
            known_user_ids[norm] = str(uid)
            uid_to_norm_map[str(uid)] = norm

        clean_av = avatar_url
        if clean_av and ("ephemeral-attachments" in clean_av or "embed/avatars" in clean_av):
            clean_av = None
        if clean_av:
            known_avatars[norm] = clean_av
            if uid:
                known_avatars[str(uid)] = clean_av

        best_av = clean_av or known_avatars.get(norm) or (known_avatars.get(str(uid)) if uid else None) or "https://cdn.discordapp.com/embed/avatars/0.png"

        if norm not in aggregated:
            aggregated[norm] = {
                "name": resolved_name,
                "user_ids": set(),
                "avatar": best_av,
                "shots": 0,
                "screens": 0,
                "gifs": 0,
                "total_seconds": 0
            }

        if uname and not uname.isdigit():
            aggregated[norm]["name"] = uname
        if best_av and "embed/avatars" not in best_av:
            aggregated[norm]["avatar"] = best_av
        if uid and str(uid).isdigit():
            aggregated[norm]["user_ids"].add(str(uid))

        t_str = datetime.fromtimestamp(msg_ts).strftime("%H:%M:%S") if msg_ts > 0 else "ONLINE"
        if act == "shot":
            aggregated[norm]["shots"] += 1
            aggregated[norm]["total_seconds"] += secs
            if msg_ts > 0:
                recent_history_events.append((msg_ts, {
                    "id": f"hist_{msg_id_str}", "time": t_str, "user": aggregated[norm]["name"],
                    "text": "wyciął nowego shota 1080p na serwerze Discord!", "pts": "+3 PKT", "style": ""
                }))
        elif act == "screen":
            aggregated[norm]["screens"] += 1
            if msg_ts > 0:
                recent_history_events.append((msg_ts, {
                    "id": f"hist_{msg_id_str}", "time": t_str, "user": aggregated[norm]["name"],
                    "text": "zapisał nowy zrzut ekranu PNG HD!", "pts": "+1 PKT", "style": ""
                }))
        elif act == "gif":
            aggregated[norm]["gifs"] += 1
            if msg_ts > 0:
                recent_history_events.append((msg_ts, {
                    "id": f"hist_{msg_id_str}", "time": t_str, "user": aggregated[norm]["name"],
                    "text": "stworzył nową animację GIF HD ze streama!", "pts": "+2 PKT", "style": "purple-line"
                }))

    seen_message_ids = set()
    seen_jump_urls = set()
    media_keywords = ["shot", "klip", "screen", "zdję", "zdje", "zrzut", "gif", "spam"]

    for guild in bot.guilds:
        for channel in guild.text_channels:
            c_name = channel.name.lower()
            if not any(k in c_name for k in media_keywords) or "kompilac" in c_name:
                continue

            try:
                perms = channel.permissions_for(guild.me)
                if not perms.read_messages or not perms.read_message_history:
                    continue

                msg_counter = 0
                async for msg in channel.history(limit=history_limit):
                    if msg.id in seen_message_ids:
                        continue
                    seen_message_ids.add(msg.id)
                    if msg.jump_url:
                        seen_jump_urls.add(msg.jump_url)

                    msg_counter += 1
                    if msg_counter % 250 == 0:
                        await asyncio.sleep(0.12)

                    msg_ts = msg.created_at.timestamp()

                    # 1. Wiadomości wysłane bezpośrednio przez użytkowników (z załącznikami)
                    if not msg.author.bot:
                        u_id = str(msg.author.id)
                        u_name = msg.author.display_name
                        u_av = str(msg.author.display_avatar.url) if msg.author.display_avatar else None
                        n_u = normalize_name(u_name)
                        if n_u:
                            known_user_ids[n_u] = u_id
                            uid_to_norm_map[u_id] = n_u
                            if u_av:
                                known_avatars[n_u] = u_av
                                known_avatars[u_id] = u_av

                        for att in msg.attachments:
                            fn = att.filename.lower().split("?")[0]
                            ct = (att.content_type or "").lower()
                            if fn.endswith((".mp4", ".mov", ".m4v", ".webm")) or "video" in ct:
                                global_shots_count += 1
                                add_entry(u_id, u_name, u_av, "shot", 30, msg_ts, str(msg.id))
                                break
                            elif fn.endswith(".gif") or "gif" in ct:
                                global_gifs_count += 1
                                add_entry(u_id, u_name, u_av, "gif", 0, msg_ts, str(msg.id))
                                break
                            elif fn.endswith((".jpg", ".jpeg", ".png", ".webp")) or "image" in ct:
                                global_screens_count += 1
                                add_entry(u_id, u_name, u_av, "screen", 0, msg_ts, str(msg.id))
                                break

                    # 2. Wiadomości wygenerowane przez bota (shoty, screeny, gify, skonwertowane klipy)
                    else:
                        detected_type = None
                        for att in msg.attachments:
                            fn = att.filename.lower().split("?")[0]
                            ct = (att.content_type or "").lower()
                            if fn.endswith((".mp4", ".mov", ".m4v", ".webm")) or "video" in ct:
                                detected_type = "shot"
                                break
                            elif fn.endswith(".gif") or "gif" in ct:
                                detected_type = "gif"
                                break
                            elif fn.endswith((".jpg", ".jpeg", ".png", ".webp")) or "image" in ct:
                                detected_type = "screen"
                                break

                        emb = msg.embeds[0] if msg.embeds else None
                        t_low = (emb.title or "").lower() if emb else ""
                        d_low = (emb.description or "").lower() if emb else ""
                        f_text = (emb.footer.text or "") if (emb and emb.footer) else ""

                        # Pomiń panele kontrolne i wyniki wyszukiwania
                        if any(skip_w in t_low for skip_w in ["panel", "studio", "hall of fame", "wyniki wyszukiwania", "właśnie odpalił", "zakończył", "dobiegła końca", "najważniejsze funkcje"]):
                            continue

                        if not detected_type and emb:
                            if "gif" in t_low or "animacj" in t_low or "🎞️" in t_low:
                                detected_type = "gif"
                            elif "screen" in t_low or "zrzut" in t_low or "📸" in t_low:
                                detected_type = "screen"
                            elif "shot" in t_low or "klip" in t_low or "🎬" in t_low or "⏱️" in d_low:
                                detected_type = "shot"

                        if not detected_type:
                            continue

                        if detected_type == "shot":
                            global_shots_count += 1
                        elif detected_type == "screen":
                            global_screens_count += 1
                        elif detected_type == "gif":
                            global_gifs_count += 1

                        # Jeśli to automatyczny zrzut/shot/gif bota -> nie przypisujemy punktów człowiekowi
                        if "automatyczn" in t_low or "automatyczn" in f_text.lower():
                            continue

                        real_uid = None
                        real_author = None
                        real_av = None

                        # Warstwa A: Metadane interakcji Discorda
                        inter_user = None
                        if getattr(msg, "interaction_metadata", None) and getattr(msg.interaction_metadata, "user", None):
                            inter_user = msg.interaction_metadata.user
                        elif getattr(msg, "interaction", None) and getattr(msg.interaction, "user", None):
                            inter_user = msg.interaction.user

                        if inter_user and not inter_user.bot:
                            real_uid = str(inter_user.id)
                            real_author = inter_user.display_name
                            real_av = str(inter_user.display_avatar.url) if inter_user.display_avatar else None

                        # Warstwa B: ID autora zaszyte w przyciskach pod shotem
                        if not real_uid:
                            _, btn_uid = extract_button_meta_from_message(msg)
                            if btn_uid:
                                real_uid = btn_uid
                                m_name, m_av = resolve_member_by_uid(btn_uid)
                                if m_name:
                                    real_author = m_name
                                if m_av:
                                    real_av = m_av

                        # Warstwa C: Stopka embeda ("Wycięte przez: Nick" / "Przez: Nick")
                        if not real_author and f_text:
                            m_auth = re.search(r"(?:wycięte przez|montaż|przez|dla)\s*:\s*([^•|\n\r]+)", f_text, re.IGNORECASE)
                            if m_auth:
                                cand = m_auth.group(1).strip()
                                if cand.lower() not in ["ziemia 2.0", "kik-bot", "automatyczny shot", "automatyczny"]:
                                    real_author = cand

                        if not real_av and emb and emb.footer and emb.footer.icon_url:
                            real_av = str(emb.footer.icon_url)

                        if real_uid or real_author:
                            add_entry(real_uid, real_author, real_av, detected_type, 30 if detected_type == "shot" else 0, msg_ts, str(msg.id))

                await asyncio.sleep(0.15)
            except Exception as e:
                print(f"[SCAN WARN {channel.name}]: {e}")

    # Uzupełnienie z pliku lokalnego archiwum (dla pewności)
    for item in load_archive():
        j_url = item.get("jump_url")
        if j_url and j_url in seen_jump_urls:
            continue
        global_shots_count += 1
        a_id = str(item.get("author_id", ""))
        a_name = item.get("author_name", "")
        if bot.user and a_id == str(bot.user.id):
            continue
        add_entry(a_id, a_name, None, "shot", item.get("duration", 30), item.get("timestamp", 0), item.get("id", ""))

    valid_users = []
    new_persisted_stats = {}

    for norm_name, user_data in list(aggregated.items()):
        if norm_name in EXCLUDED_USERS:
            continue
        uid_to_fetch = known_user_ids.get(norm_name) or (list(user_data.get("user_ids", []))[0] if user_data.get("user_ids") else None)
        if uid_to_fetch and str(uid_to_fetch).isdigit() and main_guild:
            member = main_guild.get_member(int(uid_to_fetch))
            if member:
                if member.display_avatar:
                    user_data["avatar"] = str(member.display_avatar.url)
                user_data["name"] = member.display_name
        valid_users.append(user_data)
        new_persisted_stats[norm_name] = {
            "name": user_data["name"],
            "user_ids": list(user_data.get("user_ids", [])),
            "avatar": user_data.get("avatar") or "https://cdn.discordapp.com/embed/avatars/0.png",
            "shots": user_data.get("shots", 0),
            "screens": user_data.get("screens", 0),
            "gifs": user_data.get("gifs", 0),
            "total_seconds": user_data.get("total_seconds", 0)
        }

    if new_persisted_stats:
        save_stats(new_persisted_stats)

    def calc_score(item):
        return (item.get("shots", 0) * 3) + (item.get("gifs", 0) * 2) + item.get("screens", 0)

    active_users = [u for u in valid_users if (u["shots"] + u["screens"] + u["gifs"]) > 0]
    sorted_users = sorted(active_users, key=lambda x: (calc_score(x), x["shots"], x["gifs"], x["screens"]), reverse=True)

    cached_leaderboard_data = [
        {
            "rank": idx,
            "name": u["name"],
            "avatar": u.get("avatar") or "https://cdn.discordapp.com/embed/avatars/0.png",
            "shots": u["shots"],
            "screens": u["screens"],
            "gifs": u["gifs"],
            "score": calc_score(u)
        }
        for idx, u in enumerate(sorted_users[:25], start=1)
    ]

    if recent_history_events:
        recent_history_events.sort(key=lambda x: x[0], reverse=True)
        for _, ev in recent_history_events[:10]:
            push_activity_event(ev["user"], ev["text"], ev["pts"], ev["style"], event_id=ev["id"], time_str=ev["time"])

    users_shots = sum(item["shots"] for item in sorted_users)
    users_screens = sum(item["screens"] for item in sorted_users)
    users_gifs = sum(item["gifs"] for item in sorted_users)

    cached_global_stats = {
        "shots": max(global_shots_count, users_shots, 1),
        "screens": max(global_screens_count, users_screens, 1),
        "gifs": max(global_gifs_count, users_gifs, 1),
        "intro_video_url": INTRO_VIDEO_URL
    }
    gc.collect()

# ==============================================================================
# 4. SKANOWANIE WSZYSTKICH FILMÓW KANAŁU SHOTERSKIEGO DLA KOMENDY /SZUKAJ
# ==============================================================================

def extract_streamer_and_meta_from_shot_msg(msg: discord.Message, att_filename: str, archive_by_jump: Dict[str, dict]) -> Tuple[str, str, int, str]:
    if msg.jump_url in archive_by_jump:
        arch_item = archive_by_jump[msg.jump_url]
        ch = clean_channel_name(arch_item.get("channel", ""))
        if ch:
            return ch, (arch_item.get("description") or "Shot 1080p"), int(arch_item.get("duration", 30)), (arch_item.get("author_name") or msg.author.display_name)

    streamer_name, _ = extract_button_meta_from_message(msg)
    opis = ""
    duration = 30
    author_name = msg.author.display_name

    if msg.embeds:
        emb = msg.embeds[0]
        t = emb.title or ""
        d = emb.description or ""

        if not streamer_name:
            m_tw = re.search(r"twitch\.tv/([a-zA-Z0-9_\-]+)", d, re.IGNORECASE)
            m_ki = re.search(r"kick\.com/([a-zA-Z0-9_\-]+)", d, re.IGNORECASE)
            if m_tw:
                streamer_name = clean_channel_name(m_tw.group(1), force_platform="twitch")
            elif m_ki:
                streamer_name = clean_channel_name(m_ki.group(1))
            elif ":" in t:
                cand = t.split(":", 1)[1].split("(")[0].split("[")[0].strip()
                if cand and not any(w in cand.lower() for w in ["1080p", "full hd", "live"]):
                    streamer_name = clean_channel_name(cand, force_platform="twitch" if ("twitch" in t.lower() or "🟣" in t) else None)

        if "💬 *\"" in d:
            s_idx = d.find("💬 *\"") + 5
            e_idx = d.find("\"*", s_idx)
            if e_idx != -1:
                opis = d[s_idx:e_idx]
        elif t and not t.lower().startswith("🎬 shot"):
            opis = t.replace("🎬", "").strip()

        m_dur = re.search(r"⏱️\s*\*\*(\d+)s\*\*", d)
        if m_dur:
            duration = int(m_dur.group(1))

        if emb.footer and emb.footer.text:
            m_auth = re.search(r"(?:wycięte przez|przez)\s*:\s*([^•|\n\r]+)", emb.footer.text, re.IGNORECASE)
            if m_auth:
                author_name = m_auth.group(1).strip()

    if not streamer_name and att_filename:
        base_fn = att_filename.rsplit(".", 1)[0].lower()
        for prefix in ("shot_", "clip_"):
            if base_fn.startswith(prefix):
                cand_fn = base_fn[len(prefix):]
                if cand_fn and cand_fn not in ["1080p", "kick", "twitch", "sub", "cut"]:
                    streamer_name = clean_channel_name(cand_fn)
                    break

    return (streamer_name or "inne"), (opis or f"Shot {duration}s"), duration, author_name

async def fetch_all_channel_shots(guild: Optional[discord.Guild], force_rescan: bool = False) -> List[dict]:
    global cached_search_clips, last_search_scan_time
    if not force_rescan and cached_search_clips and (time.time() - last_search_scan_time < 180):
        return cached_search_clips

    archive = load_archive()
    archive_by_jump = {item.get("jump_url"): item for item in archive if item.get("jump_url")}
    clips = []
    seen_jumps = set()
    seen_media = set()

    if guild:
        dash_config = load_dashboard_config()
        shot_ch_id = str(dash_config.get("shot_channel_id", ""))
        target_channels = [
            ch for ch in guild.text_channels
            if (str(ch.id) == shot_ch_id or any(k in ch.name.lower() for k in ["shot", "klip"])) and "kompilac" not in ch.name.lower()
        ]

        for target_ch in target_channels:
            try:
                perms = target_ch.permissions_for(guild.me)
                if not perms.read_messages or not perms.read_message_history:
                    continue

                m_idx = 0
                async for msg in target_ch.history(limit=1200):
                    m_idx += 1
                    if m_idx % 250 == 0:
                        await asyncio.sleep(0.1)

                    video_url = None
                    att_fn = ""
                    for att in msg.attachments:
                        fn_clean = att.filename.lower().split("?")[0]
                        if fn_clean.endswith((".mp4", ".mov", ".m4v", ".webm")):
                            video_url = att.url
                            att_fn = fn_clean
                            break

                    if video_url:
                        base_m = video_url.split("?")[0]
                        if base_m in seen_media or msg.jump_url in seen_jumps:
                            continue
                        seen_media.add(base_m)
                        seen_jumps.add(msg.jump_url)

                        streamer_name, opis, duration, author_name = extract_streamer_and_meta_from_shot_msg(msg, att_fn, archive_by_jump)
                        clips.append({
                            "id": str(msg.id)[-6:],
                            "channel": streamer_name,
                            "author_name": author_name,
                            "duration": duration,
                            "description": opis,
                            "jump_url": msg.jump_url,
                            "media_url": video_url,
                            "timestamp": msg.created_at.timestamp(),
                            "created_at": msg.created_at.isoformat()
                        })
            except Exception as e:
                print(f"[SEARCH SCAN ERR {target_ch.name}]: {e}")

    for item in archive:
        j_url = item.get("jump_url", "")
        m_url = (item.get("media_url") or "").split("?")[0]
        if (j_url and j_url in seen_jumps) or (m_url and m_url in seen_media):
            continue
        if j_url or m_url:
            if j_url:
                seen_jumps.add(j_url)
            if m_url:
                seen_media.add(m_url)
            clips.append(item)

    clips.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
    cached_search_clips = clips
    last_search_scan_time = time.time()
    return clips

# ==============================================================================
# 5. SILNIK NAGRYWANIA (BEZ WYKWITÓW / ARTEFAKTÓW NA SHOTACH)
# ==============================================================================

async def record_buffered_clip(channel: str, out_file: str, duration_sec: int, back_sec: int, jakosc: str = "1080p", with_logo: bool = True) -> bool:
    clean_ch = clean_channel_name(channel)
    plat = get_platform(clean_ch)
    web_url = get_channel_web_url(clean_ch)
    raw_ts = os.path.join(BASE_DIR, f"raw_{uuid.uuid4().hex[:8]}.ts")

    dump_duration = duration_sec + (2 if duration_sec <= 12 else 1)

    if plat == "kick":
        await fetch_kick_channel_meta(clean_ch, force_fresh=True)
        api_playback_url = channel_meta_cache.get(clean_ch, {}).get("playback_url")

        if api_playback_url:
            sl_m3u8_cmd = [
                "streamlink", api_playback_url, "best", "-o", raw_ts,
                "--hls-live-edge", "16", "--hls-segment-threads", "2", "--hls-duration", str(dump_duration),
                "--http-header", f"User-Agent={USER_AGENT}", "--http-header", "Referer=https://kick.com/", "--http-header", "Origin=https://kick.com"
            ]
            await run_cmd_async(sl_m3u8_cmd, timeout=dump_duration + 20.0)

        if not os.path.exists(raw_ts) or os.path.getsize(raw_ts) < 40000:
            sl_web_cmd = [
                "streamlink", web_url, "best", "-o", raw_ts,
                "--hls-live-edge", "16", "--hls-segment-threads", "2", "--hls-duration", str(dump_duration),
                "--http-header", f"User-Agent={USER_AGENT}", "--http-header", "Referer=https://kick.com/", "--http-header", "Origin=https://kick.com"
            ]
            await run_cmd_async(sl_web_cmd, timeout=dump_duration + 22.0)

        if (not os.path.exists(raw_ts) or os.path.getsize(raw_ts) < 40000) and api_playback_url:
            headers_arg = f"User-Agent: {USER_AGENT}\r\nReferer: https://kick.com/\r\nOrigin: https://kick.com\r\n"
            ff_dump_cmd = [
                "ffmpeg", "-nostdin", "-y", "-headers", headers_arg,
                "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "3",
                "-i", api_playback_url, "-map", "0:v:0", "-map", "0:a:0?", "-t", str(dump_duration), "-c", "copy", "-f", "mpegts", raw_ts
            ]
            await run_cmd_async(ff_dump_cmd, timeout=dump_duration + 20.0)
    else:
        sl_dump_cmd = [
            "streamlink", web_url, "best", "-o", raw_ts, "--twitch-disable-ads",
            "--hls-live-edge", "16", "--hls-segment-threads", "2", "--hls-duration", str(dump_duration),
            "--http-header", f"User-Agent={USER_AGENT}"
        ]
        await run_cmd_async(sl_dump_cmd, timeout=dump_duration + 22.0)

        if not os.path.exists(raw_ts) or os.path.getsize(raw_ts) < 40000:
            hls_url = await get_live_hls_url(clean_ch, force_fresh=True)
            if hls_url:
                ff_dump_cmd = [
                    "ffmpeg", "-nostdin", "-y", "-headers", f"User-Agent: {USER_AGENT}\r\n",
                    "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "3",
                    "-i", hls_url, "-map", "0:v:0", "-map", "0:a:0?", "-t", str(dump_duration), "-c", "copy", "-f", "mpegts", raw_ts
                ]
                await run_cmd_async(ff_dump_cmd, timeout=dump_duration + 20.0)

    if not os.path.exists(raw_ts) or os.path.getsize(raw_ts) < 40000:
        if os.path.exists(raw_ts):
            try:
                os.remove(raw_ts)
            except Exception:
                pass
        return False

    crf_val = "18" if jakosc == "1080p" else ("21" if jakosc == "480p" else "19")
    target_h = 1080 if jakosc == "1080p" else (480 if jakosc == "480p" else 720)
    scale_filter = f"fps=30,scale=-2:{target_h}:flags=lanczos,format=yuv420p"
    min_req_dur = 1.0 if duration_sec <= 10 else 2.5

    wm_w = 240 if target_h == 1080 else (165 if target_h == 720 else 115)
    wm_path = await asyncio.to_thread(get_prepared_logo_path, wm_w) if with_logo else None

    try:
        if wm_path and os.path.exists(wm_path) and with_logo:
            filter_complex = (
                f"[0:v:0]{scale_filter}[base];"
                f"[1:v:0]format=rgba,scale={wm_w}:-2:flags=lanczos[wm];"
                f"[base][wm]overlay=25:25:format=auto,format=yuv420p[v]"
            )
            process_cmd = [
                "ffmpeg", "-nostdin", "-y", "-threads", "2", "-fflags", "+genpts", "-ignore_unknown",
                "-i", raw_ts, "-i", wm_path, "-filter_complex", filter_complex,
                "-map", "[v]", "-map", "0:a?", "-t", str(duration_sec),
                "-c:v", "libx264", "-pix_fmt", "yuv420p",
                "-color_range", "tv", "-colorspace", "bt709", "-color_primaries", "bt709", "-color_trc", "bt709",
                "-preset", "veryfast", "-g", "30", "-crf", crf_val,
                "-maxrate", "2250k", "-bufsize", "4500k",
                "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-f", "mp4", out_file
            ]
            await run_cmd_async(process_cmd, timeout=90.0)
            if await is_valid_video_file_async(out_file, min_duration=min_req_dur):
                return True
            if os.path.exists(out_file):
                try:
                    os.remove(out_file)
                except Exception:
                    pass

        process_cmd = [
            "ffmpeg", "-nostdin", "-y", "-threads", "2", "-fflags", "+genpts",
            "-i", raw_ts, "-t", str(duration_sec), "-vf", scale_filter,
            "-c:v", "libx264", "-pix_fmt", "yuv420p",
            "-color_range", "tv", "-colorspace", "bt709",
            "-preset", "veryfast", "-g", "30", "-crf", crf_val,
            "-maxrate", "2250k", "-bufsize", "4500k",
            "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-f", "mp4", out_file
        ]
        await run_cmd_async(process_cmd, timeout=75.0)
        return await is_valid_video_file_async(out_file, min_duration=min_req_dur)
    except Exception as e:
        print(f"[LOCAL PROCESS ERR {clean_ch}]: {e}")
        return False
    finally:
        if os.path.exists(raw_ts):
            try:
                os.remove(raw_ts)
            except Exception:
                pass
        gc.collect()

async def async_download_and_process_clip(source_url: str, clip_web_url: str, out_file: str) -> bool:
    headers_arg = f"User-Agent: {USER_AGENT}\r\nReferer: https://kick.com/\r\nOrigin: https://kick.com\r\n"
    cmd_copy = [
        "ffmpeg", "-nostdin", "-y", "-threads", "2", "-headers", headers_arg,
        "-i", source_url, "-c", "copy", "-bsf:a", "aac_adtstoasc", "-movflags", "+faststart", "-f", "mp4", out_file
    ]
    await run_cmd_async(cmd_copy, timeout=35.0)
    if await is_valid_video_file_async(out_file, min_duration=1.0):
        return True
    if os.path.exists(out_file):
        try:
            os.remove(out_file)
        except Exception:
            pass
    cmd_transcode = [
        "ffmpeg", "-nostdin", "-y", "-threads", "2", "-headers", headers_arg,
        "-i", source_url, "-vf", "scale=-2:1080:flags=lanczos,format=yuv420p",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-f", "mp4", out_file
    ]
    await run_cmd_async(cmd_transcode, timeout=65.0)
    return await is_valid_video_file_async(out_file, min_duration=1.0)

async def resolve_and_download_clip(clip_url: str, clip_id: str, out_file: str):
    video_url = None
    title = "Klip 1080p"
    session = await get_session()
    try:
        async with session.get(f"https://kick.com/api/v2/clips/{clip_id}", timeout=aiohttp.ClientTimeout(total=4.5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                clip = data.get("clip", {})
                video_url = clip.get("video_url") or clip.get("clip_url")
                if clip.get("title"):
                    title = clip.get("title")
    except Exception:
        pass
    success = await async_download_and_process_clip(video_url or clip_url, clip_url, out_file)
    if success and os.path.exists(out_file):
        await ensure_file_under_discord_limit(out_file, max_mb=9.5)
        return True, title, video_url
    return False, "Nie udało się przetworzyć strumienia wideo.", None

async def resolve_and_download_twitch_clip(clip_url: str, slug: str, out_file: str) -> Tuple[bool, str, str]:
    title = "Klip 1080p"
    broadcaster = "Twitch"
    direct_mp4_url = None
    session = await get_session()
    gql_query = {
        "query": "query($slug: ID!) { clip(slug: $slug) { title broadcaster { login displayName } videoQualities { quality sourceURL } playbackAccessToken(params: {platform: \"web\", playerBackend: \"mediaplayer\", playerType: \"embed\"}) { signature value } } }",
        "variables": {"slug": slug}
    }
    headers = {"Client-ID": TWITCH_CLIENT_ID, "User-Agent": USER_AGENT, "Content-Type": "application/json"}
    try:
        async with session.post("https://gql.twitch.tv/gql", json=gql_query, headers=headers, timeout=aiohttp.ClientTimeout(total=5.0)) as resp:
            if resp.status == 200:
                data = await resp.json()
                clip_data = (data.get("data") or {}).get("clip")
                if clip_data:
                    title = clip_data.get("title") or title
                    b_info = clip_data.get("broadcaster") or {}
                    broadcaster = b_info.get("login") or b_info.get("displayName") or broadcaster
                    qualities = clip_data.get("videoQualities") or []
                    token_data = clip_data.get("playbackAccessToken") or {}
                    if qualities and token_data.get("signature") and token_data.get("value"):
                        src = qualities[0].get("sourceURL")
                        if src:
                            direct_mp4_url = f"{src}?sig={token_data['signature']}&token={urllib.parse.quote(token_data['value'])}"
    except Exception:
        pass

    if direct_mp4_url:
        try:
            async with session.get(direct_mp4_url, timeout=aiohttp.ClientTimeout(total=35.0)) as r:
                if r.status == 200:
                    with open(out_file, "wb") as f:
                        async for chunk in r.content.iter_chunked(65536):
                            f.write(chunk)
        except Exception:
            pass

    if not await is_valid_video_file_async(out_file, min_duration=1.0):
        await async_download_and_process_clip(direct_mp4_url or clip_url, clip_url, out_file)

    if await is_valid_video_file_async(out_file, min_duration=1.0):
        await ensure_file_under_discord_limit(out_file, max_mb=9.5)
        return True, title, broadcaster
    return False, "Nie udało się pobrać klipu.", broadcaster

# ==============================================================================
# 6. PANEL NA ŻYWO ORAZ WIDOKI SHOTA (PRZYWRÓCONY PANEL LIVE + BRAK REKLAM W SHOCIE)
# ==============================================================================

class StreamerSelect(Select):
    def __init__(self, options: List[discord.SelectOption]):
        super().__init__(
            placeholder="🎯 Wybierz streamera z transmisji LIVE...",
            options=options,
            custom_id="dash_select_streamer",
            row=0
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer()
        selected = self.values[0] if self.values else None
        embed, view = build_dashboard_content(selected_streamer=selected)
        await interaction.edit_original_response(embed=embed, view=view)

def get_current_dashboard_signature() -> str:
    alerts = load_alerts()
    parts = []
    for ch in sorted(alerts.keys()):
        clean_ch = clean_channel_name(ch)
        live = is_channel_live(clean_ch)
        meta = channel_meta_cache.get(clean_ch, {})
        parts.append(f"{clean_ch}:{int(live)}:{meta.get('category','')}:{meta.get('title','')[:25]}")
    return "|".join(parts)

def build_dashboard_content(selected_streamer: Optional[str] = None) -> Tuple[discord.Embed, View]:
    alerts = load_alerts()
    if not alerts:
        embed = discord.Embed(
            title="📡 PANEL TRANSMISJI NA ŻYWO – ZIEMIA 2.0",
            description="Brak skonfigurowanych kanałów do śledzenia. Dodaj je za pomocą `/alert_wlacz`.",
            color=0x808080
        )
        view = View(timeout=None)
        view.add_item(Button(label="🔄 Odśwież statusy", style=discord.ButtonStyle.secondary, custom_id="dash_refresh"))
        return embed, view

    online_channels = []
    offline_channels = []

    for ch in sorted(alerts.keys()):
        clean_ch = clean_channel_name(ch)
        disp = get_channel_display(clean_ch)
        plat = get_platform(clean_ch)
        badge_icon = "🟣" if plat == "twitch" else "🟢"
        if is_channel_live(clean_ch):
            meta = channel_meta_cache.get(clean_ch, {})
            title = (meta.get("title") or "Transmisja na żywo")[:45]
            category = (meta.get("category") or ("Twitch Stream" if plat == "twitch" else "Kick Stream"))[:20]
            online_channels.append({
                "key": clean_ch,
                "display": disp,
                "badge": badge_icon,
                "url": get_channel_web_url(clean_ch),
                "title": title,
                "category": category
            })
        else:
            offline_channels.append(f"{badge_icon}`{disp}`")

    embed = discord.Embed(
        title="📡 PANEL KICK & TWITCH NA ŻYWO – ZIEMIA 2.0",
        description=(
            f"Automatycznie monitorujemy **{len(alerts)}** kanałów (🟢 Kick & 🟣 Twitch).\n"
            "Wybierz twórcę z listy rozwijanej poniżej, aby aktywować szybkie kafelki akcji!"
        ),
        color=KICK_GREEN
    )

    if online_channels:
        live_lines = [
            f"🔴 {item['badge']} **[{item['display'].upper()}]({item['url']})**\n"
            f"└ 🎮 `{item['category']}` ▎ 📝 *{item['title']}*\n\n"
            for item in online_channels
        ]
        for i, chunk in enumerate(split_text_into_chunks(live_lines, max_chars=900)[:15]):
            field_name = f"🔴 TRANSMISJE NA ŻYWO ({len(online_channels)})" if i == 0 else f"🔴 NA ŻYWO (c.d. {i+1})"
            embed.add_field(name=field_name, value=chunk, inline=False)
    else:
        embed.add_field(name="🔴 TRANSMISJE NA ŻYWO (0)", value="*Aktualnie żaden z obserwowanych twórców nie prowadzi transmisji.*", inline=False)

    if offline_channels:
        off_chunks, curr = [], ""
        for tag in offline_channels:
            add = f"{tag}, "
            if len(curr) + len(add) > 900:
                off_chunks.append(curr.rstrip(", "))
                curr = add
            else:
                curr += add
        if curr:
            off_chunks.append(curr.rstrip(", "))
        for i, chunk in enumerate(off_chunks[:5]):
            field_name = f"⚪ OFFLINE ({len(offline_channels)})" if i == 0 else f"⚪ OFFLINE (c.d. {i+1})"
            embed.add_field(name=field_name, value=chunk, inline=False)

    embed.set_footer(text=f"Ostatnia aktualizacja: {time.strftime('%H:%M:%S')} • ZIEMIA 2.0")

    view = View(timeout=None)
    if online_channels:
        select_options = [
            discord.SelectOption(
                label=f"{item['badge']} {item['display']}",
                description=f"{item['category']} | {item['title'][:35]}",
                value=item["key"],
                default=(item["key"] == selected_streamer)
            )
            for item in online_channels[:25]
        ]
        view.add_item(StreamerSelect(options=select_options))

        active_target = selected_streamer if any(x["key"] == selected_streamer for x in online_channels) else online_channels[0]["key"]
        active_disp = get_channel_display(active_target)

        view.add_item(Button(label=f"🎬 Shot 30s: {active_disp}"[:80], style=discord.ButtonStyle.green, custom_id=f"dash_shot:{active_target}"[:100], row=1))
        view.add_item(Button(label=f"📸 Screen HD: {active_disp}"[:80], style=discord.ButtonStyle.secondary, custom_id=f"dash_scr:{active_target}"[:100], row=1))
        view.add_item(Button(label=f"🎞️ GIF HD: {active_disp}"[:80], style=discord.ButtonStyle.primary, custom_id=f"dash_gif:{active_target}"[:100], row=1))

    view.add_item(Button(label="🔄 Odśwież statusy", style=discord.ButtonStyle.secondary, custom_id="dash_refresh", row=2 if online_channels else 0))
    return embed, view

async def update_live_dashboard(selected_streamer: Optional[str] = None, force: bool = False):
    global last_dashboard_signature, last_dashboard_edit_time
    config = load_dashboard_config()
    if not config or "channel_id" not in config or "message_id" not in config:
        return

    new_sig = get_current_dashboard_signature()
    now = time.time()
    if not force and selected_streamer is None and new_sig == last_dashboard_signature and (now - last_dashboard_edit_time < 900):
        return

    try:
        channel = bot.get_channel(config["channel_id"]) or await bot.fetch_channel(config["channel_id"])
        if not channel:
            return
        embed, view = build_dashboard_content(selected_streamer)
        main_msg = await channel.fetch_message(config["message_id"])
        if main_msg:
            await main_msg.edit(embed=embed, view=view)
            last_dashboard_signature = new_sig
            last_dashboard_edit_time = now
    except Exception as e:
        print(f"[DASHBOARD UPDATE ERR] {e}")

class PersistentDescriptionModal(Modal, title="Dodaj opis do shota"):
    opis_input = TextInput(
        label="Opis lub tytuł akcji",
        style=discord.TextStyle.short,
        placeholder="np. Co on odwalił XD albo Clutch 1v3",
        max_length=120,
        required=True
    )

    def __init__(self, message: discord.Message, current_opis: str = ""):
        super().__init__()
        self.target_message = message
        if current_opis:
            self.opis_input.default = current_opis

    async def on_submit(self, interaction: discord.Interaction):
        if not self.target_message.embeds:
            await interaction.response.send_message("Nie znaleziono osadzenia shota do aktualizacji.", ephemeral=True)
            return

        embed = self.target_message.embeds[0]
        new_desc_text = self.opis_input.value.strip()
        lines = embed.description.split("\n") if embed.description else []
        param_line = next((line for line in lines if "⏱️" in line), "")

        embed.description = f"💬 *\"{new_desc_text}\"*\n\n{param_line}" if param_line else f"💬 *\"{new_desc_text}\"*"
        await self.target_message.edit(embed=embed)
        update_shot_description_in_archive(self.target_message.jump_url, new_desc_text)
        await interaction.response.send_message("✅ Opis shota został pomyślnie zaktualizowany!", ephemeral=True)

class ShotContinueView(View):
    def __init__(
        self,
        kanal: str,
        buffer_id: str,
        start_offset: int,
        sekundy: int,
        file_size_mb: float,
        autor_id: int,
        autor_name: str,
        autor_avatar_url: str,
        jakosc: str = "1080p",
        part: int = 1,
        opis: Optional[str] = None
    ):
        super().__init__(timeout=None)
        self.kanal = clean_channel_name(kanal)
        self.buffer_id = buffer_id
        self.start_offset = start_offset
        self.sekundy = sekundy
        self.file_size_mb = file_size_mb
        self.autor_id = autor_id
        self.autor_name = autor_name
        self.autor_avatar_url = autor_avatar_url
        self.jakosc = jakosc
        self.part = part
        self.opis = opis

        self.add_item(Button(
            label="➕ Nagraj dalej (+30s od końca)",
            style=discord.ButtonStyle.green,
            custom_id=f"shot_cont:{self.kanal}:{jakosc}:{part}:{autor_id}:{time.time()}",
            row=0
        ))
        self.add_item(Button(
            label="⏪ Cofnij start (+10s od początku)",
            style=discord.ButtonStyle.primary,
            custom_id=f"shot_rewind:{self.kanal}:{buffer_id}:{start_offset}:{sekundy}:{jakosc}:{autor_id}",
            row=0
        ))
        self.add_item(Button(
            label="✏️ Opis",
            style=discord.ButtonStyle.secondary,
            custom_id=f"shot_desc:{autor_id}",
            row=1
        ))
        self.add_item(Button(
            label="✂️ Odetnij (-10s od początku)",
            style=discord.ButtonStyle.secondary,
            custom_id=f"shot_trim:{self.kanal}:{buffer_id}:{start_offset}:{sekundy}:{autor_id}:{jakosc}",
            row=1
        ))
        self.add_item(Button(
            label="✂️ Odetnij (-10s od końca)",
            style=discord.ButtonStyle.secondary,
            custom_id=f"shot_trim_end:{self.kanal}:{buffer_id}:{start_offset}:{sekundy}:{autor_id}:{jakosc}",
            row=1
        ))

    def format_embed(self) -> discord.Embed:
        part_suffix = f" (Część {self.part})" if self.part > 1 else ""
        embed = discord.Embed(
            title=f"🎬 Shot LIVE{part_suffix} [🟢 {self.jakosc} Full HD]",
            color=get_platform_color(self.kanal)
        )
        opis_tekst = f"💬 *\"{self.opis}\"*\n\n" if self.opis else ""
        embed.description = (
            f"{opis_tekst}"
            f"⏱️ **{self.sekundy}s** akcji | 📺 Jakość: **{self.jakosc}** | 💾 Waga: **{self.file_size_mb:.2f} MB**"
        )
        embed.set_footer(text=f"ZIEMIA 2.0 • Wycięte przez: {self.autor_name}", icon_url=self.autor_avatar_url)
        return embed

# ==============================================================================
# 7. GENEROWANIE MEDIÓW (SHOT, SCREEN, GIF)
# ==============================================================================

async def generate_clip(
    interaction: Optional[discord.Interaction],
    kanal: str,
    sekundy: int,
    back_sec: int,
    jakosc: str = "1080p",
    part: int = 1,
    opis: Optional[str] = None,
    target_channel: Optional[discord.TextChannel] = None,
    is_auto: bool = False
) -> bool:
    clean_ch = await resolve_smart_channel(kanal)
    disp_name = get_channel_display(clean_ch)

    if disp_name in EXCLUDED_CHANNELS:
        if interaction and not is_auto:
            try:
                await interaction.followup.send("⛔ Ten kanał jest wykluczony z generowania mediów.", ephemeral=True)
            except Exception:
                pass
        return False

    if is_sleeping_stream(clean_ch):
        if interaction and not is_auto:
            try:
                await interaction.followup.send("⛔ Na kanale trwa stream typu Just Sleeping — wycinanie wstrzymane.", ephemeral=True)
            except Exception:
                pass
        return False

    final_opis = opis
    if not final_opis:
        meta = channel_meta_cache.get(clean_ch, {})
        cat = meta.get("category")
        curr_time_str = datetime.now().strftime("%H:%M")
        final_opis = f"🎮 Kategoria: {cat} ({curr_time_str})" if cat else f"Akcja z transmisji na żywo ({curr_time_str})"

    buffer_id = uuid.uuid4().hex[:8]
    master_file = os.path.join(BASE_DIR, f"master_{buffer_id}.mp4")
    out_file = os.path.join(BASE_DIR, f"shot_{buffer_id}.mp4")

    dest_channel = target_channel or (interaction.channel if interaction else None)
    if not dest_channel:
        return False

    pre_buffer = 10
    total_record_sec = sekundy + pre_buffer

    async with get_render_sem():
        success = await record_buffered_clip(clean_ch, master_file, total_record_sec, back_sec + pre_buffer, jakosc, with_logo=True)

    if not success or not os.path.exists(master_file):
        if not is_auto:
            err_msg = "⚠️ Nie udało się nagrać klipu. Kanał może być offline lub wystąpił błąd strumienia."
            sent = False
            if interaction and not target_channel:
                try:
                    await interaction.followup.send(err_msg)
                    sent = True
                except Exception:
                    pass
            if not sent and dest_channel:
                await dest_channel.send(err_msg)
        return False

    initial_offset = pre_buffer
    cut_initial_cmd = [
        "ffmpeg", "-nostdin", "-y", "-ss", str(initial_offset),
        "-i", master_file, "-t", str(sekundy), "-c", "copy", "-movflags", "+faststart", out_file
    ]
    await run_cmd_async(cut_initial_cmd, timeout=20.0)
    if not await is_valid_video_file_async(out_file, min_duration=1.0):
        shutil.copyfile(master_file, out_file)
        initial_offset = 0

    if interaction and not is_auto:
        av = str(interaction.user.display_avatar.url) if interaction.user.display_avatar else None
        record_activity(interaction.user.id, interaction.user.display_name, "shot", sekundy, av, clean_ch)

    await ensure_file_under_discord_limit(out_file, max_mb=9.5)
    file_size_mb = os.path.getsize(out_file) / (1024 * 1024)

    autor_id = bot.user.id if is_auto or not interaction else interaction.user.id
    autor_name = "Automatyczny Shot" if is_auto or not interaction else interaction.user.display_name
    autor_avatar = bot.user.display_avatar.url if is_auto or not interaction else interaction.user.display_avatar.url

    view = ShotContinueView(
        kanal=clean_ch,
        buffer_id=buffer_id,
        start_offset=initial_offset,
        sekundy=sekundy,
        file_size_mb=file_size_mb,
        autor_id=autor_id,
        autor_name=autor_name,
        autor_avatar_url=autor_avatar,
        jakosc=jakosc,
        part=part,
        opis=final_opis
    )
    embed_tile = view.format_embed()

    video_msg = None
    with open(out_file, "rb") as f:
        discord_file = discord.File(f, filename=f"shot_{disp_name}.mp4")
        if interaction and not target_channel and not is_auto:
            try:
                video_msg = await interaction.followup.send(embed=embed_tile, file=discord_file, view=view, wait=True)
            except Exception:
                pass
        if not video_msg and dest_channel:
            f.seek(0)
            discord_file = discord.File(f, filename=f"shot_{disp_name}.mp4")
            video_msg = await dest_channel.send(embed=embed_tile, file=discord_file, view=view)
            if interaction and not target_channel and not is_auto:
                try:
                    await interaction.delete_original_response()
                except Exception:
                    pass

    media_url = video_msg.attachments[0].url if (video_msg and video_msg.attachments) else ""
    jump_url = video_msg.jump_url if video_msg else ""
    add_shot_to_archive(
        shot_id=buffer_id[:6],
        channel=clean_ch,
        author_id=autor_id,
        author_name=autor_name,
        duration=sekundy,
        quality=jakosc,
        description=final_opis or "",
        message_jump_url=jump_url,
        media_url=media_url
    )

    if os.path.exists(out_file):
        try:
            os.remove(out_file)
        except Exception:
            pass

    async def cleanup_master():
        await asyncio.sleep(300)
        if os.path.exists(master_file):
            try:
                os.remove(master_file)
            except Exception:
                pass
        gc.collect()

    asyncio.create_task(cleanup_master())
    return True

async def generate_screen(interaction: Optional[discord.Interaction], kanal: str, target_channel: Optional[discord.TextChannel] = None, is_auto: bool = False) -> bool:
    clean_ch = await resolve_smart_channel(kanal)
    disp_name = get_channel_display(clean_ch)
    plat = get_platform(clean_ch)

    if disp_name in EXCLUDED_CHANNELS or is_sleeping_stream(clean_ch):
        if interaction and not is_auto:
            try:
                await interaction.followup.send("⛔ Kanał wykluczony lub trwa stream typu Just Sleeping.", ephemeral=True)
            except Exception:
                pass
        return False

    dest_channel = target_channel or (interaction.channel if interaction else None)
    if not dest_channel:
        return False

    unique_suffix = uuid.uuid4().hex[:6]
    out_file = os.path.join(BASE_DIR, f"screen_{unique_suffix}.png")
    hls_url = await get_live_hls_url(clean_ch, force_fresh=True)

    if not hls_url:
        if not is_auto:
            msg = "⚠️ Wybrany kanał jest obecnie offline lub pobieranie strumienia przekroczyło limit czasu."
            if interaction and not target_channel:
                await interaction.followup.send(msg)
            else:
                await dest_channel.send(msg)
        return False

    headers_arg = f"User-Agent: {USER_AGENT}\r\n"
    if plat == "kick":
        headers_arg += "Referer: https://kick.com/\r\nOrigin: https://kick.com\r\n"

    ffmpeg_cmd = [
        "ffmpeg", "-nostdin", "-y", "-threads", "1", "-headers", headers_arg,
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "3",
        "-i", hls_url, "-map", "0:v:0", "-vframes", "1", "-c:v", "png", out_file
    ]
    await run_cmd_async(ffmpeg_cmd, timeout=14.0)

    if not os.path.exists(out_file) or os.path.getsize(out_file) < 1024:
        if not is_auto:
            err_m = "⚠️ Nie udało się pobrać klatki ze streamu."
            if interaction and not target_channel:
                await interaction.followup.send(err_m)
            else:
                await dest_channel.send(err_m)
        return False

    await asyncio.to_thread(overlay_logo_on_image, out_file)

    if interaction and not is_auto:
        av = str(interaction.user.display_avatar.url) if interaction.user.display_avatar else None
        record_activity(interaction.user.id, interaction.user.display_name, "screen", 0, av, clean_ch)
    elif is_auto and bot.user:
        record_activity(bot.user.id, "Automatyczny", "screen", 0, None, clean_ch)

    file_size_kb = os.path.getsize(out_file) / 1024
    discord_file = discord.File(out_file, filename="screen.png")

    title_text = "📸 Automatyczny Zrzut LIVE [Full HD]" if is_auto else "📸 Zrzut ekranu HD [Full HD]"
    autorek = "Automatyczny" if is_auto or not interaction else f"Przez: {interaction.user.display_name}"
    footer_text = f"ZIEMIA 2.0 • {autorek} • Rozmiar: {file_size_kb:.1f} KB"
    icon = bot.user.display_avatar.url if is_auto or not interaction else interaction.user.display_avatar.url

    embed = discord.Embed(title=title_text, color=get_platform_color(clean_ch))
    embed.set_image(url="attachment://screen.png")
    embed.set_footer(text=footer_text, icon_url=icon)

    if interaction and not target_channel and not is_auto:
        await interaction.followup.send(embed=embed, file=discord_file)
    else:
        await dest_channel.send(embed=embed, file=discord_file)

    if os.path.exists(out_file):
        try:
            os.remove(out_file)
        except Exception:
            pass
    gc.collect()
    return True

async def generate_gif_action(
    interaction: Optional[discord.Interaction],
    kanal: str,
    sekundy: int = 5,
    target_channel: Optional[discord.TextChannel] = None,
    is_auto: bool = False
) -> bool:
    clean_ch = await resolve_smart_channel(kanal)
    disp_name = get_channel_display(clean_ch)

    if disp_name in EXCLUDED_CHANNELS or is_sleeping_stream(clean_ch):
        if interaction and not is_auto:
            try:
                await interaction.followup.send("⛔ Kanał wykluczony lub trwa stream typu Just Sleeping.", ephemeral=True)
            except Exception:
                pass
        return False

    dest_channel = target_channel or (interaction.channel if interaction else None)
    if not dest_channel:
        return False

    unique_suffix = uuid.uuid4().hex[:6]
    temp_mp4 = os.path.join(BASE_DIR, f"temp_gif_{unique_suffix}.mp4")
    out_file = os.path.join(BASE_DIR, f"gif_{unique_suffix}.gif")
    fps, width = 18, 540

    async with get_render_sem():
        success = await record_buffered_clip(clean_ch, temp_mp4, sekundy + 2, sekundy + 3, "720p", with_logo=True)
        if not success or not os.path.exists(temp_mp4):
            if not is_auto:
                msg = "⚠️ Kanał jest obecnie offline lub brak bufora wideo."
                if interaction and not target_channel:
                    await interaction.followup.send(msg)
                else:
                    await dest_channel.send(msg)
            return False

        gif_filter = (
            f"trim=0:{sekundy},setpts=PTS-STARTPTS,fps={fps}:round=near,"
            f"scale={width}:-1:flags=lanczos,unsharp=3:3:0.35:3:3:0.0,split[s0][s1];"
            f"[s0]palettegen=max_colors=256:reserve_transparent=0:stats_mode=diff[p];"
            f"[s1][p]paletteuse=dither=bayer:bayer_scale=3:diff_mode=rectangle"
        )
        await run_cmd_async([
            "ffmpeg", "-nostdin", "-y", "-threads", "2", "-i", temp_mp4,
            "-vf", gif_filter, "-t", str(sekundy), "-loop", "0", out_file
        ], timeout=55.0)

        if os.path.exists(out_file) and os.path.getsize(out_file) > int(9.4 * 1024 * 1024):
            compact_filter = (
                f"trim=0:{sekundy},setpts=PTS-STARTPTS,fps=15:round=near,"
                f"scale=460:-1:flags=lanczos,split[s0][s1];"
                f"[s0]palettegen=max_colors=256:stats_mode=diff[p];"
                f"[s1][p]paletteuse=dither=bayer:bayer_scale=4:diff_mode=rectangle"
            )
            await run_cmd_async([
                "ffmpeg", "-nostdin", "-y", "-threads", "2", "-i", temp_mp4,
                "-vf", compact_filter, "-t", str(sekundy), "-loop", "0", out_file
            ], timeout=45.0)

    if os.path.exists(temp_mp4):
        try:
            os.remove(temp_mp4)
        except Exception:
            pass

    if not os.path.exists(out_file) or os.path.getsize(out_file) < 1024:
        if not is_auto:
            err_m = "⚠️ Błąd podczas generowania GIF-a."
            if interaction and not target_channel:
                await interaction.followup.send(err_m)
            else:
                await dest_channel.send(err_m)
        return False

    if interaction and not is_auto:
        av = str(interaction.user.display_avatar.url) if interaction.user.display_avatar else None
        record_activity(interaction.user.id, interaction.user.display_name, "gif", 0, av, clean_ch)

    file_size_mb = os.path.getsize(out_file) / (1024 * 1024)
    discord_file = discord.File(out_file, filename="animacja.gif")
    footer_text = f"ZIEMIA 2.0 • Przez: {interaction.user.display_name if interaction else 'Automatyczny'} • Jakość: 540p ({fps} FPS) • Rozmiar: {file_size_mb:.2f} MB"
    icon = interaction.user.display_avatar.url if interaction else bot.user.display_avatar.url

    embed = discord.Embed(title="🎞️ GIF HD [Wysoka Jakość]", color=get_platform_color(clean_ch))
    embed.set_image(url="attachment://animacja.gif")
    embed.set_footer(text=footer_text, icon_url=icon)

    if interaction and not target_channel and not is_auto:
        await interaction.followup.send(embed=embed, file=discord_file)
    else:
        await dest_channel.send(embed=embed, file=discord_file)

    if os.path.exists(out_file):
        try:
            os.remove(out_file)
        except Exception:
            pass
    gc.collect()
    return True

# ==============================================================================
# 8. PRZECHWYTYWANIE LINKÓW DO KLIPÓW I WRZUTEK UŻYTKOWNIKÓW (ON_MESSAGE)
# ==============================================================================

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    ch_name = message.channel.name.lower() if hasattr(message.channel, "name") else ""
    av = str(message.author.display_avatar.url) if message.author.display_avatar else None

    if message.attachments:
        for att in message.attachments:
            fn = att.filename.lower().split("?")[0]
            if any(k in ch_name for k in ["screen", "zrzut", "zdj"]) and fn.endswith((".jpg", ".jpeg", ".png", ".webp")):
                record_activity(message.author.id, message.author.display_name, "screen", 0, av)
                break
            elif "gif" in ch_name and fn.endswith(".gif"):
                record_activity(message.author.id, message.author.display_name, "gif", 0, av)
                break
            elif any(k in ch_name for k in ["shot", "klip"]) and fn.endswith((".mp4", ".mov", ".m4v", ".webm")):
                record_activity(message.author.id, message.author.display_name, "shot", 30, av)
                break

    match = KICK_CLIP_REGEX.search(message.content)
    if match:
        channel_name = match.group(1) or "kick"
        clip_id = match.group(2)
        full_clip_url = match.group(0)
        temp_out = os.path.join(BASE_DIR, f"clip_{message.id}_{uuid.uuid4().hex[:6]}.mp4")
        try:
            await message.add_reaction("⏳")
        except Exception:
            pass
        try:
            success, result, _ = await resolve_and_download_clip(full_clip_url, clip_id, temp_out)
            if success and os.path.exists(temp_out):
                file_size_mb = os.path.getsize(temp_out) / (1024 * 1024)
                record_activity(message.author.id, message.author.display_name, "shot", 30, av, channel_name)
                try:
                    await message.edit(suppress=True)
                except Exception:
                    pass
                embed = discord.Embed(
                    title=f"🎬 {result}",
                    description=f"💾 **Waga:** `{file_size_mb:.2f} MB` • 📺 **Jakość:** `1080p Full HD`",
                    color=KICK_GREEN
                )
                embed.set_footer(text=f"Wycięte przez: {message.author.display_name} • ZIEMIA 2.0", icon_url=message.author.display_avatar.url)
                with open(temp_out, "rb") as f:
                    sent_msg = await message.reply(embed=embed, file=discord.File(f, filename=f"clip_{channel_name}.mp4"), mention_author=False)
                    add_shot_to_archive(str(sent_msg.id)[-6:], channel_name, message.author.id, message.author.display_name, 30, "1080p", result, sent_msg.jump_url, sent_msg.attachments[0].url if sent_msg.attachments else "")
        finally:
            try:
                await message.remove_reaction("⏳", bot.user)
            except Exception:
                pass
            if os.path.exists(temp_out):
                try:
                    os.remove(temp_out)
                except Exception:
                    pass

    tw_match = TWITCH_CLIP_REGEX.search(message.content)
    if tw_match and not match:
        slug = tw_match.group(1) or tw_match.group(3)
        full_clip_url = tw_match.group(0)
        temp_out = os.path.join(BASE_DIR, f"twclip_{message.id}_{uuid.uuid4().hex[:6]}.mp4")
        try:
            await message.add_reaction("⏳")
        except Exception:
            pass
        try:
            success, title, broadcaster = await resolve_and_download_twitch_clip(full_clip_url, slug, temp_out)
            if success and os.path.exists(temp_out):
                file_size_mb = os.path.getsize(temp_out) / (1024 * 1024)
                record_activity(message.author.id, message.author.display_name, "shot", 30, av, f"tw.{broadcaster}")
                try:
                    await message.edit(suppress=True)
                except Exception:
                    pass
                embed = discord.Embed(
                    title=f"🎬 {title}",
                    description=f"💾 **Waga:** `{file_size_mb:.2f} MB` • 📺 **Jakość:** `1080p Full HD`",
                    color=TWITCH_PURPLE
                )
                embed.set_footer(text=f"Wycięte przez: {message.author.display_name} • ZIEMIA 2.0", icon_url=message.author.display_avatar.url)
                with open(temp_out, "rb") as f:
                    sent_msg = await message.reply(embed=embed, file=discord.File(f, filename=f"clip_{broadcaster}.mp4"), mention_author=False)
                    add_shot_to_archive(str(sent_msg.id)[-6:], f"tw.{broadcaster}", message.author.id, message.author.display_name, 30, "1080p", title, sent_msg.jump_url, sent_msg.attachments[0].url if sent_msg.attachments else "")
        finally:
            try:
                await message.remove_reaction("⏳", bot.user)
            except Exception:
                pass
            if os.path.exists(temp_out):
                try:
                    os.remove(temp_out)
                except Exception:
                    pass

    await bot.process_commands(message)

# ==============================================================================
# 9. OBSŁUGA INTERAKCJI PRZYCISKÓW (PANEL LIVE + KAFELKI POD SHOTEM)
# ==============================================================================

async def get_trim_source_file(buffer_id: str, message: Optional[discord.Message]) -> Tuple[Optional[str], bool]:
    buffer_file = os.path.join(BASE_DIR, f"master_{buffer_id}.mp4")
    if os.path.exists(buffer_file) and os.path.getsize(buffer_file) > 30000:
        return buffer_file, False

    if message and message.attachments:
        att_url = message.attachments[0].url
        temp_dl = os.path.join(BASE_DIR, f"trim_dl_{uuid.uuid4().hex[:6]}.mp4")
        try:
            session = await get_session()
            async with session.get(att_url, timeout=aiohttp.ClientTimeout(total=25.0)) as resp:
                if resp.status == 200:
                    with open(temp_dl, "wb") as f:
                        async for chunk in resp.content.iter_chunked(65536):
                            f.write(chunk)
            if os.path.exists(temp_dl) and os.path.getsize(temp_dl) > 30000:
                return temp_dl, True
        except Exception:
            if os.path.exists(temp_dl):
                try:
                    os.remove(temp_dl)
                except Exception:
                    pass
    return None, False

async def fast_cut_segment(source_file: str, start_sec: int, duration_sec: int, out_file: str) -> bool:
    copy_cmd = [
        "ffmpeg", "-nostdin", "-y", "-ss", str(start_sec),
        "-i", source_file, "-t", str(duration_sec), "-c", "copy", "-movflags", "+faststart", "-f", "mp4", out_file
    ]
    await run_cmd_async(copy_cmd, timeout=20.0)
    if await is_valid_video_file_async(out_file, min_duration=1.0):
        return True
    if os.path.exists(out_file):
        try:
            os.remove(out_file)
        except Exception:
            pass
    reencode_cmd = [
        "ffmpeg", "-nostdin", "-y", "-threads", "2", "-ss", str(start_sec),
        "-i", source_file, "-t", str(duration_sec), "-c:v", "libx264", "-preset", "veryfast", "-g", "30",
        "-crf", "18", "-pix_fmt", "yuv420p", "-c:a", "copy", "-movflags", "+faststart", "-f", "mp4", out_file
    ]
    await run_cmd_async(reencode_cmd, timeout=35.0)
    return await is_valid_video_file_async(out_file, min_duration=1.0)

@bot.event
async def on_interaction(interaction: discord.Interaction):
    if interaction.type != discord.InteractionType.component:
        return

    custom_id = interaction.data.get("custom_id", "")

    if custom_id.startswith("shot_desc:"):
        current_opis = ""
        if interaction.message and interaction.message.embeds:
            desc = interaction.message.embeds[0].description or ""
            if "💬 *\"" in desc:
                start = desc.find("💬 *\"") + 5
                end = desc.find("\"*", start)
                if end != -1:
                    current_opis = desc[start:end]
        await interaction.response.send_modal(PersistentDescriptionModal(message=interaction.message, current_opis=current_opis))

    elif custom_id.startswith("shot_rewind:"):
        parts = custom_id.split(":")
        if len(parts) >= 7:
            clean_ch, buffer_id = parts[1], parts[2]
            try:
                start_offset, prev_sekundy, jakosc, autor_id = int(parts[3]), int(parts[4]), parts[5], int(parts[6])
            except ValueError:
                return

            is_admin = interaction.user.guild_permissions.administrator if hasattr(interaction.user, "guild_permissions") else False
            if interaction.user.id != autor_id and not is_admin:
                await interaction.response.send_message("⛔ Tylko autor shota lub Administrator może cofnąć jego start!", ephemeral=True)
                return
            if start_offset <= 0:
                await interaction.response.send_message("⛔ Osiągnięto już maksymalny bufor z początku nagrania!", ephemeral=True)
                return

            buffer_file = os.path.join(BASE_DIR, f"master_{buffer_id}.mp4")
            if not os.path.exists(buffer_file):
                await interaction.response.send_message("⛔ Bufor tego shota wygasł (ważny przez 5 minut). Wykonaj nowego shota!", ephemeral=True)
                return

            if not interaction.response.is_done():
                await interaction.response.defer(thinking=True)

            new_start_offset = max(start_offset - 10, 0)
            new_duration = prev_sekundy + (start_offset - new_start_offset)
            out_file = os.path.join(BASE_DIR, f"shot_sub_{interaction.id}_{uuid.uuid4().hex[:6]}.mp4")

            ok_cut = await fast_cut_segment(buffer_file, new_start_offset, new_duration, out_file)
            if not ok_cut:
                await interaction.followup.send("⚠️ Błąd podczas przycinania bufora.", ephemeral=True)
                return

            await ensure_file_under_discord_limit(out_file, max_mb=9.5)
            file_size_mb = os.path.getsize(out_file) / (1024 * 1024)

            current_opis = ""
            if interaction.message and interaction.message.embeds:
                desc = interaction.message.embeds[0].description or ""
                if "💬 *\"" in desc:
                    s_idx = desc.find("💬 *\"") + 5
                    e_idx = desc.find("\"*", s_idx)
                    if e_idx != -1:
                        current_opis = desc[s_idx:e_idx]

            view = ShotContinueView(
                kanal=clean_ch, buffer_id=buffer_id, start_offset=new_start_offset, sekundy=new_duration,
                file_size_mb=file_size_mb, autor_id=interaction.user.id, autor_name=interaction.user.display_name,
                autor_avatar_url=interaction.user.display_avatar.url, jakosc=jakosc, part=1, opis=current_opis or None
            )
            with open(out_file, "rb") as f:
                await interaction.followup.send(embed=view.format_embed(), file=discord.File(f, filename=f"shot_{get_channel_display(clean_ch)}.mp4"), view=view)
            try:
                await interaction.message.delete()
            except Exception:
                pass
            if os.path.exists(out_file):
                try:
                    os.remove(out_file)
                except Exception:
                    pass
            gc.collect()

    elif custom_id.startswith("shot_trim:") or custom_id.startswith("shot_trim_end:"):
        is_trim_end = custom_id.startswith("shot_trim_end:")
        parts = custom_id.split(":")
        if len(parts) >= 7:
            clean_ch, buffer_id = parts[1], parts[2]
            try:
                start_offset, sekundy, autor_id, jakosc = int(parts[3]), int(parts[4]), int(parts[5]), parts[6]
            except ValueError:
                return

            is_admin = interaction.user.guild_permissions.administrator if hasattr(interaction.user, "guild_permissions") else False
            if interaction.user.id != autor_id and not is_admin:
                await interaction.response.send_message("⛔ Tylko autor shota lub Administrator może go przyciąć!", ephemeral=True)
                return
            if sekundy <= 12:
                await interaction.response.send_message("⛔ Ten klip jest za krótki (12s lub mniej), aby odciąć kolejne 10 sekund!", ephemeral=True)
                return

            if not interaction.response.is_done():
                await interaction.response.defer(thinking=True)

            source_file, is_temp_dl = await get_trim_source_file(buffer_id, interaction.message)
            if not source_file:
                await interaction.followup.send("⛔ Nie udało się pobrać pliku wideo do przycięcia.", ephemeral=True)
                return

            new_duration = sekundy - 10
            if is_trim_end:
                new_start_offset = start_offset
                ss_val = 0 if is_temp_dl else new_start_offset
            else:
                new_start_offset = start_offset + 10
                ss_val = 10 if is_temp_dl else new_start_offset

            out_file = os.path.join(BASE_DIR, f"shot_sub_{interaction.id}_{uuid.uuid4().hex[:6]}.mp4")
            try:
                ok_cut = await fast_cut_segment(source_file, ss_val, new_duration, out_file)
                if not ok_cut:
                    await interaction.followup.send("⚠️ Nie udało się przyciąć materiału wideo.", ephemeral=True)
                    return

                await ensure_file_under_discord_limit(out_file, max_mb=9.5)
                file_size_mb = os.path.getsize(out_file) / (1024 * 1024)

                current_opis = ""
                if interaction.message and interaction.message.embeds:
                    desc = interaction.message.embeds[0].description or ""
                    if "💬 *\"" in desc:
                        start = desc.find("💬 *\"") + 5
                        end = desc.find("\"*", start)
                        if end != -1:
                            current_opis = desc[start:end]

                view = ShotContinueView(
                    kanal=clean_ch, buffer_id=buffer_id, start_offset=new_start_offset, sekundy=new_duration,
                    file_size_mb=file_size_mb, autor_id=interaction.user.id, autor_name=interaction.user.display_name,
                    autor_avatar_url=interaction.user.display_avatar.url, jakosc=jakosc, part=1, opis=current_opis or None
                )
                with open(out_file, "rb") as f:
                    await interaction.followup.send(embed=view.format_embed(), file=discord.File(f, filename=f"shot_{get_channel_display(clean_ch)}.mp4"), view=view)
                try:
                    await interaction.message.delete()
                except Exception:
                    pass
            finally:
                if is_temp_dl and os.path.exists(source_file):
                    try:
                        os.remove(source_file)
                    except Exception:
                        pass
                if os.path.exists(out_file):
                    try:
                        os.remove(out_file)
                    except Exception:
                        pass
                gc.collect()

    elif custom_id.startswith("shot_cont:"):
        parts = custom_id.split(":")
        if len(parts) >= 6:
            clean_ch = parts[1]
            try:
                jakosc, part, end_moment = parts[2], int(parts[3]), float(parts[5])
            except ValueError:
                return
            if not interaction.response.is_done():
                await interaction.response.defer(thinking=True)
            back_sec = max(int(time.time() - end_moment), 0)
            target_shot_ch = get_target_channel_for_type(interaction.guild, "shot") or interaction.channel
            await generate_clip(interaction=interaction, kanal=clean_ch, sekundy=30, back_sec=back_sec, jakosc=jakosc, part=part + 1, target_channel=target_shot_ch)

    elif custom_id.startswith("dash_shot:"):
        ch = custom_id.split(":", 1)[1]
        target_channel = get_target_channel_for_type(interaction.guild, "shot") or interaction.channel
        await interaction.response.send_message(f"🎬 Rozpoczęto wycinanie shota 1080p! Materiał pojawi się na {target_channel.mention}.", ephemeral=True)
        asyncio.create_task(generate_clip(interaction=interaction, kanal=ch, sekundy=30, back_sec=30, jakosc="1080p", part=1, target_channel=target_channel))

    elif custom_id.startswith("dash_scr:"):
        ch = custom_id.split(":", 1)[1]
        target_channel = get_target_channel_for_type(interaction.guild, "screen") or interaction.channel
        await interaction.response.send_message(f"📸 Pobieranie zrzutu ekranu HD na {target_channel.mention}...", ephemeral=True)
        asyncio.create_task(generate_screen(interaction=interaction, kanal=ch, target_channel=target_channel))

    elif custom_id.startswith("dash_gif:"):
        ch = custom_id.split(":", 1)[1]
        target_channel = get_target_channel_for_type(interaction.guild, "gif") or interaction.channel
        await interaction.response.send_message(f"🎞️ Tworzenie GIF-a HD na {target_channel.mention}...", ephemeral=True)
        asyncio.create_task(generate_gif_action(interaction=interaction, kanal=ch, sekundy=5, target_channel=target_channel))

    elif custom_id == "dash_refresh":
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        alerts = load_alerts()
        async def refresh_single_channel(ch):
            clean_ch = clean_channel_name(ch)
            is_live, _, _, started_at = await fetch_kick_channel_meta(clean_ch, force_fresh=True)
            set_channel_status(clean_ch, is_live, started_at)
        await asyncio.gather(*[refresh_single_channel(ch) for ch in alerts.keys()], return_exceptions=True)
        save_live_cache(live_status_cache)
        await update_live_dashboard(force=True)
        await interaction.followup.send("✅ Panel na żywo został odświeżony!", ephemeral=True)

# ==============================================================================
# 10. KOMENDA /SZUKAJ (PEŁNY KATALOG WSZYSTKICH FILMÓW STREAMERA Z PAGINACJĄ)
# ==============================================================================

class SearchPaginationView(View):
    def __init__(self, matches: List[dict], query_label: str, page: int = 0):
        super().__init__(timeout=300)
        self.matches = matches
        self.query_label = query_label
        self.page = page
        self.page_size = 10
        self.total_pages = max((len(matches) + self.page_size - 1) // self.page_size, 1)
        self.update_buttons()

    def update_buttons(self):
        self.clear_items()
        if self.total_pages > 1:
            btn_prev = Button(label="◀ Poprzednie", style=discord.ButtonStyle.secondary, disabled=(self.page <= 0))
            btn_next = Button(label="Następne ▶", style=discord.ButtonStyle.secondary, disabled=(self.page >= self.total_pages - 1))
            btn_prev.callback = self.on_prev
            btn_next.callback = self.on_next
            self.add_item(btn_prev)
            self.add_item(btn_next)

    def get_embed(self) -> discord.Embed:
        start_idx = self.page * self.page_size
        page_clips = self.matches[start_idx : start_idx + self.page_size]

        lines = []
        for idx, m in enumerate(page_clips, start=start_idx + 1):
            dur = m.get("duration", 30)
            desc = (m.get("description") or "Shot 1080p").replace("\n", " ").strip()
            if len(desc) > 65:
                desc = desc[:65] + "…"
            auth = m.get("author_name") or "Nieznany"
            dt_str = (m.get("created_at") or "")[:10]
            j_url = m.get("jump_url") or m.get("media_url") or ""

            link_part = f"[▶ Otwórz film]({j_url})" if j_url else ""
            lines.append(
                f"`{idx}.` 🎬 **Shot {dur}s** • {link_part}\n"
                f"└ 💬 *{desc}* ▎ 👤 `{auth}` ({dt_str})"
            )

        embed = discord.Embed(
            title=f"🔍 WYNIKI WYSZUKIWANIA: {self.query_label} ({len(self.matches)} filmów)",
            description="\n\n".join(lines),
            color=KICK_GREEN
        )
        embed.set_footer(text=f"Strona {self.page + 1}/{self.total_pages} • Pełny Katalog Shotów • ZIEMIA 2.0")
        return embed

    async def on_prev(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    async def on_next(self, interaction: discord.Interaction):
        self.page = min(self.total_pages - 1, self.page + 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

@bot.tree.command(name="szukaj", description="Wyświetl wszystkie filmy danego streamera z kanału shoterskiego lub szukaj po opisie")
@app_commands.describe(
    kanal="Wybierz streamera z listy, aby wyświetlić wszystkie jego filmy",
    fraza="Opcjonalnie wpisz szukaną frazę (np. nazwa streamera, słowo z opisu lub nick autora)"
)
@app_commands.autocomplete(kanal=channel_autocomplete)
@app_commands.checks.cooldown(1, 5.0, key=lambda i: (i.guild_id, i.user.id))
async def szukaj(interaction: discord.Interaction, kanal: Optional[str] = None, fraza: Optional[str] = None):
    if not interaction.response.is_done():
        await interaction.response.defer(thinking=True)

    if not fraza and not kanal:
        await interaction.followup.send("⚠️ Wybierz streamera w polu `kanal` lub wpisz szukaną frazę, aby wyświetlić filmy.", ephemeral=True)
        return

    all_clips = await fetch_all_channel_shots(interaction.guild, force_rescan=False)
    if not all_clips:
        all_clips = await fetch_all_channel_shots(interaction.guild, force_rescan=True)

    q_low = (fraza or "").strip().lower()
    target_ch = clean_channel_name(kanal) if kanal else None
    target_login = get_channel_login(target_ch) if target_ch else None

    matches = []
    for c in all_clips:
        c_ch = clean_channel_name(c.get("channel", ""))
        c_login = get_channel_login(c_ch)
        c_desc = (c.get("description") or "").lower()
        c_auth = (c.get("author_name") or "").lower()
        c_id = (c.get("id") or "").lower()

        if target_login and target_login not in c_login and target_login not in c_desc:
            continue
        if q_low and not (q_low in c_login or q_low in c_desc or q_low in c_auth or q_low in c_id):
            continue
        matches.append(c)

    if not matches and not last_search_scan_time:
        all_clips = await fetch_all_channel_shots(interaction.guild, force_rescan=True)

    label_str = (get_channel_display(target_ch).upper() if target_ch else "") or (fraza.upper() if fraza else "WSZYSTKIE")
    if not matches:
        await interaction.followup.send(f"🔍 Nie znaleziono żadnych filmów pasujących do zapytania: `{label_str}`.")
        return

    view = SearchPaginationView(matches, query_label=label_str, page=0)
    await interaction.followup.send(embed=view.get_embed(), view=view if view.total_pages > 1 else None)

# ==============================================================================
# 11. PĘTLE W TLE (TYLKO AUTO-SCREEN, MONITOR LIVE I KONSERWACJA)
# ==============================================================================

@tasks.loop(minutes=15)
async def auto_maintenance_loop():
    await asyncio.to_thread(cleanup_orphaned_temp_files, 300)
    rebuild_leaderboard_from_local_stats()
    gc.collect()

@auto_maintenance_loop.before_loop
async def before_auto_maintenance():
    await bot.wait_until_ready()
    await asyncio.sleep(15)

@tasks.loop(minutes=20)
async def auto_screen_loop():
    alerts = load_alerts()
    if not alerts:
        return
    active_streamers = [
        ch for ch in alerts.keys()
        if is_channel_live(ch) and get_channel_login(ch) not in EXCLUDED_CHANNELS and not is_sleeping_stream(ch)
    ]
    if not active_streamers:
        return
    random.shuffle(active_streamers)
    for guild in bot.guilds:
        target_channel = get_target_channel_for_type(guild, "screen")
        if target_channel:
            for candidate in active_streamers[:3]:
                try:
                    if await generate_screen(interaction=None, kanal=candidate, target_channel=target_channel, is_auto=True):
                        break
                except Exception:
                    pass

@auto_screen_loop.before_loop
async def before_auto_screen():
    await bot.wait_until_ready()
    await asyncio.sleep(25)

async def check_single_streamer(streamer: str, channel_id: int) -> bool:
    global live_status_cache, offline_counters, initial_check_done, channel_meta_cache
    clean_s = clean_channel_name(streamer)
    disp_name = get_channel_display(clean_s)
    plat_name = "Twitchu" if get_platform(clean_s) == "twitch" else "Kicku"
    badge = get_platform_badge(clean_s)
    web_url = get_channel_web_url(clean_s)

    try:
        is_live, title, category, started_at = await fetch_kick_channel_meta(clean_s)
        was_live = is_channel_live(clean_s)

        if is_live:
            offline_counters[clean_s] = 0
            channel_meta_cache[clean_s] = {
                "title": title or "Transmisja na żywo",
                "category": category or ("Twitch Stream" if get_platform(clean_s) == "twitch" else "Kick Stream"),
                "playback_url": channel_meta_cache.get(clean_s, {}).get("playback_url"),
                "url_time": channel_meta_cache.get(clean_s, {}).get("url_time", time.time())
            }
            set_channel_status(clean_s, True, started_at)
            if not initial_check_done:
                return False

            if not was_live:
                save_live_cache(live_status_cache)
                discord_channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
                if discord_channel:
                    embed = discord.Embed(
                        title=f"🔴 {disp_name} właśnie odpalił live na {plat_name}! [{badge}]",
                        url=web_url,
                        description=f"**Tytuł:** {title or 'Transmisja na żywo'}\n**Kategoria:** {category or 'Stream'}",
                        color=get_platform_color(clean_s)
                    )
                    embed.set_footer(text="Kliknij link powyżej, aby wejść na stream! • ZIEMIA 2.0")
                    try:
                        await discord_channel.send(embed=embed)
                    except Exception:
                        pass
                return True
        else:
            if not initial_check_done:
                set_channel_status(clean_s, False)
                return False

            count = offline_counters.get(clean_s, 0) + 1
            offline_counters[clean_s] = count

            if count >= 2 and was_live:
                prev_start = get_channel_started_at(clean_s) or (time.time() - 3600)
                session_duration = time.time() - prev_start
                session_shots = count_session_shots_in_archive(clean_s, prev_start)

                set_channel_status(clean_s, False)
                save_live_cache(live_status_cache)

                discord_channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
                if discord_channel:
                    summary_embed = discord.Embed(
                        title=f"⚪ {disp_name} zakończył transmisję [{badge}]",
                        url=web_url,
                        description=(
                            f"Transmisja dobiegła końca.\n\n"
                            f"⏱️ **Czas trwania live:** `{format_uptime(session_duration)}`\n"
                            f"🎬 **Shoty wycięte w tej sesji:** `{session_shots}` materiałów\n"
                            f"🎮 **Ostatnia kategoria:** `{channel_meta_cache.get(clean_s, {}).get('category', 'Stream')}`"
                        ),
                        color=0x808080
                    )
                    summary_embed.set_footer(text="Podsumowanie sesji • ZIEMIA 2.0")
                    try:
                        await discord_channel.send(embed=summary_embed)
                    except Exception:
                        pass
                return True
    except Exception as e:
        print(f"[BŁĄD MONITORA {clean_s}]: {e}")
    return False

async def safe_check_streamer(streamer: str, channel_id: int) -> bool:
    async with get_check_sem():
        changed = await check_single_streamer(streamer, channel_id)
        await asyncio.sleep(0.08)
        return changed

@tasks.loop(seconds=60)
async def check_kick_streamers():
    global initial_check_done
    alerts = load_alerts()
    any_changed = False
    if alerts:
        results = await asyncio.gather(*[safe_check_streamer(s, cid) for s, cid in alerts.items()], return_exceptions=True)
        any_changed = any(r is True for r in results)

    if not initial_check_done:
        initial_check_done = True
        save_live_cache(live_status_cache)
        await update_live_dashboard(force=True)
    else:
        await update_live_dashboard(force=any_changed)

@check_kick_streamers.error
async def check_kick_streamers_error(error):
    await asyncio.sleep(5)
    if not check_kick_streamers.is_running():
        check_kick_streamers.start()

@bot.event
async def on_ready():
    load_alerts()
    load_activity_feed()
    await asyncio.to_thread(get_prepared_logo_path, 240)
    await asyncio.to_thread(get_prepared_logo_path, 165)
    await asyncio.to_thread(cleanup_orphaned_temp_files, 60)
    rebuild_leaderboard_from_local_stats()

    try:
        _, dash_view = build_dashboard_content()
        bot.add_view(dash_view)
        print("[VIEWS] Widok stałego panelu LIVE zarejestrowany pomyślnie.")
    except Exception as e:
        print(f"[PERSISTENT VIEWS INIT ERR]: {e}")

    try:
        for guild in bot.guilds:
            bot.tree.clear_commands(guild=guild)
            await bot.tree.sync(guild=guild)
        await bot.tree.sync()
        print("[SLASH COMMANDS] Komendy Slash zsynchronizowane pomyślnie.")
    except Exception as e:
        print(f"[SLASH SYNC WARN] {e}")

    asyncio.create_task(refresh_complete_global_stats(history_limit=2500))

    if not auto_maintenance_loop.is_running():
        auto_maintenance_loop.start()
    if not check_kick_streamers.is_running():
        check_kick_streamers.start()
    if not auto_screen_loop.is_running():
        auto_screen_loop.start()
    print(f"Bot zalogowany jako: {bot.user} | Railway Eco-Mode (Auto-Screen ON, Auto-Shot/GIF OFF).")

@bot.command(name="sync")
@commands.has_permissions(administrator=True)
async def manual_sync(ctx: commands.Context):
    await ctx.send("⏳ Synchronizuję komendy, panel LIVE, katalog `/szukaj` oraz przeliczam cały Hall of Fame...")
    for guild in bot.guilds:
        bot.tree.clear_commands(guild=guild)
        await bot.tree.sync(guild=guild)
    await bot.tree.sync()
    await refresh_complete_global_stats(history_limit=2500)
    await fetch_all_channel_shots(ctx.guild, force_rescan=True)
    await update_live_dashboard(force=True)
    await ctx.send("✅ Komendy (`/shot`, `/gif`, `/screen`, `/szukaj`), panel LIVE oraz Hall of Fame zostały w 100% zsynchronizowane!")

# ==============================================================================
# 12. KOMENDY SLASH
# ==============================================================================

@bot.tree.command(name="screen", description="Pobierz bezstratny zrzut ekranu PNG w wysokiej rozdzielczości")
@app_commands.describe(kanal="Nazwa kanału na Kicku lub Twitchu")
@app_commands.autocomplete(kanal=channel_autocomplete)
@app_commands.checks.cooldown(1, 10.0, key=lambda i: (i.guild_id, i.user.id))
async def screen(interaction: discord.Interaction, kanal: str):
    if not interaction.response.is_done():
        await interaction.response.defer(thinking=True)
    await generate_screen(interaction, kanal)

@bot.tree.command(name="shot", description="Wytnij shota wstecz od momentu wywołania komendy (1080p Full HD)")
@app_commands.describe(
    kanal="Nazwa kanału na Kicku lub Twitchu",
    sekundy="Ile sekund wstecz wyciąć (np. 15, 30, 45, 60)",
    jakosc="Opcjonalny wybór rozdzielczości wideo",
    opis="Opcjonalny krótki opis lub tytuł akcji"
)
@app_commands.autocomplete(kanal=channel_autocomplete)
@app_commands.choices(jakosc=[
    app_commands.Choice(name="1080p (Full HD - Domyślna jakość)", value="1080p"),
    app_commands.Choice(name="720p (Optymalna HD)", value="720p"),
    app_commands.Choice(name="480p (Większa płynność)", value="480p")
])
@app_commands.checks.cooldown(1, 15.0, key=lambda i: (i.guild_id, i.user.id))
async def shot(interaction: discord.Interaction, kanal: str, sekundy: int = 30, jakosc: app_commands.Choice[str] = None, opis: Optional[str] = None):
    if not interaction.response.is_done():
        await interaction.response.defer(thinking=True)
    if sekundy < 5 or sekundy > 60:
        await interaction.followup.send("Długość shota musi wynosić od 5 do 60 sekund.", ephemeral=True)
        return
    wybrana_jakosc = jakosc.value if jakosc else "1080p"
    await generate_clip(interaction, kanal, sekundy=sekundy, back_sec=sekundy, jakosc=wybrana_jakosc, part=1, opis=opis)

@bot.tree.command(name="gif", description="Stwórz animowany, ostry GIF HD z ostatnich sekund transmisji (1-10s)")
@app_commands.describe(kanal="Nazwa kanału na Kicku lub Twitchu", sekundy="Długość gifa w sekundach (1-10)")
@app_commands.autocomplete(kanal=channel_autocomplete)
@app_commands.checks.cooldown(1, 15.0, key=lambda i: (i.guild_id, i.user.id))
async def make_gif(interaction: discord.Interaction, kanal: str, sekundy: int = 5):
    if not interaction.response.is_done():
        await interaction.response.defer(thinking=True)
    if sekundy < 1 or sekundy > 10:
        await interaction.followup.send("GIF może mieć od 1 do 10 sekund.", ephemeral=True)
        return
    await generate_gif_action(interaction, kanal, sekundy=sekundy)

@bot.tree.command(name="top", description="Wyświetl ranking Hall of Fame z podziałem na shoty, screeny i gify")
@app_commands.checks.cooldown(1, 5.0, key=lambda i: (i.guild_id, i.user.id))
async def top(interaction: discord.Interaction):
    if not interaction.response.is_done():
        await interaction.response.defer(thinking=True)
    rebuild_leaderboard_from_local_stats()
    if not cached_leaderboard_data:
        await refresh_complete_global_stats(history_limit=1500)
    if not cached_leaderboard_data:
        await interaction.followup.send("📊 Na serwerze nie ma jeszcze zarejestrowanej aktywności użytkowników.")
        return

    embed = discord.Embed(
        title="🏆 HALL OF FAME – ZIEMIA 2.0",
        description=(
            f"Aktywność społeczności: 🎬 **{cached_global_stats['shots']}** shotów | "
            f"📸 **{cached_global_stats['screens']}** screenów | 🎞️ **{cached_global_stats['gifs']}** gifów\n\n"
            "**Najlepsi twórcy serwera (Shot = 3 pkt, GIF = 2 pkt, Screen = 1 pkt):**"
        ),
        color=KICK_GREEN
    )
    medals = ["🥇", "🥈", "🥉"]
    lines = []
    user_position = None
    u_norm = normalize_name(interaction.user.display_name)

    for data in cached_leaderboard_data[:10]:
        idx = data["rank"]
        icon = medals[idx - 1] if idx <= 3 else f"`#{idx}`"
        lines.append(
            f"{icon} **{data['name']}**\n"
            f"└ 🎬 `{data['shots']}` shotów • 📸 `{data['screens']}` screenów • 🎞️ `{data['gifs']}` gifów (Punkty: **{data['score']}**)"
        )
    for data in cached_leaderboard_data:
        if normalize_name(data["name"]) == u_norm:
            user_position = (data["rank"], data["shots"], data["screens"], data["gifs"])
            break

    embed.add_field(name="Klasyfikacja Generalna", value="\n".join(lines) if lines else "Brak danych", inline=False)
    if user_position:
        pos, u_s, u_sc, u_g = user_position
        embed.set_footer(text=f"Twój wynik: #{pos} miejsce • {u_s} shotów | {u_sc} screenów | {u_g} gifów • ZIEMIA 2.0", icon_url=interaction.user.display_avatar.url)
    else:
        embed.set_footer(text="Wytnij shota, screena lub gifa, aby zdobyć punkty w rankingu! • ZIEMIA 2.0", icon_url=interaction.user.display_avatar.url)
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="panel_ustaw", description="Utwórz stały panel transmisji na żywo (tylko Administrator)")
@app_commands.describe(
    kanal_discord="Kanał, na którym ma powstać stały panel (np. #na-żywo)",
    kanal_na_shoty="Kanał, na który mają lądować shoty wycięte z panelu (np. #kanał-shoterski)"
)
@app_commands.checks.has_permissions(administrator=True)
async def panel_ustaw(interaction: discord.Interaction, kanal_discord: discord.TextChannel = None, kanal_na_shoty: discord.TextChannel = None):
    await interaction.response.defer(ephemeral=True)
    target_channel = kanal_discord or interaction.channel
    shot_channel = kanal_na_shoty or target_channel
    try:
        embed, view = build_dashboard_content()
        main_msg = await target_channel.send(embed=embed, view=view)
        save_dashboard_config({"channel_id": target_channel.id, "message_id": main_msg.id, "shot_channel_id": shot_channel.id})
        await interaction.followup.send(
            f"✅ Stały panel LIVE (Kick + Twitch) został utworzony na {target_channel.mention}!\n"
            f"📁 Shoty, screeny i gify z kafelków trafią na: {shot_channel.mention}.",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"⚠️ Błąd podczas tworzenia panelu: `{e}`", ephemeral=True)

@bot.tree.command(name="panel_usun", description="Usuń konfigurację stałego panelu (tylko Administrator)")
@app_commands.checks.has_permissions(administrator=True)
async def panel_usun(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    config = load_dashboard_config()
    if not config:
        await interaction.followup.send("Panel na żywo nie jest skonfigurowany.", ephemeral=True)
        return
    try:
        ch = bot.get_channel(config["channel_id"]) or await bot.fetch_channel(config["channel_id"])
        if ch:
            main_msg = await ch.fetch_message(config["message_id"])
            if main_msg:
                await main_msg.delete()
    except Exception:
        pass
    if os.path.exists(DASHBOARD_FILE):
        try:
            os.remove(DASHBOARD_FILE)
        except Exception:
            pass
    _memory_cache["dashboard"] = {}
    await interaction.followup.send("🗑️ Konfiguracja panelu na żywo została usunięta.", ephemeral=True)

@bot.tree.command(name="alert_wlacz", description="Włącz powiadomienia o starcie live (tylko Administrator)")
@app_commands.describe(kanal="Nazwa kanału na Kicku lub link twitch.tv/nazwa", kanal_discord="Kanał Discord do powiadomień", platforma="Wybierz platformę")
@app_commands.choices(platforma=[
    app_commands.Choice(name="🟢 Kick.com", value="kick"),
    app_commands.Choice(name="🟣 Twitch.tv", value="twitch")
])
@app_commands.checks.has_permissions(administrator=True)
async def alert_wlacz(interaction: discord.Interaction, kanal: str, kanal_discord: discord.TextChannel = None, platforma: app_commands.Choice[str] = None):
    target_channel = kanal_discord or interaction.channel
    forced_plat = platforma.value if platforma else None
    kanal_clean = clean_channel_name(kanal, force_platform=forced_plat)
    alerts = load_alerts()
    alerts[kanal_clean] = target_channel.id
    save_alerts(alerts)
    await interaction.response.send_message(
        f"✅ Dodano powiadomienia dla **{get_channel_display(kanal_clean)}** ({get_platform_badge(kanal_clean)}) na {target_channel.mention}!\n"
        f"Łącznie śledzonych kanałów: **{len(alerts)}**."
    )

@bot.tree.command(name="alert_wylacz", description="Wyłącz powiadomienia o live dla danego kanału (tylko Administrator)")
@app_commands.describe(kanal="Nazwa kanału")
@app_commands.autocomplete(kanal=channel_autocomplete)
@app_commands.checks.has_permissions(administrator=True)
async def alert_wylacz(interaction: discord.Interaction, kanal: str):
    kanal_clean = clean_channel_name(kanal)
    alerts = load_alerts()
    if kanal_clean not in alerts and f"tw.{kanal_clean}" in alerts:
        kanal_clean = f"tw.{kanal_clean}"
    if kanal_clean in alerts:
        disp = get_channel_display(kanal_clean)
        badge = get_platform_badge(kanal_clean)
        del alerts[kanal_clean]
        save_alerts(alerts)
        if kanal_clean in live_status_cache:
            del live_status_cache[kanal_clean]
            save_live_cache(live_status_cache)
        await interaction.response.send_message(f"🗑️ Wyłączono powiadomienia dla **{disp}** ({badge}). Pozostało: **{len(alerts)}**.")
    else:
        await interaction.response.send_message(f"Kanał **{kanal}** nie znajdował się na liście śledzonych.", ephemeral=True)

@bot.tree.command(name="alert_kopia", description="Pokaż listę streamerów w formacie do wpisania w Railway Variables")
async def alert_kopia(interaction: discord.Interaction):
    alerts = load_alerts()
    if not alerts:
        await interaction.response.send_message("Lista jest obecnie pusta.", ephemeral=True)
        return
    await interaction.response.send_message(f"📋 **Wklej to do zmiennej KICK_CHANNELS w Railway:**\n```{', '.join(alerts.keys())}```", ephemeral=True)

@bot.tree.command(name="pomoc", description="Podsumowanie najważniejszych funkcji bota")
async def pomoc(interaction: discord.Interaction):
    embed = discord.Embed(
        title="📗 Kick & Twitch Bot – Najważniejsze Funkcje",
        description="Automatyczny bot do monitorowania streamów Kick.com oraz Twitch.tv i wycinania multimediów w jakości Full HD.",
        color=KICK_GREEN
    )
    embed.add_field(
        name="🎬 Multimedia na Żywo (🟢 Kick & 🟣 Twitch)",
        value=(
            "• `/shot <kanał> [sekundy] [jakość] [opis]` – wycina wideo (5–60s) w jakości Full HD 1080p z logo.\n"
            "• `/screen <kanał>` – pobiera bezstratny zrzut ekranu PNG z logo.\n"
            "• `/gif <kanał> [sekundy]` – tworzy ostry GIF 540p (1–10s) z pełną paletą barw i logo."
        ),
        inline=False
    )
    embed.add_field(
        name="🔍 Wyszukiwanie, Panel LIVE i Ranking",
        value=(
            "• `/szukaj [kanał] [fraza]` – wyświetla wszystkie nagrane filmy danego streamera z przewijaniem stron.\n"
            "• `/panel_ustaw` – tworzy stały panel monitorowania transmisji LIVE z szybkimi kafelkami.\n"
            "• `/top` – ranking najaktywniejszych twórców serwera (Hall of Fame)."
        ),
        inline=False
    )
    embed.set_footer(text=f"Wywołano przez: {interaction.user.display_name} • ZIEMIA 2.0", icon_url=interaction.user.display_avatar.url)
    await interaction.response.send_message(embed=embed)

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CommandOnCooldown):
        komunikat = f"⏳ **Zwolnij!** Spróbuj ponownie za **{round(error.retry_after, 1)}s**."
        if interaction.response.is_done():
            await interaction.followup.send(komunikat, ephemeral=True)
        else:
            await interaction.response.send_message(komunikat, ephemeral=True)
    elif isinstance(error, app_commands.MissingPermissions):
        komunikat = "⛔ Nie posiadasz wymaganych uprawnień (Administrator)."
        if interaction.response.is_done():
            await interaction.followup.send(komunikat, ephemeral=True)
        else:
            await interaction.response.send_message(komunikat, ephemeral=True)
    else:
        print(f"Nieobsłużony błąd komendy: {error}")

if __name__ == "__main__":
    if not TOKEN:
        print("[CRITICAL] Brak zmiennej środowiskowej DISCORD_TOKEN!")
        while True:
            time.sleep(60)
    else:
        while True:
            try:
                bot.run(TOKEN)
            except Exception as e:
                print(f"[BOT RUNTIME ERROR - AUTO-RECOVERY ZA 15s]: {e}")
                traceback.print_exc()
                time.sleep(15)
