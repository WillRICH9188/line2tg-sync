# app.py —— LINE 群视频同步到 Telegram 频道（Bot 版）
# 功能：强制压缩到 ~47–48MB + 限速 + 每小时上限 + 抖动 + 重试 + 去重（保持比例不变）+ 发送队列（串行/小并发）
# 依赖：ffmpeg; python-telegram-bot 21.x, fastapi, line-bot-sdk, uvicorn, opencv-python-headless

import os
import tempfile
import time
import asyncio
import random
import hashlib
import subprocess
from collections import deque
from typing import Tuple

from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import PlainTextResponse

from linebot import LineBotApi, WebhookParser
from linebot.models import MessageEvent, VideoMessage, FileMessage, TextMessage

# ===== 必填环境变量 =====
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "").strip()
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
TG_TARGET = os.environ.get("TG_TARGET", "").strip()  # 强烈建议使用 -100xxxxxxxxxx

if not (LINE_CHANNEL_SECRET and LINE_CHANNEL_ACCESS_TOKEN and BOT_TOKEN and TG_TARGET):
    raise RuntimeError("❌ 必填环境变量缺失：请设置 LINE_CHANNEL_SECRET / LINE_CHANNEL_ACCESS_TOKEN / BOT_TOKEN / TG_TARGET")

# ===== 可选参数（可用环境变量覆盖）=====
BTN_URL = os.environ.get("BTN_URL", "").strip()

# —— 压缩相关（确保 ≤50MB，默认目标 47.5MB，保留安全余量）——
TARGET_MB  = float(os.getenv("TG_TARGET_MB", "47.5"))   # 最终目标大小（MB）
AUDIO_KBPS = int(os.getenv("TG_AUDIO_KBPS", "64"))      # 音频码率（kbps）
SCALE_WIDTH = int(os.getenv("TG_SCALE_W", "720"))       # 最大宽度（保持比例），可改 640 更狠
TARGET_FPS = int(os.getenv("TG_FPS", "24"))             # 降帧（24 基本不影响观看）

# —— 防风控：限速 + 每小时上限 + 抖动 + 重试 —— 
GLOBAL_MIN_INTERVAL = float(os.environ.get("GLOBAL_MIN_INTERVAL", "10"))  # 两次发送最小间隔（秒）
PER_HOUR_LIMIT      = int(os.environ.get("PER_HOUR_LIMIT", "60"))         # 每小时最多条数
JITTER_MAX_SEC      = float(os.environ.get("JITTER_MAX_SEC", "2"))        # 抖动上限（秒）
MAX_RETRIES         = int(os.environ.get("MAX_RETRIES", "3"))             # 失败重试次数上限

# —— 去重 —— 
DEDUP_TTL_SECONDS   = int(os.environ.get("DEDUP_TTL_SECONDS", "86400"))   # 去重缓存有效期（秒）
HASH_SAMPLE_MB      = int(os.environ.get("HASH_SAMPLE_MB", "5"))          # 采样前 N MB 做哈希

# —— 发送队列/worker 并发度（1 = 严格串行；2/3 = 小并发）——
SEND_WORKERS = int(os.getenv("SEND_WORKERS", "1"))

# ===== 初始化 SDK =====
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(LINE_CHANNEL_SECRET)

from telegram import Bot
from telegram.request import HTTPXRequest
from telegram.error import NetworkError, Forbidden, BadRequest, RetryAfter, TimedOut

# 放大连接池 + 更宽松的超时，上传大视频更稳
request = HTTPXRequest(
    connection_pool_size=50,   # 默认太小，这里放大
    pool_timeout=120,          # 等可用连接的时间
    read_timeout=60 * 60,      # 读超时放到 1 小时
    write_timeout=60 * 10,     # 写入超时
    connect_timeout=60,        # 连接超时
)
bot = Bot(BOT_TOKEN, request=request)

app = FastAPI()

# ===== 状态（限速/上限/去重/队列）=====
SEND_LOCK = asyncio.Lock()      # 限速时的串行锁
LAST_SEND_TS = 0.0
SEND_WINDOW = deque()           # 最近一小时的发送时间戳
DEDUP_CACHE = {}                # sha1 -> ts

SEND_QUEUE: asyncio.Queue = asyncio.Queue()  # 发送任务队列

# ========= 工具函数 =========

async def tg_send_text(text: str):
    await bot.send_message(chat_id=TG_TARGET, text=text)

def _sha1_sample(path: str, sample_mb: int) -> str:
    h = hashlib.sha1()
    remain = sample_mb * 1024 * 1024
    with open(path, "rb") as f:
        while remain > 0:
            chunk = f.read(min(1024 * 1024, remain))
            if not chunk:
                break
            h.update(chunk)
            remain -= len(chunk)
    return h.hexdigest()

def _dedup_hit(sha1: str) -> bool:
    now = time.time()
    for k, ts in list(DEDUP_CACHE.items()):
        if now - ts > DEDUP_TTL_SECONDS:
            DEDUP_CACHE.pop(k, None)
    return sha1 in DEDUP_CACHE

def _dedup_mark(sha1: str):
    DEDUP_CACHE[sha1] = time.time()

def _probe_duration(path: str) -> float:
    try:
        out = subprocess.check_output(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=nokey=1:noprint_wrappers=1", path],
            stderr=subprocess.STDOUT,
        ).decode().strip()
        return float(out)
    except Exception:
        return 0.0

def _probe_dims(path: str) -> Tuple[int, int]:
    try:
        import cv2
        cap = cv2.VideoCapture(path)
        w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        cap.release()
        if w > 0 and h > 0:
            return w, h
    except Exception:
        pass
    return None, None

def _smart_compress(in_path: str) -> str:
    """
    强制压到 ≤ TARGET_MB（默认 47.5MB），保持比例不变。
    策略：按目标大小 & 时长反推视频码率，若仍偏大则做兜底二压（更高 CRF/更低码率）。
    返回压缩后文件路径；失败时返回原路径。
    """
    try:
        size_mb = os.path.getsize(in_path) / (1024 * 1024)
        print(f"[compress] input size = {size_mb:.1f} MB; target = {TARGET_MB} MB")
        if size_mb <= TARGET_MB:
            return in_path

        dur = _probe_duration(in_path)
        out_path = tempfile.mktemp(suffix=".mp4")

        if dur <= 0:
            # 时长未知：保守定参
            cmd = [
                "ffmpeg", "-y", "-i", in_path,
                "-vf", f"scale='min({SCALE_WIDTH},iw)':'-2',fps={TARGET_FPS}",
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "29",
                "-maxrate", "800k", "-bufsize", "1600k",
                "-profile:v", "baseline", "-level", "3.1", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", f"{AUDIO_KBPS}k", "-ac", "1",
                "-movflags", "+faststart", out_path
            ]
            subprocess.check_call(cmd)
        else:
            # 目标总码率（kbps）= 目标大小(MB)*8192 / 时长(s)
            total_kbps = max(int(TARGET_MB * 8192 / dur), 400)  # 保底
            video_kbps = max(total_kbps - AUDIO_KBPS, 300)
            print(f"[compress] duration={dur:.2f}s, total≈{total_kbps}kbps, video≈{video_kbps}kbps")
            cmd = [
                "ffmpeg", "-y", "-i", in_path,
                "-vf", f"scale='min({SCALE_WIDTH},iw)':'-2',fps={TARGET_FPS}",
                "-c:v", "libx264", "-preset", "veryfast",
                "-b:v", f"{video_kbps}k",
                "-maxrate", f"{video_kbps*2}k", "-bufsize", f"{video_kbps*4}k",
                "-profile:v", "baseline", "-level", "3.1", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", f"{AUDIO_KBPS}k", "-ac", "1",
                "-movflags", "+faststart", out_path
            ]
            subprocess.check_call(cmd)

        # 若仍 > 目标，再兜底二压
        out_mb = os.path.getsize(out_path) / (1024 * 1024)
        print(f"[compress] pass1 size = {out_mb:.1f} MB")
        if out_mb > TARGET_MB:
            out2 = tempfile.mktemp(suffix=".mp4")
            subprocess.check_call([
                "ffmpeg", "-y", "-i", out_path,
                "-vf", f"scale='min({SCALE_WIDTH},iw)':'-2',fps={TARGET_FPS}",
                "-c:v", "libx264", "-preset", "veryfast",
                "-crf", "30", "-maxrate", "650k", "-bufsize", "1300k",
                "-profile:v", "baseline", "-level", "3.1", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", f"{AUDIO_KBPS}k", "-ac", "1",
                "-movflags", "+faststart", out2
            ])
            try:
                os.remove(out_path)
            except Exception:
                pass
            out_path = out2
            out_mb = os.path.getsize(out_path) / (1024 * 1024)
            print(f"[compress] pass2 size = {out_mb:.1f} MB")

        return out_path

    except Exception as e:
        print("[warn] ffmpeg compress failed:", e)
        return in_path

def _prune_window():
    now = time.time()
    t0 = now - 3600
    while SEND_WINDOW and SEND_WINDOW[0] < t0:
        SEND_WINDOW.popleft()

async def _respect_rate_limits():
    """
    全局限速 + 每小时上限 + 抖动（防风控关键）
    """
    global LAST_SEND_TS
    async with SEND_LOCK:
        now = time.time()
        wait = LAST_SEND_TS + GLOBAL_MIN_INTERVAL - now
        if wait > 0:
            jitter = random.uniform(0, JITTER_MAX_SEC)
            print(f"[rate] wait {wait:.1f}s + jitter {jitter:.1f}s")
            await asyncio.sleep(wait + jitter)

        _prune_window()
        if len(SEND_WINDOW) >= PER_HOUR_LIMIT:
            sleep_sec = SEND_WINDOW[0] + 3600 - time.time()
            if sleep_sec > 0:
                jitter = random.uniform(0, JITTER_MAX_SEC)
                print(f"[rate] hourly cap hit, sleep {sleep_sec:.1f}s + jitter {jitter:.1f}s")
                await asyncio.sleep(sleep_sec + jitter)

        LAST_SEND_TS = time.time()
        SEND_WINDOW.append(LAST_SEND_TS)

# ===== 发送 worker / 队列 =====
async def _do_send(final_path: str, width, height):
    """真正的发送逻辑（带限速与重试）。"""
    await _respect_rate_limits()

    attempt, backoff = 0, 2.0
    while True:
        attempt += 1
        try:
            with open(final_path, "rb") as f:
                await bot.send_video(
                    chat_id=TG_TARGET,
                    video=f,  # 流式句柄，避免一次性读入内存
                    caption=BTN_URL or None,
                    supports_streaming=True,
                    width=width or None,
                    height=height or None,
                )
            break  # 成功
        except RetryAfter as e:
            wait = float(getattr(e, "retry_after", 5))
            jitter = random.uniform(0, JITTER_MAX_SEC)
            print(f"[retry] RetryAfter {wait}s + jitter {jitter:.1f}s")
            await asyncio.sleep(wait + jitter)
        except (NetworkError, TimedOut) as e:
            if attempt >= MAX_RETRIES:
                raise RuntimeError(f"❌ 网络错误，已重试 {attempt} 次仍失败：{e!r}")
            print(f"[retry] network error {e!r}, backoff {backoff:.1f}s")
            await asyncio.sleep(backoff + random.uniform(0, JITTER_MAX_SEC))
            backoff = min(backoff * 2, 60)
        except (Forbidden, BadRequest) as e:
            raise RuntimeError(f"❌ 权限/参数错误：{e!r}")

async def send_worker():
    """固定工人数，从队列取任务顺序执行。"""
    while True:
        job = await SEND_QUEUE.get()
        try:
            await job()
        except Exception as e:
            print(f"[worker] job failed: {e!r}")
        finally:
            SEND_QUEUE.task_done()

@app.on_event("startup")
async def _start_workers():
    for _ in range(max(1, SEND_WORKERS)):
        asyncio.create_task(send_worker())
    print(f"[startup] SEND_WORKERS = {max(1, SEND_WORKERS)}")

# ===== 健康检查 =====
@app.get("/")
def root():
    return {"ok": True}

# ===== LINE Webhook =====
@app.post("/webhook", response_class=PlainTextResponse)
async def webhook(request: Request, x_line_signature: str = Header(None, alias="X-Line-Signature")):
    body = await request.body()
    if x_line_signature is None:
        raise HTTPException(400, "No X-Line-Signature header")

    try:
        events = parser.parse(body.decode("utf-8"), x_line_signature)
    except Exception as e:
        raise HTTPException(400, f"Signature invalid: {e}")

    for event in events:
        if isinstance(event, MessageEvent):
            if isinstance(event.message, VideoMessage):
                await handle_binary_message(event.message.id)
            elif isinstance(event.message, FileMessage):
                await handle_binary_message(event.message.id)
            elif isinstance(event.message, TextMessage):
                if event.message.text.strip().lower() == "ping":
                    await tg_send_text("pong from LINE webhook (bot mode)")
    return "OK"

# ===== 主流程：下载 -> 去重 -> 压缩 -> （排队）发送 =====
async def handle_binary_message(message_id: str):
    start = time.time()

    # 1) 下载 LINE 媒体
    content = line_bot_api.get_message_content(message_id)
    content_type = ""
    try:
        content_type = content.headers.get("Content-Type", "")
    except Exception:
        pass

    # 选择后缀（仅用于临时文件名）
    suffix = ".mp4"
    ct = (content_type or "").lower()
    if "quicktime" in ct or "/mov" in ct:
        suffix = ".mov"
    elif "matroska" in ct or "mkv" in ct:
        suffix = ".mkv"
    elif "webm" in ct:
        suffix = ".webm"

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp_path = tmp.name
        for chunk in content.iter_content():
            if chunk:
                tmp.write(chunk)

    # 2) 去重（前 HASH_SAMPLE_MB MB 采样）
    sha1 = _sha1_sample(tmp_path, HASH_SAMPLE_MB)
    if _dedup_hit(sha1):
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        print(f"⤴ skip duplicate (sha1={sha1[:10]}...), LINE msg={message_id}")
        return
    _dedup_mark(sha1)

    # 3) 强制压到目标大小（保持比例不变）
    final_path = _smart_compress(tmp_path)

    # 4) 探测宽高（可选，避免显示比例异常）
    width, height = _probe_dims(final_path)

    # 5) 投递到队列，由 worker 顺序上传（发送完再清理临时文件）
    async def job():
        try:
            await _do_send(final_path, width, height)
        finally:
            for p in {tmp_path, final_path}:
                try:
                    if p and os.path.exists(p):
                        os.remove(p)
                except Exception:
                    pass

    await SEND_QUEUE.put(job)

    cost = time.time() - start
    print(
        f"✔ queued video: {width}x{height}, prep {cost:.1f}s, "
        f"ctype='{content_type}', target~{TARGET_MB}MB, caption={'on' if BTN_URL else 'off'}"
    )
