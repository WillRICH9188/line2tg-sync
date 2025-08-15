# app.py —— LINE 群视频同步到 Telegram 频道（Bot 版，带限速/去重/重试，≤2GB）

import os
import tempfile
import time
import asyncio
import random
import hashlib
from collections import deque

from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import PlainTextResponse

from linebot import LineBotApi, WebhookParser
from linebot.models import MessageEvent, VideoMessage, FileMessage, TextMessage

# ===== 环境变量 =====
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "").strip()
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "").strip()

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
TG_TARGET = os.environ.get("TG_TARGET", "").strip()  # 推荐 -100xxxxxxxxxx

BTN_URL = os.environ.get("BTN_URL", "").strip()

# 可调风控参数（秒、条）
GLOBAL_MIN_INTERVAL = float(os.environ.get("GLOBAL_MIN_INTERVAL", "8"))   # 两条视频最少间隔秒
PER_HOUR_LIMIT = int(os.environ.get("PER_HOUR_LIMIT", "120"))             # 每小时上限
DEDUP_TTL_SECONDS = int(os.environ.get("DEDUP_TTL_SECONDS", "86400"))     # 去重缓存有效期（默认1天）
HASH_SAMPLE_MB = int(os.environ.get("HASH_SAMPLE_MB", "5"))               # 采样前N MB做哈希
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))                     # 失败重试上限

if not (LINE_CHANNEL_SECRET and LINE_CHANNEL_ACCESS_TOKEN and BOT_TOKEN and TG_TARGET):
    raise RuntimeError("❌ 必填环境变量缺失：请设置 LINE_CHANNEL_SECRET / LINE_CHANNEL_ACCESS_TOKEN / BOT_TOKEN / TG_TARGET")

# ===== 初始化 SDK =====
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(LINE_CHANNEL_SECRET)

# Telegram Bot（python-telegram-bot 21.x）
from telegram import Bot
from telegram.error import NetworkError, Forbidden, BadRequest, RetryAfter, TimedOut

bot = Bot(BOT_TOKEN)

app = FastAPI()

# ===== 速率控制 / 去重状态 =====
SEND_LOCK = asyncio.Lock()
LAST_SEND_TS = 0.0
SEND_WINDOW = deque()  # 最近1小时的发送时间戳
DEDUP_CACHE = {}       # sha1 -> ts

# （可选）发送纯文本
async def tg_send_text(text: str):
    await bot.send_message(chat_id=TG_TARGET, text=text)

# ---- 工具函数 ----
def _ext_from_content_type(ct: str) -> str:
    if not ct:
        return ".mp4"
    ct = ct.lower()
    if "video/mp4" in ct:
        return ".mp4"
    if "video/quicktime" in ct or "/mov" in ct:
        return ".mov"
    if "video/x-matroska" in ct or "mkv" in ct:
        return ".mkv"
    if "video/webm" in ct:
        return ".webm"
    return ".mp4"

def _probe_dims(path: str):
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

def _sha1_sample(path: str, sample_mb: int) -> str:
    """对前 sample_mb MB 数据做 sha1，足以做短期去重"""
    h = hashlib.sha1()
    bytes_to_read = sample_mb * 1024 * 1024
    with open(path, "rb") as f:
        while bytes_to_read > 0:
            chunk = f.read(min(1024 * 1024, bytes_to_read))
            if not chunk:
                break
            h.update(chunk)
            bytes_to_read -= len(chunk)
    return h.hexdigest()

def _dedup_hit(sha1: str) -> bool:
    """检查/清理去重缓存"""
    now = time.time()
    # 清理过期
    expired = [k for k, ts in DEDUP_CACHE.items() if now - ts > DEDUP_TTL_SECONDS]
    for k in expired:
        DEDUP_CACHE.pop(k, None)
    # 命中？
    return sha1 in DEDUP_CACHE

def _dedup_mark(sha1: str):
    DEDUP_CACHE[sha1] = time.time()

def _prune_window():
    """维护1小时滚动窗口"""
    now = time.time()
    one_hour_ago = now - 3600
    while SEND_WINDOW and SEND_WINDOW[0] < one_hour_ago:
        SEND_WINDOW.popleft()

async def _respect_rate_limits():
    """全局限速 + 每小时上限 + 抖动"""
    global LAST_SEND_TS
    async with SEND_LOCK:
        # 1) 最小间隔
        now = time.time()
        wait = LAST_SEND_TS + GLOBAL_MIN_INTERVAL - now
        if wait > 0:
            await asyncio.sleep(wait + random.uniform(0, 2))  # 抖动

        # 2) 每小时上限
        _prune_window()
        if len(SEND_WINDOW) >= PER_HOUR_LIMIT:
            # 等到窗口最早一个发送点 +3600 到来
            sleep_sec = SEND_WINDOW[0] + 3600 - time.time()
            if sleep_sec > 0:
                await asyncio.sleep(sleep_sec + random.uniform(0, 2))

        # 更新时间戳与窗口（占位）
        LAST_SEND_TS = time.time()
        SEND_WINDOW.append(LAST_SEND_TS)

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

async def handle_binary_message(message_id: str):
    """
    从 LINE 下载 -> 保存临时文件 -> 读取宽高
    -> 去重检查 -> 限速/重试发送到 Telegram 频道/超级群（≤2GB）
    """
    start = time.time()

    # 1) 下载 LINE 媒体
    content = line_bot_api.get_message_content(message_id)
    content_type = ""
    try:
        content_type = content.headers.get("Content-Type", "")
    except Exception:
        pass
    suffix = _ext_from_content_type(content_type)

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp_path = tmp.name
        for chunk in content.iter_content():
            if chunk:
                tmp.write(chunk)

    # 2) 去重（采样哈希）
    sha1 = _sha1_sample(tmp_path, HASH_SAMPLE_MB)
    if _dedup_hit(sha1):
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        print(f"⤴ skip duplicate (sha1={sha1[:10]}...), from LINE msg {message_id}")
        return
    _dedup_mark(sha1)

    # 3) 读取视频宽高
    width, height = _probe_dims(tmp_path)

    # 4) 限速（串行 + 最小间隔 + 每小时上限）
    await _respect_rate_limits()

    # 5) 发送（带退避重试）
    attempt = 0
    backoff = 2.0
    try:
        while True:
            attempt += 1
            try:
                with open(tmp_path, "rb") as f:
                    await bot.send_video(
                        chat_id=TG_TARGET,
                        video=f,
                        caption=BTN_URL or None,
                        supports_streaming=True,
                        width=width or None,
                        height=height or None,
                    )
                break  # 成功
            except RetryAfter as e:
                # Telegram 明确要求等待 e.retry_after 秒
                await asyncio.sleep(float(getattr(e, "retry_after", 5)) + random.uniform(0, 2))
            except (NetworkError, TimedOut) as e:
                if attempt >= MAX_RETRIES:
                    raise RuntimeError(f"❌ 网络错误已重试 {attempt} 次仍失败：{e}")
                await asyncio.sleep(backoff + random.uniform(0, 2))
                backoff = min(backoff * 2, 60)  # 指数退避，封顶 60s
            except (Forbidden, BadRequest) as e:
                raise RuntimeError(f"❌ 权限/参数错误：{e}")
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    cost = time.time() - start
    print(f"✔ synced video: {width}x{height}, {cost:.1f}s, ctype='{content_type}', caption={'on' if BTN_URL else 'off'}")
