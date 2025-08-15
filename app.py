# app.py —— LINE 群视频同步到 Telegram 频道（Bot 版本，支持 ≤2GB）

import os
import tempfile
import time
import asyncio

from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import PlainTextResponse

from linebot import LineBotApi, WebhookParser
from linebot.models import MessageEvent, VideoMessage, FileMessage, TextMessage

# ===== 环境变量 =====
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "").strip()
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "").strip()

# 仅需 Bot 凭证 & 目标频道/超级群
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
TG_TARGET = os.environ.get("TG_TARGET", "").strip()  # 推荐用 -100xxxxxxxxxx

# 可选：视频下方显示的网址（caption）
BTN_URL = os.environ.get("BTN_URL", "").strip()

if not (LINE_CHANNEL_SECRET and LINE_CHANNEL_ACCESS_TOKEN and BOT_TOKEN and TG_TARGET):
    raise RuntimeError("❌ 必填环境变量缺失：请设置 LINE_CHANNEL_SECRET / LINE_CHANNEL_ACCESS_TOKEN / BOT_TOKEN / TG_TARGET")

# ===== 初始化 SDK =====
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(LINE_CHANNEL_SECRET)

# Telegram Bot（python-telegram-bot 21.x，为异步接口）
from telegram import Bot
from telegram.error import NetworkError, Forbidden, BadRequest

bot = Bot(BOT_TOKEN)

app = FastAPI()


# （可选）发送纯文本到频道
async def tg_send_text(text: str):
    await bot.send_message(chat_id=TG_TARGET, text=text)


# ---- 工具函数 ----
def _ext_from_content_type(ct: str) -> str:
    """根据 Content-Type 猜扩展名，尽量保留原容器，帮助 TG 正确识别比例"""
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
    """用 OpenCV 读取视频宽高，失败则返回 (None, None)"""
    try:
        import cv2  # 延迟导入，避免冷启动报错
        cap = cv2.VideoCapture(path)
        w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        cap.release()
        if w > 0 and h > 0:
            return w, h
    except Exception:
        pass
    return None, None


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
                # 要求「文件也按影片发送」
                await handle_binary_message(event.message.id)
            elif isinstance(event.message, TextMessage):
                if event.message.text.strip().lower() == "ping":
                    await tg_send_text("pong from LINE webhook (bot mode)")
    return "OK"


async def handle_binary_message(message_id: str):
    """
    从 LINE 下载 -> 写入临时文件（按 Content-Type 选扩展名）
    -> 读取宽高 -> 使用 Bot API 以 send_video 发送到频道/超级群（支持 ≤2GB）。
    caption 仅放 BTN_URL（若为空则不带 caption）。
    """
    start = time.time()

    # 1) 下载 LINE 媒体并获取 Content-Type（流式）
    content = line_bot_api.get_message_content(message_id)
    content_type = ""
    try:
        content_type = content.headers.get("Content-Type", "")
    except Exception:
        pass

    suffix = _ext_from_content_type(content_type)

    # 2) 写入临时文件
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp_path = tmp.name
        for chunk in content.iter_content():
            if chunk:
                tmp.write(chunk)

    # 3) 读取视频宽高（可选，用于避免客户端显示成正方形）
    width, height = _probe_dims(tmp_path)

    # 4) 发送视频（Bot API，不需要 API_ID/API_HASH）
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
    except NetworkError as e:
        # 413: Request Entity Too Large（文件超过限制）等网络层错误
        raise RuntimeError(f"❌ Telegram NetworkError：{e}")
    except (Forbidden, BadRequest) as e:
        # 典型：Bot 不是管理员 / chat_id 不对 / 频道私有等
        raise RuntimeError(f"❌ Telegram 权限/参数错误：{e}")
    finally:
        # 5) 清理
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    cost = time.time() - start
    print(f"✔ synced video: {width}x{height}, {cost:.1f}s, ctype='{content_type}', suffix='{suffix}', caption={'on' if BTN_URL else 'off'}")
