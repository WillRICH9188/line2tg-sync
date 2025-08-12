import os
import tempfile
import time
from typing import Any, Dict

import requests
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import PlainTextResponse

from linebot import LineBotApi, WebhookParser
from linebot.models import MessageEvent, VideoMessage, FileMessage, TextMessage

# ----- LINE 环境变量 -----
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")

# ----- Telegram（用户账号 MTProto）环境变量 -----
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
TELEGRAM_STRING_SESSION = os.environ.get("TELEGRAM_STRING_SESSION", "")
TG_TARGET = os.environ.get("TG_TARGET", "")  # @username 或 -100xxx
CAPTION_PREFIX = os.environ.get("CAPTION_PREFIX", "Happy Channel")

if not (LINE_CHANNEL_SECRET and LINE_CHANNEL_ACCESS_TOKEN and API_ID and API_HASH and TELEGRAM_STRING_SESSION and TG_TARGET):
    raise RuntimeError("❌ 必填环境变量缺失：LINE/Telegram(MTProto) 相关配置未填全")

# LINE SDK
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(LINE_CHANNEL_SECRET)

# Pyrogram 用户客户端
from pyrogram import Client
tg = Client(
    name="line2tg",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=TELEGRAM_STRING_SESSION,
)

app = FastAPI()

@app.on_event("startup")
async def _startup():
    await tg.start()

@app.on_event("shutdown")
async def _shutdown():
    await tg.stop()

# （可选）发送普通文本（用户账号）
async def tg_send_text(text: str):
    await tg.send_message(chat_id=TG_TARGET, text=text)

# ----- LINE Webhook -----
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
                await handle_binary_message(event.message.id, f"{CAPTION_PREFIX} · video (from LINE)")
            elif isinstance(event.message, FileMessage):
                name = event.message.file_name or "file"
                await handle_binary_message(event.message.id, f"{CAPTION_PREFIX} · {name}")
            elif isinstance(event.message, TextMessage):
                if event.message.text.strip().lower() == "ping":
                    await tg_send_text("pong from LINE webhook")
    return "OK"

async def handle_binary_message(message_id: str, caption: str):
    """
    从 LINE 下载二进制内容，保存临时文件，再用『用户账号』发送到 Telegram。
    为确保大小不变，使用 send_document 发送原文件（不压缩、不转码）。
    """
    start = time.time()
    content = line_bot_api.get_message_content(message_id)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as tmp:
        tmp_path = tmp.name
        for chunk in content.iter_content():
            if chunk:
                tmp.write(chunk)

    size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
    cap = f"{caption}\n(size: {size_mb:.1f}MB)"

    # 发送为 document：大小保持不变，可上传到 2GB（Telegram 限制）
    await tg.send_document(chat_id=TG_TARGET, document=tmp_path, caption=cap)

    try:
        os.remove(tmp_path)
    except Exception:
        pass

    cost = time.time() - start
    print(f"✔ synced to TG (document) -> {size_mb:.1f}MB, {cost:.1f}s")

@app.get("/")
def root():
    return {"ok": True}
