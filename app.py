import os
import tempfile
import time
from typing import Any, Dict

import requests
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import PlainTextResponse

from linebot import LineBotApi, WebhookParser
from linebot.models import (
    MessageEvent,
    VideoMessage,
    FileMessage,
    TextMessage,
)

# ----- 环境变量 -----
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")  # 频道/群ID，如 -100xxxxxxxxxx
CAPTION_PREFIX = os.environ.get("CAPTION_PREFIX", "Happy Channel")  # 发送到TG时自动带上的前缀

if not (LINE_CHANNEL_SECRET and LINE_CHANNEL_ACCESS_TOKEN and TG_BOT_TOKEN and TG_CHAT_ID):
    raise RuntimeError("❌ 必填环境变量缺失：LINE_CHANNEL_SECRET / LINE_CHANNEL_ACCESS_TOKEN / TG_BOT_TOKEN / TG_CHAT_ID")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(LINE_CHANNEL_SECRET)

app = FastAPI()

# ----- Telegram 发送 -----
def tg_send_video(file_path: str, caption: str = "") -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendVideo"
    with open(file_path, "rb") as f:
        files = {"video": f}
        data = {
            "chat_id": TG_CHAT_ID,
            "caption": caption,
            "supports_streaming": True,
            "disable_notification": False,
        }
        r = requests.post(url, data=data, files=files, timeout=600)
        r.raise_for_status()
        return r.json()

# （可选）发送普通文本
def tg_send_text(text: str) -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    r = requests.post(url, data={"chat_id": TG_CHAT_ID, "text": text}, timeout=30)
    r.raise_for_status()
    return r.json()

# ----- LINE Webhook -----
@app.post("/webhook", response_class=PlainTextResponse)
async def webhook(request: Request, x_line_signature: str = Header(None)):
    body = await request.body()
    if x_line_signature is None:
        raise HTTPException(400, "No X-Line-Signature header")

    try:
        events = parser.parse(body.decode("utf-8"), x_line_signature)
    except Exception as e:
        raise HTTPException(400, f"Signature invalid: {e}")

    for event in events:
        # 只处理消息事件
        if isinstance(event, MessageEvent):
            user = getattr(event.source, "user_id", None)
            gid = getattr(event.source, "group_id", None)
            rid = getattr(event.source, "room_id", None)

            # 1) 影片（VideoMessage）
            if isinstance(event.message, VideoMessage):
                msg_id = event.message.id
                caption = f"{CAPTION_PREFIX} · video (from LINE)"
                await handle_binary_message(msg_id, caption)

            # 2) 文件（FileMessage，某些设备/客户端发视频会是文件）
            elif isinstance(event.message, FileMessage):
                msg_id = event.message.id
                name = event.message.file_name or "file"
                caption = f"{CAPTION_PREFIX} · {name}"
                await handle_binary_message(msg_id, caption)

            # 3) 可选：文本调试
            elif isinstance(event.message, TextMessage):
                txt = event.message.text.strip()
                if txt.lower() == "ping":
                    tg_send_text("pong from LINE webhook")
    return "OK"

async def handle_binary_message(message_id: str, caption: str):
    """
    从 LINE 下载二进制内容（视频/文件），保存到临时文件，再发到 Telegram。
    """
    start = time.time()
    # 下载内容：line_bot_api.get_message_content 会返回一个 StreamResponse
    content = line_bot_api.get_message_content(message_id)

    # 用临时文件保存
    suffix = ".mp4"  # 大多数视频可当 mp4 发；若想更稳，可先存为 .bin
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp_path = tmp.name
        for chunk in content.iter_content():
            if chunk:
                tmp.write(chunk)

    size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
    # 发往 Telegram
    cap = f"{caption}\n(size: {size_mb:.1f}MB)"
    tg_send_video(tmp_path, cap)

    # 清理
    try:
        os.remove(tmp_path)
    except Exception:
        pass

    cost = time.time() - start
    print(f"✔ synced video -> TG: {size_mb:.1f}MB, {cost:.1f}s")

# 健康检查
@app.get("/")
def root():
    return {"ok": True}
