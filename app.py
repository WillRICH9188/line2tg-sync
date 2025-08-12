import os
import tempfile
import time

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
TG_TARGET = os.environ.get("TG_TARGET", "")  # @username 或 -100xxxxxxxxxx

# 可选：在视频下方显示的网址（作为 caption）
BTN_URL = os.environ.get("BTN_URL", "").strip()

if not (LINE_CHANNEL_SECRET and LINE_CHANNEL_ACCESS_TOKEN and API_ID and API_HASH and TELEGRAM_STRING_SESSION and TG_TARGET):
    raise RuntimeError("❌ 必填环境变量缺失：LINE/Telegram(MTProto) 相关配置未填全")

# LINE SDK
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(LINE_CHANNEL_SECRET)

# Pyrogram 用户客户端（MTProto）
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
                await handle_binary_message(event.message.id)
            elif isinstance(event.message, FileMessage):
                # 你要求「全部当影片发送」，文件也按影片尝试发送
                await handle_binary_message(event.message.id)
            elif isinstance(event.message, TextMessage):
                if event.message.text.strip().lower() == "ping":
                    await tg_send_text("pong from LINE webhook")
    return "OK"

async def handle_binary_message(message_id: str):
    """
    从 LINE 下载内容 -> 保存到临时文件（按 Content-Type 选择扩展名）
    -> 读取宽高 -> 以 send_video 发送并显式指定 width/height（避免手机端显示成正方形）。
    caption 仅放 BTN_URL（若为空则不带 caption）。
    """
    start = time.time()

    # 1) 下载 LINE 媒体并获取 Content-Type
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

    # 3) 读取视频宽高
    width, height = _probe_dims(tmp_path)

    # 4) 组织参数，caption 只放 URL
    send_kwargs = {
        "chat_id": TG_TARGET,
        "video": tmp_path,
        "supports_streaming": True,
    }
    if BTN_URL:
        send_kwargs["caption"] = BTN_URL
    if width and height:
        send_kwargs["width"] = width
        send_kwargs["height"] = height

    # 5) 发送视频
    await tg.send_video(**send_kwargs)

    # 6) 清理
    try:
        os.remove(tmp_path)
    except Exception:
        pass

    cost = time.time() - start
    print(f"✔ synced video: {width}x{height}, {cost:.1f}s, ctype='{content_type}', suffix='{suffix}', caption={'on' if BTN_URL else 'off'}")

# 健康检查
@app.get("/")
def root():
    return {"ok": True}
