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
CAPTION_PREFIX = os.environ.get("CAPTION_PREFIX", "Happy Channel")

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
                # 你要求「无论大小都当影片发送」，这里也按影片处理
                name = event.message.file_name or "video"
                await handle_binary_message(event.message.id, f"{CAPTION_PREFIX} · {name}")
            elif isinstance(event.message, TextMessage):
                if event.message.text.strip().lower() == "ping":
                    await tg_send_text("pong from LINE webhook")
    return "OK"

async def handle_binary_message(message_id: str, caption: str):
    """
    从 LINE 下载内容 -> 保存到临时 .mp4 -> 始终以 send_video 发送（不走 document）。
    注意：Telegram 单文件上限 2GB；若源文件并非有效视频容器/编码，TG 可能报错。
    """
    start = time.time()

    # 下载 LINE 媒体
    content = line_bot_api.get_message_content(message_id)

    # 强制使用 .mp4 扩展名（多数 LINE 视频就是 mp4；扩展名能帮助 TG 识别为视频）
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
        tmp_path = tmp.name
        for chunk in content.iter_content():
            if chunk:
                tmp.write(chunk)

    size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
    cap = f"{caption}\n(size: {size_mb:.1f}MB)"

    # 始终用 send_video（可播放）。不做 document 兜底。
    await tg.send_video(
        chat_id=TG_TARGET,
        video=tmp_path,
        caption=cap,
        supports_streaming=True,  # 提示客户端按流媒体处理
    )

    # 清理临时文件
    try:
        os.remove(tmp_path)
    except Exception:
        pass

    cost = time.time() - start
    print(f"✔ synced to TG as VIDEO: {size_mb:.1f}MB, {cost:.1f}s")

# 健康检查
@app.get("/")
def root():
    return {"ok": True}
