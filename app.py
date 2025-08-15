# app.py —— LINE 群视频同步到 Telegram 频道（Bot 版本，强制压到 ~47–48MB）
# ---------------------------------------------------------------
# 说明：
# 1) 从 LINE Webhook 收到视频/文件后，先下载到临时文件
# 2) 使用 ffmpeg 计算目标码率并转码，使总文件大小落在 ~47–48MB
# 3) 比例不变：scale 使用 "scale='min(W,iw)':'-2'"，宽限制 W，-2 表示按比例取整为 2 的倍数
# 4) 若一次计算仍偏大，会做“兜底二压”（更高 CRF、更低码率）
# ---------------------------------------------------------------

import os
import tempfile
import time
import subprocess
from typing import Tuple

from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import PlainTextResponse

from linebot import LineBotApi, WebhookParser
from linebot.models import MessageEvent, VideoMessage, FileMessage, TextMessage

# ===== 环境变量 =====
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "").strip()
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "").strip()

# Telegram Bot & 目标
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
TG_TARGET = os.environ.get("TG_TARGET", "").strip()  # 建议用 -100xxxxxxxxxx

# 可选：视频下方显示的网址（caption）
BTN_URL = os.environ.get("BTN_URL", "").strip()

# 压缩参数（可用环境变量微调）
TARGET_MB = float(os.getenv("TG_TARGET_MB", "47.5"))      # 目标大小，安全余量 <50MB
AUDIO_KBPS = int(os.getenv("TG_AUDIO_KBPS", "64"))         # 音频码率
SCALE_WIDTH = int(os.getenv("TG_SCALE_W", "720"))          # 最大宽度（保持比例），可设 640 更狠
TARGET_FPS = int(os.getenv("TG_FPS", "24"))                # 降帧率，24 基本不太影响观看

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


# ========= 小工具 =========

def _probe_duration(path: str) -> float:
    """用 ffprobe 获取时长（秒），失败则返回 0.0"""
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
    """（可选）用 OpenCV 获取宽高，失败返回 (None, None)"""
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
    强制把视频压到 ≤ TARGET_MB（默认 47.5MB），返回新文件路径；
    若压缩出错，返回原文件路径。
    """
    try:
        size_mb = os.path.getsize(in_path) / (1024 * 1024)
        # 已经足够小就不动
        if size_mb <= TARGET_MB:
            return in_path

        dur = _probe_duration(in_path)
        out_path = tempfile.mktemp(suffix=".mp4")

        if dur <= 0:
            # 时长未知：走“保守定参数”策略（更狠的压缩）
            cmd = [
                "ffmpeg", "-y", "-i", in_path,
                "-vf", f"scale='min({SCALE_WIDTH},iw)':'-2',fps={TARGET_FPS}",
                "-c:v", "libx264", "-preset", "veryfast",
                "-crf", "29",                      # 比较狠
                "-maxrate", "800k", "-bufsize", "1600k",
                "-profile:v", "baseline", "-level", "3.1", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", f"{AUDIO_KBPS}k", "-ac", "1",
                "-movflags", "+faststart",
                out_path
            ]
            subprocess.check_call(cmd)
        else:
            # 计算总码率目标（kbps）：目标大小(MB)*8192 / 时长(s)
            total_kbps = max(int(TARGET_MB * 8192 / dur), 400)  # 给最低门槛
            # 分配给视频的码率
            video_kbps = max(total_kbps - AUDIO_KBPS, 300)

            cmd = [
                "ffmpeg", "-y", "-i", in_path,
                "-vf", f"scale='min({SCALE_WIDTH},iw)':'-2',fps={TARGET_FPS}",
                "-c:v", "libx264", "-preset", "veryfast",
                "-b:v", f"{video_kbps}k",
                "-maxrate", f"{video_kbps*2}k", "-bufsize", f"{video_kbps*4}k",
                "-profile:v", "baseline", "-level", "3.1", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", f"{AUDIO_KBPS}k", "-ac", "1",
                "-movflags", "+faststart",
                out_path
            ]
            subprocess.check_call(cmd)

        # 若仍然 > 目标，再兜底二压：进一步提高 CRF、降低 Maxrate
        if os.path.getsize(out_path) / (1024 * 1024) > TARGET_MB:
            out2 = tempfile.mktemp(suffix=".mp4")
            subprocess.check_call([
                "ffmpeg", "-y", "-i", out_path,
                "-vf", f"scale='min({SCALE_WIDTH},iw)':'-2',fps={TARGET_FPS}",
                "-c:v", "libx264", "-preset", "veryfast",
                "-crf", "30",                      # 再狠一点
                "-maxrate", "650k", "-bufsize", "1300k",
                "-profile:v", "baseline", "-level", "3.1", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", f"{AUDIO_KBPS}k", "-ac", "1",
                "-movflags", "+faststart",
                out2
            ])
            os.remove(out_path)
            out_path = out2

        return out_path

    except Exception as e:
        print("[warn] ffmpeg compress failed:", e)
        return in_path


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
                # 文件也按“视频”尝试发送（强制压缩后当 Video）
                await handle_binary_message(event.message.id)
            elif isinstance(event.message, TextMessage):
                if event.message.text.strip().lower() == "ping":
                    await bot.send_message(chat_id=TG_TARGET, text="pong from LINE webhook (bot mode)")
    return "OK"


async def handle_binary_message(message_id: str):
    """
    从 LINE 下载 -> 写入临时文件 -> 强制压到 ~47–48MB -> 发送到 Telegram 频道
    """
    start = time.time()

    # 1) 下载 LINE 媒体
    content = line_bot_api.get_message_content(message_id)
    content_type = ""
    try:
        content_type = content.headers.get("Content-Type", "")
    except Exception:
        pass

    # 选择后缀（仅影响封装名）
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

    # 2) 压到目标大小
    final_path = _smart_compress(tmp_path)

    # 3) （可选）探测宽高给 TG，避免个别端显示比例异常
    width, height = _probe_dims(final_path)

    # 4) 发送到 Telegram
    try:
        with open(final_path, "rb") as f:
            await bot.send_video(
                chat_id=TG_TARGET,
                video=f,
                caption=BTN_URL or None,
                supports_streaming=True,
                width=width or None,
                height=height or None,
            )
    except NetworkError as e:
        raise RuntimeError(f"❌ Telegram NetworkError：{e}")
    except (Forbidden, BadRequest) as e:
        raise RuntimeError(f"❌ Telegram 权限/参数错误：{e}")
    finally:
        # 5) 清理临时文件
        for p in {tmp_path, final_path}:
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

    cost = time.time() - start
    print(f"✔ synced video: {width}x{height}, {cost:.1f}s, ctype='{content_type}', target~{TARGET_MB}MB, caption={'on' if BTN_URL else 'off'}")
