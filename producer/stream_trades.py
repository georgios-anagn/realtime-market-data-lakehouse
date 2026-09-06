import json
import os
import queue
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import websocket
from dotenv import load_dotenv

from uploader import upload_file

load_dotenv()

FINNHUB_TOKEN = os.environ["FINNHUB_API_KEY"]
SYMBOLS = os.environ.get("SYMBOLS", "AAPL,MSFT,GOOGL,AMZN,NVDA,TSLA,META").split(",")
FLUSH_INTERVAL_SECONDS = int(os.environ.get("FLUSH_INTERVAL_SECONDS", "60"))
VOLUME_PATH = os.environ["DATABRICKS_VOLUME_PATH"]
LOCAL_LANDING_DIR = Path("data/landing")
LOCAL_LANDING_DIR.mkdir(parents=True, exist_ok=True)

buffer: "queue.Queue[dict]" = queue.Queue()


def on_message(ws, message):
    payload = json.loads(message)
    if payload.get("type") != "trade":
        return  # ignore ping/pong and other message types
    for trade in payload.get("data", []):
        buffer.put(trade)

def on_error(ws, error):
    print(f"[ws error] {error}")


def on_close(ws, code, msg):
    print(f"[ws closed] code={code} msg={msg}")


def on_open(ws):
    for symbol in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
    print(f"Subscribed to: {SYMBOLS}")


def flush_loop():
    """Runs in a background thread: every 60 seconds, drain the buffer
    to a local NDJSON file and upload it as a micro-batch."""
    while True:
        time.sleep(FLUSH_INTERVAL_SECONDS)
        records = []
        while not buffer.empty():
            records.append(buffer.get())

        if not records:
            print("No trades this interval (market may be closed).")
            continue

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        filename = f"trades_{ts}.json"
        local_file = LOCAL_LANDING_DIR / filename

        with open(local_file, "w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

        try:
            upload_file(local_file, f"{VOLUME_PATH}/{filename}")
            print(f"Uploaded {filename} ({len(records)} trades)")
        except Exception as e:
            print(f"[upload error] {e}")


if __name__ == "__main__":
    threading.Thread(target=flush_loop, daemon=True).start()

    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )
    ws.run_forever(reconnect=5)