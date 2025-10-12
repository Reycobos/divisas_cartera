import json
import gzip
import io
import websocket
import threading
import time
from datetime import datetime
import requests
import time
import hmac
import hashlib
from urllib.parse import urlencode
import os
from dotenv import load_dotenv

# ========================
# CONFIGURACIÓN
# ========================

LISTEN_KEY = "TU_LISTEN_KEY"  # se obtiene con /openApi/swap/v2/user/stream
WS_URL = f"wss://open-api-cswap-ws.bingx.com/market?listenKey={LISTEN_KEY}"

# Diccionario global que guarda los últimos funding fees detectados
latest_funding = {}


def on_message(ws, message):
    try:
        compressed = gzip.GzipFile(fileobj=io.BytesIO(message), mode='rb')
        decompressed = compressed.read().decode('utf-8')
        data = json.loads(decompressed)
    except Exception:
        data = json.loads(message)

    # Respuesta al ping
    if "ping" in str(data):
        ws.send("Pong")
        return

    # Evento de actualización de cuenta
    if data.get("e") == "ACCOUNT_UPDATE":
        reason = data["a"].get("m")
        if reason == "FUNDING_FEE":
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for b in data["a"].get("B", []):
                asset = b["a"]
                change = float(b["bc"])
                latest_funding[asset] = {
                    "timestamp": ts,
                    "asset": asset,
                    "amount": change
                }
                print(f"[{ts}] 💸 FUNDING {asset}: {change}")
            for p in data["a"].get("P", []):
                sym = p.get("s")
                side = p.get("ps")
                print(f"  ↳ {sym} ({side}) funding actualizado")

def on_error(ws, error):
    print("❌ WebSocket error:", error)

def on_close(ws, *_):
    print("🔌 WebSocket cerrado")

def on_open(ws):
    print("✅ Conectado al WebSocket BingX (ACCOUNT_UPDATE)")

def run_ws_forever():
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_open=on_open
            )
            ws.run_forever(ping_interval=50)
        except Exception as e:
            print(f"[ERROR] Reconectando WebSocket: {e}")
        time.sleep(10)

# Lanzar el listener en segundo plano
def start_bingx_ws_listener():
    thread = threading.Thread(target=run_ws_forever, daemon=True)
    thread.start()
    print("🚀 BingX WebSocket listener iniciado")
    return latest_funding
