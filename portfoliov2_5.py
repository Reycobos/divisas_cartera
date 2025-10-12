from flask import Flask, render_template, jsonify
import pandas as pd
import requests
import time
import hashlib
from dotenv import load_dotenv
import hmac
import os
from urllib.parse import urlencode
import json
import base64
import nacl.signing
import datetime
import math
from datetime import datetime, timezone
import json, urllib
from base64 import urlsafe_b64encode
from base58 import b58decode, b58encode
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from requests import Request, Session
from collections import defaultdict
from datetime import datetime, timedelta
from collections import defaultdict
import sqlite3
from db_manager import init_db, save_closed_position
from trades_processingv7 import (
    save_backpack_closed_positions,
    save_aster_closed_positions,
    save_binance_closed_positions,
)


# listen_key = get_listen_key()
# from bingx_ws_listener import start_bingx_ws_listener
# latest_funding_bingx = start_bingx_ws_listener(listen_key)



app = Flask(__name__)


# Verificar que la carpeta templates existe
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
if not os.path.exists(template_dir):
    print(f"⚠️ Creando carpeta ss: {template_dir}")
    os.makedirs(template_dir)

TEMPLATE_FILE = "indexv4.2.html" 
DB_PATH = "portfolio.db"
# Configuración de APIs
EXT_API_KEY = os.getenv("EXT_API_KEY")
EXT_API_SECRET = os.getenv("EXT_API_SECRET")
EXT_BASE_URL = os.getenv("EXT_BASE_URL", "https://api.starknet.extended.exchange")

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
BYBIT_BASE_URL = "https://api.bybit.com"

# Binance

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
BINANCE_BASE_URL = "https://fapi.binance.com"


# Backpack
BACKPACK_API_KEY = os.getenv("BACKPACK_API_KEY")
BACKPACK_API_SECRET = os.getenv("BACKPACK_API_SECRET") # Debe ser la clave privada ED25519 en base64
BACKPACK_BASE_URL = "https://api.backpack.exchange"

# Headers para Backpack
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Aster
ASTER_API_KEY = os.getenv("ASTER_API_KEY")
ASTER_API_SECRET = os.getenv("ASTER_API_SECRET")
ASTER_HOST = "https://fapi.asterdex.com"

# Aden

ORDERLY_SECRET = "GhxcFHy4s1b9EpguzyTUTdGAdEtnGXGNFEhe1gSc1WBN"  # clave privada en base58
ORDERLY_ACCOUNT_ID = os.getenv("ORDERLY_ACCOUNT_ID")
ORDERLY_BASE_URL = "https://api.orderly.org"

# generar private key desde secret base58
_private_key = Ed25519PrivateKey.from_private_bytes(b58decode(ORDERLY_SECRET))
_session = Session()

if not ORDERLY_SECRET:
    raise ValueError("❌ FALTA la variable ORDERLY_SECRET en el archivo .env")

try:
    _private_key = Ed25519PrivateKey.from_private_bytes(b58decode(ORDERLY_SECRET))
except Exception as e:
    raise ValueError(f"❌ Error al decodificar ORDERLY_SECRET: {e}")

_session = Session()

# derivar public key base58 (esta es la que va en el header orderly-key)
ORDERLY_PUBLIC_KEY_B58 = "ed25519:" + b58encode(
    _private_key.public_key().public_bytes_raw()
).decode("utf-8")

# Bing X

BINGX_API_KEY = os.getenv("BINGX_API_KEY")
BINGX_SECRET_KEY = os.getenv("BINGX_SECRET_KEY")
BINGX_BASE = "https://open-api.bingx.com"




#-------------- BinanceConfig---------

def binance_server_offset_ms():
    t0 = int(time.time() * 1000)
    r = requests.get(f"{BINANCE_BASE_URL}/fapi/v1/time", headers=UA_HEADERS, timeout=10)
    r.raise_for_status()
    server = r.json()["serverTime"]
    t1 = int(time.time() * 1000)
    return server - ((t0 + t1) // 2)

def binance_signed_get(path, params=None, off=0):
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        raise RuntimeError("Missing BINANCE_API_KEY/BINANCE_API_SECRET")
    params = dict(params or {})
    params["timestamp"] = int(time.time() * 1000) + off
    qs = urlencode(params, doseq=True)
    sig = hmac.new(BINANCE_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY, **UA_HEADERS}
    url = f"{BINANCE_BASE_URL}{path}?{qs}&signature={sig}"
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_account_binance(off=0):
    """
    Binance account info adaptado para este proyecto (dict en vez de DataFrame).
    Combina futuros + spot.
    """
    try:
        # -------- FUTUROS --------
        path = "/fapi/v2/account"
        params = {
            "timestamp": int(time.time() * 1000) + off,
            "recvWindow": 5000
        }
        qs = urlencode(params, doseq=True)
        sig = hmac.new(BINANCE_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
        url = f"{BINANCE_BASE_URL}{path}?{qs}&signature={sig}"
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY, **UA_HEADERS}

        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        data_futures = r.json() or {}

        futures_wallet_balance = float(data_futures.get("totalWalletBalance", 0))
        futures_margin_balance = float(data_futures.get("totalMarginBalance", 0))
        futures_unrealized = float(data_futures.get("totalUnrealizedProfit", 0))

        # -------- SPOT --------
        url_spot = "https://api.binance.com/api/v3/account"
        params_spot = {
            "timestamp": int(time.time() * 1000) + off,
            "recvWindow": 5000
        }
        qs_spot = urlencode(params_spot, doseq=True)
        sig_spot = hmac.new(BINANCE_API_SECRET.encode(), qs_spot.encode(), hashlib.sha256).hexdigest()
        url_spot = f"{url_spot}?{qs_spot}&signature={sig_spot}"

        r_spot = requests.get(url_spot, headers=headers, timeout=30)
        r_spot.raise_for_status()
        data_spot = r_spot.json() or {}

        # Traer precios para valuar balances spot
        prices = {p["symbol"]: float(p["price"]) for p in requests.get("https://api.binance.com/api/v3/ticker/price").json()}
        total_spot_usdt = 0.0
        for bal in data_spot.get("balances", []):
            asset = bal["asset"]
            free = float(bal["free"])
            locked = float(bal["locked"])
            amount = free + locked
            if amount == 0:
                continue
            if asset == "USDT":
                total_spot_usdt += amount
            else:
                symbol = asset + "USDT"
                if symbol in prices:
                    total_spot_usdt += amount * prices[symbol]

        # -------- FORMATO NORMALIZADO --------
        return {
            "exchange": "binance",
            "equity": futures_margin_balance + total_spot_usdt,   # equity total (spot + futures)
            "balance": futures_wallet_balance + total_spot_usdt,  # wallet futures + spot
            "unrealized_pnl": futures_unrealized,
            "initial_margin": float(data_futures.get("totalPositionInitialMargin", 0))
        }

    except Exception as e:
        print(f"❌ Binance account error: {e}")
        return None
    
def fetch_positions_binance(off=0):
    """
    Posiciones abiertas en Binance Futures
    """
    try:
        data = binance_signed_get("/fapi/v2/positionRisk", {}, off)
        rows = [d for d in data if float(d.get("positionAmt", "0")) != 0.0]
        positions = []
        for pos in rows:
            qty = float(pos["positionAmt"])
            side = "long" if qty > 0 else "short"
            positions.append({
                "exchange": "binance",
                "symbol": pos["symbol"],
                "side": side,
                "size": abs(qty),
                "entry_price": float(pos["entryPrice"]),
                "mark_price": float(pos["markPrice"]),
                "unrealized_pnl": float(pos["unRealizedProfit"]),
                "notional": float(pos["notional"]),
                "liquidation_price": float(pos["liquidationPrice"]),
                "leverage": float(pos["leverage"]),
            })
        return positions
    except Exception as e:
        print(f"❌ Binance positions error: {e}")
        return []
    
def fetch_funding_binance(limit=100, off=0):
    """
    Funding payments en Binance
    """
    try:
        data = binance_signed_get("/fapi/v1/income",
                                  {"incomeType": "FUNDING_FEE", "limit": limit},
                                  off)
        funding = []
        for f in data:
            funding.append({
                "exchange": "binance",
                "symbol": f.get("symbol", ""),
                "income": float(f.get("income", 0)),
                "asset": f.get("asset", "USDT"),
                "timestamp": f.get("time"),
                "funding_rate": None,
                "type": "FUNDING_FEE"
            })
        return funding
    except Exception as e:
        print(f"❌ Binance funding error: {e}")
        return []
    





def fmt_time(ms):
    return datetime.fromtimestamp(ms/1000).strftime("%Y-%m-%d %H:%M")
# Guardar lista de símbolos válidos en Binance Futures
# ====== DEBUG BINANCE TRADES: HELPERS ======
def fetch_closed_positions_binance(days=30, off=0):
    """
    Reconstruye posiciones cerradas de Binance usando userTrades + income.
    Cada vez que net_qty vuelve a 0 → nueva posición cerrada.
    """
    try:
        now = int(time.time() * 1000)
        start_time = now - days*24*60*60*1000

        # 1) Income global
        income = binance_signed_get("/fapi/v1/income", {
            "limit": 1000,
            "startTime": start_time,
            "endTime": now
        }, off)

        income_by_symbol = defaultdict(list)
        for inc in income:
            if inc["incomeType"] in ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE"):
                income_by_symbol[inc["symbol"]].append(inc)

        # 2) Determinar símbolos activos desde income
        symbols = [s for s in income_by_symbol.keys() if s]

        results = []

        for sym in symbols:
            # 3) Traer trades del símbolo
            try:
                trades = binance_signed_get("/fapi/v1/userTrades", {
                    "symbol": sym,
                    "limit": 1000,
                    "startTime": start_time,
                    "endTime": now
                }, off)
            except Exception as e:
                print(f"❌ userTrades error {sym}: {e}")
                continue

            if not trades:
                continue

            trades_sorted = sorted(trades, key=lambda x: x["time"])

            net_qty = 0.0
            block = []
            for t in trades_sorted:
                qty = float(t["qty"]) if t["side"] == "BUY" else -float(t["qty"])
                net_qty += qty
                block.append(t)

                # posición cerrada
                if abs(net_qty) < 1e-8:
                    open_date = fmt_time(block[0]["time"])
                    close_date = fmt_time(block[-1]["time"])

                    buys = [b for b in block if b["side"] == "BUY"]
                    sells = [s for s in block if s["side"] == "SELL"]

                    def avg_price(lst):
                        total_qty = sum(float(x["qty"]) for x in lst)
                        notional = sum(float(x["qty"]) * float(x["price"]) for x in lst)
                        return notional / total_qty if total_qty else 0.0

                    entry_price = avg_price(buys)
                    close_price = avg_price(sells)
                    size = sum(float(b["qty"]) for b in buys)

                    # 4) PnL y fees dentro de ese rango
                    start_ts, end_ts = block[0]["time"], block[-1]["time"]
                    incs = [i for i in income_by_symbol[sym] if start_ts <= i["time"] <= end_ts]
                    realized_pnl = sum(float(i["income"]) for i in incs if i["incomeType"] == "REALIZED_PNL")
                    fees = sum(float(i["income"]) for i in incs if i["incomeType"] == "COMMISSION")
                    funding = sum(float(i["income"]) for i in incs if i["incomeType"] == "FUNDING_FEE")

                    results.append({
                        "exchange": "binance",
                        "symbol": sym,
                        "side": "closed",
                        "size": size,
                        "entry_price": entry_price,
                        "close_price": close_price,
                        "notional": entry_price * size,
                        "fees": fees,
                        "funding_fee": funding,
                        "pnl": realized_pnl,
                        "realized_pnl": realized_pnl,
                        "open_date": open_date,
                        "close_date": close_date,
                    })

                    # reset
                    block = []

        print(f"✅ Binance closed positions reconstruidas: {len(results)} en {days} días")
        return results

    except Exception as e:
        print(f"❌ Binance closed positions error: {e}")
        return []


#------------- BingxCONFIG---------

#----modulos para importar funding usando websocket y listen key


# def _sign_params(params):
#     """Firma HMAC-SHA256 de los parámetros BingX"""
#     query = urlencode(params)
#     signature = hmac.new(
#         BINGX_SECRET_KEY.encode("utf-8"),
#         query.encode("utf-8"),
#         hashlib.sha256
#     ).hexdigest()
#     return signature

# def get_listen_key():
#     """Genera un nuevo listenKey para el WebSocket privado"""
#     url = f"{BINGX_BASE}/openApi/swap/v2/user/stream"
#     params = {"timestamp": int(time.time() * 1000)}
#     params["signature"] = _sign_params(params)
#     headers = {"X-BX-APIKEY": BINGX_API_KEY}
#     r = requests.post(url, headers=headers, params=params)
#     r.raise_for_status()
#     data = r.json()
#     listen_key = data.get("listenKey")
#     print(f"🎧 Nuevo listenKey obtenido: {listen_key}")
#     return listen_key

# def refresh_listen_key(listen_key):
#     """Mantiene vivo el listenKey (refrescar cada 45 minutos aprox.)"""
#     url = f"{BINGX_BASE}/openApi/swap/v2/user/stream"
#     params = {"listenKey": listen_key, "timestamp": int(time.time() * 1000)}
#     params["signature"] = _sign_params(params)
#     headers = {"X-BX-APIKEY": BINGX_API_KEY}
#     r = requests.put(url, headers=headers, params=params)
#     if r.status_code == 200:
#         print(f"🔄 ListenKey renovado correctamente: {listen_key}")
#     else:
#         print(f"⚠️ No se pudo renovar listenKey ({r.status_code}): {r.text}")

#----------- fin de los modulos para importar funding usando websocket y listen key
def _sign_params(params: dict) -> dict:
    params["timestamp"] = int(time.time() * 1000)
    qs = urlencode(params)
    signature = hmac.new(BINGX_SECRET_KEY.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()
    params["signature"] = signature
    return params
# === DEBUG de autenticación ===
    # print("[DEBUG][BingX] Query string:", qs)
    # print("[DEBUG][BingX] Signature:", signature)
    # print("[DEBUG][BingX] API Key:", BINGX_API_KEY[:6] + "..." if BINGX_API_KEY else "MISSING")
    return params

def _get(path, params=None):
    headers = {"X-BX-APIKEY": BINGX_API_KEY}
    p = _sign_params(params or {})
    url = BINGX_BASE + path
    print(f"[DEBUG][BingX] GET {url} params={p}")  # debug URL + params
    r = requests.get(url, params=p, headers=headers, timeout=15)
    # print("[DEBUG][BingX] Status:", r.status_code)
    # print("[DEBUG][BingX] Raw response:", r.text[:500])  # evita prints enormes
    r.raise_for_status()
    return r.json()
# Balances
# ======================
def fetch_account_bingx():
    try:
        data = _get("/openApi/swap/v3/user/balance")
        #print("[DEBUG][BingX] Full balance payload:", data)
        balances = data.get("data", [])
        total_equity = 0.0
        spot_balance = 0.0
        futures_balance = 0.0
        for b in balances:
            asset = b.get("asset")
            balance = float(b.get("balance", 0))
            if asset.upper() in ("USDT", "USDC"):
                spot_balance += balance
                futures_balance += balance
                total_equity += balance
        #print(f"[DEBUG][BingX] Totals: equity={total_equity}, spot={spot_balance}, futures={futures_balance}")
        return {
            "exchange": "bingx",
            "equity": total_equity,
            "balance": spot_balance,
            "unrealized_pnl": 0.0,  # no viene en balance
            "initial_margin": futures_balance,
        }
    except Exception as e:
        print(f"❌ BingX balance error: {e}")
        return None

# ======================
# Posiciones
# ======================
def fetch_positions_bingx():
    try:
        data = _get("/openApi/swap/v2/user/positions")
        import json
        #print("🔍 DEBUG RAW BINGX POSITIONS JSON ↓↓↓↓↓")
        print(json.dumps(data, indent=2))
        print("🔍 FIN DEBUG =============================")

        rows = data.get("data", [])
        positions = []
        for pos in rows:
            symbol = pos.get("symbol", "").replace("-USDT", "").replace("USDT", "").upper()
            qty = float(pos.get("positionAmt", 0))
            side = pos.get("positionSide", "").lower()
            if side not in ("long", "short"):
               side = "long" if qty > 0 else "short"  # fallback
            entry = float(pos.get("avgPrice", 0))
            mark = float(pos.get("markPrice", 0))
            unreal = float(pos.get("unrealizedProfit", 0))
            realized = float(pos.get("realisedProfit", 0))
            funding_fee = round(float(pos.get("cumFundingFee", 0)), 4)  # 👈 funding acumulado
            funding_raw = pos.get("cumFundingFee") or pos.get("fundingFee") or pos.get("funding") or 0
            funding_fee = round(float(funding_raw), 6)
            #print(f"[DEBUG][BingX] Position {symbol}: qty={qty}, entry={entry}, mark={mark}, unreal={unreal}, side={side}")
            positions.append({
                "exchange": "bingx",
                "symbol": symbol,
                "size": abs(qty),
                "side": side,
                "entry_price": entry,
                "mark_price": mark,
                "unrealized_pnl": unreal,
                "realized_pnl": realized,  # funding se llena aparte
                "leverage": float(pos.get("leverage", 0)),
                "notional": float(pos.get("positionInitialMargin", 0)),
                "funding_fee": funding_fee,
                "liquidation_price": float(pos.get("liquidationPrice", 0) or 0.0),
                


            })
        return positions
    except Exception as e:
        print(f"❌ BingX positions error: {e}")
        return []
    
# ======================
# Closed Positions
# ======================  

# ---------- Helpers de símbolo/num ----------
def _bx_to_dash(sym: str) -> str:
    """BTCUSDT -> BTC-USDT (lo exige el endpoint positionHistory)."""
    if not sym:
        return sym
    s = sym.upper()
    if "-" in s:
        return s
    for q in ("USDT", "USDC"):
        if s.endswith(q):
            return s[:-len(q)] + "-" + q
    return s

def _bx_no_dash(sym: str) -> str:
    """BTC-USDT -> BTCUSDT (consistente con tu DB/UI)."""
    return (sym or "").upper().replace("-", "")

def _num(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


# ---------- Funding opcional (suma por símbolo en rango) ----------
def _bingx_fetch_funding(income_type="FUNDING_FEE", start_ms=None, end_ms=None, limit=1000, debug=False):
    """
    Devuelve lista de registros de funding:
    [{symbol: 'BTCUSDT', income: float, timestamp: ms}, ...]
    """
    params = {
        "incomeType": income_type,
        "limit": limit,
    }
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    if end_ms is not None:
        params["endTime"] = int(end_ms)

    payload = _get("/openApi/swap/v2/user/income", params)
    data = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(data, list):
        return []
    out = []
    for it in data:
        try:
            sym = (it.get("symbol") or "").upper().replace("-", "")
            income = _num(it.get("income"), 0.0)
            ts = int(it.get("timestamp") or it.get("time") or 0)
            out.append({"symbol": sym, "income": income, "timestamp": ts})
        except Exception:
            continue
    if debug:
        print(f"✅ BingX funding: {len(out)} registros")
    return out


# ---------- Core: traer positions cerradas y guardar ----------

import time as _time

def fetch_closed_positions_bingx(symbols, days=30, include_funding=True, page_size=200, debug=False):
    """
    Descarga posiciones cerradas para una lista de símbolos (requerido por la API).
    - symbols: iterable de strings, p.ej. ["MYX-USDT", "KAITO-USDT"] o ["MYXUSDT", ...]
    - days: rango [now - days, now] en ms
    - include_funding: si True, suma FUNDING_FEE del rango para cada posición
    Devuelve lista de dicts listos para guardar.
    """
    if not symbols:
        print("⚠️ [BingX] 'symbols' vacío: la API exige símbolo. Pasa al menos uno.")
        return []

    now_ms = int(_time.time() * 1000)
    start_ms = now_ms - days * 24 * 60 * 60 * 1000

    # Funding opcional (indexado por símbolo sin guion)
    funding_idx = {}
    if include_funding:
        f_all = _bingx_fetch_funding(start_ms=start_ms, end_ms=now_ms, debug=debug)
        from collections import defaultdict as _dd
        tmp = _dd(list)
        for f in f_all:
            tmp[f["symbol"]].append(f)
        funding_idx = dict(tmp)

    results = []

    for sym in symbols:
        sym_dash = _bx_to_dash(sym)
        sym_nodash = _bx_no_dash(sym_dash)
        page = 1
        total_rows = 0

        while True:
            params = {
                "symbol": sym_dash,               # ⚠️ requerido
                "startTs": int(start_ms),      # ⚠️ nombres exactos según doc
                "endTs": int(now_ms),
                "pageId": page,                   # doc: pageId / pageIndex
                "pageSize": int(page_size),
                "recvWindow": 5000,
            }

            if debug:
                print(f"[BingX] positionHistory {sym_dash} page={page}")

            payload = _get("/openApi/swap/v1/trade/positionHistory", params)
            
            # Extraer correctamente la lista
            data = []
            if isinstance(payload, dict):
                if isinstance(payload.get("data"), dict):
                    data = payload["data"].get("positionHistory", [])
                elif isinstance(payload.get("data"), list):
                    data = payload["data"]
                else:
                    data = []
            else:
                data = payload or []
            
            if not data:
                if debug:
                    print(f"[BingX] {sym_dash} page={page}: 0 filas (estructura data vacía)")
                break


            for row in data:
                try:
                    open_ms = int(row.get("openTime") or 0)
                    close_ms = int(row.get("updateTime") or 0)
                    entry_price = float(row.get("avgPrice", 0))
                    close_price = float(row.get("avgClosePrice", 0))
                    qty = abs(float(row.get("closePositionAmt") or row.get("positionAmt") or 0))
                    realized_pnl = float(row.get("realisedProfit") or 0)
                    funding_total = float(row.get("totalFunding") or 0)
                    fee_total = float(row.get("positionCommission") or 0)
                    lev = float(row.get("leverage") or 0)
                    side = (row.get("positionSide") or "").lower()
            
                    results.append({
                        "exchange": "bingx",
                        "symbol": row["symbol"].replace("-", ""),
                        "side": side or "closed",
                        "size": qty,
                        "entry_price": entry_price,
                        "close_price": close_price,
                        "open_time": int(open_ms / 1000),
                        "close_time": int(close_ms / 1000),
                        "realized_pnl": realized_pnl,
                        "funding_total": funding_total,
                        "fee_total": fee_total,
                        "notional": entry_price * qty,
                        "leverage": lev,
                        "liquidation_price": None,
                    })
            
                    if debug:
                        print(f"  ✅ {row['symbol']} side={side} size={qty} entry={entry_price:.4f} "
                              f"close={close_price:.4f} pnl={realized_pnl:.4f} "
                              f"funding={funding_total:.4f} fee={fee_total:.4f}")
            
                except Exception as e:
                    if debug:
                        print(f"[WARN] fila malformada {row}: {e}")
                    continue

            if len(data) < page_size:
                break
            page += 1

        if debug:
            print(f"[BingX] {sym_dash} total filas: {total_rows}")

    if debug:
        print(f"✅ BingX closed positions totales: {len(results)}")
    return results


def save_bingx_closed_positions(db_path="portfolio.db", symbols=None, days=30, include_funding=True, debug=False):
    """
    Guarda posiciones cerradas de BingX en SQLite, evitando duplicados por (exchange, symbol, close_time).
    - symbols: lista de símbolos a consultar (p.ej. ["MYX-USDT", "KAITO-USDT"]).
                La API exige símbolo: si no pasas nada, no se consulta.
    """
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return

    if not symbols:
        print("⚠️ No se pasó 'symbols' a save_bingx_closed_positions. La API requiere símbolo. Nada que hacer.")
        return

    positions = fetch_closed_positions_bingx(
        symbols=symbols,
        days=days,
        include_funding=include_funding,
        debug=debug
    )
    if not positions:
        print("⚠️ No se obtuvieron posiciones cerradas de BingX.")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    saved = 0
    skipped = 0

    for pos in positions:
        try:
            # deduplicación por (exchange, symbol, close_time)
            cur.execute("""
                SELECT COUNT(*) FROM closed_positions
                WHERE exchange = ? AND symbol = ? AND close_time = ?
            """, (pos["exchange"], pos["symbol"], pos["close_time"]))
            if cur.fetchone()[0]:
                skipped += 1
                continue

            # usa tu helper centralizado que ya normaliza fees a negativas
            save_closed_position(pos)
            saved += 1

        except Exception as e:
            print(f"⚠️ Error guardando {pos.get('symbol')} (BingX): {e}")

    conn.close()
    print(f"✅ BingX guardadas: {saved} | omitidas (duplicadas): {skipped}")
     
# ======================
# Funding
# ======================
def fetch_funding_bingx(limit=100, start_time=None, end_time=None):
    """
    Funding history de BingX.
    Endpoint: GET /openApi/swap/v2/user/income
    """
    try:
        params = {
            "incomeType": "FUNDING_FEE",
            "limit": min(limit, 1000),
        }
        if start_time:
            params["startTime"] = int(start_time)
        if end_time:
            params["endTime"] = int(end_time)

        data = _get("/openApi/swap/v2/user/income", params=params)
        records = data.get("data", [])

        if not records:
            print("⚠️ No funding records found for BingX.")
            return []

        funding = []
        for rec in records:
            try:
                funding.append({
                    "exchange": "bingx",
                    "symbol": rec.get("symbol", "").replace("-", "").upper(),
                    "income": float(rec.get("amount", 0)),
                    "asset": rec.get("asset", "USDT"),
                    "timestamp": int(rec.get("time", 0)),
                    "funding_rate": None,
                    "type": rec.get("incomeType", "FUNDING_FEE"),
                })
            except Exception as e:
                print(f"[WARN] Error parsing BingX funding record: {e}")
                continue

        print(f"✅ BingX funding: {len(funding)} registros encontrados")
        return funding

    except Exception as e:
        print(f"❌ BingX funding error: {e}")
        return []


# -------------Adenconfig
# =====================

def _sign_request(req: Request) -> Request:
    ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    json_str = ""
    if req.json:
        json_str = json.dumps(req.json, separators=(',', ':'))

    url = urllib.parse.urlparse(req.url)
    message = str(ts) + req.method + url.path + json_str
    if url.query:
        message += "?" + url.query

    signature = urlsafe_b64encode(_private_key.sign(message.encode())).decode("utf-8")

    headers = {
        "orderly-timestamp": str(ts),
        "orderly-account-id": ORDERLY_ACCOUNT_ID,
        "orderly-key": ORDERLY_PUBLIC_KEY_B58,
        "orderly-signature": signature,
    }

    req.headers.update(headers)
    return req

def _send_request(method: str, path: str, params=None):
    """Versión simplificada sin debug"""
    url = f"{ORDERLY_BASE_URL}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    
    req = Request(method, url)
    signed = _sign_request(req).prepare()
    res = _session.send(signed, timeout=15)
    res.raise_for_status()
    return res.json()

# =====================
# Funciones Corregidas
# =====================

def fetch_account_aden(data=None):
    """Obtener cuenta de Aden"""
    try:
        if data is None:
            data = _send_request("GET", "/v1/positions")
        account_data = data.get("data", {})
        positions_data = account_data.get("rows", [])
        
        total_unrealized = sum(float(pos.get("unsettled_pnl", 0)) for pos in positions_data)
        total_collateral = float(account_data.get("total_collateral_value", 0))
        
        return {
            "exchange": "aden",
            "equity": total_collateral + total_unrealized,
            "balance": float(account_data.get("free_collateral", 0)),
            "unrealized_pnl": total_unrealized,
            "initial_margin": total_collateral - float(account_data.get("free_collateral", 0)),
            "total_collateral": total_collateral,
        }
    except Exception as e:
        print(f"❌ Aden account error: {e}")
        return None
    

def fetch_positions_aden(data=None):
    """Obtener posiciones de Aden"""
    try:
        if data is None:
            data = _send_request("GET", "/v1/positions")
        positions_data = data.get("data", {}).get("rows", [])

        
        
        # Obtener funding history para calcular el realized funding por símbolo
        funding_data = _send_request("GET", "/v1/funding_fee/history", {'size': 100})
        funding_rows = funding_data.get("data", {}).get("rows", [])
        
        # Calcular funding total por símbolo
        funding_by_symbol = {}
        for fee in funding_rows:
            
            symbol = fee.get("symbol", "") 
            funding_fee = float(fee.get("funding_fee", 0))
            funding_by_symbol[symbol] = funding_by_symbol.get(symbol, 0.0) - funding_fee
            #codigo viejo que ponia el funding en negativo aunque fuera positivo
            # if symbol not in funding_by_symbol:
            #     funding_by_symbol[symbol] = 0.0
            # funding_by_symbol[symbol] += funding_fee
            
        formatted_positions = []
        for pos in positions_data:
            raw_symbol = pos.get("symbol", "")
                    # quitar prefijo perp_ y sufijo _usdc para unificar
            clean_symbol = (
                  raw_symbol.lower()
                  .replace("perp_", "")
                  .replace("_usdc", "")
                  .upper()
            )
            realized_funding = funding_by_symbol.get(raw_symbol, 0.0)  
            entry_price = float(pos.get("average_open_price", 0))
            mark_price = float(pos.get("mark_price", 0))
            quantity = float(pos.get("position_qty", 0))
            unrealized_pnl = (mark_price - entry_price) * quantity
            notional = float(pos.get("cost_position", 0))
           

        
            
            formatted_pos = {
                "symbol": clean_symbol,
                "size": float(pos.get("position_qty", 0)),
                "quantity": float(pos.get("position_qty", 0)),  # ✅ Mismo que Backpack
                "side": "long" if float(pos.get("position_qty", 0)) > 0 else "short",
                "unrealized_pnl": unrealized_pnl,  # ✅ Solo ganancia/pérdida por precio
                # "unrealized_pnl": float(pos.get("unsettled_pnl", 0)),
                "realized_pnl": realized_funding,  # Para compatibilidad
                "funding_fee": realized_funding,  # ✅ Campo que busca el HTML
                "entry_price": float(pos.get("average_open_price", 0)),
                "mark_price": float(pos.get("mark_price", 0)),
                "leverage": float(pos.get("leverage", 1)),
                "liquidation_price": float(pos.get("est_liq_price", 0)),
                "notional": float(pos.get("cost_position", 0)),  # Aproximación
                "exchange": "aden"
            }
            formatted_positions.append(formatted_pos)
            print(f"✅ Aden {symbol}: realized_funding=${realized_funding:.2f}")
            print(f"  Quantity: {quantity}")
            print(f"  Entry: {entry_price}, Mark: {mark_price}")
            print(f"  CORRECT Unrealized PnL: {unrealized_pnl:.2f}")  # ✅ Solo precio
            print(f"  Realized Funding: {realized_funding:.2f}")      # ✅ Solo funding
            print(f"  Total (API Unsettled): {float(pos.get('unsettled_pnl', 0)):.2f}")
            print(f"  Verificación: {unrealized_pnl:.2f} + {realized_funding:.2f} = {unrealized_pnl + realized_funding:.2f}")
            print(f"  Notional: {float(pos.get('cost_position', 0)):.2f}")
            
            
            
        
        return formatted_positions
        
    except Exception as e:
        print(f"❌ Aden positions error: {e}")
        return []
    
def fetch_funding_aden(limit=100):
    """
    Funding history de Aden (Orderly).
    Endpoint: GET /v1/funding_fee/history
    """
    try:
        data = _send_request("GET", "/v1/funding_fee/history", {"size": min(int(limit), 500)})
        rows = data.get("data", {}).get("rows", [])
        if not rows:
            return []

        funding = []
        for f in rows:
            try:
                funding.append({
                    "exchange": "aden",
                    # limpio el símbolo para que sea consistente (ej: KAITOUSDC)
                    "symbol": f.get("symbol", "").replace("perp_", "").replace("_usdc", "").upper(),
                    # funding_fee es el pago/cobro → lo mapeamos a "income" como en Aster/Backpack
                    "income": -(float(f.get("funding_fee", 0.0))),  # invertimos signo para que cobros = positivo
                    "asset": "USDC",
                    "timestamp": f.get("created_time") or f.get("timestamp") or "",
                    "funding_rate": float(f.get("funding_rate", 0.0)) if f.get("funding_rate") is not None else None,
                    "type": "FUNDING_FEE"
                })
            except Exception as e:
                print(f"[WARNING] Error processing Aden funding row: {e}")
                continue

        #print(f"[DEBUG] Aden funding: {len(funding)} payments found")
        return funding

    except Exception as e:
        print(f"[ERROR] Failed to fetch Aden funding: {e}")
        return []

def fetch_closed_positions_aden(debug=False):
    """
    Obtener posiciones cerradas de Aden / Orderly.
    Endpoint: GET /v1/position_history
    """
    try:
        data = _send_request("GET", "/v1/position_history", {"limit": 100})
        rows = data.get("data", {}).get("rows", [])
        if not rows:
            if debug:
                print("⚠️ No se encontraron posiciones cerradas en Aden.")
            return []

        results = []
        for r in rows:
            if r.get("status") != "closed":
                continue  # solo posiciones cerradas

            try:
                raw_symbol = r.get("symbol", "")
                clean_symbol = (
                    raw_symbol.lower()
                    .replace("perp_", "")
                    .replace("_usdc", "")
                    .upper()
                )

                side = (r.get("side") or "").lower()
                entry_price = float(r.get("avg_open_price") or 0)
                close_price = float(r.get("avg_close_price") or 0)
                qty = abs(float(r.get("closed_position_qty") or r.get("max_position_qty") or 0))
                realized_pnl = float(r.get("realized_pnl") or 0)
                fee_total = float(r.get("trading_fee") or 0)
                funding_total = float(r.get("accumulated_funding_fee") or 0)
                lev = float(r.get("leverage") or 0)
                open_time = int(r.get("open_timestamp") or 0) // 1000
                close_time = int(r.get("close_timestamp") or 0) // 1000

                # 🧮 PnL sin incluir fees ni funding
                pnl = (close_price - entry_price) * qty * (1 if side == "long" else -1)

                results.append({
                    "exchange": "aden",
                    "symbol": clean_symbol,
                    "side": side,
                    "size": qty,
                    "entry_price": entry_price,
                    "close_price": close_price,
                    "fees": fee_total,
                    "funding_fee": funding_total,
                    "realized_pnl": realized_pnl,
                    "pnl": pnl,
                    "open_time": open_time,
                    "close_time": close_time,
                    "notional": entry_price * qty,
                    "leverage": lev,
                    "liquidation_price": None,
                })

                if debug:
                    print(f"✅ {clean_symbol} {side} size={qty} entry={entry_price:.4f} "
                          f"close={close_price:.4f} pnl={pnl:.4f} realized={realized_pnl:.4f}")

            except Exception as e:
                if debug:
                    print(f"[WARN] fila malformada Aden: {e}")
                continue

        print(f"✅ Aden closed positions descargadas: {len(results)}")
        return results

    except Exception as e:
        print(f"❌ Error al obtener posiciones cerradas de Aden: {e}")
        return []

def save_aden_closed_positions(db_path="portfolio.db", debug=False):
    """
    Guarda posiciones cerradas de Aden en SQLite.
    """
    import os, sqlite3
    from db_manager import save_closed_position

    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return

    closed_positions = fetch_closed_positions_aden(debug=debug)
    if not closed_positions:
        print("⚠️ No closed positions returned from Aden.")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    saved, skipped = 0, 0

    for pos in closed_positions:
        try:
            cur.execute("""
                SELECT COUNT(*) FROM closed_positions
                WHERE exchange = ? AND symbol = ? AND close_time = ?
            """, (pos["exchange"], pos["symbol"], pos["close_time"]))
            if cur.fetchone()[0]:
                skipped += 1
                continue

            save_closed_position(pos)
            saved += 1

        except Exception as e:
            print(f"⚠️ Error guardando posición {pos.get('symbol')} (Aden): {e}")

    conn.close()
    print(f"✅ Aden guardadas: {saved} | omitidas (duplicadas): {skipped}")



#---------------- AsterConfig-------------

def _aster_sign_params(params: dict) -> dict:
    """Firmar parámetros para Aster API"""
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    signature = hmac.new(
        ASTER_API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    params["signature"] = signature
    return params




def aster_signed_request(path: str, params: dict = None):
    """Función helper para requests autenticados a Aster API"""
    try:
        url = f"{ASTER_HOST}{path}"
        base_params = {
            "timestamp": int(time.time() * 1000),
            "recvWindow": 5000
        }
        
        if params:
            base_params.update(params)
            
        signed_params = _aster_sign_params(base_params)
        headers = {"X-MBX-APIKEY": ASTER_API_KEY}

        r = requests.get(url, params=signed_params, headers=headers, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Error en aster_signed_request: {e}")
        return {}

def fetch_account_aster():
    """
    Aster account info (TotalEquity, Wallet, etc).
    Endpoint: GET //api/v3/account
    """
    try:
        data = aster_signed_request("/fapi/v4/account")
        
        if not data:
            return None

        # Extraer totales directamente
        total_wallet_balance = float(data.get("totalWalletBalance", 0))
        total_unrealized_pnl = float(data.get("totalUnrealizedProfit", 0))
        total_equity = float(data.get("totalMarginBalance", 0))  # equivale a wallet + PnL
        ASTER_EQUITY = total_equity

        print(f"[DEBUG] Aster - Wallet Balance: {total_wallet_balance}, Equity: {total_equity}")
        
        
        
        return {
            "exchange": "aster",
            "equity": total_equity,
            "balance": total_wallet_balance,
            "unrealized_pnl": total_unrealized_pnl,
            "initial_margin": float(data.get("totalPositionInitialMargin", 0))
        }

    except Exception as e:
        print(f"[ERROR] Failed to fetch Aster account: {e}")
        return None
    
ASTER_EQUITY = 0.0  

def calc_liq_price(entry_price, position_amt, notional, leverage, wallet_balance, maint_rate=0.004):
    """
    Estima el precio de liquidación en cross margin.
    Usa equity (wallet + PnL no realizado) en lugar de solo wallet.
    """
    try:
        if position_amt == 0 or entry_price == 0 or notional == 0 or leverage == 0:
            return None

        maintenance_margin = notional * maint_rate

        if position_amt > 0:  # long
            liq = entry_price * (1 - 1/leverage + (wallet_balance - maintenance_margin) / notional)
        else:  # short
            liq = entry_price * (1 + 1/leverage - (wallet_balance - maintenance_margin) / notional)

        return round(liq, 6) if liq > 0 else None
    except Exception as e:
        print(f"[WARNING] Error calculating liquidation price: {e}")
        return None


def fetch_positions_aster():
    """
    Get current open positions from Aster.
    Endpoint: GET /api/v2/positionRisk
    """
    try:
        data = aster_signed_request("/fapi/v2/positionRisk")
        if not data:
            return []

        # ⚠️ Aquí deberías traer el wallet balance de la cuenta Aster
        # si ya lo calculas en otra función, puedes pasarlo como parámetro o almacenarlo global
        wallet_balance = 0.0  

        positions = []
        for position in data:
            try:
                position_amt = float(position.get("positionAmt", 0) or 0.0)
                if position_amt == 0:
                    continue

                unrealized_pnl = float(position.get("unRealizedProfit", 0) or 0.0)
                entry_price = float(position.get("entryPrice", 0) or 0.0)
                mark_price = float(position.get("markPrice", 0) or 0.0)
                notional = float(position.get("notional", 0) or 0.0)

                leverage = float(position.get("leverage", 0) or 0.0)
                if leverage == 0 and entry_price and position_amt:
                    leverage = abs(notional / (position_amt * entry_price)) if (position_amt * entry_price) else 10

                if position_amt > 0:
                    side = "long"
                elif position_amt < 0:
                    side = "short"
                else:
                    side = "flat"

                # 🔧 Liquidation Price
                liq_raw = float(position.get("liquidationPrice", 0) or 0.0)
                liquidation_price = liq_raw if liq_raw > 0 else calc_liq_price(
                    entry_price=entry_price,
                    position_amt=position_amt,
                    notional=notional,
                    leverage=leverage,
                    wallet_balance= ASTER_EQUITY,  
                    maint_rate=0.004  # valor por defecto, puedes ajustarlo
                )

                positions.append({
                    "exchange": "aster",
                    "symbol": position.get("symbol", ""),
                    "side": side,
                    "size": abs(position_amt),
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "unrealized_pnl": unrealized_pnl,
                    "notional": notional,
                    "liquidation_price": liquidation_price,
                    "leverage": leverage
                })

                print("[DEBUG][Aster] Raw position:", position)
                print("[DEBUG][Aster] Calculated liq price:", liquidation_price)

            except Exception as e:
                print(f"[WARNING] Error processing Aster position: {e}")
                continue

        return positions

    except Exception as e:
        print(f"[ERROR] Failed to fetch Aster positions: {e}")
        return []


def fetch_funding_aster(limit=100, startTime=None, endTime=None, symbol=None):
    """
    Aster funding history (user income).
    Endpoint estilo Binance: GET /api/v1/funding con incomeType=FUNDING_FEE
    
    """
    try:
        params = {
            "incomeType": "FUNDING_FEE",
            "limit": min(int(limit), 1000)
        }
        if startTime is not None:
            params["startTime"] = int(startTime)
        if endTime is not None:
            params["endTime"] = int(endTime)
        if symbol:
            params["symbol"] = symbol

        data = aster_signed_request("/fapi/v1/income", params=params)
        if not data:
            return []

        funding = []
        for item in data:
            try:
                # Formato estilo Binance:
                # { "symbol": "BTCUSDT", "incomeType": "FUNDING_FEE", "income": "0.123",
                #   "asset": "USDT", "time": 1700000000000, "info": "...", ... }
                funding.append({
                    "exchange": "aster",
                    "symbol": item.get("symbol", ""),
                    "income": float(item.get("income", 0)),
                    "asset": item.get("asset", "USDT"),
                    "timestamp": item.get("time") or item.get("timestamp") or item.get("tranTime") or "",
                    # Aster/Income no trae el funding_rate; lo dejamos en None
                    "funding_rate": None,
                    "type": "FUNDING_FEE"
                })
            except Exception as e:
                print(f"[WARNING] Error processing Aster funding item: {e}")
                continue

        #print(f"[DEBUG] Aster funding: {len(funding)} payments found")
        return funding

    except Exception as e:
        print(f"[ERROR] Failed to fetch Aster funding: {e}")
        return []


# -------------- Backpack signer (Ed25519) --------------
def _bp_sign_message(instruction: str, params: dict | None, ts_ms: int, window_ms: int = 5000) -> str:
    """
    Construye el string a firmar y devuelve la firma en Base64.
    """
    # 1) params ordenados -> querystring
    query = ""
    if params:
        from urllib.parse import urlencode
        sorted_items = sorted(params.items())
        query = urlencode(sorted_items, doseq=True)

    # 2) instrucción + timestamp & window
    if query:
        to_sign = f"instruction={instruction}&{query}&timestamp={ts_ms}&window={window_ms}"
    else:
        to_sign = f"instruction={instruction}&timestamp={ts_ms}&window={window_ms}"

    # 3) firmar con Ed25519
    try:
        seed32 = base64.b64decode(BACKPACK_API_SECRET)
    except Exception as e:
        raise RuntimeError(f"Invalid BACKPACK_API_SECRET format (expected Base64): {e}")
    if len(seed32) != 32:
        raise RuntimeError(f"BACKPACK_API_SECRET must decode to 32 bytes, got {len(seed32)}")

    signing_key = nacl.signing.SigningKey(seed32)
    sig_bytes = signing_key.sign(to_sign.encode("utf-8")).signature
    sig_b64 = base64.b64encode(sig_bytes).decode("ascii")
    return sig_b64



def backpack_signed_request(method: str, path: str, instruction: str, params: dict | None = None, body: dict | None = None):
    """
    Llama a un endpoint privado con firma Backpack.
    """
    if not BACKPACK_API_KEY or not BACKPACK_API_SECRET:
        raise RuntimeError("Missing BACKPACK_API_KEY / BACKPACK_API_SECRET")

    ts_ms = int(time.time() * 1000)
    window_ms = 5000

    # Los params que se firman son:
    sign_params = params if method.upper() == "GET" else (body or {})

    signature_b64 = _bp_sign_message(instruction, sign_params, ts_ms, window_ms)
    headers = {
        "X-API-KEY": BACKPACK_API_KEY,
        "X-SIGNATURE": signature_b64,
        "X-TIMESTAMP": str(ts_ms),
        "X-WINDOW": str(window_ms),
        "Content-Type": "application/json; charset=utf-8",
        **UA_HEADERS
    }

    url = f"{BACKPACK_BASE_URL}{path}"
    if method.upper() == "GET":
        r = requests.get(url, headers=headers, params=params or {}, timeout=30)
    elif method.upper() == "POST":
        r = requests.post(url, headers=headers, json=(body or {}), timeout=30)
    else:
        raise ValueError(f"Unsupported method: {method}")

    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        print(f"[backpack] HTTP error {e.response.status_code} for {path}: {e.response.text}")
        raise
    return r.json()

def _normalize_symbol(sym: str) -> str:
    """Normaliza símbolos de Backpack"""
    if not isinstance(sym, str):
        return sym
    parts = sym.split("_")
    if len(parts) >= 2:
        return parts[0] + parts[1]
    return sym

# -------------- Backpackconfig--------------
def fetch_account_backpack():
    """
    Backpack account equity via Capital Collateral.
    GET /api/v1/capital/collateral (Instruction: collateralQuery)
    """
    try:
        data = backpack_signed_request(
            "GET", "/api/v1/capital/collateral", instruction="collateralQuery", params=None
        )
        acct = data if isinstance(data, dict) else (data.get("data") or {})
        if not acct:
            return None

        def f(k):
            try:
                return float(acct.get(k, 0))
            except Exception:
                return 0.0

        return {
            "exchange": "backpack",
            "equity": f("netEquity"),
            "balance": f("netEquity"),  # Usar equity como balance
            "unrealized_pnl": f("pnlUnrealized"),
            "initial_margin": f("imf")  # Initial Margin Fraction
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch Backpack account: {e}")
        return None



def fetch_positions_backpack():
    """
    GET /api/v1/position (Instruction: positionQuery)
    Devuelve posiciones abiertas de Backpack.
    """
    try:
        data = backpack_signed_request(
            "GET", "/api/v1/position", instruction="positionQuery", params=None
        )
        items = data if isinstance(data, list) else (data.get("data") or [])
        if not items:
            return []

        positions = []
        for item in items:
            try:
                net_quantity = float(item.get("netQuantity", 0))
                
                # Filtrar posiciones cerradas (amount = 0)
                if net_quantity == 0:
                    continue
                    
                entry_price = float(item.get("entryPrice", 0))
                mark_price = float(item.get("markPrice", 0))
                notional = float(item.get("netExposureNotional", 0))
                unrealized_pnl = float(item.get("pnlUnrealized", 0))
                cumulative_funding = float(item.get("cumulativeFundingPayment", 0))
                
                # ✅ CALCULAR UNREALIZED PNL MANUALMENTE PARA VERIFICAR
                # Unrealized PnL = (Mark Price - Entry Price) * Quantity
                calculated_unrealized = (mark_price - entry_price) * net_quantity
                
                positions.append({
                    "exchange": "backpack",
                    "symbol": _normalize_symbol(item.get("symbol", "")),
                    "side": "LONG" if net_quantity >= 0 else "SHORT",
                    "size": abs(net_quantity),
                    "quantity": abs(net_quantity),
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "unrealized_pnl": calculated_unrealized,  # ← USAR EL CALCULADO
                    "funding_fee": cumulative_funding,
                    "realized_pnl": cumulative_funding,
                    "notional": notional,
                    "liquidation_price": float(item.get("estLiquidationPrice", 0))
                })
                
                print(f"[DEBUG] Backpack position: {item.get('symbol')}")
                print(f"  Quantity: {net_quantity}")
                print(f"  Entry: {entry_price}, Mark: {mark_price}")
                print(f"  API Unrealized: {unrealized_pnl}")
                print(f"  CALCULATED Unrealized: {calculated_unrealized}")
                print(f"  Funding: {cumulative_funding}")
                print(f"  Notional: {notional}")
                      
            except Exception as e:
                print(f"[WARNING] Error processing Backpack position: {e}")
                continue

        print(f"[DEBUG] Backpack positions: {len(positions)} found")
        return positions
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch Backpack positions: {e}")
        return []
def fetch_funding_backpack(limit=100):
    """
    GET /wapi/v1/history/funding (Instruction: fundingHistoryQueryAll)
    Trae los funding payments del usuario de Backpack.
    """
    try:
        params = {"limit": min(int(limit), 100), "sortDirection": "Desc"}
        
        data = backpack_signed_request(
            "GET", "/wapi/v1/history/funding", instruction="fundingHistoryQueryAll", params=params
        )
        items = data if isinstance(data, list) else (data.get("data") or [])
        if not items:
            return []

        funding_payments = []
        for item in items:
            try:
                # ✅ PROBAR DIFERENTES CAMPOS POSIBLES
                amount = float(item.get("quantity", 0))

                # convertir timestamp ISO → epoch ms
                ts = item.get("intervalEndTimestamp", "") or item.get("timestamp", "")
                try:
                    # Manejar diferentes formatos de timestamp
                    if "T" in ts:
                        ts_ms = int(datetime.fromisoformat(ts.replace("Z", "")).timestamp() * 1000)
                    else:
                        ts_ms = int(ts) if ts else None
                except Exception:
                    ts_ms = None

                funding_payments.append({
                    "exchange": "backpack",
                    "symbol": _normalize_symbol(item.get("symbol", "")),
                    "income": amount,
                    "asset": "USDC",
                    "timestamp": ts_ms,
                    "funding_rate": float(item.get("fundingRate", 0)),
                    "type": "FUNDING_FEE"
                })
                
                # ✅ DEBUG: Mostrar cada registro de funding
                #print(f"[DEBUG] Backpack funding: {item.get('symbol')} = {amount}")
                
            except Exception as e:
                print(f"[WARNING] Error processing Backpack funding: {e}")
                print(f"[WARNING] Problematic item: {item}")
                continue

        #print(f"[DEBUG] Backpack funding: {len(funding_payments)} payments found, total: {sum(f['income'] for f in funding_payments):.4f}")
        return funding_payments
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch Backpack funding: {e}")
        return []

    


def _format_time(ts: str) -> str:
    """
    Convierte '2025-09-19T06:57:33.557' en '2025-09-19 06:57'
    """
    if not ts:
        return "-"
    try:
        dt = datetime.fromisoformat(ts.replace("Z", ""))  # por si viene con Z
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ts


#------------- Extendedconfig------------
def extended_get(path: str, params=None):
    """Función helper para Extended API"""
    try:
        url = f"{EXT_BASE_URL}/api/v1{path}"
        timestamp = str(int(time.time() * 1000))
        
        message = timestamp + "GET" + f"/api/v1{path}" + ""
        signature = hmac.new(
            EXT_API_SECRET.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        
        headers = {
            "X-API-KEY": EXT_API_KEY,
            "X-TIMESTAMP": timestamp,
            "X-SIGNATURE": signature,
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error en extended_get: {e}")
        return {"data": {}}
    


def fetch_account_extended():
    """Obtener balance de Extended"""
    try:
        data = extended_get("/user/balance")
        raw = data.get("data") or {}
        
        #print(f"[DEBUG] Extended data: {raw}")
        
        return {
            "exchange": "extended",
            "equity": float(raw.get("equity", 0)),
            "balance": float(raw.get("balance", 0)),
            "unrealized_pnl": float(raw.get("unrealisedPnl", 0)),
            "initial_margin": float(raw.get("initialMargin", 0))
        }
    except Exception as e:
        print(f"Error Extended: {e}")
        return None

#---------Open Positions



#==========Close positions

def fetch_closed_positions_extended(limit=1000, debug=False):
    """
    Obtiene posiciones cerradas de Extended usando el endpoint de positions history.
    GET /api/v1/user/positions/history
    """
    try:
        now = int(time.time() * 1000)
        # Buscar en los últimos 90 días por defecto
        start_time = now - (90 * 24 * 60 * 60 * 1000)
        
        all_positions = []
        cursor = None
        
        while True:
            params = {
                "limit": min(limit, 100),  # Máximo por página
            }
            
            if cursor:
                params["cursor"] = cursor
            
            # Opcional: filtrar por timeframe si la API lo soporta
            # params["startTime"] = start_time
            # params["endTime"] = now
            
            data = extended_get("/user/positions/history", params)
            
            if not data or data.get("status") != "OK":
                if debug:
                    print(f"[Extended] Error en respuesta: {data}")
                break
                
            positions = data.get("data", [])
            if not positions:
                if debug:
                    print("[Extended] No hay más posiciones")
                break
                
            # Filtrar solo posiciones cerradas (tienen closedTime)
            closed_positions = [p for p in positions if p.get("closedTime")]
            all_positions.extend(closed_positions)
            
            if debug:
                print(f"[Extended] Página: {len(positions)} posiciones, {len(closed_positions)} cerradas")
            
            # Paginación
            pagination = data.get("pagination", {})
            next_cursor = pagination.get("cursor")
            
            if not next_cursor or len(positions) < params["limit"]:
                break
                
            cursor = next_cursor
            time.sleep(0.1)  # Rate limiting
        
        # Procesar y normalizar las posiciones
        results = []
        for pos in all_positions:
            try:
                market = pos.get("market", "")
                # Normalizar símbolo (quitar -USD, -PERP, etc.)
                symbol = market.replace("-USD", "").replace("-PERP", "").upper()
                
                side = (pos.get("side") or "").lower()
                size = float(pos.get("size", 0))
                entry_price = float(pos.get("openPrice", 0))
                close_price = float(pos.get("exitPrice", 0))
                realized_pnl = float(pos.get("realisedPnl", 0))
                leverage = float(pos.get("leverage", 1))
                
                open_time = pos.get("createdTime")
                close_time = pos.get("closedTime")
                
                # Para Extended, no tenemos fees y funding separados en este endpoint
                # El realizedPnl ya incluye todo (fees + funding + PnL de precio)
                fees = 0.0
                funding_fee = 0.0
                pnl_price_only = realized_pnl  # Asumimos que es solo PnL de precio por ahora
                
                results.append({
                    "exchange": "extended",
                    "symbol": symbol,
                    "side": side,
                    "size": size,
                    "entry_price": entry_price,
                    "close_price": close_price,
                    "notional": entry_price * size,
                    "fees": fees,
                    "funding_fee": funding_fee,
                    "realized_pnl": realized_pnl,
                    "pnl": pnl_price_only,
                    "open_time": int(open_time / 1000) if open_time else None,
                    "close_time": int(close_time / 1000) if close_time else None,
                    "leverage": leverage,
                    "liquidation_price": None,
                    "exit_type": pos.get("exitType", "")
                })
                
                if debug:
                    print(f"✅ [Extended] {symbol} {side} size={size:.4f} "
                          f"entry={entry_price:.4f} close={close_price:.4f} "
                          f"realized={realized_pnl:.4f}")
                          
            except Exception as e:
                if debug:
                    print(f"[WARN] Error procesando posición Extended: {e}")
                continue
        
        if debug:
            print(f"✅ Extended closed positions: {len(results)} encontradas")
            
        return results
        
    except Exception as e:
        print(f"❌ Error fetching Extended closed positions: {e}")
        return []

def save_extended_closed_positions(db_path="portfolio.db", debug=False):
    """
    Guarda posiciones cerradas de Extended en SQLite.
    """
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return

    closed_positions = fetch_closed_positions_extended(debug=debug)
    if not closed_positions:
        print("⚠️ No closed positions returned from Extended.")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    saved = 0
    skipped = 0

    for pos in closed_positions:
        try:
            # deduplicación por (exchange, symbol, close_time)
            cur.execute("""
                SELECT COUNT(*) FROM closed_positions
                WHERE exchange = ? AND symbol = ? AND close_time = ?
            """, (pos["exchange"], pos["symbol"], pos["close_time"]))
            
            if cur.fetchone()[0]:
                skipped += 1
                continue

            # Usar el helper centralizado
            save_closed_position({
                "exchange": pos["exchange"],
                "symbol": pos["symbol"],
                "side": pos["side"],
                "size": pos["size"],
                "entry_price": pos["entry_price"],
                "close_price": pos["close_price"],
                "open_time": pos["open_time"],
                "close_time": pos["close_time"],
                "realized_pnl": pos["realized_pnl"],
                "funding_total": pos.get("funding_fee", 0.0),
                "fee_total": pos.get("fees", 0.0),
                "notional": pos["notional"],
                "leverage": pos.get("leverage"),
                "liquidation_price": pos.get("liquidation_price")
            })
            saved += 1

        except Exception as e:
            print(f"⚠️ Error guardando {pos.get('symbol')} (Extended): {e}")

    conn.close()
    print(f"✅ Extended guardadas: {saved} | omitidas (duplicadas): {skipped}")

#=======================
def fetch_account_bybit():
    """Obtener balance de Bybit - VERSIÓN CORREGIDA Y SIMPLIFICADA"""
    try:
        # Configuración básica
        url = "https://api.bybit.com/v5/account/wallet-balance"
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        
        # Parámetros simples - solo accountType
        params = {"accountType": "UNIFIED"}
        
        # Crear string de parámetros para la firma (ordenado alfabéticamente)
        param_string = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        
        # String para firma (formato exacto que Bybit espera)
        sign_string = timestamp + BYBIT_API_KEY + recv_window + param_string
        
        #print(f"[DEBUG] Bybit sign string: '{sign_string}'")
        
        # Generar firma
        signature = hmac.new(
            BYBIT_API_SECRET.encode("utf-8"),
            sign_string.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        
       # print(f"[DEBUG] Bybit signature: {signature}")

        # Headers
        headers = {
            "X-BAPI-API-KEY": BYBIT_API_KEY,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "X-BAPI-SIGN": signature,
        }

        # Hacer el request
        full_url = f"{url}?{param_string}"
       # print(f"[DEBUG] Bybit final URL: {full_url}")
        
        r = requests.get(full_url, headers=headers, timeout=30)
       # print(f"[DEBUG] Bybit response status: {r.status_code}")
        
        data = r.json()
        #print(f"[DEBUG] Bybit full response: {data}")
        
        # Verificar respuesta
        if data.get("retCode") != 0:
            error_msg = data.get('retMsg', 'Unknown error')
            print(f"[ERROR] Bybit API error: {error_msg}")
            return None
        
        # Procesar datos
        balance_data = data.get("result", {}).get("list", [])
        if not balance_data:
            print("[WARNING] Bybit returned empty balance data")
            return None
            
        account_balance = balance_data[0]
        total_equity = float(account_balance.get("totalEquity", 0))
        total_balance = float(account_balance.get("totalWalletBalance", 0))
        
        print(f"[SUCCESS] Bybit - Equity: {total_equity}, Balance: {total_balance}")
        
        return {
            "exchange": "bybit", 
            "equity": total_equity,
            "balance": total_balance,
            "unrealized_pnl": total_equity - total_balance,
            "initial_margin": 0
        }
        
    except Exception as e:
        print(f"[ERROR] Bybit failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
# def index():
#     """Página principal del dashboard"""
#     return render_template(TEMPLATE_FILE)  # Asegúrate de que el archivo se llame index.html  


# def fetch_closed_positions():
#     backpack_api_key = BACKPACK_API_KEY
#     backpack_api_secret = BACKPACK_API_SECRET

#     closed_positions_backpack = get_backpack_closed_positions(backpack_api_key, backpack_api_secret)

#     return {
#         "backpack": closed_positions_backpack
#     }


#----------------Debugg    
# def get_backpack_closed_positions(api_key, api_secret):
#     """
#     Debug: imprime la respuesta completa de Backpack para borrow-lend position history.
#     """
#     import requests
#     import time
#     import hmac
#     import hashlib
#     import json

#     base_url = "https://api.backpack.exchange/api/v1/perp/borrow-lend/positionHistory"

#     # Backpack requiere timestamp y firma
#     timestamp = str(int(time.time() * 1000))
#     method = "GET"
#     endpoint = "/api/v1/perp/borrow-lend/positionHistory"

#     # Query params (ajustables: limit, symbol, etc.)
#     query_string = "limit=5"  # solo pedimos 5 para debug
#     message = f"{timestamp}{method}{endpoint}?{query_string}"

#     signature = hmac.new(api_secret.encode(), message.encode(), hashlib.sha256).hexdigest()

#     headers = {
#         "X-BP-APIKEY": api_key,
#         "X-BP-SIGNATURE": signature,
#         "X-BP-TIMESTAMP": timestamp
#     }

#     url = f"{base_url}?{query_string}"
#     response = requests.get(url, headers=headers)

#     print(f"Status code: {response.status_code}")
#     if response.status_code == 200:
#         data = response.json()
#         # Imprimir JSON bonito
#         print(json.dumps(data, indent=2))
#         return data
#     else:
#         print(f"Error {response.status_code}: {response.text}")
#         return None

# if __name__ == "__main__":
#     api_key = BACKPACK_API_KEY
#     api_secret = BACKPACK_API_SECRET

#     debug_data = get_backpack_closed_positions(api_key, api_secret)
    
# from flask import jsonify

# @app.route("/debug_backpack_closed")
# def debug_backpack_closed():
#     api_key = "TU_API_KEY"
#     api_secret = "TU_API_SECRET"
#     data = get_backpack_closed_positions(api_key, api_secret)
#     return jsonify(data)
   
#-------- Guardar posiciones antiguas
    #///// Closed positions

 

# @app.route("/api/sync_trades")
# def sync_trades():
#     """Sincroniza trades → funding → posiciones cerradas"""
#     print("📡 Sincronizando trades y funding...")

#     # Ejemplo con Binance (puedes repetir para Aster, Backpack, etc.)
#     binance_trades = fetch_trades_binance()       # 🔹 necesitas tener esta función
#     binance_funding = fetch_funding_binance()     # ya la tienes

#     process_closed_positions("binance", binance_trades, binance_funding)

#     return jsonify({"status": "ok", "exchange": "binance"})

## final    
    
    
# Routers

import re

def _base_symbol(sym: str) -> str:
    """
    2ZUSDT -> 2Z
    2ZUSDC -> 2Z
    KAITO-PERP -> KAITO
    KAITOUSDC-PERP -> KAITO
    """
    s = (sym or "").upper()
    # quitar quotes al final (con - o / opcional)
    s = re.sub(r'[-_/]?(USDT|USDC)$', '', s)
    # quitar sufijo PERP al final
    s = re.sub(r'[-_/]?PERP$', '', s)
    return s



@app.route("/api/closed_positions")
def api_closed_positions():
    """
    Agrupa posiciones cerradas en 'clusters' por símbolo base (2Z, KAITO, AVNT, ...)
    y empareja piernas usando:
      - ventana temporal (<= 15 minutos entre open/close), o
      - misma size (tolerancia relativa muy baja).
    Devuelve una lista de grupos con sus piernas + totales + fechas (min open, max close).
    """
    try:
        WINDOW_SEC = 15 * 60           # 15 minutos
        SIZE_EPS_REL = 0.001           # 0.1% de tolerancia en size

        conn = sqlite3.connect("portfolio.db")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT exchange, symbol, side, size, entry_price, close_price,
                   realized_pnl, funding_total AS funding_fee,
                   fee_total AS fees, notional, open_time, close_time
            FROM closed_positions
            ORDER BY open_time ASC
        """)
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()

        # 1) Partimos por símbolo base
        by_base = {}
        for r in rows:
            base = _base_symbol(r["symbol"])
            r["_base"] = base
            by_base.setdefault(base, []).append(r)

        groups = []

        # 2) Para cada símbolo base, creamos 'clusters'
        for base, items in by_base.items():
            # ordenar por open_time para clusterizar estable
            items.sort(key=lambda x: (x.get("open_time") or 0, x.get("close_time") or 0))
            clusters = []  # cada cluster: {"legs": [...], "open_ref": int, "close_ref": int, "size_ref": float}

            for p in items:
                ot = int(p.get("open_time") or 0)
                ct = int(p.get("close_time") or 0)
                size = float(p.get("size") or 0.0)

                # intentar encajar en un cluster existente
                best_idx = -1
                best_score = None
                for i, c in enumerate(clusters):
                    # criterio tiempo: cercano a la ref del cluster
                    time_diff = min(abs(ot - c["open_ref"]), abs(ct - c["close_ref"]))
                    fits_time = time_diff <= WINDOW_SEC

                    # criterio size: cercano a la size de referencia del cluster
                    ref = max(1e-12, c["size_ref"])
                    fits_size = abs(size - c["size_ref"]) / ref <= SIZE_EPS_REL

                    if fits_time or fits_size:
                        # score: prioriza tiempo; si empata, el más parecido en tamaño
                        score = (time_diff, abs(size - c["size_ref"]))
                        if best_score is None or score < best_score:
                            best_score = score
                            best_idx = i

                if best_idx == -1:
                    # nuevo cluster
                    clusters.append({
                        "legs": [p],
                        "open_ref": ot or ct,    # si faltase ot, usa ct
                        "close_ref": ct or ot,
                        "size_ref": size
                    })
                else:
                    c = clusters[best_idx]
                    c["legs"].append(p)
                    # actualizar refs (para absorber más piernas)
                    c["open_ref"] = min(c["open_ref"], ot or c["open_ref"])
                    c["close_ref"] = max(c["close_ref"], ct or c["close_ref"])
                    # la size de referencia mantenla (suele ser estable) o usa promedio simple
                    c["size_ref"] = (c["size_ref"] + size) / 2.0 if size > 0 else c["size_ref"]

            # 3) Construimos la respuesta agrupada por cluster
            for c in clusters:
                legs = c["legs"]

                # totales y promedios ponderados
                size_total = sum(float(x["size"] or 0.0) for x in legs)
                notional_total = sum(float(x["notional"] or 0.0) for x in legs)
                fees_total = sum(float(x["fees"] or 0.0) for x in legs)
                funding_total = sum(float(x["funding_fee"] or 0.0) for x in legs)
                realized_total = sum(float(x["realized_pnl"] or 0.0) for x in legs)
                pnl_total = realized_total - funding_total - fees_total

                # precios ponderados por size
                if size_total > 0:
                    entry_weighted = sum(float(x["entry_price"] or 0.0) * float(x["size"] or 0.0) for x in legs) / size_total
                    close_weighted = sum(float(x["close_price"] or 0.0) * float(x["size"] or 0.0) for x in legs) / size_total
                else:
                    entry_weighted = 0.0
                    close_weighted = 0.0

                open_time = min(int(x.get("open_time") or 0) for x in legs if x.get("open_time") is not None) if legs else None
                close_time = max(int(x.get("close_time") or 0) for x in legs if x.get("close_time") is not None) if legs else None

                groups.append({
                    "symbol": base,                    # ← símbolo base (2Z, KAITO, AVNT…)
                    "positions": legs,                 # ← piernas individuales (se muestran en la tabla interna)
                    "size_total": size_total,
                    "notional_total": notional_total,
                    "pnl_total": pnl_total,   # 👈 añadido
                    "fees_total": fees_total,
                    "funding_total": funding_total,
                    "realized_total": realized_total,
                    "entry_avg": entry_weighted,
                    "close_avg": close_weighted,
                    "open_date": datetime.fromtimestamp(open_time).strftime("%Y-%m-%d %H:%M") if open_time else "-",
                    "close_date": datetime.fromtimestamp(close_time).strftime("%Y-%m-%d %H:%M") if close_time else "-"
                })

        # ordenar tarjetas por close_date (desc)
        groups.sort(key=lambda g: g["close_date"], reverse=True)

        print(f"📊 Enviando {len(groups)} grupos (símbolo base + clustering tiempo/size)")
        return jsonify({"closed_positions": groups})

    except Exception as e:
        print(f"❌ Error leyendo/agrupando closed_positions: {e}")
        return jsonify({"closed_positions": []})


@app.route("/")
def index():
    return render_template(TEMPLATE_FILE)



@app.route('/api/positions')
def get_positions():
    """API para posiciones de todos los exchanges"""
    print("📡 Solicitando datos de posiciones...")
    
    all_positions = []
    
    # Backpack positions
    try:
        backpack_positions = fetch_positions_backpack()
        all_positions.extend(backpack_positions)
        
        # ✅ DEBUG TEMPORAL: Ver datos REALES de Backpack
        print("🔍 DEBUG BACKPACK POSITIONS:")
        for i, pos in enumerate(backpack_positions):
            print(f"  Position {i}: {pos['symbol']}")
            print(f"    unrealized_pnl: {pos['unrealized_pnl']} (type: {type(pos['unrealized_pnl'])})")
            print(f"    realized_funding: {pos['realized_funding']}")
            print(f"    quantity: {pos['quantity']}")
            print(f"    notional: {pos['notional']}")
            
    except Exception as e:
        print(f"❌ Backpack positions error: {e}")
    
    # Aster positions
    try:
        aster_positions = fetch_positions_aster()
        all_positions.extend(aster_positions)
    except Exception as e:
        print(f"❌ Aster positions error: {e}")
     #binance positions  
    try:
        binance_positions = fetch_positions_binance()
        all_positions.extend(binance_positions)
        
    except Exception as e:
         print(f"❌ Binance positions error: {e}")
        
   # BingX (nuevo 🚀) → fuera del bloque de Aster
    try:
        bingx_positions = fetch_positions_bingx()
        all_positions.extend(bingx_positions)
        #print(f"✅ BingX posiciones: {len(bingx_positions)}")
    except Exception as e:
        print(f"❌ BingX positions error: {e}")
    
    # Aden positions
    try:
        aden_data = _send_request("GET", "/v1/positions")
        aden_positions = fetch_positions_aden(aden_data)
        all_positions.extend(aden_positions)
        print(f"✅ Aden posiciones: {len(aden_positions)}")
    except Exception as e:
        print(f"❌ Aden positions error: {e}")
    
    print(f"📊 Total posiciones: {len(all_positions)}")
    
    # ✅ DEBUG TEMPORAL: Ver datos finales que se envían
    print("🔍 FINAL DATA BEING SENT:")
    for pos in all_positions:
        if pos['exchange'] == 'backpack':
            print(f"  {pos['symbol']} - Unrealized: {pos['unrealized_pnl']}")
    
    return jsonify({
        "positions": all_positions
    })


@app.route('/api/funding')
def get_funding():
    """API para funding history de todos los exchanges"""
    print("📡 Solicitando datos de funding...")

    all_funding = []

    # Backpack
    backpack_funding = fetch_funding_backpack(limit=50)
    all_funding.extend(backpack_funding)
    print(f"✅ Backpack funding: {len(backpack_funding)} registros")

    # Aster
    aster_funding = fetch_funding_aster(limit=50)
    all_funding.extend(aster_funding)
    print(f"✅ Aster funding: {len(aster_funding)} registros")

    # Binance
    binance_funding = fetch_funding_binance(limit=50)
    all_funding.extend(binance_funding)
    print(f"✅ Binance funding: {len(binance_funding)} registros")

    # Aden (Orderly)
    aden_funding = fetch_funding_aden(limit=50)
    all_funding.extend(aden_funding)
    print(f"✅ Aden funding: {len(aden_funding)} registros")
    
    

    # BingX (funding en vivo desde WebSocket)
    # try:
    #     if latest_funding_bingx:
    #         live_records = []
    #         for asset, rec in latest_funding_bingx.items():
    #             live_records.append({
    #                 "exchange": "bingx",
    #                 "symbol": asset,
    #                 "income": rec["amount"],
    #                 "asset": rec["asset"],
    #                 "timestamp": rec["timestamp"],
    #                 "funding_rate": None,
    #                 "type": "FUNDING_FEE"
    #             })
    #         all_funding.extend(live_records)
    #         print(f"✅ BingX funding (live): {len(live_records)} registros")
    #     else:
    #         print("⚠️ BingX funding (live): sin registros todavía")
    # except Exception as e:
    #     print(f"❌ Error obteniendo BingX funding (live): {e}")

    print(f"📊 Total registros de funding: {len(all_funding)}")

    return jsonify({
        "funding": all_funding
    })




@app.route('/api/balances')
def get_balances():
    """API para balances - VERSIÓN CORREGIDA"""
    balances = []
    
    # ✅ Aden → reutilizamos la misma llamada
    aden_data = _send_request("GET", "/v1/positions")
    aden_account = fetch_account_aden(aden_data)
    aden_positions = fetch_positions_aden(aden_data)
    if aden_account:
        aden_account["positions"] = aden_positions   # ⬅️ añadimos posiciones al objeto balance
        balances.append(aden_account)
        print(f"✅ Aden: {aden_account['equity']}")
        
    #Binance
    binance_data = fetch_account_binance()
    if binance_data:
        print(f"✅ Binance: {binance_data['equity']}")
        balances.append(binance_data)
    else:
        print("❌ Binance falló")

    # Aster - NUEVO
    aster_data = fetch_account_aster()
    if aster_data:
        print(f"✅ Aster: {aster_data['equity']}")
        balances.append(aster_data)
    else:
        print("❌ Aster falló")
    
    extended_data = fetch_account_extended()
    if extended_data:  # ✅ Verificar si no es None
        balances.append(extended_data)
        # BingX (nuevo 🚀)
    bingx_data = fetch_account_bingx()
    if bingx_data:
        balances.append(bingx_data)
    
    bybit_data = fetch_account_bybit()
    if bybit_data:  # ✅ Verificar si no es None (NO .empty)
        balances.append(bybit_data)
        
    # Backpack 
    backpack_data = fetch_account_backpack()
    if backpack_data:
        print(f"✅ Backpack: {backpack_data['equity']}")
        balances.append(backpack_data)
    else:
        print("❌ Backpack falló")
    
    # Calcular totales (con manejo de lista vacía)
    if balances:
        total_equity = sum(b['equity'] for b in balances)
        total_balance = sum(b['balance'] for b in balances)
        total_unrealized_pnl = sum(b['unrealized_pnl'] for b in balances)
    else:
        total_equity = 0
        total_balance = 0
        total_unrealized_pnl = 0
    print(f"📊 Totales: Equity=${total_equity:.2f}, Balance=${total_balance:.2f}")
    
    return jsonify({
        "exchanges": balances,
        "totals": {
            "equity": total_equity,
            "balance": total_balance,
            "unrealized_pnl": total_unrealized_pnl
        }
    })



# =====================================================
# 🚀 BLOQUE FINAL LIMPIO — EJECUCIÓN PRINCIPAL
# =====================================================


from db_manager import init_db

def main():
    print("🚀 Iniciando actualización de portfolio...")

    # --- Backpack ---
    print("⏳ Sincronizando fills cerrados de Backpack...")
    save_backpack_closed_positions("portfolio.db")
    print("✅ Posiciones cerradas de Backpack actualizadas correctamente.")
    
    # --- Aden ---
    print("⏳ Sincronizando fills cerrados de Aden...")
    save_aden_closed_positions("portfolio.db", debug=False)
    print("✅ Posiciones cerradas de Aden actualizadas correctamente.")
    

    # --- BingX ---
    print("⏳ Sincronizando fills cerrados de BingX...")

    funding = fetch_funding_bingx(limit=200)
    symbols_auto = sorted({f["symbol"] for f in funding if f.get("symbol")})
    symbols_dash = [s if "-" in s else s[:-4] + "-USDT" for s in symbols_auto]

    print(f"🔍 Símbolos detectados BingX: {symbols_dash}")

    save_bingx_closed_positions(
        db_path="portfolio.db",
        symbols=symbols_dash,
        days=30,
        include_funding=True,
        debug=True
    )
    print("✅ Posiciones cerradas de BingX actualizadas correctamente.")
    

    # --- Aster ---
    print("⏳ Sincronizando fills cerrados de Aster...")
    save_aster_closed_positions("portfolio.db",days=30, debug=True)
    print("✅ Posiciones cerradas de Aster actualizadas correctamente.")

    # --- Binance ---
    print("⏳ Sincronizando fills cerrados de Binance...")
    save_binance_closed_positions("portfolio.db", days=30, debug=False)
    print("✅ Posiciones cerradas de Binance actualizadas correctamente.")

    print("🧩 Sincronización inicial completada.")
    
 # --- Extended ---
    print("⏳ Sincronizando posiciones cerradas de Extended...")
    save_extended_closed_positions("portfolio.db", debug=True)
    print("✅ Posiciones cerradas de Extended actualizadas correctamente.")    


# =====================================================
# 🏁 EJECUCIÓN PRINCIPAL (Init DB + Main + Flask)
# =====================================================

if __name__ == "__main__":
    print("🧱 Inicializando base de datos...")
    init_db()  # Crea la tabla closed_positions si no existe

    print("✅ Base de datos lista. Ejecutando sincronización inicial...")
    main()  # sincroniza Backpack antes de arrancar Flask
    print("⏳ Sincronizando fills cerrados de Aster...")
    save_aster_closed_positions("portfolio.db",days=30, debug = True)
    print("✅ Posiciones cerradas de Aster actualizadas correctamente.")
    save_bingx_closed_positions("portfolio.db", days=30, debug=False)

    print("🌐 Lanzando servidor Flask...")
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
    


