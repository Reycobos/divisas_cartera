# adapters/bitget.py
import os
import time
import hmac
import hashlib
import base64
import requests
from typing import List, Dict, Any, Optional
import re
from urllib.parse import urlencode
import json
from dotenv import load_dotenv
load_dotenv()

# --- asegurar que podamos importar db_manager desde la carpeta raíz ---
import sys
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)  # carpeta raíz del proyecto
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
#====== Imports para prints
# from pp import (
#     p_closed_debug_header, p_closed_debug_count, p_closed_debug_norm_size,
#     p_closed_debug_prices, p_closed_debug_pnl, p_closed_debug_times, p_closed_debug_normalized,
#     p_open_summary, p_open_block,
#     p_funding_fetching, p_funding_count,
#     p_balance_equity
# )
#===========================

# Configuración
BITGET_API_KEY = os.getenv("BITGET_API_KEY")
BITGET_API_SECRET = os.getenv("BITGET_API_SECRET")
BITGET_API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE")
BITGET_BASE_URL = "https://api.bitget.com"

__all__ = [
    "fetch_bitget_all_balances",
    "fetch_bitget_open_positions", 
    "fetch_bitget_funding_fees",
    "save_bitget_closed_positions"
]

def normalize_symbol(sym: str) -> str:
    """Normaliza símbolos de Bitget según especificación"""
    if not sym: return ""
    s = sym.upper()
    s = re.sub(r'^PERP_', '', s)
    s = re.sub(r'(_|-)?(USDT|USDC|PERP)$', '', s)
    s = re.sub(r'[_-]+$', '', s)
    s = re.split(r'[_-]', s)[0]
    return s

def _bitget_sign(timestamp: str, method: str, request_path: str, 
                query_string: str = "", body: str = "") -> str:
    """Genera firma HMAC para Bitget"""
    message = timestamp + method.upper() + request_path
    if query_string:
        message += "?" + query_string
    if body:
        message += body
    
    mac = hmac.new(
        BITGET_API_SECRET.encode('utf-8'),
        message.encode('utf-8'),
        hashlib.sha256
    )
    return base64.b64encode(mac.digest()).decode()

def _bitget_request(method: str, path: str, params: Dict = None, 
                   body: Dict = None, version: str = "v2") -> Dict:
    """Realiza request autenticado a Bitget API asegurando orden idéntico al firmado."""
    if not all([BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE]):
        raise ValueError("Missing Bitget API credentials")

    timestamp = str(int(time.time() * 1000))
    # ⚠️ construimos y reutilizamos EXACTAMENTE el mismo query string para firmar y para la URL
    query_string = urlencode(params or {}, doseq=True)
    body_str = json.dumps(body) if body else ""

    # la firma incluye ?query_string si existe
    signature = _bitget_sign(timestamp, method, path, query_string, body_str)

    headers = {
        "ACCESS-KEY": BITGET_API_KEY,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": timestamp,
        "ACCESS-PASSPHRASE": BITGET_API_PASSPHRASE,
        "Content-Type": "application/json",
        "locale": "en-US"
    }

    # construimos URL final con el MISMO query string
    url = f"{BITGET_BASE_URL}{path}" + (f"?{query_string}" if query_string else "")

    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, timeout=30)
        else:
            response = requests.post(url, data=body_str if body_str else None, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Bitget API error ({path}): {e}")
        return {}

# --- helper robusto para floats de la API ('' -> 0.0, None -> 0.0) ---
def to_f(val, default=0.0) -> float:
    try:
        # Bitget a veces manda '', ' ', None
        s = str(val).strip()
        if s == "" or s.lower() == "null":
            return float(default)
        return float(s)
    except Exception:
        return float(default)

def fetch_bitget_all_balances() -> Dict[str, Any]:
    """
    Obtiene balances de Bitget usando all-account-balance endpoint
    Devuelve estructura EXACTA para frontend Balances
    """
    try:
        # 1. Obtener todos los balances de cuenta
        all_balances_data = _bitget_request("GET", "/api/v2/account/all-account-balance")
        
        spot_balance = 0.0
        futures_balance = 0.0
        margin_balance = 0.0
        
        if all_balances_data.get("code") == "00000":
            for account in all_balances_data.get("data", []):
                account_type = account.get("accountType", "").lower()
                usdt_balance = float(account.get("usdtBalance", "0") or 0)
                
                if account_type == "spot":
                    spot_balance += usdt_balance
                elif account_type == "futures":
                    futures_balance += usdt_balance
                elif account_type == "margin":
                    margin_balance += usdt_balance
                # También puedes incluir "funding", "earn", "bots" si los quieres en spot
        
        # 2. Obtener datos adicionales de futuros para unrealized PnL y margin
        unrealized_pnl = 0.0
        initial_margin = 0.0
        
        futures_account = _bitget_request("GET", "/api/v2/mix/account/accounts", {
            "productType": "USDT-FUTURES"
        })
        
        if futures_account.get("code") == "00000":
            for account in futures_account.get("data", []):
                if account.get("crossedUnrealizedPL"):
                    unrealized_pnl += float(account["crossedUnrealizedPL"])
                # Calcular initial margin como equity - available
                if account.get("usdtEquity") and account.get("available"):
                    initial_margin += float(account["usdtEquity"]) - float(account.get("available", 0))
        
        # 3. Calcular equity total
        total_equity = spot_balance + futures_balance + margin_balance
        
        return {
            "exchange": "bitget",
            "equity": total_equity,
            "balance": spot_balance + futures_balance + margin_balance,  # saldo utilizable total
            "unrealized_pnl": unrealized_pnl,
            "initial_margin": initial_margin,
            "spot": spot_balance,
            "margin": margin_balance,  # Ahora sí tenemos margin balance
            "futures": futures_balance
        }
        
    except Exception as e:
        print(f"❌ Bitget balances error: {e}")
        return {
            "exchange": "bitget",
            "equity": 0.0,
            "balance": 0.0,
            "unrealized_pnl": 0.0,
            "initial_margin": 0.0,
            "spot": 0.0,
            "margin": 0.0,
            "futures": 0.0
        }
        
    except Exception as e:
        print(f"❌ Bitget balances error: {e}")
        return {
            "exchange": "bitget",
            "equity": 0.0,
            "balance": 0.0,
            "unrealized_pnl": 0.0,
            "initial_margin": 0.0,
            "spot": 0.0,
            "margin": 0.0,
            "futures": 0.0
        }

def fetch_bitget_open_positions() -> List[Dict[str, Any]]:
    """
    Obtiene posiciones abiertas de Bitget Futures (USDT-FUTURES)
    y blinda el parseo de strings vacíos ('' -> 0.0).
    """
    positions: List[Dict[str, Any]] = []

    try:
        data = _bitget_request("GET", "/api/v2/mix/position/all-position", {
            "productType": "USDT-FUTURES"
        })
        if data.get("code") != "00000":
            return positions

        for pos in data.get("data", []) or []:
            try:
                symbol_raw = pos.get("symbol", "")
                symbol     = normalize_symbol(symbol_raw)
                hold_side  = (pos.get("holdSide") or "").lower()

                # TAMAÑO BASE: Bitget devuelve strings; pueden venir vacíos
                size = to_f(pos.get("total", 0)) or to_f(pos.get("available", 0))
                if size <= 0:
                    # Si total=0 pero hay bloqueado, puedes usar locked si te interesa
                    # size = max(size, to_f(pos.get("locked", 0)))
                    # if size <= 0: 
                    continue

                # PRECIOS
                entry_price = to_f(pos.get("openPriceAvg", 0))
                mark_price  = to_f(pos.get("markPrice", 0))

                # PnL no realizado (solo precio); si falta mark/entry, queda 0.0
                if hold_side == "long":
                    unrealized_pnl = (mark_price - entry_price) * size
                else:  # 'short' u otro
                    unrealized_pnl = (entry_price - mark_price) * size

                # FEES / FUNDING (pueden venir '' según la doc)
                fee_total    = -abs(to_f(pos.get("deductedFee", 0)))
                funding_fee  = to_f(pos.get("totalFee", 0))  # vacío si aún no hubo funding
                realized_pnl = fee_total + funding_fee

                # LIQUIDATION (si '' o <=0, lo dejas en 0.0 o None según tu frontend)
                liq_raw      = pos.get("liquidationPrice", 0)
                liq_price    = to_f(liq_raw, 0.0)

                # NOTIONAL: usar marginSize*leverage si existen; si no, size*entry o size*mark
                lev          = max(to_f(pos.get("leverage", 1.0)), 1.0)
                margin_size  = to_f(pos.get("marginSize", 0))
                notional     = margin_size * lev
                if notional <= 0:
                    # fallback razonable cuando margin info no viene
                    base_px = mark_price or entry_price
                    notional = size * base_px

                positions.append({
                    "exchange": "bitget",
                    "symbol": symbol,
                    "side": hold_side,                        # 'long' | 'short'
                    "size": size,
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "liquidation_price": liq_price if liq_price > 0 else 0.0,
                    "notional": notional,
                    "unrealized_pnl": unrealized_pnl,
                    "fee": fee_total,
                    "funding_fee": funding_fee,
                    "realized_pnl": realized_pnl
                })

            except Exception as e:
                # Log específico por símbolo si hay problema (p.ej. AIAUSDT con '' en algún campo)
                print(f"❌ Error procesando posición Bitget {pos.get('symbol')}: {e}")
                continue

    except Exception as e:
        print(f"❌ Bitget open positions error: {e}")

    return positions

def fetch_bitget_funding_fees(limit: int = 2000,
                              since: int | None = None,
                              days: int | None = None,
                              chunk: int = 100,
                              max_pages: int = 200,
                              debug: bool = False) -> list[dict]:
    """
    User funding via Account Bill:
      GET /api/v2/mix/account/bill  (Rate limit 10 req/s UID)
    - productType=USDT-FUTURES
    - Filtra businessType = contract_settle_fee
    - Paginación hacia atrás con idLessThan (endId)
    - 'since' en ms o 'days' hacia atrás (se parte en ventanas <=30 días si se usa timebox)
    """
    out: list[dict] = []
    now_ms = int(time.time() * 1000)
    if since is None:
        days = 30 if days is None else int(days)
        since = now_ms - days*24*3600*1000
    since = int(since)

    id_less = None
    pages = 0
    while pages < max_pages and len(out) < limit:
        params = {
            "productType": "USDT-FUTURES",   # ¡mayúsculas!
            "limit": str(min(100, max(1, int(chunk))))
        }
        if id_less:
            params["idLessThan"] = id_less
        # (opcional) timebox por 30 días (si prefieres por tiempo en vez de idLessThan)
        # params["startTime"] = str(since)
        # params["endTime"]   = str(now_ms)

        data = _bitget_request("GET", "/api/v2/mix/account/bill", params=params)
        if data.get("code") != "00000":
            if debug: print("❌ bill error:", data)
            break

        payload = data.get("data", {}) or {}
        bills = payload.get("bills", []) or []
        end_id = payload.get("endId")

        if debug:
            cnt = len(bills)
            rng = [int(b.get("cTime", 0) or 0) for b in bills]
            print(f"PAGE {pages+1}: count={cnt} endId={end_id} "
                  f"range=({min(rng) if rng else None}..{max(rng) if rng else None})")

        for b in bills:
            try:
                if b.get("businessType") != "contract_settle_fee":
                    continue  # funding fees solamente
                ts = int(b.get("cTime", "0") or 0)
                if ts and ts < since:
                    continue
                amt = float(b.get("amount", "0") or 0)  # firmado por Bitget, puede ser +/- 
                sym_raw = b.get("symbol") or ""
                sym = normalize_symbol(sym_raw) if 'normalize_symbol' in globals() else sym_raw
                out.append({
                    "exchange": "bitget",
                    "symbol": sym or "GENERAL",
                    "symbol_raw": sym_raw,
                    "income": amt,                     # negativo=pago, positivo=cobro
                    "asset": b.get("coin", "USDT"),
                    "timestamp": ts,
                    "funding_rate": None,
                    "type": "FUNDING_FEE",
                    "external_id": str(b.get("billId") or f"bitget|{ts}|{amt:.8f}")
                })
                if len(out) >= limit:
                    break
            except Exception as e:
                if debug: print("  · skip bill:", e)

        if not bills or not end_id:
            break
        id_less = end_id
        pages += 1
        # pequeño respiro (RL 10/s)
        time.sleep(0.05)

    out.sort(key=lambda x: x["timestamp"] or 0)
    if debug:
        if out:
            print(f"TOTAL items={len(out)} range=({out[0]['timestamp']}..{out[-1]['timestamp']})")
        else:
            print("TOTAL items=0")
    return out

def save_bitget_closed_positions(
    db_path: str = "portfolio.db",
    days: int = 30,
    debug: bool = False,
    symbol: str | None = None,
    limit: int = 100,
    max_pages: int = 10
):
    """
    Descarga posiciones CERRADAS de Bitget (USDT-M futures) en la ventana indicada
    y las guarda en SQLite SIN sobrescribir. Devuelve métricas:
    {"inserted": N, "updated": 0, "skipped": K, "skipped_no_time": X, "duplicated": D}

    - days: ventana hacia atrás
    - symbol: par opcional, ej. "BTCUSDT" (si lo pasas, productType no tiene efecto)
    - limit: 1..100 por página
    - max_pages: páginas a recorrer con idLessThan (endId)
    """

    import os, sys, time, sqlite3

    # ------- resolver ROOT para poder importar db_manager desde carpeta raíz ------
    HERE = os.path.dirname(os.path.abspath(__file__))
    ROOT = os.path.dirname(HERE)
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    # ------------------------------------------------------------------------------

    try:
        from db_manager import save_closed_position
    except Exception as e:
        print(f"❌ No module db_manager: {e}")
        return {"inserted": 0, "updated": 0, "skipped": 0, "skipped_no_time": 0, "duplicated": 0}

    # ---------- helpers de timestamps (integrados dentro de la función) -----------
    def _to_int(x):
        try:
            if x is None:
                return 0
            if isinstance(x, (int, float)):
                return int(x)
            s = str(x).strip()
            if not s:
                return 0
            return int(float(s))
        except Exception:
            return 0

    def _pick_ms(d: dict, candidates: list[str]) -> int:
        """Devuelve el primer timestamp válido (ms) para las claves candidatas."""
        for k in candidates:
            v = d.get(k)
            ms = _to_int(v)
            if ms > 0:
                if debug:
                    print(f"     ⮑ time key '{k}' -> {ms}")
                return ms
        return 0

    def _ms_to_sec(ms: int) -> int:
        # si llega en ms (~13 dígitos), pásalo a segundos
        return ms // 1000 if ms >= 10**12 else ms

    def _norm_symbol(s: str) -> str:
        s = (s or "").upper().replace("-", "")
        # Bitget suele dar "BTCUSDT" ya OK; por consistencia eliminamos guiones
        return s

    # ------------------------ validar credenciales si aplica -----------------------
    try:
        # si tienes variables globales BITGET_API_KEY/SECRET/PASSPHRASE en este módulo,
        # puedes validarlas aquí. Si no existen, omitimos la verificación.
        if debug:
            print("🧩 Bitget: guardando cerradas "
                  f"(últimos {days} días) → {db_path}  symbol={symbol or '-'}")
    except Exception:
        pass

    # ---------------------------- preparar ventana tiempo -------------------------
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - int(days) * 24 * 60 * 60 * 1000

    # ---------------------------- resolver ruta DB --------------------------------
    if not os.path.isabs(db_path):
        db_path = os.path.join(ROOT, db_path)
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return {"inserted": 0, "updated": 0, "skipped": 0, "skipped_no_time": 0, "duplicated": 0}

    # ---------------------------- llamada API con paginación ----------------------
    inserted = 0
    skipped = 0
    skipped_no_time = 0
    duplicated = 0
    updated = 0  # mantenemos 0 para compatibilidad
    seen_ids = set()  # por si la API repite páginas

    end_id = None
    page = 0

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    try:
        while page < max_pages:
            page += 1

            params = {
                "productType": "USDT-FUTURES",
                "startTime": str(start_ms),
                "endTime": str(end_ms),
                "limit": str(max(1, min(int(limit), 100)))
            }
            if symbol:
                params["symbol"] = symbol.upper()
            if end_id:
                params["idLessThan"] = end_id

            data = _bitget_request("GET", "/api/v2/mix/position/history-position", params)

            if not isinstance(data, dict):
                print(f"❌ Bitget respuesta no dict (page={page}): {type(data)}")
                break

            if data.get("code") != "00000":
                print(f"❌ Bitget API error (page={page}): {data.get('msg')} ({data.get('code')})")
                break

            d = data.get("data") or {}
            items = d.get("list") or []
            end_id = d.get("endId")  # para paginar hacia atrás

            if debug:
                print(f"📦 Página {page}: {len(items)} items  endId={end_id}")

            if not items:
                break

            for pos in items:
                try:
                    # evita procesar dos veces el mismo registro si la API repite
                    pid = str(pos.get("positionId") or "")
                    if pid:
                        if pid in seen_ids:
                            duplicated += 1
                            if debug:
                                print(f"  · dupe (positionId repetido): {pid}")
                            continue
                        seen_ids.add(pid)

                    symbol_norm = _norm_symbol(pos.get("symbol", ""))
                    side = (pos.get("holdSide") or "").lower()

                    # cantidad cerrada preferente
                    size = float(pos.get("closeTotalPos") or 0)
                    if size <= 0:
                        # fallback a openTotalPos si la API no rellena closeTotalPos
                        size = float(pos.get("openTotalPos") or 0)

                    # tiempos robustos
                    open_ms = _pick_ms(pos, ["cTime", "ctime", "createTime", "openTime", "startTime"])
                    close_ms = _pick_ms(pos, ["uTime", "utime", "updateTime", "closeTime", "endTime"])
                    open_time = _ms_to_sec(open_ms)
                    close_time = _ms_to_sec(close_ms)

                    if not open_time or not close_time:
                        skipped_no_time += 1
                        if debug:
                            print(f"  · skip {symbol_norm}: tiempos inválidos (open={open_ms}, close={close_ms})")
                        continue

                    entry_price = float(pos.get("openAvgPrice") or 0)
                    close_price = float(pos.get("closeAvgPrice") or 0)
                    net_profit = float(pos.get("netProfit") or 0)
                    total_funding = float(pos.get("totalFunding") or 0)
                    open_fee = float(pos.get("openFee") or 0)
                    close_fee = float(pos.get("closeFee") or 0)
                    fee_total = open_fee + close_fee  # normalmente negativas en Bitget

                    # ---------- deduplicación básica antes de insertar -----------------
                    cur.execute("""
                        SELECT id FROM closed_positions
                         WHERE exchange=? AND symbol=? AND close_time=?
                         LIMIT 1
                    """, ("bitget", symbol_norm, close_time))
                    row = cur.fetchone()
                    if row:
                        duplicated += 1
                        if debug:
                            print(f"  · dupe-clasico {symbol_norm}: close_time={close_time} (id={row[0]})")
                        continue

                    cur.execute("""
                        SELECT id FROM closed_positions
                         WHERE exchange=? AND symbol=? AND side=? AND close_time>0
                           AND ABS(size - ?) <= 1e-8
                           AND ABS(close_price - ?) <= 1e-10
                         LIMIT 1
                    """, ("bitget", symbol_norm, side, size, close_price))
                    row2 = cur.fetchone()
                    if row2:
                        duplicated += 1
                        if debug:
                            print(f"  · dupe-tolerancia {symbol_norm}: size/close_price coinciden (id={row2[0]})")
                        continue
                    # -------------------------------------------------------------------

                    payload = {
                        "exchange": "bitget",
                        "symbol": symbol_norm,
                        "side": side,
                        "size": size,
                        "entry_price": entry_price,
                        "close_price": close_price,
                        "open_time": open_time,
                        "close_time": close_time,
                        "realized_pnl": net_profit,
                        "funding_total": total_funding,
                        "fee_total": fee_total,
                        "notional": entry_price * size if entry_price and size else None,
                        "leverage": 1.0,
                        "liquidation_price": None
                    }

                    save_closed_position(payload)
                    inserted += 1
                    if debug:
                        print(f"  ✅ saved {symbol_norm} {side} size={size} realized={net_profit:.8f}")

                except Exception as e:
                    skipped += 1
                    if debug:
                        print(f"  · error item {pos.get('symbol')} → {e}")

            # parar si no hay más paginación
            if not end_id:
                break

    except Exception as e:
        print(f"❌ Bitget closed positions error: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if debug:
        print(f"✅ Bitget guardadas: {inserted} | omitidas: {skipped} | sin_fecha: {skipped_no_time} | duplicadas: {duplicated}")

    return {
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "skipped_no_time": skipped_no_time,
        "duplicated": duplicated
    }

## funcion vieja que falla por el tiempo que no guarda bien
# def save_bitget_closed_positions(db_path: str = "portfolio.db", days: int = 30, debug: bool = False) -> None:
#     """
#     Guarda posiciones cerradas de Bitget en SQLite con verificación de duplicados
#     y SIN sobrescrituras:
#       - Rechaza items sin open_time o close_time válidos (>0)
#       - Evita duplicados por (exchange, symbol, close_time)
#       - Evita pseudo-duplicados por (exchange, symbol, side) con size y close_price prácticamente iguales
#     """
#     try:
#         from db_manager import save_closed_position
#         import sqlite3

#         if not all([BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE]):
#             if debug:
#                 print("⚠️ Bitget: faltan credenciales. No se guardan cerradas.")
#             return

#         end_ms = int(time.time() * 1000)
#         start_ms = end_ms - (days * 24 * 60 * 60 * 1000)

#         # Usa productType en MAYÚSCULAS aquí (históricamente más estable).
#         data = _bitget_request("GET", "/api/v2/mix/position/history-position", {
#             "productType": "USDT-FUTURES",
#             "startTime": str(start_ms),
#             "endTime": str(end_ms),
#             "limit": "100"
#         })

#         if data.get("code") != "00000":
#             if debug:
#                 print(f"❌ Bitget API error: {data.get('msg', 'Unknown error')}")
#             return

#         items = (data.get("data", {}) or {}).get("list", []) or []
#         if debug:
#             print(f"📦 Bitget cerradas recibidas: {len(items)}")

#         if not os.path.exists(db_path):
#             print(f"❌ Database not found: {db_path}")
#             return

#         conn = sqlite3.connect(db_path)
#         cur = conn.cursor()

#         saved, skipped, skipped_no_time, skipped_dupe = 0, 0, 0, 0

#         for pos in items:
#             try:
#                 symbol = normalize_symbol(pos.get("symbol", "")).upper()
#                 side   = (pos.get("holdSide") or "").lower()
#                 size   = float(pos.get("openTotalPos") or 0)  # tamaño "nominal" del fill
#                 if size == 0:
#                     skipped += 1
#                     if debug: print(f"  · skip {symbol}: size=0")
#                     continue

#                 open_time = int((pos.get("ctime") or 0)) // 1000
#                 close_time = int((pos.get("utime") or 0)) // 1000

#                 # ❌ Blindaje 1: sin tiempos válidos, NO se guarda
#                 if not open_time or not close_time:
#                     skipped_no_time += 1
#                     if debug: 
#                         print(f"  · skip {symbol}: tiempos inválidos (open={open_time}, close={close_time})")
#                     continue

#                 entry_price = float(pos.get("openAvgPrice") or 0)
#                 close_price = float(pos.get("closeAvgPrice") or 0)

#                 net_profit    = float(pos.get("netProfit") or 0)
#                 total_funding = float(pos.get("totalFunding") or 0)
#                 open_fee      = float(pos.get("openFee") or 0)
#                 close_fee     = float(pos.get("closeFee") or 0)
#                 fee_total     = -abs(open_fee + close_fee)

#                 # ❌ Blindaje 2: duplicado exacto por (exchange, symbol, close_time)
#                 cur.execute("""
#                     SELECT id FROM closed_positions
#                     WHERE exchange=? AND symbol=? AND close_time=?
#                     LIMIT 1
#                 """, ("bitget", symbol, close_time))
#                 row = cur.fetchone()
#                 if row:
#                     skipped_dupe += 1
#                     if debug:
#                         print(f"  · dupe-clasico {symbol}: close_time={close_time} ya existe (id={row[0]})")
#                     continue

#                 # ❌ Blindaje 3: pseudo-duplicado por tolerancias (size & close_price) con close_time válido
#                 cur.execute("""
#                     SELECT id FROM closed_positions
#                     WHERE exchange=? AND symbol=? AND side=? AND close_time>0
#                       AND ABS(size - ?) <= 1e-8
#                       AND ABS(close_price - ?) <= 1e-10
#                     LIMIT 1
#                 """, ("bitget", symbol, side, size, close_price))
#                 row2 = cur.fetchone()
#                 if row2:
#                     skipped_dupe += 1
#                     if debug:
#                         print(f"  · dupe-tolerancia {symbol}: size/close_price coinciden (id={row2[0]})")
#                     continue

#                 # ✅ Listo para guardar SIN sobrescribir
#                 payload = {
#                     "exchange": "bitget",
#                     "symbol": symbol,
#                     "side": side,
#                     "size": size,
#                     "entry_price": entry_price,
#                     "close_price": close_price,
#                     "open_time": open_time,
#                     "close_time": close_time,
#                     "realized_pnl": net_profit,        # neto
#                     "funding_total": total_funding,
#                     "fee_total": fee_total,
#                     "notional": entry_price * size,
#                     "leverage": 1.0,
#                     "liquidation_price": None
#                 }

#                 save_closed_position(payload)  # ← no actualiza, solo inserta en nuestro flujo
#                 saved += 1
#                 if debug:
#                     print(f"  ✅ saved {symbol} {side} size={size} realized={net_profit:.6f}")

#             except Exception as e:
#                 skipped += 1
#                 if debug:
#                     print(f"  · error {pos.get('symbol')}: {e}")

#         conn.close()
#         print(f"✅ Bitget guardadas: {saved} | omitidas total: {skipped} | sin_fecha: {skipped_no_time} | duplicadas: {skipped_dupe}")

#     except Exception as e:
#         print(f"❌ Bitget closed positions error: {e}")


if __name__ == "__main__":
    import argparse
    from datetime import datetime, timezone

    def _iso(ms: int) -> str:
        try:
            return datetime.fromtimestamp(int(ms)/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
        except Exception:
            return str(ms)

    ap = argparse.ArgumentParser()
    ap.add_argument("--funding-debug", action="store_true", help="Debug interest-history paginado")
    ap.add_argument("--days", type=int, default=60, help="Ventana hacia atrás (días) si no se pasa 'since'")
    ap.add_argument("--since", type=int, default=None, help="Epoch ms de corte inferior")
    ap.add_argument("--limit", type=int, default=1000, help="Máximo de items totales a traer")
    ap.add_argument("--chunk", type=int, default=100, help="Items por request (máx 100)")
    ap.add_argument("--max-pages", type=int, default=30, help="Máximo de páginas")
    args = ap.parse_args()

    if args.funding_debug:
        print("🔎 Bitget interest-history DEBUG")
        rows = fetch_bitget_funding_fees(limit=args.limit,
                                         since=args.since,
                                         days=args.days,
                                         chunk=args.chunk,
                                         max_pages=args.max_pages,
                                         debug=True)
        print(f"\nResumen: items={len(rows)}")
        if rows:
            print("  earliest:", _iso(rows[0]["timestamp"]), "latest:", _iso(rows[-1]["timestamp"]))
            print("  sample:", rows[:3])
    else:
        # Smoke rápido de balances/abiertas/funding corto
        print("== balances ==")
        print(fetch_bitget_all_balances())
        print("\n== open positions ==")
        print(fetch_bitget_open_positions())
        print("\n== funding (debug corto) ==")
        print(fetch_bitget_funding_fees(limit=10, days=3, chunk=50, max_pages=3, debug=True))



# def debug_preview_bitget_closed(days: int = 3, symbol: Optional[str] = None) -> None:
#     """Debug: previsualiza lo que se guardaría para posiciones cerradas"""
#     print(f"🔍 Debug Bitget Closed Positions (últimos {days} días)")
    
#     end_time = int(time.time() * 1000)
#     start_time = end_time - (days * 24 * 60 * 60 * 1000)
    
#     data = _bitget_request("GET", "/api/v2/mix/position/history-position", {
#         "productType": "USDT-FUTURES",
#         "startTime": start_time,
#         "endTime": end_time,
#         "limit": 20
#     })
    
#     if data.get("code") != "00000":
#         print("❌ No se pudieron obtener datos")
#         return
    
#     for pos in data.get("data", {}).get("list", []):
#         sym = normalize_symbol(pos.get("symbol", ""))
#         if symbol and sym != symbol:
#             continue
            
#         net_profit = float(pos.get("netProfit", "0"))
#         total_funding = float(pos.get("totalFunding", "0"))
#         open_fee = float(pos.get("openFee", "0"))
#         close_fee = float(pos.get("closeFee", "0"))
#         fee_total = -abs(open_fee + close_fee)
#         price_pnl = net_profit - total_funding - fee_total
        
#         print(f"\n📊 {sym} {pos.get('holdSide')}:")
#         print(f"   Size: {pos.get('openTotalPos')}")
#         print(f"   Entry: {pos.get('openAvgPrice')} | Close: {pos.get('closeAvgPrice')}")
#         print(f"   Realized PnL (neto): {net_profit:.6f}")
#         print(f"   Price PnL: {price_pnl:.6f}")
#         print(f"   Funding: {total_funding:.6f}")
#         print(f"   Fees: {fee_total:.6f}")
#         print(f"   Open: {pos.get('cTime')} | Close: {pos.get('uTime')}")

# if __name__ == "__main__":
#     # Smoke tests
#     import sys
    
#     if "--dry-run" in sys.argv:
#         print("🧪 Bitget Adapter Smoke Tests")
        
#         # Test normalización
#         test_symbols = ["BTCUSDT", "BTC-USDT", "BTC_USDT", "PERP_BTCUSDT", "ETHUSDC-PERP"]
#         print("\n🔧 Normalization tests:")
#         for sym in test_symbols:
#             print(f"   {sym} -> {normalize_symbol(sym)}")
        
#         # Test balances
#         print("\n💰 Balance test:")
#         balances = fetch_bitget_all_balances()
#         if balances:
#             print(f"   Equity: {balances['equity']:.2f}")
#             print(f"   Spot: {balances['spot']:.2f}")
#             print(f"   Futures: {balances['futures']:.2f}")
#             print(f"   Unrealized PnL: {balances['unrealized_pnl']:.2f}")
        
#         # Test open positions
#         print("\n📈 Open positions test:")
#         positions = fetch_bitget_open_positions()
#         for pos in positions[:3]:  # Mostrar primeras 3
#             print(f"   {pos['symbol']} {pos['side']} size={pos['size']} unrealized={pos['unrealized_pnl']:.2f}")
        
#         # Test funding
#         print("\n💸 Funding test:")
#         funding = fetch_bitget_funding_fees(limit=5)
#         for f in funding:
#             print(f"   {f['asset']}: {f['income']:.6f} rate={f['funding_rate']:.6f}")
        
#         # Test closed preview
#         print("\n📊 Closed positions preview:")
#         debug_preview_bitget_closed(days=1)
        
#         print("\n✅ Smoke tests completed")

# ===================== RAW DEBUG: Historical Position =====================

# ===================== RAW DEBUG: Historical Position =====================

def debug_bitget_closed_raw(days: int = 30, symbol: str | None = None,
                            limit: int = 100, max_pages: int = 3,
                            print_items: int = 3, verbose: bool = True):
    """
    Descarga la respuesta RAW de /api/v2/mix/position/history-position
    e imprime estructura, tamaños y un sample de items (sin transformar).
    """
    import time, json
    start_ms = int(time.time() * 1000) - int(days * 24 * 60 * 60 * 1000)
    end_ms = int(time.time() * 1000)
    end_id = None
    total = 0

    print(f"🔍 Bitget RAW debug (últimos {days} días) limit={limit} páginas={max_pages} symbol={symbol or '-'}")
    for page in range(1, max_pages + 1):
        params = {
            "productType": "USDT-FUTURES",
            "startTime": str(start_ms),
            "endTime": str(end_ms),
            "limit": str(limit)
        }
        if symbol:
            params["symbol"] = symbol.upper()
        if end_id:
            params["idLessThan"] = end_id

        data = _bitget_request("GET", "/api/v2/mix/position/history-position", params)
        if not isinstance(data, dict):
            print(f"❌ Respuesta no dict: {type(data)} → {data}")
            break

        if data.get("code") != "00000":
            print(f"❌ API error: {data.get('msg')} ({data.get('code')})")
            break

        d = data.get("data") or {}
        lst = d.get("list") or []
        end_id = d.get("endId")
        count = len(lst)
        total += count
        print(f"📦 Página {page}: {count} items  endId={end_id}")

        if not count:
            break

        # imprime algunos items sin truncar claves (limita caracteres por item)
        import itertools
        for i, item in enumerate(itertools.islice(lst, 0, print_items), 1):
            try:
                print(f"  #{i}: {json.dumps(item, indent=2)[:1200]}")
            except Exception:
                print(f"  #{i}: {item}")
        if count > print_items:
            print(f"  ... ({count - print_items} más)")

        if not end_id:
            break

    print(f"✅ Total items recibidos: {total}")
    if total:
        try:
            keys = list((lst[0] or {}).keys())
            print("⚙️ Claves detectadas (primer item):", keys)
        except Exception:
            pass
    print("----------------------------------------------------------------------")
    return True

# ===================== AUTO-RUN PARA SPYDER =====================

# Ajustes rápidos para cuando pulsas Run
AUTO_DEBUG_RAW = True        # <- déjalo True para ver RAW al pulsar Run
AUTO_DAYS = 30               # ventana hacia atrás en días
AUTO_LIMIT = 50              # 1..100
AUTO_PAGES = 2               # páginas a seguir con idLessThan
AUTO_PRINT = 3               # cuantos items imprimir por página
AUTO_SYMBOL = None           # por ejemplo "BTCUSDT" o None para todo

if __name__ == "__main__":
    import os
    # Evita ejecutar otros modos; muestra RAW directamente al pulsar Run
    if AUTO_DEBUG_RAW or os.getenv("BITGET_DEBUG_RAW") == "1":
        debug_bitget_closed_raw(days=AUTO_DAYS,
                                symbol=AUTO_SYMBOL,
                                limit=AUTO_LIMIT,
                                max_pages=AUTO_PAGES,
                                print_items=AUTO_PRINT)
    else:
        print("ℹ️ Ajusta AUTO_DEBUG_RAW=True para mostrar RAW al pulsar Run.")



# # funcion para ejecutar el closed positons y luego el save closed positions
# if __name__ == "__main__":
#     import argparse
#     import os
#     from datetime import datetime, timezone

#     def _iso(ms: int) -> str:
#         try:
#             return datetime.fromtimestamp(int(ms)/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
#         except Exception:
#             return str(ms)

#     def manual_save_bitget_closed(days: int = 30, db_path: str = "portfolio.db", debug: bool = True):
#         """
#         Ejecuta la descarga de posiciones cerradas de los últimos `days` días
#         y las guarda en SQLite sin sobrescribir existentes.
#         """
#         print(f"🧩 Bitget: guardando cerradas (últimos {days} días) → {db_path}")
#         save_bitget_closed_positions(db_path=db_path, days=days, debug=debug)

#     ap = argparse.ArgumentParser()
#     ap.add_argument("--save-closed", action="store_true",
#                     help="Descarga y guarda posiciones cerradas de los últimos N días (por defecto 30).")
#     ap.add_argument("--days", type=int, default=30,
#                     help="Ventana hacia atrás en días (por defecto 30).")
#     ap.add_argument("--db", type=str, default="portfolio.db",
#                     help="Ruta a la base de datos SQLite (por defecto portfolio.db).")
#     ap.add_argument("--funding-debug", action="store_true",
#                     help="(Opcional) debug corto de funding, no afecta a cerradas.")
#     ap.add_argument("--limit", type=int, default=1000)
#     ap.add_argument("--since", type=int, default=None)
#     ap.add_argument("--chunk", type=int, default=100)
#     ap.add_argument("--max-pages", type=int, default=30)
#     args = ap.parse_args()

#     # Modo 1: ejecutar funding debug si se pidió explícitamente
#     if args.funding_debug:
#         print("🔎 Bitget interest-history DEBUG")
#         rows = fetch_bitget_funding_fees(limit=args.limit,
#                                          since=args.since,
#                                          days=args.days,
#                                          chunk=args.chunk,
#                                          max_pages=args.max_pages,
#                                          debug=True)
#         print(f"\nResumen: items={len(rows)}")
#         if rows:
#             print("  earliest:", _iso(rows[0]['timestamp']), "latest:", _iso(rows[-1]['timestamp']))
#             print("  sample:", rows[:3])
#         raise SystemExit(0)

#     # Modo 2: si se pasa --save-closed o la variable de entorno BITGET_SAVE_CLOSED_AUTO=1,
#     # ejecuta el guardado de cerradas en la ventana pedida (por defecto 30 días).
#     if args.save_closed or os.getenv("BITGET_SAVE_CLOSED_AUTO") == "1":
#         manual_save_bitget_closed(days=args.days, db_path=args.db, debug=True)
#         raise SystemExit(0)

#     # Modo 3 (por defecto): auto-guardar cerradas 30 días al dar a RUN
#     # (Comportamiento pedido: que con Run busque 30 días atrás y guarde)
#     manual_save_bitget_closed(days=30, db_path="portfolio.db", debug=True)