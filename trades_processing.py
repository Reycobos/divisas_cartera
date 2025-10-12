from collections import defaultdict
import statistics
from db_manager import save_closed_position
from datetime import datetime
from db_manager import save_closed_position
from datetime import datetime, timedelta
import sqlite3
import os
import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from datetime import datetime, timedelta
from collections import defaultdict

portfolio = "portfoliov2_5"
def _p():
    """Importa dinámicamente el módulo del portfolio activo."""
    import importlib
    return importlib.import_module(portfolio)










def build_positions_from_trades(trades):
    """
    Crea posiciones cerradas a partir de trades.
    Cada trade debe tener: symbol, side, qty, price, commission, time.
    """
    positions = []
    grouped = defaultdict(list)

    for t in trades:
        grouped[t["symbol"]].append(t)

    for sym, sym_trades in grouped.items():
        sym_trades.sort(key=lambda x: x["time"])
        qty_net = 0.0
        entry_prices = []
        open_time = None
        fees_total = 0.0

        for t in sym_trades:
            side = 1 if t["side"].lower() == "buy" else -1
            qty = float(t["qty"]) * side
            price = float(t["price"])
            commission = float(t.get("commission", 0))
            fees_total += commission

            if qty_net == 0:
                open_time = t["time"]
                entry_prices = [price]

            qty_net += qty

            if abs(qty_net) < 1e-9:
                close_time = t["time"]
                close_price = price
                entry_price = statistics.mean(entry_prices)
                realized_pnl = sum(
                    (float(tr["price"]) - entry_price) * float(tr["qty"])
                    * (1 if tr["side"].lower() == "sell" else -1)
                    for tr in sym_trades if open_time <= tr["time"] <= close_time
                )

                positions.append({
                    "symbol": sym,
                    "side": "long" if qty > 0 else "short",
                    "size": abs(qty),
                    "entry_price": entry_price,
                    "close_price": close_price,
                    "open_time": open_time,
                    "close_time": close_time,
                    "realized_pnl": realized_pnl,
                    "fee_total": -fees_total
                })

                qty_net = 0.0
                fees_total = 0.0
                entry_prices = []
            else:
                entry_prices.append(price)

    return positions


def attach_funding_to_positions(positions, funding):
    """
    funding: lista con symbol, income, timestamp
    """
    for pos in positions:
        relevant = [
            f for f in funding
            if f["symbol"] == pos["symbol"]
            and pos["open_time"] <= f["timestamp"] <= pos["close_time"]
        ]
        pos["funding_total"] = sum(f["income"] for f in relevant)
    return positions


def process_closed_positions(exchange, trades, funding):
    """
    Calcula, ajusta funding y guarda en SQLite.
    """
    positions = build_positions_from_trades(trades)
    positions = attach_funding_to_positions(positions, funding)

    for pos in positions:
        pos["exchange"] = exchange
        save_closed_position(pos)

    print(f"✅ {exchange}: {len(positions)} posiciones cerradas guardadas.")
    
#/////// BackpackConfig////////

def _parse_ts_to_ms(ts):
    """Acepta ISO, epoch s/ms y devuelve epoch ms (o None)."""
    if not ts:
        return None
    try:
        if isinstance(ts, (int, float)):
            v = int(ts)
            return v if v > 10_000_000_000 else v * 1000  # ms si es grande
        ts_str = str(ts).replace("Z", "")
        dt = datetime.fromisoformat(ts_str)
        return int(dt.timestamp() * 1000)
    except Exception:
        try:
            v = int(ts)
            return v if v > 10_000_000_000 else v * 1000
        except Exception:
            return None

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
    
def fetch_closed_positions_backpack(limit=200, debug=False):
    """
    Reconstruye posiciones cerradas de Backpack.
    - Agrupa fills por símbolo en orden temporal.
    - Cada bloque termina cuando el neto vuelve a 0.
    - Calcula entry/close, fees, realized PnL.
    - Integra funding fees en el rango temporal del bloque.
    """
    try:
        p = _p()
        backpack_signed_request = p.backpack_signed_request
        _normalize_symbol = p._normalize_symbol
        fetch_funding_backpack = p.fetch_funding_backpack

        path = "/wapi/v1/history/fills"
        instruction = "fillHistoryQueryAll"
        params = {"limit": limit, "sortDirection": "Desc"}

        data = backpack_signed_request("GET", path, instruction, params=params)
        items = data if isinstance(data, list) else (data.get("data") or [])
        if not items:
            if debug: print("[DEBUG][Backpack] No fills found")
            return []

        # Normalizar fills
        fills = []
        for f in items:
            try:
                sym = _normalize_symbol(f.get("symbol",""))
                side = (f.get("side") or "").lower()
                qty = float(f.get("quantity",0))
                price = float(f.get("price",0))
                fee = float(f.get("fee") or f.get("feeAmount") or 0.0)
                ts = _parse_ts_to_ms(f.get("timestamp"))
                signed = qty if side in ("bid","buy") else -qty
                fills.append({"symbol":sym,"side":side,"qty":qty,"price":price,
                              "fee":fee,"signed":signed,"ts":ts})
            except Exception as e:
                if debug: print("[WARN] bad fill:", f, e)
                continue
        fills.sort(key=lambda x: x["ts"])

        # Funding (todos, los filtramos después por rango)
        funding_all = fetch_funding_backpack(limit=500)

        grouped = defaultdict(list)
        for f in fills:
            grouped[f["symbol"]].append(f)

        results = []

        for sym, fs in grouped.items():
            net = 0.0
            block=[]
            for f in fs:
                net += f["signed"]
                block.append(f)
                if debug:
                    print(f"{sym} {f['side']} {f['qty']} @ {f['price']} net={net:.2f}")
                # cerrar bloque solo cuando net==0
                if abs(net) < 1e-9:
                    entry_qty = sum(x["qty"] for x in block if x["signed"]>0)
                    close_qty = sum(x["qty"] for x in block if x["signed"]<0)
                    if entry_qty and close_qty:
                        entry_avg = sum(x["qty"]*x["price"] for x in block if x["signed"]>0)/entry_qty
                        close_avg = sum(x["qty"]*x["price"] for x in block if x["signed"]<0)/close_qty
                        size = min(entry_qty, close_qty)
                        fees = sum(x["fee"] for x in block)
                        open_ms = min(x["ts"] for x in block)
                        close_ms = max(x["ts"] for x in block)
                        # integrar funding del rango temporal
                        funding_fee = sum(
                            fnd["income"] for fnd in funding_all
                            if fnd["symbol"] == sym and fnd["timestamp"] and open_ms <= fnd["timestamp"] <= close_ms
                        )
                        pnl = (close_avg - entry_avg) * size - fees + funding_fee
                        results.append({
                            "exchange": "backpack",
                            "symbol": sym,
                            "side": "closed",
                            "size": size,
                            "entry_price": entry_avg,
                            "close_price": close_avg,
                            "notional": entry_avg * size,
                            "fees": fees,
                            "funding_fee": funding_fee,
                            "realized_pnl": pnl,
                            "open_date": datetime.fromtimestamp(open_ms/1000).strftime("%Y-%m-%d %H:%M"),
                            "close_date": datetime.fromtimestamp(close_ms/1000).strftime("%Y-%m-%d %H:%M"),
                        })
                        if debug:
                            print(f"  ✅ BLOCK {sym} entry={entry_avg:.4f} close={close_avg:.4f} "
                                  f"size={size:.2f} pnl={pnl:.4f} fees={fees:.4f} funding={funding_fee:.4f}")
                    # reset
                    block=[]
                    net=0.0

        if debug:
            print(f"✅ Backpack closed positions: {len(results)}")
        return results

    except Exception as e:
        print(f"❌ Error al reconstruir closed positions Backpack: {e}")
        return []

    

def save_backpack_closed_positions(db_path="portfolio.db"):
    """
    Obtiene los fills cerrados desde Backpack, los transforma al formato interno
    y los guarda en la base de datos (tabla closed_positions).
    """
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return

    closed_positions = fetch_closed_positions_backpack(limit=200)
    if not closed_positions:
        print("⚠️ No closed positions returned from Backpack.")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    saved_count = 0
    skipped = 0

    for pos in closed_positions:
        try:
            # Convertir fechas a timestamp (en segundos)
            def to_ts(dt_str):
                try:
                    return int(datetime.fromisoformat(dt_str.replace("Z", "")).timestamp())
                except Exception:
                    return None

            open_ts = to_ts(pos.get("open_date"))
            close_ts = to_ts(pos.get("close_date"))

            # Evitar duplicados: misma exchange, symbol y close_time
            cur.execute("""
                SELECT COUNT(*) FROM closed_positions
                WHERE exchange = ? AND symbol = ? AND close_time = ?
            """, (pos["exchange"], pos["symbol"], close_ts))
            exists = cur.fetchone()[0]

            if exists:
                skipped += 1
                continue

            save_closed_position({
                "exchange": pos["exchange"],
                "symbol": pos["symbol"],
                "side": pos["side"],
                "size": pos["size"],
                "entry_price": pos["entry_price"],
                "close_price": pos["close_price"],
                "open_time": open_ts,
                "close_time": close_ts,
                "realized_pnl": pos["realized_pnl"],
                "funding_total": pos.get("funding_fee", 0.0),
                "fee_total": pos.get("fees", 0.0),
                "notional": pos["notional"],
                "leverage": None,
                "liquidation_price": None
            })
            
            saved_count += 1

        except Exception as e:
            print(f"⚠️ Error saving Backpack position {pos.get('symbol')}: {e}")
            continue

    conn.close()
    print(f"✅ Guardadas {saved_count} posiciones cerradas de Backpack (omitidas {skipped} duplicadas).")
    



#-------------BinanceConfig--------------
# Binance


def fetch_closed_positions_binance(days=30, off=0, debug=False):
    """
    Reconstruye posiciones cerradas de Binance Futures para los últimos 30 días
    usando /fapi/v1/userTrades e /fapi/v1/income.
    """
    # A partir de aquí, puedes usar tus requests normales
    # Por ejemplo:


    p = _p()
    BINANCE_API_KEY = p.BINANCE_API_KEY
    BINANCE_API_SECRET = p.BINANCE_API_SECRET
    BINANCE_BASE_URL = p.BINANCE_BASE_URL
    UA_HEADERS = getattr(p, "UA_HEADERS", {})
    
    def signed_get(path, params=None):
        params = dict(params or {})
        params["timestamp"] = int(time.time() * 1000)
        qs = urlencode(params)
        sig = hmac.new(BINANCE_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
        url = f"{BINANCE_BASE_URL}{path}?{qs}&signature={sig}"
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY, **UA_HEADERS}
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json()

    try:
        now = int(time.time() * 1000)
        start_time = now - days * 24 * 60 * 60 * 1000

        # 1️⃣ Income global (para funding, PnL, fees)
        income = signed_get("/fapi/v1/income", {"limit": 1000, "startTime": start_time, "endTime": now})
        income_by_symbol = defaultdict(list)
        for inc in income:
            if inc["incomeType"] in ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE"):
                income_by_symbol[inc["symbol"]].append(inc)

        symbols = list(income_by_symbol.keys())
        results = []

        # 2️⃣ Validar símbolos disponibles (evitar error 400)
        try:
            exchange_info = signed_get("/fapi/v1/exchangeInfo")
            valid_symbols = {s["symbol"] for s in exchange_info.get("symbols", [])}
        except Exception:
            valid_symbols = set(symbols)

        # 3️⃣ Iterar cada símbolo en ventanas de 7 días
        for sym in symbols:
            if sym not in valid_symbols:
                if debug:
                    print(f"[SKIP] {sym} no está en Binance Futures.")
                continue

            end_time = start_time
            while end_time < now:
                chunk_start = end_time
                chunk_end = min(chunk_start + 7 * 24 * 60 * 60 * 1000, now)
                end_time = chunk_end

                try:
                    trades = signed_get("/fapi/v1/userTrades", {
                        "symbol": sym,
                        "startTime": chunk_start,
                        "endTime": chunk_end,
                        "limit": 1000
                    })
                except requests.HTTPError as e:
                    if debug:
                        print(f"[WARN] {sym} ventana {chunk_start}->{chunk_end} error {e}")
                    continue

                if not trades:
                    continue

                trades.sort(key=lambda x: x["time"])
                net_qty = 0.0
                block = []

                for t in trades:
                    qty = float(t["qty"]) if t["side"] == "BUY" else -float(t["qty"])
                    net_qty += qty
                    block.append(t)

                    if abs(net_qty) < 1e-8:  # posición cerrada
                        open_t = block[0]["time"]
                        close_t = block[-1]["time"]
                        buys = [b for b in block if b["side"] == "BUY"]
                        sells = [s for s in block if s["side"] == "SELL"]

                        def avg_price(lst):
                            q = sum(float(x["qty"]) for x in lst)
                            n = sum(float(x["qty"]) * float(x["price"]) for x in lst)
                            return n / q if q else 0.0

                        entry = avg_price(buys)
                        close = avg_price(sells)
                        size = sum(float(b["qty"]) for b in buys)

                        # 4️⃣ Income asociado
                        incs = [i for i in income_by_symbol[sym] if open_t <= i["time"] <= close_t]
                        pnl = sum(float(i["income"]) for i in incs if i["incomeType"] == "REALIZED_PNL")
                        fees = sum(float(i["income"]) for i in incs if i["incomeType"] == "COMMISSION")
                        funding = sum(float(i["income"]) for i in incs if i["incomeType"] == "FUNDING_FEE")

                        results.append({
                            "exchange": "binance",
                            "symbol": sym,
                            "side": "closed",
                            "size": size,
                            "entry_price": entry,
                            "close_price": close,
                            "notional": entry * size,
                            "fees": fees,
                            "funding_fee": funding,
                            "realized_pnl": pnl,
                            "open_date": datetime.fromtimestamp(open_t / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                            "close_date": datetime.fromtimestamp(close_t / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                        })
                        block = []

        print(f"✅ Binance closed positions reconstruidas: {len(results)} en {days} días")
        return results

    except Exception as e:
        print(f"❌ Binance closed positions error: {e}")
        return []


def save_binance_closed_positions(db_path="portfolio.db", days=30):
    """
    Guarda las posiciones cerradas de Binance en la base de datos.
    """
    closed_positions = fetch_closed_positions_binance(days=days)
    if not closed_positions:
        print("⚠️ No se encontraron posiciones cerradas en Binance.")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    saved = 0
    skipped = 0

    for pos in closed_positions:
        try:
            def to_ts(dt):
                return int(datetime.fromisoformat(dt).timestamp())

            open_ts = to_ts(pos["open_date"])
            close_ts = to_ts(pos["close_date"])

            cur.execute("""
                SELECT COUNT(*) FROM closed_positions
                WHERE exchange = ? AND symbol = ? AND close_time = ?
            """, (pos["exchange"], pos["symbol"], close_ts))
            if cur.fetchone()[0]:
                skipped += 1
                continue

            cur.execute("""
                INSERT INTO closed_positions (
                    exchange, symbol, side, size, entry_price, close_price,
                    open_time, close_time, realized_pnl, funding_total,
                    fee_total, notional, leverage, liquidation_price
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pos["exchange"],
                pos["symbol"],
                pos["side"],
                pos["size"],
                pos["entry_price"],
                pos["close_price"],
                open_ts,
                close_ts,
                pos["realized_pnl"],
                pos.get("funding_fee", 0.0),
                -abs(pos.get("fees", 0.0)),
                pos["notional"],
                None,
                None
            ))
            saved += 1
        except Exception as e:
            print(f"⚠️ Error guardando {pos['symbol']}: {e}")
            continue

    conn.commit()
    conn.close()
    print(f"✅ Guardadas {saved} posiciones cerradas de Binance (omitidas {skipped} duplicadas).")

#------------ Asterconfig------------

# --- ASTER: reconstrucción de posiciones cerradas ----------------------------
def fetch_closed_positions_aster(limit=1000, debug=False):
    """
    Reconstruye posiciones cerradas de Aster Futures.
    - Usa /fapi/v1/userTrades (solo últimos 7 días)
    - Cierra bloque cuando el neto vuelve a 0
    - Calcula entry/close, fees, realized PnL y funding fees
    """
    try:
        p = _p()
        aster_signed_request = p.aster_signed_request
        _normalize_symbol = p._normalize_symbol
        fetch_funding_aster = p.fetch_funding_aster
    except Exception as e:
        print(f"⚠️ No se pudo cargar el módulo {portfolio}: {e}")

        # 1️⃣ Detectar símbolos activos mediante funding (así evitamos llamadas vacías)
        funding_all = fetch_funding_aster(limit=500)
        symbols = sorted(set(_normalize_symbol(f["symbol"]) for f in funding_all if f.get("symbol")))
        if not symbols:
            print("[Aster] No symbols found for reconstruction.")
            return []

        if debug:
            print(f"[Aster] Símbolos detectados: {symbols}")

        results = []

        # 2️⃣ Iterar sobre cada símbolo
        for sym in symbols:
            params = {"symbol": sym, "limit": limit}  # ✅ Últimos 7 días (sin rango)
            data = aster_signed_request("/fapi/v1/userTrades", params=params)
            items = data if isinstance(data, list) else (data.get("data") or [])
            if not items:
                if debug:
                    print(f"[Aster] {sym}: 0 trades encontrados")
                continue

            # 3️⃣ Normalizar estructura
            trades = []
            for t in items:
                try:
                    side = (t.get("side") or "").upper()
                    qty = float(t.get("qty") or t.get("quantity") or 0)
                    price = float(t.get("price") or 0)
                    commission = abs(float(t.get("commission", 0)))
                    realized = float(t.get("realizedPnl") or 0)
                    ts = int(t.get("time", 0))
                    signed = qty if side == "BUY" else -qty
                    trades.append({
                        "symbol": sym,
                        "side": side,
                        "qty": qty,
                        "signed": signed,
                        "price": price,
                        "fee": commission,
                        "realized": realized,
                        "ts": ts,
                    })
                except Exception as e:
                    if debug:
                        print(f"[WARN] {sym} trade malformado: {e}")
                    continue

            trades.sort(key=lambda x: x["ts"])
            if not trades:
                continue

            # 4️⃣ Funding fees para este símbolo
            fnd = [f for f in funding_all if f["symbol"] == sym]

            # 5️⃣ Reconstruir bloques de posiciones cerradas
            net = 0.0
            block = []

            for t in trades:
                net += t["signed"]
                block.append(t)

                #if debug:
                    #print(f"[Aster] {sym} {t['side']} {t['qty']} @ {t['price']} net={net:.4f}")

                if abs(net) < 1e-9:
                    buys = [x for x in block if x["signed"] > 0]
                    sells = [x for x in block if x["signed"] < 0]
                    if not buys or not sells:
                        block = []
                        net = 0.0
                        continue

                    buy_qty = sum(x["qty"] for x in buys)
                    sell_qty = sum(x["qty"] for x in sells)
                    entry_avg = sum(x["qty"] * x["price"] for x in buys) / buy_qty
                    close_avg = sum(x["qty"] * x["price"] for x in sells) / sell_qty
                    size = min(buy_qty, sell_qty)
                    fees = sum(x["fee"] for x in block)
                    pnl_trades = sum(x["realized"] for x in block)
                    open_ts = min(x["ts"] for x in block)
                    close_ts = max(x["ts"] for x in block)

                    funding_fee = sum(
                        ff["income"] for ff in fnd
                        if ff.get("timestamp") and open_ts <= ff["timestamp"] <= close_ts
                    )

                    total_pnl = pnl_trades - fees + funding_fee

                    results.append({
                        "exchange": "aster",
                        "symbol": sym,
                        "side": "closed",
                        "size": size,
                        "entry_price": entry_avg,
                        "close_price": close_avg,
                        "notional": entry_avg * size,
                        "fees": fees,
                        "funding_fee": funding_fee,
                        "realized_pnl": total_pnl,
                        "open_date": datetime.fromtimestamp(open_ts / 1000).strftime("%Y-%m-%d %H:%M"),
                        "close_date": datetime.fromtimestamp(close_ts / 1000).strftime("%Y-%m-%d %H:%M"),
                    })

                    if debug:
                        print(f"  ✅ {sym} BLOCK: size={size:.4f} entry={entry_avg:.4f} "
                              f"close={close_avg:.4f} pnl={total_pnl:.4f} fees={fees:.4f} funding={funding_fee:.4f}")

                    net = 0.0
                    block = []

        print(f"✅ Aster closed positions reconstruidas: {len(results)}")
        return results

    except Exception as e:
        print(f"❌ Error al reconstruir closed positions Aster: {e}")
        return []


def save_aster_closed_positions(db_path="portfolio.db", debug=False):
    """
    Guarda las posiciones cerradas de Aster en la base de datos SQLite.
    """
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return

    closed_positions = fetch_closed_positions_aster(debug=debug)
    if not closed_positions:
        print("⚠️ No closed positions returned from Aster.")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    saved = 0
    skipped = 0

    for pos in closed_positions:
        try:
            def to_ts(dt_str):
                try:
                    return int(datetime.fromisoformat(dt_str).timestamp())
                except Exception:
                    return None

            open_ts = to_ts(pos["open_date"])
            close_ts = to_ts(pos["close_date"])

            cur.execute("""
                SELECT COUNT(*) FROM closed_positions
                WHERE exchange = ? AND symbol = ? AND close_time = ?
            """, (pos["exchange"], pos["symbol"], close_ts))
            if cur.fetchone()[0]:
                skipped += 1
                continue

            save_closed_position({
                "exchange": pos["exchange"],
                "symbol": pos["symbol"],
                "side": pos["side"],
                "size": pos["size"],
                "entry_price": pos["entry_price"],
                "close_price": pos["close_price"],
                "open_time": open_ts,
                "close_time": close_ts,
                "realized_pnl": pos["realized_pnl"],
                "funding_total": pos.get("funding_fee", 0.0),
                "fee_total": pos.get("fees", 0.0),
                "notional": pos["notional"],
                "leverage": None,
                "liquidation_price": None
            })
            saved += 1

        except Exception as e:
            print(f"⚠️ Error guardando posición {pos.get('symbol')} (Aster): {e}")

    conn.close()
    print(f"✅ Guardadas {saved} posiciones cerradas de Aster (omitidas {skipped} duplicadas).")

    
# ======================= BingX (desde cero, solo doc) =========================



import time as _time
import hmac as _hmac
from hashlib import sha256 as _sha256
import requests as _requests


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
    
    


#===================== Fin BingX (desde cero) ================================


# ===================== Fin BingX (desde cero) ================================

    
# Codigo puntual para reconstruir posiciones viejas en Aster, mayores de 7 dias
# para ejecutarlo, usar el script al final en otro archivo.

# def fetch_all_user_trades_aster(symbol, days_back=30, limit=1000, debug=True):
#     """
#     Descarga todo el historial de trades de Aster para un símbolo, 
#     dividiendo en ventanas de ≤7 días (la API no acepta rangos mayores).
#     """
#     from datetime import datetime, timedelta
#     import time
#     from portfoliov1_9 import aster_signed_request

#     now = datetime.utcnow()
#     end = now
#     start = now - timedelta(days=days_back)
#     all_trades = []
#     total = 0

#     print(f"🕒 Descargando historial de {symbol} desde {start.date()} hasta {end.date()}...")

#     while start < end:
#         end_window = start + timedelta(days=6, hours=23)
#         if end_window > end:
#             end_window = end

#         start_ms = int(start.timestamp() * 1000)
#         end_ms = int(end_window.timestamp() * 1000)

#         params = {
#             "symbol": symbol,
#             "startTime": start_ms,
#             "endTime": end_ms,
#             "limit": limit
#         }

#         try:
#             data = aster_signed_request("/fapi/v1/userTrades", params=params)
#             if isinstance(data, list) and len(data) > 0:
#                 all_trades.extend(data)
#                 total += len(data)
#                 if debug:
#                     print(f"✅ {start.date()} → {end_window.date()} : {len(data)} trades")
#             else:
#                 if debug:
#                     print(f"⚠️ {start.date()} → {end_window.date()} : sin datos")
#         except Exception as e:
#             print(f"❌ Error en rango {start.date()} → {end_window.date()}: {e}")

#         start = end_window + timedelta(days=1)
#         time.sleep(0.6)

#     print(f"📊 Total descargado: {total} trades ({symbol})")
#     return all_trades


# def save_all_user_trades_aster_to_db_all_symbols(days_back=30, db_path="portfolio.db", debug=True):
#     """
#     Descarga y guarda en base de datos todas las posiciones cerradas de Aster
#     para todos los símbolos detectados, incluyendo históricos (por rangos de 7 días).
#     """
#     import sqlite3, time
#     from datetime import datetime, timedelta
#     from portfoliov1_9 import (
#         fetch_funding_aster,
#         aster_signed_request,
#         _normalize_symbol
#     )
#     from trades_processing import save_closed_position, _aster_recent_symbols

#     # 1️⃣ Detectar símbolos activos
#     symbols = _aster_recent_symbols(debug=debug)
#     if not symbols:
#         f_all = fetch_funding_aster(limit=1000)
#         symbols = sorted(set(_normalize_symbol(f["symbol"]) for f in f_all if f.get("symbol")))
#     if not symbols:
#         print("❌ No se detectaron símbolos activos para Aster.")
#         return

#     print(f"🔍 Procesando símbolos: {symbols}")
#     conn = sqlite3.connect(db_path)
#     cur = conn.cursor()
#     total_saved, total_skipped = 0, 0

#     for symbol in symbols:
#         print(f"\n🕒 Descargando historial de {symbol} ({days_back} días)…")
#         all_trades = fetch_all_user_trades_aster(symbol, days_back=days_back, debug=debug)
#         if not all_trades:
#             print(f"⚠️ Ningún trade encontrado para {symbol}.")
#             continue

#         # Normalizar trades
#         normalized = []
#         for t in all_trades:
#             try:
#                 side = (t.get("side") or "").upper()
#                 qty = float(t.get("qty") or 0)
#                 price = float(t.get("price") or 0)
#                 fee = -abs(float(t.get("commission", 0)))  # 🔧 fees negativas
#                 realized = float(t.get("realizedPnl") or 0)
#                 ts = int(t.get("time", 0))
#                 signed = qty if side == "BUY" else -qty
#                 normalized.append({
#                     "symbol": symbol,
#                     "side": side,
#                     "qty": qty,
#                     "signed": signed,
#                     "price": price,
#                     "fee": fee,
#                     "realized": realized,
#                     "ts": ts
#                 })
#             except Exception as e:
#                 if debug:
#                     print(f"[WARN] {symbol} trade malformado: {e}")
#                 continue

#         normalized.sort(key=lambda x: x["ts"])
#         if not normalized:
#             continue

#         # Funding asociado
#         f_all = fetch_funding_aster(limit=1000)
#         funding_symbol = [f for f in f_all if f["symbol"] == symbol]

#         # Reconstruir bloques cerrados (net = 0)
#         net, block = 0.0, []
#         saved, skipped = 0, 0

#         for t in normalized:
#             net += t["signed"]
#             block.append(t)
#             if abs(net) < 1e-9:
#                 buys = [x for x in block if x["signed"] > 0]
#                 sells = [x for x in block if x["signed"] < 0]
#                 if not buys or not sells:
#                     block, net = [], 0.0
#                     continue

#                 buy_qty = sum(x["qty"] for x in buys)
#                 sell_qty = sum(x["qty"] for x in sells)
#                 entry_avg = sum(x["qty"] * x["price"] for x in buys) / buy_qty
#                 close_avg = sum(x["qty"] * x["price"] for x in sells) / sell_qty
#                 size = min(buy_qty, sell_qty)
#                 fees = sum(x["fee"] for x in block)
#                 pnl_trades = sum(x["realized"] for x in block)
#                 open_ts = min(x["ts"] for x in block)
#                 close_ts = max(x["ts"] for x in block)

#                 funding_fee = sum(
#                     f["income"] for f in funding_symbol
#                     if f.get("timestamp") and open_ts <= f["timestamp"] <= close_ts
#                 )
#                 total_pnl = pnl_trades + fees + funding_fee  # fees ya negativas

#                 cur.execute("""
#                     SELECT COUNT(*) FROM closed_positions
#                     WHERE exchange = ? AND symbol = ? AND close_time = ?
#                 """, ("aster", symbol, int(close_ts / 1000)))
#                 if cur.fetchone()[0]:
#                     skipped += 1
#                 else:
#                     save_closed_position({
#                         "exchange": "aster",
#                         "symbol": symbol,
#                         "side": "closed",
#                         "size": size,
#                         "entry_price": entry_avg,
#                         "close_price": close_avg,
#                         "open_time": int(open_ts / 1000),
#                         "close_time": int(close_ts / 1000),
#                         "realized_pnl": total_pnl,
#                         "funding_total": funding_fee,
#                         "fee_total": fees,  # fees negativas
#                         "notional": entry_avg * size,
#                         "leverage": None,
#                         "liquidation_price": None
#                     })
#                     saved += 1
#                     if debug:
#                         print(f"  ✅ {symbol}: size={size:.2f} entry={entry_avg:.4f} close={close_avg:.4f} pnl={total_pnl:.4f}")

#                 block, net = [], 0.0

#         total_saved += saved
#         total_skipped += skipped
#         print(f"📊 {symbol}: guardadas {saved}, omitidas {skipped} duplicadas.")

#     conn.close()
#     print(f"\n✅ Historial completo Aster: guardadas {total_saved}, omitidas {total_skipped}.")

#   Script para ejecutar en otro archivo y usar el codigo de arriba
# from trades_processing import save_all_user_trades_aster_to_db_all_symbols

# # 🔧 Configura cuántos días atrás quieres reconstruir
# DAYS_BACK = 30

# print(f"🚀 Reconstruyendo historial completo de Aster ({DAYS_BACK} días)...")
# save_all_user_trades_aster_to_db_all_symbols(days_back=DAYS_BACK, debug=True)
# print("✅ Sincronización histórica completada.")






