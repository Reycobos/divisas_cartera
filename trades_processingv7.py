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
    
def fetch_closed_positions_backpack(limit=1000, days=60, debug=False):
    """
    Reconstruye posiciones cerradas de Backpack (solo PERP/IPERP).
    - Evita arrays en la query para no romper la firma.
    - Intenta PERP e IPERP en llamadas separadas; si la firma falla, hace fallback sin filtro.
    - Corrige side/entry/close y PnL para shorts, y funding ±1h.
    """
    try:
        p = _p()
        backpack_signed_request = p.backpack_signed_request
        _normalize_symbol = p._normalize_symbol
        fetch_funding_backpack = p.fetch_funding_backpack

        now_ms = int(time.time() * 1000)
        from_ms = now_ms - days * 24 * 60 * 60 * 1000

        path = "/wapi/v1/history/fills"
        instruction = "fillHistoryQueryAll"

        def _try_fetch(market_type: str | None):
            params = {
                "limit": min(int(limit), 1000),
                "sortDirection": "Asc",
                "from": from_ms,
                "to": now_ms,
            }
            if market_type:
                params["marketType"] = market_type  # ⚠️ string, no array
            return backpack_signed_request("GET", path, instruction, params=params)

        items = []
        # 1) Intento PERP
        try:
            data = _try_fetch("PERP")
            items += (data if isinstance(data, list) else (data.get("data") or []))
        except Exception as e1:
            if debug:
                print("[Backpack] PERP fetch failed:", e1)

        # 2) Intento IPERP
        try:
            data = _try_fetch("IPERP")
            items += (data if isinstance(data, list) else (data.get("data") or []))
        except Exception as e2:
            if debug:
                print("[Backpack] IPERP fetch failed:", e2)

        # 3) Fallback sin marketType si ambos fallaron o vinieron vacíos
        if not items:
            if debug:
                print("[Backpack] Fallback sin marketType (filtramos localmente)")
            data = _try_fetch(None)
            items = data if isinstance(data, list) else (data.get("data") or [])
            # filtrar aquí a PERP/IPERP
            items = [it for it in items if "PERP" in (it.get("symbol") or "").upper()]

        if not items:
            if debug:
                print("[Backpack] No fills PERP/IPERP.")
            return []

        # --- Normalización de fills
        fills = []
        for f in items:
            try:
                sym = _normalize_symbol(f.get("symbol", ""))
                side = (f.get("side") or "").lower()  # "bid"/"ask"
                qty = float(f.get("quantity", 0))
                price = float(f.get("price", 0))
                fee = float(f.get("fee") or f.get("feeAmount") or 0.0)
                ts = _parse_ts_to_ms(f.get("timestamp"))
                if ts is None:
                    continue
                signed = qty if side in ("bid", "buy") else -qty
                fills.append({
                    "symbol": sym, "side": side, "qty": qty, "price": price,
                    "fee": fee, "signed": signed, "ts": ts
                })
            except Exception as e:
                if debug:
                    print("[WARN] bad fill:", f, e)
                continue

        if not fills:
            if debug:
                print("[Backpack] No normalized fills.")
            return []

        fills.sort(key=lambda x: x["ts"])

        # --- Funding (todos) y lo indexamos por símbolo
        funding_all = fetch_funding_backpack(limit=1000) or []
        from collections import defaultdict as _dd
        funding_by_sym = _dd(list)
        for rec in funding_all:
            try:
                s = _normalize_symbol(rec.get("symbol", ""))
                rec_ts = rec.get("timestamp")
                rec_inc = float(rec.get("income") or 0)
                if s and rec_ts:
                    funding_by_sym[s].append({"timestamp": rec_ts, "income": rec_inc})
            except Exception:
                continue

        # --- Reconstrucción por bloques net=0
        grouped = _dd(list)
        for f in fills:
            grouped[f["symbol"]].append(f)

        results = []

        for sym, fs in grouped.items():
            net = 0.0
            block = []

            for f in fs:
                net += f["signed"]
                block.append(f)

                if abs(net) < 1e-9 and block:
                    first_signed = block[0]["signed"]
                    is_short = first_signed < 0

                    buys = [x for x in block if x["signed"] > 0]
                    sells = [x for x in block if x["signed"] < 0]

                    buy_qty = sum(x["qty"] for x in buys)
                    sell_qty = sum(x["qty"] for x in sells)
                    if buy_qty == 0 or sell_qty == 0:
                        block, net = [], 0.0
                        continue

                    def avg(trades):
                        q = sum(t["qty"] for t in trades)
                        n = sum(t["qty"] * t["price"] for t in trades)
                        return (n / q) if q else 0.0

                    if is_short:
                        side = "short"
                        entry_avg = avg(sells)  # vendiste primero
                        close_avg = avg(buys)
                    else:
                        side = "long"
                        entry_avg = avg(buys)   # compraste primero
                        close_avg = avg(sells)

                    size = min(buy_qty, sell_qty)
                    fees = sum(x["fee"] for x in block)
                    open_ms = min(x["ts"] for x in block)
                    close_ms = max(x["ts"] for x in block)

                    # funding ±1h
                    margin = 3600_000
                    funding_fee = 0.0
                    for fr in funding_by_sym.get(sym, []):
                        tsf = fr["timestamp"]
                        if (open_ms - margin) <= tsf <= (close_ms + margin):
                            funding_fee += fr["income"]

                    pnl = ((entry_avg - close_avg) if side == "short" else (close_avg - entry_avg)) \
                          * size - fees + funding_fee

                    results.append({
                        "exchange": "backpack",
                        "symbol": sym,
                        "side": side,
                        "size": size,
                        "entry_price": entry_avg,
                        "close_price": close_avg,
                        "notional": entry_avg * size,
                        "fees": fees,
                        "funding_fee": funding_fee,
                        "realized_pnl": pnl,
                        "open_date": datetime.fromtimestamp(open_ms / 1000).strftime("%Y-%m-%d %H:%M"),
                        "close_date": datetime.fromtimestamp(close_ms / 1000).strftime("%Y-%m-%d %H:%M"),
                    })

                    if debug:
                        print(f"[BP] {sym} {side.upper()} size={size:.4f} "
                              f"entry={entry_avg:.6f} close={close_avg:.6f} "
                              f"fees={fees:.4f} funding={funding_fee:.4f} pnl={pnl:.4f}")

                    block, net = [], 0.0

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


# codigo que saca bien fees, funding pero no reconoce los short
def fetch_closed_positions_binance(days=30, off=0, debug=False):
    """
    Reconstruye posiciones cerradas de Binance Futures en los últimos `days`.
    - Ventanas de 7 días (limitación API) con estado 'carry-over' por símbolo.
    - side correcto (long/short) por neto del bloque.
    - entry/close correctos (para short se invierte).
    - income asociado por tradeId cuando esté disponible; si no, por rango de tiempo.
    - realized_pnl = SOLO precio (tu UI ya muestra fees y funding aparte).
    """
    import time, hmac, hashlib, requests
    from urllib.parse import urlencode
    from collections import defaultdict
    from datetime import datetime, timezone

    def _iso(ms):
        try:
            return datetime.fromtimestamp(ms/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(ms)

    p = _p()
    BINANCE_API_KEY    = p.BINANCE_API_KEY
    BINANCE_API_SECRET = p.BINANCE_API_SECRET
    BINANCE_BASE_URL   = p.BINANCE_BASE_URL
    UA_HEADERS         = getattr(p, "UA_HEADERS", {})

    def signed_get(path, params=None):
        params = dict(params or {})
        params["timestamp"] = int(time.time() * 1000) + int(off)
        qs = urlencode(params, doseq=True)
        sig = hmac.new(BINANCE_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
        url = f"{BINANCE_BASE_URL}{path}?{qs}&signature={sig}"
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY, **UA_HEADERS}
        if debug:
            print(f"[GET] {path} {params}")
        r = requests.get(url, headers=headers, timeout=25)
        r.raise_for_status()
        return r.json()

    try:
        now = int(time.time() * 1000)
        start_time = now - days * 24 * 60 * 60 * 1000

        # 1) INCOME de todo el rango, paginado por 'page'
        income_by_symbol = defaultdict(list)
        page = 1
        while True:
            inc = signed_get("/fapi/v1/income", {
                "limit": 1000, "startTime": start_time, "endTime": now, "page": page
            })
            if not inc:
                break
            for i in inc:
                t = i.get("incomeType")
                sym = i.get("symbol") or ""
                if t in ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE") and sym:
                    income_by_symbol[sym].append(i)
            if len(inc) < 1000:
                break
            page += 1

        if debug:
            print("[Income] resumen:")
            for sym, arr in income_by_symbol.items():
                s_pnl = sum(float(x["income"]) for x in arr if x["incomeType"]=="REALIZED_PNL")
                s_fee = sum(float(x["income"]) for x in arr if x["incomeType"]=="COMMISSION")
                s_fnd = sum(float(x["income"]) for x in arr if x["incomeType"]=="FUNDING_FEE")
                print(f"  {sym}: pnl={s_pnl:.2f} fee={s_fee:.2f} fnd={s_fnd:.2f} items={len(arr)}")

        # 2) exchangeInfo (opcional)
        try:
            exi = signed_get("/fapi/v1/exchangeInfo")
            valid_symbols = {s["symbol"] for s in exi.get("symbols", [])}
        except Exception:
            valid_symbols = set(income_by_symbol.keys())

        results = []

        # 3) Estado carry-over por símbolo (bloque abierto y net_qty acumulado)
        carry_block_by_sym = {}   # sym -> list of trades
        carry_net_by_sym   = {}   # sym -> float

        # 4) Procesar símbolos
        for sym in list(income_by_symbol.keys()):
            if sym not in valid_symbols and debug:
                print(f"[SKIP] {sym} no en exchangeInfo")
            if debug:
                print(f"\n[Symbol] {sym} ventanas de 7d {_iso(start_time)} → {_iso(now)}")

            end_time = start_time
            # inicializar carry si existe
            block = carry_block_by_sym.get(sym, [])
            net_qty = carry_net_by_sym.get(sym, 0.0)

            while end_time < now:
                chunk_start = end_time
                chunk_end = min(chunk_start + 7*24*60*60*1000, now)
                end_time = chunk_end
                if debug:
                    print(f"  [Window] {_iso(chunk_start)} → {_iso(chunk_end)} (carry net={net_qty:.6f}, block={len(block)})")

                # Paginación userTrades por fromId
                trades_all = []
                last_id = None
                while True:
                    params = {
                        "symbol": sym,
                        "startTime": chunk_start,
                        "endTime": chunk_end,
                        "limit": 1000,
                    }
                    if last_id is not None:
                        params["fromId"] = last_id + 1
                    try:
                        tpage = signed_get("/fapi/v1/userTrades", params)
                    except Exception as e:
                        if debug:
                            print(f"    [WARN] userTrades fallo: {e}")
                        break
                    if not tpage:
                        break
                    trades_all.extend(tpage)
                    if len(tpage) < 1000:
                        break
                    last_id = int(tpage[-1]["id"])

                if debug:
                    print(f"    trades nuevos: {len(trades_all)}")

                if trades_all:
                    trades_all.sort(key=lambda x: x["time"])

                # 4.1 Prepend del carry al inicio de la ventana
                # (block ya contiene lo anterior, net_qty ya acumulado)
                for t in trades_all:
                    q = float(t["qty"])
                    qty_signed = q if t["side"] == "BUY" else -q
                    net_qty += qty_signed
                    block.append(t)

                    # ¿se cerró el bloque?
                    if abs(net_qty) < 1e-10:
                        open_t  = block[0]["time"]
                        close_t = block[-1]["time"]
                        buys  = [b for b in block if b["side"] == "BUY"]
                        sells = [s for s in block if s["side"] == "SELL"]

                        def avg_price(lst):
                            qsum = sum(float(x["qty"]) for x in lst)
                            nsum = sum(float(x["qty"]) * float(x["price"]) for x in lst)
                            return nsum / qsum if qsum else 0.0

                        long_qty  = sum(float(b["qty"]) for b in buys)
                        short_qty = sum(float(s["qty"]) for s in sells)
                        # ✅ REGLA ROBUSTA: side = primera trade del bloque
                        first_trade_side = block[0]["side"]
                        side = "long" if first_trade_side == "BUY" else "short"
                        # Para depurar, también calculamos la “dominancia” por cantidades
                        dominance_side = "long" if long_qty >= short_qty else "short"

                        if side == "long":
                            entry = avg_price(buys)
                            close = avg_price(sells)
                            size  = min(long_qty, short_qty)
                        else:
                            entry = avg_price(sells)
                            close = avg_price(buys)
                            size  = min(long_qty, short_qty)

                
                        # --- Asociar income al bloque ---
                        
                        block_trade_ids = {str(int(x["id"])) for x in block if "id" in x}
                        
                        # 1) PNL y COMMISSION: prioriza emparejar por tradeId; si no hay, cae a time-range
                        incs_pnl_fee = [
                            i for i in income_by_symbol[sym]
                            if i.get("incomeType") in ("REALIZED_PNL", "COMMISSION")
                               and i.get("tradeId") and str(i["tradeId"]) in block_trade_ids
                        ]
                        
                        if not incs_pnl_fee:
                            incs_pnl_fee = [
                                i for i in income_by_symbol[sym]
                                if i.get("incomeType") in ("REALIZED_PNL", "COMMISSION")
                                   and open_t <= i.get("time", 0) <= close_t
                            ]
                        
                        # 2) FUNDING_FEE: SIEMPRE por rango temporal (no tiene tradeId)
                        incs_funding = [
                            i for i in income_by_symbol[sym]
                            if i.get("incomeType") == "FUNDING_FEE"
                               and open_t <= i.get("time", 0) <= close_t
                        ]
                        
                        pnl     = sum(float(i["income"]) for i in incs_pnl_fee if i["incomeType"] == "REALIZED_PNL")
                        fees    = sum(float(i["income"]) for i in incs_pnl_fee if i["incomeType"] == "COMMISSION")
                        funding = sum(float(i["income"]) for i in incs_funding)
                        
                        if debug:
                            link_mode = "tradeId" if any(i.get("tradeId") for i in incs_pnl_fee) else "time-range"
                            print(f"    [BLOCK] {sym} side={side.upper()} (first={first_trade_side}, dom={dominance_side}) "
                                  f"size={size:.4f} entry={entry:.6f} close={close:.6f}")
                            print(f"      Buys={len(buys)}({long_qty:.4f}) Sells={len(sells)}({short_qty:.4f}) "
                                  f"open={_iso(open_t)} close={_iso(close_t)}")
                            print(f"      Income link: PnL/Fees={'tradeId' if any(i.get('tradeId') for i in incs_pnl_fee) else 'time-range'}, Funding=time-range")
                            print(f"      Totals → pnl={pnl:.6f} fee={fees:.6f} funding={funding:.6f}")
                        realized_total = pnl + fees + funding  # ✅ incluye todo

                        results.append({
                            "exchange": "binance",
                            "symbol": sym,
                            "side": side,
                            "size": size,
                            "entry_price": entry,
                            "close_price": close,
                            "notional": entry * size,
                            "fees": fees,                   # (normalmente negativas)
                            "funding_fee": funding,         # (+ cobro / - pago)
                            "pnl": pnl,            # SOLO precio
                            "realized_pnl": realized_total,
                            "open_time": int(open_t/1000),  # epoch s
                            "close_time": int(close_t/1000),
                            # por si tu frontend todavía usa strings:
                            "open_date":  datetime.fromtimestamp(open_t/1000).strftime("%Y-%m-%d %H:%M:%S"),
                            "close_date": datetime.fromtimestamp(close_t/1000).strftime("%Y-%m-%d %H:%M:%S"),
                        })

                        # reset bloque para el siguiente
                        block = []
                        net_qty = 0.0

                # 4.2 Guardar carry para la siguiente ventana
                carry_block_by_sym[sym] = block[:]    # copia
                carry_net_by_sym[sym]   = net_qty

            # fin ventanas

        if debug:
            print(f"\n✅ Binance closed positions totales: {len(results)}")
        return results

    except Exception as e:
        print(f"❌ Binance closed positions error: {e}")
        return []
    
    
def save_binance_closed_positions(db_path="portfolio.db", days=30, debug=False):
    """
    Guarda posiciones cerradas de Binance en SQLite.
    Dedupe por (symbol, close_time, size) para evitar falsos duplicados.
    """
    import sqlite3
    positions = fetch_closed_positions_binance(days=days, debug=debug)
    if not positions:
        print("⚠️ No se encontraron posiciones cerradas en Binance.")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    saved = 0
    skipped = 0

    for pos in positions:
        try:
            close_ts = pos.get("close_time")
            symbol   = pos["symbol"]
            size     = float(pos["size"])

            # dedupe por símbolo + close_time + size
            cur.execute("""
                SELECT COUNT(*) FROM closed_positions
                WHERE symbol = ? AND close_time = ? AND ABS(size - ?) < 1e-8
            """, (symbol, close_ts, size))
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
                symbol,
                pos["side"],
                size,
                pos["entry_price"],
                pos["close_price"],
                pos["open_time"],
                close_ts,
                pos["realized_pnl"],           # SOLO precio
                pos.get("funding_fee", 0.0),
                -abs(pos.get("fees", 0.0)),    # fees siempre negativas
                pos["notional"],
                None,
                None
            ))
            saved += 1
        except Exception as e:
            print(f"⚠️ Error guardando {symbol}: {e}")

    conn.commit()
    conn.close()
    print(f"✅ Guardadas {saved} posiciones cerradas de Binance (omitidas {skipped} duplicadas).")

## codigo anterior
# def fetch_closed_positions_binance(days=30, off=0, debug=False):
#     """
#     Reconstruye posiciones cerradas de Binance Futures para los últimos `days` días
#     usando /fapi/v1/userTrades (fills) e /fapi/v1/income (pnl/fees/funding).
#     """
#     import time, hmac, hashlib, requests, json
#     from urllib.parse import urlencode
#     from collections import defaultdict
#     from datetime import datetime, timezone

#     def _iso(ms):  # helper para logs bonitos
#         try:
#             return datetime.fromtimestamp(ms/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
#         except Exception:
#             return str(ms)

#     p = _p()
#     BINANCE_API_KEY    = p.BINANCE_API_KEY
#     BINANCE_API_SECRET = p.BINANCE_API_SECRET
#     BINANCE_BASE_URL   = p.BINANCE_BASE_URL
#     UA_HEADERS         = getattr(p, "UA_HEADERS", {})

#     def signed_get(path, params=None):
#         params = dict(params or {})
#         params["timestamp"] = int(time.time() * 1000) + int(off)
#         qs = urlencode(params, doseq=True)
#         sig = hmac.new(BINANCE_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
#         url = f"{BINANCE_BASE_URL}{path}?{qs}&signature={sig}"
#         headers = {"X-MBX-APIKEY": BINANCE_API_KEY, **UA_HEADERS}
#         if debug:
#             print(f"[GET] {path} {params}")
#         r = requests.get(url, headers=headers, timeout=25)
#         r.raise_for_status()
#         return r.json()

#     try:
#         now = int(time.time() * 1000)
#         start_time = now - days * 24 * 60 * 60 * 1000

#         # 1) INCOME (paginado por page)
#         income_by_symbol = defaultdict(list)
#         page = 1
#         total_income_items = 0
#         while True:
#             inc = signed_get("/fapi/v1/income", {
#                 "limit": 1000, "startTime": start_time, "endTime": now, "page": page
#             })
#             if not inc:
#                 break
#             got = 0
#             for i in inc:
#                 t = i.get("incomeType")
#                 sym = i.get("symbol") or ""
#                 if t in ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE") and sym:
#                     income_by_symbol[sym].append(i)
#                     got += 1
#             total_income_items += got
#             if len(inc) < 1000:
#                 break
#             page += 1

#         if debug:
#             print(f"[Income] total items={total_income_items}, symbols={list(income_by_symbol.keys())}")
#             # resumen por símbolo
#             for sym, arr in income_by_symbol.items():
#                 c_pnl = sum(1 for x in arr if x["incomeType"]=="REALIZED_PNL")
#                 c_fee = sum(1 for x in arr if x["incomeType"]=="COMMISSION")
#                 c_fnd = sum(1 for x in arr if x["incomeType"]=="FUNDING_FEE")
#                 s_pnl = sum(float(x["income"]) for x in arr if x["incomeType"]=="REALIZED_PNL")
#                 s_fee = sum(float(x["income"]) for x in arr if x["incomeType"]=="COMMISSION")
#                 s_fnd = sum(float(x["income"]) for x in arr if x["incomeType"]=="FUNDING_FEE")
#                 print(f"  {sym}: PNL {c_pnl}/{s_pnl:.2f} | FEE {c_fee}/{s_fee:.2f} | FND {c_fnd}/{s_fnd:.2f}")

#         # 2) exchangeInfo (por si quieres filtrar)
#         try:
#             exi = signed_get("/fapi/v1/exchangeInfo")
#             valid_symbols = {s["symbol"] for s in exi.get("symbols", [])}
#         except Exception:
#             valid_symbols = set(income_by_symbol.keys())

#         results = []

#         # 3) Recorrer símbolos y ventanas 7d
#         for sym in list(income_by_symbol.keys()):
#             if sym not in valid_symbols and debug:
#                 print(f"[SKIP] {sym} no en exchangeInfo")

#             end_time = start_time
#             if debug:
#                 print(f"\n[Symbol] {sym} —— ventanas de 7d desde {_iso(start_time)} a {_iso(now)}")
#             while end_time < now:
#                 chunk_start = end_time
#                 chunk_end = min(chunk_start + 7*24*60*60*1000, now)
#                 end_time = chunk_end

#                 if debug:
#                     print(f"  [Window] {_iso(chunk_start)} → {_iso(chunk_end)}")

#                 # Paginación de userTrades por fromId
#                 trades_all = []
#                 last_id = None
#                 pages = 0
#                 while True:
#                     params = {
#                         "symbol": sym,
#                         "startTime": chunk_start,
#                         "endTime": chunk_end,
#                         "limit": 1000,
#                     }
#                     if last_id is not None:
#                         params["fromId"] = last_id + 1
#                     tpage = []
#                     try:
#                         tpage = signed_get("/fapi/v1/userTrades", params)
#                     except Exception as e:
#                         if debug:
#                             print(f"    [WARN] userTrades fallo: {e}")
#                         break
#                     if not tpage:
#                         break
#                     trades_all.extend(tpage)
#                     pages += 1
#                     if len(tpage) < 1000:
#                         break
#                     last_id = int(tpage[-1]["id"])

#                 if debug:
#                     print(f"    trades: {len(trades_all)} (pages={pages})")

#                 if not trades_all:
#                     continue

#                 trades_all.sort(key=lambda x: x["time"])

#                 if debug and len(trades_all) <= 20:
#                     for tr in trades_all:
#                         print(f"      {sym} {tr['side']:>4} qty={tr['qty']} px={tr['price']} t={_iso(tr['time'])}")

#                 net_qty = 0.0
#                 block = []
#                 blocks_in_window = 0

#                 for t in trades_all:
#                     q = float(t["qty"])
#                     qty_signed = q if t["side"] == "BUY" else -q
#                     net_qty += qty_signed
#                     block.append(t)

#                     if abs(net_qty) < 1e-10:  # cerró bloque
#                         open_t = block[0]["time"]
#                         close_t = block[-1]["time"]
#                         buys  = [b for b in block if b["side"] == "BUY"]
#                         sells = [s for s in block if s["side"] == "SELL"]

#                         def avg_price(lst):
#                             qsum = sum(float(x["qty"]) for x in lst)
#                             nsum = sum(float(x["qty"]) * float(x["price"]) for x in lst)
#                             return nsum / qsum if qsum else 0.0

#                         long_qty  = sum(float(b["qty"]) for b in buys)
#                         short_qty = sum(float(s["qty"]) for s in sells)
#                         side = "long" if long_qty >= short_qty else "short"

#                         if side == "long":
#                             entry = avg_price(buys)
#                             close = avg_price(sells)
#                             size  = min(long_qty, short_qty)
#                         else:
#                             entry = avg_price(sells)
#                             close = avg_price(buys)
#                             size  = min(long_qty, short_qty)

#                         # income del rango del bloque
#                         incs = [i for i in income_by_symbol[sym] if open_t <= i["time"] <= close_t]
#                         pnl     = sum(float(i["income"]) for i in incs if i["incomeType"]=="REALIZED_PNL")
#                         fees    = sum(float(i["income"]) for i in incs if i["incomeType"]=="COMMISSION")
#                         funding = sum(float(i["income"]) for i in incs if i["incomeType"]=="FUNDING_FEE")

#                         if debug:
#                             print(f"    [BLOCK {blocks_in_window+1}] {sym} {side.upper()} "
#                                   f"size={size:.4f} entry={entry:.6f} close={close:.6f} "
#                                   f"open={_iso(open_t)} close={_iso(close_t)}")
#                             print(f"      Buys={len(buys)}({long_qty:.4f}) Sells={len(sells)}({short_qty:.4f})")
#                             print(f"      Income: pnl={pnl:.6f} fee={fees:.6f} funding={funding:.6f} items={len(incs)}")

#                         results.append({
#                             "exchange": "binance",
#                             "symbol": sym,
#                             "side": side,
#                             "size": size,
#                             "entry_price": entry,
#                             "close_price": close,
#                             "notional": entry * size,
#                             "fees": fees,                   # (negativas normalmente)
#                             "funding_fee": funding,         # (+ cobro / - pago)
#                             "realized_pnl": pnl,            # SOLO precio
#                             "open_date": datetime.fromtimestamp(open_t / 1000).strftime("%Y-%m-%d %H:%M"),
#                             "close_date": datetime.fromtimestamp(close_t / 1000).strftime("%Y-%m-%d %H:%M"),
#                         })

#                         blocks_in_window += 1
#                         block = []

#                 if debug and blocks_in_window == 0:
#                     # Si quedó bloque incompleto (posición abierta), lo mostramos
#                     if block:
#                         oq = sum(float(x["qty"]) for x in block if x["side"]=="BUY")
#                         sq = sum(float(x["qty"]) for x in block if x["side"]=="SELL")
#                         print(f"    [OPEN BLOCK] net_qty={net_qty:.6f} buys={oq:.4f} sells={sq:.4f} "
#                               f"desde {_iso(block[0]['time'])} hasta {_iso(block[-1]['time'])}")

#         if debug:
#             print(f"\n✅ Binance closed positions totales: {len(results)}")
#             # Muestra resumen final por símbolo/side
#             by_sym = defaultdict(list)
#             for r in results:
#                 by_sym[r["symbol"]].append(r)
#             for sym, arr in by_sym.items():
#                 print(f"  {sym}: {len(arr)} bloques")
#                 for k, r in enumerate(arr, 1):
#                     print(f"    [{k}] {r['side']} size={r['size']:.4f} "
#                           f"{r['entry_price']:.6f}->{r['close_price']:.6f} "
#                           f"PnL={r['realized_pnl']:.6f} Fee={r['fees']:.6f} Fnd={r['funding_fee']:.6f} "
#                           f"{r['open_date']} → {r['close_date']}")

#         return results

#     except Exception as e:
#         print(f"❌ Binance closed positions error: {e}")
#         return []



# def save_binance_closed_positions(db_path="portfolio.db", days=30, debug=False):
#     """
#     Guarda las posiciones cerradas de Binance en la base de datos.
#     Evita duplicados por (exchange, symbol, close_time).
#     Fees se guardan negativas (costo).
#     """
#     closed_positions = fetch_closed_positions_binance(days=days, debug=debug)
#     if not closed_positions:
#         print("⚠️ No se encontraron posiciones cerradas en Binance.")
#         return

#     conn = sqlite3.connect(db_path)
#     cur = conn.cursor()
#     saved = 0
#     skipped = 0

#     for pos in closed_positions:
#         try:
#             def to_ts(dt):
#                 return int(datetime.fromisoformat(dt).timestamp())

#             open_ts = to_ts(pos["open_date"])
#             close_ts = to_ts(pos["close_date"])

#             cur.execute("""
#                 SELECT COUNT(*) FROM closed_positions
#                 WHERE exchange = ? AND symbol = ? AND close_time = ?
#             """, (pos["exchange"], pos["symbol"], close_ts))
#             if cur.fetchone()[0]:
#                 skipped += 1
#                 continue

#             # Normalización: fee_total negativa (costo), funding tal cual (+/-), realized_pnl = precio
#             cur.execute("""
#                 INSERT INTO closed_positions (
#                     exchange, symbol, side, size, entry_price, close_price,
#                     open_time, close_time, realized_pnl, funding_total,
#                     fee_total, notional, leverage, liquidation_price
#                 )
#                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#             """, (
#                 pos["exchange"],
#                 pos["symbol"],
#                 pos["side"],
#                 pos["size"],
#                 pos["entry_price"],
#                 pos["close_price"],
#                 open_ts,
#                 close_ts,
#                 pos["realized_pnl"],             # SOLO precio
#                 pos.get("funding_fee", 0.0),     # funding (+ cobro / - pago)
#                 -abs(pos.get("fees", 0.0)),      # fees → siempre negativo (costo)
#                 pos["notional"],
#                 None,
#                 None
#             ))
#             saved += 1

#         except Exception as e:
#             print(f"⚠️ Error guardando {pos.get('symbol', '?')}: {e}")
#             continue

#     conn.commit()
#     conn.close()
#     print(f"✅ Guardadas {saved} posiciones cerradas de Binance (omitidas {skipped} duplicadas).")




#####################################################################
#no funciona bien, pero dejarla por el momento.
# def fetch_closed_positions_binance(days=30, off=0, debug=False):
#     """
#     Reconstruye posiciones cerradas de Binance Futures para los últimos 30 días
#     usando /fapi/v1/userTrades e /fapi/v1/income.
#     """
#     # A partir de aquí, puedes usar tus requests normales
#     # Por ejemplo:


#     p = _p()
#     BINANCE_API_KEY = p.BINANCE_API_KEY
#     BINANCE_API_SECRET = p.BINANCE_API_SECRET
#     BINANCE_BASE_URL = p.BINANCE_BASE_URL
#     UA_HEADERS = getattr(p, "UA_HEADERS", {})
    
#     def signed_get(path, params=None):
#         params = dict(params or {})
#         params["timestamp"] = int(time.time() * 1000)
#         qs = urlencode(params)
#         sig = hmac.new(BINANCE_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
#         url = f"{BINANCE_BASE_URL}{path}?{qs}&signature={sig}"
#         headers = {"X-MBX-APIKEY": BINANCE_API_KEY, **UA_HEADERS}
#         r = requests.get(url, headers=headers, timeout=20)
#         r.raise_for_status()
#         return r.json()

#     try:
#         now = int(time.time() * 1000)
#         start_time = now - days * 24 * 60 * 60 * 1000

#         # 1️⃣ Income global (para funding, PnL, fees)
#         income = signed_get("/fapi/v1/income", {"limit": 1000, "startTime": start_time, "endTime": now})
#         income_by_symbol = defaultdict(list)
#         for inc in income:
#             if inc["incomeType"] in ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE"):
#                 income_by_symbol[inc["symbol"]].append(inc)

#         symbols = list(income_by_symbol.keys())
#         results = []

#         # 2️⃣ Validar símbolos disponibles (evitar error 400)
#         try:
#             exchange_info = signed_get("/fapi/v1/exchangeInfo")
#             valid_symbols = {s["symbol"] for s in exchange_info.get("symbols", [])}
#         except Exception:
#             valid_symbols = set(symbols)

#         # 3️⃣ Iterar cada símbolo en ventanas de 7 días
#         for sym in symbols:
#             if sym not in valid_symbols:
#                 if debug:
#                     print(f"[SKIP] {sym} no está en Binance Futures.")
#                 continue

#             end_time = start_time
#             while end_time < now:
#                 chunk_start = end_time
#                 chunk_end = min(chunk_start + 7 * 24 * 60 * 60 * 1000, now)
#                 end_time = chunk_end

#                 try:
#                     trades = signed_get("/fapi/v1/userTrades", {
#                         "symbol": sym,
#                         "startTime": chunk_start,
#                         "endTime": chunk_end,
#                         "limit": 1000
#                     })
#                 except requests.HTTPError as e:
#                     if debug:
#                         print(f"[WARN] {sym} ventana {chunk_start}->{chunk_end} error {e}")
#                     continue

#                 if not trades:
#                     continue

#                 trades.sort(key=lambda x: x["time"])
#                 net_qty = 0.0
#                 block = []

#                 for t in trades:
#                     qty = float(t["qty"]) if t["side"] == "BUY" else -float(t["qty"])
#                     net_qty += qty
#                     block.append(t)

#                     if abs(net_qty) < 1e-8:  # posición cerrada
#                         open_t = block[0]["time"]
#                         close_t = block[-1]["time"]
#                         buys = [b for b in block if b["side"] == "BUY"]
#                         sells = [s for s in block if s["side"] == "SELL"]

#                         def avg_price(lst):
#                             q = sum(float(x["qty"]) for x in lst)
#                             n = sum(float(x["qty"]) * float(x["price"]) for x in lst)
#                             return n / q if q else 0.0

#                         entry = avg_price(buys)
#                         close = avg_price(sells)
#                         size = sum(float(b["qty"]) for b in buys)

#                         # 4️⃣ Income asociado
#                         incs = [i for i in income_by_symbol[sym] if open_t <= i["time"] <= close_t]
#                         pnl = sum(float(i["income"]) for i in incs if i["incomeType"] == "REALIZED_PNL")
#                         fees = sum(float(i["income"]) for i in incs if i["incomeType"] == "COMMISSION")
#                         funding = sum(float(i["income"]) for i in incs if i["incomeType"] == "FUNDING_FEE")
                        
#                         if debug:
#                             print(f"✅ {sym}: "
#                                   f"entry={entry:.4f}, close={close:.4f}, size={size:.4f}, "
#                                   f"pnl={pnl:.4f}, fees={fees:.4f}, funding={funding:.4f}, "
#                                   f"realized_total={pnl + funding + fees:.4f}")


#                         results.append({
#                             "exchange": "binance",
#                             "symbol": sym,
#                             "side": "closed",
#                             "size": size,
#                             "entry_price": entry,
#                             "close_price": close,
#                             "notional": entry * size,
#                             "fees": fees,
#                             "funding_fee": funding,
#                             "realized_pnl": pnl,
#                             "open_date": datetime.fromtimestamp(open_t / 1000).strftime("%Y-%m-%d %H:%M"),
#                             "close_date": datetime.fromtimestamp(close_t / 1000).strftime("%Y-%m-%d %H:%M"),
                     

#                         })
#                         block = []

#         print(f"✅ Binance closed positions reconstruidas: {len(results)} en {days} días")
#         return results

#     except Exception as e:
#         print(f"❌ Binance closed positions error: {e}")
#         return []

# funciona pero dejarla por ahora
# def save_binance_closed_positions(db_path="portfolio.db", days=30):
#     """
#     Guarda las posiciones cerradas de Binance en la base de datos.
#     """
#     closed_positions = fetch_closed_positions_binance(days=days)
#     if not closed_positions:
#         print("⚠️ No se encontraron posiciones cerradas en Binance.")
#         return

#     conn = sqlite3.connect(db_path)
#     cur = conn.cursor()
#     saved = 0
#     skipped = 0

#     for pos in closed_positions:
#         try:
#             def to_ts(dt):
#                 return int(datetime.fromisoformat(dt).timestamp())

#             open_ts = to_ts(pos["open_date"])
#             close_ts = to_ts(pos["close_date"])

#             cur.execute("""
#                 SELECT COUNT(*) FROM closed_positions
#                 WHERE exchange = ? AND symbol = ? AND close_time = ?
#             """, (pos["exchange"], pos["symbol"], close_ts))
#             if cur.fetchone()[0]:
#                 skipped += 1
#                 continue

#             cur.execute("""
#                 INSERT INTO closed_positions (
#                     exchange, symbol, side, size, entry_price, close_price,
#                     open_time, close_time, realized_pnl, funding_total,
#                     fee_total, notional, leverage, liquidation_price
#                 )
#                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#             """, (
#                 pos["exchange"],
#                 pos["symbol"],
#                 pos["side"],
#                 pos["size"],
#                 pos["entry_price"],
#                 pos["close_price"],
#                 open_ts,
#                 close_ts,
#                 pos["realized_pnl"],
#                 pos.get("funding_fee", 0.0),
#                 -abs(pos.get("fees", 0.0)),
#                 pos["notional"],
#                 None,
#                 None
#             ))
#             saved += 1
#         except Exception as e:
#             print(f"⚠️ Error guardando {pos['symbol']}: {e}")
#             continue

#     conn.commit()
#     conn.close()
#     print(f"✅ Guardadas {saved} posiciones cerradas de Binance (omitidas {skipped} duplicadas).")

#------------ Asterconfig------------

# --- ASTER: reconstrucción de posiciones cerradas ----------------------------
def fetch_closed_positions_aster(days=30, limit=1000, debug=False):
    """
    Reconstruye posiciones cerradas de Aster Futures con ventanas de 7 días.
    - Descarga trades en bloques de 7 días hasta cubrir `days`.
    - Cierra bloque cuando el neto vuelve a 0.
    - Calcula entry/close, fees, realized PnL y funding fees.
    """
    from datetime import datetime, timedelta
    import time

    try:
        p = _p()
        aster_signed_request = p.aster_signed_request
        _normalize_symbol = p._normalize_symbol
        fetch_funding_aster = p.fetch_funding_aster
    except Exception as e:
        print(f"⚠️ No se pudo cargar el módulo {portfolio}: {e}")
        return []

    # --- 1️⃣ Símbolos activos detectados por funding ---
    funding_all = fetch_funding_aster(limit=500) or []
    symbols = sorted({_normalize_symbol(f["symbol"]) for f in funding_all if f.get("symbol")})
    if not symbols:
        print("[Aster] No se detectaron símbolos para reconstruir.")
        return []

    if debug:
        print(f"[Aster] Símbolos detectados: {symbols}")

    results = []
    now = datetime.utcnow()
    start = now - timedelta(days=days)
    
    # --- 2️⃣ Descargar trades en bloques de 7 días (igual que Binance) ---
    for sym in symbols:
        all_trades = []
        cursor = start

        while cursor < now:
            chunk_start = cursor
            chunk_end = min(cursor + timedelta(days=7), now)
            
            params = {
                "symbol": sym,
                "limit": limit,
                "startTime": int(chunk_start.timestamp() * 1000),
                "endTime": int(chunk_end.timestamp() * 1000),
            }

            try:
                data = aster_signed_request("/fapi/v1/userTrades", params=params)
                items = data if isinstance(data, list) else (data.get("data") or [])
                if items:
                    all_trades.extend(items)
                    if debug:
                        print(f"[Aster] {sym}: {len(items)} trades ({chunk_start:%Y-%m-%d} → {chunk_end:%Y-%m-%d})")
            except Exception as e:
                if debug:
                    print(f"[Aster] Error fetching {sym} en {chunk_start:%Y-%m-%d}: {e}")
            
            cursor = chunk_end
            time.sleep(0.2)  # Rate limiting

        if not all_trades:
            if debug:
                print(f"[Aster] {sym}: sin trades en {days} días.")
            continue

        # --- 3️⃣ Normalizar estructura de trades ---
        normalized = []
        for t in all_trades:
            try:
                side = (t.get("side") or "").upper()
                qty = float(t.get("qty") or t.get("quantity") or 0)
                price = float(t.get("price") or 0)
                commission = abs(float(t.get("commission", 0)))
                realized = float(t.get("realizedPnl") or 0)
                ts = int(t.get("time", 0))
                signed = qty if side == "BUY" else -qty
                normalized.append({
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

        if not normalized:
            continue

        normalized.sort(key=lambda x: x["ts"])

        # --- 4️⃣ Funding fees del símbolo ---
        fnd = [f for f in funding_all if f["symbol"] == sym]

        # --- 5️⃣ Reconstrucción de posiciones cerradas (net = 0) ---
        net = 0.0
        block = []

        for t in normalized:
            net += t["signed"]
            block.append(t)

            # Cierre cuando net vuelve a 0
            if abs(net) < 1e-9 and block:
                buys = [x for x in block if x["signed"] > 0]
                sells = [x for x in block if x["signed"] < 0]
                if not buys or not sells:
                    block, net = [], 0.0
                    continue

                buy_qty = sum(x["qty"] for x in buys)
                sell_qty = sum(x["qty"] for x in sells)
                avg_buy = sum(x["qty"] * x["price"] for x in buys) / buy_qty
                avg_sell = sum(x["qty"] * x["price"] for x in sells) / sell_qty

                is_short = block[0]["signed"] < 0
                side = "short" if is_short else "long"

                entry_avg = avg_sell if is_short else avg_buy
                close_avg = avg_buy if is_short else avg_sell
                size = min(buy_qty, sell_qty)
                fees = sum(x["fee"] for x in block)
                pnl_trades = sum(x["realized"] for x in block)
                open_ts = min(x["ts"] for x in block)
                close_ts = max(x["ts"] for x in block)

                # Funding durante el rango
                funding_fee = sum(
                    f["income"] for f in fnd
                    if f.get("timestamp") and open_ts <= f["timestamp"] <= close_ts
                )

                total_pnl = pnl_trades - fees + funding_fee

                results.append({
                    "exchange": "aster",
                    "symbol": sym,
                    "side": side,
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
                    print(f"  ✅ [{sym}] {side.upper()} size={size:.4f} "
                          f"entry={entry_avg:.4f} close={close_avg:.4f} "
                          f"pnl={total_pnl:.4f} fees={fees:.4f} funding={funding_fee:.4f}")

                # Reset para siguiente bloque
                block, net = [], 0.0

    if debug:
        print(f"[Aster] Total símbolos procesados: {len(symbols)}, posiciones cerradas: {len(results)}")

    return results


def save_aster_closed_positions(db_path="portfolio.db", days=30, debug=False):
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
# def _bingx_fetch_funding(income_type="FUNDING_FEE", start_ms=None, end_ms=None, limit=1000, debug=False):
#     """
#     Devuelve lista de registros de funding:
#     [{symbol: 'BTCUSDT', income: float, timestamp: ms}, ...]
#     """
#     params = {
#         "incomeType": income_type,
#         "limit": limit,
#     }
#     if start_ms is not None:
#         params["startTime"] = int(start_ms)
#     if end_ms is not None:
#         params["endTime"] = int(end_ms)

#     payload = _get("/openApi/swap/v2/user/income", params)
#     data = payload.get("data") if isinstance(payload, dict) else payload
#     if not isinstance(data, list):
#         return []
#     out = []
#     for it in data:
#         try:
#             sym = (it.get("symbol") or "").upper().replace("-", "")
#             income = _num(it.get("income"), 0.0)
#             ts = int(it.get("timestamp") or it.get("time") or 0)
#             out.append({"symbol": sym, "income": income, "timestamp": ts})
#         except Exception:
#             continue
#     if debug:
#         print(f"✅ BingX funding: {len(out)} registros")
#     return out


# # ---------- Core: traer positions cerradas y guardar ----------
# def fetch_closed_positions_bingx(symbols, days=30, include_funding=True, page_size=200, debug=False):
#     """
#     Descarga posiciones cerradas para una lista de símbolos (requerido por la API).
#     - symbols: iterable de strings, p.ej. ["MYX-USDT", "KAITO-USDT"] o ["MYXUSDT", ...]
#     - days: rango [now - days, now] en ms
#     - include_funding: si True, suma FUNDING_FEE del rango para cada posición
#     Devuelve lista de dicts listos para guardar.
#     """
#     if not symbols:
#         print("⚠️ [BingX] 'symbols' vacío: la API exige símbolo. Pasa al menos uno.")
#         return []

#     now_ms = int(_time.time() * 1000)
#     start_ms = now_ms - days * 24 * 60 * 60 * 1000

#     # Funding opcional (indexado por símbolo sin guion)
#     funding_idx = {}
#     if include_funding:
#         f_all = _bingx_fetch_funding(start_ms=start_ms, end_ms=now_ms, debug=debug)
#         from collections import defaultdict as _dd
#         tmp = _dd(list)
#         for f in f_all:
#             tmp[f["symbol"]].append(f)
#         funding_idx = dict(tmp)

#     results = []

#     for sym in symbols:
#         sym_dash = _bx_to_dash(sym)
#         sym_nodash = _bx_no_dash(sym_dash)
#         page = 1
#         total_rows = 0

#         while True:
#             params = {
#                 "symbol": sym_dash,               # ⚠️ requerido
#                 "startTs": int(start_ms),      # ⚠️ nombres exactos según doc
#                 "endTs": int(now_ms),
#                 "pageId": page,                   # doc: pageId / pageIndex
#                 "pageSize": int(page_size),
#                 "recvWindow": 5000,
#             }

#             if debug:
#                 print(f"[BingX] positionHistory {sym_dash} page={page}")

#             payload = _get("/openApi/swap/v1/trade/positionHistory", params)
            
#             # Extraer correctamente la lista
#             data = []
#             if isinstance(payload, dict):
#                 if isinstance(payload.get("data"), dict):
#                     data = payload["data"].get("positionHistory", [])
#                 elif isinstance(payload.get("data"), list):
#                     data = payload["data"]
#                 else:
#                     data = []
#             else:
#                 data = payload or []
            
#             if not data:
#                 if debug:
#                     print(f"[BingX] {sym_dash} page={page}: 0 filas (estructura data vacía)")
#                 break


#             for row in data:
#                 try:
#                     open_ms = int(row.get("openTime") or 0)
#                     close_ms = int(row.get("updateTime") or 0)
#                     entry_price = float(row.get("avgPrice", 0))
#                     close_price = float(row.get("avgClosePrice", 0))
#                     qty = abs(float(row.get("closePositionAmt") or row.get("positionAmt") or 0))
#                     realized_pnl = float(row.get("realisedProfit") or 0)
#                     funding_total = float(row.get("totalFunding") or 0)
#                     fee_total = float(row.get("positionCommission") or 0)
#                     lev = float(row.get("leverage") or 0)
#                     side = (row.get("positionSide") or "").lower()
            
#                     results.append({
#                         "exchange": "bingx",
#                         "symbol": row["symbol"].replace("-", ""),
#                         "side": side or "closed",
#                         "size": qty,
#                         "entry_price": entry_price,
#                         "close_price": close_price,
#                         "open_time": int(open_ms / 1000),
#                         "close_time": int(close_ms / 1000),
#                         "realized_pnl": realized_pnl,
#                         "funding_total": funding_total,
#                         "fee_total": fee_total,
#                         "notional": entry_price * qty,
#                         "leverage": lev,
#                         "liquidation_price": None,
#                     })
            
#                     if debug:
#                         print(f"  ✅ {row['symbol']} side={side} size={qty} entry={entry_price:.4f} "
#                               f"close={close_price:.4f} pnl={realized_pnl:.4f} "
#                               f"funding={funding_total:.4f} fee={fee_total:.4f}")
            
#                 except Exception as e:
#                     if debug:
#                         print(f"[WARN] fila malformada {row}: {e}")
#                     continue

#             if len(data) < page_size:
#                 break
#             page += 1

#         if debug:
#             print(f"[BingX] {sym_dash} total filas: {total_rows}")

#     if debug:
#         print(f"✅ BingX closed positions totales: {len(results)}")
#     return results


# def save_bingx_closed_positions(db_path="portfolio.db", symbols=None, days=30, include_funding=True, debug=False):
#     """
#     Guarda posiciones cerradas de BingX en SQLite, evitando duplicados por (exchange, symbol, close_time).
#     - symbols: lista de símbolos a consultar (p.ej. ["MYX-USDT", "KAITO-USDT"]).
#                 La API exige símbolo: si no pasas nada, no se consulta.
#     """
#     if not os.path.exists(db_path):
#         print(f"❌ Database not found: {db_path}")
#         return

#     if not symbols:
#         print("⚠️ No se pasó 'symbols' a save_bingx_closed_positions. La API requiere símbolo. Nada que hacer.")
#         return

#     positions = fetch_closed_positions_bingx(
#         symbols=symbols,
#         days=days,
#         include_funding=include_funding,
#         debug=debug
#     )
#     if not positions:
#         print("⚠️ No se obtuvieron posiciones cerradas de BingX.")
#         return

#     conn = sqlite3.connect(db_path)
#     cur = conn.cursor()
#     saved = 0
#     skipped = 0

#     for pos in positions:
#         try:
#             # deduplicación por (exchange, symbol, close_time)
#             cur.execute("""
#                 SELECT COUNT(*) FROM closed_positions
#                 WHERE exchange = ? AND symbol = ? AND close_time = ?
#             """, (pos["exchange"], pos["symbol"], pos["close_time"]))
#             if cur.fetchone()[0]:
#                 skipped += 1
#                 continue

#             # usa tu helper centralizado que ya normaliza fees a negativas
#             save_closed_position(pos)
#             saved += 1

#         except Exception as e:
#             print(f"⚠️ Error guardando {pos.get('symbol')} (BingX): {e}")

#     conn.close()
#     print(f"✅ BingX guardadas: {saved} | omitidas (duplicadas): {skipped}")
    
    


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






