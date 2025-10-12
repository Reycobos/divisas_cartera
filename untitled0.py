from datetime import datetime, timedelta
import time
from portfoliov2 import aster_signed_request


def fetch_all_user_trades_aster(symbol, days_back=30, limit=1000, debug=True):
    """
    Descarga todo el historial de trades de Aster para un símbolo,
    dividiendo en ventanas de ≤7 días (la API no acepta rangos mayores).
    """
    now = datetime.utcnow()
    end = now
    start = now - timedelta(days=days_back)
    all_trades = []
    total = 0

    print(f"🕒 Descargando historial de {symbol} desde {start.date()} hasta {end.date()}...")

    while start < end:
        # rango de 6 días 23 h → evita el 400 Bad Request
        end_window = start + timedelta(days=6, hours=23)
        if end_window > end:
            end_window = end

        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end_window.timestamp() * 1000)

        params = {
            "symbol": symbol,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit
        }

        try:
            data = aster_signed_request("/fapi/v1/userTrades", params=params)
            if isinstance(data, list) and len(data) > 0:
                all_trades.extend(data)
                total += len(data)
                if debug:
                    print(f"✅ {start.date()} → {end_window.date()} : {len(data)} trades")
            else:
                if debug:
                    print(f"⚠️ {start.date()} → {end_window.date()} : sin datos")

        except Exception as e:
            print(f"❌ Error en rango {start.date()} → {end_window.date()}: {e}")

        # avanzar un día después del último rango
        start = end_window + timedelta(days=1)

        # prevenir rate-limit
        time.sleep(0.6)

    print(f"📊 Total descargado: {total} trades ({symbol})")
    return all_trades
def save_all_user_trades_aster_to_db(symbol, days_back=90, db_path="portfolio.db", debug=True):
    """
    Descarga todo el historial de trades de Aster para un símbolo (en ventanas de 7 días)
    y guarda las posiciones cerradas en portfolio.db.
    """
    import sqlite3
    from datetime import datetime
    from portfoliov1_9 import fetch_funding_aster
    from trades_processing import fetch_all_user_trades_aster, save_closed_position

    # 1️⃣ Descargar todos los trades antiguos
    trades = fetch_all_user_trades_aster(symbol, days_back=days_back, debug=debug)
    if not trades:
        print(f"⚠️ No se encontraron trades históricos para {symbol}.")
        return

    # 2️⃣ Normalizar trades
    normalized = []
    for t in trades:
        try:
            side = (t.get("side") or "").upper()
            qty = float(t.get("qty") or 0)
            price = float(t.get("price") or 0)
            fee = abs(float(t.get("commission", 0)))
            realized = float(t.get("realizedPnl") or 0)
            ts = int(t.get("time", 0))
            signed = qty if side == "BUY" else -qty
            normalized.append({
                "symbol": t["symbol"],
                "side": side,
                "qty": qty,
                "signed": signed,
                "price": price,
                "fee": fee,
                "realized": realized,
                "ts": ts
            })
        except Exception as e:
            if debug:
                print(f"[WARN] Trade malformado: {e}")
            continue

    normalized.sort(key=lambda x: x["ts"])
    if not normalized:
        print(f"⚠️ No se pudieron normalizar trades de {symbol}.")
        return

    # 3️⃣ Funding del mismo rango
    funding_all = fetch_funding_aster(limit=1000)
    funding_symbol = [f for f in funding_all if f["symbol"] == symbol]

    # 4️⃣ Agrupar en bloques cerrados (net=0)
    results = []
    net = 0.0
    block = []

    for t in normalized:
        net += t["signed"]
        block.append(t)

        if abs(net) < 1e-9:
            buys = [x for x in block if x["signed"] > 0]
            sells = [x for x in block if x["signed"] < 0]
            if not buys or not sells:
                block, net = [], 0.0
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
                f["income"] for f in funding_symbol
                if f.get("timestamp") and open_ts <= f["timestamp"] <= close_ts
            )

            total_pnl = pnl_trades - fees + funding_fee

            results.append({
                "exchange": "aster",
                "symbol": symbol,
                "side": "closed",
                "size": size,
                "entry_price": entry_avg,
                "close_price": close_avg,
                "notional": entry_avg * size,
                "fees": fees,
                "funding_fee": funding_fee,
                "realized_pnl": total_pnl,
                "open_date": datetime.fromtimestamp(open_ts / 1000).strftime("%Y-%m-%d %H:%M"),
                "close_date": datetime.fromtimestamp(close_ts / 1000).strftime("%Y-%m-%d %H:%M")
            })

            if debug:
                print(f"✅ {symbol} BLOCK: size={size:.2f} entry={entry_avg:.4f} close={close_avg:.4f} "
                      f"pnl={total_pnl:.4f} fees={fees:.4f} funding={funding_fee:.4f}")

            block, net = [], 0.0

    if not results:
        print(f"⚠️ No se detectaron posiciones cerradas en {symbol}.")
        return

    # 5️⃣ Guardar en base de datos
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    saved, skipped = 0, 0

    for pos in results:
        try:
            open_ts = int(datetime.fromisoformat(pos["open_date"]).timestamp())
            close_ts = int(datetime.fromisoformat(pos["close_date"]).timestamp())
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
                "funding_total": pos["funding_fee"],
                "fee_total": pos["fees"],
                "notional": pos["notional"],
                "leverage": None,
                "liquidation_price": None
            })
            saved += 1

        except Exception as e:
            print(f"⚠️ Error guardando posición {pos['symbol']}: {e}")
            continue

    conn.close()
    print(f"✅ Guardadas {saved} posiciones cerradas de {symbol} (omitidas {skipped} duplicadas).")



# from portfoliov2 import aster_signed_request
# import time
# from datetime import datetime

# # Ejemplo: 18-sep a 25-sep
# start = datetime(2025, 9, 19)
# end   = datetime(2025, 9, 25)
# params = {
#     "symbol": "AVNTUSDT",
#     "startTime": int(start.timestamp()*1000),
#     "endTime": int(end.timestamp()*1000),
#     "limit": 1000
# }
# data = aster_signed_request("/fapi/v1/userTrades", params=params)
# print(len(data), "trades encontrados")
# print(data[:2])
