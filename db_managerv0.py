# db_manager.py
import sqlite3
from collections import defaultdict
import statistics


DB_PATH = "portfolio.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Tabla principal de posiciones cerradas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS closed_positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        exchange TEXT,
        symbol TEXT,
        side TEXT,
        size REAL,
        entry_price REAL,
        close_price REAL,
        open_time INTEGER,
        close_time INTEGER,
        realized_pnl REAL,
        funding_total REAL,
        fee_total REAL,
        notional REAL,
        leverage REAL,
        liquidation_price REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Opcional: almacenar funding individual si quieres análisis por hora
    cur.execute("""
    CREATE TABLE IF NOT EXISTS funding_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        exchange TEXT,
        symbol TEXT,
        income REAL,
        timestamp INTEGER
    )
    """)

    conn.commit()
    conn.close()
def save_closed_position(position):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # 🔧 Normalizar fees: siempre negativas
    fee_total = -abs(position.get("fee_total", 0))
    position["fee_total"] = fee_total
    
    cur.execute("""
        INSERT INTO closed_positions (
            exchange, symbol, side, size, entry_price, close_price,
            open_time, close_time, realized_pnl, funding_total, fee_total,
            notional, leverage, liquidation_price
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        position.get("exchange"),
        position.get("symbol"),
        position.get("side"),
        position.get("size"),
        position.get("entry_price"),
        position.get("close_price"),
        position.get("open_time"),
        position.get("close_time"),
        position.get("realized_pnl"),
        position.get("funding_total"),
        position.get("fee_total"),
        position.get("notional"),
        position.get("leverage"),
        position.get("liquidation_price"),
    ))
    conn.commit()
    conn.close()

from collections import defaultdict
import statistics

def build_positions_from_trades(trades):
    """
    Agrupa trades por símbolo y determina aperturas/cierres.
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

            # detectar apertura
            if qty_net == 0:
                open_time = t["time"]
                entry_prices = [price]

            qty_net += qty

            # si la posición se cierra (vuelve a 0)
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
                    "exchange": t.get("exchange", "unknown"),
                    "symbol": sym,
                    "side": "long" if qty > 0 else "short",
                    "size": abs(qty),
                    "entry_price": entry_price,
                    "close_price": close_price,
                    "open_time": open_time,
                    "close_time": close_time,
                    "realized_pnl": realized_pnl,
                    "fee_total": -fees_total,  # negativo (costo)
                })

                # reset
                qty_net = 0.0
                fees_total = 0.0
                entry_prices = []
            else:
                entry_prices.append(price)

    return positions

def attach_funding_to_positions(positions, funding):
    """
    funding: lista de dicts con symbol, income, timestamp
    """
    for pos in positions:
        sym_funding = [
            f for f in funding
            if f["symbol"] == pos["symbol"]
            and pos["open_time"] <= f["timestamp"] <= pos["close_time"]
        ]
        pos["funding_total"] = sum(f["income"] for f in sym_funding)
    return positions

def process_closed_positions(exchange_name, trades, funding):
    positions = build_positions_from_trades(trades)
    positions = attach_funding_to_positions(positions, funding)

    for pos in positions:
        pos["exchange"] = exchange_name
        save_closed_position(pos)

    print(f"✅ Guardadas {len(positions)} posiciones cerradas de {exchange_name}.")



