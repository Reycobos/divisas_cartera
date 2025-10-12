import sqlite3
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+

# ==== CONFIG ====
DB_PATH = r"C:\ruta\hasta\portfolio.db"   # cámbialo si hace falta
TABLE = "closed_positions"                # tu tabla de posiciones cerradas
TZ = ZoneInfo("Europe/Madrid")           # ajusta si usas otra zona

# ==== DATOS DE LA OPERACIÓN (dos patas) ====
symbol = "WCT"
size = 56926.0

# Fechas
open_dt  = datetime(2025, 9, 10, 7, 32, 0, tzinfo=TZ)
close_dt = datetime(2025, 9, 12, 15, 36, 0, tzinfo=TZ)
open_ts  = int(open_dt.timestamp())      # usa *1000 si tu app guarda ms
close_ts = int(close_dt.timestamp())

# Valores por pata
long_leg = {
    "exchange": "Dexari",
    "symbol": symbol,
    "side": "LONG",
    "size": size,
    "entry_price": 0.0,
    "close_price": 0.0,
    "open_time": open_ts,
    "close_time": close_ts,
    "realized_pnl": -63.04,   # PnL sin funding
    "funding_total": -55.01,  # funding de la pata long
    "fee_total": 36.50,       # tus fees estaban en la columna del long
    "notional": 10000.0,      # si existe la columna
    "leverage": None,
    "liquidation_price": None
}

short_leg = {
    "exchange": "Paradex",
    "symbol": symbol,
    "side": "SHORT",
    "size": size,
    "entry_price": 0.0,
    "close_price": 0.0,
    "open_time": open_ts,
    "close_time": close_ts,
    "realized_pnl": 13.47,    # PnL sin funding
    "funding_total": 208.64,  # funding de la pata short
    "fee_total": 0.0,         # asumo 0 en short porque tu hoja no mostraba fees ahí
    "notional": 5000.0,       # si existe la columna
    "leverage": None,
    "liquidation_price": None
}

# Genera una etiqueta común por si tu tabla tiene algo tipo group_key/pair_id
group_value = str(uuid.uuid4())
CANDIDATE_GROUP_COLS = {"group_key", "pair_id", "bundle_id", "hedge_id"}

def get_table_cols(conn, table):
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}

def filtered_payload(payload, table_cols):
    data = {k: v for k, v in payload.items() if k in table_cols}
    # añade group_key si alguna columna candidata existe
    for col in CANDIDATE_GROUP_COLS:
        if col in table_cols:
            data[col] = group_value
            break
    return data

def insert_row(conn, table, row_dict):
    cols = ", ".join(row_dict.keys())
    qs   = ", ".join([f":{k}" for k in row_dict.keys()])
    sql  = f"INSERT INTO {table} ({cols}) VALUES ({qs})"
    cur = conn.cursor()
    cur.execute(sql, row_dict)
    conn.commit()
    return cur.lastrowid

with sqlite3.connect(DB_PATH) as conn:
    table_cols = get_table_cols(conn, TABLE)
    if not table_cols:
        raise RuntimeError(f"No encuentro columnas en {TABLE}. ¿Nombre de tabla correcto?")

    long_row  = filtered_payload(long_leg,  table_cols)
    short_row = filtered_payload(short_leg, table_cols)

    # Si tu tabla usa milisegundos:
    # if "open_time" in table_cols and "close_time" in table_cols:
    #     long_row["open_time"]  = int(open_dt.timestamp() * 1000)
    #     long_row["close_time"] = int(close_dt.timestamp() * 1000)
    #     short_row["open_time"]  = long_row["open_time"]
    #     short_row["close_time"] = long_row["close_time"]

    id_long  = insert_row(conn, TABLE, long_row)
    id_short = insert_row(conn, TABLE, short_row)

    print("Insertadas dos patas:")
    print("LONG  -> id:", id_long,  long_row)
    print("SHORT -> id:", id_short, short_row)
