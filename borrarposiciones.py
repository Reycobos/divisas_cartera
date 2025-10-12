import sqlite3

DB_PATH = "portfolio.db"   # <-- cámbialo si tu DB está en otra ruta

import sqlite3
import pandas as pd
from tabulate import tabulate
import numpy as np



# =========================
# ⚙️ TOGGLES DE EJECUCIÓN
# =========================
RUN_SYMBOL_MODE = False     # Ejecuta las funciones por símbolo
RUN_EXCHANGE_MODE = True  # Ejecuta las funciones de "todas las posiciones" por exchange







#========Funciones Wipe

def _wipe_symbol(exchange_key: str, display_name: str, symbol: str, db_path: str = DB_PATH):
    """
    Elimina todas las posiciones cerradas de un símbolo específico para un exchange dado.
    - exchange_key: valor exacto guardado en la columna 'exchange' (p. ej. 'binance', 'kucoin', 'mexc').
    - display_name: cómo se mostrará en consola (p. ej. 'Binance', 'KuCoin', 'MEXC').
    """
    print(f"🧹 Limpiando posiciones cerradas de {display_name}...")
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("""
                DELETE FROM closed_positions
                WHERE exchange = ? AND symbol = ?
            """, (exchange_key, symbol))
            deleted = cur.rowcount
            conn.commit()
        print(f"✨ Limpieza de {display_name} completada. ({deleted} filas)")
    except Exception as e:
        print(f"❌ Error al limpiar {display_name} (símbolo {symbol}): {e}")


def _wipe_all(exchange_key: str, display_name: str, db_path: str = DB_PATH):
    """
    Elimina todas las posiciones cerradas de un exchange (sin filtrar por símbolo).
    """
    print(f"🧹 Limpiando posiciones cerradas de {display_name}...")
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("""
                DELETE FROM closed_positions
                WHERE exchange = ?
            """, (exchange_key,))
            deleted = cur.rowcount
            conn.commit()
        print(f"✨ Limpieza de {display_name} completada. ({deleted} filas)")
    except Exception as e:
        print(f"❌ Error al limpiar {display_name}: {e}")


# =========================
# Wrappers por EXCHANGE
# =========================
# --- Por símbolo ---
def wipe_binance_symbol(symbol: str, db_path: str = DB_PATH):
    _wipe_symbol("binance", "Binance", symbol, db_path)

def wipe_backpack_symbol(symbol: str, db_path: str = DB_PATH):
    _wipe_symbol("backpack", "Backpack", symbol, db_path)

def wipe_aster_symbol(symbol: str, db_path: str = DB_PATH):
    _wipe_symbol("aster", "Aster", symbol, db_path)

def wipe_extended_symbol(symbol: str, db_path: str = DB_PATH):
    _wipe_symbol("extended", "Extended", symbol, db_path)

def wipe_kucoin_symbol(symbol: str, db_path: str = DB_PATH):
    _wipe_symbol("kucoin", "KuCoin", symbol, db_path)

def wipe_gate_symbol(symbol: str, db_path: str = DB_PATH):
    _wipe_symbol("gate", "Gate.io", symbol, db_path)

def wipe_bingx_symbol(symbol: str, db_path: str = DB_PATH):
    _wipe_symbol("bingx", "BingX", symbol, db_path)

def wipe_bitget_symbol(symbol: str, db_path: str = DB_PATH):
    _wipe_symbol("bitget", "Bitget", symbol, db_path)

def wipe_mexc_symbol(symbol: str, db_path: str = DB_PATH):
    _wipe_symbol("mexc", "MEXC", symbol, db_path)


# --- Todas las posiciones por exchange ---
def wipe_binance_all(db_path: str = DB_PATH):
    _wipe_all("binance", "Binance", db_path)

def wipe_backpack_all(db_path: str = DB_PATH):
    _wipe_all("backpack", "Backpack", db_path)

def wipe_aster_all(db_path: str = DB_PATH):
    _wipe_all("aster", "Aster", db_path)

def wipe_extended_all(db_path: str = DB_PATH):
    _wipe_all("extended", "Extended", db_path)

def wipe_kucoin_all(db_path: str = DB_PATH):
    _wipe_all("kucoin", "KuCoin", db_path)

def wipe_gate_all(db_path: str = DB_PATH):
    _wipe_all("gate", "Gate", db_path)

def wipe_bingx_all(db_path: str = DB_PATH):
    _wipe_all("bingx", "BingX", db_path)

def wipe_bitget_all(db_path: str = DB_PATH):
    _wipe_all("bitget", "Bitget", db_path)

def wipe_mexc_all(db_path: str = DB_PATH):
    _wipe_all("mexc", "MEXC", db_path)


if __name__ == "__main__":
    # Valida toggles
    if RUN_SYMBOL_MODE and RUN_EXCHANGE_MODE:
        print("⚠️ Configuración inválida: activa solo uno de RUN_SYMBOL_MODE o RUN_EXCHANGE_MODE.")
    elif not RUN_SYMBOL_MODE and not RUN_EXCHANGE_MODE:
        print("⚠️ No hay modo activo: activa RUN_SYMBOL_MODE o RUN_EXCHANGE_MODE.")
    elif RUN_SYMBOL_MODE:
        # =========================
        # MODO POR SÍMBOLO (ejemplos)
        # Descomenta las que necesites
        # =========================
        # wipe_binance_symbol("XPLUSDT")
        # wipe_kucoin_symbol("VOXEL")
        # wipe_mexc_symbol("GIGGLE")
        # wipe_gate_symbol("VOXELUSDT")
        pass
    elif RUN_EXCHANGE_MODE:
        # =========================
        # MODO POR EXCHANGE (todas las posiciones)
        # Descomenta las que necesites
        # =========================
        # wipe_binance_all()
        # wipe_kucoin_all()
        wipe_mexc_all()
        # wipe_gate_all()
        pass
    
#============= Balances=============
    
def read_all_positions(db_path=None, tz_name="Europe/Zurich", limit=None):
    """
    Vista compacta de closed_positions:
    - Separadores entre filas (tablefmt='psql')
    - Fechas cortas dd-mm-yy HH:MM:SS (open_dt, close_dt), sin tz
    - Oculta leverage, liq_price/liquidation_price, notional, pnl, open_time, close_time, created_at, initial_margin
    - Redondeo: realized_pnl, funding_total, fee_total, pnl_percent, apr, ini_margin -> 2 decimales
    """
    # Ruta por defecto
    if db_path is None:
        try:
            from db_manager import DB_PATH as _DEFAULT_DB_PATH
            db_path = _DEFAULT_DB_PATH
        except Exception:
            db_path = "portfolio.db"

    def _num(s): return pd.to_numeric(s, errors="coerce")
    def _guess_epoch_unit(series):
        s = _num(series.dropna()); return "ms" if (not s.empty and s.max() > 1e11) else "s"
    def _price_pnl(side, entry, close, size):
        s = (str(side) or "").lower()
        return (entry - close) * size if s == "short" else (close - entry) * size

    # Cargar
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        "SELECT * FROM closed_positions ORDER BY COALESCE(close_time, open_time) DESC",
        conn
    )
    conn.close()

    print("📊 POSICIONES CERRADAS (compacto con separadores)")
    print(f"Base: {db_path} | Total: {len(df)}")

    if df.empty:
        print("No hay posiciones.")
        return df

    # Normalización mínima
    for c in ["size","entry_price","close_price","pnl","realized_pnl",
              "funding_total","fee_total","pnl_percent","apr",
              "initial_margin","ini_margin","notional","leverage",
              "open_time","close_time"]:
        if c in df.columns: df[c] = _num(df[c])

    # Fees siempre negativos
    if "fee_total" in df.columns: df["fee_total"] = -df["fee_total"].abs()
    else: df["fee_total"] = 0.0

    if "funding_total" not in df.columns: df["funding_total"] = 0.0
    if "notional" not in df.columns:
        df["notional"] = (df.get("size", 0) * df.get("entry_price", 0)).astype(float)

    if "pnl" not in df.columns:
        df["pnl"] = [
            _price_pnl(side, entry or 0.0, close or 0.0, size or 0.0)
            for side, entry, close, size in zip(
                df.get("side", []), df.get("entry_price", []),
                df.get("close_price", []), df.get("size", [])
            )
        ]

    if "realized_pnl" not in df.columns or df["realized_pnl"].isna().all():
        df["realized_pnl"] = df["pnl"].fillna(0) + df["funding_total"].fillna(0) + df["fee_total"].fillna(0)

    # Base capital: ini_margin > initial_margin > notional/leverage > notional
    if "ini_margin" in df.columns and df["ini_margin"].notna().any():
        base_capital = df["ini_margin"].abs()
    elif "initial_margin" in df.columns and df["initial_margin"].notna().any():
        base_capital = df["initial_margin"].abs()
    elif "notional" in df.columns and "leverage" in df.columns and df["leverage"].fillna(0).ne(0).any():
        lev = df["leverage"].replace(0, pd.NA)
        base_capital = (df["notional"].abs() / lev).fillna(df["notional"].abs())
    else:
        base_capital = df["notional"].abs()
    df["__base_capital"] = base_capital

    if "pnl_percent" not in df.columns or df["pnl_percent"].isna().all():
        df["pnl_percent"] = 0.0
        mask_cap = df["__base_capital"].abs() > 0
        df.loc[mask_cap, "pnl_percent"] = (df.loc[mask_cap, "realized_pnl"] / df.loc[mask_cap, "__base_capital"]) * 100.0

    # APR coherente
    if "open_time" not in df.columns: df["open_time"] = pd.NA
    if "close_time" not in df.columns: df["close_time"] = pd.NA
    sec = (df["close_time"].fillna(0) - df["open_time"].fillna(0)).astype(float)
    sec = sec.where(sec <= 1e12, sec / 1000.0)
    days = (sec / 86400.0).clip(lower=1e-9)
    if "apr" not in df.columns or df["apr"].isna().all():
        df["apr"] = df["pnl_percent"] * (365.0 / days)

    # Fechas dd-mm-yy HH:MM:SS sin tz
    uo = _guess_epoch_unit(df["open_time"]) if "open_time" in df.columns else "s"
    uc = _guess_epoch_unit(df["close_time"]) if "close_time" in df.columns else "s"
    df["open_dt"]  = pd.to_datetime(df["open_time"],  unit=uo, utc=True).dt.tz_convert(tz_name).dt.tz_localize(None).dt.strftime("%d-%m-%y %H:%M:%S")
    df["close_dt"] = pd.to_datetime(df["close_time"], unit=uc, utc=True).dt.tz_convert(tz_name).dt.tz_localize(None).dt.strftime("%d-%m-%y %H:%M:%S")

    # Alias visible para margen y ocultar initial_margin
    if "ini_margin" not in df.columns:
        df["ini_margin"] = df["initial_margin"] if "initial_margin" in df.columns else pd.NA

    # Selección compacta (oculta initial_margin expresamente)
    drop_cols = {
        "leverage","liq_price","liquidation_price","notional","initial_margin",
        "open_time","close_time","created_at","__base_capital"
    }
    preferred = [
        "id","exchange","symbol","side","size","entry_price","close_price", "pnl",
        "realized_pnl","funding_total","fee_total","pnl_percent","apr",
        "ini_margin","open_dt","close_dt"
    ]
    show_cols = [c for c in preferred if c in df.columns]
    extras = [c for c in df.columns if c not in drop_cols and c not in show_cols]
    disp = df[show_cols + extras].copy()

    # Redondeo general a 4 y específico a 2 en columnas pedidas
    for c in disp.columns:
        if pd.api.types.is_float_dtype(disp[c]):
            disp[c] = disp[c].round(4)

    two_dec_cols = [col for col in ["realized_pnl","funding_total","fee_total","pnl_percent","apr","ini_margin"] if col in disp.columns]
    for col in two_dec_cols:
        disp[col] = disp[col].map(lambda v: f"{v:.2f}" if pd.notnull(v) else "")

    if limit is not None:
        disp = disp.head(int(limit))

    print("=" * 120)
    try:
        print(tabulate(disp, headers="keys", tablefmt="fancy_grid", showindex=False))  # con líneas
    except Exception:
        print(disp.to_string(index=False))

    # Resumen breve (dejo valores crudos sin formatear a 2 para que las medias no parezcan redondeadas)
    print("\n📈 RESUMEN:")
    pnl_total = df["realized_pnl"].sum()
    winners = (df["realized_pnl"] > 0).sum()
    losers  = (df["realized_pnl"] < 0).sum()
    print(f"PNL Total: {pnl_total:,.2f} | Ganadoras: {winners} | Perdedoras: {losers} | %PNL medio: {df['pnl_percent'].mean():.2f} | APR medio: {df['apr'].mean():.2f}")

    return df

if __name__ == "__main__":
    read_all_positions()


#====== aqui funciona el ini_margin, dejar por si acaso.

# def read_all_positions(db_path=None, tz_name="Europe/Zurich", limit=None):
#     """
#     Vista compacta de closed_positions:
#     - Líneas divisorias (tablefmt='psql')
#     - Fechas cortas dd-mm-yy (open_dt, close_dt), sin zona
#     - Oculta leverage, liq_price (o liquidation_price), notional, pnl, open_time, close_time, created_at
#     - Redondeo a 4 decimales
#     """
#     # Ruta por defecto
#     if db_path is None:
#         try:
#             from db_manager import DB_PATH as _DEFAULT_DB_PATH
#             db_path = _DEFAULT_DB_PATH
#         except Exception:
#             db_path = "portfolio.db"

#     def _num(s): return pd.to_numeric(s, errors="coerce")

#     def _guess_epoch_unit(series):
#         s = _num(series.dropna())
#         return "ms" if (not s.empty and s.max() > 1e11) else "s"

#     def _price_pnl(side, entry, close, size):
#         s = (str(side) or "").lower()
#         return (entry - close) * size if s == "short" else (close - entry) * size

#     # Cargar
#     conn = sqlite3.connect(db_path)
#     df = pd.read_sql_query(
#         "SELECT * FROM closed_positions ORDER BY COALESCE(close_time, open_time) DESC",
#         conn
#     )
#     conn.close()

#     print("📊 POSICIONES CERRADAS (compacto con separadores)")
#     print(f"Base: {db_path} | Total: {len(df)}")

#     if df.empty:
#         print("No hay posiciones.")
#         return df

#     # Normalización mínima
#     for c in ["size","entry_price","close_price","pnl","realized_pnl",
#               "funding_total","fee_total","pnl_percent","apr",
#               "initial_margin","ini_margin","notional","leverage",
#               "open_time","close_time"]:
#         if c in df.columns: df[c] = _num(df[c])

#     # Fees siempre negativos
#     if "fee_total" in df.columns:
#         df["fee_total"] = -df["fee_total"].abs()
#     else:
#         df["fee_total"] = 0.0

#     # Funding por si no existe
#     if "funding_total" not in df.columns:
#         df["funding_total"] = 0.0

#     # Notional por defecto (aunque no lo mostraremos)
#     if "notional" not in df.columns:
#         df["notional"] = (df.get("size", 0) * df.get("entry_price", 0)).astype(float)

#     # PnL de precio si falta
#     if "pnl" not in df.columns:
#         df["pnl"] = [
#             _price_pnl(side, entry or 0.0, close or 0.0, size or 0.0)
#             for side, entry, close, size in zip(
#                 df.get("side", []), df.get("entry_price", []),
#                 df.get("close_price", []), df.get("size", [])
#             )
#         ]

#     # realized_pnl si falta: pnl + funding + fees
#     if "realized_pnl" not in df.columns or df["realized_pnl"].isna().all():
#         df["realized_pnl"] = df["pnl"].fillna(0) + df["funding_total"].fillna(0) + df["fee_total"].fillna(0)

#     # Base capital para % y APR: ini_margin > initial_margin > notional/leverage > notional
#     if "ini_margin" in df.columns and df["ini_margin"].notna().any():
#         base_capital = df["ini_margin"].abs()
#     elif "initial_margin" in df.columns and df["initial_margin"].notna().any():
#         base_capital = df["initial_margin"].abs()
#     elif "notional" in df.columns and "leverage" in df.columns and df["leverage"].fillna(0).ne(0).any():
#         lev = df["leverage"].replace(0, pd.NA)
#         base_capital = (df["notional"].abs() / lev).fillna(df["notional"].abs())
#     else:
#         base_capital = df["notional"].abs()
#     df["__base_capital"] = base_capital

#     # %PnL si falta
#     if "pnl_percent" not in df.columns or df["pnl_percent"].isna().all():
#         df["pnl_percent"] = 0.0
#         mask_cap = df["__base_capital"].abs() > 0
#         df.loc[mask_cap, "pnl_percent"] = (df.loc[mask_cap, "realized_pnl"] / df.loc[mask_cap, "__base_capital"]) * 100.0

#     # APR si falta, coherente con el mismo denominador
#     if "open_time" not in df.columns: df["open_time"] = pd.NA
#     if "close_time" not in df.columns: df["close_time"] = pd.NA
#     sec = (df["close_time"].fillna(0) - df["open_time"].fillna(0)).astype(float)
#     sec = sec.where(sec <= 1e12, sec / 1000.0)
#     days = (sec / 86400.0).clip(lower=1e-9)
#     if "apr" not in df.columns or df["apr"].isna().all():
#         df["apr"] = df["pnl_percent"] * (365.0 / days)

#     # Fechas legibles sin tz (naive local)
#     uo = _guess_epoch_unit(df["open_time"]) if "open_time" in df.columns else "s"
#     uc = _guess_epoch_unit(df["close_time"]) if "close_time" in df.columns else "s"
#     df["open_dt"] = pd.to_datetime(df["open_time"], unit=uo, utc=True).dt.tz_convert(tz_name).dt.tz_localize(None)
#     df["close_dt"] = pd.to_datetime(df["close_time"], unit=uc, utc=True).dt.tz_convert(tz_name).dt.tz_localize(None)

#     # Columna 'ini_margin' para mostrar, venga de donde venga
#     if "ini_margin" not in df.columns:
#         if "initial_margin" in df.columns:
#             df["ini_margin"] = df["initial_margin"]
#         else:
#             df["ini_margin"] = pd.NA

#     # Selección compacta (sin leverage, sin liq/notional/pnl/open/close/created_at)
#     drop_cols = {"leverage","liq_price","liquidation_price","notional","pnl","open_time","close_time","created_at","__base_capital"}
#     preferred = [
#         "id","exchange","symbol","side","size","entry_price","close_price",
#         "realized_pnl","funding_total","fee_total","pnl_percent","apr",
#         "ini_margin","open_dt","close_dt"
#     ]
#     show_cols = [c for c in preferred if c in df.columns]
#     extras = [c for c in df.columns if c not in drop_cols and c not in show_cols]
#     disp = df[show_cols + extras].copy()

#     # Redondeo amable a 4 decimales
#     for c in disp.columns:
#         if pd.api.types.is_float_dtype(disp[c]):
#             disp[c] = disp[c].round(4)

#     if limit is not None:
#         disp = disp.head(int(limit))

#     print("=" * 120)
#     try:
#         print(tabulate(disp, headers="keys", tablefmt="psql", showindex=False))  # con líneas
#     except Exception:
#         print(disp.to_string(index=False))

#     # Resumen breve
#     print("\n📈 RESUMEN:")
#     pnl_total = df["realized_pnl"].sum()
#     winners = (df["realized_pnl"] > 0).sum()
#     losers  = (df["realized_pnl"] < 0).sum()
#     print(f"PNL Total: {pnl_total:,.4f} | Ganadoras: {winners} | Perdedoras: {losers} | %PNL medio: {df['pnl_percent'].mean():.4f} | APR medio: {df['apr'].mean():.4f}")

#     return df

# if __name__ == "__main__":
#     read_all_positions()
    
