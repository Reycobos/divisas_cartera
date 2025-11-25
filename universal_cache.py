# universal_cache.py
import sqlite3
import time
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# SEPARAR DBs: cache en cache.db, posiciones/funding en portfolio.db
CACHE_DB_PATH = "cache.db"  # NUEVA DB SOLO PARA CACHE
DB_PATH = "portfolio.db"  # DB ORIGINAL PARA POSICIONES/FUNDING

# TOGGLE CONFIGURABLE - Días de retención del cache
CACHE_TTL_DAYS = 5  # Puedes cambiar este valor según necesites

import re

CACHE_LOG_VERBOSE = os.getenv("CACHE_LOG_VERBOSE", "0") == "1"


def _base_symbol(sym: str) -> str:
    """
    Normaliza símbolos: BTCUSDT -> BTC, BTC-PERP -> BTC
    """
    s = (sym or "").upper()
    s = re.sub(r"[-_/]?(USDT|USDC|USD)$", "", s)
    s = re.sub(r"[-_/]?PERP$", "", s)
    return s


def init_universal_cache_db(db_path: str = CACHE_DB_PATH):
    """Inicializa la base de datos para el cache universal"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS universal_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            currency_pair TEXT,
            symbol_type TEXT DEFAULT 'futures', -- futures, spot, etc.
            last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen INTEGER DEFAULT 0,
            UNIQUE(exchange, symbol)
        )
    """
    )

    # Índices para mejor performance
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_cache_exchange_symbol ON universal_cache(exchange, symbol)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_cache_last_used ON universal_cache(last_used)"
    )

    conn.commit()
    conn.close()


def migrate_add_last_seen(db_path: str = CACHE_DB_PATH):
    """Agrega columna last_seen si no existe"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()

    try:
        cur.execute("PRAGMA table_info(universal_cache)")
        columns = [row[1] for row in cur.fetchall()]

        if "last_seen" not in columns:
            print("🔧 Agregando columna last_seen...")
            cur.execute(
                "ALTER TABLE universal_cache ADD COLUMN last_seen INTEGER DEFAULT 0"
            )

            # Inicializar con timestamp actual
            current_ts = int(time.time())
            cur.execute("UPDATE universal_cache SET last_seen = ?", (current_ts,))

            conn.commit()
            print("✅ Columna last_seen agregada")
        else:
            print("✅ Columna last_seen ya existe")
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
    finally:
        conn.close()


def cleanup_old_cache(db_path: str = CACHE_DB_PATH):
    """Limpia cache antiguo según CACHE_TTL_DAYS"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()
    cutoff_date = datetime.now() - timedelta(days=CACHE_TTL_DAYS)
    cur.execute("DELETE FROM universal_cache WHERE last_used < ?", (cutoff_date,))
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    if deleted > 0:
        print(
            f"🧹 Cache limpiado: {deleted} entradas antiguas eliminadas (TTL: {CACHE_TTL_DAYS} días)"
        )


def symbol_to_currency_pair(symbol: str, exchange: str = "gate") -> str:
    """Convierte símbolo de futuros a currency pair de spot según el exchange"""
    symbol_upper = symbol.upper()

    if exchange.lower() == "gate":
        # Gate.io: "ALPACAUSDT" -> "ALPACA_USDT"
        if symbol_upper.endswith("USDT"):
            return f"{symbol_upper[:-4]}_USDT"
        elif symbol_upper.endswith("USDC"):
            return f"{symbol_upper[:-4]}_USDC"
        elif symbol_upper.endswith("BUSD"):
            return f"{symbol_upper[:-4]}_BUSD"

    elif exchange.lower() == "binance":
        # Binance: "ALPACAUSDT" -> "ALPACAUSDT" (mismo formato)
        return symbol_upper

    elif exchange.lower() == "kucoin":
        # KuCoin: "ALPACAUSDT" -> "ALPACA-USDT"
        if symbol_upper.endswith("USDT"):
            return f"{symbol_upper[:-4]}-USDT"
        elif symbol_upper.endswith("USDC"):
            return f"{symbol_upper[:-4]}-USDC"

    # Por defecto, mantener el símbolo original
    return symbol_upper


def update_cache_from_positions(
    exchange: str,
    positions: List[Dict[str, Any]],
    db_path: str = CACHE_DB_PATH,
    log_summary: Optional[bool] = None,
):
    """Actualiza el cache con las posiciones abiertas de cualquier exchange"""
    if log_summary is None:
        log_summary = CACHE_LOG_VERBOSE

    if CACHE_LOG_VERBOSE:
        print(f"🔄 Actualizando cache universal desde {exchange}...")

    init_universal_cache_db(db_path)
    cache_updates = 0

    for position in positions:
        symbol = position.get("symbol", "")
        if symbol:
            currency_pair = symbol_to_currency_pair(symbol, exchange)
            add_to_universal_cache(exchange, symbol, currency_pair)
            cache_updates += 1

    if log_summary:
        print(
            f"✅ Cache universal actualizado con {cache_updates} símbolos de {exchange}"
        )
    cleanup_old_cache()


def add_to_universal_cache(
    exchange: str,
    symbol: str,
    currency_pair: str = None,
    symbol_type: str = "futures",
    db_path: str = CACHE_DB_PATH,
):
    """Agrega o actualiza una entrada en el cache universal"""
    if not currency_pair:
        currency_pair = symbol_to_currency_pair(symbol, exchange)

    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT OR REPLACE INTO universal_cache 
            (exchange, symbol, currency_pair, symbol_type, last_used, last_seen)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        """,
            (exchange.lower(), symbol, currency_pair, symbol_type, int(time.time())),
        )
        conn.commit()
    except Exception as e:
        print(f"❌ Error agregando al cache universal: {e}")
    finally:
        conn.close()


def remove_from_universal_cache(
    exchange: str, symbols: List[str], db_path: str = CACHE_DB_PATH
) -> int:
    """Elimina símbolos específicos del cache universal."""
    cleaned = [
        (sym.strip()) for sym in (symbols or []) if isinstance(sym, str) and sym.strip()
    ]
    if not cleaned:
        return 0

    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()
    try:
        placeholders = ",".join(["?"] * len(cleaned))
        cur.execute(
            f"DELETE FROM universal_cache WHERE exchange = ? AND symbol IN ({placeholders})",
            [exchange.lower(), *cleaned],
        )
        deleted = cur.rowcount or 0
        conn.commit()
        return deleted
    except Exception as exc:
        conn.rollback()
        print(f"❌ Error eliminando símbolos del cache: {exc}")
        return 0
    finally:
        conn.close()


def get_cached_currency_pairs(
    exchange: str = None, db_path: str = CACHE_DB_PATH
) -> List[str]:
    """Obtiene todos los currency pairs del cache"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()

    try:
        if exchange:
            cur.execute(
                "SELECT currency_pair FROM universal_cache WHERE exchange = ?",
                (exchange.lower(),),
            )
        else:
            cur.execute("SELECT currency_pair FROM universal_cache")

        pairs = [row[0] for row in cur.fetchall()]

        # Actualizar last_used - CON TRANSACCIÓN EXPLÍCITA
        if pairs:
            conn.execute("BEGIN IMMEDIATE")
            placeholders = ",".join(["?"] * len(pairs))
            if exchange:
                cur.execute(
                    f"UPDATE universal_cache SET last_used = CURRENT_TIMESTAMP WHERE currency_pair IN ({placeholders}) AND exchange = ?",
                    pairs + [exchange.lower()],
                )
            else:
                cur.execute(
                    f"UPDATE universal_cache SET last_used = CURRENT_TIMESTAMP WHERE currency_pair IN ({placeholders})",
                    pairs,
                )
            conn.commit()

        return pairs
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def get_cached_symbols(
    exchange: str = None, db_path: str = CACHE_DB_PATH
) -> List[Dict[str, Any]]:
    """Obtiene todos los símbolos del cache con información completa"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if exchange:
        cur.execute(
            "SELECT * FROM universal_cache WHERE exchange = ?", (exchange.lower(),)
        )
    else:
        cur.execute("SELECT * FROM universal_cache")

    results = [dict(row) for row in cur.fetchall()]

    # Actualizar last_used
    if results:
        ids = [str(row["id"]) for row in results]
        placeholders = ",".join(["?"] * len(ids))
        cur.execute(
            f"UPDATE universal_cache SET last_used = CURRENT_TIMESTAMP WHERE id IN ({placeholders})",
            ids,
        )
        conn.commit()

    conn.close()
    return results


def get_currency_pair_for_symbol(
    exchange: str, symbol: str, db_path: str = CACHE_DB_PATH
) -> Optional[str]:
    """Obtiene el currency pair para un símbolo específico de un exchange"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()

    cur.execute(
        "SELECT currency_pair FROM universal_cache WHERE exchange = ? AND symbol = ?",
        (exchange.lower(), symbol),
    )
    result = cur.fetchone()

    if result:
        # Actualizar last_used
        cur.execute(
            "UPDATE universal_cache SET last_used = CURRENT_TIMESTAMP WHERE exchange = ? AND symbol = ?",
            (exchange.lower(), symbol),
        )
        conn.commit()
        conn.close()
        return result[0]

    conn.close()
    return None


def search_symbols_by_base(
    base_currency: str, exchange: str = None, db_path: str = CACHE_DB_PATH
) -> List[Dict[str, Any]]:
    """Busca símbolos por currency base (ej: 'BTC' para BTCUSDT, BTC_USDT, etc.)"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    base_pattern = f"{base_currency.upper()}%"

    if exchange:
        cur.execute(
            "SELECT * FROM universal_cache WHERE exchange = ? AND (symbol LIKE ? OR currency_pair LIKE ?)",
            (exchange.lower(), base_pattern, base_pattern),
        )
    else:
        cur.execute(
            "SELECT * FROM universal_cache WHERE symbol LIKE ? OR currency_pair LIKE ?",
            (base_pattern, base_pattern),
        )

    results = [dict(row) for row in cur.fetchall()]

    # Actualizar last_used
    if results:
        ids = [str(row["id"]) for row in results]
        placeholders = ",".join(["?"] * len(ids))
        cur.execute(
            f"UPDATE universal_cache SET last_used = CURRENT_TIMESTAMP WHERE id IN ({placeholders})",
            ids,
        )
        conn.commit()

    conn.close()
    return results


# Función para agregar manualmente un pair al cache
def add_manual_pair(
    exchange: str,
    symbol: str,
    currency_pair: str = None,
    symbol_type: str = "futures",
    db_path: str = CACHE_DB_PATH,
):
    """Agrega manualmente un símbolo al cache universal"""
    if not currency_pair:
        currency_pair = symbol_to_currency_pair(symbol, exchange)

    add_to_universal_cache(exchange, symbol, currency_pair, symbol_type)
    print(
        f"✅ Agregado manualmente a cache universal: {exchange} - {symbol} -> {currency_pair}"
    )


# Función para obtener estadísticas del cache
def get_cache_stats(db_path: str = CACHE_DB_PATH) -> Dict[str, Any]:
    """Obtiene estadísticas del cache universal"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()

    # Total por exchange
    cur.execute(
        "SELECT exchange, COUNT(*) as count FROM universal_cache GROUP BY exchange"
    )
    by_exchange = {row[0]: row[1] for row in cur.fetchall()}

    # Total general
    cur.execute("SELECT COUNT(*) FROM universal_cache")
    total = cur.fetchone()[0]

    # Más antiguo y más reciente
    cur.execute("SELECT MIN(last_used), MAX(last_used) FROM universal_cache")
    oldest, newest = cur.fetchone()

    conn.close()

    return {
        "total_entries": total,
        "by_exchange": by_exchange,
        "oldest_entry": oldest,
        "newest_entry": newest,
        "cache_ttl_days": CACHE_TTL_DAYS,
    }


# ===========================
# Preferencias de exchanges abiertos
# ===========================
def init_selected_open_exchanges_table(db_path: str = CACHE_DB_PATH):
    """Crea la tabla que almacena los exchanges seleccionados en la UI."""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS selected_open_exchanges (
            exchange TEXT PRIMARY KEY,
            sort_index INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
    conn.close()


def set_selected_open_exchanges(exchanges: List[str], db_path: str = CACHE_DB_PATH):
    """Persiste la lista ordenada de exchanges seleccionados para posiciones abiertas."""
    normalized: List[str] = []
    seen = set()
    for ex in exchanges or []:
        key = (ex or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(key)

    init_selected_open_exchanges_table(db_path)
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur.execute("DELETE FROM selected_open_exchanges")
        cur.executemany(
            """
            INSERT INTO selected_open_exchanges (exchange, sort_index, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
            [(ex, idx) for idx, ex in enumerate(normalized)],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_selected_open_exchanges(db_path: str = CACHE_DB_PATH) -> List[str]:
    """Recupera los exchanges seleccionados; devuelve lista vacía si no hay preferencia."""
    init_selected_open_exchanges_table(db_path)
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT exchange FROM selected_open_exchanges ORDER BY sort_index ASC"
        )
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


## cache para autoejecutar closed positions


def init_sync_timestamps_table(db_path: str = DB_PATH):
    """Inicializa la tabla de timestamps de sincronización"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_timestamps (
            exchange TEXT PRIMARY KEY,
            last_sync_closed INTEGER,
            last_sync_funding INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    conn.commit()
    conn.close()
    print("✅ Tabla sync_timestamps inicializada")


def update_sync_timestamp(exchange: str, db_path: str = DB_PATH):
    """Registra el timestamp de la última sincronización de cerradas"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()

    # Crear tabla si no existe
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_timestamps (
            exchange TEXT PRIMARY KEY,
            last_sync_closed INTEGER,
            last_sync_funding INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    now_ms = int(time.time() * 1000)
    cur.execute(
        """
        INSERT OR REPLACE INTO sync_timestamps (exchange, last_sync_closed, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
    """,
        (exchange.lower(), now_ms),
    )

    conn.commit()
    conn.close()


def get_last_sync_timestamp(exchange: str, db_path: str = DB_PATH) -> Optional[int]:
    """Obtiene el timestamp de la última sincronización de cerradas"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()

    # Crear tabla si no existe (defensivo)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_timestamps (
            exchange TEXT PRIMARY KEY,
            last_sync_closed INTEGER,
            last_sync_funding INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    try:
        cur.execute(
            """
            SELECT last_sync_closed FROM sync_timestamps 
            WHERE exchange = ?
        """,
            (exchange.lower(),),
        )

        result = cur.fetchone()
        return result[0] if result else None
    finally:
        conn.commit()
        conn.close()


def detect_closed_positions(
    exchange: str, current_positions: List[Dict[str, Any]], db_path: str = DB_PATH
) -> set:
    """
    Detecta símbolos que estaban en caché pero ya no están en posiciones actuales
    (posiblemente cerrados)
    """
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()

    current_symbols = set()
    for pos in current_positions:
        symbol = (pos.get("symbol") or "").upper()
        normalized = _base_symbol(symbol)
        current_symbols.add(normalized)

    # Símbolos en caché del exchange
    cur.execute(
        """
        SELECT DISTINCT symbol FROM universal_cache 
        WHERE exchange = ? AND last_used >= datetime('now', '-7 days')
    """,
        (exchange.lower(),),
    )

    cached_symbols = set(row[0] for row in cur.fetchall())
    conn.close()

    # Símbolos que desaparecieron (potencialmente cerrados)
    disappeared = cached_symbols - current_symbols

    return disappeared


# Código autoejecutable para Spyder
if __name__ == "__main__":
    print("🚀 Ejecutando demostración del cache universal...")
    print(f"📅 TTL configurado: {CACHE_TTL_DAYS} días")

    # Inicializar y migrar
    init_universal_cache_db()
    migrate_add_last_seen()

    # Mostrar estadísticas
    stats = get_cache_stats()
    print(f"📊 Estadísticas del cache:")
    print(f"   Total entradas: {stats['total_entries']}")
    for exchange, count in stats["by_exchange"].items():
        print(f"   {exchange}: {count} símbolos")

    # Mostrar cache actual para Gate
    try:
        gate_pairs = get_cached_currency_pairs("gate")
        print(f"📦 Cache Gate.io: {len(gate_pairs)} pairs")
        for pair in gate_pairs[:10]:  # Mostrar solo primeros 10
            print(f"   - {pair}")
        if len(gate_pairs) > 10:
            print(f"   ... y {len(gate_pairs) - 10} más")
    except Exception as e:
        print(f"❌ Error mostrando cache Gate: {e}")
