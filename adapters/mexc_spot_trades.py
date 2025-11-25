# adapters/mexc_spot_trades.py
# -*- coding: utf-8 -*-
"""
MEXC — Spot trades → closed positions (FIFO)
Nueva implementación siguiendo documentación oficial de MEXC
"""

from __future__ import annotations
import os
import sys
import sqlite3
import time
import hmac
import hashlib
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional
from urllib.parse import urlencode
import requests

from dotenv import load_dotenv

load_dotenv()

# === Path utils ===
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from utils.symbols import normalize_symbol
from utils.time import to_s

# === MEXC Spot Configuration ===
MEXC_SPOT_BASE_URL = "https://api.mexc.com"
MEXC_API_KEY = os.getenv("MEXC_API_KEY", "")
MEXC_API_SECRET = os.getenv("MEXC_API_SECRET", "")
MEXC_RECV_WINDOW = "5000"

DB_PATH_DEFAULT = os.path.join(BASE_DIR, "portfolio.db")

# === Símbolos por defecto (pueden comentarse después) ===
SYMBOLS_DEFAULT = ["CUDISUSDT", "USDCUSDT"]
STABLES = {"USDT", "USDC"}
DUST_RATIO = 0.02
SPOT_QUOTES = ("USDT", "USDC", "USD", "BTC", "ETH")


# === Funciones de autenticación MEXC Spot ===
def _now_ms() -> int:
    return int(time.time() * 1000)


def _has_creds() -> bool:
    return bool(MEXC_API_KEY and MEXC_API_SECRET)


def _generate_signature(params: Dict[str, Any]) -> str:
    """Genera firma HMAC SHA256 para MEXC Spot"""
    query_string = urlencode(params)
    signature = hmac.new(
        MEXC_API_SECRET.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return signature


def _mexc_spot_request(
    method: str,
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    private: bool = False,
    timeout: int = 30,
    max_retries: int = 3,
) -> Dict[str, Any]:
    """
    Cliente HTTP para MEXC Spot API - CORREGIDO
    """
    url = f"{MEXC_SPOT_BASE_URL}{endpoint}"
    params = dict(params or {})

    headers = {
        "Content-Type": "application/json",
        "X-MEXC-APIKEY": MEXC_API_KEY,
    }

    # Para endpoints privados, agregar timestamp y firma
    if private:
        params["timestamp"] = _now_ms()
        params["recvWindow"] = MEXC_RECV_WINDOW

        # Generar firma
        signature = _generate_signature(params)
        params["signature"] = signature

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            if method.upper() == "GET":
                # CORREGIDO: Usar el parámetro 'params' de requests para evitar duplicación
                response = requests.get(
                    url, headers=headers, params=params, timeout=timeout
                )
            else:
                # Para POST, parámetros van en body
                response = requests.post(
                    url, headers=headers, data=params, timeout=timeout
                )

            response.raise_for_status()
            data = response.json()

            # Verificar respuesta de error MEXC
            if isinstance(data, dict) and "code" in data and data["code"] != 200:
                error_msg = data.get("msg", "Unknown error")
                raise RuntimeError(f"MEXC API error {data['code']}: {error_msg}")

            return data

        except requests.exceptions.Timeout:
            print(f"⏰ Timeout en intento {attempt}/{max_retries} para {endpoint}")
            if attempt >= max_retries:
                raise
            time.sleep(attempt * 1)
        except requests.exceptions.ConnectionError as e:
            print(f"🔌 Connection error en intento {attempt}/{max_retries}: {e}")
            if attempt >= max_retries:
                raise
            time.sleep(attempt * 1)
        except Exception as e:
            last_err = e
            if attempt >= max_retries:
                raise
            time.sleep(attempt * 1)

    raise last_err or RuntimeError("MEXC Spot request failed")


# === Funciones de base de datos ===
def get_existing_trade_hashes(db_path: str) -> set:
    """Obtiene hashes de trades ya existentes en la base de datos"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT exchange, symbol, side, open_time, close_time, size 
            FROM closed_positions 
            WHERE exchange = 'mexc' AND side IN ('spotbuy', 'spotsell', 'swapstable')
        """
        )
        existing_trades = cursor.fetchall()

        hashes = set()
        for trade in existing_trades:
            exchange, symbol, side, open_time, close_time, size = trade
            # Hash más específico incluyendo open_time y close_time
            trade_hash = (
                f"{exchange}_{symbol}_{side}_{open_time}_{close_time}_{round(size, 8)}"
            )
            hashes.add(trade_hash)

        return hashes
    finally:
        conn.close()


def _insert_row(conn: sqlite3.Connection, row: dict):
    """Inserta una fila en closed_positions si no existe"""
    cols = [
        "exchange",
        "symbol",
        "side",
        "size",
        "entry_price",
        "close_price",
        "open_time",
        "close_time",
        "pnl",
        "realized_pnl",
        "fee_total",
        "notional",
        "ignore_trade",
    ]

    # Verificar si ya existe
    check_sql = """
        SELECT COUNT(*) FROM closed_positions 
        WHERE exchange = ? AND symbol = ? AND side = ? 
        AND open_time = ? AND close_time = ? AND size = ?
    """
    check_vals = (
        row.get("exchange"),
        row.get("symbol"),
        row.get("side"),
        row.get("open_time"),
        row.get("close_time"),
        row.get("size"),
    )

    cursor = conn.cursor()
    cursor.execute(check_sql, check_vals)
    exists = cursor.fetchone()[0] > 0

    if exists:
        print(
            f"   🔄 Posición ya existe (ignorada): {row.get('symbol')} {row.get('side')} {row.get('size')}"
        )
        return  # Ya existe, no insertar

    # Insertar si no existe
    sql = f"INSERT INTO closed_positions ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})"
    vals = tuple(row.get(c, 0 if c != "ignore_trade" else 0) for c in cols)

    conn.execute(sql, vals)
    print(
        f"   💾 Nueva posición guardada: {row.get('symbol')} {row.get('side')} {row.get('size')}"
    )


# === Data classes ===
@dataclass
class Fill:
    ts: int
    pair: str
    side: str
    amount: float
    price: float
    fee: float
    fee_ccy: str

    @property
    def base_quote(self) -> Tuple[str, str]:
        return _split_pair(self.pair)

    def fee_in_quote(self) -> float:
        """Convierte la fee a QUOTE"""
        base, quote = self.base_quote
        if self.fee_ccy == quote:
            return self.fee
        if self.fee_ccy == base:
            return self.fee * self.price
        return self.fee


@dataclass
class RoundAgg:
    """Acumula PnL y fees de una ronda FIFO completa"""

    total_base_bought: float = 0.0
    cost_quote: float = 0.0
    fee_buy: float = 0.0

    total_base_sold: float = 0.0
    proceeds_quote: float = 0.0
    fee_sell: float = 0.0

    open_time: int = 0
    close_time: int = 0

    def merge_buy(self, amt: float, px: float, fee_q: float, ts: int):
        self.total_base_bought += amt
        self.cost_quote += amt * px
        self.fee_buy += fee_q
        if self.open_time == 0 or ts < self.open_time:
            self.open_time = ts

    def merge_sell(self, amt: float, px: float, fee_q: float, ts: int):
        self.total_base_sold += amt
        self.proceeds_quote += amt * px
        self.fee_sell += fee_q
        if self.close_time == 0 or ts > self.close_time:
            self.close_time = ts

    def is_valid(self) -> bool:
        return self.total_base_sold > 1e-12

    def finalize(self) -> dict:
        entry = self.cost_quote / max(self.total_base_bought, 1e-12)
        close = self.proceeds_quote / max(self.total_base_sold, 1e-12)

        pnl = (close - entry) * self.total_base_sold
        fee_total = -(self.fee_buy + self.fee_sell)
        realized = pnl + fee_total
        notional = self.cost_quote

        return {
            "size": self.total_base_sold,
            "entry_price": entry,
            "close_price": close,
            "pnl": pnl,
            "realized_pnl": realized,
            "fee_total": fee_total,
            "open_time": self.open_time,
            "close_time": self.close_time,
            "notional": notional,
        }


# === Helper functions ===
def _num(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _split_pair(cp: str) -> Tuple[str, str]:
    """'AAAUSDT' -> (base='AAA', quote='USDT')"""
    s = (cp or "").upper().strip()
    if s.endswith("USDT"):
        return s[:-4], "USDT"
    if s.endswith("USDC"):
        return s[:-4], "USDC"
    if s.endswith("USD"):
        return s[:-3], "USD"
    return s, ""


def _fmt_ms(ms) -> str:
    """Convierte ms/seg a 'YYYY-MM-DD HH:MM:SS UTC'"""
    from datetime import datetime, timezone

    try:
        ms = int(ms or 0)
        if ms and ms < 1_000_000_000_000:
            ms *= 1000
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    except Exception:
        return str(ms)


def _normalize_cached_symbol(raw_symbol: Optional[str]) -> Optional[str]:
    """Transforma simbolos del cache (spot/futuros) en un par spot válido."""
    if not raw_symbol:
        return None

    candidate = raw_symbol.upper().strip()
    for sep in ("-", "_", "/", ":"):
        candidate = candidate.replace(sep, "")

    if any(candidate.endswith(quote) for quote in SPOT_QUOTES):
        return candidate
    return None


def get_symbols_from_cache() -> List[str]:
    """Obtiene símbolos del cache universal"""
    try:
        sys.path.append(BASE_DIR)
        from universal_cache import get_cached_symbols, init_universal_cache_db

        # INICIALIZAR LA BASE DE DATOS DEL CACHE PRIMERO
        init_universal_cache_db()

        cached_symbols = get_cached_symbols("mexc")

        # Si no hay símbolos en cache, usar los por defecto
        if not cached_symbols:
            print("⚠️  No hay símbolos en cache, usando símbolos por defecto")
            return SYMBOLS_DEFAULT

        spot_symbols = []

        for symbol_data in cached_symbols:
            normalized = _normalize_cached_symbol(
                symbol_data.get("symbol")
            ) or _normalize_cached_symbol(symbol_data.get("currency_pair"))

            if normalized:
                spot_symbols.append(normalized)

        return list(set(spot_symbols))  # Remover duplicados

    except ImportError:
        print("⚠️  universal_cache no disponible, usando símbolos por defecto")
        return SYMBOLS_DEFAULT
    except Exception as e:
        print(f"⚠️  Error obteniendo símbolos del cache: {e}")
        return SYMBOLS_DEFAULT


# === Fetch layer ===
def fetch_spot_trades_for_symbol(
    symbol: str,
    days_back: int = 30,
    limit: int = 1000,
    existing_hashes: set = None,
    debug: bool = False,
) -> List[Fill]:
    """
    Descarga trades spot para un símbolo usando /api/v3/myTrades
    """
    if existing_hashes is None:
        existing_hashes = set()

    all_fills = []

    # MEXC solo permite consultar hasta 1 mes de histórico
    days_back = min(days_back, 30)
    end_time = _now_ms()
    start_time = end_time - (days_back * 24 * 3600 * 1000)

    params = {
        "symbol": symbol,
        "limit": min(limit, 1000),
        "startTime": start_time,
        "endTime": end_time,
    }

    if debug:
        print(f"📥 Descargando trades spot para {symbol} desde {_fmt_ms(start_time)}")

    try:
        # Llamada a la API de MEXC Spot
        data = _mexc_spot_request(
            "GET", "/api/v3/myTrades", params=params, private=True
        )

        # MEXC devuelve un array directamente
        trades = data if isinstance(data, list) else []

        if debug:
            print(f"   Recibidos {len(trades)} trades")

        for trade in trades:
            # Parsear según documentación de MEXC
            ts_ms = trade.get("time", 0)
            ts = int(ts_ms / 1000) if ts_ms else 0
            pair = trade.get("symbol", "").upper()
            is_buyer = trade.get("isBuyer", False)
            side = "buy" if is_buyer else "sell"
            amt = _num(trade.get("qty", 0))
            px = _num(trade.get("price", 0))
            fee = _num(trade.get("commission", 0))
            fee_ccy = (trade.get("commissionAsset") or "").upper()

            # CORREGIDO: Usar el mismo formato que en get_existing_trade_hashes
            # Para spot, open_time y close_time son iguales (trade individual)
            trade_hash = f"mexc_{pair}_{side}_{ts}_{ts}_{round(amt, 8)}"

            if trade_hash not in existing_hashes:
                fill = Fill(ts, pair, side, amt, px, fee, fee_ccy)
                all_fills.append(fill)
                existing_hashes.add(trade_hash)

                if debug:
                    print(f"   ➕ Nuevo trade: {side} {amt} {pair} @ {px}")
            else:
                if debug:
                    print(
                        f"   🔄 Trade duplicado (ignorado): {side} {amt} {pair} @ {px}"
                    )

    except Exception as e:
        print(f"❌ Error descargando {symbol}: {e}")
        if debug:
            import traceback

            traceback.print_exc()

    return all_fills


# === Main processing function ===
def save_mexc_spot_positions(
    symbols: List[str] = None,
    db_path: str = DB_PATH_DEFAULT,
    days_back: int = 30,
    debug: bool = False,
) -> Tuple[int, int]:
    """
    Procesa spot trades de MEXC con FIFO
    """
    if not _has_creds():
        print("⚠️  No hay credenciales MEXC configuradas")
        return 0, 0

    # Obtener símbolos (cache universal o por defecto)
    if symbols is None:
        symbols = get_symbols_from_cache()

    if not symbols:
        print("⚠️  No hay símbolos para procesar")
        return 0, 0

    print(f"🎯 Procesando {len(symbols)} símbolos")

    existing_hashes = get_existing_trade_hashes(db_path)
    conn = sqlite3.connect(db_path)
    saved = 0
    ignored = 0
    symbols_with_trades = 0
    total_trades_found = 0

    for symbol in symbols:
        if debug:
            print(f"\n{'='*60}")
            print(f"🔄 Procesando {symbol}")
            print(f"{'='*60}")

        fills = fetch_spot_trades_for_symbol(
            symbol, days_back, 1000, existing_hashes, debug
        )
        total_trades_found += len(fills)

        if not fills:
            if debug:
                print(f"   Sin trades nuevos para {symbol}")
            continue

        symbols_with_trades += 1
        print(f"✅ {symbol}: {len(fills)} trades encontrados")

        # Procesamiento FIFO (igual que antes)
        by_pair = defaultdict(list)
        for f in fills:
            by_pair[f.pair].append(f)

        symbol_saved = 0
        symbol_ignored = 0

        for pair, trades in by_pair.items():
            base, quote = _split_pair(pair)

            if debug:
                print(f"\n📊 Par: {pair} ({base}/{quote}) - {len(trades)} trades")

            # Swaps entre stables
            if base in STABLES and quote in STABLES:
                if debug:
                    print(f"   💱 Swap stable detectado: {base}<->{quote}")

                for f in trades:
                    fee_q = f.fee_in_quote()

                    if f.side == "buy":
                        net_base_out = f.amount * f.price
                        received_quote = f.amount
                    else:
                        net_base_out = f.amount
                        received_quote = f.amount * f.price

                    price_pnl = received_quote - net_base_out
                    realized = price_pnl - abs(fee_q)

                    row = {
                        "exchange": "mexc",
                        "symbol": f"{base}{quote}",
                        "side": "swapstable",
                        "size": abs(net_base_out),
                        "entry_price": 1.0,
                        "close_price": 1.0,
                        "pnl": price_pnl,
                        "realized_pnl": realized,
                        "fee_total": -abs(fee_q),
                        "open_time": f.ts,
                        "close_time": f.ts,
                        "notional": max(received_quote, net_base_out),
                        "ignore_trade": 0,
                    }
                    _insert_row(conn, row)
                    saved += 1
                    symbol_saved += 1
                continue

            # FIFO para tokens normales
            trades.sort(key=lambda x: x.ts)

            # Primer SELLs son depósitos/transfers
            idx = 0
            while idx < len(trades) and trades[idx].side == "sell":
                f = trades[idx]
                fee_q = f.fee_in_quote()
                row = {
                    "exchange": "mexc",
                    "symbol": normalize_symbol(f"{base}{quote}"),
                    "side": "spotsell",
                    "size": abs(f.amount),
                    "entry_price": f.price,
                    "close_price": f.price,
                    "pnl": 0.0,
                    "realized_pnl": 0.0,
                    "fee_total": -abs(fee_q),
                    "open_time": f.ts,
                    "close_time": f.ts,
                    "notional": abs(f.amount) * f.price,
                    "ignore_trade": 1,
                }
                _insert_row(conn, row)
                ignored += 1
                symbol_ignored += 1
                idx += 1

                if debug:
                    print(
                        f"   🔸 Spotsell inicial ignorado: {f.amount} {base} @ {f.price}"
                    )

            # Procesamiento FIFO
            lot_q = deque()
            round_agg = RoundAgg()
            round_started = False
            total_qty_in_round = 0.0
            inventory_base = 0.0
            peak_inventory_base = 0.0
            sells_occurred = False

            def _flush_round():
                nonlocal saved, symbol_saved, round_agg, round_started, total_qty_in_round, peak_inventory_base
                if not round_started or not round_agg.is_valid():
                    return

                data = round_agg.finalize()
                data["size"] = peak_inventory_base

                row = {
                    "exchange": "mexc",
                    "symbol": normalize_symbol(f"{base}{quote}"),
                    "side": "spotbuy",
                    "ignore_trade": 0,
                    **data,
                }
                _insert_row(conn, row)
                saved += 1
                symbol_saved += 1

                if debug:
                    print(
                        f"   ✅ Ronda cerrada: {data['size']:.4f} {base}, PnL: {data['realized_pnl']:.2f} USDT"
                    )

                round_agg = RoundAgg()
                round_started = False
                total_qty_in_round = 0.0
                peak_inventory_base = 0.0

            # Procesar trades
            for f in trades[idx:]:
                if f.side == "buy":
                    round_started = True
                    fee_q = f.fee_in_quote()

                    if f.fee_ccy.upper() == base:
                        received_base = max(f.amount - f.fee, 0.0)
                    else:
                        received_base = f.amount

                    fee_per_unit_q = fee_q / max(received_base, 1e-12)
                    lot_q.append([received_base, f.price, fee_per_unit_q, f.ts])

                    round_agg.merge_buy(f.amount, f.price, fee_q, f.ts)
                    total_qty_in_round += f.amount

                    inventory_base += received_base
                    if inventory_base > peak_inventory_base:
                        peak_inventory_base = inventory_base

                else:  # sell
                    sells_occurred = True
                    fee_q = f.fee_in_quote()
                    sell_qty = f.amount
                    sell_left = sell_qty

                    while sell_left > 1e-12 and lot_q:
                        q, p, fee_u, tsb = lot_q[0]
                        take = min(q, sell_left)

                        round_agg.merge_sell(
                            take,
                            f.price,
                            fee_q * (take / sell_qty) if sell_qty > 0 else 0.0,
                            f.ts,
                        )

                        q -= take
                        sell_left -= take
                        if q <= 1e-12:
                            lot_q.popleft()
                        else:
                            lot_q[0][0] = q

                    inventory_base = sum(q for q, *_ in lot_q)

                    dust = max(0.01, DUST_RATIO * peak_inventory_base)
                    if (inventory_base <= dust and total_qty_in_round >= 1) or (
                        not lot_q and sell_left <= 1e-12
                    ):
                        _flush_round()

            # Procesar lotes restantes
            if lot_q:
                rem_base = sum(q for q, *_ in lot_q)
                dust = max(0.01, DUST_RATIO * peak_inventory_base)

                if sells_occurred and rem_base <= dust:
                    _flush_round()
                else:
                    notional = sum(q * p for q, p, *_ in lot_q)
                    ts_open = min(ts for *_a, ts in lot_q)
                    row = {
                        "exchange": "mexc",
                        "symbol": normalize_symbol(f"{base}{quote}"),
                        "side": "spotbuy",
                        "size": rem_base,
                        "entry_price": notional / max(rem_base, 1e-12),
                        "close_price": notional / max(rem_base, 1e-12),
                        "pnl": 0.0,
                        "realized_pnl": 0.0,
                        "fee_total": 0.0,
                        "open_time": ts_open,
                        "close_time": ts_open,
                        "notional": notional,
                        "ignore_trade": 1,
                    }
                    _insert_row(conn, row)
                    ignored += 1
                    symbol_ignored += 1

                    if debug:
                        print(f"   ⚠️  Posición abierta ignorada: {rem_base:.4f} {base}")

        if symbol_saved > 0 or symbol_ignored > 0:
            print(
                f"   📊 {symbol}: {symbol_saved} guardadas, {symbol_ignored} ignoradas"
            )

    conn.commit()
    conn.close()

    # RESUMEN FINAL
    print(f"\n{'='*60}")
    if symbols_with_trades > 0:
        print(f"✅ MEXC Spot FIFO COMPLETADO:")
        print(f"   📈 Símbolos con trades: {symbols_with_trades}")
        print(f"   🔢 Trades encontrados: {total_trades_found}")
        print(f"   💾 Posiciones guardadas: {saved}")
        print(f"   ⚠️  Posiciones ignoradas: {ignored}")
    else:
        print(f"ℹ️  No se encontraron trades nuevos en MEXC Spot")
    print(f"{'='*60}")

    return saved, ignored


# === CLI ===
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="MEXC Spot FIFO → closed_positions")
    parser.add_argument(
        "--db", type=str, default=DB_PATH_DEFAULT, help="Ruta a portfolio.db"
    )
    parser.add_argument(
        "--days_back", type=int, default=30, help="Ventana de histórico (días)"
    )
    parser.add_argument(
        "--symbols",
        type=str,
        nargs="*",
        help="Símbolos a procesar (default: cache universal)",
    )
    parser.add_argument("--debug", action="store_true", help="Logs verbosos")
    args = parser.parse_args()

    print(f"\n🎯 MEXC Spot FIFO (Nueva implementación)")
    print(f"{'='*60}")
    print(
        f"📊 Símbolos: {'Desde cache universal' if not args.symbols else ', '.join(args.symbols)}"
    )
    print(f"📅 Días hacia atrás: {args.days_back}")
    print(f"💾 Base de datos: {args.db}")
    print(f"🔍 Debug: {'Sí' if args.debug else 'No'}")
    print(f"{'='*60}\n")

    save_mexc_spot_positions(
        symbols=args.symbols,
        db_path=args.db,
        days_back=args.days_back,
        debug=args.debug,
    )
