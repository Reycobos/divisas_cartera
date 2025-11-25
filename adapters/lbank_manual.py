# lbank_manual.py
"""
LBANK Manual Import Adapter
Importa trades y funding fees desde CSV exportados de LBANK.

Archivos esperados:
- Filled (trades): Time,Symbol,Direction,Price,Quantity,Fee,Realized P&L
- Futures (funding): Symbol,asset,Time,Funding Type,Amount,Total

Timezone: UTC+0 (ya viene en UTC)
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
import pandas as pd
import io
import hashlib
import re
from zoneinfo import ZoneInfo
from datetime import datetime

# === utilidades del proyecto ===
import sys
from pathlib import Path

# Añadir parent dir al path para imports desde adapters/
_PARENT = Path(__file__).resolve().parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from utils.symbols import normalize_symbol
from db_manager import init_funding_db, upsert_funding_events, save_closed_position

# Ruta a portfolio.db (en el directorio padre)
DB_PATH = _PARENT / "portfolio.db"

TZ_SRC = ZoneInfo("UTC")  # LBANK CSV está en UTC+0
TZ_APP = ZoneInfo("Europe/Zurich")  # hora local app (UTC+1)

LBANK = "lbank"
PRODUCT_FUTURES = "Futures position"


# -----------------------------
# Helpers
# -----------------------------
def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _to_s(dt: datetime) -> int:
    return int(dt.timestamp())


def _parse_time_utc_to_zurich(s: str) -> datetime:
    """
    Parsea tiempo UTC+0 de LBANK y convierte a Zurich (UTC+1).
    Formato: "2025-11-17 08:38:24 (UTC+0)"
    """
    # Extraer la parte de fecha/hora sin el "(UTC+0)"
    match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", str(s).strip())
    if not match:
        raise ValueError(f"Invalid timestamp format: {s}")

    time_str = match.group(1)
    ts = pd.to_datetime(time_str, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"Invalid timestamp: {s}")

    # El CSV está en UTC+0, Zurich es UTC+1, sumar 1 hora
    ts_adjusted = ts + pd.Timedelta(hours=1)

    # Localizar como Zurich
    if ts_adjusted.tzinfo is None:
        ts_adjusted = ts_adjusted.tz_localize(TZ_APP)

    return ts_adjusted.to_pydatetime()


def _parse_number(val) -> float:
    """Parsea un número que puede venir como string con formato."""
    if pd.isna(val):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    # Limpiar string
    s = str(val).strip().replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _normalize_lbank_symbol(symbol: str) -> str:
    """Normaliza símbolo de LBANK: GOOGLXUSDT -> GOOGLX"""
    s = str(symbol).strip().upper()
    # Quitar sufijos
    for suffix in ["USDT", "USDC", "USD"]:
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break
    return normalize_symbol(s)


def _dedupe_key_closed(row: dict) -> str:
    """Clave única para evitar duplicados"""
    base = f"{row.get('exchange','')}|{row.get('symbol','')}|{row.get('side','')}|{int(row.get('open_time') or 0)}|{int(row.get('close_time') or 0)}|{float(row.get('size') or 0):.8f}|{float(row.get('entry_price') or 0):.8f}|{float(row.get('close_price') or 0):.8f}"
    return hashlib.sha1(base.encode()).hexdigest()


# -----------------------------
# 1) FUTURES CSV -> funding_events
# -----------------------------
def parse_lbank_funding(file_bytes: bytes) -> List[dict]:
    """
    Parsea el CSV de Futures de LBANK.
    Formato: Symbol,asset,Time,Funding Type,Amount,Total
    Filtra solo Funding Type = "Funding fee"
    """
    df = pd.read_csv(io.BytesIO(file_bytes))
    df.columns = [str(c).strip() for c in df.columns]

    if "Funding Type" not in df.columns:
        raise ValueError("CSV Futures debe tener columna 'Funding Type'")

    # Filtrar solo Funding fee (case insensitive)
    df = df[
        df["Funding Type"].astype(str).str.strip().str.lower() == "funding fee"
    ].copy()

    out = []
    for _, r in df.iterrows():
        try:
            symbol_raw = str(r.get("Symbol", "")).strip()
            if not symbol_raw or symbol_raw == "nan":
                continue

            symbol = _normalize_lbank_symbol(symbol_raw)

            time_str = str(r.get("Time", "")).strip()
            dt_zurich = _parse_time_utc_to_zurich(time_str)
            ts_ms = _to_ms(dt_zurich)

            amount = _parse_number(r.get("Amount", 0))

            out.append(
                {
                    "exchange": LBANK,
                    "symbol": symbol,
                    "timestamp": ts_ms,
                    "income": amount,
                    "asset": "USDT",
                    "type": "FUNDING_FEE",
                }
            )
        except Exception as e:
            print(f"[LBANK] Error parseando funding row: {e}")
            continue

    return out


# -----------------------------
# 2) FILLED CSV -> FIFO de posiciones cerradas
# -----------------------------
@dataclass
class Lot:
    qty: float
    price: float
    fee: float
    ts_ms: int


@dataclass
class RoundTrip:
    symbol: str
    side: str
    size: float
    entry_price: float
    close_price: float
    open_ts_ms: int
    close_ts_ms: int
    pnl_price: float
    fees: float


@dataclass
class OpenPosition:
    """Posición abierta que acumula lots y cierres parciales"""

    side: str
    lots: List[Lot] = field(default_factory=list)
    # Acumuladores para cierres parciales
    total_closed_size: float = 0.0
    total_entry_notional: float = 0.0
    total_close_notional: float = 0.0
    total_fees: float = 0.0
    first_open_ts: int = 0
    last_close_ts: int = 0

    def add_lot(self, qty: float, price: float, fee: float, ts_ms: int):
        """Añade un lot de apertura"""
        self.lots.append(Lot(qty=qty, price=price, fee=fee, ts_ms=ts_ms))
        if self.first_open_ts == 0:
            self.first_open_ts = ts_ms
        # Fee de apertura se suma inmediatamente
        self.total_fees -= abs(fee)

    def close_qty(
        self, qty_to_close: float, close_price: float, close_fee: float, ts_ms: int
    ) -> float:
        """
        Cierra qty contra FIFO. Retorna la cantidad efectivamente cerrada.
        Acumula PnL y fees parciales.
        """
        remain = qty_to_close
        closed = 0.0

        while remain > 1e-12 and self.lots:
            lot = self.lots[0]
            take = min(remain, lot.qty)

            closed += take
            self.total_closed_size += take
            self.total_entry_notional += take * lot.price
            self.total_close_notional += take * close_price

            remain -= take
            lot.qty -= take
            if lot.qty < 1e-12:
                self.lots.pop(0)

        # Fee del cierre
        self.total_fees -= abs(close_fee)
        self.last_close_ts = ts_ms

        return closed

    def remaining_qty(self) -> float:
        return sum(lot.qty for lot in self.lots)

    def is_closed(self) -> bool:
        return self.remaining_qty() < 1e-12

    def to_roundtrip(self, symbol: str) -> RoundTrip:
        """Genera el RoundTrip final cuando la posición está completamente cerrada"""
        entry_avg = (
            self.total_entry_notional / self.total_closed_size
            if self.total_closed_size > 0
            else 0
        )
        close_avg = (
            self.total_close_notional / self.total_closed_size
            if self.total_closed_size > 0
            else 0
        )

        if self.side == "long":
            pnl_price = self.total_close_notional - self.total_entry_notional
        else:  # short
            pnl_price = self.total_entry_notional - self.total_close_notional

        return RoundTrip(
            symbol=symbol,
            side=self.side,
            size=self.total_closed_size,
            entry_price=entry_avg,
            close_price=close_avg,
            open_ts_ms=self.first_open_ts,
            close_ts_ms=self.last_close_ts,
            pnl_price=pnl_price,
            fees=self.total_fees,
        )


def parse_lbank_trades_fifo(file_bytes: bytes) -> List[RoundTrip]:
    """
    Parsea el CSV de Filled de LBANK.
    Formato: Time,Symbol,Direction,Price,Quantity,Fee,Realized P&L

    Direction puede ser:
    - Open Long / Close Long
    - Open Short / Close Short

    FIFO MEJORADO: Maneja cierres parciales intercalados con aperturas.
    Usa los lots FIFO para calcular el PnL de cada cierre parcial y acumula
    hasta que la posición neta llegue a 0.
    """
    df = pd.read_csv(io.BytesIO(file_bytes))
    df.columns = [c.strip() for c in df.columns]

    print(f"[LBANK] CSV columns: {list(df.columns)}")
    print(f"[LBANK] CSV rows: {len(df)}")

    # Parsear tiempo
    df["dt_zurich"] = df["Time"].apply(_parse_time_utc_to_zurich)
    df["ts_ms"] = df["dt_zurich"].apply(_to_ms)
    df["ts_s"] = (df["ts_ms"] // 1000).astype(int)

    # Normalizar símbolo
    df["symbol"] = df["Symbol"].apply(_normalize_lbank_symbol)

    # Parsear Direction
    df["direction"] = df["Direction"].astype(str).str.strip().str.lower()

    # Parsear números
    df["price"] = df["Price"].apply(_parse_number)
    df["qty"] = df["Quantity"].apply(_parse_number)
    df["fee"] = df["Fee"].apply(_parse_number)
    df["realized_pnl"] = df["Realized P&L"].apply(_parse_number)

    # Ordenar cronológicamente (ascendente)
    df = df.sort_values("ts_ms", ascending=True).reset_index(drop=True)

    print(f"[LBANK] Parsed {len(df)} trades")

    # Agrupar por símbolo
    grouped = df.groupby("symbol")

    all_roundtrips: List[RoundTrip] = []

    for symbol, grp in grouped:
        print(f"\n[LBANK] Processing {symbol}: {len(grp)} trades")

        # FIFO con lots
        lots = []  # Lista de (qty, price, fee, ts_ms)
        total_fees = 0.0
        first_open_ts = None

        # Acumuladores para el roundtrip
        total_entry_notional = 0.0
        total_close_notional = 0.0
        total_closed_size = 0.0
        total_pnl = 0.0
        side = None

        for _, row in grp.iterrows():
            direction = row["direction"]
            qty = row["qty"]
            price = row["price"]
            fee = row["fee"]
            ts_ms = row["ts_ms"]

            is_open = "open" in direction
            is_long = "long" in direction
            trade_side = "long" if is_long else "short"

            if is_open:
                # APERTURA - añadir lot
                if side is None:
                    side = trade_side
                    first_open_ts = ts_ms

                lots.append({"qty": qty, "price": price, "fee": fee, "ts_ms": ts_ms})
                total_fees -= abs(fee)  # Fee de apertura
                print(
                    f"  [OPEN] +{qty:.2f} @ {price:.4f} | lots={len(lots)} | net={sum(l['qty'] for l in lots):.2f}"
                )

            else:
                # CIERRE - consumir lots FIFO
                remain = qty
                close_fee = fee

                while remain > 1e-9 and lots:
                    lot = lots[0]
                    take = min(remain, lot["qty"])

                    # Calcular PnL para esta porción
                    entry_price = lot["price"]
                    if side == "short":
                        partial_pnl = (entry_price - price) * take
                    else:  # long
                        partial_pnl = (price - entry_price) * take

                    total_pnl += partial_pnl
                    total_entry_notional += take * entry_price
                    total_close_notional += take * price
                    total_closed_size += take

                    lot["qty"] -= take
                    remain -= take

                    if lot["qty"] < 1e-9:
                        lots.pop(0)

                total_fees -= abs(close_fee)  # Fee de cierre
                last_close_ts = ts_ms

                net_remaining = sum(l["qty"] for l in lots)
                print(
                    f"  [CLOSE] -{qty:.2f} @ {price:.4f} | pnl_partial={partial_pnl:.2f} | net={net_remaining:.2f}"
                )

                # Si la posición está completamente cerrada
                if net_remaining < 1e-9 and total_closed_size > 0:
                    entry_avg = (
                        total_entry_notional / total_closed_size
                        if total_closed_size > 0
                        else 0
                    )
                    close_avg = (
                        total_close_notional / total_closed_size
                        if total_closed_size > 0
                        else 0
                    )

                    rt = RoundTrip(
                        symbol=symbol,
                        side=side,
                        size=total_closed_size,
                        entry_price=entry_avg,
                        close_price=close_avg,
                        open_ts_ms=first_open_ts,
                        close_ts_ms=last_close_ts,
                        pnl_price=total_pnl,
                        fees=total_fees,
                    )
                    all_roundtrips.append(rt)
                    print(
                        f"  [DONE] RoundTrip: {side} size={total_closed_size:.2f} entry={entry_avg:.4f} close={close_avg:.4f} pnl={total_pnl:.2f} fees={total_fees:.4f}"
                    )

                    # Reset para siguiente posición
                    lots = []
                    total_fees = 0.0
                    first_open_ts = None
                    total_entry_notional = 0.0
                    total_close_notional = 0.0
                    total_closed_size = 0.0
                    total_pnl = 0.0
                    side = None

        # Si queda posición abierta
        if lots:
            net = sum(l["qty"] for l in lots)
            print(f"  [OPEN] Position still open: {net:.2f} {symbol}")

    print(f"\n[LBANK] Total roundtrips: {len(all_roundtrips)}")
    return all_roundtrips


def _sum_funding_between(
    funding_events: List[dict], symbol: str, open_s: int, close_s: int
) -> float:
    """Suma funding fees entre open_time y close_time para un símbolo."""
    total = 0.0
    for f in funding_events:
        if f["symbol"] != symbol:
            continue
        ts_s = f["timestamp"] // 1000
        if open_s <= ts_s <= close_s:
            total += f["income"]
    return total


# -----------------------------
# 3) MAIN: process_uploads
# -----------------------------
def process_uploads(
    exchange: str,
    product_type: str,
    futures_bytes: bytes | None,
    filled_bytes: bytes | None,
) -> dict:
    """
    Procesa los archivos CSV de LBANK.

    Args:
        exchange: Debe ser "lbank"
        product_type: "Futures position"
        futures_bytes: CSV de Futures (funding fees)
        filled_bytes: CSV de Filled (trades)

    Returns:
        dict con ok, mensajes, y estadísticas
    """
    if exchange.lower() != LBANK:
        return {
            "ok": False,
            "error": f"Exchange debe ser '{LBANK}', recibido: {exchange}",
        }

    if product_type != PRODUCT_FUTURES:
        return {"ok": False, "error": f"Product type debe ser '{PRODUCT_FUTURES}'"}

    # 1) Parsear funding (Futures)
    funding_events = []
    if futures_bytes:
        try:
            funding_events = parse_lbank_funding(futures_bytes)
            print(f"[LBANK] Parsed {len(funding_events)} funding events")
        except Exception as e:
            return {"ok": False, "error": f"Error parseando Futures CSV: {str(e)}"}

    # 2) Parsear trades (Filled)
    roundtrips = []
    if filled_bytes:
        try:
            roundtrips = parse_lbank_trades_fifo(filled_bytes)
            print(f"[LBANK] Parsed {len(roundtrips)} round trips (FIFO)")
        except Exception as e:
            import traceback

            traceback.print_exc()
            return {"ok": False, "error": f"Error parseando Filled CSV: {str(e)}"}

    # 3) Guardar funding en DB
    saved_funding = 0
    if funding_events:
        try:
            init_funding_db()
            upsert_funding_events(funding_events)
            saved_funding = len(funding_events)
            print(f"[LBANK] ✅ Guardados {saved_funding} funding events en DB")
        except Exception as e:
            return {"ok": False, "error": f"Error guardando funding: {str(e)}"}

    # 4) Guardar closed positions con funding asociado
    saved_positions = 0
    if roundtrips:
        try:
            for rt in roundtrips:
                open_s = rt.open_ts_ms // 1000
                close_s = rt.close_ts_ms // 1000

                # Calcular funding entre open y close
                funding_total = _sum_funding_between(
                    funding_events, rt.symbol, open_s, close_s
                )

                # Realized PnL = PnL precio + fees + funding
                realized_pnl = rt.pnl_price + rt.fees + funding_total

                pos_data = {
                    "exchange": LBANK,
                    "symbol": rt.symbol,
                    "side": rt.side,
                    "size": rt.size,
                    "entry_price": rt.entry_price,
                    "close_price": rt.close_price,
                    "open_time": open_s,
                    "close_time": close_s,
                    "pnl": rt.pnl_price,
                    "realized_pnl": realized_pnl,
                    "funding_total": funding_total,
                    "fee_total": rt.fees,
                    "notional": rt.size * rt.entry_price,
                    "leverage": None,
                    "liquidation_price": None,
                    "_lock_size": True,  # Evitar recálculo de size
                }

                save_closed_position(pos_data)
                saved_positions += 1

                print(
                    f"[LBANK] ✅ {rt.symbol} {rt.side} size={rt.size:.4f} "
                    f"entry={rt.entry_price:.5f} close={rt.close_price:.5f} "
                    f"pnl_price={rt.pnl_price:.2f} fees={rt.fees:.2f} "
                    f"funding={funding_total:.2f} realized={realized_pnl:.2f}"
                )

        except Exception as e:
            import traceback

            traceback.print_exc()
            return {"ok": False, "error": f"Error guardando closed positions: {str(e)}"}

    return {
        "ok": True,
        "message": f"LBANK import successful",
        "funding_saved": saved_funding,
        "positions_saved": saved_positions,
    }


# -----------------------------
# DEBUG / CLI
# -----------------------------
if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("LBANK Manual Import - Debug Mode")
    print("=" * 60)

    # Test con archivos locales
    filled_path = Path("Filled3.csv")
    futures_path = Path("futures.csv")

    if filled_path.exists():
        print(f"\n📄 Reading {filled_path}...")
        with open(filled_path, "rb") as f:
            filled_bytes = f.read()

        print("\n🔄 Parsing trades with FIFO...")
        roundtrips = parse_lbank_trades_fifo(filled_bytes)

        print("\n" + "=" * 60)
        print("ROUNDTRIPS SUMMARY:")
        print("=" * 60)
        for rt in roundtrips:
            print(f"\n{rt.symbol} | {rt.side.upper()}")
            print(f"  Size: {rt.size:.4f}")
            print(f"  Entry: {rt.entry_price:.6f}")
            print(f"  Close: {rt.close_price:.6f}")
            print(f"  PnL (price): {rt.pnl_price:.4f}")
            print(f"  Fees: {rt.fees:.4f}")

    if futures_path.exists():
        print(f"\n📄 Reading {futures_path}...")
        with open(futures_path, "rb") as f:
            futures_bytes = f.read()

        print("\n💸 Parsing funding fees...")
        funding = parse_lbank_funding(futures_bytes)

        print(f"\nFunding events: {len(funding)}")
        for f in funding[:10]:
            print(
                f"  {f['symbol']} | {f['income']:.4f} @ {datetime.fromtimestamp(f['timestamp']/1000)}"
            )

    # Guardar en DB si se pasa --save
    if "--save" in sys.argv and filled_path.exists():
        print("\n" + "=" * 60)
        print("💾 GUARDANDO EN BASE DE DATOS...")
        print("=" * 60)

        with open(filled_path, "rb") as f:
            filled_bytes = f.read()

        futures_bytes = None
        if futures_path.exists():
            with open(futures_path, "rb") as f:
                futures_bytes = f.read()

        result = process_uploads(
            exchange="lbank",
            product_type=PRODUCT_FUTURES,
            futures_bytes=futures_bytes,
            filled_bytes=filled_bytes,
        )

        print(f"\n✅ Result: {result}")
