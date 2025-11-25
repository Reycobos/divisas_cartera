# kcex_manual.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
import pandas as pd
import io
import hashlib
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

TZ_SRC = ZoneInfo("Asia/Shanghai")  # UTC+8 de los CSVs
TZ_APP = ZoneInfo("Europe/Zurich")  # hora local app

KCEX = "kcex"
PRODUCT_FUTURES = "Futures position"


# -----------------------------
# Helpers
# -----------------------------
def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _to_s(dt: datetime) -> int:
    return int(dt.timestamp())


def _parse_time_utc8_to_zurich(s: str | pd.Timestamp) -> datetime:
    """
    Parsea tiempo de KCEX CSV.
    El CSV muestra la hora en UTC+8 pero queremos verla en Zurich (UTC+1).
    
    Si el CSV dice 15:57 y debería ser 22:57 en Zurich, entonces
    el CSV ya está en UTC+1 (o el usuario quiere +7h).
    
    Ajuste: Sumar 7 horas para convertir de lo que muestra el CSV a Zurich.
    """
    ts = pd.to_datetime(s, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"Invalid timestamp: {s}")
    # Sumar 7 horas para ajustar a Zurich
    ts_adjusted = ts + pd.Timedelta(hours=7)
    # Localizar como Zurich
    if ts_adjusted.tzinfo is None:
        ts_adjusted = ts_adjusted.tz_localize(TZ_APP)
    return ts_adjusted.to_pydatetime()


def _dedupe_key_closed(row: dict) -> str:
    """Clave única para evitar duplicados"""
    base = f"{row.get('exchange','')}|{row.get('symbol','')}|{row.get('side','')}|{int(row.get('open_time') or 0)}|{int(row.get('close_time') or 0)}|{float(row.get('size') or 0):.8f}|{float(row.get('entry_price') or 0):.8f}|{float(row.get('close_price') or 0):.8f}"
    return hashlib.sha1(base.encode()).hexdigest()


# -----------------------------
# 1) CAPITAL FLOW -> funding_events
# -----------------------------
def parse_kcex_capital_flow(file_bytes: bytes) -> List[dict]:
    """
    Parsea el CSV de Capital Flow de KCEX.
    Formato: Perpetual,Time,Coin,Type,Amount
    Filtra solo Type = "Funding Fee"
    """
    df = pd.read_csv(io.BytesIO(file_bytes))
    df.columns = [str(c).strip() for c in df.columns]

    if "Type" not in df.columns:
        raise ValueError("CSV Capital Flow debe tener columna 'Type'")

    # Filtrar solo Funding Fee
    df = df[df["Type"].astype(str).str.strip() == "Funding Fee"].copy()

    out = []
    for _, r in df.iterrows():
        try:
            perpetual = str(r.get("Perpetual", "")).strip()
            if not perpetual or perpetual == "nan":
                continue

            symbol_raw = perpetual.replace("_USDT", "").replace("_", "")
            symbol = normalize_symbol(symbol_raw)

            time_str = str(r.get("Time", "")).strip()
            dt_zurich = _parse_time_utc8_to_zurich(time_str)
            ts_ms = _to_ms(dt_zurich)

            amount = float(r.get("Amount", 0))

            out.append(
                {
                    "exchange": KCEX,
                    "symbol": symbol,
                    "timestamp": ts_ms,
                    "income": amount,
                    "asset": "USDT",
                    "type": "FUNDING_FEE",
                }
            )
        except Exception as e:
            print(f"[KCEX] Error parseando funding row: {e}")
            continue

    return out


# -----------------------------
# 2) TRADE HISTORY -> FIFO de posiciones cerradas
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
        # Guardar el lot con su fee original (se usará cuando se cierre)
        self.lots.append(Lot(qty=qty, price=price, fee=fee, ts_ms=ts_ms))
        if self.first_open_ts == 0:
            self.first_open_ts = ts_ms
        # NO agregamos fee aquí, se agrega proporcional al cerrar

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
            original_lot_qty = lot.qty + (
                self.total_closed_size
                if lot == self.lots[0] and len(self.lots) == 1
                else 0
            )
            take = min(remain, lot.qty)

            closed += take
            self.total_closed_size += take
            self.total_entry_notional += take * lot.price
            self.total_close_notional += take * close_price

            # Fee de apertura proporcional a la cantidad cerrada de este lot
            # Usamos el qty original del lot (antes de cualquier cierre parcial)
            # Para esto necesitamos trackear el qty original

            remain -= take
            lot.qty -= take
            if lot.qty < 1e-12:
                # Lot completamente cerrado, agregar todo su fee de apertura
                self.total_fees -= abs(lot.fee)
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


def parse_kcex_trades_fifo(file_bytes: bytes) -> List[RoundTrip]:
    """
    Parsea el CSV de Trade History de KCEX.
    Formato: Futures,Time,Direction,Amount,Order Price,Fee,Role,Closing PNL

    Direction puede ser:
    - Open Long / Close Long
    - Open Short / Close Short

    FIFO: Acumula cierres parciales hasta que la posición se cierra completamente.
    """
    df = pd.read_csv(io.BytesIO(file_bytes))
    df.columns = [c.strip() for c in df.columns]

    # Parsear tiempo
    tloc = df["Time"].apply(_parse_time_utc8_to_zurich)
    df["ts_ms"] = tloc.apply(_to_ms)
    df["ts_s"] = (df["ts_ms"] // 1000).astype(int)

    # Normalizar símbolo
    df["symbol"] = (
        df["Futures"]
        .astype(str)
        .str.replace(" USDT", "")
        .str.replace(" ", "")
        .apply(normalize_symbol)
    )

    # Parsear Direction
    df["direction"] = df["Direction"].astype(str).str.strip()

    # Parsear Amount (ejemplo: "9000 TNSR" -> 9000)
    def parse_amount(s):
        try:
            parts = str(s).strip().split()
            return float(parts[0].replace(",", "")) if parts else 0.0
        except:
            return 0.0

    df["qty"] = df["Amount"].apply(parse_amount)
    df["price"] = pd.to_numeric(df["Order Price"], errors="coerce").fillna(0)

    # Parsear Fee (ejemplo: "0.1312 USDT" -> 0.1312)
    def parse_fee(s):
        try:
            parts = str(s).strip().split()
            return float(parts[0]) if parts else 0.0
        except:
            return 0.0

    df["fee"] = df["Fee"].apply(parse_fee)

    # Ordenar por tiempo ascendente
    df = df.sort_values("ts_ms").reset_index(drop=True)

    # Posiciones abiertas por símbolo y lado
    long_positions: Dict[str, OpenPosition] = {}
    short_positions: Dict[str, OpenPosition] = {}
    roundtrips = []

    for _, r in df.iterrows():
        sym = r["symbol"]
        direction = r["direction"]
        qty = r["qty"]
        price = r["price"]
        fee = r["fee"]
        ts_ms = r["ts_ms"]

        if "Open Long" in direction:
            if sym not in long_positions:
                long_positions[sym] = OpenPosition(side="long")
            long_positions[sym].add_lot(qty, price, fee, ts_ms)

        elif "Close Long" in direction:
            if sym in long_positions:
                pos = long_positions[sym]
                pos.close_qty(qty, price, fee, ts_ms)

                # Si la posición está completamente cerrada, crear RoundTrip
                if pos.is_closed():
                    roundtrips.append(pos.to_roundtrip(sym))
                    del long_positions[sym]

        elif "Open Short" in direction:
            if sym not in short_positions:
                short_positions[sym] = OpenPosition(side="short")
            short_positions[sym].add_lot(qty, price, fee, ts_ms)

        elif "Close Short" in direction:
            if sym in short_positions:
                pos = short_positions[sym]
                pos.close_qty(qty, price, fee, ts_ms)

                # Si la posición está completamente cerrada, crear RoundTrip
                if pos.is_closed():
                    roundtrips.append(pos.to_roundtrip(sym))
                    del short_positions[sym]

    return roundtrips


# -----------------------------
# 3) Composición: funding + closed positions
# -----------------------------
def _sum_funding_between(
    funding_events: List[dict], symbol: str, t0_s: int, t1_s: int
) -> float:
    """Suma funding fees entre dos timestamps (segundos)"""
    total = 0.0
    for f in funding_events:
        if f["symbol"] != symbol:
            continue
        f_ts_s = f["timestamp"] // 1000
        if t0_s <= f_ts_s <= t1_s:
            total += f["income"]
    return total


def process_uploads(
    exchange: str,
    product_type: str,
    capital_flow_bytes: Optional[bytes],
    trade_history_bytes: Optional[bytes],
) -> dict:
    """
    Procesa los archivos CSV de KCEX.

    Args:
        exchange: Debe ser "kcex"
        product_type: "Futures position"
        capital_flow_bytes: CSV de Capital Flow (funding fees)
        trade_history_bytes: CSV de Trade History (trades)

    Returns:
        dict con ok, mensajes, y estadísticas
    """
    if exchange.lower() != KCEX:
        return {
            "ok": False,
            "error": f"Exchange debe ser '{KCEX}', recibido: {exchange}",
        }

    if product_type != PRODUCT_FUTURES:
        return {"ok": False, "error": f"Product type debe ser '{PRODUCT_FUTURES}'"}

    # 1) Parsear funding (Capital Flow)
    funding_events = []
    if capital_flow_bytes:
        try:
            funding_events = parse_kcex_capital_flow(capital_flow_bytes)
            print(f"[KCEX] Parsed {len(funding_events)} funding events")
        except Exception as e:
            return {"ok": False, "error": f"Error parseando Capital Flow: {str(e)}"}

    # 2) Parsear trades (Trade History)
    roundtrips = []
    if trade_history_bytes:
        try:
            roundtrips = parse_kcex_trades_fifo(trade_history_bytes)
            print(f"[KCEX] Parsed {len(roundtrips)} round trips (FIFO)")
        except Exception as e:
            return {"ok": False, "error": f"Error parseando Trade History: {str(e)}"}

    # 3) Guardar funding en DB
    saved_funding = 0
    if funding_events:
        try:
            init_funding_db()
            upsert_funding_events(funding_events)
            saved_funding = len(funding_events)
            print(f"[KCEX] ✅ Guardados {saved_funding} funding events en DB")
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
                    "exchange": KCEX,
                    "symbol": rt.symbol,
                    "side": rt.side,
                    "size": rt.size,
                    "entry_price": rt.entry_price,
                    "close_price": rt.close_price,
                    "open_time": open_s,
                    "close_time": close_s,
                    "realized_pnl": realized_pnl,
                    "funding_total": funding_total,
                    "fee_total": rt.fees,
                    "notional": rt.size * rt.entry_price,
                    "leverage": None,
                    "liquidation_price": None,
                }

                save_closed_position(pos_data)
                saved_positions += 1

                print(
                    f"[KCEX] ✅ {rt.symbol} {rt.side} size={rt.size:.4f} "
                    f"entry={rt.entry_price:.5f} close={rt.close_price:.5f} "
                    f"pnl_price={rt.pnl_price:.2f} fees={rt.fees:.2f} "
                    f"funding={funding_total:.2f} realized={realized_pnl:.2f}"
                )

        except Exception as e:
            return {"ok": False, "error": f"Error guardando closed positions: {str(e)}"}

    return {
        "ok": True,
        "message": f"KCEX import successful",
        "funding_saved": saved_funding,
        "positions_saved": saved_positions,
    }
