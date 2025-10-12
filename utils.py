# utils.py
from __future__ import annotations
import re, time
from decimal import Decimal, getcontext, ROUND_HALF_EVEN
from typing import Tuple, Optional

# Precisión alta para cálculos financieros
getcontext().prec = 28

# ---- Money / Decimal ---------------------------------------------------------
def D(x) -> Decimal:
    """Convierte a Decimal de forma robusta (None, '', 0 -> 0)."""
    return Decimal(str(x if x not in (None, "",) else 0))

def usd(x) -> Decimal:
    """Redondea a 2 decimales HALF_EVEN (contable)."""
    return D(x).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)

def quant(x, places: int = 6) -> Decimal:
    """Redondeo genérico a 'places' decimales."""
    q = Decimal(10) ** -places
    return D(x).quantize(q, rounding=ROUND_HALF_EVEN)

def to_float(x: Decimal | float | int) -> float:
    return float(D(x))

def normalize_fee(x) -> Decimal:
    """Asegura que la fee sea NEGATIVA (costo)."""
    return -abs(D(x))

def safe_div(n, d, default=Decimal(0)) -> Decimal:
    d = D(d)
    return D(n) / d if d != 0 else D(default)

# ---- Tiempo / timestamps -----------------------------------------------------
def utc_now_ms() -> int:
    return int(time.time() * 1000)

def to_ms(ts: int | float | str) -> int:
    """Normaliza epoch a ms (acepta s o ms)."""
    t = int(float(ts))
    return t if t >= 10**12 else t * 1000

def to_s(ts: int | float | str) -> int:
    """Normaliza epoch a s (acepta s o ms)."""
    t = int(float(ts))
    return t // 1000 if t >= 10**12 else t

# ---- Símbolos / limpieza -----------------------------------------------------
SYMBOL_SPECIAL_CASES = {
    # añade aquí excepciones del exchange si aparecen
    "BTCUSD.P": "BTC",
    "BTC-PERP": "BTC",
    "ETH-PERP": "ETH",
}

SYM_SUFFIX_RE = re.compile(r'(_|-)?(USDT|USDC|USD|PERP)$', re.IGNORECASE)

def normalize_symbol(sym: str) -> str:
    """Devuelve el ticker base (e.g., 'BTC', 'ETH') limpiando sufijos y separadores."""
    if not sym:
        return ""
    if sym in SYMBOL_SPECIAL_CASES:
        return SYMBOL_SPECIAL_CASES[sym]
    s = sym.upper()
    s = re.sub(r'^PERP_', '', s)           # quita prefijo PERP_
    s = SYM_SUFFIX_RE.sub('', s)           # quita sufijos + separador
    s = re.sub(r'[_-]+$', '', s)           # guiones finales
    s = re.split(r'[_/-]', s)[0]           # primera parte si quedan separadores
    return s

def side_from_qty(qty) -> str:
    """Inferir 'long'/'short' desde el signo de la cantidad."""
    return "long" if D(qty) >= 0 else "short"

# ---- PnL / métricas ----------------------------------------------------------
def pnl_price(side: str, entry_price, price, size) -> Decimal:
    """
    PnL de precio (sin fees/funding) para contratos lineales.
    side='long'  -> (price - entry) * abs(size)
    side='short' -> (entry - price) * abs(size)
    """
    e, p, q = D(entry_price), D(price), abs(D(size))
    diff = (p - e) if side == "long" else (e - p)
    return diff * q

def realized_pnl_closed(side: str, entry_price, close_price, size) -> Decimal:
    """Realizado al cerrar (solo precio)."""
    return pnl_price(side, entry_price, close_price, size)

def realized_pnl_open(fee_total, funding_total) -> Decimal:
    """Para OPEN: realized = fees + funding (regla del front)."""
    return D(fee_total) + D(funding_total)

def notional(entry_price, size) -> Decimal:
    """Valor nocional aproximado (lineal): entry * |size|."""
    return D(entry_price) * abs(D(size))

# ---- Funding normalization ---------------------------------------------------
def funding_rate_to_daily_and_apr(rate_per_interval, interval_hours: int) -> Tuple[Decimal, Decimal]:
    """
    Convierte una funding rate por intervalo (e.g. 8h) a:
    - daily_rate (aprox): rate * (24 / interval_hours)
    - apr (aprox): daily_rate * 365
    """
    r = D(rate_per_interval)
    daily = r * D(24) / D(interval_hours or 1)
    apr = daily * D(365)
    return daily, apr
