# adapters/xt_spot_trades.py
# -*- coding: utf-8 -*-
"""
XT.com — Spot trades → closed positions (FIFO)

Qué hace
--------
- Descarga fills de /v4/trade para bizType=SPOT
- Detecta swaps USDT<->USDC y los guarda como side = "swapstable"
- Para el resto de tokens, calcula PnL con FIFO, agregando rondas
- Si el primer fill de un símbolo es un SELL, lo guarda con ignore_trade=1
- Si quedan compras sin vender y el token NO existe en balances spot,
  se considera retirada → ignore_trade=1

Dependencias
------------
- adapters/xt.py  → _get_spot(), normalize_symbol(), to_float()
- db_manager.py   → para insertar en closed_positions

Uso
---
from adapters.xt_spot_trades import save_xt_spot_positions
save_xt_spot_positions(db_path="portfolio.db", days_back=30)
"""

from __future__ import annotations
import os
import sys
import time
import sqlite3
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional

# === Path utils ===
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UTILS_DIR = os.path.join(BASE_DIR, 'utils')
if UTILS_DIR not in sys.path:
    sys.path.append(UTILS_DIR)

# === XT helpers (reutilizar del adapter principal) ===
try:
    from adapters.xt import (
        _get_spot, normalize_symbol, to_float, to_int, 
        _unwrap_result, _get_spot_prices, XT_SAPI_HOST
    )
except ImportError:
    # Si se ejecuta como script desde /adapters
    from xt import (
        _get_spot, normalize_symbol, to_float, to_int,
        _unwrap_result, _get_spot_prices, XT_SAPI_HOST
    )

DB_PATH_DEFAULT = os.path.join(BASE_DIR, 'portfolio.db')

# ---------- Constants ----------
STABLES = {"USDT", "USDC", "BUSD", "DAI"}
IGNORE_BASES = {"BTC", "ETH"}  # opcional: ignorar trades de BTC/ETH
DUST_RATIO = 0.01  # 1% del pico para considerar "polvo"


def _num(x: Any, d: float = 0.0) -> float:
    """Convierte a float de forma segura"""
    try:
        return float(x)
    except Exception:
        return d


def _split_symbol(symbol: str) -> Tuple[str, str]:
    """
    Convierte 'btc_usdt' -> (base='BTC', quote='USDT')
    """
    s = (symbol or '').replace('/', '_').lower()
    parts = s.split('_')
    if len(parts) >= 2:
        return parts[0].upper(), parts[1].upper()
    return s.upper(), ''


def _fmt_ts(ts: int) -> str:
    """Convierte timestamp en segundos a formato legible"""
    from datetime import datetime, timezone
    try:
        if ts > 1_000_000_000_000:  # está en ms
            ts = ts // 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts)


@dataclass
class Fill:
    """Representa un fill de spot trade"""
    ts: int           # epoch seconds
    symbol: str       # e.g., "btc_usdt"
    side: str         # buy|sell
    amount: float     # base amount
    price: float      # quote per base
    fee: float        # fee amount
    fee_ccy: str      # currency of fee
    order_id: str     # ID de la orden

    @property
    def base_quote(self) -> Tuple[str, str]:
        return _split_symbol(self.symbol)

    def fee_in_quote(self) -> float:
        """Convierte la fee a QUOTE"""
        base, quote = self.base_quote
        if self.fee_ccy.upper() == quote:
            return self.fee
        if self.fee_ccy.upper() == base:
            return self.fee * self.price
        return self.fee


@dataclass
class RoundAgg:
    """Acumulador de una ronda FIFO (compras + ventas)"""
    qty_in: float = 0.0
    cost: float = 0.0
    qty_out: float = 0.0
    proceeds: float = 0.0
    fee_buy: float = 0.0
    fee_sell: float = 0.0
    ts_open: int = 0
    ts_close: int = 0

    def merge_buy(self, qty: float, px: float, fee: float, ts: int):
        self.qty_in += qty
        self.cost += qty * px
        self.fee_buy += fee
        if self.ts_open == 0 or ts < self.ts_open:
            self.ts_open = ts

    def merge_sell(self, qty: float, px: float, fee: float, ts: int):
        self.qty_out += qty
        self.proceeds += qty * px
        self.fee_sell += fee
        if self.ts_close == 0 or ts > self.ts_close:
            self.ts_close = ts

    def is_valid(self) -> bool:
        """Una ronda es válida si tiene ventas"""
        return self.qty_out > 1e-12

    def finalize(self) -> Dict[str, Any]:
        """Calcula PnL final de la ronda"""
        avg_buy = self.cost / max(self.qty_in, 1e-12)
        avg_sell = self.proceeds / max(self.qty_out, 1e-12)
        qty_matched = min(self.qty_in, self.qty_out)
        
        pnl = self.proceeds - (qty_matched * avg_buy)
        fee_total = self.fee_buy + self.fee_sell
        realized = pnl - fee_total
        
        return {
            'size': qty_matched,
            'entry_price': avg_buy,
            'close_price': avg_sell,
            'pnl': pnl,
            'realized_pnl': realized,
            'fee_total': -abs(fee_total),
            'open_time': self.ts_open,
            'close_time': self.ts_close,
            'notional': self.cost,
        }


def get_existing_trade_hashes(db_path: str) -> set:
    """Obtiene hashes de trades ya existentes en DB - MEJORADO"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
SELECT exchange, symbol, side, open_time, close_time, size, entry_price, close_price
FROM closed_positions 
WHERE exchange = 'xt'
""")
        existing_trades = cursor.fetchall()
        
        hashes = set()
        for trade in existing_trades:
            exchange, symbol, side, open_time, close_time, size, entry_price, close_price = trade
            # Hash más específico que incluye todos los campos clave
            trade_hash = (
                f"{exchange}_{symbol}_{side}_{open_time}_{close_time}_"
                f"{round(size, 8)}_{round(entry_price, 8)}_{round(close_price, 8)}"
            )
            hashes.add(trade_hash)
            
        return hashes
    finally:
        conn.close()

def _position_hash(row: Dict[str, Any]) -> str:
    """Genera hash único para una posición basado en campos clave"""
    return (
        f"{row.get('exchange', 'xt')}_{row.get('symbol', '')}_{row.get('side', '')}_"
        f"{row.get('open_time', 0)}_{row.get('close_time', 0)}_"
        f"{round(row.get('size', 0), 8)}_{round(row.get('entry_price', 0), 8)}_"
        f"{round(row.get('close_price', 0), 8)}"
    )



def fetch_xt_spot_trades(
    days_back: int = 30,
    limit: int = 100,
    existing_hashes: set = None,
    debug: bool = False
) -> List[Fill]:
    """
    Descarga spot trades de XT usando el endpoint /v4/trade
    """
    if existing_hashes is None:
        existing_hashes = set()
    
    cli = _get_spot()
    all_fills = []
    
    if debug:
        print(f"📥 Descargando trades spot (últimos {days_back} días)")
    
    try:
        # Parámetros para la API
        params = {
            'bizType': 'SPOT',
            'limit': limit,
        }
        
        # Calcular timestamps
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - (days_back * 24 * 3600 * 1000)
        params['startTime'] = start_ms
        params['endTime'] = now_ms
        
        if debug:
            print(f"  🔄 Solicitando trades desde {_fmt_ts(start_ms//1000)}")
        
        # Usar req_get para acceder directamente al endpoint /v4/trade
        result = cli.req_get('/v4/trade', params=params)
        
        if debug:
            print(f"  📊 Respuesta recibida, procesando...")
        
        # Procesar la respuesta - los trades están en result['result']['items']
        if isinstance(result, dict) and 'result' in result:
            result_data = result['result']
            if isinstance(result_data, dict) and 'items' in result_data:
                items = result_data['items']
                if isinstance(items, list):
                    all_fills.extend(_process_trade_data(items, existing_hashes))
                    if debug:
                        print(f"  ✅ Procesados {len(items)} trades del endpoint /v4/trade")
                else:
                    if debug:
                        print(f"  ⚠️ 'items' no es una lista: {type(items)}")
            else:
                if debug:
                    print(f"  ⚠️ No se encontró 'items' en result: {list(result_data.keys()) if isinstance(result_data, dict) else type(result_data)}")
        else:
            if debug:
                print(f"  ⚠️ No se encontró 'result' en la respuesta")
            
    except Exception as e:
        print(f"⚠️ Error obteniendo trades spot: {e}")
        import traceback
        traceback.print_exc()
    
    if debug:
        print(f"✅ Total descargado: {len(all_fills)} trades nuevos")
    
    return all_fills


def _process_trade_data(trade_data: List[Dict], existing_hashes: set) -> List[Fill]:
    """Procesa los datos de trade del endpoint /v4/trade"""
    fills = []
    
    for item in trade_data:
        if not isinstance(item, dict):
            continue
            
        # Mapear campos según la estructura de /v4/trade
        symbol = (item.get('symbol') or '').lower()
        side = (item.get('orderSide') or '').lower()
        price = _num(item.get('price'))
        amount = _num(item.get('quantity'))
        fee = _num(item.get('fee'))
        fee_ccy = (item.get('feeCurrency') or '').upper()
        order_id = str(item.get('orderId') or item.get('tradeId') or '')
        
        # Timestamp
        ts_ms = _num(item.get('time'), 0)
        ts = int(ts_ms / 1000) if ts_ms > 1_000_000_000_000 else int(ts_ms)
        
        # Validar campos requeridos
        if not symbol or not side or price <= 0 or amount <= 0:
            continue
        
        # Crear hash único
        trade_hash = f"xt_{symbol}_{side}_{ts}_{round(amount, 8)}"
        
        if trade_hash not in existing_hashes:
            fills.append(Fill(
                ts=ts,
                symbol=symbol,
                side=side,
                amount=amount,
                price=price,
                fee=fee,
                fee_ccy=fee_ccy,
                order_id=order_id
            ))
            # if debug and len(fills) <= 5:  # Mostrar primeros 5 trades como ejemplo
            #     print(f"    📝 Trade {len(fills)}: {symbol} {side} {amount} @ {price}")
    
    return fills

def _insert_row(conn: sqlite3.Connection, row: Dict[str, Any], existing_hashes: set) -> bool:
    """
    Inserta una fila en closed_positions solo si no existe
    Returns: True si se insertó, False si ya existía
    """
    position_hash = _position_hash(row)
    
    # Verificar si ya existe
    if position_hash in existing_hashes:
        print(f"🔍 [DEBUG] Posición ya existe, omitiendo: {position_hash}")
        return False
    
    try:
        from db_manager import save_closed_position
        # Adaptar el formato para db_manager
        position_data = {
            'exchange': row.get('exchange', 'xt'),
            'symbol': row.get('symbol', ''),
            'side': row.get('side', 'spotbuy'),
            'size': row.get('size', 0),
            'entry_price': row.get('entry_price', 0),
            'close_price': row.get('close_price', 0),
            'open_time': row.get('open_time', 0),
            'close_time': row.get('close_time', 0),
            'pnl': row.get('pnl', 0),
            'realized_pnl': row.get('realized_pnl', 0),
            'funding_total': 0.0,  # Spot no tiene funding
            'fee_total': row.get('fee_total', 0),
            'notional': row.get('notional', 0),
            'leverage': 1.0,  # Spot siempre es leverage 1
            'liquidation_price': 0.0,  # Spot no tiene liquidation
            'initial_margin': row.get('notional', 0),  # Para spot, initial_margin = notional
            'ignore_trade': row.get('ignore_trade', 0)
        }
        print(f"💾 [DEBUG] Guardando nueva posición: {position_hash}")
        save_closed_position(position_data)
        # Actualizar el set de hashes existentes
        existing_hashes.add(position_hash)
        return True
        
    except ImportError:
        # Fallback si db_manager no está disponible
        cursor = conn.cursor()
        try:
            cursor.execute("""
INSERT INTO closed_positions (
    exchange, symbol, side, size, entry_price, close_price,
    pnl, realized_pnl, fee_total, open_time, close_time,
    notional, ignore_trade, funding_total, leverage, liquidation_price, initial_margin
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", (
                row.get('exchange'),
                row.get('symbol'),
                row.get('side'),
                row.get('size', 0),
                row.get('entry_price', 0),
                row.get('close_price', 0),
                row.get('pnl', 0),
                row.get('realized_pnl', 0),
                row.get('fee_total', 0),
                row.get('open_time', 0),
                row.get('close_time', 0),
                row.get('notional', 0),
                row.get('ignore_trade', 0),
                0.0,  # funding_total
                1.0,  # leverage  
                0.0,  # liquidation_price
                row.get('notional', 0)  # initial_margin
            ))
            print(f"💾 [DEBUG] Guardando nueva posición (fallback): {position_hash}")
            # Actualizar el set de hashes existentes
            existing_hashes.add(position_hash)
            return True
        except sqlite3.IntegrityError:
            # Si por alguna razón aún falla, ignorar silenciosamente
            print(f"⚠️ [DEBUG] IntegrityError al insertar (ya existe): {position_hash}")
            return False


def save_xt_spot_positions(
    db_path: str = DB_PATH_DEFAULT,
    days_back: int = 30,
    debug: bool = False
) -> Tuple[int, int]:
    """
    Descarga spot trades de XT y guarda posiciones cerradas usando FIFO.
    
    Returns:
        (saved, ignored): número de posiciones guardadas e ignoradas
    """
    # 1) Verificar DB
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"❌ DB no existe: {db_path}")
    
    # 2) Obtener trades existentes - MEJORADO
    existing_hashes = get_existing_trade_hashes(db_path)
    if debug:
        print(f"📊 Posiciones existentes en DB: {len(existing_hashes)}")
    
    # 3) Descargar nuevos trades
    fills = fetch_xt_spot_trades(
        days_back=days_back,
        limit=100,
        existing_hashes=existing_hashes,  # Pasar existing_hashes para evitar duplicados en fills
        debug=debug
    )
    
    if not fills:
        if debug:
            print("ℹ️ No hay trades nuevos para procesar")
        return 0, 0
    
    # 4) Obtener balances spot actuales (opcional)
    spot_have = {}
    if debug:
        print("💰 Omisión de balances - usar adapter principal para balances")
    
    # 5) Agrupar por símbolo
    by_symbol = defaultdict(list)
    for f in fills:
        by_symbol[f.symbol].append(f)
    
    if debug:
        print(f"📊 Procesando {len(by_symbol)} símbolos diferentes")
    
    # 6) Procesar FIFO por símbolo
    conn = sqlite3.connect(db_path)
    saved = 0
    ignored = 0
    
    for symbol, trades in by_symbol.items():
        base, quote = _split_symbol(symbol)
        
        # Ignorar BTC/ETH si está en IGNORE_BASES
        if base in IGNORE_BASES or quote in IGNORE_BASES:
            continue
        
        # -------- 1) Detectar swaps stables --------
        if base in STABLES and quote in STABLES:
            for f in trades:
                fee_q = f.fee_in_quote()
                
                # En swaps 1:1, el PnL es básicamente la diferencia - fees
                net_base_out = f.amount
                received_quote = f.amount * f.price
                price_pnl = received_quote - net_base_out
                realized = price_pnl - fee_q
                
                row = {
                    'exchange': 'xt',
                    'symbol': f"{base}{quote}",
                    'side': 'swapstable',
                    'size': abs(net_base_out),
                    'entry_price': 1.0,
                    'close_price': 1.0,
                    'pnl': price_pnl,
                    'realized_pnl': realized,
                    'fee_total': -abs(fee_q),
                    'open_time': f.ts,
                    'close_time': f.ts,
                    'notional': max(received_quote, net_base_out),
                    'ignore_trade': 0,
                }
                if _insert_row(conn, row, existing_hashes):  # Pasar existing_hashes
                    saved += 1
            continue
        
        # -------- 2) Tokens normales → FIFO --------
        trades.sort(key=lambda x: x.ts)
        
        # Si primeros fills son SELL → ignorar (depósito)
        idx = 0
        while idx < len(trades) and trades[idx].side == 'sell':
            f = trades[idx]
            fee_q = f.fee_in_quote()
            row = {
                'exchange': 'xt',
                'symbol': normalize_symbol(f"{base}{quote}"),
                'side': 'spotsell',
                'size': abs(f.amount),
                'entry_price': f.price,
                'close_price': f.price,
                'pnl': 0.0,
                'realized_pnl': 0.0,
                'fee_total': -abs(fee_q),
                'open_time': f.ts,
                'close_time': f.ts,
                'notional': abs(f.amount) * f.price,
                'ignore_trade': 1,
            }
            if _insert_row(conn, row, existing_hashes):  # Pasar existing_hashes
                ignored += 1
            idx += 1
        
        # Estado FIFO
        lot_q = deque()  # (qty, price, fee_per_unit, ts)
        round_agg = RoundAgg()
        round_started = False
        total_qty_in_round = 0.0
        
        inventory_base = 0.0
        peak_inventory_base = 0.0
        sells_occurred = False
        
        def _flush_round():
            nonlocal saved, round_agg, round_started, total_qty_in_round, peak_inventory_base
            if not round_started:
                return
            if not round_agg.is_valid():
                round_agg = RoundAgg()
                round_started = False
                total_qty_in_round = 0.0
                peak_inventory_base = 0.0
                return
            
            data = round_agg.finalize()
            data['size'] = peak_inventory_base
            
            row = {
                'exchange': 'xt',
                'symbol': normalize_symbol(f"{base}{quote}"),
                'side': 'spotbuy',
                'ignore_trade': 0,
                **data,
            }
            if _insert_row(conn, row, existing_hashes):  # Pasar existing_hashes
                saved += 1
            
            # reset
            round_agg = RoundAgg()
            round_started = False
            total_qty_in_round = 0.0
            peak_inventory_base = 0.0
        
        # Procesar fills
        for f in trades[idx:]:
            if f.side == 'buy':
                round_started = True
                fee_q = f.fee_in_quote()
                
                # Cantidad real recibida
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
                
                # Match FIFO
                while sell_left > 1e-12 and lot_q:
                    q, p, fee_u, tsb = lot_q[0]
                    take = min(q, sell_left)
                    
                    round_agg.merge_sell(
                        take,
                        f.price,
                        fee_q * (take / sell_qty) if sell_qty > 0 else 0.0,
                        f.ts
                    )
                    
                    q -= take
                    sell_left -= take
                    
                    if q <= 1e-12:
                        lot_q.popleft()
                    else:
                        lot_q[0][0] = q
                
                inventory_base = sum(q for q, *_ in lot_q)
                
                # Criterio de cierre
                dust = max(0.01, DUST_RATIO * peak_inventory_base)
                if (inventory_base <= dust and total_qty_in_round >= 100) or (not lot_q and sell_left <= 1e-12):
                    _flush_round()
        
        # Al terminar símbolo, verificar remanente
        if lot_q:
            rem_base = sum(q for q, *_ in lot_q)
            dust = max(0.01, DUST_RATIO * peak_inventory_base)
            
            if sells_occurred and rem_base <= dust:
                _flush_round()
            else:
                bal_base = spot_have.get(base, 0.0)
                if (not sells_occurred) or (rem_base > dust and bal_base < rem_base * 0.5):
                    # Retiro ignorado
                    notional = sum(q * p for q, p, *_ in lot_q)
                    ts_open = min(ts for *_a, ts in lot_q)
                    row = {
                        'exchange': 'xt',
                        'symbol': normalize_symbol(f"{base}{quote}"),
                        'side': 'spotbuy',
                        'size': rem_base,
                        'entry_price': notional / max(rem_base, 1e-12),
                        'close_price': notional / max(rem_base, 1e-12),
                        'pnl': 0.0,
                        'realized_pnl': 0.0,
                        'fee_total': 0.0,
                        'open_time': ts_open,
                        'close_time': ts_open,
                        'notional': notional,
                        'ignore_trade': 1,
                    }
                    if _insert_row(conn, row, existing_hashes):  # Pasar existing_hashes
                        ignored += 1
    
    conn.commit()
    conn.close()
    
    if debug:
        print(f"✅ Spot FIFO XT: guardadas={saved}, ignoradas={ignored}")
    
    return saved, ignored


# ---------- CLI ----------
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='XT.com Spot FIFO → closed_positions')
    parser.add_argument('--db', type=str, default=DB_PATH_DEFAULT, help='Ruta a portfolio.db')
    parser.add_argument('--days_back', type=int, default=30, help='Ventana de histórico (días)')
    parser.add_argument('--debug', action='store_true', help='Logs verbosos')
    args = parser.parse_args()
    
    save_xt_spot_positions(db_path=args.db, days_back=args.days_back, debug=True)