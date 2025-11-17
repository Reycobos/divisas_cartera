# adapters/mexc_spot_trades.py
# -*- coding: utf-8 -*-
"""
MEXC — Spot trades → closed positions (FIFO)

Qué hace
--------
- Descarga el historial de trades (fills) de MEXC para pares SPOT específicos.
- Implementa lógica FIFO para detectar rondas de compra/venta y calcular PnL.
- Chequea en 'portfolio.db' si la posición ya fue cerrada para evitar duplicados.
- Trata el par USDCUSDT como un 'swapstable' especial.

Dependencias
------------
- adapters.mexc   → _mexc_request, _get_mexc_keys
- universal_cache → init_universal_cache_db, add_symbol_to_cache
- utils.symbols   → normalize_symbol

Cómo usar
---------
- Importar y ejecutar save_mexc_spot_positions()
"""

from __future__ import annotations
import os, time, sqlite3
from typing import Any, Dict, List, Optional, Tuple, Set

# Importaciones asumidas del proyecto (ajustar si las rutas son diferentes)
try:
    from adapters.mexc import _mexc_request, _get_mexc_keys 
    from universal_cache import init_universal_cache_db, add_symbol_to_cache
    from utils.symbols import normalize_symbol  
    # Asumo que existe una utilidad de tiempo en 'utils'
except ImportError:
    # Mocks para desarrollo local si las dependencias no están en PYTHONPATH
    def _mexc_request(*args, **kwargs): raise NotImplementedError("Dependencia _mexc_request no disponible.")
    def _get_mexc_keys(): return "MOCK_KEY", "MOCK_SECRET"
    def init_universal_cache_db(): pass
    def add_symbol_to_cache(*args): pass
    def normalize_symbol(s): return s.replace("USDT", "").replace("USDC", "")

DB_PATH_DEFAULT = "portfolio.db"
CLOSED_POSITIONS_TABLE = "closed_positions"

# Pares obligatorios a escanear. 
# Si quieres desactivar alguno, coméntalo o remuévelo de esta lista.
MEXC_SPOT_PAIRS: List[str] = [
    "CUDISUSDT",
    "USDCUSDT", 
]

def _check_already_closed(
    conn: sqlite3.Connection,
    exchange_id: str, 
    trade_id: str
) -> bool:
    """
    Verifica si un trade_id (fill ID) de MEXC ya se ha insertado 
    como una posición cerrada en la base de datos 'closed_positions'.
    """
    cur = conn.cursor()
    # Usamos 'entry_id' para almacenar el ID del trade (fill) de MEXC.
    query = f"""
        SELECT 1 
        FROM {CLOSED_POSITIONS_TABLE} 
        WHERE exchange_id = ? AND entry_id = ?
    """
    cur.execute(query, (exchange_id, str(trade_id)))
    return cur.fetchone() is not None

def _insert_row(conn: sqlite3.Connection, row: Dict[str, Any]):
    """Inserta una fila en la tabla closed_positions."""
    fields = ', '.join(row.keys())
    placeholders = ', '.join(['?'] * len(row))
    query = f"INSERT INTO {CLOSED_POSITIONS_TABLE} ({fields}) VALUES ({placeholders})"
    conn.execute(query, list(row.values()))

def save_mexc_spot_positions(
    db_path: str = DB_PATH_DEFAULT,
    pairs: Optional[List[str]] = None,
    days_back: int = 90,
    debug: bool = False
) -> Tuple[int, int]:
    """
    Guarda las posiciones cerradas de Spot de MEXC en el portafolio (FIFO).
    
    Args:
        db_path: Ruta al archivo portfolio.db.
        pairs: Lista de pares a procesar. Si es None, usa MEXC_SPOT_PAIRS.
        days_back: Ventana de tiempo en días para buscar trades.
        debug: Imprime información de depuración.
        
    Returns:
        Tupla (registros_guardados, registros_ignorados).
    """
    
    # 1. Configuración y Conexión
    target_pairs = pairs if pairs is not None else MEXC_SPOT_PAIRS
    
    if debug:
        print(f"⏳ Procesando {len(target_pairs)} pares SPOT de MEXC...")
        
    init_universal_cache_db() 
    
    try:
        _get_mexc_keys() # Verifica que las claves estén cargadas
    except ValueError as e:
        print(f"❌ Error de configuración: {e}")
        return 0, 0

    conn = sqlite3.connect(db_path)
    saved, ignored = 0, 0
    exchange_name = "mexc"

    # Cálculo de la marca de tiempo de inicio (en milisegundos)
    start_time_ms = int((time.time() - days_back * 86400) * 1000)

    # 2. Búsqueda de Trades por Par
    for symbol in target_pairs:
        mexc_symbol = symbol.upper()
        
        # Endpoint: Historial de trades de spot del usuario (fills)
        # Asumiendo el endpoint estándar de trades/fills del usuario
        path = "/api/v3/spot/order/trades"
        
        current_page = 1
        page_size = 500 
        total_trades = []
        
        while True:
            params = {
                "symbol": mexc_symbol,
                "startTime": start_time_ms,
                "limit": page_size,
                "page": current_page,
            }
            
            try:
                # La función _mexc_request maneja la firma
                response = _mexc_request("GET", path, params=params, signed=True)
                trades = response.get("data", [])
                
                if debug:
                    print(f"   [API] {mexc_symbol} | Página {current_page} - Trades recibidos: {len(trades)}")
                    
                total_trades.extend(trades)
                
                if not trades or len(trades) < page_size:
                    break
                
                current_page += 1
                time.sleep(0.1) # Pequeña pausa para evitar rate limits

            except NotImplementedError:
                print("❌ ERROR: La función _mexc_request no está disponible (dependencia faltante).")
                return 0, 0
            except Exception as e:
                print(f"❌ Error al obtener trades de MEXC para {mexc_symbol}: {e}")
                break
        
        if not total_trades:
            continue
            
        # 3. Procesamiento y Lógica FIFO
        
        # Ordenar por tiempo (timestamp) para asegurar la lógica FIFO
        total_trades.sort(key=lambda x: x['time'])
        
        # Almacén de compras abiertas (FIFO)
        # { 'tradeId_compra': {'qty_restante': float, 'price': float, 'time': float, 'fee': float} }
        open_buys: Dict[str, Dict[str, Any]] = {}
        
        for trade in total_trades:
            # Normalizar los datos del trade de MEXC
            trade_id = trade.get('id')
            if not trade_id: continue
            
            price = float(trade['price'])
            qty = float(trade['qty'])
            # 'isBuyer': True si fue una compra, False si fue una venta
            is_buy = trade.get('isBuyer', False) 
            fee = float(trade['commission'])
            trade_time_ms = int(trade['time']) 
            trade_time_s = trade_time_ms / 1000 # Segundos
            
            # --- Chequeo de Posición Cerrada ---
            if _check_already_closed(conn, exchange_name, trade_id):
                ignored += 1
                continue
            
            # -----------------------------------

            # 3.1. Caso especial: Swaps (USDCUSDT)
            if mexc_symbol in ["USDCUSDT", "USDTUSDC"]:
                notional = price * qty
                
                row = {
                    'exchange_id': exchange_name,
                    'symbol': normalize_symbol(mexc_symbol),
                    'entry_id': str(trade_id), 
                    'side': 'swapstable',
                    'size': qty,
                    'entry_price': price,
                    'close_price': price,
                    'pnl': 0.0,
                    'realized_pnl': -fee, # En swaps, el costo es principalmente el fee
                    'fee_total': fee,
                    'open_time': trade_time_s,
                    'close_time': trade_time_s,
                    'notional': notional,
                    'ignore_trade': 0,
                }
                _insert_row(conn, row)
                saved += 1
                continue
                
            
            # 3.2. Lógica FIFO para otros pares (Buy / Sell)
            
            if is_buy: # True
                # Es una COMPRA. Se agrega al stack de compras abiertas.
                open_buys[str(trade_id)] = {
                    'qty_restante': qty, 
                    'price': price, 
                    'time': trade_time_s, 
                    'fee': fee, 
                }
                
            else: # False (es una VENTA / cierre)
                remaining_sell_qty = qty
                current_sell_price = price
                current_sell_time = trade_time_s
                sell_fee = fee
                
                buy_ids_to_remove = []
                
                # Iterar sobre las compras abiertas por orden de tiempo (FIFO)
                # Ordenamos las claves de las compras abiertas por el tiempo de apertura
                sorted_buy_ids = sorted(open_buys.keys(), key=lambda k: open_buys[k]['time'])
                
                for buy_id in sorted_buy_ids:
                    buy = open_buys[buy_id]
                    buy_qty_available = buy['qty_restante']
                    
                    if remaining_sell_qty <= 1e-12: 
                        break

                    fill_qty = min(remaining_sell_qty, buy_qty_available)
                    
                    # Cálculo de PnL
                    pnl_gross = fill_qty * (current_sell_price - buy['price'])
                    
                    # Prorrateo de fees (tanto de la compra original como de la venta actual)
                    # Fee de la compra original (prorrateado por la cantidad que se cierra)
                    fee_total_buy = buy['fee'] * (fill_qty / (buy_qty_available + (buy['fee'] / buy['price'] if buy['fee'] > 0 else 0) if buy_qty_available > 1e-12 else 1)) # Approx
                    # Fee de la venta actual (prorrateado)
                    fee_total_sell = sell_fee * (fill_qty / qty)
                    
                    fee_total = fee_total_buy + fee_total_sell
                    
                    # Registrar la posición cerrada
                    closed_row = {
                        'exchange_id': exchange_name,
                        'symbol': normalize_symbol(mexc_symbol),
                        'entry_id': buy_id, # Usamos el ID de la COMPRA como ID de la ronda
                        'side': 'spotbuy',
                        'size': fill_qty,
                        'entry_price': buy['price'],
                        'close_price': current_sell_price,
                        'pnl': pnl_gross,
                        'realized_pnl': pnl_gross - fee_total, # PnL Neto
                        'fee_total': fee_total,
                        'open_time': buy['time'],
                        'close_time': current_sell_time,
                        'notional': fill_qty * current_sell_price,
                        'ignore_trade': 0,
                    }
                    _insert_row(conn, closed_row)
                    saved += 1
                    
                    # 4. Actualizar cantidades restantes
                    remaining_sell_qty -= fill_qty
                    open_buys[buy_id]['qty_restante'] -= fill_qty
                    
                    if open_buys[buy_id]['qty_restante'] <= 1e-12:
                        buy_ids_to_remove.append(buy_id)
                
                # Eliminar compras completamente cerradas
                for buy_id in buy_ids_to_remove:
                    del open_buys[buy_id]

                # Advertencia sobre ventas excedentes
                if remaining_sell_qty > 1e-12 and debug:
                    print(f"   [WARN] Venta excedente de {remaining_sell_qty} en {mexc_symbol}. Ignorando (asumido como venta de un depósito).")


        # 4. Actualización del cache universal
        add_symbol_to_cache(exchange_name, mexc_symbol, mexc_symbol, 'spot')
            
    # 5. Cierre y Resultados
    conn.commit()
    conn.close()

    if debug:
        print(f"✅ Spot FIFO MEXC: guardadas={saved}, ignoradas={ignored}")
    
    return saved, ignored

# ---------- CLI para demostración / testing ----------
if __name__ == '__main__':
    # Usar dotenv para cargar variables de entorno (asumiendo que existe el helper)
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass 

    import argparse
    from datetime import datetime
    
    parser = argparse.ArgumentParser(description='MEXC Spot FIFO → closed_positions')
    parser.add_argument('--db', type=str, default=DB_PATH_DEFAULT, help='Ruta a portfolio.db')
    parser.add_argument('--days_back', type=int, default=30, help='Ventana de histórico (días)')
    parser.add_argument('--debug', action='store_true', help='Activa el modo de depuración.')
    
    args = parser.parse_args()
    
    print(f"--- MEXC Spot Trades Adapter ({datetime.now().isoformat()}) ---")
    
    # Se usan los pares obligatorios por defecto definidos arriba
    
    try:
        saved_count, ignored_count = save_mexc_spot_positions(
            db_path=args.db,
            days_back=args.days_back,
            debug=args.debug
        )
        print(f"\nResumen: {saved_count} posiciones cerradas guardadas, {ignored_count} trades ignorados.")
    except ValueError as e:
        print(f"\n❌ ERROR de Configuración: {e}. Por favor, revisa tus claves MEXC_API_KEY y MEXC_SECRET en .env.")
    except NotImplementedError:
        print("\n❌ ERROR: Dependencias del proyecto (mexc.py, universal_cache.py, utils) no están disponibles. Asegúrate de tenerlas en tu entorno.")
    except Exception as e:
        print(f"\n❌ ERROR Inesperado durante el proceso: {e}")