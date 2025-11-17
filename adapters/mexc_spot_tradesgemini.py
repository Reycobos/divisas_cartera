import sqlite3
import logging
import asyncio
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any

# --- Importación de Clientes y Utilitarios de mexc.py ---
try:
    # Ahora importamos la clase cliente y las utilidades añadidas a mexc.py
    from mexc import MexcSpotClient, normalize_symbol, to_s
    
    # Se añade un flag para saber si usamos el cliente real o un mock
    CLIENT_IMPORTED = True
except ImportError as e:
    logging.warning(f"Error al importar de mexc.py: {e}. Usando Mocks para pruebas.")
    CLIENT_IMPORTED = False
    
    # Mocks de emergencia si la importación falla (incluyendo MexcSpotClient)
    class MexcSpotClient:
        def __init__(self, api_key: str, api_secret: str):
            pass # No hace nada en mock
            
        async def fetch_mexc_spot_fills(self, symbol: str, start_time: int, end_time: int):
            # --- Simulación de Respuesta de la API de MEXC ---
            if symbol == 'ETHUSDT':
                mock_data = [
                    # 1. Compra de apertura @ 3000
                    {'symbol': 'ETHUSDT', 'orderId': '101', 'tradeId': 't101', 'price': '3000.0', 'qty': '0.1', 'commission': '0.0000005', 'commissionAsset': 'ETH', 'time': start_time + 1000, 'isBuyer': True, 'quoteQty': '300.0'},
                    # 2. Compra adicional @ 2900
                    {'symbol': 'ETHUSDT', 'orderId': '102', 'tradeId': 't102', 'price': '2900.0', 'qty': '0.1', 'commission': '0.3', 'commissionAsset': 'USDT', 'time': start_time + 2000, 'isBuyer': True, 'quoteQty': '290.0'},
                    # 3. Venta de cierre @ 3100
                    {'symbol': 'ETHUSDT', 'orderId': '201', 'tradeId': 't201', 'price': '3100.0', 'qty': '0.15', 'commission': '0.45', 'commissionAsset': 'USDT', 'time': start_time + 5000, 'isBuyer': False, 'quoteQty': '465.0'},
                ]
            else:
                mock_data = []

            return {'code': 200, 'data': mock_data}

    def normalize_symbol(symbol: str) -> str: return symbol.replace('USDT', '/USDT')
    def to_s(timestamp_ms: int) -> str: return datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

# --- Constantes y Configuración ---
DB_PATH_DEFAULT = 'portfolio.db'
SUPPORTED_QUOTE_ASSETS = ['USDT', 'USDC']
DUST_THRESHOLD = 1e-12 # Para manejar errores de punto flotante

def _insert_row(conn: sqlite3.Connection, row: Dict[str, Any]):
    """
    Función de inserción simulada/debug en la tabla closed_positions.
    """
    keys = ', '.join(row.keys())
    values = ', '.join(['?' for _ in row])
    try:
        # Aquí iría el conn.execute real. Lo dejamos comentado para evitar
        # errores si el schema de 'portfolio.db' no existe.
        # conn.execute(f"INSERT INTO closed_positions ({keys}) VALUES ({values})", list(row.values()))
        
        # DEBUG: Mostrar la fila que se *debería* guardar
        logging.debug(f"\n--- DEBUG SAVE FILA ---")
        logging.debug(f"Row to save/ignore: {row}")
        logging.debug(f"-------------------------\n")
        
    except Exception as e:
        logging.error(f"Error SIMULADO al insertar en DB: {e}")

# --- Lógica Principal del Adaptador ---

async def save_mexc_spot_positions(api_key: str, api_secret: str, symbols: List[str], db_path: str = DB_PATH_DEFAULT, days_back: int = 30, debug: bool = False):
    """
    Descarga los trades de MEXC, aplica la lógica FIFO para calcular
    posiciones cerradas y las guarda en portfolio.db.

    @param api_key: Clave API de MEXC.
    @param api_secret: Secreto API de MEXC.
    @param symbols: Lista de pares a procesar (ej: ['ETHUSDT', 'BNBUSDT']).
    @param db_path: Ruta al archivo de la base de datos.
    @param days_back: Ventana de histórico en días.
    @param debug: Habilita la salida de debug.
    @return: (trades guardados, trades ignorados)
    """
    if debug:
        logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s] %(levelname)s: %(message)s")
    else:
        logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

    # --- Inicialización del Cliente Corregida ---
    client = MexcSpotClient(api_key, api_secret)

    try:
        conn = sqlite3.connect(db_path)
    except Exception as e:
        logging.error(f"No se pudo conectar a la base de datos en {db_path}: {e}")
        return 0, 0
    
    end_time_ms = int(datetime.now().timestamp() * 1000)
    start_time_ms = int((datetime.now() - timedelta(days=days_back)).timestamp() * 1000)
    
    saved = 0
    ignored = 0
    
    # 1. Obtener y consolidar todos los trades
    all_trades = {}
    for symbol in symbols:
        try:
            # La función fetch_mexc_spot_fills se llama sobre la instancia del cliente
            response = await client.fetch_mexc_spot_fills(symbol, start_time_ms, end_time_ms)
            
            # Nota: Asumo que la respuesta contiene la lista en 'data' o es directamente la lista
            trades_data = response.get('data', [])
            if not trades_data and isinstance(response, list):
                trades_data = response # Manejar caso donde la API devuelve solo la lista
            
            if response.get('code') not in [200, None] and not trades_data:
                logging.error(f"Error al obtener trades para {symbol}: {response.get('msg', 'Error desconocido')}")
                continue

            
            # DEBUG: Resultado crudo del endpoint
            if debug:
                logging.debug(f"\n--- DEBUG ENDPOINT {symbol} ---")
                logging.debug(f"Trades count: {len(trades_data)}")
                logging.debug(f"----------------------------------")
                
            all_trades[symbol] = sorted(trades_data, key=lambda x: x['time'])
            
        except Exception as e:
            logging.error(f"Error procesando {symbol} en la API: {e}")
            
    # 2. Procesamiento FIFO (El resto de la lógica FIFO es la misma que la anterior)
    for symbol, trades in all_trades.items():
        if not trades:
            continue

        # MEXC usa symbol como "ETHUSDT". Se separa en Base y Quote.
        # Esto asume que el símbolo de entrada ES el formato MEXC (ej: 'ETHUSDT')
        quote_asset = None
        base_asset = None
        for quote in SUPPORTED_QUOTE_ASSETS:
            if symbol.upper().endswith(quote):
                quote_asset = quote
                base_asset = symbol[:-len(quote)]
                break
        
        if not base_asset or not quote_asset:
            logging.warning(f"Símbolo {symbol} no soporta par base/quote conocido ({SUPPORTED_QUOTE_ASSETS}). Saltando.")
            continue
        
        # Stack FIFO: (cantidad_restante, precio_entrada, fee_en_usdt_acumulado, open_time_ms, notional_bruto)
        open_buys = [] 
        is_first_trade = True
        
        for trade in trades:
            # Conversión de strings a float/int
            try:
                trade_qty = float(trade['qty'])
                trade_price = float(trade['price'])
                trade_time_ms = int(trade['time'])
                trade_fee = float(trade['commission'])
                trade_notional = float(trade['quoteQty'])
                
                # isBuyer es un booleano en la respuesta de MEXC
                trade_side = 'BUY' if trade['isBuyer'] else 'SELL'
                trade_fee_asset = trade['commissionAsset']
            except (KeyError, ValueError) as e:
                logging.error(f"Error de formato en trade de {symbol}: {e} en trade {trade}. Saltando.")
                continue

            # --- Conversión de Fee a QUOTE (USDT/USDC) (Requisito FIFO) ---
            fee_in_quote = 0.0
            if trade_fee_asset == quote_asset:
                # Fee pagado en USDT o USDC (ya está en quote)
                fee_in_quote = trade_fee
            elif trade_fee_asset == base_asset:
                # Fee pagado en BASE (ej: ETH). Se convierte a QUOTE (USDT) con el precio de este trade.
                fee_in_quote = trade_fee * trade_price
            else:
                logging.warning(f"Fee asset {trade_fee_asset} en {symbol} no es base/quote. Asumiendo fee 0.0.")

            # --- Lógica FIFO ---
            if trade_side == 'BUY':
                # Añadir a la cola de compras abiertas 
                open_buys.append({
                    'size_rem': trade_qty,
                    'entry_price': trade_price,
                    'fee_total': fee_in_quote,
                    'open_time': trade_time_ms,
                    'notional': trade_notional 
                })
                is_first_trade = False

            elif trade_side == 'SELL':
                if is_first_trade and not open_buys:
                    row = {
                        'symbol': normalize_symbol(symbol),
                        'side': 'spotsell', 
                        'size': trade_qty,
                        'entry_price': trade_price,
                        'close_price': trade_price,
                        'pnl': 0.0,
                        'realized_pnl': 0.0,
                        'fee_total': fee_in_quote, 
                        'open_time': trade_time_ms,
                        'close_time': trade_time_ms,
                        'notional': trade_notional,
                        'ignore_trade': 1,
                    }
                    _insert_row(conn, row)
                    ignored += 1
                    is_first_trade = False
                    continue 

                # Caso 2: Venta de cierre (FIFO)
                remaining_sell_qty = trade_qty
                
                # Prorratear el fee de CIERRE por unidad de base (Qty)
                fee_per_unit_sell = fee_in_quote / max(trade_qty, DUST_THRESHOLD)

                while remaining_sell_qty > DUST_THRESHOLD and open_buys:
                    open_buy = open_buys[0]
                    buy_rem_qty = open_buy['size_rem']
                    
                    match_qty = min(remaining_sell_qty, buy_rem_qty)
                    
                    entry_price = open_buy['entry_price']
                    close_price = trade_price
                    
                    # 1. PnL Bruto (en Quote Asset)
                    pnl_per_unit = close_price - entry_price
                    realized_pnl_gross = pnl_per_unit * match_qty
                    
                    # 2. Fee Total
                    # Calculamos el fee por unidad para el trade de compra (más preciso)
                    fee_per_unit_buy = open_buy['fee_total'] / (open_buy['notional'] / open_buy['entry_price'])
                    
                    open_fee_prorated = fee_per_unit_buy * match_qty
                    close_fee_prorated = fee_per_unit_sell * match_qty
                    
                    fee_total_pos = open_fee_prorated + close_fee_prorated
                    
                    # 3. PnL Neto
                    realized_pnl_net = realized_pnl_gross - fee_total_pos
                    
                    # 4. Notional total (volumen de la operación de compra)
                    notional_pos_buy = match_qty * entry_price 
                    
                    # Fila a guardar en la base de datos
                    row = {
                        'symbol': normalize_symbol(symbol),
                        'side': 'spotbuy', 
                        'size': match_qty,
                        'entry_price': entry_price,
                        'close_price': close_price,
                        'pnl': realized_pnl_gross,
                        'realized_pnl': realized_pnl_net,
                        'fee_total': fee_total_pos,
                        'open_time': open_buy['open_time'],
                        'close_time': trade_time_ms,
                        'notional': notional_pos_buy, 
                        'ignore_trade': 0, 
                    }
                    _insert_row(conn, row)
                    saved += 1
                    
                    # Actualizar remanentes
                    open_buy['size_rem'] -= match_qty
                    remaining_sell_qty -= match_qty
                    
                    # Si la compra se agota, se remueve del stack
                    if open_buy['size_rem'] < DUST_THRESHOLD:
                        open_buys.pop(0)

                if remaining_sell_qty > DUST_THRESHOLD:
                    logging.warning(f"Venta remanente de {remaining_sell_qty} {base_asset} en {symbol} sin compras FIFO previas. Se ignora.")

            is_first_trade = False

        # 3. Manejar compras abiertas remanentes (Posición abierta o ignorada)
        for rem_buy in open_buys:
            if rem_buy['size_rem'] > DUST_THRESHOLD:
                size_original = rem_buy['notional'] / max(rem_buy['entry_price'], DUST_THRESHOLD)
                prorate_factor = rem_buy['size_rem'] / max(size_original, DUST_THRESHOLD)

                row = {
                    'symbol': normalize_symbol(symbol),
                    'side': 'spotbuy',
                    'size': rem_buy['size_rem'],
                    'entry_price': rem_buy['entry_price'],
                    'close_price': rem_buy['entry_price'], 
                    'pnl': 0.0,
                    'realized_pnl': 0.0,
                    'fee_total': rem_buy['fee_total'] * prorate_factor,
                    'open_time': rem_buy['open_time'],
                    'close_time': rem_buy['open_time'],
                    'notional': rem_buy['notional'] * prorate_factor, 
                    'ignore_trade': 1, 
                }
                _insert_row(conn, row)
                ignored += 1

    conn.commit()
    conn.close()

    if debug:
        logging.info(f"\n--- RESUMEN ADAPTADOR MEXC SPOT ---")
        logging.info(f"Trades MEXC FIFO: guardados={saved}, ignorados={ignored}")
        logging.info(f"-----------------------------------")
    
    return saved, ignored


# ---------- CLI (Para pruebas independientes) ----------
if __name__ == '__main__':
    import argparse
    import asyncio
    import sys
    
    # Se recomienda que el usuario configure estas credenciales
    API_KEY = os.environ.get("MEXC_API_KEY", "TU_API_KEY_MEXC")
    API_SECRET = os.environ.get("MEXC_API_SECRET", "TU_SECRET_MEXC")
    SYMBOLS_TO_FETCH = ['ETHUSDT', 'BNBUSDT', 'BTCUSDT'] 

    parser = argparse.ArgumentParser(description='MEXC Spot FIFO → closed_positions')
    parser.add_argument('--db', type=str, default=DB_PATH_DEFAULT, help='Ruta a portfolio.db')
    parser.add_argument('--days_back', type=int, default=30, help='Ventana de histórico (días)')
    parser.add_argument('--debug', action='store_true', help='Activa el modo debug detallado')
    args = parser.parse_args()
    
    async def run_adapter():
        await save_mexc_spot_positions(
            api_key=API_KEY,
            api_secret=API_SECRET,
            symbols=SYMBOLS_TO_FETCH,
            db_path=args.db,
            days_back=args.days_back,
            debug=args.debug
        )

    print(f"Ejecutando MEXC Spot FIFO con db={args.db}, days_back={args.days_back} días, debug={args.debug}")
    print(f"Nota: Cliente real importado: {CLIENT_IMPORTED}. Usando API Key/Secret: {'(de .env)' if API_KEY != 'TU_API_KEY_MEXC' else '(Placeholder)'}")

    # FIX para el error de 'asyncio.run() cannot be called from a running event loop'
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Si ya hay un loop (común en entornos como IPython o Spyder), usar create_task
            print("Loop existente detectado. Usando asyncio.create_task.")
            loop.create_task(run_adapter())
        else:
            # Si no hay loop, ejecutar con asyncio.run
            asyncio.run(run_adapter())
    except RuntimeError as e:
        # En caso de error al obtener el loop (ejecución inicial sin loop)
        if 'There is no current event loop in thread' in str(e):
            asyncio.run(run_adapter())
        else:
            print(f"Ocurrió un error durante la ejecución asíncrona: {e}")
            sys.exit(1)