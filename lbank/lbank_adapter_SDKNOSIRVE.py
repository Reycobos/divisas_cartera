import os
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()
import logging

# --- LBank SDK Imports ---
from lbank.old_api import BlockHttpClient

# =============================================================================
# CONFIGURACIÓN (CLAVES FORZADAS)
# =============================================================================

# 🚨 REEMPLAZA ESTOS VALORES CON TUS CLAVES REALES DE LBANK 🚨
API_KEY = "95b4f84f-7631-4286-9b25-5641f4fec5c3"
SECRET_KEY = "F3F6CDC794DFC8248BE98B4DB538FDF9" 

# Base URL corregida (sin /v2 al final)
BASE_URL = "https://api.lbkex.com/"

# =============================================================================
# INICIALIZACIÓN DEL CLIENTE SDK CORREGIDA
# =============================================================================

# 🔍 Diagnóstico: Verificar que las claves no estén vacías
if not API_KEY or not SECRET_KEY:
    print("[LBANK FATAL] ❌ Error: Las claves de API están vacías. ¡Revisa la asignación!")
    LBANK_CLIENT = None
else:
    print(f"[LBANK DIAG] ✅ Claves cargadas. API Key comienza con: {API_KEY[:4]}...")

    try:
        # ✅ CORRECCIÓN: Usar RSA como método de firma (no HmacSHA256)
        LBANK_CLIENT = BlockHttpClient(
            sign_method="RSA",  # ← CAMBIADO de "HmacSHA256" a "RSA"
            api_key=API_KEY,
            api_secret=SECRET_KEY,
            base_url=BASE_URL, 
            log_level=logging.INFO 
        )
        print("[LBANK SDK] ✅ Cliente SDK inicializado correctamente con RSA")
    except Exception as e:
        print(f"[LBANK SDK] ❌ Error inicializando el cliente SDK: {e}")
        LBANK_CLIENT = None

try:
    from universal_cache import (
        init_universal_cache_db,
        add_to_universal_cache,
        get_cached_currency_pairs,
        update_cache_from_positions,
        get_cached_symbols,
        save_closed_position
    )
    CACHE_AVAILABLE = True
except ImportError:
    CACHE_AVAILABLE = False
    print("[LBANK] ⚠️ Cache universal no disponible - funcionará sin cache")

def set_debug_mode(enabled: bool):
    """Establece el nivel de log para el cliente SDK."""
    if LBANK_CLIENT:
        if enabled:
            LBANK_CLIENT.log_level = logging.DEBUG
        else:
            LBANK_CLIENT.log_level = logging.INFO

# =============================================================================
# ADAPTADOR DE BALANCES (CORREGIDO)
# =============================================================================

def fetch_lbank_all_balances() -> dict:
    """
    Obtiene todos los balances spot de LBank usando el SDK.
    Endpoint: /v2/user_info.do
    """
    if not LBANK_CLIENT:
        return {"spot": {}, "total_usdt": 0.0, "total_usd": 0.0}

    print("[LBANK] 🔄 Solicitando balances spot a LBank...")
    try:
        # El endpoint para balances es v2/user_info.do
        response = LBANK_CLIENT.http_request("post", "v2/user_info.do", payload={})
        
        print(f"[LBANK DEBUG] Respuesta completa: {response}")  # Debug
        
        if response.get("result") is not True:
            error_code = response.get("error_code", "N/A")
            error_msg = response.get("msg", "Error desconocido")
            raise Exception(f"API Error {error_code}: {error_msg}")

        data = response.get("data", {})
        
        # LBank devuelve un diccionario de diccionarios
        balances = defaultdict(float)
        
        # Iterar sobre las claves para extraer solo las monedas con saldo > 0
        for currency, info in data.items():
            try:
                # ✅ CORRECCIÓN: El campo correcto es 'freeze' y 'asset'
                if isinstance(info, dict):
                    available = float(info.get("asset", 0))
                    frozen = float(info.get("freeze", 0))
                    total = available + frozen
                    
                    if total > 0:
                        balances[currency.upper()] = total
                        print(f"[LBANK DEBUG] {currency}: available={available}, frozen={frozen}, total={total}")
                        
            except (TypeError, ValueError) as e:
                print(f"[LBANK WARN] Error procesando moneda {currency}: {e}")
                continue

        print(f"[LBANK] ✅ {len(balances)} activos con saldo encontrado: {list(balances.keys())}")
        
        return {
            "spot": dict(balances), 
            "total_usdt": 0.0, 
            "total_usd": 0.0
        }
    
    except Exception as e:
        print(f"[LBANK] ❌ Error al obtener balances: {e}")
        return {"spot": {}, "total_usdt": 0.0, "total_usd": 0.0}

# =============================================================================
# LÓGICA DE TRADES Y FIFO (CORREGIDA)
# =============================================================================

def _process_trades_fifo(trades: List[Dict]) -> List[Dict]:
    """
    Reconstruye posiciones cerradas a partir de trades brutos utilizando la lógica FIFO.
    """
    if not trades:
        return []
    
    # 1. Agrupar trades por símbolo
    trades_by_symbol = defaultdict(list)
    for trade in trades:
        try:
            # Normalizar el formato de LBank al que usamos para la reconstrucción
            processed_trade = {
                'timestamp': int(trade.get('time_ms', trade.get('time', 0))),  # Usar milisegundos
                'symbol': trade.get('symbol', '').upper().replace("_", "/"),
                'side': 'BUY' if trade.get('type') == 'buy' else 'SELL',
                'amount': float(trade.get('volume', 0)),
                'price': float(trade.get('price', 0)),
                'cost': float(trade.get('trade_money', 0)),
                'fee': float(trade.get('fee', 0)),
                'fee_currency': trade.get('fee_currency', '').upper()
            }
            
            # Solo agregar trades válidos
            if processed_trade['amount'] > 0 and processed_trade['price'] > 0:
                trades_by_symbol[processed_trade['symbol']].append(processed_trade)
                
        except (KeyError, ValueError, TypeError) as e:
            print(f"[LBANK FIFO] ⚠️ Error procesando trade: {e}, trade: {trade}")
            continue

    # 2. Reconstrucción FIFO
    closed_positions = []
    
    for symbol, symbol_trades in trades_by_symbol.items():
        # Asegurarse de que los trades estén ordenados cronológicamente
        symbol_trades.sort(key=lambda x: x['timestamp'])
        
        open_buys = []
        
        for trade in symbol_trades:
            if trade['side'] == 'BUY':
                open_buys.append(trade)
                continue

            # Es un trade de VENTA (Cierre potencial)
            remaining_sell_amount = trade['amount']
            
            # Buscar buys abiertos para cerrar
            while remaining_sell_amount > 0 and open_buys:
                open_buy = open_buys[0]
                
                # Cantidad a cerrar en esta transacción
                amount_to_close = min(remaining_sell_amount, open_buy['amount'])
                
                # Porcentaje de la posición abierta que se cierra
                ratio = amount_to_close / open_buy['amount']

                # Costos prorrateados
                open_buy_fee = open_buy['fee'] * ratio
                close_sell_fee = trade['fee'] * (amount_to_close / trade['amount']) # Fee proporcional de la venta
                
                # Calcular el P&L
                total_buy_cost = open_buy['cost'] * ratio
                total_sell_revenue = trade['cost'] * (amount_to_close / trade['amount'])
                
                gross_profit = total_sell_revenue - total_buy_cost
                net_profit = gross_profit - open_buy_fee - close_sell_fee

                # Crear la posición cerrada
                closed_positions.append({
                    'exchange': 'lbank',
                    'symbol': symbol,
                    'open_time': open_buy['timestamp'],
                    'close_time': trade['timestamp'],
                    'amount': amount_to_close,
                    'net_profit_usdt': net_profit,
                    'entry_price': open_buy['price'],
                    'exit_price': trade['price'],
                    'open_fee': open_buy_fee,
                    'close_fee': close_sell_fee,
                    'is_closed': True
                })
                
                # Actualizar cantidades
                remaining_sell_amount -= amount_to_close
                open_buy['amount'] -= amount_to_close
                
                # Si la posición de compra se ha cerrado completamente, eliminarla
                if open_buy['amount'] <= 1e-8: # Usar tolerancia
                    open_buys.pop(0)

        # Si quedan 'open_buys', son posiciones que siguen abiertas. Se ignoran aquí.

    return closed_positions

def _fetch_trades_for_symbol(symbol: str, start_date: str = None, end_date: str = None,
                             days: int = 30) -> List[dict]:
    """
    Obtiene trades de un símbolo específico usando el SDK (ventana de 1 día).
    Endpoint: /v2/transaction_history.do
    """
    if not LBANK_CLIENT:
        return []
        
    all_trades = []
    
    # Lógica de cálculo de fechas
    end_dt = datetime.now(timezone.utc)
    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        
    start_dt = end_dt - timedelta(days=days)
    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    current_start = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    final_end = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    print(f"[LBANK TRADES] Buscando {symbol} desde {current_start.strftime('%Y-%m-%d')} hasta {final_end.strftime('%Y-%m-%d')}")

    # Iterar día por día
    while current_start <= final_end:
        date_str = current_start.strftime("%Y-%m-%d")
        
        # ✅ CORRECCIÓN: Usar timestamps en milisegundos como requiere LBank
        start_timestamp = int(current_start.timestamp() * 1000)
        end_timestamp = int((current_start + timedelta(days=1)).timestamp() * 1000) - 1
        
        params = {
            "symbol": symbol.lower().replace("/", "_"),
            "startTime": str(start_timestamp),  # ← CORREGIDO: timestamp en ms
            "endTime": str(end_timestamp),      # ← CORREGIDO: timestamp en ms
            "limit": "100"
        }
        
        try:
            print(f"[LBANK TRADES] 🔍 Consultando {symbol} para {date_str}...")
            
            # 🚀 USANDO EL CLIENTE SDK OFICIAL CON RSA
            trades_response = LBANK_CLIENT.http_request(
                "post", 
                "v2/transaction_history.do", 
                payload=params
            )
            
            print(f"[LBANK TRADES DEBUG] Respuesta para {date_str}: {trades_response}")  # Debug
            
            if trades_response.get("result") is True:
                trades = trades_response.get("data", [])
                
                if isinstance(trades, list) and trades:
                    all_trades.extend(trades)
                    print(f"[LBANK TRADES] ✅ {len(trades)} trades de {symbol} encontrados en {date_str}")
                else:
                    print(f"[LBANK TRADES] ℹ️ {symbol}: No hay trades propios en {date_str}")
            else:
                error_code = trades_response.get("error_code", "N/A")
                error_msg = trades_response.get("msg", "Error desconocido")
                
                # El error 10008 (o 'currency pair nonsupport')
                if error_code == 10008 or 'nonsupport' in error_msg.lower():
                    print(f"[LBANK TRADES] ⚠️ {symbol}: Sin trades o par no soportado en {date_str} (Error {error_code})")
                else:
                    print(f"[LBANK TRADES] ❌ Error {error_code} obteniendo trades de {symbol} en {date_str}: {error_msg}")
        
        except Exception as e:
            print(f"[LBANK TRADES] ❌ Excepción al usar SDK para {symbol} en {date_str}: {e}")
        
        # Avanzar al siguiente día
        current_start = current_start + timedelta(days=1)
        time.sleep(0.3)  # Rate limiting
    
    print(f"[LBANK TRADES] 📊 Total trades encontrados para {symbol}: {len(all_trades)}")
    return all_trades

def add_manual_lbank_pair(symbol: str):
    """Añade un par manualmente al cache para forzar su seguimiento."""
    if CACHE_AVAILABLE:
        symbol = symbol.upper().replace("/", "")
        print(f"[LBANK] 📌 Agregando símbolo al cache: {symbol}")
        add_to_universal_cache("lbank", symbol)
        print(f"[LBANK] ✅ Par agregado al cache: {symbol}")
        
def get_lbank_symbols(symbols: Optional[List[str]] = None) -> set:
    """Obtiene los símbolos a procesar, priorizando la lista de entrada o el cache."""
    symbol_set = set()
    
    if symbols:
        for s in symbols:
            symbol_set.add(s.upper())
    
    if CACHE_AVAILABLE and not symbols:
        cached_symbols = get_cached_symbols("lbank")
        symbol_set.update(cached_symbols)

    return symbol_set

def save_lbank_closed_positions(symbols: Optional[List[str]] = None, days: int = 30, dry_run: bool = False) -> int:
    """
    Obtiene, procesa y guarda posiciones cerradas de LBank usando el SDK.
    """
    
    symbols_to_process = get_lbank_symbols(symbols)
    
    if not symbols_to_process:
        print("[LBANK] ⚠️ No hay símbolos en el cache ni definidos para procesar.")
        return 0

    print(f"📦 Usando {len(symbols_to_process)} símbolos base: {', '.join(symbols_to_process)}")
    
    all_closed_positions = []
    
    for symbol in symbols_to_process:
        api_pair = f"{symbol.lower()}_usdt" 
        
        print(f"\n[LBANK] 🔍 Procesando símbolo: {api_pair}")
        trades = _fetch_trades_for_symbol(api_pair, days=days)
        
        if not trades:
            print(f"[LBANK] ⚠️ No se encontraron trades para {api_pair}")
            continue
            
        print(f"[LBANK] 📊 {len(trades)} trades encontrados para {api_pair}")
        
        closed_positions = _process_trades_fifo(trades)
        all_closed_positions.extend(closed_positions)
        
        print(f"[LBANK] ✅ {len(closed_positions)} posiciones cerradas reconstruidas para {api_pair}")
        
    print(f"\n📊 Total de posiciones cerradas reconstruidas: {len(all_closed_positions)}")

    if dry_run:
        print("\n=== DRY RUN RESULTADOS (Primeras 3 posiciones) ===")
        for pos in all_closed_positions[:3]:
            print(f"[{pos['symbol']}] Cerrado {pos['amount']:.4f} @ {pos['exit_price']:.8f} (P&L: {pos['net_profit_usdt']:.2f} USDT)")
        print("===================================================")
        return len(all_closed_positions)
    
    saved_count = 0
    if all_closed_positions:
        try:
            for pos in all_closed_positions:
                save_closed_position(pos)
                saved_count += 1
            
            print(f"✅ {saved_count}/{len(all_closed_positions)} posiciones guardadas en la base de datos.")
        except Exception as e:
            print(f"❌ Error al guardar posiciones: {e}")
            
    return saved_count

# =============================================================================
# TESTING RÁPIDO
# =============================================================================
if __name__ == "__main__":
    print("LBank Adapter SDK - Testing")
    print("=" * 60)
    
    # Activar debug para ver qué está pasando con el SDK
    set_debug_mode(True)
    
    # Asegúrate de que el cache esté inicializado
    if CACHE_AVAILABLE:
        init_universal_cache_db()
        
    # Test balances
    print("\n1. Testing balances...")
    try:
        balances = fetch_lbank_all_balances()
        print(f"Spot assets: {len(balances['spot'])}")
        for coin, amount in list(balances['spot'].items())[:5]:
            print(f"  {coin}: {amount}")
    except Exception as e:
        print(f"Error: {e}")
    
    # Test closed positions (dry run)
    print("\n2. Testing closed positions (dry run)...")
    try:
        # ⚠️ IMPORTANTE: Añadimos JELLYJELLY al cache para que lo procese.
        add_manual_lbank_pair("JELLYJELLY")
        
        # Procesamos SÓLO el símbolo que queremos probar
        save_lbank_closed_positions(
            symbols=["JELLYJELLY"],
            days=15, 
            dry_run=True
        )
    except Exception as e:
        print(f"Error: {e}")