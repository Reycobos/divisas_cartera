"""
LBank Exchange Adapter - Solo Spot (sin futuros)
================================================
Implementa:
- Balances spot desde /v2/supplement/user_info.do
- Reconstrucción FIFO de posiciones cerradas desde trades (/v2/supplement/transaction_history.do)

LBank NO tiene futuros, solo spot trading.
"""

import hashlib
import hmac
import sys
import time
import requests
from collections import defaultdict
from typing import List, Dict, Any, Optional
import os

from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
load_dotenv()

# Importar el cliente del SDK
from lbank import BlockHttpClient
# ... (el resto de tus imports) ...

# =============================================================================
# CONFIGURACIÓN
# =============================================================================
BASE_URL = "https://api.lbkex.com"
# ... (API_KEY, SECRET_KEY) ...

# 🆕 Cliente del SDK
# Inicializamos el cliente una vez
try:
    LBANK_CLIENT = BlockHttpClient(
        sign_method="HmacSHA256", # Usamos HmacSHA256, el que usas en tu código
        api_key=API_KEY,
        api_secret=SECRET_KEY,
        base_url=BASE_URL,
        log_level=1 # INFO
    )
except Exception as e:
    print(f"[LBANK SDK] ❌ Error inicializando el cliente SDK: {e}")
    LBANK_CLIENT = None




# Obtener la ruta del directorio actual (adapters/)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Obtener la ruta del directorio padre (proyecto_raiz/)
parent_dir = os.path.dirname(current_dir)

# Añadir el directorio padre al sys.path para que Python lo vea
if parent_dir not in sys.path:
    sys.path.append(parent_dir)
# Importar cache universal para gestión de pares
try:
    from universal_cache import (
        init_universal_cache_db,
        add_to_universal_cache,
        get_cached_currency_pairs,
        update_cache_from_positions
    )
    CACHE_AVAILABLE = True
except ImportError:
    CACHE_AVAILABLE = False
    print("[LBANK] ⚠️ universal_cache no disponible - funcionará sin cache")

__all__ = [
    "fetch_lbank_all_balances",
    "save_lbank_closed_positions",
    "add_manual_lbank_pair",
    "set_debug_mode",
]

# =============================================================================
# CONFIGURACIÓN
# =============================================================================
API_KEY = os.getenv("LBANK_API_KEY", "")
SECRET_KEY = os.getenv("LBANK_SECRET_KEY", "")

# Para debugging
DEBUG_REQUESTS = False

# Tokens base a ignorar en el cache (no son pares de trading)
IGNORE_TOKENS = {"BTC", "ETH", "USDT", "USDC", "BUSD"}


# =============================================================================
# UTILIDADES
# =============================================================================
def set_debug_mode(enabled: bool = True):
    """
    Activa/desactiva el modo debug para ver requests detallados
    
    Args:
        enabled: True para activar debug, False para desactivar
    
    Ejemplo:
        >>> from adapters.lbank import set_debug_mode
        >>> set_debug_mode(True)  # Ver todos los requests
        >>> save_lbank_closed_positions(symbols=["BTC"], days=7, dry_run=True)
    """
    global DEBUG_REQUESTS
    DEBUG_REQUESTS = enabled
    status = "activado" if enabled else "desactivado"
    print(f"[LBANK] 🔧 Modo debug {status}")


# =============================================================================
# CACHE MANAGEMENT
# =============================================================================
def _update_cache_from_balances(balances: dict):
    """
    Actualiza el cache universal desde los balances spot de LBank
    Convierte tokens a trading pairs (ej: JELLYJELLY → jellyjelly_usdt)
    Ignora tokens base (BTC, ETH, USDT, etc.)
    """
    if not CACHE_AVAILABLE:
        return
    
    try:
        init_universal_cache_db()
        
        spot_balances = balances.get("spot", {})
        cached_count = 0
        
        for token, amount in spot_balances.items():
            token_upper = token.upper()
            
            # Ignorar tokens base y balances muy pequeños
            if token_upper in IGNORE_TOKENS or amount < 0.0001:
                continue
            
            # Convertir a trading pair: TOKEN → token_usdt
            trading_pair = f"{token.lower()}_usdt"
            
            # Agregar al cache
            add_to_universal_cache(
                exchange="lbank",
                symbol=f"{token_upper}USDT",  # Formato normalizado
                currency_pair=trading_pair,   # Formato API de LBank
                symbol_type="spot"
            )
            cached_count += 1
        
        if cached_count > 0:
            print(f"[LBANK CACHE] ✅ {cached_count} pares agregados al cache")
    
    except Exception as e:
        print(f"[LBANK CACHE] ⚠️ Error actualizando cache: {e}")


def _get_cached_trading_pairs() -> List[str]:
    """
    Obtiene los trading pairs cacheados para LBank
    Returns: Lista de pares en formato lbank (ej: ["btc_usdt", "eth_usdt"])
    """
    if not CACHE_AVAILABLE:
        return []
    
    try:
        pairs = get_cached_currency_pairs("lbank")
        return pairs or []
    except Exception as e:
        print(f"[LBANK CACHE] ⚠️ Error obteniendo cache: {e}")
        return []


def add_manual_lbank_pair(symbol: str) -> bool:
    """
    Agrega manualmente un trading pair al cache universal
    
    Args:
        symbol: Símbolo a agregar, acepta múltiples formatos:
                - "JELLYJELLY" → se convierte a jellyjelly_usdt
                - "jellyjelly" → se convierte a jellyjelly_usdt  
                - "JELLYJELLY/USDT" → se convierte a jellyjelly_usdt
                - "jellyjelly_usdt" → se usa tal cual
    
    Returns:
        True si se agregó correctamente, False si hubo error
    
    Ejemplo:
        >>> add_manual_lbank_pair("JELLYJELLY")
        [LBANK] ✅ Par agregado al cache: jellyjelly_usdt
        True
        
        >>> add_manual_lbank_pair("OP")  
        [LBANK] ✅ Par agregado al cache: op_usdt
        True
    """
    if not CACHE_AVAILABLE:
        print("[LBANK] ⚠️ Cache universal no disponible")
        return False
    
    try:
        # Normalizar el símbolo a formato LBank
        symbol = symbol.strip().upper()
        
        # Quitar /USDT si existe
        if "/" in symbol:
            symbol = symbol.split("/")[0]
        
        # Quitar _usdt si existe
        if "_" in symbol:
            base = symbol.split("_")[0]
        else:
            base = symbol
        
        # Convertir a formato LBank: lowercase_usdt
        currency_pair = f"{base.lower()}_usdt"
        
        # Agregar al cache
        init_universal_cache_db()
        add_to_universal_cache(
            exchange="lbank",
            symbol=f"{base}USDT",  # Formato normalizado
            currency_pair=currency_pair,  # Formato API
            symbol_type="spot"
        )
        
        print(f"[LBANK] ✅ Par agregado al cache: {currency_pair}")
        return True
    
    except Exception as e:
        print(f"[LBANK] ❌ Error agregando par: {e}")
        return False


# =============================================================================
# AUTENTICACIÓN LBANK
# =============================================================================
def _generate_echostr(length=35):
    """Genera un echostr aleatorio (30-40 caracteres)"""
    import random
    import string
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


def _sign_request(params: dict) -> str:
    """
    Firma los parámetros según la especificación de LBank:
    1. Ordena parámetros alfabéticamente
    2. Genera MD5 en mayúsculas
    3. Firma con HmacSHA256
    """
    # 1. Ordenar parámetros alfabéticamente (excluir 'sign')
    sorted_params = sorted(params.items())
    param_str = "&".join(f"{k}={v}" for k, v in sorted_params if k != "sign")
    
    if DEBUG_REQUESTS:
        print(f"[LBANK AUTH] Param string: {param_str}")
    
    # 2. MD5 digest en mayúsculas
    md5_digest = hashlib.md5(param_str.encode('utf-8')).hexdigest().upper()
    
    if DEBUG_REQUESTS:
        print(f"[LBANK AUTH] MD5 digest: {md5_digest}")
    
    # 3. HmacSHA256 con secret key
    signature = hmac.new(
        SECRET_KEY.encode('utf-8'),
        md5_digest.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    
    if DEBUG_REQUESTS:
        print(f"[LBANK AUTH] Signature: {signature}")
    
    return signature


def _make_signed_request(endpoint: str, params: dict = None) -> dict:
    """
    Hace una request firmada a LBank
    Headers requeridos: contentType, timestamp, signature_method, echostr
    """
    if not API_KEY or not SECRET_KEY:
        raise ValueError("LBANK_API_KEY y LBANK_SECRET_KEY deben estar configurados")
    
    url = f"{BASE_URL}{endpoint}"
    
    # Parámetros base requeridos
    base_params = {
        "api_key": API_KEY,
        "signature_method": "HmacSHA256",
        "timestamp": str(int(time.time() * 1000)),
        "echostr": _generate_echostr()
    }
    
    # Merge con parámetros adicionales
    if params:
        base_params.update(params)
    
    # Generar firma
    signature = _sign_request(base_params)
    base_params["sign"] = signature
    
    # Headers requeridos
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    if DEBUG_REQUESTS:
        print(f"[LBANK REQUEST] URL: {url}")
        print(f"[LBANK REQUEST] Params: {base_params}")
    
    try:
        response = requests.post(url, data=base_params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if DEBUG_REQUESTS:
            print(f"[LBANK RESPONSE] Status: {response.status_code}")
            print(f"[LBANK RESPONSE] Data: {data}")
        
        # Verificar respuesta de error de LBank
        if not data.get("result", False):
            error_code = data.get("error_code", "unknown")
            raise Exception(f"LBank API error: {error_code}")
        
        return data.get("data", {})
    
    except requests.exceptions.RequestException as e:
        print(f"[LBANK ERROR] Request failed: {e}")
        raise


# =============================================================================
# NORMALIZACIÓN DE SÍMBOLOS
# =============================================================================
def _normalize_symbol(raw_symbol: str) -> str:
    """
    Normaliza símbolos de LBank al formato estándar
    LBank usa formato: btc_usdt, eth_usdt, etc.
    Salida: BTC/USDT, ETH/USDT
    """
    if not raw_symbol:
        return ""
    
    # Convertir a mayúsculas y reemplazar _ por /
    parts = raw_symbol.upper().split("_")
    if len(parts) == 2:
        return f"{parts[0]}/{parts[1]}"
    
    return raw_symbol.upper()


# =============================================================================
# BALANCES
# =============================================================================
def fetch_lbank_all_balances(api_key: str = None, secret_key: str = None) -> dict:
    """
    Obtiene todos los balances spot de LBank
    
    Endpoint: POST /v2/supplement/user_info.do
    
    IMPORTANTE: También actualiza el cache universal con los pares spot
    para poder consultar trades posteriormente.
    
    Returns:
        {
            "spot": {"BTC": 1.234, "USDT": 5000.0, ...},
            "margin": {},  # LBank no tiene margin
            "futures": {}, # LBank no tiene futures
            "total_usdt": 0.0,  # No calculamos valor en USDT
            "total_usd": 0.0
        }
    """
    global API_KEY, SECRET_KEY
    
    if api_key:
        API_KEY = api_key
    if secret_key:
        SECRET_KEY = secret_key
    
    try:
        # Llamada a la API de user info
        data = _make_signed_request("/v2/supplement/user_info.do")
        
        balances = {"spot": {}, "margin": {}, "futures": {}}
        
        # data es una lista de monedas con sus balances
        if isinstance(data, list):
            for coin_info in data:
                coin = coin_info.get("coin", "").upper()
                usable_amt = float(coin_info.get("usableAmt", 0))
                
                # Solo incluir si tiene balance positivo
                if usable_amt > 0:
                    balances["spot"][coin] = usable_amt
        
        # LBank solo tiene spot, no calculamos totales en USD
        balances["total_usdt"] = 0.0
        balances["total_usd"] = 0.0
        
        # ✅ ACTUALIZAR CACHE con los balances
        # _update_cache_from_balances(balances)
        
        return balances
    
    except Exception as e:
        print(f"[LBANK] Error obteniendo balances: {e}")
        return {"spot": {}, "margin": {}, "futures": {}, "total_usdt": 0.0, "total_usd": 0.0}


# =============================================================================
# RECONSTRUCCIÓN FIFO DE POSICIONES CERRADAS
# =============================================================================
class FIFOQueue:
    """Cola FIFO para reconstruir posiciones"""
    
    def __init__(self):
        self.lots = []  # [(qty, price, timestamp, fee), ...]
    
    def add(self, qty: float, price: float, timestamp: int, fee: float):
        """Agrega un lote de entrada"""
        self.lots.append({
            "qty": qty,
            "price": price,
            "timestamp": timestamp,
            "fee": fee
        })
    
    def consume(self, qty_to_close: float) -> tuple:
        """
        Consume qty_to_close de la cola FIFO
        
        Returns:
            (avg_entry_price, total_qty_consumed, total_fees, first_timestamp)
        """
        remaining = qty_to_close
        total_value = 0.0
        total_qty = 0.0
        total_fees = 0.0
        first_ts = None
        
        while remaining > 0 and self.lots:
            lot = self.lots[0]
            
            if first_ts is None:
                first_ts = lot["timestamp"]
            
            if lot["qty"] <= remaining:
                # Consumir todo el lote
                total_value += lot["qty"] * lot["price"]
                total_qty += lot["qty"]
                total_fees += lot["fee"]
                remaining -= lot["qty"]
                self.lots.pop(0)
            else:
                # Consumir parcialmente
                consumed_qty = remaining
                total_value += consumed_qty * lot["price"]
                total_qty += consumed_qty
                # Fee proporcional
                fee_fraction = consumed_qty / lot["qty"]
                total_fees += lot["fee"] * fee_fraction
                
                # Actualizar el lote
                lot["qty"] -= consumed_qty
                lot["fee"] *= (1 - fee_fraction)
                remaining = 0
        
        avg_price = total_value / total_qty if total_qty > 0 else 0.0
        return avg_price, total_qty, total_fees, first_ts
    
    def is_empty(self) -> bool:
        return len(self.lots) == 0
    
    def total_qty(self) -> float:
        return sum(lot["qty"] for lot in self.lots)


def _fetch_trades(symbol: str = None, start_date: str = None, end_date: str = None, 
                  days: int = 30) -> List[dict]:
    """
    Obtiene trades históricos de LBank
    
    Endpoint: POST /v2/supplement/transaction_history.do
    
    Args:
        symbol: Par de trading (ej: "btc_usdt"). Si None, usa el cache
        start_date: Fecha inicio (yyyy-MM-dd o yyyy-MM-dd HH:mm:ss UTC+8)
        end_date: Fecha fin (yyyy-MM-dd o yyyy-MM-dd HH:mm:ss UTC+8)
        days: Días hacia atrás si no se especifican fechas
    
    Returns:
        Lista de trades con formato:
        [{
            "symbol": "lbk_usdt",
            "id": "trade-id",
            "orderId": "order-id",
            "price": "4.00000100",
            "qty": "12.00000000",
            "quoteQty": "48.000012",
            "commission": "10.10000000",
            "time": 1499865549590,
            "isBuyer": true,
            "isMaker": false
        }, ...]
    """
    
    # Si no hay símbolo específico, obtener del cache
    if not symbol:
        cached_pairs = _get_cached_trading_pairs()
        if not cached_pairs:
            print("[LBANK] ⚠️ No hay pares en cache. Ejecuta fetch_lbank_all_balances() primero.")
            return []
        
        # Obtener trades de todos los pares cacheados
        all_trades = []
        for pair in cached_pairs:
            trades = _fetch_trades_for_symbol(pair, start_date, end_date, days)
            all_trades.extend(trades)
            time.sleep(0.2)  # Rate limiting
        
        return all_trades
    
    # Si hay símbolo, obtener solo ese
    return _fetch_trades_for_symbol(symbol, start_date, end_date, days)


def _fetch_trades_for_symbol(symbol: str, start_date: str = None, end_date: str = None,
                             days: int = 30) -> List[dict]:
    """
    Obtiene trades de un símbolo específico usando el SDK de LBank.
    Maneja ventanas de 1 día.
    """
    if not LBANK_CLIENT:
        print("[LBANK] ❌ Cliente SDK no disponible. Imposible obtener trades.")
        return []
        
    all_trades = []
    
    # ... (La lógica de cálculo de fechas es la misma) ...
    if not end_date:
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    if not start_date:
        start = datetime.now(timezone.utc) - timedelta(days=days)
        start_date = start.strftime("%Y-%m-%d")
    
    current_start = datetime.strptime(start_date, "%Y-%m-%d")
    final_end = datetime.strptime(end_date, "%Y-%m-%d")
    
    print(f"[LBANK TRADES] Buscando {symbol} desde {start_date} hasta {end_date} (vía SDK)")

    while current_start <= final_end:
        date_str = current_start.strftime("%Y-%m-%d")
        
        # 🚨 Parámetros para el SDK (usando el endpoint principal)
        params = {
            "symbol": symbol.lower().replace("/", "_"),
            "startTime": date_str,
            "endTime": date_str,
            "limit": "100"
        }
        
        try:
            # 🚀 USANDO EL CLIENTE SDK OFICIAL
            trades_response = LBANK_CLIENT.http_request(
                "post", 
                "v2/transaction_history.do", # Usamos el endpoint principal
                payload=params
            )
            
            if trades_response.get("result") == True:
                trades = trades_response.get("data", [])
                
                if isinstance(trades, list) and trades:
                    all_trades.extend(trades)
                    print(f"[LBANK TRADES] ✅ {len(trades)} trades de {symbol} encontrados en {date_str}")
                else:
                    # No hay trades en ese día (respuesta exitosa, lista vacía)
                    if DEBUG_REQUESTS:
                        print(f"[LBANK TRADES] ℹ️ {symbol}: No hay trades propios en {date_str}")

            else:
                # La API devolvió error (e.g., 10008, 10005, etc.)
                error_code = trades_response.get("error_code", "N/A")
                error_msg = trades_response.get("msg", "Error desconocido")
                
                if error_code == 10008 or error_msg == 'currency pair nonsupport':
                    if DEBUG_REQUESTS:
                        print(f"[LBANK TRADES] ⚠️ {symbol}: Sin trades o par no soportado en {date_str} (Error {error_code})")
                else:
                    print(f"[LBANK] ❌ Error {error_code} obteniendo trades de {symbol} en {date_str}: {error_msg}")
        
        except Exception as e:
            print(f"[LBANK] ❌ Excepción al usar SDK para {symbol} en {date_str}: {e}")
        
        # Avanzar al siguiente día
        current_start = current_start + timedelta(days=1)
        time.sleep(0.3) 
    
    return all_trades


def _reconstruct_fifo_positions(trades: List[dict]) -> List[dict]:
    """
    Reconstruye posiciones cerradas usando FIFO desde trades
    
    Args:
        trades: Lista de trades ordenados por timestamp
    
    Returns:
        Lista de posiciones cerradas reconstruidas
    """
    # Agrupar por símbolo
    by_symbol = defaultdict(list)
    for trade in trades:
        symbol = _normalize_symbol(trade.get("symbol", ""))
        by_symbol[symbol].append(trade)
    
    closed_positions = []
    
    for symbol, symbol_trades in by_symbol.items():
        # Ordenar por timestamp
        symbol_trades.sort(key=lambda t: int(t.get("time", 0)))
        
        # Estado: colas separadas para long y short
        long_queue = FIFOQueue()
        short_queue = FIFOQueue()
        
        # Variables para rastrear el bloque actual
        current_block = None
        block_trades = []
        
        for trade in symbol_trades:
            price = float(trade.get("price", 0))
            qty = float(trade.get("qty", 0))
            is_buyer = trade.get("isBuyer", True)
            timestamp = int(trade.get("time", 0)) // 1000  # ms -> s
            fee = float(trade.get("commission", 0))
            
            # BUY = long, SELL = short
            side = "long" if is_buyer else "short"
            
            # Inicializar bloque si es el primero
            if current_block is None:
                current_block = {
                    "side": side,
                    "symbol": symbol,
                    "open_time": timestamp,
                    "close_time": timestamp,
                    "max_size": 0.0,
                    "total_fee": 0.0,
                    "entry_lots": [],
                    "exit_lots": []
                }
                block_trades = [trade]
            
            block_trades.append(trade)
            current_block["close_time"] = timestamp
            current_block["total_fee"] += fee
            
            # Lógica FIFO
            if side == current_block["side"]:
                # Mismo lado: aumentar posición
                if side == "long":
                    long_queue.add(qty, price, timestamp, fee)
                else:
                    short_queue.add(qty, price, timestamp, fee)
                
                current_block["entry_lots"].append({
                    "qty": qty,
                    "price": price,
                    "timestamp": timestamp,
                    "fee": fee
                })
                
                # Actualizar tamaño máximo
                current_size = long_queue.total_qty() if side == "long" else short_queue.total_qty()
                current_block["max_size"] = max(current_block["max_size"], current_size)
            
            else:
                # Lado opuesto: cerrar posición FIFO
                if current_block["side"] == "long":
                    # Cerrando longs con una venta
                    if not long_queue.is_empty():
                        avg_entry, consumed_qty, entry_fees, first_ts = long_queue.consume(qty)
                        
                        current_block["exit_lots"].append({
                            "qty": consumed_qty,
                            "price": price,
                            "timestamp": timestamp,
                            "fee": fee,
                            "entry_price": avg_entry,
                            "entry_fees": entry_fees
                        })
                        
                        # Si la cola quedó vacía, cerrar el bloque
                        if long_queue.is_empty():
                            closed_positions.append(_finalize_block(current_block))
                            current_block = None
                            block_trades = []
                
                else:
                    # Cerrando shorts con una compra
                    if not short_queue.is_empty():
                        avg_entry, consumed_qty, entry_fees, first_ts = short_queue.consume(qty)
                        
                        current_block["exit_lots"].append({
                            "qty": consumed_qty,
                            "price": price,
                            "timestamp": timestamp,
                            "fee": fee,
                            "entry_price": avg_entry,
                            "entry_fees": entry_fees
                        })
                        
                        # Si la cola quedó vacía, cerrar el bloque
                        if short_queue.is_empty():
                            closed_positions.append(_finalize_block(current_block))
                            current_block = None
                            block_trades = []
    
    return closed_positions


def _finalize_block(block: dict) -> dict:
    """
    Finaliza un bloque FIFO y calcula todas las métricas
    
    Returns:
        Dict con formato esperado por save_closed_position
    """
    symbol = block["symbol"]
    side = block["side"]
    
    # Calcular entry_price ponderado
    total_entry_value = 0.0
    total_entry_qty = 0.0
    for lot in block["entry_lots"]:
        total_entry_value += lot["qty"] * lot["price"]
        total_entry_qty += lot["qty"]
    
    entry_price = total_entry_value / total_entry_qty if total_entry_qty > 0 else 0.0
    
    # Calcular close_price ponderado
    total_exit_value = 0.0
    total_exit_qty = 0.0
    for lot in block["exit_lots"]:
        total_exit_value += lot["qty"] * lot["price"]
        total_exit_qty += lot["qty"]
    
    close_price = total_exit_value / total_exit_qty if total_exit_qty > 0 else 0.0
    
    # Size = máximo neto del bloque
    size = block["max_size"]
    
    # PnL de precio (según side)
    if side == "long":
        price_pnl = (close_price - entry_price) * size
    else:  # short
        price_pnl = (entry_price - close_price) * size
    
    # Fees (negativas)
    fee_total = -abs(block["total_fee"])
    
    # Funding (spot no tiene, dejamos en 0)
    funding_total = 0.0
    
    # Realized PnL
    realized_pnl = price_pnl + funding_total + fee_total
    
    # Notional
    notional = size * entry_price
    
    # Tiempos
    open_time = block["open_time"]
    close_time = block["close_time"]
    
    return {
        "exchange": "lbank",
        "symbol": symbol,
        "side": side,
        "size": size,
        "entry_price": entry_price,
        "close_price": close_price,
        "open_time": open_time,
        "close_time": close_time,
        "pnl": price_pnl,
        "realized_pnl": realized_pnl,
        "funding_total": funding_total,
        "fee_total": fee_total,
        "notional": notional,
        "leverage": 1.0,  # Spot = sin apalancamiento
        "initial_margin": notional,  # En spot, margin = notional
        "liquidation_price": 0.0,  # Spot no tiene liquidación
        "_lock_size": True  # No permitir que save_closed_position recalcule size
    }


def save_lbank_closed_positions(db_path: str = "portfolio.db", days: int = 30, 
                                 dry_run: bool = False, api_key: str = None, 
                                 secret_key: str = None, symbols: List[str] = None) -> int:
    """
    Reconstruye posiciones cerradas desde trades usando FIFO y las guarda en DB
    
    IMPORTANTE: Usa el cache universal para saber qué símbolos consultar.
    Puedes agregar pares manualmente con add_manual_lbank_pair() o pasarlos directamente.
    
    Args:
        db_path: Ruta a la base de datos SQLite
        days: Días hacia atrás para obtener trades
        dry_run: Si True, solo imprime sin guardar
        api_key: API key de LBank (opcional, usa env var si no se provee)
        secret_key: Secret key de LBank (opcional, usa env var si no se provee)
        symbols: Lista de símbolos específicos (ej: ["JELLYJELLY", "OP", "ARB"])
                 Si se proporciona, se agregan al cache automáticamente
                 Si es None, usa todos los pares del cache existente
    
    Returns:
        Número de posiciones guardadas
    
    Ejemplo:
        >>> # Buscar símbolos específicos
        >>> save_lbank_closed_positions(symbols=["JELLYJELLY", "OP"], days=30, dry_run=True)
        
        >>> # Usar cache existente
        >>> save_lbank_closed_positions(days=30, dry_run=True)
    """
    global API_KEY, SECRET_KEY
    
    if api_key:
        API_KEY = api_key
    if secret_key:
        SECRET_KEY = secret_key
    
    print(f"\n{'='*60}")
    print(f"🔄 Sincronizando posiciones cerradas de LBank (FIFO)")
    print(f"📅 Ventana: últimos {days} días")
    print(f"{'='*60}\n")
    
    try:
        # 1. PRIMERO obtener balances para actualizar el cache
        print("📥 Obteniendo balances para actualizar cache...")
        balances = fetch_lbank_all_balances(api_key=api_key, secret_key=secret_key)
        spot_count = len(balances.get("spot", {}))
        print(f"✅ {spot_count} monedas en balance\n")
        
        # 2. Procesar símbolos específicos o usar cache
        pairs_to_query = []
        
        if symbols:
            # Modo manual: agregar símbolos al cache y usarlos
            print(f"📌 Agregando {len(symbols)} símbolos al cache...")
            for sym in symbols:
                add_manual_lbank_pair(sym)
            print()
            
            # Obtener los pares recién agregados del cache
            pairs_to_query = _get_cached_trading_pairs()
            
            if not pairs_to_query:
                print("⚠️  Error: No se pudieron agregar los símbolos al cache")
                return 0
                
            print(f"📦 Usando {len(pairs_to_query)} pares:")
            for pair in pairs_to_query:
                print(f"   - {pair}")
            print()
        else:
            # Modo automático: usar cache existente
            cached_pairs = _get_cached_trading_pairs()
            if not cached_pairs:
                print("⚠️  No hay pares en cache")
                print("   Opciones:")
                print("   1. Agregar manualmente: add_manual_lbank_pair('JELLYJELLY')")
                print("   2. Pasar lista: save_lbank_closed_positions(symbols=['JELLYJELLY', 'OP'])")
                return 0
            
            pairs_to_query = cached_pairs
            print(f"📦 {len(pairs_to_query)} pares en cache:")
            for pair in pairs_to_query[:10]:
                print(f"   - {pair}")
            if len(pairs_to_query) > 10:
                print(f"   ... y {len(pairs_to_query) - 10} más")
            print()
        
        # 3. Obtener trades usando el cache
        print("📥 Obteniendo trades históricos de todos los pares...")
        trades = _fetch_trades(days=days)
        print(f"✅ {len(trades)} trades obtenidos\n")
        
        if not trades:
            print("⚠️  No hay trades para procesar")
            print("   Posibles causas:")
            print("   1. No has hecho trades en estos pares")
            print("   2. Los trades son más antiguos que la ventana de búsqueda")
            print("   3. Error 10008 = LBank no tiene datos para estos pares/fechas")
            print("\n💡 Sugerencia:")
            print("   - Verifica tus balances: fetch_lbank_all_balances()")
            print("   - Prueba con pares donde SEPAS que has hecho trades")
            print("   - Aumenta el período: save_lbank_closed_positions(symbols=[...], days=90)")
            return 0
        
        # 4. Reconstruir posiciones FIFO
        print("🔄 Reconstruyendo posiciones con FIFO...")
        closed_positions = _reconstruct_fifo_positions(trades)
        print(f"✅ {len(closed_positions)} posiciones cerradas reconstruidas\n")
        
        if not closed_positions:
            print("⚠️  No se reconstruyeron posiciones cerradas")
            return 0
        
        # 5. Guardar en DB (si no es dry_run)
        if dry_run:
            print("🔍 DRY RUN - No se guardará en DB\n")
            for i, pos in enumerate(closed_positions, 1):
                print(f"\n📦 Posición {i}/{len(closed_positions)}:")
                print(f"   Symbol: {pos['symbol']}")
                print(f"   Side: {pos['side']}")
                print(f"   Size: {pos['size']:.6f}")
                print(f"   Entry: ${pos['entry_price']:.6f}")
                print(f"   Close: ${pos['close_price']:.6f}")
                print(f"   PnL: ${pos['realized_pnl']:.2f}")
                print(f"   Fees: ${pos['fee_total']:.2f}")
                print(f"   Open: {datetime.fromtimestamp(pos['open_time']).strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"   Close: {datetime.fromtimestamp(pos['close_time']).strftime('%Y-%m-%d %H:%M:%S')}")
            return 0
        
        else:
            print("💾 Guardando en base de datos...")
            
            # Importar aquí para evitar dependencia circular
            try:
                from db_manager import save_closed_position
            except ImportError:
                print("⚠️  No se pudo importar db_manager. Asegúrate de que existe.")
                return 0
            
            saved_count = 0
            for pos in closed_positions:
                try:
                    save_closed_position(pos)
                    saved_count += 1
                except Exception as e:
                    print(f"❌ Error guardando posición {pos['symbol']}: {e}")
            
            print(f"✅ {saved_count}/{len(closed_positions)} posiciones guardadas\n")
            return saved_count
    
    except Exception as e:
        print(f"❌ Error en save_lbank_closed_positions: {e}")
        import traceback
        traceback.print_exc()
        return 0


# =============================================================================
# TESTING RÁPIDO
# =============================================================================
# =============================================================================
# TESTING RÁPIDO
# =============================================================================
# =============================================================================
# TESTING RÁPIDO
# =============================================================================
if __name__ == "__main__":
    print("LBank Adapter - Testing")
    print("=" * 60)
    
    # Activar debug para ver qué está pasando
    set_debug_mode(True)
    
    # Test balances
    print("\n1. Testing balances...")
    try:
        balances = fetch_lbank_all_balances()
        print(f"Spot assets: {len(balances['spot'])}")
        for coin, amount in list(balances['spot'].items())[:10]:
            print(f"  {coin}: {amount}")
    except Exception as e:
        print(f"Error: {e}")
    
    # Test closed positions (dry run)
    print("\n2. Testing closed positions (dry run)...")
    try:
        # 1. Aumentar 'days' a 15 para encontrar trades de Nov 5
        # 2. Especificar 'symbols' para poblar el cache
        symbols_to_test = ["JELLYJELLY"] # <-- CAMBIO: Solo JELLYJELLY
        
        save_lbank_closed_positions(
            symbols=symbols_to_test, 
            days=15,  # Aumentado para cubrir el 5 de Nov
            dry_run=True
        )
    except Exception as e:
        print(f"Error: {e}")