import os
import time
import hashlib
import hmac
import base64
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from collections import defaultdict
from dotenv import load_dotenv
import requests
import json

load_dotenv()

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

API_KEY = "b917c59c-5072-4bc8-9ff2-49851b46fb1d"

# Para RSA, necesitas una private key en formato PKCS#8
SECRET_KEY = "MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBAK4QgEdJSl2iKs6jYQ/nSZKR42yLnWPpzuBW+tVGvg0Zn779Kfe1h91sqwJmpZN7TdjzT9DMLFK0rufwtdi9ZuZq0ENtJSkLRUdzQzT0Cj15SwQ9futH6UdwFmu6SgiIsI0tqoRbQresWElRAN2V4QTwzNfNGlQZJxXXR0TIQDfhAgMBAAECgYAdXflWjZ33WDHitRveJAZ8rRJysMd4IO1fWi1tqEbOTQFvpqTa/wySJhBgElNjI42JydswfhIITiWoSitUCvh+JlIskpmk3nQnNs/5QfXMVjxhIz75uxh4igFBv9gbERgaeRnXEhAaZbJ+csGNEQPIA5udeKVJt1oatiDuj2M7xQJBANQw9d1iWbfI3ZsRdzeGZpIG6eTLaAzetiXy6OAgqD/jOFwrUyASi+hvMCJ4UpmgtHLSDmiebTrNY9vxyMQwjGcCQQDSAGh4ZROf3c03iHJUU4kzsyh0hzjQ54XRZt5BxgeaziDnt1tfGBTZQoi51/AUkK1Nb9hdxnftzCNJNHPhwOx3AkEA01zFu7UZC1HBNJLMPvnYuAK8/xOCXLeHlvuE7qR5E9KBNIZPcfYneOswdlWGadNDi9AjXCDbSySGIPOR+aMmZwJBAKnxwpmL0rHRIT4Lodo0MBgyqE6FD6mfc1/ey4aW55iTr3VVoQ/3wQeBHHypD5TU8Cp2lLZu4qcCSJv5Yr4TDkkCQCXcPivznZW+IfPdvmoQ+dRGLXajRlxO9CECxZxmzSujge5Tllrn++C0/vM1ynFwQWNz14ZqNX3ocQK5liTOcjM="

BASE_URL = "https://api.lbkex.com/v2/"

import os
import time
import hashlib
import hmac
import base64
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from collections import defaultdict
from dotenv import load_dotenv
import requests
import json
import random
import string

load_dotenv()

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

API_KEY = "95b4f84f-7631-4286-9b25-5641f4fec5c3"
SECRET_KEY = "F3F6CDC794DFC8248BE98B4DB538FDF9"
BASE_URL = "https://api.lbkex.com/v2/"

# =============================================================================
# CLIENTE LBANK CORREGIDO - VERSIÓN FINAL
# =============================================================================

class LBankCorrectedClient:
    def __init__(self, api_key, secret_key, base_url):
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = base_url
        
    def _sign_payload(self, payload):
        """Firma el payload usando HMAC-SHA256 según la documentación de LBank"""
        # 1. Ordenar parámetros alfabéticamente
        sorted_params = sorted(payload.items())
        params_str = '&'.join([f"{k}={v}" for k, v in sorted_params])
        
        print(f"[LBANK SIGN] Parámetros a firmar: {params_str}")
        
        # 2. Calcular MD5 del string de parámetros (en MAYÚSCULAS)
        md5_digest = hashlib.md5(params_str.encode('utf-8')).hexdigest().upper()
        print(f"[LBANK SIGN] MD5 digest: {md5_digest}")
        
        # 3. Firmar con HMAC-SHA256
        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            md5_digest.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        print(f"[LBANK SIGN] Firma HMAC-SHA256: {signature}")
        
        return signature
    
    def _get_timestamp(self):
        """Obtener timestamp en milisegundos"""
        return str(int(time.time() * 1000))
    
    def _get_echostr(self):
        """Generar echostr aleatorio"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=35))
    
    def request(self, method, endpoint, payload=None):
        """Hacer petición a la API de LBank"""
        if payload is None:
            payload = {}
            
        # Añadir parámetros requeridos para la firma
        timestamp = self._get_timestamp()
        echostr = self._get_echostr()
        signature_method = 'HmacSHA256'
        
        full_payload = {
            'api_key': self.api_key,
            'timestamp': timestamp,
            'echostr': echostr,
            'signature_method': signature_method,
            **payload
        }
        
        # Generar firma
        signature = self._sign_payload(full_payload)
        
        # ✅ CORRECCIÓN: Mantener todos los parámetros en el payload final
        final_payload = {
            'api_key': self.api_key,
            'timestamp': timestamp,
            'echostr': echostr,
            'signature_method': signature_method,
            'sign': signature,
            **payload
        }
        
        url = self.base_url + endpoint
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'lbank-connector-python/1.0.0'
        }
        
        print(f"[LBANK REQUEST] URL: {url}")
        print(f"[LBANK REQUEST] Payload final: {final_payload}")
        
        try:
            if method.upper() == 'GET':
                response = requests.get(url, params=final_payload, headers=headers, timeout=30)
            else:
                response = requests.post(url, data=final_payload, headers=headers, timeout=30)
            
            print(f"[LBANK RESPONSE] Status: {response.status_code}")
            print(f"[LBANK RESPONSE] Text: {response.text}")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"[LBANK] ❌ Error en la petición: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"[LBANK] Response content: {e.response.text}")
            return {'result': False, 'error_code': 'NETWORK_ERROR', 'msg': str(e)}
        except json.JSONDecodeError as e:
            print(f"[LBANK] ❌ Error decodificando JSON: {e}")
            return {'result': False, 'error_code': 'JSON_ERROR', 'msg': str(e)}

# Inicializar cliente corregido
LBANK_CLIENT = LBankCorrectedClient(API_KEY, SECRET_KEY, BASE_URL)

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

# =============================================================================
# ADAPTADOR DE BALANCES
# =============================================================================

def fetch_lbank_all_balances() -> dict:
    """
    Obtiene todos los balances spot de LBank usando el cliente corregido.
    """
    print("[LBANK] 🔄 Solicitando balances spot a LBank...")
    try:
        response = LBANK_CLIENT.request("POST", "user_info.do", {})
        
        print(f"[LBANK DEBUG] Respuesta user_info: {response}")
        
        if response.get("result") is not True:
            error_code = response.get("error_code", "N/A")
            error_msg = response.get("msg", "Error desconocido")
            raise Exception(f"API Error {error_code}: {error_msg}")

        data = response.get("data", {})
        balances = defaultdict(float)
        
        for currency, info in data.items():
            try:
                if isinstance(info, dict):
                    available = float(info.get("asset", 0))
                    frozen = float(info.get("freeze", 0))
                    total = available + frozen
                    
                    if total > 0:
                        balances[currency.upper()] = total
                        print(f"[LBANK BALANCE] {currency}: {total} (available: {available}, frozen: {frozen})")
                        
            except (TypeError, ValueError) as e:
                print(f"[LBANK WARN] Error procesando balance {currency}: {e}")
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
# LÓGICA DE TRADES
# =============================================================================

def _process_trades_fifo(trades: List[Dict]) -> List[Dict]:
    """Reconstruye posiciones cerradas usando lógica FIFO"""
    if not trades:
        return []
    
    trades_by_symbol = defaultdict(list)
    for trade in trades:
        try:
            processed_trade = {
                'timestamp': int(trade.get('time_ms', trade.get('time', 0))),
                'symbol': trade.get('symbol', '').upper().replace("_", "/"),
                'side': 'BUY' if trade.get('type') == 'buy' else 'SELL',
                'amount': float(trade.get('volume', 0)),
                'price': float(trade.get('price', 0)),
                'cost': float(trade.get('trade_money', 0)),
                'fee': float(trade.get('fee', 0)),
                'fee_currency': trade.get('fee_currency', '').upper()
            }
            
            if processed_trade['amount'] > 0 and processed_trade['price'] > 0:
                trades_by_symbol[processed_trade['symbol']].append(processed_trade)
                
        except (KeyError, ValueError, TypeError) as e:
            print(f"[LBANK FIFO] ⚠️ Error procesando trade: {e}")
            continue

    closed_positions = []
    
    for symbol, symbol_trades in trades_by_symbol.items():
        symbol_trades.sort(key=lambda x: x['timestamp'])
        open_buys = []
        
        for trade in symbol_trades:
            if trade['side'] == 'BUY':
                open_buys.append(trade)
                continue

            remaining_sell_amount = trade['amount']
            
            while remaining_sell_amount > 0 and open_buys:
                open_buy = open_buys[0]
                amount_to_close = min(remaining_sell_amount, open_buy['amount'])
                ratio = amount_to_close / open_buy['amount']

                open_buy_fee = open_buy['fee'] * ratio
                close_sell_fee = trade['fee'] * (amount_to_close / trade['amount'])
                
                total_buy_cost = open_buy['cost'] * ratio
                total_sell_revenue = trade['cost'] * (amount_to_close / trade['amount'])
                
                gross_profit = total_sell_revenue - total_buy_cost
                net_profit = gross_profit - open_buy_fee - close_sell_fee

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
                
                remaining_sell_amount -= amount_to_close
                open_buy['amount'] -= amount_to_close
                
                if open_buy['amount'] <= 1e-8:
                    open_buys.pop(0)

    return closed_positions

def _fetch_trades_for_symbol(symbol: str, days: int = 30) -> List[dict]:
    """
    Obtiene trades para un símbolo específico
    """
    all_trades = []
    
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)

    current_start = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    final_end = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    print(f"[LBANK TRADES] Buscando {symbol} desde {current_start.strftime('%Y-%m-%d')} hasta {final_end.strftime('%Y-%m-%d')}")

    while current_start <= final_end:
        date_str = current_start.strftime("%Y-%m-%d")
        
        start_timestamp = str(int(current_start.timestamp() * 1000))
        end_timestamp = str(int((current_start + timedelta(days=1)).timestamp() * 1000) - 1)
        
        try:
            params = {
                "symbol": symbol.lower().replace("/", "_"),
                "startTime": start_timestamp,
                "endTime": end_timestamp,
                "limit": "100"
            }
            
            print(f"[LBANK TRADES] 🔍 Consultando {symbol} para {date_str}...")
            trades_response = LBANK_CLIENT.request("POST", "transaction_history.do", params)
            
            if trades_response.get("result") is True:
                trades = trades_response.get("data", [])
                
                if isinstance(trades, list) and trades:
                    all_trades.extend(trades)
                    print(f"[LBANK TRADES] ✅ {len(trades)} trades de {symbol} en {date_str}")
                else:
                    print(f"[LBANK TRADES] ℹ️ {symbol}: No hay trades en {date_str}")
            else:
                error_code = trades_response.get("error_code", "N/A")
                error_msg = trades_response.get("msg", "Error desconocido")
                print(f"[LBANK TRADES] ⚠️ {symbol} en {date_str}: Error {error_code} - {error_msg}")
        
        except Exception as e:
            print(f"[LBANK TRADES] ❌ Error para {symbol} en {date_str}: {e}")
        
        current_start = current_start + timedelta(days=1)
        time.sleep(0.5)  # Rate limiting
    
    print(f"[LBANK TRADES] 📊 Total trades para {symbol}: {len(all_trades)}")
    return all_trades

# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def add_manual_lbank_pair(symbol: str):
    """Añade un par manualmente al cache"""
    if CACHE_AVAILABLE:
        symbol = symbol.upper().replace("/", "")
        print(f"[LBANK] 📌 Agregando símbolo al cache: {symbol}")
        add_to_universal_cache("lbank", symbol)
        print(f"[LBANK] ✅ Par agregado al cache: {symbol}")
        
def get_lbank_symbols(symbols: Optional[List[str]] = None) -> set:
    """Obtiene los símbolos a procesar"""
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
    Obtiene, procesa y guarda posiciones cerradas de LBank
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
# TESTING
# =============================================================================
if __name__ == "__main__":
    print("LBank Adapter - Cliente Corregido (V2)")
    print("=" * 60)
    
    # Asegúrate de que el cache esté inicializado
    if CACHE_AVAILABLE:
        init_universal_cache_db()
    
    # Test de conexión básica
    print("\n0. Testing conexión básica...")
    try:
        # Probar endpoint público primero
        public_response = LBANK_CLIENT.request("GET", "currencyPairs.do", {})
        if public_response.get("data"):
            print(f"✅ Conexión OK - {len(public_response['data'])} pares disponibles")
        else:
            print("❌ Problema con endpoint público")
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        
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
        add_manual_lbank_pair("BTC")
        
        save_lbank_closed_positions(
            symbols=["BTC"],
            days=3,  # Solo 3 días para prueba rápida
            dry_run=True
        )
    except Exception as e:
        print(f"Error: {e}")