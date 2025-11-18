"""
LBank API Diagnostic Tool
=========================
Prueba todos los endpoints para diagnosticar el error 10008
"""

import requests
import hashlib
import hmac
import time
import random
import string
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

# =============================================================================
# CONFIGURACIÓN
# =============================================================================
API_KEY = os.getenv("LBANK_API_KEY", "")
SECRET_KEY = os.getenv("LBANK_SECRET_KEY", "")
BASE_URL = "https://api.lbkex.com"

# Símbolos a probar
TEST_SYMBOLS = ["jellyjelly_usdt", "arb_usdt", "btc_usdt", "eth_usdt"]


# =============================================================================
# UTILIDADES
# =============================================================================
def print_section(title):
    """Imprime una sección visual"""
    print("\n" + "=" * 70)
    print(f"📋 {title}")
    print("=" * 70)


def print_json(data, title="Response"):
    """Imprime JSON formateado"""
    print(f"\n{title}:")
    print(json.dumps(data, indent=2, ensure_ascii=False))


def sign_request(params):
    """Firma los parámetros según LBank"""
    sorted_params = sorted(params.items())
    param_str = "&".join(f"{k}={v}" for k, v in sorted_params if k != "sign")
    md5_digest = hashlib.md5(param_str.encode('utf-8')).hexdigest().upper()
    signature = hmac.new(
        SECRET_KEY.encode('utf-8'),
        md5_digest.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    return signature


def make_public_request(endpoint):
    """Request GET pública"""
    url = f"{BASE_URL}{endpoint}"
    print(f"🔗 URL: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        print(f"✅ Status Code: {response.status_code}")
        return response.json()
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def make_signed_request(endpoint, params=None):
    """Request POST firmada"""
    url = f"{BASE_URL}{endpoint}"
    print(f"🔗 URL: {url}")
    
    base_params = {
        "api_key": API_KEY,
        "signature_method": "HmacSHA256",
        "timestamp": str(int(time.time() * 1000)),
        "echostr": ''.join(random.choices(string.ascii_letters + string.digits, k=32))
    }
    
    if params:
        base_params.update(params)
    
    signature = sign_request(base_params)
    base_params["sign"] = signature
    
    print(f"📤 Params: {list(base_params.keys())}")
    
    try:
        response = requests.post(url, data=base_params, timeout=30)
        print(f"✅ Status Code: {response.status_code}")
        data = response.json()
        
        # Mostrar si hay error
        if not data.get("result", False):
            error_code = data.get("error_code", "unknown")
            print(f"⚠️  API Error Code: {error_code}")
        
        return data
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


# =============================================================================
# TEST 1: PARES DISPONIBLES
# =============================================================================
print_section("TEST 1: Obtener todos los pares disponibles")
print("Endpoint: GET /v2/currencyPairs.do")

data = make_public_request("/v2/currencyPairs.do")

if data:
    pairs_list = data.get("data", data)
    print(f"\n✅ Total de pares: {len(pairs_list)}")
    
    # Buscar nuestros símbolos de prueba
    print("\n🔍 Verificando símbolos de prueba:")
    for symbol in TEST_SYMBOLS:
        exists = symbol in pairs_list
        status = "✅" if exists else "❌"
        print(f"   {status} {symbol}: {'EXISTE' if exists else 'NO ENCONTRADO'}")
    
    # Mostrar algunos pares de ejemplo
    print(f"\n📋 Primeros 20 pares:")
    for pair in pairs_list[:20]:
        print(f"   - {pair}")


# =============================================================================
# TEST 2: ACCURACY DE PARES
# =============================================================================
print_section("TEST 2: Obtener accuracy de pares")
print("Endpoint: GET /v2/accuracy.do")

data = make_public_request("/v2/accuracy.do")

if data:
    accuracy_list = data if isinstance(data, list) else data.get("data", [])
    print(f"\n✅ Total: {len(accuracy_list)} pares")
    
    # Buscar nuestros símbolos
    print("\n🔍 Accuracy de nuestros símbolos:")
    for symbol in TEST_SYMBOLS:
        found = next((item for item in accuracy_list if item.get("symbol") == symbol), None)
        if found:
            print(f"   ✅ {symbol}:")
            print(f"      Price Accuracy: {found.get('priceAccuracy')}")
            print(f"      Quantity Accuracy: {found.get('quantityAccuracy')}")
            print(f"      Min TranQua: {found.get('minTranQua', 'N/A')}")
        else:
            print(f"   ❌ {symbol}: NO ENCONTRADO")
    
    # Mostrar raw data de los primeros 5
    print("\n📄 Raw data (primeros 5):")
    print_json(accuracy_list[:5])


# =============================================================================
# TEST 3: PRECIOS ACTUALES
# =============================================================================
print_section("TEST 3: Obtener precios actuales")
print("Endpoint: GET /v2/ticker/24hr.do")

data = make_public_request("/v2/ticker/24hr.do")

if data:
    ticker_list = data.get("data", data)
    print(f"\n✅ Total: {len(ticker_list)} pares")
    
    # Buscar nuestros símbolos
    print("\n🔍 Precios de nuestros símbolos:")
    for symbol in TEST_SYMBOLS:
        found = next((item for item in ticker_list if item.get("symbol") == symbol), None)
        if found:
            print(f"   ✅ {symbol}:")
            print(f"      Price: ${found.get('ticker', {}).get('latest', 'N/A')}")
            print(f"      Volume: {found.get('ticker', {}).get('vol', 'N/A')}")
        else:
            print(f"   ❌ {symbol}: NO ENCONTRADO")


# =============================================================================
# TEST 4: TRADES RECIENTES (PÚBLICO)
# =============================================================================
print_section("TEST 4: Obtener trades recientes (PÚBLICO)")
print("Endpoint: GET /v2/trades.do?symbol=<pair>&size=10")

for symbol in TEST_SYMBOLS[:2]:  # Solo probar 2 para no saturar
    print(f"\n🔍 Probando: {symbol}")
    data = make_public_request(f"/v2/trades.do?symbol={symbol}&size=5")
    
    if data:
        trades = data.get("data", data)
        if trades:
            print(f"   ✅ {len(trades)} trades encontrados")
            print(f"   📄 Primer trade:")
            print_json(trades[0] if trades else {})
        else:
            print(f"   ⚠️  Sin trades")


# =============================================================================
# TEST 5: TRANSACTION HISTORY (PRIVADO) - ENDPOINT QUE DA ERROR
# =============================================================================
print_section("TEST 5: Transaction History (PRIVADO) - El que da error 10008")
print("Endpoint: POST /v2/supplement/transaction_history.do")

# Calcular fechas
end_date = datetime.now().strftime("%Y-%m-%d")
start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

for symbol in TEST_SYMBOLS[:2]:  # Solo probar 2
    print(f"\n🔍 Probando: {symbol}")
    print(f"   Fechas: {start_date} a {end_date}")
    
    params = {
        "symbol": symbol,
        "startTime": start_date,
        "endTime": end_date,
        "limit": "10"
    }
    
    data = make_signed_request("/v2/supplement/transaction_history.do", params)
    
    if data:
        if data.get("result"):
            trades = data.get("data", [])
            print(f"   ✅ {len(trades)} trades encontrados")
            if trades:
                print(f"   📄 Primer trade:")
                print_json(trades[0])
        else:
            error_code = data.get("error_code", "unknown")
            print(f"   ❌ Error: {error_code}")
            print(f"   📄 Raw response:")
            print_json(data)


# =============================================================================
# TEST 6: PROBAR CON DIFERENTES FORMATOS DE FECHA
# =============================================================================
print_section("TEST 6: Probar diferentes formatos de fecha")
print("Probando si el formato de fecha es el problema")

symbol = "btc_usdt"  # Usar BTC que seguro tiene trades

date_formats = [
    # Formato 1: yyyy-MM-dd
    {
        "name": "yyyy-MM-dd",
        "start": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        "end": datetime.now().strftime("%Y-%m-%d")
    },
    # Formato 2: yyyy-MM-dd HH:mm:ss
    {
        "name": "yyyy-MM-dd HH:mm:ss",
        "start": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00"),
        "end": datetime.now().strftime("%Y-%m-%d 23:59:59")
    },
    # Formato 3: Solo ayer (ventana de 1 día)
    {
        "name": "Solo ayer",
        "start": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        "end": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    },
]

for fmt in date_formats:
    print(f"\n🔍 Formato: {fmt['name']}")
    print(f"   Start: {fmt['start']}")
    print(f"   End: {fmt['end']}")
    
    params = {
        "symbol": symbol,
        "startTime": fmt['start'],
        "endTime": fmt['end'],
        "limit": "5"
    }
    
    data = make_signed_request("/v2/supplement/transaction_history.do", params)
    
    if data:
        if data.get("result"):
            trades = data.get("data", [])
            print(f"   ✅ FUNCIONA! {len(trades)} trades")
        else:
            error_code = data.get("error_code", "unknown")
            print(f"   ❌ Error: {error_code}")


# =============================================================================
# TEST 7: USER INFO (VERIFICAR AUTENTICACIÓN)
# =============================================================================
print_section("TEST 7: User Info (Verificar autenticación)")
print("Endpoint: POST /v2/supplement/user_info.do")

data = make_signed_request("/v2/supplement/user_info.do")

if data:
    if data.get("result"):
        print("   ✅ Autenticación correcta!")
        balances = data.get("data", [])
        print(f"   💰 {len(balances)} monedas en balance")
        
        # Mostrar primeras 5
        for coin in balances[:5]:
            print(f"      {coin.get('coin')}: {coin.get('usableAmt')}")
    else:
        print("   ❌ Error de autenticación")
        print_json(data)


# =============================================================================
# RESUMEN
# =============================================================================
print_section("RESUMEN Y DIAGNÓSTICO")

print("""
🔍 ANÁLISIS:

1. Si los pares EXISTEN en /v2/currencyPairs.do pero dan error 10008:
   → Verifica que hayas hecho trades en ese par
   → El endpoint solo devuelve trades que TÚ hayas hecho, no todos los del mercado

2. Si /v2/trades.do (público) funciona pero transaction_history no:
   → Confirma que tienes historial de trades propios en ese par
   → El error 10008 puede significar "sin datos para este par"

3. Si la autenticación falla:
   → Verifica LBANK_API_KEY y LBANK_SECRET_KEY

4. Si el formato de fecha es el problema:
   → Usa el formato que funcionó en el Test 6

🎯 RECOMENDACIÓN:
   - Usa /v2/trades.do (público) para ver si hay actividad en el par
   - Solo busca transaction_history en pares donde TÚ hayas hecho trades
   - Verifica tus balances en user_info para saber qué pares tienes
""")