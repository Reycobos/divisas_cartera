import os
import requests
import time
import hmac
import hashlib
import base64
import nacl.signing
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv()

# Configuración Backpack
BACKPACK_API_KEY = os.getenv("BACKPACK_API_KEY")
BACKPACK_API_SECRET = os.getenv("BACKPACK_API_SECRET")
BACKPACK_BASE_URL = "https://api.backpack.exchange"

def _bp_sign_message(instruction: str, params: dict | None, ts_ms: int, window_ms: int = 5000) -> str:
    """Firma para Backpack API"""
    try:
        query = ""
        if params:
            sorted_items = sorted(params.items())
            query = urlencode(sorted_items, doseq=True)

        if query:
            to_sign = f"instruction={instruction}&{query}&timestamp={ts_ms}&window={window_ms}"
        else:
            to_sign = f"instruction={instruction}&timestamp={ts_ms}&window={window_ms}"

        seed32 = base64.b64decode(BACKPACK_API_SECRET)
        signing_key = nacl.signing.SigningKey(seed32)
        sig_bytes = signing_key.sign(to_sign.encode("utf-8")).signature
        return base64.b64encode(sig_bytes).decode("ascii")
    except Exception as e:
        print(f"❌ Error en firma: {e}")
        return ""

def backpack_signed_request_debug(method: str, path: str, instruction: str, params: dict | None = None):
    """Request firmado a Backpack con debug completo"""
    try:
        ts_ms = int(time.time() * 1000)
        window_ms = 5000

        sign_params = params if method.upper() == "GET" else {}
        signature_b64 = _bp_sign_message(instruction, sign_params, ts_ms, window_ms)
        
        headers = {
            "X-API-KEY": BACKPACK_API_KEY,
            "X-SIGNATURE": signature_b64,
            "X-TIMESTAMP": str(ts_ms),
            "X-WINDOW": str(window_ms),
            "Content-Type": "application/json; charset=utf-8",
        }

        url = f"{BACKPACK_BASE_URL}{path}"
        print(f"🔧 Request URL: {url}")
        print(f"🔧 Params: {params}")
        print(f"🔧 Headers: {{'X-API-KEY': '{BACKPACK_API_KEY[:10]}...', 'X-SIGNATURE': '{signature_b64[:20]}...'}}")
        
        r = requests.get(url, headers=headers, params=params or {}, timeout=30)
        print(f"🔧 Response Status: {r.status_code}")
        print(f"🔧 Response Headers: {dict(r.headers)}")
        
        r.raise_for_status()
        data = r.json()
        print(f"🔧 Response Data: {data}")
        return data
    except Exception as e:
        print(f"❌ Error en request: {e}")
        if 'r' in locals():
            print(f"🔧 Response Text: {r.text}")
        return None

def debug_backpack_api_complete():
    """Debug completo de la API de Backpack"""
    print("🔍 DEBUG COMPLETO API BACKPACK")
    print("=" * 60)
    
    # 1. Probar endpoint de balances primero
    print("\n1. 📊 Probando endpoint de capital...")
    try:
        capital_data = backpack_signed_request_debug(
            "GET", "/api/v1/capital", instruction="capitalQueryAll"
        )
        print(f"✅ Capital response: {capital_data}")
    except Exception as e:
        print(f"❌ Error en capital: {e}")
    
    # 2. Probar endpoint de posiciones
    print("\n2. 📈 Probando endpoint de posiciones...")
    try:
        positions_data = backpack_signed_request_debug(
            "GET", "/api/v1/positions", instruction="positionQueryAll"
        )
        print(f"✅ Positions response: {positions_data}")
    except Exception as e:
        print(f"❌ Error en positions: {e}")
    
    # 3. Probar endpoint de fills sin símbolo
    print("\n3. 🔄 Probando endpoint de fills (sin símbolo)...")
    try:
        fills_all_data = backpack_signed_request_debug(
            "GET", "/wapi/v1/history/fills", instruction="fillHistoryQueryAll",
            params={"limit": 10}
        )
        print(f"✅ Fills all response: {fills_all_data}")
    except Exception as e:
        print(f"❌ Error en fills all: {e}")
    
    # 4. Probar endpoint de fills con diferentes símbolos
    print("\n4. 🎯 Probando endpoint de fills con diferentes símbolos...")
    symbols_to_test = ["KAITO_USDC_PERP", "SOL_USDC", "BTC_USDC"]
    
    for symbol in symbols_to_test:
        print(f"\n   Probando símbolo: {symbol}")
        try:
            fills_symbol_data = backpack_signed_request_debug(
                "GET", "/wapi/v1/history/fills", instruction="fillHistoryQueryAll",
                params={"limit": 10, "symbol": symbol}
            )
            print(f"   ✅ Fills {symbol}: {fills_symbol_data}")
        except Exception as e:
            print(f"   ❌ Error en fills {symbol}: {e}")
    
    # 5. Probar endpoint de orders
    print("\n5. 📋 Probando endpoint de órdenes...")
    try:
        orders_data = backpack_signed_request_debug(
            "GET", "/wapi/v1/history/orders", instruction="orderHistoryQueryAll",
            params={"limit": 10}
        )
        print(f"✅ Orders response: {orders_data}")
    except Exception as e:
        print(f"❌ Error en orders: {e}")

def debug_backpack_funding():
    """Debug del endpoint de funding"""
    print("\n💰 DEBUG ENDPOINT FUNDING")
    print("=" * 60)
    
    try:
        funding_data = backpack_signed_request_debug(
            "GET", "/wapi/v1/history/funding", instruction="fundingHistoryQueryAll",
            params={"limit": 50}
        )
        print(f"✅ Funding response: {funding_data}")
        
        if isinstance(funding_data, list):
            kaito_funding = [f for f in funding_data if "KAITO" in str(f.get('symbol', ''))]
            print(f"📊 Funding de KAITO encontrados: {len(kaito_funding)}")
            for f in kaito_funding[:5]:
                print(f"   - {f}")
    except Exception as e:
        print(f"❌ Error en funding: {e}")

def check_environment():
    """Verificar variables de entorno"""
    print("\n🔐 VERIFICACIÓN VARIABLES ENTORNO")
    print("=" * 60)
    
    print(f"BACKPACK_API_KEY: {'✅ Configurada' if BACKPACK_API_KEY else '❌ No configurada'}")
    if BACKPACK_API_KEY:
        print(f"   (primeros 10 chars): {BACKPACK_API_KEY[:10]}...")
    
    print(f"BACKPACK_API_SECRET: {'✅ Configurada' if BACKPACK_API_SECRET else '❌ No configurada'}")
    if BACKPACK_API_SECRET:
        print(f"   (longitud): {len(BACKPACK_API_SECRET)} chars")

def test_alternative_symbols():
    """Probar diferentes formatos de símbolo"""
    print("\n🔤 PROBAR FORMATOS ALTERNATIVOS DE SÍMBOLO")
    print("=" * 60)
    
    symbol_formats = [
        "KAITO_USDC",
        "KAITO-USDC", 
        "KAITOUSDC",
        "kaito_usdc",
        "KAITOUSDC",
        "KAITO_USDC_PERP"
    ]
    
    for symbol_format in symbol_formats:
        print(f"\n   Probando: '{symbol_format}'")
        try:
            data = backpack_signed_request_debug(
                "GET", "/wapi/v1/history/fills", instruction="fillHistoryQueryAll",
                params={"limit": 5, "symbol": symbol_format}
            )
            if data and isinstance(data, list) and len(data) > 0:
                print(f"   ✅ ENCONTRADOS {len(data)} trades con formato '{symbol_format}'")
                for trade in data[:2]:
                    print(f"      - {trade.get('symbol')}: {trade.get('side')} {trade.get('quantity')}")
            else:
                print(f"   ⚠️  Sin datos con formato '{symbol_format}'")
        except Exception as e:
            print(f"   ❌ Error con formato '{symbol_format}': {e}")

if __name__ == "__main__":
    print("🚀 Debug completo de API Backpack")
    check_environment()
    debug_backpack_api_complete()
    debug_backpack_funding()
    test_alternative_symbols()