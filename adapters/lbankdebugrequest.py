"""
DEBUG: Ver exactamente qué request está fallando con error 10008
================================================================
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

# Configuración
API_KEY = os.getenv("LBANK_API_KEY", "")
SECRET_KEY = os.getenv("LBANK_SECRET_KEY", "")
BASE_URL = "https://api.lbkex.com"

def sign_request(params):
    """Firma los parámetros según LBank"""
    sorted_params = sorted(params.items())
    param_str = "&".join(f"{k}={v}" for k, v in sorted_params if k != "sign")
    print(f"\n1️⃣ Param string (antes de MD5):")
    print(f"   {param_str}")
    
    md5_digest = hashlib.md5(param_str.encode('utf-8')).hexdigest().upper()
    print(f"\n2️⃣ MD5 digest:")
    print(f"   {md5_digest}")
    
    signature = hmac.new(
        SECRET_KEY.encode('utf-8'),
        md5_digest.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    print(f"\n3️⃣ Signature:")
    print(f"   {signature}")
    
    return signature


# Símbolos a probar (los que sabemos que EXISTEN)
TEST_SYMBOLS = [

    "jellyjelly_usdt",

]

print("=" * 70)
print("🔍 DEBUG: Transaction History Request")
print("=" * 70)

for symbol in TEST_SYMBOLS[:1]:  # Solo probar el primero
    print(f"\n{'='*70}")
    print(f"Probando: {symbol}")
    print(f"{'='*70}")
    
    # Fechas (ventana de 1 día)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    # Parámetros base
    params = {
        "api_key": API_KEY,
        "signature_method": "HmacSHA256",
        "timestamp": str(int(time.time() * 1000)),
        "echostr": ''.join(random.choices(string.ascii_letters + string.digits, k=32)),
        "symbol": symbol,
        "startTime": start_date.strftime("%Y-%m-%d"),
        "endTime": end_date.strftime("%Y-%m-%d"),
        "limit": "10"
    }
    
    print(f"\n📋 Parámetros enviados:")
    for key, value in sorted(params.items()):
        if key != "sign":
            print(f"   {key}: {value}")
    
    # Firmar
    signature = sign_request(params)
    params["sign"] = signature
    
    # Request
    url = f"{BASE_URL}/v2/supplement/transaction_history.do"
    print(f"\n🔗 URL completa:")
    print(f"   {url}")
    
    print(f"\n📤 Enviando request...")
    
    try:
        response = requests.post(url, data=params, timeout=30)
        print(f"\n✅ Status Code: {response.status_code}")
        
        data = response.json()
        print(f"\n📥 Response completa:")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        if not data.get("result", False):
            error_code = data.get("error_code", "unknown")
            print(f"\n❌ ERROR CODE: {error_code}")
            
            if error_code == "10008":
                print(f"\n🔍 ANÁLISIS DEL ERROR 10008:")
                print(f"   - Símbolo enviado: {symbol}")
                print(f"   - Formato correcto: {symbol.lower()}")
                print(f"   - Par existe en LBank: ✅ (según TEST 1)")
                print(f"   - Fechas: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
                print(f"\n💡 Posibles causas:")
                print(f"   1. No tienes trades en este par")
                print(f"   2. El símbolo necesita formato especial")
                print(f"   3. Problema con fechas")
        else:
            trades = data.get("data", [])
            print(f"\n✅ SUCCESS! {len(trades)} trades obtenidos")
            if trades:
                print(f"\n📄 Primer trade:")
                print(json.dumps(trades[0], indent=2, ensure_ascii=False))
    
    except Exception as e:
        print(f"\n❌ Exception: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*70)
    
    # Preguntar si continuar
    if len(TEST_SYMBOLS) > 1:
        continuar = input(f"\n¿Probar siguiente símbolo? (s/n): ")
        if continuar.lower() != 's':
            break


print("\n" + "="*70)
print("RESUMEN")
print("="*70)
print("""
Si ves error 10008 en TODOS los símbolos:
→ Probablemente no tienes trades propios en ninguno

Si funciona en algunos pero no en otros:
→ Normal, solo tienes trades en algunos pares

Si el formato del símbolo se ve bien:
→ El problema es que no tienes trades en ese par/período
""")