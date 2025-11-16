#!/usr/bin/env python3
"""
🔍 Diagnóstico de Funding Fees de Aster

Este script te ayuda a identificar por qué no aparecen los funding fees.
"""

import os
import sys
from datetime import datetime, timezone, timedelta

# # Asegurar que puede importar desde adapters
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_aster_funding():
    print("="*80)
    print("🔍 DIAGNÓSTICO DE FUNDING FEES DE ASTER")
    print("="*80)
    
    # 1. Verificar credenciales
    print("\n1️⃣ VERIFICANDO CREDENCIALES...")
    api_key = "8491e7b39a2782f066fa8355a3afe1883345b5228007ab11609290fdde314853"
    api_secret = "67c1601e4bfb7ae48f1513e165a4caa711b2566aeebab98a8e0d22a47e6c4138"
    
    if not api_key or not api_secret:
        print("   ❌ Faltan credenciales ASTER_API_KEY / ASTER_API_SECRET")
        return
    print(f"   ✅ API Key: {api_key[:8]}...{api_key[-4:]}")
    print(f"   ✅ API Secret: {'*' * 20}")
    
    # 2. Importar funciones
    print("\n2️⃣ IMPORTANDO FUNCIONES...")
    try:
        from adapters.aster import (
            pull_funding_aster,
            fetch_funding_aster_windowed,
            aster_signed_request
        )
        print("   ✅ Funciones importadas correctamente")
    except Exception as e:
        print(f"   ❌ Error importando: {e}")
        return
    
    # 3. Test endpoint directo
    print("\n3️⃣ TESTEANDO ENDPOINT DIRECTO /fapi/v1/income...")
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    week_ago_ms = now_ms - (7 * 24 * 60 * 60 * 1000)
    
    try:
        params = {
            "incomeType": "FUNDING_FEE",
            "startTime": week_ago_ms,
            "endTime": now_ms,
            "limit": 100
        }
        print(f"   📅 Rango: {datetime.fromtimestamp(week_ago_ms/1000, tz=timezone.utc)}")
        print(f"            → {datetime.fromtimestamp(now_ms/1000, tz=timezone.utc)}")
        
        raw_data = aster_signed_request("/fapi/v1/income", params=params)
        print(f"   📦 Registros recibidos: {len(raw_data) if raw_data else 0}")
        
        if raw_data:
            print(f"\n   📋 Primeros 3 registros:")
            for i, item in enumerate(raw_data[:3]):
                sym = item.get('symbol', '?')
                inc = item.get('income', 0)
                ts = item.get('time', item.get('timestamp', 0))
                dt = datetime.fromtimestamp(int(ts)/1000, tz=timezone.utc)
                print(f"      {i+1}. {sym}: {inc} USDT @ {dt}")
        else:
            print("   ⚠️ No hay registros en este rango")
            
    except Exception as e:
        print(f"   ❌ Error en endpoint directo: {e}")
        import traceback
        traceback.print_exc()
    
    # 4. Test fetch_funding_aster_windowed
    print("\n4️⃣ TESTEANDO fetch_funding_aster_windowed...")
    try:
        # Test con 7 días
        result_7d = fetch_funding_aster_windowed(days=7, debug=True)
        print(f"   ✅ Registros (7 días): {len(result_7d)}")
        
        # Test con since_ms
        result_since = fetch_funding_aster_windowed(
            since_ms=week_ago_ms, 
            until_ms=now_ms, 
            debug=True
        )
        print(f"   ✅ Registros (since): {len(result_since)}")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    # 5. Test pull_funding_aster (la función que usa portfolio.py)
    print("\n5️⃣ TESTEANDO pull_funding_aster...")
    
    # Activar debugging
    os.environ["ASTER_DEBUG_FUNDING"] = "1"
    
    test_cases = [
        {"force_days": 7, "label": "Force 7 días"},
        {"since": week_ago_ms, "label": f"Since {week_ago_ms}"},
        {"since": now_ms - 3600*1000, "label": "Since hace 1 hora (debería ampliar)"},
        {}, # Default
    ]
    
    for tc in test_cases:
        print(f"\n   🧪 Test: {tc.get('label', 'Default')}")
        try:
            result = pull_funding_aster(**tc)
            print(f"      ✅ Resultado: {len(result)} registros")
        except Exception as e:
            print(f"      ❌ Error: {e}")
    
    # 6. Verificar normalización en portfolio.py
    print("\n6️⃣ VERIFICANDO NORMALIZACIÓN...")
    try:
        result = pull_funding_aster(force_days=7)
        if result:
            sample = result[0]
            print(f"   📋 Registro de ejemplo:")
            print(f"      exchange: {sample.get('exchange')}")
            print(f"      symbol: {sample.get('symbol')}")
            print(f"      income: {sample.get('income')}")
            print(f"      timestamp: {sample.get('timestamp')}")
            print(f"      type: {sample.get('type')}")
            print(f"      external_id: {sample.get('external_id')}")
            
            # Verificar que tenga los campos requeridos por _std_event
            required = ['exchange', 'symbol', 'income', 'timestamp', 'type']
            missing = [f for f in required if not sample.get(f)]
            if missing:
                print(f"   ⚠️ Faltan campos: {missing}")
            else:
                print(f"   ✅ Todos los campos requeridos presentes")
        else:
            print("   ⚠️ No hay registros para verificar")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    print("\n" + "="*80)
    print("✅ DIAGNÓSTICO COMPLETADO")
    print("="*80)

if __name__ == "__main__":
    test_aster_funding()
