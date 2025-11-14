#!/usr/bin/env python3
# ===================================================================
# 🧪 SCRIPT DE DIAGNÓSTICO PARA ASTER
# ===================================================================
# 
# Testea la obtención de funding fees sin necesidad de correr portfolio.py
# 
# USO:
#   python test_aster_funding.py
#
# ===================================================================

import os
import sys
import time
from datetime import datetime, timedelta

# # Asegurar que podemos importar el adapter
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aster import (
    fetch_aster_open_positions,
    aster_signed_request,
    _sum_income,
    _sum_fees_from_user_trades,
    ASTER_API_KEY,
    ASTER_API_SECRET
)


def test_credentials():
    """Test 1: Verificar credenciales"""
    print("\n" + "="*80)
    print("🔐 TEST 1: CREDENCIALES")
    print("="*80)
    
    if not ASTER_API_KEY:
        print("❌ ASTER_API_KEY no está configurada")
        return False
    if not ASTER_API_SECRET:
        print("❌ ASTER_API_SECRET no está configurada")
        return False
    
    print(f"✅ API Key: {ASTER_API_KEY[:8]}...{ASTER_API_KEY[-4:]}")
    print(f"✅ API Secret: {'*' * 32}")
    return True


def test_api_connection():
    """Test 2: Verificar conexión con API"""
    print("\n" + "="*80)
    print("🌐 TEST 2: CONEXIÓN API")
    print("="*80)
    
    try:
        # Test simple: obtener tiempo del servidor
        data = aster_signed_request("/fapi/v1/time")
        if data and "serverTime" in data:
            print(f"✅ Conexión OK - Server time: {data['serverTime']}")
            return True
        else:
            print(f"⚠️ Respuesta inesperada: {data}")
            return False
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return False


def test_position_risk():
    """Test 3: Verificar que se pueden obtener posiciones"""
    print("\n" + "="*80)
    print("📊 TEST 3: POSICIONES")
    print("="*80)
    
    try:
        data = aster_signed_request("/fapi/v2/positionRisk")
        if not data:
            print("⚠️ No se recibieron posiciones")
            return True  # No es un error si no hay posiciones
        
        active_positions = [p for p in data if float(p.get("positionAmt", 0)) != 0]
        
        print(f"📦 Total posiciones en respuesta: {len(data)}")
        print(f"✅ Posiciones activas: {len(active_positions)}")
        
        if active_positions:
            for p in active_positions[:3]:  # Mostrar primeras 3
                print(f"   - {p.get('symbol')}: {p.get('positionAmt')} @ {p.get('entryPrice')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error obteniendo posiciones: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_funding_calculation():
    """Test 4: Verificar cálculo de funding fees"""
    print("\n" + "="*80)
    print("💰 TEST 4: FUNDING FEES")
    print("="*80)
    
    try:
        # Obtener posiciones activas
        data = aster_signed_request("/fapi/v2/positionRisk")
        if not data:
            print("⚠️ No hay posiciones para testear funding")
            return True
        
        active_positions = [p for p in data if float(p.get("positionAmt", 0)) != 0]
        if not active_positions:
            print("⚠️ No hay posiciones activas para testear funding")
            return True
        
        # Testear el primer símbolo
        test_symbol = active_positions[0].get("symbol")
        print(f"🎯 Testeando símbolo: {test_symbol}")
        
        now_ms = int(time.time() * 1000)
        
        # Test 1: Funding últimas 24h
        print("\n📅 Funding últimas 24 horas:")
        start_24h = now_ms - 24 * 60 * 60 * 1000
        funding_24h = _sum_income(test_symbol, "FUNDING_FEE", start_24h, now_ms)
        print(f"   Total: {funding_24h:.8f} USDT")
        
        # Test 2: Funding últimos 7 días
        print("\n📅 Funding últimos 7 días:")
        start_7d = now_ms - 7 * 24 * 60 * 60 * 1000
        funding_7d = _sum_income(test_symbol, "FUNDING_FEE", start_7d, now_ms)
        print(f"   Total: {funding_7d:.8f} USDT")
        
        # Test 3: Fees últimos 7 días
        print("\n📅 Fees últimos 7 días:")
        fees_7d = _sum_fees_from_user_trades(test_symbol, start_7d, now_ms)
        print(f"   Total: {fees_7d:.8f} USDT")
        
        # Test 4: Realized PnL últimos 7 días
        print("\n📅 Realized PnL últimos 7 días:")
        realized_7d = _sum_income(test_symbol, "REALIZED_PNL", start_7d, now_ms)
        print(f"   Total: {realized_7d:.8f} USDT")
        
        if funding_24h == 0 and funding_7d == 0:
            print("\n⚠️ ADVERTENCIA: Todos los valores son 0")
            print("   Esto puede indicar:")
            print("   1. No hay eventos de funding en el período")
            print("   2. Los endpoints de /income no están respondiendo correctamente")
            print("   3. El símbolo no tiene actividad de funding")
            return False
        
        print("\n✅ Cálculos de funding completados correctamente")
        return True
        
    except Exception as e:
        print(f"❌ Error calculando funding: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_full_fetch():
    """Test 5: Ejecutar fetch completo"""
    print("\n" + "="*80)
    print("🚀 TEST 5: FETCH COMPLETO")
    print("="*80)
    
    # Activar debug
    os.environ["ASTER_DEBUG_OPEN_POS"] = "1"
    
    try:
        positions = fetch_aster_open_positions()
        
        print(f"\n📊 Resultado: {len(positions)} posiciones procesadas")
        
        if positions:
            print("\n✅ POSICIONES CON COSTOS:")
            for p in positions:
                print(f"\n   🎯 {p['symbol']} ({p['side']})")
                print(f"      Size: {p['size']:.4f}")
                print(f"      Funding 24h: {p.get('funding_24h', 0):.6f} USDT")
                print(f"      Funding total: {p.get('funding_fee', 0):.6f} USDT")
                print(f"      Fees: {p.get('fee', 0):.6f} USDT")
                print(f"      Realized PnL: {p.get('realized_pnl', 0):.6f} USDT")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en fetch completo: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Ejecutar todos los tests"""
    print("\n" + "🧪" + "="*78 + "🧪")
    print("   DIAGNÓSTICO COMPLETO DE ASTER FUNDING FEES")
    print("🧪" + "="*78 + "🧪\n")
    
    results = {}
    
    # Ejecutar tests
    results["credentials"] = test_credentials()
    
    if results["credentials"]:
        results["connection"] = test_api_connection()
    else:
        print("\n❌ No se pueden ejecutar más tests sin credenciales válidas")
        return
    
    if results["connection"]:
        results["positions"] = test_position_risk()
        results["funding"] = test_funding_calculation()
        results["full_fetch"] = test_full_fetch()
    
    # Resumen
    print("\n" + "="*80)
    print("📋 RESUMEN DE TESTS")
    print("="*80)
    
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {test_name.upper()}")
    
    total = len(results)
    passed = sum(1 for v in results.values() if v)
    
    print(f"\n🎯 Resultado: {passed}/{total} tests pasados")
    
    if passed == total:
        print("\n🎉 ¡TODOS LOS TESTS PASARON!")
        print("   El adapter de Aster está funcionando correctamente.")
    else:
        print("\n⚠️ HAY PROBLEMAS QUE RESOLVER")
        print("   Revisa los errores arriba para más detalles.")


if __name__ == "__main__":
    main()