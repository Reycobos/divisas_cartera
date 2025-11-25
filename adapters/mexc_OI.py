
import json
from typing import Dict, Any, Optional

# 📦 Importamos solo lo necesario de mexc.py
# Asegúrate de que mexc.py esté en el mismo directorio o en PYTHONPATH
try:
    from mexc import _request, MEXC_BASE_URL
except ImportError:
    print("❌ ERROR: No se puede importar mexc.py")
    print("   Asegúrate de que mexc.py esté en el mismo directorio")
    exit(1)


def test_ticker(symbol: str) -> Optional[Dict[str, Any]]:
    """
    🔍 Prueba el endpoint de ticker para un símbolo
    
    Args:
        symbol: El símbolo a consultar (ej: PAYAI_USDT, BTC_USDT)
    
    Returns:
        Dict con la respuesta o None si hay error
    """
    print(f"\n{'='*60}")
    print(f"🔎 Probando símbolo: {symbol}")
    print(f"{'='*60}")
    
    try:
        # 🌐 Llamada al endpoint (público, sin firma)
        response = _request(
            method="GET",
            path="/api/v1/contract/ticker",
            params={"symbol": symbol},
            private=False,  # ⚠️ IMPORTANTE: público = sin autenticación
            timeout=10,
            max_retries=2
        )
        
        # ✅ Success
        if response.get("success"):
            data = response.get("data", {})
            print(f"✅ SUCCESS - {symbol}")
            print(f"\n📊 MAIN DATA:")
            print(f"   Last Price:    {data.get('lastPrice', 'N/A')}")
            print(f"   Fair Price:    {data.get('fairPrice', 'N/A')}")
            print(f"   Index Price:   {data.get('indexPrice', 'N/A')}")
            print(f"   24h Volume:    {data.get('volume24', 'N/A')}")
            print(f"   24h High:      {data.get('high24Price', 'N/A')}")
            print(f"   24h Low:       {data.get('lower24Price', 'N/A')}")
            print(f"   Change Rate:   {data.get('riseFallRate', 'N/A')}%")
            print(f"   Funding Rate:  {data.get('fundingRate', 'N/A')}")
            print(f"   Open Interest: {data.get('holdVol', 'N/A')}")
            
            return response
        else:
            # ❌ Error del exchange
            print(f"❌ FAIL - {symbol}")
            print(f"   Code: {response.get('code', 'N/A')}")
            print(f"   Message: {response.get('message', 'Sin mensaje')}")
            return None
            
    except Exception as e:
        # ❌ Error de conexión/timeout
        print(f"❌ ERROR - {symbol}")
        print(f"   {type(e).__name__}: {str(e)}")
        return None


def print_full_response(response: Optional[Dict[str, Any]]) -> None:
    """
    📄 Imprime la respuesta completa en formato JSON
    """
    if response:
        print(f"\n{'='*60}")
        print("📄 RESPUESTA COMPLETA (JSON):")
        print(f"{'='*60}")
        print(json.dumps(response, indent=2, ensure_ascii=False))
        
def test_contract_detail(symbol: str) -> Optional[Dict[str, Any]]:
    """
    🔍 Prueba el endpoint de contract detail para un símbolo
    
    Args:
        symbol: El símbolo a consultar (ej: PAYAI_USDT, BTC_USDT)
    
    Returns:
        Dict con la respuesta o None si hay error
    """
    print(f"\n{'='*60}")
    print(f"📜 Probando Contract Detail: {symbol}")
    print(f"{'='*60}")
    
    try:
        # 🌐 Llamada al endpoint (público, sin firma)
        response = _request(
            method="GET",
            path="/api/v1/contract/detail",
            params={"symbol": symbol},
            private=False,  # ⚠️ IMPORTANTE: público = sin autenticación
            timeout=10,
            max_retries=2
        )
        
        # ✅ Success
        if response.get("success"):
            data = response.get("data", {})
            print(f"✅ SUCCESS - {symbol}")
            print(f"\n📊 INFORMACIÓN DEL CONTRATO:")
            print(f"   Display Name:       {data.get('displayNameEn', 'N/A')}")
            print(f"   Base Coin:          {data.get('baseCoin', 'N/A')}")
            print(f"   Quote Coin:         {data.get('quoteCoin', 'N/A')}")
            print(f"   Settle Coin:        {data.get('settleCoin', 'N/A')}")
            print(f"   Contract Size:      {data.get('contractSize', 'N/A')}")
            print(f"   Min Leverage:       {data.get('minLeverage', 'N/A')}x")
            print(f"   Max Leverage:       {data.get('maxLeverage', 'N/A')}x")
            print(f"\n💰 FEES & MARGINS:")
            print(f"   Taker Fee:          {data.get('takerFeeRate', 'N/A')}")
            print(f"   Maker Fee:          {data.get('makerFeeRate', 'N/A')}")
            print(f"   Maintenance Margin: {data.get('maintenanceMarginRate', 'N/A')}")
            print(f"   Initial Margin:     {data.get('initialMarginRate', 'N/A')}")
            print(f"\n📏 TRADING LIMITS:")
            print(f"   Min Volume:         {data.get('minVol', 'N/A')} contracts")
            print(f"   Max Volume:         {data.get('maxVol', 'N/A')} contracts")
            print(f"   Price Scale:        {data.get('priceScale', 'N/A')}")
            print(f"   Vol Scale:          {data.get('volScale', 'N/A')}")
            print(f"\n🔧 STATUS:")
            state_map = {0: "Enabled", 1: "Delivery", 2: "Delivered", 3: "Offline", 4: "Paused"}
            state = data.get('state', -1)
            print(f"   State:              {state_map.get(state, 'Unknown')}")
            print(f"   API Allowed:        {data.get('apiAllowed', 'N/A')}")
            print(f"   Is New:             {data.get('isNew', 'N/A')}")
            print(f"   Is Hot:             {data.get('isHot', 'N/A')}")
            
            return response
        else:
            # ❌ Error del exchange
            print(f"❌ FAIL - {symbol}")
            print(f"   Code: {response.get('code', 'N/A')}")
            print(f"   Message: {response.get('message', 'Sin mensaje')}")
            return None
            
    except Exception as e:
        # ❌ Error de conexión/timeout
        print(f"❌ ERROR - {symbol}")
        print(f"   {type(e).__name__}: {str(e)}")
        return None

# ============================================================================
# 🚀 MAIN - Pruebas con diferentes variaciones del símbolo
# ============================================================================
if __name__ == "__main__":
    
    print("\n" + "="*60)
    print("🎯 TEST MEXC ENDPOINTS")
    print(f"Base URL: {MEXC_BASE_URL}")
    print("="*60)
    
    # 🔤 Símbolo a probar
    SYMBOL_TO_TEST = "PAYAI_USDT"  # 👈 Cambia aquí el símbolo que quieras probar
    
    print(f"\n🎯 Símbolo seleccionado: {SYMBOL_TO_TEST}")
    
    # ═══════════════════════════════════════════════════════════
    # 1️⃣ TEST TICKER (precios en tiempo real)
    # ═══════════════════════════════════════════════════════════
    print("\n" + "🔥"*30)
    print("1️⃣  ENDPOINT: /api/v1/contract/ticker")
    print("🔥"*30)
    
    ticker_response = test_ticker(SYMBOL_TO_TEST)
    
    # ═══════════════════════════════════════════════════════════
    # 2️⃣ TEST CONTRACT DETAIL (info del contrato)
    # ═══════════════════════════════════════════════════════════
    print("\n" + "🔥"*30)
    print("2️⃣  ENDPOINT: /api/v1/contract/detail")
    print("🔥"*30)
    
    detail_response = test_contract_detail(SYMBOL_TO_TEST)
    
    # ═══════════════════════════════════════════════════════════
    # 📊 RESUMEN FINAL
    # ═══════════════════════════════════════════════════════════
    print("\n" + "="*60)
    print("📊 RESUMEN DE PRUEBAS")
    print("="*60)
    print(f"Símbolo: {SYMBOL_TO_TEST}")
    print(f"  • Ticker:          {'✅ OK' if ticker_response else '❌ FAIL'}")
    print(f"  • Contract Detail: {'✅ OK' if detail_response else '❌ FAIL'}")
    
    # ═══════════════════════════════════════════════════════════
    # 🧪 PRUEBA CON MÚLTIPLES SÍMBOLOS (opcional)
    # ═══════════════════════════════════════════════════════════
    print("\n" + "="*60)
    print("🧪 ¿PROBAR MÚLTIPLES SÍMBOLOS? (descomenta abajo)")
    print("="*60)
    
    # Descomenta estas líneas para probar varios símbolos:
    """
    test_symbols = ["BTC_USDT", "ETH_USDT", "PAYAI_USDT"]
    
    for sym in test_symbols:
        print(f"\n{'─'*60}")
        test_ticker(sym)
        test_contract_detail(sym)
        import time
        time.sleep(1)  # Espera 1 seg entre símbolos (rate limit)
    """
    
    # ═══════════════════════════════════════════════════════════
    # 💡 TIPS ÚTILES
    # ═══════════════════════════════════════════════════════════
    print("\n" + "="*60)
    print("💡 CÓDIGO DE EJEMPLO PARA USAR EN TU APP:")
    print("="*60)
    print("""
# ─────────────────────────────────────────────────────────
# Obtener precio actual
# ─────────────────────────────────────────────────────────
from test_ticker_mexc import test_ticker

response = test_ticker("PAYAI_USDT")
if response and response.get("success"):
    price = response["data"].get("lastPrice")
    print(f"Precio: {price}")

# ─────────────────────────────────────────────────────────
# Obtener info del contrato
# ─────────────────────────────────────────────────────────
from test_ticker_mexc import test_contract_detail

response = test_contract_detail("PAYAI_USDT")
if response and response.get("success"):
    data = response["data"]
    max_lev = data.get("maxLeverage")
    contract_size = data.get("contractSize")
    print(f"Max Leverage: {max_lev}x")
    print(f"Contract Size: {contract_size}")
    """)


