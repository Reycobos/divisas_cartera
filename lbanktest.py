"""
Test script para las nuevas funciones de LBank Adapter
=======================================================
Ejecutar en consola de Python para probar las funciones manuales
"""

# ============================================================================
# TEST 1: Agregar un par manualmente
# ============================================================================
print("=" * 60)
print("TEST 1: Agregar par manualmente")
print("=" * 60)

from adapters.lbank import add_manual_lbank_pair

# Agregar JELLYJELLY
result = add_manual_lbank_pair("JELLYJELLY")
print(f"Resultado: {result}")
print()

# Agregar OP
result = add_manual_lbank_pair("OP")
print(f"Resultado: {result}")
print()

# Agregar ARB con formato diferente
result = add_manual_lbank_pair("ARB/USDT")
print(f"Resultado: {result}")
print()


# ============================================================================
# TEST 2: Verificar que están en el cache
# ============================================================================
print("=" * 60)
print("TEST 2: Verificar cache")
print("=" * 60)

from adapters.lbank import _get_cached_trading_pairs

pairs = _get_cached_trading_pairs()
print(f"Pares en cache: {pairs}")
print()


# ============================================================================
# TEST 3: Buscar trades con símbolos específicos (DRY RUN)
# ============================================================================
print("=" * 60)
print("TEST 3: Buscar trades de símbolos específicos")
print("=" * 60)

from adapters.lbank import save_lbank_closed_positions

# Opción A: Pasando símbolos directamente
count = save_lbank_closed_positions(
    symbols=["JELLYJELLY", "OP"],  # Tus símbolos
    days=7,
    dry_run=True  # Solo mostrar, no guardar
)
print(f"\nPosiciones encontradas: {count}")
print()


# ============================================================================
# TEST 4: Buscar trades usando cache existente (DRY RUN)
# ============================================================================
print("=" * 60)
print("TEST 4: Buscar usando cache existente")
print("=" * 60)

# Opción B: Usar cache existente (sin pasar symbols)
count = save_lbank_closed_positions(
    days=7,
    dry_run=True
)
print(f"\nPosiciones encontradas: {count}")
print()


# ============================================================================
# NOTAS
# ============================================================================
print("=" * 60)
print("NOTAS IMPORTANTES")
print("=" * 60)
print("""
✅ Funciones nuevas:
   1. add_manual_lbank_pair(symbol) - Agrega un par al cache
   2. save_lbank_closed_positions(symbols=[...]) - Busca símbolos específicos

⚠️  Sobre el error 5008:
   - Significa que el trading pair NO existe en LBank
   - Verifica en https://www.lbank.com que el par existe
   - Formato correcto: "jellyjelly_usdt" (lowercase con underscore)
   
🔍 Para verificar si un par existe:
   1. Ve a la web de LBank
   2. Busca el token (ej: JELLYJELLY)
   3. Confirma que se tradea contra USDT
   
📌 Ejemplo de uso recomendado:
   # 1. Agregar pares manualmente
   add_manual_lbank_pair("BTC")
   add_manual_lbank_pair("ETH")
   
   # 2. Buscar trades
   save_lbank_closed_positions(symbols=["BTC", "ETH"], days=30, dry_run=True)
   
   # 3. Si funcionó, guardar en DB (quitar dry_run)
   save_lbank_closed_positions(symbols=["BTC", "ETH"], days=30)
""")