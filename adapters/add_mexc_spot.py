# test_mexc_spot.py
# -*- coding: utf-8 -*-
"""
Script SÚPER SIMPLE para probar MEXC spot trades
Solo ejecutar: runfile('test_mexc_spot.py')
"""

print("\n" + "="*60)
print("🧪 TEST MEXC SPOT TRADES FIFO")
print("="*60 + "\n")

# Símbolos a procesar (hardcodeados)
SYMBOLS = ['CUDISUSDT', 'USDCUSDT']
DAYS_BACK = 40

print(f"📊 Símbolos: {', '.join(SYMBOLS)}")
print(f"📅 Días: {DAYS_BACK}")
print(f"💾 DB: portfolio.db\n")

# Verificar credenciales
print("1️⃣ Verificando credenciales...")
from adapters.mexc import _has_creds

if not _has_creds():
    print("❌ No hay credenciales MEXC")
    print("   Configura MEXC_API_KEY y MEXC_API_SECRET en .env\n")
    import sys
    sys.exit(1)

print("   ✅ Credenciales OK\n")

# Migrar DB (por si acaso)
print("2️⃣ Migrando base de datos...")
try:
    from db_manager import migrate_spot_support
    migrate_spot_support()
    print("   ✅ Migración OK\n")
except Exception as e:
    print(f"   ⚠️  {e}\n")

# Ejecutar el adapter
print("3️⃣ Procesando trades con FIFO...\n")
print("="*60 + "\n")

from adapters.mexc_spot_trades import save_mexc_spot_positions

try:
    saved, ignored = save_mexc_spot_positions(
        symbols=SYMBOLS,
        db_path='portfolio.db',
        days_back=DAYS_BACK,
        debug=True  # 👈 Ver todos los logs
    )
    
    print("\n" + "="*60)
    print("✅ PROCESO COMPLETADO")
    print("="*60)
    print(f"\n📈 Posiciones guardadas: {saved}")
    print(f"⏭️  Posiciones ignoradas: {ignored}\n")
    
    # Mostrar últimas posiciones
    print("="*60)
    print("📋 ÚLTIMAS POSICIONES EN DB")
    print("="*60 + "\n")
    
    import sqlite3
    conn = sqlite3.connect('portfolio.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT symbol, side, size, entry_price, close_price, realized_pnl, 
               datetime(close_time, 'unixepoch') as close_dt, ignore_trade
        FROM closed_positions
        WHERE exchange = 'mexc'
        ORDER BY close_time DESC
        LIMIT 10
    """)
    
    results = cursor.fetchall()
    
    if results:
        print(f"{'Symbol':<10} {'Side':<12} {'Size':<12} {'Entry':<10} {'Close':<10} {'PnL':<12} {'Fecha':<20} {'Ignore'}")
        print("-" * 100)
        
        for row in results:
            symbol, side, size, entry, close, pnl, close_dt, ignore = row
            ignore_flag = "🔸" if ignore else "✅"
            print(f"{symbol:<10} {side:<12} {size:<12.4f} {entry:<10.4f} {close:<10.4f} {pnl:<12.2f} {close_dt:<20} {ignore_flag}")
    else:
        print("ℹ️  No hay posiciones de MEXC en la DB")
    
    conn.close()
    
    print("\n" + "="*60)
    print("🎉 TEST COMPLETADO")
    print("="*60 + "\n")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}\n")
    import traceback
    traceback.print_exc()