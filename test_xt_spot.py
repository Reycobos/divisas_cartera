# test_xt_spot.py
"""
Script de prueba para XT Spot Trades FIFO

Uso en consola de Spyder:
-------------------------
# 1. Probar solo descarga de trades (sin guardar)
from adapters.xt_spot_trades import fetch_xt_spot_trades, get_existing_trade_hashes
existing = get_existing_trade_hashes("portfolio.db")
trades = fetch_xt_spot_trades(days_back=7, debug=True, existing_hashes=existing)
print(f"Trades descargados: {len(trades)}")
for t in trades[:5]:  # primeros 5
    print(f"{t.symbol} {t.side} {t.amount} @ {t.price}")

# 2. Probar guardado completo
from adapters.xt_spot_trades import save_xt_spot_positions
saved, ignored = save_xt_spot_positions(db_path="portfolio.db", days_back=30, debug=True)
print(f"✅ Guardadas: {saved}, Ignoradas: {ignored}")

# 3. Ver resultados en DB
import sqlite3
conn = sqlite3.connect("portfolio.db")
cursor = conn.cursor()
cursor.execute("""
    SELECT symbol, side, size, entry_price, close_price, realized_pnl, 
           datetime(close_time, 'unixepoch') as close_date
    FROM closed_positions 
    WHERE exchange = 'xt'
    ORDER BY close_time DESC 
    LIMIT 10
""")
for row in cursor.fetchall():
    print(row)
conn.close()

# 4. Verificar swaps de stablecoins
cursor = conn.cursor()
cursor.execute("""
    SELECT symbol, side, size, realized_pnl, fee_total,
           datetime(close_time, 'unixepoch') as date
    FROM closed_positions 
    WHERE exchange = 'xt' AND side = 'swapstable'
    ORDER BY close_time DESC
""")
print("\\n=== SWAPS STABLECOINS ===")
for row in cursor.fetchall():
    print(row)

# 5. Ver resumen por símbolo
cursor.execute("""
    SELECT symbol, 
           COUNT(*) as num_trades,
           SUM(realized_pnl) as total_pnl,
           SUM(fee_total) as total_fees
    FROM closed_positions 
    WHERE exchange = 'xt' AND ignore_trade = 0
    GROUP BY symbol
    ORDER BY total_pnl DESC
""")
print("\\n=== RESUMEN POR SÍMBOLO ===")
for row in cursor.fetchall():
    symbol, num, pnl, fees = row
    print(f"{symbol}: {num} trades, PnL: ${pnl:.2f}, Fees: ${fees:.2f}")
"""

# === FUNCIÓN COMPLETA DE TEST ===
def test_xt_spot_complete():
    """Test completo del adapter de XT spot"""
    import sqlite3
    from adapters.xt_spot_trades import (
        save_xt_spot_positions, 
        fetch_xt_spot_trades,
        get_existing_trade_hashes
    )
    
    print("="*60)
    print("🧪 TEST XT SPOT TRADES FIFO")
    print("="*60)
    
    DB = "portfolio.db"
    DAYS = 30
    
    # 1️⃣ Verificar trades existentes
    print("\n1️⃣ Verificando trades existentes...")
    existing = get_existing_trade_hashes(DB)
    print(f"   📊 Trades ya en DB: {len(existing)}")
    
    # 2️⃣ Descargar nuevos trades
    print(f"\n2️⃣ Descargando trades de últimos {DAYS} días...")
    trades = fetch_xt_spot_trades(
        days_back=DAYS, 
        debug=True, 
        existing_hashes=existing
    )
    print(f"   ✅ Trades nuevos: {len(trades)}")
    
    if trades:
        print("\n   📋 Muestra de trades:")
        for i, t in enumerate(trades[:3], 1):
            from datetime import datetime
            dt = datetime.fromtimestamp(t.ts)
            print(f"   {i}. {t.symbol} | {t.side.upper()} | "
                  f"{t.amount:.4f} @ ${t.price:.4f} | "
                  f"Fee: {t.fee:.6f} {t.fee_ccy} | {dt}")
    
    # 3️⃣ Guardar con FIFO
    print(f"\n3️⃣ Procesando con FIFO y guardando en DB...")
    saved, ignored = save_xt_spot_positions(
        db_path=DB, 
        days_back=DAYS, 
        debug=True
    )
    print(f"   ✅ Posiciones guardadas: {saved}")
    print(f"   ⚠️ Posiciones ignoradas: {ignored}")
    
    # 4️⃣ Verificar resultados
    print("\n4️⃣ Verificando resultados en DB...")
    conn = sqlite3.connect(DB)
    cursor = conn.cursor()
    
    # Últimas 5 posiciones
    cursor.execute("""
        SELECT symbol, side, size, entry_price, close_price, 
               realized_pnl, fee_total,
               datetime(close_time, 'unixepoch') as close_date,
               ignore_trade
        FROM closed_positions 
        WHERE exchange = 'xt'
        ORDER BY close_time DESC 
        LIMIT 5
    """)
    
    print("\n   📊 Últimas 5 posiciones cerradas:")
    print("   " + "-"*80)
    for row in cursor.fetchall():
        symbol, side, size, entry, close, pnl, fee, date, ignored = row
        ignore_flag = "🚫" if ignored else "✅"
        print(f"   {ignore_flag} {symbol:10} | {side:10} | "
              f"Size: {size:10.4f} | ${entry:.4f} → ${close:.4f} | "
              f"PnL: ${pnl:8.2f} | Fee: ${fee:6.2f} | {date}")
    
    # Resumen total
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN ignore_trade = 0 THEN 1 ELSE 0 END) as counted,
            SUM(CASE WHEN ignore_trade = 1 THEN 1 ELSE 0 END) as ignored,
            SUM(CASE WHEN ignore_trade = 0 THEN realized_pnl ELSE 0 END) as total_pnl,
            SUM(CASE WHEN ignore_trade = 0 THEN fee_total ELSE 0 END) as total_fees
        FROM closed_positions 
        WHERE exchange = 'xt'
    """)
    
    total, counted, ignored_db, pnl, fees = cursor.fetchone()
    
    print("\n   📈 RESUMEN TOTAL:")
    print(f"   • Total posiciones: {total}")
    print(f"   • Contadas (ignore=0): {counted}")
    print(f"   • Ignoradas (ignore=1): {ignored_db}")
    print(f"   • PnL Total: ${pnl:.2f}")
    print(f"   • Fees Totales: ${fees:.2f}")
    print(f"   • PnL Neto: ${pnl + fees:.2f}")  # fees son negativos
    
    # Swaps stablecoins
    cursor.execute("""
        SELECT COUNT(*), SUM(realized_pnl)
        FROM closed_positions 
        WHERE exchange = 'xt' AND side = 'swapstable'
    """)
    swap_count, swap_pnl = cursor.fetchone()
    if swap_count and swap_count > 0:
        print(f"\n   💱 Swaps Stablecoins: {swap_count} | PnL: ${swap_pnl:.2f}")
    
    conn.close()
    
    print("\n" + "="*60)
    print("✅ TEST COMPLETADO")
    print("="*60)


# === FUNCIÓN RÁPIDA DE VERIFICACIÓN ===
def quick_check_xt_spot():
    """Verificación rápida de posiciones XT en DB"""
    import sqlite3
    
    conn = sqlite3.connect("portfolio.db")
    cursor = conn.cursor()
    
    # Count total
    cursor.execute("""
        SELECT COUNT(*), 
               SUM(realized_pnl),
               SUM(fee_total)
        FROM closed_positions 
        WHERE exchange = 'xt'
    """)
    count, pnl, fees = cursor.fetchone()
    
    print(f"📊 XT Spot Positions: {count}")
    print(f"💰 PnL Total: ${pnl:.2f}")
    print(f"💸 Fees Total: ${fees:.2f}")
    print(f"📈 Neto: ${(pnl + fees):.2f}")
    
    # Por símbolo
    cursor.execute("""
        SELECT symbol, COUNT(*), SUM(realized_pnl)
        FROM closed_positions 
        WHERE exchange = 'xt' AND ignore_trade = 0
        GROUP BY symbol
        ORDER BY SUM(realized_pnl) DESC
        LIMIT 5
    """)
    
    print("\n🏆 Top 5 símbolos por PnL:")
    for symbol, cnt, pnl in cursor.fetchall():
        print(f"  {symbol:10} : {cnt:3} trades → ${pnl:8.2f}")
    
    conn.close()


if __name__ == '__main__':
    # Ejecutar test completo
    test_xt_spot_complete()
    
    print("\n" + "="*60)
    print("💡 Para usar en consola:")
    print("="*60)
    print("from test_xt_spot import test_xt_spot_complete, quick_check_xt_spot")
    print("test_xt_spot_complete()  # Test completo")
    print("quick_check_xt_spot()     # Verificación rápida")