# debug_save_bitget.py
import os
import sys
import re
sys.path.append(os.path.dirname(__file__))

from adapters.bitget import (
    save_bitget_closed_positions,
    debug_preview_bitget_closed,
    fetch_bitget_all_balances,
    fetch_bitget_open_positions,
    fetch_bitget_funding_fees
)
from db_manager import init_db
import sqlite3

def main():
    print("🐛 Bitget Debug Script")
    
    # 1. Inicializar DB
    init_db()
    
    # 2. Preview de lo que se guardaría
    print("\n1. 📊 PREVIEW de posiciones cerradas (últimos 3 días):")
    debug_preview_bitget_closed(days=3)
    
    # 3. Guardar realmente
    print("\n2. 💾 GUARDANDO posiciones cerradas...")
    save_bitget_closed_positions("portfolio.db", days=30, debug=True)
    
    # 4. Leer y mostrar lo guardado
    print("\n3. 📖 LEYENDO de la base de datos...")
    conn = sqlite3.connect("portfolio.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    cur.execute("""
        SELECT exchange, symbol, side, size, entry_price, close_price,
               realized_pnl, funding_total, fee_total, open_time, close_time
        FROM closed_positions 
        WHERE exchange = 'bitget'
        ORDER BY close_time DESC 
        LIMIT 5
    """)
    
    rows = cur.fetchall()
    print(f"📋 Últimas {len(rows)} posiciones Bitget en DB:")
    for r in rows:
        print(f"   {r['symbol']} {r['side']}: size={r['size']} "
              f"realized={r['realized_pnl']:.4f} funding={r['funding_total']:.4f} "
              f"fees={r['fee_total']:.4f}")
    
    conn.close()
    
    # 5. Simular JSON de /api/closed_positions
    print("\n4. 🌐 SIMULANDO /api/closed_positions response:")
    from collections import defaultdict
    import statistics
    from datetime import datetime
    
    conn = sqlite3.connect("portfolio.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    cur.execute("""
        SELECT exchange, symbol, side, size, entry_price, close_price,
               realized_pnl, funding_total AS funding_fee, fee_total AS fees,
               notional, open_time, close_time
        FROM closed_positions
        WHERE exchange = 'bitget'
        ORDER BY open_time ASC
    """)
    
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    
    # Agrupar por símbolo base (como hace la ruta real)
    def _base_symbol(sym: str) -> str:
        s = (sym or "").upper()
        s = re.sub(r'[-_/]?(USDT|USDC)$', '', s)
        s = re.sub(r'[-_/]?PERP$', '', s)
        return s
    
    by_base = {}
    for r in rows:
        base = _base_symbol(r["symbol"])
        r["_base"] = base
        by_base.setdefault(base, []).append(r)
    
    groups = []
    for base, items in by_base.items():
        size_total = sum(float(x["size"] or 0.0) for x in items)
        fees_total = sum(float(x["fees"] or 0.0) for x in items)
        funding_total = sum(float(x["funding_fee"] or 0.0) for x in items)
        realized_total = sum(float(x["realized_pnl"] or 0.0) for x in items)
        
        # Calcular PnL total por precio (como hace el front)
        pnl_total = 0
        for x in items:
            size_val = float(x["size"] or 0.0)
            entry_val = float(x["entry_price"] or 0.0)
            close_val = float(x["close_price"] or 0.0)
            side_val = (x.get("side") or "").lower()
            
            if side_val == "short":
                pnl_total += (entry_val - close_val) * size_val
            else:
                pnl_total += (close_val - entry_val) * size_val
        
        groups.append({
            "symbol": base,
            "positions": items[:2],  # solo mostrar 2 piernas como ejemplo
            "size_total": size_total,
            "notional_total": sum(float(x["notional"] or 0.0) for x in items),
            "pnl_total": pnl_total,
            "fees_total": fees_total,
            "funding_total": funding_total,
            "realized_total": realized_total,
            "entry_avg": statistics.mean([float(x["entry_price"] or 0.0) for x in items]) if items else 0,
            "close_avg": statistics.mean([float(x["close_price"] or 0.0) for x in items]) if items else 0,
            "open_date": "2024-01-01 00:00",  # simplificado
            "close_date": "2024-01-01 00:00"   # simplificado
        })
    
    print("📦 Response simulado (/api/closed_positions):")
    print(f"   Grupos: {len(groups)}")
    for g in groups[:2]:  # mostrar 2 grupos
        print(f"   └─ {g['symbol']}: size_total={g['size_total']:.4f} "
              f"pnl_price={g['pnl_total']:.4f} realized_net={g['realized_total']:.4f}")

if __name__ == "__main__":
    main()
