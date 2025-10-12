
# debug_save_mexc.py
import json
import sqlite3
from adapters.mexc import debug_preview_mexc_closed, save_mexc_closed_positions

DB_PATH = "portfolio.db"

def _alias_for_api(row: dict) -> dict:
    """
    Simula shape de /api/closed_positions:
      SELECT ..., realized_pnl, funding_total AS funding_fee, fee_total AS fees, ...
    """
    out = dict(row)
    out["funding_fee"] = out.get("funding_total", 0.0)
    out["fees"] = out.get("fee_total", 0.0)
    return out

if __name__ == "__main__":
    print("=== PREVIEW (3 días) ===")
    preview = debug_preview_mexc_closed(days=3)
    print(f"preview_count={len(preview)}")

    print("\n=== SAVING to portfolio.db ===")
    n = save_mexc_closed_positions(db_path=DB_PATH, days=3, debug=True)
    print(f"saved={n}")

    print("\n=== READ BACK (últimas 10 de MEXC) ===")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
      SELECT exchange, symbol, side, size, entry_price, close_price, open_time, close_time,
             realized_pnl, funding_total, fee_total, notional, leverage, liquidation_price
      FROM closed_positions
      WHERE exchange='mexc'
      ORDER BY id DESC
      LIMIT 10
    """)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()

    print(json.dumps(rows, indent=2))
    print("\n=== JSON simulado /api/closed_positions ===")
    aliased = [_alias_for_api(r) for r in rows]
    print(json.dumps(aliased, indent=2))
