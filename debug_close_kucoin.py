# debug_closed_kucoin.py
import os, sqlite3, datetime, json
from adapters.kucoin import fetch_closed_positions_kucoin, save_kucoin_closed_positions

DB_PATH = os.getenv("DB_PATH", "portfolio.db")
DO_SAVE = False   # pon True para guardar en la DB

def clean_base_symbol(sym: str) -> str:
    import re
    s = (sym or "").upper()
    s = re.sub(r'^PERP_', '', s)
    s = re.sub(r'(USDT|USDC|PERP)$', '', s)
    s = re.sub(r'(_|-)(USDT|USDC|PERP)$', '', s)
    s = re.sub(r'[_-]+$', '', s)
    parts = re.split(r'[_-]', s)
    return parts[0] if parts else s

def print_header(msg: str):
    print("\n" + "="*10 + f" {msg} " + "="*10 + "\n")

def main():
    print_header("FETCH (DRY-RUN) — lo que se pasaría a save_closed_position")
    items = fetch_closed_positions_kucoin(limit=20, debug=False)
    for i, it in enumerate(items, 1):
        print(f"[{i}] {it['exchange']} {it['symbol']} {it['side']} "
              f"size={it['size']:.4f} entry={it['entry_price']:.6f} close={it['close_price']:.6f}")
        print(f"    pricePNL(only)={it.get('pnl_price'):.6f}  "
              f"fees={it.get('fee_total'):.6f}  funding={it.get('funding_total'):.6f}  "
              f"realized(net)={it.get('realized_pnl'):.6f}")

    if DO_SAVE:
        print_header("SAVE — guardando en DB")
        save_kucoin_closed_positions(db_path=DB_PATH, days=7, debug=True)

    print_header("READ DB — últimas 10 cerradas KuCoin")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT exchange, symbol, side, size, entry_price, close_price,
               realized_pnl, fee_total, funding_total, open_time, close_time
        FROM closed_positions
        WHERE exchange='kucoin'
        ORDER BY close_time DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    for r in rows:
        ot = datetime.datetime.utcfromtimestamp(int(r["open_time"])).strftime("%Y-%m-%d %H:%M:%S")
        ct = datetime.datetime.utcfromtimestamp(int(r["close_time"])).strftime("%Y-%m-%d %H:%M:%S")
        print(f"- {r['symbol']} {r['side']} | pricePNL={float(r['realized_pnl']):.6f} "
              f"fees={float(r['fee_total']):.6f} funding={float(r['funding_total']):.6f} "
              f"| entry={float(r['entry_price']):.6f} close={float(r['close_price']):.6f} "
              f"| open={ot} close={ct}")
    conn.close()

    print_header("SIMULATE /api/closed_positions — payload que vería el HTML")
    # Nota: esto replica tu ruta (clave: fees/funding alias)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT exchange, symbol, side, size, entry_price, close_price,
               realized_pnl, funding_total AS funding_fee, fee_total AS fees,
               notional, open_time, close_time
        FROM closed_positions
        WHERE exchange='kucoin'
        ORDER BY open_time ASC
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    # Agrupar como hace tu ruta (simplificado, sin clustering por ventana/size para el ejemplo)
    from collections import defaultdict
    groups = defaultdict(lambda: {
        "symbol": "", "positions": [], "size_total":0, "notional_total":0,
        "pnl_total":0, "fees_total":0, "funding_total":0, "realized_total":0,
        "entry_avg":0, "close_avg":0, "open_date":"-", "close_date":"-"
    })
    for r in rows:
        base = clean_base_symbol(r["symbol"])
        groups[base]["symbol"] = base
        groups[base]["positions"].append(r)
        groups[base]["fees_total"]    += float(r.get("fees") or 0)
        groups[base]["funding_total"] += float(r.get("funding_fee") or 0)
        groups[base]["realized_total"]+= float(r.get("realized_pnl") or 0)
        # Para el ejemplo, 'pnl_total' lo dejamos a 0 porque tu HTML recalcula por fila.

    payload = {"closed_positions": list(groups.values())}
    print(json.dumps(payload, indent=2))

if __name__ == "__main__":
    main()


