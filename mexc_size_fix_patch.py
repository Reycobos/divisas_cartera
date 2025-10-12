# debug_mexc_closed.py
# Runner independiente para inspeccionar tamaños de posiciones cerradas de MEXC.
# Muestra por cada trade: symbol, side, size_after_scale, closeVol, holdVol, size usado y SRC.
# Úsalo en Spyder o ejecutando: python debug_mexc_closed.py

import pprint
import math

try:
    import mexc  # Debe estar en el mismo directorio o en el PYTHONPATH
except Exception as e:
    raise SystemExit(f"No pude importar mexc.py: {e}")

DAYS = 60         # ajusta si quieres
SYMBOL = None     # o por ejemplo "BTC_USDT"
MAX_ROWS = 200    # límite de filas a inspeccionar para no llenar la consola

def _f(x, d=0.0):
    try:
        if x is None or x == "":
            return d
        return float(x)
    except Exception:
        return d

def _try_iter_history(days=DAYS, symbol=SYMBOL):
    # La función interna suele llamarse _iter_history_positions
    # Intentamos con y sin parámetro symbol porque algunos tienen signatura distinta.
    try:
        return mexc._iter_history_positions(days=days, symbol=symbol)
    except TypeError:
        return mexc._iter_history_positions(days=days)

def _pick_size_and_src(r):
    """
    Emula la prioridad de tamaños:
      1) size_after_scale (varios alias)
      2) closeVol
      3) holdVol
    Si todo falla, intenta reconstruir por PnL de precio si hay datos suficientes.
    Devuelve (size, src)
    """
    # Candidatos de size_after_scale con variantes de nombre
    sas_candidates = (
        r.get("size_after_scale"),
        r.get("sizeAfterScale"),
        r.get("size_scaled"),
        r.get("sizeScaled"),
    )
    for cand in sas_candidates:
        if cand not in (None, "", 0, "0"):
            val = _f(cand, 0.0)
            if val > 0:
                return (val, "size_after_scale")

    # Fallbacks clásicos de MEXC
    cv = _f(r.get("closeVol"), 0.0)
    if cv > 0:
        return (cv, "closeVol")

    hv = _f(r.get("holdVol"), 0.0)
    if hv > 0:
        return (hv, "holdVol")

    # Reconstrucción por PnL de precio, si es posible
    side = (r.get("side") or r.get("positionSide") or "").lower()
    entry = _f(r.get("openAvgPrice") or r.get("avgEntryPrice") or r.get("openPrice"), 0.0)
    close = _f(r.get("closeAvgPrice") or r.get("avgClosePrice") or r.get("closePrice"), 0.0)

    # PnL “de precio” explícito si viene
    pnl_price = None
    if r.get("pnl") is not None:
        pnl_price = _f(r.get("pnl"), None)
    # Sino, intentalo con "realised" o "realized" menos fees/funding, si quieres
    if pnl_price is None:
        pnl_price = _f(r.get("realised"), 0.0)

    diff = abs(close - entry)
    if diff > 0 and pnl_price not in (None, 0.0):
        # Si es short, el PnL de precio cambia de signo respecto a long
        # pero para el tamaño nos vale el valor absoluto / diff
        size = abs(pnl_price) / diff
        if size > 0:
            return (size, "reconstructed_from_pnl")

    return (0.0, "unknown")

def _row_to_symbol(r):
    # intenta varias claves comunes
    return (r.get("symbol")
            or r.get("currency")
            or r.get("instId")
            or r.get("contract")
            or "<?>")

def _row_to_side(r):
    return (r.get("side") or r.get("positionSide") or "").lower() or "<?>"

def main():
    rows = _try_iter_history(DAYS, SYMBOL)
    if not rows:
        print("⚠️ No hay filas de posiciones cerradas para inspeccionar. Revisa credenciales/fechas.")
        return

    print(f"Encontradas {len(rows)} filas. Mostrando hasta {MAX_ROWS}.")
    print("-" * 100)
    header = f"{'#':>3} | {'symbol':<15} | {'side':<5} | {'size_after_scale':>16} | {'closeVol':>10} | {'holdVol':>10} | {'size_usado':>12} | SRC"
    print(header)
    print("-" * 100)

    for i, r in enumerate(rows[:MAX_ROWS], 1):
        sym = _row_to_symbol(r)
        side = _row_to_side(r)
        sas = r.get("size_after_scale") or r.get("sizeAfterScale") or r.get("size_scaled") or r.get("sizeScaled")
        cv  = r.get("closeVol")
        hv  = r.get("holdVol")

        size_used, src = _pick_size_and_src(r)

        print(f"{i:>3} | {sym:<15} | {side:<5} | {str(sas):>16} | {str(cv):>10} | {str(hv):>10} | {size_used:>12.6f} | {src}")

    print("-" * 100)
    print("Leyenda: si SRC = size_after_scale, perfecto. Si es closeVol/holdVol, estás en fallback. Si es reconstructed_from_pnl, no llegó size ni volúmenes.")
    print("Si quieres ver el payload final que estaría guardando tu adapter, descomenta la sección de abajo.")

    # ====== OPCIONAL: ver payload que arma tu adapter (si existe _row_to_closed_payload) ======
    # try:
    #     have = hasattr(mexc, "_row_to_closed_payload")
    #     print(f"\n¿Existe mexc._row_to_closed_payload? {have}")
    #     if have:
    #         print("\nEjemplo de 3 payloads:")
    #         for r in rows[:3]:
    #             payload = mexc._row_to_closed_payload(r)
    #             pprint.pprint(payload)
    # except Exception as e:
    #     print(f"Error mostrando payloads: {e}")

if __name__ == "__main__":
    main()
