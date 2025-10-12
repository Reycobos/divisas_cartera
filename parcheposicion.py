# intrucciones para usarlo
# meter en la consola esta linea
#python -c "from db_manager import patch_closed_position; patch_closed_position(position_id=123, changes={'size': 64314}, recompute=True)"



# =========================
# PATCHER DE POSICIONES    |
# =========================
def patch_closed_position(
    position_id: int | None = None,
    where: tuple | None = None,
    changes: dict | None = None,
    *,
    recompute: bool = True,
    db_path: str | None = None
) -> int:
    """
    Parchea una fila de closed_positions.
    - Identifica la fila por:
        A) position_id (columna id), o
        B) where=(exchange, symbol, close_time)  ó (exchange, symbol, side, close_time)
    - `changes` es un dict con los campos a actualizar.
    - Si `recompute=True`, recalcula pnl (precio), initial_margin, pnl_percent, apr y notional
      a partir de los campos actuales + los cambios aplicados, con reglas uniformes:
        * pnl_percent y apr SIEMPRE desde realized_pnl
        * notional = size * entry_price
        * initial_margin = notional / leverage si leverage>0; si no, defaults por exchange; si no, notional
    Devuelve el número de filas actualizadas (0 o 1).
    """
    import sqlite3
    import time

    def _f(x, d=0.0):
        try: return float(x)
        except: return d

    def _positive(x):
        return (x is not None) and (x > 0)

    def _price_pnl(side, entry, close, size):
        s = (side or "").lower()
        return (entry - close) * size if s == "short" else (close - entry) * size

    # Defaults y DB
    DB_PATH_LOCAL = db_path or globals().get("DB_PATH", "portfolio.db")
    DEFAULT_LEVERAGE = {"gate": 5}  # puedes ampliar: {"gate":5,"bingx":20,...}

    if not changes:
        print("⚠️ patch_closed_position: 'changes' vacío. No hago nada.")
        return 0

    # Whitelist de columnas actualizables
    allowed = {
        "exchange","symbol","side",
        "size","entry_price","close_price",
        "open_time","close_time",
        "pnl","realized_pnl","funding_total","fee_total",
        "pnl_percent","apr","initial_margin","notional","leverage","liquidation_price"
    }
    updates = {k: v for k, v in changes.items() if k in allowed}
    if not updates:
        print("⚠️ patch_closed_position: ninguna key de 'changes' es válida para actualizar.")
        return 0

    # WHERE
    where_sql = None
    where_params = []
    if position_id is not None:
        where_sql = "id = ?"
        where_params = [int(position_id)]
    elif where is not None:
        if len(where) == 3:
            where_sql = "exchange = ? AND symbol = ? AND close_time = ?"
        elif len(where) == 4:
            where_sql = "exchange = ? AND symbol = ? AND side = ? AND close_time = ?"
        else:
            raise ValueError("where debe ser (exchange, symbol, close_time) o (exchange, symbol, side, close_time)")
        where_params = list(where)
    else:
        raise ValueError("Debes pasar position_id o where=()")

    # Conexión
    conn = sqlite3.connect(DB_PATH_LOCAL)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1) Cargar fila actual
    cur.execute(f"SELECT * FROM closed_positions WHERE {where_sql} LIMIT 1", where_params)
    row = cur.fetchone()
    if not row:
        print("⚠️ patch_closed_position: no se encontró la fila objetivo.")
        conn.close()
        return 0

    rowd = dict(row)

    # 2) Aplicar cambios explícitos
    for k, v in updates.items():
        rowd[k] = v

    # 3) Recalcular métricas si procede
    if recompute:
        # Normalización de tipos base
        exchange = (rowd.get("exchange") or "").lower()
        side     = (rowd.get("side") or "").lower()
        size     = _f(rowd.get("size"))
        entry    = _f(rowd.get("entry_price"))
        close    = _f(rowd.get("close_price"))
        open_s   = int(rowd.get("open_time") or 0)
        close_s  = int(rowd.get("close_time") or 0)

        fee_total     = -abs(_f(rowd.get("fee_total", 0.0)))   # fees SIEMPRE negativas
        funding_total = _f(rowd.get("funding_total", 0.0))

        # pnl (precio)
        pnl_price = rowd.get("pnl")
        if pnl_price is None:
            pnl_price = _price_pnl(side, entry, close, size)
        else:
            pnl_price = _f(pnl_price)

        # Si size no cuadra con pnl precio, reconstituir
        diff = abs(close - entry)
        if diff > 0 and abs(pnl_price) > 0:
            size_from_pnl = abs(pnl_price) / diff
            if size <= 0 or abs(size_from_pnl - size) / max(1.0, abs(size)) > 0.05:
                size = size_from_pnl
                rowd["size"] = size

        # notional SIEMPRE a entry
        entry_notional = abs(size) * entry
        notional = entry_notional if entry_notional > 0 else _f(rowd.get("notional"))
        rowd["notional"] = notional

        # realized neto
        realized = rowd.get("realized_pnl")
        if realized is None:
            realized = pnl_price + funding_total + fee_total
        realized = _f(realized)
        rowd["realized_pnl"] = realized

        # leverage: API > deducido > default exchange > 0
        leverage = _f(rowd.get("leverage"))
        if not _positive(leverage):
            im_val = _f(rowd.get("initial_margin"))
            if _positive(im_val) and _positive(notional):
                leverage = notional / im_val
            elif exchange in DEFAULT_LEVERAGE and _positive(DEFAULT_LEVERAGE[exchange]):
                leverage = float(DEFAULT_LEVERAGE[exchange])
            else:
                leverage = 0.0
        rowd["leverage"] = leverage

        # initial_margin
        im_val = _f(rowd.get("initial_margin"))
        if not _positive(im_val):
            if _positive(leverage) and _positive(notional):
                im_val = notional / leverage
            else:
                im_val = notional
        rowd["initial_margin"] = im_val

        # pnl_percent y apr SIEMPRE desde realized_pnl
        base_capital = im_val if _positive(im_val) else notional
        pnl_percent = (realized / base_capital) * 100.0 if _positive(base_capital) else 0.0
        rowd["pnl_percent"] = pnl_percent

        days = max((close_s - open_s) / 86400.0, 1e-9) if (open_s and close_s) else 0.0
        apr = pnl_percent * (365.0 / days) if days > 0 else 0.0
        rowd["apr"] = apr

        # asegúrate de persistir pnl precio recalculado si cambió
        rowd["pnl"] = pnl_price

    # 4) Construir UPDATE dinámico (solo columnas cambiadas o recalculadas)
    set_cols = []
    vals = []
    for key in allowed:
        if key in rowd and (key in updates or recompute):  # actualiza lo pedido y lo recalculado
            set_cols.append(f"{key} = ?")
            vals.append(rowd[key])

    if not set_cols:
        print("ℹ️ patch_closed_position: nada que actualizar.")
        conn.close()
        return 0

    update_sql = f"UPDATE closed_positions SET {', '.join(set_cols)} WHERE {where_sql}"
    vals.extend(where_params)

    try:
        cur.execute(update_sql, vals)
        conn.commit()
        changed = cur.rowcount
        print(f"✅ patch_closed_position: {changed} fila(s) actualizada(s).")
        return changed
    except Exception as e:
        print(f"❌ patch_closed_position ERROR: {e}")
        print("   SQL:", update_sql)
        print("   vals:", vals)
        conn.rollback()
        return 0
    finally:
        conn.close()

