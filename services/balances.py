def aggregate(balances):
    totals = {"equity":0.0, "balance":0.0, "unrealized_pnl":0.0}
    for b in balances:
        totals["equity"] += b.get("equity",0.0)
        totals["balance"] += b.get("balance",0.0)
        totals["unrealized_pnl"] += b.get("unrealized_pnl",0.0)
    return totals

