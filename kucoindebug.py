from adapters.kucoin import fetch_closed_positions_kucoin, save_kucoin_closed_positions
# Dry-run, ver cuántas posiciones trae y algunos campos
items = fetch_closed_positions_kucoin(limit=50, debug=True)
print(len(items), items[:1])
# Guardar
save_kucoin_closed_positions("portfolio.db", debug=True)

