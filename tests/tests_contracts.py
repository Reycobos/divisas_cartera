from flask import Blueprint, jsonify
from config.toggles import should_fetch_balance
from adapters.registry import get_adapters
from services.balances import aggregate

bp = Blueprint("balances", __name__)

@bp.route("/api/balances")
def api_balances():
    rows = []
    for slug, adapter in get_adapters():
        if not should_fetch_balance(slug): continue
        b = adapter.fetch_all_balances()
        if b: rows.append(b)
    totals = aggregate(rows)
    return jsonify({"totals": totals, "exchanges": rows})
