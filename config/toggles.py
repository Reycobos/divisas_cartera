SYNC_ALL = False
SYNC_EXCHANGES = {
    "backpack": False, "aden": False, "bingx": True, "aster": False,
    "binance": False, "extended": False, "kucoin": False, "gate": True,
}
def should_sync(exchange_name: str) -> bool:
    return bool(SYNC_ALL or SYNC_EXCHANGES.get(exchange_name, False))

BALANCE_ALL = False
BALANCE_EXCHANGES = {
    "backpack": False, "aden": False, "bingx": False, "aster": False,
    "binance": False, "extended": False, "kucoin": False, "gate": True,
}
def should_fetch_balance(exchange_name: str) -> bool:
    return bool(BALANCE_ALL or BALANCE_EXCHANGES.get(exchange_name, False))


