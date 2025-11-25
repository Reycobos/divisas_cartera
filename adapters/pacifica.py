"""
🌊 PACIFICA ADAPTER - Delta Neutral Portfolio Tracker
Exchange: Pacifica (Solana Perpetual DEX)
⚠️ FUNDING: HOURLY (cada 1h, no 8h como otros exchanges)

🔐 AUTENTICACIÓN:
- ✅ Endpoints de LECTURA (GET): Solo requieren 'account' (wallet address público)
  · /api/v1/account (balances)
  · /api/v1/positions (open positions)
  · /api/v1/trades/history (closed positions)
  · /api/v1/funding/history (funding fees)

- 🔒 Endpoints de ESCRITURA (POST): Requieren firma Ed25519
  · Para trading necesitarás hardware wallet signing
  · Ver: https://docs.pacifica.fi/api-documentation/api/signing

Funcionalidades:
- ✅ Balances (account info)
- ✅ Open Positions
- ✅ Closed Positions (reconstrucción FIFO desde trades)
- ✅ Funding Fees (hourly → normalized)
- ✅ Deduplicación en DB
- ✅ Funciones de debug CLI
"""

import os
import sys
import json
import time
import hashlib
import requests
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from datetime import datetime, timezone
from dotenv import load_dotenv


# Cargar .env
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Agregar path para imports locales
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# ========== CONFIGURACIÓN ==========

# 🔐 Credenciales desde .env
# PACIFICA_ACCOUNT = os.getenv("PACIFICA_ACCOUNT")
PACIFICA_ACCOUNT = "3LTN5unrUtdYPVKdnwfXiLujmkchNecXC5KhvCNY2iSt"
# Validar que existan las credenciales
if not PACIFICA_ACCOUNT:
    raise ValueError(
        "❌ Falta PACIFICA_ACCOUNT en .env\n"
        "Agrega:\n"
        "  PACIFICA_ACCOUNT=tu_wallet_address_solana"
    )

# Base URLs
PACIFICA_BASE_URL = "https://api.pacifica.fi/api/v1"
PACIFICA_TESTNET_URL = "https://test-api.pacifica.fi/api/v1"

# Headers
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
}

print(
    f"✅ Pacifica Adapter cargado | Account: {PACIFICA_ACCOUNT[:8]}...{PACIFICA_ACCOUNT[-4:]}"
)


# ========== FUNCIONES AUXILIARES ==========


def _safe_float(value, default=0.0):
    """Convierte a float de forma segura."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _safe_int(value, default=0):
    """Convierte a int de forma segura."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _fmt_time(ms):
    """Convierte timestamp ms a formato legible."""
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    except:
        return str(ms)


def _generate_trade_hash(trade: dict) -> str:
    """
    Genera hash único para un trade (ya que Pacifica no tiene trade_id único).
    Usa: exchange + symbol + created_at + amount + price
    """
    key = (
        f"pacifica-"
        f"{trade.get('symbol')}-"
        f"{trade.get('created_at')}-"
        f"{trade.get('amount')}-"
        f"{trade.get('price')}"
    )
    return hashlib.md5(key.encode()).hexdigest()


def _request(
    method: str,
    endpoint: str,
    params: dict = None,
    testnet: bool = False,
    debug: bool = False,
) -> dict:
    """
    Wrapper para requests HTTP a Pacifica API.

    Args:
        method: GET, POST, etc
        endpoint: "/positions", "/account", etc
        params: Query params
        testnet: Si True, usa testnet URL
        debug: Si True, imprime detalles

    Returns:
        dict con data de la respuesta

    Raises:
        Exception si hay error
    """
    base_url = PACIFICA_TESTNET_URL if testnet else PACIFICA_BASE_URL
    url = f"{base_url}{endpoint}"

    try:
        if debug:
            print(f"🌐 {method} {url}")
            if params:
                print(f"   Params: {params}")

        response = requests.request(
            method, url, params=params, headers=UA_HEADERS, timeout=30
        )
        response.raise_for_status()
        data = response.json()

        # Pacifica devuelve: {"success": true, "data": [...], "error": null}
        if not data.get("success"):
            error_msg = data.get("error", "Unknown error")
            raise Exception(f"API Error: {error_msg}")

        if debug:
            print(f"   ✅ Success: {len(data.get('data', []))} items")

        return data.get("data", [])

    except requests.exceptions.Timeout:
        print(f"❌ Timeout en request a {url}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"❌ Error en request a {url}: {e}")
        raise
    except Exception as e:
        print(f"❌ Error procesando respuesta de {url}: {e}")
        raise


# ========== 1️⃣ BALANCES ==========


def fetch_pacifica_all_balances(debug: bool = False) -> dict:
    """
    GET /api/v1/account

    Returns formato compatible con HTML:
        {
            "exchange": "pacifica",
            "equity": 2150.25,
            "balance": 2000.00,
            "unrealized_pnl": 50.0,
            "spot": 0,
            "margin": 0,
            "futures": 2000.00
        }
    """
    try:
        data = _request(
            "GET", "/account", params={"account": PACIFICA_ACCOUNT}, debug=debug
        )

        # data es una lista con 1 elemento
        if isinstance(data, list) and len(data) > 0:
            account_info = data[0]
        elif isinstance(data, dict):
            account_info = data
        else:
            if debug:
                print(f"⚠️ Formato inesperado de data: {type(data)}")
            return {}

        # Obtener valores
        balance = _safe_float(account_info.get("balance"))
        equity = _safe_float(account_info.get("account_equity"))

        # Calcular unrealized_pnl
        # unrealized_pnl = equity - balance
        unrealized_pnl = equity - balance

        # 🔥 FORMATO COMPATIBLE CON HTML
        result = {
            "exchange": "pacifica",
            "equity": equity,  # ✅ NO "account_equity"
            "balance": balance,  # ✅
            "unrealized_pnl": unrealized_pnl,  # ✅ Calculado
            "spot": 0.0,  # Pacifica no tiene spot separado
            "margin": 0.0,  # Pacifica no tiene margin separado
            "futures": balance,  # Todo es futures en Pacifica
        }

        if debug:
            print(f"💰 Pacifica Balance:")
            print(f"   Equity: ${result['equity']:.2f}")
            print(f"   Balance: ${result['balance']:.2f}")
            print(f"   Unrealized PnL: ${result['unrealized_pnl']:.2f}")

        return result

    except Exception as e:
        print(f"❌ Error fetching Pacifica balances: {e}")
        import traceback

        traceback.print_exc()
        return {}


# ========== 2️⃣ PRICES (MARK PRICE) ==========


def fetch_pacifica_prices(debug: bool = False) -> dict:
    """
    GET /api/v1/info/prices

    Obtiene mark prices, funding rates y otros datos de mercado.

    Returns:
        dict: {
            "BTC": {"mark": 50000.0, "funding": 0.0001, ...},
            "ETH": {"mark": 3000.0, "funding": 0.00008, ...}
        }
    """
    try:
        data = _request("GET", "/info/prices", debug=debug)

        prices = {}
        for item in data:
            symbol_raw = item.get("symbol", "")

            # Normalizar símbolo
            symbol = (
                symbol_raw.replace("-USDC", "")
                .replace("-USDT", "")
                .replace("-USD", "")
                .replace("USDT", "")
                .replace("USDC", "")
            )

            prices[symbol] = {
                "mark": _safe_float(item.get("mark")),
                "funding": _safe_float(item.get("funding")),
                "next_funding": _safe_float(item.get("next_funding")),
                "oracle": _safe_float(item.get("oracle")),
                "open_interest": _safe_float(item.get("open_interest")),
                "volume_24h": _safe_float(item.get("volume_24h")),
            }

        if debug:
            print(f"📊 Prices fetched: {len(prices)} symbols")

        return prices

    except Exception as e:
        print(f"❌ Error fetching Pacifica prices: {e}")
        return {}


# ========== 3️⃣ OPEN POSITIONS (ENRIQUECIDO CON MARK PRICE, FEES, FUNDING) ==========


def fetch_pacifica_open_positions(debug: bool = False) -> list:
    """
    GET /api/v1/positions (enriquecido con mark_price, fees, funding, unrealized_pnl)

    🔥 VERSIÓN ENRIQUECIDA:
    - ✅ mark_price: Desde /info/prices
    - ✅ unrealized_pnl: Calculado (mark - entry) * size * side
    - ✅ fees_total: Acumulado desde trades abiertos
    - ✅ funding_total: Acumulado desde open_time
    - ✅ leverage: Calculado notional/margin

    Returns:
        [
            {
                "exchange": "pacifica",
                "symbol": "AAVE",
                "side": "short",
                "size": 223.72,
                "entry_price": 279.28,
                "mark_price": 280.50,        # ✅ NUEVO
                "unrealized_pnl": -273.04,   # ✅ NUEVO
                "fees_total": -12.50,        # ✅ NUEVO
                "funding_total": 13.16,      # ✅ ACTUALIZADO
                "leverage": 10.0,            # ✅ NUEVO
                ...
            }
        ]
    """
    try:
        # 1️⃣ Fetch posiciones base
        positions_data = _request(
            "GET", "/positions", params={"account": PACIFICA_ACCOUNT}, debug=debug
        )

        if not positions_data:
            if debug:
                print("⚠️ No hay posiciones abiertas")
            return []

        # 2️⃣ Fetch mark prices
        if debug:
            print("\n📊 Fetching mark prices...")
        prices = fetch_pacifica_prices(debug=False)

        # 3️⃣ Fetch funding history (últimos 30 días)
        if debug:
            print("💸 Fetching funding history...")
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - (30 * 24 * 60 * 60 * 1000)

        funding_events = fetch_pacifica_funding_fees(
            start_time=start_ms, end_time=now_ms, debug=False
        )

        # Agrupar por symbol
        funding_by_symbol = defaultdict(list)
        for f in funding_events:
            funding_by_symbol[f["symbol"]].append(f)

        # 4️⃣ Fetch trades (para fees)
        if debug:
            print("📜 Fetching trades history...")
        trades_history = fetch_pacifica_trades_history(
            start_time=start_ms, end_time=now_ms, debug=False
        )

        # Agrupar por symbol
        trades_by_symbol = defaultdict(list)
        for t in trades_history:
            symbol_raw = t.get("symbol", "")
            symbol = (
                symbol_raw.replace("-USDC", "")
                .replace("-USDT", "")
                .replace("-USD", "")
                .replace("USDT", "")
                .replace("USDC", "")
            )
            trades_by_symbol[symbol].append(t)

        # 5️⃣ Procesar cada posición
        positions = []

        for pos in positions_data:
            symbol_raw = pos.get("symbol", "")

            # Normalizar
            symbol = (
                symbol_raw.replace("-USDC", "")
                .replace("-USDT", "")
                .replace("-USD", "")
                .replace("USDT", "")
                .replace("USDC", "")
            )

            # Side
            side_raw = pos.get("side", "").lower()
            side = "long" if side_raw == "bid" else "short"
            side_multiplier = 1 if side == "long" else -1

            # Datos base
            size = _safe_float(pos.get("amount"))
            entry_price = _safe_float(pos.get("entry_price"))
            open_time = _safe_int(pos.get("created_at"))

            # 🔥 MARK PRICE
            mark_price = prices.get(symbol, {}).get("mark", entry_price)

            # 🔥 UNREALIZED PNL
            unrealized_pnl = (mark_price - entry_price) * size * side_multiplier

            # 🔥 FEES

            fees_total = 0.0
            for trade in trades_by_symbol.get(symbol, []):
                trade_time = _safe_int(trade.get("created_at"))
                trade_side = trade.get("side", "")

                if trade_time >= open_time and (
                    (side == "long" and "open_long" in trade_side)
                    or (side == "short" and "open_short" in trade_side)
                ):
                    fee_value = _safe_float(trade.get("fee"))
                    # ✅ Asegurar que fees sean negativas (son un gasto)
                    fees_total += -abs(fee_value)

            # 🔥 FUNDING
            funding_total = 0.0
            for f in funding_by_symbol.get(symbol, []):
                if f["timestamp"] >= open_time:
                    funding_total += f["income"]

            # 🔥 LEVERAGE
            notional = size * entry_price
            margin_used = _safe_float(pos.get("margin"))
            leverage = notional / margin_used if margin_used > 0 else 10.0
            realized_pnl = fees_total + funding_total

            # Construir dict
            position_dict = {
                "exchange": "pacifica",
                "symbol": symbol,
                "side": side,
                "size": size,
                "entry_price": entry_price,
                "mark_price": mark_price,
                "unrealized_pnl": unrealized_pnl,
                "fee": fees_total,
                "funding_fee": funding_total,
                "realized_pnl": realized_pnl,
                "isolated": pos.get("isolated", False),
                "open_time": open_time,
                "updated_at": _safe_int(pos.get("updated_at")),
                "leverage": leverage,
                "notional": notional,
                "main_margin": margin_used,  # ✅ AGREGAR main_margin desde margin de API
            }

            positions.append(position_dict)

            if debug:
                print(f"\n   {symbol} | {side.upper()}")
                print(f"      Entry: ${entry_price:.4f} | Mark: ${mark_price:.4f}")
                print(
                    f"      uPnL: {unrealized_pnl:+.2f} | Fees: {fees_total:.2f} | Funding: {funding_total:+.2f}"
                )

        if debug:
            print(f"\n✅ Total: {len(positions)} positions")
            print(f"   Total uPnL: {sum(p['unrealized_pnl'] for p in positions):+.2f}")

        return positions

    except Exception as e:
        print(f"❌ Error fetching Pacifica open positions: {e}")
        import traceback

        traceback.print_exc()
        return []


# ========== 3️⃣ TRADE HISTORY (para reconstruir closed positions) ==========


def fetch_pacifica_trades_history(
    symbol: str = None,
    start_time: int = None,
    end_time: int = None,
    limit: int = 1000,
    debug: bool = False,
) -> list:
    """
    GET /api/v1/trades/history

    Fetch raw trades history (NO son posiciones cerradas, hay que reconstruirlas con FIFO).

    Args:
        symbol: Filtrar por símbolo (opcional)
        start_time: Timestamp ms inicio
        end_time: Timestamp ms fin
        limit: Max items por request (default 1000)
        debug: Print debug info

    Returns:
        [
            {
                "history_id": 19329801,
                "order_id": 315293920,
                "symbol": "LDO",
                "amount": "0.1",
                "price": "1.1904",
                "entry_price": "1.176247",
                "fee": "0",
                "pnl": "-0.001415",
                "event_type": "fulfill_maker",  # or "fulfill_taker"
                "side": "close_short",  # open_long, open_short, close_long, close_short
                "created_at": 1759215599188,
                "cause": "normal"  # or "market_liquidation", "backstop_liquidation", "settlement"
            }
        ]
    """
    try:
        all_trades = []
        cursor = None
        page = 0

        while True:
            page += 1

            params = {"account": PACIFICA_ACCOUNT, "limit": limit}

            if symbol:
                params["symbol"] = symbol
            if start_time:
                params["start_time"] = start_time
            if end_time:
                params["end_time"] = end_time
            if cursor:
                params["cursor"] = cursor

            if debug:
                print(f"📄 Página {page} | cursor={cursor}")

            response = requests.get(
                f"{PACIFICA_BASE_URL}/trades/history",
                params=params,
                headers=UA_HEADERS,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            if not data.get("success"):
                break

            trades = data.get("data", [])
            all_trades.extend(trades)

            # Paginación
            has_more = data.get("has_more", False)
            cursor = data.get("next_cursor")

            if debug:
                print(
                    f"   Trades: {len(trades)} | Total: {len(all_trades)} | Has more: {has_more}"
                )

            if not has_more or not cursor:
                break

            time.sleep(0.1)  # Rate limiting

        if debug:
            print(f"✅ Total trades fetched: {len(all_trades)}")

        return all_trades

    except Exception as e:
        print(f"❌ Error fetching Pacifica trades history: {e}")
        return []


# ========== 4️⃣ RECONSTRUCCIÓN FIFO ==========


def reconstruct_closed_positions_from_trades(trades: list, debug: bool = False) -> list:
    """
    🔥 FUNCIÓN CLAVE: Reconstruye posiciones cerradas desde trades usando FIFO.

    Lógica FIFO (igual que Binance):
    1. Agrupar trades por symbol
    2. Para cada symbol, procesar cronológicamente:
       - "open_long" / "open_short" → ABRE posición (qty signed positivo/negativo)
       - "close_long" / "close_short" → CIERRA posición (qty signed negativo/positivo)
    3. Cuando net_qty vuelve a 0 → posición cerrada completa
    4. Calcular métricas: entry_price, close_price, pnl, fees, funding

    Args:
        trades: Lista de trades raw de /trades/history
        debug: Print debug info

    Returns:
        [
            {
                "exchange": "pacifica",
                "symbol": "BTC-USDC",
                "side": "long",  # o "short"
                "size": 0.1,
                "entry_price": 50000.0,
                "close_price": 51000.0,
                "open_time": 1700000000000,
                "close_time": 1700086400000,
                "pnl": 100.0,  # precio puro
                "realized_pnl": 95.0,  # neto (incluye fees)
                "fee": -5.0,
                "funding_total": 0.0,  # Se calcula después desde funding endpoint
                "pnl_percent": 2.0,
                "initial_margin": 5000.0,
                "notional": 50000.0,
                "leverage": 10.0,
                "liquidation_price": None,
                "is_liquidation": False,
            }
        ]
    """
    if not trades:
        return []

    # Agrupar por symbol
    trades_by_symbol = defaultdict(list)
    for t in trades:
        symbol = t.get("symbol", "")
        trades_by_symbol[symbol].append(t)

    closed_positions = []

    for symbol_raw, symbol_trades in trades_by_symbol.items():
        # Symbol sin sufijo para compatibilidad con tu sistema
        symbol = symbol_raw

        # Ordenar cronológicamente
        symbol_trades_sorted = sorted(
            symbol_trades, key=lambda x: x.get("created_at", 0)
        )

        if debug:
            print(f"\n{'='*60}")
            print(f"🔍 Symbol: {symbol} | Trades: {len(symbol_trades_sorted)}")

        net_qty = 0.0
        block = []  # Trades del bloque actual

        for trade in symbol_trades_sorted:
            side = trade.get("side", "").lower()
            amount = _safe_float(trade.get("amount"))

            # Determinar qty signed:
            # open_long / close_short → positivo
            # open_short / close_long → negativo
            if "open_long" in side or "close_short" in side:
                qty_signed = amount
            else:  # open_short o close_long
                qty_signed = -amount

            net_qty += qty_signed
            block.append(trade)

            if debug:
                print(
                    f"  {_fmt_time(trade.get('created_at'))} | {side:15s} | amt={amount:8.4f} | net_qty={net_qty:8.4f}"
                )

            # ✅ Posición cerrada: net_qty vuelve a ~0
            if abs(net_qty) < 1e-8:
                if debug:
                    print(f"  ✅ POSICIÓN CERRADA | Block size: {len(block)} trades")

                # Calcular métricas de la posición cerrada
                closed_pos = _calculate_closed_position_metrics(
                    symbol, block, debug=debug
                )
                closed_positions.append(closed_pos)

                # Reset para siguiente posición
                block = []
                net_qty = 0.0

    if debug:
        print(f"\n{'='*60}")
        print(f"✅ Total closed positions reconstruidas: {len(closed_positions)}")

    return closed_positions


def _calculate_closed_position_metrics(
    symbol: str, block: list, debug: bool = False
) -> dict:
    """
    Calcula métricas de una posición cerrada desde un bloque de trades.

    Args:
        symbol: Símbolo normalizado
        block: Lista de trades que forman la posición completa
        debug: Print debug

    Returns:
        dict con métricas de la posición
    """
    if not block:
        return {}

    # Separar opens vs closes
    opens = []
    closes = []

    for t in block:
        side = t.get("side", "").lower()
        if "open" in side:
            opens.append(t)
        else:  # close
            closes.append(t)

    if not opens or not closes:
        if debug:
            print(f"  ⚠️ Block incompleto: opens={len(opens)}, closes={len(closes)}")
        return {}

    # Determinar side de la posición (long o short)
    first_open_side = opens[0].get("side", "").lower()
    position_side = "long" if "long" in first_open_side else "short"

    # Calcular entry_price (promedio ponderado de opens)
    total_entry_notional = 0.0
    total_entry_qty = 0.0

    for t in opens:
        amount = _safe_float(t.get("amount"))
        price = _safe_float(t.get("entry_price") or t.get("price"))
        total_entry_notional += amount * price
        total_entry_qty += amount

    entry_price = total_entry_notional / total_entry_qty if total_entry_qty > 0 else 0.0

    # Calcular close_price (promedio ponderado de closes)
    total_close_notional = 0.0
    total_close_qty = 0.0

    for t in closes:
        amount = _safe_float(t.get("amount"))
        price = _safe_float(t.get("price"))
        total_close_notional += amount * price
        total_close_qty += amount

    close_price = total_close_notional / total_close_qty if total_close_qty > 0 else 0.0

    # Size = total opened
    size = total_entry_qty

    # Timestamps - convertir de ms a segundos para DB
    open_time_ms = _safe_int(block[0].get("created_at"))
    close_time_ms = _safe_int(block[-1].get("created_at"))
    open_time = open_time_ms // 1000  # Convertir a segundos
    close_time = close_time_ms // 1000  # Convertir a segundos

    # PnL de precio (sin fees) - usar el PnL reportado por la API si está disponible
    # Pacifica reporta 'pnl' en cada trade de cierre
    pnl_from_api = sum(_safe_float(t.get("pnl", 0)) for t in closes)

    # Calcular PnL manual como fallback
    if position_side == "long":
        pnl_manual = (close_price - entry_price) * size
    else:  # short
        pnl_manual = (entry_price - close_price) * size

    # Usar PnL de API si está disponible y es razonable, sino usar manual
    pnl_price = pnl_from_api if abs(pnl_from_api) > 0.001 else pnl_manual

    # Fees (sumar todos los fees del block, convertir a negativo)
    total_fees = sum(_safe_float(t.get("fee", 0)) for t in block)
    fee_total = -abs(total_fees)  # Fees siempre negativos

    # PnL realizado neto (precio + fees)
    # Nota: funding se agregará después desde el endpoint de funding
    realized_pnl = pnl_price + fee_total

    # Notional
    notional = entry_price * size

    # Initial margin (asumimos leverage 10 por ahora, se puede ajustar)
    leverage = 10.0
    initial_margin = notional / leverage if leverage > 0 else notional

    # Calcular APR y pnl_percent (timestamps ya están en segundos)
    days = (
        max((close_time - open_time) / 86400, 1e-9)
        if (open_time and close_time)
        else 0.0
    )
    pnl_percent = (realized_pnl / initial_margin) * 100.0 if initial_margin > 0 else 0.0
    apr = pnl_percent * (365.0 / days) if days > 0 else 0.0

    # Detectar liquidaciones
    is_liquidation = any(t.get("cause", "normal") != "normal" for t in block)

    if debug:
        print(f"  📊 Métricas:")
        print(f"     Side: {position_side}")
        print(f"     Size: {size:.4f}")
        print(f"     Entry: {entry_price:.2f}")
        print(f"     Close: {close_price:.2f}")
        print(f"     PnL (price): {pnl_price:.2f}")
        print(f"     Fees: {fee_total:.2f}")
        print(f"     Realized PnL: {realized_pnl:.2f}")
        print(f"     Days: {days:.2f}")
        print(f"     APR: {apr:.1f}%")
        if is_liquidation:
            print(f"     ⚠️ LIQUIDACIÓN")

    return {
        "exchange": "pacifica",
        "symbol": symbol,
        "side": position_side,
        "size": size,
        "entry_price": entry_price,
        "close_price": close_price,
        "open_time": open_time,
        "close_time": close_time,
        "pnl": pnl_price,
        "realized_pnl": realized_pnl,
        "fee_total": fee_total,
        "funding_total": 0.0,  # Se llenará después
        "pnl_percent": pnl_percent,
        "apr": apr,
        "initial_margin": initial_margin,
        "notional": notional,
        "leverage": leverage,
        "liquidation_price": None,
        "is_liquidation": is_liquidation,
        "_lock_size": True,  # Evitar que save_closed_position recalcule size
    }


# ========== 5️⃣ FUNDING FEES ==========


def fetch_pacifica_funding_fees(
    start_time: int = None, end_time: int = None, limit: int = 1000, debug: bool = False
) -> list:
    """
    GET /api/v1/funding/history

    ⚠️ CRÍTICO: Pacifica usa HOURLY funding (cada 1h).
    Retorna formato compatible con FUNDING_PULLERS de portfolio.py

    Args:
        start_time: Timestamp ms inicio (opcional)
        end_time: Timestamp ms fin (opcional)
        limit: Max items por request
        debug: Print debug

    Returns:
        [
            {
                "exchange": "pacifica",
                "symbol": "PUMP",  # ya normalizado (sin -USDC)
                "income": 2.617479,
                "timestamp": 1759222804122,
                "funding_rate": 0.0000125,
                "type": "FUNDING_FEE",
                "external_id": "2287920",
                "asset": "USDT"
            }
        ]
    """
    try:
        all_funding = []
        cursor = None
        page = 0

        while True:
            page += 1

            params = {"account": PACIFICA_ACCOUNT, "limit": limit}

            if cursor:
                params["cursor"] = cursor

            if debug:
                print(f"💸 Funding Página {page} | cursor={cursor}")

            response = requests.get(
                f"{PACIFICA_BASE_URL}/funding/history",
                params=params,
                headers=UA_HEADERS,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            if not data.get("success"):
                break

            funding_items = data.get("data", [])

            for item in funding_items:
                symbol_raw = item.get("symbol", "")
                timestamp = _safe_int(item.get("created_at"))

                # Filtrar por rango de tiempo si se especifica
                if start_time and timestamp < start_time:
                    continue
                if end_time and timestamp > end_time:
                    continue

                # Normalizar símbolo (quitar sufijos como hace _base_symbol en portfolio.py)
                # "BTC" → "BTC" (sin -USDC para compatibilidad con tu sistema)
                symbol = (
                    symbol_raw.replace("-USDC", "")
                    .replace("-USDT", "")
                    .replace("-USD", "")
                    .replace("USDT", "")
                    .replace("USDC", "")
                )

                all_funding.append(
                    {
                        "exchange": "pacifica",
                        "symbol": symbol,
                        "income": _safe_float(item.get("payout")),
                        "timestamp": timestamp,
                        "funding_rate": _safe_float(item.get("rate")),
                        "type": "FUNDING_FEE",
                        "external_id": str(item.get("history_id", "")),
                        "asset": "USDT",  # Pacifica usa USDT/USDC
                    }
                )

            # Paginación
            has_more = data.get("has_more", False)
            cursor = data.get("next_cursor")

            if debug:
                print(
                    f"   Items: {len(funding_items)} | Total: {len(all_funding)} | Has more: {has_more}"
                )

            if not has_more or not cursor:
                break

            time.sleep(0.1)  # Rate limiting

        if debug:
            print(f"✅ Total funding events: {len(all_funding)}")
            if all_funding:
                total_income = sum(f["income"] for f in all_funding)
                print(f"   Total income: {total_income:.2f} USDC")

        return all_funding

    except Exception as e:
        print(f"❌ Error fetching Pacifica funding: {e}")
        return []


def associate_funding_to_closed_positions(
    closed_positions: list, funding_events: list, debug: bool = False
) -> list:
    """
    Asocia funding fees a cada posición cerrada.

    Para cada posición:
    1. Buscar funding events del mismo symbol entre open_time y close_time
    2. Sumar funding_total
    3. Recalcular realized_pnl = pnl + fee_total + funding_total
    4. Recalcular pnl_percent y apr

    Args:
        closed_positions: Lista de posiciones cerradas
        funding_events: Lista de funding events
        debug: Print debug

    Returns:
        Lista de posiciones con funding asociado
    """
    # Agrupar funding por symbol (símbolos ya vienen normalizados)
    funding_by_symbol = defaultdict(list)
    for f in funding_events:
        funding_by_symbol[f["symbol"]].append(f)

    updated_positions = []

    for pos in closed_positions:
        symbol = pos["symbol"]  # Ya viene sin sufijos
        open_time_s = pos["open_time"]  # Ya en segundos
        close_time_s = pos["close_time"]  # Ya en segundos
        position_side = pos.get("side", "long")

        # Buscar funding del symbol en el rango de tiempo
        # funding timestamp está en ms, convertir a segundos para comparar
        relevant_funding = [
            f
            for f in funding_by_symbol.get(symbol, [])
            if open_time_s <= (f["timestamp"] // 1000) <= close_time_s
        ]

        # Sumar funding - el 'income' (payout) ya tiene el signo correcto:
        # - Positivo = recibiste funding = ganancia
        # - Negativo = pagaste funding = pérdida
        # NO invertir el signo, usarlo tal cual viene de la API
        funding_total = sum(f["income"] for f in relevant_funding)

        # Actualizar posición
        pos["funding_total"] = funding_total

        # Recalcular realized_pnl
        pnl_price = pos["pnl"]
        fee_total = pos["fee_total"]
        pos["realized_pnl"] = pnl_price + fee_total + funding_total

        # Recalcular pnl_percent y apr (timestamps ya en segundos)
        initial_margin = pos.get("initial_margin", pos.get("notional", 0))
        if initial_margin > 0:
            pos["pnl_percent"] = (pos["realized_pnl"] / initial_margin) * 100.0

            days = max((close_time_s - open_time_s) / 86400, 1e-9)
            pos["apr"] = pos["pnl_percent"] * (365.0 / days) if days > 0 else 0.0

        if debug and relevant_funding:
            print(
                f"  💸 {symbol} | Funding events: {len(relevant_funding)} | Total: {funding_total:.2f}"
            )

        updated_positions.append(pos)

    return updated_positions


# ========== 6️⃣ GUARDADO EN DB ==========


def save_pacifica_closed_positions(
    db_path: str = "portfolio.db", days: int = 30, debug: bool = False
):
    """
    Pipeline completo para guardar posiciones cerradas de Pacifica.
    Sigue el patrón de otros exchanges (mexc, gate, binance, etc).

    Args:
        db_path: Path a la base de datos (ej: "portfolio.db")
        days: Cuántos días hacia atrás buscar
        debug: Print debug info
    """
    try:
        if debug or True:  # Siempre mostrar header básico
            print(f"🌊 Pacifica: sincronizando closed positions ({days}d)...")

        # 1️⃣ Fetch trades
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - (days * 24 * 60 * 60 * 1000)

        trades = fetch_pacifica_trades_history(
            start_time=start_ms, end_time=now_ms, debug=debug
        )

        if not trades:
            if debug:
                print("   ⚠️ No hay trades para procesar")
            return

        if debug:
            print(f"   📄 {len(trades)} trades fetched")

        # 2️⃣ Reconstruir posiciones con FIFO
        closed_positions = reconstruct_closed_positions_from_trades(trades, debug=debug)

        if not closed_positions:
            if debug:
                print("   ⚠️ No se encontraron posiciones cerradas")
            return

        if debug:
            print(f"   🔄 {len(closed_positions)} posiciones reconstruidas con FIFO")

        # 3️⃣ Fetch funding
        funding_events = fetch_pacifica_funding_fees(
            start_time=start_ms, end_time=now_ms, debug=debug
        )

        if debug and funding_events:
            print(f"   💸 {len(funding_events)} funding events fetched")

        # 4️⃣ Asociar funding a posiciones
        closed_positions = associate_funding_to_closed_positions(
            closed_positions, funding_events, debug=debug
        )

        # 5️⃣ Guardar en DB con deduplicación
        # Importar db_manager (adaptado al path de tu proyecto)
        sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
        from db_manager import save_closed_position

        import sqlite3

        conn = sqlite3.connect(db_path)

        saved = 0
        skipped = 0

        for pos in closed_positions:
            # Verificar si ya existe (deduplicación)
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id FROM closed_positions
                WHERE exchange = ? AND symbol = ? AND close_time = ? AND abs(size - ?) < 0.0001
                """,
                (pos["exchange"], pos["symbol"], pos["close_time"], pos["size"]),
            )

            if cursor.fetchone():
                skipped += 1
                if debug:
                    print(
                        f"   ⏭️ Skip: {pos['symbol']} @ {_fmt_time(pos['close_time'])}"
                    )
                continue

            # Guardar
            try:
                save_closed_position(pos)
                saved += 1
                if debug:
                    print(
                        f"   ✅ {pos['symbol']} | PnL: {pos['realized_pnl']:.2f} | APR: {pos['apr']:.1f}%"
                    )
            except Exception as e:
                print(f"   ❌ Error: {pos['symbol']}: {e}")

        conn.close()

        print(f"   ✅ Pacifica: {saved} guardadas, {skipped} duplicadas")

    except Exception as e:
        print(f"   ❌ Error en Pacifica sync: {e}")
        if debug:
            import traceback

            traceback.print_exc()


def save_pacifica_funding_events(days: int = 30, debug: bool = False):
    """
    Guarda funding events en funding_events table.

    Deduplicación por ext_hash = hash(exchange + symbol + timestamp)
    """
    try:
        print(f"\n{'='*80}")
        print(f"💸 PACIFICA - Guardando Funding Events (últimos {days} días)")
        print(f"{'='*80}")

        # Fetch funding
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - (days * 24 * 60 * 60 * 1000)

        funding_events = fetch_pacifica_funding_fees(
            start_time=start_ms, end_time=now_ms, debug=debug
        )

        if not funding_events:
            print("⚠️ No hay funding events para guardar")
            return

        print(f"✅ {len(funding_events)} funding events fetched")

        # Conectar a DB
        import sqlite3

        conn = sqlite3.connect("portfolio.db")
        cursor = conn.cursor()

        # Crear tabla si no existe
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS funding_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exchange TEXT,
                symbol TEXT,
                income REAL,
                timestamp INTEGER,
                external_id TEXT,
                ext_hash TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        saved = 0
        skipped = 0

        for event in funding_events:
            # Generar hash para deduplicación
            ext_hash = hashlib.md5(
                f"{event['exchange']}-{event['symbol']}-{event['timestamp']}".encode()
            ).hexdigest()

            try:
                cursor.execute(
                    """
                    INSERT INTO funding_events (exchange, symbol, income, timestamp, external_id, ext_hash)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        event["exchange"],
                        event["symbol"],
                        event["income"],
                        event["timestamp"],
                        event.get("external_id", ""),
                        ext_hash,
                    ),
                )
                saved += 1

                if debug:
                    print(
                        f"   ✅ {event['symbol']} | {event['income']:.4f} @ {_fmt_time(event['timestamp'])}"
                    )

            except sqlite3.IntegrityError:
                # Ya existe
                skipped += 1
                if debug:
                    print(
                        f"   ⏭️ Skip: {event['symbol']} @ {_fmt_time(event['timestamp'])}"
                    )

        conn.commit()
        conn.close()

        print(f"\n{'='*80}")
        print(f"✅ Funding events guardados:")
        print(f"   Nuevos: {saved}")
        print(f"   Duplicados (skip): {skipped}")
        print(f"{'='*80}\n")

    except Exception as e:
        print(f"❌ Error guardando funding events: {e}")
        import traceback

        traceback.print_exc()


# ========== 7️⃣ FUNCIONES DE DEBUG CLI ==========


def debug_fetch_closed_positions_raw():
    """
    Debug CLI: Imprime trades raw y posiciones reconstruidas.

    Uso:
        python -c "from adapters.pacifica import debug_fetch_closed_positions_raw; debug_fetch_closed_positions_raw()"
    """
    print("=" * 80)
    print("🌊 PACIFICA - DEBUG: TRADES RAW → CLOSED POSITIONS")
    print("=" * 80)

    # Fetch trades (últimos 7 días)
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - (7 * 24 * 60 * 60 * 1000)

    print("\n1️⃣ Fetching trades (últimos 7 días)...")
    trades = fetch_pacifica_trades_history(
        start_time=start_ms, end_time=now_ms, debug=True
    )

    if not trades:
        print("⚠️ No hay trades")
        return

    print(f"\n{'='*80}")
    print("📊 TRADES RAW (primeros 5):")
    print("=" * 80)
    for t in trades[:5]:
        print(json.dumps(t, indent=2))

    # Reconstruir posiciones
    print(f"\n{'='*80}")
    print("2️⃣ Reconstruyendo posiciones con FIFO...")
    print("=" * 80)

    positions = reconstruct_closed_positions_from_trades(trades, debug=True)

    print(f"\n{'='*80}")
    print("📈 POSICIONES CERRADAS RECONSTRUIDAS:")
    print("=" * 80)

    for pos in positions:
        print(f"\n{pos['symbol']} | {pos['side'].upper()}")
        print(f"  Size: {pos['size']:.4f}")
        print(f"  Entry: {pos['entry_price']:.2f}")
        print(f"  Close: {pos['close_price']:.2f}")
        print(f"  Open: {_fmt_time(pos['open_time'])}")
        print(f"  Close: {_fmt_time(pos['close_time'])}")
        print(f"  PnL (price): {pos['pnl']:.2f}")
        print(f"  Fees: {pos['fee_total']:.2f}")
        print(f"  Realized PnL: {pos['realized_pnl']:.2f}")
        print(f"  APR: {pos['apr']:.1f}%")


def main_fetch_open_positions_raw():
    """
    Debug CLI: Imprime open positions raw.

    Uso:
        python -c "from adapters.pacifica import main_fetch_open_positions_raw; main_fetch_open_positions_raw()"
    """
    print("=" * 80)
    print("🌊 PACIFICA - DEBUG: OPEN POSITIONS")
    print("=" * 80)

    positions = fetch_pacifica_open_positions(debug=True)

    print(f"\n{'='*80}")
    print("📊 RAW RESPONSE:")
    print("=" * 80)
    print(json.dumps(positions, indent=2, default=str))


def main_fetch_funding_raw():
    """
    Debug CLI: Imprime funding events raw.

    Uso:
        python -c "from adapters.pacifica import main_fetch_funding_raw; main_fetch_funding_raw()"
    """
    print("=" * 80)
    print("🌊 PACIFICA - DEBUG: FUNDING EVENTS")
    print("=" * 80)

    # Últimos 7 días
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - (7 * 24 * 60 * 60 * 1000)

    funding = fetch_pacifica_funding_fees(
        start_time=start_ms, end_time=now_ms, debug=True
    )

    print(f"\n{'='*80}")
    print("💸 FUNDING EVENTS (primeros 10):")
    print("=" * 80)

    for f in funding[:10]:
        print(f"{f['symbol']:15s} | {f['income']:10.4f} | {_fmt_time(f['timestamp'])}")


def debug_test_connection():
    """
    Test básico de conexión.

    Uso:
        python adapters/pacifica.py
    """
    print("=" * 80)
    print("🧪 PACIFICA - TEST DE CONEXIÓN")
    print("=" * 80)

    print(f"\n✅ Credenciales cargadas:")
    print(f"   Account: {PACIFICA_ACCOUNT}")

    print(f"\n🌐 Testing balances endpoint...")
    try:
        balances = fetch_pacifica_all_balances(debug=True)
        print(f"   ✅ Success!")
        print(f"   Balance: {balances.get('balance')}")
    except Exception as e:
        print(f"   ❌ Error: {e}")

    print(f"\n🌐 Testing positions endpoint...")
    try:
        positions = fetch_pacifica_open_positions(debug=True)
        print(f"   ✅ Success! {len(positions)} positions")
    except Exception as e:
        print(f"   ❌ Error: {e}")

    print("\n" + "=" * 80)
    print("✅ Test completado")
    print("=" * 80)


# ========== MAIN ==========

if __name__ == "__main__":
    # Si se ejecuta directamente, hacer test de conexión
    debug_test_connection()
