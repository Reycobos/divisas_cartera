"""
📦 EdgeX Exchange Adapter
========================
Adapter para futuros perpetuos de EdgeX (DEX basado en StarkEx L2).

Funcionalidades:
- fetch_edgex_all_balances() → balances de la cuenta
- fetch_edgex_open_positions() → posiciones abiertas (via WebSocket o REST)
- fetch_edgex_funding_fees() → historial de funding fees
- save_edgex_closed_positions() → reconstrucción FIFO desde trade fills

Autenticación:
- Usa ECDSA sobre Stark curve (similar a StarkEx)
- Headers: X-edgeX-Api-Signature, X-edgeX-Api-Timestamp

Documentación oficial: https://edgex-1.gitbook.io/edgeX-documentation/api
"""

from __future__ import annotations
import os
import time
import json
import hashlib
import hmac
import math
import re
import threading
import sqlite3
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from decimal import Decimal
from collections import defaultdict
from urllib.parse import urlencode, quote
import requests

# Intentar importar web3/eth para firma ECDSA (opcional - fallback a HMAC si no disponible)
try:
    from eth_account import Account
    from eth_account.messages import encode_defunct

    HAS_ETH_ACCOUNT = True
except ImportError:
    HAS_ETH_ACCOUNT = False

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# =========================
# Config & credenciales
# =========================
EDGEX_BASE_URL = os.getenv("EDGEX_BASE_URL", "https://pro.edgex.exchange")
EDGEX_WS_URL = os.getenv("EDGEX_WS_URL", "wss://quote.edgex.exchange")
EDGEX_API_KEY = os.getenv("EDGEX_API_KEY", "")  # L2 Private Key (hex)
EDGEX_ACCOUNT_ID = os.getenv("EDGEX_ACCOUNT_ID", "")  # Account ID numérico

__all__ = [
    "fetch_edgex_open_positions",
    "fetch_edgex_funding_fees",
    "fetch_edgex_all_balances",
    "save_edgex_closed_positions",
    "debug_fetch_open_positions_raw",
    "debug_fetch_closed_positions_raw",
    "debug_fetch_funding_raw",
    "_edgex_request",
]

# =========================
# Cache de precios mark
# =========================
_MARK_CACHE: Dict[str, Tuple[float, float]] = {}  # { "SYMBOL": (price, timestamp) }
_MARK_TTL = 5.0  # segundos

# Mapeo de contractId → símbolo (se llena dinámicamente)
_CONTRACT_MAP: Dict[str, str] = {}

# =========================
# Normalización de símbolo (Regla A)
# =========================
SPECIAL_SYMBOL_MAP = {
    # Añade alias especiales si son necesarios
}


def normalize_symbol(sym: str) -> str:
    """
    Normaliza el símbolo al formato base (ej: BTC-USD-PERP → BTC)
    """
    if not sym:
        return ""
    s = sym.upper().strip()
    # Quitar prefijos comunes
    s = re.sub(r"^PERP_", "", s)
    # Quitar sufijos: -USD, -USDT, -PERP, _USD, etc.
    s = re.sub(r"[-_]?(USD|USDT|USDC|PERP)$", "", s)
    s = re.sub(r"[-_]+$", "", s)
    # Tomar primera parte si hay separador
    base = re.split(r"[-_]", s)[0]
    # Aplicar alias especiales
    base = SPECIAL_SYMBOL_MAP.get(base, base)
    return base


# =========================
# Helpers generales
# =========================
def _now_ms() -> int:
    """Timestamp actual en milisegundos."""
    return int(time.time() * 1000)


def _has_creds() -> bool:
    """Verifica si hay credenciales configuradas."""
    return bool(EDGEX_API_KEY and EDGEX_ACCOUNT_ID)


def _safe_float(x, default=0.0) -> float:
    """Convierte a float de forma segura."""
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def _safe_int(x, default=0) -> int:
    """Convierte a int de forma segura."""
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return default


def _ts_iso(ms: int) -> str:
    """Convierte timestamp ms a ISO string legible."""
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S %Z"
        )
    except Exception:
        return str(ms)


# =========================
# Firma ECDSA para EdgeX
# =========================
def _build_sign_string(
    method: str, path: str, params: Optional[Dict[str, Any]], ts: str
) -> str:
    """
    Construye el string a firmar según la documentación de EdgeX:
    timestamp + METHOD + path + sorted_params_string

    Ejemplo:
    1735542383256GET/api/v1/private/account/getPositionTransactionPageaccountId=543429922991899150&filterTypeList=SETTLE_FUNDING_FEE&size=10
    """
    # Ordenar parámetros alfabéticamente
    param_str = ""
    if params:
        sorted_params = sorted(
            [(k, str(v)) for k, v in params.items() if v is not None],
            key=lambda x: x[0],
        )
        param_str = "&".join(f"{k}={v}" for k, v in sorted_params)

    # Concatenar: timestamp + METHOD + path + params
    sign_content = f"{ts}{method.upper()}{path}{param_str}"
    return sign_content


def _edgex_signature(sign_content: str, private_key: str) -> str:
    """
    Genera firma ECDSA usando la clave privada de EdgeX.

    EdgeX usa firmas ECDSA sobre Stark curve, pero para APIs de terceros
    usa un esquema más simple basado en SHA3/Keccak256 + ECDSA.
    """
    # Si tenemos eth_account, usamos firma ECDSA real
    if HAS_ETH_ACCOUNT and private_key.startswith("0x"):
        try:
            # Hash del mensaje
            msg_hash = hashlib.sha3_256(sign_content.encode("utf-8")).hexdigest()
            # Firmar con eth_account
            message = encode_defunct(text=msg_hash)
            signed = Account.sign_message(message, private_key=private_key)
            return signed.signature.hex()
        except Exception as e:
            print(f"⚠️ ECDSA sign failed, falling back to HMAC: {e}")

    # Fallback: HMAC-SHA256 (si el exchange lo acepta como alternativa)
    key_bytes = (
        private_key.encode("utf-8")
        if not private_key.startswith("0x")
        else bytes.fromhex(private_key[2:])
    )
    digest = hmac.new(
        key_bytes, sign_content.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return digest


def _headers(ts: str, signature: Optional[str]) -> Dict[str, str]:
    """Construye headers para la petición."""
    hdrs = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if signature:
        hdrs["X-edgeX-Api-Signature"] = signature
        hdrs["X-edgeX-Api-Timestamp"] = ts
    return hdrs


# =========================
# Cliente HTTP
# =========================
def _edgex_request(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    private: bool = False,
    timeout: int = 25,
    max_retries: int = 3,
    retry_backoff: float = 0.75,
) -> Dict[str, Any]:
    """
    Cliente HTTP con firma y retry automático.

    Args:
        method: GET/POST/DELETE
        path: Ruta del endpoint (ej: /api/v1/private/account/...)
        params: Parámetros de la petición
        private: Si requiere autenticación
        timeout: Timeout en segundos
        max_retries: Número máximo de reintentos
        retry_backoff: Factor de backoff entre reintentos

    Returns:
        Dict con la respuesta JSON
    """
    url = f"{EDGEX_BASE_URL}{path}"
    params = dict(params or {})

    # Añadir accountId si es privado y no está en params
    if private and "accountId" not in params and EDGEX_ACCOUNT_ID:
        params["accountId"] = EDGEX_ACCOUNT_ID

    # Generar firma para endpoints privados
    ts = str(_now_ms())
    headers = {}

    if private:
        sign_content = _build_sign_string(method.upper(), path, params, ts)
        signature = _edgex_signature(sign_content, EDGEX_API_KEY)
        headers = _headers(ts, signature)
    else:
        headers = _headers(ts, None)

    # Construir URL con query params para GET
    if method.upper() in ("GET", "DELETE") and params:
        qs = urlencode(params)
        url = f"{url}?{qs}"

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            if method.upper() == "GET":
                r = requests.get(url, headers=headers, timeout=timeout)
            elif method.upper() == "DELETE":
                r = requests.delete(url, headers=headers, timeout=timeout)
            else:  # POST
                r = requests.post(
                    url, headers=headers, json=params if params else {}, timeout=timeout
                )

            r.raise_for_status()
            data = r.json() if r.text else {}

            # Verificar código de error en respuesta
            if isinstance(data, dict):
                code = data.get("code", "SUCCESS")
                if code not in ("SUCCESS", "0", 0):
                    raise RuntimeError(
                        f"EdgeX error: code={code} msg={data.get('msg', data.get('message', 'Unknown'))}"
                    )

            return data

        except Exception as e:
            last_err = e
            if attempt >= max_retries:
                raise
            time.sleep(retry_backoff * attempt)

    raise last_err or RuntimeError("EdgeX request failed")


# =========================
# Obtener metadata de contratos
# =========================
def _fetch_contract_metadata() -> Dict[str, Any]:
    """
    Obtiene metadata de contratos disponibles.
    Esto incluye el mapeo contractId → symbol.
    """
    global _CONTRACT_MAP

    try:
        # Endpoint público de metadata
        resp = _edgex_request("GET", "/api/v1/public/meta", private=False, timeout=10)

        data = resp.get("data", {})
        contracts = data.get("contractList", data.get("contracts", []))

        for c in contracts:
            cid = str(c.get("contractId", c.get("id", "")))
            sym = c.get("contractName", c.get("symbol", ""))
            if cid and sym:
                _CONTRACT_MAP[cid] = sym

        return data
    except Exception as e:
        print(f"⚠️ Error fetching contract metadata: {e}")
        return {}


def _get_symbol_from_contract_id(contract_id: str) -> str:
    """Obtiene el símbolo desde el contractId."""
    if not _CONTRACT_MAP:
        _fetch_contract_metadata()
    return _CONTRACT_MAP.get(str(contract_id), contract_id)


# =========================
# BALANCES (fetch_edgex_all_balances)
# =========================
def fetch_edgex_all_balances() -> Dict[str, Any]:
    """
    Obtiene los balances de la cuenta de EdgeX.

    Mapeo a formato esperado por la UI:
    {
        "exchange": "edgex",
        "equity": float,           # Total equity
        "balance": float,          # Available balance
        "unrealized_pnl": float,   # PnL no realizado
        "initial_margin": float,   # Margen inicial usado
        "spot": 0.0,               # EdgeX solo tiene futuros
        "margin": 0.0,
        "futures": float           # Equity en futuros
    }
    """
    if not _has_creds():
        return {
            "exchange": "edgex",
            "equity": 0.0,
            "balance": 0.0,
            "unrealized_pnl": 0.0,
            "initial_margin": 0.0,
            "spot": 0.0,
            "margin": 0.0,
            "futures": 0.0,
        }

    try:
        # Endpoint para obtener colateral/balance
        resp = _edgex_request(
            "GET",
            "/api/v1/private/account/getCollateralById",
            params={"accountId": EDGEX_ACCOUNT_ID},
            private=True,
        )

        data = resp.get("data", {})

        # Campos esperados de EdgeX
        # totalEquity, availableBalance, usedMargin, unrealizedPnl, etc.

        # Intentar diferentes estructuras de respuesta
        if isinstance(data, list):
            # Si es lista, sumar todos los activos
            equity = sum(
                _safe_float(d.get("totalEquity", d.get("equity", 0))) for d in data
            )
            balance = sum(
                _safe_float(d.get("availableBalance", d.get("available", 0)))
                for d in data
            )
            unrealized = sum(
                _safe_float(d.get("unrealizedPnl", d.get("unrealized", 0)))
                for d in data
            )
            init_margin = sum(
                _safe_float(d.get("usedMargin", d.get("initialMargin", 0)))
                for d in data
            )
        else:
            # Estructura de objeto único
            equity = _safe_float(data.get("totalEquity", data.get("equity", 0)))
            balance = _safe_float(
                data.get("availableBalance", data.get("available", 0))
            )
            unrealized = _safe_float(
                data.get("unrealizedPnl", data.get("unrealized", 0))
            )
            init_margin = _safe_float(
                data.get("usedMargin", data.get("initialMargin", 0))
            )

            # Campos alternativos
            if equity == 0:
                equity = _safe_float(data.get("totalCollateralValue", 0))
            if balance == 0:
                balance = _safe_float(data.get("availableWithdrawAmount", 0))

        return {
            "exchange": "edgex",
            "equity": float(equity),
            "balance": float(balance),
            "unrealized_pnl": float(unrealized),
            "initial_margin": float(init_margin),
            "spot": 0.0,  # EdgeX solo tiene futuros perpetuos
            "margin": 0.0,
            "futures": float(equity),
        }

    except Exception as e:
        print(f"❌ EdgeX balances error: {e}")
        return {
            "exchange": "edgex",
            "equity": 0.0,
            "balance": 0.0,
            "unrealized_pnl": 0.0,
            "initial_margin": 0.0,
            "spot": 0.0,
            "margin": 0.0,
            "futures": 0.0,
        }


# =========================
# OPEN POSITIONS (fetch_edgex_open_positions)
# =========================
def _fetch_positions_rest() -> List[Dict[str, Any]]:
    """
    Obtiene posiciones abiertas via REST API.
    Fallback si WebSocket no está disponible.
    """
    try:
        resp = _edgex_request(
            "GET",
            "/api/v1/private/account/getPositionById",
            params={"accountId": EDGEX_ACCOUNT_ID},
            private=True,
        )

        data = resp.get("data", [])
        if isinstance(data, dict):
            data = data.get("positionList", [data])

        return data if isinstance(data, list) else []

    except Exception as e:
        print(f"❌ EdgeX REST positions error: {e}")
        return []


def fetch_edgex_open_positions(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Obtiene posiciones abiertas de EdgeX.

    Usa REST API como método principal.
    El WebSocket se puede usar para actualizaciones en tiempo real.

    Formato de salida (por posición):
    {
        "exchange": "edgex",
        "symbol": "<normalize_symbol>",
        "side": "long" | "short",
        "size": float,
        "entry_price": float,
        "mark_price": float,
        "liquidation_price": float,
        "notional": float,
        "unrealized_pnl": float,
        "fee": float,
        "funding_fee": float,
        "realized_pnl": float,
        "leverage": float
    }
    """
    if not _has_creds():
        return []

    try:
        positions = _fetch_positions_rest()
        out: List[Dict[str, Any]] = []

        for pos in positions:
            # Extraer campos de la posición
            contract_id = str(pos.get("contractId", ""))
            raw_sym = pos.get("symbol", _get_symbol_from_contract_id(contract_id))

            # Tamaño: positivo = long, negativo = short (o campo side separado)
            size_raw = _safe_float(pos.get("openSize", pos.get("size", 0)))
            side_field = pos.get("side", pos.get("positionSide", ""))

            if side_field:
                side = "long" if side_field.upper() in ("BUY", "LONG", "1") else "short"
                size = abs(size_raw)
            else:
                # Inferir lado del signo del tamaño
                side = "long" if size_raw >= 0 else "short"
                size = abs(size_raw)

            if size == 0:
                continue  # Saltar posiciones vacías

            # Precios
            entry = _safe_float(
                pos.get(
                    "avgEntryPrice", pos.get("openAvgPrice", pos.get("entryPrice", 0))
                )
            )
            mark = _safe_float(pos.get("markPrice", pos.get("fairPrice", entry)))
            liq = _safe_float(pos.get("liquidatePrice", pos.get("liquidationPrice", 0)))

            # PnL y margen
            unrealized = _safe_float(pos.get("unrealizedPnl", pos.get("unrealized", 0)))
            realized = _safe_float(pos.get("realizedPnl", pos.get("realized", 0)))
            funding = _safe_float(
                pos.get("sumFunding", pos.get("fundingFee", pos.get("holdFee", 0)))
            )
            fee = _safe_float(pos.get("sumFee", pos.get("fee", 0)))

            # Leverage y notional
            leverage = _safe_float(pos.get("leverage", pos.get("maxLeverage", 0)))
            notional = size * entry if entry else 0.0

            # Si no tenemos unrealized, calcularlo
            if unrealized == 0 and entry > 0 and mark > 0:
                if side == "short":
                    unrealized = (entry - mark) * size
                else:
                    unrealized = (mark - entry) * size

            # Filtrar por símbolo si se especificó
            norm_sym = normalize_symbol(raw_sym)
            if symbol and normalize_symbol(symbol) != norm_sym:
                continue

            out.append(
                {
                    "exchange": "edgex",
                    "symbol": norm_sym,
                    "side": side,
                    "size": abs(size),
                    "entry_price": entry,
                    "mark_price": mark,
                    "liquidation_price": liq,
                    "notional": notional,
                    "unrealized_pnl": unrealized,
                    "fee": fee,
                    "fees": fee,  # Alias
                    "funding_fee": funding,
                    "funding": funding,  # Alias
                    "realized_pnl": realized,
                    "leverage": leverage,
                }
            )

        return out

    except Exception as e:
        print(f"❌ EdgeX open positions error: {e}")
        return []


# =========================
# FUNDING FEES (fetch_edgex_funding_fees)
# =========================
def fetch_edgex_funding_fees(
    limit: int = 1000,
    symbol: Optional[str] = None,
    since: Optional[int] = None,
    max_pages: int = 50,
) -> List[Dict[str, Any]]:
    """
    Obtiene historial de funding fees de EdgeX.

    Usa el endpoint getPositionTransactionPage con filterTypeList=SETTLE_FUNDING_FEE

    Formato de salida:
    {
        "exchange": "edgex",
        "symbol": "<normalize_symbol>",
        "income": float,        # Monto del funding (+ cobro, - pago)
        "asset": "USDT",
        "timestamp": int (ms),
        "funding_rate": float,
        "type": "FUNDING_FEE"
    }
    """
    if not _has_creds():
        return []

    acc: List[Dict[str, Any]] = []
    cutoff = int(since) if since else 0
    offset_data = ""
    page_size = 100  # Máximo por página

    try:
        for page in range(max_pages):
            params = {
                "accountId": EDGEX_ACCOUNT_ID,
                "size": str(page_size),
                "filterTypeList": "SETTLE_FUNDING_FEE",
            }
            if offset_data:
                params["offsetData"] = offset_data
            if symbol:
                # EdgeX puede filtrar por contractId
                params["filterContractIdList"] = symbol

            resp = _edgex_request(
                "GET",
                "/api/v1/private/account/getPositionTransactionPage",
                params=params,
                private=True,
            )

            data = resp.get("data", {})
            rows = data.get("dataList", [])

            if not rows:
                break

            for row in rows:
                ts = _safe_int(row.get("createdTime", row.get("settleTime", 0)))

                # Saltar si es anterior al cutoff
                if cutoff and ts < cutoff:
                    continue

                contract_id = str(row.get("contractId", ""))
                raw_sym = row.get("symbol", _get_symbol_from_contract_id(contract_id))

                # El funding puede estar en 'deltaFunding', 'funding', 'income', etc.
                income = _safe_float(
                    row.get(
                        "deltaFunding",
                        row.get("funding", row.get("income", row.get("amount", 0))),
                    )
                )

                rate = _safe_float(row.get("fundingRate", row.get("rate", 0)))

                # Generar ID único si no existe
                ext_id = row.get("id", row.get("transactionId", ""))
                if not ext_id:
                    ext_id = hashlib.md5(
                        f"{contract_id}-{ts}-{income}".encode()
                    ).hexdigest()[:16]

                acc.append(
                    {
                        "exchange": "edgex",
                        "symbol": normalize_symbol(raw_sym),
                        "income": income,
                        "asset": "USDT",
                        "timestamp": ts,
                        "funding_rate": rate,
                        "type": "FUNDING_FEE",
                        "external_id": str(ext_id),
                    }
                )

                if len(acc) >= limit:
                    return acc

            # Paginación
            offset_data = data.get("nextPageOffsetData", "")
            if not offset_data:
                break

            # Pequeño delay para rate limiting
            time.sleep(0.1)

        return acc

    except Exception as e:
        print(f"❌ EdgeX funding fees error: {e}")
        return acc


# =========================
# TRADE FILLS para FIFO
# =========================
def _fetch_trade_fills(
    days: int = 60,
    symbol: Optional[str] = None,
    max_pages: int = 100,
) -> List[Dict[str, Any]]:
    """
    Obtiene historial de trade fills para reconstrucción FIFO.

    Usa el endpoint getHistoryOrderFillTransactionPage.
    """
    fills: List[Dict[str, Any]] = []
    cutoff_ms = _now_ms() - days * 24 * 60 * 60 * 1000 if days > 0 else 0
    offset_data = ""
    page_size = 100

    try:
        for page in range(max_pages):
            params = {
                "accountId": EDGEX_ACCOUNT_ID,
                "size": str(page_size),
            }
            if offset_data:
                params["offsetData"] = offset_data
            if symbol:
                params["filterContractIdList"] = symbol
            if cutoff_ms > 0:
                params["filterStartCreatedTimeInclusive"] = str(cutoff_ms)

            resp = _edgex_request(
                "GET",
                "/api/v1/private/order/getHistoryOrderFillTransactionPage",
                params=params,
                private=True,
            )

            data = resp.get("data", {})
            rows = data.get("dataList", [])

            if not rows:
                break

            for row in rows:
                fills.append(row)

            offset_data = data.get("nextPageOffsetData", "")
            if not offset_data:
                break

            time.sleep(0.1)

        return fills

    except Exception as e:
        print(f"❌ EdgeX trade fills error: {e}")
        return fills


# =========================
# FIFO Reconstruction
# =========================
def _reconstruct_closed_positions_fifo(
    fills: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Reconstruye posiciones cerradas usando FIFO (First-In-First-Out).

    Lógica FIFO:
    1. Agrupa fills por símbolo
    2. Mantiene una cola de posiciones abiertas (open_queue)
    3. Cuando un fill cierra posición (lado opuesto), empareja con FIFO
    4. Calcula PnL de cada posición cerrada

    Returns:
        Lista de posiciones cerradas con métricas calculadas
    """
    closed_positions: List[Dict[str, Any]] = []

    # Agrupar fills por contractId/symbol y ordenar por tiempo
    fills_by_contract: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for fill in fills:
        contract_id = str(fill.get("contractId", ""))
        fills_by_contract[contract_id].append(fill)

    for contract_id, contract_fills in fills_by_contract.items():
        # Ordenar por tiempo ascendente
        contract_fills.sort(
            key=lambda x: _safe_int(x.get("createdTime", x.get("matchTime", 0)))
        )

        # Cola FIFO: [(size, entry_price, open_time, side, fees)]
        open_queue: List[Dict[str, Any]] = []

        symbol = _get_symbol_from_contract_id(contract_id)

        for fill in contract_fills:
            fill_side = fill.get("orderSide", "").upper()  # BUY o SELL
            fill_size = abs(_safe_float(fill.get("fillSize", 0)))
            fill_price = _safe_float(fill.get("fillPrice", 0))
            fill_time = _safe_int(fill.get("createdTime", fill.get("matchTime", 0)))
            fill_fee = _safe_float(fill.get("fillFee", 0))
            fill_realized = _safe_float(fill.get("realizePnl", 0))

            if fill_size == 0:
                continue

            # Determinar si abre o cierra posición
            # BUY abre LONG o cierra SHORT
            # SELL abre SHORT o cierra LONG

            remaining_size = fill_size

            # Intentar cerrar posiciones existentes con lado opuesto
            new_queue: List[Dict[str, Any]] = []

            for open_pos in open_queue:
                if remaining_size <= 0:
                    new_queue.append(open_pos)
                    continue

                # Verificar si es cierre (lados opuestos)
                is_close = (fill_side == "BUY" and open_pos["side"] == "short") or (
                    fill_side == "SELL" and open_pos["side"] == "long"
                )

                if not is_close:
                    new_queue.append(open_pos)
                    continue

                # Calcular cuánto se cierra
                close_size = min(remaining_size, open_pos["remaining"])

                if close_size > 0:
                    # Calcular PnL de esta porción
                    entry_price = open_pos["entry_price"]
                    close_price = fill_price

                    if open_pos["side"] == "short":
                        price_pnl = (entry_price - close_price) * close_size
                    else:  # long
                        price_pnl = (close_price - entry_price) * close_size

                    # Prorratear fees
                    entry_fee = (
                        open_pos["fee"] * (close_size / open_pos["original_size"])
                        if open_pos["original_size"] > 0
                        else 0
                    )
                    exit_fee = (
                        fill_fee * (close_size / fill_size) if fill_size > 0 else 0
                    )
                    total_fee = entry_fee + exit_fee

                    # Crear posición cerrada
                    closed = {
                        "exchange": "edgex",
                        "symbol": normalize_symbol(symbol),
                        "side": open_pos["side"],
                        "size": float(close_size),
                        "entry_price": float(entry_price),
                        "close_price": float(close_price),
                        "open_time": int(
                            open_pos["open_time"] / 1000
                        ),  # Convertir a segundos
                        "close_time": int(fill_time / 1000),
                        "pnl": float(price_pnl),
                        "fee_total": float(total_fee),
                        "funding_total": 0.0,  # Se actualiza después si está disponible
                        "realized_pnl": float(price_pnl - total_fee),  # Neto
                        "notional": float(close_size * entry_price),
                        "leverage": open_pos.get("leverage"),
                        "liquidation_price": None,
                    }
                    closed_positions.append(closed)

                    remaining_size -= close_size
                    open_pos["remaining"] -= close_size

                # Si queda algo en la posición abierta, mantenerla
                if open_pos["remaining"] > 0:
                    new_queue.append(open_pos)

            open_queue = new_queue

            # Si queda tamaño del fill, abre nueva posición
            if remaining_size > 0:
                new_side = "long" if fill_side == "BUY" else "short"
                open_queue.append(
                    {
                        "side": new_side,
                        "original_size": remaining_size,
                        "remaining": remaining_size,
                        "entry_price": fill_price,
                        "open_time": fill_time,
                        "fee": (
                            fill_fee * (remaining_size / fill_size)
                            if fill_size > 0
                            else 0
                        ),
                        "leverage": None,  # No disponible en fill individual
                    }
                )

    return closed_positions


# =========================
# CLOSED POSITIONS (save_edgex_closed_positions)
# =========================
def save_edgex_closed_positions(
    db_path: str = "portfolio.db",
    days: int = 60,
    symbol: Optional[str] = None,
    debug: bool = True,
) -> int:
    """
    Obtiene y guarda posiciones cerradas de EdgeX usando reconstrucción FIFO.

    Args:
        db_path: Ruta a la base de datos SQLite
        days: Días hacia atrás para buscar fills
        symbol: Filtrar por símbolo específico (opcional)
        debug: Imprimir información de debug

    Returns:
        Número de posiciones guardadas
    """
    if not _has_creds():
        if debug:
            print("⚠️ No hay credenciales EdgeX configuradas")
        return 0

    try:
        # Importar db_manager para guardar
        from db_manager import save_closed_position

        # 1. Obtener trade fills
        if debug:
            print(f"📥 Obteniendo trade fills de EdgeX (últimos {days} días)...")

        fills = _fetch_trade_fills(days=days, symbol=symbol)

        if not fills:
            if debug:
                print("⚠️ No se encontraron trade fills en EdgeX")
            return 0

        if debug:
            print(f"   → {len(fills)} fills obtenidos")

        # 2. Reconstruir posiciones cerradas con FIFO
        if debug:
            print("🔧 Reconstruyendo posiciones cerradas con FIFO...")

        closed = _reconstruct_closed_positions_fifo(fills)

        if not closed:
            if debug:
                print("⚠️ No se generaron posiciones cerradas")
            return 0

        if debug:
            print(f"   → {len(closed)} posiciones cerradas reconstruidas")

        # 3. Verificar DB
        if not os.path.exists(db_path):
            print(f"❌ Database not found: {db_path}")
            return 0

        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        saved = 0
        skipped = 0

        # 4. Guardar cada posición
        for pos in closed:
            try:
                # Verificar si ya existe (deduplicación)
                cur.execute(
                    """
                    SELECT id FROM closed_positions
                    WHERE exchange = ? AND symbol = ? AND close_time = ? AND size = ?
                    """,
                    (pos["exchange"], pos["symbol"], pos["close_time"], pos["size"]),
                )

                if cur.fetchone():
                    skipped += 1
                    continue

                # Guardar usando db_manager (recalcula métricas)
                save_closed_position(pos)
                saved += 1

                if debug:
                    print(
                        f"✅ EdgeX cerrada: {pos['symbol']} {pos['side']} "
                        f"size={pos['size']:.6f} pnl={pos['pnl']:.2f} "
                        f"close_time={pos['close_time']}"
                    )

            except Exception as e:
                print(f"❌ Error guardando posición EdgeX: {e}")
                continue

        conn.close()

        if debug:
            print(f"✅ EdgeX guardadas: {saved} | omitidas (duplicadas): {skipped}")

        return saved

    except ImportError:
        print("❌ No se pudo importar db_manager")
        return 0
    except Exception as e:
        print(f"❌ EdgeX closed positions error: {e}")
        return 0


# =========================
# DEBUG FUNCTIONS
# =========================
def debug_fetch_open_positions_raw(limit: int = 10) -> None:
    """
    Imprime respuesta cruda del endpoint de posiciones abiertas.
    Útil para debugging sin depender del frontend.
    """
    print("=" * 60)
    print("DEBUG: EdgeX Open Positions (RAW)")
    print("=" * 60)

    if not _has_creds():
        print("⚠️ No hay credenciales EdgeX configuradas")
        print("   Configura EDGEX_API_KEY y EDGEX_ACCOUNT_ID en .env")
        return

    try:
        resp = _edgex_request(
            "GET",
            "/api/v1/private/account/getPositionById",
            params={"accountId": EDGEX_ACCOUNT_ID},
            private=True,
        )

        print(f"\n📦 Response code: {resp.get('code', 'N/A')}")
        print(f"📦 Response keys: {list(resp.keys())}")

        data = resp.get("data", {})
        if isinstance(data, dict):
            positions = data.get("positionList", [data])
        else:
            positions = data if isinstance(data, list) else []

        print(f"\n📊 Total posiciones: {len(positions)}")

        for i, pos in enumerate(positions[:limit]):
            print(f"\n--- Posición {i+1} ---")
            print(json.dumps(pos, indent=2, ensure_ascii=False, default=str))

    except Exception as e:
        print(f"❌ Error: {e}")

    print("\n" + "=" * 60)


def debug_fetch_closed_positions_raw(days: int = 30, limit: int = 10) -> None:
    """
    Imprime trade fills crudos para debugging de reconstrucción FIFO.
    """
    print("=" * 60)
    print(f"DEBUG: EdgeX Trade Fills (RAW) - últimos {days} días")
    print("=" * 60)

    if not _has_creds():
        print("⚠️ No hay credenciales EdgeX configuradas")
        return

    try:
        fills = _fetch_trade_fills(days=days, max_pages=5)

        print(f"\n📊 Total fills: {len(fills)}")

        for i, fill in enumerate(fills[:limit]):
            print(f"\n--- Fill {i+1} ---")
            relevant = {
                "id": fill.get("id"),
                "contractId": fill.get("contractId"),
                "orderSide": fill.get("orderSide"),
                "fillSize": fill.get("fillSize"),
                "fillPrice": fill.get("fillPrice"),
                "fillFee": fill.get("fillFee"),
                "realizePnl": fill.get("realizePnl"),
                "createdTime": fill.get("createdTime"),
                "matchTime": fill.get("matchTime"),
            }
            print(json.dumps(relevant, indent=2, ensure_ascii=False))

        # Mostrar reconstrucción FIFO
        print("\n" + "-" * 40)
        print("FIFO Reconstruction:")
        closed = _reconstruct_closed_positions_fifo(fills)
        print(f"Posiciones cerradas reconstruidas: {len(closed)}")

        for i, pos in enumerate(closed[:5]):
            print(f"\n--- Closed {i+1} ---")
            print(json.dumps(pos, indent=2, ensure_ascii=False, default=str))

    except Exception as e:
        print(f"❌ Error: {e}")

    print("\n" + "=" * 60)


def debug_fetch_funding_raw(days: int = 7, limit: int = 20) -> None:
    """
    Imprime funding fees crudos para debugging.
    """
    print("=" * 60)
    print(f"DEBUG: EdgeX Funding Fees (RAW) - últimos {days} días")
    print("=" * 60)

    if not _has_creds():
        print("⚠️ No hay credenciales EdgeX configuradas")
        return

    try:
        since = _now_ms() - days * 24 * 60 * 60 * 1000
        fees = fetch_edgex_funding_fees(limit=limit, since=since)

        print(f"\n📊 Total funding events: {len(fees)}")

        total_income = sum(f.get("income", 0) for f in fees)
        print(f"💰 Total income: {total_income:.4f} USDT")

        for i, fee in enumerate(fees[:limit]):
            ts = fee.get("timestamp", 0)
            print(
                f"  {i+1}. {fee['symbol']:<10} "
                f"income={fee['income']:>10.4f} "
                f"rate={fee.get('funding_rate', 0):>10.6f} "
                f"time={_ts_iso(ts)}"
            )

    except Exception as e:
        print(f"❌ Error: {e}")

    print("\n" + "=" * 60)


# =========================
# CLI Main
# =========================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="EdgeX Exchange Adapter CLI")
    parser.add_argument("--balances", action="store_true", help="Fetch balances")
    parser.add_argument("--positions", action="store_true", help="Fetch open positions")
    parser.add_argument("--funding", action="store_true", help="Fetch funding fees")
    parser.add_argument(
        "--closed", action="store_true", help="Save closed positions (FIFO)"
    )
    parser.add_argument(
        "--debug-positions", action="store_true", help="Debug open positions raw"
    )
    parser.add_argument(
        "--debug-fills", action="store_true", help="Debug trade fills raw"
    )
    parser.add_argument(
        "--debug-funding", action="store_true", help="Debug funding raw"
    )
    parser.add_argument("--days", type=int, default=30, help="Days for history")
    parser.add_argument("--symbol", type=str, default=None, help="Filter by symbol")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to DB")

    args = parser.parse_args()

    print("🔌 EdgeX Adapter CLI")
    print(f"   Base URL: {EDGEX_BASE_URL}")
    print(
        f"   Account ID: {EDGEX_ACCOUNT_ID[:8]}..."
        if EDGEX_ACCOUNT_ID
        else "   Account ID: Not set"
    )
    print(f"   API Key: {'***' if EDGEX_API_KEY else 'Not set'}")
    print()

    if args.debug_positions:
        debug_fetch_open_positions_raw()

    if args.debug_fills:
        debug_fetch_closed_positions_raw(days=args.days)

    if args.debug_funding:
        debug_fetch_funding_raw(days=args.days)

    if args.balances:
        print("== Balances ==")
        print(json.dumps(fetch_edgex_all_balances(), indent=2))

    if args.positions:
        print("== Open Positions ==")
        positions = fetch_edgex_open_positions(symbol=args.symbol)
        for p in positions:
            print(json.dumps(p, indent=2))

    if args.funding:
        print(f"== Funding Fees (last {args.days} days) ==")
        fees = fetch_edgex_funding_fees(
            limit=100,
            symbol=args.symbol,
            since=_now_ms() - args.days * 24 * 60 * 60 * 1000,
        )
        print(f"Total: {len(fees)} events")
        for f in fees[:10]:
            print(f"  {f['symbol']}: {f['income']:.4f} @ {_ts_iso(f['timestamp'])}")

    if args.closed and not args.dry_run:
        print(f"== Saving Closed Positions (FIFO, {args.days} days) ==")
        n = save_edgex_closed_positions(days=args.days, symbol=args.symbol, debug=True)
        print(f"Saved: {n}")
    elif args.closed:
        print("== Dry run - not saving ==")
        fills = _fetch_trade_fills(days=args.days, symbol=args.symbol)
        closed = _reconstruct_closed_positions_fifo(fills)
        print(f"Would save {len(closed)} positions")
