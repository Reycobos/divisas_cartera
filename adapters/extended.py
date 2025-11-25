from flask import Flask, render_template, jsonify
import pandas as pd
import requests
import time
import hashlib
from dotenv import load_dotenv
import hmac
import os
import json
import datetime
import math
from datetime import datetime, timezone
import json, urllib
import sqlite3
from db_manager import save_closed_position


EXTENDED_OPEN_VERBOSE = os.getenv("EXTENDED_OPEN_VERBOSE", "0") == "1"


def _ext_debug(msg: str) -> None:
    if EXTENDED_OPEN_VERBOSE:
        print(msg)


# Soporte para múltiples subcuentas Extended
# Formato env vars: EXT_API_KEY_1, EXT_API_SECRET_1, EXT_API_KEY_2, EXT_API_SECRET_2, etc.
def _get_extended_accounts():
    """Detecta todas las subcuentas Extended configuradas en .env"""
    accounts = []

    # Intenta cargar cuenta principal (sin sufijo)
    main_key = os.getenv("EXT_API_KEY")
    main_secret = os.getenv("EXT_API_SECRET")
    if main_key and main_secret:
        accounts.append(
            {"api_key": main_key, "api_secret": main_secret, "label": "main"}
        )

    # Intenta cargar subcuentas numeradas (EXT_API_KEY_1, EXT_API_KEY_2, etc.)
    for i in range(1, 11):  # Extended permite hasta 10 subcuentas
        key = os.getenv(f"EXT_API_KEY_{i}")
        secret = os.getenv(f"EXT_API_SECRET_{i}")
        if key and secret:
            accounts.append({"api_key": key, "api_secret": secret, "label": f"sub{i}"})

    return accounts


EXTENDED_ACCOUNTS = _get_extended_accounts()
EXT_BASE_URL = os.getenv("EXT_BASE_URL", "https://api.starknet.extended.exchange")

if not EXTENDED_ACCOUNTS:
    print(
        "⚠️ WARNING: No Extended API keys found. Please configure EXT_API_KEY/EXT_API_SECRET or EXT_API_KEY_1/EXT_API_SECRET_1, etc."
    )
else:
    print(f"✅ Extended: {len(EXTENDED_ACCOUNTS)} subcuenta(s) configurada(s)")


# ------------- Extendedconfig------------
def extended_get(path: str, params=None, api_key=None, api_secret=None):
    """Función helper para Extended API (soporta múltiples subcuentas)"""
    if not api_key or not api_secret:
        # Fallback a primera cuenta si no se especifica
        if not EXTENDED_ACCOUNTS:
            print("⚠️ No Extended credentials configured")
            return {"data": {}}
        api_key = EXTENDED_ACCOUNTS[0]["api_key"]
        api_secret = EXTENDED_ACCOUNTS[0]["api_secret"]

    try:
        url = f"{EXT_BASE_URL}/api/v1{path}"
        timestamp = str(int(time.time() * 1000))

        message = timestamp + "GET" + f"/api/v1{path}" + ""
        signature = hmac.new(
            api_secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        headers = {
            "X-API-KEY": api_key,
            "X-TIMESTAMP": timestamp,
            "X-SIGNATURE": signature,
            "Content-Type": "application/json",
        }

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error en extended_get: {e}")
        return {"data": {}}


def fetch_account_extended():
    """Obtener balance de TODAS las subcuentas Extended y consolidar"""
    if not EXTENDED_ACCOUNTS:
        return None

    total_equity = 0
    total_balance = 0
    total_unrealized_pnl = 0
    total_initial_margin = 0

    for account in EXTENDED_ACCOUNTS:
        try:
            data = extended_get(
                "/user/balance",
                api_key=account["api_key"],
                api_secret=account["api_secret"],
            )
            raw = data.get("data") or {}

            total_equity += float(raw.get("equity", 0))
            total_balance += float(raw.get("balance", 0))
            total_unrealized_pnl += float(raw.get("unrealisedPnl", 0))
            total_initial_margin += float(raw.get("initialMargin", 0))

            _ext_debug(
                f"📊 Extended [{account['label']}]: equity={raw.get('equity', 0)} balance={raw.get('balance', 0)}"
            )

        except Exception as e:
            print(f"⚠️ Error Extended [{account['label']}]: {e}")
            continue

    return {
        "exchange": "extended",
        "equity": total_equity,
        "balance": total_balance,
        "unrealized_pnl": total_unrealized_pnl,
        "initial_margin": total_initial_margin,
    }


def fetch_open_extended_positions():
    """
    Obtener posiciones abiertas de TODAS las subcuentas Extended
    GET /api/v1/user/positions
    """
    if not EXTENDED_ACCOUNTS:
        return []

    all_positions = []

    for account in EXTENDED_ACCOUNTS:
        try:
            _ext_debug(
                f"🔍 DEBUG: Obteniendo posiciones de Extended [{account['label']}]..."
            )

            data = extended_get(
                "/user/positions",
                api_key=account["api_key"],
                api_secret=account["api_secret"],
            )
            positions = data.get("data", [])

            _ext_debug(
                f"📦 DEBUG [{account['label']}]: {len(positions)} posiciones abiertas"
            )

            # 🧾 RAW dump (máx. 3 elementos)
            if EXTENDED_OPEN_VERBOSE:
                try:
                    for i, pos in enumerate(positions[:3]):
                        try:
                            _ext_debug(
                                f"   🧾 RAW[{i}]: {json.dumps(pos, ensure_ascii=False)}"
                            )
                        except Exception:
                            _ext_debug(f"   🧾 RAW[{i}]: {pos}")
                except Exception as e:
                    _ext_debug(f"   ⚠️ DEBUG RAW dump error: {e}")

            for pos in positions:
                try:
                    market = pos.get("market", "")
                    # Normalizar símbolo (quitar -USD, -PERP, etc.)
                    symbol = market.replace("-USD", "").replace("-PERP", "").upper()

                    side = (pos.get("side") or "").lower()
                    size = float(pos.get("size", 0))
                    entry_price = float(pos.get("openPrice", 0))
                    mark_price = float(pos.get("markPrice", 0))
                    liquidation_price = float(pos.get("liquidationPrice", 0))
                    notional = float(pos.get("value", 0))
                    unrealized_pnl = float(pos.get("unrealisedPnl", 0))
                    realized_pnl = float(pos.get("realisedPnl", 0))
                    leverage = float(pos.get("leverage", 1))
                    margin = float(pos.get("margin", 0))

                    # ✅ ESQUEMA EXACTO REQUERIDO
                    formatted_pos = {
                        "exchange": "extended",  # Normalizado para consistencia
                        "symbol": symbol,
                        "side": side,
                        "size": abs(size),  # Siempre positivo
                        "quantity": abs(size),  # Compatibilidad
                        "entry_price": entry_price,
                        "mark_price": mark_price,
                        "liquidation_price": liquidation_price,
                        "notional": notional,
                        "unrealized_pnl": unrealized_pnl,
                        "realized_pnl": realized_pnl,
                        "leverage": leverage,
                        "initial_margin": margin,
                        "funding_fee": 0,  # No disponible en este endpoint
                        "fee": 0,  # No disponible en este endpoint
                    }

                    all_positions.append(formatted_pos)

                    # 🧪 Línea de chequeo estilo Gate/Aden
                    _ext_debug(
                        f"🧪 EXTENDED [{account['label']}] {symbol} side={side} entry={entry_price} mark={mark_price} liq={liquidation_price} → exchange='extended'"
                    )

                except Exception as e:
                    print(
                        f"⚠️ Error procesando posición Extended [{account['label']}]: {e}"
                    )
                    continue

        except Exception as e:
            print(
                f"❌ Error obteniendo posiciones de Extended [{account['label']}]: {e}"
            )
            continue

    _ext_debug(
        f"✅ DEBUG: {len(all_positions)} posiciones abiertas totales (todas las subcuentas)"
    )
    return all_positions


def fetch_funding_extended(
    limit=1000, start_time=None, debug=False, api_key=None, api_secret=None
):
    """
    Obtiene funding payments de Extended para una subcuenta específica.
    GET /api/v1/user/funding/history
    """
    try:
        if start_time is None:
            # Por defecto, últimos 90 días
            start_time = int(time.time() * 1000) - (90 * 24 * 60 * 60 * 1000)

        all_funding = []
        cursor = None

        while True:
            params = {
                "fromTime": start_time,
                "limit": min(limit, 100),  # Máximo por página
            }

            if cursor:
                params["cursor"] = cursor

            data = extended_get(
                "/user/funding/history", params, api_key=api_key, api_secret=api_secret
            )

            if not data or data.get("status") != "OK":
                if debug:
                    print(f"[Extended Funding] Error en respuesta: {data}")
                break

            funding_payments = data.get("data", [])
            if not funding_payments:
                if debug:
                    print("[Extended Funding] No hay más registros de funding")
                break

            all_funding.extend(funding_payments)

            # if debug:
            #     print(f"[Extended Funding] Página: {len(funding_payments)} registros")

            # Paginación
            pagination = data.get("pagination", {})
            next_cursor = pagination.get("cursor")

            if not next_cursor or len(funding_payments) < params["limit"]:
                break

            cursor = next_cursor
            time.sleep(0.1)  # Rate limiting

        # Procesar y normalizar funding payments
        results = []
        for funding in all_funding:
            try:
                market = funding.get("market", "")
                # Normalizar símbolo (quitar -USD, -PERP, etc.)
                symbol = market.replace("-USD", "").replace("-PERP", "").upper()

                funding_fee = float(funding.get("fundingFee", 0))
                funding_rate = float(funding.get("fundingRate", 0))
                paid_time = funding.get("paidTime")
                position_id = funding.get("positionId")

                results.append(
                    {
                        "exchange": "extended",
                        "symbol": symbol,
                        "income": funding_fee,
                        "asset": "USD",  # Asumimos USD para Extended
                        "timestamp": paid_time,
                        "funding_rate": funding_rate,
                        "type": "FUNDING_FEE",
                        "position_id": position_id,
                        "side": funding.get("side", "").lower(),
                        "size": float(funding.get("size", 0)),
                        "value": float(funding.get("value", 0)),
                        "mark_price": float(funding.get("markPrice", 0)),
                    }
                )

                if debug:
                    print(
                        f"💰 [Extended Funding] {symbol} fee={funding_fee:.6f} "
                        f"rate={funding_rate:.6f} time={paid_time}"
                    )

            except Exception as e:
                if debug:
                    print(f"[WARN] Error procesando funding Extended: {e}")
                continue

        if debug:
            print(f"✅ Extended funding: {len(results)} payments encontrados")

        return results

    except Exception as e:
        print(f"❌ Error fetching Extended funding: {e}")
        return []


def fetch_closed_positions_extended(limit=1000, debug=False):
    """
    Obtiene posiciones cerradas de TODAS las subcuentas Extended y asocia funding payments.
    """
    if not EXTENDED_ACCOUNTS:
        return []

    all_results = []

    for account in EXTENDED_ACCOUNTS:
        try:
            now = int(time.time() * 1000)
            start_time = now - (90 * 24 * 60 * 60 * 1000)

            # Obtener funding payments primero para esta subcuenta
            funding_payments = fetch_funding_extended(
                limit=limit,
                start_time=start_time,
                debug=debug,
                api_key=account["api_key"],
                api_secret=account["api_secret"],
            )

            # Crear índice de funding por position_id
            funding_by_position = {}
            for funding in funding_payments:
                position_id = funding.get("position_id")
                if position_id:
                    if position_id not in funding_by_position:
                        funding_by_position[position_id] = []
                    funding_by_position[position_id].append(funding)

            # Obtener posiciones cerradas de esta subcuenta
            positions_this_account = []
            cursor = None

            while True:
                params = {
                    "limit": min(limit, 100),
                }

                if cursor:
                    params["cursor"] = cursor

                data = extended_get(
                    "/user/positions/history",
                    params,
                    api_key=account["api_key"],
                    api_secret=account["api_secret"],
                )

                if not data or data.get("status") != "OK":
                    break

                positions = data.get("data", [])
                if not positions:
                    break

                # Filtrar solo posiciones cerradas (tienen closedTime)
                closed_positions = [p for p in positions if p.get("closedTime")]
                positions_this_account.extend(closed_positions)

                # Paginación
                pagination = data.get("pagination", {})
                next_cursor = pagination.get("cursor")

                if not next_cursor or len(positions) < params["limit"]:
                    break

                cursor = next_cursor
                time.sleep(0.1)

            # Procesar y normalizar las posiciones con funding de esta subcuenta
            for pos in positions_this_account:
                try:
                    market = pos.get("market", "")
                    symbol = market.replace("-USD", "").replace("-PERP", "").upper()

                    side = (pos.get("side") or "").lower()
                    size = float(pos.get("maxPositionSize", 0))
                    entry_price = float(pos.get("openPrice", 0))
                    close_price = float(pos.get("exitPrice", 0))
                    realized_pnl = float(pos.get("realisedPnl", 0))
                    leverage = float(pos.get("leverage", 1))
                    position_id = pos.get("id")

                    open_time = pos.get("createdTime")
                    close_time = pos.get("closedTime")

                    # Calcular funding total para esta posición
                    position_funding = funding_by_position.get(position_id, [])
                    funding_total = sum(f["income"] for f in position_funding)
                    # Calcular fees: realized_pnl = PnL_precio + funding + fees
                    # Por lo tanto: fees = realized_pnl - PnL_precio - funding
                    if side == "long":
                        pnl_price_only = (close_price - entry_price) * size
                    else:  # short
                        pnl_price_only = (entry_price - close_price) * size

                    fees_approx = realized_pnl - pnl_price_only - funding_total

                    all_results.append(
                        {
                            "exchange": "extended",  # Normalizado para consistencia
                            "symbol": symbol,
                            "side": side,
                            "size": size,
                            "entry_price": entry_price,
                            "close_price": close_price,
                            "notional": entry_price * size,
                            "fees": fees_approx,
                            "funding_fee": funding_total,
                            "realized_pnl": realized_pnl,
                            "pnl": pnl_price_only,
                            "open_time": int(open_time / 1000) if open_time else None,
                            "close_time": (
                                int(close_time / 1000) if close_time else None
                            ),
                            "leverage": leverage,
                            "liquidation_price": None,
                            "exit_type": pos.get("exitType", ""),
                            "position_id": position_id,
                            "funding_payments": position_funding,  # Para debug
                        }
                    )

                    if debug:
                        print(
                            f"✅ [Extended {account['label']}] {symbol} {side} size={size:.4f} "
                            f"entry={entry_price:.4f} close={close_price:.4f} "
                            f"realized={realized_pnl:.4f} funding={funding_total:.4f} "
                            f"({len(position_funding)} payments)"
                        )

                except Exception as e:
                    if debug:
                        print(
                            f"[WARN] Error procesando posición Extended [{account['label']}]: {e}"
                        )
                    continue

        except Exception as e:
            print(
                f"❌ Error fetching Extended closed positions [{account['label']}]: {e}"
            )
            continue

    if debug:
        print(
            f"✅ Extended closed positions: {len(all_results)} con funding asociado (todas las subcuentas)"
        )

    return all_results


def save_extended_closed_positions(db_path="portfolio.db", debug=False):
    """
    Guarda posiciones cerradas de Extended en SQLite con funding asociado.
    """
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return

    closed_positions = fetch_closed_positions_extended(debug=debug)
    if not closed_positions:
        print("⚠️ No closed positions returned from Extended.")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    saved = 0
    skipped = 0

    for pos in closed_positions:
        try:
            # deduplicación por (exchange, symbol, close_time)
            cur.execute(
                """
                SELECT COUNT(*) FROM closed_positions
                WHERE exchange = ? AND symbol = ? AND close_time = ?
            """,
                (pos["exchange"], pos["symbol"], pos["close_time"]),
            )

            if cur.fetchone()[0]:
                skipped += 1
                continue

            # Usar el helper centralizado
            save_closed_position(
                {
                    "exchange": pos["exchange"],
                    "symbol": pos["symbol"],
                    "side": pos["side"],
                    "size": pos["size"],
                    "entry_price": pos["entry_price"],
                    "close_price": pos["close_price"],
                    "open_time": pos["open_time"],
                    "close_time": pos["close_time"],
                    "realized_pnl": pos["realized_pnl"],
                    "funding_total": pos.get("funding_fee", 0.0),
                    "fee_total": pos.get("fees", 0.0),
                    "notional": pos["notional"],
                    "leverage": pos.get("leverage"),
                    "liquidation_price": pos.get("liquidation_price"),
                }
            )
            saved += 1

        except Exception as e:
            print(f"⚠️ Error guardando {pos.get('symbol')} (Extended): {e}")

    conn.close()
    print(f"✅ Extended guardadas: {saved} | omitidas (duplicadas): {skipped}")


# ========= DEBUG: OPEN POSITIONS (Extended) =========
def _pp(obj, maxlen=2000):
    try:
        s = json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        s = str(obj)
    return (s[:maxlen] + "...\n[truncado]") if len(s) > maxlen else s


# ========= DEBUG AUTOEJECUTABLE: OPEN POSITIONS (Extended) =========
import os, json, time, urllib.parse


def _pp(obj, maxlen=2000):
    try:
        s = json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        s = str(obj)
    return (s[:maxlen] + "...\n[truncado]") if len(s) > maxlen else s


def debug_open_extended_positions(
    raw=False, sample=3, dump_file="extended_positions_debug.json"
):
    """
    Llama a /api/v1/user/positions para TODAS las subcuentas y muestra cómo viene el JSON.
    No cambia la lógica del adapter ni la firma; solo inspecciona.
    """
    if not EXTENDED_ACCOUNTS:
        print("⚠️ No Extended accounts configured")
        return

    for account in EXTENDED_ACCOUNTS:
        print(f"\n🔎 DEBUG Extended [{account['label']}] → /user/positions")
        resp = extended_get(
            "/user/positions",
            api_key=account["api_key"],
            api_secret=account["api_secret"],
        )

        if not isinstance(resp, dict):
            print(f"⚠️ Respuesta no es dict [{account['label']}]:", type(resp))
            print(resp)
            continue

        print(f"🔑 Claves top-level [{account['label']}]:", list(resp.keys()))
        if "status" in resp:
            print(f"📌 status [{account['label']}]:", resp.get("status"))

        # Guardar dump completo para inspección
        try:
            os.makedirs("tmp", exist_ok=True)
            account_dump = f"{account['label']}_{dump_file}"
            path = os.path.join("tmp", account_dump)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(resp, f, ensure_ascii=False, indent=2)
            print(f"💾 JSON completo guardado en: {path}")
        except Exception as e:
            print(f"⚠️ No pude guardar dump [{account['label']}]:", e)

        if raw:
            print(f"—— JSON crudo [{account['label']}] (recortado) ——")
            print(_pp(resp, 4000))

        data = resp.get("data")
        print(f"📌 type(data) [{account['label']}]:", type(data).__name__)
        if isinstance(data, dict):
            print(f"🔑 data keys [{account['label']}]:", list(data.keys()))
            # Candidatas típicas a contener la lista de posiciones
            candidates = []
            for k in ("positions", "list", "items", "rows", "data"):
                v = data.get(k)
                if isinstance(v, list):
                    candidates.append((k, v))
            if candidates:
                for k, lst in candidates:
                    print(f"📦 [{account['label']}] data['{k}'] → len={len(lst)}")
                    for i, it in enumerate(lst[:sample]):
                        print(f"  🧾 [{account['label']}] {k}[{i}]:", _pp(it, 700))
            else:
                print(
                    f"⚠️ [{account['label']}] 'data' es dict pero no veo lista en ['positions','list','items','rows','data']"
                )
        elif isinstance(data, list):
            print(f"📦 [{account['label']}] data (list) → len={len(data)}")
            for i, it in enumerate(data[:sample]):
                print(f"  🧾 [{account['label']}] data[{i}]:", _pp(it, 700))
        else:
            print(f"⚠️ [{account['label']}] 'data' no es ni dict ni list. Valor:", data)

        # Info de paginación si existiera
        pg = resp.get("pagination") or (
            data.get("pagination") if isinstance(data, dict) else None
        )
        if pg:
            print(f"🧭 [{account['label']}] pagination:", _pp(pg, 600))


# if __name__ == "__main__":
#     # AUTOEJECUCIÓN controlada por env vars (con valores por defecto útiles)
#     auto = os.getenv("EXT_DEBUG_AUTO", "1") == "1"     # pon a "0" para desactivar auto-debug
#     raw  = os.getenv("EXT_DEBUG_RAW", "1") == "1"      # JSON crudo recortado
#     try:
#         sample = int(os.getenv("EXT_DEBUG_SAMPLE", "5"))
#     except Exception:
#         sample = 5
#     dump = os.getenv("EXT_DEBUG_DUMP", "extended_positions_debug.json")

#     # Soporta también flags CLI si prefieres: --open-debug --raw --sample 5
#     import argparse
#     ap = argparse.ArgumentParser()
#     ap.add_argument("--open-debug", action="store_true", help="Ejecuta el debug de /user/positions")
#     ap.add_argument("--raw", action="store_true", help="Imprime JSON crudo recortado")
#     ap.add_argument("--sample", type=int, default=sample, help="N de ejemplos a mostrar")
#     ap.add_argument("--dump", type=str, default=dump, help="Nombre del archivo de volcado")
#     args, _ = ap.parse_known_args()

#     if args.open_debug or auto:
#         debug_extended_open_positions(raw=(args.raw or raw), sample=args.sample, dump_file=args.dump)
#     else:
#         print("ℹ️ Ejecuta con --open-debug o exporta EXT_DEBUG_AUTO=1 para autoejecutar.")
