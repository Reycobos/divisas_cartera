import os
import requests
import time
import hmac
import hashlib
import base64
import nacl.signing
from urllib.parse import urlencode
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

# Configuración Backpack
BACKPACK_API_KEY = os.getenv("BACKPACK_API_KEY")
BACKPACK_API_SECRET = os.getenv("BACKPACK_API_SECRET")
BACKPACK_BASE_URL = "https://api.backpack.exchange"

def _bp_sign_message(instruction: str, params: dict | None, ts_ms: int, window_ms: int = 5000) -> str:
    """Firma para Backpack API"""
    try:
        query = ""
        if params:
            sorted_items = sorted(params.items())
            query = urlencode(sorted_items, doseq=True)

        if query:
            to_sign = f"instruction={instruction}&{query}&timestamp={ts_ms}&window={window_ms}"
        else:
            to_sign = f"instruction={instruction}&timestamp={ts_ms}&window={window_ms}"

        seed32 = base64.b64decode(BACKPACK_API_SECRET)
        signing_key = nacl.signing.SigningKey(seed32)
        sig_bytes = signing_key.sign(to_sign.encode("utf-8")).signature
        return base64.b64encode(sig_bytes).decode("ascii")
    except Exception as e:
        print(f"❌ Error en firma: {e}")
        return ""

def backpack_signed_request(method: str, path: str, instruction: str, params: dict | None = None):
    """Request firmado a Backpack"""
    ts_ms = int(time.time() * 1000)
    window_ms = 5000

    sign_params = params if method.upper() == "GET" else {}
    signature_b64 = _bp_sign_message(instruction, sign_params, ts_ms, window_ms)
    
    headers = {
        "X-API-KEY": BACKPACK_API_KEY,
        "X-SIGNATURE": signature_b64,
        "X-TIMESTAMP": str(ts_ms),
        "X-WINDOW": str(window_ms),
        "Content-Type": "application/json; charset=utf-8",
    }

    url = f"{BACKPACK_BASE_URL}{path}"
    r = requests.get(url, headers=headers, params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()

def debug_kaito_trades_reconstruction():
    """Debug específico de la reconstrucción de posiciones KAITO"""
    print("🔍 DEBUG RECONSTRUCCIÓN POSICIONES KAITO")
    print("=" * 60)
    
    try:
        # Obtener todos los trades de KAITO_USDC_PERP
        print("📡 Obteniendo todos los fills de KAITO_USDC_PERP...")
        fills_data = backpack_signed_request(
            "GET", 
            "/wapi/v1/history/fills", 
            instruction="fillHistoryQueryAll",
            params={"limit": 100, "symbol": "KAITO_USDC_PERP"}
        )
        
        if not isinstance(fills_data, list):
            print(f"❌ Respuesta inesperada: {fills_data}")
            return
        
        print(f"📊 Total fills de KAITO_USDC_PERP: {len(fills_data)}")
        
        # Ordenar por timestamp
        fills_data.sort(key=lambda x: x.get('timestamp', ''))
        
        # Mostrar todos los trades en orden
        print(f"\n📋 TODOS LOS TRADES KAITO_USDC_PERP (ordenados por tiempo):")
        net_position = 0.0
        position_blocks = []
        current_block = []
        
        for i, fill in enumerate(fills_data):
            symbol = fill.get('symbol', '')
            side = fill.get('side', '')
            quantity = float(fill.get('quantity', 0))
            price = float(fill.get('price', 0))
            timestamp = fill.get('timestamp', '')
            fee = float(fill.get('fee', 0))
            
            # Calcular cambio en posición
            if side.upper() == 'BID':  # BID = BUY = LONG
                position_change = quantity
                side_display = "BUY"
            else:  # ASK = SELL = SHORT  
                position_change = -quantity
                side_display = "SELL"
            
            net_position += position_change
            current_block.append(fill)
            
            print(f"   {i+1:2d}. {timestamp} {side_display:4s} {quantity:8.2f} @ {price:.4f} | Net: {net_position:8.2f} | Fee: {fee:.6f}")
            
            # Detectar cierre de posición (net ≈ 0)
            if abs(net_position) < 0.01 and len(current_block) > 1:
                print(f"   🎯 💥 POSICIÓN CERRADA - Net: {net_position:.4f}")
                
                # Analizar el bloque cerrado
                buys = [f for f in current_block if f['side'].upper() == 'BID']
                sells = [f for f in current_block if f['side'].upper() == 'ASK']
                
                total_buy_qty = sum(float(f['quantity']) for f in buys)
                total_sell_qty = sum(float(f['quantity']) for f in sells)
                total_fees = sum(float(f.get('fee', 0)) for f in current_block)
                
                position_blocks.append({
                    'size': total_buy_qty,
                    'total_trades': len(current_block),
                    'buy_trades': len(buys),
                    'sell_trades': len(sells),
                    'net_final': net_position,
                    'fees': total_fees,
                    'timestamp_start': current_block[0].get('timestamp'),
                    'timestamp_end': current_block[-1].get('timestamp'),
                    'trades': current_block.copy()
                })
                
                print(f"   📦 BLOQUE CERRADO:")
                print(f"      Trades: {len(current_block)} (BUY: {len(buys)}, SELL: {len(sells)})")
                print(f"      Total BUY: {total_buy_qty:.2f}, Total SELL: {total_sell_qty:.2f}")
                print(f"      Size calculado: {total_buy_qty:.2f}")
                print(f"      Fees total: {total_fees:.6f}")
                
                # Reset para siguiente posición
                current_block = []
                net_position = 0.0
        
        print(f"\n📊 RESUMEN BLOQUES CERRADOS: {len(position_blocks)}")
        for i, block in enumerate(position_blocks):
            print(f"   {i+1}. Size: {block['size']:.2f}, Trades: {block['total_trades']}")
            print(f"      From: {block['timestamp_start']}")
            print(f"      To:   {block['timestamp_end']}")
            print(f"      Net final: {block['net_final']:.4f}")
            
        # Buscar específicamente la posición problemática (6700 size)
        print(f"\n🔎 BUSCANDO POSICIÓN DE 6700:")
        target_blocks = [b for b in position_blocks if abs(b['size'] - 6700) < 100]
        
        if target_blocks:
            print(f"   ✅ ENCONTRADA posición cercana a 6700:")
            for block in target_blocks:
                print(f"      Size: {block['size']:.2f}")
                print(f"      Timestamp: {block['timestamp_start']} → {block['timestamp_end']}")
        else:
            print(f"   ❌ NO se encontró posición de ~6700")
            print(f"   📋 Sizes encontrados: {[b['size'] for b in position_blocks]}")
            
    except Exception as e:
        print(f"❌ Error en debug reconstrucción: {e}")
        import traceback
        traceback.print_exc()

def debug_specific_6700_position():
    """Debug específico para encontrar de dónde sale el 8049.3"""
    print("\n🎯 DEBUG ORIGEN DEL 8049.3")
    print("=" * 60)
    
    try:
        # Obtener más trades para ver el patrón completo
        fills_data = backpack_signed_request(
            "GET", 
            "/wapi/v1/history/fills", 
            instruction="fillHistoryQueryAll",
            params={"limit": 200, "symbol": "KAITO_USDC_PERP"}
        )
        
        if not isinstance(fills_data, list):
            return
            
        # Buscar trades alrededor de Sept 28 - Oct 1 (posición problemática)
        problem_trades = []
        for fill in fills_data:
            timestamp = fill.get('timestamp', '')
            if any(date in timestamp for date in ['2025-09-28', '2025-09-29', '2025-09-30', '2025-10-01']):
                problem_trades.append(fill)
        
        print(f"📊 Trades en rango problemático (Sep 28 - Oct 1): {len(problem_trades)}")
        
        if problem_trades:
            # Reconstruir posición manualmente
            net_qty = 0.0
            print(f"\n🔨 RECONSTRUYENDO POSICIÓN MANUALMENTE:")
            for trade in sorted(problem_trades, key=lambda x: x.get('timestamp', '')):
                side = trade.get('side', '')
                qty = float(trade.get('quantity', 0))
                price = float(trade.get('price', 0))
                timestamp = trade.get('timestamp', '')
                
                if side.upper() == 'BID':
                    net_qty += qty
                    action = "BUY"
                else:
                    net_qty -= qty
                    action = "SELL"
                    
                print(f"   {timestamp} {action:4s} {qty:8.2f} | Net: {net_qty:8.2f}")
                
                # Si la posición se cierra
                if abs(net_qty) < 0.01 and len(problem_trades) > 1:
                    print(f"   💥 POSICIÓN CERRADA - Net: {net_qty:.4f}")
                    
                    # Calcular size real (total de compras)
                    total_buys = sum(float(t['quantity']) for t in problem_trades if t['side'].upper() == 'BID')
                    print(f"   📏 TOTAL BUYS (size real): {total_buys:.2f}")
                    break
                    
        else:
            print("   ⚠️  No hay trades en el rango problemático")
            
    except Exception as e:
        print(f"❌ Error en debug específico: {e}")

def check_current_algorithm():
    """Verificar qué está haciendo el algoritmo actual"""
    print("\n🔧 VERIFICANDO ALGORITMO ACTUAL")
    print("=" * 60)
    
    try:
        # Simular lo que hace save_backpack_closed_positions
        from trades_processingv7 import build_positions_from_trades
        
        # Obtener trades
        fills_data = backpack_signed_request(
            "GET", 
            "/wapi/v1/history/fills", 
            instruction="fillHistoryQueryAll",
            params={"limit": 100, "symbol": "KAITO_USDC_PERP"}
        )
        
        if fills_data:
            # Convertir a formato que espera build_positions_from_trades
            trades_formatted = []
            for fill in fills_data:
                trades_formatted.append({
                    "symbol": fill.get('symbol', ''),
                    "side": "buy" if fill.get('side', '').upper() == 'BID' else "sell",
                    "qty": float(fill.get('quantity', 0)),
                    "price": float(fill.get('price', 0)),
                    "commission": float(fill.get('fee', 0)),
                    "time": fill.get('timestamp', ''),
                    "exchange": "backpack"
                })
            
            print(f"📊 Trades formateados para algoritmo: {len(trades_formatted)}")
            
            # Ejecutar algoritmo actual
            positions = build_positions_from_trades(trades_formatted)
            
            print(f"📦 Posiciones reconstruidas por algoritmo: {len(positions)}")
            for i, pos in enumerate(positions):
                print(f"   {i+1}. {pos['symbol']} {pos['side']} size={pos['size']:.2f}")
                print(f"      Entry: {pos['entry_price']:.4f}, Close: {pos['close_price']:.4f}")
                print(f"      Realized: {pos['realized_pnl']:.2f}, Fees: {pos['fee_total']:.2f}")
                
        else:
            print("❌ No hay trades para probar el algoritmo")
            
    except Exception as e:
        print(f"❌ Error verificando algoritmo: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🚀 Debug completo de reconstrucción KAITO")
    debug_kaito_trades_reconstruction()
    debug_specific_6700_position()
    check_current_algorithm()