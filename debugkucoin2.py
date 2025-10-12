# debug_kucoin.py
import os
import sys

# Añadir el directorio actual al path para los imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_manager import init_db
from adapters.kucoin import save_kucoin_closed_positions

def debug_kucoin_only():
    print("🔧 EJECUTANDO DEBUG SOLO DE KUCOIN")
    
    # Inicializar DB
    print("🧱 Inicializando base de datos...")
    init_db()
    
    # Ejecutar solo KuCoin
    print("🚀 Sincronizando solo KuCoin...")
    save_kucoin_closed_positions(debug=True)
    
    print("✅ Debug completado")

if __name__ == "__main__":
    debug_kucoin_only()
