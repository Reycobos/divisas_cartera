import sqlite3

def fix_kaito_pnl():
    """Corrige manualmente el PnL de KAITO a 465.4"""
    conn = sqlite3.connect("portfolio.db")
    cur = conn.cursor()
    
    # Actualizar el PnL de KAITO
    cur.execute("""
        UPDATE closed_positions 
        SET realized_pnl = 465.4 
        WHERE symbol LIKE '%KAITO%' AND exchange = 'backpack'
    """)
    
    conn.commit()
    conn.close()
    print("✅ PnL de KAITO corregido a 465.4")

# Ejecutar
if __name__ == "__main__":
    fix_kaito_pnl()
