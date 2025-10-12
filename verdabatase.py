import sqlite3
import pandas as pd

# Ruta a tu base de datos
db_path = "portfolio.db"

# Conexión a SQLite
conn = sqlite3.connect(db_path)

# Cargar la tabla de posiciones cerradas
df_closed = pd.read_sql_query("SELECT * FROM closed_positions;", conn)

# Cerrar conexión
conn.close()

# Mostrar estructura de columnas
print("🧱 Columnas en 'closed_positions':")
print(df_closed.columns.tolist())

# Mostrar primeras filas
print("\n📋 Primeras filas de las posiciones cerradas:")
print(df_closed.head())

# (Opcional) Mostrar resumen general
print("\n📊 Resumen numérico:")
print(df_closed.describe(include='all'))

# --- Filtrar columnas principales si quieres verlo más claro ---
cols_principales = [
    "exchange", "symbol", "side", "size",
    "entry_price", "close_price", "pnl", "fees",
    "funding_fee", "realized_pnl", "open_date", "close_date"
]

# Solo mostrar las columnas que existan realmente
cols_principales = [c for c in cols_principales if c in df_closed.columns]

df_view = df_closed[cols_principales].copy()

print("\n🪙 Closed positions (vista principal):")
print(df_view.head(20))  # muestra las primeras 20 filas

# 👉 En Spyder, puedes abrir df_closed o df_view desde el Variable Explorer para verlo completo

