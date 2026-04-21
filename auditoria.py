import pandas as pd

# Leer el archivo original
df = pd.read_excel("data/deuda_actual.xlsx")

# Limpiar columnas
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '', regex=False)

# Mostrar el Top 10 de valores más altos tal cual vienen de Excel
print("\n🔍 TOP 10 FACTURAS (SIN ALTERAR EL FORMATO):")
print(df[['terceros', 'saldo_final']].sort_values(by='saldo_final', ascending=False).head(10))

# Revisar si hay filas vacías al final que puedan ser "Totales"
print("\n👀 ÚLTIMAS 5 FILAS DEL EXCEL:")
print(df[['terceros', 'saldo_final']].tail())
