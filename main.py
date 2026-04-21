import pandas as pd
from google.cloud import bigquery
import os

# 1. Autenticación Profesional
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "secrets/google_key.json"
client = bigquery.Client()

# CONFIGURACIÓN: Reemplaza con el ID exacto de tu proyecto en GCP
PROJECT_ID = "cuentaspagar" # <--- ¡REVISA QUE SEA EL TUYO!
DATASET_ID = "finanzas"
TABLE_ID = "cuentas_por_pagar"
FULL_TABLE_PATH = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# ... (mismo encabezado anterior)

def verificar_y_cargar(ruta_excel):
    print("🧹 Iniciando limpieza inteligente Sharkflow...")
    df = pd.read_excel(ruta_excel)
    
    # 1. Estandarizar columnas
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '', regex=False)
    
    # 2. FILTRO ANTI-DUPLICADOS (Totales)
    # Eliminamos filas donde el nombre del tercero contenga "TOTAL" o esté vacío
    if 'terceros' in df.columns:
        # Quitamos filas vacías
        df = df[df['terceros'].notna()]
        # Quitamos la fila que diga 'TOTAL' (ignora mayúsculas/minúsculas)
        df = df[~df['terceros'].astype(str).str.contains('TOTAL', case=False, na=False)]

    columna_deuda = 'saldo_final' 
    
    # 3. Limpieza de formato matemático
    if df[columna_deuda].dtype == object:
        df[columna_deuda] = df[columna_deuda].astype(str).str.replace('$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df[columna_deuda] = pd.to_numeric(df[columna_deuda], errors='coerce').fillna(0)

    # 4. Blindaje de texto
    for col in df.columns:
        if col != columna_deuda and df[col].dtype == 'object':
            df[col] = df[col].astype(str)

    # Auditoría Final
    excel_total = df[columna_deuda].sum()
    excel_count = len(df)
    
    print(f"✅ Datos Limpios: {excel_count} facturas reales.")
    print(f"💰 Deuda Real Calculada: ${excel_total:,.0f} COP")

    # Carga (Reemplaza la tabla anterior con la limpia)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, FULL_TABLE_PATH, job_config=job_config).result()
    print("🚀 Nube actualizada con datos verificados.")

# ... (resto del script)

    # Auditoría Post-Carga (Query a la nube)
    query = f"SELECT COUNT(*) as cant, SUM({columna_deuda}) as total FROM `{FULL_TABLE_PATH}`"
    res = client.query(query).to_dataframe()
    
    bq_total = res['total'][0]
    bq_count = res['cant'][0]

    print("\n" + "="*30)
    print("🏆 RESULTADO DEL CRUCE")
    print("="*30)
    if excel_total == bq_total:
        print(f"✅ INTEGRIDAD TOTAL: ${bq_total:,.0f} COP cargados correctamente.")
    else:
        diff = excel_total - bq_total
        print(f"⚠️ DISCREPANCIA: Diferencia de ${diff:,.0f} COP.")

if __name__ == "__main__":
    verificar_y_cargar("data/deuda_actual.xlsx")