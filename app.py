import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json
from datetime import datetime

# ==========================================
# 1. AUTENTICACIÓN SHARKFLOW (NUBE VS LOCAL)
# ==========================================

# Intentar cargar desde los Secrets de Streamlit (Modo Nube)
if "gcp_service_account" in st.secrets:
    try:
        # Convertimos el diccionario de secretos a un formato que Google entienda
        info = dict(st.secrets["gcp_service_account"])
        # IMPORTANTE: Reemplazar los saltos de línea literales en la llave privada
        if "private_key" in info:
            info["private_key"] = info["private_key"].replace("\\n", "\n")
            
        credentials = service_account.Credentials.from_service_account_info(info)
        client = bigquery.Client(credentials=credentials, project=info["project_id"])
        print("✅ Conectado usando Streamlit Secrets (Nube)")
    except Exception as e:
        st.error(f"Error en los Secrets de la nube: {e}")
        st.stop()

# Si no hay secretos, buscar el archivo local (Modo Local)
else:
    ruta_local = "secrets/google_key.json"
    if os.path.exists(ruta_local):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ruta_local
        client = bigquery.Client()
        print("💻 Conectado usando archivo local JSON")
    else:
        st.error("❌ No se encontraron credenciales (ni Secrets ni archivo JSON).")
        st.stop()


# ==========================================
# 1. CONFIGURACIÓN DEL ENTORNO SHARKFLOW
# ==========================================
st.set_page_config(page_title="Sharkflow Finance", page_icon="🦈", layout="wide")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "secrets/google_key.json"

# CONFIGURACIÓN: ¡Asegúrate de que este sea tu ID real!
PROJECT_ID = "cuentaspagar" 
DATASET_ID = "finanzas"
TABLE_ID = "cuentas_por_pagar"
FULL_TABLE_PATH = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

client = bigquery.Client()

# ==========================================
# 2. CONEXIÓN A DATOS
# ==========================================
@st.cache_data(ttl=60)
def cargar_datos_nube():
    query = f"SELECT * FROM `{FULL_TABLE_PATH}` ORDER BY saldo_final DESC"
    df = client.query(query).to_dataframe()
    
    # Asegurar columnas mínimas para evitar errores
    if 'fecha_pago_proyectada' not in df.columns:
        df['fecha_pago_proyectada'] = pd.to_datetime(df['fecha_vencimiento'], errors='coerce').dt.date
    if 'estado' not in df.columns:
        df['estado'] = "Pendiente"
    if 'abono' not in df.columns:
        df['abono'] = 0.0
    return df

df_pendientes = cargar_datos_nube()

# ==========================================
# 3. SIDEBAR: INGRESO DE NUEVAS FACTURAS
# ==========================================
with st.sidebar:
    st.header("📥 Ingreso Rápido")
    with st.form("form_nueva_factura", clear_on_submit=True):
        nuevo_tercero = st.text_input("Proveedor*")
        nuevo_fv = st.text_input("Nº Factura*")
        nuevo_monto = st.number_input("Valor*", min_value=0.0, step=10000.0)
        nueva_fecha = st.date_input("Fecha Proyectada")
        submit_factura = st.form_submit_button("Agregar Factura", type="primary")
        
        if submit_factura and nuevo_tercero and nuevo_fv:
            nueva_fila = pd.DataFrame([{
                "terceros": nuevo_tercero, "fv": nuevo_fv, "saldo_final": nuevo_monto,
                "abono": 0.0, "fecha_pago_proyectada": nueva_fecha, "estado": "Pendiente",
                "entidad": "Manual", "observaciones": "Ingreso Streamlit"
            }])
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            client.load_table_from_dataframe(nueva_fila, FULL_TABLE_PATH, job_config=job_config).result()
            st.success("Guardado.")
            st.cache_data.clear()

# ==========================================
# 4. INTERFAZ PRINCIPAL
# ==========================================
st.title("💼 Cuentas x Pagar Madame Sucrée 2026")

# MÓDULO A: Gamificación
df_pequenas = df_pendientes[(df_pendientes['saldo_final'] < 1000000) & (df_pendientes['estado'] != 'Pagado')]
df_grandes = df_pendientes[(df_pendientes['saldo_final'] >= 1000000) & (df_pendientes['estado'] != 'Pagado')]
facturas_pagadas = df_pendientes[df_pendientes['estado'] == 'Pagado']

col_meta1, col_meta2, col_meta3 = st.columns(3)
with col_meta1:
    st.metric("🔥 Victorias Rápidas", f"{len(df_pequenas)} facturas")
    progreso = (len(facturas_pagadas) / (len(df_pequenas) + len(facturas_pagadas))) * 100 if not df_pequenas.empty else 100
    st.progress(int(min(progreso, 100)))
with col_meta2:
    st.metric("🏛️ Titanes", f"{len(df_grandes)} facturas")
with col_meta3:
    st.download_button("📤 Descargar Excel para Equipo", df_pendientes.to_csv(index=False).encode('utf-8'), "reporte.csv", "text/csv")

st.markdown("---")

# MÓDULO C: Filtro y Tabla
filtro = st.radio("Filtrar vista:", ["Todas", "Victorias Rápidas (<1M)", "Titanes (>1M)"], horizontal=True)

if filtro == "Victorias Rápidas (<1M)":
    df_mostrar = df_pendientes[df_pendientes['saldo_final'] < 1000000]
elif filtro == "Titanes (>1M)":
    df_mostrar = df_pendientes[df_pendientes['saldo_final'] >= 1000000]
else:
    df_mostrar = df_pendientes

# Definición de columnas visibles (Lo que faltaba)
columnas_config = {
    "terceros": "Acreedor",
    "fv": "Nº Factura",
    "abono": st.column_config.NumberColumn("Abono", format="$%d"),
    "saldo_final": st.column_config.NumberColumn("Deuda Actual", format="$%d"),
    "fecha_pago_proyectada": st.column_config.DateColumn("Target Pago"),
    "estado": st.column_config.SelectboxColumn("Estado", options=["Pendiente", "Pagado", "Reprogramado"])
}

# Ocultar el resto de columnas técnicas automáticamente
config_final = {col: columnas_config.get(col, None) for col in df_pendientes.columns}

df_editado = st.data_editor(
    df_mostrar, # <--- IMPORTANTE: Aquí usamos la versión filtrada
    column_config=config_final,
    use_container_width=True,
    hide_index=True,
    key="editor_pagos"
)

# Guardado (IMPORTANTE: El guardado debe considerar que df_editado es solo una parte)
if st.button("💾 Guardar Cambios"):
    with st.spinner("Sincronizando..."):
        # Unimos los cambios del editor con la tabla original para no perder las facturas que no se están viendo
        df_pendientes.update(df_editado) 
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df_pendientes, FULL_TABLE_PATH, job_config=job_config).result()
        st.cache_data.clear()
        st.success("Nube actualizada.")
        st.balloons()