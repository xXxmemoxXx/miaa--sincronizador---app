import streamlit as st
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
import datetime
import time
import mysql.connector
import pytz
import numpy as np

# --- 1. CONFIGURACIÓN ---
zona_local = pytz.timezone('America/Mexico_City')
st.set_page_config(page_title="MIAA Control Maestro", layout="wide")

# Credenciales (Mantengo las tuyas)
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': 'bWkrw1Uum1O&', 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': 'M144.Tec', 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# --- 2. LÓGICA DE DATOS ---

@st.cache_data(ttl=60) # Cache de 1 minuto para no saturar la BD
def consultar_postgres():
    try:
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        query = 'SELECT * FROM public."Pozos" ORDER BY "ID" ASC'
        df = pd.read_sql(query, eng_pg)
        return df
    except Exception as e:
        return f"Error al conectar con Postgres: {str(e)}"

def ejecutar_sincronizacion_total():
    # ... (Tu función original se mantiene exactamente igual)
    # Copia aquí todo el contenido de tu función ejecutar_sincronizacion_total()
    pass 

# --- 3. INTERFAZ (TABS) ---

st.title("🖥️ MIAA Control Center")

# Creación de las pestañas
tab_control, tab_datos = st.tabs(["Control de Sincronización", "📊 Datos Postgres (QGIS)"])

with tab_control:
    with st.container(border=True):
        c1, c2, c3, c4, c5 = st.columns([1.5, 1, 1, 1.5, 1.5])
        with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"], index=0)
        with c2: h_in = st.number_input("Hora", 0, 23, value=0)
        with c3: m_in = st.number_input("Min/Int", 0, 59, value=0)
        with c4:
            if "running" not in st.session_state: st.session_state.running = False
            btn_label = "🛑 PARAR" if st.session_state.running else "▶️ INICIAR"
            if st.button(btn_label, use_container_width=True):
                st.session_state.running = not st.session_state.running
                st.rerun()
        with c5:
            if st.button("🚀 FORZAR CARGA", use_container_width=True):
                st.session_state.last_logs = ejecutar_sincronizacion_total()

    # Consola
    log_txt = "<br>".join(st.session_state.get('last_logs', ["SISTEMA EN ESPERA..."]))
    st.markdown(f'<div style="background-color:black;color:#00FF00;padding:15px;font-family:Consolas;height:250px;overflow-y:auto;border-radius:5px;line-height:1.6;">{log_txt}</div>', unsafe_allow_html=True)

with tab_datos:
    st.subheader("Registros en la tabla 'Pozos'")
    if st.button("🔄 Refrescar Datos"):
        st.cache_data.clear()
    
    data_pg = consultar_postgres()
    
    if isinstance(data_pg, pd.DataFrame):
        st.write(f"Total de registros: {len(data_pg)}")
        # Buscador simple
        search = st.text_input("Buscar pozo por ID o Nombre:")
        if search:
            # Filtrar en todas las columnas si contienen el texto
            mask = data_pg.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
            data_pg = data_pg[mask]
        
        st.dataframe(data_pg, use_container_width=True, hide_index=True)
    else:
        st.error(data_pg)

# --- 4. RELOJ DE EJECUCIÓN (Fuera de los tabs para que corra siempre) ---
if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    # ... (Tu lógica de reloj original)
    # Asegúrate de incluir el st.rerun() al final para mantener el contador vivo
