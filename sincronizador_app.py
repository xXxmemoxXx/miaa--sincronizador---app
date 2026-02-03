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

# CSS: Pestañas Adaptativas y Consola Blanca con Letras Azul Oscuro
st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] {
        display: flex;
        flex-wrap: nowrap;
        background-color: #FFFFFF !important; 
        padding: 5px 5px 0px 5px;
        border-bottom: 2px solid #F0F2F6;
        overflow-x: auto;
    }
    .stTabs [data-baseweb="tab"] {
        flex: 1;
        min-width: 120px;
        height: 45px;
        background-color: #F8F9FA;
        border-radius: 5px 5px 0px 0px;
        color: #31333F;
        padding: 5px 10px;
        border: 1px solid #DDDDDD;
        border-bottom: none;
        font-size: 14px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #007bff !important;
        color: white !important;
        font-weight: bold;
    }
    .consola-log {
        background-color: #FFFFFF;
        color: #003366;
        padding: 15px;
        font-family: 'Consolas', monospace;
        height: 350px;
        overflow-y: auto;
        border-radius: 8px;
        border: 1px solid #CCCCCC;
        line-height: 1.6;
    }
    </style>
    """, unsafe_allow_html=True)

# Credenciales (Tal cual tu respaldo)
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': 'bWkrw1Uum1O&', 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': 'M144.Tec', 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# Mapeos originales
MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal', 'PRESION_(kg/cm2)': '_Presion', 'LONGITUD_DE_COLUMNA': '_Long_colum',
    'COLUMNA_DIAMETRO_1': '_Diam_colum', 'TIPO_COLUMNA': '_Tipo_colum', 'SECTOR_HIDRAULICO': '_Sector',
    'NIVEL_DINAMICO_(mts)': '_Nivel_Din', 'NIVEL_ESTATICO_(mts)': '_Nivel_Est', 'EXTRACCION_MENSUAL_(m3)': '_Vm_estr',
    'HORAS_DE_OPERACIÓN_DIARIA_(hrs)': '_Horas_op', 'DISTRITO_1': '_Distrito', 'ESTATUS': '_Estatus',
    'TELEMETRIA': '_Telemetria', 'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

MAPEO_SCADA = {
    "P-002": {
        "GASTO_(l.p.s.)":"PZ_002_TRC_CAU_INS", "PRESION_(kg/cm2)":"PZ_002_TRC_PRES_INS",
        "VOLTAJE_L1":"PZ_002_TRC_VOL_L1_L2", "VOLTAJE_L2":"PZ_002_TRC_VOL_L2_L3", "VOLTAJE_L3":"PZ_002_TRC_VOL_L1_L3",
        "AMP_L1":"PZ_002_TRC_CORR_L1", "AMP_L2":"PZ_002_TRC_CORR_L2", "AMP_L3":"PZ_002_TRC_CORR_L3",
        "LONGITUD_DE_COLUMNA":"PZ_002_TRC_LONG_COLUM", "SUMERGENCIA":"PZ_002_TRC_SUMERG", "NIVEL_DINAMICO":"PZ_002_TRC_NIV_EST",
    },
    "P-003": {
        "GASTO_(l.p.s.)":"PZ_003_CAU_INS", "PRESION_(kg/cm2)":"PZ_003_PRES_INS",
        "VOLTAJE_L1":"PZ_003_VOL_L1_L2", "VOLTAJE_L2":"PZ_003_VOL_L2_L3", "VOLTAJE_L3":"PZ_003_VOL_L1_L3",
        "AMP_L1":"PZ_003_CORR_L1", "AMP_L2":"PZ_003_CORR_L2", "AMP_L3":"PZ_003_CORR_L3",
        "LONGITUD_DE_COLUMNA":"PZ_003_LONG_COLUM", "SUMERGENCIA":"PZ_003_SUMERG", "NIVEL_DINAMICO":"PZ_003_NIV_EST",
    },
}

# --- 2. LÓGICA DE PROCESAMIENTO (RESTAURADA TOTALMENTE) ---

def ejecutar_sincronizacion_total():
    start_time = time.time()
    st.session_state.last_logs = []
    logs = []
    progreso_bar = st.progress(0, text="Preparando sincronización... 0%")
    status_text = st.empty()
    filas_pg = 0
    
    try:
        progreso_bar.progress(10, text="Leyendo Google Sheets... 10%")
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        
        if 'POZOS' not in df.columns:
            return [f"❌ Error: No se encontró la columna 'POZOS'."]

        if 'FECHA_ACTUALIZACION' in df.columns:
            df['FECHA_ACTUALIZACION'] = pd.to_datetime(df['FECHA_ACTUALIZACION'], errors='coerce')
        
        logs.append(f"✅ Google Sheets: {len(df)} registros leídos.")
        progreso_bar.progress(40, text="Consultando Base de Datos SCADA... 40%")
        
        conn_s = mysql.connector.connect(**DB_SCADA)
        all_tags = []
        for p_id in MAPEO_SCADA: all_tags.extend(MAPEO_SCADA[p_id].values())
        
        query = f"SELECT r.NAME, h.VALUE FROM vfitagnumhistory h JOIN VfiTagRef r ON h.GATEID = r.GATEID WHERE r.NAME IN ({','.join(['%s']*len(all_tags))}) AND h.FECHA >= NOW() - INTERVAL 1 DAY ORDER BY h.FECHA DESC"
        df_scada = pd.read_sql(query, conn_s, params=all_tags).drop_duplicates('NAME')
        
        for p_id, config in MAPEO_SCADA.items():
            for col_excel, tag_name in config.items():
                val = df_scada.loc[df_scada['NAME'] == tag_name, 'VALUE']
                if not val.empty and col_excel in df.columns:
                    # REGLA POSITIVOS:
                    val_num = float(val.values[0])
                    if val_num > 0:
                        df.loc[df['POZOS'] == p_id, col_excel] = round(val_num, 2)
        conn_s.close()
        
        progreso_bar.progress(70, text="Actualizando tabla INFORME... 70%")
        p_my = urllib.parse.quote_plus(DB_INFORME['password'])
        eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        with eng_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            df_sql = df.replace({np.nan: None, pd.NaT: None})
            df_sql.to_sql('INFORME', con=conn, if_exists='append', index=False)
        
        progreso_bar.progress(85, text="Sincronizando con QGIS (Postgres)... 85%")
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        
        with eng_pg.begin() as conn:
            for _, row in df.iterrows():
                id_val = str(row['ID']).strip() if pd.notnull(row['ID']) else None
                if id_val and id_val != "nan":
                    params = {'id': id_val}; sets = []
                    for csv_col, pg_col in MAPEO_POSTGRES.items():
                        if csv_col in df.columns:
                            val = row[csv_col]
                            if pd.isna(val) or str(val).lower() == 'nan': clean_val = None
                            elif pg_col == '_Ultima_actualizacion': clean_val = val.to_pydatetime() if hasattr(val, 'to_pydatetime') else val
                            elif isinstance(val, str): clean_val = float(val.replace(',', '')) if val.replace(',', '').replace('.','').isdigit() else val
                            else: clean_val = val
                            params[pg_col] = clean_val; sets.append(f'"{pg_col}" = :{pg_col}')
                    if sets:
                        res = conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)
                        filas_pg += res.rowcount
        
        duracion = round(time.time() - start_time, 2)
        logs.append(f"🐘 Postgres: {filas_pg} filas actualizadas.")
        logs.append(f"⏱️ DURACIÓN: {duracion} segundos.")
        logs.append(f"🚀 ÉXITO: {datetime.datetime.now(zona_local).strftime('%H:%M:%S')}")
        progreso_bar.progress(100, text="Sincronización finalizada")
        return logs
    except Exception as e:
        return [f"❌ Error crítico: {str(e)}"]

# --- 3. INTERFAZ ---

tab1, tab2 = st.tabs(["🔄 Sincronización Manual", "📊 Datos Postgres (QGIS)"])

with tab1:
    st.title("🖥️ MIAA Control Center")
    with st.container(border=True):
        if st.button("🚀 FORZAR CARGA DE DATOS", use_container_width=True, type="primary"):
            st.session_state.last_logs = ejecutar_sincronizacion_total()

    if 'last_logs' not in st.session_state: st.session_state.last_logs = ["SISTEMA LISTO."]
    log_txt = "<br>".join([str(l) for l in st.session_state.last_logs])
    st.markdown(f'<div class="consola-log">{log_txt}</div>', unsafe_allow_html=True)

with tab2:
    st.title("🖥️ MIAA Control Center")
    if st.button("🔄 Refrescar Tabla", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    # Visualización básica de Postgres para no romper lógica
    p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
    eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
    df_pg = pd.read_sql('SELECT * FROM public."Pozos" LIMIT 100', eng_pg)
    st.dataframe(df_pg, use_container_width=True)
