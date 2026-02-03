import streamlit as st
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
import datetime
import mysql.connector
import pytz
import numpy as np

# --- 1. CONFIGURACIÓN ---
zona_local = pytz.timezone('America/Mexico_City')
st.set_page_config(page_title="MIAA Control Maestro", layout="wide")

# Credenciales
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': 'bWkrw1Uum1O&', 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': 'M144.Tec', 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# Mapeos de columnas
MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)':                  '_Caudal',
    'PRESION_(kg/cm2)':                '_Presion',
    'LONGITUD_DE_COLUMNA':             '_Long_colum',
    'COLUMNA_DIAMETRO_1':              '_Diam_colum',
    'TIPO_COLUMNA':                    '_Tipo_colum',
    'SECTOR_HIDRAULICO':               '_Sector',
    'NIVEL_DINAMICO_(mts)':            '_Nivel_Din',
    'NIVEL_ESTATICO_(mts)':            '_Nivel_Est',
    'EXTRACCION_MENSUAL_(m3)':         '_Vm_estr',
    'HORAS_DE_OPERACIÓN_DIARIA_(hrs)': '_Horas_op',
    'DISTRITO_1':                      '_Distrito',
    'ESTATUS':                         '_Estatus',
    'TELEMETRIA':                      '_Telemetria',
    'FECHA_ACTUALIZACION':             '_Ultima_actualizacion'
}

MAPEO_SCADA = {
    "P-002": {
        "GASTO_(l.p.s.)":"PZ_002_TRC_CAU_INS",
        "PRESION_(kg/cm2)":"PZ_002_TRC_PRES_INS",
        "VOLTAJE_L1":"PZ_002_TRC_VOL_L1_L2",
        "VOLTAJE_L2":"PZ_002_TRC_VOL_L2_L3",
        "VOLTAJE_L3":"PZ_002_TRC_VOL_L1_L3",
        "AMP_L1":"PZ_002_TRC_CORR_L1",
        "AMP_L2":"PZ_002_TRC_CORR_L2",
        "AMP_L3":"PZ_002_TRC_CORR_L3",
        "LONGITUD_DE_COLUMNA":"PZ_002_TRC_LONG_COLUM",
        "SUMERGENCIA":"PZ_002_TRC_SUMERG",
        "NIVEL_DINAMICO":"PZ_002_TRC_NIV_EST",
    },
    "P-003": {
        "GASTO_(l.p.s.)":"PZ_003_CAU_INS",
        "PRESION_(kg/cm2)":"PZ_003_PRES_INS",
        "VOLTAJE_L1":"PZ_003_VOL_L1_L2",
        "VOLTAJE_L2":"PZ_003_VOL_L2_L3",
        "VOLTAJE_L3":"PZ_003_VOL_L1_L3",
        "AMP_L1":"PZ_003_CORR_L1",
        "AMP_L2":"PZ_003_CORR_L2",
        "AMP_L3":"PZ_003_CORR_L3",
        "LONGITUD_DE_COLUMNA":"PZ_003_LONG_COLUM",
        "SUMERGENCIA":"PZ_003_SUMERG",
        "NIVEL_DINAMICO":"PZ_003_NIV_EST",
    },
}

# --- 2. LÓGICA DE DATOS ---

def ejecutar_sincronizacion_web():
    logs = []
    try:
        # 1. Carga de Google Sheets
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]

        # 2. Consulta SCADA con Regla de Validación (>0)
        conn_s = mysql.connector.connect(**DB_SCADA)
        all_tags = []
        for p_id in MAPEO_SCADA: all_tags.extend(MAPEO_SCADA[p_id].values())
        
        query = f"SELECT r.NAME, h.VALUE FROM vfitagnumhistory h JOIN VfiTagRef r ON h.GATEID = r.GATEID WHERE r.NAME IN ({','.join(['%s']*len(all_tags))}) AND h.FECHA >= NOW() - INTERVAL 1 DAY ORDER BY h.FECHA DESC"
        df_scada = pd.read_sql(query, conn_s, params=all_tags).drop_duplicates('NAME')
        
        for p_id, config in MAPEO_SCADA.items():
            for col_excel, tag_name in config.items():
                res = df_scada.loc[df_scada['NAME'] == tag_name, 'VALUE']
                if not res.empty:
                    val = float(res.values[0])
                    # REGLA: Si el dato es 0 o menor, no se escribe (prevalece Sheets)
                    if val > 0:
                        df.loc[df['POZOS'] == p_id, col_excel] = round(val, 2)
        conn_s.close()
        logs.append("🧬 Datos SCADA filtrados (solo valores > 0 procesados).")

        # 3. Actualización MySQL
        p_my = urllib.parse.quote_plus(DB_INFORME['password'])
        eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        with eng_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            df.replace({np.nan: None, pd.NaT: None}).to_sql('INFORME', con=conn, if_exists='append', index=False)
        logs.append("✅ Servidor MySQL actualizado correctamente.")

        # 4. Sincronización Postgres QGIS
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        
        filas_actualizadas = 0
        with eng_pg.begin() as conn:
            for _, row in df.iterrows():
                id_val = str(row['ID']).strip() if pd.notnull(row['ID']) else None
                if id_val and id_val != "nan":
                    params = {'id': id_val}
                    sets = []
                    for csv_col, pg_col in MAPEO_POSTGRES.items():
                        if csv_col in df.columns:
                            params[pg_col] = None if pd.isna(row[csv_col]) else row[csv_col]
                            sets.append(f'"{pg_col}" = :{pg_col}')
                    if sets:
                        res = conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)
                        filas_actualizadas += res.rowcount
        
        logs.append(f"🐘 Postgres sincronizado: {filas_actualizadas} pozos actualizados.")
        logs.append(f"⏰ Finalizado: {datetime.datetime.now(zona_local).strftime('%H:%M:%S')}")
        return logs
    except Exception as e:
        return [f"❌ Error crítico: {str(e)}"]

@st.cache_data(ttl=15)
def leer_base_datos_postgres():
    try:
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        return pd.read_sql('SELECT * FROM public."Pozos" ORDER BY "ID" ASC', eng_pg)
    except Exception as e:
        st.error(f"Error de conexión Postgres: {e}")
        return pd.DataFrame()

# --- 3. INTERFAZ WEB ---

st.title("🖥️ MIAA")

# Creación de pestañas permanentes en la versión Web
tab_control, tab_postgres = st.tabs(["🚀 Panel de Sincronización", "🗄️ Base de Datos Postgres"])

with tab_control:
    st.subheader("Control de Carga de Datos")
    st.info("Presione el botón para iniciar la sincronización manual entre Google Sheets, SCADA y las bases de datos.")
    
    if st.button("🚀 INICIAR PROCESO", use_container_width=True):
        st.session_state.web_logs = ejecutar_sincronizacion_web()
    
    # Consola de salida
    historial = st.session_state.get('web_logs', ["ESPERANDO ÓRDENES..."])
    st.markdown(f'''
        <div style="background-color:#000; color:#0f0; padding:20px; border-radius:10px; font-family:Consolas, monospace; min-height:300px; border: 1px solid #333;">
            {"<br>".join(historial)}
        </div>
    ''', unsafe_allow_html=True)

with tab_postgres:
    st.subheader("Registros en public.Pozos (Postgres)")
    
    c_ref, c_empty = st.columns([1, 4])
    with c_ref:
        if st.button("🔄 Refrescar Tabla"):
            st.cache_data.clear()
            st.rerun()
    
    df_pozos = leer_base_datos_postgres()
    
    if not df_pozos.empty:
        # Buscador dinámico para la tabla
        filtro = st.text_input("🔍 Filtrar por nombre, ID o sector...")
        if filtro:
            df_pozos = df_pozos[df_pozos.astype(str).apply(lambda x: x.str.contains(filtro, case=False)).any(axis=1)]
        
        st.dataframe(df_pozos, use_container_width=True, height=600)
    else:
        st.warning("No hay datos disponibles en la tabla de Postgres.")
