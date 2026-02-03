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
st.set_page_config(page_title="MIAA Control App", layout="wide")

# Credenciales
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': 'bWkrw1Uum1O&', 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': 'M144.Tec', 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# Mapeo de columnas
MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal', 'PRESION_(kg/cm2)': '_Presion',
    'LONGITUD_DE_COLUMNA': '_Long_colum', 'COLUMNA_DIAMETRO_1': '_Diam_colum',
    'TIPO_COLUMNA': '_Tipo_colum', 'SECTOR_HIDRAULICO': '_Sector',
    'NIVEL_DINAMICO_(mts)': '_Nivel_Din', 'NIVEL_ESTATICO_(mts)': '_Nivel_Est',
    'EXTRACCION_MENSUAL_(m3)': '_Vm_estr', 'HORAS_DE_OPERACIÓN_DIARIA_(hrs)': '_Horas_op',
    'DISTRITO_1': '_Distrito', 'ESTATUS': '_Estatus',
    'TELEMETRIA': '_Telemetria', 'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

MAPEO_SCADA = {
    "P-002": {
        "GASTO_(l.p.s.)":"PZ_002_TRC_CAU_INS", "PRESION_(kg/cm2)":"PZ_002_TRC_PRES_INS",
        "VOLTAJE_L1":"PZ_002_TRC_VOL_L1_L2", "VOLTAJE_L2":"PZ_002_TRC_VOL_L2_L3",
        "VOLTAJE_L3":"PZ_002_TRC_VOL_L1_L3", "AMP_L1":"PZ_002_TRC_CORR_L1",
        "AMP_L2":"PZ_002_TRC_CORR_L2", "AMP_L3":"PZ_002_TRC_CORR_L3",
        "LONGITUD_DE_COLUMNA":"PZ_002_TRC_LONG_COLUM", "SUMERGENCIA":"PZ_002_TRC_SUMERG",
        "NIVEL_DINAMICO":"PZ_002_TRC_NIV_EST",
    },
    "P-003": {
        "GASTO_(l.p.s.)":"PZ_003_CAU_INS", "PRESION_(kg/cm2)":"PZ_003_PRES_INS",
        "VOLTAJE_L1":"PZ_003_VOL_L1_L2", "VOLTAJE_L2":"PZ_003_VOL_L2_L3",
        "VOLTAJE_L3":"PZ_003_VOL_L1_L3", "AMP_L1":"PZ_003_CORR_L1",
        "AMP_L2":"PZ_003_CORR_L2", "AMP_L3":"PZ_003_CORR_L3",
        "LONGITUD_DE_COLUMNA":"PZ_003_LONG_COLUM", "SUMERGENCIA":"PZ_003_SUMERG",
        "NIVEL_DINAMICO":"PZ_003_NIV_EST",
    }
}

# --- 2. FUNCIONES DE DATOS ---

def ejecutar_sincronizacion():
    logs = []
    try:
        # 1. Leer Sheets
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]

        # 2. SCADA con Validación > 0
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
                    if val > 0: # REGLA: Solo sobreescribe si hay dato válido > 0
                        df.loc[df['POZOS'] == p_id, col_excel] = round(val, 2)
        conn_s.close()
        logs.append("🧬 Telemetría SCADA sincronizada (Ceros ignorados).")

        # 3. MySQL Informe
        p_my = urllib.parse.quote_plus(DB_INFORME['password'])
        eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        with eng_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            df.replace({np.nan: None, pd.NaT: None}).to_sql('INFORME', con=conn, if_exists='append', index=False)
        logs.append("✅ Tabla MySQL 'INFORME' actualizada.")

        # 4. Postgres QGIS
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        
        filas_pg = 0
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
                        filas_pg += res.rowcount
        
        logs.append(f"🐘 Postgres sincronizado ({filas_pg} registros).")
        logs.append(f"🚀 Terminado: {datetime.datetime.now(zona_local).strftime('%H:%M:%S')}")
        return logs
    except Exception as e:
        return [f"❌ Error: {str(e)}"]

@st.cache_data(ttl=10)
def consultar_postgres():
    try:
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        return pd.read_sql('SELECT * FROM public."Pozos" ORDER BY "ID" ASC', eng_pg)
    except Exception as e:
        st.error(f"Error al leer Postgres: {e}")
        return pd.DataFrame()

# --- 3. INTERFAZ ---

st.sidebar.title("📱 MIAA Control App")
# AQUÍ ESTÁ EL MENÚ PARA CAMBIAR ENTRE PESTAÑAS/VISTAS
vista = st.sidebar.radio("Ir a:", ["Sincronizador", "Ver Base de Datos Postgres"])

if vista == "Sincronizador":
    st.header("⚡ Ejecución de Sincronía")
    if st.button("🚀 INICIAR CARGA MANUAL", use_container_width=True):
        st.session_state.logs_app = ejecutar_sincronizacion()
    
    logs = st.session_state.get('logs_app', ["Esperando acción..."])
    st.markdown(f'''
        <div style="background-color:#000; color:#0f0; padding:15px; border-radius:5px; font-family:monospace; min-height:250px;">
            {"<br>".join(logs)}
        </div>
    ''', unsafe_allow_html=True)

elif vista == "Ver Base de Datos Postgres":
    st.header("🗄️ Tabla 'Pozos' en Postgres")
    if st.button("🔄 Actualizar Tabla"):
        st.cache_data.clear()
    
    df_db = consultar_postgres()
    if not df_db.empty:
        st.dataframe(df_db, use_container_width=True, height=600)
    else:
        st.info("No se encontraron registros o no hay conexión.")
