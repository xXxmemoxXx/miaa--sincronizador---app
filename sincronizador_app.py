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
st.set_page_config(page_title="MIAA Control Maestro App", layout="wide")

# Credenciales
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': 'bWkrw1Uum1O&', 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': 'M144.Tec', 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

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
        
        "NIVEL_DINAMICO":"PZ_003_NIV_EST",
    },
}

# --- 2. LÓGICA DE PROCESAMIENTO ---

def ejecutar_sincronizacion():
    logs = []
    progreso = st.progress(0)
    try:
        # 1. Leer Sheets
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        progreso.progress(25)

        # 2. SCADA con Regla: > 0
        conn_s = mysql.connector.connect(**DB_SCADA)
        all_tags = []
        for p_id in MAPEO_SCADA: all_tags.extend(MAPEO_SCADA[p_id].values())
        
        query_scada = f"SELECT r.NAME, h.VALUE FROM vfitagnumhistory h JOIN VfiTagRef r ON h.GATEID = r.GATEID WHERE r.NAME IN ({','.join(['%s']*len(all_tags))}) AND h.FECHA >= NOW() - INTERVAL 1 DAY ORDER BY h.FECHA DESC"
        df_scada = pd.read_sql(query_scada, conn_s, params=all_tags).drop_duplicates('NAME')
        
        for p_id, config in MAPEO_SCADA.items():
            for col_excel, tag_name in config.items():
                res = df_scada.loc[df_scada['NAME'] == tag_name, 'VALUE']
                if not res.empty:
                    val = float(res.values[0])
                    if val > 0: # REGLA SOLICITADA: Solo escribir si es > 0
                        df.loc[df['POZOS'] == p_id, col_excel] = round(val, 2)
        conn_s.close()
        logs.append("✅ Datos SCADA validados y procesados.")
        progreso.progress(50)

        # 3. MySQL Informe
        p_my = urllib.parse.quote_plus(DB_INFORME['password'])
        eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        with eng_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            df.replace({np.nan: None, pd.NaT: None}).to_sql('INFORME', con=conn, if_exists='append', index=False)
        logs.append("✅ MySQL actualizado.")
        progreso.progress(75)

        # 4. Postgres QGIS
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        
        filas_pg = 0
        with eng_pg.begin() as conn:
            for _, row in df.iterrows():
                id_val = str(row['ID']).strip() if pd.notnull(row['ID']) else None
                if id_val and id_val != "nan":
                    params = {'id': id_val}
                    sets = [f'"{pg_col}" = :{pg_col}' for csv_col, pg_col in MAPEO_POSTGRES.items() if csv_col in df.columns]
                    for csv_col, pg_col in MAPEO_POSTGRES.items():
                        if csv_col in df.columns:
                            params[pg_col] = None if pd.isna(row[csv_col]) else row[csv_col]
                    
                    if sets:
                        res = conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)
                        filas_pg += res.rowcount
        
        logs.append(f"✅ Postgres actualizado ({filas_pg} filas).")
        logs.append(f"🚀 Sincronización terminada a las {datetime.datetime.now(zona_local).strftime('%H:%M:%S')}")
        progreso.progress(100)
        return logs
    except Exception as e:
        return [f"❌ Error crítico: {str(e)}"]

def obtener_datos_postgres():
    try:
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        return pd.read_sql('SELECT * FROM public."Pozos" ORDER BY "ID" ASC', eng_pg)
    except Exception as e:
        st.error(f"Error Postgres: {e}")
        return pd.DataFrame()

# --- 3. INTERFAZ APP ---

st.sidebar.title("📱 MIAA Control App")
opcion = st.sidebar.selectbox("Seleccione Vista", ["Sincronizador", "Base de Datos QGIS"])

if opcion == "Sincronizador":
    st.header("⚡ Sincronización de Datos")
    st.info("Esta acción actualizará MySQL y Postgres con los datos de Google Sheets y SCADA.")
    
    if st.button("🚀 INICIAR SINCRONIZACIÓNHORA", use_container_width=True):
        st.session_state.logs_app = ejecutar_sincronizacion()
    
    # Consola de Logs
    logs = st.session_state.get('logs_app', ["Esperando instrucción..."])
    st.markdown(f'''
        <div style="background-color:#1e1e1e; color:#00ff00; padding:15px; border-radius:10px; font-family:monospace; min-height:200px;">
            {"<br>".join(logs)}
        </div>
    ''', unsafe_allow_html=True)

elif opcion == "Base de Datos QGIS":
    st.header("🗄️ Base de Datos Postgres")
    
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("🔄 Refrescar"):
            st.rerun()
    
    df_pg = obtener_datos_postgres()
    if not df_pg.empty:
        # Buscador sencillo
        busqueda = st.text_input("🔍 Buscar pozo por ID o nombre...")
        if busqueda:
            df_pg = df_pg[df_pg.astype(str).apply(lambda x: x.str.contains(busqueda, case=False)).any(axis=1)]
        
        st.dataframe(df_pg, use_container_width=True, height=600)
        st.caption(f"Registros encontrados: {len(df_pg)}")
    else:
        st.warning("No se pudo cargar la tabla de Postgres.")
