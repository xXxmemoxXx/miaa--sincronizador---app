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

# --- 2. LÓGICA DE PROCESAMIENTO ---

def ejecutar_sincronizacion_total():
    start_time = time.time()
    st.session_state.last_logs = [] 
    logs = []
    progreso_bar = st.progress(0, text="Iniciando proceso...")
    
    try:
        # 1. Lectura Google Sheets
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        
        # 2. SCADA con Regla de Validación (valor > 0)
        conn_s = mysql.connector.connect(**DB_SCADA)
        all_tags = []
        for p_id in MAPEO_SCADA: all_tags.extend(MAPEO_SCADA[p_id].values())
        
        query_scada = f"SELECT r.NAME, h.VALUE FROM vfitagnumhistory h JOIN VfiTagRef r ON h.GATEID = r.GATEID WHERE r.NAME IN ({','.join(['%s']*len(all_tags))}) AND h.FECHA >= NOW() - INTERVAL 1 DAY ORDER BY h.FECHA DESC"
        df_scada = pd.read_sql(query_scada, conn_s, params=all_tags).drop_duplicates('NAME')
        
        for p_id, config in MAPEO_SCADA.items():
            for col_excel, tag_name in config.items():
                res_scada = df_scada.loc[df_scada['NAME'] == tag_name, 'VALUE']
                if not res_scada.empty:
                    valor_scada = float(res_scada.values[0])
                    # REGLA: Si es 0 o negativo, NO se sobreescribe el dato de Sheets
                    if valor_scada > 0:
                        df.loc[df['POZOS'] == p_id, col_excel] = round(valor_scada, 2)
        
        conn_s.close()
        logs.append("🧬 SCADA: Sincronizado (Ceros y negativos omitidos).")

        # 3. MySQL (Tabla INFORME)
        p_my = urllib.parse.quote_plus(DB_INFORME['password'])
        eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        with eng_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            df_sql = df.replace({np.nan: None, pd.NaT: None})
            df_sql.to_sql('INFORME', con=conn, if_exists='append', index=False)
        logs.append("✅ MySQL: Actualizado.")

        # 4. Postgres (QGIS)
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
                            val = row[csv_col]
                            # Limpieza para Postgres
                            if pd.isna(val) or str(val).lower() == 'nan': clean_val = None
                            else: clean_val = val
                            params[pg_col] = clean_val
                            sets.append(f'"{pg_col}" = :{pg_col}')
                    
                    if sets:
                        res = conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)
                        filas_pg += res.rowcount
        
        logs.append(f"🐘 Postgres: {filas_pg} registros sincronizados.")
        logs.append(f"🚀 EXITOSO: {datetime.datetime.now(zona_local).strftime('%H:%M:%S')}")
        progreso_bar.progress(100)
        return logs
    except Exception as e:
        return [f"❌ Error: {str(e)}"]

@st.cache_data(ttl=10)
def consultar_postgres():
    try:
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        return pd.read_sql('SELECT * FROM public."Pozos" ORDER BY "ID" ASC', eng_pg)
    except:
        return pd.DataFrame()

# --- 3. INTERFAZ ---

st.title("🖥️ MIAA Control Center")

# Usar Radio en el Sidebar como alternativa a pestañas si st.tabs falla por el rerun
menu = st.sidebar.radio("MENÚ PRINCIPAL", ["Control de Sincronización", "Base de Datos Postgres"])

if menu == "Control de Sincronización":
    st.subheader("🎮 Panel de Operación")
    with st.container(border=True):
        col1, col2, col3, col4, col5 = st.columns([1.5, 1, 1, 1.5, 1.5])
        with col1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
        with col2: h_in = st.number_input("Hora", 0, 23, value=0)
        with col3: m_in = st.number_input("Min/Int", 0, 59, value=1)
        with col4:
            if "running" not in st.session_state: st.session_state.running = False
            label = "🛑 PARAR" if st.session_state.running else "▶️ INICIAR"
            if st.button(label, use_container_width=True):
                st.session_state.running = not st.session_state.running
                st.rerun()
        with col5:
            if st.button("🚀 FORZAR AHORA", use_container_width=True):
                st.session_state.last_logs = ejecutar_sincronizacion_total()

    log_txt = "<br>".join(st.session_state.get('last_logs', ["SISTEMA EN ESPERA..."]))
    st.markdown(f'<div style="background-color:black;color:#00FF00;padding:15px;font-family:monospace;height:400px;overflow-y:auto;border-radius:5px;">{log_txt}</div>', unsafe_allow_html=True)

else:
    st.subheader("🗄️ Tabla 'Pozos' en Postgres")
    if st.button("🔄 Refrescar Tabla"):
        st.cache_data.clear()
    
    df_pg = consultar_postgres()
    if not df_pg.empty:
        st.dataframe(df_pg, use_container_width=True, height=700)
    else:
        st.warning("No hay conexión con la base de datos Postgres.")

# --- 4. RELOJ (SIDEBAR PARA NO INTERRUMPIR) ---
if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    if modo == "Diario":
        prox = ahora.replace(hour=h_in, minute=m_in, second=0, microsecond=0)
        if ahora >= prox: prox += datetime.timedelta(days=1)
    else:
        intervalo = m_in if m_in > 0 else 1
        total_m = ahora.hour * 60 + ahora.minute
        sig = ((total_m // intervalo) + 1) * intervalo
        prox = ahora.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(minutes=sig)

    diff = prox - ahora
    st.sidebar.divider()
    st.sidebar.metric("⏳ PRÓXIMA CARGA EN:", str(diff).split('.')[0])
    
    if diff.total_seconds() <= 1:
        st.session_state.last_logs = ejecutar_sincronizacion_total()
        st.rerun()
    
    time.sleep(1)
    st.rerun()
