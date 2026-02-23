import streamlit as st
import pandas as pd
import numpy as np
import re
from io import BytesIO
import plotly.graph_objects as go

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
st.set_page_config(page_title="Auditoría Master FARMERS", layout="wide", page_icon="🛡️")
UMBRAL_TOLERANCIA = 1.0 

st.title("🛡️🍴 Auditoría Master de Saldos (FARMERS)")

# ==============================================================================
# 1. UTILIDADES DE LIMPIEZA
# ==============================================================================

def normalizar_referencia_base(ref):
    if pd.isna(ref): return "⚠️ SIN REFERENCIA CAPTURADA"
    s = str(ref).strip()
    if not s or s.lower() in ["nan", "none", "null"]: return "⚠️ SIN REFERENCIA CAPTURADA"
    s = s.upper()
    m_pago = re.search(r'F\.?\s*(\d+)', s)
    if m_pago: return m_pago.group(1)
    m_fac = re.search(r'A\s*-\s*(\d+)', s)
    if m_fac: return m_fac.group(1)
    m_num = re.findall(r'\d+', s)
    if m_num: return m_num[-1]
    return s

def cargar_archivo_robusto(uploaded_file):
    try:
        return pd.read_excel(uploaded_file, header=None)
    except:
        uploaded_file.seek(0)
        try: return pd.read_csv(uploaded_file, header=None, encoding='latin-1')
        except: return pd.read_csv(uploaded_file, header=None, encoding='utf-8', errors='replace')

def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# ==============================================================================
# 2. PROCESAMIENTO CENTRAL CON MODO DIAGNÓSTICO
# ==============================================================================

@st.cache_data
def procesar_contpaq_diag_profundo(file):
    raw = cargar_archivo_robusto(file)
    raw_str = raw.astype(str)
    
    patron_cuenta = r"^\d{3}-\d{3}-\d{3}"
    is_cuenta = raw_str[0].str.match(patron_cuenta, na=False)
    
    df = raw.copy()
    df["meta_codigo"] = np.where(is_cuenta, df[0], np.nan)
    df["meta_nombre"] = np.where(is_cuenta, df[2], np.nan)
    df["meta_codigo"] = df["meta_codigo"].ffill()
    df["meta_nombre"] = df["meta_nombre"].ffill()
    
    is_saldo_ini = raw_str[3].str.contains("Saldo Inicial", case=False, na=False)
    df["meta_saldo_inicial_row"] = np.where(is_saldo_ini, pd.to_numeric(df[6], errors='coerce'), np.nan)
    
    saldos_dict = df.dropna(subset=["meta_saldo_inicial_row"]).set_index("meta_codigo")["meta_saldo_inicial_row"].to_dict()
    df["meta_saldo_inicial"] = df["meta_codigo"].map(saldos_dict).fillna(0)
    
    patron_fecha = r"^\d{4}-\d{2}-\d{2}"
    is_mov = raw_str[1].str.match(patron_fecha, na=False)
    movs = df[is_mov].copy()
    
    # ---> 🚨 EL DETECTOR DE FILAS FANTASMA <---
    # Buscamos cualquier fila que tenga dinero, pero que el código se saltó por no tener fecha correcta
    cargos_raw = pd.to_numeric(raw[4], errors='coerce').fillna(0)
    abonos_raw = pd.to_numeric(raw[5], errors='coerce').fillna(0)
    tiene_dinero = (cargos_raw > 0) | (abonos_raw > 0)
    
    filas_fantasma = df[tiene_dinero & ~is_mov & ~is_saldo_ini].copy()
    
    col_map = {
        0: "poliza", 1: "fecha_raw", 2: "concepto", 3: "referencia", 
        4: "cargos", 5: "abonos", 6: "saldo_acumulado", 7: "desc_linea"
    }
    movs = movs.rename(columns=col_map)
    filas_fantasma = filas_fantasma.rename(columns=col_map)
    
    for c in ["cargos", "abonos", "saldo_acumulado"]:
        movs[c] = pd.to_numeric(movs[c], errors='coerce').fillna(0)
        
    movs["fecha"] = pd.to_datetime(movs["fecha_raw"], errors="coerce")
    movs["referencia_norm"] = movs["referencia"].apply(normalizar_referencia_base)
    movs["saldo_neto"] = movs["cargos"] - movs["abonos"]
    
    movs["cliente"] = np.where(
        movs["cargos"] > 0, 
        movs["desc_linea"].astype(str).str.replace(r"^CXC\s+", "", regex=True).str.strip(), 
        movs["concepto"].astype(str).str.strip()
    )
    
    if not movs.empty:
        resumen = movs.groupby(["meta_codigo", "meta_nombre"]).agg(saldo_final_aux=("saldo_acumulado", "last")).reset_index()
        resumen["meta_saldo_inicial"] = resumen["meta_codigo"].map(saldos_dict).fillna(0)
    else:
        resumen = pd.DataFrame(columns=["meta_codigo", "meta_nombre", "saldo_final_aux", "meta_saldo_inicial"])
        
    return movs, resumen, filas_fantasma

# ==============================================================================
# 3. LÓGICA Y APP UI
# ==============================================================================

def analizar_saldos(movs, resumen):
    saldo_facturas = movs.groupby(["meta_codigo"]).agg(movimientos_netos=("saldo_neto", "sum")).reset_index()
    df = resumen.merge(saldo_facturas, on="meta_codigo", how="left").fillna(0)
    df["saldo_calculado"] = df["meta_saldo_inicial"] + df["movimientos_netos"]
    df["diferencia"] = df["saldo_final_aux"] - df["saldo_calculado"]
    df["estado"] = np.where(abs(df["diferencia"]) <= UMBRAL_TOLERANCIA, "🟢 OK", "🔴 Descuadre")
    return df

uploaded_file = st.file_uploader("📂 Sube reporte CSV", type=["xlsx", "csv"])

if uploaded_file:
    with st.spinner("🔍 Analizando con modo diagnóstico..."):
        movs, resumen, filas_fantasma = procesar_contpaq_diag_profundo(uploaded_file)
        df_audit = analizar_saldos(movs, resumen)
        diferencia_total = df_audit["diferencia"].sum()
        
    st.divider()
    st.markdown("### 🤖 Panel de Hallazgos y Descuadres a Detalle")
    
    # 1. EL DETECTOR DE FILAS FANTASMA (El más probable culpable)
    if not filas_fantasma.empty:
        st.error(f"👻 **ALERTA DE FILAS FANTASMA:** Encontramos **{len(filas_fantasma)} fila(s)** con dinero, pero que la herramienta se estaba saltando porque la fecha viene vacía o con un formato extraño en el reporte. **Revisa si estos montos suman los $212,216.41:**")
        st.dataframe(filas_fantasma[["poliza", "fecha_raw", "concepto", "referencia", "cargos", "abonos"]], use_container_width=True)
        
    # 2. MOSTRAR EXACTAMENTE QUÉ CUENTA ESTÁ DESCUADRADA
    if abs(diferencia_total) > UMBRAL_TOLERANCIA:
        st.warning(f"❌ **DETALLE DE LA DIFERENCIA MATEMÁTICA (${diferencia_total:,.2f}):** A continuación se muestra exactamente qué cuentas y clientes causan el descuadre. Compara la columna 'Saldo Teórico' vs 'Saldo Reporte'.")
        df_mal = df_audit[df_audit["estado"] != "🟢 OK"]
        st.dataframe(
            df_mal[["meta_codigo", "meta_nombre", "meta_saldo_inicial", "movimientos_netos", "saldo_calculado", "saldo_final_aux", "diferencia"]],
            use_container_width=True,
            column_config={
                "meta_saldo_inicial": st.column_config.NumberColumn("Saldo Inicial", format="$%.2f"),
                "movimientos_netos": st.column_config.NumberColumn("Neto de Movimientos", format="$%.2f"),
                "saldo_calculado": st.column_config.NumberColumn("Saldo Teórico (Debe Ser)", format="$%.2f"),
                "saldo_final_aux": st.column_config.NumberColumn("Saldo Reporte", format="$%.2f"),
                "diferencia": st.column_config.NumberColumn("Falta / Sobra", format="$%.2f"),
            }
        )
    else:
        st.success("✅ La matemática cuadra perfectamente a cero.")
