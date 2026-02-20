import streamlit as st
import pandas as pd
import numpy as np
import re
import unicodedata
from io import BytesIO
import plotly.graph_objects as go

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
st.set_page_config(page_title="Auditoría Master Farmers", layout="wide", page_icon="🛡️")
UMBRAL_TOLERANCIA = 1.0 

st.title("🛡️ Auditoría Master de Saldos (Farmers)")
st.markdown("""
Esta herramienta está adaptada para el nuevo formato de reporte (CSV):
1. **Lectura Blindada:** Detecta cuentas, saldos iniciales y cruza referencias automáticamente.
2. **Lógica de Negocio:** Detecta cruces de cuentas, facturas pendientes y cuadra el saldo inicial con los movimientos.
""")

# ==============================================================================
# 1. UTILIDADES DE LIMPIEZA Y NORMALIZACIÓN
# ==============================================================================

def normalizar_referencia_base(ref):
    """
    Extracción inteligente para emparejar 'Factura de Cliente A-2796' con 'Ap. Pago Cte. 1078 F. 2796'
    """
    if pd.isna(ref): return None
    s = str(ref).strip().upper()
    
    # Si es un pago referenciando a factura: Ej. "Ap. Pago Cte. 1071 F. 2766" -> extrae 2766
    m_pago = re.search(r'F\.?\s*(\d+)', s)
    if m_pago: return m_pago.group(1)
    
    # Si es el alta de la factura: Ej. "Factura de Cliente A-2779" -> extrae 2779
    m_fac = re.search(r'A\s*-\s*(\d+)', s)
    if m_fac: return m_fac.group(1)
    
    # Fallback: extrae el último bloque numérico que encuentre
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
# 2. PROCESAMIENTO CENTRAL (ENGINE PARA NUEVO FORMATO)
# ==============================================================================

@st.cache_data
def procesar_contpaq_engine(file):
    raw = cargar_archivo_robusto(file)
    raw_str = raw.astype(str)
    
    # 1. Detectar Cuentas (Formato: 104-001-001 en Col 0, nombre en Col 2)
    patron_cuenta = r"^\d{3}-\d{3}-\d{3}"
    is_cuenta = raw_str[0].str.match(patron_cuenta, na=False)
    
    df = raw.copy()
    df["meta_codigo"] = np.where(is_cuenta, df[0], np.nan)
    df["meta_nombre"] = np.where(is_cuenta, df[2], np.nan)
    
    df["meta_codigo"] = df["meta_codigo"].ffill()
    df["meta_nombre"] = df["meta_nombre"].ffill()
    
    # 2. Detectar Saldo Inicial (Dice "Saldo Inicial" en Col 3, valor en Col 6)
    is_saldo_ini = raw_str[3].str.contains("Saldo Inicial", case=False, na=False)
    df["meta_saldo_inicial_row"] = np.where(is_saldo_ini, pd.to_numeric(df[6], errors='coerce'), np.nan)
    
    # Mapear el saldo inicial a todas las filas de su respectiva cuenta
    saldos_dict = df.dropna(subset=["meta_saldo_inicial_row"]).set_index("meta_codigo")["meta_saldo_inicial_row"].to_dict()
    df["meta_saldo_inicial"] = df["meta_codigo"].map(saldos_dict).fillna(0)
    
    # 3. Detectar Movimientos (Tienen fecha formato YYYY-MM-DD en Col 1)
    patron_fecha = r"^\d{4}-\d{2}-\d{2}"
    is_mov = raw_str[1].str.match(patron_fecha, na=False)
    movs = df[is_mov].copy()
    
    # 4. Mapear y Limpiar Columnas
    col_map = {
        0: "poliza", 
        1: "fecha_raw", 
        2: "concepto",       # Ej: Nombre de cliente o "Factura de..."
        3: "referencia",     # Columna donde viene el detalle F. 2766 o A-2779
        4: "cargos", 
        5: "abonos", 
        6: "saldo_acumulado"
    }
    movs = movs.rename(columns=col_map)
    
    for c in ["cargos", "abonos", "saldo_acumulado"]:
        movs[c] = pd.to_numeric(movs[c], errors='coerce').fillna(0)
        
    movs["fecha"] = pd.to_datetime(movs["fecha_raw"], errors="coerce")
    
    # 5. Aplicar Mapeo Inteligente (Extracción de # Factura)
    movs["referencia_norm"] = movs["referencia"].apply(normalizar_referencia_base)
    
    # 6. Totales Auxiliar (Tomamos el último saldo acumulado como saldo final)
    if not movs.empty:
        resumen = movs.groupby(["meta_codigo", "meta_nombre"]).agg(
            saldo_final_aux=("saldo_acumulado", "last")
        ).reset_index()
        resumen["meta_saldo_inicial"] = resumen["meta_codigo"].map(saldos_dict).fillna(0)
    else:
        resumen = pd.DataFrame(columns=["meta_codigo", "meta_nombre", "saldo_final_aux", "meta_saldo_inicial"])
        
    return movs, resumen

# ==============================================================================
# 3. LÓGICA DE NEGOCIO
# ==============================================================================

@st.cache_data
def detectar_cruces(movs):
    """Detecta facturas que tocan múltiples cuentas."""
    validos = movs[movs["referencia_norm"].notna()]
    
    por_cuenta = validos.groupby(["referencia_norm", "meta_codigo", "meta_nombre"]).agg(
        cargos=("cargos", "sum"),
        abonos=("abonos", "sum")
    ).reset_index()
    
    por_cuenta["tiene_cargo"] = por_cuenta["cargos"] > 0
    por_cuenta["tiene_abono"] = por_cuenta["abonos"] > 0
    
    nivel_ref = por_cuenta.groupby("referencia_norm").agg(
        num_cuentas=("meta_codigo", "nunique"),
        hay_cargo=("tiene_cargo", "max"),
        hay_abono=("tiene_abono", "max")
    ).reset_index()
    
    cruces = nivel_ref[ (nivel_ref["num_cuentas"] > 1) & nivel_ref["hay_cargo"] & nivel_ref["hay_abono"] ]
    
    if cruces.empty:
        return pd.DataFrame()
    
    detalle = por_cuenta[por_cuenta["referencia_norm"].isin(cruces["referencia_norm"])].copy()
    detalle["saldo_en_cuenta"] = detalle["cargos"] - detalle["abonos"]
    return detalle.sort_values("referencia_norm")

def analizar_saldos(movs, resumen):
    """Construye la tabla maestra de auditoría (Semáforo)."""
    vivas = movs[movs["referencia_norm"].notna()]
    saldo_facturas = vivas.groupby(["meta_codigo"]).apply(lambda x: (x["cargos"] - x["abonos"]).sum()).reset_index(name="movimientos_netos")
    
    df = resumen.merge(saldo_facturas, on="meta_codigo", how="left").fillna(0)
    
    # El saldo teórico debería ser: Saldo Inicial + (Cargos - Abonos)
    df["saldo_calculado"] = df["meta_saldo_inicial"] + df["movimientos_netos"]
    df["diferencia"] = df["saldo_final_aux"] - df["saldo_calculado"]
    
    def clasificar(row):
        if abs(row["diferencia"]) <= UMBRAL_TOLERANCIA: return "🟢 OK"
        return "🔴 Diferencia No Explicada"
        
    df["estado"] = df.apply(clasificar, axis=1)
    return df

# ==============================================================================
# APP UI
# ==============================================================================

uploaded_file = st.file_uploader("📂 Sube reporte CONTPAQ (Excel o CSV)", type=["xlsx", "csv"])

if uploaded_file:
    with st.spinner("🚀 Procesando archivo..."):
        try:
            movs, resumen = procesar_contpaq_engine(uploaded_file)
            
            df_audit = analizar_saldos(movs, resumen)
            df_cruces = detectar_cruces(movs)
            
            # Facturas Pendientes (Detalle)
            movs_validos = movs[movs["referencia_norm"].notna()]
            facturas_pend = movs_validos.groupby(["meta_codigo", "meta_nombre", "referencia_norm"]).agg(
                fecha=("fecha", "min"),
                saldo=("cargos", lambda x: x.sum() - movs_validos.loc[x.index, "abonos"].sum())
            ).reset_index()
            facturas_pend = facturas_pend[facturas_pend["saldo"].abs() > 0.01]
            
        except Exception as e:
            st.error(f"Error procesando: {e}")
            st.stop()
            
    # KPIs Globales
    st.divider()
    col1, col2, col3, col4 = st.columns(4)
    saldo_total = df_audit["saldo_final_aux"].sum()
    diferencia_total = df_audit["diferencia"].sum()
    
    col1.metric("Saldo Contable Total", f"${saldo_total:,.2f}")
    col2.metric("Diferencia sin Soporte", f"${diferencia_total:,.2f}", delta_color="inverse")
    col3.metric("Facturas con Cruce", len(df_cruces["referencia_norm"].unique()) if not df_cruces.empty else 0)
    col4.metric("Cuentas con Error", len(df_audit[df_audit["estado"].str.contains("🔴")]))

    # Pestañas
    t1, t2, t3, t4 = st.tabs(["🚦 Semáforo", "📑 Facturas Pendientes", "🔀 Cruces de Cuentas", "📉 Gráficos"])
    
    with t1:
        st.subheader("Conciliación por Cuenta")
        ver_todo = st.toggle("Ver solo cuentas con problemas", value=False)
        df_show = df_audit[df_audit["estado"] != "🟢 OK"] if ver_todo else df_audit
        
        st.dataframe(
            df_show[["meta_codigo", "meta_nombre", "estado", "meta_saldo_inicial", "movimientos_netos", "saldo_calculado", "saldo_final_aux", "diferencia"]],
            use_container_width=True,
            column_config={
                "meta_saldo_inicial": st.column_config.NumberColumn("Saldo Inicial", format="$%.2f"),
                "movimientos_netos": st.column_config.NumberColumn("Movimientos Netos", format="$%.2f"),
                "saldo_calculado": st.column_config.NumberColumn("Saldo Calculado", format="$%.2f"),
                "saldo_final_aux": st.column_config.NumberColumn("Saldo Reporte", format="$%.2f"),
                "diferencia": st.column_config.NumberColumn("Diferencia", format="$%.2f"),
            }
        )
        st.download_button("Descargar Semáforo", to_excel(df_audit), "semaforo.xlsx")
        
    with t2:
        st.subheader("Detalle de Facturas Vivas")
        st.dataframe(
            facturas_pend.sort_values("fecha"),
            use_container_width=True,
            column_config={"saldo": st.column_config.NumberColumn("Saldo Pendiente", format="$%.2f")}
        )
        
    with t3:
        st.subheader("Referencias Cruzadas (Error común de aplicación de pagos)")
        if df_cruces.empty:
            st.success("✅ No se detectaron cruces de referencias entre cuentas.")
        else:
            st.warning("⚠️ Estas facturas tienen cargos en una cuenta y abonos en otra distinta.")
            st.dataframe(df_cruces)
            
    with t4:
        fig = go.Figure(data=[
            go.Bar(name='Facturas OK', x=['Total'], y=[saldo_total - diferencia_total], marker_color='#2ecc71'),
            go.Bar(name='Diferencias', x=['Total'], y=[diferencia_total], marker_color='#e74c3c')
        ])
        fig.update_layout(barmode='stack', title="Calidad del Saldo")
        st.plotly_chart(fig, use_container_width=True)

else:
    st.info("Esperando archivo...")
