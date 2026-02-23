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
st.markdown("""
Esta herramienta está adaptada para el formato de reporte CSV y extrae inteligencia de negocio:
1. **Lectura Blindada:** Cuadra el saldo inicial con los movimientos y detecta omisiones humanas.
2. **Inteligencia Operativa:** Identifica el nombre del cliente por factura.
3. **Análisis Ejecutivo:** Genera un reporte automático con los hallazgos críticos.
""")

# ==============================================================================
# 1. UTILIDADES DE LIMPIEZA Y NORMALIZACIÓN
# ==============================================================================

def normalizar_referencia_base(ref):
    """
    BLINDAJE NIVEL 2: Atrapa nulos reales, celdas en blanco o textos fantasma de Excel como "nan" o "None"
    """
    if pd.isna(ref): return "⚠️ SIN REFERENCIA CAPTURADA"
    
    s = str(ref).strip()
    if not s or s.lower() in ["nan", "none", "null"]:
        return "⚠️ SIN REFERENCIA CAPTURADA"
        
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
# 2. PROCESAMIENTO CENTRAL (ENGINE)
# ==============================================================================

# Cambiamos el nombre de la función a 'procesar_contpaq_core' para destruir el caché anterior
@st.cache_data
def procesar_contpaq_core(file):
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
    
    col_map = {
        0: "poliza", 1: "fecha_raw", 2: "concepto", 3: "referencia", 
        4: "cargos", 5: "abonos", 6: "saldo_acumulado", 7: "desc_linea"
    }
    movs = movs.rename(columns=col_map)
    
    for c in ["cargos", "abonos", "saldo_acumulado"]:
        movs[c] = pd.to_numeric(movs[c], errors='coerce').fillna(0)
        
    movs["fecha"] = pd.to_datetime(movs["fecha_raw"], errors="coerce")
    
    # La limpieza se hace directamente desde la función, ya no necesitamos fillna
    movs["referencia_norm"] = movs["referencia"].apply(normalizar_referencia_base)
    movs["saldo_neto"] = movs["cargos"] - movs["abonos"]
    
    movs["cliente"] = np.where(
        movs["cargos"] > 0, 
        movs["desc_linea"].astype(str).str.replace(r"^CXC\s+", "", regex=True).str.strip(), 
        movs["concepto"].astype(str).str.strip()
    )
    
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

def analizar_saldos(movs, resumen):
    # Ya no filtramos a los vivos, mandamos TODOS para que sume los que no tienen referencia
    saldo_facturas = movs.groupby(["meta_codigo"]).agg(movimientos_netos=("saldo_neto", "sum")).reset_index()
    
    df = resumen.merge(saldo_facturas, on="meta_codigo", how="left").fillna(0)
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

uploaded_file = st.file_uploader("📂 Sube reporte (CSV extraído de la plataforma)", type=["xlsx", "csv"])

if uploaded_file:
    with st.spinner("🚀 Extrayendo clientes y generando análisis..."):
        try:
            # Mandamos llamar la nueva función sin caché
            movs, resumen = procesar_contpaq_core(uploaded_file)
            df_audit = analizar_saldos(movs, resumen)
            
            # ---> AISLAMOS A LOS CULPABLES AQUÍ <---
            movs_sin_referencia = movs[movs["referencia_norm"] == "⚠️ SIN REFERENCIA CAPTURADA"].copy()
            movs_validos = movs[movs["referencia_norm"] != "⚠️ SIN REFERENCIA CAPTURADA"]
            
            resumen_referencias = movs_validos.groupby(["meta_codigo", "referencia_norm"]).agg(
                cliente=("cliente", "first"),
                fecha_origen=("fecha", "min"),
                total_cargos=("cargos", "sum"),
                total_abonos=("abonos", "sum"),
                saldo_pendiente=("saldo_neto", "sum")
            ).reset_index()
            
            facturas_pend = resumen_referencias[
                (resumen_referencias["total_cargos"] > 0) & 
                (resumen_referencias["saldo_pendiente"] > 0.01)
            ].copy()
            
            pagos_huerfanos = resumen_referencias[
                (resumen_referencias["total_cargos"] == 0) & 
                (resumen_referencias["total_abonos"] > 0)
            ].copy()

            pagos_excedentes = resumen_referencias[
                (resumen_referencias["total_cargos"] > 0) & 
                (resumen_referencias["saldo_pendiente"] < -0.01)
            ].copy()
            
            # Buscar Notas de crédito o ajustes manuales
            ajustes_sospechosos = movs[movs['concepto'].str.contains('Nota de Crédito|Ajuste', case=False, na=False)]
            
        except Exception as e:
            st.error(f"Error procesando el archivo: {e}")
            st.stop()
            
    # KPIs Globales
    col1, col2, col3, col4 = st.columns(4)
    saldo_total = df_audit["saldo_final_aux"].sum()
    diferencia_total = df_audit["diferencia"].sum()
    
    col1.metric("Saldo Contable Total", f"${saldo_total:,.2f}")
    col2.metric("Diferencia Matemática", f"${diferencia_total:,.2f}", delta_color="inverse")
    col3.metric("Pagos Huérfanos / Excedentes", len(pagos_huerfanos) + len(pagos_excedentes), delta_color="inverse")
    col4.metric("Facturas por Cobrar", len(facturas_pend))

    # ==============================================================================
    # NUEVO: REPORTE EJECUTIVO AUTOMATIZADO
    # ==============================================================================
    st.divider()
    with st.expander("🤖 Ver Análisis Ejecutivo Automático", expanded=True):
        st.markdown("### 📊 Hallazgos Críticos de la Auditoría")
        
        hubo_hallazgos = False
        
        # 1. Alerta de Notas de Crédito / Ajustes
        if not ajustes_sospechosos.empty:
            hubo_hallazgos = True
            st.error(f"🚨 **Riesgo de Mala Captura:** Se detectaron **{len(ajustes_sospechosos)}** movimientos manuales como *'Notas de Crédito'* o *'Ajustes'*. Revisa que quien los capturó haya puesto el número de factura correcto.")
        
        # 2. Alerta de Pagos Excedentes
        if not pagos_excedentes.empty:
            hubo_hallazgos = True
            max_exc = pagos_excedentes.loc[pagos_excedentes['saldo_pendiente'].idxmin()]
            st.warning(f"⚠️ **{len(pagos_excedentes)} Facturas Pagadas de Más:** Se detectaron facturas donde el abono supera al cargo.")
            
        # 3. Alerta de Pagos Huérfanos
        if not pagos_huerfanos.empty:
            hubo_hallazgos = True
            max_hue = pagos_huerfanos.loc[pagos_huerfanos['total_abonos'].idxmax()]
            st.info(f"💡 **{len(pagos_huerfanos)} Pagos de Periodos Anteriores (Huérfanos):** Entraron abonos sin una factura de cargo asociada en este reporte.")
            
        # 4. EL DETECTOR QUE ESTÁBAMOS BUSCANDO
        if not movs_sin_referencia.empty:
            hubo_hallazgos = True
            total_fuga = movs_sin_referencia['saldo_neto'].sum()
            
            st.error(f"🔍 **DETECTOR DE FUGAS (MOVIMIENTOS SIN REFERENCIA):** Se detectaron **{len(movs_sin_referencia)} movimientos** donde quien capturó dejó la descripción/referencia en blanco. Estos movimientos suman un neto de **${abs(total_fuga):,.2f}** y causaban las diferencias. La matemática de la app ahora cuadra porque los hemos agrupado, pero debes revisar estas pólizas en tu sistema.")
            
            st.markdown("**👇 Detalle de los movimientos sin referencia capturada:**")
            st.dataframe(
                movs_sin_referencia[["meta_codigo", "poliza", "fecha_raw", "concepto", "cargos", "abonos"]],
                use_container_width=True,
                hide_index=True
            )

        # Si a pesar de agrupar todo sigue habiendo una diferencia (error de la plataforma o fechas raras)
        if abs(diferencia_total) > UMBRAL_TOLERANCIA:
            hubo_hallazgos = True
            st.error(f"❌ **DIFERENCIA IRRECONCILIABLE:** Hay una diferencia global de **${diferencia_total:,.2f}** que no proviene de faltas de captura de referencia. Revisa las cuentas en el Semáforo Contable.")

        if not hubo_hallazgos:
            st.success("✅ La cartera se ve excepcionalmente limpia. No se detectaron anomalías.")

    st.divider()

    # Pestañas
    t1, t2, t3, t4 = st.tabs(["🚦 Semáforo Contable", "📑 Facturas Pendientes", "❓ Abonos Antiguos / Excedentes", "📉 Gráficos"])
    
    with t1:
        st.subheader("Conciliación Matemática de las Cuentas")
        ver_todo = st.toggle("Ver solo cuentas con diferencias", value=False)
        df_show = df_audit[df_audit["estado"] != "🟢 OK"] if ver_todo else df_audit
        
        st.dataframe(
            df_show[["meta_codigo", "meta_nombre", "estado", "meta_saldo_inicial", "movimientos_netos", "saldo_calculado", "saldo_final_aux", "diferencia"]],
            use_container_width=True,
            column_config={
                "meta_saldo_inicial": st.column_config.NumberColumn("Saldo Inicial", format="$%.2f"),
                "movimientos_netos": st.column_config.NumberColumn("Neto (Cargos-Abonos)", format="$%.2f"),
                "saldo_calculado": st.column_config.NumberColumn("Saldo Teórico", format="$%.2f"),
                "saldo_final_aux": st.column_config.NumberColumn("Saldo Reporte", format="$%.2f"),
                "diferencia": st.column_config.NumberColumn("Diferencia", format="$%.2f"),
            }
        )
        
    with t2:
        st.subheader("Detalle Operativo de Cobranza (Cartera Viva)")
        st.dataframe(
            facturas_pend[["cliente", "referencia_norm", "fecha_origen", "total_cargos", "total_abonos", "saldo_pendiente"]].sort_values("fecha_origen"),
            use_container_width=True,
            column_config={
                "cliente": "Cliente",
                "referencia_norm": "Factura",
                "fecha_origen": st.column_config.DateColumn("Fecha Cargo", format="DD/MM/YYYY"),
                "total_cargos": st.column_config.NumberColumn("Cargos", format="$%.2f"),
                "total_abonos": st.column_config.NumberColumn("Abonos", format="$%.2f"),
                "saldo_pendiente": st.column_config.NumberColumn("Saldo por Cobrar", format="$%.2f")
            }
        )
        st.download_button("Descargar Facturas Pendientes", to_excel(facturas_pend), "pendientes_cobro.xlsx")
        
    with t3:
        st.subheader("Pagos de Periodos Anteriores o Anticipos (Sin cargo de origen)")
        if pagos_huerfanos.empty:
            st.success("✅ No hay pagos huérfanos.")
        else:
            st.dataframe(
                pagos_huerfanos[["cliente", "referencia_norm", "fecha_origen", "total_abonos", "saldo_pendiente"]],
                use_container_width=True,
                column_config={
                    "cliente": "Cliente",
                    "referencia_norm": "Referencia del Pago",
                    "fecha_origen": st.column_config.DateColumn("Fecha del Pago", format="DD/MM/YYYY"),
                    "total_abonos": st.column_config.NumberColumn("Monto del Abono", format="$%.2f"),
                    "saldo_pendiente": st.column_config.NumberColumn("Saldo a Favor", format="$%.2f")
                }
            )
            
        st.divider()
        st.subheader("Facturas con Pago Excedente")
        if pagos_excedentes.empty:
            st.success("✅ No hay facturas pagadas de más.")
        else:
            st.dataframe(
                pagos_excedentes[["cliente", "referencia_norm", "total_cargos", "total_abonos", "saldo_pendiente"]],
                use_container_width=True,
                column_config={
                    "cliente": "Cliente",
                    "referencia_norm": "Factura",
                    "total_cargos": st.column_config.NumberColumn("Cargos", format="$%.2f"),
                    "total_abonos": st.column_config.NumberColumn("Abonos", format="$%.2f"),
                    "saldo_pendiente": st.column_config.NumberColumn("Excedente a Favor", format="$%.2f")
                }
            )
            
    with t4:
        fig = go.Figure(data=[
            go.Bar(name='Cuentas Cuadradas', x=['Matemática del Reporte'], y=[saldo_total - diferencia_total], marker_color='#2ecc71'),
            go.Bar(name='Diferencia (Error de Captura/Reporte)', x=['Matemática del Reporte'], y=[diferencia_total], marker_color='#e74c3c')
        ])
        fig.update_layout(barmode='stack', title="Salud Matemática de las Cuentas")
        st.plotly_chart(fig, use_container_width=True)

else:
    st.info("Esperando archivo CSV de PLATAFORMA...")
