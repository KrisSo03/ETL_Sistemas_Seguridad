import sys, traceback
import pandas as pd
from urllib.parse import unquote
from datetime import datetime
from etl.logger_setup import get_logger
from etl.db import bulk_upsert_inventario

#==================== LOGGER ====================

logger = get_logger()  #Inicializa logger central

#==================== FUENTES ====================

URLS = [
    "https://raw.githubusercontent.com/CSMore/ETL_Sistemas_Seguridad/main/data/raw/Inventario%20POS%201.xlsx",
    "https://raw.githubusercontent.com/CSMore/ETL_Sistemas_Seguridad/main/data/raw/Inventario%20POS%202.xlsx",
    "https://raw.githubusercontent.com/CSMore/ETL_Sistemas_Seguridad/main/data/raw/Inventario_3.csv"
]

#==================== E: EXTRACCIÓN ====================

def extraer_archivos_github(urls):
    #Lee CSV/XLSX desde URLs raw de GitHub, agrega columna de origen y unifica
    dfs = []
    for url in urls:
        try:
            if url.lower().endswith(".csv"):
                df = pd.read_csv(url, encoding="latin1", engine="python")
            elif url.lower().endswith(".xlsx"):
                df = pd.read_excel(url)
            else:
                logger.warning(f"Formato no soportado: {url}")
                continue
            df["Fuente_Archivo"] = unquote(url.split("/")[-1])  #Trazabilidad
            dfs.append(df)
            logger.info(f"Leído {df.shape[0]} filas de {df['Fuente_Archivo'].iloc[0]}")
        except Exception as e:
            logger.error(f"Error leyendo {url}: {e}")
    if not dfs:
        logger.warning("No se leyó ningún archivo válido.")
        return pd.DataFrame()
    df_union = pd.concat(dfs, ignore_index=True)
    logger.info(f"Unificación completa: {df_union.shape[0]} filas")
    return df_union

#==================== T: TRANSFORMACIÓN ====================
def transformar_datos(df):
    #Limpia, normaliza y conforma columnas requeridas por la tabla DW
    if df.empty:
        logger.warning("DF vacío en transformación.")
        return df
    cols = {c.lower(): c for c in df.columns}  #Mapa de columnas en minúsculas
    def get(col_aliases, default=None):
        #Devuelve la serie de la primera coincidencia
        for a in col_aliases:
            if a in cols:
                return df[cols[a]]
        return default
    df_out = pd.DataFrame({
        "codigo_producto":      get(["codigo_producto","codigo","cod_producto"]),
        "nombre":               get(["nombre","producto"]),
        "descripcion_producto": get(["descripcion_producto","descripcion","detalle"]),
        "stock":                get(["stock","existencia","cantidad"]),
        "categoria":            get(["categoria","rubro","familia"])
    })
    img_series = get(["imagen_url","imagen(url)","imagen","url_imagen"])
    df_out["imagen_url"] = img_series if img_series is not None else None
    antes = df_out.shape[0]
    df_out = df_out.dropna(subset=["nombre"])
    logger.info(f"Eliminados sin nombre: {antes - df_out.shape[0]}")
    df_out["stock"] = pd.to_numeric(df_out["stock"], errors="coerce")
    antes = df_out.shape[0]
    df_out = df_out[df_out["stock"].notnull() & (df_out["stock"] >= 0)]
    logger.info(f"Eliminados por stock inválido/negativo: {antes - df_out.shape[0]}")
    if "codigo_producto" in df_out.columns:
        antes = df_out.shape[0]
        df_out = df_out.drop_duplicates(subset=["codigo_producto"])
        logger.info(f"Duplicados por código removidos: {antes - df_out.shape[0]}")
    df_out["categoria"] = (
        df_out["categoria"].astype(str).str.strip().str.lower()
        .map({"electronica":"Electrónica","electrónica":"Electrónica","hogar":"Hogar","ropa":"Ropa","seguridad":"Seguridad","otros":"Otros"})
        .fillna("Otros")
    )
    df_out["fecha_carga_dw"] = datetime.now()
    df_out = df_out[["codigo_producto","nombre","descripcion_producto","stock","categoria","imagen_url","fecha_carga_dw"]]
    for c in ["codigo_producto","nombre","descripcion_producto","categoria","imagen_url"]:
        df_out[c] = df_out[c].astype(str)
    logger.info(f"Transformación final: {df_out.shape[0]} filas")
    return df_out

#==================== L: CARGA ====================

def load(df):
    #Carga en MySQL usando función de db.py (UPSERT por PK)
    if df.empty:
        logger.warning("No hay datos para cargar.")
        return
    n = bulk_upsert_inventario(df)
    logger.info(f"Cargados/actualizados: {n} registros")

#==================== UTIL: WRAPPER DE ETAPAS ====================

def _step(nombre, fn, *args, **kwargs):
    #Ejecuta una etapa con logs y captura de errores
    logger.info(f"[{nombre}] inicio")
    try:
        res = fn(*args, **kwargs)
        logger.info(f"[{nombre}] ok")
        return res
    except Exception as e:
        logger.exception(f"[{nombre}] falló: {e}")  #Incluye stack trace en etl.log y consola
        traceback.print_exc()
        raise

#==================== ORQUESTACIÓN ====================

def run_pipeline():
    #Orquesta E->T->L con manejo de errores y código de salida distinto de 0 si falla
    logger.info("=== INICIO PIPELINE ETL ===")
    try:
        df_raw = _step("EXTRACCION", extraer_archivos_github, URLS)
        df_dw  = _step("TRANSFORMACION", transformar_datos, df_raw)
        _step("CARGA", load, df_dw)
        logger.info("=== FIN PIPELINE ETL (OK) ===")
    except Exception:
        logger.error("=== FIN PIPELINE ETL (ERROR) ===")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
