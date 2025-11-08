import sys, traceback
import pandas as pd
from urllib.parse import unquote
from datetime import datetime
from etl.logger_setup import get_logger
from etl.db import bulk_upsert_inventario

#==================== LOGGER ====================

logger = get_logger()  #Inicializa logger central

#==================== FUENTES ====================

URLS_LOCAL = [
    "data/raw/Inventario POS 1.xlsx",
    "data/raw/Inventario POS 2.xlsx",
    "data/raw/Inventario_3.csv"
]

#==================== E: EXTRACCIÓN ====================

def extraer_local(rutas):
    #Lee archivos locales CSV/XLSX
    import io
    dfs=[]
    for p in rutas:
        try:
            if p.lower().endswith(".csv"):
                try:
                    df=pd.read_csv(p, sep=",", engine="python", on_bad_lines="skip", encoding="utf-8-sig")
                except UnicodeDecodeError:
                    try:
                        df=pd.read_csv(p, sep=",", engine="python", on_bad_lines="skip", encoding="utf-8")
                    except UnicodeDecodeError:
                        df=pd.read_csv(p, sep=",", engine="python", on_bad_lines="skip", encoding="latin1")
            elif p.lower().endswith(".xlsx"):
                df=pd.read_excel(p)
            else:
                logger.warning(f"Formato no soportado: {p}"); continue
            df["Fuente_Archivo"]=p
            dfs.append(df)
        except Exception as e:
            logger.error(f"Error leyendo {p}: {e}")
    df=pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    logger.info(f"Columnas detectadas: {list(df.columns)}")
    return df


#==================== T: TRANSFORMACIÓN ====================

def transformar_datos(df):
    #Limpia, normaliza encabezados y conforma columnas para la tabla DW
    import re, unicodedata
    USE_INT_PK = True # Si quiere que sea alfa numerico poner FALSE
    if df.empty:
        logger.warning("DF vacío en transformación."); return df
    #Normaliza: quita acentos/espacios/símbolos y baja a minúsculas
    def _norm(s):
        s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode()
        return re.sub(r"[^a-z0-9]", "", s.lower())
    #Diccionario {nombre_normalizado: nombre_original}
    cols_norm = {_norm(c): c for c in df.columns}
    logger.info(f"Columnas normalizadas: {[(_norm(c), c) for c in df.columns]}")
    #Helper para tomar la primera coincidencia disponible
    def get(cands):
        for k in cands:
            if k in cols_norm: return df[cols_norm[k]]
        return None
    #Aliases más comunes (ya normalizados); "Código Producto" -> "codigoproducto"
    col_codigo = get(["codigoproducto","codigo","codproducto","codigoprod","codprod","id","sku","itemcode"])
    col_nombre = get(["nombre","producto","titulo","nombreproducto"])
    col_desc   = get(["descripcionproducto","descripcion","detalle"])
    col_stock  = get(["stock","existencia","cantidad","unidades"])
    col_cat    = get(["categoria","rubro","familia","departamento"])
    col_img    = get(["imagenurl","imagen","urlimagen","foto"])
    #Construcción base
    out = pd.DataFrame({
        "codigo_producto": col_codigo,
        "nombre": col_nombre,
        "descripcion_producto": col_desc,
        "stock": col_stock,
        "categoria": col_cat
    })
    out["imagen_url"] = col_img if col_img is not None else None
    #Descartes clave
    antes = out.shape[0]
    out = out.dropna(subset=["nombre","codigo_producto"])
    out = out[out["codigo_producto"].astype(str).str.strip()!=""]
    logger.info(f"Descartados sin nombre/codigo: {antes - out.shape[0]}")
    #PK según esquema
    if USE_INT_PK:
        out["codigo_producto"] = pd.to_numeric(out["codigo_producto"], errors="coerce")
        a = out.shape[0]; out = out[out["codigo_producto"].notnull()]
        logger.info(f"Descartados por codigo no numérico: {a - out.shape[0]}")
        out["codigo_producto"] = out["codigo_producto"].astype("int64")
    else:
        out["codigo_producto"] = out["codigo_producto"].astype(str).str.strip()
    #Stock válido
    out["stock"] = pd.to_numeric(out["stock"], errors="coerce")
    a = out.shape[0]; out = out[out["stock"].notnull() & (out["stock"]>=0)]
    logger.info(f"Eliminados por stock inválido/negativo: {a - out.shape[0]}")
    #Duplicados por código
    a = out.shape[0]; out = out.drop_duplicates(subset=["codigo_producto"])
    logger.info(f"Duplicados por código removidos: {a - out.shape[0]}")
    #Categoría canónica
    out["categoria"] = (out["categoria"].astype(str).str.strip().str.lower()
                        .map({"electronica":"Electrónica","electrónica":"Electrónica","hogar":"Hogar","ropa":"Ropa","seguridad":"Seguridad","otros":"Otros"})
                        .fillna("Otros"))
    #Columnas finales
    out["fecha_carga_dw"] = datetime.now()
    out = out[["codigo_producto","nombre","descripcion_producto","stock","categoria","imagen_url","fecha_carga_dw"]]
    for c in ["nombre","descripcion_producto","categoria","imagen_url"]:
        out[c] = out[c].astype(str)
    #Validación de longitud por columna VARCHAR
    max_lens = {
        "nombre": 250,
        "categoria": 100,
        "imagen_url": 500,
    }
    antes = out.shape[0]
    for col, max_len in max_lens.items():
        out = out[out[col].str.len() <= max_len]
    logger.info(f"Descartados por exceso de longitud: {antes - out.shape[0]}")
    logger.info(f"Transformación final: {out.shape[0]} filas")
    return out

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
        logger.exception(f"[{nombre}] falló: {e}")
        traceback.print_exc()
        raise

#==================== ORQUESTACIÓN ====================

def run_pipeline():
    #Orquesta E->T->L con manejo de errores
    logger.info("=== INICIO PIPELINE ETL ===")
    try:
        df_raw = _step("EXTRACCION_LOCAL", extraer_local, URLS_LOCAL)
        df_dw  = _step("TRANSFORMACION", transformar_datos, df_raw)
        _step("CARGA", load, df_dw)
        logger.info("=== FIN PIPELINE ETL (OK) ===")
    except Exception:
        logger.error("=== FIN PIPELINE ETL (ERROR) ===")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
