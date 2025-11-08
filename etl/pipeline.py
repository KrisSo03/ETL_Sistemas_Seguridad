import sys, traceback
import pandas as pd
from urllib.parse import unquote
from datetime import datetime
from etl.logger_setup import get_logger
from etl.db import bulk_upsert_inventario
import re, unicodedata, math

logger = get_logger()

URLS_LOCAL = [
    "data/raw/Inventario POS 1.xlsx",
    "data/raw/Inventario POS 2.xlsx",
    "data/raw/Inventario_3.csv"
]

# ==== FUNCIÓN AUXILIAR ====

def limpiar_texto(txt):
    if pd.isna(txt):
        return ""
    txt = str(txt)
    txt = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")
    txt = re.sub(r'\s+', ' ', txt).strip()
    return txt


# ========== EXTRACCIÓN ==========

def extraer_local(rutas):
    dfs = []
    for p in rutas:
        try:
            if p.lower().endswith(".csv"):
                try:
                    df = pd.read_csv(p, sep=",", engine="python", on_bad_lines="skip", encoding="utf-8-sig")
                    encoding_used = "utf-8-sig"
                except UnicodeDecodeError:
                    try:
                        df = pd.read_csv(p, sep=",", engine="python", on_bad_lines="skip", encoding="utf-8")
                        encoding_used = "utf-8"
                    except UnicodeDecodeError:
                        df = pd.read_csv(p, sep=",", engine="python", on_bad_lines="skip", encoding="latin1")
                        encoding_used = "latin1"
                logger.info(f"Leído {p} con codificación: {encoding_used}")
            elif p.lower().endswith(".xlsx"):
                df = pd.read_excel(p)
                logger.info(f"Leído {p} como archivo Excel")
            else:
                logger.warning(f"Formato no soportado: {p}")
                continue

            df["Fuente_Archivo"] = p
            dfs.append(df)
        except Exception as e:
            logger.error(f"Error leyendo {p}: {e}")

    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    logger.info(f"Columnas detectadas: {list(df.columns)}")
    return df


# ========== TRANSFORMACIÓN ==========

def transformar_datos(df):
    import re, unicodedata
    from datetime import datetime

    def _norm(s):
        s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
        return re.sub(r"[^a-z0-9]", "", s.lower())

    if df.empty:
        logger.warning("DF vacío en transformación.")
        return df

    # Normalizar nombres de columnas
    cols_norm = {_norm(c): c for c in df.columns}
    logger.info(f"Columnas normalizadas: {[(_norm(c), c) for c in df.columns]}")

    def get(cands):
        for k in cands:
            if k in cols_norm:
                return df[cols_norm[k]]
        return None

    # Detección de columnas clave
    col_codigo = get(["codigoproducto", "codigo", "codproducto", "codigoprod", "codprod", "id", "sku", "itemcode"])
    col_nombre = get(["nombre", "producto", "titulo", "nombreproducto"])

    # Detección robusta de descripción (considerando varias variantes)
    col_desc = None
    if "Descripcion_Producto" in df.columns and "Descripción del producto" in df.columns:
        col_desc = df["Descripcion_Producto"].fillna("") + "\n" + df["Descripción del producto"].fillna("")
    elif "Descripcion_Producto" in df.columns:
        col_desc = df["Descripcion_Producto"]
    elif "Descripción del producto" in df.columns:
        col_desc = df["Descripción del producto"]
    else:
        col_desc = get([
            "descripcionproducto", "descripcion", "detalle", "descripciondelproducto",
            "desc", "descripcionprod", "descripcionitem", "descripcion_producto",
            "descripcion del producto"
        ])

    if col_desc is None:
        logger.warning("❌ No se encontró ninguna columna de descripción.")
        col_desc = pd.Series([pd.NA] * len(df))
    else:
        logger.info(f"✅ Columna de descripción detectada: {col_desc.name}")

    col_stock = get(["stock", "existencia", "cantidad", "unidades"])
    col_cat = get(["categoria", "rubro", "familia", "departamento"])
    col_img = get(["imagenurl", "imagen", "urlimagen", "foto"])

    # Construcción del DataFrame base
    out = pd.DataFrame({
        "codigo_producto": col_codigo,
        "nombre": col_nombre,
        "descripcion_producto": col_desc,
        "stock": col_stock,
        "categoria": col_cat
    })
    out["imagen_url"] = col_img if col_img is not None else None

    # Validación de código y nombre
    out = out.dropna(subset=["nombre", "codigo_producto"])
    out = out[out["codigo_producto"].astype(str).str.strip() != ""]

    # Validar y convertir código a entero
    out["codigo_producto"] = pd.to_numeric(out["codigo_producto"], errors="coerce")
    out = out[out["codigo_producto"].notnull()]
    out["codigo_producto"] = out["codigo_producto"].astype("int64")

    # Validar y convertir stock
    out["stock"] = pd.to_numeric(out["stock"], errors="coerce")
    out = out[out["stock"].notnull() & (out["stock"] >= 0)]

    # Eliminar duplicados por código
    out = out.drop_duplicates(subset=["codigo_producto"])

    # ===================== LIMPIEZA DE COLUMNAS DE TEXTO =====================

    cols_a_limpiar = {
        "nombre": 250,
        "categoria": 100,
        "imagen_url": 500
    }

    cols_a_preservar = {
        "descripcion_producto": 250
    }

    # A) LIMPIAR columnas específicas
    for col, max_len in cols_a_limpiar.items():
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str)
            out[col] = out[col].apply(limpiar_texto).str.slice(0, max_len)
            out[col] = out[col].replace("", "Sin dato disponible")

    # B) PRESERVAR descripciones (mantiene saltos, tabs, espacios)
    for col, max_len in cols_a_preservar.items():
        if col in out.columns:
            out.loc[out[col].isna(), col] = "Sin descripción disponible"
            out[col] = out[col].astype(str).str.slice(0, max_len)

    # ===================== NORMALIZACIÓN DE CATEGORÍAS =====================

    def normalizar_categoria(cat):
        if pd.isna(cat): 
            return "Otros"
        cat = str(cat).strip().lower()
        cat = unicodedata.normalize("NFKD", cat).encode("ascii", "ignore").decode()
        cat = re.sub(r"[^a-z]", "", cat)
        if "brocas" in cat:
            return "Brocas"
        elif "consumible" in cat:
            return "Consumibles"
        elif "electricidad" in cat:
            return "Electricidad"
        elif "embalaje" in cat:
            return "Embalajes"
        elif "fijacion" in cat:
            return "Fijaciones"
        elif "fontaneria" in cat:
            return "Fontanería"
        elif "herraje" in cat:
            return "Herrajes"
        elif "herramienta" in cat:
            return "Herramientas"
        elif "manual" in cat:
            return "Herramientas manuales"
        elif "iluminacion" in cat:
            return "Iluminación"
        elif "jardin" in cat:
            return "Jardinería"
        elif "pintura" in cat:
            return "Pinturas"
        elif "quimico" in cat:
            return "Químicos"
        elif "seguridad" in cat:
            return "Seguridad"
        elif "soldadura" in cat:
            return "Soldadura"
        elif "tornillo" in cat:
            return "Tornillos"
        elif "ferreteriaonline" in cat:
            return "Ferretería Online"
        elif "ferreterialocalfisico" in cat or "ferreterialocal" in cat:
            return "Ferretería Local Físico"
        else:
            return "Otros"

    out["categoria"] = out["categoria"].apply(normalizar_categoria)

    # ===================== FECHA Y REORDENAMIENTO =====================

    out["fecha_carga_dw"] = datetime.now()

    out = out[[
        "codigo_producto", "nombre", "descripcion_producto", "stock",
        "categoria", "imagen_url", "fecha_carga_dw"
    ]]

    logger.info(f"Transformación final: {out.shape[0]} filas")
    return out


# ========== CARGA ==========

def load(df):
    if df.empty:
        logger.warning("No hay datos para cargar.")
        return
    n = bulk_upsert_inventario(df)
    logger.info(f"Cargados/actualizados: {n} registros")


# ========== ORQUESTADOR ==========

def _step(nombre, fn, *args, **kwargs):
    logger.info(f"[{nombre}] inicio")
    try:
        res = fn(*args, **kwargs)
        logger.info(f"[{nombre}] ok")
        return res
    except Exception as e:
        logger.exception(f"[{nombre}] falló: {e}")
        traceback.print_exc()
        raise


def run_pipeline():
    logger.info("=== INICIO PIPELINE ETL ===")
    try:
        df_raw = _step("EXTRACCION_LOCAL", extraer_local, URLS_LOCAL)
        df_dw = _step("TRANSFORMACION", transformar_datos, df_raw)
        _step("CARGA", load, df_dw)
        logger.info("=== FIN PIPELINE ETL (OK) ===")
    except Exception:
        logger.error("=== FIN PIPELINE ETL (ERROR) ===")
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
