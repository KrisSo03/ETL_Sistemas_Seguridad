import mysql.connector as mysql

def get_connection():
    return mysql.connect(
        host="localhost", #Host de la BD
        user="root", #Usuario
        password="lead@1234", #Contraseña
        database="dwh_inventario", #Base de datos destino
    )

def bulk_upsert_inventario(df):
    #Inserta registros en la tabla inventario_consolidado con UPSERT
    conn = get_connection()
    cur = conn.cursor()
    sql = """
    INSERT INTO inventario_consolidado
      (codigo_producto, nombre, descripcion_producto, stock, categoria, imagen_url, fecha_carga_dw)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      nombre=VALUES(nombre),
      descripcion_producto=VALUES(descripcion_producto),
      stock=VALUES(stock),
      categoria=VALUES(categoria),
      imagen_url=VALUES(imagen_url),
      fecha_carga_dw=VALUES(fecha_carga_dw)
    """
    try:
        #Convierte dataframe a lista de tuplas para insertar
        cur.executemany(sql, list(df.itertuples(index=False, name=None)))
        conn.commit()
        return cur.rowcount  #Cantidad de filas insertadas/actualizadas
    except Exception as e:
        conn.rollback() #Revierte cambios si falla
        raise e
    finally:
        cur.close() #Cierra cursor
        conn.close() #Cierra conexión