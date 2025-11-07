-- Crear esquemas
CREATE DATABASE IF NOT EXISTS dwh_inventario;
CREATE DATABASE IF NOT EXISTS etl_logs;

-- ==============================
-- Esquema: dw (Data Warehouse)
-- ==============================

USE dwh_inventario;

-- Tabla final de inventario consolidado
CREATE TABLE inventario_consolidado (
  codigo_producto     VARCHAR(64)  NOT NULL PRIMARY KEY,
  nombre              VARCHAR(200) NOT NULL,
  descripcion_producto TEXT,
  stock               INT UNSIGNED NOT NULL,
  categoria           VARCHAR(100) NOT NULL,
  imagen_url          VARCHAR(500) NOT NULL,
  fecha_carga_dw      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX ix_categoria (categoria),
  INDEX ix_nombre (nombre)
);

-- ==============================
-- Esquema: etl_logs
-- ==============================

USE etl_logs;

-- Registro de ejecución del ETL
CREATE TABLE etl_run (
  id_run                  BIGINT AUTO_INCREMENT PRIMARY KEY,
  inicio_ejecucion        DATETIME NOT NULL,
  fin_ejecucion           DATETIME NULL,
  registros_extraidos     INT DEFAULT 0,
  registros_transformados INT DEFAULT 0,
  registros_cargados      INT DEFAULT 0,
  estado                  ENUM('OK', 'ERROR', 'PARCIAL') NOT NULL DEFAULT 'OK',
  mensaje_resumen         VARCHAR(500)
);

-- Registro de errores del ETL
CREATE TABLE etl_error (
  id_error       BIGINT AUTO_INCREMENT PRIMARY KEY,
  id_run         BIGINT NOT NULL,
  etapa          ENUM('EXTRACCION', 'TRANSFORMACION', 'CARGA') NOT NULL,
  fuente_archivo VARCHAR(255),
  linea_n        INT NULL,
  tipo_error     VARCHAR(100) NOT NULL,
  detalle        VARCHAR(1000) NOT NULL,
  creado_en      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (id_run) REFERENCES etl_run(id_run)
);
