import logging
from pathlib import Path

def setup_logger():
    """
    Configura el sistema de logging para el proyecto ETL.

    - Crea la carpeta 'logs' en la raíz del proyecto si no existe.
    - Registra los mensajes tanto en un archivo ('logs/etl.log') como en la consola.
    - Devuelve el logger raíz configurado.
    """

    # Ruta base del proyecto (una carpeta arriba del archivo actual)
    project_root = Path(__file__).resolve().parents[1]
    
    # Carpeta para guardar logs
    logs_folder = project_root / "logs"
    logs_folder.mkdir(exist_ok=True)
    
    # Archivo de log
    log_file_path = logs_folder / "etl.log"

    # Configuración básica: formato, nivel y archivo destino
    logging.basicConfig(
        filename=log_file_path,
        filemode="a",
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=logging.INFO
    )

    # Mostrar mensajes también en la terminal
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    console_handler.setFormatter(console_format)

    # Obtener el logger raíz y agregar el handler de consola
    root_logger = logging.getLogger()
    root_logger.addHandler(console_handler)

    root_logger.info("Logger initialized successfully.")

    return root_logger

