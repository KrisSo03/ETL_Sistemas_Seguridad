import logging
from pathlib import Path

def get_logger():
    #Crea carpeta logs si no existe
    log_folder = Path("logs")
    log_folder.mkdir(exist_ok=True)
    #Crea logger principal
    logger = logging.getLogger("ETL")
    logger.setLevel(logging.INFO)
    #Handler de archivo .log
    file_handler = logging.FileHandler(log_folder / "etl.log")
    file_handler.setLevel(logging.INFO)
    #Handler en consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    #Formato estándar para logs
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    #Asigna handlers al logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger
