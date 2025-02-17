import logging
import sys

def configuracion_logging(level=logging.INFO) -> None:
    """Configura el logging para toda la aplicacion"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout),
            #logging.FileHandler("automation.log", mode="w"),
            logging.FileHandler("automation.log", mode="a", encoding="utf-8")
        ]
    )